//! Rebalance for a `Mode::Distributed` cache: pulls a bucket from its
//! current owners the moment this node's [`OwnershipView`] says it gained
//! it, and releases a bucket's local data once this node has kept it
//! resident past the disown grace after losing it. The open()-time initial
//! pull and this module's ongoing loop share one mechanism, [`PullRequest`],
//! scoped to whichever bucket set is at hand.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use smol_str::SmolStr;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::Cluster;
use super::anti_entropy::{self, RoundOutcome};
use super::state_transfer::{self, DonorResult, Outcome};
use crate::net::{BucketPull, Mesh};
use crate::node::NodeId;
use crate::ownership::{OwnershipTracker, OwnershipView, ResidencySet, ownership_diff};
use crate::store::ShardOps;

/// One pull group: the donors to try, in rendezvous order, and the buckets
/// they all co-own.
type DonorGroup = (Vec<NodeId>, Vec<u16>);

/// Groups `buckets` by exact donor set (`view`'s live owners minus self, in
/// rendezvous order), so buckets sharing a donor set land in one
/// `StBuckets` round trip; the bucket-scoped analogue of
/// `cluster::group_by_owner_set`.
fn group_buckets_by_donor_set(
    view: &OwnershipView,
    self_node: NodeId,
    buckets: Vec<u16>,
) -> Vec<DonorGroup> {
    let mut groups: Vec<DonorGroup> = Vec::new();
    for bucket in buckets {
        let donors: Vec<NodeId> = view
            .owners_of(bucket)
            .iter()
            .copied()
            .filter(|&n| n != self_node)
            .collect();
        match groups.iter_mut().find(|(d, _)| *d == donors) {
            Some((_, group_buckets)) => group_buckets.push(bucket),
            None => groups.push((donors, vec![bucket])),
        }
    }
    groups
}

/// [`state_transfer::try_donor`]'s bucket-scoped counterpart: pulls
/// `buckets` from `donor` via [`Mesh::request_buckets`] instead of a
/// whole-cache [`Mesh::request_state`], sharing the per-donor stream-pull-
/// and-apply logic through [`state_transfer::pull_from_donor`].
#[expect(
    clippy::too_many_arguments,
    reason = "each parameter is independent context one donor attempt needs; `credited` in \
              particular must stay a caller-owned `&mut` shared across every donor \
              `pull_one_group` retries against, not something a struct could own instead \
              without losing that sharing"
)]
async fn try_donor_buckets(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    residency: &ResidencySet,
    donor: NodeId,
    buckets: Vec<u16>,
    view_hash: u64,
    credited: &mut HashSet<u16>,
) -> (DonorResult, u64, bool) {
    let pull = mesh
        .request_buckets(donor, cache.clone(), buckets, view_hash)
        .await;
    let cold = matches!(pull, Ok(BucketPull::Cold));
    let stream = pull.map(|answer| match answer {
        BucketPull::Stream(stream) => Some(stream),
        BucketPull::Stale | BucketPull::Cold => None,
    });
    // Clearing cold the moment each bucket's own `BucketDone` lands, rather
    // than waiting for the whole group, is goal 1's core requirement: a
    // group that never finishes (this donor stalls or the connection
    // drops) still leaves every bucket it did finish servable.
    // `sundog_rebalance_buckets_total{direction="in"}` is credited right
    // here too, per bucket, the moment its own `BucketDone` lands, rather
    // than batched behind the whole `PullRequest`'s completion: `credited`
    // is shared across every donor this group's `pull_one_group` retries
    // against, not local to this one attempt, so a bucket a prior donor
    // already landed and credited before the group as a whole failed is
    // never credited a second time when a later donor re-sends it.
    let (result, count) =
        state_transfer::pull_buckets_from_donor(shard, donor, async { stream }, |bucket| {
            // A pulled bucket is authoritative: this donor's own data
            // supersedes whatever this node held locally for it.
            // `mark_serving` clears the cold mark on this one decision.
            residency.mark_serving(&[bucket]);
            if credited.insert(bucket) {
                metrics::counter!(
                    "sundog_rebalance_buckets_total",
                    "cache" => cache.to_string(),
                    "direction" => "in"
                )
                .increment(1);
            }
        })
        .await;
    (result, count, cold)
}

/// How many of a group's `total_buckets` still need
/// `sundog_rebalance_buckets_total{direction="in"}` credited once the
/// group's own [`DonorResult::Done`] fires: every bucket in
/// `already_credited` was already counted live, as its own `BucketDone`
/// arrived, so crediting it again at the group level would double-count it.
/// A protocol-3 donor, which never sends per-bucket `BucketDone`, credits
/// every bucket in the group this way instead, still no later than that
/// group's own completion rather than the whole multi-group
/// [`PullRequest`]'s.
fn buckets_pending_group_credit(total_buckets: usize, already_credited: &HashSet<u16>) -> usize {
    total_buckets.saturating_sub(already_credited.len())
}

/// Passes over a group's donors in which every one declined as cold before
/// the pull gives the group up: nobody warm holds the buckets, so what has
/// landed here by other means is all there is.
const ALL_COLD_PASSES: u32 = 3;

/// Delay between retry passes over a group's whole donor list once every
/// candidate has declined or failed once: long enough for both sides'
/// `refresh_task`s to have a real chance at converging their views before
/// trying again.
const GROUP_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Tries `donors` in order for one bucket group, applying the first that
/// donates. A `StaleView` decline is expected while both sides'
/// `refresh_task`s converge after a membership change (see
/// [`crate::store::ShardOps::ae_peer_filter`]), so an all-declined pass
/// retries after [`GROUP_RETRY_BACKOFF`] rather than giving up, unless this
/// node's own view has moved past `view_hash`: then no donor ever agrees,
/// and the pull answers `None` for the caller to replan against the
/// current view. Returns the count of records landed once a donor
/// succeeds.
#[expect(
    clippy::too_many_arguments,
    reason = "each parameter is independent context one donor-group retry loop needs; grouping any subset into a struct would only rename the same eight pieces of state"
)]
async fn pull_one_group(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    ownership: &OwnershipTracker,
    residency: &ResidencySet,
    donors: Vec<NodeId>,
    buckets: Vec<u16>,
    view_hash: u64,
    per_donor: Duration,
) -> Option<u64> {
    let mut all_cold_passes = 0u32;
    // Shared across every donor this group is retried against, within one
    // pass over `donors` and across every retried pass of the outer
    // `loop`: a bucket a prior donor already landed (and credited) before
    // the group as a whole failed or moved to the next donor is never
    // credited again when a later donor re-sends it as part of the same
    // `buckets.clone()` request.
    let mut credited: HashSet<u16> = HashSet::new();
    loop {
        let mut every_donor_cold = !donors.is_empty();
        for &donor in &donors {
            let (result, count, cold) = tokio::time::timeout(
                per_donor,
                try_donor_buckets(
                    shard,
                    mesh,
                    cache,
                    residency,
                    donor,
                    buckets.clone(),
                    view_hash,
                    &mut credited,
                ),
            )
            .await
            .unwrap_or_else(|_| {
                tracing::debug!(%donor, "rebalance bucket pull to donor timed out; trying the next");
                (DonorResult::Failed, 0, false)
            });
            if result == DonorResult::Done {
                residency.mark_serving(&buckets);
                let pending = buckets_pending_group_credit(buckets.len(), &credited);
                if pending > 0 {
                    metrics::counter!(
                        "sundog_rebalance_buckets_total",
                        "cache" => cache.to_string(),
                        "direction" => "in"
                    )
                    .increment(u64::try_from(pending).unwrap_or(u64::MAX));
                }
                tracing::debug!(cache = %cache, %donor, buckets = buckets.len(), records = count, "bucket pull landed");
                return Some(u64::try_from(buckets.len()).unwrap_or(u64::MAX));
            }
            every_donor_cold &= cold;
        }
        if every_donor_cold {
            all_cold_passes += 1;
            if all_cold_passes >= ALL_COLD_PASSES {
                // Every donor is itself waiting on a pull for these buckets:
                // there is no warm copy anywhere to pull. Whatever a
                // hand-off or a forwarded write lands here is the best
                // available answer with no donor left to verify it against:
                // `mark_serving` clears the cold mark.
                tracing::debug!(cache = %cache, buckets = buckets.len(), "every donor is cold for these buckets; nothing warm to pull");
                residency.mark_serving(&buckets);
                return Some(0);
            }
        } else {
            all_cold_passes = 0;
        }
        if ownership.current().view_hash() != view_hash {
            tracing::debug!(cache = %cache, "ownership view moved mid-pull; this pull is superseded");
            return None;
        }
        tokio::time::sleep(GROUP_RETRY_BACKOFF).await;
    }
}

/// One bucket pull's eight fields, gathered so every caller builds and runs
/// one value instead of repeating an eight-argument call.
pub(crate) struct PullRequest<'a> {
    pub(crate) cluster: &'a Cluster,
    pub(crate) shard: &'a Arc<dyn ShardOps>,
    pub(crate) ownership: &'a OwnershipTracker,
    pub(crate) residency: &'a Arc<ResidencySet>,
    pub(crate) cache: &'a SmolStr,
    pub(crate) buckets: Vec<u16>,
    pub(crate) budget: Duration,
    pub(crate) concurrency: usize,
    /// Whether a bucket this pull finds owned alone (no live co-owner in
    /// the current view) may be trusted as genuinely sole-owned and marked
    /// servable outright ([`ResidencySet::mark_serving`]), rather than left
    /// cold for the caller's ordinary warm-up retries. `true` for every
    /// routine call (the ongoing `rebalance_task` loop, `warm_up_task`'s
    /// retries): by the time those run, this node's ownership view is the
    /// library's normal, live-updating one, so "no live co-owner" there
    /// means what it says. `false` only for `Cache::open`'s own initial
    /// pull when its membership wait
    /// (`crate::cache::await_initial_peers`) was needed and timed out:
    /// there, "no live co-owner" is exactly the symptom of the transient
    /// sole-owner view a lone-looking node computes before gossip has
    /// shown it any peer, not genuine single ownership, so trusting it
    /// would serve this node's own local data outright with nobody ever
    /// having vouched for it.
    pub(crate) trust_sole_owner: bool,
}

impl PullRequest<'_> {
    /// Pulls `buckets` from any current owner, grouped by donor set so two
    /// buckets sharing an owner-set-minus-self go in one `StBuckets` round
    /// trip, bounded to `concurrency` simultaneous donor streams. Answers
    /// [`Outcome::Completed`]/[`Outcome::Skipped`]/[`Outcome::NoPeers`] for
    /// an empty bucket set, a zero budget, or no live co-owner anywhere;
    /// otherwise races `budget`, answering [`Outcome::TimedOut`] or
    /// [`Outcome::Superseded`] if it runs out or the view moves on first.
    pub(crate) async fn run(self) -> Outcome {
        let Self {
            cluster,
            shard,
            ownership,
            residency,
            cache,
            buckets,
            budget,
            concurrency,
            trust_sole_owner,
        } = self;
        if buckets.is_empty() {
            return Outcome::Completed;
        }
        if budget.is_zero() {
            tracing::debug!(cache = %cache, "rebalance transfer budget is zero; leaving gained buckets to anti-entropy");
            return Outcome::Skipped;
        }

        let view = ownership.current();
        let view_hash = view.view_hash();
        let (groups, no_donor): (Vec<DonorGroup>, Vec<DonorGroup>) =
            group_buckets_by_donor_set(&view, cluster.node_id(), buckets)
                .into_iter()
                .partition(|(donors, _)| !donors.is_empty());
        // A bucket this node owns alone has nobody to pull from: what is
        // here is all there is, so it is not cold either -- but only when
        // `trust_sole_owner` says this view's "alone" answer is real
        // rather than the transient sole-owner snapshot a membership wait
        // that timed out leaves behind. Left untrusted, `alone` buckets
        // stay exactly as cold as `attach_ownership` marked them, falling
        // to `warm_up_task`'s ordinary retries below via
        // `Outcome::NoPeers`.
        let alone: Vec<u16> = no_donor.into_iter().flat_map(|(_, b)| b).collect();
        if !alone.is_empty() && trust_sole_owner {
            residency.mark_serving(&alone);
        }
        if groups.is_empty() {
            tracing::debug!(cache = %cache, "no live co-owner for any gained bucket; nothing to pull");
            return Outcome::NoPeers;
        }

        let per_donor = state_transfer::per_donor_budget(budget);
        let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
        let mesh = cluster.mesh().clone();

        let run_all = async {
            let mut set = tokio::task::JoinSet::new();
            for (donors, group_buckets) in groups {
                let semaphore = Arc::clone(&semaphore);
                let shard = Arc::clone(shard);
                let mesh = mesh.clone();
                let cache = cache.clone();
                let ownership = ownership.clone();
                let residency = Arc::clone(residency);
                set.spawn(async move {
                    let _permit = semaphore
                        .acquire_owned()
                        .await
                        .expect("invariant: semaphore is never closed");
                    pull_one_group(
                        &shard,
                        &mesh,
                        &cache,
                        &ownership,
                        &residency,
                        donors,
                        group_buckets,
                        view_hash,
                        per_donor,
                    )
                    .await
                });
            }
            // `sundog_rebalance_buckets_total{direction="in"}` is already
            // credited per bucket, inside `pull_one_group`/`try_donor_buckets`,
            // the moment each one's own `BucketDone`/group completion fires;
            // this loop only needs `superseded` for the overall `Outcome`.
            let mut superseded = false;
            while let Some(result) = set.join_next().await {
                if result.ok().flatten().is_none() {
                    superseded = true;
                }
            }
            superseded
        };

        match tokio::time::timeout(budget, run_all).await {
            Ok(superseded) => {
                if superseded {
                    Outcome::Superseded
                } else {
                    Outcome::Completed
                }
            }
            Err(_) => Outcome::TimedOut,
        }
    }
}

/// Retries [`PullRequest::run`] for whatever this node's [`OwnershipTracker`]
/// owns, after an initial `open()`-time pull that
/// [`state_transfer::Outcome::needs_warm_up`] left cold: waits for a
/// co-owner when there is none, then retries every `retry_interval` until a
/// pass lands or repeated timeouts mark the cache warm with whatever
/// landed, counted under `sundog_rebalance_pull_timeouts_total{cache}`.
/// Shares [`state_transfer::next_warm_up_step`]'s decision and
/// [`state_transfer::MAX_WARM_UP_ATTEMPTS`] cap with the whole-cache
/// analogue, [`state_transfer::warm_up_task`].
#[expect(
    clippy::too_many_arguments,
    reason = "a long-running per-cache background task carries this cache's full context for its whole lifetime; a struct would only rename these same eight fields"
)]
pub(crate) async fn warm_up_task(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    ownership: OwnershipTracker,
    residency: Arc<ResidencySet>,
    cache: SmolStr,
    budget: Duration,
    concurrency: usize,
    cancel: CancellationToken,
) {
    let retry_interval = cluster.config().ae_interval;
    let mut attempt: u32 = 0;
    let mut view_rx = ownership.subscribe();
    loop {
        let buckets: Vec<u16> = view_rx.borrow_and_update().owned_buckets().collect();
        let pull = PullRequest {
            cluster: &cluster,
            shard: &shard,
            ownership: &ownership,
            residency: &residency,
            cache: &cache,
            buckets,
            budget,
            concurrency,
            // By the time this retry loop runs, this node's ownership view
            // is the library's normal, live-updating one: "no live
            // co-owner" here means what it says.
            trust_sole_owner: true,
        };
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            outcome = pull.run() => outcome,
        };
        attempt += 1;
        match state_transfer::next_warm_up_step(outcome, attempt) {
            state_transfer::WarmUpStep::Done => {
                cluster.mark_warm(&cache);
                return;
            }
            state_transfer::WarmUpStep::WaitForPeer => {
                tracing::debug!(cache = %cache, "no co-owner for any owned bucket yet; waiting for the ownership view to change");
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    changed = view_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
            state_transfer::WarmUpStep::RetryNow => {}
            state_transfer::WarmUpStep::RetryLater => {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(retry_interval) => {}
                }
            }
            state_transfer::WarmUpStep::WarmAnyway => {
                tracing::warn!(
                    cache = %cache,
                    attempts = attempt,
                    "rebalance pull timed out repeatedly; opening warm with what landed, anti-entropy carries the rest"
                );
                metrics::counter!(
                    "sundog_rebalance_pull_timeouts_total",
                    "cache" => cache.to_string()
                )
                .increment(1);
                // The warm-up attempts ran out with no donor left to try:
                // whatever this node holds locally is the best available
                // answer now, so every still-cold bucket is declared
                // servable.
                residency.mark_all_serving();
                cluster.mark_warm(&cache);
                return;
            }
        }
    }
}

/// What one published view change asks of [`rebalance_task`]: `lost` and
/// `regained` are the difference from the previous view (`prev`); `to_pull`
/// is the difference from the latest view whose pull is not superseded
/// (`pulled`), so a bucket gained under a view that gets superseded
/// mid-pull is pulled again under the current one instead of skipped.
pub(crate) struct ViewChangePlan {
    pub(crate) lost: Vec<u16>,
    pub(crate) regained: Vec<u16>,
    pub(crate) to_pull: Vec<u16>,
}

pub(crate) fn plan_view_change(
    prev: &OwnershipView,
    pulled: &OwnershipView,
    new: &OwnershipView,
) -> ViewChangePlan {
    let (regained, lost) = ownership_diff(prev, new);
    let (to_pull, _) = ownership_diff(pulled, new);
    ViewChangePlan {
        lost,
        regained,
        to_pull,
    }
}

/// The distinct live current owners, other than `self_node`, of the
/// buckets in `due`: the peers a release hands each bucket to first.
pub(crate) fn hand_off_owners(
    view: &OwnershipView,
    self_node: NodeId,
    due: &[u16],
    live: &HashSet<NodeId>,
) -> Vec<NodeId> {
    let mut owners: Vec<NodeId> = Vec::new();
    for &bucket in due {
        for &owner in view.owners_of(bucket) {
            if owner != self_node && live.contains(&owner) && !owners.contains(&owner) {
                owners.push(owner);
            }
        }
    }
    owners
}

/// How long a released bucket may stay resident before it is dropped
/// without a hand-off: the tombstone retention less one interval for the
/// round. A copy held longer can no longer be trusted not to resurrect a
/// key the owners removed and whose tombstone they have since collected;
/// losing what the owners never pulled costs a cache entry, resurrecting a
/// delete breaks the contract that deleted entries never come back.
pub(crate) fn hand_off_cutoff(tombstone_ttl: Duration, ae_interval: Duration) -> Duration {
    tombstone_ttl.saturating_sub(ae_interval).max(ae_interval)
}

/// Drops `buckets` held past [`hand_off_cutoff`] at once, counted under
/// `sundog_rebalance_buckets_total{direction="out"}` like any release.
async fn release_without_hand_off(
    shard: &dyn ShardOps,
    residency: &ResidencySet,
    cache: &SmolStr,
    buckets: &[u16],
) {
    if buckets.is_empty() {
        return;
    }
    let removed = shard.release_buckets(buckets).await;
    residency.unmark(buckets);
    metrics::counter!(
        "sundog_rebalance_buckets_total",
        "cache" => cache.to_string(),
        "direction" => "out"
    )
    .increment(u64::try_from(buckets.len()).unwrap_or(u64::MAX));
    tracing::warn!(cache = %cache, buckets = buckets.len(), removed, "released buckets held past the tombstone retention without a hand-off");
}

/// The buckets in `due` a release may drop now: those whose every other
/// current owner is in `reconciled` (its anti-entropy round completed
/// since the bucket came due, which reconciles every bucket that round's
/// owner co-owns), has acked that *specific* bucket per `acked` (keyed by
/// bucket: an ack for one bucket a co-owner holds is never treated as
/// reconciling any other bucket the same co-owner holds), or, once
/// `overdue`, is in `unreachable` (its round failed, or it has dropped out
/// of the live set). An owner whose view still differs from this node's is
/// none of these, so the bucket stays resident until the views converge.
pub(crate) fn buckets_to_release(
    view: &OwnershipView,
    self_node: NodeId,
    due: &[u16],
    overdue: &[u16],
    reconciled: &HashSet<NodeId>,
    acked: &HashMap<u16, HashSet<NodeId>>,
    unreachable: &HashSet<NodeId>,
) -> Vec<u16> {
    due.iter()
        .copied()
        .filter(|&bucket| {
            view.owners_of(bucket).iter().all(|owner| {
                *owner == self_node
                    || reconciled.contains(owner)
                    || acked
                        .get(&bucket)
                        .is_some_and(|owners| owners.contains(owner))
                    || (overdue.contains(&bucket) && unreachable.contains(owner))
            })
        })
        .collect()
}

/// Which of `hand_off_owners`'s `owners` still need a confirming
/// anti-entropy round: an owner already in `reconciled` (a completed round
/// against it covers every bucket it co-owns) needs none; otherwise an
/// owner is skipped only once `acked`
/// ([`crate::net::Mesh::acked_owners`], bounded by
/// [`crate::config::ClusterConfig::rebalance_ack_window_value`]) covers
/// *every* bucket in `due` that owner co-owns, never on a single acked
/// bucket among several -- an ack for one bucket a co-owner holds must
/// never let another due bucket that same co-owner holds skip its
/// confirming round.
fn owners_needing_confirmation(
    view: &OwnershipView,
    owners: &[NodeId],
    due: &[u16],
    reconciled: &HashSet<NodeId>,
    acked: &HashMap<u16, HashSet<NodeId>>,
) -> Vec<NodeId> {
    owners
        .iter()
        .copied()
        .filter(|owner| {
            if reconciled.contains(owner) {
                return false;
            }
            let fully_acked = due
                .iter()
                .filter(|&&bucket| view.owners_of(bucket).contains(owner))
                .all(|&bucket| {
                    acked
                        .get(&bucket)
                        .is_some_and(|owners| owners.contains(owner))
                });
            !fully_acked
        })
        .collect()
}

/// Reacts to every change in `ownership`'s view for as long as `cancel`
/// stays live: marks newly lost buckets releasing, unmarks newly regained
/// ones, and pulls newly gained ones from their current owners. On a tick
/// piggybacked on `ae_interval`, hands off whichever buckets' disown grace
/// has elapsed to their live current owners ([`hand_off_owners`]), then
/// calls [`ShardOps::release_buckets`] for the buckets every owner answered
/// ([`buckets_to_release`]); an owner that never answers keeps the bucket
/// resident until it is reachable again or its view converges. An owner
/// that acked a specific due bucket via `Msg::StBucketAck`, within
/// [`crate::config::ClusterConfig::rebalance_ack_window_value`] and this
/// node's current view hash of now ([`crate::net::Mesh::acked_owners`]),
/// counts as reconciled for that bucket without a redundant confirming AE
/// round for it -- an ack is scoped to the one bucket it names, never
/// treated as reconciling any other due bucket the same owner co-owns, so
/// [`owners_needing_confirmation`] still runs a round against that owner
/// unless every due bucket it co-owns was acked. `disown_grace` still gates
/// which buckets reach this hand-off at all, so an ack only ever
/// accelerates the confirmation step, never the release timing floor.
#[expect(
    clippy::too_many_arguments,
    reason = "a long-running per-cache background task carries this cache's full context for its whole lifetime; a struct would only rename these same eight fields"
)]
#[expect(
    clippy::too_many_lines,
    reason = "one event loop's whole view-change and disown-grace handling reads best kept together"
)]
pub(crate) async fn rebalance_task(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    ownership: OwnershipTracker,
    residency: Arc<ResidencySet>,
    cache: SmolStr,
    disown_grace: Duration,
    concurrency: usize,
    cancel: CancellationToken,
) {
    let budget = cluster.config().state_transfer_budget;
    let ae_interval = cluster.config().ae_interval;
    let cutoff = hand_off_cutoff(cluster.config().tombstone_ttl, ae_interval);
    let mut view_rx = ownership.subscribe();
    // `prev_view` starts from `ownership.baseline()`, the tracker's
    // original seeded view -- never from a live `view_rx.borrow_and_update()`
    // here, which could already show a `refresh_task` publish that raced
    // ahead of this task's own start. See `OwnershipTracker::baseline`'s
    // own docs for why that race matters: a bucket only the seed view ever
    // called "owned" must still be recognized as lost on the first view
    // change this task observes, not silently excluded from every diff
    // because it was already gone by the time `prev_view` was captured.
    // `pulled_view` is the latest view without a superseded pull. See
    // `plan_view_change` for why the two differ.
    let mut prev_view = ownership.baseline();
    let mut pulled_view = Arc::clone(&prev_view);
    let mut ticker = tokio::time::interval(ae_interval.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            changed = view_rx.changed() => {
                if changed.is_err() {
                    return; // the tracker's sender dropped
                }
                let new_view = view_rx.borrow_and_update().clone();
                let plan = plan_view_change(&prev_view, &pulled_view, &new_view);
                prev_view = Arc::clone(&new_view);
                // A bucket this node now owns alone has nobody left to pull
                // it from: whatever is here is all there is, so it is not
                // cold, and both a miss and a hit in it are answers.
                let alone: Vec<u16> = new_view
                    .owned_buckets()
                    .filter(|&bucket| new_view.owners_of(bucket).len() == 1)
                    .collect();
                if !alone.is_empty() {
                    residency.mark_serving(&alone);
                }
                if !plan.lost.is_empty() {
                    residency.mark_releasing(&plan.lost);
                    tracing::debug!(cache = %cache, count = plan.lost.len(), "buckets lost; disown grace started");
                }
                if !plan.regained.is_empty() {
                    residency.unmark(&plan.regained);
                }
                let outcome = if plan.to_pull.is_empty() {
                    Outcome::Completed
                } else {
                    // Gained from a co-owner: cold until the pull lands.
                    residency.mark_cold(&plan.to_pull);
                    tracing::debug!(cache = %cache, count = plan.to_pull.len(), "buckets gained; pulling from current owners");
                    PullRequest {
                        cluster: &cluster,
                        shard: &shard,
                        ownership: &ownership,
                        residency: &residency,
                        cache: &cache,
                        buckets: plan.to_pull,
                        budget,
                        concurrency,
                        // This is the ongoing rebalance loop, well past
                        // `Cache::open`'s own membership wait: this node's
                        // ownership view is the library's normal,
                        // live-updating one, so "no live co-owner" here
                        // means what it says.
                        trust_sole_owner: true,
                    }
                    .run()
                    .await
                };
                // A pull the view moves past is planned again from the same
                // starting point on the next change, which is already
                // published, so nothing gained is skipped.
                if outcome != Outcome::Superseded {
                    pulled_view = new_view;
                }
            }
            _ = ticker.tick() => {
                let past_cutoff = residency.expired(cutoff);
                release_without_hand_off(shard.as_ref(), &residency, &cache, &past_cutoff).await;
                let due: Vec<u16> = residency
                    .expired(disown_grace)
                    .into_iter()
                    .filter(|bucket| !past_cutoff.contains(bucket))
                    .collect();
                if !due.is_empty() {
                    let view = ownership.current();
                    let self_node = cluster.node_id();
                    let live: HashSet<NodeId> = cluster.peers().iter().map(|peer| peer.node).collect();
                    // An owner that already acked a due bucket, within
                    // `rebalance_ack_window` and this node's current view
                    // hash, counts as reconciled for that specific bucket
                    // without a redundant confirming AE round for it;
                    // `disown_grace` still gates `due` itself, so an ack
                    // only ever accelerates this confirmation step, never
                    // the release timing. Kept separate from `reconciled`
                    // (AE-round-derived, and correctly owner-global since a
                    // round reconciles every bucket that owner co-owns):
                    // an ack names one bucket, never every bucket the
                    // acking owner co-owns.
                    let acked: HashMap<u16, HashSet<NodeId>> = cluster.mesh().acked_owners(
                        &cache,
                        &due,
                        cluster.config().rebalance_ack_window_value(),
                        view.view_hash(),
                    );
                    let mut reconciled: HashSet<NodeId> = HashSet::new();
                    // An owner missing from gossip is unreachable by
                    // definition; the view drops it on its next refresh.
                    let mut unreachable: HashSet<NodeId> = due
                        .iter()
                        .flat_map(|&bucket| view.owners_of(bucket).iter().copied())
                        .filter(|owner| *owner != self_node && !live.contains(owner))
                        .collect();
                    let hand_off = hand_off_owners(&view, self_node, &due, &live);
                    for owner in owners_needing_confirmation(&view, &hand_off, &due, &reconciled, &acked) {
                        let outcome = tokio::select! {
                            biased;
                            () = cancel.cancelled() => return,
                            outcome = anti_entropy::run_round_against(cluster.mesh(), &shard, &cache, owner) => outcome,
                        };
                        match outcome {
                            RoundOutcome::Reconciled => {
                                reconciled.insert(owner);
                            }
                            RoundOutcome::Failed => {
                                unreachable.insert(owner);
                            }
                            RoundOutcome::Stale => {}
                        }
                    }
                    let overdue = residency.expired(disown_grace * 2);
                    let due = buckets_to_release(
                        &view,
                        self_node,
                        &due,
                        &overdue,
                        &reconciled,
                        &acked,
                        &unreachable,
                    );
                    if due.is_empty() {
                        tracing::debug!(cache = %cache, "no released bucket's owners all answered its hand-off; holding until the next tick");
                        continue;
                    }
                    let removed = shard.release_buckets(&due).await;
                    residency.unmark(&due);
                    metrics::counter!(
                        "sundog_rebalance_buckets_total",
                        "cache" => cache.to_string(),
                        "direction" => "out"
                    )
                    .increment(u64::try_from(due.len()).unwrap_or(u64::MAX));
                    tracing::debug!(cache = %cache, buckets = due.len(), removed, "released buckets past their disown grace");
                }
            }
        }
    }
}

// The pure grouping tests need no cluster; the `PullRequest` tests build a
// real `Cluster` with a real `Mesh`, which panics under `sim` outside a
// driven `turmoil::Sim`.
#[cfg(all(test, not(feature = "sim")))]
mod tests {
    use std::num::NonZeroU8;

    use super::*;
    use crate::node::NodeId;

    fn view(self_node: NodeId, eligible: Vec<NodeId>, k: u8) -> OwnershipView {
        OwnershipView::compute(self_node, eligible, NonZeroU8::new(k).expect("nonzero"))
    }

    #[test]
    fn group_buckets_by_donor_set_groups_buckets_sharing_the_same_donors() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let buckets: Vec<u16> = (0..64).collect();

        let groups = group_buckets_by_donor_set(&view, self_node, buckets.clone());

        // Every bucket appears in exactly one group, and each group's donor
        // set is precisely that bucket's owners minus self.
        let mut regrouped: Vec<u16> = groups.iter().flat_map(|(_, bs)| bs.clone()).collect();
        regrouped.sort_unstable();
        assert_eq!(regrouped, buckets);
        for (donors, group_buckets) in &groups {
            for &bucket in group_buckets {
                let expected: Vec<NodeId> = view
                    .owners_of(bucket)
                    .iter()
                    .copied()
                    .filter(|&n| n != self_node)
                    .collect();
                assert_eq!(donors, &expected);
            }
        }
    }

    #[test]
    fn group_buckets_by_donor_set_excludes_self_from_every_donor_list() {
        let self_node = NodeId::from(1);
        // Solo eligible set: self is every bucket's only owner, so every
        // donor list is empty once self is excluded.
        let view = view(self_node, vec![self_node], 2);
        let groups = group_buckets_by_donor_set(&view, self_node, vec![0, 1, 2]);

        assert_eq!(groups.len(), 1);
        assert!(groups[0].0.is_empty());
    }

    #[test]
    fn buckets_pending_group_credit_excludes_buckets_already_credited_live() {
        let mut already_credited = HashSet::new();
        already_credited.insert(3u16);
        already_credited.insert(7u16);
        assert_eq!(
            buckets_pending_group_credit(5, &already_credited),
            3,
            "5 buckets minus the 2 already credited live leaves 3 for the group-level fallback"
        );
    }

    #[test]
    fn buckets_pending_group_credit_is_the_full_group_when_nothing_landed_live() {
        assert_eq!(
            buckets_pending_group_credit(4, &HashSet::new()),
            4,
            "a protocol-3 donor never fires a live BucketDone, so every bucket in the group is \
             credited at group completion instead"
        );
    }

    #[test]
    fn plan_view_change_marks_a_bucket_lost_since_the_previous_view_even_when_its_pull_was_superseded()
     {
        let self_node = NodeId::from(1);
        let k = 2;
        // v0: three nodes. v1: node 3 died, so self gained its share. v2:
        // node 4 joined, taking some of those newly gained buckets away.
        let v0 = view(self_node, (1..=3u64).map(NodeId::from).collect(), k);
        let v1 = view(self_node, (1..=2u64).map(NodeId::from).collect(), k);
        let v2 = view(self_node, [1u64, 2, 4].map(NodeId::from).to_vec(), k);
        let owned = |v: &OwnershipView| v.owned_buckets().collect::<HashSet<u16>>();
        let (o0, o1, o2) = (owned(&v0), owned(&v1), owned(&v2));

        // v0 -> v1 pulled cleanly: prev and pulled agree.
        let plan = plan_view_change(&v0, &v0, &v1);
        assert_eq!(
            plan.to_pull.iter().copied().collect::<HashSet<_>>(),
            plan.regained.iter().copied().collect::<HashSet<_>>()
        );
        for &b in &plan.lost {
            assert!(o0.contains(&b) && !o1.contains(&b));
        }

        // v1's pull gets superseded by v2: lost is measured from v1, so a
        // bucket gained under v1 and gone in v2 starts its grace, while
        // the pull covers everything v2 owns that v0 did not.
        let plan = plan_view_change(&v1, &v0, &v2);
        let gained_then_lost: Vec<u16> = o1
            .iter()
            .copied()
            .filter(|b| !o0.contains(b) && !o2.contains(b))
            .collect();
        assert!(
            !gained_then_lost.is_empty(),
            "the fixture has a bucket gained under v1 and lost in v2"
        );
        for b in &gained_then_lost {
            assert!(plan.lost.contains(b), "bucket {b} starts its disown grace");
        }
        for &b in &plan.to_pull {
            assert!(o2.contains(&b) && !o0.contains(&b));
        }
        for b in o2.iter().filter(|b| !o0.contains(b)) {
            assert!(plan.to_pull.contains(b), "bucket {b} is pulled under v2");
        }
    }

    #[tokio::test]
    async fn rebalance_task_schedules_release_for_a_bucket_only_the_baseline_view_ever_owned() {
        let cluster = solo_cluster("rebalance-unit-test-baseline-release").await;
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        // The seeded, lone-node view: exactly the transient "this node owns
        // every bucket" state a `Mode::Distributed` `open()` racing gossip
        // convergence installs.
        let baseline = tracker.baseline();
        assert_eq!(baseline.owned_buckets().count(), crate::store::BUCKET_COUNT);

        // Published *before* `rebalance_task` is spawned: two more real
        // nodes joining (three eligible in total, over `k=2`, so ownership
        // actually shrinks rather than every node still owning every
        // bucket) corrects ownership down to roughly two thirds, exactly as
        // if `refresh_task` had already raced ahead and republished before
        // this task ever read the tracker's channel -- the very race
        // `OwnershipTracker::baseline` exists to make irrelevant. By the
        // time `rebalance_task`'s `ownership.subscribe()` runs below, this
        // is already the live value, not a pending "change" the fresh
        // receiver would ever observe.
        let other_a = NodeId::from(u64::MAX);
        let other_b = NodeId::from(u64::MAX - 1);
        let corrected = Arc::new(view(
            cluster.node_id(),
            vec![cluster.node_id(), other_a, other_b],
            2,
        ));
        let bucket_only_in_baseline = baseline
            .owned_buckets()
            .find(|&b| !corrected.owns(b))
            .expect("a lone-node view owns strictly more than a three-node, k=2 split");
        tx.send(Arc::clone(&corrected))
            .expect("the tracker's own receiver keeps the channel open");

        let residency = Arc::new(ResidencySet::new());
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(rebalance_task(
            cluster.clone(),
            empty_shard(),
            tracker,
            Arc::clone(&residency),
            name.clone(),
            Duration::from_secs(3600),
            4,
            cancel.clone(),
        ));

        // Gives `rebalance_task` time to subscribe (capturing `corrected`
        // as already-seen) and start awaiting the next view change before
        // this test publishes one.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // A raw `send` of the same ownership set again: a genuine new
        // version in the watch channel, so `rebalance_task`'s
        // `view_rx.changed()` fires, without this test needing a third,
        // distinct real membership change to prove the point. What is
        // under test is `rebalance_task`'s own starting point for the
        // diff, not this second transition's content.
        tx.send(Arc::clone(&corrected))
            .expect("the tracker's own receiver keeps the channel open");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            residency.is_releasing(bucket_only_in_baseline),
            "a bucket only the tracker's seeded baseline view ever owned, already gone by the \
             very first published correction, must still start its disown grace: this is \
             `rebalance_task`'s normal lost-bucket path (`plan_view_change`) reacting off \
             `OwnershipTracker::baseline`, not a live re-borrow of the channel a race could \
             already have moved past"
        );

        cancel.cancel();
        cluster.shutdown().await;
    }

    #[test]
    fn hand_off_owners_lists_each_live_current_owner_once_and_never_self() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible.clone(), 2);
        let due: Vec<u16> = (0..256).collect();
        let live: HashSet<NodeId> = eligible
            .iter()
            .copied()
            .filter(|n| *n != self_node)
            .collect();

        let owners = hand_off_owners(&view, self_node, &due, &live);
        let distinct: HashSet<NodeId> = owners.iter().copied().collect();
        assert_eq!(owners.len(), distinct.len(), "each owner listed once");
        assert!(
            !owners.contains(&self_node),
            "self is never a hand-off target"
        );
        for owner in &owners {
            assert!(
                due.iter().any(|&b| view.owners_of(b).contains(owner)),
                "every listed owner owns a due bucket: {owner:?}"
            );
        }
        for &bucket in &due {
            for owner in view.owners_of(bucket) {
                if *owner != self_node {
                    assert!(
                        owners.contains(owner),
                        "every other owner of a due bucket is listed"
                    );
                }
            }
        }

        let dead = eligible[1];
        let mut live_minus_one = live.clone();
        live_minus_one.remove(&dead);
        assert!(
            !hand_off_owners(&view, self_node, &due, &live_minus_one).contains(&dead),
            "a dead owner is skipped"
        );
    }

    #[test]
    fn hand_off_cutoff_is_one_interval_inside_the_tombstone_retention() {
        assert_eq!(
            hand_off_cutoff(Duration::from_secs(60), Duration::from_secs(3)),
            Duration::from_secs(57)
        );
        assert_eq!(
            hand_off_cutoff(Duration::from_secs(2), Duration::from_secs(3)),
            Duration::from_secs(3),
            "never shorter than one interval"
        );
    }

    #[test]
    fn buckets_to_release_holds_a_bucket_until_every_owner_answered_or_it_is_overdue() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<u16> = (0..256).collect();

        let none: HashSet<NodeId> = HashSet::new();
        let no_acked: HashMap<u16, HashSet<NodeId>> = HashMap::new();
        let held = buckets_to_release(&view, self_node, &due, &[], &none, &no_acked, &none);
        for &bucket in &held {
            assert!(
                view.owners_of(bucket).iter().all(|o| *o == self_node),
                "with no owner answered, only a bucket owned by self alone is released: {bucket}"
            );
        }

        let answered: HashSet<NodeId> = [NodeId::from(2)].into_iter().collect();
        let released = buckets_to_release(&view, self_node, &due, &[], &answered, &no_acked, &none);
        for &bucket in &due {
            let all_answered = view
                .owners_of(bucket)
                .iter()
                .all(|o| *o == self_node || answered.contains(o));
            assert_eq!(
                released.contains(&bucket),
                all_answered,
                "bucket {bucket} is released exactly when every other owner answered"
            );
        }
        assert!(
            released.len() < due.len(),
            "some bucket waits on an owner that did not answer"
        );

        // Overdue: a stale owner still holds the bucket, an unreachable
        // one does not.
        let overdue: Vec<u16> = due.clone();
        let still_held =
            buckets_to_release(&view, self_node, &due, &overdue, &none, &no_acked, &none);
        assert_eq!(
            still_held, held,
            "an overdue bucket whose owners are merely stale stays resident"
        );
        let everyone: HashSet<NodeId> = (2..=5u64).map(NodeId::from).collect();
        let dropped = buckets_to_release(
            &view, self_node, &due, &overdue, &none, &no_acked, &everyone,
        );
        assert_eq!(
            dropped, due,
            "an overdue bucket whose owners are all unreachable is released"
        );
        let mixed = buckets_to_release(
            &view, self_node, &due, &overdue, &answered, &no_acked, &everyone,
        );
        assert_eq!(
            mixed, due,
            "reconciled and unreachable owners together release"
        );
    }

    #[test]
    fn buckets_to_release_accepts_an_acked_owner_without_a_confirming_ae_round() {
        // An owner `acked` names for this specific bucket directly releases
        // it exactly as one whose confirming AE round completed
        // (`reconciled`) would have. A bucket still needs every one of its
        // other current owners covered first: an ack accelerates
        // confirmation, it does not relax the release requirement itself.
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<u16> = (0..256).collect();
        let bucket = due
            .iter()
            .copied()
            .find(|&b| view.owners_of(b).contains(&self_node) && view.owners_of(b).len() == 2)
            .expect("some bucket is co-owned by exactly one other node at k=2");
        let other = *view
            .owners_of(bucket)
            .iter()
            .find(|&&o| o != self_node)
            .expect("a co-owned bucket has another owner");

        let none: HashSet<NodeId> = HashSet::new();
        let no_acked: HashMap<u16, HashSet<NodeId>> = HashMap::new();
        assert!(
            !buckets_to_release(&view, self_node, &[bucket], &[], &none, &no_acked, &none)
                .contains(&bucket),
            "with no ack and no round, the bucket stays resident"
        );

        let acked: HashMap<u16, HashSet<NodeId>> =
            HashMap::from([(bucket, HashSet::from([other]))]);
        assert!(
            buckets_to_release(&view, self_node, &[bucket], &[], &none, &acked, &none)
                .contains(&bucket),
            "an owner who acked this bucket releases it without a confirming AE round against it"
        );
    }

    #[test]
    fn buckets_to_release_never_releases_a_co_owned_bucket_whose_owner_only_acked_a_different_bucket()
     {
        // Regression for the bucket-blind version of this check: two due
        // buckets share the same other owner, who acked only one of them.
        // The un-acked bucket must stay resident -- an ack is scoped to the
        // bucket it names, never folded into a flat per-owner set that
        // would let it stand in for every other due bucket that owner also
        // co-owns.
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<u16> = (0..256)
            .filter(|&b| view.owners_of(b).contains(&self_node) && view.owners_of(b).len() == 2)
            .collect();
        let other = *view
            .owners_of(due[0])
            .iter()
            .find(|&&o| o != self_node)
            .expect("a co-owned bucket has another owner");
        // Two distinct due buckets `other` co-owns alongside self.
        let owner_due: Vec<u16> = due
            .iter()
            .copied()
            .filter(|&b| view.owners_of(b).contains(&other))
            .take(2)
            .collect();
        assert_eq!(
            owner_due.len(),
            2,
            "`other` co-owns at least two due buckets at this eligible set/k"
        );
        let (bucket_acked, bucket_unacked) = (owner_due[0], owner_due[1]);

        let none: HashSet<NodeId> = HashSet::new();
        // `other` acked `bucket_acked` only.
        let acked: HashMap<u16, HashSet<NodeId>> =
            HashMap::from([(bucket_acked, HashSet::from([other]))]);
        let released = buckets_to_release(
            &view,
            self_node,
            &[bucket_acked, bucket_unacked],
            &[],
            &none,
            &acked,
            &none,
        );
        assert!(
            released.contains(&bucket_acked),
            "the bucket `other` actually acked releases"
        );
        assert!(
            !released.contains(&bucket_unacked),
            "a different due bucket the same owner co-owns, which it never acked, must not \
             release on the strength of an ack for a different bucket"
        );
    }

    #[test]
    fn owners_needing_confirmation_skips_an_already_reconciled_owner() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let owners = vec![NodeId::from(2), NodeId::from(3)];
        // The full bucket range, not an empty list: `owners_needing_confirmation`
        // is only ever called (in `rebalance_task`) with owners
        // `hand_off_owners` derived from `due`, so every passed-in owner
        // co-owns at least one bucket actually in `due`; an owner with none
        // vacuously has "acked every due bucket it co-owns" and would
        // wrongly appear reconciled, a case that cannot arise in practice.
        let due: Vec<u16> = (0..256).collect();
        let reconciled: HashSet<NodeId> = [NodeId::from(2)].into_iter().collect();
        let no_acked: HashMap<u16, HashSet<NodeId>> = HashMap::new();
        assert_eq!(
            owners_needing_confirmation(&view, &owners, &due, &reconciled, &no_acked),
            vec![NodeId::from(3)],
            "an owner already reconciled, e.g. via a round covering every bucket it co-owns, \
             needs no round"
        );
        assert_eq!(
            owners_needing_confirmation(&view, &owners, &due, &HashSet::new(), &no_acked),
            owners,
            "with nobody reconciled yet and no acks, every owner still needs a round"
        );
    }

    #[test]
    fn owners_needing_confirmation_still_runs_a_round_for_an_owner_who_only_partly_acked() {
        // The same partial-ack scenario as
        // `buckets_to_release_never_releases_a_co_owned_bucket_whose_owner_only_acked_a_different_bucket`,
        // from the confirming-round side: an owner who acked only one of
        // several due buckets it co-owns must still get a confirming round,
        // since that round is what reconciles the buckets it did not ack.
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<u16> = (0..256)
            .filter(|&b| view.owners_of(b).contains(&self_node) && view.owners_of(b).len() == 2)
            .collect();
        let other = *view
            .owners_of(due[0])
            .iter()
            .find(|&&o| o != self_node)
            .expect("a co-owned bucket has another owner");
        let owner_due: Vec<u16> = due
            .iter()
            .copied()
            .filter(|&b| view.owners_of(b).contains(&other))
            .collect();
        assert!(
            owner_due.len() >= 2,
            "`other` co-owns at least two due buckets at this eligible set/k"
        );

        let reconciled: HashSet<NodeId> = HashSet::new();
        // Acked only the first of the buckets `other` co-owns.
        let acked: HashMap<u16, HashSet<NodeId>> =
            HashMap::from([(owner_due[0], HashSet::from([other]))]);
        assert_eq!(
            owners_needing_confirmation(&view, &[other], &due, &reconciled, &acked),
            vec![other],
            "a partial ack never lets an owner skip its confirming round"
        );

        // Acking every due bucket `other` co-owns does let it skip.
        let full_acked: HashMap<u16, HashSet<NodeId>> = owner_due
            .iter()
            .map(|&b| (b, HashSet::from([other])))
            .collect();
        assert!(
            owners_needing_confirmation(&view, &[other], &due, &reconciled, &full_acked).is_empty(),
            "an owner who acked every due bucket it co-owns needs no confirming round"
        );
    }

    fn empty_shard() -> Arc<dyn ShardOps> {
        Arc::new(crate::store::Shard::<u32, u32>::new(
            SmolStr::new("prices"),
            crate::store::Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            },
            crate::node::NodeId::random(),
            1024,
            None,
            None,
        )) as Arc<dyn ShardOps>
    }

    async fn solo_cluster(name: &str) -> crate::cluster::Cluster {
        crate::cluster::Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(crate::config::ClusterConfig {
                gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                ..crate::config::ClusterConfig::default()
            })
            .build()
            .await
            .expect("solo cluster builds")
    }

    #[tokio::test]
    async fn pull_buckets_completes_trivially_with_no_buckets_to_pull() {
        let cluster = solo_cluster("rebalance-unit-test-empty").await;
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );

        let outcome = PullRequest {
            cluster: &cluster,
            shard: &empty_shard(),
            ownership: &tracker,
            residency: &Arc::new(ResidencySet::new()),
            cache: &name,
            buckets: Vec::new(),
            budget: Duration::from_secs(1),
            concurrency: 4,
            trust_sole_owner: true,
        }
        .run()
        .await;
        assert_eq!(outcome, Outcome::Completed);

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn pull_buckets_reports_no_peers_when_every_bucket_has_no_other_owner() {
        let cluster = solo_cluster("rebalance-unit-test-solo").await;
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );

        let outcome = PullRequest {
            cluster: &cluster,
            shard: &empty_shard(),
            ownership: &tracker,
            residency: &Arc::new(ResidencySet::new()),
            cache: &name,
            buckets: vec![0, 1, 2],
            budget: Duration::from_secs(1),
            concurrency: 4,
            trust_sole_owner: true,
        }
        .run()
        .await;
        assert_eq!(outcome, Outcome::NoPeers);

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn warm_up_task_waits_for_a_co_owner_and_warms_once_its_pulls_give_up() {
        let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-warm-up")
            .seeds(std::iter::empty())
            .config(crate::config::ClusterConfig {
                gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                ae_interval: Duration::from_millis(50),
                ..crate::config::ClusterConfig::default()
            })
            .build()
            .await
            .expect("solo cluster builds");
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(warm_up_task(
            cluster.clone(),
            empty_shard(),
            tracker,
            Arc::new(ResidencySet::new()),
            name.clone(),
            Duration::from_millis(150),
            4,
            cancel.clone(),
        ));

        // Self-only view: no co-owner anywhere, so the task waits on the
        // view instead of spinning or marking the cache warm.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!cluster.is_warm(&name), "no co-owner in sight: still cold");
        assert!(!task.is_finished(), "the task waits for a view change");

        // A view with an unreachable co-owner: every pull times out, and
        // after the attempt cap the cache opens warm with what landed.
        let phantom = NodeId::from(u64::MAX);
        tx.send(Arc::new(view(
            cluster.node_id(),
            vec![cluster.node_id(), phantom],
            2,
        )))
        .expect("the task still holds its receiver");
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the task ends within the bound")
            .expect("the task does not panic");
        assert!(cluster.is_warm(&name), "warm after the pulls gave up");

        cancel.cancel();
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn pull_request_marks_a_bucket_servable_when_owned_alone() {
        let cluster = solo_cluster("rebalance-unit-test-alone").await;
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );

        // Self-only view: every bucket this node owns has no co-owner at
        // all.
        let bucket = tracker
            .current()
            .owned_buckets()
            .next()
            .expect("self owns at least one bucket alone");
        let residency = Arc::new(ResidencySet::new());
        // Cold, like any freshly gained bucket.
        residency.mark_cold(&[bucket]);

        let outcome = PullRequest {
            cluster: &cluster,
            shard: &empty_shard(),
            ownership: &tracker,
            residency: &residency,
            cache: &name,
            buckets: vec![bucket],
            budget: Duration::from_secs(1),
            concurrency: 4,
            trust_sole_owner: true,
        }
        .run()
        .await;

        assert_eq!(
            outcome,
            Outcome::NoPeers,
            "no co-owner anywhere for this bucket"
        );
        assert!(
            !residency.is_cold(bucket),
            "a sole-owned bucket is not cold: what is here is all there is"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn pull_request_leaves_a_bucket_cold_when_owned_alone_is_untrusted() {
        // The same self-only, owned-alone fixture as
        // `pull_request_marks_a_bucket_servable_when_owned_alone` above,
        // but with `trust_sole_owner: false`: the shape `Cache::open`'s
        // initial pull is in whenever its own membership wait was needed
        // and timed out, so "owned alone" here is exactly the transient
        // sole-owner view a lone-looking node computes before gossip has
        // shown it any peer, not genuine single ownership.
        let cluster = solo_cluster("rebalance-unit-test-alone-untrusted").await;
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );

        let bucket = tracker
            .current()
            .owned_buckets()
            .next()
            .expect("self owns at least one bucket alone");
        let residency = Arc::new(ResidencySet::new());
        residency.mark_cold(&[bucket]);

        let outcome = PullRequest {
            cluster: &cluster,
            shard: &empty_shard(),
            ownership: &tracker,
            residency: &residency,
            cache: &name,
            buckets: vec![bucket],
            budget: Duration::from_secs(1),
            concurrency: 4,
            trust_sole_owner: false,
        }
        .run()
        .await;

        assert_eq!(
            outcome,
            Outcome::NoPeers,
            "still no co-owner anywhere for this bucket in the current view"
        );
        assert!(
            residency.is_cold(bucket),
            "untrusted, this bucket stays cold: `Cache::open`'s own ordinary warm-up retries \
             get the final say once membership actually settles, instead of this pull trusting \
             a transient sole-owner view outright"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn warm_up_task_marks_a_bucket_servable_once_its_pulls_give_up() {
        let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-warm-up-give-up")
            .seeds(std::iter::empty())
            .config(crate::config::ClusterConfig {
                gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
                ae_interval: Duration::from_millis(50),
                ..crate::config::ClusterConfig::default()
            })
            .build()
            .await
            .expect("solo cluster builds");
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        // The phantom co-owner is already in the very first view this
        // task's loop reads, unlike
        // `warm_up_task_waits_for_a_co_owner_and_warms_once_its_pulls_give_up`'s
        // self-only start: every owned bucket is genuinely co-owned from
        // attempt 1, so it is cold going in rather than cleared outright
        // by the "owned alone" case on the first pass.
        let phantom = NodeId::from(u64::MAX);
        let phantom_view = Arc::new(view(cluster.node_id(), vec![cluster.node_id(), phantom], 2));
        tx.send(Arc::clone(&phantom_view))
            .expect("receiver still alive");

        let bucket = phantom_view
            .owned_buckets()
            .next()
            .expect("self owns at least one bucket");
        let residency = Arc::new(ResidencySet::new());
        residency.mark_cold(&[bucket]);

        let cancel = CancellationToken::new();
        let task = tokio::spawn(warm_up_task(
            cluster.clone(),
            empty_shard(),
            tracker,
            Arc::clone(&residency),
            name.clone(),
            Duration::from_millis(150),
            4,
            cancel.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the task ends within the bound")
            .expect("the task does not panic");

        assert!(cluster.is_warm(&name), "warm after the pulls gave up");
        assert!(
            !residency.is_cold(bucket),
            "the give-up path clears cold for a bucket whose only co-owner never answered"
        );

        cancel.cancel();
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn pull_buckets_skips_with_a_zero_budget() {
        let cluster = solo_cluster("rebalance-unit-test-zero-budget").await;
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );

        let outcome = PullRequest {
            cluster: &cluster,
            shard: &empty_shard(),
            ownership: &tracker,
            residency: &Arc::new(ResidencySet::new()),
            cache: &name,
            buckets: vec![0],
            budget: Duration::ZERO,
            concurrency: 4,
            trust_sole_owner: true,
        }
        .run()
        .await;
        assert_eq!(outcome, Outcome::Skipped);

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn pull_one_group_clears_cold_per_bucket_as_each_done_arrives_before_the_group_finishes()
    {
        use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

        let cache = SmolStr::new("prices");
        let view_hash = 42;
        let donor_node = NodeId::from(101);
        let requester_node = NodeId::from(102);

        // Bucket 0 completes; bucket 1's own chunk lands but the donor then
        // stalls forever instead of ever signaling its done -- a stand-in
        // for a connection that drops mid-group, since either way the
        // requester never observes bucket 1's `BucketDone`.
        let donor_handler = Arc::new(BucketPullHandler {
            view_hash,
            chunks: vec![
                (0u16, vec![sample_wire_record(1)]),
                (1u16, vec![sample_wire_record(2)]),
            ],
            stall_after: true,
        });
        let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
        let requester_handler = Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
        });
        let (requester, _requester_inbound) = spawn_mesh(requester_node, requester_handler).await;
        requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

        let residency = Arc::new(ResidencySet::new());
        residency.mark_cold(&[0, 1]);
        let shard = empty_shard();
        let modes: crate::membership::CacheModes = std::collections::HashMap::new();
        let k = NonZeroU8::new(2).expect("nonzero");
        let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

        let outcome = tokio::time::timeout(
            Duration::from_millis(500),
            pull_one_group(
                &shard,
                &requester,
                &cache,
                &ownership,
                &residency,
                vec![donor_node],
                vec![0, 1],
                view_hash,
                Duration::from_secs(5),
            ),
        )
        .await;
        assert!(
            outcome.is_err(),
            "the donor stalls after bucket 1's chunk, so the group never finishes within the \
             test's patience"
        );
        assert!(
            !residency.is_cold(0),
            "bucket 0's BucketDone landed and cleared its cold mark before the group finished"
        );
        assert!(
            residency.is_cold(1),
            "bucket 1's BucketDone never arrived, so it stays cold"
        );

        donor.shutdown().await;
        requester.shutdown().await;
    }

    /// `try_donor_buckets`'s `credited` parameter is shared across every
    /// donor `pull_one_group` retries a group against, precisely so a
    /// bucket the first donor already landed (and credited) before the
    /// group as a whole moved to a second donor is never credited again
    /// when that second donor re-sends it. The first donor's stream, the
    /// same stall shape
    /// `pull_one_group_clears_cold_per_bucket_as_each_done_arrives_before_the_group_finishes`
    /// uses, lands bucket 0's `BucketDone` (flushed once bucket 1's chunk
    /// proves bucket 0 is behind it) and then stalls before bucket 1's own
    /// `BucketDone` ever arrives -- a stand-in for the connection dropping
    /// mid-group, exactly the case `pull_one_group`'s per-donor
    /// `tokio::time::timeout` moves on from. The second donor then answers
    /// the same still-cold group's request with both buckets and completes
    /// it. Threading one `credited` set across both calls, exactly as
    /// `pull_one_group`'s own loop does, must end with bucket 0 counted
    /// once, not twice, guarding the regression where
    /// `sundog_rebalance_buckets_total{direction="in"}` double-counted a
    /// bucket on a donor retry.
    #[tokio::test]
    async fn try_donor_buckets_shares_credited_buckets_across_repeated_calls_for_one_group() {
        use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

        let cache = SmolStr::new("prices");
        let view_hash = 42;
        let first_donor_node = NodeId::from(201);
        let second_donor_node = NodeId::from(202);
        let requester_node = NodeId::from(203);

        let first_donor_handler = Arc::new(BucketPullHandler {
            view_hash,
            chunks: vec![
                (0u16, vec![sample_wire_record(1)]),
                (1u16, vec![sample_wire_record(2)]),
            ],
            stall_after: true,
        });
        let (first_donor, _first_donor_inbound) =
            spawn_mesh(first_donor_node, first_donor_handler).await;
        let second_donor_handler = Arc::new(BucketPullHandler {
            view_hash,
            chunks: vec![
                (0u16, vec![sample_wire_record(1)]),
                (1u16, vec![sample_wire_record(2)]),
            ],
            stall_after: false,
        });
        let (second_donor, _second_donor_inbound) =
            spawn_mesh(second_donor_node, second_donor_handler).await;
        let requester_handler = Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
        });
        let (requester, _requester_inbound) = spawn_mesh(requester_node, requester_handler).await;
        requester.update_peers(vec![
            peer_at(first_donor_node, first_donor.local_addr()),
            peer_at(second_donor_node, second_donor.local_addr()),
        ]);

        let residency = ResidencySet::new();
        residency.mark_cold(&[0, 1]);
        let shard = empty_shard();
        let mut credited: HashSet<u16> = HashSet::new();

        // Mirrors `pull_one_group`'s own per-donor timeout: the first
        // donor's stream stalls after bucket 1's chunk, so this never
        // resolves on its own within the test's patience.
        let first_call = tokio::time::timeout(
            Duration::from_millis(200),
            try_donor_buckets(
                &shard,
                &requester,
                &cache,
                &residency,
                first_donor_node,
                vec![0, 1],
                view_hash,
                &mut credited,
            ),
        )
        .await;
        assert!(
            first_call.is_err(),
            "the first donor stalls after bucket 1's chunk, so its own call never resolves \
             within the timeout, mirroring a dropped connection mid-group"
        );
        assert_eq!(
            credited,
            HashSet::from([0]),
            "bucket 0's BucketDone landed (flushed once bucket 1's chunk proved bucket 0 is \
             behind it) and was credited before the stall; bucket 1's own BucketDone never \
             arrived"
        );

        let (result_second, count_second, cold_second) = try_donor_buckets(
            &shard,
            &requester,
            &cache,
            &residency,
            second_donor_node,
            vec![0, 1],
            view_hash,
            &mut credited,
        )
        .await;
        assert_eq!(
            result_second,
            DonorResult::Done,
            "the second donor completes the whole group"
        );
        assert_eq!(
            count_second, 2,
            "the second donor redelivers bucket 0 and lands bucket 1"
        );
        assert!(!cold_second);
        assert_eq!(
            credited,
            HashSet::from([0, 1]),
            "bucket 0 stays credited exactly once even though the second donor redelivered its \
             BucketDone; only bucket 1 is newly credited"
        );

        first_donor.shutdown().await;
        second_donor.shutdown().await;
        requester.shutdown().await;
    }

    fn sample_wire_record(n: u8) -> crate::wire::WireRecord {
        crate::wire::WireRecord {
            key: bytes::Bytes::from(vec![n]),
            value: Some(bytes::Bytes::from(vec![n, n])),
            ver: crate::hlc::Hlc {
                wall_ms: u64::from(n),
                logical: 0,
                node: NodeId::from(1),
            },
            expires_at_ms: None,
        }
    }
}
