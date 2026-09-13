//! Rebalance for a `Mode::Distributed` cache: pulls a bucket from its
//! current owners the moment this node's [`OwnershipView`] says it gained
//! it, and releases a bucket's local data once this node has kept it
//! resident past the disown grace after losing it. The open()-time initial
//! pull and this module's ongoing loop share one mechanism, [`PullRequest`],
//! scoped to whichever bucket set is at hand.

use std::collections::HashSet;
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
async fn try_donor_buckets(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    donor: NodeId,
    buckets: Vec<u16>,
    view_hash: u64,
) -> (DonorResult, u64, bool) {
    let pull = mesh
        .request_buckets(donor, cache.clone(), buckets, view_hash)
        .await;
    let cold = matches!(pull, Ok(BucketPull::Cold));
    let stream = pull.map(|answer| match answer {
        BucketPull::Stream(stream) => Some(stream),
        BucketPull::Stale | BucketPull::Cold => None,
    });
    let (result, count) = state_transfer::pull_from_donor(shard, donor, async { stream }).await;
    (result, count, cold)
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
    loop {
        let mut every_donor_cold = !donors.is_empty();
        for &donor in &donors {
            let (result, count, cold) = tokio::time::timeout(
                per_donor,
                try_donor_buckets(shard, mesh, cache, donor, buckets.clone(), view_hash),
            )
            .await
            .unwrap_or_else(|_| {
                tracing::debug!(%donor, "rebalance bucket pull to donor timed out; trying the next");
                (DonorResult::Failed, 0, false)
            });
            if result == DonorResult::Done {
                residency.clear_cold(&buckets);
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
                // hand-off or a forwarded write lands here is all there is.
                tracing::debug!(cache = %cache, buckets = buckets.len(), "every donor is cold for these buckets; nothing warm to pull");
                residency.clear_cold(&buckets);
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
        // A bucket this node owns alone has nobody to pull from: what is here
        // is all there is, so it is not cold either.
        let alone: Vec<u16> = no_donor.into_iter().flat_map(|(_, b)| b).collect();
        if !alone.is_empty() {
            residency.clear_cold(&alone);
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
            let mut landed = 0u64;
            let mut superseded = false;
            while let Some(result) = set.join_next().await {
                match result.ok().flatten() {
                    Some(count) => landed += count,
                    None => superseded = true,
                }
            }
            (landed, superseded)
        };

        match tokio::time::timeout(budget, run_all).await {
            Ok((landed, superseded)) => {
                // `landed` counts buckets whose pull landed, not records.
                if landed > 0 {
                    metrics::counter!(
                        "sundog_rebalance_buckets_total",
                        "cache" => cache.to_string(),
                        "direction" => "in"
                    )
                    .increment(landed);
                }
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
/// landed. Shares [`state_transfer::next_warm_up_step`]'s decision and
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
                residency.clear_all_cold();
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

/// The buckets in `due` a release may drop now: those whose every other
/// current owner is in `reconciled` (its anti-entropy round completed
/// since the bucket came due) or, once `overdue`, in `unreachable` (its
/// round failed, or it has dropped out of the live set). An owner whose view still
/// differs from this node's is neither, so the bucket stays resident until
/// the views converge.
pub(crate) fn buckets_to_release(
    view: &OwnershipView,
    self_node: NodeId,
    due: &[u16],
    overdue: &[u16],
    reconciled: &HashSet<NodeId>,
    unreachable: &HashSet<NodeId>,
) -> Vec<u16> {
    due.iter()
        .copied()
        .filter(|&bucket| {
            view.owners_of(bucket).iter().all(|owner| {
                *owner == self_node
                    || reconciled.contains(owner)
                    || (overdue.contains(&bucket) && unreachable.contains(owner))
            })
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
/// resident until it is reachable again or its view converges.
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
    let mut view_rx = ownership.subscribe();
    // `prev_view` is the view published right before the current one;
    // `pulled_view` is the latest view without a superseded pull. See
    // `plan_view_change` for why the two differ.
    let mut prev_view = view_rx.borrow_and_update().clone();
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
                // cold, and a miss in it is an answer.
                let alone: Vec<u16> = new_view
                    .owned_buckets()
                    .filter(|&bucket| new_view.owners_of(bucket).len() == 1)
                    .collect();
                if !alone.is_empty() {
                    residency.clear_cold(&alone);
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
                let due = residency.expired(disown_grace);
                if !due.is_empty() {
                    let view = ownership.current();
                    let self_node = cluster.node_id();
                    let live: HashSet<NodeId> = cluster.peers().iter().map(|peer| peer.node).collect();
                    let mut reconciled: HashSet<NodeId> = HashSet::new();
                    // An owner missing from gossip is unreachable by
                    // definition; the view drops it on its next refresh.
                    let mut unreachable: HashSet<NodeId> = due
                        .iter()
                        .flat_map(|&bucket| view.owners_of(bucket).iter().copied())
                        .filter(|owner| *owner != self_node && !live.contains(owner))
                        .collect();
                    for owner in hand_off_owners(&view, self_node, &due, &live) {
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
                    let due = buckets_to_release(&view, self_node, &due, &overdue, &reconciled, &unreachable);
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
    fn buckets_to_release_holds_a_bucket_until_every_owner_answered_or_it_is_overdue() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<u16> = (0..256).collect();

        let none: HashSet<NodeId> = HashSet::new();
        let held = buckets_to_release(&view, self_node, &due, &[], &none, &none);
        for &bucket in &held {
            assert!(
                view.owners_of(bucket).iter().all(|o| *o == self_node),
                "with no owner answered, only a bucket owned by self alone is released: {bucket}"
            );
        }

        let answered: HashSet<NodeId> = [NodeId::from(2)].into_iter().collect();
        let released = buckets_to_release(&view, self_node, &due, &[], &answered, &none);
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
        let still_held = buckets_to_release(&view, self_node, &due, &overdue, &none, &none);
        assert_eq!(
            still_held, held,
            "an overdue bucket whose owners are merely stale stays resident"
        );
        let everyone: HashSet<NodeId> = (2..=5u64).map(NodeId::from).collect();
        let dropped = buckets_to_release(&view, self_node, &due, &overdue, &none, &everyone);
        assert_eq!(
            dropped, due,
            "an overdue bucket whose owners are all unreachable is released"
        );
        let mixed = buckets_to_release(&view, self_node, &due, &overdue, &answered, &everyone);
        assert_eq!(
            mixed, due,
            "reconciled and unreachable owners together release"
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
        }
        .run()
        .await;
        assert_eq!(outcome, Outcome::Skipped);

        cluster.shutdown().await;
    }
}
