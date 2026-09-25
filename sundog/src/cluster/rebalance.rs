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
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::Cluster;
use super::anti_entropy::{self, RoundOutcome};
use super::state_transfer::{self, DonorResult, Outcome};
use crate::net::{BucketPull, Mesh};
use crate::node::NodeId;
use crate::ownership::{
    Granularity, OwnershipTracker, OwnershipView, ResidencySet, ownership_diff, parts_of_wire_id,
    wire_ids,
};
use crate::store::part::PartSet;
use crate::store::{PartId, ShardOps};

/// One pull group: the donors to try, in rendezvous order, and the parts
/// they all co-own.
type DonorGroup = (Vec<NodeId>, Vec<PartId>);

/// Groups `parts` by exact donor set (`view`'s live owners minus self, in
/// rendezvous order), so parts sharing a donor set land in one `StBuckets`
/// round trip; the part-scoped analogue of `cluster::group_by_owner_set`.
fn group_parts_by_donor_set(
    view: &OwnershipView,
    self_node: NodeId,
    parts: Vec<PartId>,
) -> Vec<DonorGroup> {
    // First by the view's own owner slice, borrowed rather than rebuilt per
    // part, then the few distinct slices merge by donor set: two rankings
    // that differ only in where self sits name the same donors.
    let mut by_owners: Vec<(&[NodeId], Vec<PartId>)> = Vec::new();
    let mut slice_index: HashMap<&[NodeId], usize> = HashMap::new();
    for part in parts {
        let owners = view.owners_of(part);
        match slice_index.get(owners) {
            Some(&at) => by_owners[at].1.push(part),
            None => {
                slice_index.insert(owners, by_owners.len());
                by_owners.push((owners, vec![part]));
            }
        }
    }
    let mut groups: Vec<DonorGroup> = Vec::new();
    let mut index: HashMap<Vec<NodeId>, usize> = HashMap::new();
    for (owners, parts) in by_owners {
        let donors: Vec<NodeId> = owners.iter().copied().filter(|&n| n != self_node).collect();
        match index.get(&donors) {
            Some(&at) => groups[at].1.extend(parts),
            None => {
                index.insert(donors.clone(), groups.len());
                groups.push((donors, parts));
            }
        }
    }
    groups
}

/// [`state_transfer::try_donor`]'s part-scoped counterpart: pulls `parts`
/// from `donor` via [`Mesh::request_buckets`] instead of a whole-cache
/// [`Mesh::request_state`], sharing the per-donor stream-pull-and-apply logic
/// through [`state_transfer::pull_from_donor`]. The request names the parts
/// by their wire ids at `granularity`, and each finished id marks the parts
/// it names servable.
#[expect(
    clippy::too_many_arguments,
    reason = "each parameter is independent context one donor attempt needs; `credited` in \
              particular must stay a caller-owned `&mut` shared across every donor \
              `pull_one_group` retries against, not something a struct could own instead \
              without losing that sharing"
)]
async fn try_donor_parts(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    residency: &ResidencySet,
    donor: NodeId,
    parts: &[PartId],
    (granularity, view_hash): (Granularity, u64),
    credited: &mut HashSet<PartId>,
) -> (DonorResult, u64, bool) {
    let pull = mesh
        .request_buckets(
            donor,
            cache.clone(),
            wire_ids(granularity, parts),
            view_hash,
        )
        .await;
    let cold = matches!(pull, Ok(BucketPull::Cold));
    let requested: PartSet = parts.iter().copied().collect();
    let parts_in = metrics::counter!(
        "sundog_rebalance_parts_total",
        "cache" => cache.to_string(),
        "direction" => "in"
    );
    let stream = pull.map(|answer| match answer {
        BucketPull::Stream(stream) => Some(stream),
        BucketPull::Stale | BucketPull::Cold => None,
    });
    // Cold clears per id as its BucketDone lands, so a group that never
    // finishes still leaves every finished part servable.
    // sundog_rebalance_parts_total{direction="in"} is credited here too,
    // per part; `credited` is shared across every donor this group's
    // pull_one_group retries against, so a part a prior donor already
    // credited is never credited twice on a later donor's re-send.
    let (result, count) =
        state_transfer::pull_buckets_from_donor(shard, donor, async { stream }, |id| {
            let done: Vec<PartId> = parts_of_wire_id(granularity, id).collect();
            // A pulled part is authoritative regardless of any
            // warm-reloaded on-disk checkpoint; mark_serving clears both
            // the cold and unverified marks in one decision.
            residency.mark_serving(&done);
            let fresh = done
                .into_iter()
                .filter(|&part| requested.contains(part) && credited.insert(part))
                .count();
            if fresh > 0 {
                parts_in.increment(u64::try_from(fresh).unwrap_or(u64::MAX));
            }
        })
        .await;
    (result, count, cold)
}

/// How many of a group's `total_parts` still need
/// `sundog_rebalance_parts_total{direction="in"}` credited once
/// [`DonorResult::Done`] fires: parts in `already_credited` were already
/// counted live, so crediting them again here would double-count. A
/// protocol-3 donor, which never sends per-bucket `BucketDone`, credits every
/// part this way instead.
fn parts_pending_group_credit(total_parts: usize, already_credited: &HashSet<PartId>) -> usize {
    total_parts.saturating_sub(already_credited.len())
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

/// Tries `donors` in order for one part group, applying the first that
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
    parts: Vec<PartId>,
    view: (Granularity, u64),
    per_donor: Duration,
) -> Option<u64> {
    let view_hash = view.1;
    let mut all_cold_passes = 0u32;
    // Shared across every donor and retry pass, so a part a prior donor
    // already credited is never credited again on a re-send.
    let mut credited: HashSet<PartId> = HashSet::new();
    let group_len = parts.len();
    let mut parts = parts;
    loop {
        let mut every_donor_cold = !donors.is_empty();
        for &donor in &donors {
            // A part an earlier attempt finished is serving already: the
            // next attempt asks only for the rest.
            parts.retain(|part| !credited.contains(part));
            let attempt = tokio::time::timeout(
                per_donor,
                try_donor_parts(
                    shard,
                    mesh,
                    cache,
                    residency,
                    donor,
                    &parts,
                    view,
                    &mut credited,
                ),
            )
            .await;
            let timed_out = attempt.is_err();
            let (result, count, cold) = attempt.unwrap_or_else(|_| {
                tracing::debug!(
                    cache = %cache,
                    %donor,
                    per_donor_budget = ?per_donor,
                    "rebalance part pull to donor timed out; trying the next"
                );
                (DonorResult::Failed, 0, false)
            });
            let stale = result == DonorResult::Declined && !cold;
            tracing::debug!(
                cache = %cache,
                %donor,
                parts = parts.len(),
                per_donor_budget = ?per_donor,
                result = ?result,
                records = count,
                cold,
                stale,
                timed_out,
                "part pull attempt finished"
            );
            if result == DonorResult::Done {
                residency.mark_serving(&parts);
                let pending = parts_pending_group_credit(group_len, &credited);
                if pending > 0 {
                    metrics::counter!(
                        "sundog_rebalance_parts_total",
                        "cache" => cache.to_string(),
                        "direction" => "in"
                    )
                    .increment(u64::try_from(pending).unwrap_or(u64::MAX));
                }
                tracing::debug!(cache = %cache, %donor, parts = group_len, records = count, "part pull landed");
                return Some(u64::try_from(group_len).unwrap_or(u64::MAX));
            }
            every_donor_cold &= cold;
        }
        if every_donor_cold {
            all_cold_passes += 1;
            if all_cold_passes >= ALL_COLD_PASSES {
                // No warm copy anywhere to pull; whatever landed here,
                // warm-reloaded or not, is the best available answer with
                // nobody left to verify it against.
                tracing::debug!(cache = %cache, parts = parts.len(), "every donor is cold for these parts; nothing warm to pull");
                residency.mark_serving(&parts);
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

/// One part pull's fields, gathered so every caller builds and runs one
/// value instead of repeating a nine-argument call.
pub(crate) struct PullRequest<'a> {
    pub(crate) cluster: &'a Cluster,
    pub(crate) shard: &'a Arc<dyn ShardOps>,
    pub(crate) ownership: &'a OwnershipTracker,
    pub(crate) residency: &'a Arc<ResidencySet>,
    pub(crate) cache: &'a SmolStr,
    pub(crate) parts: Vec<PartId>,
    pub(crate) budget: Duration,
    pub(crate) concurrency: usize,
    /// Whether a part found owned alone (no live co-owner) may be
    /// trusted as sole-owned and marked servable outright,
    /// rather than left cold/unverified for ordinary warm-up retries.
    /// `true` for every routine call, where the ownership view is the
    /// library's normal live-updating one.
    ///
    /// For `Cache::open`'s initial pull, `crate::cache::trust_sole_owner_at_open`
    /// computes this: a cold open passes `true` regardless, since a
    /// sole-owned part there is this node's own data. A warm
    /// reopen whose membership wait timed out passes `false`, since "no
    /// live co-owner" there can be the transient view a lone-looking node
    /// computes before gossip shows it any peer, not genuine single
    /// ownership.
    pub(crate) trust_sole_owner: bool,
}

impl PullRequest<'_> {
    /// Pulls `parts` from any current owner, grouped by donor set so two
    /// parts sharing an owner-set-minus-self go in one `StBuckets` round
    /// trip, bounded to `concurrency` simultaneous donor streams. Answers
    /// [`Outcome::Completed`]/[`Outcome::Skipped`]/[`Outcome::NoPeers`] for
    /// an empty part set, a zero budget, or no live co-owner anywhere;
    /// otherwise races `budget`, answering [`Outcome::TimedOut`] or
    /// [`Outcome::Superseded`] if it runs out or the view moves on first.
    pub(crate) async fn run(self) -> Outcome {
        let Self {
            cluster,
            shard,
            ownership,
            residency,
            cache,
            parts,
            budget,
            concurrency,
            trust_sole_owner,
        } = self;
        if parts.is_empty() {
            return Outcome::Completed;
        }
        if budget.is_zero() {
            tracing::debug!(cache = %cache, "rebalance transfer budget is zero; leaving gained parts to anti-entropy");
            return Outcome::Skipped;
        }

        let view = ownership.current();
        let view_key = (view.granularity(), view.view_hash());
        let (groups, no_donor): (Vec<DonorGroup>, Vec<DonorGroup>) =
            group_parts_by_donor_set(&view, cluster.node_id(), parts)
                .into_iter()
                .partition(|(donors, _)| !donors.is_empty());
        // A part this node owns alone has nobody to pull from or verify a
        // warm-reloaded record against, so it is neither cold nor
        // unverified, but only when `trust_sole_owner` says this view's
        // "alone" answer is real. Left untrusted, it falls to
        // warm_up_task's ordinary retries via Outcome::NoPeers.
        let alone: Vec<PartId> = no_donor.into_iter().flat_map(|(_, p)| p).collect();
        if !alone.is_empty() && trust_sole_owner {
            residency.mark_serving(&alone);
        }
        if groups.is_empty() {
            tracing::debug!(cache = %cache, "no live co-owner for any gained part; nothing to pull");
            return Outcome::NoPeers;
        }

        let per_donor = state_transfer::per_donor_budget(budget);
        let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
        let mesh = cluster.mesh().clone();

        let run_all = async {
            let mut set = tokio::task::JoinSet::new();
            for (donors, group_parts) in groups {
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
                        group_parts,
                        view_key,
                        per_donor,
                    )
                    .await
                });
            }
            // sundog_rebalance_parts_total{direction="in"} is already
            // credited per part inside pull_one_group/try_donor_parts;
            // this loop only needs `superseded` for the overall Outcome.
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
        let parts: Vec<PartId> = view_rx.borrow_and_update().owned_parts().collect();
        let pull = PullRequest {
            cluster: &cluster,
            shard: &shard,
            ownership: &ownership,
            residency: &residency,
            cache: &cache,
            parts,
            budget,
            concurrency,
            // The library's normal, live-updating view by the time this retry loop runs.
            trust_sole_owner: true,
        };
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            outcome = pull.run() => outcome,
        };
        attempt += 1;
        let step = state_transfer::next_warm_up_step(outcome, attempt);
        tracing::debug!(
            cache = %cache,
            attempt,
            outcome = ?outcome,
            step = ?step,
            "rebalance warm-up step chosen"
        );
        match step {
            state_transfer::WarmUpStep::Done => {
                cluster.mark_warm(&cache);
                return;
            }
            state_transfer::WarmUpStep::WaitForPeer => {
                tracing::debug!(cache = %cache, "no co-owner for any owned part yet; waiting for the ownership view to change");
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
                // No donor left to try: whatever landed, warm-reloaded or
                // not, is declared servable.
                residency.mark_all_serving();
                cluster.mark_warm(&cache);
                return;
            }
        }
    }
}

/// What one published view change asks of [`rebalance_task`]: `regained`
/// is the difference from the previous view (`prev`); `lost` is that
/// difference plus every part in `held` (the shard's
/// [`ShardOps::held_parts`]) that `new` does not own, since a part owned only
/// under a view published and superseded while the task was busy is in
/// neither `prev` nor `new`, and what the inbound guard applied there stays
/// resident until this fold releases it; `to_pull` is the difference from the
/// latest view whose pull is not superseded (`pulled`), so a part gained
/// under a view that gets superseded mid-pull is pulled again under the
/// current one instead of skipped.
pub(crate) struct ViewChangePlan {
    pub(crate) lost: Vec<PartId>,
    pub(crate) regained: Vec<PartId>,
    pub(crate) to_pull: Vec<PartId>,
}

pub(crate) fn plan_view_change(
    prev: &OwnershipView,
    pulled: &OwnershipView,
    new: &OwnershipView,
    held: &[PartId],
) -> ViewChangePlan {
    let (regained, mut lost) = ownership_diff(prev, new);
    let mut seen: PartSet = lost.iter().copied().collect();
    lost.extend(
        held.iter()
            .copied()
            .filter(|&part| !new.owns(part) && seen.insert(part)),
    );
    let (to_pull, _) = ownership_diff(pulled, new);
    ViewChangePlan {
        lost,
        regained,
        to_pull,
    }
}

/// The distinct live current owners, other than `self_node`, of the parts
/// in `due`: the peers a release hands each part to first.
pub(crate) fn hand_off_owners(
    view: &OwnershipView,
    self_node: NodeId,
    due: &[PartId],
    live: &HashSet<NodeId>,
) -> Vec<NodeId> {
    let mut owners: Vec<NodeId> = Vec::new();
    for &part in due {
        for &owner in view.owners_of(part) {
            if owner != self_node && live.contains(&owner) && !owners.contains(&owner) {
                owners.push(owner);
            }
        }
    }
    owners
}

/// The parts in `lost` that no new owner can pull from a surviving
/// co-owner: no node but `self_node` owns the part under both `prev` and
/// `new`, so a new owner's pull asks only nodes that never held it. A node
/// that owned every part alone, because its peers had not opened the cache
/// yet, loses most of its parts this way when a view with two or more of
/// them lands, and so does every node when a cluster's view switches from
/// bucket to part granularity. A part with a surviving co-owner is left to
/// the new owner's pull.
pub(crate) fn unpullable_losses(
    prev: &OwnershipView,
    new: &OwnershipView,
    self_node: NodeId,
    lost: &[PartId],
) -> Vec<PartId> {
    lost.iter()
        .copied()
        .filter(|&part| {
            let prev_owners = prev.owners_of(part);
            !new.owners_of(part)
                .iter()
                .any(|owner| *owner != self_node && prev_owners.contains(owner))
        })
        .collect()
}

/// Each live owner of `parts` under `view` other than `self_node`, paired
/// with the parts among `parts` it owns, so one scoped round per owner
/// covers all of them.
pub(crate) fn push_targets(
    view: &OwnershipView,
    self_node: NodeId,
    parts: &[PartId],
    live: &HashSet<NodeId>,
) -> Vec<(NodeId, Vec<PartId>)> {
    let mut targets: Vec<(NodeId, Vec<PartId>)> = Vec::new();
    for &part in parts {
        for &owner in view.owners_of(part) {
            if owner == self_node || !live.contains(&owner) {
                continue;
            }
            match targets.iter_mut().find(|(node, _)| *node == owner) {
                Some((_, owned)) => owned.push(part),
                None => targets.push((owner, vec![part])),
            }
        }
    }
    targets
}

/// Pushes each target's parts to it through a part-scoped anti-entropy
/// round, which sends every entry the target
/// lacks or holds at an older version. A round that fails, or meets a
/// responder whose view has not caught up with this node's, is retried
/// every `retry` until every target has answered, `deadline` passes, or
/// `cancel` fires. The disown-grace hand-off still confirms each part before
/// it is released; this push brings the data to the new owners when the
/// view changes rather than when the grace ends.
async fn push_unpullable(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    cache: SmolStr,
    targets: Vec<(NodeId, Vec<PartId>)>,
    retry: Duration,
    deadline: Duration,
    cancel: CancellationToken,
) {
    let give_up = tokio::time::Instant::now() + deadline;
    let mut targets = targets;
    loop {
        let mut pending = Vec::with_capacity(targets.len());
        for (owner, parts) in targets {
            let outcome = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                outcome = anti_entropy::run_round_for_parts(cluster.mesh(), &shard, &cache, owner, &parts) => outcome,
            };
            if outcome.failed {
                pending.push((owner, parts));
            } else {
                tracing::debug!(cache = %cache, %owner, parts = parts.len(), bytes = outcome.bytes_moved, "pushed parts no co-owner could hand over");
            }
        }
        if pending.is_empty() {
            return;
        }
        if tokio::time::Instant::now() + retry > give_up {
            tracing::debug!(cache = %cache, owners = pending.len(), "left parts no co-owner could hand over to the disown-grace hand-off");
            return;
        }
        targets = pending;
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(retry) => {}
        }
    }
}

/// How long a released part may stay resident before it is dropped
/// without a hand-off: the tombstone retention less one interval for the
/// round. A copy held longer can no longer be trusted not to resurrect a
/// key the owners removed and whose tombstone they have since collected;
/// losing what the owners never pulled costs a cache entry, resurrecting a
/// delete breaks the contract that deleted entries never come back.
pub(crate) fn hand_off_cutoff(tombstone_ttl: Duration, ae_interval: Duration) -> Duration {
    tombstone_ttl.saturating_sub(ae_interval).max(ae_interval)
}

/// Drops `parts` held past [`hand_off_cutoff`] at once, counted under
/// `sundog_rebalance_parts_total{direction="out"}` like any release.
async fn release_without_hand_off(
    shard: &dyn ShardOps,
    residency: &ResidencySet,
    cache: &SmolStr,
    parts: &[PartId],
) {
    if parts.is_empty() {
        return;
    }
    let removed = shard.release_parts(parts).await;
    residency.unmark(parts);
    metrics::counter!(
        "sundog_rebalance_parts_total",
        "cache" => cache.to_string(),
        "direction" => "out"
    )
    .increment(u64::try_from(parts.len()).unwrap_or(u64::MAX));
    tracing::warn!(cache = %cache, parts = parts.len(), removed, "released parts held past the tombstone retention without a hand-off");
}

/// Acks recorded against wire ids, re-keyed by the parts each id names at
/// `granularity`.
fn acked_by_part(
    granularity: Granularity,
    acked: HashMap<u16, HashSet<NodeId>>,
) -> HashMap<PartId, HashSet<NodeId>> {
    let mut out: HashMap<PartId, HashSet<NodeId>> = HashMap::new();
    for (id, owners) in acked {
        for part in parts_of_wire_id(granularity, id) {
            out.entry(part).or_default().extend(owners.iter().copied());
        }
    }
    out
}

/// The parts in `due` a release may drop now: those whose every other
/// current owner is in `reconciled` (its round completed since the part came
/// due), has acked that specific part per `acked` (keyed by part, never
/// treated as reconciling any other part that owner holds), or, once
/// `overdue`, is in `unreachable`. An owner whose view still differs from
/// this node's is none of these, so the part stays resident until the views
/// converge.
pub(crate) fn parts_to_release(
    view: &OwnershipView,
    self_node: NodeId,
    due: &[PartId],
    overdue: &PartSet,
    reconciled: &HashSet<NodeId>,
    acked: &HashMap<PartId, HashSet<NodeId>>,
    unreachable: &HashSet<NodeId>,
) -> Vec<PartId> {
    due.iter()
        .copied()
        .filter(|&part| {
            view.owners_of(part).iter().all(|owner| {
                *owner == self_node
                    || reconciled.contains(owner)
                    || acked
                        .get(&part)
                        .is_some_and(|owners| owners.contains(owner))
                    || (overdue.contains(part) && unreachable.contains(owner))
            })
        })
        .collect()
}

/// Which of `hand_off_owners`'s `owners` still need a confirming
/// anti-entropy round: an owner already in `reconciled` needs none;
/// otherwise an owner is skipped only once [`crate::net::Mesh::acked_owners`]
/// covers every part in `due` that owner co-owns, never on a single acked
/// part among several.
fn owners_needing_confirmation(
    view: &OwnershipView,
    owners: &[NodeId],
    due: &[PartId],
    reconciled: &HashSet<NodeId>,
    acked: &HashMap<PartId, HashSet<NodeId>>,
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
                .filter(|&&part| view.owners_of(part).contains(owner))
                .all(|&part| {
                    acked
                        .get(&part)
                        .is_some_and(|owners| owners.contains(owner))
                });
            !fully_acked
        })
        .collect()
}

/// Reacts to every change in `ownership`'s view for as long as `cancel`
/// stays live: marks newly lost parts releasing, unmarks newly regained
/// ones, and pulls newly gained ones from their current owners. A lost part
/// this node holds data in and no surviving co-owner can hand over
/// ([`unpullable_losses`]) is pushed to its new owners at once
/// ([`push_unpullable`]), so a write accepted while this node owned the part
/// alone reaches them without waiting out the grace. On a tick piggybacked
/// on `ae_interval`, hands off whichever parts' disown grace has elapsed to
/// their live current owners ([`hand_off_owners`]), then calls
/// [`ShardOps::release_parts`] for the parts every owner answered
/// ([`parts_to_release`]); an owner that never answers keeps the part
/// resident until it is reachable again or its view converges. An owner that
/// acked a specific due part, within the ack window and current view hash,
/// counts as reconciled for that part without a redundant confirming round;
/// an ack is scoped to the parts its wire id names, so
/// [`owners_needing_confirmation`] still runs a round unless every due part
/// that owner co-owns was acked. `disown_grace` still gates which parts
/// reach this hand-off, so an ack only accelerates confirmation, never the
/// release timing floor.
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
    // original seeded view, never a live re-borrow that could already
    // show a raced-ahead publish (see `OwnershipTracker::baseline`): a
    // part only the seed view ever called owned must still be recognized
    // as lost on the first view change this task observes. `pulled_view`
    // is the latest view without a superseded pull; see `plan_view_change`
    // for why the two differ.
    let mut prev_view = ownership.baseline();
    let mut pulled_view = Arc::clone(&prev_view);
    let mut ticker = tokio::time::interval(ae_interval.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Pushes of parts lost with no surviving co-owner, one per view change
    // that lost any. Each stops on `cancel` through its child token;
    // dropping the set when this task returns aborts any left.
    let mut pushes: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            Some(_) = pushes.join_next(), if !pushes.is_empty() => {}
            changed = view_rx.changed() => {
                if changed.is_err() {
                    return; // the tracker's sender dropped
                }
                let new_view = view_rx.borrow_and_update().clone();
                let held = shard.held_parts().await;
                let plan = plan_view_change(&prev_view, &pulled_view, &new_view, &held);
                let held_set: PartSet = held.iter().copied().collect();
                let held_losses: Vec<PartId> = plan
                    .lost
                    .iter()
                    .copied()
                    .filter(|&part| held_set.contains(part))
                    .collect();
                let unpullable =
                    unpullable_losses(&prev_view, &new_view, cluster.node_id(), &held_losses);
                prev_view = Arc::clone(&new_view);
                tracing::debug!(
                    cache = %cache,
                    gained = plan.to_pull.len(),
                    lost = plan.lost.len(),
                    granularity = ?new_view.granularity(),
                    "ownership view changed"
                );
                // A part owned alone has nobody to pull from or verify a
                // record against, so it is neither cold nor unverified.
                let alone: Vec<PartId> = new_view
                    .owned_parts()
                    .filter(|&part| new_view.owners_of(part).len() == 1)
                    .collect();
                if !alone.is_empty() {
                    residency.mark_serving(&alone);
                }
                if !plan.lost.is_empty() {
                    residency.mark_releasing(&plan.lost);
                    tracing::debug!(cache = %cache, count = plan.lost.len(), "parts lost; disown grace started");
                }
                let live: HashSet<NodeId> = cluster.peers().iter().map(|peer| peer.node).collect();
                let targets = push_targets(&new_view, cluster.node_id(), &unpullable, &live);
                if !targets.is_empty() {
                    tracing::debug!(cache = %cache, parts = unpullable.len(), owners = targets.len(), "parts lost with no co-owner to hand them over; pushing to their new owners");
                    pushes.spawn(push_unpullable(
                        cluster.clone(),
                        Arc::clone(&shard),
                        cache.clone(),
                        targets,
                        cluster.config().gossip_interval.max(Duration::from_millis(1)),
                        disown_grace,
                        cancel.child_token(),
                    ));
                }
                if !plan.regained.is_empty() {
                    residency.unmark(&plan.regained);
                }
                let outcome = if plan.to_pull.is_empty() {
                    Outcome::Completed
                } else {
                    // Gained from a co-owner: cold until the pull lands.
                    residency.mark_cold(&plan.to_pull);
                    tracing::debug!(cache = %cache, count = plan.to_pull.len(), "parts gained; pulling from current owners");
                    PullRequest {
                        cluster: &cluster,
                        shard: &shard,
                        ownership: &ownership,
                        residency: &residency,
                        cache: &cache,
                        parts: plan.to_pull,
                        budget,
                        concurrency,
                        // The ongoing rebalance loop, well past open()'s membership wait.
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
                let past_cutoff: PartSet = past_cutoff.into_iter().collect();
                let due: Vec<PartId> = residency
                    .expired(disown_grace)
                    .into_iter()
                    .filter(|&part| !past_cutoff.contains(part))
                    .collect();
                if !due.is_empty() {
                    let view = ownership.current();
                    let self_node = cluster.node_id();
                    let live: HashSet<NodeId> = cluster.peers().iter().map(|peer| peer.node).collect();
                    // An owner that already acked a due part counts as
                    // reconciled for that part without a redundant round;
                    // kept separate from `reconciled` (AE-round derived,
                    // owner-global) since an ack names one wire id, never
                    // every part the owner co-owns.
                    let acked = acked_by_part(
                        view.granularity(),
                        cluster.mesh().acked_owners(
                            &cache,
                            &view.wire_ids(&due),
                            cluster.config().rebalance_ack_window_value(),
                            view.view_hash(),
                        ),
                    );
                    let mut reconciled: HashSet<NodeId> = HashSet::new();
                    // An owner missing from gossip is unreachable by
                    // definition; the view drops it on its next refresh.
                    let mut unreachable: HashSet<NodeId> = due
                        .iter()
                        .flat_map(|&part| view.owners_of(part).iter().copied())
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
                    let overdue: PartSet = residency.expired(disown_grace * 2).into_iter().collect();
                    let due = parts_to_release(
                        &view,
                        self_node,
                        &due,
                        &overdue,
                        &reconciled,
                        &acked,
                        &unreachable,
                    );
                    if due.is_empty() {
                        tracing::debug!(
                            cache = %cache,
                            reconciled = reconciled.len(),
                            acked = acked.len(),
                            "no released part's owners all answered its hand-off; holding until the next tick"
                        );
                        continue;
                    }
                    let removed = shard.release_parts(&due).await;
                    residency.unmark(&due);
                    metrics::counter!(
                        "sundog_rebalance_parts_total",
                        "cache" => cache.to_string(),
                        "direction" => "out"
                    )
                    .increment(u64::try_from(due.len()).unwrap_or(u64::MAX));
                    tracing::debug!(
                        cache = %cache,
                        parts = due.len(),
                        removed,
                        reconciled = reconciled.len(),
                        acked = acked.len(),
                        "released parts past their disown grace"
                    );
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
    fn group_parts_by_donor_set_groups_parts_sharing_the_same_donors() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let parts: Vec<PartId> = PartId::all().step_by(97).take(64).collect();

        let groups = group_parts_by_donor_set(&view, self_node, parts.clone());

        // Every part appears in exactly one group, and each group's donor
        // set is precisely that part's owners minus self.
        let mut regrouped: Vec<PartId> = groups.iter().flat_map(|(_, ps)| ps.clone()).collect();
        regrouped.sort_unstable();
        assert_eq!(regrouped, parts);
        for (donors, group_parts) in &groups {
            for &part in group_parts {
                let expected: Vec<NodeId> = view
                    .owners_of(part)
                    .iter()
                    .copied()
                    .filter(|&n| n != self_node)
                    .collect();
                assert_eq!(donors, &expected);
            }
        }
    }

    #[test]
    fn group_parts_by_donor_set_merges_rankings_that_name_the_same_donors() {
        // Two eligible nodes at k=2: every part's owners are self and the
        // other node, in either order, so every part has one donor set.
        let self_node = NodeId::from(1);
        let other = NodeId::from(2);
        let view = OwnershipView::compute_at(
            self_node,
            vec![self_node, other],
            NonZeroU8::new(2).expect("nonzero"),
            Granularity::Part,
        );
        let orders: HashSet<Vec<NodeId>> = PartId::all()
            .map(|part| view.owners_of(part).to_vec())
            .collect();
        assert_eq!(
            orders.len(),
            2,
            "the fixture ranks self first for some parts and second for others"
        );
        let groups = group_parts_by_donor_set(&view, self_node, PartId::all().collect());
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, vec![other]);
        assert_eq!(groups[0].1.len(), crate::store::part::PART_SPACE);
    }

    #[test]
    fn group_parts_by_donor_set_excludes_self_from_every_donor_list() {
        let self_node = NodeId::from(1);
        // Solo eligible set: self is every part's only owner, so every
        // donor list is empty once self is excluded.
        let view = view(self_node, vec![self_node], 2);
        let groups = group_parts_by_donor_set(&view, self_node, PartId::all().take(3).collect());

        assert_eq!(groups.len(), 1);
        assert!(groups[0].0.is_empty());
    }

    #[test]
    fn parts_pending_group_credit_excludes_parts_already_credited_live() {
        let mut already_credited = HashSet::new();
        already_credited.insert(PartId::from_raw(3));
        already_credited.insert(PartId::from_raw(7));
        assert_eq!(
            parts_pending_group_credit(5, &already_credited),
            3,
            "5 parts minus the 2 already credited live leaves 3 for the group-level fallback"
        );
    }

    #[test]
    fn parts_pending_group_credit_is_the_full_group_when_nothing_landed_live() {
        assert_eq!(
            parts_pending_group_credit(4, &HashSet::new()),
            4,
            "a protocol-3 donor never fires a live BucketDone, so every part in the group is \
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
        let owned = |v: &OwnershipView| v.owned_parts().collect::<HashSet<PartId>>();
        let (o0, o1, o2) = (owned(&v0), owned(&v1), owned(&v2));

        // v0 -> v1 pulled cleanly: prev and pulled agree.
        let plan = plan_view_change(&v0, &v0, &v1, &[]);
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
        let plan = plan_view_change(&v1, &v0, &v2, &[]);
        let gained_then_lost: Vec<PartId> = o1
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
        // The seeded, lone-node view: the transient "owns everything" state.
        let baseline = tracker.baseline();
        assert_eq!(baseline.owned_part_count(), crate::store::part::PART_SPACE);

        // Published before rebalance_task is spawned: as if refresh_task
        // had already raced ahead, the very race baseline() exists to
        // make irrelevant.
        let other_a = NodeId::from(u64::MAX);
        let other_b = NodeId::from(u64::MAX - 1);
        let corrected = Arc::new(view(
            cluster.node_id(),
            vec![cluster.node_id(), other_a, other_b],
            2,
        ));
        let bucket_only_in_baseline = baseline
            .owned_parts()
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

        // Gives rebalance_task time to subscribe before this test publishes.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // A raw resend, a genuine new channel version, so view_rx.changed()
        // fires without needing a third distinct membership change.
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
    fn plan_view_change_marks_a_held_bucket_lost_when_the_view_that_owned_it_was_never_observed() {
        let self_node = NodeId::from(1);
        let k = 2;
        // v0: three nodes. v1: node 3 died. v2: node 4 joined. The task
        // observed v0 and v2 only: v1 came and went while it was busy.
        let v0 = view(self_node, (1..=3u64).map(NodeId::from).collect(), k);
        let v1 = view(self_node, (1..=2u64).map(NodeId::from).collect(), k);
        let v2 = view(self_node, [1u64, 2, 4].map(NodeId::from).to_vec(), k);
        let owned = |v: &OwnershipView| v.owned_parts().collect::<HashSet<PartId>>();
        let (o0, o1, o2) = (owned(&v0), owned(&v1), owned(&v2));
        let only_in_v1: Vec<PartId> = o1
            .iter()
            .copied()
            .filter(|b| !o0.contains(b) && !o2.contains(b))
            .collect();
        assert!(
            !only_in_v1.is_empty(),
            "the fixture has a bucket owned under v1 alone"
        );
        let kept = *o2
            .iter()
            .find(|b| o0.contains(b))
            .expect("the fixture has a bucket owned throughout");

        // Without the held fold, the unobserved view leaves no trace.
        let blind = plan_view_change(&v0, &v0, &v2, &[]);
        for b in &only_in_v1 {
            assert!(
                !blind.lost.contains(b),
                "bucket {b} is in neither prev nor new, so the plain diff misses it"
            );
        }

        // The inbound guard applied entries to those buckets under v1:
        // the shard holds them, and the fold releases every one.
        let mut held = only_in_v1.clone();
        held.push(kept);
        let plan = plan_view_change(&v0, &v0, &v2, &held);
        for b in &only_in_v1 {
            assert!(
                plan.lost.contains(b),
                "held bucket {b} starts its disown grace"
            );
        }
        assert!(
            !plan.lost.contains(&kept),
            "a held bucket the new view owns is not lost"
        );
        let distinct: HashSet<PartId> = plan.lost.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            plan.lost.len(),
            "lost lists each bucket once"
        );
        for b in o0.iter().filter(|b| !o2.contains(b)) {
            assert!(
                plan.lost.contains(b),
                "the plain prev-to-new loss {b} stays"
            );
        }
    }

    #[tokio::test]
    async fn rebalance_task_releases_a_bucket_held_under_a_view_it_never_observed() {
        let cluster = solo_cluster("rebalance-unit-test-held-release").await;
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        let other_a = NodeId::from(u64::MAX);
        let other_b = NodeId::from(u64::MAX - 1);
        let settled = Arc::new(view(
            cluster.node_id(),
            vec![cluster.node_id(), other_a, other_b],
            2,
        ));
        let bucket = PartId::all()
            .find(|&p| !settled.owns(p))
            .expect("a three-node, k=2 split leaves some bucket unowned here");
        let shard = empty_shard();
        let residency = Arc::new(ResidencySet::new());
        let cancel = CancellationToken::new();
        let _task = tokio::spawn(rebalance_task(
            cluster.clone(),
            Arc::clone(&shard),
            tracker,
            Arc::clone(&residency),
            name.clone(),
            Duration::from_secs(3600),
            4,
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The lone-node baseline owned everything, so the first observed
        // view loses `bucket` through the plain diff; its release then
        // completes, modelled by the same unmark the release path calls.
        tx.send(Arc::clone(&settled))
            .expect("the tracker's own receiver keeps the channel open");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(residency.is_releasing(bucket), "lost from the baseline");
        residency.unmark(&[bucket]);

        // A change that moves nothing, with nothing held: no grace starts.
        tx.send(Arc::clone(&settled))
            .expect("the tracker's own receiver keeps the channel open");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !residency.is_releasing(bucket),
            "an empty unowned bucket has nothing to release"
        );

        // An entry lands in the bucket, as the inbound guard applies one
        // under a view that owned it and was superseded before this task
        // observed it. The next observed change, whose prev and new both
        // disown the bucket, still starts its grace.
        let key = (0..1_000_000u32)
            .find(|key| PartId::of_key(&postcard::to_stdvec(key).expect("encodes")) == bucket)
            .expect("some key hashes into the bucket");
        shard
            .apply_remote_batch(vec![crate::wire::WireRecord {
                key: bytes::Bytes::from(postcard::to_stdvec(&key).expect("encodes")),
                value: Some(bytes::Bytes::from(
                    postcard::to_stdvec(&7u32).expect("encodes"),
                )),
                ver: crate::hlc::Hlc {
                    wall_ms: 1,
                    logical: 0,
                    node: NodeId::from(9),
                },
                expires_at_ms: None,
            }])
            .await;
        assert_eq!(shard.held_parts().await, vec![bucket]);
        tx.send(Arc::clone(&settled))
            .expect("the tracker's own receiver keeps the channel open");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            residency.is_releasing(bucket),
            "a bucket the shard holds without owning starts its disown grace on the next \
             observed view change, whichever views the task saw in between"
        );

        cancel.cancel();
        cluster.shutdown().await;
    }

    #[test]
    fn unpullable_losses_names_every_bucket_a_lone_owner_loses() {
        let self_node = NodeId::from(1);
        let alone = view(self_node, vec![self_node], 2);
        let three = view(self_node, (1..=3u64).map(NodeId::from).collect(), 2);
        let (_, lost) = ownership_diff(&alone, &three);
        assert!(!lost.is_empty(), "a three-node split takes buckets away");
        assert_eq!(
            unpullable_losses(&alone, &three, self_node, &lost),
            lost,
            "no other node owned any bucket before, so none can hand one over"
        );
    }

    #[test]
    fn unpullable_losses_leaves_a_bucket_with_a_surviving_co_owner_to_the_pull() {
        let self_node = NodeId::from(1);
        let before = view(self_node, (1..=3u64).map(NodeId::from).collect(), 2);
        let after = view(self_node, (1..=4u64).map(NodeId::from).collect(), 2);
        let (_, lost) = ownership_diff(&before, &after);
        assert!(!lost.is_empty(), "the joiner displaces this node somewhere");
        assert!(
            unpullable_losses(&before, &after, self_node, &lost).is_empty(),
            "a joiner only displaces one owner per bucket, so the other still holds it"
        );

        // Both of a bucket's other owners change: nobody left holds it.
        let swapped = view(self_node, [1u64, 5, 6].map(NodeId::from).to_vec(), 2);
        let (_, lost) = ownership_diff(&before, &swapped);
        let orphaned: Vec<PartId> = lost
            .iter()
            .copied()
            .filter(|&bucket| {
                !swapped
                    .owners_of(bucket)
                    .iter()
                    .any(|owner| before.owners_of(bucket).contains(owner))
            })
            .collect();
        assert!(
            !orphaned.is_empty(),
            "the fixture has a bucket with no survivor"
        );
        assert_eq!(
            unpullable_losses(&before, &swapped, self_node, &lost),
            orphaned
        );
    }

    #[test]
    fn push_targets_groups_buckets_by_live_owner_and_skips_self_and_the_dead() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=4u64).map(NodeId::from).collect();
        let view = view(self_node, eligible.clone(), 2);
        let buckets: Vec<PartId> = PartId::all().take(64).collect();
        let dead = NodeId::from(4);
        let live: HashSet<NodeId> = [2u64, 3].map(NodeId::from).into_iter().collect();
        let targets = push_targets(&view, self_node, &buckets, &live);
        let owners: Vec<NodeId> = targets.iter().map(|(owner, _)| *owner).collect();
        assert!(!owners.contains(&self_node), "self is never a push target");
        assert!(
            !owners.contains(&dead),
            "a dead owner is never a push target"
        );
        let distinct: HashSet<NodeId> = owners.iter().copied().collect();
        assert_eq!(distinct.len(), owners.len(), "each owner appears once");
        for &bucket in &buckets {
            for owner in view.owners_of(bucket) {
                let listed = targets
                    .iter()
                    .any(|(node, owned)| node == owner && owned.contains(&bucket));
                assert_eq!(
                    listed,
                    live.contains(owner),
                    "bucket {bucket} goes to each live owner other than self"
                );
            }
        }
        for (owner, owned) in &targets {
            for bucket in owned {
                assert!(view.owners_of(*bucket).contains(owner));
            }
        }
    }

    #[test]
    fn hand_off_owners_lists_each_live_current_owner_once_and_never_self() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible.clone(), 2);
        let due: Vec<PartId> = PartId::all().take(256).collect();
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
    fn parts_to_release_holds_a_bucket_until_every_owner_answered_or_it_is_overdue() {
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<PartId> = PartId::all().take(256).collect();

        let none: HashSet<NodeId> = HashSet::new();
        let no_acked: HashMap<PartId, HashSet<NodeId>> = HashMap::new();
        let held = parts_to_release(
            &view,
            self_node,
            &due,
            &PartSet::new(),
            &none,
            &no_acked,
            &none,
        );
        for &bucket in &held {
            assert!(
                view.owners_of(bucket).iter().all(|o| *o == self_node),
                "with no owner answered, only a bucket owned by self alone is released: {bucket}"
            );
        }

        let answered: HashSet<NodeId> = [NodeId::from(2)].into_iter().collect();
        let released = parts_to_release(
            &view,
            self_node,
            &due,
            &PartSet::new(),
            &answered,
            &no_acked,
            &none,
        );
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
        let overdue: PartSet = due.iter().copied().collect();
        let still_held =
            parts_to_release(&view, self_node, &due, &overdue, &none, &no_acked, &none);
        assert_eq!(
            still_held, held,
            "an overdue bucket whose owners are merely stale stays resident"
        );
        let everyone: HashSet<NodeId> = (2..=5u64).map(NodeId::from).collect();
        let dropped = parts_to_release(
            &view, self_node, &due, &overdue, &none, &no_acked, &everyone,
        );
        assert_eq!(
            dropped, due,
            "an overdue bucket whose owners are all unreachable is released"
        );
        let mixed = parts_to_release(
            &view, self_node, &due, &overdue, &answered, &no_acked, &everyone,
        );
        assert_eq!(
            mixed, due,
            "reconciled and unreachable owners together release"
        );
    }

    #[test]
    fn parts_to_release_accepts_an_acked_owner_without_a_confirming_ae_round() {
        // An acked owner releases like a reconciled one; other current
        // owners must still be covered.
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<PartId> = PartId::all().take(256).collect();
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
        let no_acked: HashMap<PartId, HashSet<NodeId>> = HashMap::new();
        assert!(
            !parts_to_release(
                &view,
                self_node,
                &[bucket],
                &PartSet::new(),
                &none,
                &no_acked,
                &none
            )
            .contains(&bucket),
            "with no ack and no round, the bucket stays resident"
        );

        let acked: HashMap<PartId, HashSet<NodeId>> =
            HashMap::from([(bucket, HashSet::from([other]))]);
        assert!(
            parts_to_release(
                &view,
                self_node,
                &[bucket],
                &PartSet::new(),
                &none,
                &acked,
                &none
            )
            .contains(&bucket),
            "an owner who acked this bucket releases it without a confirming AE round against it"
        );
    }

    #[test]
    fn parts_to_release_never_releases_a_co_owned_bucket_whose_owner_only_acked_a_different_bucket()
    {
        // Regression: two due buckets share an owner who acked only one;
        // the un-acked bucket must stay resident.
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<PartId> = PartId::all()
            .take(256)
            .filter(|&b| view.owners_of(b).contains(&self_node) && view.owners_of(b).len() == 2)
            .collect();
        let other = *view
            .owners_of(due[0])
            .iter()
            .find(|&&o| o != self_node)
            .expect("a co-owned bucket has another owner");
        // Two distinct due buckets `other` co-owns alongside self.
        let owner_due: Vec<PartId> = due
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
        let acked: HashMap<PartId, HashSet<NodeId>> =
            HashMap::from([(bucket_acked, HashSet::from([other]))]);
        let released = parts_to_release(
            &view,
            self_node,
            &[bucket_acked, bucket_unacked],
            &PartSet::new(),
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
        // The full bucket range: every real caller's owners co-own at
        // least one bucket in `due`, so an empty range here would
        // vacuously look reconciled.
        let due: Vec<PartId> = PartId::all().take(256).collect();
        let reconciled: HashSet<NodeId> = [NodeId::from(2)].into_iter().collect();
        let no_acked: HashMap<PartId, HashSet<NodeId>> = HashMap::new();
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
        // Same partial-ack scenario from the confirming-round side: an
        // owner who acked only one of several due buckets still needs a round.
        let self_node = NodeId::from(1);
        let eligible: Vec<NodeId> = (1..=3u64).map(NodeId::from).collect();
        let view = view(self_node, eligible, 2);
        let due: Vec<PartId> = PartId::all()
            .take(256)
            .filter(|&b| view.owners_of(b).contains(&self_node) && view.owners_of(b).len() == 2)
            .collect();
        let other = *view
            .owners_of(due[0])
            .iter()
            .find(|&&o| o != self_node)
            .expect("a co-owned bucket has another owner");
        let owner_due: Vec<PartId> = due
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
        let acked: HashMap<PartId, HashSet<NodeId>> =
            HashMap::from([(owner_due[0], HashSet::from([other]))]);
        assert_eq!(
            owners_needing_confirmation(&view, &[other], &due, &reconciled, &acked),
            vec![other],
            "a partial ack never lets an owner skip its confirming round"
        );

        // Acking every due bucket `other` co-owns does let it skip.
        let full_acked: HashMap<PartId, HashSet<NodeId>> = owner_due
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
            parts: Vec::new(),
            budget: Duration::from_secs(1),
            concurrency: 4,
            trust_sole_owner: true,
        }
        .run()
        .await;
        assert_eq!(outcome, Outcome::Completed);

        cluster.shutdown().await;
    }

    /// `RequestHandler::st_serve`'s default composes the three bucket
    /// calls: a hash mismatch is `Stale`, and otherwise the ids stream in
    /// request order, each standing for a whole bucket.
    #[tokio::test]
    async fn st_serve_default_composes_the_bucket_availability_cold_and_chunk_calls() {
        use futures::StreamExt as _;

        use crate::net::test_support::BucketPullHandler;
        use crate::net::{RequestHandler, StServe};

        let handler = BucketPullHandler {
            view_hash: 42,
            chunks: vec![(9u16, vec![sample_wire_record(1)])],
            stall_after: false,
            requested: Default::default(),
        };
        let cache = SmolStr::new("prices");
        assert!(matches!(
            handler.st_serve(cache.clone(), vec![9, 3], 41).await,
            StServe::Stale {
                responder_view_hash: 0
            }
        ));
        let StServe::Stream {
            mut chunks,
            order,
            parts,
        } = handler.st_serve(cache, vec![9, 3], 42).await
        else {
            panic!("a matching view hash streams");
        };
        assert_eq!(order, vec![9, 3], "the default keeps request order");
        assert_eq!(parts, 2 * crate::store::PART_COUNT as u64);
        let (id, recs) = chunks.next().await.expect("the fixture's one chunk");
        assert_eq!((id, recs.len()), (9, 1));
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
            parts: PartId::all().take(3).collect(),
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

    #[cfg(feature = "spill")]
    #[tokio::test]
    async fn pull_request_marks_a_warm_reloaded_bucket_servable_when_owned_alone() {
        let cluster = solo_cluster("rebalance-unit-test-warm-alone").await;
        let name = SmolStr::new("prices");
        let k = NonZeroU8::new(2).expect("nonzero");
        let (tracker, _tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );

        // Self-only view: no co-owner ever joined the cluster.
        let bucket = tracker
            .current()
            .owned_parts()
            .next()
            .expect("self owns at least one bucket alone");
        let residency = Arc::new(ResidencySet::new());
        // Stands in for attach_spill_and_record_warm's marks after a warm reopen.
        residency.mark_cold(&[bucket]);
        residency.mark_unverified(&[bucket]);

        let outcome = PullRequest {
            cluster: &cluster,
            shard: &empty_shard(),
            ownership: &tracker,
            residency: &residency,
            cache: &name,
            parts: vec![bucket],
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
        assert!(
            !residency.is_unverified(bucket),
            "a sole-owned warm-reloaded bucket is servable too: with nobody to verify it \
             against, the replayed data is the best available answer, exactly the same \
             decision that already clears the cold mark"
        );

        cluster.shutdown().await;
    }

    #[cfg(feature = "spill")]
    #[tokio::test]
    async fn pull_request_leaves_a_warm_reloaded_bucket_cold_when_owned_alone_is_untrusted() {
        // Same fixture as the servable test above, but trust_sole_owner:
        // false, the shape open() passes when a warm reopen's membership
        // wait timed out.
        let cluster = solo_cluster("rebalance-unit-test-warm-alone-untrusted").await;
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
            .owned_parts()
            .next()
            .expect("self owns at least one bucket alone");
        let residency = Arc::new(ResidencySet::new());
        residency.mark_cold(&[bucket]);
        residency.mark_unverified(&[bucket]);

        let outcome = PullRequest {
            cluster: &cluster,
            shard: &empty_shard(),
            ownership: &tracker,
            residency: &residency,
            cache: &name,
            parts: vec![bucket],
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
            "untrusted, this bucket stays cold: the caller's ordinary warm-up retries get the \
             final say once a real answer lands, instead of this pull trusting an unverified \
             sole-owner echo outright"
        );
        assert!(
            residency.is_unverified(bucket),
            "untrusted, a warm-reloaded bucket's replay also stays unverified: nobody has \
             vouched for it yet, and this view's \"nobody to ask\" answer is not proof that \
             nobody exists"
        );

        cluster.shutdown().await;
    }

    #[cfg(feature = "spill")]
    #[tokio::test]
    async fn warm_up_task_marks_a_warm_reloaded_bucket_servable_once_its_pulls_give_up() {
        let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-warm-up-unverified")
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
        // The phantom co-owner is already in the first view read, so
        // every owned bucket is co-owned from attempt 1.
        let phantom = NodeId::from(u64::MAX);
        let phantom_view = Arc::new(view(cluster.node_id(), vec![cluster.node_id(), phantom], 2));
        tx.send(Arc::clone(&phantom_view))
            .expect("receiver still alive");

        let bucket = phantom_view
            .owned_parts()
            .next()
            .expect("self owns at least one bucket");
        let residency = Arc::new(ResidencySet::new());
        residency.mark_cold(&[bucket]);
        residency.mark_unverified(&[bucket]);

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
        assert!(
            !residency.is_unverified(bucket),
            "the give-up path clears unverified alongside cold: once the warm-up attempts run \
             out with nobody left to verify against, the replayed data is the best available \
             answer"
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
            parts: vec![PartId::from_raw(0)],
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

        // Bucket 0 completes; bucket 1's chunk lands but the donor then
        // stalls, a stand-in for a mid-group connection drop.
        let donor_handler = Arc::new(BucketPullHandler {
            view_hash,
            chunks: vec![
                (0u16, vec![sample_wire_record(1)]),
                (1u16, vec![sample_wire_record(2)]),
            ],
            stall_after: true,
            requested: Default::default(),
        });
        let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
        let requester_handler = Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
            requested: Default::default(),
        });
        let (requester, _requester_inbound) = spawn_mesh(requester_node, requester_handler).await;
        requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

        let residency = Arc::new(ResidencySet::new());
        residency.mark_cold(&two_buckets());
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
                two_buckets(),
                (Granularity::Bucket, view_hash),
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
            PartId::of_bucket(0).all(|p| !residency.is_cold(p)),
            "bucket 0's BucketDone landed and cleared its cold mark before the group finished"
        );
        assert!(
            PartId::of_bucket(1).all(|p| residency.is_cold(p)),
            "bucket 1's BucketDone never arrived, so it stays cold"
        );

        donor.shutdown().await;
        requester.shutdown().await;
    }

    #[test]
    fn acked_by_part_rekeys_each_id_by_the_parts_it_names() {
        let a = NodeId::from(1);
        let b = NodeId::from(2);
        let by_bucket = acked_by_part(
            Granularity::Bucket,
            HashMap::from([(3u16, HashSet::from([a]))]),
        );
        assert_eq!(by_bucket.len(), crate::store::PART_COUNT);
        assert!(
            by_bucket
                .iter()
                .all(|(part, owners)| part.bucket() == 3 && owners == &HashSet::from([a]))
        );

        let part = PartId::new(3, 5);
        let by_part = acked_by_part(
            Granularity::Part,
            HashMap::from([(part.raw(), HashSet::from([a, b]))]),
        );
        assert_eq!(by_part, HashMap::from([(part, HashSet::from([a, b]))]));
    }

    /// A donor that stalls after finishing bucket 0 leaves bucket 1 for the
    /// next donor, which is asked for bucket 1 alone.
    #[tokio::test]
    async fn pull_one_group_asks_a_later_donor_only_for_the_parts_no_earlier_attempt_finished() {
        use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

        let cache = SmolStr::new("prices");
        let view_hash = 42;
        let (first_node, second_node, requester_node) =
            (NodeId::from(301), NodeId::from(302), NodeId::from(303));
        let first_handler = Arc::new(BucketPullHandler {
            view_hash,
            chunks: vec![
                (0u16, vec![sample_wire_record(1)]),
                (1u16, vec![sample_wire_record(2)]),
            ],
            stall_after: true,
            requested: Default::default(),
        });
        let second_handler = Arc::new(BucketPullHandler {
            view_hash,
            chunks: vec![(1u16, vec![sample_wire_record(2)])],
            stall_after: false,
            requested: Default::default(),
        });
        let (first, _first_inbound) = spawn_mesh(first_node, Arc::clone(&first_handler) as _).await;
        let (second, _second_inbound) =
            spawn_mesh(second_node, Arc::clone(&second_handler) as _).await;
        let (requester, _requester_inbound) = spawn_mesh(
            requester_node,
            Arc::new(BucketPullHandler {
                view_hash: 0,
                chunks: Vec::new(),
                stall_after: false,
                requested: Default::default(),
            }),
        )
        .await;
        requester.update_peers(vec![
            peer_at(first_node, first.local_addr()),
            peer_at(second_node, second.local_addr()),
        ]);
        let residency = Arc::new(ResidencySet::new());
        residency.mark_cold(&two_buckets());
        let modes: crate::membership::CacheModes = std::collections::HashMap::new();
        let k = NonZeroU8::new(2).expect("nonzero");
        let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

        let landed = pull_one_group(
            &empty_shard(),
            &requester,
            &cache,
            &ownership,
            &residency,
            vec![first_node, second_node],
            two_buckets(),
            (Granularity::Bucket, view_hash),
            Duration::from_millis(300),
        )
        .await;
        assert_eq!(landed, Some(2 * crate::store::PART_COUNT as u64));
        assert_eq!(
            *second_handler.requested.lock().expect("fixture mutex"),
            vec![vec![1u16]],
            "the second donor is asked only for the bucket the first never finished"
        );
        assert!(
            two_buckets()
                .into_iter()
                .all(|part| !residency.is_cold(part))
        );

        first.shutdown().await;
        second.shutdown().await;
        requester.shutdown().await;
    }

    /// A part-granular pull of thousands of parts, most of them empty,
    /// finishes every part, and the donor records an ack for every one:
    /// done frames and acks both cross their batch sizes.
    #[tokio::test]
    async fn a_pull_of_thousands_of_parts_finishes_and_acks_every_part() {
        use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

        const PARTS: usize = 3_000;
        let cache = SmolStr::new("prices");
        let view_hash = 42;
        let (donor_node, requester_node) = (NodeId::from(401), NodeId::from(402));
        let parts: Vec<PartId> = PartId::all().step_by(7).take(PARTS).collect();
        let first = parts[0];
        let (donor, _donor_inbound) = spawn_mesh(
            donor_node,
            Arc::new(BucketPullHandler {
                view_hash,
                chunks: vec![(first.raw(), vec![sample_wire_record(1)])],
                stall_after: false,
                requested: Default::default(),
            }),
        )
        .await;
        let (requester, _requester_inbound) = spawn_mesh(
            requester_node,
            Arc::new(BucketPullHandler {
                view_hash: 0,
                chunks: Vec::new(),
                stall_after: false,
                requested: Default::default(),
            }),
        )
        .await;
        requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);
        donor.update_peers(vec![peer_at(requester_node, requester.local_addr())]);
        let residency = Arc::new(ResidencySet::new());
        residency.mark_cold(&parts);
        let modes: crate::membership::CacheModes = std::collections::HashMap::new();
        let k = NonZeroU8::new(2).expect("nonzero");
        let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

        let landed = pull_one_group(
            &empty_shard(),
            &requester,
            &cache,
            &ownership,
            &residency,
            vec![donor_node],
            parts.clone(),
            (Granularity::Part, view_hash),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(landed, Some(PARTS as u64));
        assert!(parts.iter().all(|&part| !residency.is_cold(part)));

        let ids: Vec<u16> = parts.iter().map(|part| part.raw()).collect();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let acked = donor.acked_owners(&cache, &ids, Duration::from_secs(60), view_hash);
            if acked.len() == PARTS {
                assert!(
                    acked
                        .values()
                        .all(|owners| owners.contains(&requester_node))
                );
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "only {} of {PARTS} parts acked",
                acked.len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        donor.shutdown().await;
        requester.shutdown().await;
    }

    /// Pins that `credited`, threaded across two donor calls the way
    /// `pull_one_group` does, counts bucket 0 once even though the first
    /// donor stalls after landing it and the second donor re-sends it:
    /// guards the regression where a donor retry double-counted a bucket.
    #[tokio::test]
    async fn try_donor_parts_shares_credited_parts_across_repeated_calls_for_one_group() {
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
            requested: Default::default(),
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
            requested: Default::default(),
        });
        let (second_donor, _second_donor_inbound) =
            spawn_mesh(second_donor_node, second_donor_handler).await;
        let requester_handler = Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
            requested: Default::default(),
        });
        let (requester, _requester_inbound) = spawn_mesh(requester_node, requester_handler).await;
        requester.update_peers(vec![
            peer_at(first_donor_node, first_donor.local_addr()),
            peer_at(second_donor_node, second_donor.local_addr()),
        ]);

        let residency = ResidencySet::new();
        residency.mark_cold(&two_buckets());
        let shard = empty_shard();
        let mut credited: HashSet<PartId> = HashSet::new();

        // Mirrors pull_one_group's per-donor timeout; the first donor
        // stalls after bucket 1's chunk.
        let first_call = tokio::time::timeout(
            Duration::from_millis(200),
            try_donor_parts(
                &shard,
                &requester,
                &cache,
                &residency,
                first_donor_node,
                &two_buckets(),
                (Granularity::Bucket, view_hash),
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
            PartId::of_bucket(0).collect::<HashSet<_>>(),
            "bucket 0's BucketDone landed (flushed once bucket 1's chunk proved bucket 0 is \
             behind it) and was credited before the stall; bucket 1's own BucketDone never \
             arrived"
        );

        let (result_second, count_second, cold_second) = try_donor_parts(
            &shard,
            &requester,
            &cache,
            &residency,
            second_donor_node,
            &two_buckets(),
            (Granularity::Bucket, view_hash),
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
            two_buckets().into_iter().collect::<HashSet<_>>(),
            "bucket 0 stays credited exactly once even though the second donor redelivered its \
             BucketDone; only bucket 1 is newly credited"
        );

        first_donor.shutdown().await;
        second_donor.shutdown().await;
        requester.shutdown().await;
    }

    /// Every part of buckets 0 and 1: what a bucket-mode view's pull of
    /// those two buckets covers.
    fn two_buckets() -> Vec<PartId> {
        PartId::of_bucket(0).chain(PartId::of_bucket(1)).collect()
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
