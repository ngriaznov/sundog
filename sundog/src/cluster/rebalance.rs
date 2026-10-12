//! Rebalance for a `Mode::Distributed` cache: pulls a part from its
//! current owners the moment this node's [`OwnershipView`] says it gained
//! it, and releases a part's local data once this node has kept it
//! resident past the disown grace after losing it. The open()-time initial
//! pull and this module's ongoing loop share one mechanism, [`PullRequest`],
//! scoped to whichever part set is at hand.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use smol_str::SmolStr;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::anti_entropy::{self, RoundOutcome};
use super::state_transfer::{self, DonorResult, Outcome};
use super::{Cluster, group_in_order};
use crate::net::{BucketPull, Mesh};
use crate::node::NodeId;
use crate::ownership::{
    Granularity, OwnershipTracker, OwnershipView, PullTicket, ResidencySet, ownership_diff,
    parts_of_wire_id, wire_ids,
};
use crate::store::part::PartSet;
use crate::store::{PartId, ShardOps};

/// One pull group: the donors to try, in rendezvous order, and the parts
/// they all co-own.
type DonorGroup = (Vec<NodeId>, Vec<PartId>);

/// The parts of `alone`, each owned by this node alone under `view`, that no
/// member of `absent` would co-own once eligible again: nobody else holds a
/// copy, so each serves as it stands. A part an absent member would co-own
/// stays cold. The failure detector drops a node that is stalled or on the
/// far side of a partition as readily as one that crashed, and such a node
/// still holds every key of the part, so a local miss here would answer
/// `None` for a key it holds.
pub(crate) fn sole_owned(
    view: &OwnershipView,
    alone: impl IntoIterator<Item = PartId>,
    absent: &[NodeId],
) -> Vec<PartId> {
    let Some(widened) = view.widened(absent) else {
        return alone.into_iter().collect();
    };
    alone
        .into_iter()
        .filter(|&part| {
            !widened
                .owners_of(part)
                .iter()
                .any(|owner| absent.contains(owner))
        })
        .collect()
}

/// Groups `parts` by exact donor set (`view`'s live owners minus self, in
/// rendezvous order), so parts sharing a donor set land in one `StBuckets`
/// round trip; the part-scoped analogue of `cluster::group_by_owner_set`.
fn group_parts_by_donor_set(
    view: &OwnershipView,
    self_node: NodeId,
    parts: Vec<PartId>,
) -> Vec<DonorGroup> {
    // Groups by the view's own owner slice first, borrowed rather than
    // rebuilt per part; the few resulting slices then merge by donor set,
    // since two rankings that differ only in where self sits name the
    // same donors.
    let by_owners = group_in_order(parts.into_iter().map(|part| (view.owners_of(part), [part])));
    group_in_order(by_owners.into_iter().map(|(owners, parts)| {
        let donors: Vec<NodeId> = owners.iter().copied().filter(|&n| n != self_node).collect();
        (donors, parts)
    }))
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
    ticket: &PullTicket,
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
            ticket.mark_serving(&done);
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
/// the pull gives the group up: nobody warm holds the parts, so what has
/// landed here by other means is all there is.
const ALL_COLD_PASSES: u32 = 3;

/// Anti-entropy rounds [`converge_with_cold_donors`] runs against each cold
/// donor before a group every donor declined settles.
const COLD_DONOR_ROUNDS: u32 = 3;

/// Converges `parts` with each of `donors` by anti-entropy, which a donor
/// answers whether or not it is cold: a donor still pulling these parts
/// itself can hold writes that reached it meanwhile, and a donor that
/// declines a whole group as cold for one cold part still holds the rest.
/// Up to [`COLD_DONOR_ROUNDS`] rounds per donor, each bounded by
/// `per_donor`, until every part matches or a round fails. Answers whether
/// every donor was reached: a failed or timed-out round leaves that donor's
/// copy unmerged, so the group must not settle on what landed here.
async fn converge_with_cold_donors(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    donors: &[NodeId],
    parts: &[PartId],
    per_donor: Duration,
) -> bool {
    let mut reached_all = true;
    for &donor in donors {
        let mut diverging = parts.to_vec();
        let mut reached = true;
        for _ in 0..COLD_DONOR_ROUNDS {
            let outcome = tokio::time::timeout(
                per_donor,
                anti_entropy::run_round_for_parts(mesh, shard, cache, donor, &diverging),
            )
            .await
            .ok();
            match cold_round(outcome, &mut diverging) {
                ColdRound::Unreached => {
                    reached = false;
                    break;
                }
                ColdRound::Matched => break,
                ColdRound::Diverging => {}
            }
        }
        tracing::debug!(cache = %cache, %donor, parts = parts.len(), diverging = diverging.len(), reached, "converged with a cold donor by anti-entropy");
        reached_all &= reached;
    }
    reached_all
}

/// Where one anti-entropy round against a cold donor leaves
/// [`converge_with_cold_donors`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColdRound {
    /// The round failed or ran out of time: nothing says what the donor holds.
    Unreached,
    /// Every part still diverging compared equal.
    Matched,
    /// The round reconciled what it found, and some parts still differ.
    Diverging,
}

/// Folds one round's `outcome` (`None` for a round that ran out of time)
/// into `diverging`, the parts not yet seen equal on the donor.
fn cold_round(
    outcome: Option<anti_entropy::PartRoundOutcome>,
    diverging: &mut Vec<PartId>,
) -> ColdRound {
    let Some(outcome) = outcome.filter(|outcome| !outcome.failed) else {
        return ColdRound::Unreached;
    };
    diverging.retain(|part| !outcome.matched.contains(part));
    if diverging.is_empty() {
        ColdRound::Matched
    } else {
        ColdRound::Diverging
    }
}

/// Delay between retry passes over a group's whole donor list once every
/// candidate has declined or failed once: long enough for both sides'
/// `refresh_task`s to have a real chance at converging their views before
/// trying again.
const GROUP_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// How one donor group's pull ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupPull {
    /// Every part is served: this many landed from a donor, or none when
    /// every donor stayed cold and what is here is all there is.
    Landed(u64),
    /// This node's view moved past the one the pull planned against.
    Superseded,
    /// Every donor stayed cold and some parts are still unverified: they
    /// stay cold and unverified for the warm-up retries.
    Unverified,
}

/// What a group whose donors all stayed cold settles to: the parts that
/// serve what landed here, since nothing warm exists to pull, and whether
/// any part still waits because nothing has verified its replay.
fn settle_all_cold(
    parts: &[PartId],
    unverified: impl Fn(PartId) -> bool,
) -> (Vec<PartId>, GroupPull) {
    let (waiting, settled): (Vec<PartId>, Vec<PartId>) =
        parts.iter().partition(|&&part| unverified(part));
    let pull = if waiting.is_empty() {
        GroupPull::Landed(0)
    } else {
        GroupPull::Unverified
    };
    (settled, pull)
}

/// Tries `donors` in order for one part group, applying the first that
/// donates. A `StaleView` decline is expected while both sides'
/// `refresh_task`s converge after a membership change (see
/// [`crate::store::ShardOps::ae_peer_filter`]), so an all-declined pass
/// retries after [`GROUP_RETRY_BACKOFF`] rather than giving up, unless this
/// node's own view has moved past `view_hash`: then no donor ever agrees,
/// and the pull answers [`GroupPull::Superseded`] for the caller to
/// replan against the current view. A group every donor keeps declining
/// as cold settles by [`settle_all_cold`] once
/// [`converge_with_cold_donors`] reaches every donor.
#[expect(
    clippy::too_many_arguments,
    reason = "each parameter is independent context one donor-group retry loop needs; grouping any subset into a struct would only rename the same eight pieces of state"
)]
async fn pull_one_group(
    shard: &Arc<dyn ShardOps>,
    mesh: &Mesh,
    cache: &SmolStr,
    ownership: &OwnershipTracker,
    ticket: &PullTicket,
    donors: Vec<NodeId>,
    parts: Vec<PartId>,
    view: (Granularity, u64),
    per_donor: Duration,
) -> GroupPull {
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
                    ticket,
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
                ticket.mark_serving(&parts);
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
                return GroupPull::Landed(u64::try_from(group_len).unwrap_or(u64::MAX));
            }
            every_donor_cold &= cold;
        }
        if every_donor_cold {
            all_cold_passes += 1;
            if all_cold_passes >= ALL_COLD_PASSES {
                // No warm copy anywhere to pull. A cold donor can still
                // hold writes that reached it while its own pull ran, so
                // converge with each first; a cold part then serves what
                // landed here, while a warm-reloaded part nothing has
                // verified keeps waiting, since its replay can lack a key
                // an owner holds and a read would answer a wrong miss.
                tracing::debug!(cache = %cache, parts = parts.len(), "every donor is cold for these parts; nothing warm to pull");
                if converge_with_cold_donors(shard, mesh, cache, &donors, &parts, per_donor).await {
                    let (settled, pull) =
                        settle_all_cold(&parts, |part| ticket.residency().is_replayed(part));
                    ticket.mark_serving(&settled);
                    return pull;
                }
                // A donor this node never reached can hold the only copy of
                // a key, so the group stays cold and starts its passes over.
                all_cold_passes = 0;
            }
        } else {
            all_cold_passes = 0;
        }
        if ownership.current().view_hash() != view_hash {
            tracing::debug!(cache = %cache, "ownership view moved mid-pull; this pull is superseded");
            return GroupPull::Superseded;
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

        // Opened before the view is read, so any view published after the
        // one this pull plans against counts against the ticket.
        let ticket = Arc::new(residency.open_pull());
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
            let absent = cluster.away_within(budget);
            ticket.mark_serving(&sole_owned(&view, alone, &absent));
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
                let ticket = Arc::clone(&ticket);
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
                        &ticket,
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
            // this loop only folds each group's end into the Outcome.
            let (mut superseded, mut unverified) = (false, false);
            while let Some(result) = set.join_next().await {
                match result {
                    Ok(GroupPull::Landed(_)) => {}
                    Ok(GroupPull::Unverified) => unverified = true,
                    Ok(GroupPull::Superseded) | Err(_) => superseded = true,
                }
            }
            if superseded {
                Outcome::Superseded
            } else if unverified {
                Outcome::Unverified
            } else {
                Outcome::Completed
            }
        };

        tokio::time::timeout(budget, run_all)
            .await
            .unwrap_or(Outcome::TimedOut)
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
/// current one instead of skipped, plus every part `new` owns that was
/// regained since `prev` or in `repull`: dropped by a published view since
/// the last change, or planned by a pull that did not land, since writes to
/// it went elsewhere meanwhile or it still lacks them. `stale` is every
/// part in `to_pull` that `held` already has records in: a copy kept from
/// an earlier stint of owning it can lack a write or delete another owner
/// took meanwhile, so it is not trusted until the pull lands.
pub(crate) struct ViewChangePlan {
    pub(crate) lost: Vec<PartId>,
    pub(crate) regained: Vec<PartId>,
    pub(crate) to_pull: Vec<PartId>,
    pub(crate) stale: Vec<PartId>,
}

/// Marks the parts `plan` pulls cold, and its `stale` parts stale as well:
/// a local hit there is not an answer until the pull lands and
/// [`ResidencySet::mark_serving`] clears both marks.
pub(crate) fn mark_pulling(residency: &ResidencySet, plan: &ViewChangePlan) {
    residency.mark_cold(&plan.to_pull);
    residency.mark_stale(&plan.stale);
}

pub(crate) fn plan_view_change(
    prev: &OwnershipView,
    pulled: &OwnershipView,
    new: &OwnershipView,
    held: &[PartId],
    repull: &PartSet,
) -> ViewChangePlan {
    let (regained, mut lost) = ownership_diff(prev, new);
    let mut seen: PartSet = lost.iter().copied().collect();
    lost.extend(
        held.iter()
            .copied()
            .filter(|&part| !new.owns(part) && seen.insert(part)),
    );
    // A part regained since `prev`, or in `repull` (which covers a view this
    // task never saw, and a pull the view moved past), lacks writes another
    // node took meanwhile, even when `pulled` still owned it.
    let (mut to_pull, _) = ownership_diff(pulled, new);
    let mut planned: PartSet = to_pull.iter().copied().collect();
    to_pull.extend(
        regained
            .iter()
            .copied()
            .chain(repull.iter().filter(|&part| new.owns(part)))
            .filter(|&part| planned.insert(part)),
    );
    let held: PartSet = held.iter().copied().collect();
    let stale = to_pull
        .iter()
        .copied()
        .filter(|&part| held.contains(part))
        .collect();
    ViewChangePlan {
        lost,
        regained,
        to_pull,
        stale,
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

/// Each owner of `parts` under `view` other than `self_node`, paired with
/// the parts among `parts` it owns, so one scoped round per owner covers
/// all of them. An owner gossip has dropped for the moment is listed too:
/// [`push_unpullable`] waits for it rather than leaving its parts behind.
pub(crate) fn push_targets(
    view: &OwnershipView,
    self_node: NodeId,
    parts: &[PartId],
) -> PushTargets {
    let mut targets: PushTargets = Vec::new();
    for &part in parts {
        for &owner in view.owners_of(part) {
            if owner == self_node {
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

/// Whether a retry `retry` after `now` would land past `give_up`. `None`
/// never gives up; a retry too far out to represent lands past any
/// deadline.
fn out_of_time(
    now: tokio::time::Instant,
    retry: Duration,
    give_up: Option<tokio::time::Instant>,
) -> bool {
    give_up.is_some_and(|give_up| now.checked_add(retry).is_none_or(|next| next > give_up))
}

/// Push targets: each owner with the parts pushed to it.
type PushTargets = Vec<(NodeId, Vec<PartId>)>;

/// Splits `targets` into those `live` names, which a push round can reach
/// now, and those gossip has dropped for the moment, which wait for the
/// next attempt.
fn split_reachable(targets: PushTargets, live: &HashSet<NodeId>) -> (PushTargets, PushTargets) {
    targets
        .into_iter()
        .partition(|(owner, _)| live.contains(owner))
}

/// Pushes each target's parts to it through a part-scoped anti-entropy
/// round, which sends every entry the target lacks or holds at an older
/// version. A target missing from gossip, which a failure-detector flap
/// causes as well as a departure, waits without a round; a round that
/// fails, or meets a responder whose view has not caught up with this
/// node's, is retried. Either way the target is tried again every `retry`
/// until every target has answered, `deadline` passes, or `cancel` fires.
/// The disown-grace hand-off still confirms each part before it is
/// released; this push brings the data to the new owners when the view
/// changes rather than when the grace ends.
async fn push_unpullable(
    cluster: Cluster,
    shard: Arc<dyn ShardOps>,
    cache: SmolStr,
    targets: PushTargets,
    retry: Duration,
    deadline: Duration,
    cancel: CancellationToken,
) {
    // `None` for a deadline too far out to represent: never give up.
    let give_up = tokio::time::Instant::now().checked_add(deadline);
    let mut targets = targets;
    loop {
        let live: HashSet<NodeId> = cluster.peers().iter().map(|peer| peer.node).collect();
        let (reachable, mut pending) = split_reachable(targets, &live);
        for (owner, parts) in reachable {
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
        if out_of_time(tokio::time::Instant::now(), retry, give_up) {
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
    // Parts the last pull planned that never landed.
    let mut unfinished = PartSet::new();
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
                let mut repull = residency.take_lost();
                repull.extend(unfinished.iter());
                let held = shard.held_parts().await;
                let plan = plan_view_change(&prev_view, &pulled_view, &new_view, &held, &repull);
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
                // record against, so it is neither cold nor unverified,
                // unless a member the failure detector dropped would co-own
                // it: see `sole_owned`.
                let alone = sole_owned(
                    &new_view,
                    new_view
                        .owned_parts()
                        .filter(|&part| new_view.owners_of(part).len() == 1),
                    &cluster.away_within(budget),
                );
                if !alone.is_empty() {
                    residency.mark_serving(&alone);
                }
                if !plan.lost.is_empty() {
                    residency.mark_releasing(&plan.lost);
                    tracing::debug!(cache = %cache, count = plan.lost.len(), "parts lost; disown grace started");
                }
                let targets = push_targets(&new_view, cluster.node_id(), &unpullable);
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
                let planned = plan.to_pull.clone();
                let outcome = if plan.to_pull.is_empty() {
                    Outcome::Completed
                } else {
                    // Gained from a co-owner: cold until the pull lands,
                    // and unverified where a stale copy is already here.
                    mark_pulling(&residency, &plan);
                    tracing::debug!(cache = %cache, count = plan.to_pull.len(), "parts gained; pulling from current owners");
                    let pull = PullRequest {
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
                    };
                    // A pull retrying against donors that died before the
                    // failure detector noticed runs until `budget`; shutdown
                    // must not wait that out.
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => return,
                        outcome = pull.run() => outcome,
                    }
                };
                // What the pull planned and did not land is pulled again on
                // the next change; a pull the view moves past is also planned
                // again from the same starting point, which is already
                // published, so nothing gained is skipped.
                unfinished = planned
                    .into_iter()
                    .filter(|&part| residency.is_cold(part))
                    .collect();
                if outcome != Outcome::Superseded {
                    residency.settle(&new_view);
                    pulled_view = new_view;
                }
            }
            _ = ticker.tick() => {
                // A part held cold for an absent co-owner serves once that
                // absence outlasts `budget`, the same bound a pull waits for
                // a donor, or is pulled on the view change its return brings.
                if !unfinished.is_empty() {
                    let view = ownership.current();
                    let held: Vec<PartId> = unfinished
                        .iter()
                        .filter(|&part| view.owners_of(part).len() == 1 && view.owns(part))
                        .collect();
                    if !held.is_empty() {
                        let serving = sole_owned(
                            &view,
                            held,
                            &cluster.away_within(budget),
                        );
                        if !serving.is_empty() {
                            residency.mark_serving(&serving);
                            for part in serving {
                                unfinished.remove(part);
                            }
                        }
                    }
                }
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
                    let overdue: PartSet = residency.expired(disown_grace.saturating_mul(2)).into_iter().collect();
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
mod tests;
