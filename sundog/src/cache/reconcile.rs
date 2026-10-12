//! Warm-reopen reconciliation: converges a reloaded part against its live co-owners before the
//! part serves.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use smol_str::SmolStr;

use crate::cluster::Cluster;
use crate::cluster::anti_entropy;
use crate::config::ClusterConfig;
use crate::node::NodeId;
use crate::ownership::{OwnershipTracker, ResidencySet};
use crate::store::PartId;
use crate::store::ShardOps;
use crate::store::part::PartSet;

/// Cap on rounds in [`reconcile_warm_buckets`]'s per-peer converge loop:
/// chances for a part still moving under live writes to settle, not chunks
/// of a fixed size. A failed or `Stale` round doesn't count; it retries.
pub(super) const RECONCILE_MAX_ROUNDS: u32 = 3;

/// Base retry delay after a failed round in [`reconcile_against_peer`];
/// doubles per consecutive failure, capped at [`ReconcileBudget::backoff_cap`].
const RECONCILE_RETRY_BASE: Duration = Duration::from_millis(200);

/// Per-peer bound on [`reconcile_warm_buckets`]'s converge loop: whichever
/// of `max_rounds`, `byte_budget` or `time_budget` is hit first stops it,
/// leaving that peer's still-diverging parts cold for the ordinary pull.
#[derive(Debug, Clone, Copy)]
pub(super) struct ReconcileBudget {
    pub(super) max_rounds: u32,
    pub(super) byte_budget: u64,
    pub(super) time_budget: Duration,
    pub(super) backoff_cap: Duration,
}

impl ReconcileBudget {
    /// Builds from `ClusterConfig`: `byte_budget`, `time_budget` (the same
    /// startup bound the initial pull honors), and `backoff_cap` from `ae_interval`.
    pub(super) fn from_config(config: &ClusterConfig) -> Self {
        Self {
            max_rounds: RECONCILE_MAX_ROUNDS,
            byte_budget: config.reconcile_byte_budget,
            time_budget: config.state_transfer_budget,
            backoff_cap: config.ae_interval,
        }
    }
}

/// Whether [`reconcile_warm_buckets`]'s per-peer loop keeps going, given
/// progress so far against `budget`; `still_diverging == 0` always stops it.
pub(super) fn should_keep_reconciling(
    rounds_run: u32,
    bytes_moved: u64,
    still_diverging: usize,
    elapsed: Duration,
    budget: &ReconcileBudget,
) -> bool {
    still_diverging > 0
        && rounds_run < budget.max_rounds
        && bytes_moved < budget.byte_budget
        && elapsed < budget.time_budget
}

/// How long [`reconcile_against_peer`] waits before its next retry:
/// [`RECONCILE_RETRY_BASE`] doubled per failure, capped at `backoff_cap`;
/// `None` once that wait would run past `budget.time_budget`.
pub(super) fn retry_delay(
    consecutive_failures: u32,
    elapsed: Duration,
    budget: &ReconcileBudget,
) -> Option<Duration> {
    let exponent = consecutive_failures.saturating_sub(1).min(31);
    let delay = RECONCILE_RETRY_BASE
        .saturating_mul(1_u32 << exponent)
        .min(budget.backoff_cap);
    let remaining = budget.time_budget.checked_sub(elapsed)?;
    (delay < remaining).then_some(delay)
}

/// Splits `requested` into (converged, still-diverging) from one round's
/// outcome; a `failed` round reports every part as still diverging.
pub(super) fn split_round_result(
    requested: &[PartId],
    outcome: &anti_entropy::PartRoundOutcome,
) -> (Vec<PartId>, Vec<PartId>) {
    if outcome.failed {
        return (Vec::new(), requested.to_vec());
    }
    requested
        .iter()
        .copied()
        .partition(|part| outcome.matched.contains(part))
}

/// One live co-owner's converge loop: repeatedly
/// [`anti_entropy::run_round_for_parts`] against `peer` over its
/// still-diverging subset of `parts`, marking each part serving as soon as
/// its digest matches, until nothing is left diverging or
/// [`should_keep_reconciling`]'s `budget` stops it. A failed or `Stale`
/// round retries via [`retry_delay`] without counting as a round. Returns
/// every part marked serving this way.
///
/// Factored out so [`reconcile_warm_buckets`] can run one of these per live
/// co-owner concurrently, bounding the wait by one peer's `budget` instead
/// of the sum across peers.
async fn reconcile_against_peer(
    cluster: &Cluster,
    shard_ops: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    residency: &Arc<ResidencySet>,
    peer: NodeId,
    parts: Vec<PartId>,
    budget: ReconcileBudget,
) -> HashSet<PartId> {
    let mesh = cluster.mesh();
    let started = Instant::now();
    let mut still_diverging = parts;
    let mut rounds_run: u32 = 0;
    let mut failed_in_a_row: u32 = 0;
    let mut bytes_moved: u64 = 0;
    let mut reconciled: HashSet<PartId> = HashSet::new();
    while should_keep_reconciling(
        rounds_run,
        bytes_moved,
        still_diverging.len(),
        started.elapsed(),
        &budget,
    ) {
        let outcome =
            anti_entropy::run_round_for_parts(mesh, shard_ops, cache, peer, &still_diverging).await;
        if outcome.failed {
            failed_in_a_row += 1;
        } else {
            rounds_run += 1;
            failed_in_a_row = 0;
        }
        bytes_moved += outcome.bytes_moved;
        let (converged, diverging) = split_round_result(&still_diverging, &outcome);
        if !converged.is_empty() {
            // Cleared per part once its digest matches, not held back for the rest.
            residency.mark_serving(&converged);
            reconciled.extend(converged.iter().copied());
        }
        tracing::info!(
            cache = %cache,
            peer = %peer,
            round = rounds_run,
            converged = converged.len(),
            still_diverging = diverging.len(),
            bytes_moved,
            failed = outcome.failed,
            failed_in_a_row,
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "sundog spill: warm-reopen reconciliation round against co-owner",
        );
        still_diverging = diverging;
        if outcome.failed {
            // Retried on a backoff, not counted against `max_rounds`. A peer
            // that left the live list has nobody to retry against.
            if !cluster.live_peer_ids().contains(&peer) {
                break;
            }
            match retry_delay(failed_in_a_row, started.elapsed(), &budget) {
                Some(delay) => tokio::time::sleep(delay).await,
                None => break,
            }
        }
    }
    // Every part still in `still_diverging` here gets no `ResidencySet`
    // call: it stays cold and unverified, falling to the ordinary cold-pull path.
    reconciled
}

/// Converge-before-serving reconciliation: groups `warm_parts` by live
/// co-owner, then runs [`reconcile_against_peer`]'s loop for each peer
/// concurrently over its parts, returning the union of parts marked
/// serving.
///
/// A warm-reloaded part's local state came from replaying on-disk records
/// that predate this restart and can include a value this node itself
/// deleted before going down (deletes never persist to the spill tier), so
/// it needs verification against a live co-owner rather than outright
/// trust. A part with no live co-owner, or one whose loop against a peer
/// never closed the gap, gets no [`ResidencySet`] call and stays cold and
/// unverified, falling to the caller's ordinary cold-pull path.
pub(super) async fn reconcile_warm_buckets(
    cluster: &Cluster,
    shard_ops: &Arc<dyn ShardOps>,
    cache: &SmolStr,
    ownership: &OwnershipTracker,
    residency: &Arc<ResidencySet>,
    warm_parts: &[PartId],
) -> HashSet<PartId> {
    if warm_parts.is_empty() {
        return HashSet::new();
    }

    let view = ownership.current();
    let self_node = cluster.node_id();
    let live: HashSet<NodeId> = cluster.live_peer_ids().into_iter().collect();

    // Grouped by live co-owner; a part with none is excluded, staying cold and unverified.
    let mut peer_parts: HashMap<NodeId, Vec<PartId>> = HashMap::new();
    let mut cold_left: usize = 0;
    for &part in warm_parts {
        let peers: Vec<NodeId> = view
            .owners_of(part)
            .iter()
            .copied()
            .filter(|&node| node != self_node && live.contains(&node))
            .collect();
        if peers.is_empty() {
            cold_left += 1;
            continue;
        }
        for peer in peers {
            peer_parts.entry(peer).or_default().push(part);
        }
    }

    let budget = ReconcileBudget::from_config(cluster.config());
    // Runs concurrently, not sequentially: warm parts can split across
    // several peers, and running loops one after another would multiply the
    // stall by peer count instead of bounding it by one `ReconcileBudget`.
    let per_peer = peer_parts.into_iter().map(|(peer, parts)| {
        reconcile_against_peer(cluster, shard_ops, cache, residency, peer, parts, budget)
    });
    let warm: PartSet = warm_parts.iter().copied().collect();
    let reconciled: HashSet<PartId> = futures::future::join_all(per_peer)
        .await
        .into_iter()
        .flatten()
        .filter(|&part| warm.contains(part))
        .collect();

    // Every part not marked serving stays cold and unverified: either its
    // loop never closed the gap (retried by `warm_up_task`), or it had no live co-owner.
    let unverified_left = warm_parts
        .len()
        .saturating_sub(reconciled.len())
        .saturating_sub(cold_left);
    tracing::info!(
        cache = %cache,
        marked_serving = reconciled.len(),
        unverified_left,
        cold_left,
        "sundog spill: warm-reopen reconciliation complete",
    );
    reconciled
}

/// Kani proofs over the reconciliation loop's bounds.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    fn any_duration() -> Duration {
        let secs: u64 = kani::any();
        let nanos: u32 = kani::any();
        kani::assume(nanos < 1_000_000_000);
        Duration::new(secs, nanos)
    }

    fn any_budget() -> ReconcileBudget {
        ReconcileBudget {
            max_rounds: kani::any(),
            byte_budget: kani::any(),
            time_budget: any_duration(),
            backoff_cap: any_duration(),
        }
    }

    /// A retry wait never exceeds the backoff cap, always ends inside the
    /// time budget, and is refused once the budget is spent.
    #[kani::proof]
    fn retry_delay_never_outruns_the_cap_or_the_time_budget() {
        let failures: u32 = kani::any();
        let elapsed = any_duration();
        let budget = any_budget();
        match retry_delay(failures, elapsed, &budget) {
            Some(delay) => {
                assert!(delay <= budget.backoff_cap);
                assert!(elapsed + delay < budget.time_budget);
            }
            None => {}
        }
        if elapsed >= budget.time_budget {
            assert!(retry_delay(failures, elapsed, &budget).is_none());
        }
    }

    /// The loop keeps going only with work left and every bound unspent,
    /// and stops the moment any one of them is hit.
    #[kani::proof]
    fn keep_reconciling_stops_at_every_bound() {
        let rounds_run: u32 = kani::any();
        let bytes_moved: u64 = kani::any();
        let still_diverging: usize = kani::any();
        let elapsed = any_duration();
        let budget = any_budget();
        let keep =
            should_keep_reconciling(rounds_run, bytes_moved, still_diverging, elapsed, &budget);
        let any_bound_hit = still_diverging == 0
            || rounds_run >= budget.max_rounds
            || bytes_moved >= budget.byte_budget
            || elapsed >= budget.time_budget;
        assert_eq!(keep, !any_bound_hit);
    }
}
