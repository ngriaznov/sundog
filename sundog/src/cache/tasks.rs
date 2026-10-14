//! The background loops a cache spawns when it opens: fan-out, merge coalescing, the clock-skew
//! hold's release, refresh-ahead, warm-up and rebalancing.

use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cluster::Cluster;
use crate::store::{Mode, PartId, Shard, ShardOps, now_ms};

use super::load::refresh_task;
#[cfg(doc)]
use super::open::await_initial_peers;
use super::open::{DistributedContext, trust_sole_owner_at_open};
use super::reconcile::reconcile_warm_buckets;
#[cfg(doc)]
use super::{Cache, CacheBuilder};

/// The background loops one opened cache runs: fan-out for a clustered
/// mode, warm-up and anti-entropy for `Replicated`, the analogous
/// part-scoped pull, refresh, and rebalance loops for `Distributed`,
/// tombstone GC, and the entry gauge, all under `cancel` and tracked by
/// `tasks`. `membership_settled` is `open()`'s own [`await_initial_peers`]
/// outcome, used only by [`distributed_warm_and_rebalance`]'s sole-owner shortcut.
#[expect(
    clippy::too_many_arguments,
    reason = "each parameter is independent context `open()` already has to hand; a struct would \
              only rename these same eight fields"
)]
pub(super) async fn spawn_cache_tasks<K, V>(
    cluster: &Cluster,
    shard: &Arc<Shard<K, V>>,
    name: &SmolStr,
    mode: Mode,
    cancel: &CancellationToken,
    tasks: &TaskTracker,
    distributed: Option<DistributedContext>,
    membership_settled: bool,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    if !matches!(mode, Mode::Local) {
        cluster.spawn_tracked_in(
            tasks,
            crate::cluster::fan_out::fan_out_task(
                Arc::clone(shard),
                cluster.clone(),
                shard.fan_out_queue(),
                name.clone(),
                mode,
                cancel.clone(),
            ),
        );
    }
    if matches!(mode, Mode::Replicated) {
        warm_and_repair(
            cluster,
            Arc::clone(shard) as Arc<dyn ShardOps>,
            name,
            cancel.clone(),
            tasks,
        )
        .await;
    }
    if let Some(distributed) = distributed {
        distributed_warm_and_rebalance(
            cluster,
            Arc::clone(shard) as Arc<dyn ShardOps>,
            name,
            distributed,
            membership_settled,
            cancel.clone(),
            tasks,
        )
        .await;
    }
    cluster.spawn_tracked_in(
        tasks,
        crate::cluster::tombstone_gc_task(
            Arc::clone(shard) as Arc<dyn ShardOps>,
            mode,
            cluster.config().tombstone_ttl,
            cluster.config().tombstone_max_ttl,
            cluster.absence_tracker(),
            cancel.clone(),
        ),
    );
    // Only a merging cache ever compacts (`ConflictResolver::compact` is a
    // no-op for every other resolver), so a plain LWW cache never pays for
    // a ticker that would never do anything, the same gating
    // `cluster::anti_entropy` already applies to its own merge-aware
    // exchange path via `ShardOps::merges`.
    if (Arc::clone(shard) as Arc<dyn ShardOps>).merges() {
        cluster.spawn_tracked_in(
            tasks,
            crate::cluster::crdt_compact::crdt_compact_task(
                Arc::clone(shard) as Arc<dyn ShardOps>,
                name.clone(),
                cluster.clone(),
                cluster.absence_tracker(),
                cancel.clone(),
            ),
        );
    }
    cluster.spawn_tracked_in(
        tasks,
        crate::cluster::cache_entries_gauge_task(Arc::clone(shard), name.clone(), cancel.clone()),
    );
    if !shard.merge_window().is_zero() {
        cluster.spawn_tracked_in(
            tasks,
            merge_coalesce_task(Arc::clone(shard), cancel.clone()),
        );
    }
    if matches!(mode, Mode::Distributed { .. }) && shard.holds_skewed_forwards() {
        cluster.spawn_tracked_in(tasks, skew_hold_task(Arc::clone(shard), cancel.clone()));
    }
    if let Some(requests) = shard.take_refresh_requests() {
        cluster.spawn_tracked_in(
            tasks,
            refresh_task(Arc::clone(shard), cluster.clone(), requests, cancel.clone()),
        );
    }
}

/// Drives [`Shard::flush_due_pending_merges`] for a cache configured with
/// [`CacheBuilder::merge_coalesce_window`]: sleeps until the pending map's
/// earliest deadline (or, with nothing pending, until either a fresh
/// window opens via [`Shard::merge_wake_notified`] or `cancel` fires), then
/// flushes everything due and loops. [`Cache::close`]'s own explicit flush
/// and this shard's own `Drop` both guarantee every fold lands regardless,
/// so this task's timing is never load-bearing for correctness, only for
/// the staleness bound [`Cache::merge`]'s docs commit to.
async fn merge_coalesce_task<K, V>(shard: Arc<Shard<K, V>>, cancel: CancellationToken)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    loop {
        let sleep_for = match shard.next_pending_merge_deadline_ms() {
            Some(deadline_ms) => Duration::from_millis(deadline_ms.saturating_sub(now_ms())),
            // Nothing pending: park well past any realistic window rather
            // than busy-polling, but still wake at once on a fresh window
            // via the `select!` arm below.
            None => Duration::from_secs(3600),
        };
        tokio::select! {
            () = cancel.cancelled() => return,
            () = shard.merge_wake_notified() => {}
            () = tokio::time::sleep(sleep_for) => {
                shard.flush_due_pending_merges(now_ms());
            }
        }
    }
}

/// Drives [`Shard::release_due_skewed`] for a `Mode::Distributed` cache with a
/// `max_clock_skew`: parks on [`Shard::skew_wake_notified`] while the shard
/// holds no forward it refused for skew, so an idle cache costs no wake-ups.
/// With records held it sleeps until the earliest comes due, in slices of at
/// most [`Shard::next_skew_wait`]'s poll so a wall-clock step cannot strand a
/// release, and releases whatever is due. [`Cache::close`] hands the hold to
/// the fan-out before this task is cancelled, so its timing is never
/// load-bearing for a graceful close.
pub(super) async fn skew_hold_task<K, V>(shard: Arc<Shard<K, V>>, cancel: CancellationToken)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    loop {
        // A record held after the wait is computed leaves a permit with
        // `skew_wake`, so the select below wakes at once instead of missing it.
        match shard.next_skew_wait() {
            None => tokio::select! {
                () = cancel.cancelled() => return,
                () = shard.skew_wake_notified() => {}
            },
            Some(wait) => tokio::select! {
                () = cancel.cancelled() => return,
                () = shard.skew_wake_notified() => {}
                () = tokio::time::sleep(wait) => shard.release_due_skewed().await,
            },
        }
    }
}

/// The `Mode::Distributed`-only half of [`spawn_cache_tasks`]: pulls this
/// node's initially owned parts from their current owners before the
/// cache is marked warm, the part-scoped analogue of
/// [`warm_and_repair`]'s whole-cache transfer, then starts the
/// ownership-refresh and rebalance loops. An initial pull that times out,
/// or finds no co-owner because gossip has not shown a peer yet, leaves
/// warming to [`crate::cluster::rebalance::warm_up_task`], the same
/// "wait for a peer, retry a few times, then open warm with what landed"
/// shape [`state_transfer::warm_up_task`] already has for
/// `Mode::Replicated`. `membership_settled` is `open()`'s own
/// [`await_initial_peers`] outcome, fed into the initial pull's
/// `trust_sole_owner`: a cold open trusts sole ownership regardless of
/// it, but a warm reopen trusts it only once membership has settled, so
/// a timed-out wait never vouches for a replay's stale ownership echo.
///
/// [`state_transfer::warm_up_task`]: crate::cluster::state_transfer::warm_up_task
pub(super) async fn distributed_warm_and_rebalance(
    cluster: &Cluster,
    shard_ops: Arc<dyn ShardOps>,
    name: &SmolStr,
    distributed: DistributedContext,
    membership_settled: bool,
    cancel: CancellationToken,
    tasks: &TaskTracker,
) {
    let DistributedContext {
        ownership,
        view_tx,
        residency,
        owners,
        warm_reloaded_parts,
    } = distributed;

    let budget = cluster.config().state_transfer_budget;
    let concurrency = cluster.config().rebalance_concurrency;
    let disown_grace = cluster.config().disown_grace();
    // The refresh loop starts before the first pull, so a membership change
    // during it moves the view under the pull, which replans instead of stalling.
    cluster.spawn_tracked_in(
        tasks,
        crate::ownership::refresh_task(
            cluster.clone(),
            name.clone(),
            owners,
            view_tx,
            Arc::clone(&residency),
            cancel.clone(),
        ),
    );

    let initially_owned: Vec<PartId> = ownership.current().owned_parts().collect();
    // A warm-reloaded part starts as cold as any other: this eager
    // reconciliation round runs first, clearing cold only for a part whose
    // round against every live co-owner comes back reconciled. Everything
    // else falls to the ordinary cold-pull path below, same as with no spill tier.
    let warm_candidates: Vec<PartId> = initially_owned
        .iter()
        .copied()
        .filter(|part| warm_reloaded_parts.contains(part))
        .collect();
    let reconciled = reconcile_warm_buckets(
        cluster,
        &shard_ops,
        name,
        &ownership,
        &residency,
        &warm_candidates,
    )
    .await;

    cluster.spawn_tracked_in(
        tasks,
        crate::cluster::rebalance::rebalance_task(
            cluster.clone(),
            Arc::clone(&shard_ops),
            ownership.clone(),
            Arc::clone(&residency),
            name.clone(),
            disown_grace,
            concurrency,
            cancel.clone(),
        ),
    );

    let pull_parts: Vec<PartId> = initially_owned
        .into_iter()
        .filter(|part| !reconciled.contains(part))
        .collect();
    let outcome = crate::cluster::rebalance::PullRequest {
        cluster,
        shard: &shard_ops,
        ownership: &ownership,
        residency: &residency,
        cache: name,
        parts: pull_parts,
        budget,
        concurrency,
        // See `trust_sole_owner_at_open`: a cold open trusts sole ownership
        // outright; a warm reopen trusts it only once membership_settled too,
        // else the part stays cold for `warm_up_task`'s ordinary retries.
        trust_sole_owner: trust_sole_owner_at_open(membership_settled, warm_reloaded_parts.len()),
    }
    .run()
    .await;
    if outcome.needs_warm_up() {
        cluster.spawn_tracked_in(
            tasks,
            crate::cluster::rebalance::warm_up_task(
                cluster.clone(),
                Arc::clone(&shard_ops),
                ownership,
                residency,
                name.clone(),
                budget,
                concurrency,
                cancel.clone(),
            ),
        );
    } else {
        cluster.mark_warm(name);
    }
    cluster.spawn_tracked_in(
        tasks,
        crate::cluster::anti_entropy::scheduler_task(
            cluster.clone(),
            shard_ops,
            name.clone(),
            cluster.config().ae_interval,
            cancel,
        ),
    );
}

/// The `Replicated`-only half of [`CacheBuilder::open`]: pulls the cache's
/// state from a live peer and starts the anti-entropy scheduler. Anything
/// short of a landed snapshot or a cluster with nothing to give leaves the
/// cache cold, declining to donate until the warm-up task gets it there.
async fn warm_and_repair(
    cluster: &Cluster,
    shard_ops: Arc<dyn ShardOps>,
    name: &SmolStr,
    cancel: CancellationToken,
    tasks: &TaskTracker,
) {
    let outcome = crate::cluster::state_transfer::run(cluster, &shard_ops, name).await;
    if outcome.needs_warm_up() {
        cluster.spawn_tracked_in(
            tasks,
            crate::cluster::state_transfer::warm_up_task(
                cluster.clone(),
                Arc::clone(&shard_ops),
                name.clone(),
                cancel.clone(),
            ),
        );
    }
    cluster.spawn_tracked_in(
        tasks,
        crate::cluster::anti_entropy::scheduler_task(
            cluster.clone(),
            shard_ops,
            name.clone(),
            cluster.config().ae_interval,
            cancel,
        ),
    );
}
