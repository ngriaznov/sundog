//! The checks and setup `CacheBuilder::open` runs before it spawns the cache's background loops.

use std::collections::HashSet;
use std::hash::Hash;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::watch;

use crate::cluster::Cluster;
use crate::config::ClusterConfig;
use crate::error::CacheError;
use crate::ownership::{OwnershipTracker, OwnershipView, ResidencySet};
#[cfg(feature = "spill")]
use crate::store::spill::SpillConfig;
use crate::store::{Mode, PartId, Shard, refresh_permille};

#[cfg(doc)]
use super::tasks::distributed_warm_and_rebalance;
#[cfg(doc)]
use super::{Cache, CacheBuilder};
#[cfg(doc)]
use crate::store::ConflictResolver;

/// [`CacheBuilder::open`]'s spill-attach step: a no-op when `spill` is
/// `None`, otherwise [`Shard::attach_spill`], recording the warm-reloaded
/// parts into `distributed`'s [`DistributedContext::warm_reloaded_parts`].
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] [`Shard::attach_spill`] returns.
#[cfg(feature = "spill")]
pub(super) fn attach_spill_and_record_warm<K, V>(
    shard: &Shard<K, V>,
    spill: Option<&SpillConfig>,
    distributed: &mut Option<DistributedContext>,
) -> Result<(), std::io::Error>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let Some(cfg) = spill else {
        return Ok(());
    };
    let outcome = shard.attach_spill(cfg)?;
    if let Some(ctx) = distributed.as_mut() {
        // Every part this warm reopen replayed is unverified until
        // `reconcile_warm_buckets` or an ordinary cold pull lands fresh data.
        let warm: Vec<PartId> = outcome.warm_parts.iter().copied().collect();
        ctx.residency.mark_unverified(&warm);
        ctx.warm_reloaded_parts = outcome.warm_parts;
    }
    Ok(())
}

/// Rejects a `Mode::Distributed` cache opened with `owners` under 2 or
/// under a `tombstone_ttl` shorter than
/// [`ClusterConfig::bucket_release_window`], and rejects a `Replicated` or
/// `Distributed` cache combined with `local_eviction`: any `tti`, or a
/// finite `max_capacity` with no spill tier configured. Anti-entropy would
/// silently re-pull an evicted entry back for either mode, since every owner
/// is expected to hold what it owns.
pub(super) fn validate_mode(
    name: &SmolStr,
    mode: Mode,
    local_eviction: bool,
    config: &ClusterConfig,
) -> Result<(), CacheError> {
    if let Mode::Distributed { owners } = mode {
        if owners.get() < 2 {
            return Err(CacheError::TooFewOwners {
                cache: name.clone(),
                owners,
            });
        }
        let window = config.bucket_release_window();
        if config.tombstone_ttl < window {
            return Err(CacheError::TombstoneTtlInsideReleaseWindow {
                cache: name.clone(),
                tombstone_ttl: config.tombstone_ttl,
                window,
            });
        }
    }
    if local_eviction && matches!(mode, Mode::Replicated | Mode::Distributed { .. }) {
        return Err(CacheError::ReplicatedWithLocalEviction {
            cache: name.clone(),
        });
    }
    Ok(())
}

/// Whether [`CacheBuilder::merge_coalesce_window`] combined with the
/// resolver's [`ConflictResolver::merges`] answer is a valid configuration:
/// a nonzero window needs a merging resolver, since coalescing multiple
/// [`Cache::merge`] calls into one record only preserves every fold when
/// the resolver can fold two values rather than only pick a side.
pub(super) fn validate_merge_window(window: Duration, resolver_merges: bool) -> bool {
    window.is_zero() || resolver_merges
}

/// The loading options a builder carries, as [`validate_load_config`]
/// checks them.
#[derive(Clone, Copy)]
pub(super) struct LoadConfig {
    pub(super) has_loader: bool,
    pub(super) has_batch_window: bool,
    pub(super) refresh_ahead: Option<f64>,
    pub(super) has_ttl: bool,
    pub(super) mode: Mode,
}

/// Whether the loading options a builder carries fit together, and the
/// refresh point in thousandths of a lifetime when refresh-ahead is on: a
/// batch window needs a loader to batch for, and refresh-ahead needs a
/// loader, a TTL to refresh ahead of, a mode whose reloaded value reaches
/// the nodes that read it, and a fraction strictly between 0 and 1. Pure;
/// unit tested directly.
pub(super) fn validate_load_config(config: LoadConfig) -> Result<Option<u64>, &'static str> {
    if config.has_batch_window && !config.has_loader {
        return Err("batch_window needs a loader");
    }
    let Some(fraction) = config.refresh_ahead else {
        return Ok(None);
    };
    if !config.has_loader {
        return Err("refresh_ahead needs a loader");
    }
    if !config.has_ttl {
        return Err("refresh_ahead needs a ttl");
    }
    if matches!(config.mode, Mode::Invalidation) {
        return Err("refresh_ahead does not apply to Mode::Invalidation");
    }
    refresh_permille(fraction)
        .map(Some)
        .ok_or("refresh_ahead takes a fraction strictly between 0 and 1")
}

/// The handles `attach_ownership` produces for a `Mode::Distributed` cache
/// and `spawn_cache_tasks` threads through to the background loops that
/// need them: the tracker and residency set already attached to the shard,
/// the `watch::Sender` its refresh loop publishes through, and the
/// `owners` count its view recomputes with.
pub(super) struct DistributedContext {
    pub(super) ownership: OwnershipTracker,
    pub(super) view_tx: watch::Sender<Arc<OwnershipView>>,
    pub(super) residency: Arc<ResidencySet>,
    pub(super) owners: NonZeroU8,
    /// The parts `Shard::attach_spill` warm-reloaded at least one record
    /// for. Empty here (the spill tier attaches only after the shard is
    /// shared); `CacheBuilder::open` fills it in before
    /// `spawn_cache_tasks` runs, for `distributed_warm_and_rebalance` to
    /// intersect with the initially owned parts and pick eager
    /// reconciliation candidates over an ordinary cold pull.
    pub(super) warm_reloaded_parts: HashSet<PartId>,
}

/// The cap [`await_initial_peers`] never waits past: long enough for a
/// gossip round or two, short enough a bad seed list never stalls `open()`.
const INITIAL_PEER_WAIT_CAP: Duration = Duration::from_secs(5);

/// Whether [`CacheBuilder::open`]'s `Mode::Distributed` path blocks for a
/// first known peer before computing the initial ownership view:
/// `true` only with a fixed seed list (`has_seeds`) and no peer known yet.
pub(super) fn should_await_first_peer(has_seeds: bool, peers_known: bool) -> bool {
    has_seeds && !peers_known
}

/// The rule behind `trust_sole_owner` for
/// [`distributed_warm_and_rebalance`]'s initial pull: trust sole ownership
/// at open unless this open replayed parts from a spill snapshot
/// (`replayed > 0`) and the membership wait timed out.
pub(super) fn trust_sole_owner_at_open(membership_settled: bool, replayed: usize) -> bool {
    membership_settled || replayed == 0
}

/// [`CacheBuilder::open`]'s bounded wait for a first known peer,
/// `Mode::Distributed` only, run before `attach_ownership` computes the
/// first view. When [`should_await_first_peer`] says this cluster has seeds
/// and knows no peer yet, blocks for the first peers-watch update, bounded
/// by `min(ClusterConfig::state_transfer_budget, INITIAL_PEER_WAIT_CAP)`.
/// Without this, a lone node's transient "I own everything" view could
/// outlive a peer that shows up moments later. Returns whether the wait,
/// when needed, landed a peer before its bound; logs at `info` only when a
/// wait happens.
pub(super) async fn await_initial_peers(cluster: &Cluster) -> bool {
    let peers_known = !cluster.peers().is_empty();
    if !should_await_first_peer(cluster.has_seeds(), peers_known) {
        return true;
    }
    let bound = cluster
        .config()
        .state_transfer_budget
        .min(INITIAL_PEER_WAIT_CAP);
    let mut peers = cluster.peers_watch();
    let start = Instant::now();
    let ran_to_completion = tokio::time::timeout(bound, async {
        while peers.borrow().is_empty() {
            if peers.changed().await.is_err() {
                return;
            }
        }
    })
    .await
    .is_ok();
    let settled = ran_to_completion && !peers.borrow().is_empty();
    let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    if settled {
        tracing::info!(
            elapsed_ms,
            "distributed cache open waited for the first known peer before computing its \
             initial ownership view"
        );
    } else {
        tracing::info!(
            elapsed_ms,
            bound_ms = u64::try_from(bound.as_millis()).unwrap_or(u64::MAX),
            "distributed cache open timed out waiting for a known peer; computing its initial \
             ownership view alone"
        );
    }
    settled
}

/// Attaches a freshly seeded ownership tracker and residency set to
/// `shard`, for a `Mode::Distributed` cache; every other mode leaves both
/// unset and returns `None`. Runs before `shard` is ever shared, so no
/// reader can observe anything but a real, already-computed view.
pub(super) fn attach_ownership<K, V>(
    mut shard: Shard<K, V>,
    cluster: &Cluster,
    name: &SmolStr,
    mode: Mode,
) -> (Shard<K, V>, Option<DistributedContext>)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let Mode::Distributed { owners } = mode else {
        return (shard, None);
    };
    let (ownership, view_tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        name,
        owners,
    );
    let residency = Arc::new(ResidencySet::new());
    // Cold until pulled: every initially owned part with a co-owner to pull
    // from. A part owned alone has no other copy; a node that opens before
    // gossip shows any peer holds nothing yet either way.
    let seed = ownership.current();
    let cold: Vec<PartId> = seed
        .owned_parts()
        .filter(|&part| seed.owners_of(part).len() > 1)
        .collect();
    residency.mark_cold(&cold);
    residency.settle(&seed);
    shard = shard.with_ownership(ownership.clone(), Arc::clone(&residency));
    (
        shard,
        Some(DistributedContext {
            ownership,
            view_tx,
            residency,
            owners,
            warm_reloaded_parts: HashSet::new(),
        }),
    )
}
