//! The typed cache handle and its builder. `Cache<K, V>` wraps
//! `Arc<Shard<K, V>>`; a local read decodes its own stored record under the stripe's read lock.
//!
//! [`CacheBuilder::open`] checks the requested [`Mode`] against what live
//! peers advertise for the same name before registering the shard, and
//! advertises its own choice on success.
//!
//! [`Cache::merge`] folds a value into the configured
//! [`ConflictResolver`] without a read; [`CacheBuilder::merge_coalesce_window`]
//! coalesces consecutive `merge` calls to one key into a single record per
//! window instead of one per call.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngExt as _;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cluster::Cluster;
use crate::cluster::anti_entropy;
use crate::config::ClusterConfig;
use crate::error::CacheError;
use crate::net::FetchOutcome;
use crate::node::NodeId;
use crate::ownership::{
    Granularity, OwnershipTracker, OwnershipView, ResidencySet, parts_of_wire_id, wire_ids,
};
use crate::store::PartId;
use crate::store::crdt::WriterId;
use crate::store::part::PartSet;
#[cfg(feature = "spill")]
use crate::store::spill::SpillConfig;
use crate::store::{
    ConflictResolver, Event, LwwResolver, Mode, Shard, ShardOps, Weigher, encode_key, now_ms,
};
use crate::wire::WireRecord;

/// Builds a [`Cache`]: own-and-return, per house style.
#[must_use]
pub struct CacheBuilder<K, V> {
    cluster: Cluster,
    name: SmolStr,
    mode: Mode,
    max_capacity: u64,
    ttl: Option<Duration>,
    tti: Option<Duration>,
    capacity_hint: Option<u64>,
    resolver: Arc<dyn ConflictResolver>,
    weigher: Option<Weigher<K, V>>,
    #[cfg(feature = "spill")]
    spill: Option<SpillConfig>,
    merge_coalesce_window: Duration,
    prefold_enabled: bool,
    marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V> CacheBuilder<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(cluster: Cluster, name: SmolStr) -> Self {
        Self {
            cluster,
            name,
            mode: Mode::Invalidation,
            max_capacity: u64::MAX,
            ttl: None,
            tti: None,
            capacity_hint: None,
            resolver: Arc::new(LwwResolver),
            weigher: None,
            #[cfg(feature = "spill")]
            spill: None,
            merge_coalesce_window: Duration::ZERO,
            prefold_enabled: true,
            marker: PhantomData,
        }
    }

    /// Sets the cache's clustering mode. Default: [`Mode::Invalidation`].
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    /// Bounds local entry count. Default: unbounded.
    pub fn max_capacity(mut self, max_capacity: u64) -> Self {
        self.max_capacity = max_capacity;
        self
    }

    /// Hints this shard's expected local entry count so each stripe's arena
    /// and index preallocate instead of growing one insert at a time. Pass
    /// this node's own expected resident share, never the cluster-wide total
    /// in `Mode::Distributed`. Clamped to [`CacheBuilder::max_capacity`] at
    /// [`CacheBuilder::open`] unless [`CacheBuilder::weigher`] is set, since a
    /// weigher turns `max_capacity` into a weight budget, not an entry count.
    pub fn capacity_hint(mut self, entries: u64) -> Self {
        self.capacity_hint = Some(entries);
        self
    }

    /// Sets the cache's default lifespan (TTL): every write is stamped with
    /// an absolute `expires_at_ms` that replicates with the record, so an
    /// entry expires at the same instant everywhere. Default: no expiry.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Sets a local-only max-idle (TTI), not cluster-replicated. Default: no
    /// idle expiry.
    pub fn tti(mut self, tti: Duration) -> Self {
        self.tti = Some(tti);
        self
    }

    /// Overrides the [`ConflictResolver`] that decides which of two
    /// differently-versioned records for the same key wins. Default:
    /// [`LwwResolver`], last-write-wins by [`crate::Hlc`].
    pub fn resolver(mut self, resolver: Arc<dyn ConflictResolver>) -> Self {
        self.resolver = resolver;
        self
    }

    /// Sets the window [`Cache::merge`] coalesces consecutive calls to one
    /// key within: instead of each call applying (and replicating) on its
    /// own, they fold in memory through the configured
    /// [`CacheBuilder::resolver`] and apply once, when the window that
    /// opened at the first of those calls elapses. Default: zero, so every
    /// `Cache::merge` call applies at once, equivalent to [`Cache::insert`]
    /// under a merging resolver.
    ///
    /// [`CacheBuilder::open`] returns
    /// [`CacheError::MergeWindowRequiresMergingResolver`] for a nonzero
    /// window combined with a resolver whose [`ConflictResolver::merges`]
    /// is `false`: a resolver that only ever picks a side would make
    /// coalescing silently drop every fold but the one that looks newest,
    /// not what [`Cache::merge`]'s docs promise.
    pub fn merge_coalesce_window(mut self, window: Duration) -> Self {
        self.merge_coalesce_window = window;
        self
    }

    /// Flips `engine::Engine::apply_many`'s pre-fold on or off for this
    /// cache's shard, `true` (pre-folding on) by default. `#[doc(hidden)]`:
    /// the real engine-level toggle, reachable from an integration-test
    /// binary outside this crate through
    /// [`crate::store::Shard::with_prefold_enabled`]; a benchmark
    /// measuring pre-fold's own effect is the only caller that ever needs
    /// it off, to compare against the unfolded per-record path
    /// `apply_many` otherwise always takes. Never call this outside a
    /// benchmark or test.
    #[doc(hidden)]
    pub fn prefold_enabled(mut self, enabled: bool) -> Self {
        self.prefold_enabled = enabled;
        self
    }

    /// Sets a custom per-entry weigher for size-bounded eviction:
    /// `max_capacity` becomes a weight budget rather than an entry count.
    pub fn weigher<W>(mut self, weigher: W) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.weigher = Some(Box::new(weigher));
        self
    }

    /// Configures the local SSD/NVMe spill tier: once `max_capacity` is
    /// exceeded, eviction demotes the coldest entries onto disk instead of
    /// discarding them, extending capacity beyond RAM. Off by default.
    #[cfg(feature = "spill")]
    pub fn spill(mut self, cfg: SpillConfig) -> Self {
        self.spill = Some(cfg);
        self
    }

    /// Opens the cache: builds the local shard, registers it in the
    /// cluster's shard registry, and, unless `mode` is [`Mode::Local`],
    /// starts fanning local writes out to the mesh per `mode`.
    ///
    /// For [`Mode::Replicated`], also runs state transfer before
    /// returning: a full snapshot from the lowest-node-id live peer, then
    /// one anti-entropy round against that donor, bounded by
    /// `ClusterConfig::state_transfer_budget`; a cache too large to finish
    /// opens with a partial copy anti-entropy tops up. For
    /// [`Mode::Distributed`], the same open()-time transfer instead pulls
    /// only this node's own buckets, per its freshly computed ownership
    /// view; a transfer that lands, finds nothing to pull, or times out
    /// repeatedly all still open the cache warm.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::AlreadyOpen`] if a cache named `name` is
    /// already open in this process; [`CacheError::TooFewOwners`] if
    /// `mode` is [`Mode::Distributed`] with an `owners` count under 2;
    /// [`CacheError::ReplicatedWithLocalEviction`] if `mode` is
    /// [`Mode::Replicated`] or [`Mode::Distributed`] with `tti` set, or
    /// `max_capacity` set with no `spill` tier configured (`tti` is
    /// rejected unconditionally, spill or not, since it is local-only by
    /// design); `CacheError::InvalidSpillConfig`, with the `spill` feature
    /// compiled in, for an invalid `spill` config; and
    /// [`CacheError::ModeMismatch`], best-effort, if a live peer already
    /// advertises `name` under a different [`Mode`].
    ///
    /// # Panics
    ///
    /// Panics if the shard registry lock is poisoned.
    #[expect(
        clippy::too_many_lines,
        reason = "one builder's whole open sequence -- validate, seed ownership, register, \
                  attach spill, advertise, spawn tasks -- reads best kept together rather than \
                  fragmented across helpers that would each need most of the same state passed in"
    )]
    pub async fn open(self) -> Result<Cache<K, V>, CacheError> {
        let Self {
            cluster,
            name,
            mode,
            max_capacity,
            ttl,
            tti,
            capacity_hint,
            resolver,
            weigher,
            #[cfg(feature = "spill")]
            spill,
            merge_coalesce_window,
            prefold_enabled,
            marker: _,
        } = self;

        #[cfg(feature = "spill")]
        let spill_configured = spill.is_some();
        #[cfg(not(feature = "spill"))]
        let spill_configured = false;

        let local_eviction = tti.is_some() || (max_capacity != u64::MAX && !spill_configured);
        validate_mode(&name, mode, local_eviction, cluster.config())?;

        if !validate_merge_window(merge_coalesce_window, resolver.merges()) {
            return Err(CacheError::MergeWindowRequiresMergingResolver { cache: name });
        }

        #[cfg(feature = "spill")]
        if let Some(cfg) = &spill
            && let Err(reason) = cfg.validate()
        {
            return Err(CacheError::InvalidSpillConfig {
                cache: name,
                reason,
            });
        }

        if let Some(remote) = cluster
            .advertised_cache_modes()
            .values()
            .find_map(|caches| caches.get(&name).filter(|&&m| m != mode).copied())
        {
            return Err(CacheError::ModeMismatch {
                cache: name,
                local: mode,
                remote,
            });
        }

        let mut shard = Shard::<K, V>::new(
            name.clone(),
            mode,
            cluster.node_id(),
            max_capacity,
            ttl,
            tti,
        )
        .with_cluster_config(cluster.config())
        .with_resolver(resolver)
        .with_merge_coalesce_window(merge_coalesce_window)
        .with_prefold_enabled(prefold_enabled);
        // `with_capacity_hint`/`with_weigher` each carry the other's setting
        // forward, so order is free. Skips the clamp when a weigher is set.
        if let Some(hint) = capacity_hint {
            let hint = if weigher.is_some() {
                hint
            } else {
                hint.min(max_capacity)
            };
            shard = shard.with_capacity_hint(hint);
        }
        if let Some(weigher) = weigher {
            shard = shard.with_weigher(move |key: &K, value: &V| weigher(key, value));
        }
        // Bounded wait for a first known peer, `Mode::Distributed` only,
        // before `attach_ownership` computes the first view; see
        // `should_await_first_peer` for when this waits. Runs even
        // for a cold open, since a view computed once a peer shows up beats
        // one computed alone. `membership_settled` records whether the wait
        // landed a peer; `distributed_warm_and_rebalance` uses it to gate the
        // sole-owner shortcut for a bucket this open's spill tier replayed.
        let membership_settled = if matches!(mode, Mode::Distributed { .. }) {
            await_initial_peers(&cluster).await
        } else {
            true
        };
        let (shard, distributed) = attach_ownership(shard, &cluster, &name, mode);
        let shard = Arc::new(shard);

        // The registry check-and-reserve runs before any spill I/O: two
        // `open()` calls for the same already-open name would otherwise both
        // wipe and preallocate the same `<dir>/<cache>/` region files before
        // either learns it lost to `AlreadyOpen`, corrupting whichever cache
        // is already running. Reserving this name first means a losing
        // `open()` never touches disk for it.
        let registry = cluster.shards();
        {
            let mut guard = registry
                .write()
                .expect("invariant: shard registry lock is never poisoned");
            if guard.contains_key(&name) {
                return Err(CacheError::AlreadyOpen { cache: name });
            }
            guard.insert(name.clone(), Arc::clone(&shard) as Arc<dyn ShardOps>);
        }

        // Only the `open()` that won the reservation above ever attaches a
        // spill tier. `Shard::attach_spill` runs through `&self`; its
        // `Engine`/`SpillRead` fields are `OnceLock`s, so it runs on a
        // shard already `Arc`-shared in the registry. A failure here rolls
        // the reservation back: nothing has advertised or scheduled tasks
        // for this name yet, so removing it is enough. A success threads
        // whether the tier landed a warm reopen into `distributed`, so
        // `distributed_warm_and_rebalance` knows which owned buckets get
        // eager reconciliation instead of an ordinary cold pull.
        #[cfg(feature = "spill")]
        let mut distributed = distributed;
        #[cfg(feature = "spill")]
        if let Err(source) = attach_spill_and_record_warm(&shard, spill.as_ref(), &mut distributed)
        {
            registry
                .write()
                .expect("invariant: shard registry lock is never poisoned")
                .remove(&name);
            return Err(CacheError::SpillUnavailable {
                cache: name.clone(),
                source,
            });
        }

        cluster.advertise_cache_mode(&name, mode);
        // `Replicated` and `Distributed` both have something to receive
        // before they can donate; every other mode is warm the moment it
        // opens.
        if matches!(mode, Mode::Local | Mode::Invalidation) {
            cluster.mark_warm(&name);
        }

        let cancel = cluster.cancel_token().child_token();
        let tasks = TaskTracker::new();
        spawn_cache_tasks(
            &cluster,
            &shard,
            &name,
            mode,
            &cancel,
            &tasks,
            distributed,
            membership_settled,
        )
        .await;

        Ok(Cache {
            shard,
            cluster,
            cancel,
            tasks,
        })
    }
}

/// [`CacheBuilder::open`]'s spill-attach step: a no-op when `spill` is
/// `None`, otherwise [`Shard::attach_spill`], recording the warm-reloaded
/// buckets into `distributed`'s [`DistributedContext::warm_reloaded_buckets`].
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] [`Shard::attach_spill`] returns.
#[cfg(feature = "spill")]
fn attach_spill_and_record_warm<K, V>(
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
        // Every bucket this warm reopen replayed is unverified until
        // `reconcile_warm_buckets` or an ordinary cold pull lands fresh data.
        let warm: Vec<u16> = outcome.warm_buckets.iter().copied().collect();
        ctx.residency.mark_unverified(&warm);
        ctx.warm_reloaded_buckets = outcome.warm_buckets;
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
fn validate_mode(
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
fn validate_merge_window(window: Duration, resolver_merges: bool) -> bool {
    window.is_zero() || resolver_merges
}

/// `part`'s owners under `view` other than `self_node`: the candidates a
/// [`Cache::fetch`] asks, in rendezvous order.
fn other_owners(view: &OwnershipView, part: PartId, self_node: NodeId) -> Vec<NodeId> {
    view.owners_of(part)
        .iter()
        .copied()
        .filter(|owner| *owner != self_node)
        .collect()
}

/// A small jittered backoff between retrying the same [`Cache::fetch`]
/// owner after it reports its view as stale but this node's own view has
/// not itself changed: uniformly in `[10, 50)` ms. The retries against one
/// owner span at most `ClusterConfig::fetch_timeout`, the same bound one
/// request gets, before the next owner is tried.
fn fetch_retry_backoff() -> Duration {
    Duration::from_millis(rand::rng().random_range(10..50))
}

/// Emits `sundog_fetch_total{cache, outcome}` for one [`Cache::fetch`] call.
fn record_fetch_outcome(cache: &str, outcome: &'static str) {
    metrics::counter!(
        "sundog_fetch_total",
        "cache" => cache.to_string(),
        "outcome" => outcome
    )
    .increment(1);
}

/// Decodes a [`Cache::fetch`] reply's record into its value, re-checking
/// expiry client-side (defense in depth against the responder's own expiry
/// sweep lagging) and treating a tombstone or undecodable value as a miss.
fn decode_live_value<V: DeserializeOwned>(rec: &WireRecord) -> Option<V> {
    if rec.is_tombstone() {
        return None;
    }
    if let Some(expires_at_ms) = rec.expires_at_ms
        && expires_at_ms <= now_ms()
    {
        return None;
    }
    rec.value
        .as_deref()
        .and_then(|bytes| postcard::from_bytes::<V>(bytes).ok())
}

/// The handles `attach_ownership` produces for a `Mode::Distributed` cache
/// and `spawn_cache_tasks` threads through to the background loops that
/// need them: the tracker and residency set already attached to the shard,
/// the `watch::Sender` its refresh loop publishes through, and the
/// `owners` count its view recomputes with.
struct DistributedContext {
    ownership: OwnershipTracker,
    view_tx: watch::Sender<Arc<OwnershipView>>,
    residency: Arc<ResidencySet>,
    owners: NonZeroU8,
    /// The buckets `Shard::attach_spill` warm-reloaded at least one record
    /// for. Empty here (the spill tier attaches only after the shard is
    /// shared); `CacheBuilder::open` fills it in before
    /// `spawn_cache_tasks` runs, for `distributed_warm_and_rebalance` to
    /// intersect with the initially owned buckets and pick eager
    /// reconciliation candidates over an ordinary cold pull.
    warm_reloaded_buckets: HashSet<u16>,
}

/// The cap [`await_initial_peers`] never waits past: long enough for a
/// gossip round or two, short enough a bad seed list never stalls `open()`.
const INITIAL_PEER_WAIT_CAP: Duration = Duration::from_secs(5);

/// Whether [`CacheBuilder::open`]'s `Mode::Distributed` path blocks for a
/// first known peer before computing the initial ownership view:
/// `true` only with a fixed seed list (`has_seeds`) and no peer known yet.
fn should_await_first_peer(has_seeds: bool, peers_known: bool) -> bool {
    has_seeds && !peers_known
}

/// The rule behind `trust_sole_owner` for
/// [`distributed_warm_and_rebalance`]'s initial pull: trust sole ownership
/// at open unless this open replayed buckets from a spill snapshot
/// (`replayed > 0`) and the membership wait timed out.
fn trust_sole_owner_at_open(membership_settled: bool, replayed: usize) -> bool {
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
async fn await_initial_peers(cluster: &Cluster) -> bool {
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

/// Attaches a freshly seeded bucket-ownership tracker and residency set to
/// `shard`, for a `Mode::Distributed` cache; every other mode leaves both
/// unset and returns `None`. Runs before `shard` is ever shared, so no
/// reader can observe anything but a real, already-computed view.
fn attach_ownership<K, V>(
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
    shard = shard.with_ownership(ownership.clone(), Arc::clone(&residency));
    (
        shard,
        Some(DistributedContext {
            ownership,
            view_tx,
            residency,
            owners,
            warm_reloaded_buckets: HashSet::new(),
        }),
    )
}

/// The background loops one opened cache runs: fan-out for a clustered
/// mode, warm-up and anti-entropy for `Replicated`, the analogous
/// bucket-scoped pull, refresh, and rebalance loops for `Distributed`,
/// tombstone GC, and the entry gauge, all under `cancel` and tracked by
/// `tasks`. `membership_settled` is `open()`'s own [`await_initial_peers`]
/// outcome, used only by [`distributed_warm_and_rebalance`]'s sole-owner shortcut.
#[expect(
    clippy::too_many_arguments,
    reason = "each parameter is independent context `open()` already has to hand; a struct would \
              only rename these same eight fields"
)]
async fn spawn_cache_tasks<K, V>(
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

/// The `Mode::Distributed`-only half of [`spawn_cache_tasks`]: pulls this
/// node's initially owned buckets from their current owners before the
/// cache is marked warm, the bucket-scoped analogue of
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
async fn distributed_warm_and_rebalance(
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
        warm_reloaded_buckets,
    } = distributed;

    let budget = cluster.config().state_transfer_budget;
    let concurrency = cluster.config().rebalance_concurrency;
    let disown_grace =
        cluster.config().ae_interval * cluster.config().distributed_disown_grace_rounds;
    // The refresh loop starts before the first pull, so a membership change
    // during it moves the view under the pull, which replans instead of stalling.
    cluster.spawn_tracked_in(
        tasks,
        crate::ownership::refresh_task(
            cluster.clone(),
            name.clone(),
            owners,
            view_tx,
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
        .filter(|part| warm_reloaded_buckets.contains(&part.bucket()))
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
        // else the bucket stays cold for `warm_up_task`'s ordinary retries.
        trust_sole_owner: trust_sole_owner_at_open(membership_settled, warm_reloaded_buckets.len()),
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

/// Cap on rounds in [`reconcile_warm_buckets`]'s per-peer converge loop:
/// chances for a bucket still moving under live writes to settle, not chunks
/// of a fixed size. A failed or `Stale` round doesn't count; it retries.
const RECONCILE_MAX_ROUNDS: u32 = 3;

/// Base retry delay after a failed round in [`reconcile_against_peer`];
/// doubles per consecutive failure, capped at [`ReconcileBudget::backoff_cap`].
const RECONCILE_RETRY_BASE: Duration = Duration::from_millis(200);

/// Per-peer bound on [`reconcile_warm_buckets`]'s converge loop: whichever
/// of `max_rounds`, `byte_budget` or `time_budget` is hit first stops it,
/// leaving that peer's still-diverging buckets cold for the ordinary pull.
#[derive(Debug, Clone, Copy)]
struct ReconcileBudget {
    max_rounds: u32,
    byte_budget: u64,
    time_budget: Duration,
    backoff_cap: Duration,
}

impl ReconcileBudget {
    /// Builds from `ClusterConfig`: `byte_budget`, `time_budget` (the same
    /// startup bound the initial pull honors), and `backoff_cap` from `ae_interval`.
    fn from_config(config: &ClusterConfig) -> Self {
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
fn should_keep_reconciling(
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
fn retry_delay(
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
/// outcome; a `failed` round reports every bucket as still diverging.
fn split_round_result(
    requested: &[u16],
    outcome: &anti_entropy::BucketRoundOutcome,
) -> (Vec<u16>, Vec<u16>) {
    if outcome.failed {
        return (Vec::new(), requested.to_vec());
    }
    requested
        .iter()
        .copied()
        .partition(|bucket| outcome.matched.contains(bucket))
}

/// One live co-owner's converge loop: repeatedly
/// [`anti_entropy::run_round_for_buckets`] against `peer` over its
/// still-diverging subset of wire `ids` at `granularity`, marking the parts
/// an id names serving as soon as its digest matches, until nothing is left
/// diverging or [`should_keep_reconciling`]'s `budget` stops it. A failed or
/// `Stale` round retries via [`retry_delay`] without counting as a round.
/// Returns every part marked serving this way.
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
    (granularity, ids): (Granularity, Vec<u16>),
    budget: ReconcileBudget,
) -> HashSet<PartId> {
    let mesh = cluster.mesh();
    let started = Instant::now();
    let mut still_diverging = ids;
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
            anti_entropy::run_round_for_buckets(mesh, shard_ops, cache, peer, &still_diverging)
                .await;
        if outcome.failed {
            failed_in_a_row += 1;
        } else {
            rounds_run += 1;
            failed_in_a_row = 0;
        }
        bytes_moved += outcome.bytes_moved;
        let (converged, diverging) = split_round_result(&still_diverging, &outcome);
        if !converged.is_empty() {
            // Cleared per id once its digest matches, not held back for the rest.
            let parts: Vec<PartId> = converged
                .iter()
                .flat_map(|&id| parts_of_wire_id(granularity, id))
                .collect();
            residency.mark_serving(&parts);
            reconciled.extend(parts);
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
    // Every bucket still in `still_diverging` here gets no `ResidencySet`
    // call: it stays cold and unverified, falling to the ordinary cold-pull path.
    reconciled
}

/// Converge-before-serving reconciliation: groups `warm_parts` by live
/// co-owner, then runs [`reconcile_against_peer`]'s loop for each peer
/// concurrently over the wire ids naming its parts, returning the union of
/// parts marked serving.
///
/// A warm-reloaded part's local state came from replaying on-disk records
/// that predate this restart and can include a value this node itself
/// deleted before going down (deletes never persist to the spill tier), so
/// it needs verification against a live co-owner rather than outright
/// trust. A part with no live co-owner, or one whose loop against a peer
/// never closed the gap, gets no [`ResidencySet`] call and stays cold and
/// unverified, falling to the caller's ordinary cold-pull path.
async fn reconcile_warm_buckets(
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
    let granularity = view.granularity();
    // Runs concurrently, not sequentially: warm parts can split across
    // several peers, and running loops one after another would multiply the
    // stall by peer count instead of bounding it by one `ReconcileBudget`.
    let per_peer = peer_parts.into_iter().map(|(peer, parts)| {
        let ids = (granularity, wire_ids(granularity, &parts));
        reconcile_against_peer(cluster, shard_ops, cache, residency, peer, ids, budget)
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

/// A typed handle to one named, possibly-clustered cache. Cheap to
/// `Clone`; every clone shares the same underlying [`Shard`] and the same
/// background tasks.
#[derive(Clone)]
pub struct Cache<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    shard: Arc<Shard<K, V>>,
    cluster: Cluster,
    cancel: CancellationToken,
    tasks: TaskTracker,
}

impl<K, V> std::fmt::Debug for Cache<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("name", &self.shard.name())
            .field("mode", &self.shard.mode())
            .finish_non_exhaustive()
    }
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// This cache's name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.shard.name()
    }

    /// Per-stripe `live` arena capacities; see [`crate::store::Shard::stripe_capacities`].
    /// `#[doc(hidden)]` test accessor reaching the private `shard` field. Test/benchmark only.
    #[doc(hidden)]
    #[must_use]
    pub fn stripe_capacities(&self) -> Vec<usize> {
        self.shard.stripe_capacities()
    }

    /// This node's current [`WriterId`]: its own [`Cluster::node_id`] paired
    /// with the cluster's current membership incarnation. Every caller of
    /// a merging cache's [`Cache::merge`], including the CRDT types' own
    /// `local_delta`/`add` constructors, should build its writer identity
    /// from this rather than a bare [`NodeId`], so a restart writes from
    /// a fresh slot the
    /// compaction sweep has never seen, instead of resuming (or colliding
    /// with) whatever this node wrote under its previous incarnation.
    /// Stable for the process's lifetime; every clone of this `Cache`
    /// returns the same value, since they share one underlying cluster
    /// membership.
    #[must_use]
    pub fn writer_id(&self) -> WriterId {
        WriterId::new(self.cluster.node_id(), self.cluster.local_incarnation())
    }

    /// Reads `key`, without triggering read-through.
    pub async fn get(&self, key: &K) -> Option<V> {
        self.shard.get(key).await
    }

    /// [`Cache::get`] without an async runtime.
    #[must_use]
    pub fn get_sync(&self, key: &K) -> Option<V> {
        self.shard.get_sync(key)
    }

    /// Reads `key`: local if this node owns its bucket (no network,
    /// counted `sundog_fetch_total{outcome="local"}`), otherwise one request
    /// to a live owner, tried in rendezvous-score order until one answers.
    /// Never promotes the fetched value into the local store; [`Cache::get`]
    /// for the same key immediately afterward is still a miss on this node.
    /// A bucket this node owns but has not yet pulled from a co-owner is
    /// cold: a local miss there asks the other owners before answering
    /// `Ok(None)`, and a cold owner whose every other owner is unreachable
    /// returns [`CacheError::FetchUnavailable`] rather than a miss it
    /// cannot vouch for. On a cache that isn't [`Mode::Distributed`] this
    /// is [`Cache::get`] wrapped in `Ok`, counted `outcome="local"`
    /// unconditionally, so callers don't need to branch on mode.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to encode, and
    /// [`CacheError::FetchUnavailable`] if every owner is unreachable or
    /// times out (`crate::config::ClusterConfig::fetch_timeout` per
    /// attempt), distinct from a genuine miss, which returns `Ok(None)`.
    /// An owner whose ownership view differs from this node's is retried
    /// with a short jittered backoff before the next owner is tried.
    pub async fn fetch(&self, key: &K) -> Result<Option<V>, CacheError> {
        let Some(view) = self.shard.ownership_view() else {
            let value = self.shard.get(key).await;
            record_fetch_outcome(self.shard.name(), "local");
            return Ok(value);
        };
        let key_bytes = encode_key(key)?;
        let part = PartId::of_key(&key_bytes);
        // An unverified part (warm-reloaded, not yet checked against a live
        // co-owner) counts as not owned here: it can hold a record a co-owner deleted during downtime.
        let mut owns = view.owns(part) && !self.shard.is_unverified_part(part);
        if owns {
            let value = self.shard.get(key).await;
            // A miss in a part not yet pulled from a co-owner is not an
            // answer: the other owners are asked first.
            if value.is_some() || !self.shard.is_cold_part(part) {
                record_fetch_outcome(self.shard.name(), "local");
                return Ok(value);
            }
        }

        let cache_name = SmolStr::new(self.shard.name());
        let mesh = self.cluster.mesh();
        let attempt_timeout = self.cluster.config().fetch_timeout;

        let self_node = self.cluster.node_id();
        let mut view = view;
        let mut owners = other_owners(&view, part, self_node);
        let mut owner_idx = 0usize;
        // When the current owner first answered `Stale` with this node's
        // view unchanged: its retry window runs from here.
        let mut stale_since: Option<tokio::time::Instant> = None;
        // Whether any owner answered at all, a decline included, as opposed
        // to every attempt ending in a transport error or timeout: the
        // difference between a miss and `FetchUnavailable` for a bucket
        // this node owns but has not pulled yet. With nobody else to ask,
        // this node's own copy is all there is.
        let mut any_answered = owners.is_empty();

        while owner_idx < owners.len() {
            let owner = owners[owner_idx];
            let outcome = tokio::time::timeout(
                attempt_timeout,
                mesh.fetch(
                    owner,
                    cache_name.clone(),
                    key_bytes.clone(),
                    view.view_hash(),
                ),
            )
            .await;
            match outcome {
                Ok(Ok(FetchOutcome::Found(rec))) => {
                    let value = rec.and_then(|rec| decode_live_value::<V>(&rec));
                    record_fetch_outcome(
                        &cache_name,
                        if value.is_some() { "remote" } else { "miss" },
                    );
                    return Ok(value);
                }
                Ok(Ok(FetchOutcome::Declined)) => {
                    any_answered = true;
                    owner_idx += 1;
                    stale_since = None;
                }
                Ok(Ok(FetchOutcome::Stale { .. })) => {
                    if let Some(fresh) = self.shard.ownership_view()
                        && fresh.view_hash() != view.view_hash()
                    {
                        view = fresh;
                        if view.owns(part) && !self.shard.is_unverified_part(part) {
                            let value = self.shard.get(key).await;
                            if value.is_some() || !self.shard.is_cold_part(part) {
                                record_fetch_outcome(&cache_name, "local");
                                return Ok(value);
                            }
                        }
                        owns = view.owns(part) && !self.shard.is_unverified_part(part);
                        owners = other_owners(&view, part, self_node);
                        owner_idx = 0;
                        stale_since = None;
                        any_answered = owners.is_empty();
                        continue;
                    }
                    let since = *stale_since.get_or_insert_with(tokio::time::Instant::now);
                    if since.elapsed() >= attempt_timeout {
                        owner_idx += 1;
                        stale_since = None;
                        continue;
                    }
                    tokio::time::sleep(fetch_retry_backoff()).await;
                }
                Ok(Err(_)) | Err(_) => {
                    owner_idx += 1;
                    stale_since = None;
                }
            }
        }
        if owns && any_answered {
            // Every other owner declined too, or there is none: this
            // owner's own miss is the best answer there is.
            record_fetch_outcome(&cache_name, "miss");
            return Ok(None);
        }
        record_fetch_outcome(&cache_name, "error");
        Err(CacheError::FetchUnavailable { cache: cache_name })
    }

    /// The live owners of `key`'s bucket, in rendezvous score order.
    /// `vec![self.cluster.node_id()]` on a cache that isn't
    /// [`Mode::Distributed`].
    #[must_use]
    pub fn owners_of(&self, key: &K) -> Vec<NodeId> {
        let Some(view) = self.shard.ownership_view() else {
            return vec![self.cluster.node_id()];
        };
        let Ok(key_bytes) = encode_key(key) else {
            return Vec::new();
        };
        view.owners_of(PartId::of_key(&key_bytes)).to_vec()
    }

    /// Reads whether `key` has a live entry, honoring expiry, without cloning
    /// it.
    pub async fn contains_key(&self, key: &K) -> bool {
        self.shard.contains_key(key).await
    }

    /// [`Cache::contains_key`] without an async runtime.
    #[must_use]
    pub fn contains_key_sync(&self, key: &K) -> bool {
        self.shard.contains_key_sync(key)
    }

    /// The number of live entries in this node's local copy; nodes may
    /// legitimately hold different subsets or briefly disagree under lag.
    pub async fn entry_count(&self) -> u64 {
        self.shard.entry_count().await
    }

    /// A weakly consistent snapshot of this node's local live keys, not a
    /// cluster view. O(entries).
    #[must_use]
    pub fn keys(&self) -> Vec<K> {
        self.shard.keys()
    }

    /// [`Cache::keys`] as a visitor: `f` runs once per local live key, never
    /// under a lock, and no `Vec` of every key is built.
    pub fn for_each_key(&self, f: impl FnMut(K)) {
        self.shard.for_each_key(f);
    }

    /// Reads `key`, invoking `loader` on a miss; concurrent misses collapse
    /// into one `loader` call.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Loader`] if `loader` fails, or
    /// [`CacheError::Codec`] if `key` fails to postcard-encode.
    pub async fn get_or_load<F, E>(&self, key: &K, loader: F) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.shard.get_or_load(key, loader).await
    }

    /// [`Cache::get_or_load`] for a loader that never fails; `Result` remains
    /// only for [`CacheError::Codec`].
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to postcard-encode.
    pub async fn get_or_insert_with(
        &self,
        key: &K,
        make: impl AsyncFnOnce(&K) -> V,
    ) -> Result<V, CacheError> {
        self.shard.get_or_insert_with(key, make).await
    }

    /// Writes `key` = `value`: stamps an HLC version, applies locally, and
    /// fans out per [`Mode`]. Gets the cache's default TTL, if configured.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the encoded value exceeds
    /// the frame cap, or [`CacheError::Codec`] if `key` fails to encode.
    pub async fn insert(&self, key: K, value: V) -> Result<(), CacheError> {
        self.shard.insert(key, value).await
    }

    /// [`Cache::insert`] without an async runtime: same fan-out and events.
    ///
    /// # Errors
    ///
    /// As [`Cache::insert`].
    pub fn insert_sync(&self, key: K, value: V) -> Result<(), CacheError> {
        self.shard.insert_sync(key, value)
    }

    /// [`Cache::insert`] with a lifespan for this entry alone; `ttl`
    /// overrides the cache's default and travels with the record.
    ///
    /// # Errors
    ///
    /// As [`Cache::insert`].
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<(), CacheError> {
        self.shard.insert_with_ttl(key, value, ttl).await
    }

    /// Writes many entries under one acquisition of the store's apply lock,
    /// emitting one [`Event`] per entry as [`Cache::insert`] would. **Not a
    /// transaction**: an entry that fails partway through still leaves the
    /// entries before it applied.
    ///
    /// # Errors
    ///
    /// As [`Cache::insert`], for any entry.
    pub async fn insert_many(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), CacheError> {
        self.shard.insert_many(entries).await
    }

    /// Folds `value` into `key` through the configured
    /// [`CacheBuilder::resolver`] without a read. With
    /// [`CacheBuilder::merge_coalesce_window`] left at its default zero,
    /// every call applies (and replicates) at once, equivalent to
    /// [`Cache::insert`] under a merging resolver. With a nonzero window,
    /// consecutive calls to the same key fold in memory instead, and the
    /// fold applies exactly once, when the window that opened at the first
    /// of those calls elapses: replication and every [`Event`] this key
    /// gets during the window see one record, not one per call.
    /// [`Cache::close`], and dropping this cache's last handle, both flush
    /// whatever is still pending regardless of the window.
    ///
    /// [`Cache::get`] never consults a pending fold: a value folded in but
    /// not yet flushed is invisible to a read for as long as it stays
    /// pending, up to one whole window from the call that opened it.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the encoded value exceeds
    /// the frame cap, or [`CacheError::Codec`] if `key` fails to encode.
    pub async fn merge(&self, key: K, value: V) -> Result<(), CacheError> {
        self.shard.merge(key, value).await
    }

    /// [`Cache::insert_many`] with one lifespan applied to every entry,
    /// overriding the cache's default.
    ///
    /// # Errors
    ///
    /// As [`Cache::insert_many`].
    pub async fn insert_many_with_ttl(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
        ttl: Duration,
    ) -> Result<(), CacheError> {
        self.shard.insert_many_with_ttl(entries, ttl).await
    }

    /// Removes `key`: writes a tombstone and fans it out per [`Mode`].
    ///
    /// # Errors
    ///
    /// Returns a [`CacheError`] if the removal cannot apply or fan out.
    pub async fn remove(&self, key: &K) -> Result<(), CacheError> {
        self.shard.remove(key).await
    }

    /// [`Cache::remove`] without an async runtime: same fan-out and events.
    ///
    /// # Errors
    ///
    /// As [`Cache::remove`].
    pub fn remove_sync(&self, key: &K) -> Result<(), CacheError> {
        self.shard.remove_sync(key)
    }

    /// [`Cache::remove`] for many keys at once, the tombstone counterpart of
    /// [`Cache::insert_many`].
    ///
    /// # Errors
    ///
    /// Returns a [`CacheError`] if any key fails to encode for the wire.
    pub async fn remove_many(&self, keys: impl IntoIterator<Item = K>) -> Result<(), CacheError> {
        self.shard.remove_many(keys).await
    }

    /// Tombstones every key this node currently holds, not a coordinated
    /// cluster-wide reset: an entry never reached from a peer, or a
    /// concurrent write outracing the tombstone's HLC, survives.
    ///
    /// # Errors
    ///
    /// As [`Cache::remove_many`].
    pub async fn clear(&self) -> Result<(), CacheError> {
        self.shard.clear().await
    }

    /// Drops the local copy of `key` without writing a tombstone or fanning
    /// out. The entry may reappear on the next anti-entropy round.
    pub async fn invalidate_local(&self, key: &K) {
        self.shard.invalidate_local(key).await;
    }

    /// Subscribes to this cache's change events, each tagged with its
    /// [`crate::store::Origin`].
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event<K, V>> {
        self.shard.events()
    }

    /// Closes this cache: stops its background tasks and waits for them,
    /// closes its spill tier if one is configured, drops it from the
    /// cluster's shard registry, and clears its gossiped mode, so peers
    /// stop seeing it advertised and this node stops serving or applying
    /// replication traffic for it. The name is free to `open()` again
    /// when this returns. Closing the spill tier lets its flusher thread
    /// drain its queue and exit on its own.
    ///
    /// Closing is idempotent. A clone kept past `close` keeps working as a
    /// local, detached cache: its reads and writes reach the same in-memory
    /// [`Shard`], and nothing replicates. The one exception is a
    /// `Mode::Distributed` write for a bucket this node does not own, which
    /// has no local copy to land in: it fails with [`CacheError::Closed`]
    /// rather than being accepted and never sent. A cache never explicitly closed
    /// still has its spill tier closed by `Cluster::shutdown`.
    pub async fn close(self) {
        // Flushed before sealing: a pending coalesced merge this flush
        // applies still gets a chance at the fan-out task's final drain,
        // exactly like any other write landing right before close.
        self.shard.flush_all_pending_merges();
        // Sealed before the tasks are cancelled: a write accepted until
        // now is in the backlog the fan-out task drains on its way out,
        // and a write from now on fails with `CacheError::Closed`.
        self.shard.fan_out_queue().seal();
        self.cancel.cancel();
        self.tasks.close();
        self.tasks.wait().await;
        self.shard.fan_out_queue().close();
        self.shard.close_spill_checkpointed().await;
        self.cluster.forget_cache(self.shard.name());
    }
}

// These tests dial real loopback sockets, which the `sim` transport cannot serve.
#[cfg(all(test, not(feature = "sim")))]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;
    use crate::cluster::Cluster;
    use crate::cluster::test_support::wait_until;
    use crate::store::bucket_of;
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    fn loopback_config() -> ClusterConfig {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        ClusterConfig {
            gossip_bind_addr: loopback,
            data_bind_addr: loopback,
            ae_interval: Duration::from_millis(200),
            tombstone_ttl: Duration::from_secs(2),
            state_transfer_budget: Duration::from_secs(5),
            ..ClusterConfig::default()
        }
    }

    async fn wait_for_peer_count(cluster: &Cluster, expected: usize) {
        wait_until(
            Duration::from_secs(15),
            "peers converge within the bound",
            async || cluster.peers().len() >= expected,
        )
        .await;
    }

    /// A loopback UDP address nothing listens on, for forcing a wait to time out.
    async fn dead_gossip_addr() -> SocketAddr {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind an ephemeral loopback udp port to reserve a dead gossip address");
        socket
            .local_addr()
            .expect("a freshly bound udp socket reports a local address")
    }

    #[test]
    fn should_await_first_peer_only_when_seeded_and_genuinely_alone() {
        assert!(
            should_await_first_peer(true, false),
            "seeds present and peers empty waits"
        );
        assert!(
            !should_await_first_peer(false, false),
            "no seeds does not wait, even with peers still empty"
        );
        assert!(
            !should_await_first_peer(true, true),
            "peers already known does not wait, even with seeds configured"
        );
        assert!(
            !should_await_first_peer(false, true),
            "no seeds and peers already known: nothing to wait for either way"
        );
    }

    #[test]
    fn trust_sole_owner_at_open_only_distrusts_a_timed_out_warm_reopen() {
        assert!(
            trust_sole_owner_at_open(true, 0),
            "cold open, membership settled: trusted"
        );
        assert!(
            trust_sole_owner_at_open(true, 3),
            "warm reopen, membership settled: trusted"
        );
        assert!(
            trust_sole_owner_at_open(false, 0),
            "cold open, membership wait timed out: still trusted, nothing replayed to distrust"
        );
        assert!(
            !trust_sole_owner_at_open(false, 3),
            "warm reopen, membership wait timed out: not trusted, a replayed bucket found owned \
             alone could be the replay's own stale echo of ownership"
        );
    }

    #[tokio::test]
    async fn await_initial_peers_returns_immediately_without_seeds() {
        let cluster = Cluster::builder("cache-it-await-peers-no-seeds")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("solo cluster builds");
        assert!(
            cluster.peers().is_empty(),
            "a fresh solo cluster knows no peer yet"
        );

        let start = Instant::now();
        let settled = await_initial_peers(&cluster).await;

        assert!(
            settled,
            "a seedless cluster is a legitimate one-node cluster, not a race to wait out"
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "no wait should have happened at all, so this returns almost instantly"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn await_initial_peers_settles_once_a_seeded_peer_joins() {
        let b = Cluster::builder("cache-it-await-peers-settles")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("b builds");
        let a = Cluster::builder("cache-it-await-peers-settles")
            .seeds([b.local_gossip_addr()])
            .config(loopback_config())
            .build()
            .await
            .expect("a builds");

        // No prior `wait_for_peer_count`: `await_initial_peers` itself is under test.
        let settled = await_initial_peers(&a).await;

        assert!(settled, "the seeded peer shows up well within the bound");
        assert!(
            !a.peers().is_empty(),
            "a's peer list is non-empty once the wait reports settled"
        );

        a.shutdown().await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn await_initial_peers_times_out_when_seeded_but_nobody_answers() {
        let dead_seed = dead_gossip_addr().await;
        let config = ClusterConfig {
            state_transfer_budget: Duration::from_millis(200),
            ..loopback_config()
        };
        let cluster = Cluster::builder("cache-it-await-peers-timeout")
            .seeds([dead_seed])
            .config(config)
            .build()
            .await
            .expect("cluster builds even though its one seed answers nobody");

        let start = Instant::now();
        let settled = await_initial_peers(&cluster).await;
        let elapsed = start.elapsed();

        assert!(
            !settled,
            "nobody ever answers the dead seed, so the wait must time out"
        );
        assert!(cluster.peers().is_empty(), "still genuinely alone");
        assert!(
            elapsed >= Duration::from_millis(180),
            "the wait should run close to its full 200ms bound, took {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "the wait must not run anywhere near the `INITIAL_PEER_WAIT_CAP` default, took \
             {elapsed:?}"
        );

        cluster.shutdown().await;
    }

    /// A loopback TCP address nobody listens on, distinct from [`dead_gossip_addr`]'s UDP one.
    fn dead_tcp_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("bind an ephemeral loopback tcp port to reserve a dead address");
        listener
            .local_addr()
            .expect("a freshly bound tcp listener reports a local address")
    }

    /// Pins that a cold open trusts sole ownership outright even when its
    /// only seed never answers, guarding the regression that left every
    /// bucket cold behind a timed-out membership wait.
    #[tokio::test]
    async fn distributed_open_trusts_sole_ownership_when_its_membership_wait_times_out() {
        let dead_seed = dead_tcp_addr();
        let config = ClusterConfig {
            // Short enough that `await_initial_peers`'s wait times out quickly.
            state_transfer_budget: Duration::from_secs(1),
            ..loopback_config()
        };
        let cluster = Cluster::builder("cache-it-cold-open-sole-owner")
            .seeds([dead_seed])
            .config(config)
            .build()
            .await
            .expect("cluster builds even though its one seed answers nobody");

        let cache = tokio::time::timeout(
            Duration::from_secs(10),
            cluster
                .cache::<u32, String>("prices")
                .mode(Mode::Distributed {
                    owners: NonZeroU8::new(2).expect("nonzero"),
                })
                .open(),
        )
        .await
        .expect("open completes within its own short membership wait")
        .expect("open succeeds even though the wait timed out");

        cache
            .insert(1, "one".to_string())
            .await
            .expect("insert succeeds on a node that owns every bucket alone");

        let key_bytes = encode_key(&1u32).expect("a u32 key always encodes");
        let bucket = bucket_of(&key_bytes);

        tokio::time::timeout(Duration::from_secs(5), async {
            while cache.shard.is_cold_bucket(bucket) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the key's bucket is not cold within a few seconds");

        assert!(
            !cache.shard.is_cold_bucket(bucket),
            "sole ownership on a cold open is trusted outright, not left cold behind a peer \
             that never appears"
        );
        assert_eq!(
            cache.fetch(&1).await.expect("fetch succeeds"),
            Some("one".to_string()),
            "a bucket trusted as sole-owned is served locally, not held back for a peer that \
             never appears"
        );
        assert!(
            cluster.peers().is_empty(),
            "this assertion holds before any peer ever appears, exactly the shape the timed-out \
             wait leaves behind"
        );

        cluster.shutdown().await;
    }

    #[test]
    fn reconcile_budget_from_config_takes_its_byte_time_and_backoff_bounds_from_the_config() {
        let config = ClusterConfig {
            reconcile_byte_budget: 12_345,
            state_transfer_budget: Duration::from_millis(4_321),
            ae_interval: Duration::from_millis(765),
            ..ClusterConfig::default()
        };
        let budget = ReconcileBudget::from_config(&config);
        assert_eq!(budget.max_rounds, RECONCILE_MAX_ROUNDS);
        assert_eq!(budget.byte_budget, 12_345);
        assert_eq!(budget.time_budget, Duration::from_millis(4_321));
        assert_eq!(budget.backoff_cap, Duration::from_millis(765));
    }

    /// Pins that `should_keep_reconciling` stops once `elapsed` alone reaches `time_budget`.
    #[test]
    fn should_keep_reconciling_stops_once_the_time_budget_is_spent() {
        let budget = ReconcileBudget {
            max_rounds: 3,
            byte_budget: 1_000,
            time_budget: Duration::from_secs(1),
            backoff_cap: Duration::from_millis(200),
        };
        for (elapsed, expect_continue) in [
            (Duration::from_millis(999), true),
            (Duration::from_secs(1), false),
            (Duration::from_secs(2), false),
        ] {
            assert_eq!(
                should_keep_reconciling(0, 0, 5, elapsed, &budget),
                expect_continue,
                "elapsed {elapsed:?}"
            );
        }
    }

    /// Pins that `retry_delay` doubles from `RECONCILE_RETRY_BASE` per
    /// failure, caps at `backoff_cap`, and stops at the time budget.
    #[test]
    fn retry_delay_doubles_from_the_base_caps_at_the_backoff_cap_and_stops_at_the_time_budget() {
        struct Case {
            name: &'static str,
            consecutive_failures: u32,
            elapsed: Duration,
            expect: Option<Duration>,
        }
        let budget = ReconcileBudget {
            max_rounds: 3,
            byte_budget: 1_000,
            time_budget: Duration::from_secs(1),
            backoff_cap: Duration::from_millis(500),
        };
        let cases = [
            Case {
                name: "first failure waits the base delay",
                consecutive_failures: 1,
                elapsed: Duration::ZERO,
                expect: Some(Duration::from_millis(200)),
            },
            Case {
                name: "second failure doubles it",
                consecutive_failures: 2,
                elapsed: Duration::ZERO,
                expect: Some(Duration::from_millis(400)),
            },
            Case {
                name: "third failure caps at the backoff cap",
                consecutive_failures: 3,
                elapsed: Duration::ZERO,
                expect: Some(Duration::from_millis(500)),
            },
            Case {
                name: "a failure count past the shift width still caps, never overflows",
                consecutive_failures: 40,
                elapsed: Duration::ZERO,
                expect: Some(Duration::from_millis(500)),
            },
            Case {
                name: "a zero failure count is treated as the first",
                consecutive_failures: 0,
                elapsed: Duration::ZERO,
                expect: Some(Duration::from_millis(200)),
            },
            Case {
                name: "a wait that still ends inside the time budget is taken",
                consecutive_failures: 1,
                elapsed: Duration::from_millis(799),
                expect: Some(Duration::from_millis(200)),
            },
            Case {
                name: "a wait that would land exactly on the deadline stops the loop",
                consecutive_failures: 1,
                elapsed: Duration::from_millis(800),
                expect: None,
            },
            Case {
                name: "time budget already spent",
                consecutive_failures: 1,
                elapsed: Duration::from_secs(1),
                expect: None,
            },
            Case {
                name: "time budget already exceeded",
                consecutive_failures: 2,
                elapsed: Duration::from_secs(3),
                expect: None,
            },
        ];
        for case in cases {
            assert_eq!(
                retry_delay(case.consecutive_failures, case.elapsed, &budget),
                case.expect,
                "case: {}",
                case.name
            );
        }
    }

    /// Pins that `should_keep_reconciling` stops at whichever bound (rounds,
    /// bytes, time, or nothing left diverging) is hit first.
    #[test]
    fn should_keep_reconciling_stops_at_whichever_bound_is_hit_first() {
        struct Case {
            name: &'static str,
            rounds_run: u32,
            bytes_moved: u64,
            still_diverging: usize,
            elapsed: Duration,
            expect_continue: bool,
        }
        let budget = ReconcileBudget {
            max_rounds: 3,
            byte_budget: 1_000,
            time_budget: Duration::from_secs(1),
            backoff_cap: Duration::from_millis(200),
        };
        let cases = [
            Case {
                name: "rounds exhausted",
                rounds_run: 3,
                bytes_moved: 0,
                still_diverging: 5,
                elapsed: Duration::ZERO,
                expect_continue: false,
            },
            Case {
                name: "rounds past the cap",
                rounds_run: 4,
                bytes_moved: 0,
                still_diverging: 5,
                elapsed: Duration::ZERO,
                expect_continue: false,
            },
            Case {
                name: "byte budget exhausted",
                rounds_run: 0,
                bytes_moved: 1_000,
                still_diverging: 5,
                elapsed: Duration::ZERO,
                expect_continue: false,
            },
            Case {
                name: "byte budget exceeded",
                rounds_run: 0,
                bytes_moved: 1_001,
                still_diverging: 5,
                elapsed: Duration::ZERO,
                expect_continue: false,
            },
            Case {
                name: "nothing left diverging",
                rounds_run: 0,
                bytes_moved: 0,
                still_diverging: 0,
                elapsed: Duration::ZERO,
                expect_continue: false,
            },
            Case {
                name: "one round ran with zero progress, two left",
                rounds_run: 1,
                bytes_moved: 0,
                still_diverging: 5,
                elapsed: Duration::ZERO,
                expect_continue: true,
            },
            Case {
                name: "under every bound with work left and the time budget nearly spent",
                rounds_run: 1,
                bytes_moved: 10,
                still_diverging: 2,
                elapsed: Duration::from_millis(999),
                expect_continue: true,
            },
        ];
        for case in cases {
            assert_eq!(
                should_keep_reconciling(
                    case.rounds_run,
                    case.bytes_moved,
                    case.still_diverging,
                    case.elapsed,
                    &budget
                ),
                case.expect_continue,
                "case: {}",
                case.name
            );
        }
    }

    /// Pins that `split_round_result` folds a `failed` round as every bucket
    /// diverging regardless of `matched`, else splits by membership in it.
    #[test]
    fn split_round_result_folds_a_failed_round_and_otherwise_splits_by_matched() {
        struct Case {
            name: &'static str,
            requested: &'static [u16],
            outcome: anti_entropy::BucketRoundOutcome,
            expect_converged: &'static [u16],
            expect_diverging: &'static [u16],
        }
        let cases = [
            Case {
                name: "a failed round leaves every requested bucket diverging, even one \
                       nominally in matched",
                requested: &[1, 2, 3],
                outcome: anti_entropy::BucketRoundOutcome {
                    matched: HashSet::from([1]),
                    still_diverged: HashSet::new(),
                    bytes_moved: 0,
                    failed: true,
                },
                expect_converged: &[],
                expect_diverging: &[1, 2, 3],
            },
            Case {
                name: "a successful round splits matched from still-diverging",
                requested: &[1, 2, 3],
                outcome: anti_entropy::BucketRoundOutcome {
                    matched: HashSet::from([1, 3]),
                    still_diverged: HashSet::from([2]),
                    bytes_moved: 500,
                    failed: false,
                },
                expect_converged: &[1, 3],
                expect_diverging: &[2],
            },
            Case {
                name: "everything requested matched: nothing left diverging",
                requested: &[4, 5],
                outcome: anti_entropy::BucketRoundOutcome {
                    matched: HashSet::from([4, 5]),
                    still_diverged: HashSet::new(),
                    bytes_moved: 0,
                    failed: false,
                },
                expect_converged: &[4, 5],
                expect_diverging: &[],
            },
        ];
        for case in cases {
            let (converged, diverging) = split_round_result(case.requested, &case.outcome);
            assert_eq!(converged, case.expect_converged, "case: {}", case.name);
            assert_eq!(diverging, case.expect_diverging, "case: {}", case.name);
        }
    }

    /// A `Mode::Distributed` shard-and-context pair, `attach_ownership`'s
    /// shape, for testing without a full `open()`.
    fn distributed_context_for_test(
        cluster: &Cluster,
        name: &SmolStr,
    ) -> (Arc<dyn ShardOps>, DistributedContext) {
        let shard = Shard::<u32, String>::new(
            name.clone(),
            Mode::distributed(),
            cluster.node_id(),
            u64::MAX,
            None,
            None,
        );
        let (shard, distributed) = attach_ownership(shard, cluster, name, Mode::distributed());
        let distributed = distributed.expect("Mode::distributed always attaches a context");
        (Arc::new(shard), distributed)
    }

    #[tokio::test]
    async fn reconcile_warm_buckets_leaves_a_bucket_cold_with_no_live_co_owner_to_verify_against() {
        let cluster = Cluster::builder("cache-it-reconcile-alone")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let name = SmolStr::new("scratch");
        let (shard_ops, distributed) = distributed_context_for_test(&cluster, &name);

        let warm_parts: Vec<PartId> = [0u16, 500, 1023]
            .into_iter()
            .flat_map(PartId::of_bucket)
            .collect();
        distributed.residency.mark_cold(&warm_parts);
        let reconciled = reconcile_warm_buckets(
            &cluster,
            &shard_ops,
            &name,
            &distributed.ownership,
            &distributed.residency,
            &warm_parts,
        )
        .await;

        assert!(
            reconciled.is_empty(),
            "a bucket with no live co-owner has nobody to verify its replayed state against, \
             so reconcile_warm_buckets itself leaves it cold rather than vacuously treating it \
             as reconciled -- it falls through to PullRequest::run's own \"owned alone\" case \
             next, which is the site that actually decides a co-owner-less bucket is servable"
        );
        for part in warm_parts {
            assert!(
                distributed.residency.is_cold(part),
                "part {part} stays cold with nobody for reconcile_warm_buckets itself to \
                 reconcile against, falling through to the ordinary cold-pull path"
            );
        }

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn reconcile_warm_buckets_never_runs_a_round_for_an_empty_bucket_list() {
        let cluster = Cluster::builder("cache-it-reconcile-empty")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let name = SmolStr::new("scratch");
        let (shard_ops, distributed) = distributed_context_for_test(&cluster, &name);

        let reconciled = reconcile_warm_buckets(
            &cluster,
            &shard_ops,
            &name,
            &distributed.ownership,
            &distributed.residency,
            &[],
        )
        .await;

        assert!(
            reconciled.is_empty(),
            "an empty warm-bucket list, the non-warm-reopen case, never runs a round"
        );

        cluster.shutdown().await;
    }

    /// Pins the core reconciliation mechanism against a real live co-owner:
    /// `c` starts cold-seeded, pulls `b`'s real data via a genuine
    /// anti-entropy round, and only then clears cold via `mark_serving`.
    #[tokio::test]
    async fn reconcile_warm_buckets_clears_cold_once_a_real_round_against_a_live_co_owner_reconciles()
     {
        const TOTAL: u32 = 200;
        let name = SmolStr::new("reconcile-real-round");

        let b = Cluster::builder("cache-it-reconcile-real-round")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("b builds");
        let cache_b = b
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone, owning every bucket");
        for key in 0..TOTAL {
            cache_b
                .insert(key, format!("v{key}"))
                .await
                .expect("b owns every bucket while alone");
        }

        let c = Cluster::builder("cache-it-reconcile-real-round")
            .seeds([b.local_gossip_addr()])
            .config(loopback_config())
            .build()
            .await
            .expect("c builds");
        wait_for_peer_count(&b, 1).await;
        wait_for_peer_count(&c, 1).await;
        // Bypasses `open()`'s own advertise call: without it b never learns c
        // has `name` open, so every round against it answers `Stale`.
        c.advertise_cache_mode(&name, Mode::distributed());

        // c's shard for `name`, built directly rather than through a real `open()`.
        let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
        let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
        assert!(
            !warm_parts.is_empty(),
            "with only b and c eligible at owners=2, c owns every bucket"
        );
        distributed_c.residency.mark_cold(&warm_parts);

        // b's tracker catches up to c joining on its own refresh cadence,
        // answering every round `Stale` until then, so this polls rather
        // than assuming one round suffices.
        let mut reconciled: HashSet<PartId> = HashSet::new();
        wait_until(
            Duration::from_secs(10),
            "b's view catches up to c joining, so the round against it reconciles",
            async || {
                reconciled = reconcile_warm_buckets(
                    &c,
                    &shard_ops_c,
                    &name,
                    &distributed_c.ownership,
                    &distributed_c.residency,
                    &warm_parts,
                )
                .await;
                reconciled.len() == warm_parts.len()
            },
        )
        .await;

        for &bucket in &warm_parts {
            assert!(
                !distributed_c.residency.is_cold(bucket),
                "bucket {bucket} clears cold once its digest matches b's in a round against it"
            );
        }
        let recovered: usize = shard_ops_c
            .entries_for_buckets(buckets_of(&warm_parts))
            .await
            .into_iter()
            .map(|(_, entries)| entries.len())
            .sum();
        assert_eq!(
            recovered, TOTAL as usize,
            "the round actually pulled b's data in, not merely declared the buckets reconciled"
        );

        c.shutdown().await;
        b.shutdown().await;
    }

    /// Pins [`reconcile_warm_buckets`]'s per-peer processing: three real
    /// nodes so `c`'s warm buckets split across two live co-owners (`b` and
    /// `d`), and one call converges every bucket regardless of which peer
    /// it's grouped under.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end three-node scenario (settle b/c/d, split c's warm buckets \
                  across b and d, reconcile, assert both peers' shares converge): splitting it \
                  would only scatter state (b, c, d, shard_ops_c, distributed_c, warm_parts) \
                  across helper signatures"
    )]
    async fn reconcile_warm_buckets_converges_buckets_split_across_two_live_co_owners() {
        use crate::cluster::test_support::registered_shard;

        let name = SmolStr::new("reconcile-two-peers");

        let b = Cluster::builder("cache-it-reconcile-two-peers")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("b builds");
        b.cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone, owning every bucket");

        let seed = b.local_gossip_addr();
        let c = Cluster::builder("cache-it-reconcile-two-peers")
            .seeds([seed])
            .config(loopback_config())
            .build()
            .await
            .expect("c builds");
        let d = Cluster::builder("cache-it-reconcile-two-peers")
            .seeds([seed])
            .config(loopback_config())
            .build()
            .await
            .expect("d builds");
        wait_for_peer_count(&b, 2).await;
        wait_for_peer_count(&c, 2).await;
        wait_for_peer_count(&d, 2).await;

        // Without this, neither b nor d learns c has `name` open, and every round answers Stale.
        c.advertise_cache_mode(&name, Mode::distributed());
        d.cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("d opens alone of data, owning its own share of buckets");

        let node_b = b.node_id();
        let node_d = d.node_id();

        // Until gossip carries d's mode to c, c sees only b as a co-owner.
        // Rebuilding the context on each retry (not just the round) is what
        // waits out that convergence instead of freezing an early view.
        let mut built: Option<SplitWarmContext> = None;
        wait_until(
            Duration::from_secs(10),
            "c's own gossiped view learns d also has `name` open, so c's warm buckets split \
             across both b and d rather than naming b the only co-owner",
            async || {
                let (shard_ops, distributed) = distributed_context_for_test(&c, &name);
                let warm_parts: Vec<PartId> =
                    distributed.ownership.current().owned_parts().collect();
                let view = distributed.ownership.current();
                let with_b = warm_parts
                    .iter()
                    .copied()
                    .find(|&bucket| view.owners_of(bucket).contains(&node_b));
                let with_d = warm_parts
                    .iter()
                    .copied()
                    .find(|&bucket| view.owners_of(bucket).contains(&node_d));
                match (with_b, with_d) {
                    (Some(part_with_b), Some(part_with_d)) => {
                        built = Some(SplitWarmContext {
                            shard_ops,
                            distributed,
                            warm_parts,
                            part_with_b,
                            part_with_d,
                        });
                        true
                    }
                    _ => false,
                }
            },
        )
        .await;
        let SplitWarmContext {
            shard_ops: shard_ops_c,
            distributed: distributed_c,
            warm_parts,
            part_with_b,
            part_with_d,
        } = built.expect("wait_until only returns once the closure itself reported ready");
        distributed_c.residency.mark_cold(&warm_parts);

        // One planted key per peer, applied straight to each donor's shard,
        // proves one call pulls data from both groups, not just whichever runs first.
        let key_from_b = key_for_part(part_with_b);
        let key_from_d = key_for_part(part_with_d);
        registered_shard(&b, &name)
            .apply_remote_batch(vec![test_record(key_from_b, "from-b", node_b)])
            .await;
        registered_shard(&d, &name)
            .apply_remote_batch(vec![test_record(key_from_d, "from-d", node_d)])
            .await;

        // b's and d's trackers catch up to c joining on their own refresh
        // cadence, the same real-time delay the single-peer test polls around.
        let mut reconciled: HashSet<PartId> = HashSet::new();
        wait_until(
            Duration::from_secs(10),
            "b's and d's views catch up to c joining, so both peers' rounds reconcile",
            async || {
                reconciled = reconcile_warm_buckets(
                    &c,
                    &shard_ops_c,
                    &name,
                    &distributed_c.ownership,
                    &distributed_c.residency,
                    &warm_parts,
                )
                .await;
                reconciled.len() == warm_parts.len()
            },
        )
        .await;

        for &bucket in &warm_parts {
            assert!(
                !distributed_c.residency.is_cold(bucket),
                "bucket {bucket} clears cold once its digest matches its live co-owner's, \
                 whether that co-owner is b or d"
            );
        }
        assert_record_value(&shard_ops_c, key_from_b, "from-b").await;
        assert_record_value(&shard_ops_c, key_from_d, "from-d").await;

        c.shutdown().await;
        d.shutdown().await;
        b.shutdown().await;
    }

    /// Raw-shard test context, rebuilt on each `wait_until` retry, plus
    /// which of `warm_parts` landed with each of the two live co-owners.
    struct SplitWarmContext {
        shard_ops: Arc<dyn ShardOps>,
        distributed: DistributedContext,
        warm_parts: Vec<PartId>,
        part_with_b: PartId,
        part_with_d: PartId,
    }

    /// The first `u32` key (bounded scan) whose part is `part`, for
    /// planting a record at a known part.
    fn key_for_part(part: PartId) -> u32 {
        (0u32..10_000_000)
            .find(|key| PartId::of_key(&encode_key(key).expect("u32 key encodes")) == part)
            .expect("some u32 key among the first ten million maps to every part")
    }

    /// The distinct buckets `parts` fall in, for the bucket-keyed
    /// [`ShardOps`] reads.
    fn buckets_of(parts: &[PartId]) -> Vec<u16> {
        let mut buckets: Vec<u16> = parts.iter().map(|part| part.bucket()).collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
    }

    /// A [`WireRecord`] for `key`/`value`, versioned at `node`, for seeding a shard directly.
    fn test_record(key: u32, value: &str, node: NodeId) -> WireRecord {
        WireRecord {
            key: encode_key(&key).expect("u32 encodes"),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&value.to_string()).expect("string encodes"),
            )),
            ver: crate::hlc::Hlc {
                wall_ms: 1,
                logical: 0,
                node,
            },
            expires_at_ms: None,
        }
    }

    /// Asserts `shard`'s live record for `key` decodes to `expected`.
    async fn assert_record_value(shard: &Arc<dyn ShardOps>, key: u32, expected: &str) {
        let recs = shard
            .records_for(vec![encode_key(&key).expect("u32 encodes")])
            .await;
        assert_eq!(
            recs.len(),
            1,
            "key {key} must have landed on the reconciled shard"
        );
        let value: String = recs[0]
            .value
            .as_ref()
            .map(|bytes| postcard::from_bytes(bytes).expect("string decodes"))
            .expect("a live record, not a tombstone");
        assert_eq!(
            value, expected,
            "key {key} must carry the value its donor planted"
        );
    }

    /// Guards CLAUDE.md's "deleted or expired entries never resurrect":
    /// a warm reopen's on-disk replay can carry a stale live copy of a key
    /// this node itself deleted before going down. `c` stands in for such a
    /// replay, seeded with a live record older than a tombstone `b` holds;
    /// `reconcile_warm_buckets` must correct it to the tombstone before the
    /// bucket clears cold.
    #[tokio::test]
    async fn reconcile_warm_buckets_corrects_a_stale_replayed_record_against_a_live_co_owners_tombstone()
     {
        let name = SmolStr::new("reconcile-resurrection-guard");
        let key = 7u32;
        // The default loopback config's short tombstone_ttl would race this
        // test's wait_until against a real GC sweep; a generous ttl avoids that.
        let config = ClusterConfig {
            tombstone_ttl: Duration::from_secs(300),
            ..loopback_config()
        };

        let b = Cluster::builder("cache-it-reconcile-resurrection")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("b builds");
        let cache_b = b
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone, owning every bucket");
        cache_b
            .insert(key, "v1".to_string())
            .await
            .expect("b owns every bucket while alone");
        cache_b
            .remove(&key)
            .await
            .expect("b deletes the key, stamping a tombstone newer than the stale replay below");

        let c = Cluster::builder("cache-it-reconcile-resurrection")
            .seeds([b.local_gossip_addr()])
            .config(config)
            .build()
            .await
            .expect("c builds");
        wait_for_peer_count(&b, 1).await;
        wait_for_peer_count(&c, 1).await;
        c.advertise_cache_mode(&name, Mode::distributed());

        let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
        let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
        assert!(
            !warm_parts.is_empty(),
            "with only b and c eligible at owners=2, c owns every bucket"
        );

        // Stands in for what a warm reopen's replay would install: a stale
        // live copy of `key`, versioned older than b's tombstone above,
        // applied via the same versioned-apply entry point replay uses.
        let stale = WireRecord {
            key: encode_key(&key).expect("u32 encodes"),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&"stale-v1".to_string()).expect("string encodes"),
            )),
            ver: crate::hlc::Hlc {
                wall_ms: 1,
                logical: 0,
                node: c.node_id(),
            },
            expires_at_ms: None,
        };
        shard_ops_c.apply_remote_batch(vec![stale]).await;
        distributed_c.residency.mark_cold(&warm_parts);

        // b's tracker catches up to c joining on its own refresh cadence, as above.
        let mut reconciled: HashSet<PartId> = HashSet::new();
        wait_until(
            Duration::from_secs(10),
            "b's view catches up to c joining, so the round against it reconciles",
            async || {
                reconciled = reconcile_warm_buckets(
                    &c,
                    &shard_ops_c,
                    &name,
                    &distributed_c.ownership,
                    &distributed_c.residency,
                    &warm_parts,
                )
                .await;
                reconciled.len() == warm_parts.len()
            },
        )
        .await;

        for &bucket in &warm_parts {
            assert!(
                !distributed_c.residency.is_cold(bucket),
                "bucket {bucket} clears cold once its digest matches b's in a round against it"
            );
        }

        let recs = shard_ops_c
            .records_for(vec![encode_key(&key).expect("u32 encodes")])
            .await;
        assert_eq!(
            recs.len(),
            1,
            "the reconciliation round must have applied b's tombstone for the key"
        );
        assert!(
            recs[0].is_tombstone(),
            "the stale replayed value this node itself deleted before going down must never be \
             served: the converge-before-serving loop against b, who holds the tombstone, \
             corrects it before the bucket ever clears cold"
        );

        c.shutdown().await;
        b.shutdown().await;
    }

    /// Pins that when the only co-owner is unreachable, every round fails
    /// and the warm bucket stays cold, falling through to the ordinary
    /// cold-pull path.
    #[tokio::test]
    async fn reconcile_warm_buckets_leaves_a_bucket_cold_when_its_only_co_owners_round_fails() {
        let name = SmolStr::new("reconcile-real-round-failed");

        let b = Cluster::builder("cache-it-reconcile-real-round-failed")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("b builds");
        let _cache_b = b
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone");

        let c = Cluster::builder("cache-it-reconcile-real-round-failed")
            .seeds([b.local_gossip_addr()])
            // Tightened so the test bounds itself instead of waiting out loopback_config's 5s.
            .config(loopback_config().with(|config| {
                config.state_transfer_budget = Duration::from_secs(1);
            }))
            .build()
            .await
            .expect("c builds");
        wait_for_peer_count(&b, 1).await;
        wait_for_peer_count(&c, 1).await;
        // Without this, b never learns c has `name` open, and every round answers Stale.
        c.advertise_cache_mode(&name, Mode::distributed());

        let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
        let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
        assert!(!warm_parts.is_empty());
        distributed_c.residency.mark_cold(&warm_parts);

        // Warms up to a first converged round first, so the round below
        // fails on b's unreachability specifically, not an unsettled view.
        wait_until(
            Duration::from_secs(10),
            "b's view catches up to c joining, so a warm-up round against it reconciles",
            async || {
                !reconcile_warm_buckets(
                    &c,
                    &shard_ops_c,
                    &name,
                    &distributed_c.ownership,
                    &distributed_c.residency,
                    &warm_parts,
                )
                .await
                .is_empty()
            },
        )
        .await;
        distributed_c.residency.mark_cold(&warm_parts);

        // c's gossip-derived live list still names b live briefly, so the
        // round below attempts and fails on the dial, not a no-co-owner skip.
        b.shutdown().await;

        let started = Instant::now();
        let reconciled = reconcile_warm_buckets(
            &c,
            &shard_ops_c,
            &name,
            &distributed_c.ownership,
            &distributed_c.residency,
            &warm_parts,
        )
        .await;

        assert!(
            reconciled.is_empty(),
            "a round against an unreachable co-owner never reconciles any bucket"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the retries stop at the 1s time budget or when b leaves the live list, never \
             running on to loopback_config's 5s: took {:?}",
            started.elapsed()
        );
        for &bucket in &warm_parts {
            assert!(
                distributed_c.residency.is_cold(bucket),
                "bucket {bucket} stays cold once its only co-owner's round fails, falling \
                 through to the ordinary cold-pull path rather than serving unverified"
            );
        }

        c.shutdown().await;
    }

    /// A bucket `0..total` never touches, so both sides' empty digests
    /// match once b's view stops answering `Stale`, untouched by real data.
    fn unused_bucket(total: u32) -> u16 {
        let used: HashSet<u16> = (0..total)
            .map(|key| bucket_of(&encode_key(&key).expect("u32 key encodes")))
            .collect();
        (0..u16::try_from(crate::store::BUCKET_COUNT).expect("BUCKET_COUNT fits u16"))
            .find(|bucket| !used.contains(bucket))
            .expect("BUCKET_COUNT buckets is far more than a small test's `total` can fill")
    }

    /// Polls `probe_bucket` (an [`unused_bucket`]) until a round against it
    /// stops answering `Stale`: b's tracker catching up to c joining. An
    /// untouched bucket avoids repairing the real data the caller tests.
    async fn wait_for_live_co_owner_view_to_settle(
        c: &Cluster,
        shard_ops_c: &Arc<dyn ShardOps>,
        name: &SmolStr,
        node_b: NodeId,
        probe_bucket: u16,
    ) {
        wait_until(
            Duration::from_secs(10),
            "b's view catches up to c joining, so a round against it stops answering Stale",
            async || {
                !anti_entropy::run_round_for_buckets(
                    c.mesh(),
                    shard_ops_c,
                    name,
                    node_b,
                    &[probe_bucket],
                )
                .await
                .failed
            },
        )
        .await;
    }

    /// A single `reconcile_warm_buckets` call must itself loop more than one
    /// round to close a real gap: a round reports a bucket `still_diverged`
    /// from the mismatch list before the repair lands, so only a later
    /// round's fresh digest exchange reports it `matched`.
    /// [`RECONCILE_MAX_ROUNDS`] gives the loop that confirming round.
    ///
    /// `b`'s view is warmed up first against an untouched bucket, isolating
    /// the two-rounds-needed behavior from b's own refresh-cadence delay.
    #[tokio::test]
    async fn reconcile_warm_buckets_loops_a_bounded_number_of_times_when_one_round_cannot_close_the_gap()
     {
        const TOTAL: u32 = 50;
        let name = SmolStr::new("reconcile-needs-two-rounds");

        let b = Cluster::builder("cache-it-reconcile-needs-two-rounds")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("b builds");
        let node_b = b.node_id();
        let cache_b = b
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone, owning every bucket");
        for key in 0..TOTAL {
            cache_b
                .insert(key, format!("v{key}"))
                .await
                .expect("b owns every bucket while alone");
        }
        let probe_bucket = unused_bucket(TOTAL);
        let divergent_parts: HashSet<PartId> = (0..TOTAL)
            .map(|key| PartId::of_key(&encode_key(&key).expect("u32 key encodes")))
            .collect();

        let c = Cluster::builder("cache-it-reconcile-needs-two-rounds")
            .seeds([b.local_gossip_addr()])
            .config(loopback_config())
            .build()
            .await
            .expect("c builds");
        wait_for_peer_count(&b, 1).await;
        wait_for_peer_count(&c, 1).await;
        c.advertise_cache_mode(&name, Mode::distributed());

        let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
        let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
        assert!(
            !warm_parts.is_empty(),
            "with only b and c eligible at owners=2, c owns every bucket"
        );
        distributed_c.residency.mark_cold(&warm_parts);

        wait_for_live_co_owner_view_to_settle(&c, &shard_ops_c, &name, node_b, probe_bucket).await;

        // A single call: if it ran only one round, a repaired bucket would
        // still report `still_diverged`.
        let reconciled = reconcile_warm_buckets(
            &c,
            &shard_ops_c,
            &name,
            &distributed_c.ownership,
            &distributed_c.residency,
            &warm_parts,
        )
        .await;

        assert_eq!(
            reconciled.len(),
            warm_parts.len(),
            "a single call's own internal loop, not an external retry, closes the gap: round \
             one repairs the divergence but reports it still diverged, round two's fresh digest \
             exchange confirms the match"
        );
        for &bucket in &divergent_parts {
            assert!(
                !distributed_c.residency.is_cold(bucket),
                "bucket {bucket} clears cold once the loop's confirming round finds no mismatch"
            );
        }
        let recovered: usize = shard_ops_c
            .entries_for_buckets(buckets_of(&warm_parts))
            .await
            .into_iter()
            .map(|(_, entries)| entries.len())
            .sum();
        assert_eq!(
            recovered, TOTAL as usize,
            "the loop's first round actually pulled b's data in"
        );

        c.shutdown().await;
        b.shutdown().await;
    }

    /// The other side of the loop-needs-two-rounds test: a byte budget too
    /// low for a second round makes [`should_keep_reconciling`] refuse it,
    /// so a repaired-but-unconfirmed bucket stays cold and unverified rather
    /// than clearing via `mark_serving` on an unconfirmed repair. Untouched
    /// buckets in the same round still converge immediately.
    #[tokio::test]
    async fn reconcile_warm_buckets_stops_at_the_byte_budget_and_leaves_the_bucket_cold_and_unverified()
     {
        const TOTAL: u32 = 50;
        let name = SmolStr::new("reconcile-byte-budget-stop");

        let b = Cluster::builder("cache-it-reconcile-byte-budget-stop")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("b builds");
        let node_b = b.node_id();
        let cache_b = b
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone, owning every bucket");
        for key in 0..TOTAL {
            cache_b
                .insert(key, format!("v{key}"))
                .await
                .expect("b owns every bucket while alone");
        }
        let probe_bucket = unused_bucket(TOTAL);
        let divergent_parts: HashSet<PartId> = (0..TOTAL)
            .map(|key| PartId::of_key(&encode_key(&key).expect("u32 key encodes")))
            .collect();

        // A budget so small the first repair byte exceeds it: one round only.
        let config = ClusterConfig {
            reconcile_byte_budget: 1,
            ..loopback_config()
        };
        let c = Cluster::builder("cache-it-reconcile-byte-budget-stop")
            .seeds([b.local_gossip_addr()])
            .config(config)
            .build()
            .await
            .expect("c builds");
        wait_for_peer_count(&b, 1).await;
        wait_for_peer_count(&c, 1).await;
        c.advertise_cache_mode(&name, Mode::distributed());

        let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
        let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
        assert!(
            !warm_parts.is_empty(),
            "with only b and c eligible at owners=2, c owns every bucket"
        );
        distributed_c.residency.mark_cold(&warm_parts);

        wait_for_live_co_owner_view_to_settle(&c, &shard_ops_c, &name, node_b, probe_bucket).await;

        let reconciled = reconcile_warm_buckets(
            &c,
            &shard_ops_c,
            &name,
            &distributed_c.ownership,
            &distributed_c.residency,
            &warm_parts,
        )
        .await;

        assert!(
            divergent_parts
                .iter()
                .all(|bucket| !reconciled.contains(bucket)),
            "the byte budget stops the loop after its one repairing round, before the \
             confirming round that would have reconciled the genuinely divergent buckets"
        );
        for &bucket in &divergent_parts {
            assert!(
                distributed_c.residency.is_cold(bucket),
                "bucket {bucket} stays cold: the budget cut the loop off before a confirming \
                 round ever ran for it, so no ResidencySet call was ever made for it"
            );
        }
        let recovered: usize = shard_ops_c
            .entries_for_buckets(buckets_of(&warm_parts))
            .await
            .into_iter()
            .map(|(_, entries)| entries.len())
            .sum();
        assert_eq!(
            recovered, TOTAL as usize,
            "the one round the budget allowed still genuinely repaired the divergence -- the \
             bucket is left unverified, not un-repaired"
        );

        c.shutdown().await;
        b.shutdown().await;
    }

    /// Joins a fresh node onto `cluster_name`'s chitchat cluster (three
    /// nodes total once this and the seed and a later joiner are all up, so
    /// `owners = 2` leaves each bucket owned by exactly two of the three,
    /// and every node fails to own a real share of buckets) and opens
    /// `cache_name` as `Mode::distributed()`, two owners, on it.
    async fn join_distributed(
        seed_addr: SocketAddr,
        cluster_name: &'static str,
        cache_name: &'static str,
        peer_count: usize,
    ) -> (Cluster, Cache<u32, String>) {
        let cluster = Cluster::builder(cluster_name)
            .seeds([seed_addr])
            .config(loopback_config())
            .build()
            .await
            .expect("node builds");
        wait_for_peer_count(&cluster, peer_count).await;
        let cache = tokio::time::timeout(
            Duration::from_secs(20),
            cluster
                .cache::<u32, String>(cache_name)
                .mode(Mode::distributed())
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("open succeeds");
        (cluster, cache)
    }

    /// A three-node `Mode::Distributed { owners: 2 }` cluster, all open on
    /// `cache_name`, plus a key whose bucket the first node does not own
    /// (its live owners are exactly the second and third nodes).
    async fn three_node_distributed(
        cluster_name: &'static str,
        cache_name: &'static str,
    ) -> (
        (Cluster, Cache<u32, String>),
        (Cluster, Cache<u32, String>),
        (Cluster, Cache<u32, String>),
        u32,
    ) {
        let a = Cluster::builder(cluster_name)
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("node a builds");
        let cache_a = a
            .cache::<u32, String>(cache_name)
            .mode(Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            })
            .open()
            .await
            .expect("a opens alone, owning everything");
        let seed = a.local_gossip_addr();

        let (b, cache_b) = join_distributed(seed, cluster_name, cache_name, 1).await;
        wait_for_peer_count(&a, 1).await;
        let (c, cache_c) = join_distributed(seed, cluster_name, cache_name, 2).await;
        wait_for_peer_count(&a, 2).await;
        wait_for_peer_count(&b, 2).await;

        // A key whose bucket `a` does not own: with three eligible nodes and
        // owners = 2, roughly a third of keys land here. `a`'s ownership
        // view only recomputes once its background refresh task has
        // observed both the membership change above and the gossiped
        // cache-mode advertisement `b`/`c` sent on their own `open()`,
        // strictly a separate, slightly later event than peer liveness,
        // so this polls rather than searching the instant peers converge.
        let unowned_key = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(key) =
                    (0..10_000u32).find(|&k| !cache_a.owners_of(&k).contains(&a.node_id()))
                {
                    return key;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a's ownership view converges to include b and c within the bound");

        ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key)
    }

    /// `b` accepts any stamp; `a` bounds skew at one second. A record
    /// stamped an hour ahead that `b` holds never reaches `a` through
    /// anti-entropy, `a`'s clock never moves toward it, and `b`, whose
    /// clock absorbed it, has its later writes refused by `a` too.
    #[tokio::test]
    async fn a_record_stamped_past_max_clock_skew_never_reaches_a_bounded_node() {
        let name = "cache-it-clock-skew";
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(loopback_config().with(|c| c.max_clock_skew = Some(Duration::from_secs(1))))
            .build()
            .await
            .expect("a builds");
        let b = Cluster::builder(name)
            .seeds([a.local_gossip_addr()])
            .config(loopback_config().with(|c| c.max_clock_skew = None))
            .build()
            .await
            .expect("b builds");
        wait_for_peer_count(&a, 1).await;
        let open = |cluster: &Cluster| {
            cluster
                .cache::<u32, String>("prices")
                .mode(Mode::Replicated)
                .open()
        };
        let cache_a = open(&a).await.expect("a opens");
        let cache_b = open(&b).await.expect("b opens");

        cache_b.insert(2, "before".into()).await.expect("insert");
        wait_until(
            Duration::from_secs(10),
            "b's write stamped by its own clock reaches a",
            async || cache_a.get(&2).await.is_some(),
        )
        .await;

        let ahead = crate::hlc::Hlc {
            wall_ms: crate::store::now_ms() + 3_600_000,
            logical: 0,
            node: b.node_id(),
        };
        ShardOps::apply_remote(
            cache_b.shard.as_ref(),
            crate::wire::WireRecord {
                key: bytes::Bytes::from(postcard::to_stdvec(&1u32).expect("key encodes")),
                value: Some(bytes::Bytes::from(
                    postcard::to_stdvec(&"ahead".to_string()).expect("value encodes"),
                )),
                ver: ahead,
                expires_at_ms: None,
            },
        )
        .await;
        assert_eq!(cache_b.get(&1).await.as_deref(), Some("ahead"));
        cache_b.insert(4, "after".into()).await.expect("insert");

        // Ten anti-entropy intervals: every round offers a both keys.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            cache_a.get(&1).await,
            None,
            "a refuses the record stamped an hour ahead"
        );
        assert_eq!(
            cache_a.get(&4).await,
            None,
            "b's clock absorbed the hour, so its next write is refused as well"
        );

        cache_a.insert(3, "local".into()).await.expect("insert");
        let key = bytes::Bytes::from(postcard::to_stdvec(&3u32).expect("key encodes"));
        let held = ShardOps::bucket_entries(cache_a.shard.as_ref(), crate::store::bucket_of(&key))
            .await
            .into_iter()
            .find(|entry| entry.key == key)
            .expect("a holds its own write");
        assert!(
            held.version.wall_ms < ahead.wall_ms - 3_000_000,
            "a's clock never took the refused stamp"
        );

        b.shutdown().await;
        a.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_returns_local_value_without_a_network_request_when_this_node_owns_the_bucket() {
        let cluster = Cluster::builder("cache-it-fetch-local")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, String>("prices")
            .mode(Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            })
            .open()
            .await
            .expect("a solo node opens warm, owning every bucket");
        cache.insert(1, "one".to_string()).await.expect("insert");

        assert_eq!(
            cache.fetch(&1).await.expect("fetch succeeds"),
            Some("one".to_string()),
            "a solo node owns every bucket, so fetch reads locally"
        );
        assert_eq!(
            cache.fetch(&2).await.expect("fetch succeeds"),
            None,
            "a genuine local miss is Ok(None), not an error"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_reaches_a_remote_owner_and_returns_its_value() {
        let ((a, cache_a), (b, _cache_b), (c, cache_c), unowned_key) =
            three_node_distributed("cache-it-fetch-remote", "prices").await;

        // Written through whichever of the two real owners it lands on;
        // `insert` on a non-owner forwards, so writing through `c` (an
        // owner or not) always reaches the true owners either way.
        cache_c
            .insert(unowned_key, "remote".to_string())
            .await
            .expect("insert");

        wait_until(
            Duration::from_secs(10),
            "fetch reaches a real owner and returns its value within the bound",
            async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "remote"),
        )
        .await;

        assert_eq!(
            cache_a.get(&unowned_key).await,
            None,
            "fetch never promotes the value into a's own local store"
        );

        a.shutdown().await;
        b.shutdown().await;
        c.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_asks_the_other_owner_for_a_miss_in_a_cold_bucket_and_answers_a_miss_once_every_owner_is_cold()
     {
        // Two nodes, two owners per bucket: both own every bucket. Once
        // both are warm, `a` drops its copy of a key and marks the bucket
        // cold again by hand, so a fetch on `a` has to ask `b`; then `b`
        // does the same, and `a`'s fetch is a miss rather than an error.
        // Anti-entropy is effectively off: it would repair a dropped copy
        // from the co-owner and race every step below.
        let name = "distributed-cold-fetch";
        let mut config = loopback_config();
        config.ae_interval = Duration::from_secs(3600);
        config.tombstone_ttl = config.bucket_release_window();
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let cache_a = a
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect("a opens alone");
        let b = Cluster::builder(name)
            .seeds([a.local_gossip_addr()])
            .config(config)
            .build()
            .await
            .expect("node b builds");
        wait_for_peer_count(&b, 1).await;
        let cache_b = b
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens");
        wait_for_peer_count(&a, 1).await;
        cache_b
            .insert(7, "seven".to_string())
            .await
            .expect("insert");
        tokio::time::timeout(Duration::from_secs(10), async {
            while cache_a.get(&7).await.is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the write reaches its other owner");
        // Both warm-up pulls have landed: nothing re-delivers a copy the
        // steps below drop by hand.
        tokio::time::timeout(Duration::from_secs(20), async {
            while !(a.is_warm(&SmolStr::new(name)) && b.is_warm(&SmolStr::new(name))) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("both nodes finish warming up");
        let bucket = PartId::of_key(&encode_key(&7u32).expect("u32 encodes"));
        let residency_a = cache_a.shard.residency().expect("a is distributed");
        let residency_b = cache_b.shard.residency().expect("b is distributed");
        // Both views list both nodes, or the fetch below ends `Stale`.
        tokio::time::timeout(Duration::from_secs(10), async {
            while cache_a.owners_of(&7).len() != 2 || cache_b.owners_of(&7).len() != 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("both views settle on two owners");

        cache_a.invalidate_local(&7).await;
        residency_a.mark_cold(&[bucket]);
        assert_eq!(
            cache_a.fetch(&7).await.expect("fetch reaches b").as_deref(),
            Some("seven"),
            "a cold local miss is answered by the other owner"
        );
        assert_eq!(cache_a.get(&7).await, None, "fetch never promotes locally");

        cache_b.invalidate_local(&7).await;
        residency_b.mark_cold(&[bucket]);
        assert_eq!(
            cache_a.fetch(&7).await.expect("a miss, not an error"),
            None,
            "every owner cold and empty is a miss"
        );

        residency_a.clear_cold(&[bucket]);
        assert_eq!(
            cache_a.fetch(&7).await.expect("local"),
            None,
            "a warm bucket answers its own miss without asking anyone"
        );

        cache_b.close().await;
        cache_a.close().await;
        b.shutdown().await;
        a.shutdown().await;
    }

    /// Pins the unverified-bucket guard at the `Cache::fetch` layer: `a`'s
    /// shard is seeded with a stale replay of `key` that `b` deleted first.
    /// `fetch` must ask `b` while the bucket is unverified,
    /// then answer locally once reconciliation clears the marker.
    #[cfg(feature = "spill")]
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end two-node scenario (stage a stale replay, assert the unverified \
                  guard, reconcile, assert it serves locally again): splitting it would only \
                  scatter state (a, b, cache_a, cache_b, the shared key/bucket) across helper \
                  signatures"
    )]
    async fn fetch_never_returns_a_local_hit_from_an_unverified_bucket_until_reconciliation_clears_it()
     {
        let name = SmolStr::new("fetch-unverified-guard");
        let key = 7u32;
        let config = ClusterConfig {
            tombstone_ttl: Duration::from_secs(300),
            ..loopback_config()
        };

        let b = Cluster::builder("cache-it-fetch-unverified")
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("b builds");
        let cache_b = b
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone, owning every bucket");
        cache_b
            .insert(key, "v1".to_string())
            .await
            .expect("b owns every bucket while alone");
        cache_b
            .remove(&key)
            .await
            .expect("b deletes the key: the co-owner's own delete during a's downtime");

        let a = Cluster::builder("cache-it-fetch-unverified")
            .seeds([b.local_gossip_addr()])
            .config(config)
            .build()
            .await
            .expect("a builds");
        wait_for_peer_count(&b, 1).await;
        wait_for_peer_count(&a, 1).await;
        a.advertise_cache_mode(&name, Mode::distributed());

        // Built directly via `attach_ownership`, keeping the typed `Shard`
        // so a real `Cache` can wrap it.
        let shard = Shard::<u32, String>::new(
            name.clone(),
            Mode::distributed(),
            a.node_id(),
            u64::MAX,
            None,
            None,
        );
        let (shard, distributed) = attach_ownership(shard, &a, &name, Mode::distributed());
        let distributed = distributed.expect("Mode::distributed always attaches a context");
        let cache_a = Cache {
            shard: Arc::new(shard),
            cluster: a.clone(),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };

        let warm_parts: Vec<PartId> = distributed.ownership.current().owned_parts().collect();
        assert!(
            !warm_parts.is_empty(),
            "with only b and a eligible at owners=2, a owns every bucket"
        );
        let bucket = bucket_of(&encode_key(&key).expect("u32 encodes"));
        assert!(
            warm_parts.contains(&bucket),
            "with owners=2 and exactly two real nodes, a owns key's bucket too"
        );

        // Stands in for what a warm reopen's replay would install: a stale
        // live copy of `key`, via the same versioned-apply entry point replay uses.
        let shard_ops_a = Arc::clone(&cache_a.shard) as Arc<dyn ShardOps>;
        let stale = WireRecord {
            key: encode_key(&key).expect("u32 encodes"),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&"stale".to_string()).expect("string encodes"),
            )),
            ver: crate::hlc::Hlc {
                wall_ms: 1,
                logical: 0,
                node: a.node_id(),
            },
            expires_at_ms: None,
        };
        shard_ops_a.apply_remote_batch(vec![stale]).await;
        distributed.residency.mark_cold(&warm_parts);
        distributed.residency.mark_unverified(&warm_parts);

        assert_eq!(
            cache_a.fetch(&key).await.expect("b answers"),
            None,
            "the bucket is unverified: a's own stale hit is never trusted, so the fetch must \
             ask b, whose real tombstone is the correct answer"
        );
        assert_eq!(
            cache_a.get(&key).await,
            Some("stale".to_string()),
            "the stale record is still physically present locally; only the fetch path \
             refuses to trust it while unverified"
        );

        let mut reconciled: HashSet<PartId> = HashSet::new();
        wait_until(
            Duration::from_secs(10),
            "b's view catches up to a joining, so the round against it reconciles",
            async || {
                reconciled = reconcile_warm_buckets(
                    &a,
                    &shard_ops_a,
                    &name,
                    &distributed.ownership,
                    &distributed.residency,
                    &warm_parts,
                )
                .await;
                reconciled.len() == warm_parts.len()
            },
        )
        .await;
        assert!(
            !distributed.residency.is_unverified(bucket),
            "reconciliation clears the unverified mark alongside cold"
        );

        // b goes away entirely, so a's answer below must come from its own corrected state.
        cache_b.close().await;
        b.shutdown().await;

        assert_eq!(
            cache_a
                .fetch(&key)
                .await
                .expect("local, with no owner left to ask"),
            None,
            "reconciliation both corrected the stale copy and cleared unverified: the bucket \
             now serves its own tombstone locally, with no co-owner needed"
        );

        cache_a.close().await;
        a.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_retries_a_stale_owner_for_one_fetch_timeout_before_giving_up() {
        // Two solo clusters that never gossip, `b` injected into `a`'s mesh
        // by hand, so `b`'s view hash never matches the one `a` sends: every
        // fetch to `b` is answered `Stale`. `a`'s own view lists `b` and a
        // phantom third node, so it has buckets it does not own to fetch.
        let name = "distributed-fetch-stale";
        let mut config = loopback_config();
        config.fetch_timeout = Duration::from_millis(300);
        let a = Cluster::builder("distributed-fetch-stale-a")
            .seeds(std::iter::empty())
            .config(config)
            .build()
            .await
            .expect("node a builds");
        let b = Cluster::builder("distributed-fetch-stale-b")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("node b builds");
        let cache_b = b
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens alone");
        cache_b.insert(1, "one".to_string()).await.expect("insert");
        let peer_b = b.local_peer();
        a.mesh().update_peers(vec![peer_b.clone()]);

        let phantom = NodeId::from(u64::MAX);
        let owners = Mode::DEFAULT_OWNERS;
        let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
            a.node_id(),
            &[],
            &std::collections::HashMap::new(),
            &SmolStr::new(name),
            owners,
        );
        let view = Arc::new(crate::ownership::OwnershipView::compute(
            a.node_id(),
            vec![a.node_id(), peer_b.node, phantom],
            owners,
        ));
        tx.send(Arc::clone(&view)).expect("receiver alive");
        let shard = Shard::<u32, String>::new(
            SmolStr::new(name),
            Mode::distributed(),
            a.node_id(),
            u64::MAX,
            None,
            None,
        )
        .with_ownership(tracker, Arc::new(crate::ownership::ResidencySet::new()));
        let cache_a = Cache {
            shard: Arc::new(shard),
            cluster: a.clone(),
            cancel: CancellationToken::new(),
            tasks: TaskTracker::new(),
        };
        // A key `b` does not hold: a held record answers a fetch whatever
        // the views say, so only a miss exercises the stale retry.
        let key = (2..1_000_000u32)
            .find(|key| {
                let bucket = PartId::of_key(&encode_key(key).expect("u32 encodes"));
                !view.owns(bucket) && view.owners_of(bucket).contains(&peer_b.node)
            })
            .expect("a key owned by b and not by a");

        // A bucket `a` owns with only the phantom as co-owner: cold, its
        // one co-owner unreachable, a miss `a` cannot vouch for; warm, its
        // own miss is the answer.
        let cold_key = (2..1_000_000u32)
            .find(|key| {
                let bucket = PartId::of_key(&encode_key(key).expect("u32 encodes"));
                view.owns(bucket) && !view.owners_of(bucket).contains(&peer_b.node)
            })
            .expect("a key owned by a and the phantom");
        let cold_bucket = PartId::of_key(&encode_key(&cold_key).expect("u32 encodes"));
        let residency = cache_a.shard.residency().expect("a is distributed");
        residency.mark_cold(&[cold_bucket]);
        assert!(
            matches!(
                cache_a.fetch(&cold_key).await,
                Err(CacheError::FetchUnavailable { .. })
            ),
            "a cold owner with its only co-owner unreachable cannot vouch for a miss"
        );
        residency.clear_cold(&[cold_bucket]);
        assert_eq!(
            cache_a.fetch(&cold_key).await.expect("local"),
            None,
            "a warm owner answers its own miss"
        );

        let started = tokio::time::Instant::now();
        let result = cache_a.fetch(&key).await;
        let elapsed = started.elapsed();
        assert!(
            matches!(result, Err(CacheError::FetchUnavailable { .. })),
            "a permanently stale owner and an unreachable one leave nothing to answer: {result:?}"
        );
        assert!(
            elapsed >= Duration::from_millis(300),
            "the stale owner is retried for a whole fetch_timeout: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "the retries are bounded by the owners' windows: {elapsed:?}"
        );

        cache_b.close().await;
        b.shutdown().await;
        a.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_returns_fetch_unavailable_when_every_owner_is_down() {
        let ((a, cache_a), (b, cache_b), (c, _cache_c), unowned_key) =
            three_node_distributed("cache-it-fetch-unavailable", "prices").await;
        cache_b
            .insert(unowned_key, "value".to_string())
            .await
            .expect("insert");
        wait_until(
            Duration::from_secs(10),
            "the value reaches a real owner first, proving the key is genuinely resident",
            async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "value"),
        )
        .await;

        // Both of the key's real owners go down; `a` never owned it and
        // gossip has not yet had time to recompute `a`'s view around their
        // departure, so every dial fails outright.
        b.shutdown().await;
        c.shutdown().await;

        assert!(matches!(
            cache_a.fetch(&unowned_key).await,
            Err(CacheError::FetchUnavailable { cache }) if cache == "prices"
        ));

        a.shutdown().await;
    }

    #[tokio::test]
    async fn owners_of_matches_the_ownership_view_for_the_keys_bucket() {
        let ((a, cache_a), (b, _cache_b), (c, _cache_c), unowned_key) =
            three_node_distributed("cache-it-owners-of", "prices").await;

        let owners = cache_a.owners_of(&unowned_key);
        assert_eq!(owners.len(), 2, "owners = 2 for this cache");
        assert!(
            !owners.contains(&a.node_id()),
            "this key was chosen specifically because a does not own it"
        );
        // Every owner reported is a real, live node in this cluster.
        for owner in &owners {
            assert!(*owner == a.node_id() || *owner == b.node_id() || *owner == c.node_id());
        }

        a.shutdown().await;
        b.shutdown().await;
        c.shutdown().await;
    }

    /// The end-to-end proof that the write forward, the fan-out group-by-
    /// owner-set, and the fetch read path all compose: a write through a
    /// node that does not own the key's bucket never becomes locally
    /// visible on the writer, forwards to the real owners, and both the
    /// writer and another non-owner read the value back correctly through
    /// [`Cache::fetch`].
    #[tokio::test]
    async fn write_through_a_non_owner_composes_with_fetch_on_every_node() {
        let ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key) =
            three_node_distributed("cache-it-write-through-non-owner", "prices").await;

        // `a` was chosen specifically because it does not own this key's
        // bucket: this insert never touches a's engine, only forwards.
        cache_a
            .insert(unowned_key, "through-a".to_string())
            .await
            .expect("insert forwards");
        assert_eq!(
            cache_a.get(&unowned_key).await,
            None,
            "a forwarded write never becomes locally visible on the writer"
        );

        // Every node, owner or not, reads the same value back through
        // fetch: the writer itself, and whichever of b/c is not a real
        // owner either.
        let owners = cache_a.owners_of(&unowned_key);
        for cache in [&cache_a, &cache_b, &cache_c] {
            wait_until(
                Duration::from_secs(10),
                "fetch converges to the forwarded value on every node",
                async || matches!(cache.fetch(&unowned_key).await, Ok(Some(value)) if value == "through-a"),
            )
            .await;
        }
        assert_eq!(owners.len(), 2);

        a.shutdown().await;
        b.shutdown().await;
        c.shutdown().await;
    }

    #[tokio::test]
    async fn close_then_reopen_the_same_name_succeeds() {
        let cluster = Cluster::builder("cache-it-close-reopen")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cache = cluster
            .cache::<u32, String>("scratch")
            .open()
            .await
            .expect("first open succeeds");
        cache.insert(1, "a".into()).await.expect("insert");
        cache.close().await;

        let reopened = cluster
            .cache::<u32, String>("scratch")
            .open()
            .await
            .expect("closing frees the name for a fresh open");
        assert_eq!(
            reopened.get(&1).await,
            None,
            "the reopened cache starts empty, not resuming the closed shard's state"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn close_returns_only_once_every_background_task_has_stopped() {
        let cluster = Cluster::builder("cache-it-close-waits")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, String>("orders")
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("open succeeds");
        let tasks = cache.tasks.clone();
        assert!(
            !tasks.is_empty(),
            "a Replicated cache runs background tasks"
        );

        cache.close().await;

        assert!(tasks.is_closed() && tasks.is_empty());
        assert!(cluster.health().caches.is_empty());
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn closing_one_clone_leaves_another_clone_usable() {
        let cluster = Cluster::builder("cache-it-close-clone")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cache = cluster
            .cache::<u32, String>("shared")
            .open()
            .await
            .expect("open succeeds");
        let surviving = cache.clone();

        cache.close().await;

        surviving
            .insert(2, "still here".into())
            .await
            .expect("a surviving clone keeps writing locally after another clone closes");
        assert_eq!(
            surviving.get(&2).await,
            Some("still here".to_string()),
            "a surviving clone keeps reading locally after another clone closes"
        );
        assert!(
            surviving.shard.fan_out_queue().drain().is_empty(),
            "a detached clone queues nothing for a fan-out task that no longer runs"
        );

        cluster.shutdown().await;
    }

    #[test]
    fn validate_merge_window_accepts_zero_regardless_of_the_resolver() {
        assert!(validate_merge_window(Duration::ZERO, false));
        assert!(validate_merge_window(Duration::ZERO, true));
    }

    #[test]
    fn validate_merge_window_requires_a_merging_resolver_once_nonzero() {
        assert!(!validate_merge_window(Duration::from_millis(1), false));
        assert!(validate_merge_window(Duration::from_millis(1), true));
    }

    #[tokio::test]
    async fn merge_coalesce_window_rejects_a_non_merging_resolver() {
        let cluster = Cluster::builder("cache-it-merge-window-rejects-non-merging")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let err = cluster
            .cache::<u32, u32>("counters")
            .merge_coalesce_window(Duration::from_millis(5))
            .open()
            .await
            .expect_err("the default LwwResolver does not merge, so a nonzero window is rejected");
        assert!(matches!(
            err,
            CacheError::MergeWindowRequiresMergingResolver { .. }
        ));

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn prefold_enabled_defaults_to_true_and_the_toggle_reaches_the_shard() {
        let cluster = Cluster::builder("cache-it-prefold-enabled")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let on = cluster
            .cache::<u32, u32>("counters-on")
            .mode(Mode::Local)
            .open()
            .await
            .expect("open succeeds");
        assert!(
            on.shard.prefold_enabled(),
            "pre-fold defaults to on, matching the engine's own default"
        );

        let off = cluster
            .cache::<u32, u32>("counters-off")
            .mode(Mode::Local)
            .prefold_enabled(false)
            .open()
            .await
            .expect("open succeeds");
        assert!(
            !off.shard.prefold_enabled(),
            "CacheBuilder::prefold_enabled(false) reaches the shard's engine"
        );

        on.close().await;
        off.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn cache_builder_capacity_hint_reaches_the_shard() {
        let cluster = Cluster::builder("cache-it-capacity-hint")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let hint = 3_000u64;
        let expected = usize::try_from(hint.div_ceil(crate::store::BUCKET_COUNT as u64))
            .expect("hint fits usize");
        let cache = cluster
            .cache::<u32, u32>("counters")
            .mode(Mode::Local)
            .capacity_hint(hint)
            .open()
            .await
            .expect("open succeeds");
        assert_eq!(
            cache.shard.stripe_capacities(),
            vec![expected; crate::store::BUCKET_COUNT],
            "the hint reached the shard's engine, presizing every stripe"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn cache_stripe_capacities_matches_the_shards() {
        let cluster = Cluster::builder("cache-it-cache-stripe-capacities")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let hint = 500u64;
        let cache = cluster
            .cache::<u32, u32>("counters")
            .mode(Mode::Local)
            .capacity_hint(hint)
            .open()
            .await
            .expect("open succeeds");
        assert_eq!(
            cache.stripe_capacities(),
            cache.shard.stripe_capacities(),
            "Cache::stripe_capacities is a plain forward to the shard's own accessor, for a \
             crate outside sundog to reach it without seeing the private `shard` field"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn capacity_hint_above_max_capacity_is_clamped_without_a_weigher() {
        let cluster = Cluster::builder("cache-it-capacity-hint-clamped")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let max_capacity = 100u64;
        let hint = 3_000u64;
        let expected = usize::try_from(max_capacity.div_ceil(crate::store::BUCKET_COUNT as u64))
            .expect("clamped hint fits usize");
        let cache = cluster
            .cache::<u32, u32>("counters")
            .mode(Mode::Local)
            .max_capacity(max_capacity)
            .capacity_hint(hint)
            .open()
            .await
            .expect("open succeeds");
        assert_eq!(
            cache.shard.stripe_capacities(),
            vec![expected; crate::store::BUCKET_COUNT],
            "with no weigher, max_capacity bounds entry count, so a hint above it is clamped"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn capacity_hint_above_max_capacity_is_kept_as_is_with_a_weigher() {
        let cluster = Cluster::builder("cache-it-capacity-hint-unclamped")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let max_capacity = 100u64;
        let hint = 3_000u64;
        let expected = usize::try_from(hint.div_ceil(crate::store::BUCKET_COUNT as u64))
            .expect("hint fits usize");
        let cache = cluster
            .cache::<u32, u32>("counters")
            .mode(Mode::Local)
            .max_capacity(max_capacity)
            .weigher(|_key: &u32, _value: &u32| 1)
            .capacity_hint(hint)
            .open()
            .await
            .expect("open succeeds");
        assert_eq!(
            cache.shard.stripe_capacities(),
            vec![expected; crate::store::BUCKET_COUNT],
            "with a weigher, max_capacity bounds weight, not entry count, so the hint is kept as is"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn capacity_hint_defaults_to_none_and_matches_pre_change_rss() {
        let cluster = Cluster::builder("cache-it-capacity-hint-none")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cache = cluster
            .cache::<u32, u32>("counters")
            .mode(Mode::Local)
            .open()
            .await
            .expect("open succeeds");
        assert_eq!(
            cache.shard.stripe_capacities(),
            vec![0; crate::store::BUCKET_COUNT],
            "no hint: every stripe starts at zero allocation, matching an engine with no \
             capacity_hint at all"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn writer_id_pairs_the_node_with_the_cluster_incarnation() {
        let cluster = Cluster::builder("cache-it-writer-id")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, PnCounter>("counters")
            .resolver(Arc::new(PnCounterResolver))
            .open()
            .await
            .expect("open succeeds");

        let writer = cache.writer_id();
        assert_eq!(
            writer,
            WriterId::new(cluster.node_id(), cluster.local_incarnation()),
            "Cache::writer_id pairs this node's id with the cluster's own \
             membership incarnation, the same pairing WriterId::new takes"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn writer_id_is_stable_across_clones_and_repeated_calls() {
        let cluster = Cluster::builder("cache-it-writer-id-stable")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, PnCounter>("counters")
            .resolver(Arc::new(PnCounterResolver))
            .open()
            .await
            .expect("open succeeds");
        let clone = cache.clone();

        assert_eq!(
            cache.writer_id(),
            cache.writer_id(),
            "two calls on the same handle agree"
        );
        assert_eq!(
            cache.writer_id(),
            clone.writer_id(),
            "a clone shares the same underlying cluster membership, so it \
             reports the identical writer identity, never a fresh one"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    /// The end-to-end reason `Cache::writer_id` exists: a value merged under
    /// it round-trips through the exact merging path CRDT compaction relies
    /// on, exercised at the `Cache` layer rather than only unit-tested on
    /// `WriterId` itself.
    #[tokio::test]
    async fn a_value_merged_under_writer_id_reads_back_correctly() {
        let cluster = Cluster::builder("cache-it-writer-id-merge")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, PnCounter>("counters")
            .resolver(Arc::new(PnCounterResolver))
            .open()
            .await
            .expect("open succeeds");

        cache
            .merge(1, PnCounter::local_delta(cache.writer_id(), 5))
            .await
            .expect("merge");
        assert_eq!(cache.get(&1).await.map(|c| c.value()), Some(5));

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn merge_with_a_zero_window_applies_immediately() {
        let cluster = Cluster::builder("cache-it-merge-zero-window")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, PnCounter>("counters")
            .resolver(Arc::new(PnCounterResolver))
            .open()
            .await
            .expect("open succeeds");
        let writer = cache.writer_id();

        cache
            .merge(1, PnCounter::local_delta(writer, 1))
            .await
            .expect("merge");
        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(1),
            "a zero coalesce window (the default) applies every merge call at once, \
             like insert"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn merge_within_a_window_coalesces_into_one_apply_and_one_event() {
        let cluster = Cluster::builder("cache-it-merge-coalesce-window")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let window = Duration::from_millis(150);
        let cache = cluster
            .cache::<u32, PnCounter>("counters")
            .resolver(Arc::new(PnCounterResolver))
            .merge_coalesce_window(window)
            .open()
            .await
            .expect("open succeeds");
        let writer = cache.writer_id();
        let mut events = cache.events();

        cache
            .merge(1, PnCounter::local_delta(writer, 1))
            .await
            .expect("merge 1");
        cache
            .merge(1, PnCounter::local_delta(writer, 2))
            .await
            .expect("merge 2");
        cache
            .merge(1, PnCounter::local_delta(writer, 3))
            .await
            .expect("merge 3");

        assert_eq!(
            cache.get(&1).await,
            None,
            "a pending coalesced fold is invisible to get until its window flushes"
        );
        assert!(
            events.try_recv().is_err(),
            "nothing applies, so nothing publishes, before the window elapses"
        );

        tokio::time::sleep(window * 3).await;

        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(3),
            "the coalesced fold of all three calls lands as one write once the window \
             elapses"
        );
        match events
            .recv()
            .await
            .expect("exactly one event for the whole window")
        {
            Event::Created { key, .. } => assert_eq!(key, 1),
            other => panic!("expected Event::Created for the window's one apply, got {other:?}"),
        }
        assert!(
            events.try_recv().is_err(),
            "three merge calls in one window publish exactly one event, not three"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn close_flushes_a_pending_coalesced_merge() {
        let cluster = Cluster::builder("cache-it-merge-close-flush")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");
        let cache = cluster
            .cache::<u32, PnCounter>("counters")
            .resolver(Arc::new(PnCounterResolver))
            .merge_coalesce_window(Duration::from_secs(60))
            .open()
            .await
            .expect("open succeeds");
        let survivor = cache.clone();
        let writer = cache.writer_id();

        cache
            .merge(1, PnCounter::local_delta(writer, 7))
            .await
            .expect("merge");
        assert_eq!(
            survivor.get(&1).await,
            None,
            "still pending: the 60s window has not elapsed"
        );

        cache.close().await;

        assert_eq!(
            survivor.get(&1).await.map(|c| c.value()),
            Some(7),
            "close flushes whatever was still pending, regardless of the window"
        );

        cluster.shutdown().await;
    }

    /// A free loopback UDP port: bound and released, so a node can be
    /// seeded with a peer that does not exist yet.
    fn free_udp_port() -> u16 {
        std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("bind a loopback udp socket")
            .local_addr()
            .expect("bound socket has an address")
            .port()
    }

    #[tokio::test]
    async fn distributed_cache_rejects_a_tombstone_ttl_inside_the_release_window() {
        let name = "distributed-release-window-guard";
        let config = loopback_config().with(|c| {
            c.ae_interval = Duration::from_secs(3);
            c.distributed_disown_grace_rounds = 3;
            c.tombstone_ttl = Duration::from_secs(15);
        });
        let cluster = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(config)
            .build()
            .await
            .expect("the cluster builds; the rule binds distributed caches only");

        let err = cluster
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect_err("a distributed cache under a short retention is rejected");
        assert!(
            matches!(
                &err,
                CacheError::TombstoneTtlInsideReleaseWindow { cache, tombstone_ttl, window }
                    if cache == name
                        && *tombstone_ttl == Duration::from_secs(15)
                        && *window == Duration::from_secs(24)
            ),
            "{err:?}"
        );

        let replicated = cluster
            .cache::<u32, String>(name)
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("a replicated cache is not bound by the release window");
        replicated.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn distributed_cache_opened_before_any_peer_is_known_pulls_its_buckets_once_one_appears()
    {
        // `a` is seeded with the port `b` binds later, so `b` opens its
        // cache before anyone has gossiped to it: its first view is the
        // self-only one, owning every bucket with no co-owner to pull from.
        // Anti-entropy is effectively off, so only the warm-up pull can
        // bring `a`'s entries over.
        let name = "distributed-late-peer";
        let mut config = loopback_config();
        config.ae_interval = Duration::from_secs(3600);
        config.tombstone_ttl = config.bucket_release_window();
        config.gossip_interval = Duration::from_millis(200);
        // The released port can be taken by a test running alongside before
        // `b` binds it; then the pair is torn down and set up on a new one.
        let (a, cache_a, b) = 'setup: {
            for _ in 0..5 {
                let port_b = free_udp_port();
                let a = Cluster::builder(name)
                    .seeds([SocketAddr::from((Ipv4Addr::LOCALHOST, port_b))])
                    .config(config.clone())
                    .build()
                    .await
                    .expect("node a builds");
                let cache_a = a
                    .cache::<u32, String>(name)
                    .mode(Mode::distributed())
                    .open()
                    .await
                    .expect("a opens alone");
                assert!(
                    !a.is_warm(&SmolStr::new(name)),
                    "a cache with no co-owner in sight is not warm yet"
                );
                for key in 0..200u32 {
                    cache_a
                        .insert(key, format!("v{key}"))
                        .await
                        .expect("a owns every bucket while alone");
                }
                let mut config_b = config.clone();
                config_b.gossip_bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port_b));
                match Cluster::builder(name)
                    .seeds(std::iter::empty())
                    .config(config_b)
                    .build()
                    .await
                {
                    Ok(b) => break 'setup (a, cache_a, b),
                    Err(error) => {
                        eprintln!("port {port_b} was taken before b bound it ({error}); retrying");
                        cache_a.close().await;
                        a.shutdown().await;
                    }
                }
            }
            panic!("five pre-picked ports were all taken before b could bind one");
        };
        let cache_b = b
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect("b opens before it knows a");

        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let mut held = 0;
                for key in 0..200u32 {
                    if cache_b.get(&key).await.as_deref() == Some(format!("v{key}").as_str()) {
                        held += 1;
                    }
                }
                if held == 200 && b.is_warm(&SmolStr::new(name)) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("b pulls every bucket it owns from a once gossip shows a, and is warm");

        cache_b.close().await;
        cache_a.close().await;
        b.shutdown().await;
        a.shutdown().await;
    }

    /// Polls until every key in `0..total` is held by exactly the two
    /// owners every node's view agrees on, or panics after 30 seconds with
    /// a histogram of keys by holder count.
    async fn wait_until_settled_on_agreed_owners(
        nodes: &[(NodeId, &Cache<u32, String>)],
        total: u32,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let mut by_holders = [0u32; 4];
            let mut settled = 0u32;
            for key in 0..total {
                let expected = format!("v{key}");
                let mut owners: Vec<Vec<NodeId>> = Vec::new();
                let mut holders: Vec<NodeId> = Vec::new();
                for (node, cache) in nodes {
                    let mut view_owners = cache.owners_of(&key);
                    view_owners.sort_unstable();
                    owners.push(view_owners);
                    if cache.get(&key).await.as_deref() == Some(expected.as_str()) {
                        holders.push(*node);
                    }
                }
                holders.sort_unstable();
                by_holders[holders.len()] += 1;
                if owners.iter().all(|o| *o == owners[0])
                    && owners[0].len() == 2
                    && holders == owners[0]
                {
                    settled += 1;
                }
            }
            if settled == total {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "every key settles on exactly its two agreed owners with nothing lost; settled {settled} of {total}, keys by holder count [0, 1, 2, 3]: {by_holders:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn two_nodes_joining_at_once_lose_no_bucket_the_origin_releases() {
        // `a` holds everything alone. `b` and `c` learn of each other and of
        // `a` before either opens the cache, so both first views already
        // place about a third of the buckets on {b, c}: a pull from the
        // other joiner finds nothing there. `a`'s release hands those
        // buckets off before dropping them.
        let name = "distributed-double-join";
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("node a builds");
        let cache_a = a
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect("a opens alone");
        let total = 300u32;
        for key in 0..total {
            cache_a
                .insert(key, format!("v{key}"))
                .await
                .expect("a owns every bucket while alone");
        }
        let seed = a.local_gossip_addr();
        let b = Cluster::builder(name)
            .seeds([seed])
            .config(loopback_config())
            .build()
            .await
            .expect("node b builds");
        let c = Cluster::builder(name)
            .seeds([seed])
            .config(loopback_config())
            .build()
            .await
            .expect("node c builds");
        wait_for_peer_count(&b, 2).await;
        wait_for_peer_count(&c, 2).await;
        let (cache_b, cache_c) = tokio::join!(
            b.cache::<u32, String>(name)
                .mode(Mode::distributed())
                .open(),
            c.cache::<u32, String>(name)
                .mode(Mode::distributed())
                .open(),
        );
        let cache_b = cache_b.expect("b opens");
        let cache_c = cache_c.expect("c opens");

        // Past the disown grace (three anti-entropy intervals) plus the
        // hand-off, every key is held by exactly the two owners every view
        // agrees on: nothing lost, nothing lingering on the origin.
        let nodes = [
            (a.node_id(), &cache_a),
            (b.node_id(), &cache_b),
            (c.node_id(), &cache_c),
        ];
        let caches = [&cache_a, &cache_b, &cache_c];
        wait_until_settled_on_agreed_owners(&nodes, total).await;
        for key in 0..total {
            for cache in caches {
                assert_eq!(
                    cache
                        .fetch(&key)
                        .await
                        .expect("fetch reaches an owner")
                        .as_deref(),
                    Some(format!("v{key}").as_str()),
                    "key {key} is fetchable from every node"
                );
            }
        }

        cache_c.close().await;
        cache_b.close().await;
        cache_a.close().await;
        c.shutdown().await;
        b.shutdown().await;
        a.shutdown().await;
    }

    #[tokio::test]
    async fn forwarded_writes_reach_their_owners_when_the_writer_shuts_down_at_once() {
        // `a` forwards thousands of writes for buckets it does not own and
        // shuts down in the same breath: the fan-out task finishes the
        // batch, the mesh flushes it, and every key lands on the survivors,
        // which own everything between them once `a` is gone.
        let ((a, cache_a), (b, cache_b), (c, cache_c), _) =
            three_node_distributed("distributed-forward-flush", "forward-flush").await;
        let keys: Vec<u32> = (0..1_000_000u32)
            .filter(|key| !cache_a.owners_of(key).contains(&a.node_id()))
            .take(3_000)
            .collect();
        cache_a
            .insert_many(keys.iter().map(|&key| (key, format!("v{key}"))))
            .await
            .expect("insert_many forwards");
        cache_a.close().await;
        a.shutdown().await;

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let mut landed = 0usize;
                for key in &keys {
                    let expected = format!("v{key}");
                    if cache_b.get(key).await.as_deref() == Some(expected.as_str())
                        && cache_c.get(key).await.as_deref() == Some(expected.as_str())
                    {
                        landed += 1;
                    }
                }
                if landed == keys.len() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("every forwarded write lands on both survivors");

        cache_c.close().await;
        cache_b.close().await;
        c.shutdown().await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn a_forwarded_write_after_close_fails_with_closed_instead_of_vanishing() {
        let ((a, cache_a), (b, cache_b), (c, cache_c), key_a_does_not_own) =
            three_node_distributed("distributed-write-after-close", "write-after-close").await;
        let owned_by_a = (0..1_000_000u32)
            .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
            .expect("a owns some bucket");
        let late_handle = cache_a.clone();
        cache_a.close().await;
        assert!(
            matches!(
                late_handle
                    .insert(key_a_does_not_own, "late".to_string())
                    .await,
                Err(CacheError::Closed { .. })
            ),
            "a forwarded write after close is refused"
        );
        assert!(matches!(
            late_handle.remove(&key_a_does_not_own).await,
            Err(CacheError::Closed { .. })
        ));
        late_handle
            .insert(owned_by_a, "detached".to_string())
            .await
            .expect("a write this node applies itself still lands, detached");
        assert_eq!(
            late_handle.get(&owned_by_a).await.as_deref(),
            Some("detached")
        );

        cache_c.close().await;
        cache_b.close().await;
        c.shutdown().await;
        b.shutdown().await;
        a.shutdown().await;
    }

    /// Three nodes under `config`, each with `name` open as a
    /// `Mode::distributed()` cache and every peer count settled.
    async fn three_node_distributed_with(
        name: &'static str,
        config: ClusterConfig,
    ) -> (
        (Cluster, Cache<u32, String>),
        (Cluster, Cache<u32, String>),
        (Cluster, Cache<u32, String>),
    ) {
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(config.clone())
            .build()
            .await
            .expect("node a builds");
        let cache_a = a
            .cache::<u32, String>(name)
            .mode(Mode::distributed())
            .open()
            .await
            .expect("a opens alone");
        let seed = a.local_gossip_addr();
        let join = |peer_count: usize| {
            let config = config.clone();
            async move {
                let cluster = Cluster::builder(name)
                    .seeds([seed])
                    .config(config)
                    .build()
                    .await
                    .expect("node builds");
                wait_for_peer_count(&cluster, peer_count).await;
                let cache = cluster
                    .cache::<u32, String>(name)
                    .mode(Mode::distributed())
                    .open()
                    .await
                    .expect("node opens");
                (cluster, cache)
            }
        };
        let (b, cache_b) = join(1).await;
        wait_for_peer_count(&a, 1).await;
        let (c, cache_c) = join(2).await;
        wait_for_peer_count(&a, 2).await;
        wait_for_peer_count(&b, 2).await;
        ((a, cache_a), (b, cache_b), (c, cache_c))
    }

    /// A `WireRecord` for `key`/`value` as `node` would write it now.
    fn wire_record_from(key: u32, value: &str, node: NodeId) -> crate::wire::WireRecord {
        crate::wire::WireRecord {
            key: encode_key(&key).expect("u32 encodes"),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&value.to_string()).expect("string encodes"),
            )),
            ver: crate::hlc::Hlc {
                wall_ms: u64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("clock past the epoch")
                        .as_millis(),
                )
                .expect("fits"),
                logical: 0,
                node,
            },
            expires_at_ms: None,
        }
    }

    /// A write fanned out under a stale ownership view reaches only the
    /// owners that view names. The receiver's inbound loop notices the
    /// batch's view hash is not its own and re-forwards the records to
    /// their owners under its view, so the owner the writer missed gets
    /// the record without waiting for an anti-entropy round to pair the
    /// two; a batch already re-forwarded once travels no further.
    #[tokio::test]
    async fn a_forward_batch_under_a_stale_view_reaches_the_owner_the_writer_missed() {
        use crate::net::OutFrame;
        use crate::wire::{Msg, WireRecord};

        // Anti-entropy is effectively off: it would repair the missing
        // copy and hide whether the re-forward did.
        let name = "distributed-stale-forward";
        let mut config = loopback_config();
        config.ae_interval = Duration::from_secs(3600);
        config.tombstone_ttl = config.bucket_release_window();
        let ((a, cache_a), (b, cache_b), (c, cache_c)) =
            three_node_distributed_with(name, config).await;

        // Two keys whose owners are exactly `b` and `c`, under every node's
        // settled view.
        let mut keys = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let found: Vec<u32> = (0..100_000u32)
                    .filter(|key| {
                        let owners = cache_a.owners_of(key);
                        owners.len() == 2
                            && !owners.contains(&a.node_id())
                            && cache_b.owners_of(key) == owners
                            && cache_c.owners_of(key) == owners
                    })
                    .take(2)
                    .collect();
                if found.len() == 2 {
                    return found;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("every view settles on b and c owning some bucket");
        let hop_one_key = keys.pop().expect("two keys");
        let key = keys.pop().expect("two keys");
        let cache_name = SmolStr::new(name);
        let stale_hash = cache_b
            .shard
            .ownership_view_hash()
            .expect("b is distributed")
            ^ 1;
        let record = |key: u32, value: &str| wire_record_from(key, value, a.node_id());
        let forward = |hops: u8, rec: WireRecord| {
            let frame = OutFrame::new(Msg::ForwardBatch {
                cache: cache_name.clone(),
                view_hash: stale_hash,
                hops,
                recs: vec![rec],
            })
            .expect("encodes");
            let mesh = a.mesh().clone();
            let to = b.node_id();
            async move {
                mesh.send_frames_awaiting(to, vec![frame]).await;
            }
        };

        // As if `a` still believed `b` and a departed node owned the
        // bucket: the batch reaches `b` alone, under a hash `b` does not
        // recognise, and `b` passes it on to `c`.
        forward(0, record(key, "stale-routed")).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while cache_c.get(&key).await.as_deref() != Some("stale-routed") {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("b re-forwards the batch to c, the owner a's stale view missed");
        assert_eq!(cache_b.get(&key).await.as_deref(), Some("stale-routed"));

        // The same batch already one hop past its writer stops at `b`.
        forward(1, record(hop_one_key, "hop-one")).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while cache_b.get(&hop_one_key).await.as_deref() != Some("hop-one") {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("b applies the hop-one batch");
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            cache_c.get(&hop_one_key).await,
            None,
            "a batch at the hop cap is applied where it lands and re-forwarded no further"
        );

        cache_c.close().await;
        cache_b.close().await;
        cache_a.close().await;
        c.shutdown().await;
        b.shutdown().await;
        a.shutdown().await;
    }

    #[tokio::test]
    async fn distributed_cache_rejects_owners_below_two() {
        let cluster = Cluster::builder("cache-it-distributed-owners-below-two")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let err = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Distributed {
                owners: std::num::NonZeroU8::new(1).expect("nonzero"),
            })
            .open()
            .await
            .expect_err("a single owner is rejected");

        assert!(
            matches!(err, CacheError::TooFewOwners { .. }),
            "expected TooFewOwners, got {err:?}"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn distributed_cache_open_attaches_an_ownership_view() {
        let cluster = Cluster::builder("cache-it-distributed-attaches-view")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let cache = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Distributed {
                owners: std::num::NonZeroU8::new(2).expect("nonzero"),
            })
            .open()
            .await
            .expect("open succeeds");

        assert!(
            ShardOps::ownership_view(&*cache.shard).is_some(),
            "opening a Distributed cache attaches a real ownership view before it's shared"
        );

        cache.close().await;
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn distributed_cache_rejects_tti() {
        let cluster = Cluster::builder("cache-it-distributed-rejects-tti")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let err = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Distributed {
                owners: std::num::NonZeroU8::new(2).expect("nonzero"),
            })
            .tti(Duration::from_secs(30))
            .open()
            .await
            .expect_err("tti is rejected for Distributed just like Replicated");

        assert!(
            matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
            "expected ReplicatedWithLocalEviction, got {err:?}"
        );

        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn distributed_cache_rejects_finite_max_capacity_without_spill() {
        let cluster = Cluster::builder("cache-it-distributed-no-spill-max-capacity")
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("build succeeds");

        let err = cluster
            .cache::<u32, String>("scratch")
            .mode(Mode::Distributed {
                owners: std::num::NonZeroU8::new(2).expect("nonzero"),
            })
            .max_capacity(100)
            .open()
            .await
            .expect_err("Distributed + finite max_capacity + no spill is rejected");

        assert!(
            matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
            "expected ReplicatedWithLocalEviction, got {err:?}"
        );

        cluster.shutdown().await;
    }

    #[cfg(feature = "spill")]
    mod spill_gate {
        use super::*;

        /// A directory path under the OS temp dir, unique to this test
        /// process and call. Never created on disk: config validation in
        /// `open()` is pure arithmetic and never touches the filesystem.
        fn fresh_spill_dir(label: &str) -> std::path::PathBuf {
            std::env::temp_dir().join(format!(
                "sundog-spill-gate-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock is after the unix epoch")
                    .as_nanos()
            ))
        }

        #[tokio::test]
        async fn open_returns_spill_unavailable_when_dir_is_a_regular_file() {
            let cluster = Cluster::builder("cache-it-spill-dir-is-a-file")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let path = fresh_spill_dir("dir-is-a-file");
            std::fs::write(&path, b"not a directory")
                .expect("create a regular file at the spill dir path");

            let cfg = SpillConfig::new(&path, 1 << 20).region_bytes(4096);
            let err = cluster
                .cache::<u32, String>("scratch")
                .spill(cfg)
                .open()
                .await
                .expect_err("a spill dir that is actually a regular file cannot be created under");
            assert!(
                matches!(err, CacheError::SpillUnavailable { .. }),
                "expected SpillUnavailable, got {err:?}"
            );

            let _ = std::fs::remove_file(&path);
            cluster.shutdown().await;
        }

        #[tokio::test]
        async fn open_rejects_a_spill_config_whose_capacity_is_under_two_regions() {
            let cluster = Cluster::builder("cache-it-spill-under-two-regions")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            // Default region_bytes is 64 MiB; one byte under two regions.
            let cfg = SpillConfig::new(fresh_spill_dir("under-two-regions"), 128 * 1024 * 1024 - 1);
            let err = cluster
                .cache::<u32, String>("scratch")
                .spill(cfg)
                .open()
                .await
                .expect_err("a capacity under two regions is rejected");

            assert!(
                matches!(err, CacheError::InvalidSpillConfig { .. }),
                "expected InvalidSpillConfig, got {err:?}"
            );

            cluster.shutdown().await;
        }

        #[tokio::test]
        async fn open_rejects_replicated_with_max_capacity_and_no_spill() {
            let cluster = Cluster::builder("cache-it-spill-no-spill-max-capacity")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let err = cluster
                .cache::<u32, String>("scratch")
                .mode(Mode::Replicated)
                .max_capacity(100)
                .open()
                .await
                .expect_err("Replicated + finite max_capacity + no spill is rejected");

            assert!(
                matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
                "expected ReplicatedWithLocalEviction, got {err:?}"
            );

            cluster.shutdown().await;
        }

        #[tokio::test]
        async fn open_accepts_replicated_with_max_capacity_once_spill_is_configured() {
            let cluster = Cluster::builder("cache-it-spill-relaxes-gate")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let cfg = SpillConfig::new(fresh_spill_dir("relaxes-gate"), 256 * 1024 * 1024);
            let cache = cluster
                .cache::<u32, String>("scratch")
                .mode(Mode::Replicated)
                .max_capacity(100)
                .spill(cfg)
                .open()
                .await
                .expect("Replicated + finite max_capacity is accepted once spill is configured");

            cache.close().await;
            cluster.shutdown().await;
        }

        #[tokio::test]
        async fn distributed_cache_opens_with_spill_and_a_capacity() {
            let cluster = Cluster::builder("cache-it-distributed-spill-relaxes-gate")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let cfg = SpillConfig::new(
                fresh_spill_dir("distributed-relaxes-gate"),
                256 * 1024 * 1024,
            );
            let cache = cluster
                .cache::<u32, String>("scratch")
                .mode(Mode::Distributed {
                    owners: std::num::NonZeroU8::new(2).expect("nonzero"),
                })
                .max_capacity(100)
                .spill(cfg)
                .open()
                .await
                .expect("Distributed + finite max_capacity is accepted once spill is configured");

            cache.close().await;
            cluster.shutdown().await;
        }

        #[tokio::test]
        async fn open_still_rejects_replicated_with_tti_even_with_spill() {
            let cluster = Cluster::builder("cache-it-spill-tti-still-rejected")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let cfg = SpillConfig::new(fresh_spill_dir("tti-still-rejected"), 256 * 1024 * 1024);
            let err = cluster
                .cache::<u32, String>("scratch")
                .mode(Mode::Replicated)
                .tti(Duration::from_secs(30))
                .spill(cfg)
                .open()
                .await
                .expect_err("tti stays an unconditional error for Replicated, spill or not");

            assert!(
                matches!(err, CacheError::ReplicatedWithLocalEviction { .. }),
                "expected ReplicatedWithLocalEviction, got {err:?}"
            );

            cluster.shutdown().await;
        }

        /// Polls `cond` until it returns `true` or `timeout` elapses,
        /// returning the final result either way. Never a fixed sleep.
        async fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                if cond() {
                    return true;
                }
                if tokio::time::Instant::now() >= deadline {
                    return cond();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        /// Opens a cache under `mode` with a tiny `max_capacity` and a real
        /// spill tier, inserts past that capacity, then waits up to a
        /// bound for eviction to spill one of the two keys. Spilling is
        /// observed via `get_sync` reading a miss, `Shard::get_sync`'s
        /// documented contract for a currently-spilled entry. It then
        /// reads the key back through `get` and asserts the disk
        /// round-trip.
        async fn spill_composes_with_max_capacity(mode: Mode, label: &str) {
            let cluster = Cluster::builder("cache-it-spill-compose")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let cfg = SpillConfig::new(fresh_spill_dir(label), 1 << 20).region_bytes(4096);
            let cache = cluster
                .cache::<u32, String>("scratch")
                .mode(mode)
                .max_capacity(1)
                .spill(cfg)
                .open()
                .await
                .expect("opens with spill composing with max_capacity");

            cache.insert(1, "one".to_string()).await.expect("insert 1");
            cache.insert(2, "two".to_string()).await.expect("insert 2");

            assert!(
                poll_until(Duration::from_secs(5), || {
                    cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
                })
                .await,
                "eviction spills exactly one of the two keys under a tiny max_capacity"
            );
            let (spilled_key, expected) = if cache.get_sync(&1).is_none() {
                (1u32, "one".to_string())
            } else {
                (2u32, "two".to_string())
            };
            assert_eq!(
                cache.get(&spilled_key).await,
                Some(expected),
                "the spilled key reads back correctly through the disk tier"
            );

            cache.close().await;
            cluster.shutdown().await;
        }

        #[tokio::test]
        async fn spill_composes_with_max_capacity_under_mode_local() {
            spill_composes_with_max_capacity(Mode::Local, "compose-local").await;
        }

        #[tokio::test]
        async fn spill_composes_with_max_capacity_under_mode_invalidation() {
            spill_composes_with_max_capacity(Mode::Invalidation, "compose-invalidation").await;
        }

        /// `Shard::attach_spill` runs only for the `open()` that wins the
        /// registry reservation, so the losing `open()` never touches the
        /// first cache's region files.
        #[tokio::test]
        async fn reopening_the_same_spill_cache_name_returns_already_open_without_corrupting_it() {
            let cluster = Cluster::builder("cache-it-spill-reopen-guard")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let dir = fresh_spill_dir("reopen-guard");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let cache = cluster
                .cache::<u32, String>("scratch")
                .max_capacity(1)
                .spill(cfg)
                .open()
                .await
                .expect("first open succeeds");

            cache.insert(1, "one".to_string()).await.expect("insert 1");
            cache.insert(2, "two".to_string()).await.expect("insert 2");
            assert!(
                poll_until(Duration::from_secs(5), || {
                    cache.get_sync(&1).is_none() || cache.get_sync(&2).is_none()
                })
                .await,
                "eviction spills exactly one of the two keys under a tiny max_capacity"
            );
            let (spilled_key, expected) = if cache.get_sync(&1).is_none() {
                (1u32, "one".to_string())
            } else {
                (2u32, "two".to_string())
            };

            // Same name and SpillConfig, same directory. The registry
            // reservation must reject this second open before
            // `Shard::attach_spill` ever wipes and preallocates the first
            // cache's region files.
            let cfg2 = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let err = cluster
                .cache::<u32, String>("scratch")
                .max_capacity(1)
                .spill(cfg2)
                .open()
                .await
                .expect_err("the name is already open");
            assert!(
                matches!(err, CacheError::AlreadyOpen { .. }),
                "expected AlreadyOpen, got {err:?}"
            );

            assert_eq!(
                cache.get(&spilled_key).await,
                Some(expected),
                "the first cache's spilled value must still be readable after the rejected \
                 second open"
            );

            cache.close().await;
            cluster.shutdown().await;
        }

        #[tokio::test]
        async fn close_stops_the_tier_and_a_surviving_clone_evicts_by_deleting() {
            let cluster = Cluster::builder("cache-it-spill-close-stops-tier")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let cfg =
                SpillConfig::new(fresh_spill_dir("close-stops-tier"), 1 << 20).region_bytes(4096);
            let cache = cluster
                .cache::<u32, String>("scratch")
                .max_capacity(1)
                .spill(cfg)
                .open()
                .await
                .expect("opens with spill");
            let surviving = cache.clone();

            assert!(!surviving.shard.spill_tier_closed(), "the tier starts open");
            cache.close().await;
            assert!(
                surviving.shard.spill_tier_closed(),
                "Cache::close stops the engine's attached spill tier, visible through any \
                 surviving clone since they share one Shard"
            );

            // A surviving clone keeps working locally. With the tier
            // closed, capacity eviction on it deletes rather than spills,
            // since there is no live tier to hand a victim to. A spilled
            // entry still counts as live, so `entry_count` distinguishes a
            // delete from a spill.
            surviving
                .insert(1, "one".to_string())
                .await
                .expect("insert 1");
            surviving
                .insert(2, "two".to_string())
                .await
                .expect("insert 2");
            assert_eq!(
                surviving.entry_count().await,
                1,
                "eviction deletes rather than spills once the tier is closed"
            );

            cluster.shutdown().await;
        }

        /// Cache-level counterpart to the engine- and shard-level spilled-merge
        /// tests: a counter spills between two colliding `Cache::merge` calls,
        /// and the second still folds against the spilled side's real bytes
        /// instead of dropping them.
        #[tokio::test]
        async fn merging_into_a_spilled_counter_folds_instead_of_dropping_a_side() {
            let cluster = Cluster::builder("cache-it-merge-spilled")
                .seeds(std::iter::empty())
                .config(loopback_config())
                .build()
                .await
                .expect("build succeeds");

            let cfg =
                SpillConfig::new(fresh_spill_dir("merge-spilled"), 1 << 20).region_bytes(4096);
            let cache = cluster
                .cache::<u32, PnCounter>("counters")
                .resolver(Arc::new(PnCounterResolver))
                .max_capacity(1)
                // Always exceeds the cap of 1 on its own, so the single
                // counter key spills right after the first merge instead of
                // needing a second key to create eviction pressure.
                .weigher(|_k: &u32, _v: &PnCounter| 2)
                .spill(cfg)
                .open()
                .await
                .expect("opens with spill");

            let writer_a = WriterId::new(NodeId::from(11), 1);
            let writer_b = WriterId::new(NodeId::from(22), 1);

            cache
                .merge(1, PnCounter::local_delta(writer_a, 3))
                .await
                .expect("first increment");

            assert!(
                poll_until(Duration::from_secs(5), || cache.get_sync(&1).is_none()).await,
                "the over-weight entry spills once eviction runs"
            );

            cache
                .merge(1, PnCounter::local_delta(writer_b, 4))
                .await
                .expect("second increment, colliding with the spilled record");

            assert_eq!(
                cache.get(&1).await.map(|c| c.value()),
                Some(7),
                "merging into a spilled record folds both sides' contributions instead of \
                 dropping the one that was on disk"
            );

            cache.close().await;
            cluster.shutdown().await;
        }
    }
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
