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
use crate::error::{CacheError, RemoteLoaderError};
use crate::explain::{self, DistributedRead, OwnerProbe, ReadExplanation, ReadSource, Reads};
use crate::net::{FetchOutcome, LoadOutcome};
use crate::node::NodeId;
use crate::ownership::{OwnershipTracker, OwnershipView, ResidencySet};
use crate::store::PartId;
use crate::store::crdt::WriterId;
use crate::store::part::PartSet;
#[cfg(feature = "spill")]
use crate::store::spill::SpillConfig;
use crate::store::{
    ConflictResolver, Event, Lifetime, LoadError, Loader, LocalRearm, LwwResolver, Mode,
    RefreshRequest, Shard, ShardOps, Sourced, Ttl, Weigher, encode_key, now_ms, refresh_permille,
};
use crate::verdict;
use crate::wire::WireRecord;

/// Builds a [`Cache`]: own-and-return, per house style.
#[must_use]
pub struct CacheBuilder<K, V> {
    cluster: Cluster,
    name: SmolStr,
    mode: Mode,
    max_capacity: u64,
    max_resident_bytes: u64,
    ttl: Option<Duration>,
    tti: Option<Duration>,
    capacity_hint: Option<u64>,
    resolver: Arc<dyn ConflictResolver>,
    weigher: Option<Weigher<K, V>>,
    #[cfg(feature = "spill")]
    spill: Option<SpillConfig>,
    merge_coalesce_window: Duration,
    prefold_enabled: bool,
    loader: Option<Loader<K, V>>,
    batch_window: Option<(Duration, usize)>,
    refresh_ahead: Option<f64>,
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
            max_resident_bytes: u64::MAX,
            ttl: None,
            tti: None,
            capacity_hint: None,
            resolver: Arc::new(LwwResolver),
            weigher: None,
            #[cfg(feature = "spill")]
            spill: None,
            merge_coalesce_window: Duration::ZERO,
            prefold_enabled: true,
            loader: None,
            batch_window: None,
            refresh_ahead: None,
            marker: PhantomData,
        }
    }

    /// Sets the cache's clustering mode. Default: [`Mode::Invalidation`].
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    /// Bounds local entry count. Default: unbounded. A write that takes
    /// the cache past it evicts the least recently used of a sample of the
    /// other entries, and its own entry only when nothing else is left.
    pub fn max_capacity(mut self, max_capacity: u64) -> Self {
        self.max_capacity = max_capacity;
        self
    }

    /// Sets a soft memory ceiling on this node's copy of the cache, in the
    /// bytes [`Cache::resident_bytes`] reports. A local write that finds the
    /// ceiling reached fails with [`CacheError::OverMemoryCeiling`] instead
    /// of growing memory, and a [`Cache::get_or_load`] fill returns its
    /// value without storing it. Removals and writes arriving from peers
    /// are always accepted, so replicas keep identical contents in every
    /// mode, and resident bytes can pass the ceiling by what peers send.
    /// Unlike [`CacheBuilder::max_capacity`], nothing is evicted, so this is
    /// allowed in [`Mode::Replicated`] and [`Mode::Distributed`] without a
    /// spill tier. Default: no ceiling, and then no byte counting either:
    /// [`Cache::resident_bytes`] is `None` and writes skip the check.
    pub fn max_resident_bytes(mut self, bytes: u64) -> Self {
        self.max_resident_bytes = bytes;
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

    /// Registers the loader [`Cache::load`] and [`Cache::load_many`] answer
    /// a miss through, one that reads many keys in one call, such as one
    /// `SELECT … WHERE id IN (…)`. A key missing from the returned map is
    /// one the source does not hold. Replaces any loader set before.
    pub fn batch_loader<F, Fut, E>(mut self, load: F) -> Self
    where
        F: Fn(Vec<K>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<HashMap<K, V>, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.loader = Some(Loader::batch(load));
        self
    }

    /// [`CacheBuilder::batch_loader`] for a source read one key at a time:
    /// `None` for a key it does not hold. The keys of one batch load
    /// concurrently. Replaces any loader set before.
    pub fn loader<F, Fut, E>(mut self, load: F) -> Self
    where
        F: Fn(K) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Option<V>, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.loader = Some(Loader::single(load));
        self
    }

    /// Collects the keys this node misses within `window` of the first into
    /// one loader call of at most `max_keys` keys; a larger batch splits
    /// into concurrent calls. Default: a zero window, so each call loads
    /// the keys it misses at once, at most
    /// [`crate::store::DEFAULT_MAX_BATCH_KEYS`] a call. Needs a loader.
    pub fn batch_window(mut self, window: Duration, max_keys: usize) -> Self {
        self.batch_window = Some((window, max_keys));
        self
    }

    /// Reloads an entry before it expires: a read that finds the entry past
    /// `fraction` of its lifetime, `0.8` for the last fifth, answers with
    /// it and asks for one reload through the loader. One node reloads each
    /// key, the one its loads gather on, and its new value replaces the
    /// entry like any write, so a hot key never expires and its source sees
    /// one load per lifetime. A reload the source fails keeps the entry
    /// until it expires; one that finds the key gone leaves it to expire.
    /// Needs a loader and a [`CacheBuilder::ttl`], and works in every
    /// [`Mode`] but [`Mode::Invalidation`]; `fraction` must lie strictly
    /// between 0 and 1.
    pub fn refresh_ahead(mut self, fraction: f64) -> Self {
        self.refresh_ahead = Some(fraction);
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
    /// only this node's own parts, per its freshly computed ownership
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
            max_resident_bytes,
            ttl,
            tti,
            capacity_hint,
            resolver,
            weigher,
            #[cfg(feature = "spill")]
            spill,
            merge_coalesce_window,
            prefold_enabled,
            loader,
            batch_window,
            refresh_ahead,
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

        let refresh_permille = match validate_load_config(LoadConfig {
            has_loader: loader.is_some(),
            has_batch_window: batch_window.is_some(),
            refresh_ahead,
            has_ttl: ttl.is_some(),
            mode,
        }) {
            Ok(permille) => permille,
            Err(reason) => {
                return Err(CacheError::InvalidLoadConfig {
                    cache: name,
                    reason,
                });
            }
        };
        let loader = match (loader, batch_window) {
            (Some(loader), Some((window, max_keys))) => Some(loader.with_window(window, max_keys)),
            (loader, _) => loader,
        };

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
        .with_prefold_enabled(prefold_enabled)
        .with_max_resident_bytes(max_resident_bytes);
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
        let has_loader = loader.is_some();
        if let Some(loader) = loader {
            shard = shard.with_loader(loader);
        }
        if let Some(permille) = refresh_permille {
            shard = shard.with_refresh_ahead(permille);
        }
        // Bounded wait for a first known peer, `Mode::Distributed` only,
        // before `attach_ownership` computes the first view; see
        // `should_await_first_peer` for when this waits. Runs even
        // for a cold open, since a view computed once a peer shows up beats
        // one computed alone. `membership_settled` records whether the wait
        // landed a peer; `distributed_warm_and_rebalance` uses it to gate the
        // sole-owner shortcut for a part this open's spill tier replayed.
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
        // `distributed_warm_and_rebalance` knows which owned parts get
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
        if has_loader {
            cluster.advertise_loader(&name);
        }
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
/// parts into `distributed`'s [`DistributedContext::warm_reloaded_parts`].
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

/// The live peers a key of cache `name`, open here under `mode`, may load
/// or refresh on: they speak [`crate::wire::PROTOCOL_LOAD`], advertise a
/// loader for `name`, and have it open under the same [`Mode`].
fn loader_peers(cluster: &Cluster, name: &str, mode: Mode) -> HashSet<NodeId> {
    let loaders = cluster.advertised_loaders();
    let modes = cluster.advertised_cache_modes();
    cluster
        .peers()
        .into_iter()
        .filter(|peer| {
            crate::wire::peer_supports(peer.protocol, crate::wire::PROTOCOL_LOAD)
                && loaders
                    .get(&peer.node)
                    .is_some_and(|caches| caches.contains(name))
                && modes.get(&peer.node).and_then(|caches| caches.get(name)) == Some(&mode)
        })
        .map(|peer| peer.node)
        .collect()
}

/// Whether [`Cache::load`] in `mode` gathers each key's loads on one node.
/// `Mode::Local` has no peers, and in `Mode::Replicated` a loaded value
/// reaches every node anyway, so each loads for itself. Pure; unit tested
/// directly.
fn routes_loads(mode: Mode) -> bool {
    matches!(mode, Mode::Invalidation | Mode::Distributed { .. })
}

/// The node a key's loads gather on, from this node's side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadTarget {
    Here,
    Peer(NodeId),
}

/// The node `part`'s loads gather on, among this node `me` and the peers in
/// `capable` that can load for this cache. In `Mode::Distributed`, the
/// first of the part's `owners`, in rank order, that is `me` or capable,
/// and `me` when none is. In every other mode, the rendezvous winner for
/// `part` among `me` and `capable`, ties to the lower [`NodeId`], so every
/// node that agrees on who can load picks the same one. Pure; unit tested
/// directly.
fn load_target(
    mode: Mode,
    part: PartId,
    me: NodeId,
    owners: &[NodeId],
    capable: &HashSet<NodeId>,
) -> LoadTarget {
    let chosen = match mode {
        Mode::Distributed { .. } => owners
            .iter()
            .copied()
            .find(|&node| node == me || capable.contains(&node))
            .unwrap_or(me),
        _ => capable
            .iter()
            .copied()
            .chain(std::iter::once(me))
            .max_by_key(|&node| {
                (
                    crate::ownership::rendezvous_score(node, part.raw()),
                    std::cmp::Reverse(node),
                )
            })
            .unwrap_or(me),
    };
    if chosen == me {
        LoadTarget::Here
    } else {
        LoadTarget::Peer(chosen)
    }
}

/// The loading options a builder carries, as [`validate_load_config`]
/// checks them.
#[derive(Clone, Copy)]
struct LoadConfig {
    has_loader: bool,
    has_batch_window: bool,
    refresh_ahead: Option<f64>,
    has_ttl: bool,
    mode: Mode,
}

/// Whether the loading options a builder carries fit together, and the
/// refresh point in thousandths of a lifetime when refresh-ahead is on: a
/// batch window needs a loader to batch for, and refresh-ahead needs a
/// loader, a TTL to refresh ahead of, a mode whose reloaded value reaches
/// the nodes that read it, and a fraction strictly between 0 and 1. Pure;
/// unit tested directly.
fn validate_load_config(config: LoadConfig) -> Result<Option<u64>, &'static str> {
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

/// Where a `Mode::Distributed` read's answer came from.
enum OwnerAnswer<V> {
    /// This node owns the key's part warm: its own copy.
    Local(Option<V>),
    /// An owner's record, a tombstone or expired record included, or
    /// `None` for a key the owner never held.
    Remote(Option<WireRecord>),
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

/// Counts one [`Cache::fetch`], started at `started`, in
/// `sundog_fetch_total{cache,outcome}`, records `outcome` on its `span` and,
/// when it made at least one owner request (`attempts`), records its
/// duration in `sundog_fetch_duration_seconds{cache,outcome}`. A fetch that
/// asked no owner is not timed, whatever its outcome.
fn record_fetch_outcome(
    span: &tracing::Span,
    cache: &str,
    outcome: &'static str,
    attempts: u32,
    started: std::time::Instant,
) {
    metrics::counter!(
        "sundog_fetch_total",
        "cache" => cache.to_string(),
        "outcome" => outcome
    )
    .increment(1);
    span.record("outcome", outcome);
    if attempts > 0 {
        metrics::histogram!(
            "sundog_fetch_duration_seconds",
            "cache" => cache.to_string(),
            "outcome" => outcome
        )
        .record(started.elapsed());
    }
}

/// Decodes a [`Cache::fetch`] reply's record into its value, re-checking
/// expiry client-side (defense in depth against the responder's own expiry
/// sweep lagging) and treating a tombstone or undecodable value as a miss.
fn decode_live_value<V: DeserializeOwned>(rec: &WireRecord) -> Option<V> {
    if explain::dead_record(rec, now_ms()).is_some() {
        return None;
    }
    rec.value
        .as_deref()
        .and_then(|bytes| postcard::from_bytes::<V>(bytes).ok())
}

/// What a [`Cache::fetch`] makes of an owner's record at `now_ms`, the
/// judgement [`decode_live_value`] makes, named.
fn record_reads<V: DeserializeOwned>(rec: &WireRecord, now_ms: u64) -> Reads {
    explain::dead_record(rec, now_ms).unwrap_or_else(|| {
        if rec
            .value
            .as_deref()
            .is_some_and(|bytes| postcard::from_bytes::<V>(bytes).is_ok())
        {
            Reads::Value
        } else {
            Reads::Undecodable
        }
    })
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
    /// The parts `Shard::attach_spill` warm-reloaded at least one record
    /// for. Empty here (the spill tier attaches only after the shard is
    /// shared); `CacheBuilder::open` fills it in before
    /// `spawn_cache_tasks` runs, for `distributed_warm_and_rebalance` to
    /// intersect with the initially owned parts and pick eager
    /// reconciliation candidates over an ordinary cold pull.
    warm_reloaded_parts: HashSet<PartId>,
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
/// at open unless this open replayed parts from a spill snapshot
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

/// Attaches a freshly seeded ownership tracker and residency set to
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
    if let Some(requests) = shard.take_refresh_requests() {
        cluster.spawn_tracked_in(
            tasks,
            refresh_task(Arc::clone(shard), cluster.clone(), requests, cancel.clone()),
        );
    }
}

/// The most refresh requests [`refresh_task`] takes off its queue at once.
const REFRESH_BURST: usize = 1_024;

/// Drains the reloads reads ask for under [`CacheBuilder::refresh_ahead`].
/// A key this node refreshes, the node [`load_target`] picks for it, and a
/// key a peer's hint sent here reload in one [`Shard::refresh`] pass per
/// drained burst; a key another node refreshes goes to it as a
/// [`crate::wire::Msg::RefreshHint`], or reloads here when the hint cannot
/// be sent. Counts `sundog_refreshes_total{cache, outcome}` and
/// `sundog_refresh_hints_sent_total{cache}`.
async fn refresh_task<K, V>(
    shard: Arc<Shard<K, V>>,
    cluster: Cluster,
    mut requests: tokio::sync::mpsc::Receiver<RefreshRequest<K>>,
    cancel: CancellationToken,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let name = SmolStr::new(shard.name());
    let mode = shard.mode();
    let outcome = |outcome: &'static str| metrics::counter!("sundog_refreshes_total", "cache" => name.to_string(), "outcome" => outcome);
    let (loaded, absent, failed) = (outcome("loaded"), outcome("absent"), outcome("failed"));
    let hints_sent =
        metrics::counter!("sundog_refresh_hints_sent_total", "cache" => name.to_string());
    let mut burst = Vec::new();
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            received = requests.recv_many(&mut burst, REFRESH_BURST) => {
                if received == 0 {
                    return;
                }
            }
        }
        let me = cluster.node_id();
        let capable = loader_peers(&cluster, &name, mode);
        let view = shard.ownership_view();
        let mut here = Vec::with_capacity(burst.len());
        for request in burst.drain(..) {
            let target = if request.hinted || matches!(mode, Mode::Local) {
                LoadTarget::Here
            } else {
                let part = PartId::of_key(&request.key_bytes);
                let owners = view.as_ref().map_or(&[][..], |view| view.owners_of(part));
                load_target(mode, part, me, owners, &capable)
            };
            match target {
                LoadTarget::Peer(peer)
                    if cluster.mesh().send_refresh_hint(
                        peer,
                        name.clone(),
                        request.key_bytes.clone(),
                        request.ver,
                    ) =>
                {
                    hints_sent.increment(1);
                }
                _ => here.push(request),
            }
        }
        if !here.is_empty() {
            let tally = shard.refresh(here).await;
            loaded.increment(tally.loaded);
            absent.increment(tally.absent);
            failed.increment(tally.failed);
        }
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

/// Cap on rounds in [`reconcile_warm_buckets`]'s per-peer converge loop:
/// chances for a part still moving under live writes to settle, not chunks
/// of a fixed size. A failed or `Stale` round doesn't count; it retries.
const RECONCILE_MAX_ROUNDS: u32 = 3;

/// Base retry delay after a failed round in [`reconcile_against_peer`];
/// doubles per consecutive failure, capped at [`ReconcileBudget::backoff_cap`].
const RECONCILE_RETRY_BASE: Duration = Duration::from_millis(200);

/// Per-peer bound on [`reconcile_warm_buckets`]'s converge loop: whichever
/// of `max_rounds`, `byte_budget` or `time_budget` is hit first stops it,
/// leaving that peer's still-diverging parts cold for the ordinary pull.
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
/// outcome; a `failed` round reports every part as still diverging.
fn split_round_result(
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

    /// Reads `key`: local if this node owns its part (no network,
    /// counted `sundog_fetch_total{outcome="local"}`), otherwise one request
    /// to a live owner, tried in rendezvous-score order until one answers.
    /// Never promotes the fetched value into the local store; [`Cache::get`]
    /// for the same key immediately afterward is still a miss on this node.
    /// A part this node owns but has not yet pulled from a co-owner is
    /// cold: a local miss there asks the other owners, and when none of
    /// them answers from a warm copy it returns
    /// [`CacheError::FetchUnavailable`] rather than a miss it cannot vouch
    /// for, since the part's previous owner can still hold the record
    /// through its disown grace. A part this node owns alone because the
    /// failure detector dropped its other owners stays cold the same way,
    /// for up to [`ClusterConfig::state_transfer_budget`], since an owner
    /// dropped that way can be stalled or across a partition and still hold
    /// the key. While ownership moves, a hit can be stale:
    /// a node this node's view still lists as an owner may already have
    /// given the part up, and its copy lacks the writes and deletes made
    /// since. Once the views agree the key reads its current value. On a
    /// cache that isn't [`Mode::Distributed`] this
    /// is [`Cache::get`] wrapped in `Ok`, counted `outcome="local"`
    /// unconditionally, so callers don't need to branch on mode.
    ///
    /// A fetch that asks an owner records its duration in
    /// `sundog_fetch_duration_seconds{cache,outcome}` under its outcome:
    /// `remote`, `miss`, `error`, or `local` when a changed ownership view
    /// hands the key back to this node after an owner was asked. A fetch
    /// that asks no owner is not timed. Each fetch runs in a debug-level
    /// `sundog.fetch` span carrying the cache, the last owner asked (this
    /// node for a local answer), the outcome (none for a key that fails to
    /// encode) and the number of owner requests.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to encode, and
    /// [`CacheError::FetchUnavailable`] if every owner is unreachable,
    /// times out (`crate::config::ClusterConfig::fetch_timeout` per
    /// attempt) or declines while this node's own copy is cold, distinct
    /// from a genuine miss, which returns `Ok(None)`.
    /// An owner whose ownership view differs from this node's is retried
    /// with a short jittered backoff before the next owner is tried.
    /// [`Cache::explain`] shows how a fetch of a key decides, without
    /// fetching it.
    pub async fn fetch(&self, key: &K) -> Result<Option<V>, CacheError> {
        let span = tracing::debug_span!(
            "sundog.fetch",
            cache = %self.shard.name(),
            owner = tracing::field::Empty,
            outcome = tracing::field::Empty,
            attempts = 0u32,
        );
        let fetch = self.fetch_in(key, &span);
        tracing::Instrument::instrument(fetch, span.clone()).await
    }

    /// [`Cache::fetch`] inside its `sundog.fetch` span. Every field is
    /// recorded on `span` itself, so a span the subscriber disables records
    /// nothing and never writes to the caller's span.
    async fn fetch_in(&self, key: &K, span: &tracing::Span) -> Result<Option<V>, CacheError> {
        let started = std::time::Instant::now();
        let Some(view) = self.shard.ownership_view() else {
            let value = self.shard.get(key).await;
            span.record("owner", tracing::field::display(self.cluster.node_id()));
            record_fetch_outcome(span, self.shard.name(), "local", 0, started);
            return Ok(value);
        };
        let key_bytes = encode_key(key)?;
        let mut attempts = 0u32;
        let answer = self
            .owner_answer(key, &key_bytes, view, span, &mut attempts)
            .await;
        let (outcome, fetched) = match answer {
            Ok(OwnerAnswer::Local(value)) => ("local", Ok(value)),
            Ok(OwnerAnswer::Remote(rec)) => {
                if let Some(rec) = &rec
                    && rec.value.is_some()
                {
                    self.shard.note_read(
                        key,
                        &rec.key,
                        crate::store::hash_key_bytes(&rec.key),
                        rec.ver,
                        rec.expires_at_ms,
                        now_ms(),
                    );
                }
                let value = rec.and_then(|rec| decode_live_value::<V>(&rec));
                (if value.is_some() { "remote" } else { "miss" }, Ok(value))
            }
            Err(err) => ("error", Err(err)),
        };
        // One call for every answer, with the owner requests this fetch
        // made, so a local answer reached after asking an owner is timed
        // exactly like a remote one.
        record_fetch_outcome(span, self.shard.name(), outcome, attempts, started);
        fetched
    }

    /// [`Cache::fetch`]'s answer for a `Mode::Distributed` key under
    /// `view`: this node's own copy when it owns the key's part warm, or
    /// else the record the first owner to answer holds, tried in rendezvous
    /// order with the stale-view retry `fetch` documents. Counts each owner
    /// request in `attempts` and records it, and the owner asked, on
    /// `span`.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::FetchUnavailable`] when no owner answers.
    async fn owner_answer(
        &self,
        key: &K,
        key_bytes: &bytes::Bytes,
        view: Arc<OwnershipView>,
        span: &tracing::Span,
        attempts: &mut u32,
    ) -> Result<OwnerAnswer<V>, CacheError> {
        let part = PartId::of_key(key_bytes);
        let self_node = self.cluster.node_id();
        if let Some(value) = self.local_answer(key, &view, part).await {
            span.record("owner", tracing::field::display(self_node));
            return Ok(OwnerAnswer::Local(value));
        }

        let cache_name = SmolStr::new(self.shard.name());
        let mesh = self.cluster.mesh();
        let attempt_timeout = self.cluster.config().fetch_timeout;

        let mut view = view;
        let mut owners = other_owners(&view, part, self_node);
        let mut owner_idx = 0usize;
        // When the current owner first answered `Stale` with this node's
        // view unchanged: its retry window runs from here.
        let mut stale_since: Option<tokio::time::Instant> = None;

        while owner_idx < owners.len() {
            let owner = owners[owner_idx];
            *attempts += 1;
            span.record("owner", tracing::field::display(owner));
            span.record("attempts", *attempts);
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
                Ok(Ok(FetchOutcome::Found(rec))) => return Ok(OwnerAnswer::Remote(rec)),
                Ok(Ok(FetchOutcome::Stale { .. })) => {
                    if let Some(fresh) = self.shard.ownership_view()
                        && fresh.view_hash() != view.view_hash()
                    {
                        view = fresh;
                        if let Some(value) = self.local_answer(key, &view, part).await {
                            span.record("owner", tracing::field::display(self_node));
                            return Ok(OwnerAnswer::Local(value));
                        }
                        owners = other_owners(&view, part, self_node);
                        owner_idx = 0;
                        stale_since = None;
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
                // A decline is as good as no answer: that owner holds no
                // warm copy to vouch for the key.
                Ok(Ok(FetchOutcome::Declined) | Err(_)) | Err(_) => {
                    owner_idx += 1;
                    stale_since = None;
                }
            }
        }
        Err(CacheError::FetchUnavailable { cache: cache_name })
    }

    /// This node's own answer to a fetch of `key` in `part` under `view`:
    /// `Some` with the value, or with `None` for a warm miss, when this
    /// node's copy answers, by [`verdict::local_verdict`]; `None` when only
    /// another owner's copy does. A distrusted part (warm-reloaded or
    /// regained, not yet checked against a live co-owner) can hold a record
    /// a co-owner deleted while this node was down or not an owner, and a
    /// cold part's miss says nothing while its previous owner can still
    /// hold the record through the disown grace. The part's marks are read
    /// before the record, so a pull landing during the read cannot clear a
    /// cold mark the decision then trusts.
    async fn local_answer(&self, key: &K, view: &OwnershipView, part: PartId) -> Option<Option<V>> {
        let residency = self.shard.residency_of(view, part);
        if !verdict::reads_local(&residency) {
            return None;
        }
        let value = self.shard.get(key).await;
        verdict::local_verdict(&residency, value.is_some())
            .answers()
            .then_some(value)
    }

    /// The live owners of `key`'s part, in rendezvous score order.
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

    /// Why a read of `key` answers what it answers on this node: what this
    /// node stores for the key and, on a [`Mode::Distributed`] cache, this
    /// node's residency marks for the key's part, what its own copy makes
    /// of a [`Cache::fetch`], what it answers a peer's fetch, what each
    /// other owner answers, and where a fetch takes its answer.
    ///
    /// It is not a read. It calls no get, fetch or loader, counts no hit,
    /// miss, fetch or loader metric, opens no `sundog.fetch` span, touches
    /// no idle timer of this node's copy, takes no TTL, starts no refresh
    /// and promotes no spilled entry. A spilled entry is reported from its pointer, without
    /// a disk read.
    ///
    /// The explanation is a snapshot taken in order: the ownership view,
    /// the residency marks, the local record, then the other owners. Each
    /// other owner is asked at once with the fetch a [`Cache::fetch`]
    /// sends, under the same view hash, each bounded by
    /// `ClusterConfig::fetch_timeout` and never retried, so the call
    /// returns within about one `fetch_timeout`. Each probe costs a request,
    /// may dial a connection and counts as wire traffic on both nodes; an
    /// owner holding the key spilled reads it from disk to answer. [`DistributedRead::view_moved_to`] reports a
    /// view that changed during the probes.
    ///
    /// [`ReadExplanation::source`] predicts a fetch from these answers: a
    /// fetch also retries an owner with a stale view, and its owners can
    /// change their state between the probe and the fetch. A
    /// [`ProbeAnswer::Declined`](crate::explain::ProbeAnswer::Declined)
    /// covers every reason an owner declines, and an owner's
    /// [`ProbeAnswer::Held`](crate::explain::ProbeAnswer::Held) record does
    /// not show its part is warm.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to encode. An owner that
    /// declines or gives no answer is part of the explanation, not an
    /// error.
    pub async fn explain(&self, key: &K) -> Result<ReadExplanation, CacheError> {
        let key_bytes = encode_key(key)?;
        let part = PartId::of_key(&key_bytes);
        let node = self.cluster.node_id();
        let view = self.shard.ownership_view();
        let residency = view
            .as_ref()
            .map(|view| self.shard.residency_of(view, part));
        let (at_ms, local) = self.shard.local_record(&key_bytes);
        let mut explanation = ReadExplanation {
            cache: SmolStr::new(self.shard.name()),
            node,
            mode: self.shard.mode(),
            part,
            at_ms,
            local,
            source: ReadSource::Local {
                hit: local.is_live(),
            },
            distributed: None,
        };
        let (Some(view), Some(residency)) = (view, residency) else {
            return Ok(explanation);
        };
        let local_read = verdict::local_verdict(&residency, local.is_live());
        let view_hash = view.view_hash();
        let probes = futures::future::join_all(
            other_owners(&view, part, node)
                .into_iter()
                .map(|owner| self.probe_owner(owner, &key_bytes, view_hash, at_ms)),
        )
        .await;
        explanation.source = verdict::read_source(local_read, &probes);
        explanation.distributed = Some(DistributedRead {
            view_hash,
            view_moved_to: self
                .shard
                .ownership_view_hash()
                .filter(|hash| *hash != view_hash),
            owners: view.owners_of(part).to_vec(),
            residency,
            local_read,
            serves_peers: verdict::serves_peers(&residency, &local),
            probes,
        });
        Ok(explanation)
    }

    /// `owner`'s answer to the fetch [`Cache::fetch`] sends it for
    /// `key_bytes` under `view_hash`, bounded by `fetch_timeout`, with its
    /// record judged at `at_ms`.
    async fn probe_owner(
        &self,
        owner: NodeId,
        key_bytes: &bytes::Bytes,
        view_hash: u64,
        at_ms: u64,
    ) -> OwnerProbe {
        let reply = tokio::time::timeout(
            self.cluster.config().fetch_timeout,
            self.cluster.mesh().fetch(
                owner,
                SmolStr::new(self.shard.name()),
                key_bytes.clone(),
                view_hash,
            ),
        )
        .await
        .ok();
        OwnerProbe {
            node: owner,
            answer: explain::classify_probe(reply, |rec| record_reads::<V>(rec, at_ms)),
        }
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

    /// Bytes this node's live entries hold in memory, counted only when
    /// [`CacheBuilder::max_resident_bytes`] sets a ceiling and `None`
    /// without one: a fixed per-entry overhead plus each entry's heap-held
    /// key and value bytes, a spilled entry counting only its key. The
    /// figure the ceiling is checked against, and a lower bound on the
    /// process memory the entries occupy.
    #[must_use]
    pub fn resident_bytes(&self) -> Option<u64> {
        self.shard.resident_bytes()
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
    /// An invalidation for `key` that lands while `loader` runs means the
    /// loader may have read the source before the change it announces: the
    /// call returns the loaded value without storing it, and the next read
    /// loads afresh.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Loader`] if `loader` fails, or
    /// [`CacheError::Codec`] if `key` fails to postcard-encode. A fill that
    /// finishes at the [`CacheBuilder::max_resident_bytes`] ceiling returns
    /// its value without storing it.
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

    /// Reads `key` through the loader registered with
    /// [`CacheBuilder::batch_loader`] or [`CacheBuilder::loader`]: the
    /// cached value on a hit, otherwise what the loader returns, `None` when
    /// the source does not hold the key. Concurrent misses on one key
    /// collapse into one load, and keys missing within the
    /// [`CacheBuilder::batch_window`] share one loader call. A loaded value
    /// is written under the cache's default TTL and fans out per [`Mode`]
    /// like a [`Cache::get_or_load`] fill; a key the source lacks is not
    /// cached, so the next read asks again.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::NoLoader`] if no loader is registered,
    /// [`CacheError::Loader`] if the loader fails, and
    /// [`CacheError::Codec`] if `key` fails to encode.
    pub async fn load(&self, key: &K) -> Result<Option<V>, CacheError> {
        if routes_loads(self.shard.mode()) {
            self.shard
                .load_from(key, &|keys| Box::pin(self.route_load(keys)))
                .await
        } else {
            self.shard.load(key).await
        }
    }

    /// [`Cache::load`] for many keys: the keys missing here go to the loader
    /// together. The map holds every key the cache or the source holds.
    ///
    /// # Errors
    ///
    /// As [`Cache::load`]; a loader failure for any key fails the call.
    pub async fn load_many(
        &self,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<HashMap<K, V>, CacheError> {
        if routes_loads(self.shard.mode()) {
            self.shard
                .load_many_from(keys, &|keys| Box::pin(self.route_load(keys)))
                .await
        } else {
            self.shard.load_many(keys).await
        }
    }

    /// The source [`Cache::load`] reads a miss from in `Mode::Invalidation`
    /// and `Mode::Distributed`: each key goes to the node its loads gather
    /// on, per [`load_target`], one request per node, and this node's own
    /// keys to its loader, all at once. A peer that declines or cannot be
    /// reached has its keys loaded here instead; a peer whose loader fails
    /// fails the batch with [`RemoteLoaderError`].
    async fn route_load(&self, keys: Vec<K>) -> Result<Sourced<K, V>, LoadError> {
        let mode = self.shard.mode();
        let me = self.cluster.node_id();
        let capable = self.loader_peers();
        let view = self.shard.ownership_view();
        let mut here = Vec::new();
        let mut remote: HashMap<NodeId, Vec<(K, bytes::Bytes)>> = HashMap::new();
        for key in keys {
            let Ok(key_bytes) = encode_key(&key) else {
                here.push(key);
                continue;
            };
            let part = PartId::of_key(&key_bytes);
            let owners = view.as_ref().map_or(&[][..], |view| view.owners_of(part));
            match load_target(mode, part, me, owners, &capable) {
                LoadTarget::Here => here.push(key),
                LoadTarget::Peer(peer) => remote.entry(peer).or_default().push((key, key_bytes)),
            }
        }
        let local = async {
            if here.is_empty() {
                Ok(HashMap::new())
            } else {
                self.shard.call_loader(here).await
            }
        };
        let remotes = futures::future::join_all(
            remote
                .into_iter()
                .map(|(peer, keys)| self.load_on(peer, keys)),
        );
        let (local, remotes) = tokio::join!(local, remotes);
        let mut sourced = Sourced {
            store: local?,
            answer: HashMap::new(),
        };
        for answer in remotes {
            let answer = answer?;
            sourced.store.extend(answer.store);
            sourced.answer.extend(answer.answer);
        }
        Ok(sourced)
    }

    /// Asks `peer` to load `keys`. A record it stores comes back to be
    /// stored here too in `Mode::Invalidation`, where each node caches its
    /// own working set, and only answered with in `Mode::Distributed`,
    /// where `peer` owns the key and this node does not.
    async fn load_on(
        &self,
        peer: NodeId,
        keys: Vec<(K, bytes::Bytes)>,
    ) -> Result<Sourced<K, V>, LoadError> {
        let request = keys
            .iter()
            .map(|(_, key_bytes)| key_bytes.clone())
            .collect();
        let outcome = self
            .cluster
            .mesh()
            .load(peer, SmolStr::new(self.shard.name()), request)
            .await;
        let (found, uncached) = match outcome {
            Ok(LoadOutcome::Loaded { found, uncached }) => (found, uncached),
            Ok(LoadOutcome::Failed(message)) => {
                return Err(Box::new(RemoteLoaderError {
                    node: peer,
                    message,
                }));
            }
            Ok(LoadOutcome::Declined) | Err(_) => {
                let keys = keys.into_iter().map(|(key, _)| key).collect();
                return Ok(Sourced {
                    store: self.shard.call_loader(keys).await?,
                    answer: HashMap::new(),
                });
            }
        };
        let mut by_bytes: HashMap<bytes::Bytes, K> = keys
            .into_iter()
            .map(|(key, key_bytes)| (key_bytes, key))
            .collect();
        let mut sourced = Sourced {
            store: HashMap::new(),
            answer: HashMap::new(),
        };
        let stores_here = matches!(self.shard.mode(), Mode::Invalidation);
        let records = found
            .into_iter()
            .filter_map(|rec| rec.value.map(|value| (rec.key, value, stores_here)));
        let values = uncached.into_iter().map(|(key, value)| (key, value, false));
        for (key_bytes, value_bytes, store) in records.chain(values) {
            let (Some(key), Ok(value)) = (
                by_bytes.remove(&key_bytes),
                postcard::from_bytes::<V>(&value_bytes),
            ) else {
                continue;
            };
            if store {
                sourced.store.insert(key, value);
            } else {
                sourced.answer.insert(key, value);
            }
        }
        Ok(sourced)
    }

    /// [`loader_peers`] for this cache.
    fn loader_peers(&self) -> HashSet<NodeId> {
        loader_peers(&self.cluster, self.shard.name(), self.shard.mode())
    }

    /// Writes `key` = `value`: stamps an HLC version, applies locally, and
    /// fans out per [`Mode`]. Gets the cache's default TTL, if configured.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the encoded value exceeds
    /// the frame cap, [`CacheError::OverMemoryCeiling`] if this node is at
    /// its [`CacheBuilder::max_resident_bytes`] ceiling, or
    /// [`CacheError::Codec`] if `key` fails to encode.
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
    /// As [`Cache::insert`], for any entry. A batch that starts at the
    /// memory ceiling applies nothing; one that starts under it applies
    /// whole.
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
    /// As [`Cache::insert`].
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

    /// Gives `key`'s live entry a lifespan of `ttl` from now and keeps its
    /// value, replicating like [`Cache::insert_with_ttl`]. Returns whether
    /// the key was live.
    ///
    /// The write re-puts the value this node reads, stamped as that value's
    /// successor. A write or remove stamped after the value supersedes it,
    /// so `expire` never brings back a key removed, or a value replaced, in
    /// a later millisecond on any node. In `Mode::Distributed`, a node that
    /// does not own the key's part warm asks an owner for the record, as
    /// [`Cache::fetch`] does, and forwards the write to the owners. Under a
    /// merging [`CacheBuilder::resolver`], the resolver sets the merged
    /// record's expiry, and the `crate::crdt` resolvers keep none.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to encode, and
    /// [`CacheError::FetchUnavailable`] if this node needs an owner's copy
    /// and no owner answers.
    pub async fn expire(&self, key: &K, ttl: Duration) -> Result<bool, CacheError> {
        self.rearm(key, Lifetime::For(ttl)).await
    }

    /// [`Cache::expire`] with the cache's default TTL: the entry's lifespan
    /// restarts from now, and on a cache with no [`CacheBuilder::ttl`] it
    /// stops expiring.
    ///
    /// # Errors
    ///
    /// As [`Cache::expire`].
    pub async fn touch(&self, key: &K) -> Result<bool, CacheError> {
        self.rearm(key, Lifetime::Default).await
    }

    /// [`Cache::expire`] with no lifespan: the entry stops expiring.
    ///
    /// # Errors
    ///
    /// As [`Cache::expire`].
    pub async fn persist(&self, key: &K) -> Result<bool, CacheError> {
        self.rearm(key, Lifetime::Unbounded).await
    }

    /// The remaining lifetime of `key`'s live entry in this node's copy,
    /// read like [`Cache::get_sync`]: `None` when this node holds no live
    /// entry, and never a disk read. Takes no TTL and extends none.
    #[must_use]
    pub fn ttl_of(&self, key: &K) -> Option<Ttl> {
        self.shard.ttl_of(key)
    }

    /// [`Cache::expire`]'s body for any `lifetime`: this node's own copy
    /// when it holds the key's part warm, an owner's record otherwise.
    async fn rearm(&self, key: &K, lifetime: Lifetime) -> Result<bool, CacheError> {
        match self.shard.rearm_local(key, lifetime).await? {
            LocalRearm::Rearmed => return Ok(true),
            LocalRearm::Absent => return Ok(false),
            LocalRearm::NotHeld => {}
        }
        let Some(view) = self.shard.ownership_view() else {
            return Ok(false);
        };
        let key_bytes = encode_key(key)?;
        let answer = self
            .owner_answer(key, &key_bytes, view, &tracing::Span::none(), &mut 0)
            .await?;
        match answer {
            // The view moved to one that owns the part warm meanwhile.
            OwnerAnswer::Local(_) => {
                Ok(self.shard.rearm_local(key, lifetime).await? == LocalRearm::Rearmed)
            }
            OwnerAnswer::Remote(None) => Ok(false),
            OwnerAnswer::Remote(Some(base)) => {
                self.shard.forward_rearm(key.clone(), base, lifetime).await
            }
        }
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
mod tests;

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
