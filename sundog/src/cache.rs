//! The typed cache handle and its builder. `Cache<K, V>` wraps
//! `Arc<Shard<K, V>>`; a local read decodes its own stored record under the stripe's read lock.
//!
//! [`CacheBuilder::open`] checks the requested [`Mode`] against what live
//! peers advertise for the same name before registering the shard, and
//! advertises its own choice on success.
//!
//! [`Cache::merge`] folds a value into the configured
//! [`ConflictResolver`] without a read; [`CacheBuilder::merge_coalesce_window`]
//! coalesces consecutive `merge` and `merge_with_ttl` calls to one key into a
//! single record per window instead of one per call.

// The owner read path: asking the owners of a key and decoding their answers.
mod fetch;
// Load routing: choosing the node a miss loads on, and the refresh-ahead task.
mod load;
// The checks and setup `CacheBuilder::open` runs before it spawns the loops.
mod open;
// Warm-reopen reconciliation of spill-reloaded buckets against their peers.
mod reconcile;
// The background loops a cache runs: fan-out, merge coalescing, warm-up, rebalancing.
mod tasks;

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cluster::Cluster;
#[cfg(doc)]
use crate::config::ClusterConfig;
use crate::error::CacheError;
use crate::explain::{DistributedRead, ReadExplanation, ReadSource};
use crate::node::NodeId;
use crate::store::crdt::WriterId;
#[cfg(feature = "spill")]
use crate::store::spill::SpillConfig;
use crate::store::{
    ConflictResolver, Event, Lifetime, Loader, LocalRearm, LwwResolver, Mode, PartId, Shard,
    ShardOps, Ttl, Weigher, encode_key,
};
use crate::verdict;

use fetch::{OwnerAnswer, other_owners};
use load::routes_loads;
#[cfg(feature = "spill")]
use open::attach_spill_and_record_warm;
use open::{
    LoadConfig, attach_ownership, await_initial_peers, validate_load_config, validate_merge_window,
    validate_mode,
};
use tasks::spawn_cache_tasks;

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
    /// The clock this cache's shard reads, in place of the system clock.
    #[cfg(test)]
    clock: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
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
            #[cfg(test)]
            clock: None,
            marker: PhantomData,
        }
    }

    /// Reads time from `clock` instead of the system clock, so a test moves
    /// every node's notion of time at once. A test cluster gives every node
    /// the same clock. Absent from the `sim` build, which compiles no
    /// loopback-cluster tests.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn clock(mut self, clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.clock = Some(clock);
        self
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
    ///
    /// A merging resolver, such as [`crate::crdt::PnCounterResolver`] or
    /// [`crate::crdt::OrSetResolver`], folds both records into one value
    /// instead, in every mode. It fits a count or a set of members that
    /// several nodes update at once and that the cache itself owns; a copy
    /// of data that lives elsewhere keeps the default. Nodes compare a
    /// cache's mode when it opens, not its resolver, so open the cache with
    /// the same resolver on every node. The book's
    /// [Choosing a resolver](https://ngriaznov.github.io/sundog/resolvers.html)
    /// page says when a merging resolver fits and how its values expire.
    pub fn resolver(mut self, resolver: Arc<dyn ConflictResolver>) -> Self {
        self.resolver = resolver;
        self
    }

    /// Sets the window [`Cache::merge`] and [`Cache::merge_with_ttl`]
    /// coalesce consecutive calls to one key within: instead of each call
    /// applying (and replicating) on its own, they fold in memory through
    /// the configured [`CacheBuilder::resolver`], each call carrying its own
    /// deadline, and apply once, when the window that opened at the first of
    /// those calls elapses. Default: zero, so every `Cache::merge` call
    /// applies at once, equivalent to [`Cache::insert`] under a merging
    /// resolver.
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
            #[cfg(test)]
            clock,
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
        #[cfg(test)]
        if let Some(clock) = clock {
            shard = shard.with_clock(clock);
        }
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
    /// The folded record carries the cache's default TTL, if any; a
    /// merging resolver keeps the later deadline of the two sides.
    ///
    /// # Errors
    ///
    /// As [`Cache::insert`].
    pub async fn merge(&self, key: K, value: V) -> Result<(), CacheError> {
        self.shard.merge(key, value).await
    }

    /// [`Cache::merge`] with a lifespan for this contribution alone; `ttl`
    /// overrides the cache's default and travels with the record. The
    /// folded record's deadline is the resolver's merge of the two sides'
    /// deadlines: the shipped resolvers keep the later one, so a stored
    /// value that outlives `ttl` keeps its own deadline.
    ///
    /// # Errors
    ///
    /// As [`Cache::insert`].
    pub async fn merge_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<(), CacheError> {
        self.shard.merge_with_ttl(key, value, ttl).await
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
    /// record's expiry: the `crate::crdt` resolvers keep the later of the two
    /// deadlines, an unset side deferring to the other, so `expire` extends a
    /// deadline and never shortens it, [`Cache::touch`] moves it to the
    /// default TTL from now when that is later, and [`Cache::persist`] leaves
    /// it.
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
    /// stops expiring. Under a merging resolver the deadline moves only when
    /// the new one is later, as [`Cache::expire`] describes.
    ///
    /// # Errors
    ///
    /// As [`Cache::expire`].
    pub async fn touch(&self, key: &K) -> Result<bool, CacheError> {
        self.rearm(key, Lifetime::Default).await
    }

    /// [`Cache::expire`] with no lifespan: the entry stops expiring. Under a
    /// merging resolver the deadline stays, as [`Cache::expire`] describes.
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
