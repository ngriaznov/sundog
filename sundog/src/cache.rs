//! The typed cache handle and its builder. `Cache<K, V>` wraps
//! `Arc<Shard<K, V>>`; local reads never deserialize.
//!
//! [`CacheBuilder::open`] checks the requested [`Mode`] against what live
//! peers advertise for the same name before registering the shard, and
//! advertises its own choice on success.

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::broadcast;

use crate::cluster::Cluster;
use crate::error::CacheError;
use crate::store::{ConflictResolver, Event, LwwResolver, Mode, Shard, ShardOps, Weigher};

/// Builds a [`Cache`]: own-and-return, per house style.
#[must_use]
pub struct CacheBuilder<K, V> {
    cluster: Cluster,
    name: SmolStr,
    mode: Mode,
    max_capacity: u64,
    ttl: Option<Duration>,
    tti: Option<Duration>,
    resolver: Arc<dyn ConflictResolver>,
    weigher: Option<Weigher<K, V>>,
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
            resolver: Arc::new(LwwResolver),
            weigher: None,
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

    /// Sets a custom per-entry weigher for size-bounded eviction:
    /// `max_capacity` becomes a weight budget rather than an entry count.
    pub fn weigher<W>(mut self, weigher: W) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.weigher = Some(Box::new(weigher));
        self
    }

    /// Opens the cache: builds the local shard, registers it in the
    /// cluster's shard registry, and, unless `mode` is [`Mode::Local`],
    /// starts fanning local writes out to the mesh per `mode`.
    ///
    /// For [`Mode::Replicated`], `open()` also runs state transfer before
    /// returning: pulls a full snapshot from the lowest-node-id live peer,
    /// then runs one anti-entropy round against that donor, bounded by
    /// `ClusterConfig::state_transfer_budget`. A cache too large to finish
    /// inside the budget opens with a partial copy anti-entropy tops up.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::AlreadyOpen`] if a cache named `name` is
    /// already open in this process.
    ///
    /// Returns [`CacheError::ReplicatedWithLocalEviction`] if `mode` is
    /// [`Mode::Replicated`] and `max_capacity`/`tti` was also set: a local
    /// eviction would be silently re-pulled by the next anti-entropy round.
    ///
    /// Returns [`CacheError::ModeMismatch`] if a live peer already
    /// advertises `name` under a different [`Mode`]. Best-effort: a
    /// background sweep in `cluster` logs whatever mismatch this misses.
    ///
    /// # Panics
    ///
    /// Panics if the shard registry lock is poisoned.
    pub async fn open(self) -> Result<Cache<K, V>, CacheError> {
        let Self {
            cluster,
            name,
            mode,
            max_capacity,
            ttl,
            tti,
            resolver,
            weigher,
            marker: _,
        } = self;

        if matches!(mode, Mode::Replicated) && (max_capacity != u64::MAX || tti.is_some()) {
            return Err(CacheError::ReplicatedWithLocalEviction { cache: name });
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
        .with_tombstone_ttl(cluster.config().tombstone_ttl)
        .with_tombstone_max_ttl(cluster.config().tombstone_max_ttl)
        .with_max_frame(cluster.config().max_frame)
        .with_resolver(resolver);
        if let Some(weigher) = weigher {
            shard = shard.with_weigher(move |key: &K, value: &V| weigher(key, value));
        }
        let shard = Arc::new(shard);

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
        cluster.advertise_cache_mode(&name, mode);
        // Only a `Replicated` cache has anything to receive before it can
        // donate; the other modes are warm the moment they open.
        if !matches!(mode, Mode::Replicated) {
            cluster.mark_warm(&name);
        }

        if !matches!(mode, Mode::Local) {
            cluster.spawn_tracked(crate::cluster::fan_out_task(
                Arc::clone(&shard),
                cluster.clone(),
                shard.fan_out_queue(),
                name.clone(),
                mode,
                cluster.cancel_token(),
            ));
        }
        if matches!(mode, Mode::Replicated) {
            warm_and_repair(&cluster, Arc::clone(&shard) as Arc<dyn ShardOps>, &name).await;
        }
        cluster.spawn_tracked(crate::cluster::tombstone_gc_task(
            Arc::clone(&shard) as Arc<dyn ShardOps>,
            mode,
            cluster.config().tombstone_ttl,
            cluster.config().tombstone_max_ttl,
            cluster.absence_tracker(),
            cluster.cancel_token(),
        ));
        cluster.spawn_tracked(crate::cluster::cache_entries_gauge_task(
            Arc::clone(&shard),
            name.clone(),
            cluster.cancel_token(),
        ));

        Ok(Cache { shard })
    }
}

/// The `Replicated`-only half of [`CacheBuilder::open`]: pulls the cache's
/// state from a live peer and starts the anti-entropy scheduler. Anything
/// short of a landed snapshot or a cluster with nothing to give leaves the
/// cache cold, declining to donate until the warm-up task gets it there.
async fn warm_and_repair(cluster: &Cluster, shard_ops: Arc<dyn ShardOps>, name: &SmolStr) {
    let outcome = crate::cluster::state_transfer::run(cluster, &shard_ops, name).await;
    if !matches!(
        outcome,
        crate::cluster::state_transfer::Outcome::Completed
            | crate::cluster::state_transfer::Outcome::NoDonor
    ) {
        cluster.spawn_tracked(crate::cluster::state_transfer::warm_up_task(
            cluster.clone(),
            Arc::clone(&shard_ops),
            name.clone(),
            cluster.cancel_token(),
        ));
    }
    cluster.spawn_tracked(crate::cluster::anti_entropy::scheduler_task(
        cluster.clone(),
        shard_ops,
        name.clone(),
        cluster.config().ae_interval,
        cluster.cancel_token(),
    ));
}

/// A typed handle to one named, possibly-clustered cache. Cheap to
/// `Clone`; every clone shares the same underlying [`Shard`].
#[derive(Clone)]
pub struct Cache<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    shard: Arc<Shard<K, V>>,
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

    /// Reads `key`, without triggering read-through.
    pub async fn get(&self, key: &K) -> Option<V> {
        self.shard.get(key).await
    }

    /// Reads whether `key` has a live entry, honoring expiry, without cloning
    /// it.
    pub async fn contains_key(&self, key: &K) -> bool {
        self.shard.contains_key(key).await
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
}
