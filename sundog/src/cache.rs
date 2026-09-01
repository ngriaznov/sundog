//! The typed public cache handle and its builder. `Cache<K, V>` is a thin
//! wrapper over `Arc<Shard<K, V>>` — serialization happens only at the wire
//! boundary; local reads never deserialize.

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
    /// entry expires at the same instant on every node. Default: no expiry.
    ///
    /// Individual writes can override it — [`Cache::insert_with_ttl`],
    /// [`Cache::insert_many_with_ttl`], and [`Cache::get_or_load_with_ttl`]
    /// give one entry (or one batch) its own lifespan, shorter or longer than
    /// this default, and work on a cache with no default at all.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Sets a local-only max-idle (TTI). Deliberately not cluster-replicated.
    /// Default: no idle expiry.
    pub fn tti(mut self, tti: Duration) -> Self {
        self.tti = Some(tti);
        self
    }

    /// Overrides the [`ConflictResolver`] that decides, at the wire level,
    /// which of two differently-versioned records for the same key wins —
    /// consulted by every versioned apply (local write, replicated write,
    /// state transfer, anti-entropy repair). Default: [`LwwResolver`]
    /// (last-write-wins by [`crate::Hlc`]). See [`ConflictResolver::winner`]
    /// for the correctness contract a custom resolver must satisfy for
    /// cluster convergence to keep holding.
    pub fn resolver(mut self, resolver: Arc<dyn ConflictResolver>) -> Self {
        self.resolver = resolver;
        self
    }

    /// Sets a custom per-entry weigher for size-bounded eviction, passed
    /// through to `moka`'s own weigher hook — `max_capacity` is then a
    /// weight budget rather than an entry count. Default: every entry
    /// weighs 1, so `max_capacity` counts entries (moka's own default).
    pub fn weigher<W>(mut self, weigher: W) -> Self
    where
        W: Fn(&K, &V) -> u32 + Send + Sync + 'static,
    {
        self.weigher = Some(Box::new(weigher));
        self
    }

    /// Opens the cache: builds the local shard and registers it in the
    /// cluster's shard registry, so inbound replication/invalidation and
    /// anti-entropy/state-transfer requests for this name can find it from
    /// this point on; unless `mode` is [`Mode::Local`], starts fanning this
    /// shard's own local writes out to the mesh per `mode`.
    ///
    /// For [`Mode::Replicated`], `open()` also runs state transfer before
    /// returning: pulls a full snapshot from the lowest-node-id live
    /// peer, applies it through the same versioned-apply path as live
    /// traffic, then runs one immediate anti-entropy round against that
    /// donor. This is bounded by
    /// [`ClusterConfig::state_transfer_budget`](crate::config::ClusterConfig::state_transfer_budget)
    /// — an unresponsive or empty cluster does not block `open()` forever,
    /// and a cache too large to finish inside the budget opens with a
    /// partial copy that anti-entropy tops up — and is
    /// followed by a background anti-entropy scheduler for the life of the
    /// cache. `Mode::Invalidation` gets neither: its nodes are meant to hold
    /// independent subsets, so there is no cluster-wide snapshot to warm
    /// from (see `cluster::anti_entropy`'s module docs).
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::AlreadyOpen`] if a cache named `name` is already
    /// open in this process — the shard registry is type-erased, so a second
    /// `open()` under a different `K`/`V` can't be checked for compatibility
    /// and is always rejected, even when the type happens to match.
    ///
    /// Returns [`CacheError::ReplicatedWithLocalEviction`] if `mode` is
    /// [`Mode::Replicated`] and `max_capacity`/`tti` was also set: every node
    /// in `Replicated` mode is expected to hold every entry, so a local
    /// capacity/idle eviction would just get silently re-pulled back by the
    /// next anti-entropy round against a peer that still has it, defeating
    /// the bound entirely.
    ///
    /// # Panics
    ///
    /// Panics if the shard registry lock is poisoned, which only happens if
    /// an earlier call already panicked while holding it.
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

        if !matches!(mode, Mode::Local) {
            cluster.spawn_tracked(crate::cluster::fan_out_task(
                Arc::clone(&shard),
                cluster.clone(),
                shard.fan_out_events(),
                name.clone(),
                mode,
                cluster.cancel_token(),
            ));
        }
        if matches!(mode, Mode::Replicated) {
            let shard_ops = Arc::clone(&shard) as Arc<dyn ShardOps>;
            let outcome = crate::cluster::state_transfer::run(&cluster, &shard_ops, &name).await;
            if outcome == crate::cluster::state_transfer::Outcome::NoPeers {
                cluster.spawn_tracked(crate::cluster::state_transfer::late_sync_task(
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
        cluster.spawn_tracked(crate::cluster::tombstone_gc_task(
            Arc::clone(&shard) as Arc<dyn ShardOps>,
            mode,
            cluster.config().tombstone_ttl,
            cluster.config().tombstone_max_ttl,
            cluster.absence_tracker(),
            cluster.cancel_token(),
        ));

        Ok(Cache { shard })
    }
}

/// A typed handle to one named, possibly-clustered cache.
///
/// Cheap to `Clone`; every clone shares the same underlying [`Shard`].
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

    /// The number of live entries in this node's local copy of the cache.
    ///
    /// Counts only this node: in `Invalidation` mode nodes legitimately hold
    /// different subsets, and even in `Replicated` mode replication lag means
    /// two nodes may briefly disagree.
    pub async fn entry_count(&self) -> u64 {
        self.shard.entry_count().await
    }

    /// Reads `key`, invoking `loader` on a miss; concurrent misses on the
    /// same key are collapsed into one `loader` call.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Loader`] if `loader` fails. Returns
    /// [`CacheError::Codec`] if `key` fails to postcard-encode.
    pub async fn get_or_load<F, E>(&self, key: &K, loader: F) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.shard.get_or_load(key, loader).await
    }

    /// [`Cache::get_or_load`], but a fill (a miss that runs `loader`) gets
    /// `ttl` as its lifespan instead of the cache's default. A hit returns
    /// the existing entry as-is, whatever its lifespan.
    ///
    /// # Errors
    ///
    /// As [`Cache::get_or_load`].
    pub async fn get_or_load_with_ttl<F, E>(
        &self,
        key: &K,
        ttl: Duration,
        loader: F,
    ) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.shard.get_or_load_with_ttl(key, ttl, loader).await
    }

    /// Writes `key` = `value`: stamps an HLC version, applies locally, and
    /// fans out per [`Mode`]. The entry gets the cache's default TTL, if
    /// one is configured — [`Cache::insert_with_ttl`] gives it its own.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the encoded value exceeds the
    /// configured frame cap. Returns [`CacheError::Codec`] if `key` fails to
    /// postcard-encode.
    pub async fn insert(&self, key: K, value: V) -> Result<(), CacheError> {
        self.shard.insert(key, value).await
    }

    /// [`Cache::insert`] with a lifespan for this entry alone: `ttl`
    /// overrides the cache's default (shorter or longer), and gives an entry
    /// an expiry on a cache configured with none. The absolute deadline
    /// travels with the record, so the entry expires at the same instant on
    /// every node, and a stale copy can't outlive it anywhere — the same
    /// guarantee a default-TTL entry has.
    ///
    /// # Errors
    ///
    /// As [`Cache::insert`].
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<(), CacheError> {
        self.shard.insert_with_ttl(key, value, ttl).await
    }

    /// Writes many entries at once: each is stamped with its own HLC version
    /// and applied locally under one acquisition of the store's apply lock
    /// (the "amortized lock path" a bulk fill wants), emitting one [`Event`]
    /// per entry exactly as [`Cache::insert`] would. **Not a transaction** —
    /// there is no single version or all-or-nothing outcome for the batch:
    /// if an entry partway through fails to encode or is too large, the
    /// entries before it are still applied. Replication to peers happens the
    /// same way individual inserts do (one `Msg::Replicate`-class send per
    /// entry); this call itself never builds a batched wire message —
    /// `net::conn`'s per-peer writer opportunistically coalesces whatever
    /// lands in the outbox into `Msg::ReplicateBatch` frames.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if any entry's encoded wire
    /// frame exceeds the configured frame cap. Returns [`CacheError::Codec`]
    /// if a key fails to postcard-encode.
    pub async fn insert_many(
        &self,
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), CacheError> {
        self.shard.insert_many(entries).await
    }

    /// [`Cache::insert_many`] with one lifespan applied to every entry in
    /// the batch, overriding the cache's default — see
    /// [`Cache::insert_with_ttl`].
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
    /// Returns a [`CacheError`] if the removal cannot be applied or fanned out.
    pub async fn remove(&self, key: &K) -> Result<(), CacheError> {
        self.shard.remove(key).await
    }

    /// Drops the local copy of `key` without writing a tombstone or fanning
    /// out — an escape hatch for tests and manual cache-busting; the entry
    /// may reappear on the next replicated write or anti-entropy round.
    pub async fn invalidate_local(&self, key: &K) {
        self.shard.invalidate_local(key).await;
    }

    /// Subscribes to this cache's change events (created/updated/removed,
    /// each tagged with its [`crate::store::Origin`]).
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event<K, V>> {
        self.shard.events()
    }
}
