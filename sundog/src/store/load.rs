//! A cache's registered [`Loader`] and the read-through it backs:
//! [`Shard::load`] and [`Shard::load_many`] answer a miss by asking the
//! loader, collapse concurrent misses on one key into one load, and group
//! the keys missing at once into one loader call.
//!
//! With a zero batch window, the default, each call hands the keys it
//! misses to the loader at once. With a nonzero window, the first call to
//! queue a key leads the batch: it waits out the window, or until
//! [`Loader::max_keys`] keys are queued, then loads every queued key in one
//! call. A leader that drops before it takes the queue releases every
//! queued load, so the callers waiting on them load again.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures::future::{BoxFuture, join_all};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{Notify, watch};

use super::engine::{self, Inflight, JoinOutcome};
use super::{PartId, Shard, ShardOps, SharedLoaderFailure, encode_key};
use crate::error::CacheError;
use crate::hlc::Hlc;
use crate::net::LoadServe;
use crate::wire::WireRecord;

/// The error a [`Loader`] reports, type-erased.
pub type LoadError = Box<dyn std::error::Error + Send + Sync + 'static>;

type LoadFn<K, V> =
    dyn Fn(Vec<K>) -> BoxFuture<'static, Result<HashMap<K, V>, LoadError>> + Send + Sync;

/// The most keys one loader call takes unless [`Loader::with_window`] sets
/// another bound.
pub const DEFAULT_MAX_BATCH_KEYS: usize = 1024;

/// A cache's registered loader: given keys, returns the values the source
/// holds for them. A key missing from the returned map is one the source
/// does not hold.
pub struct Loader<K, V> {
    load: Arc<LoadFn<K, V>>,
    window: Duration,
    max_keys: usize,
}

impl<K, V> Clone for Loader<K, V> {
    fn clone(&self) -> Self {
        Self {
            load: Arc::clone(&self.load),
            window: self.window,
            max_keys: self.max_keys,
        }
    }
}

impl<K, V> std::fmt::Debug for Loader<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Loader")
            .field("window", &self.window)
            .field("max_keys", &self.max_keys)
            .finish_non_exhaustive()
    }
}

impl<K, V> Loader<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + 'static,
{
    /// A loader that reads many keys in one call, such as one
    /// `SELECT … WHERE id IN (…)`. Zero batch window, at most
    /// [`DEFAULT_MAX_BATCH_KEYS`] keys a call.
    pub fn batch<F, Fut, E>(load: F) -> Self
    where
        F: Fn(Vec<K>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HashMap<K, V>, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            load: Arc::new(move |keys| {
                let loading = load(keys);
                Box::pin(async move { loading.await.map_err(|err| Box::new(err) as LoadError) })
            }),
            window: Duration::ZERO,
            max_keys: DEFAULT_MAX_BATCH_KEYS,
        }
    }

    /// A loader that reads one key a call, `None` for a key the source does
    /// not hold. A batch runs its keys' calls concurrently and fails with
    /// the first error.
    pub fn single<F, Fut, E>(load: F) -> Self
    where
        F: Fn(K) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<V>, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let load = Arc::new(load);
        Self::batch(move |keys: Vec<K>| {
            let calls: Vec<_> = keys
                .into_iter()
                .map(|key| {
                    let loading = load(key.clone());
                    async move { (key, loading.await) }
                })
                .collect();
            async move {
                let mut found = HashMap::new();
                for (key, loaded) in join_all(calls).await {
                    if let Some(value) = loaded? {
                        found.insert(key, value);
                    }
                }
                Ok::<_, E>(found)
            }
        })
    }

    /// Collects the keys missing within `window` of the first into one
    /// loader call, which takes at most `max_keys` keys; a larger batch
    /// splits into concurrent calls. `max_keys` under 1 counts as 1.
    #[must_use]
    pub fn with_window(mut self, window: Duration, max_keys: usize) -> Self {
        self.window = window;
        self.max_keys = max_keys.max(1);
        self
    }

    /// How long a batch waits for more keys after its first.
    #[must_use]
    pub fn window(&self) -> Duration {
        self.window
    }

    /// The most keys one loader call takes.
    #[must_use]
    pub fn max_keys(&self) -> usize {
        self.max_keys
    }
}

/// What a [`SourceFn`] returned for one batch: the values to store as fills
/// and the values to answer with but not store, such as a value another
/// node loaded for a key this node does not own. A key in neither is one
/// the source does not hold.
pub(crate) struct Sourced<K, V> {
    pub(crate) store: HashMap<K, V>,
    pub(crate) answer: HashMap<K, V>,
}

/// Where a batch's keys are read from: the registered loader, or a router
/// that sends some keys to the node their loads gather on.
pub(crate) type SourceFn<'a, K, V> =
    dyn Fn(Vec<K>) -> BoxFuture<'a, Result<Sourced<K, V>, LoadError>> + Send + Sync + 'a;

/// What one queued load ended with, for the call that queued it: the
/// loaded value, `None` for a key the source does not hold, or the
/// loader's failure.
type Slot<V> = Result<Option<V>, Arc<dyn std::error::Error + Send + Sync>>;

/// One key this node is loading: the in-flight entry other readers join,
/// the stamp taken before the load, and the slot the batch fills.
pub(super) struct Queued<K, V> {
    key: K,
    key_bytes: Bytes,
    hash: u64,
    ver: Hlc,
    inflight: Arc<Inflight<V>>,
    slot: Arc<OnceLock<Slot<V>>>,
}

/// The loads waiting for a batch window to close. Empty whenever the
/// loader's window is zero.
pub(super) struct LoadQueue<K, V> {
    pending: StdMutex<Vec<Queued<K, V>>>,
    /// Notified once the queue holds [`Loader::max_keys`] keys, so the
    /// leader flushes before its window closes.
    full: Notify,
}

impl<K, V> LoadQueue<K, V> {
    pub(super) fn new() -> Self {
        Self {
            pending: StdMutex::new(Vec::new()),
            full: Notify::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Queued<K, V>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// How a call learns how one of its keys' loads ended.
enum Wait<K, V> {
    /// A load this call queued, with its own slot.
    Owned {
        key: K,
        inflight: Arc<Inflight<V>>,
        done: watch::Receiver<bool>,
        slot: Arc<OnceLock<Slot<V>>>,
    },
    /// A load another caller runs.
    Joined {
        key: K,
        inflight: Arc<Inflight<V>>,
        done: watch::Receiver<bool>,
    },
}

/// What a finished load answers a call that waited on it.
enum Resolved<V> {
    Found(V),
    Absent,
    Failed(Arc<dyn std::error::Error + Send + Sync>),
    /// The load ended with no outcome, or stored its value: read again.
    Retry,
}

/// The outcome a joined waiter reads off a finished in-flight load. Pure
/// over the load's recorded outcome; unit tested directly.
fn resolve_joined<V: Clone>(inflight: &Inflight<V>) -> Resolved<V> {
    if let Some(err) = inflight.error.get() {
        return Resolved::Failed(Arc::clone(err));
    }
    if let Some(value) = inflight.value.get() {
        return Resolved::Found(value.clone());
    }
    if inflight.is_absent() {
        return Resolved::Absent;
    }
    Resolved::Retry
}

/// The outcome the call that queued a load reads off its slot, `Retry`
/// when the load was released before it ran. Pure; unit tested directly.
fn resolve_owned<V: Clone>(slot: &OnceLock<Slot<V>>) -> Resolved<V> {
    match slot.get() {
        Some(Ok(Some(value))) => Resolved::Found(value.clone()),
        Some(Ok(None)) => Resolved::Absent,
        Some(Err(err)) => Resolved::Failed(Arc::clone(err)),
        None => Resolved::Retry,
    }
}

/// Splits `items` into groups of at most `max_keys`, in order. Pure; unit
/// tested directly.
fn chunked<T>(items: Vec<T>, max_keys: usize) -> Vec<Vec<T>> {
    let max_keys = max_keys.max(1);
    let mut chunks = Vec::with_capacity(items.len().div_ceil(max_keys));
    let mut current = Vec::with_capacity(max_keys.min(items.len()));
    for item in items {
        current.push(item);
        if current.len() == max_keys {
            chunks.push(std::mem::replace(
                &mut current,
                Vec::with_capacity(max_keys),
            ));
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Releases every queued load if the batch leader drops before it takes the
/// queue.
struct ReleaseQueueOnDrop<'a, K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    shard: &'a Shard<K, V>,
    armed: bool,
}

impl<K, V> Drop for ReleaseQueueOnDrop<'_, K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.armed {
            let queued = std::mem::take(&mut *self.shard.load_queue.lock());
            for item in queued {
                self.shard
                    .engine
                    .abandon_inflight(&item.key_bytes, item.hash, &item.inflight);
            }
        }
    }
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Registers the loader [`Shard::load`] and [`Shard::load_many`] answer
    /// a miss through.
    #[must_use]
    pub fn with_loader(mut self, loader: Loader<K, V>) -> Self {
        self.loader = Some(loader);
        self.loader_calls = Some(metrics::counter!(
            "sundog_loader_calls_total",
            "cache" => self.name.to_string()
        ));
        self
    }

    /// Whether a loader is registered.
    #[must_use]
    pub fn has_loader(&self) -> bool {
        self.loader.is_some()
    }

    /// Reads `key`, loading it through the registered [`Loader`] on a miss:
    /// `None` when the source does not hold it. Concurrent misses on one key
    /// collapse into one load, and keys missing within the loader's batch
    /// window share one loader call. A loaded value is stored like a
    /// [`Shard::get_or_load`] fill, under the cache's default TTL.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::NoLoader`] if no loader is registered,
    /// [`CacheError::Loader`] if the loader fails, and
    /// [`CacheError::Codec`] if `key` fails to encode.
    pub async fn load(&self, key: &K) -> Result<Option<V>, CacheError> {
        self.load_from(key, &|keys| {
            Box::pin(async move {
                Ok(Sourced {
                    store: self.call_loader(keys).await?,
                    answer: HashMap::new(),
                })
            })
        })
        .await
    }

    /// [`Shard::load`] with a miss read from `source`; see
    /// [`Shard::load_many_from`].
    pub(crate) async fn load_from(
        &self,
        key: &K,
        source: &SourceFn<'_, K, V>,
    ) -> Result<Option<V>, CacheError> {
        if self.loader.is_none() {
            return Err(CacheError::NoLoader {
                cache: self.name.clone(),
            });
        }
        let key_bytes = encode_key(key)?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        if let Some(value) = self.read_resident(key, key_bytes.as_ref(), hash, self.now_ms()) {
            self.hits.increment(1);
            return Ok(Some(value));
        }
        let mut found = self
            .load_many_from(std::iter::once(key.clone()), source)
            .await?;
        Ok(found.remove(key))
    }

    /// [`Shard::load`] for many keys at once: the keys this node misses go
    /// to the loader together. The map holds every key the cache or the
    /// source holds; a key in neither is left out.
    ///
    /// # Errors
    ///
    /// As [`Shard::load`]; a loader failure for any key fails the call.
    pub async fn load_many(
        &self,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<HashMap<K, V>, CacheError> {
        self.load_many_from(keys, &|keys| {
            Box::pin(async move {
                Ok(Sourced {
                    store: self.call_loader(keys).await?,
                    answer: HashMap::new(),
                })
            })
        })
        .await
    }

    /// Runs the registered loader on `keys` without touching the cache:
    /// what the source holds for them. Empty without a loader. Counts
    /// `sundog_loader_calls_total{cache}`.
    pub(crate) async fn call_loader(&self, keys: Vec<K>) -> Result<HashMap<K, V>, LoadError> {
        let Some(loader) = &self.loader else {
            return Ok(HashMap::new());
        };
        if let Some(calls) = &self.loader_calls {
            calls.increment(1);
        }
        (loader.load)(keys).await
    }

    /// [`Shard::load_many`] with the keys this node misses read from
    /// `source`, which the cache layer points at the node each key's loads
    /// gather on. Batching, collapse and the fill guard are the same.
    pub(crate) async fn load_many_from(
        &self,
        keys: impl IntoIterator<Item = K>,
        source: &SourceFn<'_, K, V>,
    ) -> Result<HashMap<K, V>, CacheError> {
        let Some(loader) = self.loader.as_ref() else {
            return Err(CacheError::NoLoader {
                cache: self.name.clone(),
            });
        };
        let mut seen = HashSet::new();
        let mut todo = Vec::new();
        for key in keys {
            if seen.insert(key.clone()) {
                let key_bytes = encode_key(&key)?;
                let hash = engine::hash_key_bytes(key_bytes.as_ref());
                todo.push((key, key_bytes, hash));
            }
        }
        let mut found = HashMap::with_capacity(todo.len());
        while !todo.is_empty() {
            let (owned, waits) = self.claim(std::mem::take(&mut todo), &mut found).await;
            if !owned.is_empty() {
                self.submit(loader, source, owned).await;
            }
            for wait in waits {
                let (key, resolved) = match wait {
                    Wait::Owned {
                        key,
                        inflight,
                        mut done,
                        slot,
                    } => {
                        let _ = done.wait_for(|done| *done).await;
                        drop(inflight);
                        (key, resolve_owned(&slot))
                    }
                    Wait::Joined {
                        key,
                        inflight,
                        mut done,
                    } => {
                        let _ = done.wait_for(|done| *done).await;
                        let resolved = resolve_joined(&inflight);
                        if !matches!(resolved, Resolved::Retry) {
                            self.hits.increment(1);
                        }
                        (key, resolved)
                    }
                };
                match resolved {
                    Resolved::Found(value) => {
                        found.insert(key, value);
                    }
                    Resolved::Absent => {}
                    Resolved::Failed(err) => {
                        return Err(CacheError::Loader(Box::new(SharedLoaderFailure(err))));
                    }
                    Resolved::Retry => {
                        let key_bytes = encode_key(&key)?;
                        let hash = engine::hash_key_bytes(key_bytes.as_ref());
                        todo.push((key, key_bytes, hash));
                    }
                }
            }
        }
        Ok(found)
    }

    /// Serves a peer's [`crate::wire::Msg::Load`] for `keys`, encoded, as
    /// [`Shard::load_many`] reads them here: the record stored for each key
    /// the cache or source holds, or the loaded value alone where the fill
    /// was not stored. A `Mode::Distributed` shard declines while any
    /// requested key's part is cold or unverified here, as a fetch does. An
    /// undecodable key counts as one the source lacks.
    pub(crate) async fn serve_load_encoded(&self, keys: Vec<Bytes>) -> LoadServe {
        if self.loader.is_none() {
            return LoadServe::Unavailable;
        }
        if keys.iter().any(|key| {
            let part = PartId::of_key(key);
            self.is_cold_part(part) || self.is_unverified_part(part)
        }) {
            return LoadServe::Unavailable;
        }
        let decoded: Vec<K> = keys
            .iter()
            .filter_map(|key| postcard::from_bytes::<K>(key).ok())
            .collect();
        let loaded = match self.load_many(decoded).await {
            Ok(loaded) => loaded,
            Err(CacheError::Loader(err)) => return LoadServe::Failed(err.to_string()),
            Err(err) => return LoadServe::Failed(err.to_string()),
        };
        let held: Vec<K> = loaded.keys().cloned().collect();
        let mut stored: HashMap<Bytes, WireRecord> = self
            .records_for_typed(&held)
            .await
            .into_iter()
            .filter(|rec| !rec.is_tombstone())
            .map(|rec| (rec.key.clone(), rec))
            .collect();
        let mut found = Vec::with_capacity(loaded.len());
        let mut uncached = Vec::new();
        for (key, value) in loaded {
            let Ok(key_bytes) = encode_key(&key) else {
                continue;
            };
            if let Some(rec) = stored.remove(&key_bytes) {
                found.push(rec);
            } else {
                uncached.push((
                    key_bytes,
                    Bytes::from(
                        postcard::to_stdvec(&value)
                            .expect("invariant: a value the shard holds postcard-encodes"),
                    ),
                ));
            }
        }
        LoadServe::Loaded { found, uncached }
    }

    /// Answers each of `todo` from the cache where it can, and otherwise
    /// joins the load in flight for it or starts one, stamped now. Returns
    /// the loads this call starts and every load the call waits on.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_async,
            clippy::unused_async_trait_impl,
            reason = "the spill build reads a spilled key back here"
        )
    )]
    async fn claim(
        &self,
        todo: Vec<(K, Bytes, u64)>,
        found: &mut HashMap<K, V>,
    ) -> (Vec<Queued<K, V>>, Vec<Wait<K, V>>) {
        let mut owned = Vec::new();
        let mut waits = Vec::with_capacity(todo.len());
        for (key, key_bytes, hash) in todo {
            let now = self.now_ms();
            if let Some(value) = self.read_resident(&key, key_bytes.as_ref(), hash, now) {
                self.hits.increment(1);
                found.insert(key, value);
                continue;
            }
            match self.engine.miss_or_join(&key_bytes, hash, now) {
                JoinOutcome::Hit(value) => {
                    self.hits.increment(1);
                    found.insert(key, value);
                }
                JoinOutcome::Join(inflight, done) => {
                    waits.push(Wait::Joined {
                        key,
                        inflight,
                        done,
                    });
                }
                JoinOutcome::Owner(inflight) => {
                    #[cfg(feature = "spill")]
                    {
                        let guard = self.engine.guard_inflight(
                            key_bytes.clone(),
                            hash,
                            Arc::clone(&inflight),
                        );
                        if let Some((ver, value)) =
                            self.get_spilled_by_bytes(key_bytes.as_ref(), hash).await
                        {
                            self.hits.increment(1);
                            // Joined waiters answer with the value, promoted
                            // back to RAM or not, iff the key still holds it.
                            self.engine.finish_spilled_read(
                                &key_bytes,
                                hash,
                                &inflight,
                                ver,
                                value.clone(),
                                self.now_ms(),
                            );
                            guard.complete();
                            found.insert(key, value);
                            continue;
                        }
                        guard.complete();
                    }
                    let slot = Arc::new(OnceLock::new());
                    let done = inflight.subscribe();
                    owned.push(Queued {
                        key: key.clone(),
                        key_bytes,
                        hash,
                        ver: self.stamp_local(),
                        inflight: Arc::clone(&inflight),
                        slot: Arc::clone(&slot),
                    });
                    waits.push(Wait::Owned {
                        key,
                        inflight,
                        done,
                        slot,
                    });
                }
            }
        }
        (owned, waits)
    }

    /// Loads `items` now with a zero window, or queues them for the batch
    /// leader. The first caller to queue into an empty queue leads: it
    /// waits out the window, or until the queue is full, then loads
    /// everything queued.
    async fn submit(
        &self,
        loader: &Loader<K, V>,
        source: &SourceFn<'_, K, V>,
        items: Vec<Queued<K, V>>,
    ) {
        if loader.window.is_zero() {
            self.run_batch(loader, source, items).await;
            return;
        }
        let leads = {
            let mut pending = self.load_queue.lock();
            let leads = pending.is_empty();
            pending.extend(items);
            if pending.len() >= loader.max_keys {
                self.load_queue.full.notify_one();
            }
            leads
        };
        if !leads {
            return;
        }
        let mut release = ReleaseQueueOnDrop {
            shard: self,
            armed: true,
        };
        tokio::select! {
            () = tokio::time::sleep(loader.window) => {}
            () = self.load_queue.full.notified() => {}
        }
        let batch = std::mem::take(&mut *self.load_queue.lock());
        release.armed = false;
        self.run_batch(loader, source, batch).await;
    }

    /// Loads `items` in calls of at most [`Loader::max_keys`] keys, run
    /// concurrently.
    async fn run_batch(
        &self,
        loader: &Loader<K, V>,
        source: &SourceFn<'_, K, V>,
        items: Vec<Queued<K, V>>,
    ) {
        join_all(
            chunked(items, loader.max_keys)
                .into_iter()
                .map(|chunk| self.run_chunk(source, chunk)),
        )
        .await;
    }

    /// One source call: fills each key's slot before ending its in-flight
    /// load, so every waiter it wakes finds the outcome. A key the source
    /// returns to store is stored like a [`Shard::get_or_load`] fill, one
    /// it returns to answer with ends uncached, a key it leaves out ends
    /// absent, and a failure fails every key. Each counts as a miss.
    /// Dropping this future mid-call releases every load it holds.
    async fn run_chunk(&self, source: &SourceFn<'_, K, V>, chunk: Vec<Queued<K, V>>) {
        let guards: Vec<_> = chunk
            .iter()
            .map(|item| {
                self.engine.guard_inflight(
                    item.key_bytes.clone(),
                    item.hash,
                    Arc::clone(&item.inflight),
                )
            })
            .collect();
        let keys = chunk.iter().map(|item| item.key.clone()).collect();
        match source(keys).await {
            Ok(Sourced {
                mut store,
                mut answer,
            }) => {
                for (item, guard) in chunk.into_iter().zip(guards) {
                    if let Some(value) = store.remove(&item.key) {
                        let _ = item.slot.set(Ok(Some(value.clone())));
                        self.finish_fill(
                            &item.key,
                            &item.key_bytes,
                            item.hash,
                            item.ver,
                            &item.inflight,
                            value,
                        );
                    } else if let Some(value) = answer.remove(&item.key) {
                        let _ = item.slot.set(Ok(Some(value.clone())));
                        self.engine.finish_uncached(
                            &item.key_bytes,
                            item.hash,
                            &item.inflight,
                            value,
                        );
                        self.misses.increment(1);
                    } else {
                        let _ = item.slot.set(Ok(None));
                        self.engine
                            .finish_absent(&item.key_bytes, item.hash, &item.inflight);
                        self.misses.increment(1);
                    }
                    guard.complete();
                }
            }
            Err(err) => {
                let err: Arc<dyn std::error::Error + Send + Sync> = Arc::from(err);
                for (item, guard) in chunk.into_iter().zip(guards) {
                    let _ = item.slot.set(Err(Arc::clone(&err)));
                    self.engine.fail_inflight(
                        &item.key_bytes,
                        item.hash,
                        &item.inflight,
                        Arc::clone(&err),
                    );
                    guard.complete();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use smol_str::SmolStr;

    use super::*;
    use crate::node::NodeId;
    use crate::store::{Event, FanOutItem, Mode, Origin, ShardOps};

    #[derive(Debug, thiserror::Error)]
    #[error("source unavailable")]
    struct Down;

    /// Every batch a test loader saw, in call order.
    type Calls = Arc<Mutex<Vec<Vec<u32>>>>;

    fn shard(mode: Mode) -> Shard<u32, String> {
        Shard::new(
            SmolStr::new("test"),
            mode,
            NodeId::from(1),
            10_000,
            None,
            None,
        )
    }

    /// A batch loader that records each call's keys and holds every even
    /// key as `v{key}`.
    fn recording_loader(calls: &Calls) -> Loader<u32, String> {
        let calls = Arc::clone(calls);
        Loader::batch(move |keys: Vec<u32>| {
            let calls = Arc::clone(&calls);
            async move {
                let mut sorted = keys.clone();
                sorted.sort_unstable();
                calls.lock().expect("calls lock").push(sorted);
                Ok::<_, Down>(
                    keys.into_iter()
                        .filter(|key| key.is_multiple_of(2))
                        .map(|key| (key, format!("v{key}")))
                        .collect(),
                )
            }
        })
    }

    fn calls_of(calls: &Calls) -> Vec<Vec<u32>> {
        calls.lock().expect("calls lock").clone()
    }

    #[tokio::test]
    async fn load_without_a_loader_fails_with_no_loader() {
        let s = shard(Mode::Replicated);
        assert!(!s.has_loader());
        assert!(matches!(s.load(&2).await, Err(CacheError::NoLoader { .. })));
        assert!(matches!(
            s.load_many([2, 4]).await,
            Err(CacheError::NoLoader { .. })
        ));
    }

    #[tokio::test]
    async fn load_answers_a_hit_without_the_loader() {
        let calls = Calls::default();
        let s = shard(Mode::Replicated).with_loader(recording_loader(&calls));
        assert!(s.has_loader());
        s.insert(2, "cached".to_string()).await.expect("insert");
        assert_eq!(s.load(&2).await.expect("load"), Some("cached".to_string()));
        assert_eq!(calls_of(&calls), Vec::<Vec<u32>>::new());
    }

    #[tokio::test]
    async fn load_stores_what_the_loader_returns_and_fans_it_out() {
        let calls = Calls::default();
        let s = shard(Mode::Replicated).with_loader(recording_loader(&calls));
        let mut events = s.events();
        assert_eq!(s.load(&2).await.expect("load"), Some("v2".to_string()));
        assert_eq!(
            s.get(&2).await,
            Some("v2".to_string()),
            "the fill is stored"
        );
        assert!(matches!(
            events.try_recv(),
            Ok(Event::Created {
                key: 2,
                origin: Origin::Local,
                ..
            })
        ));
        assert!(matches!(&s.fan_out.drain()[..], [FanOutItem::Applied(2)]));
        assert_eq!(s.load(&2).await.expect("load"), Some("v2".to_string()));
        assert_eq!(calls_of(&calls), vec![vec![2]], "the second read hits");
    }

    #[tokio::test]
    async fn load_answers_none_for_a_key_the_source_lacks_and_caches_nothing() {
        let calls = Calls::default();
        let s = shard(Mode::Replicated).with_loader(recording_loader(&calls));
        assert_eq!(s.load(&3).await.expect("load"), None);
        assert_eq!(s.get(&3).await, None);
        assert_eq!(s.fan_out.drain().len(), 0, "nothing fans out");
        assert_eq!(s.load(&3).await.expect("load"), None);
        assert_eq!(
            calls_of(&calls),
            vec![vec![3], vec![3]],
            "a miss is not cached, so the next read asks the source again"
        );
    }

    #[tokio::test]
    async fn load_many_sends_only_the_missing_keys_in_one_call() {
        let calls = Calls::default();
        let s = shard(Mode::Replicated).with_loader(recording_loader(&calls));
        s.insert(4, "cached".to_string()).await.expect("insert");
        let found = s.load_many([2, 3, 4, 6, 2]).await.expect("load_many");
        assert_eq!(
            found,
            HashMap::from([
                (2, "v2".to_string()),
                (4, "cached".to_string()),
                (6, "v6".to_string())
            ])
        );
        assert_eq!(calls_of(&calls), vec![vec![2, 3, 6]]);
    }

    #[tokio::test]
    async fn load_many_splits_a_batch_past_max_keys() {
        let calls = Calls::default();
        let s = shard(Mode::Replicated)
            .with_loader(recording_loader(&calls).with_window(Duration::ZERO, 2));
        let found = s.load_many([2, 4, 6, 8, 10]).await.expect("load_many");
        assert_eq!(found.len(), 5);
        let mut batches = calls_of(&calls);
        batches.sort();
        assert_eq!(batches, vec![vec![2, 4], vec![6, 8], vec![10]]);
    }

    #[tokio::test]
    async fn concurrent_loads_of_one_key_collapse_into_one_call() {
        let calls = Calls::default();
        let release = Arc::new(Notify::new());
        let loader = {
            let calls = Arc::clone(&calls);
            let release = Arc::clone(&release);
            Loader::batch(move |keys: Vec<u32>| {
                let calls = Arc::clone(&calls);
                let release = Arc::clone(&release);
                async move {
                    calls.lock().expect("calls lock").push(keys.clone());
                    release.notified().await;
                    Ok::<_, Down>(
                        keys.into_iter()
                            .map(|key| (key, format!("v{key}")))
                            .collect(),
                    )
                }
            })
        };
        let s = shard(Mode::Replicated).with_loader(loader);
        let readers = join_all((0..8).map(|_| s.load(&2)));
        let unblock = async {
            while calls_of(&calls).is_empty() {
                tokio::task::yield_now().await;
            }
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            release.notify_one();
        };
        let (answers, ()) = tokio::join!(readers, unblock);
        for answer in answers {
            assert_eq!(answer.expect("load"), Some("v2".to_string()));
        }
        assert_eq!(calls_of(&calls), vec![vec![2]]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_batch_window_groups_concurrent_misses_into_one_call() {
        let calls = Calls::default();
        let s = shard(Mode::Replicated)
            .with_loader(recording_loader(&calls).with_window(Duration::from_millis(5), 100));
        let (a, b, c) = tokio::join!(s.load(&2), s.load(&4), s.load_many([6, 7]));
        assert_eq!(a.expect("load"), Some("v2".to_string()));
        assert_eq!(b.expect("load"), Some("v4".to_string()));
        assert_eq!(
            c.expect("load_many"),
            HashMap::from([(6, "v6".to_string())])
        );
        assert_eq!(calls_of(&calls), vec![vec![2, 4, 6, 7]]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_queue_flushes_before_the_window_closes() {
        let calls = Calls::default();
        let s = shard(Mode::Replicated)
            .with_loader(recording_loader(&calls).with_window(Duration::from_secs(3600), 2));
        let started = tokio::time::Instant::now();
        let (a, b) = tokio::join!(s.load(&2), s.load(&4));
        assert_eq!(a.expect("load"), Some("v2".to_string()));
        assert_eq!(b.expect("load"), Some("v4".to_string()));
        assert!(started.elapsed() < Duration::from_secs(3600));
        assert_eq!(calls_of(&calls), vec![vec![2, 4]]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_leader_dropped_inside_its_window_hands_the_load_to_a_waiter() {
        let calls = Calls::default();
        let s = shard(Mode::Replicated)
            .with_loader(recording_loader(&calls).with_window(Duration::from_millis(50), 100));
        let mut leader = Box::pin(s.load(&2));
        tokio::select! {
            _ = &mut leader => panic!("the window has not closed"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        let joined = s.load(&2);
        tokio::pin!(joined);
        tokio::select! {
            _ = &mut joined => panic!("the joined read waits on the leader's load"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        drop(leader);
        assert_eq!(joined.await.expect("load"), Some("v2".to_string()));
        assert_eq!(
            calls_of(&calls),
            vec![vec![2]],
            "the released load runs once, under the waiter that took it over"
        );
    }

    #[tokio::test]
    async fn a_loader_failure_fails_every_key_of_the_call_and_stores_nothing() {
        let s = shard(Mode::Replicated).with_loader(Loader::batch(|_keys: Vec<u32>| async {
            Err::<HashMap<u32, String>, _>(Down)
        }));
        let err = s.load_many([2, 4]).await.expect_err("the loader fails");
        assert!(matches!(err, CacheError::Loader(_)));
        assert_eq!(err.to_string(), "read-through loader failed");
        assert_eq!(s.get(&2).await, None);
        assert_eq!(s.get(&4).await, None);
    }

    #[tokio::test]
    async fn a_single_key_loader_answers_present_and_absent_keys() {
        let calls = Arc::new(AtomicUsize::new(0));
        let s = shard(Mode::Replicated).with_loader(Loader::single({
            let calls = Arc::clone(&calls);
            move |key: u32| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok::<_, Down>(key.is_multiple_of(2).then(|| format!("v{key}"))) }
            }
        }));
        let found = s.load_many([2, 3, 4]).await.expect("load_many");
        assert_eq!(
            found,
            HashMap::from([(2, "v2".to_string()), (4, "v4".to_string())])
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3, "one call a key");
    }

    #[tokio::test]
    async fn an_invalidation_during_a_load_leaves_the_value_uncached() {
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let s = shard(Mode::Invalidation).with_loader(Loader::batch({
            let release = Arc::clone(&release);
            let started = Arc::clone(&started);
            move |keys: Vec<u32>| {
                let release = Arc::clone(&release);
                let started = Arc::clone(&started);
                async move {
                    started.notify_one();
                    release.notified().await;
                    Ok::<_, Down>(
                        keys.into_iter()
                            .map(|key| (key, "stale".to_string()))
                            .collect(),
                    )
                }
            }
        }));
        let race = async {
            started.notified().await;
            let key_bytes = Bytes::from(postcard::to_stdvec(&2u32).expect("encode"));
            let ver = Hlc {
                wall_ms: super::super::now_ms(),
                logical: 0,
                node: NodeId::from(2),
            };
            ShardOps::invalidate(&s, key_bytes, ver).await;
            release.notify_one();
        };
        let (loaded, ()) = tokio::join!(s.load(&2), race);
        assert_eq!(loaded.expect("load"), Some("stale".to_string()));
        assert_eq!(s.get(&2).await, None);
    }

    #[test]
    fn chunked_splits_in_order_with_a_short_tail() {
        assert_eq!(
            chunked((1..=5).collect(), 2),
            vec![vec![1, 2], vec![3, 4], vec![5]]
        );
        assert_eq!(chunked((1..=4).collect(), 2), vec![vec![1, 2], vec![3, 4]]);
        assert_eq!(chunked(vec![1, 2], 10), vec![vec![1, 2]]);
        assert_eq!(
            chunked(vec![1, 2], 0),
            vec![vec![1], vec![2]],
            "0 counts as 1"
        );
        assert_eq!(chunked(Vec::<u8>::new(), 3), Vec::<Vec<u8>>::new());
    }

    #[test]
    fn resolve_owned_reads_each_slot_outcome() {
        let slot = OnceLock::new();
        assert!(matches!(resolve_owned::<u8>(&slot), Resolved::Retry));
        let _ = slot.set(Ok(Some(4)));
        assert!(matches!(resolve_owned(&slot), Resolved::Found(4)));
        let slot = OnceLock::new();
        let _ = slot.set(Ok(None));
        assert!(matches!(resolve_owned::<u8>(&slot), Resolved::Absent));
        let slot = OnceLock::new();
        let err: Arc<dyn std::error::Error + Send + Sync> = Arc::new(std::io::Error::other("down"));
        let _ = slot.set(Err(err));
        assert!(matches!(resolve_owned::<u8>(&slot), Resolved::Failed(_)));
    }
}
