//! Store: moka-backed shards, versioned apply, and the digest machinery that
//! makes anti-entropy cheap. Plan §3, §7, §8.
//!
//! `Shard` intentionally holds no handle to `net::Mesh` — its constructor
//! signature is fixed by `docs/INTERFACES.md` and takes none. Every local
//! mutation (`insert`, `remove`, and `get_or_load`'s fill) publishes an
//! `Origin::Local` [`Event`] on [`Shard::events`]; correlating that stream to
//! wire fan-out (`Mesh::send` per [`Mode`]) is the composition layer's job.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use moka::Expiry;
use moka::notification::RemovalCause;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use xxhash_rust::xxh3::xxh3_64;

use crate::config::ClusterConfig;
use crate::error::{CacheError, CodecError};
use crate::hlc::{Hlc, HlcClock};
use crate::node::NodeId;
use crate::wire::{MAX_FRAME, WireRecord};

/// Number of anti-entropy buckets per shard: `bucket(k) = xxh3(key_bytes) & (BUCKET_COUNT - 1)`.
pub const BUCKET_COUNT: usize = 1024;

/// Records per [`WireRecord`] batch yielded by [`ShardOps::snapshot_chunks`] (plan §9).
const SNAPSHOT_CHUNK_SIZE: usize = 500;

/// Capacity of each shard's [`Event`] broadcast channel. Slow subscribers that
/// fall this far behind miss events (`broadcast::error::RecvError::Lagged`)
/// rather than applying backpressure to writers.
const EVENTS_CAPACITY: usize = 1024;

/// A named cache's clustering behavior, mirroring Infinispan's cache modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// No cluster traffic at all; a plain local `moka` cache.
    Local,
    /// Every node caches independently; writes broadcast an [`crate::wire::Msg::Invalidate`].
    Invalidation,
    /// Every node holds every entry; writes broadcast the full [`crate::wire::Msg::Replicate`].
    Replicated,
}

/// Who caused a cache [`Event`]: this node's own API call, or a message
/// received from a remote peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Origin {
    /// Caused by a local `insert`/`remove`/`get_or_load` call.
    Local,
    /// Caused by an inbound wire message from the given peer.
    Remote(NodeId),
}

/// A change notification published on [`Shard::events`] / `Cache::events`.
#[derive(Debug, Clone)]
pub enum Event<K, V> {
    /// A key was inserted where none existed before.
    Created {
        /// The key that was created.
        key: K,
        /// Its new value.
        value: V,
        /// What caused the write.
        origin: Origin,
    },
    /// An existing key's value changed.
    Updated {
        /// The key that was updated.
        key: K,
        /// Its new value.
        value: V,
        /// What caused the write.
        origin: Origin,
    },
    /// A key was removed (a tombstone was applied).
    Removed {
        /// The key that was removed.
        key: K,
        /// What caused the removal.
        origin: Origin,
    },
}

/// The type-erased surface the network layer drives a shard through, wire
/// bytes in and out — the boundary where postcard (de)serialization actually
/// happens (plan §7: "local reads never deserialize"). Implemented by
/// `Shard<K, V>` for any `K`, `V` meeting its bounds; held as
/// `Arc<dyn ShardOps>` in the cluster's cache registry.
///
/// Async methods return `BoxFuture` rather than using `async fn` in the
/// trait, matching `Discovery`'s object-safety pattern (`dyn ShardOps` must
/// be usable from a `HashMap<SmolStr, Arc<dyn ShardOps>>`).
pub trait ShardOps: Send + Sync {
    /// Applies an inbound replicated record iff its version is newer than
    /// what's stored — the versioned-apply rule that makes replication
    /// commutative (plan §4). The single path shared by local writes, live
    /// replication, state transfer, and anti-entropy repair.
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()>;

    /// Applies an inbound invalidation: drops the local copy of `key` iff
    /// `ver` is newer than the locally stored version.
    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()>;

    /// Returns this shard's current per-bucket XOR digests, `(bucket, digest)`
    /// for all [`BUCKET_COUNT`] buckets — the first step of an anti-entropy
    /// round (plan §8).
    fn digests(&self) -> BoxFuture<'_, Vec<(u16, u64)>>;

    /// Returns `(key, version)` for every live entry and un-GC'd tombstone in
    /// `bucket`, for a peer that reported a digest mismatch there.
    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>>;

    /// Returns the full [`WireRecord`] for each of `keys` that this shard
    /// holds (present entries and tombstones alike), answering an `AePull`.
    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>>;

    /// Streams the shard's full contents in ~500-record chunks for state
    /// transfer to a joining node (plan §9). Iteration is weakly consistent —
    /// safe because every chunk is applied through the same versioned
    /// [`ShardOps::apply_remote`] path as live traffic.
    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>>;

    /// Garbage-collects tombstones older than the configured
    /// `tombstone_ttl`, keeping the digest and entry set consistent.
    fn gc_tombstones(&self) -> BoxFuture<'_, ()>;
}

/// A stored value paired with the version it was last written at — the
/// per-key version table, folded into the cached value itself (plan §7) —
/// and its absolute expiry, so every replica converts the same origin-stamped
/// deadline into a local remaining duration (plan §7).
#[derive(Debug, Clone)]
pub struct Stored<V> {
    /// The current value.
    pub value: V,
    /// The version this value was written at.
    pub ver: Hlc,
    /// Absolute expiry in epoch milliseconds, or `None` for no TTL.
    pub expires_at_ms: Option<u64>,
}

/// A tombstone: the version of the delete that created it, and when it
/// becomes eligible for garbage collection.
#[derive(Debug, Clone, Copy)]
struct Tombstone {
    ver: Hlc,
    gc_deadline_ms: u64,
}

/// What a versioned write carries into [`Shard::apply`]: a live value, or a
/// deletion marker.
enum Incoming<V> {
    Put {
        value: V,
        expires_at_ms: Option<u64>,
    },
    Tombstone,
}

/// Converts an absolute epoch-millisecond expiry into the remaining duration
/// from now, for moka's [`Expiry`] hook. A deadline already in the past
/// yields `Duration::ZERO`: expired-on-arrival records still went through
/// version comparison in [`Shard::apply`] before reaching here (plan §7).
fn remaining_from_absolute(expires_at_ms: Option<u64>) -> Option<Duration> {
    let expires_at_ms = expires_at_ms?;
    Some(Duration::from_millis(
        expires_at_ms.saturating_sub(now_ms()),
    ))
}

/// Converts absolute per-entry expiry into moka's relative-duration `Expiry`
/// hook (plan §7); TTI, by contrast, is configured directly on the
/// `CacheBuilder` since it is local-only (plan §7, §13).
struct AbsoluteExpiry;

impl<K, V> Expiry<K, Arc<Stored<V>>> for AbsoluteExpiry {
    fn expire_after_create(
        &self,
        _key: &K,
        value: &Arc<Stored<V>>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        remaining_from_absolute(value.expires_at_ms)
    }

    fn expire_after_update(
        &self,
        _key: &K,
        value: &Arc<Stored<V>>,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        remaining_from_absolute(value.expires_at_ms)
    }
}

/// Wraps a stampede-collapsed loader failure (`Arc<E>`, from moka's
/// `try_get_with`) as a boxable [`std::error::Error`] for [`CacheError::Loader`].
#[derive(Debug)]
struct LoaderFailure<E>(Arc<E>);

impl<E: std::fmt::Display> std::fmt::Display for LoaderFailure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl<E: std::error::Error + 'static> std::error::Error for LoaderFailure<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The bucket a key's postcard bytes hash into: `xxh3(key_bytes) & (BUCKET_COUNT - 1)`.
fn bucket_of(key_bytes: &[u8]) -> u16 {
    let bucket = xxh3_64(key_bytes) & (BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
}

/// `xxh3(key_bytes ‖ postcard(ver))` — the digest contribution of one live
/// entry or tombstone (plan §8).
fn entry_fingerprint(key_bytes: &[u8], ver: Hlc) -> u64 {
    let mut buf = Vec::with_capacity(key_bytes.len() + 20);
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(
        &postcard::to_stdvec(&ver).expect("invariant: Hlc always postcard-encodes"),
    );
    xxh3_64(&buf)
}

/// Postcard-encodes `key`, its wire form and digest-hash input alike (plan §7).
///
/// Debug builds assert the encoding round-trips to itself: a key type whose
/// `Serialize` impl isn't canonical (e.g. iteration-order-dependent, as a
/// `HashMap`-typed key would be) would silently corrupt digests and break
/// wire identity (plan §13).
fn encode_key<K>(key: &K) -> Result<Bytes, CodecError>
where
    K: Serialize + DeserializeOwned,
{
    let bytes = postcard::to_stdvec(key)?;
    debug_assert!(
        postcard::from_bytes::<K>(&bytes)
            .ok()
            .and_then(|decoded| postcard::to_stdvec(&decoded).ok())
            .is_some_and(|re_encoded| re_encoded == bytes),
        "key's postcard encoding must be canonical/deterministic — no map-typed keys (plan §13)"
    );
    Ok(Bytes::from(bytes))
}

/// A typed named cache: a `moka` cache of `K -> Arc<Stored<V>>` (values
/// `Arc`-wrapped so remote fan-out clones are pointer clones), plus the
/// tombstone map and digest array that back [`ShardOps`]. The typed
/// `Cache<K, V>` handle users hold (`crate::cache`) is a thin wrapper over
/// `Arc<Shard<K, V>>`.
///
/// # Bounds
///
/// `K`'s postcard encoding doubles as its wire form and its digest-hash
/// input, so it must encode deterministically — no map-typed keys (plan §13).
pub struct Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    name: SmolStr,
    mode: Mode,
    cache: moka::future::Cache<K, Arc<Stored<V>>>,
    events: broadcast::Sender<Event<K, V>>,
    /// Guards only the synchronous HLC bump itself, never held across `.await`.
    clock: StdMutex<HlcClock>,
    /// Also the apply-serialization lock: every versioned write holds this
    /// for its whole read-decide-mutate sequence, moka calls included, so
    /// concurrent applies to the shard can't interleave a stale decision.
    /// Safe to hold across those `.await`s because the eviction listener
    /// (which may fire synchronously from inside them) never touches this
    /// lock — it only does lock-free atomic digest updates.
    tombstones: Arc<AsyncMutex<HashMap<Bytes, Tombstone>>>,
    digest: Arc<[AtomicU64]>,
    ttl: Option<Duration>,
    tombstone_ttl_ms: u64,
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Builds a new shard. `node` stamps this shard's local writes.
    ///
    /// Tombstone GC uses [`ClusterConfig::default`]'s `tombstone_ttl` until
    /// overridden via [`Shard::with_tombstone_ttl`]: `Shard::new`'s signature
    /// is fixed by `docs/INTERFACES.md` and takes no `ClusterConfig`, so a
    /// live cluster's configured value reaches this shard through that
    /// follow-up call instead.
    ///
    /// # Panics
    ///
    /// The eviction listener installed here panics only if a key already
    /// admitted into the cache (thus already known to postcard-encode) were
    /// to somehow fail re-encoding — not expected to happen in practice.
    #[must_use]
    pub fn new(
        name: SmolStr,
        mode: Mode,
        node: NodeId,
        max_capacity: u64,
        ttl: Option<Duration>,
        tti: Option<Duration>,
    ) -> Self {
        let digest: Arc<[AtomicU64]> = (0..BUCKET_COUNT)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into();
        let digest_for_listener = Arc::clone(&digest);

        let mut builder = moka::future::Cache::<K, Arc<Stored<V>>>::builder()
            .max_capacity(max_capacity)
            .expire_after(AbsoluteExpiry)
            .eviction_listener(
                move |key: Arc<K>, value: Arc<Stored<V>>, cause: RemovalCause| {
                    // Only moka-decided removals land here: TTL/TTI expiry and
                    // size eviction, both driven by housekeeping this function
                    // has no visibility into (plan §8, §13). Anything *we*
                    // cause — a replace or an explicit remove inside `apply`,
                    // `invalidate_local`, or `ShardOps::invalidate` — is already
                    // subtracted there directly, using the value that call
                    // itself observed; relying on this listener for that too
                    // would double-subtract, since moka may batch its
                    // notification arbitrarily far past the call that caused it.
                    if !matches!(cause, RemovalCause::Expired | RemovalCause::Size) {
                        return;
                    }
                    let key_bytes = postcard::to_stdvec(&*key)
                        .expect("invariant: keys admitted into the cache always postcard-encode");
                    let bucket = usize::from(bucket_of(&key_bytes));
                    digest_for_listener[bucket]
                        .fetch_xor(entry_fingerprint(&key_bytes, value.ver), Ordering::Relaxed);
                },
            );
        if let Some(tti) = tti {
            builder = builder.time_to_idle(tti);
        }

        Self {
            name,
            mode,
            cache: builder.build(),
            events: broadcast::channel(EVENTS_CAPACITY).0,
            clock: StdMutex::new(HlcClock::new(node)),
            tombstones: Arc::new(AsyncMutex::new(HashMap::new())),
            digest,
            ttl,
            tombstone_ttl_ms: duration_ms(ClusterConfig::default().tombstone_ttl),
        }
    }

    /// Overrides the tombstone retention period used by
    /// [`ShardOps::gc_tombstones`] (defaults to [`ClusterConfig::default`]'s
    /// value, since [`Shard::new`]'s signature is fixed and takes no
    /// `ClusterConfig`). Own-and-return, so the composition layer can thread
    /// a live cluster's configured `tombstone_ttl` through right after
    /// construction.
    #[must_use]
    pub fn with_tombstone_ttl(mut self, tombstone_ttl: Duration) -> Self {
        self.tombstone_ttl_ms = duration_ms(tombstone_ttl);
        self
    }

    /// This shard's cache name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// This shard's clustering mode.
    #[must_use]
    pub const fn mode(&self) -> Mode {
        self.mode
    }

    fn stamp_local(&self) -> Hlc {
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .now(now_ms())
    }

    fn observe_remote(&self, remote: Hlc) {
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observe(now_ms(), remote);
    }

    fn ttl_expiry(&self) -> Option<u64> {
        self.ttl.map(|d| now_ms().saturating_add(duration_ms(d)))
    }

    /// The versioned-apply core (plan §4): applies `incoming` at `ver` iff it
    /// is newer than whatever this shard currently holds for `key` (a live
    /// entry or a tombstone), publishing the resulting [`Event`] on success.
    /// Idempotent and commutative — the single path shared by local writes,
    /// replicated writes, state transfer, and anti-entropy repair.
    async fn apply(
        &self,
        key: K,
        key_bytes: Bytes,
        ver: Hlc,
        incoming: Incoming<V>,
        origin: Origin,
    ) {
        let mut tombstones = self.tombstones.lock().await;

        let prior_tombstone = tombstones.get(&key_bytes).copied();
        let stored_ver = match prior_tombstone {
            Some(t) => Some(t.ver),
            None => self.cache.get(&key).await.map(|s| s.ver),
        };
        if stored_ver.is_some_and(|sv| ver <= sv) {
            return;
        }

        let bucket = usize::from(bucket_of(&key_bytes));
        let new_fp = entry_fingerprint(&key_bytes, ver);
        let had_live = prior_tombstone.is_none() && stored_ver.is_some();

        // Subtract whatever this write displaces ourselves rather than
        // leaning on the eviction listener: moka may batch a `Replaced`
        // notification past this point (housekeeping is opportunistic), and
        // `digests()` must be correct the instant this call returns. The
        // listener is reserved for evictions moka decides on its own
        // (TTL/TTI/size) that this function has no visibility into.
        if let Some(t) = prior_tombstone {
            self.digest[bucket].fetch_xor(entry_fingerprint(&key_bytes, t.ver), Ordering::Relaxed);
            tombstones.remove(&key_bytes);
        } else if let Some(sv) = stored_ver {
            self.digest[bucket].fetch_xor(entry_fingerprint(&key_bytes, sv), Ordering::Relaxed);
        }
        self.digest[bucket].fetch_xor(new_fp, Ordering::Relaxed);

        match incoming {
            Incoming::Put {
                value,
                expires_at_ms,
            } => {
                let stored = Arc::new(Stored {
                    value,
                    ver,
                    expires_at_ms,
                });
                self.cache.insert(key.clone(), Arc::clone(&stored)).await;
                drop(tombstones);
                let event = if had_live {
                    Event::Updated {
                        key,
                        value: stored.value.clone(),
                        origin,
                    }
                } else {
                    Event::Created {
                        key,
                        value: stored.value.clone(),
                        origin,
                    }
                };
                let _ = self.events.send(event);
            }
            Incoming::Tombstone => {
                let deadline_ms = now_ms().saturating_add(self.tombstone_ttl_ms);
                tombstones.insert(
                    key_bytes,
                    Tombstone {
                        ver,
                        gc_deadline_ms: deadline_ms,
                    },
                );
                if had_live {
                    let _ = self.cache.remove(&key).await;
                }
                drop(tombstones);
                let _ = self.events.send(Event::Removed { key, origin });
            }
        }
    }

    /// Records the version of a fresh read-through fill for digest/tombstone
    /// bookkeeping (the half of [`Shard::apply`]'s `Put` arm that isn't
    /// "insert into moka", since moka does that itself inside
    /// `try_get_with_by_ref`). Called from within that call's stampede-
    /// collapsed init future, so it runs at most once per genuine miss.
    async fn record_fresh_load(&self, key_bytes: &Bytes, ver: Hlc) {
        let mut tombstones = self.tombstones.lock().await;
        let bucket = usize::from(bucket_of(key_bytes));
        if let Some(t) = tombstones.remove(key_bytes) {
            self.digest[bucket].fetch_xor(entry_fingerprint(key_bytes, t.ver), Ordering::Relaxed);
        }
        self.digest[bucket].fetch_xor(entry_fingerprint(key_bytes, ver), Ordering::Relaxed);
    }

    /// Reads `key`, without triggering read-through. Tombstones never enter
    /// `moka`, so a deleted key simply isn't present here.
    pub async fn get(&self, key: &K) -> Option<V> {
        self.cache.get(key).await.map(|stored| stored.value.clone())
    }

    /// Reads `key`, invoking `loader` on a miss. Concurrent callers racing on
    /// the same missing key are collapsed into one `loader` call (moka
    /// `get_with`'s stampede protection).
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Loader`] if `loader` fails.
    pub async fn get_or_load<F, E>(&self, key: &K, loader: F) -> Result<V, CacheError>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let key_bytes = encode_key(key)?;
        let stored = self
            .cache
            .try_get_with_by_ref(key, async {
                let value = loader(key).await?;
                let ver = self.stamp_local();
                self.record_fresh_load(&key_bytes, ver).await;
                let stored = Arc::new(Stored {
                    value,
                    ver,
                    expires_at_ms: self.ttl_expiry(),
                });
                let _ = self.events.send(Event::Created {
                    key: key.clone(),
                    value: stored.value.clone(),
                    origin: Origin::Local,
                });
                Ok(stored)
            })
            .await
            .map_err(|err: Arc<E>| CacheError::Loader(Box::new(LoaderFailure(err))))?;
        Ok(stored.value.clone())
    }

    /// Stamps and applies a local write, then fans it out per [`Mode`]
    /// (`Invalidate` for `Mode::Invalidation`, `Replicate` for
    /// `Mode::Replicated`, nothing for `Mode::Local`) — via the composition
    /// layer's subscription to [`Shard::events`], since `Shard` holds no
    /// `Mesh` handle itself.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ValueTooLarge`] if the postcard-encoded value
    /// exceeds [`MAX_FRAME`].
    pub async fn insert(&self, key: K, value: V) -> Result<(), CacheError> {
        let key_bytes = encode_key(&key)?;
        let value_bytes = postcard::to_stdvec(&value).map_err(CodecError::from)?;
        if value_bytes.len() > MAX_FRAME {
            return Err(CacheError::ValueTooLarge {
                cache: self.name.clone(),
                size: value_bytes.len(),
                limit: MAX_FRAME,
            });
        }
        let ver = self.stamp_local();
        let expires_at_ms = self.ttl_expiry();
        self.apply(
            key,
            key_bytes,
            ver,
            Incoming::Put {
                value,
                expires_at_ms,
            },
            Origin::Local,
        )
        .await;
        Ok(())
    }

    /// Stamps and applies a local tombstone, then fans it out per [`Mode`]
    /// (see [`Shard::insert`]'s note on how fan-out actually happens).
    ///
    /// # Errors
    ///
    /// Returns a [`CacheError`] if the key cannot be encoded for the wire.
    pub async fn remove(&self, key: &K) -> Result<(), CacheError> {
        let key_bytes = encode_key(key)?;
        let ver = self.stamp_local();
        self.apply(
            key.clone(),
            key_bytes,
            ver,
            Incoming::Tombstone,
            Origin::Local,
        )
        .await;
        Ok(())
    }

    /// Drops the local copy of `key` without writing a tombstone or fanning
    /// out — an escape hatch for tests and manual cache-busting; the entry
    /// may reappear on the next replicated write or anti-entropy round.
    pub async fn invalidate_local(&self, key: &K) {
        let Ok(key_bytes) = postcard::to_stdvec(key) else {
            return;
        };
        // Serializes against `apply` on the same key; `remove` (not
        // `invalidate`) so the departing version comes back directly rather
        // than through the eviction listener, which may batch its
        // notification arbitrarily far past this call.
        let _guard = self.tombstones.lock().await;
        if let Some(old) = self.cache.remove(key).await {
            let bucket = usize::from(bucket_of(&key_bytes));
            self.digest[bucket]
                .fetch_xor(entry_fingerprint(&key_bytes, old.ver), Ordering::Relaxed);
        }
    }

    /// Subscribes to this shard's change events.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event<K, V>> {
        self.events.subscribe()
    }
}

impl<K, V> ShardOps for Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.observe_remote(rec.ver);
            let Ok(key) = postcard::from_bytes::<K>(&rec.key) else {
                tracing::warn!(cache = %self.name, "apply_remote: undecodable key bytes");
                return;
            };
            let origin = Origin::Remote(rec.ver.node);
            match rec.value {
                Some(value_bytes) => {
                    let Ok(value) = postcard::from_bytes::<V>(&value_bytes) else {
                        tracing::warn!(cache = %self.name, "apply_remote: undecodable value bytes");
                        return;
                    };
                    self.apply(
                        key,
                        rec.key,
                        rec.ver,
                        Incoming::Put {
                            value,
                            expires_at_ms: rec.expires_at_ms,
                        },
                        origin,
                    )
                    .await;
                }
                None => {
                    self.apply(key, rec.key, rec.ver, Incoming::Tombstone, origin)
                        .await;
                }
            }
        })
    }

    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.observe_remote(ver);
            let Ok(decoded_key) = postcard::from_bytes::<K>(&key) else {
                tracing::warn!(cache = %self.name, "invalidate: undecodable key bytes");
                return;
            };

            let tombstones = self.tombstones.lock().await;
            let prior_tombstone = tombstones.get(&key).copied();
            let stored_ver = match prior_tombstone {
                Some(t) => Some(t.ver),
                None => self.cache.get(&decoded_key).await.map(|s| s.ver),
            };
            if stored_ver.is_some_and(|sv| ver <= sv) {
                return;
            }
            let had_live = prior_tombstone.is_none() && stored_ver.is_some();
            let removed = if had_live {
                self.cache.remove(&decoded_key).await
            } else {
                None
            };
            drop(tombstones);
            if let Some(old) = removed {
                let bucket = usize::from(bucket_of(&key));
                self.digest[bucket].fetch_xor(entry_fingerprint(&key, old.ver), Ordering::Relaxed);
                let _ = self.events.send(Event::Removed {
                    key: decoded_key,
                    origin: Origin::Remote(ver.node),
                });
            }
        })
    }

    fn digests(&self) -> BoxFuture<'_, Vec<(u16, u64)>> {
        Box::pin(async move {
            self.digest
                .iter()
                .enumerate()
                .map(|(bucket, d)| {
                    let bucket = u16::try_from(bucket)
                        .expect("invariant: bucket index < BUCKET_COUNT fits in u16");
                    (bucket, d.load(Ordering::Relaxed))
                })
                .collect()
        })
    }

    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
        Box::pin(async move {
            let mut out = Vec::new();
            {
                let tombstones = self.tombstones.lock().await;
                out.extend(
                    tombstones
                        .iter()
                        .filter(|(k, _)| bucket_of(k) == bucket)
                        .map(|(k, t)| (k.clone(), t.ver)),
                );
            }
            out.extend(self.cache.iter().filter_map(|(key, stored)| {
                let key_bytes = postcard::to_stdvec(&*key).ok()?;
                (bucket_of(&key_bytes) == bucket).then(|| (Bytes::from(key_bytes), stored.ver))
            }));
            out
        })
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async move {
            let mut out = Vec::with_capacity(keys.len());
            for key_bytes in keys {
                let Ok(key) = postcard::from_bytes::<K>(&key_bytes) else {
                    continue;
                };
                let tomb = self.tombstones.lock().await.get(&key_bytes).copied();
                if let Some(t) = tomb {
                    out.push(WireRecord {
                        key: key_bytes,
                        value: None,
                        ver: t.ver,
                        expires_at_ms: None,
                    });
                    continue;
                }
                if let Some(stored) = self.cache.get(&key).await {
                    let Ok(value_bytes) = postcard::to_stdvec(&stored.value) else {
                        continue;
                    };
                    out.push(WireRecord {
                        key: key_bytes,
                        value: Some(Bytes::from(value_bytes)),
                        ver: stored.ver,
                        expires_at_ms: stored.expires_at_ms,
                    });
                }
            }
            out
        })
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        let cache = self.cache.clone();
        let tombstones = Arc::clone(&self.tombstones);
        let fut = async move {
            let mut records: Vec<WireRecord> = cache
                .iter()
                .filter_map(|(key, stored)| {
                    let key_bytes = postcard::to_stdvec(&*key).ok()?;
                    let value_bytes = postcard::to_stdvec(&stored.value).ok()?;
                    Some(WireRecord {
                        key: Bytes::from(key_bytes),
                        value: Some(Bytes::from(value_bytes)),
                        ver: stored.ver,
                        expires_at_ms: stored.expires_at_ms,
                    })
                })
                .collect();
            records.extend(
                tombstones
                    .lock()
                    .await
                    .iter()
                    .map(|(key_bytes, t)| WireRecord {
                        key: key_bytes.clone(),
                        value: None,
                        ver: t.ver,
                        expires_at_ms: None,
                    }),
            );
            records
                .chunks(SNAPSHOT_CHUNK_SIZE)
                .map(<[WireRecord]>::to_vec)
                .collect::<Vec<_>>()
        };
        Box::pin(stream::once(fut).flat_map(stream::iter))
    }

    fn gc_tombstones(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let now = now_ms();
            let mut tombstones = self.tombstones.lock().await;
            let digest = &self.digest;
            tombstones.retain(|key_bytes, t| {
                let keep = t.gc_deadline_ms > now;
                if !keep {
                    let bucket = usize::from(bucket_of(key_bytes));
                    digest[bucket]
                        .fetch_xor(entry_fingerprint(key_bytes, t.ver), Ordering::Relaxed);
                }
                keep
            });
        })
    }
}

#[cfg(test)]
mod tests {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    use super::*;

    fn shard<K, V>(node: u64) -> Shard<K, V>
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        Shard::new(
            SmolStr::new("test"),
            Mode::Replicated,
            NodeId::from(node),
            10_000,
            None,
            None,
        )
    }

    fn hlc(wall_ms: u64, node: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(node),
        }
    }

    fn key_bytes<K: Serialize>(key: &K) -> Bytes {
        Bytes::from(postcard::to_stdvec(key).expect("test key encodes"))
    }

    #[test]
    fn mode_is_copy_and_comparable() {
        assert_eq!(Mode::Local, Mode::Local);
        assert_ne!(Mode::Local, Mode::Replicated);
    }

    #[test]
    fn origin_distinguishes_local_from_remote() {
        assert_ne!(Origin::Local, Origin::Remote(NodeId::from(1)));
    }

    #[test]
    fn remaining_from_absolute_converts_and_floors_at_zero() {
        assert_eq!(remaining_from_absolute(None), None);
        let past = remaining_from_absolute(Some(1)).expect("some");
        assert_eq!(past, Duration::ZERO);
        let future_ms = now_ms() + 5_000;
        let d = remaining_from_absolute(Some(future_ms)).expect("some");
        assert!(d <= Duration::from_secs(5) && d > Duration::from_secs(4));
    }

    #[tokio::test]
    async fn newer_remote_tombstone_beats_older_local_put() {
        let s = shard::<u32, String>(1);
        s.insert(1, "a".into()).await.expect("insert");
        assert_eq!(s.get(&1).await, Some("a".into()));

        let rec = WireRecord {
            key: key_bytes(&1u32),
            value: None,
            ver: hlc(u64::MAX / 2, 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, rec).await;
        assert_eq!(s.get(&1).await, None);
    }

    #[tokio::test]
    async fn newer_remote_put_beats_older_local_tombstone() {
        let s = shard::<u32, String>(1);
        s.insert(1, "a".into()).await.expect("insert");
        s.remove(&1).await.expect("remove");
        assert_eq!(s.get(&1).await, None);

        let rec = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from_static(b"\x01b")),
            ver: hlc(u64::MAX / 2, 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, rec).await;
        assert_eq!(s.get(&1).await, Some("b".into()));
    }

    #[tokio::test]
    async fn stale_remote_writes_are_rejected_idempotently() {
        let s = shard::<u32, String>(1);
        let winner = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from_static(b"\x01x")),
            ver: hlc(1_000_000, 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, winner.clone()).await;
        assert_eq!(s.get(&1).await, Some("x".into()));

        let stale = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from_static(b"\x01y")),
            ver: hlc(1, 3),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, stale).await;
        assert_eq!(
            s.get(&1).await,
            Some("x".into()),
            "older write must not overwrite a newer one"
        );

        // Re-applying the exact same winning record must also be a no-op.
        ShardOps::apply_remote(&s, winner).await;
        assert_eq!(s.get(&1).await, Some("x".into()));
    }

    #[tokio::test]
    async fn invalidate_respects_newer_local_write() {
        let s = shard::<u32, String>(1);
        s.insert(1, "fresh".into()).await.expect("insert");

        // An invalidation for an old version must not evict a newer local write.
        ShardOps::invalidate(&s, key_bytes(&1u32), hlc(1, 9)).await;
        assert_eq!(s.get(&1).await, Some("fresh".into()));

        // A newer invalidation does evict it, and writes no tombstone.
        ShardOps::invalidate(&s, key_bytes(&1u32), hlc(u64::MAX / 2, 9)).await;
        assert_eq!(s.get(&1).await, None);
        assert!(
            ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn insert_update_remove_emit_matching_events() {
        let s = shard::<u32, String>(1);
        let mut events = s.events();

        s.insert(1, "a".into()).await.expect("insert");
        match events.recv().await.expect("created") {
            Event::Created {
                key: 1,
                value,
                origin: Origin::Local,
            } => assert_eq!(value, "a"),
            other => panic!("unexpected event: {other:?}"),
        }

        s.insert(1, "b".into()).await.expect("insert");
        match events.recv().await.expect("updated") {
            Event::Updated {
                key: 1,
                value,
                origin: Origin::Local,
            } => assert_eq!(value, "b"),
            other => panic!("unexpected event: {other:?}"),
        }

        s.remove(&1).await.expect("remove");
        match events.recv().await.expect("removed") {
            Event::Removed {
                key: 1,
                origin: Origin::Local,
            } => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_or_load_fills_once_and_emits_created() {
        let s = shard::<u32, String>(1);
        let mut events = s.events();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let c = std::sync::Arc::clone(&calls);
        let loaded = s
            .get_or_load(
                &7,
                async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok("loaded".to_string())
                },
            )
            .await
            .expect("load succeeds");
        assert_eq!(loaded, "loaded");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        match events.recv().await.expect("created") {
            Event::Created {
                key: 7,
                value,
                origin: Origin::Local,
            } => assert_eq!(value, "loaded"),
            other => panic!("unexpected event: {other:?}"),
        }

        // A hit must not call the loader again.
        let c2 = std::sync::Arc::clone(&calls);
        let hit = s
            .get_or_load(
                &7,
                async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                    c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok("should-not-run".to_string())
                },
            )
            .await
            .expect("hit succeeds");
        assert_eq!(hit, "loaded");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("loader boom")]
    struct BoomError;

    #[tokio::test]
    async fn get_or_load_propagates_loader_error() {
        let s = shard::<u32, String>(1);
        let err = s
            .get_or_load(&1, async move |_key: &u32| -> Result<String, BoomError> {
                Err(BoomError)
            })
            .await
            .expect_err("loader failed");
        assert!(matches!(err, CacheError::Loader(_)));
    }

    #[tokio::test]
    async fn value_too_large_is_rejected() {
        let s = shard::<u32, Vec<u8>>(1);
        let big = vec![0u8; MAX_FRAME + 1];
        let err = s
            .insert(1, big)
            .await
            .expect_err("must reject oversized value");
        assert!(matches!(err, CacheError::ValueTooLarge { .. }));
    }

    #[tokio::test]
    async fn snapshot_chunks_covers_all_live_entries() {
        let s = shard::<u32, String>(1);
        for i in 0..5u32 {
            s.insert(i, i.to_string()).await.expect("insert");
        }
        let mut stream = ShardOps::snapshot_chunks(&s);
        let mut total = 0usize;
        while let Some(chunk) = stream.next().await {
            total += chunk.len();
        }
        assert_eq!(total, 5);
    }

    #[tokio::test]
    async fn roundtrip_through_shard_ops_converges_two_shards() {
        let a = shard::<u32, String>(1);
        let b = shard::<u32, String>(2);
        a.insert(42, "hello".into()).await.expect("insert");

        let recs = ShardOps::records_for(&a, vec![key_bytes(&42u32)]).await;
        assert_eq!(recs.len(), 1);
        ShardOps::apply_remote(&b, recs[0].clone()).await;
        assert_eq!(b.get(&42).await, Some("hello".into()));

        a.remove(&42).await.expect("remove");
        let recs = ShardOps::records_for(&a, vec![key_bytes(&42u32)]).await;
        assert_eq!(recs.len(), 1);
        assert!(recs[0].is_tombstone());
        ShardOps::apply_remote(&b, recs[0].clone()).await;
        assert_eq!(b.get(&42).await, None);
    }

    #[tokio::test]
    async fn gc_tombstones_drops_expired_entries_and_updates_digest() {
        let mut s = shard::<u32, String>(1);
        s.remove(&1).await.expect("remove creates a tombstone");
        assert!(
            !ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty()
        );

        // Force the tombstone already recorded to read as expired.
        s.tombstone_ttl_ms = 0;
        {
            let mut tombstones = s.tombstones.lock().await;
            for t in tombstones.values_mut() {
                t.gc_deadline_ms = 0;
            }
        }

        ShardOps::gc_tombstones(&s).await;
        assert!(
            ShardOps::records_for(&s, vec![key_bytes(&1u32)])
                .await
                .is_empty()
        );
        assert_digest_matches_full_recompute(&s).await;
    }

    /// One full pass over live entries + tombstones (not the 1024 separate
    /// `bucket_entries` calls that would otherwise be needed), so this stays
    /// cheap enough to call after every op in a several-hundred-op sequence.
    async fn assert_digest_matches_full_recompute<K, V>(s: &Shard<K, V>)
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let mut expected = vec![0u64; BUCKET_COUNT];
        for (key, stored) in &s.cache {
            let key_bytes = postcard::to_stdvec(&*key).expect("test key encodes");
            expected[usize::from(bucket_of(&key_bytes))] ^=
                entry_fingerprint(&key_bytes, stored.ver);
        }
        for (key_bytes, t) in s.tombstones.lock().await.iter() {
            expected[usize::from(bucket_of(key_bytes))] ^= entry_fingerprint(key_bytes, t.ver);
        }
        for (bucket, digest) in ShardOps::digests(s).await {
            assert_eq!(
                digest,
                expected[usize::from(bucket)],
                "bucket {bucket} incremental digest diverged from full recompute"
            );
        }
    }

    #[tokio::test]
    async fn digest_incremental_matches_full_recompute_after_random_ops() {
        let s = shard::<u32, u64>(1);
        let mut rng = StdRng::seed_from_u64(0xC0FF_EE42);

        for i in 0..300 {
            let key = rng.random_range(0..16u32);
            match rng.random_range(0..3u32) {
                0 => {
                    let _ = s.insert(key, u64::from(key) * 31).await;
                }
                1 => {
                    let _ = s.remove(&key).await;
                }
                _ => {
                    let rec = WireRecord {
                        key: key_bytes(&key),
                        value: if rng.random_bool(0.5) {
                            Some(Bytes::from(
                                postcard::to_stdvec(&(u64::from(key) * 7)).expect("encode"),
                            ))
                        } else {
                            None
                        },
                        ver: hlc(rng.random_range(1..u64::MAX / 4), rng.random_range(2..5)),
                        expires_at_ms: None,
                    };
                    ShardOps::apply_remote(&s, rec).await;
                }
            }
            if i % 10 == 0 {
                assert_digest_matches_full_recompute(&s).await;
            }
        }
        assert_digest_matches_full_recompute(&s).await;
    }
}
