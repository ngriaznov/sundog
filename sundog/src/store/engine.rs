//! The store engine: [`BUCKET_COUNT`] stripes, each one anti-entropy bucket,
//! each a [`parking_lot::RwLock`] over the bucket's live entries and
//! tombstones.
//!
//! A read takes a stripe's read lock, finds the key by its postcard bytes in a
//! [`hashbrown::HashTable`], and clones the value. Keys up to `KEY_STACK_BUF`
//! bytes encode on the stack. A versioned write (`apply_locked`) runs
//! synchronously under the stripe's write lock. Anti-entropy enumerates a
//! bucket by locking one stripe.
//!
//! Expiry is checked on every read and reclaimed by [`Engine::sweep`], which
//! visits only stripes with an entry due. Capacity eviction is sampled LRU:
//! [`Engine::enforce_capacity`] locks one stripe at a time, weighs up to
//! `EVICTION_SAMPLE` entries from a rotating offset, and evicts the
//! least recently read, until total weight fits.
//!
//! [`super::Shard::get_or_load`] collapses concurrent misses through a
//! per-stripe map of in-flight loads. A waiter subscribes to the load's
//! completion channel under the stripe lock, so a completion cannot slip
//! between its lookup and its wait. `InflightGuard` frees a cancelled load so a
//! waiter takes over.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use parking_lot::RwLock;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::wire::WireRecord;

use super::{
    BUCKET_COUNT, BucketEntries, ConflictResolver, Incoming, PART_COUNT, PartEntries, RecordView,
    Stored, Tombstone, Weigher, Winner, entry_fingerprint,
};

/// Stack-buffer size for a key's postcard encoding on the read path, large
/// enough for every key type this crate ships and most user key types. A key
/// that doesn't fit falls back to one heap allocation.
const KEY_STACK_BUF: usize = 128;

/// How many live entries one capacity-eviction pass weighs before evicting the
/// least recently read of them.
const EVICTION_SAMPLE: usize = 8;

/// Holds one postcard-encoded key: on the stack when it fits [`KEY_STACK_BUF`],
/// on the heap otherwise.
enum KeyBuf {
    Stack([u8; KEY_STACK_BUF], usize),
    Heap(Vec<u8>),
}

impl KeyBuf {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Stack(buf, len) => &buf[..*len],
            Self::Heap(v) => v,
        }
    }
}

/// Encodes `key` for a read, on the stack when it fits.
fn encode_key_for_read<K: Serialize>(key: &K) -> Result<KeyBuf, CodecError> {
    let mut buf = [0u8; KEY_STACK_BUF];
    let stack_len = match postcard::to_slice(key, &mut buf) {
        Ok(written) => Some(written.len()),
        Err(_) => None,
    };
    match stack_len {
        Some(len) => Ok(KeyBuf::Stack(buf, len)),
        None => Ok(KeyBuf::Heap(postcard::to_stdvec(key)?)),
    }
}

/// The 64-bit xxh3 of a key's encoded bytes, computed once per operation and
/// reused as both the stripe index ([`stripe_index_from_hash`]) and the hash
/// [`hashbrown::HashTable`] stores each entry under.
pub(crate) fn hash_key_bytes(key_bytes: &[u8]) -> u64 {
    xxh3_64(key_bytes)
}

/// The stripe, an anti-entropy bucket, a precomputed key hash belongs to.
pub(crate) fn stripe_index_from_hash(hash: u64) -> usize {
    usize::try_from(hash & (BUCKET_COUNT as u64 - 1))
        .expect("invariant: masked to BUCKET_COUNT - 1, always fits in usize")
}

/// The second-level anti-entropy part, one of [`PART_COUNT`], a precomputed
/// key hash belongs to within its bucket: the six hash bits above the ten
/// [`stripe_index_from_hash`] consumes.
pub(crate) fn part_index_from_hash(hash: u64) -> usize {
    ((hash >> 10) & 63) as usize
}

/// The flat index into `Engine::digest`, `BUCKET_COUNT * PART_COUNT` atomics
/// long, holding `bucket`'s `part`th part digest.
fn digest_slot(bucket: usize, part: usize) -> usize {
    bucket * PART_COUNT + part
}

fn hasher_for<K, V>(live: &Live<K, V>) -> u64 {
    xxh3_64(live.key_bytes.as_ref())
}

/// One live entry: the value, its version and expiry ([`Stored`] inline, no
/// `Arc`), its weight for capacity accounting, and the last time it was read.
/// The read timestamp is written only when TTI or a finite capacity is
/// configured.
struct Live<K, V> {
    key_bytes: Bytes,
    key: K,
    stored: Stored<V>,
    weight: u32,
    last_access_ms: AtomicU64,
}

/// One in-progress [`Engine::get_or_load`] fill, shared by every caller racing
/// on the same missing key. Carries no value: a successful fill is visible to
/// joined waiters by re-reading the stripe once notified. Only a failure
/// travels through here explicitly.
pub(crate) struct Inflight<V> {
    /// Flips to `true` once the fill finishes. A waiter subscribes in
    /// [`Engine::miss_or_join`], under the same stripe lock that removes a
    /// finished fill from the map, so a receiver always exists before the
    /// flip it waits for.
    done: watch::Sender<bool>,
    /// Set iff the fill failed; a joined waiter that finds this populated after
    /// being woken returns the same [`crate::error::CacheError::Loader`]
    /// the owner did.
    pub(crate) error: OnceLock<Arc<dyn std::error::Error + Send + Sync>>,
    _marker: PhantomData<fn() -> V>,
}

impl<V> Inflight<V> {
    fn new() -> Self {
        Self {
            done: watch::channel(false).0,
            error: OnceLock::new(),
            _marker: PhantomData,
        }
    }

    /// Wakes every subscribed waiter; a receiver subscribed before this call
    /// observes the change even if it only starts waiting afterwards.
    fn finish(&self) {
        self.done.send_replace(true);
    }
}

/// One stripe: an anti-entropy bucket's worth of live entries, tombstones, and
/// in-flight loads, all under the one [`parking_lot::RwLock`] that owns this
/// struct.
pub(crate) struct Stripe<K, V> {
    live: HashTable<Live<K, V>>,
    tombstones: HashMap<Bytes, Tombstone>,
    inflight: HashMap<Bytes, Arc<Inflight<V>>>,
    /// The minimum `expires_at_ms` among this stripe's live entries, `u64::MAX`
    /// if none. A lower bound, not necessarily tight, since only
    /// [`Engine::sweep`] recomputes it exactly.
    next_expiry_ms: u64,
}

impl<K, V> Stripe<K, V> {
    fn new() -> Self {
        Self {
            live: HashTable::new(),
            tombstones: HashMap::new(),
            inflight: HashMap::new(),
            next_expiry_ms: u64::MAX,
        }
    }
}

/// Removes the live entry at `key_bytes`, hashing to `hash`, returning its
/// weight and version.
fn remove_live<K, V>(
    table: &mut HashTable<Live<K, V>>,
    hash: u64,
    key_bytes: &[u8],
) -> Option<(u32, Hlc)> {
    match table.entry(hash, |l| l.key_bytes.as_ref() == key_bytes, hasher_for) {
        Entry::Occupied(occ) => {
            let (removed, _vacant) = occ.remove();
            Some((removed.weight, removed.stored.ver))
        }
        Entry::Vacant(_) => None,
    }
}

/// Whether `incoming` at `ver` loses to whatever is already stored at `sv`, the
/// [`ConflictResolver`]-consultation half of [`apply_locked`]'s decision. See
/// [`super::Shard::apply`]'s docs for the correctness contract.
fn incoming_loses<V>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    sv: Hlc,
    stored_encoded: Option<&[u8]>,
    stored_expires_at_ms: Option<u64>,
    ver: Hlc,
    incoming: &Incoming<V>,
) -> bool {
    if sv == ver {
        return true;
    }
    let needs_value_bytes = resolver.needs_value_bytes();
    let stored_view = RecordView {
        value: stored_encoded.filter(|_| needs_value_bytes),
        ver: sv,
        expires_at_ms: stored_expires_at_ms,
    };
    let (incoming_encoded, incoming_expires_at_ms) = match incoming {
        Incoming::Put {
            encoded,
            expires_at_ms,
            ..
        } => (Some(encoded.as_ref()), *expires_at_ms),
        Incoming::Tombstone => (None, None),
    };
    let incoming_view = RecordView {
        value: incoming_encoded.filter(|_| needs_value_bytes),
        ver,
        expires_at_ms: incoming_expires_at_ms,
    };
    resolver.winner(key_bytes, stored_view, incoming_view) == Winner::A
}

/// Whether a read of `live` at `now_ms` sees nothing: past its expiry, or
/// idle for `tti_ms` or longer. Lazy expiry and idle eviction both hinge on
/// this; a sweep only reclaims what it already reports absent.
fn absent_at<K, V>(live: &Live<K, V>, tti_ms: Option<u64>, now_ms: u64) -> bool {
    if let Some(exp) = live.stored.expires_at_ms
        && now_ms >= exp
    {
        return true;
    }
    if let Some(tti) = tti_ms {
        let last = live.last_access_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) >= tti {
            return true;
        }
    }
    false
}

/// The outcome of [`apply_locked`]: the caller's `key` back plus what changed,
/// to build an [`super::Event`] and decide on fan-out.
pub(crate) enum ApplyOutcome<K, V> {
    /// `incoming` lost to what was already stored; nothing changed.
    Rejected,
    /// A value was written. `created` is `false` for a value that replaced an
    /// existing live entry.
    Put { key: K, value: V, created: bool },
    /// A tombstone was written, replacing whatever, live entry or nothing, was
    /// there before. Unlike `Put`'s `created`, this carries no
    /// prior-liveness flag.
    Tombstoned { key: K },
}

impl<K, V> ApplyOutcome<K, V> {
    /// The key a write landed on; `None` for a rejected write, which changed
    /// nothing and has nothing to fan out.
    pub(crate) fn key(&self) -> Option<&K> {
        match self {
            Self::Rejected => None,
            Self::Put { key, .. } | Self::Tombstoned { key } => Some(key),
        }
    }
}

/// The versioned-apply core: applies `incoming` at `ver` for `key`
/// (`key_bytes`/`hash` its postcard-encoded bytes and their xxh3 hash) iff
/// `resolver` picks it over whatever `stripe` currently holds, updating
/// `digest_bucket` and `total_weight` to match. Fully synchronous: the
/// caller holds `stripe`'s write lock for this call's entire duration.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_locked<K, V>(
    stripe: &mut Stripe<K, V>,
    digest_bucket: &AtomicU64,
    total_weight: &AtomicU64,
    weigher: Option<&Weigher<K, V>>,
    tti_ms: Option<u64>,
    hash: u64,
    key: K,
    key_bytes: Bytes,
    ver: Hlc,
    incoming: Incoming<V>,
    resolver: &dyn ConflictResolver,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    now_ms: u64,
) -> ApplyOutcome<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    let prior_tombstone = stripe.tombstones.get(key_bytes.as_ref()).copied();
    // `visible` is what a read at `now_ms` would see: an expired or idle
    // entry still takes part in conflict resolution and still gets displaced,
    // but a write over it counts as a creation, the same as after a sweep.
    let stored_live = if prior_tombstone.is_none() {
        stripe
            .live
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes.as_ref())
            .map(|l| {
                (
                    l.stored.ver,
                    l.stored.encoded.clone(),
                    l.stored.expires_at_ms,
                    !absent_at(l, tti_ms, now_ms),
                )
            })
    } else {
        None
    };
    let stored_ver = prior_tombstone
        .map(|t| t.ver)
        .or_else(|| stored_live.as_ref().map(|(v, _, _, _)| *v));

    if let Some(sv) = stored_ver {
        let stored_encoded = stored_live.as_ref().map(|(_, enc, _, _)| enc.as_ref());
        let stored_expires_at_ms = stored_live.as_ref().and_then(|(_, _, e, _)| *e);
        if incoming_loses(
            resolver,
            key_bytes.as_ref(),
            sv,
            stored_encoded,
            stored_expires_at_ms,
            ver,
            &incoming,
        ) {
            return ApplyOutcome::Rejected;
        }
    }

    let had_live = prior_tombstone.is_none() && stored_ver.is_some();
    let was_visible = stored_live
        .as_ref()
        .is_some_and(|(_, _, _, visible)| *visible);
    let new_fp = entry_fingerprint(key_bytes.as_ref(), ver);

    // Subtracts whatever this write displaces before adding the new fingerprint
    // in.
    if let Some(t) = prior_tombstone {
        digest_bucket.fetch_xor(
            entry_fingerprint(key_bytes.as_ref(), t.ver),
            Ordering::Relaxed,
        );
        stripe.tombstones.remove(key_bytes.as_ref());
    } else if let Some(sv) = stored_ver {
        digest_bucket.fetch_xor(entry_fingerprint(key_bytes.as_ref(), sv), Ordering::Relaxed);
    }
    digest_bucket.fetch_xor(new_fp, Ordering::Relaxed);

    match incoming {
        Incoming::Put {
            value,
            expires_at_ms,
            encoded,
        } => apply_put(
            stripe,
            total_weight,
            weigher,
            hash,
            key,
            key_bytes,
            ver,
            value,
            expires_at_ms,
            encoded,
            had_live,
            was_visible,
            now_ms,
        ),
        Incoming::Tombstone => apply_tombstone(
            stripe,
            total_weight,
            hash,
            key,
            key_bytes,
            ver,
            had_live,
            tombstone_ttl_ms,
            tombstone_max_ttl_ms,
            now_ms,
        ),
    }
}

/// The `Incoming::Put` half of [`apply_locked`]'s write: installs the new
/// value, corrects total weight for whatever it displaced (`had_live`), and
/// reports `created` unless a readable entry (`was_visible`) was replaced.
#[allow(clippy::too_many_arguments)]
fn apply_put<K, V>(
    stripe: &mut Stripe<K, V>,
    total_weight: &AtomicU64,
    weigher: Option<&Weigher<K, V>>,
    hash: u64,
    key: K,
    key_bytes: Bytes,
    ver: Hlc,
    value: V,
    expires_at_ms: Option<u64>,
    encoded: Bytes,
    had_live: bool,
    was_visible: bool,
    now_ms: u64,
) -> ApplyOutcome<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    let weight = weigher.map_or(1, |w| w(&key, &value));
    let old_weight = if had_live {
        remove_live(&mut stripe.live, hash, key_bytes.as_ref()).map(|(w, _)| w)
    } else {
        None
    };
    stripe.live.insert_unique(
        hash,
        Live {
            key_bytes,
            key: key.clone(),
            stored: Stored {
                value: value.clone(),
                encoded,
                ver,
                expires_at_ms,
            },
            weight,
            last_access_ms: AtomicU64::new(now_ms),
        },
        hasher_for,
    );
    if let Some(exp) = expires_at_ms {
        stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
    }
    total_weight.fetch_add(u64::from(weight), Ordering::Relaxed);
    if let Some(ow) = old_weight {
        total_weight.fetch_sub(u64::from(ow), Ordering::Relaxed);
    }
    ApplyOutcome::Put {
        key,
        value,
        created: !was_visible,
    }
}

/// The `Incoming::Tombstone` half of [`apply_locked`]'s write: removes the
/// displaced live entry from total weight, then records the tombstone with its
/// two GC deadlines.
#[allow(clippy::too_many_arguments)]
fn apply_tombstone<K, V>(
    stripe: &mut Stripe<K, V>,
    total_weight: &AtomicU64,
    hash: u64,
    key: K,
    key_bytes: Bytes,
    ver: Hlc,
    had_live: bool,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    now_ms: u64,
) -> ApplyOutcome<K, V>
where
    K: Hash + Eq,
{
    if had_live
        && let Some((old_weight, _)) = remove_live(&mut stripe.live, hash, key_bytes.as_ref())
    {
        total_weight.fetch_sub(u64::from(old_weight), Ordering::Relaxed);
    }
    stripe.tombstones.insert(
        key_bytes,
        Tombstone {
            ver,
            ttl_deadline_ms: now_ms.saturating_add(tombstone_ttl_ms),
            max_deadline_ms: now_ms.saturating_add(tombstone_max_ttl_ms),
        },
    );
    ApplyOutcome::Tombstoned { key }
}

/// The outcome of [`Engine::miss_or_join`]: a fast-path re-check hit, joining
/// an already in-flight load, or this call becoming the one that runs the
/// loader.
pub(crate) enum JoinOutcome<V> {
    Hit(V),
    /// An in-flight load to wait on, with a receiver subscribed under the
    /// stripe lock: `changed()` resolves once the owner finishes, or
    /// immediately if that already happened.
    Join(Arc<Inflight<V>>, watch::Receiver<bool>),
    Owner(Arc<Inflight<V>>),
}

/// A drop guard that frees a cancelled [`Engine::get_or_load`] fill: if the
/// caller's future drops before [`InflightGuard::complete`] runs, the in-flight
/// entry is removed and waiters are notified so one of them takes over.
pub(crate) struct InflightGuard<'a, K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    engine: &'a Engine<K, V>,
    key_bytes: Bytes,
    hash: u64,
    inflight: Arc<Inflight<V>>,
    completed: bool,
}

impl<K, V> InflightGuard<'_, K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Marks the fill as finished, so the drop path becomes a no-op.
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl<K, V> Drop for InflightGuard<'_, K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !self.completed {
            self.engine.finish_inflight(&self.key_bytes, self.hash);
            self.inflight.finish();
        }
    }
}

/// `Engine<K, V>` owns [`BUCKET_COUNT`] independently locked stripes, one per
/// anti-entropy bucket, plus the per-bucket XOR digests and the total live
/// weight for sampled-LRU eviction.
pub(crate) struct Engine<K, V> {
    stripes: Box<[RwLock<Stripe<K, V>>]>,
    digest: Box<[AtomicU64]>,
    total_weight: AtomicU64,
    max_capacity: u64,
    tti_ms: Option<u64>,
    weigher: Option<Weigher<K, V>>,
    evict_cursor: AtomicU64,
}

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        max_capacity: u64,
        tti: Option<Duration>,
        weigher: Option<Weigher<K, V>>,
    ) -> Self {
        Self {
            stripes: (0..BUCKET_COUNT)
                .map(|_| RwLock::new(Stripe::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            digest: (0..BUCKET_COUNT * PART_COUNT)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            total_weight: AtomicU64::new(0),
            max_capacity,
            tti_ms: tti.map(super::duration_ms),
            weigher,
            // Any nonzero seed; xorshift64* never recovers from a zero state.
            evict_cursor: AtomicU64::new(0x9E37_79B9_7F4A_7C15),
        }
    }

    fn is_absent(&self, live: &Live<K, V>, now_ms: u64) -> bool {
        absent_at(live, self.tti_ms, now_ms)
    }

    /// Whether a read updates `last_access_ms`, only when it is consulted for
    /// TTI or sampled for capacity eviction.
    fn tracks_last_access(&self) -> bool {
        self.tti_ms.is_some() || self.max_capacity != u64::MAX
    }

    fn touch(&self, live: &Live<K, V>, now_ms: u64) {
        if self.tracks_last_access() {
            live.last_access_ms.store(now_ms, Ordering::Relaxed);
        }
    }

    /// Reads `key`: a stripe read lock, a hashbrown lookup by the key's
    /// postcard-encoded bytes, and a value clone. No mutation beyond the
    /// recency touch, when configured.
    pub(crate) fn get(&self, key: &K, now_ms: u64) -> Option<V> {
        let key_buf = encode_key_for_read(key).ok()?;
        let key_bytes = key_buf.as_slice();
        self.get_by_bytes(key_bytes, hash_key_bytes(key_bytes), now_ms)
    }

    /// [`Engine::get`] for a key already encoded and hashed, so a caller that
    /// holds both, as the `get_or_load` loop does, skips re-encoding.
    pub(crate) fn get_by_bytes(&self, key_bytes: &[u8], hash: u64, now_ms: u64) -> Option<V> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes)?;
        if self.is_absent(live, now_ms) {
            return None;
        }
        self.touch(live, now_ms);
        Some(live.stored.value.clone())
    }

    /// Whether `key` has a live, unexpired, non-idle entry.
    pub(crate) fn contains_key(&self, key: &K, now_ms: u64) -> bool {
        let Ok(key_buf) = encode_key_for_read(key) else {
            return false;
        };
        let key_bytes = key_buf.as_slice();
        let hash = hash_key_bytes(key_bytes);
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let Some(live) = stripe
            .live
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes)
        else {
            return false;
        };
        if self.is_absent(live, now_ms) {
            return false;
        }
        self.touch(live, now_ms);
        true
    }

    /// Every live, unexpired, non-idle key. O(entries): a full pass over every
    /// stripe.
    pub(crate) fn keys(&self, now_ms: u64) -> Vec<K> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| !self.is_absent(live, now_ms))
                    .map(|live| live.key.clone()),
            );
        }
        out
    }

    /// The full [`WireRecord`] for `key_bytes`, present entry or tombstone
    /// alike.
    pub(crate) fn record_for(&self, key_bytes: &[u8], now_ms: u64) -> Option<WireRecord> {
        let hash = hash_key_bytes(key_bytes);
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        if let Some(t) = stripe.tombstones.get(key_bytes) {
            return Some(WireRecord {
                key: Bytes::copy_from_slice(key_bytes),
                value: None,
                ver: t.ver,
                expires_at_ms: None,
            });
        }
        let live = stripe
            .live
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes)?;
        if self.is_absent(live, now_ms) {
            return None;
        }
        Some(WireRecord {
            key: Bytes::copy_from_slice(key_bytes),
            value: Some(live.stored.encoded.clone()),
            ver: live.stored.ver,
            expires_at_ms: live.stored.expires_at_ms,
        })
    }

    /// [`Engine::record_for`] for every requested bucket, one stripe lock each:
    /// O(bucket size) per bucket, not O(shard size). A bucket at or past
    /// [`BUCKET_COUNT`], which only a misbehaving peer names, yields nothing.
    pub(crate) fn collect_buckets(&self, wanted: &[u16], now_ms: u64) -> BucketEntries {
        wanted
            .iter()
            .filter(|&&bucket| usize::from(bucket) < BUCKET_COUNT)
            .map(|&bucket| {
                let stripe = self.stripes[usize::from(bucket)].read();
                let mut entries = Vec::with_capacity(stripe.live.len() + stripe.tombstones.len());
                entries.extend(
                    stripe
                        .live
                        .iter()
                        .filter(|live| !self.is_absent(live, now_ms))
                        .map(|live| (live.key_bytes.clone(), live.stored.ver)),
                );
                entries.extend(
                    stripe
                        .tombstones
                        .iter()
                        .map(|(key_bytes, t)| (key_bytes.clone(), t.ver)),
                );
                (bucket, entries)
            })
            .collect()
    }

    /// [`Engine::collect_buckets`] at part granularity: `(key, version)` for
    /// every live entry and un-GC'd tombstone in each requested `(bucket,
    /// part)` pair, one stripe read lock per distinct bucket in `wanted`. An
    /// out-of-range bucket ([`BUCKET_COUNT`] or past) or part ([`PART_COUNT`]
    /// or past) is skipped rather than indexed.
    pub(crate) fn collect_parts(&self, wanted: &[(u16, u8)], now_ms: u64) -> PartEntries {
        let mut by_bucket: std::collections::BTreeMap<u16, Vec<u8>> =
            std::collections::BTreeMap::new();
        for &(bucket, part) in wanted {
            if usize::from(bucket) >= BUCKET_COUNT || usize::from(part) >= PART_COUNT {
                continue;
            }
            by_bucket.entry(bucket).or_default().push(part);
        }
        let mut out = Vec::new();
        for (bucket, parts) in by_bucket {
            let stripe = self.stripes[usize::from(bucket)].read();
            for part in parts {
                let mut entries = Vec::new();
                entries.extend(
                    stripe
                        .live
                        .iter()
                        .filter(|live| !self.is_absent(live, now_ms))
                        .filter(|live| {
                            part_index_from_hash(hash_key_bytes(live.key_bytes.as_ref()))
                                == usize::from(part)
                        })
                        .map(|live| (live.key_bytes.clone(), live.stored.ver)),
                );
                entries.extend(
                    stripe
                        .tombstones
                        .iter()
                        .filter(|(key_bytes, _)| {
                            part_index_from_hash(hash_key_bytes(key_bytes)) == usize::from(part)
                        })
                        .map(|(key_bytes, t)| (key_bytes.clone(), t.ver)),
                );
                out.push(((bucket, part), entries));
            }
        }
        out
    }

    /// This engine's current per-bucket XOR digests, `(bucket, digest)` for all
    /// buckets. Each bucket digest is the XOR of its [`PART_COUNT`] part
    /// digests, computed on demand.
    pub(crate) fn digests(&self) -> Vec<(u16, u64)> {
        (0..BUCKET_COUNT)
            .map(|bucket| {
                let idx =
                    u16::try_from(bucket).expect("invariant: index < BUCKET_COUNT fits in u16");
                let digest = (0..PART_COUNT).fold(0u64, |acc, part| {
                    acc ^ self.digest[digest_slot(bucket, part)].load(Ordering::Relaxed)
                });
                (idx, digest)
            })
            .collect()
    }

    /// The number of live entries plus un-GC'd tombstones in `bucket`,
    /// without cloning or enumerating any of them: `O(1)` past the stripe's
    /// read lock. A bucket at or past [`BUCKET_COUNT`] yields `0`. Lets an
    /// anti-entropy responder decide the part-digest threshold without
    /// paying to materialize a bucket's full listing first.
    pub(crate) fn bucket_len(&self, bucket: u16) -> usize {
        if usize::from(bucket) >= BUCKET_COUNT {
            return 0;
        }
        let stripe = self.stripes[usize::from(bucket)].read();
        stripe.live.len() + stripe.tombstones.len()
    }

    /// This engine's current part digests for `bucket`: [`PART_COUNT`] values,
    /// one per part, in ascending part order. A bucket at or past
    /// [`BUCKET_COUNT`], which only a misbehaving peer names, yields an empty
    /// vec.
    pub(crate) fn part_digests(&self, bucket: u16) -> Vec<u64> {
        if usize::from(bucket) >= BUCKET_COUNT {
            return Vec::new();
        }
        (0..PART_COUNT)
            .map(|part| self.digest[digest_slot(usize::from(bucket), part)].load(Ordering::Relaxed))
            .collect()
    }

    /// Every live entry and tombstone as [`WireRecord`]s, for
    /// [`super::ShardOps::snapshot_chunks`].
    pub(crate) fn snapshot_records(&self, now_ms: u64) -> Vec<WireRecord> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| !self.is_absent(live, now_ms))
                    .map(|live| WireRecord {
                        key: live.key_bytes.clone(),
                        value: Some(live.stored.encoded.clone()),
                        ver: live.stored.ver,
                        expires_at_ms: live.stored.expires_at_ms,
                    }),
            );
            out.extend(stripe.tombstones.iter().map(|(key_bytes, t)| WireRecord {
                key: key_bytes.clone(),
                value: None,
                ver: t.ver,
                expires_at_ms: None,
            }));
        }
        out
    }

    /// Drops tombstones past `tombstone_ttl`, or past the hard cap
    /// `tombstone_max_ttl` while `any_member_absent`, correcting the
    /// digest.
    pub(crate) fn gc_tombstones(&self, any_member_absent: bool, now_ms: u64) {
        for (idx, stripe_lock) in self.stripes.iter().enumerate() {
            let mut stripe = stripe_lock.write();
            stripe.tombstones.retain(|key_bytes, t| {
                let past_ttl = now_ms >= t.ttl_deadline_ms;
                let past_max = now_ms >= t.max_deadline_ms;
                let collect = past_ttl && (!any_member_absent || past_max);
                if collect {
                    let part = part_index_from_hash(hash_key_bytes(key_bytes));
                    self.digest[digest_slot(idx, part)]
                        .fetch_xor(entry_fingerprint(key_bytes, t.ver), Ordering::Relaxed);
                }
                !collect
            });
        }
    }

    /// The engine's only free-running housekeeping: visits every stripe whose
    /// `next_expiry_ms` is due, or every stripe if TTI is configured,
    /// removes expired/idle live entries, corrects the digest and total
    /// weight, and recomputes `next_expiry_ms` exactly.
    pub(crate) fn sweep(&self, now_ms: u64) {
        for (idx, stripe_lock) in self.stripes.iter().enumerate() {
            let due = stripe_lock.read().next_expiry_ms <= now_ms;
            if !due && self.tti_ms.is_none() {
                continue;
            }
            let mut stripe = stripe_lock.write();
            let mut removed_weight = 0u64;
            let mut new_next = u64::MAX;
            stripe.live.retain(|live| {
                if self.is_absent(live, now_ms) {
                    let part = part_index_from_hash(hash_key_bytes(live.key_bytes.as_ref()));
                    self.digest[digest_slot(idx, part)].fetch_xor(
                        entry_fingerprint(&live.key_bytes, live.stored.ver),
                        Ordering::Relaxed,
                    );
                    removed_weight += u64::from(live.weight);
                    false
                } else {
                    if let Some(exp) = live.stored.expires_at_ms {
                        new_next = new_next.min(exp);
                    }
                    true
                }
            });
            stripe.next_expiry_ms = new_next;
            drop(stripe);
            if removed_weight > 0 {
                self.total_weight
                    .fetch_sub(removed_weight, Ordering::Relaxed);
            }
        }
    }

    /// The number of live entries across every stripe. O(`BUCKET_COUNT`) locks,
    /// not O(entries).
    pub(crate) fn live_entry_count(&self) -> u64 {
        self.stripes
            .iter()
            .map(|s| u64::try_from(s.read().live.len()).unwrap_or(u64::MAX))
            .fold(0u64, u64::saturating_add)
    }

    /// xorshift64: fast and allocation-free for choosing which stripe to look
    /// at next and where to start sampling. No correctness property depends
    /// on its output.
    fn next_random(&self) -> u64 {
        let mut x = self.evict_cursor.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.evict_cursor.store(x, Ordering::Relaxed);
        x
    }

    fn next_pseudo_random_bucket(&self) -> usize {
        stripe_index_from_hash(self.next_random())
    }

    /// Where in a stripe of `len` entries the next sample starts. Rotating the
    /// start keeps every entry reachable, instead of always weighing the
    /// table's first slots.
    fn sample_offset(&self, len: usize) -> usize {
        if len == 0 {
            0
        } else {
            usize::try_from(self.next_random() % len as u64).unwrap_or(0)
        }
    }

    /// Evicts the coldest of up to [`EVICTION_SAMPLE`] entries in `bucket`.
    /// Returns `false` when the stripe holds nothing to evict.
    fn evict_one_sampled(&self, bucket: usize) -> bool {
        let mut stripe = self.stripes[bucket].write();
        let offset = self.sample_offset(stripe.live.len());
        let Some(victim_bytes) = stripe
            .live
            .iter()
            .skip(offset)
            .chain(stripe.live.iter().take(offset))
            .take(EVICTION_SAMPLE)
            .min_by_key(|live| live.last_access_ms.load(Ordering::Relaxed))
            .map(|live| live.key_bytes.clone())
        else {
            return false;
        };
        let hash = hash_key_bytes(victim_bytes.as_ref());
        let Entry::Occupied(occ) = stripe.live.entry(
            hash,
            |l| l.key_bytes.as_ref() == victim_bytes.as_ref(),
            hasher_for,
        ) else {
            return false;
        };
        let (removed, _vacant) = occ.remove();
        let part = part_index_from_hash(hash);
        self.digest[digest_slot(bucket, part)].fetch_xor(
            entry_fingerprint(&removed.key_bytes, removed.stored.ver),
            Ordering::Relaxed,
        );
        self.total_weight
            .fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
        true
    }

    /// Evicts one entry from the first non-empty stripe at or after `bucket`,
    /// wrapping around once. Returns the stripe it evicted from, or `None`
    /// when every stripe is empty.
    fn evict_one_scanning(&self, bucket: usize) -> Option<usize> {
        (0..BUCKET_COUNT)
            .map(|step| (bucket + step) % BUCKET_COUNT)
            .find(|&candidate| self.evict_one_sampled(candidate))
    }

    /// After a write to `start_bucket` may have pushed total weight over
    /// `max_capacity`, evicts sampled-cold entries, starting at
    /// `start_bucket` then pseudo-random stripes, until it is back under
    /// the cap. A random probe that lands on an empty stripe falls back to
    /// a scan for the next non-empty one, so the loop ends only under the
    /// cap or with nothing left to evict. Never holds two stripe locks at
    /// once; a no-op when `max_capacity` is [`u64::MAX`].
    pub(crate) fn enforce_capacity(&self, start_bucket: usize) {
        if self.max_capacity == u64::MAX {
            return;
        }
        let mut bucket = start_bucket;
        while self.total_weight.load(Ordering::Relaxed) > self.max_capacity {
            if !self.evict_one_sampled(bucket) && self.evict_one_scanning(bucket).is_none() {
                return;
            }
            bucket = self.next_pseudo_random_bucket();
        }
    }

    /// Applies a batch of versioned writes that all hash into `bucket`, under
    /// one write-lock acquisition for the whole group. Runs
    /// [`Self::enforce_capacity`] once afterward, outside the write lock,
    /// iff the batch put anything.
    pub(crate) fn apply_many(
        &self,
        bucket: usize,
        entries: Vec<(u64, K, Bytes, Hlc, Incoming<V>)>,
        resolver: &dyn ConflictResolver,
        tombstone_ttl_ms: u64,
        tombstone_max_ttl_ms: u64,
        now_ms: u64,
    ) -> Vec<ApplyOutcome<K, V>> {
        let mut outcomes = Vec::with_capacity(entries.len());
        let mut wrote = false;
        {
            let mut stripe = self.stripes[bucket].write();
            for (hash, key, key_bytes, ver, incoming) in entries {
                let part = part_index_from_hash(hash);
                let digest_bucket = &self.digest[digest_slot(bucket, part)];
                let outcome = apply_locked(
                    &mut stripe,
                    digest_bucket,
                    &self.total_weight,
                    self.weigher.as_ref(),
                    self.tti_ms,
                    hash,
                    key,
                    key_bytes,
                    ver,
                    incoming,
                    resolver,
                    tombstone_ttl_ms,
                    tombstone_max_ttl_ms,
                    now_ms,
                );
                wrote |= matches!(outcome, ApplyOutcome::Put { .. });
                outcomes.push(outcome);
            }
        }
        if wrote {
            self.enforce_capacity(bucket);
        }
        outcomes
    }

    /// Applies an inbound [`super::ShardOps::invalidate`]: drops the live entry
    /// at `key_bytes` iff `ver` is newer than whatever version is stored,
    /// writing no tombstone of its own. Returns the departing version on an
    /// actual removal, `None` otherwise.
    pub(crate) fn invalidate(&self, key_bytes: &[u8], hash: u64, ver: Hlc) -> Option<Hlc> {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        let prior_tombstone = stripe.tombstones.get(key_bytes).copied();
        let stored_ver = match prior_tombstone {
            Some(t) => Some(t.ver),
            None => stripe
                .live
                .find(hash, |l| l.key_bytes.as_ref() == key_bytes)
                .map(|l| l.stored.ver),
        };
        if stored_ver.is_some_and(|sv| ver <= sv) {
            return None;
        }
        let had_live = prior_tombstone.is_none() && stored_ver.is_some();
        if !had_live {
            return None;
        }
        let (weight, old_ver) = remove_live(&mut stripe.live, hash, key_bytes)?;
        drop(stripe);
        let part = part_index_from_hash(hash);
        self.digest[digest_slot(bucket, part)]
            .fetch_xor(entry_fingerprint(key_bytes, old_ver), Ordering::Relaxed);
        self.total_weight
            .fetch_sub(u64::from(weight), Ordering::Relaxed);
        Some(old_ver)
    }

    /// Drops the local live entry at `key_bytes` unconditionally, no version
    /// check, no tombstone, for [`super::Shard::invalidate_local`]'s
    /// cache-busting escape hatch.
    pub(crate) fn invalidate_local(&self, key_bytes: &[u8], hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some((weight, ver)) = remove_live(&mut stripe.live, hash, key_bytes) {
            drop(stripe);
            let part = part_index_from_hash(hash);
            self.digest[digest_slot(bucket, part)]
                .fetch_xor(entry_fingerprint(key_bytes, ver), Ordering::Relaxed);
            self.total_weight
                .fetch_sub(u64::from(weight), Ordering::Relaxed);
        }
    }

    /// The lock-protected first half of [`super::Shard::get_or_load`]: a
    /// fast-path re-check, then either joining an already in-flight load or
    /// registering as the new owner.
    pub(crate) fn miss_or_join(&self, key_bytes: &Bytes, hash: u64, now_ms: u64) -> JoinOutcome<V> {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some(live) = stripe
            .live
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes.as_ref())
            && !self.is_absent(live, now_ms)
        {
            self.touch(live, now_ms);
            return JoinOutcome::Hit(live.stored.value.clone());
        }
        if let Some(existing) = stripe.inflight.get(key_bytes.as_ref()) {
            return JoinOutcome::Join(Arc::clone(existing), existing.done.subscribe());
        }
        let inflight = Arc::new(Inflight::new());
        stripe
            .inflight
            .insert(key_bytes.clone(), Arc::clone(&inflight));
        JoinOutcome::Owner(inflight)
    }

    /// Builds the [`InflightGuard`] that frees `inflight` if its loader future
    /// drops early.
    pub(crate) fn guard_inflight(
        &self,
        key_bytes: Bytes,
        hash: u64,
        inflight: Arc<Inflight<V>>,
    ) -> InflightGuard<'_, K, V> {
        InflightGuard {
            engine: self,
            key_bytes,
            hash,
            inflight,
            completed: false,
        }
    }

    /// Removes `key_bytes`'s in-flight entry from its stripe. Setting an error,
    /// on failure, is the caller's job against the `Inflight` handle it
    /// already holds.
    fn finish_inflight(&self, key_bytes: &Bytes, hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        stripe.inflight.remove(key_bytes.as_ref());
    }

    /// Applies a successful [`super::Shard::get_or_load`] fill: removes any
    /// prior tombstone or live entry for `key`, unconditionally installs
    /// the loader's value, and removes the `inflight` entry, all under one
    /// stripe write-lock acquisition. Returns whether a live entry
    /// already existed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn complete_fresh_load(
        &self,
        key: &K,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        value: V,
        encoded: Bytes,
        expires_at_ms: Option<u64>,
        now_ms: u64,
        inflight: &Inflight<V>,
    ) -> bool {
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        let had_live = {
            let mut stripe = self.stripes[bucket].write();
            stripe.inflight.remove(key_bytes.as_ref());
            let digest_bucket = &self.digest[digest_slot(bucket, part)];
            let mut had_live = false;
            if let Some(t) = stripe.tombstones.remove(key_bytes.as_ref()) {
                digest_bucket.fetch_xor(
                    entry_fingerprint(key_bytes.as_ref(), t.ver),
                    Ordering::Relaxed,
                );
            } else if let Some((old_weight, old_ver)) =
                remove_live(&mut stripe.live, hash, key_bytes.as_ref())
            {
                had_live = true;
                digest_bucket.fetch_xor(
                    entry_fingerprint(key_bytes.as_ref(), old_ver),
                    Ordering::Relaxed,
                );
                self.total_weight
                    .fetch_sub(u64::from(old_weight), Ordering::Relaxed);
            }
            digest_bucket.fetch_xor(
                entry_fingerprint(key_bytes.as_ref(), ver),
                Ordering::Relaxed,
            );
            let weight = self.weigher.as_ref().map_or(1, |w| w(key, &value));
            stripe.live.insert_unique(
                hash,
                Live {
                    key_bytes: key_bytes.clone(),
                    key: key.clone(),
                    stored: Stored {
                        value,
                        encoded,
                        ver,
                        expires_at_ms,
                    },
                    weight,
                    last_access_ms: AtomicU64::new(now_ms),
                },
                hasher_for,
            );
            if let Some(exp) = expires_at_ms {
                stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
            }
            self.total_weight
                .fetch_add(u64::from(weight), Ordering::Relaxed);
            had_live
        };
        inflight.finish();
        self.enforce_capacity(bucket);
        had_live
    }

    /// Records a failed loader run: removes the `inflight` entry and stores
    /// `error` so every joined waiter returns the same
    /// [`crate::error::CacheError::Loader`].
    pub(crate) fn fail_inflight(
        &self,
        key_bytes: &Bytes,
        hash: u64,
        inflight: &Inflight<V>,
        error: Arc<dyn std::error::Error + Send + Sync>,
    ) {
        let _ = inflight.error.set(error);
        self.finish_inflight(key_bytes, hash);
        inflight.finish();
    }
}

/// One live entry as [`Engine::debug_snapshot`] reports it: key bytes, encoded
/// value, version.
#[cfg(test)]
type DebugLive = (Bytes, Bytes, Hlc);

/// One tombstone as [`Engine::debug_snapshot`] reports it: key bytes and
/// version.
#[cfg(test)]
type DebugTombstone = (Bytes, Hlc);

#[cfg(test)]
impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Recomputes every part digest from scratch, `BUCKET_COUNT * PART_COUNT`
    /// values indexed by [`digest_slot`], to check against the incrementally
    /// maintained ones.
    pub(crate) fn recompute_digests(&self) -> Vec<u64> {
        let mut out = vec![0u64; BUCKET_COUNT * PART_COUNT];
        for (idx, stripe_lock) in self.stripes.iter().enumerate() {
            let stripe = stripe_lock.read();
            for live in &stripe.live {
                let part = part_index_from_hash(hash_key_bytes(live.key_bytes.as_ref()));
                out[digest_slot(idx, part)] ^= entry_fingerprint(&live.key_bytes, live.stored.ver);
            }
            for (key_bytes, t) in &stripe.tombstones {
                let part = part_index_from_hash(hash_key_bytes(key_bytes));
                out[digest_slot(idx, part)] ^= entry_fingerprint(key_bytes, t.ver);
            }
        }
        out
    }

    /// Every stripe's raw contents, for building a canonical state to compare
    /// across replicas.
    pub(crate) fn debug_snapshot(&self) -> (Vec<DebugLive>, Vec<DebugTombstone>) {
        let mut live_out = Vec::new();
        let mut tomb_out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            for live in &stripe.live {
                live_out.push((
                    live.key_bytes.clone(),
                    live.stored.encoded.clone(),
                    live.stored.ver,
                ));
            }
            for (key_bytes, t) in &stripe.tombstones {
                tomb_out.push((key_bytes.clone(), t.ver));
            }
        }
        (live_out, tomb_out)
    }

    /// Total live entries and current total weight, for capacity eviction
    /// tests.
    pub(crate) fn debug_totals(&self) -> (u64, u64) {
        (
            self.live_entry_count(),
            self.total_weight.load(Ordering::Relaxed),
        )
    }

    /// Forces the tombstone at `key_bytes` past `ttl_deadline_ms`, past
    /// `max_deadline_ms` too when `past_max`.
    pub(crate) fn debug_force_tombstone_ttl_past(&self, key_bytes: &[u8], past_max: bool) {
        let hash = hash_key_bytes(key_bytes);
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some(t) = stripe.tombstones.get_mut(key_bytes) {
            t.ttl_deadline_ms = 0;
            if past_max {
                t.max_deadline_ms = 0;
            }
        }
    }

    /// Direct access to one stripe's lock, for tests proving stripe
    /// independence at the lock level: a raw [`parking_lot::RwLock`] blocks
    /// the OS thread that waits on it.
    pub(crate) fn stripe_lock(&self, bucket: usize) -> &RwLock<Stripe<K, V>> {
        &self.stripes[bucket]
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;
    use crate::hlc::HlcClock;
    use crate::node::NodeId;
    use crate::store::LwwResolver;

    fn engine_u32_string(max_capacity: u64, tti: Option<Duration>) -> Engine<u32, String> {
        Engine::new(max_capacity, tti, None)
    }

    fn key_bytes(key: u32) -> Bytes {
        Bytes::from(postcard::to_stdvec(&key).expect("u32 encodes"))
    }

    fn hlc(wall_ms: u64, node: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(node),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn put<K, V>(
        engine: &Engine<K, V>,
        key: K,
        key_bytes: Bytes,
        value: V,
        ver: Hlc,
        expires_at_ms: Option<u64>,
        now_ms: u64,
    ) -> ApplyOutcome<K, V>
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let hash = hash_key_bytes(key_bytes.as_ref());
        let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = engine.stripes[bucket].write();
        let resolver = LwwResolver;
        apply_locked(
            &mut stripe,
            &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
            &engine.total_weight,
            engine.weigher.as_ref(),
            engine.tti_ms,
            hash,
            key,
            key_bytes,
            ver,
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            },
            &resolver,
            60_000,
            600_000,
            now_ms,
        )
    }

    #[test]
    fn read_of_an_expired_entry_is_absent_before_any_sweep() {
        let engine = engine_u32_string(u64::MAX, None);
        let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
        assert_eq!(engine.get(&1, 0), Some("a".to_string()));
        // Past the deadline, but no sweep has run yet.
        assert_eq!(
            engine.get(&1, 100),
            None,
            "an expired entry reads as absent immediately"
        );
    }

    #[test]
    fn contains_key_and_record_for_read_an_expired_entry_as_absent_before_any_sweep() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), Some(50), 0);
        assert!(engine.contains_key(&1, 0));
        assert!(engine.record_for(kb.as_ref(), 0).is_some());

        // Past the deadline, but no sweep has run yet.
        assert!(
            !engine.contains_key(&1, 100),
            "an expired entry reads as absent from contains_key immediately"
        );
        assert!(
            engine.record_for(kb.as_ref(), 100).is_none(),
            "record_for treats an expired live entry as absent, no sweep needed"
        );
    }

    #[test]
    fn sweep_removes_expired_entries_and_corrects_the_digest() {
        let engine = engine_u32_string(u64::MAX, None);
        let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
        let _ = put(&engine, 2, key_bytes(2), "b".into(), hlc(1, 1), None, 0);

        engine.sweep(100);
        assert_eq!(engine.get(&1, 100), None);
        assert_eq!(engine.get(&2, 100), Some("b".to_string()));
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        let (entries, _) = engine.debug_totals();
        assert_eq!(entries, 1, "only the non-expired entry survives the sweep");
    }

    #[test]
    fn next_expiry_ms_skip_logic_leaves_stripes_with_nothing_due_untouched() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(&engine, key, kb, "a".into(), hlc(1, 1), Some(10_000), 0);
        assert_eq!(engine.stripe_lock(bucket).read().next_expiry_ms, 10_000);

        // Sweeping well before the deadline does not touch this stripe.
        engine.sweep(1);
        assert_eq!(engine.get(&key, 1), Some("a".to_string()));
        assert_eq!(engine.stripe_lock(bucket).read().next_expiry_ms, 10_000);
    }

    #[test]
    fn tti_idle_eviction() {
        let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
        let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
        assert_eq!(
            engine.get(&1, 50),
            Some("a".to_string()),
            "read at 50ms refreshes idle clock"
        );
        assert_eq!(
            engine.get(&1, 140),
            Some("a".to_string()),
            "90ms since the last read, still alive"
        );
        assert_eq!(engine.get(&1, 400), None, "idle past the 100ms TTI");
    }

    #[test]
    fn weighted_capacity_eviction_stays_within_bound_and_evicts_colder_first() {
        // "Coldest first" holds only within one bucket. Find several keys in
        // the same bucket so a single sampling pass sees all of them.
        let target_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(0).as_ref()));
        let mut same_bucket_keys = vec![0u32];
        let mut candidate = 1u32;
        while same_bucket_keys.len() < 5 {
            if stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()))
                == target_bucket
            {
                same_bucket_keys.push(candidate);
            }
            candidate += 1;
        }

        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        // Five 5-unit entries under a 20-unit cap: exactly one goes.
        let engine = Engine::<u32, String>::new(20, None, Some(weigher));
        for (i, &k) in same_bucket_keys.iter().enumerate() {
            let now = u64::try_from(i).expect("small") * 100;
            let _ = put(
                &engine,
                k,
                key_bytes(k),
                "x".repeat(5),
                hlc(u64::from(k) + 1, 1),
                None,
                now,
            );
        }
        let (entries_before, weight_before) = engine.debug_totals();
        assert_eq!(entries_before, 5);
        assert_eq!(weight_before, 25);

        engine.enforce_capacity(target_bucket);

        let (entries_after, weight_after) = engine.debug_totals();
        assert!(
            weight_after <= 20,
            "total weight {weight_after} stays within the 20-unit cap"
        );
        assert_eq!(
            entries_after, 4,
            "exactly one 5-unit entry is evicted to clear a 5-unit overage"
        );
        assert_eq!(
            engine.get(&same_bucket_keys[0], 1_000),
            None,
            "the coldest (first-inserted) entry is the one evicted"
        );
        for &k in &same_bucket_keys[1..] {
            assert!(engine.get(&k, 1_000).is_some(), "hotter entries survive");
        }
    }

    #[test]
    fn collect_buckets_reports_a_removed_key_with_its_tombstone_version() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 3u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(&engine, key, kb.clone(), "a".into(), hlc(1, 1), None, 0);

        {
            let mut stripe = engine.stripe_lock(bucket).write();
            let resolver = LwwResolver;
            let _ = apply_locked(
                &mut stripe,
                &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
                &engine.total_weight,
                engine.weigher.as_ref(),
                engine.tti_ms,
                hash,
                key,
                kb.clone(),
                hlc(2, 1),
                Incoming::Tombstone,
                &resolver,
                60_000,
                600_000,
                0,
            );
        }
        assert_eq!(engine.get(&key, 0), None);

        let bucket_u16 = u16::try_from(bucket).expect("invariant: bucket < BUCKET_COUNT");
        let entries = engine.collect_buckets(&[bucket_u16], 0);
        assert_eq!(entries.len(), 1);
        let (_, records) = &entries[0];
        assert!(
            records
                .iter()
                .any(|(k, ver)| k.as_ref() == kb.as_ref() && *ver == hlc(2, 1)),
            "a removed key still appears in its bucket's entries, carrying the tombstone's \
             version: {records:?}"
        );
    }

    #[test]
    fn capacity_eviction_rotates_past_an_empty_start_bucket_into_other_stripes() {
        let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
        let engine = Engine::<u32, String>::new(3, None, Some(weigher));

        // 8 keys landing in 8 distinct, non-empty stripes.
        let mut other_keys = Vec::new();
        let mut used_buckets = std::collections::HashSet::new();
        let mut candidate = 0u32;
        while other_keys.len() < 8 {
            let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()));
            if used_buckets.insert(bucket) {
                other_keys.push(candidate);
            }
            candidate += 1;
        }
        for (i, &k) in other_keys.iter().enumerate() {
            let now = u64::try_from(i).expect("small");
            let _ = put(
                &engine,
                k,
                key_bytes(k),
                k.to_string(),
                hlc(u64::from(k) + 1, 1),
                None,
                now,
            );
        }

        // A key landing in a stripe none of the above touched: the eviction
        // start point, but with only one entry to give up.
        let start_key = loop {
            let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()));
            if !used_buckets.contains(&bucket) {
                break candidate;
            }
            candidate += 1;
        };
        let start_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(start_key).as_ref()));
        let _ = put(
            &engine,
            start_key,
            key_bytes(start_key),
            start_key.to_string(),
            hlc(1_000, 1),
            None,
            100,
        );

        let (entries_before, weight_before) = engine.debug_totals();
        assert_eq!(entries_before, 9);
        assert_eq!(weight_before, 9);

        engine.enforce_capacity(start_bucket);

        let (entries_after, weight_after) = engine.debug_totals();
        assert!(
            weight_after <= 3,
            "total weight {weight_after} stays within the 3-unit cap"
        );
        assert!(
            entries_after < entries_before - 1,
            "the start stripe alone (1 entry) cannot account for a {}-entry eviction: \
             enforce_capacity rotated into other stripes",
            entries_before - entries_after
        );
        assert_eq!(
            engine.get(&start_key, 100),
            None,
            "the start stripe's own entry is evicted too"
        );
    }

    #[tokio::test]
    async fn stampede_collapses_to_one_loader_n_minus_one_hits() {
        const CONCURRENCY: usize = 32;
        let engine = Arc::new(engine_u32_string(u64::MAX, None));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..CONCURRENCY {
            let engine = Arc::clone(&engine);
            let calls = Arc::clone(&calls);
            tasks.spawn(async move {
                let key = 7u32;
                let kb = key_bytes(key);
                let hash = hash_key_bytes(kb.as_ref());
                loop {
                    if let Some(v) = engine.get(&key, 0) {
                        return v;
                    }
                    match engine.miss_or_join(&kb, hash, 0) {
                        JoinOutcome::Hit(v) => return v,
                        JoinOutcome::Join(inflight, mut done) => {
                            let _ = done.changed().await;
                            if let Some(e) = inflight.error.get() {
                                panic!("unexpected loader failure: {e}");
                            }
                        }
                        JoinOutcome::Owner(inflight) => {
                            let guard =
                                engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));
                            calls.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            let value = "loaded-once".to_string();
                            let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                            let had_live = engine.complete_fresh_load(
                                &key,
                                &kb,
                                hash,
                                hlc(1, 1),
                                value.clone(),
                                encoded,
                                None,
                                0,
                                &inflight,
                            );
                            assert!(!had_live, "a genuine miss sees no prior live entry");
                            guard.complete();
                            return value;
                        }
                    }
                }
            });
        }
        let mut results = Vec::with_capacity(CONCURRENCY);
        while let Some(r) = tasks.join_next().await {
            results.push(r.expect("spawned call does not panic"));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "exactly one loader call under a stampede"
        );
        assert!(results.iter().all(|v| v == "loaded-once"));
    }

    #[tokio::test]
    async fn loader_error_is_shared_by_joined_waiters() {
        let engine = Arc::new(engine_u32_string(u64::MAX, None));
        let key = 9u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());

        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        let guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));

        // The receiver subscribes under the stripe lock, so a late waiter still
        // sees the failure.
        let JoinOutcome::Join(joined, mut done) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("second caller joins the existing load");
        };

        let boom: Arc<dyn std::error::Error + Send + Sync> =
            Arc::new(std::io::Error::other("boom"));
        engine.fail_inflight(&kb, hash, &inflight, boom);
        guard.complete();

        done.changed().await.expect("the owner finished the fill");
        assert!(
            joined.error.get().is_some(),
            "the joined waiter sees the error"
        );
        assert_eq!(
            engine.get(&key, 0),
            None,
            "a failed load never installs a value"
        );
    }

    #[tokio::test]
    async fn a_cancelled_loader_lets_a_waiter_take_over() {
        let engine = Arc::new(engine_u32_string(u64::MAX, None));
        let key = 11u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());

        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        {
            // Dropped without `complete()`, simulating a cancelled loader
            // future.
            let _guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));
        }

        assert!(
            !engine
                .stripe_lock(stripe_index_from_hash(hash))
                .read()
                .inflight
                .contains_key(kb.as_ref())
        );
        // A fresh caller becomes the new owner instead of joining a dead entry.
        match engine.miss_or_join(&kb, hash, 0) {
            JoinOutcome::Owner(_) => {}
            _ => panic!("a cancelled load's key is free for a new owner"),
        }
    }

    #[tokio::test]
    async fn a_waiter_that_starts_waiting_after_the_owner_finished_still_wakes() {
        let engine = Arc::new(engine_u32_string(u64::MAX, None));
        let key = 13u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());

        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        let JoinOutcome::Join(_, mut done) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("second caller joins the existing load");
        };
        // The owner completes before the waiter ever polls its receiver.
        let encoded = Bytes::from(postcard::to_stdvec("late").expect("encode"));
        engine.complete_fresh_load(
            &key,
            &kb,
            hash,
            hlc(1, 1),
            "late".to_string(),
            encoded,
            None,
            0,
            &inflight,
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), done.changed())
            .await
            .expect("a completion that preceded the wait is not lost")
            .expect("the owner finished the fill");
        assert_eq!(engine.get(&key, 0), Some("late".to_string()));
    }

    #[test]
    fn miss_or_join_hits_on_the_locked_recheck_after_a_concurrent_insert() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 5u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());

        assert_eq!(engine.get(&key, 0), None, "the fast-path read misses first");
        // A write lands on the key between the caller's fast-path miss and its
        // call to `miss_or_join`, e.g. a concurrent `get_or_load` owner or a
        // plain remote write.
        let _ = put(&engine, key, kb.clone(), "late".into(), hlc(1, 1), None, 0);

        match engine.miss_or_join(&kb, hash, 0) {
            JoinOutcome::Hit(v) => assert_eq!(v, "late"),
            _ => panic!("the locked re-check finds the entry that landed after the fast-path miss"),
        }
    }

    #[test]
    fn complete_fresh_load_replaces_a_tombstone_and_keeps_digest_and_weight_correct() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 21u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);

        let _ = put(&engine, key, kb.clone(), "old".into(), hlc(1, 1), None, 0);
        {
            let mut stripe = engine.stripe_lock(bucket).write();
            let resolver = LwwResolver;
            let _ = apply_locked(
                &mut stripe,
                &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
                &engine.total_weight,
                engine.weigher.as_ref(),
                engine.tti_ms,
                hash,
                key,
                kb.clone(),
                hlc(2, 1),
                Incoming::Tombstone,
                &resolver,
                60_000,
                600_000,
                0,
            );
        }
        assert_eq!(
            engine.get(&key, 0),
            None,
            "a tombstone sits where the load will land"
        );

        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("no live entry: this caller becomes the owner");
        };
        let encoded = Bytes::from(postcard::to_stdvec("fresh").expect("encode"));
        let had_live = engine.complete_fresh_load(
            &key,
            &kb,
            hash,
            hlc(3, 1),
            "fresh".to_string(),
            encoded,
            None,
            0,
            &inflight,
        );
        assert!(!had_live, "a tombstone is not a live entry");
        assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        let (entries, weight) = engine.debug_totals();
        assert_eq!(entries, 1);
        assert_eq!(weight, 1);
    }

    #[test]
    fn complete_fresh_load_replaces_a_live_entry_that_landed_during_the_load() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 22u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());

        assert_eq!(engine.get(&key, 0), None);
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };

        // A write lands on the same key while the load is in flight, e.g. a
        // replicated write racing the local loader.
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "raced-in".into(),
            hlc(5, 2),
            None,
            0,
        );
        assert_eq!(engine.get(&key, 0), Some("raced-in".to_string()));

        let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
        let had_live = engine.complete_fresh_load(
            &key,
            &kb,
            hash,
            hlc(1, 1),
            "loaded".to_string(),
            encoded,
            None,
            0,
            &inflight,
        );
        assert!(had_live, "the entry that landed during the load was live");
        assert_eq!(
            engine.get(&key, 0),
            Some("loaded".to_string()),
            "complete_fresh_load installs unconditionally, even over a racer with a newer Hlc"
        );
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        let (entries, weight) = engine.debug_totals();
        assert_eq!(entries, 1);
        assert_eq!(weight, 1);
    }

    #[test]
    fn eviction_sampling_starts_from_rotating_offsets() {
        let engine = engine_u32_string(u64::MAX, None);
        let offsets: std::collections::HashSet<usize> =
            (0..64).map(|_| engine.sample_offset(40)).collect();
        assert!(
            offsets.iter().all(|&o| o < 40),
            "an offset always lies inside the stripe"
        );
        assert!(
            offsets.iter().any(|&o| o >= EVICTION_SAMPLE),
            "sampling reaches past the first {EVICTION_SAMPLE} slots: {offsets:?}"
        );
        assert_eq!(engine.sample_offset(0), 0, "an empty stripe has one offset");
    }

    #[tokio::test]
    async fn concurrent_inserts_across_stripes_all_land() {
        let engine = Arc::new(engine_u32_string(u64::MAX, None));
        let keys: Vec<u32> = (0..64).collect();
        let mut tasks = tokio::task::JoinSet::new();
        for k in keys.clone() {
            let engine = Arc::clone(&engine);
            tasks.spawn(async move {
                let kb = key_bytes(k);
                let hash = hash_key_bytes(kb.as_ref());
                let bucket = stripe_index_from_hash(hash);
                let mut stripe = engine.stripe_lock(bucket).write();
                let resolver = LwwResolver;
                let encoded = Bytes::from(postcard::to_stdvec(&k.to_string()).expect("encode"));
                let _ = apply_locked(
                    &mut stripe,
                    &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
                    &engine.total_weight,
                    None,
                    None,
                    hash,
                    k,
                    kb,
                    hlc(u64::from(k) + 1, 1),
                    Incoming::Put {
                        value: k.to_string(),
                        expires_at_ms: None,
                        encoded,
                    },
                    &resolver,
                    60_000,
                    600_000,
                    0,
                );
            });
        }
        while let Some(r) = tasks.join_next().await {
            r.expect("spawned call does not panic");
        }
        for k in keys {
            assert_eq!(engine.get(&k, 0), Some(k.to_string()));
        }
    }

    #[test]
    fn two_different_stripes_lock_independently() {
        let engine = Arc::new(engine_u32_string(u64::MAX, None));
        let (bucket_a, bucket_b) = (0usize, 1usize);

        let held = Arc::clone(&engine);
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            let _guard = held.stripe_lock(bucket_a).write();
            tx.send(()).expect("send");
            std::thread::sleep(Duration::from_millis(150));
        });
        rx.recv().expect("lock holder signals it has the lock");

        assert!(
            engine.stripe_lock(bucket_b).try_write().is_some(),
            "a different stripe's lock is not blocked"
        );
        assert!(
            engine.stripe_lock(bucket_a).try_write().is_none(),
            "the held stripe's lock is still contended"
        );
        handle.join().expect("lock holder thread does not panic");
        assert!(
            engine.stripe_lock(bucket_a).try_write().is_some(),
            "released after the holder finishes"
        );
    }

    #[test]
    fn get_by_bytes_reads_what_get_reads() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(7);
        let hash = hash_key_bytes(kb.as_ref());
        assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), None);
        let _ = put(
            &engine,
            7,
            kb.clone(),
            "seven".into(),
            hlc(1, 1),
            Some(50),
            0,
        );
        assert_eq!(
            engine.get_by_bytes(kb.as_ref(), hash, 0),
            Some("seven".to_string())
        );
        assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), engine.get(&7, 0));
        assert_eq!(
            engine.get_by_bytes(kb.as_ref(), hash, 100),
            None,
            "an expired entry reads as absent by bytes too"
        );
    }

    #[test]
    fn collect_buckets_ignores_a_bucket_outside_the_stripe_range() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let bucket = u16::try_from(stripe_index_from_hash(hash_key_bytes(kb.as_ref())))
            .expect("bucket fits");
        let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), None, 0);

        assert!(
            engine.collect_buckets(&[u16::MAX], 0).is_empty(),
            "a bucket past BUCKET_COUNT yields nothing instead of indexing past the stripes"
        );
        let mixed = engine.collect_buckets(&[u16::MAX, bucket, 1024], 0);
        assert_eq!(mixed.len(), 1, "only the in-range bucket is answered");
        assert_eq!(mixed[0].0, bucket);
        assert_eq!(mixed[0].1, vec![(kb, hlc(1, 1))]);
    }

    #[test]
    fn a_write_over_an_expired_unswept_entry_reports_created() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), Some(50), 0);

        let replaced = put(&engine, 1, kb.clone(), "b".into(), hlc(2, 1), Some(50), 10);
        assert!(
            matches!(replaced, ApplyOutcome::Put { created: false, .. }),
            "a write over a readable entry is an update"
        );
        let over_expired = put(&engine, 1, kb.clone(), "c".into(), hlc(3, 1), Some(100), 60);
        assert!(
            matches!(over_expired, ApplyOutcome::Put { created: true, .. }),
            "a write over an entry a read no longer sees is a creation, sweep or no sweep"
        );
        // The expired entry was still displaced: one live entry, one weight.
        assert_eq!(engine.debug_totals(), (1, 1));
        assert_eq!(engine.get(&1, 60), Some("c".to_string()));
    }

    #[test]
    fn a_write_over_an_idle_entry_reports_created() {
        let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
        let kb = key_bytes(1);
        let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), None, 0);
        assert_eq!(
            engine.get(&1, 150),
            None,
            "idle past the TTI reads as absent"
        );
        let over_idle = put(&engine, 1, kb, "b".into(), hlc(2, 1), None, 150);
        assert!(matches!(over_idle, ApplyOutcome::Put { created: true, .. }));
        assert_eq!(engine.debug_totals(), (1, 1));
    }

    #[test]
    fn enforce_capacity_clears_an_overage_needing_thousands_of_evictions() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(10_000, None, Some(weigher));
        // 6,000 one-unit entries, then one 9,500-unit entry: back under the
        // cap only after more than 5,500 evictions.
        for k in 1..=6_000u32 {
            let _ = put(
                &engine,
                k,
                key_bytes(k),
                "x".into(),
                hlc(u64::from(k), 1),
                None,
                0,
            );
        }
        let big = 7_000u32;
        let big_bytes = key_bytes(big);
        let start_bucket = stripe_index_from_hash(hash_key_bytes(big_bytes.as_ref()));
        let _ = put(
            &engine,
            big,
            big_bytes,
            "y".repeat(9_500),
            hlc(10_000, 1),
            None,
            1,
        );
        assert_eq!(engine.debug_totals().1, 15_500);

        engine.enforce_capacity(start_bucket);

        let (entries, weight) = engine.debug_totals();
        assert!(
            weight <= 10_000,
            "total weight {weight} is back under the cap"
        );
        assert!(
            entries <= 501,
            "{entries} entries remain; the overage cost more than 5,499 evictions"
        );
    }

    #[test]
    fn enforce_capacity_stops_once_every_stripe_is_empty() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(5, None, Some(weigher));
        // One entry heavier than the whole cap: evicting it is all there is.
        let _ = put(&engine, 1, key_bytes(1), "z".repeat(50), hlc(1, 1), None, 0);
        engine.enforce_capacity(0);
        assert_eq!(engine.debug_totals(), (0, 0));
        assert!(
            !engine.evict_one_sampled(0),
            "an empty stripe evicts nothing"
        );
        assert_eq!(engine.evict_one_scanning(0), None);
    }

    #[test]
    fn digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions() {
        use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

        let weigher: Weigher<u32, u64> = Box::new(|_k, _v| 1);
        let engine = Engine::<u32, u64>::new(12, None, Some(weigher));
        let mut rng = StdRng::seed_from_u64(0x5EED);
        let mut clock = HlcClock::new(NodeId::from(1));

        for i in 0..300u64 {
            let key = rng.random_range(0..24u32);
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let now = i * 10;
            match rng.random_range(0..4u32) {
                0 => {
                    let ver = clock.now(now);
                    let value = u64::from(key) * 31;
                    let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                    let mut stripe = engine.stripe_lock(bucket).write();
                    let resolver = LwwResolver;
                    let _ = apply_locked(
                        &mut stripe,
                        &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
                        &engine.total_weight,
                        engine.weigher.as_ref(),
                        engine.tti_ms,
                        hash,
                        key,
                        kb,
                        ver,
                        Incoming::Put {
                            value,
                            expires_at_ms: Some(now + 500),
                            encoded,
                        },
                        &resolver,
                        1_000,
                        10_000,
                        now,
                    );
                    drop(stripe);
                    engine.enforce_capacity(bucket);
                }
                1 => {
                    let ver = clock.now(now);
                    let mut stripe = engine.stripe_lock(bucket).write();
                    let resolver = LwwResolver;
                    let _ = apply_locked(
                        &mut stripe,
                        &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
                        &engine.total_weight,
                        engine.weigher.as_ref(),
                        engine.tti_ms,
                        hash,
                        key,
                        kb,
                        ver,
                        Incoming::Tombstone,
                        &resolver,
                        1_000,
                        10_000,
                        now,
                    );
                }
                2 => engine.sweep(now),
                _ => engine.gc_tombstones(false, now),
            }
            if i % 15 == 0 {
                assert_eq!(
                    engine.digests(),
                    engine.recompute_digests_paired(),
                    "iteration {i}"
                );
                assert_eq!(
                    engine.recompute_digests(),
                    (0..BUCKET_COUNT)
                        .flat_map(|b| (0..PART_COUNT).map(move |p| (b, p)))
                        .map(|(b, p)| engine.part_digests(u16::try_from(b).expect("fits"))[p])
                        .collect::<Vec<u64>>(),
                    "iteration {i}: part digests match the full recompute, not only their \
                     bucket aggregate"
                );
            }
        }
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
    }

    trait PairedDigests {
        fn recompute_digests_paired(&self) -> Vec<(u16, u64)>;
    }
    impl<K, V> PairedDigests for Engine<K, V>
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        fn recompute_digests_paired(&self) -> Vec<(u16, u64)> {
            let parts = self.recompute_digests();
            (0..BUCKET_COUNT)
                .map(|bucket| {
                    let digest = (0..PART_COUNT)
                        .fold(0u64, |acc, part| acc ^ parts[digest_slot(bucket, part)]);
                    (u16::try_from(bucket).expect("fits"), digest)
                })
                .collect()
        }
    }

    /// The recomputed part digests as `(bucket, part, digest)` triples, paired
    /// with the bucket each flat index belongs to, for tests that check part
    /// digests directly rather than only their bucket aggregate.
    fn recompute_part_digests_paired<K, V>(engine: &Engine<K, V>) -> Vec<(u16, u8, u64)>
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let parts = engine.recompute_digests();
        (0..BUCKET_COUNT)
            .flat_map(|bucket| {
                let parts = &parts;
                (0..PART_COUNT).map(move |part| {
                    (
                        u16::try_from(bucket).expect("fits"),
                        u8::try_from(part).expect("fits"),
                        parts[digest_slot(bucket, part)],
                    )
                })
            })
            .collect()
    }

    #[test]
    fn bucket_len_counts_live_entries_and_tombstones_without_materializing_them() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 5u32;
        let kb = key_bytes(key);
        let bucket =
            u16::try_from(stripe_index_from_hash(hash_key_bytes(kb.as_ref()))).expect("fits");
        assert_eq!(engine.bucket_len(bucket), 0);
        let _ = put(&engine, key, kb, "a".into(), hlc(1, 1), None, 0);
        assert_eq!(engine.bucket_len(bucket), 1);
        assert_eq!(
            engine.bucket_len(u16::MAX),
            0,
            "a bucket past BUCKET_COUNT counts as 0, not an index panic"
        );
    }

    #[test]
    fn part_digests_xor_to_the_bucket_digest() {
        use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

        let engine = engine_u32_string(u64::MAX, None);
        let mut rng = StdRng::seed_from_u64(0xFEED_1234);
        for i in 0..200u32 {
            let ver = hlc(u64::from(i) + 1, 1);
            let _ = put(&engine, i, key_bytes(i), i.to_string(), ver, None, 0);
            let _ = rng.random_range(0..1u32);
        }
        for (bucket, digest) in engine.digests() {
            let parts = engine.part_digests(bucket);
            assert_eq!(parts.len(), PART_COUNT);
            let xored = parts.iter().fold(0u64, |acc, d| acc ^ d);
            assert_eq!(
                xored, digest,
                "bucket {bucket}'s part digests XOR to its bucket digest"
            );
        }
        assert_eq!(
            recompute_part_digests_paired(&engine)
                .into_iter()
                .map(|(bucket, part, digest)| {
                    let _ = part;
                    (bucket, digest)
                })
                .fold(std::collections::HashMap::new(), |mut acc, (b, d)| {
                    *acc.entry(b).or_insert(0u64) ^= d;
                    acc
                }),
            engine.digests().into_iter().collect(),
            "the full recompute agrees with the incrementally maintained part digests"
        );
    }

    #[test]
    fn collect_parts_returns_exactly_that_parts_entries_including_tombstones() {
        let engine = engine_u32_string(u64::MAX, None);
        // Find two keys sharing a bucket but landing in different parts, and a
        // third key in a different bucket entirely.
        let mut by_bucket_part: HashMap<(usize, usize), u32> = HashMap::new();
        let mut same_bucket_diff_part: Option<(u32, u32)> = None;
        let mut candidate = 0u32;
        while same_bucket_diff_part.is_none() {
            let kb = key_bytes(candidate);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let part = part_index_from_hash(hash);
            if let Some(&other) = by_bucket_part
                .iter()
                .find(|((b, p), _)| *b == bucket && *p != part)
                .map(|(_, k)| k)
            {
                same_bucket_diff_part = Some((other, candidate));
            }
            by_bucket_part.insert((bucket, part), candidate);
            candidate += 1;
        }
        let (key_a, key_b) = same_bucket_diff_part.expect("found within the loop above");
        let kb_a = key_bytes(key_a);
        let kb_b = key_bytes(key_b);
        let bucket = stripe_index_from_hash(hash_key_bytes(kb_a.as_ref()));
        let part_a = part_index_from_hash(hash_key_bytes(kb_a.as_ref()));
        let part_b = part_index_from_hash(hash_key_bytes(kb_b.as_ref()));

        let _ = put(&engine, key_a, kb_a.clone(), "a".into(), hlc(1, 1), None, 0);
        // key_b becomes a tombstone, still expected in its part's listing.
        let _ = put(&engine, key_b, kb_b.clone(), "b".into(), hlc(1, 1), None, 0);
        {
            let hash_b = hash_key_bytes(kb_b.as_ref());
            let mut stripe = engine.stripe_lock(bucket).write();
            let resolver = LwwResolver;
            let _ = apply_locked(
                &mut stripe,
                &engine.digest[digest_slot(bucket, part_b)],
                &engine.total_weight,
                engine.weigher.as_ref(),
                engine.tti_ms,
                hash_b,
                key_b,
                kb_b.clone(),
                hlc(2, 1),
                Incoming::Tombstone,
                &resolver,
                60_000,
                600_000,
                0,
            );
        }

        let bucket_u16 = u16::try_from(bucket).expect("fits");
        let req_a: (u16, u8) = (bucket_u16, u8::try_from(part_a).expect("fits"));
        let req_b: (u16, u8) = (bucket_u16, u8::try_from(part_b).expect("fits"));
        let result = engine.collect_parts(&[req_a, req_b], 0);
        assert_eq!(result.len(), 2);
        let a_entries = &result
            .iter()
            .find(|(key, _)| *key == req_a)
            .expect("part_a present")
            .1;
        assert_eq!(a_entries, &vec![(kb_a, hlc(1, 1))]);
        let b_entries = &result
            .iter()
            .find(|(key, _)| *key == req_b)
            .expect("part_b present")
            .1;
        assert_eq!(
            b_entries,
            &vec![(kb_b, hlc(2, 1))],
            "a tombstoned key still appears in its part's listing, at its tombstone version"
        );
    }

    #[test]
    fn collect_parts_ignores_out_of_range_bucket_or_part() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = u16::try_from(stripe_index_from_hash(hash)).expect("fits");
        let part = u8::try_from(part_index_from_hash(hash)).expect("fits");
        let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), None, 0);

        assert!(
            engine.collect_parts(&[(u16::MAX, part)], 0).is_empty(),
            "a bucket past BUCKET_COUNT yields nothing"
        );
        assert!(
            engine.collect_parts(&[(bucket, u8::MAX)], 0).is_empty(),
            "a part past PART_COUNT yields nothing"
        );
        let mixed = engine.collect_parts(&[(u16::MAX, part), (bucket, part), (bucket, u8::MAX)], 0);
        assert_eq!(
            mixed.len(),
            1,
            "only the in-range (bucket, part) is answered"
        );
        assert_eq!(mixed[0].0, (bucket, part));
        assert_eq!(mixed[0].1, vec![(kb, hlc(1, 1))]);
    }
}
