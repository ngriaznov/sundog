//! The bespoke store engine: [`BUCKET_COUNT`] independently locked stripes,
//! each stripe doubling as one anti-entropy bucket, holding both live
//! entries and tombstones under a single [`parking_lot::RwLock`]. This is
//! what replaced `moka`: a read takes one stripe's read lock, looks a key up
//! by its postcard-encoded bytes in a [`hashbrown::HashTable`] (no
//! allocation for keys that fit [`KEY_STACK_BUF`]), and clones the value —
//! well under moka's per-read overhead, with no background housekeeping
//! thread to flush. A versioned write ([`apply_locked`]) is a plain
//! synchronous function operating on `&mut Stripe`, called while holding
//! that stripe's write lock for the call's whole duration — no `.await`
//! anywhere inside it. And because a stripe *is* an anti-entropy bucket,
//! [`Engine::collect_buckets`] touches only the stripes a caller actually
//! asked about, not the whole shard.
//!
//! Capacity eviction is sampled LRU: when the shard's total live weight
//! exceeds its configured cap, [`Engine::enforce_capacity`] repeatedly takes
//! one stripe's write lock at a time (the stripe just written to, then
//! pseudo-random stripes), looks at up to 8 of that stripe's live entries,
//! and evicts whichever was least recently read — never a full LRU order,
//! never two stripe locks held at once.
//!
//! [`super::Shard::get_or_load`]'s stampede collapse is built on the
//! primitives here too: a per-stripe `inflight` map of in-progress loads,
//! joined by concurrent callers via a [`tokio::sync::Notify`], with a drop
//! guard ([`InflightGuard`]) that frees a cancelled load so a waiter can
//! take over rather than hang.

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
use tokio::sync::Notify;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::wire::WireRecord;

use super::{
    BUCKET_COUNT, BucketEntries, ConflictResolver, Incoming, RecordView, Stored, Tombstone,
    Weigher, Winner, entry_fingerprint,
};

/// Stack-buffer size for a key's postcard encoding on the read path: large
/// enough for every key type this crate ships and most user key types.
/// A key that doesn't fit falls back to one heap allocation — a read never
/// allocates for the key in the common case.
const KEY_STACK_BUF: usize = 128;

/// Holds one postcard-encoded key: on the stack when it fits
/// [`KEY_STACK_BUF`], on the heap otherwise.
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

/// The 64-bit xxh3 of a key's encoded bytes — computed once per operation
/// and reused both as the stripe index ([`stripe_index_from_hash`]) and as
/// the hash [`hashbrown::HashTable`] stores each entry under.
pub(crate) fn hash_key_bytes(key_bytes: &[u8]) -> u64 {
    xxh3_64(key_bytes)
}

/// The stripe (= anti-entropy bucket) a precomputed key hash belongs to.
pub(crate) fn stripe_index_from_hash(hash: u64) -> usize {
    usize::try_from(hash & (BUCKET_COUNT as u64 - 1))
        .expect("invariant: masked to BUCKET_COUNT - 1, always fits in usize")
}

fn hasher_for<K, V>(live: &Live<K, V>) -> u64 {
    xxh3_64(live.key_bytes.as_ref())
}

/// One live entry: the value, its version and expiry ([`Stored`] inline, no
/// `Arc`), its weight for capacity accounting, and the last time it was
/// read — written only when TTI or a finite capacity is configured, so a
/// plain unbounded, no-idle-timeout read never touches shared memory beyond
/// cloning the value.
struct Live<K, V> {
    key_bytes: Bytes,
    key: K,
    stored: Stored<V>,
    weight: u32,
    last_access_ms: AtomicU64,
}

/// One in-progress [`Engine::get_or_load`] fill, shared by every caller
/// racing on the same missing key. Carries no value: a successful fill is
/// visible to joined waiters by re-reading the stripe once notified: only a
/// failure needs to travel through here explicitly.
pub(crate) struct Inflight<V> {
    /// Wakes every joined waiter once the fill finishes, one way or another
    /// — `pub(crate)` rather than engine-private: `Shard::get_or_load` (in
    /// the parent module) waits on this directly.
    pub(crate) notify: Notify,
    /// Set iff the fill failed; a joined waiter that finds this populated
    /// after being woken returns the same [`crate::error::CacheError::Loader`]
    /// the owner did. Same cross-module visibility reason as `notify`.
    pub(crate) error: OnceLock<Arc<dyn std::error::Error + Send + Sync>>,
    _marker: PhantomData<fn() -> V>,
}

impl<V> Inflight<V> {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            error: OnceLock::new(),
            _marker: PhantomData,
        }
    }
}

/// One stripe: an anti-entropy bucket's worth of live entries, tombstones,
/// and in-flight loads, all under the one [`parking_lot::RwLock`] that owns
/// this struct.
pub(crate) struct Stripe<K, V> {
    live: HashTable<Live<K, V>>,
    tombstones: HashMap<Bytes, Tombstone>,
    inflight: HashMap<Bytes, Arc<Inflight<V>>>,
    /// The minimum `expires_at_ms` among this stripe's live entries,
    /// `u64::MAX` if none — a lower bound, not necessarily tight (a removal
    /// never tightens it; only [`Engine::sweep`] recomputes it exactly), so
    /// the sweeper can skip a stripe with nothing due without ever missing
    /// one that is.
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

/// Removes the live entry at `key_bytes` (already known to hash to `hash`)
/// if present, returning its weight and version for the caller's digest/
/// weight bookkeeping.
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

/// Whether `incoming` (at `ver`) loses to whatever is already stored (`sv`,
/// plus its encoded value/expiry when the resolver reads them) — the
/// [`ConflictResolver`]-consultation half of [`apply_locked`]'s decision,
/// factored out only for readability. See [`super::Shard::apply`]'s docs
/// (unchanged) for the correctness contract.
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

/// The outcome of [`apply_locked`], carrying the caller's `key` back (the
/// function consumed it) plus what changed, for the caller to build an
/// [`super::Event`] and decide on fan-out.
pub(crate) enum ApplyOutcome<K, V> {
    /// `incoming` lost to what was already stored; nothing changed.
    Rejected,
    /// A value was written. `created` is `false` for a value that replaced
    /// an existing live entry.
    Put { key: K, value: V, created: bool },
    /// A tombstone was written, replacing whatever (live entry or nothing)
    /// was there before — the caller's [`super::Event::Removed`] doesn't
    /// distinguish the two cases, so unlike `Put`'s `created` this carries
    /// no prior-liveness flag.
    Tombstoned { key: K },
}

/// The versioned-apply core: applies `incoming` at `ver` for `key`
/// (`key_bytes`/`hash` its postcard-encoded bytes and their xxh3 hash) iff
/// `resolver` picks it over whatever `stripe` currently holds — a live entry
/// or a tombstone, mutually exclusive — updating `digest_bucket` and
/// `total_weight` to match. Fully synchronous: the caller holds `stripe`'s
/// write lock for this call's entire duration, no `.await` anywhere inside.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_locked<K, V>(
    stripe: &mut Stripe<K, V>,
    digest_bucket: &AtomicU64,
    total_weight: &AtomicU64,
    weigher: Option<&Weigher<K, V>>,
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
    let stored_live = if prior_tombstone.is_none() {
        stripe
            .live
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes.as_ref())
            .map(|l| {
                (
                    l.stored.ver,
                    l.stored.encoded.clone(),
                    l.stored.expires_at_ms,
                )
            })
    } else {
        None
    };
    let stored_ver = prior_tombstone
        .map(|t| t.ver)
        .or_else(|| stored_live.as_ref().map(|(v, _, _)| *v));

    if let Some(sv) = stored_ver {
        let stored_encoded = stored_live.as_ref().map(|(_, enc, _)| enc.as_ref());
        let stored_expires_at_ms = stored_live.as_ref().and_then(|(_, _, e)| *e);
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
    let new_fp = entry_fingerprint(key_bytes.as_ref(), ver);

    // Subtract whatever this write displaces before adding the new
    // fingerprint in, exactly where moka's eviction listener used to run
    // for a `Replaced` cause (it never did — this bookkeeping has always
    // happened here, synchronously, on the write itself).
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

/// The `Incoming::Put` half of [`apply_locked`]'s write, factored out only
/// to keep that function under clippy's line-count lint — behavior is
/// identical to inlining it: installs the new value, corrects total weight
/// for whatever it displaced, and reports whether this created a fresh
/// entry or replaced a live one.
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
        created: !had_live,
    }
}

/// The `Incoming::Tombstone` half of [`apply_locked`]'s write, factored out
/// for the same reason as [`apply_put`]: removes the displaced live entry
/// (if any) from total weight, then records the tombstone with its two GC
/// deadlines.
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

/// The outcome of [`Engine::miss_or_join`]: a fast-path re-check hit
/// (someone else's write landed between the caller's lock-free fast path
/// and this lock acquisition), joining an already in-flight load, or this
/// call becoming the one that runs the loader.
pub(crate) enum JoinOutcome<V> {
    Hit(V),
    Join(Arc<Inflight<V>>),
    Owner(Arc<Inflight<V>>),
}

/// A drop guard that frees a cancelled [`Engine::get_or_load`] fill: if the
/// caller's future is dropped before [`InflightGuard::complete`] runs (the
/// loader was cancelled), the in-flight entry is removed and waiters are
/// notified so one of them takes over rather than hangs forever.
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
    /// Marks the fill as finished (success or failure already recorded by
    /// the caller) so the drop path becomes a no-op.
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
            self.inflight.notify.notify_waiters();
        }
    }
}

/// `Engine<K, V>` owns [`BUCKET_COUNT`] independently locked stripes — one
/// per anti-entropy bucket — plus the per-bucket XOR digests and the total
/// live weight used for sampled-LRU capacity eviction. See the module docs.
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
            digest: (0..BUCKET_COUNT)
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
        if let Some(exp) = live.stored.expires_at_ms
            && now_ms >= exp
        {
            return true;
        }
        if let Some(tti) = self.tti_ms {
            let last = live.last_access_ms.load(Ordering::Relaxed);
            if now_ms.saturating_sub(last) >= tti {
                return true;
            }
        }
        false
    }

    /// Whether a read through this engine should pay for updating
    /// `last_access_ms` — only when it would ever be consulted (TTI) or
    /// sampled (capacity eviction needs *some* recency signal to sample by).
    fn tracks_last_access(&self) -> bool {
        self.tti_ms.is_some() || self.max_capacity != u64::MAX
    }

    fn touch(&self, live: &Live<K, V>, now_ms: u64) {
        if self.tracks_last_access() {
            live.last_access_ms.store(now_ms, Ordering::Relaxed);
        }
    }

    /// Reads `key`: a stripe read lock, a hashbrown lookup by the key's
    /// postcard-encoded bytes (stack-allocated when short), and a value
    /// clone. No mutation beyond the recency touch (only when configured).
    pub(crate) fn get(&self, key: &K, now_ms: u64) -> Option<V> {
        let key_buf = encode_key_for_read(key).ok()?;
        let key_bytes = key_buf.as_slice();
        let hash = hash_key_bytes(key_bytes);
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

    /// Every live, unexpired, non-idle key. O(entries): a full pass over
    /// every stripe.
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
    /// alike — one stripe lock, no decode back to `K` needed at all.
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

    /// [`Engine::record_for`] for every requested bucket, one stripe lock
    /// each — O(bucket size) per bucket, not O(shard size), since a stripe
    /// *is* a bucket.
    pub(crate) fn collect_buckets(&self, wanted: &[u16], now_ms: u64) -> BucketEntries {
        wanted
            .iter()
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

    /// This engine's current per-bucket XOR digests, `(bucket, digest)` for
    /// all [`BUCKET_COUNT`] buckets.
    pub(crate) fn digests(&self) -> Vec<(u16, u64)> {
        self.digest
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let bucket = u16::try_from(i).expect("invariant: index < BUCKET_COUNT fits in u16");
                (bucket, d.load(Ordering::Relaxed))
            })
            .collect()
    }

    /// Every live (unexpired, non-idle) entry and every tombstone, as
    /// [`WireRecord`]s — full state, for [`super::ShardOps::snapshot_chunks`].
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

    /// Drops tombstones past `tombstone_ttl` (or, while `any_member_absent`,
    /// past the hard cap `tombstone_max_ttl`), correcting the digest.
    pub(crate) fn gc_tombstones(&self, any_member_absent: bool, now_ms: u64) {
        for (idx, stripe_lock) in self.stripes.iter().enumerate() {
            let mut stripe = stripe_lock.write();
            let digest_bucket = &self.digest[idx];
            stripe.tombstones.retain(|key_bytes, t| {
                let past_ttl = now_ms >= t.ttl_deadline_ms;
                let past_max = now_ms >= t.max_deadline_ms;
                let collect = past_ttl && (!any_member_absent || past_max);
                if collect {
                    digest_bucket.fetch_xor(entry_fingerprint(key_bytes, t.ver), Ordering::Relaxed);
                }
                !collect
            });
        }
    }

    /// The free-running housekeeping moka used to do implicitly: visits
    /// every stripe whose `next_expiry_ms` is due (every stripe at all, if
    /// TTI is configured, since idling isn't reflected there), removes
    /// expired/idle live entries, corrects the digest and total weight, and
    /// recomputes that stripe's `next_expiry_ms` exactly.
    pub(crate) fn sweep(&self, now_ms: u64) {
        for (idx, stripe_lock) in self.stripes.iter().enumerate() {
            let due = stripe_lock.read().next_expiry_ms <= now_ms;
            if !due && self.tti_ms.is_none() {
                continue;
            }
            let mut stripe = stripe_lock.write();
            let digest_bucket = &self.digest[idx];
            let mut removed_weight = 0u64;
            let mut new_next = u64::MAX;
            stripe.live.retain(|live| {
                if self.is_absent(live, now_ms) {
                    digest_bucket.fetch_xor(
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

    /// The number of live entries across every stripe. O(`BUCKET_COUNT`)
    /// lock acquisitions, not O(entries).
    pub(crate) fn live_entry_count(&self) -> u64 {
        self.stripes
            .iter()
            .map(|s| u64::try_from(s.read().live.len()).unwrap_or(u64::MAX))
            .fold(0u64, u64::saturating_add)
    }

    fn next_pseudo_random_bucket(&self) -> usize {
        // xorshift64*: fast, allocation-free, plenty uniform for sampling
        // which stripe to look at next — no correctness property depends on
        // its output, only that it spreads eviction pressure around.
        let mut x = self.evict_cursor.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.evict_cursor.store(x, Ordering::Relaxed);
        stripe_index_from_hash(x)
    }

    fn evict_one_sampled(&self, bucket: usize) {
        let mut stripe = self.stripes[bucket].write();
        let Some(victim_bytes) = stripe
            .live
            .iter()
            .take(8)
            .min_by_key(|live| live.last_access_ms.load(Ordering::Relaxed))
            .map(|live| live.key_bytes.clone())
        else {
            return;
        };
        let hash = hash_key_bytes(victim_bytes.as_ref());
        if let Entry::Occupied(occ) = stripe.live.entry(
            hash,
            |l| l.key_bytes.as_ref() == victim_bytes.as_ref(),
            hasher_for,
        ) {
            let (removed, _vacant) = occ.remove();
            self.digest[bucket].fetch_xor(
                entry_fingerprint(&removed.key_bytes, removed.stored.ver),
                Ordering::Relaxed,
            );
            self.total_weight
                .fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
        }
    }

    /// After a write to `start_bucket` may have pushed total weight over
    /// `max_capacity`, evicts sampled-cold entries — starting at
    /// `start_bucket`, then pseudo-random stripes — until it no longer is.
    /// Never holds two stripe locks at once; a no-op when `max_capacity` is
    /// [`u64::MAX`] (unbounded).
    pub(crate) fn enforce_capacity(&self, start_bucket: usize) {
        if self.max_capacity == u64::MAX {
            return;
        }
        let mut bucket = start_bucket;
        // Bounded rather than an unconditional `loop`: a pathological
        // configuration (every stripe sampled empty, e.g. capacity set
        // below the weight of entries this call itself cannot see yet)
        // must not spin forever.
        for attempt in 0..BUCKET_COUNT.saturating_mul(4) {
            if self.total_weight.load(Ordering::Relaxed) <= self.max_capacity {
                return;
            }
            if attempt > 0 {
                bucket = self.next_pseudo_random_bucket();
            }
            self.evict_one_sampled(bucket);
        }
    }

    /// Applies a batch of versioned writes that all hash into `bucket`,
    /// under one write-lock acquisition for the whole group — the
    /// "amortized lock path" [`super::Shard::insert_many`],
    /// [`super::Shard::remove_many`], and
    /// [`super::ShardOps::apply_remote_batch`] all want, matching how the
    /// old per-stripe `tokio::sync::Mutex` was held across a group. Runs
    /// [`Self::enforce_capacity`] once afterward, outside the write lock, iff
    /// the batch put anything (a batch of pure removals can only shrink
    /// total weight, never grow it, so there is nothing to enforce).
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
            let digest_bucket = &self.digest[bucket];
            for (hash, key, key_bytes, ver, incoming) in entries {
                let outcome = apply_locked(
                    &mut stripe,
                    digest_bucket,
                    &self.total_weight,
                    self.weigher.as_ref(),
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

    /// Applies an inbound [`super::ShardOps::invalidate`]: drops the live
    /// entry at `key_bytes` iff `ver` is newer than whatever version is
    /// currently stored there (live or tombstoned) — writes no tombstone of
    /// its own, since an invalidation carries no value for a
    /// [`ConflictResolver`] to arbitrate with, only an `Hlc` to compare.
    /// Returns the departing version on an actual removal, for the caller to
    /// build an [`super::Event::Removed`]; `None` both when nothing was
    /// stored and when the stored version already won.
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
        self.digest[bucket].fetch_xor(entry_fingerprint(key_bytes, old_ver), Ordering::Relaxed);
        self.total_weight
            .fetch_sub(u64::from(weight), Ordering::Relaxed);
        Some(old_ver)
    }

    /// Drops the local live entry at `key_bytes` unconditionally — no
    /// version check, no tombstone written — for
    /// [`super::Shard::invalidate_local`]'s cache-busting escape hatch.
    pub(crate) fn invalidate_local(&self, key_bytes: &[u8], hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some((weight, ver)) = remove_live(&mut stripe.live, hash, key_bytes) {
            drop(stripe);
            self.digest[bucket].fetch_xor(entry_fingerprint(key_bytes, ver), Ordering::Relaxed);
            self.total_weight
                .fetch_sub(u64::from(weight), Ordering::Relaxed);
        }
    }

    /// The lock-protected first half of [`super::Shard::get_or_load`]: a
    /// fast-path re-check (in case a write landed between the caller's
    /// lock-free [`Engine::get`] and this call), then either joining an
    /// already in-flight load or registering this call as the new owner.
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
            return JoinOutcome::Join(Arc::clone(existing));
        }
        let inflight = Arc::new(Inflight::new());
        stripe
            .inflight
            .insert(key_bytes.clone(), Arc::clone(&inflight));
        JoinOutcome::Owner(inflight)
    }

    /// Builds the [`InflightGuard`] that frees `inflight` if the loader
    /// future this call is about to run is dropped before finishing.
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

    /// Removes `key_bytes`'s in-flight entry from its stripe. Setting an
    /// error (on failure) or nothing (on cancellation) is the caller's job,
    /// against the `Inflight` handle it already holds — by the time this
    /// returns, the map no longer holds that handle at all.
    fn finish_inflight(&self, key_bytes: &Bytes, hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        stripe.inflight.remove(key_bytes.as_ref());
    }

    /// Applies a successful [`super::Shard::get_or_load`] fill: removes any
    /// prior tombstone or (in the race window while the loader ran with no
    /// lock held) live entry for `key`, unconditionally installs the
    /// loader's value — a fresh load always wins, there is no competing
    /// remote version to arbitrate — and removes the now-finished
    /// `inflight` entry, all under one stripe write-lock acquisition.
    /// Returns whether a live entry already existed (a genuine miss should
    /// see `false`).
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
        let had_live = {
            let mut stripe = self.stripes[bucket].write();
            stripe.inflight.remove(key_bytes.as_ref());
            let digest_bucket = &self.digest[bucket];
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
        inflight.notify.notify_waiters();
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
        inflight.notify.notify_waiters();
    }
}

/// One live entry as [`Engine::debug_snapshot`] reports it: key bytes, its
/// encoded value, and its version.
#[cfg(test)]
type DebugLive = (Bytes, Bytes, Hlc);

/// One tombstone as [`Engine::debug_snapshot`] reports it: key bytes and its
/// version.
#[cfg(test)]
type DebugTombstone = (Bytes, Hlc);

#[cfg(test)]
impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Test-only: recomputes all `BUCKET_COUNT` digests from scratch by
    /// scanning every stripe's live entries and tombstones, to check
    /// against the incrementally maintained digest.
    pub(crate) fn recompute_digests(&self) -> Vec<u64> {
        let mut out = vec![0u64; BUCKET_COUNT];
        for (idx, stripe_lock) in self.stripes.iter().enumerate() {
            let stripe = stripe_lock.read();
            for live in &stripe.live {
                out[idx] ^= entry_fingerprint(&live.key_bytes, live.stored.ver);
            }
            for (key_bytes, t) in &stripe.tombstones {
                out[idx] ^= entry_fingerprint(key_bytes, t.ver);
            }
        }
        out
    }

    /// Test-only: every stripe's raw contents, for building a canonical
    /// state to compare across replicas.
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

    /// Test-only: total live entries and current total weight, for capacity
    /// eviction tests.
    pub(crate) fn debug_totals(&self) -> (u64, u64) {
        (
            self.live_entry_count(),
            self.total_weight.load(Ordering::Relaxed),
        )
    }

    /// Test-only: forces the tombstone at `key_bytes` (if any) to read as
    /// past its `ttl_deadline_ms`, and past `max_deadline_ms` too iff
    /// `past_max` — for tests that drive [`Self::gc_tombstones`]'s
    /// deferral/hard-cap logic without waiting on a real `tombstone_ttl`.
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

    /// Test-only direct access to one stripe's lock, for tests that must
    /// prove stripe independence at the lock level (a raw
    /// [`parking_lot::RwLock`] doesn't yield to an async runtime the way the
    /// old per-stripe `tokio::sync::Mutex` did, so that property is now
    /// exercised directly here rather than through `Shard::insert`).
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
            &engine.digest[bucket],
            &engine.total_weight,
            engine.weigher.as_ref(),
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

        // Sweeping well before the deadline must not touch this stripe.
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
        // Sampling only ever looks within one stripe at a time (never a
        // global LRU order — see `Engine::enforce_capacity`'s docs), so
        // "coldest first" is only a guaranteed property *within* a bucket.
        // Find several keys that land in the same bucket so a single
        // sampling pass sees all of them at once.
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
        // Five 5-unit entries under a 20-unit cap: exactly one must go.
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
            "total weight {weight_after} must stay within the 20-unit cap"
        );
        assert_eq!(
            entries_after, 4,
            "exactly one 5-unit entry must be evicted to clear a 5-unit overage"
        );
        assert_eq!(
            engine.get(&same_bucket_keys[0], 1_000),
            None,
            "the coldest (first-inserted) entry must be the one evicted"
        );
        for &k in &same_bucket_keys[1..] {
            assert!(
                engine.get(&k, 1_000).is_some(),
                "hotter entries must survive"
            );
        }
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
                        JoinOutcome::Join(inflight) => {
                            let notified = inflight.notify.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            notified.await;
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
                            assert!(!had_live, "a genuine miss must not see a prior live entry");
                            guard.complete();
                            return value;
                        }
                    }
                }
            });
        }
        let mut results = Vec::with_capacity(CONCURRENCY);
        while let Some(r) = tasks.join_next().await {
            results.push(r.expect("task does not panic"));
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
            panic!("first caller must become the owner");
        };
        let guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));

        // A second caller joins the same in-flight load and registers for a
        // wakeup (`enable()`) synchronously, before anything can notify it —
        // exactly as `Shard::get_or_load`'s real `Join` arm does, before its
        // first `.await`. Doing this inline rather than inside a spawned
        // task matters: `Notify::notify_waiters` only wakes waiters already
        // registered at the moment it's called, so a registration deferred
        // into a not-yet-polled spawned task would race `fail_inflight`
        // below and hang forever.
        let JoinOutcome::Join(joined) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("second caller must join the existing load");
        };
        let notified = joined.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let boom: Arc<dyn std::error::Error + Send + Sync> =
            Arc::new(std::io::Error::other("boom"));
        engine.fail_inflight(&kb, hash, &inflight, boom);
        guard.complete();

        notified.await;
        assert!(
            joined.error.get().is_some(),
            "the joined waiter must see the error"
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
            panic!("first caller must become the owner");
        };
        {
            // The guard is dropped here without `complete()` — simulating the
            // loader future being cancelled before it finished.
            let _guard = engine.guard_inflight(kb.clone(), hash, Arc::clone(&inflight));
        }

        // A waiter registered before cancellation must be woken...
        assert!(
            !engine
                .stripe_lock(stripe_index_from_hash(hash))
                .read()
                .inflight
                .contains_key(kb.as_ref())
        );
        // ...and a fresh caller must be able to become the new owner rather
        // than perpetually join a dead entry.
        match engine.miss_or_join(&kb, hash, 0) {
            JoinOutcome::Owner(_) => {}
            _ => panic!("a cancelled load's key must be free for a new owner"),
        }
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
                    &engine.digest[bucket],
                    &engine.total_weight,
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
            r.expect("task does not panic");
        }
        for k in keys {
            assert_eq!(engine.get(&k, 0), Some(k.to_string()));
        }
    }

    #[test]
    fn two_different_stripes_lock_independently() {
        let engine = Arc::new(engine_u32_string(u64::MAX, None));
        // Two buckets guaranteed distinct.
        let (bucket_a, bucket_b) = (0usize, 1usize);

        let held = Arc::clone(&engine);
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            let _guard = held.stripe_lock(bucket_a).write();
            tx.send(()).expect("send");
            std::thread::sleep(Duration::from_millis(150));
        });
        rx.recv().expect("lock holder signals it has the lock");

        // A different stripe's lock must be free immediately.
        assert!(
            engine.stripe_lock(bucket_b).try_write().is_some(),
            "a different stripe's lock must not be blocked"
        );
        // The held stripe's lock must not be free yet.
        assert!(
            engine.stripe_lock(bucket_a).try_write().is_none(),
            "the held stripe's lock must still be contended"
        );
        handle.join().expect("lock holder thread does not panic");
        assert!(
            engine.stripe_lock(bucket_a).try_write().is_some(),
            "released after the holder finishes"
        );
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
                        &engine.digest[bucket],
                        &engine.total_weight,
                        engine.weigher.as_ref(),
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
                        &engine.digest[bucket],
                        &engine.total_weight,
                        engine.weigher.as_ref(),
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
            self.recompute_digests()
                .into_iter()
                .enumerate()
                .map(|(i, d)| (u16::try_from(i).expect("fits"), d))
                .collect()
        }
    }
}
