//! The store engine: [`BUCKET_COUNT`] stripes, each one anti-entropy bucket,
//! each a [`parking_lot::RwLock`] over the bucket's live entries and
//! tombstones.
//!
//! A read takes a stripe's read lock, finds the key by its postcard bytes in
//! a [`hashbrown::HashTable`], and decodes the record's value half. Keys up
//! to `KEY_STACK_BUF` bytes encode on the stack. A versioned write
//! (`apply_locked`) runs synchronously under the stripe's write lock.
//! Anti-entropy enumerates a bucket by locking one stripe.
//!
//! Expiry is checked on every read and reclaimed by [`Engine::sweep`], which
//! visits only stripes with an entry due. Capacity eviction is sampled LRU:
//! [`Engine::enforce_capacity`] locks one stripe at a time and evicts the
//! coldest of a sampled batch under that one lock hold, until total weight
//! fits under `max_capacity`. [`Engine::live_entry_count`] is a counter
//! every insert and remove path maintains.
//!
//! [`super::Shard::get_or_load`] collapses concurrent misses through a
//! per-stripe map of in-flight loads: a waiter subscribes to the load's
//! completion channel under the stripe lock, so no completion can slip
//! between a lookup and a wait. `InflightGuard` frees a cancelled load so a
//! waiter takes over.
//!
//! Each [`Live`] entry holds its record in a [`Record`]: inline up to
//! `INLINE_CAP` bytes, boxed on the heap beyond that. Its version is
//! flattened across `wall_ms`/`node`/`logical` instead of one `Hlc` field;
//! `Live::ver`/`Live::set_ver` reassemble and overwrite it as a whole.
//!
//! Under `feature = "spill"`, a live entry can move to disk instead of
//! being evicted; see [`super::spill::SpillTier`] for that mechanism.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Deref;
#[cfg(all(feature = "spill", test))]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use hashbrown::HashTable;
use parking_lot::RwLock;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::wire::WireRecord;

use super::crdt;
use super::part::PartSet;
#[cfg(feature = "spill")]
pub(crate) use super::spill::Reservation;
#[cfg(feature = "spill")]
use super::spill::{
    Admission, SpillJob, SpillLoc, SpillSink, SpillTier, spill_record_len, spilled_is_current,
};
use super::{
    BUCKET_COUNT, BucketEntries, BucketPart, ConflictResolver, Incoming, KeyVersion, Merged,
    PART_COUNT, PartEntries, PartId, Quiescence, RecordView, Tombstone, Weigher, Winner,
    entry_fingerprint,
};

/// A never-constructed placeholder for
/// [`spill::Reservation`](super::spill::Reservation), so both builds share
/// one eviction-chain signature.
#[cfg(not(feature = "spill"))]
#[allow(
    dead_code,
    reason = "never constructed on this build; see the doc comment above"
)]
pub(crate) struct Reservation<'a>(PhantomData<&'a ()>);

/// Stack-buffer size for a key's postcard encoding on the read path, large
/// enough for every key type this crate ships and most user key types. A key
/// that doesn't fit falls back to one heap allocation.
const KEY_STACK_BUF: usize = 128;

/// How many live entries one capacity-eviction pass weighs before evicting the
/// least recently read of them.
const EVICTION_SAMPLE: usize = 8;

/// How many entries one [`Engine::enforce_capacity`] lock hold samples, and
/// at most how many of the coldest it evicts.
const EVICTION_BATCH_SAMPLE: usize = 32;
const EVICTION_BATCH: usize = 8;

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

/// One record [`Engine::compact`]'s scan found a compacted form for.
pub(crate) struct CompactCandidate<K> {
    pub(crate) key: K,
    pub(crate) key_bytes: Bytes,
    pub(crate) stale_ver: Hlc,
    pub(crate) new_encoded: Bytes,
}

/// One [`Engine::compact`] call's result: the records whose resolver
/// produced a compacted form, each with the version it is read at, and
/// how many stripes the call walked before its entry budget ran out. A
/// caller that wants the whole keyspace examined keeps calling until the
/// visited stripes add up to [`BUCKET_COUNT`](crate::store::BUCKET_COUNT).
pub(crate) struct CompactScan<K> {
    pub(crate) candidates: Vec<CompactCandidate<K>>,
    pub(crate) stripes_visited: usize,
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
    xxh3_64(record_key(&live.record))
}

/// Sentinel [`Live::expiry`] marking "never expires".
const NEVER_EXPIRES: u32 = u32::MAX;

/// Sentinel [`Live::expiry`] for a deadline past [`MAX_INLINE_TTL_MS`]
/// above `wall_ms`; the real deadline then lives in `Stripe::long_ttl`.
const LONG_TTL: u32 = u32::MAX - 1;

/// The largest inline delta [`encode_expiry`] stores above `wall_ms`
/// (`u32::MAX - 2`, ~49.71 days); past this, [`LONG_TTL`] applies instead.
const MAX_INLINE_TTL_MS: u64 = (u32::MAX - 2) as u64;

/// Encodes a write's `expires_at_ms` relative to `wall_ms`, for
/// [`Live::expiry`]: [`NEVER_EXPIRES`] for `None`, an exact delta within
/// [`MAX_INLINE_TTL_MS`], or [`LONG_TTL`] with the deadline as the pair's
/// `Some` second element, stored in `Stripe::long_ttl`.
fn encode_expiry(wall_ms: u64, expires_at_ms: Option<u64>) -> (u32, Option<u64>) {
    match expires_at_ms {
        None => (NEVER_EXPIRES, None),
        Some(exp) => match exp.checked_sub(wall_ms) {
            // `exp < wall_ms` happens (a dead-on-arrival TTL, or a remote
            // write predating its own wall_ms); `expiry` only ever adds to
            // `wall_ms`, so this always takes the exact side-table path.
            Some(delta) if delta <= MAX_INLINE_TTL_MS => (
                u32::try_from(delta).expect("delta <= MAX_INLINE_TTL_MS < u32::MAX fits in a u32"),
                None,
            ),
            _ => (LONG_TTL, Some(exp)),
        },
    }
}

/// The sole reader of a stored [`Live::expiry`]: `None`, `long_ttl`'s
/// value, or `wall_ms` plus the inline delta, per the sentinel.
fn decode_expiry(
    wall_ms: u64,
    expiry: u32,
    key_bytes: &[u8],
    long_ttl: &HashMap<Bytes, u64>,
) -> Option<u64> {
    match expiry {
        NEVER_EXPIRES => None,
        LONG_TTL => long_ttl.get(key_bytes).copied(),
        delta => Some(wall_ms + u64::from(delta)),
    }
}

/// Truncates `now_ms` to [`Live::last_access_ms`]'s stored form (the low 32
/// bits); [`idle_elapsed_ms`] recovers the true gap from this alone.
#[allow(
    clippy::cast_possible_truncation,
    reason = "the truncation is the point: idle_elapsed_ms recovers the true elapsed \
              duration from the low 32 bits alone via a wrapping subtraction"
)]
fn touch_stamp(now_ms: u64) -> u32 {
    now_ms as u32
}

/// Milliseconds since an entry was last touched at
/// [`touch_stamp`]`(earlier now)`, correct as long as the true gap is under
/// ~49.71 days, the ceiling [`clamp_tti_ms`] enforces.
#[allow(
    clippy::cast_possible_truncation,
    reason = "matches touch_stamp's own truncation of now_ms before the wrapping subtraction"
)]
fn idle_elapsed_ms(now_ms: u64, last_access_ms: u32) -> u64 {
    u64::from((now_ms as u32).wrapping_sub(last_access_ms))
}

/// Clamps a configured time-to-idle to [`MAX_INLINE_TTL_MS`], past which
/// [`idle_elapsed_ms`] never fires and the entry stays readable forever.
fn clamp_tti_ms(tti_ms: u64) -> u64 {
    tti_ms.min(MAX_INLINE_TTL_MS)
}

/// Max inline payload length for [`Record`], which sits at exactly 24
/// bytes either way, pinned by `record_size_is_24_bytes`.
const INLINE_CAP: usize = 22;

/// A live entry's raw `LEN ++ KEY ++ VALUE` record bytes: inline for
/// records up to [`INLINE_CAP`] bytes (a `Cache<String, String>` record
/// always lands here, with zero heap allocations), an owned `Box<[u8]>`
/// beyond that. Never shared and never `Clone`: a caller needing an owned
/// copy builds a fresh `Bytes::copy_from_slice` from
/// [`record_key_bytes`]/[`record_value_bytes`] instead.
enum Record {
    /// Inline payload: the first `len` bytes of `buf` are the record.
    Inline { len: u8, buf: [u8; INLINE_CAP] },
    /// Heap payload for a record over [`INLINE_CAP`] bytes.
    Heap(Box<[u8]>),
}

impl Record {
    /// Builds a `Record` holding a copy of `bytes`: inline when
    /// `bytes.len() <= INLINE_CAP`, a fresh boxed copy otherwise.
    fn from_slice(bytes: &[u8]) -> Record {
        if bytes.len() <= INLINE_CAP {
            let mut buf = [0u8; INLINE_CAP];
            buf[..bytes.len()].copy_from_slice(bytes);
            let len = u8::try_from(bytes.len()).expect("bytes.len() <= INLINE_CAP fits in a u8");
            Record::Inline { len, buf }
        } else {
            Record::Heap(Box::from(bytes))
        }
    }

    /// The record's bytes, from whichever variant holds them.
    fn as_slice(&self) -> &[u8] {
        match self {
            Record::Inline { len, buf } => &buf[..*len as usize],
            Record::Heap(boxed) => boxed.as_ref(),
        }
    }
}

impl Deref for Record {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for Record {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(test)]
mod record_tests {
    use super::*;

    #[test]
    fn record_size_is_24_bytes() {
        assert_eq!(std::mem::size_of::<Record>(), 24);
    }

    #[test]
    fn record_round_trips_inline_at_0_18_and_22_bytes() {
        for len in [0usize, 18, 22] {
            let bytes = vec![b'x'; len];
            let record = Record::from_slice(&bytes);
            assert!(
                matches!(record, Record::Inline { .. }),
                "len = {len} should stay inline"
            );
            assert_eq!(record.as_slice(), bytes.as_slice(), "len = {len}");
        }
    }

    #[test]
    fn record_round_trips_heap_at_23_and_300_bytes() {
        for len in [23usize, 300] {
            let bytes = vec![b'y'; len];
            let record = Record::from_slice(&bytes);
            assert!(
                matches!(record, Record::Heap(_)),
                "len = {len} should go to the heap"
            );
            assert_eq!(record.as_slice(), bytes.as_slice(), "len = {len}");
        }
    }

    #[test]
    fn record_as_slice_and_deref_agree() {
        for bytes in [&b""[..], &b"short"[..], &vec![b'z'; 100][..]] {
            let record = Record::from_slice(bytes);
            assert_eq!(record.as_slice(), bytes);
            assert_eq!(&*record, bytes, "Deref must match as_slice");
            assert_eq!(record.as_ref(), bytes, "AsRef must match as_slice");
        }
    }
}

/// Builds a resident record: `LEN ++ KEY ++ VALUE`, where `LEN` is
/// `key_bytes.len()` postcard-encoded (1 byte under 128 bytes).
fn build_resident_record(key_bytes: &[u8], value_bytes: &[u8]) -> Record {
    let key_len = u32::try_from(key_bytes.len()).expect("a key's postcard encoding fits in u32");
    let len_prefix = postcard::to_stdvec(&key_len).expect("a u32 always encodes");
    let mut buf = Vec::with_capacity(len_prefix.len() + key_bytes.len() + value_bytes.len());
    buf.extend_from_slice(&len_prefix);
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(value_bytes);
    Record::from_slice(&buf)
}

/// Builds a spilled record: `LEN ++ KEY` only, always a fresh allocation
/// rather than a slice of the old record (which would keep its value
/// bytes resident).
#[cfg_attr(not(any(test, feature = "spill")), allow(dead_code))]
fn build_key_only_record(key_bytes: &[u8]) -> Record {
    build_resident_record(key_bytes, &[])
}

/// Splits a record (`LEN ++ KEY ++ VALUE`) into its `(KEY, VALUE)` halves,
/// zero-copy.
fn split_record(record: &[u8]) -> (&[u8], &[u8]) {
    let (key_len, rest): (u32, &[u8]) = postcard::take_from_bytes(record)
        .expect("record's LEN prefix is this engine's own postcard output");
    let key_len = key_len as usize;
    (&rest[..key_len], &rest[key_len..])
}

/// `record`'s key half, `split_record(record).0`.
fn record_key(record: &[u8]) -> &[u8] {
    split_record(record).0
}

/// `record`'s value half, `split_record(record).1`, zero-copy: the value
/// counterpart to [`record_key`], for a decode that never crosses the lock.
fn record_value(record: &[u8]) -> &[u8] {
    split_record(record).1
}

/// `record`'s key half as an owned `Bytes`, copied out: [`Record`] holds
/// no shared backing to slice into, so this always allocates.
fn record_key_bytes(record: &Record) -> Bytes {
    Bytes::copy_from_slice(record_key(record))
}

/// `record`'s value half as an independently owned `Bytes`, copied out the
/// same way as [`record_key_bytes`]. Empty for a spilled record.
fn record_value_bytes(record: &Record) -> Bytes {
    Bytes::copy_from_slice(record_value(record))
}

/// Decodes `key_bytes` back into `K`. Panics on a decode failure: a live
/// key reading as absent is a worse lie than a loud panic.
fn decode_key<K: DeserializeOwned>(key_bytes: &[u8]) -> K {
    postcard::from_bytes(key_bytes).expect("key_bytes is this engine's own postcard output")
}

/// Decodes `value_bytes` back into `V`, mirroring [`decode_key`].
fn decode_value<V: DeserializeOwned>(value_bytes: &[u8]) -> V {
    postcard::from_bytes(value_bytes).expect("value_bytes is this engine's own postcard output")
}

/// One live entry. Stores only `record`'s raw postcard bytes, never a
/// decoded key or value; a reader decodes on demand
/// ([`record_key`]/[`record_value`]/[`record_value_bytes`]/[`decode_key`]/
/// [`decode_value`]). Its version is flattened across `wall_ms`/`node`/
/// `logical` rather than one `Hlc`; [`Live::ver`]/[`Live::set_ver`]
/// reassemble it. `K`/`V` are phantom-only.
struct Live<K, V> {
    /// `key || value`, framed per [`build_resident_record`]. A spilled
    /// entry (`feature = "spill"`) holds the key half alone.
    record: Record,
    /// This write's version, flattened: [`Hlc::wall_ms`], the primary
    /// ordering key.
    wall_ms: u64,
    /// This write's version, flattened: [`Hlc::node`], the final tiebreak.
    /// Kept as the full [`NodeId`] for `NodeId::merge_derived`'s determinism.
    node: NodeId,
    /// This entry's expiry, flattened above `wall_ms`. Written via
    /// [`encode_expiry`], read via [`decode_expiry`].
    expiry: u32,
    /// This write's version, flattened: [`Hlc::logical`], packed beside
    /// `weight` instead of paying `Hlc`'s own tail padding a second time.
    logical: u32,
    /// Capacity-accounting weight: `1` by default, a caller value under a
    /// `Weigher`, `0` while a spill hand-off for this entry is in flight.
    weight: u32,
    /// [`touch_stamp`] of this entry's last read, read only through
    /// [`idle_elapsed_ms`] (a rollover confuses a raw comparison).
    last_access_ms: AtomicU32,
    /// Whether `record` holds a value or points at disk with none. Absent
    /// in a non-`spill` build: every entry is implicitly resident.
    #[cfg(feature = "spill")]
    state: EntryState,
    /// Zero-sized; carries `Live`'s variance over `K`/`V` since no field
    /// is `K`- or `V`-typed.
    _marker: PhantomData<fn(&K, &V)>,
}

impl<K, V> Live<K, V> {
    /// Reassembles this entry's version from its flattened fields.
    fn ver(&self) -> Hlc {
        Hlc {
            wall_ms: self.wall_ms,
            logical: self.logical,
            node: self.node,
        }
    }

    /// Overwrites this entry's version, splitting `v` back into the
    /// flattened `wall_ms`/`node`/`logical` fields.
    fn set_ver(&mut self, v: Hlc) {
        self.wall_ms = v.wall_ms;
        self.logical = v.logical;
        self.node = v.node;
    }

    /// Builds a `Live` entry, splitting `ver` into its flattened fields and
    /// storing `last_access_ms` via [`touch_stamp`].
    #[allow(clippy::too_many_arguments)]
    fn new(
        record: Record,
        ver: Hlc,
        expiry: u32,
        weight: u32,
        last_access_ms: u64,
        #[cfg(feature = "spill")] state: EntryState,
    ) -> Self {
        Self {
            record,
            wall_ms: ver.wall_ms,
            node: ver.node,
            expiry,
            logical: ver.logical,
            weight,
            last_access_ms: AtomicU32::new(touch_stamp(last_access_ms)),
            #[cfg(feature = "spill")]
            state,
            _marker: PhantomData,
        }
    }
}

/// [`Live::record`]'s residency.
#[cfg(feature = "spill")]
enum EntryState {
    /// `record` is `[key_len][key][value]`; the value is in RAM.
    Resident,
    /// `record` is `[key_len][key]` only, a fresh copy of the key half,
    /// never a `.slice()` of the old record.
    Spilled(SpillLoc),
}

/// Whether `live`'s record currently holds a value in RAM. Always `true`
/// in a non-`spill` build.
fn is_resident<K, V>(live: &Live<K, V>) -> bool {
    #[cfg(feature = "spill")]
    {
        matches!(live.state, EntryState::Resident)
    }
    #[cfg(not(feature = "spill"))]
    {
        let _ = live;
        true
    }
}

/// Whether `live` is eligible to be sampled as an eviction victim:
/// [`is_resident`] and its weight is not already zeroed by a hand-off to a
/// spill tier still in flight. A `Resident` entry at weight `0` is such a
/// hand-off, awaiting the flusher's `install`, and is never picked a
/// second time while pending. Pure; unit tested directly.
fn is_spill_candidate<K, V>(live: &Live<K, V>) -> bool {
    is_resident(live) && live.weight > 0
}

/// Whether `live`'s record is currently spilled: the mirror of
/// [`is_resident`]. Always `false` in a non-`spill` build.
fn is_spilled<K, V>(live: &Live<K, V>) -> bool {
    #[cfg(feature = "spill")]
    {
        matches!(live.state, EntryState::Spilled(_))
    }
    #[cfg(not(feature = "spill"))]
    {
        let _ = live;
        false
    }
}

/// A currently-spilled entry's pointer: key bytes, version, expiry, and
/// the disk location, the bits an off-lock disk read and, where
/// applicable, a `WireRecord` need. Reported to a spill-aware caller by
/// [`Engine::snapshot_spilled`] and [`Engine::records_for_or_spilled`].
#[cfg(feature = "spill")]
pub(crate) type SpilledPointer = (Bytes, Hlc, Option<u64>, SpillLoc);

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
    live: Slab<K, V>,
    tombstones: HashMap<Bytes, Tombstone>,
    inflight: HashMap<Bytes, Arc<Inflight<V>>>,
    /// The absolute `expires_at_ms` for every live entry whose
    /// [`Live::expiry`] holds [`LONG_TTL`].
    long_ttl: HashMap<Bytes, u64>,
    /// The minimum `expires_at_ms` among this stripe's live entries, `u64::MAX`
    /// if none. A lower bound, not necessarily tight, since only
    /// [`Engine::sweep`] recomputes it exactly.
    next_expiry_ms: u64,
}

impl<K, V> Stripe<K, V> {
    fn new() -> Self {
        Self {
            live: Slab::new(),
            tombstones: HashMap::new(),
            inflight: HashMap::new(),
            long_ttl: HashMap::new(),
            next_expiry_ms: u64::MAX,
        }
    }

    /// A stripe whose `live` slab presizes its arena and index for
    /// `capacity` entries.
    fn with_capacity(capacity: usize) -> Self {
        Self {
            live: Slab::with_capacity(capacity),
            tombstones: HashMap::new(),
            inflight: HashMap::new(),
            long_ttl: HashMap::new(),
            next_expiry_ms: u64::MAX,
        }
    }
}

/// How many slots a full arena of `len` entries grows by: a quarter of
/// `len`, and never fewer than [`ARENA_MIN_GROWTH`]. `Vec`'s own doubling
/// leaves a stripe that just outgrew its arena half empty, and a
/// [`Live`] slot is the largest per-entry cost after the record itself;
/// growing by a quarter keeps the arena at least four fifths full while
/// each entry is still copied only a bounded number of times on average.
fn arena_growth(len: usize) -> usize {
    (len / 4).max(ARENA_MIN_GROWTH)
}

/// The fewest slots [`arena_growth`] adds, so a small stripe does not
/// reallocate on every insert.
const ARENA_MIN_GROWTH: usize = 4;

/// One stripe's live entries: a dense arena plus a hash index mapping each
/// live key's hash to its arena slot. No holes, no free list:
/// `entries.len()` is always the exact live count, so a full-stripe walker
/// iterates `entries` directly with no liveness check. [`Stripe::live`]'s
/// storage.
struct Slab<K, V> {
    /// The arena: one slot per live entry, indexed by `u32`.
    entries: Vec<Live<K, V>>,
    /// `u32` slot indices, hashed and compared through [`hasher_for`]/
    /// [`record_key`] on the slot's own record.
    index: HashTable<u32>,
}

impl<K, V> Slab<K, V> {
    /// An empty slab: no allocation until the first insert, matching
    /// `HashTable::new()`'s own zero-allocation behavior.
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            index: HashTable::new(),
        }
    }

    /// A slab presized for `capacity` entries: the arena and index each
    /// allocate once, up front, reached through [`Stripe::with_capacity`].
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            index: HashTable::with_capacity(capacity),
        }
    }

    /// This slab's live entry count: `entries.len()`, always exact since
    /// the arena carries no holes or tombstoned slots.
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// This slab's reserved arena capacity: `entries.capacity()`.
    fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// The live entry at `hash` for which `eq` holds, if any.
    fn find(&self, hash: u64, mut eq: impl FnMut(&Live<K, V>) -> bool) -> Option<&Live<K, V>> {
        let entries = &self.entries;
        let &slot = self.index.find(hash, |&i| eq(&entries[i as usize]))?;
        Some(&self.entries[slot as usize])
    }

    /// [`Slab::find`]'s mutable counterpart.
    fn find_mut(
        &mut self,
        hash: u64,
        mut eq: impl FnMut(&Live<K, V>) -> bool,
    ) -> Option<&mut Live<K, V>> {
        let entries = &self.entries;
        let slot = *self.index.find(hash, |&i| eq(&entries[i as usize]))?;
        Some(&mut self.entries[slot as usize])
    }

    /// Looks `hash` up under `eq`: [`SlabEntry::Occupied`] naming the slot
    /// when found, [`SlabEntry::Vacant`] carrying `hash`/`hasher` otherwise.
    fn entry<F>(
        &mut self,
        hash: u64,
        mut eq: impl FnMut(&Live<K, V>) -> bool,
        hasher: F,
    ) -> SlabEntry<'_, K, V, F>
    where
        F: Fn(&Live<K, V>) -> u64,
    {
        let found = {
            let entries = &self.entries;
            self.index
                .find(hash, |&i| eq(&entries[i as usize]))
                .copied()
        };
        match found {
            Some(slot) => SlabEntry::Occupied(SlabOccupiedEntry {
                slab: self,
                hash,
                slot: slot as usize,
                hasher,
            }),
            None => SlabEntry::Vacant(SlabVacantEntry {
                slab: self,
                hash,
                hasher,
            }),
        }
    }

    /// Pushes `value` onto the arena and indexes it at `hash`, returning
    /// the slot. No duplicate-key check.
    fn insert_unique(
        &mut self,
        hash: u64,
        value: Live<K, V>,
        hasher: impl Fn(&Live<K, V>) -> u64,
    ) -> u32 {
        let slot = u32::try_from(self.entries.len())
            .expect("a stripe never holds anywhere near u32::MAX live entries");
        if self.entries.len() == self.entries.capacity() {
            self.entries.reserve_exact(arena_growth(self.entries.len()));
        }
        self.entries.push(value);
        let entries = &self.entries;
        self.index
            .insert_unique(hash, slot, |&i| hasher(&entries[i as usize]));
        slot
    }

    /// Removes the live entry at `hash` for which `eq` holds, if any. No
    /// production call site uses this; only tests do.
    #[allow(
        dead_code,
        reason = "no production call site removes through this convenience wrapper yet; \
                  exercised directly by slab_remove_of_absent_key_returns_none and \
                  slab_random_permutation_insert_remove_matches_a_hashmap_mirror"
    )]
    fn remove(
        &mut self,
        hash: u64,
        eq: impl FnMut(&Live<K, V>) -> bool,
        hasher: impl Fn(&Live<K, V>) -> u64,
    ) -> Option<Live<K, V>> {
        match self.entry(hash, eq, hasher) {
            SlabEntry::Occupied(occ) => Some(occ.remove().0),
            SlabEntry::Vacant(_) => None,
        }
    }

    /// Swap-removes arena slot `slot` (index row hashing to `hash`),
    /// repointing the moved entry's index row so `entries` stays dense.
    fn remove_at(
        &mut self,
        hash: u64,
        slot: usize,
        hasher: &impl Fn(&Live<K, V>) -> u64,
    ) -> Live<K, V> {
        let slot_u32 = u32::try_from(slot).expect("slot is a valid arena index, so it fits a u32");
        let index_row = self
            .index
            .find_entry(hash, |&i| i == slot_u32)
            .expect("remove_at is only called for a slot this slab's index still names");
        index_row.remove();
        let last = self.entries.len() - 1;
        let removed = self.entries.swap_remove(slot);
        if slot != last {
            let moved_hash = hasher(&self.entries[slot]);
            let last_u32 =
                u32::try_from(last).expect("last is a valid arena index, so it fits a u32");
            let moved_row = self
                .index
                .find_mut(moved_hash, |&i| i == last_u32)
                .expect("the entry swapped into `slot` still has its own index row at `last`");
            *moved_row = slot_u32;
        }
        removed
    }

    /// Keeps only entries for which `f` returns `true`, dense afterwards: a
    /// kept entry's slot never moves; a dropped one is swap-removed.
    fn retain(
        &mut self,
        hasher: impl Fn(&Live<K, V>) -> u64,
        mut f: impl FnMut(&Live<K, V>) -> bool,
    ) {
        let mut i = 0;
        while i < self.entries.len() {
            if f(&self.entries[i]) {
                i += 1;
            } else {
                let hash = hasher(&self.entries[i]);
                self.remove_at(hash, i, &hasher);
            }
        }
    }

    /// Every live entry, in arena order (no caller depends on the order).
    fn iter(&self) -> std::slice::Iter<'_, Live<K, V>> {
        self.entries.iter()
    }

    /// Drains and returns every live entry, emptying both the arena and the
    /// index.
    fn drain(&mut self) -> std::vec::Drain<'_, Live<K, V>> {
        self.index.clear();
        self.entries.drain(..)
    }

    /// Shrinks the arena and index to fit, rehashing every entry's index
    /// row. Called by [`Engine::compact`]'s paced shrink pass.
    fn shrink_to_fit(&mut self, hasher: impl Fn(&Live<K, V>) -> u64) {
        self.entries.shrink_to_fit();
        let entries = &self.entries;
        self.index.shrink_to_fit(|&i| hasher(&entries[i as usize]));
    }
}

/// [`Slab::entry`]'s result: an occupied arena slot or a vacant hash+hasher
/// pair ready for [`SlabVacantEntry::insert`].
enum SlabEntry<'a, K, V, F> {
    Occupied(SlabOccupiedEntry<'a, K, V, F>),
    Vacant(SlabVacantEntry<'a, K, V, F>),
}

/// An occupied [`Slab`] arena slot found by [`Slab::entry`].
struct SlabOccupiedEntry<'a, K, V, F> {
    slab: &'a mut Slab<K, V>,
    hash: u64,
    slot: usize,
    hasher: F,
}

impl<'a, K, V, F> SlabOccupiedEntry<'a, K, V, F>
where
    F: Fn(&Live<K, V>) -> u64,
{
    /// A reference to this slot's entry.
    #[cfg_attr(not(any(test, feature = "spill")), allow(dead_code))]
    fn get(&self) -> &Live<K, V> {
        &self.slab.entries[self.slot]
    }

    /// A mutable reference to this slot's entry. No production site uses
    /// this; kept for symmetry with hashbrown's own `OccupiedEntry` API.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "every production mutation site uses Slab::find_mut directly instead; \
                      kept for symmetry with SlabOccupiedEntry::get and hashbrown's own \
                      OccupiedEntry API"
        )
    )]
    fn get_mut(&mut self) -> &mut Live<K, V> {
        &mut self.slab.entries[self.slot]
    }

    /// Takes this slot's entry out via [`Slab::remove_at`], returning it
    /// with a [`SlabVacantEntry`] for the same hash.
    fn remove(self) -> (Live<K, V>, SlabVacantEntry<'a, K, V, F>) {
        let removed = self.slab.remove_at(self.hash, self.slot, &self.hasher);
        (
            removed,
            SlabVacantEntry {
                slab: self.slab,
                hash: self.hash,
                hasher: self.hasher,
            },
        )
    }
}

/// A vacant [`Slab`] hash slot found by [`Slab::entry`], or handed back by
/// [`SlabOccupiedEntry::remove`].
struct SlabVacantEntry<'a, K, V, F> {
    slab: &'a mut Slab<K, V>,
    hash: u64,
    hasher: F,
}

impl<K, V, F> SlabVacantEntry<'_, K, V, F>
where
    F: Fn(&Live<K, V>) -> u64,
{
    /// Inserts `value` at this vacant hash via [`Slab::insert_unique`]. No
    /// production path uses this; kept for symmetry with hashbrown's API.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "every production write path calls Slab::insert_unique directly instead; \
                      kept for a caller that only has a SlabEntry::Vacant in hand, and for \
                      symmetry with hashbrown's own VacantEntry API"
        )
    )]
    fn insert(self, value: Live<K, V>) -> u32 {
        self.slab.insert_unique(self.hash, value, self.hasher)
    }
}

#[cfg(test)]
mod slab_tests {
    use super::*;

    fn key_bytes(key: u32) -> Bytes {
        Bytes::from(postcard::to_stdvec(&key).expect("u32 encodes"))
    }

    fn hash_of(key: u32) -> u64 {
        hash_key_bytes(key_bytes(key).as_ref())
    }

    fn eq_for(key: u32) -> impl FnMut(&Live<u32, Vec<u8>>) -> bool {
        move |l: &Live<u32, Vec<u8>>| record_key(&l.record) == key_bytes(key).as_ref()
    }

    fn live_for(key: u32, value: &[u8]) -> Live<u32, Vec<u8>> {
        Live::new(
            build_resident_record(key_bytes(key).as_ref(), value),
            Hlc {
                wall_ms: 1,
                logical: 0,
                node: NodeId::from(1),
            },
            NEVER_EXPIRES,
            1,
            0,
            #[cfg(feature = "spill")]
            EntryState::Resident,
        )
    }

    #[test]
    fn slab_find_locates_inserted_key() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        let hash = hash_of(7);
        slab.insert_unique(hash, live_for(7, b"v7"), hasher_for);
        let found = slab
            .find(hash, eq_for(7))
            .expect("just-inserted key is findable");
        assert_eq!(record_value(&found.record), b"v7");
    }

    #[test]
    fn arena_growth_adds_a_quarter_and_at_least_the_minimum() {
        assert_eq!(arena_growth(0), ARENA_MIN_GROWTH);
        assert_eq!(arena_growth(8), ARENA_MIN_GROWTH);
        assert_eq!(arena_growth(16), 4);
        assert_eq!(arena_growth(100), 25);
        assert_eq!(arena_growth(1_000_000), 250_000);
    }

    #[test]
    fn slab_arena_stays_four_fifths_full_as_it_grows() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        for key in 0..5_000u32 {
            slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
            let len = slab.len();
            assert!(
                slab.capacity() <= len + arena_growth(len.saturating_sub(1)),
                "at {len} entries the arena holds {} slots",
                slab.capacity()
            );
            if len >= 20 {
                assert!(
                    slab.capacity() * 4 <= len * 5,
                    "at {len} entries the arena holds {} slots, more than a quarter spare",
                    slab.capacity()
                );
            }
        }
        for key in 0..5_000u32 {
            assert!(slab.find(hash_of(key), eq_for(key)).is_some());
        }
    }

    #[test]
    fn slab_find_returns_none_for_absent_key() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        slab.insert_unique(hash_of(1), live_for(1, b"v1"), hasher_for);
        assert!(slab.find(hash_of(2), eq_for(2)).is_none());
    }

    #[test]
    fn slab_insert_grows_entries_by_one_and_is_findable() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        assert_eq!(slab.len(), 0);
        let hash = hash_of(3);
        let slot = slab.insert_unique(hash, live_for(3, b"v3"), hasher_for);
        assert_eq!(slot, 0);
        assert_eq!(slab.len(), 1);
        assert!(slab.find(hash, eq_for(3)).is_some());
        let live = slab
            .find_mut(hash, eq_for(3))
            .expect("just-inserted key is mutably findable");
        live.weight = 42;
        assert_eq!(slab.find(hash, eq_for(3)).unwrap().weight, 42);
    }

    #[test]
    fn slab_occupied_entry_get_mut_mutates_the_slot_visibly_through_find() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        let hash = hash_of(9);
        slab.insert_unique(hash, live_for(9, b"v9"), hasher_for);
        let SlabEntry::Occupied(mut occ) = slab.entry(hash, eq_for(9), hasher_for) else {
            panic!("key 9 is present");
        };
        occ.get_mut().weight = 77;
        assert_eq!(
            slab.find(hash, eq_for(9)).unwrap().weight,
            77,
            "a mutation through get_mut is visible on a later find"
        );
    }

    #[test]
    fn slab_vacant_entry_insert_makes_the_value_findable() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        let hash = hash_of(11);
        let SlabEntry::Vacant(vacant) = slab.entry(hash, eq_for(11), hasher_for) else {
            panic!("key 11 is absent from an empty slab");
        };
        let slot = vacant.insert(live_for(11, b"v11"));
        assert_eq!(slot, 0, "the first insert lands in arena slot 0");
        assert_eq!(slab.len(), 1);
        let found = slab
            .find(hash, eq_for(11))
            .expect("the value inserted through a vacant entry is findable");
        assert_eq!(record_value(&found.record), b"v11");
    }

    #[test]
    fn slab_remove_swaps_last_entry_into_freed_slot_and_fixes_its_index() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        let (ha, hb, hc) = (hash_of(10), hash_of(20), hash_of(30));
        slab.insert_unique(ha, live_for(10, b"a"), hasher_for);
        slab.insert_unique(hb, live_for(20, b"b"), hasher_for);
        slab.insert_unique(hc, live_for(30, b"c"), hasher_for);
        assert_eq!(slab.len(), 3);

        // Removing slot 0 swap-moves key 30 into it; its index row must
        // repoint.
        let SlabEntry::Occupied(occ) = slab.entry(ha, eq_for(10), hasher_for) else {
            panic!("key 10 is present");
        };
        assert_eq!(record_value(&occ.get().record), b"a");
        let (removed, _vacant) = occ.remove();
        assert_eq!(record_value(&removed.record), b"a");

        assert_eq!(slab.len(), 2);
        let moved = slab
            .find(hc, eq_for(30))
            .expect("the swapped-in entry keeps its own findability at its own hash");
        assert_eq!(record_value(&moved.record), b"c");
        assert!(slab.find(hb, eq_for(20)).is_some());
        assert!(slab.find(ha, eq_for(10)).is_none());

        // Every remaining entry is still reachable from its own hash.
        let keys: Vec<u32> = slab
            .iter()
            .map(|live| decode_key(record_key(&live.record)))
            .collect();
        for key in keys {
            assert!(slab.find(hash_of(key), eq_for(key)).is_some());
        }
    }

    #[test]
    fn slab_remove_of_the_last_slot_needs_no_fixup() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        let (ha, hb) = (hash_of(1), hash_of(2));
        slab.insert_unique(ha, live_for(1, b"a"), hasher_for);
        slab.insert_unique(hb, live_for(2, b"b"), hasher_for);

        let SlabEntry::Occupied(occ) = slab.entry(hb, eq_for(2), hasher_for) else {
            panic!("key 2 is present");
        };
        let (removed, _vacant) = occ.remove();
        assert_eq!(record_value(&removed.record), b"b");
        assert_eq!(slab.len(), 1);
        assert!(slab.find(ha, eq_for(1)).is_some());
        assert!(slab.find(hb, eq_for(2)).is_none());
    }

    #[test]
    fn slab_remove_of_absent_key_returns_none() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        slab.insert_unique(hash_of(1), live_for(1, b"a"), hasher_for);
        assert!(slab.remove(hash_of(99), eq_for(99), hasher_for).is_none());
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn slab_retain_keeps_dense_and_visits_every_removed_entry_once() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        for key in 0..10u32 {
            slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
        }
        let mut visited: Vec<u32> = Vec::new();
        slab.retain(hasher_for, |live| {
            let k: u32 = decode_key(record_key(&live.record));
            assert!(!visited.contains(&k), "key {k} visited more than once");
            visited.push(k);
            k.is_multiple_of(2)
        });
        visited.sort_unstable();
        assert_eq!(
            visited,
            (0..10).collect::<Vec<_>>(),
            "retain visits every entry exactly once"
        );
        assert_eq!(slab.len(), 5);
        for key in (0..10u32).step_by(2) {
            assert!(slab.find(hash_of(key), eq_for(key)).is_some());
        }
        for key in (1..10u32).step_by(2) {
            assert!(slab.find(hash_of(key), eq_for(key)).is_none());
        }
    }

    #[test]
    fn slab_iter_visits_every_live_entry_exactly_once_in_some_order() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        let keys: Vec<u32> = (0..7).collect();
        for &key in &keys {
            slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
        }
        let mut seen: Vec<u32> = slab
            .iter()
            .map(|live| decode_key(record_key(&live.record)))
            .collect();
        seen.sort_unstable();
        assert_eq!(seen, keys);
    }

    #[test]
    fn slab_drain_empties_and_returns_every_entry() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        let keys: Vec<u32> = (0..5).collect();
        for &key in &keys {
            slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
        }
        let mut drained: Vec<u32> = slab
            .drain()
            .map(|live| decode_key(record_key(&live.record)))
            .collect();
        drained.sort_unstable();
        assert_eq!(drained, keys);
        assert_eq!(slab.len(), 0);
        assert!(slab.find(hash_of(0), eq_for(0)).is_none());
    }

    #[test]
    fn slab_with_capacity_presizes_entries_and_index_with_no_growth_up_to_hint() {
        let mut slab: Slab<u32, Vec<u8>> = Slab::with_capacity(64);
        assert!(slab.entries.capacity() >= 64);
        let entries_cap_before = slab.entries.capacity();
        for key in 0..64u32 {
            slab.insert_unique(hash_of(key), live_for(key, b"v"), hasher_for);
        }
        assert_eq!(
            slab.entries.capacity(),
            entries_cap_before,
            "no growth up to the hint"
        );
        assert_eq!(slab.len(), 64);
        for key in 0..64u32 {
            assert!(slab.find(hash_of(key), eq_for(key)).is_some());
        }

        // After removing all but one entry, shrink drops the oversized
        // allocation.
        for key in 1..64u32 {
            slab.remove(hash_of(key), eq_for(key), hasher_for);
        }
        assert_eq!(slab.len(), 1);
        slab.shrink_to_fit(hasher_for);
        assert!(slab.entries.capacity() < entries_cap_before);
        assert!(slab.find(hash_of(0), eq_for(0)).is_some());
    }

    #[test]
    fn slab_random_permutation_insert_remove_matches_a_hashmap_mirror() {
        use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

        let mut slab: Slab<u32, Vec<u8>> = Slab::new();
        let mut mirror: HashMap<u32, u8> = HashMap::new();
        let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF);

        for step in 0..2000u32 {
            let key = rng.random_range(0..64u32);
            let hash = hash_of(key);
            let value_byte = (step % 251) as u8;
            let was_present = mirror.contains_key(&key);
            if was_present {
                if rng.random_bool(0.5) {
                    let removed = slab.remove(hash, eq_for(key), hasher_for);
                    assert!(
                        removed.is_some(),
                        "step {step}: key {key} is in the mirror but not the slab"
                    );
                    slab.insert_unique(hash, live_for(key, &[value_byte]), hasher_for);
                    mirror.insert(key, value_byte);
                } else {
                    let removed = slab.remove(hash, eq_for(key), hasher_for);
                    assert!(
                        removed.is_some(),
                        "step {step}: key {key} is in the mirror but not the slab"
                    );
                    mirror.remove(&key);
                }
            } else {
                assert!(
                    slab.remove(hash, eq_for(key), hasher_for).is_none(),
                    "step {step}: key {key} is absent from the mirror but the slab found it"
                );
                slab.insert_unique(hash, live_for(key, &[value_byte]), hasher_for);
                mirror.insert(key, value_byte);
            }

            assert_eq!(
                slab.len(),
                mirror.len(),
                "step {step}: length diverged from the mirror"
            );
            for (&k, &v) in &mirror {
                let found = slab.find(hash_of(k), eq_for(k)).unwrap_or_else(|| {
                    panic!("step {step}: key {k} is in the mirror but not the slab")
                });
                assert_eq!(
                    record_value(&found.record),
                    [v],
                    "step {step}: key {k}'s value diverged"
                );
            }
        }

        // shrink_to_fit at the end must not disturb any surviving entry.
        slab.shrink_to_fit(hasher_for);
        for (&k, &v) in &mirror {
            let found = slab
                .find(hash_of(k), eq_for(k))
                .expect("shrink_to_fit must not drop or misplace a surviving entry");
            assert_eq!(record_value(&found.record), [v]);
        }
    }
}

/// What [`remove_live`] reports about the entry it took out of `live`:
/// its weight, already `0` for a [`EntryState::Spilled`] entry, its
/// version, and whether it held a spilled payload. Every caller that
/// discards a live entry needs `was_spilled` to keep `sundog_spill_entries{cache}`
/// from drifting, via [`Engine::note_spill_departure`] or
/// [`Engine::note_spill_departures`].
struct RemovedLive {
    weight: u32,
    ver: Hlc,
    was_spilled: bool,
}

/// Removes the live entry at `key_bytes`, hashing to `hash`, and clears any
/// `long_ttl` row it held: the one place every removal path funnels through.
fn remove_live<K, V>(
    stripe: &mut Stripe<K, V>,
    hash: u64,
    key_bytes: &[u8],
) -> Option<RemovedLive> {
    match stripe
        .live
        .entry(hash, |l| record_key(&l.record) == key_bytes, hasher_for)
    {
        SlabEntry::Occupied(occ) => {
            let (removed, _vacant) = occ.remove();
            stripe.long_ttl.remove(key_bytes);
            Some(RemovedLive {
                weight: removed.weight,
                ver: removed.ver(),
                was_spilled: is_spilled(&removed),
            })
        }
        SlabEntry::Vacant(_) => None,
    }
}

/// What consulting the resolver on a real key collision decided, the
/// [`ConflictResolver`]-consultation half of [`apply_locked`]'s decision. See
/// [`super::Shard::apply`]'s docs for the correctness contract.
enum Resolution {
    /// `incoming` lost outright: nothing changes.
    IncomingLoses,
    /// `incoming` won outright: proceed with `incoming` exactly as given.
    IncomingWins,
    /// The resolver folded both sides into a new value, to be stored (or
    /// not, if [`merge_version`] finds nothing to do) under a version it
    /// decides from `sv`, `ver`, and how the merged bytes compare to each
    /// side's own bytes.
    Merged {
        value: Bytes,
        expires_at_ms: Option<u64>,
    },
}

/// Consults `resolver` on `incoming` at `ver` against whatever is stored at
/// `sv`. Returns [`Resolution::IncomingLoses`] on the equal-version fast
/// path (an already-applied record seen again) without calling `resolver`.
///
/// [`ConflictResolver::merge`] runs only when [`ConflictResolver::merges`]
/// is `true` and both sides carry a real value; a tombstone or an
/// unreadable spilled side never reaches it, and [`ConflictResolver::winner`]
/// decides instead. No side is ever merged against a fabricated value:
/// [`apply_locked`] reads a spilled side's real bytes through
/// [`read_spilled_for_conflict`] first, so a readable spilled side merges
/// like a resident one.
fn resolve_conflict<V>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    sv: Hlc,
    stored_encoded: Option<&[u8]>,
    stored_expires_at_ms: Option<u64>,
    ver: Hlc,
    incoming: &Incoming<V>,
) -> Resolution {
    if sv == ver {
        return Resolution::IncomingLoses;
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
    if resolver.merges()
        && stored_view.value.is_some()
        && incoming_view.value.is_some()
        && let Some(Merged {
            value,
            expires_at_ms,
        }) = resolver.merge(key_bytes, stored_view, incoming_view)
    {
        return Resolution::Merged {
            value,
            expires_at_ms,
        };
    }
    match resolver.winner(key_bytes, stored_view, incoming_view) {
        Winner::A => Resolution::IncomingLoses,
        Winner::B => Resolution::IncomingWins,
    }
}

/// What a resolver's [`ConflictResolver::merge`] reply becomes once [`merge_version`]
/// has weighed the two inputs' versions against how the merged bytes compare
/// to each side's own bytes.
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
enum MergedVersion {
    /// The merge changes nothing this node doesn't already have at a version
    /// at least as high: no write.
    NoOp,
    /// Store the merged bytes under this version.
    Store(Hlc),
}

/// Decides the version and disposition of a [`ConflictResolver::merge`]
/// reply, from `sv`/`stored` (this stripe's own version and bytes) and
/// `ver`/`incoming` (the colliding write's) against the resolver's folded
/// `merged` bytes. Four outcomes: identical content everywhere adopts the
/// greater of `sv`/`ver` under [`Hlc`]'s `Ord`, or no-ops if `sv` already
/// is; a merge reducing to `incoming` with `ver > sv` stores its `(ver,
/// merged)` pair verbatim; one reducing to `stored` with `sv > ver` is a
/// no-op; otherwise mints a version, [`NodeId::merge_derived`] from the
/// merged bytes, that strictly dominates both inputs under `Hlc`'s `Ord`.
/// `permutation_convergence` and
/// `pn_counter_bidirectional_gossip_converges_in_one_round` in
/// `store::prop_tests` prove this converges, including the one-round case
/// under `cluster::anti_entropy`'s bidirectional exchange.
fn merge_version(
    sv: Hlc,
    ver: Hlc,
    stored: &[u8],
    incoming: &[u8],
    merged: &[u8],
) -> MergedVersion {
    if merged == stored && merged == incoming {
        let v = sv.max(ver);
        if v == sv {
            MergedVersion::NoOp
        } else {
            MergedVersion::Store(v)
        }
    } else if merged == incoming && ver > sv {
        MergedVersion::Store(ver)
    } else if merged == stored && sv > ver {
        MergedVersion::NoOp
    } else {
        let base_wall = sv.wall_ms.max(ver.wall_ms);
        let base_logical = sv.logical.max(ver.logical);
        let (wall_ms, logical) = match base_logical.checked_add(1) {
            Some(logical) => (base_wall, logical),
            // `logical` has no room left to grow within this `wall_ms`: carry
            // into `wall_ms` instead so the minted stamp still strictly
            // dominates both inputs (see this function's doc).
            None => (base_wall.saturating_add(1), 0),
        };
        MergedVersion::Store(Hlc {
            wall_ms,
            logical,
            node: NodeId::merge_derived(xxh3_64(merged)),
        })
    }
}

/// [`apply_locked`]'s full collision decision: consults [`resolve_conflict`]
/// and, on [`Resolution::Merged`], rebinds `ver`/`incoming` to the version
/// and value [`merge_version`] decides. Returns `None` for a rejected write
/// (outright loss, the engine-enforced tombstone/spill guard, a redelivered
/// already-absorbed merge, or bytes that fail to decode as `V`) and
/// `Some((ver, incoming))` otherwise: unchanged for an outright win, rebound
/// for a merge.
fn resolve_and_rebind<V: DeserializeOwned>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    sv: Hlc,
    stored_encoded: Option<&[u8]>,
    stored_expires_at_ms: Option<u64>,
    ver: Hlc,
    incoming: Incoming<V>,
) -> Option<(Hlc, Incoming<V>)> {
    match resolve_conflict(
        resolver,
        key_bytes,
        sv,
        stored_encoded,
        stored_expires_at_ms,
        ver,
        &incoming,
    ) {
        Resolution::IncomingLoses => None,
        Resolution::IncomingWins => Some((ver, incoming)),
        Resolution::Merged {
            value,
            expires_at_ms,
        } => {
            // `resolve_conflict`'s guard confirms both sides carry a real
            // value whenever it returns `Merged`, so these are always
            // `Some`/`Put`; falling back to rejecting rather than trusting
            // that invariant blindly costs nothing.
            let stored = stored_encoded?;
            let Incoming::Put {
                encoded: incoming_encoded,
                ..
            } = &incoming
            else {
                return None;
            };
            match merge_version(sv, ver, stored, incoming_encoded, &value) {
                MergedVersion::NoOp => None,
                MergedVersion::Store(new_ver) => {
                    // Never trust a resolver's bytes blindly: reject, don't panic.
                    let Ok(decoded): Result<V, _> = postcard::from_bytes(&value) else {
                        return None;
                    };
                    Some((
                        new_ver,
                        Incoming::Put {
                            value: decoded,
                            expires_at_ms,
                            encoded: value,
                        },
                    ))
                }
            }
        }
    }
}

/// One [`Engine::apply_many`] batch entry, before or after pre-fold:
/// precomputed hash, the typed key, its wire-encoded bytes, the write's
/// version, and the value or tombstone it carries. Named only to keep
/// [`prefold_batch`]'s and [`Engine::apply_many`]'s signatures under
/// clippy's type-complexity threshold; structurally identical to the plain
/// tuple every caller already builds.
type BatchEntry<K, V> = (u64, K, Bytes, Hlc, Incoming<V>);

/// [`prefold_batch`]'s per-run fold: `seed_ver`/`seed_incoming` and every
/// subsequent `(ver, incoming)` in `rest`, in order, folded into one
/// survivor by repeatedly calling [`resolve_and_rebind`], the same function
/// [`apply_locked`] calls against real stored state, with the running
/// accumulator standing in for "stored". Every entry is a real
/// `Incoming::Put`: [`prefold_batch`] never lets a run cross an
/// `Incoming::Tombstone`. Runs entirely outside any stripe lock; see
/// [`prefold_batch`]'s doc for why seeding with the key's real stored
/// record, when there is one, keeps the minted version identical to
/// sequential application's, not only the bytes.
fn fold_run<V: DeserializeOwned>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    seed_ver: Hlc,
    seed_incoming: Incoming<V>,
    rest: Vec<(Hlc, Incoming<V>)>,
) -> (Hlc, Incoming<V>) {
    let mut acc_ver = seed_ver;
    let mut acc_incoming = seed_incoming;
    for (ver, incoming) in rest {
        let (stored_encoded, stored_expires_at_ms) = match &acc_incoming {
            Incoming::Put {
                encoded,
                expires_at_ms,
                ..
            } => (Some(encoded.as_ref()), *expires_at_ms),
            Incoming::Tombstone => (None, None),
        };
        if let Some((new_ver, new_incoming)) = resolve_and_rebind(
            resolver,
            key_bytes,
            acc_ver,
            stored_encoded,
            stored_expires_at_ms,
            ver,
            incoming,
        ) {
            acc_ver = new_ver;
            acc_incoming = new_incoming;
        }
    }
    (acc_ver, acc_incoming)
}

/// Groups `entries` by key bytes, preserving each key's own relative order
/// however its entries are interleaved with other keys' in the batch, and
/// splits each key's indices into maximal runs of consecutive
/// `Incoming::Put` entries. A run never crosses an `Incoming::Tombstone`: a
/// tombstone always starts a singleton run of its own, on both sides of it,
/// since `merge` is never consulted against a tombstone's value-less side
/// (see [`resolve_conflict`]), and pre-folding takes the same stance
/// rather than relying on that guard alone. Shared by [`prefold_batch`] and
/// [`Engine::apply_many`]'s seed lookup so both agree on exactly which runs
/// are long enough to fold.
fn group_prefold_runs<K, V>(entries: &[BatchEntry<K, V>]) -> Vec<Vec<usize>> {
    let mut by_key: HashMap<Bytes, Vec<usize>> = HashMap::new();
    for (i, entry) in entries.iter().enumerate() {
        by_key.entry(entry.2.clone()).or_default().push(i);
    }
    let mut runs: Vec<Vec<usize>> = Vec::new();
    for indices in by_key.into_values() {
        let mut current: Vec<usize> = Vec::new();
        for idx in indices {
            match &entries[idx].4 {
                Incoming::Put { .. } => current.push(idx),
                Incoming::Tombstone => {
                    if !current.is_empty() {
                        runs.push(std::mem::take(&mut current));
                    }
                    runs.push(vec![idx]);
                }
            }
        }
        if !current.is_empty() {
            runs.push(current);
        }
    }
    runs
}

/// The key's real currently-stored record, in the same value-aware,
/// tombstone/spill-degraded shape [`apply_locked`] itself reads before
/// calling [`resolve_and_rebind`]: `None` against a tombstone, an absent
/// key, or a currently-spilled entry whose bytes weren't already prefetched
/// into `prefetched_spilled` (no value bytes to fold), `Some` with the
/// stored `Hlc`, encoded bytes, and TTL otherwise. Read under a brief stripe
/// read lock, before any decode or fold work runs; see
/// [`Engine::apply_many`]'s call site.
/// [`peek_stored_seed`]'s and [`peek_prefold_seeds`]'s result for one key:
/// the real stored `(version, encoded bytes, TTL)` to seed
/// [`prefold_batch`]'s fold with, or `None` when there is none to seed
/// with. Named only to keep the two functions' signatures under clippy's
/// type-complexity threshold.
type PrefoldSeed = Option<(Hlc, Bytes, Option<u64>)>;

fn peek_stored_seed<K, V>(
    stripe: &Stripe<K, V>,
    hash: u64,
    key_bytes: &[u8],
    #[cfg(feature = "spill")] prefetched_spilled: &HashMap<SpillLoc, Bytes>,
) -> PrefoldSeed {
    if stripe.tombstones.contains_key(key_bytes) {
        return None;
    }
    let live = stripe
        .live
        .find(hash, |l| record_key(&l.record) == key_bytes)?;
    // `prefetched_spilled` is read off-lock, before this stripe's lock, in
    // `prefetch_spilled_conflict_bytes`; a miss degrades to no seed.
    #[cfg(feature = "spill")]
    let encoded = match &live.state {
        EntryState::Resident => record_value_bytes(&live.record),
        EntryState::Spilled(loc) => prefetched_spilled.get(loc).cloned()?,
    };
    #[cfg(not(feature = "spill"))]
    let encoded = record_value_bytes(&live.record);
    Some((
        live.ver(),
        encoded,
        decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl),
    ))
}

/// [`Engine::apply_many`]'s seed lookup: for every `runs` entry long enough
/// to fold (`len() >= 2`), [`peek_stored_seed`]s that key once, keyed by
/// its wire bytes. Takes the stripe read lock for exactly this pass;
/// [`Engine::apply_many`] drops the guard immediately after this returns,
/// before any decode or fold work, which all runs lock-free afterward in
/// [`prefold_batch`]. `prefetched_spilled`, when the `spill` feature is
/// on, is [`prefetch_spilled_conflict_bytes`]'s result, computed before
/// this same read lock is taken.
fn peek_prefold_seeds<K, V>(
    stripe: &Stripe<K, V>,
    entries: &[BatchEntry<K, V>],
    runs: &[Vec<usize>],
    #[cfg(feature = "spill")] prefetched_spilled: &HashMap<SpillLoc, Bytes>,
) -> HashMap<Bytes, PrefoldSeed> {
    let mut seeds = HashMap::new();
    for run in runs {
        if run.len() < 2 {
            continue;
        }
        let (hash, _, key_bytes, ..) = &entries[run[0]];
        seeds.entry(key_bytes.clone()).or_insert_with(|| {
            peek_stored_seed(
                stripe,
                *hash,
                key_bytes.as_ref(),
                #[cfg(feature = "spill")]
                prefetched_spilled,
            )
        });
    }
    seeds
}

/// [`Engine::apply_many`]'s pre-fold: run only when the resolver's
/// [`ConflictResolver::merges`] is `true` and pre-folding is on, folds
/// every maximal run [`group_prefold_runs`] found into one survivor with
/// [`fold_run`], outside any stripe lock. A run whose key has a real stored
/// record ([`peek_prefold_seeds`]) folds that record in first, matching
/// sequential application's order: `merge_version`'s content-join is
/// fold-order independent but its mint arm's running max is not, so seeding
/// first keeps the minted version identical too, not only the bytes. A run
/// with no stored record folds from its own first entry.
///
/// Returns one slot per original index: `Some` where [`apply_locked`] still
/// runs, `None` where a run absorbed the entry ([`Engine::apply_many`]
/// reports those as [`ApplyOutcome::Rejected`]).
fn prefold_batch<K, V: DeserializeOwned>(
    entries: Vec<BatchEntry<K, V>>,
    runs: Vec<Vec<usize>>,
    resolver: &dyn ConflictResolver,
    stored_seeds: &HashMap<Bytes, PrefoldSeed>,
) -> Vec<Option<BatchEntry<K, V>>> {
    let mut slots: Vec<Option<BatchEntry<K, V>>> = entries.into_iter().map(Some).collect();
    for run in runs {
        if run.len() < 2 {
            // Alone for this run: nothing to fold, left exactly as given.
            continue;
        }
        let last = *run.last().expect("checked run.len() >= 2 above");
        let mut positions = run.into_iter();
        let seed_idx = positions.next().expect("checked run.len() >= 2 above");
        let (hash, key, key_bytes, seed_ver, seed_incoming) = slots[seed_idx]
            .take()
            .expect("each batch index belongs to at most one run");
        let rest: Vec<(Hlc, Incoming<V>)> = positions
            .map(|idx| {
                let (_, _, _, ver, incoming) = slots[idx]
                    .take()
                    .expect("each batch index belongs to at most one run");
                (ver, incoming)
            })
            .collect();

        // Decoding the peeked stored bytes never happens under the read
        // lock `peek_prefold_seeds` took: that lock is gone by here.
        let stored_decoded = stored_seeds
            .get(key_bytes.as_ref())
            .and_then(Option::as_ref)
            .and_then(|(sv, encoded, expires_at_ms)| {
                postcard::from_bytes::<V>(encoded)
                    .ok()
                    .map(|value| (*sv, value, encoded.clone(), *expires_at_ms))
            });

        let (folded_ver, folded_incoming) = match stored_decoded {
            Some((sv, value, encoded, expires_at_ms)) => {
                let stored_incoming = Incoming::Put {
                    value,
                    expires_at_ms,
                    encoded,
                };
                let mut whole_run = Vec::with_capacity(rest.len() + 1);
                whole_run.push((seed_ver, seed_incoming));
                whole_run.extend(rest);
                fold_run(resolver, key_bytes.as_ref(), sv, stored_incoming, whole_run)
            }
            None => fold_run(resolver, key_bytes.as_ref(), seed_ver, seed_incoming, rest),
        };
        slots[last] = Some((hash, key, key_bytes, folded_ver, folded_incoming));
    }
    slots
}

/// Whether a read of `live` at `now_ms` sees nothing: past its expiry, or
/// idle for `tti_ms` or longer. Lazy expiry and idle eviction both hinge on
/// this; a sweep only reclaims what it already reports absent.
fn absent_at<K, V>(
    live: &Live<K, V>,
    key_bytes: &[u8],
    long_ttl: &HashMap<Bytes, u64>,
    tti_ms: Option<u64>,
    now_ms: u64,
) -> bool {
    if let Some(exp) = decode_expiry(live.wall_ms, live.expiry, key_bytes, long_ttl)
        && now_ms >= exp
    {
        return true;
    }
    if let Some(tti) = tti_ms {
        let last = live.last_access_ms.load(Ordering::Relaxed);
        if idle_elapsed_ms(now_ms, last) >= tti {
            return true;
        }
    }
    false
}

/// [`Engine::compact`]'s paced shrink-pass rule: `true` once a stripe's
/// live count `len` sits at or below an eighth of its slab `capacity`,
/// the point past which the arena wastes at least seven times what it
/// holds. Well under the range an unhinted `Vec`'s own amortized growth
/// settles into, so a naturally growing stripe is never shrunk out from
/// under itself; this only fires for an oversized `capacity_hint` or a
/// stripe drained by ownership rebalancing. Pure; unit tested directly.
fn should_shrink_stripe(len: usize, capacity: usize) -> bool {
    capacity > 0 && len <= capacity / 8
}

/// [`Engine::enforce_capacity`]'s stop rule once one pass evicted nothing:
/// whether to return immediately, rather than pay for
/// [`Engine::evict_one_scanning`]'s full-stripe scan, and instead let the
/// flusher's own installs bring `pending_spill_weight` down on their own.
/// `true` whenever `pending_spill_weight` is still positive: some hand-off
/// is in flight, and it will resolve, install or abandon, shortly with no
/// further eviction needed on this call's part. `false` once it is back to
/// zero, this loop's ordinary cue to fall back to the scan exactly as it
/// always has, spill tier configured or not. Pure; unit tested directly.
fn defer_to_flusher(pending_spill_weight: u64) -> bool {
    pending_spill_weight > 0
}

/// [`Engine::enforce_capacity_with_reservation`]'s stop rule after a batch
/// reports a nonzero [`EvictOutcome::deficit`]: `0` once `current`
/// (weight read fresh after the batch) has already dropped to
/// `max_capacity` or under, `deficit` unchanged otherwise. A concurrent
/// flusher install or another victim's success can already satisfy
/// `current` even though `deficit` alone still looks owed. Pure; unit
/// tested directly.
fn deficit_once_still_over_capacity(current: u64, max_capacity: u64, deficit: u64) -> u64 {
    if current <= max_capacity { 0 } else { deficit }
}

/// How many of `sampled_weights` (coldest first) one lock hold evicts: the
/// fewest that clear `over_by`, at most [`EVICTION_BATCH`], and never more
/// than the colder half of the sample, so recency still decides under a
/// burst.
fn eviction_batch_size(over_by: u64, sampled_weights: &[u32]) -> usize {
    let cap = EVICTION_BATCH.min(sampled_weights.len().div_ceil(2));
    let mut cleared = 0u64;
    for (evicted, &weight) in sampled_weights.iter().take(cap).enumerate() {
        if cleared >= over_by {
            return evicted;
        }
        cleared += u64::from(weight);
    }
    cap
}

/// What one [`Engine::evict_one_sampled`]/[`Engine::evict_batch_sampled`]
/// lock hold accomplished, for [`Engine::enforce_capacity`]'s loop. A
/// spill hand-off frees its victim's weight at the same moment a physical
/// removal does, [`Engine::evict_victim_locked`]'s doc has the details, so
/// this carries only the one number either kind of victim contributes to;
/// nothing here needs to tell them apart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EvictOutcome {
    /// Weight freed this pass, physically removed or handed to a spill
    /// tier; `0` when the stripe this pass sampled held no victim, an
    /// empty stripe among them.
    removed_weight: u64,
    /// Sum of every [`VictimOutcome::PendingReservation`] this pass
    /// reported: bytes both the reservation and `admit`'s global headroom
    /// refused. Always `0` when `reservation` was `None`. A nonzero value
    /// tells [`Engine::enforce_capacity_with_reservation`] to stop rather
    /// than spin retrying an exhausted budget.
    deficit: u64,
}

impl EvictOutcome {
    /// Whether this pass accomplished nothing at all.
    fn made_no_progress(self) -> bool {
        self.removed_weight == 0
    }
}

/// What handing one victim to [`Engine::evict_victim_locked`] accomplished.
enum VictimOutcome {
    /// Physically removed from `live`: frees `weight` and one live entry,
    /// both the caller's job to fold into `total_weight`/`live_count`.
    Removed(u32),
    /// Committed to a configured spill tier: still `Resident` in `live`, at
    /// weight `0`, the entry's fate already decided under the stripe lock
    /// via [`SpillTier::would_accept`], but the channel send itself,
    /// [`SpillTier::enqueue`], deliberately deferred until the lock is
    /// released; see [`Engine::try_spill_victim`]. `weight` is what the
    /// entry carried right before hand-off, already zeroed on the entry
    /// itself, so it is the caller's to fold into `total_weight` exactly
    /// like `Removed`'s; only `live_count` differs between the two. The
    /// caller finishes the hand-off with [`Engine::finish_spill_handoff`]
    /// once the lock is dropped: on a full queue that restores the weight
    /// through [`SpillSink::abandon`] exactly as a downstream write or
    /// install failure would, never a physical removal. Only ever
    /// constructed under `feature = "spill"`, the only build where anything
    /// can be handed off in the first place.
    #[cfg(feature = "spill")]
    PendingSpill(u32, SpillJob),
    /// A configured spill tier refused this victim's hand-off
    /// ([`SpillTier::would_accept`] declining: too large, closed, or the
    /// flush queue is full), and `keep_resident` says to leave it
    /// resident rather than delete it, the `Mode::Replicated`/
    /// `Mode::Distributed` policy `Shard::attach_spill` sets on the tier.
    /// `stripe.live` is untouched: the entry keeps its full weight and
    /// stays a spill candidate for a later eviction pass to sample and
    /// retry, once the tier has room again.
    /// Reported to the caller exactly like [`VictimOutcome::Vanished`], no
    /// weight or count to fold in, since nothing here changed. Only ever
    /// constructed under `feature = "spill"`.
    #[cfg(feature = "spill")]
    Deferred,
    /// [`SpillTier::would_accept`] returned [`Admission::RefusedPending`];
    /// `stripe.live` is untouched, like [`VictimOutcome::Deferred`].
    #[cfg(feature = "spill")]
    PendingReservation(u32),
    /// Vanished between sampling and this call, a race with another
    /// writer on the same stripe; nothing to do.
    Vanished,
}

/// What [`Engine::try_spill_victim`] found for a resident victim.
#[cfg(feature = "spill")]
enum SpillAttempt {
    /// The tier commits to taking the victim; see
    /// [`Engine::try_spill_victim`]'s docs for what the two fields mean.
    Committed(u32, SpillJob),
    /// [`SpillTier::would_accept`] returned [`Admission::RefusedFinal`].
    /// `keep_resident` is the tier's policy at refusal time.
    Refused { keep_resident: bool },
    /// [`SpillTier::would_accept`] returned [`Admission::RefusedPending`].
    /// `deficit` is how many more reserved bytes would have covered it.
    Pending { deficit: u32 },
    /// No tier configured, or the victim raced away, removed or not
    /// resident anymore, between sampling and this call. The ordinary
    /// remove-and-XOR path runs exactly as without this policy.
    NotApplicable,
}

/// The outcome of [`apply_locked`]: the caller's `key` back plus what changed,
/// to build an [`super::Event`] and decide on fan-out.
pub(crate) enum ApplyOutcome<K, V> {
    /// `incoming` loses to what is already stored; nothing changes.
    Rejected,
    /// Writes a value. `created` is `false` when the value replaces an
    /// existing live entry.
    Put { key: K, value: V, created: bool },
    /// Writes a tombstone, replacing whatever, live entry or nothing,
    /// preceded it. Unlike `Put`'s `created`, this carries no
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

/// Reads a currently-[`EntryState::Spilled`] stored record's value bytes off
/// disk, so [`resolve_conflict`]'s collision decision can fold a
/// value-aware resolver against them instead of degrading to its
/// value-less guard. Called off-lock by [`prefetch_spilled_conflict_bytes`]
/// for the overwhelming majority of calls, and under the stripe write lock
/// by [`apply_locked`] itself on the rare fallback where prefetch missed
/// this pointer, since losing a real value to a stale prefetch would
/// violate [`resolve_conflict`]'s never-drop-a-real-value guarantee.
///
/// `None` on anything [`SpillTier::read_at`] treats as an ordinary outcome
/// (a torn write, a failed checksum, a rotated-past generation, a genuine
/// I/O error): degrades like a value-less stored side, never panics.
#[cfg(feature = "spill")]
fn read_spilled_for_conflict(tier: &SpillTier, loc: SpillLoc) -> Option<Bytes> {
    tier.read_at(loc)
        .ok()
        .flatten()
        .map(|spilled| spilled.encoded)
}

/// Reads back, off any stripe lock, the value bytes of every currently-
/// [`EntryState::Spilled`] stored record among `keys` a merge might need,
/// feeding both [`peek_stored_seed`]'s pre-fold seeding and
/// [`apply_locked`]'s stored-side lookup; see [`read_spilled_for_conflict`]
/// for why a fallback under the write lock still exists. A brief stripe
/// read lock finds which keys are spilled, then every read happens off-lock,
/// keyed by [`SpillLoc`] so a moved entry misses rather than serving stale
/// content. Skips the read lock entirely when `resolver` never inspects
/// stored bytes ([`ConflictResolver::needs_value_bytes`] is `false`) or no
/// tier is attached.
#[cfg(feature = "spill")]
fn prefetch_spilled_conflict_bytes<'a, K, V>(
    stripe_lock: &RwLock<Stripe<K, V>>,
    keys: impl IntoIterator<Item = (u64, &'a Bytes)>,
    resolver: &dyn ConflictResolver,
    spill: Option<&SpillTier>,
) -> HashMap<SpillLoc, Bytes> {
    let Some(tier) = spill.filter(|_| resolver.needs_value_bytes()) else {
        return HashMap::new();
    };
    let mut locs: std::collections::HashSet<SpillLoc> = std::collections::HashSet::new();
    {
        let stripe = stripe_lock.read();
        for (hash, key_bytes) in keys {
            if stripe.tombstones.contains_key(key_bytes.as_ref()) {
                continue;
            }
            if let Some(live) = stripe
                .live
                .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
                && let EntryState::Spilled(loc) = &live.state
            {
                locs.insert(*loc);
            }
        }
    }
    locs.into_iter()
        .filter_map(|loc| read_spilled_for_conflict(tier, loc).map(|bytes| (loc, bytes)))
        .collect()
}

/// One entry's identity for the versioned-apply path, in place of three positional parameters.
pub(crate) struct EntryKey<K> {
    hash: u64,
    key: K,
    key_bytes: Bytes,
}

/// An [`Incoming::Put`]'s three fields, bundled for [`apply_put`].
pub(crate) struct PutWrite<V> {
    value: V,
    expires_at_ms: Option<u64>,
    encoded: Bytes,
}

/// Borrowed engine-wide state one versioned-apply call needs, in place of a dozen positional parameters.
pub(crate) struct ApplyCtx<'a, K, V> {
    /// Already resolved for this entry's part; a batch apply builds a fresh one per entry.
    digest_bucket: &'a AtomicU64,
    total_weight: &'a AtomicU64,
    live_count: &'a AtomicU64,
    weigher: Option<&'a Weigher<K, V>>,
    tti_ms: Option<u64>,
    resolver: &'a dyn ConflictResolver,
    #[cfg(feature = "spill")]
    prefetched_spilled: &'a HashMap<SpillLoc, Bytes>,
    #[cfg(feature = "spill")]
    spill: Option<&'a SpillTier>,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    now_ms: u64,
}

/// What [`Engine::complete_fresh_load`] did with a fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FillOutcome {
    /// The loaded value is installed; `had_live` says whether it replaced a
    /// live entry.
    Installed { had_live: bool },
    /// A write or removal at least as new as the fill landed while the
    /// loader ran, and stays in place of the loaded value.
    Superseded,
}

/// Whether a fill stamped `fill` replaces what its key holds, `existing`
/// being the version of the tombstone or live entry there, if any. A fill
/// is stamped before its loader runs, so a write or removal that lands
/// during the load is newer and wins, the same versioned-apply rule every
/// other write follows. Pure; unit tested directly.
fn fill_supersedes(existing: Option<Hlc>, fill: Hlc) -> bool {
    existing.is_none_or(|current| current < fill)
}

/// A successful [`super::Shard::get_or_load`] fill's write, for [`Engine::complete_fresh_load`].
pub(crate) struct FreshLoad<'a, K, V> {
    pub(crate) key: &'a K,
    pub(crate) key_bytes: &'a Bytes,
    pub(crate) hash: u64,
    pub(crate) ver: Hlc,
    pub(crate) value: V,
    pub(crate) encoded: Bytes,
    pub(crate) expires_at_ms: Option<u64>,
}

/// One [`Engine::compact_replace_if_current`] call's write.
pub(crate) struct CompactReplace<'a, K, V> {
    pub(crate) key: &'a K,
    pub(crate) key_bytes: &'a [u8],
    pub(crate) hash: u64,
    pub(crate) expected_ver: Hlc,
    pub(crate) new_ver: Hlc,
    pub(crate) value: V,
    pub(crate) encoded: Bytes,
}

/// The versioned-apply core: applies `incoming` at `ver` for `key` iff
/// `resolver` picks it over whatever `stripe` holds, updating
/// `digest_bucket`, `total_weight`, and `live_count` to match. A collision
/// may fold both sides via [`ConflictResolver::merge`], rebinding
/// `ver`/`incoming` to the merged version ([`merge_version`]) and bytes
/// before anything downstream runs, so a merge stores and fingerprints like
/// any other `Put`. Synchronous under the caller's write lock on `stripe`;
/// no disk I/O of its own. `prefetched_spilled` (`spill` builds) is
/// [`prefetch_spilled_conflict_bytes`]'s off-lock read of the stored side's
/// bytes; a lookup miss falls back to [`read_spilled_for_conflict`] under
/// this lock rather than treating the side as value-less. Returns whether
/// this displaced a [`EntryState::Spilled`] entry (`false` for `Rejected`);
/// see [`Engine::note_spill_departure`].
pub(crate) fn apply_locked<K, V>(
    stripe: &mut Stripe<K, V>,
    ctx: &ApplyCtx<'_, K, V>,
    entry: EntryKey<K>,
    mut ver: Hlc,
    mut incoming: Incoming<V>,
) -> (ApplyOutcome<K, V>, bool)
where
    K: Hash + Eq + Clone,
    V: Clone + DeserializeOwned,
{
    let EntryKey {
        hash,
        key,
        key_bytes,
    } = entry;
    let prior_tombstone = stripe.tombstones.get(key_bytes.as_ref()).copied();
    // `visible` is what a read at `now_ms` would see: an expired or idle
    // entry still takes part in conflict resolution and still gets displaced,
    // but a write over it counts as a creation, the same as after a sweep.
    let stored_live = if prior_tombstone.is_none() {
        stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            .map(|l| {
                // A currently-spilled entry has no value bytes resident in
                // RAM; see this function's doc for `prefetched_spilled` and
                // the off-lock/under-lock read split.
                #[cfg(feature = "spill")]
                let encoded = match &l.state {
                    EntryState::Resident => Some(record_value_bytes(&l.record)),
                    EntryState::Spilled(loc) => {
                        ctx.prefetched_spilled.get(loc).cloned().or_else(|| {
                            ctx.spill
                                .filter(|_| ctx.resolver.needs_value_bytes())
                                .and_then(|tier| read_spilled_for_conflict(tier, *loc))
                        })
                    }
                };
                #[cfg(not(feature = "spill"))]
                let encoded = Some(record_value_bytes(&l.record));
                let kb = key_bytes.as_ref();
                (
                    l.ver(),
                    encoded,
                    decode_expiry(l.wall_ms, l.expiry, kb, &stripe.long_ttl),
                    !absent_at(l, kb, &stripe.long_ttl, ctx.tti_ms, ctx.now_ms),
                )
            })
    } else {
        None
    };
    let stored_ver = prior_tombstone
        .map(|t| t.ver)
        .or_else(|| stored_live.as_ref().map(|(v, _, _, _)| *v));

    if let Some(sv) = stored_ver {
        let stored_encoded = stored_live
            .as_ref()
            .and_then(|(_, enc, _, _)| enc.as_deref());
        let stored_expires_at_ms = stored_live.as_ref().and_then(|(_, _, e, _)| *e);
        match resolve_and_rebind(
            ctx.resolver,
            key_bytes.as_ref(),
            sv,
            stored_encoded,
            stored_expires_at_ms,
            ver,
            incoming,
        ) {
            None => return (ApplyOutcome::Rejected, false),
            Some((new_ver, new_incoming)) => {
                ver = new_ver;
                incoming = new_incoming;
            }
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
        ctx.digest_bucket.fetch_xor(
            entry_fingerprint(key_bytes.as_ref(), t.ver),
            Ordering::Relaxed,
        );
        stripe.tombstones.remove(key_bytes.as_ref());
    } else if let Some(sv) = stored_ver {
        ctx.digest_bucket
            .fetch_xor(entry_fingerprint(key_bytes.as_ref(), sv), Ordering::Relaxed);
    }
    ctx.digest_bucket.fetch_xor(new_fp, Ordering::Relaxed);

    let entry = EntryKey {
        hash,
        key,
        key_bytes,
    };
    match incoming {
        Incoming::Put {
            value,
            expires_at_ms,
            encoded,
        } => apply_put(
            stripe,
            ctx,
            entry,
            ver,
            PutWrite {
                value,
                expires_at_ms,
                encoded,
            },
            had_live,
            was_visible,
        ),
        Incoming::Tombstone => apply_tombstone(stripe, ctx, entry, ver, had_live),
    }
}

/// The `Incoming::Put` half of [`apply_locked`]'s write: installs the new
/// value, corrects total weight for whatever it displaced (`had_live`), bumps
/// `live_count` iff nothing physically live occupied the key before, and
/// reports `created` unless a readable entry (`was_visible`) is replaced.
/// The returned `bool` is whether the displaced entry, if any, held a
/// spilled payload. An overwrite of a spilled key always installs
/// fresh as resident, so this is the only place such an entry departs
/// without a matching promotion.
fn apply_put<K, V>(
    stripe: &mut Stripe<K, V>,
    ctx: &ApplyCtx<'_, K, V>,
    entry: EntryKey<K>,
    ver: Hlc,
    put: PutWrite<V>,
    had_live: bool,
    was_visible: bool,
) -> (ApplyOutcome<K, V>, bool)
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    let EntryKey {
        hash,
        key,
        key_bytes,
    } = entry;
    let PutWrite {
        value,
        expires_at_ms,
        encoded,
    } = put;
    let weight = ctx.weigher.map_or(1, |w| w(&key, &value));
    let removed = if had_live {
        remove_live(stripe, hash, key_bytes.as_ref())
    } else {
        None
    };
    let displaced_spilled = removed.as_ref().is_some_and(|r| r.was_spilled);
    let old_weight = removed.map(|r| r.weight);
    // `remove_live` cleared any stale `long_ttl` row this write displaces;
    // only a TTL past the delta's range needs a fresh one.
    let (expiry, long_ttl_value) = encode_expiry(ver.wall_ms, expires_at_ms);
    if let Some(deadline) = long_ttl_value {
        stripe.long_ttl.insert(key_bytes.clone(), deadline);
    }
    // A fresh write always installs resident: spilling only ever happens
    // through sampled eviction, never directly on a write.
    stripe.live.insert_unique(
        hash,
        Live::new(
            build_resident_record(key_bytes.as_ref(), encoded.as_ref()),
            ver,
            expiry,
            weight,
            ctx.now_ms,
            #[cfg(feature = "spill")]
            EntryState::Resident,
        ),
        hasher_for,
    );
    if let Some(exp) = expires_at_ms {
        stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
    }
    ctx.total_weight
        .fetch_add(u64::from(weight), Ordering::Relaxed);
    if let Some(ow) = old_weight {
        ctx.total_weight.fetch_sub(u64::from(ow), Ordering::Relaxed);
    } else {
        ctx.live_count.fetch_add(1, Ordering::Relaxed);
    }
    (
        ApplyOutcome::Put {
            key,
            value,
            created: !was_visible,
        },
        displaced_spilled,
    )
}

/// The `Incoming::Tombstone` half of [`apply_locked`]'s write: removes the
/// displaced live entry from total weight and `live_count`, then records the
/// tombstone with its two GC deadlines. The returned `bool` is whether the
/// displaced entry, if any, held a spilled payload.
fn apply_tombstone<K, V>(
    stripe: &mut Stripe<K, V>,
    ctx: &ApplyCtx<'_, K, V>,
    entry: EntryKey<K>,
    ver: Hlc,
    had_live: bool,
) -> (ApplyOutcome<K, V>, bool)
where
    K: Hash + Eq,
{
    let EntryKey {
        hash,
        key,
        key_bytes,
    } = entry;
    let mut displaced_spilled = false;
    if had_live && let Some(removed) = remove_live(stripe, hash, key_bytes.as_ref()) {
        ctx.total_weight
            .fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
        ctx.live_count.fetch_sub(1, Ordering::Relaxed);
        displaced_spilled = removed.was_spilled;
    }
    stripe.tombstones.insert(
        key_bytes,
        Tombstone {
            ver,
            ttl_deadline_ms: ctx.now_ms.saturating_add(ctx.tombstone_ttl_ms),
            max_deadline_ms: ctx.now_ms.saturating_add(ctx.tombstone_max_ttl_ms),
        },
    );
    (ApplyOutcome::Tombstoned { key }, displaced_spilled)
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
    live_count: AtomicU64,
    max_capacity: u64,
    /// The configured time-to-idle, clamped by [`clamp_tti_ms`].
    tti_ms: Option<u64>,
    weigher: Option<Weigher<K, V>>,
    evict_cursor: AtomicU64,
    /// The local SSD/NVMe spill tier, once attached by
    /// [`Engine::set_spill`]. Unset until then, and always unset in a
    /// non-`spill` build. A `OnceLock`, not a plain `Option` behind
    /// `&mut self`, so [`super::Shard::attach_spill`] can attach a tier to
    /// an engine that is already `Arc`-shared and registered. The shard
    /// registry reservation wins before any disk I/O runs, and that lets
    /// the tier attach afterward without needing exclusive access.
    #[cfg(feature = "spill")]
    spill: OnceLock<Arc<SpillTier>>,
    /// Handle for `sundog_spill_entries{cache}`, created once in
    /// [`Engine::set_spill`] for the same reason `Shard::hits`/
    /// `Shard::misses` are: label resolution costs more than the paths
    /// that touch this gauge can afford per call. Those paths are install,
    /// promote, reclaim, and every write or removal that displaces a
    /// [`EntryState::Spilled`] entry from `live`.
    #[cfg(feature = "spill")]
    spill_entries_gauge: OnceLock<metrics::Gauge>,
    /// Test-only mirror of `spill_entries_gauge`'s value, updated in
    /// lockstep everywhere the gauge is. The `metrics` crate's default
    /// recorder is a silent no-op with nothing for a unit test to read
    /// back, so this pub(crate) counter gives engine tests something to
    /// assert against without installing a real Prometheus recorder.
    #[cfg(all(feature = "spill", test))]
    spill_entries_test_count: AtomicI64,
    /// Weight already zeroed out of `total_weight` by a spill hand-off,
    /// [`Engine::evict_one_sampled`]/[`Engine::evict_batch_sampled`]
    /// committing to [`VictimOutcome::PendingSpill`], but not yet
    /// resolved: the entry stays fully resident in RAM until
    /// [`SpillSink::install`] flips it to [`EntryState::Spilled`], which
    /// subtracts its share back out, or [`SpillSink::abandon`] restores it
    /// to `total_weight` instead. `Engine::enforce_capacity`'s over-cap
    /// check adds this to `total_weight`, so RAM a lagging flusher hasn't
    /// caught up on still counts against the cap; see the doc on that
    /// method. Always `0` in a non-`spill` build, which never creates a
    /// hand-off in the first place. Every job carries its own weight, and
    /// `install` and `abandon` release exactly that amount whether or not
    /// the entry still matches, so a write, tombstone, or expiry that
    /// displaces a pending key strands nothing here.
    #[cfg(feature = "spill")]
    pending_spill_weight: AtomicU64,
    /// Whether [`Engine::apply_many`] pre-folds a batch by key ahead of the
    /// stripe lock, for a resolver whose [`ConflictResolver::merges`] is
    /// `true`. `true` by default; [`Engine::set_prefold_enabled`] is the
    /// only way to turn it off, to compare pre-fold's effect on throughput
    /// against the unfolded per-record path it otherwise always takes. Has
    /// no effect at all on a resolver that doesn't merge; see
    /// `apply_many`'s docs.
    prefold_enabled: AtomicBool,
    /// Where [`Engine::compact`]'s next call resumes: the index of the next
    /// stripe it has not yet visited, so consecutive ticks rotate through
    /// every stripe in turn instead of always starting over at stripe `0`.
    /// Read and stored as a plain stripe index (always `< BUCKET_COUNT`),
    /// which [`stripe_index_from_hash`]'s mask leaves unchanged: the same
    /// helper `evict_cursor` uses, reused here for a value that is already
    /// a valid index rather than a hash needing one. Starts at `0`; unlike
    /// `evict_cursor` this is a rotation pointer, not a PRNG state, so a
    /// zero seed is fine.
    compact_cursor: AtomicU64,
    #[cfg(test)]
    eviction_lock_acquisitions: AtomicU64,
    /// Test-only: how many stripe read locks [`Engine::compact`] has taken
    /// across its whole run, one per stripe visited, never more than one
    /// held at a time since each is dropped before the next is taken.
    #[cfg(test)]
    compact_lock_acquisitions: AtomicU64,
}

/// [`Engine::finalize_checkpoint_writes`]'s outcome for one entry.
#[cfg(feature = "spill")]
enum FinalizeWriteOutcome {
    /// Still `Resident` at its captured version and not expired: flipped
    /// to `Spilled` and kept.
    Kept(u32),
    /// The key gained a tombstone between capture and this call.
    DroppedTombstoned,
    /// No longer present (removed, or its tombstone already collected).
    DroppedGone,
    /// The version no longer matches what was captured, or it's no longer
    /// `Resident`.
    DroppedVersionChanged,
    /// The live entry is now past its expiry.
    DroppedExpired,
}

/// Per-reason breakdown [`Engine::finalize_checkpoint_writes`] returns
/// alongside its survivors.
#[cfg(feature = "spill")]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CheckpointFinalizeCounts {
    pub(crate) kept: u64,
    pub(crate) dropped_tombstoned: u64,
    pub(crate) dropped_gone: u64,
    pub(crate) dropped_version_changed: u64,
    pub(crate) dropped_expired: u64,
}

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// `capacity_hint`, when `Some`, presizes every stripe for
    /// `ceil(hint / BUCKET_COUNT)` entries, so a cold fill never grows.
    pub(crate) fn new(
        max_capacity: u64,
        tti: Option<Duration>,
        weigher: Option<Weigher<K, V>>,
        capacity_hint: Option<u64>,
    ) -> Self {
        let per_stripe_capacity = capacity_hint.map(|hint| {
            usize::try_from(hint.div_ceil(BUCKET_COUNT as u64))
                .expect("invariant: a capacity hint divided by BUCKET_COUNT fits in usize")
        });
        Self {
            stripes: (0..BUCKET_COUNT)
                .map(|_| {
                    RwLock::new(match per_stripe_capacity {
                        Some(capacity) => Stripe::with_capacity(capacity),
                        None => Stripe::new(),
                    })
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            digest: (0..BUCKET_COUNT * PART_COUNT)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            total_weight: AtomicU64::new(0),
            live_count: AtomicU64::new(0),
            max_capacity,
            tti_ms: tti.map(|d| clamp_tti_ms(super::duration_ms(d))),
            weigher,
            // Any nonzero seed; xorshift64* never recovers from a zero state.
            evict_cursor: AtomicU64::new(0x9E37_79B9_7F4A_7C15),
            #[cfg(feature = "spill")]
            spill: OnceLock::new(),
            #[cfg(feature = "spill")]
            spill_entries_gauge: OnceLock::new(),
            #[cfg(all(feature = "spill", test))]
            spill_entries_test_count: AtomicI64::new(0),
            #[cfg(feature = "spill")]
            pending_spill_weight: AtomicU64::new(0),
            prefold_enabled: AtomicBool::new(true),
            compact_cursor: AtomicU64::new(0),
            #[cfg(test)]
            eviction_lock_acquisitions: AtomicU64::new(0),
            #[cfg(test)]
            compact_lock_acquisitions: AtomicU64::new(0),
        }
    }

    /// Attaches `tier` as this engine's spill tier and creates its
    /// `sundog_spill_entries{cache}` gauge handle. Called once, by
    /// [`super::Shard::attach_spill`], through `&self`: `spill` and
    /// `spill_entries_gauge` are `OnceLock`s so this can run after the
    /// engine is already `Arc`-shared. It attaches only once this shard
    /// has won its name in the cluster's shard registry.
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same engine.
    #[cfg(feature = "spill")]
    pub(crate) fn set_spill(&self, tier: Arc<SpillTier>) {
        self.spill_entries_gauge
            .set(metrics::gauge!(
                "sundog_spill_entries",
                "cache" => tier.cache_name().to_string(),
            ))
            .unwrap_or_else(|_| panic!("invariant: set_spill runs at most once per engine"));
        self.spill
            .set(tier)
            .unwrap_or_else(|_| panic!("invariant: set_spill runs at most once per engine"));
    }

    /// Decrements `sundog_spill_entries{cache}` by `count`, plus the
    /// test-only mirror counter, iff a spill tier's gauge is attached.
    /// Always a no-op in a non-`spill` build. The counterpart to
    /// [`Engine::note_spill_arrival`]; every write or removal path that
    /// takes a [`EntryState::Spilled`] entry out of `live` calls this, or
    /// [`Engine::note_spill_departure`], its one-entry shorthand, so the
    /// gauge never drifts from how many entries are spilled.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_self,
            reason = "the gauge this decrements only exists under feature = \"spill\""
        )
    )]
    fn note_spill_departures(&self, count: usize) {
        #[cfg(feature = "spill")]
        {
            if count == 0 {
                return;
            }
            if let Some(gauge) = self.spill_entries_gauge.get() {
                gauge.decrement(count_f64(count));
            }
            #[cfg(test)]
            self.spill_entries_test_count
                .fetch_sub(i64::try_from(count).unwrap_or(i64::MAX), Ordering::Relaxed);
        }
        #[cfg(not(feature = "spill"))]
        {
            let _ = count;
        }
    }

    /// [`Engine::note_spill_departures`] for the common case: a write or
    /// removal that displaces at most one live entry.
    fn note_spill_departure(&self, was_spilled: bool) {
        self.note_spill_departures(usize::from(was_spilled));
    }

    /// Increments `sundog_spill_entries{cache}`, plus the test-only mirror
    /// counter. The counterpart to [`Engine::note_spill_departures`], called
    /// wherever a `live` entry newly becomes [`EntryState::Spilled`].
    #[cfg(feature = "spill")]
    fn note_spill_arrival(&self) {
        if let Some(gauge) = self.spill_entries_gauge.get() {
            gauge.increment(1.0);
        }
        #[cfg(test)]
        self.spill_entries_test_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// This engine's spill tier, once [`Engine::set_spill`] has run.
    #[cfg(feature = "spill")]
    pub(crate) fn spill(&self) -> Option<&Arc<SpillTier>> {
        self.spill.get()
    }

    /// `pending_spill_weight`'s current value, for
    /// [`Engine::enforce_capacity`]'s over-cap check: RAM a spill hand-off
    /// has already zeroed out of `total_weight` but that a lagging flusher
    /// has not yet freed. Always `0` in a non-`spill` build, which never
    /// creates a hand-off, so `enforce_capacity`'s behavior there is
    /// unaffected.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_self,
            reason = "the field this reads only exists under feature = \"spill\""
        )
    )]
    fn pending_spill_weight_or_zero(&self) -> u64 {
        #[cfg(feature = "spill")]
        {
            self.pending_spill_weight.load(Ordering::Relaxed)
        }
        #[cfg(not(feature = "spill"))]
        {
            0
        }
    }

    /// [`Engine::pending_spill_weight_or_zero`], for tests: this engine's
    /// current `pending_spill_weight`, only ever nonzero once a spill
    /// hand-off has zeroed a victim's weight and before its flusher
    /// install or abandon resolves it.
    #[cfg(all(feature = "spill", test))]
    pub(crate) fn debug_pending_spill_weight(&self) -> u64 {
        self.pending_spill_weight_or_zero()
    }

    fn is_absent(
        &self,
        live: &Live<K, V>,
        key_bytes: &[u8],
        long_ttl: &HashMap<Bytes, u64>,
        now_ms: u64,
    ) -> bool {
        absent_at(live, key_bytes, long_ttl, self.tti_ms, now_ms)
    }

    /// Whether a read updates `last_access_ms`, only when it is consulted for
    /// TTI or sampled for capacity eviction.
    fn tracks_last_access(&self) -> bool {
        self.tti_ms.is_some() || self.max_capacity != u64::MAX
    }

    fn touch(&self, live: &Live<K, V>, now_ms: u64) {
        if self.tracks_last_access() {
            live.last_access_ms
                .store(touch_stamp(now_ms), Ordering::Relaxed);
        }
    }

    /// Reads `key`: a stripe read lock, a hashbrown lookup by the key's
    /// postcard-encoded bytes, and a value decode. No mutation beyond the
    /// recency touch, when configured.
    pub(crate) fn get(&self, key: &K, now_ms: u64) -> Option<V> {
        let key_buf = encode_key_for_read(key).ok()?;
        let key_bytes = key_buf.as_slice();
        self.get_by_bytes(key_bytes, hash_key_bytes(key_bytes), now_ms)
    }

    /// [`Engine::get`] for a key already encoded and hashed, so a caller that
    /// holds both, as the `get_or_load` loop does, skips re-encoding.
    ///
    /// `None` for a currently-[`EntryState::Spilled`] entry too. A spill-aware
    /// caller, `get`/`get_or_load`, checks [`Engine::spilled_loc`] next; a
    /// spill-blind one, `get_sync`, treats this like a miss, per its
    /// documented contract.
    pub(crate) fn get_by_bytes(&self, key_bytes: &[u8], hash: u64, now_ms: u64) -> Option<V> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        if !is_resident(live) {
            return None;
        }
        self.touch(live, now_ms);
        Some(decode_value(record_value(live.record.as_slice())))
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
            .find(hash, |l| record_key(&l.record) == key_bytes)
        else {
            return false;
        };
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
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
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .map(|live| decode_key(record_key(&live.record))),
            );
        }
        out
    }

    /// [`Engine::keys`] one stripe at a time: each stripe's live keys are
    /// cloned under its read lock, then `f` runs on them with no lock held.
    pub(crate) fn for_each_key(&self, now_ms: u64, mut f: impl FnMut(K)) {
        for stripe_lock in &self.stripes {
            let stripe_keys: Vec<K> = {
                let stripe = stripe_lock.read();
                stripe
                    .live
                    .iter()
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .map(|live| decode_key(record_key(&live.record)))
                    .collect()
            };
            for key in stripe_keys {
                f(key);
            }
        }
    }

    /// The full [`WireRecord`] for `key_bytes`, present entry or tombstone
    /// alike. `None` for a currently-spilled entry: the fan-out records path
    /// this feeds skips it, correct because a peer's next anti-entropy
    /// round repairs it.
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
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        is_resident(live).then(|| WireRecord {
            key: Bytes::copy_from_slice(key_bytes),
            value: Some(record_value_bytes(&live.record)),
            ver: live.ver(),
            expires_at_ms: decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl),
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
                        .filter(|live| {
                            !self.is_absent(
                                live,
                                record_key(&live.record),
                                &stripe.long_ttl,
                                now_ms,
                            )
                        })
                        .map(|live| KeyVersion {
                            key: record_key_bytes(&live.record),
                            version: live.ver(),
                        }),
                );
                entries.extend(stripe.tombstones.iter().map(|(key_bytes, t)| KeyVersion {
                    key: key_bytes.clone(),
                    version: t.ver,
                }));
                (bucket, entries)
            })
            .collect()
    }

    /// [`Engine::collect_buckets`] at part granularity: a [`KeyVersion`] for
    /// every live entry and un-GC'd tombstone in each requested
    /// [`BucketPart`], one stripe read lock per distinct bucket in `wanted`.
    /// An out-of-range bucket ([`BUCKET_COUNT`] or past) or part
    /// ([`PART_COUNT`] or past) is skipped rather than indexed.
    pub(crate) fn collect_parts(&self, wanted: &[BucketPart], now_ms: u64) -> PartEntries {
        let mut by_bucket: std::collections::BTreeMap<u16, Vec<u8>> =
            std::collections::BTreeMap::new();
        for &BucketPart { bucket, part } in wanted {
            if usize::from(bucket) >= BUCKET_COUNT || usize::from(part) >= PART_COUNT {
                continue;
            }
            by_bucket.entry(bucket).or_default().push(part);
        }
        let mut out = Vec::new();
        for (bucket, mut parts) in by_bucket {
            parts.sort_unstable();
            parts.dedup();
            // One pass over the stripe, hashing each key once, routing every
            // entry to its part's slot; a bucket's 64 parts cost one listing.
            let mut slot_of_part = [usize::MAX; PART_COUNT];
            for (slot, &part) in parts.iter().enumerate() {
                slot_of_part[usize::from(part)] = slot;
            }
            let mut per_part: Vec<Vec<KeyVersion>> = vec![Vec::new(); parts.len()];
            let stripe = self.stripes[usize::from(bucket)].read();
            let live_entries = stripe
                .live
                .iter()
                .filter(|live| {
                    !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                })
                .map(|live| (record_key_bytes(&live.record), live.ver()));
            let tombstone_entries = stripe
                .tombstones
                .iter()
                .map(|(key_bytes, t)| (key_bytes.clone(), t.ver));
            for (key_bytes, ver) in live_entries.chain(tombstone_entries) {
                let slot = slot_of_part[part_index_from_hash(hash_key_bytes(key_bytes.as_ref()))];
                if slot != usize::MAX {
                    per_part[slot].push(KeyVersion {
                        key: key_bytes.clone(),
                        version: ver,
                    });
                }
            }
            drop(stripe);
            out.extend(
                parts
                    .into_iter()
                    .zip(per_part)
                    .map(|(part, entries)| (BucketPart { bucket, part }, entries)),
            );
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

    /// Every stripe's current `live` arena capacity, in stripe order.
    pub(crate) fn stripe_capacities(&self) -> Vec<usize> {
        self.stripes
            .iter()
            .map(|s| s.read().live.capacity())
            .collect()
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

    /// Every resident live entry and tombstone as [`WireRecord`]s, for
    /// [`super::ShardOps::snapshot_chunks`]. A currently-spilled entry is
    /// never included here: see [`Engine::snapshot_spilled`], its sibling,
    /// for the pointers a spill-aware caller reads off-lock and folds in.
    ///
    pub(crate) fn snapshot_records(&self, now_ms: u64) -> Vec<WireRecord> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .filter(|&live| is_resident(live))
                    .map(|live| WireRecord {
                        key: record_key_bytes(&live.record),
                        value: Some(record_value_bytes(&live.record)),
                        ver: live.ver(),
                        expires_at_ms: decode_expiry(
                            live.wall_ms,
                            live.expiry,
                            record_key(&live.record),
                            &stripe.long_ttl,
                        ),
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

    /// [`Engine::snapshot_records`]'s sibling: every currently-spilled live
    /// entry's pointer, `(key_bytes, ver, expires_at_ms, loc)`, snapshotted
    /// under each stripe's read lock alongside `snapshot_records`' pass. A
    /// spill-aware caller reads these off-lock, via `spawn_blocking` behind
    /// the tier's read semaphore, and folds the results into the snapshot,
    /// dropping any whose read comes back `None`.
    #[cfg(feature = "spill")]
    pub(crate) fn snapshot_spilled(&self, now_ms: u64) -> Vec<SpilledPointer> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .filter_map(|live| match &live.state {
                        EntryState::Spilled(loc) => Some((
                            record_key_bytes(&live.record),
                            live.ver(),
                            decode_expiry(
                                live.wall_ms,
                                live.expiry,
                                record_key(&live.record),
                                &stripe.long_ttl,
                            ),
                            *loc,
                        )),
                        EntryState::Resident => None,
                    }),
            );
        }
        out
    }

    /// A warm-reopen checkpoint's other half: every currently-resident live
    /// entry's key, version, expiry, and encoded value bytes, snapshotted
    /// under each stripe's read lock like [`Engine::snapshot_spilled`], but
    /// for the entries that method skips. `Shard::close_spill_checkpointed`
    /// hands this to `SpillTier::checkpoint_flush`, so a clean close can
    /// warm-reopen every live entry, not only ones already spilled.
    #[cfg(feature = "spill")]
    pub(crate) fn checkpoint_resident_entries(
        &self,
        now_ms: u64,
    ) -> Vec<(Bytes, Hlc, Option<u64>, Bytes)> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .filter(|live| is_resident(live))
                    .map(|live| {
                        (
                            record_key_bytes(&live.record),
                            live.ver(),
                            decode_expiry(
                                live.wall_ms,
                                live.expiry,
                                record_key(&live.record),
                                &stripe.long_ttl,
                            ),
                            record_value_bytes(&live.record),
                        )
                    }),
            );
        }
        out
    }

    /// Re-validates every entry [`SpillTier::checkpoint_flush`] just wrote
    /// to disk against the live engine state, under each key's own stripe
    /// write lock, before `Shard::close_spill_checkpointed` folds the
    /// survivors into the final checkpoint snapshot.
    ///
    /// `checkpoint_flush` writes with no version check, since these entries
    /// never went through the ordinary pending-spill hand-off, leaving a
    /// window for a racing `Cache` clone to remove, tombstone, or overwrite
    /// the key before this call. Keeps only entries still `Resident` at
    /// exactly the captured version with no tombstone, flips each survivor
    /// to [`EntryState::Spilled`] in the same lock (dropping `total_weight`
    /// by its weight, zeroing the entry's own weight), and leaves
    /// `pending_spill_weight` untouched since these writes never entered
    /// that accounting. `now_ms` is the timestamp already captured before
    /// this call, so an entry that expired during the write is excluded.
    ///
    /// Returns the survivors with a [`CheckpointFinalizeCounts`] breakdown
    /// by drop reason, for `Shard::close_spill_checkpointed` to log and
    /// meter.
    #[cfg(feature = "spill")]
    pub(crate) fn finalize_checkpoint_writes(
        &self,
        entries: Vec<SpilledPointer>,
        now_ms: u64,
    ) -> (Vec<SpilledPointer>, CheckpointFinalizeCounts) {
        let mut survivors = Vec::with_capacity(entries.len());
        let mut counts = CheckpointFinalizeCounts::default();
        for (key_bytes, ver, expires_at_ms, loc) in entries {
            let hash = hash_key_bytes(key_bytes.as_ref());
            let stripe_idx = stripe_index_from_hash(hash);
            let outcome = {
                let mut guard = self.stripes[stripe_idx].write();
                // One explicit reborrow: see `sweep`'s identical comment.
                let stripe: &mut Stripe<K, V> = &mut guard;
                if stripe.tombstones.contains_key(key_bytes.as_ref()) {
                    FinalizeWriteOutcome::DroppedTombstoned
                } else if let Some(live) = stripe
                    .live
                    .find_mut(hash, |l| record_key(&l.record) == key_bytes.as_ref())
                {
                    if live.ver() != ver || !matches!(live.state, EntryState::Resident) {
                        FinalizeWriteOutcome::DroppedVersionChanged
                    } else if self.is_absent(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms) {
                        FinalizeWriteOutcome::DroppedExpired
                    } else {
                        let weight = live.weight;
                        live.record = build_key_only_record(record_key(&live.record));
                        live.state = EntryState::Spilled(loc);
                        live.weight = 0;
                        FinalizeWriteOutcome::Kept(weight)
                    }
                } else {
                    FinalizeWriteOutcome::DroppedGone
                }
            };
            match outcome {
                FinalizeWriteOutcome::Kept(weight) => {
                    self.total_weight
                        .fetch_sub(u64::from(weight), Ordering::Relaxed);
                    self.note_spill_arrival();
                    counts.kept += 1;
                    survivors.push((key_bytes, ver, expires_at_ms, loc));
                }
                FinalizeWriteOutcome::DroppedTombstoned => counts.dropped_tombstoned += 1,
                FinalizeWriteOutcome::DroppedGone => counts.dropped_gone += 1,
                FinalizeWriteOutcome::DroppedVersionChanged => counts.dropped_version_changed += 1,
                FinalizeWriteOutcome::DroppedExpired => counts.dropped_expired += 1,
            }
        }
        (survivors, counts)
    }

    /// [`Engine::record_for`] for many keys in one pass, but reporting a
    /// currently-spilled entry's pointer instead of treating it as absent.
    /// The AE-pull-reply path, `ShardOps::records_for`, reads the spilled
    /// half off-lock, via `spawn_blocking` behind the tier's read
    /// semaphore, and folds any successful read back in as a `WireRecord`,
    /// dropping the rest. Nothing here promotes; a served-from-disk record
    /// leaves `record` unchanged.
    #[cfg(feature = "spill")]
    pub(crate) fn records_for_or_spilled(
        &self,
        keys: &[Bytes],
        now_ms: u64,
    ) -> (Vec<WireRecord>, Vec<SpilledPointer>) {
        let mut records = Vec::new();
        let mut spilled = Vec::new();
        for key_bytes in keys {
            let hash = hash_key_bytes(key_bytes.as_ref());
            let stripe = self.stripes[stripe_index_from_hash(hash)].read();
            if let Some(t) = stripe.tombstones.get(key_bytes.as_ref()) {
                records.push(WireRecord {
                    key: key_bytes.clone(),
                    value: None,
                    ver: t.ver,
                    expires_at_ms: None,
                });
                continue;
            }
            let Some(live) = stripe
                .live
                .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            else {
                continue;
            };
            if self.is_absent(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms) {
                continue;
            }
            match &live.state {
                EntryState::Resident => records.push(WireRecord {
                    key: key_bytes.clone(),
                    value: Some(record_value_bytes(&live.record)),
                    ver: live.ver(),
                    expires_at_ms: decode_expiry(
                        live.wall_ms,
                        live.expiry,
                        key_bytes.as_ref(),
                        &stripe.long_ttl,
                    ),
                }),
                EntryState::Spilled(loc) => {
                    spilled.push((
                        key_bytes.clone(),
                        live.ver(),
                        decode_expiry(
                            live.wall_ms,
                            live.expiry,
                            key_bytes.as_ref(),
                            &stripe.long_ttl,
                        ),
                        *loc,
                    ));
                }
            }
        }
        (records, spilled)
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
            let mut guard = stripe_lock.write();
            // One explicit reborrow, so stripe.live/stripe.long_ttl below
            // are disjoint field projections the borrow checker can split.
            let stripe: &mut Stripe<K, V> = &mut guard;
            let mut removed_weight = 0u64;
            let mut removed_count = 0u64;
            let mut removed_spilled = 0usize;
            let mut new_next = u64::MAX;
            let mut cleared_long_ttl: Vec<Bytes> = Vec::new();
            stripe.live.retain(hasher_for, |live| {
                let key_bytes = record_key(&live.record);
                if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
                    let part = part_index_from_hash(hash_key_bytes(key_bytes));
                    self.digest[digest_slot(idx, part)]
                        .fetch_xor(entry_fingerprint(key_bytes, live.ver()), Ordering::Relaxed);
                    removed_weight += u64::from(live.weight);
                    removed_count += 1;
                    if is_spilled(live) {
                        removed_spilled += 1;
                    }
                    if live.expiry == LONG_TTL {
                        cleared_long_ttl.push(Bytes::copy_from_slice(key_bytes));
                    }
                    false
                } else {
                    if let Some(exp) =
                        decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl)
                    {
                        new_next = new_next.min(exp);
                    }
                    true
                }
            });
            for key_bytes in &cleared_long_ttl {
                stripe.long_ttl.remove(key_bytes.as_ref());
            }
            stripe.next_expiry_ms = new_next;
            drop(guard);
            if removed_weight > 0 {
                self.total_weight
                    .fetch_sub(removed_weight, Ordering::Relaxed);
            }
            if removed_count > 0 {
                self.live_count.fetch_sub(removed_count, Ordering::Relaxed);
            }
            self.note_spill_departures(removed_spilled);
        }
    }

    /// One rate-limited pass of the CRDT writer-retirement sweep: visits
    /// stripes from [`Engine::compact_cursor`] under a read lock each,
    /// calling `resolver.compact(key_bytes, encoded, now_ms, retire, quiet,
    /// bounds)` on every resident live entry and collecting the `Some`
    /// results; never mutates a stripe's live entries, versions, or keys
    /// this way, and [`super::ShardOps::compact_pass`] applies each
    /// candidate separately, via [`Engine::compact_replace_if_current`]. A
    /// [`EntryState::Spilled`] entry is skipped until promoted back to
    /// `Resident`.
    ///
    /// Once a visited stripe's scan finishes, [`should_shrink_stripe`]
    /// decides whether its `live` slab is worth reclaiming (an oversized
    /// `capacity_hint` or a stripe drained by rebalancing): if so, this
    /// pass takes a separate write lock and calls [`Slab::shrink_to_fit`].
    ///
    /// `max_entries` bounds entries examined, not entries eligible; a
    /// stripe already in progress always finishes. The cursor advances past
    /// every visited stripe, wrapping at `BUCKET_COUNT`; `max_entries == 0`
    /// visits nothing. Returns the candidates and how many stripes were
    /// walked.
    pub(crate) fn compact(
        &self,
        resolver: &dyn ConflictResolver,
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: super::CompactionBounds,
        max_entries: usize,
    ) -> CompactScan<K> {
        if max_entries == 0 {
            return CompactScan {
                candidates: Vec::new(),
                stripes_visited: 0,
            };
        }
        let start = stripe_index_from_hash(self.compact_cursor.load(Ordering::Relaxed));
        let mut out = Vec::new();
        let mut examined = 0usize;
        let mut visited = 0usize;
        let mut next_start = start;
        while visited < BUCKET_COUNT {
            let idx = (start + visited) % BUCKET_COUNT;
            visited += 1;
            next_start = (idx + 1) % BUCKET_COUNT;
            let stripe = self.stripes[idx].read();
            self.note_compact_lock_acquisition();
            for live in stripe.live.iter() {
                // Never compacted off-lock: reading its bytes back in would
                // cost a disk read this sweep does not pay for.
                if !is_resident(live) {
                    continue;
                }
                let encoded = record_value_bytes(&live.record);
                examined += 1;
                if let Some(new_encoded) = resolver.compact(
                    record_key(&live.record),
                    encoded.as_ref(),
                    now_ms,
                    retire,
                    quiet,
                    bounds,
                ) {
                    out.push(CompactCandidate {
                        key: decode_key(record_key(&live.record)),
                        key_bytes: record_key_bytes(&live.record),
                        stale_ver: live.ver(),
                        new_encoded,
                    });
                }
            }
            let shrink = should_shrink_stripe(stripe.live.len(), stripe.live.capacity());
            drop(stripe);
            // A separate write lock: reclaiming is rare enough that a
            // second lock here beats holding one for the whole scan.
            if shrink {
                self.stripes[idx].write().live.shrink_to_fit(hasher_for);
            }
            if examined >= max_entries {
                break;
            }
        }
        self.compact_cursor
            .store(next_start as u64, Ordering::Relaxed);
        CompactScan {
            candidates: out,
            stripes_visited: visited,
        }
    }

    /// Replaces a live, resident entry's payload and version with
    /// `replace.value`/`replace.encoded`/`replace.new_ver` iff it is still
    /// exactly at `replace.expected_ver`: the version-gated, no-merge write
    /// [`super::ShardOps::compact_pass`] uses for every [`Self::compact`]
    /// candidate, in place of the ordinary merge-based apply path. A
    /// merge-based apply would silently restore the receipt a compaction
    /// pass pruned, since [`ConflictResolver::merge`] only ever unions
    /// state, so this replaces directly instead. Mints a fresh version from
    /// the compacted bytes ([`super::compacted_version`]) rather than
    /// reusing `expected_ver`, since two different contents must never
    /// share one version; see
    /// `pn_counter_compaction_under_gossip_keeps_the_exact_total` in
    /// `store::prop_tests` for why this still converges. Returns `false`, a
    /// no-op, once the entry has moved on: a different version, not live,
    /// or currently spilled.
    pub(crate) fn compact_replace_if_current(&self, replace: CompactReplace<'_, K, V>) -> bool {
        let CompactReplace {
            key,
            key_bytes,
            hash,
            expected_ver,
            new_ver,
            value,
            encoded,
        } = replace;
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        let (old_weight, new_weight) = {
            let mut guard = self.stripes[bucket].write();
            // One explicit reborrow, as in `sweep`, so the fields below
            // borrow disjointly.
            let stripe: &mut Stripe<K, V> = &mut guard;
            let Some(live) = stripe
                .live
                .find_mut(hash, |l| record_key(&l.record) == key_bytes)
            else {
                return false;
            };
            if live.ver() != expected_ver || !is_resident(live) {
                return false;
            }
            let new_weight = self.weigher.as_ref().map_or(1, |w| w(key, &value));
            let old_weight = live.weight;
            let old_wall_ms = live.wall_ms;
            let old_expiry = live.expiry;
            live.record = build_resident_record(record_key(&live.record), encoded.as_ref());
            live.weight = new_weight;
            live.set_ver(new_ver);
            // new_ver carries a new wall_ms; re-deriving expiry from the
            // real absolute deadline keeps it exact across the version bump.
            let abs_expiry = decode_expiry(old_wall_ms, old_expiry, key_bytes, &stripe.long_ttl);
            let (expiry, long_ttl_value) = encode_expiry(new_ver.wall_ms, abs_expiry);
            live.expiry = expiry;
            match long_ttl_value {
                Some(deadline) => {
                    stripe
                        .long_ttl
                        .insert(Bytes::copy_from_slice(key_bytes), deadline);
                }
                None => {
                    stripe.long_ttl.remove(key_bytes);
                }
            }
            (old_weight, new_weight)
        };
        self.digest[digest_slot(bucket, part)].fetch_xor(
            entry_fingerprint(key_bytes, expected_ver) ^ entry_fingerprint(key_bytes, new_ver),
            Ordering::Relaxed,
        );
        self.total_weight
            .fetch_add(u64::from(new_weight), Ordering::Relaxed);
        self.total_weight
            .fetch_sub(u64::from(old_weight), Ordering::Relaxed);
        true
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn note_compact_lock_acquisition(&self) {
        #[cfg(test)]
        self.compact_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
    }

    /// The number of live entries across every stripe.
    pub(crate) fn live_entry_count(&self) -> u64 {
        self.live_count.load(Ordering::Relaxed)
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

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn note_eviction_lock_acquisition(&self) {
        #[cfg(test)]
        self.eviction_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Whether a configured spill tier commits to taking `victim_bytes`,
    /// found at `hash` in `bucket` with `stripe` already write-locked, in
    /// place of physically removing it. [`SpillAttempt::NotApplicable`] when
    /// no tier is configured or the victim is not [`EntryState::Resident`]
    /// anymore; [`SpillAttempt::Refused`] when
    /// [`SpillTier::would_accept`] declines (too large, closed, flush queue
    /// full), leaving [`Engine::evict_victim_locked`] to decide the
    /// victim's fate from `keep_resident`. On
    /// [`SpillAttempt::Committed`], the weight returned is already zeroed
    /// on the entry in place, and the job is the caller's to hand to
    /// [`Engine::finish_spill_handoff`] once the stripe lock is released;
    /// [`SpillTier::enqueue`] itself runs later, off this lock, and a full
    /// queue found there is handled like any downstream write failure via
    /// [`SpillSink::abandon`].
    #[cfg(feature = "spill")]
    fn try_spill_victim(
        &self,
        stripe: &mut Stripe<K, V>,
        bucket: usize,
        hash: u64,
        victim_bytes: &Bytes,
        reservation: Option<&mut Reservation<'_>>,
    ) -> SpillAttempt {
        let Some(tier) = self.spill() else {
            return SpillAttempt::NotApplicable;
        };
        let Some(live) = stripe
            .live
            .find_mut(hash, |l| record_key(&l.record) == victim_bytes.as_ref())
        else {
            return SpillAttempt::NotApplicable;
        };
        if !matches!(live.state, EntryState::Resident) {
            return SpillAttempt::NotApplicable;
        }
        let encoded = record_value_bytes(&live.record);
        match tier.would_accept(reservation, victim_bytes.len(), encoded.len()) {
            Admission::RefusedFinal => {
                return SpillAttempt::Refused {
                    keep_resident: tier.keep_resident_when_refused(),
                };
            }
            Admission::RefusedPending => {
                return SpillAttempt::Pending {
                    deficit: spill_record_len(victim_bytes.len(), encoded.len()),
                };
            }
            Admission::Accepted => {}
        }
        let weight = live.weight;
        let admitted_bytes = spill_record_len(victim_bytes.len(), encoded.len());
        let job = SpillJob {
            stripe_idx: bucket,
            hash,
            key_bytes: record_key_bytes(&live.record),
            ver: live.ver(),
            expires_at_ms: decode_expiry(
                live.wall_ms,
                live.expiry,
                victim_bytes.as_ref(),
                &stripe.long_ttl,
            ),
            encoded,
            weight,
            admitted_bytes,
        };
        live.weight = 0;
        SpillAttempt::Committed(weight, job)
    }

    /// Finishes a hand-off [`Engine::evict_victim_locked`] committed to via
    /// [`Engine::try_spill_victim`], after the stripe lock is released:
    /// runs the one part of the hand-off that touches the flusher's
    /// channel, [`SpillTier::enqueue`]. A full queue gets `job` back with
    /// its key bytes, and [`SpillSink::abandon`] restores the victim's
    /// weight exactly as it would for a write or install failure
    /// discovered later downstream. No tier configured is unreachable
    /// here, but still handled rather than assumed.
    #[cfg(feature = "spill")]
    fn finish_spill_handoff(&self, job: SpillJob) {
        let Some(tier) = self.spill() else { return };
        let stripe_idx = job.stripe_idx;
        let hash = job.hash;
        let ver = job.ver;
        let weight = job.weight;
        let admitted_bytes = job.admitted_bytes;
        if let Err(job) = tier.enqueue(job) {
            // A job that never reaches the flusher must not permanently
            // shrink `admit`'s total, so release its bytes before abandon.
            tier.release(admitted_bytes);
            let job = *job;
            self.abandon(stripe_idx, &job.key_bytes, hash, ver, weight);
        }
    }

    /// Removes or spills `victim_bytes`, found at `hash` in `bucket` with
    /// `stripe` already write-locked. Hands a [`EntryState::Resident`] victim
    /// to a configured spill tier when [`Engine::try_spill_victim`] commits
    /// to it: weight zeroed in place, reported as
    /// [`VictimOutcome::PendingSpill`], while `stripe.live`, the digest,
    /// and `live_count` stay untouched until the flusher installs it. On a
    /// refusal, `keep_resident` picks between
    /// [`VictimOutcome::Deferred`] (victim stays resident, untouched
    /// weight) and the ordinary remove-and-XOR path,
    /// [`VictimOutcome::Removed`]; a vanished entry reports
    /// [`VictimOutcome::Vanished`]. Total weight and `live_count` stay the
    /// caller's job; a [`VictimOutcome::PendingSpill`] job still needs
    /// [`Engine::finish_spill_handoff`] once `stripe` is dropped.
    /// `reservation`, forwarded to [`Engine::try_spill_victim`], is the
    /// pre-lock budget a caller carried in; unused once `spill` is off.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            unused_variables,
            clippy::needless_pass_by_value,
            reason = "`reservation` is never read once `try_spill_victim` compiles out; kept \
                      as a real parameter so every caller keeps one signature across both \
                      builds"
        )
    )]
    fn evict_victim_locked(
        &self,
        stripe: &mut Stripe<K, V>,
        bucket: usize,
        victim_bytes: &Bytes,
        reservation: Option<&mut Reservation<'_>>,
    ) -> VictimOutcome {
        let hash = hash_key_bytes(victim_bytes.as_ref());
        #[cfg(feature = "spill")]
        match self.try_spill_victim(stripe, bucket, hash, victim_bytes, reservation) {
            SpillAttempt::Committed(weight, job) => {
                return VictimOutcome::PendingSpill(weight, job);
            }
            SpillAttempt::Refused { keep_resident } => {
                if keep_resident {
                    return VictimOutcome::Deferred;
                }
            }
            SpillAttempt::Pending { deficit } => {
                return VictimOutcome::PendingReservation(deficit);
            }
            SpillAttempt::NotApplicable => {}
        }
        let SlabEntry::Occupied(occ) = stripe.live.entry(
            hash,
            |l| record_key(&l.record) == victim_bytes.as_ref(),
            hasher_for,
        ) else {
            return VictimOutcome::Vanished;
        };
        let (removed, _vacant) = occ.remove();
        stripe.long_ttl.remove(victim_bytes.as_ref());
        let part = part_index_from_hash(hash);
        self.digest[digest_slot(bucket, part)].fetch_xor(
            entry_fingerprint(record_key(&removed.record), removed.ver()),
            Ordering::Relaxed,
        );
        VictimOutcome::Removed(removed.weight)
    }

    /// The sampling window [`Engine::evict_one_sampled`] and [`Engine::evict_batch_sampled`] both draw from.
    /// Walks `stripe.live` in arena order, so a tie on [`idle_elapsed_ms`]
    /// resolves to whichever comes last: an undocumented tie-break.
    fn sample_candidates<'s>(
        &self,
        stripe: &'s Stripe<K, V>,
        sample_size: usize,
    ) -> impl Iterator<Item = &'s Live<K, V>> {
        let offset = self.sample_offset(stripe.live.len());
        stripe
            .live
            .iter()
            .skip(offset)
            .chain(stripe.live.iter().take(offset))
            .take(sample_size)
            .filter(|live| is_spill_candidate(live))
    }

    /// Evicts the coldest of up to [`EVICTION_SAMPLE`] resident entries in
    /// `bucket`. Returns what happened: nothing to evict, a physical
    /// removal, or a hand-off to the spill tier. A thin wrapper,
    /// `reservation: None`, over
    /// [`Engine::evict_one_sampled_with_reservation`]; production code
    /// reaches eviction only through [`Engine::enforce_capacity`], calling
    /// the `_with_reservation` twin directly, so this wrapper is test-only.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "kept as the stable, reservation-less entry point every existing direct \
                      test call site already uses"
        )
    )]
    fn evict_one_sampled(&self, bucket: usize, now_ms: u64) -> EvictOutcome {
        self.evict_one_sampled_with_reservation(bucket, None, now_ms)
    }

    /// [`Engine::evict_one_sampled`], threading `reservation` through to
    /// [`Engine::evict_victim_locked`].
    fn evict_one_sampled_with_reservation(
        &self,
        bucket: usize,
        reservation: Option<&mut Reservation<'_>>,
        now_ms: u64,
    ) -> EvictOutcome {
        self.note_eviction_lock_acquisition();
        let mut stripe = self.stripes[bucket].write();
        let Some(victim_bytes) = self
            .sample_candidates(&stripe, EVICTION_SAMPLE)
            .max_by_key(|live| idle_elapsed_ms(now_ms, live.last_access_ms.load(Ordering::Relaxed)))
            .map(|live| record_key_bytes(&live.record))
        else {
            return EvictOutcome::default();
        };
        let outcome = self.evict_victim_locked(&mut stripe, bucket, &victim_bytes, reservation);
        drop(stripe);
        match outcome {
            VictimOutcome::Removed(weight) => {
                self.total_weight
                    .fetch_sub(u64::from(weight), Ordering::Relaxed);
                self.live_count.fetch_sub(1, Ordering::Relaxed);
                EvictOutcome {
                    removed_weight: u64::from(weight),
                    deficit: 0,
                }
            }
            #[cfg(feature = "spill")]
            VictimOutcome::PendingSpill(weight, job) => {
                self.total_weight
                    .fetch_sub(u64::from(weight), Ordering::Relaxed);
                self.pending_spill_weight
                    .fetch_add(u64::from(weight), Ordering::Relaxed);
                self.finish_spill_handoff(job);
                EvictOutcome {
                    removed_weight: u64::from(weight),
                    deficit: 0,
                }
            }
            #[cfg(feature = "spill")]
            VictimOutcome::Deferred => EvictOutcome::default(),
            #[cfg(feature = "spill")]
            VictimOutcome::PendingReservation(deficit) => EvictOutcome {
                removed_weight: 0,
                deficit: u64::from(deficit),
            },
            VictimOutcome::Vanished => EvictOutcome::default(),
        }
    }

    /// Evicts one entry from the first non-empty stripe at or after `bucket`,
    /// wrapping around once. Returns `None` only when every stripe holds
    /// nothing to evict. A thin wrapper, `reservation: None`, over
    /// [`Engine::evict_one_scanning_with_reservation`]; test-only.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "kept as the stable, reservation-less entry point every existing direct \
                      test call site already uses"
        )
    )]
    fn evict_one_scanning(&self, bucket: usize, now_ms: u64) -> Option<EvictOutcome> {
        self.evict_one_scanning_with_reservation(bucket, None, now_ms)
    }

    /// [`Engine::evict_one_scanning`], reborrowing `reservation` into each
    /// candidate stripe's scan, so the same budget carries across every
    /// stripe visited. Stops early, `Some`, the first time a candidate
    /// reports [`EvictOutcome::deficit`]: one reservation covers every
    /// stripe, so a deficit in one is a deficit in all.
    fn evict_one_scanning_with_reservation(
        &self,
        bucket: usize,
        mut reservation: Option<&mut Reservation<'_>>,
        now_ms: u64,
    ) -> Option<EvictOutcome> {
        (0..BUCKET_COUNT)
            .map(|step| (bucket + step) % BUCKET_COUNT)
            .find_map(|candidate| {
                let outcome = self.evict_one_sampled_with_reservation(
                    candidate,
                    reservation.as_deref_mut(),
                    now_ms,
                );
                (!outcome.made_no_progress() || outcome.deficit > 0).then_some(outcome)
            })
    }

    /// Evicts the coldest of up to [`EVICTION_BATCH_SAMPLE`] resident
    /// entries in `bucket` under one lock hold, as many as
    /// [`eviction_batch_size`] allows for `over_by`. Each victim is removed
    /// or, with a spill tier configured and room in its queue, handed off
    /// instead; see [`Engine::evict_victim_locked`]. A thin wrapper,
    /// `reservation: None`, over
    /// [`Engine::evict_batch_sampled_with_reservation`].
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "kept as the stable, reservation-less entry point every existing direct \
                      test call site already uses"
        )
    )]
    fn evict_batch_sampled(&self, bucket: usize, over_by: u64, now_ms: u64) -> EvictOutcome {
        self.evict_batch_sampled_with_reservation(bucket, over_by, None, now_ms)
    }

    /// [`Engine::evict_batch_sampled`], reborrowing `reservation` into each
    /// victim's own [`Engine::evict_victim_locked`] call, so the same
    /// pre-lock budget carries across every victim this lock hold evicts.
    /// Victims are ranked coldest-first by [`idle_elapsed_ms`] as of
    /// `now_ms`, correct across a `last_access_ms` rollover.
    fn evict_batch_sampled_with_reservation(
        &self,
        bucket: usize,
        over_by: u64,
        mut reservation: Option<&mut Reservation<'_>>,
        now_ms: u64,
    ) -> EvictOutcome {
        self.note_eviction_lock_acquisition();
        let mut stripe = self.stripes[bucket].write();
        let mut sampled: Vec<(Bytes, u64, u32)> = self
            .sample_candidates(&stripe, EVICTION_BATCH_SAMPLE)
            .map(|live| {
                (
                    record_key_bytes(&live.record),
                    idle_elapsed_ms(now_ms, live.last_access_ms.load(Ordering::Relaxed)),
                    live.weight,
                )
            })
            .collect();
        if sampled.is_empty() {
            return EvictOutcome::default();
        }
        sampled.sort_unstable_by_key(|&(_, idle, _)| std::cmp::Reverse(idle));
        let weights: Vec<u32> = sampled.iter().map(|&(_, _, w)| w).collect();
        let victims = eviction_batch_size(over_by, &weights);

        let mut removed_weight = 0u64;
        let mut removed_count = 0u64;
        #[cfg_attr(
            not(feature = "spill"),
            allow(
                unused_mut,
                reason = "only VictimOutcome::PendingReservation, spill-only, ever mutates this"
            )
        )]
        let mut deficit = 0u64;
        #[cfg(feature = "spill")]
        let mut pending_spills: Vec<SpillJob> = Vec::new();
        #[cfg(feature = "spill")]
        let mut pending_weight_added = 0u64;
        for (key_bytes, _, _) in sampled.into_iter().take(victims) {
            match self.evict_victim_locked(
                &mut stripe,
                bucket,
                &key_bytes,
                reservation.as_deref_mut(),
            ) {
                VictimOutcome::Removed(weight) => {
                    removed_weight += u64::from(weight);
                    removed_count += 1;
                }
                #[cfg(feature = "spill")]
                VictimOutcome::PendingSpill(weight, job) => {
                    removed_weight += u64::from(weight);
                    pending_weight_added += u64::from(weight);
                    pending_spills.push(job);
                }
                #[cfg(feature = "spill")]
                VictimOutcome::Deferred => {}
                #[cfg(feature = "spill")]
                VictimOutcome::PendingReservation(bytes) => {
                    deficit += u64::from(bytes);
                }
                VictimOutcome::Vanished => {}
            }
        }
        drop(stripe);
        #[cfg(feature = "spill")]
        if pending_weight_added > 0 {
            self.pending_spill_weight
                .fetch_add(pending_weight_added, Ordering::Relaxed);
        }
        // Every victim's channel send waits until here, past the stripe
        // lock this whole batch shared: cloning a victim's bytes and
        // deciding its fate needs that lock, but handing the job to the
        // flusher's channel does not, so this is the one point per batch
        // where that per-victim cost, rather than per-eviction-pass, comes
        // off the lock.
        #[cfg(feature = "spill")]
        for job in pending_spills {
            self.finish_spill_handoff(job);
        }
        if removed_weight > 0 {
            self.total_weight
                .fetch_sub(removed_weight, Ordering::Relaxed);
        }
        if removed_count > 0 {
            self.live_count.fetch_sub(removed_count, Ordering::Relaxed);
        }
        EvictOutcome {
            removed_weight,
            deficit,
        }
    }

    /// After a write to `start_bucket` may have pushed total weight over
    /// `max_capacity`, evicts sampled-cold entries, starting at
    /// `start_bucket` then pseudo-random stripes, until it is back under
    /// the cap, up to [`EVICTION_BATCH`] entries per lock hold. A random
    /// probe that lands on an empty stripe falls back to a scan for the next
    /// non-empty one, so the loop ends only under the cap or with nothing
    /// left to evict. Never holds two stripe locks at once; a no-op when
    /// `max_capacity` is [`u64::MAX`].
    ///
    /// The over-cap check weighs `total_weight` plus `pending_spill_weight`:
    /// a spill hand-off zeroes and frees its victim's weight from
    /// `total_weight` the instant it commits, but the victim stays fully
    /// resident until the flusher's install resolves it, so
    /// `pending_spill_weight` is what keeps that still-resident RAM
    /// counting against the cap in the meantime. When a pass evicts
    /// nothing and some hand-off is still pending, [`defer_to_flusher`]
    /// has this return rather than pay for
    /// [`Engine::evict_one_scanning`]'s full-stripe scan, trusting the
    /// flusher's own installs to bring `pending_spill_weight` back down
    /// shortly with no further eviction needed. With nothing pending, the
    /// scan runs exactly as it always has, spill tier configured or not. A
    /// thin wrapper, `reservation: None`, over
    /// [`Engine::enforce_capacity_with_reservation`].
    pub(crate) fn enforce_capacity(&self, start_bucket: usize, now_ms: u64) {
        let _ = self.enforce_capacity_with_reservation(start_bucket, None, now_ms);
    }

    /// [`Engine::enforce_capacity`], reborrowing `reservation` into every
    /// batch and scan call this loop makes, so one caller-supplied budget
    /// carries across as many lock holds as it takes to fall back under
    /// the cap.
    ///
    /// Returns the sum of [`VictimOutcome::PendingReservation`] bytes this
    /// call's eviction passes hit, where both the reservation's remaining
    /// budget and `admit`'s global headroom refused the same victim. `0`
    /// means every victim fit, was evicted, or was refused for a reason no
    /// retry changes. A nonzero return stops the loop immediately, leaving
    /// each stopped victim as [`VictimOutcome::Deferred`] does (fully
    /// resident, untouched weight) for a later pass to resolve; the caller
    /// (`Shard::retry_reservation_deficit`) pays it down with a fresh
    /// reservation.
    pub(crate) fn enforce_capacity_with_reservation(
        &self,
        start_bucket: usize,
        mut reservation: Option<&mut Reservation<'_>>,
        now_ms: u64,
    ) -> u64 {
        if self.max_capacity == u64::MAX {
            return 0;
        }
        let mut bucket = start_bucket;
        loop {
            let total = self.total_weight.load(Ordering::Relaxed);
            let pending = self.pending_spill_weight_or_zero();
            let current = total.saturating_add(pending);
            if current <= self.max_capacity {
                return 0;
            }
            let over_by = current - self.max_capacity;
            let batch_outcome = self.evict_batch_sampled_with_reservation(
                bucket,
                over_by,
                reservation.as_deref_mut(),
                now_ms,
            );
            if batch_outcome.deficit > 0 {
                let total = self.total_weight.load(Ordering::Relaxed);
                let pending = self.pending_spill_weight_or_zero();
                let current = total.saturating_add(pending);
                return deficit_once_still_over_capacity(
                    current,
                    self.max_capacity,
                    batch_outcome.deficit,
                );
            }
            if batch_outcome.made_no_progress() {
                if defer_to_flusher(self.pending_spill_weight_or_zero()) {
                    return 0;
                }
                match self.evict_one_scanning_with_reservation(
                    bucket,
                    reservation.as_deref_mut(),
                    now_ms,
                ) {
                    None => return 0,
                    Some(outcome) if outcome.deficit > 0 => return outcome.deficit,
                    Some(_) => {}
                }
            }
            bucket = self.next_pseudo_random_bucket();
        }
    }

    /// Flips [`Engine::apply_many`]'s pre-fold on or off, `true` by default.
    /// Only a benchmark or test ever needs it off, to compare pre-fold's
    /// effect on throughput against the unfolded per-record path it
    /// otherwise always takes; nothing on a real write path ever calls it.
    /// [`super::Shard::with_prefold_enabled`] is the `#[doc(hidden)]` seam
    /// that reaches this from `crate::cache::CacheBuilder::prefold_enabled`,
    /// in turn reachable from an integration-test binary outside this
    /// crate, so this stays outside any `#[cfg(test)]` gate.
    pub(crate) fn set_prefold_enabled(&self, enabled: bool) {
        self.prefold_enabled.store(enabled, Ordering::Relaxed);
    }

    /// The current value of [`Engine::set_prefold_enabled`]'s flag. Only a
    /// test reads it back; the write path only ever needs the flag itself,
    /// via [`Self::apply_many`].
    #[cfg(test)]
    pub(crate) fn prefold_enabled(&self) -> bool {
        self.prefold_enabled.load(Ordering::Relaxed)
    }

    /// Applies a batch of versioned writes that all hash into `bucket`,
    /// under one write-lock acquisition for the whole group. Runs
    /// [`Self::enforce_capacity`] once afterward, outside the write lock,
    /// iff the batch put anything.
    ///
    /// When `resolver` merges and pre-folding is enabled
    /// (`prefold_enabled`, toggled only via [`Engine::set_prefold_enabled`]),
    /// `entries` are grouped into runs and folded through [`prefold_batch`]
    /// before any [`apply_locked`] call, collapsing repeated keys to one
    /// call each with every other index reported [`ApplyOutcome::Rejected`];
    /// see [`prefold_batch`]'s doc for why the result matches sequential
    /// application exactly, version and all.
    ///
    /// [`prefetch_spilled_conflict_bytes`] reads back any currently-spilled
    /// stored sides off-lock first, before the write lock this call holds
    /// for the whole batch; see [`apply_locked`]'s doc for the under-lock
    /// fallback on a miss.
    ///
    /// A thin wrapper, `reservation: None`, over
    /// [`Engine::apply_many_with_reservation`].
    pub(crate) fn apply_many(
        &self,
        bucket: usize,
        entries: Vec<BatchEntry<K, V>>,
        resolver: &dyn ConflictResolver,
        tombstone_ttl_ms: u64,
        tombstone_max_ttl_ms: u64,
        now_ms: u64,
    ) -> Vec<ApplyOutcome<K, V>> {
        self.apply_many_with_reservation(
            bucket,
            entries,
            resolver,
            tombstone_ttl_ms,
            tombstone_max_ttl_ms,
            now_ms,
            None,
        )
        .0
    }

    /// [`Engine::apply_many`], additionally threading `reservation` into
    /// [`Engine::enforce_capacity_with_reservation`] when the batch wrote
    /// anything, spending the pre-lock budget one caller reserved before
    /// its first stripe lock. The tuple's second element is that method's
    /// own deficit, `0` when nothing was written or every victim resolved.
    #[allow(
        clippy::too_many_arguments,
        reason = "one caller per bucket, from an already-grouped batch; splitting these into a struct would not make either call site clearer"
    )]
    pub(crate) fn apply_many_with_reservation(
        &self,
        bucket: usize,
        entries: Vec<BatchEntry<K, V>>,
        resolver: &dyn ConflictResolver,
        tombstone_ttl_ms: u64,
        tombstone_max_ttl_ms: u64,
        now_ms: u64,
        reservation: Option<&mut Reservation<'_>>,
    ) -> (Vec<ApplyOutcome<K, V>>, u64) {
        let prefold = resolver.merges() && self.prefold_enabled.load(Ordering::Relaxed);
        #[cfg(feature = "spill")]
        let prefetched_spilled = prefetch_spilled_conflict_bytes(
            &self.stripes[bucket],
            entries
                .iter()
                .map(|(hash, _, key_bytes, ..)| (*hash, key_bytes)),
            resolver,
            self.spill().map(Arc::as_ref),
        );
        let entries: Vec<Option<BatchEntry<K, V>>> = if prefold {
            let runs = group_prefold_runs(&entries);
            let stored_seeds = {
                let stripe = self.stripes[bucket].read();
                peek_prefold_seeds(
                    &stripe,
                    &entries,
                    &runs,
                    #[cfg(feature = "spill")]
                    &prefetched_spilled,
                )
            };
            prefold_batch(entries, runs, resolver, &stored_seeds)
        } else {
            entries.into_iter().map(Some).collect()
        };
        let mut outcomes = Vec::with_capacity(entries.len());
        let mut wrote = false;
        {
            let mut stripe = self.stripes[bucket].write();
            for entry in entries {
                let Some((hash, key, key_bytes, ver, incoming)) = entry else {
                    // Absorbed into its run's survivor by `prefold_batch`;
                    // that survivor's own `apply_locked` call, elsewhere in
                    // this same loop, is this entry's only real contribution.
                    outcomes.push(ApplyOutcome::Rejected);
                    continue;
                };
                let part = part_index_from_hash(hash);
                let ctx = ApplyCtx {
                    digest_bucket: &self.digest[digest_slot(bucket, part)],
                    total_weight: &self.total_weight,
                    live_count: &self.live_count,
                    weigher: self.weigher.as_ref(),
                    tti_ms: self.tti_ms,
                    resolver,
                    #[cfg(feature = "spill")]
                    prefetched_spilled: &prefetched_spilled,
                    #[cfg(feature = "spill")]
                    spill: self.spill().map(Arc::as_ref),
                    tombstone_ttl_ms,
                    tombstone_max_ttl_ms,
                    now_ms,
                };
                let (outcome, displaced_spilled) = apply_locked(
                    &mut stripe,
                    &ctx,
                    EntryKey {
                        hash,
                        key,
                        key_bytes,
                    },
                    ver,
                    incoming,
                );
                self.note_spill_departure(displaced_spilled);
                wrote |= matches!(outcome, ApplyOutcome::Put { .. });
                outcomes.push(outcome);
            }
        }
        let deficit = if wrote {
            self.enforce_capacity_with_reservation(bucket, reservation, now_ms)
        } else {
            0
        };
        (outcomes, deficit)
    }

    /// Folds a [`RemovedLive`]'s departure into digest, weight, count, and spill bookkeeping; `keep_count` skips the live-count decrement for a caller about to insert a replacement.
    fn account_removed(
        &self,
        bucket: usize,
        part: usize,
        key_bytes: &[u8],
        removed: &RemovedLive,
        keep_count: bool,
    ) {
        self.digest[digest_slot(bucket, part)]
            .fetch_xor(entry_fingerprint(key_bytes, removed.ver), Ordering::Relaxed);
        self.total_weight
            .fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
        if !keep_count {
            self.live_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.note_spill_departure(removed.was_spilled);
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
                .find(hash, |l| record_key(&l.record) == key_bytes)
                .map(Live::ver),
        };
        if stored_ver.is_some_and(|sv| ver <= sv) {
            return None;
        }
        let had_live = prior_tombstone.is_none() && stored_ver.is_some();
        if !had_live {
            return None;
        }
        let removed = remove_live(&mut stripe, hash, key_bytes)?;
        drop(stripe);
        let ver_out = removed.ver;
        let part = part_index_from_hash(hash);
        self.account_removed(bucket, part, key_bytes, &removed, false);
        Some(ver_out)
    }

    /// Drops the local live entry at `key_bytes` unconditionally, no version
    /// check, no tombstone, for [`super::Shard::invalidate_local`]'s
    /// cache-busting escape hatch.
    pub(crate) fn invalidate_local(&self, key_bytes: &[u8], hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some(removed) = remove_live(&mut stripe, hash, key_bytes) {
            drop(stripe);
            let part = part_index_from_hash(hash);
            self.account_removed(bucket, part, key_bytes, &removed, false);
        }
    }

    /// Removes every locally held entry, live or tombstone, in each of
    /// `buckets`: one write-lock hold per bucket, resetting that stripe's
    /// digest atomics, [`Engine::live_count`], [`Engine::total_weight`], and
    /// spill bookkeeping to match, the same accounting a per-key removal
    /// keeps, batched here per stripe instead of per key. Writes no
    /// tombstone of its own, so a released bucket carries nothing for
    /// anti-entropy or replication to fan back out; a peer later applying a
    /// fresh write for a key in a released bucket lands it under the
    /// ordinary ownership-gated apply path, never a resurrection of the
    /// bucket's prior contents. A bucket at or past [`BUCKET_COUNT`], which
    /// only a misbehaving caller names, is skipped. Returns the total number of
    /// entries removed, live and tombstoned together, across every bucket
    /// given.
    pub(crate) fn release_buckets(&self, buckets: &[u16]) -> u64 {
        let mut removed_total = 0u64;
        for &bucket in buckets {
            if usize::from(bucket) >= BUCKET_COUNT {
                continue;
            }
            let bucket_idx = usize::from(bucket);
            let (removed_weight, removed_live, removed_tombstones, removed_spilled) = {
                let mut stripe = self.stripes[bucket_idx].write();
                let mut removed_weight = 0u64;
                let mut removed_live = 0u64;
                let mut removed_spilled = 0usize;
                for live in stripe.live.drain() {
                    removed_weight += u64::from(live.weight);
                    removed_live += 1;
                    if is_spilled(&live) {
                        removed_spilled += 1;
                    }
                }
                let removed_tombstones = stripe.tombstones.len();
                stripe.tombstones.clear();
                stripe.long_ttl.clear();
                stripe.next_expiry_ms = u64::MAX;
                (
                    removed_weight,
                    removed_live,
                    removed_tombstones,
                    removed_spilled,
                )
            };
            for part in 0..PART_COUNT {
                self.digest[digest_slot(bucket_idx, part)].store(0, Ordering::Relaxed);
            }
            if removed_weight > 0 {
                self.total_weight
                    .fetch_sub(removed_weight, Ordering::Relaxed);
            }
            if removed_live > 0 {
                self.live_count.fetch_sub(removed_live, Ordering::Relaxed);
            }
            self.note_spill_departures(removed_spilled);
            removed_total = removed_total
                .saturating_add(removed_live)
                .saturating_add(u64::try_from(removed_tombstones).unwrap_or(u64::MAX));
        }
        removed_total
    }

    /// [`Engine::release_buckets`] for single parts: drops every live entry
    /// and tombstone in `parts` and zeroes their part digests, leaving the
    /// rest of each bucket where it is. A bucket named by all
    /// [`PART_COUNT`] of its parts takes the whole-bucket path. Returns the
    /// total number of entries removed, live and tombstoned together.
    pub(crate) fn release_parts(&self, parts: &[PartId]) -> u64 {
        let mut masks: HashMap<u16, u64> = HashMap::new();
        for part in parts {
            *masks.entry(part.bucket()).or_default() |= 1u64 << part.part();
        }
        let mut removed_total = 0u64;
        for (bucket, mask) in masks {
            if mask == u64::MAX {
                removed_total = removed_total.saturating_add(self.release_buckets(&[bucket]));
                continue;
            }
            let released =
                |key: &[u8]| mask & (1u64 << part_index_from_hash(hash_key_bytes(key))) != 0;
            let bucket_idx = usize::from(bucket);
            let (removed_weight, removed_live, removed_tombstones, removed_spilled) = {
                let mut stripe = self.stripes[bucket_idx].write();
                let mut removed_weight = 0u64;
                let mut removed_live = 0u64;
                let mut removed_spilled = 0usize;
                stripe.live.retain(hasher_for, |live| {
                    if released(record_key(&live.record)) {
                        removed_weight += u64::from(live.weight);
                        removed_live += 1;
                        if is_spilled(live) {
                            removed_spilled += 1;
                        }
                        false
                    } else {
                        true
                    }
                });
                let before = stripe.tombstones.len();
                stripe.tombstones.retain(|key, _| !released(key));
                let removed_tombstones = before - stripe.tombstones.len();
                stripe.long_ttl.retain(|key, _| !released(key));
                (
                    removed_weight,
                    removed_live,
                    removed_tombstones,
                    removed_spilled,
                )
            };
            for part in 0..PART_COUNT {
                if mask & (1u64 << part) != 0 {
                    self.digest[digest_slot(bucket_idx, part)].store(0, Ordering::Relaxed);
                }
            }
            if removed_weight > 0 {
                self.total_weight
                    .fetch_sub(removed_weight, Ordering::Relaxed);
            }
            if removed_live > 0 {
                self.live_count.fetch_sub(removed_live, Ordering::Relaxed);
            }
            self.note_spill_departures(removed_spilled);
            removed_total = removed_total
                .saturating_add(removed_live)
                .saturating_add(u64::try_from(removed_tombstones).unwrap_or(u64::MAX));
        }
        removed_total
    }

    /// Every part holding at least one live entry or un-GC'd tombstone,
    /// ascending: one stripe read lock at a time, skipping empty stripes
    /// without touching an entry.
    pub(crate) fn held_parts(&self) -> Vec<PartId> {
        let mut held = PartSet::new();
        for stripe in &*self.stripes {
            let stripe = stripe.read();
            if stripe.live.len() == 0 && stripe.tombstones.is_empty() {
                continue;
            }
            for live in stripe.live.iter() {
                held.insert(PartId::from_hash(hash_key_bytes(record_key(&live.record))));
            }
            for key in stripe.tombstones.keys() {
                held.insert(PartId::from_hash(hash_key_bytes(key)));
            }
        }
        held.iter().collect()
    }

    /// The lock-protected first half of [`super::Shard::get_or_load`]: a
    /// fast-path re-check, then either joining an already in-flight load or
    /// registering as the new owner.
    ///
    /// A currently-spilled entry never takes the `Hit` branch. There is no
    /// value here to clone. A spill-aware caller checks
    /// [`Engine::spilled_loc`] before ever reaching this call, so it only
    /// falls through to here once it already knows the entry, if any, is
    /// resident or absent.
    pub(crate) fn miss_or_join(&self, key_bytes: &Bytes, hash: u64, now_ms: u64) -> JoinOutcome<V> {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some(live) = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            && !self.is_absent(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms)
            && is_resident(live)
        {
            let value = decode_value(record_value(live.record.as_slice()));
            self.touch(live, now_ms);
            return JoinOutcome::Hit(value);
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

    /// Applies a successful [`super::Shard::get_or_load`] fill under one
    /// stripe write-lock acquisition: removes the `inflight` entry, and,
    /// when [`fill_supersedes`] allows it, replaces any prior tombstone or
    /// live entry for `write.key` with the loader's value and corrects
    /// `live_count` for the net change. A tombstone or live entry at least
    /// as new as the fill, a removal or write that landed while the loader
    /// ran, stays, and the fill is dropped.
    pub(crate) fn complete_fresh_load(
        &self,
        write: FreshLoad<'_, K, V>,
        now_ms: u64,
        inflight: &Inflight<V>,
    ) -> FillOutcome {
        let FreshLoad {
            key,
            key_bytes,
            hash,
            ver,
            value,
            encoded,
            expires_at_ms,
        } = write;
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        let outcome = {
            let mut stripe = self.stripes[bucket].write();
            stripe.inflight.remove(key_bytes.as_ref());
            let existing = stripe
                .tombstones
                .get(key_bytes.as_ref())
                .map(|t| t.ver)
                .or_else(|| {
                    stripe
                        .live
                        .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
                        .map(Live::ver)
                });
            if !fill_supersedes(existing, ver) {
                drop(stripe);
                inflight.finish();
                return FillOutcome::Superseded;
            }
            let mut had_live = false;
            if let Some(t) = stripe.tombstones.remove(key_bytes.as_ref()) {
                self.digest[digest_slot(bucket, part)].fetch_xor(
                    entry_fingerprint(key_bytes.as_ref(), t.ver),
                    Ordering::Relaxed,
                );
            } else if let Some(removed) = remove_live(&mut stripe, hash, key_bytes.as_ref()) {
                had_live = true;
                // A replacement is about to land below, so `live_count` nets
                // to no change: `keep_count = true`.
                self.account_removed(bucket, part, key_bytes.as_ref(), &removed, true);
            }
            if !had_live {
                self.live_count.fetch_add(1, Ordering::Relaxed);
            }
            self.digest[digest_slot(bucket, part)].fetch_xor(
                entry_fingerprint(key_bytes.as_ref(), ver),
                Ordering::Relaxed,
            );
            let weight = self.weigher.as_ref().map_or(1, |w| w(key, &value));
            // `remove_live` already cleared any stale `long_ttl` row above.
            let (expiry, long_ttl_value) = encode_expiry(ver.wall_ms, expires_at_ms);
            if let Some(deadline) = long_ttl_value {
                stripe.long_ttl.insert(key_bytes.clone(), deadline);
            }
            stripe.live.insert_unique(
                hash,
                Live::new(
                    build_resident_record(key_bytes.as_ref(), encoded.as_ref()),
                    ver,
                    expiry,
                    weight,
                    now_ms,
                    #[cfg(feature = "spill")]
                    EntryState::Resident,
                ),
                hasher_for,
            );
            if let Some(exp) = expires_at_ms {
                stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
            }
            self.total_weight
                .fetch_add(u64::from(weight), Ordering::Relaxed);
            FillOutcome::Installed { had_live }
        };
        inflight.finish();
        self.enforce_capacity(bucket, now_ms);
        outcome
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

    /// Snapshots a currently-[`EntryState::Spilled`] entry's pointer under the
    /// stripe read lock: `None` for a resident entry, an absent key, or one
    /// a read at `now_ms` would see as expired. Touches `last_access_ms`.
    /// The entry's `ver` travels alongside the pointer so a later
    /// promotion can re-verify it is still current via
    /// [`spilled_is_current`]. The caller drops the read lock, reads the
    /// pointer's bytes off-lock, via `spawn_blocking` behind the tier's
    /// read semaphore, then reacquires the stripe write lock for
    /// [`Engine::promote_locked`].
    #[cfg(feature = "spill")]
    pub(crate) fn spilled_loc(
        &self,
        key_bytes: &[u8],
        hash: u64,
        now_ms: u64,
    ) -> Option<(Hlc, SpillLoc)> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        match &live.state {
            EntryState::Spilled(loc) => {
                let loc = *loc;
                let ver = live.ver();
                self.touch(live, now_ms);
                Some((ver, loc))
            }
            EntryState::Resident => None,
        }
    }

    /// Flips a currently-[`EntryState::Spilled`] entry back to
    /// [`EntryState::Resident`] in place, iff [`spilled_is_current`] still
    /// holds for `read_ver` against what is currently stored. A tombstone
    /// or a version change since the disk read started means the
    /// promotion is a silent no-op. The caller's read already succeeded
    /// independently of this; only the RAM reinstall is skipped. Decodes
    /// `key` from `key_bytes` for the one weigher call this makes. Adds the
    /// freshly weighed entry's weight back to `total_weight`. **Never touches the digest or
    /// `live_count`**: same key, same `ver`, same fingerprint, so nothing
    /// about the entry's replicated identity changes. Also decrements
    /// `sundog_spill_entries{cache}`, the mirror of
    /// `SpillSink::install`'s increment, since a resident entry is not
    /// counted among currently-spilled entries. Returns whether it
    /// promoted.
    #[cfg(feature = "spill")]
    pub(crate) fn promote_locked(
        &self,
        key_bytes: &[u8],
        hash: u64,
        read_ver: Hlc,
        value: &V,
        encoded: &Bytes,
    ) -> bool {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        let stored_tombstone_ver = stripe.tombstones.get(key_bytes).map(|t| t.ver);
        let Some(live) = stripe
            .live
            .find_mut(hash, |l| record_key(&l.record) == key_bytes)
        else {
            return false;
        };
        if !spilled_is_current(stored_tombstone_ver, Some(live.ver()), read_ver) {
            return false;
        }
        if !matches!(live.state, EntryState::Spilled(_)) {
            // Already resident: a racing promotion, or a fresh write that
            // happens to share this version, got there first.
            return false;
        }
        let key: K = decode_key(key_bytes);
        let weight = self.weigher.as_ref().map_or(1, |w| w(&key, value));
        live.record = build_resident_record(key_bytes, encoded.as_ref());
        live.state = EntryState::Resident;
        live.weight = weight;
        drop(stripe);
        self.total_weight
            .fetch_add(u64::from(weight), Ordering::Relaxed);
        self.note_spill_departure(true);
        true
    }
}

/// The engine-side callback surface [`SpillTier`]'s flusher drives, so
/// installing a flushed record and reclaiming a rotated-out region both flip
/// existing `live` entries in place rather than routing back through
/// `apply_locked`'s fan-out/event machinery: neither changes a key's
/// version, value, or expiry, so nothing about it differs from the
/// cluster's point of view.
#[cfg(feature = "spill")]
impl<K, V> SpillSink for Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Resolves the job either way: `weight` leaves `pending_spill_weight`
    /// whether the flip happens or the key moved on, so a write, tombstone,
    /// or expiry that displaced a pending key never strands its share.
    fn install(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        loc: SpillLoc,
        weight: u32,
    ) -> bool {
        let flipped = {
            let mut stripe = self.stripes[stripe_idx].write();
            let stored_tombstone_ver = stripe.tombstones.get(key_bytes.as_ref()).map(|t| t.ver);
            match stripe
                .live
                .find_mut(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            {
                Some(live)
                    if spilled_is_current(stored_tombstone_ver, Some(live.ver()), ver)
                        && live.weight == 0
                        && matches!(live.state, EntryState::Resident) =>
                {
                    // A fresh copy of the key half, never a slice of the
                    // record (which would keep its value bytes resident).
                    live.record = build_key_only_record(record_key(&live.record));
                    live.state = EntryState::Spilled(loc);
                    true
                }
                _ => false,
            }
        };
        self.pending_spill_weight
            .fetch_sub(u64::from(weight), Ordering::Relaxed);
        if flipped {
            self.note_spill_arrival();
        }
        flipped
    }

    /// Originates a fresh entry straight from an on-disk [`SpillLoc`], with
    /// no value bytes ever read into RAM: the warm tier-reopen replay
    /// primitive, distinct from `install`, which only flips an
    /// already-present entry. `record` holds only the key half
    /// ([`build_key_only_record`]); the entry starts at weight `0` like a
    /// freshly installed one (a later [`Engine::promote_locked`] recomputes
    /// the real weight). `weight` mirrors `install`'s parameter for
    /// signature symmetry and is never folded into `total_weight`.
    ///
    /// Never overwrites a key already present, live or tombstoned: `false`
    /// and a no-op either way, so this can never resurrect a deleted key or
    /// clobber a fresher write. Folds the fresh entry's fingerprint into
    /// the bucket's digest like every other insert path.
    #[allow(clippy::too_many_arguments)]
    fn install_new(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        expires_at_ms: Option<u64>,
        loc: SpillLoc,
        _weight: u32,
    ) -> bool {
        let part = part_index_from_hash(hash);
        let inserted = {
            let mut stripe = self.stripes[stripe_idx].write();
            if stripe.tombstones.contains_key(key_bytes.as_ref())
                || stripe
                    .live
                    .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
                    .is_some()
            {
                false
            } else {
                let (expiry, long_ttl_value) = encode_expiry(ver.wall_ms, expires_at_ms);
                if let Some(deadline) = long_ttl_value {
                    stripe.long_ttl.insert(key_bytes.clone(), deadline);
                }
                stripe.live.insert_unique(
                    hash,
                    Live::new(
                        build_key_only_record(key_bytes.as_ref()),
                        ver,
                        expiry,
                        0,
                        ver.wall_ms,
                        EntryState::Spilled(loc),
                    ),
                    hasher_for,
                );
                if let Some(exp) = expires_at_ms {
                    stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
                }
                true
            }
        };
        if inserted {
            self.digest[digest_slot(stripe_idx, part)].fetch_xor(
                entry_fingerprint(key_bytes.as_ref(), ver),
                Ordering::Relaxed,
            );
            self.live_count.fetch_add(1, Ordering::Relaxed);
            self.note_spill_arrival();
        }
        inserted
    }

    /// Resolves a job the tier could not write: `weight` leaves
    /// `pending_spill_weight` unconditionally, and goes back onto the entry
    /// and `total_weight` only while the entry is still the pending one.
    fn abandon(&self, stripe_idx: usize, key_bytes: &Bytes, hash: u64, ver: Hlc, weight: u32) {
        let restored = {
            let mut stripe = self.stripes[stripe_idx].write();
            match stripe
                .live
                .find_mut(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            {
                Some(live)
                    if live.ver() == ver
                        && live.weight == 0
                        && matches!(live.state, EntryState::Resident) =>
                {
                    live.weight = weight;
                    true
                }
                _ => false,
            }
        };
        self.pending_spill_weight
            .fetch_sub(u64::from(weight), Ordering::Relaxed);
        if restored {
            self.total_weight
                .fetch_add(u64::from(weight), Ordering::Relaxed);
        }
    }

    fn reclaim(&self, region: u32, generation: u32, keys: &[(usize, Bytes)]) -> usize {
        let mut removed = 0usize;
        for (stripe_idx, key_bytes) in keys {
            let hash = hash_key_bytes(key_bytes.as_ref());
            let bucket = *stripe_idx;
            let removed_this = {
                let mut stripe = self.stripes[bucket].write();
                let SlabEntry::Occupied(occ) = stripe.live.entry(
                    hash,
                    |l| record_key(&l.record) == key_bytes.as_ref(),
                    hasher_for,
                ) else {
                    continue;
                };
                let still_points_here = matches!(
                    &occ.get().state,
                    EntryState::Spilled(loc) if loc.region == region && loc.generation == generation
                );
                if !still_points_here {
                    continue;
                }
                let (removed_entry, _vacant) = occ.remove();
                stripe.long_ttl.remove(key_bytes.as_ref());
                let part = part_index_from_hash(hash);
                self.digest[digest_slot(bucket, part)].fetch_xor(
                    entry_fingerprint(record_key(&removed_entry.record), removed_entry.ver()),
                    Ordering::Relaxed,
                );
                true
            };
            if removed_this {
                self.live_count.fetch_sub(1, Ordering::Relaxed);
                removed += 1;
            }
        }
        self.note_spill_departures(removed);
        removed
    }
}

/// A gauge only needs `f64`'s exact-integer range, up to 2^53, which
/// comfortably covers any realistic entry count with no meaningful precision
/// loss. Mirrors `spill::bytes_used_f64`.
#[cfg(feature = "spill")]
#[expect(
    clippy::cast_precision_loss,
    reason = "a gauge only needs f64's exact-integer range, up to 2^53, which comfortably \
              covers any realistic entry count"
)]
fn count_f64(n: usize) -> f64 {
    n as f64
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
            for live in stripe.live.iter() {
                let part = part_index_from_hash(hash_key_bytes(record_key(&live.record)));
                out[digest_slot(idx, part)] ^=
                    entry_fingerprint(record_key(&live.record), live.ver());
            }
            for (key_bytes, t) in &stripe.tombstones {
                let part = part_index_from_hash(hash_key_bytes(key_bytes));
                out[digest_slot(idx, part)] ^= entry_fingerprint(key_bytes, t.ver);
            }
        }
        out
    }

    /// Every stripe's raw contents, for building a canonical state to compare
    /// across replicas. Every test that calls this configures no spill tier,
    /// so nothing is ever spilled; it panics if that ever changes, rather
    /// than silently reporting a partial snapshot.
    pub(crate) fn debug_snapshot(&self) -> (Vec<DebugLive>, Vec<DebugTombstone>) {
        let mut live_out = Vec::new();
        let mut tomb_out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            for live in stripe.live.iter() {
                assert!(
                    is_resident(live),
                    "debug_snapshot: no resident bytes for a spilled entry; this helper is \
                     for tests that never configure a spill tier"
                );
                live_out.push((
                    record_key_bytes(&live.record),
                    record_value_bytes(&live.record),
                    live.ver(),
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

    /// [`Engine::live_entry_count`] by a full pass over every stripe.
    pub(crate) fn recompute_live_entry_count(&self) -> u64 {
        self.stripes
            .iter()
            .map(|s| u64::try_from(s.read().live.len()).unwrap_or(u64::MAX))
            .fold(0u64, u64::saturating_add)
    }

    pub(crate) fn debug_eviction_lock_acquisitions(&self) -> u64 {
        self.eviction_lock_acquisitions.load(Ordering::Relaxed)
    }

    /// Test-only: how many stripe read locks [`Engine::compact`] has taken
    /// in total, across every call on this engine.
    pub(crate) fn debug_compact_lock_acquisitions(&self) -> u64 {
        self.compact_lock_acquisitions.load(Ordering::Relaxed)
    }

    /// Test-only: [`Engine::compact`]'s current cursor position, the index
    /// of the next stripe a future call resumes from.
    pub(crate) fn debug_compact_cursor(&self) -> u64 {
        self.compact_cursor.load(Ordering::Relaxed)
    }

    /// Test-only: pins [`Engine::compact`]'s cursor to `stripe`, so a test
    /// can start a pass from a known stripe instead of wherever the
    /// previous pass left it.
    pub(crate) fn debug_set_compact_cursor(&self, stripe: u64) {
        self.compact_cursor.store(stripe, Ordering::Relaxed);
    }

    /// Test-only: inserts a live entry already pointing at `loc`, with the
    /// same digest/`live_count`/`sundog_spill_entries` bookkeeping a genuine
    /// spill install would have left behind, bypassing the normal
    /// Resident-only write path. Lets mutation tests exercise a spilled key
    /// with no real disk I/O and no dependency on a real [`SpillTier`]'s
    /// flusher thread. Takes no decoded key: `Live` carries none, so the
    /// caller only ever needs `key_bytes`.
    #[cfg(feature = "spill")]
    pub(crate) fn debug_insert_spilled(
        &self,
        key_bytes: &Bytes,
        ver: Hlc,
        expires_at_ms: Option<u64>,
        loc: SpillLoc,
        now_ms: u64,
    ) {
        let hash = hash_key_bytes(key_bytes.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        {
            let mut stripe = self.stripes[bucket].write();
            let (expiry, long_ttl_value) = encode_expiry(ver.wall_ms, expires_at_ms);
            if let Some(deadline) = long_ttl_value {
                stripe.long_ttl.insert(key_bytes.clone(), deadline);
            }
            stripe.live.insert_unique(
                hash,
                Live::new(
                    build_key_only_record(key_bytes.as_ref()),
                    ver,
                    expiry,
                    0,
                    now_ms,
                    EntryState::Spilled(loc),
                ),
                hasher_for,
            );
            if let Some(exp) = expires_at_ms {
                stripe.next_expiry_ms = stripe.next_expiry_ms.min(exp);
            }
        }
        self.digest[digest_slot(bucket, part)].fetch_xor(
            entry_fingerprint(key_bytes.as_ref(), ver),
            Ordering::Relaxed,
        );
        self.live_count.fetch_add(1, Ordering::Relaxed);
        self.note_spill_arrival();
    }

    /// This engine's current `sundog_spill_entries{cache}` value, mirrored
    /// through [`Engine::note_spill_arrival`]/[`Engine::note_spill_departures`]
    /// regardless of whether a real gauge, and thus a real metrics
    /// recorder, is attached. See `spill_entries_test_count`'s own doc for
    /// why tests need this instead of reading the gauge itself.
    #[cfg(feature = "spill")]
    pub(crate) fn debug_spill_entries_count(&self) -> i64 {
        self.spill_entries_test_count.load(Ordering::Relaxed)
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
        Engine::new(max_capacity, tti, None, None)
    }

    fn key_bytes(key: u32) -> Bytes {
        Bytes::from(postcard::to_stdvec(&key).expect("u32 encodes"))
    }

    /// [`key_bytes`]'s `String`-keyed counterpart, for a key long enough to
    /// force [`Record`]'s heap path.
    #[cfg(feature = "spill")]
    fn string_key_bytes(key: &str) -> Bytes {
        Bytes::from(postcard::to_stdvec(key).expect("a string key encodes"))
    }

    fn hlc(wall_ms: u64, node: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(node),
        }
    }

    /// Always empty: every raw `apply_locked` test site below skips real spill prefetch.
    #[cfg(feature = "spill")]
    fn empty_prefetched() -> &'static HashMap<SpillLoc, Bytes> {
        static EMPTY: std::sync::OnceLock<HashMap<SpillLoc, Bytes>> = std::sync::OnceLock::new();
        EMPTY.get_or_init(HashMap::new)
    }

    /// An [`ApplyCtx`] against `engine`'s own weigher/tti, no spill prefetch or tier, for a test driving [`apply_locked`] directly.
    fn test_ctx<'a, K, V>(
        engine: &'a Engine<K, V>,
        bucket: usize,
        hash: u64,
        resolver: &'a dyn ConflictResolver,
        tombstone_ttl_ms: u64,
        tombstone_max_ttl_ms: u64,
        now_ms: u64,
    ) -> ApplyCtx<'a, K, V> {
        ApplyCtx {
            digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
            total_weight: &engine.total_weight,
            live_count: &engine.live_count,
            weigher: engine.weigher.as_ref(),
            tti_ms: engine.tti_ms,
            resolver,
            #[cfg(feature = "spill")]
            prefetched_spilled: empty_prefetched(),
            #[cfg(feature = "spill")]
            spill: None,
            tombstone_ttl_ms,
            tombstone_max_ttl_ms,
            now_ms,
        }
    }

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
        put_with_resolver(
            engine,
            key,
            key_bytes,
            value,
            ver,
            expires_at_ms,
            now_ms,
            &LwwResolver,
        )
    }

    /// [`put`]'s tombstone counterpart, for a test that needs a real
    /// tombstone in place rather than a live entry.
    fn tombstone<K, V>(engine: &Engine<K, V>, key: K, key_bytes: Bytes, ver: Hlc, now_ms: u64)
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let hash = hash_key_bytes(key_bytes.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let resolver = LwwResolver;
        #[cfg(feature = "spill")]
        let prefetched_spilled = prefetch_spilled_conflict_bytes(
            &engine.stripes[bucket],
            std::iter::once((hash, &key_bytes)),
            &resolver,
            engine.spill().map(Arc::as_ref),
        );
        let mut stripe = engine.stripes[bucket].write();
        let ctx = ApplyCtx {
            digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
            total_weight: &engine.total_weight,
            live_count: &engine.live_count,
            weigher: engine.weigher.as_ref(),
            tti_ms: engine.tti_ms,
            resolver: &resolver,
            #[cfg(feature = "spill")]
            prefetched_spilled: &prefetched_spilled,
            #[cfg(feature = "spill")]
            spill: engine.spill().map(Arc::as_ref),
            tombstone_ttl_ms: 60_000,
            tombstone_max_ttl_ms: 600_000,
            now_ms,
        };
        let _ = apply_locked(
            &mut stripe,
            &ctx,
            EntryKey {
                hash,
                key,
                key_bytes,
            },
            ver,
            Incoming::Tombstone,
        );
    }

    /// [`put`]'s counterpart for a test that needs a resolver other than
    /// [`LwwResolver`], in particular one whose [`ConflictResolver::merge`]
    /// can return `Some`.
    #[allow(clippy::too_many_arguments)]
    fn put_with_resolver<K, V>(
        engine: &Engine<K, V>,
        key: K,
        key_bytes: Bytes,
        value: V,
        ver: Hlc,
        expires_at_ms: Option<u64>,
        now_ms: u64,
        resolver: &dyn ConflictResolver,
    ) -> ApplyOutcome<K, V>
    where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let hash = hash_key_bytes(key_bytes.as_ref());
        let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
        let bucket = stripe_index_from_hash(hash);
        // Mirrors `Engine::apply_many`'s own prefetch (see `put` above), so
        // a merge against an already-spilled stored side, exercised
        // directly through this helper, reads its bytes back off-lock
        // exactly as the real write path does.
        #[cfg(feature = "spill")]
        let prefetched_spilled = prefetch_spilled_conflict_bytes(
            &engine.stripes[bucket],
            std::iter::once((hash, &key_bytes)),
            resolver,
            engine.spill().map(Arc::as_ref),
        );
        let ctx = ApplyCtx {
            digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
            total_weight: &engine.total_weight,
            live_count: &engine.live_count,
            weigher: engine.weigher.as_ref(),
            tti_ms: engine.tti_ms,
            resolver,
            #[cfg(feature = "spill")]
            prefetched_spilled: &prefetched_spilled,
            #[cfg(feature = "spill")]
            spill: engine.spill().map(Arc::as_ref),
            tombstone_ttl_ms: 60_000,
            tombstone_max_ttl_ms: 600_000,
            now_ms,
        };
        let (outcome, displaced_spilled) = {
            let mut stripe = engine.stripes[bucket].write();
            apply_locked(
                &mut stripe,
                &ctx,
                EntryKey {
                    hash,
                    key,
                    key_bytes,
                },
                ver,
                Incoming::Put {
                    value,
                    expires_at_ms,
                    encoded,
                },
            )
        };
        engine.note_spill_departure(displaced_spilled);
        outcome
    }

    /// Test-only resolver: whenever both sides carry a value, merges by
    /// taking the lexicographically greater decoded `String`, an
    /// intentionally trivial join-semilattice (`max` is commutative,
    /// associative, and idempotent) that lets a test assert the engine wires
    /// a real [`ConflictResolver::merge`] reply through end to end, including
    /// the redelivery no-op case. Falls back to plain `Hlc` order whenever
    /// either side is a tombstone or spilled.
    struct MaxStringResolver;

    impl ConflictResolver for MaxStringResolver {
        fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
            if a.ver >= b.ver { Winner::A } else { Winner::B }
        }

        fn merges(&self) -> bool {
            true
        }

        fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
            let (Some(av), Some(bv)) = (a.value, b.value) else {
                return None;
            };
            let da: String = postcard::from_bytes(av).expect("test value decodes");
            let db: String = postcard::from_bytes(bv).expect("test value decodes");
            let merged = da.max(db);
            Some(Merged {
                value: Bytes::from(postcard::to_stdvec(&merged).expect("merged value encodes")),
                expires_at_ms: None,
            })
        }
    }

    /// Test-only resolver whose `merge` always claims a merge, regardless of
    /// whether those bytes decode as the shard's value type, and whose
    /// `winner` falls back to plain `Hlc` order, like `LwwResolver`: the
    /// buggy-resolver shape [`resolve_conflict`]'s engine-enforced
    /// value-presence guard exists to contain.
    struct AlwaysMergeResolver {
        /// The bytes to hand back as the "merged" value; deliberately
        /// invalid postcard for the decode-failure test.
        value: &'static [u8],
    }

    impl ConflictResolver for AlwaysMergeResolver {
        fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
            if a.ver >= b.ver { Winner::A } else { Winner::B }
        }

        fn merges(&self) -> bool {
            true
        }

        fn merge(&self, _key: &[u8], _a: RecordView<'_>, _b: RecordView<'_>) -> Option<Merged> {
            Some(Merged {
                value: Bytes::from_static(self.value),
                expires_at_ms: None,
            })
        }
    }

    #[test]
    fn apply_locked_merge_of_identical_content_adopts_the_greater_version_or_no_ops() {
        let engine = engine_u32_string(u64::MAX, None);
        let resolver = MaxStringResolver;
        let va = hlc(1, 1);
        let vb = hlc(5, 2);
        let created =
            put_with_resolver(&engine, 1, key_bytes(1), "x".into(), va, None, 0, &resolver);
        assert!(matches!(created, ApplyOutcome::Put { created: true, .. }));

        // `max("x", "x") == "x"`: the merge reduces to bytes both sides
        // already share, so this is pure version reconciliation. `vb` is the
        // real greater version, so it is adopted verbatim.
        let reconciled =
            put_with_resolver(&engine, 1, key_bytes(1), "x".into(), vb, None, 0, &resolver);
        assert!(matches!(reconciled, ApplyOutcome::Put { .. }));
        let after_vb = engine
            .record_for(key_bytes(1).as_ref(), 0)
            .expect("key is live");
        assert_eq!(
            after_vb.ver, vb,
            "identical content adopts whichever input version is greater"
        );

        // `vc` carries the same content again but is the lesser version:
        // `sv` (now `vb`) is already the greater side, so this no-ops.
        let vc = hlc(3, 3);
        let no_op = put_with_resolver(&engine, 1, key_bytes(1), "x".into(), vc, None, 0, &resolver);
        assert!(
            matches!(no_op, ApplyOutcome::Rejected),
            "identical content at a lesser version is a no-op once the greater version is stored"
        );
        let still_stored = engine
            .record_for(key_bytes(1).as_ref(), 0)
            .expect("key is still live");
        assert_eq!(
            still_stored.ver, vb,
            "the stored version is untouched by the no-op"
        );
    }

    #[test]
    fn apply_locked_merge_that_reduces_to_incoming_adopts_its_exact_version() {
        let engine = engine_u32_string(u64::MAX, None);
        let resolver = MaxStringResolver;
        let va = hlc(1, 1);
        let vb = hlc(2, 2);
        let created =
            put_with_resolver(&engine, 1, key_bytes(1), "a".into(), va, None, 0, &resolver);
        assert!(matches!(created, ApplyOutcome::Put { created: true, .. }));

        let merged =
            put_with_resolver(&engine, 1, key_bytes(1), "z".into(), vb, None, 0, &resolver);
        let ApplyOutcome::Put { value, created, .. } = merged else {
            panic!("a Merged outcome must land as an ApplyOutcome::Put");
        };
        assert_eq!(value, "z", "max(\"a\", \"z\") == \"z\"");
        assert!(!created, "a merge replaces an already-live entry");
        assert_eq!(engine.get(&1, 0), Some("z".to_string()));

        let stored = engine
            .record_for(key_bytes(1).as_ref(), 0)
            .expect("merged key is live");
        assert_eq!(
            stored.ver, vb,
            "the merge reduces to incoming's own bytes and incoming is the real greater \
             version, so its exact version is adopted verbatim"
        );
        assert!(
            !stored.ver.node.is_merge_derived(),
            "adopting incoming's own version carries its real node id, never a minted one"
        );
    }

    #[test]
    fn apply_locked_merge_that_reduces_to_stored_is_a_no_op_when_stored_already_dominates() {
        let engine = engine_u32_string(u64::MAX, None);
        let resolver = MaxStringResolver;
        let va = hlc(5, 1);
        let created =
            put_with_resolver(&engine, 1, key_bytes(1), "z".into(), va, None, 0, &resolver);
        assert!(matches!(created, ApplyOutcome::Put { created: true, .. }));

        // `max("z", "a") == "z"`: the merge reduces to what's already
        // stored, and `sv` is the real greater version, so nothing changes.
        let vb = hlc(1, 2);
        let no_op = put_with_resolver(&engine, 1, key_bytes(1), "a".into(), vb, None, 0, &resolver);
        assert!(
            matches!(no_op, ApplyOutcome::Rejected),
            "incoming's content and version are both already fully absorbed"
        );
        let after = engine
            .record_for(key_bytes(1).as_ref(), 0)
            .expect("key is still live");
        assert_eq!(after.ver, va);
        assert_eq!(engine.get(&1, 0), Some("z".to_string()));
    }

    #[test]
    fn apply_locked_treats_a_redelivered_absorbed_merge_as_a_no_op() {
        let engine: Engine<u32, std::collections::BTreeSet<String>> =
            Engine::new(u64::MAX, None, None, None);
        let resolver = UnionSetResolver;
        let va = hlc(1, 1);
        let vb = hlc(5, 2);
        let _ = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            string_set(&["a"]),
            va,
            None,
            0,
            &resolver,
        );
        let _ = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            string_set(&["b"]),
            vb,
            None,
            0,
            &resolver,
        );
        let before = engine
            .record_for(key_bytes(1).as_ref(), 0)
            .expect("merged key is live");

        // Redeliver `vb`'s own value: its content is already fully absorbed
        // into the stored union (the merge reduces to stored), and the mint
        // above strictly outranks `vb` under `Hlc`'s real order even though
        // their `wall_ms` ties (the mint's `logical` is one higher), so
        // this lands on the no-op arm rather than re-publishing.
        let redelivered = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            string_set(&["b"]),
            vb,
            None,
            0,
            &resolver,
        );
        assert!(
            matches!(redelivered, ApplyOutcome::Rejected),
            "an absorbed input redelivered unchanged is a no-op, not a fresh write"
        );
        let after = engine
            .record_for(key_bytes(1).as_ref(), 0)
            .expect("merged key is still live");
        assert_eq!(before.ver, after.ver);
        assert_eq!(before.value, after.value);
    }

    /// Test-only resolver: merges by set union of postcard-decoded
    /// `BTreeSet<String>` values, a trivial join-semilattice standing in for
    /// a real CRDT like [`super::super::crdt::PnCounter`] or
    /// [`super::super::crdt::OrSet`]: content that grows whenever either
    /// side contributes an element the other lacks, unlike
    /// [`MaxStringResolver`]'s `max`, which can reproduce one side's bytes
    /// exactly and so cannot exercise the case below. `merges()` is `true`:
    /// besides the mint-arm test below, this is also `apply_many_prefold`'s
    /// resolver, since set union is a real join-semilattice (commutative,
    /// associative, idempotent) and so a fold-order-independence property
    /// test can lean on it.
    struct UnionSetResolver;

    impl ConflictResolver for UnionSetResolver {
        fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
            if a.ver >= b.ver { Winner::A } else { Winner::B }
        }

        fn merges(&self) -> bool {
            true
        }

        fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
            let (Some(av), Some(bv)) = (a.value, b.value) else {
                return None;
            };
            let da: std::collections::BTreeSet<String> =
                postcard::from_bytes(av).expect("test value decodes");
            let db: std::collections::BTreeSet<String> =
                postcard::from_bytes(bv).expect("test value decodes");
            let merged: std::collections::BTreeSet<String> = da.union(&db).cloned().collect();
            Some(Merged {
                value: Bytes::from(postcard::to_stdvec(&merged).expect("merged value encodes")),
                expires_at_ms: None,
            })
        }
    }

    fn string_set(elems: &[&str]) -> std::collections::BTreeSet<String> {
        elems.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn apply_locked_merge_that_grows_content_mints_a_strictly_greater_version_even_on_a_wall_ms_tie()
     {
        let engine: Engine<u32, std::collections::BTreeSet<String>> =
            Engine::new(u64::MAX, None, None, None);
        let resolver = UnionSetResolver;

        let va = hlc(1, 1);
        let vb = hlc(5, 2);
        let _ = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            string_set(&["a"]),
            va,
            None,
            0,
            &resolver,
        );
        let first = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            string_set(&["b"]),
            vb,
            None,
            0,
            &resolver,
        );
        assert!(matches!(first, ApplyOutcome::Put { .. }));

        let after_first = engine
            .record_for(key_bytes(1).as_ref(), 0)
            .expect("merged key is live");
        let first_bytes =
            postcard::to_stdvec(&string_set(&["a", "b"])).expect("test value encodes");
        assert_eq!(
            after_first.ver,
            Hlc {
                wall_ms: 5,
                logical: 1,
                node: NodeId::merge_derived(xxh3_64(&first_bytes)),
            },
            "the union of two disjoint sides mints wall_ms = max(inputs), \
             logical = max(inputs) + 1, and a node derived from the merged bytes"
        );

        // `vc`'s own content, "c", is absent from both prior inputs, and its
        // `wall_ms` ties the stored version's exactly: the case a bare
        // componentwise max cannot tell apart from a no-op. The `+ 1` on
        // `logical` at mint time keeps this merge strictly ahead of the
        // prior one despite the tie.
        let vc = hlc(5, 3);
        let second = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            string_set(&["c"]),
            vc,
            None,
            0,
            &resolver,
        );
        assert!(
            matches!(second, ApplyOutcome::Put { .. }),
            "a resolver-driven merge of two real values is never dropped"
        );

        let after_second = engine
            .record_for(key_bytes(1).as_ref(), 0)
            .expect("key is still live");
        assert!(
            after_second.ver > after_first.ver,
            "the second merge's version strictly outranks the first, even though wall_ms tied"
        );
        assert_eq!(
            engine.get(&1, 0),
            Some(string_set(&["a", "b", "c"])),
            "the write's content is never lost"
        );
    }

    #[test]
    fn resolve_conflict_never_consults_merge_against_a_tombstone() {
        let engine = engine_u32_string(u64::MAX, None);
        // Valid postcard for the `String` "hi": what would land if the
        // guard failed to hold and `merge` were wrongly consulted against
        // the tombstone below, instead of the ordinary Hlc-order `winner`
        // pick.
        let resolver = AlwaysMergeResolver {
            value: &[2, b'h', b'i'],
        };
        tombstone(&engine, 1, key_bytes(1), hlc(1, 1), 0);
        assert_eq!(engine.get(&1, 0), None);

        // Genuinely newer than the tombstone by `Hlc`: an ordinary
        // winner-only resolver would let this win outright, and this one
        // must too, since the tombstone's value-less side means `merge` is
        // never consulted no matter what `merges()` claims.
        let outcome = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            "new".into(),
            hlc(2, 2),
            None,
            0,
            &resolver,
        );
        assert!(matches!(outcome, ApplyOutcome::Put { .. }));
        assert_eq!(
            engine.get(&1, 0),
            Some("new".to_string()),
            "the incoming write's own value lands via the ordinary winner fallback, never the \
             resolver's synthesized merge bytes, since a tombstone's value-less side means \
             merge is never consulted"
        );
    }

    #[test]
    fn apply_locked_rejects_a_merge_whose_bytes_fail_to_decode() {
        let engine = engine_u32_string(u64::MAX, None);
        let lww = LwwResolver;
        let _ = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            "a".into(),
            hlc(1, 1),
            None,
            0,
            &lww,
        );
        // Not valid postcard for a `String`: an over-long varint length
        // prefix with no data behind it.
        let garbage = AlwaysMergeResolver {
            value: &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        };
        let outcome = put_with_resolver(
            &engine,
            1,
            key_bytes(1),
            "b".into(),
            hlc(2, 2),
            None,
            0,
            &garbage,
        );
        assert!(
            matches!(outcome, ApplyOutcome::Rejected),
            "bytes that fail to decode as V are rejected, never panicked on"
        );
        assert_eq!(
            engine.get(&1, 0),
            Some("a".to_string()),
            "a rejected merge leaves the previously stored value untouched"
        );
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
    fn for_each_key_skips_an_expired_entry_and_visits_every_live_key_exactly_once() {
        let engine = engine_u32_string(u64::MAX, None);
        let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
        let _ = put(&engine, 2, key_bytes(2), "b".into(), hlc(1, 1), None, 0);
        let _ = put(&engine, 3, key_bytes(3), "c".into(), hlc(1, 1), None, 0);

        let mut visited = Vec::new();
        engine.for_each_key(100, |k| visited.push(k));
        visited.sort_unstable();
        assert_eq!(
            visited,
            vec![2, 3],
            "the expired key is skipped, every live key is visited exactly once"
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
    fn removing_a_long_ttl_entry_also_clears_its_side_table_row() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let long_deadline = MAX_INLINE_TTL_MS + 5_000;
        let _ = put(
            &engine,
            1,
            kb.clone(),
            "a".into(),
            hlc(1, 1),
            Some(long_deadline),
            0,
        );
        assert!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "a long TTL write leaves its absolute deadline in the side table"
        );

        engine.invalidate_local(kb.as_ref(), hash);

        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "removing the entry clears its side-table row"
        );
    }

    #[test]
    fn sweep_of_an_expired_long_ttl_entry_clears_its_side_table_row() {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let deadline = MAX_INLINE_TTL_MS + 5_000;
        let _ = put(
            &engine,
            1,
            kb.clone(),
            "a".into(),
            hlc(1, 1),
            Some(deadline),
            0,
        );
        assert!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref())
        );

        engine.sweep(deadline + 1);

        assert_eq!(engine.get(&1, deadline + 1), None, "swept: reads as absent");
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "sweep clears the side-table row of what it expires"
        );
    }

    #[test]
    fn sweep_of_a_tti_idle_long_ttl_entry_clears_its_side_table_row() {
        // A TTI far shorter than the long-TTL deadline, so this entry
        // goes idle and is swept through absent_at's TTI branch first.
        let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let deadline = MAX_INLINE_TTL_MS + 5_000;
        let _ = put(
            &engine,
            1,
            kb.clone(),
            "a".into(),
            hlc(1, 1),
            Some(deadline),
            0,
        );
        assert!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "a long TTL write leaves its absolute deadline in the side table"
        );
        let (entries_before, _) = engine.debug_totals();
        assert_eq!(entries_before, 1);

        // Idle past the 100ms TTI, nowhere near `deadline`.
        engine.sweep(500);

        let (entries_after, _) = engine.debug_totals();
        assert_eq!(entries_after, 0, "sweep evicts the tti-idle entry");
        assert_eq!(engine.get(&1, 500), None, "swept: reads as absent");
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "sweep clears the side-table row of what it evicts for idleness, \
             not only what it expires"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one scenario per named overwrite site (apply_put, complete_fresh_load, \
                  compact_replace_if_current), matching the spec's single test name; splitting \
                  it would only scatter the shared long_deadline setup"
    )]
    fn overwriting_a_long_ttl_key_clears_its_stale_side_table_row() {
        let long_deadline = MAX_INLINE_TTL_MS + 5_000;

        // `apply_put`'s overwrite path.
        {
            let engine = engine_u32_string(u64::MAX, None);
            let kb = key_bytes(1);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let _ = put(
                &engine,
                1,
                kb.clone(),
                "a".into(),
                hlc(1, 1),
                Some(long_deadline),
                0,
            );
            assert!(
                engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref())
            );
            let _ = put(&engine, 1, kb.clone(), "b".into(), hlc(2, 1), None, 0);
            assert!(
                !engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref()),
                "apply_put clears the displaced entry's stale side-table row"
            );
        }

        // `complete_fresh_load`'s overwrite path.
        {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 2u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
                panic!("first caller becomes the owner");
            };
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "a".into(),
                hlc(1, 1),
                Some(long_deadline),
                0,
            );
            assert!(
                engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref())
            );
            let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
            let _ = engine.complete_fresh_load(
                FreshLoad {
                    key: &key,
                    key_bytes: &kb,
                    hash,
                    ver: hlc(2, 1),
                    value: "loaded".to_string(),
                    encoded,
                    expires_at_ms: None,
                },
                0,
                &inflight,
            );
            assert!(
                !engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref()),
                "complete_fresh_load clears the displaced entry's stale side-table row"
            );
        }

        // compact_replace_if_current's path: a wall_ms jump alone can make
        // a stored LONG_TTL row stale by making the deadline inline again.
        {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 3u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "a".into(),
                hlc(1, 1),
                Some(long_deadline),
                0,
            );
            assert!(
                engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref())
            );
            let new_ver = hlc(long_deadline - 10, 1);
            let replaced = engine.compact_replace_if_current(CompactReplace {
                key: &key,
                key_bytes: kb.as_ref(),
                hash,
                expected_ver: hlc(1, 1),
                new_ver,
                value: "compacted".to_string(),
                encoded: Bytes::from(postcard::to_stdvec("compacted").expect("encode")),
            });
            assert!(replaced);
            assert!(
                !engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref()),
                "compact_replace_if_current clears a row a wall_ms jump made stale"
            );
            assert_eq!(
                engine.get(&key, long_deadline - 1),
                Some("compacted".to_string()),
                "still readable just before its unchanged absolute deadline"
            );
            assert_eq!(
                engine.get(&key, long_deadline + 1),
                None,
                "expired exactly at its unchanged absolute deadline"
            );
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one scenario per named overwrite site (apply_put, complete_fresh_load), \
                  matching the spec's single test name; splitting it would only scatter the \
                  shared long_deadline setup"
    )]
    fn overwriting_a_short_ttl_key_with_a_long_ttl_value_installs_its_side_table_row() {
        let long_deadline = MAX_INLINE_TTL_MS + 5_000;

        // `apply_put`'s overwrite path: a short-TTL live entry, then a fresh
        // write past `MAX_INLINE_TTL_MS` lands on the same key.
        {
            let engine = engine_u32_string(u64::MAX, None);
            let kb = key_bytes(1);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let _ = put(
                &engine,
                1,
                kb.clone(),
                "a".into(),
                hlc(1, 1),
                Some(5_000),
                0,
            );
            assert!(
                !engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref()),
                "the displaced entry's TTL fit inline: no side-table row yet"
            );
            let _ = put(
                &engine,
                1,
                kb.clone(),
                "b".into(),
                hlc(2, 1),
                Some(long_deadline),
                0,
            );
            assert_eq!(
                engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .get(kb.as_ref())
                    .copied(),
                Some(long_deadline),
                "apply_put installs a fresh side-table row when the new write is the long one"
            );
            assert_eq!(
                engine.get(&1, long_deadline - 1),
                Some("b".to_string()),
                "still readable just before its long deadline"
            );
            assert_eq!(
                engine.get(&1, long_deadline + 1),
                None,
                "expired exactly at its long deadline"
            );
        }

        // `complete_fresh_load`'s overwrite path: a no-TTL live entry, then a
        // fresh load installs a long TTL.
        {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 2u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
                panic!("first caller becomes the owner");
            };
            let _ = put(&engine, key, kb.clone(), "a".into(), hlc(1, 1), None, 0);
            assert!(
                !engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref()),
                "the displaced entry never expires: no side-table row yet"
            );
            let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
            let _ = engine.complete_fresh_load(
                FreshLoad {
                    key: &key,
                    key_bytes: &kb,
                    hash,
                    ver: hlc(2, 1),
                    value: "loaded".to_string(),
                    encoded,
                    expires_at_ms: Some(long_deadline),
                },
                0,
                &inflight,
            );
            assert_eq!(
                engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .get(kb.as_ref())
                    .copied(),
                Some(long_deadline),
                "complete_fresh_load installs a fresh side-table row when the new write is the long one"
            );
            assert_eq!(
                engine.get(&key, long_deadline - 1),
                Some("loaded".to_string()),
                "still readable just before its long deadline"
            );
            assert_eq!(
                engine.get(&key, long_deadline + 1),
                None,
                "expired exactly at its long deadline"
            );
        }
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
        let engine = Engine::<u32, String>::new(20, None, Some(weigher), None);
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

        engine.enforce_capacity(target_bucket, 500);

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
        tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);
        assert_eq!(engine.get(&key, 0), None);

        let bucket_u16 = u16::try_from(bucket).expect("invariant: bucket < BUCKET_COUNT");
        let entries = engine.collect_buckets(&[bucket_u16], 0);
        assert_eq!(entries.len(), 1);
        let (_, records) = &entries[0];
        assert!(
            records
                .iter()
                .any(|kv| kv.key.as_ref() == kb.as_ref() && kv.version == hlc(2, 1)),
            "a removed key still appears in its bucket's entries, carrying the tombstone's \
             version: {records:?}"
        );
    }

    /// A pair of `u32` keys, scanned from `1` up to `limit`, that land in
    /// the same bucket, plus that bucket's own index.
    fn same_bucket_pair(limit: u32) -> (u32, u32, usize) {
        let mut by_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
        for k in 1..limit {
            let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
            let group = by_bucket.entry(bucket).or_default();
            group.push(k);
            if group.len() == 2 {
                return (group[0], group[1], bucket);
            }
        }
        panic!("no colliding pair found within the first {limit} keys");
    }

    #[test]
    fn release_buckets_removes_every_live_entry_and_tombstone_and_resets_the_digest() {
        let engine = engine_u32_string(u64::MAX, None);
        let (live_key, tomb_key, bucket) = same_bucket_pair(100_000);
        let bucket_u16 = u16::try_from(bucket).expect("invariant: bucket < BUCKET_COUNT");

        let _ = put(
            &engine,
            live_key,
            key_bytes(live_key),
            "live".to_string(),
            hlc(1, 1),
            None,
            0,
        );
        tombstone(&engine, tomb_key, key_bytes(tomb_key), hlc(1, 1), 0);

        let removed = engine.release_buckets(&[bucket_u16]);

        assert_eq!(removed, 2, "one live entry and one tombstone removed");
        assert_eq!(engine.get(&live_key, 0), None, "the live entry is gone");
        assert!(
            engine.collect_buckets(&[bucket_u16], 0)[0].1.is_empty(),
            "the tombstone is gone too: nothing is left in the released bucket"
        );
        assert_eq!(engine.debug_totals(), (0, 0));
        for part in 0..PART_COUNT {
            assert_eq!(
                engine.digest[digest_slot(bucket, part)].load(Ordering::Relaxed),
                0,
                "every part digest in the released bucket is reset to zero"
            );
        }
    }

    #[test]
    fn release_buckets_leaves_a_different_buckets_digest_and_entry_untouched() {
        let engine = engine_u32_string(u64::MAX, None);
        let (live_key, _, released_bucket) = same_bucket_pair(100_000);
        let other_key = (1..100_000u32)
            .find(|&k| {
                stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) != released_bucket
            })
            .expect("a distinct bucket is found quickly");
        let other_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(other_key).as_ref()));
        let other_part = part_index_from_hash(hash_key_bytes(key_bytes(other_key).as_ref()));

        let _ = put(
            &engine,
            live_key,
            key_bytes(live_key),
            "live".to_string(),
            hlc(1, 1),
            None,
            0,
        );
        let _ = put(
            &engine,
            other_key,
            key_bytes(other_key),
            "untouched".to_string(),
            hlc(1, 1),
            None,
            0,
        );
        let digest_before =
            engine.digest[digest_slot(other_bucket, other_part)].load(Ordering::Relaxed);

        let removed =
            engine.release_buckets(&[u16::try_from(released_bucket).expect("invariant: fits")]);

        assert_eq!(removed, 1);
        assert_eq!(
            engine.digest[digest_slot(other_bucket, other_part)].load(Ordering::Relaxed),
            digest_before,
            "an untouched bucket's digest is unaffected"
        );
        assert_eq!(
            engine.get(&other_key, 0),
            Some("untouched".to_string()),
            "a different bucket's entry survives release"
        );
    }

    #[test]
    fn release_buckets_skips_a_bucket_at_or_past_bucket_count_and_returns_zero() {
        let engine = engine_u32_string(u64::MAX, None);
        let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
        let out_of_range = u16::try_from(BUCKET_COUNT).expect("invariant: BUCKET_COUNT fits u16");

        let removed = engine.release_buckets(&[out_of_range]);

        assert_eq!(removed, 0, "a bucket past BUCKET_COUNT removes nothing");
        assert_eq!(
            engine.get(&1, 0),
            Some("a".to_string()),
            "an out-of-range bucket touches nothing"
        );
    }

    #[test]
    fn capacity_eviction_rotates_past_an_empty_start_bucket_into_other_stripes() {
        let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
        let engine = Engine::<u32, String>::new(3, None, Some(weigher), None);

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

        engine.enforce_capacity(start_bucket, 500);

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
                            let outcome = engine.complete_fresh_load(
                                FreshLoad {
                                    key: &key,
                                    key_bytes: &kb,
                                    hash,
                                    ver: hlc(1, 1),
                                    value: value.clone(),
                                    encoded,
                                    expires_at_ms: None,
                                },
                                0,
                                &inflight,
                            );
                            assert_eq!(
                                outcome,
                                FillOutcome::Installed { had_live: false },
                                "a genuine miss sees no prior live entry"
                            );
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
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: hlc(1, 1),
                value: "late".to_string(),
                encoded,
                expires_at_ms: None,
            },
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

        let _ = put(&engine, key, kb.clone(), "old".into(), hlc(1, 1), None, 0);
        tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);
        assert_eq!(
            engine.get(&key, 0),
            None,
            "a tombstone sits where the load will land"
        );

        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("no live entry: this caller becomes the owner");
        };
        let encoded = Bytes::from(postcard::to_stdvec("fresh").expect("encode"));
        let outcome = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: hlc(3, 1),
                value: "fresh".to_string(),
                encoded,
                expires_at_ms: None,
            },
            0,
            &inflight,
        );
        assert_eq!(
            outcome,
            FillOutcome::Installed { had_live: false },
            "an older tombstone gives way, and it is not a live entry"
        );
        assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        let (entries, weight) = engine.debug_totals();
        assert_eq!(entries, 1);
        assert_eq!(weight, 1);
    }

    #[test]
    fn fill_supersedes_only_an_absent_or_older_entry() {
        assert!(fill_supersedes(None, hlc(1, 1)), "nothing to displace");
        assert!(
            fill_supersedes(Some(hlc(1, 1)), hlc(2, 1)),
            "an older entry gives way"
        );
        assert!(
            !fill_supersedes(Some(hlc(3, 1)), hlc(2, 1)),
            "a newer entry that landed during the load stays"
        );
        assert!(
            !fill_supersedes(Some(hlc(2, 1)), hlc(2, 1)),
            "the same version is not newer"
        );
    }

    #[test]
    fn complete_fresh_load_keeps_a_newer_write_that_landed_during_the_load() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 22u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());

        assert_eq!(engine.get(&key, 0), None);
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };

        // A write lands on the same key while the load is in flight, e.g. a
        // replicated write racing the local loader, stamped after the fill.
        let _ = put(
            &engine,
            key,
            kb.clone(),
            "raced-in".into(),
            hlc(5, 2),
            None,
            0,
        );

        let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
        let outcome = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: hlc(1, 1),
                value: "loaded".to_string(),
                encoded,
                expires_at_ms: None,
            },
            0,
            &inflight,
        );
        assert_eq!(outcome, FillOutcome::Superseded);
        assert_eq!(
            engine.get(&key, 0),
            Some("raced-in".to_string()),
            "the newer write keeps its place over the older fill"
        );
        assert!(
            matches!(engine.miss_or_join(&kb, hash, 0), JoinOutcome::Hit(_)),
            "the superseded fill left no in-flight entry behind"
        );
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        let (entries, weight) = engine.debug_totals();
        assert_eq!(entries, 1);
        assert_eq!(weight, 1);
    }

    #[test]
    fn complete_fresh_load_keeps_a_newer_tombstone_that_landed_during_the_load() {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 23u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());

        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        // A removal lands while the loader still reads the old row.
        tombstone(&engine, key, kb.clone(), hlc(4, 1), 0);

        let encoded = Bytes::from(postcard::to_stdvec("stale").expect("encode"));
        let outcome = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: hlc(2, 1),
                value: "stale".to_string(),
                encoded,
                expires_at_ms: None,
            },
            0,
            &inflight,
        );
        assert_eq!(outcome, FillOutcome::Superseded);
        assert_eq!(
            engine.get(&key, 0),
            None,
            "the removal stays removed; the stale fill never resurrects the key"
        );
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
        let (entries, weight) = engine.debug_totals();
        assert_eq!(entries, 0);
        assert_eq!(weight, 0);
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
                let _ = put(
                    &engine,
                    k,
                    kb,
                    k.to_string(),
                    hlc(u64::from(k) + 1, 1),
                    None,
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
        assert_eq!(
            mixed[0].1,
            vec![KeyVersion {
                key: kb,
                version: hlc(1, 1)
            }]
        );
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
        let engine = Engine::<u32, String>::new(10_000, None, Some(weigher), None);
        // 6,000 one-unit entries, then one warmer 9,500-unit entry: back
        // under the cap only after more than 5,500 evictions of cold ones.
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

        engine.enforce_capacity(start_bucket, 100);

        let (entries, weight) = engine.debug_totals();
        assert!(
            weight <= 10_000,
            "total weight {weight} is back under the cap"
        );
        assert!(
            entries <= 501,
            "{entries} entries remain; the warm big entry was evicted instead of cold ones"
        );
    }

    #[test]
    fn defer_to_flusher_true_only_while_pending_spill_weight_is_positive() {
        assert!(
            !defer_to_flusher(0),
            "nothing pending: enforce_capacity's ordinary scanning fallback runs"
        );
        assert!(
            defer_to_flusher(1),
            "anything still pending: trust the flusher rather than scan every stripe"
        );
        assert!(defer_to_flusher(u64::MAX));
    }

    #[test]
    fn should_shrink_stripe_true_well_below_its_allocation() {
        assert!(
            should_shrink_stripe(1, 64),
            "1/64 is well below the eighth threshold"
        );
        assert!(
            should_shrink_stripe(0, 100),
            "a fully drained stripe is always well below its own allocation"
        );
        assert!(
            should_shrink_stripe(8, 64),
            "8/64 sits exactly at the eighth threshold, still shrunk"
        );
    }

    #[test]
    fn should_shrink_stripe_false_near_or_above_its_allocation() {
        assert!(
            !should_shrink_stripe(64, 64),
            "at capacity: nothing to reclaim"
        );
        assert!(
            !should_shrink_stripe(40, 64),
            "40/64 is well past the eighth threshold"
        );
        assert!(
            !should_shrink_stripe(9, 64),
            "9/64 sits one entry past the eighth threshold"
        );
        assert!(
            !should_shrink_stripe(0, 0),
            "never allocated: nothing to shrink further"
        );
    }

    #[test]
    fn deficit_once_still_over_capacity_zeroes_a_deficit_the_cap_no_longer_needs() {
        assert_eq!(
            deficit_once_still_over_capacity(35, 30, 15),
            15,
            "still 5 units over the cap: the deficit is still owed"
        );
        assert_eq!(
            deficit_once_still_over_capacity(25, 30, 15),
            0,
            "already under the cap, by the batch's own other progress or an unrelated \
             concurrent flusher install: the stale deficit is dropped rather than paid down \
             with an avoidable extra reserve() wait"
        );
        assert_eq!(
            deficit_once_still_over_capacity(30, 30, 15),
            0,
            "exactly at the cap counts as satisfied, matching enforce_capacity_with_reservation's \
             own loop-top `current <= max_capacity` check"
        );
        assert_eq!(
            deficit_once_still_over_capacity(31, 30, 15),
            15,
            "one unit over is still over: the deficit survives unchanged"
        );
    }

    #[test]
    fn eviction_batch_size_takes_the_fewest_entries_that_clear_the_overage() {
        assert_eq!(
            eviction_batch_size(0, &[3, 3, 3]),
            0,
            "no overage evicts nothing"
        );
        assert_eq!(
            eviction_batch_size(2, &[5, 5, 5]),
            1,
            "the first sampled entry alone already clears a 2-unit overage"
        );
        assert_eq!(
            eviction_batch_size(6, &[3, 3, 3]),
            2,
            "3 clears none of a 6-unit overage, 3+3 clears all of it"
        );
        assert_eq!(
            eviction_batch_size(100, &[1, 1, 1]),
            2,
            "a large overage takes only the colder half of the sample"
        );
        assert_eq!(
            eviction_batch_size(100, &[1; 32]),
            EVICTION_BATCH,
            "a full sample evicts at most EVICTION_BATCH"
        );
        assert_eq!(
            eviction_batch_size(100, &[7]),
            1,
            "a lone sampled entry is evicted, as the single-entry path did"
        );
        assert_eq!(
            eviction_batch_size(5, &[]),
            0,
            "nothing sampled means nothing to evict"
        );
    }

    #[test]
    fn enforce_capacity_batches_evictions_under_a_burst_ten_thousand_over_the_cap() {
        let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
        // A 40,000-unit cap left dense after eviction (~39 entries/stripe)
        // so a random probe rarely lands on an already-empty stripe; the
        // point here is measuring the batch size, not the scanning
        // fallback's cost on a nearly-drained table.
        let engine = Engine::<u32, String>::new(40_000, None, Some(weigher), None);
        // 50,000 one-unit entries: 10,000 over the cap.
        for k in 1..=50_000u32 {
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
        let (entries_before, weight_before) = engine.debug_totals();
        assert_eq!(entries_before, 50_000);
        assert_eq!(weight_before, 50_000);

        engine.enforce_capacity(0, 0);

        let (entries_after, weight_after) = engine.debug_totals();
        assert!(
            weight_after <= 40_000,
            "total weight {weight_after} is back under the cap"
        );
        let evicted = entries_before - entries_after;
        let acquisitions = engine.debug_eviction_lock_acquisitions();
        assert!(
            acquisitions * 2 < evicted,
            "{acquisitions} lock acquisitions to evict {evicted} entries: batching should need \
             far fewer acquisitions than entries evicted"
        );
    }

    #[test]
    fn engine_reads_back_an_arc_string_value_serialized_via_serdes_rc_feature() {
        let engine = Engine::<u32, Arc<String>>::new(u64::MAX, None, None, None);
        let value = Arc::new("shared".to_string());
        let _ = put(
            &engine,
            1,
            key_bytes(1),
            Arc::clone(&value),
            hlc(1, 1),
            None,
            0,
        );
        assert_eq!(
            engine.get(&1, 0),
            Some(value),
            "an Arc<String> value round-trips through postcard's serde `rc` support"
        );
    }

    #[test]
    fn enforce_capacity_stops_once_every_stripe_is_empty() {
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(5, None, Some(weigher), None);
        // One entry heavier than the whole cap: evicting it is all there is.
        let _ = put(&engine, 1, key_bytes(1), "z".repeat(50), hlc(1, 1), None, 0);
        engine.enforce_capacity(0, 0);
        assert_eq!(engine.debug_totals(), (0, 0));
        assert!(
            engine.evict_one_sampled(0, 0).made_no_progress(),
            "an empty stripe evicts nothing"
        );
        assert_eq!(engine.evict_one_scanning(0, 0), None);
    }

    /// [`digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions`]'s
    /// shared step: applies `incoming` directly through [`apply_locked`],
    /// under a fresh stripe lock, with a 1s/10s tombstone deadline and an
    /// [`LwwResolver`].
    fn apply_direct<K, V>(
        engine: &Engine<K, V>,
        bucket: usize,
        entry: EntryKey<K>,
        ver: Hlc,
        incoming: Incoming<V>,
        now_ms: u64,
    ) where
        K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let hash = entry.hash;
        let mut stripe = engine.stripe_lock(bucket).write();
        let resolver = LwwResolver;
        let apply_ctx = test_ctx(engine, bucket, hash, &resolver, 1_000, 10_000, now_ms);
        let _ = apply_locked(&mut stripe, &apply_ctx, entry, ver, incoming);
    }

    #[test]
    fn digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions() {
        use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

        let weigher: Weigher<u32, u64> = Box::new(|_k, _v| 1);
        let engine = Engine::<u32, u64>::new(12, None, Some(weigher), None);
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
                    apply_direct(
                        &engine,
                        bucket,
                        EntryKey {
                            hash,
                            key,
                            key_bytes: kb,
                        },
                        ver,
                        Incoming::Put {
                            value,
                            expires_at_ms: Some(now + 500),
                            encoded,
                        },
                        now,
                    );
                    engine.enforce_capacity(bucket, now);
                }
                1 => {
                    let ver = clock.now(now);
                    apply_direct(
                        &engine,
                        bucket,
                        EntryKey {
                            hash,
                            key,
                            key_bytes: kb,
                        },
                        ver,
                        Incoming::Tombstone,
                        now,
                    );
                }
                2 => engine.sweep(now),
                _ => engine.gc_tombstones(false, now),
            }
            assert_eq!(
                engine.live_entry_count(),
                engine.recompute_live_entry_count(),
                "iteration {i}: the incrementally maintained live count diverged from a full \
                 recount"
            );
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

    /// One key's mirror state for the interleaving test below.
    enum MirrorState {
        Live(String),
        Tombstoned,
    }

    /// Pins that `get_by_bytes`/`keys`/`digests`/`bucket_len` agree with a
    /// fixed-seed put/overwrite/tombstone/evict interleaving's own mirror.
    #[test]
    fn apply_locked_put_overwrite_tombstone_evict_interleaving_matches_pre_slab_behavior() {
        use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(60, None, Some(weigher), None);
        let mut rng = StdRng::seed_from_u64(0x5AB5_1AB5);
        let mut clock = HlcClock::new(NodeId::from(1));
        let mut mirror: HashMap<u32, MirrorState> = HashMap::new();

        for i in 0..2000u64 {
            let key = rng.random_range(0..16u32);
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let now = i * 10;
            match rng.random_range(0..4u32) {
                // Put and overwrite are the same call: a fresh HLC tick
                // always wins.
                0 | 1 => {
                    let ver = clock.now(now);
                    let value = format!("v{key}-{i}");
                    let _ = put(&engine, key, kb, value.clone(), ver, None, now);
                    mirror.insert(key, MirrorState::Live(value));
                    engine.enforce_capacity(bucket, now);
                }
                2 => {
                    let ver = clock.now(now);
                    tombstone(&engine, key, kb, ver, now);
                    mirror.insert(key, MirrorState::Tombstoned);
                }
                _ => {
                    let _ = engine.evict_one_sampled(bucket, now);
                }
            }

            // enforce_capacity/evict_one_sampled can remove any resident
            // entry, so resync every Live mirror row before checking below.
            mirror.retain(|&k, state| match state {
                MirrorState::Live(_) => {
                    let kb = key_bytes(k);
                    let khash = hash_key_bytes(kb.as_ref());
                    engine.get_by_bytes(kb.as_ref(), khash, now).is_some()
                }
                MirrorState::Tombstoned => true,
            });

            for (&k, state) in &mirror {
                if let MirrorState::Live(v) = state {
                    let kb = key_bytes(k);
                    let khash = hash_key_bytes(kb.as_ref());
                    assert_eq!(
                        engine.get_by_bytes(kb.as_ref(), khash, now),
                        Some(v.clone()),
                        "iteration {i}: key {k} diverged from the mirror"
                    );
                }
            }

            let mut expected_keys: Vec<u32> = mirror
                .iter()
                .filter_map(|(&k, state)| matches!(state, MirrorState::Live(_)).then_some(k))
                .collect();
            expected_keys.sort_unstable();
            let mut actual_keys = engine.keys(now);
            actual_keys.sort_unstable();
            assert_eq!(
                actual_keys, expected_keys,
                "iteration {i}: keys() diverged from the mirror"
            );

            assert_eq!(
                engine.digests(),
                engine.recompute_digests_paired(),
                "iteration {i}: digests() diverged from a full recompute"
            );

            let mut expected_bucket_counts = vec![0usize; BUCKET_COUNT];
            for &k in mirror.keys() {
                let kb = key_bytes(k);
                let khash = hash_key_bytes(kb.as_ref());
                expected_bucket_counts[stripe_index_from_hash(khash)] += 1;
            }
            for (b, &expected) in expected_bucket_counts.iter().enumerate() {
                let bucket_u16 = u16::try_from(b).expect("fits");
                assert_eq!(
                    engine.bucket_len(bucket_u16),
                    expected,
                    "iteration {i}: bucket {b}'s len diverged from the mirror"
                );
            }
        }
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
        tombstone(&engine, key_b, kb_b.clone(), hlc(2, 1), 0);

        let bucket_u16 = u16::try_from(bucket).expect("fits");
        let req_a = BucketPart {
            bucket: bucket_u16,
            part: u8::try_from(part_a).expect("fits"),
        };
        let req_b = BucketPart {
            bucket: bucket_u16,
            part: u8::try_from(part_b).expect("fits"),
        };
        let result = engine.collect_parts(&[req_a, req_b], 0);
        assert_eq!(result.len(), 2);
        let a_entries = &result
            .iter()
            .find(|(key, _)| *key == req_a)
            .expect("part_a present")
            .1;
        assert_eq!(
            a_entries,
            &vec![KeyVersion {
                key: kb_a,
                version: hlc(1, 1)
            }]
        );
        let b_entries = &result
            .iter()
            .find(|(key, _)| *key == req_b)
            .expect("part_b present")
            .1;
        assert_eq!(
            b_entries,
            &vec![KeyVersion {
                key: kb_b,
                version: hlc(2, 1)
            }],
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
            engine
                .collect_parts(
                    &[BucketPart {
                        bucket: u16::MAX,
                        part
                    }],
                    0
                )
                .is_empty(),
            "a bucket past BUCKET_COUNT yields nothing"
        );
        assert!(
            engine
                .collect_parts(
                    &[BucketPart {
                        bucket,
                        part: u8::MAX
                    }],
                    0
                )
                .is_empty(),
            "a part past PART_COUNT yields nothing"
        );
        let mixed = engine.collect_parts(
            &[
                BucketPart {
                    bucket: u16::MAX,
                    part,
                },
                BucketPart { bucket, part },
                BucketPart {
                    bucket,
                    part: u8::MAX,
                },
            ],
            0,
        );
        assert_eq!(
            mixed.len(),
            1,
            "only the in-range (bucket, part) is answered"
        );
        assert_eq!(mixed[0].0, BucketPart { bucket, part });
        assert_eq!(
            mixed[0].1,
            vec![KeyVersion {
                key: kb,
                version: hlc(1, 1)
            }]
        );
    }

    #[test]
    fn is_resident_true_for_resident_false_for_spilled() {
        let resident = Live::<u32, String>::new(
            build_resident_record(key_bytes(1).as_ref(), b"v"),
            hlc(1, 1),
            NEVER_EXPIRES,
            1,
            0,
            #[cfg(feature = "spill")]
            EntryState::Resident,
        );
        assert!(is_resident(&resident));

        #[cfg(feature = "spill")]
        {
            let spilled = Live::<u32, String>::new(
                build_key_only_record(key_bytes(1).as_ref()),
                hlc(1, 1),
                NEVER_EXPIRES,
                0,
                0,
                EntryState::Spilled(SpillLoc {
                    region: 0,
                    offset: 0,
                    len: 1,
                    generation: 0,
                }),
            );
            assert!(!is_resident(&spilled));
        }
    }

    #[test]
    fn is_spill_candidate_true_only_for_a_resident_entry_with_nonzero_weight() {
        let resident_hot = Live::<u32, String>::new(
            build_resident_record(key_bytes(1).as_ref(), b"v"),
            hlc(1, 1),
            NEVER_EXPIRES,
            3,
            0,
            #[cfg(feature = "spill")]
            EntryState::Resident,
        );
        assert!(is_spill_candidate(&resident_hot));

        let resident_pending = Live::<u32, String>::new(
            build_resident_record(key_bytes(1).as_ref(), b"v"),
            hlc(1, 1),
            NEVER_EXPIRES,
            0,
            0,
            #[cfg(feature = "spill")]
            EntryState::Resident,
        );
        assert!(
            !is_spill_candidate(&resident_pending),
            "weight zero means a hand-off to the spill tier is already in flight"
        );

        #[cfg(feature = "spill")]
        {
            let spilled = Live::<u32, String>::new(
                build_key_only_record(key_bytes(1).as_ref()),
                hlc(1, 1),
                NEVER_EXPIRES,
                0,
                0,
                EntryState::Spilled(SpillLoc {
                    region: 0,
                    offset: 0,
                    len: 1,
                    generation: 0,
                }),
            );
            assert!(!is_spill_candidate(&spilled));
        }
    }

    #[test]
    fn build_resident_record_round_trips_key_and_value() {
        for (key_bytes, value_bytes) in [
            (&b""[..], &b""[..]),
            (&b"k"[..], &b"v"[..]),
            (&b"a-longer-key"[..], &b"a-longer-value-too"[..]),
        ] {
            let record = build_resident_record(key_bytes, value_bytes);
            assert_eq!(record_key(&record), key_bytes);
            assert_eq!(record_key_bytes(&record).as_ref(), key_bytes);
            assert_eq!(record_value_bytes(&record).as_ref(), value_bytes);
        }
    }

    #[test]
    fn split_record_recovers_the_value_half_at_every_key_length_boundary() {
        // 127 and below encode LEN in one postcard varint byte; 128 and
        // above spill into a second, crossing the continuation-bit boundary.
        for key_len in [0usize, 1, 127, 128, 129] {
            let key_bytes = vec![b'k'; key_len];
            let value_bytes = b"value".to_vec();
            let record = build_resident_record(&key_bytes, &value_bytes);
            let (key_half, value_half) = split_record(&record);
            assert_eq!(key_half, key_bytes.as_slice(), "key_len = {key_len}");
            assert_eq!(value_half, value_bytes.as_slice(), "key_len = {key_len}");
        }
    }

    #[test]
    fn build_key_only_record_produces_an_empty_value_half() {
        let record = build_key_only_record(b"a-key");
        let (key_half, value_half) = split_record(&record);
        assert_eq!(key_half, b"a-key");
        assert!(value_half.is_empty());
    }

    #[test]
    fn record_value_matches_split_record_and_is_zero_copy() {
        for (key_bytes, value_bytes) in [
            (&b""[..], &b""[..]),
            (&b"k"[..], &b"v"[..]),
            (&b"a-longer-key"[..], &b"a-longer-value-too"[..]),
        ] {
            let record = build_resident_record(key_bytes, value_bytes);
            let value = record_value(&record);
            assert_eq!(value, split_record(&record).1);
            assert_eq!(value, value_bytes);
        }
    }

    #[test]
    fn encode_expiry_round_trips_never_a_short_delta_and_the_max_inline_ttl_boundary() {
        let wall_ms = 1_000u64;
        let empty: HashMap<Bytes, u64> = HashMap::new();

        let (expiry, side_table) = encode_expiry(wall_ms, None);
        assert_eq!(expiry, NEVER_EXPIRES);
        assert_eq!(side_table, None);
        assert_eq!(decode_expiry(wall_ms, expiry, b"k", &empty), None);

        for delta in [0u64, 1, 50, MAX_INLINE_TTL_MS] {
            let exp = wall_ms + delta;
            let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
            assert_eq!(side_table, None, "delta = {delta} fits inline");
            assert_ne!(expiry, NEVER_EXPIRES, "delta = {delta}");
            assert_ne!(expiry, LONG_TTL, "delta = {delta}");
            assert_eq!(
                decode_expiry(wall_ms, expiry, b"k", &empty),
                Some(exp),
                "delta = {delta}"
            );
        }
    }

    #[test]
    fn encode_expiry_falls_back_to_the_long_ttl_marker_past_max_inline_ttl_ms() {
        let wall_ms = 1_000u64;
        for exp in [
            wall_ms + MAX_INLINE_TTL_MS + 1,
            wall_ms + MAX_INLINE_TTL_MS + 1_000,
            u64::MAX,
        ] {
            let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
            assert_eq!(expiry, LONG_TTL, "exp = {exp}");
            assert_eq!(side_table, Some(exp), "exp = {exp}");
        }
    }

    #[test]
    fn encode_expiry_falls_back_to_the_long_ttl_marker_for_a_dead_on_arrival_expiry_before_wall_ms()
    {
        // expires_at_ms < wall_ms happens (a dead-on-arrival TTL); expiry
        // must not silently round up to wall_ms.
        let wall_ms = 20_000u64;
        for exp in [0u64, wall_ms - 1, wall_ms - 10_135] {
            let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
            assert_eq!(expiry, LONG_TTL, "exp = {exp}");
            assert_eq!(side_table, Some(exp), "exp = {exp}");
            let mut long_ttl: HashMap<Bytes, u64> = HashMap::new();
            long_ttl.insert(Bytes::from_static(b"k"), exp);
            assert_eq!(
                decode_expiry(wall_ms, expiry, b"k", &long_ttl),
                Some(exp),
                "exp = {exp}"
            );
        }
    }

    #[test]
    fn decode_expiry_reads_the_long_ttl_side_table_for_the_long_ttl_marker() {
        let wall_ms = 1_000u64;
        let mut long_ttl: HashMap<Bytes, u64> = HashMap::new();
        long_ttl.insert(key_bytes(1), 9_999_999_999u64);
        assert_eq!(
            decode_expiry(wall_ms, LONG_TTL, key_bytes(1).as_ref(), &long_ttl),
            Some(9_999_999_999)
        );
        assert_eq!(
            decode_expiry(wall_ms, LONG_TTL, key_bytes(2).as_ref(), &long_ttl),
            None,
            "no row for this key: never invents a deadline that isn't there"
        );
    }

    #[test]
    fn idle_elapsed_ms_is_correct_across_a_touch_stamp_rollover() {
        // No rollover: an ordinary elapsed duration, well under u32::MAX.
        assert_eq!(idle_elapsed_ms(1_000, touch_stamp(400)), 600);
        assert_eq!(idle_elapsed_ms(1_000, touch_stamp(1_000)), 0);

        // now_ms crossed one u32::MAX rollover after the touch: both
        // truncate to their low 32 bits, so wrapping subtraction still works.
        let touched_at = u64::from(u32::MAX) - 50;
        let now = touched_at + 200;
        assert!(
            now > u64::from(u32::MAX),
            "now must actually cross the rollover for this case to mean anything"
        );
        assert_eq!(idle_elapsed_ms(now, touch_stamp(touched_at)), 200);

        // The touch lands just past a rollover, exercised on the truncated
        // stamp directly.
        let last_access_ms: u32 = 5; // wrapped just past 0
        let now_ms = (1u64 << 32) + 105; // one rollover past the touch, true elapsed: 100ms
        assert_eq!(idle_elapsed_ms(now_ms, last_access_ms), 100);
    }

    #[test]
    fn clamp_tti_ms_caps_a_tti_past_max_inline_ttl_ms_to_it() {
        assert_eq!(
            clamp_tti_ms(1_000),
            1_000,
            "well under the ceiling: untouched"
        );
        assert_eq!(
            clamp_tti_ms(MAX_INLINE_TTL_MS),
            MAX_INLINE_TTL_MS,
            "exactly at the ceiling: untouched"
        );
        assert_eq!(
            clamp_tti_ms(MAX_INLINE_TTL_MS + 1),
            MAX_INLINE_TTL_MS,
            "one past the ceiling: capped"
        );
        assert_eq!(
            clamp_tti_ms(u64::MAX),
            MAX_INLINE_TTL_MS,
            "far past the ceiling: capped"
        );
    }

    #[test]
    fn engine_new_clamps_a_configured_tti_past_max_inline_ttl_ms() {
        let past_ceiling = Duration::from_millis(MAX_INLINE_TTL_MS + 10_000);
        let engine = engine_u32_string(u64::MAX, Some(past_ceiling));
        assert_eq!(engine.tti_ms, Some(MAX_INLINE_TTL_MS));
    }

    #[test]
    fn engine_capacity_hint_presizes_each_stripe_to_ceil_hint_over_bucket_count() {
        // Not a multiple of BUCKET_COUNT, so every stripe's own share only
        // divides evenly once rounded up.
        let hint = 3_000u64;
        let expected = usize::try_from(hint.div_ceil(BUCKET_COUNT as u64))
            .expect("a hint this small divided by BUCKET_COUNT fits in usize");
        let engine = Engine::<u32, String>::new(u64::MAX, None, None, Some(hint));
        assert_eq!(
            engine.stripe_capacities(),
            vec![expected; BUCKET_COUNT],
            "every stripe presizes to ceil(hint / BUCKET_COUNT), none left at zero or grown past it"
        );
    }

    #[test]
    fn engine_no_capacity_hint_allocates_nothing_until_first_write() {
        let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
        assert_eq!(
            engine.stripe_capacities(),
            vec![0; BUCKET_COUNT],
            "no hint: every stripe starts exactly as Stripe::new leaves it, zero allocation"
        );
        let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
        let total: usize = engine.stripe_capacities().into_iter().sum();
        assert!(
            total > 0,
            "the first write grows only the one stripe its key lands in, matching today's \
             unhinted behavior exactly"
        );
    }

    #[test]
    fn tti_past_max_inline_ttl_ms_still_expires_within_the_clamped_bound() {
        // With no clamp, a tti this far past MAX_INLINE_TTL_MS would leave
        // the entry readable forever. Two separate engines, since a read
        // would reset the idle clock the second assertion checks.
        let past_ceiling = Duration::from_millis(MAX_INLINE_TTL_MS + 10_000);

        let still_within = engine_u32_string(u64::MAX, Some(past_ceiling));
        let _ = put(
            &still_within,
            1,
            key_bytes(1),
            "a".into(),
            hlc(1, 1),
            None,
            0,
        );
        assert_eq!(
            still_within.get(&1, MAX_INLINE_TTL_MS - 1),
            Some("a".to_string()),
            "still within the clamped tti"
        );

        let past_clamp = engine_u32_string(u64::MAX, Some(past_ceiling));
        let _ = put(&past_clamp, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
        assert_eq!(
            past_clamp.get(&1, MAX_INLINE_TTL_MS),
            None,
            "idle past the clamped tti"
        );
    }

    #[test]
    fn decode_key_and_decode_value_round_trip_postcard_bytes() {
        let key_bytes = postcard::to_stdvec(&42u32).expect("u32 encodes");
        let value_bytes = postcard::to_stdvec(&"a value".to_string()).expect("string encodes");
        assert_eq!(decode_key::<u32>(&key_bytes), 42u32);
        assert_eq!(decode_value::<String>(&value_bytes), "a value".to_string());
    }

    /// Pins `Live`'s non-spill size: `record` (24) + `wall_ms` (8) +
    /// `node` (8) + four 4-byte fields (16) = 56, no padding.
    #[test]
    #[cfg(not(feature = "spill"))]
    fn live_size_is_56_bytes_without_spill() {
        assert_eq!(std::mem::size_of::<Live<String, String>>(), 56);
    }

    /// Pins `Live`'s spill-enabled size: `EntryState` adds a 1-byte tag
    /// plus padding, 76 bytes rounded up to 80 for 8-byte alignment.
    #[test]
    #[cfg(feature = "spill")]
    fn live_size_is_80_bytes_with_spill() {
        assert_eq!(std::mem::size_of::<Live<String, String>>(), 80);
    }

    #[test]
    fn live_ver_round_trips_through_set_ver() {
        let mut live = Live::<u32, String>::new(
            build_resident_record(key_bytes(1).as_ref(), b"v"),
            hlc(1, 1),
            NEVER_EXPIRES,
            1,
            0,
            #[cfg(feature = "spill")]
            EntryState::Resident,
        );
        assert_eq!(live.ver(), hlc(1, 1));
        let new_ver = Hlc {
            wall_ms: 9,
            logical: 3,
            node: NodeId::merge_derived(7),
        };
        live.set_ver(new_ver);
        assert_eq!(live.ver(), new_ver);
    }

    #[test]
    fn evict_outcome_made_no_progress_only_when_nothing_was_freed() {
        assert!(EvictOutcome::default().made_no_progress());
        assert!(
            !EvictOutcome {
                removed_weight: 1,
                deficit: 0,
            }
            .made_no_progress()
        );
    }

    #[test]
    fn a_pending_spill_entry_is_never_sampled_as_a_victim() {
        // A weight-0 Resident entry, exactly what a successful hand-off
        // leaves behind while the flusher's install is still in flight,
        // must never be picked a second time: not by the single-victim
        // sampler, and not by the batch one.
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(&engine, key, kb.clone(), "x".repeat(5), hlc(1, 1), None, 0);
        {
            let mut stripe = engine.stripe_lock(bucket).write();
            let live = stripe
                .live
                .find_mut(hash, |l| record_key(&l.record) == kb.as_ref())
                .expect("entry is present");
            live.weight = 0;
        }

        assert!(
            engine.evict_one_sampled(bucket, 0).made_no_progress(),
            "the only entry in this stripe is pending; single-victim sampling finds nothing"
        );
        assert!(
            engine
                .evict_batch_sampled(bucket, 100, 0)
                .made_no_progress(),
            "the only entry in this stripe is pending; batch sampling finds nothing either"
        );
        assert_eq!(
            engine.get(&key, 0),
            Some("x".repeat(5)),
            "the pending entry is untouched: still resident, still readable"
        );
    }

    #[test]
    fn sampled_eviction_still_picks_the_coldest_entry_across_a_last_access_rollover() {
        // key_old's touch predates a touch_stamp rollover; a naive
        // min_by_key over raw stamps would wrongly call key_new colder.
        let target_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(0).as_ref()));
        let key_old = 0u32;
        let mut candidate = 1u32;
        let key_new = loop {
            if stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()))
                == target_bucket
            {
                break candidate;
            }
            candidate += 1;
        };

        let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
        let before_rollover = (1u64 << 32) - 100;
        let after_rollover = (1u64 << 32) + 50;
        let _ = put(
            &engine,
            key_old,
            key_bytes(key_old),
            "old".to_string(),
            hlc(1, 1),
            None,
            before_rollover,
        );
        let _ = put(
            &engine,
            key_new,
            key_bytes(key_new),
            "new".to_string(),
            hlc(2, 1),
            None,
            after_rollover,
        );

        let now_ms = after_rollover + 10;
        let outcome = engine.evict_one_sampled(target_bucket, now_ms);
        assert_eq!(outcome.removed_weight, 1, "one entry evicted");
        assert_eq!(
            engine.get(&key_old, now_ms),
            None,
            "key_old, touched longest ago in true time, is the one evicted"
        );
        assert_eq!(
            engine.get(&key_new, now_ms),
            Some("new".to_string()),
            "key_new survives: its truncated stamp is smaller, but it is the true warmer entry"
        );
    }

    #[test]
    fn evict_one_sampled_breaks_a_last_access_tie_by_arena_order() {
        // key_first/key_second share a bucket and the same now_ms, so
        // idle_elapsed_ms ties; the winner comes down to arena order in
        // sample_candidates feeding max_by_key, which returns the last
        // equally-maximal element. Pins today's resolution as a regression
        // guard, not a documented contract.
        let (key_first, key_second, bucket) = same_bucket_pair(100_000);
        let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
        let now = 0u64;
        let _ = put(
            &engine,
            key_first,
            key_bytes(key_first),
            "first".to_string(),
            hlc(1, 1),
            None,
            now,
        );
        let _ = put(
            &engine,
            key_second,
            key_bytes(key_second),
            "second".to_string(),
            hlc(2, 1),
            None,
            now,
        );

        let outcome = engine.evict_one_sampled(bucket, now);
        assert_eq!(outcome.removed_weight, 1, "one entry evicted");
        assert_eq!(
            engine.get(&key_first, now),
            None,
            "today's concrete tie-break: the first-inserted key is evicted"
        );
        assert_eq!(
            engine.get(&key_second, now),
            Some("second".to_string()),
            "today's concrete tie-break: the second-inserted key survives"
        );
    }

    #[cfg(feature = "spill")]
    mod spill_payload {
        use super::*;
        use crate::store::spill::SpillSink;
        #[cfg(not(feature = "sim"))]
        use crate::store::spill::{SpillConfig, SpillTier};

        fn loc(region: u32, offset: u32, len: u32, generation: u32) -> SpillLoc {
            SpillLoc {
                region,
                offset,
                len,
                generation,
            }
        }

        #[test]
        fn release_buckets_decrements_the_spilled_entries_gauge_for_a_departing_spilled_entry() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
            assert_eq!(engine.debug_spill_entries_count(), 1);

            let removed =
                engine.release_buckets(&[u16::try_from(bucket).expect("invariant: fits")]);

            assert_eq!(
                removed, 1,
                "the spilled entry counts toward the removed total same as a resident one"
            );
            assert_eq!(
                engine.debug_spill_entries_count(),
                0,
                "release_buckets corrects the spilled-entries gauge for a departing spilled entry"
            );
            assert_eq!(engine.live_entry_count(), 0);
        }

        #[test]
        fn snapshot_spilled_reports_pointers_for_spilled_entries_only() {
            let engine = engine_u32_string(u64::MAX, None);
            let key1 = 1u32;
            let kb1 = key_bytes(key1);
            let l = loc(0, 0, 4, 0);
            engine.debug_insert_spilled(&kb1, hlc(1, 1), Some(500), l, 0);
            let key2 = 2u32;
            let kb2 = key_bytes(key2);
            let _ = put(
                &engine,
                key2,
                kb2.clone(),
                "resident".to_string(),
                hlc(1, 1),
                None,
                0,
            );

            let spilled = engine.snapshot_spilled(0);
            assert_eq!(spilled.len(), 1, "only the spilled key is reported here");
            let (k, ver, expires_at_ms, reported_loc) = &spilled[0];
            assert_eq!(k.as_ref(), kb1.as_ref());
            assert_eq!(*ver, hlc(1, 1));
            assert_eq!(*expires_at_ms, Some(500));
            assert_eq!(*reported_loc, l);

            let resident = engine.snapshot_records(0);
            assert_eq!(
                resident.len(),
                1,
                "snapshot_records reports only the resident key"
            );
            assert_eq!(resident[0].key.as_ref(), kb2.as_ref());
        }

        #[test]
        fn records_for_or_spilled_splits_a_resident_and_a_spilled_key() {
            let engine = engine_u32_string(u64::MAX, None);
            let spilled_key = 1u32;
            let kb_spilled = key_bytes(spilled_key);
            let l = loc(0, 0, 4, 0);
            let spilled_ver = hlc(1, 1);
            engine.debug_insert_spilled(&kb_spilled, spilled_ver, Some(500), l, 0);

            let resident_key = 2u32;
            let kb_resident = key_bytes(resident_key);
            let _ = put(
                &engine,
                resident_key,
                kb_resident.clone(),
                "resident".to_string(),
                hlc(2, 1),
                None,
                0,
            );

            let (records, spilled) =
                engine.records_for_or_spilled(&[kb_resident.clone(), kb_spilled.clone()], 0);

            assert_eq!(
                records.len(),
                1,
                "only the resident key comes back as a WireRecord"
            );
            assert_eq!(records[0].key.as_ref(), kb_resident.as_ref());
            assert_eq!(
                records[0].value.as_deref(),
                Some(
                    postcard::to_stdvec(&"resident".to_string())
                        .expect("test value encodes")
                        .as_slice()
                )
            );
            assert_eq!(records[0].ver, hlc(2, 1));

            assert_eq!(
                spilled.len(),
                1,
                "only the spilled key comes back as a pointer"
            );
            let (k, ver, expires_at_ms, reported_loc) = &spilled[0];
            assert_eq!(k.as_ref(), kb_spilled.as_ref());
            assert_eq!(*ver, spilled_ver);
            assert_eq!(*expires_at_ms, Some(500));
            assert_eq!(*reported_loc, l);
        }

        #[test]
        fn get_by_bytes_and_record_for_return_none_for_a_spilled_entry() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);

            assert_eq!(
                engine.get(&key, 0),
                None,
                "a spilled entry never answers get with a value"
            );
            assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), None);
            assert!(
                engine.contains_key(&key, 0),
                "existence doesn't need the value bytes"
            );
            assert!(
                engine.record_for(kb.as_ref(), 0).is_none(),
                "record_for skips a spilled entry; fan-out simply drops it, repaired later by AE"
            );
        }

        #[test]
        fn miss_or_join_never_returns_hit_for_a_spilled_entry() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);

            match engine.miss_or_join(&kb, hash, 0) {
                JoinOutcome::Hit(_) => panic!("a spilled entry has no resident value to hit on"),
                JoinOutcome::Owner(_) | JoinOutcome::Join(..) => {}
            }
        }

        #[test]
        fn spilled_loc_snapshots_the_pointer_and_touches_last_access() {
            let engine = Engine::<u32, String>::new(10, None, None, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let ver = hlc(1, 1);
            let l = loc(2, 8, 4, 1);
            engine.debug_insert_spilled(&kb, ver, None, l, 0);

            assert_eq!(engine.spilled_loc(kb.as_ref(), hash, 100), Some((ver, l)));

            let bucket = stripe_index_from_hash(hash);
            let stripe = engine.stripe_lock(bucket).read();
            let live = stripe
                .live
                .iter()
                .find(|live| record_key(&live.record) == kb.as_ref())
                .expect("the entry is present");
            assert_eq!(
                live.last_access_ms.load(Ordering::Relaxed),
                100,
                "spilled_loc touches last_access for a capacity-tracking engine"
            );
            drop(stripe);

            let missing = key_bytes(999);
            assert_eq!(
                engine.spilled_loc(missing.as_ref(), hash_key_bytes(missing.as_ref()), 100),
                None,
                "an absent key yields None"
            );

            let key2 = 2u32;
            let kb2 = key_bytes(key2);
            let _ = put(
                &engine,
                key2,
                kb2.clone(),
                "resident".to_string(),
                hlc(1, 1),
                None,
                0,
            );
            assert_eq!(
                engine.spilled_loc(kb2.as_ref(), hash_key_bytes(kb2.as_ref()), 100),
                None,
                "a resident entry yields None too"
            );
        }

        #[test]
        fn promote_locked_restores_residency_without_touching_the_digest_or_live_count() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let ver = hlc(5, 1);
            engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 10, 0), 0);

            let digest_before = engine.digests();
            let (live_count_before, weight_before) = engine.debug_totals();
            assert_eq!(weight_before, 0, "a spilled entry contributes zero weight");

            let promoted = engine.promote_locked(
                kb.as_ref(),
                hash,
                ver,
                &"restored".to_string(),
                &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
            );
            assert!(
                promoted,
                "the version matches and nothing displaced it: promotion succeeds"
            );

            assert_eq!(engine.get(&key, 0), Some("restored".to_string()));
            assert_eq!(
                engine.digests(),
                digest_before,
                "promotion never touches the digest"
            );
            let (live_count_after, weight_after) = engine.debug_totals();
            assert_eq!(
                live_count_after, live_count_before,
                "promotion never touches live_count"
            );
            assert_eq!(
                weight_after, 1,
                "promotion adds the freshly weighed entry's weight back to total_weight"
            );
        }

        #[test]
        fn promote_locked_is_a_noop_once_already_resident() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let ver = hlc(5, 1);
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "already-here".to_string(),
                ver,
                None,
                0,
            );

            let promoted = engine.promote_locked(
                kb.as_ref(),
                hash,
                ver,
                &"stale-read".to_string(),
                &Bytes::from_static(b"stale"),
            );
            assert!(
                !promoted,
                "nothing to promote: the entry is already resident"
            );
            assert_eq!(engine.get(&key, 0), Some("already-here".to_string()));
        }

        #[test]
        fn promote_locked_invokes_the_weigher_with_the_original_key_and_value() {
            let calls: Arc<std::sync::Mutex<Vec<(u32, String)>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            let recorded = Arc::clone(&calls);
            let weigher: Weigher<u32, String> = Box::new(move |k, v| {
                recorded.lock().expect("lock").push((*k, v.clone()));
                1
            });
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let ver = hlc(5, 1);
            engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 10, 0), 0);

            let promoted = engine.promote_locked(
                kb.as_ref(),
                hash,
                ver,
                &"restored".to_string(),
                &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
            );
            assert!(promoted);

            let calls = calls.lock().expect("lock");
            assert_eq!(
                calls.as_slice(),
                &[(key, "restored".to_string())],
                "the weigher sees the original key, decoded from key_bytes, and the original \
                 value passed in, not a stale or spilled-pointer substitute"
            );
        }

        #[test]
        fn flusher_install_is_a_noop_when_a_newer_write_lands_first() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let old_ver = hlc(1, 1);
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "old".to_string(),
                old_ver,
                None,
                0,
            );
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "new".to_string(),
                hlc(2, 1),
                None,
                0,
            );

            let installed =
                SpillSink::install(&engine, bucket, &kb, hash, old_ver, loc(0, 0, 4, 0), 0);
            assert!(
                !installed,
                "a stale flush is discarded once a newer write has landed"
            );
            assert_eq!(
                engine.get(&key, 0),
                Some("new".to_string()),
                "the newer write is untouched"
            );
        }

        #[test]
        fn flusher_install_is_a_noop_when_a_tombstone_lands_first() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(1, 1);
            let _ = put(&engine, key, kb.clone(), "old".to_string(), ver, None, 0);
            tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);

            let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 0);
            assert!(
                !installed,
                "a stale flush is discarded once a tombstone has landed"
            );
            assert_eq!(
                engine.get(&key, 0),
                None,
                "the key stays deleted; a late flush never resurrects it"
            );
        }

        #[test]
        fn region_reclaim_purges_a_key_still_pointing_at_the_reclaimed_generation() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(3, 0, 10, 0), 0);

            let removed = SpillSink::reclaim(&engine, 3, 0, &[(bucket, kb.clone())]);
            assert_eq!(
                removed, 1,
                "the key's pointer still names this exact region and generation"
            );
            assert_eq!(engine.get(&key, 0), None);
            let (live_count, weight) = engine.debug_totals();
            assert_eq!((live_count, weight), (0, 0));
        }

        #[test]
        fn region_reclaim_of_a_long_ttl_key_clears_its_side_table_row() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let long_deadline = MAX_INLINE_TTL_MS + 5_000;
            engine.debug_insert_spilled(&kb, hlc(1, 1), Some(long_deadline), loc(3, 0, 10, 0), 0);
            assert!(
                engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref()),
                "a long TTL spilled entry leaves its absolute deadline in the side table"
            );

            let removed = SpillSink::reclaim(&engine, 3, 0, &[(bucket, kb.clone())]);
            assert_eq!(
                removed, 1,
                "the key's pointer still names this exact region and generation"
            );
            assert!(
                !engine
                    .stripe_lock(bucket)
                    .read()
                    .long_ttl
                    .contains_key(kb.as_ref()),
                "reclaiming the entry clears its side-table row; no row outlives its entry"
            );
        }

        #[test]
        fn region_reclaim_skips_a_key_that_was_promoted_since_being_recorded() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(1, 1);
            engine.debug_insert_spilled(&kb, ver, None, loc(3, 0, 10, 0), 0);
            assert!(engine.promote_locked(
                kb.as_ref(),
                hash,
                ver,
                &"restored".to_string(),
                &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
            ));

            let digest_before = engine.digests();
            let removed = SpillSink::reclaim(&engine, 3, 0, &[(bucket, kb.clone())]);
            assert_eq!(
                removed, 0,
                "a key promoted back to resident survives its old region's reclaim"
            );
            assert_eq!(engine.get(&key, 0), Some("restored".to_string()));
            assert_eq!(engine.digests(), digest_before);
        }

        #[test]
        fn region_reclaim_skips_a_key_that_was_overwritten_since_being_recorded() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(3, 0, 10, 0), 0);
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "fresh".to_string(),
                hlc(2, 1),
                None,
                0,
            );

            let removed = SpillSink::reclaim(&engine, 3, 0, &[(bucket, kb.clone())]);
            assert_eq!(
                removed, 0,
                "an overwritten key's stale reverse-index row is left alone"
            );
            assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
        }

        /// Puts `live.weight` at `hash`/`kb` back to `0`, moves `weight` out
        /// of `total_weight` and into `pending_spill_weight`, exactly the
        /// state a successful hand-off to a spill tier leaves behind while
        /// the flusher's write is still in flight. Lets a test drive
        /// `SpillSink::abandon`/`SpillSink::install` directly, the same way
        /// this module already drives `reclaim` directly, with no real disk
        /// or flusher thread needed.
        fn simulate_pending_handoff<K, V>(
            engine: &Engine<K, V>,
            bucket: usize,
            hash: u64,
            kb: &Bytes,
            weight: u32,
        ) where
            K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
            V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        {
            {
                let mut stripe = engine.stripe_lock(bucket).write();
                let live = stripe
                    .live
                    .find_mut(hash, |l| record_key(&l.record) == kb.as_ref())
                    .expect("entry is present");
                live.weight = 0;
            }
            engine
                .total_weight
                .fetch_sub(u64::from(weight), Ordering::Relaxed);
            engine
                .pending_spill_weight
                .fetch_add(u64::from(weight), Ordering::Relaxed);
        }

        #[test]
        fn abandon_restores_the_weight_of_a_still_pending_entry() {
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(1, 1);
            let _ = put(&engine, key, kb.clone(), "x".repeat(7), ver, None, 0);
            let (_, weight_before) = engine.debug_totals();
            assert_eq!(weight_before, 7);

            simulate_pending_handoff(&engine, bucket, hash, &kb, 7);
            let (_, weight_pending) = engine.debug_totals();
            assert_eq!(weight_pending, 0);
            assert_eq!(
                engine.debug_pending_spill_weight(),
                7,
                "the hand-off's weight moved into pending_spill_weight"
            );

            SpillSink::abandon(&engine, bucket, &kb, hash, ver, 7);

            let (_, weight_after) = engine.debug_totals();
            assert_eq!(
                weight_after, 7,
                "abandon recomputes the weight through the weigher and adds it back to \
                 total_weight"
            );
            assert_eq!(
                engine.debug_pending_spill_weight(),
                0,
                "abandon moves the weight back out of pending_spill_weight too"
            );
            let stripe = engine.stripe_lock(bucket).read();
            let live = stripe
                .live
                .iter()
                .find(|l| record_key(&l.record) == kb.as_ref())
                .expect("entry is present");
            assert_eq!(
                live.weight, 7,
                "the entry's own weight field is restored too, not just the total"
            );
        }

        #[test]
        fn abandon_is_a_noop_once_the_keys_stored_state_has_changed() {
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let old_ver = hlc(1, 1);
            let _ = put(&engine, key, kb.clone(), "x".repeat(7), old_ver, None, 0);
            simulate_pending_handoff(&engine, bucket, hash, &kb, 7);

            // A fresh write lands before the abandoned job is ever noticed:
            // this is exactly what `apply_put`'s own weight bookkeeping
            // already resolved correctly, so abandon must leave it alone.
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "fresh".to_string(),
                hlc(2, 1),
                None,
                0,
            );
            let (_, weight_before_abandon) = engine.debug_totals();
            assert_eq!(weight_before_abandon, 5, "\"fresh\".len() == 5");

            SpillSink::abandon(&engine, bucket, &kb, hash, old_ver, 7);

            let (_, weight_after) = engine.debug_totals();
            assert_eq!(
                weight_after, weight_before_abandon,
                "a key whose stored state changed since hand-off is left alone"
            );
            assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
            assert_eq!(
                engine.debug_pending_spill_weight(),
                0,
                "the job's weight leaves pending_spill_weight even though the key moved on"
            );
        }

        #[test]
        fn abandon_on_a_key_that_is_no_longer_present_is_a_noop() {
            let engine = engine_u32_string(u64::MAX, None);
            let kb = key_bytes(999);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let (_, weight_before) = engine.debug_totals();

            SpillSink::abandon(&engine, bucket, &kb, hash, hlc(1, 1), 0);

            let (_, weight_after) = engine.debug_totals();
            assert_eq!(weight_after, weight_before);
        }

        #[test]
        fn install_releases_its_share_of_pending_spill_weight() {
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(1, 1);
            let _ = put(&engine, key, kb.clone(), "x".repeat(9), ver, None, 0);
            simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
            assert_eq!(engine.debug_pending_spill_weight(), 9);

            let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 9);
            assert!(installed);

            assert_eq!(
                engine.debug_pending_spill_weight(),
                0,
                "install moves the hand-off's weight out of pending_spill_weight, not just \
                 the entry's own payload"
            );
            let (_, weight_after) = engine.debug_totals();
            assert_eq!(
                weight_after, 0,
                "install never adds to total_weight; the entry stays at weight 0, Spilled"
            );
        }

        #[test]
        fn install_copies_the_key_into_a_fresh_allocation_instead_of_slicing() {
            // A key over INLINE_CAP forces the heap path, where a
            // pointer-identity check is meaningful.
            let engine = Engine::<String, String>::new(u64::MAX, None, None, None);
            let key = "k".repeat(40);
            let kb = string_key_bytes(&key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(1, 1);
            let _ = put(&engine, key, kb.clone(), "value".to_string(), ver, None, 0);
            simulate_pending_handoff(&engine, bucket, hash, &kb, 1);

            let record_ptr_before = {
                let stripe = engine.stripe_lock(bucket).read();
                let live = stripe
                    .live
                    .iter()
                    .find(|l| record_key(&l.record) == kb.as_ref())
                    .expect("entry is present");
                live.record.as_ref().as_ptr()
            };

            let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 1);
            assert!(installed);

            let record_ptr_after = {
                let stripe = engine.stripe_lock(bucket).read();
                let live = stripe
                    .live
                    .iter()
                    .find(|l| record_key(&l.record) == kb.as_ref())
                    .expect("entry is present");
                live.record.as_ref().as_ptr()
            };

            assert_ne!(
                record_ptr_before, record_ptr_after,
                "install builds a fresh key-only allocation rather than slicing the old \
                 resident record: a slice would share its refcount and keep the value bytes \
                 physically resident in RAM"
            );
        }

        #[test]
        fn install_of_an_inline_record_reuses_the_slot_with_no_heap_allocation() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(1, 1);
            let _ = put(&engine, key, kb.clone(), "value".to_string(), ver, None, 0);
            simulate_pending_handoff(&engine, bucket, hash, &kb, 1);

            let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 1);
            assert!(installed);

            let stripe = engine.stripe_lock(bucket).read();
            let live = stripe
                .live
                .iter()
                .find(|l| record_key(&l.record) == kb.as_ref())
                .expect("entry is present");
            assert!(
                matches!(live.record, Record::Inline { .. }),
                "a key-only record for a small u32 key stays inline: no heap allocation"
            );
            assert_eq!(record_key(&live.record), kb.as_ref());
            assert!(
                record_value(&live.record).is_empty(),
                "no old value bytes are retained anywhere"
            );
        }

        #[test]
        fn install_new_never_resurrects_a_tombstoned_key() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            tombstone(&engine, key, kb.clone(), hlc(1, 1), 0);

            let inserted = SpillSink::install_new(
                &engine,
                bucket,
                &kb,
                hash,
                hlc(0, 0),
                None,
                loc(0, 0, 4, 0),
                1,
            );

            assert!(
                !inserted,
                "install_new never resurrects a key a tombstone already covers"
            );
            assert_eq!(engine.get(&key, 0), None);
            let (live_count, _) = engine.debug_totals();
            assert_eq!(live_count, 0);
        }

        #[test]
        fn install_new_never_overwrites_an_already_present_live_entry() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(2, 1);
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "resident".to_string(),
                ver,
                None,
                0,
            );
            let digest_before = engine.digests();

            let inserted = SpillSink::install_new(
                &engine,
                bucket,
                &kb,
                hash,
                hlc(1, 1),
                None,
                loc(0, 0, 4, 0),
                1,
            );

            assert!(
                !inserted,
                "install_new never overwrites a key that is already present, even at an \
                 older version"
            );
            assert_eq!(engine.get(&key, 0), Some("resident".to_string()));
            assert_eq!(
                engine.digests(),
                digest_before,
                "a refused install_new never touches the digest"
            );
        }

        #[test]
        fn install_releases_pending_weight_even_when_a_newer_write_displaced_the_key() {
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let old_ver = hlc(1, 1);
            let _ = put(&engine, key, kb.clone(), "x".repeat(9), old_ver, None, 0);
            simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
            let _ = put(
                &engine,
                key,
                kb.clone(),
                "fresh".to_string(),
                hlc(2, 1),
                None,
                0,
            );

            let installed =
                SpillSink::install(&engine, bucket, &kb, hash, old_ver, loc(0, 0, 4, 0), 9);

            assert!(!installed, "the newer write wins; nothing flips to Spilled");
            assert_eq!(
                engine.debug_pending_spill_weight(),
                0,
                "the job's weight leaves pending_spill_weight even though the key moved on"
            );
            let (_, weight_after) = engine.debug_totals();
            assert_eq!(
                weight_after, 5,
                "total_weight holds only the fresh value's weight"
            );
        }

        #[test]
        fn abandon_releases_pending_weight_even_when_a_tombstone_displaced_the_key() {
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let old_ver = hlc(1, 1);
            let _ = put(&engine, key, kb.clone(), "x".repeat(9), old_ver, None, 0);
            simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
            tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);

            SpillSink::abandon(&engine, bucket, &kb, hash, old_ver, 9);

            assert_eq!(
                engine.debug_pending_spill_weight(),
                0,
                "the job's weight leaves pending_spill_weight although the key is tombstoned"
            );
            let (_, weight_after) = engine.debug_totals();
            assert_eq!(weight_after, 0, "nothing is restored onto a tombstoned key");
        }

        #[test]
        fn enforce_capacity_counts_pending_spill_weight_toward_the_cap_and_defers_to_the_flusher() {
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(50, None, Some(weigher), None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let _ = put(&engine, key, kb.clone(), "x".repeat(80), hlc(1, 1), None, 0);
            simulate_pending_handoff(&engine, bucket, hash, &kb, 80);
            // total_weight alone (0) already fits the 50-unit cap; only
            // adding pending_spill_weight (80) back in shows this bucket is
            // still, in effect, 30 units over.
            assert_eq!(engine.debug_totals().1, 0);
            assert_eq!(engine.debug_pending_spill_weight(), 80);

            engine.enforce_capacity(bucket, 0);

            assert_eq!(
                engine.debug_eviction_lock_acquisitions(),
                1,
                "one batch pass finds nothing else to evict in this bucket; the stop rule \
                 returns immediately rather than paying for a full-stripe scan"
            );
            assert_eq!(
                engine.get(&key, 0),
                Some("x".repeat(80)),
                "the pending entry itself is left alone: still resident, still readable"
            );
            assert_eq!(
                engine.debug_pending_spill_weight(),
                80,
                "still nobody resolved it"
            );
        }

        #[test]
        fn insert_over_a_spilled_key_replaces_it_with_a_fresh_resident_entry() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
            let (live_count_before, weight_before) = engine.debug_totals();
            assert_eq!(weight_before, 0);
            assert_eq!(
                engine.debug_spill_entries_count(),
                1,
                "debug_insert_spilled counts against sundog_spill_entries like a real install"
            );

            let outcome = put(
                &engine,
                key,
                kb.clone(),
                "fresh".to_string(),
                hlc(2, 1),
                None,
                0,
            );
            assert!(
                matches!(outcome, ApplyOutcome::Put { created: false, .. }),
                "a spilled key is still live: a write over it is an update, not a creation"
            );
            assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
            let (live_count_after, weight_after) = engine.debug_totals();
            assert_eq!(
                live_count_after, live_count_before,
                "one spilled entry is replaced by one resident one: no net live_count change"
            );
            assert_eq!(weight_after, 1);
            assert_eq!(engine.digests(), engine.recompute_digests_paired());
            assert_eq!(
                engine.debug_spill_entries_count(),
                0,
                "an overwrite of a spilled key must decrement sundog_spill_entries, the same as \
                 a promotion would"
            );
        }

        #[test]
        fn tombstone_over_a_spilled_key_removes_it_and_corrects_weight_and_live_count() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
            assert_eq!(engine.debug_spill_entries_count(), 1);

            {
                let mut stripe = engine.stripe_lock(bucket).write();
                let resolver = LwwResolver;
                let apply_ctx = test_ctx(&engine, bucket, hash, &resolver, 60_000, 600_000, 0);
                let (outcome, displaced_spilled) = apply_locked(
                    &mut stripe,
                    &apply_ctx,
                    EntryKey {
                        hash,
                        key,
                        key_bytes: kb.clone(),
                    },
                    hlc(2, 1),
                    Incoming::Tombstone,
                );
                assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
                assert!(
                    displaced_spilled,
                    "apply_locked reports that the tombstoned entry was spilled"
                );
                engine.note_spill_departure(displaced_spilled);
            }
            assert_eq!(engine.get(&key, 0), None);
            let (live_count, weight) = engine.debug_totals();
            assert_eq!((live_count, weight), (0, 0));
            assert_eq!(engine.digests(), engine.recompute_digests_paired());
            assert_eq!(
                engine.debug_spill_entries_count(),
                0,
                "a tombstone over a spilled key must decrement sundog_spill_entries"
            );
        }

        #[test]
        fn invalidate_removes_a_spilled_key_at_a_newer_version() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let ver = hlc(1, 1);
            engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 4, 0), 0);
            assert_eq!(engine.debug_spill_entries_count(), 1);

            let removed_ver = engine.invalidate(kb.as_ref(), hash, hlc(2, 1));
            assert_eq!(removed_ver, Some(ver));
            assert_eq!(engine.get(&key, 0), None);
            let (live_count, weight) = engine.debug_totals();
            assert_eq!((live_count, weight), (0, 0));
            assert_eq!(
                engine.debug_spill_entries_count(),
                0,
                "invalidate of a spilled key must decrement sundog_spill_entries"
            );
        }

        #[test]
        fn invalidate_local_removes_a_spilled_key_unconditionally() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
            assert_eq!(engine.debug_spill_entries_count(), 1);

            engine.invalidate_local(kb.as_ref(), hash);
            assert_eq!(engine.get(&key, 0), None);
            let (live_count, weight) = engine.debug_totals();
            assert_eq!((live_count, weight), (0, 0));
            assert_eq!(
                engine.debug_spill_entries_count(),
                0,
                "invalidate_local of a spilled key must decrement sundog_spill_entries"
            );
        }

        #[test]
        fn sweep_removes_an_expired_spilled_key_and_corrects_the_digest() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            engine.debug_insert_spilled(&kb, hlc(1, 1), Some(50), loc(0, 0, 4, 0), 0);
            assert_eq!(engine.debug_spill_entries_count(), 1);

            engine.sweep(100);
            assert_eq!(engine.get(&key, 100), None);
            let (live_count, weight) = engine.debug_totals();
            assert_eq!((live_count, weight), (0, 0));
            assert_eq!(engine.digests(), engine.recompute_digests_paired());
            assert_eq!(
                engine.debug_spill_entries_count(),
                0,
                "an expiry sweep of a spilled key must decrement sundog_spill_entries"
            );
        }

        #[test]
        fn complete_fresh_load_over_a_spilled_key_decrements_spill_entries() {
            // A rare race: `get_spilled_by_bytes` already failed to promote
            // this key, since a concurrent tombstone or newer write raced
            // its read, yet the entry, sampled independently right here,
            // is still `Spilled` when the loader's fill lands.
            // `complete_fresh_load` replaces it, being newer, and must
            // still keep `sundog_spill_entries` correct.
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
            assert_eq!(engine.debug_spill_entries_count(), 1);

            let inflight = Arc::new(Inflight::<String>::new());
            let encoded = Bytes::from(postcard::to_stdvec(&"loaded".to_string()).unwrap());
            let outcome = engine.complete_fresh_load(
                FreshLoad {
                    key: &key,
                    key_bytes: &kb,
                    hash,
                    ver: hlc(2, 1),
                    value: "loaded".to_string(),
                    encoded,
                    expires_at_ms: None,
                },
                0,
                &inflight,
            );
            assert_eq!(
                outcome,
                FillOutcome::Installed { had_live: true },
                "the spilled entry counted as already-live for complete_fresh_load's purposes"
            );
            assert_eq!(engine.get(&key, 0), Some("loaded".to_string()));
            assert_eq!(
                engine.debug_spill_entries_count(),
                0,
                "complete_fresh_load displacing a spilled key must decrement \
                 sundog_spill_entries"
            );
        }

        #[test]
        fn gc_tombstones_never_touches_a_spilled_live_entry() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
            let digest_before = engine.digests();

            engine.gc_tombstones(true, u64::MAX);

            assert_eq!(
                engine.digests(),
                digest_before,
                "gc_tombstones only ever touches stripe.tombstones"
            );
            let (live_count, _) = engine.debug_totals();
            assert_eq!(live_count, 1, "the spilled live entry is untouched");
        }

        #[test]
        fn is_spill_candidate_excludes_a_weight_zeroed_entry_across_a_longer_pending_window() {
            // A `Reservation` wait suspends longer than a non-blocking pass
            // leaves open.
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let _ = put(&engine, key, kb.clone(), "x".repeat(9), hlc(1, 1), None, 0);
            simulate_pending_handoff(&engine, bucket, hash, &kb, 9);

            for _ in 0..1_000 {
                assert!(
                    engine.evict_one_sampled(bucket, 0).made_no_progress(),
                    "a weight-zeroed pending entry is never re-selected, however long it \
                     stays pending"
                );
            }
            assert_eq!(
                engine.get(&key, 0),
                Some("x".repeat(9)),
                "still resident and readable throughout"
            );
        }

        // --- Real disk: the flusher's actual eviction+install lifecycle.
        // Never combined with `sim`: its virtual clock gives no determinism
        // over real filesystem I/O or the flusher's OS thread.
        #[cfg(not(feature = "sim"))]
        mod io {
            use std::time::{Duration, Instant};

            use super::*;

            fn temp_dir(label: &str) -> std::path::PathBuf {
                let dir = std::env::temp_dir().join(format!(
                    "sundog-engine-spill-test-{label}-{}-{:?}",
                    std::process::id(),
                    std::thread::current().id(),
                ));
                let _ = std::fs::remove_dir_all(&dir);
                dir
            }

            /// Polls `cond` until it returns `true` or `timeout` elapses,
            /// returning the final result either way. Never a fixed sleep.
            fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
                let start = Instant::now();
                loop {
                    if cond() {
                        return true;
                    }
                    if start.elapsed() >= timeout {
                        return cond();
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }

            const POLL_TIMEOUT: Duration = Duration::from_secs(5);

            fn is_spilled<V>(engine: &Engine<u32, V>, kb: &Bytes) -> bool
            where
                V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
            {
                let hash = hash_key_bytes(kb.as_ref());
                let bucket = stripe_index_from_hash(hash);
                let stripe = engine.stripe_lock(bucket).read();
                stripe.live.iter().any(|l| {
                    record_key(&l.record) == kb.as_ref()
                        && matches!(l.state, EntryState::Spilled(_))
                })
            }

            #[test]
            fn set_spill_makes_it_visible_via_spill_accessor() {
                let dir = temp_dir("set-spill-accessor");
                let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
                let tier = Arc::new(SpillTier::open(&cfg, "accessor").expect("tier opens"));
                let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
                assert!(engine.spill().is_none());
                engine.set_spill(Arc::clone(&tier));
                assert!(engine.spill().is_some());
                let _ = std::fs::remove_dir_all(&dir);
            }

            #[test]
            fn eviction_hands_off_weight_immediately_and_install_never_double_subtracts_it() {
                let dir = temp_dir("evict");
                let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
                let tier = Arc::new(SpillTier::open(&cfg, "evict").expect("tier opens"));
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                let key = 1u32;
                let kb = key_bytes(key);
                let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
                let _ = put(&engine, key, kb.clone(), "x".repeat(20), hlc(1, 1), None, 0);

                let digest_before = engine.digests();
                let (live_count_before, weight_before) = engine.debug_totals();

                // `evict_one_sampled` is the real caller: it folds
                // `evict_victim_locked`'s freed weight into `total_weight`
                // right there, synchronously, with no dependency on the
                // flusher thread ever running.
                let pass_outcome = engine.evict_one_sampled(bucket, 0);
                assert_eq!(
                    pass_outcome.removed_weight, 20,
                    "a spill hand-off's freed weight counts the same as a physical removal's"
                );
                let (_, weight_at_handoff) = engine.debug_totals();
                assert_eq!(
                    weight_at_handoff,
                    weight_before - 20,
                    "hand-off zeroes the victim's weight and frees it from total_weight \
                     immediately, before any disk write or install has happened"
                );
                // The flusher may already have installed the victim by now;
                // either way its weight is 0 from the hand-off on.
                assert_eq!(
                    {
                        let stripe = engine.stripe_lock(bucket).read();
                        stripe
                            .live
                            .iter()
                            .find(|l| record_key(&l.record) == kb.as_ref())
                            .map(|l| l.weight)
                    },
                    Some(0),
                    "the victim keeps weight 0 from the hand-off, resident or spilled"
                );

                // A second sampling pass, called before the flusher's
                // install is known to have landed, must not pick the same
                // key again: whether it is still the pending hand-off or
                // has already been flipped to `Spilled` by now, either way
                // `is_spill_candidate` excludes it.
                assert!(
                    engine.evict_one_sampled(bucket, 0).made_no_progress(),
                    "the only entry in this stripe is either still pending or already \
                     spilled; double-sampling before install must find nothing"
                );

                assert!(
                    poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
                    "the flusher installs the spilled entry"
                );

                assert_eq!(
                    engine.digests(),
                    digest_before,
                    "spilling and installing never touch the digest"
                );
                let (live_count_after, weight_after) = engine.debug_totals();
                assert_eq!(
                    live_count_after, live_count_before,
                    "spilling and installing never touch live_count"
                );
                assert_eq!(
                    weight_after, weight_at_handoff,
                    "install only flips the payload to Spilled; the weight was already zeroed \
                     and freed at hand-off, so total_weight does not move again here"
                );

                let _ = std::fs::remove_dir_all(&dir);
            }

            #[test]
            fn install_new_creates_a_fresh_spilled_entry_readable_via_read_at_and_folds_the_digest()
            {
                let dir = temp_dir("install-new");
                let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
                let tier = Arc::new(SpillTier::open(&cfg, "install-new").expect("tier opens"));

                // A throwaway producer spills one record to disk to get a
                // genuine SpillLoc.
                let producer: Arc<Engine<u32, String>> =
                    Arc::new(Engine::new(u64::MAX, None, None, None));
                producer.set_spill(Arc::clone(&tier));
                tier.attach(Arc::downgrade(
                    &(Arc::clone(&producer) as Arc<dyn SpillSink>),
                ));
                let key = 7u32;
                let kb = key_bytes(key);
                let hash = hash_key_bytes(kb.as_ref());
                let bucket = stripe_index_from_hash(hash);
                let ver = hlc(1, 1);
                let _ = put(
                    &producer,
                    key,
                    kb.clone(),
                    "spilled-value".to_string(),
                    ver,
                    Some(999),
                    0,
                );
                let _ = producer.evict_one_sampled(bucket, 0);
                assert!(poll_until(POLL_TIMEOUT, || is_spilled(&producer, &kb)));
                let loc = {
                    let stripe = producer.stripe_lock(bucket).read();
                    let live = stripe
                        .live
                        .iter()
                        .find(|l| record_key(&l.record) == kb.as_ref())
                        .expect("entry is present");
                    match live.state {
                        EntryState::Spilled(loc) => loc,
                        EntryState::Resident => {
                            panic!("poll_until above confirms the producer's entry is spilled")
                        }
                    }
                };

                let target = Engine::<u32, String>::new(u64::MAX, None, None, None);
                let digest_before = target.digests();

                let inserted =
                    SpillSink::install_new(&target, bucket, &kb, hash, ver, Some(999), loc, 1);
                assert!(inserted);

                // Resident-free: `get` reads nothing, since no value ever
                // landed in RAM on the target engine.
                assert_eq!(target.get(&key, 0), None);
                {
                    let stripe = target.stripe_lock(bucket).read();
                    let live = stripe
                        .live
                        .iter()
                        .find(|l| record_key(&l.record) == kb.as_ref())
                        .expect("entry is present");
                    assert!(
                        matches!(live.state, EntryState::Spilled(l) if l == loc),
                        "Spilled(loc)-only, pointing at the exact same on-disk location"
                    );
                    assert_eq!(live.weight, 0, "a Spilled entry always carries weight 0");
                }

                // Readable via read_at, since install_new never re-writes
                // or copies the value bytes itself.
                let read_back = tier
                    .read_at(loc)
                    .expect("read_at succeeds")
                    .expect("the record is still there");
                assert_eq!(read_back.ver, ver);
                assert_eq!(read_back.expires_at_ms, Some(999));
                assert_eq!(decode_value::<String>(&read_back.encoded), "spilled-value");

                let (live_count, weight) = target.debug_totals();
                assert_eq!(live_count, 1);
                assert_eq!(
                    weight, 0,
                    "a spilled entry never counts toward total_weight"
                );

                let mut expected_digest = digest_before;
                expected_digest[bucket].1 ^= entry_fingerprint(kb.as_ref(), ver);
                assert_eq!(
                    target.digests(),
                    expected_digest,
                    "install_new folds the fresh entry's fingerprint into the bucket digest"
                );
                assert_eq!(
                    target.digests(),
                    target.recompute_digests_paired(),
                    "the incrementally folded digest matches a full recompute from scratch"
                );

                tier.close();
                let _ = std::fs::remove_dir_all(&dir);
            }

            #[test]
            fn evict_one_sampled_abandons_the_victim_when_the_flush_queue_channel_is_full() {
                let dir = temp_dir("enqueue-err-abandon");
                // A generous flush_queue_bytes exercises the channel's
                // slot-count limit, not the byte bound.
                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(1 << 20);
                let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
                let tier = Arc::new(SpillTier::open(&cfg, "channel-full").expect("tier opens"));
                // Paused before the flusher thread is even spawned, so it
                // never gets a chance to drain any of the filler jobs
                // below before the real eviction's own `enqueue` needs the
                // channel to still be completely full.
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                // Fill the flusher's channel to its exact slot capacity
                // with throwaway jobs, so the real eviction below finds no
                // room: the one way `finish_spill_handoff` reaches
                // `abandon` rather than `install`.
                for i in 0..slots {
                    let filler = SpillJob {
                        stripe_idx: 0,
                        hash: 0,
                        key_bytes: Bytes::from(format!("filler-{i}")),
                        ver: hlc(0, 0),
                        expires_at_ms: None,
                        encoded: Bytes::from_static(b"f"),
                        weight: 1,
                        // These fillers bypass would_accept, sent straight
                        // to enqueue to exercise the channel's slot limit.
                        admitted_bytes: 0,
                    };
                    tier.enqueue(filler)
                        .unwrap_or_else(|_| panic!("channel has room for filler {i}"));
                }

                let key = 1u32;
                let kb = key_bytes(key);
                let hash = hash_key_bytes(kb.as_ref());
                let bucket = stripe_index_from_hash(hash);
                let _ = put(&engine, key, kb.clone(), "x".repeat(30), hlc(1, 1), None, 0);

                let outcome = engine.evict_one_sampled(bucket, 0);
                assert_eq!(
                    outcome.removed_weight, 30,
                    "the hand-off still commits and frees the weight from total_weight \
                     right away, exactly as a physical removal would; abandon only puts it \
                     back once the enqueue that follows is found to have failed"
                );

                assert_eq!(
                    engine.get(&key, 0),
                    Some("x".repeat(30)),
                    "abandon restores full residency: the value is still there and readable"
                );
                let (_, weight_after) = engine.debug_totals();
                assert_eq!(
                    weight_after, 30,
                    "abandon restores the victim's weight to total_weight"
                );
                assert_eq!(
                    engine.debug_pending_spill_weight(),
                    0,
                    "abandon moves the weight back out of pending_spill_weight too"
                );
                let stripe = engine.stripe_lock(bucket).read();
                let live = stripe
                    .live
                    .iter()
                    .find(|l| record_key(&l.record) == kb.as_ref())
                    .expect("entry is present");
                assert_eq!(
                    live.weight, 30,
                    "the entry's own weight field is restored, not just the total"
                );
                drop(stripe);

                let _ = std::fs::remove_dir_all(&dir);
            }

            // The mirror of `enforce_capacity_leaves_a_refused_victim_resident_
            // and_retries_once_the_queue_drains` below: `keep_resident_when_
            // refused` is left at its default of `false` (never set), so a
            // refused hand-off falls back to the ordinary delete exactly as
            // every mode did before that policy existed.
            #[test]
            fn enforce_capacity_via_real_hand_off_bounds_ram_and_falls_back_to_queue_full() {
                let dir = temp_dir("real-backlog");
                // A flush queue that holds one small record but not two, so
                // `would_accept` refuses a second hand-off while the first
                // is still queued: the flusher is paused, so nothing ever
                // drains it.
                let record_len_upper_bound = 300u64;
                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(record_len_upper_bound);
                let tier = Arc::new(SpillTier::open(&cfg, "backlog").expect("tier opens"));
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                let engine = Engine::<u32, String>::new(250, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                // Two keys landing in the same stripe, so a single bucket's
                // sampling sees both, colder one first.
                let mut same_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
                let mut keys = Vec::new();
                for k in 1..100_000u32 {
                    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
                    let group = same_bucket.entry(bucket).or_default();
                    group.push(k);
                    if group.len() == 2 {
                        keys = group.clone();
                        break;
                    }
                }
                assert_eq!(
                    keys.len(),
                    2,
                    "1024 stripes; two collisions are found quickly"
                );
                let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(keys[0]).as_ref()));

                let _ = put(
                    &engine,
                    keys[0],
                    key_bytes(keys[0]),
                    "x".repeat(200),
                    hlc(1, 1),
                    None,
                    0,
                );
                let _ = put(
                    &engine,
                    keys[1],
                    key_bytes(keys[1]),
                    "y".repeat(200),
                    hlc(2, 1),
                    None,
                    1,
                );

                // First eviction: the colder key (`now_ms: 0`) hands off
                // cleanly, with the queue empty.
                engine.evict_one_sampled(bucket, 2);
                assert_eq!(
                    engine.get(&keys[0], 0),
                    Some("x".repeat(200)),
                    "still fully resident: a hand-off, not a removal"
                );
                assert_eq!(engine.debug_pending_spill_weight(), 200);
                let combined_after_first =
                    engine.debug_totals().1 + engine.debug_pending_spill_weight();
                assert!(
                    combined_after_first <= 250 + 200,
                    "combined weight {combined_after_first} exceeds the cap by more than one \
                     victim's worth"
                );

                // Second eviction: the queue already holds one record's
                // worth and the flusher is paused, so `would_accept` now
                // refuses; the victim falls back to an ordinary delete
                // instead of piling up a second fully-resident value.
                engine.evict_one_sampled(bucket, 2);
                assert_eq!(
                    engine.get(&keys[1], 0),
                    None,
                    "queue_full falls back to the ordinary delete-and-XOR path"
                );
                let (total_after, pending_after) =
                    (engine.debug_totals().1, engine.debug_pending_spill_weight());
                assert_eq!(total_after, 0);
                assert_eq!(pending_after, 200);
                assert!(
                    total_after + pending_after <= 250 + 200,
                    "combined weight never exceeds the cap by more than one victim's worth"
                );

                // The public entry point sees the same combined accounting
                // and returns immediately: nothing left to evict in this
                // bucket, and pending_spill_weight is still positive.
                engine.enforce_capacity(bucket, 2);

                // Once the flusher is allowed to run, the one queued job
                // installs and pending_spill_weight drains back to zero.
                tier.resume_flusher();
                assert!(poll_until(POLL_TIMEOUT, || engine
                    .debug_pending_spill_weight()
                    == 0));
                assert!(poll_until(POLL_TIMEOUT, || is_spilled(
                    &engine,
                    &key_bytes(keys[0])
                )));

                let _ = std::fs::remove_dir_all(&dir);
            }

            /// The `Mode::Replicated` mirror of
            /// `enforce_capacity_via_real_hand_off_bounds_ram_and_falls_back_to_queue_full`:
            /// identical setup, `tier.set_keep_resident_when_refused(true)`
            /// the only difference, so the second victim's refused hand-off
            /// leaves it fully resident instead of falling back to a
            /// delete, and a later `enforce_capacity` pass, once the queue
            /// has room again, retries and spills it.
            #[test]
            fn enforce_capacity_leaves_a_refused_victim_resident_and_retries_once_the_queue_drains()
            {
                let dir = temp_dir("real-backlog-keep-resident");
                let record_len_upper_bound = 300u64;
                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(record_len_upper_bound);
                let tier =
                    Arc::new(SpillTier::open(&cfg, "backlog-keep-resident").expect("tier opens"));
                tier.set_keep_resident_when_refused(true);
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                // A cap well under either single entry's own weight (200),
                // unlike the flag-off mirror's 250: that test only needs
                // one hand-off to land to fall back under its cap, but this
                // one means to keep the deferred victim's weight alone
                // over the cap even after the first hand-off fully
                // resolves, so the later retry has something left to do.
                let engine = Engine::<u32, String>::new(50, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                let mut same_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
                let mut keys = Vec::new();
                for k in 1..100_000u32 {
                    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
                    let group = same_bucket.entry(bucket).or_default();
                    group.push(k);
                    if group.len() == 2 {
                        keys = group.clone();
                        break;
                    }
                }
                assert_eq!(
                    keys.len(),
                    2,
                    "1024 stripes; two collisions are found quickly"
                );
                let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(keys[0]).as_ref()));

                let _ = put(
                    &engine,
                    keys[0],
                    key_bytes(keys[0]),
                    "x".repeat(200),
                    hlc(1, 1),
                    None,
                    0,
                );
                let _ = put(
                    &engine,
                    keys[1],
                    key_bytes(keys[1]),
                    "y".repeat(200),
                    hlc(2, 1),
                    None,
                    1,
                );

                // First eviction: the colder key hands off cleanly, with
                // the queue empty.
                engine.evict_one_sampled(bucket, 2);
                assert_eq!(engine.debug_pending_spill_weight(), 200);

                // Second eviction: the queue already holds one record's
                // worth and the flusher is paused, so `would_accept`
                // refuses again, but this tier's `keep_resident_when_
                // refused` is set, so the victim is left fully resident
                // instead of deleted.
                engine.evict_one_sampled(bucket, 2);
                assert_eq!(
                    engine.get(&keys[1], 0),
                    Some("y".repeat(200)),
                    "a Mode::Replicated victim refused by the tier stays fully resident, never \
                     deleted"
                );
                let (total_after, pending_after) =
                    (engine.debug_totals().1, engine.debug_pending_spill_weight());
                assert_eq!(
                    total_after, 200,
                    "the deferred victim's weight is untouched, still counted in total_weight"
                );
                assert_eq!(pending_after, 200, "unchanged from the first hand-off");
                assert!(
                    total_after + pending_after > 50,
                    "combined weight is still over the cap: nothing was freed by deferring"
                );
                {
                    let stripe = engine.stripe_lock(bucket).read();
                    let live = stripe
                        .live
                        .iter()
                        .find(|l| record_key(&l.record) == key_bytes(keys[1]).as_ref())
                        .expect("the deferred entry is still present");
                    assert_eq!(live.weight, 200, "its own weight field is untouched too");
                }

                // The public entry point sees the same combined accounting,
                // finds no further progress to make while a hand-off is
                // still pending, and returns rather than spin.
                engine.enforce_capacity(bucket, 2);
                assert_eq!(
                    engine.debug_totals().1,
                    200,
                    "enforce_capacity made no further change: still over the cap"
                );

                // Once the flusher drains the first job, `would_accept` has
                // room again for the deferred victim, and the next
                // `enforce_capacity` pass spills it.
                tier.resume_flusher();
                assert!(poll_until(POLL_TIMEOUT, || tier.queued_bytes() == 0));
                engine.enforce_capacity(bucket, 2);
                assert!(
                    poll_until(POLL_TIMEOUT, || is_spilled(&engine, &key_bytes(keys[1]))),
                    "the retried victim is spilled once the tier has room again"
                );

                let _ = std::fs::remove_dir_all(&dir);
            }

            #[test]
            fn finish_spill_handoff_err_releases_the_jobs_admitted_bytes() {
                let dir = temp_dir("release-admitted-bytes-on-err");
                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(1 << 20);
                let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
                let tier = Arc::new(SpillTier::open(&cfg, "release-on-err").expect("tier opens"));
                // Paused before the flusher spawns, so the channel stays
                // completely full when the real eviction's enqueue needs it.
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                for i in 0..slots {
                    let filler = SpillJob {
                        stripe_idx: 0,
                        hash: 0,
                        key_bytes: Bytes::from(format!("filler-{i}")),
                        ver: hlc(0, 0),
                        expires_at_ms: None,
                        encoded: Bytes::from_static(b"f"),
                        weight: 1,
                        admitted_bytes: 0,
                    };
                    tier.enqueue(filler)
                        .unwrap_or_else(|_| panic!("channel has room for filler {i}"));
                }

                let key = 1u32;
                let kb = key_bytes(key);
                let hash = hash_key_bytes(kb.as_ref());
                let bucket = stripe_index_from_hash(hash);
                let _ = put(&engine, key, kb.clone(), "x".repeat(30), hlc(1, 1), None, 0);

                let queued_before = tier.queued_bytes();
                let outcome = engine.evict_one_sampled(bucket, 0);
                assert_eq!(
                    outcome.removed_weight, 30,
                    "the hand-off still commits and frees the weight right away"
                );
                let queued_after = tier.queued_bytes();

                assert_eq!(
                    queued_after, queued_before,
                    "the abandoned job's admitted bytes return to admit: queued_bytes ends \
                     where it started, not permanently inflated by a job that never reached \
                     the flusher's channel"
                );
                assert_eq!(
                    engine.get(&key, 0),
                    Some("x".repeat(30)),
                    "abandon restores full residency"
                );

                let _ = std::fs::remove_dir_all(&dir);
            }

            #[tokio::test]
            async fn a_sufficient_reservation_never_falls_back_to_the_global_admit_check() {
                let dir = temp_dir("reservation-never-falls-back");
                let value = "x".repeat(20);
                // The real postcard-encoded byte length, what
                // spill_record_len/would_accept see, not the char count.
                let encoded_len = postcard::to_stdvec(&value)
                    .expect("test value encodes")
                    .len();
                let key = 1u32;
                let kb = key_bytes(key);
                let record_len = spill_record_len(kb.len(), encoded_len);
                // A flush queue sized to exactly one record, so the next
                // admission is refused once admit is drained.
                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(u64::from(record_len));
                let tier =
                    Arc::new(SpillTier::open(&cfg, "reservation-never").expect("tier opens"));
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                let hash = hash_key_bytes(kb.as_ref());
                let bucket = stripe_index_from_hash(hash);
                let _ = put(&engine, key, kb.clone(), value.clone(), hlc(1, 1), None, 0);

                // Drains admit entirely into a caller-held Reservation
                // before eviction ever samples the victim.
                let mut reservation = tier
                    .reserve(record_len, Duration::from_secs(5))
                    .await
                    .expect("the whole (single-record) queue is free before this call");
                assert_eq!(
                    tier.would_accept(None, kb.len(), encoded_len),
                    Admission::RefusedFinal,
                    "with no reservation threaded through, admit's own fallback refuses: \
                     fully drained by the reservation above"
                );

                let outcome =
                    engine.evict_one_sampled_with_reservation(bucket, Some(&mut reservation), 0);
                assert_eq!(
                    outcome.removed_weight,
                    value.len() as u64,
                    "the reservation's own pre-paid budget admits the victim; admit itself \
                     never needed to grant anything"
                );
                assert_eq!(
                    engine.get(&key, 0),
                    Some(value),
                    "still fully resident: a hand-off, not a removal"
                );

                drop(reservation);
                let _ = std::fs::remove_dir_all(&dir);
            }

            #[tokio::test]
            async fn committed_bytes_returned_match_the_sum_of_admitted_job_lengths() {
                let dir = temp_dir("admitted-bytes-sum");
                let value_a = "x".repeat(20);
                let value_b = "y".repeat(20);
                // The real postcard-encoded byte lengths, what
                // spill_record_len/would_accept see.
                let encoded_len_a = postcard::to_stdvec(&value_a)
                    .expect("test value encodes")
                    .len();
                let encoded_len_b = postcard::to_stdvec(&value_b)
                    .expect("test value encodes")
                    .len();
                let key_a = 1u32;
                let kb_a = key_bytes(key_a);
                let hash_a = hash_key_bytes(kb_a.as_ref());
                let bucket = stripe_index_from_hash(hash_a);
                let record_len_a = spill_record_len(kb_a.len(), encoded_len_a);

                let mut key_b = None;
                for k in 2..100_000u32 {
                    let kb = key_bytes(k);
                    if stripe_index_from_hash(hash_key_bytes(kb.as_ref())) == bucket {
                        key_b = Some(k);
                        break;
                    }
                }
                let key_b = key_b.expect("a second key collides into the same bucket quickly");
                let kb_b = key_bytes(key_b);
                let record_len_b = spill_record_len(kb_b.len(), encoded_len_b);

                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(u64::from(record_len_a) + u64::from(record_len_b));
                let tier = Arc::new(SpillTier::open(&cfg, "admitted-bytes").expect("tier opens"));
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                let _ = put(
                    &engine,
                    key_a,
                    kb_a.clone(),
                    value_a.clone(),
                    hlc(1, 1),
                    None,
                    0,
                );
                let _ = put(
                    &engine,
                    key_b,
                    kb_b.clone(),
                    value_b.clone(),
                    hlc(2, 1),
                    None,
                    1,
                );

                // A reservation covering exactly one victim's record: the
                // colder key spends it fully; the second falls back to admit.
                let mut reservation = tier
                    .reserve(record_len_a, Duration::from_secs(5))
                    .await
                    .expect("the whole queue is free before this call");

                let first =
                    engine.evict_one_sampled_with_reservation(bucket, Some(&mut reservation), 2);
                assert_eq!(
                    first.removed_weight,
                    value_a.len() as u64,
                    "the colder key hands off from the reservation's own budget"
                );
                let second =
                    engine.evict_one_sampled_with_reservation(bucket, Some(&mut reservation), 2);
                assert_eq!(
                    second.removed_weight,
                    value_b.len() as u64,
                    "the second key hands off from admit's own fallback, the reservation's \
                     budget already spent"
                );

                drop(reservation);

                assert_eq!(
                    tier.queued_bytes(),
                    u64::from(record_len_a) + u64::from(record_len_b),
                    "every byte admit ever handed out across this call, whether through the \
                     caller's own reservation or its own try_acquire_many fallback, is \
                     accounted for by exactly these two committed jobs"
                );

                let _ = std::fs::remove_dir_all(&dir);
            }

            /// Pins that [`Engine::enforce_capacity_with_reservation`]
            /// reports a deficit once both budgets are exhausted mid-batch.
            #[tokio::test]
            async fn enforce_capacity_with_reservation_returns_the_deficit_once_both_budgets_are_exhausted()
             {
                let dir = temp_dir("enforce-capacity-deficit");
                let value = "x".repeat(20);
                let encoded_len = postcard::to_stdvec(&value)
                    .expect("test value encodes")
                    .len();

                let mut bucket = None;
                let mut keys = Vec::new();
                for k in 1..200_000u32 {
                    let kb = key_bytes(k);
                    let b = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
                    match bucket {
                        Some(target) if b != target => continue,
                        None => bucket = Some(b),
                        Some(_) => {}
                    }
                    keys.push(k);
                    if keys.len() == 4 {
                        break;
                    }
                }
                assert_eq!(
                    keys.len(),
                    4,
                    "1024 stripes; four collisions are found quickly"
                );
                let bucket = bucket.expect("set once the first key fixed the target stripe");

                let record_len_0 = spill_record_len(key_bytes(keys[0]).len(), encoded_len);
                let record_len_1 = spill_record_len(key_bytes(keys[1]).len(), encoded_len);

                // Sized to exactly the coldest victim's record, so reserve
                // drains the tier's whole admit budget into the reservation.
                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(u64::from(record_len_0));
                let tier = Arc::new(
                    SpillTier::open(&cfg, "enforce-capacity-deficit").expect("tier opens"),
                );
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                // Four 20-byte values against a 30-unit cap attempts the
                // second victim within this same lock hold.
                let engine = Engine::<u32, String>::new(30, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                for (i, &k) in keys.iter().enumerate() {
                    let now_ms = u64::try_from(i).expect("four keys, fits comfortably");
                    let _ = put(
                        &engine,
                        k,
                        key_bytes(k),
                        value.clone(),
                        hlc(now_ms + 1, 1),
                        None,
                        now_ms,
                    );
                }

                let mut reservation = tier
                    .reserve(record_len_0, Duration::from_secs(5))
                    .await
                    .expect("the whole (single-record) queue is free before this call");

                let deficit =
                    engine.enforce_capacity_with_reservation(bucket, Some(&mut reservation), 10);
                assert_eq!(
                    deficit,
                    u64::from(record_len_1),
                    "keys[1]'s own record length: right now, neither the reservation's \
                     remaining budget nor admit's own headroom covers it"
                );

                assert_eq!(
                    engine.get(&keys[0], 0),
                    Some(value.clone()),
                    "the coldest victim hands off cleanly, spent from the reservation"
                );
                assert_eq!(engine.debug_pending_spill_weight(), 20);
                assert_eq!(
                    engine.get(&keys[1], 0),
                    Some(value.clone()),
                    "left fully resident: a pending refusal, not a removal and not a \
                     keep-resident deferral either, no drop of any kind recorded for it"
                );
                assert_eq!(
                    engine.get(&keys[2], 0),
                    Some(value.clone()),
                    "never even sampled: the deficit stopped the pass before a second batch \
                     or scan could reach it"
                );
                assert_eq!(engine.get(&keys[3], 0), Some(value));
                let (_, total_after) = engine.debug_totals();
                assert_eq!(
                    total_after, 60,
                    "keys[1..4] still fully resident and counted: three victims' worth"
                );

                drop(reservation);
                let _ = std::fs::remove_dir_all(&dir);
            }

            /// Pins that the closing `enforce_capacity` call physically
            /// deletes a deficit-left victim once retries are exhausted.
            #[tokio::test]
            async fn enforce_capacity_deletes_a_deficit_left_victim_once_reservation_retries_give_up()
             {
                let dir = temp_dir("enforce-capacity-deficit-fallback");
                let value = "x".repeat(20);
                let encoded_len = postcard::to_stdvec(&value)
                    .expect("test value encodes")
                    .len();

                let mut bucket = None;
                let mut keys = Vec::new();
                for k in 1..200_000u32 {
                    let kb = key_bytes(k);
                    let b = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
                    match bucket {
                        Some(target) if b != target => continue,
                        None => bucket = Some(b),
                        Some(_) => {}
                    }
                    keys.push(k);
                    if keys.len() == 4 {
                        break;
                    }
                }
                assert_eq!(
                    keys.len(),
                    4,
                    "1024 stripes; four collisions are found quickly"
                );
                let bucket = bucket.expect("set once the first key fixed the target stripe");

                let record_len_0 = spill_record_len(key_bytes(keys[0]).len(), encoded_len);
                let record_len_1 = spill_record_len(key_bytes(keys[1]).len(), encoded_len);

                let cfg = SpillConfig::new(&dir, 1 << 20)
                    .region_bytes(4096)
                    .flush_queue_bytes(u64::from(record_len_0));
                let tier = Arc::new(
                    SpillTier::open(&cfg, "enforce-capacity-deficit-fallback").expect("tier opens"),
                );
                tier.pause_flusher();
                let weigher: Weigher<u32, String> =
                    Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
                let engine = Engine::<u32, String>::new(30, None, Some(weigher), None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                for (i, &k) in keys.iter().enumerate() {
                    let now_ms = u64::try_from(i).expect("four keys, fits comfortably");
                    let _ = put(
                        &engine,
                        k,
                        key_bytes(k),
                        value.clone(),
                        hlc(now_ms + 1, 1),
                        None,
                        now_ms,
                    );
                }

                let mut reservation = tier
                    .reserve(record_len_0, Duration::from_secs(5))
                    .await
                    .expect("the whole (single-record) queue is free before this call");

                let deficit =
                    engine.enforce_capacity_with_reservation(bucket, Some(&mut reservation), 10);
                assert_eq!(deficit, u64::from(record_len_1));
                assert_eq!(
                    engine.get(&keys[1], 0),
                    Some(value.clone()),
                    "left fully resident by the first pass, exactly like the sibling test above"
                );

                // The reservation returns its spent remainder on drop, so
                // nothing is left for the closing call below to spend.
                drop(reservation);

                // The closing call falls back to the ordinary
                // `reservation: None` path and keeps evicting.
                engine.enforce_capacity(bucket, 10);

                assert_eq!(
                    engine.get(&keys[1], 0),
                    None,
                    "keep_resident_when_refused defaults to false: the closing fallback must \
                     physically delete the leftover victim, not leave max_capacity exceeded \
                     forever because a reservation retry gave up on it"
                );
                // keys[0]'s pending hand-off makes defer_to_flusher stop
                // this fallback early instead of scanning further.
                let (_, total_after_fallback) = engine.debug_totals();
                assert!(
                    total_after_fallback < 60,
                    "the closing fallback must make real progress against the cap, not just \
                     record a metric: total={total_after_fallback}"
                );

                let _ = std::fs::remove_dir_all(&dir);
            }

            /// `resolve_conflict`'s tombstone/spill guard only ever skips
            /// calling a misbehaving resolver's own `merge`; a well-behaved
            /// value-aware resolver's *own* fallback (`winner` returning
            /// `A`/`B` by `Hlc` order whenever it is handed a value-less
            /// side) must never fire here merely because the stored side
            /// is spilled rather than genuinely value-less. This
            /// pins that `apply_locked` reads a spilled stored record's real
            /// bytes back off disk before consulting the resolver, so a
            /// merge against it folds exactly as it would against a resident
            /// record, rather than discarding whichever side loses the
            /// outright `Hlc` fallback.
            #[test]
            fn merge_against_a_spilled_counter_folds_the_stored_side_instead_of_dropping_it() {
                use crate::store::crdt::{PnCounter, PnCounterResolver};

                let dir = temp_dir("merge-spilled");
                let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
                let tier = Arc::new(SpillTier::open(&cfg, "merge-spilled").expect("tier opens"));
                let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

                let resolver = PnCounterResolver;
                let key = 1u32;
                let kb = key_bytes(key);
                let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
                let node_a = crate::store::crdt::WriterId::new(NodeId::from(11), 1);
                let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);

                let _ = put_with_resolver(
                    &engine,
                    key,
                    kb.clone(),
                    PnCounter::local_delta(node_a, 3),
                    hlc(1, 1),
                    None,
                    0,
                    &resolver,
                );

                let _ = engine.evict_one_sampled(bucket, 0);
                assert!(
                    poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
                    "the flusher installs the spilled entry"
                );

                // Colliding write from a second, disjoint writer: before the
                // spilled-value read existed, the stored side's value-less
                // view would send `PnCounterResolver` to its plain-`Hlc`
                // fallback, and this strictly newer incoming version would
                // win outright, discarding node_a's spilled contribution
                // instead of folding it in.
                let outcome = put_with_resolver(
                    &engine,
                    key,
                    kb.clone(),
                    PnCounter::local_delta(node_b, 4),
                    hlc(5, 2),
                    None,
                    0,
                    &resolver,
                );

                match outcome {
                    ApplyOutcome::Put { value, .. } => {
                        assert_eq!(
                            value.value(),
                            7,
                            "merging against a spilled stored record folds its contribution \
                             instead of dropping it"
                        );
                    }
                    ApplyOutcome::Rejected => {
                        panic!("expected a Put outcome carrying the merged counter, got Rejected")
                    }
                    ApplyOutcome::Tombstoned { .. } => {
                        panic!("expected a Put outcome carrying the merged counter, got Tombstoned")
                    }
                }
                assert_eq!(
                    engine.get(&key, 0).map(|c| c.value()),
                    Some(7),
                    "the merged counter reads back resident, carrying both sides' \
                     contributions"
                );

                let _ = std::fs::remove_dir_all(&dir);
            }

            /// `peek_stored_seed`'s prefold seeding is spill-aware exactly
            /// like `apply_locked`'s own stored-side lookup: a same-key,
            /// multi-entry batch landing on an already-spilled stored record
            /// folds `P` in *first*, ahead of the batch's own entries, the
            /// same left-to-right order sequential per-record application
            /// uses, so the version the batched fold mints matches
            /// sequential application's exactly, not only its content.
            /// Seeding from a spilled `P` this way is what this test pins:
            /// treating a spilled `P` as absent, seeding the fold with the
            /// batch's own first entry instead, would still mint correct
            /// content (`apply_locked`'s own final call still re-folds the
            /// real `P` in) but not the identical `Hlc` sequential
            /// application mints.
            /// This test's own `(version, value)` reader, shared by its
            /// sequential-reference and batched-under-test engines.
            fn stored_ver_and_value(
                engine: &Engine<u32, crate::store::crdt::PnCounter>,
                key: u32,
                kb: &Bytes,
                bucket: usize,
            ) -> (Hlc, i128) {
                let ver = engine
                    .collect_buckets(&[u16::try_from(bucket).expect("bucket fits u16")], 0)
                    .into_iter()
                    .flat_map(|(_, entries)| entries)
                    .find(|kv| kv.key.as_ref() == kb.as_ref())
                    .map(|kv| kv.version)
                    .expect("the key is live");
                let value = engine
                    .get(&key, 0)
                    .expect("the key reads back resident")
                    .value();
                (ver, value)
            }

            /// A fresh engine, seeded with one `(seed, seed_ver)` record at
            /// `key`, then evicted and confirmed spilled: this test's own
            /// `P`, shared by its batched-under-test engine. Returns the temp
            /// dir (removed by the caller once done) alongside the engine.
            fn spilled_pn_counter_engine(
                dir_name: &str,
                key: u32,
                kb: &Bytes,
                bucket: usize,
                seed: crate::store::crdt::PnCounter,
                seed_ver: Hlc,
                resolver: &dyn ConflictResolver,
            ) -> (
                std::path::PathBuf,
                Arc<Engine<u32, crate::store::crdt::PnCounter>>,
            ) {
                let dir = temp_dir(dir_name);
                let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
                let tier = Arc::new(SpillTier::open(&cfg, dir_name).expect("tier opens"));
                let engine = Engine::new(u64::MAX, None, None, None);
                engine.set_spill(Arc::clone(&tier));
                let engine = Arc::new(engine);
                tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));
                let _ =
                    put_with_resolver(&engine, key, kb.clone(), seed, seed_ver, None, 0, resolver);
                let _ = engine.evict_one_sampled(bucket, 0);
                assert!(
                    poll_until(POLL_TIMEOUT, || is_spilled(&engine, kb)),
                    "the flusher installs the spilled entry"
                );
                (dir, engine)
            }

            #[test]
            fn prefold_seeds_a_spilled_stored_record_so_the_batch_matches_sequential_application() {
                use crate::store::crdt::{PnCounter, PnCounterResolver};

                let resolver = PnCounterResolver;
                let key = 1u32;
                let kb = key_bytes(key);
                let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
                let node_p_id = NodeId::from(1);
                let node_p = crate::store::crdt::WriterId::new(node_p_id, 1);
                let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);
                let node_c = crate::store::crdt::WriterId::new(NodeId::from(33), 1);
                // A nonzero `logical` simulates `P` already carrying prior
                // merge history (a realistic starting point for a spilled
                // counter): with an all-fresh, all-zero-`logical` `P`, the
                // mint arm's `max(..) + 1` chain lands on the identical
                // `(wall_ms, logical)` regardless of fold order (both paths
                // always cost exactly two total merge steps for three
                // leaves), which would let this test pass even with the bug
                // it's meant to catch.
                let p_ver = Hlc {
                    wall_ms: 1,
                    logical: 5,
                    node: node_p_id,
                };
                let e0 = (hlc(5, 2), PnCounter::local_delta(node_b, 4));
                let e1 = (hlc(6, 3), PnCounter::local_delta(node_c, 9));

                // The reference: `P`, then each batch entry, one real
                // `apply_many` call at a time against a plain resident
                // engine, never spilled, since spilling is this test's own
                // artifact, not part of the CRDT history being compared.
                let sequential = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
                for (value, ver) in [
                    (PnCounter::local_delta(node_p, 3), p_ver),
                    (e0.1.clone(), e0.0),
                    (e1.1.clone(), e1.0),
                ] {
                    let _ = put_with_resolver(
                        &sequential,
                        key,
                        kb.clone(),
                        value,
                        ver,
                        None,
                        0,
                        &resolver,
                    );
                }
                let (sequential_ver, sequential_value) =
                    stored_ver_and_value(&sequential, key, &kb, bucket);

                // The batch under test: `P` applied and spilled exactly like
                // the sibling test above, then `e0` and `e1` folded through
                // one real `apply_many` call, prefold-eligible since both
                // share a key and `PnCounterResolver::merges()` is `true`.
                let (dir, batched) = spilled_pn_counter_engine(
                    "prefold-seed-spilled",
                    key,
                    &kb,
                    bucket,
                    PnCounter::local_delta(node_p, 3),
                    p_ver,
                    &resolver,
                );
                assert!(
                    batched.prefold_enabled(),
                    "prefold is on by default: this batch must actually exercise it"
                );

                let hash = hash_key_bytes(kb.as_ref());
                let e0_encoded = Bytes::from(postcard::to_stdvec(&e0.1).expect("encodes"));
                let e1_encoded = Bytes::from(postcard::to_stdvec(&e1.1).expect("encodes"));
                let outcomes = batched.apply_many(
                    bucket,
                    vec![
                        (
                            hash,
                            key,
                            kb.clone(),
                            e0.0,
                            Incoming::Put {
                                value: e0.1,
                                expires_at_ms: None,
                                encoded: e0_encoded,
                            },
                        ),
                        (
                            hash,
                            key,
                            kb.clone(),
                            e1.0,
                            Incoming::Put {
                                value: e1.1,
                                expires_at_ms: None,
                                encoded: e1_encoded,
                            },
                        ),
                    ],
                    &resolver,
                    60_000,
                    600_000,
                    0,
                );
                assert_eq!(
                    outcomes.len(),
                    2,
                    "one outcome per entry given, pre-fold or not"
                );

                let (batched_ver, batched_value) = stored_ver_and_value(&batched, key, &kb, bucket);

                assert_eq!(
                    batched_value, sequential_value,
                    "both paths fold every side's contribution into the same total"
                );
                assert_eq!(
                    batched_ver, sequential_ver,
                    "seeding the fold with the real (spilled) stored record first mints the \
                     exact version sequential per-record application would have, not merely \
                     the same content"
                );

                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }

    /// Coverage for [`Engine::compact`]: how its cursor rotates across
    /// calls, that it never locks more of the engine than its budget needs,
    /// and that a spilled entry is never handed to the resolver.
    /// [`crate::store::crdt::PnCounter`]/[`crate::store::crdt::PnCounterResolver`]
    /// (real types, not a stub) exercise the resolver-hook plumbing end to
    /// end, since a fake resolver would only prove this method calls
    /// *something*, not that it hands the real hook real arguments.
    mod compact_sweep {
        use super::*;
        use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};

        /// The smallest key whose hash lands in stripe `target`. Exists for
        /// every `target < BUCKET_COUNT`: `stripe_index_from_hash` hashes
        /// over the full `u32` key space, uniformly enough that some small
        /// key lands in every stripe.
        fn key_for_stripe(target: usize) -> u32 {
            (0u32..1 << 20)
                .find(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == target)
                .expect("some u32 key well within range hashes into every stripe")
        }

        fn writer(id: u64) -> WriterId {
            WriterId::new(NodeId::from(id), 1)
        }

        /// `count` distinct keys, all landing in stripe `target`.
        fn same_stripe_keys(target: usize, count: usize) -> Vec<u32> {
            (0u32..1 << 20)
                .filter(|&k| {
                    stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == target
                })
                .take(count)
                .collect()
        }

        fn seed_counter(
            engine: &Engine<u32, PnCounter>,
            resolver: PnCounterResolver,
            key: u32,
            writer_id: WriterId,
        ) {
            let kb = key_bytes(key);
            let _ = put_with_resolver(
                engine,
                key,
                kb,
                PnCounter::local_delta(writer_id, 1),
                hlc(1, 1),
                None,
                0,
                &resolver,
            );
        }

        /// Four resident counters, each alone in its own stripe, each
        /// carrying one retirement-eligible writer. One entry per
        /// [`Engine::compact`] call, cursor pinned to `0` beforehand: each
        /// call must return exactly the one entry sitting between the
        /// cursor and the next seeded stripe, in ascending stripe order,
        /// and leave the cursor immediately past the stripe it visited,
        /// proving rotation carries forward across calls instead of
        /// restarting at stripe `0` (which would instead return the same
        /// first entry every time) or skipping ahead arbitrarily.
        #[test]
        fn compact_rotates_the_cursor_across_stripes_between_calls() {
            let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
            let resolver = PnCounterResolver;
            let targets = [3usize, 200, 500, 900];
            let keys: Vec<u32> = targets.iter().map(|&t| key_for_stripe(t)).collect();
            for (i, &key) in keys.iter().enumerate() {
                seed_counter(&engine, resolver, key, writer(u64::try_from(i).unwrap()));
            }

            engine.debug_set_compact_cursor(0);
            let retire_everyone = |_: WriterId| true;
            let mut seen = Vec::new();
            for &target in &targets {
                let out = engine
                    .compact(
                        &resolver,
                        1_000,
                        &retire_everyone,
                        Quiescence::Churning,
                        crate::store::CompactionBounds::three_bounds(0),
                        1,
                    )
                    .candidates;
                assert_eq!(
                    out.len(),
                    1,
                    "exactly one seeded entry sits between the cursor and the next target"
                );
                seen.push(out[0].key);
                assert_eq!(
                    engine.debug_compact_cursor(),
                    u64::try_from((target + 1) % BUCKET_COUNT).unwrap(),
                    "the cursor advances to just past the stripe it last visited"
                );
            }
            assert_eq!(
                seen, keys,
                "four calls visit the four seeded stripes in rotation, none repeated, none \
                 skipped"
            );
        }

        /// A single entry seeded in stripe `5`, cursor pinned to stripe
        /// `0`: one [`Engine::compact`] call with `max_entries: 1` must
        /// stop the instant its budget is met, locking stripes `0..=5`
        /// (six stripes) and no more, never the whole 1024-stripe engine,
        /// and never fewer than it needed to find the one entry.
        /// Each stripe's lock is taken and dropped before the next is
        /// touched (`Engine::compact`'s own structure never calls
        /// `self.stripes[idx].read()` a second time before the previous
        /// guard is dropped), so this count is exactly the number of
        /// distinct stripes visited, one lock each.
        #[test]
        fn compact_locks_only_as_many_stripes_as_its_budget_needs() {
            let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
            let resolver = PnCounterResolver;
            let key = key_for_stripe(5);
            seed_counter(&engine, resolver, key, writer(1));

            engine.debug_set_compact_cursor(0);
            let retire_everyone = |_: WriterId| true;
            let before = engine.debug_compact_lock_acquisitions();
            let out = engine
                .compact(
                    &resolver,
                    1_000,
                    &retire_everyone,
                    Quiescence::Churning,
                    crate::store::CompactionBounds::three_bounds(0),
                    1,
                )
                .candidates;
            let after = engine.debug_compact_lock_acquisitions();

            assert_eq!(out.len(), 1);
            assert_eq!(
                after - before,
                6,
                "stripes 0..=5 are visited (the entry sits in stripe 5) — not the whole engine"
            );
        }

        /// One entry seeded into a stripe an inflated [`Engine::new`]
        /// `capacity_hint` presized far past what it ever held:
        /// [`Engine::compact`]'s visit to that stripe must call
        /// [`should_shrink_stripe`] and, finding `1 <= 100 / 8`, shrink the
        /// slab back down toward its one live entry instead of leaving it
        /// at its original oversized allocation.
        #[test]
        fn compact_shrinks_an_overallocated_stripe_toward_its_len() {
            let target = 7usize;
            let per_stripe_capacity = 100usize;
            let hint = per_stripe_capacity as u64 * BUCKET_COUNT as u64;
            let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, Some(hint));
            assert_eq!(
                engine.stripe_capacities()[target],
                per_stripe_capacity,
                "the hint presizes every stripe, including this one, before any write"
            );

            let resolver = PnCounterResolver;
            let key = key_for_stripe(target);
            seed_counter(&engine, resolver, key, writer(1));

            engine.debug_set_compact_cursor(target as u64);
            let retire_everyone = |_: WriterId| true;
            let _ = engine.compact(
                &resolver,
                1_000,
                &retire_everyone,
                Quiescence::Churning,
                crate::store::CompactionBounds::three_bounds(0),
                1,
            );

            let shrunk = engine.stripe_capacities()[target];
            assert!(
                shrunk < per_stripe_capacity,
                "one entry in a 100-capacity stripe sits well under the eighth-capacity \
                 threshold, so the visit shrinks it: got {shrunk}"
            );
            assert!(
                shrunk <= 8,
                "shrink_to_fit brings the arena back down toward its one live entry, not only \
                 partway: got {shrunk}"
            );
        }

        /// Four entries seeded into an 8-capacity stripe: `4 > 8 / 8`, so
        /// [`should_shrink_stripe`] says no.
        #[test]
        fn compact_leaves_a_well_utilized_stripe_alone() {
            let target = 9usize;
            let per_stripe_capacity = 8usize;
            let hint = per_stripe_capacity as u64 * BUCKET_COUNT as u64;
            let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, Some(hint));

            let resolver = PnCounterResolver;
            let keys = same_stripe_keys(target, 4);
            assert_eq!(keys.len(), 4, "four distinct keys land in this stripe");
            for (i, &k) in keys.iter().enumerate() {
                seed_counter(&engine, resolver, k, writer(u64::try_from(i).unwrap()));
            }
            assert_eq!(engine.stripe_capacities()[target], per_stripe_capacity);

            engine.debug_set_compact_cursor(target as u64);
            let retire_everyone = |_: WriterId| true;
            let _ = engine.compact(
                &resolver,
                1_000,
                &retire_everyone,
                Quiescence::Churning,
                crate::store::CompactionBounds::three_bounds(0),
                4,
            );

            assert_eq!(
                engine.stripe_capacities()[target],
                per_stripe_capacity,
                "4 of 8 slots filled sits above the eighth-capacity shrink threshold, so the \
                 visit leaves this stripe's allocation untouched"
            );
        }

        /// A resident entry and a spilled one, both carrying a
        /// retirement-eligible writer: only the resident entry is ever
        /// handed to [`ConflictResolver::compact`]. A spilled payload has
        /// no bytes in RAM for a resolver to read without a disk read this
        /// sweep does not pay for; it must be silently skipped, not
        /// errored or panicked on, and must never suppress the resident
        /// candidate sharing the pass.
        #[cfg(feature = "spill")]
        #[test]
        fn compact_never_hands_a_spilled_payload_to_the_resolver() {
            let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
            let resolver = PnCounterResolver;

            let spilled_key = 1u32;
            let spilled_kb = key_bytes(spilled_key);
            engine.debug_insert_spilled(
                &spilled_kb,
                hlc(1, 1),
                None,
                SpillLoc {
                    region: 0,
                    offset: 0,
                    len: 4,
                    generation: 0,
                },
                0,
            );

            let resident_key = 2u32;
            seed_counter(&engine, resolver, resident_key, writer(1));

            engine.debug_set_compact_cursor(0);
            let retire_everyone = |_: WriterId| true;
            let out = engine
                .compact(
                    &resolver,
                    1_000,
                    &retire_everyone,
                    Quiescence::Churning,
                    crate::store::CompactionBounds::three_bounds(0),
                    usize::MAX,
                )
                .candidates;

            assert_eq!(
                out.len(),
                1,
                "the spilled entry is skipped; only the resident one is a candidate"
            );
            assert_eq!(out[0].key, resident_key);
        }
    }

    /// Property coverage for [`merge_version`] alone: every claim its doc
    /// comment's convergence argument rests on is checked here rather than
    /// assumed from the construction, namely that the mint arm strictly
    /// dominates both inputs, that different merged bytes mint different
    /// nodes, that each of the other three arms returns exactly what the
    /// rule says, and that a mint is always recognizable as one (never a
    /// real node's id).
    mod merge_version {
        use proptest::prelude::*;

        use super::*;

        /// A real single-writer stamp: `wall_ms`/`logical` unconstrained,
        /// `node` drawn from the lower half of the `u64` range only,
        /// everything [`NodeId::random`] can ever produce. A generated pair
        /// here can never itself be merge-derived, so any `Store` arm that
        /// reproduces one of `sv`/`ver` verbatim carries a real node id, and
        /// only the mint arm ever introduces one that isn't.
        fn arb_real_hlc() -> impl Strategy<Value = Hlc> {
            (any::<u64>(), any::<u32>(), 0..(1u64 << 63)).prop_map(|(wall_ms, logical, node)| Hlc {
                wall_ms,
                logical,
                node: NodeId::from(node),
            })
        }

        /// Short byte strings, generated independently for `stored`,
        /// `incoming`, and `merged` so the mint-arm tests below can assume
        /// pairwise inequality without biasing the distribution.
        fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
            proptest::collection::vec(any::<u8>(), 0..8)
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(512))]

            /// (a) + (d): whenever the merged bytes match neither side's own
            /// bytes, the mint arm fires unconditionally (none of the other
            /// three arms' equality preconditions can hold), and its result
            /// both strictly dominates `sv` and `ver` under `Hlc`'s `Ord`
            /// and is recognizable as a mint, never a real node's stamp.
            #[test]
            fn mint_arm_dominates_both_inputs_and_is_always_merge_derived(
                sv in arb_real_hlc(),
                ver in arb_real_hlc(),
                stored in arb_bytes(),
                incoming in arb_bytes(),
                merged in arb_bytes(),
            ) {
                prop_assume!(merged != stored);
                prop_assume!(merged != incoming);
                let MergedVersion::Store(minted) =
                    merge_version(sv, ver, &stored, &incoming, &merged)
                else {
                    panic!("merged bytes matching neither side always mints a version");
                };
                prop_assert!(minted > sv);
                prop_assert!(minted > ver);
                prop_assert!(minted.node.is_merge_derived());
            }

            /// (b): two mints for merged bytes that differ from each other
            /// (and from both sides, so both trigger the mint arm) mint
            /// different node components: the property that keeps two
            /// nodes minting for genuinely different content from ever
            /// colliding on the same version.
            #[test]
            fn mint_arm_different_merged_bytes_mint_different_nodes(
                sv in arb_real_hlc(),
                ver in arb_real_hlc(),
                stored in arb_bytes(),
                incoming in arb_bytes(),
                merged_a in arb_bytes(),
                merged_b in arb_bytes(),
            ) {
                prop_assume!(merged_a != stored && merged_a != incoming);
                prop_assume!(merged_b != stored && merged_b != incoming);
                prop_assume!(merged_a != merged_b);
                let MergedVersion::Store(a) =
                    merge_version(sv, ver, &stored, &incoming, &merged_a)
                else {
                    panic!("merged bytes matching neither side always mints a version");
                };
                let MergedVersion::Store(b) =
                    merge_version(sv, ver, &stored, &incoming, &merged_b)
                else {
                    panic!("merged bytes matching neither side always mints a version");
                };
                prop_assert_ne!(a.node, b.node);
            }

            /// (c), first arm: identical content on both sides is pure
            /// version reconciliation: adopt whichever of `sv`/`ver` is
            /// greater, or do nothing if `sv` already is.
            #[test]
            fn identical_content_arm_adopts_the_greater_version_or_no_ops(
                sv in arb_real_hlc(),
                ver in arb_real_hlc(),
                content in arb_bytes(),
            ) {
                prop_assume!(sv != ver);
                let result = merge_version(sv, ver, &content, &content, &content);
                let greater = sv.max(ver);
                if greater == sv {
                    prop_assert_eq!(result, MergedVersion::NoOp);
                } else {
                    prop_assert_eq!(result, MergedVersion::Store(greater));
                }
            }

            /// (c), second arm: the merge reducing to incoming's own bytes,
            /// with incoming genuinely newer, adopts incoming's `(ver,
            /// merged)` pair verbatim.
            #[test]
            fn reduces_to_incoming_arm_adopts_incoming_verbatim(
                sv in arb_real_hlc(),
                ver in arb_real_hlc(),
                stored in arb_bytes(),
                incoming in arb_bytes(),
            ) {
                prop_assume!(ver > sv);
                prop_assume!(stored != incoming);
                let result = merge_version(sv, ver, &stored, &incoming, &incoming);
                prop_assert_eq!(result, MergedVersion::Store(ver));
            }

            /// (c), third arm: the merge reducing to stored's own bytes,
            /// with stored genuinely newer, is a no-op: incoming is fully
            /// absorbed already.
            #[test]
            fn reduces_to_stored_arm_is_a_no_op(
                sv in arb_real_hlc(),
                ver in arb_real_hlc(),
                stored in arb_bytes(),
                incoming in arb_bytes(),
            ) {
                prop_assume!(sv > ver);
                prop_assume!(stored != incoming);
                let result = merge_version(sv, ver, &stored, &incoming, &stored);
                prop_assert_eq!(result, MergedVersion::NoOp);
            }
        }

        /// (a) at the one corner `arb_real_hlc`'s random `logical` values
        /// have negligible odds of ever hitting: both inputs' `logical` is
        /// already `u32::MAX`, tied on `wall_ms` too. The `+ 1` has no room
        /// to grow `logical`, so it must carry into `wall_ms` instead for
        /// the mint to still strictly dominate both inputs.
        #[test]
        fn mint_arm_dominates_on_a_logical_overflow_at_a_wall_ms_tie() {
            let sv = Hlc {
                wall_ms: 1_000,
                logical: u32::MAX,
                node: NodeId::from(1u64),
            };
            let ver = Hlc {
                wall_ms: 1_000,
                logical: u32::MAX,
                node: NodeId::from(2u64),
            };
            let stored = b"stored".to_vec();
            let incoming = b"incoming".to_vec();
            let merged = b"merged".to_vec();
            let MergedVersion::Store(minted) = merge_version(sv, ver, &stored, &incoming, &merged)
            else {
                panic!("merged bytes matching neither side always mints a version");
            };
            assert!(minted > sv, "{minted:?} must dominate {sv:?}");
            assert!(minted > ver, "{minted:?} must dominate {ver:?}");
            assert_eq!(
                minted.wall_ms, 1_001,
                "the overflow must carry into wall_ms"
            );
            assert_eq!(minted.logical, 0, "logical resets once it carries");
            assert!(minted.node.is_merge_derived());
        }
    }

    /// Coverage for [`Engine::apply_many`]'s pre-fold: that a non-merging
    /// resolver never triggers it, that a run never crosses a tombstone,
    /// and, the property [`prefold_batch`]'s and [`fold_run`]'s docs argue
    /// for from the resolver's join-semilattice contract, that
    /// pre-fold changes only how many `apply_locked` calls a batch costs,
    /// never the `(version, bytes)` it leaves stored or the set of keys it
    /// reports a real outcome for.
    mod apply_many_prefold {
        use proptest::prelude::*;

        use super::*;

        type SetEngine = Engine<u32, std::collections::BTreeSet<String>>;
        type SetEntry = (
            u64,
            u32,
            Bytes,
            Hlc,
            Incoming<std::collections::BTreeSet<String>>,
        );

        /// Builds one `apply_many` entry: a `Put` of the single element
        /// `elem` for `key` under `ver`.
        fn put_entry(key: u32, ver: Hlc, elem: &str) -> SetEntry {
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let value = string_set(&[elem]);
            let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
            (
                hash,
                key,
                kb,
                ver,
                Incoming::Put {
                    value,
                    expires_at_ms: None,
                    encoded,
                },
            )
        }

        /// Applies `entries` to `engine` the way a real caller does: grouped
        /// by the stripe each entry's hash falls into, one
        /// [`Engine::apply_many`] call per touched stripe.
        /// [`Engine::apply_many`] itself trusts its caller to have already
        /// grouped a batch by stripe; it never checks `bucket` against the
        /// entries it's given.
        fn apply_batch(
            engine: &SetEngine,
            entries: Vec<SetEntry>,
            resolver: &dyn ConflictResolver,
        ) -> Vec<ApplyOutcome<u32, std::collections::BTreeSet<String>>> {
            let mut by_bucket: HashMap<usize, Vec<SetEntry>> = HashMap::new();
            for entry in entries {
                by_bucket
                    .entry(stripe_index_from_hash(entry.0))
                    .or_default()
                    .push(entry);
            }
            let mut outcomes = Vec::new();
            for (bucket, group) in by_bucket {
                outcomes.extend(engine.apply_many(bucket, group, resolver, 60_000, 600_000, 0));
            }
            outcomes
        }

        fn arb_elem() -> impl Strategy<Value = &'static str> {
            prop_oneof![Just("a"), Just("b"), Just("c"), Just("d")]
        }

        /// One batch entry spec: a small key, so several entries collide on
        /// one key within a batch; a real per-writer [`Hlc`] over a narrow
        /// `wall_ms` range and a handful of `node`s, so ties and near-ties,
        /// the cases `merge_version` treats specially, are common; and a
        /// one-element `Put`.
        fn arb_entry_spec() -> impl Strategy<Value = (u32, u64, u64, &'static str)> {
            (0u32..4, 0u64..12, 1u64..4, arb_elem())
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Replaying the identical batch through two fresh engines, one
            /// pre-folding (the default) and one with it forced off, stores
            /// byte-for-byte, `Hlc`-for-`Hlc` the same `(version, bytes)`
            /// per key: `UnionSetResolver`'s join (set union) is
            /// commutative, associative, and idempotent, which is exactly
            /// what makes fold order irrelevant to the final *content*, the
            /// same argument `merge_version`'s doc makes for two
            /// anti-entropy replicas folding in either order, and
            /// `prefold_batch`'s seeding with the key's real stored record
            /// is what extends that fold-order independence to the minted
            /// *version* too, and reports a real,
            /// non-[`ApplyOutcome::Rejected`] outcome for exactly the same
            /// set of keys either way.
            ///
            /// A `setup` batch is applied to both engines first, through
            /// the exact same on/off split, so every key the later `main`
            /// batch touches already has a real stored record with a
            /// non-trivial version behind it, the case
            /// [`prefold_batch`]'s seeding exists for: without it, folding
            /// `main`'s own entries together before ever consulting that
            /// stored record mints a different version than sequential
            /// application, even though both still converge on the same
            /// bytes.
            #[test]
            fn prefold_on_and_off_store_identical_state_and_publish_the_same_keys(
                setup in proptest::collection::vec(arb_entry_spec(), 0..12),
                main in proptest::collection::vec(arb_entry_spec(), 1..40),
            ) {
                let resolver = UnionSetResolver;
                let build = |specs: &[(u32, u64, u64, &'static str)]| -> Vec<SetEntry> {
                    specs
                        .iter()
                        .map(|&(key, wall_ms, node, elem)| put_entry(key, hlc(wall_ms, node), elem))
                        .collect()
                };

                let on: SetEngine = Engine::new(u64::MAX, None, None, None);
                let off: SetEngine = Engine::new(u64::MAX, None, None, None);
                off.set_prefold_enabled(false);

                // Seeds each engine with real stored state per key before
                // the batch under comparison ever runs, through the same
                // on/off split as `main` below.
                apply_batch(&on, build(&setup), &resolver);
                apply_batch(&off, build(&setup), &resolver);

                let on_outcomes = apply_batch(&on, build(&main), &resolver);
                let off_outcomes = apply_batch(&off, build(&main), &resolver);
                prop_assert_eq!(on_outcomes.len(), main.len());
                prop_assert_eq!(off_outcomes.len(), main.len());

                let touched_keys: std::collections::BTreeSet<u32> = setup
                    .iter()
                    .chain(main.iter())
                    .map(|&(key, ..)| key)
                    .collect();
                for key in touched_keys {
                    let kb = key_bytes(key);
                    prop_assert_eq!(
                        on.record_for(kb.as_ref(), 0),
                        off.record_for(kb.as_ref(), 0),
                        "pre-fold changes only how many apply_locked calls a batch costs, \
                         never the stored (version, bytes) — including once a key already \
                         has a real stored record behind it"
                    );
                }

                let on_keys: std::collections::BTreeSet<u32> =
                    on_outcomes.iter().filter_map(ApplyOutcome::key).copied().collect();
                let off_keys: std::collections::BTreeSet<u32> =
                    off_outcomes.iter().filter_map(ApplyOutcome::key).copied().collect();
                prop_assert_eq!(
                    on_keys, off_keys,
                    "pre-fold on and off publish a real outcome for the same set of keys"
                );
            }
        }

        #[test]
        fn a_non_merging_resolver_never_prefolds() {
            let engine: Engine<u32, String> = engine_u32_string(u64::MAX, None);
            let resolver = LwwResolver;
            let k = 1u32;
            let kb = key_bytes(k);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let value_entry = |ver: Hlc, value: &str| {
                let value = value.to_string();
                let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
                (
                    hash,
                    k,
                    kb.clone(),
                    ver,
                    Incoming::Put {
                        value,
                        expires_at_ms: None,
                        encoded,
                    },
                )
            };
            let entries = vec![
                value_entry(hlc(1, 1), "a"),
                value_entry(hlc(2, 1), "b"),
                value_entry(hlc(3, 1), "c"),
            ];

            let outcomes = engine.apply_many(bucket, entries, &resolver, 60_000, 600_000, 0);
            assert_eq!(outcomes.len(), 3);
            assert!(
                matches!(outcomes[0], ApplyOutcome::Put { created: true, .. }),
                "the first entry for a fresh key always creates it"
            );
            // A pre-folding gate that (wrongly) fired here would report
            // `Rejected` for every index a run absorbed; `LwwResolver`'s
            // `merges()` is `false`, so every entry instead applies
            // individually, exactly as before pre-fold existed.
            assert!(
                matches!(outcomes[1], ApplyOutcome::Put { created: false, .. }),
                "a non-merging resolver applies every entry on its own, never folded away"
            );
            assert!(matches!(
                outcomes[2],
                ApplyOutcome::Put { created: false, .. }
            ));
            assert_eq!(engine.get(&k, 0), Some("c".to_string()));
        }

        #[test]
        fn prefold_never_folds_a_run_across_a_tombstone() {
            let resolver = UnionSetResolver;
            let k = 1u32;
            let kb = key_bytes(k);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let build = || -> Vec<SetEntry> {
                vec![
                    put_entry(k, hlc(1, 1), "a"),
                    put_entry(k, hlc(2, 1), "b"),
                    (hash, k, kb.clone(), hlc(3, 1), Incoming::Tombstone),
                    put_entry(k, hlc(4, 1), "c"),
                ]
            };

            let prefolded: SetEngine = Engine::new(u64::MAX, None, None, None);
            let outcomes = prefolded.apply_many(bucket, build(), &resolver, 60_000, 600_000, 0);
            assert_eq!(outcomes.len(), 4);
            assert!(
                matches!(outcomes[0], ApplyOutcome::Rejected),
                "folded into the pre-tombstone run's survivor at index 1"
            );
            assert!(
                matches!(outcomes[1], ApplyOutcome::Put { created: true, .. }),
                "the pre-tombstone run's survivor: the union of the first two puts"
            );
            assert!(matches!(outcomes[2], ApplyOutcome::Tombstoned { .. }));
            assert!(
                matches!(outcomes[3], ApplyOutcome::Put { created: true, .. }),
                "a fresh key again after the tombstone, never folded with anything before it"
            );
            assert_eq!(
                prefolded.get(&k, 0),
                Some(string_set(&["c"])),
                "the tombstone must cut the fold: the post-tombstone put never sees \"a\"/\"b\""
            );

            let sequential: SetEngine = Engine::new(u64::MAX, None, None, None);
            sequential.set_prefold_enabled(false);
            let sequential_outcomes =
                sequential.apply_many(bucket, build(), &resolver, 60_000, 600_000, 0);
            assert!(matches!(
                sequential_outcomes[0],
                ApplyOutcome::Put { created: true, .. }
            ));
            assert!(matches!(
                sequential_outcomes[1],
                ApplyOutcome::Put { created: false, .. }
            ));
            assert!(matches!(
                sequential_outcomes[2],
                ApplyOutcome::Tombstoned { .. }
            ));
            assert!(matches!(
                sequential_outcomes[3],
                ApplyOutcome::Put { created: true, .. }
            ));

            assert_eq!(
                prefolded.record_for(kb.as_ref(), 0),
                sequential.record_for(kb.as_ref(), 0),
                "pre-fold changes only which position carries the real outcome, \
                 never the final stored record"
            );
        }

        #[test]
        fn set_prefold_enabled_toggles_the_flag_the_getter_reports() {
            let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
            assert!(
                engine.prefold_enabled(),
                "pre-fold defaults to on, matching apply_many's own default"
            );

            engine.set_prefold_enabled(false);
            assert!(!engine.prefold_enabled());

            engine.set_prefold_enabled(true);
            assert!(engine.prefold_enabled());
        }
    }
}

/// Kani proofs over the engine's pure arithmetic: every input, not a sample.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Every deadline encodes without panicking and reads back as itself:
    /// `None` as never, a deadline inside the inline range as an exact
    /// delta, anything else through the side table.
    #[kani::proof]
    fn expiry_encodes_and_decodes_every_deadline() {
        let wall_ms: u64 = kani::any();
        let expires_at_ms: Option<u64> = kani::any();
        let (expiry, side) = encode_expiry(wall_ms, expires_at_ms);
        match expires_at_ms {
            None => {
                assert_eq!(expiry, NEVER_EXPIRES);
                assert!(side.is_none());
            }
            Some(exp) if exp >= wall_ms && exp - wall_ms <= MAX_INLINE_TTL_MS => {
                assert_ne!(expiry, NEVER_EXPIRES);
                assert_ne!(expiry, LONG_TTL);
                assert!(side.is_none());
                assert_eq!(wall_ms + u64::from(expiry), exp);
            }
            Some(exp) => {
                assert_eq!(expiry, LONG_TTL);
                assert_eq!(side, Some(exp));
            }
        }
    }

    /// The u32 touch stamp recovers any idle gap up to the clamped
    /// time-to-idle ceiling, across a rollover of the stamp.
    #[kani::proof]
    fn idle_elapsed_recovers_any_gap_under_the_stamp_span() {
        let earlier: u64 = kani::any();
        let now: u64 = kani::any();
        kani::assume(now >= earlier);
        kani::assume(now - earlier <= MAX_INLINE_TTL_MS);
        assert_eq!(idle_elapsed_ms(now, touch_stamp(earlier)), now - earlier);
    }

    /// A clamped time-to-idle never exceeds the stamp's span or the value asked for.
    #[kani::proof]
    fn clamped_tti_stays_within_the_stamp_span() {
        let tti_ms: u64 = kani::any();
        let clamped = clamp_tti_ms(tti_ms);
        assert!(clamped <= MAX_INLINE_TTL_MS);
        assert!(clamped <= tti_ms);
    }

    /// Any hash lands inside the bucket table, the part table and the flat digest table.
    #[kani::proof]
    fn hash_indexes_stay_within_the_bucket_and_part_tables() {
        let hash: u64 = kani::any();
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        assert!(bucket < BUCKET_COUNT);
        assert!(part < PART_COUNT);
        assert!(digest_slot(bucket, part) < BUCKET_COUNT * PART_COUNT);
    }

    /// A stripe shrinks only at or under an eighth of its allocation, never
    /// with nothing allocated and never while holding more than it has room for.
    #[kani::proof]
    fn shrink_fires_only_at_an_eighth_of_the_allocation() {
        let len: usize = kani::any();
        let capacity: usize = kani::any();
        let shrink = should_shrink_stripe(len, capacity);
        if shrink {
            assert!(capacity > 0);
            assert!(len.checked_mul(8).is_some_and(|eight| eight <= capacity));
        }
        if capacity == 0 || len > capacity / 8 {
            assert!(!shrink);
        }
    }
}
