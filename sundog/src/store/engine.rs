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
//! `EVICTION_BATCH_SAMPLE` entries from a rotating offset, and evicts up to
//! `EVICTION_BATCH` of the coldest under that one lock hold, until total
//! weight fits. [`Engine::live_entry_count`] is a counter every insert and
//! remove path maintains.
//!
//! [`super::Shard::get_or_load`] collapses concurrent misses through a
//! per-stripe map of in-flight loads. A waiter subscribes to the load's
//! completion channel under the stripe lock, so a completion cannot slip
//! between its lookup and its wait. `InflightGuard` frees a cancelled load so a
//! waiter takes over.
//!
//! # The optional spill tier
//!
//! A live entry's `payload` is `Payload::Resident`, the value in RAM, or,
//! only under `feature = "spill"`, `Payload::Spilled`, a pointer into a
//! [`super::spill::SpillTier`]'s region log with the value on disk.
//! Weight, `ver`, and `expires_at_ms` are common `Live` fields regardless
//! of `payload`'s variant, so eviction, expiry, and the digest never need
//! to know which one a key is in: a spilled entry's weight is always `0`,
//! and `entry_fingerprint` never reads the value. `Engine::evict_one_sampled`
//! and `Engine::evict_batch_sampled` hand a `Payload::Resident` victim to a
//! configured tier's `try_spill` instead of deleting it. Once the tier
//! accepts the job, the victim's weight is zeroed in place and freed from
//! `total_weight` immediately, the same instant a physical removal would
//! free it, while the entry itself stays in `live`, `live_count`, and the
//! digest, `Resident` at weight `0`, until the tier's flusher, `Engine`'s
//! [`super::spill::SpillSink`] impl, installs it and flips its payload to
//! `Spilled`, or a failed write hands that weight back through
//! [`super::spill::SpillSink::abandon`]. A `Resident` entry at weight `0`
//! is a hand-off already in flight and is never sampled as a victim again;
//! nor is a `Payload::Spilled` one. If the record can never fit any region,
//! or the tier is closed, the ordinary delete-and-XOR path runs instead,
//! weight and all, exactly as without a tier — decided under the stripe
//! lock, before hand-off, via `SpillTier::would_accept`. A queue with no
//! room right now is different: that can only be discovered by actually
//! trying to send, so `Engine::evict_victim_locked` commits to the
//! hand-off first, and the actual channel send, `SpillTier::enqueue`, runs
//! only once the stripe lock is released, in
//! `Engine::finish_spill_handoff`. A full queue found there is handled
//! exactly like a downstream failed write: `SpillSink::abandon` restores
//! the weight, the entry stays resident, never a physical removal.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
#[cfg(all(feature = "spill", test))]
use std::sync::atomic::AtomicI64;
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

#[cfg(feature = "spill")]
use super::spill::{SpillJob, SpillLoc, SpillSink, SpillTier, spilled_is_current};
use super::{
    BUCKET_COUNT, BucketEntries, ConflictResolver, Incoming, PART_COUNT, PartEntries, RecordView,
    Tombstone, Weigher, Winner, entry_fingerprint,
};

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

/// One live entry: its version and expiry, its payload, its weight for
/// capacity accounting, and the last time it was read. The payload lives
/// in RAM, or, under `feature = "spill"`, on disk. `ver`/`expires_at_ms`
/// sit on `Live` itself rather than inside `payload`, since every reader
/// that never touches the value, eviction, expiry, the digest,
/// anti-entropy's listings, needs only these two fields, whichever
/// `payload` variant is current. The read timestamp is written only when
/// TTI or a finite capacity is configured.
struct Live<K, V> {
    key_bytes: Bytes,
    key: K,
    ver: Hlc,
    expires_at_ms: Option<u64>,
    payload: Payload<V>,
    weight: u32,
    last_access_ms: AtomicU64,
}

/// A live entry's value: in RAM, or, only under `feature = "spill"`, a
/// pointer into a [`super::spill::SpillTier`]'s region log. `weight` on
/// [`Live`] is always `0` while `Spilled`. A non-`spill` build never
/// compiles the `Spilled` arm, so `Payload<V>` collapses to a plain
/// one-variant wrapper around `Resident`'s two fields, with no layout or
/// cost overhead over storing those two fields directly.
enum Payload<V> {
    Resident {
        /// The current value.
        value: V,
        /// `value`'s postcard-encoded bytes. On the local-origin path,
        /// `insert`/`insert_many`/`get_or_load`'s fill, these are the
        /// first encode's bytes; on the replica-apply path,
        /// `apply_remote_batch`, they are the verbatim wire bytes. Always
        /// equal to `postcard::to_stdvec(&value)`, or to wire bytes
        /// decoding to a structurally equal `value`.
        encoded: Bytes,
    },
    /// The value lives on disk at this location; nothing here is in RAM.
    #[cfg(feature = "spill")]
    Spilled(SpillLoc),
}

/// Whether `live`'s payload is currently in RAM: eviction and spill
/// candidacy hinge on this, since a [`Payload::Spilled`] entry is never
/// sampled as a victim. It holds nothing to spill, and physically
/// deleting it is region reclaim's job alone, not sampled LRU's.
fn is_resident<K, V>(live: &Live<K, V>) -> bool {
    matches!(live.payload, Payload::Resident { .. })
}

/// Whether `live` is eligible to be sampled as an eviction victim:
/// [`is_resident`] and its weight has not already been zeroed by an
/// earlier hand-off to a spill tier. A `Resident` entry at weight `0` is a
/// spill already in flight, still awaiting the flusher's `install`, and
/// must never be picked a second time while it is pending. Pure; unit
/// tested directly.
fn is_spill_candidate<K, V>(live: &Live<K, V>) -> bool {
    is_resident(live) && live.weight > 0
}

/// Whether `live`'s payload is currently spilled: the mirror of
/// [`is_resident`], used only to decide whether removing this entry from
/// `live` must also decrement `sundog_spill_entries{cache}`. Always `false`
/// in a non-`spill` build, which never compiles the `Spilled` arm.
fn is_spilled<K, V>(live: &Live<K, V>) -> bool {
    #[cfg(feature = "spill")]
    {
        matches!(live.payload, Payload::Spilled(_))
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

/// What [`remove_live`] reports about the entry it took out of `live`:
/// its weight, already `0` for a [`Payload::Spilled`] entry, its
/// version, and whether it was spilled. Every caller that discards a
/// live entry needs `was_spilled` to keep `sundog_spill_entries{cache}`
/// from drifting, via [`Engine::note_spill_departure`] or
/// [`Engine::note_spill_departures`].
struct RemovedLive {
    weight: u32,
    ver: Hlc,
    was_spilled: bool,
}

/// Removes the live entry at `key_bytes`, hashing to `hash`.
fn remove_live<K, V>(
    table: &mut HashTable<Live<K, V>>,
    hash: u64,
    key_bytes: &[u8],
) -> Option<RemovedLive> {
    match table.entry(hash, |l| l.key_bytes.as_ref() == key_bytes, hasher_for) {
        Entry::Occupied(occ) => {
            let (removed, _vacant) = occ.remove();
            Some(RemovedLive {
                weight: removed.weight,
                ver: removed.ver,
                was_spilled: is_spilled(&removed),
            })
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
    if let Some(exp) = live.expires_at_ms
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
    /// tier; `0` when the stripe this pass sampled held no victim, for
    /// example because it was empty.
    removed_weight: u64,
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
    /// released — see [`Engine::try_spill_victim`]. `weight` is what the
    /// entry carried right before hand-off, already zeroed on the entry
    /// itself, so it is the caller's to fold into `total_weight` exactly
    /// like `Removed`'s — only `live_count` differs between the two. The
    /// caller finishes the hand-off with [`Engine::finish_spill_handoff`]
    /// once the lock is dropped: on a full queue that restores the weight
    /// through [`SpillSink::abandon`] exactly as a downstream write or
    /// install failure would, never a physical removal. Only ever
    /// constructed under `feature = "spill"`, the only build where anything
    /// can be handed off in the first place.
    #[cfg(feature = "spill")]
    PendingSpill(u32, SpillJob),
    /// Vanished between sampling and this call, a race with another
    /// writer on the same stripe; nothing to do.
    Vanished,
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
/// `digest_bucket`, `total_weight`, and `live_count` to match. Fully
/// synchronous: the caller holds `stripe`'s write lock for this call's
/// entire duration. The returned `bool` is whether this call displaced a
/// [`Payload::Spilled`] entry from `live`. It is `false` for a
/// `Rejected` outcome, which changes nothing. The caller uses it to keep
/// `sundog_spill_entries{cache}` correct; see
/// [`Engine::note_spill_departure`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_locked<K, V>(
    stripe: &mut Stripe<K, V>,
    digest_bucket: &AtomicU64,
    total_weight: &AtomicU64,
    live_count: &AtomicU64,
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
) -> (ApplyOutcome<K, V>, bool)
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
                // A currently-spilled entry has no value bytes to offer a
                // resolver; it gets the same degraded view a tombstone
                // already gets: a value-aware `ConflictResolver` sees
                // `stored_view.value == None`.
                let encoded = match &l.payload {
                    Payload::Resident { encoded, .. } => Some(encoded.clone()),
                    #[cfg(feature = "spill")]
                    Payload::Spilled(_) => None,
                };
                (
                    l.ver,
                    encoded,
                    l.expires_at_ms,
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
        let stored_encoded = stored_live
            .as_ref()
            .and_then(|(_, enc, _, _)| enc.as_deref());
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
            return (ApplyOutcome::Rejected, false);
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
            live_count,
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
            live_count,
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
/// value, corrects total weight for whatever it displaced (`had_live`), bumps
/// `live_count` iff nothing physically live occupied the key before, and
/// reports `created` unless a readable entry (`was_visible`) was replaced.
/// The returned `bool` is whether the displaced entry, if any, was
/// [`Payload::Spilled`]. An overwrite of a spilled key always installs
/// fresh as resident, so this is the only place such an entry departs
/// without a matching promotion.
#[allow(clippy::too_many_arguments)]
fn apply_put<K, V>(
    stripe: &mut Stripe<K, V>,
    total_weight: &AtomicU64,
    live_count: &AtomicU64,
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
) -> (ApplyOutcome<K, V>, bool)
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    let weight = weigher.map_or(1, |w| w(&key, &value));
    let removed = if had_live {
        remove_live(&mut stripe.live, hash, key_bytes.as_ref())
    } else {
        None
    };
    let displaced_spilled = removed.as_ref().is_some_and(|r| r.was_spilled);
    let old_weight = removed.map(|r| r.weight);
    // A fresh write always installs resident: spilling only ever happens
    // through sampled eviction, never directly on a write.
    stripe.live.insert_unique(
        hash,
        Live {
            key_bytes,
            key: key.clone(),
            ver,
            expires_at_ms,
            payload: Payload::Resident {
                value: value.clone(),
                encoded,
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
    } else {
        live_count.fetch_add(1, Ordering::Relaxed);
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
/// displaced entry, if any, was [`Payload::Spilled`].
#[allow(clippy::too_many_arguments)]
fn apply_tombstone<K, V>(
    stripe: &mut Stripe<K, V>,
    total_weight: &AtomicU64,
    live_count: &AtomicU64,
    hash: u64,
    key: K,
    key_bytes: Bytes,
    ver: Hlc,
    had_live: bool,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    now_ms: u64,
) -> (ApplyOutcome<K, V>, bool)
where
    K: Hash + Eq,
{
    let mut displaced_spilled = false;
    if had_live && let Some(removed) = remove_live(&mut stripe.live, hash, key_bytes.as_ref()) {
        total_weight.fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
        live_count.fetch_sub(1, Ordering::Relaxed);
        displaced_spilled = removed.was_spilled;
    }
    stripe.tombstones.insert(
        key_bytes,
        Tombstone {
            ver,
            ttl_deadline_ms: now_ms.saturating_add(tombstone_ttl_ms),
            max_deadline_ms: now_ms.saturating_add(tombstone_max_ttl_ms),
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
    /// [`Payload::Spilled`] entry from `live`.
    #[cfg(feature = "spill")]
    spill_entries_gauge: OnceLock<metrics::Gauge>,
    /// Test-only mirror of `spill_entries_gauge`'s value, updated in
    /// lockstep everywhere the gauge is. The `metrics` crate's default
    /// recorder is a silent no-op with nothing for a unit test to read
    /// back, so this pub(crate) counter gives engine tests something to
    /// assert against without installing a real Prometheus recorder.
    #[cfg(all(feature = "spill", test))]
    spill_entries_test_count: AtomicI64,
    #[cfg(test)]
    eviction_lock_acquisitions: AtomicU64,
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
            live_count: AtomicU64::new(0),
            max_capacity,
            tti_ms: tti.map(super::duration_ms),
            weigher,
            // Any nonzero seed; xorshift64* never recovers from a zero state.
            evict_cursor: AtomicU64::new(0x9E37_79B9_7F4A_7C15),
            #[cfg(feature = "spill")]
            spill: OnceLock::new(),
            #[cfg(feature = "spill")]
            spill_entries_gauge: OnceLock::new(),
            #[cfg(all(feature = "spill", test))]
            spill_entries_test_count: AtomicI64::new(0),
            #[cfg(test)]
            eviction_lock_acquisitions: AtomicU64::new(0),
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
    /// takes a [`Payload::Spilled`] entry out of `live` calls this, or
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
    /// wherever a `live` entry newly becomes [`Payload::Spilled`]: from
    /// `SpillSink::install`, and from the test-only
    /// [`Engine::debug_insert_spilled`].
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
    ///
    /// `None` for a currently-[`Payload::Spilled`] entry too. A spill-aware
    /// caller, `get`/`get_or_load`, checks [`Engine::spilled_loc`] next; a
    /// spill-blind one, `get_sync`, treats this like a miss, per its
    /// documented contract.
    pub(crate) fn get_by_bytes(&self, key_bytes: &[u8], hash: u64, now_ms: u64) -> Option<V> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes)?;
        if self.is_absent(live, now_ms) {
            return None;
        }
        match &live.payload {
            Payload::Resident { value, .. } => {
                self.touch(live, now_ms);
                Some(value.clone())
            }
            #[cfg(feature = "spill")]
            Payload::Spilled(_) => None,
        }
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

    /// [`Engine::keys`] one stripe at a time: each stripe's live keys are
    /// cloned under its read lock, then `f` runs on them with no lock held.
    pub(crate) fn for_each_key(&self, now_ms: u64, mut f: impl FnMut(K)) {
        for stripe_lock in &self.stripes {
            let stripe_keys: Vec<K> = {
                let stripe = stripe_lock.read();
                stripe
                    .live
                    .iter()
                    .filter(|live| !self.is_absent(live, now_ms))
                    .map(|live| live.key.clone())
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
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes)?;
        if self.is_absent(live, now_ms) {
            return None;
        }
        match &live.payload {
            Payload::Resident { encoded, .. } => Some(WireRecord {
                key: Bytes::copy_from_slice(key_bytes),
                value: Some(encoded.clone()),
                ver: live.ver,
                expires_at_ms: live.expires_at_ms,
            }),
            #[cfg(feature = "spill")]
            Payload::Spilled(_) => None,
        }
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
                        .map(|live| (live.key_bytes.clone(), live.ver)),
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
        for (bucket, mut parts) in by_bucket {
            parts.sort_unstable();
            parts.dedup();
            // One pass over the stripe, hashing each key once, routing every
            // entry to its part's slot; a bucket's 64 parts cost one listing.
            let mut slot_of_part = [usize::MAX; PART_COUNT];
            for (slot, &part) in parts.iter().enumerate() {
                slot_of_part[usize::from(part)] = slot;
            }
            let mut per_part: Vec<Vec<(Bytes, Hlc)>> = vec![Vec::new(); parts.len()];
            let stripe = self.stripes[usize::from(bucket)].read();
            let live_entries = stripe
                .live
                .iter()
                .filter(|live| !self.is_absent(live, now_ms))
                .map(|live| (&live.key_bytes, live.ver));
            let tombstone_entries = stripe
                .tombstones
                .iter()
                .map(|(key_bytes, t)| (key_bytes, t.ver));
            for (key_bytes, ver) in live_entries.chain(tombstone_entries) {
                let slot = slot_of_part[part_index_from_hash(hash_key_bytes(key_bytes.as_ref()))];
                if slot != usize::MAX {
                    per_part[slot].push((key_bytes.clone(), ver));
                }
            }
            drop(stripe);
            out.extend(
                parts
                    .into_iter()
                    .zip(per_part)
                    .map(|(part, entries)| ((bucket, part), entries)),
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
    /// Non-`spill` builds never compile a `Payload::Spilled` arm, so the
    /// `filter_map` below degenerates to an infallible `map`; the allow
    /// below is scoped to that configuration.
    #[cfg_attr(not(feature = "spill"), allow(clippy::unnecessary_filter_map))]
    pub(crate) fn snapshot_records(&self, now_ms: u64) -> Vec<WireRecord> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| !self.is_absent(live, now_ms))
                    .filter_map(|live| match &live.payload {
                        Payload::Resident { encoded, .. } => Some(WireRecord {
                            key: live.key_bytes.clone(),
                            value: Some(encoded.clone()),
                            ver: live.ver,
                            expires_at_ms: live.expires_at_ms,
                        }),
                        #[cfg(feature = "spill")]
                        Payload::Spilled(_) => None,
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
                    .filter(|live| !self.is_absent(live, now_ms))
                    .filter_map(|live| match &live.payload {
                        Payload::Spilled(loc) => {
                            Some((live.key_bytes.clone(), live.ver, live.expires_at_ms, *loc))
                        }
                        Payload::Resident { .. } => None,
                    }),
            );
        }
        out
    }

    /// [`Engine::record_for`] for many keys in one pass, but reporting a
    /// currently-spilled entry's pointer instead of treating it as absent.
    /// The AE-pull-reply path, `ShardOps::records_for`, reads the spilled
    /// half off-lock, via `spawn_blocking` behind the tier's read
    /// semaphore, and folds any successful read back in as a `WireRecord`,
    /// dropping the rest. Nothing here promotes; a served-from-disk record
    /// leaves `payload` as it was.
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
                .find(hash, |l| l.key_bytes.as_ref() == key_bytes.as_ref())
            else {
                continue;
            };
            if self.is_absent(live, now_ms) {
                continue;
            }
            match &live.payload {
                Payload::Resident { encoded, .. } => records.push(WireRecord {
                    key: key_bytes.clone(),
                    value: Some(encoded.clone()),
                    ver: live.ver,
                    expires_at_ms: live.expires_at_ms,
                }),
                Payload::Spilled(loc) => {
                    spilled.push((key_bytes.clone(), live.ver, live.expires_at_ms, *loc));
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
            let mut stripe = stripe_lock.write();
            let mut removed_weight = 0u64;
            let mut removed_count = 0u64;
            let mut removed_spilled = 0usize;
            let mut new_next = u64::MAX;
            stripe.live.retain(|live| {
                if self.is_absent(live, now_ms) {
                    let part = part_index_from_hash(hash_key_bytes(live.key_bytes.as_ref()));
                    self.digest[digest_slot(idx, part)].fetch_xor(
                        entry_fingerprint(&live.key_bytes, live.ver),
                        Ordering::Relaxed,
                    );
                    removed_weight += u64::from(live.weight);
                    removed_count += 1;
                    if is_spilled(live) {
                        removed_spilled += 1;
                    }
                    false
                } else {
                    if let Some(exp) = live.expires_at_ms {
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
            if removed_count > 0 {
                self.live_count.fetch_sub(removed_count, Ordering::Relaxed);
            }
            self.note_spill_departures(removed_spilled);
        }
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
    /// place of physically removing it. `None` with no tier configured, a
    /// victim that has since stopped being [`Payload::Resident`], or a
    /// record [`SpillTier::would_accept`] declines outright, too large to
    /// ever fit a region or the tier closed; the ordinary remove-and-XOR
    /// path runs in every one of those cases, exactly as before. On
    /// `Some((weight, job))`, `weight` is the victim's weight *before* this
    /// call, already zeroed on the entry in place and thus already excluded
    /// from what a fresh read of `live.weight` would report, and `job` is
    /// the caller's to hand to [`Engine::finish_spill_handoff`] once the
    /// stripe lock is released.
    ///
    /// Deliberately does not call [`SpillTier::enqueue`] itself: that is
    /// the one part of a hand-off that touches the flusher's channel, worth
    /// keeping off this lock, and it is safe to defer because
    /// `SpillTier::would_accept`'s checks — too large, closed — are the
    /// only ways this decision could otherwise need to unwind, and both are
    /// already settled here, under the lock, before the weight is zeroed. A
    /// full queue, the only way `enqueue` can still fail, is handled
    /// exactly like a downstream write or install failure already is:
    /// [`SpillSink::abandon`] restores the weight, never a physical
    /// removal — seeing `would_accept` succeed here is not a guarantee the
    /// record ever reaches disk, only that it is now this victim's only
    /// path off `live`.
    #[cfg(feature = "spill")]
    fn try_spill_victim(
        &self,
        stripe: &mut Stripe<K, V>,
        bucket: usize,
        hash: u64,
        victim_bytes: &Bytes,
    ) -> Option<(u32, SpillJob)> {
        let tier = self.spill()?;
        let live = stripe
            .live
            .find_mut(hash, |l| l.key_bytes.as_ref() == victim_bytes.as_ref())?;
        let Payload::Resident { encoded, .. } = &live.payload else {
            return None;
        };
        if !tier.would_accept(victim_bytes.len(), encoded.len()) {
            return None;
        }
        let job = SpillJob {
            stripe_idx: bucket,
            hash,
            key_bytes: victim_bytes.clone(),
            ver: live.ver,
            expires_at_ms: live.expires_at_ms,
            encoded: encoded.clone(),
        };
        let weight = live.weight;
        live.weight = 0;
        Some((weight, job))
    }

    /// Finishes a hand-off [`Engine::evict_victim_locked`] committed to via
    /// [`Engine::try_spill_victim`], after the stripe lock that decided it
    /// has already been released: the one part of the hand-off that touches
    /// the flusher's channel, [`SpillTier::enqueue`]. A full queue gets
    /// `job` back, key bytes included, with nothing cloned to recover them;
    /// [`SpillSink::abandon`] then restores the victim's weight exactly as
    /// it would for a write or install failure discovered later, downstream
    /// in the flusher itself. No tier configured is unreachable here, since
    /// `try_spill_victim` never commits to a hand-off without one, but is
    /// still handled rather than assumed.
    #[cfg(feature = "spill")]
    fn finish_spill_handoff(&self, job: SpillJob) {
        let Some(tier) = self.spill() else { return };
        let stripe_idx = job.stripe_idx;
        let hash = job.hash;
        let ver = job.ver;
        if let Err(job) = tier.enqueue(job) {
            self.abandon(stripe_idx, &job.key_bytes, hash, ver);
        }
    }

    /// Removes or spills `victim_bytes`, found at `hash` in `bucket` with
    /// `stripe` already write-locked. Hands a [`Payload::Resident`] victim
    /// to a configured spill tier when [`try_spill_victim`] commits to it:
    /// weight zeroed in place right there, so this reports it as
    /// [`VictimOutcome::PendingSpill`] with that freed weight and the job
    /// still to enqueue, while `stripe.live`, the digest, and `live_count`
    /// stay untouched until the flusher, `Engine`'s [`SpillSink`] impl,
    /// installs it. The entry stays resident, at weight `0`, until then.
    /// Otherwise runs the ordinary remove-and-XOR path,
    /// [`VictimOutcome::Removed`]. A [`Payload::Spilled`] victim, or a
    /// `Resident` one already at weight `0`, a hand-off already pending, is
    /// never handed here: the sampling passes above filter both out via
    /// [`is_spill_candidate`]. A race where the entry vanished between
    /// sampling and this call reports [`VictimOutcome::Vanished`].
    ///
    /// Total weight and `live_count` are the caller's job: this only
    /// mutates `stripe.live` and the digest, so a batch caller can fold
    /// several victims' weight into one pair of atomic updates after the
    /// loop. The caller is likewise responsible for calling
    /// [`Engine::finish_spill_handoff`] on a [`VictimOutcome::PendingSpill`]
    /// job, once it has dropped `stripe`.
    ///
    /// [`try_spill_victim`]: Engine::try_spill_victim
    fn evict_victim_locked(
        &self,
        stripe: &mut Stripe<K, V>,
        bucket: usize,
        victim_bytes: &Bytes,
    ) -> VictimOutcome {
        let hash = hash_key_bytes(victim_bytes.as_ref());
        #[cfg(feature = "spill")]
        if let Some((weight, job)) = self.try_spill_victim(stripe, bucket, hash, victim_bytes) {
            return VictimOutcome::PendingSpill(weight, job);
        }
        let Entry::Occupied(occ) = stripe.live.entry(
            hash,
            |l| l.key_bytes.as_ref() == victim_bytes.as_ref(),
            hasher_for,
        ) else {
            return VictimOutcome::Vanished;
        };
        let (removed, _vacant) = occ.remove();
        let part = part_index_from_hash(hash);
        self.digest[digest_slot(bucket, part)].fetch_xor(
            entry_fingerprint(&removed.key_bytes, removed.ver),
            Ordering::Relaxed,
        );
        VictimOutcome::Removed(removed.weight)
    }

    /// Evicts the coldest of up to [`EVICTION_SAMPLE`] resident entries in
    /// `bucket`. Returns what happened: nothing to evict, a physical
    /// removal, or a hand-off to the spill tier.
    fn evict_one_sampled(&self, bucket: usize) -> EvictOutcome {
        self.note_eviction_lock_acquisition();
        let mut stripe = self.stripes[bucket].write();
        let offset = self.sample_offset(stripe.live.len());
        let Some(victim_bytes) = stripe
            .live
            .iter()
            .skip(offset)
            .chain(stripe.live.iter().take(offset))
            .take(EVICTION_SAMPLE)
            .filter(|live| is_spill_candidate(live))
            .min_by_key(|live| live.last_access_ms.load(Ordering::Relaxed))
            .map(|live| live.key_bytes.clone())
        else {
            return EvictOutcome::default();
        };
        let outcome = self.evict_victim_locked(&mut stripe, bucket, &victim_bytes);
        drop(stripe);
        match outcome {
            VictimOutcome::Removed(weight) => {
                self.total_weight
                    .fetch_sub(u64::from(weight), Ordering::Relaxed);
                self.live_count.fetch_sub(1, Ordering::Relaxed);
                EvictOutcome {
                    removed_weight: u64::from(weight),
                }
            }
            #[cfg(feature = "spill")]
            VictimOutcome::PendingSpill(weight, job) => {
                self.total_weight
                    .fetch_sub(u64::from(weight), Ordering::Relaxed);
                self.finish_spill_handoff(job);
                EvictOutcome {
                    removed_weight: u64::from(weight),
                }
            }
            VictimOutcome::Vanished => EvictOutcome::default(),
        }
    }

    /// Evicts one entry from the first non-empty stripe at or after `bucket`,
    /// wrapping around once. Returns `None` only when every stripe was found
    /// to hold nothing to evict.
    fn evict_one_scanning(&self, bucket: usize) -> Option<EvictOutcome> {
        (0..BUCKET_COUNT)
            .map(|step| (bucket + step) % BUCKET_COUNT)
            .find_map(|candidate| {
                let outcome = self.evict_one_sampled(candidate);
                (!outcome.made_no_progress()).then_some(outcome)
            })
    }

    /// Evicts the coldest of up to [`EVICTION_BATCH_SAMPLE`] resident
    /// entries in `bucket` under one lock hold, as many as
    /// [`eviction_batch_size`] allows for `over_by`. Each victim is removed
    /// or, with a spill tier configured and room in its queue, handed off
    /// instead; see [`Engine::evict_victim_locked`].
    fn evict_batch_sampled(&self, bucket: usize, over_by: u64) -> EvictOutcome {
        self.note_eviction_lock_acquisition();
        let mut stripe = self.stripes[bucket].write();
        let offset = self.sample_offset(stripe.live.len());
        let mut sampled: Vec<(Bytes, u64, u32)> = stripe
            .live
            .iter()
            .skip(offset)
            .chain(stripe.live.iter().take(offset))
            .take(EVICTION_BATCH_SAMPLE)
            .filter(|live| is_spill_candidate(live))
            .map(|live| {
                (
                    live.key_bytes.clone(),
                    live.last_access_ms.load(Ordering::Relaxed),
                    live.weight,
                )
            })
            .collect();
        if sampled.is_empty() {
            return EvictOutcome::default();
        }
        sampled.sort_unstable_by_key(|&(_, last_access, _)| last_access);
        let weights: Vec<u32> = sampled.iter().map(|&(_, _, w)| w).collect();
        let victims = eviction_batch_size(over_by, &weights);

        let mut removed_weight = 0u64;
        let mut removed_count = 0u64;
        #[cfg(feature = "spill")]
        let mut pending_spills: Vec<SpillJob> = Vec::new();
        for (key_bytes, _, _) in sampled.into_iter().take(victims) {
            match self.evict_victim_locked(&mut stripe, bucket, &key_bytes) {
                VictimOutcome::Removed(weight) => {
                    removed_weight += u64::from(weight);
                    removed_count += 1;
                }
                #[cfg(feature = "spill")]
                VictimOutcome::PendingSpill(weight, job) => {
                    removed_weight += u64::from(weight);
                    pending_spills.push(job);
                }
                VictimOutcome::Vanished => {}
            }
        }
        drop(stripe);
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
        EvictOutcome { removed_weight }
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
    /// A spill hand-off frees its victim's weight from `total_weight` at
    /// the same instant a physical removal would, so this loop's own
    /// `total_weight` re-check at the top of every iteration already sees
    /// a spilling pass's progress; with a spill tier configured, this
    /// behaves exactly as it does without one.
    pub(crate) fn enforce_capacity(&self, start_bucket: usize) {
        if self.max_capacity == u64::MAX {
            return;
        }
        let mut bucket = start_bucket;
        loop {
            let current = self.total_weight.load(Ordering::Relaxed);
            if current <= self.max_capacity {
                return;
            }
            let over_by = current - self.max_capacity;
            let batch_outcome = self.evict_batch_sampled(bucket, over_by);
            if batch_outcome.made_no_progress() && self.evict_one_scanning(bucket).is_none() {
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
                let (outcome, displaced_spilled) = apply_locked(
                    &mut stripe,
                    digest_bucket,
                    &self.total_weight,
                    &self.live_count,
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
                self.note_spill_departure(displaced_spilled);
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
                .map(|l| l.ver),
        };
        if stored_ver.is_some_and(|sv| ver <= sv) {
            return None;
        }
        let had_live = prior_tombstone.is_none() && stored_ver.is_some();
        if !had_live {
            return None;
        }
        let removed = remove_live(&mut stripe.live, hash, key_bytes)?;
        drop(stripe);
        let part = part_index_from_hash(hash);
        self.digest[digest_slot(bucket, part)]
            .fetch_xor(entry_fingerprint(key_bytes, removed.ver), Ordering::Relaxed);
        self.total_weight
            .fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
        self.live_count.fetch_sub(1, Ordering::Relaxed);
        self.note_spill_departure(removed.was_spilled);
        Some(removed.ver)
    }

    /// Drops the local live entry at `key_bytes` unconditionally, no version
    /// check, no tombstone, for [`super::Shard::invalidate_local`]'s
    /// cache-busting escape hatch.
    pub(crate) fn invalidate_local(&self, key_bytes: &[u8], hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some(removed) = remove_live(&mut stripe.live, hash, key_bytes) {
            drop(stripe);
            let part = part_index_from_hash(hash);
            self.digest[digest_slot(bucket, part)]
                .fetch_xor(entry_fingerprint(key_bytes, removed.ver), Ordering::Relaxed);
            self.total_weight
                .fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
            self.live_count.fetch_sub(1, Ordering::Relaxed);
            self.note_spill_departure(removed.was_spilled);
        }
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
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes.as_ref())
            && !self.is_absent(live, now_ms)
        {
            match &live.payload {
                Payload::Resident { value, .. } => {
                    let value = value.clone();
                    self.touch(live, now_ms);
                    return JoinOutcome::Hit(value);
                }
                #[cfg(feature = "spill")]
                Payload::Spilled(_) => {}
            }
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
    /// the loader's value, corrects `live_count` for the net change, and
    /// removes the `inflight` entry, all under one stripe write-lock
    /// acquisition. Returns whether a live entry already existed.
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
        let mut displaced_spilled = false;
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
            } else if let Some(removed) = remove_live(&mut stripe.live, hash, key_bytes.as_ref()) {
                had_live = true;
                digest_bucket.fetch_xor(
                    entry_fingerprint(key_bytes.as_ref(), removed.ver),
                    Ordering::Relaxed,
                );
                self.total_weight
                    .fetch_sub(u64::from(removed.weight), Ordering::Relaxed);
                displaced_spilled = removed.was_spilled;
            }
            if !had_live {
                self.live_count.fetch_add(1, Ordering::Relaxed);
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
                    ver,
                    expires_at_ms,
                    payload: Payload::Resident { value, encoded },
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
        self.note_spill_departure(displaced_spilled);
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

    /// Snapshots a currently-[`Payload::Spilled`] entry's pointer under the
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
            .find(hash, |l| l.key_bytes.as_ref() == key_bytes)?;
        if self.is_absent(live, now_ms) {
            return None;
        }
        match &live.payload {
            Payload::Spilled(loc) => {
                let loc = *loc;
                let ver = live.ver;
                self.touch(live, now_ms);
                Some((ver, loc))
            }
            Payload::Resident { .. } => None,
        }
    }

    /// Flips a currently-[`Payload::Spilled`] entry back to
    /// [`Payload::Resident`] in place, iff [`spilled_is_current`] still
    /// holds for `read_ver` against what is currently stored. A tombstone
    /// or a version change since the disk read started means the
    /// promotion is a silent no-op. The caller's read already succeeded
    /// independently of this; only the RAM reinstall is skipped. Adds the
    /// freshly weighed entry's weight back to `total_weight`. **Never
    /// touches the digest or `live_count`**: same key, same `ver`, same
    /// fingerprint, so nothing about the entry's replicated identity
    /// changes. Also decrements `sundog_spill_entries{cache}`, the mirror
    /// of `SpillSink::install`'s increment, since a resident entry is not
    /// counted among currently-spilled entries. Returns whether it
    /// promoted.
    #[cfg(feature = "spill")]
    pub(crate) fn promote_locked(
        &self,
        key_bytes: &[u8],
        hash: u64,
        read_ver: Hlc,
        value: V,
        encoded: Bytes,
    ) -> bool {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        let stored_tombstone_ver = stripe.tombstones.get(key_bytes).map(|t| t.ver);
        let Some(live) = stripe
            .live
            .find_mut(hash, |l| l.key_bytes.as_ref() == key_bytes)
        else {
            return false;
        };
        if !spilled_is_current(stored_tombstone_ver, Some(live.ver), read_ver) {
            return false;
        }
        if !matches!(live.payload, Payload::Spilled(_)) {
            // Already resident: a racing promotion, or a fresh write that
            // happens to share this version, got there first.
            return false;
        }
        let weight = self.weigher.as_ref().map_or(1, |w| w(&live.key, &value));
        live.payload = Payload::Resident { value, encoded };
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
    fn install(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        loc: SpillLoc,
    ) -> bool {
        {
            let mut stripe = self.stripes[stripe_idx].write();
            let stored_tombstone_ver = stripe.tombstones.get(key_bytes.as_ref()).map(|t| t.ver);
            let Some(live) = stripe
                .live
                .find_mut(hash, |l| l.key_bytes.as_ref() == key_bytes.as_ref())
            else {
                return false;
            };
            if !spilled_is_current(stored_tombstone_ver, Some(live.ver), ver)
                || !matches!(live.payload, Payload::Resident { .. })
            {
                return false;
            }
            // The victim's weight was already zeroed on this entry, and
            // subtracted from `total_weight`, at hand-off time in
            // `Engine::try_spill_victim`; `Spilled`'s invariant is that
            // weight is always `0`, which already holds here.
            debug_assert_eq!(
                live.weight, 0,
                "invariant: a spill candidate's weight is zeroed at hand-off"
            );
            live.payload = Payload::Spilled(loc);
        }
        self.note_spill_arrival();
        true
    }

    fn abandon(&self, stripe_idx: usize, key_bytes: &Bytes, hash: u64, ver: Hlc) {
        let weight = {
            let mut stripe = self.stripes[stripe_idx].write();
            let Some(live) = stripe
                .live
                .find_mut(hash, |l| l.key_bytes.as_ref() == key_bytes.as_ref())
            else {
                return;
            };
            if live.ver != ver || live.weight != 0 {
                // The key's stored state has already moved on: a fresh
                // write, a tombstone, or (impossible in practice, but
                // never assumed) an install that already ran. Whatever it
                // was already accounted for the weight this job would have
                // restored; leave it alone.
                return;
            }
            let weight = match &live.payload {
                Payload::Resident { value, .. } => {
                    self.weigher.as_ref().map_or(1, |w| w(&live.key, value))
                }
                Payload::Spilled(_) => return,
            };
            live.weight = weight;
            weight
        };
        self.total_weight
            .fetch_add(u64::from(weight), Ordering::Relaxed);
    }

    fn reclaim(&self, region: u32, generation: u32, keys: &[(usize, Bytes)]) -> usize {
        let mut removed = 0usize;
        for (stripe_idx, key_bytes) in keys {
            let hash = hash_key_bytes(key_bytes.as_ref());
            let bucket = *stripe_idx;
            let removed_this = {
                let mut stripe = self.stripes[bucket].write();
                let Entry::Occupied(occ) = stripe.live.entry(
                    hash,
                    |l| l.key_bytes.as_ref() == key_bytes.as_ref(),
                    hasher_for,
                ) else {
                    continue;
                };
                let still_points_here = matches!(
                    &occ.get().payload,
                    Payload::Spilled(loc) if loc.region == region && loc.generation == generation
                );
                if !still_points_here {
                    continue;
                }
                let (removed_entry, _vacant) = occ.remove();
                let part = part_index_from_hash(hash);
                self.digest[digest_slot(bucket, part)].fetch_xor(
                    entry_fingerprint(&removed_entry.key_bytes, removed_entry.ver),
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
#[allow(clippy::cast_precision_loss)]
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
            for live in &stripe.live {
                let part = part_index_from_hash(hash_key_bytes(live.key_bytes.as_ref()));
                out[digest_slot(idx, part)] ^= entry_fingerprint(&live.key_bytes, live.ver);
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
            for live in &stripe.live {
                let encoded = match &live.payload {
                    Payload::Resident { encoded, .. } => encoded.clone(),
                    #[cfg(feature = "spill")]
                    Payload::Spilled(_) => panic!(
                        "debug_snapshot: no resident bytes for a spilled entry; this helper is \
                         for tests that never configure a spill tier"
                    ),
                };
                live_out.push((live.key_bytes.clone(), encoded, live.ver));
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

    /// Test-only: inserts a live entry already pointing at `loc`, with the
    /// same digest/`live_count`/`sundog_spill_entries` bookkeeping a genuine
    /// spill install would have left behind, bypassing the normal
    /// Resident-only write path. Lets mutation tests exercise a spilled key
    /// with no real disk I/O and no dependency on a real [`SpillTier`]'s
    /// flusher thread.
    #[cfg(feature = "spill")]
    pub(crate) fn debug_insert_spilled(
        &self,
        key: K,
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
            stripe.live.insert_unique(
                hash,
                Live {
                    key_bytes: key_bytes.clone(),
                    key,
                    ver,
                    expires_at_ms,
                    payload: Payload::Spilled(loc),
                    weight: 0,
                    last_access_ms: AtomicU64::new(now_ms),
                },
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
        let (outcome, displaced_spilled) = {
            let mut stripe = engine.stripes[bucket].write();
            let resolver = LwwResolver;
            apply_locked(
                &mut stripe,
                &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
                &engine.total_weight,
                &engine.live_count,
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
        };
        // Mirrors `Engine::apply_many`'s own bookkeeping, so a test driving
        // writes through this helper sees the same `sundog_spill_entries`
        // behavior a real caller would.
        engine.note_spill_departure(displaced_spilled);
        outcome
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
                &engine.live_count,
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
                &engine.live_count,
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
                    &engine.live_count,
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

        engine.enforce_capacity(start_bucket);

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
        let engine = Engine::<u32, String>::new(40_000, None, Some(weigher));
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

        engine.enforce_capacity(0);

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
        let engine = Engine::<u32, Arc<String>>::new(u64::MAX, None, None);
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
        let engine = Engine::<u32, String>::new(5, None, Some(weigher));
        // One entry heavier than the whole cap: evicting it is all there is.
        let _ = put(&engine, 1, key_bytes(1), "z".repeat(50), hlc(1, 1), None, 0);
        engine.enforce_capacity(0);
        assert_eq!(engine.debug_totals(), (0, 0));
        assert!(
            engine.evict_one_sampled(0).made_no_progress(),
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
                        &engine.live_count,
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
                        &engine.live_count,
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
                &engine.live_count,
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

    #[test]
    fn is_resident_true_for_resident_false_for_spilled() {
        let resident = Live::<u32, String> {
            key_bytes: key_bytes(1),
            key: 1,
            ver: hlc(1, 1),
            expires_at_ms: None,
            payload: Payload::Resident {
                value: "v".to_string(),
                encoded: Bytes::from_static(b"v"),
            },
            weight: 1,
            last_access_ms: AtomicU64::new(0),
        };
        assert!(is_resident(&resident));

        #[cfg(feature = "spill")]
        {
            let spilled = Live::<u32, String> {
                key_bytes: key_bytes(1),
                key: 1,
                ver: hlc(1, 1),
                expires_at_ms: None,
                payload: Payload::Spilled(SpillLoc {
                    region: 0,
                    offset: 0,
                    len: 1,
                    generation: 0,
                }),
                weight: 0,
                last_access_ms: AtomicU64::new(0),
            };
            assert!(!is_resident(&spilled));
        }
    }

    #[test]
    fn is_spill_candidate_true_only_for_a_resident_entry_with_nonzero_weight() {
        let resident_hot = Live::<u32, String> {
            key_bytes: key_bytes(1),
            key: 1,
            ver: hlc(1, 1),
            expires_at_ms: None,
            payload: Payload::Resident {
                value: "v".to_string(),
                encoded: Bytes::from_static(b"v"),
            },
            weight: 3,
            last_access_ms: AtomicU64::new(0),
        };
        assert!(is_spill_candidate(&resident_hot));

        let resident_pending = Live::<u32, String> {
            key_bytes: key_bytes(1),
            key: 1,
            ver: hlc(1, 1),
            expires_at_ms: None,
            payload: Payload::Resident {
                value: "v".to_string(),
                encoded: Bytes::from_static(b"v"),
            },
            weight: 0,
            last_access_ms: AtomicU64::new(0),
        };
        assert!(
            !is_spill_candidate(&resident_pending),
            "weight zero means a hand-off to the spill tier is already in flight"
        );

        #[cfg(feature = "spill")]
        {
            let spilled = Live::<u32, String> {
                key_bytes: key_bytes(1),
                key: 1,
                ver: hlc(1, 1),
                expires_at_ms: None,
                payload: Payload::Spilled(SpillLoc {
                    region: 0,
                    offset: 0,
                    len: 1,
                    generation: 0,
                }),
                weight: 0,
                last_access_ms: AtomicU64::new(0),
            };
            assert!(!is_spill_candidate(&spilled));
        }
    }

    #[test]
    fn evict_outcome_made_no_progress_only_when_nothing_was_freed() {
        assert!(EvictOutcome::default().made_no_progress());
        assert!(!EvictOutcome { removed_weight: 1 }.made_no_progress());
    }

    #[test]
    fn a_pending_spill_entry_is_never_sampled_as_a_victim() {
        // A weight-0 Resident entry, exactly what a successful hand-off
        // leaves behind while the flusher's install is still in flight,
        // must never be picked a second time: not by the single-victim
        // sampler, and not by the batch one.
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher));
        let key = 1u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(&engine, key, kb.clone(), "x".repeat(5), hlc(1, 1), None, 0);
        {
            let mut stripe = engine.stripe_lock(bucket).write();
            let live = stripe
                .live
                .find_mut(hash, |l| l.key_bytes.as_ref() == kb.as_ref())
                .expect("entry is present");
            live.weight = 0;
        }

        assert!(
            engine.evict_one_sampled(bucket).made_no_progress(),
            "the only entry in this stripe is pending; single-victim sampling finds nothing"
        );
        assert!(
            engine.evict_batch_sampled(bucket, 100).made_no_progress(),
            "the only entry in this stripe is pending; batch sampling finds nothing either"
        );
        assert_eq!(
            engine.get(&key, 0),
            Some("x".repeat(5)),
            "the pending entry is untouched: still resident, still readable"
        );
    }

    #[cfg(feature = "spill")]
    mod spill_payload {
        use super::*;
        use crate::store::spill::{SpillConfig, SpillSink, SpillTier};

        fn loc(region: u32, offset: u32, len: u32, generation: u32) -> SpillLoc {
            SpillLoc {
                region,
                offset,
                len,
                generation,
            }
        }

        #[test]
        fn snapshot_spilled_reports_pointers_for_spilled_entries_only() {
            let engine = engine_u32_string(u64::MAX, None);
            let key1 = 1u32;
            let kb1 = key_bytes(key1);
            let l = loc(0, 0, 4, 0);
            engine.debug_insert_spilled(key1, &kb1, hlc(1, 1), Some(500), l, 0);
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
            engine.debug_insert_spilled(spilled_key, &kb_spilled, spilled_ver, Some(500), l, 0);

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
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);

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
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);

            match engine.miss_or_join(&kb, hash, 0) {
                JoinOutcome::Hit(_) => panic!("a spilled entry has no resident value to hit on"),
                JoinOutcome::Owner(_) | JoinOutcome::Join(..) => {}
            }
        }

        #[test]
        fn spilled_loc_snapshots_the_pointer_and_touches_last_access() {
            let engine = Engine::<u32, String>::new(10, None, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let ver = hlc(1, 1);
            let l = loc(2, 8, 4, 1);
            engine.debug_insert_spilled(key, &kb, ver, None, l, 0);

            assert_eq!(engine.spilled_loc(kb.as_ref(), hash, 100), Some((ver, l)));

            let bucket = stripe_index_from_hash(hash);
            let stripe = engine.stripe_lock(bucket).read();
            let live = stripe
                .live
                .iter()
                .find(|live| live.key_bytes.as_ref() == kb.as_ref())
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
            engine.debug_insert_spilled(key, &kb, ver, None, loc(0, 0, 10, 0), 0);

            let digest_before = engine.digests();
            let (live_count_before, weight_before) = engine.debug_totals();
            assert_eq!(weight_before, 0, "a spilled entry contributes zero weight");

            let promoted = engine.promote_locked(
                kb.as_ref(),
                hash,
                ver,
                "restored".to_string(),
                Bytes::from_static(b"restored-bytes"),
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
                "stale-read".to_string(),
                Bytes::from_static(b"stale"),
            );
            assert!(
                !promoted,
                "nothing to promote: the entry is already resident"
            );
            assert_eq!(engine.get(&key, 0), Some("already-here".to_string()));
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
                SpillSink::install(&engine, bucket, &kb, hash, old_ver, loc(0, 0, 4, 0));
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
            {
                let mut stripe = engine.stripe_lock(bucket).write();
                let resolver = LwwResolver;
                let _ = apply_locked(
                    &mut stripe,
                    &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
                    &engine.total_weight,
                    &engine.live_count,
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

            let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0));
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
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(3, 0, 10, 0), 0);

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
        fn region_reclaim_skips_a_key_that_was_promoted_since_being_recorded() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let ver = hlc(1, 1);
            engine.debug_insert_spilled(key, &kb, ver, None, loc(3, 0, 10, 0), 0);
            assert!(engine.promote_locked(
                kb.as_ref(),
                hash,
                ver,
                "restored".to_string(),
                Bytes::from_static(b"bytes"),
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
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(3, 0, 10, 0), 0);
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

        /// Puts `live.weight` at `hash`/`kb` back to `0` and subtracts
        /// `weight` from `total_weight`, exactly the state a successful
        /// hand-off to a spill tier leaves behind while the flusher's write
        /// is still in flight. Lets a test drive `SpillSink::abandon`
        /// directly, the same way this module already drives `install` and
        /// `reclaim` directly, with no real disk or flusher thread needed.
        fn simulate_pending_handoff(
            engine: &Engine<u32, String>,
            bucket: usize,
            hash: u64,
            kb: &Bytes,
            weight: u32,
        ) {
            {
                let mut stripe = engine.stripe_lock(bucket).write();
                let live = stripe
                    .live
                    .find_mut(hash, |l| l.key_bytes.as_ref() == kb.as_ref())
                    .expect("entry is present");
                live.weight = 0;
            }
            engine
                .total_weight
                .fetch_sub(u64::from(weight), Ordering::Relaxed);
        }

        #[test]
        fn abandon_restores_the_weight_of_a_still_pending_entry() {
            let weigher: Weigher<u32, String> =
                Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher));
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

            SpillSink::abandon(&engine, bucket, &kb, hash, ver);

            let (_, weight_after) = engine.debug_totals();
            assert_eq!(
                weight_after, 7,
                "abandon recomputes the weight through the weigher and adds it back to \
                 total_weight"
            );
            let stripe = engine.stripe_lock(bucket).read();
            let live = stripe
                .live
                .iter()
                .find(|l| l.key_bytes.as_ref() == kb.as_ref())
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
            let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher));
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

            SpillSink::abandon(&engine, bucket, &kb, hash, old_ver);

            let (_, weight_after) = engine.debug_totals();
            assert_eq!(
                weight_after, weight_before_abandon,
                "a key whose stored state changed since hand-off is left alone"
            );
            assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
        }

        #[test]
        fn abandon_on_a_key_that_is_no_longer_present_is_a_noop() {
            let engine = engine_u32_string(u64::MAX, None);
            let kb = key_bytes(999);
            let hash = hash_key_bytes(kb.as_ref());
            let bucket = stripe_index_from_hash(hash);
            let (_, weight_before) = engine.debug_totals();

            SpillSink::abandon(&engine, bucket, &kb, hash, hlc(1, 1));

            let (_, weight_after) = engine.debug_totals();
            assert_eq!(weight_after, weight_before);
        }

        #[test]
        fn insert_over_a_spilled_key_replaces_it_with_a_fresh_resident_entry() {
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
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
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
            assert_eq!(engine.debug_spill_entries_count(), 1);

            {
                let mut stripe = engine.stripe_lock(bucket).write();
                let resolver = LwwResolver;
                let (outcome, displaced_spilled) = apply_locked(
                    &mut stripe,
                    &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
                    &engine.total_weight,
                    &engine.live_count,
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
            engine.debug_insert_spilled(key, &kb, ver, None, loc(0, 0, 4, 0), 0);
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
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
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
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), Some(50), loc(0, 0, 4, 0), 0);
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
            // `complete_fresh_load` unconditionally replaces it, and must
            // still keep `sundog_spill_entries` correct.
            let engine = engine_u32_string(u64::MAX, None);
            let key = 1u32;
            let kb = key_bytes(key);
            let hash = hash_key_bytes(kb.as_ref());
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
            assert_eq!(engine.debug_spill_entries_count(), 1);

            let inflight = Arc::new(Inflight::<String>::new());
            let encoded = Bytes::from(postcard::to_stdvec(&"loaded".to_string()).unwrap());
            let had_live = engine.complete_fresh_load(
                &key,
                &kb,
                hash,
                hlc(2, 1),
                "loaded".to_string(),
                encoded,
                None,
                0,
                &inflight,
            );
            assert!(
                had_live,
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
            engine.debug_insert_spilled(key, &kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
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

            fn is_spilled(engine: &Engine<u32, String>, kb: &Bytes) -> bool {
                let hash = hash_key_bytes(kb.as_ref());
                let bucket = stripe_index_from_hash(hash);
                let stripe = engine.stripe_lock(bucket).read();
                stripe.live.iter().any(|l| {
                    l.key_bytes.as_ref() == kb.as_ref() && matches!(l.payload, Payload::Spilled(_))
                })
            }

            #[test]
            fn set_spill_makes_it_visible_via_spill_accessor() {
                let dir = temp_dir("set-spill-accessor");
                let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
                let tier = Arc::new(SpillTier::open(&cfg, "accessor").expect("tier opens"));
                let engine = Engine::<u32, String>::new(u64::MAX, None, None);
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
                let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher));
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
                let pass_outcome = engine.evict_one_sampled(bucket);
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
                assert!(
                    matches!(
                        {
                            let stripe = engine.stripe_lock(bucket).read();
                            stripe
                                .live
                                .iter()
                                .find(|l| l.key_bytes.as_ref() == kb.as_ref())
                                .map(|l| (l.weight, matches!(l.payload, Payload::Resident { .. })))
                        },
                        Some((0, true))
                    ),
                    "the victim stays Resident at weight 0 until the flusher installs it"
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
        }
    }
}
