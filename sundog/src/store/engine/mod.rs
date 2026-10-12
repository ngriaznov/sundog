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
//! fits under `max_capacity`. The entries the triggering write just stored
//! go last, only once nothing else is left to evict. [`Engine::live_entry_count`] is a counter
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

use crate::explain::LocalRecord;
use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::wire::WireRecord;

use super::crdt;
use super::part::PartSet;
#[cfg(feature = "spill")]
pub(crate) use super::spill::Reservation;
#[cfg(feature = "spill")]
use super::spill::{
    Admission, ReclaimRef, SpillJob, SpillLoc, SpillSink, SpillTier, keeps_refused_victim_resident,
    spill_record_len, spilled_is_current,
};
use super::{
    BUCKET_COUNT, BucketEntries, BucketPart, ConflictResolver, KeyVersion, PART_COUNT, PartEntries,
    PartId, Quiescence, Tombstone, Ttl, Weigher, entry_fingerprint,
};

mod apply;
mod expiry;
mod keys;
mod merge;
mod prefold;
mod record;
mod slab;

#[cfg(doc)]
use self::apply::apply_locked;
use self::expiry::{
    LONG_TTL, absent_at, clamp_tti_ms, decode_expiry, encode_expiry, idle_elapsed_ms, lapse_cause,
    touch_stamp,
};
use self::keys::{digest_slot, encode_key_for_read, hasher_for};
pub(crate) use self::keys::{hash_key_bytes, part_index_from_hash, stripe_index_from_hash};
#[cfg(doc)]
use self::merge::{merge_version, resolve_conflict};
#[cfg(doc)]
use self::prefold::peek_stored_seed;
#[cfg(feature = "spill")]
use self::record::build_key_only_record;
#[cfg(any(test, doc))]
use self::record::entry_bytes;
use self::record::{
    Record, build_resident_record, decode_key, decode_value, record_key, record_key_bytes,
    record_value, record_value_bytes, set_record,
};
use self::slab::{Slab, SlabEntry};

/// A never-constructed placeholder for
/// [`spill::Reservation`](super::spill::Reservation), so both builds share
/// one eviction-chain signature.
#[cfg(not(feature = "spill"))]
#[allow(
    dead_code,
    reason = "never constructed on this build; see the doc comment above"
)]
pub(crate) struct Reservation<'a>(PhantomData<&'a ()>);

/// How many live entries one capacity-eviction pass weighs before evicting the
/// least recently read of them.
const EVICTION_SAMPLE: usize = 8;

/// How many entries one [`Engine::enforce_capacity`] lock hold samples, and
/// at most how many of the coldest it evicts.
const EVICTION_BATCH_SAMPLE: usize = 32;
const EVICTION_BATCH: usize = 8;

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

#[cfg(test)]
mod record_tests;

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

/// The keys the writes in progress just stored, by hash. The eviction a
/// write triggers samples every other entry first and takes these only
/// once nothing else is left to evict, so a write never makes room by
/// evicting itself while an older entry remains. An entry heavier than the
/// whole cap never fits, so it is sampled like any other. A caller
/// applying one batch across several stripes keeps one `Written` for the
/// whole batch; the eviction clears it once a scan finds nothing but these
/// entries, so later passes for the same batch skip that scan. Each hash
/// is already an xxh3 of the key bytes, so the table keys on it as is.
#[derive(Debug, Default)]
pub(crate) struct Written(HashTable<u64>);

impl Written {
    /// `hash` alone.
    pub(crate) fn of(hash: u64) -> Self {
        let mut written = Self::default();
        written.insert(hash);
        written
    }

    /// Adds `hash`.
    pub(crate) fn insert(&mut self, hash: u64) {
        if !self.contains(hash) {
            self.0.insert_unique(hash, hash, |&held| held);
        }
    }

    fn contains(&self, hash: u64) -> bool {
        self.0.find(hash, |&held| held == hash).is_some()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Drops every hash, leaving the writes' entries to ordinary sampling.
    fn clear(&mut self) {
        self.0.clear();
    }

    /// Whether `live` is one of the entries these writes stored.
    fn holds<K, V>(&self, live: &Live<K, V>) -> bool {
        !self.0.is_empty() && self.contains(hash_key_bytes(record_key(&live.record)))
    }
}

/// What an eviction pass does with a victim its spill tier refuses
/// ([`Admission::RefusedFinal`](super::spill::Admission): the flush queue is
/// full, the record is too large for a region, or the tier is closed).
/// [`keeps_refused_victim_resident`] combines it with the tier's own flag.
/// Without a spill tier, no victim is ever refused and the choice is moot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OnRefusal {
    /// The tier decides: a `Mode::Replicated` or `Mode::Distributed` tier
    /// leaves a refused victim resident, any other mode deletes it. A write's
    /// eviction runs under this policy.
    TierPolicy,
    /// A refused victim stays resident whatever the mode, and the pass
    /// reports it as refused. A promotion's eviction runs under this policy:
    /// a spilled-key read never deletes an entry.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            dead_code,
            reason = "only the promotion path constructs it, and promotion is spill-only"
        )
    )]
    KeepResident,
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
    /// Set iff the fill finished without storing its value, because the
    /// shard sat at its memory ceiling or an invalidation landed during the
    /// load, or once the owner read the key's spilled value off disk and
    /// the key still held it ([`Engine::finish_spilled_read`]): a joined
    /// waiter returns this value instead of loading again.
    pub(crate) value: OnceLock<V>,
    /// Set when an invalidation for the key lands while the load runs. The
    /// loader may have read the source before the change the invalidation
    /// announces, so [`Engine::complete_fresh_load`] answers with the value
    /// and stores nothing.
    invalidated: AtomicBool,
    /// Set iff the load finished and the source holds nothing for the key:
    /// a joined waiter answers `None` instead of loading again.
    absent: AtomicBool,
}

impl<V> Inflight<V> {
    fn new() -> Self {
        Self {
            done: watch::channel(false).0,
            error: OnceLock::new(),
            value: OnceLock::new(),
            invalidated: AtomicBool::new(false),
            absent: AtomicBool::new(false),
        }
    }

    /// A receiver for this load's end, for the caller that owns the load;
    /// [`watch::Receiver::wait_for`] on it returns at once if the load
    /// already ended.
    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.done.subscribe()
    }

    /// Whether the load ended with the source holding nothing for the key.
    pub(crate) fn is_absent(&self) -> bool {
        self.absent.load(Ordering::Relaxed)
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

#[cfg(test)]
mod slab_tests;

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
/// [`Engine::evict_one_scanning`]'s scan across stripes, and leave the rest
/// to the flusher and later passes. Only ever `true` while some hand-off is
/// in flight, and then in either of two cases: the tier `refused` the
/// pass's victim, most often because the flush queue is full, which
/// refuses every other stripe's victim alike until the flusher drains it;
/// or `total_weight`, the resident weight no hand-off covers, is already
/// within `limit`, so the installs alone bring the cache within it. A
/// refusal particular to its victim, a record larger than a region or than
/// the queue's free bytes, stops the pass as well. `false` otherwise, this
/// loop's cue to fall back to the scan, spill tier configured or not: a
/// cache still over `limit` without its hand-offs stays over it after they
/// install. Pure; unit tested directly.
fn defer_to_flusher(
    refused: bool,
    total_weight: u64,
    pending_spill_weight: u64,
    limit: u64,
) -> bool {
    pending_spill_weight > 0 && (refused || total_weight <= limit)
}

/// [`Engine::evict_down_to`]'s stop rule after a batch reports a nonzero
/// [`EvictOutcome::deficit`]: `0` once `current` (weight read fresh after
/// the batch) has already dropped to `limit` or under, `deficit` unchanged
/// otherwise. A concurrent flusher install or another victim's success can
/// already satisfy `current` even though `deficit` alone still looks owed.
/// Pure; unit tested directly.
fn deficit_once_still_over_capacity(current: u64, limit: u64, deficit: u64) -> u64 {
    if current <= limit { 0 } else { deficit }
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
    /// Whether this pass left a victim resident because the spill tier
    /// refused its hand-off ([`VictimOutcome::Deferred`]): a full flush
    /// queue, a record larger than a region or than the queue's free bytes,
    /// or a closed tier.
    refused: bool,
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
    /// via [`SpillTier::would_accept_under`], but the channel send itself,
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
    /// ([`SpillTier::would_accept_under`] declining: too large, closed, or the
    /// flush queue is full), and `keep_resident` says to leave it
    /// resident rather than delete it: the `Mode::Replicated`/
    /// `Mode::Distributed` policy `Shard::attach_spill` sets on the tier, or
    /// [`OnRefusal::KeepResident`], which a promotion's eviction passes in
    /// every mode.
    /// `stripe.live` is untouched: the entry keeps its full weight and
    /// stays a spill candidate for a later eviction pass to sample and
    /// retry, once the tier has room again.
    /// Reported to the caller exactly like [`VictimOutcome::Vanished`], no
    /// weight or count to fold in, since nothing here changed. Only ever
    /// constructed under `feature = "spill"`.
    #[cfg(feature = "spill")]
    Deferred,
    /// [`SpillTier::would_accept_under`] returned [`Admission::RefusedPending`];
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
    /// [`SpillTier::would_accept_under`] returned [`Admission::RefusedFinal`].
    /// `keep_resident` is [`keeps_refused_victim_resident`] of the pass's
    /// [`OnRefusal`] and the tier's policy at refusal time.
    Refused { keep_resident: bool },
    /// [`SpillTier::would_accept_under`] returned [`Admission::RefusedPending`].
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

/// What [`Engine::complete_fresh_load`] did with a fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FillOutcome {
    /// The loaded value is installed; `had_live` says whether it replaced a
    /// live entry.
    Installed { had_live: bool },
    /// A write or removal at least as new as the fill landed while the
    /// loader ran, and stays in place of the loaded value.
    Superseded,
    /// An invalidation landed while the loader ran, so the loaded value may
    /// predate the change it announces: nothing is stored, and joined
    /// waiters answer with the value.
    Invalidated,
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
    /// Resident bytes across every stripe's live entries, per
    /// [`entry_bytes`], once [`Engine::count_resident_bytes`] turns counting
    /// on: shared with each stripe's [`Slab`], which moves it on every
    /// insert and removal, and moved by [`set_record`] on every in-place
    /// record swap. `None`, the default, counts nothing.
    resident_bytes: Option<Arc<AtomicU64>>,
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
            resident_bytes: None,
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

    /// [`Engine::get_by_bytes`] that also returns the entry's version and
    /// absolute expiry, `None` for none: what refresh-ahead checks a read
    /// against.
    pub(crate) fn get_with_lifetime(
        &self,
        key_bytes: &[u8],
        hash: u64,
        now_ms: u64,
    ) -> Option<(V, Hlc, Option<u64>)> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) || !is_resident(live) {
            return None;
        }
        self.touch(live, now_ms);
        let deadline = decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl);
        Some((
            decode_value(record_value(live.record.as_slice())),
            live.ver(),
            deadline,
        ))
    }

    /// The version and absolute expiry of the key's live entry, resident or
    /// spilled, and `None` with no live entry. Not an access.
    pub(crate) fn lifetime_of(
        &self,
        key_bytes: &[u8],
        hash: u64,
        now_ms: u64,
    ) -> Option<(Hlc, Option<u64>)> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        Some((
            live.ver(),
            decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl),
        ))
    }

    /// What this engine stores for `key_bytes` at `now_ms`: a tombstone,
    /// a live entry resident or spilled, an entry a read no longer returns
    /// and a sweep has not reclaimed, or nothing. One stripe read lock, no
    /// recency touch, no value decode, no disk read.
    pub(crate) fn inspect(&self, key_bytes: &[u8], now_ms: u64) -> LocalRecord {
        let hash = hash_key_bytes(key_bytes);
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        if let Some(t) = stripe.tombstones.get(key_bytes) {
            return LocalRecord::Tombstone { version: t.ver };
        }
        let Some(live) = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)
        else {
            return LocalRecord::Absent;
        };
        let version = live.ver();
        let expires_at_ms = decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl);
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            LocalRecord::Lapsed {
                version,
                expires_at_ms,
                cause: lapse_cause(expires_at_ms, now_ms),
            }
        } else {
            LocalRecord::Live {
                version,
                expires_at_ms,
                spilled: is_spilled(live),
            }
        }
    }

    /// Registers a reload of a key that may still hold a live entry, for
    /// refresh-ahead: readers keep answering from the entry while the
    /// reload runs, and [`Engine::miss_or_join`] joins it only once the
    /// entry is gone. `None` when a load for the key is already in flight.
    /// The reload ends like any other load, through
    /// [`Engine::complete_fresh_load`] or [`Engine::abandon_inflight`].
    pub(crate) fn begin_refresh(&self, key_bytes: &Bytes, hash: u64) -> Option<Arc<Inflight<V>>> {
        let mut stripe = self.stripes[stripe_index_from_hash(hash)].write();
        if stripe.inflight.contains_key(key_bytes.as_ref()) {
            return None;
        }
        let inflight = Arc::new(Inflight::new());
        stripe
            .inflight
            .insert(key_bytes.clone(), Arc::clone(&inflight));
        Some(inflight)
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
                        let key_only = build_key_only_record(record_key(&live.record));
                        set_record(live, key_only, self.resident_bytes.as_deref());
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
            let record = build_resident_record(record_key(&live.record), encoded.as_ref());
            set_record(live, record, self.resident_bytes.as_deref());
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

    /// Turns resident byte counting on, for an engine that holds no
    /// entries yet: from here every insert, removal and in-place record
    /// swap moves [`Engine::resident_bytes`]. Off by default, so an engine
    /// nobody caps pays nothing for it.
    #[must_use]
    pub(crate) fn count_resident_bytes(mut self) -> Self {
        let total = Arc::new(AtomicU64::new(0));
        for stripe in &mut self.stripes {
            let live = &mut stripe.get_mut().live;
            debug_assert_eq!(live.len(), 0, "counting starts on an empty engine");
            live.resident_bytes = Some(Arc::clone(&total));
        }
        self.resident_bytes = Some(total);
        self
    }

    /// Resident bytes across every stripe's live entries, `None` unless
    /// [`Engine::count_resident_bytes`] turned counting on: each entry's
    /// fixed overhead plus its record's heap bytes. A lower bound on the
    /// process memory the entries occupy, since allocator rounding and spare
    /// arena capacity are not counted.
    pub(crate) fn resident_bytes(&self) -> Option<u64> {
        self.resident_bytes
            .as_ref()
            .map(|total| total.load(Ordering::Relaxed))
    }

    /// [`Engine::resident_bytes`] recomputed from scratch by walking every
    /// stripe: the reference the maintained counter must equal.
    #[cfg(test)]
    pub(crate) fn recount_resident_bytes(&self) -> u64 {
        self.stripes
            .iter()
            .map(|stripe| stripe.read().live.iter().map(entry_bytes).sum::<u64>())
            .sum()
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
    /// [`SpillTier::would_accept_under`] declines (too large, closed, flush queue
    /// full), leaving [`Engine::evict_victim_locked`] to decide the
    /// victim's fate from `keep_resident`; `on_refusal` is the pass's
    /// policy, which also picks the reason [`SpillTier::would_accept_under`]
    /// records the refusal under. On [`SpillAttempt::Committed`], the
    /// weight returned is already zeroed on the entry in place, and the job
    /// is the caller's to hand to [`Engine::finish_spill_handoff`] once the
    /// stripe lock is released; [`SpillTier::enqueue`] itself runs later,
    /// off this lock, and a full queue found there is handled like any
    /// downstream write failure via [`SpillSink::abandon`].
    #[cfg(feature = "spill")]
    fn try_spill_victim(
        &self,
        stripe: &mut Stripe<K, V>,
        bucket: usize,
        hash: u64,
        victim_bytes: &Bytes,
        reservation: Option<&mut Reservation<'_>>,
        on_refusal: OnRefusal,
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
        match tier.would_accept_under(on_refusal, reservation, victim_bytes.len(), encoded.len()) {
            Admission::RefusedFinal => {
                return SpillAttempt::Refused {
                    keep_resident: keeps_refused_victim_resident(
                        on_refusal,
                        tier.keep_resident_when_refused(),
                    ),
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
    /// discovered later downstream. `on_refusal` is the pass's policy, which
    /// picks the reason [`SpillTier::enqueue`] records that failure under. No
    /// tier configured is unreachable here, but still handled rather than
    /// assumed.
    #[cfg(feature = "spill")]
    fn finish_spill_handoff(&self, on_refusal: OnRefusal, job: SpillJob) {
        let Some(tier) = self.spill() else { return };
        let stripe_idx = job.stripe_idx;
        let hash = job.hash;
        let ver = job.ver;
        let weight = job.weight;
        let admitted_bytes = job.admitted_bytes;
        if let Err(job) = tier.enqueue(on_refusal, job) {
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
    /// refusal, `keep_resident` (`on_refusal` combined with the tier's own
    /// policy, see [`keeps_refused_victim_resident`]) picks between
    /// [`VictimOutcome::Deferred`] (victim stays resident, untouched
    /// weight) and the ordinary remove-and-XOR path,
    /// [`VictimOutcome::Removed`]; a vanished entry reports
    /// [`VictimOutcome::Vanished`]. Total weight and `live_count` stay the
    /// caller's job; a [`VictimOutcome::PendingSpill`] job still needs
    /// [`Engine::finish_spill_handoff`] once `stripe` is dropped.
    /// `reservation`, forwarded to [`Engine::try_spill_victim`], is the
    /// pre-lock budget a caller carried in; unused once `spill` is off, as
    /// is `on_refusal`.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            unused_variables,
            clippy::needless_pass_by_value,
            reason = "`reservation` and `on_refusal` are never read once `try_spill_victim` \
                      compiles out; kept as real parameters so every caller keeps one \
                      signature across both builds"
        )
    )]
    fn evict_victim_locked(
        &self,
        stripe: &mut Stripe<K, V>,
        bucket: usize,
        victim_bytes: &Bytes,
        reservation: Option<&mut Reservation<'_>>,
        on_refusal: OnRefusal,
    ) -> VictimOutcome {
        let hash = hash_key_bytes(victim_bytes.as_ref());
        #[cfg(feature = "spill")]
        match self.try_spill_victim(stripe, bucket, hash, victim_bytes, reservation, on_refusal) {
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
    /// resolves to whichever comes last: an undocumented tie-break. The
    /// candidates `written` holds are never in the window, except one
    /// heavier than the whole cap, which never fits; only a candidate's key
    /// is hashed to check.
    fn sample_candidates<'s>(
        &self,
        stripe: &'s Stripe<K, V>,
        sample_size: usize,
        written: &'s Written,
    ) -> impl Iterator<Item = &'s Live<K, V>> {
        let offset = self.sample_offset(stripe.live.len());
        stripe
            .live
            .iter()
            .skip(offset)
            .chain(stripe.live.iter().take(offset))
            .filter(|live| {
                !is_spill_candidate(live)
                    || u64::from(live.weight) > self.max_capacity
                    || !written.holds(live)
            })
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
        self.evict_one_sampled_with_reservation(
            bucket,
            None,
            &Written::default(),
            OnRefusal::TierPolicy,
            now_ms,
        )
    }

    /// [`Engine::evict_one_sampled`], threading `reservation` and
    /// `on_refusal` through to [`Engine::evict_victim_locked`] and passing
    /// over the entries `written` holds.
    fn evict_one_sampled_with_reservation(
        &self,
        bucket: usize,
        reservation: Option<&mut Reservation<'_>>,
        written: &Written,
        on_refusal: OnRefusal,
        now_ms: u64,
    ) -> EvictOutcome {
        self.note_eviction_lock_acquisition();
        let mut stripe = self.stripes[bucket].write();
        let Some(victim_bytes) = self
            .sample_candidates(&stripe, EVICTION_SAMPLE, written)
            .max_by_key(|live| idle_elapsed_ms(now_ms, live.last_access_ms.load(Ordering::Relaxed)))
            .map(|live| record_key_bytes(&live.record))
        else {
            return EvictOutcome::default();
        };
        let outcome =
            self.evict_victim_locked(&mut stripe, bucket, &victim_bytes, reservation, on_refusal);
        drop(stripe);
        match outcome {
            VictimOutcome::Removed(weight) => {
                self.total_weight
                    .fetch_sub(u64::from(weight), Ordering::Relaxed);
                self.live_count.fetch_sub(1, Ordering::Relaxed);
                EvictOutcome {
                    removed_weight: u64::from(weight),
                    deficit: 0,
                    refused: false,
                }
            }
            #[cfg(feature = "spill")]
            VictimOutcome::PendingSpill(weight, job) => {
                self.total_weight
                    .fetch_sub(u64::from(weight), Ordering::Relaxed);
                self.pending_spill_weight
                    .fetch_add(u64::from(weight), Ordering::Relaxed);
                self.finish_spill_handoff(on_refusal, job);
                EvictOutcome {
                    removed_weight: u64::from(weight),
                    deficit: 0,
                    refused: false,
                }
            }
            #[cfg(feature = "spill")]
            VictimOutcome::Deferred => EvictOutcome {
                refused: true,
                ..EvictOutcome::default()
            },
            #[cfg(feature = "spill")]
            VictimOutcome::PendingReservation(deficit) => EvictOutcome {
                removed_weight: 0,
                deficit: u64::from(deficit),
                refused: false,
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
        self.evict_one_scanning_with_reservation(
            bucket,
            None,
            &Written::default(),
            OnRefusal::TierPolicy,
            now_ms,
        )
        .map(|(_, outcome)| outcome)
    }

    /// [`Engine::evict_one_scanning`], reborrowing `reservation` into each
    /// candidate stripe's scan, so the same budget carries across every
    /// stripe visited. Stops early, `Some`, the first time a candidate
    /// reports [`EvictOutcome::deficit`]: one reservation covers every
    /// stripe, so a deficit in one is a deficit in all. Stops early too,
    /// `Some` with no progress, at a victim refused while a hand-off is in
    /// flight, the case [`defer_to_flusher`] leaves to the flusher. Passes
    /// over the entries `written` holds, so `None` means every stripe holds
    /// nothing else to evict. `Some` carries the stripe it stopped at.
    /// `on_refusal` applies to every victim the scan tries.
    fn evict_one_scanning_with_reservation(
        &self,
        bucket: usize,
        mut reservation: Option<&mut Reservation<'_>>,
        written: &Written,
        on_refusal: OnRefusal,
        now_ms: u64,
    ) -> Option<(usize, EvictOutcome)> {
        (0..BUCKET_COUNT)
            .map(|step| (bucket + step) % BUCKET_COUNT)
            .find_map(|candidate| {
                let outcome = self.evict_one_sampled_with_reservation(
                    candidate,
                    reservation.as_deref_mut(),
                    written,
                    on_refusal,
                    now_ms,
                );
                let refused_while_busy = outcome.refused && self.pending_spill_weight_or_zero() > 0;
                (!outcome.made_no_progress() || outcome.deficit > 0 || refused_while_busy)
                    .then_some((candidate, outcome))
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
        self.evict_batch_sampled_with_reservation(
            bucket,
            over_by,
            None,
            &Written::default(),
            false,
            OnRefusal::TierPolicy,
            now_ms,
        )
    }

    /// [`Engine::evict_batch_sampled`], reborrowing `reservation` into each
    /// victim's own [`Engine::evict_victim_locked`] call, so the same
    /// pre-lock budget carries across every victim this lock hold evicts.
    /// Victims are ranked coldest-first by [`idle_elapsed_ms`] as of
    /// `now_ms`, correct across a `last_access_ms` rollover. With
    /// `only_if_dense`, a sample of fewer than [`EVICTION_SAMPLE`]
    /// candidates gives up nothing, so a sparse stripe is never drained.
    /// `on_refusal` applies to every victim of the batch.
    #[allow(
        clippy::too_many_arguments,
        reason = "one caller per eviction pass; the policy and the density flag are separate \
                  per-pass decisions, and a struct would not make either call site clearer"
    )]
    fn evict_batch_sampled_with_reservation(
        &self,
        bucket: usize,
        over_by: u64,
        mut reservation: Option<&mut Reservation<'_>>,
        written: &Written,
        only_if_dense: bool,
        on_refusal: OnRefusal,
        now_ms: u64,
    ) -> EvictOutcome {
        self.note_eviction_lock_acquisition();
        let mut stripe = self.stripes[bucket].write();
        let mut sampled: Vec<(Bytes, u64, u32)> = self
            .sample_candidates(&stripe, EVICTION_BATCH_SAMPLE, written)
            .map(|live| {
                (
                    record_key_bytes(&live.record),
                    idle_elapsed_ms(now_ms, live.last_access_ms.load(Ordering::Relaxed)),
                    live.weight,
                )
            })
            .collect();
        if sampled.is_empty() || (only_if_dense && sampled.len() < EVICTION_SAMPLE) {
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
        #[cfg_attr(
            not(feature = "spill"),
            allow(
                unused_mut,
                reason = "only VictimOutcome::Deferred, spill-only, ever sets this"
            )
        )]
        let mut refused = false;
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
                on_refusal,
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
                VictimOutcome::Deferred => refused = true,
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
            self.finish_spill_handoff(on_refusal, job);
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
            refused,
        }
    }

    /// After a write to `start_bucket` may have pushed total weight over
    /// `max_capacity`, evicts sampled-cold entries, starting at
    /// `start_bucket` then pseudo-random stripes, until it is back under
    /// the cap, up to [`EVICTION_BATCH`] entries per lock hold. A random
    /// probe that lands on an empty stripe falls back to a scan for the next
    /// non-empty one. When the stripe the scan found holds at least
    /// [`EVICTION_SAMPLE`] candidates, the next pass stays there and evicts
    /// the colder half of its sample, so draining a dense stripe in an
    /// otherwise sparse cache pays for one scan per batch rather than one
    /// per victim; a sparser stripe is left to the random probes. The loop
    /// ends under the cap, with nothing left to evict, or with the rest
    /// left to the flusher, as below. Never holds
    /// two stripe locks at once; a no-op when `max_capacity` is
    /// [`u64::MAX`].
    ///
    /// The over-cap check weighs `total_weight` plus `pending_spill_weight`:
    /// a spill hand-off zeroes and frees its victim's weight from
    /// `total_weight` the instant it commits, but the victim stays fully
    /// resident until the flusher's install resolves it, so
    /// `pending_spill_weight` is what keeps that still-resident RAM
    /// counting against the cap in the meantime. When a pass evicts nothing
    /// while some hand-off is still pending, [`defer_to_flusher`] has this
    /// return rather than pay for [`Engine::evict_one_scanning`]'s scan
    /// across stripes, if the tier refused the pass's victim, most often
    /// for a full flush queue, or `total_weight` alone is within the cap.
    /// Otherwise the scan runs, spill tier configured or not, and stops
    /// early at a victim refused while a hand-off is in flight.
    ///
    /// The entries `written` holds, the ones the triggering write just
    /// stored, are passed over until nothing else is left to evict, and
    /// evicted only then, so the cap holds even for a write heavier than
    /// the whole cap. A thin wrapper, `reservation: None`, over
    /// [`Engine::enforce_capacity_with_reservation`].
    pub(crate) fn enforce_capacity(&self, start_bucket: usize, written: &mut Written, now_ms: u64) {
        let _ = self.enforce_capacity_with_reservation(start_bucket, None, written, now_ms);
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
        reservation: Option<&mut Reservation<'_>>,
        written: &mut Written,
        now_ms: u64,
    ) -> u64 {
        self.evict_down_to(
            self.max_capacity,
            start_bucket,
            reservation,
            written,
            OnRefusal::TierPolicy,
            now_ms,
        )
    }

    /// [`Engine::enforce_capacity_with_reservation`]'s loop, evicting until
    /// `total_weight` plus `pending_spill_weight` is within `limit`, at
    /// most `max_capacity`. [`Engine::promote_locked`] passes the cap less
    /// the weight it is about to reinstall, to make that room first. A
    /// no-op when `max_capacity` is [`u64::MAX`]. Passes over the entries
    /// `written` holds until a scan finds nothing else to evict, then
    /// clears `written` and goes back to `start_bucket`, where the write
    /// that triggered this call landed.
    ///
    /// `on_refusal` is what every victim the tier refuses meets. Under
    /// [`OnRefusal::KeepResident`] a refused victim is never deleted, so the
    /// loop can end over `limit`: it returns once a pass defers to the
    /// flusher or a scan of every stripe finds nothing to evict, and the
    /// caller sees the weight still over the limit.
    fn evict_down_to(
        &self,
        limit: u64,
        start_bucket: usize,
        mut reservation: Option<&mut Reservation<'_>>,
        written: &mut Written,
        on_refusal: OnRefusal,
        now_ms: u64,
    ) -> u64 {
        if self.max_capacity == u64::MAX {
            return 0;
        }
        let mut bucket = start_bucket;
        // Whether a scan just found a victim in `bucket`: the one pass that
        // follows samples that stripe, and evicts there only from a full
        // sample.
        let mut after_scan = false;
        loop {
            let total = self.total_weight.load(Ordering::Relaxed);
            let pending = self.pending_spill_weight_or_zero();
            let current = total.saturating_add(pending);
            if current <= limit {
                return 0;
            }
            let over_by = current - limit;
            let batch_outcome = self.evict_batch_sampled_with_reservation(
                bucket,
                over_by,
                reservation.as_deref_mut(),
                written,
                after_scan,
                on_refusal,
                now_ms,
            );
            if batch_outcome.deficit > 0 {
                let total = self.total_weight.load(Ordering::Relaxed);
                let pending = self.pending_spill_weight_or_zero();
                let current = total.saturating_add(pending);
                return deficit_once_still_over_capacity(current, limit, batch_outcome.deficit);
            }
            if batch_outcome.made_no_progress() && !after_scan {
                if defer_to_flusher(
                    batch_outcome.refused,
                    self.total_weight.load(Ordering::Relaxed),
                    self.pending_spill_weight_or_zero(),
                    limit,
                ) {
                    return 0;
                }
                match self.evict_one_scanning_with_reservation(
                    bucket,
                    reservation.as_deref_mut(),
                    written,
                    on_refusal,
                    now_ms,
                ) {
                    // Only the writes' own entries are left: they go too,
                    // so the cap holds.
                    None if !written.is_empty() => {
                        written.clear();
                        bucket = start_bucket;
                        continue;
                    }
                    None => return 0,
                    Some((_, outcome)) if outcome.deficit > 0 => return outcome.deficit,
                    // The scan stopped at a victim refused while a hand-off
                    // is in flight.
                    Some((_, outcome)) if outcome.made_no_progress() => return 0,
                    Some((found, _)) => {
                        bucket = found;
                        after_scan = true;
                        continue;
                    }
                }
            }
            after_scan = false;
            bucket = self.next_pseudo_random_bucket();
        }
    }

    /// The lifetime left at `now_ms` on the key's live entry, resident or
    /// spilled, and `None` with no live entry. Not an access: the recency
    /// stamp stays as it was.
    pub(crate) fn ttl_of(&self, key_bytes: &[u8], hash: u64, now_ms: u64) -> Option<Ttl> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        Some(
            match decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl) {
                None => Ttl::Never,
                Some(deadline) => {
                    Ttl::Remaining(Duration::from_millis(deadline.saturating_sub(now_ms)))
                }
            },
        )
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
    /// writing no tombstone of its own, and marks a load in flight for the
    /// key so its fill stores nothing. Returns the departing version on an
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
        if let Some(inflight) = stripe.inflight.get(key_bytes) {
            inflight.invalidated.store(true, Ordering::Relaxed);
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
    /// cache-busting escape hatch, and marks a load in flight for the key so
    /// its fill stores nothing.
    pub(crate) fn invalidate_local(&self, key_bytes: &[u8], hash: u64) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if let Some(inflight) = stripe.inflight.get(key_bytes) {
            inflight.invalidated.store(true, Ordering::Relaxed);
        }
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
    /// and tombstone in `parts`, zeroes their part digests, and leaves the
    /// rest of each bucket untouched. A bucket named by all
    /// [`PART_COUNT`] of its parts takes the whole-bucket path. Returns the
    /// total entries removed, live and tombstoned together.
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
    /// stripe write-lock acquisition: removes the `inflight` entry, stores
    /// nothing if an invalidation landed during the load, and,
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
            if inflight.invalidated.load(Ordering::Relaxed) {
                drop(stripe);
                let _ = inflight.value.set(value);
                inflight.finish();
                return FillOutcome::Invalidated;
            }
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
        self.enforce_capacity(bucket, &mut Written::of(hash), now_ms);
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

    /// Records a loader run that found nothing for the key: removes the
    /// `inflight` entry, and every joined waiter answers `None`.
    pub(crate) fn finish_absent(&self, key_bytes: &Bytes, hash: u64, inflight: &Inflight<V>) {
        inflight.absent.store(true, Ordering::Relaxed);
        self.finish_inflight(key_bytes, hash);
        inflight.finish();
    }

    /// Ends `inflight` with no outcome, as a dropped [`InflightGuard`]
    /// does: every joined waiter wakes, re-reads and takes the load over.
    pub(crate) fn abandon_inflight(&self, key_bytes: &Bytes, hash: u64, inflight: &Inflight<V>) {
        self.finish_inflight(key_bytes, hash);
        inflight.finish();
    }

    /// Ends `inflight` once its owner read the key's spilled value, `value`
    /// at `read_ver`, off disk: under one stripe write lock, hands `value`
    /// to every joined waiter iff the key still holds `read_ver`, spilled
    /// or promoted back, with no tombstone and unexpired at `now_ms`, and
    /// removes the in-flight entry. A removal, expiry or newer write since
    /// the read leaves the waiters to read again, so a call that joins
    /// after it never answers the old value: the check and the removal
    /// share the lock every join takes.
    #[cfg(feature = "spill")]
    pub(crate) fn finish_spilled_read(
        &self,
        key_bytes: &Bytes,
        hash: u64,
        inflight: &Inflight<V>,
        read_ver: Hlc,
        value: V,
        now_ms: u64,
    ) {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        let stored_tombstone_ver = stripe.tombstones.get(key_bytes.as_ref()).map(|t| t.ver);
        let current = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes.as_ref())
            .is_some_and(|live| {
                spilled_is_current(stored_tombstone_ver, Some(live.ver()), read_ver)
                    && !self.is_absent(live, key_bytes.as_ref(), &stripe.long_ttl, now_ms)
            });
        if current {
            let _ = inflight.value.set(value);
        }
        stripe.inflight.remove(key_bytes.as_ref());
        drop(stripe);
        inflight.finish();
    }

    /// Records a loader run whose value is not stored: removes the
    /// `inflight` entry and hands `value` to every joined waiter.
    pub(crate) fn finish_uncached(
        &self,
        key_bytes: &Bytes,
        hash: u64,
        inflight: &Inflight<V>,
        value: V,
    ) {
        let _ = inflight.value.set(value);
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
    /// `key` from `key_bytes` for the one weigher call this makes.
    ///
    /// Makes room first: while the entry is still spilled, and so never an
    /// eviction candidate, [`Engine::evict_down_to`] evicts down to
    /// `max_capacity` less the entry's weight, under
    /// [`OnRefusal::KeepResident`] in every mode: a victim the tier refuses,
    /// most often for a full flush queue, stays resident, never deleted,
    /// and is recorded under `sundog_spill_dropped_total{reason="deferred"}`.
    /// The entry's weight then goes back to `total_weight` only if it fits
    /// within the cap ([`claimed_total`]), claimed atomically so concurrent
    /// promotions never take the same room twice. Without room, the read
    /// stands and the entry stays on disk: an entry heavier than the whole
    /// cap ([`promotion_fits`]), a tier refusing every victim, or a
    /// concurrent promotion that claimed the room first. A promotion
    /// therefore never takes `total_weight` past the cap, never evicts the
    /// entry it reads, and never deletes another entry, whatever the
    /// cache's mode; a write's eviction keeps the tier's own policy.
    ///
    /// **Never touches the digest or `live_count`**: same key, same `ver`,
    /// same fingerprint, so nothing about the entry's replicated identity
    /// changes. Also decrements `sundog_spill_entries{cache}`, the mirror of
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
        now_ms: u64,
    ) -> bool {
        let bucket = stripe_index_from_hash(hash);
        if !spilled_and_current(&self.stripes[bucket].read(), key_bytes, hash, read_ver) {
            return false;
        }
        let key: K = decode_key(key_bytes);
        let weight = self.weigher.as_ref().map_or(1, |w| w(&key, value));
        if !promotion_fits(weight, self.max_capacity) {
            return false;
        }
        let _ = self.evict_down_to(
            self.max_capacity.saturating_sub(u64::from(weight)),
            bucket,
            None,
            &mut Written::default(),
            OnRefusal::KeepResident,
            now_ms,
        );
        if !self.claim_weight(weight) {
            return false;
        }
        self.reinstall_claimed(key_bytes, hash, read_ver, weight, encoded)
    }

    /// [`Engine::promote_locked`]'s last step, once `weight` is claimed in
    /// `total_weight`: rechecks under the write lock that the key is still
    /// spilled at `read_ver`, since a write, removal or racing promotion
    /// may have landed while the room was made, and reinstalls it resident
    /// at `weight`. A failed recheck gives the claim back and returns
    /// `false`.
    #[cfg(feature = "spill")]
    fn reinstall_claimed(
        &self,
        key_bytes: &[u8],
        hash: u64,
        read_ver: Hlc,
        weight: u32,
        encoded: &Bytes,
    ) -> bool {
        let bucket = stripe_index_from_hash(hash);
        let mut stripe = self.stripes[bucket].write();
        if !spilled_and_current(&stripe, key_bytes, hash, read_ver) {
            drop(stripe);
            self.total_weight
                .fetch_sub(u64::from(weight), Ordering::Relaxed);
            return false;
        }
        let live = stripe
            .live
            .find_mut(hash, |l| record_key(&l.record) == key_bytes)
            .expect("invariant: spilled_and_current found it under this same lock");
        set_record(
            live,
            build_resident_record(key_bytes, encoded.as_ref()),
            self.resident_bytes.as_deref(),
        );
        live.state = EntryState::Resident;
        live.weight = weight;
        drop(stripe);
        self.note_spill_departure(true);
        true
    }

    /// Adds `weight` to `total_weight` iff the sum stays within
    /// `max_capacity` ([`claimed_total`]), as one atomic step, so
    /// concurrent [`Engine::promote_locked`] calls never claim the same
    /// room twice. Returns whether it claimed.
    #[cfg(feature = "spill")]
    fn claim_weight(&self, weight: u32) -> bool {
        let mut total = self.total_weight.load(Ordering::Relaxed);
        loop {
            let Some(claimed) = claimed_total(total, weight, self.max_capacity) else {
                return false;
            };
            match self.total_weight.compare_exchange_weak(
                total,
                claimed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => total = actual,
            }
        }
    }
}

/// Whether `stripe` holds `key_bytes` spilled at `read_ver` with no
/// tombstone for the key: the entry [`Engine::promote_locked`] may still
/// reinstall.
#[cfg(feature = "spill")]
fn spilled_and_current<K, V>(
    stripe: &Stripe<K, V>,
    key_bytes: &[u8],
    hash: u64,
    read_ver: Hlc,
) -> bool {
    let stored_tombstone_ver = stripe.tombstones.get(key_bytes).map(|t| t.ver);
    stripe
        .live
        .find(hash, |l| record_key(&l.record) == key_bytes)
        .is_some_and(|live| {
            spilled_is_current(stored_tombstone_ver, Some(live.ver()), read_ver)
                && matches!(live.state, EntryState::Spilled(_))
        })
}

/// Whether an entry of `weight` may come back into RAM under
/// `max_capacity`: never one heavier than the whole cap, which no eviction
/// makes room for. Pure; unit tested directly.
#[cfg(feature = "spill")]
const fn promotion_fits(weight: u32, max_capacity: u64) -> bool {
    (weight as u64) <= max_capacity
}

/// `total` with a promoted entry's `weight` added, iff the sum stays
/// within `max_capacity`: [`Engine::promote_locked`]'s claim on the room it
/// made. Pure; unit tested directly.
#[cfg(feature = "spill")]
fn claimed_total(total: u64, weight: u32, max_capacity: u64) -> Option<u64> {
    total
        .checked_add(u64::from(weight))
        .filter(|&sum| sum <= max_capacity)
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
                    let key_only = build_key_only_record(record_key(&live.record));
                    set_record(live, key_only, self.resident_bytes.as_deref());
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

    fn reclaim(&self, region: u32, generation: u32, rows: &[ReclaimRef]) -> usize {
        let mut removed = 0usize;
        for row in rows {
            let bucket = usize::from(row.stripe);
            let removed_this = {
                let mut stripe = self.stripes[bucket].write();
                // Location identifies the entry: one record occupies one
                // offset of one region generation, so no key compare is
                // needed, and an entry since overwritten, promoted or
                // spilled again elsewhere no longer matches.
                let SlabEntry::Occupied(occ) = stripe.live.entry(
                    row.hash,
                    |l| {
                        matches!(
                            &l.state,
                            EntryState::Spilled(loc) if loc.region == region
                                && loc.generation == generation
                                && loc.offset == row.offset
                        )
                    },
                    hasher_for,
                ) else {
                    continue;
                };
                let (removed_entry, _vacant) = occ.remove();
                let key_bytes = record_key(&removed_entry.record);
                stripe.long_ttl.remove(key_bytes);
                let part = part_index_from_hash(row.hash);
                self.digest[digest_slot(bucket, part)].fetch_xor(
                    entry_fingerprint(key_bytes, removed_entry.ver()),
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
mod tests;

/// Kani proofs over the engine's pure arithmetic: every input, not a sample.
#[cfg(kani)]
mod kani_proofs;
