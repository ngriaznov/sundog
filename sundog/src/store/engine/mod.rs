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
//!
//! This module holds the data model: [`Live`], [`Stripe`], [`Written`],
//! [`Inflight`] and the [`Engine`] struct with its constructor and
//! accounting. The behavior lives in child modules. `keys`, `record`,
//! `expiry` and `slab` hold the entry representation and the hash
//! arithmetic. `merge`, `prefold` and `apply` hold the versioned write path.
//! `read`, `serve`, `removal`, `inflight`, `housekeeping` and `evict` hold
//! the `Engine` methods for point reads, anti-entropy serving, invalidation
//! and ownership release, loads, sweeping and compaction, and capacity
//! eviction. `checkpoint`, `promote` and `sink` hold the spill-only
//! methods, and `debug` the test-only accessors.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
#[cfg(all(feature = "spill", test))]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use hashbrown::HashTable;
use parking_lot::RwLock;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::hlc::Hlc;
use crate::node::NodeId;

#[cfg(doc)]
use super::ConflictResolver;
#[cfg(feature = "spill")]
pub(crate) use super::spill::Reservation;
#[cfg(all(doc, feature = "spill"))]
use super::spill::SpillSink;
#[cfg(all(doc, feature = "spill"))]
use super::spill::keeps_refused_victim_resident;
#[cfg(feature = "spill")]
use super::spill::{SpillLoc, SpillTier};
use super::{BUCKET_COUNT, PART_COUNT, Tombstone, Weigher, entry_fingerprint};

mod apply;
#[cfg(feature = "spill")]
mod checkpoint;
#[cfg(test)]
mod debug;
mod evict;
mod expiry;
mod housekeeping;
mod inflight;
/// Kani proofs over the engine's pure arithmetic: every input, not a sample.
#[cfg(kani)]
mod kani_proofs;
mod keys;
mod merge;
mod prefold;
#[cfg(feature = "spill")]
mod promote;
mod read;
mod record;
#[cfg(test)]
mod record_tests;
mod removal;
mod serve;
#[cfg(feature = "spill")]
mod sink;
mod slab;
#[cfg(test)]
mod slab_tests;
#[cfg(test)]
mod tests;

#[cfg(doc)]
use self::apply::apply_locked;
pub(crate) use self::apply::{ApplyScope, Arrival};
#[cfg(feature = "spill")]
pub(crate) use self::checkpoint::CheckpointFinalizeCounts;
#[cfg(doc)]
use self::evict::VictimOutcome;
#[cfg(doc)]
use self::expiry::encode_expiry;
#[cfg(all(test, not(feature = "sim")))]
pub(crate) use self::expiry::pause_gap_ms;
#[cfg(doc)]
use self::expiry::{LONG_TTL, decode_expiry, idle_elapsed_ms};
pub(crate) use self::expiry::{
    Lapsed, TombstoneRetention, expiry_tombstone_retention, gc_period_ms,
};
use self::expiry::{absent_at, absent_under, clamp_tti_ms, touch_stamp};
pub(crate) use self::housekeeping::{
    CompactCandidate, CompactRefusal, CompactReplace, CompactScan,
};
pub(crate) use self::inflight::{FillOutcome, FreshLoad, JoinOutcome};
#[cfg(any(test, kani, doc))]
pub(crate) use self::keys::part_index_from_hash;
use self::keys::{digest_slot, hasher_for};
pub(crate) use self::keys::{hash_key_bytes, stripe_index_from_hash};
pub(crate) use self::merge::{Fold, MergeCap, fold_sides};
#[cfg(doc)]
use self::merge::{merge_version, resolve_conflict};
#[cfg(doc)]
use self::prefold::peek_stored_seed;
use self::record::{Record, record_key};
#[cfg(doc)]
use self::record::{
    build_key_only_record, build_resident_record, decode_key, decode_value, entry_bytes,
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
    /// [`idle_elapsed_ms`] (a rollover confuses a raw comparison, and a
    /// stamp ahead of an observer's clock reads as zero idle).
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
///
/// It also totals, across the same batch, the peer tombstones the apply
/// ignored as lapsed; see [`Written::ignored_tombstones`].
#[derive(Debug, Default)]
pub(crate) struct Written {
    hashes: HashTable<u64>,
    ignored_tombstones: u64,
}

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
            self.hashes.insert_unique(hash, hash, |&held| held);
        }
    }

    fn contains(&self, hash: u64) -> bool {
        self.hashes.find(hash, |&held| held == hash).is_some()
    }

    fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Drops every hash, leaving the writes' entries to ordinary sampling.
    fn clear(&mut self) {
        self.hashes.clear();
    }

    /// How many peer tombstones the applies sharing this `Written` ignored as
    /// lapsed so far.
    pub(crate) const fn ignored_tombstones(&self) -> u64 {
        self.ignored_tombstones
    }

    /// Whether `live` is one of the entries these writes stored.
    fn holds<K, V>(&self, live: &Live<K, V>) -> bool {
        !self.hashes.is_empty() && self.contains(hash_key_bytes(record_key(&live.record)))
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

/// The outcome of [`apply_locked`]: the caller's `key` back plus what changed,
/// to build an [`super::Event`] and decide on fan-out. `version` is the
/// version the write ended up stored at: after a merging resolver rebinds
/// it, the rebound version, not the caller's stamp. The fan-out queue carries
/// it, so a write whose entry is gone by the time it fans out still has an
/// identity to invalidate peers with.
pub(crate) enum ApplyOutcome<K, V> {
    /// `incoming` loses to what is already stored; nothing changes.
    Rejected,
    /// Writes a value. `created` is `false` when the value replaces an
    /// existing live entry.
    Put {
        key: K,
        value: V,
        created: bool,
        version: Hlc,
    },
    /// Writes a tombstone, replacing whatever, live entry or nothing,
    /// preceded it. Unlike `Put`'s `created`, this carries no
    /// prior-liveness flag.
    Tombstoned { key: K, version: Hlc },
    /// A merge of `key_bytes` would leave the value `size` bytes long, past
    /// the engine's [`MergeCap`] and larger than the value it replaces;
    /// nothing changes.
    TooLarge { key_bytes: Bytes, size: usize },
}

impl<K, V> ApplyOutcome<K, V> {
    /// The value size of a merge refused for passing the cap; `None` for any
    /// other outcome.
    pub(crate) const fn too_large(&self) -> Option<usize> {
        match self {
            Self::TooLarge { size, .. } => Some(*size),
            Self::Rejected | Self::Put { .. } | Self::Tombstoned { .. } => None,
        }
    }

    /// The key a write landed on; `None` for a rejected or refused write,
    /// which changed nothing and has nothing to fan out.
    pub(crate) fn key(&self) -> Option<&K> {
        match self {
            Self::Rejected | Self::TooLarge { .. } => None,
            Self::Put { key, .. } | Self::Tombstoned { key, .. } => Some(key),
        }
    }

    /// The version the write landed at; `None` for a rejected or refused
    /// write.
    pub(crate) fn version(&self) -> Option<Hlc> {
        match self {
            Self::Rejected | Self::TooLarge { .. } => None,
            Self::Put { version, .. } | Self::Tombstoned { version, .. } => Some(*version),
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
    /// The configured time-to-idle, clamped by [`clamp_tti_ms`] to
    /// `MAX_TTI_MS` (~48.7 days).
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
    /// The frame cap a merge keeps a value within, as
    /// `(cache name length, limit)`; the limit is `usize::MAX`, the default,
    /// for no cap. Set by [`Engine::set_merge_cap`].
    merge_cap_limit: AtomicUsize,
    merge_cap_cache_len: AtomicUsize,
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
    /// What the last [`Engine::gc_tombstones`] call was told about whether
    /// collection defers, or what [`Engine::starting_gc_deferred`] set before
    /// the first call. A tombstone that arrives already past the retention
    /// that call applies is dropped rather than stored; see
    /// [`expiry::drops_lapsed_tombstone`].
    gc_deferring: AtomicBool,
    /// The wall-clock reading of the last [`Engine::gc_tombstones`] call, at
    /// least `1`; `0` before the first. The pause guard measures the gap
    /// between two collections on it ([`expiry::paused_since`]).
    last_gc_ms: AtomicU64,
    /// The instant the hold a pause started lasts to; `0` when none. While
    /// the clock is before it, collection and arrivals defer; see
    /// [`expiry::gc_defer_decision`].
    pause_hold_until_ms: AtomicU64,
    /// Whether the pause guard is on; see [`Engine::guarding_pauses`].
    pause_guard: bool,
    #[cfg(test)]
    eviction_lock_acquisitions: AtomicU64,
    /// Test-only: how many stripe read locks [`Engine::compact`] has taken
    /// across its whole run, one per stripe visited, never more than one
    /// held at a time since each is dropped before the next is taken.
    #[cfg(test)]
    compact_lock_acquisitions: AtomicU64,
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
            merge_cap_limit: AtomicUsize::new(usize::MAX),
            merge_cap_cache_len: AtomicUsize::new(0),
            compact_cursor: AtomicU64::new(0),
            gc_deferring: AtomicBool::new(false),
            last_gc_ms: AtomicU64::new(0),
            pause_hold_until_ms: AtomicU64::new(0),
            pause_guard: false,
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

    fn is_absent(
        &self,
        live: &Live<K, V>,
        key_bytes: &[u8],
        long_ttl: &HashMap<Bytes, u64>,
        now_ms: u64,
    ) -> bool {
        absent_at(live, key_bytes, long_ttl, self.tti_ms, now_ms)
    }

    /// [`Engine::is_absent`] under a record view: [`Lapsed::Shown`] reports
    /// an entry past its expiry present.
    fn is_absent_under(
        &self,
        live: &Live<K, V>,
        key_bytes: &[u8],
        long_ttl: &HashMap<Bytes, u64>,
        now_ms: u64,
        lapsed: Lapsed,
    ) -> bool {
        absent_under(live, key_bytes, long_ttl, self.tti_ms, now_ms, lapsed)
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

    /// The number of live entries across every stripe.
    pub(crate) fn live_entry_count(&self) -> u64 {
        self.live_count.load(Ordering::Relaxed)
    }

    /// The number of tombstones across every stripe, counted under each
    /// stripe's read lock in turn. A tombstone is not a live entry, so
    /// [`Engine::live_entry_count`], the weight and the capacity ignore it.
    pub(crate) fn tombstone_count(&self) -> u64 {
        self.stripes
            .iter()
            .map(|s| u64::try_from(s.read().tombstones.len()).unwrap_or(u64::MAX))
            .fold(0u64, u64::saturating_add)
    }

    /// Starts this engine with tombstone collection counted as deferring
    /// (`deferred`), until the first [`Engine::gc_tombstones`] call says
    /// otherwise: a tombstone that arrives before that call is judged
    /// against its cap deadline, not its `tombstone_ttl` one: a delete's
    /// `tombstone_max_ttl`, while an expiry tombstone's cap is its ttl
    /// deadline. An engine built by [`Engine::new`] starts not deferring.
    #[must_use]
    pub(crate) fn starting_gc_deferred(mut self, deferred: bool) -> Self {
        *self.gc_deferring.get_mut() = deferred;
        self
    }

    /// Whether tombstone collection currently counts as deferring; see
    /// [`Engine::starting_gc_deferred`] and [`Engine::gc_tombstones`].
    #[cfg(test)]
    pub(crate) fn gc_deferring(&self) -> bool {
        self.gc_deferring.load(Ordering::Relaxed)
    }

    /// Turns the pause guard on (`on`) for an engine that defers tombstone
    /// collection: a node that went longer than [`expiry::pause_gap_ms`]
    /// without collecting is treated as having missed the deletes its peers
    /// made, so it defers for `tombstone_ttl` from then on. Evaluated at
    /// apply time ([`Engine::deferring_at`]) and at the next collection
    /// ([`Engine::gc_tombstones`]). Off by default, and for a mode that
    /// defers nothing ([`Mode::defers_tombstone_gc`]), whose peers hold no
    /// tombstones for it.
    #[must_use]
    pub(crate) fn guarding_pauses(mut self, on: bool) -> Self {
        self.pause_guard = on;
        self
    }

    /// Whether an arrival at `now_ms` is judged against the cap deadline
    /// rather than the `tombstone_ttl` one (the two are equal for the
    /// tombstone of an expired entry): the last collection decided to
    /// defer, or the pause guard sees a gap since that collection longer than
    /// [`expiry::pause_gap_ms`] for `tombstone_ttl_ms`, or the hold a pause
    /// started still lasts. Read from the clock rather than from the last
    /// collection alone, so the guard is in force for the first arrival after
    /// a resume, before the gc task has woken.
    pub(crate) fn deferring_at(&self, now_ms: u64, tombstone_ttl_ms: u64) -> bool {
        if !self.pause_guard {
            return self.gc_deferring.load(Ordering::Relaxed);
        }
        // The collection time first, with `Acquire`: the collection that
        // wrote it published its hold and deferral before it, so they are at
        // least as new as the time read here.
        let last_gc_ms = self.last_gc_ms.load(Ordering::Acquire);
        let defer = self.gc_deferring.load(Ordering::Acquire);
        let paused = expiry::paused_since(last_gc_ms, now_ms, tombstone_ttl_ms);
        expiry::gc_defer_decision(
            defer,
            paused,
            self.pause_hold_until_ms.load(Ordering::Acquire),
            now_ms,
        )
        .0
    }

    /// The wall-clock reading of the last collection; `0` before the first.
    #[cfg(test)]
    pub(crate) fn last_gc_ms(&self) -> u64 {
        self.last_gc_ms.load(Ordering::Relaxed)
    }

    /// The instant the current pause hold lasts to; `0` when none.
    #[cfg(test)]
    pub(crate) fn pause_hold_until_ms(&self) -> u64 {
        self.pause_hold_until_ms.load(Ordering::Relaxed)
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
