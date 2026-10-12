//! Local NVMe/SSD spill tier: a FIFO ring of fixed-size region files that
//! extends a size-bounded cache's resident capacity onto disk.
//!
//! A `SpillTier` owns `region_count_for(capacity_bytes, region_bytes)`
//! preallocated region files; `SpillTier::open` recreates them from
//! scratch every call. One region is active, appending new records at its
//! `write_cursor`; when a record doesn't fit, the next region rotates in,
//! purging its still-current keys via `SpillSink::reclaim` first. A
//! `generation` counter per region lets `SpillTier::read_at` recognize a
//! pointer into a region that has since rotated out from under it.
//!
//! One dedicated flusher thread, fed by a bounded channel, batches writes
//! into one positional write per region touched and installs each record
//! through `SpillSink` individually; a write that never reaches
//! `install` calls `SpillSink::abandon` to restore the victim's weight.
//!
//! Spilling never changes what goes on the wire: a purely local
//! representation choice for an already-accepted, versioned value, needing
//! no `wire::PROTOCOL_VERSION` bump.
//!
//! With `SpillConfig::warm_reopen` on, a clean close writes every resident
//! live record into the region ring, then a checkpoint snapshot (key,
//! version, expiry, and on-disk location per live entry) next to the
//! region files. `SpillTier::reopen` trusts that snapshot alone, falling
//! back to `SpillTier::open`'s cold path once the tier sat closed longer
//! than the tombstone TTL. A crash, a close with `warm_reopen` off, or a
//! successful reopen (which deletes the snapshot it replayed) all leave
//! the next open cold. Default: `false`.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::hlc::Hlc;

mod admission;
mod checkpoint;
mod config;
mod flusher;
mod open;
mod record;
mod rules;
mod snapshot;
#[cfg(all(test, not(feature = "sim")))]
mod test_support;

pub use config::SpillConfig;

#[cfg(doc)]
use crate::store::engine::OnRefusal;
#[cfg(doc)]
use flusher::flusher_loop;
#[cfg(doc)]
use rules::{FLUSH_QUEUE_CAPACITY, record_too_large, refusal_drop_reason};

use record::{decode_record, pread_exact};
#[cfg(test)]
pub(crate) use rules::flush_queue_slots;
#[cfg(test)]
pub(crate) use rules::region_count_for;
pub(crate) use rules::{keeps_refused_victim_resident, spill_record_len, spilled_is_current};
#[cfg(test)]
pub(crate) use snapshot::write_stale_snapshot_for_test;
#[cfg(all(test, not(feature = "sim")))]
pub(crate) use test_support::dropped_since_last_snapshot;

/// Corruption/format-skew guard at the front of every [`SpillRecordHeader`].
/// Built from its ASCII bytes so the constant's value and its on-disk byte
/// order always agree: `SPILL_MAGIC.to_le_bytes() == *b"SPIL"`.
const SPILL_MAGIC: u32 = u32::from_le_bytes(*b"SPIL");
/// Fixed on-disk header size preceding every record's key and value bytes.
const HEADER_LEN: usize = size_of::<SpillRecordHeader>();

/// Where one spilled record lives: which region, at what offset and length,
/// stamped with the region's generation at write time. A read whose region
/// generation has since moved on treats the record as gone. `Hash` lets a
/// caller key a prefetched-bytes map by location, e.g.
/// `engine::prefetch_spilled_conflict_bytes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpillLoc {
    pub(crate) region: u32,
    pub(crate) offset: u32,
    pub(crate) len: u32,
    pub(crate) generation: u32,
}

/// One row of a region's reverse index: an entry written into the region,
/// which the region's reclaim purges if the entry still points there. The
/// entry is found by `hash` in stripe `stripe` and confirmed by its
/// location, the region and generation being reclaimed plus `offset`, so
/// the row carries no copy of the key: 16 bytes, where a `(usize, Bytes)`
/// row is 40 bytes plus a heap allocation holding the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReclaimRef {
    /// The entry's key hash, as its stripe's index hashes it.
    pub(crate) hash: u64,
    /// The record's offset within its region.
    pub(crate) offset: u32,
    /// The entry's stripe.
    pub(crate) stripe: u16,
}

impl ReclaimRef {
    /// The row for an entry of stripe `stripe_idx`, hashed `hash`, whose
    /// record sits at `offset`.
    ///
    /// # Panics
    ///
    /// Panics if `stripe_idx` does not fit in `u16`: a stripe index is
    /// always below [`BUCKET_COUNT`](crate::store::BUCKET_COUNT).
    pub(crate) fn new(stripe_idx: usize, hash: u64, offset: u32) -> Self {
        Self {
            hash,
            offset,
            stripe: u16::try_from(stripe_idx)
                .expect("invariant: a stripe index is below BUCKET_COUNT and fits in u16"),
        }
    }
}

/// What `Engine::evict_*_sampled` hands the flusher: everything needed to
/// write the record and, later, to install it back into the right stripe.
pub(crate) struct SpillJob {
    pub(crate) stripe_idx: usize,
    /// `key_bytes`'s hash, already computed by the eviction site that built
    /// this job. Carried through to [`SpillSink::install`] so the sink
    /// never has to rehash the key on this hot path.
    pub(crate) hash: u64,
    pub(crate) key_bytes: Bytes,
    pub(crate) ver: Hlc,
    pub(crate) expires_at_ms: Option<u64>,
    pub(crate) encoded: Bytes,
    /// The weight the eviction site zeroed on the entry and moved into the
    /// engine's pending hand-off total. The job carries it so the sink
    /// releases exactly this amount when the job resolves, whatever
    /// happened to the entry in the meantime.
    pub(crate) weight: u32,
    /// Bytes of `Inner::admit` capacity this job owns, acquired from a
    /// [`Reservation`] or `admit`'s fallback. Released via
    /// [`SpillTier::release`] if the job never reaches the flusher, so the
    /// admission budget isn't shrunk permanently.
    pub(crate) admitted_bytes: u32,
}

/// A record read back off disk: everything a promotion needs to reconstruct
/// the resident payload.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SpilledBytes {
    pub(crate) ver: Hlc,
    pub(crate) expires_at_ms: Option<u64>,
    pub(crate) encoded: Bytes,
}

/// The engine-side callback surface the flusher drives. `Engine<K, V>`
/// implements this; the flusher holds only a `Weak<dyn SpillSink>` and exits
/// once the upgrade fails.
pub(crate) trait SpillSink: Send + Sync + 'static {
    /// The flusher wrote `key_bytes`'s record at `loc`. `hash` is the
    /// job's already-computed key hash, handed back here so the sink never
    /// has to rehash `key_bytes` on this hot path. Under the stripe write
    /// lock, flip the entry to `Payload::Spilled(loc)` if
    /// [`spilled_is_current`] holds for its current tombstone/live state and
    /// `ver`, and return `true`; otherwise leave it untouched and return
    /// `false`. Never re-inserts a key that is not already present.
    fn install(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        loc: SpillLoc,
        weight: u32,
    ) -> bool;

    /// Originates a fresh entry pointing at `loc`, with no value bytes read
    /// into RAM; distinct from [`SpillSink::install`], which only flips an
    /// already-present entry. Used by [`SpillTier::reopen`]'s replay.
    /// Under the stripe write lock: leaves an already-present key
    /// untouched and returns `false` (never resurrects or clobbers);
    /// otherwise inserts at `Payload::Spilled(loc)`, folds its fingerprint
    /// into the digest, and returns `true`.
    #[allow(clippy::too_many_arguments)]
    fn install_new(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        hash: u64,
        ver: Hlc,
        expires_at_ms: Option<u64>,
        loc: SpillLoc,
        weight: u32,
    ) -> bool;

    /// `region` at `generation` is about to be reused. Under each stripe
    /// write lock, remove every listed entry whose payload is still
    /// `Spilled(loc)` with `loc.region == region && loc.generation ==
    /// generation && loc.offset == row.offset`, XOR its fingerprint out of
    /// the digest, and decrement `live_count`. Returns the count removed.
    fn reclaim(&self, region: u32, generation: u32, rows: &[ReclaimRef]) -> usize;

    /// A queued job for `key_bytes` is never installed: its region write
    /// fails, or the tier stops accepting jobs while this one is still
    /// queued unwritten. Either way the victim's weight, zeroed at
    /// hand-off, needs restoring. Under the stripe write lock, finds a
    /// `Resident` entry at exactly `ver` with weight `0`, recomputes its
    /// weight through the weigher, stores it, and adds it back to
    /// `total_weight`. A key whose stored state changed in the meantime, a
    /// fresh write, a tombstone, or (never expected, but never assumed) an
    /// install that already ran, is left untouched: that change already
    /// accounted for the weight this job would have restored.
    fn abandon(&self, stripe_idx: usize, key_bytes: &Bytes, hash: u64, ver: Hlc, weight: u32);
}

/// Fixed header preceding one record's key and value bytes on disk:
/// `[SpillRecordHeader][key_bytes][value_bytes]`. All eight-byte fields lead
/// so the `#[repr(C)]` layout has no implicit padding. That is required
/// for `zerocopy`'s `IntoBytes`/`FromBytes` derives, which reject types
/// with unaccounted-for padding bytes.
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout, Clone, Copy, Debug)]
#[repr(C)]
struct SpillRecordHeader {
    /// `xxh3_64` over `[key_bytes || value_bytes]`, re-verified on every
    /// read. The tier's only defense against a torn write from an unclean
    /// shutdown; no fsync is ever issued.
    checksum: u64,
    /// `u64::MAX` sentinel for `None`.
    expires_at_ms: u64,
    wall_ms: u64,
    node: u64,
    magic: u32,
    key_len: u32,
    value_len: u32,
    logical: u32,
}

/// A checkpoint snapshot's fixed header (not `zerocopy`, since entries
/// after it vary in length), written once at a clean [`SpillTier::close`]
/// and deleted once [`SpillTier::reopen`] replays it; no fsync, so a torn
/// write just fails its checksum, like a missing file.
struct SnapshotHeader {
    region_bytes: u64,
    /// Epoch milliseconds the writing [`SpillTier::close`] ran the
    /// checkpoint at; [`SpillTier::reopen`] refuses one too old to trust
    /// against the cluster's tombstone TTL (see its docs).
    closed_at_ms: u64,
    format_version: u32,
    region_count: u32,
}

/// One live entry a checkpoint recorded, enough for
/// [`SpillSink::install_new`]. Tombstones and expired entries are never
/// written here; see [`SpillTier::write_checkpoint_snapshot`].
#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapshotEntry {
    key: Bytes,
    ver: Hlc,
    expires_at_ms: Option<u64>,
    loc: SpillLoc,
}

/// Per-region mutable state: the pre-opened file handle, one syscall per
/// read/write with no `open()` on the hot path, the write cursor, the
/// generation, and the reverse index of entries written into this region.
/// That index is populated on every install that returns `true`, and
/// drained whenever this region is reclaimed.
struct RegionState {
    file: File,
    write_cursor: AtomicU32,
    generation: AtomicU32,
    /// Bytes this region currently contributes to `Inner::bytes_used`.
    /// Reset to 0, and subtracted from the tier total, on reclaim.
    used_bytes: AtomicU64,
    reverse_index: Mutex<Vec<ReclaimRef>>,
}

/// State shared between [`SpillTier`] and its flusher thread via `Arc`.
struct Inner {
    regions: Box<[RegionState]>,
    /// `region_bytes`, already validated to fit in `u32`.
    region_bytes: u32,
    active: AtomicU32,
    bytes_used: AtomicU64,
    /// Set by [`SpillTier::close`]; a `try_spill` after this always falls
    /// through to the caller's unconditional-delete fallback.
    closed: AtomicBool,
    cache_name: String,
    /// This tier's own directory, `cfg.dir.join(cache_name)`, kept past
    /// `open()` so [`SpillTier::close`] can write a checkpoint snapshot
    /// with no `SpillConfig` in hand.
    dir: PathBuf,
    /// [`SpillConfig::warm_reopen_value`], validated, read by
    /// [`SpillTier::warm_reopen_value`] at close time.
    warm_reopen: bool,
    /// `SpillConfig::flush_queue_bytes_value()`, validated: the byte
    /// budget `admit`'s permits encode, read back by [`SpillTier::attach`]
    /// and [`SpillTier::queued_bytes`].
    flush_queue_bytes: u64,
    /// `SpillConfig::spill_wait_timeout_value()`, validated: consulted by
    /// [`SpillTier::reserve`] callers deciding how long to wait before
    /// non-blocking admission.
    spill_wait_timeout: Duration,
    /// One permit per byte of `flush_queue_bytes_value()`, acquired and
    /// `.forget()`-ed on admission, returned via `add_permits` from
    /// [`flusher_loop`] once a job leaves the channel. Bounds the
    /// flusher's RAM backlog independently of [`FLUSH_QUEUE_CAPACITY`]'s
    /// slot count.
    admit: Semaphore,
    /// Set by [`SpillTier::set_keep_resident_when_refused`], `false` until
    /// then. `true` means a refused hand-off ([`SpillTier::would_accept_under`]
    /// or [`SpillTier::enqueue`] declining) leaves its victim resident
    /// instead of the caller falling back to a delete: `Shard::attach_spill`
    /// sets this for a `Mode::Replicated` or `Mode::Distributed` shard, where
    /// a local delete would have anti-entropy repair the entry back in from
    /// every peer that still holds it.
    keep_resident_when_refused: AtomicBool,
    /// Test-only: [`flusher_loop`] blocks here instead of pulling its next
    /// job, so a test can hold a job queued, and its bytes counted against
    /// `flush_queue_bytes`, for as long as it needs to.
    #[cfg(test)]
    flusher_paused: AtomicBool,
    /// Lifetime count of jobs [`flusher_loop`] pulled off its channel.
    /// [`SpillTier::checkpoint_flush`] diffs this around its join to
    /// report this drain's count.
    flusher_jobs_drained: AtomicU64,
    /// Lifetime count of jobs `write_segment` abandoned rather than
    /// installed (`reason = "disk_error"`). Diffed the same way as
    /// `flusher_jobs_drained`.
    flusher_jobs_abandoned: AtomicU64,
}

impl Inner {
    /// Increments `sundog_spill_dropped_total{cache,reason}`. `reason` is
    /// one of: `"too_large"`, the record can never fit any region, checked
    /// by [`record_too_large`] before it is ever queued; `"closed"`, a
    /// [`SpillTier::try_spill`] call after [`SpillTier::close`];
    /// `"queue_full"`, the flusher's bounded channel has no room, or none
    /// is attached; `"obsolete"`, the flusher wrote the record, but
    /// [`SpillSink::install`] rejected it because the key's state had
    /// already moved on; `"deferred"`, one of the first three refusals but
    /// recorded under this reason instead because the caller leaves the
    /// victim resident, by [`SpillTier::set_keep_resident_when_refused`] or
    /// a promotion's [`OnRefusal::KeepResident`] (see
    /// [`refusal_drop_reason`]); or `"disk_error"`, every job in a segment
    /// whose write failed, though `abandon` still restores the victim,
    /// making a failing disk visible here, not just in a log line.
    fn record_dropped(&self, reason: &'static str) {
        metrics::counter!(
            "sundog_spill_dropped_total",
            "cache" => self.cache_name.clone(),
            "reason" => reason,
        )
        .increment(1);
    }

    /// Increments `sundog_spill_writes_total{cache}` by `count`, once per
    /// flush batch for however many of its jobs installed, rather than
    /// once per record.
    fn record_writes(&self, count: u64) {
        metrics::counter!("sundog_spill_writes_total", "cache" => self.cache_name.clone())
            .increment(count);
    }

    fn record_region_reclaim(&self) {
        metrics::counter!(
            "sundog_spill_region_reclaims_total",
            "cache" => self.cache_name.clone(),
        )
        .increment(1);
    }

    fn publish_bytes_used(&self) {
        metrics::gauge!("sundog_spill_bytes_used", "cache" => self.cache_name.clone())
            .set(bytes_used_f64(self.bytes_used.load(Ordering::Acquire)));
    }

    /// Increments `sundog_spill_wait_seconds_total{cache}` by `elapsed`'s
    /// whole seconds. `metrics::Counter` only holds a `u64`, so sub-second
    /// waits truncate to zero.
    fn record_wait_seconds(&self, elapsed: Duration) {
        metrics::counter!("sundog_spill_wait_seconds_total", "cache" => self.cache_name.clone())
            .increment(elapsed.as_secs());
    }

    /// Increments `sundog_spill_waiters{cache}` and returns a guard
    /// decrementing it on drop, held across [`SpillTier::reserve`]'s one
    /// `.await` so every exit path decrements it once.
    fn record_waiter_delta(&self) -> WaiterGuard<'_> {
        metrics::gauge!("sundog_spill_waiters", "cache" => self.cache_name.clone()).increment(1.0);
        WaiterGuard { inner: self }
    }

    /// Increments `sundog_spill_wait_timeouts_total{cache}`, called only
    /// when [`SpillTier::reserve`]'s timeout elapses.
    fn record_wait_timeout(&self) {
        metrics::counter!("sundog_spill_wait_timeouts_total", "cache" => self.cache_name.clone())
            .increment(1);
    }
}

/// RAII guard from [`Inner::record_waiter_delta`]: decrements
/// `sundog_spill_waiters{cache}` on drop.
struct WaiterGuard<'a> {
    inner: &'a Inner,
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        metrics::gauge!("sundog_spill_waiters", "cache" => self.inner.cache_name.clone())
            .decrement(1.0);
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a gauge only needs f64's exact-integer range, up to 2^53, which comfortably \
              covers realistic spill capacities, petabytes"
)]
fn bytes_used_f64(bytes: u64) -> f64 {
    bytes as f64
}

/// [`SpillTier::checkpoint_flush`]'s result: written records plus how many
/// flush-queue jobs were drained and abandoned. `Default` covers a
/// panicked `spawn_blocking` task.
#[derive(Default)]
pub(crate) struct CheckpointFlushOutcome {
    pub(crate) written: Vec<(Bytes, Hlc, Option<u64>, SpillLoc)>,
    pub(crate) jobs_drained: u64,
    pub(crate) jobs_abandoned: u64,
}

/// A FIFO ring of fixed-size region files extending a cache's resident
/// capacity onto disk. See the module docs for the write/read/rotation
/// mechanics. Opaque: constructed with [`SpillTier::open`], driven through
/// [`SpillTier::attach`]/[`SpillTier::try_spill`]/[`SpillTier::read_at`], torn
/// down with [`SpillTier::close`].
pub(crate) struct SpillTier {
    inner: Arc<Inner>,
    sender: Mutex<Option<SyncSender<SpillJob>>>,
    /// The real flusher thread's join handle, set by [`SpillTier::attach`].
    /// [`SpillTier::checkpoint_flush`] joins it so every already-queued job
    /// lands before the checkpoint writes; ordinary [`SpillTier::close`]
    /// never joins it.
    flusher_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

/// [`SpillTier::reserve`] gave up waiting before its timeout elapsed. The
/// caller falls back to the ordinary non-blocking [`SpillTier::would_accept`]
/// path; see `SpillConfig::spill_wait_timeout`'s docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SpillWaitTimedOut;

/// [`SpillTier::would_accept_under`]'s outcome: not a plain `bool`, since a
/// caller holding a `Reservation` must tell a recorded refusal apart from
/// a transient one it may retry. See [`SpillTier::would_accept_under`]'s
/// docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Room found; already committed (from the reservation, or acquired
    /// and `.forget()`-ed from `admit`).
    Accepted,
    /// Refused, `sundog_spill_dropped_total` already incremented under the
    /// applicable reason.
    RefusedFinal,
    /// Refused only because a `Reservation`'s remaining budget and
    /// `admit`'s headroom don't cover this record right now. No drop
    /// recorded; the caller may retry or fall back.
    RefusedPending,
}

/// Pre-lock flush-queue budget an apply call carries into
/// `apply_many_with_reservation`, threaded by `&mut` through every bucket
/// its eviction touches. [`Reservation::spend`] never awaits, so it's
/// safe to hold across a stripe write lock; unspent budget returns to the
/// semaphore on drop.
pub(crate) struct Reservation<'a> {
    admit: &'a Semaphore,
    remaining: u32,
}

impl Reservation<'_> {
    /// The pre-paid budget not yet spent.
    pub(crate) fn remaining(&self) -> u32 {
        self.remaining
    }

    /// Spends up to `bytes` from this reservation's pre-paid budget
    /// without touching the semaphore. Returns `true` with `remaining`
    /// decremented if covered; `false` and untouched otherwise, leaving
    /// the caller to fall back to `admit.try_acquire_many`. Never awaits.
    pub(crate) fn spend(&mut self, bytes: u32) -> bool {
        if self.remaining >= bytes {
            self.remaining -= bytes;
            true
        } else {
            false
        }
    }
}

impl Drop for Reservation<'_> {
    /// Returns any unspent budget to `admit`, overhead-free when a batch
    /// triggers no eviction.
    fn drop(&mut self) {
        self.admit.add_permits(self.remaining as usize);
    }
}

impl SpillTier {
    /// One positional read of the record at `loc`. `Ok(None)` when the
    /// region's generation has moved past `loc.generation`, since it
    /// rotated out from under this pointer, or the record fails its
    /// checksum, from a torn write or corruption. Both are ordinary,
    /// expected outcomes, not errors. `Err` only for a genuine I/O
    /// failure. Blocking: call from `spawn_blocking` or a dedicated
    /// thread, never inline in async code.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`io::Error`] if the positional read itself
    /// fails.
    pub(crate) fn read_at(&self, loc: SpillLoc) -> io::Result<Option<SpilledBytes>> {
        let Some(region) = self.inner.regions.get(loc.region as usize) else {
            return Ok(None);
        };
        if region.generation.load(Ordering::Acquire) != loc.generation {
            return Ok(None);
        }
        let mut buf = vec![0u8; loc.len as usize];
        pread_exact(&region.file, &mut buf, u64::from(loc.offset))?;
        // The region may rotate while this read is in flight; a generation
        // bump after the fact means these bytes may already belong to an
        // unrelated later record.
        if region.generation.load(Ordering::Acquire) != loc.generation {
            return Ok(None);
        }
        Ok(decode_record(&buf))
    }

    /// Live, un-reclaimed, bytes across all regions.
    pub(crate) fn bytes_used(&self) -> u64 {
        self.inner.bytes_used.load(Ordering::Acquire)
    }

    /// The cache name this tier is [`SpillTier::open`]ed under. The
    /// `cache` label every metric the spill module or its `SpillSink` caller,
    /// `engine::Engine`, publishes carries.
    pub(crate) fn cache_name(&self) -> &str {
        &self.inner.cache_name
    }

    /// Stops accepting new spills and drops the flusher's sender, so its
    /// `recv()` loop drains whatever is already queued and then exits on its
    /// own. Never joins the flusher thread: this must be safe to call from
    /// an async context without blocking it. Region file handles close via
    /// `Drop` once every clone of the shared inner state is gone. This
    /// alone never checkpoints; `warm_reopen` on is what makes
    /// `Shard::close_spill_checkpointed` pay that cost first, via
    /// [`SpillTier::checkpoint_flush`] and
    /// [`SpillTier::write_checkpoint_snapshot`].
    pub(crate) fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        *self.sender.lock() = None;
    }

    /// Whether [`SpillConfig::warm_reopen`] is on for this tier.
    pub(crate) fn warm_reopen_value(&self) -> bool {
        self.inner.warm_reopen
    }

    /// Whether [`SpillTier::close`] has run. Test-facing: production code
    /// only ever needs `try_spill`'s own `false` return to know a tier is
    /// unusable, never a direct closed check.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests;

/// Kani proofs over the spill tier's sizing arithmetic.
#[cfg(kani)]
mod kani_proofs;
