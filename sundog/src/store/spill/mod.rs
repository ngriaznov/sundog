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

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::hlc::Hlc;

use super::engine::OnRefusal;

mod config;
mod flusher;
mod record;
mod rules;
mod snapshot;
#[cfg(all(test, not(feature = "sim")))]
mod test_support;

pub use config::SpillConfig;

use flusher::rotate;
use record::{
    decode_record, decode_record_with_key, pread_exact, pwrite_all, record_checksum,
    region_file_name, remove_stale_region_files,
};
#[cfg(all(test, not(feature = "sim")))]
pub(crate) use rules::flush_queue_slots;
pub(crate) use rules::{
    keeps_refused_victim_resident, region_count_for, spill_record_len, spilled_is_current,
};
use rules::{next_region_index, record_too_large, records_fitting_region, refusal_drop_reason};
#[cfg(test)]
pub(crate) use snapshot::write_stale_snapshot_for_test;
use snapshot::{
    build_snapshot_bytes, delete_snapshot, remove_stale_snapshot_files, snapshot_eligibility,
    write_snapshot_atomic,
};
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
    /// Lifetime count of jobs [`write_segment`] abandoned rather than
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
    /// Opens, or reopens, the tier at `cfg.dir.join(cache_name)`.
    ///
    /// Every `*.reg` file already in that directory is removed, then
    /// `region_count_for(cfg.capacity_bytes, cfg.region_bytes_value())`
    /// fresh region files are created and preallocated to
    /// `cfg.region_bytes_value()` bytes each. The index lives only in RAM
    /// and starts empty on every call: bytes left over from a prior run
    /// are unreferenced by anything new, so nothing is ever read back
    /// from a previous incarnation's region files.
    ///
    /// Does not start the flusher thread. Call [`SpillTier::attach`] once
    /// the engine implementing [`SpillSink`] exists. `Shard::attach_spill`
    /// goes through [`SpillTier::reopen`] instead, whose cold-fallback
    /// calls `open_impl` directly, so nothing else still calls this.
    ///
    /// Under `feature = "sim"`, this still does real filesystem I/O and
    /// runs the flusher on a real OS thread: `sim` swaps only `net::tcp`'s
    /// transport for turmoil's, giving no determinism or virtual-time
    /// guarantee here, so `spill` and `sim` are never enabled together in
    /// this crate's CI.
    ///
    /// # Errors
    ///
    /// Returns an error if `cfg` fails [`SpillConfig::validate`], the
    /// directory cannot be created or listed, a stale `*.reg` file cannot be
    /// removed, or a region file cannot be created or preallocated.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn open(cfg: &SpillConfig, cache_name: &str) -> io::Result<Self> {
        Self::open_impl(cfg, cache_name, &[])
    }

    /// [`SpillTier::open`], but every region index in `readonly_regions` is
    /// reopened read-only after being preallocated, so a later write to it
    /// fails deterministically, EBADF rather than any real filesystem
    /// permission, without needing root or leaving the test's own process
    /// unable to preallocate the file in the first place. Test-only: lets a
    /// test force [`write_segment`]'s write to fail and prove every job in
    /// that segment reaches [`SpillSink::abandon`].
    ///
    /// # Errors
    ///
    /// Same as [`SpillTier::open`].
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn open_with_readonly_regions_for_test(
        cfg: &SpillConfig,
        cache_name: &str,
        readonly_regions: &[u32],
    ) -> io::Result<Self> {
        Self::open_impl(cfg, cache_name, readonly_regions)
    }

    fn open_impl(
        cfg: &SpillConfig,
        cache_name: &str,
        readonly_regions: &[u32],
    ) -> io::Result<Self> {
        cfg.validate()
            .map_err(|reason| io::Error::new(io::ErrorKind::InvalidInput, reason))?;

        let dir = cfg.dir.join(cache_name);
        fs::create_dir_all(&dir)?;
        remove_stale_region_files(&dir)?;
        remove_stale_snapshot_files(&dir)?;

        let region_bytes = cfg.region_bytes_value();
        // `validate` already guarantees this fits; the fallback keeps this
        // conversion total rather than panicking on a config this module did
        // not itself validate, such as a direct, non-`validate`d test caller.
        let region_bytes_u32 = u32::try_from(region_bytes).unwrap_or(u32::MAX);
        let region_count = region_count_for(cfg.capacity_bytes, region_bytes).max(2);

        let mut regions = Vec::with_capacity(region_count as usize);
        for idx in 0..region_count {
            let path = dir.join(region_file_name(idx));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            file.set_len(region_bytes)?;
            let file = if readonly_regions.contains(&idx) {
                drop(file);
                OpenOptions::new().read(true).write(false).open(&path)?
            } else {
                file
            };
            regions.push(RegionState {
                file,
                write_cursor: AtomicU32::new(0),
                generation: AtomicU32::new(0),
                used_bytes: AtomicU64::new(0),
                reverse_index: Mutex::new(Vec::new()),
            });
        }

        let flush_queue_bytes = cfg.flush_queue_bytes_value();
        // `validate` bounds this well under any real disk budget; the
        // fallback keeps it total rather than panicking on an unvalidated
        // config.
        let admit_permits = usize::try_from(flush_queue_bytes).unwrap_or(usize::MAX);
        let inner = Arc::new(Inner {
            regions: regions.into_boxed_slice(),
            region_bytes: region_bytes_u32,
            active: AtomicU32::new(0),
            bytes_used: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            cache_name: cache_name.to_string(),
            dir,
            warm_reopen: cfg.warm_reopen_value(),
            flush_queue_bytes,
            spill_wait_timeout: cfg.spill_wait_timeout_value(),
            admit: Semaphore::new(admit_permits),
            keep_resident_when_refused: AtomicBool::new(false),
            #[cfg(test)]
            flusher_paused: AtomicBool::new(false),
            flusher_jobs_drained: AtomicU64::new(0),
            flusher_jobs_abandoned: AtomicU64::new(0),
        });

        Ok(Self {
            inner,
            sender: Mutex::new(None),
            flusher_handle: Mutex::new(None),
        })
    }

    /// Sets this tier's refusal policy: whether a resident victim whose
    /// hand-off [`SpillTier::would_accept_under`] or [`SpillTier::enqueue`]
    /// declines is left resident, to be retried on a later eviction pass,
    /// rather than the caller falling back to a delete. `Shard::attach_spill`
    /// calls this once, right after [`SpillTier::open`], from the shard's
    /// `Mode`: `Mode::Replicated` and `Mode::Distributed` pass `true`, since
    /// every peer or co-owner still holds the entry and a local delete would
    /// have anti-entropy repair it back in; the other modes pass `false`, the
    /// delete fallback that keeps RAM bounded with no repair loop to guard
    /// against. A promotion's eviction overrides this flag with
    /// [`OnRefusal::KeepResident`]: a spilled-key read never deletes an entry
    /// in any mode. Also steers which reason
    /// [`SpillTier::would_accept_under`]/[`SpillTier::enqueue`] record a
    /// refusal under: `"deferred"` in place of `"too_large"`/`"closed"`/
    /// `"queue_full"` once this is `true`, so an operator sees backpressure
    /// rather than a delete reason that fails to describe what happened.
    pub(crate) fn set_keep_resident_when_refused(&self, keep: bool) {
        self.inner
            .keep_resident_when_refused
            .store(keep, Ordering::Release);
    }

    /// Whether [`SpillTier::set_keep_resident_when_refused`] set this
    /// tier's policy to leave a refused victim resident. Read by
    /// `engine::Engine::try_spill_victim` to decide a refused victim's
    /// fate, through [`keeps_refused_victim_resident`], and by
    /// [`SpillTier::would_accept_under`] and [`SpillTier::enqueue`] for
    /// their refusal-reason bookkeeping, through [`refusal_drop_reason`].
    pub(crate) fn keep_resident_when_refused(&self) -> bool {
        self.inner
            .keep_resident_when_refused
            .load(Ordering::Acquire)
    }

    /// The reason a refusal of `specific` records under for a pass running
    /// `on_refusal`: [`refusal_drop_reason`] of
    /// [`keeps_refused_victim_resident`] and this tier's own policy.
    fn refusal_reason(&self, on_refusal: OnRefusal, specific: &'static str) -> &'static str {
        refusal_drop_reason(
            keeps_refused_victim_resident(on_refusal, self.keep_resident_when_refused()),
            specific,
        )
    }

    /// Non-blocking, best-effort: `false` means the record can never fit any
    /// region, `reason = "too_large"`, or [`SpillTier::close`] has run,
    /// `reason = "closed"`, or the flusher's queue has no room, or none is
    /// attached, `reason = "queue_full"` either way. The caller
    /// must fall back to an unconditional delete. Never touches disk on
    /// this call, so it is safe to call while holding a stripe write
    /// lock.
    ///
    /// Built from [`SpillTier::would_accept`] and [`SpillTier::enqueue`],
    /// which `Engine::try_spill_victim` calls separately instead: the first,
    /// cheap and lock-hold-safe, decides eviction's fallback right there
    /// under the stripe lock, while the second, the only part that touches
    /// the channel, runs after the lock is released. See the module docs.
    /// Test-only: production has exactly one caller of either half, and it
    /// needs them split.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn try_spill(&self, job: SpillJob) -> bool {
        if self.would_accept(None, job.key_bytes.len(), job.encoded.len()) != Admission::Accepted {
            return false;
        }
        self.enqueue(OnRefusal::TierPolicy, job).is_ok()
    }

    /// [`SpillTier::would_accept_under`] with [`OnRefusal::TierPolicy`]:
    /// the tier's own flag decides which reason a refusal records.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the tier-policy entry point the direct tests call; production calls \
                      `would_accept_under`"
        )
    )]
    pub(crate) fn would_accept(
        &self,
        reservation: Option<&mut Reservation<'_>>,
        key_len: usize,
        value_len: usize,
    ) -> Admission {
        self.would_accept_under(OnRefusal::TierPolicy, reservation, key_len, value_len)
    }

    /// Whether a record built from `key_len` and `value_len` bytes could be
    /// admitted right now: not closed, small enough for a region, and
    /// within `flush_queue_bytes`. `reservation`, when given, is spent
    /// first via [`Reservation::spend`]; otherwise this falls back to
    /// `admit.try_acquire_many`. Never awaits, so, unlike
    /// [`SpillTier::enqueue`], this is cheap enough to call under a stripe
    /// write lock. [`Admission::RefusedFinal`] records a drop under
    /// `sundog_spill_dropped_total` (`"too_large"`, `"closed"`,
    /// `"queue_full"`, or `"deferred"` when the caller leaves the victim
    /// resident: [`keeps_refused_victim_resident`] of `on_refusal` and the
    /// tier's [`SpillTier::set_keep_resident_when_refused`] flag) -- always
    /// for `closed`/`too_large`, and for `queue_full` when `reservation` is
    /// `None`. [`Admission::RefusedPending`] is reachable only with an
    /// insufficient `reservation` and a drained `admit`; no drop is
    /// recorded, and the caller retries or falls back later.
    pub(crate) fn would_accept_under(
        &self,
        on_refusal: OnRefusal,
        reservation: Option<&mut Reservation<'_>>,
        key_len: usize,
        value_len: usize,
    ) -> Admission {
        let refusal_reason = |specific: &'static str| self.refusal_reason(on_refusal, specific);
        if self.inner.closed.load(Ordering::Acquire) {
            self.inner.record_dropped(refusal_reason("closed"));
            return Admission::RefusedFinal;
        }
        let record_len_u64 = HEADER_LEN as u64 + key_len as u64 + value_len as u64;
        if record_too_large(record_len_u64, u64::from(self.inner.region_bytes)) {
            self.inner.record_dropped(refusal_reason("too_large"));
            return Admission::RefusedFinal;
        }
        let record_len = spill_record_len(key_len, value_len);
        let reservation_in_play = reservation.is_some();
        if let Some(reservation) = reservation
            && reservation.spend(record_len)
        {
            return Admission::Accepted;
        }
        if let Ok(permit) = self.inner.admit.try_acquire_many(record_len) {
            permit.forget();
            Admission::Accepted
        } else if reservation_in_play {
            Admission::RefusedPending
        } else {
            self.inner.record_dropped(refusal_reason("queue_full"));
            Admission::RefusedFinal
        }
    }

    /// This tier's configured [`SpillConfig::spill_wait_timeout`], exposed
    /// here since `SpillConfig` isn't retained past [`SpillTier::open`].
    pub(crate) fn spill_wait_timeout_value(&self) -> Duration {
        self.inner.spill_wait_timeout
    }

    /// Waits, at most `timeout`, for `bytes` of flush-queue admission to
    /// become free, then commits them to the returned [`Reservation`]: the
    /// acquired permits are `.forget()`-ed on arrival, so they stop
    /// counting as available before anything is queued. `bytes` is clamped
    /// to this tier's total permit count so a request larger than the tier
    /// could ever hold does not await forever. Called once per apply-batch
    /// call, strictly before any stripe lock.
    /// `Duration::ZERO` degenerates to a single non-blocking check,
    /// reproducing today's instant-refuse behavior.
    ///
    /// # Errors
    ///
    /// Returns [`SpillWaitTimedOut`] once `timeout` elapses with the
    /// requested bytes still unavailable; the caller falls back to
    /// `reservation: None` for the rest of that call.
    pub(crate) async fn reserve(
        &self,
        bytes: u32,
        timeout: Duration,
    ) -> Result<Reservation<'_>, SpillWaitTimedOut> {
        let clamped = bytes.min(self.whole_queue_permits());
        let started = Instant::now();
        // Covers every exit, including a dropped future, since
        // `_waiter`'s Drop always runs.
        let _waiter = self.inner.record_waiter_delta();
        // Not the owned variant: nothing here needs to outlive this call.
        let outcome = tokio::time::timeout(timeout, self.inner.admit.acquire_many(clamped)).await;
        self.inner.record_wait_seconds(started.elapsed());
        match outcome {
            Ok(Ok(permit)) => {
                permit.forget();
                Ok(Reservation {
                    admit: &self.inner.admit,
                    remaining: clamped,
                })
            }
            // Unreachable, since `admit` is never closed; treated as a
            // timeout since the fallback is identical, but not counted
            // under `sundog_spill_wait_timeouts_total`.
            Ok(Err(_)) => Err(SpillWaitTimedOut),
            Err(_) => {
                self.inner.record_wait_timeout();
                Err(SpillWaitTimedOut)
            }
        }
    }

    /// The most [`SpillTier::reserve`] ever grants: one permit per byte of
    /// `flush_queue_bytes`, saturating at `u32::MAX`.
    pub(crate) fn whole_queue_permits(&self) -> u32 {
        u32::try_from(self.inner.flush_queue_bytes).unwrap_or(u32::MAX)
    }

    /// Returns `bytes` of admitted-but-never-queued capacity to `admit`.
    /// Called by `finish_spill_handoff`'s `Err` branch before `abandon`
    /// restores the victim's weight, so an unqueued job doesn't
    /// permanently shrink the admission budget.
    pub(crate) fn release(&self, bytes: u32) {
        self.inner.admit.add_permits(bytes as usize);
    }

    /// Enqueues `job` onto the flusher's channel. Callers that already
    /// separately checked [`SpillTier::would_accept_under`] get `job` back
    /// in `Err` on failure, since a full or missing channel is the only way
    /// this can fail; a caller with no further use for the job on failure
    /// can discard it, exactly like `try_spill`'s plain `bool`. `reason =
    /// "queue_full"` covers both a full queue and one with none
    /// attached, or `reason = "deferred"` in either case when the caller
    /// leaves the victim resident: `on_refusal` is
    /// [`OnRefusal::KeepResident`], or the tier's
    /// [`SpillTier::set_keep_resident_when_refused`] policy is set. Never
    /// touches disk, but does take the channel's own lock, so, unlike
    /// [`SpillTier::would_accept_under`], this is not meant to run while
    /// holding a stripe write lock. The permits `would_accept_under`
    /// admitted for `job` are already the caller's; this call touches no
    /// counter of its own, since only [`flusher_loop`] returns them via
    /// `admit.add_permits` once the job is pulled off the channel.
    pub(crate) fn enqueue(
        &self,
        on_refusal: OnRefusal,
        job: SpillJob,
    ) -> Result<(), Box<SpillJob>> {
        let sent = {
            let sender = self.sender.lock();
            let Some(tx) = sender.as_ref() else {
                self.inner
                    .record_dropped(self.refusal_reason(on_refusal, "queue_full"));
                return Err(Box::new(job));
            };
            tx.try_send(job)
        };
        match sent {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(job) | mpsc::TrySendError::Disconnected(job)) => {
                self.inner
                    .record_dropped(self.refusal_reason(on_refusal, "queue_full"));
                Err(Box::new(job))
            }
        }
    }

    /// Bytes currently sitting in the flusher's channel, queued but not yet
    /// taken off it: recomputed from `admit`'s available permits.
    /// Test-facing.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn queued_bytes(&self) -> u64 {
        let available = u64::try_from(self.inner.admit.available_permits()).unwrap_or(u64::MAX);
        self.inner.flush_queue_bytes.saturating_sub(available)
    }

    /// Test-only: blocks [`flusher_loop`] before it pulls its next job off
    /// the channel, so a job's bytes stay counted in `queued_bytes` (and,
    /// upstream, a hand-off's weight stays in
    /// `crate::store::engine::Engine::pending_spill_weight`) for as long as
    /// the test needs. [`SpillTier::resume_flusher`] undoes this.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn pause_flusher(&self) {
        self.inner.flusher_paused.store(true, Ordering::Release);
    }

    /// Undoes [`SpillTier::pause_flusher`].
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn resume_flusher(&self) {
        self.inner.flusher_paused.store(false, Ordering::Release);
    }

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
    /// `cache` label every metric this module or its `SpillSink` caller,
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

    /// The checkpoint half of a warm-reopen-enabled close: blocks until the
    /// flusher thread [`SpillTier::attach`] started drains its queue and
    /// exits (the only place this module joins that thread; run this in a
    /// blocking context), then writes `entries`, the caller's
    /// currently-resident records, into the region ring via
    /// [`flush_batch`]'s framing, without calling [`SpillSink::install`]
    /// since none of their weight was zeroed for a pending spill; only the
    /// resulting [`SpillLoc`] matters. Also reports `jobs_drained` and
    /// `jobs_abandoned` as diffs of [`Inner`]'s lifetime counters taken
    /// around the join.
    pub(crate) fn checkpoint_flush(
        &self,
        sink: &dyn SpillSink,
        entries: Vec<(Bytes, Hlc, Option<u64>, Bytes)>,
    ) -> CheckpointFlushOutcome {
        *self.sender.lock() = None;
        let drained_before = self.inner.flusher_jobs_drained.load(Ordering::Relaxed);
        let abandoned_before = self.inner.flusher_jobs_abandoned.load(Ordering::Relaxed);
        if let Some(handle) = self.flusher_handle.lock().take() {
            let _ = handle.join();
        }
        let jobs_drained = self
            .inner
            .flusher_jobs_drained
            .load(Ordering::Relaxed)
            .saturating_sub(drained_before);
        let jobs_abandoned = self
            .inner
            .flusher_jobs_abandoned
            .load(Ordering::Relaxed)
            .saturating_sub(abandoned_before);
        let written = if entries.is_empty() {
            Vec::new()
        } else {
            checkpoint_write_resident(&self.inner, sink, entries)
        };
        CheckpointFlushOutcome {
            written,
            jobs_drained,
            jobs_abandoned,
        }
    }

    /// Serializes `entries`, every live spilled pointer the caller's
    /// checkpoint gathered, into this directory's checkpoint snapshot file
    /// atomically (see [`write_snapshot_atomic`]). Called once, right
    /// before [`SpillTier::close`], only when `warm_reopen` is on.
    pub(crate) fn write_checkpoint_snapshot(
        &self,
        entries: &[(Bytes, Hlc, Option<u64>, SpillLoc)],
        now_ms: u64,
    ) {
        let region_bytes = u64::from(self.inner.region_bytes);
        let region_count = u32::try_from(self.inner.regions.len()).unwrap_or(u32::MAX);
        let bytes = build_snapshot_bytes(entries, region_bytes, region_count, now_ms);
        write_snapshot_atomic(&self.inner.dir, &bytes);
    }

    /// Whether [`SpillTier::close`] has run. Test-facing: production code
    /// only ever needs `try_spill`'s own `false` return to know a tier is
    /// unusable, never a direct closed check.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Opens the tier at `cfg.dir.join(cache_name)`, preferring to replay a
    /// clean close's checkpoint snapshot over [`SpillTier::open`]'s cold
    /// path. Never fails outright over an ineligible or corrupt snapshot:
    /// every such case falls back to [`SpillTier::open_impl`], so this
    /// returns `Err` only on a genuine I/O failure even the cold fallback
    /// would hit. [`ReopenOutcome::reason`] names whichever eligibility
    /// check first rules out a warm reopen:
    ///
    /// 1. `"disabled"`: [`SpillConfig::warm_reopen_value`] is `false`.
    /// 2. `"no_snapshot"`: no trusted snapshot (missing, truncated, bad
    ///    checksum).
    /// 3. `"stale_snapshot"`: an older [`SNAPSHOT_FORMAT_VERSION`].
    /// 4. `"config_mismatch"`: `region_bytes`/`region_count` don't match
    ///    `cfg`, meaning the tier was resized.
    /// 5. `"downtime_exceeded"`: closed longer ago than `tombstone_ttl_ms`.
    /// 6. `"bad_region"`: a region or entry fails to validate; one bad
    ///    entry falls the whole tier back cold.
    ///
    /// Past those checks, this replays only the snapshot. Each surviving
    /// entry is dropped if `owned` disallows its part or `expires_at_ms`
    /// is past `now_ms`, and otherwise installed via
    /// [`SpillSink::install_new`]. The snapshot is deleted on success, so
    /// a second open with no intervening clean close is cold.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`io::Error`] only if the cold fallback
    /// itself fails; see [`SpillTier::open`].
    #[allow(
        clippy::too_many_lines,
        reason = "one scripted eligibility-then-replay sequence, logging each stage in order"
    )]
    pub(crate) fn reopen(
        cfg: &SpillConfig,
        cache_name: &str,
        sink: &dyn SpillSink,
        now_ms: u64,
        tombstone_ttl_ms: u64,
        owned: impl Fn(crate::store::PartId) -> bool,
    ) -> io::Result<ReopenOutcome> {
        cfg.validate()
            .map_err(|reason| io::Error::new(io::ErrorKind::InvalidInput, reason))?;

        if !cfg.warm_reopen_value() {
            return Self::reopen_cold_fallback(cfg, cache_name, "disabled", 0, 0);
        }

        let dir = cfg.dir.join(cache_name);
        let region_bytes = cfg.region_bytes_value();
        // See open_impl's copy of this conversion.
        let region_bytes_u32 = u32::try_from(region_bytes).unwrap_or(u32::MAX);
        let region_count = region_count_for(cfg.capacity_bytes, region_bytes).max(2);

        let (_header, snapshot_entries) = match snapshot_eligibility(
            &dir,
            region_bytes,
            region_count,
            now_ms,
            tombstone_ttl_ms,
        ) {
            Ok(snapshot) => snapshot,
            Err(reason) => return Self::reopen_cold_fallback(cfg, cache_name, reason, 0, 0),
        };

        let entries_read = u64::try_from(snapshot_entries.len()).unwrap_or(u64::MAX);
        tracing::info!(
            cache = %cache_name,
            count = entries_read,
            "sundog spill: reopen read entries from the snapshot",
        );

        let region_files = match open_regions_for_replay(&dir, region_count, region_bytes_u32) {
            Ok(region_files) => region_files,
            Err(err) => {
                tracing::warn!(
                    cache = %cache_name,
                    error = %err,
                    "sundog spill: warm reopen could not reopen a region; falling back cold",
                );
                return Self::reopen_cold_fallback(cfg, cache_name, "bad_region", entries_read, 0);
            }
        };

        let (region_states, dropped_bad) = validate_snapshot_entries(
            &region_files,
            region_bytes_u32,
            region_count,
            &snapshot_entries,
        );
        let Some(region_states) = region_states else {
            tracing::info!(
                cache = %cache_name,
                dropped_bad,
                "sundog spill: reopen dropped entries as bad; falling back cold",
            );
            return Self::reopen_cold_fallback(
                cfg,
                cache_name,
                "bad_region",
                entries_read,
                dropped_bad,
            );
        };

        let InstalledRecords {
            reverse_index_by_region,
            used_bytes_by_region,
            records_installed,
            parts_installed,
            dropped_unowned,
            dropped_expired,
            refused_present,
        } = install_snapshot_entries(snapshot_entries, region_count, sink, now_ms, owned);
        tracing::info!(
            cache = %cache_name,
            dropped_unowned,
            dropped_expired,
            installed = records_installed,
            refused_present,
            "sundog spill: reopen installed snapshot entries",
        );

        let mut bytes_used_total = 0u64;
        let mut regions = Vec::with_capacity(region_count as usize);
        for (((file, state), used), reverse_index) in region_files
            .into_iter()
            .zip(region_states)
            .zip(used_bytes_by_region)
            .zip(reverse_index_by_region)
        {
            bytes_used_total += used;
            regions.push(RegionState {
                file,
                write_cursor: AtomicU32::new(state.write_cursor),
                generation: AtomicU32::new(state.generation),
                used_bytes: AtomicU64::new(used),
                reverse_index: Mutex::new(reverse_index),
            });
        }

        let flush_queue_bytes = cfg.flush_queue_bytes_value();
        // See open_impl's copy of this conversion.
        let admit_permits = usize::try_from(flush_queue_bytes).unwrap_or(usize::MAX);

        let inner = Arc::new(Inner {
            regions: regions.into_boxed_slice(),
            region_bytes: region_bytes_u32,
            active: AtomicU32::new(0),
            bytes_used: AtomicU64::new(bytes_used_total),
            closed: AtomicBool::new(false),
            cache_name: cache_name.to_string(),
            dir: dir.clone(),
            warm_reopen: cfg.warm_reopen_value(),
            flush_queue_bytes,
            spill_wait_timeout: cfg.spill_wait_timeout_value(),
            admit: Semaphore::new(admit_permits),
            keep_resident_when_refused: AtomicBool::new(false),
            #[cfg(test)]
            flusher_paused: AtomicBool::new(false),
            flusher_jobs_drained: AtomicU64::new(0),
            flusher_jobs_abandoned: AtomicU64::new(0),
        });

        // Only safe to remove now that every entry is installed; a crash
        // before this line leaves it for the next attempt to replay.
        delete_snapshot(&dir);

        Ok(ReopenOutcome {
            tier: Self {
                inner,
                sender: Mutex::new(None),
                flusher_handle: Mutex::new(None),
            },
            warm: true,
            reason: None,
            records_installed,
            warm_parts: parts_installed,
            entries_read,
            dropped_unowned,
            dropped_expired,
            dropped_bad,
            refused_present,
        })
    }

    /// [`SpillTier::reopen`]'s fallback: [`SpillTier::open_impl`] wrapped
    /// in a [`ReopenOutcome`] naming why the warm path failed.
    /// `entries_read`/`dropped_bad` carry whatever was already known; `0`
    /// for a reason that never got that far.
    fn reopen_cold_fallback(
        cfg: &SpillConfig,
        cache_name: &str,
        reason: &'static str,
        entries_read: u64,
        dropped_bad: u64,
    ) -> io::Result<ReopenOutcome> {
        let tier = Self::open_impl(cfg, cache_name, &[])?;
        Ok(ReopenOutcome {
            tier,
            warm: false,
            reason: Some(reason),
            records_installed: 0,
            warm_parts: HashSet::new(),
            entries_read,
            dropped_unowned: 0,
            dropped_expired: 0,
            dropped_bad,
            refused_present: 0,
        })
    }
}

/// [`SpillTier::reopen`]'s outcome: the tier itself, ready to use like
/// [`SpillTier::open`]'s, alongside whether the warm path was taken and,
/// when not, [`ReopenOutcome::reason`] naming why. Every count below is
/// `0` (and `warm_parts` empty) for a cold fallback.
pub(crate) struct ReopenOutcome {
    pub(crate) tier: SpillTier,
    /// `true` once every eligibility check passed and every snapshot
    /// entry validated.
    pub(crate) warm: bool,
    /// `None` for a warm reopen; otherwise the reason a cold fallback was
    /// taken. See [`SpillTier::reopen`]'s docs for the exact set.
    pub(crate) reason: Option<&'static str>,
    /// Records [`SpillSink::install_new`] accepted during a warm replay.
    pub(crate) records_installed: u64,
    /// Distinct parts `records_installed` came from, so the caller can
    /// narrow eager reconciliation to these parts instead of every owned
    /// part.
    pub(crate) warm_parts: HashSet<crate::store::PartId>,
    /// Total entries the snapshot named, before any filter ran.
    pub(crate) entries_read: u64,
    /// Entries dropped because `owned` no longer allows their part.
    pub(crate) dropped_unowned: u64,
    /// Entries dropped because their `expires_at_ms` was already past
    /// `now_ms`.
    pub(crate) dropped_expired: u64,
    /// Entries [`validate_snapshot_entries`] rejected; nonzero here is why
    /// this reopen fell back cold under `reason = "bad_region"`.
    pub(crate) dropped_bad: u64,
    /// Entries [`SpillSink::install_new`] refused because the key was
    /// already present, live or tombstoned.
    pub(crate) refused_present: u64,
}

/// Opens every region file `0..region_count` in `dir` for continued
/// read/write, no truncate. Never reads a region's content;
/// [`validate_snapshot_entries`] reads only the byte ranges the snapshot
/// names.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] if a region file cannot be opened
/// or its length doesn't match `region_bytes`; [`SpillTier::reopen`] then
/// falls the whole tier back cold, never a partial warm reopen.
fn open_regions_for_replay(
    dir: &Path,
    region_count: u32,
    region_bytes: u32,
) -> io::Result<Vec<File>> {
    let mut files = Vec::with_capacity(region_count as usize);
    for idx in 0..region_count {
        let path = dir.join(region_file_name(idx));
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let len = file.metadata()?.len();
        if len != u64::from(region_bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "region file size does not match the configured region_bytes",
            ));
        }
        files.push(file);
    }
    Ok(files)
}

/// One region's replayed cursor/generation, derived from the snapshot
/// entries naming it: `write_cursor` is the highest `offset + len` among
/// them, `generation` their shared value. A region no entry names
/// defaults to `0`/`0`, like a freshly opened one.
#[derive(Clone, Copy, Default)]
struct RegionReplayState {
    generation: u32,
    write_cursor: u32,
}

/// Where a snapshot entry at `loc` ends in its region, or `None` when its
/// region is not one of `region_count` or its bytes run past
/// `region_bytes`.
fn snapshot_loc_end(loc: SpillLoc, region_bytes: u32, region_count: u32) -> Option<u32> {
    if loc.region >= region_count {
        return None;
    }
    loc.offset
        .checked_add(loc.len)
        .filter(|&end| end <= region_bytes)
}

/// Validates each `entries` item: `region` must be real, `offset + len`
/// must fit `region_bytes`, [`decode_record_with_key`] at that offset must
/// match the entry's key/version, and every entry naming a region must
/// agree on its generation. Scans all of them so the returned count is
/// exact for `sundog_spill_reopen_entries_total{stage="dropped_bad"}`; the
/// first element is `None` once that count is nonzero (one bad entry
/// falls the whole tier back cold), `Some` otherwise, naming every
/// region's replayed cursor and generation.
fn validate_snapshot_entries(
    region_files: &[File],
    region_bytes: u32,
    region_count: u32,
    entries: &[SnapshotEntry],
) -> (Option<Vec<RegionReplayState>>, u64) {
    let mut states: Vec<Option<RegionReplayState>> = vec![None; region_count as usize];
    let mut bad = 0u64;
    for entry in entries {
        let valid = (|| -> Option<()> {
            let region_idx = entry.loc.region;
            let end = snapshot_loc_end(entry.loc, region_bytes, region_count)?;
            let file = region_files.get(region_idx as usize)?;
            let mut buf = vec![0u8; entry.loc.len as usize];
            pread_exact(file, &mut buf, u64::from(entry.loc.offset)).ok()?;
            let (decoded_key, decoded) = decode_record_with_key(&buf)?;
            if decoded_key != entry.key || decoded.ver != entry.ver {
                return None;
            }
            let slot = &mut states[region_idx as usize];
            match slot {
                Some(existing) if existing.generation == entry.loc.generation => {
                    existing.write_cursor = existing.write_cursor.max(end);
                }
                Some(_) => return None,
                None => {
                    *slot = Some(RegionReplayState {
                        generation: entry.loc.generation,
                        write_cursor: end,
                    });
                }
            }
            Some(())
        })();
        if valid.is_none() {
            bad += 1;
        }
    }
    if bad > 0 {
        (None, bad)
    } else {
        (
            Some(states.into_iter().map(Option::unwrap_or_default).collect()),
            0,
        )
    }
}

/// [`install_snapshot_entries`]'s result: each region's reverse index and
/// used-byte total, the overall accepted count and the distinct parts it
/// came from, plus a per-reason breakdown of everything dropped instead.
struct InstalledRecords {
    reverse_index_by_region: Vec<Vec<ReclaimRef>>,
    used_bytes_by_region: Vec<u64>,
    records_installed: u64,
    parts_installed: HashSet<crate::store::PartId>,
    /// Entries dropped because `owned` no longer allows their part.
    dropped_unowned: u64,
    /// Entries dropped because their `expires_at_ms` was already past
    /// `now_ms`.
    dropped_expired: u64,
    /// Entries [`SpillSink::install_new`] refused because the key was
    /// already present, live or tombstoned.
    refused_present: u64,
}

/// Installs every validated snapshot entry via [`SpillSink::install_new`],
/// first dropping one whose `expires_at_ms` is already past `now_ms` or
/// whose part `owned` disallows.
fn install_snapshot_entries(
    entries: Vec<SnapshotEntry>,
    region_count: u32,
    sink: &dyn SpillSink,
    now_ms: u64,
    owned: impl Fn(crate::store::PartId) -> bool,
) -> InstalledRecords {
    let mut reverse_index_by_region: Vec<Vec<ReclaimRef>> =
        (0..region_count).map(|_| Vec::new()).collect();
    let mut used_bytes_by_region = vec![0u64; region_count as usize];
    let mut records_installed = 0u64;
    let mut parts_installed: HashSet<crate::store::PartId> = HashSet::new();
    let mut dropped_unowned = 0u64;
    let mut dropped_expired = 0u64;
    let mut refused_present = 0u64;

    for entry in entries {
        if entry
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
        {
            dropped_expired += 1;
            continue;
        }
        let hash = crate::store::engine::hash_key_bytes(&entry.key);
        let part = crate::store::PartId::from_hash(hash);
        if !owned(part) {
            dropped_unowned += 1;
            continue;
        }
        let stripe_idx = usize::from(part.bucket());
        if sink.install_new(
            stripe_idx,
            &entry.key,
            hash,
            entry.ver,
            entry.expires_at_ms,
            entry.loc,
            0,
        ) {
            records_installed += 1;
            parts_installed.insert(part);
            let region_idx = entry.loc.region as usize;
            used_bytes_by_region[region_idx] += u64::from(entry.loc.len);
            reverse_index_by_region[region_idx].push(ReclaimRef::new(
                stripe_idx,
                hash,
                entry.loc.offset,
            ));
        } else {
            refused_present += 1;
        }
    }

    InstalledRecords {
        reverse_index_by_region,
        used_bytes_by_region,
        records_installed,
        parts_installed,
        dropped_unowned,
        dropped_expired,
        refused_present,
    }
}

/// Writes `entries`, the checkpoint's currently-resident live records,
/// into the region ring using [`write_segment`]'s framing, batched by
/// [`records_fitting_region`] and rotated with [`rotate`], but never
/// through [`SpillSink::install`] (see [`SpillTier::checkpoint_flush`]).
/// An oversized entry is skipped; a failed region write is logged and its
/// entries skipped, rather than aborting the whole checkpoint. `entries`
/// can outgrow the ring, so a later write here can reuse a
/// region an earlier write in this call already used, which `rotate`'s
/// `sink.reclaim` can't see. `pending_keys_by_region` tracks the keys
/// this call wrote per region, so on reuse they retire into `stale_keys`
/// and get dropped from `out`, leaving only entries that survived to the
/// end of the call.
fn checkpoint_write_resident(
    inner: &Inner,
    sink: &dyn SpillSink,
    entries: Vec<(Bytes, Hlc, Option<u64>, Bytes)>,
) -> Vec<(Bytes, Hlc, Option<u64>, SpillLoc)> {
    let mut out = Vec::with_capacity(entries.len());
    let mut pending: Vec<(Bytes, Hlc, Option<u64>, Bytes)> = entries
        .into_iter()
        .filter(|(key, _, _, encoded)| {
            !record_too_large(
                (HEADER_LEN as u64) + key.len() as u64 + encoded.len() as u64,
                u64::from(inner.region_bytes),
            )
        })
        .collect();

    let region_count = u32::try_from(inner.regions.len()).unwrap_or(u32::MAX);
    let mut pending_keys_by_region: HashMap<u32, Vec<Bytes>> = HashMap::new();
    let mut stale_keys: HashSet<Bytes> = HashSet::new();
    let mut active = inner.active.load(Ordering::Acquire);
    while !pending.is_empty() {
        let region = &inner.regions[active as usize];
        let cursor = region.write_cursor.load(Ordering::Acquire);
        let generation = region.generation.load(Ordering::Acquire);
        let lens: Vec<u32> = pending
            .iter()
            .map(|(key, _, _, encoded)| spill_record_len(key.len(), encoded.len()))
            .collect();
        let take = records_fitting_region(cursor, inner.region_bytes, &lens);
        if take == 0 {
            // Keys this call already wrote to the region rotating in are
            // about to be overwritten.
            let next = next_region_index(active, region_count);
            if let Some(keys) = pending_keys_by_region.remove(&next) {
                stale_keys.extend(keys);
            }
            active = rotate(inner, sink, active);
            continue;
        }

        let segment: Vec<(Bytes, Hlc, Option<u64>, Bytes)> = pending.drain(..take).collect();
        let mut buf = Vec::new();
        let mut locs = Vec::with_capacity(segment.len());
        let mut write_cursor = cursor;
        for (key, ver, expires_at_ms, encoded) in &segment {
            let key_len = u32::try_from(key.len()).unwrap_or(u32::MAX);
            let value_len = u32::try_from(encoded.len()).unwrap_or(u32::MAX);
            let header = SpillRecordHeader {
                checksum: record_checksum(key, encoded),
                expires_at_ms: expires_at_ms.unwrap_or(u64::MAX),
                wall_ms: ver.wall_ms,
                node: ver.node.as_u64(),
                magic: SPILL_MAGIC,
                key_len,
                value_len,
                logical: ver.logical,
            };
            buf.extend_from_slice(header.as_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(encoded);
            let record_len = spill_record_len(key.len(), encoded.len());
            locs.push(SpillLoc {
                region: active,
                offset: write_cursor,
                len: record_len,
                generation,
            });
            write_cursor += record_len;
        }

        if let Err(err) = pwrite_all(&region.file, &buf, u64::from(cursor)) {
            tracing::warn!(
                cache = %inner.cache_name,
                error = %err,
                "sundog spill: checkpoint write failed for a segment; those entries are \
                 skipped",
            );
        } else {
            region.write_cursor.store(write_cursor, Ordering::Release);
            let mut installed_bytes = 0u64;
            for ((key, ver, expires_at_ms, _), loc) in segment.into_iter().zip(locs) {
                installed_bytes += u64::from(loc.len);
                pending_keys_by_region
                    .entry(active)
                    .or_default()
                    .push(key.clone());
                out.push((key, ver, expires_at_ms, loc));
            }
            region
                .used_bytes
                .fetch_add(installed_bytes, Ordering::AcqRel);
            inner
                .bytes_used
                .fetch_add(installed_bytes, Ordering::AcqRel);
        }

        if !pending.is_empty() {
            let next = next_region_index(active, region_count);
            if let Some(keys) = pending_keys_by_region.remove(&next) {
                stale_keys.extend(keys);
            }
            active = rotate(inner, sink, active);
        }
    }
    inner.active.store(active, Ordering::Release);
    inner.publish_bytes_used();
    if !stale_keys.is_empty() {
        out.retain(|(key, ..)| !stale_keys.contains(key));
    }
    out
}

#[cfg(test)]
mod tests;

/// Kani proofs over the spill tier's sizing arithmetic.
#[cfg(kani)]
mod kani_proofs;
