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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use xxhash_rust::xxh3::Xxh3Default;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::hlc::Hlc;
use crate::node::NodeId;

/// Region size a [`SpillConfig`] uses when [`SpillConfig::region_bytes`] is
/// never called.
const DEFAULT_REGION_BYTES: u64 = 64 * 1024 * 1024;
/// Concurrent-disk-read bound a [`SpillConfig`] uses when
/// [`SpillConfig::read_concurrency`] is never called. Consulted by the
/// engine's read path, `spawn_blocking` behind a semaphore of this size,
/// not by anything in this module.
const DEFAULT_READ_CONCURRENCY: usize = 16;
/// Wait bound used when [`SpillConfig::spill_wait_timeout`] is never
/// called: 2s, leaving margin over the ~8s
/// `state_transfer::per_donor_budget`.
const DEFAULT_SPILL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
/// Sanity cap [`SpillConfig::validate`] enforces on
/// [`SpillConfig::spill_wait_timeout`], so an absurd value errors at open
/// time rather than as a later rebalance failure.
const MAX_SPILL_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
/// Floor on the flusher's job queue slot count. [`SpillTier::attach`]
/// sizes the channel from `flush_queue_bytes_value() / HEADER_LEN`,
/// clamped between this and [`FLUSH_QUEUE_SLOTS_MAX`].
pub(crate) const FLUSH_QUEUE_CAPACITY: usize = 8192;
/// Ceiling [`SpillTier::attach`]'s byte-derived slot count clamps to,
/// guarding a huge `flush_queue_bytes` paired with tiny records from
/// allocating an oversized channel buffer.
pub(crate) const FLUSH_QUEUE_SLOTS_MAX: usize = 262_144;
/// Bound on how many jobs one flusher batch coalesces into as few
/// positional writes as its rotations require. After a blocking `recv`
/// returns the first job, [`flusher_loop`] drains the channel greedily with
/// `try_recv` until it hits this many jobs or [`FLUSH_BATCH_MAX_BYTES`],
/// whichever comes first, then hands the whole batch to [`flush_batch`].
const FLUSH_BATCH_MAX_JOBS: usize = 512;
/// Bound on the summed record length, header included, one flusher batch
/// coalesces before it stops draining and writes what it has. 1 MiB keeps a
/// batch's transient encode buffer small next to a typical page cache while
/// still amortizing the write syscall over hundreds of records.
const FLUSH_BATCH_MAX_BYTES: usize = 1024 * 1024;
/// Corruption/format-skew guard at the front of every [`SpillRecordHeader`].
/// Built from its ASCII bytes so the constant's value and its on-disk byte
/// order always agree: `SPILL_MAGIC.to_le_bytes() == *b"SPIL"`.
const SPILL_MAGIC: u32 = u32::from_le_bytes(*b"SPIL");
/// Fixed on-disk header size preceding every record's key and value bytes.
const HEADER_LEN: usize = size_of::<SpillRecordHeader>();
/// Corruption/format-skew guard at the front of every [`SnapshotHeader`],
/// distinct from [`SPILL_MAGIC`] so a snapshot and a region file can't be
/// confused even if misnamed.
const SNAPSHOT_MAGIC: u32 = u32::from_le_bytes(*b"SPLS");
/// Format version stamped into every snapshot, bumped when its binary
/// layout changes. A mismatch is `"stale_snapshot"`, distinct from
/// `"no_snapshot"` (fails to parse at all).
const SNAPSHOT_FORMAT_VERSION: u32 = 1;
/// File name a tier's checkpoint snapshot is written under, inside
/// `cfg.dir.join(cache_name)`.
const SNAPSHOT_FILE_NAME: &str = "snapshot";
/// Name [`write_snapshot_atomic`] writes bytes under before renaming to
/// [`SNAPSHOT_FILE_NAME`]; a crash in between leaves this file and no real
/// one, so the next reopen sees `"no_snapshot"`.
const SNAPSHOT_TMP_FILE_NAME: &str = "snapshot.tmp";
/// Byte length of every [`SnapshotHeader`]: `checksum`(8) + `magic`(4) +
/// `format_version`(4) + `region_bytes`(8) + `region_count`(4) +
/// `closed_at_ms`(8) + `entry_count`(8).
const SNAPSHOT_HEADER_LEN: usize = 44;
/// Byte length of one [`SnapshotEntry`]'s fixed portion, before its
/// variable-length key bytes: `wall_ms`(8) + `logical`(4) + `node`(8) +
/// `expires_at_ms`(8) + `region`(4) + `offset`(4) + `len`(4) +
/// `generation`(4) + `key_len`(4).
const SNAPSHOT_ENTRY_FIXED_LEN: usize = 48;

/// Disk budget and layout knobs for a cache's optional spill tier.
///
/// `dir`/`capacity_bytes` have no default, since a disk budget is never
/// safe to assume, but `region_bytes`, `read_concurrency`, and
/// `flush_queue_bytes` do. Construct with [`SpillConfig::new`] and adjust
/// any default with the matching builder method;
/// [`SpillConfig::region_bytes_value`], [`SpillConfig::read_concurrency_value`],
/// and [`SpillConfig::flush_queue_bytes_value`] read back whatever is in
/// effect. When the tier this config opens refuses a hand-off, a
/// `Mode::Replicated` cache keeps the victim resident, at its full weight,
/// for a later eviction pass to retry, while a `Mode::Local` or
/// `Mode::Invalidation` cache evicts it exactly as it always has.
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// Directory the tier's region files live under. `SpillTier::open`
    /// creates and owns a per-cache subdirectory inside it. Two caches
    /// never share a directory even when given the same `dir`.
    pub dir: PathBuf,
    /// Disk budget for this cache's spill tier, in bytes. Must be at least
    /// twice `region_bytes_value()`. See `SpillConfig::validate`, a
    /// crate-internal check called by `CacheBuilder::open`.
    pub capacity_bytes: u64,
    region_bytes: u64,
    read_concurrency: usize,
    /// `None` tracks `region_bytes`, one region's worth, the documented
    /// default; `Some` is an explicit [`SpillConfig::flush_queue_bytes`]
    /// override. Tracking rather than snapshotting `region_bytes` at
    /// [`SpillConfig::new`] time means a later [`SpillConfig::region_bytes`]
    /// call keeps this bound automatically at most half of
    /// `capacity_bytes`, since `validate` already requires `capacity_bytes`
    /// be at least twice `region_bytes`.
    flush_queue_bytes: Option<u64>,
    /// See [`SpillConfig::spill_wait_timeout`]; read once by
    /// [`SpillTier::open`].
    spill_wait_timeout: Duration,
    /// See [`SpillConfig::warm_reopen`].
    warm_reopen: bool,
}

impl SpillConfig {
    /// Starts a config with the default `region_bytes`, 64 MiB,
    /// `read_concurrency`, 16, `flush_queue_bytes`, one region (so it
    /// tracks `region_bytes` for as long as neither is overridden), and
    /// `spill_wait_timeout`, two seconds.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, capacity_bytes: u64) -> Self {
        Self {
            dir: dir.into(),
            capacity_bytes,
            region_bytes: DEFAULT_REGION_BYTES,
            read_concurrency: DEFAULT_READ_CONCURRENCY,
            flush_queue_bytes: None,
            spill_wait_timeout: DEFAULT_SPILL_WAIT_TIMEOUT,
            warm_reopen: false,
        }
    }

    /// Overrides the per-region file size, default 64 MiB. Own-and-return.
    #[must_use]
    pub fn region_bytes(mut self, bytes: u64) -> Self {
        self.region_bytes = bytes;
        self
    }

    /// Overrides the bound on concurrent disk reads, default 16.
    /// Own-and-return.
    #[must_use]
    pub fn read_concurrency(mut self, n: usize) -> Self {
        self.read_concurrency = n;
        self
    }

    /// Overrides the bound on the flusher's queued-but-unwritten backlog,
    /// in bytes, default one region. A hand-off that would push the
    /// tier's queued bytes past this falls back to an ordinary delete
    /// (`sundog_spill_dropped_total{reason="queue_full"}`) instead of
    /// piling up fully-resident, unwritten values in RAM: this is what
    /// keeps a lagging disk a plain-eviction problem rather than an
    /// unbounded-RSS one. Own-and-return.
    #[must_use]
    pub fn flush_queue_bytes(mut self, bytes: u64) -> Self {
        self.flush_queue_bytes = Some(bytes);
        self
    }

    /// Overrides how long a bulk write or rebalance/replication chunk
    /// apply waits for flush-queue room before the immediate
    /// `queue_full`/`deferred` refuse. Default two seconds;
    /// `Duration::ZERO` disables the wait. Set against
    /// `cluster::state_transfer::per_donor_budget`: too large a value can
    /// turn a slow chunk into a spurious rebalance failure.
    #[must_use]
    pub fn spill_wait_timeout(mut self, timeout: Duration) -> Self {
        self.spill_wait_timeout = timeout;
        self
    }

    /// Whether `SpillTier::open` may replay a clean close's snapshot
    /// instead of the wipe-and-recreate cold path. Default `false`. See
    /// `SpillTier::reopen`'s docs for when it still falls back cold.
    #[must_use]
    pub fn warm_reopen(mut self, enabled: bool) -> Self {
        self.warm_reopen = enabled;
        self
    }

    /// The region size currently in effect.
    #[must_use]
    pub fn region_bytes_value(&self) -> u64 {
        self.region_bytes
    }

    /// The concurrent-read bound currently in effect.
    #[must_use]
    pub fn read_concurrency_value(&self) -> usize {
        self.read_concurrency
    }

    /// The flush-queue byte bound currently in effect: an explicit
    /// [`SpillConfig::flush_queue_bytes`] override, or `region_bytes_value()`
    /// otherwise.
    #[must_use]
    pub fn flush_queue_bytes_value(&self) -> u64 {
        self.flush_queue_bytes.unwrap_or(self.region_bytes)
    }

    /// This config's spill-wait timeout, default two seconds. See
    /// [`SpillConfig::spill_wait_timeout`].
    #[must_use]
    pub fn spill_wait_timeout_value(&self) -> Duration {
        self.spill_wait_timeout
    }

    /// This config's `warm_reopen` setting, default `false`. See
    /// [`SpillConfig::warm_reopen`].
    #[must_use]
    pub fn warm_reopen_value(&self) -> bool {
        self.warm_reopen
    }

    /// Checks this config before [`SpillTier::open`] uses it to open a tier.
    ///
    /// Rejects a zero `region_bytes`, a `region_bytes` too large to address
    /// with the tier's 32-bit on-disk offsets, a `capacity_bytes` less than
    /// twice `region_bytes`, a `flush_queue_bytes` too small to ever hold
    /// even the smallest possible record (header plus a zero-length key and
    /// value), a `flush_queue_bytes` bigger than `capacity_bytes` itself,
    /// and a `spill_wait_timeout` at or above the sanity cap
    /// `MAX_SPILL_WAIT_TIMEOUT` (`Duration::ZERO` remains a valid opt-out).
    /// The `capacity_bytes` rule prevents a hazard where a
    /// single region would be both the active writer and the only candidate
    /// for FIFO reclaim, so `next_region_index` would immediately reclaim the
    /// region it is currently writing to. With this held, `region_count_for`
    /// always resolves to at least two regions.
    ///
    /// # Errors
    ///
    /// Returns a static reason string naming the field that failed.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.region_bytes == 0 {
            return Err("region_bytes must be greater than zero");
        }
        if self.region_bytes > u64::from(u32::MAX) {
            return Err("region_bytes must fit in 32 bits");
        }
        if self.capacity_bytes < 2 * self.region_bytes {
            return Err("capacity_bytes must be at least twice region_bytes");
        }
        let flush_queue_bytes = self.flush_queue_bytes_value();
        if flush_queue_bytes < HEADER_LEN as u64 {
            return Err("flush_queue_bytes must hold at least one record");
        }
        if flush_queue_bytes > self.capacity_bytes {
            return Err("flush_queue_bytes must not exceed capacity_bytes");
        }
        if self.spill_wait_timeout >= MAX_SPILL_WAIT_TIMEOUT {
            return Err("spill_wait_timeout must be under the sanity cap");
        }
        Ok(())
    }
}

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
    /// write lock, remove every listed key whose payload is still
    /// `Spilled(loc)` with `loc.region == region && loc.generation ==
    /// generation`, XOR its fingerprint out of the digest, and decrement
    /// `live_count`. Returns the count removed.
    fn reclaim(&self, region: u32, generation: u32, keys: &[(usize, Bytes)]) -> usize;

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

/// Region count for a capacity/region-size pair. Pure; unit-tested directly.
/// [`SpillConfig::validate`] additionally requires the result be at least
/// 2, since a lone region would be both the active writer and the only
/// candidate for FIFO reclaim. This function itself stays a simple
/// division; the floor is enforced by the caller, [`SpillTier::open`].
pub(crate) fn region_count_for(capacity_bytes: u64, region_bytes: u64) -> u32 {
    u32::try_from((capacity_bytes / region_bytes.max(1)).max(1)).unwrap_or(u32::MAX)
}

/// Slot count for [`SpillTier::attach`]'s flush channel:
/// `flush_queue_bytes` divided by the header size, clamped to
/// `[FLUSH_QUEUE_CAPACITY, FLUSH_QUEUE_SLOTS_MAX]`, so larger records get
/// more slots than a fixed count would. Pure; unit tested directly.
pub(crate) fn flush_queue_slots(flush_queue_bytes: u64) -> usize {
    let slots = (flush_queue_bytes / HEADER_LEN as u64)
        .clamp(FLUSH_QUEUE_CAPACITY as u64, FLUSH_QUEUE_SLOTS_MAX as u64);
    // Bounded well within `usize` by the clamp; the fallback keeps this
    // total instead of panicking.
    usize::try_from(slots).unwrap_or(FLUSH_QUEUE_SLOTS_MAX)
}

/// Whether `record_len` more bytes fit in a region of `region_bytes` bytes
/// whose write cursor already sits at `write_cursor`. Checked arithmetic: a
/// pathological `record_len` never wraps into a false "yes".
pub(crate) fn record_fits(write_cursor: u32, region_bytes: u32, record_len: u32) -> bool {
    write_cursor
        .checked_add(record_len)
        .is_some_and(|end| end <= region_bytes)
}

/// Whether a record of `record_len` bytes, header plus key plus value,
/// could ever fit in *any* region of `region_bytes` bytes. `try_spill`
/// rejects a record that fails this before it is ever queued. No rotation
/// would help it.
pub(crate) fn record_too_large(record_len: u64, region_bytes: u64) -> bool {
    record_len > region_bytes
}

/// Whether a `record_len`-byte record fits in a `flush_queue_bytes`-byte
/// budget already holding `queued_bytes`. Test-only: pins the boundary and
/// overflow arithmetic `Inner::admit` performs internally. Pure.
#[cfg(test)]
pub(crate) fn record_fits_queue(
    queued_bytes: u64,
    record_len: u64,
    flush_queue_bytes: u64,
) -> bool {
    queued_bytes
        .checked_add(record_len)
        .is_some_and(|total| total <= flush_queue_bytes)
}

/// Total on-disk length of a `key_len`+`value_len`-byte record: header
/// plus key plus value, as a `u32`. Called by both `would_accept`'s
/// admission check and `try_spill_victim`'s bookkeeping so they agree on
/// the exact byte count. Saturates to `u32::MAX` on overflow. Pure; unit
/// tested directly.
pub(crate) fn spill_record_len(key_len: usize, value_len: usize) -> u32 {
    let total = (HEADER_LEN as u64)
        .saturating_add(key_len as u64)
        .saturating_add(value_len as u64);
    u32::try_from(total).unwrap_or(u32::MAX)
}

/// The `sundog_spill_dropped_total` reason a refusal is recorded under:
/// `"deferred"` when `keep_resident_when_refused` is set, since the caller
/// leaves the victim resident and retries it on a later eviction pass
/// rather than deleting it, so an operator sees backpressure instead of a
/// delete reason that fails to describe what happened; `specific`
/// (`"too_large"`, `"closed"`, or `"queue_full"`) unchanged otherwise. Pure;
/// unit tested directly.
pub(crate) fn refusal_drop_reason(
    keep_resident_when_refused: bool,
    specific: &'static str,
) -> &'static str {
    if keep_resident_when_refused {
        "deferred"
    } else {
        specific
    }
}

/// The flusher's batch-splitting rule: how many of `record_lens`, taken in
/// order, fit consecutively in a region of `region_bytes` bytes whose write
/// cursor already sits at `write_cursor`, before the first one that does
/// not. Returns `record_lens.len()` when every record fits. [`flush_batch`]
/// calls this once per region a batch touches: it writes the leading
/// `records_fitting_region(..)` records in one buffer, rotates, then calls
/// this again with `write_cursor` reset to `0` and the unconsumed remainder,
/// so a batch that spans a rotation still never lets a record straddle two
/// regions. Pure; unit-tested directly, including the exact-boundary and
/// spans-a-rotation cases.
pub(crate) fn records_fitting_region(
    write_cursor: u32,
    region_bytes: u32,
    record_lens: &[u32],
) -> usize {
    let mut cursor = write_cursor;
    for (taken, &len) in record_lens.iter().enumerate() {
        if !record_fits(cursor, region_bytes, len) {
            return taken;
        }
        // `record_fits` above already ruled out overflow for this add.
        cursor += len;
    }
    record_lens.len()
}

/// The next region in FIFO round-robin order after `current`. Pure, total.
/// Never returns `current` when `region_count >= 2`, enforced by
/// [`SpillConfig::validate`]. That keeps the active-write region and the
/// next-to-reclaim region always distinct.
pub(crate) fn next_region_index(current: u32, region_count: u32) -> u32 {
    (current + 1) % region_count.max(1)
}

/// Whether a record spilled at `spilled_ver` still describes the key's
/// current state: no tombstone, and a live entry at that version. Used by
/// the flusher's install and by promotion; both are no-ops when this is
/// `false`. A key missing from `live`, `stored_live_ver == None`, is never
/// re-added, and a tombstone or a differing live version always wins over
/// the stale flush.
pub(crate) fn spilled_is_current(
    stored_tombstone_ver: Option<Hlc>,
    stored_live_ver: Option<Hlc>,
    spilled_ver: Hlc,
) -> bool {
    stored_tombstone_ver.is_none() && stored_live_ver == Some(spilled_ver)
}

/// Per-region mutable state: the pre-opened file handle, one syscall per
/// read/write with no `open()` on the hot path, the write cursor, the
/// generation, and the reverse index of keys currently pointing into this
/// region. That index is populated on every install that returns `true`,
/// and drained whenever this region is reclaimed.
struct RegionState {
    file: File,
    write_cursor: AtomicU32,
    generation: AtomicU32,
    /// Bytes this region currently contributes to `Inner::bytes_used`.
    /// Reset to 0, and subtracted from the tier total, on reclaim.
    used_bytes: AtomicU64,
    reverse_index: Mutex<Vec<(usize, Bytes)>>,
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
    /// then. `true` means a refused hand-off ([`SpillTier::would_accept`]
    /// or [`SpillTier::enqueue`] declining) leaves its victim resident
    /// instead of the caller falling back to a delete: `Shard::attach_spill`
    /// sets this for a `Mode::Replicated` shard, where a local delete would
    /// have anti-entropy repair the entry back in from every peer that
    /// still holds it.
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
    /// recorded under this reason instead because
    /// [`SpillTier::set_keep_resident_when_refused`] set this tier's
    /// policy (see [`refusal_drop_reason`]); or `"disk_error"`, every job
    /// in a segment whose write failed, though `abandon` still restores
    /// the victim, making a failing disk visible here, not just in a log
    /// line.
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

/// [`SpillTier::would_accept`]'s outcome: not a plain `bool`, since a
/// caller holding a `Reservation` must tell a recorded refusal apart from
/// a transient one it may retry. See [`SpillTier::would_accept`]'s docs.
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

    /// Starts the flusher thread, fed by a fresh bounded channel.
    /// `try_spill` returns `false` for every call before this and every call
    /// after [`SpillTier::close`]. The flusher holds only `sink`, a `Weak`,
    /// and exits as soon as the upgrade fails, so the caller may drop its
    /// last strong reference at any time without joining anything.
    ///
    /// A second call replaces the channel: any job still buffered on the old
    /// one is dropped along with the old sender, since the old flusher
    /// thread's receiver is dropped too. Callers attach once, right after
    /// constructing the engine that implements [`SpillSink`]. The
    /// channel's slot count is [`flush_queue_slots`], so a config with
    /// small typical records gets a slot count sized to its byte budget
    /// instead of always [`FLUSH_QUEUE_CAPACITY`].
    pub(crate) fn attach(&self, sink: Weak<dyn SpillSink>) {
        let (tx, rx) = mpsc::sync_channel(flush_queue_slots(self.inner.flush_queue_bytes));
        let inner = Arc::clone(&self.inner);
        let name = format!("sundog-spill-{}", inner.cache_name);
        let handle = thread::Builder::new()
            .name(name)
            .spawn(move || flusher_loop(&inner, &rx, &sink))
            .ok();
        if let Some(handle) = handle {
            *self.sender.lock() = Some(tx);
            *self.flusher_handle.lock() = Some(handle);
        }
    }

    /// Sets this tier's refusal policy: whether a resident victim whose
    /// hand-off [`SpillTier::would_accept`] or [`SpillTier::enqueue`]
    /// declines is left resident, to be retried on a later eviction pass,
    /// rather than the caller falling back to a delete. `Shard::attach_spill`
    /// calls this once, right after [`SpillTier::open`], from the shard's
    /// `Mode`: `Mode::Replicated` passes `true`, since every peer still
    /// holds the entry and a local delete would have anti-entropy
    /// repair it back in; `Mode::Local`/`Mode::Invalidation` never call
    /// this, leaving the default of `false`, the delete fallback those
    /// modes need to keep RAM bounded with no repair loop to guard
    /// against. Also steers which reason
    /// [`SpillTier::would_accept`]/[`SpillTier::enqueue`] record a refusal
    /// under: `"deferred"` in place of `"too_large"`/`"closed"`/
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
    /// fate, and by this tier's own refusal-reason bookkeeping via
    /// [`refusal_drop_reason`].
    pub(crate) fn keep_resident_when_refused(&self) -> bool {
        self.inner
            .keep_resident_when_refused
            .load(Ordering::Acquire)
    }

    /// [`refusal_drop_reason`] bound to this tier's own policy: the reason
    /// [`SpillTier::would_accept`]/[`SpillTier::enqueue`] record a refusal
    /// of `specific` under.
    fn refusal_reason(&self, specific: &'static str) -> &'static str {
        refusal_drop_reason(self.keep_resident_when_refused(), specific)
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
        self.enqueue(job).is_ok()
    }

    /// Whether a record built from `key_len` and `value_len` bytes could be
    /// admitted right now: not closed, small enough for a region, and
    /// within `flush_queue_bytes`. `reservation`, when given, is spent
    /// first via [`Reservation::spend`]; otherwise this falls back to
    /// `admit.try_acquire_many`. Never awaits, so, unlike
    /// [`SpillTier::enqueue`], this is cheap enough to call under a stripe
    /// write lock. [`Admission::RefusedFinal`] records a drop under
    /// `sundog_spill_dropped_total` (`"too_large"`, `"closed"`,
    /// `"queue_full"`, or `"deferred"` once
    /// [`SpillTier::set_keep_resident_when_refused`] is set) -- always for
    /// `closed`/`too_large`, and for `queue_full` when `reservation` is
    /// `None`. [`Admission::RefusedPending`] is reachable only with an
    /// insufficient `reservation` and a drained `admit`; no drop is
    /// recorded, and the caller retries or falls back later.
    pub(crate) fn would_accept(
        &self,
        reservation: Option<&mut Reservation<'_>>,
        key_len: usize,
        value_len: usize,
    ) -> Admission {
        if self.inner.closed.load(Ordering::Acquire) {
            self.inner.record_dropped(self.refusal_reason("closed"));
            return Admission::RefusedFinal;
        }
        let record_len_u64 = HEADER_LEN as u64 + key_len as u64 + value_len as u64;
        if record_too_large(record_len_u64, u64::from(self.inner.region_bytes)) {
            self.inner.record_dropped(self.refusal_reason("too_large"));
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
            self.inner.record_dropped(self.refusal_reason("queue_full"));
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
        let total_permits = u32::try_from(self.inner.flush_queue_bytes).unwrap_or(u32::MAX);
        let clamped = bytes.min(total_permits);
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

    /// Returns `bytes` of admitted-but-never-queued capacity to `admit`.
    /// Called by `finish_spill_handoff`'s `Err` branch before `abandon`
    /// restores the victim's weight, so an unqueued job doesn't
    /// permanently shrink the admission budget.
    pub(crate) fn release(&self, bytes: u32) {
        self.inner.admit.add_permits(bytes as usize);
    }

    /// Enqueues `job` onto the flusher's channel. Callers that already
    /// separately checked [`SpillTier::would_accept`] get `job` back in
    /// `Err` on failure, since a full or missing channel is the only way
    /// this can fail; a caller with no further use for the job on failure
    /// can discard it, exactly like `try_spill`'s plain `bool`. `reason =
    /// "queue_full"` covers both a full queue and one with none
    /// attached, or `reason = "deferred"` in either case once
    /// [`SpillTier::set_keep_resident_when_refused`] set this tier's
    /// policy. Never touches disk, but does take the channel's own lock,
    /// so, unlike [`SpillTier::would_accept`], this is not meant to run
    /// while holding a stripe write lock. The permits `would_accept`
    /// admitted for `job` are already the caller's; this call touches no
    /// counter of its own, since only [`flusher_loop`] returns them via
    /// `admit.add_permits` once the job is pulled off the channel.
    pub(crate) fn enqueue(&self, job: SpillJob) -> Result<(), Box<SpillJob>> {
        let sent = {
            let sender = self.sender.lock();
            let Some(tx) = sender.as_ref() else {
                self.inner.record_dropped(self.refusal_reason("queue_full"));
                return Err(Box::new(job));
            };
            tx.try_send(job)
        };
        match sent {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(job) | mpsc::TrySendError::Disconnected(job)) => {
                self.inner.record_dropped(self.refusal_reason("queue_full"));
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
            if region_idx >= region_count {
                return None;
            }
            let end = entry.loc.offset.checked_add(entry.loc.len)?;
            if end > region_bytes {
                return None;
            }
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
    reverse_index_by_region: Vec<Vec<(usize, Bytes)>>,
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
    let mut reverse_index_by_region: Vec<Vec<(usize, Bytes)>> =
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
            reverse_index_by_region[region_idx].push((stripe_idx, entry.key));
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

fn region_file_name(idx: u32) -> String {
    format!("spill-{idx:08x}.reg")
}

fn remove_stale_region_files(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("reg") {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Removes a leftover checkpoint snapshot, temp name included, since a
/// cold open recreates every region from scratch. A missing file is not
/// an error.
fn remove_stale_snapshot_files(dir: &Path) -> io::Result<()> {
    for path in [snapshot_path(dir), snapshot_tmp_path(dir)] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// The checkpoint snapshot's path inside a tier's own directory.
fn snapshot_path(dir: &Path) -> PathBuf {
    dir.join(SNAPSHOT_FILE_NAME)
}

/// [`write_snapshot_atomic`]'s temporary-name counterpart to
/// [`snapshot_path`].
fn snapshot_tmp_path(dir: &Path) -> PathBuf {
    dir.join(SNAPSHOT_TMP_FILE_NAME)
}

/// `xxh3_64` over every byte but the leading `checksum` field, mirroring
/// [`SpillRecordHeader`]'s checksum reasoning.
fn snapshot_checksum(buf: &[u8]) -> u64 {
    let mut hasher = Xxh3Default::new();
    hasher.update(&buf[size_of::<u64>()..]);
    hasher.digest()
}

/// Serializes `entries` (live, un-tombstoned, un-expired pointers) into
/// one snapshot buffer: a fixed [`SnapshotHeader`] naming
/// `region_bytes`/`region_count`/`closed_at_ms`, followed by one
/// variable-length record per entry. Manual serialization, not
/// `zerocopy`, since keys vary in length. `checksum` is computed last and
/// patched into the first eight bytes.
fn build_snapshot_bytes(
    entries: &[(Bytes, Hlc, Option<u64>, SpillLoc)],
    region_bytes: u64,
    region_count: u32,
    closed_at_ms: u64,
) -> Vec<u8> {
    let mut buf =
        Vec::with_capacity(SNAPSHOT_HEADER_LEN + entries.len() * (SNAPSHOT_ENTRY_FIXED_LEN + 16));
    buf.extend_from_slice(&0u64.to_le_bytes()); // checksum placeholder
    buf.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&region_bytes.to_le_bytes());
    buf.extend_from_slice(&region_count.to_le_bytes());
    buf.extend_from_slice(&closed_at_ms.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    debug_assert_eq!(buf.len(), SNAPSHOT_HEADER_LEN);
    for (key, ver, expires_at_ms, loc) in entries {
        buf.extend_from_slice(&ver.wall_ms.to_le_bytes());
        buf.extend_from_slice(&ver.logical.to_le_bytes());
        buf.extend_from_slice(&ver.node.as_u64().to_le_bytes());
        buf.extend_from_slice(&expires_at_ms.unwrap_or(u64::MAX).to_le_bytes());
        buf.extend_from_slice(&loc.region.to_le_bytes());
        buf.extend_from_slice(&loc.offset.to_le_bytes());
        buf.extend_from_slice(&loc.len.to_le_bytes());
        buf.extend_from_slice(&loc.generation.to_le_bytes());
        buf.extend_from_slice(&(u32::try_from(key.len()).unwrap_or(u32::MAX)).to_le_bytes());
        buf.extend_from_slice(key);
    }
    let checksum = snapshot_checksum(&buf);
    buf[..size_of::<u64>()].copy_from_slice(&checksum.to_le_bytes());
    buf
}

/// Writes `bytes` to a temp file, then renames it to [`snapshot_path`]: a
/// crash in between leaves only the temp file, so reopen sees
/// `"no_snapshot"`. No fsync. A failed write or rename is logged and
/// ignored.
fn write_snapshot_atomic(dir: &Path, bytes: &[u8]) {
    let tmp = snapshot_tmp_path(dir);
    if let Err(err) = fs::write(&tmp, bytes) {
        tracing::warn!(
            dir = %dir.display(),
            error = %err,
            "sundog spill: checkpoint snapshot write failed",
        );
        return;
    }
    if let Err(err) = fs::rename(&tmp, snapshot_path(dir)) {
        tracing::warn!(
            dir = %dir.display(),
            error = %err,
            "sundog spill: checkpoint snapshot rename failed",
        );
    }
}

/// Removes `dir`'s checkpoint snapshot after a successful reopen, so a
/// second open with no intervening close takes the cold path instead of
/// replaying against overwritten regions.
fn delete_snapshot(dir: &Path) {
    match fs::remove_file(snapshot_path(dir)) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(
                dir = %dir.display(),
                error = %err,
                "sundog spill: checkpoint snapshot delete failed after a warm reopen",
            );
        }
    }
}

/// Parses `buf`, a whole snapshot file's bytes, into a [`SnapshotHeader`]
/// and its [`SnapshotEntry`] list, or `None` if it doesn't check out (too
/// short, bad checksum/magic, truncated). `format_version` isn't checked
/// here, since a future format may not even parse;
/// [`snapshot_eligibility`] checks it after parsing succeeds.
fn parse_snapshot(buf: &[u8]) -> Option<(SnapshotHeader, Vec<SnapshotEntry>)> {
    if buf.len() < SNAPSHOT_HEADER_LEN {
        return None;
    }
    let checksum = u64::from_le_bytes(buf[0..8].try_into().ok()?);
    if snapshot_checksum(buf) != checksum {
        return None;
    }
    let magic = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    if magic != SNAPSHOT_MAGIC {
        return None;
    }
    let format_version = u32::from_le_bytes(buf[12..16].try_into().ok()?);
    let region_bytes = u64::from_le_bytes(buf[16..24].try_into().ok()?);
    let region_count = u32::from_le_bytes(buf[24..28].try_into().ok()?);
    let closed_at_ms = u64::from_le_bytes(buf[28..36].try_into().ok()?);
    let entry_count = u64::from_le_bytes(buf[36..44].try_into().ok()?);

    let mut pos = SNAPSHOT_HEADER_LEN;
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        if buf.len() < pos + SNAPSHOT_ENTRY_FIXED_LEN {
            return None;
        }
        let wall_ms = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let logical = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let node = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let expires_raw = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let region = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let offset = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let generation = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let key_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if buf.len() < pos + key_len {
            return None;
        }
        let key = Bytes::copy_from_slice(&buf[pos..pos + key_len]);
        pos += key_len;
        entries.push(SnapshotEntry {
            key,
            ver: Hlc {
                wall_ms,
                logical,
                node: NodeId::from(node),
            },
            expires_at_ms: (expires_raw != u64::MAX).then_some(expires_raw),
            loc: SpillLoc {
                region,
                offset,
                len,
                generation,
            },
        });
    }

    Some((
        SnapshotHeader {
            region_bytes,
            closed_at_ms,
            format_version,
            region_count,
        },
        entries,
    ))
}

/// Test-only: writes `dir` a snapshot stale by one format version,
/// everything else intact, so only [`snapshot_eligibility`]'s
/// `"stale_snapshot"` check can fail against it. Used to prove
/// `Shard::attach_spill`'s metric recording hits `reason="stale_snapshot"`
/// through the real call site.
#[cfg(test)]
pub(crate) fn write_stale_snapshot_for_test(
    dir: &Path,
    region_bytes: u64,
    region_count: u32,
    closed_at_ms: u64,
) {
    let mut bytes = build_snapshot_bytes(&[], region_bytes, region_count, closed_at_ms);
    bytes[12..16].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.wrapping_add(1).to_le_bytes());
    let checksum = snapshot_checksum(&bytes);
    bytes[..8].copy_from_slice(&checksum.to_le_bytes());
    write_snapshot_atomic(dir, &bytes);
}

/// [`SpillTier::reopen`]'s eligibility check: a snapshot must be present
/// and parse cleanly (`"no_snapshot"` otherwise), name the current
/// [`SNAPSHOT_FORMAT_VERSION`] (`"stale_snapshot"`), match
/// `region_bytes`/`region_count` (`"config_mismatch"`), and have closed
/// within `tombstone_ttl_ms` of `now_ms` (`"downtime_exceeded"`). `Ok`
/// names the parsed snapshot; `Err` names the [`ReopenOutcome::reason`]
/// to record.
fn snapshot_eligibility(
    dir: &Path,
    region_bytes: u64,
    region_count: u32,
    now_ms: u64,
    tombstone_ttl_ms: u64,
) -> Result<(SnapshotHeader, Vec<SnapshotEntry>), &'static str> {
    let bytes = fs::read(snapshot_path(dir)).map_err(|_| "no_snapshot")?;
    let (header, entries) = parse_snapshot(&bytes).ok_or("no_snapshot")?;
    if header.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err("stale_snapshot");
    }
    if header.region_bytes != region_bytes || header.region_count != region_count {
        return Err("config_mismatch");
    }
    if header.closed_at_ms == 0 || now_ms.saturating_sub(header.closed_at_ms) > tombstone_ttl_ms {
        return Err("downtime_exceeded");
    }
    Ok((header, entries))
}

fn record_checksum(key_bytes: &[u8], value_bytes: &[u8]) -> u64 {
    let mut hasher = Xxh3Default::new();
    hasher.update(key_bytes);
    hasher.update(value_bytes);
    hasher.digest()
}

fn build_header(job: &SpillJob, key_len: u32, value_len: u32) -> SpillRecordHeader {
    SpillRecordHeader {
        checksum: record_checksum(&job.key_bytes, &job.encoded),
        expires_at_ms: job.expires_at_ms.unwrap_or(u64::MAX),
        wall_ms: job.ver.wall_ms,
        node: job.ver.node.as_u64(),
        magic: SPILL_MAGIC,
        key_len,
        value_len,
        logical: job.ver.logical,
    }
}

/// Parses `buf`, one record's bytes as read off disk, into its key bytes
/// alongside [`SpilledBytes`], or `None` for a bad buffer, magic, length,
/// or checksum. [`decode_record`] wraps this, discarding the key, for
/// [`SpillTier::read_at`]'s caller, which already knows the key; reopen's
/// region scan does not.
fn decode_record_with_key(buf: &[u8]) -> Option<(Bytes, SpilledBytes)> {
    let (header, rest) = SpillRecordHeader::read_from_prefix(buf).ok()?;
    if header.magic != SPILL_MAGIC {
        return None;
    }
    let key_len = header.key_len as usize;
    let value_len = header.value_len as usize;
    if rest.len() != key_len + value_len {
        return None;
    }
    let (key_bytes, value_bytes) = rest.split_at(key_len);
    if record_checksum(key_bytes, value_bytes) != header.checksum {
        return None;
    }
    let expires_at_ms = (header.expires_at_ms != u64::MAX).then_some(header.expires_at_ms);
    Some((
        Bytes::copy_from_slice(key_bytes),
        SpilledBytes {
            ver: Hlc {
                wall_ms: header.wall_ms,
                logical: header.logical,
                node: NodeId::from(header.node),
            },
            expires_at_ms,
            encoded: Bytes::copy_from_slice(value_bytes),
        },
    ))
}

/// Parses `buf`, one record's bytes as read off disk, into
/// [`SpilledBytes`], or `None` for anything that doesn't check out: a short
/// buffer, a bad magic, a length mismatch, or a checksum mismatch. Every
/// one of these is treated identically. A corrupted or torn record reads
/// like one that is never there.
fn decode_record(buf: &[u8]) -> Option<SpilledBytes> {
    decode_record_with_key(buf).map(|(_, sb)| sb)
}

fn flusher_loop(inner: &Arc<Inner>, rx: &Receiver<SpillJob>, sink: &Weak<dyn SpillSink>) {
    loop {
        wait_while_flusher_paused(inner);
        let Ok(first) = rx.recv() else {
            return;
        };
        // Credited back before the sink-upgrade check: crediting only on a
        // live sink would permanently shrink the admission budget whenever
        // the last strong SpillSink reference drops at this instant.
        inner.admit.add_permits(job_record_len_or_zero(&first));
        let Some(sink) = sink.upgrade() else {
            return;
        };
        let mut batch_bytes = job_record_len_or_zero(&first);
        let mut batch = vec![first];
        while batch.len() < FLUSH_BATCH_MAX_JOBS && batch_bytes < FLUSH_BATCH_MAX_BYTES {
            let Ok(job) = rx.try_recv() else {
                break;
            };
            inner.admit.add_permits(job_record_len_or_zero(&job));
            batch_bytes += job_record_len_or_zero(&job);
            batch.push(job);
        }
        inner
            .flusher_jobs_drained
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        flush_batch(inner, sink.as_ref(), batch);
    }
}

/// Test-only hook [`flusher_loop`] polls before pulling its next job off
/// the channel: spins while [`SpillTier::pause_flusher`] holds it paused,
/// a no-op otherwise. Always a no-op outside `cfg(test)`, so this costs
/// nothing in production.
#[cfg(test)]
fn wait_while_flusher_paused(inner: &Inner) {
    while inner.flusher_paused.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(2));
    }
}

#[cfg(not(test))]
fn wait_while_flusher_paused(_inner: &Inner) {}

/// A job's on-disk record length, header plus key plus value, together with
/// the header's own `key_len`/`value_len` fields, computed once per job and
/// reused for both the batch-drain byte bound and the record it builds.
/// `None` only if `job`'s key or value is too long to ever pass
/// `SpillTier::would_accept`'s own `record_too_large` check before this job
/// is queued; unreachable, never assumed.
fn job_record_lens(job: &SpillJob) -> Option<(u32, u32, u32)> {
    let key_len = u32::try_from(job.key_bytes.len()).ok()?;
    let value_len = u32::try_from(job.encoded.len()).ok()?;
    let header_len = u32::try_from(HEADER_LEN).ok()?;
    let record_len = header_len.checked_add(key_len)?.checked_add(value_len)?;
    Some((record_len, key_len, value_len))
}

fn job_record_len_or_zero(job: &SpillJob) -> usize {
    job_record_lens(job).map_or(0, |(record_len, ..)| record_len as usize)
}

/// One job drained into a flush batch, with its record length and header
/// field lengths already computed: reused for the batch-splitting decision
/// ([`records_fitting_region`]) and, once a segment is decided, for the
/// header this job contributes to it.
struct PreparedJob {
    job: SpillJob,
    record_len: u32,
    key_len: u32,
    value_len: u32,
}

/// Encodes and writes `jobs` in as few positional writes as the active
/// region's remaining space allows: one per region the batch touches, a
/// rotation between them where [`records_fitting_region`] says the
/// next record stops fitting, never a record straddling two regions. Each
/// written record then installs through `sink` individually, since its
/// stripe lock is necessarily per-entry, but the batch's confirmed write
/// count and byte total post to `inner` once for the whole batch, and each
/// region's reverse-index rows post once per region the batch touches
/// rather than once per record. A region whose write fails calls
/// `sink.abandon` for every job that segment held; jobs in a segment
/// already written in the same batch are unaffected.
fn flush_batch(inner: &Inner, sink: &dyn SpillSink, jobs: Vec<SpillJob>) {
    let mut pending: Vec<PreparedJob> = Vec::with_capacity(jobs.len());
    for job in jobs {
        if let Some((record_len, key_len, value_len)) = job_record_lens(&job) {
            pending.push(PreparedJob {
                job,
                record_len,
                key_len,
                value_len,
            });
        }
        // else: unreachable, see `job_record_lens`; drop the job exactly as
        // the pre-batching `flush_one` did, rather than ever panic this
        // thread over a job that could never have been queued.
    }

    let mut batch_installed = 0u64;
    let mut batch_bytes = 0u64;
    let mut active = inner.active.load(Ordering::Acquire);

    while !pending.is_empty() {
        let region = &inner.regions[active as usize];
        let cursor = region.write_cursor.load(Ordering::Acquire);
        let generation = region.generation.load(Ordering::Acquire);
        let lens: Vec<u32> = pending.iter().map(|p| p.record_len).collect();
        let take = records_fitting_region(cursor, inner.region_bytes, &lens);

        if take == 0 {
            // Not even the next record fits what is left of the active
            // region; rotate into an empty one and retry against it.
            // `try_spill`/`would_accept` already bounds every queued job's
            // record length to at most `region_bytes`, so a freshly rotated,
            // empty region always fits at least one more record.
            active = rotate(inner, sink, active);
            continue;
        }

        let segment: Vec<PreparedJob> = pending.drain(..take).collect();
        let (installed, bytes) = write_segment(inner, sink, active, cursor, generation, segment);
        batch_installed += installed;
        batch_bytes += bytes;

        if !pending.is_empty() {
            active = rotate(inner, sink, active);
        }
    }

    if batch_installed > 0 {
        inner.record_writes(batch_installed);
    }
    if batch_bytes > 0 {
        inner.bytes_used.fetch_add(batch_bytes, Ordering::AcqRel);
        inner.publish_bytes_used();
    }
}

/// Writes one segment, every record in it destined for the same region and
/// generation, as one positional write, then installs each record
/// individually. Returns `(installed_count, installed_bytes)`, folded into
/// the enclosing batch's single counter increment and byte total. A failed
/// write calls `sink.abandon` and records `sundog_spill_dropped_total`
/// with `reason = "disk_error"` for every job in `segment`, then returns
/// `(0, 0)`; the region's write cursor is left untouched either way the
/// write itself resolves, since it only ever advances past bytes on disk.
fn write_segment(
    inner: &Inner,
    sink: &dyn SpillSink,
    region_idx: u32,
    base_offset: u32,
    generation: u32,
    segment: Vec<PreparedJob>,
) -> (u64, u64) {
    let region = &inner.regions[region_idx as usize];

    let mut buf = Vec::new();
    let mut locs = Vec::with_capacity(segment.len());
    let mut cursor = base_offset;
    for prepared in &segment {
        let header = build_header(&prepared.job, prepared.key_len, prepared.value_len);
        buf.extend_from_slice(header.as_bytes());
        buf.extend_from_slice(&prepared.job.key_bytes);
        buf.extend_from_slice(&prepared.job.encoded);
        locs.push(SpillLoc {
            region: region_idx,
            offset: cursor,
            len: prepared.record_len,
            generation,
        });
        // `records_fitting_region` already validated this segment's summed
        // record lengths fit below `region_bytes` starting from `base_offset`.
        cursor += prepared.record_len;
    }

    if let Err(err) = pwrite_all(&region.file, &buf, u64::from(base_offset)) {
        tracing::warn!(
            cache = %inner.cache_name,
            error = %err,
            "sundog spill: region write failed, dropping the batch"
        );
        inner
            .flusher_jobs_abandoned
            .fetch_add(segment.len() as u64, Ordering::Relaxed);
        for prepared in segment {
            let job = prepared.job;
            sink.abandon(
                job.stripe_idx,
                &job.key_bytes,
                job.hash,
                job.ver,
                job.weight,
            );
            inner.record_dropped("disk_error");
        }
        return (0, 0);
    }
    region.write_cursor.store(cursor, Ordering::Release);

    let mut installed_count = 0u64;
    let mut installed_bytes = 0u64;
    let mut newly_indexed = Vec::with_capacity(segment.len());
    for (prepared, loc) in segment.into_iter().zip(locs) {
        let SpillJob {
            stripe_idx,
            hash,
            key_bytes,
            ver,
            weight,
            ..
        } = prepared.job;
        if sink.install(stripe_idx, &key_bytes, hash, ver, loc, weight) {
            installed_count += 1;
            installed_bytes += u64::from(loc.len);
            newly_indexed.push((stripe_idx, key_bytes));
        } else {
            inner.record_dropped("obsolete");
        }
    }
    if !newly_indexed.is_empty() {
        region
            .used_bytes
            .fetch_add(installed_bytes, Ordering::AcqRel);
        region.reverse_index.lock().extend(newly_indexed);
    }
    (installed_count, installed_bytes)
}

/// Reclaims `next_region_index(current, region_count)`, the next region due
/// for reuse: walks its reverse index, hands every listed key to
/// `sink.reclaim` *before* bumping the generation or resetting the cursor.
/// Only a key whose pointer, at that moment, still names this
/// region/generation is gone. Then makes it the new active region.
/// Returns the newly active region's index. Infallible: reclaim and
/// rotation are pure bookkeeping, with no I/O of their own.
fn rotate(inner: &Inner, sink: &dyn SpillSink, current: u32) -> u32 {
    let region_count = u32::try_from(inner.regions.len()).unwrap_or(u32::MAX);
    let next = next_region_index(current, region_count);
    let region = &inner.regions[next as usize];

    let generation = region.generation.load(Ordering::Acquire);
    let keys = std::mem::take(&mut *region.reverse_index.lock());
    let _purged = sink.reclaim(next, generation, &keys);

    let freed = region.used_bytes.swap(0, Ordering::AcqRel);
    inner.bytes_used.fetch_sub(freed, Ordering::AcqRel);

    region.generation.fetch_add(1, Ordering::AcqRel);
    region.write_cursor.store(0, Ordering::Release);
    inner.active.store(next, Ordering::Release);

    inner.record_region_reclaim();
    inner.publish_bytes_used();

    next
}

#[cfg(unix)]
fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(windows)]
fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut read = 0usize;
    while read < buf.len() {
        let n = file.seek_read(&mut buf[read..], offset + read as u64)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        read += n;
    }
    Ok(())
}

#[cfg(unix)]
fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

#[cfg(windows)]
fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        written += n;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Pure functions: no I/O, no tokio, safe under every feature combo. ---

    #[test]
    fn region_count_for_returns_at_least_one_region() {
        assert_eq!(region_count_for(0, 64), 1);
        assert_eq!(region_count_for(63, 64), 1);
        assert_eq!(region_count_for(64, 64), 1);
        assert_eq!(region_count_for(128, 64), 2);
        assert_eq!(region_count_for(200, 64), 3);
    }

    #[test]
    fn slot_count_exceeds_the_old_fixed_capacity_for_small_records() {
        // Enough 256-byte records to fill FLUSH_QUEUE_CAPACITY well before
        // this byte budget.
        let flush_queue_bytes = 8192 * 256;
        assert!(flush_queue_slots(flush_queue_bytes) > FLUSH_QUEUE_CAPACITY);
    }

    #[test]
    fn flush_queue_slots_stays_at_the_floor_for_a_tiny_byte_budget() {
        assert_eq!(flush_queue_slots(0), FLUSH_QUEUE_CAPACITY);
        assert_eq!(flush_queue_slots(1), FLUSH_QUEUE_CAPACITY);
    }

    #[test]
    fn flush_queue_slots_clamps_at_the_ceiling_for_a_huge_byte_budget() {
        assert_eq!(flush_queue_slots(u64::MAX), FLUSH_QUEUE_SLOTS_MAX);
    }

    #[test]
    fn record_fits_at_the_exact_boundary_and_one_byte_over() {
        assert!(record_fits(60, 64, 4));
        assert!(!record_fits(61, 64, 4));
        assert!(record_fits(0, 64, 64));
        assert!(!record_fits(0, 64, 65));
    }

    #[test]
    fn record_fits_never_wraps_on_a_pathological_record_len() {
        assert!(!record_fits(u32::MAX - 1, 64, u32::MAX));
        assert!(!record_fits(10, u32::MAX, u32::MAX));
    }

    #[test]
    fn records_fitting_region_takes_every_record_when_they_all_fit() {
        // Four 16-byte records exactly fill a 64-byte region starting at 0.
        assert_eq!(records_fitting_region(0, 64, &[16, 16, 16, 16]), 4);
    }

    #[test]
    fn records_fitting_region_stops_at_the_first_record_that_does_not_fit() {
        // 60 bytes are free; a 4-byte record lands exactly on the boundary
        // and is taken, a following 1-byte record is not.
        assert_eq!(records_fitting_region(60, 64, &[4, 1]), 1);
        // The same 4-byte record one byte later does not fit at all.
        assert_eq!(records_fitting_region(61, 64, &[4]), 0);
    }

    #[test]
    fn records_fitting_region_an_empty_batch_takes_nothing() {
        assert_eq!(records_fitting_region(0, 64, &[]), 0);
    }

    #[test]
    fn records_fitting_region_a_batch_spanning_a_rotation_splits_in_two() {
        // A region with 20 bytes free; a four-record batch of 8 bytes each
        // fits two before the third would overrun, exactly the split
        // `flush_batch` uses to write the first segment, rotate, and retry
        // the remainder against a freshly emptied region (write_cursor 0).
        let lens = [8u32, 8, 8, 8];
        let first_segment = records_fitting_region(0, 20, &lens);
        assert_eq!(first_segment, 2);
        let remainder = &lens[first_segment..];
        // After a rotation the new region starts at cursor 0, where every
        // remaining record fits.
        assert_eq!(records_fitting_region(0, 20, remainder), remainder.len());
    }

    #[test]
    fn record_too_large_rejects_over_region_bytes_and_accepts_exact_fit() {
        assert!(!record_too_large(64, 64));
        assert!(record_too_large(65, 64));
        assert!(!record_too_large(0, 0));
    }

    #[test]
    fn record_fits_queue_at_the_exact_boundary_and_one_byte_over() {
        assert!(record_fits_queue(60, 4, 64));
        assert!(!record_fits_queue(61, 4, 64));
        assert!(record_fits_queue(0, 64, 64));
        assert!(!record_fits_queue(0, 65, 64));
    }

    #[test]
    fn record_fits_queue_never_wraps_on_a_pathological_record_len() {
        assert!(!record_fits_queue(u64::MAX - 1, u64::MAX, 64));
        assert!(!record_fits_queue(10, u64::MAX, u64::MAX));
    }

    #[test]
    fn spill_record_len_is_header_plus_key_plus_value() {
        let header_len = u32::try_from(HEADER_LEN).unwrap();
        assert_eq!(spill_record_len(4, 4), header_len + 8);
        assert_eq!(spill_record_len(0, 0), header_len);
    }

    #[test]
    fn spill_record_len_saturates_instead_of_wrapping_on_a_pathological_input() {
        assert_eq!(spill_record_len(usize::MAX, usize::MAX), u32::MAX);
    }

    #[test]
    fn refusal_drop_reason_passes_through_the_specific_reason_by_default() {
        assert_eq!(refusal_drop_reason(false, "too_large"), "too_large");
        assert_eq!(refusal_drop_reason(false, "closed"), "closed");
        assert_eq!(refusal_drop_reason(false, "queue_full"), "queue_full");
    }

    #[test]
    fn refusal_drop_reason_reports_deferred_when_keep_resident_when_refused_is_set() {
        assert_eq!(refusal_drop_reason(true, "too_large"), "deferred");
        assert_eq!(refusal_drop_reason(true, "closed"), "deferred");
        assert_eq!(refusal_drop_reason(true, "queue_full"), "deferred");
    }

    #[test]
    fn next_region_index_wraps_from_the_last_region_to_the_first() {
        assert_eq!(next_region_index(0, 3), 1);
        assert_eq!(next_region_index(1, 3), 2);
        assert_eq!(next_region_index(2, 3), 0);
        assert_ne!(next_region_index(0, 2), 0);
        assert_ne!(next_region_index(1, 2), 1);
    }

    fn hlc(wall_ms: u64, logical: u32) -> Hlc {
        Hlc {
            wall_ms,
            logical,
            node: NodeId::from(7u64),
        }
    }

    #[test]
    fn spilled_is_current_true_when_live_version_matches_and_no_tombstone() {
        let v = hlc(10, 0);
        assert!(spilled_is_current(None, Some(v), v));
    }

    #[test]
    fn spilled_is_current_false_when_tombstoned() {
        let v = hlc(10, 0);
        assert!(!spilled_is_current(Some(hlc(5, 0)), Some(v), v));
        assert!(!spilled_is_current(Some(v), Some(v), v));
    }

    #[test]
    fn spilled_is_current_false_when_live_version_differs() {
        let spilled = hlc(10, 0);
        assert!(!spilled_is_current(None, Some(hlc(11, 0)), spilled));
        assert!(!spilled_is_current(None, Some(hlc(9, 0)), spilled));
    }

    #[test]
    fn spilled_is_current_false_when_nothing_live() {
        assert!(!spilled_is_current(None, None, hlc(10, 0)));
    }

    /// Builds one record's raw on-disk bytes for
    /// [`decode_record_with_key`] tests.
    fn raw_record(key: &[u8], value: &[u8], ver: Hlc, expires_at_ms: Option<u64>) -> Vec<u8> {
        let job = SpillJob {
            stripe_idx: 0,
            hash: 0,
            key_bytes: Bytes::copy_from_slice(key),
            ver,
            expires_at_ms,
            encoded: Bytes::copy_from_slice(value),
            weight: 1,
            admitted_bytes: 0,
        };
        let key_len = u32::try_from(key.len()).unwrap();
        let value_len = u32::try_from(value.len()).unwrap();
        let header = build_header(&job, key_len, value_len);
        let mut buf = Vec::new();
        buf.extend_from_slice(header.as_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        buf
    }

    // --- decode_record_with_key ---

    #[test]
    fn decode_record_with_key_round_trips_the_key_alongside_the_value() {
        let ver = hlc(7, 2);
        let buf = raw_record(b"a-key", b"a-value", ver, Some(555));

        let (key, sb) = decode_record_with_key(&buf).expect("a well-formed record decodes");
        assert_eq!(key.as_ref(), b"a-key");
        assert_eq!(sb.ver, ver);
        assert_eq!(sb.expires_at_ms, Some(555));
        assert_eq!(sb.encoded.as_ref(), b"a-value");
    }

    #[test]
    fn decode_record_with_key_rejects_a_bit_flipped_checksum() {
        let mut buf = raw_record(b"k", b"v", hlc(1, 0), None);
        let last = buf.len() - 1;
        buf[last] ^= 0x01;
        assert!(decode_record_with_key(&buf).is_none());
    }

    #[test]
    fn decode_record_still_discards_the_key_exactly_as_before() {
        let buf = raw_record(b"k", b"v", hlc(1, 0), None);
        let sb = decode_record(&buf).expect("a well-formed record decodes");
        assert_eq!(sb.encoded.as_ref(), b"v");
    }

    // --- SpillConfig / validate ---

    #[test]
    fn spill_config_defaults_are_64_mib_regions_16_way_read_concurrency_and_one_region_of_flush_queue()
     {
        let cfg = SpillConfig::new("/tmp/does-not-matter", 1 << 30);
        assert_eq!(cfg.region_bytes_value(), 64 * 1024 * 1024);
        assert_eq!(cfg.read_concurrency_value(), 16);
        assert_eq!(cfg.flush_queue_bytes_value(), 64 * 1024 * 1024);
    }

    #[test]
    fn spill_config_builder_methods_override_the_defaults() {
        let cfg = SpillConfig::new("/tmp/does-not-matter", 1 << 30)
            .region_bytes(4096)
            .read_concurrency(4)
            .flush_queue_bytes(8192);
        assert_eq!(cfg.region_bytes_value(), 4096);
        assert_eq!(cfg.read_concurrency_value(), 4);
        assert_eq!(cfg.flush_queue_bytes_value(), 8192);
    }

    #[test]
    fn validate_rejects_zero_region_bytes() {
        let cfg = SpillConfig::new("/tmp/x", 1024).region_bytes(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_region_bytes_over_u32_max() {
        let cfg = SpillConfig::new("/tmp/x", u64::MAX).region_bytes(u64::from(u32::MAX) + 1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_capacity_under_twice_region_bytes() {
        let cfg = SpillConfig::new("/tmp/x", 100).region_bytes(64);
        assert!(cfg.validate().is_err());
        let ok = SpillConfig::new("/tmp/x", 128).region_bytes(64);
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn validate_rejects_flush_queue_bytes_under_one_record() {
        let cfg = SpillConfig::new("/tmp/x", 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(HEADER_LEN as u64 - 1);
        assert!(cfg.validate().is_err());
        let ok = SpillConfig::new("/tmp/x", 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(HEADER_LEN as u64);
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn validate_rejects_flush_queue_bytes_over_capacity_bytes() {
        let cfg = SpillConfig::new("/tmp/x", 1024)
            .region_bytes(64)
            .flush_queue_bytes(1025);
        assert!(cfg.validate().is_err());
        let ok = SpillConfig::new("/tmp/x", 1024)
            .region_bytes(64)
            .flush_queue_bytes(1024);
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn validate_rejects_an_absurd_spill_wait_timeout() {
        let cfg = SpillConfig::new("/tmp/x", 1 << 20)
            .region_bytes(4096)
            .spill_wait_timeout(Duration::from_secs(60));
        assert!(cfg.validate().is_err());
        let ok = SpillConfig::new("/tmp/x", 1 << 20)
            .region_bytes(4096)
            .spill_wait_timeout(Duration::from_secs(59));
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn validate_accepts_spill_wait_timeout_zero_as_an_explicit_opt_out() {
        let cfg = SpillConfig::new("/tmp/x", 1 << 20)
            .region_bytes(4096)
            .spill_wait_timeout(Duration::ZERO);
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.spill_wait_timeout_value(), Duration::ZERO);
    }

    #[test]
    fn spill_wait_timeout_defaults_to_two_seconds() {
        let cfg = SpillConfig::new("/tmp/x", 1 << 20);
        assert_eq!(cfg.spill_wait_timeout_value(), Duration::from_secs(2));
    }

    #[test]
    fn spill_wait_timeout_builder_overrides_the_default() {
        let cfg = SpillConfig::new("/tmp/x", 1 << 20).spill_wait_timeout(Duration::from_secs(5));
        assert_eq!(cfg.spill_wait_timeout_value(), Duration::from_secs(5));
    }

    #[test]
    fn default_warm_reopen_is_false() {
        let cfg = SpillConfig::new("/tmp/x", 1 << 20);
        assert!(!cfg.warm_reopen_value());
    }

    #[test]
    fn warm_reopen_builder_overrides_the_default() {
        let cfg = SpillConfig::new("/tmp/x", 1 << 20).warm_reopen(true);
        assert!(cfg.warm_reopen_value());
    }

    // --- SpillTier: I/O tests. Real disk, real thread; never combined with
    // `sim` since its virtual clock gives no determinism over real
    // filesystem I/O or the flusher's OS thread.
    #[cfg(not(feature = "sim"))]
    mod io {
        use std::collections::HashMap as StdHashMap;
        use std::sync::Mutex as StdMutex;
        use std::time::{Duration, Instant};

        use super::*;

        /// Polls `cond` until it returns `true` or `timeout` elapses,
        /// returning the final result either way. Never a fixed sleep: every
        /// timing-sensitive assertion in this module goes through this.
        fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
            let start = Instant::now();
            loop {
                if cond() {
                    return true;
                }
                if start.elapsed() >= timeout {
                    return cond();
                }
                thread::sleep(Duration::from_millis(5));
            }
        }

        const POLL_TIMEOUT: Duration = Duration::from_secs(5);

        /// One `reclaim` call: the region and generation being reused, and
        /// the `(stripe_idx, key_bytes)` pairs to purge.
        type ReclaimCall = (u32, u32, Vec<(usize, Bytes)>);

        /// One `install_new` call: `install`'s fields plus `expires_at_ms`.
        type InstallNewCall = (usize, Bytes, Hlc, Option<u64>, SpillLoc);

        #[derive(Default)]
        struct RecordingSink {
            installs: StdMutex<Vec<(usize, Bytes, Hlc, SpillLoc)>>,
            install_news: StdMutex<Vec<InstallNewCall>>,
            reclaims: StdMutex<Vec<ReclaimCall>>,
            live: StdMutex<StdHashMap<Bytes, (Hlc, SpillLoc)>>,
            abandons: StdMutex<Vec<(usize, Bytes, u64, Hlc)>>,
        }

        impl RecordingSink {
            fn install_count(&self) -> usize {
                self.installs.lock().unwrap().len()
            }

            fn install_new_count(&self) -> usize {
                self.install_news.lock().unwrap().len()
            }

            fn abandon_count(&self) -> usize {
                self.abandons.lock().unwrap().len()
            }
        }

        impl SpillSink for RecordingSink {
            fn install(
                &self,
                stripe_idx: usize,
                key_bytes: &Bytes,
                _hash: u64,
                ver: Hlc,
                loc: SpillLoc,
                _weight: u32,
            ) -> bool {
                self.installs
                    .lock()
                    .unwrap()
                    .push((stripe_idx, key_bytes.clone(), ver, loc));
                self.live
                    .lock()
                    .unwrap()
                    .insert(key_bytes.clone(), (ver, loc));
                true
            }

            #[allow(clippy::too_many_arguments)]
            fn install_new(
                &self,
                stripe_idx: usize,
                key_bytes: &Bytes,
                _hash: u64,
                ver: Hlc,
                expires_at_ms: Option<u64>,
                loc: SpillLoc,
                _weight: u32,
            ) -> bool {
                let mut live = self.live.lock().unwrap();
                if live.contains_key(key_bytes) {
                    return false;
                }
                live.insert(key_bytes.clone(), (ver, loc));
                drop(live);
                self.install_news.lock().unwrap().push((
                    stripe_idx,
                    key_bytes.clone(),
                    ver,
                    expires_at_ms,
                    loc,
                ));
                true
            }

            fn reclaim(&self, region: u32, generation: u32, keys: &[(usize, Bytes)]) -> usize {
                self.reclaims
                    .lock()
                    .unwrap()
                    .push((region, generation, keys.to_vec()));
                let mut live = self.live.lock().unwrap();
                let mut removed = 0;
                for (_, key) in keys {
                    if let Some((_, loc)) = live.get(key)
                        && loc.region == region
                        && loc.generation == generation
                    {
                        live.remove(key);
                        removed += 1;
                    }
                }
                removed
            }

            fn abandon(
                &self,
                stripe_idx: usize,
                key_bytes: &Bytes,
                hash: u64,
                ver: Hlc,
                _weight: u32,
            ) {
                // The engine-level weight-restoring behavior itself is
                // covered directly in `store::engine`'s tests; this test
                // double only records that the call happened, for this
                // module's own write-failure test.
                self.abandons
                    .lock()
                    .unwrap()
                    .push((stripe_idx, key_bytes.clone(), hash, ver));
            }
        }

        fn temp_dir(label: &str) -> PathBuf {
            let dir = std::env::temp_dir().join(format!(
                "sundog-spill-test-{label}-{}-{:?}",
                std::process::id(),
                thread::current().id(),
            ));
            let _ = fs::remove_dir_all(&dir);
            dir
        }

        fn job(key: &str, value: &[u8], ver: Hlc) -> SpillJob {
            SpillJob {
                stripe_idx: 0,
                hash: 0,
                key_bytes: Bytes::copy_from_slice(key.as_bytes()),
                ver,
                expires_at_ms: Some(999),
                encoded: Bytes::copy_from_slice(value),
                weight: 1,
                admitted_bytes: 0,
            }
        }

        #[test]
        fn flushed_job_installs_and_read_at_returns_the_same_bytes_ver_and_expiry() {
            let dir = temp_dir("roundtrip");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            let ver = hlc(42, 3);
            let j = job("hello", b"world-value", ver);
            assert!(tier.try_spill(j));

            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 1));
            let loc = sink.installs.lock().unwrap()[0].3;

            let bytes = tier
                .read_at(loc)
                .unwrap()
                .expect("record should be present");
            assert_eq!(bytes.ver, ver);
            assert_eq!(bytes.expires_at_ms, Some(999));
            assert_eq!(bytes.encoded.as_ref(), b"world-value");

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn read_at_returns_none_for_a_stale_generation() {
            // Each region holds one record, so writing a third job rotates
            // region 0 out from under the first job's pointer. Waiting for
            // each job's install before sending the next keeps this
            // deterministic under the default flush_queue_bytes bound
            // too, one region's worth here: only one record is ever
            // queued at a time, and the rotation this test depends on is
            // unaffected by writes landing one at a time versus batched.
            let dir = temp_dir("stale-gen");
            let record_len = HEADER_LEN as u64 + 1 + 1; // 1-byte key, 1-byte value
            let cfg = SpillConfig::new(&dir, 2 * record_len).region_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            assert!(tier.try_spill(job("a", b"1", hlc(1, 0))));
            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 1));
            assert!(tier.try_spill(job("b", b"2", hlc(2, 0))));
            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 2));
            assert!(tier.try_spill(job("c", b"3", hlc(3, 0))));
            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 3));

            let loc_a = sink.installs.lock().unwrap()[0].3;
            assert_eq!(tier.read_at(loc_a).unwrap(), None);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn read_at_returns_none_for_a_corrupted_record() {
            let dir = temp_dir("corrupt");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            assert!(tier.try_spill(job("k", b"original-value", hlc(1, 0))));
            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 1));
            let loc = sink.installs.lock().unwrap()[0].3;

            // Flip a byte inside the value payload, after the header and key,
            // leaving every length field intact so parsing proceeds and only
            // the checksum fails.
            let region_path = dir.join("cache-a").join(region_file_name(loc.region));
            let file = OpenOptions::new().write(true).open(&region_path).unwrap();
            let corrupt_offset = u64::from(loc.offset) + HEADER_LEN as u64 + 1 /* key len */;
            pwrite_all(&file, b"!", corrupt_offset).unwrap();

            assert_eq!(tier.read_at(loc).unwrap(), None);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn rotation_reclaims_the_oldest_region_and_reports_its_keys() {
            // region_bytes fits two records; five writes force a rotation
            // into region 1, empty, and then back into region 0, which by
            // then holds the first two jobs' keys. Waiting for each job's
            // own install before sending the next keeps this deterministic
            // under the default flush_queue_bytes bound, one region's
            // worth here (room for two records): the rotations this test
            // depends on are unaffected by writes landing one at a time
            // versus batched.
            let dir = temp_dir("rotation");
            let record_len = HEADER_LEN as u64 + 6 + 4; // fixed-width key/value
            let region_bytes = record_len * 2;
            let cfg = SpillConfig::new(&dir, region_bytes * 2).region_bytes(region_bytes);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            for i in 0..5u32 {
                let key = format!("key-{i:02}");
                assert!(tier.try_spill(job(&key, b"1234", hlc(u64::from(i) + 1, 0))));
                let installed_so_far = i as usize + 1;
                assert!(poll_until(POLL_TIMEOUT, || {
                    sink.install_count() == installed_so_far
                }));
            }
            assert!(poll_until(POLL_TIMEOUT, || sink
                .reclaims
                .lock()
                .unwrap()
                .len()
                == 2));

            let reclaims = sink.reclaims.lock().unwrap();
            let (region, generation, keys) = &reclaims[1];
            assert_eq!(*region, 0);
            assert_eq!(*generation, 0);
            let mut key_strings: Vec<String> = keys
                .iter()
                .map(|(_, k)| String::from_utf8(k.to_vec()).unwrap())
                .collect();
            key_strings.sort();
            assert_eq!(key_strings, vec!["key-00", "key-01"]);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn flush_batch_spanning_a_rotation_installs_correctly_on_both_sides() {
            // Two records exactly fill one region. Handing all four jobs to
            // one direct `flush_batch` call, rather than through `try_spill`
            // and the flusher thread, makes the rotation deterministic: the
            // batch writes the first two, rotates once into the never-yet-
            // used second region, and writes the last two there, a record
            // never straddling the two.
            let dir = temp_dir("batch-rotation");
            let record_len = HEADER_LEN as u64 + 6 + 4; // fixed-width key/value
            let region_bytes = record_len * 2;
            let cfg = SpillConfig::new(&dir, region_bytes * 2).region_bytes(region_bytes);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());

            let jobs: Vec<SpillJob> = (0..4u32)
                .map(|i| job(&format!("key-{i:02}"), b"1234", hlc(u64::from(i) + 1, 0)))
                .collect();
            flush_batch(&tier.inner, &*sink, jobs);

            assert_eq!(sink.install_count(), 4, "every job in the batch installs");
            assert_eq!(
                sink.reclaims.lock().unwrap().len(),
                1,
                "one rotation, into the never-yet-used second region"
            );

            let installs = sink.installs.lock().unwrap().clone();
            let regions: Vec<u32> = installs.iter().map(|(_, _, _, loc)| loc.region).collect();
            assert_eq!(
                regions,
                vec![0, 0, 1, 1],
                "the batch's four records land two per side of the one rotation"
            );
            for (idx, (_, key_bytes, _, loc)) in installs.iter().enumerate() {
                assert_eq!(key_bytes.as_ref(), format!("key-{idx:02}").as_bytes());
                let bytes = tier
                    .read_at(*loc)
                    .unwrap()
                    .expect("record present on both sides of the rotation");
                assert_eq!(bytes.encoded.as_ref(), b"1234");
            }

            let _ = fs::remove_dir_all(&dir);
        }

        /// `checkpoint_write_resident`'s self-collision guard: a
        /// wraparound overwrites a region this call already wrote, and
        /// only surviving entries' `SpillLoc`s come back.
        #[test]
        fn checkpoint_write_resident_drops_entries_its_own_wraparound_overwrites_and_reopen_replays_the_survivors()
         {
            let dir = temp_dir("checkpoint-wraparound");
            let record_len = HEADER_LEN as u64 + 5 + 4; // fixed-width "xxx-N"/4-byte value
            let region_bytes = record_len;
            let cfg = SpillConfig::new(&dir, region_bytes * 2)
                .region_bytes(region_bytes)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = RecordingSink::default();

            // Ordinary flush: old-0 lands in region 0, so its reverse index
            // knows about it.
            flush_batch(&tier.inner, &sink, vec![job("old-0", b"1234", hlc(1, 0))]);
            assert_eq!(sink.install_count(), 1);

            // Three entries into a two-record ring: res-0, then res-1
            // (reclaiming old-0), then res-2 wraps back and overwrites
            // res-0.
            let entries: Vec<(Bytes, Hlc, Option<u64>, Bytes)> = ["res-0", "res-1", "res-2"]
                .iter()
                .enumerate()
                .map(|(i, key)| {
                    (
                        Bytes::from(key.as_bytes().to_vec()),
                        hlc(u64::try_from(i).unwrap_or(u64::MAX) + 10, 0),
                        None,
                        Bytes::from_static(b"1234"),
                    )
                })
                .collect();
            let newly_written = tier.checkpoint_flush(&sink, entries).written;

            assert_eq!(
                sink.reclaims.lock().unwrap().len(),
                3,
                "three rotations: into the empty region 1, back into region 0, then into \
                 region 1 a second time"
            );
            let reclaimed_old =
                sink.reclaims
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(region, generation, keys)| {
                        *region == 0
                            && *generation == 0
                            && keys.iter().any(|(_, k)| k.as_ref() == b"old-0")
                    });
            assert!(
                reclaimed_old,
                "the reclaim-during-checkpoint path actually ran against the pre-existing entry"
            );

            let written_keys: Vec<&[u8]> = newly_written.iter().map(|(k, ..)| k.as_ref()).collect();
            assert_eq!(
                written_keys,
                vec![b"res-1".as_slice(), b"res-2".as_slice()],
                "res-0's bytes were overwritten by res-2 within this same checkpoint call, so \
                 only the entries whose bytes survived to the end of the call come back -- no \
                 location in the result points at bytes this same call already reclaimed"
            );

            tier.write_checkpoint_snapshot(&newly_written, 1_000);
            tier.close();

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("reopen against a cleanly closed tier succeeds");
            assert!(
                outcome.warm,
                "the surviving entries' own on-disk bytes must still validate cleanly; reason: \
                 {:?}",
                outcome.reason
            );
            assert_eq!(outcome.records_installed, 2);
            let mut installed_keys: Vec<Bytes> = fresh_sink
                .install_news
                .lock()
                .unwrap()
                .iter()
                .map(|(_, key, ..)| key.clone())
                .collect();
            installed_keys.sort();
            assert_eq!(
                installed_keys,
                vec![Bytes::from_static(b"res-1"), Bytes::from_static(b"res-2")],
                "reopen replays exactly the entries whose bytes survived the checkpoint, no \
                 more (no old-0, reclaimed mid-checkpoint) and no less (no res-0, overwritten \
                 mid-checkpoint)"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        /// Captures `sundog_spill_writes_total{cache}` increments for one
        /// dedicated cache name, ignoring every other metric: the
        /// write-count counterpart to [`DropCounts`]/[`DropRecorder`] below.
        #[derive(Clone, Default)]
        struct WriteCounts(Arc<StdMutex<u64>>);

        impl WriteCounts {
            fn get(&self) -> u64 {
                *self.0.lock().unwrap()
            }
        }

        struct WriteCounter {
            counts: WriteCounts,
        }

        impl metrics::CounterFn for WriteCounter {
            fn increment(&self, value: u64) {
                *self.counts.0.lock().unwrap() += value;
            }

            fn absolute(&self, value: u64) {
                *self.counts.0.lock().unwrap() = value;
            }
        }

        struct WriteRecorder {
            counts: WriteCounts,
        }

        impl metrics::Recorder for WriteRecorder {
            fn describe_counter(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn describe_gauge(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn describe_histogram(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn register_counter(
                &self,
                key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Counter {
                let this_cache = key
                    .labels()
                    .any(|l| l.key() == "cache" && l.value() == WRITE_COUNTS_CACHE);
                if key.name() != "sundog_spill_writes_total" || !this_cache {
                    return metrics::Counter::noop();
                }
                metrics::Counter::from_arc(Arc::new(WriteCounter {
                    counts: self.counts.clone(),
                }))
            }

            fn register_gauge(
                &self,
                _key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Gauge {
                metrics::Gauge::noop()
            }

            fn register_histogram(
                &self,
                _key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Histogram {
                metrics::Histogram::noop()
            }
        }

        /// The cache name only this test opens, so the process-global
        /// recorder ignores writes from every other test's tier.
        const WRITE_COUNTS_CACHE: &str = "write-counts-only";

        #[test]
        fn flusher_batches_two_thousand_records_and_installs_every_one_byte_exact() {
            const COUNT: u32 = 2_000;

            let write_counts = WriteCounts::default();
            // Same single-process-global-slot race every other metrics-
            // reading test in this module tolerates: if another test already
            // won the slot, this one skips the counter assertion below.
            let recorder_installed = metrics::set_global_recorder(WriteRecorder {
                counts: write_counts.clone(),
            })
            .is_ok();

            let dir = temp_dir("batch-2000");
            let cfg = SpillConfig::new(&dir, 1 << 24).region_bytes(1 << 20);
            let tier = SpillTier::open(&cfg, WRITE_COUNTS_CACHE).unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            let expected: Vec<(String, Vec<u8>)> = (0..COUNT)
                .map(|i| (format!("key-{i:05}"), format!("value-{i:05}").into_bytes()))
                .collect();
            for i in 0..COUNT {
                let (key, value) = &expected[i as usize];
                let ver = hlc(u64::from(i) + 1, 0);
                assert!(tier.try_spill(job(key, value, ver)));
            }

            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == COUNT as usize));

            let installs = sink.installs.lock().unwrap().clone();
            assert_eq!(installs.len(), COUNT as usize);
            for (idx, (expected_key, expected_value)) in expected.iter().enumerate() {
                let (_, key_bytes, _, loc) = &installs[idx];
                assert_eq!(
                    key_bytes.as_ref(),
                    expected_key.as_bytes(),
                    "installs land in the order jobs were sent, batched or not"
                );
                let bytes = tier
                    .read_at(*loc)
                    .unwrap()
                    .expect("every installed record reads back");
                assert_eq!(bytes.encoded.as_ref(), expected_value.as_slice());
            }

            if recorder_installed {
                assert_eq!(
                    write_counts.get(),
                    u64::from(COUNT),
                    "the writes counter equals the install count, batched or not"
                );
            }

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn bytes_used_reflects_confirmed_installs_and_drops_on_reclaim() {
            // Same layout as the rotation test: two records per region.
            // Waiting for each job's own install before sending the next
            // keeps this deterministic under the default
            // flush_queue_bytes bound, one region's worth here.
            let dir = temp_dir("bytes-used");
            let record_len = HEADER_LEN as u64 + 6 + 4;
            let region_bytes = record_len * 2;
            let cfg = SpillConfig::new(&dir, region_bytes * 2).region_bytes(region_bytes);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            for i in 0..4u32 {
                let key = format!("key-{i:02}");
                assert!(tier.try_spill(job(&key, b"1234", hlc(u64::from(i) + 1, 0))));
                let installed_so_far = i as usize + 1;
                assert!(poll_until(POLL_TIMEOUT, || {
                    sink.install_count() == installed_so_far
                }));
            }
            assert!(poll_until(POLL_TIMEOUT, || tier.bytes_used() == record_len * 4));

            // A fifth job rotates region 0 out from under the first two
            // records, reclaiming them and freeing their bytes.
            assert!(tier.try_spill(job("key-04", b"1234", hlc(5, 0))));
            assert!(poll_until(POLL_TIMEOUT, || tier.bytes_used() == record_len * 3));

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn try_spill_rejects_a_job_larger_than_region_bytes() {
            let dir = temp_dir("too-large");
            let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            let oversized = job("k", &[0u8; 100], hlc(1, 0));
            assert!(!tier.try_spill(oversized));
            assert_eq!(sink.install_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn would_accept_refuses_once_the_flush_queue_bytes_bound_would_be_exceeded() {
            let dir = temp_dir("byte-bound");
            let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            // Paused before the flusher thread is even spawned, so it never
            // gets a chance to race this test and drain the job the
            // assertions below depend on staying queued.
            tier.pause_flusher();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            // A standalone check here would double-count: try_spill's own
            // would_accept call below already commits the bytes on success.
            assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));
            assert_eq!(tier.queued_bytes(), record_len);
            assert_eq!(
                tier.would_accept(None, 4, 4),
                Admission::RefusedFinal,
                "the queue already holds one record's worth; a same-size second job would \
                 push it past flush_queue_bytes"
            );

            tier.resume_flusher();
            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 1));
            assert!(
                poll_until(POLL_TIMEOUT, || tier.queued_bytes() == 0),
                "queued_bytes drains back to zero once the flusher takes the job"
            );
            assert_eq!(
                tier.would_accept(None, 4, 4),
                Admission::Accepted,
                "room again once the backlog has drained"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn would_accept_never_refuses_when_the_caller_pre_reserved_enough_bytes() {
            // A pre-reserved second job succeeds via Reservation::spend alone.
            let dir = temp_dir("byte-bound-reserved");
            let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
            let record_len_u32 = u32::try_from(record_len).unwrap();
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            tier.pause_flusher();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));
            assert_eq!(tier.queued_bytes(), record_len, "admit is fully drained");
            assert_eq!(
                tier.would_accept(None, 4, 4),
                Admission::RefusedFinal,
                "the ordinary fallback still refuses with no reservation and no room"
            );

            let mut reservation = Reservation {
                admit: &tier.inner.admit,
                remaining: record_len_u32,
            };
            assert_eq!(
                tier.would_accept(Some(&mut reservation), 4, 4),
                Admission::Accepted,
                "a sufficient reservation admits the record with admit still fully drained"
            );
            drop(reservation);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn would_accept_returns_refused_pending_when_a_reservation_is_in_play_but_both_budgets_are_exhausted()
         {
            // Insufficient reservation + drained admit: RefusedPending, not a drop.
            let dir = temp_dir("byte-bound-pending");
            let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            tier.pause_flusher();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));
            assert_eq!(tier.queued_bytes(), record_len, "admit is fully drained");

            let mut reservation = Reservation {
                admit: &tier.inner.admit,
                remaining: 1,
            };
            assert_eq!(
                tier.would_accept(Some(&mut reservation), 4, 4),
                Admission::RefusedPending,
                "a reservation short of the record's own length, with admit also fully \
                 drained, is a pending refusal, not a final one"
            );
            assert_eq!(
                reservation.remaining, 1,
                "the insufficient reservation is left untouched, not partially spent"
            );
            assert_eq!(
                tier.queued_bytes(),
                record_len,
                "admit's own accounting is unaffected by a pending refusal: no permits \
                 acquired, none released"
            );
            drop(reservation);

            let _ = fs::remove_dir_all(&dir);
        }

        #[tokio::test]
        async fn reserve_resolves_once_the_flusher_frees_room() {
            let dir = temp_dir("reserve-resolves");
            let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
            let record_len_u32 = u32::try_from(record_len).unwrap();
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            // Paused before the flusher spawns, so admit stays drained.
            tier.pause_flusher();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));
            assert_eq!(tier.queued_bytes(), record_len, "admit is fully drained");

            // Pinned since a Reservation borrows this tier; a timed-out
            // poll keeps the future's registration with admit.
            let reserve_fut = tier.reserve(record_len_u32, Duration::from_secs(5));
            tokio::pin!(reserve_fut);

            // Not resolved yet: the queue is still fully drained.
            let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut reserve_fut).await;
            assert!(
                not_yet.is_err(),
                "no room yet; reserve must still be waiting"
            );

            tier.resume_flusher();
            let reservation = tokio::time::timeout(Duration::from_secs(5), &mut reserve_fut)
                .await
                .expect("reserve resolves once the flusher frees room")
                .expect("reserve succeeds once the flusher frees room");
            // queued_bytes() can't tell this apart from a job still queued.
            assert_eq!(tier.queued_bytes(), record_len);
            drop(reservation);
            assert_eq!(
                tier.queued_bytes(),
                0,
                "dropping the reservation returns its unspent bytes to admit"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        #[tokio::test]
        async fn reserve_times_out_when_the_flusher_never_resumes() {
            let dir = temp_dir("reserve-times-out");
            let record_len = HEADER_LEN as u64 + 4 + 4;
            let record_len_u32 = u32::try_from(record_len).unwrap();
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            tier.pause_flusher();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));

            let start = Instant::now();
            let short_timeout = Duration::from_millis(100);
            let result = tier.reserve(record_len_u32, short_timeout).await;
            let elapsed = start.elapsed();

            assert!(matches!(result, Err(SpillWaitTimedOut)));
            assert!(
                elapsed >= short_timeout,
                "reserve must not return before its own timeout elapses: waited {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_secs(5),
                "reserve must not hang past its own timeout: waited {elapsed:?}"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        #[tokio::test]
        async fn reserve_clamps_a_request_larger_than_total_permits() {
            let dir = temp_dir("reserve-clamps");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(64);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();

            // Larger than admit could ever hold: must clamp, not hang.
            let reservation = tokio::time::timeout(
                Duration::from_secs(5),
                tier.reserve(u32::MAX, Duration::from_secs(5)),
            )
            .await
            .expect("reserve resolves promptly once clamped")
            .expect("reserve succeeds against the tier's whole, empty queue");
            drop(reservation);

            let _ = fs::remove_dir_all(&dir);
        }

        #[tokio::test]
        async fn unspent_reservation_bytes_return_on_drop() {
            let dir = temp_dir("reservation-drop");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(100);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();

            let mut reservation = tier
                .reserve(100, Duration::from_secs(5))
                .await
                .expect("the whole queue is free");
            assert_eq!(tier.queued_bytes(), 100, "the full reservation is held");

            assert!(
                reservation.spend(40),
                "40 of the 100 reserved bytes spend cleanly"
            );
            drop(reservation);

            assert_eq!(
                tier.queued_bytes(),
                40,
                "only the 40 spent bytes stay charged against admit; the 60 never spent \
                 return to it on drop"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn spill_tier_exposes_its_configured_spill_wait_timeout() {
            let dir = temp_dir("tier-timeout-accessor");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .spill_wait_timeout(Duration::from_secs(7));
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            assert_eq!(tier.spill_wait_timeout_value(), Duration::from_secs(7));
            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn release_returns_bytes_to_admit() {
            let dir = temp_dir("release");
            let record_len = HEADER_LEN as u64 + 4 + 4;
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            assert_eq!(
                tier.would_accept(None, 4, 4),
                Admission::Accepted,
                "admits the one record's worth"
            );
            assert_eq!(tier.queued_bytes(), record_len);

            tier.release(u32::try_from(record_len).unwrap());
            assert_eq!(
                tier.queued_bytes(),
                0,
                "release returns the given bytes to admit, exactly the way \
                 finish_spill_handoff's Err branch relies on for a job that never reaches the \
                 flusher's channel"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn flusher_loop_credits_a_dequeued_jobs_bytes_even_when_the_sink_has_already_died() {
            let dir = temp_dir("flusher-loop-dead-sink");
            let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            assert_eq!(tier.queued_bytes(), 0, "nothing queued yet");

            // Debits admit first so crediting it back is observable.
            assert_eq!(
                tier.would_accept(None, 4, 4),
                Admission::Accepted,
                "admits the one record's worth"
            );
            assert_eq!(tier.queued_bytes(), record_len, "admit is fully drained");

            // One job already on the channel, as finish_spill_handoff leaves it.
            let (tx, rx) = mpsc::sync_channel(1);
            tx.send(job("aaaa", b"bbbb", hlc(1, 0))).unwrap();
            // Dropped so a second recv() returns Err, forcing one dequeue.
            drop(tx);

            // Strong reference already gone before upgrade().
            let sink: Weak<dyn SpillSink> = {
                let strong: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
                Arc::downgrade(&strong)
            };

            flusher_loop(&tier.inner, &rx, &sink);

            assert_eq!(
                tier.queued_bytes(),
                0,
                "a job dequeued off the channel must credit its bytes back to `admit` even \
                 when the sink has already died, or this tier's admission budget shrinks \
                 permanently by that job's own bytes for the rest of this tier's lifetime"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        /// Captures wait-related metrics for one dedicated cache name, so
        /// concurrent tests never perturb these counts.
        #[derive(Clone, Default)]
        struct WaitMetrics {
            waiters: Arc<StdMutex<f64>>,
            wait_seconds_total: Arc<StdMutex<u64>>,
            wait_timeouts_total: Arc<StdMutex<u64>>,
        }

        struct WaitCounter {
            field: Arc<StdMutex<u64>>,
        }

        impl metrics::CounterFn for WaitCounter {
            fn increment(&self, value: u64) {
                *self.field.lock().unwrap() += value;
            }

            fn absolute(&self, value: u64) {
                *self.field.lock().unwrap() = value;
            }
        }

        struct WaitGauge {
            current: Arc<StdMutex<f64>>,
        }

        impl metrics::GaugeFn for WaitGauge {
            fn increment(&self, value: f64) {
                *self.current.lock().unwrap() += value;
            }

            fn decrement(&self, value: f64) {
                *self.current.lock().unwrap() -= value;
            }

            fn set(&self, value: f64) {
                *self.current.lock().unwrap() = value;
            }
        }

        struct WaitMetricsRecorder {
            metrics: WaitMetrics,
        }

        impl metrics::Recorder for WaitMetricsRecorder {
            fn describe_counter(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn describe_gauge(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn describe_histogram(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn register_counter(
                &self,
                key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Counter {
                let this_cache = key
                    .labels()
                    .any(|l| l.key() == "cache" && l.value() == WAIT_METRICS_CACHE);
                if !this_cache {
                    return metrics::Counter::noop();
                }
                match key.name() {
                    "sundog_spill_wait_seconds_total" => {
                        metrics::Counter::from_arc(Arc::new(WaitCounter {
                            field: self.metrics.wait_seconds_total.clone(),
                        }))
                    }
                    "sundog_spill_wait_timeouts_total" => {
                        metrics::Counter::from_arc(Arc::new(WaitCounter {
                            field: self.metrics.wait_timeouts_total.clone(),
                        }))
                    }
                    _ => metrics::Counter::noop(),
                }
            }

            fn register_gauge(
                &self,
                key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Gauge {
                let this_cache = key
                    .labels()
                    .any(|l| l.key() == "cache" && l.value() == WAIT_METRICS_CACHE);
                if !this_cache || key.name() != "sundog_spill_waiters" {
                    return metrics::Gauge::noop();
                }
                metrics::Gauge::from_arc(Arc::new(WaitGauge {
                    current: self.metrics.waiters.clone(),
                }))
            }

            fn register_histogram(
                &self,
                _key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Histogram {
                metrics::Histogram::noop()
            }
        }

        /// The cache name only this test opens, so the process-global
        /// recorder ignores waiter/wait activity from every other test.
        const WAIT_METRICS_CACHE: &str = "wait-metrics-only";

        #[tokio::test]
        async fn reserve_tracks_waiters_wait_seconds_and_wait_timeouts() {
            let metrics = WaitMetrics::default();
            let installed = metrics::set_global_recorder(WaitMetricsRecorder {
                metrics: metrics.clone(),
            })
            .is_ok();
            if !installed {
                // Another test already holds the global recorder slot.
                return;
            }

            // waiters rises while pending; wait_seconds accumulates real time.
            let dir = temp_dir("wait-metrics-resolves");
            let record_len = HEADER_LEN as u64 + 4 + 4;
            let record_len_u32 = u32::try_from(record_len).unwrap();
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, WAIT_METRICS_CACHE).unwrap();
            tier.pause_flusher();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));
            assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));

            let reserve_fut = tier.reserve(record_len_u32, Duration::from_secs(10));
            tokio::pin!(reserve_fut);
            let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut reserve_fut).await;
            assert!(
                not_yet.is_err(),
                "no room yet; reserve must still be pending"
            );
            assert!(
                (*metrics.waiters.lock().unwrap() - 1.0).abs() < f64::EPSILON,
                "sundog_spill_waiters counts this one pending reserve call"
            );

            // Lower-bounded so the whole-second truncation is observable.
            tokio::time::sleep(Duration::from_millis(1200)).await;
            tier.resume_flusher();
            let reservation = tokio::time::timeout(Duration::from_secs(5), &mut reserve_fut)
                .await
                .expect("reserve resolves once the flusher frees room")
                .expect("reserve succeeds once the flusher frees room");
            drop(reservation);

            assert!(
                metrics.waiters.lock().unwrap().abs() < f64::EPSILON,
                "the guard decrements sundog_spill_waiters back to zero once reserve resolves"
            );
            assert!(
                *metrics.wait_seconds_total.lock().unwrap() >= 1,
                "sundog_spill_wait_seconds_total accumulates at least the one whole second this \
                 call was genuinely blocked"
            );
            assert_eq!(
                *metrics.wait_timeouts_total.lock().unwrap(),
                0,
                "this call resolved on its own; it never hit its own timeout"
            );
            let _ = fs::remove_dir_all(&dir);

            // An elapsed timeout increments wait_timeouts_total once.
            let dir = temp_dir("wait-metrics-timeout");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .flush_queue_bytes(record_len);
            let tier = SpillTier::open(&cfg, WAIT_METRICS_CACHE).unwrap();
            tier.pause_flusher();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));
            assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));

            let result = tier
                .reserve(record_len_u32, Duration::from_millis(100))
                .await;
            assert!(matches!(result, Err(SpillWaitTimedOut)));
            assert_eq!(
                *metrics.wait_timeouts_total.lock().unwrap(),
                1,
                "the elapsed timeout increments sundog_spill_wait_timeouts_total exactly once"
            );
            assert!(
                metrics.waiters.lock().unwrap().abs() < f64::EPSILON,
                "the guard decrements sundog_spill_waiters on the timeout path too"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn write_segment_failure_abandons_every_job_in_the_failed_segment() {
            // One region reopened read-only, so writing fails for every
            // job; DROP_REASONS_CACHE filters this tier's drops.
            let counts = DropCounts::default();
            let installed = metrics::set_global_recorder(DropRecorder {
                counts: counts.clone(),
            })
            .is_ok();

            let dir = temp_dir("write-fails");
            let record_len = HEADER_LEN as u64 + 6 + 4; // fixed-width key/value
            let region_bytes = record_len * 4;
            let cfg = SpillConfig::new(&dir, region_bytes * 2).region_bytes(region_bytes);
            let tier =
                SpillTier::open_with_readonly_regions_for_test(&cfg, DROP_REASONS_CACHE, &[0])
                    .unwrap();
            let sink = Arc::new(RecordingSink::default());

            let jobs: Vec<SpillJob> = (0..3u32)
                .map(|i| job(&format!("key-{i:02}"), b"1234", hlc(u64::from(i) + 1, 0)))
                .collect();
            flush_batch(&tier.inner, &*sink, jobs);

            assert_eq!(
                sink.install_count(),
                0,
                "the failed write never installs anything"
            );
            assert_eq!(
                sink.abandon_count(),
                3,
                "every job in the failed segment reaches abandon, not just the first"
            );
            if installed {
                assert_eq!(
                    counts.get("disk_error"),
                    3,
                    "every job in the failed segment is also counted under \
                     sundog_spill_dropped_total{{reason=\"disk_error\"}}, even though \
                     abandon's restore-to-resident behavior above is unchanged"
                );
            }

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn try_spill_returns_false_before_attach_and_after_close() {
            let dir = temp_dir("before-after");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();

            assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));

            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));
            assert!(tier.try_spill(job("k2", b"v", hlc(2, 0))));

            tier.close();
            assert!(!tier.try_spill(job("k3", b"v", hlc(3, 0))));

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn open_recreates_region_files_and_removes_stale_reg_files() {
            let dir = temp_dir("recreate");
            let cache_dir = dir.join("cache-a");
            fs::create_dir_all(&cache_dir).unwrap();
            fs::write(
                cache_dir.join("garbage.reg"),
                b"leftover-from-a-crashed-run",
            )
            .unwrap();
            write_snapshot_atomic(&cache_dir, b"leftover-from-a-crashed-run");
            fs::write(cache_dir.join("keep-me.txt"), b"not a region file").unwrap();

            let cfg = SpillConfig::new(&dir, 256).region_bytes(64);
            let _tier = SpillTier::open(&cfg, "cache-a").unwrap();

            assert!(!cache_dir.join("garbage.reg").exists());
            assert!(
                !snapshot_path(&cache_dir).exists(),
                "a snapshot left over from a prior incarnation never survives a fresh, \
                 fully-recreated set of regions"
            );
            assert!(cache_dir.join("keep-me.txt").exists());
            for idx in 0..region_count_for(256, 64) {
                let path = cache_dir.join(region_file_name(idx));
                assert!(path.exists());
                assert_eq!(fs::metadata(&path).unwrap().len(), 64);
            }

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn close_drains_the_queue() {
            let dir = temp_dir("drain");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            for i in 0..5u32 {
                let key = format!("k{i}");
                assert!(tier.try_spill(job(&key, b"v", hlc(u64::from(i) + 1, 0))));
            }
            tier.close();

            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 5));

            let _ = fs::remove_dir_all(&dir);
        }

        // --- Checkpoint snapshot serialization, no fsync ---

        #[test]
        fn snapshot_bytes_round_trip_through_parse_snapshot() {
            let loc_a = SpillLoc {
                region: 1,
                offset: 0,
                len: 40,
                generation: 0,
            };
            let loc_b = SpillLoc {
                region: 2,
                offset: 40,
                len: 41,
                generation: 3,
            };
            let entries = vec![
                (Bytes::from_static(b"a"), hlc(1, 0), Some(999), loc_a),
                (Bytes::from_static(b"bb"), hlc(2, 5), None, loc_b),
            ];

            let bytes = build_snapshot_bytes(&entries, 1 << 20, 7, 123_456);
            let (header, parsed) = parse_snapshot(&bytes).expect("a freshly built snapshot parses");

            assert_eq!(header.region_bytes, 1 << 20);
            assert_eq!(header.region_count, 7);
            assert_eq!(header.closed_at_ms, 123_456);
            assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);
            assert_eq!(parsed.len(), 2);
            assert_eq!(parsed[0].key.as_ref(), b"a");
            assert_eq!(parsed[0].ver, hlc(1, 0));
            assert_eq!(parsed[0].expires_at_ms, Some(999));
            assert_eq!(parsed[0].loc, loc_a);
            assert_eq!(parsed[1].key.as_ref(), b"bb");
            assert_eq!(parsed[1].ver, hlc(2, 5));
            assert_eq!(parsed[1].expires_at_ms, None);
            assert_eq!(parsed[1].loc, loc_b);
        }

        #[test]
        fn write_snapshot_atomic_then_snapshot_eligibility_round_trips() {
            let dir = temp_dir("snapshot-roundtrip");
            fs::create_dir_all(&dir).unwrap();

            let bytes = build_snapshot_bytes(&[], 4096, 3, 1_000);
            write_snapshot_atomic(&dir, &bytes);

            assert!(
                !snapshot_tmp_path(&dir).exists(),
                "the temporary file is renamed away, never left behind on success"
            );
            let (header, entries) = snapshot_eligibility(&dir, 4096, 3, 1_500, 600_000)
                .expect("a freshly written snapshot within tombstone_ttl is eligible");
            assert_eq!(header.closed_at_ms, 1_000);
            assert!(entries.is_empty());

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn a_truncated_snapshot_falls_back_to_open_cleanly() {
            // root plays SpillConfig::dir; open() computes the same
            // root.join(cache_name) this test writes into.
            let root = temp_dir("snapshot-truncated");
            let cache_name = "cache-a";
            let cache_dir = root.join(cache_name);
            fs::create_dir_all(&cache_dir).unwrap();
            let bytes = build_snapshot_bytes(&[], 4096, 3, 123_456);
            write_snapshot_atomic(&cache_dir, &bytes[..bytes.len() / 2]);

            assert!(
                parse_snapshot(&fs::read(snapshot_path(&cache_dir)).unwrap()).is_none(),
                "a truncated snapshot is never trusted"
            );

            // open() never reads the snapshot, so a corrupt one can't
            // block it.
            let cfg = SpillConfig::new(&root, 1 << 20).region_bytes(4096);
            assert!(SpillTier::open(&cfg, cache_name).is_ok());

            let _ = fs::remove_dir_all(&root);
        }

        #[test]
        fn a_snapshot_with_a_flipped_checksum_bit_reads_as_none() {
            let dir = temp_dir("snapshot-corrupt");
            fs::create_dir_all(&dir).unwrap();
            let bytes = build_snapshot_bytes(&[], 1 << 20, 7, 123_456);
            write_snapshot_atomic(&dir, &bytes);

            let path = snapshot_path(&dir);
            let mut bytes = fs::read(&path).unwrap();
            // Flips a bit past the checksum field, so only it catches this.
            let flip_at = size_of::<u64>();
            bytes[flip_at] ^= 0x01;
            fs::write(&path, &bytes).unwrap();

            assert!(
                parse_snapshot(&bytes).is_none(),
                "a bit-flipped snapshot fails its checksum"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        // --- SpillSink::install_new ---

        #[test]
        fn install_new_inserts_a_fresh_key_into_the_sinks_live_map() {
            let sink = RecordingSink::default();
            let kb = Bytes::from_static(b"fresh-key");
            let ver = hlc(1, 0);
            let loc = SpillLoc {
                region: 0,
                offset: 0,
                len: 16,
                generation: 0,
            };

            let inserted = sink.install_new(0, &kb, 0, ver, Some(999), loc, 1);

            assert!(inserted);
            assert_eq!(sink.install_new_count(), 1);
            assert_eq!(sink.live.lock().unwrap().get(&kb), Some(&(ver, loc)));
        }

        #[test]
        fn install_new_never_overwrites_an_already_present_key() {
            let sink = RecordingSink::default();
            let kb = Bytes::from_static(b"already-there");
            let first_ver = hlc(1, 0);
            let loc = SpillLoc {
                region: 0,
                offset: 0,
                len: 16,
                generation: 0,
            };
            assert!(sink.install_new(0, &kb, 0, first_ver, None, loc, 1));

            let second_ver = hlc(2, 0);
            let second_loc = SpillLoc {
                region: 1,
                offset: 0,
                len: 16,
                generation: 0,
            };
            let inserted = sink.install_new(0, &kb, 0, second_ver, None, second_loc, 1);

            assert!(
                !inserted,
                "install_new never overwrites a key that is already present"
            );
            assert_eq!(sink.install_new_count(), 1);
            assert_eq!(
                sink.live.lock().unwrap().get(&kb),
                Some(&(first_ver, loc)),
                "the original entry is untouched"
            );
        }

        // --- SpillTier::reopen ---

        /// Builds a checkpoint snapshot on disk from `sink`'s recorded
        /// live entries, mirroring what a real checkpoint writes.
        fn write_snapshot_for_test(
            cache_dir: &Path,
            sink: &RecordingSink,
            region_bytes: u64,
            region_count: u32,
            closed_at_ms: u64,
            expires_at_ms: Option<u64>,
        ) {
            let entries: Vec<(Bytes, Hlc, Option<u64>, SpillLoc)> = sink
                .live
                .lock()
                .unwrap()
                .iter()
                .map(|(key, (ver, loc))| (key.clone(), *ver, expires_at_ms, *loc))
                .collect();
            let bytes = build_snapshot_bytes(&entries, region_bytes, region_count, closed_at_ms);
            write_snapshot_atomic(cache_dir, &bytes);
        }

        #[test]
        fn reopen_warm_reload_recovers_a_record_written_before_a_clean_close() {
            let dir = temp_dir("reopen-warm-basic");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            let ver = hlc(1, 0);
            flush_batch(&tier.inner, &*sink, vec![job("hello", b"world", ver)]);
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, None);

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("reopen against a cleanly closed tier succeeds");

            assert!(
                outcome.warm,
                "a valid snapshot within tombstone_ttl reopens warm"
            );
            assert_eq!(outcome.reason, None);
            assert_eq!(outcome.records_installed, 1);
            assert_eq!(
                outcome.warm_parts,
                HashSet::from([crate::store::PartId::of_key(b"hello")]),
                "warm_parts names exactly the one part a record was actually replayed for"
            );
            assert_eq!(fresh_sink.install_new_count(), 1);
            let (recovered_ver, loc) = *fresh_sink
                .live
                .lock()
                .unwrap()
                .get(&Bytes::from_static(b"hello"))
                .expect("the record replayed");
            assert_eq!(recovered_ver, ver);
            let bytes = outcome
                .tier
                .read_at(loc)
                .unwrap()
                .expect("the reopened tier still serves the record's original on-disk bytes");
            assert_eq!(bytes.encoded.as_ref(), b"world");
            assert!(
                !snapshot_path(&cache_dir).exists(),
                "a successful warm reopen deletes the snapshot it just replayed"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_falls_back_cold_when_a_snapshot_entrys_record_fails_to_decode() {
            let dir = temp_dir("reopen-bad-record");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(&tier.inner, &*sink, vec![job("a", b"1", hlc(1, 0))]);
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

            // Corrupts the pointed-at record; the flip must land inside
            // its bytes, not the zero-filled padding.
            let record_len = HEADER_LEN + 1 + 1;
            let region_path = cache_dir.join(region_file_name(0));
            let mut bytes = fs::read(&region_path).unwrap();
            bytes[record_len - 1] ^= 0x01;
            fs::write(&region_path, &bytes).unwrap();

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("the cold fallback still succeeds");

            assert!(
                !outcome.warm,
                "one bad entry falls the whole reopen back cold, conservatively"
            );
            assert_eq!(outcome.reason, Some("bad_region"));
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_falls_back_cold_when_a_snapshot_entry_points_past_its_region() {
            let dir = temp_dir("reopen-past-region");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let _tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let region_count = region_count_for(1 << 20, 4096);
            let bogus_loc = SpillLoc {
                region: 0,
                offset: 4090,
                len: 16,
                generation: 0,
            };
            let entries = vec![(Bytes::from_static(b"k"), hlc(1, 0), Some(999), bogus_loc)];
            let bytes = build_snapshot_bytes(&entries, 4096, region_count, 1_000);
            write_snapshot_atomic(&cache_dir, &bytes);

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("the cold fallback still succeeds");

            assert!(!outcome.warm);
            assert_eq!(outcome.reason, Some("bad_region"));
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_falls_back_cold_when_the_snapshot_checksum_is_corrupt() {
            let dir = temp_dir("reopen-corrupt-checksum");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

            let path = snapshot_path(&cache_dir);
            let mut bytes = fs::read(&path).unwrap();
            let flip_at = size_of::<u64>();
            bytes[flip_at] ^= 0x01;
            fs::write(&path, &bytes).unwrap();

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("the cold fallback still succeeds");

            assert!(!outcome.warm);
            assert_eq!(
                outcome.reason,
                Some("no_snapshot"),
                "a checksum failure collapses to the same \"nothing to trust\" outcome a \
                 missing snapshot gets"
            );
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_with_no_snapshot_falls_back_to_open_with_reason_no_snapshot() {
            // A crash: region files exist but nothing ever closed cleanly,
            // so no snapshot was written.
            let dir = temp_dir("reopen-no-snapshot");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let _tier = SpillTier::open(&cfg, "cache-a").unwrap();

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("the cold fallback still succeeds");

            assert!(!outcome.warm);
            assert_eq!(outcome.reason, Some("no_snapshot"));
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_refuses_past_tombstone_ttl_and_falls_back_to_open() {
            let dir = temp_dir("reopen-ttl-exceeded");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            // A deterministic closed_at_ms so the tombstone_ttl comparison
            // is exact, independent of test timing.
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

            let fresh_sink = RecordingSink::default();
            let tombstone_ttl_ms = 600_000;
            let now_ms = 1_000 + tombstone_ttl_ms + 1;
            let outcome = SpillTier::reopen(
                &cfg,
                "cache-a",
                &fresh_sink,
                now_ms,
                tombstone_ttl_ms,
                |_| true,
            )
            .expect("the cold fallback still succeeds");

            assert!(!outcome.warm);
            assert_eq!(outcome.reason, Some("downtime_exceeded"));
            assert_eq!(outcome.records_installed, 0);
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_refuses_on_a_stale_snapshot_format_version_and_falls_back_to_open() {
            let dir = temp_dir("reopen-stale-snapshot");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let _tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");

            // Everything but format_version checks out.
            let region_count = region_count_for(1 << 20, 4096);
            write_stale_snapshot_for_test(&cache_dir, 4096, region_count, 1_000);

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_000, 600_000, |_| true)
                .expect("the cold fallback still succeeds");

            assert!(!outcome.warm);
            assert_eq!(outcome.reason, Some("stale_snapshot"));
            assert_eq!(outcome.records_installed, 0);
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_refuses_on_a_region_bytes_or_region_count_mismatch_and_falls_back_to_open() {
            let dir = temp_dir("reopen-config-mismatch");
            let region_len = HEADER_LEN as u64 + 1 + 1;
            let cfg = SpillConfig::new(&dir, 2 * region_len)
                .region_bytes(region_len)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
            tier.close();
            let region_count = region_count_for(2 * region_len, region_len).max(2);
            write_snapshot_for_test(
                &cache_dir,
                &sink,
                region_len,
                region_count,
                1_000,
                Some(999),
            );

            // A different region_bytes than the snapshot was written under.
            let resized_cfg = SpillConfig::new(&dir, 4 * region_len)
                .region_bytes(2 * region_len)
                .warm_reopen(true);
            let fresh_sink = RecordingSink::default();
            let outcome =
                SpillTier::reopen(&resized_cfg, "cache-a", &fresh_sink, 1_000, 600_000, |_| {
                    true
                })
                .expect("the cold fallback still succeeds");

            assert!(!outcome.warm);
            assert_eq!(outcome.reason, Some("config_mismatch"));
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_is_disabled_by_default_even_against_an_otherwise_eligible_snapshot() {
            let dir = temp_dir("reopen-disabled");
            // `warm_reopen` left at its default, `false`.
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("the cold fallback still succeeds");

            assert!(!outcome.warm);
            assert_eq!(outcome.reason, Some("disabled"));
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_falls_back_to_open_when_a_region_file_is_missing() {
            let dir = temp_dir("reopen-missing-region");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

            // The snapshot names this dir eligible, but a region file is gone.
            fs::remove_file(cache_dir.join(region_file_name(1))).unwrap();

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("the cold fallback still succeeds");

            assert!(!outcome.warm);
            assert_eq!(outcome.reason, Some("bad_region"));
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_filters_out_a_bucket_this_node_no_longer_owns() {
            let dir = temp_dir("reopen-owned-filter");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, None);

            let fresh_sink = RecordingSink::default();
            let outcome =
                SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| false)
                    .expect("reopen against a cleanly closed tier succeeds");

            assert!(outcome.warm, "the tier itself still reopens warm");
            assert_eq!(
                outcome.records_installed, 0,
                "every record is filtered out by an owned-bucket predicate that owns nothing"
            );
            assert!(
                outcome.warm_parts.is_empty(),
                "no bucket was actually replayed into, so warm_parts names none"
            );
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_warm_buckets_names_only_the_buckets_a_record_survived_the_owned_filter_for() {
            // Two keys in different buckets; only one passes the owned
            // predicate, so warm_parts must name only that one.
            let dir = temp_dir("reopen-owned-filter-partial");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(
                &tier.inner,
                &*sink,
                vec![job("k", b"1", hlc(1, 0)), job("a", b"2", hlc(1, 0))],
            );
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, None);

            let kept_bucket = crate::store::PartId::of_key(b"k");
            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |p| {
                p == kept_bucket
            })
            .expect("reopen against a cleanly closed tier succeeds");

            assert!(outcome.warm);
            assert_eq!(
                outcome.warm_parts,
                HashSet::from([kept_bucket]),
                "only the owned part's key survives the filter and lands in warm_parts"
            );

            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reopen_drops_an_already_expired_record() {
            let dir = temp_dir("reopen-expired");
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(4096)
                .warm_reopen(true);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let cache_dir = dir.join("cache-a");
            let sink = Arc::new(RecordingSink::default());
            flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
            tier.close();
            let region_count = region_count_for(1 << 20, 4096);
            // `now_ms` past this expiry when reopen runs, below.
            write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

            let fresh_sink = RecordingSink::default();
            let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
                .expect("reopen against a cleanly closed tier succeeds");

            assert!(outcome.warm);
            assert_eq!(
                outcome.records_installed, 0,
                "an already-expired record is dropped"
            );
            assert_eq!(fresh_sink.install_new_count(), 0);

            let _ = fs::remove_dir_all(&dir);
        }

        /// A [`SpillSink`] whose `install` always rejects the flush, so
        /// `flush_one` counts `sundog_spill_dropped_total{reason="obsolete"}`
        /// instead of `sundog_spill_writes_total`.
        struct RejectingSink;

        impl SpillSink for RejectingSink {
            fn install(&self, _: usize, _: &Bytes, _: u64, _: Hlc, _: SpillLoc, _: u32) -> bool {
                false
            }

            #[allow(clippy::too_many_arguments)]
            fn install_new(
                &self,
                _: usize,
                _: &Bytes,
                _: u64,
                _: Hlc,
                _: Option<u64>,
                _: SpillLoc,
                _: u32,
            ) -> bool {
                false
            }

            fn reclaim(&self, _: u32, _: u32, _: &[(usize, Bytes)]) -> usize {
                0
            }

            fn abandon(&self, _: usize, _: &Bytes, _: u64, _: Hlc, _: u32) {}
        }

        /// Captures `sundog_spill_dropped_total{reason}` increments by
        /// `reason`, ignoring every other metric.
        #[derive(Clone, Default)]
        struct DropCounts(Arc<StdMutex<StdHashMap<String, u64>>>);

        impl DropCounts {
            fn get(&self, reason: &str) -> u64 {
                *self.0.lock().unwrap().get(reason).unwrap_or(&0)
            }
        }

        struct ReasonCounter {
            reason: String,
            counts: DropCounts,
        }

        impl metrics::CounterFn for ReasonCounter {
            fn increment(&self, value: u64) {
                *self
                    .counts
                    .0
                    .lock()
                    .unwrap()
                    .entry(self.reason.clone())
                    .or_insert(0) += value;
            }

            fn absolute(&self, value: u64) {
                *self
                    .counts
                    .0
                    .lock()
                    .unwrap()
                    .entry(self.reason.clone())
                    .or_insert(0) = value;
            }
        }

        struct DropRecorder {
            counts: DropCounts,
        }

        impl metrics::Recorder for DropRecorder {
            fn describe_counter(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn describe_gauge(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn describe_histogram(
                &self,
                _key: metrics::KeyName,
                _unit: Option<metrics::Unit>,
                _description: metrics::SharedString,
            ) {
            }

            fn register_counter(
                &self,
                key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Counter {
                let this_cache = key
                    .labels()
                    .any(|l| l.key() == "cache" && l.value() == DROP_REASONS_CACHE);
                if key.name() != "sundog_spill_dropped_total" || !this_cache {
                    return metrics::Counter::noop();
                }
                let reason = key
                    .labels()
                    .find(|l| l.key() == "reason")
                    .map(|l| l.value().to_string())
                    .unwrap_or_default();
                metrics::Counter::from_arc(Arc::new(ReasonCounter {
                    reason,
                    counts: self.counts.clone(),
                }))
            }

            fn register_gauge(
                &self,
                _key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Gauge {
                metrics::Gauge::noop()
            }

            fn register_histogram(
                &self,
                _key: &metrics::Key,
                _metadata: &metrics::Metadata<'_>,
            ) -> metrics::Histogram {
                metrics::Histogram::noop()
            }
        }

        /// The cache name only this test opens, so the process-global
        /// recorder ignores drops from every other test's tier.
        const DROP_REASONS_CACHE: &str = "drop-reasons-only";

        #[test]
        fn try_spill_and_flush_record_the_documented_drop_reason_for_each_case() {
            let counts = DropCounts::default();
            // `metrics::set_global_recorder` is a single process-global
            // slot: if another test in this binary already won it, this one
            // silently observes nothing and skips its assertions, the same
            // way `tests/prometheus_exporter.rs`'s own tests tolerate
            // losing that race rather than assuming they run first.
            let installed = metrics::set_global_recorder(DropRecorder {
                counts: counts.clone(),
            })
            .is_ok();
            if !installed {
                return;
            }

            // too_large: a record too big for the tier's own region size.
            let dir = temp_dir("reasons-too-large");
            let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
            let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
            let sink: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&sink));
            assert!(!tier.try_spill(job("k", &[0u8; 100], hlc(1, 0))));
            assert_eq!(counts.get("too_large"), 1);
            assert_eq!(counts.get("closed"), 0);
            let _ = fs::remove_dir_all(&dir);

            // closed: try_spill after SpillTier::close.
            let dir = temp_dir("reasons-closed");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
            let sink: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&sink));
            tier.close();
            assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));
            assert_eq!(counts.get("closed"), 1);
            let _ = fs::remove_dir_all(&dir);

            // queue_full: never attached, so there is no channel to send on.
            let dir = temp_dir("reasons-queue-full");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
            assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));
            assert_eq!(counts.get("queue_full"), 1);
            let _ = fs::remove_dir_all(&dir);

            // obsolete: the flusher writes the record, but the sink rejects
            // installing it.
            let dir = temp_dir("reasons-obsolete");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
            let sink: Arc<dyn SpillSink> = Arc::new(RejectingSink);
            tier.attach(Arc::downgrade(&sink));
            assert!(tier.try_spill(job("k", b"v", hlc(1, 0))));
            assert!(poll_until(POLL_TIMEOUT, || counts.get("obsolete") == 1));
            let _ = fs::remove_dir_all(&dir);

            // deferred: the same three refusals, but recorded under
            // "deferred" instead once `set_keep_resident_when_refused(true)`
            // is in effect, and the specific reason's own count stays put.
            let dir = temp_dir("reasons-deferred-too-large");
            let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
            let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
            tier.set_keep_resident_when_refused(true);
            let sink: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&sink));
            assert!(!tier.try_spill(job("k", &[0u8; 100], hlc(1, 0))));
            assert_eq!(counts.get("deferred"), 1);
            assert_eq!(
                counts.get("too_large"),
                1,
                "unchanged from the earlier case"
            );
            let _ = fs::remove_dir_all(&dir);

            let dir = temp_dir("reasons-deferred-closed");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
            tier.set_keep_resident_when_refused(true);
            let sink: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&sink));
            tier.close();
            assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));
            assert_eq!(counts.get("deferred"), 2);
            assert_eq!(counts.get("closed"), 1, "unchanged from the earlier case");
            let _ = fs::remove_dir_all(&dir);

            let dir = temp_dir("reasons-deferred-queue-full");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
            tier.set_keep_resident_when_refused(true);
            assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));
            assert_eq!(counts.get("deferred"), 3);
            assert_eq!(
                counts.get("queue_full"),
                1,
                "unchanged from the earlier case"
            );
            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn keep_resident_when_refused_defaults_to_false_and_reflects_the_setter() {
            let dir = temp_dir("keep-resident-flag");
            let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            assert!(!tier.keep_resident_when_refused());
            tier.set_keep_resident_when_refused(true);
            assert!(tier.keep_resident_when_refused());
            tier.set_keep_resident_when_refused(false);
            assert!(!tier.keep_resident_when_refused());
            let _ = fs::remove_dir_all(&dir);
        }
    }
}

/// Kani proofs over the spill tier's sizing arithmetic.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Any capacity and region size give at least one region without panicking.
    #[kani::proof]
    fn region_count_is_at_least_one_for_any_sizes() {
        let capacity_bytes: u64 = kani::any();
        let region_bytes: u64 = kani::any();
        assert!(region_count_for(capacity_bytes, region_bytes) >= 1);
    }

    /// Any flush queue byte size lands between the slot floor and ceiling.
    #[kani::proof]
    fn flush_queue_slots_stay_within_the_clamp() {
        let flush_queue_bytes: u64 = kani::any();
        let slots = flush_queue_slots(flush_queue_bytes);
        assert!(slots >= FLUSH_QUEUE_CAPACITY);
        assert!(slots <= FLUSH_QUEUE_SLOTS_MAX);
    }
}
