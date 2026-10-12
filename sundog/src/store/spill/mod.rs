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

use super::engine::OnRefusal;

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
/// effect. When the tier this config opens refuses a hand-off, a write's
/// eviction in a `Mode::Replicated` or `Mode::Distributed` cache keeps the
/// victim resident, at its full weight, for a later eviction pass to retry,
/// while a `Mode::Local` or `Mode::Invalidation` cache deletes it. A read's
/// promotion keeps the victim resident in every mode.
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
    /// tier's queued bytes past this falls back, for a write's eviction in
    /// `Mode::Local` or `Mode::Invalidation`, to an ordinary delete
    /// (`sundog_spill_dropped_total{reason="queue_full"}`) instead of
    /// piling up fully-resident, unwritten values in RAM: this is what
    /// keeps a lagging disk a plain-eviction problem rather than an
    /// unbounded-RSS one. A `Replicated` or `Distributed` cache, and a read's
    /// eviction in any mode, leaves the refused victim resident instead
    /// (`reason="deferred"`). Own-and-return.
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
/// `"deferred"` when `keeps_resident` is set
/// ([`keeps_refused_victim_resident`]), since the caller leaves the victim
/// resident and retries it on a later eviction pass rather than deleting
/// it, so an operator sees backpressure instead of a
/// delete reason that fails to describe what happened; `specific`
/// (`"too_large"`, `"closed"`, or `"queue_full"`) unchanged otherwise. Pure;
/// unit tested directly.
pub(crate) fn refusal_drop_reason(keeps_resident: bool, specific: &'static str) -> &'static str {
    if keeps_resident { "deferred" } else { specific }
}

/// Whether an eviction pass leaves a victim its tier refuses resident,
/// rather than deleting it: always under [`OnRefusal::KeepResident`], and
/// under [`OnRefusal::TierPolicy`] exactly when `tier_keeps_resident`, the
/// tier's own flag ([`SpillTier::set_keep_resident_when_refused`]), is set.
/// Pure; unit tested directly.
pub(crate) fn keeps_refused_victim_resident(
    on_refusal: OnRefusal,
    tier_keeps_resident: bool,
) -> bool {
    match on_refusal {
        OnRefusal::KeepResident => true,
        OnRefusal::TierPolicy => tier_keeps_resident,
    }
}

/// Test-only: the `sundog_spill_dropped_total{cache, reason}` increments
/// `snapshotter` saw for `cache` since its previous snapshot, by reason,
/// zero counts left out: a snapshot drains the counters.
#[cfg(all(test, not(feature = "sim")))]
pub(crate) fn dropped_since_last_snapshot(
    snapshotter: &metrics_util::debugging::Snapshotter,
    cache: &str,
) -> std::collections::BTreeMap<String, u64> {
    let mut by_reason = std::collections::BTreeMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let label = |name: &str| {
            key.key()
                .labels()
                .find(|l| l.key() == name)
                .map(|l| l.value().to_string())
        };
        if key.key().name() != "sundog_spill_dropped_total"
            || label("cache").as_deref() != Some(cache)
        {
            continue;
        }
        if let (Some(reason), metrics_util::debugging::DebugValue::Counter(n)) =
            (label("reason"), value)
            && n > 0
        {
            *by_reason.entry(reason).or_insert(0) += n;
        }
    }
    by_reason
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
    let (header, key_bytes, value_bytes) = split_record(buf)?;
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

/// Splits `buf` into its header, key and value, or `None` for a short
/// buffer, a bad magic, or lengths that don't add up to exactly the bytes
/// after the header. Checks no checksum.
fn split_record(buf: &[u8]) -> Option<(SpillRecordHeader, &[u8], &[u8])> {
    let (header, rest) = SpillRecordHeader::read_from_prefix(buf).ok()?;
    if header.magic != SPILL_MAGIC {
        return None;
    }
    let key_len = usize::try_from(header.key_len).ok()?;
    let value_len = usize::try_from(header.value_len).ok()?;
    if key_len.checked_add(value_len)? != rest.len() {
        return None;
    }
    let (key_bytes, value_bytes) = rest.split_at(key_len);
    Some((header, key_bytes, value_bytes))
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
            newly_indexed.push(ReclaimRef::new(stripe_idx, hash, loc.offset));
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
/// for reuse: walks its reverse index, hands every listed entry to
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
    let rows = std::mem::take(&mut *region.reverse_index.lock());
    let _purged = sink.reclaim(next, generation, &rows);

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
mod tests;

/// Kani proofs over the spill tier's sizing arithmetic.
#[cfg(kani)]
mod kani_proofs;
