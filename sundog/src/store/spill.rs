//! Local NVMe/SSD spill tier: a FIFO ring of fixed-size region files that
//! extends a size-bounded cache's resident capacity onto disk.
//!
//! # Shape
//!
//! A `SpillTier` owns `region_count_for(capacity_bytes, region_bytes)`
//! preallocated region files, `<dir>/<cache>/spill-XXXXXXXX.reg`. `open`
//! recreates them from scratch every time; see `SpillTier::open`. One
//! region is "active": new records append to it at its `write_cursor`. When
//! a record doesn't fit, the next region in round-robin order is reclaimed
//! and becomes the new active region. Its still-current keys are purged
//! from the engine's `live` tables via `SpillSink::reclaim`, *before* it is
//! reused. A `generation` counter per region, bumped on every reclaim, lets
//! a read recognize a pointer into a region that has since rotated out
//! from under it. See `SpillTier::read_at`.
//!
//! Writes go through one dedicated flusher thread, `std::thread` rather
//! than a tokio task, since `SpillTier::try_spill` must work from sync and
//! async callers alike, with no blocking I/O on the caller's stripe-locked
//! hot path. It is fed by a bounded [`std::sync::mpsc::sync_channel`]. The
//! flusher never re-inserts a key into the engine's tables. It calls back
//! through `SpillSink` to flip an *existing* entry's payload in place, and
//! only when the entry's current state still matches what was spilled,
//! verified via `spilled_is_current`. A victim's weight is zeroed and freed
//! from `total_weight` the moment eviction hands it off, before this
//! thread ever touches it, so a queued-but-unwritten job that never
//! reaches `install`, a failed region write, calls back through
//! `SpillSink::abandon` instead, to put that weight back. Reads are
//! positional, `pread`/`pwrite`-style, one syscall each, with no `open()`
//! on the hot path, using a `read_exact_at`/`write_all_at` unix
//! implementation and a `seek_read`/`seek_write` windows one below.
//!
//! # No wire effect
//!
//! Spilling is a purely local, per-node representation choice for a value
//! already accepted and versioned. It never changes what goes on the wire.
//! A promoted or served-from-disk value round-trips identically to a
//! resident one, so it needs no `wire::PROTOCOL_VERSION` bump and no
//! `store::model`/`sundog-fuzz` changes: those cover "everything downstream
//! of a successful wire decode," and spilling has no wire decode of its own.
//!
//! # `sim` interaction
//!
//! The `sim` feature swaps only `net::tcp`'s transport seam for turmoil's; it
//! gives no determinism or virtual-time guarantee over this module's real
//! filesystem I/O or the flusher's real OS thread. A `SpillConfig`'d cache
//! driven inside a turmoil `Sim` does real wall-clock disk I/O interleaved
//! with virtual-time network traffic, an orthogonal, non-composable
//! combination. `spill` and `sim` are never enabled together in this crate's
//! CI, and this module's own I/O tests are gated accordingly; see the
//! `tests` module below.
//!
//! # A note on this module's `pub(crate)` surface
//!
//! `store::engine`'s `Payload::Spilled` integration consumes every item
//! here: eviction hands a victim to `try_spill`, `Engine`'s `SpillSink` impl
//! installs and reclaims through `spilled_is_current`, and abandons a job
//! whose write never reached `install`, `open`/`attach` are
//! called from `Shard::with_spill`, and `read_at`/`bytes_used`/
//! `SpilledBytes` back `Shard::get`/`Shard::get_or_load`'s promotion path
//! and the anti-entropy/snapshot read path, both behind `spawn_blocking`.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Weak};
use std::thread;

use bytes::Bytes;
use parking_lot::Mutex;
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
/// Bound on the flusher's job queue: a scheduling buffer, not a capacity
/// knob. Config-independent by design: every [`SpillConfig`] gets the same
/// bound regardless of `capacity_bytes`. A bulk insert can evict thousands
/// of entries under one `enforce_capacity` lock hold, all queued in a
/// burst; at the old bound of 256 most of a large burst overflowed the
/// queue and was dropped with `reason = "queue_full"` instead of spilled.
/// 8192 absorbs that: a [`SpillJob`] holds two refcounted [`Bytes`] handles
/// plus a few words, so the queue's own memory footprint stays small even
/// at this depth, and the bytes those handles point at are already
/// resident in RAM regardless of whether the job sits in this queue or the
/// entry sits in `live` — queuing it costs nothing beyond what is already
/// paid for.
const FLUSH_QUEUE_CAPACITY: usize = 8192;
/// Corruption/format-skew guard at the front of every [`SpillRecordHeader`].
/// Built from its ASCII bytes so the constant's value and its on-disk byte
/// order always agree: `SPILL_MAGIC.to_le_bytes() == *b"SPIL"`.
const SPILL_MAGIC: u32 = u32::from_le_bytes(*b"SPIL");
/// Fixed on-disk header size preceding every record's key and value bytes.
const HEADER_LEN: usize = size_of::<SpillRecordHeader>();

/// Disk budget and layout knobs for a cache's optional spill tier.
///
/// `dir`/`capacity_bytes` have no default, since a disk budget is never
/// safe to assume, but `region_bytes` and `read_concurrency` do. Construct
/// with [`SpillConfig::new`] and adjust either default with the matching
/// builder method; [`SpillConfig::region_bytes_value`] and
/// [`SpillConfig::read_concurrency_value`] read back whatever is in effect.
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
}

impl SpillConfig {
    /// Starts a config with the default `region_bytes`, 64 MiB, and
    /// `read_concurrency`, 16.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, capacity_bytes: u64) -> Self {
        Self {
            dir: dir.into(),
            capacity_bytes,
            region_bytes: DEFAULT_REGION_BYTES,
            read_concurrency: DEFAULT_READ_CONCURRENCY,
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

    /// Checks this config before it is used to [`SpillTier::open`] a tier.
    ///
    /// Rejects a zero `region_bytes`, a `region_bytes` too large to address
    /// with the tier's 32-bit on-disk offsets, and a `capacity_bytes` less
    /// than twice `region_bytes`. The last rule prevents a hazard where a
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
        Ok(())
    }
}

/// Where one spilled record lives: which region, at what offset and length,
/// stamped with the region's generation at write time. A read whose region
/// generation has since moved on treats the record as gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    ) -> bool;

    /// `region` at `generation` is about to be reused. Under each stripe
    /// write lock, remove every listed key whose payload is still
    /// `Spilled(loc)` with `loc.region == region && loc.generation ==
    /// generation`, XOR its fingerprint out of the digest, and decrement
    /// `live_count`. Returns how many were removed.
    fn reclaim(&self, region: u32, generation: u32, keys: &[(usize, Bytes)]) -> usize;

    /// A queued job for `key_bytes` was never installed: its region write
    /// failed, or the tier stopped accepting jobs while this one was still
    /// queued unwritten. Either way the victim's weight, zeroed at
    /// hand-off, needs restoring. Under the stripe write lock, finds a
    /// `Resident` entry at exactly `ver` with weight `0`, recomputes its
    /// weight through the weigher, stores it, and adds it back to
    /// `total_weight`. A key whose stored state changed in the meantime, a
    /// fresh write, a tombstone, or (impossible in practice, but never
    /// assumed) an install that already ran, is left untouched: that
    /// change already accounted for the weight this job would have
    /// restored.
    fn abandon(&self, stripe_idx: usize, key_bytes: &Bytes, hash: u64, ver: Hlc);
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

/// Region count for a capacity/region-size pair. Pure; unit-tested directly.
/// [`SpillConfig::validate`] additionally requires the result be at least
/// 2, since a lone region would be both the active writer and the only
/// candidate for FIFO reclaim. This function itself stays a simple
/// division; the floor is enforced by the caller, [`SpillTier::open`].
pub(crate) fn region_count_for(capacity_bytes: u64, region_bytes: u64) -> u32 {
    u32::try_from((capacity_bytes / region_bytes.max(1)).max(1)).unwrap_or(u32::MAX)
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
}

impl Inner {
    /// Increments `sundog_spill_dropped_total{cache,reason}`. `reason` is
    /// one of: `"too_large"`, the record can never fit any region, checked
    /// by [`record_too_large`] before it is ever queued; `"closed"`, a
    /// [`SpillTier::try_spill`] call after [`SpillTier::close`];
    /// `"queue_full"`, the flusher's bounded channel has no room, or was
    /// never attached; or `"obsolete"`, the flusher wrote the record, but
    /// [`SpillSink::install`] rejected it because the key's state had
    /// already moved on.
    fn record_dropped(&self, reason: &'static str) {
        metrics::counter!(
            "sundog_spill_dropped_total",
            "cache" => self.cache_name.clone(),
            "reason" => reason,
        )
        .increment(1);
    }

    fn record_write(&self) {
        metrics::counter!("sundog_spill_writes_total", "cache" => self.cache_name.clone())
            .increment(1);
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
}

// A gauge only needs f64's exact-integer range, up to 2^53, which
// comfortably covers realistic spill capacities, petabytes, with no
// meaningful precision loss. Unlike an entry count, a byte count routinely
// exceeds `u32::MAX`.
#[allow(clippy::cast_precision_loss)]
fn bytes_used_f64(bytes: u64) -> f64 {
    bytes as f64
}

/// A FIFO ring of fixed-size region files extending a cache's resident
/// capacity onto disk. See the module docs for the write/read/rotation
/// mechanics. Opaque: constructed with [`SpillTier::open`], driven through
/// [`SpillTier::attach`]/[`SpillTier::try_spill`]/[`SpillTier::read_at`], torn
/// down with [`SpillTier::close`].
pub(crate) struct SpillTier {
    inner: Arc<Inner>,
    sender: Mutex<Option<SyncSender<SpillJob>>>,
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
    /// the engine implementing [`SpillSink`] exists.
    ///
    /// # Errors
    ///
    /// Returns an error if `cfg` fails [`SpillConfig::validate`], the
    /// directory cannot be created or listed, a stale `*.reg` file cannot be
    /// removed, or a region file cannot be created or preallocated.
    pub(crate) fn open(cfg: &SpillConfig, cache_name: &str) -> io::Result<Self> {
        cfg.validate()
            .map_err(|reason| io::Error::new(io::ErrorKind::InvalidInput, reason))?;

        let dir = cfg.dir.join(cache_name);
        fs::create_dir_all(&dir)?;
        remove_stale_region_files(&dir)?;

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
            regions.push(RegionState {
                file,
                write_cursor: AtomicU32::new(0),
                generation: AtomicU32::new(0),
                used_bytes: AtomicU64::new(0),
                reverse_index: Mutex::new(Vec::new()),
            });
        }

        let inner = Arc::new(Inner {
            regions: regions.into_boxed_slice(),
            region_bytes: region_bytes_u32,
            active: AtomicU32::new(0),
            bytes_used: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            cache_name: cache_name.to_string(),
        });

        Ok(Self {
            inner,
            sender: Mutex::new(None),
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
    /// constructing the engine that implements [`SpillSink`].
    pub(crate) fn attach(&self, sink: Weak<dyn SpillSink>) {
        let (tx, rx) = mpsc::sync_channel(FLUSH_QUEUE_CAPACITY);
        let inner = Arc::clone(&self.inner);
        let name = format!("sundog-spill-{}", inner.cache_name);
        let spawned = thread::Builder::new()
            .name(name)
            .spawn(move || flusher_loop(&inner, &rx, &sink))
            .is_ok();
        if spawned {
            *self.sender.lock() = Some(tx);
        }
    }

    /// Non-blocking, best-effort: `false` means the record can never fit any
    /// region, `reason = "too_large"`, or [`SpillTier::close`] has run,
    /// `reason = "closed"`, or the flusher's queue has no room, or was
    /// never attached, `reason = "queue_full"` either way. The caller
    /// must fall back to an unconditional delete. Never touches disk on
    /// this call, so it is safe to call while holding a stripe write
    /// lock.
    pub(crate) fn try_spill(&self, job: SpillJob) -> bool {
        if self.inner.closed.load(Ordering::Acquire) {
            self.inner.record_dropped("closed");
            return false;
        }
        let record_len = HEADER_LEN as u64 + job.key_bytes.len() as u64 + job.encoded.len() as u64;
        if record_too_large(record_len, u64::from(self.inner.region_bytes)) {
            self.inner.record_dropped("too_large");
            return false;
        }
        let sent = {
            let sender = self.sender.lock();
            sender.as_ref().is_some_and(|tx| tx.try_send(job).is_ok())
        };
        if !sent {
            self.inner.record_dropped("queue_full");
        }
        sent
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
        // The region may have rotated while this read was in flight; a
        // generation bump after the fact means these bytes may already
        // belong to an unrelated later record.
        if region.generation.load(Ordering::Acquire) != loc.generation {
            return Ok(None);
        }
        Ok(decode_record(&buf))
    }

    /// Live, un-reclaimed, bytes across all regions.
    pub(crate) fn bytes_used(&self) -> u64 {
        self.inner.bytes_used.load(Ordering::Acquire)
    }

    /// The cache name this tier was [`SpillTier::open`]ed under. The
    /// `cache` label every metric this module or its `SpillSink` caller,
    /// `engine::Engine`, publishes carries.
    pub(crate) fn cache_name(&self) -> &str {
        &self.inner.cache_name
    }

    /// Stops accepting new spills and drops the flusher's sender, so its
    /// `recv()` loop drains whatever is already queued and then exits on its
    /// own. Never joins the flusher thread: this must be safe to call from
    /// an async context without blocking it. Region file handles close via
    /// `Drop` once every clone of the shared inner state is gone. Nothing is
    /// fsync'd or persisted, matching the tier's fully lossy contract.
    pub(crate) fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        *self.sender.lock() = None;
    }

    /// Whether [`SpillTier::close`] has run. Test-facing: production code
    /// only ever needs `try_spill`'s own `false` return to know a tier is
    /// unusable, never a direct closed check.
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
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

/// Parses `buf`, one record's bytes as read off disk, into
/// [`SpilledBytes`], or `None` for anything that doesn't check out: a short
/// buffer, a bad magic, a length mismatch, or a checksum mismatch. Every
/// one of these is treated identically. A corrupted or torn record reads
/// like a record that was never there.
fn decode_record(buf: &[u8]) -> Option<SpilledBytes> {
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
    Some(SpilledBytes {
        ver: Hlc {
            wall_ms: header.wall_ms,
            logical: header.logical,
            node: NodeId::from(header.node),
        },
        expires_at_ms,
        encoded: Bytes::copy_from_slice(value_bytes),
    })
}

fn flusher_loop(inner: &Arc<Inner>, rx: &Receiver<SpillJob>, sink: &Weak<dyn SpillSink>) {
    while let Ok(job) = rx.recv() {
        let Some(sink) = sink.upgrade() else {
            return;
        };
        flush_one(inner, sink.as_ref(), job);
    }
}

fn flush_one(inner: &Inner, sink: &dyn SpillSink, job: SpillJob) {
    let Ok(key_len) = u32::try_from(job.key_bytes.len()) else {
        return; // unreachable: try_spill already bounds this by region_bytes
    };
    let Ok(value_len) = u32::try_from(job.encoded.len()) else {
        return; // unreachable: try_spill already bounds this by region_bytes
    };
    let Ok(header_len) = u32::try_from(HEADER_LEN) else {
        return; // unreachable: HEADER_LEN is a small fixed constant
    };
    let Some(record_len) = header_len
        .checked_add(key_len)
        .and_then(|n| n.checked_add(value_len))
    else {
        return; // unreachable: try_spill already bounds this by region_bytes
    };

    let mut active = inner.active.load(Ordering::Acquire);
    let cursor = inner.regions[active as usize]
        .write_cursor
        .load(Ordering::Acquire);
    if !record_fits(cursor, inner.region_bytes, record_len) {
        active = rotate(inner, sink, active);
    }

    let region = &inner.regions[active as usize];
    let offset = region.write_cursor.load(Ordering::Acquire);
    let generation = region.generation.load(Ordering::Acquire);

    let header = build_header(&job, key_len, value_len);
    let mut buf = Vec::with_capacity(record_len as usize);
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&job.key_bytes);
    buf.extend_from_slice(&job.encoded);

    if let Err(err) = pwrite_all(&region.file, &buf, u64::from(offset)) {
        tracing::warn!(
            cache = %inner.cache_name,
            error = %err,
            "sundog spill: region write failed, dropping the write"
        );
        sink.abandon(job.stripe_idx, &job.key_bytes, job.hash, job.ver);
        return;
    }
    region
        .write_cursor
        .store(offset + record_len, Ordering::Release);

    let loc = SpillLoc {
        region: active,
        offset,
        len: record_len,
        generation,
    };
    if sink.install(job.stripe_idx, &job.key_bytes, job.hash, job.ver, loc) {
        region
            .used_bytes
            .fetch_add(u64::from(record_len), Ordering::AcqRel);
        inner
            .bytes_used
            .fetch_add(u64::from(record_len), Ordering::AcqRel);
        region
            .reverse_index
            .lock()
            .push((job.stripe_idx, job.key_bytes));
        inner.record_write();
        inner.publish_bytes_used();
    } else {
        inner.record_dropped("obsolete");
    }
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
    fn record_too_large_rejects_over_region_bytes_and_accepts_exact_fit() {
        assert!(!record_too_large(64, 64));
        assert!(record_too_large(65, 64));
        assert!(!record_too_large(0, 0));
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

    // --- SpillConfig / validate ---

    #[test]
    fn spill_config_defaults_are_64_mib_regions_and_16_way_read_concurrency() {
        let cfg = SpillConfig::new("/tmp/does-not-matter", 1 << 30);
        assert_eq!(cfg.region_bytes_value(), 64 * 1024 * 1024);
        assert_eq!(cfg.read_concurrency_value(), 16);
    }

    #[test]
    fn spill_config_builder_methods_override_the_defaults() {
        let cfg = SpillConfig::new("/tmp/does-not-matter", 1 << 30)
            .region_bytes(4096)
            .read_concurrency(4);
        assert_eq!(cfg.region_bytes_value(), 4096);
        assert_eq!(cfg.read_concurrency_value(), 4);
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
        /// the `(stripe_idx, key_bytes)` pairs it was asked to purge.
        type ReclaimCall = (u32, u32, Vec<(usize, Bytes)>);

        #[derive(Default)]
        struct RecordingSink {
            installs: StdMutex<Vec<(usize, Bytes, Hlc, SpillLoc)>>,
            reclaims: StdMutex<Vec<ReclaimCall>>,
            live: StdMutex<StdHashMap<Bytes, (Hlc, SpillLoc)>>,
        }

        impl RecordingSink {
            fn install_count(&self) -> usize {
                self.installs.lock().unwrap().len()
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

            fn abandon(&self, _stripe_idx: usize, _key_bytes: &Bytes, _hash: u64, _ver: Hlc) {
                // No production caller of this test double ever fails a
                // write, so there is nothing for a real sink to restore
                // here; the engine-level `abandon` behavior itself is
                // covered directly in `store::engine`'s tests.
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
            // region 0 out from under the first job's pointer.
            let dir = temp_dir("stale-gen");
            let record_len = HEADER_LEN as u64 + 1 + 1; // 1-byte key, 1-byte value
            let cfg = SpillConfig::new(&dir, 2 * record_len).region_bytes(record_len);
            let tier = SpillTier::open(&cfg, "cache-a").unwrap();
            let sink = Arc::new(RecordingSink::default());
            tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

            assert!(tier.try_spill(job("a", b"1", hlc(1, 0))));
            assert!(tier.try_spill(job("b", b"2", hlc(2, 0))));
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
            // then holds the first two jobs' keys.
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
            }
            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 5));
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
        fn bytes_used_reflects_confirmed_installs_and_drops_on_reclaim() {
            // Same layout as the rotation test: two records per region.
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
            }
            assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 4));
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
            fs::write(cache_dir.join("keep-me.txt"), b"not a region file").unwrap();

            let cfg = SpillConfig::new(&dir, 256).region_bytes(64);
            let _tier = SpillTier::open(&cfg, "cache-a").unwrap();

            assert!(!cache_dir.join("garbage.reg").exists());
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

        /// A [`SpillSink`] whose `install` always rejects the flush, so
        /// `flush_one` counts `sundog_spill_dropped_total{reason="obsolete"}`
        /// instead of `sundog_spill_writes_total`.
        struct RejectingSink;

        impl SpillSink for RejectingSink {
            fn install(&self, _: usize, _: &Bytes, _: u64, _: Hlc, _: SpillLoc) -> bool {
                false
            }

            fn reclaim(&self, _: u32, _: u32, _: &[(usize, Bytes)]) -> usize {
                0
            }

            fn abandon(&self, _: usize, _: &Bytes, _: u64, _: Hlc) {}
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
        }
    }
}
