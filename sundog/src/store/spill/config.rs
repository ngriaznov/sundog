//! `SpillConfig`: the disk budget and layout knobs for a cache's optional spill tier.

use std::path::PathBuf;
use std::time::Duration;

use super::HEADER_LEN;
#[cfg(doc)]
use super::SpillTier;

/// Region size a [`SpillConfig`] uses when [`SpillConfig::region_bytes`] is
/// never called.
const DEFAULT_REGION_BYTES: u64 = 64 * 1024 * 1024;
/// Concurrent-disk-read bound a [`SpillConfig`] uses when
/// [`SpillConfig::read_concurrency`] is never called. Consulted by the
/// engine's read path, `spawn_blocking` behind a semaphore of this size,
/// not by anything in the spill module.
const DEFAULT_READ_CONCURRENCY: usize = 16;
/// Wait bound used when [`SpillConfig::spill_wait_timeout`] is never
/// called: 2s, leaving margin over the ~8s
/// `state_transfer::per_donor_budget`.
const DEFAULT_SPILL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
/// Sanity cap [`SpillConfig::validate`] enforces on
/// [`SpillConfig::spill_wait_timeout`], so an absurd value errors at open
/// time rather than as a later rebalance failure.
const MAX_SPILL_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

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
