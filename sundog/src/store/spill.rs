//! Configuration for the optional local SSD/NVMe spill tier.
//!
//! This module is a stub: it carries only [`crate::store::spill::SpillConfig`]
//! so that [`crate::cache::CacheBuilder::spill`] and
//! [`crate::error::CacheError`]'s spill variants have a concrete type to
//! name. The region log, flusher, and read path land separately.

/// Default region size: 64 MiB.
const DEFAULT_REGION_BYTES: u64 = 64 * 1024 * 1024;

/// Default number of concurrent spilled-value reads.
const DEFAULT_READ_CONCURRENCY: usize = 16;

/// Configures the local SSD/NVMe spill tier: [`crate::cache::CacheBuilder::spill`]
/// takes one of these. `dir` and `capacity_bytes` have no default — a disk
/// budget must be explicit — while `region_bytes` (default 64 MiB) and
/// `read_concurrency` (default 16) are tuned via their own builder methods.
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// The per-cache directory the tier's region files live under.
    pub dir: std::path::PathBuf,
    /// The disk budget: how many bytes of region files the tier maintains.
    pub capacity_bytes: u64,
    region_bytes: u64,
    read_concurrency: usize,
}

impl SpillConfig {
    /// Starts a config for a tier rooted at `dir`, budgeted at
    /// `capacity_bytes`, with the default `region_bytes` and
    /// `read_concurrency`.
    #[must_use]
    pub fn new(dir: impl Into<std::path::PathBuf>, capacity_bytes: u64) -> Self {
        Self {
            dir: dir.into(),
            capacity_bytes,
            region_bytes: DEFAULT_REGION_BYTES,
            read_concurrency: DEFAULT_READ_CONCURRENCY,
        }
    }

    /// Overrides the region file size. Default: 64 MiB.
    #[must_use]
    pub fn region_bytes(mut self, bytes: u64) -> Self {
        self.region_bytes = bytes;
        self
    }

    /// Overrides the maximum number of concurrent spilled-value reads.
    /// Default: 16.
    #[must_use]
    pub fn read_concurrency(mut self, n: usize) -> Self {
        self.read_concurrency = n;
        self
    }

    /// Checks the config's numeric invariants ahead of opening the tier:
    /// `region_bytes` must be nonzero, and `capacity_bytes` must be at least
    /// two regions, so the FIFO ring always has a distinct writer region and
    /// a distinct next-to-reclaim region. Returns the rejection reason on
    /// failure.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.region_bytes == 0 {
            return Err("region_bytes must be greater than zero");
        }
        if self.capacity_bytes < 2 * self.region_bytes {
            return Err("capacity_bytes must be at least twice region_bytes");
        }
        Ok(())
    }
}
