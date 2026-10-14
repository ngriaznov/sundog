//! Opening a spill tier: the cold start, the warm reopen from a checkpoint snapshot and the snapshot replay helpers.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};

use parking_lot::Mutex;
use tokio::sync::Semaphore;

use crate::hlc::Hlc;

use super::record::{
    decode_record_with_key, pread_exact, region_file_name, remove_stale_region_files,
};
use super::rules::region_count_for;
#[cfg(doc)]
use super::snapshot::SNAPSHOT_FORMAT_VERSION;
use super::snapshot::{delete_snapshot, remove_stale_snapshot_files, snapshot_eligibility};
use super::{
    Inner, ReclaimRef, RegionState, SnapshotEntry, SpillConfig, SpillLoc, SpillSink, SpillTier,
};

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
    /// test force `write_segment`'s write to fail and prove every job in
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
        // conversion total rather than panicking on a config the spill module did
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
    /// [`ReopenOutcome::max_ver`] reports the greatest version the snapshot
    /// recorded, which the caller folds into its clock.
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

        let max_ver = max_snapshot_version(&snapshot_entries);
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
            max_ver,
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
            max_ver: None,
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
    /// The greatest version the snapshot recorded across every validated
    /// entry, including ones the replay then dropped as expired or unowned:
    /// those stamps are this node's own history. `None` for a cold
    /// fallback or an empty snapshot. The caller folds it into its clock so
    /// a later local stamp exceeds every replayed version.
    pub(crate) max_ver: Option<Hlc>,
}

/// The greatest version among `entries`, by wall time, then logical
/// counter, then node: what a clock must pass for every later stamp to
/// outrank every replayed entry. `None` for no entries.
pub(super) fn max_snapshot_version(entries: &[SnapshotEntry]) -> Option<Hlc> {
    entries.iter().map(|entry| entry.ver).max()
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
pub(super) fn snapshot_loc_end(loc: SpillLoc, region_bytes: u32, region_count: u32) -> Option<u32> {
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
