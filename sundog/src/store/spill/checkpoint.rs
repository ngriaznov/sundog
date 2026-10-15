//! Checkpoint writer for warm reopen: drains the flusher, writes resident records into the ring and persists the snapshot.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;

use bytes::Bytes;
use zerocopy::IntoBytes;

use crate::hlc::Hlc;

#[cfg(doc)]
use super::flusher::flush_batch;
use super::flusher::rotate;
use super::record::{pwrite_all, record_checksum};
use super::rules::{next_region_index, record_too_large, records_fitting_region, spill_record_len};
use super::snapshot::{build_snapshot_bytes, write_snapshot_atomic};
use super::{
    CheckpointFlushOutcome, HEADER_LEN, Inner, SPILL_MAGIC, SpillLoc, SpillRecordHeader, SpillSink,
    SpillTier,
};

impl SpillTier {
    /// Test-only: how many times [`SpillTier::checkpoint_flush`] has run.
    #[cfg(test)]
    pub(crate) fn checkpoint_flushes(&self) -> u64 {
        self.inner.checkpoint_flushes.load(Ordering::Relaxed)
    }

    /// Test-only: how many times [`SpillTier::read_at`] has been called.
    #[cfg(test)]
    pub(crate) fn read_calls(&self) -> u64 {
        self.inner.read_calls.load(Ordering::Relaxed)
    }

    /// The checkpoint half of a warm-reopen-enabled close: blocks until the
    /// flusher thread [`SpillTier::attach`] started drains its queue and
    /// exits (the only place the spill module joins that thread; run this in a
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
        #[cfg(test)]
        self.inner
            .checkpoint_flushes
            .fetch_add(1, Ordering::Relaxed);
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
}

/// Writes `entries`, the checkpoint's currently-resident live records,
/// into the region ring using `write_segment`'s framing, batched by
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
