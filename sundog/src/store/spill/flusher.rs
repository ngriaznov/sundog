//! The spill flusher thread: batching queued jobs into positional writes, rotating regions and installing records.

use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Weak};
use std::thread;
#[cfg(test)]
use std::time::Duration;

use zerocopy::IntoBytes;

use super::record::{build_header, pwrite_all};
#[cfg(doc)]
use super::rules::FLUSH_QUEUE_CAPACITY;
use super::rules::{flush_queue_slots, next_region_index, records_fitting_region};
use super::{HEADER_LEN, Inner, ReclaimRef, SpillJob, SpillLoc, SpillSink, SpillTier};

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

impl SpillTier {
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
}

pub(super) fn flusher_loop(
    inner: &Arc<Inner>,
    rx: &Receiver<SpillJob>,
    sink: &Weak<dyn SpillSink>,
) {
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
pub(super) fn flush_batch(inner: &Inner, sink: &dyn SpillSink, jobs: Vec<SpillJob>) {
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
pub(super) fn rotate(inner: &Inner, sink: &dyn SpillSink, current: u32) -> u32 {
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
