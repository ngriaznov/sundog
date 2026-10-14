//! `tombstone_gc_task` ticks on the wall-clock grid of its period, not on a
//! phase counted from when it started.

use super::*;
use crate::hlc::Hlc;
use crate::store::{BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion};
use crate::wire::WireRecord;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::sync::Mutex;

/// A [`ShardOps`] test double recording the wall-clock reading of every
/// `gc_tombstones` call. Every method the gc task does not call panics, since
/// that would be a test bug.
struct GcProbe<C: Fn() -> u64 + Send + Sync> {
    wall_clock: C,
    collected_at: Mutex<Vec<u64>>,
}

impl<C: Fn() -> u64 + Send + Sync> ShardOps for GcProbe<C> {
    fn apply_remote(&self, _rec: WireRecord) -> BoxFuture<'_, ()> {
        unimplemented!("the gc task never calls apply_remote")
    }

    fn apply_remote_batch(&self, _recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        unimplemented!("the gc task never calls apply_remote_batch")
    }

    fn invalidate(&self, _key: Bytes, _ver: Hlc) -> BoxFuture<'_, ()> {
        unimplemented!("the gc task never calls invalidate")
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        unimplemented!("the gc task never calls digests")
    }

    fn bucket_entries(&self, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        unimplemented!("the gc task never calls bucket_entries")
    }

    fn entries_for_buckets(
        &self,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries> {
        unimplemented!("the gc task never calls entries_for_buckets")
    }

    fn bucket_lens(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        unimplemented!("the gc task never calls bucket_lens")
    }

    fn part_digests(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        unimplemented!("the gc task never calls part_digests")
    }

    fn entries_for_parts(
        &self,
        _parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        unimplemented!("the gc task never calls entries_for_parts")
    }

    fn records_for(&self, _keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        unimplemented!("the gc task never calls records_for")
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        unimplemented!("the gc task never calls snapshot_chunks")
    }

    fn gc_tombstones(&self, _any_member_absent: bool) -> BoxFuture<'_, ()> {
        let now = (self.wall_clock)();
        self.collected_at
            .lock()
            .expect("invariant: fixture mutex is never poisoned")
            .push(now);
        Box::pin(async {})
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// The task collects once on start, then at each multiple of a quarter of
/// `tombstone_ttl` on the wall clock it reads, starting from an arbitrary
/// offset into a period. A tokio interval, which counts its period from when
/// the task started, would collect one period after the start instead.
#[tokio::test(start_paused = true)]
async fn the_gc_task_collects_on_multiples_of_its_period_on_the_wall_clock_it_is_given() {
    const PERIOD_MS: u64 = 150_000;
    const START_MS: u64 = 1_700_000_000_123;
    assert_ne!(START_MS % PERIOD_MS, 0, "the start is off the grid");
    // A wall clock that reads START_MS now and moves with the paused tokio
    // clock, which advances in whole milliseconds.
    let origin = tokio::time::Instant::now();
    let wall_clock = move || {
        let elapsed_ms = (origin.elapsed().as_micros() + 500) / 1_000;
        START_MS + u64::try_from(elapsed_ms).expect("a test runs for far less than 2^64 ms")
    };
    let probe = Arc::new(GcProbe {
        wall_clock,
        collected_at: Mutex::new(Vec::new()),
    });
    let cancel = CancellationToken::new();
    let task = tokio::spawn(tombstone_gc_task(
        Arc::clone(&probe) as Arc<dyn ShardOps>,
        Mode::Replicated,
        Duration::from_millis(4 * PERIOD_MS),
        Duration::from_hours(24),
        absence::AbsenceTracker::default(),
        wall_clock,
        cancel.clone(),
    ));

    tokio::time::sleep(Duration::from_millis(3 * PERIOD_MS)).await;
    cancel.cancel();
    task.await.expect("the gc task ends once cancelled");

    let first_tick = START_MS / PERIOD_MS * PERIOD_MS + PERIOD_MS;
    assert_eq!(
        *probe
            .collected_at
            .lock()
            .expect("invariant: fixture mutex is never poisoned"),
        [
            START_MS,
            first_tick,
            first_tick + PERIOD_MS,
            first_tick + 2 * PERIOD_MS
        ],
        "one collection on start, then one at each following multiple of the period"
    );
}
