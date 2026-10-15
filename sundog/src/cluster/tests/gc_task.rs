//! `tombstone_gc_task` ticks on the wall-clock grid of its period, not on a
//! phase counted from when it started, and defers collection for the first
//! `tombstone_ttl` after it starts.

use super::*;
use crate::hlc::Hlc;
use crate::store::{BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion};
use crate::wire::WireRecord;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::sync::Mutex;

/// A [`ShardOps`] test double recording the wall-clock reading and the
/// `defer` argument of every `gc_tombstones` call. Every method the gc task
/// does not call panics, since that would be a test bug.
struct GcProbe<C: Fn() -> u64 + Send + Sync> {
    wall_clock: C,
    collected_at: Mutex<Vec<u64>>,
    deferred: Mutex<Vec<bool>>,
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

    fn gc_tombstones(&self, defer: bool) -> BoxFuture<'_, ()> {
        let now = (self.wall_clock)();
        self.collected_at
            .lock()
            .expect("invariant: fixture mutex is never poisoned")
            .push(now);
        self.deferred
            .lock()
            .expect("invariant: fixture mutex is never poisoned")
            .push(defer);
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
        deferred: Mutex::new(Vec::new()),
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

/// A wall clock that reads `start_ms` now and moves with the paused tokio
/// clock, which advances in whole milliseconds.
fn paused_wall_clock(start_ms: u64) -> impl Fn() -> u64 + Copy + Send + Sync {
    let origin = tokio::time::Instant::now();
    move || {
        let elapsed_ms = (origin.elapsed().as_micros() + 500) / 1_000;
        start_ms + u64::try_from(elapsed_ms).expect("a test runs for far less than 2^64 ms")
    }
}

/// Runs the gc task in `mode` over a probe with no member absent for
/// `run_for`, from a wall clock reading `start_ms`, and returns what the
/// probe saw: each collection's wall-clock reading and `defer` argument.
async fn gc_calls(
    mode: Mode,
    tombstone_ttl: Duration,
    start_ms: u64,
    run_for: Duration,
) -> Vec<(u64, bool)> {
    let wall_clock = paused_wall_clock(start_ms);
    let probe = Arc::new(GcProbe {
        wall_clock,
        collected_at: Mutex::new(Vec::new()),
        deferred: Mutex::new(Vec::new()),
    });
    let cancel = CancellationToken::new();
    let task = tokio::spawn(tombstone_gc_task(
        Arc::clone(&probe) as Arc<dyn ShardOps>,
        mode,
        tombstone_ttl,
        Duration::from_hours(24),
        absence::AbsenceTracker::default(),
        wall_clock,
        cancel.clone(),
    ));
    tokio::time::sleep(run_for).await;
    cancel.cancel();
    task.await.expect("the gc task ends once cancelled");

    let at = probe
        .collected_at
        .lock()
        .expect("invariant: fixture mutex is never poisoned")
        .clone();
    let deferred = probe
        .deferred
        .lock()
        .expect("invariant: fixture mutex is never poisoned")
        .clone();
    at.into_iter().zip(deferred).collect()
}

/// With no member absent, a `Replicated` or `Distributed` task still defers
/// every collection that runs less than `tombstone_ttl` after it started and
/// none after, whatever the grid places them at.
#[tokio::test(start_paused = true)]
async fn the_gc_task_defers_for_the_first_tombstone_ttl_after_it_starts_and_not_after() {
    const START_MS: u64 = 1_700_000_000_500;
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    for mode in [Mode::Replicated, Mode::Distributed { owners }] {
        // A period of 2 s (a quarter of the ttl), on a grid 1.5 s ahead.
        assert_eq!(START_MS % 2_000, 500);
        let calls = gc_calls(
            mode,
            Duration::from_secs(8),
            START_MS,
            Duration::from_secs(12),
        )
        .await;
        assert_eq!(
            calls,
            [
                (START_MS, true),
                (START_MS + 1_500, true),
                (START_MS + 3_500, true),
                (START_MS + 5_500, true),
                (START_MS + 7_500, true),
                (START_MS + 9_500, false),
                (START_MS + 11_500, false),
            ],
            "{mode:?}: deferred while the task has run for less than 8 s"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_gc_task_of_a_local_or_invalidation_cache_never_defers() {
    const START_MS: u64 = 1_700_000_000_500;
    for mode in [Mode::Local, Mode::Invalidation] {
        let calls = gc_calls(
            mode,
            Duration::from_secs(8),
            START_MS,
            Duration::from_secs(12),
        )
        .await;
        assert_eq!(calls.len(), 7, "{mode:?}");
        assert!(
            calls.iter().all(|&(_, defer)| !defer),
            "{mode:?}: nothing to protect from a returning peer: {calls:?}"
        );
    }
}

#[test]
fn gc_defers_follows_the_mode_the_absent_members_and_the_uptime() {
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let settle = Duration::from_secs(10);
    for mode in [Mode::Replicated, Mode::Distributed { owners }] {
        for (absent_defers, uptime, expected) in [
            (false, Duration::ZERO, true),
            (false, Duration::from_millis(9_999), true),
            (false, settle, false),
            (false, Duration::from_hours(1), false),
            (true, Duration::ZERO, true),
            (true, settle, true),
            (true, Duration::from_hours(1), true),
        ] {
            assert_eq!(
                gc_defers(mode, absent_defers, uptime, settle),
                expected,
                "{mode:?} absent_defers {absent_defers} uptime {uptime:?}"
            );
        }
    }
    for mode in [Mode::Local, Mode::Invalidation] {
        for (absent_defers, uptime) in [
            (false, Duration::ZERO),
            (true, Duration::ZERO),
            (false, settle),
        ] {
            assert!(
                !gc_defers(mode, absent_defers, uptime, settle),
                "{mode:?} never defers"
            );
        }
    }
}
