//! The pause guard through an opened cache: the gc task's collections reach
//! the shard's engine, which measures the gap between them. The behaviour of
//! the guard is pinned at the shard (`store/tests/pause_guard.rs`) and at the
//! task (`cluster/tests/gc_task.rs`), on clocks the tests move; this test
//! asserts the wiring between the two, and no timing.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

#[tokio::test]
async fn a_replicated_caches_gc_task_feeds_the_engines_pause_guard() {
    let clock = Arc::new(AtomicU64::new(now_ms() + 3_600_000));
    let cluster = Cluster::builder("cache-it-pause-guard")
        .config(ClusterConfig {
            // Long enough that the task's grid never ticks again while the
            // test runs, and that nothing here could trip the guard.
            tombstone_ttl: Duration::from_hours(1),
            ..loopback_config()
        })
        .seeds(std::iter::empty())
        .build()
        .await
        .expect("node builds");
    let reader = Arc::clone(&clock);
    let cache = cluster
        .cache::<u32, String>("cache-it-pause-guard")
        .mode(Mode::Replicated)
        .clock(Arc::new(move || reader.load(Ordering::SeqCst)))
        .open()
        .await
        .expect("opens");

    let first_collection = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let (last_gc_ms, hold_until_ms) = cache.shard.pause_state();
            if last_gc_ms != 0 {
                return (last_gc_ms, hold_until_ms);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the task collects once on start");
    assert_eq!(
        first_collection,
        (clock.load(Ordering::SeqCst), 0),
        "read from the cache's own clock; a first collection is no pause"
    );
    cluster.shutdown().await;
}
