//! Plan §11.3, layer 3, item 3: concurrent `get_or_load` misses on the same
//! cold key collapse into exactly one loader call (moka's stampede
//! protection, plan §7/§10).
//!
//! Real-transport-only: see `tests/anti_entropy_repair.rs`'s module doc for
//! why this excludes `sim`.

#![cfg(not(feature = "sim"))]

mod common;

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use sundog::Mode;

const CONCURRENCY: usize = 64;

#[tokio::test]
async fn get_or_load_stampede_collapses_to_one_loader_call() {
    let nodes = common::spawn_cluster_group("it-stampede", 1).await;
    let cluster = nodes[0].cluster.clone();

    let cache = cluster
        .cache::<u32, String>("sessions")
        .mode(Mode::Local)
        .open()
        .await
        .expect("open succeeds");

    let calls = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        let cache = cache.clone();
        let calls = Arc::clone(&calls);
        tasks.spawn(async move {
            cache
                .get_or_load(&42, async move |_key: &u32| -> Result<String, Infallible> {
                    calls.fetch_add(1, SeqCst);
                    // Widens the race window: every concurrent caller must
                    // still be waiting on this single in-flight load.
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok("loaded-once".to_string())
                })
                .await
                .expect("loader succeeds")
        });
    }

    let mut results = Vec::with_capacity(CONCURRENCY);
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("stampede task does not panic"));
    }

    assert_eq!(
        calls.load(SeqCst),
        1,
        "loader must run exactly once under a stampede of {CONCURRENCY} concurrent misses"
    );
    assert!(results.iter().all(|value| value == "loaded-once"));
    assert_eq!(results.len(), CONCURRENCY);

    common::shutdown_all(nodes).await;
}
