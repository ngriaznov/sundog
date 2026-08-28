//! Plan §11.3, layer 3, item 2: `Mode::Invalidation`. Two nodes independently
//! cache the same key via `get_or_load`; a newer write on one invalidates the
//! other's stale copy. Part 2 exercises the version guard (plan §4) under
//! genuine concurrency — see its own comment for why the guard is proven via
//! a convergence invariant rather than a specific predetermined winner.
//!
//! Real-transport-only: see `tests/anti_entropy_repair.rs`'s module doc for
//! why this excludes `sim`.

#![cfg(not(feature = "sim"))]

mod common;

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use sundog::Mode;

#[tokio::test]
async fn invalidation_mode_invalidates_stale_copies_and_never_a_newer_local_write() {
    let nodes = common::spawn_cluster_group("it-invalidation-guard", 2).await;
    let a = nodes[0].cluster.clone();
    let b = nodes[1].cluster.clone();

    let cache_a = a
        .cache::<u32, String>("profiles")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("a opens");
    let cache_b = b
        .cache::<u32, String>("profiles")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("b opens");

    // Part 1: both nodes independently warm key 1 via `get_or_load`
    // (invalidation mode never replicates values, so both loaders genuinely
    // run); A's later write then invalidates B's now-stale independent copy.
    let calls_a = Arc::new(AtomicUsize::new(0));
    let calls_b = Arc::new(AtomicUsize::new(0));
    let (ca, cb) = (Arc::clone(&calls_a), Arc::clone(&calls_b));
    let loaded_a = cache_a
        .get_or_load(&1, async move |_key: &u32| -> Result<String, Infallible> {
            ca.fetch_add(1, Relaxed);
            Ok("v0".to_string())
        })
        .await
        .expect("a loads");
    let loaded_b = cache_b
        .get_or_load(&1, async move |_key: &u32| -> Result<String, Infallible> {
            cb.fetch_add(1, Relaxed);
            Ok("v0".to_string())
        })
        .await
        .expect("b loads");
    assert_eq!(loaded_a, "v0");
    assert_eq!(loaded_b, "v0");
    assert_eq!(
        calls_a.load(Relaxed),
        1,
        "A's loader must run for A's own miss"
    );
    assert_eq!(
        calls_b.load(Relaxed),
        1,
        "invalidation mode never replicates values, so B's loader must run independently"
    );

    tokio::time::sleep(Duration::from_millis(5)).await;
    cache_a
        .insert(1, "a-fresh".into())
        .await
        .expect("a writes a newer version");
    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.is_none()
    })
    .await;

    // Part 2: the version guard (`ver <= stored` in `Shard::invalidate`)
    // under genuine concurrency. A and B race a write to the *same* key,
    // fired via `tokio::join!` for true simultaneity rather than sequential
    // `.await`s. There is no way, via the public API, to construct a
    // deterministic "an invalidate strictly older than an already-live
    // entry arrives late" race: the sender's own clock guarantees any
    // message it sends is newer than anything it has already observed, so
    // manufacturing genuine staleness needs real concurrent writers — and
    // two concurrent HLC stamps that land in the same wall-clock
    // millisecond are, correctly, tie-broken by node id (`Hlc`'s doc
    // comment), which is unpredictable by design. So this asserts the
    // guard's real, deterministic consequence instead of a winner: exactly
    // one node ends up holding a live copy — never a split-brain of two
    // different non-empty values, and never both empty. A broken guard
    // (unconditional delete, or never deleting) would violate this and get
    // caught. See this suite's reported needs.
    let (write_a, write_b) = tokio::join!(
        cache_a.insert(2, "from-a".to_string()),
        cache_b.insert(2, "from-b".to_string()),
    );
    write_a.expect("a writes key 2");
    write_b.expect("b writes key 2");

    common::eventually(Duration::from_secs(10), || async {
        cache_a.get(&2).await.is_some() != cache_b.get(&2).await.is_some()
    })
    .await;

    let (on_a, on_b) = (cache_a.get(&2).await, cache_b.get(&2).await);
    match (on_a, on_b) {
        (Some(value), None) => assert_eq!(value, "from-a"),
        (None, Some(value)) => assert_eq!(value, "from-b"),
        other => panic!("split-brain after concurrent writes: {other:?}"),
    }

    common::shutdown_all(nodes).await;
}
