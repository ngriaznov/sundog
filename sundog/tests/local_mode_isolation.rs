//! Plan §11.3, layer 3, item 8: `Mode::Local` never produces cross-node
//! traffic — a write on A must never be observable on B.
//!
//! Real-transport-only: see `tests/anti_entropy_repair.rs`'s module doc for
//! why this excludes `sim`.

#![cfg(not(feature = "sim"))]

mod common;

use std::time::Duration;

use sundog::Mode;

#[tokio::test]
async fn local_mode_never_leaks_writes_across_nodes() {
    let nodes = common::spawn_cluster_group("it-local-mode", 2).await;
    let a = nodes[0].cluster.clone();
    let b = nodes[1].cluster.clone();

    let cache_a = a
        .cache::<u32, String>("scratch")
        .mode(Mode::Local)
        .open()
        .await
        .expect("a opens");
    let cache_b = b
        .cache::<u32, String>("scratch")
        .mode(Mode::Local)
        .open()
        .await
        .expect("b opens");

    cache_a
        .insert(1, "only-on-a".into())
        .await
        .expect("a inserts");

    // `Mode::Local` publishes no wire message at all (`store::mod`'s fan-out
    // match has no arm for it), so there is no "delivered" event to await
    // and no watch stream to race against — the only public-API-observable
    // proof is polling past the settle window a real cross-node message
    // would need, then asserting it never showed.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        cache_b.get(&1).await,
        None,
        "Mode::Local must never fan a write out to peers"
    );
    assert_eq!(cache_a.get(&1).await, Some("only-on-a".to_string()));

    common::shutdown_all(nodes).await;
}
