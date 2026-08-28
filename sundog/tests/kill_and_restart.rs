//! Plan §11.3, layer 3, item 5: kill one of three nodes, confirm the
//! remaining two keep replicating, then restart a fresh node in its place
//! and confirm membership re-converges and it warms up via state transfer.
//!
//! `sundog`'s public API has no way to simulate an abrupt process crash from
//! outside the crate — only a graceful [`sundog::Cluster::shutdown`] — so
//! "kill" here means that; see this suite's reported needs.
//!
//! Real-transport-only: see `tests/anti_entropy_repair.rs`'s module doc for
//! why this excludes `sim`.

#![cfg(not(feature = "sim"))]

mod common;

use std::time::Duration;

use sundog::Mode;

#[tokio::test]
async fn kill_one_node_and_restart_a_fresh_one_in_its_place() {
    let nodes = common::spawn_cluster_group("it-kill-restart", 3).await;
    let a = nodes[0].cluster.clone();
    let b = nodes[1].cluster.clone();
    let c = nodes[2].cluster.clone();
    let seed_a = nodes[0].gossip_addr;
    let seed_b = nodes[1].gossip_addr;

    let cache_a = a
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = b
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");
    let cache_c = c
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("c opens");

    cache_a
        .insert(1, "before-kill".into())
        .await
        .expect("a writes before c dies");
    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.is_some() && cache_c.get(&1).await.is_some()
    })
    .await;

    c.shutdown().await;
    common::eventually(Duration::from_secs(20), || async { a.peers().len() == 1 }).await;
    common::eventually(Duration::from_secs(20), || async { b.peers().len() == 1 }).await;

    // The remaining pair keeps replicating with C gone.
    cache_a
        .insert(2, "after-kill".into())
        .await
        .expect("a writes after c dies");
    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&2).await.is_some()
    })
    .await;

    // Restart a fresh node (a new process incarnation, new `NodeId`) in C's
    // place, seeded from the two survivors.
    let node_c2 = common::join_node("it-kill-restart", [seed_a, seed_b]).await;
    common::wait_for_peer_count(&node_c2.cluster, 2, Duration::from_secs(20)).await;
    common::eventually(Duration::from_secs(20), || async { a.peers().len() == 2 }).await;
    common::eventually(Duration::from_secs(20), || async { b.peers().len() == 2 }).await;

    let cache_c2 = tokio::time::timeout(
        Duration::from_secs(30),
        node_c2
            .cluster
            .cache::<u32, String>("sessions")
            .mode(Mode::Replicated)
            .open(),
    )
    .await
    .expect("open completes within the state-transfer budget")
    .expect("the fresh node opens");

    assert_eq!(cache_c2.get(&1).await, Some("before-kill".to_string()));
    assert_eq!(
        cache_c2.get(&2).await,
        Some("after-kill".to_string()),
        "the fresh node must warm up with everything written while it was gone"
    );

    a.shutdown().await;
    b.shutdown().await;
    node_c2.cluster.shutdown().await;
}
