//! Plan §11.3, layer 3, item 4: state transfer on join (plan §9). Node A
//! accumulates ~2000 entries before B ever exists; B joins and opens the same
//! `Mode::Replicated` cache, and `open()` — which blocks on state transfer —
//! must return with the full entry count already present, with no writes of
//! any kind occurring after B joins.
//!
//! Real-transport-only: see `tests/anti_entropy_repair.rs`'s module doc for
//! why this excludes `sim`.

#![cfg(not(feature = "sim"))]

mod common;

use std::time::Duration;

use sundog::{Cache, Mode};

const ENTRY_COUNT: u32 = 2_000;

#[tokio::test]
async fn state_transfer_warms_a_late_joiner_with_thousands_of_entries() {
    let nodes = common::spawn_cluster_group("it-state-transfer-warm", 1).await;
    let a = nodes[0].cluster.clone();
    let donor_seed = nodes[0].gossip_addr;

    let cache_a = a
        .cache::<u32, String>("catalog")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    for i in 0..ENTRY_COUNT {
        cache_a
            .insert(i, format!("item-{i}"))
            .await
            .expect("a preloads an entry");
    }

    let node_b = common::join_node("it-state-transfer-warm", [donor_seed]).await;
    common::wait_for_peer_count(&node_b.cluster, 1, Duration::from_secs(20)).await;

    let cache_b = tokio::time::timeout(
        Duration::from_secs(30),
        node_b
            .cluster
            .cache::<u32, String>("catalog")
            .mode(Mode::Replicated)
            .open(),
    )
    .await
    .expect("open completes within the state-transfer budget")
    .expect("b opens");

    assert_eq!(
        entries_present(&cache_b, ENTRY_COUNT).await,
        ENTRY_COUNT as usize,
        "state transfer must warm every entry before open() returns, with no writes since B joined"
    );

    a.shutdown().await;
    node_b.cluster.shutdown().await;
}

async fn entries_present(cache: &Cache<u32, String>, n: u32) -> usize {
    let mut present = 0usize;
    for i in 0..n {
        if cache.get(&i).await.is_some() {
            present += 1;
        }
    }
    present
}
