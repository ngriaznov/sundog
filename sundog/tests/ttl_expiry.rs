//! Plan §11.3, layer 3, item 7: TTL travels as an absolute expiry (plan §7).
//! An entry inserted on A with a 1s TTL expires on B too — even though B's
//! cache is opened with no TTL of its own, proving the deadline that expires
//! it is the one A stamped at write time, not something B derives locally.
//!
//! Real-transport-only: see `tests/anti_entropy_repair.rs`'s module doc for
//! why this excludes `sim`.

#![cfg(not(feature = "sim"))]

mod common;

use std::time::Duration;

use sundog::Mode;

#[tokio::test]
async fn absolute_ttl_set_at_the_origin_expires_the_entry_on_every_node() {
    let nodes = common::spawn_cluster_group("it-ttl", 2).await;
    let a = nodes[0].cluster.clone();
    let b = nodes[1].cluster.clone();

    let cache_a = a
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .ttl(Duration::from_secs(1))
        .open()
        .await
        .expect("a opens");
    // No `.ttl(..)` here: B's copy must still expire, because the deadline
    // travels on the wire as an absolute `expires_at_ms` (plan §7), not from
    // B's own (absent) local TTL configuration.
    let cache_b = b
        .cache::<u32, String>("sessions")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    cache_a
        .insert(1, "short-lived".into())
        .await
        .expect("a inserts with a 1s ttl");
    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.is_some()
    })
    .await;

    common::eventually(Duration::from_secs(5), || async {
        cache_a.get(&1).await.is_none()
    })
    .await;
    common::eventually(Duration::from_secs(5), || async {
        cache_b.get(&1).await.is_none()
    })
    .await;

    common::shutdown_all(nodes).await;
}
