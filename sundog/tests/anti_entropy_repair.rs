//! Plan §11.3, layer 3, item 6: anti-entropy repairs a divergence that live
//! fan-out traffic can no longer reach.
//!
//! `sundog`'s public API exposes no way to sever or drop messages on an
//! already-open data-plane connection from outside the crate — there is no
//! partition-injection hook alongside `Cluster`/`Cache` — so true network
//! divergence (write to A while B is disconnected, then heal) isn't
//! reachable through the public surface; see this suite's reported needs.
//! This is the closest honest equivalent: [`sundog::Cache::invalidate_local`]
//! is itself a *documented, public* escape hatch ("for tests and manual
//! cache-busting") that drops a key locally without a tombstone — exactly
//! what a lost `Replicate` message leaves behind. With no further write ever
//! touching that key, only a periodic anti-entropy round (never live
//! fan-out) can bring it back.
//!
//! Real-transport-only: under `sim` the crate's TCP seam is `turmoil::net`
//! (`src/net/tcp.rs`), which only works driven inside a `turmoil::Sim` —
//! see `tests/sim.rs` for that lane instead.

#![cfg(not(feature = "sim"))]

mod common;

use std::time::Duration;

use sundog::Mode;

#[tokio::test]
async fn anti_entropy_repairs_an_entry_live_fan_out_can_no_longer_reach() {
    let nodes = common::spawn_cluster_group("it-anti-entropy", 2).await;
    let a = nodes[0].cluster.clone();
    let b = nodes[1].cluster.clone();

    let cache_a = a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = b
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");

    cache_a.insert(1, "hello".into()).await.expect("a inserts");
    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.is_some()
    })
    .await;

    cache_b.invalidate_local(&1).await;
    assert_eq!(cache_b.get(&1).await, None);

    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.is_some()
    })
    .await;
    assert_eq!(
        cache_b.get(&1).await,
        Some("hello".to_string()),
        "anti-entropy must repair the dropped entry within a few fast rounds"
    );

    common::shutdown_all(nodes).await;
}
