//! Smoke validation for the rightsize container harness: one
//! `sundog-testnode` container boots and answers `count` over its control
//! port. Gated on `SUNDOG_CONTAINER_TESTS=1`.
//!
//! Run `SUNDOG_CONTAINER_TESTS=1 SUNDOG_TEST_BASE_IMAGE=rz-base:local
//! RIGHTSIZE_BACKEND=docker cargo test --release -p sundog --test
//! container_smoke -- --nocapture`.

mod container_util;

use std::sync::Arc;
use std::time::Duration;

use rightsize::Network;

#[tokio::test]
async fn single_node_boots_and_answers_count_over_the_control_port() {
    if !container_util::container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());

    let node = container_util::Node::spawn(&net, "smoke-cluster", "n1", &[]).await;

    let count = node.count().await.expect("count replies with a number");
    assert_eq!(count, 0, "a freshly booted node holds no entries yet");

    let peers = node.peers().await.expect("peers replies with a number");
    assert_eq!(peers, 0, "a lone node has no live peers");

    node.put("hello", "world")
        .await
        .expect("put on the node's own control connection succeeds");
    container_util::eventually(Duration::from_secs(5), || async {
        node.count().await == Ok(1)
    })
    .await;
    assert_eq!(node.get("hello").await, Ok(Some("world".to_string())));

    node.stop().await.expect("container stops cleanly");
    net.close().await.expect("network closes cleanly");
}
