//! Shared harness for `tests/tls.rs` and `tests/prometheus_exporter.rs`: a
//! fast-cycling [`ClusterConfig`], a bounded-wait polling helper, and the
//! small [`Node`] bookkeeping struct both files build their own real,
//! loopback-`Static`-discovery [`Cluster`]s around.
#![allow(dead_code)]

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use sundog::{Cluster, ClusterConfig};

/// A fast-cycling [`ClusterConfig`] for tests: loopback-only bind addresses
/// and a tight anti-entropy/tombstone cadence, so convergence is fast.
#[must_use]
pub fn fast_config() -> ClusterConfig {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = loopback;
        c.data_bind_addr = loopback;
        c.ae_interval = Duration::from_millis(150);
        c.tombstone_ttl = Duration::from_secs(2);
    })
}

/// Polls `cond` on a short fixed cadence until it returns `true`, or panics
/// once `timeout` elapses. `sundog`'s public API exposes only point-in-time
/// snapshots, never a change stream, so bounded polling is the only
/// race-free way to assert convergence from here.
/// # Panics
///
/// Panics if `cond` has not returned `true` by `timeout`.
pub async fn eventually<F, Fut>(timeout: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A running node plus the loopback gossip address it was built with,
/// tracked here since the public API has no accessor for it.
pub struct Node {
    pub cluster: Cluster,
    pub gossip_addr: SocketAddr,
}

/// Waits until `cluster` reports at least `expected` live peers.
/// # Panics
///
/// Panics if the bound is not reached within `timeout`.
pub async fn wait_for_peer_count(cluster: &Cluster, expected: usize, timeout: Duration) {
    eventually(timeout, || async { cluster.peers().len() >= expected }).await;
}

/// Shuts every node in `nodes` down gracefully, sequentially, keeping
/// failures attributable to a specific node.
pub async fn shutdown_all(nodes: Vec<Node>) {
    for node in nodes {
        node.cluster.shutdown().await;
    }
}
