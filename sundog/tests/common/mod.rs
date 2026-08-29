//! Shared harness for sundog's remaining in-process tests (`tests/tls.rs`,
//! `tests/prometheus_exporter.rs`): a fast-cycling [`ClusterConfig`], a
//! bounded-wait polling helper so no test ever asserts convergence after a
//! fixed `sleep`, and the small [`Node`] bookkeeping struct both files build
//! their own real, loopback-`Static`-discovery [`Cluster`]s around.
//!
//! The former general-purpose multi-node group builders (`spawn_cluster_group`,
//! `join_node`, and their `_with` variants) moved to `tests/containers.rs`'s
//! rightsize-based harness (`tests/container_util`) along with the tests that
//! used them.
//!
//! Each `tests/*.rs` file that uses this module pulls in only the helpers it
//! needs — unused ones in any given binary are expected, hence the blanket
//! `dead_code` allow.
#![allow(dead_code)]

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use sundog::{Cluster, ClusterConfig};

/// A fast-cycling [`ClusterConfig`] for tests: loopback-only bind addresses
/// (skips the outbound-interface probe the zeroconf `0.0.0.0` default would
/// otherwise trigger) and a tight anti-entropy/tombstone cadence, so
/// convergence in every test below is a matter of milliseconds, not seconds.
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
/// once `timeout` elapses. `sundog`'s public API only exposes cluster/cache
/// state as point-in-time snapshots (`Cluster::peers()`, `Cache::get`), never
/// as a change stream a test could `.await` directly outside the crate, so
/// bounded polling — never a fixed `sleep` used as the assertion itself — is
/// the only race-free way to assert eventual convergence from here.
///
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

/// A running node plus the loopback gossip address it was built with.
///
/// Tracked here, rather than read back from `Cluster`, because the public API
/// has no accessor for a cluster's own advertised gossip address (only other
/// live nodes' addresses, via [`Cluster::peers`]) — see this suite's reported
/// API wart.
pub struct Node {
    pub cluster: Cluster,
    pub gossip_addr: SocketAddr,
}

/// Waits until `cluster` reports at least `expected` live peers.
///
/// # Panics
///
/// Panics if the bound is not reached within `timeout`.
pub async fn wait_for_peer_count(cluster: &Cluster, expected: usize, timeout: Duration) {
    eventually(timeout, || async { cluster.peers().len() >= expected }).await;
}

/// Shuts every node in `nodes` down gracefully, sequentially. Order doesn't
/// matter for correctness — each `Cluster::shutdown` only tears down its own
/// membership/mesh — but sequential, awaited shutdowns keep failures
/// attributable to a specific node rather than racing.
pub async fn shutdown_all(nodes: Vec<Node>) {
    for node in nodes {
        node.cluster.shutdown().await;
    }
}
