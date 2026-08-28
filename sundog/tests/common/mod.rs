//! Shared harness for sundog's in-process multi-node integration suite (plan
//! §11.3): real [`Cluster`]s wired together with `Static` discovery over
//! pre-reserved loopback addresses, plus a bounded-wait polling helper so no
//! test ever asserts convergence after a fixed `sleep`.
//!
//! Each `tests/*.rs` file is its own binary and pulls in only the helpers it
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

/// Reserves a loopback UDP port the same way `Membership::spawn` reserves its
/// own zeroconf gossip port internally (probe-bind, read `local_addr`, drop):
/// the only way, from outside the crate, to learn a concrete gossip address
/// *before* a node exists — which every node in a `Static`-seeded group needs
/// to know about every other node.
async fn reserve_gossip_addr() -> SocketAddr {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback udp port to reserve a gossip address");
    socket
        .local_addr()
        .expect("a just-bound udp socket reports a local address")
}

/// Builds `n` nodes on cluster `name`, wired via `Static` discovery over
/// pre-reserved loopback gossip addresses, and waits until every node has
/// observed all `n - 1` others. Uses [`fast_config`]; see
/// [`spawn_cluster_group_with`] to override it.
///
/// # Panics
///
/// Panics if any node fails to build, or membership does not converge within
/// 20 seconds.
pub async fn spawn_cluster_group(name: &str, n: usize) -> Vec<Node> {
    spawn_cluster_group_with(name, n, fast_config).await
}

/// [`spawn_cluster_group`] with a caller-supplied [`ClusterConfig`] template
/// (still overridden per node with its own reserved `gossip_bind_addr`).
///
/// # Panics
///
/// Panics if any node fails to build, or membership does not converge within
/// 20 seconds.
pub async fn spawn_cluster_group_with(
    name: &str,
    n: usize,
    config: impl Fn() -> ClusterConfig,
) -> Vec<Node> {
    let mut addrs = Vec::with_capacity(n);
    for _ in 0..n {
        addrs.push(reserve_gossip_addr().await);
    }

    let mut nodes = Vec::with_capacity(n);
    for &gossip_addr in &addrs {
        let seeds: Vec<SocketAddr> = addrs
            .iter()
            .copied()
            .filter(|addr| *addr != gossip_addr)
            .collect();
        let cluster = Cluster::builder(name)
            .seeds(seeds)
            .config(config().with(|c| c.gossip_bind_addr = gossip_addr))
            .build()
            .await
            .expect("node builds");
        nodes.push(Node {
            cluster,
            gossip_addr,
        });
    }

    for node in &nodes {
        wait_for_peer_count(&node.cluster, n.saturating_sub(1), Duration::from_secs(20)).await;
    }
    nodes
}

/// Joins one more node onto an already-running group, seeded from `seeds`
/// (typically one or more survivors' [`Node::gossip_addr`]). Uses
/// [`fast_config`]; see [`join_node_with`] to override it.
///
/// # Panics
///
/// Panics if the node fails to build.
pub async fn join_node(name: &str, seeds: impl IntoIterator<Item = SocketAddr>) -> Node {
    join_node_with(name, seeds, fast_config).await
}

/// [`join_node`] with a caller-supplied [`ClusterConfig`] template.
///
/// # Panics
///
/// Panics if the node fails to build.
pub async fn join_node_with(
    name: &str,
    seeds: impl IntoIterator<Item = SocketAddr>,
    config: impl Fn() -> ClusterConfig,
) -> Node {
    let gossip_addr = reserve_gossip_addr().await;
    let cluster = Cluster::builder(name)
        .seeds(seeds)
        .config(config().with(|c| c.gossip_bind_addr = gossip_addr))
        .build()
        .await
        .expect("joining node builds");
    Node {
        cluster,
        gossip_addr,
    }
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
