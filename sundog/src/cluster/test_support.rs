//! Shared helpers for `cluster`'s and `cache`'s real-transport test modules,
//! neither of which runs under the `sim` feature.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use smol_str::SmolStr;

use super::{Cluster, ShardRegistryExt};
use crate::config::ClusterConfig;
use crate::store::ShardOps;

/// Loopback-only config: skips the outbound-interface probe and keeps
/// anti-entropy/tombstone timing tight for fast, deterministic tests.
pub(crate) fn loopback_config() -> ClusterConfig {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    ClusterConfig {
        gossip_bind_addr: loopback,
        data_bind_addr: loopback,
        ae_interval: Duration::from_millis(200),
        tombstone_ttl: Duration::from_secs(2),
        // A one-second first-peer grace, so a lone node opens fast.
        state_transfer_budget: Duration::from_secs(5),
        ..ClusterConfig::default()
    }
}

pub(crate) async fn wait_for_peer_count(cluster: &Cluster, expected: usize) {
    let mut peers = cluster.inner.membership.peers();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if peers.borrow().len() >= expected {
                return;
            }
            if peers.changed().await.is_err() {
                return;
            }
        }
    })
    .await
    .expect("peers converge within the bound");
}

/// Waits until `cluster`'s live peer set is empty, for a departure or
/// crash scenario the failure detector needs a little time to notice.
pub(crate) async fn wait_for_no_peers(cluster: &Cluster) {
    let mut peers = cluster.inner.membership.peers();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if peers.borrow().is_empty() {
                return;
            }
            if peers.changed().await.is_err() {
                return;
            }
        }
    })
    .await
    .expect("the peer disappears from the live set within the bound");
}

/// `cluster`'s registered shard for `cache`, as
/// [`super::anti_entropy::run_round_against`] takes it: the same handle
/// `Cache::open` installs in the registry.
pub(crate) fn registered_shard(cluster: &Cluster, cache: &SmolStr) -> Arc<dyn ShardOps> {
    cluster
        .shards()
        .read_shards()
        .get(cache)
        .cloned()
        .expect("the cache was opened, so its shard is registered")
}

/// Polls `cond` every 20ms until it returns `true`, or panics with `msg` once
/// `budget` elapses.
pub(crate) async fn wait_until(budget: Duration, msg: &str, mut cond: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(budget, async {
        loop {
            if cond().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect(msg);
}
