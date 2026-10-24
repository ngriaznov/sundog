//! A cluster whose caches never merge forgets the members that left.

use super::*;
use crate::cluster::test_support::wait_for_no_peers;

/// A joiner that leaves again is one more record in the tracker of the node
/// that saw it. A cluster of caches under the default resolver runs no CRDT
/// sweep, so the gc task every cache runs forgets the record, a receipt
/// lifetime after the departure.
#[tokio::test]
async fn a_cluster_with_only_a_plain_cache_forgets_a_peer_that_left() {
    let config = ClusterConfig {
        // A receipt lives 900 ms, and a gc tick comes every second.
        crdt_retire_after: Duration::from_millis(300),
        crdt_sweep_interval: Some(Duration::from_millis(100)),
        ..loopback_config()
    };
    let cluster_a = Cluster::builder("cluster-it-forget-departed")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let _plain = cluster_a
        .cache::<u32, String>("plain")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a plain Replicated cache opens");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-forget-departed")
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_a, 1).await;
    assert_eq!(
        cluster_a.absence_tracker().record_count(),
        1,
        "node a tracks the joiner"
    );

    cluster_b.shutdown().await;
    wait_for_no_peers(&cluster_a).await;
    wait_until(
        Duration::from_secs(20),
        "node a forgets the peer that left",
        async || cluster_a.absence_tracker().record_count() == 0,
    )
    .await;

    cluster_a.shutdown().await;
}
