//! The live peer set a cluster hands its consumers: one entry per remote
//! node, never this node's own id.

use super::*;
use crate::node::NodeId;

/// A node restarted under its persisted id before the failure detector
/// reaps the old generation is listed once, at the new generation.
#[tokio::test]
async fn a_restart_under_the_same_node_id_lists_one_peer_at_the_new_generation() {
    let id = NodeId::from(0x00c0_ffee_u64);
    let name = "cluster-it-restart-dedupe";
    let cluster_b = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let gossip_b = cluster_b.inner.membership.local_peer().gossip_addr;
    let build_a = || async {
        Cluster::builder(name)
            .seeds([gossip_b])
            .node_id(id)
            .config(loopback_config())
            .build()
            .await
            .expect("node a builds")
    };
    let old = build_a().await;
    wait_for_peer_count(&cluster_b, 1).await;
    let old_incarnation = old.local_incarnation();
    old.crash().await;
    // Incarnations are millisecond stamps; keep the restart's strictly later.
    tokio::time::sleep(Duration::from_millis(5)).await;

    let new = build_a().await;
    let new_incarnation = new.local_incarnation();
    assert!(new_incarnation > old_incarnation);
    wait_until(
        Duration::from_secs(15),
        "b lists the restarted generation",
        async || {
            cluster_b
                .peers()
                .iter()
                .any(|peer| peer.incarnation == new_incarnation)
        },
    )
    .await;
    let peers = cluster_b.peers();
    assert_eq!(peers.len(), 1, "one entry per node: {peers:?}");
    assert_eq!(peers[0].incarnation, new_incarnation);
    assert_eq!(cluster_b.health().live_peers, 1);

    new.shutdown().await;
    cluster_b.shutdown().await;
}

/// Two processes configured with one node id: the first sees the second as a
/// twin, does not count it as a peer and reports it in `Health`.
#[tokio::test]
async fn a_twin_with_the_local_node_id_is_excluded_and_reported() {
    let id = NodeId::from(0x7e57_u64);
    let name = "cluster-it-twin";
    let first = Cluster::builder(name)
        .seeds(std::iter::empty())
        .node_id(id)
        .config(loopback_config())
        .build()
        .await
        .expect("first builds");
    let gossip = first.inner.membership.local_peer().gossip_addr;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let twin = Cluster::builder(name)
        .seeds([gossip])
        .node_id(id)
        .config(loopback_config())
        .build()
        .await
        .expect("a second node with the same id builds");

    wait_until(
        Duration::from_secs(15),
        "the first node sees its twin",
        async || first.health().twin_nodes == 1,
    )
    .await;
    assert!(first.peers().is_empty(), "a twin is not a peer");
    assert_eq!(first.health().live_peers, 0);

    twin.shutdown().await;
    first.shutdown().await;
}
