//! Shared helpers and the topic modules of the cluster unit tests.

use super::test_support::{loopback_config, wait_for_peer_count, wait_until};
use super::*;

mod accept;
mod ae_expiry;
mod ae_repair;
mod background;
mod builder;
mod distributed;
mod forget_cache;
mod gc_task;
mod inbound;
mod invalidation_fold;
mod modes;
mod request_handler;
mod resolvers;
mod serve_chunks;
mod solo;
mod warmth;

/// A single-node cluster over loopback, the common case for a test that
/// needs no peers at all.
async fn solo_cluster(name: &str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds")
}

async fn two_node_cluster(cluster_name: &str) -> (Cluster, Cluster) {
    two_node_cluster_with_config(cluster_name, loopback_config()).await
}

/// A fresh two-node loopback pair on `config`, joined and converged
/// before this returns: `b` seeds off `a`'s gossip address, then both
/// see one live peer.
async fn two_node_cluster_with_config(
    cluster_name: &str,
    config: ClusterConfig,
) -> (Cluster, Cluster) {
    let cluster_a = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;

    let cluster_b = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("node b builds");

    wait_for_peer_count(&cluster_a, 1).await;
    wait_for_peer_count(&cluster_b, 1).await;
    (cluster_a, cluster_b)
}

/// A fresh 3-node cluster, all-to-all: every node sees the other two
/// before this returns.
async fn three_node_cluster(cluster_name: &str) -> (Cluster, Cluster, Cluster) {
    let (cluster_a, cluster_b) = two_node_cluster(cluster_name).await;
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let gossip_b = cluster_b.inner.membership.local_peer().gossip_addr;
    let cluster_c = Cluster::builder(cluster_name)
        .seeds([gossip_a, gossip_b])
        .config(loopback_config())
        .build()
        .await
        .expect("node c builds");
    wait_for_peer_count(&cluster_a, 2).await;
    wait_for_peer_count(&cluster_b, 2).await;
    wait_for_peer_count(&cluster_c, 2).await;
    (cluster_a, cluster_b, cluster_c)
}

/// Polls until `cluster` sees a live peer advertising `cache` under some
/// [`Mode`]. Needed before a second `open()`, or it could race the first.
async fn wait_for_cache_advertised(cluster: &Cluster, cache: &str) {
    wait_until(
        Duration::from_secs(15),
        "peer's cache-mode advertisement converges within the bound",
        async || {
            cluster
                .advertised_cache_modes()
                .values()
                .any(|caches| caches.contains_key(cache))
        },
    )
    .await;
}

/// `key`'s part, from its postcard-encoded bytes.
fn part_of_u32(key: u32) -> crate::store::PartId {
    crate::store::PartId::of_key(
        &postcard::to_stdvec(&key).expect("a u32 key always postcard-encodes"),
    )
}

/// Mirrors `store::bucket_of`'s formula so a test can compute which
/// anti-entropy bucket a key lands in without that private function.
fn bucket_of_u32(key: u32) -> u16 {
    let bytes = postcard::to_stdvec(&key).expect("a u32 key always postcard-encodes");
    let bucket = xxhash_rust::xxh3::xxh3_64(&bytes) & (crate::store::BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("masked to BUCKET_COUNT - 1, always fits in u16")
}
