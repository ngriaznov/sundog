//! One-node clusters on loopback for the recipe tests: no multicast and no
//! seeds.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use sundog::{Cluster, ClusterConfig};

/// A node with no state-transfer wait, so a `Replicated` cache opens warm at
/// once.
pub(crate) async fn solo_cluster(name: &str) -> Cluster {
    solo(name, Duration::ZERO).await
}

/// A node with the default state-transfer budget, which a `Distributed` cache
/// needs nonzero; alone, it owns every part and has nothing to pull.
pub(crate) async fn solo_distributed_cluster(name: &str) -> Cluster {
    solo(name, ClusterConfig::default().state_transfer_budget).await
}

async fn solo(name: &str, state_transfer_budget: Duration) -> Cluster {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    Cluster::builder(name)
        .config(ClusterConfig::default().with(|c| {
            c.gossip_bind_addr = loopback;
            c.data_bind_addr = loopback;
            c.state_transfer_budget = state_transfer_budget;
        }))
        .seeds(Vec::<SocketAddr>::new())
        .build()
        .await
        .expect("a loopback node with no seeds builds")
}
