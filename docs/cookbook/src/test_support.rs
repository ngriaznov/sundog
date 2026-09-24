//! A one-node cluster on loopback for the recipe tests: no multicast, no
//! seeds, and no state-transfer wait, so a `Replicated` cache opens warm at
//! once.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use sundog::{Cluster, ClusterConfig};

pub(crate) async fn solo_cluster(name: &str) -> Cluster {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    Cluster::builder(name)
        .config(ClusterConfig::default().with(|c| {
            c.gossip_bind_addr = loopback;
            c.data_bind_addr = loopback;
            c.state_transfer_budget = Duration::ZERO;
        }))
        .seeds(Vec::<SocketAddr>::new())
        .build()
        .await
        .expect("a loopback node with no seeds builds")
}
