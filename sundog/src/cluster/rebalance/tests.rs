//! Shared helpers and the topic modules of the rebalance unit tests.

use std::num::NonZeroU8;

use super::*;
use crate::node::NodeId;

/// A reference model of which parts this node holds every write for,
/// driven against the real `refresh_task` bookkeeping
/// ([`ResidencySet::record_published`]), [`plan_view_change`] and
/// [`ResidencySet::settle`] under the orderings the watch channel
/// allows: views published faster than the rebalance task looks
/// coalesce, and a pull can be superseded partway.
mod model;
mod planning;
mod pull_group;
mod pull_request;
mod release;

fn view(self_node: NodeId, eligible: Vec<NodeId>, k: u8) -> OwnershipView {
    OwnershipView::compute(self_node, eligible, NonZeroU8::new(k).expect("nonzero"))
}

fn empty_shard() -> Arc<dyn ShardOps> {
    Arc::new(crate::store::Shard::<u32, u32>::new(
        SmolStr::new("prices"),
        crate::store::Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        },
        crate::node::NodeId::random(),
        1024,
        None,
        None,
    )) as Arc<dyn ShardOps>
}

async fn solo_cluster(name: &str) -> crate::cluster::Cluster {
    crate::cluster::Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(crate::config::ClusterConfig {
            gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
            data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
            ..crate::config::ClusterConfig::default()
        })
        .build()
        .await
        .expect("solo cluster builds")
}

fn sample_wire_record(n: u8) -> crate::wire::WireRecord {
    crate::wire::WireRecord {
        key: bytes::Bytes::from(vec![n]),
        value: Some(bytes::Bytes::from(vec![n, n])),
        ver: crate::hlc::Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    }
}
