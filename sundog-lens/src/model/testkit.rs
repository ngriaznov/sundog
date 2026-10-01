//! Fixture builders for tests: members, snapshots and ownership digests with
//! deterministic ids and addresses.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::SystemTime;

use smol_str::SmolStr;
use sundog::membership::Peer;
use sundog::observe::{ClusterSnapshot, Member, MemberStatus};
use sundog::store::Mode;
use sundog::wire::PROTOCOL_VERSION;
use sundog::{NodeId, NodeName};

use super::ownership::OwnershipDigest;

/// The gossip address of fixture node `index`: `127.0.0.(10 + index):7946`.
#[must_use]
pub fn gossip_addr(index: u8) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 10 + index], 7946))
}

/// The node id of fixture node `index` at identity `generation`.
#[must_use]
pub fn node_id(index: u8, generation: u16) -> NodeId {
    NodeId::from(0x1000 + u64::from(index) + (u64::from(generation) << 16))
}

/// A member at fixture address `index` with identity `generation` and the
/// given incarnation, advertising `it` as `Distributed` with two owners.
#[must_use]
pub fn member_at(index: u8, generation: u16, incarnation: u64, status: MemberStatus) -> Member {
    let node = node_id(index, generation);
    let owners = NonZeroU8::new(2).expect("2 is nonzero");
    Member::new(
        Peer {
            node,
            name: NodeName::new("host", node),
            gossip_addr: gossip_addr(index),
            data_addr: SocketAddr::from(([127, 0, 0, 10 + index], 39211)),
            incarnation,
            protocol: PROTOCOL_VERSION,
        },
        status,
        SystemTime::UNIX_EPOCH,
        BTreeMap::from([(SmolStr::new("it"), Mode::Distributed { owners })]),
    )
}

/// Fixture member `index` with the given status.
#[must_use]
pub fn member(index: u8, status: MemberStatus) -> Member {
    member_at(index, 0, 1, status)
}

/// A snapshot of `live` live members, `1..=live`.
#[must_use]
pub fn snapshot(live: u8) -> ClusterSnapshot {
    ClusterSnapshot::new(
        "fixture",
        (1..=live)
            .map(|index| member(index, MemberStatus::Live))
            .collect(),
        0,
    )
}

/// The ownership digest of `cache` in `snapshot` with two owners, or `None`
/// when no member is eligible.
#[must_use]
pub fn ownership_digest(snapshot: &ClusterSnapshot, cache: &str) -> Option<OwnershipDigest> {
    let shares = snapshot.ownership(cache, Mode::DEFAULT_OWNERS)?;
    Some(OwnershipDigest::from_shares(Arc::new(shares), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_ids_and_addresses_are_distinct() {
        assert_ne!(node_id(1, 0), node_id(2, 0));
        assert_ne!(node_id(1, 0), node_id(1, 1));
        assert_eq!(gossip_addr(1), "127.0.0.11:7946".parse().unwrap());
    }

    #[test]
    fn a_snapshot_holds_live_members_in_node_order() {
        let snapshot = snapshot(3);
        assert_eq!(snapshot.members.len(), 3);
        assert!(
            snapshot
                .members
                .iter()
                .all(|m| m.status == MemberStatus::Live)
        );
        let ids: Vec<_> = snapshot.members.iter().map(|m| m.peer.node).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn ownership_digests_exist_only_with_a_live_advertiser() {
        assert!(ownership_digest(&snapshot(2), "it").is_some());
        assert!(ownership_digest(&snapshot(2), "other").is_none());
        let down = ClusterSnapshot::new("c", vec![member(1, MemberStatus::Down)], 0);
        assert!(ownership_digest(&down, "it").is_none());
    }
}
