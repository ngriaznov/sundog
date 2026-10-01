//! Fixture builders for tests: members, snapshots and ownership digests with
//! deterministic ids and addresses.

use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use smol_str::SmolStr;
use sundog::membership::Peer;
use sundog::observe::{ClusterSnapshot, Member, MemberStatus};
use sundog::store::Mode;
use sundog::wire::PROTOCOL_VERSION;
use sundog::{NodeId, NodeName};

use super::Model;
use super::ownership::OwnershipDigest;
use crate::source::Update;

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
    member_with(
        index,
        generation,
        incarnation,
        status,
        &[("it", distributed(2))],
    )
}

/// A member at fixture address `index` advertising `caches`.
#[must_use]
pub fn member_with(
    index: u8,
    generation: u16,
    incarnation: u64,
    status: MemberStatus,
    caches: &[(&str, Mode)],
) -> Member {
    let node = node_id(index, generation);
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
        caches
            .iter()
            .map(|&(name, mode)| (SmolStr::new(name), mode))
            .collect(),
    )
}

/// `Mode::Distributed` with `owners` owners per part.
///
/// # Panics
///
/// Panics when `owners` is 0.
#[must_use]
pub fn distributed(owners: u8) -> Mode {
    Mode::Distributed {
        owners: NonZeroU8::new(owners).expect("owners is nonzero"),
    }
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
    ownership_digest_after(snapshot, cache, None)
}

/// As [`ownership_digest`], compared against the digest it replaces.
#[must_use]
pub fn ownership_digest_after(
    snapshot: &ClusterSnapshot,
    cache: &str,
    previous: Option<&OwnershipDigest>,
) -> Option<OwnershipDigest> {
    let shares = snapshot.ownership(cache, Mode::DEFAULT_OWNERS)?;
    Some(OwnershipDigest::from_shares(Arc::new(shares), previous))
}

/// Fixture member `index` advertising `it` as `Distributed` with two owners
/// and `churn`, `pn` and `os` as `Replicated`.
#[must_use]
pub fn full_member(index: u8, status: MemberStatus) -> Member {
    member_with(
        index,
        0,
        1,
        status,
        &[
            ("it", distributed(2)),
            ("churn", Mode::Replicated),
            ("pn", Mode::Replicated),
            ("os", Mode::Replicated),
        ],
    )
}

/// A snapshot of eight full members: `n1` to `n5` live, `n6` departing, `n7`
/// down and `n8` left.
#[must_use]
pub fn mixed_snapshot() -> ClusterSnapshot {
    use MemberStatus::{Departing, Down, Left, Live};
    let statuses = [Live, Live, Live, Live, Live, Departing, Down, Left];
    ClusterSnapshot::new(
        "fixture",
        (1u8..)
            .zip(statuses)
            .map(|(index, status)| full_member(index, status))
            .collect(),
        0,
    )
}

/// A model that has watched a small story from `base`: three live members at
/// 0 s, eight live at 5 s, then `n6` departing, `n7` down and `n8` left at
/// 10 s, ticked to 20 s so the last view has settled. Wall-clock time is the
/// Unix epoch plus the same offsets, so the model is the same on every run
/// except for `base`.
///
/// # Panics
///
/// Panics when a fixture snapshot has no eligible member, which it never has.
#[must_use]
pub fn fixture_model(base: Instant) -> Model {
    let at = |secs: u64| base + Duration::from_secs(secs);
    let wall = |secs: u64| SystemTime::UNIX_EPOCH + Duration::from_secs(secs);
    let all_live = |count: u8| {
        ClusterSnapshot::new(
            "fixture",
            (1..=count)
                .map(|index| full_member(index, MemberStatus::Live))
                .collect(),
            0,
        )
    };
    let mut model = Model::new();
    for (secs, snapshot) in [(0, all_live(3)), (5, all_live(8)), (10, mixed_snapshot())] {
        let digest = ownership_digest_after(&snapshot, "it", model.ownership("it"))
            .expect("a live member advertises it");
        model.apply(
            Update::Snapshot(Arc::new(snapshot), at(secs)),
            at(secs),
            wall(secs),
        );
        model.apply(Update::Ownership(digest), at(secs), wall(secs));
    }
    model.tick(at(20));
    model
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
    fn members_carry_the_caches_they_are_given() {
        let member = member_with(
            2,
            0,
            1,
            MemberStatus::Live,
            &[("a", Mode::Replicated), ("b", distributed(3))],
        );
        assert_eq!(member.caches.len(), 2);
        assert_eq!(member.caches["a"], Mode::Replicated);
        assert_eq!(member.caches["b"], distributed(3));
        assert_eq!(
            member_at(2, 0, 1, MemberStatus::Live).caches["it"],
            distributed(2)
        );
    }

    #[test]
    fn the_mixed_snapshot_holds_every_status() {
        let snapshot = mixed_snapshot();
        let count = |status| {
            snapshot
                .members
                .iter()
                .filter(|m| m.status == status)
                .count()
        };
        assert_eq!(count(MemberStatus::Live), 5);
        assert_eq!(count(MemberStatus::Departing), 1);
        assert_eq!(count(MemberStatus::Down), 1);
        assert_eq!(count(MemberStatus::Left), 1);
        assert!(snapshot.members.iter().all(|m| m.caches.len() == 4));
        assert_eq!(
            full_member(1, MemberStatus::Live).caches["os"],
            Mode::Replicated
        );
    }

    #[test]
    fn the_fixture_model_has_watched_the_story() {
        let base = Instant::now();
        let model = fixture_model(base);
        assert_eq!(model.snapshot().unwrap().members.len(), 8);
        assert_eq!(model.ownership("it").unwrap().eligible.len(), 5);
        assert_eq!(model.settled("it"), Some(true));
        assert_eq!(model.now(), Some(base + Duration::from_secs(20)));
        let tags: Vec<_> = model.events().iter().map(|e| e.kind.tag()).collect();
        for tag in ["JOIN", "LEAVE", "DOWN", "LEFT", "VIEW", "SETTLED"] {
            assert!(tags.contains(&tag), "{tag} missing from {tags:?}");
        }
    }

    #[test]
    fn ownership_digests_exist_only_with_a_live_advertiser() {
        assert!(ownership_digest(&snapshot(2), "it").is_some());
        assert!(ownership_digest(&snapshot(2), "other").is_none());
        let down = ClusterSnapshot::new("c", vec![member(1, MemberStatus::Down)], 0);
        assert!(ownership_digest(&down, "it").is_none());
    }
}
