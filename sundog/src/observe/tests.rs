use std::time::UNIX_EPOCH;

use proptest::prelude::*;

use super::*;
use crate::membership::published_state_for_test;
use crate::ownership::{eligible_owners, ownership_granularity, view_hash, view_hash_at};
use crate::wire;

/// Every part in the key space.
const PARTS: usize = 65_536;

fn distributed(owners: u8) -> Mode {
    Mode::Distributed {
        owners: NonZeroU8::new(owners).expect("nonzero"),
    }
}

fn k(owners: u8) -> NonZeroU8 {
    NonZeroU8::new(owners).expect("nonzero")
}

fn peer(node: u64, incarnation: u64, protocol: u16) -> Peer {
    Peer {
        node: NodeId::from(node),
        name: NodeName::new("host", NodeId::from(node)),
        gossip_addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        data_addr: SocketAddr::from(([127, 0, 0, 1], 8000)),
        incarnation,
        protocol,
    }
}

pub(super) fn caches(entries: &[(&str, Mode)]) -> BTreeMap<SmolStr, Mode> {
    entries
        .iter()
        .map(|&(name, mode)| (SmolStr::new(name), mode))
        .collect()
}

fn member(node: u64, status: MemberStatus, protocol: u16, advertised: &[(&str, Mode)]) -> Member {
    Member::new(
        peer(node, 1, protocol),
        status,
        UNIX_EPOCH,
        caches(advertised),
    )
}

fn at(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().map(|&id| NodeId::from(id)).collect()
}

#[test]
fn status_of_maps_liveness_and_departure_to_each_status() {
    assert_eq!(status_of(true, false), MemberStatus::Live);
    assert_eq!(status_of(true, true), MemberStatus::Departing);
    assert_eq!(status_of(false, true), MemberStatus::Left);
    assert_eq!(status_of(false, false), MemberStatus::Down);
}

#[test]
fn observe_member_reads_peer_status_and_caches() {
    let expected = peer(7, 3, wire::PROTOCOL_VERSION);
    let advertised = [("sessions", distributed(2)), ("flags", Mode::Replicated)];
    let (id, state) = published_state_for_test(&expected, &advertised, false);

    let (read, status, read_caches) =
        observe_member(&id, &state, true).expect("a published state is a member");
    assert_eq!(read, expected, "the peer record is parse_peer's");
    assert_eq!(status, MemberStatus::Live);
    assert_eq!(read_caches, caches(&advertised));

    let (_, status, _) = observe_member(&id, &state, false).expect("a dead member");
    assert_eq!(status, MemberStatus::Down, "dropped with no departure");
}

#[test]
fn observe_member_is_none_without_sundog_node_state() {
    let id = ChitchatId::new(
        "observer.host-0000000000000001".to_string(),
        1,
        SocketAddr::from(([127, 0, 0, 1], 7000)),
    );
    let empty = NodeState::for_test();
    assert!(observe_member(&id, &empty, true).is_none());
    assert!(observe_member(&id, &empty, false).is_none());

    let mut cache_only = NodeState::for_test();
    cache_only.set("cache:sessions", "local");
    assert!(
        observe_member(&id, &cache_only, true).is_none(),
        "no node id, no member: the rule that keeps an observer off every peer list"
    );
}

#[test]
fn observe_member_keeps_caches_while_departing() {
    let advertised = [("sessions", distributed(2))];
    let (id, state) = published_state_for_test(&peer(7, 1, 6), &advertised, true);

    let (_, status, read_caches) = observe_member(&id, &state, true).expect("a member");
    assert_eq!(status, MemberStatus::Departing);
    assert_eq!(
        read_caches,
        caches(&advertised),
        "a departing member still shows what it held"
    );
    let (_, status, read_caches) = observe_member(&id, &state, false).expect("a member");
    assert_eq!(status, MemberStatus::Left);
    assert_eq!(read_caches, caches(&advertised));
}

#[test]
fn observe_member_skips_an_unrecognized_mode_token() {
    let (id, mut state) =
        published_state_for_test(&peer(7, 1, 6), &[("sessions", Mode::Replicated)], false);
    state.set("cache:bogus", "not-a-real-mode");

    let (_, _, read_caches) = observe_member(&id, &state, true).expect("a member");
    assert_eq!(read_caches, caches(&[("sessions", Mode::Replicated)]));
}

#[test]
fn next_members_keeps_since_while_the_status_holds_and_resets_it_on_a_change() {
    let observed = |node, incarnation, status| -> Observed {
        (peer(node, incarnation, 6), status, BTreeMap::new())
    };
    let first = next_members(
        &[],
        vec![
            observed(1, 1, MemberStatus::Live),
            observed(2, 1, MemberStatus::Live),
            observed(3, 1, MemberStatus::Live),
        ],
        at(100),
    );
    assert!(first.iter().all(|member| member.since == at(100)));

    let second = next_members(
        &first,
        vec![
            observed(1, 1, MemberStatus::Live),
            observed(2, 1, MemberStatus::Departing),
            observed(3, 2, MemberStatus::Live),
            observed(4, 1, MemberStatus::Live),
        ],
        at(200),
    );
    let since = |node: u64| {
        second
            .iter()
            .find(|member| member.peer.node == NodeId::from(node))
            .map(|member| member.since)
    };
    assert_eq!(since(1), Some(at(100)), "the same status keeps its since");
    assert_eq!(since(2), Some(at(200)), "a status change resets it");
    assert_eq!(since(3), Some(at(200)), "a new incarnation resets it");
    assert_eq!(since(4), Some(at(200)), "a new member starts now");

    let third = next_members(&second, vec![observed(1, 1, MemberStatus::Live)], at(300));
    assert_eq!(third.len(), 1, "a member gossip no longer holds is dropped");
    assert_eq!(third[0].since, at(100));
}

#[test]
fn next_members_sorts_by_node_then_incarnation() {
    let observed = |node, incarnation| -> Observed {
        (
            peer(node, incarnation, 6),
            MemberStatus::Live,
            BTreeMap::new(),
        )
    };
    let members = next_members(
        &[],
        vec![
            observed(3, 1),
            observed(1, 2),
            observed(1, 1),
            observed(2, 1),
        ],
        at(1),
    );
    let order: Vec<(u64, u64)> = members
        .iter()
        .map(|member| (member.peer.node.as_u64(), member.peer.incarnation))
        .collect();
    assert_eq!(order, [(1, 1), (1, 2), (2, 1), (3, 1)]);
}

#[test]
fn dead_status_maps_a_live_reading_to_its_dead_one() {
    assert_eq!(dead_status(MemberStatus::Live), MemberStatus::Down);
    assert_eq!(dead_status(MemberStatus::Departing), MemberStatus::Left);
}

#[test]
fn next_members_keeps_a_left_member_left_when_a_stale_heartbeat_revives_it() {
    let observed =
        |incarnation, status| -> Observed { (peer(1, incarnation, 6), status, BTreeMap::new()) };
    let left = next_members(&[], vec![observed(1, MemberStatus::Left)], at(100));

    let revived = next_members(&left, vec![observed(1, MemberStatus::Departing)], at(200));
    assert_eq!(
        revived[0].status,
        MemberStatus::Left,
        "a departure is final"
    );
    assert_eq!(revived[0].since, at(100), "and keeps its since");
    assert_eq!(revived, left);

    let later = next_members(&revived, vec![observed(1, MemberStatus::Left)], at(300));
    assert_eq!(later, left, "the dead reading agrees");

    let restarted = next_members(&left, vec![observed(2, MemberStatus::Live)], at(400));
    assert_eq!(
        restarted[0].status,
        MemberStatus::Live,
        "a new incarnation starts over"
    );

    let down = next_members(&[], vec![observed(1, MemberStatus::Down)], at(100));
    let recovered = next_members(&down, vec![observed(1, MemberStatus::Live)], at(200));
    assert_eq!(
        recovered[0].status,
        MemberStatus::Live,
        "a stalled node that returns is live again"
    );
    assert_eq!(recovered[0].since, at(200));
}

#[test]
fn next_snapshot_publishes_only_what_a_subscriber_would_see_change() {
    let observed = |node, status, advertised: &[(&str, Mode)]| -> Observed {
        (peer(node, 1, 6), status, caches(advertised))
    };
    let current = ClusterSnapshot::new(
        "c",
        next_members(
            &[],
            vec![
                observed(1, MemberStatus::Live, &[("d", Mode::Replicated)]),
                observed(2, MemberStatus::Live, &[]),
            ],
            at(100),
        ),
        1,
    );
    let same = || {
        vec![
            observed(2, MemberStatus::Live, &[]),
            observed(1, MemberStatus::Live, &[("d", Mode::Replicated)]),
        ]
    };

    assert!(
        next_snapshot(&current, same(), 1, at(200)).is_none(),
        "an identical read publishes nothing, whatever the instant"
    );

    let mut changed_cache = same();
    changed_cache[1].2 = caches(&[("d", Mode::Local)]);
    let next = next_snapshot(&current, changed_cache, 1, at(200)).expect("a cache changed");
    assert_eq!(next.cluster, "c");
    assert_eq!(next.members[0].caches, caches(&[("d", Mode::Local)]));
    assert_eq!(
        next.members[0].since,
        at(100),
        "an unchanged status keeps its since"
    );

    let mut changed_status = same();
    changed_status[0].1 = MemberStatus::Down;
    let next = next_snapshot(&current, changed_status, 1, at(200)).expect("a status changed");
    assert_eq!(next.members[1].status, MemberStatus::Down);
    assert_eq!(next.members[1].since, at(200));

    let next = next_snapshot(&current, same(), 2, at(200)).expect("the anonymous count moved");
    assert_eq!(next.anonymous, 2);
    assert_eq!(next.members, current.members);

    let mut joined = same();
    joined.push(observed(3, MemberStatus::Live, &[]));
    let next = next_snapshot(&current, joined, 1, at(200)).expect("a member joined");
    assert_eq!(next.members.len(), 3);

    let next = next_snapshot(
        &current,
        vec![observed(1, MemberStatus::Live, &[("d", Mode::Replicated)])],
        1,
        at(200),
    )
    .expect("a member vanished");
    assert_eq!(next.members.len(), 1);
}

#[test]
fn settle_window_is_at_least_a_refresh_and_four_gossip_intervals() {
    assert_eq!(
        settle_window(Duration::from_millis(200)),
        REFRESH_INTERVAL,
        "a short interval leaves the refresh as the floor"
    );
    assert_eq!(
        settle_window(Duration::from_millis(500)),
        Duration::from_secs(2)
    );
    assert_eq!(settle_window(Duration::MAX), Duration::MAX, "no overflow");
}

#[test]
fn dead_entry_settled_when_seen_before_or_the_heartbeat_has_held_still() {
    let settle = Duration::from_secs(1);
    assert!(!dead_entry_settled(Duration::ZERO, false, settle));
    assert!(!dead_entry_settled(
        Duration::from_millis(999),
        false,
        settle
    ));
    assert!(dead_entry_settled(settle, false, settle));
    assert!(dead_entry_settled(Duration::from_secs(9), false, settle));
    assert!(
        dead_entry_settled(Duration::ZERO, true, settle),
        "a member the snapshot already holds reports at once"
    );
}

#[test]
fn hold_keeps_the_instant_while_the_value_holds_and_resets_it_on_a_change() {
    let start = Instant::now();
    let later = start + Duration::from_secs(5);
    assert_eq!(
        hold(None, 7u64, start),
        (7, start),
        "a first sight starts now"
    );
    assert_eq!(
        hold(Some((7u64, start)), 7, later),
        (7, start),
        "same value"
    );
    assert_eq!(hold(Some((7u64, start)), 8, later), (8, later), "advanced");
}

/// A chitchat id and state for a sundog member.
fn gossiped(node: u64, advertised: &[(&str, Mode)], departing: bool) -> (ChitchatId, NodeState) {
    published_state_for_test(&peer(node, 1, 6), advertised, departing)
}

fn entry(
    (id, state): &(ChitchatId, NodeState),
    live: bool,
) -> (&ChitchatId, Option<&NodeState>, bool) {
    (id, Some(state), live)
}

#[test]
fn read_skips_self_and_counts_a_live_entry_without_node_state_as_anonymous() {
    let me = ChitchatId::new(
        "observer.host-0000000000000009".to_string(),
        1,
        SocketAddr::from(([127, 0, 0, 1], 7009)),
    );
    let other_observer = ChitchatId::new(
        "observer.host-000000000000000a".to_string(),
        1,
        SocketAddr::from(([127, 0, 0, 1], 7010)),
    );
    let no_state = ChitchatId::new(
        "host-000000000000000b".to_string(),
        1,
        SocketAddr::from(([127, 0, 0, 1], 7011)),
    );
    let silent_dead = ChitchatId::new(
        "observer.host-000000000000000c".to_string(),
        1,
        SocketAddr::from(([127, 0, 0, 1], 7012)),
    );
    let empty = NodeState::for_test();
    let live_member = gossiped(1, &[("d", Mode::Replicated)], false);

    let mut reader = Reader::default();
    let (observed, anonymous) = reader.read(
        &me,
        [
            (&me, Some(&empty), true),
            entry(&live_member, true),
            (&other_observer, Some(&empty), true),
            (&no_state, None, true),
            (&silent_dead, Some(&empty), false),
            (&no_state, None, false),
        ],
        &[],
        Instant::now(),
        Duration::ZERO,
    );
    assert_eq!(
        anonymous, 2,
        "another observer and a live entry with no node state, not self"
    );
    assert_eq!(
        observed.len(),
        1,
        "a dead entry with no node state is no member"
    );
    assert_eq!(observed[0].0, peer(1, 1, 6));
    assert_eq!(observed[0].1, MemberStatus::Live);
}

#[test]
fn read_reports_a_dead_entry_once_it_stopped_not_while_it_joins() {
    let me = ChitchatId::new(
        "observer.host-0000000000000009".to_string(),
        1,
        SocketAddr::from(([127, 0, 0, 1], 7009)),
    );
    let stopped = gossiped(1, &[("d", Mode::Replicated)], false);
    let left = gossiped(2, &[], true);
    let settle = Duration::from_secs(1);
    let start = Instant::now();
    let dead = || [entry(&stopped, false), entry(&left, false)];

    let mut reader = Reader::default();
    let (observed, _) = reader.read(&me, dead(), &[], start, settle);
    assert!(
        observed.is_empty(),
        "a node on first sight is still joining, not down or left"
    );
    let (observed, _) = reader.read(&me, dead(), &[], start + settle / 2, settle);
    assert!(
        observed.is_empty(),
        "two reads close together prove nothing"
    );

    let (observed, _) = reader.read(&me, dead(), &[], start + settle, settle);
    let statuses: Vec<MemberStatus> = observed.iter().map(|(_, status, _)| *status).collect();
    assert_eq!(statuses, [MemberStatus::Down, MemberStatus::Left]);

    // A node the snapshot already holds is reported at once, so a crash
    // reads as Down in the read after the failure detector's verdict.
    let published = vec![Member::new(
        peer(1, 1, 6),
        MemberStatus::Live,
        UNIX_EPOCH,
        BTreeMap::new(),
    )];
    let mut fresh = Reader::default();
    let (observed, _) = fresh.read(&me, dead(), &published, start, settle);
    let nodes: Vec<u64> = observed.iter().map(|(p, _, _)| p.node.as_u64()).collect();
    assert_eq!(nodes, [1], "node 2 is not in the snapshot yet");
    assert_eq!(observed[0].1, MemberStatus::Down);

    // Back in the live set, an entry forgets its dead history.
    let (observed, _) = reader.read(&me, [entry(&stopped, true)], &[], start + settle, settle);
    assert_eq!(observed[0].1, MemberStatus::Live);
    assert!(reader.dead.is_empty());
}

#[test]
fn read_parses_a_state_again_only_when_it_changed() {
    let me = ChitchatId::new(
        "observer.host-0000000000000009".to_string(),
        1,
        SocketAddr::from(([127, 0, 0, 1], 7009)),
    );
    let mut member = gossiped(1, &[("d", Mode::Replicated)], false);
    let other = gossiped(2, &[], false);
    let mut reader = Reader::default();
    let start = Instant::now();

    for _ in 0..5 {
        let (observed, _) = reader.read(
            &me,
            [entry(&member, true), entry(&other, true)],
            &[],
            start,
            Duration::ZERO,
        );
        assert_eq!(observed.len(), 2);
    }
    assert_eq!(
        reader.parses, 2,
        "five reads of two unchanged states parse each once"
    );

    // A liveness verdict is not a state change.
    let (observed, _) = reader.read(
        &me,
        [entry(&member, false), entry(&other, true)],
        &[],
        start,
        Duration::ZERO,
    );
    assert_eq!(reader.parses, 2, "live to dead re-reads nothing");
    assert_eq!(observed[0].1, MemberStatus::Down);

    member.1.set("cache:late", "replicated");
    let (observed, _) = reader.read(
        &me,
        [entry(&member, true), entry(&other, true)],
        &[],
        start,
        Duration::ZERO,
    );
    assert_eq!(reader.parses, 3, "only the changed state parses again");
    assert!(observed[0].2.contains_key("late"));

    let (_, _) = reader.read(&me, [entry(&other, true)], &[], start, Duration::ZERO);
    assert_eq!(
        reader.parsed.len(),
        1,
        "a node gossip dropped leaves the cache"
    );
}

#[test]
fn snapshot_new_sorts_members() {
    let unsorted = vec![
        member(3, MemberStatus::Live, 6, &[]),
        Member::new(
            peer(1, 2, 6),
            MemberStatus::Down,
            at(5),
            caches(&[("a", Mode::Local)]),
        ),
        member(1, MemberStatus::Left, 6, &[]),
    ];
    let snapshot = ClusterSnapshot::new("prod", unsorted, 2);

    assert_eq!(snapshot.cluster, "prod");
    assert_eq!(snapshot.anonymous, 2);
    let order: Vec<(u64, u64)> = snapshot
        .members
        .iter()
        .map(|member| (member.peer.node.as_u64(), member.peer.incarnation))
        .collect();
    assert_eq!(order, [(1, 1), (1, 2), (3, 1)]);
    let second = &snapshot.members[1];
    assert_eq!(
        second.status,
        MemberStatus::Down,
        "Member::new keeps its fields"
    );
    assert_eq!(second.since, at(5));
    assert_eq!(second.caches, caches(&[("a", Mode::Local)]));
}

#[test]
fn is_live_is_true_for_live_and_departing_only() {
    assert!(MemberStatus::Live.is_live());
    assert!(MemberStatus::Departing.is_live());
    assert!(!MemberStatus::Left.is_live());
    assert!(!MemberStatus::Down.is_live());
}

#[test]
fn ownership_is_none_without_a_live_advertiser() {
    let d = [("d", distributed(2))];
    assert!(
        ClusterSnapshot::new("c", Vec::new(), 0)
            .ownership("d", k(2))
            .is_none()
    );
    let snapshot = ClusterSnapshot::new(
        "c",
        vec![
            member(1, MemberStatus::Live, 6, &[("d", Mode::Replicated)]),
            member(2, MemberStatus::Live, 6, &[("other", distributed(2))]),
            member(3, MemberStatus::Departing, 6, &d),
            member(4, MemberStatus::Left, 6, &d),
            member(5, MemberStatus::Down, 6, &d),
        ],
        0,
    );
    assert!(snapshot.ownership("d", k(2)).is_none());
}

#[test]
fn ownership_excludes_departing_left_down_old_protocol_and_other_owner_counts() {
    let d2 = [("d", distributed(2))];
    let snapshot = ClusterSnapshot::new(
        "c",
        vec![
            member(1, MemberStatus::Live, 6, &d2),
            member(2, MemberStatus::Live, 6, &d2),
            member(3, MemberStatus::Departing, 6, &d2),
            member(4, MemberStatus::Left, 6, &d2),
            member(5, MemberStatus::Down, 6, &d2),
            member(6, MemberStatus::Live, wire::PROTOCOL_DISTRIBUTED - 1, &d2),
            member(7, MemberStatus::Live, 6, &[("d", distributed(3))]),
            member(8, MemberStatus::Live, 6, &[("d", Mode::Replicated)]),
        ],
        0,
    );

    let shares = snapshot.ownership("d", k(2)).expect("two live advertisers");
    assert_eq!(shares.eligible(), nodes(&[1, 2]));
    for out in [3, 4, 5, 6, 7, 8] {
        assert_eq!(shares.parts_owned_by(NodeId::from(out)), 0, "node {out}");
    }

    let shares = snapshot.ownership("d", k(3)).expect("one live advertiser");
    assert_eq!(shares.eligible(), nodes(&[7]));
}

#[test]
fn ownership_ranks_buckets_when_an_eligible_member_predates_part_ownership() {
    let d = [("d", distributed(2))];
    let current = wire::PROTOCOL_PART_OWNERSHIP;
    let mixed = ClusterSnapshot::new(
        "c",
        vec![
            member(1, MemberStatus::Live, wire::PROTOCOL_VERSION, &d),
            member(2, MemberStatus::Live, current - 1, &d),
            member(3, MemberStatus::Live, wire::PROTOCOL_VERSION, &d),
        ],
        0,
    );
    let shares = mixed.ownership("d", k(2)).expect("eligible members");
    assert!(!shares.ranks_parts(), "one older member keeps buckets");
    assert_eq!(shares.view_hash(), view_hash(&nodes(&[1, 2, 3])));
    for bucket in [0, 17, 1023] {
        let mut parts = PartId::of_bucket(bucket);
        let first = parts.next().expect("a bucket has parts");
        assert!(
            parts.all(|part| shares.owners_of(part) == shares.owners_of(first)),
            "a bucket view gives every part of bucket {bucket} the same owners"
        );
    }

    let current_only = ClusterSnapshot::new(
        "c",
        vec![
            member(1, MemberStatus::Live, current, &d),
            member(3, MemberStatus::Live, wire::PROTOCOL_VERSION, &d),
        ],
        0,
    );
    let shares = current_only.ownership("d", k(2)).expect("eligible members");
    assert!(shares.ranks_parts());
    assert_eq!(
        shares.view_hash(),
        view_hash_at(&nodes(&[1, 3]), Granularity::Part)
    );
}

#[test]
fn ownership_resolves_a_node_the_way_its_peers_do_across_two_incarnations() {
    // A rolling upgrade with a persisted node id: the old incarnation
    // (before part ownership) gossips its departure while the new one
    // is already live.
    let d = [("d", distributed(2))];
    let old_protocol = wire::PROTOCOL_PART_OWNERSHIP - 1;
    let incarnation = |node, incarnation, protocol, status| {
        Member::new(
            peer(node, incarnation, protocol),
            status,
            UNIX_EPOCH,
            caches(&d),
        )
    };
    let members = vec![
        incarnation(1, 1, wire::PROTOCOL_VERSION, MemberStatus::Live),
        incarnation(2, 1, old_protocol, MemberStatus::Departing),
        incarnation(2, 2, wire::PROTOCOL_VERSION, MemberStatus::Live),
        incarnation(3, 1, wire::PROTOCOL_VERSION, MemberStatus::Live),
    ];
    let shares = ClusterSnapshot::new("c", members.clone(), 0)
        .ownership("d", k(2))
        .expect("eligible members");
    assert_eq!(shares.eligible(), nodes(&[1, 2, 3]));
    assert!(
        !shares.ranks_parts(),
        "the departing incarnation is the first record of node 2"
    );

    // Node 1's own view, from the peers and modes `membership::run`
    // builds for it.
    let others: Vec<&Member> = members
        .iter()
        .filter(|m| m.status.is_live() && m.peer.node != NodeId::from(1))
        .collect();
    let peers: Vec<Peer> = others.iter().map(|m| m.peer.clone()).collect();
    let mut modes: CacheModes = HashMap::new();
    for other in &others {
        let advertised = if other.status == MemberStatus::Departing {
            HashMap::new()
        } else {
            other
                .caches
                .iter()
                .map(|(n, &mode)| (n.clone(), mode))
                .collect()
        };
        modes.insert(other.peer.node, advertised);
    }
    let cache = SmolStr::new("d");
    let eligible = eligible_owners(NodeId::from(1), &peers, &modes, &cache, k(2));
    let granularity = ownership_granularity(NodeId::from(1), &peers, &eligible);
    let view = OwnershipView::compute_at(NodeId::from(1), eligible, k(2), granularity);

    assert_eq!(granularity, Granularity::Bucket);
    assert_eq!(view.view_hash(), shares.view_hash());
    for part in PartId::all() {
        assert_eq!(view.owners_of(part), shares.owners_of(part));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// The lens's ownership is the view every eligible member computes
    /// for itself, which is the one thing it exists to show.
    #[test]
    fn ownership_matches_every_eligible_members_own_view(
        ids in proptest::collection::btree_set(1u64..10_000, 1..=8),
        traits in proptest::collection::vec((0u8..6, 0u8..8, 0u8..4), 8),
        owners in 1u8..=3,
    ) {
        let other_owners = if owners == 3 { 2 } else { owners + 1 };
        let members: Vec<Member> = ids
            .iter()
            .zip(&traits)
            .map(|(&id, &(status, mode, protocol))| {
                let status = match status {
                    0..=2 => MemberStatus::Live,
                    3 => MemberStatus::Departing,
                    4 => MemberStatus::Left,
                    _ => MemberStatus::Down,
                };
                let advertised: Vec<(&str, Mode)> = match mode {
                    0..=4 => vec![("d", distributed(owners))],
                    5 => vec![("d", distributed(other_owners))],
                    6 => vec![("d", Mode::Replicated)],
                    _ => Vec::new(),
                };
                let protocol = if protocol == 0 { 4 } else { wire::PROTOCOL_VERSION };
                member(id, status, protocol, &advertised)
            })
            .collect();
        let snapshot = ClusterSnapshot::new("c", members.clone(), 0);
        let owners = k(owners);

        let mut expected: Vec<NodeId> = members
            .iter()
            .filter(|m| {
                m.status == MemberStatus::Live
                    && m.peer.protocol >= wire::PROTOCOL_DISTRIBUTED
                    && m.caches.get("d") == Some(&Mode::Distributed { owners })
            })
            .map(|m| m.peer.node)
            .collect();
        expected.sort_unstable();
        let Some(shares) = snapshot.ownership("d", owners) else {
            prop_assert!(expected.is_empty());
            return Ok(());
        };
        prop_assert_eq!(shares.eligible(), expected.as_slice());

        // What a member sees: every live member, a departing one with
        // no advertised caches; a left or down one not at all.
        let cache = SmolStr::new("d");
        for own in members.iter().filter(|m| {
            expected.contains(&m.peer.node) && m.peer.protocol == wire::PROTOCOL_VERSION
        }) {
            let others: Vec<&Member> = members
                .iter()
                .filter(|m| m.status.is_live() && m.peer.node != own.peer.node)
                .collect();
            let peers: Vec<Peer> = others.iter().map(|m| m.peer.clone()).collect();
            let modes: CacheModes = others
                .iter()
                .map(|m| {
                    let advertised = if m.status == MemberStatus::Departing {
                        std::collections::HashMap::new()
                    } else {
                        m.caches.iter().map(|(n, &mode)| (n.clone(), mode)).collect()
                    };
                    (m.peer.node, advertised)
                })
                .collect();
            let eligible = eligible_owners(own.peer.node, &peers, &modes, &cache, owners);
            let granularity = ownership_granularity(own.peer.node, &peers, &eligible);
            let view = OwnershipView::compute_at(own.peer.node, eligible, owners, granularity);

            prop_assert_eq!(view.view_hash(), shares.view_hash());
            prop_assert_eq!(view.granularity() == Granularity::Part, shares.ranks_parts());
            for part in PartId::all() {
                prop_assert_eq!(view.owners_of(part), shares.owners_of(part));
            }
        }
    }
}

#[test]
fn ownership_shares_accessors_describe_the_view() {
    let d = [("d", distributed(2))];
    let snapshot = ClusterSnapshot::new(
        "c",
        vec![
            member(3, MemberStatus::Live, wire::PROTOCOL_VERSION, &d),
            member(1, MemberStatus::Live, wire::PROTOCOL_VERSION, &d),
            member(2, MemberStatus::Live, wire::PROTOCOL_VERSION, &d),
        ],
        0,
    );
    let shares = snapshot.ownership("d", k(2)).expect("three advertisers");

    assert_eq!(shares.cache(), "d");
    assert_eq!(shares.owners(), k(2));
    assert!(shares.ranks_parts());
    assert_eq!(shares.eligible(), nodes(&[1, 2, 3]));
    assert_eq!(
        shares.view_hash(),
        view_hash_at(&nodes(&[1, 2, 3]), Granularity::Part)
    );
    let reference =
        OwnershipView::compute_at(NodeId::from(1), nodes(&[1, 2, 3]), k(2), Granularity::Part);
    for part in PartId::all().step_by(97) {
        let owners = shares.owners_of(part);
        assert_eq!(owners.len(), 2);
        assert_ne!(owners[0], owners[1]);
        assert_eq!(owners, reference.owners_of(part));
    }
    let clone = shares.clone();
    assert_eq!(
        clone.view_hash(),
        shares.view_hash(),
        "clones share one view"
    );
}

#[test]
fn parts_owned_by_sums_to_part_space_times_min_owners_and_is_zero_off_the_eligible_set() {
    assert_eq!(PartId::all().count(), PARTS);
    let d2 = [("d", distributed(2))];
    let d3 = [("d", distributed(3))];
    let live = |node, advertised: &[(&str, Mode)]| {
        member(node, MemberStatus::Live, wire::PROTOCOL_VERSION, advertised)
    };
    let total = |shares: &OwnershipShares| -> usize {
        shares
            .eligible()
            .iter()
            .map(|&node| shares.parts_owned_by(node))
            .sum()
    };

    let three = ClusterSnapshot::new("c", vec![live(1, &d2), live(2, &d2), live(3, &d2)], 0);
    let shares = three.ownership("d", k(2)).expect("eligible");
    assert_eq!(total(&shares), PARTS * 2);
    assert!(
        shares
            .eligible()
            .iter()
            .all(|&node| shares.parts_owned_by(node) > 0)
    );
    assert_eq!(
        shares.parts_owned_by(NodeId::from(99)),
        0,
        "off the eligible set"
    );

    let one = ClusterSnapshot::new("c", vec![live(1, &d2)], 0);
    let shares = one.ownership("d", k(2)).expect("eligible");
    assert_eq!(
        shares.parts_owned_by(NodeId::from(1)),
        PARTS,
        "a lone member owns every part"
    );

    let two = ClusterSnapshot::new("c", vec![live(1, &d3), live(2, &d3)], 0);
    let shares = two.ownership("d", k(3)).expect("eligible");
    assert_eq!(
        total(&shares),
        PARTS * 2,
        "min(owners, eligible) owners a part"
    );
}
