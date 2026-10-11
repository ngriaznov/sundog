use std::net::SocketAddr;

use proptest::prelude::*;
use rand::seq::SliceRandom as _;
use rand::{SeedableRng as _, rngs::StdRng};

use super::*;
use crate::node::NodeName;

fn peer(node: u64, protocol: u16) -> Peer {
    Peer {
        node: NodeId::from(node),
        name: NodeName::new("host", NodeId::from(node)),
        gossip_addr: "127.0.0.1:0".parse::<SocketAddr>().expect("valid addr"),
        data_addr: "127.0.0.1:0".parse::<SocketAddr>().expect("valid addr"),
        incarnation: 0,
        protocol,
    }
}

fn modes_with(entries: &[(NodeId, &str, Mode)]) -> CacheModes {
    let mut modes: CacheModes = HashMap::new();
    for &(node, cache, mode) in entries {
        modes
            .entry(node)
            .or_default()
            .insert(SmolStr::new(cache), mode);
    }
    modes
}

#[test]
fn rendezvous_score_is_deterministic_across_repeated_calls() {
    let node = NodeId::from(42);
    assert_eq!(rendezvous_score(node, 7), rendezvous_score(node, 7));
}

#[test]
fn rendezvous_score_varies_with_bucket() {
    let node = NodeId::from(42);
    assert_ne!(rendezvous_score(node, 1), rendezvous_score(node, 2));
}

#[test]
fn owners_of_bucket_returns_top_k_by_descending_score() {
    let eligible: Vec<NodeId> = (1..=20u64).map(NodeId::from).collect();
    let bucket = 5;
    let mut expected: Vec<(u64, NodeId)> = eligible
        .iter()
        .map(|&n| (rendezvous_score(n, bucket), n))
        .collect();
    expected.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let expected_top3: Vec<NodeId> = expected.into_iter().take(3).map(|(_, n)| n).collect();

    assert_eq!(owners_of_bucket(&eligible, bucket, 3), expected_top3);
}

#[test]
fn owners_of_bucket_breaks_ties_by_ascending_node_id() {
    let tied = vec![
        (100, NodeId::from(9)),
        (100, NodeId::from(3)),
        (100, NodeId::from(7)),
    ];
    assert_eq!(
        rank_by_score(tied, 2),
        vec![NodeId::from(3), NodeId::from(7)],
        "equal scores break ties by ascending NodeId"
    );
}

#[test]
fn owners_of_bucket_degenerates_to_every_eligible_node_when_k_exceeds_eligible_count() {
    let eligible = vec![NodeId::from(1), NodeId::from(2)];
    let result = owners_of_bucket(&eligible, 42, 5);
    assert_eq!(result.len(), 2);
    assert!(eligible.iter().all(|n| result.contains(n)));
}

/// Holds `successor` equal to a fresh build of the same inputs, field by
/// field.
fn assert_same_view(successor: &OwnershipView, fresh: &OwnershipView) {
    assert_eq!(successor.view_hash, fresh.view_hash);
    assert_eq!(successor.eligible, fresh.eligible);
    assert_eq!(successor.stride, fresh.stride);
    assert_eq!(successor.owners, fresh.owners, "every unit's ranked owners");
    assert!(successor.owned_parts().eq(fresh.owned_parts()));
    assert_eq!(successor.owns_in_bucket, fresh.owns_in_bucket);
    assert_eq!(successor.co_owners, fresh.co_owners);
}

/// The nodes `1..=n` minus `left`, plus `joined`.
fn moved(n: u64, left: &[u64], joined: &[u64]) -> Vec<NodeId> {
    (1..=n)
        .filter(|node| !left.contains(node))
        .chain(joined.iter().copied())
        .map(NodeId::from)
        .collect()
}

proptest! {
    #[test]
    fn a_successor_view_equals_a_fresh_build(
        n in 1u64..24,
        k in 1u8..4,
        left in proptest::collection::vec(2u64..24, 0..4),
        joined in proptest::collection::vec(100u64..110, 0..4),
    ) {
        let self_node = NodeId::from(1);
        let k = NonZeroU8::new(k).expect("nonzero");
        let before = OwnershipView::compute_at(self_node, moved(n, &[], &[]), k, Granularity::Bucket);
        let eligible = moved(n, &left, &joined);
        let successor = before.successor(eligible.clone(), k, Granularity::Bucket);
        assert_same_view(
            &successor,
            &OwnershipView::compute_at(self_node, eligible, k, Granularity::Bucket),
        );
    }

    #[test]
    fn owners_of_bucket_is_invariant_to_input_order(seed in any::<u64>(), n in 1u64..30) {
        let mut nodes: Vec<NodeId> = (1..=n).map(NodeId::from).collect();
        let baseline = owners_of_bucket(&nodes, 11, 3);
        let mut rng = StdRng::seed_from_u64(seed);
        nodes.shuffle(&mut rng);
        let shuffled = owners_of_bucket(&nodes, 11, 3);
        prop_assert_eq!(baseline, shuffled);
    }
}

#[test]
fn view_hash_is_invariant_to_input_order_and_duplicates() {
    let a = vec![NodeId::from(1), NodeId::from(2), NodeId::from(3)];
    let b = vec![
        NodeId::from(3),
        NodeId::from(1),
        NodeId::from(2),
        NodeId::from(1),
    ];
    assert_eq!(view_hash(&a), view_hash(&b));
}

#[test]
fn view_hash_differs_for_different_live_sets() {
    let a = vec![NodeId::from(1), NodeId::from(2)];
    let b = vec![NodeId::from(1), NodeId::from(3)];
    assert_ne!(view_hash(&a), view_hash(&b));
}

#[test]
fn eligible_owners_excludes_a_protocol_two_peer() {
    let self_node = NodeId::from(1);
    let other = NodeId::from(2);
    let k = NonZeroU8::new(2).expect("nonzero");
    let peers = vec![peer(2, 2)];
    let modes = modes_with(&[(other, "cache", Mode::Distributed { owners: k })]);
    let cache = SmolStr::new("cache");

    assert_eq!(
        eligible_owners(self_node, &peers, &modes, &cache, k),
        vec![self_node]
    );
}

#[test]
fn eligible_owners_excludes_a_peer_that_has_not_opened_this_cache() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(2).expect("nonzero");
    let peers = vec![peer(2, 3)];
    let modes: CacheModes = HashMap::new();
    let cache = SmolStr::new("cache");

    assert_eq!(
        eligible_owners(self_node, &peers, &modes, &cache, k),
        vec![self_node]
    );
}

#[test]
fn eligible_owners_excludes_a_peer_advertising_a_different_owners_count() {
    let self_node = NodeId::from(1);
    let other = NodeId::from(2);
    let k = NonZeroU8::new(3).expect("nonzero");
    let peers = vec![peer(2, 3)];
    let modes = modes_with(&[(
        other,
        "cache",
        Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        },
    )]);
    let cache = SmolStr::new("cache");

    assert_eq!(
        eligible_owners(self_node, &peers, &modes, &cache, k),
        vec![self_node]
    );
}

#[test]
fn eligible_owners_always_includes_self_regardless_of_self_advertisement() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(2).expect("nonzero");
    // Self, but speaking a protocol too old to be eligible and
    // advertising nothing about this cache.
    let peers = vec![peer(1, 2)];
    let modes: CacheModes = HashMap::new();
    let cache = SmolStr::new("cache");

    assert!(eligible_owners(self_node, &peers, &modes, &cache, k).contains(&self_node));
}

#[test]
fn eligible_peers_never_adds_a_seat_of_its_own() {
    let k = NonZeroU8::new(2).expect("nonzero");
    let cache = SmolStr::new("cache");
    let distributed = Mode::Distributed { owners: k };
    let peers = vec![peer(2, 3), peer(3, 3), peer(4, 2), peer(5, 3), peer(6, 3)];
    let modes = modes_with(&[
        (NodeId::from(2), "cache", distributed),
        (NodeId::from(3), "cache", distributed),
        (NodeId::from(4), "cache", distributed),
        (NodeId::from(5), "cache", Mode::Replicated),
    ]);

    assert_eq!(
        eligible_peers(&peers, &modes, &cache, k),
        vec![NodeId::from(2), NodeId::from(3)],
        "a protocol-two peer, another mode and a silent peer are out"
    );
    assert!(
        eligible_peers(&[], &HashMap::new(), &cache, k).is_empty(),
        "no peer yields no seat, where eligible_owners yields the caller's"
    );
    assert_eq!(
        eligible_owners(NodeId::from(1), &[], &HashMap::new(), &cache, k),
        vec![NodeId::from(1)]
    );
}

#[test]
fn shared_owned_parts_is_empty_for_a_peer_owning_nothing_in_common() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(1).expect("nonzero");
    let view = OwnershipView::compute(self_node, vec![self_node], k);
    let stranger = NodeId::from(99);

    let shared = shared_owned_parts(&view, stranger);
    assert!(shared.is_empty(), "{shared:?}");
}

#[test]
fn shared_owned_parts_lists_exactly_the_parts_both_nodes_own() {
    let self_node = NodeId::from(1);
    let peer_node = NodeId::from(2);
    let eligible: Vec<NodeId> = (1..=5u64).map(NodeId::from).collect();
    let k = NonZeroU8::new(2).expect("nonzero");
    for granularity in [Granularity::Bucket, Granularity::Part] {
        let view = OwnershipView::compute_at(self_node, eligible.clone(), k, granularity);
        let shared = shared_owned_parts(&view, peer_node);
        assert!(!shared.is_empty(), "{shared:?}");
        for part in PartId::all() {
            let both = view.owns(part) && view.owners_of(part).contains(&peer_node);
            assert_eq!(shared.contains(&part), both, "{granularity:?} {part:?}");
        }
    }
}

#[test]
fn ownership_diff_reports_disjoint_gained_and_lost_sets() {
    let self_node = NodeId::from(1);
    let old_eligible: Vec<NodeId> = (1..=10u64).map(NodeId::from).collect();
    let new_eligible: Vec<NodeId> = (1..=10u64)
        .filter(|&n| n != 1)
        .chain(101..=110u64)
        .map(NodeId::from)
        .collect();
    let k = NonZeroU8::new(3).expect("nonzero");

    let old = OwnershipView::compute(self_node, old_eligible, k);
    let new = OwnershipView::compute(self_node, new_eligible, k);
    let (gained, lost) = ownership_diff(&old, &new);

    for &b in &gained {
        assert!(!old.owns(b) && new.owns(b));
    }
    for &b in &lost {
        assert!(old.owns(b) && !new.owns(b));
    }
    assert!(gained.iter().all(|b| !lost.contains(b)));
    assert!(
        !gained.is_empty() || !lost.is_empty(),
        "replacing most of the eligible set changes at least one bucket's ownership"
    );
}

#[test]
fn ownership_view_compute_always_places_self_somewhere_even_when_omitted_from_the_input() {
    let self_node = NodeId::from(7);
    let eligible = vec![NodeId::from(1), NodeId::from(2)];
    let k = NonZeroU8::new(1).expect("nonzero");

    let view = OwnershipView::compute(self_node, eligible, k);

    assert!(
        view.owned_parts().next().is_some(),
        "self is folded into eligible and owns at least one part somewhere"
    );
}

#[test]
fn ownership_view_owns_and_owners_of_and_owned_parts_agree_at_both_granularities() {
    let self_node = NodeId::from(1);
    let eligible = vec![self_node, NodeId::from(2), NodeId::from(3)];
    let k = NonZeroU8::new(2).expect("nonzero");
    for granularity in [Granularity::Bucket, Granularity::Part] {
        let view = OwnershipView::compute_at(self_node, eligible.clone(), k, granularity);
        assert_eq!(view.granularity(), granularity);
        assert_eq!(view.owned_parts().count(), view.owned_part_count());
        for part in PartId::all() {
            assert_eq!(view.owners_of(part).len(), 2);
            assert_eq!(
                view.owns(part),
                view.owners_of(part).contains(&self_node),
                "{granularity:?} {part:?}"
            );
        }
    }
}

#[test]
fn a_bucket_view_gives_every_part_of_a_bucket_the_bucket_s_owners() {
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=7u64).map(NodeId::from).collect();
    let view = OwnershipView::compute(
        self_node,
        eligible.clone(),
        NonZeroU8::new(3).expect("nonzero"),
    );
    for bucket in [0u16, 5, 1023] {
        let expected = owners_of_bucket(&eligible, bucket, 3);
        for part in PartId::of_bucket(bucket) {
            assert_eq!(view.owners_of(part), expected.as_slice());
        }
    }
}

#[test]
fn a_part_view_ranks_each_part_by_its_own_score() {
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=9u64).map(NodeId::from).collect();
    let k = NonZeroU8::new(2).expect("nonzero");
    let view = OwnershipView::compute_at(self_node, eligible.clone(), k, Granularity::Part);
    for part in [PartId::new(0, 0), PartId::new(7, 33), PartId::new(1023, 63)] {
        let expected = rank_by_score(
            eligible
                .iter()
                .map(|&n| (rendezvous_score(n, part.raw()), n))
                .collect(),
            2,
        );
        assert_eq!(view.owners_of(part), expected.as_slice());
    }
    let per_bucket: std::collections::HashSet<Vec<NodeId>> = PartId::of_bucket(3)
        .map(|part| view.owners_of(part).to_vec())
        .collect();
    assert!(
        per_bucket.len() > 1,
        "parts of one bucket land on different owners"
    );
}

#[test]
fn a_part_view_spreads_ownership_far_more_evenly_than_a_bucket_view_at_a_hundred_nodes() {
    let eligible: Vec<NodeId> = (1..=100u64).map(NodeId::from).collect();
    let k = NonZeroU8::new(2).expect("nonzero");
    let spread = |granularity: Granularity| -> (f64, f64) {
        let view = OwnershipView::compute_at(eligible[0], eligible.clone(), k, granularity);
        let mut load: HashMap<NodeId, usize> = HashMap::new();
        for part in PartId::all() {
            for &owner in view.owners_of(part) {
                *load.entry(owner).or_default() += 1;
            }
        }
        #[allow(clippy::cast_precision_loss, reason = "small exact counts")]
        let fair = (PART_SPACE * 2) as f64 / 100.0;
        #[allow(clippy::cast_precision_loss, reason = "small exact counts")]
        let ratio = |n: usize| n as f64 / fair;
        let max = load.values().copied().max().unwrap_or(0);
        let min = eligible
            .iter()
            .map(|n| load.get(n).copied().unwrap_or(0))
            .min()
            .unwrap_or(0);
        (ratio(max), ratio(min))
    };
    let (part_max, part_min) = spread(Granularity::Part);
    let (bucket_max, bucket_min) = spread(Granularity::Bucket);
    assert!(
        part_max < 1.15 && part_min > 0.85,
        "part view: {part_min:.2}..{part_max:.2}"
    );
    assert!(
        bucket_max > 1.3 || bucket_min < 0.7,
        "a bucket view at a hundred nodes is visibly lumpier: {bucket_min:.2}..{bucket_max:.2}"
    );
}

#[test]
fn owns_any_in_bucket_is_whether_any_owned_part_lies_in_the_bucket() {
    let self_node = NodeId::from(1);
    let eligible: Vec<NodeId> = (1..=6u64).map(NodeId::from).collect();
    let k = NonZeroU8::new(2).expect("nonzero");
    for granularity in [Granularity::Bucket, Granularity::Part] {
        let view = OwnershipView::compute_at(self_node, eligible.clone(), k, granularity);
        for bucket in 0..u16::try_from(BUCKET_COUNT).expect("fits") {
            assert_eq!(
                view.owns_any_in_bucket(bucket),
                PartId::of_bucket(bucket).any(|part| view.owns(part)),
                "bucket {bucket} at {granularity:?}"
            );
        }
    }
    let alone = OwnershipView::compute(self_node, vec![self_node], k);
    assert!(alone.owns_any_in_bucket(0));
    assert!(
        !alone.owns_any_in_bucket(u16::MAX),
        "a bucket past the space is never owned"
    );
}

#[test]
fn co_owners_are_every_other_owner_of_an_owned_part() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(2).expect("nonzero");
    for granularity in [Granularity::Bucket, Granularity::Part] {
        let eligible: Vec<NodeId> = (1..=8u64).map(NodeId::from).collect();
        let view = OwnershipView::compute_at(self_node, eligible, k, granularity);
        let mut expected: Vec<NodeId> = view
            .owned_parts()
            .flat_map(|part| view.owners_of(part).to_vec())
            .filter(|&node| node != self_node)
            .collect();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(view.co_owners(), expected.as_slice(), "at {granularity:?}");
        assert!(!view.co_owners().contains(&self_node));
    }
    let alone = OwnershipView::compute(self_node, vec![self_node], k);
    assert!(alone.co_owners().is_empty(), "a lone node has no co-owner");
}

#[test]
fn co_owners_with_returning_nodes_match_a_view_built_with_them_eligible() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(2).expect("nonzero");
    for granularity in [Granularity::Bucket, Granularity::Part] {
        let everyone: Vec<NodeId> = (1..=12u64).map(NodeId::from).collect();
        let full = OwnershipView::compute_at(self_node, everyone.clone(), k, granularity);
        let returning = [NodeId::from(4), NodeId::from(9)];
        let remaining: Vec<NodeId> = everyone
            .iter()
            .copied()
            .filter(|node| !returning.contains(node))
            .collect();
        let view = OwnershipView::compute_at(self_node, remaining, k, granularity);
        assert_eq!(
            view.co_owners_with(&returning),
            full.co_owners(),
            "at {granularity:?}"
        );
        assert_eq!(
            view.co_owners_with(&[]),
            view.co_owners(),
            "no returning node leaves the co-owners as they are"
        );
        assert_eq!(
            view.co_owners_with(&[self_node, NodeId::from(2)]),
            view.co_owners(),
            "nodes already eligible change nothing"
        );
    }
    let alone = OwnershipView::compute(self_node, vec![self_node], k);
    assert_eq!(
        alone.co_owners_with(&[NodeId::from(2)]),
        vec![NodeId::from(2)],
        "with two owners per part, a lone node's one returning peer co-owns every part"
    );
}

#[test]
fn a_part_view_successor_equals_a_fresh_build_across_joins_leaves_and_switches() {
    let self_node = NodeId::from(1);
    let two = NonZeroU8::new(2).expect("nonzero");
    let before = OwnershipView::compute_at(self_node, moved(20, &[], &[]), two, Granularity::Part);
    let cases = [
        ("a join", moved(20, &[], &[21]), two, Granularity::Part),
        ("a leave", moved(20, &[7], &[]), two, Granularity::Part),
        (
            "a join and a leave",
            moved(20, &[7, 9], &[21]),
            two,
            Granularity::Part,
        ),
        ("no change", moved(20, &[], &[]), two, Granularity::Part),
        (
            "another k",
            moved(20, &[], &[]),
            NonZeroU8::new(3).expect("nonzero"),
            Granularity::Part,
        ),
        (
            "bucket ranking",
            moved(20, &[], &[]),
            two,
            Granularity::Bucket,
        ),
        ("a lone node", vec![self_node], two, Granularity::Part),
    ];
    for (what, eligible, k, granularity) in cases {
        let successor = before.successor(eligible.clone(), k, granularity);
        let fresh = OwnershipView::compute_at(self_node, eligible, k, granularity);
        assert_eq!(successor.view_hash(), fresh.view_hash(), "{what}");
        assert_same_view(&successor, &fresh);
    }
    let alone = OwnershipView::compute_at(self_node, vec![self_node], two, Granularity::Part);
    assert_same_view(
        &alone.successor(moved(20, &[], &[]), two, Granularity::Part),
        &before,
    );
}

#[test]
fn carried_top_ranks_kept_and_joined_or_hands_back_a_unit_that_lost_a_member() {
    let mut top = Vec::new();
    assert!(carried_top(
        &[9, 7],
        &[8, 3],
        |_| true,
        2,
        &mut top,
        |a: &u8, b| b.cmp(a)
    ));
    assert_eq!(top, vec![9, 8]);
    assert!(!carried_top(
        &[9, 7],
        &[8],
        |&x| x != 7,
        2,
        &mut top,
        |a: &u8, b| b.cmp(a)
    ));
}

#[test]
fn units_name_every_bucket_or_every_part() {
    assert_eq!(units(Granularity::Bucket).count(), BUCKET_COUNT);
    assert_eq!(units(Granularity::Part).count(), PART_SPACE);
    assert_eq!(units(Granularity::Part).last(), Some(u16::MAX));
    assert_eq!(
        with_self(
            NodeId::from(2),
            vec![NodeId::from(3), NodeId::from(2), NodeId::from(3)]
        ),
        vec![NodeId::from(2), NodeId::from(3)]
    );
}

#[test]
fn top_by_score_matches_a_full_sort() {
    let eligible: Vec<NodeId> = (1..=40u64).map(NodeId::from).collect();
    let mut top = Vec::new();
    for raw in [0u16, 1, 999, 65_535] {
        for k in 1..=4usize {
            top_by_score(&eligible, raw, k, &mut top);
            let expected = rank_by_score(
                eligible
                    .iter()
                    .map(|&n| (rendezvous_score(n, raw), n))
                    .collect(),
                u8::try_from(k).expect("small"),
            );
            let got: Vec<NodeId> = top.iter().map(|&(_, n)| n).collect();
            assert_eq!(got, expected, "raw {raw} k {k}");
        }
    }
    top_by_score(&[NodeId::from(1)], 5, 3, &mut top);
    assert_eq!(top.len(), 1, "fewer nodes than k keeps every node");
}

#[test]
fn a_bucket_view_keeps_the_legacy_hash_and_a_part_view_does_not() {
    let self_node = NodeId::from(1);
    let eligible = vec![self_node, NodeId::from(2)];
    let k = NonZeroU8::new(1).expect("nonzero");
    let bucket = OwnershipView::compute_at(self_node, eligible.clone(), k, Granularity::Bucket);
    let part = OwnershipView::compute_at(self_node, eligible.clone(), k, Granularity::Part);
    assert_eq!(bucket.view_hash(), view_hash(&eligible));
    assert_ne!(part.view_hash(), bucket.view_hash());
    assert_eq!(part.view_hash(), view_hash_at(&eligible, Granularity::Part));
    let reordered = vec![NodeId::from(2), self_node, self_node];
    assert_eq!(
        view_hash_at(&reordered, Granularity::Part),
        part.view_hash(),
        "order and duplicates never change a part view's hash either"
    );
}

#[test]
fn ownership_granularity_is_part_only_when_every_eligible_peer_speaks_it() {
    let self_node = NodeId::from(1);
    let eligible = vec![self_node, NodeId::from(2), NodeId::from(3)];
    let current = wire::PROTOCOL_PART_OWNERSHIP;
    let everyone = vec![peer(2, current), peer(3, current)];
    let one_older = vec![peer(2, current), peer(3, current - 1)];
    let expected_when_all = if wire::PROTOCOL_VERSION >= current {
        Granularity::Part
    } else {
        Granularity::Bucket
    };
    assert_eq!(
        ownership_granularity(self_node, &everyone, &eligible),
        expected_when_all
    );
    assert_eq!(
        ownership_granularity(self_node, &one_older, &eligible),
        Granularity::Bucket,
        "one peer on an older protocol keeps the whole cluster on buckets"
    );
    assert_eq!(
        ownership_granularity(self_node, &[], &[self_node]),
        expected_when_all,
        "a lone node has no older peer to wait for"
    );
}

#[test]
fn for_members_is_part_only_when_every_protocol_speaks_part_ownership() {
    let current = wire::PROTOCOL_PART_OWNERSHIP;
    assert_eq!(
        Granularity::for_members([current, current + 1]),
        Granularity::Part
    );
    assert_eq!(
        Granularity::for_members([current, current - 1]),
        Granularity::Bucket,
        "one member on an older protocol keeps the cluster on buckets"
    );
    assert_eq!(
        Granularity::for_members([1]),
        Granularity::Bucket,
        "a protocol-one member has no part ownership"
    );
    assert_eq!(
        Granularity::for_members([]),
        Granularity::Part,
        "no member holds the cluster back, and this build adds no term"
    );
    assert_eq!(
        Granularity::for_members([current]),
        Granularity::for_protocols([current]),
        "the members' answer is a build's own when the build speaks part ownership"
    );
}

/// `refresh_task` decides whether a view changed from
/// [`view_hash_at`] over [`eligible_owners`] at
/// [`ownership_granularity`], before building anything: that hash is
/// the built view's, at either granularity.
#[test]
fn a_view_hash_is_known_before_the_view_is_built() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(2).expect("nonzero");
    let cache = SmolStr::new("prices");
    let mode = Mode::Distributed { owners: k };
    let modes = modes_with(&[
        (NodeId::from(2), "prices", mode),
        (NodeId::from(3), "prices", mode),
    ]);
    let current = wire::PROTOCOL_VERSION;
    for peers in [
        vec![peer(2, current), peer(3, current)],
        vec![peer(2, current), peer(3, wire::PROTOCOL_DISTRIBUTED)],
    ] {
        let eligible = eligible_owners(self_node, &peers, &modes, &cache, k);
        let granularity = ownership_granularity(self_node, &peers, &eligible);
        let built = compute_view(self_node, &peers, &modes, &cache, k);
        assert_eq!(built.granularity(), granularity);
        assert_eq!(view_hash_at(&eligible, granularity), built.view_hash());
    }
}

#[test]
fn wire_ids_and_parts_of_wire_id_round_trip_at_each_granularity() {
    let parts = vec![PartId::new(4, 0), PartId::new(4, 9), PartId::new(900, 63)];
    assert_eq!(wire_ids(Granularity::Bucket, &parts), vec![4, 900]);
    assert_eq!(wire_ids(Granularity::Part, &parts), {
        let mut raw: Vec<u16> = parts.iter().map(|p| p.raw()).collect();
        raw.sort_unstable();
        raw
    });
    let whole: Vec<PartId> = parts_of_wire_id(Granularity::Bucket, 4).collect();
    assert_eq!(whole, PartId::of_bucket(4).collect::<Vec<_>>());
    let one: Vec<PartId> = parts_of_wire_id(Granularity::Part, parts[1].raw()).collect();
    assert_eq!(one, vec![parts[1]]);
}

#[test]
fn owned_bucket_equivalent_sums_to_a_whole_bucket_per_sixty_four_parts() {
    assert!((owned_bucket_equivalent(0) - 0.0).abs() < f64::EPSILON);
    assert!((owned_bucket_equivalent(64) - 1.0).abs() < f64::EPSILON);
    assert!((owned_bucket_equivalent(PART_SPACE) - 1024.0).abs() < f64::EPSILON);
    assert!((owned_bucket_equivalent(96) - 1.5).abs() < f64::EPSILON);
}

#[test]
fn eligible_lists_the_ranked_nodes_with_self() {
    let k = NonZeroU8::new(2).expect("nonzero");
    let view = OwnershipView::compute_at(
        NodeId::from(9),
        vec![NodeId::from(5), NodeId::from(3), NodeId::from(5)],
        k,
        Granularity::Bucket,
    );
    assert_eq!(
        view.eligible(),
        [NodeId::from(3), NodeId::from(5), NodeId::from(9)],
        "ascending and deduplicated, with the caller's own seat folded in"
    );
}

#[test]
fn ownership_view_view_hash_matches_the_free_function() {
    let self_node = NodeId::from(1);
    let eligible = vec![self_node, NodeId::from(2)];
    let k = NonZeroU8::new(1).expect("nonzero");
    let view = OwnershipView::compute(self_node, eligible.clone(), k);

    assert_eq!(view.view_hash(), view_hash(&eligible));
}

#[test]
fn ownership_tracker_seed_never_returns_a_default_or_empty_view() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(2).expect("nonzero");
    let cache = SmolStr::new("cache");

    let (tracker, _tx) = OwnershipTracker::seed(self_node, &[], &HashMap::new(), &cache, k);
    let view = tracker.current();

    assert_eq!(
        view.owned_part_count(),
        PART_SPACE,
        "the sole eligible node owns every part, never zero of them"
    );
}

#[test]
fn ownership_tracker_current_reflects_the_seeded_view() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(1).expect("nonzero");
    let cache = SmolStr::new("cache");
    let eligible_peers = vec![peer(2, 3)];
    let modes = modes_with(&[(NodeId::from(2), "cache", Mode::Distributed { owners: k })]);

    let (tracker, _tx) = OwnershipTracker::seed(self_node, &eligible_peers, &modes, &cache, k);
    let expected = eligible_owners(self_node, &eligible_peers, &modes, &cache, k);

    assert_eq!(tracker.current().view_hash(), view_hash(&expected));
}

#[test]
fn ownership_tracker_subscribe_returns_a_receiver_that_sees_future_publishes() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(1).expect("nonzero");
    let cache = SmolStr::new("cache");
    let (tracker, tx) = OwnershipTracker::seed(self_node, &[], &HashMap::new(), &cache, k);
    let sub = tracker.subscribe();

    let new_view = Arc::new(OwnershipView::compute(
        self_node,
        vec![self_node, NodeId::from(2)],
        k,
    ));
    let expected_hash = new_view.view_hash();
    tx.send(new_view).expect("receiver still alive");

    assert_eq!(sub.borrow().view_hash(), expected_hash);
}

#[test]
fn ownership_tracker_baseline_stays_the_seeded_view_even_after_a_later_publish() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(2).expect("nonzero");
    let cache = SmolStr::new("cache");
    // The sole eligible node owns every bucket: the transient view
    // open() computes while racing gossip convergence.
    let (tracker, tx) = OwnershipTracker::seed(self_node, &[], &HashMap::new(), &cache, k);
    let baseline_hash = tracker.baseline().view_hash();
    assert_eq!(
        tracker.baseline().owned_part_count(),
        PART_SPACE,
        "the seeded, lone-node baseline owns every part"
    );

    // Simulates refresh_task correcting the view before a reader ever
    // re-derives its own starting point from the channel.
    let corrected = Arc::new(OwnershipView::compute(
        self_node,
        vec![self_node, NodeId::from(2)],
        k,
    ));
    let corrected_hash = corrected.view_hash();
    tx.send(Arc::clone(&corrected))
        .expect("receiver still alive");

    assert_eq!(
        tracker.current().view_hash(),
        corrected_hash,
        "current tracks the latest publish"
    );
    assert_eq!(
        tracker.baseline().view_hash(),
        baseline_hash,
        "baseline stays pinned to the tracker's original seeded view regardless of later \
         publishes"
    );
}

/// A `metrics::Recorder` that records every `sundog_owned_buckets` and
/// `sundog_owned_parts` set for one cache, so a test can observe a gauge
/// publish without the process-global recorder slot.
struct OwnedGaugeRecorder {
    cache: String,
    sets: Arc<parking_lot::Mutex<Vec<f64>>>,
    part_sets: Arc<parking_lot::Mutex<Vec<f64>>>,
}

struct OwnedGauge(Arc<parking_lot::Mutex<Vec<f64>>>);

impl metrics::GaugeFn for OwnedGauge {
    fn increment(&self, _value: f64) {}
    fn decrement(&self, _value: f64) {}
    fn set(&self, value: f64) {
        self.0.lock().push(value);
    }
}

impl metrics::Recorder for OwnedGaugeRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        metrics::Counter::noop()
    }

    fn register_gauge(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        let this_cache = key
            .labels()
            .any(|l| l.key() == "cache" && l.value() == self.cache);
        if !this_cache {
            return metrics::Gauge::noop();
        }
        let sets = match key.name() {
            "sundog_owned_buckets" => &self.sets,
            "sundog_owned_parts" => &self.part_sets,
            _ => return metrics::Gauge::noop(),
        };
        metrics::Gauge::from_arc(Arc::new(OwnedGauge(Arc::clone(sets))))
    }

    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

#[test]
fn ownership_tracker_seed_publishes_the_owned_gauges_for_its_first_view() {
    let self_node = NodeId::from(1);
    let other = NodeId::from(2);
    let k = NonZeroU8::new(1).expect("nonzero");
    let cache = SmolStr::new("seeded");
    let sets = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let part_sets = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let recorder = OwnedGaugeRecorder {
        cache: cache.to_string(),
        sets: Arc::clone(&sets),
        part_sets: Arc::clone(&part_sets),
    };
    let modes = modes_with(&[(other, "seeded", Mode::Distributed { owners: k })]);
    let peers = vec![peer(2, wire::PROTOCOL_DISTRIBUTED)];
    // The view once open()'s membership wait lands a peer; membership
    // won't change again so this seed is the only publish.
    let (tracker, _tx) = metrics::with_local_recorder(&recorder, || {
        OwnershipTracker::seed(self_node, &peers, &modes, &cache, k)
    });
    let parts = tracker.current().owned_part_count();
    let owned = owned_bucket_equivalent(parts);
    assert_eq!(
        sets.lock().as_slice(),
        &[owned],
        "the seed sets the gauge once, to the seeded view's owned share in buckets"
    );
    assert_eq!(
        part_sets.lock().as_slice(),
        &[f64::from(u32::try_from(parts).expect("fits u32"))],
        "and the part gauge once, to its owned-part count"
    );
    let total = f64::from(u32::try_from(BUCKET_COUNT).expect("fits u32"));
    assert!(
        owned > 0.0 && owned < total,
        "one owner over two nodes is a real split of the bucket space, not all or nothing: \
         {owned} of {total}"
    );
}

/// A part by raw id, for residency tests that only need distinct parts.
fn p(raw: u16) -> PartId {
    PartId::from_raw(raw)
}

#[test]
fn residency_of_reports_each_mark_alone() {
    let set = ResidencySet::new();
    let now = Instant::now();
    assert_eq!(set.residency_of(p(1), true, now), Residency::new(true));
    assert_eq!(set.residency_of(p(1), false, now), Residency::new(false));

    set.mark_cold(&[p(2)]);
    set.mark_stale(&[p(3)]);
    set.mark_releasing(&[p(4)]);
    let only = |part, owns| set.residency_of(part, owns, Instant::now());
    assert_eq!(
        only(p(2), true),
        Residency {
            cold_marked: true,
            ..Residency::new(true)
        }
    );
    assert_eq!(
        only(p(3), true),
        Residency {
            stale: true,
            ..Residency::new(true)
        }
    );
    let releasing = only(p(4), false);
    assert!(releasing.releasing_for.is_some() && releasing.resident() && !releasing.owns);

    let all = OwnershipView::compute(NodeId::from(1u64), vec![NodeId::from(1u64)], NonZeroU8::MIN);
    set.settle(&all);
    assert!(
        !only(p(5), true).unsettled,
        "every part of the settled view is settled"
    );
    let other = OwnershipView::compute(
        NodeId::from(2u64),
        vec![NodeId::from(1u64), NodeId::from(2u64)],
        NonZeroU8::MIN,
    );
    set.settle(&other);
    let gained = PartId::all()
        .find(|&part| !other.owns(part))
        .expect("two nodes split the parts");
    assert!(
        only(gained, true).unsettled && only(gained, true).implicitly_cold(),
        "a part outside the settled view is unsettled, so implicitly cold while owned"
    );
    set.mark_serving(&[gained]);
    assert!(!only(gained, true).unsettled, "a served part settles");
}

#[cfg(feature = "spill")]
#[test]
fn residency_of_reports_a_replayed_part_apart_from_a_stale_one() {
    let set = ResidencySet::new();
    set.mark_unverified(&[p(1)]);
    let replayed = set.residency_of(p(1), true, Instant::now());
    assert!(replayed.unverified && !replayed.stale && replayed.distrusted());
}

#[test]
fn residency_of_measures_the_grace_from_the_first_mark_and_unmark_restarts_it() {
    let set = ResidencySet::new();
    set.mark_releasing(&[p(1)]);
    let since = Instant::now();
    let later = since + Duration::from_secs(5);
    let first = set
        .residency_of(p(1), false, later)
        .releasing_for
        .expect("releasing");
    assert!(first >= Duration::from_secs(5), "{first:?}");
    set.mark_releasing(&[p(1)]);
    let again = set
        .residency_of(p(1), false, later)
        .releasing_for
        .expect("releasing");
    assert!(
        again >= first,
        "a second mark keeps the first clock: {again:?} {first:?}"
    );
    set.unmark(&[p(1)]);
    assert_eq!(set.residency_of(p(1), false, later).releasing_for, None);
    assert_eq!(
        set.residency_of(p(2), false, since).releasing_for,
        None,
        "a part never marked releasing reports no grace"
    );
}

#[test]
fn settlement_is_unsettled_is_the_one_definition() {
    let set = ResidencySet::new();
    assert!(
        !set.is_unsettled(p(1)),
        "nothing is unsettled before a settle"
    );
    let other = OwnershipView::compute(
        NodeId::from(2u64),
        vec![NodeId::from(1u64), NodeId::from(2u64)],
        NonZeroU8::MIN,
    );
    set.settle(&other);
    for part in PartId::all() {
        assert_eq!(
            set.is_unsettled(part),
            set.residency_of(part, true, Instant::now()).unsettled,
            "{part:?}"
        );
        assert_eq!(set.is_unsettled(part), !other.owns(part), "{part:?}");
    }
}

#[test]
fn residency_set_is_releasing_tracks_marked_parts() {
    let set = ResidencySet::new();
    assert!(!set.is_releasing(p(3)));

    set.mark_releasing(&[p(3), p(4)]);
    assert!(set.is_releasing(p(3)));
    assert!(set.is_releasing(p(4)));

    set.unmark(&[p(3)]);
    assert!(!set.is_releasing(p(3)));
    assert!(set.is_releasing(p(4)));
}

#[test]
fn residency_set_cold_marks_clear_per_part_or_all_at_once() {
    let set = ResidencySet::new();
    assert!(!set.is_cold(p(1)));
    set.mark_cold(&[p(1), p(2), p(3)]);
    assert!(set.is_cold(p(1)) && set.is_cold(p(2)) && set.is_cold(p(3)));
    assert!(
        !set.is_releasing(p(1)),
        "cold and releasing are separate marks"
    );
    set.clear_cold(&[p(2)]);
    assert!(set.is_cold(p(1)) && !set.is_cold(p(2)) && set.is_cold(p(3)));
    set.mark_all_serving();
    assert!(!set.is_cold(p(1)) && !set.is_cold(p(3)));
}

#[cfg(feature = "spill")]
#[test]
fn residency_set_unverified_marks_clear_per_part_and_are_distinct_from_cold() {
    let set = ResidencySet::new();
    assert!(!set.is_unverified(p(1)));

    set.mark_unverified(&[p(1), p(2), p(3)]);
    assert!(set.is_unverified(p(1)) && set.is_unverified(p(2)) && set.is_unverified(p(3)));
    assert!(
        !set.is_cold(p(1)),
        "unverified and cold are separate marks: marking one never marks the other"
    );

    set.clear_unverified(&[p(2)]);
    assert!(set.is_unverified(p(1)) && !set.is_unverified(p(2)) && set.is_unverified(p(3)));
}

#[test]
fn residency_set_stale_marks_distrust_a_hit_without_holding_the_part_back() {
    let set = ResidencySet::new();
    set.mark_stale(&[p(1), p(2)]);
    assert!(
        set.is_unverified(p(1)) && !set.is_replayed(p(1)),
        "a stale copy's hit is not trusted, but it is not a replay waiting on a warm donor"
    );
    assert!(!set.is_cold(p(1)), "stale and cold are separate marks");

    set.clear_unverified(&[p(1)]);
    assert!(!set.is_unverified(p(1)) && set.is_unverified(p(2)));
    set.mark_serving(&[p(2)]);
    assert!(!set.is_unverified(p(2)));
    set.mark_stale(&[p(3)]);
    set.mark_all_serving();
    assert!(!set.is_unverified(p(3)));
}

#[cfg(feature = "spill")]
#[test]
fn residency_set_cold_and_unverified_clear_independently() {
    let set = ResidencySet::new();
    set.mark_cold(&[p(1)]);
    set.mark_unverified(&[p(1)]);
    assert!(set.is_cold(p(1)) && set.is_unverified(p(1)));

    // cold and unverified clear independently; only mark_serving
    // clears both together.
    set.clear_cold(&[p(1)]);
    assert!(!set.is_cold(p(1)) && set.is_unverified(p(1)));

    set.mark_cold(&[p(1)]);
    set.clear_unverified(&[p(1)]);
    assert!(set.is_cold(p(1)) && !set.is_unverified(p(1)));
}

#[cfg(feature = "spill")]
#[test]
fn residency_set_mark_serving_clears_both_cold_and_unverified_together() {
    let set = ResidencySet::new();
    set.mark_cold(&[p(1), p(2)]);
    set.mark_unverified(&[p(1)]);
    assert!(set.is_cold(p(1)) && set.is_cold(p(2)) && set.is_unverified(p(1)));

    // mark_serving clears whichever marks a part carries.
    set.mark_serving(&[p(1), p(2)]);
    assert!(!set.is_cold(p(1)) && !set.is_cold(p(2)) && !set.is_unverified(p(1)));
}

#[test]
fn residency_set_a_part_the_settled_view_misses_stays_unsettled_until_it_serves() {
    let set = ResidencySet::new();
    let alone = NonZeroU8::new(1).expect("nonzero");
    let (me, other) = (NodeId::from(1), NodeId::from(2));
    let settled = OwnershipView::compute(me, vec![me, other], alone);
    let gained = PartId::all()
        .find(|&part| !settled.owns(part))
        .expect("two nodes split the parts");
    let kept = PartId::all()
        .find(|&part| settled.owns(part))
        .expect("two nodes split the parts");

    assert!(
        !set.is_unsettled(gained),
        "nothing is unsettled before a view settles"
    );
    set.settle(&settled);
    assert!(set.is_unsettled(gained));
    assert!(
        !set.is_unsettled(kept),
        "a part the settled view owns is settled"
    );

    set.mark_serving(&[gained]);
    assert!(
        !set.is_unsettled(gained),
        "its own pull landing settles the part"
    );

    set.settle(&settled);
    assert!(
        set.is_unsettled(gained),
        "settling again forgets what served under the previous settled view"
    );
    set.mark_all_serving();
    assert!(
        !set.is_unsettled(gained),
        "giving up the warm-up settles every part"
    );

    set.settle(&OwnershipView::compute(me, vec![me], alone));
    assert!(
        !set.is_unsettled(gained),
        "a settled view that owns the part settles it"
    );
}

/// A part a published view drops is unsettled at once and reaches
/// `take_lost`, even when `rebalance_task` never sees that view, and a
/// settle of an older view cannot vouch for it again.
#[test]
fn residency_set_a_dropped_part_stays_unsettled_and_is_taken_as_lost() {
    let set = ResidencySet::new();
    let alone = NonZeroU8::new(1).expect("nonzero");
    let (me, other) = (NodeId::from(1), NodeId::from(2));
    let whole = OwnershipView::compute(me, vec![me], alone);
    let split = OwnershipView::compute(me, vec![me, other], alone);
    let dropped = PartId::all()
        .find(|&part| !split.owns(part))
        .expect("two nodes split the parts");
    let kept = PartId::all()
        .find(|&part| split.owns(part))
        .expect("two nodes split the parts");

    set.settle(&whole);
    set.mark_serving(&[dropped]);
    set.record_published(&whole, &split);
    set.record_published(&split, &whole);
    assert!(
        set.is_unsettled(dropped),
        "dropped and owned again: writes made in between went elsewhere"
    );
    assert!(
        !set.is_unsettled(kept),
        "a part owned throughout stays settled"
    );

    set.settle(&whole);
    assert!(
        set.is_unsettled(dropped),
        "settling a view that owns it cannot vouch for a drop not yet taken"
    );

    let lost = set.take_lost();
    assert!(lost.contains(dropped) && !lost.contains(kept));
    assert!(
        set.take_lost().iter().next().is_none(),
        "taking drains the lost parts"
    );
    set.settle(&whole);
    assert!(
        !set.is_unsettled(dropped),
        "once taken and pulled, a later settle vouches for it"
    );
}

#[test]
fn residency_set_a_pull_landing_after_its_part_is_dropped_serves_nothing() {
    let set = ResidencySet::new();
    let alone = NonZeroU8::new(1).expect("nonzero");
    let (me, other) = (NodeId::from(1), NodeId::from(2));
    let whole = OwnershipView::compute(me, vec![me], alone);
    let split = OwnershipView::compute(me, vec![me, other], alone);
    let dropped = PartId::all()
        .find(|&part| !split.owns(part))
        .expect("two nodes split the parts");
    let kept = PartId::all()
        .find(|&part| split.owns(part))
        .expect("two nodes split the parts");

    set.record_published(&split, &whole);
    set.settle(&split);
    set.mark_cold(&[dropped, kept]);
    set.record_published(&whole, &split);
    set.mark_serving(&[dropped, kept]);
    assert!(
        set.is_cold(dropped),
        "a pull the view moved past forwarded its records away, so its part stays cold"
    );
    assert!(
        !set.is_cold(kept),
        "a part still owned serves once its pull lands"
    );

    set.record_published(&split, &whole);
    assert!(
        set.is_unsettled(dropped),
        "owned again, the part waits for its own pull instead of the one that landed while dropped"
    );
}

#[test]
fn a_pull_ticket_never_vouches_for_a_part_dropped_and_owned_again_while_it_ran() {
    let set = Arc::new(ResidencySet::new());
    let alone = NonZeroU8::new(1).expect("nonzero");
    let (me, other) = (NodeId::from(1), NodeId::from(2));
    let whole = OwnershipView::compute(me, vec![me], alone);
    let split = OwnershipView::compute(me, vec![me, other], alone);
    let flapped = PartId::all()
        .find(|&part| !split.owns(part))
        .expect("two nodes split the parts");
    let kept = PartId::all()
        .find(|&part| split.owns(part))
        .expect("two nodes split the parts");

    set.record_published(&split, &whole);
    set.settle(&whole);
    set.mark_cold(&[flapped, kept]);
    let ticket = set.open_pull();
    set.record_published(&whole, &split);
    let stale = set.open_pull();
    set.record_published(&split, &whole);
    let later = set.open_pull();

    ticket.mark_serving(&[flapped, kept]);
    assert!(
        set.is_cold(flapped),
        "records for the part went to its other owners while it was dropped"
    );
    assert!(!set.is_cold(kept), "a part owned throughout serves");
    stale.mark_serving(&[flapped]);
    assert!(
        set.is_cold(flapped),
        "a pull planned from a view the published one had moved past started without the part"
    );

    later.mark_serving(&[flapped]);
    assert!(
        !set.is_cold(flapped),
        "a pull that started after the part came back saw every record"
    );

    drop((ticket, stale, later));
    assert!(
        set.settlement.read().pulls.is_empty(),
        "a dropped ticket releases its hold"
    );
}

#[test]
fn residency_set_mark_all_serving_leaves_a_part_the_published_view_does_not_own() {
    let set = ResidencySet::new();
    let alone = NonZeroU8::new(1).expect("nonzero");
    let (me, other) = (NodeId::from(1), NodeId::from(2));
    let whole = OwnershipView::compute(me, vec![me], alone);
    let split = OwnershipView::compute(me, vec![me, other], alone);
    let dropped = PartId::all()
        .find(|&part| !split.owns(part))
        .expect("two nodes split the parts");
    let kept = PartId::all()
        .find(|&part| split.owns(part))
        .expect("two nodes split the parts");

    set.settle(&whole);
    set.mark_cold(&[dropped, kept]);
    set.record_published(&whole, &split);
    set.mark_all_serving();
    assert!(set.is_cold(dropped) && !set.is_cold(kept));

    set.record_published(&split, &whole);
    assert!(set.is_unsettled(dropped));
}

#[cfg(feature = "spill")]
#[test]
fn residency_set_mark_all_serving_clears_every_cold_and_unverified_mark() {
    let set = ResidencySet::new();
    set.mark_cold(&[p(1), p(2), p(3)]);
    set.mark_unverified(&[p(2), p(3)]);
    set.mark_releasing(&[p(9)]);

    set.mark_all_serving();

    assert!(!set.is_cold(p(1)) && !set.is_cold(p(2)) && !set.is_cold(p(3)));
    assert!(!set.is_unverified(p(2)) && !set.is_unverified(p(3)));
    assert!(
        set.is_releasing(p(9)),
        "mark_all_serving touches only the cold and unverified marks"
    );
}

#[test]
fn residency_set_releasing_parts_and_releasing_in_bucket_read_the_marks_in_bulk() {
    let set = ResidencySet::new();
    let releasing = set.releasing_parts();
    assert!(releasing.is_empty(), "{releasing:?}");
    assert!(!set.releasing_in_bucket(7));
    set.mark_releasing(&[PartId::new(7, 3), PartId::new(9, 0)]);
    let mut releasing = set.releasing_parts();
    releasing.sort_unstable();
    assert_eq!(releasing, {
        let mut expected = vec![PartId::new(7, 3), PartId::new(9, 0)];
        expected.sort_unstable();
        expected
    });
    assert!(set.releasing_in_bucket(7));
    assert!(set.releasing_in_bucket(9));
    assert!(!set.releasing_in_bucket(8));
    // Past `PART_COUNT` marks the per-bucket check reads the bucket's
    // own parts instead of scanning every mark; same answers.
    set.mark_releasing(&PartId::of_bucket(100).collect::<Vec<_>>());
    assert!(set.releasing_in_bucket(7));
    assert!(set.releasing_in_bucket(100));
    assert!(!set.releasing_in_bucket(8));
}

#[test]
fn residency_set_expired_returns_only_parts_past_grace() {
    let set = ResidencySet::new();
    set.mark_releasing(&[p(1), p(2)]);

    assert!(
        set.expired(Duration::from_secs(3600)).is_empty(),
        "nothing is past a huge grace yet"
    );
    let mut due = set.expired(Duration::ZERO);
    due.sort_unstable();
    assert_eq!(due, vec![p(1), p(2)], "everything is past a zero grace");
}

#[test]
fn residency_set_flap_resets_the_release_clock_instead_of_accumulating() {
    let set = ResidencySet::new();
    set.mark_releasing(&[p(5)]);
    std::thread::sleep(Duration::from_millis(400));
    // The part regains ownership before its grace elapses.
    set.unmark(&[p(5)]);
    // It's lost again immediately: the clock must restart from here,
    // not continue running from the first mark 400ms ago. A 200ms
    // grace leaves scheduler jitter a wide margin either way.
    set.mark_releasing(&[p(5)]);

    assert!(
        !set.expired(Duration::from_millis(200)).contains(&p(5)),
        "a flap resets the release clock rather than letting it accumulate across the gap"
    );
}
