//! Tests for the push rule, repair plans, peer choice, scheduling jitter and mismatch chunking.

use std::time::Duration;

use super::*;

#[test]
fn push_needed_skips_only_the_exact_version_the_peer_sent() {
    let ver = |wall_ms, node: u64| Hlc {
        wall_ms,
        logical: 0,
        node: NodeId::from(node),
    };
    assert!(push_needed(ver(5, 1), None));
    assert!(!push_needed(ver(5, 1), Some(ver(5, 1))));
    assert!(push_needed(ver(5, 1), Some(ver(4, 1))));
    assert!(push_needed(ver(5, 1), Some(ver(5, 2))));
}

#[test]
fn matched_parts_is_every_requested_part_in_scope_of_a_bucket_the_reply_did_not_name() {
    let requested: HashSet<PartId> = [PartId::new(4, 0), PartId::new(4, 5), PartId::new(9, 1)]
        .into_iter()
        .collect();
    // Bucket 4 matched, bucket 9 named mismatched: only bucket 4's
    // requested parts count.
    let whole = [(4, PartMask::ALL), (9, PartMask::ALL)];
    assert_eq!(
        matched_parts(&requested, &whole, &HashSet::from([9])),
        HashSet::from([PartId::new(4, 0), PartId::new(4, 5)])
    );
    // A mask narrows a matched bucket to the parts it names.
    let masked = [(4, [5].into_iter().collect()), (9, PartMask::ALL)];
    assert_eq!(
        matched_parts(&requested, &masked, &HashSet::new()),
        HashSet::from([PartId::new(4, 5), PartId::new(9, 1)])
    );
    // A bucket never covered never matches, even unnamed.
    assert!(matched_parts(&requested, &[], &HashSet::new()).is_empty());
}

#[test]
fn a_repair_plan_repairs_every_part_unscoped_and_only_its_scope_otherwise() {
    assert_eq!(RepairPlan::default().repairable(7), PartMask::ALL);
    let plan = RepairPlan {
        in_scope: Some(HashMap::from([(7, [1, 2].into_iter().collect())])),
        ..RepairPlan::default()
    };
    assert_eq!(plan.repairable(7).parts().collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(
        plan.repairable(8),
        PartMask::default(),
        "a bucket outside the scope repairs nothing"
    );
}

#[test]
fn retain_peer_owned_pushes_keeps_only_keys_whose_part_the_peer_owns() {
    use crate::ownership::{Granularity, OwnershipTracker, OwnershipView, ResidencySet};
    use crate::store::{Mode, Shard};

    let self_node = NodeId::from(1);
    let peer = NodeId::from(2);
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        self_node,
        &[],
        &HashMap::new(),
        &SmolStr::new("prices"),
        owners,
    );
    let view = Arc::new(OwnershipView::compute_at(
        self_node,
        (1..=5u64).map(NodeId::from).collect(),
        owners,
        Granularity::Part,
    ));
    tx.send(Arc::clone(&view)).expect("receiver alive");
    let shard: Arc<dyn ShardOps> = Arc::new(
        Shard::<u32, String>::new(
            SmolStr::new("prices"),
            Mode::Distributed { owners },
            self_node,
            u64::MAX,
            None,
            None,
        )
        .with_ownership(tracker, Arc::new(ResidencySet::new())),
    );
    let key_of = |key: u32| crate::store::encode_key(&key).expect("u32 key encodes");
    let peers_key = (0u32..1_000_000)
        .find(|&k| view.owners_of(part_of_u32_any(k)).contains(&peer))
        .expect("the peer owns some key");
    let other_key = (0u32..1_000_000)
        .find(|&k| !view.owners_of(part_of_u32_any(k)).contains(&peer))
        .expect("the peer does not own some key");
    let mut pushes = vec![key_of(peers_key), key_of(other_key)];
    retain_peer_owned_pushes(&shard, peer, &mut pushes);
    assert_eq!(pushes, vec![key_of(peers_key)]);

    // No view: identity.
    let plain: Arc<dyn ShardOps> = Arc::new(Shard::<u32, String>::new(
        SmolStr::new("plain"),
        Mode::Replicated,
        self_node,
        u64::MAX,
        None,
        None,
    ));
    let mut pushes = vec![key_of(peers_key), key_of(other_key)];
    retain_peer_owned_pushes(&plain, peer, &mut pushes);
    assert_eq!(pushes.len(), 2);
}

/// The ownership part a u32 test key hashes into, under either transport.
fn part_of_u32_any(key: u32) -> PartId {
    PartId::of_key(&crate::store::encode_key(&key).expect("u32 key encodes"))
}

#[test]
fn a_streaming_peer_is_skipped_a_bounded_number_of_times() {
    assert!(!should_skip_round(false, 0), "idle peers are never skipped");
    for skipped in 0..MAX_STREAMING_SKIPS {
        assert!(should_skip_round(true, skipped));
    }
    assert!(
        !should_skip_round(true, MAX_STREAMING_SKIPS),
        "a steady trickle cannot starve anti-entropy"
    );
}

#[test]
fn choose_peer_prefers_a_dirty_peer_over_live_ones() {
    let mut rng = rand::rng();
    let dirty = vec![NodeId::from(1)];
    let live = vec![NodeId::from(2), NodeId::from(3)];
    let (peer, was_dirty, give_back) =
        choose_peer(dirty, live, &mut rng).expect("a peer is chosen");
    assert_eq!(peer, NodeId::from(1));
    assert!(was_dirty);
    assert!(give_back.is_empty(), "{give_back:?}");
}

#[test]
fn choose_peer_hands_back_the_unchosen_dirty_peer() {
    let mut rng = rand::rng();
    let dirty = vec![NodeId::from(1), NodeId::from(2)];
    let (peer, was_dirty, give_back) =
        choose_peer(dirty.clone(), Vec::new(), &mut rng).expect("a peer is chosen");
    assert!(was_dirty);
    let other = dirty
        .into_iter()
        .find(|&p| p != peer)
        .expect("two dirty peers, one chosen");
    assert_eq!(give_back, vec![other]);
}

#[test]
fn choose_peer_falls_back_to_a_live_peer_with_no_dirty_ones() {
    let mut rng = rand::rng();
    let live = vec![NodeId::from(5)];
    let (peer, was_dirty, give_back) =
        choose_peer(Vec::new(), live, &mut rng).expect("a peer is chosen");
    assert_eq!(peer, NodeId::from(5));
    assert!(!was_dirty);
    assert!(give_back.is_empty(), "{give_back:?}");
}

#[test]
fn choose_peer_returns_none_with_no_peers_at_all() {
    let mut rng = rand::rng();
    assert_eq!(choose_peer(Vec::new(), Vec::new(), &mut rng), None);
}

#[test]
fn jittered_stays_within_the_expected_band() {
    for _ in 0..200 {
        let d = jittered(Duration::from_millis(1000));
        assert!(d >= Duration::from_millis(500) && d < Duration::from_millis(1500));
    }
}

#[test]
fn jittered_floors_at_one_millisecond_for_a_zero_interval() {
    assert!(jittered(Duration::ZERO) >= Duration::from_millis(1));
}

fn mismatch_bucket(bucket: u16) -> AeMismatch {
    AeMismatch::Bucket(bucket, Vec::new())
}

/// Pins `chunk_mismatches`'s chunk sizes and order, and that `0`
/// floors to `1` rather than looping forever.
#[test]
fn chunk_mismatches_splits_into_bounded_chunks_preserving_order() {
    struct Case {
        name: &'static str,
        count: u16,
        chunk_size: usize,
        expected_chunk_lens: &'static [usize],
    }
    let cases = [
        Case {
            name: "fewer mismatches than one chunk: a single chunk",
            count: 3,
            chunk_size: 64,
            expected_chunk_lens: &[3],
        },
        Case {
            name: "exactly one chunk's worth: a single chunk",
            count: 4,
            chunk_size: 4,
            expected_chunk_lens: &[4],
        },
        Case {
            name: "more than one chunk: full chunks then a remainder",
            count: 10,
            chunk_size: 4,
            expected_chunk_lens: &[4, 4, 2],
        },
        Case {
            name: "no mismatches: no chunks at all",
            count: 0,
            chunk_size: 4,
            expected_chunk_lens: &[],
        },
        Case {
            name: "a zero chunk size is floored to one, not an infinite loop",
            count: 3,
            chunk_size: 0,
            expected_chunk_lens: &[1, 1, 1],
        },
    ];
    for case in cases {
        let mismatches: Vec<AeMismatch> = (0..case.count).map(mismatch_bucket).collect();
        let chunks = chunk_mismatches(mismatches, case.chunk_size);
        let lens: Vec<usize> = chunks.iter().map(Vec::len).collect();
        assert_eq!(lens, case.expected_chunk_lens, "{}", case.name);
        let flattened: Vec<u16> = chunks
            .into_iter()
            .flatten()
            .map(|m| AeMismatch::bucket(&m))
            .collect();
        let expected: Vec<u16> = (0..case.count).collect();
        assert_eq!(
            flattened, expected,
            "{}: every mismatch survives, in order",
            case.name
        );
    }
}
