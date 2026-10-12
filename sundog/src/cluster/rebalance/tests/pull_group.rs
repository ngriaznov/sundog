//! Tests for cold rounds, group settling and the per-group pull across donors.

use super::*;

/// [`empty_shard`] planning against `ownership`, so a part-scoped
/// anti-entropy round has a view to send.
fn owned_shard(ownership: &OwnershipTracker, residency: &Arc<ResidencySet>) -> Arc<dyn ShardOps> {
    Arc::new(
        crate::store::Shard::<u32, u32>::new(
            SmolStr::new("prices"),
            crate::store::Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            },
            crate::node::NodeId::random(),
            1024,
            None,
            None,
        )
        .with_ownership(ownership.clone(), Arc::clone(residency)),
    ) as Arc<dyn ShardOps>
}

#[test]
fn cold_round_settles_nothing_on_a_round_that_never_reached_the_donor() {
    let (a, b) = (PartId::from_raw(1), PartId::from_raw(2));
    let round = |failed: bool, matched: &[PartId]| anti_entropy::PartRoundOutcome {
        matched: matched.iter().copied().collect(),
        still_diverged: HashSet::new(),
        bytes_moved: 0,
        failed,
    };

    let mut diverging = vec![a, b];
    assert_eq!(
        cold_round(None, &mut diverging),
        ColdRound::Unreached,
        "a round that ran out of time"
    );
    assert_eq!(
        cold_round(Some(round(true, &[])), &mut diverging),
        ColdRound::Unreached,
        "a failed or stale round"
    );
    assert_eq!(diverging, vec![a, b], "an unreached round compares nothing");

    assert_eq!(
        cold_round(Some(round(false, &[a])), &mut diverging),
        ColdRound::Diverging
    );
    assert_eq!(diverging, vec![b]);
    assert_eq!(
        cold_round(Some(round(false, &[b])), &mut diverging),
        ColdRound::Matched
    );
    assert!(diverging.is_empty(), "{diverging:?}");
}

#[test]
fn settle_all_cold_serves_cold_parts_and_holds_back_unverified_ones() {
    let parts = [
        PartId::from_raw(1),
        PartId::from_raw(2),
        PartId::from_raw(3),
    ];
    let (settled, pull) = settle_all_cold(&parts, |part| part == PartId::from_raw(2));
    assert_eq!(settled, vec![PartId::from_raw(1), PartId::from_raw(3)]);
    assert_eq!(pull, GroupPull::Unverified);
    let (settled, pull) = settle_all_cold(&parts, |_| false);
    assert_eq!(settled, parts.to_vec());
    assert_eq!(pull, GroupPull::Landed(0));
}

/// The warm-reopen race: this node replayed part `a` from disk, the only
/// donor is still cold on it, and the part that donor could not give is
/// the one whose replay may lack keys. Serving it would answer a wrong
/// miss, so it stays cold and unverified while plain cold `b` serves.
#[cfg(feature = "spill")]
#[tokio::test]
async fn pull_one_group_keeps_an_unverified_part_waiting_when_every_donor_is_cold() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let (donor_node, requester_node) = (NodeId::from(111), NodeId::from(112));
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);
    let view_hash = ownership.current().view_hash();
    let donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        cold: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
    let (requester, _requester_inbound) =
        spawn_mesh(requester_node, Arc::new(BucketPullHandler::default())).await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

    let (a, b) = (PartId::new(3, 0), PartId::new(3, 1));
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&[a, b]);
    residency.mark_unverified(&[a]);

    let pull = pull_one_group(
        &owned_shard(&ownership, &residency),
        &requester,
        &cache,
        &ownership,
        &residency.open_pull(),
        vec![donor_node],
        vec![a, b],
        (Granularity::Part, view_hash),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(pull, GroupPull::Unverified);
    assert!(
        residency.is_cold(a) && residency.is_unverified(a),
        "the unverified replay keeps both marks, so reads ask the other owners"
    );
    assert!(
        !residency.is_cold(b),
        "the plain cold part serves what landed"
    );

    donor.shutdown().await;
    requester.shutdown().await;
}

/// Two owners that both regained a part hold only stale copies, and
/// each is the other's cold donor. Once they converge, no copy anywhere
/// is more complete, so the stale part serves like a plain cold one
/// instead of waiting for a warm donor that never comes.
#[tokio::test]
async fn pull_one_group_serves_a_stale_part_once_every_donor_is_cold() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let (donor_node, requester_node) = (NodeId::from(121), NodeId::from(122));
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);
    let view_hash = ownership.current().view_hash();
    let donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        cold: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
    let (requester, _requester_inbound) =
        spawn_mesh(requester_node, Arc::new(BucketPullHandler::default())).await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

    let part = PartId::new(3, 0);
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&[part]);
    residency.mark_stale(&[part]);

    let pull = pull_one_group(
        &owned_shard(&ownership, &residency),
        &requester,
        &cache,
        &ownership,
        &residency.open_pull(),
        vec![donor_node],
        vec![part],
        (Granularity::Part, view_hash),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(pull, GroupPull::Landed(0));
    assert!(
        !residency.is_cold(part) && !residency.is_unverified(part),
        "the stale part serves once converged with every cold donor"
    );

    donor.shutdown().await;
    requester.shutdown().await;
}

/// A donor that declines a group as cold can still hold the group's
/// keys, so a group whose converge never reaches it keeps its parts
/// cold rather than serving what landed here.
#[tokio::test]
async fn pull_one_group_keeps_a_cold_group_cold_while_its_donor_answers_anti_entropy_stale() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let (donor_node, requester_node) = (NodeId::from(131), NodeId::from(132));
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);
    let view_hash = ownership.current().view_hash();
    let donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        cold: true,
        ae_stale: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
    let (requester, _requester_inbound) =
        spawn_mesh(requester_node, Arc::new(BucketPullHandler::default())).await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

    let part = PartId::new(3, 0);
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&[part]);

    let pull = tokio::time::timeout(
        Duration::from_secs(2),
        pull_one_group(
            &owned_shard(&ownership, &residency),
            &requester,
            &cache,
            &ownership,
            &residency.open_pull(),
            vec![donor_node],
            vec![part],
            (Granularity::Part, view_hash),
            Duration::from_millis(500),
        ),
    )
    .await;
    assert!(
        pull.is_err(),
        "the group keeps retrying instead of settling: {pull:?}"
    );
    assert!(
        residency.is_cold(part),
        "reads keep asking the other owners"
    );

    donor.shutdown().await;
    requester.shutdown().await;
}

#[tokio::test]
async fn pull_one_group_clears_cold_per_bucket_as_each_done_arrives_before_the_group_finishes() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let view_hash = 42;
    let donor_node = NodeId::from(101);
    let requester_node = NodeId::from(102);

    // Bucket 0 completes; bucket 1's chunk lands but the donor then
    // stalls, a stand-in for a mid-group connection drop.
    let donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![
            (0u16, vec![sample_wire_record(1)]),
            (1u16, vec![sample_wire_record(2)]),
        ],
        stall_after: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
    let requester_handler = Arc::new(BucketPullHandler {
        view_hash: 0,
        chunks: Vec::new(),
        stall_after: false,
        ..Default::default()
    });
    let (requester, _requester_inbound) = spawn_mesh(requester_node, requester_handler).await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&two_buckets());
    let shard = empty_shard();
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

    let outcome = tokio::time::timeout(
        Duration::from_millis(500),
        pull_one_group(
            &shard,
            &requester,
            &cache,
            &ownership,
            &residency.open_pull(),
            vec![donor_node],
            two_buckets(),
            (Granularity::Bucket, view_hash),
            Duration::from_secs(5),
        ),
    )
    .await;
    assert!(
        outcome.is_err(),
        "the donor stalls after bucket 1's chunk, so the group never finishes within the \
         test's patience"
    );
    assert!(
        PartId::of_bucket(0).all(|p| !residency.is_cold(p)),
        "bucket 0's BucketDone landed and cleared its cold mark before the group finished"
    );
    assert!(
        PartId::of_bucket(1).all(|p| residency.is_cold(p)),
        "bucket 1's BucketDone never arrived, so it stays cold"
    );

    donor.shutdown().await;
    requester.shutdown().await;
}

/// A donor that stalls after finishing bucket 0 leaves bucket 1 for the
/// next donor, which is asked for bucket 1 alone.
#[tokio::test]
async fn pull_one_group_asks_a_later_donor_only_for_the_parts_no_earlier_attempt_finished() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let view_hash = 42;
    let (first_node, second_node, requester_node) =
        (NodeId::from(301), NodeId::from(302), NodeId::from(303));
    let first_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![
            (0u16, vec![sample_wire_record(1)]),
            (1u16, vec![sample_wire_record(2)]),
        ],
        stall_after: true,
        ..Default::default()
    });
    let second_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![(1u16, vec![sample_wire_record(2)])],
        stall_after: false,
        ..Default::default()
    });
    let (first, _first_inbound) = spawn_mesh(first_node, Arc::clone(&first_handler) as _).await;
    let (second, _second_inbound) = spawn_mesh(second_node, Arc::clone(&second_handler) as _).await;
    let (requester, _requester_inbound) = spawn_mesh(
        requester_node,
        Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
            ..Default::default()
        }),
    )
    .await;
    requester.update_peers(vec![
        peer_at(first_node, first.local_addr()),
        peer_at(second_node, second.local_addr()),
    ]);
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&two_buckets());
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

    let landed = pull_one_group(
        &empty_shard(),
        &requester,
        &cache,
        &ownership,
        &residency.open_pull(),
        vec![first_node, second_node],
        two_buckets(),
        (Granularity::Bucket, view_hash),
        Duration::from_millis(300),
    )
    .await;
    assert_eq!(
        landed,
        GroupPull::Landed(2 * crate::store::PART_COUNT as u64)
    );
    assert_eq!(
        *second_handler.requested.lock().expect("fixture mutex"),
        vec![vec![1u16]],
        "the second donor is asked only for the bucket the first never finished"
    );
    assert!(
        two_buckets()
            .into_iter()
            .all(|part| !residency.is_cold(part))
    );

    first.shutdown().await;
    second.shutdown().await;
    requester.shutdown().await;
}

/// A part-granular pull of thousands of parts, most of them empty,
/// finishes every part, and the donor records an ack for every one:
/// done frames and acks both cross their batch sizes.
#[tokio::test]
async fn a_pull_of_thousands_of_parts_finishes_and_acks_every_part() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    const PARTS: usize = 3_000;
    let cache = SmolStr::new("prices");
    let view_hash = 42;
    let (donor_node, requester_node) = (NodeId::from(401), NodeId::from(402));
    let parts: Vec<PartId> = PartId::all().step_by(7).take(PARTS).collect();
    let first = parts[0];
    let (donor, _donor_inbound) = spawn_mesh(
        donor_node,
        Arc::new(BucketPullHandler {
            view_hash,
            chunks: vec![(first.raw(), vec![sample_wire_record(1)])],
            stall_after: false,
            ..Default::default()
        }),
    )
    .await;
    let (requester, _requester_inbound) = spawn_mesh(
        requester_node,
        Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
            ..Default::default()
        }),
    )
    .await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);
    donor.update_peers(vec![peer_at(requester_node, requester.local_addr())]);
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&parts);
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

    let landed = pull_one_group(
        &empty_shard(),
        &requester,
        &cache,
        &ownership,
        &residency.open_pull(),
        vec![donor_node],
        parts.clone(),
        (Granularity::Part, view_hash),
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(landed, GroupPull::Landed(PARTS as u64));
    assert!(parts.iter().all(|&part| !residency.is_cold(part)));

    let ids: Vec<u16> = parts.iter().map(|part| part.raw()).collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let acked = donor.acked_owners(&cache, &ids, Duration::from_secs(60), view_hash);
        if acked.len() == PARTS {
            assert!(
                acked
                    .values()
                    .all(|owners| owners.contains(&requester_node))
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {} of {PARTS} parts acked",
            acked.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    donor.shutdown().await;
    requester.shutdown().await;
}

/// Pins that `credited`, threaded across two donor calls the way
/// `pull_one_group` does, counts bucket 0 once even though the first
/// donor stalls after landing it and the second donor re-sends it:
/// guards the regression where a donor retry double-counted a bucket.
#[tokio::test]
async fn try_donor_parts_shares_credited_parts_across_repeated_calls_for_one_group() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let view_hash = 42;
    let first_donor_node = NodeId::from(201);
    let second_donor_node = NodeId::from(202);
    let requester_node = NodeId::from(203);

    let first_donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![
            (0u16, vec![sample_wire_record(1)]),
            (1u16, vec![sample_wire_record(2)]),
        ],
        stall_after: true,
        ..Default::default()
    });
    let (first_donor, _first_donor_inbound) =
        spawn_mesh(first_donor_node, first_donor_handler).await;
    let second_donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![
            (0u16, vec![sample_wire_record(1)]),
            (1u16, vec![sample_wire_record(2)]),
        ],
        stall_after: false,
        ..Default::default()
    });
    let (second_donor, _second_donor_inbound) =
        spawn_mesh(second_donor_node, second_donor_handler).await;
    let requester_handler = Arc::new(BucketPullHandler {
        view_hash: 0,
        chunks: Vec::new(),
        stall_after: false,
        ..Default::default()
    });
    let (requester, _requester_inbound) = spawn_mesh(requester_node, requester_handler).await;
    requester.update_peers(vec![
        peer_at(first_donor_node, first_donor.local_addr()),
        peer_at(second_donor_node, second_donor.local_addr()),
    ]);

    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&two_buckets());
    let shard = empty_shard();
    let mut credited: HashSet<PartId> = HashSet::new();

    // Mirrors pull_one_group's per-donor timeout; the first donor
    // stalls after bucket 1's chunk.
    let first_call = tokio::time::timeout(
        Duration::from_millis(200),
        try_donor_parts(
            &shard,
            &requester,
            &cache,
            &residency.open_pull(),
            first_donor_node,
            &two_buckets(),
            (Granularity::Bucket, view_hash),
            &mut credited,
        ),
    )
    .await;
    assert!(
        first_call.is_err(),
        "the first donor stalls after bucket 1's chunk, so its own call never resolves \
         within the timeout, mirroring a dropped connection mid-group"
    );
    assert_eq!(
        credited,
        PartId::of_bucket(0).collect::<HashSet<_>>(),
        "bucket 0's BucketDone landed (flushed once bucket 1's chunk proved bucket 0 is \
         behind it) and was credited before the stall; bucket 1's own BucketDone never \
         arrived"
    );

    let (result_second, count_second, cold_second) = try_donor_parts(
        &shard,
        &requester,
        &cache,
        &residency.open_pull(),
        second_donor_node,
        &two_buckets(),
        (Granularity::Bucket, view_hash),
        &mut credited,
    )
    .await;
    assert_eq!(
        result_second,
        DonorResult::Done,
        "the second donor completes the whole group"
    );
    assert_eq!(
        count_second, 2,
        "the second donor redelivers bucket 0 and lands bucket 1"
    );
    assert!(!cold_second);
    assert_eq!(
        credited,
        two_buckets().into_iter().collect::<HashSet<_>>(),
        "bucket 0 stays credited exactly once even though the second donor redelivered its \
         BucketDone; only bucket 1 is newly credited"
    );

    first_donor.shutdown().await;
    second_donor.shutdown().await;
    requester.shutdown().await;
}

/// Every part of buckets 0 and 1: what a bucket-mode view's pull of
/// those two buckets covers.
fn two_buckets() -> Vec<PartId> {
    PartId::of_bucket(0).chain(PartId::of_bucket(1)).collect()
}
