//! Ownership views, per-part residency and the scoped digests a shard answers under them.

use super::*;
use crate::store::shard_ops::MaskedDigest;

/// [`distributed_shard`] under a part-granular view.
fn part_distributed_shard(
    self_node: NodeId,
    eligible: Vec<NodeId>,
    residency: Arc<ResidencySet>,
) -> (Shard<u32, String>, Arc<OwnershipView>) {
    let (shard, _, tx) =
        distributed_shard::<u32, String>(self_node, eligible.clone(), 2, residency);
    let view = Arc::new(OwnershipView::compute_at(
        self_node,
        eligible,
        NonZeroU8::new(2).expect("nonzero"),
        Granularity::Part,
    ));
    tx.send(Arc::clone(&view)).expect("receiver still alive");
    // The tracker keeps its own receiver; the sender can go.
    drop(tx);
    (shard, view)
}

/// The first `u32` key, scanning from `0`, for which `pick` holds.
fn find_key(pick: impl Fn(PartId) -> bool) -> u32 {
    (0..10_000_000u32)
        .find(|&key| pick(PartId::of_key(&key_bytes(&key))))
        .expect("a matching key is found within a generous scan range")
}

#[tokio::test]
async fn held_parts_and_release_parts_work_part_by_part_on_a_shard() {
    let s = shard::<u32, String>(1);
    let held = ShardOps::held_parts(&s).await;
    assert!(held.is_empty(), "{held:?}");
    let a = 1u32;
    let part_a = PartId::of_key(&key_bytes(&a));
    let b = find_key(|part| part.bucket() == part_a.bucket() && part != part_a);
    let part_b = PartId::of_key(&key_bytes(&b));
    s.insert(a, "a".to_string()).await.expect("insert");
    s.insert(b, "b".to_string()).await.expect("insert");
    let mut expected = vec![part_a, part_b];
    expected.sort_unstable();
    assert_eq!(ShardOps::held_parts(&s).await, expected);

    assert_eq!(ShardOps::release_parts(&s, &[part_a]).await, 1);
    assert!(s.get(&a).await.is_none());
    assert_eq!(s.get(&b).await, Some("b".to_string()));
    assert_eq!(ShardOps::held_parts(&s).await, vec![part_b]);
}

#[test]
fn is_cold_part_and_is_unverified_part_read_the_residency_set_per_part() {
    let plain = shard::<u32, String>(1);
    assert!(!ShardOps::is_cold_part(&plain, PartId::new(7, 1)));
    assert!(!ShardOps::is_unverified_part(&plain, PartId::new(7, 1)));

    let residency = Arc::new(ResidencySet::new());
    let (s, _view, _tx) = distributed_shard::<u32, String>(
        NodeId::from(1),
        (1..=3u64).map(NodeId::from).collect(),
        2,
        Arc::clone(&residency),
    );
    let cold = PartId::new(7, 1);
    residency.mark_cold(&[cold]);
    assert!(ShardOps::is_cold_part(&s, cold));
    assert!(
        !ShardOps::is_cold_part(&s, PartId::new(7, 2)),
        "only the marked part"
    );
    assert!(
        ShardOps::is_cold_bucket(&s, 7),
        "the bucket form means any part"
    );
    assert!(
        !ShardOps::is_unverified_part(&s, cold),
        "cold is not unverified"
    );
    residency.mark_serving(&[cold]);
    assert!(!ShardOps::is_cold_part(&s, cold));
}

/// The tracker publishes a view before `rebalance_task` marks what it
/// gains cold: a part gained in between still reads as cold, so a local
/// miss there is never taken as an answer.
#[test]
fn a_part_gained_under_a_view_not_yet_settled_is_cold_until_it_serves() {
    let (me, peer, other, victim) = (
        NodeId::from(1),
        NodeId::from(2),
        NodeId::from(3),
        NodeId::from(4),
    );
    let residency = Arc::new(ResidencySet::new());
    let (s, before, tx) = distributed_shard::<u32, String>(
        me,
        vec![me, peer, other, victim],
        2,
        Arc::clone(&residency),
    );
    residency.settle(&before);

    let after = Arc::new(OwnershipView::compute(
        me,
        vec![me, peer, other],
        NonZeroU8::new(2).expect("nonzero"),
    ));
    tx.send(Arc::clone(&after)).expect("receiver still alive");
    let gained = PartId::all()
        .find(|&part| after.owns(part) && !before.owns(part))
        .expect("losing a node hands some of its parts to this one");
    let kept = PartId::all()
        .find(|&part| after.owns(part) && before.owns(part))
        .expect("this node keeps some parts");
    let not_owned = PartId::all()
        .find(|&part| !after.owns(part))
        .expect("with two owners among three nodes, some parts are elsewhere");

    assert!(
        ShardOps::is_cold_part(&s, gained),
        "gained under a published view, no mark_cold yet: still cold"
    );
    assert!(!ShardOps::is_cold_part(&s, kept));
    assert!(
        !ShardOps::is_cold_part(&s, not_owned),
        "a part this node does not own is never cold here"
    );

    residency.mark_serving(&[gained]);
    assert!(!ShardOps::is_cold_part(&s, gained), "its pull landed");
}

#[tokio::test]
async fn ae_scoped_digests_for_names_buckets_under_a_bucket_view() {
    let self_node = NodeId::from(1);
    let peer = NodeId::from(2);
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(
        self_node,
        (1..=5u64).map(NodeId::from).collect(),
        2,
        residency,
    );
    let scoped = ShardOps::ae_scoped_digests_for(&s, peer)
        .await
        .expect("a distributed shard has scoped digests");
    assert_eq!(scoped.view_hash, view.view_hash());
    let bucket_form: Vec<(u16, u64)> = ShardOps::ae_digests_for(&s, peer)
        .await
        .into_iter()
        .map(|bd| (bd.bucket, bd.digest))
        .collect();
    assert_eq!(
        scoped.scope,
        Scope::Buckets(bucket_form),
        "the scope is ae_digests_for's buckets"
    );
    assert!(
        ShardOps::ae_scoped_digests_for(&shard::<u32, String>(1), peer)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn ae_scoped_digests_for_masks_each_shared_part_under_a_part_view() {
    let self_node = NodeId::from(1);
    let peer = NodeId::from(2);
    let residency = Arc::new(ResidencySet::new());
    let (s, view) = part_distributed_shard(
        self_node,
        (1..=5u64).map(NodeId::from).collect(),
        Arc::clone(&residency),
    );
    let shared = find_key(|part| view.owns(part) && view.owners_of(part).contains(&peer));
    let shared_part = PartId::of_key(&key_bytes(&shared));
    s.insert(shared, "v".to_string())
        .await
        .expect("owner's insert lands");
    // A part the peer owns and this node is releasing stays in scope.
    let releasing = PartId::all()
        .find(|&part| !view.owns(part) && view.owners_of(part).contains(&peer))
        .expect("the peer owns some part this node does not");
    residency.mark_releasing(&[releasing]);

    let scoped = ShardOps::ae_scoped_digests_for(&s, peer)
        .await
        .expect("a distributed shard has scoped digests");
    assert_eq!(scoped.view_hash, view.view_hash());
    let Scope::Parts(masked) = &scoped.scope else {
        panic!("a part view masks its scope");
    };
    assert!(
        masked
            .windows(2)
            .all(|pair| pair[0].bucket < pair[1].bucket),
        "one entry per bucket, ascending"
    );
    let mut expected: Vec<PartId> = crate::ownership::shared_owned_parts(&view, peer);
    expected.push(releasing);
    expected.sort_unstable();
    let mut named: Vec<PartId> = masked
        .iter()
        .flat_map(|m| m.mask.parts().map(|part| PartId::new(m.bucket, part)))
        .collect();
    named.sort_unstable();
    assert_eq!(named, expected, "every shared part, plus the releasing one");
    let entry = masked
        .iter()
        .find(|m| m.bucket == shared_part.bucket())
        .expect("the shared part's bucket is in scope");
    let part_digests = ShardOps::part_digests(&s, vec![shared_part.bucket()]).await;
    assert_eq!(
        entry.digest,
        entry.mask.fold(&part_digests[0].digests),
        "a bucket's scoped digest folds its engine part digests over its mask"
    );
    assert_ne!(entry.digest, 0, "the bucket holding a record has a digest");
}

#[test]
fn a_scope_covers_whole_buckets_or_their_masks_and_retains_by_them() {
    let mut buckets = Scope::Buckets(vec![(3, 30), (5, 50)]);
    assert_eq!(
        buckets.coverage(),
        vec![(3, PartMask::ALL), (5, PartMask::ALL)]
    );
    buckets.retain(|bucket, _| bucket == 5);
    assert_eq!(buckets, Scope::Buckets(vec![(5, 50)]));

    let mask = |parts: &[u8]| parts.iter().copied().collect::<PartMask>();
    let entry = |bucket, parts: &[u8]| MaskedDigest {
        bucket,
        mask: mask(parts),
        digest: u64::from(bucket),
    };
    let mut parts = Scope::Parts(vec![entry(3, &[1]), entry(5, &[2, 9])]);
    assert_eq!(parts.coverage(), vec![(3, mask(&[1])), (5, mask(&[2, 9]))]);
    parts.retain(|_, mask| mask.contains(9));
    assert_eq!(parts, Scope::Parts(vec![entry(5, &[2, 9])]));
}

#[tokio::test]
async fn a_distributed_shard_leaves_a_key_it_does_not_own_to_the_owners() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);
    let owned = find_key_by_ownership(&view, true);
    let unowned = find_key_by_ownership(&view, false);
    s.insert(owned, "owned".to_string()).await.expect("insert");
    let _ = s.fan_out_queue().drain();

    assert!(
        s.expire(&owned, Duration::from_secs(9))
            .await
            .expect("expire")
    );
    assert_eq!(
        s.rearm_local(&unowned, Lifetime::Unbounded)
            .await
            .expect("rearm"),
        LocalRearm::NotHeld
    );
    assert!(
        !s.persist(&unowned).await.expect("persist"),
        "this shard holds no copy of a key it does not own"
    );

    let base = WireRecord {
        key: key_bytes(&unowned),
        ver: hlc(now_ms(), 7),
        value: Some(Bytes::from(
            postcard::to_stdvec(&"theirs".to_string()).expect("encodes"),
        )),
        expires_at_ms: None,
    };
    let _ = s.fan_out_queue().drain();
    assert!(
        s.forward_rearm(
            unowned,
            base.clone(),
            Lifetime::For(Duration::from_secs(60))
        )
        .await
        .expect("forward")
    );
    let forwarded = s.fan_out_queue().drain();
    assert!(
        matches!(
            forwarded.as_slice(),
            [FanOutItem::Forward(rec)] if rec.ver > base.ver
                && rec.ver.wall_ms == base.ver.wall_ms
                && rec.value == base.value
                && rec.expires_at_ms.is_some()
        ),
        "the owner's record goes back to the owners re-armed: {forwarded:?}"
    );
    assert_eq!(s.get(&unowned).await, None, "and never lands here");

    let tomb = WireRecord {
        value: None,
        ..base
    };
    assert!(
        !s.forward_rearm(unowned, tomb, Lifetime::Unbounded)
            .await
            .expect("forward"),
        "a tombstone has nothing to re-arm"
    );
    let drained = s.fan_out_queue().drain();
    assert!(drained.is_empty(), "{drained:?}");
}

/// `Shard::residency_of` over every residency state a read tells
/// apart: it reports each mark the case set, and its derived `cold`,
/// `distrusted` and `resident` equal what `ShardOps::is_cold_part`,
/// `ShardOps::is_unverified_part` and the `records_for` guard say.
/// `current_residency` follows the tracker's view as it moves.
#[tokio::test]
async fn residency_of_matches_the_single_mark_readers_over_every_residency_state() {
    for case in read_cases::every_read_case() {
        let built = read_cases::case_shard("residency-cases", &case).await;
        let r = built.shard.residency_of(&built.view, built.part);
        assert_eq!(
            (r.owns, r.cold_marked, r.unsettled, r.stale, r.unverified),
            (
                case.owns,
                case.cold_marked,
                case.unsettled,
                case.stale,
                case.unverified
            ),
            "{case:?}"
        );
        assert_eq!(r.releasing_for.is_some(), case.releasing, "{case:?}");
        assert_eq!(
            r.cold(),
            ShardOps::is_cold_part(&built.shard, built.part),
            "{case:?}"
        );
        assert_eq!(
            r.distrusted(),
            ShardOps::is_unverified_part(&built.shard, built.part),
            "{case:?}"
        );
        assert_eq!(
            r.resident(),
            Shard::<u32, String>::is_resident_part(
                &(Arc::clone(&built.view), Arc::clone(&built.residency)),
                built.part
            ),
            "{case:?}"
        );
        let current = built.shard.current_residency(built.part);
        assert_eq!(
            Residency {
                releasing_for: current.releasing_for.map(|_| Duration::ZERO),
                ..current
            },
            Residency {
                releasing_for: r.releasing_for.map(|_| Duration::ZERO),
                ..r
            },
            "the same marks under the tracker's own view, the grace measured later: {case:?}"
        );
    }

    let built = read_cases::case_shard(
        "residency-moves",
        &read_cases::ReadCase {
            owns: false,
            cold_marked: false,
            unsettled: false,
            stale: false,
            unverified: false,
            releasing: false,
            held: read_cases::Held::Nothing,
        },
    )
    .await;
    assert!(!built.shard.current_residency(built.part).owns);
    let solo = Arc::new(OwnershipView::compute(
        NodeId::from(1u64),
        vec![NodeId::from(1u64)],
        NonZeroU8::MIN,
    ));
    built
        .view_tx
        .send(solo)
        .expect("the tracker holds a receiver");
    assert!(
        built.shard.current_residency(built.part).owns,
        "current_residency reads the view the tracker holds now"
    );
}

/// `local_record` under an injected clock: a live entry with its TTL,
/// a tombstone, a key never written, and the live entry once the clock
/// passes its expiry, each with the version and expiry `records_for`
/// sends.
#[tokio::test]
async fn local_record_reports_live_tombstone_lapsed_and_absent() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let fake_now = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&fake_now);
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_millis(100)),
        None,
    )
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)));
    s.insert(1, "a".into()).await.expect("insert");
    s.insert(2, "b".into()).await.expect("insert");
    s.remove(&2).await.expect("remove");

    let records = ShardOps::records_for(&s, vec![key_bytes(&1u32), key_bytes(&2u32)]).await;
    let (live, deleted) = (&records[0], &records[1]);
    assert_eq!(
        s.local_record(&key_bytes(&1u32)),
        (
            1_000,
            LocalRecord::Live {
                version: live.ver,
                expires_at_ms: Some(1_100),
                spilled: false,
            }
        )
    );
    assert_eq!(live.expires_at_ms, Some(1_100));
    assert_eq!(
        s.local_record(&key_bytes(&2u32)).1,
        LocalRecord::Tombstone {
            version: deleted.ver
        }
    );
    assert_eq!(s.local_record(&key_bytes(&3u32)).1, LocalRecord::Absent);

    fake_now.store(1_300, Ordering::SeqCst);
    assert_eq!(
        s.local_record(&key_bytes(&1u32)),
        (
            1_300,
            LocalRecord::Lapsed {
                version: live.ver,
                expires_at_ms: Some(1_100),
                cause: crate::explain::Lapse::Expired,
            }
        ),
        "judged at the shard's clock"
    );
}

/// `local_record` reports what the shard holds whatever its residency:
/// it is not gated on ownership.
#[tokio::test]
async fn local_record_reports_the_held_record_over_every_residency_state() {
    for case in read_cases::every_read_case() {
        let built = read_cases::case_shard("local-record-cases", &case).await;
        let record = built.shard.local_record(&built.key_bytes).1;
        match case.held {
            read_cases::Held::Nothing => assert_eq!(record, LocalRecord::Absent, "{case:?}"),
            read_cases::Held::Live => assert!(record.is_live(), "{case:?}: {record:?}"),
            read_cases::Held::Tombstone => assert!(
                matches!(record, LocalRecord::Tombstone { .. }),
                "{case:?}: {record:?}"
            ),
        }
    }
}

/// `local_record` is not a read: it counts no hit, miss or timed read,
/// and it leaves an entry's `tti` idle clock where the last real access
/// put it. Of two entries written together under a 100ms `tti`, the one
/// inspected at 90ms is idle at 150ms and the one read at 90ms still
/// reads.
#[tokio::test]
async fn local_record_is_not_a_read() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::sync::atomic::{AtomicU64, Ordering};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let fake_now = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&fake_now);
    let s = Shard::<u32, String>::new(
        SmolStr::new("local-record-not-a-read"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        Some(Duration::from_millis(100)),
    )
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)));
    // Hits, misses and timed reads counted since the last call: a
    // snapshot drains the histograms.
    let reads = || {
        let (mut hits, mut misses, mut timed) = (0, 0, 0);
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            match (key.key().name(), value) {
                ("sundog_cache_hits_total", DebugValue::Counter(n)) => hits += n,
                ("sundog_cache_misses_total", DebugValue::Counter(n)) => misses += n,
                ("sundog_read_duration_seconds", DebugValue::Histogram(v)) => {
                    timed += v.len();
                }
                _ => {}
            }
        }
        (hits, misses, timed)
    };
    s.insert(1, "inspected".into()).await.expect("insert");
    s.insert(2, "read".into()).await.expect("insert");
    s.insert(3, "deleted".into()).await.expect("insert");
    s.remove(&3).await.expect("remove");

    fake_now.store(1_090, Ordering::SeqCst);
    let (at_ms, live) = s.local_record(&key_bytes(&1u32));
    assert_eq!(at_ms, 1_090, "judged at the shard's clock");
    assert!(live.is_live(), "{live:?}");
    assert!(matches!(
        s.local_record(&key_bytes(&3u32)).1,
        LocalRecord::Tombstone { .. }
    ));
    assert_eq!(s.local_record(&key_bytes(&9u32)).1, LocalRecord::Absent);
    assert_eq!(reads(), (0, 0, 0), "an inspection counts no read");

    assert_eq!(s.get(&2).await, Some("read".to_string()));
    fake_now.store(1_150, Ordering::SeqCst);
    assert_eq!(
        s.get(&1).await,
        None,
        "idle since its write at 1000: the inspection at 1090 was no access"
    );
    assert_eq!(
        s.get(&2).await,
        Some("read".to_string()),
        "idle only since its read at 1090"
    );
    let (hits, misses, _) = reads();
    assert_eq!(
        (hits, misses),
        (2, 1),
        "the reads count, the inspections did not"
    );
    assert!(
        matches!(
            s.local_record(&key_bytes(&1u32)).1,
            LocalRecord::Lapsed {
                cause: crate::explain::Lapse::Idle,
                ..
            }
        ),
        "the unread entry reports lapsed for idleness"
    );
}

#[test]
fn residency_of_a_shard_without_a_residency_set_reports_ownership_only() {
    let s = shard::<u32, String>(1);
    let part = PartId::of_key(&key_bytes(&1u32));
    assert_eq!(s.current_residency(part), Residency::new(true));
    let other = OwnershipView::compute(
        NodeId::from(2u64),
        vec![NodeId::from(1u64), NodeId::from(2u64)],
        NonZeroU8::MIN,
    );
    assert_eq!(
        s.residency_of(&other, part),
        Residency::new(other.owns(part)),
        "ownership comes from the view passed in"
    );
}

#[test]
fn shard_ownership_view_is_none_without_with_ownership() {
    let s = shard::<u32, String>(1);
    assert!(ShardOps::ownership_view(&s).is_none());
    assert!(ShardOps::ownership_view_hash(&s).is_none());
    assert!(!s.has_residency());
}

#[test]
fn shard_ownership_view_returns_the_current_view_after_with_ownership() {
    let self_node = NodeId::from(1);
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, _tx) = OwnershipTracker::seed(
        self_node,
        &[],
        &std::collections::HashMap::new(),
        &SmolStr::new("test"),
        k,
    );
    let expected_hash = tracker.current().view_hash();
    let residency = Arc::new(ResidencySet::new());

    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Local,
        self_node,
        10_000,
        None,
        None,
    )
    .with_ownership(tracker, residency);

    assert!(s.has_residency());
    let view = ShardOps::ownership_view(&s).expect("ownership view attached");
    assert_eq!(view.view_hash(), expected_hash);
    assert_eq!(
        ShardOps::ownership_view_hash(&s),
        Some(expected_hash),
        "ownership_view_hash mirrors ownership_view's own view_hash"
    );
}
