//! Unit tests for [`Shard`](super::Shard) and the store's pure functions.

use std::collections::HashMap;

use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};
use tokio::sync::watch;

use super::*;

/// A `Mode::Distributed` shard for `self_node`, its ownership view
/// computed directly from `eligible` via `OwnershipView::compute` (no
/// cluster, no `Peer`/`CacheModes` fixture), attached before the shard
/// is ever shared, matching production's `attach_ownership`. Returns
/// the shard's own view alongside for a test to compute expected
/// owned/unowned keys against, and the tracker's `watch::Sender` so a
/// test can publish a further view.
fn distributed_shard<K, V>(
    self_node: NodeId,
    eligible: Vec<NodeId>,
    k: u8,
    residency: Arc<ResidencySet>,
) -> (
    Shard<K, V>,
    Arc<OwnershipView>,
    watch::Sender<Arc<OwnershipView>>,
)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let owners = NonZeroU8::new(k).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        self_node,
        &[],
        &HashMap::new(),
        &SmolStr::new("test"),
        owners,
    );
    let view = Arc::new(OwnershipView::compute(self_node, eligible, owners));
    tx.send(Arc::clone(&view)).expect("receiver still alive");
    let shard = Shard::<K, V>::new(
        SmolStr::new("test"),
        Mode::Distributed { owners },
        self_node,
        u64::MAX,
        None,
        None,
    )
    .with_ownership(tracker, residency);
    (shard, view, tx)
}

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

/// Every part of `bucket`: what a bucket-granularity view owns or
/// releases together.
fn whole(bucket: u16) -> Vec<PartId> {
    PartId::of_bucket(bucket).collect()
}

/// `bucket`'s first part, which a bucket-granularity view owns exactly
/// when it owns the bucket.
fn first_part(bucket: u16) -> PartId {
    PartId::new(bucket, 0)
}

/// The first `u32` key, scanning from `0`, whose part `view.owns`
/// matches `owns`.
fn find_key_by_ownership(view: &OwnershipView, owns: bool) -> u32 {
    (0..1_000_000u32)
        .find(|&key| view.owns(PartId::of_key(&key_bytes(&key))) == owns)
        .expect("a matching key is found within a generous scan range")
}

fn shard<K, V>(node: u64) -> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    Shard::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(node),
        10_000,
        None,
        None,
    )
}

fn hlc(wall_ms: u64, node: u64) -> Hlc {
    Hlc {
        wall_ms,
        logical: 0,
        node: NodeId::from(node),
    }
}

/// `xxh3(key_bytes ‖ postcard(ver))` spelled out, the definition
/// [`entry_fingerprint`] computes.
fn fingerprint_by_definition(key_bytes: &[u8], ver: Hlc) -> u64 {
    let mut joined = key_bytes.to_vec();
    joined.extend_from_slice(&postcard::to_stdvec(&ver).expect("an Hlc encodes"));
    xxh3_64(&joined)
}

#[test]
fn entry_fingerprint_hashes_key_and_version_on_either_side_of_the_stack_buffer() {
    let widest = Hlc {
        wall_ms: u64::MAX,
        logical: u32::MAX,
        node: NodeId::from(u64::MAX),
    };
    let widest_len = postcard::to_stdvec(&widest).expect("an Hlc encodes").len();
    let edge = FINGERPRINT_STACK_BYTES - widest_len;
    for ver in [hlc(0, 0), hlc(1_727_000_000_000, 7), widest] {
        for len in [
            0,
            1,
            16,
            edge - 1,
            edge,
            edge + 1,
            FINGERPRINT_STACK_BYTES,
            300,
        ] {
            let key: Vec<u8> = (0..len)
                .map(|i| u8::try_from(i % 251).expect("below 251"))
                .collect();
            assert_eq!(
                entry_fingerprint(&key, ver),
                fingerprint_by_definition(&key, ver),
                "a {len}-byte key at {ver:?}"
            );
        }
    }
}

/// Nodes compare digests built from these fingerprints, so the value is
/// part of the protocol: pinned against the one release N-1 computes.
#[test]
fn entry_fingerprint_keeps_its_value_across_releases() {
    assert_eq!(
        entry_fingerprint(b"key-000000000042", hlc(1_727_000_000_000, 7)),
        16_473_212_977_212_482_666
    );
}

/// A wall clock one second ahead of the real one: newer than anything a
/// test shard stamps, and well inside the default `max_clock_skew`.
fn ahead_ms() -> u64 {
    now_ms() + 1_000
}

fn key_bytes<K: Serialize>(key: &K) -> Bytes {
    Bytes::from(postcard::to_stdvec(key).expect("test key encodes"))
}

#[test]
fn compacted_version_is_the_stale_versions_successor_keyed_by_content() {
    let stale = hlc(5_000, 1);
    let minted = compacted_version(stale, b"folded");
    assert!(minted > stale);
    assert_eq!((minted.wall_ms, minted.logical), (5_000, stale.logical + 1));
    assert!(minted.node.is_merge_derived());
    assert_eq!(
        compacted_version(stale, b"folded"),
        minted,
        "the same stale version and bytes mint the same version on any replica"
    );
    assert_ne!(
        compacted_version(stale, b"folded differently"),
        minted,
        "different bytes from the same stale version mint different versions"
    );
}

#[test]
fn compacted_version_carries_wall_ms_into_logical_zero_on_overflow() {
    let stale = Hlc {
        wall_ms: 9_000,
        logical: u32::MAX,
        node: NodeId::from(1),
    };
    let minted = compacted_version(stale, b"x");
    assert_eq!((minted.wall_ms, minted.logical), (9_001, 0));
}

/// Unwraps every item as [`FanOutItem::Applied`], panicking on a
/// [`FanOutItem::Forward`]: every test that calls this drains an
/// owner's or a non-`Distributed` shard's queue, which never forwards.
fn applied_keys<K: std::fmt::Debug>(items: Vec<FanOutItem<K>>) -> Vec<K> {
    items
        .into_iter()
        .map(|item| match item {
            FanOutItem::Applied(key) => key,
            FanOutItem::Forward(rec) => {
                panic!("expected only FanOutItem::Applied, found a forwarded record: {rec:?}")
            }
        })
        .collect()
}

#[test]
fn mode_is_copy_and_comparable() {
    assert_eq!(Mode::Local, Mode::Local);
    assert_ne!(Mode::Local, Mode::Replicated);
}

#[test]
fn mode_distributed_defaults_to_two_owners_and_round_trips_its_token() {
    assert_eq!(Mode::DEFAULT_OWNERS.get(), 2);
    assert_eq!(
        Mode::distributed(),
        Mode::Distributed {
            owners: Mode::DEFAULT_OWNERS
        }
    );
    assert_eq!(Mode::distributed().as_token(), "distributed:2");
    assert_eq!(
        Mode::from_token(&Mode::distributed().as_token()),
        Some(Mode::distributed())
    );
}

#[test]
fn mode_as_token_and_from_token_round_trip_every_variant() {
    let distributed = Mode::Distributed {
        owners: NonZeroU8::new(3).expect("nonzero"),
    };
    for mode in [
        Mode::Local,
        Mode::Invalidation,
        Mode::Replicated,
        distributed,
    ] {
        assert_eq!(Mode::from_token(&mode.as_token()), Some(mode));
    }
}

#[test]
fn mode_as_token_distributed_carries_the_owners_count() {
    let mode = Mode::Distributed {
        owners: NonZeroU8::new(5).expect("nonzero"),
    };
    assert_eq!(mode.as_token().as_str(), "distributed:5");
}

#[test]
fn mode_from_token_rejects_zero_owners() {
    assert_eq!(Mode::from_token("distributed:0"), None);
}

#[test]
fn mode_from_token_rejects_a_non_numeric_owners_suffix() {
    assert_eq!(Mode::from_token("distributed:abc"), None);
}

#[test]
fn mode_from_token_rejects_a_missing_owners_suffix() {
    assert_eq!(Mode::from_token("distributed:"), None);
    assert_eq!(Mode::from_token("distributed"), None);
}

#[test]
fn mode_from_token_rejects_an_owners_count_past_u8() {
    assert_eq!(Mode::from_token("distributed:256"), None);
}

#[test]
fn mode_from_token_rejects_an_unrecognized_token() {
    assert_eq!(Mode::from_token("bogus"), None);
}

#[test]
fn origin_distinguishes_local_from_remote() {
    assert_ne!(Origin::Local, Origin::Remote(NodeId::from(1)));
}

#[tokio::test]
async fn newer_remote_tombstone_beats_older_local_put() {
    let s = shard::<u32, String>(1);
    s.insert(1, "a".into()).await.expect("insert");
    assert_eq!(s.get(&1).await, Some("a".into()));

    let rec = WireRecord {
        key: key_bytes(&1u32),
        value: None,
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, rec).await;
    assert_eq!(s.get(&1).await, None);
}

#[tokio::test]
async fn newer_remote_put_beats_older_local_tombstone() {
    let s = shard::<u32, String>(1);
    s.insert(1, "a".into()).await.expect("insert");
    s.remove(&1).await.expect("remove");
    assert_eq!(s.get(&1).await, None);

    let rec = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from_static(b"\x01b")),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, rec).await;
    assert_eq!(s.get(&1).await, Some("b".into()));
}

#[tokio::test]
async fn stale_remote_writes_are_rejected_idempotently() {
    let s = shard::<u32, String>(1);
    let winner = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from_static(b"\x01x")),
        ver: hlc(1_000_000, 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, winner.clone()).await;
    assert_eq!(s.get(&1).await, Some("x".into()));

    let stale = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from_static(b"\x01y")),
        ver: hlc(1, 3),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, stale).await;
    assert_eq!(
        s.get(&1).await,
        Some("x".into()),
        "an older write does not overwrite a newer one"
    );

    // Re-applying the exact same winning record is also a no-op.
    ShardOps::apply_remote(&s, winner).await;
    assert_eq!(s.get(&1).await, Some("x".into()));
}

/// A shard on a fixed clock at `NOW_MS` with a one-minute skew bound.
fn skew_bounded_shard() -> Shard<u32, String> {
    const NOW_MS: u64 = 1_700_000_000_000;
    shard::<u32, String>(1)
        .with_clock(Arc::new(|| NOW_MS))
        .with_max_clock_skew(Some(Duration::from_mins(1)))
}

/// The version `shard` holds for `key`, read from its bucket listing.
async fn held_version(shard: &Shard<u32, String>, key: u32) -> Option<Hlc> {
    let key_bytes = key_bytes(&key);
    ShardOps::bucket_entries(shard, bucket_of(&key_bytes))
        .await
        .into_iter()
        .find(|entry| entry.key == key_bytes)
        .map(|entry| entry.version)
}

fn put(key: u32, value: &str, ver: Hlc) -> WireRecord {
    WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("value encodes"),
        )),
        ver,
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn a_remote_record_stamped_past_max_clock_skew_is_refused_and_moves_no_clock() {
    let s = skew_bounded_shard();
    let now = s.now_ms();
    let within = now + 60_000;
    let past = now + 60_001;
    ShardOps::apply_remote_batch(
        &s,
        vec![
            put(1, "within", hlc(within, 2)),
            put(2, "past", hlc(past, 2)),
        ],
    )
    .await;
    assert_eq!(s.get(&1).await.as_deref(), Some("within"));
    assert_eq!(s.get(&2).await, None, "a stamp past the bound is refused");

    s.insert(3, "local".into()).await.expect("insert");
    let local = held_version(&s, 3).await.expect("the local write is held");
    assert_eq!(
        local.wall_ms, within,
        "the clock took the accepted stamp and never the refused one"
    );

    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(past, 9)).await;
    assert_eq!(
        s.get(&1).await.as_deref(),
        Some("within"),
        "an invalidation stamped past the bound is refused too"
    );
    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(within, 9)).await;
    assert_eq!(s.get(&1).await, None);
}

#[tokio::test]
async fn a_refused_record_applies_once_the_clock_comes_within_the_bound() {
    let now = Arc::new(std::sync::atomic::AtomicU64::new(1_700_000_000_000));
    let clock = Arc::clone(&now);
    let s = shard::<u32, String>(1)
        .with_clock(Arc::new(move || clock.load(Ordering::Relaxed)))
        .with_max_clock_skew(Some(Duration::from_mins(1)));
    let stamp = hlc(now.load(Ordering::Relaxed) + 3_600_000, 2);
    ShardOps::apply_remote(&s, put(1, "early", stamp)).await;
    assert_eq!(s.get(&1).await, None);

    now.fetch_add(3_600_000 - 60_000, Ordering::Relaxed);
    ShardOps::apply_remote(&s, put(1, "early", stamp)).await;
    assert_eq!(s.get(&1).await.as_deref(), Some("early"));
}

#[tokio::test]
async fn no_max_clock_skew_accepts_a_stamp_any_distance_ahead() {
    let s = skew_bounded_shard().with_max_clock_skew(None);
    let far = hlc(s.now_ms() + 365 * 24 * 3_600_000, 2);
    ShardOps::apply_remote(&s, put(1, "far", far)).await;
    assert_eq!(s.get(&1).await.as_deref(), Some("far"));
    s.insert(2, "local".into()).await.expect("insert");
    assert!(
        held_version(&s, 2).await.expect("held") > far,
        "an unbounded shard's clock follows the far stamp"
    );
}

#[test]
fn clock_jumped_flags_a_lead_past_the_bound_only() {
    assert!(!clock_jumped(60_000, Some(60_000)));
    assert!(clock_jumped(60_001, Some(60_000)));
    assert!(!clock_jumped(0, Some(0)));
    assert!(!clock_jumped(u64::MAX, None), "no bound never flags");
}

#[test]
fn with_cluster_config_takes_the_configured_max_clock_skew() {
    let config = ClusterConfig::default().with(|c| c.max_clock_skew = Some(Duration::from_secs(5)));
    assert_eq!(
        shard::<u32, String>(1)
            .with_cluster_config(&config)
            .max_clock_skew_ms,
        Some(5_000)
    );
    let config = ClusterConfig::default().with(|c| c.max_clock_skew = None);
    assert_eq!(
        shard::<u32, String>(1)
            .with_cluster_config(&config)
            .max_clock_skew_ms,
        None
    );
}

#[tokio::test]
async fn invalidate_respects_newer_local_write() {
    let s = shard::<u32, String>(1);
    s.insert(1, "fresh".into()).await.expect("insert");

    // An invalidation for an old version does not evict a newer local
    // write.
    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(1, 9)).await;
    assert_eq!(s.get(&1).await, Some("fresh".into()));

    // A newer invalidation does evict it, and writes no tombstone.
    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(ahead_ms(), 9)).await;
    assert_eq!(s.get(&1).await, None);
    let records = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert!(records.is_empty(), "{records:?}");
}

#[tokio::test]
async fn insert_update_remove_emit_matching_events() {
    let s = shard::<u32, String>(1);
    let mut events = s.events();

    s.insert(1, "a".into()).await.expect("insert");
    match events.recv().await.expect("created") {
        Event::Created {
            key: 1,
            value,
            origin: Origin::Local,
        } => assert_eq!(value, "a"),
        other => panic!("unexpected event: {other:?}"),
    }

    s.insert(1, "b".into()).await.expect("insert");
    match events.recv().await.expect("updated") {
        Event::Updated {
            key: 1,
            value,
            origin: Origin::Local,
        } => assert_eq!(value, "b"),
        other => panic!("unexpected event: {other:?}"),
    }

    s.remove(&1).await.expect("remove");
    match events.recv().await.expect("removed") {
        Event::Removed {
            key: 1,
            origin: Origin::Local,
        } => {}
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn fill_fans_out_everywhere_but_invalidation() {
    assert!(fill_fans_out(Mode::Replicated));
    assert!(fill_fans_out(Mode::distributed()));
    assert!(
        fill_fans_out(Mode::Local),
        "the fan-out task sends nothing for Local"
    );
    assert!(!fill_fans_out(Mode::Invalidation));
}

#[tokio::test]
async fn an_invalidation_fill_queues_no_fan_out_and_a_replicated_fill_does() {
    let invalidation = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Invalidation,
        NodeId::from(1),
        10_000,
        None,
        None,
    );
    invalidation
        .get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                Ok("loaded".to_string())
            },
        )
        .await
        .expect("load succeeds");
    assert!(
        invalidation.fan_out.drain().is_empty(),
        "an Invalidation fill sends no invalidation to peers"
    );

    let replicated = shard::<u32, String>(1);
    replicated
        .get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                Ok("loaded".to_string())
            },
        )
        .await
        .expect("load succeeds");
    assert_eq!(applied_keys(replicated.fan_out.drain()), vec![7]);
}

#[tokio::test]
async fn get_or_load_keeps_a_remove_that_lands_while_the_loader_runs() {
    let s = shard::<u32, String>(1);
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let load = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let race = async {
        started.notified().await;
        s.remove(&7).await.expect("remove");
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(
        loaded.expect("load succeeds"),
        "stale",
        "the read that raced the removal answers with what it loaded"
    );
    assert_eq!(s.get(&7).await, None, "the removal stays removed");
    let _ = s.fan_out.drain();
    assert_eq!(
        s.get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                Ok("reloaded".to_string())
            }
        )
        .await
        .expect("load succeeds"),
        "reloaded",
        "the next read loads afresh over the removal"
    );
}

#[tokio::test]
async fn get_or_load_keeps_an_insert_that_lands_while_the_loader_runs() {
    let s = shard::<u32, String>(1);
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let load = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let race = async {
        started.notified().await;
        s.insert(7, "fresh".to_string()).await.expect("insert");
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(loaded.expect("load succeeds"), "stale");
    assert_eq!(
        s.get(&7).await,
        Some("fresh".to_string()),
        "the write that landed during the load keeps its place"
    );
    assert_eq!(
        applied_keys(s.fan_out.drain()),
        vec![7],
        "only the insert fans out; the superseded fill sends nothing"
    );
}

#[tokio::test]
async fn get_or_load_drops_a_fill_an_invalidation_lands_during() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Invalidation,
        NodeId::from(1),
        10_000,
        None,
        None,
    );
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let load = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let race = async {
        started.notified().await;
        ShardOps::invalidate(&s, key_bytes(&7u32), hlc(now_ms(), 2)).await;
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(
        loaded.expect("load succeeds"),
        "stale",
        "the read that raced the invalidation answers with what it loaded"
    );
    assert_eq!(
        s.get(&7).await,
        None,
        "a value read before the key changed elsewhere is not cached"
    );
    assert_eq!(
        s.get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                Ok("fresh".to_string())
            }
        )
        .await
        .expect("load succeeds"),
        "fresh",
        "the next read loads afresh"
    );
    assert_eq!(s.get(&7).await, Some("fresh".to_string()));
}

#[tokio::test]
async fn get_or_load_drops_a_fill_a_local_invalidation_lands_during() {
    let s = shard::<u32, String>(1);
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let load = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let race = async {
        started.notified().await;
        s.invalidate_local(&7).await;
        release.notify_one();
    };
    let (loaded, ()) = tokio::join!(load, race);
    assert_eq!(loaded.expect("load succeeds"), "stale");
    assert_eq!(s.get(&7).await, None, "the busted key stays uncached");
}

#[tokio::test]
async fn get_or_load_waiters_share_a_dropped_fill_without_loading_again() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Invalidation,
        NodeId::from(1),
        10_000,
        None,
        None,
    );
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let started = tokio::sync::Notify::new();
    let release = tokio::sync::Notify::new();
    let first = s.get_or_load(
        &7,
        async |_key: &u32| -> Result<String, std::convert::Infallible> {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            started.notify_one();
            release.notified().await;
            Ok("stale".to_string())
        },
    );
    let rest = async {
        started.notified().await;
        let joined = s.get_or_load(
            &7,
            async |_key: &u32| -> Result<String, std::convert::Infallible> {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok("second".to_string())
            },
        );
        let race = async {
            tokio::task::yield_now().await;
            ShardOps::invalidate(&s, key_bytes(&7u32), hlc(now_ms(), 2)).await;
            release.notify_one();
        };
        let (joined, ()) = tokio::join!(joined, race);
        joined
    };
    let (first, joined) = tokio::join!(first, rest);
    assert_eq!(first.expect("load succeeds"), "stale");
    assert_eq!(joined.expect("load succeeds"), "stale");
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the joined read answers with the dropped fill's value"
    );
    assert_eq!(s.get(&7).await, None);
}

#[tokio::test]
async fn get_or_load_fills_once_and_emits_created() {
    let s = shard::<u32, String>(1);
    let mut events = s.events();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let c = std::sync::Arc::clone(&calls);
    let loaded = s
        .get_or_load(
            &7,
            async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok("loaded".to_string())
            },
        )
        .await
        .expect("load succeeds");
    assert_eq!(loaded, "loaded");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    match events.recv().await.expect("created") {
        Event::Created {
            key: 7,
            value,
            origin: Origin::Local,
        } => assert_eq!(value, "loaded"),
        other => panic!("unexpected event: {other:?}"),
    }

    // A hit does not call the loader again.
    let c2 = std::sync::Arc::clone(&calls);
    let hit = s
        .get_or_load(
            &7,
            async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok("should-not-run".to_string())
            },
        )
        .await
        .expect("hit succeeds");
    assert_eq!(hit, "loaded");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[derive(Debug, thiserror::Error)]
#[error("loader boom")]
struct BoomError;

/// A `Mode::Local` shard capped at 100 unit-weight entries, filled to
/// the cap with keys `0..100`.
async fn full_local_shard() -> Shard<u32, String> {
    let s = Shard::<u32, String>::new(
        SmolStr::new("full"),
        Mode::Local,
        NodeId::from(1u64),
        100,
        None,
        None,
    );
    s.insert_many((0..100u32).map(|k| (k, k.to_string())))
        .await
        .expect("fills to the cap");
    assert_eq!(s.entry_count().await, 100);
    s
}

/// With more stripes than entries, most writes land in a stripe of their
/// own, where the eviction they trigger starts: each one still reads back
/// at once, and an older entry makes the room.
#[tokio::test]
async fn every_write_past_max_capacity_reads_back_at_once() {
    let s = full_local_shard().await;
    for k in 100..300u32 {
        s.insert(k, k.to_string()).await.expect("insert");
        assert_eq!(s.get(&k).await, Some(k.to_string()), "key {k}");
        assert_eq!(s.entry_count().await, 100, "key {k}: the cap holds");
    }
}

/// A batch spread over many stripes keeps its own entries over older
/// ones, though each stripe's group enforces the cap on its own: a batch
/// within the cap stays whole, and one past it leaves no older entry.
#[tokio::test]
async fn a_batch_past_max_capacity_keeps_its_own_entries() {
    let s = full_local_shard().await;
    s.insert_many((1_000..1_060u32).map(|k| (k, k.to_string())))
        .await
        .expect("insert_many");
    for k in 1_000..1_060u32 {
        assert_eq!(s.get(&k).await, Some(k.to_string()), "key {k}");
    }
    assert_eq!(s.entry_count().await, 100);

    s.insert_many((2_000..2_150u32).map(|k| (k, k.to_string())))
        .await
        .expect("insert_many");
    assert_eq!(s.entry_count().await, 100, "the cap holds");
    for k in (0..100u32).chain(1_000..1_060) {
        assert_eq!(s.get(&k).await, None, "older key {k} went first");
    }
}

/// A peer's batch keeps its own entries over older ones across its
/// stripe groups: the later group's scan wraps past the earlier group's
/// entry to an older one.
#[tokio::test]
async fn a_remote_batch_past_max_capacity_keeps_its_own_entries() {
    let stripe = |k: u32| engine::stripe_index_from_hash(engine::hash_key_bytes(&key_bytes(&k)));
    let first = (0..100_000u32)
        .find(|&k| stripe(k) < 300)
        .expect("a low stripe");
    let last = (0..100_000u32)
        .find(|&k| stripe(k) > 700)
        .expect("a high stripe");
    let older: Vec<u32> = (0u32..)
        .filter(|&k| (300..=700).contains(&stripe(k)))
        .take(3)
        .collect();
    let s = Shard::<u32, String>::new(
        SmolStr::new("remote-batch"),
        Mode::Replicated,
        NodeId::from(1u64),
        3,
        None,
        None,
    );
    for &k in &older {
        s.insert(k, k.to_string()).await.expect("insert");
    }

    ShardOps::apply_remote_batch(
        &s,
        vec![
            wire_record(last, "last", hlc(10, 2)),
            wire_record(first, "first", hlc(10, 2)),
        ],
    )
    .await;

    assert_eq!(s.entry_count().await, 3, "the cap holds");
    assert_eq!(s.get(&first).await.as_deref(), Some("first"));
    assert_eq!(s.get(&last).await.as_deref(), Some("last"));
}

/// A write heavier than the whole cap never fits, so its eviction
/// samples it like any other entry: alone in its stripe, it goes and
/// the older entry stays.
#[tokio::test]
async fn a_write_heavier_than_max_capacity_goes_and_the_older_entry_stays() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("heavy"),
        Mode::Local,
        NodeId::from(1u64),
        5,
        None,
        None,
    )
    .with_weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let stripe = |k: u32| engine::stripe_index_from_hash(engine::hash_key_bytes(&key_bytes(&k)));
    let heavy = (2..100_000u32)
        .find(|&k| stripe(k) != stripe(1))
        .expect("another stripe");
    s.insert(1, "x".to_string()).await.expect("insert");

    s.insert(heavy, "x".repeat(10)).await.expect("insert");

    assert_eq!(s.get(&heavy).await, None, "the heavy write goes");
    assert_eq!(
        s.get(&1).await.as_deref(),
        Some("x"),
        "the older entry stays"
    );
    assert_eq!(s.entry_count().await, 1);
}

/// A loaded value is not evicted by the fill that stores it.
#[tokio::test]
async fn get_or_load_past_max_capacity_keeps_the_loaded_value() {
    let s = full_local_shard().await;
    for k in 100..200u32 {
        let loaded = s
            .get_or_load(&k, async move |key: &u32| -> Result<String, BoomError> {
                Ok(key.to_string())
            })
            .await
            .expect("loads");
        assert_eq!(loaded, k.to_string());
        assert_eq!(s.get(&k).await, Some(k.to_string()), "key {k}");
    }
    assert_eq!(s.entry_count().await, 100);
}

#[tokio::test]
async fn get_or_load_propagates_loader_error() {
    let s = shard::<u32, String>(1);
    let err = s
        .get_or_load(&1, async move |_key: &u32| -> Result<String, BoomError> {
            Err(BoomError)
        })
        .await
        .expect_err("loader failed");
    assert!(matches!(err, CacheError::Loader(_)));
}

#[tokio::test]
async fn get_or_load_stampede_collapses_to_one_loader_call() {
    const CONCURRENCY: usize = 64;
    let s = Arc::new(shard::<u32, String>(1));
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        let s = Arc::clone(&s);
        let calls = std::sync::Arc::clone(&calls);
        tasks.spawn(async move {
            s.get_or_load(&42, async move |_key: &u32| -> Result<String, BoomError> {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Widens the race window so every caller waits on this one
                // load.
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok("loaded-once".to_string())
            })
            .await
            .expect("loader succeeds")
        });
    }

    let mut results = Vec::with_capacity(CONCURRENCY);
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("stampede caller does not panic"));
    }

    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the loader runs exactly once under a stampede of {CONCURRENCY} concurrent misses"
    );
    assert!(results.iter().all(|value| value == "loaded-once"));
    assert_eq!(results.len(), CONCURRENCY);
}

#[tokio::test]
async fn get_or_load_stampede_with_a_failing_loader_shares_one_error_with_every_waiter() {
    const CONCURRENCY: usize = 8;
    let s = Arc::new(shard::<u32, String>(1));
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        let s = Arc::clone(&s);
        let calls = std::sync::Arc::clone(&calls);
        tasks.spawn(async move {
            s.get_or_load(&99, async move |_key: &u32| -> Result<String, BoomError> {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Widens the race window so every caller joins this one
                // load before it fails.
                tokio::time::sleep(Duration::from_millis(30)).await;
                Err(BoomError)
            })
            .await
        });
    }

    let mut results = Vec::with_capacity(CONCURRENCY);
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("stampede caller does not panic"));
    }

    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the loader runs exactly once under a stampede of {CONCURRENCY} concurrent misses"
    );
    assert_eq!(results.len(), CONCURRENCY);
    assert!(
        results
            .iter()
            .all(|r| matches!(r, Err(CacheError::Loader(_)))),
        "every joined waiter returns the same Loader failure"
    );
}

/// Deterministically lands a write in the narrow gap between
/// `get_or_load`'s fast-path miss and `Engine::miss_or_join`'s locked
/// re-check, by giving the shard a clock whose second call (the one
/// feeding `miss_or_join`) performs the write itself before returning a
/// timestamp. No real concurrency, and no flakiness: the write always
/// lands on the second clock read, deterministically.
#[tokio::test]
async fn get_or_load_hits_when_a_write_lands_between_the_fast_path_miss_and_the_locked_recheck() {
    let s0 = shard::<u32, String>(1);
    let engine_for_clock = Arc::clone(&s0.engine);
    let resolver_for_clock = Arc::clone(&s0.resolver);
    let tombstone_ttl_ms = s0.tombstone_ttl_ms;
    let tombstone_max_ttl_ms = s0.tombstone_max_ttl_ms;

    let call_idx = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let call_idx_for_clock = Arc::clone(&call_idx);
    let clock: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(move || {
        let n = call_idx_for_clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n == 1 {
            let kb = key_bytes(&7u32);
            let hash = engine::hash_key_bytes(kb.as_ref());
            let bucket = engine::stripe_index_from_hash(hash);
            let value = "landed".to_string();
            let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
            let _ = engine_for_clock.apply_many(
                bucket,
                vec![(
                    hash,
                    7u32,
                    kb,
                    hlc(1, 9),
                    Incoming::Put {
                        value,
                        expires_at_ms: None,
                        encoded,
                    },
                )],
                resolver_for_clock.as_ref(),
                tombstone_ttl_ms,
                tombstone_max_ttl_ms,
                0,
            );
        }
        0
    });
    let s = s0.with_clock(clock);

    let loaded = s
        .get_or_load(&7, async move |_key: &u32| -> Result<String, BoomError> {
            panic!(
                "the value already landed before this call reaches the loader; \
                 get_or_load must resolve via the locked re-check's Hit instead"
            );
        })
        .await
        .expect("resolves via the Hit path, never reaching the loader");
    assert_eq!(loaded, "landed");
    assert_eq!(
        call_idx.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "exactly two clock reads: the fast-path miss, then the locked re-check that hits"
    );
}

#[tokio::test]
async fn value_too_large_is_rejected() {
    let s = shard::<u32, Vec<u8>>(1);
    let big = vec![0u8; MAX_FRAME + 1];
    let err = s
        .insert(1, big)
        .await
        .expect_err("oversized value rejected");
    assert!(matches!(err, CacheError::ValueTooLarge { .. }));
}

/// A Replicated shard whose memory ceiling is one byte: empty, it takes
/// one write, then refuses every local write after it.
fn one_byte_ceiling_shard() -> Shard<u32, String> {
    shard::<u32, String>(1).with_max_resident_bytes(1)
}

#[tokio::test]
async fn resident_bytes_rises_on_insert_and_falls_to_zero_on_remove() {
    let s = shard::<u32, String>(1).with_max_resident_bytes(1 << 40);
    assert_eq!(s.resident_bytes(), Some(0));
    s.insert(1, "v".repeat(100)).await.expect("insert");
    let one = s.resident_bytes().expect("a capped shard counts");
    assert!(
        one > 100,
        "an entry counts at least its value bytes, got {one}"
    );
    s.insert(2, "v".repeat(100)).await.expect("insert");
    assert_eq!(
        s.resident_bytes(),
        Some(2 * one),
        "a same-shaped entry costs the same"
    );
    s.remove(&1).await.expect("remove");
    s.remove(&2).await.expect("remove");
    assert_eq!(s.resident_bytes(), Some(0));
}

#[test]
fn engine_rebuilds_carry_the_prefold_flag_and_the_ceiling_forward() {
    let s = shard::<u32, String>(1)
        .with_prefold_enabled(false)
        .with_max_resident_bytes(1 << 40)
        .with_capacity_hint(64)
        .with_weigher(|_k: &u32, _v: &String| 1)
        .with_max_resident_bytes(1 << 30);
    assert!(!s.prefold_enabled(), "every rebuild keeps pre-fold off");
    assert_eq!(
        s.resident_bytes(),
        Some(0),
        "every rebuild keeps counting on"
    );
    let uncapped = shard::<u32, String>(1)
        .with_prefold_enabled(false)
        .with_max_resident_bytes(u64::MAX);
    assert!(
        !uncapped.prefold_enabled(),
        "no ceiling leaves the engine as it was"
    );
}

#[tokio::test]
async fn an_uncapped_shard_counts_no_bytes_and_refuses_nothing() {
    let s = shard::<u32, String>(1);
    s.insert(1, "v".repeat(100)).await.expect("insert");
    assert_eq!(s.resident_bytes(), None);
    assert_eq!(s.engine.resident_bytes(), None);
    let capped_then_lifted = shard::<u32, String>(1)
        .with_max_resident_bytes(1)
        .with_max_resident_bytes(u64::MAX);
    capped_then_lifted
        .insert_many((0..8).map(|k| (k, "v".to_string())))
        .await
        .expect("u64::MAX removes the ceiling");
    assert_eq!(capped_then_lifted.resident_bytes(), None);
}

#[tokio::test]
async fn insert_at_the_memory_ceiling_is_refused_until_a_remove_frees_room() {
    let s = one_byte_ceiling_shard();
    s.insert(1, "a".to_string())
        .await
        .expect("an empty shard is under its ceiling");
    let resident_bytes = s.resident_bytes();
    let err = s
        .insert(2, "b".to_string())
        .await
        .expect_err("the shard is at its ceiling");
    assert!(
        matches!(
            &err,
            CacheError::OverMemoryCeiling { cache, resident_bytes: found, ceiling: 1 }
                if cache == "test" && Some(*found) == resident_bytes
        ),
        "got {err:?}"
    );
    assert!(matches!(
        s.insert_sync(2, "b".to_string()),
        Err(CacheError::OverMemoryCeiling { .. })
    ));
    assert!(matches!(
        s.insert_with_ttl(2, "b".to_string(), Duration::from_secs(60))
            .await,
        Err(CacheError::OverMemoryCeiling { .. })
    ));
    assert_eq!(s.get(&2).await, None, "no refused write landed");

    s.remove(&1).await.expect("a removal is never refused");
    s.insert(2, "b".to_string())
        .await
        .expect("the removal brought the shard back under its ceiling");
    assert_eq!(s.get(&2).await.as_deref(), Some("b"));
}

#[tokio::test]
async fn merge_and_insert_many_at_the_memory_ceiling_apply_nothing() {
    let s = one_byte_ceiling_shard();
    s.insert_many([(1, "a".to_string()), (2, "b".to_string())])
        .await
        .expect("a batch that starts under the ceiling applies whole");
    assert_eq!(s.entry_count().await, 2);
    assert!(matches!(
        s.insert_many([(3, "c".to_string()), (4, "d".to_string())])
            .await,
        Err(CacheError::OverMemoryCeiling { .. })
    ));
    assert!(matches!(
        s.insert_many_with_ttl([(3, "c".to_string())], Duration::from_secs(60))
            .await,
        Err(CacheError::OverMemoryCeiling { .. })
    ));
    assert!(matches!(
        s.merge(5, "e".to_string()).await,
        Err(CacheError::OverMemoryCeiling { .. })
    ));
    assert_eq!(s.entry_count().await, 2, "no refused entry landed");
}

#[tokio::test]
async fn writes_from_peers_apply_past_the_memory_ceiling() {
    let s = one_byte_ceiling_shard();
    s.insert(1, "a".to_string()).await.expect("insert");
    let record = |key: u32| WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from(postcard::to_stdvec("peer").expect("encode"))),
        ver: hlc(10, 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote_batch(&s, vec![record(2), record(3)]).await;
    ShardOps::apply_remote(&s, record(4)).await;
    for key in 2..=4 {
        assert_eq!(
            s.get(&key).await.as_deref(),
            Some("peer"),
            "key {key}: a replica never refuses what a peer applied"
        );
    }
    assert_eq!(s.resident_bytes(), Some(s.engine.recount_resident_bytes()));
}

#[tokio::test]
async fn get_or_load_at_the_memory_ceiling_returns_the_value_uncached() {
    let s = one_byte_ceiling_shard();
    s.insert(1, "a".to_string()).await.expect("insert");
    let before = s.resident_bytes();
    for round in 0..2 {
        let value = s
            .get_or_load(&2, async |_key: &u32| -> Result<String, BoomError> {
                Ok("loaded".to_string())
            })
            .await
            .expect("the loader succeeds");
        assert_eq!(value, "loaded", "round {round}");
        assert_eq!(
            s.get(&2).await,
            None,
            "round {round}: the fill is not stored"
        );
    }
    assert_eq!(s.resident_bytes(), before);
    let made = s
        .get_or_insert_with(&3, async |_key: &u32| "made".to_string())
        .await
        .expect("an infallible fill");
    assert_eq!(made, "made");
    assert_eq!(s.get(&3).await, None);
}

#[tokio::test]
async fn get_or_load_stampede_at_the_memory_ceiling_shares_one_uncached_value() {
    const CONCURRENCY: usize = 32;
    let s = Arc::new(one_byte_ceiling_shard());
    s.insert(1, "a".to_string()).await.expect("insert");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENCY {
        let s = Arc::clone(&s);
        let calls = Arc::clone(&calls);
        tasks.spawn(async move {
            s.get_or_load(&42, async move |_key: &u32| -> Result<String, BoomError> {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok("loaded-once".to_string())
            })
            .await
            .expect("loader succeeds")
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.expect("caller does not panic"), "loaded-once");
    }
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "every joined waiter takes the owner's uncached value instead of loading again"
    );
    assert_eq!(s.get(&42).await, None);
}

#[tokio::test]
async fn insert_many_applies_entries_before_an_oversized_one_then_fails() {
    let s = shard::<u32, Vec<u8>>(1);
    let ok_value = vec![0u8; 4];
    let big = vec![0u8; MAX_FRAME + 1];
    let err = s
        .insert_many([(1u32, ok_value.clone()), (2u32, big)])
        .await
        .expect_err("the oversized second entry is rejected");
    assert!(matches!(err, CacheError::ValueTooLarge { .. }));
    assert_eq!(
        s.get(&1).await,
        Some(ok_value),
        "insert_many is not a transaction: the key before the oversized one already applied"
    );
}

#[tokio::test]
async fn long_key_round_trips_through_the_heap_encoding_path() {
    // Longer than engine::KEY_STACK_BUF (128 bytes) once postcard-encoded,
    // so both the write and the read fall back to a heap allocation.
    let s = shard::<String, String>(1);
    let key = "k".repeat(200);
    s.insert(key.clone(), "v".into()).await.expect("insert");
    assert_eq!(s.get(&key).await, Some("v".to_string()));
    assert!(s.contains_key(&key).await);
}

#[tokio::test]
async fn snapshot_chunks_covers_all_live_entries() {
    let s = shard::<u32, String>(1);
    for i in 0..5u32 {
        s.insert(i, i.to_string()).await.expect("insert");
    }
    let mut stream = ShardOps::snapshot_chunks(&s);
    let mut total = 0usize;
    while let Some(chunk) = stream.next().await {
        total += chunk.len();
    }
    assert_eq!(total, 5);
}

#[tokio::test]
async fn snapshot_chunks_splits_by_byte_size_not_just_record_count() {
    let s = shard::<u32, Vec<u8>>(1);
    let value_size = 100_000;
    let record_count = 50u32;
    for i in 0..record_count {
        s.insert(i, vec![0u8; value_size]).await.expect("insert");
    }

    let mut stream = ShardOps::snapshot_chunks(&s);
    let mut chunk_count = 0usize;
    let mut total_records = 0usize;
    while let Some(chunk) = stream.next().await {
        chunk_count += 1;
        total_records += chunk.len();
        let encoded_size: usize = chunk
            .iter()
            .map(|rec| postcard::to_stdvec(rec).expect("test record encodes").len())
            .sum();
        assert!(
            encoded_size < MAX_FRAME,
            "one snapshot chunk, wrapped in a Msg::StChunk, stays under the wire frame cap"
        );
    }
    assert_eq!(total_records, record_count as usize);
    assert!(
        chunk_count > 1,
        "50 * 100KB records (5MB) does not fit in one wire-frame-bounded chunk"
    );
}

#[tokio::test]
async fn roundtrip_through_shard_ops_converges_two_shards() {
    let a = shard::<u32, String>(1);
    let b = shard::<u32, String>(2);
    a.insert(42, "hello".into()).await.expect("insert");

    let recs = ShardOps::records_for(&a, vec![key_bytes(&42u32)]).await;
    assert_eq!(recs.len(), 1);
    ShardOps::apply_remote(&b, recs[0].clone()).await;
    assert_eq!(b.get(&42).await, Some("hello".into()));

    a.remove(&42).await.expect("remove");
    let recs = ShardOps::records_for(&a, vec![key_bytes(&42u32)]).await;
    assert_eq!(recs.len(), 1);
    assert!(recs[0].is_tombstone());
    ShardOps::apply_remote(&b, recs[0].clone()).await;
    assert_eq!(b.get(&42).await, None);
}

#[tokio::test]
async fn apply_remote_batch_skips_undecodable_records_and_applies_the_rest() {
    let s = shard::<u32, String>(1);
    // Postcard decoding of any non-unit type fails on an empty slice: no
    // length-prefix or varint byte to even start on.
    let undecodable_key = WireRecord {
        key: Bytes::new(),
        value: Some(Bytes::from(postcard::to_stdvec("ignored").expect("encode"))),
        ver: hlc(10, 2),
        expires_at_ms: None,
    };
    let undecodable_value = WireRecord {
        key: key_bytes(&2u32),
        value: Some(Bytes::new()),
        ver: hlc(10, 2),
        expires_at_ms: None,
    };
    let valid = WireRecord {
        key: key_bytes(&3u32),
        value: Some(Bytes::from(postcard::to_stdvec("ok").expect("encode"))),
        ver: hlc(10, 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote_batch(&s, vec![undecodable_key, undecodable_value, valid]).await;

    assert_eq!(
        s.get(&3).await,
        Some("ok".to_string()),
        "the one decodable record in the batch still applies"
    );
    assert_eq!(
        s.get(&2).await,
        None,
        "a record with undecodable value bytes is skipped, not applied"
    );
    assert_digest_matches_full_recompute(&s).await;
}

#[tokio::test]
async fn invalidate_with_undecodable_key_bytes_is_a_silent_no_op() {
    let s = shard::<u32, String>(1);
    s.insert(1, "a".into()).await.expect("insert");

    ShardOps::invalidate(&s, Bytes::new(), hlc(ahead_ms(), 2)).await;

    assert_eq!(
        s.get(&1).await,
        Some("a".into()),
        "an invalidate call for undecodable key bytes touches nothing"
    );
    assert_digest_matches_full_recompute(&s).await;
}

/// Two keys guaranteed to land in different stripes, for the striping tests
/// below.
fn two_keys_in_different_stripes() -> (u32, u32) {
    let a = 0u32;
    let a_stripe = bucket_of(&key_bytes(&a));
    let b = (1u32..10_000)
        .find(|b| bucket_of(&key_bytes(b)) != a_stripe)
        .expect("some key among the first 10,000 lands in a different stripe from key 0");
    (a, b)
}

#[tokio::test]
async fn insert_stripe_locks_are_independent_per_bucket() {
    let s = Arc::new(shard::<u32, String>(1));
    let (key_a, key_b) = two_keys_in_different_stripes();
    let bucket_a = usize::from(bucket_of(&key_bytes(&key_a)));
    let bucket_b = usize::from(bucket_of(&key_bytes(&key_b)));

    let held = Arc::clone(&s);
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let _guard = held.engine.stripe_lock(bucket_a).write();
        tx.send(()).expect("send");
        std::thread::sleep(Duration::from_millis(150));
    });
    rx.recv().expect("lock holder signals it has the lock");

    assert!(
        s.engine.stripe_lock(bucket_b).try_write().is_some(),
        "a different stripe's lock is not blocked"
    );
    assert!(
        s.engine.stripe_lock(bucket_a).try_write().is_none(),
        "the held stripe's lock is still contended"
    );
    handle.join().expect("lock holder thread does not panic");
    assert!(
        s.engine.stripe_lock(bucket_a).try_write().is_some(),
        "released after the holder finishes"
    );

    // The released stripe still accepts writes end to end.
    s.insert(key_a, "a".into()).await.expect("insert");
    assert_eq!(s.get(&key_a).await, Some("a".into()));
}

#[tokio::test]
async fn concurrent_inserts_across_many_stripes_all_land() {
    const DISTINCT_STRIPES: usize = 16;
    let s = Arc::new(shard::<u32, String>(1));
    let mut keys = vec![0u32];
    let mut seen_stripes = HashSet::from([bucket_of(&key_bytes(&0u32))]);
    let mut candidate = 1u32;
    while seen_stripes.len() < DISTINCT_STRIPES {
        if seen_stripes.insert(bucket_of(&key_bytes(&candidate))) {
            keys.push(candidate);
        }
        candidate += 1;
    }

    let handles: Vec<_> = keys
        .iter()
        .copied()
        .map(|k| {
            let s = Arc::clone(&s);
            tokio::spawn(async move { s.insert(k, format!("v{k}")).await })
        })
        .collect();
    for handle in handles {
        handle
            .await
            .expect("spawned insert does not panic")
            .expect("insert");
    }
    for k in keys {
        assert_eq!(s.get(&k).await, Some(format!("v{k}")));
    }
}

#[tokio::test]
async fn absolute_ttl_on_the_wire_expires_a_shard_configured_with_no_ttl_of_its_own() {
    let a = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_millis(50)),
        None,
    );
    let b = shard::<u32, String>(2); // no ttl configured here

    a.insert(1, "short-lived".into()).await.expect("insert");
    let recs = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    assert_eq!(recs.len(), 1);
    assert!(
        recs[0].expires_at_ms.is_some(),
        "the wire record carries the absolute deadline `a` computed"
    );
    ShardOps::apply_remote(&b, recs[0].clone()).await;
    assert_eq!(b.get(&1).await, Some("short-lived".into()));

    // Waits well past the 50ms TTL, matching the digest-flushing test
    // below.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(a.get(&1).await, None, "the origin's own copy expires");
    assert_eq!(
        b.get(&1).await,
        None,
        "b expires the entry from the wire-carried deadline alone"
    );
}

#[tokio::test]
async fn per_entry_ttl_overrides_the_shard_default_and_replicates() {
    let a = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_secs(30)),
        None,
    );
    let b = shard::<u32, String>(2);
    let short = Duration::from_millis(50);

    a.insert_with_ttl(1, "short".into(), short)
        .await
        .expect("insert with ttl");
    a.insert(2, "default".into()).await.expect("insert");
    a.insert_many_with_ttl([(3, "short-batch".to_string())], short)
        .await
        .expect("insert_many with ttl");
    a.get_or_load(&4, async |_| Ok::<_, std::io::Error>("default-fill".into()))
        .await
        .expect("fill");
    // A hit does not re-stamp: key 1 keeps its short override.
    let hit = a
        .get_or_load(&1, async |_| Ok::<_, std::io::Error>("never-called".into()))
        .await
        .expect("hit");
    assert_eq!(hit, "short");

    let recs = ShardOps::records_for(&a, (1..=4u32).map(|k| key_bytes(&k)).collect()).await;
    assert_eq!(recs.len(), 4);
    for rec in &recs {
        assert!(
            rec.expires_at_ms.is_some(),
            "every record carries a deadline"
        );
    }
    ShardOps::apply_remote_batch(&b, recs).await;
    for key in 1..=4u32 {
        assert!(b.get(&key).await.is_some(), "b holds key {key} on arrival");
    }

    // Past the short TTL, short of the 30s default.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    for (shard, name) in [(&a, "a"), (&b, "b")] {
        assert_eq!(shard.get(&1).await, None, "{name}: short insert expires");
        assert_eq!(shard.get(&3).await, None, "{name}: short batch expires");
        assert_eq!(
            shard.get(&2).await.as_deref(),
            Some("default"),
            "{name}: the default-TTL entry outlives the overrides"
        );
        assert_eq!(
            shard.get(&4).await.as_deref(),
            Some("default-fill"),
            "{name}: a read-through fill takes the default, never an override"
        );
    }
}

#[test]
fn rearmed_version_is_the_bases_successor_keyed_by_the_new_deadline() {
    let base = hlc(5_000, 1);
    let minted = rearmed_version(base, Some(9_000));
    assert!(minted > base);
    assert_eq!((minted.wall_ms, minted.logical), (5_000, base.logical + 1));
    assert!(minted.node.is_merge_derived());
    assert_eq!(
        rearmed_version(base, Some(9_000)),
        minted,
        "the same re-arm of the same record mints the same version on any node"
    );
    assert_ne!(rearmed_version(base, Some(9_001)), minted);
    assert_ne!(rearmed_version(base, None), minted);
    assert_ne!(
        rearmed_version(hlc(5_000, 2), Some(9_000)).node,
        minted.node,
        "another base record mints another node id"
    );
    assert!(
        minted < hlc(5_001, 1),
        "a write stamped a millisecond after the base outranks the re-arm"
    );
}

/// A `Mode::Replicated` shard for `node` on a clock the test sets.
fn clocked_shard(
    node: u64,
    default_ttl: Option<Duration>,
    clock: &Arc<std::sync::atomic::AtomicU64>,
) -> Shard<u32, String> {
    let reader = Arc::clone(clock);
    Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(node),
        10_000,
        default_ttl,
        None,
    )
    .with_clock(Arc::new(move || {
        reader.load(std::sync::atomic::Ordering::SeqCst)
    }))
}

#[tokio::test]
async fn expire_gives_a_live_entry_a_new_lifespan_and_keeps_its_value() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let clock = Arc::new(AtomicU64::new(1_000_000));
    let s = clocked_shard(1, None, &clock);
    s.insert(1, "a".into()).await.expect("insert");
    let before = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    let _ = s.fan_out_queue().drain();
    let mut events = s.events();

    assert!(s.expire(&1, Duration::from_secs(30)).await.expect("expire"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(Duration::from_secs(30))));
    assert_eq!(s.get(&1).await, Some("a".to_string()));
    let after = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert_eq!(after[0].expires_at_ms, Some(1_030_000));
    assert_eq!(
        after[0].ver,
        rearmed_version(before[0].ver, Some(1_030_000)),
        "the re-arm is stamped as the value's successor"
    );
    assert!(
        matches!(
            s.fan_out_queue().drain().as_slice(),
            [FanOutItem::Applied(1)]
        ),
        "the re-arm fans out like any local write"
    );
    assert!(matches!(
        events.try_recv(),
        Ok(Event::Updated { key: 1, ref value, origin: Origin::Local }) if value == "a"
    ));

    clock.store(1_030_000, Ordering::SeqCst);
    assert_eq!(s.get(&1).await, None, "it expires at the new deadline");
    assert_eq!(s.ttl_of(&1), None);
}

#[tokio::test]
async fn expire_touch_and_persist_answer_false_for_a_key_that_is_not_live() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let clock = Arc::new(AtomicU64::new(1_000_000));
    let s = clocked_shard(1, None, &clock);
    assert!(!s.expire(&1, Duration::from_secs(5)).await.expect("expire"));
    assert!(!s.touch(&1).await.expect("touch"));
    assert!(!s.persist(&1).await.expect("persist"));

    s.insert(2, "removed".into()).await.expect("insert");
    s.remove(&2).await.expect("remove");
    assert!(!s.expire(&2, Duration::from_secs(5)).await.expect("expire"));
    assert_eq!(s.get(&2).await, None, "a removed key stays removed");

    s.insert_with_ttl(3, "expired".into(), Duration::from_secs(1))
        .await
        .expect("insert");
    clock.store(1_002_000, Ordering::SeqCst);
    assert!(!s.persist(&3).await.expect("persist"));
    assert_eq!(s.get(&3).await, None, "an expired key stays expired");
    let _ = s.fan_out_queue().drain();
    assert!(!s.touch(&4).await.expect("touch"));
    assert!(
        s.fan_out_queue().drain().is_empty(),
        "nothing to re-arm, nothing to fan out"
    );
}

#[tokio::test]
async fn touch_restarts_the_default_ttl_and_persist_makes_the_entry_never_expire() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let clock = Arc::new(AtomicU64::new(1_000_000));
    let s = clocked_shard(1, Some(Duration::from_secs(10)), &clock);
    s.insert(1, "a".into()).await.expect("insert");
    clock.store(1_004_000, Ordering::SeqCst);
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(Duration::from_secs(6))));

    assert!(s.touch(&1).await.expect("touch"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(Duration::from_secs(10))));
    assert!(s.expire(&1, Duration::from_secs(1)).await.expect("expire"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(Duration::from_secs(1))));
    assert!(s.persist(&1).await.expect("persist"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Never));

    clock.store(9_000_000, Ordering::SeqCst);
    assert_eq!(
        s.get(&1).await,
        Some("a".to_string()),
        "a persisted entry never expires"
    );
}

#[tokio::test]
async fn touch_without_a_default_ttl_makes_the_entry_never_expire() {
    use std::sync::atomic::AtomicU64;

    let clock = Arc::new(AtomicU64::new(1_000_000));
    let s = clocked_shard(1, None, &clock);
    s.insert_with_ttl(1, "a".into(), Duration::from_secs(5))
        .await
        .expect("insert");
    assert!(s.touch(&1).await.expect("touch"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Never));
}

#[tokio::test]
async fn expire_with_a_zero_ttl_expires_the_entry_here_and_on_a_replica() {
    use std::sync::atomic::AtomicU64;

    let clock = Arc::new(AtomicU64::new(1_000_000));
    let a = clocked_shard(1, None, &clock);
    let b = clocked_shard(2, None, &clock);
    a.insert(1, "a".into()).await.expect("insert");
    let original = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    ShardOps::apply_remote_batch(&b, original).await;

    assert!(a.expire(&1, Duration::ZERO).await.expect("expire"));
    assert_eq!(a.get(&1).await, None);
    let rearmed = WireRecord {
        key: key_bytes(&1u32),
        ver: rearmed_version(
            ShardOps::records_for(&b, vec![key_bytes(&1u32)]).await[0].ver,
            Some(1_000_000),
        ),
        value: Some(Bytes::from(
            postcard::to_stdvec(&"a".to_string()).expect("encodes"),
        )),
        expires_at_ms: Some(1_000_000),
    };
    ShardOps::apply_remote_batch(&b, vec![rearmed]).await;
    assert_eq!(
        b.get(&1).await,
        None,
        "a re-arm that arrives already expired still replaces the older copy"
    );
}

/// Under a merging resolver the re-arm applies through the resolver
/// like any write, and the `crdt` resolvers give a merged record no
/// expiry: the value stays and the entry does not expire.
#[tokio::test]
async fn expire_under_a_crdt_resolver_keeps_the_value_and_the_resolvers_expiry() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let s = Shard::<u32, PnCounter>::new(
        SmolStr::new("test-crdt-expire"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_resolver(Arc::new(PnCounterResolver));
    let writer = crdt::WriterId::new(NodeId::from(11), 1);
    s.merge(1, PnCounter::local_delta(writer, 3))
        .await
        .expect("merge");

    assert!(s.expire(&1, Duration::from_secs(60)).await.expect("expire"));
    assert_eq!(s.get(&1).await.map(|c| c.value()), Some(3));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Never));
}

/// The re-arm's successor stamp outranks the value it read and nothing
/// written after it: a remove a peer stamped later survives the
/// re-arm in either delivery order, and a peer still holding the value
/// takes the new lifetime.
#[tokio::test]
async fn a_rearm_never_outvotes_a_remove_stamped_after_the_value_it_read() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let clock_a = Arc::new(AtomicU64::new(1_000_000));
    let clock_b = Arc::new(AtomicU64::new(1_000_000));
    let clock_c = Arc::new(AtomicU64::new(1_000_000));
    let a = clocked_shard(1, None, &clock_a);
    let b = clocked_shard(2, None, &clock_b);
    let c = clocked_shard(3, None, &clock_c);
    a.insert(1, "v".into()).await.expect("insert");
    let original = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    ShardOps::apply_remote_batch(&b, original.clone()).await;
    ShardOps::apply_remote_batch(&c, original).await;

    clock_b.store(1_000_500, Ordering::SeqCst);
    b.remove(&1).await.expect("remove");
    clock_a.store(1_001_000, Ordering::SeqCst);
    assert!(a.expire(&1, Duration::from_secs(60)).await.expect("expire"));

    let rearm = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    let tombstone = ShardOps::records_for(&b, vec![key_bytes(&1u32)]).await;
    ShardOps::apply_remote_batch(&b, rearm.clone()).await;
    ShardOps::apply_remote_batch(&a, tombstone).await;
    assert_eq!(
        b.get(&1).await,
        None,
        "the later remove survives the re-arm"
    );
    assert_eq!(
        a.get(&1).await,
        None,
        "and replaces it where it arrives second"
    );

    ShardOps::apply_remote_batch(&c, rearm).await;
    clock_c.store(1_001_000, Ordering::SeqCst);
    assert_eq!(
        c.ttl_of(&1),
        Some(Ttl::Remaining(Duration::from_secs(60))),
        "a replica still holding the value takes the new lifetime"
    );
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

/// `rearm_local` over every residency state a read tells apart: the
/// local copy answers iff the part is owned and trusted and the read
/// hits or the part is warm, re-arming a live entry and
/// reporting a warm miss absent; every other state is the owners'.
#[tokio::test]
async fn rearm_local_follows_the_read_rule_over_every_residency_state() {
    for case in read_cases::every_read_case() {
        let built = read_cases::case_shard("rearm-cases", &case).await;
        let answers = case.owns && !case.distrusted() && (case.live() || !case.cold());
        let expected = if !answers {
            LocalRearm::NotHeld
        } else if case.live() {
            LocalRearm::Rearmed
        } else {
            LocalRearm::Absent
        };
        assert_eq!(
            built
                .shard
                .rearm_local(&built.key, Lifetime::Unbounded)
                .await
                .expect("rearm"),
            expected,
            "{case:?}"
        );
    }
}

/// An owned part not yet pulled cannot vouch for a miss, so a re-arm
/// that finds nothing there leaves the key to the other owners, while
/// a copy it does hold re-arms here.
/// `rearm_local` reads the part's residency again once its fan-out wait
/// ends: a part this node owned when the re-arm began and gave up while
/// it waited for backlog room is not re-armed from its copy.
#[tokio::test]
async fn rearm_local_decides_under_the_view_past_its_fan_out_wait() {
    use read_cases::{Held, ReadCase};
    let built = read_cases::case_shard(
        "rearm-past-the-wait",
        &ReadCase {
            owns: true,
            cold_marked: false,
            unsettled: false,
            stale: false,
            unverified: false,
            releasing: false,
            held: Held::Live,
        },
    )
    .await;
    let s = built
        .shard
        .with_fan_out_backlog_capacity(1)
        .with_fan_out_wait_timeout(Duration::from_secs(10));
    let queue = s.fan_out_queue();
    let n1 = NodeId::from(1u64);
    let elsewhere = (0..u32::MAX)
        .find(|k| !built.view.owns(PartId::of_key(&key_bytes(k))))
        .expect("node 2 owns some parts");
    s.insert(elsewhere, "forwarded".to_string())
        .await
        .expect("a forward fills the backlog");
    assert_eq!(queue.len(), 1);

    let rearm = s.rearm_local(&built.key, Lifetime::Unbounded);
    tokio::pin!(rearm);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut rearm)
            .await
            .is_err(),
        "the re-arm waits for backlog room"
    );
    let moved = (3..u64::MAX)
        .map(|last| {
            OwnershipView::compute(n1, (2..=last).map(NodeId::from).collect(), NonZeroU8::MIN)
        })
        .find(|view| !view.owns(built.part))
        .expect("enough other nodes take the part from node 1");
    built
        .view_tx
        .send(Arc::new(moved))
        .expect("the tracker holds a receiver");
    queue.drain();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), &mut rearm)
            .await
            .expect("the re-arm proceeds once the backlog drains")
            .expect("re-arms"),
        LocalRearm::NotHeld,
        "the part moved during the wait: its owners re-arm it"
    );
}

#[tokio::test]
async fn a_rearm_in_a_cold_owned_part_answers_only_from_a_copy_it_holds() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) =
        distributed_shard::<u32, String>(self_node, eligible, 2, Arc::clone(&residency));
    let held = find_key_by_ownership(&view, true);
    let missing = (held + 1..u32::MAX)
        .find(|k| view.owns(PartId::of_key(&key_bytes(k))))
        .expect("another owned key");
    s.insert(held, "held".to_string()).await.expect("insert");
    residency.mark_cold(&[
        PartId::of_key(&key_bytes(&held)),
        PartId::of_key(&key_bytes(&missing)),
    ]);

    assert_eq!(
        s.rearm_local(&missing, Lifetime::Unbounded)
            .await
            .expect("rearm"),
        LocalRearm::NotHeld,
        "a cold part's miss is no answer"
    );
    assert_eq!(
        s.rearm_local(&held, Lifetime::For(Duration::from_secs(5)))
            .await
            .expect("rearm"),
        LocalRearm::Rearmed,
        "a copy held in a cold part still re-arms"
    );
    assert!(matches!(s.ttl_of(&held), Some(Ttl::Remaining(_))));
}

/// Covers both [`ShardOps::apply_remote`] and
/// [`ShardOps::apply_remote_batch`], and both a key that expired for
/// real and one that never existed.
#[tokio::test]
async fn dead_on_arrival_ttl_record_never_resurrects_a_locally_expired_entry() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_millis(50)),
        None,
    );
    s.insert(1, "original".into()).await.expect("insert");
    assert_eq!(s.get(&1).await, Some("original".into()));

    // Let it expire locally for real, well past the 50ms TTL.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(
        s.get(&1).await,
        None,
        "sanity: the original entry is gone before the dead-on-arrival record arrives"
    );

    let doa = WireRecord {
        key: key_bytes(&1u32),
        // Clearly newer than anything this shard's clock has stamped, so
        // only the absolute deadline can cause the rejection.
        ver: hlc(now_ms() + 10_000, 2),
        value: Some(Bytes::from(
            postcard::to_stdvec(&"resurrected".to_string()).expect("test value encodes"),
        )),
        expires_at_ms: Some(now_ms().saturating_sub(60_000)),
    };

    ShardOps::apply_remote(&s, doa.clone()).await;
    assert_eq!(
        s.get(&1).await,
        None,
        "a dead-on-arrival record does not resurrect a locally-expired entry, via apply_remote"
    );

    let s2 = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_millis(50)),
        None,
    );
    s2.insert(1, "original".into()).await.expect("insert");
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(s2.get(&1).await, None, "sanity: gone here too");

    ShardOps::apply_remote_batch(&s2, vec![doa]).await;
    assert_eq!(
        s2.get(&1).await,
        None,
        "same guarantee via apply_remote_batch"
    );
}

#[tokio::test]
async fn gc_tombstones_drops_expired_entries_and_updates_digest() {
    let mut s = shard::<u32, String>(1);
    s.remove(&1).await.expect("remove creates a tombstone");
    let records = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert!(!records.is_empty(), "{records:?}");

    // Force the tombstone already recorded to read as expired.
    s.tombstone_ttl_ms = 0;
    s.engine
        .debug_force_tombstone_ttl_past(key_bytes(&1u32).as_ref(), false);

    ShardOps::gc_tombstones(&s, false).await;
    let records = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert!(records.is_empty(), "{records:?}");
    assert_digest_matches_full_recompute(&s).await;
}

/// Forces one tombstone's `ttl_deadline_ms`, and `max_deadline_ms` too when
/// `past_max`.
fn force_tombstone_past_ttl<K, V>(s: &Shard<K, V>, key_bytes: &Bytes, past_max: bool)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    s.engine
        .debug_force_tombstone_ttl_past(key_bytes.as_ref(), past_max);
}

#[tokio::test]
async fn gc_tombstones_defers_collection_while_a_member_is_absent() {
    let mut s = shard::<u32, String>(1);
    s.remove(&1).await.expect("remove creates a tombstone");
    s.tombstone_ttl_ms = 0;
    force_tombstone_past_ttl(&s, &key_bytes(&1u32), false);

    ShardOps::gc_tombstones(&s, true).await;
    assert!(
        !ShardOps::records_for(&s, vec![key_bytes(&1u32)])
            .await
            .is_empty(),
        "a tombstone past tombstone_ttl but not tombstone_max_ttl survives \
         collection while a member is absent"
    );
    assert_digest_matches_full_recompute(&s).await;
}

#[tokio::test]
async fn gc_tombstones_proceeds_once_no_member_is_absent() {
    let mut s = shard::<u32, String>(1);
    s.remove(&1).await.expect("remove creates a tombstone");
    s.tombstone_ttl_ms = 0;
    force_tombstone_past_ttl(&s, &key_bytes(&1u32), false);

    ShardOps::gc_tombstones(&s, true).await;
    assert!(
        !ShardOps::records_for(&s, vec![key_bytes(&1u32)])
            .await
            .is_empty(),
        "deferred while the member was absent"
    );
    assert_digest_matches_full_recompute(&s).await;

    ShardOps::gc_tombstones(&s, false).await;
    assert!(
        ShardOps::records_for(&s, vec![key_bytes(&1u32)])
            .await
            .is_empty(),
        "collected once the member returned (no member absent any more)"
    );
    assert_digest_matches_full_recompute(&s).await;
}

#[tokio::test]
async fn gc_tombstones_hard_cap_overrides_deferral() {
    let mut s = shard::<u32, String>(1);
    s.remove(&1).await.expect("remove creates a tombstone");
    s.tombstone_ttl_ms = 0;
    s.tombstone_max_ttl_ms = 0;
    force_tombstone_past_ttl(&s, &key_bytes(&1u32), true);

    ShardOps::gc_tombstones(&s, true).await;
    assert!(
        ShardOps::records_for(&s, vec![key_bytes(&1u32)])
            .await
            .is_empty(),
        "a tombstone past tombstone_max_ttl is collected even while a member is absent"
    );
    assert_digest_matches_full_recompute(&s).await;
}

#[tokio::test]
async fn run_pending_tasks_flushes_a_quiet_shards_stale_ttl_eviction_digest() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_millis(50)),
        None,
    );
    s.insert(1, "value".into()).await.expect("insert");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Logical expiry is visible before the engine's sweep corrects the
    // digest.
    let entries = ShardOps::bucket_entries(&s, bucket_of(&key_bytes(&1u32))).await;
    assert!(entries.is_empty(), "{entries:?}");

    // Without this, the sweep never runs for a quiet shard on its own.
    ShardOps::run_pending_tasks(&s).await;
    assert_digest_matches_full_recompute(&s).await;
}

/// Compares the incrementally-maintained digest against a full recompute.
async fn assert_digest_matches_full_recompute<K, V>(s: &Shard<K, V>)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let expected_parts = s.engine.recompute_digests();
    for BucketDigest { bucket, digest } in ShardOps::digests(s).await {
        let expected = (0..PART_COUNT).fold(0u64, |acc, part| {
            acc ^ expected_parts[usize::from(bucket) * PART_COUNT + part]
        });
        assert_eq!(
            digest, expected,
            "bucket {bucket} incremental digest diverged from full recompute"
        );
    }
}

#[tokio::test]
async fn digest_incremental_matches_full_recompute_after_random_ops() {
    let s = shard::<u32, u64>(1);
    let mut rng = StdRng::seed_from_u64(0xC0FF_EE42);

    for i in 0..300 {
        let key = rng.random_range(0..16u32);
        match rng.random_range(0..3u32) {
            0 => {
                let _ = s.insert(key, u64::from(key) * 31).await;
            }
            1 => {
                let _ = s.remove(&key).await;
            }
            _ => {
                let rec = WireRecord {
                    key: key_bytes(&key),
                    value: if rng.random_bool(0.5) {
                        Some(Bytes::from(
                            postcard::to_stdvec(&(u64::from(key) * 7)).expect("encode"),
                        ))
                    } else {
                        None
                    },
                    ver: hlc(rng.random_range(1..u64::MAX / 4), rng.random_range(2..5)),
                    expires_at_ms: None,
                };
                ShardOps::apply_remote(&s, rec).await;
            }
        }
        if i % 10 == 0 {
            assert_digest_matches_full_recompute(&s).await;
        }
    }
    assert_digest_matches_full_recompute(&s).await;
}

#[tokio::test]
async fn bucket_lens_counts_without_materializing_entries() {
    let s = shard::<u32, String>(1);
    let key = 3u32;
    let kb = key_bytes(&key);
    let bucket = bucket_of(&kb);
    s.insert(key, "three".into()).await.expect("insert");
    s.remove(&99u32).await.expect("tombstone somewhere else");

    let lens = ShardOps::bucket_lens(&s, vec![bucket, u16::MAX]).await;
    let len = lens
        .iter()
        .find(|bl| bl.bucket == bucket)
        .expect("the populated bucket is answered")
        .len;
    assert!(len >= 1, "bucket_lens counts at least the inserted entry");
    let oob_len = lens
        .iter()
        .find(|bl| bl.bucket == u16::MAX)
        .expect("an out-of-range bucket is still answered, at 0")
        .len;
    assert_eq!(oob_len, 0);
}

#[tokio::test]
async fn part_digests_xor_to_the_bucket_digest_at_the_shard_layer() {
    let s = shard::<u32, String>(1);
    for k in 0..500u32 {
        s.insert(k, k.to_string()).await.expect("insert");
    }
    let all_buckets: Vec<u16> = (0..u16::try_from(BUCKET_COUNT).expect("fits")).collect();
    let bucket_digests = ShardOps::digests(&s).await;
    let part_digests = ShardOps::part_digests(&s, all_buckets).await;
    assert_eq!(part_digests.len(), BUCKET_COUNT);
    for BucketDigest { bucket, digest } in bucket_digests {
        let parts = &part_digests
            .iter()
            .find(|bpd| bpd.bucket == bucket)
            .expect("every bucket answered")
            .digests;
        assert_eq!(parts.len(), PART_COUNT);
        let xored = parts.iter().fold(0u64, |acc, d| acc ^ d);
        assert_eq!(xored, digest, "bucket {bucket}'s parts XOR to its digest");
    }
}

#[tokio::test]
async fn part_digests_ignores_a_bucket_outside_the_stripe_range() {
    let s = shard::<u32, String>(1);
    s.insert(1, "a".into()).await.expect("insert");
    let out = ShardOps::part_digests(&s, vec![u16::MAX]).await;
    assert!(
        out.iter().all(|bpd| bpd.digests.is_empty()),
        "a bucket past BUCKET_COUNT answers an empty part-digest vec"
    );
}

#[tokio::test]
async fn entries_for_parts_returns_exactly_the_requested_parts_entries() {
    let s = shard::<u32, String>(1);
    let key = 7u32;
    let kb = key_bytes(&key);
    let hash = engine::hash_key_bytes(kb.as_ref());
    let bucket = bucket_of(&kb);
    let part = u8::try_from(engine::part_index_from_hash(hash)).expect("fits");
    s.insert(key, "seven".into()).await.expect("insert");

    let result = ShardOps::entries_for_parts(&s, vec![BucketPart { bucket, part }]).await;
    assert_eq!(result.len(), 1);
    let (got, entries) = &result[0];
    assert_eq!(*got, BucketPart { bucket, part });
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key.as_ref(), kb.as_ref());
}

#[tokio::test]
async fn entries_for_parts_dedupes_and_sorts_requested_pairs() {
    let s = shard::<u32, String>(1);
    s.insert(1, "a".into()).await.expect("insert");
    let kb = key_bytes(&1u32);
    let bucket = bucket_of(&kb);
    let part = u8::try_from(engine::part_index_from_hash(engine::hash_key_bytes(
        kb.as_ref(),
    )))
    .expect("fits");

    let result = ShardOps::entries_for_parts(
        &s,
        vec![
            BucketPart { bucket, part },
            BucketPart { bucket, part },
            BucketPart { bucket, part },
        ],
    )
    .await;
    assert_eq!(
        result.len(),
        1,
        "a duplicated (bucket, part) request is answered exactly once"
    );
}

#[tokio::test]
async fn with_max_frame_overrides_the_default_cap() {
    let s = shard::<u32, Vec<u8>>(1).with_max_frame(64);
    let err = s
        .insert(1, vec![0u8; 100])
        .await
        .expect_err("a value over the overridden cap is rejected");
    assert!(matches!(err, CacheError::ValueTooLarge { .. }));

    s.insert(2, Vec::new())
        .await
        .expect("a small value still fits under the overridden cap");
}

#[tokio::test]
async fn insert_rejects_when_the_value_alone_fits_but_the_wire_frame_does_not() {
    // The value's own encoding fits a 25-byte cap; the record's full wire
    // frame does not.
    let s = shard::<u32, Vec<u8>>(1).with_max_frame(25);
    let err = s
        .insert(1, vec![0u8; 20])
        .await
        .expect_err("the full wire frame, not only the value, counts toward the cap");
    assert!(matches!(err, CacheError::ValueTooLarge { .. }));
}

#[tokio::test]
async fn with_weigher_drives_the_engines_total_weight() {
    let s = shard::<u32, Vec<u8>>(1)
        .with_weigher(|_key: &u32, value: &Vec<u8>| u32::try_from(value.len()).unwrap_or(u32::MAX));
    s.insert(1, vec![0u8; 7]).await.expect("insert");
    let (_, weight) = s.engine.debug_totals();
    assert_eq!(
        weight, 7,
        "a custom weigher drives the engine's total weight, not the default of 1 per entry"
    );
}

#[cfg(feature = "spill")]
#[tokio::test]
async fn with_spill_lets_eviction_extend_capacity_onto_disk() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-with-spill-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);

    let s = Shard::<u32, String>::new(
        SmolStr::new("test-spill"),
        Mode::Local,
        NodeId::from(1),
        3, // a tiny weight cap: at most 3 one-weight entries fit resident
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("the tier's directory and region files open");

    for k in 0..20u32 {
        s.insert(k, format!("value-{k}")).await.expect("insert");
    }

    assert_eq!(
        s.entry_count().await,
        20,
        "eviction demotes the coldest entries onto disk instead of deleting them once a \
         spill tier is configured, so live_count never shrinks below what was written"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Shard-level counterpart to `engine::tests::spill_payload::io::
/// merge_against_a_spilled_counter_folds_the_stored_side_instead_of_dropping_it`:
/// a merging resolver's stored side spills between two colliding
/// `Shard::merge` calls, and the second still folds against it instead
/// of the outright win/loss a value-less view would produce.
#[cfg(feature = "spill")]
#[tokio::test]
async fn merge_against_a_spilled_counter_folds_instead_of_dropping_a_side() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-merge-spill-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);

    let s = Shard::<u32, PnCounter>::new(
        SmolStr::new("test-merge-spill"),
        Mode::Local,
        NodeId::from(1),
        1, // a tiny weight cap: the weigher below always exceeds it
        None,
        None,
    )
    .with_resolver(Arc::new(PnCounterResolver))
    .with_weigher(|_key: &u32, _value: &PnCounter| 2)
    .with_spill(&cfg)
    .expect("the tier's directory and region files open");

    let writer_a = crdt::WriterId::new(NodeId::from(11), 1);
    let writer_b = crdt::WriterId::new(NodeId::from(22), 1);

    s.merge(1, PnCounter::local_delta(writer_a, 3))
        .await
        .expect("first increment");

    let spilled = tokio::time::timeout(Duration::from_secs(5), async {
        while s.get_sync(&1).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        spilled,
        "the single over-weight entry spills once eviction runs"
    );

    s.merge(1, PnCounter::local_delta(writer_b, 4))
        .await
        .expect("second increment, colliding with the spilled record");

    assert_eq!(
        s.get(&1).await.map(|c| c.value()),
        Some(7),
        "merging into a spilled record folds both sides' contributions instead of \
         dropping the one that was on disk"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that a warm reopen leaves a reloaded bucket cold until the
/// caller's own reconciliation clears it; the record stays readable meanwhile.
#[cfg(feature = "spill")]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end two-life scenario: splitting it would only scatter state \
              (dir, cfg, view, key, bucket) across helper signatures"
)]
async fn attach_spill_via_a_warm_reopen_leaves_the_reloaded_buckets_cold_until_reconciliation() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-warm-reopen-cold-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);

    let self_node = NodeId::from(201u64);
    let peer = NodeId::from(202u64);
    let owners = NonZeroU8::new(2).expect("nonzero");
    let view = Arc::new(OwnershipView::compute(
        self_node,
        vec![self_node, peer],
        owners,
    ));
    let key = find_key_by_ownership(&view, true);
    let bucket = bucket_of(&key_bytes(&key));

    // First life: a tiny weight cap forces the entry to disk before the
    // checkpointed close.
    let (tracker1, tx1) = OwnershipTracker::seed(
        self_node,
        &[],
        &HashMap::new(),
        &SmolStr::new("warm-cold"),
        owners,
    );
    tx1.send(Arc::clone(&view)).expect("receiver still alive");
    let residency1 = Arc::new(ResidencySet::new());
    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("warm-cold"),
        Mode::Distributed { owners },
        self_node,
        1, // a tiny weight cap: the one entry spills
        None,
        None,
    )
    .with_ownership(tracker1, residency1);
    let warm0 = shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");
    assert!(
        warm0.warm_parts.is_empty(),
        "a brand-new directory has nothing to warm-reopen the first time"
    );

    shard1.insert(key, "v".to_string()).await.expect("insert");
    // Eviction samples within one bucket at a time; filling `key`'s bucket
    // with newer entries concentrates eviction onto `key`, the oldest one there.
    let fillers: Vec<u32> = (0..1_000_000u32)
        .filter(|&k| k != key && bucket_of(&key_bytes(&k)) == bucket)
        .take(20)
        .collect();
    assert!(
        fillers.len() >= 8,
        "at least an eviction sample's worth of same-bucket fillers is found quickly"
    );
    for filler in fillers {
        shard1
            .insert(filler, "filler".to_string())
            .await
            .expect("insert filler");
    }
    let spilled = tokio::time::timeout(Duration::from_secs(5), async {
        while shard1.get_sync(&key).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        spilled,
        "the target key spills to disk once eviction runs past the tiny weight cap"
    );
    shard1.close_spill_checkpointed().await;

    // Second life: same directory, fresh tracker/residency, bucket marked
    // cold up front.
    let (tracker2, tx2) = OwnershipTracker::seed(
        self_node,
        &[],
        &HashMap::new(),
        &SmolStr::new("warm-cold"),
        owners,
    );
    tx2.send(Arc::clone(&view)).expect("receiver still alive");
    let residency2 = Arc::new(ResidencySet::new());
    residency2.mark_cold(&whole(bucket));
    let shard2 = Shard::<u32, String>::new(
        SmolStr::new("warm-cold"),
        Mode::Distributed { owners },
        self_node,
        1,
        None,
        None,
    )
    .with_ownership(tracker2, Arc::clone(&residency2));

    let warm = shard2
        .attach_spill(&cfg)
        .expect("a checkpoint snapshot within tombstone_ttl reopens warm");
    assert_eq!(
        warm.warm_parts
            .iter()
            .map(|part| part.bucket())
            .collect::<HashSet<u16>>(),
        HashSet::from([bucket]),
        "the second life's attach_spill warm-reloads parts of exactly the one bucket the \
         replayed records belong to"
    );
    assert!(warm.warm_parts.contains(&PartId::of_key(&key_bytes(&key))));
    assert!(
        residency2.is_cold(PartId::of_key(&key_bytes(&key))),
        "attach_spill alone never clears cold: only the caller's eager reconciliation does"
    );
    assert_eq!(
        shard2.get(&key).await,
        Some("v".to_string()),
        "the warm-reloaded record is still readable even while its bucket stays marked cold"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that a checkpointed close recovers every live entry, resident or
/// already spilled, and never resurrects a key tombstoned before the close.
#[cfg(feature = "spill")]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end three-key scenario (resident, spilled, tombstoned): splitting \
              it would only scatter state (dir, cfg, node, the three keys) across helper \
              signatures"
)]
async fn a_checkpointed_close_replays_every_live_entry_and_never_a_tombstoned_key() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-checkpoint-replay-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let node = NodeId::from(301u64);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("checkpoint-replay"),
        Mode::Local,
        node,
        2, // room for exactly two resident entries at once
        None,
        None,
    );
    let warm0 = shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");
    assert!(warm0.warm_parts.is_empty());

    let resident_key = 1_000_001u32;
    let spilled_key = 1_000_002u32;
    let tombstoned_key = 1_000_003u32;

    shard1
        .insert(resident_key, "resident-value".to_string())
        .await
        .expect("insert resident");
    shard1
        .insert(spilled_key, "spilled-value".to_string())
        .await
        .expect("insert spilled");

    // Filling `spilled_key`'s bucket concentrates eviction there,
    // leaving `resident_key`'s different bucket untouched.
    let spilled_bucket = bucket_of(&key_bytes(&spilled_key));
    let resident_bucket = bucket_of(&key_bytes(&resident_key));
    assert_ne!(
        spilled_bucket, resident_bucket,
        "the two keys must land in different buckets for this test to isolate eviction \
         pressure onto only one of them"
    );
    let fillers: Vec<u32> = (2_000_000..3_000_000u32)
        .filter(|&k| bucket_of(&key_bytes(&k)) == spilled_bucket)
        .take(20)
        .collect();
    assert!(
        fillers.len() >= 8,
        "at least an eviction sample's worth of same-bucket fillers is found quickly"
    );
    for filler in fillers {
        shard1
            .insert(filler, "filler".to_string())
            .await
            .expect("insert filler");
    }
    let spilled = tokio::time::timeout(Duration::from_secs(5), async {
        while shard1.get_sync(&spilled_key).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        spilled,
        "the target key spills to disk once eviction runs past the weight cap"
    );

    shard1
        .insert(tombstoned_key, "gone".to_string())
        .await
        .expect("insert tombstoned");
    shard1
        .remove(&tombstoned_key)
        .await
        .expect("remove tombstoned");

    shard1.close_spill_checkpointed().await;

    let shard2 = Shard::<u32, String>::new(
        SmolStr::new("checkpoint-replay"),
        Mode::Local,
        node,
        2,
        None,
        None,
    );
    let warm = shard2
        .attach_spill(&cfg)
        .expect("a checkpoint snapshot within tombstone_ttl reopens warm");
    assert!(
        warm.warm_parts
            .iter()
            .any(|part| part.bucket() == resident_bucket),
        "the still-resident key's bucket replayed too, not only the spilled one"
    );
    assert!(
        warm.warm_parts
            .iter()
            .any(|part| part.bucket() == spilled_bucket)
    );

    assert_eq!(
        shard2.get(&resident_key).await,
        Some("resident-value".to_string()),
        "a key never spilled during the first life still survives the checkpoint"
    );
    assert_eq!(
        shard2.get(&spilled_key).await,
        Some("spilled-value".to_string()),
        "a key already spilled when the tier closed survives the checkpoint too"
    );
    assert_eq!(
        shard2.get(&tombstoned_key).await,
        None,
        "a key tombstoned before the close never resurrects via checkpoint replay"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins the simplest resurrection case: a spilled-then-deleted key must
/// never come back on a checkpointed reopen, with no peer or GC involved.
#[cfg(feature = "spill")]
#[tokio::test]
async fn a_deleted_spilled_key_never_resurrects_across_a_checkpointed_reopen() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-no-resurrection-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let node = NodeId::from(302u64);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("no-resurrection"),
        Mode::Local,
        node,
        1, // a tiny weight cap: the one entry spills
        None,
        None,
    );
    shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");

    let key = 42u32;
    shard1.insert(key, "v".to_string()).await.expect("insert");
    let bucket = bucket_of(&key_bytes(&key));
    let fillers: Vec<u32> = (0..1_000_000u32)
        .filter(|&k| k != key && bucket_of(&key_bytes(&k)) == bucket)
        .take(20)
        .collect();
    assert!(fillers.len() >= 8);
    for filler in fillers {
        shard1
            .insert(filler, "filler".to_string())
            .await
            .expect("insert filler");
    }
    let spilled = tokio::time::timeout(Duration::from_secs(5), async {
        while shard1.get_sync(&key).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        spilled,
        "the key spills to disk once eviction runs past the tiny weight cap"
    );

    // Deleted while still spilled: a delete never rewrites the on-disk
    // region bytes (see `engine::apply_tombstone`), so a scan-based
    // reopen would still find them.
    shard1.remove(&key).await.expect("remove the spilled key");

    shard1.close_spill_checkpointed().await;

    // Reopens well inside the default ten-minute tombstone_ttl gate.
    let shard2 = Shard::<u32, String>::new(
        SmolStr::new("no-resurrection"),
        Mode::Local,
        node,
        1,
        None,
        None,
    );
    // `bucket` still warm-reloads its live filler keys; only the deleted
    // key is checked below.
    let _warm = shard2
        .attach_spill(&cfg)
        .expect("a checkpoint snapshot within tombstone_ttl reopens warm");
    assert_eq!(
        shard2.get(&key).await,
        None,
        "the deleted key never resurrects: no peer, no anti-entropy round, no tombstone \
         GC, just replay from the checkpoint snapshot alone"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins the exact race `Engine::finalize_checkpoint_writes` closes: a
/// key deleted between resident-capture and disk-write must not survive
/// the checkpoint.
#[cfg(feature = "spill")]
#[tokio::test]
async fn close_spill_checkpoint_drops_a_key_deleted_between_capture_and_disk_write() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-close-spill-race-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let node = NodeId::from(303u64);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("close-spill-race"),
        Mode::Local,
        node,
        1 << 20, // generous cap: this test drives the checkpoint's own
        // engine methods directly, not a real eviction
        None,
        None,
    );
    shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");

    let key = 77u32;
    shard1.insert(key, "v".to_string()).await.expect("insert");

    let now = shard1.now_ms();
    // Phase 1: capture every resident live entry before anything else changes.
    let resident = shard1.engine.checkpoint_resident_entries(now);
    assert_eq!(resident.len(), 1, "the one resident key is captured");

    // The race: a second clone deletes the key between capture and the write below.
    shard1
        .remove(&key)
        .await
        .expect("remove races the checkpoint");

    let tier = Arc::clone(shard1.engine.spill().expect("attach_spill attaches a tier"));
    let sink = Arc::clone(&shard1.engine) as Arc<dyn spill::SpillSink>;
    // Phase 2: writes the now-stale bytes to disk regardless of the race.
    let flush_outcome = tier.checkpoint_flush(sink.as_ref(), resident);
    let newly_written = flush_outcome.written;
    assert_eq!(
        newly_written.len(),
        1,
        "checkpoint_flush writes the captured entry to disk with no version check, \
         exactly as its own docs describe -- this is the vulnerable write the next phase \
         must correct for"
    );

    // Phase 3, the fix: re-validates against live state and drops the
    // now-tombstoned entry.
    let (survivors, finalize_counts) = shard1.engine.finalize_checkpoint_writes(newly_written, now);
    assert!(
        survivors.is_empty(),
        "a key deleted between the capture and the checkpoint write must never survive \
         finalize_checkpoint_writes"
    );
    assert_eq!(
        finalize_counts.dropped_tombstoned, 1,
        "the race drops the entry under the tombstoned reason specifically"
    );

    let mut entries = shard1.engine.snapshot_spilled(now);
    entries.extend(survivors);
    assert!(
        entries.is_empty(),
        "the deleted key must be absent from the final checkpoint snapshot"
    );

    tier.write_checkpoint_snapshot(&entries, now);
    tier.close();

    let shard2 = Shard::<u32, String>::new(
        SmolStr::new("close-spill-race"),
        Mode::Local,
        node,
        1 << 20,
        None,
        None,
    );
    shard2
        .attach_spill(&cfg)
        .expect("a checkpoint snapshot within tombstone_ttl reopens warm");
    assert_eq!(
        shard2.get(&key).await,
        None,
        "the key deleted mid-checkpoint never resurrects on reopen"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that `finalize_checkpoint_writes` calls `note_spill_arrival`
/// itself, since it flips `Resident` to `Spilled` without going through
/// `SpillSink::install`.
#[cfg(feature = "spill")]
#[tokio::test]
async fn close_spill_checkpoint_counts_a_surviving_resident_entry_toward_the_spill_entries_metric()
{
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-checkpoint-gauge-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let node = NodeId::from(304u64);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("checkpoint-gauge"),
        Mode::Local,
        node,
        1 << 20, // generous: nothing is ever evicted by ordinary means
        None,
        None,
    )
    .with_max_resident_bytes(1 << 40);
    shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");
    shard1.insert(1u32, "v".repeat(40)).await.expect("insert");
    assert_eq!(
        shard1.engine.debug_spill_entries_count(),
        0,
        "a resident entry never counts toward sundog_spill_entries before any checkpoint"
    );
    let resident_before = shard1.resident_bytes().expect("a capped shard counts");

    shard1.close_spill_checkpointed().await;
    assert!(
        shard1.resident_bytes() < Some(resident_before),
        "the checkpoint's flip to Spilled drops the value's heap bytes"
    );
    assert_eq!(
        shard1.resident_bytes(),
        Some(shard1.engine.recount_resident_bytes())
    );

    assert_eq!(
        shard1.engine.debug_spill_entries_count(),
        1,
        "close_spill_checkpointed's checkpoint flips the one resident entry to Spilled via \
         Engine::finalize_checkpoint_writes, which must call note_spill_arrival itself \
         since it never goes through SpillSink::install"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Captures `sundog_spill_reopen_total{outcome,reason}` increments for one
/// cache name.
#[cfg(feature = "spill")]
#[derive(Clone, Default)]
struct ReopenCounts(Arc<StdMutex<HashMap<(String, String), u64>>>);

#[cfg(feature = "spill")]
impl ReopenCounts {
    fn get(&self, outcome: &str, reason: &str) -> u64 {
        *self
            .0
            .lock()
            .unwrap()
            .get(&(outcome.to_string(), reason.to_string()))
            .unwrap_or(&0)
    }
}

#[cfg(feature = "spill")]
struct ReopenOutcomeCounter {
    outcome_reason: (String, String),
    counts: ReopenCounts,
}

#[cfg(feature = "spill")]
impl metrics::CounterFn for ReopenOutcomeCounter {
    fn increment(&self, value: u64) {
        *self
            .counts
            .0
            .lock()
            .unwrap()
            .entry(self.outcome_reason.clone())
            .or_insert(0) += value;
    }

    fn absolute(&self, value: u64) {
        *self
            .counts
            .0
            .lock()
            .unwrap()
            .entry(self.outcome_reason.clone())
            .or_insert(0) = value;
    }
}

#[cfg(feature = "spill")]
struct ReopenRecorder {
    cache: String,
    counts: ReopenCounts,
}

#[cfg(feature = "spill")]
impl metrics::Recorder for ReopenRecorder {
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
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        let this_cache = key
            .labels()
            .any(|l| l.key() == "cache" && l.value() == self.cache);
        if key.name() != "sundog_spill_reopen_total" || !this_cache {
            return metrics::Counter::noop();
        }
        let outcome = key
            .labels()
            .find(|l| l.key() == "outcome")
            .map(|l| l.value().to_string())
            .unwrap_or_default();
        let reason = key
            .labels()
            .find(|l| l.key() == "reason")
            .map(|l| l.value().to_string())
            .unwrap_or_default();
        metrics::Counter::from_arc(Arc::new(ReopenOutcomeCounter {
            outcome_reason: (outcome, reason),
            counts: self.counts.clone(),
        }))
    }

    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// Pins that `attach_spill` records a `stale_snapshot` cold fallback
/// under `sundog_spill_reopen_total`, the one reopen reason the exporter
/// test cannot reach.
#[cfg(feature = "spill")]
#[test]
fn attach_spill_records_a_stale_snapshot_cold_fallback_under_its_metric() {
    const CACHE: &str = "attach-spill-stale-snapshot-metric";
    let counts = ReopenCounts::default();
    // The global recorder slot is process-wide; if another test already
    // won it, this one observes nothing and skips its assertions.
    let installed = metrics::set_global_recorder(ReopenRecorder {
        cache: CACHE.to_string(),
        counts: counts.clone(),
    })
    .is_ok();

    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-stale-snapshot-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let region_bytes = 4096u64;
    let capacity_bytes = 1u64 << 20;
    let cfg = spill::SpillConfig::new(&dir, capacity_bytes)
        .region_bytes(region_bytes)
        .warm_reopen(true);
    let cache_dir = dir.join(CACHE);
    std::fs::create_dir_all(&cache_dir).expect("scratch tier directory creates");
    let region_count = spill::region_count_for(capacity_bytes, region_bytes).max(2);
    spill::write_stale_snapshot_for_test(&cache_dir, region_bytes, region_count, 1_000);

    let s = Shard::<u32, String>::new(
        SmolStr::new(CACHE),
        Mode::Local,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    let warm = s
        .attach_spill(&cfg)
        .expect("the cold fallback still succeeds even against a stale snapshot");
    assert!(
        warm.warm_parts.is_empty(),
        "a stale snapshot never reopens warm, so nothing is in warm_parts"
    );

    if installed {
        assert_eq!(
            counts.get("cold_fallback", "stale_snapshot"),
            1,
            "sundog_spill_reopen_total{{outcome=\"cold_fallback\", \
             reason=\"stale_snapshot\"}} records the stale-snapshot cold fallback"
        );
        assert_eq!(
            counts.get("warm", ""),
            0,
            "a stale snapshot never records a warm reopen"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that reattaching with no clean close in between (a crash) falls
/// back cold with reason `"no_snapshot"`.
#[cfg(feature = "spill")]
#[test]
fn attach_spill_twice_with_no_close_between_falls_back_cold_with_reason_no_snapshot() {
    const CACHE: &str = "attach-spill-twice-no-close";
    let counts = ReopenCounts::default();
    // The global recorder slot is process-wide; if already won, this
    // test skips its assertions.
    let installed = metrics::set_global_recorder(ReopenRecorder {
        cache: CACHE.to_string(),
        counts: counts.clone(),
    })
    .is_ok();

    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-twice-no-close-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new(CACHE),
        Mode::Local,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    let warm1 = shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");
    assert!(warm1.warm_parts.is_empty());
    // Life 1 ends with no checkpointed close: the tier drops, writing no
    // snapshot.
    drop(shard1);

    let shard2 = Shard::<u32, String>::new(
        SmolStr::new(CACHE),
        Mode::Local,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    let warm2 = shard2
        .attach_spill(&cfg)
        .expect("the cold fallback still succeeds against a directory with no snapshot");
    assert!(
        warm2.warm_parts.is_empty(),
        "no snapshot exists to warm-reopen from"
    );

    if installed {
        assert_eq!(
            counts.get("cold_fallback", "no_snapshot"),
            2,
            "sundog_spill_reopen_total{{outcome=\"cold_fallback\", \
             reason=\"no_snapshot\"}} records both lives' cold fallback: the first life's \
             directory starts out with no snapshot exactly as much as the second life's \
             does after a close with no checkpoint"
        );
        assert_eq!(counts.get("warm", ""), 0, "no snapshot ever reopens warm");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// A resolver where the longer value wins, `Hlc` breaking ties on equal
/// length. `(len, ver)` compared lexicographically satisfies
/// `ConflictResolver::winner`'s contract.
#[derive(Debug, Clone, Copy)]
struct LongestValueWins;

impl ConflictResolver for LongestValueWins {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        let len_a = a.value.map_or(0, <[u8]>::len);
        let len_b = b.value.map_or(0, <[u8]>::len);
        match len_a.cmp(&len_b) {
            std::cmp::Ordering::Greater => Winner::A,
            std::cmp::Ordering::Less => Winner::B,
            std::cmp::Ordering::Equal => {
                if a.ver >= b.ver {
                    Winner::A
                } else {
                    Winner::B
                }
            }
        }
    }
}

/// Always prefers whichever record is passed as `a`, violating
/// `ConflictResolver::winner`'s antisymmetry requirement.
#[derive(Debug, Clone, Copy)]
struct AlwaysA;

impl ConflictResolver for AlwaysA {
    fn winner(&self, _key: &[u8], _a: RecordView<'_>, _b: RecordView<'_>) -> Winner {
        Winner::A
    }
}

#[test]
fn longest_value_wins_is_antisymmetric_over_sample_pairs() {
    let resolver = LongestValueWins;
    let samples = [
        RecordView {
            value: Some(b"a"),
            ver: hlc(1, 1),
            expires_at_ms: None,
        },
        RecordView {
            value: Some(b"abc"),
            ver: hlc(2, 2),
            expires_at_ms: None,
        },
        RecordView {
            value: None,
            ver: hlc(3, 1),
            expires_at_ms: None,
        },
        RecordView {
            value: Some(b"ab"),
            ver: hlc(1, 9),
            expires_at_ms: None,
        },
        // Same length as "ab" above, different version: the only pair
        // here that exercises the equal-length tie-break by `Hlc`.
        RecordView {
            value: Some(b"zz"),
            ver: hlc(5, 3),
            expires_at_ms: None,
        },
    ];
    for &a in &samples {
        for &b in &samples {
            if a.ver == b.ver {
                continue; // Shard::apply never asks about equal versions.
            }
            let ab = resolver.winner(b"k", a, b);
            let ba = resolver.winner(b"k", b, a);
            assert_ne!(ab, ba, "swapping the arguments swaps the winner");
        }
    }

    // `AlwaysA` is exactly the antisymmetry violation the trait docs
    // warn about: swapping the arguments does not swap the winner.
    let broken = AlwaysA;
    let (x, y) = (samples[0], samples[1]);
    assert_eq!(broken.winner(b"k", x, y), Winner::A);
    assert_eq!(broken.winner(b"k", y, x), Winner::A);
}

#[test]
fn merged_carries_the_resolvers_value_and_ttl() {
    let merged = Merged {
        value: Bytes::from_static(b"merged-bytes"),
        expires_at_ms: Some(42),
    };
    let Merged {
        value,
        expires_at_ms,
    } = merged;
    assert_eq!(value, Bytes::from_static(b"merged-bytes"));
    assert_eq!(expires_at_ms, Some(42));
}

#[test]
fn merged_equality_compares_value_and_ttl() {
    let a = Merged {
        value: Bytes::from_static(b"x"),
        expires_at_ms: None,
    };
    let b = Merged {
        value: Bytes::from_static(b"x"),
        expires_at_ms: None,
    };
    let different_value = Merged {
        value: Bytes::from_static(b"y"),
        expires_at_ms: None,
    };
    let different_ttl = Merged {
        value: Bytes::from_static(b"x"),
        expires_at_ms: Some(1),
    };
    assert_eq!(a, b);
    assert_ne!(a, different_value);
    assert_ne!(a, different_ttl);
}

#[test]
fn conflict_resolver_merges_defaults_to_false_and_is_overridable() {
    assert!(
        !LwwResolver.merges(),
        "the default resolver never returns Merged"
    );
    assert!(
        !LongestValueWins.merges(),
        "a custom A/B-only resolver inherits the false default without overriding it"
    );
    assert!(
        AlwaysMerge {
            value: Bytes::from_static(b"x"),
            expires_at_ms: None,
        }
        .merges(),
        "a resolver that returns Merged overrides merges() to true"
    );
}

#[test]
fn conflict_resolver_compact_defaults_to_none_and_is_overridable() {
    let retire_everyone = |_: crdt::WriterId| true;
    assert_eq!(
        LwwResolver.compact(
            b"k",
            b"v",
            0,
            &retire_everyone,
            Quiescence::Settled,
            CompactionBounds::three_bounds(0)
        ),
        None,
        "the default resolver never compacts, regardless of key/value/\
         now/retire/quiet/bound"
    );
    assert_eq!(
        LongestValueWins.compact(
            b"k",
            b"v",
            12_345,
            &retire_everyone,
            Quiescence::Churning,
            CompactionBounds::three_bounds(999)
        ),
        None,
        "a custom winner-only resolver inherits the None default without \
         overriding compact"
    );

    let retire_none = |_: crdt::WriterId| false;
    let out = AlwaysCompacts
        .compact(
            b"key",
            b"val",
            42,
            &retire_none,
            Quiescence::Settled,
            CompactionBounds::three_bounds(100),
        )
        .expect("a resolver that overrides compact can return Some");
    assert_eq!(
        out.as_ref(),
        b"key:val|42|false|Settled|100",
        "every parameter compact was called with reaches the resolver's \
         own implementation"
    );
}

#[tokio::test]
async fn shard_ops_compact_pass_defaults_to_a_no_op() {
    let s = shard::<u32, String>(1);
    s.insert(1, "v".to_string()).await.expect("insert");
    let writer = crdt::WriterId::new(NodeId::from(9), 1);
    let retire = move |w: crdt::WriterId| w == writer;

    let CompactPassOutcome {
        retired, compacted, ..
    } = ShardOps::compact_pass(
        &s,
        0,
        &retire,
        Quiescence::Settled,
        CompactionBounds::three_bounds(0),
        100,
    )
    .await;

    assert!(
        retired.is_empty(),
        "a shard with no resolver set (LwwResolver) never retires a writer"
    );
    assert_eq!(compacted, 0, "the default compact_pass compacts nothing");
    assert_eq!(
        s.get(&1).await,
        Some("v".to_string()),
        "compact_pass's no-op default never touches the resident value"
    );
}

#[tokio::test]
async fn shard_merges_reflects_the_resolvers_merges_flag() {
    let default_resolver = shard::<u32, Vec<u8>>(1);
    assert!(
        !ShardOps::merges(&default_resolver),
        "a shard with no resolver set carries LwwResolver's false"
    );

    let merging = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from_static(b"x"),
        expires_at_ms: None,
    }));
    assert!(
        ShardOps::merges(&merging),
        "a shard forwards its resolver's merges() through ShardOps"
    );
}

#[tokio::test]
async fn custom_resolver_overrides_plain_hlc_order() {
    let s = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(LongestValueWins));

    let long_but_old = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(
            postcard::to_stdvec(&vec![0u8; 10]).expect("encode"),
        )),
        ver: hlc(100, 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, long_but_old).await;
    assert_eq!(s.get(&1).await, Some(vec![0u8; 10]));

    // Strictly newer by Hlc, but shorter: loses under LongestValueWins.
    let short_but_new = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(
            postcard::to_stdvec(&vec![0u8; 3]).expect("encode"),
        )),
        ver: hlc(200, 3),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, short_but_new).await;
    assert_eq!(
        s.get(&1).await,
        Some(vec![0u8; 10]),
        "the default Hlc-only rule does not apply once a custom resolver is installed"
    );

    // Longer *and* newer: wins outright.
    let long_and_new = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(
            postcard::to_stdvec(&vec![0u8; 20]).expect("encode"),
        )),
        ver: hlc(300, 3),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, long_and_new).await;
    assert_eq!(s.get(&1).await, Some(vec![0u8; 20]));
}

/// The permutation-convergence property holds for a custom resolver too,
/// not only the default one.
#[tokio::test]
async fn custom_resolver_converges_across_permutations_and_duplicates() {
    let a = shard::<u32, Vec<u8>>(10).with_resolver(Arc::new(LongestValueWins));
    let b = shard::<u32, Vec<u8>>(20).with_resolver(Arc::new(LongestValueWins));

    let mut records = Vec::new();
    for i in 0..6u64 {
        let key = u32::try_from(i % 3).expect("small");
        let len = usize::try_from((i * 7) % 11).expect("small");
        let value = vec![u8::try_from(i).expect("small"); len];
        records.push(WireRecord {
            key: key_bytes(&key),
            value: Some(Bytes::from(postcard::to_stdvec(&value).expect("encode"))),
            ver: hlc(1_000 + i * 3, i % 4 + 1),
            expires_at_ms: None,
        });
    }
    records.push(WireRecord {
        key: key_bytes(&1u32),
        value: None,
        ver: hlc(1_100, 9),
        expires_at_ms: None,
    });

    for rec in &records {
        ShardOps::apply_remote(&a, rec.clone()).await;
    }
    // Reverse order, each record applied twice: reordering and
    // duplication together, the two hazards anti-entropy tolerates.
    for rec in records.iter().rev() {
        ShardOps::apply_remote(&b, rec.clone()).await;
        ShardOps::apply_remote(&b, rec.clone()).await;
    }

    for key in 0..3u32 {
        assert_eq!(
            a.get(&key).await,
            b.get(&key).await,
            "key {key} diverged across permutations under a shared custom resolver"
        );
    }
    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&b).await,
        "digests diverged between replicas under the same custom resolver"
    );
}

/// A resolver whose `merge` always claims to have folded both sides into
/// fixed bytes, regardless of whether those bytes decode as the shard's
/// value type, and whose `winner` falls back to plain `Hlc` order, like
/// `LwwResolver`. `resolve_conflict` itself is private to `engine.rs`, so
/// this is how its engine-enforced value-presence guard and
/// decode-failure rejection get exercised from out here, through the
/// public `Shard`/`ShardOps` surface only.
#[derive(Debug, Clone)]
struct AlwaysMerge {
    value: Bytes,
    expires_at_ms: Option<u64>,
}

impl ConflictResolver for AlwaysMerge {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], _a: RecordView<'_>, _b: RecordView<'_>) -> Option<Merged> {
        Some(Merged {
            value: self.value.clone(),
            expires_at_ms: self.expires_at_ms,
        })
    }
}

/// A resolver whose `compact` always claims to have folded something,
/// echoing every parameter it was called with into the returned bytes
/// so a test can assert each one actually reached the implementation,
/// not merely that `Some`/`None` came back.
#[derive(Debug, Clone, Copy)]
struct AlwaysCompacts;

impl ConflictResolver for AlwaysCompacts {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn compact(
        &self,
        key: &[u8],
        value: &[u8],
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
    ) -> Option<Bytes> {
        let retired_probe = retire(crdt::WriterId::new(NodeId::from(1), 1));
        Some(Bytes::from(format!(
            "{}|{now_ms}|{retired_probe}|{quiet:?}|{}",
            String::from_utf8_lossy(key).into_owned() + ":" + &String::from_utf8_lossy(value),
            bounds.retire_after_ms
        )))
    }
}

/// [`ShardOps::compact_pass`]'s real, non-default implementation on
/// [`Shard`]: shard-level plumbing tests covering a fake retire
/// predicate injected directly, the compacted record's shape, its
/// unchanged `Hlc` and lack of any `Event`, the `Mode::Distributed`
/// skip-not-forward rule for an unowned bucket, the spilled-entry
/// skip, rate-limited cursor rotation across several calls with
/// returned counts matching actual work, the non-merging early-out,
/// and two shards compacting independently converging once
/// cross-applied.
mod compact_pass_tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    fn writer(id: u64) -> crdt::WriterId {
        crdt::WriterId::new(NodeId::from(id), 1)
    }

    #[tokio::test]
    async fn folds_a_dead_writer_in_place_with_no_event_or_version_change() {
        let s = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
        let dead = writer(11);
        let alive = writer(22);
        s.merge(1, PnCounter::local_delta(dead, 3))
            .await
            .expect("first writer's contribution");
        s.merge(1, PnCounter::local_delta(alive, 4))
            .await
            .expect("second writer's contribution");
        assert_eq!(s.get(&1).await.map(|c| c.value()), Some(7));
        let before = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
        assert_eq!(before.len(), 1);

        let mut events = s.events();
        let retire = move |w: crdt::WriterId| w == dead;
        // A `bound_ms` far larger than any `now_ms` gap this test ever
        // spans: stage two only folds a retired entry once it has aged
        // past `2 * bound_ms`, and this test means to isolate stage
        // one's own idempotency, not stage two's separate threshold.
        let CompactPassOutcome {
            retired, compacted, ..
        } = ShardOps::compact_pass(
            &s,
            1_000,
            &retire,
            Quiescence::Settled,
            CompactionBounds::three_bounds(1_000_000_000),
            100,
        )
        .await;

        assert_eq!(
            retired,
            vec![dead],
            "exactly the one dead writer is reported"
        );
        assert_eq!(compacted, 1, "exactly the one touched record is counted");
        assert_eq!(
            s.get(&1).await.map(|c| c.value()),
            Some(7),
            "folding a writer's slot into its retired total never changes the value"
        );
        assert!(
            matches!(
                events.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "compaction applies through `Engine::compact_replace_if_current`, a direct \
             in-place replace, never `Shard::apply` — no `Event` fires for it, unlike \
             every ordinary write"
        );
        let after = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
        assert_eq!(after.len(), 1);
        assert!(
            after[0].ver > before[0].ver,
            "the version-gated direct replace still mints a fresh, strictly greater \
             version — never leaving the old one in place — so a version alone still \
             uniquely names this content, exactly as `resolve_and_rebind`'s own \
             equal-version no-op rule requires"
        );
        assert_ne!(
            after[0].value, before[0].value,
            "the resident bytes themselves change shape once a writer is retired"
        );

        // Idempotent: the record now holds nothing live for that writer,
        // so a second pass with the identical retire predicate finds
        // nothing left to do.
        let CompactPassOutcome {
            retired: retired_again,
            compacted: compacted_again,
            ..
        } = ShardOps::compact_pass(
            &s,
            2_000,
            &retire,
            Quiescence::Settled,
            CompactionBounds::three_bounds(1_000_000_000),
            100,
        )
        .await;
        assert!(retired_again.is_empty(), "{retired_again:?}");
        assert_eq!(compacted_again, 0);
    }

    #[tokio::test]
    async fn never_calls_a_non_merging_resolvers_compact() {
        struct SpyNeverMerges {
            calls: Arc<AtomicUsize>,
        }
        impl ConflictResolver for SpyNeverMerges {
            fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
                if a.ver >= b.ver { Winner::A } else { Winner::B }
            }
            // `merges()` is left at its `false` default: this resolver
            // never claims to merge.
            fn compact(
                &self,
                _key: &[u8],
                _value: &[u8],
                _now_ms: u64,
                _retire: &dyn Fn(crdt::WriterId) -> bool,
                _quiet: Quiescence,
                _bounds: CompactionBounds,
            ) -> Option<Bytes> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Some(Bytes::from_static(b"should never be reached"))
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let s = shard::<u32, String>(1).with_resolver(Arc::new(SpyNeverMerges {
            calls: Arc::clone(&calls),
        }));
        s.insert(1, "v".to_string()).await.expect("insert");

        let retire_everyone = |_: crdt::WriterId| true;
        let CompactPassOutcome {
            retired, compacted, ..
        } = ShardOps::compact_pass(
            &s,
            0,
            &retire_everyone,
            Quiescence::Settled,
            CompactionBounds::three_bounds(0),
            100,
        )
        .await;

        assert!(retired.is_empty(), "{retired:?}");
        assert_eq!(compacted, 0);
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "ConflictResolver::compact's own contract (\"only ever called while merges() \
             is true\") is enforced here, not merely assumed: an override that would \
             otherwise happily compact is never even invoked for a non-merging resolver"
        );
    }

    #[tokio::test]
    async fn skips_a_bucket_the_current_view_no_longer_owns() {
        let self_node = NodeId::from(1);
        let residency = Arc::new(ResidencySet::new());
        let (s, initial_view, tx) = distributed_shard::<u32, PnCounter>(
            self_node,
            vec![self_node],
            1,
            Arc::clone(&residency),
        );
        let s = s.with_resolver(Arc::new(PnCounterResolver));

        let new_view = OwnershipView::compute(
            self_node,
            vec![self_node, NodeId::from(2)],
            NonZeroU8::new(1).expect("nonzero"),
        );
        let key = (0..1_000_000u32)
            .find(|&k| !new_view.owns(PartId::of_key(&key_bytes(&k))))
            .expect("some bucket flips ownership once a second node joins");
        let bucket = PartId::of_key(&key_bytes(&key));
        assert!(initial_view.owns(bucket));

        let dead = writer(11);
        s.merge(key, PnCounter::local_delta(dead, 3))
            .await
            .expect("owner's merge lands");
        let before = ShardOps::records_for(&s, vec![key_bytes(&key)]).await;
        assert_eq!(before.len(), 1);
        // Drains the setup write's own `FanOutItem::Applied` entry so
        // the check below only ever sees what `compact_pass` itself
        // did to the queue, never the ordinary write that seeded it.
        let _ = s.fan_out.drain();

        // Membership widens and this bucket moves to the other node; the
        // shard's own physical copy is still sitting right there, mid
        // disown-grace, exactly as `a_releasing_bucket_still_answers_
        // digests_and_entries` sets up for the ordinary write path.
        // `mark_releasing` keeps the bucket servable by `records_for`'s
        // own outbound-serving guard below, so the *before*/*after*
        // comparison actually reads the resident record rather than
        // the guard's "not resident at all" empty answer either time.
        tx.send(Arc::new(new_view)).expect("receiver still alive");
        residency.mark_releasing(&[bucket]);
        assert!(
            !ShardOps::ownership_view(&s).expect("attached").owns(bucket),
            "sanity: this node no longer owns the bucket under the new view"
        );

        let retire = move |w: crdt::WriterId| w == dead;
        let CompactPassOutcome {
            retired: _retired,
            compacted,
            ..
        } = ShardOps::compact_pass(
            &s,
            1_000,
            &retire,
            Quiescence::Settled,
            CompactionBounds::three_bounds(1),
            100,
        )
        .await;

        assert_eq!(
            compacted, 0,
            "a compaction candidate for a bucket this shard's current view no longer \
             owns is never applied"
        );
        assert!(
            s.fan_out.drain().is_empty(),
            "unlike an ordinary write, a skipped compaction candidate is never forwarded \
             to the bucket's new owners either"
        );
        let after = ShardOps::records_for(&s, vec![key_bytes(&key)]).await;
        assert_eq!(
            before, after,
            "the unowned bucket's resident record is left completely untouched"
        );
    }

    #[tokio::test]
    async fn processes_the_whole_shard_over_several_rate_limited_calls() {
        const KEYS: u32 = 12;
        let s = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
        let dead = writer(9);
        let alive = writer(99);
        for k in 0..KEYS {
            s.merge(k, PnCounter::local_delta(dead, 1))
                .await
                .expect("merge");
            s.merge(k, PnCounter::local_delta(alive, 1))
                .await
                .expect("merge");
        }

        let retire = move |w: crdt::WriterId| w == dead;
        let mut total_compacted = 0usize;
        let mut all_retired: HashSet<crdt::WriterId> = HashSet::new();
        for _ in 0..BUCKET_COUNT {
            let CompactPassOutcome {
                retired, compacted, ..
            } = ShardOps::compact_pass(
                &s,
                1_000,
                &retire,
                Quiescence::Settled,
                CompactionBounds::three_bounds(1),
                3,
            )
            .await;
            total_compacted += compacted;
            all_retired.extend(retired);
            if total_compacted >= KEYS as usize {
                break;
            }
        }

        assert_eq!(
            total_compacted, KEYS as usize,
            "a small per-tick budget still processes every candidate, given enough \
             rate-limited calls — the returned counts sum to exactly the work actually \
             done, never more and never less"
        );
        assert_eq!(
            all_retired,
            HashSet::from([dead]),
            "the same writer, dead in every one of the twelve records, is reported \
             exactly once across the whole run, not once per record"
        );
        for k in 0..KEYS {
            assert_eq!(
                s.get(&k).await.map(|c| c.value()),
                Some(2),
                "key {k}'s value is unaffected by compaction"
            );
        }
    }

    #[cfg(feature = "spill")]
    #[tokio::test]
    async fn never_hands_a_spilled_entry_to_the_resolver() {
        use crate::store::spill::SpillLoc;

        let s = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
        let dead = writer(9);

        // An ordinary resident entry.
        s.merge(2, PnCounter::local_delta(dead, 4))
            .await
            .expect("merge");

        // A fabricated spilled entry for the very same dead writer,
        // bypassing any real spill tier: `Engine::compact` (and
        // therefore `Shard::compact_pass`, which never reads a stripe
        // directly) must skip it regardless of what its bytes would
        // decode to.
        s.engine.debug_insert_spilled(
            &key_bytes(&1u32),
            hlc(1, 1),
            None,
            SpillLoc {
                region: 0,
                offset: 0,
                len: 4,
                generation: 0,
            },
            0,
        );

        let retire = move |w: crdt::WriterId| w == dead;
        let CompactPassOutcome {
            retired, compacted, ..
        } = ShardOps::compact_pass(
            &s,
            1_000,
            &retire,
            Quiescence::Settled,
            CompactionBounds::three_bounds(1),
            100,
        )
        .await;

        assert_eq!(
            compacted, 1,
            "the spilled entry is skipped; only the resident one is compacted"
        );
        assert_eq!(retired, vec![dead]);
        assert_eq!(
            s.get(&2).await.map(|c| c.value()),
            Some(4),
            "the resident counter's own value is unchanged by compaction"
        );
    }

    /// Two replicas, holding the same pre-compaction state,
    /// independently decide to retire a *different* writer each,
    /// through this task's own real `Shard::compact_pass` plumbing, not
    /// `PnCounter::compact` called directly. Cross-applying each
    /// shard's resulting record through the other's ordinary
    /// replicated-apply path converges both to the exact combined
    /// total, and to bit-identical resident state, not merely an equal
    /// `.value()`.
    #[tokio::test]
    /// Two replicas compacting byte-identical records at the same
    /// version mint the same version, so their digests keep agreeing
    /// and anti-entropy has nothing to exchange after a pass.
    async fn two_replicas_compacting_identical_records_mint_identical_versions() {
        let a = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
        let b = shard::<u32, PnCounter>(2).with_resolver(Arc::new(PnCounterResolver));
        let dead = writer(9);
        let alive = writer(99);
        for k in 0..5u32 {
            a.merge(k, PnCounter::local_delta(dead, 3))
                .await
                .expect("merge");
            a.merge(k, PnCounter::local_delta(alive, 4))
                .await
                .expect("merge");
        }
        let keys: Vec<Bytes> = (0..5u32).map(|k| key_bytes(&k)).collect();
        for rec in ShardOps::records_for(&a, keys).await {
            ShardOps::apply_remote(&b, rec).await;
        }
        assert_eq!(
            ShardOps::digests(&a).await,
            ShardOps::digests(&b).await,
            "b warmed from a's records, versions included"
        );

        let retire = move |w: crdt::WriterId| w == dead;
        let bounds = CompactionBounds::three_bounds(1);
        let outcome_a =
            ShardOps::compact_pass(&a, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
        let outcome_b =
            ShardOps::compact_pass(&b, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
        assert_eq!((outcome_a.compacted, outcome_b.compacted), (5, 5));
        assert_eq!(
            ShardOps::digests(&a).await,
            ShardOps::digests(&b).await,
            "the same compaction on the same records mints the same versions on both"
        );
    }

    #[tokio::test]
    /// A merge apply drops a fold receipt older than the receipt ttl even
    /// when a peer's copy carries it back in, so two replicas whose sweeps
    /// prune at different moments still converge on the receipt-free
    /// bytes instead of re-importing each other's receipts forever.
    async fn merge_apply_settles_an_aged_receipt_a_peer_carries_back_in() {
        let bounds = CompactionBounds::three_bounds(1_000);
        let now = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let clock = {
            let now = Arc::clone(&now);
            Arc::new(move || now.load(std::sync::atomic::Ordering::Relaxed))
                as Arc<dyn Fn() -> u64 + Send + Sync>
        };
        let s = shard::<u32, PnCounter>(1)
            .with_resolver(Arc::new(PnCounterResolver))
            .with_crdt_bounds(bounds)
            .with_clock(clock);
        let dead = writer(9);
        let alive = writer(99);
        s.merge(1, PnCounter::local_delta(dead, 3))
            .await
            .expect("merge");
        s.merge(1, PnCounter::local_delta(alive, 4))
            .await
            .expect("merge");

        let retire = move |w: crdt::WriterId| w == dead;
        // Stage one at 1_000, stage two past two bounds: a receipt at 1_000.
        ShardOps::compact_pass(&s, 1_000, &retire, Quiescence::Settled, bounds, 100).await;
        let folded =
            ShardOps::compact_pass(&s, 3_001, &retire, Quiescence::Settled, bounds, 100).await;
        assert_eq!(folded.compacted, 1, "folded with a receipt");
        let with_receipt = ShardOps::records_for(&s, vec![key_bytes(&1u32)])
            .await
            .into_iter()
            .next()
            .expect("resident");
        // Stage three past three bounds: the receipt is gone locally.
        let pruned =
            ShardOps::compact_pass(&s, 4_001, &retire, Quiescence::Settled, bounds, 100).await;
        assert_eq!(pruned.compacted, 1, "receipt pruned");
        let pruned_bytes = s.get(&1).await.expect("resident").encode().expect("encode");

        // A peer's copy still carrying the receipt merges in at a time
        // past the ttl: the merge unions the receipt, settle drops it.
        now.store(4_001, std::sync::atomic::Ordering::Relaxed);
        ShardOps::apply_remote(&s, with_receipt).await;
        let after = s.get(&1).await.expect("resident");
        assert_eq!(after.value(), 7, "the exact total is untouched");
        assert_eq!(
            after.encode().expect("encode"),
            pruned_bytes,
            "the receipt the peer carried back in is settled away on apply"
        );
    }

    #[tokio::test]
    /// A record that another write touched between the scan and the
    /// version-gated apply is skipped that pass and rewritten on the
    /// next: the writer stays eligible, so nothing is left behind.
    async fn a_record_touched_between_scan_and_apply_is_retried_on_the_next_pass() {
        let s = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
        let dead = writer(9);
        let alive = writer(99);
        s.merge(1, PnCounter::local_delta(dead, 5))
            .await
            .expect("merge");
        s.merge(1, PnCounter::local_delta(alive, 1))
            .await
            .expect("merge");
        let retire = move |w: crdt::WriterId| w == dead;
        let bounds = CompactionBounds::three_bounds(1);

        let scan = s.engine.compact(
            s.resolver.as_ref(),
            1_000,
            &retire,
            Quiescence::Settled,
            bounds,
            100,
        );
        let engine::CompactCandidate {
            key,
            key_bytes,
            stale_ver,
            new_encoded,
        } = scan
            .candidates
            .into_iter()
            .next()
            .expect("the dead writer's record is a candidate");
        // A concurrent write lands before the apply.
        s.merge(1, PnCounter::local_delta(alive, 2))
            .await
            .expect("merge");
        let value = PnCounter::decode(&new_encoded).expect("decodes");
        let applied = s.engine.compact_replace_if_current(engine::CompactReplace {
            key: &key,
            key_bytes: key_bytes.as_ref(),
            hash: engine::hash_key_bytes(key_bytes.as_ref()),
            expected_ver: stale_ver,
            new_ver: compacted_version(stale_ver, new_encoded.as_ref()),
            value,
            encoded: new_encoded,
        });
        assert!(!applied, "the stale candidate is refused");
        assert_eq!(s.get(&1).await.map(|c| c.value()), Some(7));

        let next =
            ShardOps::compact_pass(&s, 1_001, &retire, Quiescence::Settled, bounds, 100).await;
        assert_eq!(next.retired, vec![dead], "still eligible on the next pass");
        assert_eq!(
            next.compacted, 1,
            "rewritten with the concurrent write folded in"
        );
        assert_eq!(s.get(&1).await.map(|c| c.value()), Some(7));
    }

    #[tokio::test]
    async fn two_shards_compacting_independently_converge() {
        let a = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
        let b = shard::<u32, PnCounter>(2).with_resolver(Arc::new(PnCounterResolver));
        let w1 = writer(101);
        let w2 = writer(202);

        for shard in [&a, &b] {
            shard
                .merge(1, PnCounter::local_delta(w1, 10))
                .await
                .expect("w1's contribution");
            shard
                .merge(1, PnCounter::local_delta(w2, 50))
                .await
                .expect("w2's contribution");
        }
        assert_eq!(a.get(&1).await.map(|c| c.value()), Some(60));
        assert_eq!(b.get(&1).await.map(|c| c.value()), Some(60));

        // Independent decisions: `a` alone judges `w1` dead, `b` alone
        // judges `w2` dead, with the same tick timestamp on both sides
        // so the two sides' `retired` entries land at the identical
        // `since_ms`.
        let retire_w1 = move |w: crdt::WriterId| w == w1;
        let retire_w2 = move |w: crdt::WriterId| w == w2;
        let CompactPassOutcome {
            retired: retired_a,
            compacted: compacted_a,
            ..
        } = ShardOps::compact_pass(
            &a,
            5_000,
            &retire_w1,
            Quiescence::Settled,
            CompactionBounds::three_bounds(1),
            100,
        )
        .await;
        let CompactPassOutcome {
            retired: retired_b,
            compacted: compacted_b,
            ..
        } = ShardOps::compact_pass(
            &b,
            5_000,
            &retire_w2,
            Quiescence::Settled,
            CompactionBounds::three_bounds(1),
            100,
        )
        .await;
        assert_eq!(retired_a, vec![w1]);
        assert_eq!(compacted_a, 1);
        assert_eq!(retired_b, vec![w2]);
        assert_eq!(compacted_b, 1);

        // Each side still reads its own exact total after compacting
        // only its own writer.
        assert_eq!(a.get(&1).await.map(|c| c.value()), Some(60));
        assert_eq!(b.get(&1).await.map(|c| c.value()), Some(60));

        let rec_a = ShardOps::records_for(&a, vec![key_bytes(&1u32)])
            .await
            .into_iter()
            .next()
            .expect("a holds the key");
        let rec_b = ShardOps::records_for(&b, vec![key_bytes(&1u32)])
            .await
            .into_iter()
            .next()
            .expect("b holds the key");

        ShardOps::apply_remote(&a, rec_b).await;
        ShardOps::apply_remote(&b, rec_a).await;

        assert_eq!(
            a.get(&1).await.map(|c| c.value()),
            Some(60),
            "cross-applying the other side's independently-compacted record recovers \
             the exact combined total on a"
        );
        assert_eq!(
            b.get(&1).await.map(|c| c.value()),
            Some(60),
            "cross-applying the other side's independently-compacted record recovers \
             the exact combined total on b"
        );
        assert_eq!(
            a.get(&1).await,
            b.get(&1).await,
            "both replicas converge to bit-identical state, not merely an equal value: \
             each now carries both writers' slots folded into its own retired map"
        );
    }
}

#[tokio::test]
async fn merge_against_a_tombstone_is_never_consulted_and_falls_back_to_hlc_order() {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from(postcard::to_stdvec(&99u32).expect("encode")),
        expires_at_ms: None,
    }));
    // Subscribed before the setup write below: a receiver created after
    // it fires would never see it and `recv().await` would hang.
    let mut events = s.events();

    // A bare tombstone for a key that has never existed: `stored_ver` is
    // `None` going in, so this never consults `AlwaysMerge` at all: it
    // would otherwise see its own tombstone as a value-less "incoming"
    // and have the engine's guard skip `merge`, same as the colliding
    // write below, which is exactly what this test means to isolate
    // instead.
    ShardOps::apply_remote(
        &s,
        WireRecord {
            key: key_bytes(&1u32),
            value: None,
            ver: hlc(1, 1),
            expires_at_ms: None,
        },
    )
    .await;
    assert_eq!(
        s.get(&1).await,
        None,
        "tombstoned before the colliding write arrives"
    );
    let _ = events
        .recv()
        .await
        .expect("Removed for the setup tombstone");

    // Genuinely newer than the tombstone by `Hlc`: the tombstone's
    // value-less side means `merge` is never consulted no matter what
    // `merges()` claims, so this lands via the ordinary Hlc-order
    // `winner` fallback, with its own real value, never `AlwaysMerge`'s
    // synthesized 99.
    let incoming = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&42u32).expect("encode"))),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, incoming).await;

    assert_eq!(
        s.get(&1).await,
        Some(42),
        "the incoming write's own value lands via the ordinary winner fallback, never the \
         resolver's synthesized merge bytes, since a tombstone's value-less side means \
         merge is never consulted"
    );
    match events
        .recv()
        .await
        .expect("an event for the colliding write")
    {
        Event::Created { key, value, origin } => {
            assert_eq!(key, 1);
            assert_eq!(value, 42);
            assert_eq!(origin, Origin::Remote(NodeId::from(2)));
        }
        other => panic!("expected Event::Created for a write over a tombstone, got {other:?}"),
    }
}

/// A spilled stored side carries a real value once `apply_locked`
/// reads its bytes back off disk (see `engine::read_spilled_for_conflict`):
/// `AlwaysMerge`'s `merge` always returns `Some`, and now that both
/// sides have real values, `resolve_conflict`'s guard has nothing to
/// stop it, so the merge applies exactly as it would against a resident
/// stored side. `merge_against_a_tombstone_is_never_consulted_and_falls_back_to_hlc_order`,
/// just above, is the guard's remaining case: a tombstone is genuinely
/// value-less, spilled or not, and `merge` is never consulted for it.
#[cfg(feature = "spill")]
#[tokio::test]
async fn merge_against_a_spilled_side_folds_once_its_bytes_are_read_back() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-merge-spill-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);

    let s = Shard::<u32, u32>::new(
        SmolStr::new("merge-spill-test"),
        Mode::Local,
        NodeId::from(1),
        1, // capacity 1: the second insert spills the first
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("the tier's directory and region files open")
    .with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from(postcard::to_stdvec(&999u32).expect("encode")),
        expires_at_ms: None,
    }));

    s.insert(1, 10).await.expect("insert 1");
    s.insert(2, 20).await.expect("insert 2");

    // Eviction to disk runs on the flusher thread, off this call's path,
    // so poll rather than assume it is already done. Polls with
    // `get_sync`, never `get`: `get` promotes a spilled entry back to
    // residency on a hit (see its docs), which would silently undo the
    // very spilled state this test needs in place when the colliding
    // write arrives below.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let spilled_key = loop {
        if s.get_sync(&1).is_none() {
            break 1u32;
        }
        if s.get_sync(&2).is_none() {
            break 2u32;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "neither key spilled within the poll bound"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    // `get_sync` never promotes, unlike `get`: reading the key here
    // would either promote the spilled entry or require touching disk,
    // either way risking exactly the promotion this test must avoid
    // before the colliding write below.
    assert!(
        s.get_sync(&spilled_key).is_none(),
        "the key must still be spilled when the colliding write arrives"
    );

    let incoming = WireRecord {
        key: key_bytes(&spilled_key),
        value: Some(Bytes::from(postcard::to_stdvec(&123u32).expect("encode"))),
        ver: hlc(ahead_ms(), 9),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, incoming).await;

    // `get` here, after the collision has already been resolved, reads
    // back whatever the merge actually stored.
    assert_eq!(
        s.get(&spilled_key).await,
        Some(999),
        "apply_locked reads the spilled stored side's real bytes back before consulting \
         the resolver, so a Merged reply against it applies exactly as it would against a \
         resident side, rather than degrading to the guard's value-less fallback"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn merge_with_bytes_that_fail_to_decode_rejects_the_incoming_record() {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from_static(&[0x80]), // truncated varint: never decodes as u32
        expires_at_ms: None,
    }));
    s.insert(1, 7).await.expect("insert");
    let (_, weight_before) = s.engine.debug_totals();

    let incoming = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&5u32).expect("encode"))),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, incoming).await;

    assert_eq!(
        s.get(&1).await,
        Some(7),
        "a Merged reply whose bytes fail to decode as V is rejected, not stored"
    );
    let (_, weight_after) = s.engine.debug_totals();
    assert_eq!(
        weight_before, weight_after,
        "a rejected decode failure leaves the engine's bookkeeping untouched"
    );
}

#[tokio::test]
async fn merged_outcome_stores_the_merged_bytes_under_a_merge_derived_version_and_publishes_one_event()
 {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from(postcard::to_stdvec(&42u32).expect("encode")),
        expires_at_ms: None,
    }));
    // Subscribed before the setup insert: a receiver created after it
    // fires would never see it and `recv().await` would hang.
    let mut events = s.events();
    s.insert(1, 7).await.expect("insert");
    let _ = events.recv().await.expect("Created for the setup insert");

    let incoming = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&5u32).expect("encode"))),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, incoming).await;

    assert_eq!(
        s.get(&1).await,
        Some(42),
        "the resolver's merged value lands, not either input's raw value"
    );

    match events.recv().await.expect("exactly one event is published") {
        Event::Updated { key, value, origin } => {
            assert_eq!(key, 1);
            assert_eq!(value, 42);
            assert_eq!(origin, Origin::Remote(NodeId::from(2)));
        }
        other => panic!("expected Event::Updated for a merge, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "a merge outcome publishes exactly one event, never a second for the same write"
    );

    let stored = s
        .engine
        .record_for(key_bytes(&1u32).as_ref(), 0)
        .expect("merged key is live");
    assert!(
        stored.ver.node.is_merge_derived(),
        "the merged bytes match neither side's own bytes, so the engine mints a fresh \
         version no real node's clock could produce"
    );
}

/// A resolver whose merge sums both sides as `u32`, so repeated folding,
/// [`Shard::merge`]'s in-memory coalescing included, is observable
/// as an actual running total rather than just "some merge happened".
/// Falls back to plain `Hlc` order when either side lacks a value,
/// matching every other merging resolver's tombstone/spill guard.
#[derive(Debug, Clone, Copy, Default)]
struct SumResolver;

impl ConflictResolver for SumResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return None;
        };
        let (Ok(x), Ok(y)) = (
            postcard::from_bytes::<u32>(av),
            postcard::from_bytes::<u32>(bv),
        ) else {
            return None;
        };
        Some(Merged {
            value: Bytes::from(postcard::to_stdvec(&(x + y)).expect("u32 always encodes")),
            expires_at_ms: None,
        })
    }
}

/// A resolver that keeps whichever side decodes to the greater `u32` and
/// never overrides [`ConflictResolver::merge`]: legal per
/// [`ConflictResolver::merges`]'s own doc ("`true` is always correct to
/// return, even for a resolver whose `merge` never actually returns
/// `Some`") and exactly the shape needed to drive [`Shard::merge`]'s
/// pending fold through a real `Winner::A`/`Winner::B` decision between
/// two real `Put` values, rather than only through the tombstone/decode-
/// failure fallback every other test resolver's `A`/`B` arm takes.
#[derive(Debug, Clone, Copy, Default)]
struct MaxWinsResolver;

impl ConflictResolver for MaxWinsResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return if a.ver >= b.ver { Winner::A } else { Winner::B };
        };
        match (
            postcard::from_bytes::<u32>(av),
            postcard::from_bytes::<u32>(bv),
        ) {
            (Ok(x), Ok(y)) if x >= y => Winner::A,
            (Ok(_), Ok(_)) => Winner::B,
            _ => {
                if a.ver >= b.ver {
                    Winner::A
                } else {
                    Winner::B
                }
            }
        }
    }

    fn merges(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn merge_a_losing_calls_own_version_never_overwrites_the_pending_entrys_version() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(MaxWinsResolver))
        .with_merge_coalesce_window(Duration::from_secs(60));

    s.merge(1, 10).await.expect("merge 1: the eventual winner");
    let (ver_after_win, _) = s
        .pending_merge_snapshot(&1)
        .expect("pending after the first call");

    // Decodes to a smaller `u32`, so `MaxWinsResolver` picks `Winner::A`
    // (the already-pending 10 over this incoming 3): a losing call.
    s.merge(1, 3).await.expect("merge 2: loses against 10");

    let (ver_now, value_now) = s
        .pending_merge_snapshot(&1)
        .expect("still pending: the window hasn't elapsed");
    assert_eq!(
        ver_now, ver_after_win,
        "a losing merge call must not advance the pending entry's version, mirroring \
         resolve_and_rebind's IncomingLoses no-op in the engine — the record's stored \
         version must keep describing its own content"
    );
    assert_eq!(
        value_now, 10,
        "nor may it touch the pending entry's content"
    );

    s.flush_all_pending_merges();
    assert_eq!(
        s.get(&1).await,
        Some(10),
        "the losing call never overwrote the winner's value once flushed either"
    );
}

/// [`Engine::apply_many`]'s pre-fold (`engine::prefold_batch`) collapses
/// several same-key records in one `apply_remote_batch` call into one
/// real apply, so a fan-out batch carrying distinct origin nodes'
/// writes to the same key publishes exactly one event, not one per
/// contributing record, the pre-pre-fold behavior, and that event
/// names only the run's last record's origin, documented on
/// [`Origin::Remote`] as an accepted cost of the fold.
#[tokio::test]
async fn apply_remote_batch_prefold_publishes_one_event_naming_the_runs_last_origin() {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(SumResolver));
    let mut events = s.events();

    let rec = |node: u64, wall_ms: u64, value: u32| WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&value).expect("encode"))),
        ver: hlc(wall_ms, node),
        expires_at_ms: None,
    };

    // Three records for one key, from three distinct origin nodes, in
    // one batch: the shape a peer's fan-out or anti-entropy-pull reply
    // bundling several nodes' writes to the same key takes.
    let recs = vec![rec(2, 1, 2), rec(3, 2, 3), rec(4, 3, 4)];
    ShardOps::apply_remote_batch(&s, recs).await;

    assert_eq!(
        s.get(&1).await,
        Some(9),
        "pre-fold still stores the full merge of every record in the run"
    );

    match events
        .recv()
        .await
        .expect("one event for the whole folded run")
    {
        Event::Created { key, value, origin } => {
            assert_eq!(key, 1);
            assert_eq!(value, 9);
            assert_eq!(
                origin,
                Origin::Remote(NodeId::from(4)),
                "the folded run's event names the run's last record's origin, not \
                 every contributor's"
            );
        }
        other => {
            panic!("expected exactly one Event::Created for the folded run, got {other:?}")
        }
    }
    assert!(
        events.try_recv().is_err(),
        "three same-key records in one fan-out batch publish exactly one event under \
         pre-fold, not three"
    );
}

#[test]
fn merge_deadline_ms_adds_the_window_to_the_start_time() {
    assert_eq!(merge_deadline_ms(1_000, Duration::from_millis(250)), 1_250);
    assert_eq!(
        merge_deadline_ms(u64::MAX, Duration::from_millis(1)),
        u64::MAX,
        "saturates rather than wrapping"
    );
}

#[test]
fn merge_window_elapsed_is_true_at_and_after_the_deadline_only() {
    assert!(!merge_window_elapsed(999, 1_000), "before the deadline");
    assert!(
        merge_window_elapsed(1_000, 1_000),
        "exactly at the deadline"
    );
    assert!(merge_window_elapsed(1_001, 1_000), "past the deadline");
}

#[test]
fn drain_due_deadlines_pops_only_the_entries_actually_due_in_deadline_order() {
    let mut by_deadline = BTreeMap::from([
        ((100, 0), "a"),
        ((50, 0), "b"),
        ((100, 1), "c"),
        ((200, 0), "d"),
    ]);

    let due = drain_due_deadlines(&mut by_deadline, 100);
    assert_eq!(
        due,
        vec!["b", "a", "c"],
        "every entry at or before the deadline, in ascending deadline order, seq breaking \
         the tie between the two sharing deadline 100"
    );
    assert_eq!(
        by_deadline,
        BTreeMap::from([((200, 0), "d")]),
        "an entry past the deadline is left untouched"
    );
}

#[test]
fn drain_due_deadlines_is_empty_when_nothing_is_due_or_pending() {
    let due = drain_due_deadlines(&mut BTreeMap::<(u64, u64), u32>::new(), 1_000);
    assert!(due.is_empty(), "{due:?}");

    let mut by_deadline = BTreeMap::from([((1_000, 0), 1u32)]);
    assert!(
        drain_due_deadlines(&mut by_deadline, 999).is_empty(),
        "nothing is due yet"
    );
    assert_eq!(by_deadline.len(), 1, "the not-yet-due entry stays put");
}

#[tokio::test]
async fn merge_with_a_zero_window_applies_at_once_like_insert() {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(SumResolver));
    let mut events = s.events();

    s.merge(1, 5).await.expect("merge");
    assert_eq!(
        s.get(&1).await,
        Some(5),
        "a zero coalesce window applies every merge call immediately"
    );
    match events
        .recv()
        .await
        .expect("one event for the immediate apply")
    {
        Event::Created { key, value, .. } => {
            assert_eq!(key, 1);
            assert_eq!(value, 5);
        }
        other => panic!("expected Event::Created, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_within_a_window_folds_in_memory_and_applies_once() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(Duration::from_secs(60));
    let mut events = s.events();

    s.merge(1, 2).await.expect("merge 1");
    s.merge(1, 3).await.expect("merge 2");
    s.merge(1, 4).await.expect("merge 3");

    assert_eq!(
        s.get(&1).await,
        None,
        "nothing has applied yet: the 60s window has not elapsed"
    );
    assert!(
        events.try_recv().is_err(),
        "nothing applies, so nothing publishes, before the flush"
    );

    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        Some(9),
        "the fold of all three calls (2 + 3 + 4) lands as one write"
    );
    match events
        .recv()
        .await
        .expect("exactly one event for the whole window")
    {
        Event::Created { key, value, .. } => {
            assert_eq!(key, 1);
            assert_eq!(value, 9);
        }
        other => panic!("expected one Event::Created for the fold, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "three merge calls in one window publish exactly one event, not three"
    );
}

#[tokio::test]
async fn merge_does_not_read_the_stored_value_before_the_window_flushes() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(Duration::from_secs(60));
    s.insert(1, 100).await.expect("setup insert");

    s.merge(1, 5).await.expect("merge");
    assert_eq!(
        s.get(&1).await,
        Some(100),
        "merge never reads the stored value before its window flushes: the pending \
         fold, not the sum, is what's in memory until then"
    );

    s.flush_all_pending_merges();
    assert_eq!(
        s.get(&1).await,
        Some(105),
        "the flush's ordinary apply folds the pending value against what was stored, \
         through the same resolver"
    );
}

#[test]
fn merge_coalesce_window_defaults_to_zero() {
    let s = shard::<u32, u32>(1);
    assert_eq!(s.merge_window(), Duration::ZERO);
}

#[test]
fn prefold_enabled_defaults_to_true_and_with_prefold_enabled_overrides_it() {
    let s = shard::<u32, u32>(1);
    assert!(
        s.prefold_enabled(),
        "pre-fold defaults to on, matching the engine's own default"
    );

    let s = s.with_prefold_enabled(false);
    assert!(!s.prefold_enabled(), "the toggle reached the engine");

    let s = s.with_prefold_enabled(true);
    assert!(s.prefold_enabled());
}

#[test]
fn shard_with_capacity_hint_presizes_engine() {
    let s = shard::<u32, u32>(1);
    assert_eq!(
        s.stripe_capacities(),
        vec![0; BUCKET_COUNT],
        "no hint yet: every stripe starts at zero allocation"
    );

    let hint = 3_000u64;
    let expected = usize::try_from(hint.div_ceil(BUCKET_COUNT as u64)).expect("hint fits usize");
    let s = s.with_capacity_hint(hint);
    assert_eq!(
        s.stripe_capacities(),
        vec![expected; BUCKET_COUNT],
        "the hint reached the engine and presized every stripe"
    );
}

#[tokio::test]
async fn with_capacity_hint_after_with_weigher_keeps_the_weigher() {
    let s = shard::<u32, Vec<u8>>(1)
        .with_weigher(|_key: &u32, value: &Vec<u8>| u32::try_from(value.len()).unwrap_or(u32::MAX))
        .with_capacity_hint(10);
    s.insert(1, vec![0u8; 7]).await.expect("insert");
    let (_, weight) = s.engine.debug_totals();
    assert_eq!(
        weight, 7,
        "with_capacity_hint's rebuild must carry an already-installed weigher forward \
         instead of reverting to the default of 1 per entry"
    );
}

#[test]
fn with_merge_coalesce_window_overrides_the_default() {
    let s = shard::<u32, u32>(1).with_merge_coalesce_window(Duration::from_millis(7));
    assert_eq!(s.merge_window(), Duration::from_millis(7));
}

#[tokio::test]
async fn flush_due_pending_merges_only_flushes_once_the_deadline_is_reached() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_clock(Arc::new(|| 0u64))
        .with_merge_coalesce_window(Duration::from_millis(100));

    s.merge(1, 1).await.expect("merge");
    let deadline = s.next_pending_merge_deadline_ms().expect("one key pending");
    assert_eq!(deadline, 100);

    s.flush_due_pending_merges(deadline - 1);
    assert_eq!(s.get(&1).await, None, "not due yet");

    s.flush_due_pending_merges(deadline);
    assert_eq!(s.get(&1).await, Some(1), "due exactly at the deadline");
}

#[tokio::test]
async fn merge_flushes_pending_folds_on_drop() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(Duration::from_secs(60));
    // Subscribed before the merges below: a receiver created after they
    // fire would never see the flush's event.
    let mut events = s.events();

    s.merge(1, 2).await.expect("merge 1");
    s.merge(1, 3).await.expect("merge 2");
    assert!(
        events.try_recv().is_err(),
        "still pending: nothing has applied yet"
    );

    drop(s);

    match events.recv().await.expect("drop flushes the pending fold") {
        Event::Created { key, value, .. } => {
            assert_eq!(key, 1);
            assert_eq!(value, 5);
        }
        other => panic!("expected Event::Created from the drop flush, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_falls_back_to_hlc_order_when_the_resolver_does_not_merge() {
    // `Shard::with_merge_coalesce_window` is a raw setter with no
    // validation of its own: `crate::cache::CacheBuilder` is the
    // validated entry point, so a non-merging resolver combined with a
    // window must still behave sanely: the pending fold degrades to
    // picking a side, never panicking.
    let s = shard::<u32, u32>(1).with_merge_coalesce_window(Duration::from_secs(60));

    s.merge(1, 1).await.expect("merge 1");
    s.merge(1, 2).await.expect("merge 2");
    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        Some(2),
        "LwwResolver::winner picks a side by Hlc order; the later call's own stamp wins"
    );
}

#[tokio::test]
async fn contains_key_reflects_insert_remove_and_ttl_expiry() {
    let s = shard::<u32, String>(1);
    assert!(!s.contains_key(&1).await, "missing key");

    s.insert(1, "a".into()).await.expect("insert");
    assert!(s.contains_key(&1).await, "present after insert");

    s.remove(&1).await.expect("remove");
    assert!(!s.contains_key(&1).await, "gone after remove");

    let ttl_shard = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_millis(50)),
        None,
    );
    ttl_shard
        .insert(2, "short-lived".into())
        .await
        .expect("insert with default ttl");
    assert!(ttl_shard.contains_key(&2).await, "present before expiry");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!ttl_shard.contains_key(&2).await, "gone after ttl expiry");
}

#[tokio::test]
async fn bulk_writes_queue_every_key_for_fan_out_and_a_drain_takes_them_all() {
    let s = shard::<u32, String>(1);
    let queue = s.fan_out_queue();
    s.insert_many((0..10_000u32).map(|k| (k, k.to_string())))
        .await
        .expect("bulk insert");
    assert_eq!(queue.len(), 10_000, "nothing is lost to a lagged channel");
    let mut keys = applied_keys(queue.drain());
    keys.sort_unstable();
    assert_eq!(keys, (0..10_000u32).collect::<Vec<_>>());
    assert_eq!(queue.len(), 0);

    s.remove_many(0..10_000u32).await.expect("bulk remove");
    assert_eq!(queue.drain().len(), 10_000);
    s.insert(1, "one".into()).await.expect("insert");
    s.insert(1, "one again".into()).await.expect("insert");
    assert_eq!(
        applied_keys(queue.drain()),
        vec![1, 1],
        "single writes queue their key each time"
    );
}

#[cfg(feature = "spill")]
#[test]
fn a_retry_round_is_futile_only_when_the_whole_queue_covered_nothing() {
    const WHOLE: u32 = 4096;
    assert!(
        retry_round_is_futile(WHOLE, WHOLE, WHOLE, 5000),
        "the whole queue, nothing spent, a deficit still owed: the victim exceeds the queue"
    );
    assert!(
        !retry_round_is_futile(300, WHOLE, 300, 700),
        "granted less than the queue and nothing spent: the next round asks for 700, \
         which the queue can grant"
    );
    assert!(
        !retry_round_is_futile(WHOLE, WHOLE, 96, 700),
        "the round spent from its reservation: progress, keep retrying"
    );
    assert!(
        !retry_round_is_futile(WHOLE, WHOLE, WHOLE, 0),
        "nothing owed: the loop ends on its own"
    );
}

#[test]
fn fan_out_over_capacity_at_or_above_the_cap_only() {
    assert!(!fan_out_over_capacity(4, 5), "under capacity: no wait");
    assert!(fan_out_over_capacity(5, 5), "at capacity: wait");
    assert!(fan_out_over_capacity(6, 5), "over capacity: wait");
    assert!(
        !fan_out_over_capacity(0, 1),
        "an empty queue is always under a nonzero capacity"
    );
}

#[tokio::test]
async fn insert_many_awaits_fan_out_room_and_proceeds_once_the_backlog_drains() {
    let s = shard::<u32, String>(1)
        .with_fan_out_backlog_capacity(5)
        .with_fan_out_wait_timeout(Duration::from_secs(10));
    let queue = s.fan_out_queue();

    // Fills the backlog to capacity; this call itself never waits.
    s.insert_many((0..5u32).map(|k| (k, k.to_string())))
        .await
        .expect("fills the backlog to capacity");
    assert_eq!(queue.len(), 5);

    // At capacity: the next insert_many must await room before pushing.
    let insert_fut = s.insert_many((5..7u32).map(|k| (k, k.to_string())));
    tokio::pin!(insert_fut);
    let not_yet = tokio::time::timeout(Duration::from_millis(150), &mut insert_fut).await;
    assert!(
        not_yet.is_err(),
        "insert_many must still be waiting for backlog room at capacity"
    );

    // The fan-out task's drain frees room and wakes the waiter.
    let drained = queue.drain();
    assert_eq!(
        drained.len(),
        5,
        "drains exactly what was there before the wait"
    );

    tokio::time::timeout(Duration::from_secs(5), &mut insert_fut)
        .await
        .expect("insert_many proceeds once the backlog drains")
        .expect("insert_many succeeds");
    let mut keys = applied_keys(queue.drain());
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![5, 6],
        "the write that waited still lands once room frees up"
    );
}

#[tokio::test]
async fn insert_many_proceeds_once_its_fan_out_wait_times_out() {
    let s = shard::<u32, String>(1)
        .with_fan_out_backlog_capacity(1)
        .with_fan_out_wait_timeout(Duration::from_millis(100));
    let queue = s.fan_out_queue();

    s.insert(0, "zero".into())
        .await
        .expect("fills the backlog to capacity");
    assert_eq!(queue.len(), 1);

    // Nothing drains the queue; the wait must time out and the write still land.
    let started = tokio::time::Instant::now();
    s.insert_many([(1u32, "one".to_string())])
        .await
        .expect("insert_many proceeds regardless once its wait times out");
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "the call actually waited out its configured timeout"
    );

    let mut keys = applied_keys(queue.drain());
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![0, 1],
        "the write is never dropped or refused: it lands over capacity instead"
    );
}

#[tokio::test]
async fn fan_out_queue_wakes_a_waiter_for_a_push_before_or_after_the_wait() {
    let queue = Arc::new(FanOutQueue::<u32>::new(SmolStr::new("test"), true));
    let _ = queue.push(7);
    tokio::time::timeout(Duration::from_secs(1), queue.wait_nonempty())
        .await
        .expect("a push before the wait is not missed");
    assert_eq!(queue.drain(), vec![7]);

    let waiter = {
        let queue = Arc::clone(&queue);
        tokio::spawn(async move {
            queue.wait_nonempty().await;
            queue.drain()
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = queue.extend([1, 2]);
    let drained = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("a push after the wait wakes it")
        .expect("waiter completes");
    assert_eq!(drained, vec![1, 2]);
}

#[tokio::test]
async fn hand_off_bulk_waits_for_a_full_batch_unless_flushed() {
    let s = shard::<u32, String>(1);
    let mut landed: Vec<u32> =
        (0..u32::try_from(REPLICATE_BATCH_COUNT - 1).expect("fits")).collect();

    s.hand_off_bulk(&mut landed, false);
    assert_eq!(
        landed.len(),
        REPLICATE_BATCH_COUNT - 1,
        "short of a batch, nothing moves"
    );
    let drained = s.fan_out.drain();
    assert!(drained.is_empty(), "{drained:?}");

    landed.push(u32::MAX);
    s.hand_off_bulk(&mut landed, false);
    assert!(landed.is_empty(), "a full batch is handed off whole");
    assert_eq!(s.fan_out.drain().len(), REPLICATE_BATCH_COUNT);

    let mut remainder = vec![1, 2, 3];
    s.hand_off_bulk(&mut remainder, true);
    assert!(remainder.is_empty(), "a flush hands off whatever is left");
    assert_eq!(applied_keys(s.fan_out.drain()), vec![1, 2, 3]);

    let mut nothing: Vec<u32> = Vec::new();
    s.hand_off_bulk(&mut nothing, true);
    assert!(
        s.fan_out.drain().is_empty(),
        "an empty flush queues nothing"
    );
}

#[tokio::test]
async fn insert_many_queues_only_the_keys_that_landed() {
    // Under `AlwaysA` whatever is stored wins, so a bulk write over an
    // existing key is rejected and must not reach the fan-out queue.
    let s = shard::<u32, String>(1).with_resolver(Arc::new(AlwaysA));
    s.insert(1, "first".into()).await.expect("insert");
    let _ = s.fan_out.drain();

    s.insert_many([(1, "again".to_string()), (2, "two".to_string())])
        .await
        .expect("insert_many");

    assert_eq!(
        applied_keys(s.fan_out.drain()),
        vec![2],
        "only the write that landed is queued for replication"
    );
    assert_eq!(s.get(&1).await, Some("first".to_string()));
    assert_eq!(s.get(&2).await, Some("two".to_string()));
}

#[tokio::test]
async fn remove_many_queues_only_the_keys_that_landed() {
    let s = shard::<u32, String>(1).with_resolver(Arc::new(AlwaysA));
    s.insert(1, "first".into()).await.expect("insert");
    let _ = s.fan_out.drain();

    s.remove_many([1, 3]).await.expect("remove_many");

    assert_eq!(
        applied_keys(s.fan_out.drain()),
        vec![3],
        "the rejected tombstone never reaches the fan-out queue"
    );
    assert_eq!(s.get(&1).await, Some("first".to_string()));
}

#[tokio::test]
async fn remove_many_tombstones_every_key_and_leaves_others_untouched() {
    let s = shard::<u32, String>(1);
    for k in 0..5u32 {
        s.insert(k, k.to_string()).await.expect("insert");
    }

    let mut events = s.events();
    s.remove_many([0u32, 1, 2]).await.expect("remove_many");

    for k in 0..3u32 {
        assert_eq!(s.get(&k).await, None, "key {k} is tombstoned");
        let recs = ShardOps::records_for(&s, vec![key_bytes(&k)]).await;
        assert_eq!(recs.len(), 1);
        assert!(recs[0].is_tombstone(), "key {k} reads back as a tombstone");
    }
    for k in 3..5u32 {
        assert_eq!(
            s.get(&k).await,
            Some(k.to_string()),
            "key {k} was not in the batch and survives"
        );
    }

    let mut removed = HashSet::new();
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("event arrives")
            .expect("channel open")
        {
            Event::Removed { key, .. } => {
                removed.insert(key);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(removed, HashSet::from([0u32, 1, 2]));
}

#[tokio::test]
async fn clear_empties_a_populated_shard_and_leaves_tombstones() {
    let s = shard::<u32, String>(1);
    for k in 0..10u32 {
        s.insert(k, k.to_string()).await.expect("insert");
    }
    assert_eq!(s.entry_count().await, 10);

    s.clear().await.expect("clear");
    assert_eq!(s.entry_count().await, 0);

    for k in 0..10u32 {
        let recs = ShardOps::records_for(&s, vec![key_bytes(&k)]).await;
        assert_eq!(recs.len(), 1, "key {k} leaves a tombstone");
        assert!(recs[0].is_tombstone());
    }
}

#[tokio::test]
async fn get_or_insert_with_fills_on_miss_and_skips_make_on_hit() {
    let s = shard::<u32, String>(1);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let c1 = std::sync::Arc::clone(&calls);
    let filled = s
        .get_or_insert_with(&1, async move |_key: &u32| {
            c1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            "loaded".to_string()
        })
        .await
        .expect("fill succeeds");
    assert_eq!(filled, "loaded");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let c2 = std::sync::Arc::clone(&calls);
    let hit = s
        .get_or_insert_with(&1, async move |_key: &u32| {
            c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            "should-not-run".to_string()
        })
        .await
        .expect("hit succeeds");
    assert_eq!(hit, "loaded");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn keys_returns_exactly_the_live_keys() {
    let s = shard::<u32, String>(1);
    for k in 0..5u32 {
        s.insert(k, k.to_string()).await.expect("insert");
    }
    s.remove(&2).await.expect("remove");

    let mut keys = s.keys();
    keys.sort_unstable();
    assert_eq!(keys, vec![0, 1, 3, 4]);
}

#[tokio::test]
async fn for_each_key_visits_the_same_set_as_keys() {
    let s = shard::<u32, String>(1);
    for k in 0..5u32 {
        s.insert(k, k.to_string()).await.expect("insert");
    }
    s.remove(&2).await.expect("remove");

    let mut visited = Vec::new();
    s.for_each_key(|k| visited.push(k));
    visited.sort_unstable();
    let mut keys = s.keys();
    keys.sort_unstable();
    assert_eq!(visited, keys);
}

#[tokio::test]
async fn get_sync_reads_exactly_what_get_would() {
    let s = shard::<u32, String>(1);
    assert_eq!(s.get_sync(&1), None, "missing key");

    s.insert(1, "a".into()).await.expect("insert");
    assert_eq!(s.get_sync(&1), Some("a".to_string()));
    assert_eq!(s.get_sync(&1), s.get(&1).await);
}

#[tokio::test]
async fn contains_key_sync_reflects_insert_and_remove() {
    let s = shard::<u32, String>(1);
    assert!(!s.contains_key_sync(&1), "missing key");

    s.insert(1, "a".into()).await.expect("insert");
    assert!(s.contains_key_sync(&1), "present after insert");

    s.remove(&1).await.expect("remove");
    assert!(!s.contains_key_sync(&1), "gone after remove");
}

#[test]
fn insert_sync_writes_a_value_with_no_async_runtime() {
    let s = shard::<u32, String>(1);
    s.insert_sync(1, "a".into()).expect("insert_sync");
    assert_eq!(s.get_sync(&1), Some("a".to_string()));
}

/// Pins that a cache with no spill tier sees zero behavior change:
/// `reserve_for_batch` resolves immediately with `reservation: None`.
#[tokio::test]
async fn spill_less_cache_never_touches_reserve() {
    let s = shard::<u32, String>(1);
    s.insert_many((0..50u32).map(|k| (k, k.to_string())))
        .await
        .expect("insert_many");
    for k in 0..50u32 {
        assert_eq!(s.get(&k).await, Some(k.to_string()));
    }
    s.remove_many(0..50u32).await.expect("remove_many");
    for k in 0..50u32 {
        assert_eq!(s.get(&k).await, None);
    }
}

#[test]
fn remove_sync_tombstones_a_value_with_no_async_runtime() {
    let s = shard::<u32, String>(1);
    s.insert_sync(1, "a".into()).expect("insert_sync");
    s.remove_sync(&1).expect("remove_sync");
    assert_eq!(s.get_sync(&1), None);
}

#[tokio::test]
async fn a_sealed_fan_out_queue_keeps_its_backlog_and_refuses_a_forwarded_write() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);
    let owned = find_key_by_ownership(&view, true);
    let unowned = find_key_by_ownership(&view, false);
    s.insert(unowned, "before".to_string())
        .await
        .expect("a forward before the seal is accepted");

    s.fan_out.seal();
    assert!(
        matches!(
            s.insert(unowned, "after".to_string()).await,
            Err(CacheError::Closed { .. })
        ),
        "a forwarded write after the seal is refused, not accepted and never sent"
    );
    assert!(matches!(
        s.remove(&unowned).await,
        Err(CacheError::Closed { .. })
    ));
    assert!(matches!(
        s.insert_many(vec![(unowned, "bulk".to_string())]).await,
        Err(CacheError::Closed { .. })
    ));
    s.insert(owned, "local".to_string())
        .await
        .expect("a write this node applies itself still lands, detached");
    assert_eq!(s.get(&owned).await.as_deref(), Some("local"));

    let backlog = s.fan_out.drain();
    assert_eq!(
        backlog.len(),
        1,
        "the forward from before the seal is still there for the final drain"
    );
    assert!(matches!(backlog[0], FanOutItem::Forward(_)));
}

#[test]
fn a_local_mode_shard_queues_nothing_for_fan_out() {
    let s = Shard::<u32, String>::new(
        SmolStr::new("local"),
        Mode::Local,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    s.insert_sync(1, "a".into()).expect("insert");
    s.remove_sync(&1).expect("remove");
    assert!(s.fan_out.drain().is_empty(), "nothing drains a Local shard");
}

#[test]
fn a_closed_fan_out_queue_drops_its_backlog_and_accepts_nothing() {
    let s = shard::<u32, String>(1);
    s.insert_sync(1, "a".into()).expect("insert");
    s.fan_out_queue().close();
    s.insert_sync(2, "b".into())
        .expect("a write this node applies itself still lands after close, detached");
    let drained = s.fan_out.drain();
    assert!(drained.is_empty(), "{drained:?}");
    assert_eq!(s.get_sync(&2), Some("b".to_string()), "writes still land");
}

#[tokio::test]
async fn with_clock_drives_every_shard_timestamp() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let fake_now = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&fake_now);
    let clock_fn: Arc<dyn Fn() -> u64 + Send + Sync> =
        Arc::new(move || reader.load(Ordering::SeqCst));
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_millis(100)),
        None,
    )
    .with_clock(Arc::clone(&clock_fn));

    s.insert(1, "a".into()).await.expect("insert");
    let recs = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert_eq!(
        recs[0].ver.wall_ms, 1_000,
        "the HLC stamp reads the injected clock, not the system clock"
    );
    assert_eq!(
        recs[0].expires_at_ms,
        Some(1_100),
        "the expiry deadline is computed from the injected clock"
    );
    assert_eq!(
        s.get(&1).await,
        Some("a".into()),
        "not yet past the injected clock's TTL"
    );

    // Advance the fake clock, not real time, past the TTL.
    fake_now.store(1_300, Ordering::SeqCst);
    assert_eq!(
        s.get(&1).await,
        None,
        "reads are TTL-blind but still honor the injected clock for expiry"
    );
    ShardOps::run_pending_tasks(&s).await;
    assert_eq!(
        s.entry_count().await,
        0,
        "the sweep ran against the injected clock, not real time"
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

/// A `WireRecord` for `key`/`value` at `ver`, matching this test
/// module's other hand-built remote records.
fn wire_record(key: u32, value: &str, ver: Hlc) -> WireRecord {
    WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("test value encodes"),
        )),
        ver,
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn insert_on_a_non_owner_never_becomes_visible_to_local_get() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let unowned_keys: Vec<u32> = (0..5_000u32)
        .filter(|&key| !view.owns(PartId::of_key(&key_bytes(&key))))
        .take(200)
        .collect();
    assert!(
        !unowned_keys.is_empty(),
        "some unowned key is found within a 5,000-key scan"
    );

    for &key in &unowned_keys {
        s.insert(key, "v".to_string())
            .await
            .expect("a non-owner's insert never errors");
        assert!(
            s.get(&key).await.is_none(),
            "a non-owner's write for key {key} must never become locally visible"
        );
    }
}

#[tokio::test]
async fn clear_on_a_distributed_cache_only_tombstones_owned_buckets() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let owned_key = find_key_by_ownership(&view, true);
    let unowned_key = find_key_by_ownership(&view, false);
    s.insert(owned_key, "owned".to_string())
        .await
        .expect("owner's insert lands");
    s.insert(unowned_key, "forwarded".to_string())
        .await
        .expect("a non-owner's insert never errors");

    assert_eq!(s.get(&owned_key).await, Some("owned".to_string()));
    assert!(
        s.get(&unowned_key).await.is_none(),
        "the forwarded write never became locally visible in the first place"
    );

    s.clear().await.expect("clear");

    assert!(
        s.get(&owned_key).await.is_none(),
        "clear tombstones the owned key"
    );
    assert!(
        s.get(&unowned_key).await.is_none(),
        "still absent: nothing was ever held for the unowned bucket to begin with"
    );
}

#[tokio::test]
async fn insert_many_on_a_distributed_cache_forwards_unowned_entries_and_applies_owned_ones() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let owned_key = find_key_by_ownership(&view, true);
    let unowned_key = find_key_by_ownership(&view, false);

    s.insert_many([
        (owned_key, "owned".to_string()),
        (unowned_key, "forwarded".to_string()),
    ])
    .await
    .expect("bulk insert");

    assert_eq!(s.get(&owned_key).await, Some("owned".to_string()));
    assert!(
        s.get(&unowned_key).await.is_none(),
        "a bulk write forwarded for an unowned bucket never becomes locally visible"
    );
    let forwarded = s.fan_out_queue().drain();
    assert!(
        forwarded.iter().any(|item| matches!(
            item,
            FanOutItem::Forward(rec) if rec.key.as_ref() == key_bytes(&unowned_key).as_ref()
        )),
        "the unowned key lands on the fan-out queue as a Forward item: {forwarded:?}"
    );
}

#[tokio::test]
async fn remove_many_on_a_distributed_cache_forwards_unowned_tombstones_and_applies_owned_ones() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let owned_key = find_key_by_ownership(&view, true);
    let unowned_key = find_key_by_ownership(&view, false);
    s.insert(owned_key, "owned".to_string())
        .await
        .expect("owner's insert lands");
    let _ = s.fan_out_queue().drain();

    s.remove_many([owned_key, unowned_key])
        .await
        .expect("bulk remove");

    assert!(
        s.get(&owned_key).await.is_none(),
        "the owned key is tombstoned"
    );
    assert!(
        s.get(&unowned_key).await.is_none(),
        "still absent: it was never held to begin with"
    );
    let forwarded = s.fan_out_queue().drain();
    assert!(
        forwarded.iter().any(|item| matches!(
            item,
            FanOutItem::Forward(rec) if rec.key.as_ref() == key_bytes(&unowned_key).as_ref()
        )),
        "the unowned tombstone lands on the fan-out queue as a Forward item: {forwarded:?}"
    );
}

#[tokio::test]
async fn apply_remote_batch_drops_records_for_an_unowned_bucket_and_counts_them() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let owned_key = find_key_by_ownership(&view, true);
    let unowned_key = find_key_by_ownership(&view, false);
    let owned_rec = wire_record(owned_key, "owned", hlc(1, 9));
    let unowned_rec = wire_record(unowned_key, "dropped", hlc(1, 9));

    ShardOps::apply_remote_batch(&s, vec![owned_rec, unowned_rec]).await;

    assert_eq!(
        s.get(&owned_key).await,
        Some("owned".to_string()),
        "a record for a bucket this node owns still applies"
    );
    assert!(
        s.get(&unowned_key).await.is_none(),
        "a record for a bucket this node does not own is dropped, never applied"
    );
}

#[tokio::test]
async fn held_buckets_lists_a_bucket_with_an_entry_the_current_view_no_longer_owns() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let key = find_key_by_ownership(&view, true);
    let bucket = bucket_of(&key_bytes(&key));
    ShardOps::apply_remote_batch(&s, vec![wire_record(key, "held", hlc(1, 9))]).await;
    assert_eq!(
        ShardOps::held_buckets(&s).await,
        vec![bucket],
        "the one bucket with an entry is held; every empty bucket is not"
    );

    // A view that grew past this node's share of `bucket`, as a
    // membership change publishes while rebalance_task is busy.
    let moved = (6..=64u64)
        .map(|extra| {
            let eligible = (1..=extra).map(NodeId::from).collect::<Vec<_>>();
            OwnershipView::compute(self_node, eligible, NonZeroU8::new(2).expect("nonzero"))
        })
        .find(|v| !v.owns(first_part(bucket)))
        .expect("some larger eligible set takes the bucket away from self");
    tx.send(Arc::new(moved)).expect("receiver still alive");

    assert_eq!(
        ShardOps::held_buckets(&s).await,
        vec![bucket],
        "held_buckets is ungated: the entry is still resident under the new view"
    );
    assert_eq!(
        ShardOps::bucket_lens(&s, vec![bucket]).await,
        vec![BucketLen { bucket, len: 0 }],
        "bucket_lens stays residency-gated: the same bucket reads as empty"
    );
}

#[tokio::test]
async fn apply_remote_batch_forwards_a_record_for_a_releasing_bucket_on_instead_of_applying_it() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) =
        distributed_shard::<u32, String>(self_node, eligible, 2, Arc::clone(&residency));

    let unowned_key = find_key_by_ownership(&view, false);
    let unowned_bucket = bucket_of(&key_bytes(&unowned_key));
    // Mid disown-grace: outbound `is_resident` (owns || releasing) says
    // yes for this bucket, but the inbound-apply guard stays strict
    // current-view ownership: the record goes on to the current owners
    // through the fan-out queue and never lands here.
    residency.mark_releasing(&whole(unowned_bucket));

    let rec = wire_record(unowned_key, "redirected", hlc(1, 9));
    ShardOps::apply_remote_batch(&s, vec![rec.clone()]).await;

    assert!(
        s.get(&unowned_key).await.is_none(),
        "residency never widens what the inbound-apply guard applies"
    );
    assert_eq!(
        s.fan_out_queue().drain(),
        vec![FanOutItem::Forward(rec)],
        "the record is queued for the bucket's current owners"
    );

    // A bucket this node never held is still dropped outright.
    residency.unmark(&whole(unowned_bucket));
    let rec = wire_record(unowned_key, "dropped", hlc(2, 9));
    ShardOps::apply_remote_batch(&s, vec![rec]).await;
    assert!(s.get(&unowned_key).await.is_none());
    assert!(
        s.fan_out_queue().drain().is_empty(),
        "nothing is queued for a dropped record"
    );
}

#[tokio::test]
async fn digests_never_reports_an_unowned_bucket() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let reported: HashSet<u16> = ShardOps::digests(&s)
        .await
        .into_iter()
        .map(|bd| bd.bucket)
        .collect();
    for &bucket in &reported {
        assert!(
            view.owns(first_part(bucket)),
            "digests reported unowned bucket {bucket}"
        );
    }
    assert!(
        reported.len() < BUCKET_COUNT,
        "a 5-eligible/k=2 view owns a strict subset of buckets, so at least one is never \
         reported"
    );
}

#[tokio::test]
async fn outbound_guard_returns_nothing_for_an_unowned_bucket_on_every_serving_method() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);

    let unowned_key = find_key_by_ownership(&view, false);
    let unowned_bucket = bucket_of(&key_bytes(&unowned_key));

    assert_eq!(
        ShardOps::bucket_lens(&s, vec![unowned_bucket]).await,
        vec![BucketLen {
            bucket: unowned_bucket,
            len: 0
        }],
        "bucket_lens reports zero for an unowned bucket"
    );
    assert_eq!(
        ShardOps::part_digests(&s, vec![unowned_bucket]).await,
        vec![BucketPartDigests {
            bucket: unowned_bucket,
            digests: Vec::new()
        }],
        "part_digests reports nothing for an unowned bucket"
    );
    let part = BucketPart {
        bucket: unowned_bucket,
        part: 0u8,
    };
    assert_eq!(
        ShardOps::entries_for_parts(&s, vec![part]).await,
        vec![(part, Vec::new())],
        "entries_for_parts reports nothing for an unowned bucket's part"
    );
    assert!(
        ShardOps::records_for(&s, vec![key_bytes(&unowned_key)])
            .await
            .is_empty(),
        "records_for never answers a key in an unowned bucket"
    );
    let snapshot: Vec<WireRecord> = ShardOps::snapshot_chunks(&s)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect();
    assert!(
        snapshot
            .iter()
            .all(|rec| bucket_of(rec.key.as_ref()) != unowned_bucket),
        "snapshot_chunks never includes a record from an unowned bucket"
    );
}

#[tokio::test]
async fn a_releasing_bucket_still_answers_digests_and_entries() {
    let self_node = NodeId::from(1);
    // Self starts as the sole eligible node: it owns every bucket, so an
    // ordinary insert lands for real.
    let residency = Arc::new(ResidencySet::new());
    let (s, initial_view, tx) =
        distributed_shard::<u32, String>(self_node, vec![self_node], 1, Arc::clone(&residency));

    let new_view = OwnershipView::compute(
        self_node,
        vec![self_node, NodeId::from(2)],
        NonZeroU8::new(1).expect("nonzero"),
    );
    let key = (0..1_000_000u32)
        .find(|&k| !new_view.owns(PartId::of_key(&key_bytes(&k))))
        .expect("some bucket flips ownership once a second node joins");
    let part = PartId::of_key(&key_bytes(&key));
    assert!(initial_view.owns(part));

    s.insert(key, "still here".to_string())
        .await
        .expect("owner's insert lands");

    // Membership widens and this bucket moves to the other node,
    // exactly what `rebalance_task` reacts to by starting this bucket's
    // disown grace.
    tx.send(Arc::new(new_view)).expect("receiver still alive");
    residency.mark_releasing(&[part]);
    assert!(
        !ShardOps::ownership_view(&s).expect("attached").owns(part),
        "sanity: this node no longer owns the bucket under the new view"
    );

    let digests: HashSet<u16> = ShardOps::digests(&s)
        .await
        .into_iter()
        .map(|bd| bd.bucket)
        .collect();
    assert!(
        digests.contains(&part.bucket()),
        "a releasing bucket still answers digests"
    );

    let entries = ShardOps::entries_for_buckets(&s, vec![part.bucket()]).await;
    assert_eq!(entries.len(), 1);
    assert!(
        entries[0]
            .1
            .iter()
            .any(|kv| kv.key.as_ref() == key_bytes(&key).as_ref()),
        "the pre-existing entry in a releasing bucket is still served"
    );
}

#[test]
fn fan_out_item_applied_and_forward_both_drain_through_one_queue() {
    let queue = FanOutQueue::<FanOutItem<u32>>::new(SmolStr::new("test"), true);
    let rec = wire_record(7, "v", hlc(1, 1));
    let _ = queue.push(FanOutItem::Applied(1));
    let _ = queue.push(FanOutItem::Forward(rec.clone()));

    assert_eq!(
        queue.drain(),
        vec![FanOutItem::Applied(1), FanOutItem::Forward(rec)],
        "one queue drains both applied and forwarded items, in push order"
    );
}

#[tokio::test]
async fn forwarded_writes_emit_created_and_removed_events_and_never_updated() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(self_node, eligible, 2, residency);
    let mut events = s.events();

    let key = find_key_by_ownership(&view, false);
    s.insert(key, "first".to_string())
        .await
        .expect("forwarded insert");
    match events.recv().await.expect("an event was published") {
        Event::Created {
            key: k,
            value,
            origin,
        } => {
            assert_eq!(k, key);
            assert_eq!(value, "first");
            assert_eq!(origin, Origin::Local);
        }
        other => panic!("expected Event::Created, got {other:?}"),
    }

    // A second forwarded insert for the same key: a non-owner holds no
    // prior copy, so it still cannot distinguish create from update:
    // this stays Created, never Updated, the documented difference from
    // every other write path.
    s.insert(key, "second".to_string())
        .await
        .expect("forwarded insert");
    match events.recv().await.expect("an event was published") {
        Event::Created { key: k, .. } => assert_eq!(k, key),
        other => panic!("a forwarded write never emits Event::Updated, got {other:?}"),
    }

    s.remove(&key).await.expect("forwarded remove");
    match events.recv().await.expect("an event was published") {
        Event::Removed { key: k, origin } => {
            assert_eq!(k, key);
            assert_eq!(origin, Origin::Local);
        }
        other => panic!("expected Event::Removed, got {other:?}"),
    }
}

#[tokio::test]
async fn release_buckets_removes_every_entry_and_tombstone_and_corrects_the_digest() {
    let s = shard::<u32, String>(1);
    let mut by_bucket: HashMap<u16, Vec<u32>> = HashMap::new();
    let mut pair: Vec<u32> = Vec::new();
    for k in 0..100_000u32 {
        let bucket = bucket_of(&key_bytes(&k));
        let group = by_bucket.entry(bucket).or_default();
        group.push(k);
        if group.len() == 2 {
            pair = group.clone();
            break;
        }
    }
    assert_eq!(
        pair.len(),
        2,
        "1024 buckets; a same-bucket pair is found quickly"
    );
    let released_bucket = bucket_of(&key_bytes(&pair[0]));

    s.insert(pair[0], "live".to_string()).await.expect("insert");
    s.insert(pair[1], "gone".to_string()).await.expect("insert");
    s.remove(&pair[1]).await.expect("remove");

    let other_key = (0..100_000u32)
        .find(|&k| bucket_of(&key_bytes(&k)) != released_bucket)
        .expect("a distinct bucket is found quickly");
    s.insert(other_key, "untouched".to_string())
        .await
        .expect("insert");

    let removed = ShardOps::release_buckets(&s, &[released_bucket]).await;

    assert_eq!(removed, 2, "one live entry and one tombstone removed");
    assert!(s.get(&pair[0]).await.is_none());
    assert_eq!(s.get(&other_key).await, Some("untouched".to_string()));
    let digest_for_released = ShardOps::digests(&s)
        .await
        .into_iter()
        .find(|bd| bd.bucket == released_bucket)
        .map(|bd| bd.digest);
    assert_eq!(
        digest_for_released,
        Some(0),
        "the released bucket's digest resets to zero"
    );
}

#[test]
fn is_cold_bucket_follows_the_residency_set_and_is_false_elsewhere() {
    let plain = shard::<u32, String>(1);
    assert!(!ShardOps::is_cold_bucket(&plain, 7));

    let residency = Arc::new(ResidencySet::new());
    let (s, _view, _tx) = distributed_shard::<u32, String>(
        NodeId::from(1),
        (1..=3u64).map(NodeId::from).collect(),
        2,
        Arc::clone(&residency),
    );
    assert!(!ShardOps::is_cold_bucket(&s, 7));
    residency.mark_cold(&whole(7));
    assert!(ShardOps::is_cold_bucket(&s, 7));
    residency.clear_cold(&whole(7));
    assert!(!ShardOps::is_cold_bucket(&s, 7));
}

#[cfg(feature = "spill")]
#[test]
fn is_unverified_bucket_follows_the_residency_set_and_is_false_elsewhere() {
    let plain = shard::<u32, String>(1);
    assert!(!ShardOps::is_unverified_bucket(&plain, 7));

    let residency = Arc::new(ResidencySet::new());
    let (s, _view, _tx) = distributed_shard::<u32, String>(
        NodeId::from(1),
        (1..=3u64).map(NodeId::from).collect(),
        2,
        Arc::clone(&residency),
    );
    assert!(!ShardOps::is_unverified_bucket(&s, 7));
    residency.mark_unverified(&whole(7));
    assert!(ShardOps::is_unverified_bucket(&s, 7));
    // Distinct from cold: marking one leaves the other alone.
    assert!(!ShardOps::is_cold_bucket(&s, 7));
    residency.clear_unverified(&whole(7));
    assert!(!ShardOps::is_unverified_bucket(&s, 7));
}

#[test]
fn ae_peer_filter_is_identity_for_a_non_distributed_shard() {
    let s = shard::<u32, String>(1);
    let peers = vec![NodeId::from(2), NodeId::from(3)];
    assert_eq!(
        ShardOps::ae_peer_filter(&s, peers.clone(), peers.clone()),
        (peers.clone(), peers)
    );
}

#[tokio::test]
async fn ae_digests_for_is_the_full_digest_list_for_a_non_distributed_shard() {
    let s = shard::<u32, String>(1);
    s.insert(7, "v".to_string()).await.expect("insert");
    let all = ShardOps::digests(&s).await;
    assert_eq!(all.len(), BUCKET_COUNT);
    assert_eq!(ShardOps::ae_digests_for(&s, NodeId::from(2)).await, all);
}

#[tokio::test]
async fn ae_digests_for_keeps_only_buckets_the_peer_co_owns_or_this_shard_is_releasing_to() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) =
        distributed_shard::<u32, String>(self_node, eligible, 2, Arc::clone(&residency));
    let peer = NodeId::from(2);
    let bucket_at = |i: usize| u16::try_from(i).expect("invariant: BUCKET_COUNT fits u16");
    let shared_owned = (0..BUCKET_COUNT)
        .map(bucket_at)
        .find(|&b| view.owns(first_part(b)) && view.owners_of(first_part(b)).contains(&peer))
        .expect("some bucket is owned by both self and the peer");
    let owned_not_shared = (0..BUCKET_COUNT)
        .map(bucket_at)
        .find(|&b| view.owns(first_part(b)) && !view.owners_of(first_part(b)).contains(&peer))
        .expect("some bucket is owned by self but not the peer");
    let peer_only = (0..BUCKET_COUNT)
        .map(bucket_at)
        .find(|&b| !view.owns(first_part(b)) && view.owners_of(first_part(b)).contains(&peer))
        .expect("some bucket is owned by the peer but not self");
    let neither = (0..BUCKET_COUNT)
        .map(bucket_at)
        .find(|&b| !view.owns(first_part(b)) && !view.owners_of(first_part(b)).contains(&peer))
        .expect("some bucket is owned by neither self nor the peer");
    // A bucket mid disown-grace that the peer now owns stays in the
    // round: the anti-entropy backstop for a lost rebalance pull.
    residency.mark_releasing(&[whole(peer_only), whole(neither)].concat());

    let listed: HashSet<u16> = ShardOps::ae_digests_for(&s, peer)
        .await
        .into_iter()
        .map(|bd| bd.bucket)
        .collect();
    assert!(
        listed.contains(&shared_owned),
        "a co-owned bucket is listed"
    );
    assert!(
        listed.contains(&peer_only),
        "a releasing bucket the peer owns is listed"
    );
    assert!(
        !listed.contains(&owned_not_shared),
        "an owned bucket the peer does not own is not listed"
    );
    assert!(
        !listed.contains(&neither),
        "a releasing bucket the peer does not own is not listed"
    );
    for bucket in &listed {
        assert!(
            view.owners_of(first_part(*bucket)).contains(&peer),
            "every listed bucket is owned by the peer: {bucket}"
        );
        assert!(
            view.owns(first_part(*bucket)) || residency.is_releasing(first_part(*bucket)),
            "every listed bucket is resident here: {bucket}"
        );
    }
    let full = ShardOps::digests(&s).await;
    assert!(
        listed.len() < full.len(),
        "the peer-scoped list is a strict subset"
    );
}

#[test]
fn ae_peer_filter_narrows_to_the_current_cohort() {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) =
        distributed_shard::<u32, String>(self_node, eligible.clone(), 2, residency);

    let mut cohort: HashSet<NodeId> = HashSet::new();
    for i in 0..BUCKET_COUNT {
        let bucket = u16::try_from(i).expect("invariant: BUCKET_COUNT fits u16");
        if view.owns(first_part(bucket)) {
            cohort.extend(view.owners_of(first_part(bucket)).iter().copied());
        }
    }
    cohort.remove(&self_node);
    assert!(!cohort.is_empty(), "some peer co-owns a bucket with self");

    let candidates: Vec<NodeId> = eligible
        .iter()
        .copied()
        .filter(|&n| n != self_node)
        .collect();
    let stranger = candidates.iter().copied().find(|n| !cohort.contains(n));

    let (dirty, live) = ShardOps::ae_peer_filter(&s, candidates.clone(), candidates.clone());

    for peer in dirty.iter().chain(live.iter()) {
        assert!(
            cohort.contains(peer),
            "ae_peer_filter kept a peer sharing no owned bucket with this shard: {peer:?}"
        );
    }
    if let Some(stranger) = stranger {
        assert!(
            !dirty.contains(&stranger) && !live.contains(&stranger),
            "a peer sharing nothing with this shard is filtered out"
        );
    }
    for peer in candidates.iter().filter(|n| cohort.contains(n)) {
        assert!(
            dirty.contains(peer) && live.contains(peer),
            "a co-owning peer stays in the cohort"
        );
    }
}

/// `Shard`-layer coverage of the spill read path: every method here
/// goes through a real [`crate::store::spill::SpillTier`] and its
/// dedicated flusher thread, never a synthetic engine-level shortcut, so
/// I/O tests are gated like `spill.rs`'s own.
#[cfg(all(feature = "spill", not(feature = "sim")))]
mod spill_reads {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;
    use crate::store::spill::{SpillConfig, SpillLoc, SpillSink};

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sundog-shard-spill-test-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id(),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Polls `cond` until it returns `true` or `timeout` elapses,
    /// returning the final result either way. Never a fixed sleep.
    async fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if cond() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return cond();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    const POLL_TIMEOUT: Duration = Duration::from_secs(5);

    /// Takes every one of `shard`'s spill read permits, so no disk read
    /// of its own starts until the returned permit drops.
    async fn hold_spill_reads(shard: &Shard<u32, String>) -> tokio::sync::OwnedSemaphorePermit {
        let semaphore = Arc::clone(&shard.spill_read.get().expect("a spill tier").semaphore);
        let permits = u32::try_from(semaphore.available_permits()).expect("a small count");
        semaphore
            .acquire_many_owned(permits)
            .await
            .expect("the read semaphore is open")
    }

    /// A `Mode::Local` shard with a real spill tier attached, under
    /// `max_capacity`. Returns the shard and its scratch directory,
    /// cleaned up by the caller.
    fn spill_shard(label: &str, max_capacity: u64) -> (Shard<u32, String>, PathBuf) {
        spill_shard_in(label, "spill-test", Mode::Local, max_capacity)
    }

    /// [`spill_shard`] named `name`, which labels its metrics, under
    /// `mode`.
    fn spill_shard_in(
        label: &str,
        name: &str,
        mode: Mode,
        max_capacity: u64,
    ) -> (Shard<u32, String>, PathBuf) {
        let dir = temp_dir(label);
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let shard = Shard::<u32, String>::new(
            SmolStr::new(name),
            mode,
            NodeId::from(1u64),
            max_capacity,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        (shard, dir)
    }

    /// `Shard::attach_spill` sets the tier's `keep_resident_when_refused`
    /// policy from the shard's own `Mode`: `true` for `Mode::Replicated`
    /// and `Mode::Distributed`, since every peer, or every co-owner,
    /// still holds the entry and a local delete would just have
    /// anti-entropy, or a rebalance donor pull, repair it back in,
    /// `false` for `Mode::Local`, which has no peer to repair from and
    /// so deletes a refused victim on a write.
    #[test]
    fn with_spill_sets_keep_resident_when_refused_from_mode() {
        let dir = temp_dir("keep-resident-replicated");
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let replicated = Shard::<u32, String>::new(
            SmolStr::new("spill-test-replicated"),
            Mode::Replicated,
            NodeId::from(1u64),
            u64::MAX,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        assert!(
            replicated
                .engine
                .spill()
                .expect("with_spill attaches a tier")
                .keep_resident_when_refused(),
            "Mode::Replicated sets the tier's keep_resident_when_refused policy"
        );
        let _ = std::fs::remove_dir_all(&dir);

        let dir = temp_dir("keep-resident-distributed");
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let distributed = Shard::<u32, String>::new(
            SmolStr::new("spill-test-distributed"),
            Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            },
            NodeId::from(1u64),
            u64::MAX,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        assert!(
            distributed
                .engine
                .spill()
                .expect("with_spill attaches a tier")
                .keep_resident_when_refused(),
            "Mode::Distributed sets the tier's keep_resident_when_refused policy too"
        );
        let _ = std::fs::remove_dir_all(&dir);

        let dir = temp_dir("keep-resident-local");
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let local = Shard::<u32, String>::new(
            SmolStr::new("spill-test-local"),
            Mode::Local,
            NodeId::from(1u64),
            u64::MAX,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        assert!(
            !local
                .engine
                .spill()
                .expect("with_spill attaches a tier")
                .keep_resident_when_refused(),
            "Mode::Local leaves the tier's keep_resident_when_refused policy at its default"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn release_buckets_corrects_the_spilled_entries_gauge_on_a_shard_with_spill() {
        let (s, dir) = spill_shard("release-buckets-spill", 1);
        s.insert(1, "one".to_string()).await.expect("insert 1");
        s.insert(2, "two".to_string()).await.expect("insert 2");
        assert!(
            poll_until(POLL_TIMEOUT, || s.engine.debug_spill_entries_count() > 0).await,
            "one of the two entries spills under a capacity of 1"
        );

        let bucket1 = bucket_of(&key_bytes(&1u32));
        let bucket2 = bucket_of(&key_bytes(&2u32));
        let removed = ShardOps::release_buckets(&s, &[bucket1, bucket2]).await;

        assert_eq!(
            removed, 2,
            "both entries removed, whichever one had spilled"
        );
        assert_eq!(
            s.engine.debug_spill_entries_count(),
            0,
            "release_buckets corrects the spilled-entries gauge for its own bucket set"
        );
        assert!(s.get(&1).await.is_none());
        assert!(s.get(&2).await.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Inserts two entries under `max_capacity == 1`, forcing eviction to
    /// spill one of them, then waits up to a bound for the flusher to
    /// install it. This is observed through the tier's own
    /// `bytes_used`, so it never depends on which of the two keys the
    /// sampler picked. Returns `(shard, spilled_key, spilled_value,
    /// resident_key, resident_value, dir)`.
    async fn shard_with_one_spilled_entry(
        label: &str,
    ) -> (Shard<u32, String>, u32, String, u32, String, PathBuf) {
        let (shard, dir) = spill_shard(label, 1);
        shard.insert(1, "one".to_string()).await.expect("insert 1");
        shard.insert(2, "two".to_string()).await.expect("insert 2");

        assert!(
            poll_until(POLL_TIMEOUT, || {
                shard
                    .engine
                    .spill()
                    .is_some_and(|tier| tier.bytes_used() > 0)
            })
            .await,
            "eviction spills exactly one of the two keys within the poll bound"
        );
        if shard.get_sync(&1).is_none() {
            (shard, 1, "one".to_string(), 2, "two".to_string(), dir)
        } else {
            (shard, 2, "two".to_string(), 1, "one".to_string(), dir)
        }
    }

    /// `local_record` of a spilled key reports it from its pointer: no
    /// spill read is counted or timed, and the entry stays spilled, so
    /// `get_sync` still misses it; a `get` of it then reads it from disk
    /// and is counted.
    #[tokio::test]
    async fn local_record_of_a_spilled_key_reads_no_disk_and_does_not_promote() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let (shard, spilled_key, spilled_value, resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("local-record-spilled").await;
        // Disk reads counted and timed since the last call: a snapshot
        // drains both.
        let reads = || {
            let (mut counted, mut timed) = (0, 0);
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                match (key.key().name(), value) {
                    ("sundog_spill_reads_total", DebugValue::Counter(n)) => counted += n,
                    ("sundog_spill_read_duration_seconds", DebugValue::Histogram(v)) => {
                        timed += v.len();
                    }
                    _ => {}
                }
            }
            (counted, timed)
        };
        assert_eq!(reads(), (0, 0));

        let spilled = shard.local_record(&key_bytes(&spilled_key)).1;
        let resident = shard.local_record(&key_bytes(&resident_key)).1;
        assert!(
            matches!(spilled, LocalRecord::Live { spilled: true, .. }),
            "{spilled:?}"
        );
        assert!(
            matches!(resident, LocalRecord::Live { spilled: false, .. }),
            "{resident:?}"
        );
        assert_eq!(reads(), (0, 0), "no disk read");
        assert_eq!(
            shard.get_sync(&spilled_key),
            None,
            "the key is still spilled"
        );

        assert_eq!(shard.get(&spilled_key).await, Some(spilled_value));
        assert_eq!(reads(), (1, 1), "a get of it reads the disk");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A write past the cap spills an older entry, never itself: under
    /// a cap of one, the second key stays resident and the first spills.
    #[tokio::test]
    async fn a_write_past_the_cap_spills_an_older_entry_rather_than_itself() {
        let (shard, dir) = spill_shard("spills-the-older", 1);
        shard.insert(1, "one".to_string()).await.expect("insert 1");
        shard.insert(2, "two".to_string()).await.expect("insert 2");
        assert!(
            poll_until(POLL_TIMEOUT, || {
                shard
                    .engine
                    .spill()
                    .is_some_and(|tier| tier.bytes_used() > 0)
            })
            .await,
            "one key spills within the poll bound"
        );
        assert_eq!(
            shard.get_sync(&2),
            Some("two".to_string()),
            "the write stays resident"
        );
        assert_eq!(shard.get_sync(&1), None, "the older key spilled");
        assert_eq!(shard.get(&1).await, Some("one".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn get_sync_misses_a_spilled_key_while_get_promotes_it() {
        let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("get-sync-vs-get").await;

        assert_eq!(
            shard.get_sync(&spilled_key),
            None,
            "get_sync never touches disk; a spilled key reads as a miss"
        );
        assert_eq!(
            shard.get(&spilled_key).await,
            Some(spilled_value.clone()),
            "get reads the spilled value back off disk"
        );
        assert_eq!(
            shard.get_sync(&spilled_key),
            Some(spilled_value),
            "the disk read promoted the key back to residency"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The re-arm is a write over the capacity of one entry, so the
    /// eviction it triggers can spill the entry straight back: the value
    /// stays readable either way and keeps the new lifetime.
    #[tokio::test]
    async fn expire_reads_a_spilled_value_back_and_ttl_of_needs_no_disk_read() {
        let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("expire-spilled").await;

        assert_eq!(shard.ttl_of(&spilled_key), Some(Ttl::Never));
        assert_eq!(
            shard.get_sync(&spilled_key),
            None,
            "reading the lifetime left the entry spilled"
        );
        assert!(
            shard
                .expire(&spilled_key, Duration::from_secs(60))
                .await
                .expect("expire")
        );
        assert!(matches!(
            shard.ttl_of(&spilled_key),
            Some(Ttl::Remaining(left)) if left <= Duration::from_secs(60)
                && left > Duration::from_secs(50)
        ));
        assert_eq!(
            shard.get(&spilled_key).await,
            Some(spilled_value),
            "the value survives the re-arm, resident or spilled again"
        );
        assert!(matches!(
            shard.ttl_of(&spilled_key),
            Some(Ttl::Remaining(left)) if left <= Duration::from_secs(60)
                && left > Duration::from_secs(50)
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn contains_key_and_contains_key_sync_both_see_a_spilled_key_with_zero_disk_reads() {
        let (shard, spilled_key, _spilled_value, _resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("contains-key").await;

        assert!(
            shard.contains_key_sync(&spilled_key),
            "existence doesn't need the value bytes"
        );
        assert!(shard.contains_key(&spilled_key).await);
        // Neither existence check triggered a promotion: still spilled.
        assert_eq!(
            shard.get_sync(&spilled_key),
            None,
            "contains_key/contains_key_sync never read the disk"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn get_or_load_promotes_on_hit_without_a_duplicate_loader_call() {
        let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("get-or-load").await;

        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let loaded = shard
            .get_or_load(
                &spilled_key,
                async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok("should-not-run".to_string())
                },
            )
            .await
            .expect("resolves via the spilled-read path");
        assert_eq!(loaded, spilled_value.clone());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "the loader never runs once the disk read already found the key"
        );
        assert_eq!(
            shard.get_sync(&spilled_key),
            Some(spilled_value),
            "the owner's disk read promoted the key back to residency"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn get_or_insert_with_promotes_on_hit() {
        let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("get-or-insert-with").await;

        let made = shard
            .get_or_insert_with(&spilled_key, async move |_key: &u32| {
                "should-not-run".to_string()
            })
            .await
            .expect("resolves via the spilled-read path");
        assert_eq!(made, spilled_value.clone());
        assert_eq!(shard.get_sync(&spilled_key), Some(spilled_value));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Each disk read is counted and timed once, a batched read
    /// (`snapshot_chunks`) and a read of its own (`get`) alike.
    #[tokio::test]
    async fn every_disk_read_is_counted_and_timed_once() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("timed-reads").await;
        // Disk reads counted and timed since the last call: a snapshot
        // drains both.
        let reads = || {
            let (mut counted, mut timed) = (0, 0);
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                match (key.key().name(), value) {
                    ("sundog_spill_reads_total", DebugValue::Counter(n)) => counted += n,
                    ("sundog_spill_read_duration_seconds", DebugValue::Histogram(v)) => {
                        timed += v.len();
                    }
                    _ => {}
                }
            }
            (counted, timed)
        };
        assert_eq!(reads(), (0, 0));

        let records: Vec<WireRecord> = ShardOps::snapshot_chunks(&shard)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(
            records.len(),
            2,
            "both records, the spilled one read from disk"
        );
        assert_eq!(reads(), (1, 1), "the batched read is counted and timed");

        assert_eq!(shard.get(&spilled_key).await, Some(spilled_value));
        assert_eq!(reads(), (1, 1), "the get's own read is counted and timed");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn snapshot_chunks_folds_in_the_spilled_value_from_disk_without_promoting_it() {
        let (shard, spilled_key, spilled_value, resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("snapshot-chunks").await;

        // The state-transfer donor path: a peer joining fresh drives
        // this to pull every record this node holds, spilled ones
        // included.
        let chunks: Vec<Vec<WireRecord>> = ShardOps::snapshot_chunks(&shard).collect().await;
        let records: Vec<WireRecord> = chunks.into_iter().flatten().collect();

        let spilled_key_bytes = encode_key(&spilled_key).expect("key encodes");
        let record = records
            .iter()
            .find(|r| r.key.as_ref() == spilled_key_bytes.as_ref())
            .expect("the donor snapshot includes the spilled key's record");
        assert_eq!(
            record.value.as_deref(),
            Some(
                postcard::to_stdvec(&spilled_value)
                    .expect("test value encodes")
                    .as_slice()
            ),
            "the chunk carries the spilled value, read from disk"
        );

        let resident_key_bytes = encode_key(&resident_key).expect("key encodes");
        assert!(
            records
                .iter()
                .any(|r| r.key.as_ref() == resident_key_bytes.as_ref()),
            "the resident key's record is included too"
        );

        assert_eq!(
            shard.get_sync(&spilled_key),
            None,
            "the donor path never promotes: the entry stays Spilled after being read for \
             the snapshot"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn insert_on_a_spilled_key_discards_the_pending_spill() {
        let (shard, spilled_key, _old_value, resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("insert-over-spilled").await;

        // Frees the other live key's weight first, so the capacity-1
        // shard has room for the fresh write below without immediately
        // sampling it back out again: this test is about what one write
        // installs, not a second race against `enforce_capacity`.
        shard.remove(&resident_key).await.expect("remove");

        shard
            .insert(spilled_key, "fresh".to_string())
            .await
            .expect("insert");
        assert_eq!(
            shard.get_sync(&spilled_key),
            Some("fresh".to_string()),
            "a fresh write always installs resident, discarding whatever was spilled"
        );
        assert!(shard.contains_key_sync(&spilled_key));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn remove_on_a_spilled_key_prevents_a_late_flush_from_resurrecting_it() {
        // No real tier needed here: this drives `SpillSink::install`
        // directly, the call a real flusher makes, to deterministically
        // simulate a flush of the pre-removal value landing *after*
        // the remove. This is the narrow race a real disk's timing
        // cannot be made to land on demand.
        let shard = Shard::<u32, String>::new(
            SmolStr::new("spill-remove-race"),
            Mode::Local,
            NodeId::from(1u64),
            u64::MAX,
            None,
            None,
        );
        shard
            .insert(1, "original".to_string())
            .await
            .expect("insert");
        shard.remove(&1).await.expect("remove");
        assert_eq!(shard.get_sync(&1), None);

        let key_bytes = Bytes::from(postcard::to_stdvec(&1u32).expect("key encodes"));
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let bucket = engine::stripe_index_from_hash(hash);
        let installed = shard.engine.install(
            bucket,
            &key_bytes,
            hash,
            Hlc {
                wall_ms: 1,
                logical: 0,
                node: NodeId::from(1u64),
            },
            SpillLoc {
                region: 0,
                offset: 0,
                len: 4,
                generation: 0,
            },
            0,
        );
        assert!(!installed, "a late flush never resurrects a removed key");
        assert_eq!(shard.get_sync(&1), None, "the tombstone still wins");
        assert!(!shard.contains_key_sync(&1));
    }

    #[tokio::test]
    async fn invalidate_local_on_a_spilled_key_removes_it_unconditionally() {
        let (shard, spilled_key, _spilled_value, resident_key, resident_value, dir) =
            shard_with_one_spilled_entry("invalidate-local").await;

        shard.invalidate_local(&spilled_key).await;
        assert_eq!(shard.get_sync(&spilled_key), None);
        assert!(!shard.contains_key_sync(&spilled_key));
        // The untouched, still-resident key survives.
        assert_eq!(shard.get_sync(&resident_key), Some(resident_value));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn clear_tombstones_a_spilled_key() {
        let (shard, spilled_key, _spilled_value, resident_key, _resident_value, dir) =
            shard_with_one_spilled_entry("clear").await;

        shard.clear().await.expect("clear");
        assert_eq!(shard.get_sync(&spilled_key), None);
        assert!(!shard.contains_key_sync(&spilled_key));
        assert_eq!(shard.get_sync(&resident_key), None);
        assert!(!shard.contains_key_sync(&resident_key));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Pins that `reserve_for_batch` suspends `insert_many` while another
    /// caller holds the tier's flush-queue budget, resuming
    /// once it frees.
    #[tokio::test]
    async fn apply_grouped_suspends_under_a_saturated_queue_and_resumes_once_room_frees() {
        let dir = temp_dir("suspend-apply-grouped");
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(64)
            .spill_wait_timeout(Duration::from_secs(5));
        let shard = Shard::<u32, String>::new(
            SmolStr::new("suspend-apply-grouped"),
            Mode::Local,
            NodeId::from(1u64),
            u64::MAX,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));

        // Another caller holds the entire flush-queue budget already.
        let held = tier
            .reserve(64, Duration::from_secs(5))
            .await
            .expect("the whole (tiny) queue is free before this call");

        let insert_fut = shard.insert_many([(1u32, "a".to_string())]);
        tokio::pin!(insert_fut);
        let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut insert_fut).await;
        assert!(
            not_yet.is_err(),
            "insert_many's own apply_grouped call must still be waiting on reserve()"
        );

        drop(held);

        let result = tokio::time::timeout(Duration::from_secs(5), &mut insert_fut)
            .await
            .expect("insert_many resumes once the reservation frees room");
        assert!(
            result.is_ok(),
            "the write itself still succeeds: {result:?}"
        );
        assert_eq!(shard.get(&1).await, Some("a".to_string()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Pins that `apply_remote_batch` suspends and resumes identically to
    /// `apply_grouped` under a saturated flush-queue.
    #[tokio::test]
    async fn apply_remote_batch_suspends_identically_under_a_saturated_queue() {
        let dir = temp_dir("suspend-apply-remote-batch");
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(64)
            .spill_wait_timeout(Duration::from_secs(5));
        let shard = Shard::<u32, String>::new(
            SmolStr::new("suspend-apply-remote-batch"),
            Mode::Local,
            NodeId::from(1u64),
            u64::MAX,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));

        // Another caller holds the entire flush-queue budget already.
        let held = tier
            .reserve(64, Duration::from_secs(5))
            .await
            .expect("the whole (tiny) queue is free before this call");

        let rec = WireRecord {
            key: key_bytes(&1u32),
            value: Some(Bytes::from(postcard::to_stdvec("a").expect("encode"))),
            ver: hlc(1, 1),
            expires_at_ms: None,
        };
        let mut batch_fut = ShardOps::apply_remote_batch(&shard, vec![rec]);
        let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut batch_fut).await;
        assert!(
            not_yet.is_err(),
            "apply_remote_batch's own reserve_for_batch call must still be waiting on \
             reserve()"
        );

        drop(held);

        tokio::time::timeout(Duration::from_secs(5), &mut batch_fut)
            .await
            .expect("apply_remote_batch resumes once the reservation frees room");
        assert_eq!(shard.get(&1).await, Some("a".to_string()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Pins that `apply_remote_batch` groups by stripe before its one
    /// `reserve_for_batch` suspension point, so the suspension cannot
    /// reorder a batch: two same-key, same-`Hlc` puts must apply first-wins.
    #[tokio::test]
    async fn apply_remote_batch_preserves_per_bucket_order_once_reserve_can_suspend() {
        let dir = temp_dir("order-apply-remote-batch");
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(64)
            .spill_wait_timeout(Duration::from_secs(5));
        let shard = Shard::<u32, String>::new(
            SmolStr::new("order-apply-remote-batch"),
            Mode::Local,
            NodeId::from(1u64),
            u64::MAX,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));

        let held = tier
            .reserve(64, Duration::from_secs(5))
            .await
            .expect("the whole (tiny) queue is free before this call");

        let ver = hlc(1, 1);
        let first = WireRecord {
            key: key_bytes(&7u32),
            value: Some(Bytes::from(postcard::to_stdvec("first").expect("encode"))),
            ver,
            expires_at_ms: None,
        };
        let second = WireRecord {
            key: key_bytes(&7u32),
            value: Some(Bytes::from(postcard::to_stdvec("second").expect("encode"))),
            ver,
            expires_at_ms: None,
        };
        let mut batch_fut = ShardOps::apply_remote_batch(&shard, vec![first, second]);
        let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut batch_fut).await;
        assert!(
            not_yet.is_err(),
            "this batch's reserve_for_batch call must still be waiting on reserve()"
        );

        drop(held);

        tokio::time::timeout(Duration::from_secs(5), &mut batch_fut)
            .await
            .expect("apply_remote_batch resumes once the reservation frees room");
        assert_eq!(
            shard.get(&7).await,
            Some("first".to_string()),
            "the batch's own arrival order survives the suspension: \"first\" applies before \
             \"second\", so the equal-Hlc \"second\" write loses to it, exactly as it would \
             with no suspension at all"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Pins `retry_reservation_deficit`'s closing fallback end to end:
    /// once its retry budget is exhausted with a real deficit outstanding,
    /// it must call `Engine::enforce_capacity` itself rather than leave
    /// `max_capacity` exceeded. The four keys collide into stripe 0,
    /// the `start_bucket` both call sites pass.
    #[tokio::test]
    async fn retry_reservation_deficit_deletes_the_leftover_victim_once_its_own_budget_is_exhausted()
     {
        let dir = temp_dir("retry-deficit-shard-fallback");
        let value = "x".repeat(20);
        let encoded_len = postcard::to_stdvec(&value)
            .expect("test value encodes")
            .len();

        let mut keys = Vec::new();
        for k in 1..200_000u32 {
            let kb = key_bytes(&k);
            if engine::stripe_index_from_hash(engine::hash_key_bytes(kb.as_ref())) != 0 {
                continue;
            }
            keys.push(k);
            if keys.len() == 4 {
                break;
            }
        }
        assert_eq!(
            keys.len(),
            4,
            "1024 stripes; four keys landing in stripe 0 specifically are found quickly"
        );

        let record_len_0 = spill::spill_record_len(key_bytes(&keys[0]).len(), encoded_len);

        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            // One record's worth: the reservation clamps down to this
            // tiny tier total, covering at most one victim.
            .flush_queue_bytes(u64::from(record_len_0))
            .spill_wait_timeout(Duration::from_millis(150));
        let shard = Shard::<u32, String>::new(
            SmolStr::new("retry-deficit-shard-fallback"),
            Mode::Local,
            NodeId::from(1u64),
            30,
            None,
            None,
        )
        .with_weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX))
        .with_spill(&cfg)
        .expect("tier opens");
        let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
        // Paused before this call enqueues anything, so nothing ever
        // frees a byte back.
        tier.pause_flusher();

        shard
            .insert_many(keys.iter().map(|&k| (k, value.clone())))
            .await
            .expect(
                "insert_many completes even though most of its own evictions are refused \
                 for the whole run",
            );

        let mut resident_or_spilled = 0;
        for &k in &keys {
            if shard.get(&k).await.is_some() {
                resident_or_spilled += 1;
            }
        }
        assert!(
            resident_or_spilled < keys.len(),
            "keep_resident_when_refused defaults to false: retry_reservation_deficit's own \
             closing fallback must physically delete at least the victim a reservation \
             retry gave up on, not leave every key resident with max_capacity exceeded \
             forever because a background retry ran out of budget"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `retry_reservation_deficit` leaves the batch's own entries for
    /// last: its closing pass starts in stripe 0, where the batch wrote
    /// a key, and evicts an older key there instead. The flusher stays
    /// paused and the flush queue holds one record, so the batch's
    /// reservation covers one hand-off and the retry times out.
    #[tokio::test]
    async fn retry_reservation_deficit_keeps_the_batchs_own_entries() {
        let dir = temp_dir("retry-deficit-keeps-the-batch");
        let value = "x".repeat(20);
        let encoded_len = postcard::to_stdvec(&value)
            .expect("test value encodes")
            .len();
        let stripe =
            |k: u32| engine::stripe_index_from_hash(engine::hash_key_bytes(&key_bytes(&k)));
        // Keys of two encoded bytes alike, so one record length fits each.
        let in_stripe_zero = (128..16_384u32)
            .find(|&k| stripe(k) == 0)
            .expect("a key in stripe 0");
        let others: Vec<u32> = (128..16_384u32)
            .filter(|&k| stripe(k) != 0)
            .take(3)
            .collect();
        let [older_a, older_b, elsewhere] = others[..] else {
            panic!("three keys outside stripe 0");
        };
        let record_len = spill::spill_record_len(key_bytes(&in_stripe_zero).len(), encoded_len);
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(u64::from(record_len))
            .spill_wait_timeout(Duration::from_millis(150));
        let shard = Shard::<u32, String>::new(
            SmolStr::new("retry-deficit-keeps-the-batch"),
            Mode::Local,
            NodeId::from(1u64),
            40,
            None,
            None,
        )
        .with_weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX))
        .with_spill(&cfg)
        .expect("tier opens");
        let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
        tier.pause_flusher();
        for k in [older_a, older_b] {
            shard.insert(k, value.clone()).await.expect("insert");
        }

        shard
            .insert_many([(in_stripe_zero, value.clone()), (elsewhere, value.clone())])
            .await
            .expect("insert_many");

        assert_eq!(shard.get_sync(&in_stripe_zero), Some(value.clone()));
        assert_eq!(shard.get_sync(&elsewhere), Some(value.clone()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Pins that `retry_reservation_deficit` shares
    /// `reserve_for_batch`'s deadline instead of opening a second, fresh
    /// `spill_wait_timeout` window: the whole `apply_grouped` call stays
    /// bounded by one timeout total. A background task holds the tier's
    /// admission budget for `HOG_HOLD` first, so a shared deadline
    /// (finishes near `SPILL_WAIT_TIMEOUT`) is distinguishable from a
    /// fresh one (`HOG_HOLD` + `SPILL_WAIT_TIMEOUT`).
    #[tokio::test]
    async fn apply_grouped_bounds_its_whole_reservation_wait_to_one_spill_wait_timeout() {
        const SPILL_WAIT_TIMEOUT: Duration = Duration::from_millis(300);
        /// Half the budget, leaving a clear gap between the "shared" and
        /// "fresh" deadline totals so scheduling jitter cannot blur them.
        const HOG_HOLD: Duration = Duration::from_millis(150);

        let dir = temp_dir("retry-deficit-shared-budget");
        let value = "x".repeat(20);
        let encoded_len = postcard::to_stdvec(&value)
            .expect("test value encodes")
            .len();

        let mut keys = Vec::new();
        for k in 1..200_000u32 {
            let kb = key_bytes(&k);
            if engine::stripe_index_from_hash(engine::hash_key_bytes(kb.as_ref())) != 0 {
                continue;
            }
            keys.push(k);
            if keys.len() == 4 {
                break;
            }
        }
        assert_eq!(
            keys.len(),
            4,
            "1024 stripes; four keys landing in stripe 0 specifically are found quickly"
        );

        let record_len_0 = spill::spill_record_len(key_bytes(&keys[0]).len(), encoded_len);

        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(u64::from(record_len_0))
            .spill_wait_timeout(SPILL_WAIT_TIMEOUT);
        let shard = Shard::<u32, String>::new(
            SmolStr::new("retry-deficit-shared-budget"),
            Mode::Local,
            NodeId::from(1u64),
            30,
            None,
            None,
        )
        .with_weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX))
        .with_spill(&cfg)
        .expect("tier opens");
        let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
        tier.pause_flusher();

        // Holds the whole admission budget for `HOG_HOLD`, forcing
        // `reserve_for_batch` below to wait a known slice of its timeout.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let hog_tier = Arc::clone(&tier);
        let hog = tokio::spawn(async move {
            let reservation = hog_tier
                .reserve(record_len_0, Duration::from_secs(5))
                .await
                .expect("the whole (single-record) queue is free before this task ever runs");
            let _ = ready_tx.send(());
            tokio::time::sleep(HOG_HOLD).await;
            drop(reservation);
        });
        ready_rx
            .await
            .expect("the hog task reserves the whole queue before insert_many starts racing it");

        let started = std::time::Instant::now();
        shard
            .insert_many(keys.iter().map(|&k| (k, value.clone())))
            .await
            .expect(
                "insert_many completes even though most of its own evictions are refused \
                 for the whole run",
            );
        let elapsed = started.elapsed();
        hog.await.expect("the hog task never panics");

        assert!(
            elapsed < SPILL_WAIT_TIMEOUT + Duration::from_millis(100),
            "reserve_for_batch's own wait ({HOG_HOLD:?}, out for the hog) plus \
             retry_reservation_deficit's own retry must share one \
             {SPILL_WAIT_TIMEOUT:?} budget, not a fresh one each: elapsed {elapsed:?} \
             should land around {SPILL_WAIT_TIMEOUT:?}, well short of \
             {HOG_HOLD:?} + {SPILL_WAIT_TIMEOUT:?}, which is what a second, independent \
             full-budget wait would cost"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Pins that `apply_grouped`'s batch-wide `Reservation` reaches a real
    /// eviction pass: every write past `max_capacity` still lands, never a
    /// plain delete.
    #[tokio::test]
    async fn apply_grouped_threads_a_real_reservation_through_a_real_eviction_pass() {
        let dir = temp_dir("reservation-through-real-eviction");
        // Generous budget: the point is that the reservation reaches
        // real eviction, not byte-budget precision.
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(1 << 16)
            .spill_wait_timeout(Duration::from_secs(5));
        let shard = Shard::<u32, String>::new(
            SmolStr::new("reservation-through-real-eviction"),
            Mode::Local,
            NodeId::from(1u64),
            5, // weight 1 per entry with no custom weigher: caps at 5 live entries
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");

        shard
            .insert_many((0..10u32).map(|k| (k, "v".repeat(5))))
            .await
            .expect("insert_many");

        // Every write landed, resident or spilled, never a plain delete.
        for k in 0..10u32 {
            assert!(
                shard.get(&k).await.is_some(),
                "key {k} is present, whether resident or spilled"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A batch whose evictions need more than the whole flush queue,
    /// local (`insert_many`) or replicated (`apply_remote_batch`), hands
    /// its first reservation's unspent budget back before the retry asks
    /// for more. The retry, clamped to the whole queue, is then granted
    /// as the flusher drains, rather than waiting out
    /// `spill_wait_timeout` and evicting with no reservation at all.
    ///
    /// Every record is the same length and the queue holds ten and a
    /// half of them, so the first reservation always leaves half a
    /// record unspent; the flusher stays paused through the per-bucket
    /// loop, so the retry's deficit always exceeds the whole queue.
    #[tokio::test]
    async fn a_batch_past_a_clamped_reservation_never_waits_out_its_budget() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        const KEYS: u32 = 200;
        // Keys 1000.. encode in two bytes and "v" x 10 in eleven.
        const FIRST_KEY: u32 = 1_000;
        let record_len = u64::from(crate::store::spill::spill_record_len(2, 11));
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        for (name, remote) in [("clamped-local", false), ("clamped-remote", true)] {
            let dir = temp_dir(name);
            let cfg = SpillConfig::new(&dir, 1 << 20)
                .region_bytes(64 * 1024)
                .flush_queue_bytes(record_len * 10 + record_len / 2)
                .spill_wait_timeout(Duration::from_secs(5));
            let shard = Shard::<u32, String>::new(
                SmolStr::new(name),
                Mode::Local,
                NodeId::from(1u64),
                5,
                None,
                None,
            )
            .with_spill(&cfg)
            .expect("tier opens");
            let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
            tier.pause_flusher();
            let resume = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(100));
                tier.resume_flusher();
            });

            let keys = FIRST_KEY..FIRST_KEY + KEYS;
            let started = std::time::Instant::now();
            if remote {
                let records = keys
                    .clone()
                    .map(|k| wire_record(k, "vvvvvvvvvv", hlc(u64::from(k), 2)))
                    .collect();
                ShardOps::apply_remote_batch(&shard, records).await;
            } else {
                shard
                    .insert_many(keys.clone().map(|k| (k, "v".repeat(10))))
                    .await
                    .expect("insert_many");
            }
            let took = started.elapsed();
            resume.join().expect("the resume thread finishes");

            let (mut timeouts, mut dropped) = (0, 0);
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                let of_this_cache = key
                    .key()
                    .labels()
                    .any(|label| label.key() == "cache" && label.value() == name);
                if let (true, DebugValue::Counter(n)) = (of_this_cache, value) {
                    match key.key().name() {
                        "sundog_spill_wait_timeouts_total" => timeouts += n,
                        "sundog_spill_dropped_total" => dropped += n,
                        _ => {}
                    }
                }
            }
            assert_eq!(
                timeouts, 0,
                "{name}: no reservation wait runs out ({took:?})"
            );
            assert_eq!(dropped, 0, "{name}: every eviction reaches disk");
            assert!(
                took < Duration::from_secs(4),
                "{name}: the batch finishes well inside its 5s budget, took {took:?}"
            );
            for k in keys {
                assert!(
                    shard.get(&k).await.is_some(),
                    "{name}: key {k} is present, resident or spilled"
                );
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// A victim whose spill record needs more than the whole flush queue
    /// can never be admitted. The first retry holds the whole queue and
    /// covers nothing, so the batch goes straight to its closing
    /// no-reservation pass rather than retrying, or waiting, until its
    /// 5s budget runs out; that pass still brings the shard back to its
    /// cap, dropping every victim as `queue_full` on this `Mode::Local`
    /// tier.
    #[tokio::test]
    async fn a_victim_larger_than_the_flush_queue_ends_the_retries_at_once() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        const NAME: &str = "oversize-victims";
        const KEYS: u32 = 20;
        const CAP: u32 = 2;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let dir = temp_dir(NAME);
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(64 * 1024)
            .flush_queue_bytes(u64::from(crate::store::spill::spill_record_len(2, 11)))
            .spill_wait_timeout(Duration::from_secs(5));
        let shard = Shard::<u32, String>::new(
            SmolStr::new(NAME),
            Mode::Local,
            NodeId::from(1u64),
            u64::from(CAP),
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");

        let started = std::time::Instant::now();
        shard
            .insert_many((1_000..1_000 + KEYS).map(|k| (k, "v".repeat(200))))
            .await
            .expect("insert_many");
        let took = started.elapsed();

        // One snapshot for every counter: a snapshot drains them.
        let counters = snapshotter.snapshot().into_vec();
        let counter = |name: &str| -> u64 {
            counters
                .iter()
                .filter(|(key, ..)| key.key().name() == name)
                .map(|(.., value)| match value {
                    DebugValue::Counter(n) => *n,
                    _ => 0,
                })
                .sum()
        };
        let dropped_for = |reason: &str| -> u64 {
            counters
                .iter()
                .filter(|(key, ..)| {
                    key.key().name() == "sundog_spill_dropped_total"
                        && key
                            .key()
                            .labels()
                            .any(|l| l.key() == "reason" && l.value() == reason)
                })
                .map(|(.., value)| match value {
                    DebugValue::Counter(n) => *n,
                    _ => 0,
                })
                .sum()
        };
        assert_eq!(
            counter("sundog_spill_wait_timeouts_total"),
            0,
            "no reservation wait runs out ({took:?})"
        );
        assert_eq!(
            shard.entry_count().await,
            u64::from(CAP),
            "the closing pass brings the shard back to its cap"
        );
        assert_eq!(
            dropped_for("queue_full"),
            u64::from(KEYS - CAP),
            "every victim the queue could never admit is dropped as queue_full"
        );
        assert_eq!(
            counter("sundog_spill_dropped_total"),
            dropped_for("queue_full"),
            "a Local write's drops record no other reason, `deferred` among them"
        );
        assert!(
            took < Duration::from_secs(2),
            "the batch ends its retries at once, took {took:?} of its 5s budget"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A read that promotes a spilled entry back to RAM makes room for
    /// it first, through `get` and through `get_or_load`'s spilled-key
    /// arm alike: reading every key of a shard spilled far past its cap
    /// leaves at most the cap resident, every key still live, no loader
    /// called, and the last key read in RAM. With 1024 stripes and a
    /// cap of four, each key is alone in its stripe, so a pass that
    /// sampled the promoted entry's own stripe after reinstalling it
    /// would evict that entry straight back to disk.
    ///
    /// Every hand-off installs before the last key is read. A last key
    /// still in a hand-off reads as a RAM hit, and the flusher's later
    /// install leaves it on disk however the read behaves.
    #[tokio::test]
    async fn reading_spilled_keys_keeps_the_shard_within_its_cap() {
        const KEYS: u32 = 24;
        const CAP: u32 = 4;
        let (shard, dir) = spill_shard("promote-within-cap", u64::from(CAP));
        shard
            .insert_many((0..KEYS).map(|k| (k, format!("value-{k}"))))
            .await
            .expect("insert_many");
        let resident =
            |shard: &Shard<u32, String>| (0..KEYS).filter(|k| shard.get_sync(k).is_some()).count();
        assert!(
            poll_until(POLL_TIMEOUT, || resident(&shard) <= CAP as usize).await,
            "the writes spill all but the cap"
        );

        let loads = AtomicUsize::new(0);
        for k in 0..KEYS {
            if k == KEYS - 1 {
                assert!(
                    poll_until(POLL_TIMEOUT, || {
                        shard.engine.debug_pending_spill_weight() == 0
                    })
                    .await,
                    "every hand-off installs before the last key is read"
                );
            }
            let value = if k % 2 == 0 {
                shard.get(&k).await
            } else {
                shard
                    .get_or_load(&k, async |_| {
                        loads.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, std::io::Error>(String::new())
                    })
                    .await
                    .ok()
            };
            assert_eq!(value, Some(format!("value-{k}")), "key {k} reads back");
        }
        assert_eq!(
            loads.load(Ordering::Relaxed),
            0,
            "every key reads back off disk"
        );
        assert!(
            poll_until(POLL_TIMEOUT, || resident(&shard) <= CAP as usize).await,
            "reads leave at most the cap resident, {} of {KEYS} are",
            resident(&shard)
        );
        let (live, weight) = shard.engine.debug_totals();
        assert_eq!(
            live,
            u64::from(KEYS),
            "every key stays live, in RAM or on disk"
        );
        assert!(
            weight <= u64::from(CAP),
            "resident weight {weight} is within the cap"
        );
        assert!(
            shard.get_sync(&(KEYS - 1)).is_some(),
            "the last key read stays in RAM"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A spilled entry heavier than the whole cap reads back off disk
    /// on every read and stays there: no eviction makes room for it,
    /// so reinstalling it would leave the shard over its cap.
    #[tokio::test]
    async fn a_spilled_entry_heavier_than_the_cap_reads_from_disk_and_stays_there() {
        const CAP: u64 = 10;
        let dir = temp_dir("oversize-promotion");
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let shard = Shard::<u32, String>::new(
            SmolStr::new("oversize-promotion"),
            Mode::Local,
            NodeId::from(1u64),
            CAP,
            None,
            None,
        )
        .with_weigher(|_key: &u32, value: &String| u32::try_from(value.len()).unwrap_or(u32::MAX))
        .with_spill(&cfg)
        .expect("tier opens");
        let big = "x".repeat(64);
        shard.insert(1, big.clone()).await.expect("insert");
        assert!(
            poll_until(POLL_TIMEOUT, || shard.get_sync(&1).is_none()).await,
            "the write spills the entry heavier than the cap"
        );
        for read in 1..=3 {
            assert_eq!(shard.get(&1).await, Some(big.clone()), "read {read}");
            assert!(
                shard.get_sync(&1).is_none(),
                "read {read} leaves the entry on disk"
            );
        }
        assert_eq!(shard.engine.debug_totals(), (1, 0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Spills all but the cap of `KEYS` keys in a shard of `mode`, takes
    /// the whole flush queue, then reads every spilled key twice, first
    /// with `get`, then with `get_or_load` and a loader that fails the
    /// test if called: each read answers its value from disk, no entry
    /// is deleted, the keys stay spilled for want of room, and the
    /// refusals count as `deferred`, never as a delete reason: each read's
    /// scan refuses the victim of each of the `CAP` resident stripes
    /// once. With the queue free again, a read promotes.
    async fn reads_behind_a_full_queue_delete_nothing(mode: Mode, name: &str) {
        use crate::store::spill::dropped_since_last_snapshot;
        const KEYS: u32 = 24;
        const CAP: u32 = 4;
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let (shard, dir) = spill_shard_in(name, name, mode, u64::from(CAP));
        shard
            .insert_many((0..KEYS).map(|k| (k, format!("value-{k}"))))
            .await
            .expect("insert_many");
        let resident =
            |shard: &Shard<u32, String>| (0..KEYS).filter(|k| shard.get_sync(k).is_some()).count();
        assert!(
            poll_until(POLL_TIMEOUT, || {
                resident(&shard) <= CAP as usize && shard.engine.debug_pending_spill_weight() == 0
            })
            .await,
            "the writes spill all but the cap, every hand-off installed"
        );
        let spilled: Vec<u32> = (0..KEYS).filter(|k| shard.get_sync(k).is_none()).collect();
        assert_eq!(spilled.len(), (KEYS - CAP) as usize);
        assert!(
            dropped_since_last_snapshot(&snapshotter, name).is_empty(),
            "the writes drop nothing"
        );

        let hold = shard.debug_hold_flush_queue().await;
        let loads = AtomicUsize::new(0);
        for round in 1..=2 {
            for &key in &spilled {
                let value = if round == 1 {
                    shard.get(&key).await
                } else {
                    shard
                        .get_or_load(&key, async |_| {
                            loads.fetch_add(1, Ordering::Relaxed);
                            Ok::<_, std::io::Error>(String::new())
                        })
                        .await
                        .ok()
                };
                assert_eq!(
                    value,
                    Some(format!("value-{key}")),
                    "round {round}: key {key} answers from disk"
                );
            }
        }
        assert_eq!(loads.load(Ordering::Relaxed), 0, "no read calls the loader");

        assert_eq!(
            shard.engine.debug_totals(),
            (u64::from(KEYS), u64::from(CAP)),
            "every key stays live and the resident weight is unchanged"
        );
        for key in 0..KEYS {
            assert_eq!(
                shard.get_sync(&key).is_none(),
                spilled.contains(&key),
                "key {key} keeps its place, spilled or resident"
            );
        }
        let dropped = dropped_since_last_snapshot(&snapshotter, name);
        assert_eq!(
            dropped.keys().collect::<Vec<_>>(),
            ["deferred"],
            "the refusals count as deferred and nothing else: {dropped:?}"
        );
        assert_eq!(
            dropped["deferred"],
            u64::from(CAP) * 2 * spilled.len() as u64,
            "each read's scan refuses the victim of each resident stripe once"
        );

        drop(hold);
        let key = spilled[0];
        assert_eq!(shard.get(&key).await, Some(format!("value-{key}")));
        assert!(
            poll_until(POLL_TIMEOUT, || shard.get_sync(&key).is_some()).await,
            "with room in the queue the read promotes"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_local_shard_reads_spilled_keys_behind_a_full_queue_without_deleting_any() {
        reads_behind_a_full_queue_delete_nothing(Mode::Local, "reads-full-queue-local").await;
    }

    #[tokio::test]
    async fn an_invalidation_shard_reads_spilled_keys_behind_a_full_queue_without_deleting_any() {
        reads_behind_a_full_queue_delete_nothing(
            Mode::Invalidation,
            "reads-full-queue-invalidation",
        )
        .await;
    }

    /// Concurrent `get_or_load` calls of a spilled key that its read
    /// cannot promote, one heavier than the whole cap, still collapse
    /// to one disk read: the owner hands the value to every joined
    /// waiter instead of leaving each to read the disk again.
    ///
    /// The test holds every read permit until all the calls have
    /// started, so the owner waits for a permit while the others join
    /// it. Unheld, a blocking thread that finishes the read before the
    /// owner first polls it ends the owner's load inside that poll, and
    /// the next call starts a load of its own.
    #[tokio::test]
    async fn concurrent_loads_of_an_unpromotable_spilled_key_read_the_disk_once() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        const CALLS: usize = 16;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let dir = temp_dir("unpromotable-single-flight");
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let shard = Shard::<u32, String>::new(
            SmolStr::new("unpromotable-single-flight"),
            Mode::Local,
            NodeId::from(1u64),
            10,
            None,
            None,
        )
        .with_weigher(|_key: &u32, value: &String| u32::try_from(value.len()).unwrap_or(u32::MAX))
        .with_spill(&cfg)
        .expect("tier opens");
        let big = "x".repeat(64);
        shard.insert(1, big.clone()).await.expect("insert");
        assert!(
            poll_until(POLL_TIMEOUT, || shard.get_sync(&1).is_none()).await,
            "the write spills the entry heavier than the cap"
        );
        let _ = snapshotter.snapshot();

        let held = hold_spill_reads(&shard).await;
        let loads = AtomicUsize::new(0);
        let mut calls = std::pin::pin!(futures::future::join_all((0..CALLS).map(|_| {
            shard.get_or_load(&1, async |_| {
                loads.fetch_add(1, Ordering::Relaxed);
                Ok::<_, std::io::Error>(String::new())
            })
        })));
        assert!(
            futures::poll!(calls.as_mut()).is_pending(),
            "the owner waits for a read permit and every other call joins its load"
        );
        drop(held);
        let answers = calls.await;
        assert!(
            answers
                .iter()
                .all(|answer| answer.as_ref().ok() == Some(&big)),
            "every call answers the spilled value"
        );
        assert_eq!(loads.load(Ordering::Relaxed), 0, "no call runs the loader");
        let disk_hits: u64 = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, ..)| {
                key.key().name() == "sundog_spill_reads_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == "hit")
            })
            .map(|(.., value)| match value {
                DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum();
        assert_eq!(disk_hits, 1, "{CALLS} concurrent loads read the disk once");
        assert!(shard.get_sync(&1).is_none(), "the entry stays on disk");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `load_many`'s counterpart of
    /// [`concurrent_loads_of_an_unpromotable_spilled_key_read_the_disk_once`]:
    /// concurrent calls of a spilled key too heavy to promote share the
    /// owner's one disk read and never reach the loader. The read
    /// permits are held while the calls start, for the same reason.
    #[tokio::test]
    async fn concurrent_load_manys_of_an_unpromotable_spilled_key_read_the_disk_once() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        const CALLS: usize = 16;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let dir = temp_dir("unpromotable-load-many");
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let loads = Arc::new(AtomicUsize::new(0));
        let shard = Shard::<u32, String>::new(
            SmolStr::new("unpromotable-load-many"),
            Mode::Local,
            NodeId::from(1u64),
            10,
            None,
            None,
        )
        .with_weigher(|_key: &u32, value: &String| u32::try_from(value.len()).unwrap_or(u32::MAX))
        .with_loader(crate::store::load::Loader::batch({
            let loads = Arc::clone(&loads);
            move |_keys: Vec<u32>| {
                loads.fetch_add(1, Ordering::Relaxed);
                async { Ok::<_, std::io::Error>(HashMap::new()) }
            }
        }))
        .with_spill(&cfg)
        .expect("tier opens");
        let big = "x".repeat(64);
        shard.insert(1, big.clone()).await.expect("insert");
        assert!(
            poll_until(POLL_TIMEOUT, || shard.get_sync(&1).is_none()).await,
            "the write spills the entry heavier than the cap"
        );
        let _ = snapshotter.snapshot();

        let held = hold_spill_reads(&shard).await;
        let mut calls = std::pin::pin!(futures::future::join_all(
            (0..CALLS).map(|_| shard.load_many([1u32]))
        ));
        assert!(
            futures::poll!(calls.as_mut()).is_pending(),
            "the owner waits for a read permit and every other call joins its load"
        );
        drop(held);
        let answers = calls.await;
        assert!(
            answers
                .iter()
                .all(|answer| answer.as_ref().ok().and_then(|found| found.get(&1)) == Some(&big)),
            "every call answers the spilled value"
        );
        assert_eq!(
            loads.load(Ordering::Relaxed),
            0,
            "no call reaches the loader"
        );
        let disk_hits: u64 = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, ..)| {
                key.key().name() == "sundog_spill_reads_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == "hit")
            })
            .map(|(.., value)| match value {
                DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum();
        assert_eq!(
            disk_hits, 1,
            "{CALLS} concurrent load_manys read the disk once"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A `load_many` dropped while it waits to read a later key's
    /// spilled value frees the in-flight entries of the keys it already
    /// claimed: a later load of such a key runs rather than join a load
    /// that never ends.
    #[tokio::test]
    async fn a_load_many_dropped_mid_claim_frees_the_keys_it_claimed() {
        let dir = temp_dir("load-many-dropped-mid-claim");
        let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
        let shard = Shard::<u32, String>::new(
            SmolStr::new("load-many-dropped-mid-claim"),
            Mode::Local,
            NodeId::from(1u64),
            1,
            None,
            None,
        )
        .with_loader(crate::store::load::Loader::batch(
            |keys: Vec<u32>| async move {
                Ok::<_, std::io::Error>(
                    keys.into_iter()
                        .map(|key| (key, format!("loaded-{key}")))
                        .collect::<HashMap<_, _>>(),
                )
            },
        ))
        .with_spill(&cfg)
        .expect("tier opens");
        shard.insert(2, "two".to_string()).await.expect("insert 2");
        shard
            .insert(3, "three".to_string())
            .await
            .expect("insert 3");
        assert!(
            poll_until(POLL_TIMEOUT, || {
                shard.get_sync(&2).is_none() || shard.get_sync(&3).is_none()
            })
            .await,
            "one of the two keys spills under the one-entry cap"
        );
        let spilled = if shard.get_sync(&2).is_none() { 2 } else { 3 };

        let held = hold_spill_reads(&shard).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), shard.load_many([1, spilled]))
                .await
                .is_err(),
            "the call claims key 1, then waits on the held read permits for the spilled key"
        );
        drop(held);

        let loaded = tokio::time::timeout(Duration::from_secs(5), shard.load(&1))
            .await
            .expect("a later load of the claimed key runs rather than hang");
        assert_eq!(loaded.expect("load"), Some("loaded-1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A write burst far past the cap settles within it once the
    /// flusher installs its hand-offs: an eviction pass that found
    /// nothing to evict leaves the rest to the flusher only when the
    /// hand-offs in flight are all that hold the shard over its cap.
    #[tokio::test]
    async fn a_write_burst_past_the_cap_settles_within_it() {
        const KEYS: u32 = 8_000;
        const CAP: u64 = 2_000;
        let dir = temp_dir("write-burst-within-cap");
        // Generous past the 2s default, so a contended CI disk slows the
        // burst rather than dropping a victim at its reservation wait.
        let cfg = SpillConfig::new(&dir, 4 << 20)
            .region_bytes(256 * 1024)
            .spill_wait_timeout(Duration::from_secs(20));
        let shard = Shard::<u32, String>::new(
            SmolStr::new("write-burst-within-cap"),
            Mode::Local,
            NodeId::from(1u64),
            CAP,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        shard
            .insert_many((0..KEYS).map(|k| (k, format!("value-{k}"))))
            .await
            .expect("insert_many");
        let resident = || -> u64 {
            (0..KEYS)
                .filter(|k| shard.get_sync(k).is_some())
                .map(|_| 1)
                .sum()
        };
        assert!(
            poll_until(POLL_TIMEOUT, || resident() <= CAP).await,
            "the burst settles within the cap, {} of {KEYS} resident",
            resident()
        );
        let (live, weight) = shard.engine.debug_totals();
        assert_eq!(live, u64::from(KEYS), "every key stays live");
        assert!(weight <= CAP, "resident weight {weight} is within the cap");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Extends [`insert_sync_writes_a_value_with_no_async_runtime`]:
    /// `insert_sync` completes with no runtime even while another task
    /// holds the whole reservation.
    #[test]
    fn insert_sync_completes_synchronously_while_another_task_holds_the_whole_reservation() {
        let dir = temp_dir("insert-sync-under-reservation");
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(64);
        let shard = Shard::<u32, String>::new(
            SmolStr::new("insert-sync-under-reservation"),
            Mode::Local,
            NodeId::from(1u64),
            u64::MAX,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("a fresh runtime builds");
            rt.block_on(async move {
                let reservation = tier
                    .reserve(64, Duration::from_secs(30))
                    .await
                    .expect("the whole queue is free before this call");
                ready_tx.send(()).expect("the main thread is listening");
                let _ = release_rx.recv();
                drop(reservation);
            });
        });
        ready_rx
            .recv()
            .expect("the holder thread reports it holds the reservation");

        // Runs with no async runtime here while the reservation stays
        // live on another thread.
        shard.insert_sync(1, "a".to_string()).expect("insert_sync");
        assert_eq!(shard.get_sync(&1), Some("a".to_string()));

        release_tx.send(()).expect("the holder thread is listening");
        holder.join().expect("the holder thread does not panic");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
