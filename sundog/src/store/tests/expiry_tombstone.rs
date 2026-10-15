//! The tombstone a sweep leaves for an expired entry in the replicated
//! modes: what keeps it out, what it is served as, how it is collected and
//! received, and the modes and resolvers that leave none.

use std::sync::atomic::{AtomicU64, Ordering};

use super::merging::SumResolver;
use super::*;

const START_MS: u64 = 1_000_000;
const EXPIRES_AT_MS: u64 = START_MS + 100;
const TOMBSTONE_TTL: Duration = Duration::from_secs(60);
const TOMBSTONE_MAX_TTL: Duration = Duration::from_secs(600);

fn writer(wall_ms: u64) -> Hlc {
    hlc(wall_ms, 2)
}

/// A record for key 1 that expires at [`EXPIRES_AT_MS`].
fn expiring(value: &str, wall_ms: u64) -> WireRecord {
    let mut rec = wire_record(1, value, writer(wall_ms));
    rec.expires_at_ms = Some(EXPIRES_AT_MS);
    rec
}

fn manual_clock() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(START_MS))
}

fn clocked<V>(mode: Mode, clock: &Arc<AtomicU64>) -> Shard<u32, V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let reader = Arc::clone(clock);
    Shard::<u32, V>::new(
        SmolStr::new("test"),
        mode,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_tombstone_ttl(TOMBSTONE_TTL)
    .with_tombstone_max_ttl(TOMBSTONE_MAX_TTL)
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
}

/// A `Mode::Replicated` shard holding key 1's expired overwrite, not yet
/// swept, with the clock past its deadline.
async fn shard_with_an_expired_overwrite(clock: &Arc<AtomicU64>) -> Shard<u32, String> {
    let s = clocked::<String>(Mode::Replicated, clock);
    ShardOps::apply_remote(&s, wire_record(1, "old", writer(1_000))).await;
    ShardOps::apply_remote(&s, expiring("new", 2_000)).await;
    clock.store(EXPIRES_AT_MS + 100, Ordering::SeqCst);
    s
}

async fn takes_the_old_value(s: &Shard<u32, String>) -> bool {
    ShardOps::apply_remote(s, wire_record(1, "old", writer(1_000))).await;
    s.get(&1).await.is_some()
}

#[tokio::test]
async fn a_swept_expired_entry_is_collected_by_the_delete_retention_counted_from_its_expiry() {
    let clock = manual_clock();
    let s = shard_with_an_expired_overwrite(&clock).await;
    ShardOps::run_pending_tasks(&s).await;
    assert_eq!(s.tombstone_count(), 1);

    let ttl_deadline = EXPIRES_AT_MS + 60_000;
    clock.store(ttl_deadline - 1, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, false).await;
    assert!(
        !takes_the_old_value(&s).await,
        "within the retention the floor holds"
    );
    assert_digest_matches_full_recompute(&s).await;

    clock.store(ttl_deadline, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, false).await;
    assert_eq!(s.tombstone_count(), 0, "collected when the retention ends");
    assert_digest_matches_full_recompute(&s).await;
    assert!(
        takes_the_old_value(&s).await,
        "past the retention a stale copy is accepted again, as after a delete"
    );
}

#[tokio::test]
async fn a_member_absent_defers_collection_of_an_expiry_tombstone_to_the_cap() {
    let clock = manual_clock();
    let s = shard_with_an_expired_overwrite(&clock).await;
    ShardOps::run_pending_tasks(&s).await;

    clock.store(EXPIRES_AT_MS + 120_000, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, true).await;
    assert_eq!(s.tombstone_count(), 1, "an absent member holds it");
    assert!(!takes_the_old_value(&s).await);

    clock.store(EXPIRES_AT_MS + 600_000, Ordering::SeqCst);
    ShardOps::gc_tombstones(&s, true).await;
    assert_eq!(s.tombstone_count(), 0, "the cap collects it regardless");
}

#[tokio::test]
async fn entry_count_leaves_the_same_floor_as_the_housekeeping_sweep() {
    let clock = manual_clock();
    let s = shard_with_an_expired_overwrite(&clock).await;
    assert_eq!(s.entry_count().await, 0);
    assert_eq!(s.tombstone_count(), 1, "the gauge's own sweep tombstones");
    assert!(!takes_the_old_value(&s).await);
}

#[tokio::test]
async fn the_floor_holds_through_apply_remote_batch() {
    let clock = manual_clock();
    let s = shard_with_an_expired_overwrite(&clock).await;
    ShardOps::run_pending_tasks(&s).await;
    ShardOps::apply_remote_batch(
        &s,
        vec![
            wire_record(1, "old", writer(1_000)),
            wire_record(1, "older", writer(500)),
        ],
    )
    .await;
    assert_eq!(s.get(&1).await, None);
    assert!(!s.local_record(&key_bytes(&1u32)).1.is_live());
}

#[tokio::test]
async fn a_swept_expired_entry_is_served_as_a_tombstone_with_its_expiry() {
    let clock = manual_clock();
    let s = shard_with_an_expired_overwrite(&clock).await;
    let digests = ShardOps::digests(&s).await;
    ShardOps::run_pending_tasks(&s).await;

    assert_eq!(
        ShardOps::digests(&s).await,
        digests,
        "the sweep moves no digest: replicas that sweep at different instants agree"
    );
    let served = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert_eq!(served.len(), 1);
    assert_eq!(served[0].value, None);
    assert_eq!(served[0].ver, writer(2_000));
    assert_eq!(served[0].expires_at_ms, Some(EXPIRES_AT_MS));
    let listed = ShardOps::bucket_entries(&s, bucket_of(key_bytes(&1u32).as_ref())).await;
    assert_eq!(
        listed.iter().map(|kv| kv.version).collect::<Vec<_>>(),
        vec![writer(2_000)],
        "anti-entropy lists the tombstone"
    );
}

#[tokio::test]
async fn a_peer_takes_the_served_tombstone_and_collects_it_at_the_same_instant() {
    let clock = manual_clock();
    let a = shard_with_an_expired_overwrite(&clock).await;
    ShardOps::run_pending_tasks(&a).await;
    let served = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;

    // The peer holds the overwritten value and learns of the expiry by
    // anti-entropy: its older copy goes.
    let b = clocked::<String>(Mode::Replicated, &clock);
    ShardOps::apply_remote(&b, wire_record(1, "old", writer(1_000))).await;
    ShardOps::apply_remote_batch(&b, served.clone()).await;
    assert_eq!(b.get(&1).await, None, "the stale copy is deleted");
    assert_eq!(b.tombstone_count(), 1);

    let ttl_deadline = EXPIRES_AT_MS + 60_000;
    clock.store(ttl_deadline, Ordering::SeqCst);
    ShardOps::gc_tombstones(&a, false).await;
    ShardOps::gc_tombstones(&b, false).await;
    assert_eq!(
        (a.tombstone_count(), b.tombstone_count()),
        (0, 0),
        "both collect at the expiry plus the retention, whenever each learned of it"
    );

    // A replica that collected it does not take it back from one that has
    // not collected yet.
    ShardOps::apply_remote_batch(&a, served).await;
    assert_eq!(
        a.tombstone_count(),
        0,
        "a lapsed tombstone is not re-seeded"
    );
    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&clocked::<String>(Mode::Replicated, &clock)).await,
        "nothing of it is fingerprinted"
    );
}

#[tokio::test]
async fn a_late_peer_applies_a_delete_tombstone_whatever_its_version() {
    let clock = manual_clock();
    let s = clocked::<String>(Mode::Replicated, &clock);
    ShardOps::apply_remote(
        &s,
        WireRecord {
            key: key_bytes(&1u32),
            value: None,
            ver: writer(1),
            expires_at_ms: None,
        },
    )
    .await;
    assert_eq!(s.tombstone_count(), 1, "a delete counts from its arrival");
}

#[tokio::test]
async fn a_node_that_missed_the_overwrite_is_repaired_by_the_swept_tombstone() {
    let clock = manual_clock();
    let a = shard_with_an_expired_overwrite(&clock).await;
    ShardOps::run_pending_tasks(&a).await;
    let b = clocked::<String>(Mode::Replicated, &clock);
    ShardOps::apply_remote(&b, wire_record(1, "old", writer(1_000))).await;
    assert_eq!(
        b.get(&1).await,
        Some("old".into()),
        "b missed the overwrite"
    );

    // What anti-entropy's two directions carry.
    let from_b = ShardOps::records_for(&b, vec![key_bytes(&1u32)]).await;
    ShardOps::apply_remote_batch(&a, from_b).await;
    assert_eq!(a.get(&1).await, None, "the old value does not spread to a");
    let from_a = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    ShardOps::apply_remote_batch(&b, from_a).await;
    assert_eq!(b.get(&1).await, None, "b's stale copy is removed");
    assert_eq!(ShardOps::digests(&a).await, ShardOps::digests(&b).await);
}

#[tokio::test]
async fn a_swept_shortened_lifetime_deletes_a_peers_longer_lived_copy() {
    let clock = manual_clock();
    let a = clocked::<String>(Mode::Replicated, &clock);
    let b = clocked::<String>(Mode::Replicated, &clock);
    a.insert(1, "v".into()).await.expect("insert");
    let long_lived = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    ShardOps::apply_remote_batch(&b, long_lived).await;

    // a shortens the lifetime; b, cut off, never hears.
    assert!(a.expire(&1, Duration::from_secs(30)).await.expect("expire"));
    clock.store(START_MS + 31_000, Ordering::SeqCst);
    ShardOps::run_pending_tasks(&a).await;
    assert_eq!(a.get(&1).await, None);
    assert_eq!(
        b.get(&1).await,
        Some("v".into()),
        "b still holds the old copy"
    );

    let served = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    assert_eq!(
        served[0].value, None,
        "the shortened entry is served as a tombstone"
    );
    ShardOps::apply_remote_batch(&b, served).await;
    assert_eq!(
        b.get(&1).await,
        None,
        "the cut-off peer's longer-lived copy goes instead of bringing the entry back"
    );
}

#[tokio::test]
async fn an_insert_after_the_sweep_supersedes_the_tombstone() {
    let clock = manual_clock();
    let s = shard_with_an_expired_overwrite(&clock).await;
    ShardOps::run_pending_tasks(&s).await;
    assert_eq!(s.tombstone_count(), 1);

    s.insert(1, "fresh".into()).await.expect("insert");

    assert_eq!(s.get(&1).await, Some("fresh".into()));
    assert_eq!(s.tombstone_count(), 0);
    assert_digest_matches_full_recompute(&s).await;
}

#[tokio::test]
async fn a_read_through_fill_after_the_sweep_stores_and_replaces_the_tombstone() {
    let clock = manual_clock();
    let s = shard_with_an_expired_overwrite(&clock).await;
    ShardOps::run_pending_tasks(&s).await;
    let mut events = s.events();

    let loaded = s
        .get_or_load(&1, async |_| Ok::<_, std::io::Error>("loaded".to_string()))
        .await
        .expect("loads");

    assert_eq!(loaded, "loaded");
    assert_eq!(s.get(&1).await, Some("loaded".into()), "the fill is stored");
    assert_eq!(s.tombstone_count(), 0);
    assert!(
        matches!(events.try_recv(), Ok(Event::Created { key: 1, .. })),
        "the fill reports a creation"
    );
    assert_eq!(s.entry_count().await, 1);
}

#[tokio::test]
async fn local_and_invalidation_caches_leave_nothing_behind() {
    for mode in [Mode::Local, Mode::Invalidation] {
        let clock = manual_clock();
        let s = clocked::<String>(mode, &clock);
        ShardOps::apply_remote(&s, expiring("v", 2_000)).await;
        clock.store(EXPIRES_AT_MS + 100, Ordering::SeqCst);
        ShardOps::run_pending_tasks(&s).await;
        assert_eq!(s.tombstone_count(), 0, "{mode:?}");
        assert_eq!(
            s.local_record(&key_bytes(&1u32)).1,
            LocalRecord::Absent,
            "{mode:?}"
        );
    }
}

#[tokio::test]
async fn a_distributed_cache_tombstones_an_owned_expired_entry() {
    let owners = NonZeroU8::new(2).expect("nonzero");
    let node = NodeId::from(1);
    let clock = manual_clock();
    let (tracker, tx) =
        OwnershipTracker::seed(node, &[], &HashMap::new(), &SmolStr::new("test"), owners);
    let view = Arc::new(OwnershipView::compute(
        node,
        vec![node, NodeId::from(2)],
        owners,
    ));
    tx.send(Arc::clone(&view)).expect("receiver alive");
    let reader = Arc::clone(&clock);
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Distributed { owners },
        node,
        u64::MAX,
        None,
        None,
    )
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
    .with_ownership(tracker, Arc::new(ResidencySet::new()));
    let owned = find_key_by_ownership(&view, true);

    let mut rec = wire_record(owned, "v", writer(2_000));
    rec.expires_at_ms = Some(EXPIRES_AT_MS);
    ShardOps::apply_remote(&s, rec).await;
    assert_eq!(s.get(&owned).await, Some("v".into()));
    clock.store(EXPIRES_AT_MS + 100, Ordering::SeqCst);

    ShardOps::run_pending_tasks(&s).await;

    assert_eq!(
        s.tombstone_count(),
        1,
        "a Distributed owner keeps the floor too"
    );
    let old = wire_record(owned, "old", writer(1_000));
    ShardOps::apply_remote(&s, old).await;
    assert_eq!(s.get(&owned).await, None);
}

#[tokio::test]
async fn a_merging_resolver_leaves_no_tombstone_and_keeps_its_deterministic_sweep() {
    let clock = manual_clock();
    let reader = Arc::clone(&clock);
    let s = Shard::<u32, u32>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_resolver(Arc::new(SumResolver))
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)));
    let mut rec = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&5u32).expect("encodes"))),
        ver: writer(2_000),
        expires_at_ms: Some(EXPIRES_AT_MS),
    };
    ShardOps::apply_remote(&s, rec.clone()).await;
    clock.store(EXPIRES_AT_MS + 100, Ordering::SeqCst);

    ShardOps::run_pending_tasks(&s).await;

    assert_eq!(s.tombstone_count(), 0);
    assert_eq!(s.local_record(&key_bytes(&1u32)).1, LocalRecord::Absent);
    // A merging cache reads an expired side as absent whether or not it was
    // swept, so an older fold starts a new value on every node.
    rec.ver = writer(1_000);
    rec.expires_at_ms = None;
    ShardOps::apply_remote(&s, rec).await;
    assert_eq!(s.get(&1).await, Some(5));
}

#[tokio::test]
async fn an_idle_entry_dropped_by_the_sweep_leaves_no_tombstone() {
    let clock = manual_clock();
    let reader = Arc::clone(&clock);
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        Some(Duration::from_secs(10)),
    )
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)));
    s.insert(1, "v".into()).await.expect("insert");
    clock.store(START_MS + 60_000, Ordering::SeqCst);

    ShardOps::run_pending_tasks(&s).await;

    assert_eq!(s.entry_count().await, 0);
    assert_eq!(
        s.tombstone_count(),
        0,
        "a tombstone would delete the key on peers that did not idle"
    );
}

/// A cold part declines a peer's fetch for a key it holds nothing for, so the
/// asker consults the next owner; a held record is served. The tombstone a
/// swept expired entry leaves is held, so the cold owner then answers the
/// fetch with a definitive miss, as it does for any delete, where it declined
/// after a sweep that left nothing.
#[tokio::test]
async fn a_cold_distributed_owner_answers_a_fetch_for_a_swept_expired_key_with_a_miss() {
    use crate::explain::ServeVerdict;
    use crate::verdict::serves_peers;

    let clock = manual_clock();
    let residency = Arc::new(ResidencySet::new());
    let (s, view, _tx) = distributed_shard::<u32, String>(
        NodeId::from(1),
        (1..=3u64).map(NodeId::from).collect(),
        2,
        Arc::clone(&residency),
    );
    let reader = Arc::clone(&clock);
    let s = s.with_clock(Arc::new(move || reader.load(Ordering::SeqCst)));
    let key = find_key_by_ownership(&view, true);
    let part = PartId::of_key(&key_bytes(&key));
    let mut rec = wire_record(key, "v", writer(2_000));
    rec.expires_at_ms = Some(EXPIRES_AT_MS);
    ShardOps::apply_remote(&s, rec).await;
    residency.mark_cold(&[part]);
    let verdict = |s: &Shard<u32, String>| {
        serves_peers(
            &s.current_residency(part),
            &s.local_record(&key_bytes(&key)).1,
        )
    };
    assert_eq!(verdict(&s), ServeVerdict::Serve, "a live entry is served");

    clock.store(EXPIRES_AT_MS + 100, Ordering::SeqCst);
    assert_eq!(
        verdict(&s),
        ServeVerdict::DeclineCold,
        "an expired entry that is not yet swept is not held"
    );
    ShardOps::run_pending_tasks(&s).await;
    assert_eq!(
        verdict(&s),
        ServeVerdict::Serve,
        "the swept entry's tombstone is held: a definitive miss"
    );
}

/// Key `key` holding an older copy, then the tombstone of an entry that
/// expired at [`EXPIRES_AT_MS`] arriving from a peer: whether the older copy
/// survives it.
async fn the_older_copy_survives_the_expiry_tombstone(s: &Shard<u32, String>, key: u32) -> bool {
    ShardOps::apply_remote(s, wire_record(key, "old", writer(1_000))).await;
    let mut tombstone = wire_record(key, "", writer(2_000));
    tombstone.value = None;
    tombstone.expires_at_ms = Some(EXPIRES_AT_MS);
    ShardOps::apply_remote(s, tombstone).await;
    s.get(&key).await.is_some()
}

/// Past the tombstone's `tombstone_ttl` deadline, within its cap.
const PAST_TTL: u64 = EXPIRES_AT_MS + 120_000;
/// Past the tombstone's `tombstone_max_ttl` deadline too.
const PAST_CAP: u64 = EXPIRES_AT_MS + 700_000;

#[tokio::test]
async fn a_new_shard_keeps_an_expiry_tombstone_lapsed_by_ttl_until_a_collection_ends_the_deferral()
{
    let clock = manual_clock();
    let s = clocked::<String>(Mode::Replicated, &clock);
    clock.store(PAST_TTL, Ordering::SeqCst);

    assert!(
        !the_older_copy_survives_the_expiry_tombstone(&s, 1).await,
        "a node that may hold a copy from before it went down takes the tombstone"
    );
    assert_eq!(s.tombstone_count(), 1);
    assert_digest_matches_full_recompute(&s).await;

    ShardOps::gc_tombstones(&s, false).await;
    assert_eq!(s.tombstone_count(), 0, "the first collection takes it");
    assert!(
        the_older_copy_survives_the_expiry_tombstone(&s, 2).await,
        "once collection stops deferring, a lapsed tombstone is a no-op"
    );
    assert_eq!(s.tombstone_count(), 0);
    assert_digest_matches_full_recompute(&s).await;
}

#[tokio::test]
async fn a_deferred_collection_keeps_a_tombstone_lapsed_by_ttl_until_one_that_does_not_defer() {
    let clock = manual_clock();
    let s = clocked::<String>(Mode::Replicated, &clock);
    clock.store(PAST_TTL, Ordering::SeqCst);

    ShardOps::gc_tombstones(&s, true).await;

    assert!(!the_older_copy_survives_the_expiry_tombstone(&s, 1).await);
    ShardOps::gc_tombstones(&s, false).await;
    assert!(the_older_copy_survives_the_expiry_tombstone(&s, 2).await);
}

#[tokio::test]
async fn a_new_shard_drops_an_expiry_tombstone_lapsed_past_its_cap_whatever_it_holds() {
    let clock = manual_clock();
    let s = clocked::<String>(Mode::Replicated, &clock);
    clock.store(PAST_CAP, Ordering::SeqCst);

    assert!(the_older_copy_survives_the_expiry_tombstone(&s, 1).await);
    assert_eq!(s.tombstone_count(), 0);
    ShardOps::apply_remote_batch(
        &s,
        vec![{
            let mut tombstone = wire_record(3, "", writer(2_000));
            tombstone.value = None;
            tombstone.expires_at_ms = Some(EXPIRES_AT_MS);
            tombstone
        }],
    )
    .await;
    assert_eq!(s.tombstone_count(), 0, "nor does it store one over nothing");
}

#[tokio::test]
async fn a_lapsed_expiry_tombstone_converges_in_either_order_with_the_older_copy() {
    for (clock_ms, kept) in [(PAST_TTL, true), (PAST_CAP, false)] {
        let mut states = Vec::new();
        for tombstone_first in [true, false] {
            let clock = manual_clock();
            let s = clocked::<String>(Mode::Replicated, &clock);
            clock.store(clock_ms, Ordering::SeqCst);
            let old = wire_record(1, "old", writer(1_000));
            let mut tombstone = wire_record(1, "", writer(2_000));
            tombstone.value = None;
            tombstone.expires_at_ms = Some(EXPIRES_AT_MS);
            let batch = if tombstone_first {
                vec![tombstone, old]
            } else {
                vec![old, tombstone]
            };
            ShardOps::apply_remote_batch(&s, batch).await;
            assert_eq!(s.tombstone_count(), u64::from(kept), "clock {clock_ms}");
            states.push((s.get(&1).await, ShardOps::digests(&s).await));
        }
        assert_eq!(states[0], states[1], "clock {clock_ms}");
    }
}

#[tokio::test]
async fn a_new_distributed_shard_defers_collection_too() {
    let owners = NonZeroU8::new(2).expect("nonzero");
    let node = NodeId::from(1);
    let clock = manual_clock();
    let (tracker, tx) =
        OwnershipTracker::seed(node, &[], &HashMap::new(), &SmolStr::new("test"), owners);
    let view = Arc::new(OwnershipView::compute(
        node,
        vec![node, NodeId::from(2)],
        owners,
    ));
    tx.send(Arc::clone(&view)).expect("receiver alive");
    let reader = Arc::clone(&clock);
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Distributed { owners },
        node,
        u64::MAX,
        None,
        None,
    )
    .with_tombstone_ttl(TOMBSTONE_TTL)
    .with_tombstone_max_ttl(TOMBSTONE_MAX_TTL)
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
    .with_ownership(tracker, Arc::new(ResidencySet::new()));
    let owned: Vec<u32> = (0..10_000u32)
        .filter(|key| view.owns(PartId::of_key(&key_bytes(key))))
        .take(2)
        .collect();
    clock.store(PAST_TTL, Ordering::SeqCst);

    assert!(!the_older_copy_survives_the_expiry_tombstone(&s, owned[0]).await);
    ShardOps::gc_tombstones(&s, false).await;
    assert!(the_older_copy_survives_the_expiry_tombstone(&s, owned[1]).await);
}

#[tokio::test]
async fn only_a_replicated_or_distributed_shard_starts_deferred() {
    let owners = NonZeroU8::new(2).expect("nonzero");
    let clock = manual_clock();
    for (mode, deferred) in [
        (Mode::Local, false),
        (Mode::Invalidation, false),
        (Mode::Replicated, true),
        (Mode::Distributed { owners }, true),
    ] {
        let s = clocked::<String>(mode, &clock);
        assert_eq!(s.engine.gc_deferring(), deferred, "{mode:?}");
    }

    let s = clocked::<String>(Mode::Local, &clock);
    clock.store(PAST_TTL, Ordering::SeqCst);
    assert!(
        the_older_copy_survives_the_expiry_tombstone(&s, 1).await,
        "a lapsed tombstone is dropped at once"
    );
    assert_eq!(s.tombstone_count(), 0);
}

#[tokio::test]
async fn every_engine_rebuild_keeps_the_start_deferral() {
    let clock = manual_clock();
    let shards = [
        clocked::<String>(Mode::Replicated, &clock).with_capacity_hint(16),
        clocked::<String>(Mode::Replicated, &clock).with_weigher(|_, _| 1),
        clocked::<String>(Mode::Replicated, &clock).with_max_resident_bytes(1 << 30),
    ];
    clock.store(PAST_TTL, Ordering::SeqCst);
    for (index, s) in shards.iter().enumerate() {
        assert!(s.engine.gc_deferring(), "rebuild {index}");
        assert!(
            !the_older_copy_survives_the_expiry_tombstone(s, 1).await,
            "rebuild {index}"
        );
    }
    let local = clocked::<String>(Mode::Local, &clock).with_capacity_hint(16);
    assert!(!local.engine.gc_deferring(), "a rebuilt Local engine");
}
