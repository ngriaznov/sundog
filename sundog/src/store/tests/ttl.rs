//! Per-entry TTLs, `expire`, `touch`, `persist` and the local rearm rule.

use super::*;

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
    assert_eq!(
        applied_writes(s.fan_out_queue().drain()),
        vec![(1, after[0].ver)],
        "the re-arm fans out like any local write, at its rearmed version"
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
/// like any write, and the `crdt` resolvers keep the later of the two
/// deadlines: the value stays and `expire` sets the deadline of an entry
/// that had none.
#[tokio::test]
async fn expire_under_a_crdt_resolver_keeps_the_value_and_sets_the_later_deadline() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let clock = Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    let reader = Arc::clone(&clock);
    let s = Shard::<u32, PnCounter>::new(
        SmolStr::new("test-crdt-expire"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_resolver(Arc::new(PnCounterResolver))
    .with_clock(Arc::new(move || {
        reader.load(std::sync::atomic::Ordering::SeqCst)
    }));
    let writer = crdt::WriterId::new(NodeId::from(11), 1);
    s.merge(1, PnCounter::local_delta(writer, 3))
        .await
        .expect("merge");
    assert_eq!(s.ttl_of(&1), Some(Ttl::Never));

    assert!(s.expire(&1, Duration::from_secs(60)).await.expect("expire"));
    assert_eq!(s.get(&1).await.map(|c| c.value()), Some(3));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(Duration::from_secs(60))));
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
async fn a_clock_stepped_back_does_not_idle_out_a_just_read_entry() {
    let clock = Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    let reader = Arc::clone(&clock);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Local,
        NodeId::from(1),
        10_000,
        None,
        Some(Duration::from_millis(100)),
    )
    .with_clock(Arc::new(move || {
        reader.load(std::sync::atomic::Ordering::SeqCst)
    }));
    shard.insert(1, "v".into()).await.expect("insert");
    assert_eq!(shard.get(&1).await, Some("v".into()));

    clock.store(1_000_000 - 500, std::sync::atomic::Ordering::SeqCst);

    assert_eq!(shard.get(&1).await, Some("v".into()), "get");
    assert!(shard.contains_key(&1).await, "contains_key");
    assert!(shard.contains_key_sync(&1), "contains_key_sync");
    assert_eq!(shard.keys(), vec![1], "keys");
    assert_eq!(
        shard.entry_count().await,
        1,
        "entry_count sweeps and counts"
    );
    ShardOps::run_pending_tasks(&shard).await;
    assert_eq!(
        shard.entry_count().await,
        1,
        "run_pending_tasks leaves the entry live"
    );
    assert_eq!(shard.get(&1).await, Some("v".into()), "still readable");
}

/// The headline guarantee: an expired overwrite that the sweep removed still
/// rejects the older value it overwrote, as it did before the sweep.
#[tokio::test]
async fn a_swept_expired_overwrite_still_rejects_the_value_it_overwrote() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let clock = Arc::new(AtomicU64::new(1_000_000));
    let s = clocked_shard(1, None, &clock);
    let hlc = |wall_ms: u64| Hlc {
        wall_ms,
        logical: 0,
        node: NodeId::from(2),
    };
    ShardOps::apply_remote(&s, wire_record(1, "old", hlc(1_000))).await;
    let mut v2 = wire_record(1, "new", hlc(2_000));
    v2.expires_at_ms = Some(1_000_100);
    ShardOps::apply_remote(&s, v2).await;
    assert_eq!(s.get(&1).await, Some("new".into()));

    clock.store(1_000_200, Ordering::SeqCst);
    assert_eq!(s.get(&1).await, None, "the overwrite expired");

    // Control: before any sweep the expired overwrite still outranks the old value.
    ShardOps::apply_remote(&s, wire_record(1, "old", hlc(1_000))).await;
    assert_eq!(
        s.get(&1).await,
        None,
        "control: the old value stays rejected"
    );

    ShardOps::run_pending_tasks(&s).await;
    assert_eq!(
        s.local_record(&key_bytes(&1u32)).1,
        LocalRecord::Tombstone {
            version: hlc(2_000)
        },
        "the sweep keeps the expired overwrite's version as a tombstone"
    );
    ShardOps::apply_remote(&s, wire_record(1, "old", hlc(1_000))).await;
    assert_eq!(
        s.get(&1).await,
        None,
        "a swept expired overwrite does not bring back the value it overwrote"
    );

    ShardOps::apply_remote(&s, wire_record(1, "newer", hlc(3_000))).await;
    assert_eq!(
        s.get(&1).await,
        Some("newer".into()),
        "a write newer than the expired overwrite still lands"
    );
}
