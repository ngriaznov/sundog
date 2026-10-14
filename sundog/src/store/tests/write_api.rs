//! The write and enumeration API: bulk writes, fan-out backpressure, `keys`, `clear` and the sync variants.

use super::*;
use crate::net::REPLICATE_BATCH_COUNT;

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
async fn remove_many_deletes_over_a_resolver_that_prefers_the_stored_value() {
    // `AlwaysA` keeps whatever is stored for two values, but a tombstone and
    // a value settle by version: every local tombstone is newer than what it
    // meets, so it lands and reaches the fan-out queue.
    let s = shard::<u32, String>(1).with_resolver(Arc::new(AlwaysA));
    s.insert(1, "first".into()).await.expect("insert");
    let _ = s.fan_out.drain();

    s.remove_many([1, 3]).await.expect("remove_many");

    assert_eq!(applied_keys(s.fan_out.drain()), vec![1, 3]);
    assert_eq!(s.get(&1).await, None);
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
