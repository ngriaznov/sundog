//! The entry cap and the memory ceiling: what a write past either bound keeps, refuses or drops.

use super::*;

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
