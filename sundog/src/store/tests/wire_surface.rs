//! The wire-facing surface of a shard: key encoding, snapshot chunks and remote batches.

use super::*;

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
