//! Tombstone collection and the incremental bucket and part digests.

use super::*;

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
