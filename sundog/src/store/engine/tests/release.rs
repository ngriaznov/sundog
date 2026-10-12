//! Tests for `release_buckets`, `release_parts` and `held_parts`: ownership hand-off removes entries and resets digests.

use super::*;

#[test]
fn release_buckets_removes_every_live_entry_and_tombstone_and_resets_the_digest() {
    let engine = engine_u32_string(u64::MAX, None);
    let (live_key, tomb_key, bucket) = same_bucket_pair(100_000);
    let bucket_u16 = u16::try_from(bucket).expect("invariant: bucket < BUCKET_COUNT");

    let _ = put(
        &engine,
        live_key,
        key_bytes(live_key),
        "live".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    tombstone(&engine, tomb_key, key_bytes(tomb_key), hlc(1, 1), 0);

    let removed = engine.release_buckets(&[bucket_u16]);

    assert_eq!(removed, 2, "one live entry and one tombstone removed");
    assert_eq!(engine.get(&live_key, 0), None, "the live entry is gone");
    assert!(
        engine.collect_buckets(&[bucket_u16], 0)[0].1.is_empty(),
        "the tombstone is gone too: nothing is left in the released bucket"
    );
    assert_eq!(engine.debug_totals(), (0, 0));
    for part in 0..PART_COUNT {
        assert_eq!(
            engine.digest[digest_slot(bucket, part)].load(Ordering::Relaxed),
            0,
            "every part digest in the released bucket is reset to zero"
        );
    }
}

#[test]
fn release_buckets_leaves_a_different_buckets_digest_and_entry_untouched() {
    let engine = engine_u32_string(u64::MAX, None);
    let (live_key, _, released_bucket) = same_bucket_pair(100_000);
    let other_key = (1..100_000u32)
        .find(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) != released_bucket)
        .expect("a distinct bucket is found quickly");
    let other_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(other_key).as_ref()));
    let other_part = part_index_from_hash(hash_key_bytes(key_bytes(other_key).as_ref()));

    let _ = put(
        &engine,
        live_key,
        key_bytes(live_key),
        "live".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let _ = put(
        &engine,
        other_key,
        key_bytes(other_key),
        "untouched".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let digest_before =
        engine.digest[digest_slot(other_bucket, other_part)].load(Ordering::Relaxed);

    let removed =
        engine.release_buckets(&[u16::try_from(released_bucket).expect("invariant: fits")]);

    assert_eq!(removed, 1);
    assert_eq!(
        engine.digest[digest_slot(other_bucket, other_part)].load(Ordering::Relaxed),
        digest_before,
        "an untouched bucket's digest is unaffected"
    );
    assert_eq!(
        engine.get(&other_key, 0),
        Some("untouched".to_string()),
        "a different bucket's entry survives release"
    );
}

/// The first two keys of `0..limit` sharing a bucket but not a part.
fn same_bucket_different_parts(limit: u32) -> (u32, u32) {
    let mut by_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
    for k in 0..limit {
        let hash = hash_key_bytes(key_bytes(k).as_ref());
        let keys = by_bucket.entry(stripe_index_from_hash(hash)).or_default();
        if let Some(&first) = keys.iter().find(|&&other| {
            part_index_from_hash(hash_key_bytes(key_bytes(other).as_ref()))
                != part_index_from_hash(hash)
        }) {
            return (first, k);
        }
        keys.push(k);
    }
    panic!("no same-bucket, different-part pair within the first {limit} keys");
}

#[test]
fn release_parts_drops_only_the_named_part_and_resets_only_its_digest() {
    let engine = engine_u32_string(u64::MAX, None);
    let (released_key, kept_key) = same_bucket_different_parts(100_000);
    let released = PartId::from_hash(hash_key_bytes(key_bytes(released_key).as_ref()));
    let kept = PartId::from_hash(hash_key_bytes(key_bytes(kept_key).as_ref()));
    assert_eq!(released.bucket(), kept.bucket());
    let bucket = usize::from(released.bucket());

    let _ = put(
        &engine,
        released_key,
        key_bytes(released_key),
        "released".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let _ = put(
        &engine,
        kept_key,
        key_bytes(kept_key),
        "kept".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let kept_digest =
        engine.digest[digest_slot(bucket, usize::from(kept.part()))].load(Ordering::Relaxed);
    assert_ne!(kept_digest, 0);

    let removed = engine.release_parts(&[released]);

    assert_eq!(removed, 1, "only the released part's entry goes");
    assert_eq!(engine.get(&released_key, 0), None);
    assert_eq!(engine.get(&kept_key, 0), Some("kept".to_string()));
    assert_eq!(
        engine.digest[digest_slot(bucket, usize::from(released.part()))].load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        engine.digest[digest_slot(bucket, usize::from(kept.part()))].load(Ordering::Relaxed),
        kept_digest,
        "the bucket's other part keeps its digest"
    );
    assert_eq!(engine.held_parts(), vec![kept]);
}

#[test]
fn release_parts_drops_a_released_part_s_tombstone_too() {
    let engine = engine_u32_string(u64::MAX, None);
    let (released_key, kept_key) = same_bucket_different_parts(100_000);
    let released = PartId::from_hash(hash_key_bytes(key_bytes(released_key).as_ref()));
    tombstone(&engine, released_key, key_bytes(released_key), hlc(1, 1), 0);
    tombstone(&engine, kept_key, key_bytes(kept_key), hlc(1, 1), 0);
    assert_eq!(engine.held_parts().len(), 2, "a tombstone holds its part");

    assert_eq!(engine.release_parts(&[released]), 1);
    assert_eq!(
        engine.held_parts(),
        vec![PartId::from_hash(hash_key_bytes(
            key_bytes(kept_key).as_ref()
        ))]
    );
}

#[test]
fn release_parts_of_every_part_of_a_bucket_is_releasing_the_bucket() {
    let engine = engine_u32_string(u64::MAX, None);
    let (live_key, tomb_key, bucket) = same_bucket_pair(100_000);
    let _ = put(
        &engine,
        live_key,
        key_bytes(live_key),
        "live".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    tombstone(&engine, tomb_key, key_bytes(tomb_key), hlc(1, 1), 0);
    let whole: Vec<PartId> = PartId::of_bucket(u16::try_from(bucket).expect("fits")).collect();

    assert_eq!(engine.release_parts(&whole), 2);
    assert_eq!(engine.debug_totals(), (0, 0));
    let held = engine.held_parts();
    assert!(held.is_empty(), "{held:?}");
}

#[test]
fn held_parts_names_each_part_holding_an_entry_once() {
    let engine = engine_u32_string(u64::MAX, None);
    let held = engine.held_parts();
    assert!(held.is_empty(), "{held:?}");
    let keys = [1u32, 2, 3, 1];
    for key in keys {
        let _ = put(&engine, key, key_bytes(key), "v".into(), hlc(1, 1), None, 0);
    }
    let mut expected: Vec<PartId> = keys
        .iter()
        .map(|&key| PartId::from_hash(hash_key_bytes(key_bytes(key).as_ref())))
        .collect();
    expected.sort_unstable();
    expected.dedup();
    assert_eq!(engine.held_parts(), expected);
}

#[test]
fn release_buckets_skips_a_bucket_at_or_past_bucket_count_and_returns_zero() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    let out_of_range = u16::try_from(BUCKET_COUNT).expect("invariant: BUCKET_COUNT fits u16");

    let removed = engine.release_buckets(&[out_of_range]);

    assert_eq!(removed, 0, "a bucket past BUCKET_COUNT removes nothing");
    assert_eq!(
        engine.get(&1, 0),
        Some("a".to_string()),
        "an out-of-range bucket touches nothing"
    );
}
