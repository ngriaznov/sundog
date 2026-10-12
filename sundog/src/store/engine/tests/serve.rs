//! Tests for the anti-entropy serving methods: `collect_buckets`, `collect_parts`, `bucket_len` and `part_digests`.

use super::*;

#[test]
fn collect_buckets_reports_a_removed_key_with_its_tombstone_version() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 3u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb.clone(), "a".into(), hlc(1, 1), None, 0);
    tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);
    assert_eq!(engine.get(&key, 0), None);

    let bucket_u16 = u16::try_from(bucket).expect("invariant: bucket < BUCKET_COUNT");
    let entries = engine.collect_buckets(&[bucket_u16], 0);
    assert_eq!(entries.len(), 1);
    let (_, records) = &entries[0];
    assert!(
        records
            .iter()
            .any(|kv| kv.key.as_ref() == kb.as_ref() && kv.version == hlc(2, 1)),
        "a removed key still appears in its bucket's entries, carrying the tombstone's \
         version: {records:?}"
    );
}

#[test]
fn collect_buckets_ignores_a_bucket_outside_the_stripe_range() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let bucket =
        u16::try_from(stripe_index_from_hash(hash_key_bytes(kb.as_ref()))).expect("bucket fits");
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), None, 0);

    assert!(
        engine.collect_buckets(&[u16::MAX], 0).is_empty(),
        "a bucket past BUCKET_COUNT yields nothing instead of indexing past the stripes"
    );
    let mixed = engine.collect_buckets(&[u16::MAX, bucket, 1024], 0);
    assert_eq!(mixed.len(), 1, "only the in-range bucket is answered");
    assert_eq!(mixed[0].0, bucket);
    assert_eq!(
        mixed[0].1,
        vec![KeyVersion {
            key: kb,
            version: hlc(1, 1)
        }]
    );
}

/// The recomputed part digests as `(bucket, part, digest)` triples, paired
/// with the bucket each flat index belongs to, for tests that check part
/// digests directly rather than only their bucket aggregate.
fn recompute_part_digests_paired<K, V>(engine: &Engine<K, V>) -> Vec<(u16, u8, u64)>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let parts = engine.recompute_digests();
    (0..BUCKET_COUNT)
        .flat_map(|bucket| {
            let parts = &parts;
            (0..PART_COUNT).map(move |part| {
                (
                    u16::try_from(bucket).expect("fits"),
                    u8::try_from(part).expect("fits"),
                    parts[digest_slot(bucket, part)],
                )
            })
        })
        .collect()
}

#[test]
fn bucket_len_counts_live_entries_and_tombstones_without_materializing_them() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 5u32;
    let kb = key_bytes(key);
    let bucket = u16::try_from(stripe_index_from_hash(hash_key_bytes(kb.as_ref()))).expect("fits");
    assert_eq!(engine.bucket_len(bucket), 0);
    let _ = put(&engine, key, kb, "a".into(), hlc(1, 1), None, 0);
    assert_eq!(engine.bucket_len(bucket), 1);
    assert_eq!(
        engine.bucket_len(u16::MAX),
        0,
        "a bucket past BUCKET_COUNT counts as 0, not an index panic"
    );
}

#[test]
fn part_digests_xor_to_the_bucket_digest() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let engine = engine_u32_string(u64::MAX, None);
    let mut rng = StdRng::seed_from_u64(0xFEED_1234);
    for i in 0..200u32 {
        let ver = hlc(u64::from(i) + 1, 1);
        let _ = put(&engine, i, key_bytes(i), i.to_string(), ver, None, 0);
        let _ = rng.random_range(0..1u32);
    }
    for (bucket, digest) in engine.digests() {
        let parts = engine.part_digests(bucket);
        assert_eq!(parts.len(), PART_COUNT);
        let xored = parts.iter().fold(0u64, |acc, d| acc ^ d);
        assert_eq!(
            xored, digest,
            "bucket {bucket}'s part digests XOR to its bucket digest"
        );
    }
    assert_eq!(
        recompute_part_digests_paired(&engine)
            .into_iter()
            .map(|(bucket, part, digest)| {
                let _ = part;
                (bucket, digest)
            })
            .fold(std::collections::HashMap::new(), |mut acc, (b, d)| {
                *acc.entry(b).or_insert(0u64) ^= d;
                acc
            }),
        engine.digests().into_iter().collect(),
        "the full recompute agrees with the incrementally maintained part digests"
    );
}

#[test]
fn collect_parts_returns_exactly_that_parts_entries_including_tombstones() {
    let engine = engine_u32_string(u64::MAX, None);
    // Find two keys sharing a bucket but landing in different parts, and a
    // third key in a different bucket entirely.
    let mut by_bucket_part: HashMap<(usize, usize), u32> = HashMap::new();
    let mut same_bucket_diff_part: Option<(u32, u32)> = None;
    let mut candidate = 0u32;
    while same_bucket_diff_part.is_none() {
        let kb = key_bytes(candidate);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let part = part_index_from_hash(hash);
        if let Some(&other) = by_bucket_part
            .iter()
            .find(|((b, p), _)| *b == bucket && *p != part)
            .map(|(_, k)| k)
        {
            same_bucket_diff_part = Some((other, candidate));
        }
        by_bucket_part.insert((bucket, part), candidate);
        candidate += 1;
    }
    let (key_a, key_b) = same_bucket_diff_part.expect("found within the loop above");
    let kb_a = key_bytes(key_a);
    let kb_b = key_bytes(key_b);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb_a.as_ref()));
    let part_a = part_index_from_hash(hash_key_bytes(kb_a.as_ref()));
    let part_b = part_index_from_hash(hash_key_bytes(kb_b.as_ref()));

    let _ = put(&engine, key_a, kb_a.clone(), "a".into(), hlc(1, 1), None, 0);
    // key_b becomes a tombstone, still expected in its part's listing.
    let _ = put(&engine, key_b, kb_b.clone(), "b".into(), hlc(1, 1), None, 0);
    tombstone(&engine, key_b, kb_b.clone(), hlc(2, 1), 0);

    let bucket_u16 = u16::try_from(bucket).expect("fits");
    let req_a = BucketPart {
        bucket: bucket_u16,
        part: u8::try_from(part_a).expect("fits"),
    };
    let req_b = BucketPart {
        bucket: bucket_u16,
        part: u8::try_from(part_b).expect("fits"),
    };
    let result = engine.collect_parts(&[req_a, req_b], 0);
    assert_eq!(result.len(), 2);
    let a_entries = &result
        .iter()
        .find(|(key, _)| *key == req_a)
        .expect("part_a present")
        .1;
    assert_eq!(
        a_entries,
        &vec![KeyVersion {
            key: kb_a,
            version: hlc(1, 1)
        }]
    );
    let b_entries = &result
        .iter()
        .find(|(key, _)| *key == req_b)
        .expect("part_b present")
        .1;
    assert_eq!(
        b_entries,
        &vec![KeyVersion {
            key: kb_b,
            version: hlc(2, 1)
        }],
        "a tombstoned key still appears in its part's listing, at its tombstone version"
    );
}

#[test]
fn collect_parts_ignores_out_of_range_bucket_or_part() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = u16::try_from(stripe_index_from_hash(hash)).expect("fits");
    let part = u8::try_from(part_index_from_hash(hash)).expect("fits");
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), None, 0);

    assert!(
        engine
            .collect_parts(
                &[BucketPart {
                    bucket: u16::MAX,
                    part
                }],
                0
            )
            .is_empty(),
        "a bucket past BUCKET_COUNT yields nothing"
    );
    assert!(
        engine
            .collect_parts(
                &[BucketPart {
                    bucket,
                    part: u8::MAX
                }],
                0
            )
            .is_empty(),
        "a part past PART_COUNT yields nothing"
    );
    let mixed = engine.collect_parts(
        &[
            BucketPart {
                bucket: u16::MAX,
                part,
            },
            BucketPart { bucket, part },
            BucketPart {
                bucket,
                part: u8::MAX,
            },
        ],
        0,
    );
    assert_eq!(
        mixed.len(),
        1,
        "only the in-range (bucket, part) is answered"
    );
    assert_eq!(mixed[0].0, BucketPart { bucket, part });
    assert_eq!(
        mixed[0].1,
        vec![KeyVersion {
            key: kb,
            version: hlc(1, 1)
        }]
    );
}
