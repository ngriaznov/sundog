//! Tests for digest and byte accounting: concurrent writes, stripe independence, digest recomputation and `resident_bytes`.

use super::*;

#[tokio::test]
async fn concurrent_inserts_across_stripes_all_land() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let keys: Vec<u32> = (0..64).collect();
    let mut tasks = tokio::task::JoinSet::new();
    for k in keys.clone() {
        let engine = Arc::clone(&engine);
        tasks.spawn(async move {
            let kb = key_bytes(k);
            let _ = put(
                &engine,
                k,
                kb,
                k.to_string(),
                hlc(u64::from(k) + 1, 1),
                None,
                0,
            );
        });
    }
    while let Some(r) = tasks.join_next().await {
        r.expect("spawned call does not panic");
    }
    for k in keys {
        assert_eq!(engine.get(&k, 0), Some(k.to_string()));
    }
}

#[test]
fn two_different_stripes_lock_independently() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let (bucket_a, bucket_b) = (0usize, 1usize);

    let held = Arc::clone(&engine);
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let _guard = held.stripe_lock(bucket_a).write();
        tx.send(()).expect("send");
        std::thread::sleep(Duration::from_millis(150));
    });
    rx.recv().expect("lock holder signals it has the lock");

    assert!(
        engine.stripe_lock(bucket_b).try_write().is_some(),
        "a different stripe's lock is not blocked"
    );
    assert!(
        engine.stripe_lock(bucket_a).try_write().is_none(),
        "the held stripe's lock is still contended"
    );
    handle.join().expect("lock holder thread does not panic");
    assert!(
        engine.stripe_lock(bucket_a).try_write().is_some(),
        "released after the holder finishes"
    );
}

/// [`digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions`]'s
/// shared step: applies `incoming` directly through [`apply_locked`],
/// under a fresh stripe lock, with a 1s/10s tombstone deadline and an
/// [`LwwResolver`].
fn apply_direct<K, V>(
    engine: &Engine<K, V>,
    bucket: usize,
    entry: EntryKey<K>,
    ver: Hlc,
    incoming: Incoming<V>,
    now_ms: u64,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = entry.hash;
    let mut stripe = engine.stripe_lock(bucket).write();
    let resolver = LwwResolver;
    let apply_ctx = test_ctx(engine, bucket, hash, &resolver, 1_000, 10_000, now_ms);
    let _ = apply_locked(&mut stripe, &apply_ctx, entry, ver, incoming);
}

#[test]
fn digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let weigher: Weigher<u32, u64> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, u64>::new(12, None, Some(weigher), None);
    let mut rng = StdRng::seed_from_u64(0x5EED);
    let mut clock = HlcClock::new(NodeId::from(1));

    for i in 0..300u64 {
        let key = rng.random_range(0..24u32);
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let now = i * 10;
        match rng.random_range(0..4u32) {
            0 => {
                let ver = clock.now(now);
                let value = u64::from(key) * 31;
                let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                apply_direct(
                    &engine,
                    bucket,
                    EntryKey {
                        hash,
                        key,
                        key_bytes: kb,
                    },
                    ver,
                    Incoming::Put {
                        value,
                        expires_at_ms: Some(now + 500),
                        encoded,
                    },
                    now,
                );
                engine.enforce_capacity(bucket, &mut Written::default(), now);
            }
            1 => {
                let ver = clock.now(now);
                apply_direct(
                    &engine,
                    bucket,
                    EntryKey {
                        hash,
                        key,
                        key_bytes: kb,
                    },
                    ver,
                    Incoming::Tombstone,
                    now,
                );
            }
            2 => engine.sweep(now),
            _ => engine.gc_tombstones(false, now),
        }
        assert_eq!(
            engine.live_entry_count(),
            engine.recompute_live_entry_count(),
            "iteration {i}: the incrementally maintained live count diverged from a full \
             recount"
        );
        if i % 15 == 0 {
            assert_eq!(
                engine.digests(),
                engine.recompute_digests_paired(),
                "iteration {i}"
            );
            assert_eq!(
                engine.recompute_digests(),
                (0..BUCKET_COUNT)
                    .flat_map(|b| (0..PART_COUNT).map(move |p| (b, p)))
                    .map(|(b, p)| engine.part_digests(u16::try_from(b).expect("fits"))[p])
                    .collect::<Vec<u64>>(),
                "iteration {i}: part digests match the full recompute, not only their \
                 bucket aggregate"
            );
        }
    }
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn resident_bytes_is_none_and_slabs_count_nothing_until_counting_is_turned_on() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(
        &engine,
        1,
        key_bytes(1),
        "v".repeat(100),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(engine.resident_bytes(), None);
    assert!(
        engine
            .stripes
            .iter()
            .all(|stripe| stripe.read().live.resident_bytes.is_none()),
        "an uncapped engine's slabs carry no counter to move"
    );
    let counted = engine_u32_string(u64::MAX, None).count_resident_bytes();
    assert_eq!(counted.resident_bytes(), Some(0));
    assert!(
        counted
            .stripes
            .iter()
            .all(|stripe| stripe.read().live.resident_bytes.is_some())
    );
}

#[test]
fn resident_bytes_counts_each_entry_overhead_plus_its_heap_record() {
    let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
    let (short, long) = (1u32, 2u32);
    let _ = put(
        &engine,
        short,
        key_bytes(short),
        "s".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(ENTRY_OVERHEAD_BYTES),
        "an inline record adds only the entry overhead"
    );
    let long_value = "v".repeat(100);
    let _ = put(
        &engine,
        long,
        key_bytes(long),
        long_value.clone(),
        hlc(2, 1),
        None,
        0,
    );
    // One LEN prefix byte, then the key and value bytes.
    let long_record =
        1 + key_bytes(long).len() + postcard::to_stdvec(&long_value).expect("encode").len();
    assert_eq!(
        engine.resident_bytes(),
        Some(2 * ENTRY_OVERHEAD_BYTES + long_record as u64),
        "a heap record adds its whole length on top of the overhead"
    );
    let _ = put(
        &engine,
        long,
        key_bytes(long),
        "t".to_string(),
        hlc(3, 1),
        None,
        0,
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(2 * ENTRY_OVERHEAD_BYTES),
        "overwriting with an inline value releases the old heap bytes"
    );
    for (key, wall_ms) in [(short, 4), (long, 5)] {
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        apply_direct(
            &engine,
            stripe_index_from_hash(hash),
            EntryKey {
                hash,
                key,
                key_bytes: kb,
            },
            hlc(wall_ms, 1),
            Incoming::Tombstone,
            wall_ms,
        );
    }
    assert_eq!(
        engine.resident_bytes(),
        Some(0),
        "removals release every byte"
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes())
    );
}

#[test]
fn resident_bytes_matches_a_full_recount_after_random_ops_including_sweeps_and_evictions() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let engine = engine_u32_string(12, None).count_resident_bytes();
    let mut rng = StdRng::seed_from_u64(0xB17E5);
    let mut clock = HlcClock::new(NodeId::from(1));

    for i in 0..400u64 {
        let key = rng.random_range(0..24u32);
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let now = i * 10;
        let entry = EntryKey {
            hash,
            key,
            key_bytes: kb,
        };
        match rng.random_range(0..4u32) {
            0 => {
                // Lengths on both sides of INLINE_CAP, so records move
                // between the inline and heap variants.
                let value = "x".repeat(rng.random_range(0..64usize));
                let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                let incoming = Incoming::Put {
                    value,
                    expires_at_ms: Some(now + 500),
                    encoded,
                };
                apply_direct(&engine, bucket, entry, clock.now(now), incoming, now);
                engine.enforce_capacity(bucket, &mut Written::default(), now);
            }
            1 => {
                let ver = clock.now(now);
                apply_direct(&engine, bucket, entry, ver, Incoming::Tombstone, now);
            }
            2 => engine.sweep(now),
            _ => engine.gc_tombstones(false, now),
        }
        assert_eq!(
            engine.resident_bytes(),
            Some(engine.recount_resident_bytes()),
            "iteration {i}: the maintained resident byte total diverged from a full recount"
        );
    }
}

#[test]
fn compact_replace_moves_resident_bytes_by_the_heap_delta() {
    let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
    let key = 7u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let _ = put(&engine, key, kb.clone(), "a".repeat(80), hlc(1, 1), None, 0);
    let before = engine.resident_bytes().expect("counting is on");
    let compacted = "a".repeat(30);
    assert!(engine.compact_replace_if_current(CompactReplace {
        key: &key,
        key_bytes: kb.as_ref(),
        hash,
        expected_ver: hlc(1, 1),
        new_ver: hlc(2, 1),
        value: compacted.clone(),
        encoded: Bytes::from(postcard::to_stdvec(&compacted).expect("encode")),
    }));
    assert_eq!(
        before - engine.resident_bytes().expect("counting is on"),
        50,
        "the record shrinks by exactly the 50 value bytes compaction dropped"
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes())
    );
}
