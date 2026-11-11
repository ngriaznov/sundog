//! Tests for `Engine::sweep` and `should_shrink_stripe`: expired entries, the long-TTL side table and stripe shrinking.

use super::*;

#[test]
fn sweep_removes_expired_entries_and_corrects_the_digest() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
    let _ = put(&engine, 2, key_bytes(2), "b".into(), hlc(1, 1), None, 0);

    engine.sweep(100, None);
    assert_eq!(engine.get(&1, 100), None);
    assert_eq!(engine.get(&2, 100), Some("b".to_string()));
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let (entries, _) = engine.debug_totals();
    assert_eq!(entries, 1, "only the non-expired entry survives the sweep");
}

#[test]
fn removing_a_long_ttl_entry_also_clears_its_side_table_row() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let long_deadline = MAX_INLINE_TTL_MS + 5_000;
    let _ = put(
        &engine,
        1,
        kb.clone(),
        "a".into(),
        hlc(1, 1),
        Some(long_deadline),
        0,
    );
    assert!(
        engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "a long TTL write leaves its absolute deadline in the side table"
    );

    engine.invalidate_local(kb.as_ref(), hash);

    assert!(
        !engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "removing the entry clears its side-table row"
    );
}

#[test]
fn sweep_of_an_expired_long_ttl_entry_clears_its_side_table_row() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let deadline = MAX_INLINE_TTL_MS + 5_000;
    let _ = put(
        &engine,
        1,
        kb.clone(),
        "a".into(),
        hlc(1, 1),
        Some(deadline),
        0,
    );
    assert!(
        engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref())
    );

    engine.sweep(deadline + 1, None);

    assert_eq!(engine.get(&1, deadline + 1), None, "swept: reads as absent");
    assert!(
        !engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "sweep clears the side-table row of what it expires"
    );
}

#[test]
fn sweep_of_a_tti_idle_long_ttl_entry_clears_its_side_table_row() {
    // A TTI far shorter than the long-TTL deadline, so this entry
    // goes idle and is swept through absent_at's TTI branch first.
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let deadline = MAX_INLINE_TTL_MS + 5_000;
    let _ = put(
        &engine,
        1,
        kb.clone(),
        "a".into(),
        hlc(1, 1),
        Some(deadline),
        0,
    );
    assert!(
        engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "a long TTL write leaves its absolute deadline in the side table"
    );
    let (entries_before, _) = engine.debug_totals();
    assert_eq!(entries_before, 1);

    // Idle past the 100ms TTI, nowhere near `deadline`.
    engine.sweep(500, None);

    let (entries_after, _) = engine.debug_totals();
    assert_eq!(entries_after, 0, "sweep evicts the tti-idle entry");
    assert_eq!(engine.get(&1, 500), None, "swept: reads as absent");
    assert!(
        !engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "sweep clears the side-table row of what it evicts for idleness, \
         not only what it expires"
    );
}

#[test]
fn next_expiry_ms_skip_logic_leaves_stripes_with_nothing_due_untouched() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb, "a".into(), hlc(1, 1), Some(10_000), 0);
    assert_eq!(engine.stripe_lock(bucket).read().next_expiry_ms, 10_000);

    // Sweeping well before the deadline does not touch this stripe.
    engine.sweep(1, None);
    assert_eq!(engine.get(&key, 1), Some("a".to_string()));
    assert_eq!(engine.stripe_lock(bucket).read().next_expiry_ms, 10_000);
}

#[test]
fn should_shrink_stripe_true_well_below_its_allocation() {
    assert!(
        should_shrink_stripe(1, 64),
        "1/64 is well below the eighth threshold"
    );
    assert!(
        should_shrink_stripe(0, 100),
        "a fully drained stripe is always well below its own allocation"
    );
    assert!(
        should_shrink_stripe(8, 64),
        "8/64 sits exactly at the eighth threshold, still shrunk"
    );
}

#[test]
fn should_shrink_stripe_false_near_or_above_its_allocation() {
    assert!(
        !should_shrink_stripe(64, 64),
        "at capacity: nothing to reclaim"
    );
    assert!(
        !should_shrink_stripe(40, 64),
        "40/64 is well past the eighth threshold"
    );
    assert!(
        !should_shrink_stripe(9, 64),
        "9/64 sits one entry past the eighth threshold"
    );
    assert!(
        !should_shrink_stripe(0, 0),
        "never allocated: nothing to shrink further"
    );
}

#[test]
fn stripe_prealloc_divides_the_hint_across_stripes_and_caps_it() {
    assert_eq!(stripe_prealloc(0), 0);
    assert_eq!(stripe_prealloc(1), 1);
    assert_eq!(stripe_prealloc(BUCKET_COUNT as u64 * 10), 10);
    assert_eq!(stripe_prealloc(u64::MAX), MAX_STRIPE_PREALLOC);
    assert_eq!(stripe_prealloc(1 << 60), MAX_STRIPE_PREALLOC);
}

/// The stripe holding the key whose bucket is `bucket`'s, as its capacity.
fn capacity_of(engine: &Engine<u32, String>, bucket: usize) -> usize {
    engine.stripe_capacities()[bucket]
}

/// An engine presized for `per_stripe` entries in every stripe, holding the
/// first key of [`same_bucket_pair`] in its bucket.
fn presized_engine_with_one_entry(per_stripe: u64) -> (Engine<u32, String>, u32, usize) {
    let engine =
        Engine::<u32, String>::new(u64::MAX, None, None, Some(per_stripe * BUCKET_COUNT as u64));
    let (key, _, bucket) = same_bucket_pair(100_000);
    let _ = put(&engine, key, key_bytes(key), "v".into(), hlc(1, 1), None, 0);
    (engine, key, bucket)
}

#[test]
fn shrink_batch_walks_the_budget_from_the_cursor_and_wraps() {
    assert_eq!(shrink_batch(0, 0), (0, 0));
    assert_eq!(shrink_batch(5, 0), (0, 5));
    assert_eq!(shrink_batch(0, 10), (10, 10));
    assert_eq!(shrink_batch(1020, 10), (10, 6));
    assert_eq!(shrink_batch(0, BUCKET_COUNT), (BUCKET_COUNT, 0));
    assert_eq!(shrink_batch(7, usize::MAX), (BUCKET_COUNT, 7));
    assert_eq!(shrink_batch(BUCKET_COUNT + 3, 1), (1, 4));
}

#[test]
fn shrink_pass_reclaims_an_oversized_presize_without_touching_a_full_stripe() {
    let (engine, _, bucket) = presized_engine_with_one_entry(64);
    assert_eq!(capacity_of(&engine, bucket), 64);

    let shrunk = engine.shrink_pass(BUCKET_COUNT);

    assert_eq!(
        shrunk, BUCKET_COUNT,
        "every stripe is at most an eighth full"
    );
    assert!(capacity_of(&engine, bucket) < 64);
    assert_eq!(engine.debug_totals().0, 1, "the entry stays");
    assert_eq!(
        engine.shrink_pass(BUCKET_COUNT),
        0,
        "nothing is left to reclaim"
    );
}

#[test]
fn shrink_pass_visits_only_its_budget_and_resumes_where_it_stopped() {
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, Some(64 * BUCKET_COUNT as u64));

    assert_eq!(engine.shrink_pass(10), 10);
    assert_eq!(engine.shrink_pass(BUCKET_COUNT), BUCKET_COUNT - 10);
    let caps = engine.stripe_capacities();
    assert!(caps.iter().all(|&c| c < 64), "every stripe was reached");
}

#[test]
fn sweep_shrinks_a_stripe_it_emptied() {
    let (engine, key, bucket) = presized_engine_with_one_entry(64);
    let _ = put(
        &engine,
        key,
        key_bytes(key),
        "v".into(),
        hlc(2, 1),
        Some(50),
        0,
    );

    engine.sweep(100, None);

    assert_eq!(engine.get(&key, 100), None);
    assert!(capacity_of(&engine, bucket) < 64, "the swept stripe shrank");
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn release_buckets_and_release_parts_shrink_the_stripes_they_empty() {
    let (engine, key, bucket) = presized_engine_with_one_entry(64);
    engine.release_buckets(&[u16::try_from(bucket).expect("bucket fits")]);
    assert!(capacity_of(&engine, bucket) < 64, "release_buckets");

    let (engine, key2, bucket2) = presized_engine_with_one_entry(64);
    assert_eq!(key, key2);
    let part = PartId::new(
        u16::try_from(bucket2).expect("bucket fits"),
        u8::try_from(part_index_from_hash(hash_key_bytes(
            key_bytes(key2).as_ref(),
        )))
        .expect("part fits"),
    );
    assert_eq!(engine.release_parts(&[part]), 1);
    assert!(capacity_of(&engine, bucket2) < 64, "release_parts");
}

#[test]
fn shrink_if_sparse_reclaims_the_tombstone_and_long_ttl_maps() {
    let mut stripe = Stripe::<u32, String>::new();
    for i in 0..64u32 {
        stripe.tombstones.insert(
            key_bytes(i),
            Tombstone {
                ver: hlc(1, 1),
                ttl_deadline_ms: 0,
                max_deadline_ms: 0,
                expired_at_ms: None,
            },
        );
        stripe.long_ttl.insert(key_bytes(i), 1);
    }
    stripe.tombstones.retain(|_, _| false);
    stripe.long_ttl.clear();
    assert!(stripe.tombstones.capacity() >= 64 && stripe.long_ttl.capacity() >= 64);

    stripe.shrink_if_sparse();

    assert_eq!(stripe.tombstones.capacity(), 0);
    assert_eq!(stripe.long_ttl.capacity(), 0);
}
