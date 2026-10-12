//! Tests for `Engine::sweep` and `should_shrink_stripe`: expired entries, the long-TTL side table and stripe shrinking.

use super::*;

#[test]
fn sweep_removes_expired_entries_and_corrects_the_digest() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
    let _ = put(&engine, 2, key_bytes(2), "b".into(), hlc(1, 1), None, 0);

    engine.sweep(100);
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

    engine.sweep(deadline + 1);

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
    engine.sweep(500);

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
    engine.sweep(1);
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
