//! The `Shard` builder methods and their defaults.

use super::*;

#[tokio::test]
async fn value_too_large_is_rejected() {
    let s = shard::<u32, Vec<u8>>(1);
    let big = vec![0u8; MAX_FRAME + 1];
    let err = s
        .insert(1, big)
        .await
        .expect_err("oversized value rejected");
    assert!(matches!(err, CacheError::ValueTooLarge { .. }));
}

#[tokio::test]
async fn with_max_frame_overrides_the_default_cap() {
    let s = shard::<u32, Vec<u8>>(1).with_max_frame(64);
    let err = s
        .insert(1, vec![0u8; 100])
        .await
        .expect_err("a value over the overridden cap is rejected");
    assert!(matches!(err, CacheError::ValueTooLarge { .. }));

    s.insert(2, Vec::new())
        .await
        .expect("a small value still fits under the overridden cap");
}

#[tokio::test]
async fn insert_rejects_when_the_value_alone_fits_but_the_wire_frame_does_not() {
    // The value's own encoding fits a 25-byte cap; the record's full wire
    // frame does not.
    let s = shard::<u32, Vec<u8>>(1).with_max_frame(25);
    let err = s
        .insert(1, vec![0u8; 20])
        .await
        .expect_err("the full wire frame, not only the value, counts toward the cap");
    assert!(matches!(err, CacheError::ValueTooLarge { .. }));
}

#[tokio::test]
async fn with_weigher_drives_the_engines_total_weight() {
    let s = shard::<u32, Vec<u8>>(1)
        .with_weigher(|_key: &u32, value: &Vec<u8>| u32::try_from(value.len()).unwrap_or(u32::MAX));
    s.insert(1, vec![0u8; 7]).await.expect("insert");
    let (_, weight) = s.engine.debug_totals();
    assert_eq!(
        weight, 7,
        "a custom weigher drives the engine's total weight, not the default of 1 per entry"
    );
}

#[test]
fn merge_coalesce_window_defaults_to_zero() {
    let s = shard::<u32, u32>(1);
    assert_eq!(s.merge_window(), Duration::ZERO);
}

#[test]
fn prefold_enabled_defaults_to_true_and_with_prefold_enabled_overrides_it() {
    let s = shard::<u32, u32>(1);
    assert!(
        s.prefold_enabled(),
        "pre-fold defaults to on, matching the engine's own default"
    );

    let s = s.with_prefold_enabled(false);
    assert!(!s.prefold_enabled(), "the toggle reached the engine");

    let s = s.with_prefold_enabled(true);
    assert!(s.prefold_enabled());
}

#[test]
fn shard_with_capacity_hint_presizes_engine() {
    let s = shard::<u32, u32>(1);
    assert_eq!(
        s.stripe_capacities(),
        vec![0; BUCKET_COUNT],
        "no hint yet: every stripe starts at zero allocation"
    );

    let hint = 3_000u64;
    let expected = usize::try_from(hint.div_ceil(BUCKET_COUNT as u64)).expect("hint fits usize");
    let s = s.with_capacity_hint(hint);
    assert_eq!(
        s.stripe_capacities(),
        vec![expected; BUCKET_COUNT],
        "the hint reached the engine and presized every stripe"
    );
}

#[tokio::test]
async fn with_capacity_hint_after_with_weigher_keeps_the_weigher() {
    let s = shard::<u32, Vec<u8>>(1)
        .with_weigher(|_key: &u32, value: &Vec<u8>| u32::try_from(value.len()).unwrap_or(u32::MAX))
        .with_capacity_hint(10);
    s.insert(1, vec![0u8; 7]).await.expect("insert");
    let (_, weight) = s.engine.debug_totals();
    assert_eq!(
        weight, 7,
        "with_capacity_hint's rebuild must carry an already-installed weigher forward \
         instead of reverting to the default of 1 per entry"
    );
}

#[test]
fn with_merge_coalesce_window_overrides_the_default() {
    let s = shard::<u32, u32>(1).with_merge_coalesce_window(Duration::from_millis(7));
    assert_eq!(s.merge_window(), Duration::from_millis(7));
}
