//! Tests for expiry: lapse causes, TTL and TTI deadlines, the long-TTL side table and the expiry encoding.

use super::*;

#[test]
fn lapse_cause_prefers_expiry_and_defaults_to_idle() {
    assert_eq!(lapse_cause(Some(100), 100), Lapse::Expired);
    assert_eq!(lapse_cause(Some(100), 101), Lapse::Expired);
    assert_eq!(
        lapse_cause(Some(101), 100),
        Lapse::Idle,
        "an entry absent before its expiry went idle"
    );
    assert_eq!(lapse_cause(None, 100), Lapse::Idle);
}

#[test]
fn read_of_an_expired_entry_is_absent_before_any_sweep() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
    assert_eq!(engine.get(&1, 0), Some("a".to_string()));
    // Past the deadline, but no sweep has run yet.
    assert_eq!(
        engine.get(&1, 100),
        None,
        "an expired entry reads as absent immediately"
    );
}

#[test]
fn contains_key_and_record_for_read_an_expired_entry_as_absent_before_any_sweep() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(1);
    let _ = put(&engine, 1, kb.clone(), "a".into(), hlc(1, 1), Some(50), 0);
    assert!(engine.contains_key(&1, 0));
    assert!(engine.record_for(kb.as_ref(), 0).is_some());

    // Past the deadline, but no sweep has run yet.
    assert!(
        !engine.contains_key(&1, 100),
        "an expired entry reads as absent from contains_key immediately"
    );
    assert!(
        engine.record_for(kb.as_ref(), 100).is_none(),
        "record_for treats an expired live entry as absent, no sweep needed"
    );
}

#[test]
fn for_each_key_skips_an_expired_entry_and_visits_every_live_key_exactly_once() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), Some(50), 0);
    let _ = put(&engine, 2, key_bytes(2), "b".into(), hlc(1, 1), None, 0);
    let _ = put(&engine, 3, key_bytes(3), "c".into(), hlc(1, 1), None, 0);

    let mut visited = Vec::new();
    engine.for_each_key(100, |k| visited.push(k));
    visited.sort_unstable();
    assert_eq!(
        visited,
        vec![2, 3],
        "the expired key is skipped, every live key is visited exactly once"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per named overwrite site (apply_put, complete_fresh_load, \
              compact_replace_if_current), matching the spec's single test name; splitting \
              it would only scatter the shared long_deadline setup"
)]
fn overwriting_a_long_ttl_key_clears_its_stale_side_table_row() {
    let long_deadline = MAX_INLINE_TTL_MS + 5_000;

    // `apply_put`'s overwrite path.
    {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
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
                .contains_key(kb.as_ref())
        );
        let _ = put(&engine, 1, kb.clone(), "b".into(), hlc(2, 1), None, 0);
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "apply_put clears the displaced entry's stale side-table row"
        );
    }

    // `complete_fresh_load`'s overwrite path.
    {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 2u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        let _ = put(
            &engine,
            key,
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
                .contains_key(kb.as_ref())
        );
        let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
        let _ = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: hlc(2, 1),
                value: "loaded".to_string(),
                encoded,
                expires_at_ms: None,
            },
            0,
            &inflight,
        );
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "complete_fresh_load clears the displaced entry's stale side-table row"
        );
    }

    // compact_replace_if_current's path: a wall_ms jump alone can make
    // a stored LONG_TTL row stale by making the deadline inline again.
    {
        let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
        let key = 3u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(
            &engine,
            key,
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
                .contains_key(kb.as_ref())
        );
        let new_ver = hlc(long_deadline - 10, 1);
        let replaced = engine.compact_replace_if_current(CompactReplace {
            key: &key,
            key_bytes: kb.as_ref(),
            hash,
            expected_ver: hlc(1, 1),
            new_ver,
            value: "compacted".to_string(),
            encoded: Bytes::from(postcard::to_stdvec("compacted").expect("encode")),
        });
        assert!(replaced);
        assert_eq!(
            engine.resident_bytes(),
            Some(engine.recount_resident_bytes()),
            "an in-place compaction moves the resident byte total by its heap delta"
        );
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "compact_replace_if_current clears a row a wall_ms jump made stale"
        );
        assert_eq!(
            engine.get(&key, long_deadline - 1),
            Some("compacted".to_string()),
            "still readable just before its unchanged absolute deadline"
        );
        assert_eq!(
            engine.get(&key, long_deadline + 1),
            None,
            "expired exactly at its unchanged absolute deadline"
        );
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario per named overwrite site (apply_put, complete_fresh_load), \
              matching the spec's single test name; splitting it would only scatter the \
              shared long_deadline setup"
)]
fn overwriting_a_short_ttl_key_with_a_long_ttl_value_installs_its_side_table_row() {
    let long_deadline = MAX_INLINE_TTL_MS + 5_000;

    // `apply_put`'s overwrite path: a short-TTL live entry, then a fresh
    // write past `MAX_INLINE_TTL_MS` lands on the same key.
    {
        let engine = engine_u32_string(u64::MAX, None);
        let kb = key_bytes(1);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let _ = put(
            &engine,
            1,
            kb.clone(),
            "a".into(),
            hlc(1, 1),
            Some(5_000),
            0,
        );
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "the displaced entry's TTL fit inline: no side-table row yet"
        );
        let _ = put(
            &engine,
            1,
            kb.clone(),
            "b".into(),
            hlc(2, 1),
            Some(long_deadline),
            0,
        );
        assert_eq!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .get(kb.as_ref())
                .copied(),
            Some(long_deadline),
            "apply_put installs a fresh side-table row when the new write is the long one"
        );
        assert_eq!(
            engine.get(&1, long_deadline - 1),
            Some("b".to_string()),
            "still readable just before its long deadline"
        );
        assert_eq!(
            engine.get(&1, long_deadline + 1),
            None,
            "expired exactly at its long deadline"
        );
    }

    // `complete_fresh_load`'s overwrite path: a no-TTL live entry, then a
    // fresh load installs a long TTL.
    {
        let engine = engine_u32_string(u64::MAX, None);
        let key = 2u32;
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 0) else {
            panic!("first caller becomes the owner");
        };
        let _ = put(&engine, key, kb.clone(), "a".into(), hlc(1, 1), None, 0);
        assert!(
            !engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .contains_key(kb.as_ref()),
            "the displaced entry never expires: no side-table row yet"
        );
        let encoded = Bytes::from(postcard::to_stdvec("loaded").expect("encode"));
        let _ = engine.complete_fresh_load(
            FreshLoad {
                key: &key,
                key_bytes: &kb,
                hash,
                ver: hlc(2, 1),
                value: "loaded".to_string(),
                encoded,
                expires_at_ms: Some(long_deadline),
            },
            0,
            &inflight,
        );
        assert_eq!(
            engine
                .stripe_lock(bucket)
                .read()
                .long_ttl
                .get(kb.as_ref())
                .copied(),
            Some(long_deadline),
            "complete_fresh_load installs a fresh side-table row when the new write is the long one"
        );
        assert_eq!(
            engine.get(&key, long_deadline - 1),
            Some("loaded".to_string()),
            "still readable just before its long deadline"
        );
        assert_eq!(
            engine.get(&key, long_deadline + 1),
            None,
            "expired exactly at its long deadline"
        );
    }
}

#[test]
fn tti_idle_eviction() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    assert_eq!(
        engine.get(&1, 50),
        Some("a".to_string()),
        "read at 50ms refreshes idle clock"
    );
    assert_eq!(
        engine.get(&1, 140),
        Some("a".to_string()),
        "90ms since the last read, still alive"
    );
    assert_eq!(engine.get(&1, 400), None, "idle past the 100ms TTI");
}

#[test]
fn encode_expiry_round_trips_never_a_short_delta_and_the_max_inline_ttl_boundary() {
    let wall_ms = 1_000u64;
    let empty: HashMap<Bytes, u64> = HashMap::new();

    let (expiry, side_table) = encode_expiry(wall_ms, None);
    assert_eq!(expiry, NEVER_EXPIRES);
    assert_eq!(side_table, None);
    assert_eq!(decode_expiry(wall_ms, expiry, b"k", &empty), None);

    for delta in [0u64, 1, 50, MAX_INLINE_TTL_MS] {
        let exp = wall_ms + delta;
        let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
        assert_eq!(side_table, None, "delta = {delta} fits inline");
        assert_ne!(expiry, NEVER_EXPIRES, "delta = {delta}");
        assert_ne!(expiry, LONG_TTL, "delta = {delta}");
        assert_eq!(
            decode_expiry(wall_ms, expiry, b"k", &empty),
            Some(exp),
            "delta = {delta}"
        );
    }
}

#[test]
fn encode_expiry_falls_back_to_the_long_ttl_marker_past_max_inline_ttl_ms() {
    let wall_ms = 1_000u64;
    for exp in [
        wall_ms + MAX_INLINE_TTL_MS + 1,
        wall_ms + MAX_INLINE_TTL_MS + 1_000,
        u64::MAX,
    ] {
        let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
        assert_eq!(expiry, LONG_TTL, "exp = {exp}");
        assert_eq!(side_table, Some(exp), "exp = {exp}");
    }
}

#[test]
fn encode_expiry_falls_back_to_the_long_ttl_marker_for_a_dead_on_arrival_expiry_before_wall_ms() {
    // expires_at_ms < wall_ms happens (a dead-on-arrival TTL); expiry
    // must not silently round up to wall_ms.
    let wall_ms = 20_000u64;
    for exp in [0u64, wall_ms - 1, wall_ms - 10_135] {
        let (expiry, side_table) = encode_expiry(wall_ms, Some(exp));
        assert_eq!(expiry, LONG_TTL, "exp = {exp}");
        assert_eq!(side_table, Some(exp), "exp = {exp}");
        let mut long_ttl: HashMap<Bytes, u64> = HashMap::new();
        long_ttl.insert(Bytes::from_static(b"k"), exp);
        assert_eq!(
            decode_expiry(wall_ms, expiry, b"k", &long_ttl),
            Some(exp),
            "exp = {exp}"
        );
    }
}

#[test]
fn decode_expiry_reads_the_long_ttl_side_table_for_the_long_ttl_marker() {
    let wall_ms = 1_000u64;
    let mut long_ttl: HashMap<Bytes, u64> = HashMap::new();
    long_ttl.insert(key_bytes(1), 9_999_999_999u64);
    assert_eq!(
        decode_expiry(wall_ms, LONG_TTL, key_bytes(1).as_ref(), &long_ttl),
        Some(9_999_999_999)
    );
    assert_eq!(
        decode_expiry(wall_ms, LONG_TTL, key_bytes(2).as_ref(), &long_ttl),
        None,
        "no row for this key: never invents a deadline that isn't there"
    );
}

#[test]
fn idle_elapsed_ms_is_correct_across_a_touch_stamp_rollover() {
    // No rollover: an ordinary elapsed duration, well under u32::MAX.
    assert_eq!(idle_elapsed_ms(1_000, touch_stamp(400)), 600);
    assert_eq!(idle_elapsed_ms(1_000, touch_stamp(1_000)), 0);

    // now_ms crossed one u32::MAX rollover after the touch: both
    // truncate to their low 32 bits, so wrapping subtraction still works.
    let touched_at = u64::from(u32::MAX) - 50;
    let now = touched_at + 200;
    assert!(
        now > u64::from(u32::MAX),
        "now must actually cross the rollover for this case to mean anything"
    );
    assert_eq!(idle_elapsed_ms(now, touch_stamp(touched_at)), 200);

    // The touch lands just past a rollover, exercised on the truncated
    // stamp directly.
    let last_access_ms: u32 = 5; // wrapped just past 0
    let now_ms = (1u64 << 32) + 105; // one rollover past the touch, true elapsed: 100ms
    assert_eq!(idle_elapsed_ms(now_ms, last_access_ms), 100);
}

#[test]
fn clamp_tti_ms_caps_a_tti_past_max_tti_ms_to_it() {
    assert_eq!(
        clamp_tti_ms(1_000),
        1_000,
        "well under the ceiling: untouched"
    );
    assert_eq!(
        clamp_tti_ms(MAX_TTI_MS),
        MAX_TTI_MS,
        "exactly at the ceiling: untouched"
    );
    assert_eq!(
        clamp_tti_ms(MAX_TTI_MS + 1),
        MAX_TTI_MS,
        "one past the ceiling: capped"
    );
    assert_eq!(
        clamp_tti_ms(u64::MAX),
        MAX_TTI_MS,
        "far past the ceiling: capped"
    );
}

#[test]
fn engine_new_clamps_a_configured_tti_past_max_tti_ms() {
    let past_ceiling = Duration::from_millis(MAX_TTI_MS + 10_000);
    let engine = engine_u32_string(u64::MAX, Some(past_ceiling));
    assert_eq!(engine.tti_ms, Some(MAX_TTI_MS));
}

#[test]
fn tti_past_max_tti_ms_still_expires_within_the_clamped_bound() {
    // With no clamp, a tti this far past MAX_TTI_MS would leave
    // the entry readable forever. Two separate engines, since a read
    // would reset the idle clock the second assertion checks.
    let past_ceiling = Duration::from_millis(MAX_TTI_MS + 10_000);

    let still_within = engine_u32_string(u64::MAX, Some(past_ceiling));
    let _ = put(
        &still_within,
        1,
        key_bytes(1),
        "a".into(),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(
        still_within.get(&1, MAX_TTI_MS - 1),
        Some("a".to_string()),
        "still within the clamped tti"
    );

    let past_clamp = engine_u32_string(u64::MAX, Some(past_ceiling));
    let _ = put(&past_clamp, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    assert_eq!(
        past_clamp.get(&1, MAX_TTI_MS),
        None,
        "idle past the clamped tti"
    );
}

#[test]
fn idle_elapsed_ms_is_zero_for_a_stamp_ahead_of_now() {
    assert_eq!(
        idle_elapsed_ms(999, touch_stamp(1_000)),
        0,
        "a stamp newer than the observer's now is zero idle, not ~49 days"
    );
}

#[test]
fn idle_elapsed_ms_reads_the_lead_window_as_zero_and_the_gap_below_it_exactly() {
    let now = 5_000_000_000u64;
    assert_eq!(idle_elapsed_ms(now, touch_stamp(now + 1)), 0, "1 ms ahead");
    assert_eq!(
        idle_elapsed_ms(now, touch_stamp(now + MAX_STAMP_LEAD_MS)),
        0,
        "exactly at the edge of the lead window"
    );
    assert_eq!(
        idle_elapsed_ms(now, touch_stamp(now - MAX_TTI_MS)),
        MAX_TTI_MS,
        "a gap of exactly MAX_TTI_MS is the true gap"
    );
    assert_eq!(
        idle_elapsed_ms(now, touch_stamp(now - MAX_TTI_MS - 1)),
        0,
        "one past MAX_TTI_MS is the first lead value"
    );
    assert_eq!(
        idle_elapsed_ms(now, touch_stamp(now + MAX_STAMP_LEAD_MS + 1)),
        MAX_TTI_MS,
        "a lead past the window reads as the oldest representable gap"
    );
    // A stamp 15 ms ahead whose truncation straddles the u32 rollover.
    let straddling_now = (1u64 << 32) - 10;
    assert_eq!(
        idle_elapsed_ms(straddling_now, touch_stamp((1u64 << 32) + 5)),
        0
    );
    // A stamp near u32::MAX with now just past the rollover is the true small gap.
    assert_eq!(
        idle_elapsed_ms((1u64 << 32) + 10, touch_stamp(u64::from(u32::MAX) - 4)),
        15
    );
}

#[test]
fn clamp_tti_ms_leaves_room_for_the_lead_window() {
    assert_eq!(clamp_tti_ms(u64::MAX), MAX_TTI_MS);
    assert_eq!(MAX_TTI_MS + MAX_STAMP_LEAD_MS, u64::from(u32::MAX));
    const { assert!(MAX_TTI_MS <= MAX_INLINE_TTL_MS) };
}

#[test]
fn a_read_sampled_before_the_latest_touch_still_sees_the_entry() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 1_000);
    assert_eq!(engine.get(&1, 1_001), Some("a".to_string()));
    assert_eq!(
        engine.get(&1, 1_000),
        Some("a".to_string()),
        "a reader whose now was sampled before the latest touch still sees the entry"
    );
}

#[test]
fn a_sweep_sampled_before_a_touch_keeps_the_touched_entry() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 1_000);
    assert_eq!(engine.get(&1, 1_001), Some("a".to_string()));
    engine.sweep(999, None);
    assert_eq!(
        engine.live_entry_count(),
        1,
        "a sweep that sampled now before a touch does not delete the touched entry"
    );
}

#[test]
fn a_touched_entry_is_present_to_an_observer_whose_now_is_behind_the_touch() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 1_000);
    assert_eq!(engine.get(&1, 1_010), Some("a".to_string()));

    assert!(engine.contains_key(&1, 1_005), "contains_key");
    assert_eq!(engine.keys(1_005), vec![1], "keys");
    let mut seen = Vec::new();
    engine.for_each_key(1_005, |k| seen.push(k));
    assert_eq!(seen, vec![1], "for_each_key");
    assert!(
        matches!(
            engine.inspect(key_bytes(1).as_ref(), 1_005),
            LocalRecord::Live { .. }
        ),
        "inspect does not report the entry lapsed"
    );
    assert!(
        engine.record_for(key_bytes(1).as_ref(), 1_005).is_some(),
        "record_for"
    );
}

#[test]
fn a_touched_entry_still_idles_out_once_the_observer_catches_up() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 1_000);
    assert_eq!(engine.get(&1, 1_010), Some("a".to_string()));
    assert_eq!(engine.get(&1, 1_109), Some("a".to_string()), "99 ms idle");
    assert_eq!(
        engine.get(&1, 1_209),
        None,
        "100 ms idle after the 1_109 touch"
    );
}
