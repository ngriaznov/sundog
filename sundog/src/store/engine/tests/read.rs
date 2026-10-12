//! Tests for the point reads: `inspect`, `ttl_of`, `get_by_bytes` and `Arc`-valued entries.

use super::*;

/// One engine holding each kind of record [`Engine::inspect`] tells
/// apart at `now`: key 1 live with no TTL, 2 live with a TTL, 3 live
/// past the inline deadline range, 4 expired and not swept, 5 a
/// tombstone; 6 is never written.
fn engine_with_every_record(now: u64) -> (Engine<u32, String>, u64) {
    let engine = engine_u32_string(u64::MAX, None);
    let far = 1_000 + u64::from(u32::MAX) * 2;
    for (key, expires_at_ms) in [
        (1, None),
        (2, Some(7_000)),
        (3, Some(far)),
        (4, Some(1_500)),
    ] {
        put(
            &engine,
            key,
            key_bytes(key),
            format!("v{key}"),
            hlc(1_000, 1),
            expires_at_ms,
            now,
        );
    }
    tombstone(&engine, 5u32, key_bytes(5), hlc(1_200, 2), now);
    (engine, far)
}

#[test]
fn inspect_reports_absent_tombstone_live_and_lapsed() {
    let now = 2_000;
    let (engine, far) = engine_with_every_record(now);
    let inspect = |key: u32, at: u64| engine.inspect(key_bytes(key).as_ref(), at);
    let live = |expires_at_ms| LocalRecord::Live {
        version: hlc(1_000, 1),
        expires_at_ms,
        spilled: false,
    };
    assert_eq!(inspect(1, now), live(None));
    assert_eq!(inspect(2, now), live(Some(7_000)));
    assert_eq!(
        inspect(3, now),
        live(Some(far)),
        "a deadline past the inline range is read from the long-TTL table"
    );
    assert_eq!(
        inspect(4, now),
        LocalRecord::Lapsed {
            version: hlc(1_000, 1),
            expires_at_ms: Some(1_500),
            cause: Lapse::Expired,
        }
    );
    assert_eq!(
        inspect(5, now),
        LocalRecord::Tombstone {
            version: hlc(1_200, 2)
        }
    );
    assert_eq!(inspect(6, now), LocalRecord::Absent);
    assert_eq!(
        inspect(2, 7_000),
        LocalRecord::Lapsed {
            version: hlc(1_000, 1),
            expires_at_ms: Some(7_000),
            cause: Lapse::Expired,
        },
        "an expiry equal to now has passed"
    );

    engine.sweep(now);
    assert_eq!(
        inspect(4, now),
        LocalRecord::Absent,
        "a swept expired entry is gone"
    );
    assert_eq!(inspect(1, now), live(None), "the sweep keeps a live entry");
}

#[test]
fn inspect_agrees_with_record_for_and_lifetime_of() {
    let now = 2_000;
    let (engine, _) = engine_with_every_record(now);
    for key in 1..=6 {
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let inspected = engine.inspect(kb.as_ref(), now);
        let record = engine.record_for(kb.as_ref(), now);
        assert_eq!(inspected.is_held(), record.is_some(), "key {key}");
        let lifetime = engine.lifetime_of(kb.as_ref(), hash, now);
        assert_eq!(inspected.is_live(), lifetime.is_some(), "key {key}");
        match inspected {
            LocalRecord::Live {
                version,
                expires_at_ms,
                ..
            } => {
                assert_eq!(lifetime, Some((version, expires_at_ms)), "key {key}");
                let record = record.expect("a live entry has a record");
                assert_eq!(
                    (record.ver, record.expires_at_ms),
                    (version, expires_at_ms),
                    "key {key}"
                );
            }
            LocalRecord::Tombstone { version } => {
                assert_eq!(record.map(|r| r.ver), Some(version), "key {key}");
            }
            LocalRecord::Absent | LocalRecord::Lapsed { .. } => {}
        }
    }
}

#[test]
fn inspect_reports_an_idle_entry_lapsed_and_never_resets_the_idle_clock() {
    let engine = engine_u32_string(10, Some(Duration::from_millis(100)));
    put(
        &engine,
        1u32,
        key_bytes(1),
        "idle".to_string(),
        hlc(0, 1),
        None,
        0,
    );
    put(
        &engine,
        2u32,
        key_bytes(2),
        "both".to_string(),
        hlc(0, 1),
        Some(120),
        0,
    );
    let inspect = |key: u32, at: u64| engine.inspect(key_bytes(key).as_ref(), at);
    assert!(inspect(1, 90).is_live());
    assert_eq!(last_access(&engine, 1), 0, "an inspect is no access");
    assert_eq!(
        engine.get(&1, 140),
        None,
        "idle since the write at 0, not the inspect at 90"
    );
    assert_eq!(
        inspect(1, 140),
        LocalRecord::Lapsed {
            version: hlc(0, 1),
            expires_at_ms: None,
            cause: Lapse::Idle,
        }
    );
    assert_eq!(
        inspect(2, 140),
        LocalRecord::Lapsed {
            version: hlc(0, 1),
            expires_at_ms: Some(120),
            cause: Lapse::Expired,
        },
        "an entry both idle and expired reads expired"
    );
    assert_eq!(last_access(&engine, 2), 0);
}

#[test]
fn ttl_of_reads_each_kind_of_deadline_and_nothing_for_an_absent_key() {
    let engine = engine_u32_string(u64::MAX, None);
    let now = 2_000;
    let deadline = |key: u32| {
        let kb = key_bytes(key);
        engine.ttl_of(kb.as_ref(), hash_key_bytes(kb.as_ref()), now)
    };
    put(
        &engine,
        1u32,
        key_bytes(1),
        "never".to_string(),
        hlc(1_000, 1),
        None,
        now,
    );
    put(
        &engine,
        2u32,
        key_bytes(2),
        "soon".to_string(),
        hlc(1_000, 1),
        Some(7_000),
        now,
    );
    let far = 1_000 + u64::from(u32::MAX) * 2;
    put(
        &engine,
        3u32,
        key_bytes(3),
        "far".to_string(),
        hlc(1_000, 1),
        Some(far),
        now,
    );
    put(
        &engine,
        4u32,
        key_bytes(4),
        "gone".to_string(),
        hlc(1_000, 1),
        Some(1_500),
        now,
    );

    assert_eq!(
        deadline(1),
        Some(Ttl::Never),
        "an entry with no TTL never expires"
    );
    assert_eq!(deadline(2), Some(remaining(5_000)));
    assert_eq!(
        deadline(3),
        Some(remaining(far - now)),
        "a deadline past the inline range"
    );
    assert_eq!(
        deadline(4),
        None,
        "an expired entry has no deadline to read"
    );
    assert_eq!(deadline(5), None, "nor does a key never written");
}

#[test]
fn get_by_bytes_reads_what_get_reads() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());
    assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), None);
    let _ = put(
        &engine,
        7,
        kb.clone(),
        "seven".into(),
        hlc(1, 1),
        Some(50),
        0,
    );
    assert_eq!(
        engine.get_by_bytes(kb.as_ref(), hash, 0),
        Some("seven".to_string())
    );
    assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), engine.get(&7, 0));
    assert_eq!(
        engine.get_by_bytes(kb.as_ref(), hash, 100),
        None,
        "an expired entry reads as absent by bytes too"
    );
}

#[test]
fn engine_reads_back_an_arc_string_value_serialized_via_serdes_rc_feature() {
    let engine = Engine::<u32, Arc<String>>::new(u64::MAX, None, None, None);
    let value = Arc::new("shared".to_string());
    let _ = put(
        &engine,
        1,
        key_bytes(1),
        Arc::clone(&value),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(
        engine.get(&1, 0),
        Some(value),
        "an Arc<String> value round-trips through postcard's serde `rc` support"
    );
}
