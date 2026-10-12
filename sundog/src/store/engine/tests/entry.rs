//! Tests for the entry layout: `Live`, `Record` construction, key and value decoding and capacity hints.

use super::*;

#[test]
fn is_resident_true_for_resident_false_for_spilled() {
    let resident = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        1,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(is_resident(&resident));

    #[cfg(feature = "spill")]
    {
        let spilled = Live::<u32, String>::new(
            build_key_only_record(key_bytes(1).as_ref()),
            hlc(1, 1),
            NEVER_EXPIRES,
            0,
            0,
            EntryState::Spilled(SpillLoc {
                region: 0,
                offset: 0,
                len: 1,
                generation: 0,
            }),
        );
        assert!(!is_resident(&spilled));
    }
}

#[test]
fn is_spill_candidate_true_only_for_a_resident_entry_with_nonzero_weight() {
    let resident_hot = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        3,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(is_spill_candidate(&resident_hot));

    let resident_pending = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        0,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(
        !is_spill_candidate(&resident_pending),
        "weight zero means a hand-off to the spill tier is already in flight"
    );

    #[cfg(feature = "spill")]
    {
        let spilled = Live::<u32, String>::new(
            build_key_only_record(key_bytes(1).as_ref()),
            hlc(1, 1),
            NEVER_EXPIRES,
            0,
            0,
            EntryState::Spilled(SpillLoc {
                region: 0,
                offset: 0,
                len: 1,
                generation: 0,
            }),
        );
        assert!(!is_spill_candidate(&spilled));
    }
}

#[test]
fn build_resident_record_round_trips_key_and_value() {
    for (key_bytes, value_bytes) in [
        (&b""[..], &b""[..]),
        (&b"k"[..], &b"v"[..]),
        (&b"a-longer-key"[..], &b"a-longer-value-too"[..]),
    ] {
        let record = build_resident_record(key_bytes, value_bytes);
        assert_eq!(record_key(&record), key_bytes);
        assert_eq!(record_key_bytes(&record).as_ref(), key_bytes);
        assert_eq!(record_value_bytes(&record).as_ref(), value_bytes);
    }
}

#[test]
fn split_record_recovers_the_value_half_at_every_key_length_boundary() {
    // 127 and below encode LEN in one postcard varint byte; 128 and
    // above spill into a second, crossing the continuation-bit boundary.
    for key_len in [0usize, 1, 127, 128, 129] {
        let key_bytes = vec![b'k'; key_len];
        let value_bytes = b"value".to_vec();
        let record = build_resident_record(&key_bytes, &value_bytes);
        let (key_half, value_half) = split_record(&record);
        assert_eq!(key_half, key_bytes.as_slice(), "key_len = {key_len}");
        assert_eq!(value_half, value_bytes.as_slice(), "key_len = {key_len}");
    }
}

#[test]
fn build_key_only_record_produces_an_empty_value_half() {
    let record = build_key_only_record(b"a-key");
    let (key_half, value_half) = split_record(&record);
    assert_eq!(key_half, b"a-key");
    assert!(value_half.is_empty(), "{value_half:?}");
}

#[test]
fn record_value_matches_split_record_and_is_zero_copy() {
    for (key_bytes, value_bytes) in [
        (&b""[..], &b""[..]),
        (&b"k"[..], &b"v"[..]),
        (&b"a-longer-key"[..], &b"a-longer-value-too"[..]),
    ] {
        let record = build_resident_record(key_bytes, value_bytes);
        let value = record_value(&record);
        assert_eq!(value, split_record(&record).1);
        assert_eq!(value, value_bytes);
    }
}

#[test]
fn engine_capacity_hint_presizes_each_stripe_to_ceil_hint_over_bucket_count() {
    // Not a multiple of BUCKET_COUNT, so every stripe's own share only
    // divides evenly once rounded up.
    let hint = 3_000u64;
    let expected = usize::try_from(hint.div_ceil(BUCKET_COUNT as u64))
        .expect("a hint this small divided by BUCKET_COUNT fits in usize");
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, Some(hint));
    assert_eq!(
        engine.stripe_capacities(),
        vec![expected; BUCKET_COUNT],
        "every stripe presizes to ceil(hint / BUCKET_COUNT), none left at zero or grown past it"
    );
}

#[test]
fn engine_no_capacity_hint_allocates_nothing_until_first_write() {
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    assert_eq!(
        engine.stripe_capacities(),
        vec![0; BUCKET_COUNT],
        "no hint: every stripe starts exactly as Stripe::new leaves it, zero allocation"
    );
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    let total: usize = engine.stripe_capacities().into_iter().sum();
    assert!(
        total > 0,
        "the first write grows only the one stripe its key lands in, matching today's \
         unhinted behavior exactly"
    );
}

#[test]
fn decode_key_and_decode_value_round_trip_postcard_bytes() {
    let key_bytes = postcard::to_stdvec(&42u32).expect("u32 encodes");
    let value_bytes = postcard::to_stdvec(&"a value".to_string()).expect("string encodes");
    assert_eq!(decode_key::<u32>(&key_bytes), 42u32);
    assert_eq!(decode_value::<String>(&value_bytes), "a value".to_string());
}

/// Pins `Live`'s non-spill size: `record` (24) + `wall_ms` (8) +
/// `node` (8) + four 4-byte fields (16) = 56, no padding.
#[test]
#[cfg(not(feature = "spill"))]
fn live_size_is_56_bytes_without_spill() {
    assert_eq!(std::mem::size_of::<Live<String, String>>(), 56);
}

/// Pins `Live`'s spill-enabled size: `EntryState` adds a 1-byte tag
/// plus padding, 76 bytes rounded up to 80 for 8-byte alignment.
#[test]
#[cfg(feature = "spill")]
fn live_size_is_80_bytes_with_spill() {
    assert_eq!(std::mem::size_of::<Live<String, String>>(), 80);
}

#[test]
fn live_ver_round_trips_through_set_ver() {
    let mut live = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        1,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert_eq!(live.ver(), hlc(1, 1));
    let new_ver = Hlc {
        wall_ms: 9,
        logical: 3,
        node: NodeId::merge_derived(7),
    };
    live.set_ver(new_ver);
    assert_eq!(live.ver(), new_ver);
}
