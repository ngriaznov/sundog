//! Unit tests for `Record`: its size, inline and heap storage, and its `Deref` and `AsRef` views.

use super::*;

#[test]
fn record_size_is_24_bytes() {
    assert_eq!(std::mem::size_of::<Record>(), 24);
}

#[test]
fn record_round_trips_inline_at_0_18_and_22_bytes() {
    for len in [0usize, 18, 22] {
        let bytes = vec![b'x'; len];
        let record = Record::from_slice(&bytes);
        assert!(
            matches!(record, Record::Inline { .. }),
            "len = {len} should stay inline"
        );
        assert_eq!(record.as_slice(), bytes.as_slice(), "len = {len}");
    }
}

#[test]
fn record_round_trips_heap_at_23_and_300_bytes() {
    for len in [23usize, 300] {
        let bytes = vec![b'y'; len];
        let record = Record::from_slice(&bytes);
        assert!(
            matches!(record, Record::Heap(_)),
            "len = {len} should go to the heap"
        );
        assert_eq!(record.as_slice(), bytes.as_slice(), "len = {len}");
    }
}

#[test]
fn record_as_slice_and_deref_agree() {
    for bytes in [&b""[..], &b"short"[..], &vec![b'z'; 100][..]] {
        let record = Record::from_slice(bytes);
        assert_eq!(record.as_slice(), bytes);
        assert_eq!(&*record, bytes, "Deref must match as_slice");
        assert_eq!(record.as_ref(), bytes, "AsRef must match as_slice");
    }
}
