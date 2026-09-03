//! Property tests for the wire format: encode, decode, and re-encode
//! reproduces identical bytes and an equal value, for arbitrary
//! [`WireRecord`]s and [`Msg`]s across postcard and the raw-record layout.

use bytes::Bytes;
use proptest::prelude::*;
use smol_str::SmolStr;

use super::{Cell, Msg, WireRecord, decode, encode};
use crate::hlc::Hlc;
use crate::node::NodeId;

fn node_id_strategy() -> impl Strategy<Value = NodeId> {
    any::<u64>().prop_map(NodeId::from)
}

fn hlc_strategy() -> impl Strategy<Value = Hlc> {
    (any::<u64>(), any::<u32>(), node_id_strategy()).prop_map(|(wall_ms, logical, node)| Hlc {
        wall_ms,
        logical,
        node,
    })
}

fn bytes_strategy() -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..64).prop_map(Bytes::from)
}

fn smol_str_strategy() -> impl Strategy<Value = SmolStr> {
    "[a-zA-Z0-9_]{0,16}".prop_map(|s| SmolStr::new(&s))
}

fn wire_record_strategy() -> impl Strategy<Value = WireRecord> {
    (
        bytes_strategy(),
        proptest::option::of(bytes_strategy()),
        hlc_strategy(),
        proptest::option::of(any::<u64>()),
    )
        .prop_map(|(key, value, ver, expires_at_ms)| WireRecord {
            key,
            value,
            ver,
            expires_at_ms,
        })
}

/// The three raw-record-layout variants: `Replicate`, `ReplicateBatch`,
/// `StChunk`.
fn raw_record_msg_strategy() -> impl Strategy<Value = Msg> {
    prop_oneof![
        (smol_str_strategy(), wire_record_strategy())
            .prop_map(|(cache, rec)| Msg::Replicate { cache, rec }),
        (
            smol_str_strategy(),
            proptest::collection::vec(wire_record_strategy(), 0..8),
            any::<bool>(),
        )
            .prop_map(|(cache, recs, done)| Msg::StChunk { cache, recs, done }),
        (
            smol_str_strategy(),
            proptest::collection::vec(wire_record_strategy(), 0..8),
        )
            .prop_map(|(cache, recs)| Msg::ReplicateBatch { cache, recs }),
    ]
}

fn msg_strategy() -> impl Strategy<Value = Msg> {
    prop_oneof![
        (node_id_strategy(), any::<u64>())
            .prop_map(|(node, incarnation)| Msg::Hello { node, incarnation }),
        (smol_str_strategy(), bytes_strategy(), hlc_strategy())
            .prop_map(|(cache, key, ver)| Msg::Invalidate { cache, key, ver }),
        (smol_str_strategy(), wire_record_strategy())
            .prop_map(|(cache, rec)| Msg::Replicate { cache, rec }),
        smol_str_strategy().prop_map(|cache| Msg::StRequest { cache }),
        (
            smol_str_strategy(),
            proptest::collection::vec(wire_record_strategy(), 0..8),
            any::<bool>(),
        )
            .prop_map(|(cache, recs, done)| Msg::StChunk { cache, recs, done }),
        (
            smol_str_strategy(),
            proptest::collection::vec((any::<u16>(), any::<u64>()), 0..16),
        )
            .prop_map(|(cache, buckets)| Msg::AeDigest { cache, buckets }),
        (
            smol_str_strategy(),
            any::<u16>(),
            proptest::collection::vec((bytes_strategy(), hlc_strategy()), 0..8),
        )
            .prop_map(|(cache, bucket, entries)| Msg::AeBucket {
                cache,
                bucket,
                entries,
            }),
        (
            smol_str_strategy(),
            proptest::collection::vec(bytes_strategy(), 0..8),
        )
            .prop_map(|(cache, keys)| Msg::AePull { cache, keys }),
        (smol_str_strategy(), any::<u16>(), cells_strategy(),).prop_map(
            |(cache, bucket, cells)| Msg::AeSketch {
                cache,
                bucket,
                cells
            }
        ),
        (
            smol_str_strategy(),
            proptest::collection::vec(any::<u16>(), 0..8),
        )
            .prop_map(|(cache, buckets)| Msg::AeEntries { cache, buckets }),
        (
            smol_str_strategy(),
            any::<u16>(),
            proptest::collection::vec(any::<u64>(), 0..8),
        )
            .prop_map(|(cache, bucket, hashes)| Msg::AePullHashes {
                cache,
                bucket,
                hashes
            }),
        (
            smol_str_strategy(),
            proptest::collection::vec(wire_record_strategy(), 0..8),
        )
            .prop_map(|(cache, recs)| Msg::ReplicateBatch { cache, recs }),
        Just(Msg::ReqDone),
        part_msg_strategy(),
    ]
}

/// The four part-digest anti-entropy variants, split out of [`msg_strategy`]
/// so neither function trips clippy's line-count lint.
fn part_msg_strategy() -> impl Strategy<Value = Msg> {
    prop_oneof![
        (
            smol_str_strategy(),
            any::<u16>(),
            proptest::collection::vec(any::<u64>(), 0..8),
        )
            .prop_map(|(cache, bucket, digests)| Msg::AePartDigests {
                cache,
                bucket,
                digests
            }),
        (
            smol_str_strategy(),
            proptest::collection::vec((any::<u16>(), any::<u8>()), 0..16),
        )
            .prop_map(|(cache, parts)| Msg::AeParts { cache, parts }),
        (
            smol_str_strategy(),
            any::<u16>(),
            any::<u8>(),
            proptest::collection::vec((bytes_strategy(), hlc_strategy()), 0..8),
        )
            .prop_map(|(cache, bucket, part, entries)| Msg::AePart {
                cache,
                bucket,
                part,
                entries,
            }),
        (
            smol_str_strategy(),
            any::<u16>(),
            any::<u8>(),
            cells_strategy(),
        )
            .prop_map(|(cache, bucket, part, cells)| Msg::AePartSketch {
                cache,
                bucket,
                part,
                cells,
            }),
    ]
}

/// Real `Cell`s built through `Iblt`'s `pub(crate)` API, since `Cell`'s
/// fields are private outside `cluster::sketch`.
fn cells_strategy() -> impl Strategy<Value = Vec<Cell>> {
    proptest::collection::vec((any::<u64>(), hlc_strategy()), 0..8).prop_map(|elems| {
        let mut iblt = crate::cluster::sketch::Iblt::new(6);
        for (key_hash, ver) in elems {
            iblt.insert(key_hash, ver);
        }
        iblt.into_cells()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// A [`WireRecord`]'s postcard encoding is byte-stable across re-encoding.
    #[test]
    fn wire_record_postcard_encoding_is_byte_stable(rec in wire_record_strategy()) {
        let encoded1 = postcard::to_stdvec(&rec).expect("WireRecord always postcard-encodes");
        let decoded: WireRecord =
            postcard::from_bytes(&encoded1).expect("freshly encoded bytes always decode");
        let encoded2 = postcard::to_stdvec(&decoded).expect("decoded value always re-encodes");
        prop_assert_eq!(encoded1, encoded2);
        prop_assert_eq!(rec, decoded);
    }

    /// Same property for the full [`Msg`] enum through `encode`/`decode`,
    /// covering every variant including tombstones and boundary sizes.
    #[test]
    fn msg_postcard_encoding_is_byte_stable(msg in msg_strategy()) {
        let encoded1 = encode(&msg).expect("Msg always encodes under MAX_FRAME");
        let decoded = decode(&encoded1).expect("freshly encoded frame always decodes");
        let encoded2 = encode(&decoded).expect("decoded value always re-encodes");
        prop_assert_eq!(encoded1, encoded2);
        prop_assert_eq!(msg, decoded);
    }

    /// The raw-record layout's key/value `Bytes` decode as zero-copy
    /// slices into the received frame: a decoded record's pointers fall
    /// within the original frame's allocation, not a fresh one.
    #[test]
    fn raw_record_frames_decode_without_copying_payload_bytes(msg in raw_record_msg_strategy()) {
        let frame = encode(&msg).expect("encodes");
        let frame_start = frame.as_ptr() as usize;
        let frame_end = frame_start + frame.len();
        let decoded = decode(&frame).expect("decodes");
        let recs: &[WireRecord] = match &decoded {
            Msg::Replicate { rec, .. } => std::slice::from_ref(rec),
            Msg::ReplicateBatch { recs, .. } | Msg::StChunk { recs, .. } => recs,
            _ => unreachable!("raw_record_msg_strategy only generates record-carrying variants"),
        };
        for rec in recs {
            if !rec.key.is_empty() {
                let ptr = rec.key.as_ptr() as usize;
                prop_assert!(ptr >= frame_start && ptr < frame_end);
            }
            if let Some(value) = &rec.value
                && !value.is_empty()
            {
                let ptr = value.as_ptr() as usize;
                prop_assert!(ptr >= frame_start && ptr < frame_end);
            }
        }
    }
}
