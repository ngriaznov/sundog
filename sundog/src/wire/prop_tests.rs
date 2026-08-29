//! Property tests for the wire format (plan §11.1): postcard encoding must be
//! deterministic for arbitrary [`WireRecord`]s and [`Msg`]s — encode, decode,
//! re-encode always reproduces the exact same bytes, and decodes back to an
//! equal value.

use bytes::Bytes;
use proptest::prelude::*;
use smol_str::SmolStr;

use super::{Msg, WireRecord, decode, encode};
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
        (
            smol_str_strategy(),
            proptest::collection::vec(wire_record_strategy(), 0..8),
        )
            .prop_map(|(cache, recs)| Msg::ReplicateBatch { cache, recs }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// A [`WireRecord`]'s postcard encoding is byte-stable: encoding,
    /// decoding, and re-encoding always reproduces the exact same bytes.
    #[test]
    fn wire_record_postcard_encoding_is_byte_stable(rec in wire_record_strategy()) {
        let encoded1 = postcard::to_stdvec(&rec).expect("WireRecord always postcard-encodes");
        let decoded: WireRecord =
            postcard::from_bytes(&encoded1).expect("just-encoded bytes always decode");
        let encoded2 = postcard::to_stdvec(&decoded).expect("decoded value always re-encodes");
        prop_assert_eq!(encoded1, encoded2);
        prop_assert_eq!(rec, decoded);
    }

    /// Same property for the full [`Msg`] enum, through the wire module's own
    /// `encode`/`decode` (which also enforces `MAX_FRAME`).
    #[test]
    fn msg_postcard_encoding_is_byte_stable(msg in msg_strategy()) {
        let encoded1 = encode(&msg).expect("Msg always encodes under MAX_FRAME");
        let decoded = decode(&encoded1).expect("just-encoded frame always decodes");
        let encoded2 = encode(&decoded).expect("decoded value always re-encodes");
        prop_assert_eq!(encoded1, encoded2);
        prop_assert_eq!(msg, decoded);
    }
}
