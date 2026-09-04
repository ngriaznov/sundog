//! Writes one valid encoding of every message shape into each fuzz target's
//! corpus. Run with plain cargo: `cargo +nightly run --bin gen_seeds`.

use std::fs;

use bytes::Bytes;
use sundog::hlc::Hlc;
use sundog::node::NodeId;
use sundog::wire::{Cell, Msg, WireRecord};

fn main() {
    let node = NodeId::from(7);
    let ver = Hlc {
        wall_ms: 1_700_000_000_000,
        logical: 3,
        node,
    };
    let rec = |key: &[u8], value: Option<&[u8]>| WireRecord {
        key: Bytes::copy_from_slice(key),
        value: value.map(Bytes::copy_from_slice),
        ver,
        expires_at_ms: Some(1_900_000_000_000),
    };
    let msgs = [
        Msg::Hello {
            node,
            incarnation: 42,
            protocol: sundog::wire::PROTOCOL_VERSION,
        },
        Msg::Invalidate {
            cache: "users".into(),
            key: Bytes::from_static(b"\x01k"),
            ver,
        },
        Msg::Replicate {
            cache: "users".into(),
            rec: rec(b"\x01k", Some(b"\x05value")),
        },
        Msg::Replicate {
            cache: "users".into(),
            rec: rec(b"\x01t", None),
        },
        Msg::ReplicateBatch {
            cache: "users".into(),
            recs: vec![rec(b"\x01a", Some(b"1")), rec(b"\x01b", None)],
        },
        Msg::StRequest {
            cache: "users".into(),
        },
        Msg::StChunk {
            cache: "users".into(),
            recs: vec![rec(b"\x01c", Some(b"xyz"))],
            done: true,
        },
        Msg::AeDigest {
            cache: "users".into(),
            buckets: vec![(0, 1), (1023, u64::MAX)],
        },
        Msg::AeBucket {
            cache: "users".into(),
            bucket: 512,
            entries: vec![(Bytes::from_static(b"\x01k"), ver)],
        },
        Msg::AeSketch {
            cache: "users".into(),
            bucket: 512,
            // `Cell`'s fields are private to `cluster::sketch`, so a seed
            // can only reach `Cell::default`, a valid empty-cell encoding.
            cells: vec![Cell::default(); 6],
        },
        Msg::AeEntries {
            cache: "users".into(),
            buckets: vec![0, 512, 1023],
        },
        Msg::AePull {
            cache: "users".into(),
            keys: vec![Bytes::from_static(b"\x01k")],
        },
        Msg::AePullHashes {
            cache: "users".into(),
            bucket: 512,
            hashes: vec![1, 2, u64::MAX],
        },
    ];
    for target in ["decode_never_panics", "decode_encode_roundtrip"] {
        let dir = format!("corpus/{target}");
        fs::create_dir_all(&dir).expect("corpus dir");
        for (i, msg) in msgs.iter().enumerate() {
            let frame = sundog::wire::encode(msg).expect("seed encodes");
            fs::write(format!("{dir}/seed-{i:02}"), &frame).expect("seed writes");
        }
    }
    println!("seeded {} frames per target", msgs.len());
}
