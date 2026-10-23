//! The requester side of a digest exchange: the slices of one bucket's
//! listing join into one mismatch.

use bytes::Bytes;
use smol_str::SmolStr;

use super::reply::{push_bucket_listing, push_mismatch};
use crate::hlc::Hlc;
use crate::net::AeMismatch;
use crate::node::NodeId;
use crate::store::KeyVersion;
use crate::wire::Msg;

fn entry(n: u8) -> KeyVersion {
    KeyVersion {
        key: Bytes::from(vec![n]),
        version: Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: NodeId::from(1),
        },
    }
}

fn listing_message(bucket: u16, entries: &[KeyVersion]) -> Msg {
    Msg::AeBucket {
        cache: SmolStr::new("users"),
        bucket,
        entries: entries
            .iter()
            .map(|kv| (kv.key.clone(), kv.version))
            .collect(),
    }
}

#[test]
fn the_slices_of_one_bucket_join_in_arrival_order() {
    let mut out = Vec::new();
    push_bucket_listing(&mut out, 3, vec![entry(1), entry(2)]);
    push_bucket_listing(&mut out, 3, vec![entry(3)]);

    assert_eq!(
        out,
        vec![AeMismatch::Bucket(3, vec![entry(1), entry(2), entry(3)])]
    );
}

#[test]
fn a_slice_finds_its_bucket_past_other_buckets_and_other_kinds_of_mismatch() {
    let mut out = vec![AeMismatch::Sketch(5, Vec::new())];
    push_bucket_listing(&mut out, 3, vec![entry(1)]);
    push_bucket_listing(&mut out, 7, vec![entry(2)]);
    push_bucket_listing(&mut out, 3, vec![entry(3)]);

    assert_eq!(
        out,
        vec![
            AeMismatch::Sketch(5, Vec::new()),
            AeMismatch::Bucket(3, vec![entry(1), entry(3)]),
            AeMismatch::Bucket(7, vec![entry(2)]),
        ]
    );
}

#[test]
fn an_empty_listing_is_still_a_mismatch_of_its_bucket() {
    let mut out = Vec::new();
    push_bucket_listing(&mut out, 9, Vec::new());

    assert_eq!(out, vec![AeMismatch::Bucket(9, Vec::new())]);
}

#[test]
fn reply_messages_fold_in_as_one_mismatch_per_bucket_whatever_arrives_between_them() {
    let mut out = Vec::new();
    for msg in [
        listing_message(3, &[entry(1)]),
        Msg::AeSketch {
            cache: SmolStr::new("users"),
            bucket: 5,
            cells: Vec::new(),
        },
        listing_message(3, &[entry(2)]),
        Msg::AePartDigests {
            cache: SmolStr::new("users"),
            bucket: 8,
            digests: vec![1, 2],
        },
        Msg::ReqDone,
    ] {
        push_mismatch(msg, &mut out);
    }

    assert_eq!(
        out,
        vec![
            AeMismatch::Bucket(3, vec![entry(1), entry(2)]),
            AeMismatch::Sketch(5, Vec::new()),
            AeMismatch::PartDigests(8, vec![1, 2]),
        ]
    );
}
