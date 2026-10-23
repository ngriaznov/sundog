//! Shared helpers and the topic modules of the anti-entropy unit tests.

use super::*;

#[cfg(not(feature = "sim"))]
mod classify;
mod diff;
#[cfg(not(feature = "sim"))]
mod parts;
#[cfg(not(feature = "sim"))]
mod pull;
#[cfg(not(feature = "sim"))]
mod repair;
mod rules;
mod tombstone_drain;

/// A postcard-encoded `(key, value)` pair and the `WireRecord`
/// `apply_remote` takes. Real-transport only.
#[cfg(not(feature = "sim"))]
fn encode_test_record(key: u32, value: &str, node: NodeId) -> (Bytes, WireRecord) {
    let key_bytes = Bytes::from(postcard::to_stdvec(&key).expect("test key encodes"));
    let value_bytes = Bytes::from(postcard::to_stdvec(value).expect("test value encodes"));
    let rec = WireRecord {
        key: key_bytes.clone(),
        value: Some(value_bytes),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node,
        },
        expires_at_ms: None,
    };
    (key_bytes, rec)
}

/// `n` distinct u32 keys, each landing in a different bucket.
/// Real-transport only.
#[cfg(not(feature = "sim"))]
fn keys_in_distinct_buckets(n: usize) -> Vec<(u32, u16)> {
    let mut found = Vec::new();
    let mut seen = HashSet::new();
    for key in 0u32.. {
        let bytes = crate::store::encode_key(&key).expect("u32 key encodes");
        let bucket = crate::store::bucket_of(&bytes);
        if seen.insert(bucket) {
            found.push((key, bucket));
            if found.len() == n {
                break;
            }
        }
    }
    found
}
