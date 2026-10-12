//! Key encoding for reads, key hashing and the stripe, part and digest-slot arithmetic.

use serde::Serialize;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::CodecError;
use crate::store::{BUCKET_COUNT, PART_COUNT};

use super::Live;
use super::record::record_key;

/// Stack-buffer size for a key's postcard encoding on the read path, large
/// enough for every key type this crate ships and most user key types. A key
/// that doesn't fit falls back to one heap allocation.
const KEY_STACK_BUF: usize = 128;

/// Holds one postcard-encoded key: on the stack when it fits [`KEY_STACK_BUF`],
/// on the heap otherwise.
pub(super) enum KeyBuf {
    Stack([u8; KEY_STACK_BUF], usize),
    Heap(Vec<u8>),
}

impl KeyBuf {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Stack(buf, len) => &buf[..*len],
            Self::Heap(v) => v,
        }
    }
}

/// Encodes `key` for a read, on the stack when it fits.
pub(super) fn encode_key_for_read<K: Serialize>(key: &K) -> Result<KeyBuf, CodecError> {
    let mut buf = [0u8; KEY_STACK_BUF];
    let stack_len = match postcard::to_slice(key, &mut buf) {
        Ok(written) => Some(written.len()),
        Err(_) => None,
    };
    match stack_len {
        Some(len) => Ok(KeyBuf::Stack(buf, len)),
        None => Ok(KeyBuf::Heap(postcard::to_stdvec(key)?)),
    }
}

/// The 64-bit xxh3 of a key's encoded bytes, computed once per operation and
/// reused as both the stripe index ([`stripe_index_from_hash`]) and the hash
/// [`hashbrown::HashTable`] stores each entry under.
pub(crate) fn hash_key_bytes(key_bytes: &[u8]) -> u64 {
    xxh3_64(key_bytes)
}

/// The stripe, an anti-entropy bucket, a precomputed key hash belongs to.
pub(crate) fn stripe_index_from_hash(hash: u64) -> usize {
    usize::try_from(hash & (BUCKET_COUNT as u64 - 1))
        .expect("invariant: masked to BUCKET_COUNT - 1, always fits in usize")
}

/// The second-level anti-entropy part, one of [`PART_COUNT`], a precomputed
/// key hash belongs to within its bucket: the six hash bits above the ten
/// [`stripe_index_from_hash`] consumes.
pub(crate) fn part_index_from_hash(hash: u64) -> usize {
    ((hash >> 10) & 63) as usize
}

/// The flat index into `Engine::digest`, `BUCKET_COUNT * PART_COUNT` atomics
/// long, holding `bucket`'s `part`th part digest.
pub(super) fn digest_slot(bucket: usize, part: usize) -> usize {
    bucket * PART_COUNT + part
}

pub(super) fn hasher_for<K, V>(live: &Live<K, V>) -> u64 {
    xxh3_64(record_key(&live.record))
}
