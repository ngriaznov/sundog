//! The `Record` byte buffer of a live entry and the helpers that build, split, decode and account for it.

use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde::de::DeserializeOwned;

use super::Live;

/// Max inline payload length for [`Record`], which sits at exactly 24
/// bytes either way, pinned by `record_size_is_24_bytes`.
const INLINE_CAP: usize = 22;

/// A live entry's raw `LEN ++ KEY ++ VALUE` record bytes: inline for
/// records up to [`INLINE_CAP`] bytes (a `Cache<String, String>` record
/// always lands here, with zero heap allocations), an owned `Box<[u8]>`
/// beyond that. Never shared and never `Clone`: a caller needing an owned
/// copy builds a fresh `Bytes::copy_from_slice` from
/// [`record_key_bytes`]/[`record_value_bytes`] instead.
pub(super) enum Record {
    /// Inline payload: the first `len` bytes of `buf` are the record.
    Inline { len: u8, buf: [u8; INLINE_CAP] },
    /// Heap payload for a record over [`INLINE_CAP`] bytes.
    Heap(Box<[u8]>),
}

impl Record {
    /// Builds a `Record` holding a copy of `bytes`: inline when
    /// `bytes.len() <= INLINE_CAP`, a fresh boxed copy otherwise.
    pub(super) fn from_slice(bytes: &[u8]) -> Record {
        if bytes.len() <= INLINE_CAP {
            let mut buf = [0u8; INLINE_CAP];
            buf[..bytes.len()].copy_from_slice(bytes);
            let len = u8::try_from(bytes.len()).expect("bytes.len() <= INLINE_CAP fits in a u8");
            Record::Inline { len, buf }
        } else {
            Record::Heap(Box::from(bytes))
        }
    }

    /// Bytes this record holds on the heap: `0` inline, its whole length
    /// boxed.
    fn heap_len(&self) -> usize {
        match self {
            Record::Inline { .. } => 0,
            Record::Heap(boxed) => boxed.len(),
        }
    }

    /// The record's bytes, from whichever variant holds them.
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Record::Inline { len, buf } => &buf[..*len as usize],
            Record::Heap(boxed) => boxed.as_ref(),
        }
    }
}

impl Deref for Record {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for Record {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Builds a resident record: `LEN ++ KEY ++ VALUE`, where `LEN` is
/// `key_bytes.len()` postcard-encoded (1 byte under 128 bytes).
pub(super) fn build_resident_record(key_bytes: &[u8], value_bytes: &[u8]) -> Record {
    let key_len = u32::try_from(key_bytes.len()).expect("a key's postcard encoding fits in u32");
    let len_prefix = postcard::to_stdvec(&key_len).expect("a u32 always encodes");
    let mut buf = Vec::with_capacity(len_prefix.len() + key_bytes.len() + value_bytes.len());
    buf.extend_from_slice(&len_prefix);
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(value_bytes);
    Record::from_slice(&buf)
}

/// Builds a spilled record: `LEN ++ KEY` only, always a fresh allocation
/// rather than a slice of the old record (which would keep its value
/// bytes resident).
#[cfg_attr(not(any(test, feature = "spill")), allow(dead_code))]
pub(super) fn build_key_only_record(key_bytes: &[u8]) -> Record {
    build_resident_record(key_bytes, &[])
}

/// Splits a record (`LEN ++ KEY ++ VALUE`) into its `(KEY, VALUE)` halves,
/// zero-copy.
pub(super) fn split_record(record: &[u8]) -> (&[u8], &[u8]) {
    let (key_len, rest): (u32, &[u8]) = postcard::take_from_bytes(record)
        .expect("record's LEN prefix is this engine's own postcard output");
    let key_len = key_len as usize;
    (&rest[..key_len], &rest[key_len..])
}

/// `record`'s key half, `split_record(record).0`.
pub(super) fn record_key(record: &[u8]) -> &[u8] {
    split_record(record).0
}

/// `record`'s value half, `split_record(record).1`, zero-copy: the value
/// counterpart to [`record_key`], for a decode that never crosses the lock.
pub(super) fn record_value(record: &[u8]) -> &[u8] {
    split_record(record).1
}

/// `record`'s key half as an owned `Bytes`, copied out: [`Record`] holds
/// no shared backing to slice into, so this always allocates.
pub(super) fn record_key_bytes(record: &Record) -> Bytes {
    Bytes::copy_from_slice(record_key(record))
}

/// `record`'s value half as an independently owned `Bytes`, copied out the
/// same way as [`record_key_bytes`]. Empty for a spilled record.
pub(super) fn record_value_bytes(record: &Record) -> Bytes {
    Bytes::copy_from_slice(record_value(record))
}

/// Decodes `key_bytes` back into `K`. Panics on a decode failure: a live
/// key reading as absent is a worse lie than a loud panic.
pub(super) fn decode_key<K: DeserializeOwned>(key_bytes: &[u8]) -> K {
    postcard::from_bytes(key_bytes).expect("key_bytes is this engine's own postcard output")
}

/// Decodes `value_bytes` back into `V`, mirroring [`decode_key`].
pub(super) fn decode_value<V: DeserializeOwned>(value_bytes: &[u8]) -> V {
    postcard::from_bytes(value_bytes).expect("value_bytes is this engine's own postcard output")
}

/// Resident bytes every live entry costs besides its record's heap bytes:
/// its [`Live`] slot and its `u32` row plus control byte in the stripe's
/// hash index.
pub(super) const ENTRY_OVERHEAD_BYTES: u64 =
    (size_of::<Live<(), ()>>() + size_of::<u32>() + 1) as u64;

/// The resident bytes `live` accounts for: [`ENTRY_OVERHEAD_BYTES`] plus
/// whatever its record holds on the heap. Allocator rounding and a stripe's
/// spare arena capacity are not counted.
pub(super) fn entry_bytes<K, V>(live: &Live<K, V>) -> u64 {
    ENTRY_OVERHEAD_BYTES + live.record.heap_len() as u64
}

/// Replaces `live`'s record with `record` and, when the engine counts
/// resident bytes, moves the total by the difference in heap bytes: every
/// in-place record swap goes through here, so the total stays exact.
pub(super) fn set_record<K, V>(
    live: &mut Live<K, V>,
    record: Record,
    resident_bytes: Option<&AtomicU64>,
) {
    if let Some(total) = resident_bytes {
        let (old, new) = (live.record.heap_len() as u64, record.heap_len() as u64);
        if new >= old {
            total.fetch_add(new - old, Ordering::Relaxed);
        } else {
            total.fetch_sub(old - new, Ordering::Relaxed);
        }
    }
    live.record = record;
}
