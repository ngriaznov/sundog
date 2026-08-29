//! The wire format: what actually crosses the data-plane TCP mesh, and the
//! postcard encode/decode helpers every connection uses. Plan §6.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::node::NodeId;

/// Hard cap on any single wire frame, in bytes (4 MiB). Oversized values are
/// rejected at the API boundary (`CacheError::ValueTooLarge`) rather than
/// fragmented — plan §6, §13.
pub const MAX_FRAME: usize = 4 * 1024 * 1024;

/// One versioned key/value record as it travels the wire: a live entry when
/// `value` is `Some`, a tombstone when `value` is `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRecord {
    /// The postcard-encoded cache key.
    pub key: Bytes,
    /// The postcard-encoded value, or `None` for a tombstone.
    pub value: Option<Bytes>,
    /// The write's HLC version stamp.
    pub ver: Hlc,
    /// Absolute expiry in epoch milliseconds, or `None` for no TTL.
    pub expires_at_ms: Option<u64>,
}

impl WireRecord {
    /// Returns `true` if this record is a tombstone (a deletion marker).
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}

/// Every message exchanged on the data-plane mesh. Plan §6.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Msg {
    /// Sent once a new connection is established: identifies the sender.
    Hello {
        /// The sending node's id.
        node: NodeId,
        /// The sending node's membership incarnation number.
        incarnation: u64,
    },
    /// Invalidation-mode fan-out: "the entry at `key` changed, drop your copy."
    Invalidate {
        /// The target cache's name.
        cache: SmolStr,
        /// The postcard-encoded key that changed.
        key: Bytes,
        /// The version of the write that caused the invalidation.
        ver: Hlc,
    },
    /// Replication-mode fan-out: the full record to apply.
    Replicate {
        /// The target cache's name.
        cache: SmolStr,
        /// The record to apply.
        rec: WireRecord,
    },
    /// Requests a full snapshot stream of a cache, for state transfer on join.
    StRequest {
        /// The cache to snapshot.
        cache: SmolStr,
    },
    /// One chunk of a state-transfer snapshot stream.
    StChunk {
        /// The cache being transferred.
        cache: SmolStr,
        /// A batch of records (~500 per plan §9).
        recs: Vec<WireRecord>,
        /// `true` on the final chunk of the stream.
        done: bool,
    },
    /// Anti-entropy round, step 1: this node's per-bucket digest array.
    AeDigest {
        /// The cache being reconciled.
        cache: SmolStr,
        /// `(bucket, xor_digest)` pairs, one per bucket (1 024 total).
        buckets: Vec<(u16, u64)>,
    },
    /// Anti-entropy round, step 2: key/version listing for a mismatched bucket.
    AeBucket {
        /// The cache being reconciled.
        cache: SmolStr,
        /// Which bucket this listing covers.
        bucket: u16,
        /// `(key, version)` pairs live in that bucket on the sender.
        entries: Vec<(Bytes, Hlc)>,
    },
    /// Anti-entropy round, step 3: "send me your full records for these keys."
    AePull {
        /// The cache being reconciled.
        cache: SmolStr,
        /// The keys the requester is missing or holds an older version of.
        keys: Vec<Bytes>,
    },
    /// Replication-mode fan-out, batched: several records to apply together.
    /// Never built at enqueue time — `net::conn`'s per-peer writer
    /// opportunistically coalesces consecutive same-cache queued
    /// [`Msg::Replicate`] messages into this on the wire (plan §6's smart
    /// batching), applied under one acquisition of the store's apply
    /// serialization lock (`store::ShardOps::apply_remote_batch`). Appended
    /// after every pre-existing variant so their encodings are unchanged.
    ReplicateBatch {
        /// The target cache's name.
        cache: SmolStr,
        /// The records to apply, in order.
        recs: Vec<WireRecord>,
    },
}

/// Encodes a message to its postcard wire form.
///
/// # Errors
///
/// Returns [`CodecError::Postcard`] if serialization fails (unexpected for
/// these types, but `postcard::to_stdvec` is fallible in general) or
/// [`CodecError::FrameTooLarge`] if the encoded frame would exceed
/// [`MAX_FRAME`].
pub fn encode(msg: &Msg) -> Result<Vec<u8>, CodecError> {
    let bytes = postcard::to_stdvec(msg)?;
    if bytes.len() > MAX_FRAME {
        return Err(CodecError::FrameTooLarge {
            size: bytes.len(),
            limit: MAX_FRAME,
        });
    }
    Ok(bytes)
}

/// Decodes a message from its postcard wire form.
///
/// # Errors
///
/// Returns [`CodecError::FrameTooLarge`] if `frame` exceeds [`MAX_FRAME`], or
/// [`CodecError::Postcard`] if the bytes are not a valid encoding of [`Msg`].
pub fn decode(frame: &[u8]) -> Result<Msg, CodecError> {
    if frame.len() > MAX_FRAME {
        return Err(CodecError::FrameTooLarge {
            size: frame.len(),
            limit: MAX_FRAME,
        });
    }
    Ok(postcard::from_bytes(frame)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_hlc() -> Hlc {
        Hlc {
            wall_ms: 1_700_000_000_000,
            logical: 7,
            node: NodeId::from(42),
        }
    }

    fn sample_record(value: Option<&str>) -> WireRecord {
        WireRecord {
            key: Bytes::from_static(b"user:1"),
            value: value.map(|v| Bytes::copy_from_slice(v.as_bytes())),
            ver: sample_hlc(),
            expires_at_ms: Some(1_700_000_600_000),
        }
    }

    fn roundtrip(msg: &Msg) {
        let encoded = encode(msg).expect("encodes");
        let decoded = decode(&encoded).expect("decodes");
        assert_eq!(*msg, decoded);
    }

    #[test]
    fn tombstone_has_no_value() {
        assert!(sample_record(None).is_tombstone());
        assert!(!sample_record(Some("x")).is_tombstone());
    }

    #[test]
    fn roundtrip_hello() {
        roundtrip(&Msg::Hello {
            node: NodeId::from(1),
            incarnation: 3,
        });
    }

    #[test]
    fn roundtrip_invalidate() {
        roundtrip(&Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: Bytes::from_static(b"k1"),
            ver: sample_hlc(),
        });
    }

    #[test]
    fn roundtrip_replicate() {
        roundtrip(&Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: sample_record(Some("v1")),
        });
    }

    #[test]
    fn roundtrip_replicate_tombstone() {
        roundtrip(&Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: sample_record(None),
        });
    }

    #[test]
    fn roundtrip_st_request() {
        roundtrip(&Msg::StRequest {
            cache: SmolStr::new("users"),
        });
    }

    #[test]
    fn roundtrip_st_chunk() {
        roundtrip(&Msg::StChunk {
            cache: SmolStr::new("users"),
            recs: vec![sample_record(Some("v1")), sample_record(None)],
            done: true,
        });
    }

    #[test]
    fn roundtrip_ae_digest() {
        roundtrip(&Msg::AeDigest {
            cache: SmolStr::new("users"),
            buckets: vec![(0, 111), (1023, 222)],
        });
    }

    #[test]
    fn roundtrip_ae_bucket() {
        roundtrip(&Msg::AeBucket {
            cache: SmolStr::new("users"),
            bucket: 42,
            entries: vec![(Bytes::from_static(b"k1"), sample_hlc())],
        });
    }

    #[test]
    fn roundtrip_ae_pull() {
        roundtrip(&Msg::AePull {
            cache: SmolStr::new("users"),
            keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
        });
    }

    #[test]
    fn roundtrip_replicate_batch() {
        roundtrip(&Msg::ReplicateBatch {
            cache: SmolStr::new("users"),
            recs: vec![sample_record(Some("v1")), sample_record(None)],
        });
    }

    #[test]
    fn oversized_frame_is_rejected_on_decode() {
        let oversized = vec![0u8; MAX_FRAME + 1];
        assert!(matches!(
            decode(&oversized),
            Err(CodecError::FrameTooLarge { .. })
        ));
    }
}

#[cfg(test)]
mod prop_tests;
