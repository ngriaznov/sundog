//! The wire format of the data-plane mesh and its encode/decode helpers.
//!
//! Every frame starts with a one-byte discriminant. Control messages carry
//! `FRAME_KIND_POSTCARD` and are postcard-encoded. The record-carrying
//! variants [`Msg::Replicate`], [`Msg::ReplicateBatch`], and [`Msg::StChunk`]
//! carry `FRAME_KIND_RAW_RECORD`: a fixed header read through `zerocopy`'s
//! [`FromBytes`] and [`IntoBytes`] views, then each record's key and value
//! bytes back to back.
//!
//! Decoding a raw-record frame slices `Bytes` views out of the received
//! buffer with no payload copy. Encoding assembles one exact-size `BytesMut`
//! from bytes the caller already owns, including the cached
//! `store::Stored::encoded` value.
//!
//! Under feature `tls`, rustls copies application bytes through its own
//! buffers; the zero-copy path holds only up to the TCP framing layer.
//!
//! A [`WireRecord`] decoded from a raw-record frame borrows from that frame's
//! buffer. A replica that caches the value as `store::Stored::encoded` keeps
//! the whole frame alive while the entry lives. A frame is capped under
//! [`MAX_FRAME`], so the retention is bounded.

use std::mem::size_of;

use bytes::{BufMut as _, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::node::NodeId;

/// One cell of the anti-entropy IBLT sketch, defined in `cluster::sketch`.
pub use crate::cluster::sketch::Cell;

/// Hard cap on any single wire frame: 4 MiB. Oversized values are rejected
/// at the API boundary (`CacheError::ValueTooLarge`) instead.
pub const MAX_FRAME: usize = 4 * 1024 * 1024;

/// The wire protocol this build speaks, carried in every [`Msg::Hello`] and
/// gossiped with membership. A node answers a peer only with what that
/// peer's protocol understands, so a cluster mixing this version with the
/// one before it keeps replicating and repairing through a rolling upgrade.
///
/// - 1: the 0.3 releases. [`Msg::Hello`] has no `protocol` field.
/// - 2: 0.4. Adds [`Msg::AePartDigests`], [`Msg::AeParts`],
///   [`Msg::AePart`], [`Msg::AePartSketch`], and [`Msg::StUnavailable`].
pub const PROTOCOL_VERSION: u16 = 2;

/// The oldest peer protocol this build still serves in full.
pub const MIN_PROTOCOL_VERSION: u16 = 1;

/// The protocol that introduced the part-digest anti-entropy replies; a
/// responder sends none of them to an older peer.
pub const PROTOCOL_PART_DIGESTS: u16 = 2;

/// The protocol that introduced [`Msg::StUnavailable`]; a cold donor serves
/// an older peer rather than declining it.
pub const PROTOCOL_ST_UNAVAILABLE: u16 = 2;

/// Whether a peer speaking `peer_protocol` understands a message kind
/// introduced in protocol `since`.
#[must_use]
pub const fn peer_supports(peer_protocol: u16, since: u16) -> bool {
    peer_protocol >= since
}

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
    /// Returns `true` if this record is a tombstone.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}

/// Every message exchanged on the data-plane mesh. `#[non_exhaustive]`: new
/// kinds are added as the protocols grow, so matching code needs a wildcard
/// arm rather than a breaking release for every addition.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Msg {
    /// Sent once a new connection is established: identifies the sender.
    Hello {
        node: NodeId,
        /// The sender's membership incarnation number.
        incarnation: u64,
        /// The sender's [`PROTOCOL_VERSION`]. Last on purpose: a protocol-1
        /// sender omits it and decodes here as protocol 1, and a newer
        /// sender's further trailing fields are ignored.
        protocol: u16,
    },
    /// Invalidation-mode fan-out: "the entry at `key` changed, drop your
    /// copy." `key` is postcard-encoded; `ver` is the write's version.
    Invalidate {
        cache: SmolStr,
        key: Bytes,
        ver: Hlc,
    },
    /// Replication-mode fan-out: the full record. Raw-record layout, not
    /// postcard.
    Replicate { cache: SmolStr, rec: WireRecord },
    /// Requests a full snapshot stream of a cache, for state transfer on join.
    StRequest { cache: SmolStr },
    /// One chunk (~500 records) of a state-transfer snapshot stream, `done`
    /// set on the final one. Raw-record layout, not postcard.
    StChunk {
        cache: SmolStr,
        recs: Vec<WireRecord>,
        done: bool,
    },
    /// Anti-entropy round, step 1: this node's per-bucket digest array, as
    /// `(bucket, xor_digest)` pairs, one per bucket (1 024 total).
    AeDigest {
        cache: SmolStr,
        buckets: Vec<(u16, u64)>,
    },
    /// Anti-entropy round, step 2: `(key, version)` pairs live in the
    /// mismatched `bucket` on the sender.
    AeBucket {
        cache: SmolStr,
        bucket: u16,
        entries: Vec<(Bytes, Hlc)>,
    },
    /// Anti-entropy round, step 3: "send me your full records for these
    /// keys," `keys` being ones the requester is missing or holds stale.
    AePull { cache: SmolStr, keys: Vec<Bytes> },
    /// Replication-mode fan-out, batched: several records to apply
    /// together. `net::conn`'s per-peer writer coalesces consecutive
    /// same-cache queued [`Msg::Replicate`] messages into this; the batch
    /// applies under one lock acquisition. Raw-record layout, not postcard.
    ReplicateBatch {
        cache: SmolStr,
        recs: Vec<WireRecord>,
    },
    /// Marks the end of one reply on a request/response connection kept
    /// open for reuse. Declared after every variant that predates it, so
    /// their postcard encodings stay unchanged.
    ReqDone,
    /// Anti-entropy round, step 2, large-bucket path: an IBLT sketch over a
    /// mismatched bucket's `(key_hash, version)` pairs, sent instead of
    /// [`Msg::AeBucket`] once the bucket's own listing would cost more on
    /// the wire than the sketch. The initiator subtracts its own local
    /// sketch from this one and peels the result; on failure it falls back
    /// to [`Msg::AeEntries`].
    AeSketch {
        cache: SmolStr,
        bucket: u16,
        /// `ClusterConfig::ae_sketch_cells` of them, built by the sender.
        cells: Vec<Cell>,
    },
    /// Anti-entropy round, step 2 fallback: the sketch-fallback request for
    /// the full `(key, version)` listing of these buckets, after one or
    /// more `AeSketch` replies failed to decode. Answered like `AeDigest`.
    AeEntries { cache: SmolStr, buckets: Vec<u16> },
    /// Anti-entropy round, step 3, sketch-decoded path: the sketch-decoded
    /// counterpart to [`Msg::AePull`], requesting records for the entries
    /// of `bucket` whose key hash is one of `hashes`. Answered like `AePull`.
    AePullHashes {
        cache: SmolStr,
        bucket: u16,
        hashes: Vec<u64>,
    },
    /// Anti-entropy round, step 2, part path: this node's 64 part digests for
    /// a mismatched `bucket` whose entry count passed
    /// `ClusterConfig::ae_part_min_bucket`, sent instead of [`Msg::AeBucket`]
    /// or [`Msg::AeSketch`]. The initiator compares these against its own
    /// part digests for the same bucket and requests only the parts that
    /// differ, via [`Msg::AeParts`].
    AePartDigests {
        cache: SmolStr,
        bucket: u16,
        digests: Vec<u64>,
    },
    /// Anti-entropy round, step 3, part path: "send me your part digests'
    /// mismatched `(bucket, part)` pairs' answer," the part-grained
    /// counterpart of [`Msg::AeDigest`]. Answered one reply per part
    /// ([`Msg::AePart`] or [`Msg::AePartSketch`]), then [`Msg::ReqDone`].
    AeParts {
        cache: SmolStr,
        parts: Vec<(u16, u8)>,
    },
    /// A part's full `(key, version)` listing, the part-grained counterpart
    /// of [`Msg::AeBucket`].
    AePart {
        cache: SmolStr,
        bucket: u16,
        part: u8,
        entries: Vec<(Bytes, Hlc)>,
    },
    /// A part's IBLT sketch, the part-grained counterpart of
    /// [`Msg::AeSketch`], sent once the part's own entry count passes
    /// `ClusterConfig::ae_sketch_min_bucket`.
    AePartSketch {
        cache: SmolStr,
        bucket: u16,
        part: u8,
        cells: Vec<Cell>,
    },
    /// The answer to a [`Msg::StRequest`] from a node that cannot donate
    /// `cache` yet: it has not completed its own state transfer for it, or
    /// never opened it. The requester moves on to its next candidate.
    StUnavailable { cache: SmolStr },
}

/// Frame discriminant: everything after this byte is a postcard-encoded
/// [`Msg`].
const FRAME_KIND_POSTCARD: u8 = 0;
/// Frame discriminant: everything after this byte is the raw-record layout
/// for [`Msg::Replicate`]/[`Msg::ReplicateBatch`]/[`Msg::StChunk`].
const FRAME_KIND_RAW_RECORD: u8 = 1;

const RAW_KIND_REPLICATE: u8 = 0;
const RAW_KIND_REPLICATE_BATCH: u8 = 1;
const RAW_KIND_ST_CHUNK: u8 = 2;

/// Record-level flag: this record is a tombstone (`WireRecord::value` is
/// `None`).
const RECORD_FLAG_TOMBSTONE: u8 = 0b01;
/// Record-level flag: `expires_at_ms` is `Some` and the header's field holds
/// its value. Without this flag the field is meaningless, always `0`, so a
/// real expiry of `0` is never ambiguous with "no expiry."
const RECORD_FLAG_HAS_EXPIRY: u8 = 0b10;

/// Fixed header preceding a raw-record frame's payload: which of the three
/// record-carrying [`Msg`] variants this is, the state-transfer `done` flag
/// (always `0` outside [`Msg::StChunk`]), the cache name's byte length, and
/// how many [`RecordHeader`]-prefixed records follow. Every field is a
/// byte-order-explicit, alignment-1 `zerocopy` integer: no implicit padding.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct RawFrameHeader {
    msg_kind: u8,
    done: u8,
    cache_len: U16,
    record_count: U32,
}

/// Fixed header preceding one record's key bytes (and, unless the tombstone
/// flag is set, its value bytes) inside a raw-record frame.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct RecordHeader {
    wall_ms: U64,
    logical: U32,
    node: U64,
    expires_at_ms: U64,
    key_len: U32,
    value_len: U32,
    flags: u8,
}

/// Exact per-record fixed overhead the raw-record layout adds ahead of each
/// record's key/value bytes. `store::chunk_records_for_snapshot` uses it for
/// exact chunk-budget arithmetic.
pub(crate) const RECORD_HEADER_LEN: usize = size_of::<RecordHeader>();

/// Exact byte length a lone `Msg::Replicate { cache, rec }` with the given
/// cache-name/key/value lengths encodes to. Lets `Shard::insert`/`insert_many`
/// enforce `max_frame` from lengths already in hand, without a real encode.
#[must_use]
pub(crate) fn replicate_frame_len(cache_len: usize, key_len: usize, value_len: usize) -> usize {
    1 + size_of::<RawFrameHeader>() + cache_len + RECORD_HEADER_LEN + key_len + value_len
}

/// The widest postcard encoding of one [`Cell`]: each of its six integer
/// fields at its maximum varint width.
pub(crate) const CELL_MAX_WIRE_LEN: usize = 5 + 10 + 10 + 5 + 10 + 10;

/// An upper bound on the byte length a [`Msg::AeSketch`] with `cells` cells
/// and a `cache_len`-byte cache name encodes to: the frame discriminant, the
/// variant tag, the name and cell-count lengths and the bucket at their
/// widest, then every cell at its widest. `Cluster::build` rejects an
/// `ae_sketch_cells` this puts past `max_frame`.
#[must_use]
pub(crate) fn ae_sketch_frame_max_len(cache_len: usize, cells: usize) -> usize {
    (1 + 2 + 10 + cache_len + 3 + 10).saturating_add(cells.saturating_mul(CELL_MAX_WIRE_LEN))
}

fn check_frame_len(len: usize) -> Result<(), CodecError> {
    if len > MAX_FRAME {
        return Err(CodecError::FrameTooLarge {
            size: len,
            limit: MAX_FRAME,
        });
    }
    Ok(())
}

/// Encodes a message to its wire form: postcard for control messages, the
/// raw-record layout for `Replicate`/`ReplicateBatch`/`StChunk`.
///
/// # Errors
///
/// Returns [`CodecError::FrameTooLarge`] if the frame would exceed
/// [`MAX_FRAME`], [`CodecError::Postcard`] if a control message fails to
/// serialize, or [`CodecError::MalformedFrame`] if a raw-record frame's
/// cache name or record count overflows the layout's fixed-width fields.
pub fn encode(msg: &Msg) -> Result<Bytes, CodecError> {
    match msg {
        Msg::Replicate { cache, rec } => {
            encode_raw_frame(RAW_KIND_REPLICATE, cache, std::slice::from_ref(rec), false)
        }
        Msg::ReplicateBatch { cache, recs } => {
            encode_raw_frame(RAW_KIND_REPLICATE_BATCH, cache, recs, false)
        }
        Msg::StChunk { cache, recs, done } => {
            encode_raw_frame(RAW_KIND_ST_CHUNK, cache, recs, *done)
        }
        _ => encode_postcard(msg),
    }
}

fn encode_postcard(msg: &Msg) -> Result<Bytes, CodecError> {
    let body = postcard::to_stdvec(msg)?;
    let mut buf = BytesMut::with_capacity(1 + body.len());
    buf.put_u8(FRAME_KIND_POSTCARD);
    buf.extend_from_slice(&body);
    check_frame_len(buf.len())?;
    Ok(buf.freeze())
}

fn encode_raw_frame(
    kind: u8,
    cache: &SmolStr,
    recs: &[WireRecord],
    done: bool,
) -> Result<Bytes, CodecError> {
    let cache_bytes = cache.as_bytes();
    let cache_len = u16::try_from(cache_bytes.len())
        .map_err(|_| CodecError::MalformedFrame("cache name exceeds 64 KiB"))?;
    let record_count = u32::try_from(recs.len())
        .map_err(|_| CodecError::MalformedFrame("record count exceeds u32::MAX"))?;

    let total = recs.iter().fold(
        1 + size_of::<RawFrameHeader>() + cache_bytes.len(),
        |acc, rec| {
            acc + RECORD_HEADER_LEN + rec.key.len() + rec.value.as_ref().map_or(0, Bytes::len)
        },
    );

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u8(FRAME_KIND_RAW_RECORD);
    buf.extend_from_slice(
        RawFrameHeader {
            msg_kind: kind,
            done: u8::from(done),
            cache_len: U16::new(cache_len),
            record_count: U32::new(record_count),
        }
        .as_bytes(),
    );
    buf.extend_from_slice(cache_bytes);

    for rec in recs {
        let key_len = u32::try_from(rec.key.len())
            .map_err(|_| CodecError::MalformedFrame("record key exceeds u32::MAX"))?;
        let (value_len, tombstone_flag) = match &rec.value {
            Some(value) => (
                u32::try_from(value.len())
                    .map_err(|_| CodecError::MalformedFrame("record value exceeds u32::MAX"))?,
                0,
            ),
            None => (0, RECORD_FLAG_TOMBSTONE),
        };
        let (expires_at_ms, expiry_flag) = match rec.expires_at_ms {
            Some(ms) => (ms, RECORD_FLAG_HAS_EXPIRY),
            None => (0, 0),
        };
        buf.extend_from_slice(
            RecordHeader {
                wall_ms: U64::new(rec.ver.wall_ms),
                logical: U32::new(rec.ver.logical),
                node: U64::new(rec.ver.node.as_u64()),
                expires_at_ms: U64::new(expires_at_ms),
                key_len: U32::new(key_len),
                value_len: U32::new(value_len),
                flags: tombstone_flag | expiry_flag,
            }
            .as_bytes(),
        );
        buf.extend_from_slice(&rec.key);
        if let Some(value) = &rec.value {
            buf.extend_from_slice(value);
        }
    }

    check_frame_len(buf.len())?;
    Ok(buf.freeze())
}

/// Takes `len` bytes at `offset` out of `body` as a zero-copy `Bytes` slice,
/// or a [`CodecError::MalformedFrame`] if that range runs past `body`'s end
/// rather than panicking. `body` is peer-sent data, never trusted well-formed.
fn take(body: &Bytes, offset: usize, len: usize) -> Result<Bytes, CodecError> {
    let end = offset.checked_add(len).ok_or(CodecError::MalformedFrame(
        "record length overflows a frame offset",
    ))?;
    if end > body.len() {
        return Err(CodecError::MalformedFrame("raw-record frame truncated"));
    }
    Ok(body.slice(offset..end))
}

/// Caps how many records [`decode_raw_frame`] preallocates its output `Vec`
/// for, independent of the frame's claimed `record_count`. A short, corrupt
/// frame claiming billions of records must not trigger a huge allocation.
const RECORD_COUNT_PREALLOC_CAP: usize = 4096;

fn decode_raw_frame(body: &Bytes) -> Result<Msg, CodecError> {
    let (header, _) = RawFrameHeader::read_from_prefix(body.as_ref())
        .map_err(|_| CodecError::MalformedFrame("raw frame header truncated"))?;
    let mut offset = size_of::<RawFrameHeader>();

    let cache_len = usize::from(header.cache_len.get());
    let cache_bytes = take(body, offset, cache_len)?;
    offset += cache_len;
    let cache = SmolStr::new(
        std::str::from_utf8(&cache_bytes)
            .map_err(|_| CodecError::MalformedFrame("cache name is not valid utf-8"))?,
    );

    let record_count = usize::try_from(header.record_count.get()).unwrap_or(usize::MAX);
    let mut recs = Vec::with_capacity(record_count.min(RECORD_COUNT_PREALLOC_CAP));
    for _ in 0..record_count {
        let (rh, _) = RecordHeader::read_from_prefix(&body.as_ref()[offset.min(body.len())..])
            .map_err(|_| CodecError::MalformedFrame("record header truncated"))?;
        offset += size_of::<RecordHeader>();

        let key_len = usize::try_from(rh.key_len.get()).unwrap_or(usize::MAX);
        let key = take(body, offset, key_len)?;
        offset += key_len;

        let value = if rh.flags & RECORD_FLAG_TOMBSTONE != 0 {
            None
        } else {
            let value_len = usize::try_from(rh.value_len.get()).unwrap_or(usize::MAX);
            let value = take(body, offset, value_len)?;
            offset += value_len;
            Some(value)
        };

        let expires_at_ms =
            (rh.flags & RECORD_FLAG_HAS_EXPIRY != 0).then_some(rh.expires_at_ms.get());
        let ver = Hlc {
            wall_ms: rh.wall_ms.get(),
            logical: rh.logical.get(),
            node: NodeId::from(rh.node.get()),
        };
        recs.push(WireRecord {
            key,
            value,
            ver,
            expires_at_ms,
        });
    }

    match header.msg_kind {
        RAW_KIND_REPLICATE => {
            let mut recs = recs.into_iter();
            let rec = recs.next().ok_or(CodecError::MalformedFrame(
                "Replicate frame carries no record",
            ))?;
            if recs.next().is_some() {
                return Err(CodecError::MalformedFrame(
                    "Replicate frame carries more than one record",
                ));
            }
            Ok(Msg::Replicate { cache, rec })
        }
        RAW_KIND_REPLICATE_BATCH => Ok(Msg::ReplicateBatch { cache, recs }),
        RAW_KIND_ST_CHUNK => Ok(Msg::StChunk {
            cache,
            recs,
            done: header.done != 0,
        }),
        _ => Err(CodecError::MalformedFrame(
            "unknown raw-record message kind",
        )),
    }
}

/// Decodes a message from its wire form. `frame` need only be borrowed: a
/// raw-record frame's key/value bytes are sliced out as zero-copy `Bytes`
/// views sharing `frame`'s backing allocation.
///
/// # Errors
///
/// Returns [`CodecError::FrameTooLarge`] if `frame` exceeds [`MAX_FRAME`],
/// [`CodecError::Postcard`] if a control message's bytes aren't a valid
/// [`Msg`] encoding, or [`CodecError::MalformedFrame`] if a raw-record
/// frame's header, cache name, or record lengths don't fit the bytes present.
pub fn decode(frame: &Bytes) -> Result<Msg, CodecError> {
    check_frame_len(frame.len())?;
    let Some(&kind) = frame.first() else {
        return Err(CodecError::MalformedFrame("empty frame"));
    };
    let body = frame.slice(1..);
    match kind {
        FRAME_KIND_POSTCARD => match postcard::from_bytes(&body) {
            Ok(msg) => Ok(msg),
            Err(err) => decode_legacy_hello(&body).ok_or_else(|| CodecError::from(err)),
        },
        FRAME_KIND_RAW_RECORD => decode_raw_frame(&body),
        _ => Err(CodecError::MalformedFrame("unknown frame discriminant")),
    }
}

/// [`Msg::Hello`] as protocol 1 encodes it: the same variant index, no
/// `protocol` field. Only shape a protocol-2 decode can fail on that still
/// means something, so [`decode`] falls back to it.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
enum LegacyMsg {
    Hello { node: NodeId, incarnation: u64 },
}

fn decode_legacy_hello(body: &[u8]) -> Option<Msg> {
    let LegacyMsg::Hello { node, incarnation } = postcard::from_bytes(body).ok()?;
    Some(Msg::Hello {
        node,
        incarnation,
        protocol: 1,
    })
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
            protocol: PROTOCOL_VERSION,
        });
    }

    #[test]
    fn a_hello_without_a_protocol_field_decodes_as_protocol_one() {
        let legacy = LegacyMsg::Hello {
            node: NodeId::from(1),
            incarnation: 3,
        };
        let mut frame = vec![FRAME_KIND_POSTCARD];
        frame.extend(postcard::to_stdvec(&legacy).expect("encodes"));
        assert_eq!(
            decode(&Bytes::from(frame)).expect("a 0.3 hello decodes"),
            Msg::Hello {
                node: NodeId::from(1),
                incarnation: 3,
                protocol: 1,
            }
        );
    }

    #[test]
    fn a_hello_with_trailing_fields_from_a_newer_protocol_decodes() {
        let mut frame = encode(&Msg::Hello {
            node: NodeId::from(1),
            incarnation: 3,
            protocol: PROTOCOL_VERSION + 1,
        })
        .expect("encodes")
        .to_vec();
        frame.extend_from_slice(&[0x07, 0x09, 0x11]);
        assert_eq!(
            decode(&Bytes::from(frame)).expect("trailing fields are ignored"),
            Msg::Hello {
                node: NodeId::from(1),
                incarnation: 3,
                protocol: PROTOCOL_VERSION + 1,
            }
        );
    }

    #[test]
    fn a_truncated_non_hello_frame_still_fails_to_decode() {
        let frame = encode(&Msg::ReqDone).expect("encodes");
        let mut body = frame.to_vec();
        body.push(0xFF);
        body.push(0xFF);
        // A bare variant index past the enum's range is neither a message
        // nor a legacy hello.
        let bogus = Bytes::from(vec![FRAME_KIND_POSTCARD, 0x7F]);
        assert!(decode(&bogus).is_err());
        assert!(
            decode(&Bytes::from(body)).is_ok(),
            "trailing bytes after ReqDone are ignored"
        );
    }

    #[test]
    fn peer_supports_compares_against_the_introducing_protocol() {
        assert!(peer_supports(2, PROTOCOL_PART_DIGESTS));
        assert!(peer_supports(3, PROTOCOL_ST_UNAVAILABLE));
        assert!(!peer_supports(1, PROTOCOL_PART_DIGESTS));
        assert!(!peer_supports(1, PROTOCOL_ST_UNAVAILABLE));
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
    fn roundtrip_replicate_no_expiry() {
        roundtrip(&Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: WireRecord {
                expires_at_ms: None,
                ..sample_record(Some("v1"))
            },
        });
    }

    /// `u64::MAX` must round-trip as a real expiry, not `None`, since
    /// `RECORD_FLAG_HAS_EXPIRY` is an explicit flag, not a sentinel value.
    #[test]
    fn roundtrip_replicate_expiry_at_u64_max() {
        roundtrip(&Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: WireRecord {
                expires_at_ms: Some(u64::MAX),
                ..sample_record(Some("v1"))
            },
        });
    }

    #[test]
    fn roundtrip_replicate_empty_key_and_value() {
        roundtrip(&Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: WireRecord {
                key: Bytes::new(),
                value: Some(Bytes::new()),
                ver: sample_hlc(),
                expires_at_ms: None,
            },
        });
    }

    #[test]
    fn roundtrip_replicate_empty_cache_name() {
        roundtrip(&Msg::Replicate {
            cache: SmolStr::new(""),
            rec: sample_record(Some("v1")),
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
    fn roundtrip_st_chunk_empty() {
        roundtrip(&Msg::StChunk {
            cache: SmolStr::new("users"),
            recs: Vec::new(),
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

    /// `Cell`'s fields are private; this builds one through `Iblt`'s
    /// `pub(crate)` API instead of a struct literal.
    fn sample_cells() -> Vec<Cell> {
        let mut iblt = crate::cluster::sketch::Iblt::new(6);
        iblt.insert(1, sample_hlc());
        iblt.into_cells()
    }

    #[test]
    fn roundtrip_ae_sketch() {
        roundtrip(&Msg::AeSketch {
            cache: SmolStr::new("users"),
            bucket: 42,
            cells: sample_cells(),
        });
    }

    #[test]
    fn roundtrip_ae_sketch_empty_cells() {
        roundtrip(&Msg::AeSketch {
            cache: SmolStr::new("users"),
            bucket: 0,
            cells: Vec::new(),
        });
    }

    #[test]
    fn roundtrip_ae_entries() {
        roundtrip(&Msg::AeEntries {
            cache: SmolStr::new("users"),
            buckets: vec![0, 512, 1023],
        });
    }

    #[test]
    fn roundtrip_ae_pull_hashes() {
        roundtrip(&Msg::AePullHashes {
            cache: SmolStr::new("users"),
            bucket: 7,
            hashes: vec![1, 2, u64::MAX],
        });
    }

    #[test]
    fn roundtrip_ae_part_digests() {
        roundtrip(&Msg::AePartDigests {
            cache: SmolStr::new("users"),
            bucket: 42,
            digests: (0..64u64).collect(),
        });
    }

    #[test]
    fn roundtrip_ae_parts() {
        roundtrip(&Msg::AeParts {
            cache: SmolStr::new("users"),
            parts: vec![(0, 0), (42, 7), (1023, 63)],
        });
    }

    #[test]
    fn roundtrip_ae_part() {
        roundtrip(&Msg::AePart {
            cache: SmolStr::new("users"),
            bucket: 42,
            part: 7,
            entries: vec![(Bytes::from_static(b"k1"), sample_hlc())],
        });
    }

    #[test]
    fn roundtrip_ae_part_sketch() {
        roundtrip(&Msg::AePartSketch {
            cache: SmolStr::new("users"),
            bucket: 42,
            part: 7,
            cells: sample_cells(),
        });
    }

    #[test]
    fn roundtrip_st_unavailable() {
        roundtrip(&Msg::StUnavailable {
            cache: SmolStr::new("users"),
        });
    }

    #[test]
    fn roundtrip_ae_part_sketch_empty_cells() {
        roundtrip(&Msg::AePartSketch {
            cache: SmolStr::new("users"),
            bucket: 0,
            part: 0,
            cells: Vec::new(),
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
    fn roundtrip_req_done() {
        roundtrip(&Msg::ReqDone);
    }

    #[test]
    fn oversized_frame_is_rejected_on_decode() {
        let oversized = Bytes::from(vec![0u8; MAX_FRAME + 1]);
        assert!(matches!(
            decode(&oversized),
            Err(CodecError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn empty_frame_is_rejected_on_decode() {
        assert!(matches!(
            decode(&Bytes::new()),
            Err(CodecError::MalformedFrame(_))
        ));
    }

    #[test]
    fn unknown_frame_discriminant_is_rejected() {
        assert!(matches!(
            decode(&Bytes::from_static(&[0xff])),
            Err(CodecError::MalformedFrame(_))
        ));
    }

    #[test]
    fn truncated_raw_record_frame_is_rejected_not_panicking() {
        let full = encode(&Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: sample_record(Some("v1")),
        })
        .expect("encodes");
        for cut in 1..full.len() {
            let truncated = full.slice(0..cut);
            assert!(
                matches!(decode(&truncated), Err(CodecError::MalformedFrame(_))),
                "truncating a raw-record frame to {cut} bytes must error, never panic"
            );
        }
    }

    /// A [`Msg::Replicate`] frame's raw-record payload decodes a key/value
    /// `Bytes` sharing the frame's backing storage at its exact expected
    /// offset, never copying it.
    #[test]
    fn decoded_record_bytes_are_zero_copy_slices_of_the_frame() {
        let cache = SmolStr::new("users");
        let rec = sample_record(Some("v1"));
        let key_len = rec.key.len();
        let frame = encode(&Msg::Replicate {
            cache: cache.clone(),
            rec,
        })
        .expect("encodes");
        let key_offset = 1 + size_of::<RawFrameHeader>() + cache.len() + RECORD_HEADER_LEN;
        let expected_key_ptr = frame.as_ptr().wrapping_add(key_offset);
        let expected_value_ptr = frame.as_ptr().wrapping_add(key_offset + key_len);

        let Msg::Replicate { rec, .. } = decode(&frame).expect("decodes") else {
            panic!("decoded back to something other than Replicate");
        };
        assert_eq!(
            rec.key.as_ptr(),
            expected_key_ptr,
            "decoded key must be a zero-copy slice into the received frame"
        );
        assert_eq!(
            rec.value.expect("value present").as_ptr(),
            expected_value_ptr,
            "decoded value must be a zero-copy slice into the received frame"
        );
    }

    #[test]
    fn ae_sketch_frame_max_len_bounds_a_real_encode_from_above_and_tightly() {
        // Every cell XOR-accumulates well-spread elements, so each field
        // encodes as a varint of a random full-width integer.
        let spread = |i: u64| i.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut iblt = crate::cluster::sketch::Iblt::new(240);
        for i in 0..2_000u64 {
            iblt.insert(
                spread(i),
                Hlc {
                    wall_ms: spread(i ^ 0x55),
                    logical: u32::try_from(spread(i ^ 0xAA) >> 32).expect("fits"),
                    node: NodeId::from(spread(i ^ 0xFF)),
                },
            );
        }
        let cache = SmolStr::new("users");
        let actual = encode(&Msg::AeSketch {
            cache: cache.clone(),
            bucket: u16::MAX,
            cells: iblt.into_cells(),
        })
        .expect("encodes")
        .len();
        let bound = ae_sketch_frame_max_len(cache.len(), 240);
        assert!(actual <= bound, "{actual} bytes on the wire, bound {bound}");
        assert!(
            actual * 10 >= bound * 8,
            "the bound stays within a quarter of the real size: {actual} vs {bound}"
        );
        assert_eq!(ae_sketch_frame_max_len(255, usize::MAX), usize::MAX);
    }

    #[test]
    fn replicate_frame_len_matches_a_real_encode() {
        let rec = sample_record(Some("v1"));
        let cache = SmolStr::new("users");
        let predicted = replicate_frame_len(
            cache.len(),
            rec.key.len(),
            rec.value.as_ref().map_or(0, Bytes::len),
        );
        let actual = encode(&Msg::Replicate { cache, rec })
            .expect("encodes")
            .len();
        assert_eq!(predicted, actual);
    }
}

#[cfg(test)]
mod prop_tests;
