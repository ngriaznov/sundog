//! The wire format: what crosses the data-plane TCP mesh, and the
//! encode/decode helpers every connection uses.
//!
//! Every frame starts with a one-byte discriminant. Control messages
//! (`Hello`, `StRequest`, `AeDigest`, `AeBucket`, `AeSketch`, `AeEntries`,
//! `AePull`, `AePullHashes`, `ReqDone`) carry `FRAME_KIND_POSTCARD` and are
//! postcard-encoded exactly as before. The
//! three record-carrying variants — [`Msg::Replicate`], [`Msg::ReplicateBatch`],
//! [`Msg::StChunk`] — carry `FRAME_KIND_RAW_RECORD` and use a dedicated
//! length-prefixed layout instead: a fixed-size header (read via the
//! `zerocopy` crate's safe [`FromBytes`]/[`IntoBytes`] views, never
//! `unsafe`) followed by each record's key and value bytes back to back.
//! Decoding this layout slices `Bytes` views directly out of the received
//! frame (`Bytes::slice`, an `Arc` refcount bump that shares the frame's
//! backing allocation through its own handle, independent of the frame
//! argument's lifetime) — no payload copy on receive, unlike postcard's
//! decode-into-owned-`Vec<u8>` path. Encoding assembles straight into one
//! exact-reserve `BytesMut` from
//! already-owned `Bytes` (a record's key, and — since `store::Stored::encoded`
//! caches it — its value), so there is no intermediate postcard `Vec` either.
//!
//! **`tls` caveat.** This zero-copy path only holds up to the TCP framing
//! layer. When the `tls` feature wraps a connection in rustls, rustls owns
//! its own internal read/write buffers and copies application bytes through
//! them independently of what `wire::decode`/`encode` do above — the
//! zero-copy property described here is about avoiding *this module's own*
//! copies, not about eliminating every copy in the transport stack.
//!
//! **Memory-retention note.** A [`WireRecord`] decoded off a raw-record
//! frame borrows its `key`/`value` from that frame's backing buffer. If a
//! replica-applied record's value bytes end up cached verbatim as
//! `store::Stored::encoded`, that `Stored` keeps the whole
//! originating frame buffer alive for as long as the entry stays cached —
//! bounded in practice, since a `ReplicateBatch`/`StChunk` frame is itself
//! capped well under [`MAX_FRAME`] (`net::conn::REPLICATE_BATCH_BUDGET`,
//! `store::SNAPSHOT_CHUNK_ENVELOPE_HEADROOM`), not a concern for a lone
//! `Replicate` frame sized to one record.

use std::mem::size_of;

use bytes::{BufMut as _, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::node::NodeId;

/// One cell of the anti-entropy IBLT sketch ([`Msg::AeSketch`]'s payload) —
/// defined in `cluster::sketch` (an internal implementation module) and
/// re-exported here only so a wire message can carry a `Vec<Cell>`; every
/// field stays private to that module, so this type is otherwise opaque
/// outside it — nothing outside `cluster::sketch` constructs, inspects, or
/// matches on one.
pub use crate::cluster::sketch::Cell;

/// Hard cap on any single wire frame, in bytes (4 MiB). Oversized values are
/// rejected at the API boundary (`CacheError::ValueTooLarge`) rather than
/// fragmented across multiple frames.
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

/// Every message exchanged on the data-plane mesh.
///
/// `#[non_exhaustive]`: this crate adds new wire message kinds as its
/// anti-entropy and state-transfer protocols grow (`Msg::AeSketch`,
/// `AeEntries`, and `AePullHashes` are exactly this, added on top of
/// 0.2.0's exhaustive shape) — matching downstream code adds a wildcard arm
/// once, here, rather than needing another breaking release for every
/// future addition.
#[non_exhaustive]
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
    /// Replication-mode fan-out: the full record to apply. Wire-encoded via
    /// the raw-record layout (this module's docs), not postcard.
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
    /// One chunk of a state-transfer snapshot stream. Wire-encoded via the
    /// raw-record layout (this module's docs), not postcard.
    StChunk {
        /// The cache being transferred.
        cache: SmolStr,
        /// A batch of records (~500 per chunk).
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
    /// [`Msg::Replicate`] messages into this on the wire, applied under one
    /// acquisition of the store's apply serialization lock
    /// (`store::ShardOps::apply_remote_batch`). Wire-encoded via the
    /// raw-record layout (this module's docs), not postcard.
    ReplicateBatch {
        /// The target cache's name.
        cache: SmolStr,
        /// The records to apply, in order.
        recs: Vec<WireRecord>,
    },
    /// Marks the end of one reply on a request/response connection kept
    /// open for reuse: sent once after the last `AeBucket`/`Replicate`
    /// reply to an `AeDigest`/`AePull` request, so the requester knows the
    /// reply is complete without relying on the connection closing.
    /// `StRequest`'s reply already has its own per-chunk `done` flag on
    /// `StChunk` and needs no analogous marker. Appended after every
    /// variant that predates it so their postcard encodings (by declaration
    /// index, not just their `as isize` discriminant) stay unchanged.
    ReqDone,
    /// Anti-entropy round, step 2 (large-bucket path): an IBLT sketch
    /// (`cluster::sketch`'s module docs) over a mismatched bucket's
    /// `(key_hash, version)` pairs, sent instead of [`Msg::AeBucket`] once
    /// the bucket's own listing would cost more on the wire than the sketch
    /// (`ClusterConfig::ae_sketch_min_bucket`). The initiator subtracts its
    /// own local sketch of the bucket from this one and peels the result;
    /// on success this replaces the whole `AeBucket` round trip for that
    /// bucket, on failure (`cluster::sketch::Undecodable`) the
    /// initiator falls back to [`Msg::AeEntries`].
    ///
    /// [`ClusterConfig::ae_sketch_min_bucket`]: crate::config::ClusterConfig::ae_sketch_min_bucket
    AeSketch {
        /// The cache being reconciled.
        cache: SmolStr,
        /// Which bucket this sketch covers.
        bucket: u16,
        /// The sketch's cells (`ClusterConfig::ae_sketch_cells` of them, as
        /// built by the sender).
        cells: Vec<Cell>,
    },
    /// Anti-entropy round, step 2 fallback: "send me the full `(key,
    /// version)` listing for these buckets" — the initiator's request after
    /// one or more `AeSketch` replies failed to decode. Answered exactly
    /// like `AeDigest`'s own reply shape: one [`Msg::AeBucket`] per
    /// requested bucket, then [`Msg::ReqDone`].
    AeEntries {
        /// The cache being reconciled.
        cache: SmolStr,
        /// The buckets to list in full.
        buckets: Vec<u16>,
    },
    /// Anti-entropy round, step 3 (sketch-decoded path): "send me your full
    /// records for the entries of `bucket` whose key hash (`xxh3_64` of the
    /// key's wire bytes) is one of `hashes`" — the sketch-decoded
    /// counterpart to [`Msg::AePull`], used when the initiator peeled an
    /// `AeSketch` reply and learned only the key *hashes* it is missing or
    /// holds stale, never the keys themselves. Answered exactly like
    /// `AePull`'s own reply shape: [`Msg::Replicate`] records, then
    /// [`Msg::ReqDone`].
    AePullHashes {
        /// The cache being reconciled.
        cache: SmolStr,
        /// Which bucket the requested hashes were reported in.
        bucket: u16,
        /// Key hashes the requester is missing or holds an older version of.
        hashes: Vec<u64>,
    },
}

/// Frame discriminant: everything after this byte is a postcard-encoded [`Msg`].
const FRAME_KIND_POSTCARD: u8 = 0;
/// Frame discriminant: everything after this byte is the raw-record layout
/// (this module's docs) for [`Msg::Replicate`]/[`Msg::ReplicateBatch`]/[`Msg::StChunk`].
const FRAME_KIND_RAW_RECORD: u8 = 1;

const RAW_KIND_REPLICATE: u8 = 0;
const RAW_KIND_REPLICATE_BATCH: u8 = 1;
const RAW_KIND_ST_CHUNK: u8 = 2;

/// Record-level flag: this record is a tombstone (`WireRecord::value` is `None`).
const RECORD_FLAG_TOMBSTONE: u8 = 0b01;
/// Record-level flag: `expires_at_ms` is `Some` and the header's
/// `expires_at_ms` field holds its value — without this flag the field is
/// meaningless (always written as `0`), so a real expiry of `0` and "no
/// expiry" are never ambiguous the way a sentinel value would make them.
const RECORD_FLAG_HAS_EXPIRY: u8 = 0b10;

/// Fixed header preceding a raw-record frame's payload: which of the three
/// record-carrying [`Msg`] variants this is, the state-transfer `done` flag
/// (meaningless — always `0` — outside [`Msg::StChunk`]), the cache name's
/// byte length, and how many [`RecordHeader`]-prefixed records follow. Every
/// field is a byte-order-explicit, alignment-1 `zerocopy` integer, so this
/// struct has no implicit padding and can sit at any offset in a frame.
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
/// record's key/value bytes — used by `store::chunk_records_for_snapshot`
/// for exact (not approximated) chunk-budget arithmetic, since this layout,
/// unlike postcard's variable-length framing, has no size variance to
/// approximate.
pub(crate) const RECORD_HEADER_LEN: usize = size_of::<RecordHeader>();

/// Exact byte length a lone `Msg::Replicate { cache, rec }` carrying one
/// record with the given cache-name/key/value lengths encodes to under the
/// raw-record layout — lets `Shard::insert`/`insert_many` enforce
/// `max_frame` from lengths already in hand instead of paying for a
/// real encode just to measure it.
#[must_use]
pub(crate) fn replicate_frame_len(cache_len: usize, key_len: usize, value_len: usize) -> usize {
    1 + size_of::<RawFrameHeader>() + cache_len + RECORD_HEADER_LEN + key_len + value_len
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
/// raw-record layout for `Replicate`/`ReplicateBatch`/`StChunk` (this
/// module's docs).
///
/// # Errors
///
/// Returns [`CodecError::FrameTooLarge`] if the encoded frame would exceed
/// [`MAX_FRAME`], [`CodecError::Postcard`] if a control message fails to
/// postcard-serialize (unexpected for these types, but fallible in
/// general), or [`CodecError::MalformedFrame`] if a raw-record frame's cache
/// name or record count doesn't fit the layout's fixed-width fields (a
/// cache name over 64 KiB, or a batch of billions of records — never hit at
/// this crate's target scale of clusters and cache sizes).
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

/// Takes `len` bytes at `offset` out of `body` as a zero-copy `Bytes` slice
/// (an `Arc` refcount bump, no payload copy), or a [`CodecError::MalformedFrame`]
/// if that range runs past the end of `body` rather than panicking — `body`
/// is data a peer sent, never trusted to be well-formed.
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
/// for, independent of the frame's own claimed `record_count` — a short,
/// corrupt frame claiming billions of records must not itself trigger a
/// huge allocation before the length checks below ever run.
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

/// Decodes a message from its wire form. `frame` need only be borrowed — a
/// raw-record frame's key/value bytes are sliced out as zero-copy `Bytes`
/// views (this module's docs) via `Bytes::slice`, which shares the backing
/// allocation through its own independent `Arc` handle rather than borrowing
/// from `frame`'s lifetime, so the returned [`Msg`] outlives this call fine.
///
/// # Errors
///
/// Returns [`CodecError::FrameTooLarge`] if `frame` exceeds [`MAX_FRAME`],
/// [`CodecError::Postcard`] if a control message's bytes aren't a valid
/// [`Msg`] encoding, or [`CodecError::MalformedFrame`] if a raw-record
/// frame's header, cache name, or record lengths don't fit the bytes
/// actually present.
pub fn decode(frame: &Bytes) -> Result<Msg, CodecError> {
    check_frame_len(frame.len())?;
    let Some(&kind) = frame.first() else {
        return Err(CodecError::MalformedFrame("empty frame"));
    };
    let body = frame.slice(1..);
    match kind {
        FRAME_KIND_POSTCARD => Ok(postcard::from_bytes(&body)?),
        FRAME_KIND_RAW_RECORD => decode_raw_frame(&body),
        _ => Err(CodecError::MalformedFrame("unknown frame discriminant")),
    }
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
    fn roundtrip_replicate_no_expiry() {
        roundtrip(&Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: WireRecord {
                expires_at_ms: None,
                ..sample_record(Some("v1"))
            },
        });
    }

    /// `u64::MAX` would collide with a sentinel-based "no expiry" encoding —
    /// this crate uses an explicit flag bit instead (`RECORD_FLAG_HAS_EXPIRY`),
    /// so this must round-trip as a real expiry, not as `None`.
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

    /// `Cell`'s fields are private to `cluster::sketch`; a wire test builds
    /// one the same way any real sender would, through `Iblt`'s own
    /// pub(crate) API, rather than a struct literal it has no access to.
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

    /// A [`Msg::Replicate`] frame's raw-record payload must decode a
    /// key/value `Bytes` that shares the received frame's backing storage at
    /// its exact expected offset, rather than copying it — the zero-copy
    /// property this module's docs describe.
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
