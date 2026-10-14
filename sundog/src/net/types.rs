//! The data plane's message and outcome types.

#[cfg(doc)]
use super::{Mesh, RequestHandler};
#[cfg(doc)]
use crate::wire;

use bytes::Bytes;
use futures::stream::BoxStream;

use crate::error::CodecError;
use crate::node::NodeId;
use crate::store::KeyVersion;
use crate::wire::{Cell, Msg, WireRecord};

/// Backpressure class for a fan-out message, selecting the per-class drop
/// policy on outbox overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MsgClass {
    /// Overflow drops the oldest queued invalidation, or merged record of an
    /// `Invalidation` cache: a storm on a dead peer must never stall writers,
    /// and a newer write supersedes an older one. The drop marks the peer
    /// dirty and counts in `sundog_backlog_dropped_total`.
    Invalidate,
    /// Overflow drops the new message and marks the peer dirty so the next
    /// anti-entropy round targets it first. A frame whose send fails stays
    /// with the peer's writer and goes out on its next connection; a
    /// connection that carried frames and broke marks the peer dirty once
    /// the writer has redialed.
    Replicate,
}

/// One inbound message, tagged with the peer it arrived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMsg {
    /// The peer that sent `msg`.
    pub from: NodeId,
    pub msg: Msg,
}

/// One anti-entropy digest-exchange reply, as [`Mesh::ae_round`] collects
/// them: a mismatched bucket's full listing, or, once too large for that,
/// an IBLT sketch for the initiator to peel against its own, or its part
/// digests. `#[non_exhaustive]`, like [`crate::wire::Msg`]: a new reply
/// shape is not a breaking release.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AeMismatch {
    /// `(bucket, entries)`: the bucket's full listing.
    Bucket(u16, Vec<KeyVersion>),
    /// `(bucket, cells)`: an IBLT sketch over the bucket.
    Sketch(u16, Vec<Cell>),
    /// `(bucket, digests)`: the bucket's 64 part digests, sent instead of a
    /// listing or sketch once the bucket's entry count passed
    /// `ClusterConfig::ae_part_min_bucket`. The initiator compares these
    /// against its own part digests and requests only the mismatched parts
    /// via [`Mesh::ae_parts`].
    PartDigests(u16, Vec<u64>),
}

impl AeMismatch {
    /// The bucket this reply covers, regardless of which shape it took.
    #[must_use]
    pub const fn bucket(&self) -> u16 {
        match self {
            Self::Bucket(bucket, _) | Self::Sketch(bucket, _) | Self::PartDigests(bucket, _) => {
                *bucket
            }
        }
    }
}

/// One reply to [`Mesh::ae_parts`]: a mismatched part's full listing, or,
/// once too large for that, an IBLT sketch, the part-grained counterpart of
/// [`AeMismatch`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AePartReply {
    /// The part's full `(key, version)` listing.
    Listing {
        bucket: u16,
        part: u8,
        entries: Vec<KeyVersion>,
    },
    /// An IBLT sketch over the part.
    Sketch {
        bucket: u16,
        part: u8,
        cells: Vec<Cell>,
    },
}

impl AePartReply {
    /// The bucket this reply's part belongs to, whichever shape it took.
    #[must_use]
    pub const fn bucket(&self) -> u16 {
        match self {
            Self::Listing { bucket, .. } | Self::Sketch { bucket, .. } => *bucket,
        }
    }

    /// The part within [`AePartReply::bucket`] this reply covers.
    #[must_use]
    pub const fn part(&self) -> u8 {
        match self {
            Self::Listing { part, .. } | Self::Sketch { part, .. } => *part,
        }
    }
}

/// One reply to [`Mesh::fetch`]: the responder's answer to a
/// distribution-mode read for a key it may or may not currently own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FetchOutcome {
    /// The responder answered: `Some` record, or `None` for a definitive
    /// miss.
    Found(Option<WireRecord>),
    /// The responder holds no record in a part it trusts and its own view
    /// hash differs from the request's; it declined rather than risk a
    /// miss against a stale owner set.
    Stale { responder_view_hash: u64 },
    /// The responder declined without answering: it cannot vouch for the
    /// key (a cache it does not have open as a distribution-mode cache, a
    /// part it distrusts whatever the views say, or a cold part with no
    /// record on an equal view hash). The requester moves on to the next
    /// owner.
    Declined,
}

/// How a donor answered [`Mesh::request_buckets`].
pub(crate) enum BucketPull {
    /// The donor is transferring: its chunk stream.
    Stream(BoxStream<'static, Result<BucketStreamItem, CodecError>>),
    /// The donor's own view hash differs from the request's.
    Stale,
    /// The donor owns a requested bucket but has not pulled it yet, so its
    /// copy is not a source; the requester tries its next donor.
    Cold,
}

/// One item off a [`BucketPull::Stream`]: a data chunk, or, against a donor
/// speaking at least [`wire::PROTOCOL_ST_BUCKET_DONE_ACK`], a signal that
/// the donor released one bucket ahead of the group's trailing `done: true`
/// chunk. An older donor never sends `BucketDone`.
pub(crate) enum BucketStreamItem {
    /// Records for the bucket currently streaming.
    Chunk(Vec<WireRecord>),
    /// The donor released this bucket; triggers the caller's
    /// `on_bucket_done` ahead of the stream's own end.
    BucketDone(u16),
}

/// One reply to [`Mesh::ae_round_scoped`]: the scoped anti-entropy digest
/// exchange's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AeRoundOutcome {
    /// Every mismatched bucket's reply, exactly like [`Mesh::ae_round`]'s.
    Mismatches(Vec<AeMismatch>),
    /// The responder's own view hash differs from the request's; it
    /// declined the round rather than answer against a stale owner set.
    /// The next scheduled round retries with a freshly re-borrowed view.
    Stale { responder_view_hash: u64 },
}

/// A [`RequestHandler`]'s answer to a [`crate::wire::Msg::Fetch`].
pub(crate) enum FetchServe {
    /// `Some` record held in a part the responder owns or is releasing,
    /// whatever the views say, or `None` for a definitive miss on an equal
    /// view hash in a warm part.
    Found(Option<WireRecord>),
    /// The responder holds no record in a part it trusts and its own view
    /// hash differs from the requester's.
    Stale { responder_view_hash: u64 },
    /// The named cache is not open here or not a distribution-mode cache,
    /// the same "unknown cache degrades gracefully" shape every other
    /// method on this trait already has, or the key's part is distrusted
    /// whatever the views say, or cold with no record on an equal view
    /// hash.
    Unavailable,
}

/// A [`RequestHandler`]'s answer to a [`crate::wire::Msg::Load`].
pub(crate) enum LoadServe {
    /// The record stored for each requested key the responder or its
    /// source holds, and each value it loaded without storing.
    Loaded {
        found: Vec<WireRecord>,
        uncached: Vec<(Bytes, Bytes)>,
    },
    /// The responder's loader failed, with the error's message.
    Failed(String),
    /// The responder cannot load for the requester: no loader, the cache
    /// not open, or a requested key's part not warm here.
    Unavailable,
}

/// What a [`Mesh::load`] request came back with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LoadOutcome {
    /// See [`LoadServe::Loaded`].
    Loaded {
        found: Vec<WireRecord>,
        uncached: Vec<(Bytes, Bytes)>,
    },
    /// See [`LoadServe::Failed`].
    Failed(String),
    /// The responder declined; the requester loads elsewhere.
    Declined,
}

/// A [`RequestHandler`]'s answer to a scoped anti-entropy digest
/// exchange, [`crate::wire::Msg::AeDigestScoped`] or
/// [`crate::wire::Msg::AeDigestMasked`]: the responder's digests to
/// compare, view hashes matching, or a decline.
pub(crate) enum AeServeOutcome<T> {
    /// The view hashes match: the responder's digests.
    Digests(T),
    /// The responder's own view hash differs from the requester's.
    Stale { responder_view_hash: u64 },
    /// The named cache is not open here, or not a distribution-mode cache.
    Unavailable,
}

/// A [`RequestHandler`]'s answer to a [`crate::wire::Msg::StBuckets`] pull,
/// decided against one ownership view.
pub(crate) enum StServe {
    /// The responder's own view hash differs from the requester's.
    Stale { responder_view_hash: u64 },
    /// The responder owns a requested id but has not pulled it yet.
    Cold,
    /// The responder donates: each requested id's chunks tagged with the
    /// id, the order the stream finishes ids in, and how many parts they
    /// cover. An id with nothing to send yields no chunk.
    Stream {
        chunks: BoxStream<'static, (u16, Vec<WireRecord>)>,
        order: Vec<u16>,
        parts: u64,
    },
}
