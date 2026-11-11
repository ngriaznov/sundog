//! The wire format of the data-plane mesh and its encode/decode helpers.
//!
//! Every frame starts with a one-byte discriminant. Control messages carry
//! `FRAME_KIND_POSTCARD` and are postcard-encoded. The record-carrying
//! variants [`Msg::Replicate`], [`Msg::ReplicateBatch`], [`Msg::StChunk`],
//! [`Msg::StBucketChunk`], and [`Msg::ForwardBatch`] carry
//! `FRAME_KIND_RAW_RECORD`: a fixed header read through `zerocopy`'s
//! [`FromBytes`] and [`IntoBytes`] views, then each record's key and value
//! bytes back to back. A [`Msg::ForwardBatch`] frame carries one more fixed
//! header between the cache name and the records: the sender's view hash
//! and the batch's hop count.
//!
//! Decoding a raw-record frame slices `Bytes` views out of the received
//! buffer with no payload copy. Encoding assembles one exact-size `BytesMut`
//! from bytes the caller already owns, including a resident entry's cached
//! encoded value (`engine::Live::payload`'s `Resident::encoded`).
//!
//! Under feature `tls`, rustls copies application bytes through its own
//! buffers; the zero-copy path holds only up to the TCP framing layer.
//!
//! A [`WireRecord`] decoded from a raw-record frame borrows from that frame's
//! buffer. A replica that caches the value as its resident entry's encoded
//! bytes keeps the whole frame alive while the entry lives. A frame is
//! capped under [`MAX_FRAME`], so the retention is bounded.

use std::mem::size_of;

use bytes::{BufMut as _, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::node::NodeId;

/// One cell of the anti-entropy IBLT sketch, defined in `sketch`.
pub use crate::sketch::Cell;

/// Hard cap on any single wire frame: 4 MiB. Oversized values are rejected
/// at the API boundary (`CacheError::ValueTooLarge`) instead.
pub const MAX_FRAME: usize = 4 * 1024 * 1024;

/// Byte budget for one coalesced [`Msg::ReplicateBatch`] frame, well under
/// [`MAX_FRAME`]. The connection writer coalesces by it, and the fan-out
/// layer (`cluster::fan_out_batch`) pre-batches drained write bursts by the
/// same rules.
pub(crate) const REPLICATE_BATCH_BUDGET: usize = 256 * 1024;

/// Count cap alongside [`REPLICATE_BATCH_BUDGET`], so a long run of tiny
/// records doesn't grow one batch frame without bound.
pub(crate) const REPLICATE_BATCH_COUNT: usize = 4096;

/// The wire protocol this build speaks, carried in every [`Msg::Hello`] and
/// gossiped with membership. A node answers a peer only with what that
/// peer's protocol understands, so a cluster mixing this version with the
/// one before it keeps replicating and repairing through a rolling upgrade.
///
/// - 1: the 0.3 releases. [`Msg::Hello`] has no `protocol` field.
/// - 2: 0.4. Adds [`Msg::AePartDigests`], [`Msg::AeParts`],
///   [`Msg::AePart`], [`Msg::AePartSketch`], and [`Msg::StUnavailable`].
/// - 3: 0.6. Adds distribution mode's [`Msg::Fetch`], [`Msg::FetchReply`],
///   [`Msg::FetchDeclined`], [`Msg::AeDigestScoped`], [`Msg::StBuckets`],
///   [`Msg::StBucketChunk`], [`Msg::ForwardBatch`], and [`Msg::StaleView`].
/// - 4: Adds [`Msg::StBucketDone`] and [`Msg::StBucketAck`].
/// - 5: Part-level ownership for `Mode::Distributed`. Adds
///   [`Msg::AeDigestMasked`]. See [`PROTOCOL_PART_OWNERSHIP`].
/// - 6: [`Msg::Hello`] names the sender's cluster. See
///   [`PROTOCOL_CLUSTER_ID`].
/// - 7: Loading through another node's loader. Adds [`Msg::Load`],
///   [`Msg::Loaded`], [`Msg::LoadFailed`], [`Msg::LoadDeclined`] and
///   [`Msg::RefreshHint`]. See [`PROTOCOL_LOAD`].
/// - 8: A merging `Mode::Invalidation` cache sends [`Msg::Replicate`] and
///   [`Msg::ReplicateBatch`] records in place of [`Msg::Invalidate`]. See
///   [`PROTOCOL_INVALIDATION_FOLD`].
/// - 9: A reply to [`Msg::AeEntries`] may carry one bucket's listing across
///   several [`Msg::AeBucket`] messages. See [`PROTOCOL_AE_LISTING_CHUNKS`].
pub const PROTOCOL_VERSION: u16 = 9;

/// The oldest peer protocol this build still serves in full.
pub const MIN_PROTOCOL_VERSION: u16 = 1;

/// The protocol that introduced the part-digest anti-entropy replies; a
/// responder sends none of them to an older peer.
pub const PROTOCOL_PART_DIGESTS: u16 = 2;

/// The protocol that introduced [`Msg::StUnavailable`]; a cold donor serves
/// an older peer rather than declining it.
pub const PROTOCOL_ST_UNAVAILABLE: u16 = 2;

/// The protocol that introduced distribution mode's wire messages:
/// [`Msg::Fetch`], [`Msg::FetchReply`], [`Msg::FetchDeclined`],
/// [`Msg::AeDigestScoped`], [`Msg::StBuckets`], [`Msg::StBucketChunk`],
/// [`Msg::ForwardBatch`], and
/// [`Msg::StaleView`]. A
/// peer speaking less never receives one of these: such a peer never becomes
/// eligible to own a bucket in the first place, but the wire layer gates it
/// regardless, as the hard guarantee.
pub const PROTOCOL_DISTRIBUTED: u16 = 3;

/// The protocol that introduced [`Msg::StBucketDone`] and
/// [`Msg::StBucketAck`]: a donor gates sending the first on the requester's
/// advertised protocol, and a receiver gates sending the second on the
/// donor's. A peer speaking less never sees either message; the state
/// transfer that bucket is part of falls back to today's whole-group
/// release timing for that pull, exactly as protocol 3 behaves now.
pub const PROTOCOL_ST_BUCKET_DONE_ACK: u16 = 4;

/// The protocol that introduced part-level ownership for
/// `Mode::Distributed`. A node computes a part-granular view only when
/// every eligible peer speaks it; under it, the `u16` ids in
/// [`Msg::StBuckets`] and its replies name parts instead of buckets,
/// anti-entropy sends [`Msg::AeDigestMasked`] instead of
/// [`Msg::AeDigestScoped`], and both sides agree on the view's hash before
/// reading an id.
pub const PROTOCOL_PART_OWNERSHIP: u16 = 5;

/// The protocol whose [`Msg::Hello`] carries `cluster`, the sender's
/// [`cluster_id`]. An accepting node drops a connection whose `Hello` names
/// a different cluster, so a peer still dialing an address another
/// cluster's node now holds never writes into it. A `Hello` from an older
/// peer decodes with `cluster` [`UNNAMED_CLUSTER`] and is accepted.
pub const PROTOCOL_CLUSTER_ID: u16 = 6;

/// The protocol that introduced loading through another node's loader:
/// [`Msg::Load`] and its replies [`Msg::Loaded`], [`Msg::LoadFailed`] and
/// [`Msg::LoadDeclined`], and the one-way [`Msg::RefreshHint`]. A node
/// picks a peer to load or refresh a key only among peers that speak it,
/// and the wire layer refuses to send either to an older peer regardless.
pub const PROTOCOL_LOAD: u16 = 7;

/// The protocol under which a `Mode::Invalidation` cache whose resolver
/// merges ships its live records as [`Msg::Replicate`] and
/// [`Msg::ReplicateBatch`] instead of [`Msg::Invalidate`]. A receiving
/// `Invalidation` cache folds such a record into the key it holds and
/// ignores it for a key it does not. A sender gives a peer on an older
/// protocol [`Msg::Invalidate`] for every write, since that peer would store
/// the record as an ordinary replicated entry. A merged record uses the
/// existing replicate frames.
pub const PROTOCOL_INVALIDATION_FOLD: u16 = 8;

/// The protocol under which a responder may answer one bucket of an
/// [`Msg::AeEntries`] request with several [`Msg::AeBucket`] messages, each
/// holding a slice of the bucket's listing, which the requester concatenates
/// in arrival order. A requester joins the messages of a bucket whatever the
/// responder's protocol; a responder sends a peer on an older protocol one
/// message per bucket, since that peer would read each message as the whole
/// bucket.
pub const PROTOCOL_AE_LISTING_CHUNKS: u16 = 9;

/// The `cluster` a [`Msg::Hello`] carries when its sender names none: a
/// peer older than [`PROTOCOL_CLUSTER_ID`], or a bare [`crate::net::Mesh`]
/// spawned outside a cluster. Matches every cluster.
pub const UNNAMED_CLUSTER: u64 = 0;

/// The `cluster` a [`Msg::Hello`] carries for the cluster named `name`:
/// its xxh3 hash, never [`UNNAMED_CLUSTER`].
#[must_use]
pub fn cluster_id(name: &str) -> u64 {
    match xxhash_rust::xxh3::xxh3_64(name.as_bytes()) {
        UNNAMED_CLUSTER => 1,
        id => id,
    }
}

/// Whether a connection whose `Hello` names cluster `peer` may talk to a
/// node of cluster `local`: they match, or either names none.
#[must_use]
pub const fn same_cluster(local: u64, peer: u64) -> bool {
    local == UNNAMED_CLUSTER || peer == UNNAMED_CLUSTER || local == peer
}

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
    /// Absolute expiry in epoch milliseconds, or `None` for no TTL. On a
    /// tombstone, `Some` marks the tombstone of an entry that expired and
    /// holds the expiry, which the receiver counts its retention from; a
    /// delete's tombstone carries `None` and counts its retention from `ver`.
    /// A peer that predates this reading
    /// ignores the field on a tombstone.
    pub expires_at_ms: Option<u64>,
}

impl WireRecord {
    /// Returns `true` if this record is a tombstone.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// This record with its key and value copied into bytes of their own. A
    /// record decoded from a frame shares the frame's buffer, which stays
    /// allocated while any slice of it lives: a record kept past its apply,
    /// in a re-forward, a redirect to a part's owners or the skew hold, is
    /// detached first, so it holds its own bytes and not the whole frame
    /// it came in.
    #[must_use]
    pub(crate) fn detached(&self) -> Self {
        Self {
            key: Bytes::copy_from_slice(&self.key),
            value: self.value.as_deref().map(Bytes::copy_from_slice),
            ver: self.ver,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

/// Every message exchanged on the data-plane mesh. `#[non_exhaustive]`: new
/// kinds are added as the protocols grow, so matching code needs a wildcard
/// arm rather than a breaking release for every addition.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Msg {
    /// Sent once a new connection is established: identifies the sender.
    ///
    /// An acceptor closes a connection whose first frame is longer than
    /// 1,024 bytes, so a `Hello` stays within that size for every release
    /// that has to interoperate with a build that enforces the limit. A
    /// `Hello` is at most 128 bytes. Anything larger goes in a later message
    /// gated on the peer's protocol.
    Hello {
        /// The sender's node id.
        node: NodeId,
        /// The sender's membership incarnation number.
        incarnation: u64,
        /// The sender's [`PROTOCOL_VERSION`]. A protocol-1 sender omits it
        /// and decodes here as protocol 1.
        protocol: u16,
        /// The sender's [`cluster_id`], or [`UNNAMED_CLUSTER`]. Last on
        /// purpose: a sender older than [`PROTOCOL_CLUSTER_ID`] omits it
        /// and decodes here as [`UNNAMED_CLUSTER`], and a newer sender's
        /// further trailing fields are ignored.
        cluster: u64,
    },
    /// Invalidation-mode fan-out: "the entry at `key` changed, drop your
    /// copy." `key` is postcard-encoded; `ver` is the write's version. An
    /// `Invalidation` cache whose resolver merges sends it for a removal,
    /// and for every write to a peer older than
    /// [`PROTOCOL_INVALIDATION_FOLD`]; its other writes travel as
    /// [`Msg::Replicate`] records.
    Invalidate {
        /// The cache's name.
        cache: SmolStr,
        /// The postcard-encoded cache key.
        key: Bytes,
        /// The write's HLC version stamp.
        ver: Hlc,
    },
    /// Replication-mode fan-out: the full record. Raw-record layout, not
    /// postcard. An `Invalidation` cache whose resolver merges sends its
    /// live records this way to a peer speaking
    /// [`PROTOCOL_INVALIDATION_FOLD`], and the peer folds one into a key it
    /// holds and ignores it otherwise.
    Replicate {
        /// The cache's name.
        cache: SmolStr,
        /// The record to apply.
        rec: WireRecord,
    },
    /// Requests a full snapshot stream of a cache, for state transfer on join.
    StRequest {
        /// The cache's name.
        cache: SmolStr,
    },
    /// One chunk (~500 records) of a state-transfer snapshot stream, `done`
    /// set on the final one. Raw-record layout, not postcard.
    StChunk {
        /// The cache's name.
        cache: SmolStr,
        /// The snapshot records in this chunk.
        recs: Vec<WireRecord>,
        /// Whether this is the final chunk.
        done: bool,
    },
    /// Anti-entropy round, step 1: this node's per-bucket digest array, as
    /// `(bucket, xor_digest)` pairs, one per bucket (1 024 total).
    AeDigest {
        /// The cache's name.
        cache: SmolStr,
        /// One `(bucket, xor_digest)` pair per bucket.
        buckets: Vec<(u16, u64)>,
    },
    /// Anti-entropy round, step 2: `(key, version)` pairs live in the
    /// mismatched `bucket` on the sender. A reply to [`Msg::AeEntries`] may
    /// repeat it for one bucket, each message holding the next slice of the
    /// listing, and the receiver concatenates them; see
    /// [`PROTOCOL_AE_LISTING_CHUNKS`].
    AeBucket {
        /// The cache's name.
        cache: SmolStr,
        /// The mismatched bucket.
        bucket: u16,
        /// `(key, version)` pairs live in the bucket on the sender.
        entries: Vec<(Bytes, Hlc)>,
    },
    /// Anti-entropy round, step 3: "send me your full records for these
    /// keys," `keys` being ones the requester is missing or holds stale.
    AePull {
        /// The cache's name.
        cache: SmolStr,
        /// The keys the requester is missing or holds stale.
        keys: Vec<Bytes>,
    },
    /// Replication-mode fan-out, batched: several records to apply
    /// together. `net::conn`'s per-peer writer coalesces consecutive
    /// same-cache queued [`Msg::Replicate`] messages into this; the batch
    /// applies under one lock acquisition. Raw-record layout, not postcard.
    /// Carries merged `Invalidation` records the same way [`Msg::Replicate`]
    /// does.
    ReplicateBatch {
        /// The cache's name.
        cache: SmolStr,
        /// The records to apply together.
        recs: Vec<WireRecord>,
    },
    /// Marks the end of one reply on a request/response connection kept
    /// open for reuse. Declared after every variant that predates it, so
    /// their postcard encodings stay unchanged.
    ReqDone,
    /// Anti-entropy round, step 2, large-bucket path: an IBLT sketch over a
    /// mismatched bucket's `(key_hash, version)` pairs, sent instead of
    /// [`Msg::AeBucket`] once the bucket's own listing would cost more on
    /// the wire than the sketch: it holds more entries than
    /// `ClusterConfig::ae_sketch_min_bucket`, or its listing would pass half
    /// a frame. The initiator subtracts its own local sketch from this one
    /// and peels the result; on failure it falls back to [`Msg::AeEntries`].
    AeSketch {
        /// The cache's name.
        cache: SmolStr,
        /// The mismatched bucket the sketch covers.
        bucket: u16,
        /// `ClusterConfig::ae_sketch_cells` of them, built by the sender.
        cells: Vec<Cell>,
    },
    /// Anti-entropy round, step 2 fallback: the sketch-fallback request for
    /// the full `(key, version)` listing of these buckets, after one or
    /// more `AeSketch` replies failed to decode. Answered like `AeDigest`,
    /// except that a listing too large for one frame arrives as several
    /// [`Msg::AeBucket`] messages to a peer on [`PROTOCOL_AE_LISTING_CHUNKS`]
    /// or later.
    AeEntries {
        /// The cache's name.
        cache: SmolStr,
        /// The buckets whose full listing is requested.
        buckets: Vec<u16>,
    },
    /// Anti-entropy round, step 3, sketch-decoded path: the sketch-decoded
    /// counterpart to [`Msg::AePull`], requesting records for the entries
    /// of `bucket` whose key hash is one of `hashes`. Answered like `AePull`.
    AePullHashes {
        /// The cache's name.
        cache: SmolStr,
        /// The bucket the hashes belong to.
        bucket: u16,
        /// Key hashes of the entries requested.
        hashes: Vec<u64>,
    },
    /// Anti-entropy round, step 2, part path: this node's 64 part digests for
    /// a mismatched `bucket` whose entry count passed
    /// `ClusterConfig::ae_part_min_bucket`, sent instead of [`Msg::AeBucket`]
    /// or [`Msg::AeSketch`]. The initiator compares these against its own
    /// part digests for the same bucket and requests only the parts that
    /// differ, via [`Msg::AeParts`].
    AePartDigests {
        /// The cache's name.
        cache: SmolStr,
        /// The mismatched bucket.
        bucket: u16,
        /// The bucket's 64 part digests.
        digests: Vec<u64>,
    },
    /// Anti-entropy round, step 3, part path: "send me your part digests'
    /// mismatched `(bucket, part)` pairs' answer," the part-grained
    /// counterpart of [`Msg::AeDigest`]. Answered one reply per part
    /// ([`Msg::AePart`] or [`Msg::AePartSketch`]), then [`Msg::ReqDone`].
    AeParts {
        /// The cache's name.
        cache: SmolStr,
        /// The `(bucket, part)` pairs whose digests mismatch.
        parts: Vec<(u16, u8)>,
    },
    /// A part's full `(key, version)` listing, the part-grained counterpart
    /// of [`Msg::AeBucket`].
    AePart {
        /// The cache's name.
        cache: SmolStr,
        /// The bucket the part belongs to.
        bucket: u16,
        /// The part's index within its bucket.
        part: u8,
        /// `(key, version)` pairs live in the part on the sender.
        entries: Vec<(Bytes, Hlc)>,
    },
    /// A part's IBLT sketch, the part-grained counterpart of
    /// [`Msg::AeSketch`], sent by the same rule: the part holds more entries
    /// than `ClusterConfig::ae_sketch_min_bucket`, or its listing would pass
    /// half a frame.
    AePartSketch {
        /// The cache's name.
        cache: SmolStr,
        /// The bucket the part belongs to.
        bucket: u16,
        /// The part's index within its bucket.
        part: u8,
        /// The part's sketch cells, built by the sender.
        cells: Vec<Cell>,
    },
    /// The answer to a [`Msg::StRequest`] from a node that cannot donate
    /// `cache` yet: it has not completed its own state transfer for it, or
    /// never opened it. The requester moves on to its next candidate.
    StUnavailable {
        /// The cache's name.
        cache: SmolStr,
    },
    /// Distribution-mode read: "I don't own `key`'s bucket under my view; do
    /// you?" Sent to a candidate owner in rendezvous-score order, carrying
    /// the requester's `view_hash` for the epoch check. Introduced in
    /// protocol 3.
    Fetch {
        /// The cache's name.
        cache: SmolStr,
        /// The postcard-encoded cache key.
        key: Bytes,
        /// The requester's ownership-view hash.
        view_hash: u64,
    },
    /// Answers [`Msg::Fetch`]: the record the responder holds for `key` in
    /// a trusted part it owns or is releasing, sent whatever the two views
    /// say, or `None` for a definitive miss, sent only on an equal view
    /// hash and a warm part. With no record in a trusted part and a view
    /// hash that differs from the request's, the responder sends
    /// [`Msg::StaleView`] instead.
    FetchReply {
        /// The record held for the key, or `None` for a definitive miss.
        rec: Option<WireRecord>,
    },
    /// Declines a [`Msg::Fetch`] without answering it: the responder cannot
    /// vouch for the key, because it does not have `cache` open as a
    /// distribution-mode cache, its copy of the key's part is unverified or
    /// stale whatever the views say, or the part is cold and it holds no
    /// record on an equal view hash. Distinct from a [`Msg::FetchReply`]
    /// carrying `None`, which is a definitive miss; the requester moves on
    /// to its next candidate owner. Introduced in protocol 3.
    FetchDeclined {
        /// The cache's name.
        cache: SmolStr,
    },
    /// Anti-entropy round, step 1, distribution-mode: like [`Msg::AeDigest`],
    /// but scoped to the sender's own owned buckets and carrying its
    /// `view_hash` for the epoch check. A brand-new variant, not a field
    /// added to [`Msg::AeDigest`]: mutating that struct variant's fields
    /// would change the wire encoding of every mode's anti-entropy traffic,
    /// not only distribution mode's.
    AeDigestScoped {
        /// The cache's name.
        cache: SmolStr,
        /// The sender's ownership-view hash.
        view_hash: u64,
        /// `(bucket, xor_digest)` pairs for the sender's owned buckets.
        buckets: Vec<(u16, u64)>,
    },
    /// Rebalance: "send me everything you hold for these buckets," the
    /// bucket-scoped counterpart of [`Msg::StRequest`]. Carries the
    /// requester's `view_hash` so a donor whose own view has since diverged
    /// can answer [`Msg::StaleView`] instead of transferring against a
    /// stale owner set.
    StBuckets {
        /// The cache's name.
        cache: SmolStr,
        /// The buckets the requester wants everything for.
        buckets: Vec<u16>,
        /// The requester's ownership-view hash.
        view_hash: u64,
    },
    /// One chunk of an [`Msg::StBuckets`] reply, the bucket-scoped
    /// counterpart of [`Msg::StChunk`]; `done` marks the final chunk.
    /// Raw-record layout, not postcard.
    StBucketChunk {
        /// The cache's name.
        cache: SmolStr,
        /// The bucket records in this chunk.
        recs: Vec<WireRecord>,
        /// Whether this is the final chunk.
        done: bool,
    },
    /// Any ownership-scoped request's requester and responder computed
    /// different view hashes: the responder declines outright, sent
    /// instead of a [`Msg::FetchReply`], an [`Msg::AeDigestScoped`] reply,
    /// or an [`Msg::StBucketChunk`] stream.
    StaleView {
        /// The cache's name.
        cache: SmolStr,
        /// The responder's ownership-view hash.
        responder_view_hash: u64,
    },
    /// Distribution-mode fan-out: records for the receiver to apply as an
    /// owner, stamped with the sender's `view_hash` so the receiver can
    /// tell a write routed under a stale owner set from one routed under
    /// its own. A receiver whose view hash differs re-forwards a `hops == 0`
    /// batch to the records' owners under its own view, sender excluded, as
    /// a `hops == 1` batch; a batch at `hops == 1` is applied and never
    /// re-forwarded, so a disagreement between two views costs one extra
    /// hop, never a loop. Raw-record layout, not postcard. Introduced in
    /// protocol 3; a distribution-mode cache never fans out to an older
    /// peer.
    ForwardBatch {
        /// The cache's name.
        cache: SmolStr,
        /// The sender's ownership-view hash.
        view_hash: u64,
        /// Hops the batch has travelled: 0 from the writer, 1 once re-forwarded.
        hops: u8,
        /// The records to apply as an owner.
        recs: Vec<WireRecord>,
    },
    /// Marks the donor's last chunk for one bucket of an [`Msg::StBuckets`]
    /// group: "bucket `bucket` is fully sent." Sent on the same connection
    /// right after that bucket's final [`Msg::StBucketChunk{done: false}`],
    /// gated on the requester's advertised protocol. Lets the receiver
    /// release that bucket from cold as soon as it lands, instead of
    /// waiting for the whole group's trailing `done: true` chunk. Declared
    /// last, after every variant that predates it, so their postcard
    /// encodings stay unchanged. Introduced in protocol 4.
    StBucketDone {
        /// The cache's name.
        cache: SmolStr,
        /// The bucket whose last chunk was sent.
        bucket: u16,
    },
    /// Answers [`Msg::StBucketDone`]: "I applied and released `bucket`."
    /// Best-effort: its absence (an older peer, a requester crash, a
    /// connection break) is never an error, only a slower release via the
    /// existing timer-and-confirming-AE-round path. `view_hash` is the
    /// requester's own `OwnershipView` hash at the
    /// moment it requested the buckets carrying this ack (the same value
    /// sent on [`Msg::StBuckets`]): the donor discards the ack rather than
    /// trusting it once its own current view hash has moved past that
    /// value, so an ack from before a view change never counts toward a
    /// bucket's release. Declared last, after every variant that predates
    /// it, so their postcard encodings stay unchanged. Introduced in
    /// protocol 4.
    StBucketAck {
        /// The cache's name.
        cache: SmolStr,
        /// The bucket applied and released.
        bucket: u16,
        /// The requester's ownership-view hash when it requested the buckets.
        view_hash: u64,
    },
    /// Anti-entropy round, step 1, under a part-granular view: per bucket,
    /// a mask of the parts the round covers (bit `p` is part `p`) and the
    /// XOR of their part digests, with the sender's `view_hash`. The
    /// responder folds its own part digests over the same mask and answers
    /// each bucket that differs with [`Msg::AePartDigests`]. Declared last,
    /// so every earlier variant's postcard encoding stays unchanged.
    /// Introduced in protocol 5.
    AeDigestMasked {
        /// The cache's name.
        cache: SmolStr,
        /// The sender's ownership-view hash.
        view_hash: u64,
        /// `(bucket, part_mask, xor_digest)` per bucket.
        buckets: Vec<(u16, u64, u64)>,
    },
    /// "Load these keys through your loader": sent to the node a key's
    /// loads gather on. The responder answers from its cache where it can,
    /// joins a load already running for a key, and loads the rest in one
    /// loader call, so every node asking for a key in the same moment
    /// shares one load. Introduced in protocol 7.
    Load {
        /// The cache's name.
        cache: SmolStr,
        /// The keys to load.
        keys: Vec<Bytes>,
    },
    /// Answers [`Msg::Load`], in as many messages as the records need:
    /// `found` holds the record the responder stores for each requested key
    /// it or its source holds, and `uncached` pairs a key with a value the
    /// responder loaded but did not store, at its memory ceiling or
    /// because an invalidation overtook the load. A requested key in
    /// neither is one the source does not hold.
    Loaded {
        /// The records the responder stores for requested keys.
        found: Vec<WireRecord>,
        /// `(key, value)` pairs loaded but not stored.
        uncached: Vec<(Bytes, Bytes)>,
    },
    /// Answers [`Msg::Load`] when the responder's loader failed, with the
    /// error's message.
    LoadFailed {
        /// The cache's name.
        cache: SmolStr,
        /// The loader error's message.
        message: String,
    },
    /// Declines a [`Msg::Load`]: the responder has no loader for `cache`,
    /// does not have it open, or, in `Mode::Distributed`, cannot vouch for
    /// a requested key's part yet. The requester loads elsewhere.
    LoadDeclined {
        /// The cache's name.
        cache: SmolStr,
    },
    /// "This key nears its expiry": one-way, sent to the node that
    /// refreshes `key` by a node whose read found the entry at version `ver`
    /// past its refresh point. The refresher reloads the key once, if it
    /// still holds that version. Introduced in protocol 7.
    RefreshHint {
        /// The cache's name.
        cache: SmolStr,
        /// The postcard-encoded cache key.
        key: Bytes,
        /// The version of the entry the reading node found.
        ver: Hlc,
    },
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
const RAW_KIND_ST_BUCKET_CHUNK: u8 = 3;
const RAW_KIND_FORWARD_BATCH: u8 = 4;

/// Record-level flag: this record is a tombstone (`WireRecord::value` is
/// `None`).
const RECORD_FLAG_TOMBSTONE: u8 = 0b01;
/// Record-level flag: `expires_at_ms` is `Some` and the header's field holds
/// its value. Without this flag the field is meaningless, always `0`, so a
/// real expiry of `0` is never ambiguous with "no expiry."
const RECORD_FLAG_HAS_EXPIRY: u8 = 0b10;

/// Fixed header preceding a raw-record frame's payload: which of the
/// record-carrying [`Msg`] variants this is, the state-transfer `done` flag
/// (always `0` outside [`Msg::StChunk`]/[`Msg::StBucketChunk`]), the cache
/// name's byte length, and how many [`RecordHeader`]-prefixed records
/// follow. Every field is a byte-order-explicit, alignment-1 `zerocopy`
/// integer: no implicit padding.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct RawFrameHeader {
    msg_kind: u8,
    done: u8,
    cache_len: U16,
    record_count: U32,
}

/// The extra fixed header a [`Msg::ForwardBatch`] frame carries between
/// the cache name and its records: the sender's ownership view hash and
/// the batch's hop count.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct ForwardHeader {
    view_hash: U64,
    hops: u8,
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

/// Exact byte length a [`Msg::ForwardBatch`] of one record encodes to, given
/// the cache-name/key/value lengths: [`replicate_frame_len`] plus the
/// [`ForwardHeader`]. A `Distributed` owner fans a record out to its
/// co-owners as a forward, so this is the widest frame one record travels
/// in, and the one a merged value is held within.
#[must_use]
pub(crate) fn forward_frame_len(cache_len: usize, key_len: usize, value_len: usize) -> usize {
    replicate_frame_len(cache_len, key_len, value_len) + size_of::<ForwardHeader>()
}

/// The widest postcard encoding of one [`Cell`]: each of its six integer
/// fields at its maximum varint width.
pub(crate) const CELL_MAX_WIRE_LEN: usize = 5 + 10 + 10 + 5 + 10 + 10;

/// The widest postcard encoding of one found [`WireRecord`] of a
/// [`Msg::Loaded`] reply, beyond its key's and value's own bytes: the key's
/// length, the value's presence tag and length, the version's `wall_ms`,
/// `logical` and `node`, and the expiry's presence tag and `u64`, each at its
/// maximum varint width.
pub(crate) const LOADED_RECORD_MAX_WIRE_LEN: usize = 10 + (1 + 10) + (10 + 5 + 10) + (1 + 10);

/// The widest postcard encoding of one `uncached` pair of a [`Msg::Loaded`]
/// reply, beyond the key's and value's own bytes: the two lengths.
pub(crate) const LOADED_UNCACHED_MAX_WIRE_LEN: usize = 10 + 10;

/// The widest postcard encoding of one `(key, version)` entry of a
/// [`Msg::AeBucket`] or [`Msg::AePart`] listing, beyond the key's own bytes:
/// the key's length, then the version's `wall_ms`, `logical` and `node`, each
/// as a varint at its maximum width.
pub(crate) const AE_ENTRY_MAX_WIRE_LEN: usize = 10 + 10 + 5 + 10;

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
        Msg::Replicate { cache, rec } => encode_raw_frame(
            RAW_KIND_REPLICATE,
            cache,
            std::slice::from_ref(rec),
            RawExtra::None,
        ),
        Msg::ReplicateBatch { cache, recs } => {
            encode_raw_frame(RAW_KIND_REPLICATE_BATCH, cache, recs, RawExtra::None)
        }
        Msg::StChunk { cache, recs, done } => {
            encode_raw_frame(RAW_KIND_ST_CHUNK, cache, recs, RawExtra::Done(*done))
        }
        Msg::StBucketChunk { cache, recs, done } => {
            encode_raw_frame(RAW_KIND_ST_BUCKET_CHUNK, cache, recs, RawExtra::Done(*done))
        }
        Msg::ForwardBatch {
            cache,
            view_hash,
            hops,
            recs,
        } => encode_raw_frame(
            RAW_KIND_FORWARD_BATCH,
            cache,
            recs,
            RawExtra::Forward {
                view_hash: *view_hash,
                hops: *hops,
            },
        ),
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

/// What a raw-record frame carries beyond its cache name and records: the
/// state-transfer `done` flag in the fixed header, or a
/// [`Msg::ForwardBatch`]'s [`ForwardHeader`] after the cache name.
#[derive(Clone, Copy)]
enum RawExtra {
    None,
    Done(bool),
    Forward { view_hash: u64, hops: u8 },
}

fn encode_raw_frame(
    kind: u8,
    cache: &SmolStr,
    recs: &[WireRecord],
    extra: RawExtra,
) -> Result<Bytes, CodecError> {
    let done = matches!(extra, RawExtra::Done(true));
    let extra_len = match extra {
        RawExtra::Forward { .. } => size_of::<ForwardHeader>(),
        RawExtra::None | RawExtra::Done(_) => 0,
    };
    let cache_bytes = cache.as_bytes();
    let cache_len = u16::try_from(cache_bytes.len())
        .map_err(|_| CodecError::MalformedFrame("cache name exceeds 64 KiB"))?;
    let record_count = u32::try_from(recs.len())
        .map_err(|_| CodecError::MalformedFrame("record count exceeds u32::MAX"))?;

    let total = recs.iter().fold(
        1 + size_of::<RawFrameHeader>() + cache_bytes.len() + extra_len,
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
    if let RawExtra::Forward { view_hash, hops } = extra {
        buf.extend_from_slice(
            ForwardHeader {
                view_hash: U64::new(view_hash),
                hops,
            }
            .as_bytes(),
        );
    }

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

    let forward = if header.msg_kind == RAW_KIND_FORWARD_BATCH {
        let (fh, _) = ForwardHeader::read_from_prefix(&body.as_ref()[offset.min(body.len())..])
            .map_err(|_| CodecError::MalformedFrame("forward header truncated"))?;
        offset += size_of::<ForwardHeader>();
        Some(fh)
    } else {
        None
    };

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
        RAW_KIND_ST_BUCKET_CHUNK => Ok(Msg::StBucketChunk {
            cache,
            recs,
            done: header.done != 0,
        }),
        RAW_KIND_FORWARD_BATCH => {
            let fh = forward.ok_or(CodecError::MalformedFrame(
                "ForwardBatch frame carries no forward header",
            ))?;
            Ok(Msg::ForwardBatch {
                cache,
                view_hash: fh.view_hash.get(),
                hops: fh.hops,
                recs,
            })
        }
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
            Err(err) => decode_older_hello(&body).ok_or_else(|| CodecError::from(err)),
        },
        FRAME_KIND_RAW_RECORD => decode_raw_frame(&body),
        _ => Err(CodecError::MalformedFrame("unknown frame discriminant")),
    }
}

/// [`Msg::Hello`] as protocols 2 to 5 encode it: the same variant index,
/// no `cluster` field.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
enum UnnamedHello {
    Hello {
        node: NodeId,
        incarnation: u64,
        protocol: u16,
    },
}

/// [`Msg::Hello`] as protocol 1 encodes it: the same variant index, no
/// `protocol` field either.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
enum LegacyMsg {
    Hello { node: NodeId, incarnation: u64 },
}

/// The only shapes a current decode can fail on that still mean something:
/// an older peer's [`Msg::Hello`], so [`decode`] falls back to them, newest
/// first.
fn decode_older_hello(body: &[u8]) -> Option<Msg> {
    if let Ok(UnnamedHello::Hello {
        node,
        incarnation,
        protocol,
    }) = postcard::from_bytes(body)
    {
        return Some(Msg::Hello {
            node,
            incarnation,
            protocol,
            cluster: UNNAMED_CLUSTER,
        });
    }
    let LegacyMsg::Hello { node, incarnation } = postcard::from_bytes(body).ok()?;
    Some(Msg::Hello {
        node,
        incarnation,
        protocol: 1,
        cluster: UNNAMED_CLUSTER,
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod prop_tests;

/// Kani proof over the replicate frame length arithmetic.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// For lengths within a frame, the computed length never overflows and
    /// covers the header and every byte of the name, key and value.
    #[kani::proof]
    fn replicate_frame_len_covers_every_byte_without_overflow() {
        let cache_len: usize = kani::any();
        let key_len: usize = kani::any();
        let value_len: usize = kani::any();
        kani::assume(cache_len <= MAX_FRAME && key_len <= MAX_FRAME && value_len <= MAX_FRAME);
        let len = replicate_frame_len(cache_len, key_len, value_len);
        assert_eq!(
            len,
            1 + size_of::<RawFrameHeader>() + cache_len + RECORD_HEADER_LEN + key_len + value_len
        );
        assert!(len > cache_len + key_len + value_len);
    }
}
