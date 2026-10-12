//! The value types a `Shard` exchanges with the cluster layer: modes, TTLs,
//! events, and the digest and compaction records.

use std::num::NonZeroU8;
use std::time::Duration;

use bytes::Bytes;
use smol_str::SmolStr;

use super::{crdt, duration_ms};
use crate::hlc::Hlc;
use crate::node::NodeId;

#[cfg(doc)]
use super::{BUCKET_COUNT, ConflictResolver, PART_COUNT, Shard, ShardOps};
#[cfg(doc)]
use crate::config::ClusterConfig;

/// A named cache's clustering behavior: how writes fan out to other nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mode {
    /// No cluster traffic; entries live only on this node.
    Local,
    /// Every node caches independently; writes broadcast an
    /// [`crate::wire::Msg::Invalidate`].
    Invalidation,
    /// Every node holds every entry; writes broadcast the full
    /// [`crate::wire::Msg::Replicate`].
    Replicated,
    /// Each key lives on exactly `owners` live nodes, chosen by rendezvous
    /// hashing over the cache's live, protocol-compatible peers.
    Distributed {
        /// How many live nodes each bucket is replicated to. Must be at
        /// least 2: `CacheBuilder::open` rejects 1, since a single owner is
        /// a lost bucket the instant it leaves.
        owners: NonZeroU8,
    },
}

impl Mode {
    /// How many owners [`Mode::distributed`] gives each bucket.
    pub const DEFAULT_OWNERS: NonZeroU8 = NonZeroU8::new(2).expect("2 is nonzero");

    /// [`Mode::Distributed`] with [`Mode::DEFAULT_OWNERS`] owners per
    /// bucket: one replica to lose without losing the bucket. The cluster
    /// itself is discovered, never configured; `owners` is the one number
    /// a caller may still want to raise, by spelling the variant out.
    #[must_use]
    pub const fn distributed() -> Self {
        Self::Distributed {
            owners: Self::DEFAULT_OWNERS,
        }
    }

    /// The wire token gossiped for this mode under a `cache:<name>` chitchat
    /// key. A stable string, not a `Debug`/`Display` impl, so renaming a
    /// variant never changes the wire.
    pub(crate) fn as_token(self) -> SmolStr {
        match self {
            Self::Local => SmolStr::new_static("local"),
            Self::Invalidation => SmolStr::new_static("invalidation"),
            Self::Replicated => SmolStr::new_static("replicated"),
            Self::Distributed { owners } => smol_str::format_smolstr!("distributed:{owners}"),
        }
    }

    /// Parses [`Mode::as_token`]'s output back into a `Mode`, or `None` for
    /// anything else, so an unrecognized token skips that peer rather than
    /// failing the whole gossip round.
    pub(crate) fn from_token(token: &str) -> Option<Self> {
        match token {
            "local" => Some(Self::Local),
            "invalidation" => Some(Self::Invalidation),
            "replicated" => Some(Self::Replicated),
            _ => token
                .strip_prefix("distributed:")
                .and_then(|n| n.parse::<u8>().ok())
                .and_then(NonZeroU8::new)
                .map(|owners| Self::Distributed { owners }),
        }
    }
}

/// A live entry's remaining lifetime, as [`Shard::ttl_of`] reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ttl {
    /// The entry has no expiry.
    Never,
    /// The entry expires after this long.
    Remaining(Duration),
}

/// Who caused a cache [`Event`]: this node's own API call, or a message
/// received from a remote peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Origin {
    /// Caused by a local `insert`/`remove`/`get_or_load` call.
    Local,
    /// Caused by an inbound wire message from the given peer.
    ///
    /// For a record a [`ConflictResolver::merge`] resolver produced, `NodeId`
    /// here is the merge-derived id the engine minted for it, not the id of
    /// any node that authored a write: it names no member of the
    /// cluster.
    ///
    /// `ShardOps::apply_remote_batch`'s pre-fold narrows this further for a
    /// batch carrying several records for one key from distinct origins (a
    /// peer's fan-out or anti-entropy-pull reply bundling more than one
    /// node's writes to the same key): pre-fold collapses that whole run
    /// into one real apply and one published event, and the event's
    /// `NodeId` here names only the run's last record's origin; the other
    /// contributors' `NodeId`s are folded into the stored content but never
    /// surface on the event stream. This is an accepted cost of folding a
    /// batch's own same-key records together before the stripe lock (see
    /// `engine::prefold_batch`), not a bug: the alternative is one event
    /// per contributing record, which is what pre-fold exists to avoid.
    Remote(NodeId),
}

/// A change notification published on [`Shard::events`] / `Cache::events`.
#[derive(Debug, Clone)]
pub enum Event<K, V> {
    /// A key is inserted where none existed before.
    Created { key: K, value: V, origin: Origin },
    /// An existing key's value changes.
    Updated { key: K, value: V, origin: Origin },
    /// A key is removed; a tombstone is applied.
    Removed { key: K, origin: Origin },
}

/// A second-level anti-entropy part: a bucket and one of its
/// [`PART_COUNT`] parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketPart {
    /// The bucket this part belongs to.
    pub bucket: u16,
    /// The part within [`BucketPart::bucket`], `0..PART_COUNT`.
    pub part: u8,
}

/// A bucket paired with its anti-entropy digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketDigest {
    /// The bucket this digest covers.
    pub bucket: u16,
    /// The digest itself.
    pub digest: u64,
}

/// A key paired with the version stamped on it, as an anti-entropy or
/// state-transfer listing carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyVersion {
    /// The record's wire-encoded key bytes.
    pub key: Bytes,
    /// The version stamped on the record.
    pub version: Hlc,
}

/// A bucket paired with its live, un-GC'd entry count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketLen {
    /// The bucket this count covers.
    pub bucket: u16,
    /// The bucket's entry count.
    pub len: usize,
}

/// A bucket paired with its [`PART_COUNT`] part digests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketPartDigests {
    /// The bucket these digests cover.
    pub bucket: u16,
    /// One digest per part, in part order.
    pub digests: Vec<u64>,
}

/// Entries per bucket, as an anti-entropy exchange reports them: every
/// requested bucket present, empty lists included.
pub type BucketEntries = Vec<(u16, Vec<KeyVersion>)>;

/// Entries per part, as a second-level anti-entropy exchange reports them:
/// every requested `(bucket, part)` pair present, empty lists included.
pub type PartEntries = Vec<(BucketPart, Vec<KeyVersion>)>;

/// The two ages the CRDT compaction sweep and the merge apply path work
/// with, in milliseconds: `retire_after_ms` (`ClusterConfig::crdt_retire_after`)
/// is how long a writer must be gone before stage one retires it and, twice
/// over, how old a retirement must be before stage two folds it;
/// `receipt_ttl_ms` is how long a fold receipt lives after the writer's
/// retirement instant before both the sweep and every merge apply drop it.
/// The receipt must outlive every reachable replica's own fold of the same
/// writer plus one anti-entropy exchange, and a replica folds on its first
/// sweep after the retirement is two bounds old, so the receipt lives the
/// longer of three bounds and two bounds plus two sweep periods
/// ([`ClusterConfig::crdt_compaction_bounds`]).
/// Whether a merging cache's membership has settled this compaction tick.
/// `Settled` lets [`ConflictResolver::compact`]'s second stage fold aged
/// retirements; `Churning` defers it. See `crdt_cache_is_quiet`'s doc for
/// the settling rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quiescence {
    /// Every relevant member has settled: continuously present or absent
    /// long enough to trust as dead.
    Settled,
    /// At least one relevant member is still flapping in or out.
    Churning,
}

impl Quiescence {
    /// `Settled` for `true`, `Churning` for `false`.
    #[must_use]
    pub fn from_settled(settled: bool) -> Self {
        if settled {
            Self::Settled
        } else {
            Self::Churning
        }
    }

    /// Whether this is [`Quiescence::Settled`].
    #[must_use]
    pub fn is_settled(self) -> bool {
        matches!(self, Self::Settled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionBounds {
    /// `ClusterConfig::crdt_retire_after`, in milliseconds.
    pub retire_after_ms: u64,
    /// How long a fold receipt lives past the writer's retirement instant.
    pub receipt_ttl_ms: u64,
}

impl CompactionBounds {
    /// Bounds for `retire_after` with the sweep running every `sweep_period`.
    #[must_use]
    pub fn new(retire_after: Duration, sweep_period: Duration) -> Self {
        let retire_after_ms = duration_ms(retire_after);
        let period_ms = duration_ms(sweep_period);
        let three_bounds = retire_after_ms.saturating_mul(3);
        let two_bounds_two_periods = retire_after_ms
            .saturating_mul(2)
            .saturating_add(period_ms.saturating_mul(2));
        Self {
            retire_after_ms,
            receipt_ttl_ms: three_bounds.max(two_bounds_two_periods),
        }
    }

    /// Bounds with the receipt living exactly three bounds: what a sweep
    /// running at least four times per bound amounts to, and the shape
    /// every unit test of the CRDT types assumes.
    #[must_use]
    pub fn three_bounds(retire_after_ms: u64) -> Self {
        Self {
            retire_after_ms,
            receipt_ttl_ms: retire_after_ms.saturating_mul(3),
        }
    }
}

/// What one [`ShardOps::compact_pass`] call did: the writers its scan found
/// eligible (see the note on the metric in the implementation), the
/// records it rewrote, and how many stripes it walked, so the
/// caller can keep calling until the visited stripes add up to
/// [`BUCKET_COUNT`] and the whole keyspace has been examined this tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactPassOutcome {
    /// Writers the scan found eligible for retirement this call.
    pub retired: Vec<crdt::WriterId>,
    /// Records rewritten in their compacted form this call.
    pub compacted: usize,
    /// Stripes the scan walked before its entry budget ran out.
    pub stripes_visited: usize,
}
