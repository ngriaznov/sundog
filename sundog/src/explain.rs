//! What [`Cache::explain`](crate::Cache::explain) reports about a read of
//! one key: what this node stores for it and, for a `Mode::Distributed`
//! cache, this node's residency marks for the key's part, what its own copy
//! makes of a fetch, what it answers a peer's fetch, and what each other
//! owner answers when asked.

use std::fmt;
use std::time::Duration;

use smol_str::SmolStr;

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::net::FetchOutcome;
use crate::node::NodeId;
use crate::store::{Mode, PartId};
use crate::wire::WireRecord;

/// This node's residency marks for one part of a `Mode::Distributed`
/// cache, read once, in the order the marks' writers take their locks.
/// Each mark is read on its own, so a mark a writer moves during the read
/// can show its state from just before or just after the move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent residency marks; the verdict tests exhaust their product"
)]
pub struct Residency {
    /// Whether this node's ownership view names it an owner of the part.
    pub owns: bool,
    /// How long ago the part's disown grace began, while this node still
    /// holds its copy of a part it no longer owns.
    pub releasing_for: Option<Duration>,
    /// Whether rebalance marked the part cold: owned, not yet pulled from a
    /// co-owner.
    pub cold_marked: bool,
    /// Whether the part lies outside the last settled view and no pull or
    /// verification has served it since. Every part this node does not own
    /// reads unsettled once a view has settled; for an owned part it is
    /// the window between a published view and rebalance marking its
    /// gained parts cold.
    pub unsettled: bool,
    /// Whether a warm spill-tier reopen replayed the part from disk with no
    /// co-owner check yet.
    pub unverified: bool,
    /// Whether this node owns the part again while it holds a copy from
    /// owning it before, which lacks what changed while it was not an
    /// owner.
    pub stale: bool,
}

impl Residency {
    /// Marks for a part with none set: owned or not, with nothing cold,
    /// distrusted or releasing.
    pub(crate) const fn new(owns: bool) -> Self {
        Self {
            owns,
            releasing_for: None,
            cold_marked: false,
            unsettled: false,
            unverified: false,
            stale: false,
        }
    }

    /// Whether a local miss in the part says nothing about the key: marked
    /// cold, or owned and unsettled.
    #[must_use]
    pub const fn cold(&self) -> bool {
        self.cold_marked || (self.unsettled && self.owns)
    }

    /// Whether the part counts as cold only for being owned and unsettled,
    /// before rebalance marks it.
    #[must_use]
    pub const fn implicitly_cold(&self) -> bool {
        !self.cold_marked && self.unsettled && self.owns
    }

    /// Whether a local hit in the part is not trusted: replayed and not yet
    /// verified, or stale.
    #[must_use]
    pub const fn distrusted(&self) -> bool {
        self.unverified || self.stale
    }

    /// Whether this node serves the part's records to a peer: owned, or
    /// mid disown grace.
    #[must_use]
    pub const fn resident(&self) -> bool {
        self.owns || self.releasing_for.is_some()
    }
}

/// What this node's own copy of a `Mode::Distributed` key's part makes of a
/// fetch, before any owner is asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LocalRead {
    /// Owned, trusted and holding a live entry: the fetch answers it here.
    Hit,
    /// Owned, trusted, warm and holding no live entry: the fetch answers
    /// `None` here.
    Miss,
    /// Not an owner of the part: the fetch asks the owners.
    NotOwner,
    /// Owned, but a local hit is not trusted: the fetch asks the other
    /// owners whatever the local copy holds.
    Distrusted,
    /// Owned and trusted but cold, with no live entry: the miss says
    /// nothing, so the fetch asks the other owners.
    ColdMiss,
}

impl LocalRead {
    /// Whether the fetch answers here, without asking an owner.
    #[must_use]
    pub const fn answers(self) -> bool {
        matches!(self, Self::Hit | Self::Miss)
    }
}

/// What this node answers a peer's `Fetch` for a key of a
/// `Mode::Distributed` cache.
///
/// The verdicts are decided in order: distrust declines whatever the views
/// say, a held record is sent whatever the views say, a view hash that
/// differs is then stale, and only on an equal view hash does a cold part
/// decline and a warm one answer a definitive miss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServeVerdict {
    /// Sends the record it holds, live or a tombstone, in a trusted part,
    /// whatever the two views say.
    Serve,
    /// Holds no record in a trusted part and the asker's view differs from
    /// this node's: the asker refreshes its view and asks again.
    Stale,
    /// Holds no record, on an equal view, in a trusted warm part: a
    /// definitive miss.
    Miss,
    /// Declines: the part is distrusted, so even a held record is not
    /// sent, whatever the views say.
    DeclineDistrusted,
    /// Declines: the part is cold and holds no record, on an equal view, so
    /// a miss says nothing.
    DeclineCold,
}

/// Why a read of one key answers what it answers on this node: what this
/// node stores for the key and, for a `Mode::Distributed` cache, how its
/// fetch decides and what each other owner holds. Built by
/// [`Cache::explain`](crate::Cache::explain).
///
/// The [`Display`](fmt::Display) form is a few lines of human text for
/// logs and incident notes. Its layout is not part of the semver contract;
/// read the fields to act on an explanation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReadExplanation {
    /// The cache's name.
    pub cache: SmolStr,
    /// This node.
    pub node: NodeId,
    /// The cache's mode.
    pub mode: Mode,
    /// The key's part.
    pub part: PartId,
    /// The cache's clock, in epoch milliseconds, when the local record was
    /// read. Every expiry in the explanation is judged at it.
    pub at_ms: u64,
    /// What this node stores for the key.
    pub local: LocalRecord,
    /// Where a [`Cache::fetch`](crate::Cache::fetch) takes its answer,
    /// predicted from [`DistributedRead::local_read`] and
    /// [`DistributedRead::probes`].
    pub source: ReadSource,
    /// How a `Mode::Distributed` fetch decides; `None` in every other mode.
    pub distributed: Option<DistributedRead>,
}

/// How a `Mode::Distributed` fetch of the key decides on this node, and
/// what each other owner answers.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DistributedRead {
    /// The hash of the ownership view every verdict and probe used.
    pub view_hash: u64,
    /// The hash of this node's view after the probes, when it differs from
    /// [`view_hash`](Self::view_hash): the view moved while the owners were
    /// asked.
    pub view_moved_to: Option<u64>,
    /// The key's owners in rendezvous order, this node included when it is
    /// one, as [`Cache::owners_of`](crate::Cache::owners_of) returns them.
    pub owners: Vec<NodeId>,
    /// This node's residency marks for the key's part.
    pub residency: Residency,
    /// What this node's own copy makes of a fetch.
    pub local_read: LocalRead,
    /// What this node answers a peer's fetch sent with an equal view hash.
    /// Never [`ServeVerdict::Stale`].
    pub serves_peers: ServeVerdict,
    /// Each other owner's answer, in [`owners`](Self::owners) order.
    pub probes: Vec<OwnerProbe>,
}

/// What this node's engine stores for a key, read without touching it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LocalRecord {
    /// No entry and no tombstone: never written here, swept after
    /// expiring, or released with its part.
    Absent,
    /// A tombstone: the key was deleted.
    #[non_exhaustive]
    Tombstone {
        /// The delete's version.
        version: Hlc,
    },
    /// A live entry a read returns.
    #[non_exhaustive]
    Live {
        /// The write's version.
        version: Hlc,
        /// The absolute expiry in epoch milliseconds, `None` for none.
        expires_at_ms: Option<u64>,
        /// Whether the value is in the spill tier on disk. Reported without
        /// a disk read, so a read can still miss when that read fails.
        spilled: bool,
    },
    /// An entry a read no longer returns, not yet swept.
    #[non_exhaustive]
    Lapsed {
        /// The write's version.
        version: Hlc,
        /// The absolute expiry in epoch milliseconds, `None` for none.
        expires_at_ms: Option<u64>,
        /// Why a read no longer returns it.
        cause: Lapse,
    },
}

impl LocalRecord {
    /// Whether a read returns the entry.
    #[must_use]
    pub const fn is_live(&self) -> bool {
        matches!(self, Self::Live { .. })
    }

    /// Whether this node sends the record to a peer whose fetch it serves:
    /// a live entry or a tombstone.
    #[must_use]
    pub const fn is_held(&self) -> bool {
        matches!(self, Self::Live { .. } | Self::Tombstone { .. })
    }
}

/// Why a stored entry no longer reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lapse {
    /// Its expiry passed. Wins when the entry is also idle.
    Expired,
    /// It went unread for the cache's `tti`.
    Idle,
}

/// One other owner's answer to the key's fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct OwnerProbe {
    /// The owner asked.
    pub node: NodeId,
    /// Its answer.
    pub answer: ProbeAnswer,
}

/// What an owner answers a fetch of the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProbeAnswer {
    /// It sends the record it holds. A held record is sent whatever the two
    /// views say, and from a part the owner is releasing, so this does not
    /// show the owner's part is warm.
    Held(ProbedRecord),
    /// It holds no record, on an equal view, in a trusted warm part: a
    /// definitive miss.
    Miss,
    /// It holds no record in a part it trusts and its view differs from
    /// this node's. A fetch retries it; the explanation counts it as no
    /// answer.
    #[non_exhaustive]
    StaleView {
        /// The owner's view hash.
        responder_view_hash: u64,
    },
    /// It declines: the cache is not open there or not `Mode::Distributed`,
    /// or its copy of the part is distrusted whatever the views say, or
    /// cold with no record on an equal view.
    Declined,
    /// It gives no answer.
    Unreached(Unreached),
}

impl ProbeAnswer {
    /// Whether a fetch takes this answer: a held record or a miss.
    #[must_use]
    pub const fn answers(&self) -> bool {
        matches!(self, Self::Held(_) | Self::Miss)
    }
}

/// The record an owner sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProbedRecord {
    /// The record's version.
    pub version: Hlc,
    /// The absolute expiry in epoch milliseconds, `None` for none.
    pub expires_at_ms: Option<u64>,
    /// What a fetch makes of the record.
    pub reads: Reads,
}

/// What a fetch makes of an owner's record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reads {
    /// The value.
    Value,
    /// A miss: the record is a tombstone.
    Deleted,
    /// A miss: the record's expiry passed.
    Expired,
    /// A miss: the value does not decode as the cache's value type.
    Undecodable,
}

/// Why an owner gives no answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Unreached {
    /// It is not in this node's mesh.
    NotAMember,
    /// It speaks a protocol too old for a `Mode::Distributed` fetch.
    ProtocolTooOld,
    /// It did not answer within `ClusterConfig::fetch_timeout`.
    TimedOut,
    /// The connection failed.
    Io(std::io::ErrorKind),
    /// Its reply did not decode.
    Codec,
}

/// Where a fetch of the key takes its answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadSource {
    /// This node's own copy.
    #[non_exhaustive]
    Local {
        /// Whether it returns a value.
        hit: bool,
    },
    /// The first owner, in rendezvous order, whose answer a fetch takes.
    #[non_exhaustive]
    Owner {
        /// The owner.
        node: NodeId,
        /// Whether its record reads as a value.
        hit: bool,
    },
    /// No owner answers: the fetch returns
    /// [`CacheError::FetchUnavailable`](crate::CacheError::FetchUnavailable).
    Unavailable,
}

impl ReadSource {
    /// Whether a fetch returns a value.
    #[must_use]
    pub const fn returns_value(&self) -> bool {
        matches!(
            self,
            Self::Local { hit: true } | Self::Owner { hit: true, .. }
        )
    }
}

/// Whether `rec` reads as no value at `now_ms` whatever it holds: a
/// tombstone, or past its expiry. `None` for a record whose value a read
/// decodes. Pure; unit tested directly.
pub(crate) fn dead_record(rec: &WireRecord, now_ms: u64) -> Option<Reads> {
    if rec.is_tombstone() {
        Some(Reads::Deleted)
    } else if rec.expires_at_ms.is_some_and(|expires| expires <= now_ms) {
        Some(Reads::Expired)
    } else {
        None
    }
}

/// An owner's answer from its fetch reply, `None` when the reply did not
/// arrive within the fetch timeout. `reads` says what a fetch makes of a
/// sent record. Pure; unit tested directly.
pub(crate) fn classify_probe(
    reply: Option<Result<FetchOutcome, CodecError>>,
    reads: impl FnOnce(&WireRecord) -> Reads,
) -> ProbeAnswer {
    match reply {
        None => ProbeAnswer::Unreached(Unreached::TimedOut),
        Some(Ok(FetchOutcome::Found(Some(rec)))) => ProbeAnswer::Held(ProbedRecord {
            version: rec.ver,
            expires_at_ms: rec.expires_at_ms,
            reads: reads(&rec),
        }),
        Some(Ok(FetchOutcome::Found(None))) => ProbeAnswer::Miss,
        Some(Ok(FetchOutcome::Stale {
            responder_view_hash,
        })) => ProbeAnswer::StaleView {
            responder_view_hash,
        },
        Some(Ok(FetchOutcome::Declined)) => ProbeAnswer::Declined,
        Some(Err(CodecError::Io(err))) => ProbeAnswer::Unreached(match err.kind() {
            std::io::ErrorKind::NotFound => Unreached::NotAMember,
            std::io::ErrorKind::Unsupported => Unreached::ProtocolTooOld,
            std::io::ErrorKind::TimedOut => Unreached::TimedOut,
            kind => Unreached::Io(kind),
        }),
        Some(Err(
            CodecError::Postcard(_)
            | CodecError::MalformedFrame(_)
            | CodecError::FrameTooLarge { .. },
        )) => ProbeAnswer::Unreached(Unreached::Codec),
    }
}

/// `wall_ms.logical@node`.
struct Version(Hlc);

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}@{}", self.0.wall_ms, self.0.logical, self.0.node)
    }
}

/// `, expires at N ms`, or nothing for no expiry.
struct Expiry(Option<u64>);

impl fmt::Display for Expiry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(at) => write!(f, ", expires at {at} ms"),
            None => Ok(()),
        }
    }
}

impl fmt::Display for LocalRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Absent => f.write_str("absent"),
            Self::Tombstone { version } => write!(f, "tombstone {}", Version(version)),
            Self::Live {
                version,
                expires_at_ms,
                spilled,
            } => write!(
                f,
                "live {}{}{}",
                Version(version),
                Expiry(expires_at_ms),
                if spilled { ", spilled" } else { "" }
            ),
            Self::Lapsed {
                version,
                expires_at_ms,
                cause,
            } => write!(
                f,
                "lapsed {}{}, {cause:?}",
                Version(version),
                Expiry(expires_at_ms)
            ),
        }
    }
}

impl fmt::Display for ReadSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let outcome = |hit: bool| if hit { "hit" } else { "miss" };
        match *self {
            Self::Local { hit } => write!(f, "this node, {}", outcome(hit)),
            Self::Owner { node, hit } => write!(f, "owner {node}, {}", outcome(hit)),
            Self::Unavailable => f.write_str("unavailable"),
        }
    }
}

impl fmt::Display for Residency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.owns { "owned" } else { "not owned" })?;
        if let Some(since) = self.releasing_for {
            write!(f, ", releasing for {since:?}")?;
        }
        for (set, mark) in [
            (self.cold_marked, "cold"),
            (self.unsettled, "unsettled"),
            (self.unverified, "unverified"),
            (self.stale, "stale"),
        ] {
            if set {
                write!(f, ", {mark}")?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for ProbeAnswer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Held(rec) => write!(
                f,
                "held {}{}, reads {:?}",
                Version(rec.version),
                Expiry(rec.expires_at_ms),
                rec.reads
            ),
            Self::Miss => f.write_str("miss"),
            Self::StaleView {
                responder_view_hash,
            } => write!(f, "stale view {responder_view_hash:016x}"),
            Self::Declined => f.write_str("declined"),
            Self::Unreached(why) => write!(f, "unreached, {why:?}"),
        }
    }
}

impl fmt::Display for ReadExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "cache {} on node {}, {:?}, part {}/{}, at {} ms",
            self.cache,
            self.node,
            self.mode,
            self.part.bucket(),
            self.part.part(),
            self.at_ms
        )?;
        writeln!(f, "local: {}", self.local)?;
        write!(f, "source: {}", self.source)?;
        let Some(read) = &self.distributed else {
            return Ok(());
        };
        write!(f, "\nview: {:016x}", read.view_hash)?;
        if let Some(moved) = read.view_moved_to {
            write!(f, ", moved to {moved:016x}")?;
        }
        write!(f, "\nowners:")?;
        for owner in &read.owners {
            write!(f, " {owner}")?;
        }
        write!(
            f,
            "\nresidency: {}\nlocal read: {:?}\nserves peers: {:?}",
            read.residency, read.local_read, read.serves_peers
        )?;
        for probe in &read.probes {
            write!(f, "\nprobe {}: {}", probe.node, probe.answer)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::io;

    use bytes::Bytes;

    use super::*;

    fn hlc(wall_ms: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 2,
            node: NodeId::from(7u64),
        }
    }

    fn record(value: Option<&'static [u8]>, expires_at_ms: Option<u64>) -> WireRecord {
        WireRecord {
            key: Bytes::from_static(b"k"),
            value: value.map(Bytes::from_static),
            ver: hlc(10),
            expires_at_ms,
        }
    }

    #[test]
    fn dead_record_is_a_tombstone_or_an_expiry_at_or_before_now() {
        assert_eq!(dead_record(&record(None, None), 100), Some(Reads::Deleted));
        assert_eq!(
            dead_record(&record(None, Some(500)), 100),
            Some(Reads::Deleted),
            "a tombstone with an expiry reads deleted"
        );
        assert_eq!(dead_record(&record(Some(b"v"), None), 100), None);
        assert_eq!(
            dead_record(&record(Some(b"v"), Some(100)), 100),
            Some(Reads::Expired),
            "an expiry equal to now has passed"
        );
        assert_eq!(dead_record(&record(Some(b"v"), Some(101)), 100), None);
        assert_eq!(
            dead_record(&record(Some(b"v"), Some(99)), 100),
            Some(Reads::Expired)
        );
    }

    #[test]
    fn classify_probe_maps_every_reply_and_error_kind() {
        let io_err = |kind: io::ErrorKind| Some(Err(CodecError::Io(io::Error::new(kind, "x"))));
        let held = classify_probe(
            Some(Ok(FetchOutcome::Found(Some(record(Some(b"v"), Some(9)))))),
            |rec| {
                assert_eq!(rec.ver, hlc(10), "reads judges the sent record");
                Reads::Undecodable
            },
        );
        assert_eq!(
            held,
            ProbeAnswer::Held(ProbedRecord {
                version: hlc(10),
                expires_at_ms: Some(9),
                reads: Reads::Undecodable,
            })
        );
        let unused = |_: &WireRecord| -> Reads { panic!("only a sent record is judged") };
        let cases = [
            (Some(Ok(FetchOutcome::Found(None))), ProbeAnswer::Miss),
            (
                Some(Ok(FetchOutcome::Stale {
                    responder_view_hash: 42,
                })),
                ProbeAnswer::StaleView {
                    responder_view_hash: 42,
                },
            ),
            (Some(Ok(FetchOutcome::Declined)), ProbeAnswer::Declined),
            (None, ProbeAnswer::Unreached(Unreached::TimedOut)),
            (
                io_err(io::ErrorKind::NotFound),
                ProbeAnswer::Unreached(Unreached::NotAMember),
            ),
            (
                io_err(io::ErrorKind::Unsupported),
                ProbeAnswer::Unreached(Unreached::ProtocolTooOld),
            ),
            (
                io_err(io::ErrorKind::TimedOut),
                ProbeAnswer::Unreached(Unreached::TimedOut),
            ),
            (
                io_err(io::ErrorKind::ConnectionRefused),
                ProbeAnswer::Unreached(Unreached::Io(io::ErrorKind::ConnectionRefused)),
            ),
            (
                Some(Err(CodecError::Postcard(
                    postcard::Error::DeserializeUnexpectedEnd,
                ))),
                ProbeAnswer::Unreached(Unreached::Codec),
            ),
            (
                Some(Err(CodecError::MalformedFrame("short"))),
                ProbeAnswer::Unreached(Unreached::Codec),
            ),
            (
                Some(Err(CodecError::FrameTooLarge { size: 2, limit: 1 })),
                ProbeAnswer::Unreached(Unreached::Codec),
            ),
        ];
        for (reply, expected) in cases {
            assert_eq!(classify_probe(reply, unused), expected);
        }
    }

    #[test]
    fn answers_is_true_only_for_a_held_record_or_a_miss() {
        let held = ProbeAnswer::Held(ProbedRecord {
            version: hlc(1),
            expires_at_ms: None,
            reads: Reads::Deleted,
        });
        assert!(held.answers(), "a held tombstone still answers");
        assert!(ProbeAnswer::Miss.answers());
        assert!(
            !ProbeAnswer::StaleView {
                responder_view_hash: 1
            }
            .answers()
        );
        assert!(!ProbeAnswer::Declined.answers());
        assert!(!ProbeAnswer::Unreached(Unreached::TimedOut).answers());
    }

    #[test]
    fn a_local_record_is_live_only_when_a_read_returns_it_and_held_when_served() {
        let records = [
            (LocalRecord::Absent, false, false),
            (LocalRecord::Tombstone { version: hlc(1) }, false, true),
            (
                LocalRecord::Live {
                    version: hlc(1),
                    expires_at_ms: None,
                    spilled: false,
                },
                true,
                true,
            ),
            (
                LocalRecord::Live {
                    version: hlc(1),
                    expires_at_ms: Some(5),
                    spilled: true,
                },
                true,
                true,
            ),
            (
                LocalRecord::Lapsed {
                    version: hlc(1),
                    expires_at_ms: Some(5),
                    cause: Lapse::Expired,
                },
                false,
                false,
            ),
            (
                LocalRecord::Lapsed {
                    version: hlc(1),
                    expires_at_ms: None,
                    cause: Lapse::Idle,
                },
                false,
                false,
            ),
        ];
        for (record, live, held) in records {
            assert_eq!(record.is_live(), live, "{record:?}");
            assert_eq!(record.is_held(), held, "{record:?}");
        }
    }

    #[test]
    fn a_read_source_returns_a_value_only_for_a_hit() {
        let owner = NodeId::from(3u64);
        assert!(ReadSource::Local { hit: true }.returns_value());
        assert!(!ReadSource::Local { hit: false }.returns_value());
        assert!(
            ReadSource::Owner {
                node: owner,
                hit: true
            }
            .returns_value()
        );
        assert!(
            !ReadSource::Owner {
                node: owner,
                hit: false
            }
            .returns_value()
        );
        assert!(!ReadSource::Unavailable.returns_value());
    }

    /// One explanation per shape: a `Mode::Distributed` one with every
    /// line and mark set, and a `Mode::Local` one.
    fn sample_explanations() -> (ReadExplanation, ReadExplanation) {
        let (n1, n2, n3) = (NodeId::from(1u64), NodeId::from(2u64), NodeId::from(3u64));
        let distributed = ReadExplanation {
            cache: SmolStr::new("users"),
            node: n1,
            mode: Mode::distributed(),
            part: PartId::new(513, 9),
            at_ms: 1_000,
            local: LocalRecord::Lapsed {
                version: hlc(900),
                expires_at_ms: Some(950),
                cause: Lapse::Expired,
            },
            source: ReadSource::Owner {
                node: n2,
                hit: true,
            },
            distributed: Some(DistributedRead {
                view_hash: 0xab,
                view_moved_to: Some(0xcd),
                owners: vec![n2, n1, n3],
                residency: Residency {
                    owns: true,
                    releasing_for: Some(Duration::from_millis(5)),
                    cold_marked: true,
                    unsettled: true,
                    unverified: true,
                    stale: true,
                },
                local_read: LocalRead::Distrusted,
                serves_peers: ServeVerdict::DeclineDistrusted,
                probes: vec![
                    OwnerProbe {
                        node: n2,
                        answer: ProbeAnswer::Held(ProbedRecord {
                            version: hlc(990),
                            expires_at_ms: None,
                            reads: Reads::Value,
                        }),
                    },
                    OwnerProbe {
                        node: n3,
                        answer: ProbeAnswer::StaleView {
                            responder_view_hash: 0xef,
                        },
                    },
                ],
            }),
        };
        let local = ReadExplanation {
            cache: SmolStr::new("sessions"),
            node: n1,
            mode: Mode::Local,
            part: PartId::new(0, 0),
            at_ms: 2_000,
            local: LocalRecord::Live {
                version: hlc(1_500),
                expires_at_ms: Some(3_000),
                spilled: true,
            },
            source: ReadSource::Local { hit: true },
            distributed: None,
        };
        (distributed, local)
    }

    #[test]
    fn display_pins_every_line() {
        let (distributed, local) = sample_explanations();
        assert_eq!(
            distributed.to_string(),
            "cache users on node 0000000000000001, Distributed { owners: 2 }, part 513/9, at 1000 ms\n\
             local: lapsed 900.2@0000000000000007, expires at 950 ms, Expired\n\
             source: owner 0000000000000002, hit\n\
             view: 00000000000000ab, moved to 00000000000000cd\n\
             owners: 0000000000000002 0000000000000001 0000000000000003\n\
             residency: owned, releasing for 5ms, cold, unsettled, unverified, stale\n\
             local read: Distrusted\n\
             serves peers: DeclineDistrusted\n\
             probe 0000000000000002: held 990.2@0000000000000007, reads Value\n\
             probe 0000000000000003: stale view 00000000000000ef"
        );
        assert_eq!(
            local.to_string(),
            "cache sessions on node 0000000000000001, Local, part 0/0, at 2000 ms\n\
             local: live 1500.2@0000000000000007, expires at 3000 ms, spilled\n\
             source: this node, hit"
        );
    }

    #[test]
    fn display_names_every_variant() {
        let version = hlc(4);
        let seen: HashSet<String> = [
            LocalRecord::Absent.to_string(),
            LocalRecord::Tombstone { version }.to_string(),
            LocalRecord::Live {
                version,
                expires_at_ms: None,
                spilled: false,
            }
            .to_string(),
            LocalRecord::Lapsed {
                version,
                expires_at_ms: None,
                cause: Lapse::Idle,
            }
            .to_string(),
            ReadSource::Local { hit: false }.to_string(),
            ReadSource::Unavailable.to_string(),
            Residency::new(false).to_string(),
            ProbeAnswer::Miss.to_string(),
            ProbeAnswer::Declined.to_string(),
            ProbeAnswer::Unreached(Unreached::Io(io::ErrorKind::ConnectionReset)).to_string(),
        ]
        .into_iter()
        .collect();
        let expected: HashSet<String> = [
            "absent",
            "tombstone 4.2@0000000000000007",
            "live 4.2@0000000000000007",
            "lapsed 4.2@0000000000000007, Idle",
            "this node, miss",
            "unavailable",
            "not owned",
            "miss",
            "declined",
            "unreached, Io(ConnectionReset)",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert_eq!(seen, expected);
    }
}
