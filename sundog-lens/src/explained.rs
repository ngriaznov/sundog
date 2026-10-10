//! A test node's answer to `explain <key>`, read into types the interface
//! draws.
//!
//! `sundog-testnode` answers `explain <key>` on its control port with one
//! line of JSON: `Cache::explain` of the key on its `"it"` cache. The lens
//! mirrors that reply here instead of sharing a type, because
//! `ReadExplanation` and every type under it are `#[non_exhaustive]` and
//! carry no serde impls. The three files under `tests/fixtures/explain/` hold
//! the node's encoder output, and tests on both sides read them.
//!
//! The parser is tolerant. It ignores fields it does not know, defaults a
//! field that does not apply to a record, source or answer kind, and reads a
//! token it has no name for as `Other` with the token verbatim. A line that
//! is not an explanation is a [`ParseError`].
//!
//! The module does no I/O. [`Reading`] is one node's answer, [`Explained`]
//! the answers of every node asked, and [`agreement`] states whether those
//! answers match each other and the placement [`locate`](crate::locate::locate)
//! computes from gossip. A reading holds the reply's text as received, so the
//! interface draws it through [`printable`].

use std::fmt;
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use smol_str::SmolStr;

use crate::key::printable;
use crate::locate::Located;
use crate::ui::text;

/// How many characters of a reply a [`ParseError`] quotes.
const QUOTE_CHARS: usize = 40;

/// Defines a mirror enum of one lowercase reply token: a variant per known
/// token, `Other` for any other token, `From<String>` to read it and
/// `describe` to put it in words.
macro_rules! tokens {
    ($(
        $(#[$meta:meta])*
        $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident = $token:literal => $words:literal,)+
        }
    )+) => {$(
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
        #[serde(from = "String")]
        pub enum $name {
            $($(#[$variant_meta])* $variant,)+
            /// A token this build has no name for, verbatim.
            Other(String),
        }

        impl From<String> for $name {
            fn from(token: String) -> Self {
                match token.as_str() {
                    $($token => Self::$variant,)+
                    _ => Self::Other(token),
                }
            }
        }

        impl $name {
            /// The token in words; an unknown token reads verbatim, with
            /// every character the interface cannot draw replaced.
            #[must_use]
            pub fn describe(&self) -> String {
                match self {
                    $(Self::$variant => $words.to_owned(),)+
                    Self::Other(token) => printable(token),
                }
            }
        }
    )+};
}

tokens! {
    /// What a node's own copy makes of a fetch.
    LocalRead {
        /// The node answers the fetch with its live entry.
        Hit = "hit" => "hit",
        /// The node answers the fetch with a miss.
        Miss = "miss" => "miss",
        /// The node does not own the part, so the fetch asks the owners.
        NotOwner = "not_owner" => "not an owner",
        /// The node owns the part but does not trust a local hit, so the
        /// fetch asks the owners.
        Distrusted = "distrusted" => "distrusted",
        /// The node owns the part but it is cold, so a miss says nothing and
        /// the fetch asks the owners.
        ColdMiss = "cold_miss" => "cold miss",
    }

    /// What a node answers a peer's fetch.
    ServeVerdict {
        /// The node sends the record it holds.
        Serve = "serve" => "serves",
        /// The node holds nothing and its view differs from the asker's.
        Stale = "stale" => "stale view",
        /// The node holds nothing, on an equal view, in a warm part.
        Miss = "miss" => "miss",
        /// The node declines because its copy of the part is distrusted.
        DeclineDistrusted = "decline_distrusted" => "declines (distrusted)",
        /// The node declines because the part is cold.
        DeclineCold = "decline_cold" => "declines (cold)",
    }

    /// What a fetch makes of an owner's record.
    Reads {
        /// The value.
        Value = "value" => "a value",
        /// A miss: the record is a tombstone.
        Deleted = "deleted" => "a miss: deleted",
        /// A miss: the record's expiry passed.
        Expired = "expired" => "a miss: expired",
        /// A miss: the value does not decode as the cache's value type.
        Undecodable = "undecodable" => "a miss: undecodable",
    }

    /// Why a read no longer returns a record the node still holds.
    Lapse {
        /// The record's expiry passed.
        Expired = "expired" => "expired",
        /// The record went idle.
        Idle = "idle" => "idle",
    }
}

/// Why an owner gave no answer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub enum Why {
    /// The owner is not in the node's mesh.
    NotAMember,
    /// The owner speaks a protocol too old for the fetch.
    ProtocolTooOld,
    /// The owner did not answer within the fetch timeout.
    TimedOut,
    /// An I/O error, named by its kind: `ConnectionRefused`.
    Io(String),
    /// The reply did not decode.
    Codec,
    /// A token this build has no name for, verbatim.
    Other(String),
}

impl From<String> for Why {
    fn from(token: String) -> Self {
        match token.as_str() {
            "not_a_member" => Self::NotAMember,
            "protocol_too_old" => Self::ProtocolTooOld,
            "timed_out" => Self::TimedOut,
            "codec" => Self::Codec,
            _ => match token.strip_prefix("io:") {
                Some(kind) => Self::Io(kind.to_owned()),
                None => Self::Other(token),
            },
        }
    }
}

impl Why {
    /// The reason in words; an unknown token reads verbatim, with every
    /// character the interface cannot draw replaced.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::NotAMember => "not a member of the mesh".to_owned(),
            Self::ProtocolTooOld => "protocol too old".to_owned(),
            Self::TimedOut => "timed out".to_owned(),
            Self::Io(kind) => format!("io error ({})", printable(kind)),
            Self::Codec => "codec error".to_owned(),
            Self::Other(token) => printable(token),
        }
    }
}

/// What a node stores for a key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "WireLocal")]
pub enum Local {
    /// The node holds no record.
    Absent,
    /// The node holds a tombstone.
    Tombstone {
        /// The tombstone's version, `wall_ms.logical@node`.
        version: String,
    },
    /// The node holds a live entry.
    Live {
        /// The entry's version, `wall_ms.logical@node`.
        version: String,
        /// When the entry expires, in epoch milliseconds; `None` for never.
        expires_at_ms: Option<u64>,
        /// Whether the value is in the spill tier.
        spilled: bool,
    },
    /// The node holds an entry a read no longer returns.
    Lapsed {
        /// The entry's version, `wall_ms.logical@node`.
        version: String,
        /// The entry's expiry, in epoch milliseconds; `None` for none.
        expires_at_ms: Option<u64>,
        /// Why a read no longer returns it.
        cause: Lapse,
    },
    /// A kind this build has no name for, verbatim.
    Other(String),
}

/// The wire form of [`Local`]: a kind and the fields that kind carries.
#[derive(Deserialize)]
struct WireLocal {
    kind: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    expires_at_ms: Option<u64>,
    #[serde(default)]
    spilled: bool,
    #[serde(default)]
    cause: String,
}

impl From<WireLocal> for Local {
    fn from(wire: WireLocal) -> Self {
        match wire.kind.as_str() {
            "absent" => Self::Absent,
            "tombstone" => Self::Tombstone {
                version: wire.version,
            },
            "live" => Self::Live {
                version: wire.version,
                expires_at_ms: wire.expires_at_ms,
                spilled: wire.spilled,
            },
            "lapsed" => Self::Lapsed {
                version: wire.version,
                expires_at_ms: wire.expires_at_ms,
                cause: Lapse::from(wire.cause),
            },
            _ => Self::Other(wire.kind),
        }
    }
}

/// Where a fetch takes its answer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "WireSource")]
pub enum Source {
    /// The node answers from its own copy.
    Local {
        /// Whether the fetch returns a value.
        hit: bool,
    },
    /// The first owner whose probe answers.
    Owner {
        /// The owner's node id, 16 hex digits.
        node: String,
        /// Whether the fetch returns a value.
        hit: bool,
    },
    /// No owner answers, so the fetch fails.
    Unavailable,
    /// A kind this build has no name for, verbatim.
    Other(String),
}

/// The wire form of [`Source`]: a kind and the fields that kind carries.
#[derive(Deserialize)]
struct WireSource {
    kind: String,
    #[serde(default)]
    node: String,
    #[serde(default)]
    hit: bool,
}

impl From<WireSource> for Source {
    fn from(wire: WireSource) -> Self {
        match wire.kind.as_str() {
            "local" => Self::Local { hit: wire.hit },
            "owner" => Self::Owner {
                node: wire.node,
                hit: wire.hit,
            },
            "unavailable" => Self::Unavailable,
            _ => Self::Other(wire.kind),
        }
    }
}

/// A node's residency marks for one part.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent residency marks, one reply field each"
)]
pub struct Residency {
    /// Whether the node's ownership view names it an owner of the part.
    pub owns: bool,
    /// How long ago the disown grace began, in milliseconds, while the node
    /// still holds a part it no longer owns.
    #[serde(default)]
    pub releasing_ms: Option<u64>,
    /// Whether rebalance marked the part cold.
    #[serde(default)]
    pub cold_marked: bool,
    /// Whether the part lies outside the last settled view and no pull or
    /// verification has served it since.
    #[serde(default)]
    pub unsettled: bool,
    /// Whether a warm spill-tier reopen replayed the part with no co-owner
    /// check yet.
    #[serde(default)]
    pub unverified: bool,
    /// Whether the node owns the part again while it holds a copy from
    /// owning it before.
    #[serde(default)]
    pub stale: bool,
}

/// One other owner's answer to the key's fetch.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "WireProbe")]
pub struct Probe {
    /// The owner asked, 16 hex digits.
    pub node: String,
    /// What it answered.
    pub answer: Answer,
}

/// What an owner answered to a probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// The owner sends its record.
    Held {
        /// The record's version, `wall_ms.logical@node`.
        version: String,
        /// When the record expires, in epoch milliseconds; `None` for never.
        expires_at_ms: Option<u64>,
        /// What a fetch makes of the record.
        reads: Reads,
    },
    /// The owner holds nothing, on an equal view, in a warm part.
    Miss,
    /// The owner holds nothing and its view differs.
    StaleView {
        /// The owner's view hash, 16 hex digits.
        responder_view: String,
    },
    /// The owner cannot vouch for the key.
    Declined,
    /// The owner gave no answer.
    Unreached {
        /// Why not.
        why: Why,
    },
    /// An answer this build has no name for, verbatim.
    Other(String),
}

impl Answer {
    /// The answer in words: `held`, `miss`, `stale view`, `declined` or
    /// `unreached (timed out)`; an unknown answer reads verbatim, with every
    /// character the interface cannot draw replaced.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Held { .. } => "held".to_owned(),
            Self::Miss => "miss".to_owned(),
            Self::StaleView { .. } => "stale view".to_owned(),
            Self::Declined => "declined".to_owned(),
            Self::Unreached { why } => format!("unreached ({})", why.describe()),
            Self::Other(token) => printable(token),
        }
    }
}

/// The wire form of [`Probe`]: an answer and the fields that answer carries.
#[derive(Deserialize)]
struct WireProbe {
    node: String,
    answer: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    expires_at_ms: Option<u64>,
    #[serde(default)]
    reads: String,
    #[serde(default)]
    responder_view: String,
    #[serde(default)]
    why: String,
}

impl From<WireProbe> for Probe {
    fn from(wire: WireProbe) -> Self {
        let answer = match wire.answer.as_str() {
            "held" => Answer::Held {
                version: wire.version,
                expires_at_ms: wire.expires_at_ms,
                reads: Reads::from(wire.reads),
            },
            "miss" => Answer::Miss,
            "stale_view" => Answer::StaleView {
                responder_view: wire.responder_view,
            },
            "declined" => Answer::Declined,
            "unreached" => Answer::Unreached {
                why: Why::from(wire.why),
            },
            _ => Answer::Other(wire.answer),
        };
        Self {
            node: wire.node,
            answer,
        }
    }
}

/// How a `Distributed` fetch of the key decides on the answering node, and
/// what each other owner answers.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Distributed {
    /// The hash of the ownership view every verdict and probe used, 16 hex
    /// digits.
    pub view: String,
    /// The hash of the node's view after the probes, when it moved.
    #[serde(default)]
    pub view_moved_to: Option<String>,
    /// The key's owners in fetch order, 16 hex digits each.
    pub owners: Vec<String>,
    /// The node's residency marks for the key's part.
    pub residency: Residency,
    /// What the node's own copy makes of a fetch.
    pub local_read: LocalRead,
    /// What the node answers a peer's fetch.
    pub serves_peers: ServeVerdict,
    /// Each other owner's answer, in owner order.
    #[serde(default)]
    pub probes: Vec<Probe>,
}

/// One node's answer to `explain <key>`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Reading {
    /// The cache's name.
    pub cache: String,
    /// The node that answers, 16 hex digits.
    pub node: String,
    /// The cache's mode: `local`, `invalidation`, `replicated` or
    /// `distributed:<owners>`.
    pub mode: String,
    /// The key's bucket.
    pub bucket: u16,
    /// The key's part within its bucket.
    pub part: u8,
    /// The node's clock, in epoch milliseconds, when it read its record.
    pub at_ms: u64,
    /// What the node stores for the key.
    pub local: Local,
    /// Where a fetch takes its answer.
    pub source: Source,
    /// How a `Distributed` fetch decides; `None` in every other mode.
    #[serde(default)]
    pub distributed: Option<Distributed>,
}

/// Why a reply is not an explanation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    /// The start of the reply, cut to 40 characters and fit to draw.
    pub quote: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reply is not an explanation: {}", self.quote)
    }
}

impl std::error::Error for ParseError {}

/// The start of `line`, fit to draw: `…` ends a reply that is cut.
fn quote_of(line: &str) -> String {
    let line = line.trim();
    if line.is_empty() {
        return "(empty)".to_owned();
    }
    let mut quote = printable(&line.chars().take(QUOTE_CHARS).collect::<String>());
    if line.chars().nth(QUOTE_CHARS).is_some() {
        quote.push('…');
    }
    quote
}

/// Reads the reply line of an `explain` command.
///
/// # Errors
///
/// Returns a [`ParseError`] quoting the start of `line` when it is not JSON
/// or lacks a field every explanation carries.
pub fn parse_reply(line: &str) -> Result<Reading, ParseError> {
    serde_json::from_str(line).map_err(|_| ParseError {
        quote: quote_of(line),
    })
}

/// What one asked node answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The node explained the key.
    Read(Box<Reading>),
    /// The node gave no explanation, for the reason in words.
    Failed(String),
}

/// One asked node and its answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAnswer {
    /// The node's slot label.
    pub label: SmolStr,
    /// What it answered.
    pub outcome: Outcome,
}

impl NodeAnswer {
    /// The node's reading, when it answered with one.
    #[must_use]
    pub fn reading(&self) -> Option<&Reading> {
        match &self.outcome {
            Outcome::Read(reading) => Some(reading),
            Outcome::Failed(_) => None,
        }
    }
}

/// The nodes that hold one ownership view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewGroup {
    /// The view hash, 16 hex digits.
    pub view: String,
    /// The slot labels of the nodes that hold it, in slot order.
    pub nodes: Vec<SmolStr>,
}

/// The answers to one `explain` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Explained {
    /// The request these answer.
    pub id: u64,
    /// The key asked about: the text after `explain `.
    pub key: String,
    /// When the nodes were asked.
    pub asked: SystemTime,
    /// One answer per node asked, in slot order.
    pub nodes: Vec<NodeAnswer>,
}

impl Explained {
    /// The nodes that answered with a reading, by slot label, in slot order.
    pub fn readings(&self) -> impl Iterator<Item = (&SmolStr, &Reading)> {
        self.nodes
            .iter()
            .filter_map(|answer| answer.reading().map(|reading| (&answer.label, reading)))
    }

    /// The nodes that answered with a `Distributed` reading, grouped by the
    /// view their verdicts and probes used. Groups and the nodes in them
    /// keep slot order.
    #[must_use]
    pub fn views(&self) -> Vec<ViewGroup> {
        let mut groups: Vec<ViewGroup> = Vec::new();
        for (label, reading) in self.readings() {
            let Some(distributed) = &reading.distributed else {
                continue;
            };
            match groups
                .iter_mut()
                .find(|group| group.view == distributed.view)
            {
                Some(group) => group.nodes.push(label.clone()),
                None => groups.push(ViewGroup {
                    view: distributed.view.clone(),
                    nodes: vec![label.clone()],
                }),
            }
        }
        groups
    }

    /// How to name the node whose id is `node` (16 hex digits): its slot
    /// label when it was asked and answered, else the first four digits of
    /// its id.
    #[must_use]
    pub fn label_of(&self, node: &str) -> String {
        self.readings()
            .find(|(_, reading)| reading.node == node)
            .map_or_else(
                || text::short_id(node).to_owned(),
                |(label, _)| label.to_string(),
            )
    }
}

/// Whether the nodes' answers match each other and the lens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agreement {
    /// The answering nodes grouped by the view they used.
    pub groups: Vec<ViewGroup>,
    /// The answering nodes whose view, part or owners differ from the lens's,
    /// in slot order.
    pub apart: Vec<SmolStr>,
}

impl Agreement {
    /// Whether at least one node answered with a `Distributed` reading and
    /// every one holds one view, the lens's, and places the key as the lens
    /// does.
    #[must_use]
    pub fn agrees(&self) -> bool {
        self.groups.len() == 1 && self.apart.is_empty()
    }
}

/// Compares every `Distributed` reading in `explained` with the placement
/// `located` computes from gossip. A reading differs when its view hash, its
/// part or its owners in fetch order differ. A node that failed, and a node
/// whose cache is not `Distributed`, takes no part in the comparison.
#[must_use]
pub fn agreement(explained: &Explained, located: &Located) -> Agreement {
    let view = format!("{:016x}", located.view_hash);
    let owners: Vec<String> = located
        .owners
        .iter()
        .map(|owner| owner.node.to_string())
        .collect();
    let part = (located.part.bucket(), located.part.part());
    let apart = explained
        .readings()
        .filter(|(_, reading)| {
            reading.distributed.as_ref().is_some_and(|distributed| {
                distributed.view != view
                    || distributed.owners != owners
                    || (reading.bucket, reading.part) != part
            })
        })
        .map(|(label, _)| label.clone())
        .collect();
    Agreement {
        groups: explained.views(),
        apart,
    }
}

/// A node's residency marks in words, comma separated, or `none`: `cold`
/// (rebalance marked the part cold), `warming` (an owner whose part lies
/// outside the last settled view and has not been pulled), `unverified`,
/// `stale`, and `releasing 4.2 s` (a part the node no longer owns).
///
/// A node that does not own the part never reads `warming`: once a view
/// settles, every part a node does not own reads unsettled.
#[must_use]
pub fn marks_text(residency: &Residency) -> String {
    let mut marks = Vec::new();
    if let Some(ms) = residency.releasing_ms {
        marks.push(format!("releasing {}", span_text(ms)));
    }
    if residency.cold_marked {
        marks.push("cold".to_owned());
    }
    if residency.unsettled && residency.owns {
        marks.push("warming".to_owned());
    }
    if residency.unverified {
        marks.push("unverified".to_owned());
    }
    if residency.stale {
        marks.push("stale".to_owned());
    }
    if marks.is_empty() {
        "none".to_owned()
    } else {
        marks.join(", ")
    }
}

/// When a record expires, counted from the node's clock at `at_ms`:
/// `expires in 4.2 s`, `expired 4.2 s ago` or `never expires`. Both instants
/// come from the same reply, so a skewed clock on the lens or on another node
/// does not move the text.
#[must_use]
pub fn expiry_text(at_ms: u64, expires_at_ms: Option<u64>) -> String {
    match expires_at_ms {
        None => "never expires".to_owned(),
        Some(at) if at > at_ms => format!("expires in {}", span_text(at - at_ms)),
        Some(at) => format!("expired {} ago", span_text(at_ms - at)),
    }
}

/// `ms` as a span: seconds with one decimal under 100 s, then minutes and
/// seconds, hours and minutes, and days and hours.
fn span_text(ms: u64) -> String {
    let span = Duration::from_millis(ms);
    match span.as_secs() {
        0..100 => text::seconds(span),
        secs @ 100..3_600 => format!("{} min {} s", secs / 60, secs % 60),
        secs @ 3_600..86_400 => format!("{} h {} min", secs / 3_600, secs % 3_600 / 60),
        secs => format!("{} d {} h", secs / 86_400, secs % 86_400 / 3_600),
    }
}

/// Canned answers for the interface's render tests and goldens.
///
/// The module is part of the library, not of the tests, so the integration
/// tests import it; it is not part of the interface's API.
#[doc(hidden)]
pub mod fixture {
    use std::time::SystemTime;

    use smol_str::SmolStr;
    use sundog::observe::MemberStatus;

    use super::{
        Answer, Distributed, Explained, Local, LocalRead, NodeAnswer, Outcome, Probe, Reading,
        Reads, Residency, ServeVerdict, Source,
    };
    use crate::key::KeySpec;
    use crate::locate::locate;
    use crate::model::Model;
    use crate::ui::data;

    /// The node's clock in the fixture replies, in epoch milliseconds.
    pub const AT_MS: u64 = 1_760_054_321_987;

    /// How long before [`AT_MS`] the fixture entry was written, in
    /// milliseconds.
    const WRITTEN_BEFORE_MS: u64 = 21_875;

    /// How long after [`AT_MS`] the fixture entry expires, in milliseconds.
    const EXPIRES_AFTER_MS: u64 = 38_125;

    /// The answers of every live node of `model` to `explain key` about its
    /// `Distributed` cache `it`, as a converged cluster gives them: every
    /// node holds the lens's view and owners, each owner holds the entry and
    /// serves it, and each other node reads it from the first owner. The
    /// request is number `id` and was asked at `asked`.
    ///
    /// # Panics
    ///
    /// Panics when `key` is not a key or the model ranks no `it` cache.
    #[must_use]
    pub fn explained(model: &Model, id: u64, key: &str, asked: SystemTime) -> Explained {
        let spec = KeySpec::parse(key).expect("the fixture key parses");
        let located =
            locate(model, Some("it"), &spec).expect("the fixture model ranks the cache it");
        let view = format!("{:016x}", located.view_hash);
        let owners: Vec<String> = located
            .owners
            .iter()
            .map(|owner| owner.node.to_string())
            .collect();
        let first = owners.first().cloned().unwrap_or_default();
        let version = format!("{}.3@{first}", AT_MS - WRITTEN_BEFORE_MS);
        let expires = Some(AT_MS + EXPIRES_AFTER_MS);
        let held = Answer::Held {
            version: version.clone(),
            expires_at_ms: expires,
            reads: Reads::Value,
        };
        let nodes = data::all_node_rows(model)
            .into_iter()
            .filter(|row| row.status() == MemberStatus::Live)
            .map(|row| {
                let node = row.full_id();
                let owns = owners.contains(&node);
                let probes = owners
                    .iter()
                    .filter(|owner| **owner != node)
                    .map(|owner| Probe {
                        node: owner.clone(),
                        answer: held.clone(),
                    })
                    .collect();
                let reading = Reading {
                    cache: "it".to_owned(),
                    node,
                    mode: format!("distributed:{}", located.owners_per_part),
                    bucket: located.part.bucket(),
                    part: located.part.part(),
                    at_ms: AT_MS,
                    local: if owns {
                        Local::Live {
                            version: version.clone(),
                            expires_at_ms: expires,
                            spilled: false,
                        }
                    } else {
                        Local::Absent
                    },
                    source: if owns {
                        Source::Local { hit: true }
                    } else {
                        Source::Owner {
                            node: first.clone(),
                            hit: true,
                        }
                    },
                    distributed: Some(Distributed {
                        view: view.clone(),
                        view_moved_to: None,
                        owners: owners.clone(),
                        residency: Residency {
                            owns,
                            releasing_ms: None,
                            cold_marked: false,
                            unsettled: !owns,
                            unverified: false,
                            stale: false,
                        },
                        local_read: if owns {
                            LocalRead::Hit
                        } else {
                            LocalRead::NotOwner
                        },
                        serves_peers: if owns {
                            ServeVerdict::Serve
                        } else {
                            ServeVerdict::Miss
                        },
                        probes,
                    }),
                };
                NodeAnswer {
                    label: SmolStr::new(row.label()),
                    outcome: Outcome::Read(Box::new(reading)),
                }
            })
            .collect();
        Explained {
            id,
            key: key.to_owned(),
            asked,
            nodes,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU8;

    use serde_json::{Value, json};
    use sundog::NodeId;
    use sundog::observe::MemberStatus;
    use sundog::store::PartId;

    use crate::key::KeySpec;
    use crate::locate::LocatedOwner;
    use crate::model::derive::Settle;

    use super::*;

    const OWNER_LINE: &str = include_str!("../tests/fixtures/explain/owner.json");
    const NON_OWNER_LINE: &str = include_str!("../tests/fixtures/explain/non_owner.json");
    const CRASHED_OWNER_LINE: &str = include_str!("../tests/fixtures/explain/crashed_owner.json");

    const A: &str = "6f3ac1e29d54b807";
    const B: &str = "c40d9e7a15f2338b";
    const C: &str = "1b88e5d0a7c64f92";
    const VIEW: &str = "5d69e3db4c1a02f7";
    const MOVED: &str = "71c0a2f4e8b3195d";

    fn parse(line: &str) -> Reading {
        parse_reply(line).unwrap_or_else(|error| panic!("{line}: {error}"))
    }

    fn marks(owns: bool) -> Residency {
        Residency {
            owns,
            releasing_ms: None,
            cold_marked: false,
            unsettled: false,
            unverified: false,
            stale: false,
        }
    }

    fn held(version: &str, expires_at_ms: Option<u64>) -> Answer {
        Answer::Held {
            version: version.to_owned(),
            expires_at_ms,
            reads: Reads::Value,
        }
    }

    fn probe(node: &str, answer: Answer) -> Probe {
        Probe {
            node: node.to_owned(),
            answer,
        }
    }

    /// The reading `owner.json` describes: node a, the first owner of `k17`,
    /// holds the live entry itself.
    fn owner_reading() -> Reading {
        let version = format!("1760054300112.3@{C}");
        Reading {
            cache: "it".to_owned(),
            node: A.to_owned(),
            mode: "distributed:2".to_owned(),
            bucket: 305,
            part: 17,
            at_ms: 1_760_054_321_987,
            local: Local::Live {
                version: version.clone(),
                expires_at_ms: Some(1_760_054_360_112),
                spilled: false,
            },
            source: Source::Local { hit: true },
            distributed: Some(Distributed {
                view: VIEW.to_owned(),
                view_moved_to: None,
                owners: vec![A.to_owned(), B.to_owned()],
                residency: marks(true),
                local_read: LocalRead::Hit,
                serves_peers: ServeVerdict::Serve,
                probes: vec![probe(B, held(&version, Some(1_760_054_360_112)))],
            }),
        }
    }

    /// The reading `non_owner.json` describes: node c owns nothing of the
    /// part and reads from the first owner.
    fn non_owner_reading() -> Reading {
        let version = format!("1760054300112.3@{C}");
        Reading {
            node: C.to_owned(),
            at_ms: 1_760_054_322_104,
            local: Local::Absent,
            source: Source::Owner {
                node: A.to_owned(),
                hit: true,
            },
            distributed: Some(Distributed {
                residency: Residency {
                    unsettled: true,
                    ..marks(false)
                },
                local_read: LocalRead::NotOwner,
                serves_peers: ServeVerdict::Miss,
                probes: vec![
                    probe(A, held(&version, Some(1_760_054_360_112))),
                    probe(B, held(&version, Some(1_760_054_360_112))),
                ],
                ..owner_reading()
                    .distributed
                    .expect("the owner is distributed")
            }),
            ..owner_reading()
        }
    }

    /// The reading `crashed_owner.json` describes: node a is gone, so its
    /// probe is refused, and node c's view moved during the call.
    fn crashed_owner_reading() -> Reading {
        Reading {
            node: C.to_owned(),
            bucket: 882,
            part: 37,
            at_ms: 1_760_054_331_640,
            local: Local::Absent,
            source: Source::Owner {
                node: B.to_owned(),
                hit: true,
            },
            distributed: Some(Distributed {
                view_moved_to: Some(MOVED.to_owned()),
                residency: Residency {
                    unsettled: true,
                    ..marks(false)
                },
                local_read: LocalRead::NotOwner,
                serves_peers: ServeVerdict::Miss,
                probes: vec![
                    probe(
                        A,
                        Answer::Unreached {
                            why: Why::Io("ConnectionRefused".to_owned()),
                        },
                    ),
                    probe(B, held(&format!("1760054290456.0@{B}"), None)),
                ],
                ..owner_reading()
                    .distributed
                    .expect("the owner is distributed")
            }),
            ..owner_reading()
        }
    }

    fn answered(label: &str, reading: Reading) -> NodeAnswer {
        NodeAnswer {
            label: SmolStr::new(label),
            outcome: Outcome::Read(Box::new(reading)),
        }
    }

    fn failed(label: &str, why: &str) -> NodeAnswer {
        NodeAnswer {
            label: SmolStr::new(label),
            outcome: Outcome::Failed(why.to_owned()),
        }
    }

    fn explained(nodes: Vec<NodeAnswer>) -> Explained {
        Explained {
            id: 7,
            key: "k17".to_owned(),
            asked: SystemTime::UNIX_EPOCH,
            nodes,
        }
    }

    /// `reading` holding `view` instead.
    fn with_view(mut reading: Reading, view: &str) -> Reading {
        reading
            .distributed
            .as_mut()
            .expect("the reading is distributed")
            .view = view.to_owned();
        reading
    }

    /// The placement of `k17` as the lens computes it: node a, then node b,
    /// in the view the fixtures name.
    fn located() -> Located {
        let owner = |rank: usize, hex: &str, slot: &str| LocatedOwner {
            rank,
            node: hex.parse::<NodeId>().expect("16 hex digits"),
            slot: SmolStr::new(slot),
            status: Some(MemberStatus::Live),
            gossip: None,
            data: None,
        };
        Located {
            cache: SmolStr::new("it"),
            key: KeySpec::parse("k17").expect("k17 parses"),
            part: PartId::new(305, 17),
            owners_per_part: NonZeroU8::new(2).expect("two is nonzero"),
            ranks_parts: true,
            view_hash: 0x5d69_e3db_4c1a_02f7,
            eligible: 3,
            settle: Settle {
                settled: true,
                gossip_only: true,
            },
            conflicted: false,
            owners: vec![owner(1, A, "n1"), owner(2, B, "n2")],
        }
    }

    fn names(labels: &[SmolStr]) -> Vec<&str> {
        labels.iter().map(SmolStr::as_str).collect()
    }

    #[test]
    fn the_fixture_replies_parse_to_the_readings_they_describe() {
        assert_eq!(parse(OWNER_LINE), owner_reading());
        assert_eq!(parse(NON_OWNER_LINE), non_owner_reading());
        assert_eq!(parse(CRASHED_OWNER_LINE), crashed_owner_reading());
    }

    #[test]
    fn every_record_source_and_answer_kind_parses() {
        let local = |value: Value| serde_json::from_value::<Local>(value).expect("a local record");
        assert_eq!(local(json!({"kind": "absent"})), Local::Absent);
        assert_eq!(
            local(json!({"kind": "tombstone", "version": "5.1@x"})),
            Local::Tombstone {
                version: "5.1@x".to_owned()
            }
        );
        assert_eq!(
            local(json!({
                "kind": "live", "version": "5.1@x", "expires_at_ms": 80, "spilled": true
            })),
            Local::Live {
                version: "5.1@x".to_owned(),
                expires_at_ms: Some(80),
                spilled: true,
            }
        );
        for (cause, expected) in [("expired", Lapse::Expired), ("idle", Lapse::Idle)] {
            assert_eq!(
                local(json!({
                    "kind": "lapsed", "version": "5.1@x", "expires_at_ms": 80, "cause": cause
                })),
                Local::Lapsed {
                    version: "5.1@x".to_owned(),
                    expires_at_ms: Some(80),
                    cause: expected,
                }
            );
        }

        let source = |value: Value| serde_json::from_value::<Source>(value).expect("a source");
        assert_eq!(
            source(json!({"kind": "local", "hit": true})),
            Source::Local { hit: true }
        );
        assert_eq!(
            source(json!({"kind": "owner", "node": A, "hit": false})),
            Source::Owner {
                node: A.to_owned(),
                hit: false
            }
        );
        assert_eq!(source(json!({"kind": "unavailable"})), Source::Unavailable);

        let answer = |value: Value| {
            let probe: Probe = serde_json::from_value(value).expect("a probe");
            assert_eq!(probe.node, B);
            probe.answer
        };
        assert_eq!(
            answer(json!({
                "node": B, "answer": "held", "version": "5.1@x",
                "expires_at_ms": null, "reads": "deleted"
            })),
            Answer::Held {
                version: "5.1@x".to_owned(),
                expires_at_ms: None,
                reads: Reads::Deleted,
            }
        );
        assert_eq!(answer(json!({"node": B, "answer": "miss"})), Answer::Miss);
        assert_eq!(
            answer(json!({"node": B, "answer": "stale_view", "responder_view": MOVED})),
            Answer::StaleView {
                responder_view: MOVED.to_owned()
            }
        );
        assert_eq!(
            answer(json!({"node": B, "answer": "declined"})),
            Answer::Declined
        );
        assert_eq!(
            answer(json!({"node": B, "answer": "unreached", "why": "timed_out"})),
            Answer::Unreached { why: Why::TimedOut }
        );
    }

    #[test]
    fn an_unknown_token_parses_as_other_and_renders_verbatim() {
        let line = json!({
            "cache": "it", "node": A, "mode": "distributed:2",
            "bucket": 1, "part": 2, "at_ms": 9,
            "local": {"kind": "other:Pinned"},
            "source": {"kind": "other:Relay"},
            "distributed": {
                "view": VIEW, "owners": [A],
                "residency": {"owns": true},
                "local_read": "other:Warm",
                "serves_peers": "other:Park",
                "probes": [
                    {"node": B, "answer": "other:Gone"},
                    {"node": B, "answer": "held", "version": "1.0@x", "reads": "other:Hash"},
                    {"node": B, "answer": "unreached", "why": "other:Quic"},
                ],
            },
        })
        .to_string();
        let reading = parse(&line);
        assert_eq!(reading.local, Local::Other("other:Pinned".to_owned()));
        assert_eq!(reading.source, Source::Other("other:Relay".to_owned()));
        let distributed = reading.distributed.expect("a distributed section");
        assert_eq!(
            distributed.local_read,
            LocalRead::Other("other:Warm".to_owned())
        );
        assert_eq!(distributed.local_read.describe(), "other:Warm");
        assert_eq!(
            distributed.serves_peers,
            ServeVerdict::Other("other:Park".to_owned())
        );
        assert_eq!(distributed.serves_peers.describe(), "other:Park");
        let answers: Vec<_> = distributed.probes.iter().map(|p| &p.answer).collect();
        assert_eq!(answers[0], &Answer::Other("other:Gone".to_owned()));
        assert_eq!(answers[0].describe(), "other:Gone");
        assert_eq!(
            answers[1],
            &Answer::Held {
                version: "1.0@x".to_owned(),
                expires_at_ms: None,
                reads: Reads::Other("other:Hash".to_owned()),
            }
        );
        assert_eq!(
            answers[2],
            &Answer::Unreached {
                why: Why::Other("other:Quic".to_owned())
            }
        );
        assert_eq!(answers[2].describe(), "unreached (other:Quic)");

        let lapsed = json!({"kind": "lapsed", "version": "1.0@x", "cause": "other:Rot"});
        let local: Local = serde_json::from_value(lapsed).expect("a lapsed record");
        assert_eq!(
            local,
            Local::Lapsed {
                version: "1.0@x".to_owned(),
                expires_at_ms: None,
                cause: Lapse::Other("other:Rot".to_owned()),
            }
        );
    }

    #[test]
    fn missing_optional_fields_default_and_unknown_fields_are_ignored() {
        let bare = json!({
            "cache": "it", "node": A, "mode": "replicated",
            "bucket": 1, "part": 2, "at_ms": 9,
            "local": {"kind": "live", "version": "1.0@x"},
            "source": {"kind": "local"},
            "added_later": {"nested": [1, 2, 3]},
        })
        .to_string();
        let reading = parse(&bare);
        assert_eq!(
            reading.local,
            Local::Live {
                version: "1.0@x".to_owned(),
                expires_at_ms: None,
                spilled: false,
            }
        );
        assert_eq!(reading.source, Source::Local { hit: false });
        assert_eq!(reading.distributed, None);

        let null_expiry = json!({"kind": "live", "version": "1.0@x", "expires_at_ms": null});
        assert_eq!(
            serde_json::from_value::<Local>(null_expiry).expect("a live record"),
            reading.local,
            "a null expiry and an omitted one both mean never"
        );

        let minimal = json!({
            "cache": "it", "node": A, "mode": "distributed:2",
            "bucket": 1, "part": 2, "at_ms": 9,
            "local": {"kind": "absent", "extra": 1},
            "source": {"kind": "unavailable"},
            "distributed": {
                "view": VIEW, "owners": [A, B],
                "residency": {"owns": false, "later": true},
                "local_read": "miss", "serves_peers": "miss",
            },
        });
        let distributed = parse(&minimal.to_string())
            .distributed
            .expect("a distributed section");
        assert_eq!(distributed.view_moved_to, None);
        assert_eq!(distributed.probes, []);
        assert_eq!(distributed.residency, marks(false));

        let without = |pointer: &str, field: &str| {
            let mut reply = minimal.clone();
            reply
                .pointer_mut(pointer)
                .and_then(Value::as_object_mut)
                .expect("an object")
                .remove(field);
            reply.to_string()
        };
        for (pointer, field) in [
            ("", "cache"),
            ("", "node"),
            ("", "mode"),
            ("", "bucket"),
            ("", "part"),
            ("", "at_ms"),
            ("", "local"),
            ("", "source"),
            ("/local", "kind"),
            ("/source", "kind"),
            ("/distributed", "view"),
            ("/distributed", "owners"),
            ("/distributed", "residency"),
            ("/distributed", "local_read"),
            ("/distributed", "serves_peers"),
            ("/distributed/residency", "owns"),
        ] {
            let reply = without(pointer, field);
            assert!(parse_reply(&reply).is_err(), "{field} is required: {reply}");
        }
        for (pointer, field) in [("", "distributed"), ("/distributed", "probes")] {
            let reply = without(pointer, field);
            assert!(parse_reply(&reply).is_ok(), "{field} is optional: {reply}");
        }
    }

    #[test]
    fn a_reply_that_is_not_json_is_a_parse_error_quoting_its_start() {
        let error = parse_reply("not json at all").expect_err("not json");
        assert_eq!(error.quote, "not json at all");
        assert_eq!(
            error.to_string(),
            "reply is not an explanation: not json at all"
        );

        let long = "x".repeat(QUOTE_CHARS + 10);
        let quote = parse_reply(&long).expect_err("not json").quote;
        assert_eq!(quote, format!("{}…", "x".repeat(QUOTE_CHARS)));
        let exact = "y".repeat(QUOTE_CHARS);
        assert_eq!(parse_reply(&exact).expect_err("not json").quote, exact);

        assert_eq!(
            parse_reply("a\u{1b}[2Jé\tb").expect_err("not json").quote,
            "a·[2J··b",
            "control characters and glyphs outside the allowlist read as dots"
        );
        assert_eq!(parse_reply("  ").expect_err("empty").quote, "(empty)");
        assert_eq!(parse_reply("").expect_err("empty").quote, "(empty)");
        assert_eq!(
            parse_reply("[1,2]").expect_err("an array").quote,
            "[1,2]",
            "json that is not an explanation is quoted too"
        );
        assert_eq!(
            parse_reply("{\"a\":1}").expect_err("no fields").quote,
            "{\"a\":1}"
        );
        let error: &dyn std::error::Error = &error;
        assert!(error.source().is_none());
    }

    #[test]
    fn views_group_the_nodes_that_hold_the_same_view() {
        let replicated = Reading {
            distributed: None,
            ..owner_reading()
        };
        let answers = explained(vec![
            answered("n1", owner_reading()),
            answered("n2", with_view(non_owner_reading(), MOVED)),
            failed("n3", "no answer within 3 s"),
            answered("n4", non_owner_reading()),
            answered("n5", replicated),
            answered("n6", crashed_owner_reading()),
        ]);
        assert_eq!(
            answers.views(),
            [
                ViewGroup {
                    view: VIEW.to_owned(),
                    nodes: vec!["n1".into(), "n4".into(), "n6".into()],
                },
                ViewGroup {
                    view: MOVED.to_owned(),
                    nodes: vec!["n2".into()],
                },
            ],
            "slot order; a failed node and a non-distributed reading hold no view"
        );
        let labels: Vec<_> = answers
            .readings()
            .map(|(label, _)| label.as_str())
            .collect();
        assert_eq!(labels, ["n1", "n2", "n4", "n5", "n6"]);
        assert!(answers.nodes[2].reading().is_none());
        assert_eq!(
            answers.nodes[0]
                .reading()
                .map(|reading| reading.node.as_str()),
            Some(A)
        );
        assert_eq!(explained(vec![failed("n1", "x")]).views(), []);
    }

    #[test]
    fn the_fixture_answers_are_a_converged_cluster_that_agrees_with_the_lens() {
        use crate::locate::locate;
        use crate::model::testkit;

        let model = testkit::fixture_model(std::time::Instant::now());
        let asked = SystemTime::UNIX_EPOCH + Duration::from_secs(7);
        let answers = fixture::explained(&model, 4, "k1", asked);
        assert_eq!(
            (answers.id, answers.key.as_str(), answers.asked),
            (4, "k1", asked)
        );
        let labels: Vec<&str> = answers.nodes.iter().map(|a| a.label.as_str()).collect();
        assert_eq!(labels, ["n1", "n2", "n3", "n4", "n5"], "the live nodes");

        let located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
        let owners: Vec<String> = located.owners.iter().map(|o| o.node.to_string()).collect();
        let outcome = agreement(&answers, &located);
        assert!(outcome.agrees(), "{outcome:?}");
        assert_eq!(outcome.groups.len(), 1);
        assert_eq!(outcome.groups[0].nodes.len(), 5);
        for (label, reading) in answers.readings() {
            let dist = reading.distributed.as_ref().expect("distributed");
            assert_eq!(dist.owners, owners, "{label}");
            assert_eq!(
                (reading.bucket, reading.part),
                (located.part.bucket(), located.part.part())
            );
            assert_eq!(reading.mode, "distributed:2");
            let owns = owners.contains(&reading.node);
            assert_eq!(dist.residency.owns, owns, "{label}");
            assert_eq!(
                dist.probes.len(),
                owners.len() - usize::from(owns),
                "{label}"
            );
            assert!(dist.probes.iter().all(|p| p.node != reading.node));
            if owns {
                assert!(matches!(reading.local, Local::Live { .. }), "{label}");
                assert_eq!(reading.source, Source::Local { hit: true });
                assert_eq!(dist.local_read, LocalRead::Hit);
            } else {
                assert_eq!(reading.local, Local::Absent);
                assert_eq!(
                    reading.source,
                    Source::Owner {
                        node: owners[0].clone(),
                        hit: true
                    }
                );
                assert_eq!(dist.local_read, LocalRead::NotOwner);
                assert!(dist.residency.unsettled);
            }
        }
    }

    #[test]
    fn agreement_compares_views_and_owners_with_the_lens() {
        let lens = located();

        let agreeing = explained(vec![
            answered("n1", owner_reading()),
            answered("n2", non_owner_reading()),
            failed("n3", "no answer (connection refused)"),
        ]);
        let result = agreement(&agreeing, &lens);
        assert!(result.agrees(), "{result:?}");
        assert_eq!(result.groups.len(), 1);
        assert_eq!(names(&result.groups[0].nodes), ["n1", "n2"]);
        assert_eq!(names(&result.apart), Vec::<&str>::new());

        let split = explained(vec![
            answered("n1", owner_reading()),
            answered("n2", with_view(non_owner_reading(), MOVED)),
        ]);
        let result = agreement(&split, &lens);
        assert!(!result.agrees());
        assert_eq!(result.groups.len(), 2);
        assert_eq!(names(&result.apart), ["n2"]);

        let all_moved = explained(vec![
            answered("n1", with_view(owner_reading(), MOVED)),
            answered("n2", with_view(non_owner_reading(), MOVED)),
        ]);
        let result = agreement(&all_moved, &lens);
        assert!(
            !result.agrees(),
            "the nodes agree with each other, not the lens"
        );
        assert_eq!(result.groups.len(), 1);
        assert_eq!(names(&result.apart), ["n1", "n2"]);

        let mut reordered = owner_reading();
        reordered
            .distributed
            .as_mut()
            .expect("distributed")
            .owners
            .reverse();
        let result = agreement(&explained(vec![answered("n1", reordered)]), &lens);
        assert_eq!(names(&result.apart), ["n1"], "fetch order counts");
        assert!(!result.agrees());

        let elsewhere = Reading {
            bucket: 306,
            ..owner_reading()
        };
        let result = agreement(&explained(vec![answered("n1", elsewhere)]), &lens);
        assert_eq!(names(&result.apart), ["n1"], "the part counts");

        let other_part = Reading {
            part: 18,
            ..owner_reading()
        };
        let result = agreement(&explained(vec![answered("n1", other_part)]), &lens);
        assert_eq!(names(&result.apart), ["n1"]);

        let replicated = Reading {
            distributed: None,
            ..owner_reading()
        };
        let nobody = explained(vec![answered("n1", replicated), failed("n2", "x")]);
        let result = agreement(&nobody, &lens);
        assert!(result.groups.is_empty() && result.apart.is_empty());
        assert!(!result.agrees(), "nothing was compared");
    }

    #[test]
    fn marks_text_hides_the_unsettled_mark_of_a_non_owner() {
        let unsettled = |owns| Residency {
            unsettled: true,
            ..marks(owns)
        };
        assert_eq!(marks_text(&unsettled(false)), "none");
        assert_eq!(marks_text(&unsettled(true)), "warming");
        let stale = Residency {
            stale: true,
            ..unsettled(false)
        };
        assert_eq!(marks_text(&stale), "stale");
    }

    #[test]
    fn marks_text_names_cold_warming_unverified_stale_and_releasing() {
        assert_eq!(marks_text(&marks(true)), "none");
        assert_eq!(marks_text(&marks(false)), "none");
        let one = |mark: Residency| marks_text(&mark);
        assert_eq!(
            one(Residency {
                cold_marked: true,
                ..marks(true)
            }),
            "cold"
        );
        assert_eq!(
            one(Residency {
                unverified: true,
                ..marks(true)
            }),
            "unverified"
        );
        assert_eq!(
            one(Residency {
                stale: true,
                ..marks(true)
            }),
            "stale"
        );
        assert_eq!(
            one(Residency {
                releasing_ms: Some(4_200),
                ..marks(false)
            }),
            "releasing 4.2 s"
        );
        let everything = Residency {
            owns: true,
            releasing_ms: Some(125_000),
            cold_marked: true,
            unsettled: true,
            unverified: true,
            stale: true,
        };
        assert_eq!(
            marks_text(&everything),
            "releasing 2 min 5 s, cold, warming, unverified, stale"
        );
    }

    #[test]
    fn expiry_text_counts_from_the_nodes_own_clock() {
        assert_eq!(expiry_text(1_000, Some(5_200)), "expires in 4.2 s");
        assert_eq!(expiry_text(5_200, Some(1_000)), "expired 4.2 s ago");
        assert_eq!(expiry_text(1_000, Some(1_000)), "expired 0.0 s ago");
        assert_eq!(expiry_text(1_000, None), "never expires");

        // The text depends on the two instants of one reply and nothing else,
        // so a clock set to any epoch gives the same words.
        let epoch = 1_760_054_321_987;
        assert_eq!(
            expiry_text(epoch, Some(epoch + 4_200)),
            expiry_text(1_000, Some(5_200))
        );
        assert_eq!(
            expiry_text(epoch, Some(epoch + 4_200)),
            "expires in 4.2 s",
            "the reading's own at_ms, not the lens's clock"
        );

        assert_eq!(expiry_text(0, Some(99_900)), "expires in 99.9 s");
        assert_eq!(expiry_text(0, Some(130_000)), "expires in 2 min 10 s");
        assert_eq!(
            expiry_text(0, Some(3 * 3_600_000 + 5 * 60_000)),
            "expires in 3 h 5 min"
        );
        assert_eq!(
            expiry_text(0, Some(2 * 86_400_000 + 7 * 3_600_000)),
            "expires in 2 d 7 h"
        );
        assert!(expiry_text(0, Some(u64::MAX)).starts_with("expires in "));
        assert!(expiry_text(u64::MAX, Some(0)).starts_with("expired "));
    }

    #[test]
    fn describe_covers_every_token() {
        let local_read = [
            ("hit", "hit"),
            ("miss", "miss"),
            ("not_owner", "not an owner"),
            ("distrusted", "distrusted"),
            ("cold_miss", "cold miss"),
        ];
        for (token, words) in local_read {
            let read = LocalRead::from(token.to_owned());
            assert!(!matches!(read, LocalRead::Other(_)), "{token}");
            assert_eq!(read.describe(), words);
        }
        let serves = [
            ("serve", "serves"),
            ("stale", "stale view"),
            ("miss", "miss"),
            ("decline_distrusted", "declines (distrusted)"),
            ("decline_cold", "declines (cold)"),
        ];
        for (token, words) in serves {
            let verdict = ServeVerdict::from(token.to_owned());
            assert!(!matches!(verdict, ServeVerdict::Other(_)), "{token}");
            assert_eq!(verdict.describe(), words);
        }
        let reads = [
            ("value", "a value"),
            ("deleted", "a miss: deleted"),
            ("expired", "a miss: expired"),
            ("undecodable", "a miss: undecodable"),
        ];
        for (token, words) in reads {
            let reads = Reads::from(token.to_owned());
            assert!(!matches!(reads, Reads::Other(_)), "{token}");
            assert_eq!(reads.describe(), words);
        }
        for (token, words) in [("expired", "expired"), ("idle", "idle")] {
            let lapse = Lapse::from(token.to_owned());
            assert!(!matches!(lapse, Lapse::Other(_)), "{token}");
            assert_eq!(lapse.describe(), words);
        }
        let why = [
            ("not_a_member", Why::NotAMember, "not a member of the mesh"),
            ("protocol_too_old", Why::ProtocolTooOld, "protocol too old"),
            ("timed_out", Why::TimedOut, "timed out"),
            (
                "io:ConnectionRefused",
                Why::Io("ConnectionRefused".to_owned()),
                "io error (ConnectionRefused)",
            ),
            ("codec", Why::Codec, "codec error"),
        ];
        for (token, expected, words) in why {
            let why = Why::from(token.to_owned());
            assert_eq!(why, expected);
            assert_eq!(why.describe(), words);
        }

        let answers = [
            (held("1.0@x", None), "held"),
            (Answer::Miss, "miss"),
            (
                Answer::StaleView {
                    responder_view: MOVED.to_owned(),
                },
                "stale view",
            ),
            (Answer::Declined, "declined"),
            (
                Answer::Unreached { why: Why::TimedOut },
                "unreached (timed out)",
            ),
        ];
        for (answer, words) in answers {
            assert_eq!(answer.describe(), words);
        }
    }

    #[test]
    fn a_token_the_interface_cannot_draw_reads_with_dots() {
        let hostile = "other:\u{1b}[2J\n";
        assert_eq!(
            LocalRead::from(hostile.to_owned()).describe(),
            "other:·[2J·"
        );
        assert_eq!(Why::from(hostile.to_owned()).describe(), "other:·[2J·");
        assert_eq!(Why::Io("é\u{7}".to_owned()).describe(), "io error (··)");
        assert_eq!(Answer::Other("é".to_owned()).describe(), "·");
    }

    #[test]
    fn an_owner_that_was_not_asked_is_named_by_its_short_id() {
        let answers = explained(vec![
            answered("n1", owner_reading()),
            failed("n2", "no answer within 3 s"),
            answered("n3", non_owner_reading()),
        ]);
        assert_eq!(answers.label_of(A), "n1");
        assert_eq!(answers.label_of(C), "n3");
        assert_eq!(
            answers.label_of(B),
            "c40d",
            "node b was not asked, so its id stands for it"
        );
        assert_eq!(answers.label_of("ab"), "ab");
        assert_eq!(answers.label_of(""), "");
    }
}
