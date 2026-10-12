//! A test node's answer to `explain <key>`, read into types the interface
//! draws.
//!
//! `sundog-testnode` answers `explain <key>` on its control port with one
//! line of JSON: `Cache::explain` of the key on its `"it"` cache. The lens
//! mirrors that reply here instead of sharing a type, because
//! `ReadExplanation` and every type under it are `#[non_exhaustive]` and
//! carry no serde impls. The four files under `tests/fixtures/explain/` hold
//! the node's encoder output, and tests on both sides read them.
//!
//! The parser is tolerant. It ignores fields it does not know, defaults a
//! field that does not apply to a record, source or answer kind, and reads a
//! token it has no name for as `Other` with the token verbatim. A line that
//! is not an explanation is a [`ParseError`].
//!
//! The module does no I/O. [`Reading`] is one node's answer, [`Explained`]
//! the answers of every node asked, [`agreement`] states whether those
//! answers match each other and the [`Placement`] the lens computes from
//! gossip, and [`verdict`] puts that in a sentence. A reading holds the
//! reply's text as received, so the interface draws it through [`printable`].

use std::fmt;
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use smol_str::SmolStr;

use crate::key::printable;
use crate::locate::Placement;
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

/// Compares every `Distributed` reading in `explained` with the `placement`
/// the lens computes from gossip. A reading differs when its view hash, its
/// part or its owners in fetch order differ. A node that failed, and a node
/// whose cache is not `Distributed`, takes no part in the comparison.
#[must_use]
pub fn agreement(explained: &Explained, placement: &Placement) -> Agreement {
    let view = format!("{:016x}", placement.view_hash);
    let owners: Vec<String> = placement.owners.iter().map(ToString::to_string).collect();
    let part = (placement.part.bucket(), placement.part.part());
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

/// What the answers say against each other and against the lens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every `Distributed` answer holds the lens's view and owners.
    Agree {
        /// How many nodes answered.
        nodes: usize,
    },
    /// No answer holds the lens's view: the view moved since the nodes were
    /// asked.
    Moved {
        /// The view the answers used, eight digits.
        from: String,
        /// The lens's view, eight digits.
        to: String,
    },
    /// The nodes hold different views.
    Split {
        /// The nodes in each view, as words.
        groups: String,
    },
    /// The nodes agree on a view but not with the lens on the placement.
    Apart {
        /// The nodes that differ, as words.
        nodes: String,
        /// Whether more than one node differs.
        many: bool,
        /// The lens's view, eight digits.
        view: String,
    },
    /// The nodes explained another cache than the one the block above places
    /// the key in: nothing to compare.
    OtherCache {
        /// The cache the nodes explained.
        asked: String,
        /// The cache the lens placed the key in.
        shown: String,
    },
    /// Nothing to compare, for the reason in words.
    Nothing(&'static str),
}

impl Verdict {
    /// The verdict in a sentence. `after_freeze` says the answers were asked
    /// after the display froze, when asking again cannot move the lens's
    /// view.
    #[must_use]
    pub fn words(&self, after_freeze: bool) -> String {
        match self {
            Self::Agree { nodes: 1 } => "1 node agrees, the lens computes the same".to_owned(),
            Self::Agree { nodes } => {
                format!("{nodes} nodes agree, the lens computes the same")
            }
            Self::Moved { from, to } if after_freeze => {
                format!("the nodes hold {from}, the frozen display holds {to}: Esc, p unfreezes")
            }
            Self::Moved { from, to } => {
                format!("view moved {from} -> {to}: Enter asks again")
            }
            Self::Split { groups } => groups.clone(),
            Self::Apart { nodes, many, view } => format!(
                "{nodes} {} from the lens, which holds view {view}",
                if *many { "differ" } else { "differs" }
            ),
            Self::OtherCache { asked, shown } => format!(
                "the nodes explained cache {asked} and the block above is cache {shown}: \
                 nothing to compare"
            ),
            Self::Nothing(why) => (*why).to_owned(),
        }
    }
}

/// Compares `answers` with `placement`, when the lens placed the key.
#[must_use]
pub fn verdict(answers: &Explained, placement: Option<&Placement>) -> Verdict {
    let Some(placement) = placement else {
        return Verdict::Nothing("the lens has no placement of the key to compare with");
    };
    // Test nodes explain their cache `it`, whatever cache the block places
    // the key in.
    if let Some((_, reading)) = answers
        .readings()
        .find(|(_, reading)| reading.cache != placement.cache.as_str())
    {
        return Verdict::OtherCache {
            asked: printable(&reading.cache),
            shown: printable(&placement.cache),
        };
    }
    let compared = agreement(answers, placement);
    let lens = format!("{:016x}", placement.view_hash);
    let short = |view: &str| printable(view.get(..8).unwrap_or(view));
    let Some(first) = compared.groups.first() else {
        return Verdict::Nothing("no node gave a Distributed reading to compare");
    };
    if compared.groups.iter().all(|group| group.view != lens) {
        return Verdict::Moved {
            from: short(&first.view),
            to: short(&lens),
        };
    }
    if compared.groups.len() > 1 {
        let groups: Vec<String> = compared
            .groups
            .iter()
            .map(|group| {
                let nodes: Vec<String> = group.nodes.iter().map(|node| printable(node)).collect();
                format!(
                    "{} {} {}",
                    nodes.join(" "),
                    if nodes.len() == 1 { "holds" } else { "hold" },
                    short(&group.view)
                )
            })
            .collect();
        return Verdict::Split {
            groups: groups.join(", "),
        };
    }
    if compared.apart.is_empty() {
        let nodes = compared.groups.iter().map(|group| group.nodes.len()).sum();
        return Verdict::Agree { nodes };
    }
    let nodes: Vec<String> = compared.apart.iter().map(|node| printable(node)).collect();
    Verdict::Apart {
        many: nodes.len() > 1,
        nodes: nodes.join(" "),
        view: short(&lens),
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
mod tests;
