//! The reply to the `explain <key>` control line: `Cache::explain`'s report
//! on `"it"` as one line of JSON.
//!
//! `ReadExplanation` and every type under it are `#[non_exhaustive]`, carry
//! no serde impls and keep their `Display` layout outside semver, so the
//! test node reads their public fields and writes its own reply type,
//! [`ExplainReply`]. Every id is a 16-digit hex [`NodeId`](sundog::NodeId),
//! every version is `wall_ms.logical@node`, and every variant is a lowercase
//! token. A variant this build does not know becomes `other:` and its
//! `Debug` text with each character outside `[A-Za-z0-9_:.]` replaced by
//! `_`, so a reply never carries whitespace in a token. A field that does
//! not apply to a record, source or answer kind is left out of the line.
//!
//! The reply is compact JSON, so it is one line that starts with `{` and
//! never with `err `. The three files under
//! `sundog-lens/tests/fixtures/explain/` hold the encoder's output for three
//! replies, and a test compares the encoder's bytes with them.

use std::time::Duration;

use serde::Serialize;
use sundog::explain::{
    DistributedRead, Lapse, LocalRead, LocalRecord, OwnerProbe, ProbeAnswer, ReadSource, Reads,
    Residency, ServeVerdict, Unreached,
};
use sundog::{Hlc, Mode, ReadExplanation};

/// Everything one node says about a read of one key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct ExplainReply {
    /// The cache's name.
    pub cache: String,
    /// The node that answers.
    pub node: String,
    /// The cache's mode: `local`, `invalidation`, `replicated` or
    /// `distributed:<owners>`.
    pub mode: String,
    /// The key's bucket.
    pub bucket: u16,
    /// The key's part within its bucket.
    pub part: u8,
    /// The cache's clock when the local record was read.
    pub at_ms: u64,
    /// What the node stores for the key.
    pub local: Local,
    /// Where a fetch takes its answer.
    pub source: Source,
    /// How a `Mode::Distributed` fetch decides; `null` in every other mode.
    pub distributed: Option<Distributed>,
}

/// What a node stores for a key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Local {
    /// `absent`, `tombstone`, `live` or `lapsed`.
    pub kind: String,
    /// The record's version, for every kind but `absent`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// The record's expiry, for `live` and `lapsed`.
    #[serde(skip_serializing_if = "Expiry::is_omitted")]
    pub expires_at_ms: Expiry,
    /// Whether the value is in the spill tier, for `live`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spilled: Option<bool>,
    /// Why a read no longer returns the record, for `lapsed`: `expired` or
    /// `idle`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}

/// Where a fetch of the key takes its answer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Source {
    /// `local`, `owner` or `unavailable`.
    pub kind: String,
    /// The owner whose answer a fetch takes, for `owner`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Whether the fetch returns a value, for `local` and `owner`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hit: Option<bool>,
}

/// How a `Mode::Distributed` fetch of the key decides on the answering
/// node, and what each other owner answers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Distributed {
    /// The hash of the ownership view every verdict and probe used.
    pub view: String,
    /// The hash of the node's view after the probes, when it moved.
    pub view_moved_to: Option<String>,
    /// The key's owners in rendezvous order.
    pub owners: Vec<String>,
    /// The node's residency marks for the key's part.
    pub residency: Marks,
    /// What the node's own copy makes of a fetch: `hit`, `miss`,
    /// `not_owner`, `distrusted` or `cold_miss`.
    pub local_read: String,
    /// What the node answers a peer's fetch: `serve`, `stale`, `miss`,
    /// `decline_distrusted` or `decline_cold`.
    pub serves_peers: String,
    /// Each other owner's answer, in owner order.
    pub probes: Vec<Probe>,
}

/// A node's residency marks for one part.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent residency marks, one JSON field each"
)]
pub(crate) struct Marks {
    /// Whether the node's ownership view names it an owner of the part.
    pub owns: bool,
    /// How long ago the disown grace began, while the node still holds a
    /// part it no longer owns.
    pub releasing_ms: Option<u64>,
    /// Whether rebalance marked the part cold.
    pub cold_marked: bool,
    /// Whether the part lies outside the last settled view and no pull or
    /// verification has served it since.
    pub unsettled: bool,
    /// Whether a warm spill-tier reopen replayed the part with no co-owner
    /// check yet.
    pub unverified: bool,
    /// Whether the node owns the part again while it holds a copy from
    /// owning it before.
    pub stale: bool,
}

/// One other owner's answer to the key's fetch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Probe {
    /// The owner asked.
    pub node: String,
    /// `held`, `miss`, `stale_view`, `declined` or `unreached`.
    pub answer: String,
    /// The record's version, for `held`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// The record's expiry, for `held`.
    #[serde(skip_serializing_if = "Expiry::is_omitted")]
    pub expires_at_ms: Expiry,
    /// What a fetch makes of the record, for `held`: `value`, `deleted`,
    /// `expired` or `undecodable`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reads: Option<String>,
    /// The owner's view hash, for `stale_view`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub responder_view: Option<String>,
    /// Why the owner gave no answer, for `unreached`: `not_a_member`,
    /// `protocol_too_old`, `timed_out`, `io:<Kind>` or `codec`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
}

/// An absolute expiry in epoch milliseconds, written `null` for none and
/// left out of the line for a record kind that has no expiry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub(crate) enum Expiry {
    /// The kind has no expiry field.
    #[default]
    Omitted,
    /// The record never expires.
    Never,
    /// The record expires at this instant.
    At(u64),
}

impl Expiry {
    /// Whether the kind has no expiry field. Serde's skip predicate.
    const fn is_omitted(&self) -> bool {
        matches!(self, Self::Omitted)
    }
}

impl From<Option<u64>> for Expiry {
    fn from(expires_at_ms: Option<u64>) -> Self {
        expires_at_ms.map_or(Self::Never, Self::At)
    }
}

/// `reply` as the one line the control port writes. A reply holds strings,
/// numbers and bools, which always serialize.
pub(crate) fn to_line(reply: &ExplainReply) -> String {
    serde_json::to_string(reply)
        .expect("invariant: a reply of strings, numbers and bools always serializes")
}

/// The reply that reports `explanation`.
pub(crate) fn from_explanation(explanation: &ReadExplanation) -> ExplainReply {
    ExplainReply {
        cache: explanation.cache.to_string(),
        node: explanation.node.to_string(),
        mode: mode_token(explanation.mode),
        bucket: explanation.part.bucket(),
        part: explanation.part.part(),
        at_ms: explanation.at_ms,
        local: local(&explanation.local),
        source: source(&explanation.source),
        distributed: explanation.distributed.as_ref().map(distributed),
    }
}

fn local(record: &LocalRecord) -> Local {
    match *record {
        LocalRecord::Absent => Local {
            kind: "absent".to_string(),
            ..Local::default()
        },
        LocalRecord::Tombstone { version, .. } => Local {
            kind: "tombstone".to_string(),
            version: Some(version_text(version)),
            ..Local::default()
        },
        LocalRecord::Live {
            version,
            expires_at_ms,
            spilled,
            ..
        } => Local {
            kind: "live".to_string(),
            version: Some(version_text(version)),
            expires_at_ms: expires_at_ms.into(),
            spilled: Some(spilled),
            ..Local::default()
        },
        LocalRecord::Lapsed {
            version,
            expires_at_ms,
            cause,
            ..
        } => Local {
            kind: "lapsed".to_string(),
            version: Some(version_text(version)),
            expires_at_ms: expires_at_ms.into(),
            cause: Some(lapse_token(cause)),
            ..Local::default()
        },
        ref unknown => Local {
            kind: other_token(&format!("{unknown:?}")),
            ..Local::default()
        },
    }
}

fn source(source: &ReadSource) -> Source {
    match *source {
        ReadSource::Local { hit, .. } => Source {
            kind: "local".to_string(),
            hit: Some(hit),
            ..Source::default()
        },
        ReadSource::Owner { node, hit, .. } => Source {
            kind: "owner".to_string(),
            node: Some(node.to_string()),
            hit: Some(hit),
        },
        ReadSource::Unavailable => Source {
            kind: "unavailable".to_string(),
            ..Source::default()
        },
        ref unknown => Source {
            kind: other_token(&format!("{unknown:?}")),
            ..Source::default()
        },
    }
}

fn distributed(read: &DistributedRead) -> Distributed {
    Distributed {
        view: hash_text(read.view_hash),
        view_moved_to: read.view_moved_to.map(hash_text),
        owners: read.owners.iter().map(ToString::to_string).collect(),
        residency: marks(&read.residency),
        local_read: local_read_token(read.local_read),
        serves_peers: serve_token(read.serves_peers),
        probes: read.probes.iter().map(probe).collect(),
    }
}

fn marks(residency: &Residency) -> Marks {
    Marks {
        owns: residency.owns,
        releasing_ms: residency.releasing_for.map(millis),
        cold_marked: residency.cold_marked,
        unsettled: residency.unsettled,
        unverified: residency.unverified,
        stale: residency.stale,
    }
}

fn probe(probe: &OwnerProbe) -> Probe {
    probe_answer(probe.node.to_string(), &probe.answer)
}

/// The probe of owner `node` that answered `answer`.
fn probe_answer(node: String, answer: &ProbeAnswer) -> Probe {
    let bare = |token: &str| Probe {
        node,
        answer: token.to_string(),
        ..Probe::default()
    };
    match *answer {
        ProbeAnswer::Held(record) => Probe {
            version: Some(version_text(record.version)),
            expires_at_ms: record.expires_at_ms.into(),
            reads: Some(reads_token(record.reads)),
            ..bare("held")
        },
        ProbeAnswer::Miss => bare("miss"),
        ProbeAnswer::StaleView {
            responder_view_hash,
            ..
        } => Probe {
            responder_view: Some(hash_text(responder_view_hash)),
            ..bare("stale_view")
        },
        ProbeAnswer::Declined => bare("declined"),
        ProbeAnswer::Unreached(why) => Probe {
            why: Some(unreached_token(why)),
            ..bare("unreached")
        },
        ref unknown => bare(&other_token(&format!("{unknown:?}"))),
    }
}

fn mode_token(mode: Mode) -> String {
    match mode {
        Mode::Local => "local".to_string(),
        Mode::Invalidation => "invalidation".to_string(),
        Mode::Replicated => "replicated".to_string(),
        Mode::Distributed { owners } => format!("distributed:{owners}"),
        unknown => other_token(&format!("{unknown:?}")),
    }
}

fn local_read_token(read: LocalRead) -> String {
    match read {
        LocalRead::Hit => "hit".to_string(),
        LocalRead::Miss => "miss".to_string(),
        LocalRead::NotOwner => "not_owner".to_string(),
        LocalRead::Distrusted => "distrusted".to_string(),
        LocalRead::ColdMiss => "cold_miss".to_string(),
        unknown => other_token(&format!("{unknown:?}")),
    }
}

fn serve_token(verdict: ServeVerdict) -> String {
    match verdict {
        ServeVerdict::Serve => "serve".to_string(),
        ServeVerdict::Stale => "stale".to_string(),
        ServeVerdict::Miss => "miss".to_string(),
        ServeVerdict::DeclineDistrusted => "decline_distrusted".to_string(),
        ServeVerdict::DeclineCold => "decline_cold".to_string(),
        unknown => other_token(&format!("{unknown:?}")),
    }
}

fn reads_token(reads: Reads) -> String {
    match reads {
        Reads::Value => "value".to_string(),
        Reads::Deleted => "deleted".to_string(),
        Reads::Expired => "expired".to_string(),
        Reads::Undecodable => "undecodable".to_string(),
        unknown => other_token(&format!("{unknown:?}")),
    }
}

fn unreached_token(why: Unreached) -> String {
    match why {
        Unreached::NotAMember => "not_a_member".to_string(),
        Unreached::ProtocolTooOld => "protocol_too_old".to_string(),
        Unreached::TimedOut => "timed_out".to_string(),
        Unreached::Io(kind) => format!("io:{}", sanitize(&format!("{kind:?}"))),
        Unreached::Codec => "codec".to_string(),
        unknown => other_token(&format!("{unknown:?}")),
    }
}

fn lapse_token(cause: Lapse) -> String {
    match cause {
        Lapse::Expired => "expired".to_string(),
        Lapse::Idle => "idle".to_string(),
        unknown => other_token(&format!("{unknown:?}")),
    }
}

/// `other:` and `debug` through [`sanitize`]: the token for a variant this
/// build has no name for.
fn other_token(debug: &str) -> String {
    format!("other:{}", sanitize(debug))
}

/// `text` with every character outside `[A-Za-z0-9_:.]` replaced by `_`.
fn sanitize(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `wall_ms.logical@node`.
fn version_text(version: Hlc) -> String {
    format!("{}.{}@{}", version.wall_ms, version.logical, version.node)
}

/// A view hash as 16 lowercase hex digits.
fn hash_text(hash: u64) -> String {
    format!("{hash:016x}")
}

/// `duration` in whole milliseconds, saturating.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::num::NonZeroU8;

    use serde_json::{Value, json};
    use sundog::NodeId;

    use super::*;

    const OWNER_LINE: &str = include_str!("../../sundog-lens/tests/fixtures/explain/owner.json");
    const NON_OWNER_LINE: &str =
        include_str!("../../sundog-lens/tests/fixtures/explain/non_owner.json");
    const CRASHED_OWNER_LINE: &str =
        include_str!("../../sundog-lens/tests/fixtures/explain/crashed_owner.json");

    const A: &str = "6f3ac1e29d54b807";
    const B: &str = "c40d9e7a15f2338b";
    const C: &str = "1b88e5d0a7c64f92";
    const VIEW: &str = "5d69e3db4c1a02f7";

    fn marks(owns: bool, unsettled: bool) -> Marks {
        Marks {
            owns,
            unsettled,
            ..Marks::default()
        }
    }

    fn held(node: &str, version: &str, expires_at_ms: Expiry) -> Probe {
        Probe {
            node: node.to_string(),
            answer: "held".to_string(),
            version: Some(version.to_string()),
            expires_at_ms,
            reads: Some("value".to_string()),
            ..Probe::default()
        }
    }

    /// Node a, the first owner of `k17`, holds the live entry itself.
    fn owner_reply() -> ExplainReply {
        let version = format!("1760054300112.3@{C}");
        ExplainReply {
            cache: "it".to_string(),
            node: A.to_string(),
            mode: "distributed:2".to_string(),
            bucket: 305,
            part: 17,
            at_ms: 1_760_054_321_987,
            local: Local {
                kind: "live".to_string(),
                version: Some(version.clone()),
                expires_at_ms: Expiry::At(1_760_054_360_112),
                spilled: Some(false),
                ..Local::default()
            },
            source: Source {
                kind: "local".to_string(),
                hit: Some(true),
                ..Source::default()
            },
            distributed: Some(Distributed {
                view: VIEW.to_string(),
                view_moved_to: None,
                owners: vec![A.to_string(), B.to_string()],
                residency: marks(true, false),
                local_read: "hit".to_string(),
                serves_peers: "serve".to_string(),
                probes: vec![held(B, &version, Expiry::At(1_760_054_360_112))],
            }),
        }
    }

    /// Node c owns no part of `k17`: it stores nothing and a fetch takes the
    /// first owner's record.
    fn non_owner_reply() -> ExplainReply {
        let version = format!("1760054300112.3@{C}");
        ExplainReply {
            cache: "it".to_string(),
            node: C.to_string(),
            mode: "distributed:2".to_string(),
            bucket: 305,
            part: 17,
            at_ms: 1_760_054_322_104,
            local: Local {
                kind: "absent".to_string(),
                ..Local::default()
            },
            source: Source {
                kind: "owner".to_string(),
                node: Some(A.to_string()),
                hit: Some(true),
            },
            distributed: Some(Distributed {
                view: VIEW.to_string(),
                view_moved_to: None,
                owners: vec![A.to_string(), B.to_string()],
                residency: marks(false, true),
                local_read: "not_owner".to_string(),
                serves_peers: "miss".to_string(),
                probes: vec![
                    held(A, &version, Expiry::At(1_760_054_360_112)),
                    held(B, &version, Expiry::At(1_760_054_360_112)),
                ],
            }),
        }
    }

    /// Node c asks the owners of `k42` after node a crashed: a's connection
    /// is refused, b answers, and the view moved while the owners were asked.
    fn crashed_owner_reply() -> ExplainReply {
        let version = format!("1760054290456.0@{B}");
        ExplainReply {
            cache: "it".to_string(),
            node: C.to_string(),
            mode: "distributed:2".to_string(),
            bucket: 882,
            part: 37,
            at_ms: 1_760_054_331_640,
            local: Local {
                kind: "absent".to_string(),
                ..Local::default()
            },
            source: Source {
                kind: "owner".to_string(),
                node: Some(B.to_string()),
                hit: Some(true),
            },
            distributed: Some(Distributed {
                view: VIEW.to_string(),
                view_moved_to: Some("71c0a2f4e8b3195d".to_string()),
                owners: vec![A.to_string(), B.to_string()],
                residency: marks(false, true),
                local_read: "not_owner".to_string(),
                serves_peers: "miss".to_string(),
                probes: vec![
                    Probe {
                        node: A.to_string(),
                        answer: "unreached".to_string(),
                        why: Some("io:ConnectionRefused".to_string()),
                        ..Probe::default()
                    },
                    held(B, &version, Expiry::Never),
                ],
            }),
        }
    }

    #[test]
    fn the_encoder_writes_the_fixture_replies_byte_for_byte() {
        for (name, reply, fixture) in [
            ("owner", owner_reply(), OWNER_LINE),
            ("non_owner", non_owner_reply(), NON_OWNER_LINE),
            ("crashed_owner", crashed_owner_reply(), CRASHED_OWNER_LINE),
        ] {
            assert_eq!(to_line(&reply), fixture.trim_end(), "{name}.json");
        }
    }

    #[test]
    fn every_reply_is_one_line_of_json_that_never_starts_with_err() {
        let hostile = ExplainReply {
            cache: "a\nb\r\"c\\d\u{2028}".to_string(),
            ..owner_reply()
        };
        for reply in [
            owner_reply(),
            non_owner_reply(),
            crashed_owner_reply(),
            hostile.clone(),
        ] {
            let line = to_line(&reply);
            assert!(!line.contains(['\n', '\r']), "{line}");
            assert!(line.starts_with('{'), "{line}");
            assert!(!line.starts_with("err "), "{line}");
            let parsed: Value = serde_json::from_str(&line).expect("the line is JSON");
            assert_eq!(parsed["cache"], reply.cache);
        }
        assert_eq!(
            to_line(&hostile).lines().count(),
            1,
            "an escaped newline stays inside its string"
        );
    }

    #[test]
    fn a_reply_leaves_out_the_fields_its_kinds_do_not_have() {
        let absent = serde_json::to_value(non_owner_reply().local).expect("serializes");
        assert_eq!(absent, json!({ "kind": "absent" }));
        let live = serde_json::to_value(owner_reply().local).expect("serializes");
        assert_eq!(
            live,
            json!({
                "kind": "live",
                "version": format!("1760054300112.3@{C}"),
                "expires_at_ms": 1_760_054_360_112_u64,
                "spilled": false,
            })
        );
        let unavailable = serde_json::to_value(Source {
            kind: "unavailable".to_string(),
            ..Source::default()
        })
        .expect("serializes");
        assert_eq!(unavailable, json!({ "kind": "unavailable" }));
        let replicated = ExplainReply {
            distributed: None,
            ..owner_reply()
        };
        assert_eq!(
            serde_json::to_value(&replicated).expect("serializes")["distributed"],
            Value::Null,
            "a cache with no ownership says null"
        );
    }

    #[test]
    fn an_expiry_is_omitted_null_or_a_number() {
        let expiry = |expires_at_ms| {
            let local = Local {
                kind: "live".to_string(),
                expires_at_ms,
                ..Local::default()
            };
            serde_json::to_value(local).expect("serializes")
        };
        assert_eq!(expiry(Expiry::Omitted), json!({ "kind": "live" }));
        assert_eq!(
            expiry(Expiry::Never),
            json!({ "kind": "live", "expires_at_ms": null })
        );
        assert_eq!(
            expiry(Expiry::At(5)),
            json!({ "kind": "live", "expires_at_ms": 5 })
        );
        assert_eq!(Expiry::from(None), Expiry::Never);
        assert_eq!(Expiry::from(Some(9)), Expiry::At(9));
        assert_eq!(Expiry::default(), Expiry::Omitted);
    }

    #[test]
    fn an_unknown_variant_becomes_an_other_token_without_whitespace() {
        assert_eq!(sanitize("a.b:c_d9"), "a.b:c_d9", "the allowlist passes");
        assert_eq!(
            sanitize("A b\nC{d: 1}\"\u{e9}\t"),
            "A_b_C_d:_1____",
            "each character outside the allowlist becomes one underscore"
        );
        let token = other_token("Foo { bar: 1 }\nbaz");
        assert_eq!(token, "other:Foo___bar:_1___baz");
        assert!(!token.contains(char::is_whitespace));
    }

    #[test]
    fn the_token_mappers_name_every_constructible_variant() {
        let owners = NonZeroU8::new(3).expect("3 is nonzero");
        for (mode, token) in [
            (Mode::Local, "local"),
            (Mode::Invalidation, "invalidation"),
            (Mode::Replicated, "replicated"),
            (Mode::Distributed { owners }, "distributed:3"),
        ] {
            assert_eq!(mode_token(mode), token);
        }
        for (read, token) in [
            (LocalRead::Hit, "hit"),
            (LocalRead::Miss, "miss"),
            (LocalRead::NotOwner, "not_owner"),
            (LocalRead::Distrusted, "distrusted"),
            (LocalRead::ColdMiss, "cold_miss"),
        ] {
            assert_eq!(local_read_token(read), token);
        }
        for (verdict, token) in [
            (ServeVerdict::Serve, "serve"),
            (ServeVerdict::Stale, "stale"),
            (ServeVerdict::Miss, "miss"),
            (ServeVerdict::DeclineDistrusted, "decline_distrusted"),
            (ServeVerdict::DeclineCold, "decline_cold"),
        ] {
            assert_eq!(serve_token(verdict), token);
        }
        for (reads, token) in [
            (Reads::Value, "value"),
            (Reads::Deleted, "deleted"),
            (Reads::Expired, "expired"),
            (Reads::Undecodable, "undecodable"),
        ] {
            assert_eq!(reads_token(reads), token);
        }
        for (why, token) in [
            (Unreached::NotAMember, "not_a_member"),
            (Unreached::ProtocolTooOld, "protocol_too_old"),
            (Unreached::TimedOut, "timed_out"),
            (
                Unreached::Io(ErrorKind::ConnectionRefused),
                "io:ConnectionRefused",
            ),
            (Unreached::Io(ErrorKind::NotFound), "io:NotFound"),
            (Unreached::Codec, "codec"),
        ] {
            assert_eq!(unreached_token(why), token);
        }
        assert_eq!(lapse_token(Lapse::Expired), "expired");
        assert_eq!(lapse_token(Lapse::Idle), "idle");
    }

    #[test]
    fn a_probe_carries_only_the_fields_of_its_answer() {
        let node = NodeId::from(7u64).to_string();
        let probe = |answer| serde_json::to_value(probe_answer(node.clone(), &answer));
        assert_eq!(
            probe(ProbeAnswer::Miss).expect("serializes"),
            json!({ "node": "0000000000000007", "answer": "miss" })
        );
        assert_eq!(
            probe(ProbeAnswer::Declined).expect("serializes"),
            json!({ "node": "0000000000000007", "answer": "declined" })
        );
        for (why, token) in [
            (Unreached::TimedOut, "timed_out"),
            (
                Unreached::Io(ErrorKind::ConnectionReset),
                "io:ConnectionReset",
            ),
        ] {
            assert_eq!(
                probe(ProbeAnswer::Unreached(why)).expect("serializes"),
                json!({
                    "node": "0000000000000007",
                    "answer": "unreached",
                    "why": token,
                })
            );
        }
    }

    #[test]
    fn a_record_that_reads_absent_has_no_version_and_a_source_with_no_owner() {
        let absent = local(&LocalRecord::Absent);
        assert_eq!(absent.kind, "absent");
        assert_eq!(absent.version, None);
        assert_eq!(absent.expires_at_ms, Expiry::Omitted);
        let unavailable = source(&ReadSource::Unavailable);
        assert_eq!(unavailable.kind, "unavailable");
        assert_eq!((unavailable.node, unavailable.hit), (None, None));
    }

    #[test]
    fn hashes_print_as_sixteen_digits_and_durations_saturate() {
        assert_eq!(hash_text(0x5d), "000000000000005d");
        assert_eq!(hash_text(u64::MAX), "ffffffffffffffff");
        assert_eq!(millis(Duration::from_millis(1_500)), 1_500);
        assert_eq!(millis(Duration::from_micros(999)), 0);
        assert_eq!(millis(Duration::MAX), u64::MAX);
        assert_eq!(
            version_text(Hlc {
                wall_ms: 12,
                logical: 3,
                node: NodeId::from(255u64),
            }),
            "12.3@00000000000000ff"
        );
    }
}
