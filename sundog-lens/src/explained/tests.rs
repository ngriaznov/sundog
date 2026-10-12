//! Tests of reply parsing, grouping, verdicts and the words the interface draws.

use std::num::NonZeroU8;

use serde_json::{Value, json};
use sundog::NodeId;
use sundog::observe::MemberStatus;
use sundog::store::PartId;

use crate::key::KeySpec;
use crate::locate::{Located, LocatedOwner};
use crate::model::derive::Settle;

use super::*;

const OWNER_LINE: &str = include_str!("../../tests/fixtures/explain/owner.json");
const NON_OWNER_LINE: &str = include_str!("../../tests/fixtures/explain/non_owner.json");
const CRASHED_OWNER_LINE: &str = include_str!("../../tests/fixtures/explain/crashed_owner.json");
const LAPSED_STALE_LINE: &str = include_str!("../../tests/fixtures/explain/lapsed_stale.json");

const A: &str = "6f3ac1e29d54b807";
const B: &str = "c40d9e7a15f2338b";
const C: &str = "1b88e5d0a7c64f92";
const D: &str = "9a2e7fb3c05d1148";
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

/// The reading `lapsed_stale.json` describes: node c releases a part it
/// no longer owns and holds a copy whose expiry passed, with every mark
/// set, and the three owners answer a stale view, a decline and a miss,
/// so no owner serves the fetch.
fn lapsed_stale_reading() -> Reading {
    Reading {
        node: C.to_owned(),
        mode: "distributed:3".to_owned(),
        bucket: 640,
        part: 9,
        at_ms: 1_760_054_340_250,
        local: Local::Lapsed {
            version: format!("1760054280033.1@{A}"),
            expires_at_ms: Some(1_760_054_336_050),
            cause: Lapse::Expired,
        },
        source: Source::Unavailable,
        distributed: Some(Distributed {
            owners: vec![A.to_owned(), B.to_owned(), D.to_owned()],
            residency: Residency {
                owns: false,
                releasing_ms: Some(4_200),
                cold_marked: true,
                unsettled: true,
                unverified: true,
                stale: true,
            },
            local_read: LocalRead::NotOwner,
            serves_peers: ServeVerdict::DeclineDistrusted,
            probes: vec![
                probe(
                    A,
                    Answer::StaleView {
                        responder_view: MOVED.to_owned(),
                    },
                ),
                probe(B, Answer::Declined),
                probe(D, Answer::Miss),
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
    assert_eq!(parse(LAPSED_STALE_LINE), lapsed_stale_reading());
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
    let outcome = agreement(&answers, &located.placement());
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
    let lens = located().placement();

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
fn a_located_key_places_as_its_fields_say() {
    let placement = located().placement();
    assert_eq!(placement.cache, "it");
    assert_eq!(placement.part, PartId::new(305, 17));
    assert_eq!(placement.view_hash, 0x5d69_e3db_4c1a_02f7);
    assert_eq!(
        placement.owners,
        [A, B].map(|hex| hex.parse::<NodeId>().expect("16 hex digits"))
    );
}

#[test]
fn the_verdict_names_what_the_answers_say_against_the_lens() {
    let lens = located().placement();
    let verdict_of = |nodes: Vec<NodeAnswer>| verdict(&explained(nodes), Some(&lens));

    let agree = verdict_of(vec![
        answered("n1", owner_reading()),
        answered("n2", non_owner_reading()),
    ]);
    assert_eq!(agree, Verdict::Agree { nodes: 2 });
    assert_eq!(
        agree.words(false),
        "2 nodes agree, the lens computes the same"
    );
    assert_eq!(
        verdict_of(vec![answered("n1", owner_reading())]).words(false),
        "1 node agrees, the lens computes the same"
    );

    let moved = verdict_of(vec![
        answered("n1", with_view(owner_reading(), MOVED)),
        answered("n2", with_view(non_owner_reading(), MOVED)),
    ]);
    assert_eq!(
        moved,
        Verdict::Moved {
            from: "71c0a2f4".to_owned(),
            to: "5d69e3db".to_owned()
        }
    );
    assert_eq!(
        moved.words(false),
        "view moved 71c0a2f4 -> 5d69e3db: Enter asks again"
    );
    assert_eq!(
        moved.words(true),
        "the nodes hold 71c0a2f4, the frozen display holds 5d69e3db: Esc, p unfreezes"
    );

    let split = verdict_of(vec![
        answered("n1", owner_reading()),
        answered("n2", with_view(non_owner_reading(), MOVED)),
    ]);
    assert_eq!(
        split.words(false),
        "n1 holds 5d69e3db, n2 holds 71c0a2f4",
        "{split:?}"
    );

    let mut elsewhere = owner_reading();
    elsewhere
        .distributed
        .as_mut()
        .expect("distributed")
        .owners
        .reverse();
    let apart = verdict_of(vec![
        answered("n1", elsewhere.clone()),
        answered("n2", non_owner_reading()),
    ]);
    assert_eq!(
        apart,
        Verdict::Apart {
            nodes: "n1".to_owned(),
            many: false,
            view: "5d69e3db".to_owned()
        }
    );
    assert_eq!(
        apart.words(false),
        "n1 differs from the lens, which holds view 5d69e3db"
    );
    let both = verdict_of(vec![
        answered("n1", elsewhere.clone()),
        answered("n2", elsewhere),
    ]);
    assert_eq!(
        both.words(false),
        "n1 n2 differ from the lens, which holds view 5d69e3db"
    );

    let other = Reading {
        cache: "other".to_owned(),
        ..owner_reading()
    };
    assert_eq!(
        verdict_of(vec![answered("n1", other)]).words(false),
        "the nodes explained cache other and the block above is cache it: \
         nothing to compare"
    );

    let replicated = Reading {
        distributed: None,
        ..owner_reading()
    };
    assert_eq!(
        verdict_of(vec![answered("n1", replicated), failed("n2", "x")]).words(false),
        "no node gave a Distributed reading to compare"
    );
    assert_eq!(
        verdict(&explained(vec![answered("n1", owner_reading())]), None).words(false),
        "the lens has no placement of the key to compare with"
    );
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
