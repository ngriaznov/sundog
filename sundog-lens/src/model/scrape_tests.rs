//! Tests of how the model folds scrape reports: metrics, exporter events,
//! the peer-count check and settling with metrics.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use sundog::observe::{ClusterSnapshot, MemberStatus};

use super::lifelines::PhaseKind;
use super::*;
use crate::model::testkit;
use crate::source::expo::{self, Sample};
use crate::source::names;
use crate::source::scrape::ScrapeError;

const FIXTURE: &str = include_str!("../../tests/fixtures/metrics.prom");
const WALL: SystemTime = SystemTime::UNIX_EPOCH;

fn gauge(name: &str, labels: &[(&str, &str)], value: f64) -> Sample {
    Sample {
        name: name.to_owned(),
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        value,
    }
}

fn after(base: Instant, millis: u64) -> Instant {
    base + Duration::from_millis(millis)
}

fn report(
    index: u8,
    generation: u16,
    at: Instant,
    outcome: Result<Vec<Sample>, ScrapeError>,
    ready: Option<bool>,
) -> Update {
    Update::Scrape(ScrapeReport {
        addr: testkit::gossip_addr(index),
        node: testkit::node_id(index, generation),
        at,
        outcome,
        ready,
    })
}

fn answer(index: u8, at: Instant, samples: Vec<Sample>) -> Update {
    report(index, 0, at, Ok(samples), None)
}

fn fail(index: u8, at: Instant) -> Update {
    report(index, 0, at, Err(ScrapeError::Timeout), None)
}

fn tags(events: &[Event]) -> Vec<&'static str> {
    events.iter().map(|event| event.kind.tag()).collect()
}

/// A model that has seen `snapshot` and the `it` ownership computed from it,
/// both at `now`.
fn watching(snapshot: ClusterSnapshot, now: Instant) -> Model {
    let digest = testkit::ownership_digest(&snapshot, "it").expect("a live advertiser");
    let mut model = Model::new();
    model.apply(Update::Snapshot(Arc::new(snapshot), now), now, WALL);
    model.apply(Update::Ownership(digest), now, WALL);
    model
}

/// A model that has seen `snapshot` at `now` and no ownership.
fn observing(snapshot: ClusterSnapshot, now: Instant) -> Model {
    let mut model = Model::new();
    model.apply(Update::Snapshot(Arc::new(snapshot), now), now, WALL);
    model
}

/// The `owned_parts` sample of node `index` agreeing with the digest.
fn owned(model: &Model, index: u8) -> Sample {
    let parts = model
        .ownership("it")
        .unwrap()
        .parts_owned_by(testkit::node_id(index, 0));
    gauge(
        names::OWNED_PARTS,
        &[("cache", "it")],
        f64::from(u32::try_from(parts).unwrap()),
    )
}

#[test]
fn an_answering_scrape_folds_into_the_nodes_metrics_and_raises_one_event() {
    let base = Instant::now();
    let mut model = observing(testkit::snapshot(1), base);
    let addr = testkit::gossip_addr(1);
    assert!(model.metrics(addr).is_none() && model.exporter(addr).is_none());
    let events = model.apply(answer(1, base, expo::parse(FIXTURE)), base, WALL);
    assert_eq!(tags(&events), ["EXPORTER"]);
    let metrics = model.metrics(addr).unwrap();
    assert_eq!(metrics.node(), testkit::node_id(1, 0));
    assert_eq!(metrics.owned_parts("it"), Some(43_616.0));
    assert_eq!(model.exporter(addr).unwrap().node(), testkit::node_id(1, 0));
    assert!(model.scrape(addr).unwrap().outcome.is_ok());
}

#[test]
fn a_node_id_new_to_the_address_starts_its_metrics_and_exporter_afresh() {
    let base = Instant::now();
    let mut model = observing(testkit::snapshot(1), base);
    let addr = testkit::gossip_addr(1);
    model.apply(answer(1, base, expo::parse(FIXTURE)), base, WALL);
    model.apply(
        answer(1, after(base, 1000), expo::parse(FIXTURE)),
        after(base, 1000),
        WALL,
    );
    assert_eq!(model.metrics(addr).unwrap().folds(), 2);
    let events = model.apply(
        report(1, 1, after(base, 2000), Ok(expo::parse(FIXTURE)), None),
        after(base, 2000),
        WALL,
    );
    assert_eq!(
        tags(&events),
        ["EXPORTER"],
        "the new process answers for the first time"
    );
    assert_eq!(model.metrics(addr).unwrap().folds(), 1);
    assert_eq!(model.metrics(addr).unwrap().node(), testkit::node_id(1, 1));
    assert_eq!(model.exporter(addr).unwrap().node(), testkit::node_id(1, 1));
}

#[test]
fn rising_dropped_frames_raise_a_drop_naming_the_node_and_peer() {
    let base = Instant::now();
    let mut model = observing(testkit::snapshot(1), base);
    let dropped = |value| {
        vec![gauge(
            names::BACKLOG_DROPPED,
            &[("peer", "00000000000000aa")],
            value,
        )]
    };
    model.apply(answer(1, base, dropped(40.0)), base, WALL);
    let events = model.apply(
        answer(1, after(base, 1000), dropped(352.0)),
        after(base, 1000),
        WALL,
    );
    assert_eq!(
        events.iter().map(|e| e.kind.clone()).collect::<Vec<_>>(),
        [EventKind::Drop {
            node: testkit::node_id(1, 0),
            peer: "00000000000000aa".into(),
            frames: 312,
        }]
    );
    assert!(model.events().iter().any(|e| e.kind.tag() == "DROP"));
    let events = model.apply(
        answer(1, after(base, 2000), dropped(352.0)),
        after(base, 2000),
        WALL,
    );
    assert!(events.is_empty());
}

#[test]
fn a_state_transfer_starting_raises_one_xfer() {
    let base = Instant::now();
    let mut model = observing(testkit::snapshot(1), base);
    let xfer = |value| {
        vec![gauge(
            names::STATE_TRANSFER_RECORDS,
            &[("cache", "it")],
            value,
        )]
    };
    model.apply(answer(1, base, xfer(0.0)), base, WALL);
    let events = model.apply(
        answer(1, after(base, 1000), xfer(800.0)),
        after(base, 1000),
        WALL,
    );
    assert_eq!(tags(&events), ["XFER"]);
    let events = model.apply(
        answer(1, after(base, 2000), xfer(1600.0)),
        after(base, 2000),
        WALL,
    );
    assert!(events.is_empty(), "the running transfer is not a new one");
}

#[test]
fn readiness_flips_raise_ready_and_unready() {
    let base = Instant::now();
    let mut model = observing(testkit::snapshot(1), base);
    let probe = |secs: u64, ready| report(1, 0, after(base, secs * 1000), Ok(Vec::new()), ready);
    model.apply(probe(0, Some(false)), base, WALL);
    let events = model.apply(probe(2, Some(true)), after(base, 2000), WALL);
    assert_eq!(tags(&events), ["READY"]);
    let events = model.apply(probe(4, Some(false)), after(base, 4000), WALL);
    assert_eq!(tags(&events), ["UNREADY"]);
    assert_eq!(
        model.exporter(testkit::gossip_addr(1)).unwrap().ready(),
        Some(false)
    );
}

#[test]
fn two_failed_scrapes_of_a_live_node_raise_unreachable_once() {
    let base = Instant::now();
    let mut model = observing(testkit::snapshot(1), base);
    model.apply(answer(1, base, Vec::new()), base, WALL);
    let events = model.apply(fail(1, after(base, 1000)), after(base, 1000), WALL);
    assert!(events.is_empty());
    let events = model.apply(fail(1, after(base, 2000)), after(base, 2000), WALL);
    assert_eq!(tags(&events), ["UNREACHABLE"]);
    assert_eq!(
        events[0].kind,
        EventKind::Unreachable {
            node: testkit::node_id(1, 0)
        }
    );
    let events = model.apply(fail(1, after(base, 3000)), after(base, 3000), WALL);
    assert!(events.is_empty());
    let state = model.exporter(testkit::gossip_addr(1)).unwrap();
    assert!(state.unreachable());
    assert_eq!(state.failures(), 3);
}

#[test]
fn two_failed_scrapes_of_a_node_gossip_dropped_raise_an_exporter_event_instead() {
    let base = Instant::now();
    let snapshot = ClusterSnapshot::new("c", vec![testkit::member(1, MemberStatus::Down)], 0);
    let mut model = Model::new();
    model.apply(Update::Snapshot(Arc::new(snapshot), base), base, WALL);
    model.apply(fail(1, base), base, WALL);
    let events = model.apply(fail(1, after(base, 1000)), after(base, 1000), WALL);
    assert_eq!(tags(&events), ["EXPORTER"]);
}

#[test]
fn a_collision_is_reported_once_and_does_not_make_the_node_suspect() {
    let base = Instant::now();
    let mut model = observing(testkit::snapshot(2), base);
    let addr = testkit::gossip_addr(1);
    model.apply(answer(1, base, Vec::new()), base, WALL);
    let collision = |secs: u64| {
        report(
            1,
            0,
            after(base, secs * 1000),
            Err(ScrapeError::Collision("http://shared/metrics".into())),
            None,
        )
    };
    let events = model.apply(collision(1), after(base, 1000), WALL);
    assert_eq!(tags(&events), ["EXPORTER"]);
    let EventKind::Exporter { detail, .. } = &events[0].kind else {
        panic!("an exporter event");
    };
    assert!(detail.contains("http://shared/metrics"), "{detail}");
    assert!(
        model
            .apply(collision(2), after(base, 2000), WALL)
            .is_empty()
    );
    assert_eq!(
        model.lifelines().node(addr).unwrap().current(),
        Some(PhaseKind::Live)
    );
    assert_eq!(model.exporter(addr).unwrap().failures(), 0);
    assert!(model.exporter(addr).unwrap().mapping_error().is_some());
}

#[test]
fn an_exporter_naming_the_node_as_its_own_peer_is_flagged_as_mismapped() {
    let base = Instant::now();
    let mut model = observing(testkit::snapshot(2), base);
    let addr = testkit::gossip_addr(1);
    let own = testkit::node_id(1, 0).to_string();
    let events = model.apply(
        answer(
            1,
            base,
            vec![gauge(names::BACKLOG_DROPPED, &[("peer", &own)], 0.0)],
        ),
        base,
        WALL,
    );
    assert!(model.exporter(addr).unwrap().mismatch());
    let flagged: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::Exporter { detail, .. } if detail.contains("wrong exporter") => Some(()),
            _ => None,
        })
        .collect();
    assert_eq!(flagged.len(), 1);
}

#[test]
fn a_peer_count_that_disagrees_turns_amber_only_after_three_seconds() {
    let base = Instant::now();
    let mut model = watching(testkit::snapshot(3), base);
    let addr = testkit::gossip_addr(1);
    assert_eq!(model.peers(addr), None, "no metrics yet");
    let peers = |value| vec![gauge(names::LIVE_PEERS, &[], value)];
    model.apply(answer(1, base, peers(1.0)), base, WALL);
    let view = model.peers(addr).unwrap();
    assert!((view.reported - 1.0).abs() < f64::EPSILON);
    assert_eq!(view.expected, 2);
    assert!(!view.amber);
    model.tick(after(base, 2999));
    assert!(!model.peers(addr).unwrap().amber);
    model.tick(after(base, 3000));
    assert!(model.peers(addr).unwrap().amber);
    model.apply(
        answer(1, after(base, 3500), peers(2.0)),
        after(base, 3500),
        WALL,
    );
    let view = model.peers(addr).unwrap();
    assert!(!view.amber, "agreement clears the alert at once");
    assert!((view.reported - 2.0).abs() < f64::EPSILON);
}

#[test]
fn a_peer_count_that_agrees_is_never_amber_and_a_dropped_node_has_no_view() {
    let base = Instant::now();
    let mut model = watching(testkit::snapshot(3), base);
    let addr = testkit::gossip_addr(1);
    model.apply(
        answer(1, base, vec![gauge(names::LIVE_PEERS, &[], 2.0)]),
        base,
        WALL,
    );
    model.tick(after(base, 10_000));
    assert!(!model.peers(addr).unwrap().amber);
    let down = ClusterSnapshot::new(
        "fixture",
        vec![
            testkit::member(1, MemberStatus::Down),
            testkit::member(2, MemberStatus::Live),
            testkit::member(3, MemberStatus::Live),
        ],
        0,
    );
    model.apply(
        Update::Snapshot(Arc::new(down), after(base, 11_000)),
        after(base, 11_000),
        WALL,
    );
    assert_eq!(model.peers(addr), None, "gossip lists the node down");
}

#[test]
fn a_snapshot_that_changes_the_live_count_starts_the_peer_alert_clock() {
    let base = Instant::now();
    let mut model = watching(testkit::snapshot(3), base);
    let addr = testkit::gossip_addr(1);
    model.apply(
        answer(1, base, vec![gauge(names::LIVE_PEERS, &[], 2.0)]),
        base,
        WALL,
    );
    let four = after(base, 1000);
    model.apply(
        Update::Snapshot(Arc::new(testkit::snapshot(4)), four),
        four,
        WALL,
    );
    assert!(!model.peers(addr).unwrap().amber);
    model.tick(after(base, 4000));
    assert!(
        model.peers(addr).unwrap().amber,
        "three seconds after the count changed"
    );
}

#[test]
fn metrics_settle_a_view_only_when_every_reporting_node_agrees_and_is_quiet() {
    let base = Instant::now();
    let mut model = watching(testkit::snapshot(3), base);
    let round = |model: &mut Model, secs: u64, skew: usize| {
        let at = after(base, secs * 1000);
        let mut events = Vec::new();
        for index in 1..=3u8 {
            let mut sample = owned(model, index);
            if index == 3 {
                sample.value += f64::from(u32::try_from(skew).unwrap());
            }
            events.extend(model.apply(answer(index, at, vec![sample]), at, WALL));
        }
        events
    };
    // The first round only sets the baseline: no quiet scrape yet.
    round(&mut model, 1, 0);
    let verdict = model.settle("it").unwrap();
    assert!(!verdict.settled && !verdict.gossip_only);
    // Node 3 still reports a count that differs from the computed one.
    round(&mut model, 2, 40);
    round(&mut model, 3, 40);
    round(&mut model, 4, 40);
    let verdict = model.settle("it").unwrap();
    assert!(!verdict.settled, "n3 disagrees even though the view is old");
    assert!(!verdict.gossip_only);
    // n3 catches up: every node agrees and has been quiet, so the cache
    // settles with n3's report.
    let events = round(&mut model, 5, 0);
    assert_eq!(model.settled("it"), Some(true));
    let settled: Vec<_> = events
        .iter()
        .filter(|event| event.kind.tag() == "SETTLED")
        .collect();
    assert_eq!(settled.len(), 1);
    let EventKind::Settled { took, .. } = &settled[0].kind else {
        panic!("a settled event");
    };
    assert_eq!(*took, Duration::from_secs(5));
}

#[test]
fn parts_pulled_in_keep_a_view_unsettled_until_they_stop() {
    let base = Instant::now();
    let mut model = watching(testkit::snapshot(2), base);
    let pulled = |model: &Model, index: u8, total: f64| {
        vec![
            owned(model, index),
            gauge(
                names::REBALANCE_PARTS,
                &[("cache", "it"), ("direction", "in")],
                total,
            ),
        ]
    };
    let mut total = 0.0;
    for (secs, step) in [(1, 0.0), (2, 100.0), (3, 100.0), (4, 0.0)] {
        total += step;
        let at = after(base, secs * 1000);
        for index in 1..=2u8 {
            let samples = pulled(&model, index, total);
            model.apply(answer(index, at, samples), at, WALL);
        }
    }
    assert_eq!(
        model.settled("it"),
        Some(false),
        "parts were still arriving at 3 s"
    );
    for secs in [5, 6] {
        let at = after(base, secs * 1000);
        for index in 1..=2u8 {
            let samples = pulled(&model, index, total);
            model.apply(answer(index, at, samples), at, WALL);
        }
    }
    assert_eq!(model.settled("it"), Some(true));
}

#[test]
fn a_scrape_older_than_the_view_does_not_vote() {
    let start = Instant::now();
    let view_at = after(start, 10_000);
    let mut model = watching(testkit::snapshot(2), view_at);
    for index in 1..=2u8 {
        let stale = owned(&model, index);
        model.apply(answer(index, start, vec![stale]), after(view_at, 100), WALL);
    }
    let verdict = model.settle("it").unwrap();
    assert!(
        verdict.gossip_only,
        "reports from before the view are not evidence"
    );
    assert!(!verdict.settled);
    model.tick(after(view_at, 3000));
    assert_eq!(
        model.settled("it"),
        Some(true),
        "the gossip hold still settles it"
    );
}

#[test]
fn coverage_sums_what_the_eligible_nodes_report() {
    let base = Instant::now();
    let mut model = watching(testkit::snapshot(3), base);
    assert_eq!(model.coverage("it"), None, "nobody reports");
    assert_eq!(model.coverage("other"), None);
    for index in 1..=3u8 {
        let sample = owned(&model, index);
        model.apply(answer(index, base, vec![sample]), base, WALL);
    }
    let coverage = model.coverage("it").unwrap();
    assert!((coverage - 1.0).abs() < 1e-9, "{coverage}");
    let mut half = watching(testkit::snapshot(3), base);
    let sample = owned(&half, 1);
    half.apply(answer(1, base, vec![sample]), base, WALL);
    let partial = half.coverage("it").unwrap();
    assert!(partial > 0.0 && partial < 0.5, "{partial}");
}

#[test]
fn divergence_spans_the_entry_counts_of_the_replicated_advertisers() {
    let base = Instant::now();
    let snapshot = ClusterSnapshot::new(
        "fixture",
        (1..=3)
            .map(|index| testkit::full_member(index, MemberStatus::Live))
            .collect(),
        0,
    );
    let mut model = watching(snapshot, base);
    assert_eq!(model.divergence("churn"), None);
    for (index, entries) in [(1u8, 10.0), (2, 12.0), (3, 25.0)] {
        let sample = gauge(names::CACHE_ENTRIES, &[("cache", "churn")], entries);
        model.apply(answer(index, base, vec![sample]), base, WALL);
    }
    assert_eq!(model.divergence("churn"), Some(15.0));
    assert_eq!(
        model.divergence("it"),
        None,
        "a Distributed cache has no divergence"
    );
    assert_eq!(model.divergence("nope"), None);
}
