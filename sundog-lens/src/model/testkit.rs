//! Fixture builders for tests: members, snapshots and ownership digests with
//! deterministic ids and addresses.

use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use smol_str::SmolStr;
use sundog::membership::Peer;
use sundog::observe::{ClusterSnapshot, Member, MemberStatus};
use sundog::store::Mode;
use sundog::wire::PROTOCOL_VERSION;
use sundog::{NodeId, NodeName};

use super::Model;
use super::ownership::OwnershipDigest;
use crate::source::Update;

/// The gossip address of fixture node `index`: `127.0.0.(10 + index):7946`.
#[must_use]
pub fn gossip_addr(index: u8) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 10 + index], 7946))
}

/// A loopback address nothing listens on, whose connections are refused.
///
/// Its port lies below every default ephemeral range (Linux starts at
/// 32768, macOS and Windows at 49152), so no socket bound to port 0, a
/// concurrent test's listener included, is handed it while a connect to it
/// is in flight. The port is the first in 20000..30000, from an offset
/// derived from the process id, that a probe listener can bind.
///
/// # Panics
///
/// Panics when every port in that range is taken.
#[must_use]
pub fn refusing_addr() -> SocketAddr {
    const FIRST: u16 = 20_000;
    const SPAN: u16 = 10_000;
    let offset = u16::try_from(std::process::id() % u32::from(SPAN)).expect("below SPAN");
    (0..SPAN)
        .map(|step| FIRST + (offset + step) % SPAN)
        .map(|port| SocketAddr::from(([127, 0, 0, 1], port)))
        .find(|addr| std::net::TcpListener::bind(addr).is_ok())
        .expect("a free port in 20000..30000")
}

/// The node id of fixture node `index` at identity `generation`. The leading
/// hex digits grow with `index` (up to 80), so ids sort in index order and
/// their four-digit short forms differ.
#[must_use]
pub fn node_id(index: u8, generation: u16) -> NodeId {
    let lead = (0x1a4f + u64::from(index) * 0x300) & 0xFFFF;
    NodeId::from((lead << 48) | 0x5d2e_9b01_0000 | u64::from(generation))
}

/// A member at fixture address `index` with identity `generation` and the
/// given incarnation, advertising `it` as `Distributed` with two owners.
#[must_use]
pub fn member_at(index: u8, generation: u16, incarnation: u64, status: MemberStatus) -> Member {
    member_with(
        index,
        generation,
        incarnation,
        status,
        &[("it", distributed(2))],
    )
}

/// A member at fixture address `index` advertising `caches`.
#[must_use]
pub fn member_with(
    index: u8,
    generation: u16,
    incarnation: u64,
    status: MemberStatus,
    caches: &[(&str, Mode)],
) -> Member {
    member_since(
        index,
        generation,
        incarnation,
        status,
        SystemTime::UNIX_EPOCH,
        caches,
    )
}

/// As [`member_with`], first seen in its status at `since`.
#[must_use]
pub fn member_since(
    index: u8,
    generation: u16,
    incarnation: u64,
    status: MemberStatus,
    since: SystemTime,
    caches: &[(&str, Mode)],
) -> Member {
    let node = node_id(index, generation);
    Member::new(
        Peer {
            node,
            name: NodeName::new("host", node),
            gossip_addr: gossip_addr(index),
            data_addr: SocketAddr::from(([127, 0, 0, 10 + index], 39211)),
            incarnation,
            protocol: PROTOCOL_VERSION,
        },
        status,
        since,
        caches
            .iter()
            .map(|&(name, mode)| (SmolStr::new(name), mode))
            .collect(),
    )
}

/// `Mode::Distributed` with `owners` owners per part.
///
/// # Panics
///
/// Panics when `owners` is 0.
#[must_use]
pub fn distributed(owners: u8) -> Mode {
    Mode::Distributed {
        owners: NonZeroU8::new(owners).expect("owners is nonzero"),
    }
}

/// Fixture member `index` with the given status.
#[must_use]
pub fn member(index: u8, status: MemberStatus) -> Member {
    member_at(index, 0, 1, status)
}

/// A snapshot of `live` live members, `1..=live`.
#[must_use]
pub fn snapshot(live: u8) -> ClusterSnapshot {
    ClusterSnapshot::new(
        "fixture",
        (1..=live)
            .map(|index| member(index, MemberStatus::Live))
            .collect(),
        0,
    )
}

/// A snapshot of `live` live members, `1..=live`, each advertising `it` as
/// `Distributed` with `owners` owners.
#[must_use]
pub fn snapshot_with_owners(live: u8, owners: u8) -> ClusterSnapshot {
    ClusterSnapshot::new(
        "fixture",
        (1..=live)
            .map(|index| {
                member_with(
                    index,
                    0,
                    1,
                    MemberStatus::Live,
                    &[("it", distributed(owners))],
                )
            })
            .collect(),
        0,
    )
}

/// The ownership digest of `cache` in `snapshot` with two owners, or `None`
/// when no member is eligible.
#[must_use]
pub fn ownership_digest(snapshot: &ClusterSnapshot, cache: &str) -> Option<OwnershipDigest> {
    ownership_digest_after(snapshot, cache, None)
}

/// As [`ownership_digest`], compared against the digest it replaces.
#[must_use]
pub fn ownership_digest_after(
    snapshot: &ClusterSnapshot,
    cache: &str,
    previous: Option<&OwnershipDigest>,
) -> Option<OwnershipDigest> {
    ownership_digest_with_owners(snapshot, cache, Mode::DEFAULT_OWNERS, previous)
}

/// As [`ownership_digest_after`], ranking with `owners` owners per part.
#[must_use]
pub fn ownership_digest_with_owners(
    snapshot: &ClusterSnapshot,
    cache: &str,
    owners: NonZeroU8,
    previous: Option<&OwnershipDigest>,
) -> Option<OwnershipDigest> {
    let shares = snapshot.ownership(cache, owners)?;
    Some(OwnershipDigest::from_shares(Arc::new(shares), previous))
}

/// Fixture member `index` advertising `it` as `Distributed` with two owners
/// and `churn`, `pn` and `os` as `Replicated`.
#[must_use]
pub fn full_member(index: u8, status: MemberStatus) -> Member {
    full_member_since(index, status, SystemTime::UNIX_EPOCH)
}

/// As [`full_member`], first seen in its status at `since`.
#[must_use]
pub fn full_member_since(index: u8, status: MemberStatus, since: SystemTime) -> Member {
    member_since(
        index,
        0,
        1,
        status,
        since,
        &[
            ("it", distributed(2)),
            ("churn", Mode::Replicated),
            ("pn", Mode::Replicated),
            ("os", Mode::Replicated),
        ],
    )
}

/// A snapshot of eight full members: `n1` to `n5` live, `n6` departing, `n7`
/// down and `n8` left.
#[must_use]
pub fn mixed_snapshot() -> ClusterSnapshot {
    use MemberStatus::{Departing, Down, Left, Live};
    let statuses = [Live, Live, Live, Live, Live, Departing, Down, Left];
    ClusterSnapshot::new(
        "fixture",
        (1u8..)
            .zip(statuses)
            .map(|(index, status)| full_member(index, status))
            .collect(),
        0,
    )
}

/// A model that has watched a small story from `base`: three live members at
/// 0 s, eight live at 5 s, then `n6` departing, `n7` down and `n8` left at
/// 10 s, ticked to 20 s so the last view has settled. Each member is stamped
/// with when it first showed in its status. Wall-clock time is the Unix epoch
/// plus the same offsets, so the model is the same on every run except for
/// `base`.
///
/// # Panics
///
/// Panics when a fixture snapshot has no eligible member, which it never has.
#[must_use]
pub fn fixture_model(base: Instant) -> Model {
    use MemberStatus::{Departing, Down, Left, Live};
    let at = |secs: u64| base + Duration::from_secs(secs);
    let wall = |secs: u64| SystemTime::UNIX_EPOCH + Duration::from_secs(secs);
    // Members 1 to 3 are live from 0 s, 4 to 8 from 5 s; at 10 s the last
    // three change status.
    let story = |secs: u64| {
        let statuses = |index: u8| match (secs, index) {
            (10.., 6) => (Departing, 10),
            (10.., 7) => (Down, 10),
            (10.., 8) => (Left, 10),
            (_, 1..=3) => (Live, 0),
            _ => (Live, 5),
        };
        let count = if secs == 0 { 3 } else { 8 };
        ClusterSnapshot::new(
            "fixture",
            (1..=count)
                .map(|index| {
                    let (status, since) = statuses(index);
                    full_member_since(index, status, wall(since))
                })
                .collect(),
            0,
        )
    };
    let mut model = Model::new();
    // The fixture processes carry incarnation 1 ms: they predate the lens,
    // so the first three are the discovery baseline.
    model.set_started(wall(0) + Duration::from_secs(1));
    for secs in [0, 5, 10] {
        let snapshot = story(secs);
        let digest = ownership_digest_after(&snapshot, "it", model.ownership("it"))
            .expect("a live member advertises it");
        model.apply(
            Update::Snapshot(Arc::new(snapshot), at(secs)),
            at(secs),
            wall(secs),
        );
        model.apply(Update::Ownership(digest), at(secs), wall(secs));
    }
    model.tick(at(20));
    model
}

/// A model that has seen one live member at `base` and held still for
/// [`DISCOVERY_QUIET`](crate::model::DISCOVERY_QUIET), so it no longer
/// treats ownership digests as the discovery baseline, and the instant it
/// stands at.
#[must_use]
pub fn past_discovery(base: Instant) -> (Model, Instant) {
    let mut model = Model::new();
    model.apply(
        Update::Snapshot(Arc::new(snapshot(1)), base),
        base,
        // Later than every fixture incarnation: the member predates the lens.
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    );
    let now = base + crate::model::DISCOVERY_QUIET;
    model.tick(now);
    assert!(!model.discovering());
    (model, now)
}

/// The exporter body the metrics fixtures are cut from.
const EXPORTER_BODY: &str = include_str!("../../tests/fixtures/metrics.prom");

/// How many one-second scrape rounds [`fixture_model_with_metrics`] folds in.
pub const FIXTURE_SCRAPES: u32 = 60;

/// The samples one node's exporter reports at round `round`: the captured
/// exporter body with counters grown cumulatively at a rate that swings with
/// the round and the node, `sundog_live_peers` set to the observer's count and
/// `sundog_owned_parts` set to `owned`.
fn exporter_samples(
    round: u32,
    phase: f64,
    owned: f64,
    peers: f64,
) -> Vec<crate::source::expo::Sample> {
    let mut samples = crate::source::expo::parse(EXPORTER_BODY);
    // The cumulative growth: the sum of a rate that swings between 0.4 and
    // 2.5 and ramps up over the first half minute.
    let growth: f64 = (0..=round)
        .map(|j| {
            let t = f64::from(j);
            let swing = 1.4 + 0.7 * (t / 9.0 + phase).sin() + 0.35 * (t / 2.7 + 2.0 * phase).sin();
            swing * (0.4 + 0.6 * (t / 30.0).min(1.0))
        })
        .sum();
    for sample in &mut samples {
        // Rebalance counters stay where the capture left them: no part moves.
        if sample.name == "sundog_rebalance_parts_total" {
            continue;
        }
        if sample.name.ends_with("_total") {
            sample.value = (sample.value * growth).round();
        } else if sample.name == "sundog_owned_parts" {
            sample.value = owned;
        } else if sample.name == "sundog_live_peers" {
            sample.value = peers;
        }
    }
    samples
}

/// [`fixture_model`] followed by [`FIXTURE_SCRAPES`] seconds of exporter
/// scrapes of every live and departing member, one a second from 21 s on.
/// Every node reports the parts the observer computes for it except `n3`,
/// which still reports 900 fewer, so one row reads `↻`. The counters grow
/// with a swinging rate, so the charts have a shape.
///
/// # Panics
///
/// Panics when the fixture model has no `it` digest, which it always has.
#[must_use]
pub fn fixture_model_with_metrics(base: Instant) -> Model {
    fixture_model_with_scrapes(base, FIXTURE_SCRAPES)
}

/// As [`fixture_model_with_metrics`], with `rounds` scrape rounds.
///
/// # Panics
///
/// Panics when the fixture model has no `it` digest, which it always has.
#[must_use]
pub fn fixture_model_with_scrapes(base: Instant, rounds: u32) -> Model {
    let mut model = fixture_model(base);
    let digest = model
        .ownership("it")
        .expect("the fixture has an it digest")
        .clone();
    let members: Vec<_> = model
        .snapshot()
        .expect("the fixture has a snapshot")
        .members
        .iter()
        .filter(|member| member.status.is_live())
        .map(|member| (member.peer.node, member.peer.gossip_addr))
        .collect();
    let live = members.len();
    for round in 0..rounds {
        let secs = 21 + u64::from(round);
        let at = base + Duration::from_secs(secs);
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(secs);
        for (index, &(node, addr)) in members.iter().enumerate() {
            let owned = digest.parts_owned_by(node);
            let reported = if index == 2 {
                owned.saturating_sub(900)
            } else {
                owned
            };
            let phase = f64::from(u32::try_from(index).unwrap_or(0));
            let samples = exporter_samples(
                round,
                phase,
                crate::model::count_to_f64(reported),
                crate::model::count_to_f64(live - 1),
            );
            model.apply(
                Update::Scrape(crate::source::ScrapeReport {
                    addr,
                    node,
                    at,
                    outcome: Ok(samples),
                    ready: Some(true),
                }),
                at,
                wall,
            );
        }
    }
    model.tick(base + Duration::from_secs(21 + u64::from(rounds)));
    model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_ids_and_addresses_are_distinct() {
        assert_ne!(node_id(1, 0), node_id(2, 0));
        assert_ne!(node_id(1, 0), node_id(1, 1));
        assert_eq!(gossip_addr(1), "127.0.0.11:7946".parse().unwrap());
    }

    #[test]
    fn a_refusing_address_is_loopback_below_every_ephemeral_range_and_refuses() {
        let addr = refusing_addr();
        assert!(addr.ip().is_loopback());
        assert!((20_000..30_000).contains(&addr.port()), "{addr}");
        let refused = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5))
            .expect_err("nothing listens there");
        assert_eq!(refused.kind(), std::io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn a_snapshot_holds_live_members_in_node_order() {
        let snapshot = snapshot(3);
        assert_eq!(snapshot.members.len(), 3);
        assert!(
            snapshot
                .members
                .iter()
                .all(|m| m.status == MemberStatus::Live)
        );
        let ids: Vec<_> = snapshot.members.iter().map(|m| m.peer.node).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn members_carry_the_caches_they_are_given() {
        let member = member_with(
            2,
            0,
            1,
            MemberStatus::Live,
            &[("a", Mode::Replicated), ("b", distributed(3))],
        );
        assert_eq!(member.caches.len(), 2);
        assert_eq!(member.caches["a"], Mode::Replicated);
        assert_eq!(member.caches["b"], distributed(3));
        assert_eq!(
            member_at(2, 0, 1, MemberStatus::Live).caches["it"],
            distributed(2)
        );
    }

    #[test]
    fn the_mixed_snapshot_holds_every_status() {
        let snapshot = mixed_snapshot();
        let count = |status| {
            snapshot
                .members
                .iter()
                .filter(|m| m.status == status)
                .count()
        };
        assert_eq!(count(MemberStatus::Live), 5);
        assert_eq!(count(MemberStatus::Departing), 1);
        assert_eq!(count(MemberStatus::Down), 1);
        assert_eq!(count(MemberStatus::Left), 1);
        assert!(snapshot.members.iter().all(|m| m.caches.len() == 4));
        assert_eq!(
            full_member(1, MemberStatus::Live).caches["os"],
            Mode::Replicated
        );
    }

    #[test]
    fn the_fixture_model_has_watched_the_story() {
        let base = Instant::now();
        let model = fixture_model(base);
        assert_eq!(model.snapshot().unwrap().members.len(), 8);
        assert_eq!(model.ownership("it").unwrap().eligible.len(), 5);
        assert_eq!(model.settled("it"), Some(true));
        assert_eq!(model.now(), Some(base + Duration::from_secs(20)));
        let tags: Vec<_> = model.events().iter().map(|e| e.kind.tag()).collect();
        for tag in ["JOIN", "LEAVE", "DOWN", "LEFT", "VIEW", "SETTLED"] {
            assert!(tags.contains(&tag), "{tag} missing from {tags:?}");
        }
    }

    #[test]
    fn ownership_digests_exist_only_with_a_live_advertiser() {
        assert!(ownership_digest(&snapshot(2), "it").is_some());
        assert!(ownership_digest(&snapshot(2), "other").is_none());
        let down = ClusterSnapshot::new("c", vec![member(1, MemberStatus::Down)], 0);
        assert!(ownership_digest(&down, "it").is_none());
    }

    #[test]
    fn the_metrics_fixture_scrapes_every_live_member_into_charts_and_rates() {
        let base = Instant::now();
        let model = fixture_model_with_metrics(base);
        let live = model
            .snapshot()
            .unwrap()
            .members
            .iter()
            .filter(|m| m.status.is_live())
            .count();
        assert_eq!(live, 6);
        let addr = gossip_addr(1);
        let metrics = model.metrics(addr).expect("n1 is scraped");
        assert_eq!(metrics.folds(), FIXTURE_SCRAPES);
        assert_eq!(metrics.ops().len(), FIXTURE_SCRAPES as usize - 1);
        assert!(metrics.ops().last().unwrap() > 100.0);
        assert!(
            metrics
                .live_peers()
                .is_some_and(|peers| (peers - 5.0).abs() < 1e-9)
        );
        // The third node still reports fewer parts than the observer computes.
        let digest = model.ownership("it").unwrap();
        let third = digest.eligible[2];
        let reported = model
            .metrics(gossip_addr(3))
            .unwrap()
            .owned_parts("it")
            .unwrap();
        assert!(
            (reported + 900.0 - crate::model::count_to_f64(digest.parts_owned_by(third))).abs()
                < 1e-9
        );
        assert_eq!(
            model.settled("it"),
            Some(false),
            "n3 keeps the cache from settling"
        );
        assert_eq!(
            model.now(),
            Some(base + Duration::from_secs(21 + u64::from(FIXTURE_SCRAPES)))
        );
    }
}
