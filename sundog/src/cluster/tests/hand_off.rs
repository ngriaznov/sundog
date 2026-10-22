//! Tests of a graceful leave's hand-off to its co-owners: the decisions that
//! share the budget and retry a failed round, the round loop that applies
//! them, and a leaving node handing a write to a co-owner.

use std::num::NonZeroU8;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use tokio::time::Instant;
use tracing_subscriber::layer::SubscriberExt as _;

use super::*;
use crate::cluster::anti_entropy::PartRoundOutcome;
use crate::cluster::rebalance::GROUP_RETRY_BACKOFF;
use crate::cluster::test_support::{registered_shard, scoped_subscriber};
use crate::hlc::Hlc;
use crate::ownership::{Granularity, OwnershipTracker, ResidencySet};
use crate::wire::WireRecord;

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

/// `count` distinct parts.
fn parts(count: usize) -> Vec<PartId> {
    (0..count)
        .map(|bucket| PartId::new(u16::try_from(bucket).expect("a few parts"), 0))
        .collect()
}

/// [`LEAVE_HAND_OFF_ROUNDS`] as a count.
fn round_limit() -> usize {
    usize::try_from(LEAVE_HAND_OFF_ROUNDS).expect("a small count")
}

/// What a scripted co-owner answers to one round.
#[derive(Clone)]
enum Answer {
    /// The round fails, or the co-owner answers `Stale`: every part stays
    /// diverged.
    Fail,
    /// The round completes and every requested part matches.
    MatchAll,
    /// The round completes and exactly these requested parts match.
    Match(Vec<PartId>),
    /// The round completes and nothing matches.
    MatchNone,
}

/// A co-owner that answers its rounds from a script, the last answer for
/// ever once the script runs out, and records when each round was asked and
/// over which parts.
struct ScriptedPeer {
    script: Vec<Answer>,
    asked: Mutex<Vec<(Instant, Vec<PartId>)>>,
}

impl ScriptedPeer {
    fn new(script: impl IntoIterator<Item = Answer>) -> Self {
        let script: Vec<Answer> = script.into_iter().collect();
        assert!(!script.is_empty(), "a peer answers something");
        Self {
            script,
            asked: Mutex::new(Vec::new()),
        }
    }

    /// The round `hand_off_with_peer` runs: ready at once.
    fn round(&self, requested: Vec<PartId>) -> std::future::Ready<PartRoundOutcome> {
        let mut asked = self.asked.lock().expect("lock");
        let answer = self.script[asked.len().min(self.script.len() - 1)].clone();
        asked.push((Instant::now(), requested.clone()));
        let requested: HashSet<PartId> = requested.into_iter().collect();
        let (matched, failed) = match answer {
            Answer::Fail => (HashSet::new(), true),
            Answer::MatchAll => (requested.clone(), false),
            Answer::Match(parts) => (
                parts
                    .into_iter()
                    .filter(|part| requested.contains(part))
                    .collect(),
                false,
            ),
            Answer::MatchNone => (HashSet::new(), false),
        };
        let still_diverged = requested.difference(&matched).copied().collect();
        std::future::ready(PartRoundOutcome {
            matched,
            still_diverged,
            bytes_moved: 0,
            failed,
        })
    }

    fn asked(&self) -> Vec<(Instant, Vec<PartId>)> {
        self.asked.lock().expect("lock").clone()
    }

    /// The times of the rounds asked so far.
    fn times(&self) -> Vec<Instant> {
        self.asked().into_iter().map(|(at, _)| at).collect()
    }
}

/// [`hand_off_with_peer`] against `peer`, which stays in the live set.
async fn hand_off(peer: &ScriptedPeer, parts: Vec<PartId>, deadline: Instant) -> Vec<PartId> {
    hand_off_with_peer(
        "prices",
        NodeId::from(2),
        |requested| peer.round(requested),
        || true,
        parts,
        deadline,
    )
    .await
}

/// The co-owner `peers[index]` stands for.
fn co_owner(index: usize) -> NodeId {
    NodeId::from(2 + u64::try_from(index).expect("a few peers"))
}

/// A group of two parts for `co_owner(index)`, whose shard is `index`.
fn group(index: usize) -> HandOffGroup<usize> {
    HandOffGroup {
        cache: SmolStr::new("prices"),
        shard: index,
        peer: co_owner(index),
        parts: parts(2),
    }
}

/// [`hand_off_in_shares`] over `groups`, each round answered by the scripted
/// peer its shard indexes, every co-owner in the live set.
async fn hand_off_all(
    peers: &[ScriptedPeer],
    groups: Vec<HandOffGroup<usize>>,
    overall: Instant,
) -> usize {
    hand_off_in_shares(
        groups,
        overall,
        |index, _cache, _peer, requested| peers[index].round(requested),
        |_| true,
    )
    .await
}

#[test]
fn group_deadline_gives_each_group_an_equal_share_of_the_time_left() {
    let base = Instant::now();
    // (now, overall, groups left, deadline), all in milliseconds from `base`.
    let table: [(u64, u64, usize, u64, &str); 8] = [
        (0, 1000, 1, 1000, "the last group gets everything left"),
        (0, 1000, 2, 500, "two groups split it"),
        (0, 1000, 4, 250, "four groups split it"),
        (
            100,
            1000,
            3,
            400,
            "a share of the time left, not of the whole",
        ),
        (0, 1000, 0, 1000, "no group left reads as one"),
        (1000, 1000, 3, 1000, "no time left, no time to share"),
        (
            1500,
            1000,
            2,
            1500,
            "a budget already past gives a deadline of now",
        ),
        (
            0,
            1000,
            usize::MAX,
            0,
            "a count past u32 shrinks the share to nothing",
        ),
    ];
    for (now, overall, groups_left, expected, why) in table {
        assert_eq!(
            group_deadline(base + ms(now), base + ms(overall), groups_left),
            base + ms(expected),
            "{why}: now {now}, overall {overall}, {groups_left} left"
        );
    }
}

#[test]
fn group_deadline_passes_the_time_a_quick_group_leaves_to_the_groups_after_it() {
    let base = Instant::now();
    let overall = base + ms(900);
    assert_eq!(group_deadline(base, overall, 3), base + ms(300));
    // The first group finishes after 50 ms: the other two split 850 ms.
    assert_eq!(group_deadline(base + ms(50), overall, 2), base + ms(475));
    // The second finishes after 100 ms: the last gets everything left.
    assert_eq!(group_deadline(base + ms(100), overall, 1), overall);
}

/// A round that completed with two parts left and one round completed, the
/// co-owner live and the deadline ahead.
const COMPLETED: RoundEnd = RoundEnd {
    failed: false,
    diverging: 2,
    completed_rounds: 1,
    deadline_passed: false,
    peer_live: true,
};

/// A round that failed, with the same state otherwise and no completed round.
const FAILED: RoundEnd = RoundEnd {
    failed: true,
    completed_rounds: 0,
    ..COMPLETED
};

#[test]
fn hand_off_step_retries_a_failed_round_and_counts_only_completed_ones() {
    let (failed, spent) = (FAILED, LEAVE_HAND_OFF_ROUNDS);
    let table = [
        (
            failed,
            HandOffStep::RetryAfterBackoff,
            "a failed round retries",
        ),
        (
            RoundEnd {
                completed_rounds: spent,
                ..failed
            },
            HandOffStep::RetryAfterBackoff,
            "a failed round never spends the completed rounds",
        ),
        (
            RoundEnd {
                deadline_passed: true,
                ..failed
            },
            HandOffStep::Done(HandOffEnd::DeadlinePassed),
            "a failed round past the deadline ends the group",
        ),
        (
            RoundEnd {
                peer_live: false,
                ..failed
            },
            HandOffStep::Done(HandOffEnd::PeerLeft),
            "a co-owner that left the live set is not retried",
        ),
        (
            RoundEnd {
                diverging: 0,
                ..failed
            },
            HandOffStep::Done(HandOffEnd::Matched),
            "nothing diverging needs no retry",
        ),
        (
            RoundEnd {
                deadline_passed: true,
                peer_live: false,
                ..failed
            },
            HandOffStep::Done(HandOffEnd::DeadlinePassed),
            "the deadline is named before a co-owner that left",
        ),
        (
            RoundEnd {
                diverging: 0,
                deadline_passed: true,
                peer_live: false,
                ..failed
            },
            HandOffStep::Done(HandOffEnd::Matched),
            "matching is named before anything else",
        ),
    ];
    for (end, expected, why) in table {
        assert_eq!(hand_off_step(end), expected, "{why}: {end:?}");
    }
}

#[test]
fn hand_off_step_runs_the_next_round_until_the_completed_rounds_are_spent() {
    let (live_round, spent) = (COMPLETED, LEAVE_HAND_OFF_ROUNDS);
    let table = [
        (
            live_round,
            HandOffStep::NextRound,
            "a completed round with parts left runs the next",
        ),
        (
            RoundEnd {
                completed_rounds: spent - 1,
                ..live_round
            },
            HandOffStep::NextRound,
            "the last allowed round still runs",
        ),
        (
            RoundEnd {
                completed_rounds: spent,
                ..live_round
            },
            HandOffStep::Done(HandOffEnd::RoundsSpent),
            "the completed rounds are spent",
        ),
        (
            RoundEnd {
                diverging: 0,
                ..live_round
            },
            HandOffStep::Done(HandOffEnd::Matched),
            "every part matches",
        ),
        (
            RoundEnd {
                deadline_passed: true,
                ..live_round
            },
            HandOffStep::Done(HandOffEnd::DeadlinePassed),
            "the deadline passed after a completed round",
        ),
        (
            RoundEnd {
                peer_live: false,
                ..live_round
            },
            HandOffStep::Done(HandOffEnd::PeerLeft),
            "a co-owner that left the live set is done",
        ),
    ];
    for (end, expected, why) in table {
        assert_eq!(hand_off_step(end), expected, "{why}: {end:?}");
    }
}

#[test]
fn every_way_a_group_ends_names_its_cause() {
    let table = [
        (HandOffEnd::Matched, "every part matches"),
        (
            HandOffEnd::DeadlinePassed,
            "the group's share of state_transfer_budget ran out",
        ),
        (HandOffEnd::PeerLeft, "the co-owner left the live set"),
        (HandOffEnd::RoundsSpent, "the completed rounds were spent"),
    ];
    for (end, expected) in table {
        assert_eq!(end.to_string(), expected, "{end:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_leave_hand_off_round_is_retried_and_the_parts_match_within_the_budget() {
    let peer = ScriptedPeer::new([Answer::Fail, Answer::MatchAll]);
    let all = parts(2);
    let diverging = hand_off(&peer, all.clone(), Instant::now() + ms(5_000)).await;

    assert!(diverging.is_empty(), "still diverging: {diverging:?}");
    let asked = peer.asked();
    assert_eq!(
        asked.len(),
        2,
        "a failed round is retried within the budget"
    );
    assert_eq!(
        asked[1].0 - asked[0].0,
        GROUP_RETRY_BACKOFF,
        "the retry waits the backoff"
    );
    assert_eq!(asked[1].1, all, "the retry covers the same parts");
}

#[tokio::test(start_paused = true)]
async fn a_stale_then_failed_then_matched_sequence_ends_with_every_part_matched() {
    let peer = ScriptedPeer::new([Answer::Fail, Answer::Fail, Answer::MatchAll]);
    let start = Instant::now();
    let diverging = hand_off(&peer, parts(3), start + ms(5_000)).await;

    assert!(diverging.is_empty(), "still diverging: {diverging:?}");
    assert_eq!(
        peer.times(),
        [
            start,
            start + GROUP_RETRY_BACKOFF,
            start + 2 * GROUP_RETRY_BACKOFF
        ],
        "each retry waits one backoff after the failed round"
    );
}

#[tokio::test(start_paused = true)]
async fn failed_rounds_do_not_count_against_the_completed_round_limit() {
    // More failed rounds than the limit allows, then as many completed
    // rounds as it allows, each matching one more part.
    let all = parts(round_limit());
    let failed = 2 * round_limit();
    let script = std::iter::repeat_n(Answer::Fail, failed)
        .chain(
            all[..all.len() - 1]
                .iter()
                .map(|&part| Answer::Match(vec![part])),
        )
        .chain([Answer::MatchAll]);
    let peer = ScriptedPeer::new(script);
    let diverging = hand_off(&peer, all, Instant::now() + ms(5_000)).await;

    assert!(diverging.is_empty(), "still diverging: {diverging:?}");
    assert_eq!(
        peer.asked().len(),
        failed + round_limit(),
        "the failed rounds leave every completed round the limit allows"
    );
}

#[tokio::test(start_paused = true)]
async fn completed_rounds_stop_at_the_limit_with_no_backoff_and_no_liveness_check() {
    let peer = ScriptedPeer::new([Answer::MatchNone]);
    let all = parts(2);
    let start = Instant::now();
    let live_checks = AtomicUsize::new(0);
    let diverging = hand_off_with_peer(
        "prices",
        NodeId::from(2),
        |requested| peer.round(requested),
        || {
            live_checks.fetch_add(1, Ordering::SeqCst);
            true
        },
        all.clone(),
        start + ms(5_000),
    )
    .await;

    assert_eq!(diverging, all, "nothing matched");
    assert_eq!(
        peer.times(),
        vec![start; round_limit()],
        "the completed rounds run back to back, no more than the limit"
    );
    assert_eq!(
        live_checks.load(Ordering::SeqCst),
        0,
        "a co-owner that answered is not checked for liveness"
    );
}

#[tokio::test(start_paused = true)]
async fn each_completed_round_covers_only_the_parts_still_diverging() {
    let all = parts(3);
    let peer = ScriptedPeer::new([
        Answer::Match(vec![all[0]]),
        Answer::Match(vec![all[1]]),
        Answer::MatchAll,
    ]);
    let diverging = hand_off(&peer, all.clone(), Instant::now() + ms(5_000)).await;

    assert!(diverging.is_empty(), "still diverging: {diverging:?}");
    let requested: Vec<Vec<PartId>> = peer.asked().into_iter().map(|(_, parts)| parts).collect();
    assert_eq!(
        requested,
        vec![all.clone(), all[1..].to_vec(), all[2..].to_vec()]
    );
}

#[tokio::test(start_paused = true)]
async fn a_co_owner_whose_every_round_fails_is_retried_until_the_deadline() {
    let peer = ScriptedPeer::new([Answer::Fail]);
    let all = parts(2);
    let start = Instant::now();
    let deadline = start + GROUP_RETRY_BACKOFF * 9 / 2;
    let diverging = hand_off(&peer, all.clone(), deadline).await;

    assert_eq!(diverging, all);
    assert_eq!(
        Instant::now(),
        deadline,
        "the last wait stops at the deadline"
    );
    let times = peer.times();
    assert_eq!(times.len(), 5, "{times:?}");
    assert!(
        times.iter().all(|&at| at < deadline),
        "no round starts at or past the deadline: {times:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_round_still_running_at_the_deadline_is_dropped() {
    let all = parts(2);
    let start = Instant::now();
    let deadline = start + ms(300);
    let diverging = hand_off_with_peer(
        "prices",
        NodeId::from(2),
        |_| std::future::pending::<PartRoundOutcome>(),
        || true,
        all.clone(),
        deadline,
    )
    .await;

    assert_eq!(diverging, all);
    assert_eq!(Instant::now(), deadline);
}

#[tokio::test(start_paused = true)]
async fn a_co_owner_that_leaves_the_live_set_stops_retrying() {
    let peer = ScriptedPeer::new([Answer::Fail]);
    let all = parts(2);
    let live_checks = AtomicUsize::new(0);
    let diverging = hand_off_with_peer(
        "prices",
        NodeId::from(2),
        |requested| peer.round(requested),
        // Live for the first retry, gone for the second.
        || live_checks.fetch_add(1, Ordering::SeqCst) == 0,
        all.clone(),
        Instant::now() + ms(5_000),
    )
    .await;

    assert_eq!(diverging, all, "the parts stay diverging");
    assert_eq!(
        peer.asked().len(),
        2,
        "one retry, then the co-owner is gone"
    );
    assert_eq!(
        live_checks.load(Ordering::SeqCst),
        2,
        "the live set is read once per failed round"
    );
}

#[tokio::test(start_paused = true)]
async fn a_co_owner_whose_every_round_fails_stops_at_its_share_and_the_next_group_still_runs() {
    let peers = [
        ScriptedPeer::new([Answer::Fail]),
        ScriptedPeer::new([Answer::MatchAll]),
    ];
    let start = Instant::now();
    let skipped = hand_off_all(&peers, vec![group(0), group(1)], start + ms(1_000)).await;

    assert_eq!(skipped, 0, "every group started");
    let first = peers[0].times();
    assert!(!first.is_empty(), "the unreachable co-owner is asked");
    assert!(
        first.iter().all(|&at| at < start + ms(500)),
        "the unreachable co-owner stops at its half: {first:?}"
    );
    assert_eq!(
        peers[1].times(),
        [start + ms(500)],
        "the next group starts when the first one's share ends"
    );
}

#[tokio::test(start_paused = true)]
async fn a_group_whose_round_hangs_is_dropped_at_its_share_and_the_next_group_still_runs() {
    let peers = [ScriptedPeer::new([Answer::MatchAll])];
    let start = Instant::now();
    let skipped = hand_off_in_shares(
        vec![group(0), group(0)],
        start + ms(1_000),
        |index, _cache, peer, requested| {
            let hangs = peer == co_owner(0) && peers[index].asked().is_empty();
            let answer = peers[index].round(requested);
            async move {
                if hangs {
                    std::future::pending().await
                } else {
                    answer.await
                }
            }
        },
        |_| true,
    )
    .await;

    assert_eq!(skipped, 0);
    assert_eq!(
        peers[0].times(),
        [start, start + ms(500)],
        "the second group starts at the first one's share, not at the overall deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn a_co_owner_that_leaves_the_live_set_ends_its_group_and_the_next_one_still_runs() {
    let peers = [
        ScriptedPeer::new([Answer::Fail]),
        ScriptedPeer::new([Answer::MatchAll]),
    ];
    let asked = Mutex::new(Vec::new());
    let start = Instant::now();
    let skipped = hand_off_in_shares(
        vec![group(0), group(1)],
        start + ms(1_000),
        |index, _cache, _peer, requested| peers[index].round(requested),
        |peer| {
            asked.lock().expect("lock").push(peer);
            peer != co_owner(0)
        },
    )
    .await;

    assert_eq!(skipped, 0);
    assert_eq!(
        peers[0].asked().len(),
        1,
        "the co-owner that left is not retried"
    );
    assert_eq!(
        *asked.lock().expect("lock"),
        [co_owner(0)],
        "the live set is read for the group's own co-owner, after its failed round only"
    );
    assert_eq!(
        peers[1].times(),
        [start],
        "the next group starts at once, not when the first one's share ends"
    );
}

#[tokio::test(start_paused = true)]
async fn each_round_runs_for_its_own_groups_shard_cache_and_co_owner() {
    let peer = ScriptedPeer::new([Answer::MatchAll]);
    let seen = Mutex::new(Vec::new());
    let groups = vec![
        HandOffGroup {
            cache: SmolStr::new("prices"),
            shard: 7_u8,
            peer: NodeId::from(2),
            parts: parts(1),
        },
        HandOffGroup {
            cache: SmolStr::new("users"),
            shard: 9_u8,
            peer: NodeId::from(3),
            parts: parts(3),
        },
    ];
    let skipped = hand_off_in_shares(
        groups,
        Instant::now() + ms(1_000),
        |shard, cache, peer_id, requested| {
            seen.lock()
                .expect("lock")
                .push((shard, cache, peer_id, requested.len()));
            peer.round(requested)
        },
        |_| true,
    )
    .await;

    assert_eq!(skipped, 0);
    assert_eq!(
        *seen.lock().expect("lock"),
        [
            (7, SmolStr::new("prices"), NodeId::from(2), 1),
            (9, SmolStr::new("users"), NodeId::from(3), 3),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn the_time_a_quick_group_leaves_unused_passes_to_the_groups_after_it() {
    let peers = [
        ScriptedPeer::new([Answer::MatchAll]),
        ScriptedPeer::new([Answer::Fail]),
        ScriptedPeer::new([Answer::Fail]),
    ];
    let start = Instant::now();
    let overall = start + ms(900);
    let skipped = hand_off_all(&peers, vec![group(0), group(1), group(2)], overall).await;

    assert_eq!(skipped, 0);
    assert_eq!(
        peers[2].times()[0],
        start + ms(450),
        "the second group had half of 900 ms, not a third"
    );
    assert_eq!(Instant::now(), overall, "the last group had what was left");
}

#[tokio::test(start_paused = true)]
async fn groups_not_started_when_the_overall_budget_has_passed_are_counted_and_logged() {
    let (events, _guard) = Events::capture();
    let peers = [
        ScriptedPeer::new([Answer::MatchAll]),
        ScriptedPeer::new([Answer::MatchAll]),
    ];
    let skipped = hand_off_all(&peers, vec![group(0), group(1)], Instant::now()).await;

    assert_eq!(skipped, 2, "a budget of zero starts nothing");
    assert!(peers.iter().all(|peer| peer.asked().is_empty()));
    let logged = events.at(tracing::Level::DEBUG, "skips a group");
    assert_eq!(logged.len(), 2, "one line per skipped group: {logged:?}");
    for (line, index) in logged.iter().zip(0..) {
        assert!(line.contains("cache=prices"), "{line}");
        assert!(
            line.contains(&format!("peer={}", co_owner(index))),
            "{line}"
        );
    }
}

/// Every tracing event of the tests below as `(level, fields)`.
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<(tracing::Level, String)>>>);

/// One event's fields as `name=value` pairs, the message included.
struct Fields(String);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        write!(self.0, "{}={value:?}", field.name()).expect("a string takes a write");
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Events {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0
            .lock()
            .expect("lock")
            .push((*event.metadata().level(), fields.0));
    }
}

impl Events {
    /// Installs the layer as this thread's subscriber while the guard lives.
    fn capture() -> (Self, crate::cluster::test_support::ScopedSubscriber) {
        let events = Self::default();
        let guard = scoped_subscriber(tracing_subscriber::registry().with(events.clone()));
        (events, guard)
    }

    /// The events at `level` whose fields contain `needle`.
    fn at(&self, level: tracing::Level, needle: &str) -> Vec<String> {
        self.0
            .lock()
            .expect("lock")
            .iter()
            .filter(|(seen, fields)| *seen == level && fields.contains(needle))
            .map(|(_, fields)| fields.clone())
            .collect()
    }
}

#[tokio::test(start_paused = true)]
async fn a_group_that_ends_with_parts_still_diverging_warns_with_the_cache_the_co_owner_and_the_count()
 {
    let (events, _guard) = Events::capture();
    let peer = ScriptedPeer::new([Answer::MatchNone]);
    let diverging = hand_off(&peer, parts(7), Instant::now() + ms(5_000)).await;
    assert_eq!(diverging.len(), 7);

    let warned = events.at(tracing::Level::WARN, "parts still diverging");
    assert_eq!(warned.len(), 1, "{warned:?}");
    for field in [
        "cache=prices".to_owned(),
        format!("peer={}", NodeId::from(2)),
        "diverging=7".to_owned(),
    ] {
        assert!(warned[0].contains(&field), "{field} missing: {warned:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn the_warning_for_a_group_with_parts_still_diverging_names_why_the_group_ended() {
    let (events, _guard) = Events::capture();
    let all = parts(2);
    let start = Instant::now();

    // The completed rounds are spent.
    let spent = ScriptedPeer::new([Answer::MatchNone]);
    hand_off(&spent, all.clone(), start + ms(5_000)).await;
    // The share runs out on a co-owner that never answers.
    let silent = ScriptedPeer::new([Answer::Fail]);
    hand_off(&silent, all.clone(), Instant::now() + ms(250)).await;
    // The share runs out on a round that never ends.
    hand_off_with_peer(
        "prices",
        NodeId::from(2),
        |_| std::future::pending::<PartRoundOutcome>(),
        || true,
        all.clone(),
        Instant::now() + ms(250),
    )
    .await;
    // The co-owner leaves the live set.
    let gone = ScriptedPeer::new([Answer::Fail]);
    hand_off_with_peer(
        "prices",
        NodeId::from(2),
        |requested| gone.round(requested),
        || false,
        all,
        Instant::now() + ms(5_000),
    )
    .await;

    let warned = events.at(tracing::Level::WARN, "parts still diverging");
    assert_eq!(warned.len(), 4, "{warned:?}");
    let reasons = [
        ("reason=the completed rounds were spent", 1),
        (
            "reason=the group's share of state_transfer_budget ran out",
            2,
        ),
        ("reason=the co-owner left the live set", 1),
    ];
    for (reason, count) in reasons {
        let seen = warned.iter().filter(|line| line.contains(reason)).count();
        assert_eq!(seen, count, "{reason}: {warned:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_retry_after_a_failed_round_logs_at_debug_and_a_matched_group_does_not_warn() {
    let (events, _guard) = Events::capture();
    let peer = ScriptedPeer::new([Answer::Fail, Answer::MatchAll]);
    let diverging = hand_off(&peer, parts(2), Instant::now() + ms(5_000)).await;
    assert!(diverging.is_empty(), "still diverging: {diverging:?}");

    let retries = events.at(tracing::Level::DEBUG, "retrying");
    assert_eq!(retries.len(), 1, "{retries:?}");
    assert!(retries[0].contains("cache=prices"), "{retries:?}");
    assert!(retries[0].contains("diverging=2"), "{retries:?}");
    assert!(
        events.at(tracing::Level::WARN, "").is_empty(),
        "a group that matched logs no warning"
    );
}

/// A `Distributed` shard that `cluster` registers under `name`, as an opened
/// cache would, with `view` as its ownership view. `Cluster::shutdown`
/// hands off from it. The sender publishes later views to it.
fn register_leaver_shard(
    cluster: &Cluster,
    name: &SmolStr,
    owners: NonZeroU8,
    view: Arc<OwnershipView>,
) -> (Arc<dyn ShardOps>, watch::Sender<Arc<OwnershipView>>) {
    let (tracker, view_tx) =
        OwnershipTracker::seed(cluster.node_id(), &[], &HashMap::new(), name, owners);
    view_tx
        .send(view)
        .expect("the tracker's own receiver keeps the channel open");
    let shard: Arc<dyn ShardOps> = Arc::new(
        Shard::<u32, String>::new(
            name.clone(),
            Mode::Distributed { owners },
            cluster.node_id(),
            u64::MAX,
            None,
            None,
        )
        .with_ownership(tracker, Arc::new(ResidencySet::new())),
    );
    cluster
        .shards()
        .write_shards()
        .insert(name.clone(), Arc::clone(&shard));
    (shard, view_tx)
}

/// The record a write of `key` leaves on the node `node` that took it.
fn written(key: u32, node: NodeId) -> WireRecord {
    WireRecord {
        key: Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
        value: Some(Bytes::from(
            postcard::to_stdvec(&format!("v{key}")).expect("value encodes"),
        )),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node,
        },
        expires_at_ms: None,
    }
}

/// A node that alone holds a write leaves while its view still differs from
/// its co-owner's, as it does right after a membership change: every round
/// it runs answers `Stale` until the views agree. The leave retries and
/// hands the write over. The view differs by a third node that is not live,
/// and the test publishes the agreed view once the leave has been retrying
/// for a few backoffs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leaving_node_retries_stale_rounds_and_hands_its_only_write_to_the_co_owner() {
    let owners = NonZeroU8::new(2).expect("nonzero");
    let name = SmolStr::new("prices");
    // Anti-entropy runs once an hour, so only the hand-off moves the write.
    let mut config = ClusterConfig {
        ae_interval: Duration::from_secs(3600),
        state_transfer_budget: Duration::from_secs(10),
        ..loopback_config()
    };
    config.tombstone_ttl = config.bucket_release_window();
    let (cluster_a, cluster_b) =
        two_node_cluster_with_config("cluster-it-leave-hand-off-stale", config).await;
    let (a, b) = (cluster_a.node_id(), cluster_b.node_id());
    let cache_b = tokio::time::timeout(
        Duration::from_secs(15),
        cluster_b
            .cache::<u32, String>(name.as_str())
            .mode(Mode::Distributed { owners })
            .open(),
    )
    .await
    .expect("b opens within the bound")
    .expect("b opens");

    let phantom = NodeId::from(u64::MAX);
    let view = |nodes: Vec<NodeId>| {
        Arc::new(OwnershipView::compute_at(
            a,
            nodes,
            owners,
            Granularity::Part,
        ))
    };
    let disagreeing = view(vec![a, b, phantom]);
    let agreed = view(vec![a, b]);
    assert_ne!(disagreeing.view_hash(), agreed.view_hash());
    let (shard_a, view_tx) =
        register_leaver_shard(&cluster_a, &name, owners, Arc::clone(&disagreeing));
    let keys: Vec<u32> = (0..2_000_u32)
        .filter(|&key| {
            let held_by = disagreeing.owners_of(part_of_u32(key));
            held_by.contains(&a) && held_by.contains(&b)
        })
        .take(8)
        .collect();
    assert_eq!(
        keys.len(),
        8,
        "a third of the parts are co-owned by a and b"
    );
    shard_a
        .apply_remote_batch(keys.iter().map(|&key| written(key, a)).collect())
        .await;
    cluster_a.advertise_cache_mode(&name, Mode::Distributed { owners });
    let shard_b = registered_shard(&cluster_b, &name);
    wait_until(
        Duration::from_secs(10),
        "b computes the view a is about to publish",
        async || shard_b.ownership_view_hash() == Some(agreed.view_hash()),
    )
    .await;
    for key in &keys {
        assert_eq!(
            cache_b.get(key).await,
            None,
            "b lacks key {key} until a leaves"
        );
    }

    let leave = async {
        tokio::time::timeout(Duration::from_secs(20), cluster_a.shutdown())
            .await
            .expect("a leaves within the bound");
    };
    let publish = async {
        tokio::time::sleep(3 * GROUP_RETRY_BACKOFF).await;
        view_tx
            .send(Arc::clone(&agreed))
            .expect("the tracker's own receiver keeps the channel open");
    };
    tokio::join!(leave, publish);

    wait_until(
        Duration::from_secs(5),
        "a's only copies of its writes reach b",
        async || {
            for &key in &keys {
                if cache_b.get(&key).await != Some(format!("v{key}")) {
                    return false;
                }
            }
            true
        },
    )
    .await;
    cluster_b.shutdown().await;
}

/// A leave whose budget is zero starts no group, logs one warning naming
/// how many it skipped, and still departs.
#[tokio::test]
async fn a_leave_with_no_budget_skips_its_groups_warns_once_and_still_departs() {
    let owners = NonZeroU8::new(2).expect("nonzero");
    let name = SmolStr::new("prices");
    let config = ClusterConfig {
        state_transfer_budget: Duration::ZERO,
        ..loopback_config()
    };
    let (cluster_a, cluster_b) =
        two_node_cluster_with_config("cluster-it-leave-hand-off-no-budget", config).await;
    let both = Arc::new(OwnershipView::compute_at(
        cluster_a.node_id(),
        vec![cluster_a.node_id(), cluster_b.node_id()],
        owners,
        Granularity::Part,
    ));
    let (_shard, _view_tx) = register_leaver_shard(&cluster_a, &name, owners, both);

    let (events, _guard) = Events::capture();
    tokio::time::timeout(Duration::from_secs(20), cluster_a.shutdown())
        .await
        .expect("a leaves within the bound");

    let warned = events.at(tracing::Level::WARN, "ran out of state_transfer_budget");
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert!(warned[0].contains("skipped_groups=1"), "{warned:?}");
    cluster_b.shutdown().await;
}

/// A co-owner that stops answering and leaves the live set ends the retries:
/// the leave neither gives up on it at its first failed round nor holds
/// shutdown for the whole `state_transfer_budget`. The co-owner crashes, so
/// the leaving node's rounds fail until its failure detector drops the
/// co-owner, and the leave then warns that the group ended for that reason.
#[tokio::test]
async fn a_leave_stops_retrying_a_co_owner_that_crashed_once_the_failure_detector_drops_it() {
    let owners = NonZeroU8::new(2).expect("nonzero");
    let name = SmolStr::new("prices");
    // A budget the failure detector's verdict beats by a wide margin.
    let config = ClusterConfig {
        ae_interval: Duration::from_secs(3600),
        state_transfer_budget: Duration::from_secs(120),
        ..loopback_config()
    };
    let (cluster_a, cluster_b) =
        two_node_cluster_with_config("cluster-it-leave-hand-off-crashed", config).await;
    let (a, b) = (cluster_a.node_id(), cluster_b.node_id());
    let both = Arc::new(OwnershipView::compute_at(
        a,
        vec![a, b],
        owners,
        Granularity::Part,
    ));
    let (_shard, _view_tx) = register_leaver_shard(&cluster_a, &name, owners, both);
    cluster_b.crash().await;
    assert!(
        cluster_a.live_peer_ids().contains(&b),
        "a still counts b live when it starts to leave"
    );

    let (events, _guard) = Events::capture();
    tokio::time::timeout(Duration::from_secs(60), cluster_a.shutdown())
        .await
        .expect("a leaves long before its state_transfer_budget");

    let warned = events.at(tracing::Level::WARN, "parts still diverging");
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert!(
        warned[0].contains("reason=the co-owner left the live set"),
        "{warned:?}"
    );
}
