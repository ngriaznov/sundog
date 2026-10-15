//! Tests of the pure decision rules: record reads, startup waits, reconcile bounds, load routing
//! and config validation.

use super::*;
use crate::cache::fetch::{decode_live_value, record_reads};
use crate::cache::open::should_await_first_peer;
use crate::cache::reconcile::{
    RECONCILE_MAX_ROUNDS, ReconcileBudget, retry_delay, should_keep_reconciling, split_round_result,
};

/// `record_reads` names what `decode_live_value` makes of a fetched
/// record: a value exactly when it decodes one, and otherwise why not.
#[test]
fn record_reads_names_what_decode_live_value_makes_of_a_record() {
    let now = now_ms();
    let value = |v: &str| Some(bytes::Bytes::from(postcard::to_stdvec(v).expect("encodes")));
    let record = |value: Option<bytes::Bytes>, expires_at_ms: Option<u64>| WireRecord {
        key: bytes::Bytes::from_static(b"k"),
        value,
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1u64),
        },
        expires_at_ms,
    };
    let hour = 3_600_000;
    let cases = [
        (record(value("v"), None), Reads::Value),
        (record(value("v"), Some(now + hour)), Reads::Value),
        (record(value("v"), Some(now - hour)), Reads::Expired),
        (record(None, None), Reads::Deleted),
        (record(None, Some(now - hour)), Reads::Deleted),
        (
            record(Some(bytes::Bytes::from_static(&[0xff])), None),
            Reads::Undecodable,
        ),
    ];
    for (rec, reads) in cases {
        assert_eq!(record_reads::<String>(&rec, now), reads, "{rec:?}");
        assert_eq!(
            decode_live_value::<String>(&rec).is_some(),
            reads == Reads::Value,
            "{rec:?}"
        );
    }
}

#[test]
fn should_await_first_peer_only_when_seeded_and_genuinely_alone() {
    assert!(
        should_await_first_peer(true, false),
        "seeds present and peers empty waits"
    );
    assert!(
        !should_await_first_peer(false, false),
        "no seeds does not wait, even with peers still empty"
    );
    assert!(
        !should_await_first_peer(true, true),
        "peers already known does not wait, even with seeds configured"
    );
    assert!(
        !should_await_first_peer(false, true),
        "no seeds and peers already known: nothing to wait for either way"
    );
}

#[test]
fn trust_sole_owner_at_open_only_distrusts_a_timed_out_warm_reopen() {
    assert!(
        trust_sole_owner_at_open(true, 0),
        "cold open, membership settled: trusted"
    );
    assert!(
        trust_sole_owner_at_open(true, 3),
        "warm reopen, membership settled: trusted"
    );
    assert!(
        trust_sole_owner_at_open(false, 0),
        "cold open, membership wait timed out: still trusted, nothing replayed to distrust"
    );
    assert!(
        !trust_sole_owner_at_open(false, 3),
        "warm reopen, membership wait timed out: not trusted, a replayed bucket found owned \
         alone could be the replay's own stale echo of ownership"
    );
}

#[test]
fn reconcile_budget_from_config_takes_its_byte_time_and_backoff_bounds_from_the_config() {
    let config = ClusterConfig {
        reconcile_byte_budget: 12_345,
        state_transfer_budget: Duration::from_millis(4_321),
        ae_interval: Duration::from_millis(765),
        ..ClusterConfig::default()
    };
    let budget = ReconcileBudget::from_config(&config);
    assert_eq!(budget.max_rounds, RECONCILE_MAX_ROUNDS);
    assert_eq!(budget.byte_budget, 12_345);
    assert_eq!(budget.time_budget, Duration::from_millis(4_321));
    assert_eq!(budget.backoff_cap, Duration::from_millis(765));
}

/// Pins that `should_keep_reconciling` stops once `elapsed` alone reaches `time_budget`.
#[test]
fn should_keep_reconciling_stops_once_the_time_budget_is_spent() {
    let budget = ReconcileBudget {
        max_rounds: 3,
        byte_budget: 1_000,
        time_budget: Duration::from_secs(1),
        backoff_cap: Duration::from_millis(200),
    };
    for (elapsed, expect_continue) in [
        (Duration::from_millis(999), true),
        (Duration::from_secs(1), false),
        (Duration::from_secs(2), false),
    ] {
        assert_eq!(
            should_keep_reconciling(0, 0, 5, elapsed, &budget),
            expect_continue,
            "elapsed {elapsed:?}"
        );
    }
}

/// Pins that `retry_delay` doubles from `RECONCILE_RETRY_BASE` per
/// failure, caps at `backoff_cap`, and stops at the time budget.
#[test]
fn retry_delay_doubles_from_the_base_caps_at_the_backoff_cap_and_stops_at_the_time_budget() {
    struct Case {
        name: &'static str,
        consecutive_failures: u32,
        elapsed: Duration,
        expect: Option<Duration>,
    }
    let budget = ReconcileBudget {
        max_rounds: 3,
        byte_budget: 1_000,
        time_budget: Duration::from_secs(1),
        backoff_cap: Duration::from_millis(500),
    };
    let cases = [
        Case {
            name: "first failure waits the base delay",
            consecutive_failures: 1,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(200)),
        },
        Case {
            name: "second failure doubles it",
            consecutive_failures: 2,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(400)),
        },
        Case {
            name: "third failure caps at the backoff cap",
            consecutive_failures: 3,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(500)),
        },
        Case {
            name: "a failure count past the shift width still caps, never overflows",
            consecutive_failures: 40,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(500)),
        },
        Case {
            name: "a zero failure count is treated as the first",
            consecutive_failures: 0,
            elapsed: Duration::ZERO,
            expect: Some(Duration::from_millis(200)),
        },
        Case {
            name: "a wait that still ends inside the time budget is taken",
            consecutive_failures: 1,
            elapsed: Duration::from_millis(799),
            expect: Some(Duration::from_millis(200)),
        },
        Case {
            name: "a wait that would land exactly on the deadline stops the loop",
            consecutive_failures: 1,
            elapsed: Duration::from_millis(800),
            expect: None,
        },
        Case {
            name: "time budget already spent",
            consecutive_failures: 1,
            elapsed: Duration::from_secs(1),
            expect: None,
        },
        Case {
            name: "time budget already exceeded",
            consecutive_failures: 2,
            elapsed: Duration::from_secs(3),
            expect: None,
        },
    ];
    for case in cases {
        assert_eq!(
            retry_delay(case.consecutive_failures, case.elapsed, &budget),
            case.expect,
            "case: {}",
            case.name
        );
    }
}

/// Pins that `should_keep_reconciling` stops at whichever bound (rounds,
/// bytes, time, or nothing left diverging) is hit first.
#[test]
fn should_keep_reconciling_stops_at_whichever_bound_is_hit_first() {
    struct Case {
        name: &'static str,
        rounds_run: u32,
        bytes_moved: u64,
        still_diverging: usize,
        elapsed: Duration,
        expect_continue: bool,
    }
    let budget = ReconcileBudget {
        max_rounds: 3,
        byte_budget: 1_000,
        time_budget: Duration::from_secs(1),
        backoff_cap: Duration::from_millis(200),
    };
    let cases = [
        Case {
            name: "rounds exhausted",
            rounds_run: 3,
            bytes_moved: 0,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "rounds past the cap",
            rounds_run: 4,
            bytes_moved: 0,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "byte budget exhausted",
            rounds_run: 0,
            bytes_moved: 1_000,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "byte budget exceeded",
            rounds_run: 0,
            bytes_moved: 1_001,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "nothing left diverging",
            rounds_run: 0,
            bytes_moved: 0,
            still_diverging: 0,
            elapsed: Duration::ZERO,
            expect_continue: false,
        },
        Case {
            name: "one round ran with zero progress, two left",
            rounds_run: 1,
            bytes_moved: 0,
            still_diverging: 5,
            elapsed: Duration::ZERO,
            expect_continue: true,
        },
        Case {
            name: "under every bound with work left and the time budget nearly spent",
            rounds_run: 1,
            bytes_moved: 10,
            still_diverging: 2,
            elapsed: Duration::from_millis(999),
            expect_continue: true,
        },
    ];
    for case in cases {
        assert_eq!(
            should_keep_reconciling(
                case.rounds_run,
                case.bytes_moved,
                case.still_diverging,
                case.elapsed,
                &budget
            ),
            case.expect_continue,
            "case: {}",
            case.name
        );
    }
}

/// Pins that `split_round_result` folds a `failed` round as every bucket
/// diverging regardless of `matched`, else splits by membership in it.
#[test]
fn split_round_result_folds_a_failed_round_and_otherwise_splits_by_matched() {
    fn parts(raw: &[u16]) -> HashSet<PartId> {
        raw.iter().map(|&r| PartId::from_raw(r)).collect()
    }
    struct Case {
        name: &'static str,
        requested: &'static [u16],
        outcome: anti_entropy::PartRoundOutcome,
        expect_converged: &'static [u16],
        expect_diverging: &'static [u16],
    }
    let cases = [
        Case {
            name: "a failed round leaves every requested bucket diverging, even one \
                   nominally in matched",
            requested: &[1, 2, 3],
            outcome: anti_entropy::PartRoundOutcome {
                matched: parts(&[1]),
                still_diverged: HashSet::new(),
                bytes_moved: 0,
                failed: true,
            },
            expect_converged: &[],
            expect_diverging: &[1, 2, 3],
        },
        Case {
            name: "a successful round splits matched from still-diverging",
            requested: &[1, 2, 3],
            outcome: anti_entropy::PartRoundOutcome {
                matched: parts(&[1, 3]),
                still_diverged: parts(&[2]),
                bytes_moved: 500,
                failed: false,
            },
            expect_converged: &[1, 3],
            expect_diverging: &[2],
        },
        Case {
            name: "everything requested matched: nothing left diverging",
            requested: &[4, 5],
            outcome: anti_entropy::PartRoundOutcome {
                matched: parts(&[4, 5]),
                still_diverged: HashSet::new(),
                bytes_moved: 0,
                failed: false,
            },
            expect_converged: &[4, 5],
            expect_diverging: &[],
        },
    ];
    for case in cases {
        let requested: Vec<PartId> = case
            .requested
            .iter()
            .map(|&r| PartId::from_raw(r))
            .collect();
        let (converged, diverging) = split_round_result(&requested, &case.outcome);
        let raw = |parts: Vec<PartId>| -> Vec<u16> { parts.into_iter().map(PartId::raw).collect() };
        assert_eq!(raw(converged), case.expect_converged, "case: {}", case.name);
        assert_eq!(raw(diverging), case.expect_diverging, "case: {}", case.name);
    }
}

#[test]
fn routes_loads_only_where_values_do_not_reach_every_node() {
    assert!(!routes_loads(Mode::Local));
    assert!(!routes_loads(Mode::Replicated));
    assert!(routes_loads(Mode::Invalidation));
    assert!(routes_loads(Mode::distributed()));
}

#[test]
fn load_target_in_distributed_takes_the_first_owner_that_can_load() {
    let me = NodeId::from(1);
    let (a, b) = (NodeId::from(2), NodeId::from(3));
    let part = PartId::from_raw(7);
    let both: HashSet<NodeId> = [a, b].into_iter().collect();
    let mode = Mode::distributed();
    let none = HashSet::new();
    assert_eq!(
        load_target(mode, part, me, &[a, b], &both, &none),
        LoadTarget::Peer(a)
    );
    assert_eq!(
        load_target(mode, part, me, &[a, b], &[b].into_iter().collect(), &none),
        LoadTarget::Peer(b),
        "an owner with no loader is skipped"
    );
    assert_eq!(
        load_target(mode, part, me, &[me, a], &both, &none),
        LoadTarget::Here,
        "this node, as the first owner, loads its own keys"
    );
    assert_eq!(
        load_target(mode, part, me, &[a, b], &HashSet::new(), &none),
        LoadTarget::Here,
        "with no owner able to load, this node loads"
    );
    assert_eq!(
        load_target(mode, part, me, &[], &both, &none),
        LoadTarget::Here
    );
}

#[test]
fn load_target_in_distributed_skips_the_owners_already_tried() {
    let me = NodeId::from(1);
    let (a, b, c) = (NodeId::from(2), NodeId::from(3), NodeId::from(4));
    let part = PartId::from_raw(7);
    let capable: HashSet<NodeId> = [a, b, c].into_iter().collect();
    let mode = Mode::distributed();
    let tried = |nodes: &[NodeId]| nodes.iter().copied().collect::<HashSet<_>>();
    assert_eq!(
        load_target(mode, part, me, &[a, b], &capable, &tried(&[a])),
        LoadTarget::Peer(b),
        "the next owner takes what the first declined"
    );
    assert_eq!(
        load_target(mode, part, me, &[a, b, c], &capable, &tried(&[a, b])),
        LoadTarget::Peer(c),
        "every owner already tried is skipped"
    );
    assert_eq!(
        load_target(mode, part, me, &[a, b, c], &capable, &tried(&[b])),
        LoadTarget::Peer(a),
        "rank order holds, a later owner's refusal does not move the pick"
    );
    assert_eq!(
        load_target(mode, part, me, &[a, me, b], &capable, &tried(&[a])),
        LoadTarget::Here,
        "this node, as the next owner, loads its own keys"
    );
    assert_eq!(
        load_target(mode, part, me, &[a, b], &capable, &tried(&[a, b])),
        LoadTarget::Here,
        "with every owner tried, this node loads"
    );
    assert_eq!(
        load_target(
            mode,
            part,
            me,
            &[a, b, c],
            &[a, c].into_iter().collect(),
            &tried(&[a])
        ),
        LoadTarget::Peer(c),
        "an owner with no loader is skipped after the declined one"
    );
    assert_eq!(
        load_target(mode, part, me, &[], &capable, &tried(&[a])),
        LoadTarget::Here,
        "a part with no owners loads here"
    );
}

#[test]
fn load_target_elsewhere_moves_to_this_node_once_the_winner_is_tried() {
    let nodes: Vec<NodeId> = (1..=5).map(NodeId::from).collect();
    let me = nodes[0];
    let capable: HashSet<NodeId> = nodes[1..].iter().copied().collect();
    let mut peers_seen = 0;
    for raw in [0u16, 1, 99, 4_000, 65_535] {
        let part = PartId::from_raw(raw);
        let LoadTarget::Peer(winner) =
            load_target(Mode::Invalidation, part, me, &[], &capable, &HashSet::new())
        else {
            continue;
        };
        peers_seen += 1;
        assert_eq!(
            load_target(
                Mode::Invalidation,
                part,
                me,
                &[],
                &capable,
                &[winner].into_iter().collect()
            ),
            LoadTarget::Here,
            "part {raw}: a winner that declined leaves the key to this node"
        );
    }
    assert!(peers_seen > 0, "some part gathers on a peer");
}

#[test]
fn load_target_elsewhere_is_one_rendezvous_winner_every_node_agrees_on() {
    let nodes: Vec<NodeId> = (1..=5).map(NodeId::from).collect();
    for raw in [0u16, 1, 99, 4_000, 65_535] {
        let part = PartId::from_raw(raw);
        let picks: HashSet<NodeId> = nodes
            .iter()
            .map(|&me| {
                let capable: HashSet<NodeId> = nodes.iter().copied().filter(|&n| n != me).collect();
                match load_target(Mode::Invalidation, part, me, &[], &capable, &HashSet::new()) {
                    LoadTarget::Here => me,
                    LoadTarget::Peer(peer) => peer,
                }
            })
            .collect();
        assert_eq!(
            picks.len(),
            1,
            "part {raw}: every node picks the same loader"
        );
    }
    let me = NodeId::from(1);
    assert_eq!(
        load_target(
            Mode::Invalidation,
            PartId::from_raw(3),
            me,
            &[],
            &HashSet::new(),
            &HashSet::new()
        ),
        LoadTarget::Here,
        "alone, this node loads"
    );
    let spread: HashSet<NodeId> = (0..64u16)
        .map(|raw| {
            let capable: HashSet<NodeId> = nodes[1..].iter().copied().collect();
            match load_target(
                Mode::Invalidation,
                PartId::from_raw(raw),
                me,
                &[],
                &capable,
                &HashSet::new(),
            ) {
                LoadTarget::Here => me,
                LoadTarget::Peer(peer) => peer,
            }
        })
        .collect();
    assert!(spread.len() > 1, "parts spread across the loaders");
}

fn load_config(has_loader: bool, has_batch_window: bool) -> LoadConfig {
    LoadConfig {
        has_loader,
        has_batch_window,
        refresh_ahead: None,
        has_ttl: true,
        mode: Mode::Replicated,
    }
}

#[test]
fn validate_load_config_needs_a_loader_for_a_batch_window() {
    assert_eq!(validate_load_config(load_config(false, false)), Ok(None));
    assert_eq!(validate_load_config(load_config(true, false)), Ok(None));
    assert_eq!(validate_load_config(load_config(true, true)), Ok(None));
    assert_eq!(
        validate_load_config(load_config(false, true)),
        Err("batch_window needs a loader")
    );
}

#[test]
fn validate_load_config_checks_refresh_ahead() {
    let refresh = |fraction, has_loader, has_ttl, mode| {
        validate_load_config(LoadConfig {
            has_loader,
            has_batch_window: false,
            refresh_ahead: Some(fraction),
            has_ttl,
            mode,
        })
    };
    for mode in [Mode::Local, Mode::Replicated, Mode::distributed()] {
        assert_eq!(refresh(0.8, true, true, mode), Ok(Some(800)), "{mode:?}");
    }
    assert_eq!(
        refresh(0.8, false, true, Mode::Replicated),
        Err("refresh_ahead needs a loader")
    );
    assert_eq!(
        refresh(0.8, true, false, Mode::Replicated),
        Err("refresh_ahead needs a ttl")
    );
    assert_eq!(
        refresh(0.8, true, true, Mode::Invalidation),
        Err("refresh_ahead does not apply to Mode::Invalidation")
    );
    assert_eq!(
        refresh(1.0, true, true, Mode::Replicated),
        Err("refresh_ahead takes a fraction strictly between 0 and 1")
    );
}

#[test]
fn validate_merge_window_accepts_zero_regardless_of_the_resolver() {
    assert!(validate_merge_window(Duration::ZERO, false));
    assert!(validate_merge_window(Duration::ZERO, true));
}

#[test]
fn validate_merge_window_requires_a_merging_resolver_once_nonzero() {
    assert!(!validate_merge_window(Duration::from_millis(1), false));
    assert!(validate_merge_window(Duration::from_millis(1), true));
}
