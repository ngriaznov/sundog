//! The CRDT writer-retirement scenario: churn and restart under a PN-counter cache.

use crate::ae_round::ae_round_with_sketch;
use crate::handler::{ShardHandler, dispatch_inbound};
use crate::support::{
    SimResult, TICK, block_on, cache_name, peer_list_of, run_steps, run_until, sim_seed, steps_for,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use sundog::ConflictResolver;
use sundog::config::ClusterConfig;
use sundog::crdt::{PnCounter, PnCounterResolver, WriterId};
use sundog::membership::Peer;
use sundog::net::{InboundMsg, Mesh, RequestHandler};
use sundog::node::NodeId;
use sundog::store::{CompactionBounds, Mode, Quiescence, Shard, ShardOps};
use turmoil::{Builder, Sim};

// ---------------------------------------------------------------------------
// CRDT writer retirement: churn + restart scenario. `ConflictResolver::compact`/
// `ShardOps::compact_pass` (`sundog::store`) and `PnCounter::compact`'s
// effects (`sundog::crdt`) are reachable through the public API; the
// retirement-eligibility and cache-quiet predicates that decide *when* a
// real cluster calls them
// (`cluster.rs::crdt_writer_is_retirement_eligible`/`crdt_cache_is_quiet`)
// are `pub(crate)` and unreachable from an integration test, so -- exactly
// as this file already reimplements `cluster::rebalance`'s reaction to a
// membership change in `republish_view` above -- this section reimplements
// that eligibility rule directly against a hand-scripted membership
// timeline. `now_ms` is read from [`turmoil::Sim::elapsed`], so a "short
// `crdt_retire_after`" means short virtual time, not real wall-clock time.

/// One member's retirement-relevant state as this harness tracks it,
/// mirroring `cluster.rs::MemberView` (`pub(crate)`, unreachable from here).
#[derive(Debug, Clone, Copy)]
enum CrdtPresence {
    /// Continuously live under `incarnation` since `since_ms`.
    Present { since_ms: u64, incarnation: u64 },
    /// Continuously absent (dropped from the live view without a graceful
    /// departure) since `since_ms`.
    Absent { since_ms: u64 },
}

type CrdtMembership = HashMap<NodeId, CrdtPresence>;

/// Whether writer `w` is dead: its node has been continuously absent for
/// at least `bound_ms`, or is live under any *other* incarnation than
/// `w`'s own. No ordering is required, so a clock stepping backward across
/// a restart can never pin an old incarnation forever.
fn crdt_writer_is_dead(w: WriterId, presence: CrdtPresence, now_ms: u64, bound_ms: u64) -> bool {
    match presence {
        CrdtPresence::Absent { since_ms } => now_ms.saturating_sub(since_ms) >= bound_ms,
        CrdtPresence::Present { incarnation, .. } => incarnation != w.incarnation(),
    }
}

/// Whether `presence` has held unchanged, continuously present or
/// continuously absent alike, for at least `bound_ms`: the per-member
/// half of the cache-wide quiet rule.
fn crdt_member_is_settled(presence: CrdtPresence, now_ms: u64, bound_ms: u64) -> bool {
    let since_ms = match presence {
        CrdtPresence::Present { since_ms, .. } | CrdtPresence::Absent { since_ms } => since_ms,
    };
    now_ms.saturating_sub(since_ms) >= bound_ms
}

/// The cache-wide quiet rule: every member other than `local` is
/// settled.
fn crdt_cache_is_quiet(
    members: &CrdtMembership,
    local: NodeId,
    now_ms: u64,
    bound_ms: u64,
) -> Quiescence {
    Quiescence::from_settled(
        members
            .iter()
            .filter(|&(&node, _)| node != local)
            .all(|(_, &presence)| crdt_member_is_settled(presence, now_ms, bound_ms)),
    )
}

/// This simulation's virtual time so far, in milliseconds.
fn elapsed_ms(sim: &Sim<'_>) -> u64 {
    u64::try_from(sim.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// One compaction sweep tick, reimplementing `cluster.rs::crdt_compact_task`'s
/// own per-tick logic against this harness's hand-scripted `members`:
/// builds the `retire` closure from [`crdt_writer_is_dead`] (excluding
/// `local`, which never retires its own slot) and `quiet` from
/// [`crdt_cache_is_quiet`], then drives [`ShardOps::compact_pass`] directly
/// from the test thread -- the same "drive the production task's own logic
/// by hand" idiom [`republish_view`] already uses for rebalance above.
fn run_compact_tick<S: ShardOps>(
    shard: &S,
    local: NodeId,
    members: &CrdtMembership,
    now_ms: u64,
    bound_ms: u64,
    batch: usize,
) -> (Vec<WriterId>, usize) {
    let quiet = crdt_cache_is_quiet(members, local, now_ms, bound_ms);
    let members_for_retire = members.clone();
    let retire = move |w: WriterId| {
        w.node() != local
            && members_for_retire
                .get(&w.node())
                .is_some_and(|&presence| crdt_writer_is_dead(w, presence, now_ms, bound_ms))
    };
    let outcome = block_on(ShardOps::compact_pass(
        shard,
        now_ms,
        &retire,
        quiet,
        CompactionBounds::three_bounds(bound_ms),
        batch,
    ));
    (outcome.retired, outcome.compacted)
}

/// A minimal per-node loop for the CRDT-compaction scenarios: inbound
/// dispatch plus periodic anti-entropy only, no write tick and no fan-out
/// -- every write and every compaction sweep in these scenarios is driven
/// directly from the test thread against the very same `Arc<Shard<..>>`
/// this loop shares, exactly as [`run_partition_heal`]'s own `write_side`
/// does against [`HealNode`] above. `incarnation` is read once per
/// (re)spawn, so [`Sim::bounce`] re-running this host's closure after the
/// test driver bumps it is what gives a restarted node a fresh membership
/// incarnation, mirroring how, in production, `Cluster::local_incarnation`
/// and `Mesh::spawn`'s own incarnation are the very same number.
async fn crdt_ae_only_loop<S: ShardOps + 'static>(
    shard: Arc<S>,
    bind_port: u16,
    node: NodeId,
    incarnation: u64,
    peer_list: Vec<Peer>,
    ae_period: Duration,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], bind_port));
    let (mesh, mut inbound) = Mesh::spawn(
        bind_addr,
        node,
        incarnation,
        &ClusterConfig::default(),
        handler,
    )
    .await?;
    mesh.update_peers(peer_list.clone());
    let peer_ids: Vec<NodeId> = peer_list.iter().map(|peer| peer.node).collect();
    let mut ae_tick = tokio::time::interval(ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(shard.as_ref(), msg).await;
            }
            _ = ae_tick.tick() => {
                for &peer in &peer_ids {
                    let _ = ae_round_with_sketch(&mesh, shard.as_ref(), peer, None).await;
                }
            }
        }
    }
}

/// One [`spawn_crdt_host`] call's fixed identity: everything about a node
/// that doesn't change across a [`Sim::bounce`] restart. Grouped into one
/// struct purely to keep `spawn_crdt_host` under clippy's argument-count
/// threshold -- each field is otherwise independent.
struct CrdtHostSpec {
    host: &'static str,
    node: NodeId,
    incarnation: Arc<AtomicU64>,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
}

/// Spawns [`crdt_ae_only_loop`] as a turmoil host, reading `spec.incarnation`
/// fresh on every (re)spawn so [`Sim::bounce`] gives a restarted node a new
/// membership incarnation.
fn spawn_crdt_host<S>(sim: &mut Sim<'_>, spec: CrdtHostSpec, shard: Arc<S>, ae_period: Duration)
where
    S: ShardOps + 'static,
{
    let CrdtHostSpec {
        host,
        node,
        incarnation,
        port,
        peers,
    } = spec;
    sim.host(host, move || {
        let shard = Arc::clone(&shard);
        let peer_list = peer_list_of(&peers);
        let inc = incarnation.load(Ordering::SeqCst);
        async move { crdt_ae_only_loop(shard, port, node, inc, peer_list, ae_period).await }
    });
}

/// Churn and restart under [`PnCounterResolver`]:
/// a three-node replicated [`PnCounter`] cache with a short, virtual-time
/// `crdt_retire_after` (`BOUND_MS`). Node C is crashed and quickly bounced
/// back under a fresh incarnation (absence under the bound: retirement
/// defers), then crashed again for longer than the bound (absence over the
/// bound: retirement fires once every member has settled). Exact totals
/// are checked on every node at every phase. Stage one has two distinct
/// triggers with different timing: a writer dead by *supersession* (a new
/// incarnation of its own node is now live) is retired on the very next
/// tick, with no quiet wait at all, while a writer dead by *absence*
/// (its node has dropped out of view) still needs `crdt_retire_after` to
/// elapse. Stage two folds a retirement's per-writer metadata into the
/// bounded scalar accumulator only once quiet AND aged past
/// `2 * BOUND_MS`, even with a second writer retired in the meantime, so
/// the record does not keep growing with churn.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario's full churn schedule and assertions read best kept together, \
              mirroring distributed_rebalance_under_churn's own allow above"
)]
fn crdt_compaction_pncounter_churn_and_restart_retires_and_folds_exactly() {
    const PORT: u16 = 5210;
    const BOUND_MS: u64 = 2_000;
    const BATCH: usize = 64;
    let key = "ctr".to_string();

    let node_a = NodeId::from(9201);
    let node_b = NodeId::from(9202);
    let node_c = NodeId::from(9203);
    let (host_a, host_b, host_c) = ("crdt-pn-a", "crdt-pn-b", "crdt-pn-c");

    let make_shard = |node: NodeId| -> Arc<Shard<String, PnCounter>> {
        Arc::new(
            Shard::new(cache_name(), Mode::Replicated, node, 10_000, None, None)
                .with_resolver(Arc::new(PnCounterResolver) as Arc<dyn ConflictResolver>),
        )
    };
    let shard_a = make_shard(node_a);
    let shard_b = make_shard(node_b);
    let shard_c = make_shard(node_c);

    let inc_c = Arc::new(AtomicU64::new(1));
    let ae_period = Duration::from_millis(50);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xC8D7_1001))
        .tick_duration(TICK)
        .build();

    spawn_crdt_host(
        &mut sim,
        CrdtHostSpec {
            host: host_a,
            node: node_a,
            incarnation: Arc::new(AtomicU64::new(1)),
            port: PORT,
            peers: vec![(node_b, host_b, PORT), (node_c, host_c, PORT)],
        },
        Arc::clone(&shard_a),
        ae_period,
    );
    spawn_crdt_host(
        &mut sim,
        CrdtHostSpec {
            host: host_b,
            node: node_b,
            incarnation: Arc::new(AtomicU64::new(1)),
            port: PORT,
            peers: vec![(node_a, host_a, PORT), (node_c, host_c, PORT)],
        },
        Arc::clone(&shard_b),
        ae_period,
    );
    spawn_crdt_host(
        &mut sim,
        CrdtHostSpec {
            host: host_c,
            node: node_c,
            incarnation: Arc::clone(&inc_c),
            port: PORT,
            peers: vec![(node_a, host_a, PORT), (node_b, host_b, PORT)],
        },
        Arc::clone(&shard_c),
        ae_period,
    );

    let writer_alpha = WriterId::new(node_a, 1);
    let writer_beta = WriterId::new(node_b, 1);
    let writer_gamma1 = WriterId::new(node_c, 1);

    block_on(shard_a.insert(key.clone(), PnCounter::local_delta(writer_alpha, 10))).unwrap();
    block_on(shard_b.insert(key.clone(), PnCounter::local_delta(writer_beta, 20))).unwrap();
    block_on(shard_c.insert(key.clone(), PnCounter::local_delta(writer_gamma1, 30))).unwrap();

    let value_on = |shard: &Arc<Shard<String, PnCounter>>| -> Option<i128> {
        block_on(shard.get(&key)).map(|c| c.value())
    };

    run_until(&mut sim, steps_for(Duration::from_secs(2)), || {
        value_on(&shard_a) == Some(60)
            && value_on(&shard_b) == Some(60)
            && value_on(&shard_c) == Some(60)
    })
    .expect("the three writers' initial deltas converge to the exact total on every node");

    let mut members: CrdtMembership = HashMap::new();
    for &(node, inc) in &[(node_a, 1u64), (node_b, 1u64), (node_c, 1u64)] {
        members.insert(
            node,
            CrdtPresence::Present {
                since_ms: 0,
                incarnation: inc,
            },
        );
    }

    // -- Phase 1: crash C; less than the bound must defer retirement
    // everywhere, and no data is lost while it defers. --
    sim.crash(host_c);
    members.insert(
        node_c,
        CrdtPresence::Absent {
            since_ms: elapsed_ms(&sim),
        },
    );
    run_steps(&mut sim, steps_for(Duration::from_millis(BOUND_MS / 2)));
    let now = elapsed_ms(&sim);
    let (retired_a, _) = run_compact_tick(shard_a.as_ref(), node_a, &members, now, BOUND_MS, BATCH);
    let (retired_b, _) = run_compact_tick(shard_b.as_ref(), node_b, &members, now, BOUND_MS, BATCH);
    assert!(
        retired_a.is_empty() && retired_b.is_empty(),
        "C absent for only half the bound: retirement must defer on every node, got a={retired_a:?} b={retired_b:?}"
    );
    assert_eq!(
        value_on(&shard_a),
        Some(60),
        "no data is lost while retirement is deferred"
    );
    assert_eq!(value_on(&shard_b), Some(60));

    // -- Phase 2: bounce C under a fresh incarnation. Restart never loses
    // C's pre-restart total, and C resumes contributing under its new slot
    // immediately. --
    inc_c.store(2, Ordering::SeqCst);
    sim.bounce(host_c);
    members.insert(
        node_c,
        CrdtPresence::Present {
            since_ms: elapsed_ms(&sim),
            incarnation: 2,
        },
    );
    let writer_gamma2 = WriterId::new(node_c, 2);
    block_on(shard_c.insert(key.clone(), PnCounter::local_delta(writer_gamma2, 5))).unwrap();

    run_until(&mut sim, steps_for(Duration::from_secs(2)), || {
        value_on(&shard_a) == Some(65)
            && value_on(&shard_b) == Some(65)
            && value_on(&shard_c) == Some(65)
    })
    .expect("C's post-restart write converges on top of its untouched pre-restart total");

    // -- Phase 3: stage one runs on `retire` alone, with no quiet
    // requirement (`crdt_writer_is_retirement_eligible`'s doc: "stage one
    // ... runs unconditionally on `retire` alone"). The moment C's restart
    // makes incarnation 2 live, writer_gamma1 (incarnation 1) is
    // unambiguously dead by supersession -- no absence timer involved, so
    // there is nothing for a quiet wait to bound -- and the very next
    // compaction tick on any node retires it, independently, stage one,
    // exact, value unchanged. --
    run_steps(&mut sim, steps_for(Duration::from_millis(BOUND_MS / 2)));
    let stage_one_now = elapsed_ms(&sim);
    let (retired_a, compacted_a) = run_compact_tick(
        shard_a.as_ref(),
        node_a,
        &members,
        stage_one_now,
        BOUND_MS,
        BATCH,
    );
    assert_eq!(
        retired_a,
        vec![writer_gamma1],
        "C's pre-restart incarnation is dead by supersession the instant the restart is \
         observed, independent of any quiet wait: it is retired on the very next tick, got \
         {retired_a:?}"
    );
    assert_eq!(
        compacted_a, 1,
        "exactly the one touched record was compacted"
    );
    assert_eq!(
        value_on(&shard_a),
        Some(65),
        "stage one never changes the counter's value"
    );

    let (retired_b, _) = run_compact_tick(
        shard_b.as_ref(),
        node_b,
        &members,
        stage_one_now,
        BOUND_MS,
        BATCH,
    );
    assert_eq!(
        retired_b,
        vec![writer_gamma1],
        "B independently reaches the same retirement decision"
    );

    // Let A's and B's stage-one-compacted records replicate around, and
    // confirm the total is still exact everywhere, C included, after a
    // real anti-entropy round carried the compacted bytes.
    run_until(&mut sim, steps_for(Duration::from_secs(2)), || {
        value_on(&shard_a) == Some(65)
            && value_on(&shard_b) == Some(65)
            && value_on(&shard_c) == Some(65)
    })
    .expect("the compacted record still carries the exact total once it replicates");

    // -- Phase 4: crash C for longer than the bound this time (its own
    // absence, not merely superseded by a restart), and confirm its live
    // (incarnation-2) writer is retired the same way once quiet again. --
    sim.crash(host_c);
    members.insert(
        node_c,
        CrdtPresence::Absent {
            since_ms: elapsed_ms(&sim),
        },
    );
    run_steps(&mut sim, steps_for(Duration::from_millis(BOUND_MS - 200)));
    let now = elapsed_ms(&sim);
    let (retired_a, _) = run_compact_tick(shard_a.as_ref(), node_a, &members, now, BOUND_MS, BATCH);
    assert!(
        retired_a.is_empty(),
        "C has been absent for less than the bound: still deferred, got {retired_a:?}"
    );

    run_steps(&mut sim, steps_for(Duration::from_millis(400)));
    let stage_one_now_2 = elapsed_ms(&sim);
    let (retired_a, _) = run_compact_tick(
        shard_a.as_ref(),
        node_a,
        &members,
        stage_one_now_2,
        BOUND_MS,
        BATCH,
    );
    assert_eq!(
        retired_a,
        vec![writer_gamma2],
        "C's second, longer absence retires its (still-live-writer) incarnation-2 slot"
    );
    assert_eq!(
        value_on(&shard_a),
        Some(65),
        "the total is still exact with both of C's incarnations retired"
    );

    // -- Stage two: once writer_gamma1's retirement has aged past `2 *
    // BOUND_MS` and the cache is still quiet, its per-writer metadata
    // folds into the bounded scalar accumulator and the record shrinks,
    // even though a *second* writer (writer_gamma2) has meanwhile also been
    // retired -- the record's metadata does not keep growing with churn.
    let size_with_two_retired = block_on(shard_a.get(&key)).unwrap().encode().unwrap().len();

    run_steps(&mut sim, steps_for(Duration::from_millis(2 * BOUND_MS)));
    let stage_two_now = elapsed_ms(&sim);
    let (retired_again, compacted_again) = run_compact_tick(
        shard_a.as_ref(),
        node_a,
        &members,
        stage_two_now,
        BOUND_MS,
        BATCH,
    );
    assert!(
        retired_again.is_empty(),
        "stage two folds an already-retired writer further; it never re-retires it as new, got {retired_again:?}"
    );
    assert!(
        compacted_again > 0,
        "stage two must change the resident record"
    );
    let size_after_stage_two = block_on(shard_a.get(&key)).unwrap().encode().unwrap().len();
    assert!(
        size_after_stage_two < size_with_two_retired,
        "stage two drops writer_gamma1's whole per-writer retired entry in favor of the two-scalar \
         accumulator, so the record must shrink even with a second writer (writer_gamma2) also \
         retired in the meantime: with_two_retired={size_with_two_retired} \
         after_stage_two={size_after_stage_two}"
    );
    assert_eq!(
        value_on(&shard_a),
        Some(65),
        "stage two never changes the counter's value"
    );

    run_until(&mut sim, steps_for(Duration::from_secs(2)), || {
        value_on(&shard_a) == Some(65) && value_on(&shard_b) == Some(65)
    })
    .expect("the stage-two-folded record still carries the exact total once replicated");
}
