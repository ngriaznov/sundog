//! The partition-heal run driver and its metric printers.

use super::counting::{CountingResolver, CountingShard};
use super::{HealConfig, HealMetrics, Variant};
use crate::handler::{ShardHandler, dispatch_inbound};
use crate::support::{SimResult, TICK, block_on, peer_list_of, run_steps, run_until, steps_for};
use smol_str::SmolStr;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use sundog::ConflictResolver;
use sundog::config::ClusterConfig;
use sundog::crdt::{PnCounter, PnCounterResolver, WriterId};
use sundog::net::{InboundMsg, Mesh, RequestHandler};
use sundog::node::NodeId;
use sundog::store::{Mode, Shard, ShardOps};
use turmoil::{Builder, Sim};
use xxhash_rust::xxh3::xxh3_64;

/// One node's three handles onto the same real shard: `real` for direct
/// local writes and reads from the test driver, `push` wrapping it for the
/// inbound-dispatch path, `pull`/`pull_dyn` wrapping it (the same instance,
/// two views) for the anti-entropy-initiator path through
/// `sundog::run_round_against`. See [`CountingShard`]'s own doc for why push
/// and pull need separate wrapper instances at all.
struct HealNode {
    real: Arc<Shard<String, PnCounter>>,
    push: Arc<CountingShard<Shard<String, PnCounter>>>,
    pull: Arc<CountingShard<Shard<String, PnCounter>>>,
    pull_dyn: Arc<dyn ShardOps>,
}

impl HealNode {
    fn new(real: Arc<Shard<String, PnCounter>>) -> Self {
        let push = Arc::new(CountingShard::new(Arc::clone(&real)));
        let pull = Arc::new(CountingShard::new(Arc::clone(&real)));
        let pull_dyn: Arc<dyn ShardOps> = Arc::clone(&pull) as Arc<dyn ShardOps>;
        Self {
            real,
            push,
            pull,
            pull_dyn,
        }
    }

    fn records(&self) -> u64 {
        self.push.records() + self.pull.records()
    }

    fn redundant(&self) -> u64 {
        self.push.redundant() + self.pull.redundant()
    }

    fn redundant_pulls(&self) -> u64 {
        self.pull.redundant()
    }
}

fn merged_key(i: u32) -> String {
    format!("counter:{i}")
}

fn decomposed_key(i: u32, side: &str) -> String {
    format!("counter:{i}:{side}")
}

/// How many of `keys` counters land in the both-sides-written overlap:
/// `conflict_fraction * keys`, rounded and clamped into `0..=keys`.
fn overlap_count(keys: u32, conflict_fraction: f64) -> u32 {
    let scaled = (f64::from(keys) * conflict_fraction).round();
    if scaled <= 0.0 {
        return 0;
    }
    if scaled >= f64::from(keys) {
        return keys;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "scaled is already clamped into (0.0, f64::from(keys)) by the checks above, so \
                  this truncating, sign-losing cast can never truncate or flip sign"
    )]
    let overlap = scaled as u32;
    overlap
}

/// The exact grand total every counter converges to: side A contributes
/// `increments_per_side` to each of the `overlap_count` counters it writes,
/// side B contributes `increments_per_side` to every one of `keys` counters
/// (its write set is the overlap plus the rest), and `PnCounter::local_delta`
/// is cumulative, so only each writer's final round survives the merge.
pub(super) fn expected_total(keys: u32, conflict_fraction: f64, increments_per_side: u32) -> i128 {
    let overlap = overlap_count(keys, conflict_fraction);
    let inc = i128::from(increments_per_side);
    (i128::from(keys) + i128::from(overlap)) * inc
}

/// Writes `increments_per_side` cumulative rounds to every counter in
/// `range` from `writer`, under `side`'s label -- `"a"` or `"b"` -- keyed per
/// [`Variant`]: merged writes the shared `counter:{i}` key every writer
/// folds into; decomposed writes this side's own `counter:{i}:{side}` key,
/// never contended since no other writer ever touches it.
fn write_side(
    shard: &Shard<String, PnCounter>,
    writer: WriterId,
    side: &'static str,
    range: std::ops::Range<u32>,
    variant: Variant,
    increments_per_side: u32,
) {
    block_on(async {
        for round in 1..=increments_per_side {
            for i in range.clone() {
                let key = match variant {
                    Variant::Merged => merged_key(i),
                    Variant::Decomposed => decomposed_key(i, side),
                };
                let _ = shard
                    .insert(key, PnCounter::local_delta(writer, u64::from(round)))
                    .await;
            }
        }
    });
}

/// Reads a shard's converged grand total back out, matching [`write_side`]'s
/// own key scheme per [`Variant`]: one shared key per counter for merged, two
/// per-side keys per counter for decomposed.
fn heal_digests_of(shard: &Shard<String, PnCounter>) -> Vec<(u16, u64)> {
    block_on(ShardOps::digests(shard))
        .into_iter()
        .map(|bd| (bd.bucket, bd.digest))
        .collect()
}

fn counter_total(shard: &Shard<String, PnCounter>, keys: u32, variant: Variant) -> i128 {
    block_on(async {
        let mut total = 0i128;
        for i in 0..keys {
            match variant {
                Variant::Merged => {
                    if let Some(counter) = shard.get(&merged_key(i)).await {
                        total += counter.value();
                    }
                }
                Variant::Decomposed => {
                    for side in ["a", "b"] {
                        if let Some(counter) = shard.get(&decomposed_key(i, side)).await {
                            total += counter.value();
                        }
                    }
                }
            }
        }
        total
    })
}

/// A namespace unique to `cfg`: folded into every host name, the cache name,
/// and every node id this run builds, so two `run_partition_heal` calls in
/// one process -- sequential or, under `cargo test`'s default parallelism,
/// concurrent -- never share a host, a cache-labeled metric, or a node id,
/// regardless of how many fields they otherwise have in common.
fn heal_run_id(cfg: &HealConfig) -> String {
    let tag = cfg.variant.tag();
    let hash = xxh3_64(
        format!(
            "{tag}-{}-{}-{}-{}-{}",
            cfg.seed, cfg.keys, cfg.conflict_fraction, cfg.increments_per_side, cfg.ae_interval_ms
        )
        .as_bytes(),
    );
    format!("heal-{tag}-{hash:016x}")
}

fn heal_node_id(run_id: &str, role: &str) -> NodeId {
    NodeId::from(xxh3_64(format!("{run_id}-{role}").as_bytes()))
}

/// A partition-heal node's whole role: dispatch inbound traffic through its
/// `push` [`CountingShard`], and run `sundog::run_round_against` -- the
/// production anti-entropy round, not a reimplementation -- against every
/// peer on `peers` on a timer through its `pull` one. Writes are applied
/// directly to `real` from outside this loop, the same direct-write pattern
/// `run_partition_delete_scenario` uses, so this loop only has to keep the
/// mesh alive and reconcile.
///
/// `ae_rounds` only counts a round against a peer in `counted_peers`: node
/// b and node c are never partitioned from each other, so their own mutual
/// rounds settle before the measurement window opens and stay a routine,
/// always-no-op background rate the same as with any other steady pair
/// (see `run_partition_heal`'s own pre-heal settle step) -- counting them
/// alongside the rounds that repair the a/b-c split would mix an
/// irrelevant, scale-independent tick rate into a metric meant to measure
/// the repair itself.
#[derive(Clone)]
struct HealNodeParams {
    node: NodeId,
    port: u16,
    peers: Vec<(NodeId, String, u16)>,
    counted_peers: Vec<NodeId>,
    ae_period: Duration,
    ae_rounds: Arc<AtomicU64>,
    cache: SmolStr,
    outbox_capacity: usize,
}

async fn heal_node_loop(
    params: HealNodeParams,
    real: Arc<Shard<String, PnCounter>>,
    push: Arc<CountingShard<Shard<String, PnCounter>>>,
    pull: Arc<dyn ShardOps>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(real));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], params.port));
    let config = ClusterConfig::default().with(|c| c.outbox_capacity = params.outbox_capacity);
    let (mesh, mut inbound) = Mesh::spawn(bind_addr, params.node, 1, &config, handler).await?;

    let borrowed: Vec<(NodeId, &str, u16)> = params
        .peers
        .iter()
        .map(|(node, host, port)| (*node, host.as_str(), *port))
        .collect();
    let peer_list = peer_list_of(&borrowed);
    mesh.update_peers(peer_list.clone());
    let peer_ids: Vec<NodeId> = peer_list.iter().map(|peer| peer.node).collect();

    let mut ae_tick = tokio::time::interval(params.ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(push.as_ref(), msg).await;
            }
            _ = ae_tick.tick() => {
                for &peer in &peer_ids {
                    sundog::run_round_against(&mesh, &pull, &params.cache, peer).await;
                    if params.counted_peers.contains(&peer) {
                        params.ae_rounds.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one node's registration: host, identity, its peer roster, the shared per-run \
              counters, and its own counting-shard handles all belong together at one call site"
)]
fn spawn_heal_host(
    sim: &mut Sim<'_>,
    host: &str,
    node: NodeId,
    port: u16,
    peers: Vec<(NodeId, String, u16)>,
    counted_peers: Vec<NodeId>,
    ae_period: Duration,
    ae_rounds: Arc<AtomicU64>,
    cache: SmolStr,
    outbox_capacity: usize,
    heal_node: &HealNode,
) {
    let params = HealNodeParams {
        node,
        port,
        peers,
        counted_peers,
        ae_period,
        ae_rounds,
        cache,
        outbox_capacity,
    };
    let real = Arc::clone(&heal_node.real);
    let push = Arc::clone(&heal_node.push);
    let pull = Arc::clone(&heal_node.pull_dyn);
    sim.host(host, move || {
        let params = params.clone();
        let real = Arc::clone(&real);
        let push = Arc::clone(&push);
        let pull = Arc::clone(&pull);
        async move { heal_node_loop(params, real, push, pull).await }
    });
}

/// [`run_partition_heal`]'s wire-metrics measurement window, guarded by
/// this mutex since `frames_sent_total`/`bytes_sent_total` are
/// process-wide, shared with every other test in this binary, so only one
/// partition-heal run's window is open at a time.
static HEAL_WIRE_METRICS_LOCK: StdMutex<()> = StdMutex::new(());

/// Runs one partition-heal scenario end to end and returns its metrics from
/// the heal onward: a fresh `turmoil::Sim`, three nodes with node `a` split
/// from `b`/`c` (who stay mutually connected), the write schedule
/// `cfg.conflict_fraction` and `cfg.variant` select, a heal, then anti-entropy
/// -- run through the production `sundog::run_round_against` entry point,
/// never a reimplementation of it -- until every node's fingerprint agrees
/// and every counter reads the exact expected total.
#[allow(
    clippy::too_many_lines,
    reason = "one scenario driver's full setup, write/heal schedule, and convergence check read \
              best kept together"
)]
pub(super) fn run_partition_heal(cfg: HealConfig) -> HealMetrics {
    const PORT: u16 = 4900;

    let tag = cfg.variant.tag();
    let run_id = heal_run_id(&cfg);
    let cache = SmolStr::new(run_id.clone());
    let node_a = heal_node_id(&run_id, "a");
    let node_b = heal_node_id(&run_id, "b");
    let node_c = heal_node_id(&run_id, "c");
    let host_a = format!("{run_id}-a");
    let host_b = format!("{run_id}-b");
    let host_c = format!("{run_id}-c");

    let counting_resolver = match cfg.variant {
        Variant::Merged => Some(Arc::new(CountingResolver::new(PnCounterResolver))),
        Variant::Decomposed => None,
    };
    let dyn_resolver: Option<Arc<dyn ConflictResolver>> = counting_resolver
        .as_ref()
        .map(|r| Arc::clone(r) as Arc<dyn ConflictResolver>);

    let build_shard = |node: NodeId| -> Shard<String, PnCounter> {
        let shard = Shard::new(cache.clone(), Mode::Replicated, node, 200_000, None, None);
        match &dyn_resolver {
            Some(r) => shard.with_resolver(Arc::clone(r)),
            None => shard,
        }
    };

    let a = HealNode::new(Arc::new(build_shard(node_a)));
    let b = HealNode::new(Arc::new(build_shard(node_b)));
    let c = HealNode::new(Arc::new(build_shard(node_c)));

    let ae_period = Duration::from_millis(cfg.ae_interval_ms);
    let outbox_capacity = (cfg.keys as usize)
        .saturating_mul(4)
        .max(ClusterConfig::default().outbox_capacity);
    let ae_rounds = Arc::new(AtomicU64::new(0));

    // Zero message latency, unlike every other scenario in this file: with
    // it at the usual few milliseconds, the exact number of rounds counted
    // before convergence is detected depends on how many random-latency
    // draws a run's own frame count consumes -- itself a function of key
    // count -- so the round count picks up scale-dependent scheduling noise
    // having nothing to do with the anti-entropy protocol's own cost, the
    // harness-artifact failure mode this redesign exists to remove. At zero
    // latency every delivery lands in the same tick every run, so a
    // config's round count depends only on the protocol and the data, not
    // on how many bytes happened to shift a message's place in the RNG
    // stream.
    let mut sim = Builder::new()
        .rng_seed(cfg.seed)
        .tick_duration(TICK)
        .max_message_latency(Duration::ZERO)
        .build();

    spawn_heal_host(
        &mut sim,
        &host_a,
        node_a,
        PORT,
        vec![
            (node_b, host_b.clone(), PORT),
            (node_c, host_c.clone(), PORT),
        ],
        vec![node_b, node_c],
        ae_period,
        Arc::clone(&ae_rounds),
        cache.clone(),
        outbox_capacity,
        &a,
    );
    spawn_heal_host(
        &mut sim,
        &host_b,
        node_b,
        PORT,
        vec![
            (node_a, host_a.clone(), PORT),
            (node_c, host_c.clone(), PORT),
        ],
        vec![node_a],
        ae_period,
        Arc::clone(&ae_rounds),
        cache.clone(),
        outbox_capacity,
        &b,
    );
    spawn_heal_host(
        &mut sim,
        &host_c,
        node_c,
        PORT,
        vec![
            (node_a, host_a.clone(), PORT),
            (node_b, host_b.clone(), PORT),
        ],
        vec![node_a],
        ae_period,
        Arc::clone(&ae_rounds),
        cache.clone(),
        outbox_capacity,
        &c,
    );

    // Two-way split: node-a alone on one side, node-b/node-c together on the
    // other. Applied before any step runs, so node-a's mesh never gets the
    // chance to dial node-b/node-c at all -- see the predecessor of this
    // driver's own long-standing note on why that ordering matters for
    // determinism (`ReqPool`'s freshness window is real wall-clock time,
    // never virtualized).
    sim.partition(host_a.as_str(), host_b.as_str());
    sim.partition(host_a.as_str(), host_c.as_str());

    // Let the mesh's connections settle (node-b/node-c's with each other;
    // node-a's dial attempts toward either fail fast and back off, already
    // partitioned).
    run_steps(&mut sim, steps_for(Duration::from_millis(100)));

    let overlap = overlap_count(cfg.keys, cfg.conflict_fraction);
    write_side(
        a.real.as_ref(),
        WriterId::new(node_a, 1),
        "a",
        0..overlap,
        cfg.variant,
        cfg.increments_per_side,
    );
    write_side(
        b.real.as_ref(),
        WriterId::new(node_b, 1),
        "b",
        0..cfg.keys,
        cfg.variant,
        cfg.increments_per_side,
    );

    // Give node-b/node-c, never partitioned from each other, time to settle
    // between themselves before the measurement window starts, so the
    // reported cost is anti-entropy repairing node-a's side of the split
    // rather than ordinary in-partition b/c traffic.
    run_steps(&mut sim, steps_for(Duration::from_millis(cfg.partition_ms)));

    let wire_guard = HEAL_WIRE_METRICS_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let ae_rounds_before = ae_rounds.load(Ordering::Relaxed);
    let records_before = a.records() + b.records() + c.records();
    let redundant_before = a.redundant() + b.redundant() + c.redundant();
    let redundant_pulls_before = a.redundant_pulls() + b.redundant_pulls() + c.redundant_pulls();
    let folds_before = counting_resolver.as_ref().map_or(0, |r| r.folds());

    sim.repair(host_a.as_str(), host_b.as_str());
    sim.repair(host_a.as_str(), host_c.as_str());

    let expected = expected_total(cfg.keys, cfg.conflict_fraction, cfg.increments_per_side);
    let budget = steps_for(ae_period * 40 + Duration::from_secs(2));
    let steps_to_converge = run_until(&mut sim, budget, || {
        let da = heal_digests_of(a.real.as_ref());
        let db = heal_digests_of(b.real.as_ref());
        let dc = heal_digests_of(c.real.as_ref());
        da == db
            && db == dc
            && counter_total(a.real.as_ref(), cfg.keys, cfg.variant) == expected
            && counter_total(b.real.as_ref(), cfg.keys, cfg.variant) == expected
            && counter_total(c.real.as_ref(), cfg.keys, cfg.variant) == expected
    })
    .unwrap_or_else(|| {
        panic!(
            "{tag}: partition-heal did not converge within the budget (keys={}, conflict_fraction={})",
            cfg.keys, cfg.conflict_fraction
        )
    });

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let ae_rounds_spent = ae_rounds.load(Ordering::Relaxed) - ae_rounds_before;
    let records = a.records() + b.records() + c.records() - records_before;
    let redundant = a.redundant() + b.redundant() + c.redundant() - redundant_before;
    let redundant_pulls =
        a.redundant_pulls() + b.redundant_pulls() + c.redundant_pulls() - redundant_pulls_before;
    let folds = counting_resolver.as_ref().map_or(0, |r| r.folds()) - folds_before;
    drop(wire_guard);

    let actual_total = counter_total(a.real.as_ref(), cfg.keys, cfg.variant);

    let metrics = HealMetrics {
        variant: tag,
        keys: cfg.keys,
        conflict_fraction: cfg.conflict_fraction,
        seed: cfg.seed,
        ae_rounds: ae_rounds_spent,
        virtual_ms: steps_to_ms(steps_to_converge),
        frames,
        bytes,
        records,
        applies: records.saturating_sub(redundant),
        folds,
        redundant_pulls,
        expected_total: expected,
        actual_total,
        lost_updates: expected - actual_total,
    };
    print_heal_metrics(&metrics);
    metrics
}

fn steps_to_ms(steps: usize) -> u64 {
    u64::try_from(steps)
        .expect("test-scale step counts fit in a u64")
        .saturating_mul(u64::try_from(TICK.as_millis()).expect("TICK's millis count fits in a u64"))
}

fn print_heal_metrics(m: &HealMetrics) {
    eprintln!(
        "SIM partition_heal variant={} keys={} conflict_fraction={} seed={:#x} ae_rounds={} \
         virtual_ms={} frames={} bytes={} records={} applies={} folds={} redundant_pulls={} \
         expected_total={} actual_total={} lost_updates={}",
        m.variant,
        m.keys,
        m.conflict_fraction,
        m.seed,
        m.ae_rounds,
        m.virtual_ms,
        m.frames,
        m.bytes,
        m.records,
        m.applies,
        m.folds,
        m.redundant_pulls,
        m.expected_total,
        m.actual_total,
        m.lost_updates
    );
}

/// A ratio only needs `f64`'s exact-integer range, up to 2^53, comfortably
/// covering this family's counters at every scale this grid drives; mirrors
/// `engine::count_f64` and `spill::bytes_used_f64`'s own precedent for the
/// same allow.
#[allow(clippy::cast_precision_loss)]
fn heal_ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        f64::NAN
    } else {
        numerator as f64 / denominator as f64
    }
}

pub(super) fn print_heal_comparison(decomposed: &HealMetrics, merged: &HealMetrics) {
    eprintln!(
        "SIM partition_heal_pair keys={} conflict_fraction={} seed={:#x} ae_rounds_ratio={:.3} \
         virtual_ms_ratio={:.3} bytes_ratio={:.3}",
        merged.keys,
        merged.conflict_fraction,
        merged.seed,
        heal_ratio(merged.ae_rounds, decomposed.ae_rounds),
        heal_ratio(merged.virtual_ms, decomposed.virtual_ms),
        heal_ratio(merged.bytes, decomposed.bytes),
    );
}
