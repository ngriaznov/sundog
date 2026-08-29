//! Deterministic simulation suite: drives `net::Mesh` and
//! `store::Shard`/`ShardOps` directly inside a `turmoil` simulation, against
//! a hand-scripted membership feed built from `Peer` values — no chitchat,
//! no real UDP/TCP. `cluster.rs`'s composition (membership-driven
//! `update_peers`, the inbound dispatch loop, the anti-entropy scheduler,
//! and state-transfer's donor-retry loop) is `pub(crate)` and unusable from
//! here, so this file re-implements the relevant slice of each against the
//! same public `Mesh`/`ShardOps` surface `cluster.rs` itself drives — see
//! this suite's own "needs" note in the owning agent's report.
//!
//! `turmoil`'s simulated TCP objects (reached through `net`'s transport seam,
//! `src/net/tcp.rs`) must be created and driven from *within* the owning
//! host's own future — a `Mesh` cannot be shared across hosts. A `Shard` has
//! no such constraint (pure state, no simulated I/O), so every scenario below
//! builds each node's `Shard` up front and hands an `Arc` clone to both that
//! node's host future (for wiring into a `Mesh`-backed `RequestHandler` and
//! for applying inbound/state-transfer records) and to the test function
//! itself (for assertions).

#![cfg(feature = "sim")]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use sundog::config::ClusterConfig;
use sundog::hlc::Hlc;
use sundog::membership::Peer;
use sundog::net::{InboundMsg, Mesh, MsgClass, RequestHandler};
use sundog::node::{NodeId, NodeName};
use sundog::store::{Mode, Shard, ShardOps};
use sundog::wire::{Msg, WireRecord};
use turmoil::{Builder, Sim};

type TestShard = Shard<u32, String>;
type SimResult = turmoil::Result;

/// Deterministic by default; the scheduled fresh-seed CI job overrides via
/// `SUNDOG_SIM_SEED` and the chosen value is echoed so a red run names the
/// seed to replay. `simulation_is_reproducible_for_a_fixed_seed` keeps its
/// own fixed seeds regardless.
fn sim_seed(default: u64) -> u64 {
    std::env::var("SUNDOG_SIM_SEED").map_or(default, |raw| {
        let seed: u64 = raw
            .parse()
            .expect("SUNDOG_SIM_SEED must be a u64 turmoil seed");
        eprintln!("sim seed override: replay with SUNDOG_SIM_SEED={seed}");
        seed
    })
}

const CACHE: &str = "sim-users";
/// Simulated time per `Sim::step()`: small relative to every interval below,
/// so scheduling stays close to the nominal cadence without needing an
/// impractical number of steps to cover a few seconds of simulated time.
const TICK: Duration = Duration::from_millis(5);
/// Bounds every request/response network call in this harness (anti-entropy
/// exchange, state-transfer stream reads). Turmoil's `fail_rate` drops a
/// message outright with no retransmission (unlike a real dropped TCP
/// segment, which the OS retries): if the dropped message was the one thing
/// a reader was waiting on, that read stalls forever rather than erroring.
/// Real `net::Mesh` never needs this — real TCP retransmits — but a harness
/// exercising turmoil's lossier model does; see this suite's own "needs"
/// note in the owning agent's report.
const NET_TIMEOUT: Duration = Duration::from_millis(500);

fn cache_name() -> SmolStr {
    SmolStr::new(CACHE)
}

fn key_bytes(key: u32) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("u32 always postcard-encodes"))
}

/// Runs a `ShardOps` future to completion without any ambient runtime —
/// valid because `Shard`'s async methods only ever await plain
/// `tokio::sync` primitives and `moka`, neither of which needs a reactor —
/// for reading shard state from outside any turmoil host (the test function
/// itself, which is not itself async).
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}

fn digests_of(shard: &TestShard) -> Vec<(u16, u64)> {
    block_on(ShardOps::digests(shard))
}

fn value_of(shard: &TestShard, key: u32) -> Option<String> {
    block_on(shard.get(&key))
}

fn new_shard(node: NodeId) -> TestShard {
    Shard::new(cache_name(), Mode::Replicated, node, 100_000, None, None)
}

/// Builds a shard pre-populated with `keys`, for state-transfer donors.
fn seed_shard(node: NodeId, keys: impl IntoIterator<Item = u32>) -> TestShard {
    let shard = new_shard(node);
    block_on(async {
        for key in keys {
            shard
                .insert(key, format!("seed:{key}"))
                .await
                .expect("small values never exceed the frame cap");
        }
    });
    shard
}

fn peer_list_of(peers: &[(NodeId, &'static str, u16)]) -> Vec<Peer> {
    peers
        .iter()
        .map(|&(node, host, port)| Peer {
            node,
            name: NodeName::new(host, node),
            gossip_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            data_addr: SocketAddr::new(turmoil::lookup(host), port),
            incarnation: 1,
        })
        .collect()
}

async fn dispatch_inbound(shard: &TestShard, msg: Msg) {
    match msg {
        Msg::Invalidate { key, ver, .. } => ShardOps::invalidate(shard, key, ver).await,
        Msg::Replicate { rec, .. } => ShardOps::apply_remote(shard, rec).await,
        Msg::ReplicateBatch { recs, .. } => ShardOps::apply_remote_batch(shard, recs).await,
        Msg::Hello { .. }
        | Msg::StRequest { .. }
        | Msg::StChunk { .. }
        | Msg::AeDigest { .. }
        | Msg::AeBucket { .. }
        | Msg::AePull { .. }
        | Msg::ReqDone => {}
    }
}

/// Wraps a shared `Shard` as the `net::RequestHandler` a `Mesh` answers
/// inbound state-transfer/anti-entropy requests through — the single-shard
/// stand-in for `cluster.rs`'s `ClusterRequestHandler` over a whole registry.
struct ShardHandler(Arc<TestShard>);

impl RequestHandler for ShardHandler {
    fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        ShardOps::snapshot_chunks(self.0.as_ref())
    }

    fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>> {
        let shard = Arc::clone(&self.0);
        Box::pin(async move { ShardOps::digests(shard.as_ref()).await })
    }

    fn bucket_entries(&self, _cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
        let shard = Arc::clone(&self.0);
        Box::pin(async move { ShardOps::bucket_entries(shard.as_ref(), bucket).await })
    }

    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, sundog::store::BucketEntries> {
        let shard = Arc::clone(&self.0);
        Box::pin(async move { ShardOps::entries_for_buckets(shard.as_ref(), buckets).await })
    }

    fn records_for(&self, _cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        let shard = Arc::clone(&self.0);
        Box::pin(async move { ShardOps::records_for(shard.as_ref(), keys).await })
    }
}

/// Fans `key`'s current record out to `peers`, `dup_factor` times each — the
/// harness stand-in for `cluster::fan_out_one`'s `Mode::Replicated` arm.
/// Sending the same record repeatedly is this suite's "duplicate storm":
/// idempotent apply must make it a no-op past the first delivery.
async fn fan_out(shard: &TestShard, mesh: &Mesh, peers: &[NodeId], key: u32, dup_factor: usize) {
    let Some(rec) = ShardOps::records_for(shard, vec![key_bytes(key)])
        .await
        .into_iter()
        .next()
    else {
        return;
    };
    for &peer in peers {
        for _ in 0..dup_factor.max(1) {
            mesh.send(
                peer,
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: cache_name(),
                    rec: rec.clone(),
                },
            );
        }
    }
}

/// One anti-entropy round against `peer`, re-implementing
/// `cluster::anti_entropy::run_round_against`'s digest-exchange-then-diff
/// logic (`pub(crate)`, unreachable here) over the same public `Mesh`/
/// `ShardOps` calls it drives. Returns whether the round completed (`false`
/// on any network failure, e.g. a simulated link failure or a crashed peer).
async fn ae_round_once(mesh: &Mesh, shard: &TestShard, peer: NodeId) -> bool {
    let local_buckets = ShardOps::digests(shard).await;
    let Ok(Ok(mismatched)) = tokio::time::timeout(
        NET_TIMEOUT,
        mesh.ae_round(peer, cache_name(), local_buckets),
    )
    .await
    else {
        return false;
    };

    let mut push_keys = Vec::new();
    let mut pull_keys = Vec::new();
    for (bucket, peer_entries) in mismatched {
        diff_bucket(shard, bucket, &peer_entries, &mut push_keys, &mut pull_keys).await;
    }

    if !push_keys.is_empty() {
        for rec in ShardOps::records_for(shard, push_keys).await {
            mesh.send(
                peer,
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: cache_name(),
                    rec,
                },
            );
        }
    }
    if !pull_keys.is_empty() {
        return match tokio::time::timeout(NET_TIMEOUT, mesh.ae_pull(peer, cache_name(), pull_keys))
            .await
        {
            Ok(Ok(records)) => {
                for rec in records {
                    ShardOps::apply_remote(shard, rec).await;
                }
                true
            }
            Ok(Err(_)) | Err(_) => false,
        };
    }
    true
}

async fn diff_bucket(
    shard: &TestShard,
    bucket: u16,
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
) {
    let peer_by_key: HashMap<Bytes, Hlc> = peer_entries.iter().cloned().collect();
    let mut local_keys = HashSet::with_capacity(peer_by_key.len());

    for (key, local_ver) in ShardOps::bucket_entries(shard, bucket).await {
        local_keys.insert(key.clone());
        match peer_by_key.get(&key) {
            Some(&peer_ver) if local_ver > peer_ver => push_keys.push(key),
            Some(&peer_ver) if local_ver < peer_ver => pull_keys.push(key),
            Some(_) => {}
            None => push_keys.push(key),
        }
    }
    for key in peer_by_key.into_keys() {
        if !local_keys.contains(&key) {
            pull_keys.push(key);
        }
    }
}

/// One symmetric peer's whole role in scenarios 1 and 2: write its own key
/// range on a timer (fanning each out), run anti-entropy against its peer on
/// a separate timer, and dispatch inbound traffic — forever.
#[derive(Clone)]
struct NodeParams {
    node: NodeId,
    label: &'static str,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
    keys: Vec<u32>,
    write_period: Duration,
    ae_period: Duration,
    dup_factor: usize,
    ae_failures: Option<Arc<AtomicUsize>>,
}

async fn node_loop(params: NodeParams, shard: Arc<TestShard>) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], params.port));
    let (mesh, mut inbound) = Mesh::spawn(
        bind_addr,
        params.node,
        1,
        &ClusterConfig::default(),
        handler,
    )
    .await?;

    let peer_list = peer_list_of(&params.peers);
    mesh.update_peers(peer_list.clone());
    let peer_ids: Vec<NodeId> = peer_list.iter().map(|peer| peer.node).collect();

    let mut keys = params.keys.into_iter();
    let mut write_tick = tokio::time::interval(params.write_period);
    write_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ae_tick = tokio::time::interval(params.ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(shard.as_ref(), msg).await;
            }
            _ = write_tick.tick() => {
                if let Some(key) = keys.next() {
                    let value = format!("{}:{key}", params.label);
                    let _ = shard.insert(key, value).await;
                    fan_out(shard.as_ref(), &mesh, &peer_ids, key, params.dup_factor).await;
                }
            }
            _ = ae_tick.tick() => {
                for &peer in &peer_ids {
                    if !ae_round_once(&mesh, shard.as_ref(), peer).await
                        && let Some(counter) = params.ae_failures.as_ref()
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

/// A pair of symmetric peers' fixed setup: hostnames, ids, disjoint key
/// ranges to write, and shared timing/behavior knobs.
struct PairSpec {
    host_a: &'static str,
    host_b: &'static str,
    node_a: NodeId,
    node_b: NodeId,
    port: u16,
    keys_a: Vec<u32>,
    keys_b: Vec<u32>,
    write_period: Duration,
    ae_period: Duration,
    dup_factor: usize,
}

fn spawn_symmetric_pair(
    sim: &mut Sim<'_>,
    spec: PairSpec,
    shard_a: Arc<TestShard>,
    shard_b: Arc<TestShard>,
    ae_failures: Option<Arc<AtomicUsize>>,
) {
    let params_a = NodeParams {
        node: spec.node_a,
        label: "a",
        port: spec.port,
        peers: vec![(spec.node_b, spec.host_b, spec.port)],
        keys: spec.keys_a,
        write_period: spec.write_period,
        ae_period: spec.ae_period,
        dup_factor: spec.dup_factor,
        ae_failures: ae_failures.clone(),
    };
    sim.host(spec.host_a, move || {
        let shard = Arc::clone(&shard_a);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: spec.node_b,
        label: "b",
        port: spec.port,
        peers: vec![(spec.node_a, spec.host_a, spec.port)],
        keys: spec.keys_b,
        write_period: spec.write_period,
        ae_period: spec.ae_period,
        dup_factor: spec.dup_factor,
        ae_failures,
    };
    sim.host(spec.host_b, move || {
        let shard = Arc::clone(&shard_b);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });
}

fn run_steps(sim: &mut Sim<'_>, count: usize) {
    for _ in 0..count {
        sim.step().expect("turmoil step succeeds");
    }
}

fn steps_for(duration: Duration) -> usize {
    let ticks = duration.as_millis() / TICK.as_millis();
    usize::try_from(ticks)
        .expect("test-scale durations fit in a usize step count")
        .max(1)
}

/// Steps `sim` until `converged` reports success or `max_steps` is spent,
/// returning the step count on success.
fn run_until(
    sim: &mut Sim<'_>,
    max_steps: usize,
    mut converged: impl FnMut() -> bool,
) -> Option<usize> {
    for step in 1..=max_steps {
        sim.step().expect("turmoil step succeeds");
        if converged() {
            return Some(step);
        }
    }
    None
}

#[test]
fn partition_during_writes_converges_within_five_ae_rounds() {
    let node_a = NodeId::from(1);
    let node_b = NodeId::from(2);
    let ae_period = Duration::from_millis(250);
    let keys_a: Vec<u32> = (0..5).collect();
    let keys_b: Vec<u32> = (100..105).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xA11C_E001))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "node-a",
            host_b: "node-b",
            node_a,
            node_b,
            port: 4000,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(50),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    // Partition first, so the whole write burst on both sides happens while
    // split, then heal and bound convergence to five AE-round intervals.
    sim.partition("node-a", "node-b");
    run_steps(&mut sim, steps_for(Duration::from_millis(750)));
    sim.repair("node-a", "node-b");

    let budget = steps_for(ae_period * 5 + Duration::from_millis(500));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "digests must converge within five AE-round intervals of healing"
    );

    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert!(
            value_of(&shard_a, key).is_some(),
            "node-a missing key {key}"
        );
        assert!(
            value_of(&shard_b, key).is_some(),
            "node-b missing key {key}"
        );
    }
    assert_eq!(value_of(&shard_a, 100).as_deref(), Some("b:100"));
    assert_eq!(value_of(&shard_b, 0).as_deref(), Some("a:0"));
}

struct StormStats {
    steps_to_converge: usize,
    ae_failures: usize,
    keys_present_a: usize,
    keys_present_b: usize,
}

/// Message loss + a wide latency spread (reorder) + a deliberate per-write
/// duplicate storm, all running concurrently with live writes on both
/// sides. No partition here: `fail_rate` alone breaks and re-establishes
/// individual links throughout the run.
fn run_storm_scenario(seed: u64) -> StormStats {
    let node_a = NodeId::from(11);
    let node_b = NodeId::from(12);
    let keys_a: Vec<u32> = (0..8).collect();
    let keys_b: Vec<u32> = (200..208).collect();
    let total_keys = keys_a.len() + keys_b.len();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));
    let ae_failures = Arc::new(AtomicUsize::new(0));

    let write_period = Duration::from_millis(30);
    let mut sim = Builder::new()
        .rng_seed(seed)
        .tick_duration(TICK)
        // Turmoil's `fail_rate` has no TCP-segment retransmission (its own
        // documented limitation): a single dropped chunk can break a whole
        // in-flight connection outright, and a large message — the ~10 KiB
        // full-digest array sent every anti-entropy round — crosses many
        // chunks, so its odds of a break compound per chunk. A rate that reads as
        // "occasional loss" for one small message is "nearly every AE round
        // breaks" for this one; kept low enough that rounds still routinely
        // succeed within a handful of `ae_period` retries.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "node-a",
            host_b: "node-b",
            node_a,
            node_b,
            port: 4100,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period,
            ae_period: Duration::from_millis(150),
            dup_factor: 4,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        Some(Arc::clone(&ae_failures)),
    );

    // Both sides' write bursts run concurrently with AE from the start
    // (that's the "storm"), so digest equality can hold trivially early —
    // e.g. neither side has written anything yet — well before every key
    // exists anywhere to converge on. A fixed time margin isn't a safe proxy
    // for "the write plan finished issuing" (the inbound-dispatch branch of
    // `node_loop`'s `select!` can win repeatedly under a heavy duplicate
    // storm, delaying — never dropping — a late write past any fixed
    // budget), so wait explicitly for every key to exist on its own
    // origin shard before treating digest equality as real convergence.
    let own_writes_issued =
        |shard: &TestShard, keys: &[u32]| keys.iter().all(|&key| value_of(shard, key).is_some());
    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        own_writes_issued(&shard_a, &keys_a) && own_writes_issued(&shard_b, &keys_b)
    })
    .expect("both sides finish issuing their own write plan within the budget");

    let budget = steps_for(Duration::from_secs(20));
    let steps_to_converge = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    })
    .expect("anti-entropy converges despite loss/reorder/duplication within the budget");

    let present = |shard: &TestShard| {
        keys_a
            .iter()
            .chain(keys_b.iter())
            .filter(|&&key| value_of(shard, key).is_some())
            .count()
    };
    let keys_present_a = present(&shard_a);
    let keys_present_b = present(&shard_b);
    assert_eq!(
        keys_present_a, total_keys,
        "node-a must hold every key once converged"
    );
    assert_eq!(
        keys_present_b, total_keys,
        "node-b must hold every key once converged"
    );

    StormStats {
        steps_to_converge,
        ae_failures: ae_failures.load(Ordering::Relaxed),
        keys_present_a,
        keys_present_b,
    }
}

#[test]
fn loss_reorder_duplicate_storm_still_converges() {
    run_storm_scenario(sim_seed(0x5707_2201));
}

#[test]
fn simulation_is_reproducible_for_a_fixed_seed() {
    // Digest *values* embed each write's `Hlc`, which is stamped from real
    // wall-clock time (`store::now_ms`) — not reproducible across two
    // separate process runs even under the same turmoil seed. What *is* a
    // pure function of the seed is turmoil's own network schedule (loss,
    // latency, reorder), so this asserts on outcomes that depend only on
    // that: how many simulated steps convergence took, how many AE rounds
    // hit a network failure, and which keys ended up present.
    let run1 = run_storm_scenario(0x5EED_0042);
    let run2 = run_storm_scenario(0x5EED_0042);

    assert_eq!(
        run1.steps_to_converge, run2.steps_to_converge,
        "the same seed must converge at the same simulated step"
    );
    assert_eq!(
        run1.ae_failures, run2.ae_failures,
        "the same seed must reproduce the same count of failed AE rounds"
    );
    assert_eq!(run1.keys_present_a, run2.keys_present_a);
    assert_eq!(run1.keys_present_b, run2.keys_present_b);
}

fn spawn_donor(
    sim: &mut Sim<'_>,
    host: &'static str,
    node: NodeId,
    port: u16,
    shard: Arc<TestShard>,
    peers: Vec<(NodeId, &'static str, u16)>,
) {
    sim.host(host, move || {
        let shard = Arc::clone(&shard);
        let peers = peers.clone();
        async move { donor_software(node, port, peers, shard).await }
    });
}

async fn donor_software(
    node: NodeId,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let (mesh, mut inbound) =
        Mesh::spawn(bind_addr, node, 1, &ClusterConfig::default(), handler).await?;
    mesh.update_peers(peer_list_of(&peers));
    loop {
        let Some(InboundMsg { msg, .. }) = inbound.recv().await else {
            return Ok(());
        };
        dispatch_inbound(shard.as_ref(), msg).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_receiver(
    sim: &mut Sim<'_>,
    host: &'static str,
    node: NodeId,
    port: u16,
    donors: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
    applied: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
) {
    sim.host(host, move || {
        let shard = Arc::clone(&shard);
        let donors = donors.clone();
        let applied = Arc::clone(&applied);
        let done = Arc::clone(&done);
        async move { receiver_software(node, port, donors, shard, applied, done).await }
    });
}

async fn receiver_software(
    node: NodeId,
    port: u16,
    donors: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
    applied: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let (mesh, mut inbound) =
        Mesh::spawn(bind_addr, node, 1, &ClusterConfig::default(), handler).await?;
    mesh.update_peers(peer_list_of(&donors));
    let donor_ids: Vec<NodeId> = donors.iter().map(|&(id, _, _)| id).collect();

    let ok = warm_up(&mesh, shard.as_ref(), &donor_ids, &applied).await;
    done.store(ok, Ordering::Relaxed);

    loop {
        let Some(InboundMsg { msg, .. }) = inbound.recv().await else {
            return Ok(());
        };
        dispatch_inbound(shard.as_ref(), msg).await;
    }
}

/// A deliberately simplified stand-in for `cluster::state_transfer::run`
/// (`pub(crate)`, unreachable from an external test binary): tries each
/// donor in order, applying records as they stream in, and falls through to
/// the next donor the moment the stream reports an error — the same
/// "a crashed donor surfaces as a stream error, not a clean end" contract
/// `net::conn::state_stream` guarantees and the production retry loop relies
/// on: the receiver notices the failure, re-picks a donor, and re-requests.
async fn warm_up(mesh: &Mesh, shard: &TestShard, donors: &[NodeId], applied: &AtomicUsize) -> bool {
    for &donor in donors {
        let Ok(Ok(mut stream)) =
            tokio::time::timeout(NET_TIMEOUT, mesh.request_state(donor, cache_name())).await
        else {
            continue;
        };
        let mut broke = false;
        loop {
            match tokio::time::timeout(NET_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    applied.fetch_add(chunk.len(), Ordering::Relaxed);
                    ShardOps::apply_remote_batch(shard, chunk).await;
                }
                Ok(Some(Err(_))) | Err(_) => {
                    broke = true;
                    break;
                }
                Ok(None) => break,
            }
        }
        if !broke {
            return true;
        }
    }
    false
}

#[test]
fn donor_crash_mid_state_transfer_repicks_and_completes() {
    let donor1 = NodeId::from(21);
    let donor2 = NodeId::from(22);
    let receiver = NodeId::from(23);
    let total_keys = 1_200u32;
    let port = 4200;

    let shard_d1 = Arc::new(seed_shard(donor1, 0..total_keys));
    let shard_d2 = Arc::new(seed_shard(donor2, 0..total_keys));
    let shard_r = Arc::new(new_shard(receiver));

    let applied = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xD0A0_5501))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(2))
        .max_message_latency(Duration::from_millis(30))
        .build();

    spawn_donor(
        &mut sim,
        "donor-1",
        donor1,
        port,
        Arc::clone(&shard_d1),
        vec![(receiver, "receiver", port)],
    );
    spawn_donor(
        &mut sim,
        "donor-2",
        donor2,
        port,
        Arc::clone(&shard_d2),
        vec![(receiver, "receiver", port)],
    );
    spawn_receiver(
        &mut sim,
        "receiver",
        receiver,
        port,
        vec![(donor1, "donor-1", port), (donor2, "donor-2", port)],
        Arc::clone(&shard_r),
        Arc::clone(&applied),
        Arc::clone(&done),
    );

    let mut crashed = false;
    let max_steps = steps_for(Duration::from_secs(20));
    for _ in 0..max_steps {
        sim.step().expect("turmoil step succeeds");
        if !crashed && applied.load(Ordering::Relaxed) >= 200 {
            sim.crash("donor-1");
            crashed = true;
        }
        if done.load(Ordering::Relaxed) {
            break;
        }
    }

    assert!(
        crashed,
        "test setup sanity: donor-1 must actually be crashed mid-transfer"
    );
    assert!(
        done.load(Ordering::Relaxed),
        "receiver re-picks the surviving donor and completes warm-up within the step budget"
    );
    for key in [0u32, 599, total_keys - 1] {
        assert_eq!(
            value_of(&shard_r, key),
            value_of(&shard_d2, key),
            "receiver's warmed copy must match the surviving donor for key {key}"
        );
    }
}
