//! Deterministic simulation suite: drives `net::Mesh` and
//! `store::Shard`/`ShardOps` directly inside a `turmoil` simulation, against
//! a hand-scripted membership feed built from `Peer` values, with no real
//! UDP or TCP. `cluster.rs`'s composition is `pub(crate)` and unusable from
//! here, so this file reimplements the relevant slice against the same
//! public `Mesh`/`ShardOps` surface `cluster.rs` itself drives.
//!
//! `turmoil`'s simulated TCP objects must be created and driven from within
//! the owning host's own future; a `Mesh` cannot be shared across hosts. A
//! `Shard` has no such constraint, so each scenario builds it up front and
//! shares an `Arc` clone with both the host future and the test itself.

#![cfg(feature = "sim")]

mod ae_round;
mod convergence;
mod deletes;
mod dist;
mod handler;
mod pair;
mod sketch;
mod state_transfer;
mod support;

use crate::ae_round::ae_round_with_sketch;
use crate::handler::{ShardHandler, dispatch_inbound};
use crate::support::{
    SimResult, TICK, block_on, cache_name, peer_list_of, run_steps, run_until, sim_seed, steps_for,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use sundog::config::ClusterConfig;
use sundog::crdt::{PnCounter, PnCounterResolver, WriterId};
use sundog::hlc::Hlc;
use sundog::membership::Peer;
use sundog::net::{InboundMsg, Mesh, RequestHandler};
use sundog::node::NodeId;
use sundog::store::{
    BucketDigest, BucketLen, BucketPart, BucketPartDigests, CompactionBounds, KeyVersion, Mode,
    Quiescence, Shard, ShardOps,
};
use sundog::wire::WireRecord;
use sundog::{ConflictResolver, Merged, RecordView, Winner};
use turmoil::{Builder, Sim};
use xxhash_rust::xxh3::xxh3_64;

// ---------------------------------------------------------------------
// Partition-heal family: three nodes, a two-way partition (node `a` split
// from `b`/`c`, which stay mutually connected) during which side a and
// side b each increment a share of `keys` counters, then a heal. Run in
// two variants over the same shape, one merged `PnCounter` key per counter
// under `PnCounterResolver`, and per-writer keys decomposed under the
// default `LwwResolver`, measuring anti-entropy rounds, virtual time,
// frames, bytes, records, engine applies, resolver folds, and redundant
// pulls spent reconciling after the heal, against the exact expected
// total. `SUNDOG_SIM_FULL` controls the grid's size: the fast default
// (2,000 keys, conflict fraction 0.0/1.0, one seed) or the full sweep
// (2,000 and 20,000 keys, the whole 0.0/0.1/0.5/1.0 conflict-fraction
// range, three seeds).
// ---------------------------------------------------------------------

/// Which side of the fair comparison a partition-heal run exercises: the
/// same content, keyed either per-writer under [`LwwResolver`] (so no key is
/// ever really contended) or on one shared key under [`PnCounterResolver`]
/// (so every overlapping counter is a real, folded merge). This is the
/// fair A/B comparison [`partition_heal_comparison`] measures across every
/// configured key count, conflict fraction, and seed.
///
/// [`LwwResolver`]: sundog::LwwResolver
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    Decomposed,
    Merged,
}

impl Variant {
    const fn tag(self) -> &'static str {
        match self {
            Self::Decomposed => "decomposed",
            Self::Merged => "merged",
        }
    }
}

const HEAL_DEFAULT_KEYS: u32 = 2_000;
const HEAL_DEFAULT_CONFLICT_FRACTIONS: [f64; 2] = [0.0, 1.0];
const HEAL_FULL_KEYS: [u32; 2] = [2_000, 20_000];
const HEAL_FULL_CONFLICT_FRACTIONS: [f64; 4] = [0.0, 0.1, 0.5, 1.0];
const HEAL_FULL_SEEDS: [u64; 3] = [0xC0DE_7001, 0xC0DE_7002, 0xC0DE_7003];
const HEAL_INCREMENTS_PER_SIDE: u32 = 3;
const HEAL_PARTITION_MS: u64 = 500;
/// Deliberately generous relative to a single round's own cost, even at
/// this family's largest key count: a repair large enough to need several
/// `REPAIR_BATCH` chunks spans several `sim.step()`s within one
/// `run_round_against` call, and node b's and node c's own independent
/// tick (never partitioned from node a, so *also* a candidate to relay
/// node a's content, and equally capable of fully repairing a pair on its
/// own as node a's own round against that same pair is) can fall on any
/// step in between. A short tick period lets that race resolve differently
/// depending on how many chunks a given key count happens to need -- the
/// scale-sensitive non-determinism the "Findings" section's own
/// non-monotonic figures came from. A period long enough that every node's
/// own round, at every scale this grid drives, always finishes well before
/// the next tick could possibly fire removes the race by removing the
/// overlap: the ordering the two paths land in step order stays the same
/// regardless of key count, since neither path is still running when
/// the next tick becomes possible.
///
/// This is a deliberate trade-off, not an oversight: production's own
/// default `ae_interval` is 200ms (`ClusterConfig::default`,
/// `sundog/src/cluster.rs`), 5x shorter than this constant, specifically
/// because a shorter tick can let node b/c's own independent round overlap
/// the multi-step repair this family drives -- the exact race this constant
/// is chosen to avoid. Every assertion in this family (determinism,
/// monotonicity, exact convergence) therefore measures the resolver and
/// anti-entropy logic in isolation from that scheduling race, not the
/// bidirectional exchange's round count at production's actual tick
/// cadence: a merging resolver's bidirectional exchange trades a round
/// for an extra push/pull pair per divergent key, a real slowdown at
/// production's actual 200ms cadence. This generous margin only shows
/// that nothing here gets slower still; it does not bound how much slower
/// the bidirectional exchange itself is at that cadence;
/// [`partition_heal_at_the_default_ae_interval`] runs the same grid at
/// 200ms to measure that directly.
const HEAL_AE_INTERVAL_MS: u64 = 1000;
/// Every anti-entropy round in this family runs against at most two peers
/// on a 1s tick; a generous multiple of the handful of rounds convergence
/// needs at every scale this grid drives, so a run spending more than
/// this many rounds signals a genuine slowdown rather than ordinary
/// scheduling noise.
const HEAL_MAX_AE_ROUNDS: u64 = 60;
/// `40_000` sits past the `20_000` scale where the Findings section's own
/// non-monotonic round counts appear, giving this pin headroom beyond
/// that exact scale, not merely up to it.
const HEAL_MONOTONIC_KEYS: [u32; 5] = [2_000, 8_000, 16_000, 20_000, 40_000];

/// `SUNDOG_SIM_FULL=1` switches [`partition_heal_comparison`] from the fast
/// default grid to the full one: more key counts, the whole conflict-fraction
/// range, and three seeds instead of one.
fn heal_sim_full() -> bool {
    std::env::var("SUNDOG_SIM_FULL").is_ok_and(|v| v == "1")
}

/// One partition-heal scenario's full parameterization; see the module doc
/// comment above [`run_partition_heal`] for what each field drives.
#[derive(Debug, Clone, Copy, PartialEq)]
struct HealConfig {
    seed: u64,
    variant: Variant,
    keys: u32,
    conflict_fraction: f64,
    increments_per_side: u32,
    partition_ms: u64,
    ae_interval_ms: u64,
}

impl HealConfig {
    fn new(seed: u64, variant: Variant, keys: u32, conflict_fraction: f64) -> Self {
        Self {
            seed,
            variant,
            keys,
            conflict_fraction,
            increments_per_side: HEAL_INCREMENTS_PER_SIDE,
            partition_ms: HEAL_PARTITION_MS,
            ae_interval_ms: HEAL_AE_INTERVAL_MS,
        }
    }
}

/// One [`run_partition_heal`] call's reported metrics, from the heal onward.
/// `records` is every record either direction applied (or attempted to
/// apply), `applies` is the subset that changed the touched key's stored
/// `(version, bytes)`, `folds` is how many times the resolver's `merge`
/// itself returned `Some`, and `redundant_pulls` is the pull-direction
/// subset of `records - applies` -- the "Findings" section's own
/// redundant-pull cost of the bidirectional exchange, isolated from the
/// push direction.
#[derive(Debug, Clone, PartialEq)]
struct HealMetrics {
    variant: &'static str,
    keys: u32,
    conflict_fraction: f64,
    seed: u64,
    ae_rounds: u64,
    virtual_ms: u64,
    frames: u64,
    bytes: u64,
    records: u64,
    applies: u64,
    folds: u64,
    redundant_pulls: u64,
    expected_total: i128,
    actual_total: i128,
    lost_updates: i128,
}

/// Wraps a [`ConflictResolver`] to count every `Some` its [`ConflictResolver::merge`]
/// returns: the "resolver folds" metric, a test-visible counter on the
/// resolver itself rather than anything the production `ConflictResolver`
/// contract exposes.
struct CountingResolver<R> {
    inner: R,
    folds: AtomicU64,
}

impl<R> CountingResolver<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            folds: AtomicU64::new(0),
        }
    }

    fn folds(&self) -> u64 {
        self.folds.load(Ordering::Relaxed)
    }
}

impl<R: ConflictResolver> ConflictResolver for CountingResolver<R> {
    fn winner(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        self.inner.winner(key, a, b)
    }

    fn needs_value_bytes(&self) -> bool {
        self.inner.needs_value_bytes()
    }

    fn merges(&self) -> bool {
        self.inner.merges()
    }

    fn merge(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let merged = self.inner.merge(key, a, b);
        if merged.is_some() {
            self.folds.fetch_add(1, Ordering::Relaxed);
        }
        merged
    }
}

/// Wraps a shard's `ShardOps` surface to count, on THIS wrapper instance
/// specifically, every record passed to `apply_remote`/`apply_remote_batch`
/// and how many of them left the touched key's stored `(version, bytes)` --
/// its exact `WireRecord` -- unchanged: a genuine no-op fold, by the same
/// byte-exact criterion `resolve_and_rebind` itself uses to skip
/// re-publishing and re-replicating a redelivered merge. A node keeps two
/// independently-counting instances wrapping the very same inner shard: one
/// given to `dispatch_inbound` for the push path, one given to
/// `run_round_against` for the pull path -- `ShardOps::apply_remote_batch`'s
/// own signature carries no tag saying which direction a call arrived
/// through, so which wrapper instance a call lands on is the only thing
/// that tells push apart from pull. [`HealNode`] pairs the two up around one
/// real shard.
struct CountingShard<S> {
    inner: Arc<S>,
    records: AtomicU64,
    redundant: AtomicU64,
}

impl<S> CountingShard<S> {
    fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            records: AtomicU64::new(0),
            redundant: AtomicU64::new(0),
        }
    }

    fn records(&self) -> u64 {
        self.records.load(Ordering::Relaxed)
    }

    fn redundant(&self) -> u64 {
        self.redundant.load(Ordering::Relaxed)
    }
}

impl<S: ShardOps + 'static> ShardOps for CountingShard<S> {
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()> {
        self.apply_remote_batch(vec![rec])
    }

    fn apply_remote_batch(&self, recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if recs.is_empty() {
                return;
            }
            self.records.fetch_add(recs.len() as u64, Ordering::Relaxed);
            let keys: Vec<Bytes> = recs.iter().map(|rec| rec.key.clone()).collect();
            let before: HashMap<Bytes, WireRecord> = self
                .inner
                .records_for(keys.clone())
                .await
                .into_iter()
                .map(|rec| (rec.key.clone(), rec))
                .collect();
            self.inner.apply_remote_batch(recs).await;
            let after = self.inner.records_for(keys).await;
            let redundant = after
                .into_iter()
                .filter(|rec| before.get(&rec.key).is_some_and(|prior| prior == rec))
                .count();
            if redundant > 0 {
                self.redundant
                    .fetch_add(redundant as u64, Ordering::Relaxed);
            }
        })
    }

    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()> {
        self.inner.invalidate(key, ver)
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        self.inner.digests()
    }

    fn ae_digests_for(&self, peer: NodeId) -> BoxFuture<'_, Vec<BucketDigest>> {
        self.inner.ae_digests_for(peer)
    }

    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        self.inner.bucket_entries(bucket)
    }

    fn entries_for_buckets(
        &self,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, sundog::store::BucketEntries> {
        self.inner.entries_for_buckets(buckets)
    }

    fn bucket_lens(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        self.inner.bucket_lens(buckets)
    }

    fn part_digests(&self, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        self.inner.part_digests(buckets)
    }

    fn entries_for_parts(
        &self,
        parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, sundog::store::PartEntries> {
        self.inner.entries_for_parts(parts)
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        self.inner.records_for(keys)
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        self.inner.snapshot_chunks()
    }

    fn gc_tombstones(&self, any_member_absent: bool) -> BoxFuture<'_, ()> {
        self.inner.gc_tombstones(any_member_absent)
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        self.inner.run_pending_tasks()
    }

    fn merges(&self) -> bool {
        self.inner.merges()
    }
}

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
fn expected_total(keys: u32, conflict_fraction: f64, increments_per_side: u32) -> i128 {
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
fn run_partition_heal(cfg: HealConfig) -> HealMetrics {
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

fn print_heal_comparison(decomposed: &HealMetrics, merged: &HealMetrics) {
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

/// Correctness only, at the default grid: every combination of variant and
/// conflict fraction converges to the exact expected total. The convergence
/// loop inside `run_partition_heal` already gates on this (and on all three
/// nodes' fingerprints agreeing) before it ever returns, so a `HealMetrics`
/// this test receives at all already reflects a converged, correct run;
/// `lost_updates == 0` restates that guarantee explicitly rather than
/// relying on it being implicit.
#[test]
fn partition_heal_variant_reaches_exact_totals() {
    let seed = sim_seed(0xC0DE_7100);
    for variant in [Variant::Decomposed, Variant::Merged] {
        for &conflict_fraction in &HEAL_DEFAULT_CONFLICT_FRACTIONS {
            let metrics = run_partition_heal(HealConfig::new(
                seed,
                variant,
                HEAL_DEFAULT_KEYS,
                conflict_fraction,
            ));
            assert_eq!(
                metrics.lost_updates,
                0,
                "{}: keys={} conflict_fraction={conflict_fraction} did not converge to the exact total",
                variant.tag(),
                metrics.keys
            );
        }
    }
}

/// The fair decomposed-vs-merged comparison: for every config in the grid,
/// runs decomposed then merged with the same seed, prints one `SIM` line
/// per run and one `SIM partition_heal_pair` line per pair, and asserts only
/// that both sides converged correctly and within
/// [`HEAL_MAX_AE_ROUNDS`] -- never a specific ratio between them, which the
/// printed `SIM`/`SIM partition_heal_pair` lines are there to read, not a
/// property to pin.
/// `SUNDOG_SIM_FULL=1` swaps the fast default grid (2,000 keys, conflict
/// fraction 0.0/1.0, one seed) for the full one (2,000 and 20,000 keys, the
/// whole 0.0/0.1/0.5/1.0 conflict-fraction range, three seeds).
#[test]
fn partition_heal_comparison() {
    let full = heal_sim_full();
    let keys_grid: &[u32] = if full {
        &HEAL_FULL_KEYS
    } else {
        &[HEAL_DEFAULT_KEYS]
    };
    let fractions: &[f64] = if full {
        &HEAL_FULL_CONFLICT_FRACTIONS
    } else {
        &HEAL_DEFAULT_CONFLICT_FRACTIONS
    };
    let default_seed = [sim_seed(0xC0DE_7200)];
    let seeds: &[u64] = if full {
        &HEAL_FULL_SEEDS
    } else {
        &default_seed
    };

    for &keys in keys_grid {
        for &conflict_fraction in fractions {
            for &seed in seeds {
                let decomposed = run_partition_heal(HealConfig::new(
                    seed,
                    Variant::Decomposed,
                    keys,
                    conflict_fraction,
                ));
                let merged = run_partition_heal(HealConfig::new(
                    seed,
                    Variant::Merged,
                    keys,
                    conflict_fraction,
                ));

                for metrics in [&decomposed, &merged] {
                    assert_eq!(
                        metrics.lost_updates, 0,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         did not converge to the exact total",
                        metrics.variant
                    );
                    assert!(
                        metrics.ae_rounds <= HEAL_MAX_AE_ROUNDS,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         spent {} anti-entropy rounds, over the {HEAL_MAX_AE_ROUNDS} sanity bound",
                        metrics.variant,
                        metrics.ae_rounds
                    );
                }
                print_heal_comparison(&decomposed, &merged);
            }
        }
    }
}

/// `ClusterConfig::default`'s `ae_interval`: the tick
/// [`partition_heal_at_the_default_ae_interval`] runs at.
const HEAL_DEFAULT_AE_INTERVAL_MS: u64 = 200;

/// [`partition_heal_comparison`] at production's default 200 ms
/// anti-entropy tick instead of [`HEAL_AE_INTERVAL_MS`]'s race-free one
/// second. At this cadence a node's next round can start while a peer's
/// multi-batch repair is still landing, so the rounds interleave differently
/// from run to run; the test asserts only what holds under any
/// interleaving: both variants converge to the exact total within
/// [`HEAL_MAX_AE_ROUNDS`]. It prints the same `SIM` lines as
/// [`partition_heal_comparison`] for reading the round and byte ratios at
/// this cadence, and `SUNDOG_SIM_FULL=1` widens the grid the same way.
#[test]
fn partition_heal_at_the_default_ae_interval() {
    let full = heal_sim_full();
    let keys_grid: &[u32] = if full {
        &HEAL_FULL_KEYS
    } else {
        &[HEAL_DEFAULT_KEYS]
    };
    let fractions: &[f64] = if full {
        &HEAL_FULL_CONFLICT_FRACTIONS
    } else {
        &HEAL_DEFAULT_CONFLICT_FRACTIONS
    };
    let default_seed = [sim_seed(0xC0DE_7400)];
    let seeds: &[u64] = if full {
        &HEAL_FULL_SEEDS
    } else {
        &default_seed
    };
    let at_default_tick = |seed, variant, keys, conflict_fraction| HealConfig {
        ae_interval_ms: HEAL_DEFAULT_AE_INTERVAL_MS,
        ..HealConfig::new(seed, variant, keys, conflict_fraction)
    };

    for &keys in keys_grid {
        for &conflict_fraction in fractions {
            for &seed in seeds {
                let decomposed = run_partition_heal(at_default_tick(
                    seed,
                    Variant::Decomposed,
                    keys,
                    conflict_fraction,
                ));
                let merged = run_partition_heal(at_default_tick(
                    seed,
                    Variant::Merged,
                    keys,
                    conflict_fraction,
                ));
                for metrics in [&decomposed, &merged] {
                    assert_eq!(
                        metrics.lost_updates, 0,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         did not converge to the exact total at a {HEAL_DEFAULT_AE_INTERVAL_MS} ms tick",
                        metrics.variant
                    );
                    assert!(
                        metrics.ae_rounds <= HEAL_MAX_AE_ROUNDS,
                        "{}: keys={keys} conflict_fraction={conflict_fraction} seed={seed:#x} \
                         spent {} anti-entropy rounds at a {HEAL_DEFAULT_AE_INTERVAL_MS} ms tick, \
                         over the {HEAL_MAX_AE_ROUNDS} sanity bound",
                        metrics.variant,
                        metrics.ae_rounds
                    );
                }
                print_heal_comparison(&decomposed, &merged);
            }
        }
    }
}

/// The same config, run twice in one process, must yield identical metrics
/// on every field `run_partition_heal`'s own simulation determines:
/// `run_partition_heal`'s per-run namespacing (see [`heal_run_id`]) is what
/// makes two calls in one process share nothing, so nothing about the
/// second run's `Sim`, shards, or counters can be influenced by the
/// first's. `frames`/`bytes` are the one deliberate exception: they read
/// `sundog::net`'s process-wide wire counters (see
/// [`HEAL_WIRE_METRICS_LOCK`]'s own doc), so under `cargo test`'s default
/// parallelism a wholly unrelated sim test running concurrently on another
/// thread can add to those same two counters mid-window -- a real source of
/// noise this test's own two `run_partition_heal` calls cannot serialize
/// against, since it comes from other tests entirely. Every field this
/// harness itself controls is still asserted equal.
#[test]
fn partition_heal_is_deterministic_for_a_fixed_config() {
    let cfg = HealConfig::new(
        sim_seed(0xC0DE_7300),
        Variant::Merged,
        HEAL_DEFAULT_KEYS,
        1.0,
    );
    let first = run_partition_heal(cfg);
    let second = run_partition_heal(cfg);
    let msg = "the same partition-heal config run twice in one process must yield identical \
               metrics on every field this harness itself controls (frames/bytes excepted -- \
               see this test's own doc)";
    assert_eq!(
        (
            first.variant,
            first.keys,
            first.conflict_fraction,
            first.seed
        ),
        (
            second.variant,
            second.keys,
            second.conflict_fraction,
            second.seed
        ),
        "{msg}"
    );
    assert_eq!(
        (
            first.ae_rounds,
            first.virtual_ms,
            first.records,
            first.applies
        ),
        (
            second.ae_rounds,
            second.virtual_ms,
            second.records,
            second.applies
        ),
        "{msg}"
    );
    assert_eq!(
        (
            first.folds,
            first.redundant_pulls,
            first.expected_total,
            first.actual_total,
            first.lost_updates,
        ),
        (
            second.folds,
            second.redundant_pulls,
            second.expected_total,
            second.actual_total,
            second.lost_updates,
        ),
        "{msg}"
    );
}

/// Pins the "Findings" section's own non-monotonic round-count signature:
/// the harness artifact that reimplemented anti-entropy produced
/// non-monotonic round counts as key count grew (3 rounds at 8,000 keys,
/// 19 at 16,000, 5 at 20,000). Driving rounds through the production
/// `run_round_against` entry point instead makes rounds monotone
/// non-decreasing in key count, at fixed full conflict, for both variants.
#[test]
fn partition_heal_rounds_are_monotone_in_key_count() {
    let seed = sim_seed(0xC0DE_7400);
    for variant in [Variant::Decomposed, Variant::Merged] {
        let mut previous = 0u64;
        for &keys in &HEAL_MONOTONIC_KEYS {
            let metrics = run_partition_heal(HealConfig::new(seed, variant, keys, 1.0));
            assert_eq!(
                metrics.lost_updates,
                0,
                "{}: keys={keys} did not converge to the exact total",
                variant.tag()
            );
            assert!(
                metrics.ae_rounds >= previous,
                "{}: ae_rounds regressed from {previous} at a smaller key count to {} at \
                 keys={keys} -- rounds must be monotone non-decreasing in key count",
                variant.tag(),
                metrics.ae_rounds
            );
            previous = metrics.ae_rounds;
        }
    }
}

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
