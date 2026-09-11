//! CRDT merge resolver benchmark: real, in-process, loopback `Cluster`s,
//! public API only. Not a correctness suite; it measures and prints.
//!
//! Gated on `SUNDOG_BENCH=1`, an `eprintln!` and early return otherwise, so
//! a plain `cargo test` run still compiles without the wall-clock cost.
//! Every scenario shares the process-wide wire counters in `sundog::net`,
//! so run this binary single-threaded:
//!
//! ```text
//! SUNDOG_BENCH=1 cargo test --release -p sundog --test crdt_bench \
//!     -- --test-threads=1 --nocapture
//! ```
//!
//! With `--features prometheus`, every `BENCH` line also carries a
//! `sundog_ae_repaired_total` scrape for its cache(s):
//!
//! ```text
//! SUNDOG_BENCH=1 cargo test --release -p sundog --features prometheus \
//!     --test crdt_bench -- --test-threads=1 --nocapture
//! ```
//!
//! `WRITERS` (`SUNDOG_BENCH_WRITERS`, default 8), `ITERS`
//! (`SUNDOG_BENCH_ITERS`, default 200) and the repetition count
//! (`SUNDOG_BENCH_REPS`, default 3) are all env-overridable; the defaults
//! finish every scenario in well under a minute. Scenarios 7 and 8 scale a
//! third axis instead, the entity count `N` (`SUNDOG_BENCH_KEYS`), and fix
//! the writer count at 3 (one per warm-cluster node) and each writer's
//! contribution to each entity at a single increment, so `N` alone governs
//! their cost; their defaults are lower than the plan's own (documented at
//! each default's definition) to keep both variants of both scenarios
//! inside a 3-minute budget on a 4-core box. A smoke run:
//!
//! ```text
//! SUNDOG_BENCH=1 SUNDOG_BENCH_WRITERS=2 SUNDOG_BENCH_ITERS=50 \
//!     cargo test --release -p sundog --features prometheus \
//!     --test crdt_bench -- --test-threads=1 --nocapture
//! ```
//!
//! Each `BENCH` line is one `key=value`-per-metric record, `grep`able.
//! Scenarios 1-4 and 6 build a fresh 3-node `Replicated` cluster per
//! repetition (identical `fast_config()` topology throughout: 150ms AE
//! interval, 2s tombstone TTL); every writer targets a single cache handle
//! (`cache_a`), matching `replication_bench.rs`'s own concurrent-writer
//! shape. Scenarios 5/5a/5b build a single-node `Mode::Local` cluster with
//! no peers, isolating per-apply CPU cost from all network/AE noise.
//! Scenario 7 (`cold_join_initial_replication`) builds the same warm 3-node
//! `fast_config()` trio, lets it converge, then joins a fourth node against
//! it and times the join; scenario 8 (`large_entity_convergence`) builds
//! the same trio and writes `N` entities concurrently from all three nodes
//! with anti-entropy live throughout, timing convergence from the last
//! write. Both print one `BENCH` line per variant — `decomposed` (`3N`
//! per-writer keys under `LwwResolver`) and `merged` (`N` keys under
//! `PnCounterResolver`) — sharing topology, `fast_config()`, the 3-writer
//! count, and total logical increments between the two. No key ever
//! expires or is removed in any scenario — TTL is irrelevant here, stated
//! to rule out a confound rather than leave it implicit. Every numeric
//! field is the median of at least [`repetitions`] independent runs, so a
//! single noisy run never skews a reported number.

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, ClusterConfig, ConflictResolver, Mode, NodeId, RecordView, Winner};

#[cfg(feature = "prometheus")]
use std::sync::OnceLock;
#[cfg(feature = "prometheus")]
use sundog::PrometheusHandle;

fn bench_enabled() -> bool {
    std::env::var("SUNDOG_BENCH").as_deref() == Ok("1")
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Concurrent writers per scenario. `SUNDOG_BENCH_WRITERS`, default 8.
fn writers() -> u32 {
    env_u32("SUNDOG_BENCH_WRITERS", 8)
}

/// Writes per writer per scenario. `SUNDOG_BENCH_ITERS`, default 200.
fn iters() -> u32 {
    env_u32("SUNDOG_BENCH_ITERS", 200)
}

/// Independent repetitions per scenario before taking the median, so a
/// single noisy run doesn't skew a reported number. `SUNDOG_BENCH_REPS`,
/// default 3.
fn repetitions() -> u32 {
    env_u32("SUNDOG_BENCH_REPS", 3)
}

/// The entity count `N` scenarios 7 and 8 scale, `SUNDOG_BENCH_KEYS`
/// overriding `default`. Each scenario passes its own default; both are
/// lower than the plan's own defaults (`20_000` and `100_000` respectively),
/// documented where each is defined.
fn scale_keys(default: u32) -> u32 {
    env_u32("SUNDOG_BENCH_KEYS", default)
}

/// How much larger the no-network micro-benchmark's op count is than
/// [`iters`]: those scenarios pay no network/AE cost per op, so a much
/// larger count still finishes in a fraction of a second.
const MICRO_MULTIPLIER: u32 = 100;

fn micro_ops() -> u32 {
    iters().saturating_mul(MICRO_MULTIPLIER)
}

// ---------------------------------------------------------------------
// Cluster helpers, copied locally from `replication_bench.rs` (integration
// test binaries share nothing beyond `mod common`).
// ---------------------------------------------------------------------

/// Mirrors `replication_bench.rs`'s own copy: the only way, from outside the
/// crate, to learn a gossip address before the node that binds it exists.
async fn reserve_gossip_addr() -> SocketAddr {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback udp port to reserve a gossip address");
    socket
        .local_addr()
        .expect("a freshly bound udp socket reports a local address")
}

fn node_config(gossip_bind_addr: SocketAddr) -> ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip_bind_addr;
    })
}

/// Builds `n` real, loopback-`Static`-discovery clusters that all seed each
/// other, and waits until every one reports `n - 1` live peers.
async fn peer_group(cluster_name: &str, n: usize) -> Vec<Cluster> {
    let mut gossip_addrs = Vec::with_capacity(n);
    for _ in 0..n {
        gossip_addrs.push(reserve_gossip_addr().await);
    }

    let mut clusters = Vec::with_capacity(n);
    for (i, &addr) in gossip_addrs.iter().enumerate() {
        let seeds = gossip_addrs
            .iter()
            .enumerate()
            .filter(|&(j, _)| j != i)
            .map(|(_, &seed)| seed);
        let cluster = Cluster::builder(cluster_name)
            .seeds(seeds)
            .config(node_config(addr))
            .build()
            .await
            .unwrap_or_else(|error| panic!("node {i} builds: {error}"));
        clusters.push(cluster);
    }

    for cluster in &clusters {
        common::wait_for_peer_count(cluster, n - 1, Duration::from_secs(30)).await;
    }
    clusters
}

/// A single-node, loopback, `Static`-discovery cluster: every micro-
/// benchmark runs directly against this one node's shard, with no peer, no
/// fan-out, and no anti-entropy loop, isolating per-apply CPU cost from all
/// network noise.
async fn local_cluster(name: &str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("single-node loopback cluster builds")
}

/// Builds three loopback `Mode::Replicated` caches of the same name across
/// `cluster_a`/`cluster_b`/`cluster_c`, sharing one `resolver`.
async fn open_replicated_trio<V>(
    cluster_a: &Cluster,
    cluster_b: &Cluster,
    cluster_c: &Cluster,
    cache_name: &str,
    resolver: Arc<dyn ConflictResolver>,
) -> (
    sundog::Cache<u32, V>,
    sundog::Cache<u32, V>,
    sundog::Cache<u32, V>,
)
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let (a, b, c) = tokio::join!(
        cluster_a
            .cache::<u32, V>(cache_name)
            .mode(Mode::Replicated)
            .resolver(resolver.clone())
            .open(),
        cluster_b
            .cache::<u32, V>(cache_name)
            .mode(Mode::Replicated)
            .resolver(resolver.clone())
            .open(),
        cluster_c
            .cache::<u32, V>(cache_name)
            .mode(Mode::Replicated)
            .resolver(resolver)
            .open(),
    );
    (
        a.expect("cache a opens"),
        b.expect("cache b opens"),
        c.expect("cache c opens"),
    )
}

// ---------------------------------------------------------------------
// Latency percentiles and cross-repetition medians.
// ---------------------------------------------------------------------

/// The `p`th percentile (0-100) of an ascending-sorted `durations`,
/// nearest-rank. `Duration::ZERO` for an empty slice.
fn percentile_of(sorted: &[Duration], p: f64) -> Duration {
    let Some(last_idx) = sorted.len().checked_sub(1) else {
        return Duration::ZERO;
    };
    #[allow(clippy::cast_precision_loss)]
    let rank = (p / 100.0) * last_idx as f64;
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "rank is always within [0, last_idx], both nonnegative"
    )]
    let idx = (rank.round() as usize).min(last_idx);
    sorted[idx]
}

/// The median of `values`: the middle element for an odd count, the
/// midpoint of the two middle elements for an even one. `0.0` for an empty
/// slice.
fn median_f64(mut values: Vec<f64>) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).expect("bench metrics are never NaN"));
    let n = values.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        values[n / 2]
    } else {
        f64::midpoint(values[n / 2 - 1], values[n / 2])
    }
}

/// [`median_f64`]'s counterpart for exact integer counts (lost updates,
/// frame/byte deltas, resident key counts).
fn median_u64(values: &[u64]) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    if n == 0 {
        return 0;
    }
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        u64::midpoint(sorted[n / 2 - 1], sorted[n / 2])
    }
}

/// The median, across `items`, of the `f64` field `f` projects out of each.
/// Every `BENCH` line reports each of its numeric fields this way, over at
/// least [`repetitions`] independent runs.
fn median_field_f64<T>(items: &[T], f: impl Fn(&T) -> f64) -> f64 {
    median_f64(items.iter().map(f).collect())
}

/// [`median_field_f64`]'s counterpart for exact integer counts.
fn median_field_u64<T>(items: &[T], f: impl Fn(&T) -> u64) -> u64 {
    let values: Vec<u64> = items.iter().map(f).collect();
    median_u64(&values)
}

// ---------------------------------------------------------------------
// `sundog_ae_repaired_total` scrape, cfg-gated on `prometheus`: this
// binary's one claim on the process-global Prometheus recorder slot,
// installed lazily on first use, before the first benchmark in a run opens
// its first cache — a cache binds its per-cache metric handles when it
// opens, so the recorder has to exist first or those handles stay on the
// no-op recorder and every scrape reads back zero.
// ---------------------------------------------------------------------

#[cfg(feature = "prometheus")]
static METRICS_HANDLE: OnceLock<Option<PrometheusHandle>> = OnceLock::new();

#[cfg(feature = "prometheus")]
fn metrics_handle() -> Option<&'static PrometheusHandle> {
    METRICS_HANDLE
        .get_or_init(|| sundog::prometheus_handle().ok())
        .as_ref()
}

/// Finds `metric{label1="value1",...} <number>` in Prometheus
/// text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Mirrors `spill_bench.rs`'s own
/// `scraped_metric`, kept local since integration test binaries don't
/// share code beyond `mod common`.
#[cfg(feature = "prometheus")]
fn scraped_metric(body: &str, metric: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let wanted: Vec<String> = labels
        .iter()
        .map(|&(k, v)| format!("{k}=\"{v}\""))
        .collect();
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (line_labels, value) = rest.split_once('}')?;
        let line_labels: Vec<&str> = line_labels.split(',').collect();
        if !wanted.iter().all(|w| line_labels.contains(&w.as_str())) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// The current value of `sundog_ae_repaired_total{cache=cache_name}`, or 0
/// if the recorder never installed or the counter never incremented.
#[cfg(feature = "prometheus")]
fn ae_repaired_total(cache_name: &str) -> u64 {
    let value = metrics_handle().and_then(|h| {
        scraped_metric(
            &h.render(),
            "sundog_ae_repaired_total",
            &[("cache", cache_name)],
        )
    });
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "sundog_ae_repaired_total is a nonnegative counter"
    )]
    let count = value.unwrap_or(0.0).round() as u64;
    count
}

/// A per-repetition snapshot of `sundog_ae_repaired_total{cache=cache_name}`,
/// for a before/after delta around one repetition's writes and convergence
/// wait — the same pattern every scenario already uses for
/// `frames_sent_total`/`bytes_sent_total`. Every repetition of a scenario
/// shares one process-global counter for a given `cache_name`, so without
/// this delta a scrape taken after all repetitions have run would report
/// their sum, not one repetition's own repair count. `0` under a build
/// without `prometheus`, where there is no counter to scrape.
#[cfg(feature = "prometheus")]
fn ae_repaired_snapshot(cache_name: &str) -> u64 {
    ae_repaired_total(cache_name)
}

#[cfg(not(feature = "prometheus"))]
fn ae_repaired_snapshot(_cache_name: &str) -> u64 {
    0
}

/// Renders `median` — the median, across a scenario's repetitions, of each
/// one's own before/after [`ae_repaired_snapshot`] delta — as a `BENCH`-line
/// field. Empty under a build without `prometheus`, matching every other
/// `prometheus`-gated field on the line.
#[cfg(feature = "prometheus")]
fn ae_repaired_field(median: u64) -> String {
    format!(" ae_repaired_total={median}")
}

#[cfg(not(feature = "prometheus"))]
fn ae_repaired_field(_median: u64) -> String {
    String::new()
}

/// [`ae_repaired_field`]'s counterpart for a `BENCH` line reporting more
/// than one cache's repair count under distinct labels.
#[cfg(feature = "prometheus")]
fn ae_repaired_field_named(label: &str, median: u64) -> String {
    format!(" ae_repaired_total_{label}={median}")
}

#[cfg(not(feature = "prometheus"))]
fn ae_repaired_field_named(_label: &str, _median: u64) -> String {
    String::new()
}

/// The current value of `sundog_state_transfer_records_total{cache=cache_name}`
/// — the count of entries a state transfer has applied to `cache_name` on
/// this process, incremented once per donor a `Mode::Replicated` cache
/// opens against — or 0 if the recorder never installed or no transfer has
/// completed yet.
#[cfg(feature = "prometheus")]
fn entries_received_total(cache_name: &str) -> u64 {
    let value = metrics_handle().and_then(|h| {
        scraped_metric(
            &h.render(),
            "sundog_state_transfer_records_total",
            &[("cache", cache_name)],
        )
    });
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "sundog_state_transfer_records_total is a nonnegative counter"
    )]
    let count = value.unwrap_or(0.0).round() as u64;
    count
}

/// [`ae_repaired_snapshot`]'s counterpart for [`entries_received_total`], for
/// a before/after delta around scenario 7's join.
#[cfg(feature = "prometheus")]
fn entries_received_snapshot(cache_name: &str) -> u64 {
    entries_received_total(cache_name)
}

#[cfg(not(feature = "prometheus"))]
fn entries_received_snapshot(_cache_name: &str) -> u64 {
    0
}

/// [`ae_repaired_field`]'s counterpart for a `BENCH` line reporting
/// scenario 7's join-time [`entries_received_snapshot`] delta.
#[cfg(feature = "prometheus")]
fn entries_received_field(median: u64) -> String {
    format!(" entries_received={median}")
}

#[cfg(not(feature = "prometheus"))]
fn entries_received_field(_median: u64) -> String {
    String::new()
}

// ---------------------------------------------------------------------
// A resolver used only by scenario 5b: `LwwResolver`'s exact comparison,
// with `needs_value_bytes()` forced to `true`. This does not isolate real
// byte-decode cost from merge-logic cost: `apply_locked`'s stored-side
// lookup clones a resident entry's already-encoded `Bytes` unconditionally,
// before any resolver runs, regardless of `needs_value_bytes()` — a cheap
// refcount bump, not a decode. `needs_value_bytes()` only gates whether
// those already-cloned bytes are exposed to the resolver's `RecordView`
// (an `Option::filter`), so forcing it to `true` here adds at most that
// filter's own cost, not real materialization work. Scenario 5 vs 5b is
// expected to show a near-zero delta given the current engine — that
// result would confirm this exact reasoning, not indicate a broken
// isolation.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
struct LwwForcedBytesResolver;

impl ConflictResolver for LwwForcedBytesResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn needs_value_bytes(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------
// Metrics shapes shared by every repetition of a scenario; a `BENCH` line
// reports the median of each field over [`repetitions`] of these.
// ---------------------------------------------------------------------

struct WriteRepMetrics {
    writes_per_sec: f64,
    p50_micros: f64,
    p99_micros: f64,
    lost_updates: u64,
    converge_secs: f64,
    frames: u64,
    bytes: u64,
    resident_keys: u64,
    ae_repaired: u64,
}

struct MicroRepMetrics {
    ns_per_apply: f64,
    p50_nanos: f64,
    p99_nanos: f64,
    ae_repaired: u64,
}

struct ResidentRepMetrics {
    keys_decomposed: u64,
    keys_merged: u64,
    ae_repaired_decomposed: u64,
    ae_repaired_merged: u64,
}

fn print_write_bench(name: &str, writers: u32, iters: u32, keys: &str, reps: &[WriteRepMetrics]) {
    println!(
        "BENCH {name} writers={writers} iters={iters} keys={keys} reps={} writes_per_sec={:.1} \
         p50_micros={:.1} p99_micros={:.1} lost_updates={} converge_secs={:.3} \
         frames_sent_total={} bytes_sent_total={} resident_keys={}{}",
        reps.len(),
        median_field_f64(reps, |m| m.writes_per_sec),
        median_field_f64(reps, |m| m.p50_micros),
        median_field_f64(reps, |m| m.p99_micros),
        median_field_u64(reps, |m| m.lost_updates),
        median_field_f64(reps, |m| m.converge_secs),
        median_field_u64(reps, |m| m.frames),
        median_field_u64(reps, |m| m.bytes),
        median_field_u64(reps, |m| m.resident_keys),
        ae_repaired_field(median_field_u64(reps, |m| m.ae_repaired)),
    );
}

fn print_micro_bench(name: &str, ops: u32, reps: &[MicroRepMetrics]) {
    println!(
        "BENCH {name} ops={ops} reps={} ns_per_apply={:.1} p50_nanos={:.1} p99_nanos={:.1}{}",
        reps.len(),
        median_field_f64(reps, |m| m.ns_per_apply),
        median_field_f64(reps, |m| m.p50_nanos),
        median_field_f64(reps, |m| m.p99_nanos),
        ae_repaired_field(median_field_u64(reps, |m| m.ae_repaired)),
    );
}

// ---------------------------------------------------------------------
// Scenario 1: naive_lww_counter — the lost-update problem, control, not a
// target to beat. `WRITERS` concurrent tasks race `get -> +1 -> insert`
// against one shared key under the default `LwwResolver`.
// ---------------------------------------------------------------------

async fn run_naive_rep(writers: u32, iters: u32, cache_name: &str) -> WriteRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let key = 0u32;
    let clusters = peer_group("bench-crdt-naive", 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = tokio::join!(
        cluster_a
            .cache::<u32, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_c
            .cache::<u32, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    cache_a.insert(key, 0).await.expect("seed succeeds");
    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&key).await == Some(0) && cache_c.get(&key).await == Some(0)
    })
    .await;

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let ae_repaired_before = ae_repaired_snapshot(cache_name);

    let started = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|_| {
            let cache_a = cache_a.clone();
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iters as usize);
                for _ in 0..iters {
                    let t0 = Instant::now();
                    let current = cache_a.get(&key).await.unwrap_or(0);
                    cache_a
                        .insert(key, current + 1)
                        .await
                        .expect("insert succeeds");
                    latencies.push(t0.elapsed());
                }
                latencies
            })
        })
        .collect();

    let mut all_latencies = Vec::with_capacity((writers * iters) as usize);
    for handle in handles {
        all_latencies.extend(handle.await.expect("writer worker did not panic"));
    }
    let elapsed = started.elapsed();
    all_latencies.sort_unstable();

    let total_writes = writers * iters;
    let expected = u64::from(writers) * u64::from(iters);
    let final_value = cache_a.get(&key).await.unwrap_or(0);
    let lost_updates = expected.saturating_sub(final_value);

    let convergence_started = Instant::now();
    common::eventually(Duration::from_secs(30), || async {
        cache_b.get(&key).await == Some(final_value) && cache_c.get(&key).await == Some(final_value)
    })
    .await;
    let converge_secs = convergence_started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let ae_repaired = ae_repaired_snapshot(cache_name).saturating_sub(ae_repaired_before);

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    WriteRepMetrics {
        writes_per_sec: f64::from(total_writes) / elapsed.as_secs_f64(),
        p50_micros: percentile_of(&all_latencies, 50.0).as_secs_f64() * 1_000_000.0,
        p99_micros: percentile_of(&all_latencies, 99.0).as_secs_f64() * 1_000_000.0,
        lost_updates,
        converge_secs,
        frames,
        bytes,
        resident_keys: 1,
        ae_repaired,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn naive_lww_counter() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let writers = writers();
    let iters = iters();
    let reps = repetitions();
    let cache_name = "crdt-naive";

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_naive_rep(writers, iters, cache_name)).await);
    }

    print_write_bench("naive_lww_counter", writers, iters, "1", &rep_metrics);
}

// ---------------------------------------------------------------------
// Scenario 2: decomposed_counter — today's best-practice workaround using
// only existing public API: `WRITERS` disjoint keys, each writer touching
// only its own, still `LwwResolver`. The real performance bar scenario 3 is
// measured against.
// ---------------------------------------------------------------------

async fn run_decomposed_rep(writers: u32, iters: u32, cache_name: &str) -> WriteRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let clusters = peer_group("bench-crdt-decomposed", 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = tokio::join!(
        cluster_a
            .cache::<u32, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_c
            .cache::<u32, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let ae_repaired_before = ae_repaired_snapshot(cache_name);

    let started = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let cache_a = cache_a.clone();
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iters as usize);
                for i in 1..=iters {
                    let t0 = Instant::now();
                    cache_a
                        .insert(w, u64::from(i))
                        .await
                        .expect("insert succeeds");
                    latencies.push(t0.elapsed());
                }
                latencies
            })
        })
        .collect();

    let mut all_latencies = Vec::with_capacity((writers * iters) as usize);
    for handle in handles {
        all_latencies.extend(handle.await.expect("writer worker did not panic"));
    }
    let elapsed = started.elapsed();
    all_latencies.sort_unstable();

    let mut total = 0u64;
    for w in 0..writers {
        total += cache_a.get(&w).await.unwrap_or(0);
    }
    let expected = u64::from(writers) * u64::from(iters);
    let lost_updates = expected.saturating_sub(total);

    let convergence_started = Instant::now();
    common::eventually(Duration::from_secs(30), || async {
        cache_b.entry_count().await == u64::from(writers)
            && cache_c.entry_count().await == u64::from(writers)
    })
    .await;
    let converge_secs = convergence_started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let ae_repaired = ae_repaired_snapshot(cache_name).saturating_sub(ae_repaired_before);
    let resident_keys = cache_b.entry_count().await;

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    WriteRepMetrics {
        writes_per_sec: f64::from(writers * iters) / elapsed.as_secs_f64(),
        p50_micros: percentile_of(&all_latencies, 50.0).as_secs_f64() * 1_000_000.0,
        p99_micros: percentile_of(&all_latencies, 99.0).as_secs_f64() * 1_000_000.0,
        lost_updates,
        converge_secs,
        frames,
        bytes,
        resident_keys,
        ae_repaired,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn decomposed_counter() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let writers = writers();
    let iters = iters();
    let reps = repetitions();
    let cache_name = "crdt-decomposed";

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_decomposed_rep(writers, iters, cache_name)).await);
    }

    print_write_bench(
        "decomposed_counter",
        writers,
        iters,
        &writers.to_string(),
        &rep_metrics,
    );
}

// ---------------------------------------------------------------------
// Scenarios 3/4: merged_counter_blind / merged_counter_rmw — `WRITERS`
// writers each keep a private cumulative total and call
// `PnCounter::local_delta` against one shared key, `PnCounterResolver`
// installed. `read_before_write` isolates whether 3's speedup comes from
// the resolver or merely from skipping the read round trip that 4 keeps.
// ---------------------------------------------------------------------

async fn run_merged_rep(
    writers: u32,
    iters: u32,
    cache_name: &str,
    read_before_write: bool,
) -> WriteRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let key = 0u32;
    let clusters = peer_group(cache_name, 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = Box::pin(open_replicated_trio::<PnCounter>(
        &cluster_a,
        &cluster_b,
        &cluster_c,
        cache_name,
        Arc::new(PnCounterResolver),
    ))
    .await;

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let ae_repaired_before = ae_repaired_snapshot(cache_name);

    let started = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let cache_a = cache_a.clone();
            let node = NodeId::from(u64::from(w));
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iters as usize);
                for i in 1..=iters {
                    let t0 = Instant::now();
                    if read_before_write {
                        let _ = cache_a.get(&key).await;
                    }
                    cache_a
                        .insert(key, PnCounter::local_delta(node, u64::from(i)))
                        .await
                        .expect("insert succeeds");
                    latencies.push(t0.elapsed());
                }
                latencies
            })
        })
        .collect();

    let mut all_latencies = Vec::with_capacity((writers * iters) as usize);
    for handle in handles {
        all_latencies.extend(handle.await.expect("writer worker did not panic"));
    }
    let elapsed = started.elapsed();
    all_latencies.sort_unstable();

    let expected = i64::from(writers) * i64::from(iters);
    let final_value = cache_a.get(&key).await.map_or(0, |c| c.value());
    let lost_updates = u64::try_from((expected - final_value).max(0)).unwrap_or(0);

    let convergence_started = Instant::now();
    common::eventually(Duration::from_secs(30), || async {
        cache_b.get(&key).await.map(|c| c.value()) == Some(final_value)
            && cache_c.get(&key).await.map(|c| c.value()) == Some(final_value)
    })
    .await;
    let converge_secs = convergence_started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let ae_repaired = ae_repaired_snapshot(cache_name).saturating_sub(ae_repaired_before);

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    WriteRepMetrics {
        writes_per_sec: f64::from(writers * iters) / elapsed.as_secs_f64(),
        p50_micros: percentile_of(&all_latencies, 50.0).as_secs_f64() * 1_000_000.0,
        p99_micros: percentile_of(&all_latencies, 99.0).as_secs_f64() * 1_000_000.0,
        lost_updates,
        converge_secs,
        frames,
        bytes,
        resident_keys: 1,
        ae_repaired,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn merged_counter_blind() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let writers = writers();
    let iters = iters();
    let reps = repetitions();
    let cache_name = "crdt-merged-blind";

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_merged_rep(writers, iters, cache_name, false)).await);
    }

    print_write_bench("merged_counter_blind", writers, iters, "1", &rep_metrics);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn merged_counter_rmw() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let writers = writers();
    let iters = iters();
    let reps = repetitions();
    let cache_name = "crdt-merged-rmw";

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_merged_rep(writers, iters, cache_name, true)).await);
    }

    print_write_bench("merged_counter_rmw", writers, iters, "1", &rep_metrics);
}

// ---------------------------------------------------------------------
// Scenario 5/5b: apply_ns_lww / apply_ns_merge / apply_ns_lww_forced_bytes —
// per-apply CPU cost, isolated from network/AE: a tight loop against a
// single-node `Mode::Local` cache, timing directly against one
// pre-populated, always-colliding key (every insert after the seed lands
// under a higher local Hlc, so every one hits the conflict-resolution
// branch rather than the "created" fast path).
// ---------------------------------------------------------------------

async fn run_micro_rep<V>(
    cluster_label: &str,
    cache_name: &str,
    resolver: Arc<dyn ConflictResolver>,
    ops: u32,
    seed: V,
    make: impl Fn(u32) -> V,
) -> MicroRepMetrics
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let key = 0u32;
    let cluster = local_cluster(cluster_label).await;
    let cache = cluster
        .cache::<u32, V>(cache_name)
        .mode(Mode::Local)
        .resolver(resolver)
        .open()
        .await
        .expect("cache opens");

    cache.insert(key, seed).await.expect("seed succeeds");
    let ae_repaired_before = ae_repaired_snapshot(cache_name);

    let mut latencies = Vec::with_capacity(ops as usize);
    let started = Instant::now();
    for i in 1..=ops {
        let t0 = Instant::now();
        cache.insert(key, make(i)).await.expect("insert succeeds");
        latencies.push(t0.elapsed());
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let ae_repaired = ae_repaired_snapshot(cache_name).saturating_sub(ae_repaired_before);

    cluster.shutdown().await;

    MicroRepMetrics {
        ns_per_apply: elapsed.as_secs_f64() * 1_000_000_000.0 / f64::from(ops),
        p50_nanos: percentile_of(&latencies, 50.0).as_secs_f64() * 1_000_000_000.0,
        p99_nanos: percentile_of(&latencies, 99.0).as_secs_f64() * 1_000_000_000.0,
        ae_repaired,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_ns_lww() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let ops = micro_ops();
    let reps = repetitions();
    let cache_name = "crdt-apply-lww";

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(
            run_micro_rep(
                "bench-crdt-apply-lww",
                cache_name,
                Arc::new(sundog::LwwResolver),
                ops,
                0u64,
                u64::from,
            )
            .await,
        );
    }

    print_micro_bench("apply_ns_lww", ops, &rep_metrics);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_ns_merge() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let ops = micro_ops();
    let reps = repetitions();
    let cache_name = "crdt-apply-merge";
    let node = NodeId::from(0u64);

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(
            run_micro_rep(
                "bench-crdt-apply-merge",
                cache_name,
                Arc::new(PnCounterResolver),
                ops,
                PnCounter::local_delta(node, 0),
                |i| PnCounter::local_delta(node, u64::from(i)),
            )
            .await,
        );
    }

    print_micro_bench("apply_ns_merge", ops, &rep_metrics);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_ns_lww_forced_bytes() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let ops = micro_ops();
    let reps = repetitions();
    let cache_name = "crdt-apply-lww-forced-bytes";

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(
            run_micro_rep(
                "bench-crdt-apply-lww-forced-bytes",
                cache_name,
                Arc::new(LwwForcedBytesResolver),
                ops,
                0u64,
                u64::from,
            )
            .await,
        );
    }

    print_micro_bench("apply_ns_lww_forced_bytes", ops, &rep_metrics);
}

// ---------------------------------------------------------------------
// Scenario 6: resident_keys_at_rest — a fresh 3-node cluster, populated with
// the same decomposed (`WRITERS` keys) and merged (one `PnCounter` key,
// written sequentially from a single node) shapes scenarios 2 and 3
// exercise, then left to converge with no further writes. Not a
// measurement of scenarios 2/3's own post-benchmark state — a standalone
// re-creation of the same two shapes, built once here rather than threaded
// through from those scenarios' clusters. The clearest, most durable win:
// O(1) resident keys for merge vs. O(`WRITERS`) for decomposition.
// ---------------------------------------------------------------------

async fn run_resident_rep(
    writers: u32,
    iters: u32,
    decomposed_name: &str,
    merged_name: &str,
) -> ResidentRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let clusters = peer_group("bench-crdt-resident", 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (dec_a, dec_b, dec_c) = tokio::join!(
        cluster_a
            .cache::<u32, u64>(decomposed_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, u64>(decomposed_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_c
            .cache::<u32, u64>(decomposed_name)
            .mode(Mode::Replicated)
            .open(),
    );
    let dec_a = dec_a.expect("decomposed a opens");
    let dec_b = dec_b.expect("decomposed b opens");
    let dec_c = dec_c.expect("decomposed c opens");

    let (mrg_a, mrg_b, mrg_c) = Box::pin(open_replicated_trio::<PnCounter>(
        &cluster_a,
        &cluster_b,
        &cluster_c,
        merged_name,
        Arc::new(PnCounterResolver),
    ))
    .await;

    let ae_repaired_decomposed_before = ae_repaired_snapshot(decomposed_name);
    let ae_repaired_merged_before = ae_repaired_snapshot(merged_name);

    for w in 0..writers {
        for i in 1..=iters {
            dec_a
                .insert(w, u64::from(i))
                .await
                .expect("decomposed insert succeeds");
        }
    }

    let node0 = NodeId::from(0u64);
    for cumulative in 1..=(u64::from(writers) * u64::from(iters)) {
        mrg_a
            .insert(0u32, PnCounter::local_delta(node0, cumulative))
            .await
            .expect("merged insert succeeds");
    }

    common::eventually(Duration::from_secs(30), || async {
        dec_b.entry_count().await == u64::from(writers)
            && dec_c.entry_count().await == u64::from(writers)
            && mrg_b.entry_count().await == 1
            && mrg_c.entry_count().await == 1
    })
    .await;

    let keys_decomposed = dec_b.entry_count().await;
    let keys_merged = mrg_b.entry_count().await;
    let ae_repaired_decomposed =
        ae_repaired_snapshot(decomposed_name).saturating_sub(ae_repaired_decomposed_before);
    let ae_repaired_merged =
        ae_repaired_snapshot(merged_name).saturating_sub(ae_repaired_merged_before);

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    ResidentRepMetrics {
        keys_decomposed,
        keys_merged,
        ae_repaired_decomposed,
        ae_repaired_merged,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn resident_keys_at_rest() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let writers = writers();
    let iters = iters();
    let reps = repetitions();
    let decomposed_name = "crdt-resident-decomposed";
    let merged_name = "crdt-resident-merged";

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(
            Box::pin(run_resident_rep(
                writers,
                iters,
                decomposed_name,
                merged_name,
            ))
            .await,
        );
    }

    let ae_decomposed = ae_repaired_field_named(
        "decomposed",
        median_field_u64(&rep_metrics, |m| m.ae_repaired_decomposed),
    );
    let ae_merged = ae_repaired_field_named(
        "merged",
        median_field_u64(&rep_metrics, |m| m.ae_repaired_merged),
    );
    println!(
        "BENCH resident_keys_at_rest writers={writers} iters={iters} reps={} \
         resident_keys_decomposed={} resident_keys_merged={}{ae_decomposed}{ae_merged}",
        rep_metrics.len(),
        median_field_u64(&rep_metrics, |m| m.keys_decomposed),
        median_field_u64(&rep_metrics, |m| m.keys_merged),
    );
}

// ---------------------------------------------------------------------
// Scenarios 7-8 shared shape: `N` counters (`keys`, `SUNDOG_BENCH_KEYS`),
// `SCALE_WRITERS` writers — one per warm-cluster node, not the
// `SUNDOG_BENCH_WRITERS`-controlled count scenarios 1-4 use — each
// contributing exactly [`SCALE_INCREMENTS_PER_WRITER`] to every counter, so
// `keys` alone scales cost and every counter's converged value is
// [`scale_expected_total`] regardless of `keys`. The decomposed variant
// gives writer `w` its own key `counter:{i}:{w}` per counter `i` under the
// default `LwwResolver` (`SCALE_WRITERS * keys` resident keys); the merged
// variant gives every counter one key `i` under `PnCounterResolver`
// (`keys` resident keys). Both variants apply the identical
// `SCALE_WRITERS * keys` logical increments.
// ---------------------------------------------------------------------

/// The writers scenarios 7 and 8 share: one per warm-cluster node, so
/// "every counter incremented by every node" falls out of the cluster's own
/// membership rather than an arbitrary writer count.
const SCALE_WRITERS: u32 = 3;

/// Each writer's contribution to every counter in scenarios 7 and 8: the
/// smallest count that still satisfies "incremented by every node", so
/// total write volume is `SCALE_WRITERS * keys` regardless of how large
/// `keys` is set.
const SCALE_INCREMENTS_PER_WRITER: u64 = 1;

/// Every counter's expected value once every writer's increment has
/// landed: `SCALE_WRITERS` writers times [`SCALE_INCREMENTS_PER_WRITER`]
/// each.
fn scale_expected_total() -> u64 {
    u64::from(SCALE_WRITERS) * SCALE_INCREMENTS_PER_WRITER
}

/// The decomposed variant's per-writer key for counter `i` written by
/// writer `w`.
fn decomposed_key(i: u32, w: u32) -> String {
    format!("counter:{i}:{w}")
}

/// The decomposed variant's reader: sums counter `i`'s `SCALE_WRITERS`
/// per-writer keys on `cache`, `0` for any writer that hasn't landed yet. A
/// counter is converged when this sum matches [`scale_expected_total`].
async fn decomposed_counter_sum(cache: &sundog::Cache<String, u64>, i: u32) -> u64 {
    let mut sum = 0u64;
    for w in 0..SCALE_WRITERS {
        sum += cache.get(&decomposed_key(i, w)).await.unwrap_or(0);
    }
    sum
}

/// `true` once every one of `keys` counters sums to [`scale_expected_total`]
/// on every cache in `caches`, decomposed-variant reader. `entry_count`
/// alone is not a converged signal here: a key can exist the moment any one
/// writer's record lands, before the other writers' records — or, for the
/// merged variant's [`merged_all_converged`], before a remote merge — have
/// applied, so an `entry_count`-only gate can pass while a counter still
/// reads a partial sum. Short-circuits on the first unconverged counter, so
/// an early, mostly-unconverged poll stays cheap.
async fn decomposed_all_converged(caches: &[&sundog::Cache<String, u64>], keys: u32) -> bool {
    for i in 0..keys {
        for cache in caches {
            if decomposed_counter_sum(cache, i).await != scale_expected_total() {
                return false;
            }
        }
    }
    true
}

/// [`decomposed_all_converged`]'s merged-variant counterpart: `true` once
/// every one of `keys` counters reads exactly `expected` on every cache in
/// `caches`.
async fn merged_all_converged(
    caches: &[&sundog::Cache<u32, PnCounter>],
    keys: u32,
    expected: i64,
) -> bool {
    for i in 0..keys {
        for cache in caches {
            if cache.get(&i).await.map(|c| c.value()) != Some(expected) {
                return false;
            }
        }
    }
    true
}

// ---------------------------------------------------------------------
// Scenario 7: cold_join_initial_replication — a warm 3-node trio converges
// on `SCALE_WRITERS * keys` logical increments, then a fourth node joins
// and pulls the resulting state. `Cache::open` on a `Mode::Replicated`
// cache blocks until the full state-transfer snapshot from the live donor
// with the lowest node id has landed and one anti-entropy round against
// that donor has run, so timing from just before the fourth node is built
// to just after its cache reports every counter's exact total is exactly
// the cold-join replication cost this scenario measures.
// ---------------------------------------------------------------------

/// `SUNDOG_BENCH_KEYS` default for `cold_join_initial_replication`. The
/// plan's own default is `20_000`; at that size, the population writes are
/// cheap (one `insert_many` per writer), but the exact-total correctness
/// pass this scenario runs before and after the join — `SCALE_WRITERS`
/// real `.get()` round trips per counter, on top of `keys` themselves —
/// does not pipeline and would risk the 3-minute budget once
/// `repetitions()` reps and both variants are added up on a 4-core box.
/// `2_000` keeps that pass fast while still exercising a cold join against a
/// many-key cache; `SUNDOG_BENCH_KEYS` overrides for a real capacity run.
const COLD_JOIN_KEYS_DEFAULT: u32 = 2_000;

struct ColdJoinRepMetrics {
    join_secs: f64,
    frames: u64,
    bytes: u64,
    entries_received: u64,
}

fn print_cold_join_bench(variant: &str, keys: u32, reps: &[ColdJoinRepMetrics]) {
    println!(
        "BENCH cold_join_initial_replication_{variant} keys={keys} reps={} join_secs={:.3} \
         frames_sent_total={} bytes_sent_total={}{}",
        reps.len(),
        median_field_f64(reps, |m| m.join_secs),
        median_field_u64(reps, |m| m.frames),
        median_field_u64(reps, |m| m.bytes),
        entries_received_field(median_field_u64(reps, |m| m.entries_received)),
    );
}

async fn run_cold_join_decomposed_rep(keys: u32) -> ColdJoinRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let cluster_label = "bench-crdt-cold-join-decomposed";
    let cache_name = "crdt-cold-join-decomposed";
    let clusters = peer_group(cluster_label, 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = tokio::join!(
        cluster_a
            .cache::<String, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<String, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_c
            .cache::<String, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    for (writer, cache) in [&cache_a, &cache_b, &cache_c].into_iter().enumerate() {
        let writer = u32::try_from(writer).expect("3-element writer index fits in u32");
        let entries = (0..keys).map(|i| (decomposed_key(i, writer), SCALE_INCREMENTS_PER_WRITER));
        cache
            .insert_many(entries)
            .await
            .expect("decomposed population insert_many succeeds");
    }

    common::eventually(Duration::from_secs(30), || async {
        decomposed_all_converged(&[&cache_a, &cache_b, &cache_c], keys).await
    })
    .await;

    let seed_addr = cluster_a
        .peers()
        .first()
        .map(|peer| peer.gossip_addr)
        .expect("cluster_a reports at least one live peer after peer_group's wait");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let entries_received_before = entries_received_snapshot(cache_name);

    let started = Instant::now();
    let fourth = Cluster::builder(cluster_label)
        .seeds([seed_addr])
        .config(node_config(reserve_gossip_addr().await))
        .build()
        .await
        .expect("fourth node builds");
    let joiner = fourth
        .cache::<String, u64>(cache_name)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("fourth opens the decomposed cache");
    common::eventually(Duration::from_secs(30), || async {
        decomposed_all_converged(&[&joiner], keys).await
    })
    .await;
    let join_secs = started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let entries_received =
        entries_received_snapshot(cache_name).saturating_sub(entries_received_before);

    fourth.shutdown().await;
    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    ColdJoinRepMetrics {
        join_secs,
        frames,
        bytes,
        entries_received,
    }
}

async fn run_cold_join_merged_rep(keys: u32) -> ColdJoinRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let cluster_label = "bench-crdt-cold-join-merged";
    let cache_name = "crdt-cold-join-merged";
    let clusters = peer_group(cluster_label, 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = Box::pin(open_replicated_trio::<PnCounter>(
        &cluster_a,
        &cluster_b,
        &cluster_c,
        cache_name,
        Arc::new(PnCounterResolver),
    ))
    .await;

    for (writer, cache) in [&cache_a, &cache_b, &cache_c].into_iter().enumerate() {
        let writer = u32::try_from(writer).expect("3-element writer index fits in u32");
        let node = NodeId::from(u64::from(writer));
        let entries =
            (0..keys).map(|i| (i, PnCounter::local_delta(node, SCALE_INCREMENTS_PER_WRITER)));
        cache
            .insert_many(entries)
            .await
            .expect("merged population insert_many succeeds");
    }

    let expected = i64::try_from(scale_expected_total()).unwrap_or(i64::MAX);
    common::eventually(Duration::from_secs(30), || async {
        merged_all_converged(&[&cache_a, &cache_b, &cache_c], keys, expected).await
    })
    .await;

    let seed_addr = cluster_a
        .peers()
        .first()
        .map(|peer| peer.gossip_addr)
        .expect("cluster_a reports at least one live peer after peer_group's wait");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let entries_received_before = entries_received_snapshot(cache_name);

    let started = Instant::now();
    let fourth = Cluster::builder(cluster_label)
        .seeds([seed_addr])
        .config(node_config(reserve_gossip_addr().await))
        .build()
        .await
        .expect("fourth node builds");
    let joiner = fourth
        .cache::<u32, PnCounter>(cache_name)
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("fourth opens the merged cache");
    common::eventually(Duration::from_secs(30), || async {
        merged_all_converged(&[&joiner], keys, expected).await
    })
    .await;
    let join_secs = started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let entries_received =
        entries_received_snapshot(cache_name).saturating_sub(entries_received_before);

    fourth.shutdown().await;
    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    ColdJoinRepMetrics {
        join_secs,
        frames,
        bytes,
        entries_received,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cold_join_initial_replication_decomposed() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let keys = scale_keys(COLD_JOIN_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_cold_join_decomposed_rep(keys)).await);
    }

    print_cold_join_bench("decomposed", keys, &rep_metrics);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cold_join_initial_replication_merged() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let keys = scale_keys(COLD_JOIN_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_cold_join_merged_rep(keys)).await);
    }

    print_cold_join_bench("merged", keys, &rep_metrics);
}

// ---------------------------------------------------------------------
// Scenario 8: large_entity_convergence — `SCALE_WRITERS` writers, one per
// warm-cluster node, each concurrently write every one of `keys` counters
// (one `.insert` per counter, not a bulk `insert_many`, since the point is
// genuinely concurrent cross-node writes racing anti-entropy rather than a
// single burst) while `fast_config()`'s anti-entropy loop keeps running.
// Convergence is timed from the last writer's last write.
// ---------------------------------------------------------------------

/// `SUNDOG_BENCH_KEYS` default for `large_entity_convergence`, lowered from
/// the plan's own `100_000` for the same reason [`COLD_JOIN_KEYS_DEFAULT`] is:
/// this scenario's writers issue one real `.insert` per counter rather than
/// a bulk `insert_many`, and its convergence check reads every counter back
/// from all three nodes, so `100_000` would risk the 3-minute budget once
/// `repetitions()` reps and both variants are added up on a 4-core box.
/// `4_000` keeps the same shape observable — many entities, concurrent
/// writers, anti-entropy live — well inside budget; `SUNDOG_BENCH_KEYS`
/// overrides for a real capacity run.
const LARGE_ENTITY_KEYS_DEFAULT: u32 = 4_000;

struct ScaleConvergeRepMetrics {
    converge_secs: f64,
    frames: u64,
    bytes: u64,
    ae_repaired: u64,
    resident_keys: u64,
    lost_updates: u64,
}

fn print_scale_convergence_bench(variant: &str, keys: u32, reps: &[ScaleConvergeRepMetrics]) {
    println!(
        "BENCH large_entity_convergence_{variant} keys={keys} reps={} converge_secs={:.3} \
         frames_sent_total={} bytes_sent_total={} resident_keys={} lost_updates={}{}",
        reps.len(),
        median_field_f64(reps, |m| m.converge_secs),
        median_field_u64(reps, |m| m.frames),
        median_field_u64(reps, |m| m.bytes),
        median_field_u64(reps, |m| m.resident_keys),
        median_field_u64(reps, |m| m.lost_updates),
        ae_repaired_field(median_field_u64(reps, |m| m.ae_repaired)),
    );
}

async fn run_large_entity_decomposed_rep(keys: u32) -> ScaleConvergeRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let cluster_label = "bench-crdt-large-entity-decomposed";
    let cache_name = "crdt-large-entity-decomposed";
    let clusters = peer_group(cluster_label, 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = tokio::join!(
        cluster_a
            .cache::<String, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<String, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
        cluster_c
            .cache::<String, u64>(cache_name)
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let ae_repaired_before = ae_repaired_snapshot(cache_name);

    let handles: Vec<_> = [cache_a.clone(), cache_b.clone(), cache_c.clone()]
        .into_iter()
        .enumerate()
        .map(|(writer, cache)| {
            let writer = u32::try_from(writer).expect("3-element writer index fits in u32");
            tokio::spawn(async move {
                for i in 0..keys {
                    cache
                        .insert(decomposed_key(i, writer), SCALE_INCREMENTS_PER_WRITER)
                        .await
                        .expect("decomposed insert succeeds");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.expect("writer task did not panic");
    }

    // Sampled immediately after the writers finish and before the
    // convergence wait below, mirroring the naive scenario's own
    // `lost_updates`: `eventually` panics on timeout rather than returning a
    // partial result, so sampling any later would make this structurally
    // zero rather than a measurement of the post-write, pre-convergence gap.
    let mut lost_updates = 0u64;
    for i in 0..keys {
        lost_updates +=
            scale_expected_total().saturating_sub(decomposed_counter_sum(&cache_a, i).await);
    }

    let convergence_started = Instant::now();
    common::eventually(Duration::from_secs(60), || async {
        decomposed_all_converged(&[&cache_a, &cache_b, &cache_c], keys).await
    })
    .await;
    let converge_secs = convergence_started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let ae_repaired = ae_repaired_snapshot(cache_name).saturating_sub(ae_repaired_before);
    let resident_keys = cache_b.entry_count().await;

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    ScaleConvergeRepMetrics {
        converge_secs,
        frames,
        bytes,
        ae_repaired,
        resident_keys,
        lost_updates,
    }
}

async fn run_large_entity_merged_rep(keys: u32) -> ScaleConvergeRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let cluster_label = "bench-crdt-large-entity-merged";
    let cache_name = "crdt-large-entity-merged";
    let clusters = peer_group(cluster_label, 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = Box::pin(open_replicated_trio::<PnCounter>(
        &cluster_a,
        &cluster_b,
        &cluster_c,
        cache_name,
        Arc::new(PnCounterResolver),
    ))
    .await;

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let ae_repaired_before = ae_repaired_snapshot(cache_name);

    let handles: Vec<_> = [cache_a.clone(), cache_b.clone(), cache_c.clone()]
        .into_iter()
        .enumerate()
        .map(|(writer, cache)| {
            let writer = u32::try_from(writer).expect("3-element writer index fits in u32");
            let node = NodeId::from(u64::from(writer));
            tokio::spawn(async move {
                for i in 0..keys {
                    cache
                        .insert(i, PnCounter::local_delta(node, SCALE_INCREMENTS_PER_WRITER))
                        .await
                        .expect("merged insert succeeds");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.expect("writer task did not panic");
    }

    // Sampled immediately after the writers finish and before the
    // convergence wait below, mirroring the naive scenario's own
    // `lost_updates`: `eventually` panics on timeout rather than returning a
    // partial result, so sampling any later would make this structurally
    // zero rather than a measurement of the post-write, pre-convergence gap.
    let mut lost_updates = 0u64;
    for i in 0..keys {
        let actual = cache_a.get(&i).await.map_or(0, |c| c.value());
        let actual = u64::try_from(actual).unwrap_or(0);
        lost_updates += scale_expected_total().saturating_sub(actual);
    }

    let expected = i64::try_from(scale_expected_total()).unwrap_or(i64::MAX);
    let convergence_started = Instant::now();
    common::eventually(Duration::from_secs(60), || async {
        merged_all_converged(&[&cache_a, &cache_b, &cache_c], keys, expected).await
    })
    .await;
    let converge_secs = convergence_started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let ae_repaired = ae_repaired_snapshot(cache_name).saturating_sub(ae_repaired_before);
    let resident_keys = cache_b.entry_count().await;

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    ScaleConvergeRepMetrics {
        converge_secs,
        frames,
        bytes,
        ae_repaired,
        resident_keys,
        lost_updates,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn large_entity_convergence_decomposed() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let keys = scale_keys(LARGE_ENTITY_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_large_entity_decomposed_rep(keys)).await);
    }

    print_scale_convergence_bench("decomposed", keys, &rep_metrics);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn large_entity_convergence_merged() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let keys = scale_keys(LARGE_ENTITY_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_large_entity_merged_rep(keys)).await);
    }

    print_scale_convergence_bench("merged", keys, &rep_metrics);
}
