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
//! finish every scenario in well under a minute. A smoke run:
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
//! no peers, isolating per-apply CPU cost from all network/AE noise. No key
//! ever expires or is removed in any scenario — TTL is irrelevant here,
//! stated to rule out a confound rather than leave it implicit. Every
//! numeric field is the median of at least [`repetitions`] independent
//! runs, so a single noisy run never skews a reported number.

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
