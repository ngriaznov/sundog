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
//! finish every scenario in well under a minute. Scenarios 7, 8, and 9
//! scale a third axis instead, an entity or batch count `N`
//! (`SUNDOG_BENCH_KEYS`); scenarios 7 and 8 fix the writer count at 3 (one
//! per warm-cluster node) and each writer's contribution to each entity at
//! a single increment, so `N` alone governs their cost, and scenario 9
//! reuses the same knob for its own batch size, no network or writer count
//! involved. Their defaults are lower than the plan's own (documented at
//! each default's definition) to keep every variant inside a 3-minute
//! budget on a 4-core box. A smoke run:
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
//!
//! Scenarios 9-12 isolate a merging resolver's two write-path levers: Lever
//! A (`Engine::apply_many`'s batch pre-fold, `sundog/src/store/engine.rs`)
//! in scenarios 9 and 10, Lever B (`Cache::merge`'s coalescing window) in
//! scenarios 11 and 12. Both levers' on/off (or window) comparisons are
//! measured through the real toggle now: `CacheBuilder::prefold_enabled`
//! (`#[doc(hidden)]`, `sundog/src/cache.rs`) threads down to
//! `Shard::with_prefold_enabled` and `Engine::set_prefold_enabled`, so this
//! binary, an ordinary downstream crate, can open a cache with pre-fold
//! genuinely off rather than approximating it. Scenario 9
//! (`apply_many_prefold`) runs the identical `insert_many` batch against
//! two caches that differ only in that flag — "on" the default, "off"
//! `.prefold_enabled(false)` — so both sides pay the same single stripe-lock
//! acquisition and the same batch shape; only whether `apply_many` folds a
//! same-key run before applying it differs. Scenario 10
//! (`replicated_hot_counter_receive`) applies the same toggle to the two
//! *receiving* nodes: a receiving node's batch shape comes from the
//! sender's own fan-out, not from anything this crate builds directly, but
//! `CacheBuilder::prefold_enabled` reaches that node's engine exactly the
//! same way regardless of who assembled the batch, so both an "on" and a
//! real "off" run are reported. Scenarios 11 and 12 use `Cache::merge` and
//! `CacheBuilder::merge_coalesce_window` directly, both genuinely public, so
//! neither needed a new seam.
//! Scenarios 9-12 all report "engine applies" as `Cache::events()`'s own
//! count: one event per non-no-op apply, the only public-API signal for
//! how many times the engine actually applied, per `Cache::merge`'s and the
//! resolver contract's own docs.
//!
//! Scenario 13 (`sketch_path_convergence`) checks a merging resolver's
//! scale-hardening on the IBLT sketch path: `ClusterConfig::ae_sketch_min_bucket`
//! lowered, and `keys` filler entries forced into one anti-entropy bucket,
//! so a deliberately seeded mismatch there (`Cache::invalidate_local` on one
//! node, past state transfer entirely) answers with a sketch
//! (`cluster::sketch::Iblt`) rather than a listing. `lww` (non-merging) and
//! `pn_counter` (merging) both run it, sharing the `keys` knob. It reports
//! `sundog_ae_sketch_total`'s existing `decoded`/`fallback` outcome
//! counters — no new metric, since that counter already covers a peel
//! success and a peel fallback exactly — as "`sketch_peeled`" and
//! "`sketch_fallback`", plus their sum as "`sketch_rounds`": how many times
//! this scenario's bucket answered a mismatch through the sketch mechanism
//! at all, successfully or not, before it reconverged.

mod common;

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, ClusterConfig, ConflictResolver, Mode, NodeId, RecordView, Winner};
use xxhash_rust::xxh3::xxh3_64;

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
/// `cluster_a`/`cluster_b`/`cluster_c`, sharing one `resolver`, pre-fold on
/// (the default) for all three.
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
    Box::pin(open_replicated_trio_with_prefold(
        cluster_a,
        cluster_b,
        cluster_c,
        cache_name,
        resolver,
        [true, true, true],
    ))
    .await
}

/// Like [`open_replicated_trio`], but opens each node's cache with
/// `CacheBuilder::prefold_enabled` (`sundog/src/cache.rs`'s `#[doc(hidden)]`
/// mirror of `Engine::set_prefold_enabled`) set from `prefold_enabled`,
/// node a/b/c in order. Scenario 10 uses this to build its two *receiving*
/// nodes with the flag genuinely off, rather than scenario 9's
/// identical-batch, differently-toggled-cache comparison — there is no
/// local batch to build differently on the receive path, only the real
/// flag each receiving node opens with.
async fn open_replicated_trio_with_prefold<V>(
    cluster_a: &Cluster,
    cluster_b: &Cluster,
    cluster_c: &Cluster,
    cache_name: &str,
    resolver: Arc<dyn ConflictResolver>,
    prefold_enabled: [bool; 3],
) -> (
    sundog::Cache<u32, V>,
    sundog::Cache<u32, V>,
    sundog::Cache<u32, V>,
)
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let [a_prefold, b_prefold, c_prefold] = prefold_enabled;
    let (a, b, c) = tokio::join!(
        cluster_a
            .cache::<u32, V>(cache_name)
            .mode(Mode::Replicated)
            .resolver(resolver.clone())
            .prefold_enabled(a_prefold)
            .open(),
        cluster_b
            .cache::<u32, V>(cache_name)
            .mode(Mode::Replicated)
            .resolver(resolver.clone())
            .prefold_enabled(b_prefold)
            .open(),
        cluster_c
            .cache::<u32, V>(cache_name)
            .mode(Mode::Replicated)
            .resolver(resolver)
            .prefold_enabled(c_prefold)
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

/// The current value of `sundog_ae_sketch_total{cache=cache_name,outcome=outcome}`
/// — scenario 13's own peel-success (`outcome="decoded"`) and fallback
/// (`outcome="fallback"`) counts, the existing metric
/// `cluster::anti_entropy::handle_sketch_mismatch` already emits, reused
/// here rather than adding a new one — or 0 if the recorder never installed
/// or the counter never incremented.
#[cfg(feature = "prometheus")]
fn sketch_outcome_total(cache_name: &str, outcome: &str) -> u64 {
    let value = metrics_handle().and_then(|h| {
        scraped_metric(
            &h.render(),
            "sundog_ae_sketch_total",
            &[("cache", cache_name), ("outcome", outcome)],
        )
    });
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "sundog_ae_sketch_total is a nonnegative counter"
    )]
    let count = value.unwrap_or(0.0).round() as u64;
    count
}

/// [`ae_repaired_snapshot`]'s counterpart for [`sketch_outcome_total`], for
/// a before/after delta around one repetition's writes and convergence
/// wait.
#[cfg(feature = "prometheus")]
fn sketch_outcome_snapshot(cache_name: &str, outcome: &str) -> u64 {
    sketch_outcome_total(cache_name, outcome)
}

#[cfg(not(feature = "prometheus"))]
fn sketch_outcome_snapshot(_cache_name: &str, _outcome: &str) -> u64 {
    0
}

/// Renders scenario 13's own `sketch_rounds`/`sketch_peeled`/`sketch_fallback`
/// fields: `sketch_rounds` is `decoded + fallback`, how many times this
/// scenario's buckets answered a mismatch through the sketch mechanism at
/// all, successfully or not. Empty under a build without `prometheus`,
/// matching every other `prometheus`-gated field on a `BENCH` line.
#[cfg(feature = "prometheus")]
fn sketch_outcome_field(decoded: u64, fallback: u64) -> String {
    format!(
        " sketch_rounds={} sketch_peeled={decoded} sketch_fallback={fallback}",
        decoded + fallback
    )
}

#[cfg(not(feature = "prometheus"))]
fn sketch_outcome_field(_decoded: u64, _fallback: u64) -> String {
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

// ---------------------------------------------------------------------
// Shared by scenarios 9-12: counts every `Event` a cache's public
// `Cache::events()` broadcast stream carries. One event publishes per
// non-no-op apply — a redelivered merge that reproduces exactly what's
// already stored publishes nothing, per `merge_version`'s own no-op arm — so
// this is the one public-API signal for how many times the engine actually
// applied, the "engine applies" field on
// every `BENCH` line below. Runs until the cache's sender side drops (the
// cache closes) or the caller aborts the returned handle; a lagged receiver
// (the counting task falling behind the publish rate) adds the lagged count
// rather than losing it, so a slow poll still reports the true total.
// ---------------------------------------------------------------------

fn spawn_event_counter<K, V>(
    cache: &sundog::Cache<K, V>,
) -> (tokio::task::JoinHandle<()>, Arc<AtomicU64>)
where
    K: std::hash::Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let mut events = cache.events();
    let count = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&count);
    let handle = tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(_) => {
                    counted.fetch_add(1, Ordering::Relaxed);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    counted.fetch_add(n, Ordering::Relaxed);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    (handle, count)
}

// ---------------------------------------------------------------------
// Scenario 9: apply_many_prefold — Lever A, batch pre-folding ahead of the
// stripe lock (`Engine::apply_many`, `sundog/src/store/engine.rs`). No
// network: a fresh single-node `Mode::Local` cluster per rep, matching
// scenario 5's own isolation of apply cost from all network/AE noise. Two
// batch shapes, both of `batch_size` (`SUNDOG_BENCH_KEYS`, default 1,000)
// records: `one_key`, every record colliding on a single key (pre-fold's
// best case, a run of `batch_size` puts folded to one survivor), and
// `many_keys`, one record per distinct key (pre-fold's worst case: nothing
// to fold, so its only cost is the batch's own by-key grouping pass). Each
// shape runs under both `LwwResolver` (`merges() == false`, pre-fold never
// engages regardless of the flag) and `PnCounterResolver` (`merges() ==
// true`) — "on" and "off" now open two caches that differ only in
// `CacheBuilder::prefold_enabled`, both driven by the identical
// `insert_many` call over the identical batch: the only thing that differs
// between the two timed passes is whether `Engine::apply_many` folds a
// same-key run before applying it. The `many_keys` shape's on/off pair is
// expected to land close together for both resolvers — there is nothing to
// fold either way, so it isolates pre-fold's idle grouping-pass overhead
// from its `one_key` fold benefit.
// ---------------------------------------------------------------------

/// `SUNDOG_BENCH_KEYS` default for `apply_many_prefold`'s batch size,
/// matching the plan's own 1,000-record batch.
const PREFOLD_BATCH_DEFAULT: u32 = 1_000;

fn prefold_batch_size() -> u32 {
    scale_keys(PREFOLD_BATCH_DEFAULT)
}

struct PrefoldRepMetrics {
    record_ns_on: f64,
    batch_ns_on: f64,
    record_ns_off: f64,
    batch_ns_off: f64,
}

fn print_prefold_bench(name: &str, batch_size: u32, reps: &[PrefoldRepMetrics]) {
    println!(
        "BENCH {name} batch_size={batch_size} reps={} record_ns_on={:.1} \
         batch_ns_on={:.1} record_ns_off={:.1} batch_ns_off={:.1}",
        reps.len(),
        median_field_f64(reps, |m| m.record_ns_on),
        median_field_f64(reps, |m| m.batch_ns_on),
        median_field_f64(reps, |m| m.record_ns_off),
        median_field_f64(reps, |m| m.batch_ns_off),
    );
}

/// `run_prefold_rep`'s one timed pass: opens `cache_name` on `cluster` with
/// `CacheBuilder::prefold_enabled` set from `prefold_enabled`, times one
/// `insert_many(entries)` call against it, then closes the cache so the
/// name is free for the other pass to reopen.
async fn timed_insert_many<V>(
    cluster: &Cluster,
    cache_name: &str,
    resolver: Arc<dyn ConflictResolver>,
    prefold_enabled: bool,
    entries: Vec<(u32, V)>,
) -> Duration
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let cache = cluster
        .cache::<u32, V>(cache_name)
        .mode(Mode::Local)
        .resolver(resolver)
        .prefold_enabled(prefold_enabled)
        .open()
        .await
        .expect("cache opens");
    let started = Instant::now();
    cache
        .insert_many(entries)
        .await
        .expect("insert_many applies");
    let elapsed = started.elapsed();
    cache.close().await;
    elapsed
}

/// One `apply_many_prefold` rep for one (shape, resolver) combo: opens two
/// fresh single-node caches under the same name in turn — "on"
/// (`CacheBuilder::prefold_enabled`'s default `true`) and "off"
/// (`.prefold_enabled(false)`) — closing the first before opening the
/// second so both can use the same `cache_name`, and times the identical
/// `batch_size`-record `insert_many` call against each. `swap_order` runs
/// "off" before "on" instead of the reverse — the caller alternates it
/// across reps so whichever pass runs second on a freshly built cluster
/// (and so benefits from the first pass's allocator/JIT warm-up) is not
/// always the same one, canceling that bias out across reps rather than
/// always favoring "off".
///
/// Both passes take the identical single stripe-lock acquisition for the
/// whole batch (`insert_many` always does, pre-fold or not — see
/// `Engine::apply_many`) and pay the identical per-call fan-out push, so the
/// only thing that can differ between them is whether `apply_many` actually
/// folds a same-key run before applying it — a real measurement of the
/// flag, not a proxy. `LwwResolver::merges()` is `false`, so it never
/// triggers pre-fold at all regardless of the flag: its on/off gap is
/// expected to land near zero, which `print_prefold_isolated_delta` checks
/// by subtracting it from `PnCounterResolver`'s gap over the same shape.
async fn run_prefold_rep<V>(
    cluster_label: &str,
    cache_name: &str,
    resolver: Arc<dyn ConflictResolver>,
    batch_size: u32,
    one_key: bool,
    swap_order: bool,
    make: impl Fn(u32) -> V,
) -> PrefoldRepMetrics
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let cluster = local_cluster(cluster_label).await;

    let key_for = |i: u32| if one_key { 0 } else { i };
    let entries: Vec<(u32, V)> = (0..batch_size).map(|i| (key_for(i), make(i))).collect();

    let (on_elapsed, off_elapsed) = if swap_order {
        let off_elapsed = timed_insert_many(
            &cluster,
            cache_name,
            resolver.clone(),
            false,
            entries.clone(),
        )
        .await;
        let on_elapsed = timed_insert_many(&cluster, cache_name, resolver, true, entries).await;
        (on_elapsed, off_elapsed)
    } else {
        let on_elapsed = timed_insert_many(
            &cluster,
            cache_name,
            resolver.clone(),
            true,
            entries.clone(),
        )
        .await;
        let off_elapsed = timed_insert_many(&cluster, cache_name, resolver, false, entries).await;
        (on_elapsed, off_elapsed)
    };

    cluster.shutdown().await;

    PrefoldRepMetrics {
        record_ns_on: on_elapsed.as_secs_f64() * 1_000_000_000.0 / f64::from(batch_size),
        batch_ns_on: on_elapsed.as_secs_f64() * 1_000_000_000.0,
        record_ns_off: off_elapsed.as_secs_f64() * 1_000_000_000.0 / f64::from(batch_size),
        batch_ns_off: off_elapsed.as_secs_f64() * 1_000_000_000.0,
    }
}

/// `LwwResolver` never triggers pre-fold (`merges()` is `false`), so its
/// on/off gap, measured through the identical `run_prefold_rep` toggle, is
/// expected to land near zero; subtracting it from `PnCounterResolver`'s
/// gap over the same shape and batch size is a sanity check on that
/// expectation rather than a correction for proxy overhead, now that both
/// resolvers' "on" and "off" passes run the identical `insert_many` call.
fn print_prefold_isolated_delta(
    name: &str,
    batch_size: u32,
    lww: &[PrefoldRepMetrics],
    pncounter: &[PrefoldRepMetrics],
) {
    let lww_gap_ns = median_field_f64(lww, |m| m.batch_ns_off - m.batch_ns_on);
    let pncounter_gap_ns = median_field_f64(pncounter, |m| m.batch_ns_off - m.batch_ns_on);
    println!(
        "BENCH {name} batch_size={batch_size} lww_gap_ns={lww_gap_ns:.1} \
         pncounter_gap_ns={pncounter_gap_ns:.1} \
         prefold_isolated_gap_ns={:.1}",
        pncounter_gap_ns - lww_gap_ns,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_many_prefold() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let batch_size = prefold_batch_size();
    let reps = repetitions();
    let node = NodeId::from(0u64);

    for (shape_name, one_key) in [("one_key", true), ("many_keys", false)] {
        let mut lww_metrics = Vec::with_capacity(reps as usize);
        for rep_idx in 0..reps {
            lww_metrics.push(
                run_prefold_rep(
                    &format!("bench-crdt-prefold-{shape_name}-lww"),
                    &format!("crdt-prefold-{shape_name}-lww"),
                    Arc::new(sundog::LwwResolver),
                    batch_size,
                    one_key,
                    rep_idx % 2 == 1,
                    u64::from,
                )
                .await,
            );
        }
        print_prefold_bench(
            &format!("apply_many_prefold_{shape_name}_lww"),
            batch_size,
            &lww_metrics,
        );

        let mut pncounter_metrics = Vec::with_capacity(reps as usize);
        for rep_idx in 0..reps {
            pncounter_metrics.push(
                run_prefold_rep(
                    &format!("bench-crdt-prefold-{shape_name}-pncounter"),
                    &format!("crdt-prefold-{shape_name}-pncounter"),
                    Arc::new(PnCounterResolver),
                    batch_size,
                    one_key,
                    rep_idx % 2 == 1,
                    move |i| PnCounter::local_delta(node, u64::from(i) + 1),
                )
                .await,
            );
        }
        print_prefold_bench(
            &format!("apply_many_prefold_{shape_name}_pncounter"),
            batch_size,
            &pncounter_metrics,
        );
        print_prefold_isolated_delta(
            &format!("apply_many_prefold_{shape_name}"),
            batch_size,
            &lww_metrics,
            &pncounter_metrics,
        );
    }
}

// ---------------------------------------------------------------------
// Scenario 10: replicated_hot_counter_receive — Lever A on the replication
// receive path: the same eight-writer single-`PnCounter`-key shape as
// scenarios 3/4, but instrumented on the two nodes that only ever *receive*
// the resulting writes rather than the one issuing them, so a concurrent
// writer burst on `cache_a` arrives at `cache_b`/`cache_c` as fan-out and
// anti-entropy batches carrying several records for the same key —
// `apply_remote_batch`'s own batch shape, exactly what pre-fold folds. Runs
// twice, "on" (both receivers' default `prefold_enabled(true)`) and "off"
// (both receivers opened with `.prefold_enabled(false)`, `cache_a` itself
// left on since it is never the one instrumented) — a real off variant,
// since `CacheBuilder::prefold_enabled` reaches a receiving node's engine
// the same way regardless of who assembled the batch it applies.
// ---------------------------------------------------------------------

struct HotCounterReceiveRepMetrics {
    applies_b: u64,
    applies_c: u64,
    converge_secs: f64,
    frames: u64,
    bytes: u64,
    lost_updates: u64,
}

fn print_hot_counter_receive_bench(
    variant: &str,
    writers: u32,
    iters: u32,
    reps: &[HotCounterReceiveRepMetrics],
) {
    println!(
        "BENCH replicated_hot_counter_receive_{variant} writers={writers} iters={iters} reps={} \
         applies_b={} applies_c={} converge_secs={:.3} frames_sent_total={} \
         bytes_sent_total={} lost_updates={}",
        reps.len(),
        median_field_u64(reps, |m| m.applies_b),
        median_field_u64(reps, |m| m.applies_c),
        median_field_f64(reps, |m| m.converge_secs),
        median_field_u64(reps, |m| m.frames),
        median_field_u64(reps, |m| m.bytes),
        median_field_u64(reps, |m| m.lost_updates),
    );
}

async fn run_hot_counter_receive_rep(
    writers: u32,
    iters: u32,
    cache_name: &str,
    receivers_prefold_enabled: bool,
) -> HotCounterReceiveRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let key = 0u32;
    let clusters = peer_group(cache_name, 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = Box::pin(open_replicated_trio_with_prefold::<PnCounter>(
        &cluster_a,
        &cluster_b,
        &cluster_c,
        cache_name,
        Arc::new(PnCounterResolver),
        [true, receivers_prefold_enabled, receivers_prefold_enabled],
    ))
    .await;

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let (applies_task_b, applies_count_b) = spawn_event_counter(&cache_b);
    let (applies_task_c, applies_count_c) = spawn_event_counter(&cache_c);

    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let cache_a = cache_a.clone();
            let node = NodeId::from(u64::from(w));
            tokio::spawn(async move {
                for i in 1..=iters {
                    cache_a
                        .insert(key, PnCounter::local_delta(node, u64::from(i)))
                        .await
                        .expect("insert succeeds");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.expect("writer worker did not panic");
    }

    let expected = i64::from(writers) * i64::from(iters);
    let snapshot = cache_a.get(&key).await.map_or(0, |c| c.value());
    let lost_updates = u64::try_from((expected - snapshot).max(0)).unwrap_or(0);

    let convergence_started = Instant::now();
    common::eventually(Duration::from_secs(30), || async {
        cache_a.get(&key).await.map(|c| c.value()) == Some(expected)
            && cache_b.get(&key).await.map(|c| c.value()) == Some(expected)
            && cache_c.get(&key).await.map(|c| c.value()) == Some(expected)
    })
    .await;
    let converge_secs = convergence_started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;

    applies_task_b.abort();
    applies_task_c.abort();
    let applies_b = applies_count_b.load(Ordering::Relaxed);
    let applies_c = applies_count_c.load(Ordering::Relaxed);

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    HotCounterReceiveRepMetrics {
        applies_b,
        applies_c,
        converge_secs,
        frames,
        bytes,
        lost_updates,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn replicated_hot_counter_receive() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let writers = writers();
    let iters = iters();
    let reps = repetitions();
    let cache_name = "crdt-hot-counter-receive";

    for (variant, receivers_prefold_enabled) in [("on", true), ("off", false)] {
        let mut rep_metrics = Vec::with_capacity(reps as usize);
        for _ in 0..reps {
            rep_metrics.push(
                Box::pin(run_hot_counter_receive_rep(
                    writers,
                    iters,
                    cache_name,
                    receivers_prefold_enabled,
                ))
                .await,
            );
        }
        print_hot_counter_receive_bench(variant, writers, iters, &rep_metrics);
    }
}

// ---------------------------------------------------------------------
// Scenario 11: merged_counter_coalesced — Lever B, local delta coalescing
// (`Cache::merge`, `CacheBuilder::merge_coalesce_window`). The same
// eight-writer single-counter shape as scenarios 3/4, `Cache::merge`
// replacing `insert`, across three coalescing windows: 0 (immediate apply,
// `Cache::merge`'s own equivalent of `insert` under a merging resolver),
// 1ms, and 10ms. "Engine applies" is `cache_a`'s own [`spawn_event_counter`]
// total — the number of times the coalesced folds actually reached the
// engine, expected to fall well below `writers * iters` (the number of
// client-side `merge` calls) as the window widens. `lost_updates` is a
// snapshot of `cache_a` taken immediately after the writers finish and
// before the convergence wait, the same transient post-write,
// pre-convergence gap scenario 8's own `lost_updates` measures — for a
// nonzero window this also carries the write side's own coalescing delay
// (a fold not yet flushed is invisible to `get`, `Cache::merge`'s own
// documented staleness bound), not only replication lag. `converge_secs`
// times from the last writer's last call to every one of the three nodes
// reading the exact expected total, so it likewise includes that staleness
// bound rather than only the network repair cost scenarios 3/4 isolate.
// ---------------------------------------------------------------------

/// The three `Cache::merge` coalescing windows this scenario compares, in
/// milliseconds.
const COALESCE_WINDOWS_MS: [u64; 3] = [0, 1, 10];

struct CoalescedRepMetrics {
    merges_per_sec: f64,
    p50_micros: f64,
    p99_micros: f64,
    engine_applies: u64,
    frames: u64,
    bytes: u64,
    converge_secs: f64,
    lost_updates: u64,
}

fn print_coalesced_bench(
    name: &str,
    writers: u32,
    iters: u32,
    window_ms: u64,
    reps: &[CoalescedRepMetrics],
) {
    println!(
        "BENCH {name} writers={writers} iters={iters} window_ms={window_ms} reps={} \
         merges_per_sec={:.1} p50_micros={:.1} p99_micros={:.1} engine_applies={} \
         frames_sent_total={} bytes_sent_total={} converge_secs={:.3} lost_updates={}",
        reps.len(),
        median_field_f64(reps, |m| m.merges_per_sec),
        median_field_f64(reps, |m| m.p50_micros),
        median_field_f64(reps, |m| m.p99_micros),
        median_field_u64(reps, |m| m.engine_applies),
        median_field_u64(reps, |m| m.frames),
        median_field_u64(reps, |m| m.bytes),
        median_field_f64(reps, |m| m.converge_secs),
        median_field_u64(reps, |m| m.lost_updates),
    );
}

async fn run_coalesced_rep(
    writers: u32,
    iters: u32,
    cache_name: &str,
    window: Duration,
) -> CoalescedRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let key = 0u32;
    let clusters = peer_group(cache_name, 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (a, b, c) = tokio::join!(
        cluster_a
            .cache::<u32, PnCounter>(cache_name)
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .merge_coalesce_window(window)
            .open(),
        cluster_b
            .cache::<u32, PnCounter>(cache_name)
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .merge_coalesce_window(window)
            .open(),
        cluster_c
            .cache::<u32, PnCounter>(cache_name)
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .merge_coalesce_window(window)
            .open(),
    );
    let cache_a = a.expect("a opens");
    let cache_b = b.expect("b opens");
    let cache_c = c.expect("c opens");

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let (applies_task, applies_count) = spawn_event_counter(&cache_a);

    let started = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let cache_a = cache_a.clone();
            let node = NodeId::from(u64::from(w));
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iters as usize);
                for i in 1..=iters {
                    let t0 = Instant::now();
                    cache_a
                        .merge(key, PnCounter::local_delta(node, u64::from(i)))
                        .await
                        .expect("merge succeeds");
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
    let snapshot = cache_a.get(&key).await.map_or(0, |c| c.value());
    let lost_updates = u64::try_from((expected - snapshot).max(0)).unwrap_or(0);

    let convergence_started = Instant::now();
    common::eventually(Duration::from_secs(30), || async {
        cache_a.get(&key).await.map(|c| c.value()) == Some(expected)
            && cache_b.get(&key).await.map(|c| c.value()) == Some(expected)
            && cache_c.get(&key).await.map(|c| c.value()) == Some(expected)
    })
    .await;
    let converge_secs = convergence_started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;

    applies_task.abort();
    let engine_applies = applies_count.load(Ordering::Relaxed);

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }

    CoalescedRepMetrics {
        merges_per_sec: f64::from(writers * iters) / elapsed.as_secs_f64(),
        p50_micros: percentile_of(&all_latencies, 50.0).as_secs_f64() * 1_000_000.0,
        p99_micros: percentile_of(&all_latencies, 99.0).as_secs_f64() * 1_000_000.0,
        engine_applies,
        frames,
        bytes,
        converge_secs,
        lost_updates,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn merged_counter_coalesced() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let writers = writers();
    let iters = iters();
    let reps = repetitions();

    for window_ms in COALESCE_WINDOWS_MS {
        let cache_name = format!("crdt-merged-coalesced-{window_ms}ms");
        let window = Duration::from_millis(window_ms);

        let mut rep_metrics = Vec::with_capacity(reps as usize);
        for _ in 0..reps {
            rep_metrics
                .push(Box::pin(run_coalesced_rep(writers, iters, &cache_name, window)).await);
        }

        print_coalesced_bench(
            "merged_counter_coalesced",
            writers,
            iters,
            window_ms,
            &rep_metrics,
        );
    }
}

// ---------------------------------------------------------------------
// Scenario 12: large_entity_convergence_coalesced — scenario 8's `merged`
// variant with `Cache::merge` and a 1ms coalescing window replacing a
// direct `insert` per write. Shares scenario 8's own `keys`
// (`SUNDOG_BENCH_KEYS`) knob and default, so a full benchmark run covers
// both entity counts the same way scenario 8's own doc run does — once at
// the default, once at a larger scale.
// ---------------------------------------------------------------------

/// The coalescing window `large_entity_convergence_coalesced` runs at.
const COALESCED_LARGE_ENTITY_WINDOW_MS: u64 = 1;

async fn run_large_entity_coalesced_rep(keys: u32) -> ScaleConvergeRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let cluster_label = "bench-crdt-large-entity-coalesced";
    let cache_name = "crdt-large-entity-coalesced";
    let window = Duration::from_millis(COALESCED_LARGE_ENTITY_WINDOW_MS);
    let clusters = peer_group(cluster_label, 3).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("peer_group(_, 3) returns exactly 3 clusters"));

    let (a, b, c) = tokio::join!(
        cluster_a
            .cache::<u32, PnCounter>(cache_name)
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .merge_coalesce_window(window)
            .open(),
        cluster_b
            .cache::<u32, PnCounter>(cache_name)
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .merge_coalesce_window(window)
            .open(),
        cluster_c
            .cache::<u32, PnCounter>(cache_name)
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .merge_coalesce_window(window)
            .open(),
    );
    let cache_a = a.expect("a opens");
    let cache_b = b.expect("b opens");
    let cache_c = c.expect("c opens");

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
                        .merge(i, PnCounter::local_delta(node, SCALE_INCREMENTS_PER_WRITER))
                        .await
                        .expect("merged merge succeeds");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.expect("writer task did not panic");
    }

    // Sampled immediately after the writers finish and before the
    // convergence wait below, mirroring scenario 8's own `lost_updates`:
    // for this scenario the gap also carries the coalescing window's own
    // staleness bound on top of replication lag, since a pending fold is
    // invisible to `get` until it flushes.
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
async fn large_entity_convergence_coalesced() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let keys = scale_keys(LARGE_ENTITY_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_large_entity_coalesced_rep(keys)).await);
    }

    print_scale_convergence_bench("coalesced", keys, &rep_metrics);
}

// ---------------------------------------------------------------------
// Scenario 13: sketch_path_convergence — a merging resolver's
// scale-hardening on the IBLT sketch path (`sundog/src/cluster/sketch.rs`,
// `sundog/src/cluster/anti_entropy.rs`). `ClusterConfig::ae_sketch_min_bucket`
// is lowered to [`SKETCH_PATH_MIN_BUCKET`], and every one of `keys` filler
// entries is forced into the *same* anti-entropy bucket
// ([`keys_in_one_bucket`], the same deterministic technique
// `tests/prometheus_exporter.rs`'s own `keys_in_one_bucket`/`bucket_of` pair
// forces a dense bucket with — copied locally, integration test binaries
// share nothing beyond `mod common`), so that one bucket clears the
// lowered threshold regardless of `store::BUCKET_COUNT`'s usual averaging.
// This sidesteps a write burst racing `cluster::fan_out_task`'s live
// replication: at any key count this benchmark's budget can afford, that
// live fan-out always finishes replicating before anti-entropy's own
// `ae_interval` next fires, which is why scenarios 8/12 report zero
// `ae_repaired_total` at their own defaults and why a bare write burst here
// would too. Once every node holds the full dense bucket,
// `Cache::invalidate_local` drops a quarter of it on `cache_b` only — an
// escape hatch that removes a local copy without a tombstone or fan-out,
// creating a real bucket digest mismatch with no write race involved —
// mirroring `tests/prometheus_exporter.rs`'s own `seed_sketch_mismatch`.
// The next anti-entropy round against a threshold this low answers with a
// sketch. `lww` (default `LwwResolver`, non-merging) and `pn_counter`
// (`PnCounterResolver`, merging) both run this, so `pn_counter`'s own
// `sketch_peeled`/`sketch_fallback` counts are the bidirectional-exchange
// sketch load `sketch.rs`'s `two_sided_version_mismatches_decode_at_the_default_shape`
// and `anti_entropy`'s
// `a_merging_bucket_above_the_threshold_converges_through_the_sketch_path`
// pin at unit scale, now measured through a real, multi-round anti-entropy
// loop instead. It reports `sundog_ae_sketch_total`'s existing `decoded`/
// `fallback` outcome counters — no new metric, since that counter already
// covers a peel success and a peel fallback exactly — as `sketch_peeled`
// and `sketch_fallback`, plus their sum as `sketch_rounds`: how many times
// this scenario's dense bucket answered a mismatch through the sketch
// mechanism at all, successfully or not, over however many anti-entropy
// rounds ran before it reconverged.
// ---------------------------------------------------------------------

/// `SUNDOG_BENCH_KEYS` default for `sketch_path_convergence`: the size of
/// the single dense anti-entropy bucket this scenario builds. Comfortably
/// under `cluster::sketch`'s rated 100-element symmetric difference once a
/// quarter of it is invalidated (50 elements: see
/// [`SKETCH_PATH_INVALIDATE_FRACTION`]), so the default run's sketch
/// mismatch decodes rather than falling back; `SUNDOG_BENCH_KEYS` raises
/// the dense bucket size (and so the invalidated fraction) past that rating
/// to see fallbacks appear.
const SKETCH_PATH_KEYS_DEFAULT: u32 = 200;

/// `ClusterConfig::ae_sketch_min_bucket` this scenario's clusters run at —
/// far under [`SKETCH_PATH_KEYS_DEFAULT`], so the one dense bucket this
/// scenario builds always clears it and takes the sketch path rather than
/// `ClusterConfig::default`'s 384-entry listing threshold, mirroring
/// `tests/sim.rs`'s own forced `ae_sketch_min_bucket` of 4 for the same
/// reason.
const SKETCH_PATH_MIN_BUCKET: usize = 4;

/// The fraction of the dense bucket [`sundog::Cache::invalidate_local`]
/// drops on `cache_b` to seed this scenario's mismatch: a quarter, so the
/// default `keys=200` run seeds a 50-element one-sided difference, half of
/// `cluster::sketch`'s rated 100-element capacity.
const SKETCH_PATH_INVALIDATE_FRACTION: usize = 4;

/// The anti-entropy bucket a `u32` key hashes into, mirroring
/// `store::stripe_index_from_hash`'s formula; `tests/prometheus_exporter.rs`
/// carries the identical helper for the same reason (copied locally —
/// integration test binaries share nothing beyond `mod common`).
fn sketch_path_bucket_of(key: u32) -> u16 {
    let bytes = postcard::to_stdvec(&key).expect("u32 key encodes");
    let bucket = xxh3_64(&bytes) & (sundog::store::BUCKET_COUNT as u64 - 1);
    u16::try_from(bucket).expect("invariant: masked to BUCKET_COUNT - 1, always fits in u16")
}

/// `count` keys guaranteed to land in the same anti-entropy bucket, so this
/// scenario's dense bucket size is exactly `count` regardless of
/// `store::BUCKET_COUNT`'s usual per-bucket averaging.
fn keys_in_one_bucket(count: usize) -> Vec<u32> {
    let target = sketch_path_bucket_of(0);
    (0..)
        .filter(|&k| sketch_path_bucket_of(k) == target)
        .take(count)
        .collect()
}

/// [`node_config`], but with `ae_sketch_min_bucket` lowered to `min_bucket`.
fn sketch_node_config(gossip_bind_addr: SocketAddr, min_bucket: usize) -> ClusterConfig {
    node_config(gossip_bind_addr).with(|c| {
        c.ae_sketch_min_bucket = min_bucket;
    })
}

/// [`peer_group`], but every node's config comes from [`sketch_node_config`]
/// instead of [`node_config`], so every anti-entropy round in this
/// scenario's cluster answers a mismatch with an IBLT sketch rather than a
/// listing once a bucket clears `min_bucket`.
async fn sketch_peer_group(cluster_name: &str, n: usize, min_bucket: usize) -> Vec<Cluster> {
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
            .config(sketch_node_config(addr, min_bucket))
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

struct SketchPathRepMetrics {
    converge_secs: f64,
    frames: u64,
    bytes: u64,
    ae_repaired: u64,
    sketch_peeled: u64,
    sketch_fallback: u64,
    lost_updates: u64,
}

fn print_sketch_path_bench(
    variant: &str,
    keys: u32,
    min_bucket: usize,
    reps: &[SketchPathRepMetrics],
) {
    println!(
        "BENCH sketch_path_convergence_{variant} keys={keys} ae_sketch_min_bucket={min_bucket} \
         reps={} converge_secs={:.3} frames_sent_total={} bytes_sent_total={} \
         lost_updates={}{}{}",
        reps.len(),
        median_field_f64(reps, |m| m.converge_secs),
        median_field_u64(reps, |m| m.frames),
        median_field_u64(reps, |m| m.bytes),
        median_field_u64(reps, |m| m.lost_updates),
        ae_repaired_field(median_field_u64(reps, |m| m.ae_repaired)),
        sketch_outcome_field(
            median_field_u64(reps, |m| m.sketch_peeled),
            median_field_u64(reps, |m| m.sketch_fallback),
        ),
    );
}

/// Waits for `cache_b`/`cache_c` to hold `dense_keys`' last entry (proof the
/// whole dense bucket landed everywhere), invalidates
/// [`SKETCH_PATH_INVALIDATE_FRACTION`] of it on `cache_b` alone, snapshots
/// this scenario's metrics, then times `cache_b`'s reconvergence — the
/// shape both variants below share once their own cache/resolver/dense
/// bucket is built.
async fn seed_and_time_sketch_mismatch(
    cache_name: &str,
    cache_b: &impl SketchProbe,
    cache_c: &impl SketchProbe,
    dense_keys: &[u32],
) -> SketchPathRepMetrics {
    let last_dense_key = *dense_keys.last().expect("keys is nonzero");
    common::eventually(Duration::from_secs(30), || async {
        cache_b.has(last_dense_key).await && cache_c.has(last_dense_key).await
    })
    .await;

    let invalidate_count = (dense_keys.len() / SKETCH_PATH_INVALIDATE_FRACTION).max(1);
    let invalidated = &dense_keys[..invalidate_count];
    for &key in invalidated {
        cache_b.drop_local(key).await;
    }

    let frames_before = sundog::net::frames_sent_total();
    let bytes_before = sundog::net::bytes_sent_total();
    let ae_repaired_before = ae_repaired_snapshot(cache_name);
    let sketch_peeled_before = sketch_outcome_snapshot(cache_name, "decoded");
    let sketch_fallback_before = sketch_outcome_snapshot(cache_name, "fallback");

    // A snapshot of how many of the invalidated keys are still missing on
    // `cache_b`, immediately after the invalidation above and before the
    // convergence wait below — every one of them, since `invalidate_local`
    // just dropped their local copies; mirrors every other scenario's own
    // `lost_updates`, the transient post-write, pre-convergence gap.
    let mut lost_updates = 0u64;
    for &key in invalidated {
        if !cache_b.has(key).await {
            lost_updates += 1;
        }
    }

    let convergence_started = Instant::now();
    common::eventually(Duration::from_secs(60), || async {
        for &key in invalidated {
            if !cache_b.has(key).await {
                return false;
            }
        }
        true
    })
    .await;
    let converge_secs = convergence_started.elapsed().as_secs_f64();

    let frames = sundog::net::frames_sent_total() - frames_before;
    let bytes = sundog::net::bytes_sent_total() - bytes_before;
    let ae_repaired = ae_repaired_snapshot(cache_name).saturating_sub(ae_repaired_before);
    let sketch_peeled =
        sketch_outcome_snapshot(cache_name, "decoded").saturating_sub(sketch_peeled_before);
    let sketch_fallback =
        sketch_outcome_snapshot(cache_name, "fallback").saturating_sub(sketch_fallback_before);

    SketchPathRepMetrics {
        converge_secs,
        frames,
        bytes,
        ae_repaired,
        sketch_peeled,
        sketch_fallback,
        lost_updates,
    }
}

/// [`seed_and_time_sketch_mismatch`]'s seam over the two variants' distinct
/// cache value types: `has` a key, and `drop_local` it the way
/// `Cache::invalidate_local` does.
trait SketchProbe {
    fn has(&self, key: u32) -> impl Future<Output = bool> + Send;
    fn drop_local(&self, key: u32) -> impl Future<Output = ()> + Send;
}

impl SketchProbe for sundog::Cache<u32, u64> {
    async fn has(&self, key: u32) -> bool {
        self.get(&key).await.is_some()
    }

    async fn drop_local(&self, key: u32) {
        self.invalidate_local(&key).await;
    }
}

impl SketchProbe for sundog::Cache<u32, PnCounter> {
    async fn has(&self, key: u32) -> bool {
        self.get(&key).await.is_some()
    }

    async fn drop_local(&self, key: u32) {
        self.invalidate_local(&key).await;
    }
}

async fn run_sketch_path_lww_rep(keys: u32, min_bucket: usize) -> SketchPathRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let cluster_label = "bench-crdt-sketch-path-lww";
    let cache_name = "crdt-sketch-path-lww";
    let clusters = sketch_peer_group(cluster_label, 3, min_bucket).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("sketch_peer_group(_, 3, _) returns exactly 3 clusters"));

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

    let dense_keys = keys_in_one_bucket(keys as usize);
    cache_a
        .insert_many(dense_keys.iter().map(|&k| (k, 1u64)))
        .await
        .expect("dense bulk insert succeeds");

    let metrics = seed_and_time_sketch_mismatch(cache_name, &cache_b, &cache_c, &dense_keys).await;

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }
    metrics
}

async fn run_sketch_path_pn_counter_rep(keys: u32, min_bucket: usize) -> SketchPathRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let cluster_label = "bench-crdt-sketch-path-pn-counter";
    let cache_name = "crdt-sketch-path-pn-counter";
    let clusters = sketch_peer_group(cluster_label, 3, min_bucket).await;
    let [cluster_a, cluster_b, cluster_c] = <[Cluster; 3]>::try_from(clusters)
        .unwrap_or_else(|_| panic!("sketch_peer_group(_, 3, _) returns exactly 3 clusters"));

    let (cache_a, cache_b, cache_c) = Box::pin(open_replicated_trio::<PnCounter>(
        &cluster_a,
        &cluster_b,
        &cluster_c,
        cache_name,
        Arc::new(PnCounterResolver),
    ))
    .await;

    let dense_keys = keys_in_one_bucket(keys as usize);
    let node = NodeId::from(0u64);
    cache_a
        .insert_many(
            dense_keys
                .iter()
                .map(|&k| (k, PnCounter::local_delta(node, 1))),
        )
        .await
        .expect("dense bulk insert succeeds");

    let metrics = seed_and_time_sketch_mismatch(cache_name, &cache_b, &cache_c, &dense_keys).await;

    for cluster in [cluster_a, cluster_b, cluster_c] {
        cluster.shutdown().await;
    }
    metrics
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sketch_path_convergence_lww() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let keys = scale_keys(SKETCH_PATH_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_sketch_path_lww_rep(keys, SKETCH_PATH_MIN_BUCKET)).await);
    }

    print_sketch_path_bench("lww", keys, SKETCH_PATH_MIN_BUCKET, &rep_metrics);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sketch_path_convergence_pn_counter() {
    if !bench_enabled() {
        eprintln!("skipping: SUNDOG_BENCH=1 not set");
        return;
    }

    let keys = scale_keys(SKETCH_PATH_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics
            .push(Box::pin(run_sketch_path_pn_counter_rep(keys, SKETCH_PATH_MIN_BUCKET)).await);
    }

    print_sketch_path_bench("pn_counter", keys, SKETCH_PATH_MIN_BUCKET, &rep_metrics);
}
