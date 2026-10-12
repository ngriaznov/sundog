//! The write-path scenarios: LWW, decomposed, merged and micro benchmarks.

use crate::common;
#[cfg(feature = "prometheus")]
use crate::scrape::metrics_handle;
use crate::scrape::{ae_repaired_field, ae_repaired_field_named, ae_repaired_snapshot};
use crate::support::{
    iters, local_cluster, median_field_f64, median_field_u64, open_replicated_trio, peer_group,
    percentile_of, repetitions, synthetic_writer, writers,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, ConflictResolver, Mode, RecordView, Winner};

/// How much larger the no-network micro-benchmark's op count is than
/// [`iters`]: those scenarios pay no network/AE cost per op, so a much
/// larger count still finishes in a fraction of a second.
const MICRO_MULTIPLIER: u32 = 100;

fn micro_ops() -> u32 {
    iters().saturating_mul(MICRO_MULTIPLIER)
}

// ---------------------------------------------------------------------
// A resolver used only by scenario 5b: `LwwResolver`'s exact comparison,
// with `needs_value_bytes()` forced to `true`. This does not isolate real
// byte-decode cost from merge-logic cost: `apply_locked`'s stored-side
// lookup clones a resident entry's already-encoded `Bytes` unconditionally,
// before any resolver runs, regardless of `needs_value_bytes()`, a cheap
// refcount bump, not a decode. `needs_value_bytes()` only gates whether
// those already-cloned bytes are exposed to the resolver's `RecordView`
// (an `Option::filter`), so forcing it to `true` here adds at most that
// filter's own cost, not real materialization work. Scenario 5 vs 5b is
// expected to show a near-zero delta given the current engine, that
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
// Scenario 1: naive_lww_counter, the lost-update problem, control, not a
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
    require_bench!();

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
// Scenario 2: decomposed_counter, today's best-practice workaround using
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
    require_bench!();

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
// Scenarios 3/4: merged_counter_blind / merged_counter_rmw, `WRITERS`
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
            let writer = synthetic_writer(u64::from(w));
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iters as usize);
                for i in 1..=iters {
                    let t0 = Instant::now();
                    if read_before_write {
                        let _ = cache_a.get(&key).await;
                    }
                    cache_a
                        .insert(key, PnCounter::local_delta(writer, u64::from(i)))
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

    let expected = i128::from(writers) * i128::from(iters);
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
    require_bench!();

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
    require_bench!();

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
// Scenario 5/5b: apply_ns_lww / apply_ns_merge / apply_ns_lww_forced_bytes,
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
    require_bench!();

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
    require_bench!();

    let ops = micro_ops();
    let reps = repetitions();
    let cache_name = "crdt-apply-merge";
    let writer = synthetic_writer(0);

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(
            run_micro_rep(
                "bench-crdt-apply-merge",
                cache_name,
                Arc::new(PnCounterResolver),
                ops,
                PnCounter::local_delta(writer, 0),
                |i| PnCounter::local_delta(writer, u64::from(i)),
            )
            .await,
        );
    }

    print_micro_bench("apply_ns_merge", ops, &rep_metrics);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_ns_lww_forced_bytes() {
    require_bench!();

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
// Scenario 6: resident_keys_at_rest, a fresh 3-node cluster, populated with
// the same decomposed (`WRITERS` keys) and merged (one `PnCounter` key,
// written sequentially from a single node) shapes scenarios 2 and 3
// exercise, then left to converge with no further writes. Not a
// measurement of scenarios 2/3's own post-benchmark state, a standalone
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

    let sole_writer = synthetic_writer(0);
    for cumulative in 1..=(u64::from(writers) * u64::from(iters)) {
        mrg_a
            .insert(0u32, PnCounter::local_delta(sole_writer, cumulative))
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
    require_bench!();

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
