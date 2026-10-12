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
//! involved. Their defaults are set low (documented at each default's
//! definition) to keep every variant inside a 3-minute budget on a 4-core
//! box. A smoke run:
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
//! no peers, isolating per-apply CPU cost from all network/AE noise. No
//! key ever expires or is removed in any scenario, since TTL is
//! irrelevant here. Every numeric field is the median of at least
//! [`repetitions`] independent runs, so a single noisy run never skews a
//! reported number.
//!
//! Scenarios 9-12 isolate a merging resolver's two write-path levers
//! through the real `CacheBuilder::prefold_enabled` and
//! `merge_coalesce_window` toggles rather than approximating either: Lever
//! A (`Engine::apply_many`'s batch pre-fold, `sundog/src/store/engine.rs`)
//! in scenarios 9 and 10, Lever B (`Cache::merge`'s coalescing window) in
//! scenarios 11 and 12. All four report "engine applies" as
//! `Cache::events()`'s own count: one event per non-no-op apply, the only
//! public-API signal for how many times the engine applied, per
//! `Cache::merge`'s and the resolver contract's own docs.
//!
//! Scenarios 7-14 each carry their full rationale on their own `run_*` or
//! `print_*` function; this is only an index:
//!
//! - Scenario 7, `cold_join_initial_replication`: times a fourth node's
//!   join against an already-converged warm 3-node trio.
//! - Scenario 8, `large_entity_convergence`: times convergence of `N`
//!   concurrently written entities under live anti-entropy.
//! - Scenario 9, `apply_many_prefold`: Lever A on the local apply path,
//!   isolated from the network on a single-node cluster.
//! - Scenario 10, `replicated_hot_counter_receive`: Lever A on the
//!   replication receive path instead of the local apply path.
//! - Scenario 11, `merged_counter_coalesced`: Lever B across three
//!   coalescing window sizes.
//! - Scenario 12, `large_entity_convergence_coalesced`: scenario 8's
//!   `merged` variant with Lever B's coalescing window replacing `insert`.
//! - Scenario 13, `sketch_path_convergence`: forces a deliberate bucket
//!   mismatch onto the IBLT sketch path (`sundog_ae_sketch_total`).
//! - Scenario 14, `crdt_compaction_writer_churn`: churns one writer
//!   role's `NodeId` incarnation and times the compaction sweep's
//!   retirement of the dead incarnations.

#[path = "../common/mod.rs"]
mod common;

use crate::scale::{
    LARGE_ENTITY_KEYS_DEFAULT, SCALE_INCREMENTS_PER_WRITER, ScaleConvergeRepMetrics,
    merged_all_converged, print_scale_convergence_bench, scale_expected_total,
};
#[cfg(feature = "prometheus")]
use crate::scrape::metrics_handle;
use crate::scrape::{
    ae_repaired_field, ae_repaired_snapshot, crdt_compactions_field, crdt_compactions_snapshot,
    crdt_retired_writers_field, crdt_retired_writers_snapshot, sketch_outcome_field,
    sketch_outcome_snapshot,
};
use crate::support::{
    env_u32, iters, local_cluster, median_field_f64, median_field_u64, node_config,
    open_replicated_trio, open_replicated_trio_with_prefold, peer_group, percentile_of,
    repetitions, scale_keys, spawn_event_counter, synthetic_writer, writers,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, ClusterConfig, ConflictResolver, Mode, NodeId};
use xxhash_rust::xxh3::xxh3_64;

fn bench_enabled() -> bool {
    std::env::var("SUNDOG_BENCH").as_deref() == Ok("1")
}

/// Returns from the calling scenario fn unless `SUNDOG_BENCH=1` is set.
macro_rules! require_bench {
    () => {
        if !$crate::bench_enabled() {
            eprintln!("skipping: SUNDOG_BENCH=1 not set");
            return;
        }
    };
}

mod scale;
mod scrape;
mod support;
mod write_path;

/// `SUNDOG_BENCH_KEYS` default for `apply_many_prefold`'s batch size:
/// 1,000 records.
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

/// Scenario 9, `apply_many_prefold`: Lever A, batch pre-folding ahead of
/// the stripe lock (`Engine::apply_many`, `sundog/src/store/engine.rs`).
/// No network: a fresh single-node `Mode::Local` cluster per rep, matching
/// scenario 5's own isolation of apply cost from all network/AE noise. Two
/// batch shapes, both of `batch_size` (`SUNDOG_BENCH_KEYS`, default 1,000)
/// records: `one_key`, every record colliding on a single key (pre-fold's
/// best case, a run of `batch_size` puts folded to one survivor), and
/// `many_keys`, one record per distinct key (pre-fold's worst case:
/// nothing to fold, so its only cost is the batch's own by-key grouping
/// pass). Each shape runs under both `LwwResolver` (`merges() == false`,
/// pre-fold never engages regardless of the flag) and `PnCounterResolver`
/// (`merges() == true`): "on" and "off" open two caches that differ only
/// in `CacheBuilder::prefold_enabled`, both driven by the identical
/// `insert_many` call over the identical batch, so the only thing that
/// differs between the two timed passes is whether `Engine::apply_many`
/// folds a same-key run before applying it. The `many_keys` shape's on/off
/// pair lands close together for both resolvers: there is nothing to fold
/// either way, so it isolates pre-fold's idle grouping-pass overhead from
/// its `one_key` fold benefit.
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
/// fresh single-node caches under the same name in turn, "on"
/// (`CacheBuilder::prefold_enabled`'s default `true`) and "off"
/// (`.prefold_enabled(false)`), closing the first before opening the
/// second so both can use the same `cache_name`, and times the identical
/// `batch_size`-record `insert_many` call against each. `swap_order` runs
/// "off" before "on" instead of the reverse, the caller alternates it
/// across reps so whichever pass runs second on a freshly built cluster
/// (and so benefits from the first pass's allocator/JIT warm-up) is not
/// always the same one, canceling that bias out across reps rather than
/// always favoring "off".
///
/// Both passes take the identical single stripe-lock acquisition for the
/// whole batch (`insert_many` always does, pre-fold or not, see
/// `Engine::apply_many`) and pay the identical per-call fan-out push, so the
/// only thing that can differ between them is whether `apply_many`
/// folds a same-key run before applying it, a real measurement of the
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
    require_bench!();

    let batch_size = prefold_batch_size();
    let reps = repetitions();
    let writer = synthetic_writer(0);

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
                    move |i| PnCounter::local_delta(writer, u64::from(i) + 1),
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

struct HotCounterReceiveRepMetrics {
    applies_b: u64,
    applies_c: u64,
    converge_secs: f64,
    frames: u64,
    bytes: u64,
    lost_updates: u64,
}

/// Scenario 10, `replicated_hot_counter_receive`: Lever A on the
/// replication receive path: the same eight-writer single-`PnCounter`-key
/// shape as scenarios 3/4, but instrumented on the two nodes that only
/// ever *receive* the resulting writes rather than the one issuing them,
/// so a concurrent writer burst on `cache_a` arrives at `cache_b`/`cache_c`
/// as fan-out and anti-entropy batches carrying several records for the
/// same key, `apply_remote_batch`'s own batch shape, exactly what
/// pre-fold folds. Runs twice, "on" (both receivers' default
/// `prefold_enabled(true)`) and "off" (both receivers opened with
/// `.prefold_enabled(false)`, `cache_a` itself left on since it is never
/// the one instrumented): a real off variant, since
/// `CacheBuilder::prefold_enabled` reaches a receiving node's engine the
/// same way regardless of who assembled the batch it applies.
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
            let writer = synthetic_writer(u64::from(w));
            tokio::spawn(async move {
                for i in 1..=iters {
                    cache_a
                        .insert(key, PnCounter::local_delta(writer, u64::from(i)))
                        .await
                        .expect("insert succeeds");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.expect("writer worker did not panic");
    }

    let expected = i128::from(writers) * i128::from(iters);
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
    require_bench!();

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

/// Scenario 11, `merged_counter_coalesced`: Lever B, local delta
/// coalescing (`Cache::merge`, `CacheBuilder::merge_coalesce_window`). The
/// same eight-writer single-counter shape as scenarios 3/4, `Cache::merge`
/// replacing `insert`, across three coalescing windows: 0 (immediate
/// apply, `Cache::merge`'s own equivalent of `insert` under a merging
/// resolver), 1ms, and 10ms. "Engine applies" is `cache_a`'s own
/// [`spawn_event_counter`] total, the number of times the coalesced folds
/// reach the engine, expected to fall well below `writers *
/// iters` (the number of client-side `merge` calls) as the window widens.
/// `lost_updates` is a snapshot of `cache_a` taken immediately after the
/// writers finish and before the convergence wait, the same transient
/// post-write, pre-convergence gap scenario 8's own `lost_updates`
/// measures; for a nonzero window this also carries the write side's own
/// coalescing delay (a fold not yet flushed is invisible to `get`,
/// `Cache::merge`'s own documented staleness bound), not only replication
/// lag. `converge_secs` times from the last writer's last call to every
/// one of the three nodes reading the exact expected total, so it
/// likewise includes that staleness bound rather than only the network
/// repair cost scenarios 3/4 isolate.
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
            let writer = synthetic_writer(u64::from(w));
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iters as usize);
                for i in 1..=iters {
                    let t0 = Instant::now();
                    cache_a
                        .merge(key, PnCounter::local_delta(writer, u64::from(i)))
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

    let expected = i128::from(writers) * i128::from(iters);
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
    require_bench!();

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

/// The coalescing window `large_entity_convergence_coalesced` runs at.
const COALESCED_LARGE_ENTITY_WINDOW_MS: u64 = 1;

/// Scenario 12, `large_entity_convergence_coalesced`: scenario 8's
/// `merged` variant with `Cache::merge` and a
/// [`COALESCED_LARGE_ENTITY_WINDOW_MS`] coalescing window replacing a
/// direct `insert` per write. Shares scenario 8's own `keys`
/// (`SUNDOG_BENCH_KEYS`) knob and default.
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
            let writer_id = synthetic_writer(u64::from(writer));
            tokio::spawn(async move {
                for i in 0..keys {
                    cache
                        .merge(
                            i,
                            PnCounter::local_delta(writer_id, SCALE_INCREMENTS_PER_WRITER),
                        )
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

    let expected = i128::from(scale_expected_total());
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
    require_bench!();

    let keys = scale_keys(LARGE_ENTITY_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_large_entity_coalesced_rep(keys)).await);
    }

    print_scale_convergence_bench("coalesced", keys, &rep_metrics);
}

/// `SUNDOG_BENCH_KEYS` default for `sketch_path_convergence`: the size of
/// the single dense anti-entropy bucket this scenario builds. Comfortably
/// under `cluster::sketch`'s rated 100-element symmetric difference once a
/// quarter of it is invalidated (50 elements: see
/// [`SKETCH_PATH_INVALIDATE_FRACTION`]), so the default run's sketch
/// mismatch decodes rather than falling back; `SUNDOG_BENCH_KEYS` raises
/// the dense bucket size (and so the invalidated fraction) past that rating
/// to see fallbacks appear.
const SKETCH_PATH_KEYS_DEFAULT: u32 = 200;

/// `ClusterConfig::ae_sketch_min_bucket` this scenario's clusters run at,
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
/// carries the identical helper for the same reason (copied locally,
/// since integration test binaries share nothing beyond `mod common`).
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
    let mut clusters: Vec<Cluster> = Vec::with_capacity(n);
    for i in 0..n {
        let seeds: Vec<SocketAddr> = clusters.iter().map(Cluster::local_gossip_addr).collect();
        let cluster = Cluster::builder(cluster_name)
            .seeds(seeds)
            .config(sketch_node_config(
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                min_bucket,
            ))
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

/// Scenario 13, `sketch_path_convergence`: a merging resolver's
/// scale-hardening on the IBLT sketch path (`sundog/src/cluster/sketch.rs`,
/// `sundog/src/cluster/anti_entropy.rs`). `ClusterConfig::ae_sketch_min_bucket`
/// is lowered to [`SKETCH_PATH_MIN_BUCKET`], and every one of `keys`
/// filler entries is forced into the *same* anti-entropy bucket
/// ([`keys_in_one_bucket`], the same deterministic technique
/// `tests/prometheus_exporter.rs`'s own `keys_in_one_bucket`/`bucket_of`
/// pair uses to force a dense bucket, copied locally since integration
/// test binaries share nothing beyond `mod common`), so that one bucket
/// clears the lowered threshold regardless of `store::BUCKET_COUNT`'s
/// usual averaging. This sidesteps a write burst racing
/// `cluster::fan_out_task`'s live replication: at any key count this
/// benchmark's budget can afford, that live fan-out always finishes
/// replicating before anti-entropy's own `ae_interval` next fires, which
/// is why scenarios 8/12 report zero `ae_repaired_total` at their own
/// defaults and why a bare write burst here would too. Once every node
/// holds the full dense bucket, `Cache::invalidate_local` drops a quarter
/// of it on `cache_b` only, an escape hatch that removes a local copy
/// without a tombstone or fan-out, creating a real bucket digest mismatch
/// with no write race involved, mirroring
/// `tests/prometheus_exporter.rs`'s own `seed_sketch_mismatch`. The next
/// anti-entropy round against a threshold this low answers with a sketch.
/// `lww` (default `LwwResolver`, non-merging) and `pn_counter`
/// (`PnCounterResolver`, merging) both run this, so `pn_counter`'s own
/// `sketch_peeled`/`sketch_fallback` counts are the bidirectional-exchange
/// sketch load `sketch.rs`'s
/// `two_sided_version_mismatches_decode_at_the_default_shape` and
/// `anti_entropy`'s
/// `a_merging_bucket_above_the_threshold_converges_through_the_sketch_path`
/// pin at unit scale, now measured through a real, multi-round
/// anti-entropy loop instead. Reports `sundog_ae_sketch_total`'s existing
/// `decoded`/`fallback` outcome counters as `sketch_peeled` and
/// `sketch_fallback`, plus their sum as `sketch_rounds`: how many times
/// this scenario's dense bucket answers a mismatch through the sketch
/// mechanism at all, successfully or not, over however many anti-entropy
/// rounds run before it reconverges.
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
/// this scenario's metrics, then times `cache_b`'s reconvergence, the
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
    // convergence wait below, every one of them, since `invalidate_local`
    // dropped their local copies; mirrors every other scenario's own
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
    let writer = synthetic_writer(0);
    cache_a
        .insert_many(
            dense_keys
                .iter()
                .map(|&k| (k, PnCounter::local_delta(writer, 1))),
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
    require_bench!();

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
    require_bench!();

    let keys = scale_keys(SKETCH_PATH_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics
            .push(Box::pin(run_sketch_path_pn_counter_rep(keys, SKETCH_PATH_MIN_BUCKET)).await);
    }

    print_sketch_path_bench("pn_counter", keys, SKETCH_PATH_MIN_BUCKET, &rep_metrics);
}

/// `N` counters churned in [`crdt_compaction_writer_churn`].
/// `SUNDOG_BENCH_CHURN_COUNTERS` overrides.
const CHURN_COUNTERS_DEFAULT: u32 = 3;

fn churn_counters() -> u32 {
    env_u32("SUNDOG_BENCH_CHURN_COUNTERS", CHURN_COUNTERS_DEFAULT)
}

/// `K`, the number of times the churn scenario's single writer role is torn
/// down and rebuilt under the same persisted `NodeId`.
/// `SUNDOG_BENCH_CHURN_REPLACEMENTS` overrides.
const CHURN_REPLACEMENTS_DEFAULT: u32 = 3;

fn churn_replacements() -> u32 {
    env_u32(
        "SUNDOG_BENCH_CHURN_REPLACEMENTS",
        CHURN_REPLACEMENTS_DEFAULT,
    )
}

/// Each writer incarnation's one-shot contribution to every counter: fixed
/// so the fully-converged total is always
/// `churn_replacements() * CHURN_INCREMENT_PER_WRITER`, independent of
/// `churn_counters()`.
const CHURN_INCREMENT_PER_WRITER: u64 = 10;

/// `ClusterConfig::crdt_retire_after` for this scenario: short enough that
/// stage two's `2x` bound elapses within a couple of the compaction sweep's
/// own 30-second-floor ticks, long enough that one replacement round's
/// build-and-join (well under a second on loopback) never itself outlives
/// it and gets treated as a genuine absence.
const CHURN_RETIRE_AFTER: Duration = Duration::from_secs(2);

/// How long to wait, after the last replacement lands, for the compaction
/// sweep to run several times over: `crdt_compact_task`'s ticker fires
/// immediately on the observer's cache opening (before any replacement has
/// happened) and every `(CHURN_RETIRE_AFTER / 4).max(30s)` (30s here)
/// after that, so five ticks comfortably covers stage one (the first tick
/// after every dead incarnation is visible) and stage two (a later tick,
/// once `2 * CHURN_RETIRE_AFTER` has aged past a dead incarnation's own
/// retirement, folding it into the bounded scalar) with slack to spare.
fn churn_compaction_wait() -> Duration {
    Duration::from_secs(150)
}

/// [`merged_all_converged`]'s single-cache counterpart for the churn
/// scenario's one observer cache.
async fn churn_converged(
    cache: &sundog::Cache<u32, PnCounter>,
    counters: u32,
    expected: i128,
) -> bool {
    for i in 0..counters {
        if cache.get(&i).await.map(|c| c.value()) != Some(expected) {
            return false;
        }
    }
    true
}

struct ChurnRepMetrics {
    bytes_before_compaction: u64,
    bytes_after_compaction: u64,
    retired_writers_total: u64,
    compactions_total: u64,
}

fn print_churn_bench(name: &str, counters: u32, replacements: u32, metrics: &ChurnRepMetrics) {
    println!(
        "BENCH {name} counters={counters} replacements={replacements} reps=1 \
         record_bytes_before_compaction={} record_bytes_after_compaction={}{}{}",
        metrics.bytes_before_compaction,
        metrics.bytes_after_compaction,
        crdt_retired_writers_field(metrics.retired_writers_total),
        crdt_compactions_field(metrics.compactions_total),
    );
}

/// One churn round: builds a fresh writer incarnation under
/// `writer_node_id`, joined to `observer_addr`, writes
/// [`CHURN_INCREMENT_PER_WRITER`] to every one of `counters` counters, and
/// waits for the observer to see the cumulative total through this round.
/// Returns the built cluster so the caller decides whether to tear it down
/// (every round but the last) or keep it running as the still-live
/// "current" writer (the last round), see [`run_crdt_compaction_churn`].
async fn run_churn_writer_round(
    cluster_label: &str,
    cache_name: &str,
    observer_addr: SocketAddr,
    observer_cache: &sundog::Cache<u32, PnCounter>,
    writer_node_id: NodeId,
    round: u32,
    counters: u32,
) -> Cluster {
    let writer_cluster = Cluster::builder(cluster_label)
        .node_id(writer_node_id)
        .seeds([observer_addr])
        .config(
            node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).with(|c| {
                c.crdt_retire_after = CHURN_RETIRE_AFTER;
            }),
        )
        .build()
        .await
        .unwrap_or_else(|error| panic!("writer round {round} builds: {error}"));
    let writer_cache = writer_cluster
        .cache::<u32, PnCounter>(cache_name)
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .unwrap_or_else(|error| panic!("writer round {round} opens the churn cache: {error}"));
    let writer = writer_cache.writer_id();

    for i in 0..counters {
        writer_cache
            .insert(
                i,
                PnCounter::local_delta(writer, CHURN_INCREMENT_PER_WRITER),
            )
            .await
            .unwrap_or_else(|error| panic!("writer round {round} counter {i} inserts: {error}"));
    }

    let expected_so_far = i128::from(round + 1) * i128::from(CHURN_INCREMENT_PER_WRITER);
    common::eventually(Duration::from_secs(15), || async {
        churn_converged(observer_cache, counters, expected_so_far).await
    })
    .await;

    writer_cluster
}

/// Scenario 14, `crdt_compaction_writer_churn`: the two-stage writer
/// retirement rule. [`CHURN_COUNTERS`] `PnCounter` keys, one logical
/// writer role that is torn down and rebuilt [`CHURN_REPLACEMENTS`] times
/// under the same persisted `NodeId` (`ClusterBuilder::node_id`), joining
/// a stable observer node each time. Reusing the node id means every
/// rebuild mints a fresh incarnation (`Cache::writer_id`, pairing that
/// node id with `Cluster::local_incarnation`) and the *previous*
/// incarnation is dead the instant the new one is visible live
/// (`membership::incarnation_is_dead`), with no absence wait needed for
/// stage one. `ClusterConfig::crdt_retire_after` is set to
/// [`CHURN_RETIRE_AFTER`], a few seconds, so stage two's `2x` bound ages
/// comfortably within a couple of the compaction sweep's own
/// 30-second-floor ticks (`(crdt_retire_after / 4).max(30s)`,
/// `crdt_compact_task`, `sundog/src/cluster.rs`). That fixed floor, not
/// `CHURN_COUNTERS` or `CHURN_REPLACEMENTS`, dominates this scenario's
/// wall-clock cost, so unlike every scenario above it this one reports a
/// single run rather than a median over [`repetitions`].
///
/// Reports one counter's encoded record size (`PnCounter::encode().len()`),
/// still carrying every historical incarnation's own live slot right
/// after the last replacement since no sweep has run yet at that point,
/// against its size once the sweep has had time to retire the dead
/// incarnations (stage one) and fold them into the bounded scalar
/// accumulator (stage two), so the metadata bound is visible directly in
/// the two numbers on the printed `BENCH` line, plus the
/// retired-writer/compaction counters under `--features prometheus`. The
/// caller asserts the aggregate total is unaffected by however many
/// replacements or compaction passes ran, and that the record never ends
/// up larger after compaction than it was before.
async fn run_crdt_compaction_churn() -> ChurnRepMetrics {
    #[cfg(feature = "prometheus")]
    let _ = metrics_handle();

    let counters = churn_counters();
    let replacements = churn_replacements();
    let cluster_label = "bench-crdt-churn";
    let cache_name = "crdt-churn";

    let observer = Cluster::builder(cluster_label)
        .seeds(std::iter::empty())
        .config(
            node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).with(|c| {
                c.crdt_retire_after = CHURN_RETIRE_AFTER;
            }),
        )
        .build()
        .await
        .expect("observer node builds");
    let observer_addr = observer.local_gossip_addr();
    let observer_cache = observer
        .cache::<u32, PnCounter>(cache_name)
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("observer opens the churn cache");

    let retired_before = crdt_retired_writers_snapshot(cache_name);
    let compactions_before = crdt_compactions_snapshot(cache_name);

    // The one writer role, replaced `replacements` times under the same
    // persisted node id: every rebuild is a brand-new process as far as
    // `PnCounter`/`WriterId` are concerned, restarting its own contribution
    // from zero under its fresh incarnation.
    let writer_node_id = NodeId::random();
    let mut bytes_before_compaction = 0u64;
    // Holds the final round's cluster past the loop: every earlier
    // incarnation is torn down as soon as its round converges, so its slot
    // goes dead (superseded by the next round's fresh incarnation), but the
    // last one stays up as the still-live "current" writer through the
    // compaction wait below, matching real churn, only past writers
    // retire, never the one still running.
    let mut current_writer_cluster: Option<Cluster> = None;
    for round in 0..replacements {
        let writer_cluster = run_churn_writer_round(
            cluster_label,
            cache_name,
            observer_addr,
            &observer_cache,
            writer_node_id,
            round,
            counters,
        )
        .await;

        if round + 1 == replacements {
            bytes_before_compaction = observer_cache
                .get(&0)
                .await
                .expect("counter 0 has landed on the observer")
                .encode()
                .expect("PnCounter::encode never fails on a resident value")
                .len() as u64;
            current_writer_cluster = Some(writer_cluster);
        } else {
            writer_cluster.shutdown().await;
        }
    }

    let expected = i128::from(replacements) * i128::from(CHURN_INCREMENT_PER_WRITER);
    tokio::time::sleep(churn_compaction_wait()).await;

    common::eventually(Duration::from_secs(30), || async {
        churn_converged(&observer_cache, counters, expected).await
    })
    .await;

    let bytes_after_compaction = observer_cache
        .get(&0)
        .await
        .expect("counter 0 is still resident after compaction")
        .encode()
        .expect("PnCounter::encode never fails on a resident value")
        .len() as u64;
    assert!(
        bytes_after_compaction <= bytes_before_compaction,
        "compaction should never leave a record larger than it started: \
         before={bytes_before_compaction} after={bytes_after_compaction}"
    );

    let retired_writers_total =
        crdt_retired_writers_snapshot(cache_name).saturating_sub(retired_before);
    let compactions_total =
        crdt_compactions_snapshot(cache_name).saturating_sub(compactions_before);

    if let Some(current_writer_cluster) = current_writer_cluster {
        current_writer_cluster.shutdown().await;
    }
    observer.shutdown().await;

    ChurnRepMetrics {
        bytes_before_compaction,
        bytes_after_compaction,
        retired_writers_total,
        compactions_total,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crdt_compaction_writer_churn() {
    require_bench!();

    let counters = churn_counters();
    let replacements = churn_replacements();

    let metrics = Box::pin(run_crdt_compaction_churn()).await;

    print_churn_bench(
        "crdt_compaction_writer_churn",
        counters,
        replacements,
        &metrics,
    );
}
