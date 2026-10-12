//! The coalescing-window scenarios.

use crate::common;
use crate::scale::{
    LARGE_ENTITY_KEYS_DEFAULT, SCALE_INCREMENTS_PER_WRITER, ScaleConvergeRepMetrics,
    merged_all_converged, print_scale_convergence_bench, scale_expected_total,
};
use crate::scrape::ae_repaired_snapshot;
#[cfg(feature = "prometheus")]
use crate::scrape::metrics_handle;
use crate::support::{
    iters, median_field_f64, median_field_u64, peer_group, percentile_of, repetitions, scale_keys,
    spawn_event_counter, synthetic_writer, writers,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, Mode};

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
