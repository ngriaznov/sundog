//! The pre-fold and hot-counter receive scenarios.

use crate::common;
#[cfg(feature = "prometheus")]
use crate::scrape::metrics_handle;
use crate::support::{
    iters, local_cluster, median_field_f64, median_field_u64, open_replicated_trio_with_prefold,
    peer_group, repetitions, scale_keys, spawn_event_counter, synthetic_writer, writers,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, ConflictResolver, Mode};

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
