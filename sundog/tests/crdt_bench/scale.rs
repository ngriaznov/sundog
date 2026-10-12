//! The cold-join and large-entity convergence scenarios and their shared helpers.

use crate::common;
#[cfg(feature = "prometheus")]
use crate::scrape::metrics_handle;
use crate::scrape::{
    ae_repaired_field, ae_repaired_snapshot, entries_received_field, entries_received_snapshot,
};
use crate::support::{
    median_field_f64, median_field_u64, node_config, open_replicated_trio, peer_group, repetitions,
    scale_keys, synthetic_writer,
};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, Mode};

// ---------------------------------------------------------------------
// Scenarios 7-8 shared shape: `N` counters (`keys`, `SUNDOG_BENCH_KEYS`),
// `SCALE_WRITERS` writers, one per warm-cluster node, not the
// `SUNDOG_BENCH_WRITERS`-controlled count scenarios 1-4 use, each
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
pub(crate) const SCALE_INCREMENTS_PER_WRITER: u64 = 1;

/// Every counter's expected value once every writer's increment has
/// landed: `SCALE_WRITERS` writers times [`SCALE_INCREMENTS_PER_WRITER`]
/// each.
pub(crate) fn scale_expected_total() -> u64 {
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
/// writer's record lands, before the other writers' records, or, for the
/// merged variant's [`merged_all_converged`], before a remote merge, have
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
pub(crate) async fn merged_all_converged(
    caches: &[&sundog::Cache<u32, PnCounter>],
    keys: u32,
    expected: i128,
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

/// `SUNDOG_BENCH_KEYS` default for `cold_join_initial_replication`. At
/// `20_000` keys, the population writes are
/// cheap (one `insert_many` per writer), but the exact-total correctness
/// pass this scenario runs before and after the join (`SCALE_WRITERS`
/// real `.get()` round trips per counter, on top of `keys` themselves)
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

/// Scenario 7, `cold_join_initial_replication`: a warm 3-node trio
/// converges on `SCALE_WRITERS * keys` logical increments, then a fourth
/// node joins and pulls the resulting state. `Cache::open` on a
/// `Mode::Replicated` cache blocks until the full state-transfer snapshot
/// from the live donor with the lowest node id has landed and one
/// anti-entropy round against that donor has run, so timing from
/// immediately before the fourth node is built to immediately after its cache reports every
/// counter's exact total is exactly the cold-join replication cost this
/// scenario measures. Prints one `BENCH` line per `variant`: `decomposed`
/// (`3N` per-writer keys under `LwwResolver`) and `merged` (`N` keys under
/// `PnCounterResolver`), sharing topology, `fast_config()`, the 3-writer
/// count, and total logical increments between the two.
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
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
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
        let writer_id = synthetic_writer(u64::from(writer));
        let entries = (0..keys).map(|i| {
            (
                i,
                PnCounter::local_delta(writer_id, SCALE_INCREMENTS_PER_WRITER),
            )
        });
        cache
            .insert_many(entries)
            .await
            .expect("merged population insert_many succeeds");
    }

    let expected = i128::from(scale_expected_total());
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
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
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
    require_bench!();

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
    require_bench!();

    let keys = scale_keys(COLD_JOIN_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_cold_join_merged_rep(keys)).await);
    }

    print_cold_join_bench("merged", keys, &rep_metrics);
}

/// `SUNDOG_BENCH_KEYS` default for `large_entity_convergence`, lowered from
/// `100_000` for the same reason [`COLD_JOIN_KEYS_DEFAULT`] is:
/// this scenario's writers issue one real `.insert` per counter rather than
/// a bulk `insert_many`, and its convergence check reads every counter back
/// from all three nodes, so `100_000` would risk the 3-minute budget once
/// `repetitions()` reps and both variants are added up on a 4-core box.
/// `4_000` keeps the same shape observable, many entities, concurrent
/// writers, anti-entropy live, well inside budget; `SUNDOG_BENCH_KEYS`
/// overrides for a real capacity run.
pub(crate) const LARGE_ENTITY_KEYS_DEFAULT: u32 = 4_000;

pub(crate) struct ScaleConvergeRepMetrics {
    pub(crate) converge_secs: f64,
    pub(crate) frames: u64,
    pub(crate) bytes: u64,
    pub(crate) ae_repaired: u64,
    pub(crate) resident_keys: u64,
    pub(crate) lost_updates: u64,
}

/// Scenario 8, `large_entity_convergence`: `SCALE_WRITERS` writers, one per
/// warm-cluster node, each concurrently write every one of `keys` counters
/// (one `.insert` per counter, not a bulk `insert_many`, since the point is
/// genuinely concurrent cross-node writes racing anti-entropy rather than a
/// single burst) while `fast_config()`'s anti-entropy loop keeps running.
/// Convergence is timed from the last writer's last write. Also prints
/// scenario 12's `coalesced` variant, which reuses this same `variant`
/// line shape.
pub(crate) fn print_scale_convergence_bench(
    variant: &str,
    keys: u32,
    reps: &[ScaleConvergeRepMetrics],
) {
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
            let writer_id = synthetic_writer(u64::from(writer));
            tokio::spawn(async move {
                for i in 0..keys {
                    cache
                        .insert(
                            i,
                            PnCounter::local_delta(writer_id, SCALE_INCREMENTS_PER_WRITER),
                        )
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
async fn large_entity_convergence_decomposed() {
    require_bench!();

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
    require_bench!();

    let keys = scale_keys(LARGE_ENTITY_KEYS_DEFAULT);
    let reps = repetitions();

    let mut rep_metrics = Vec::with_capacity(reps as usize);
    for _ in 0..reps {
        rep_metrics.push(Box::pin(run_large_entity_merged_rep(keys)).await);
    }

    print_scale_convergence_bench("merged", keys, &rep_metrics);
}
