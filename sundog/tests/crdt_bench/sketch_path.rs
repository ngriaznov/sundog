//! The sketch-path convergence scenarios.

use crate::common;
#[cfg(feature = "prometheus")]
use crate::scrape::metrics_handle;
use crate::scrape::{
    ae_repaired_field, ae_repaired_snapshot, sketch_outcome_field, sketch_outcome_snapshot,
};
use crate::support::{
    median_field_f64, median_field_u64, node_config, open_replicated_trio, repetitions, scale_keys,
    synthetic_writer,
};
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, ClusterConfig, Mode};
use xxhash_rust::xxh3::xxh3_64;

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
/// the `tests/sim/` suite's own forced `ae_sketch_min_bucket` of 4 for the same
/// reason.
const SKETCH_PATH_MIN_BUCKET: usize = 4;

/// The fraction of the dense bucket [`sundog::Cache::invalidate_local`]
/// drops on `cache_b` to seed this scenario's mismatch: a quarter, so the
/// default `keys=200` run seeds a 50-element one-sided difference, half of
/// `cluster::sketch`'s rated 100-element capacity.
const SKETCH_PATH_INVALIDATE_FRACTION: usize = 4;

/// The anti-entropy bucket a `u32` key hashes into, mirroring
/// `store::stripe_index_from_hash`'s formula; `tests/prometheus_exporter/`
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
/// the `tests/prometheus_exporter/` suite's own `keys_in_one_bucket`/`bucket_of`
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
/// the `tests/prometheus_exporter/` suite's own `seed_sketch_mismatch`. The next
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
