//! Environment knobs, cluster builders and statistics helpers the scenarios share.

use crate::common;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use sundog::crdt::WriterId;
use sundog::{Cluster, ClusterConfig, ConflictResolver, Mode, NodeId};

pub(crate) fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Concurrent writers per scenario. `SUNDOG_BENCH_WRITERS`, default 8.
pub(crate) fn writers() -> u32 {
    env_u32("SUNDOG_BENCH_WRITERS", 8)
}

/// Writes per writer per scenario. `SUNDOG_BENCH_ITERS`, default 200.
pub(crate) fn iters() -> u32 {
    env_u32("SUNDOG_BENCH_ITERS", 200)
}

/// Independent repetitions per scenario before taking the median, so a
/// single noisy run doesn't skew a reported number. `SUNDOG_BENCH_REPS`,
/// default 3.
pub(crate) fn repetitions() -> u32 {
    env_u32("SUNDOG_BENCH_REPS", 3)
}

/// The entity count `N` scenarios 7 and 8 scale, `SUNDOG_BENCH_KEYS`
/// overriding `default`. Each scenario passes its own default, `20_000`
/// and `100_000` respectively, documented where each is defined.
pub(crate) fn scale_keys(default: u32) -> u32 {
    env_u32("SUNDOG_BENCH_KEYS", default)
}

// ---------------------------------------------------------------------
// Cluster helpers, copied locally from `replication_bench.rs` (integration
// test binaries share nothing beyond `mod common`).
// ---------------------------------------------------------------------

pub(crate) fn node_config(gossip_bind_addr: SocketAddr) -> ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip_bind_addr;
    })
}

/// Builds `n` real, loopback-`Static`-discovery clusters, each seeding every
/// node already built (gossip is bidirectional, so this converges to the
/// same full mesh mutual seeding would), and waits until every one reports
/// `n - 1` live peers.
pub(crate) async fn peer_group(cluster_name: &str, n: usize) -> Vec<Cluster> {
    let mut clusters: Vec<Cluster> = Vec::with_capacity(n);
    for i in 0..n {
        let seeds: Vec<SocketAddr> = clusters.iter().map(Cluster::local_gossip_addr).collect();
        let cluster = Cluster::builder(cluster_name)
            .seeds(seeds)
            .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
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
pub(crate) async fn local_cluster(name: &str) -> Cluster {
    Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("single-node loopback cluster builds")
}

/// A synthetic, fixed-incarnation writer identity for a benchmark's own
/// simulated writer index `id`. Every scenario before the churn scenario at
/// the end of this file assigns each concurrent writer a distinct `NodeId`
/// that is never a real cluster member and never restarts, so pairing it
/// with a fixed incarnation of `1` is exact, not an approximation: none of
/// those scenarios runs anywhere near the compaction sweep's own
/// 30-second-floor cadence (`sundog/src/cluster.rs`), so retirement never
/// engages and the incarnation value itself carries no meaning there.
pub(crate) fn synthetic_writer(id: u64) -> WriterId {
    WriterId::new(NodeId::from(id), 1)
}

/// Builds three loopback `Mode::Replicated` caches of the same name across
/// `cluster_a`/`cluster_b`/`cluster_c`, sharing one `resolver`, pre-fold on
/// (the default) for all three.
pub(crate) async fn open_replicated_trio<V>(
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
/// identical-batch, differently-toggled-cache comparison, there is no
/// local batch to build differently on the receive path, only the real
/// flag each receiving node opens with.
pub(crate) async fn open_replicated_trio_with_prefold<V>(
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
pub(crate) fn percentile_of(sorted: &[Duration], p: f64) -> Duration {
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
pub(crate) fn median_field_f64<T>(items: &[T], f: impl Fn(&T) -> f64) -> f64 {
    median_f64(items.iter().map(f).collect())
}

/// [`median_field_f64`]'s counterpart for exact integer counts.
pub(crate) fn median_field_u64<T>(items: &[T], f: impl Fn(&T) -> u64) -> u64 {
    let values: Vec<u64> = items.iter().map(f).collect();
    median_u64(&values)
}

// ---------------------------------------------------------------------
// Shared by scenarios 9-12: counts every `Event` a cache's public
// `Cache::events()` broadcast stream carries. One event publishes per
// non-no-op apply, a redelivered merge that reproduces exactly what's
// already stored publishes nothing, per `merge_version`'s own no-op arm, so
// this is the one public-API signal for how many times the engine
// applied, the "engine applies" field on
// every `BENCH` line below. Runs until the cache's sender side drops (the
// cache closes) or the caller aborts the returned handle; a lagged receiver
// (the counting task falling behind the publish rate) adds the lagged count
// rather than losing it, so a slow poll still reports the true total.
// ---------------------------------------------------------------------

pub(crate) fn spawn_event_counter<K, V>(
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
