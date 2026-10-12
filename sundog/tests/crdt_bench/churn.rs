//! The CRDT compaction writer-churn scenario.

use crate::common;
#[cfg(feature = "prometheus")]
use crate::scrape::metrics_handle;
use crate::scrape::{
    crdt_compactions_field, crdt_compactions_snapshot, crdt_retired_writers_field,
    crdt_retired_writers_snapshot,
};
use crate::support::{env_u32, node_config};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cluster, Mode, NodeId};

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
