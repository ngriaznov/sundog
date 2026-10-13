//! Tests for an `Invalidation` cache with a merging resolver across nodes: a write ships the merged record, a holder folds it in, a non-holder ignores it, and a removal still drops the copy.

use super::*;
use crate::Cache;
use crate::cluster::test_support::wait_until;
use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};

/// Opens `counters` on `cluster` in `Mode::Invalidation` with
/// [`PnCounterResolver`].
async fn open_counters(cluster: &Cluster) -> Cache<u32, PnCounter> {
    cluster
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Invalidation)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("opens")
}

fn delta(cluster: &Cluster, by: u64) -> PnCounter {
    PnCounter::local_delta(
        WriterId::new(cluster.node_id(), cluster.local_incarnation()),
        by,
    )
}

fn value(counter: Option<PnCounter>) -> Option<i128> {
    counter.map(|c| c.value())
}

/// Waits until `cluster` has seen a record from `peer`, then leaves room for
/// the apply that follows the sighting to finish.
async fn record_from_reached(cluster: &Cluster, peer: &Cluster) {
    wait_until(
        Duration::from_secs(10),
        "the record reaches the node",
        async || {
            cluster
                .inner
                .inbound_activity
                .recent(peer.node_id(), Duration::from_secs(10))
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn a_merging_invalidation_write_folds_into_holders_and_is_ignored_elsewhere() {
    let (cluster_a, cluster_b, cluster_c) =
        three_node_cluster("cluster-it-invalidation-fold").await;
    let cache_a = open_counters(&cluster_a).await;
    let cache_b = open_counters(&cluster_b).await;
    let cache_c = open_counters(&cluster_c).await;

    // B holds the counter; A and C hold nothing, so B's record is
    // ignored at both.
    cache_b
        .merge(1, delta(&cluster_b, 3))
        .await
        .expect("b merges");
    record_from_reached(&cluster_a, &cluster_b).await;
    record_from_reached(&cluster_c, &cluster_b).await;
    assert_eq!(value(cache_a.get(&1).await), None, "A ignored B's record");
    assert_eq!(value(cache_c.get(&1).await), None, "C ignored B's record");

    // A writes the key it did not hold. B holds it, so B folds A's
    // record in rather than dropping its copy; C still holds nothing.
    cache_a
        .merge(1, delta(&cluster_a, 5))
        .await
        .expect("a merges");
    wait_until(
        Duration::from_secs(10),
        "b folds a's record into its copy",
        async || value(cache_b.get(&1).await) == Some(8),
    )
    .await;
    assert_eq!(
        value(cache_a.get(&1).await),
        Some(5),
        "A keeps its own write"
    );
    record_from_reached(&cluster_c, &cluster_a).await;
    assert_eq!(
        value(cache_c.get(&1).await),
        None,
        "C holds nothing for the key"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

#[tokio::test]
async fn a_removal_in_a_merging_invalidation_cache_still_drops_the_copy() {
    let (cluster_a, cluster_b, cluster_c) =
        three_node_cluster("cluster-it-invalidation-remove").await;
    let cache_a = open_counters(&cluster_a).await;
    let cache_b = open_counters(&cluster_b).await;
    let cache_c = open_counters(&cluster_c).await;
    cache_b
        .merge(1, delta(&cluster_b, 3))
        .await
        .expect("b merges");
    cache_c
        .merge(1, delta(&cluster_c, 4))
        .await
        .expect("c merges");
    tokio::time::sleep(Duration::from_millis(5)).await;

    cache_a.remove(&1).await.expect("a removes");

    wait_until(
        Duration::from_secs(10),
        "b and c drop their copies",
        async || cache_b.get(&1).await.is_none() && cache_c.get(&1).await.is_none(),
    )
    .await;

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}
