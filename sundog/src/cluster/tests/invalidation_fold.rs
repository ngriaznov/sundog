//! Tests for an `Invalidation` cache with a merging resolver across nodes: a write ships the merged record, a holder folds it in, a non-holder ignores it, a removal still drops the copy, and a folded value expires with its deadline.

use super::*;
use crate::Cache;
use crate::cluster::test_support::wait_until;
use crate::store::Ttl;
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

/// The lifetime of the counters in the expiry test: long enough for a
/// debug-build cluster to deliver a record inside it, short enough to wait out.
const LIFETIME: Duration = Duration::from_secs(5);

/// [`open_counters`] with every value living for [`LIFETIME`].
async fn open_expiring_counters(cluster: &Cluster) -> Cache<u32, PnCounter> {
    cluster
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Invalidation)
        .ttl(LIFETIME)
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

/// Waits until `cluster` has seen a record from `peer` within the last
/// `window`, then leaves room for the apply that follows the sighting to
/// finish.
async fn record_from_reached_within(cluster: &Cluster, peer: &Cluster, window: Duration) {
    wait_until(
        Duration::from_secs(10),
        "the record reaches the node",
        async || {
            cluster
                .inner
                .inbound_activity
                .recent(peer.node_id(), window)
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// [`record_from_reached_within`] a window wider than any test here runs.
async fn record_from_reached(cluster: &Cluster, peer: &Cluster) {
    record_from_reached_within(cluster, peer, Duration::from_secs(10)).await;
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

#[tokio::test]
async fn a_folded_invalidation_counter_expires_on_its_holders_and_the_next_write_starts_afresh() {
    let (cluster_a, cluster_b, cluster_c) =
        three_node_cluster("cluster-it-invalidation-fold-expiry").await;
    let cache_a = open_expiring_counters(&cluster_a).await;
    let cache_b = open_expiring_counters(&cluster_b).await;
    let cache_c = open_expiring_counters(&cluster_c).await;

    // A holds the counter. B ignores A's record, so B's own write that
    // follows starts a separate contribution that A then folds in.
    cache_a
        .merge(1, delta(&cluster_a, 3))
        .await
        .expect("a merges");
    record_from_reached(&cluster_b, &cluster_a).await;
    assert_eq!(value(cache_b.get(&1).await), None, "B ignored A's record");
    cache_b
        .merge(1, delta(&cluster_b, 4))
        .await
        .expect("b merges");
    wait_until(
        Duration::from_secs(10),
        "a folds b's record into its copy",
        async || value(cache_a.get(&1).await) == Some(7),
    )
    .await;
    record_from_reached(&cluster_c, &cluster_b).await;
    assert_eq!(
        value(cache_b.get(&1).await),
        Some(4),
        "B keeps its own write"
    );
    assert_eq!(value(cache_c.get(&1).await), None, "C holds nothing");
    for (name, cache) in [("A", &cache_a), ("B", &cache_b)] {
        assert!(
            matches!(cache.ttl_of(&1), Some(Ttl::Remaining(_))),
            "{name} carries a deadline for the value it holds"
        );
    }

    tokio::time::sleep(LIFETIME + Duration::from_millis(500)).await;
    for (name, cache) in [("A", &cache_a), ("B", &cache_b), ("C", &cache_c)] {
        assert_eq!(value(cache.get(&1).await), None, "{name}'s copy expired");
        assert_eq!(
            cache.entry_count().await,
            0,
            "{name}'s sweep removed the entry"
        );
    }

    // B writes again. A's copy is gone, so A ignores the record rather
    // than folding the expired 7 into it; B starts a new value.
    cache_b
        .merge(1, delta(&cluster_b, 1))
        .await
        .expect("b merges again");
    record_from_reached_within(&cluster_a, &cluster_b, Duration::from_secs(3)).await;
    assert_eq!(
        value(cache_b.get(&1).await),
        Some(1),
        "the expired contributions do not fold into the new write"
    );
    assert_eq!(value(cache_a.get(&1).await), None, "A ignored the record");
    assert_eq!(cache_a.entry_count().await, 0, "A stored nothing");
    assert_eq!(value(cache_c.get(&1).await), None, "C holds nothing");

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}
