//! A graceful close or cluster shutdown sends the merge folds still pending in a coalesce window to the peers.

use super::clear_nonowner::three_nodes_the_first_coalescing;
use super::*;

/// How long a test waits for a peer to receive what a shutdown sends.
const DELIVERY: Duration = Duration::from_secs(30);

/// Two loopback nodes of `name` with a `PnCounter` cache open under `mode`,
/// the second seeded by the first. The first coalesces its merges for a
/// minute, so a fold stays pending there until a close or a shutdown flushes
/// it; the second applies at once.
async fn two_counter_nodes(
    name: &'static str,
    mode: Mode,
) -> [(Cluster, Cache<u32, PnCounter>); 2] {
    let mut nodes = Vec::new();
    for index in 0..2usize {
        let builder = Cluster::builder(name).config(loopback_config());
        let cluster = match nodes.first() {
            None => builder.seeds(std::iter::empty()),
            Some((seed, _)) => builder.seeds([Cluster::local_gossip_addr(seed)]),
        }
        .build()
        .await
        .expect("node builds");
        wait_for_peer_count(&cluster, index).await;
        let mut cache = cluster
            .cache::<u32, PnCounter>("counters")
            .mode(mode)
            .resolver(Arc::new(PnCounterResolver));
        if index == 0 {
            cache = cache.merge_coalesce_window(Duration::from_secs(60));
        }
        let cache = cache.open().await.expect("opens");
        nodes.push((cluster, cache));
    }
    let b = nodes.pop().expect("b");
    let a = nodes.pop().expect("a");
    [a, b]
}

/// What `cache` reads for `key`, from its own copy.
async fn counter(cache: &Cache<u32, PnCounter>, key: u32) -> Option<i128> {
    cache.get(&key).await.map(|c| c.value())
}

#[tokio::test]
async fn a_replicated_peer_receives_the_pending_merge_when_the_writer_shuts_down() {
    let [(cluster_a, cache_a), (cluster_b, cache_b)] =
        two_counter_nodes("cache-it-shutdown-merge-replicated", Mode::Replicated).await;
    let writer = cache_a.writer_id();
    cache_a
        .merge(1, PnCounter::local_delta(writer, 7))
        .await
        .expect("merge");
    assert_eq!(counter(&cache_b, 1).await, None, "still pending on a");

    cluster_a.shutdown().await;

    wait_until(
        DELIVERY,
        "the pending merge accepted by a reaches b when a shuts down",
        async || counter(&cache_b, 1).await == Some(7),
    )
    .await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn a_replicated_peer_receives_the_pending_merge_when_the_writer_closes() {
    let [(cluster_a, cache_a), (cluster_b, cache_b)] =
        two_counter_nodes("cache-it-close-merge-replicated", Mode::Replicated).await;
    let writer = cache_a.writer_id();
    cache_a
        .merge(1, PnCounter::local_delta(writer, 7))
        .await
        .expect("merge");
    assert_eq!(counter(&cache_b, 1).await, None, "still pending on a");

    cache_a.close().await;

    wait_until(DELIVERY, "close flushes the merge to b", async || {
        counter(&cache_b, 1).await == Some(7)
    })
    .await;
    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn a_replicated_peers_counter_includes_the_pending_merge_after_the_writer_shuts_down() {
    let [(cluster_a, cache_a), (cluster_b, cache_b)] =
        two_counter_nodes("cache-it-shutdown-merge-adds", Mode::Replicated).await;
    cache_b
        .merge(1, PnCounter::local_delta(cache_b.writer_id(), 3))
        .await
        .expect("b merges at once");
    wait_until(DELIVERY, "b's contribution reaches a", async || {
        counter(&cache_a, 1).await == Some(3)
    })
    .await;
    cache_a
        .merge(1, PnCounter::local_delta(cache_a.writer_id(), 7))
        .await
        .expect("a's merge waits in its window");
    assert_eq!(
        counter(&cache_a, 1).await,
        Some(3),
        "the pending fold is invisible to a read"
    );
    assert_eq!(counter(&cache_b, 1).await, Some(3));

    cluster_a.shutdown().await;

    wait_until(
        DELIVERY,
        "b's counter holds both contributions",
        async || counter(&cache_b, 1).await == Some(10),
    )
    .await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn an_invalidation_peer_folds_the_pending_merge_into_its_copy_when_the_writer_shuts_down() {
    let [(cluster_a, cache_a), (cluster_b, cache_b)] =
        two_counter_nodes("cache-it-shutdown-merge-invalidation", Mode::Invalidation).await;
    cache_b
        .merge(1, PnCounter::local_delta(cache_b.writer_id(), 3))
        .await
        .expect("b holds the key");
    cache_a
        .merge(1, PnCounter::local_delta(cache_a.writer_id(), 7))
        .await
        .expect("a's merge waits in its window");
    assert_eq!(
        counter(&cache_b, 1).await,
        Some(3),
        "b sees only its own contribution while a's fold is pending"
    );

    cluster_a.shutdown().await;

    wait_until(DELIVERY, "b's copy folds a's pending merge", async || {
        counter(&cache_b, 1).await == Some(10)
    })
    .await;
    cluster_b.shutdown().await;
}

#[tokio::test]
async fn a_distributed_non_owners_pending_merge_reaches_the_owners_when_it_shuts_down() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), key) =
        three_nodes_the_first_coalescing("cache-it-shutdown-merge-distributed").await;
    cache_b
        .merge(key, PnCounter::local_delta(cache_b.writer_id(), 5))
        .await
        .expect("an owner merges");
    wait_until(DELIVERY, "the owners hold the counter", async || {
        cache_b.fetch(&key).await.ok().flatten().map(|v| v.value()) == Some(5)
            && cache_c.fetch(&key).await.ok().flatten().map(|v| v.value()) == Some(5)
    })
    .await;
    cache_a
        .merge(key, PnCounter::local_delta(cache_a.writer_id(), 3))
        .await
        .expect("the non-owner's merge waits in its window");

    a.shutdown().await;

    wait_until(
        DELIVERY,
        "both owners hold the counter with the non-owner's fold",
        async || {
            cache_b.fetch(&key).await.ok().flatten().map(|v| v.value()) == Some(8)
                && cache_c.fetch(&key).await.ok().flatten().map(|v| v.value()) == Some(8)
        },
    )
    .await;
    b.shutdown().await;
    c.shutdown().await;
}
