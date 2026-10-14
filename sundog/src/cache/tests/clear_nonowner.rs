//! `Cache::clear` on a `Mode::Distributed` node applies a pending merge fold for a key the node does not own, and never deletes the owners' value.

use super::*;

/// Three `Mode::Distributed` nodes of `name` with a `PnCounter` cache. The
/// first coalesces its merges for a minute, so a fold stays pending there;
/// the others apply at once. Returns once the first node's view has a key it
/// does not own, with that key.
async fn three_nodes_the_first_coalescing(
    name: &'static str,
) -> (
    (Cluster, Cache<u32, PnCounter>),
    (Cluster, Cache<u32, PnCounter>),
    (Cluster, Cache<u32, PnCounter>),
    u32,
) {
    let mut nodes: Vec<(Cluster, Cache<u32, PnCounter>)> = Vec::new();
    for index in 0..3usize {
        let builder = Cluster::builder(name).config(loopback_config());
        let cluster = match nodes.first() {
            None => builder.seeds(std::iter::empty()),
            Some((seed, _)) => builder.seeds([seed.local_gossip_addr()]),
        }
        .build()
        .await
        .expect("node builds");
        wait_for_peer_count(&cluster, index).await;
        let mut cache = cluster
            .cache::<u32, PnCounter>(name)
            .mode(Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            })
            .resolver(Arc::new(PnCounterResolver));
        if index == 0 {
            cache = cache.merge_coalesce_window(Duration::from_secs(60));
        }
        let cache = tokio::time::timeout(Duration::from_secs(20), cache.open())
            .await
            .expect("open completes within the state-transfer budget")
            .expect("open succeeds");
        nodes.push((cluster, cache));
    }
    let key = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(key) =
                (0..10_000u32).find(|&k| !nodes[0].1.owners_of(&k).contains(&nodes[0].0.node_id()))
            {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the first node's view converges");
    let c = nodes.pop().expect("third node");
    let b = nodes.pop().expect("second node");
    let a = nodes.pop().expect("first node");
    (a, b, c, key)
}

#[tokio::test]
async fn clear_on_a_non_owner_forwards_its_pending_fold_and_deletes_nothing_at_the_owners() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), key) =
        three_nodes_the_first_coalescing("cache-it-clear-nonowner-pending").await;
    cache_b
        .merge(key, PnCounter::local_delta(cache_b.writer_id(), 5))
        .await
        .expect("an owner merges");
    wait_until(
        Duration::from_secs(30),
        "the owners hold the counter",
        async || {
            cache_b.fetch(&key).await.ok().flatten().map(|v| v.value()) == Some(5)
                && cache_c.fetch(&key).await.ok().flatten().map(|v| v.value()) == Some(5)
        },
    )
    .await;
    cache_a
        .merge(key, PnCounter::local_delta(cache_a.writer_id(), 3))
        .await
        .expect("the non-owner's merge waits in its window");

    cache_a.clear().await.expect("clear");

    wait_until(
        Duration::from_secs(30),
        "the owners hold the counter with the non-owner's fold, and no tombstone removed it",
        async || {
            cache_b.fetch(&key).await.ok().flatten().map(|v| v.value()) == Some(8)
                && cache_c.fetch(&key).await.ok().flatten().map(|v| v.value()) == Some(8)
        },
    )
    .await;
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}
