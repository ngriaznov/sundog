//! Closing a clone of a cache after its name was reopened leaves the reopened cache alone.

use super::*;

const NAME: &str = "stale";

async fn solo(cluster_name: &'static str) -> Cluster {
    Cluster::builder(cluster_name)
        .config(loopback_config())
        .seeds(std::iter::empty())
        .build()
        .await
        .expect("node builds")
}

async fn open(cluster: &Cluster) -> Cache<u32, String> {
    cluster
        .cache::<u32, String>(NAME)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("opens")
}

fn registered(cluster: &Cluster) -> Option<Arc<dyn crate::store::ShardOps>> {
    crate::cluster::test_support::registry_entry(cluster, &SmolStr::new(NAME))
}

fn lists(cluster: &Cluster) -> bool {
    cluster
        .health()
        .caches
        .iter()
        .any(|cache| cache.name == NAME && cache.mode == Mode::Replicated)
}

/// The reopened cache stays registered, listed in health, warm and
/// writable after the stale clone's close, and closing it unregisters it.
#[tokio::test]
async fn closing_a_stale_clone_after_reopen_leaves_the_reopened_cache_registered() {
    let cluster = solo("cache-it-stale-close").await;
    let c1 = open(&cluster).await;
    let c2 = c1.clone();
    c1.close().await;
    assert!(!lists(&cluster), "the first close frees the name");
    let c3 = open(&cluster).await;
    c3.insert(1, "v".to_string()).await.expect("insert");

    c2.close().await;

    assert!(
        lists(&cluster),
        "the reopened cache is still listed in health"
    );
    let shard = registered(&cluster).expect("the reopened cache is still registered");
    assert!(
        std::ptr::addr_eq(Arc::as_ptr(&shard), Arc::as_ptr(&c3.shard)),
        "the registry holds the reopened cache's shard"
    );
    assert!(cluster.is_warm(&SmolStr::new(NAME)), "its warmth stays");
    c3.insert(2, "w".to_string()).await.expect("insert");
    assert_eq!(c3.get(&1).await, Some("v".to_string()));
    assert_eq!(c3.get(&2).await, Some("w".to_string()));

    c3.close().await;
    assert!(!lists(&cluster), "closing the reopened cache unlists it");
    assert!(registered(&cluster).is_none());
    cluster.shutdown().await;
}

/// A repeat close with nothing reopened has no registration left to touch.
#[tokio::test]
async fn closing_a_clone_twice_without_a_reopen_is_idempotent() {
    let cluster = solo("cache-it-double-close").await;
    let c1 = open(&cluster).await;
    let c2 = c1.clone();
    c1.close().await;
    c2.close().await;
    assert!(registered(&cluster).is_none());
    assert!(!lists(&cluster));
    open(&cluster)
        .await
        .insert(1, "v".to_string())
        .await
        .expect("the name reopens");
    cluster.shutdown().await;
}

/// Two clones closing at once leave the name free, and reopenable, when both
/// return.
#[tokio::test]
async fn two_clones_closing_at_once_free_the_name() {
    let cluster = solo("cache-it-concurrent-close").await;
    let c1 = open(&cluster).await;
    let c2 = c1.clone();
    tokio::join!(c1.close(), c2.close());
    assert!(registered(&cluster).is_none());
    let reopened = open(&cluster).await;
    reopened.insert(1, "v".to_string()).await.expect("insert");
    assert!(lists(&cluster));
    cluster.shutdown().await;
}

/// A close after the cluster shut down has nothing registered to remove.
#[tokio::test]
async fn closing_a_cache_after_the_cluster_shut_down_is_a_no_op() {
    let cluster = solo("cache-it-close-after-shutdown").await;
    let cache = open(&cluster).await;
    let survivor = cache.clone();
    cluster.clone().shutdown().await;
    cache.close().await;
    survivor.close().await;
}

/// The peer's view of what `a` advertises for the cache.
fn advertises_mode(b: &Cluster, a: &Cluster) -> bool {
    b.advertised_cache_modes()
        .get(&a.node_id())
        .is_some_and(|modes| modes.get(NAME) == Some(&Mode::Replicated))
}

fn advertises_loader(b: &Cluster, a: &Cluster) -> bool {
    b.advertised_loaders()
        .get(&a.node_id())
        .is_some_and(|caches| caches.contains(NAME))
}

/// The stale close leaves the reopened cache's gossiped mode and loader up
/// across several gossip intervals; a delete it sent would reach the peer
/// within one or two.
#[tokio::test]
async fn closing_a_stale_clone_leaves_the_reopened_caches_gossip_up() {
    let a = solo("cache-it-stale-close-gossip").await;
    let b = Cluster::builder("cache-it-stale-close-gossip")
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let open_with_loader = || async {
        a.cache::<u32, String>(NAME)
            .mode(Mode::Replicated)
            .loader(|key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) })
            .open()
            .await
            .expect("opens")
    };
    let c1 = open_with_loader().await;
    wait_until(
        Duration::from_secs(10),
        "b sees a's mode and loader",
        async || advertises_mode(&b, &a) && advertises_loader(&b, &a),
    )
    .await;
    let c2 = c1.clone();
    c1.close().await;
    wait_until(
        Duration::from_secs(10),
        "b sees the first close clear both",
        async || !advertises_mode(&b, &a) && !advertises_loader(&b, &a),
    )
    .await;
    let c3 = open_with_loader().await;
    wait_until(
        Duration::from_secs(10),
        "b sees the reopened cache's mode and loader",
        async || advertises_mode(&b, &a) && advertises_loader(&b, &a),
    )
    .await;

    c2.close().await;

    for _ in 0..15 {
        assert!(
            advertises_mode(&b, &a) && advertises_loader(&b, &a),
            "the stale close cleared what the reopened cache advertises"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    c3.close().await;
    wait_until(
        Duration::from_secs(10),
        "closing the reopened cache clears both",
        async || !advertises_mode(&b, &a) && !advertises_loader(&b, &a),
    )
    .await;
    b.shutdown().await;
    a.shutdown().await;
}
