//! A `Mode::Distributed` read-through fill on a node that owns nothing of the key answers the caller and keeps no copy.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::nonowner_read_through::settled_trio_with;
use super::*;

/// Three `Mode::distributed()` nodes of `name`, only the first with a
/// loader, which holds `v{key}` for every key and counts its calls in the
/// returned counter. No owner of a key is a capable loader peer, so
/// `load_target` falls back to the first node. Returns once the first node's
/// view ranks all three, with a key it does not own.
async fn three_nodes_loading_on_the_first(
    name: &'static str,
) -> (
    (Cluster, Cache<u32, String>),
    Cluster,
    Cluster,
    u32,
    Arc<AtomicUsize>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node a builds");
    let counted = Arc::clone(&calls);
    let cache_a = a
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .loader(move |key: u32| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) }
        })
        .open()
        .await
        .expect("a opens");
    let seed = a.local_gossip_addr();
    let (b, _cache_b) = join_distributed(seed, name, name, 1).await;
    wait_for_peer_count(&a, 1).await;
    let (c, _cache_c) = join_distributed(seed, name, name, 2).await;
    wait_for_peer_count(&a, 2).await;
    let key = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(key) =
                (0..10_000u32).find(|&k| !cache_a.owners_of(&k).contains(&a.node_id()))
            {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a's view converges");
    ((a, cache_a), b, c, key, calls)
}

#[tokio::test]
async fn get_or_load_on_a_distributed_non_owner_keeps_no_copy() {
    let trio = settled_trio_with("nonowner-fill-get-or-load", loopback_config()).await;
    let (key, cache_a) = (trio.key, trio.non_owner());
    let loaded = cache_a
        .get_or_load(&key, async |_: &u32| {
            Ok::<_, SourceDown>("filled".to_string())
        })
        .await
        .expect("get_or_load");
    assert_eq!(loaded, "filled", "the caller gets the loaded value");
    assert_eq!(
        cache_a.get(&key).await,
        None,
        "a non-owner keeps no copy of a read-through fill"
    );
    assert!(!cache_a.contains_key(&key).await);
    assert!(cache_a.keys().is_empty(), "no key is held");
    trio.shut_down().await;
}

#[tokio::test]
async fn get_or_insert_with_on_a_distributed_non_owner_keeps_no_copy() {
    let trio = settled_trio_with("nonowner-fill-insert-with", loopback_config()).await;
    let (key, cache_a) = (trio.key, trio.non_owner());
    let made = cache_a
        .get_or_insert_with(&key, async |_: &u32| "made".to_string())
        .await
        .expect("get_or_insert_with");
    assert_eq!(made, "made");
    assert_eq!(cache_a.get(&key).await, None);
    trio.shut_down().await;
}

#[tokio::test]
async fn load_on_a_distributed_non_owner_with_no_capable_owner_keeps_no_copy() {
    let ((a, cache_a), b, c, key, calls) =
        three_nodes_loading_on_the_first("nonowner-fill-load").await;
    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}")),
        "the caller gets the loaded value"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the loader ran here");
    assert_eq!(
        cache_a.get(&key).await,
        None,
        "a non-owner keeps no copy of a load"
    );
    assert!(cache_a.keys().is_empty(), "no key is held");
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn load_many_on_a_distributed_non_owner_keeps_no_copy() {
    let ((a, cache_a), b, c, key, _calls) =
        three_nodes_loading_on_the_first("nonowner-fill-load-many").await;
    let other = (key + 1..10_000u32)
        .find(|k| !cache_a.owners_of(k).contains(&a.node_id()))
        .expect("a second key a does not own");
    let found = cache_a.load_many([key, other]).await.expect("load_many");
    assert_eq!(
        found,
        HashMap::from([(key, format!("v{key}")), (other, format!("v{other}"))])
    );
    assert_eq!(cache_a.get(&key).await, None);
    assert_eq!(cache_a.get(&other).await, None);
    assert!(cache_a.keys().is_empty(), "no key is held");
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_removed_key_is_not_served_from_a_distributed_non_owners_fill() {
    let trio = settled_trio_with(
        "nonowner-fill-stale",
        loopback_config().with(|c| c.tombstone_ttl = Duration::from_secs(300)),
    )
    .await;
    let (key, cache_a) = (trio.key, trio.non_owner());
    let owners = trio.owners();
    trio.hold_at_the_owners("owned").await;
    let ran = AtomicUsize::new(0);
    let served = cache_a
        .get_or_load(&key, async |_: &u32| {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok::<_, SourceDown>("filled".to_string())
        })
        .await
        .expect("get_or_load");
    assert_eq!(served, "owned", "an owner holds the key, so no loader runs");
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert_eq!(cache_a.get(&key).await, None);

    owners[0].remove(&key).await.expect("remove");
    wait_until(CONVERGE, "owners drop the key", async || {
        owners[0].get(&key).await.is_none() && owners[1].get(&key).await.is_none()
    })
    .await;
    assert_eq!(
        cache_a.get(&key).await,
        None,
        "a removed key is not served from a non-owner's stale copy"
    );
    let reloaded = cache_a
        .get_or_load(&key, async |_: &u32| {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok::<_, SourceDown>("reloaded".to_string())
        })
        .await
        .expect("get_or_load");
    assert_eq!(
        reloaded, "reloaded",
        "the owners dropped the key, so the loader runs"
    );
    assert_eq!(ran.load(Ordering::SeqCst), 1);
    assert_eq!(cache_a.get(&key).await, None);
    trio.shut_down().await;
}
