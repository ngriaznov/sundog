//! Tests for conflict resolution through a cache: custom, counter, set and default resolvers.

use super::*;
use crate::hlc::Hlc;
use crate::store::Event;
use crate::store::crdt::{OrSet, OrSetResolver, PnCounter, PnCounterResolver};
use crate::store::{ConflictResolver, RecordView, Winner, crdt};
use crate::wire::WireRecord;
use bytes::Bytes;

/// A resolver that keeps whichever record has the longer value, for
/// exercising [`crate::cache::CacheBuilder::resolver`] end to end.
#[derive(Debug, Clone, Copy)]
struct KeepsTheLongerValue;

impl ConflictResolver for KeepsTheLongerValue {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        let len_a = a.value.map_or(0, <[u8]>::len);
        let len_b = b.value.map_or(0, <[u8]>::len);
        if len_b > len_a { Winner::B } else { Winner::A }
    }
}

#[tokio::test]
async fn cache_opened_with_a_custom_resolver_keeps_the_longer_value_over_a_newer_shorter_remote_write()
 {
    let cluster = solo_cluster("cluster-it-custom-resolver").await;

    let cache = cluster
        .cache::<u32, Vec<u8>>("resolved")
        .mode(Mode::Local)
        .resolver(Arc::new(KeepsTheLongerValue))
        .open()
        .await
        .expect("open succeeds");
    cache.insert(1, vec![0u8; 10]).await.expect("insert");

    let name = SmolStr::new("resolved");
    let shard = cluster
        .shards()
        .read()
        .expect("shard registry lock is never poisoned")
        .get(&name)
        .cloned()
        .expect("shard is registered under the name it was opened with");

    let shorter_but_newer = WireRecord {
        key: Bytes::from(postcard::to_stdvec(&1u32).expect("u32 key encodes")),
        value: Some(Bytes::from(
            postcard::to_stdvec(&vec![0u8; 3]).expect("value encodes"),
        )),
        ver: Hlc {
            wall_ms: crate::store::now_ms() + 1_000,
            logical: 0,
            node: NodeId::from(9),
        },
        expires_at_ms: None,
    };
    ShardOps::apply_remote(shard.as_ref(), shorter_but_newer).await;

    assert_eq!(
        cache.get(&1).await,
        Some(vec![0u8; 10]),
        "the custom resolver keeps the longer value over a newer-but-shorter remote write"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_removed_key_stays_removed_under_a_resolver_that_prefers_a_longer_value() {
    let cluster = solo_cluster("cluster-it-custom-resolver-remove").await;
    let cache = cluster
        .cache::<u32, Vec<u8>>("resolved")
        .mode(Mode::Local)
        .resolver(Arc::new(KeepsTheLongerValue))
        .open()
        .await
        .expect("open succeeds");
    let name = SmolStr::new("resolved");
    let shard = cluster
        .shards()
        .read()
        .expect("shard registry lock is never poisoned")
        .get(&name)
        .cloned()
        .expect("shard is registered under the name it was opened with");
    let record = |value: Option<&Vec<u8>>, ver: Hlc| WireRecord {
        key: Bytes::from(postcard::to_stdvec(&1u32).expect("u32 key encodes")),
        value: value.map(|v| Bytes::from(postcard::to_stdvec(v).expect("value encodes"))),
        ver,
        expires_at_ms: None,
    };

    cache.insert(1, vec![0u8; 3]).await.expect("insert");
    cache.remove(&1).await.expect("remove");
    assert_eq!(cache.get(&1).await, None);

    // A longer value stamped before the removal arrives late: the resolver
    // prefers it, but the tombstone is newer, so the key stays deleted.
    let removed_at = crate::store::now_ms();
    ShardOps::apply_remote(
        shard.as_ref(),
        record(
            Some(&vec![0u8; 10]),
            Hlc {
                wall_ms: removed_at.saturating_sub(60_000),
                logical: 0,
                node: NodeId::from(9),
            },
        ),
    )
    .await;
    assert_eq!(
        cache.get(&1).await,
        None,
        "an older, longer value never resurrects a removed key"
    );

    // A remote tombstone newer than a longer live value deletes it.
    cache.insert(2, vec![0u8; 10]).await.expect("insert");
    let mut tombstone = record(
        None,
        Hlc {
            wall_ms: crate::store::now_ms() + 1_000,
            logical: 0,
            node: NodeId::from(9),
        },
    );
    tombstone.key = Bytes::from(postcard::to_stdvec(&2u32).expect("u32 key encodes"));
    ShardOps::apply_remote(shard.as_ref(), tombstone).await;
    assert_eq!(
        cache.get(&2).await,
        None,
        "a newer tombstone deletes a longer value"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn pn_counter_resolver_converges_a_three_node_cluster_to_the_exact_concurrent_total() {
    const INCREMENTS_PER_NODE: u64 = 5;

    let (cluster_a, cluster_b, cluster_c) = three_node_cluster("cluster-it-crdt-pncounter").await;

    let cache_a = cluster_a
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("b opens");
    let cache_c = cluster_c
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("c opens");

    let wid_a = crdt::WriterId::new(cluster_a.node_id(), cluster_a.local_incarnation());
    let wid_b = crdt::WriterId::new(cluster_b.node_id(), cluster_b.local_incarnation());
    let wid_c = crdt::WriterId::new(cluster_c.node_id(), cluster_c.local_incarnation());

    // Every node blindly increments the same key with no read
    // beforehand and no coordination between them, concurrently: each
    // writer runs as its own spawned task so it interleaves fairly with
    // every cluster's own gossip/anti-entropy/fan-out background tasks,
    // not just with the other two writers.
    let write_a = {
        let cache_a = cache_a.clone();
        async move {
            for cumulative in 1..=INCREMENTS_PER_NODE {
                cache_a
                    .insert(1, PnCounter::local_delta(wid_a, cumulative))
                    .await
                    .expect("a's blind increment applies");
            }
        }
    };
    let write_b = {
        let cache_b = cache_b.clone();
        async move {
            for cumulative in 1..=INCREMENTS_PER_NODE {
                cache_b
                    .insert(1, PnCounter::local_delta(wid_b, cumulative))
                    .await
                    .expect("b's blind increment applies");
            }
        }
    };
    let write_c = {
        let cache_c = cache_c.clone();
        async move {
            for cumulative in 1..=INCREMENTS_PER_NODE {
                cache_c
                    .insert(1, PnCounter::local_delta(wid_c, cumulative))
                    .await
                    .expect("c's blind increment applies");
            }
        }
    };
    let ta = tokio::spawn(write_a);
    let tb = tokio::spawn(write_b);
    let tc = tokio::spawn(write_c);
    ta.await.expect("a's writer task doesn't panic");
    tb.await.expect("b's writer task doesn't panic");
    tc.await.expect("c's writer task doesn't panic");

    let expected_total = i128::from(INCREMENTS_PER_NODE * 3);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let vals = [
            cache_a.get(&1).await.map(|c| c.value()),
            cache_b.get(&1).await.map(|c| c.value()),
            cache_c.get(&1).await.map(|c| c.value()),
        ];
        if vals.iter().all(|v| *v == Some(expected_total)) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "every node converges to the exact concurrent total with no lost \
             updates, last seen {vals:?} (expected {expected_total})"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

/// The Cache-level counterpart of `OrSet`'s own extensive merge/compact
/// unit tests: opens a real `Cache<String, OrSet<String>>` and drives
/// it through `insert`/`merge`/`get`, exercising the generic
/// `postcard::to_stdvec::<V>`/`from_bytes::<V>` path `Shard`/`Cache`
/// actually store and read values through, not `OrSet::encode`/
/// `decode` called directly, as every lower-level `or_set` test does.
/// `OrSet<T>` carries no `#[derive(Serialize, Deserialize)]`; its own
/// hand-rolled impls are what this path depends on.
#[tokio::test]
async fn or_set_cache_round_trips_through_the_generic_serde_path() {
    let cluster = solo_cluster("cluster-it-crdt-orset").await;
    let cache = cluster
        .cache::<String, OrSet<String>>("os")
        .mode(Mode::Replicated)
        .resolver(Arc::new(OrSetResolver::<String>::new()))
        .open()
        .await
        .expect("opens");

    let writer = crdt::WriterId::new(cluster.node_id(), cluster.local_incarnation());
    cache
        .insert("k".to_string(), OrSet::add(writer, 0, "x".to_string()))
        .await
        .expect("insert");
    let after_add = cache.get(&"k".to_string()).await.expect("present");
    assert!(after_add.contains(&"x".to_string()));

    let removed = OrSet::remove(&after_add, &"x".to_string());
    cache.merge("k".to_string(), removed).await.expect("merge");
    let after_remove = cache.get(&"k".to_string()).await.expect("still present");
    assert!(
        !after_remove.contains(&"x".to_string()),
        "the removed element must not survive a round trip through the real \
         storage/network encoding, not just OrSet::merge called directly"
    );

    cluster.shutdown().await;
}

/// Contrast with the merged test above: a plain get-then-insert
/// read-modify-write, the pattern [`PnCounterResolver`] exists to
/// replace, loses updates under the default [`LwwResolver`] even
/// though every node's write lands. The three reads below are staged
/// to happen before any of the three writes rather than raced against
/// each other, so the loss this demonstrates is deterministic, never
/// a matter of which write happens to land first.
#[tokio::test]
async fn get_then_insert_read_modify_write_loses_updates_under_the_default_lww_resolver() {
    let (cluster_a, cluster_b, cluster_c) = three_node_cluster("cluster-it-crdt-lww-rmw").await;

    // Default resolver: no `.resolver(...)` call, i.e. `LwwResolver`.
    let cache_a = cluster_a
        .cache::<u32, u64>("plain-counter")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, u64>("plain-counter")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");
    let cache_c = cluster_c
        .cache::<u32, u64>("plain-counter")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("c opens");

    cache_a.insert(1, 0).await.expect("baseline write");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if cache_a.get(&1).await == Some(0)
            && cache_b.get(&1).await == Some(0)
            && cache_c.get(&1).await == Some(0)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the baseline replicates to every node"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // All three nodes read the same pre-increment value...
    let read_a = cache_a.get(&1).await.expect("a reads the baseline");
    let read_b = cache_b.get(&1).await.expect("b reads the baseline");
    let read_c = cache_c.get(&1).await.expect("c reads the baseline");

    // ...then each writes back its own "+1", unaware of the others.
    cache_a
        .insert(1, read_a + 1)
        .await
        .expect("a writes its increment");
    cache_b
        .insert(1, read_b + 1)
        .await
        .expect("b writes its increment");
    cache_c
        .insert(1, read_c + 1)
        .await
        .expect("c writes its increment");

    // Three concurrent "+1"s from a shared base of 0 sum to 3 if none
    // are lost. Every write here carries the same value (1), so
    // whichever one LWW keeps, the converged total is 1 regardless of
    // arrival order: the other two increments are gone.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let vals = [
            cache_a.get(&1).await,
            cache_b.get(&1).await,
            cache_c.get(&1).await,
        ];
        if vals.iter().all(|v| *v == Some(1)) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "every node converges to the one surviving write, last seen {vals:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

#[tokio::test]
async fn a_remove_after_a_pending_merge_never_reaches_the_peer_as_a_value() {
    let (cluster_a, cluster_b) = two_node_cluster("cluster-it-pending-merge-remove").await;
    let window = Duration::from_millis(150);
    let cache_a = cluster_a
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .merge_coalesce_window(window)
        .open()
        .await
        .expect("a opens");
    let cache_b = cluster_b
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("b opens");
    wait_for_cache_advertised(&cluster_a, "counters").await;
    wait_for_cache_advertised(&cluster_b, "counters").await;
    let mut events_b = cache_b.events();
    let writer = cache_a.writer_id();

    cache_a
        .merge(1, PnCounter::local_delta(writer, 5))
        .await
        .expect("merge");
    cache_a.remove(&1).await.expect("remove");
    // A second key, merged and left to flush, proves the window passed and
    // replication ran while key 1 stayed gone.
    cache_a
        .merge(2, PnCounter::local_delta(writer, 7))
        .await
        .expect("merge a bystander");
    wait_until(
        Duration::from_secs(10),
        "the bystander's fold reaches the peer",
        async || cache_b.get(&2).await.is_some(),
    )
    .await;

    assert_eq!(cache_a.get(&1).await, None);
    assert_eq!(
        cache_b.get(&1).await,
        None,
        "the peer never held the removed key"
    );
    while let Ok(event) = events_b.try_recv() {
        assert!(
            !matches!(
                &event,
                Event::Created { key: 1, .. } | Event::Updated { key: 1, .. }
            ),
            "the peer never saw a value for the removed key: {event:?}"
        );
    }

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}
