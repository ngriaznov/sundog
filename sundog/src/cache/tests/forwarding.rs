//! Tests of writes forwarded to owners, clock-skew bounds and ownership lookups.

use super::*;

/// `b` accepts any stamp; `a` bounds skew at one second. A record
/// stamped an hour ahead that `b` holds never reaches `a` through
/// anti-entropy, `a`'s clock never moves toward it, and `b`, whose
/// clock absorbed it, has its later writes refused by `a` too.
#[tokio::test]
async fn a_record_stamped_past_max_clock_skew_never_reaches_a_bounded_node() {
    let name = "cache-it-clock-skew";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config().with(|c| c.max_clock_skew = Some(Duration::from_secs(1))))
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config().with(|c| c.max_clock_skew = None))
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>("prices")
            .mode(Mode::Replicated)
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    cache_b.insert(2, "before".into()).await.expect("insert");
    wait_until(
        Duration::from_secs(10),
        "b's write stamped by its own clock reaches a",
        async || cache_a.get(&2).await.is_some(),
    )
    .await;

    let ahead = crate::hlc::Hlc {
        wall_ms: crate::store::now_ms() + 3_600_000,
        logical: 0,
        node: b.node_id(),
    };
    ShardOps::apply_remote(
        cache_b.shard.as_ref(),
        crate::wire::WireRecord {
            key: bytes::Bytes::from(postcard::to_stdvec(&1u32).expect("key encodes")),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&"ahead".to_string()).expect("value encodes"),
            )),
            ver: ahead,
            expires_at_ms: None,
        },
    )
    .await;
    assert_eq!(cache_b.get(&1).await.as_deref(), Some("ahead"));
    cache_b.insert(4, "after".into()).await.expect("insert");

    // Ten anti-entropy intervals: every round offers a both keys.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        cache_a.get(&1).await,
        None,
        "a refuses the record stamped an hour ahead"
    );
    assert_eq!(
        cache_a.get(&4).await,
        None,
        "b's clock absorbed the hour, so its next write is refused as well"
    );

    cache_a.insert(3, "local".into()).await.expect("insert");
    let key = bytes::Bytes::from(postcard::to_stdvec(&3u32).expect("key encodes"));
    let held = ShardOps::bucket_entries(cache_a.shard.as_ref(), crate::store::bucket_of(&key))
        .await
        .into_iter()
        .find(|entry| entry.key == key)
        .expect("a holds its own write");
    assert!(
        held.version.wall_ms < ahead.wall_ms - 3_000_000,
        "a's clock never took the refused stamp"
    );

    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn owners_of_matches_the_ownership_view_for_the_keys_bucket() {
    let ((a, cache_a), (b, _cache_b), (c, _cache_c), unowned_key) =
        three_node_distributed("cache-it-owners-of", "prices").await;

    let owners = cache_a.owners_of(&unowned_key);
    assert_eq!(owners.len(), 2, "owners = 2 for this cache");
    assert!(
        !owners.contains(&a.node_id()),
        "this key was chosen specifically because a does not own it"
    );
    // Every owner reported is a real, live node in this cluster.
    for owner in &owners {
        assert!(*owner == a.node_id() || *owner == b.node_id() || *owner == c.node_id());
    }

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

/// The end-to-end proof that the write forward, the fan-out group-by-
/// owner-set, and the fetch read path all compose: a write through a
/// node that does not own the key's bucket never becomes locally
/// visible on the writer, forwards to the real owners, and both the
/// writer and another non-owner read the value back correctly through
/// [`Cache::fetch`].
#[tokio::test]
async fn write_through_a_non_owner_composes_with_fetch_on_every_node() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key) =
        three_node_distributed("cache-it-write-through-non-owner", "prices").await;

    // `a` was chosen specifically because it does not own this key's
    // bucket: this insert never touches a's engine, only forwards.
    cache_a
        .insert(unowned_key, "through-a".to_string())
        .await
        .expect("insert forwards");
    assert_eq!(
        cache_a.get(&unowned_key).await,
        None,
        "a forwarded write never becomes locally visible on the writer"
    );

    // Every node, owner or not, reads the same value back through
    // fetch: the writer itself, and whichever of b/c is not a real
    // owner either.
    let owners = cache_a.owners_of(&unowned_key);
    for cache in [&cache_a, &cache_b, &cache_c] {
        wait_until(
            Duration::from_secs(10),
            "fetch converges to the forwarded value on every node",
            async || matches!(cache.fetch(&unowned_key).await, Ok(Some(value)) if value == "through-a"),
        )
        .await;
    }
    assert_eq!(owners.len(), 2);

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn forwarded_writes_reach_their_owners_when_the_writer_shuts_down_at_once() {
    // `a` forwards thousands of writes for buckets it does not own and
    // shuts down in the same breath: the fan-out task finishes the
    // batch, the mesh flushes it, and every key lands on the survivors,
    // which own everything between them once `a` is gone.
    let ((a, cache_a), (b, cache_b), (c, cache_c), _) =
        three_node_distributed("distributed-forward-flush", "forward-flush").await;
    let keys: Vec<u32> = (0..1_000_000u32)
        .filter(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .take(3_000)
        .collect();
    cache_a
        .insert_many(keys.iter().map(|&key| (key, format!("v{key}"))))
        .await
        .expect("insert_many forwards");
    cache_a.close().await;
    a.shutdown().await;

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut landed = 0usize;
            for key in &keys {
                let expected = format!("v{key}");
                if cache_b.get(key).await.as_deref() == Some(expected.as_str())
                    && cache_c.get(key).await.as_deref() == Some(expected.as_str())
                {
                    landed += 1;
                }
            }
            if landed == keys.len() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("every forwarded write lands on both survivors");

    cache_c.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
}

/// A `WireRecord` for `key`/`value` as `node` would write it now.
fn wire_record_from(key: u32, value: &str, node: NodeId) -> crate::wire::WireRecord {
    crate::wire::WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock past the epoch")
                    .as_millis(),
            )
            .expect("fits"),
            logical: 0,
            node,
        },
        expires_at_ms: None,
    }
}

/// A write fanned out under a stale ownership view reaches only the
/// owners that view names. The receiver's inbound loop notices the
/// batch's view hash is not its own and re-forwards the records to
/// their owners under its view, so the owner the writer missed gets
/// the record without waiting for an anti-entropy round to pair the
/// two; a batch already re-forwarded once travels no further.
#[tokio::test]
async fn a_forward_batch_under_a_stale_view_reaches_the_owner_the_writer_missed() {
    use crate::net::OutFrame;
    use crate::wire::{Msg, WireRecord};

    // Anti-entropy is effectively off: it would repair the missing
    // copy and hide whether the re-forward did.
    let name = "distributed-stale-forward";
    let mut config = loopback_config();
    config.ae_interval = Duration::from_secs(3600);
    config.tombstone_ttl = config.bucket_release_window();
    let ((a, cache_a), (b, cache_b), (c, cache_c)) =
        three_node_distributed_with(name, config).await;

    // Two keys whose owners are exactly `b` and `c`, under every node's
    // settled view.
    let mut keys = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let found: Vec<u32> = (0..100_000u32)
                .filter(|key| {
                    let owners = cache_a.owners_of(key);
                    owners.len() == 2
                        && !owners.contains(&a.node_id())
                        && cache_b.owners_of(key) == owners
                        && cache_c.owners_of(key) == owners
                })
                .take(2)
                .collect();
            if found.len() == 2 {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every view settles on b and c owning some bucket");
    let hop_one_key = keys.pop().expect("two keys");
    let key = keys.pop().expect("two keys");
    let cache_name = SmolStr::new(name);
    let stale_hash = cache_b
        .shard
        .ownership_view_hash()
        .expect("b is distributed")
        ^ 1;
    let record = |key: u32, value: &str| wire_record_from(key, value, a.node_id());
    let forward = |hops: u8, rec: WireRecord| {
        let frame = OutFrame::new(Msg::ForwardBatch {
            cache: cache_name.clone(),
            view_hash: stale_hash,
            hops,
            recs: vec![rec],
        })
        .expect("encodes");
        let mesh = a.mesh().clone();
        let to = b.node_id();
        async move {
            mesh.send_frames_awaiting(to, vec![frame]).await;
        }
    };

    // As if `a` still believed `b` and a departed node owned the
    // bucket: the batch reaches `b` alone, under a hash `b` does not
    // recognise, and `b` passes it on to `c`.
    forward(0, record(key, "stale-routed")).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_c.get(&key).await.as_deref() != Some("stale-routed") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b re-forwards the batch to c, the owner a's stale view missed");
    assert_eq!(cache_b.get(&key).await.as_deref(), Some("stale-routed"));

    // The same batch already one hop past its writer stops at `b`.
    forward(1, record(hop_one_key, "hop-one")).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_b.get(&hop_one_key).await.as_deref() != Some("hop-one") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b applies the hop-one batch");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        cache_c.get(&hop_one_key).await,
        None,
        "a batch at the hop cap is applied where it lands and re-forwarded no further"
    );

    cache_c.close().await;
    cache_b.close().await;
    cache_a.close().await;
    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}
