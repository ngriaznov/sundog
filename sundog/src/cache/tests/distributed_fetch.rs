//! Tests of the distributed read path: `fetch` against local, remote, cold and unavailable owners.

use super::*;

#[tokio::test]
async fn fetch_returns_local_value_without_a_network_request_when_this_node_owns_the_bucket() {
    let cluster = Cluster::builder("cache-it-fetch-local")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("a solo node opens warm, owning every bucket");
    cache.insert(1, "one".to_string()).await.expect("insert");

    assert_eq!(
        cache.fetch(&1).await.expect("fetch succeeds"),
        Some("one".to_string()),
        "a solo node owns every bucket, so fetch reads locally"
    );
    assert_eq!(
        cache.fetch(&2).await.expect("fetch succeeds"),
        None,
        "a genuine local miss is Ok(None), not an error"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn fetch_reaches_a_remote_owner_and_returns_its_value() {
    let ((a, cache_a), (b, _cache_b), (c, cache_c), unowned_key) =
        three_node_distributed("cache-it-fetch-remote", "prices").await;

    // Written through whichever of the two real owners it lands on;
    // `insert` on a non-owner forwards, so writing through `c` (an
    // owner or not) always reaches the true owners either way.
    cache_c
        .insert(unowned_key, "remote".to_string())
        .await
        .expect("insert");

    wait_until(
        Duration::from_secs(10),
        "fetch reaches a real owner and returns its value within the bound",
        async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "remote"),
    )
    .await;

    assert_eq!(
        cache_a.get(&unowned_key).await,
        None,
        "fetch never promotes the value into a's own local store"
    );

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn fetch_asks_the_other_owner_for_a_miss_in_a_cold_bucket_and_is_unavailable_while_every_owner_is_cold()
 {
    // Two nodes, two owners per bucket: both own every bucket. Once
    // both are warm, `a` drops its copy of a key and marks the bucket
    // cold again by hand, so a fetch on `a` has to ask `b`; then `b`
    // does the same, and `a`'s fetch is `FetchUnavailable`: no warm
    // copy vouches for the miss.
    // Anti-entropy is effectively off: it would repair a dropped copy
    // from the co-owner and race every step below.
    let name = "distributed-cold-fetch";
    let mut config = loopback_config();
    config.ae_interval = Duration::from_secs(3600);
    config.tombstone_ttl = config.bucket_release_window();
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let cache_a = a
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("a opens alone");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&b, 1).await;
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens");
    wait_for_peer_count(&a, 1).await;
    cache_b
        .insert(7, "seven".to_string())
        .await
        .expect("insert");
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_a.get(&7).await.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the write reaches its other owner");
    // Both warm-up pulls have landed: nothing re-delivers a copy the
    // steps below drop by hand.
    tokio::time::timeout(Duration::from_secs(20), async {
        while !(a.is_warm(&SmolStr::new(name)) && b.is_warm(&SmolStr::new(name))) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both nodes finish warming up");
    let bucket = PartId::of_key(&encode_key(&7u32).expect("u32 encodes"));
    let residency_a = cache_a.shard.residency().expect("a is distributed");
    let residency_b = cache_b.shard.residency().expect("b is distributed");
    // Both views list both nodes, or the fetch below ends `Stale`.
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_a.owners_of(&7).len() != 2 || cache_b.owners_of(&7).len() != 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both views settle on two owners");

    cache_a.invalidate_local(&7).await;
    residency_a.mark_cold(&[bucket]);
    assert_eq!(
        cache_a.fetch(&7).await.expect("fetch reaches b").as_deref(),
        Some("seven"),
        "a cold local miss is answered by the other owner"
    );
    assert_eq!(cache_a.get(&7).await, None, "fetch never promotes locally");

    cache_b.invalidate_local(&7).await;
    residency_b.mark_cold(&[bucket]);
    assert!(
        matches!(
            cache_a.fetch(&7).await,
            Err(CacheError::FetchUnavailable { .. })
        ),
        "every owner cold leaves no copy to vouch for a miss"
    );

    residency_a.clear_cold(&[bucket]);
    assert_eq!(
        cache_a.fetch(&7).await.expect("local"),
        None,
        "a warm bucket answers its own miss without asking anyone"
    );

    cache_b.close().await;
    cache_a.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// A part this node owns again after losing it holds whatever it kept
/// from before, which misses every write and delete made while another
/// node owned it. `a` holds such a stale copy of `key`, which `b`
/// deleted, and regains the part through the rebalance planner: while
/// the pull runs, `fetch` must answer from `b`, never the stale hit.
#[tokio::test]
async fn fetch_never_serves_a_stale_hit_from_a_regained_part_while_its_pull_runs() {
    let name = SmolStr::new("fetch-regained-stale");
    let (id_a, id_b) = (NodeId::from(0xa), NodeId::from(0xb));
    // The view a gave the part up under: two nodes since gone outranked
    // a for key's part.
    let without_a = OwnershipView::compute_at(
        id_a,
        vec![id_b, NodeId::from(0xc), NodeId::from(0xd)],
        std::num::NonZeroU8::new(2).expect("nonzero"),
        crate::ownership::Granularity::Part,
    );
    let key = (0..64u32)
        .find(|key| !without_a.owns(PartId::of_key(&encode_key(key).expect("u32 encodes"))))
        .expect("a owns about half the parts in a four-node view");
    let config = ClusterConfig {
        tombstone_ttl: Duration::from_secs(300),
        ..loopback_config()
    };

    let b = Cluster::builder("cache-it-fetch-regained")
        .node_id(id_b)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every part");
    cache_b
        .insert(key, "v1".to_string())
        .await
        .expect("b owns every part while alone");
    cache_b
        .remove(&key)
        .await
        .expect("b deletes the key while a does not own its part");

    let a = Cluster::builder("cache-it-fetch-regained")
        .node_id(id_a)
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("a builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&a, 1).await;
    a.advertise_cache_mode(&name, Mode::distributed());

    let shard = Shard::<u32, String>::new(
        name.clone(),
        Mode::distributed(),
        a.node_id(),
        u64::MAX,
        None,
        None,
    );
    let (shard, distributed) = attach_ownership(shard, &a, &name, Mode::distributed());
    let distributed = distributed.expect("Mode::distributed always attaches a context");
    let cache_a = Cache {
        shard: Arc::new(shard),
        cluster: a.clone(),
        cancel: CancellationToken::new(),
        tasks: TaskTracker::new(),
    };
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    let new = distributed.ownership.current();

    // What a kept from owning the part before: the write, not the delete.
    let shard_ops_a = Arc::clone(&cache_a.shard) as Arc<dyn ShardOps>;
    let stale = WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"v1".to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: b.node_id(),
        },
        expires_at_ms: None,
    };
    shard_ops_a.apply_remote_batch(vec![stale]).await;
    assert_eq!(cache_a.get(&key).await, Some("v1".to_string()));

    let held = shard_ops_a.held_parts().await;
    let plan = crate::cluster::rebalance::plan_view_change(
        &without_a,
        &without_a,
        &new,
        &held,
        &PartSet::new(),
    );
    assert!(plan.to_pull.contains(&part), "a owns key's part again");
    crate::cluster::rebalance::mark_pulling(&distributed.residency, &plan);

    assert_eq!(
        cache_a.fetch(&key).await.expect("b answers"),
        None,
        "a's copy of the regained part predates b's delete: only b's tombstone answers"
    );

    cache_b.close().await;
    cache_a.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// Pins the unverified-bucket guard at the `Cache::fetch` layer: `a`'s
/// shard is seeded with a stale replay of `key` that `b` deleted first.
/// `fetch` must ask `b` while the bucket is unverified,
/// then answer locally once reconciliation clears the marker.
#[cfg(feature = "spill")]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end two-node scenario (stage a stale replay, assert the unverified \
              guard, reconcile, assert it serves locally again): splitting it would only \
              scatter state (a, b, cache_a, cache_b, the shared key/bucket) across helper \
              signatures"
)]
async fn fetch_never_returns_a_local_hit_from_an_unverified_bucket_until_reconciliation_clears_it()
{
    let name = SmolStr::new("fetch-unverified-guard");
    let key = 7u32;
    let config = ClusterConfig {
        tombstone_ttl: Duration::from_secs(300),
        ..loopback_config()
    };

    let b = Cluster::builder("cache-it-fetch-unverified")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    cache_b
        .insert(key, "v1".to_string())
        .await
        .expect("b owns every bucket while alone");
    cache_b
        .remove(&key)
        .await
        .expect("b deletes the key: the co-owner's own delete during a's downtime");

    let a = Cluster::builder("cache-it-fetch-unverified")
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("a builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&a, 1).await;
    a.advertise_cache_mode(&name, Mode::distributed());

    // Built directly via `attach_ownership`, keeping the typed `Shard`
    // so a real `Cache` can wrap it.
    let shard = Shard::<u32, String>::new(
        name.clone(),
        Mode::distributed(),
        a.node_id(),
        u64::MAX,
        None,
        None,
    );
    let (shard, distributed) = attach_ownership(shard, &a, &name, Mode::distributed());
    let distributed = distributed.expect("Mode::distributed always attaches a context");
    let cache_a = Cache {
        shard: Arc::new(shard),
        cluster: a.clone(),
        cancel: CancellationToken::new(),
        tasks: TaskTracker::new(),
    };

    let warm_parts: Vec<PartId> = distributed.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and a eligible at owners=2, a owns every bucket"
    );
    let bucket = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    assert!(
        warm_parts.contains(&bucket),
        "with owners=2 and exactly two real nodes, a owns key's part too"
    );

    // Stands in for what a warm reopen's replay would install: a stale
    // live copy of `key`, via the same versioned-apply entry point replay uses.
    let shard_ops_a = Arc::clone(&cache_a.shard) as Arc<dyn ShardOps>;
    let stale = WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"stale".to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: a.node_id(),
        },
        expires_at_ms: None,
    };
    shard_ops_a.apply_remote_batch(vec![stale]).await;
    distributed.residency.mark_cold(&warm_parts);
    distributed.residency.mark_unverified(&warm_parts);

    assert_eq!(
        cache_a.fetch(&key).await.expect("b answers"),
        None,
        "the bucket is unverified: a's own stale hit is never trusted, so the fetch must \
         ask b, whose real tombstone is the correct answer"
    );
    assert_eq!(
        cache_a.get(&key).await,
        Some("stale".to_string()),
        "the stale record is still physically present locally; only the fetch path \
         refuses to trust it while unverified"
    );

    let mut reconciled: HashSet<PartId> = HashSet::new();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to a joining, so the round against it reconciles",
        async || {
            reconciled = reconcile_warm_buckets(
                &a,
                &shard_ops_a,
                &name,
                &distributed.ownership,
                &distributed.residency,
                &warm_parts,
            )
            .await;
            reconciled.len() == warm_parts.len()
        },
    )
    .await;
    assert!(
        !distributed.residency.is_unverified(bucket),
        "reconciliation clears the unverified mark alongside cold"
    );

    // b goes away entirely, so a's answer below must come from its own corrected state.
    cache_b.close().await;
    b.shutdown().await;

    assert_eq!(
        cache_a
            .fetch(&key)
            .await
            .expect("local, with no owner left to ask"),
        None,
        "reconciliation both corrected the stale copy and cleared unverified: the bucket \
         now serves its own tombstone locally, with no co-owner needed"
    );

    cache_a.close().await;
    a.shutdown().await;
}

#[tokio::test]
async fn fetch_retries_a_stale_owner_for_one_fetch_timeout_before_giving_up() {
    // Two solo nodes of one cluster with no seeds, so they never gossip,
    // `b` injected into `a`'s mesh by hand, so `b`'s view hash never
    // matches the one `a` sends: every
    // fetch to `b` is answered `Stale`. `a`'s own view lists `b` and a
    // phantom third node, so it has buckets it does not own to fetch.
    let name = "distributed-fetch-stale";
    let mut config = loopback_config();
    config.fetch_timeout = Duration::from_millis(300);
    let a = Cluster::builder("distributed-fetch-stale")
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("node a builds");
    let b = Cluster::builder("distributed-fetch-stale")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone");
    cache_b.insert(1, "one".to_string()).await.expect("insert");
    let peer_b = b.local_peer();
    a.mesh().update_peers(vec![peer_b.clone()]);

    let phantom = NodeId::from(u64::MAX);
    let owners = Mode::DEFAULT_OWNERS;
    let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
        a.node_id(),
        &[],
        &std::collections::HashMap::new(),
        &SmolStr::new(name),
        owners,
    );
    let view = Arc::new(crate::ownership::OwnershipView::compute(
        a.node_id(),
        vec![a.node_id(), peer_b.node, phantom],
        owners,
    ));
    tx.send(Arc::clone(&view)).expect("receiver alive");
    let shard = Shard::<u32, String>::new(
        SmolStr::new(name),
        Mode::distributed(),
        a.node_id(),
        u64::MAX,
        None,
        None,
    )
    .with_ownership(tracker, Arc::new(crate::ownership::ResidencySet::new()));
    let cache_a = Cache {
        shard: Arc::new(shard),
        cluster: a.clone(),
        cancel: CancellationToken::new(),
        tasks: TaskTracker::new(),
    };
    // A key `b` does not hold: a held record answers a fetch whatever
    // the views say, so only a miss exercises the stale retry.
    let key = (2..1_000_000u32)
        .find(|key| {
            let bucket = PartId::of_key(&encode_key(key).expect("u32 encodes"));
            !view.owns(bucket) && view.owners_of(bucket).contains(&peer_b.node)
        })
        .expect("a key owned by b and not by a");

    // A bucket `a` owns with only the phantom as co-owner: cold, its
    // one co-owner unreachable, a miss `a` cannot vouch for; warm, its
    // own miss is the answer.
    let cold_key = (2..1_000_000u32)
        .find(|key| {
            let bucket = PartId::of_key(&encode_key(key).expect("u32 encodes"));
            view.owns(bucket) && !view.owners_of(bucket).contains(&peer_b.node)
        })
        .expect("a key owned by a and the phantom");
    let cold_bucket = PartId::of_key(&encode_key(&cold_key).expect("u32 encodes"));
    let residency = cache_a.shard.residency().expect("a is distributed");
    residency.mark_cold(&[cold_bucket]);
    assert!(
        matches!(
            cache_a.fetch(&cold_key).await,
            Err(CacheError::FetchUnavailable { .. })
        ),
        "a cold owner with its only co-owner unreachable cannot vouch for a miss"
    );
    residency.clear_cold(&[cold_bucket]);
    assert_eq!(
        cache_a.fetch(&cold_key).await.expect("local"),
        None,
        "a warm owner answers its own miss"
    );

    let started = tokio::time::Instant::now();
    let result = cache_a.fetch(&key).await;
    let elapsed = started.elapsed();
    assert!(
        matches!(result, Err(CacheError::FetchUnavailable { .. })),
        "a permanently stale owner and an unreachable one leave nothing to answer: {result:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(300),
        "the stale owner is retried for a whole fetch_timeout: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the retries are bounded by the owners' windows: {elapsed:?}"
    );

    cache_b.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// `owner_answer`'s second local branch: a fetch that set out under a
/// view where `b` owns the key's part, answered `Stale` by `b`, finds
/// this node's view moved to one where it owns the part itself and
/// decides under that view by the same verdict as the first branch: a
/// warm part answers its own hit or miss, while a cold or stale one
/// asks the other owners, here none.
#[tokio::test]
async fn owner_answer_decides_locally_under_a_view_that_moved_during_the_fetch() {
    let name = "owner-answer-moved-view";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node a builds");
    let b = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node b builds");
    let cache_b = b
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone");
    let peer_b = b.local_peer();
    a.mesh().update_peers(vec![peer_b.clone()]);

    let owners = std::num::NonZeroU8::MIN;
    let old = Arc::new(crate::ownership::OwnershipView::compute(
        a.node_id(),
        vec![a.node_id(), peer_b.node],
        owners,
    ));
    let fresh = Arc::new(crate::ownership::OwnershipView::compute(
        a.node_id(),
        vec![a.node_id()],
        owners,
    ));
    let keys = (2..1_000_000u32)
        .filter(|key| !old.owns(PartId::of_key(&encode_key(key).expect("u32 encodes"))));
    let [missing, held, cold, stale] = keys.take(4).collect::<Vec<_>>()[..] else {
        panic!("b owns some parts of the old view");
    };

    let (cache_a, residency) = cache_under_view(&a, name, &fresh);
    let held_bytes = encode_key(&held).expect("u32 encodes");
    ShardOps::apply_remote_batch(
        cache_a.shard.as_ref(),
        vec![WireRecord {
            key: held_bytes,
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&"mine".to_string()).expect("string encodes"),
            )),
            ver: crate::hlc::Hlc {
                wall_ms: crate::store::now_ms(),
                logical: 0,
                node: a.node_id(),
            },
            expires_at_ms: None,
        }],
    )
    .await;
    let part_of = |key: u32| PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    residency.mark_cold(&[part_of(cold)]);
    residency.mark_stale(&[part_of(stale)]);

    let answer = |key: u32| {
        let cache_a = &cache_a;
        let old = Arc::clone(&old);
        async move {
            let mut attempts = 0;
            let answer = cache_a
                .owner_answer(
                    &key,
                    &encode_key(&key).expect("u32 encodes"),
                    old,
                    &tracing::Span::none(),
                    &mut attempts,
                )
                .await;
            (answer, attempts)
        }
    };
    let (got, attempts) = answer(missing).await;
    assert!(
        matches!(got, Ok(OwnerAnswer::Local(None))),
        "a warm miss answers"
    );
    assert_eq!(attempts, 1, "b, asked once, answered stale");
    let (got, _) = answer(held).await;
    assert!(
        matches!(got, Ok(OwnerAnswer::Local(Some(ref value))) if value == "mine"),
        "a warm hit answers"
    );
    for key in [cold, stale] {
        let (got, _) = answer(key).await;
        assert!(
            matches!(got, Err(CacheError::FetchUnavailable { .. })),
            "a cold or stale part asks the other owners, of which there are none"
        );
    }

    cache_b.close().await;
    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test]
async fn fetch_returns_fetch_unavailable_when_every_owner_is_down() {
    // `a`'s failure detector never drops `b` or `c` here. Once it drops
    // one while the other still runs, `a` co-owns the key's part with
    // the survivor and pulls the value from it, so a fetch after the
    // second crash reads `a`'s own copy.
    let a_config = ClusterConfig {
        phi_threshold: 1_000.0,
        ..loopback_config()
    };
    let ((a, cache_a), (b, cache_b), (c, _cache_c), unowned_key) =
        three_node_distributed_with_a("cache-it-fetch-unavailable", "prices", a_config).await;
    cache_b
        .insert(unowned_key, "value".to_string())
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "the value reaches a real owner first, proving the key is genuinely resident",
        async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "value"),
    )
    .await;

    // Both of the key's real owners crash; `a` never owned it and its
    // failure detector never drops them, so its view still names them
    // and every dial fails outright.
    b.crash().await;
    c.crash().await;

    let result = cache_a.fetch(&unowned_key).await;
    assert!(
        matches!(&result, Err(CacheError::FetchUnavailable { cache }) if cache == "prices"),
        "a fetch with every owner down answers FetchUnavailable, got {result:?}; \
         a holds a local copy: {}",
        cache_a.get_sync(&unowned_key).is_some()
    );

    a.shutdown().await;
}
