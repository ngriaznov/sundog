//! Tests for `PullRequest`, `warm_up_task` and the `rebalance_task` cancellation and budget paths.

use super::*;

#[tokio::test]
async fn pull_buckets_completes_trivially_with_no_buckets_to_pull() {
    let cluster = solo_cluster("rebalance-unit-test-empty").await;
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, _tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );

    let outcome = PullRequest {
        cluster: &cluster,
        shard: &empty_shard(),
        ownership: &tracker,
        residency: &Arc::new(ResidencySet::new()),
        cache: &name,
        parts: Vec::new(),
        budget: Duration::from_secs(1),
        concurrency: 4,
        trust_sole_owner: true,
    }
    .run()
    .await;
    assert_eq!(outcome, Outcome::Completed);

    cluster.shutdown().await;
}

/// `RequestHandler::st_serve`'s default composes the three bucket
/// calls: a hash mismatch is `Stale`, and otherwise the ids stream in
/// request order, each standing for a whole bucket.
#[tokio::test]
async fn st_serve_default_composes_the_bucket_availability_cold_and_chunk_calls() {
    use futures::StreamExt as _;

    use crate::net::test_support::BucketPullHandler;
    use crate::net::{RequestHandler, StServe};

    let handler = BucketPullHandler {
        view_hash: 42,
        chunks: vec![(9u16, vec![sample_wire_record(1)])],
        stall_after: false,
        ..Default::default()
    };
    let cache = SmolStr::new("prices");
    assert!(matches!(
        handler.st_serve(cache.clone(), vec![9, 3], 41).await,
        StServe::Stale {
            responder_view_hash: 0
        }
    ));
    let StServe::Stream {
        mut chunks,
        order,
        parts,
    } = handler.st_serve(cache, vec![9, 3], 42).await
    else {
        panic!("a matching view hash streams");
    };
    assert_eq!(order, vec![9, 3], "the default keeps request order");
    assert_eq!(parts, 2 * crate::store::PART_COUNT as u64);
    let (id, recs) = chunks.next().await.expect("the fixture's one chunk");
    assert_eq!((id, recs.len()), (9, 1));
}

#[tokio::test]
async fn pull_buckets_reports_no_peers_when_every_bucket_has_no_other_owner() {
    let cluster = solo_cluster("rebalance-unit-test-solo").await;
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, _tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );

    let outcome = PullRequest {
        cluster: &cluster,
        shard: &empty_shard(),
        ownership: &tracker,
        residency: &Arc::new(ResidencySet::new()),
        cache: &name,
        parts: PartId::all().take(3).collect(),
        budget: Duration::from_secs(1),
        concurrency: 4,
        trust_sole_owner: true,
    }
    .run()
    .await;
    assert_eq!(outcome, Outcome::NoPeers);

    cluster.shutdown().await;
}

#[tokio::test]
async fn warm_up_task_waits_for_a_co_owner_and_warms_once_its_pulls_give_up() {
    let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-warm-up")
        .seeds(std::iter::empty())
        .config(crate::config::ClusterConfig {
            gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
            data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
            ae_interval: Duration::from_millis(50),
            ..crate::config::ClusterConfig::default()
        })
        .build()
        .await
        .expect("solo cluster builds");
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );
    let cancel = CancellationToken::new();
    let task = tokio::spawn(warm_up_task(
        cluster.clone(),
        empty_shard(),
        tracker,
        Arc::new(ResidencySet::new()),
        name.clone(),
        Duration::from_millis(150),
        4,
        cancel.clone(),
    ));

    // Self-only view: no co-owner anywhere, so the task waits on the
    // view instead of spinning or marking the cache warm.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!cluster.is_warm(&name), "no co-owner in sight: still cold");
    assert!(!task.is_finished(), "the task waits for a view change");

    // A view with an unreachable co-owner: every pull times out, and
    // after the attempt cap the cache opens warm with what landed.
    let phantom = NodeId::from(u64::MAX);
    tx.send(Arc::new(view(
        cluster.node_id(),
        vec![cluster.node_id(), phantom],
        2,
    )))
    .expect("the task still holds its receiver");
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the task ends within the bound")
        .expect("the task does not panic");
    assert!(cluster.is_warm(&name), "warm after the pulls gave up");

    cancel.cancel();
    cluster.shutdown().await;
}

#[cfg(feature = "spill")]
#[tokio::test]
async fn pull_request_marks_a_warm_reloaded_bucket_servable_when_owned_alone() {
    let cluster = solo_cluster("rebalance-unit-test-warm-alone").await;
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, _tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );

    // Self-only view: no co-owner ever joined the cluster.
    let bucket = tracker
        .current()
        .owned_parts()
        .next()
        .expect("self owns at least one bucket alone");
    let residency = Arc::new(ResidencySet::new());
    // Stands in for attach_spill_and_record_warm's marks after a warm reopen.
    residency.mark_cold(&[bucket]);
    residency.mark_unverified(&[bucket]);

    let outcome = PullRequest {
        cluster: &cluster,
        shard: &empty_shard(),
        ownership: &tracker,
        residency: &residency,
        cache: &name,
        parts: vec![bucket],
        budget: Duration::from_secs(1),
        concurrency: 4,
        trust_sole_owner: true,
    }
    .run()
    .await;

    assert_eq!(
        outcome,
        Outcome::NoPeers,
        "no co-owner anywhere for this bucket"
    );
    assert!(
        !residency.is_cold(bucket),
        "a sole-owned bucket is not cold: what is here is all there is"
    );
    assert!(
        !residency.is_unverified(bucket),
        "a sole-owned warm-reloaded bucket is servable too: with nobody to verify it \
         against, the replayed data is the best available answer, exactly the same \
         decision that already clears the cold mark"
    );

    cluster.shutdown().await;
}

#[cfg(feature = "spill")]
#[tokio::test]
async fn pull_request_leaves_a_warm_reloaded_bucket_cold_when_owned_alone_is_untrusted() {
    // Same fixture as the servable test above, but trust_sole_owner:
    // false, the shape open() passes when a warm reopen's membership
    // wait timed out.
    let cluster = solo_cluster("rebalance-unit-test-warm-alone-untrusted").await;
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, _tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );

    let bucket = tracker
        .current()
        .owned_parts()
        .next()
        .expect("self owns at least one bucket alone");
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&[bucket]);
    residency.mark_unverified(&[bucket]);

    let outcome = PullRequest {
        cluster: &cluster,
        shard: &empty_shard(),
        ownership: &tracker,
        residency: &residency,
        cache: &name,
        parts: vec![bucket],
        budget: Duration::from_secs(1),
        concurrency: 4,
        trust_sole_owner: false,
    }
    .run()
    .await;

    assert_eq!(
        outcome,
        Outcome::NoPeers,
        "still no co-owner anywhere for this bucket in the current view"
    );
    assert!(
        residency.is_cold(bucket),
        "untrusted, this bucket stays cold: the caller's ordinary warm-up retries get the \
         final say once a real answer lands, instead of this pull trusting an unverified \
         sole-owner echo outright"
    );
    assert!(
        residency.is_unverified(bucket),
        "untrusted, a warm-reloaded bucket's replay also stays unverified: nobody has \
         vouched for it yet, and this view's \"nobody to ask\" answer is not proof that \
         nobody exists"
    );

    cluster.shutdown().await;
}

#[cfg(feature = "spill")]
#[tokio::test]
async fn warm_up_task_marks_a_warm_reloaded_bucket_servable_once_its_pulls_give_up() {
    let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-warm-up-unverified")
        .seeds(std::iter::empty())
        .config(crate::config::ClusterConfig {
            gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
            data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
            ae_interval: Duration::from_millis(50),
            ..crate::config::ClusterConfig::default()
        })
        .build()
        .await
        .expect("solo cluster builds");
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );
    // The phantom co-owner is already in the first view read, so
    // every owned bucket is co-owned from attempt 1.
    let phantom = NodeId::from(u64::MAX);
    let phantom_view = Arc::new(view(cluster.node_id(), vec![cluster.node_id(), phantom], 2));
    tx.send(Arc::clone(&phantom_view))
        .expect("receiver still alive");

    let bucket = phantom_view
        .owned_parts()
        .next()
        .expect("self owns at least one bucket");
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&[bucket]);
    residency.mark_unverified(&[bucket]);

    let cancel = CancellationToken::new();
    let task = tokio::spawn(warm_up_task(
        cluster.clone(),
        empty_shard(),
        tracker,
        Arc::clone(&residency),
        name.clone(),
        Duration::from_millis(150),
        4,
        cancel.clone(),
    ));

    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the task ends within the bound")
        .expect("the task does not panic");

    assert!(cluster.is_warm(&name), "warm after the pulls gave up");
    assert!(
        !residency.is_cold(bucket),
        "the give-up path clears cold for a bucket whose only co-owner never answered"
    );
    assert!(
        !residency.is_unverified(bucket),
        "the give-up path clears unverified alongside cold: once the warm-up attempts run \
         out with nobody left to verify against, the replayed data is the best available \
         answer"
    );

    cancel.cancel();
    cluster.shutdown().await;
}

/// A pull whose only donor never answers usefully runs until the
/// transfer budget; cancelling the task (what `Cluster::shutdown` does)
/// ends it at once instead of waiting that out. The donor is live in
/// gossip but never opened the cache, so every attempt is declined and
/// the pull keeps retrying, as it does against a donor that crashed
/// before the failure detector noticed.
#[tokio::test]
async fn rebalance_task_stops_mid_pull_when_cancelled() {
    let config = crate::config::ClusterConfig {
        gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        state_transfer_budget: Duration::from_secs(120),
        ..crate::config::ClusterConfig::default()
    };
    let cluster = crate::cluster::Cluster::builder("rebalance-unit-test-cancel-mid-pull")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("a builds");
    let donor = crate::cluster::Cluster::builder("rebalance-unit-test-cancel-mid-pull")
        .seeds([cluster.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("the donor builds");
    tokio::time::timeout(Duration::from_secs(15), async {
        while !cluster
            .peers()
            .iter()
            .any(|peer| peer.node == donor.node_id())
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the donor is live in gossip");

    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );
    let residency = Arc::new(ResidencySet::new());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rebalance_task(
        cluster.clone(),
        empty_shard(),
        tracker,
        Arc::clone(&residency),
        name,
        Duration::from_secs(3600),
        4,
        cancel.clone(),
    ));

    // Half the parts go to a phantom node, then come back co-owned
    // with the donor: the second view gains them and pulls from it.
    let phantom = NodeId::from(u64::MAX);
    let half = view(cluster.node_id(), vec![cluster.node_id(), phantom], 1);
    tx.send(Arc::new(half)).expect("receiver still alive");
    tokio::time::timeout(Duration::from_secs(5), async {
        while residency.expired(Duration::ZERO).is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first view change is handled");
    let full = view(
        cluster.node_id(),
        vec![cluster.node_id(), donor.node_id()],
        2,
    );
    let gained = full
        .owned_parts()
        .find(|&part| residency.is_releasing(part))
        .expect("a part the phantom took");
    tx.send(Arc::new(full)).expect("receiver still alive");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !residency.is_cold(gained) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the pull starts");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        residency.is_cold(gained),
        "the declined pull is still retrying"
    );

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("the task ends within two seconds of cancel, not at the transfer budget")
        .expect("the task does not panic");

    donor.shutdown().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn pull_buckets_skips_with_a_zero_budget() {
    let cluster = solo_cluster("rebalance-unit-test-zero-budget").await;
    let name = SmolStr::new("prices");
    let k = NonZeroU8::new(2).expect("nonzero");
    let (tracker, _tx) = OwnershipTracker::seed(
        cluster.node_id(),
        &cluster.peers(),
        &cluster.advertised_cache_modes(),
        &name,
        k,
    );

    let outcome = PullRequest {
        cluster: &cluster,
        shard: &empty_shard(),
        ownership: &tracker,
        residency: &Arc::new(ResidencySet::new()),
        cache: &name,
        parts: vec![PartId::from_raw(0)],
        budget: Duration::ZERO,
        concurrency: 4,
        trust_sole_owner: true,
    }
    .run()
    .await;
    assert_eq!(outcome, Outcome::Skipped);

    cluster.shutdown().await;
}
