//! Shared helpers and the topic modules of the rebalance unit tests.

use std::num::NonZeroU8;

use super::*;
use crate::node::NodeId;

/// A reference model of which parts this node holds every write for,
/// driven against the real `refresh_task` bookkeeping
/// ([`ResidencySet::record_published`]), [`plan_view_change`] and
/// [`ResidencySet::settle`] under the orderings the watch channel
/// allows: views published faster than the rebalance task looks
/// coalesce, and a pull can be superseded partway.
mod model;
mod planning;
mod release;

fn view(self_node: NodeId, eligible: Vec<NodeId>, k: u8) -> OwnershipView {
    OwnershipView::compute(self_node, eligible, NonZeroU8::new(k).expect("nonzero"))
}

fn empty_shard() -> Arc<dyn ShardOps> {
    Arc::new(crate::store::Shard::<u32, u32>::new(
        SmolStr::new("prices"),
        crate::store::Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        },
        crate::node::NodeId::random(),
        1024,
        None,
        None,
    )) as Arc<dyn ShardOps>
}

/// [`empty_shard`] planning against `ownership`, so a part-scoped
/// anti-entropy round has a view to send.
fn owned_shard(ownership: &OwnershipTracker, residency: &Arc<ResidencySet>) -> Arc<dyn ShardOps> {
    Arc::new(
        crate::store::Shard::<u32, u32>::new(
            SmolStr::new("prices"),
            crate::store::Mode::Distributed {
                owners: NonZeroU8::new(2).expect("nonzero"),
            },
            crate::node::NodeId::random(),
            1024,
            None,
            None,
        )
        .with_ownership(ownership.clone(), Arc::clone(residency)),
    ) as Arc<dyn ShardOps>
}

async fn solo_cluster(name: &str) -> crate::cluster::Cluster {
    crate::cluster::Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(crate::config::ClusterConfig {
            gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
            data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
            ..crate::config::ClusterConfig::default()
        })
        .build()
        .await
        .expect("solo cluster builds")
}

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

#[test]
fn cold_round_settles_nothing_on_a_round_that_never_reached_the_donor() {
    let (a, b) = (PartId::from_raw(1), PartId::from_raw(2));
    let round = |failed: bool, matched: &[PartId]| anti_entropy::PartRoundOutcome {
        matched: matched.iter().copied().collect(),
        still_diverged: HashSet::new(),
        bytes_moved: 0,
        failed,
    };

    let mut diverging = vec![a, b];
    assert_eq!(
        cold_round(None, &mut diverging),
        ColdRound::Unreached,
        "a round that ran out of time"
    );
    assert_eq!(
        cold_round(Some(round(true, &[])), &mut diverging),
        ColdRound::Unreached,
        "a failed or stale round"
    );
    assert_eq!(diverging, vec![a, b], "an unreached round compares nothing");

    assert_eq!(
        cold_round(Some(round(false, &[a])), &mut diverging),
        ColdRound::Diverging
    );
    assert_eq!(diverging, vec![b]);
    assert_eq!(
        cold_round(Some(round(false, &[b])), &mut diverging),
        ColdRound::Matched
    );
    assert!(diverging.is_empty(), "{diverging:?}");
}

#[test]
fn settle_all_cold_serves_cold_parts_and_holds_back_unverified_ones() {
    let parts = [
        PartId::from_raw(1),
        PartId::from_raw(2),
        PartId::from_raw(3),
    ];
    let (settled, pull) = settle_all_cold(&parts, |part| part == PartId::from_raw(2));
    assert_eq!(settled, vec![PartId::from_raw(1), PartId::from_raw(3)]);
    assert_eq!(pull, GroupPull::Unverified);
    let (settled, pull) = settle_all_cold(&parts, |_| false);
    assert_eq!(settled, parts.to_vec());
    assert_eq!(pull, GroupPull::Landed(0));
}

/// The warm-reopen race: this node replayed part `a` from disk, the only
/// donor is still cold on it, and the part that donor could not give is
/// the one whose replay may lack keys. Serving it would answer a wrong
/// miss, so it stays cold and unverified while plain cold `b` serves.
#[cfg(feature = "spill")]
#[tokio::test]
async fn pull_one_group_keeps_an_unverified_part_waiting_when_every_donor_is_cold() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let (donor_node, requester_node) = (NodeId::from(111), NodeId::from(112));
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);
    let view_hash = ownership.current().view_hash();
    let donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        cold: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
    let (requester, _requester_inbound) =
        spawn_mesh(requester_node, Arc::new(BucketPullHandler::default())).await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

    let (a, b) = (PartId::new(3, 0), PartId::new(3, 1));
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&[a, b]);
    residency.mark_unverified(&[a]);

    let pull = pull_one_group(
        &owned_shard(&ownership, &residency),
        &requester,
        &cache,
        &ownership,
        &residency.open_pull(),
        vec![donor_node],
        vec![a, b],
        (Granularity::Part, view_hash),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(pull, GroupPull::Unverified);
    assert!(
        residency.is_cold(a) && residency.is_unverified(a),
        "the unverified replay keeps both marks, so reads ask the other owners"
    );
    assert!(
        !residency.is_cold(b),
        "the plain cold part serves what landed"
    );

    donor.shutdown().await;
    requester.shutdown().await;
}

/// Two owners that both regained a part hold only stale copies, and
/// each is the other's cold donor. Once they converge, no copy anywhere
/// is more complete, so the stale part serves like a plain cold one
/// instead of waiting for a warm donor that never comes.
#[tokio::test]
async fn pull_one_group_serves_a_stale_part_once_every_donor_is_cold() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let (donor_node, requester_node) = (NodeId::from(121), NodeId::from(122));
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);
    let view_hash = ownership.current().view_hash();
    let donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        cold: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
    let (requester, _requester_inbound) =
        spawn_mesh(requester_node, Arc::new(BucketPullHandler::default())).await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

    let part = PartId::new(3, 0);
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&[part]);
    residency.mark_stale(&[part]);

    let pull = pull_one_group(
        &owned_shard(&ownership, &residency),
        &requester,
        &cache,
        &ownership,
        &residency.open_pull(),
        vec![donor_node],
        vec![part],
        (Granularity::Part, view_hash),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(pull, GroupPull::Landed(0));
    assert!(
        !residency.is_cold(part) && !residency.is_unverified(part),
        "the stale part serves once converged with every cold donor"
    );

    donor.shutdown().await;
    requester.shutdown().await;
}

/// A donor that declines a group as cold can still hold the group's
/// keys, so a group whose converge never reaches it keeps its parts
/// cold rather than serving what landed here.
#[tokio::test]
async fn pull_one_group_keeps_a_cold_group_cold_while_its_donor_answers_anti_entropy_stale() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let (donor_node, requester_node) = (NodeId::from(131), NodeId::from(132));
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);
    let view_hash = ownership.current().view_hash();
    let donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        cold: true,
        ae_stale: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
    let (requester, _requester_inbound) =
        spawn_mesh(requester_node, Arc::new(BucketPullHandler::default())).await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

    let part = PartId::new(3, 0);
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&[part]);

    let pull = tokio::time::timeout(
        Duration::from_secs(2),
        pull_one_group(
            &owned_shard(&ownership, &residency),
            &requester,
            &cache,
            &ownership,
            &residency.open_pull(),
            vec![donor_node],
            vec![part],
            (Granularity::Part, view_hash),
            Duration::from_millis(500),
        ),
    )
    .await;
    assert!(
        pull.is_err(),
        "the group keeps retrying instead of settling: {pull:?}"
    );
    assert!(
        residency.is_cold(part),
        "reads keep asking the other owners"
    );

    donor.shutdown().await;
    requester.shutdown().await;
}

#[tokio::test]
async fn pull_one_group_clears_cold_per_bucket_as_each_done_arrives_before_the_group_finishes() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let view_hash = 42;
    let donor_node = NodeId::from(101);
    let requester_node = NodeId::from(102);

    // Bucket 0 completes; bucket 1's chunk lands but the donor then
    // stalls, a stand-in for a mid-group connection drop.
    let donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![
            (0u16, vec![sample_wire_record(1)]),
            (1u16, vec![sample_wire_record(2)]),
        ],
        stall_after: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(donor_node, donor_handler).await;
    let requester_handler = Arc::new(BucketPullHandler {
        view_hash: 0,
        chunks: Vec::new(),
        stall_after: false,
        ..Default::default()
    });
    let (requester, _requester_inbound) = spawn_mesh(requester_node, requester_handler).await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);

    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&two_buckets());
    let shard = empty_shard();
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

    let outcome = tokio::time::timeout(
        Duration::from_millis(500),
        pull_one_group(
            &shard,
            &requester,
            &cache,
            &ownership,
            &residency.open_pull(),
            vec![donor_node],
            two_buckets(),
            (Granularity::Bucket, view_hash),
            Duration::from_secs(5),
        ),
    )
    .await;
    assert!(
        outcome.is_err(),
        "the donor stalls after bucket 1's chunk, so the group never finishes within the \
         test's patience"
    );
    assert!(
        PartId::of_bucket(0).all(|p| !residency.is_cold(p)),
        "bucket 0's BucketDone landed and cleared its cold mark before the group finished"
    );
    assert!(
        PartId::of_bucket(1).all(|p| residency.is_cold(p)),
        "bucket 1's BucketDone never arrived, so it stays cold"
    );

    donor.shutdown().await;
    requester.shutdown().await;
}

/// A donor that stalls after finishing bucket 0 leaves bucket 1 for the
/// next donor, which is asked for bucket 1 alone.
#[tokio::test]
async fn pull_one_group_asks_a_later_donor_only_for_the_parts_no_earlier_attempt_finished() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let view_hash = 42;
    let (first_node, second_node, requester_node) =
        (NodeId::from(301), NodeId::from(302), NodeId::from(303));
    let first_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![
            (0u16, vec![sample_wire_record(1)]),
            (1u16, vec![sample_wire_record(2)]),
        ],
        stall_after: true,
        ..Default::default()
    });
    let second_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![(1u16, vec![sample_wire_record(2)])],
        stall_after: false,
        ..Default::default()
    });
    let (first, _first_inbound) = spawn_mesh(first_node, Arc::clone(&first_handler) as _).await;
    let (second, _second_inbound) = spawn_mesh(second_node, Arc::clone(&second_handler) as _).await;
    let (requester, _requester_inbound) = spawn_mesh(
        requester_node,
        Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
            ..Default::default()
        }),
    )
    .await;
    requester.update_peers(vec![
        peer_at(first_node, first.local_addr()),
        peer_at(second_node, second.local_addr()),
    ]);
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&two_buckets());
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

    let landed = pull_one_group(
        &empty_shard(),
        &requester,
        &cache,
        &ownership,
        &residency.open_pull(),
        vec![first_node, second_node],
        two_buckets(),
        (Granularity::Bucket, view_hash),
        Duration::from_millis(300),
    )
    .await;
    assert_eq!(
        landed,
        GroupPull::Landed(2 * crate::store::PART_COUNT as u64)
    );
    assert_eq!(
        *second_handler.requested.lock().expect("fixture mutex"),
        vec![vec![1u16]],
        "the second donor is asked only for the bucket the first never finished"
    );
    assert!(
        two_buckets()
            .into_iter()
            .all(|part| !residency.is_cold(part))
    );

    first.shutdown().await;
    second.shutdown().await;
    requester.shutdown().await;
}

/// A part-granular pull of thousands of parts, most of them empty,
/// finishes every part, and the donor records an ack for every one:
/// done frames and acks both cross their batch sizes.
#[tokio::test]
async fn a_pull_of_thousands_of_parts_finishes_and_acks_every_part() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    const PARTS: usize = 3_000;
    let cache = SmolStr::new("prices");
    let view_hash = 42;
    let (donor_node, requester_node) = (NodeId::from(401), NodeId::from(402));
    let parts: Vec<PartId> = PartId::all().step_by(7).take(PARTS).collect();
    let first = parts[0];
    let (donor, _donor_inbound) = spawn_mesh(
        donor_node,
        Arc::new(BucketPullHandler {
            view_hash,
            chunks: vec![(first.raw(), vec![sample_wire_record(1)])],
            stall_after: false,
            ..Default::default()
        }),
    )
    .await;
    let (requester, _requester_inbound) = spawn_mesh(
        requester_node,
        Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
            ..Default::default()
        }),
    )
    .await;
    requester.update_peers(vec![peer_at(donor_node, donor.local_addr())]);
    donor.update_peers(vec![peer_at(requester_node, requester.local_addr())]);
    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&parts);
    let modes: crate::membership::CacheModes = std::collections::HashMap::new();
    let k = NonZeroU8::new(2).expect("nonzero");
    let (ownership, _tx) = OwnershipTracker::seed(requester_node, &[], &modes, &cache, k);

    let landed = pull_one_group(
        &empty_shard(),
        &requester,
        &cache,
        &ownership,
        &residency.open_pull(),
        vec![donor_node],
        parts.clone(),
        (Granularity::Part, view_hash),
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(landed, GroupPull::Landed(PARTS as u64));
    assert!(parts.iter().all(|&part| !residency.is_cold(part)));

    let ids: Vec<u16> = parts.iter().map(|part| part.raw()).collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let acked = donor.acked_owners(&cache, &ids, Duration::from_secs(60), view_hash);
        if acked.len() == PARTS {
            assert!(
                acked
                    .values()
                    .all(|owners| owners.contains(&requester_node))
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {} of {PARTS} parts acked",
            acked.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    donor.shutdown().await;
    requester.shutdown().await;
}

/// Pins that `credited`, threaded across two donor calls the way
/// `pull_one_group` does, counts bucket 0 once even though the first
/// donor stalls after landing it and the second donor re-sends it:
/// guards the regression where a donor retry double-counted a bucket.
#[tokio::test]
async fn try_donor_parts_shares_credited_parts_across_repeated_calls_for_one_group() {
    use crate::net::test_support::{BucketPullHandler, peer_at, spawn_mesh};

    let cache = SmolStr::new("prices");
    let view_hash = 42;
    let first_donor_node = NodeId::from(201);
    let second_donor_node = NodeId::from(202);
    let requester_node = NodeId::from(203);

    let first_donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![
            (0u16, vec![sample_wire_record(1)]),
            (1u16, vec![sample_wire_record(2)]),
        ],
        stall_after: true,
        ..Default::default()
    });
    let (first_donor, _first_donor_inbound) =
        spawn_mesh(first_donor_node, first_donor_handler).await;
    let second_donor_handler = Arc::new(BucketPullHandler {
        view_hash,
        chunks: vec![
            (0u16, vec![sample_wire_record(1)]),
            (1u16, vec![sample_wire_record(2)]),
        ],
        stall_after: false,
        ..Default::default()
    });
    let (second_donor, _second_donor_inbound) =
        spawn_mesh(second_donor_node, second_donor_handler).await;
    let requester_handler = Arc::new(BucketPullHandler {
        view_hash: 0,
        chunks: Vec::new(),
        stall_after: false,
        ..Default::default()
    });
    let (requester, _requester_inbound) = spawn_mesh(requester_node, requester_handler).await;
    requester.update_peers(vec![
        peer_at(first_donor_node, first_donor.local_addr()),
        peer_at(second_donor_node, second_donor.local_addr()),
    ]);

    let residency = Arc::new(ResidencySet::new());
    residency.mark_cold(&two_buckets());
    let shard = empty_shard();
    let mut credited: HashSet<PartId> = HashSet::new();

    // Mirrors pull_one_group's per-donor timeout; the first donor
    // stalls after bucket 1's chunk.
    let first_call = tokio::time::timeout(
        Duration::from_millis(200),
        try_donor_parts(
            &shard,
            &requester,
            &cache,
            &residency.open_pull(),
            first_donor_node,
            &two_buckets(),
            (Granularity::Bucket, view_hash),
            &mut credited,
        ),
    )
    .await;
    assert!(
        first_call.is_err(),
        "the first donor stalls after bucket 1's chunk, so its own call never resolves \
         within the timeout, mirroring a dropped connection mid-group"
    );
    assert_eq!(
        credited,
        PartId::of_bucket(0).collect::<HashSet<_>>(),
        "bucket 0's BucketDone landed (flushed once bucket 1's chunk proved bucket 0 is \
         behind it) and was credited before the stall; bucket 1's own BucketDone never \
         arrived"
    );

    let (result_second, count_second, cold_second) = try_donor_parts(
        &shard,
        &requester,
        &cache,
        &residency.open_pull(),
        second_donor_node,
        &two_buckets(),
        (Granularity::Bucket, view_hash),
        &mut credited,
    )
    .await;
    assert_eq!(
        result_second,
        DonorResult::Done,
        "the second donor completes the whole group"
    );
    assert_eq!(
        count_second, 2,
        "the second donor redelivers bucket 0 and lands bucket 1"
    );
    assert!(!cold_second);
    assert_eq!(
        credited,
        two_buckets().into_iter().collect::<HashSet<_>>(),
        "bucket 0 stays credited exactly once even though the second donor redelivered its \
         BucketDone; only bucket 1 is newly credited"
    );

    first_donor.shutdown().await;
    second_donor.shutdown().await;
    requester.shutdown().await;
}

/// Every part of buckets 0 and 1: what a bucket-mode view's pull of
/// those two buckets covers.
fn two_buckets() -> Vec<PartId> {
    PartId::of_bucket(0).chain(PartId::of_bucket(1)).collect()
}

fn sample_wire_record(n: u8) -> crate::wire::WireRecord {
    crate::wire::WireRecord {
        key: bytes::Bytes::from(vec![n]),
        value: Some(bytes::Bytes::from(vec![n, n])),
        ver: crate::hlc::Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    }
}
