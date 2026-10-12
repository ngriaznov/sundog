//! Tests for distributed caches: the refresh task, hand-off grouping and rebalance across nodes.

use super::*;
use crate::hlc::Hlc;
use crate::ownership::Granularity;
use crate::wire::WireRecord;
use bytes::Bytes;

#[tokio::test]
async fn refresh_task_republishes_on_a_membership_change_and_skips_an_unchanged_hash() {
    let cluster_a = solo_cluster("cluster-it-refresh-task").await;
    let name = SmolStr::new("prices");
    let k = std::num::NonZeroU8::new(2).expect("nonzero");

    let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
        cluster_a.node_id(),
        &cluster_a.peers(),
        &cluster_a.advertised_cache_modes(),
        &name,
        k,
    );
    let mut sub = tracker.subscribe();
    let initial_hash = sub.borrow().view_hash();

    let cancel = CancellationToken::new();
    tokio::spawn(crate::ownership::refresh_task(
        cluster_a.clone(),
        name.clone(),
        k,
        tx,
        Arc::new(crate::ownership::ResidencySet::new()),
        cancel.clone(),
    ));

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-refresh-task")
        .seeds([gossip_a])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    cluster_b.advertise_cache_mode(&name, Mode::Distributed { owners: k });
    wait_for_peer_count(&cluster_a, 1).await;

    tokio::time::timeout(Duration::from_secs(10), sub.changed())
        .await
        .expect("refresh_task republishes within the bound")
        .expect("the sender stays alive");
    let republished_hash = sub.borrow_and_update().view_hash();
    assert_ne!(
        republished_hash, initial_hash,
        "b becoming eligible changes the computed view"
    );

    // Re-advertising the exact same mode bumps b's gossip version (a
    // real membership change `cache_modes_watch` fires on) without
    // changing the eligible set at all: the recomputed view hash is
    // identical, so `send_if_modified` never republishes. A bounded
    // quiescence window with no further `changed()` is the direct
    // observation of that skip.
    cluster_b.advertise_cache_mode(&name, Mode::Distributed { owners: k });
    let unchanged = tokio::time::timeout(Duration::from_millis(800), sub.changed()).await;
    assert!(
        unchanged.is_err(),
        "an unchanged eligible set never republishes a new view"
    );
    assert_eq!(
        tracker.current().view_hash(),
        republished_hash,
        "the view stayed exactly what it was"
    );

    cancel.cancel();
    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

/// A short-`ae_interval`, short-disown-grace config for the rebalance
/// test below: a real grace elapses in ten seconds, not the default 30.
/// A joiner's pull of its share of the 65,536 parts lands about two
/// seconds after it starts in a debug build, so the grace outlasts it
/// on a runner several times slower.
fn rebalance_config() -> ClusterConfig {
    let mut config = ClusterConfig {
        ae_interval: Duration::from_millis(50),
        // A 30 s grace: long enough that the mid-grace check below
        // still lands inside it on a loaded runner.
        distributed_disown_grace_rounds: 600,
        ..loopback_config()
    };
    config.tombstone_ttl = config.bucket_release_window();
    config
}

#[test]
fn hand_off_groups_pair_each_owned_part_with_its_live_co_owners_only() {
    let (me, live_peer, dead_peer) = (NodeId::from(1), NodeId::from(2), NodeId::from(3));
    let view = OwnershipView::compute(
        me,
        vec![me, live_peer, dead_peer],
        std::num::NonZeroU8::new(2).expect("nonzero"),
    );
    let live: HashSet<NodeId> = [live_peer].into_iter().collect();
    let groups = hand_off_groups(&view, me, &live);
    assert_eq!(groups.len(), 1, "a co-owner that is not live gets nothing");
    let (peer, parts) = &groups[0];
    assert_eq!(*peer, live_peer);
    let expected: Vec<PartId> = view
        .owned_parts()
        .filter(|&part| view.owners_of(part).contains(&live_peer))
        .collect();
    assert_eq!(
        parts, &expected,
        "every part this node co-owns with the live peer"
    );
    assert!(!parts.is_empty(), "{parts:?}");
}

#[allow(
    clippy::too_many_lines,
    reason = "one scripted three-node join-then-release scenario"
)]
// Multi-threaded: every node's grace expires in the same tick, and on
// one shared thread their hand-off rounds, each over about 22k parts,
// stall the others' gossip long enough for the failure detector to flap
// the view, which restarts the grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebalance_moves_a_bucket_to_a_joiner_and_the_old_owner_releases_it_after_the_grace() {
    let config = rebalance_config();
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let name = SmolStr::new("prices");

    let cluster_a = Cluster::builder("cluster-it-rebalance-join")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("a builds");
    let cache_a = cluster_a
        .cache::<u32, String>(name.as_str())
        .mode(Mode::Distributed { owners })
        .open()
        .await
        .expect("a opens alone");

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-rebalance-join")
        .seeds([gossip_a])
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&cluster_a, 1).await;
    let cache_b = tokio::time::timeout(
        Duration::from_secs(10),
        cluster_b
            .cache::<u32, String>(name.as_str())
            .mode(Mode::Distributed { owners })
            .open(),
    )
    .await
    .expect("b opens within the bound")
    .expect("b opens");
    wait_for_peer_count(&cluster_b, 1).await;

    // With only a and b live, owners = 2 means both hold everything.
    cache_a
        .insert_many((0..500u32).map(|k| (k, k.to_string())))
        .await
        .expect("a fills the cache before c ever joins");
    wait_until(
        Duration::from_secs(10),
        "b holds everything too, before c joins",
        async || cache_b.entry_count().await == 500,
    )
    .await;

    let grace = config.disown_grace();
    // No displaced owner's grace starts before c exists.
    let before_c = tokio::time::Instant::now();
    let cluster_c = Cluster::builder("cluster-it-rebalance-join")
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("c builds");
    let cache_c = tokio::time::timeout(
        Duration::from_secs(10),
        cluster_c
            .cache::<u32, String>(name.as_str())
            .mode(Mode::Distributed { owners })
            .open(),
    )
    .await
    .expect("c opens within the bound")
    .expect("c opens");
    wait_for_peer_count(&cluster_a, 2).await;
    wait_for_peer_count(&cluster_b, 2).await;
    wait_for_peer_count(&cluster_c, 2).await;

    // A key whose bucket c has newly gained, once c's own ownership
    // view has recomputed around a and b's cache-mode gossip landing.
    let (moved_key, a_displaced) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for key in 0..500u32 {
                let owners_now = cache_c.owners_of(&key);
                if owners_now.contains(&cluster_c.node_id()) {
                    let a_displaced = !owners_now.contains(&cluster_a.node_id());
                    return (key, a_displaced);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("c's view converges to own at least one of the 500 keys' buckets");
    let displaced_cache = if a_displaced { &cache_a } else { &cache_b };

    wait_until(
        Duration::from_secs(10),
        "c's rebalance pull lands the moved key's value",
        async || cache_c.get(&moved_key).await == Some(moved_key.to_string()),
    )
    .await;

    // Right after the pull lands, still inside the disown grace, the
    // displaced node has not released anything yet.
    let elapsed = before_c.elapsed();
    assert!(
        elapsed < grace,
        "setup ran {elapsed:?}, past the {grace:?} grace, so the check below cannot land mid-grace"
    );
    assert_eq!(
        displaced_cache.get(&moved_key).await,
        Some(moved_key.to_string()),
        "the displaced owner still resides the key {elapsed:?} into a {grace:?} grace"
    );

    wait_until(
        grace * 2,
        "the displaced owner releases the key once the disown grace elapses",
        async || displaced_cache.get(&moved_key).await.is_none(),
    )
    .await;
    assert!(
        before_c.elapsed() >= grace,
        "the displaced owner released the key {:?} after c joined, before the {grace:?} grace",
        before_c.elapsed()
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

/// A node that owned every bucket alone, because its peers had not
/// opened the cache yet, hands the buckets it then loses to both new
/// owners as soon as its view changes. Anti-entropy and the disown
/// grace both run once an hour here, so only the push on the view
/// change can move the entries.
#[allow(
    clippy::too_many_lines,
    reason = "one scripted lone-owner-then-three-node scenario"
)]
#[tokio::test]
async fn a_lone_owner_pushes_each_bucket_it_loses_to_the_new_owners_on_the_view_change() {
    const TOTAL: u32 = 300;
    let mut config = ClusterConfig {
        ae_interval: Duration::from_secs(3600),
        state_transfer_budget: Duration::from_secs(2),
        ..loopback_config()
    };
    config.tombstone_ttl = config.bucket_release_window();
    let owners = std::num::NonZeroU8::new(2).expect("nonzero");
    let name = SmolStr::new("prices");
    let cluster_name = "cluster-it-lone-owner-push";

    let cluster_a = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("a builds");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cluster_c = Cluster::builder(cluster_name)
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("c builds");
    for cluster in [&cluster_a, &cluster_b, &cluster_c] {
        wait_for_peer_count(cluster, 2).await;
    }
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>(name.as_str())
            .mode(Mode::Distributed { owners })
            .open()
    };
    let (cache_b, cache_c) = tokio::join!(open(&cluster_b), open(&cluster_c));
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    // a's shard under the view it seeded before any peer advertised
    // the cache: a owns every bucket alone and holds every entry.
    let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
        cluster_a.node_id(),
        &[],
        &HashMap::new(),
        &name,
        owners,
    );
    let residency = Arc::new(crate::ownership::ResidencySet::new());
    let shard_a: Arc<dyn ShardOps> = Arc::new(
        Shard::<u32, String>::new(
            name.clone(),
            Mode::Distributed { owners },
            cluster_a.node_id(),
            u64::MAX,
            None,
            None,
        )
        .with_ownership(tracker.clone(), Arc::clone(&residency)),
    );
    shard_a
        .apply_remote_batch(
            (0..TOTAL)
                .map(|key| WireRecord {
                    key: Bytes::from(postcard::to_stdvec(&key).expect("key encodes")),
                    value: Some(Bytes::from(
                        postcard::to_stdvec(&format!("v{key}")).expect("value encodes"),
                    )),
                    ver: Hlc {
                        wall_ms: 1,
                        logical: 0,
                        node: cluster_a.node_id(),
                    },
                    expires_at_ms: None,
                })
                .collect(),
        )
        .await;

    // b and c take a into their views once it advertises the cache.
    cluster_a.advertise_cache_mode(&name, Mode::Distributed { owners });
    let three = Arc::new(OwnershipView::compute_at(
        cluster_a.node_id(),
        vec![
            cluster_a.node_id(),
            cluster_b.node_id(),
            cluster_c.node_id(),
        ],
        owners,
        Granularity::Part,
    ));
    wait_until(
        Duration::from_secs(10),
        "b and c compute the same three-node view a is about to publish",
        async || {
            (0..TOTAL).all(|key| {
                let mut expected = three.owners_of(part_of_u32(key)).to_vec();
                expected.sort_unstable();
                [&cache_b, &cache_c].iter().all(|cache| {
                    let mut seen = cache.owners_of(&key);
                    seen.sort_unstable();
                    seen == expected
                })
            })
        },
    )
    .await;
    let lost: Vec<u32> = (0..TOTAL)
        .filter(|&key| !three.owns(part_of_u32(key)))
        .collect();
    assert!(
        !lost.is_empty(),
        "a three-node, two-owner split leaves a some key it does not own"
    );

    let cancel = CancellationToken::new();
    let task = tokio::spawn(crate::cluster::rebalance::rebalance_task(
        cluster_a.clone(),
        Arc::clone(&shard_a),
        tracker,
        Arc::clone(&residency),
        name.clone(),
        Duration::from_secs(3600),
        4,
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.send(Arc::clone(&three))
        .expect("the tracker's own receiver keeps the channel open");

    wait_until(
        Duration::from_secs(10),
        "every entry a lost reaches both of its new owners",
        async || {
            for &key in &lost {
                let expected = Some(format!("v{key}"));
                if cache_b.get(&key).await != expected || cache_c.get(&key).await != expected {
                    return false;
                }
            }
            true
        },
    )
    .await;
    for &key in &lost {
        assert!(
            residency.is_releasing(part_of_u32(key)),
            "a keeps serving part {} through its disown grace",
            part_of_u32(key)
        );
    }

    cancel.cancel();
    task.await.expect("rebalance_task exits on cancel");
    cluster_c.shutdown().await;
    cluster_b.shutdown().await;
    cluster_a.shutdown().await;
}
