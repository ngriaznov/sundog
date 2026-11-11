//! Tests for applying repairs and the merging round, over real transport.

use std::time::Duration;

use super::*;

/// A merging round pulls before it pushes. A key whose pulled record
/// already contains the local one lands at exactly the peer's version and
/// is not pushed back; a key whose merge mints a new version still is.
#[tokio::test]
async fn a_merging_round_does_not_push_back_a_key_it_adopted_from_the_peer() {
    use crate::cluster::test_support::{loopback_config, registered_shard, wait_for_peer_count};
    use crate::store::Mode;
    use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};

    let config = loopback_config();
    let cluster_a = Cluster::builder("cluster-it-apply-repairs-merging")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let node_a = cluster_a.node_id();
    cluster_a
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Replicated)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("a opens");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-apply-repairs-merging")
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_b, 1).await;
    let node_b = cluster_b.node_id();
    let cache_b = tokio::time::timeout(
        Duration::from_secs(20),
        cluster_b
            .cache::<u32, PnCounter>("counters")
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .open(),
    )
    .await
    .expect("open completes within the state-transfer budget")
    .expect("b opens");

    let name = SmolStr::new("counters");
    let shard_a = registered_shard(&cluster_a, &name);
    let shard_b = registered_shard(&cluster_b, &name);
    let record = |key: u32, counter: &PnCounter, wall_ms, node| {
        let key_bytes = Bytes::from(postcard::to_stdvec(&key).expect("key encodes"));
        let rec = WireRecord {
            key: key_bytes.clone(),
            value: Some(Bytes::from(
                postcard::to_stdvec(counter).expect("counter encodes"),
            )),
            ver: Hlc {
                wall_ms,
                logical: 0,
                node,
            },
            expires_at_ms: None,
        };
        (key_bytes, rec)
    };
    let one = |writer: u64| PnCounter::local_delta(WriterId::new(NodeId::from(writer), 1), 1);

    // Key 1: a's counter already holds b's increment.
    let (adopted_key, b_adopted) = record(1, &one(1), 1, node_b);
    let (_, a_adopted) = record(1, &one(1).merge(&one(2)), 2, node_a);
    // Key 2: each side holds only its own increment.
    let (minted_key, b_minted) = record(2, &one(3), 1, node_b);
    let (_, a_minted) = record(2, &one(4), 2, node_a);
    shard_b.apply_remote_batch(vec![b_adopted, b_minted]).await;
    shard_a
        .apply_remote_batch(vec![a_adopted.clone(), a_minted.clone()])
        .await;

    let repairs = apply_repairs(
        cluster_b.mesh(),
        &shard_b,
        &name,
        node_a,
        vec![adopted_key.clone(), minted_key.clone()],
        vec![adopted_key.clone(), minted_key.clone()],
        Vec::new(),
        true,
    )
    .await;
    assert!(repairs.complete, "a reachable peer answers every pull");

    let stored = shard_b.records_for(vec![adopted_key, minted_key]).await;
    let (adopted, minted) = (&stored[0], &stored[1]);
    assert_eq!(
        adopted.ver, a_adopted.ver,
        "b adopts a's record for key 1 as is"
    );
    assert!(
        minted.ver.node.is_merge_derived(),
        "key 2 merges to a minted version"
    );
    let pulled = wire_record_len(name.len(), &a_adopted) + wire_record_len(name.len(), &a_minted);
    assert_eq!(
        repairs.bytes_moved,
        pulled + wire_record_len(name.len(), minted),
        "both keys are pulled and only the minted one is pushed back"
    );
    assert_eq!(
        cache_b.get(&1u32).await.map(|c| c.value()),
        Some(2),
        "b's counter for key 1 holds both increments"
    );
}

/// Pins that `apply_repairs`'s pushed/pulled byte count matches
/// `wire_record_len` computed independently for one pushed and one
/// pulled record between two real nodes.
#[tokio::test]
async fn apply_repairs_reports_the_bytes_it_moved() {
    use crate::cluster::test_support::{loopback_config, registered_shard, wait_for_peer_count};
    use crate::store::Mode;

    let config = loopback_config();
    let cluster_a = Cluster::builder("cluster-it-apply-repairs-bytes")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let node_a = cluster_a.node_id();
    cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");

    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-apply-repairs-bytes")
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_b, 1).await;
    let cache_b = tokio::time::timeout(
        Duration::from_secs(20),
        cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
    )
    .await
    .expect("open completes within the state-transfer budget")
    .expect("b opens");

    let name = SmolStr::new("users");
    let shard_a = registered_shard(&cluster_a, &name);
    let shard_b = registered_shard(&cluster_b, &name);

    let (pull_key_bytes, pull_rec) = encode_test_record(1, "a-value", node_a);
    shard_a.apply_remote(pull_rec.clone()).await;
    let (push_key_bytes, push_rec) = encode_test_record(2, "b-value", cluster_b.node_id());
    shard_b.apply_remote(push_rec.clone()).await;

    let expected = wire_record_len(name.len(), &push_rec) + wire_record_len(name.len(), &pull_rec);

    let repairs = apply_repairs(
        cluster_b.mesh(),
        &shard_b,
        &name,
        node_a,
        vec![push_key_bytes],
        vec![pull_key_bytes],
        Vec::new(),
        false,
    )
    .await;
    assert!(repairs.complete, "a reachable peer answers every pull");
    let bytes_moved = repairs.bytes_moved;

    assert_eq!(
        bytes_moved, expected,
        "the byte count covers exactly the pushed and pulled record, no more and no less"
    );
    assert_eq!(
        cache_b.get(&1u32).await,
        Some("a-value".to_string()),
        "the pull side of apply_repairs actually landed the record it counted"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

/// A pull, by key or by hash, that the peer never answers leaves the
/// round's repairs incomplete, and so does a push to a peer missing
/// from the mesh, whose records never leave this node.
#[tokio::test]
async fn apply_repairs_reports_incomplete_when_a_peer_is_out_of_reach() {
    use crate::cluster::test_support::{loopback_config, registered_shard};
    use crate::store::Mode;

    let name = SmolStr::new("users");
    let cluster = Cluster::builder("cluster-it-apply-repairs-unanswered")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("cluster builds alone");
    cluster
        .cache::<u32, String>(name.clone())
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("opens alone");
    let shard = registered_shard(&cluster, &name);
    let (key, rec) = encode_test_record(1, "held", cluster.node_id());
    shard.apply_remote(rec).await;
    // A node id gossip never named: every request to it fails at once,
    // as one to a peer a failure-detector flap just dropped does.
    let gone = NodeId::from(u64::MAX);

    let by_key = apply_repairs(
        cluster.mesh(),
        &shard,
        &name,
        gone,
        Vec::new(),
        vec![key.clone()],
        Vec::new(),
        false,
    )
    .await;
    assert!(!by_key.complete, "an unanswered pull by key is incomplete");

    let by_hash = apply_repairs(
        cluster.mesh(),
        &shard,
        &name,
        gone,
        Vec::new(),
        Vec::new(),
        vec![(0, vec![1])],
        true,
    )
    .await;
    assert!(
        !by_hash.complete,
        "an unanswered pull by hash is incomplete"
    );

    let push_only = apply_repairs(
        cluster.mesh(),
        &shard,
        &name,
        gone,
        vec![key],
        Vec::new(),
        Vec::new(),
        false,
    )
    .await;
    assert!(
        !push_only.complete,
        "a push to a peer missing from the mesh is incomplete"
    );
    assert_eq!(
        push_only.bytes_moved, 0,
        "a push whose frames reached no outbox moved nothing"
    );

    cluster.shutdown().await;
}

/// Reads the `sundog_ae_repaired_total` the recorder saw for `cache`.
#[cfg(not(feature = "sim"))]
fn repaired_total(snapshotter: &metrics_util::debugging::Snapshotter, cache: &str) -> u64 {
    use metrics_util::MetricKind;
    use metrics_util::debugging::DebugValue;

    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(key, ..)| {
            key.kind() == MetricKind::Counter
                && key.key().name() == "sundog_ae_repaired_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "cache" && label.value() == cache)
        })
        .map_or(0, |(.., value)| match value {
            DebugValue::Counter(n) => n,
            other => panic!("a counter, not {other:?}"),
        })
}

/// A push counts the records of the frames the outbox took, not the records
/// it was asked to send: a one-slot outbox toward a peer that never answers
/// drops most of forty frames, and the drops are no repair.
#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn enqueue_pushed_counts_only_the_frames_the_outbox_takes() {
    use crate::config::ClusterConfig;
    use crate::net::test_support::{BucketPullHandler, peer_at};

    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let config = ClusterConfig {
        outbox_capacity: 1,
        ..ClusterConfig::default()
    };
    let (mesh, _inbound) = Mesh::spawn(
        "127.0.0.1:0".parse().expect("valid loopback addr"),
        NodeId::from(1),
        1,
        &config,
        Arc::new(BucketPullHandler::default()),
    )
    .await
    .expect("bind loopback");
    // Nothing listens there: the writer never drains the outbox.
    let silent = std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    mesh.update_peers(vec![peer_at(NodeId::from(2), silent)]);

    // One record per frame: each is over half the batch budget.
    let records: Vec<WireRecord> = (0..40u8)
        .map(|k| {
            let (_, mut rec) = encode_test_record(u32::from(k), "", NodeId::from(1));
            rec.value = Some(Bytes::from(vec![k; wire::REPLICATE_BATCH_BUDGET / 2 + 1]));
            rec
        })
        .collect();
    let offered_bytes = records_wire_len(&SmolStr::new("it"), &records);
    let mut tally = RepairTally::default();
    let all_queued = enqueue_pushed(
        &mesh,
        NodeId::from(2),
        &SmolStr::new("it"),
        records,
        &mut tally,
    );

    assert!(!all_queued, "a full outbox takes no more frames");
    assert!(
        (1..40).contains(&tally.repaired),
        "some frames queue and some drop: {}",
        tally.repaired
    );
    assert_eq!(
        tally.bytes_moved,
        offered_bytes / 40 * tally.repaired,
        "the bytes are those of the queued frames"
    );
    assert_eq!(
        repaired_total(&snapshotter, "it"),
        tally.repaired,
        "the counter reads what the outbox took"
    );
    mesh.shutdown().await;
}

/// A pull counts what the shard takes in: a record stamped past the
/// receiver's `max_clock_skew` is refused, moves no bytes and is no repair,
/// and the same record offered again in the next round is not one either.
#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn apply_repairs_counts_no_pulled_record_the_receiver_refuses() {
    use crate::cluster::test_support::{loopback_config, registered_shard, wait_for_peer_count};
    use crate::store::Mode;

    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let name = SmolStr::new("users");
    let cluster_a = Cluster::builder("cluster-it-ae-refused")
        .seeds(std::iter::empty())
        .config(loopback_config().with(|c| c.max_clock_skew = None))
        .build()
        .await
        .expect("node a builds");
    let node_a = cluster_a.node_id();
    cluster_a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-ae-refused")
        .seeds([gossip_a])
        .config(loopback_config().with(|c| c.max_clock_skew = Some(Duration::from_secs(1))))
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_b, 1).await;
    tokio::time::timeout(
        Duration::from_secs(20),
        cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
    )
    .await
    .expect("open completes")
    .expect("b opens");
    let shard_a = registered_shard(&cluster_a, &name);
    let shard_b = registered_shard(&cluster_b, &name);

    let (ahead_key, mut ahead) = encode_test_record(1, "ahead", node_a);
    ahead.ver.wall_ms = crate::store::now_ms() + 3_600_000;
    shard_a.apply_remote(ahead).await;
    let (fine_key, fine) = encode_test_record(2, "fine", node_a);
    shard_a.apply_remote(fine.clone()).await;

    let pull = |keys: Vec<Bytes>| {
        apply_repairs(
            cluster_b.mesh(),
            &shard_b,
            &name,
            node_a,
            Vec::new(),
            keys,
            Vec::new(),
            false,
        )
    };
    let repairs = pull(vec![ahead_key.clone()]).await;
    assert!(repairs.complete, "a reachable peer answers the pull");
    assert!(
        shard_b
            .records_for(vec![ahead_key.clone()])
            .await
            .is_empty(),
        "b refuses the record stamped an hour ahead"
    );
    assert_eq!(repairs.bytes_moved, 0, "a refused record moves no bytes");
    assert_eq!(repaired_total(&snapshotter, "users"), 0);
    assert_eq!(
        pull(vec![ahead_key]).await.bytes_moved,
        0,
        "nor on the next round"
    );

    let both = pull(vec![fine_key]).await;
    assert_eq!(
        both.bytes_moved,
        wire_record_len(name.len(), &fine),
        "a record the shard takes in counts its bytes"
    );
    assert_eq!(repaired_total(&snapshotter, "users"), 1);

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}

/// The dirty-peer hint is per cache: one cache's scheduler picking first
/// leaves the mark for the cache whose overflow set it.
#[cfg(not(feature = "sim"))]
#[tokio::test]
async fn a_dirty_peer_is_targeted_first_by_every_cache_not_only_the_first_to_pick() {
    use crate::cluster::test_support::{loopback_config, registered_shard, wait_for_peer_count};
    use crate::store::Mode;

    let config = crate::config::ClusterConfig {
        ae_interval: Duration::from_secs(3600),
        tombstone_ttl: Duration::from_secs(3600 * 4),
        ..loopback_config()
    };
    let cluster_a = Cluster::builder("cluster-it-dirty-hint")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    for name in ["cache-a", "cache-b"] {
        cluster_a
            .cache::<u32, String>(name)
            .mode(Mode::Replicated)
            .open()
            .await
            .expect("opens");
    }
    let gossip_a = cluster_a.inner.membership.local_peer().gossip_addr;
    let cluster_b = Cluster::builder("cluster-it-dirty-hint")
        .seeds([gossip_a])
        .config(config)
        .build()
        .await
        .expect("node b builds");
    wait_for_peer_count(&cluster_a, 1).await;
    let node_b = cluster_b.node_id();
    let (name_a, name_b) = (SmolStr::new("cache-a"), SmolStr::new("cache-b"));
    let shard_a = registered_shard(&cluster_a, &name_a);
    let shard_b = registered_shard(&cluster_a, &name_b);
    let watch_a = cluster_a.mesh().watch_dirty(&name_a);
    let watch_b = cluster_a.mesh().watch_dirty(&name_b);
    cluster_a.mesh().mark_dirty(node_b);
    let live = cluster_a.live_peer_ids();

    // Cache b's scheduler wakes first and takes the mark for itself.
    assert_eq!(
        pick_peer(&cluster_a, &shard_b, &name_b, live.clone()),
        Some((node_b, true))
    );
    // Cache a overflowed too: its own pick still sees the mark.
    assert_eq!(
        pick_peer(&cluster_a, &shard_a, &name_a, live.clone()),
        Some((node_b, true)),
        "cache a's scheduler targets the dirty peer, not a random live one"
    );
    // Both consumed theirs: the next pick is a plain live one.
    assert_eq!(
        pick_peer(&cluster_a, &shard_a, &name_a, live),
        Some((node_b, false))
    );
    drop((watch_a, watch_b));
    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
}
