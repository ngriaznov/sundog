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

    cluster.shutdown().await;
}
