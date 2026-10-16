//! The mixed-version scenarios that run the previous release beside this one.

use crate::container_util::{
    DISTRIBUTED_RUST_LOG, Node, PREVIOUS_RELEASE_PROTOCOL, build_previous_testnode, build_testnode,
    eventually, eventually_reporting, eventually_with_logs, seed, wait_for_peers,
};
use crate::distributed::{
    collect_node_ids, fetch_mismatches, kv_range, ownership_snapshot_matches, sample_kv_entries,
    sum_counts,
};
use crate::support::{CONVERGE_WAIT, scrape_metric};
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

/// A rolling upgrade in miniature: the previous release's node and this
/// one's share a cluster, each donates to and repairs the other, and every
/// message the old node receives is one it can decode.
#[tokio::test]
async fn the_previous_release_and_this_one_interoperate_in_both_roles() {
    const ENTRIES: u32 = 5_000;

    require_containers!();

    let previous = build_previous_testnode();
    let net = Arc::new(Network::new_network());
    // The old node originates the data; the new node joins it.
    let old = Node::spawn_binary(&net, "mixed-cluster", "n1", &[], &[], previous).await;
    old.fill(ENTRIES).await.expect("bulk fill on the old node");
    let new = Node::spawn(&net, "mixed-cluster", "n2", &[&seed("n1")]).await;
    wait_for_peers(&[&old, &new], 1).await;
    eventually(Duration::from_secs(60), || async {
        new.count().await == Ok(ENTRIES as usize)
    })
    .await;

    // Live replication both ways.
    new.put("from-new", "v").await.expect("put on the new node");
    old.put("from-old", "v").await.expect("put on the old node");
    eventually(Duration::from_secs(30), || async {
        old.get("from-new").await == Ok(Some("v".to_string()))
            && new.get("from-old").await == Ok(Some("v".to_string()))
    })
    .await;

    // Anti-entropy both ways: a copy dropped on either side comes back from
    // the other, the old node initiating rounds the new one answers in the
    // old shapes, and the new node initiating rounds the old one serves.
    old.drop_key("k100").await.expect("drop on the old node");
    new.drop_key("k200").await.expect("drop on the new node");
    eventually(Duration::from_secs(60), || async {
        old.get("k100").await == Ok(Some("v100".to_string()))
            && new.get("k200").await == Ok(Some("v200".to_string()))
    })
    .await;

    // A second old node joins with the new node as its only seed, so the
    // new node is its donor.
    let old2 = Node::spawn_binary(&net, "mixed-cluster", "n3", &[&seed("n2")], &[], previous).await;
    wait_for_peers(&[&old, &new, &old2], 2).await;
    eventually(Duration::from_secs(60), || async {
        old2.count().await == Ok(ENTRIES as usize + 2)
    })
    .await;

    old.stop().await.expect("old node stops");
    new.stop().await.expect("new node stops");
    old2.stop().await.expect("second old node stops");
    net.close().await.expect("network closes");
}

/// An observer joins gossip beside a mixed-version `Mode::Distributed`
/// cluster and stays invisible to it: no member counts it as a peer, so
/// ownership across the previous release's node and this release's is
/// untouched. The observer sees all three members live, reports each member
/// a share of the parts that is neither empty nor the whole space (a whole
/// number of buckets while the previous release predates part ownership),
/// with every part owned twice across the shares, and follows a
/// graceful leave: the departed node counts as left and the survivors split
/// the parts between them.
#[tokio::test]
async fn an_observer_beside_the_previous_release_stays_invisible() {
    const OWNERS: u8 = 2;
    const CLUSTER: &str = "observer-mixed-cluster";
    const WATCH_WAIT: Duration = Duration::from_secs(60);
    let part_count = sundog::store::PART_COUNT as u64;
    let part_space = sundog::store::BUCKET_COUNT as u64 * part_count;

    require_containers!();

    let previous = build_previous_testnode();
    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "distributed"),
        ("SUNDOG_TESTNODE_OWNERS", "2"),
    ];
    let old = Node::spawn_binary(&net, CLUSTER, "n1", &[], &env, previous).await;
    let n2 = Node::spawn_with_env(&net, CLUSTER, "n2", &[&seed("n1")], &env).await;
    let n3 = Node::spawn_with_env(&net, CLUSTER, "n3", &[&seed("n1")], &env).await;
    let observer = Node::spawn_observer(&net, CLUSTER, "obs", &[&seed("n1")]).await;
    let members = [&old, &n2, &n3];

    // The observer shows three live members and nobody else, while every
    // member still counts exactly its two peers: the observer is no peer.
    eventually_reporting(
        "the observer sees three live members",
        WATCH_WAIT,
        &[&old, &n2, &n3, &observer],
        || async {
            match observer.members().await.as_deref() {
                Ok("live=3 departing=0 left=0 down=0 anon=0") => Ok(()),
                other => Err(format!("members reply {other:?}")),
            }
        },
    )
    .await;
    wait_for_peers(&members, 2).await;

    // Every share lies strictly between none and all of the parts, which
    // only holds while all three members, the previous release's included,
    // are eligible, and the shares count every part `OWNERS` times. While
    // the previous release predates part ownership the observer ranks whole
    // buckets, so each share is also a whole number of buckets; once every
    // member supports part ownership it ranks single parts and the shares
    // need not fall on bucket boundaries.
    let bucket_ranked = !sundog::wire::peer_supports(
        PREVIOUS_RELEASE_PROTOCOL,
        sundog::wire::PROTOCOL_PART_OWNERSHIP,
    );
    let ids = collect_node_ids(&members).await;
    eventually_reporting(
        "the shares count every part twice across all three members",
        WATCH_WAIT,
        &[&old, &n2, &n3, &observer],
        || async {
            let mut sum = 0;
            let mut seen = Vec::new();
            for &id in &ids {
                let reply = observer.share("it", OWNERS, id).await;
                seen.push(format!("{id}: {reply:?}"));
                if let Ok(Some(share)) = reply
                    && share > 0
                    && share < part_space
                    && (!bucket_ranked || share % part_count == 0)
                {
                    sum += share;
                }
            }
            if sum == part_space * u64::from(OWNERS) {
                Ok(())
            } else {
                Err(format!("sum {sum}, shares [{}]", seen.join(", ")))
            }
        },
    )
    .await;
    wait_for_peers(&members, 2).await;

    // A graceful leave: the observer counts the node as left and the two
    // survivors own every part twice over, which is all of them each.
    let survivors = [&old, &n3];
    let survivor_ids = [ids[0], ids[2]];
    n2.stop().await.expect("n2 stops");
    eventually_reporting(
        "the leave reaches the observer",
        WATCH_WAIT,
        &[&old, &n3, &observer],
        || async {
            let reply = observer.members().await;
            let mut seen = vec![format!("members {reply:?}")];
            let mut ok = reply.as_deref() == Ok("live=2 departing=0 left=1 down=0 anon=0");
            for id in survivor_ids {
                let share = observer.share("it", OWNERS, id).await;
                ok &= share == Ok(Some(part_space));
                seen.push(format!("{id}: {share:?}"));
            }
            if ok { Ok(()) } else { Err(seen.join(", ")) }
        },
    )
    .await;
    wait_for_peers(&survivors, 1).await;

    observer.stop().await.expect("the observer stops");
    old.stop().await.expect("the previous-release node stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// Chunked-pull-with-independent-release interop
/// (`Msg::StBucketDone`/`Msg::StBucketAck`, gated on
/// `wire::PROTOCOL_ST_BUCKET_DONE_ACK`) across a mixed-version
/// `Mode::Distributed` cluster in both donor roles: two established nodes
/// hold every key, then a third joins on `donor_bin`'s protocol and pulls
/// its share. `new_is_donor` swaps which release runs the donor pair versus
/// the joiner. Either direction must converge on every key, fetchable
/// everywhere, since a premature group termination would leave a key
/// missing and a duplicate release is harmless by construction.
async fn distributed_rebalance_interoperates_between_releases(new_is_donor: bool) {
    const OWNERS: u8 = 2;
    const FILL_KEYS: u32 = 1_500;
    const SAMPLE_SIZE: usize = 100;
    const CLUSTER: &str = "dist-mixed-cluster";
    const REBALANCE_WAIT: Duration = Duration::from_secs(240);
    const SETTLE_WAIT: Duration = Duration::from_secs(180);

    require_containers!();

    let previous = build_previous_testnode();
    let current = build_testnode();
    let (donor_bin, joiner_bin) = if new_is_donor {
        (current, previous)
    } else {
        (previous, current)
    };

    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "distributed"),
        ("SUNDOG_TESTNODE_OWNERS", "2"),
        // Debug on the cluster state machine, so a timeout has more than
        // "condition not met" to go on.
        ("RUST_LOG", DISTRIBUTED_RUST_LOG),
    ];
    let n1 = Node::spawn_binary(&net, CLUSTER, "n1", &[], &env, donor_bin).await;
    let n2 = Node::spawn_binary(&net, CLUSTER, "n2", &[&seed("n1")], &env, donor_bin).await;
    wait_for_peers(&[&n1, &n2], 1).await;

    n1.fill(FILL_KEYS).await.expect("bulk fill succeeds");
    for (node, id) in [&n1, &n2]
        .into_iter()
        .zip(collect_node_ids(&[&n1, &n2]).await)
    {
        eprintln!("node {} has id {id}", node.name());
    }
    eventually_with_logs(Duration::from_secs(60), &[&n1, &n2], || async {
        sum_counts(&[&n1, &n2]).await == Some(usize::from(OWNERS) * FILL_KEYS as usize)
    })
    .await;

    let joiner = Node::spawn_binary(
        &net,
        CLUSTER,
        "n3",
        &[&seed("n1"), &seed("n2")],
        &env,
        joiner_bin,
    )
    .await;
    let nodes = [&n1, &n2, &joiner];
    wait_for_peers(&nodes, 2).await;

    // The joiner's pull has landed. This checkout's joiner reports it via
    // `sundog_rebalance_parts_total{direction="in"}`; a previous-release
    // joiner serves no metrics, so the donors report `direction="served"`
    // instead. The settle wait and fetch check below assert both roles:
    // every key on exactly `OWNERS` nodes, fetchable everywhere.
    eventually_with_logs(REBALANCE_WAIT, &nodes, || async {
        if new_is_donor {
            scrape_metric(&n1, "sundog_rebalance_parts_total", ("direction", "served")).await > 0
                && scrape_metric(&n2, "sundog_rebalance_parts_total", ("direction", "served")).await
                    > 0
        } else {
            scrape_metric(&joiner, "sundog_rebalance_parts_total", ("direction", "in")).await > 0
        }
    })
    .await;

    let ids = collect_node_ids(&nodes).await;
    let tagged: Vec<(&Node, u64)> = nodes.iter().copied().zip(ids).collect();
    let sample = sample_kv_entries(0x6011_ec7e, FILL_KEYS, SAMPLE_SIZE);

    eventually_with_logs(SETTLE_WAIT, &nodes, || async {
        if sum_counts(&nodes).await != Some(usize::from(OWNERS) * FILL_KEYS as usize) {
            return false;
        }
        ownership_snapshot_matches(tagged[0].0, &tagged, &sample, usize::from(OWNERS)).await
    })
    .await;

    let mismatches = fetch_mismatches(&nodes, &sample).await;
    assert!(
        mismatches.is_empty(),
        "every sampled key must be fetchable with the right value from every node, with \
         no bucket lost to a premature group termination: {mismatches:?}"
    );

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    joiner.stop().await.expect("joiner stops");
    net.close().await.expect("network closes");
}

/// [`distributed_rebalance_interoperates_between_releases`], an N donor serving an N-1 requester's rebalance pull.
#[tokio::test]
async fn distributed_rebalance_interoperates_with_a_current_donor_and_a_previous_release_requester()
{
    distributed_rebalance_interoperates_between_releases(true).await;
}

/// [`distributed_rebalance_interoperates_between_releases`], an N-1 donor serving an N requester's rebalance pull.
#[tokio::test]
async fn distributed_rebalance_interoperates_with_a_previous_release_donor_and_a_current_requester()
{
    distributed_rebalance_interoperates_between_releases(false).await;
}

/// The last step of a rolling upgrade: once the previous-release node leaves,
/// the survivors rank single parts and every key ends on exactly `OWNERS` of
/// them, fetchable from each. While the previous release predates part
/// ownership, every current node ranks whole buckets until it leaves, and the
/// leave moves most parts to new owners at once.
#[tokio::test]
async fn distributed_ownership_ranks_parts_once_the_last_previous_release_node_leaves() {
    const OWNERS: u8 = 2;
    const FILL_KEYS: u32 = 1_500;
    const SAMPLE_SIZE: usize = 100;
    const CLUSTER: &str = "dist-switch-cluster";
    const SWITCH_WAIT: Duration = Duration::from_secs(120);
    const SETTLE_WAIT: Duration = Duration::from_secs(240);
    let part_space = (sundog::store::BUCKET_COUNT * sundog::store::PART_COUNT) as u64;
    let part_count = sundog::store::PART_COUNT as u64;

    require_containers!();

    let previous = build_previous_testnode();
    let current = build_testnode();
    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "distributed"),
        ("SUNDOG_TESTNODE_OWNERS", "2"),
        ("RUST_LOG", DISTRIBUTED_RUST_LOG),
    ];
    let old = Node::spawn_binary(&net, CLUSTER, "n1", &[], &env, previous).await;
    let n2 = Node::spawn_binary(&net, CLUSTER, "n2", &[&seed("n1")], &env, current).await;
    let n3 = Node::spawn_binary(&net, CLUSTER, "n3", &[&seed("n1")], &env, current).await;
    let n4 = Node::spawn_binary(&net, CLUSTER, "n4", &[&seed("n1")], &env, current).await;
    let everyone = [&old, &n2, &n3, &n4];
    wait_for_peers(&everyone, 3).await;

    n2.fill(FILL_KEYS).await.expect("bulk fill succeeds");
    eventually_with_logs(SETTLE_WAIT, &everyone, || async {
        sum_counts(&everyone).await == Some(usize::from(OWNERS) * FILL_KEYS as usize)
    })
    .await;
    // While the previous release ranks whole buckets, each current node owns
    // whole buckets' worth of parts.
    let survivors = [&n2, &n3, &n4];
    let bucket_ranked = !sundog::wire::peer_supports(
        PREVIOUS_RELEASE_PROTOCOL,
        sundog::wire::PROTOCOL_PART_OWNERSHIP,
    );
    if bucket_ranked {
        for node in survivors {
            let owned = scrape_metric(node, "sundog_owned_parts", ("cache", "it")).await;
            assert_eq!(
                owned % part_count,
                0,
                "{} owns whole buckets while a previous-release node is eligible",
                node.name()
            );
        }
    }

    old.stop().await.expect("the previous-release node stops");
    wait_for_peers(&survivors, 2).await;

    // Ranking parts: the survivors' shares fall off bucket boundaries, and
    // together they assign every part `OWNERS` times.
    eventually_with_logs(SWITCH_WAIT, &survivors, || async {
        let mut sum = 0;
        let mut off_bucket_boundary = false;
        for node in survivors {
            let owned = scrape_metric(node, "sundog_owned_parts", ("cache", "it")).await;
            sum += owned;
            off_bucket_boundary |= owned % part_count != 0;
        }
        sum == part_space * u64::from(OWNERS) && off_bucket_boundary
    })
    .await;

    let ids = collect_node_ids(&survivors).await;
    let tagged: Vec<(&Node, u64)> = survivors.iter().copied().zip(ids).collect();
    let sample = sample_kv_entries(0x5317_c400, FILL_KEYS, SAMPLE_SIZE);
    eventually_with_logs(SETTLE_WAIT, &survivors, || async {
        if sum_counts(&survivors).await != Some(usize::from(OWNERS) * FILL_KEYS as usize) {
            return false;
        }
        ownership_snapshot_matches(tagged[0].0, &tagged, &sample, usize::from(OWNERS)).await
    })
    .await;
    let all_entries = kv_range(FILL_KEYS);
    let mismatches = fetch_mismatches(&survivors, &all_entries).await;
    assert!(
        mismatches.is_empty(),
        "every key stays fetchable once the survivors rank parts: {mismatches:?}"
    );

    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    n4.stop().await.expect("n4 stops");
    net.close().await.expect("network closes");
}

/// A merge on the current release's node stamps its version with a
/// merge-derived node id rather than a real single writer's; this pins that
/// the previous release's node, whose own resolver knows nothing of merges,
/// still stores and serves a record stamped that way, and the two nodes
/// converge on the merged value.
///
/// `new` opens `"it"` with `sundog-testnode`'s interop-only merge resolver
/// (`SUNDOG_TESTNODE_RESOLVER=sum_counter`), which sums two conflicting
/// decimal-string counters on merge rather than picking whichever write is
/// most recent. `old` writes first; `new` then writes a conflicting value of
/// its own, merging it against what it recently replicated from `old` instead of
/// overwriting it. The merged sum differs from both inputs, so `old` ending
/// up there proves it applied the merge-derived-stamped bytes it received
/// rather than coincidentally already holding the right value.
#[tokio::test]
async fn the_previous_release_stores_and_serves_a_merge_derived_version_from_the_current_node() {
    const KEY: &str = "counter";
    const OLD_VALUE: &str = "3";
    const NEW_VALUE: &str = "5";
    const MERGED_VALUE: &str = "8";

    require_containers!();

    let previous = build_previous_testnode();
    let net = Arc::new(Network::new_network());
    let old = Node::spawn_binary(&net, "merge-derived-cluster", "n1", &[], &[], previous).await;
    let new = Node::spawn_with_env(
        &net,
        "merge-derived-cluster",
        "n2",
        &[&seed("n1")],
        &[("SUNDOG_TESTNODE_RESOLVER", "sum_counter")],
    )
    .await;
    wait_for_peers(&[&old, &new], 1).await;

    old.put(KEY, OLD_VALUE)
        .await
        .expect("the old node writes the counter first");
    eventually(CONVERGE_WAIT, || async {
        new.get(KEY).await == Ok(Some(OLD_VALUE.to_string()))
    })
    .await;

    // A second, conflicting write on the new node: its own resolver merges it
    // against the value it recently replicated from `old` instead of overwriting
    // it, storing the sum under a merge-derived version and fanning that
    // out to `old`.
    new.put(KEY, NEW_VALUE)
        .await
        .expect("the new node merges its own write in");

    eventually(CONVERGE_WAIT, || async {
        old.get(KEY).await == Ok(Some(MERGED_VALUE.to_string()))
            && new.get(KEY).await == Ok(Some(MERGED_VALUE.to_string()))
    })
    .await;

    old.stop().await.expect("old node stops");
    new.stop().await.expect("new node stops");
    net.close().await.expect("network closes");
}

/// A delete the previous release's node makes drains from this release's
/// node and stays gone. The previous release keeps its tombstone from the
/// moment it lands and offers it to anti-entropy again after this node has
/// collected its own; this node ignores that copy once its retention has
/// passed, so its tombstone count reaches zero and stays there while the old
/// node still offers.
///
/// The test node's `tombstone_ttl` is 20 s and its anti-entropy interval 2 s.
/// The delete is made after this node's first 20 s of start deferral, so the
/// deferral does not mask the collection under test.
#[tokio::test]
async fn a_delete_the_previous_release_makes_drains_from_this_node_and_stays_gone() {
    const TOMBSTONE_TTL: Duration = Duration::from_secs(20);
    const AE_INTERVAL: Duration = Duration::from_secs(2);
    const KEY: &str = "doomed";

    require_containers!();

    let previous = build_previous_testnode();
    let net = Arc::new(Network::new_network());
    let old = Node::spawn_binary(&net, "mixed-delete-cluster", "n1", &[], &[], previous).await;
    let new = Node::spawn(&net, "mixed-delete-cluster", "n2", &[&seed("n1")]).await;
    wait_for_peers(&[&old, &new], 1).await;

    old.put(KEY, "v").await.expect("the old node writes");
    eventually(CONVERGE_WAIT, || async {
        new.get(KEY).await == Ok(Some("v".to_string()))
    })
    .await;

    // Past the new node's start deferral.
    tokio::time::sleep(TOMBSTONE_TTL).await;
    old.del(KEY).await.expect("the old node deletes");

    // The delete reaches this node and is stored: the drain below is not
    // vacuous.
    eventually(CONVERGE_WAIT, || async {
        new.get(KEY).await == Ok(None)
            && scrape_metric(&new, "sundog_tombstones", ("cache", "it")).await > 0
    })
    .await;

    // The tombstone counts its retention from the delete's stamp and leaves
    // this node at the first collection after it.
    eventually(TOMBSTONE_TTL * 4, || async {
        scrape_metric(&new, "sundog_tombstones", ("cache", "it")).await == 0
    })
    .await;

    // The old node still offers its copy for a while; this node ignores it.
    for round in 0..6 {
        tokio::time::sleep(AE_INTERVAL).await;
        assert_eq!(
            scrape_metric(&new, "sundog_tombstones", ("cache", "it")).await,
            0,
            "round {round}: a collected tombstone is not taken again"
        );
        assert_eq!(new.get(KEY).await, Ok(None), "round {round}");
        assert_eq!(old.get(KEY).await, Ok(None), "round {round}");
    }

    old.stop().await.expect("old node stops");
    new.stop().await.expect("new node stops");
    net.close().await.expect("network closes");
}
