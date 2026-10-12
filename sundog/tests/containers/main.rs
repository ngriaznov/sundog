//! Container-backed multi-node scenarios for everything that needs real,
//! separate processes on a real network: membership convergence,
//! replication, tombstones, state transfer, and anti-entropy, exclusively
//! through the `rightsize` crate. See `tests/container_util` for the
//! harness and `sundog-testnode` for the control protocol.
//!
//! Gated on `SUNDOG_CONTAINER_TESTS=1`, an `eprintln!` and early return
//! otherwise, so a plain `cargo test --workspace` run still compiles
//! without a container backend or the musl target. Run:
//!
//! ```text
//! SUNDOG_CONTAINER_TESTS=1 SUNDOG_TEST_BASE_IMAGE=rz-base:local RIGHTSIZE_BACKEND=docker \
//!     cargo test --release -p sundog --test containers -- --test-threads=1
//! ```

#[path = "../container_util/mod.rs"]
mod container_util;

use crate::container_util::{
    DISTRIBUTED_RUST_LOG, METRICS_PORT, Node, PREVIOUS_RELEASE_PROTOCOL, build_previous_testnode,
    build_testnode, eventually, eventually_reporting, eventually_with_logs, seed, wait_for_peers,
};
use crate::distributed::{
    collect_node_ids, fetch_mismatches, kv_entry, kv_range, ownership_snapshot_matches,
    sample_kv_entries, sum_counts,
};
use crate::support::{CONVERGE_WAIT, scrape_metric};
use futures::stream;
use futures::stream::StreamExt as _;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rightsize::{Network, Wait};
use std::sync::Arc;
use std::time::Duration;

/// Returns from the calling test fn unless `SUNDOG_CONTAINER_TESTS=1` is
/// set.
macro_rules! require_containers {
    () => {
        if !$crate::container_util::container_tests_enabled() {
            eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
            return;
        }
    };
}

mod anti_entropy;
mod chaos;
mod distributed;
mod replication;
mod scale;
mod support;

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

/// Sum of [`fill`][Node::fill]'s `k{i}`/`v{i}` UTF-8 byte lengths (key plus
/// value) over `0..count`: the exact formula `sundog-testnode`'s
/// `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` weigher applies per entry, so a RAM
/// budget or spill capacity computed from this holds a known, exact
/// fraction of `fill`'s entries rather than a guess.
fn fill_weight_bytes(count: u32) -> u64 {
    (0..count)
        .map(|i| (format!("k{i}").len() + format!("v{i}").len()) as u64)
        .sum()
}

/// Directory the spill tier lives under inside a spilling node's container;
/// a fresh, empty filesystem every time a container starts, so reusing this
/// same path across a restart never sees the previous container's files.
const SPILL_DIR: &str = "/spill";

/// The `SUNDOG_TESTNODE_*` env vars for a spilling `Mode::Replicated` node:
/// `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` bounds RAM to `ram_budget_bytes` via
/// the byte-counting weigher, and `SUNDOG_TESTNODE_SPILL_DIR`/
/// `..._SPILL_CAPACITY_BYTES`/`..._SPILL_REGION_BYTES` compose a spill tier
/// under [`SPILL_DIR`] sized to `spill_capacity_bytes`/`region_bytes`.
/// `..._SPILL_FLUSH_QUEUE_BYTES` sets the flusher's byte backlog bound to
/// `flush_queue_bytes` explicitly, rather than the default of one region's
/// worth: with `region_bytes` set as small as [`SPILL_REGION_BYTES`] for
/// these tests, that default would bound the backlog far tighter than the
/// disk budget it is meant to protect, refusing hand-offs the tier has
/// ample room for. `warm_reopen` sets `SUNDOG_TESTNODE_WARM_REOPEN`,
/// `true` only for scenarios that preserve the spill dir across a
/// restart to exercise the warm path, `false` elsewhere.
fn spill_node_env(
    ram_budget_bytes: u64,
    spill_capacity_bytes: u64,
    region_bytes: u64,
    flush_queue_bytes: u64,
    warm_reopen: bool,
) -> Vec<(String, String)> {
    vec![
        (
            "SUNDOG_TESTNODE_MAX_CAPACITY_BYTES".to_string(),
            ram_budget_bytes.to_string(),
        ),
        (
            "SUNDOG_TESTNODE_SPILL_DIR".to_string(),
            SPILL_DIR.to_string(),
        ),
        (
            "SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES".to_string(),
            spill_capacity_bytes.to_string(),
        ),
        (
            "SUNDOG_TESTNODE_SPILL_REGION_BYTES".to_string(),
            region_bytes.to_string(),
        ),
        (
            "SUNDOG_TESTNODE_SPILL_FLUSH_QUEUE_BYTES".to_string(),
            flush_queue_bytes.to_string(),
        ),
        (
            "SUNDOG_TESTNODE_WARM_REOPEN".to_string(),
            warm_reopen.to_string(),
        ),
    ]
}

/// Reads `fill_keys` random `k{i}`/`v{i}` entries through `node`,
/// concurrently, and returns a description of every one that came back
/// wrong (empty on full agreement). Concurrent rather than sequential so a
/// large sample stays fast across the container network's per-connection
/// round trip.
async fn sample_mismatches(node: &Node, keys: Vec<u32>) -> Vec<String> {
    stream::iter(keys)
        .map(|i| async move {
            let key = format!("k{i}");
            let expected = format!("v{i}");
            let got = node.get(&key).await;
            (key, expected, got)
        })
        .buffer_unordered(64)
        .filter_map(|(key, expected, got)| async move {
            (got != Ok(Some(expected.clone())))
                .then(|| format!("{key}: expected Ok(Some({expected:?})), got {got:?}"))
        })
        .collect()
        .await
}

/// A tiny RAM budget on node `a`, backed by a spill tier generously sized to
/// hold everything eviction demotes to it, still serves a full 3-node
/// `Mode::Replicated` cluster's writes correctly, and the cluster settles
/// with no anti-entropy repair loop once nothing is left to reconcile:
/// `sundog_ae_repaired_total` never moves on either node absent an actual
/// gap, spilling or not.
#[tokio::test]
async fn replicated_cluster_serves_spilled_entries_and_settles_without_repair_loops() {
    const FILL_COUNT: u32 = 3_000;
    /// Small enough that the computed capacity below (tens of KB) still
    /// clears `SpillConfig::validate`'s `capacity_bytes >= 2 * region_bytes`.
    const SPILL_REGION_BYTES: u64 = 16 * 1024;
    const CLUSTER: &str = "spill-cluster";

    require_containers!();

    // A third holds roughly a third of the fill's entries; the capacity
    // budget below covers every entry's key/value bytes plus a generous
    // per-record on-disk header allowance (the real header is much
    // smaller), doubled again, so eviction never needs to reclaim a region
    // still holding live data.
    let total_weight = fill_weight_bytes(FILL_COUNT);
    let ram_budget_bytes = total_weight / 3;
    let spill_capacity_bytes = (total_weight + u64::from(FILL_COUNT) * 64) * 2;
    let a_env = spill_node_env(
        ram_budget_bytes,
        spill_capacity_bytes,
        SPILL_REGION_BYTES,
        spill_capacity_bytes,
        false,
    );
    let a_env_refs: Vec<(&str, &str)> = a_env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let net = Arc::new(Network::new_network());
    let a = Node::spawn_with_env(&net, CLUSTER, "a", &[], &a_env_refs).await;
    let b = Node::spawn(&net, CLUSTER, "b", &[&seed("a")]).await;
    let c = Node::spawn(&net, CLUSTER, "c", &[&seed("a"), &seed("b")]).await;
    wait_for_peers(&[&a, &b, &c], 2).await;

    b.fill(FILL_COUNT)
        .await
        .expect("bulk fill through b succeeds");
    for node in [&a, &b, &c] {
        eventually(CONVERGE_WAIT, || async {
            node.count().await == Ok(FILL_COUNT as usize)
        })
        .await;
    }

    let spilled = scrape_metric(&a, "sundog_spill_entries", ("cache", "it")).await;
    assert!(
        spilled > 0,
        "a's tiny RAM budget ({ram_budget_bytes} bytes for {total_weight} bytes of fill) should \
         have spilled some of the {FILL_COUNT} entries, got {spilled}"
    );
    // `it` is `Mode::Replicated`, so `SpillTier::keep_resident_when_refused`
    // is set: a refused hand-off leaves its victim resident and retried
    // instead of falling back to a delete, so no entry is ever dropped
    // this way.
    let dropped_queue_full =
        scrape_metric(&a, "sundog_spill_dropped_total", ("reason", "queue_full")).await;
    assert_eq!(
        dropped_queue_full, 0,
        "a Mode::Replicated victim refused by the spill tier is deferred, never deleted, so \
         reason=\"queue_full\" must never be recorded"
    );
    let dropped_deferred =
        scrape_metric(&a, "sundog_spill_dropped_total", ("reason", "deferred")).await;
    eprintln!(
        "a deferred {dropped_deferred} hand-offs the spill tier refused, out of {FILL_COUNT} \
         entries written (backpressure, not a failure: each stays resident until a later \
         eviction pass retries it)"
    );

    let mismatches = sample_mismatches(&a, (0..FILL_COUNT).collect()).await;
    assert!(
        mismatches.is_empty(),
        "every fill key must read correctly through a, resident or spilled: {mismatches:?}"
    );

    // Quiescence check: with nothing left to reconcile, `sundog_ae_repaired_
    // total` must not move across several further anti-entropy intervals. A
    // bounded poll cannot express "nothing happens for a while", so these
    // two fixed windows are a deliberate exception to this file's own
    // bounded-poll rule. sundog-testnode's `ae_interval` is 2s; two 6s
    // windows span three rounds each. Each node's repair count travels as a
    // `(a, b)` pair rather than four similarly-named bindings.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let repaired_before = (
        scrape_metric(&a, "sundog_ae_repaired_total", ("cache", "it")).await,
        scrape_metric(&b, "sundog_ae_repaired_total", ("cache", "it")).await,
    );
    tokio::time::sleep(Duration::from_secs(6)).await;
    let repaired_after = (
        scrape_metric(&a, "sundog_ae_repaired_total", ("cache", "it")).await,
        scrape_metric(&b, "sundog_ae_repaired_total", ("cache", "it")).await,
    );
    assert_eq!(
        repaired_before.0, repaired_after.0,
        "a's repair counter must settle once the cluster has nothing left to reconcile, spilling \
         or not"
    );
    assert_eq!(
        repaired_before.1, repaired_after.1,
        "b's repair counter must settle once the cluster has nothing left to reconcile"
    );

    let io_errors = scrape_metric(&a, "sundog_spill_reads_total", ("outcome", "io_error")).await;
    assert_eq!(
        io_errors, 0,
        "no spilled-value read on a should ever hit a disk io_error in this run"
    );

    // Concurrent, not sequential: each `stop()` waits for a graceful leave,
    // so stopping three nodes one after another would triple that wait.
    let (a_stopped, b_stopped, c_stopped) = tokio::join!(a.stop(), b.stop(), c.stop());
    a_stopped.expect("a stops");
    b_stopped.expect("b stops");
    c_stopped.expect("c stops");
    net.close().await.expect("network closes");
}

/// A spilling node's tier is discarded, not restored, across a restart: the
/// freshly reopened tier starts at zero `sundog_spill_entries`, then the node
/// rewarms to a full copy via state transfer from its still-live peers and,
/// once its RAM budget is exceeded again, resumes serving some of its reads
/// from disk.
#[tokio::test]
async fn spilling_node_survives_a_restart_and_rewarms_from_peers() {
    const FILL_COUNT: u32 = 3_000;
    const SPILL_REGION_BYTES: u64 = 16 * 1024;
    const SAMPLE_SIZE: usize = 300;
    const CLUSTER: &str = "spill-restart-cluster";

    require_containers!();

    let total_weight = fill_weight_bytes(FILL_COUNT);
    let ram_budget_bytes = total_weight / 3;
    let spill_capacity_bytes = (total_weight + u64::from(FILL_COUNT) * 64) * 2;
    let a_env = spill_node_env(
        ram_budget_bytes,
        spill_capacity_bytes,
        SPILL_REGION_BYTES,
        spill_capacity_bytes,
        false,
    );
    let a_env_refs: Vec<(&str, &str)> = a_env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let net = Arc::new(Network::new_network());
    let a = Node::spawn_with_env(&net, CLUSTER, "a", &[], &a_env_refs).await;
    let b = Node::spawn(&net, CLUSTER, "b", &[&seed("a")]).await;
    let c = Node::spawn(&net, CLUSTER, "c", &[&seed("a"), &seed("b")]).await;
    wait_for_peers(&[&a, &b, &c], 2).await;

    b.fill(FILL_COUNT)
        .await
        .expect("bulk fill through b succeeds");
    for node in [&a, &b, &c] {
        eventually(CONVERGE_WAIT, || async {
            node.count().await == Ok(FILL_COUNT as usize)
        })
        .await;
    }
    let spilled_before_restart = scrape_metric(&a, "sundog_spill_entries", ("cache", "it")).await;
    assert!(
        spilled_before_restart > 0,
        "a should already be spilling some of the {FILL_COUNT} entries before the restart"
    );

    a.stop().await.expect("a stops ahead of its restart");
    wait_for_peers(&[&b, &c], 1).await;

    // A fresh container filesystem under the same alias and the same spill
    // dir: the point is that `attach_spill` always opens a brand new tier
    // from scratch (`SpillTier::open` "recreates them from scratch every
    // time"), so this stands in for that discard-and-reopen either way.
    //
    // Waits for `GET /metrics` rather than the usual `testnode-ready` log
    // line: `ClusterBuilder::prometheus_listen`'s HTTP server comes up
    // during `Cluster::builder(..).build()`, well before the `Mode::
    // Replicated` cache's state transfer even starts, let alone finishes
    // re-filling and re-spilling past the RAM budget. Racing a scrape
    // against `testnode-ready` (printed only once state transfer has
    // already landed) would never observe a genuine zero; this does.
    let a = Node::spawn_with_env_and_wait(
        &net,
        CLUSTER,
        "a",
        &[&seed("b"), &seed("c")],
        &a_env_refs,
        Wait::for_http("/metrics").for_port(METRICS_PORT),
    )
    .await;

    let spill_entries_at_start = scrape_metric(&a, "sundog_spill_entries", ("cache", "it")).await;
    assert_eq!(
        spill_entries_at_start, 0,
        "a freshly reopened spill tier starts empty, discarding whatever was on disk before the \
         restart rather than resuming it"
    );

    // The control port is not necessarily up yet at this point (state
    // transfer, then `churn`'s own open, run before it binds), so `count`
    // failing early is expected and `eventually` keeps retrying.
    eventually(CONVERGE_WAIT, || async {
        a.count().await == Ok(FILL_COUNT as usize)
    })
    .await;

    // Once the RAM budget is exceeded again by the rewarmed content, a
    // should resume spilling.
    eventually(Duration::from_secs(30), || async {
        scrape_metric(&a, "sundog_spill_entries", ("cache", "it")).await > 0
    })
    .await;

    let mut rng = StdRng::seed_from_u64(0x5111_2357);
    let sample: Vec<u32> = (0..SAMPLE_SIZE)
        .map(|_| rng.random_range(0..FILL_COUNT))
        .collect();
    let mismatches = sample_mismatches(&a, sample).await;
    assert!(
        mismatches.is_empty(),
        "every sampled key must read correctly through the restarted a, resident or spilled: \
         {mismatches:?}"
    );

    // Concurrent, not sequential: see the first scenario's own comment on
    // why stopping three nodes one after another wastes two full shutdown
    // grace periods for nothing.
    let (a_stopped, b_stopped, c_stopped) = tokio::join!(a.stop(), b.stop(), c.stop());
    a_stopped.expect("a stops");
    b_stopped.expect("b stops");
    c_stopped.expect("c stops");
    net.close().await.expect("network closes");
}

/// The warm-reopen companion to
/// `spilling_node_survives_a_restart_and_rewarms_from_peers`: that scenario's
/// restart always lands cold since its filesystem is fresh, while this one
/// bind-mounts a host directory so the spill dir survives the restart and
/// asserts it lands warm (`outcome="warm"`, never `"cold_fallback"`), with
/// `sundog_spill_reopen_records_total` close to what was spilled before.
#[tokio::test]
async fn spilling_node_reopens_warm_across_a_restart_with_its_spill_dir_preserved() {
    const FILL_COUNT: u32 = 3_000;
    const SPILL_REGION_BYTES: u64 = 16 * 1024;
    const CLUSTER: &str = "spill-warm-reopen-cluster";

    require_containers!();

    let host_spill_dir = std::env::temp_dir().join(format!(
        "sundog-it-containers-spill-warm-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&host_spill_dir).expect("create the host-side spill dir to mount in");
    let host_spill_dir = host_spill_dir.to_string_lossy().into_owned();
    let mounts = [(host_spill_dir.as_str(), SPILL_DIR)];

    let total_weight = fill_weight_bytes(FILL_COUNT);
    let ram_budget_bytes = total_weight / 3;
    let spill_capacity_bytes = (total_weight + u64::from(FILL_COUNT) * 64) * 2;
    let a_env = spill_node_env(
        ram_budget_bytes,
        spill_capacity_bytes,
        SPILL_REGION_BYTES,
        spill_capacity_bytes,
        true,
    );
    let a_env_refs: Vec<(&str, &str)> = a_env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let net = Arc::new(Network::new_network());
    let a = Node::spawn_with_env_mounts_and_wait(
        &net,
        CLUSTER,
        "a",
        &[],
        &a_env_refs,
        &mounts,
        Wait::for_log_message("testnode-ready", 1),
    )
    .await;
    let b = Node::spawn(&net, CLUSTER, "b", &[&seed("a")]).await;
    let c = Node::spawn(&net, CLUSTER, "c", &[&seed("a"), &seed("b")]).await;
    wait_for_peers(&[&a, &b, &c], 2).await;

    b.fill(FILL_COUNT)
        .await
        .expect("bulk fill through b succeeds");
    for node in [&a, &b, &c] {
        eventually(CONVERGE_WAIT, || async {
            node.count().await == Ok(FILL_COUNT as usize)
        })
        .await;
    }
    let spilled_before_restart = scrape_metric(&a, "sundog_spill_entries", ("cache", "it")).await;
    assert!(
        spilled_before_restart > 0,
        "a should already be spilling some of the {FILL_COUNT} entries before the restart"
    );
    // a's first open, against the still-empty mounted directory, is a cold
    // fallback like any first open.
    assert_eq!(
        scrape_metric(&a, "sundog_spill_reopen_total", ("outcome", "warm")).await,
        0,
        "nothing to warm-reopen before a has ever closed its spill tier"
    );

    a.stop().await.expect("a stops ahead of its restart");
    wait_for_peers(&[&b, &c], 1).await;

    // Same host directory remounted: the region files and checkpoint survive
    // this restart, so `attach_spill` finds a snapshot it trusts and warms up.
    let a = Node::spawn_with_env_mounts_and_wait(
        &net,
        CLUSTER,
        "a",
        &[&seed("b"), &seed("c")],
        &a_env_refs,
        &mounts,
        Wait::for_http("/metrics").for_port(METRICS_PORT),
    )
    .await;

    eventually(CONVERGE_WAIT, || async {
        a.count().await == Ok(FILL_COUNT as usize)
    })
    .await;

    assert_eq!(
        scrape_metric(&a, "sundog_spill_reopen_total", ("outcome", "warm")).await,
        1,
        "the restart's own attach_spill call lands exactly one warm reopen"
    );
    let records_installed =
        scrape_metric(&a, "sundog_spill_reopen_records_total", ("cache", "it")).await;
    assert!(
        records_installed * 4 >= spilled_before_restart * 3,
        "the warm reopen should recover the large majority of what was spilled before the \
         restart ({records_installed} of {spilled_before_restart})"
    );

    let (a_stopped, b_stopped, c_stopped) = tokio::join!(a.stop(), b.stop(), c.stop());
    a_stopped.expect("a stops");
    b_stopped.expect("b stops");
    c_stopped.expect("c stops");
    net.close().await.expect("network closes");
    let _ = std::fs::remove_dir_all(&host_spill_dir);
}

/// A mixed-version case: an N-1 peer answering an N node's eager
/// post-reopen anti-entropy round after a warm reload. `a` runs this
/// checkout with a bind-mounted spill dir; `b` and `c` run the previous
/// release. While `a` is down, `b` and `c` become every bucket's owner and
/// take a batch of writes `a`'s warm replay never observes (replay only
/// reads what was on disk when `a` last closed). `a` restarts, reopens
/// warm (never clearing cold on replay alone), and must pick those writes
/// up through its eager per-co-owner round served by a previous-release peer.
#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one scripted end-to-end scenario")]
async fn distributed_warm_reopen_interoperates_with_a_previous_release_co_owner() {
    const OWNERS: u8 = 2;
    const FILL_KEYS: u32 = 1_500;
    const POST_RESTART_KEYS: u32 = 200;
    const SAMPLE_SIZE: usize = 100;
    const SPILL_REGION_BYTES: u64 = 16 * 1024;
    const CLUSTER: &str = "dist-warm-reopen-mixed-cluster";
    const REBALANCE_WAIT: Duration = Duration::from_secs(240);
    const SETTLE_WAIT: Duration = Duration::from_secs(180);

    require_containers!();

    let previous = build_previous_testnode();
    let host_spill_dir = std::env::temp_dir().join(format!(
        "sundog-it-containers-spill-warm-mixed-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&host_spill_dir).expect("create the host-side spill dir to mount in");
    let host_spill_dir = host_spill_dir.to_string_lossy().into_owned();
    let mounts = [(host_spill_dir.as_str(), SPILL_DIR)];

    // Sized off the whole fill; generous capacity is harmless and still
    // forces real spilling.
    let total_weight = fill_weight_bytes(FILL_KEYS);
    let ram_budget_bytes = total_weight / 3;
    let spill_capacity_bytes = (total_weight + u64::from(FILL_KEYS) * 64) * 2;
    let mut a_env = spill_node_env(
        ram_budget_bytes,
        spill_capacity_bytes,
        SPILL_REGION_BYTES,
        spill_capacity_bytes,
        true,
    );
    a_env.push((
        "SUNDOG_TESTNODE_MODE".to_string(),
        "distributed".to_string(),
    ));
    a_env.push(("SUNDOG_TESTNODE_OWNERS".to_string(), OWNERS.to_string()));
    a_env.push(("RUST_LOG".to_string(), DISTRIBUTED_RUST_LOG.to_string()));
    let a_env_refs: Vec<(&str, &str)> = a_env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let dist_env = [
        ("SUNDOG_TESTNODE_MODE", "distributed"),
        ("SUNDOG_TESTNODE_OWNERS", "2"),
        ("RUST_LOG", DISTRIBUTED_RUST_LOG),
    ];

    let net = Arc::new(Network::new_network());
    let a = Node::spawn_binary_with_env_mounts_and_wait(
        &net,
        CLUSTER,
        "a",
        &[],
        &a_env_refs,
        &mounts,
        build_testnode(),
        Wait::for_log_message("testnode-ready", 1),
    )
    .await;
    let b = Node::spawn_binary(&net, CLUSTER, "b", &[&seed("a")], &dist_env, previous).await;
    let c = Node::spawn_binary(
        &net,
        CLUSTER,
        "c",
        &[&seed("a"), &seed("b")],
        &dist_env,
        previous,
    )
    .await;
    wait_for_peers(&[&a, &b, &c], 2).await;

    a.fill(FILL_KEYS)
        .await
        .expect("bulk fill through a succeeds");
    eventually(REBALANCE_WAIT, || async {
        sum_counts(&[&a, &b, &c]).await == Some(usize::from(OWNERS) * FILL_KEYS as usize)
    })
    .await;

    let spilled_before_restart = scrape_metric(&a, "sundog_spill_entries", ("cache", "it")).await;
    assert!(
        spilled_before_restart > 0,
        "a should already be spilling some of the {FILL_KEYS} entries before the restart"
    );

    a.stop().await.expect("a stops ahead of its restart");
    wait_for_peers(&[&b, &c], 1).await;

    // While a is down, writes through b land on b and c, the only two live
    // owners: a's own spill replay never observes this state.
    for (key, value) in (0..POST_RESTART_KEYS).map(|i| kv_entry(FILL_KEYS + i)) {
        b.put(&key, &value)
            .await
            .expect("a write during a's downtime still succeeds against its live co-owners");
    }

    // Same host directory remounted: the region files and checkpoint survive this restart.
    let a = Node::spawn_binary_with_env_mounts_and_wait(
        &net,
        CLUSTER,
        "a",
        &[&seed("b"), &seed("c")],
        &a_env_refs,
        &mounts,
        build_testnode(),
        Wait::for_http("/metrics").for_port(METRICS_PORT),
    )
    .await;

    let nodes = [&a, &b, &c];
    wait_for_peers(&nodes, 2).await;

    eventually(SETTLE_WAIT, || async {
        sum_counts(&nodes).await
            == Some(usize::from(OWNERS) * (FILL_KEYS + POST_RESTART_KEYS) as usize)
    })
    .await;

    assert_eq!(
        scrape_metric(&a, "sundog_spill_reopen_total", ("outcome", "warm")).await,
        1,
        "a's restart against its preserved spill dir must land warm, not fall back cold, for \
         this to actually exercise goal 2's eager post-reopen reconciliation path"
    );

    // Every write during a's downtime must be fetchable everywhere via the
    // eager AE round, not a later full pull.
    let post_restart_entries: Vec<(String, String)> = (0..POST_RESTART_KEYS)
        .map(|i| kv_entry(FILL_KEYS + i))
        .collect();
    let post_restart_mismatches = fetch_mismatches(&nodes, &post_restart_entries).await;
    assert!(
        post_restart_mismatches.is_empty(),
        "every key written during a's downtime must be fetchable with the right value from \
         every node once a's eager post-reopen AE round against its previous-release co-owner \
         completes: {post_restart_mismatches:?}"
    );

    let pre_restart_sample = sample_kv_entries(0x6011_ec7e, FILL_KEYS, SAMPLE_SIZE);
    let pre_restart_mismatches = fetch_mismatches(&nodes, &pre_restart_sample).await;
    assert!(
        pre_restart_mismatches.is_empty(),
        "every sampled key from before the restart must still be fetchable with the right \
         value, with no bucket lost to a's warm reopen or the ownership churn around its \
         downtime: {pre_restart_mismatches:?}"
    );

    let (a_stopped, b_stopped, c_stopped) = tokio::join!(a.stop(), b.stop(), c.stop());
    a_stopped.expect("a stops");
    b_stopped.expect("b stops");
    c_stopped.expect("c stops");
    net.close().await.expect("network closes");
    let _ = std::fs::remove_dir_all(&host_spill_dir);
}

/// The sum of every node's `loads` count.
async fn total_loads(nodes: &[&Node]) -> u64 {
    let mut total = 0;
    for node in nodes {
        total += node.loads().await.expect("loads answers");
    }
    total
}

/// Loads `key` from every node at once; true when every node answers
/// `src-{key}.*` and the cluster ran exactly one loader call for it.
async fn loads_gather_once(nodes: &[&Node], key: &str) -> bool {
    let before = total_loads(nodes).await;
    let answers = futures::future::join_all(nodes.iter().map(|node| node.load(key))).await;
    let prefix = format!("src-{key}.");
    let all_answered = answers
        .iter()
        .all(|answer| matches!(answer, Ok(Some(value)) if value.starts_with(&prefix)));
    all_answered && total_loads(nodes).await == before + 1
}

/// Three `Mode::Invalidation` nodes with a slow loader: once each sees the
/// others advertise a loader, every node loading the same key at the same
/// moment shares one loader call, on the node the key's loads gather on,
/// and each node keeps the value it read.
#[tokio::test]
async fn invalidation_loads_of_a_key_gather_into_one_loader_call() {
    const CLUSTER: &str = "load-gather-invalidation";
    require_containers!();

    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "invalidation"),
        ("SUNDOG_TESTNODE_LOADER_DELAY_MS", "500"),
    ];
    let n1 = Node::spawn_with_env(&net, CLUSTER, "n1", &[], &env).await;
    let n2 = Node::spawn_with_env(&net, CLUSTER, "n2", &[&seed("n1")], &env).await;
    let n3 = Node::spawn_with_env(&net, CLUSTER, "n3", &[&seed("n1")], &env).await;
    let nodes = [&n1, &n2, &n3];
    wait_for_peers(&nodes, 2).await;

    // Until the loader adverts reach every node, a node may load for
    // itself; each attempt uses a fresh key.
    let mut attempt = 0;
    eventually_with_logs(Duration::from_secs(60), &nodes, || {
        attempt += 1;
        let key = format!("probe{attempt}");
        async move { loads_gather_once(&nodes, &key).await }
    })
    .await;
    for round in 0..5 {
        assert!(
            loads_gather_once(&nodes, &format!("hot{round}")).await,
            "round {round}: three nodes loading one key share one loader call"
        );
    }
    for node in nodes {
        assert!(
            node.get("hot0").await.expect("get").is_some(),
            "each node keeps the value it loaded"
        );
    }

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// Three `Mode::Distributed` nodes, two owners per key, with a slow
/// loader: every node loading the same key at the same moment shares one
/// loader call on the key's first owner, both owners end up holding the
/// value, and the node that owns nothing of it keeps no copy.
#[tokio::test]
async fn distributed_loads_of_a_key_gather_on_its_first_owner() {
    const CLUSTER: &str = "load-gather-distributed";
    require_containers!();

    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "distributed"),
        ("SUNDOG_TESTNODE_OWNERS", "2"),
        ("SUNDOG_TESTNODE_LOADER_DELAY_MS", "500"),
    ];
    let n1 = Node::spawn_with_env(&net, CLUSTER, "n1", &[], &env).await;
    let n2 = Node::spawn_with_env(&net, CLUSTER, "n2", &[&seed("n1")], &env).await;
    let n3 = Node::spawn_with_env(&net, CLUSTER, "n3", &[&seed("n1")], &env).await;
    let nodes = [&n1, &n2, &n3];
    wait_for_peers(&nodes, 2).await;

    let mut attempt = 0;
    eventually_with_logs(Duration::from_secs(60), &nodes, || {
        attempt += 1;
        let key = format!("probe{attempt}");
        async move { loads_gather_once(&nodes, &key).await }
    })
    .await;
    let mut ids = Vec::new();
    for node in nodes {
        ids.push(node.node_id().await.expect("id"));
    }
    for round in 0..5 {
        let key = format!("hot{round}");
        assert!(
            loads_gather_once(&nodes, &key).await,
            "round {round}: three nodes loading one key share one loader call"
        );
        let owners = n1.owners(&key).await.expect("owners");
        eventually_with_logs(Duration::from_secs(30), &nodes, || {
            let (owners, ids, key) = (owners.clone(), ids.clone(), key.clone());
            async move {
                let mut right = true;
                for (node, id) in nodes.iter().zip(&ids) {
                    let held = node.get(&key).await.expect("get").is_some();
                    right &= held == owners.contains(id);
                }
                right
            }
        })
        .await;
    }

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// Three `Mode::Replicated` nodes with a 3s TTL and refresh-ahead at 0.5:
/// a key every node reads every 200ms for 9s never misses, and the cluster
/// reloads it about once a refresh interval, not once a node.
#[tokio::test]
async fn refresh_ahead_keeps_a_replicated_key_read_on_every_node_alive() {
    const CLUSTER: &str = "refresh-ahead-replicated";
    require_containers!();

    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "replicated"),
        ("SUNDOG_TESTNODE_LOADER_DELAY_MS", "0"),
        ("SUNDOG_TESTNODE_TTL_MS", "3000"),
        ("SUNDOG_TESTNODE_REFRESH_AHEAD", "0.5"),
    ];
    let n1 = Node::spawn_with_env(&net, CLUSTER, "n1", &[], &env).await;
    let n2 = Node::spawn_with_env(&net, CLUSTER, "n2", &[&seed("n1")], &env).await;
    let n3 = Node::spawn_with_env(&net, CLUSTER, "n3", &[&seed("n1")], &env).await;
    let nodes = [&n1, &n2, &n3];
    wait_for_peers(&nodes, 2).await;
    // Room for the loader adverts to reach every node.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let first = n1.load("hot").await.expect("load");
    assert!(first.is_some_and(|value| value.starts_with("src-hot.")));
    let before = total_loads(&nodes).await;
    eventually_with_logs(Duration::from_secs(10), &nodes, || async {
        n2.get("hot").await.expect("get").is_some() && n3.get("hot").await.expect("get").is_some()
    })
    .await;
    let started = tokio::time::Instant::now();
    let mut seen = std::collections::HashSet::new();
    while started.elapsed() < Duration::from_secs(9) {
        for node in nodes {
            let value = node.get("hot").await.expect("get");
            assert!(
                value.is_some(),
                "a key read through its refresh window never expires, {:?} in",
                started.elapsed()
            );
            seen.extend(value);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let reloads = total_loads(&nodes).await - before;
    assert!(
        (4..=8).contains(&reloads),
        "about one reload a 1.5s refresh interval across the cluster: {reloads}"
    );
    assert!(
        seen.len() > 1,
        "the reloaded values reach every node: {seen:?}"
    );

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// Beside the previous release in a `Mode::Distributed` cluster, every key a
/// current node loads takes one loader call and the value reaches the
/// previous release's node wherever it owns the key. A previous release
/// that speaks [`sundog::wire::PROTOCOL_LOAD`] loads too: a current node's
/// load of a key whose first owner is the previous release runs on the
/// previous release's loader. An older one has no loader, and the current
/// nodes load every key.
#[tokio::test]
async fn loads_beside_the_previous_release_take_one_call_a_key_and_reach_it() {
    const CLUSTER: &str = "load-mixed";
    const KEYS: u64 = 12;
    require_containers!();

    let previous_loads =
        sundog::wire::peer_supports(PREVIOUS_RELEASE_PROTOCOL, sundog::wire::PROTOCOL_LOAD);
    let previous = build_previous_testnode();
    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "distributed"),
        ("SUNDOG_TESTNODE_OWNERS", "2"),
        ("SUNDOG_TESTNODE_LOADER_DELAY_MS", "0"),
    ];
    let old_env = if previous_loads { &env[..] } else { &env[..2] };
    let old = Node::spawn_binary(&net, CLUSTER, "n1", &[], old_env, previous).await;
    let n2 = Node::spawn_with_env(&net, CLUSTER, "n2", &[&seed("n1")], &env).await;
    let n3 = Node::spawn_with_env(&net, CLUSTER, "n3", &[&seed("n1")], &env).await;
    let nodes = [&old, &n2, &n3];
    wait_for_peers(&nodes, 2).await;
    let old_id = old.node_id().await.expect("id");
    let loaders: &[&Node] = if previous_loads { &nodes } else { &nodes[1..] };

    if previous_loads {
        // Until the previous release's loader advert reaches n3, n3 loads
        // its keys elsewhere; each probe is a fresh key whose first owner is
        // the previous release.
        let mut attempt = 0;
        eventually_with_logs(Duration::from_secs(60), &nodes, || {
            attempt += 1;
            let key = format!("probe{attempt}");
            async move {
                let [old, n2, n3] = nodes;
                if n2.owners(&key).await.expect("owners").first() != Some(&old_id) {
                    return false;
                }
                let before = old.loads().await.expect("loads");
                n3.load(&key).await.expect("load").is_some()
                    && old.loads().await.expect("loads") == before + 1
            }
        })
        .await;
    }

    let before = total_loads(loaders).await;
    let mut old_owned = 0;
    for index in 0..KEYS {
        let key = format!("mixed{index}");
        let value = n3
            .load(&key)
            .await
            .expect("a load beside the previous release answers");
        assert!(value.is_some_and(|value| value.starts_with(&format!("src-{key}."))));
        let owners = n2.owners(&key).await.expect("owners");
        if owners.contains(&old_id) {
            old_owned += 1;
            eventually_with_logs(Duration::from_secs(30), &nodes, || async {
                old.get(&key).await.expect("get").is_some()
            })
            .await;
        }
    }
    assert!(old_owned > 0, "the previous release owns some of the keys");
    assert_eq!(
        total_loads(loaders).await - before,
        KEYS,
        "one loader call a key across the nodes with a loader"
    );

    old.stop().await.expect("old node stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// The metric families a read moves: hits and misses, fetch counts and fetch
/// timings.
const READ_FAMILIES: [&str; 4] = [
    "sundog_cache_hits_total",
    "sundog_cache_misses_total",
    "sundog_fetch_total",
    "sundog_fetch_duration_seconds",
];

/// The sorted lines of Prometheus exposition `body` that belong to
/// [`READ_FAMILIES`], samples of a histogram's `_bucket`, `_sum` and `_count`
/// included and `# TYPE` lines left out.
fn read_series_lines(body: &str) -> Vec<String> {
    let mut lines: Vec<String> = body
        .lines()
        .filter(|line| {
            READ_FAMILIES.iter().any(|family| {
                line.strip_prefix(family)
                    .is_some_and(|rest| rest.starts_with(['{', '_']))
            })
        })
        .map(str::to_string)
        .collect();
    lines.sort();
    lines
}

/// The read series each of `nodes` exports now, one sorted line list per node.
async fn read_series_of(nodes: &[&Node]) -> Vec<Vec<String>> {
    let mut series = Vec::new();
    for node in nodes {
        let body = node.metrics().await.expect("a node serves /metrics");
        series.push(read_series_lines(&body));
    }
    series
}

#[test]
fn read_series_lines_keep_the_read_families_and_drop_the_rest() {
    let body = "\
# TYPE sundog_fetch_total counter
sundog_fetch_total{cache=\"it\",outcome=\"remote\"} 2
sundog_cache_hits_total{cache=\"it\"} 7
sundog_cache_misses_total{cache=\"it\"} 1
sundog_fetch_duration_seconds_bucket{cache=\"it\",outcome=\"remote\",le=\"0.1\"} 2
sundog_fetch_duration_seconds_sum{cache=\"it\",outcome=\"remote\"} 0.004
sundog_fetch_duration_seconds_count{cache=\"it\",outcome=\"remote\"} 2
sundog_fetch_totally_other{cache=\"it\"} 3
sundog_read_duration_seconds_count{cache=\"it\"} 9
sundog_ae_parts_total{outcome=\"listing\"} 4
";
    let lines = read_series_lines(body);
    assert_eq!(lines.len(), 6, "{lines:?}");
    assert!(lines.iter().all(|line| !line.starts_with('#')), "{lines:?}");
    assert!(
        lines.iter().all(|line| !line.contains("totally_other")
            && !line.contains("sundog_read_duration")
            && !line.contains("sundog_ae_")),
        "{lines:?}"
    );
    assert!(
        lines.windows(2).all(|pair| pair[0] <= pair[1]),
        "the lines are sorted: {lines:?}"
    );
    assert_eq!(read_series_lines(""), Vec::<String>::new());
}

/// The `distributed` section of an `explain` reply.
fn distributed_of(reply: &serde_json::Value) -> &serde_json::Value {
    let read = &reply["distributed"];
    assert!(
        read.is_object(),
        "a Distributed cache explains its fetch: {reply}"
    );
    read
}

/// The owners an `explain` reply names, in the reply's order.
fn owners_of_reply(reply: &serde_json::Value) -> Vec<String> {
    distributed_of(reply)["owners"]
        .as_array()
        .unwrap_or_else(|| panic!("owners is a list: {reply}"))
        .iter()
        .map(|owner| owner.as_str().expect("an owner is a string").to_string())
        .collect()
}

/// Waits until every one of `nodes` names the same two owners of `key`.
async fn wait_for_the_same_two_owners(nodes: &[&Node], key: &str) {
    eventually_reporting(
        "every node names the same two owners of the key",
        Duration::from_secs(30),
        nodes,
        || async {
            let mut seen = Vec::new();
            for node in nodes {
                seen.push(node.owners(key).await?);
            }
            if seen[0].len() == 2 && seen.iter().all(|owners| *owners == seen[0]) {
                Ok(())
            } else {
                Err(format!("{seen:?}"))
            }
        },
    )
    .await;
}

/// Waits until every one of `nodes` explains a read of `key` from one view.
async fn wait_for_one_explained_view(nodes: &[&Node], key: &str) {
    eventually_reporting(
        "every node explains from one view",
        Duration::from_secs(30),
        nodes,
        || async {
            let mut views = Vec::new();
            for node in nodes {
                let reply = node.explain(key).await?;
                views.push(distributed_of(&reply)["view"].to_string());
            }
            if views.iter().all(|view| *view == views[0]) {
                Ok(())
            } else {
                Err(format!("{views:?}"))
            }
        },
    )
    .await;
}

/// Asserts what `explain` of the stored key reports on the node `name`,
/// whose id is `id`: the cluster's one `view`, the owners `owner_hexes` in
/// order, and by `owns` either the owner's own live copy and the other
/// owner's held record, or the non-owner's two held probes with the first
/// owner as its source.
fn assert_explained_node(
    name: &str,
    reply: &serde_json::Value,
    id: u64,
    view: &serde_json::Value,
    owner_hexes: &[String],
    owns: bool,
) {
    let hex = format!("{id:016x}");
    assert_eq!(reply["cache"], "it", "{name}: {reply}");
    assert_eq!(reply["node"], hex, "{name}: {reply}");
    assert_eq!(reply["mode"], "distributed:2", "{name}: {reply}");
    let read = distributed_of(reply);
    assert_eq!(read["view"], *view, "{name}: one view");
    assert!(read["view_moved_to"].is_null(), "{name}: {reply}");
    assert_eq!(
        owners_of_reply(reply),
        owner_hexes,
        "{name}: the owners are Node::owners' in its order"
    );
    let probes = read["probes"].as_array().expect("probes is a list");
    assert!(
        probes.iter().all(|probe| probe["answer"] == "held"
            && probe["reads"] == "value"
            && owner_hexes.contains(&probe["node"].as_str().unwrap_or_default().to_string())),
        "{name}: every probe finds the value at an owner: {reply}"
    );
    assert_eq!(read["residency"]["owns"], owns, "{name}: {reply}");
    if owns {
        assert_eq!(read["local_read"], "hit", "{name}: {reply}");
        assert_eq!(read["serves_peers"], "serve", "{name}: {reply}");
        assert_eq!(reply["local"]["kind"], "live", "{name}: {reply}");
        assert_eq!(reply["source"]["kind"], "local", "{name}: {reply}");
        assert_eq!(reply["source"]["hit"], true, "{name}: {reply}");
        assert_eq!(probes.len(), 1, "{name}: the other owner is asked: {reply}");
        assert_ne!(probes[0]["node"], hex, "{name}: not asked of itself");
    } else {
        assert_eq!(read["local_read"], "not_owner", "{name}: {reply}");
        assert_eq!(reply["local"]["kind"], "absent", "{name}: {reply}");
        assert_eq!(probes.len(), 2, "{name}: both owners are asked: {reply}");
        let asked: Vec<&str> = probes
            .iter()
            .map(|probe| probe["node"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(asked, owner_hexes, "{name}: probes follow owner order");
        assert_eq!(reply["source"]["kind"], "owner", "{name}: {reply}");
        assert_eq!(reply["source"]["node"], owner_hexes[0], "{name}: {reply}");
        assert_eq!(reply["source"]["hit"], true, "{name}: {reply}");
    }
}

/// Three `Mode::Distributed` nodes, two owners per key. After a key is
/// written and both owners hold it, `explain` on every node reports one view
/// and one owner list, the owners' own copy and the other owner's held
/// record, and the non-owner's two held probes with the first owner as its
/// source. None of it moves the series a read moves, and the non-owner's
/// probes show as frames it sends.
#[tokio::test]
async fn explain_reports_each_nodes_view_of_a_key_and_moves_no_read_counter() {
    const CLUSTER: &str = "explain-distributed";
    const KEY: &str = "explained";
    require_containers!();

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn_distributed(&net, CLUSTER, "n1", &[], Some(2)).await;
    let n2 = Node::spawn_distributed(&net, CLUSTER, "n2", &[&seed("n1")], Some(2)).await;
    let n3 = Node::spawn_distributed(&net, CLUSTER, "n3", &[&seed("n1")], Some(2)).await;
    let nodes = [&n1, &n2, &n3];
    wait_for_peers(&nodes, 2).await;
    let mut ids = Vec::new();
    for node in nodes {
        ids.push(node.node_id().await.expect("id"));
    }

    wait_for_the_same_two_owners(&nodes, KEY).await;
    let owners = n1.owners(KEY).await.expect("owners");
    let hex = |id: u64| format!("{id:016x}");
    let owner_hexes: Vec<String> = owners.iter().copied().map(hex).collect();
    let non_owner = ids
        .iter()
        .position(|id| !owners.contains(id))
        .expect("three nodes at two owners leave one non-owner");

    n1.put(KEY, "v").await.expect("put");
    eventually_with_logs(Duration::from_secs(30), &nodes, || async {
        for (node, id) in nodes.iter().zip(&ids) {
            let held = node.get(KEY).await == Ok(Some("v".to_string()));
            if held != owners.contains(id) {
                return false;
            }
            if node.fetch(KEY).await != Ok(Some("v".to_string())) {
                return false;
            }
        }
        true
    })
    .await;

    let before = read_series_of(&nodes).await;
    for (node, series) in nodes.iter().zip(&before) {
        assert!(
            !series.is_empty(),
            "{} exports read series after its reads: they are the baseline",
            node.name()
        );
    }
    wait_for_one_explained_view(&nodes, KEY).await;

    let (frames_before, _) = nodes[non_owner].netstats().await.expect("netstats");
    let mut replies = Vec::new();
    for node in nodes {
        replies.push(node.explain(KEY).await.expect("explain"));
    }
    let (frames_after, _) = nodes[non_owner].netstats().await.expect("netstats");
    assert!(
        frames_after >= frames_before + 2,
        "the non-owner sent a fetch to each of two owners: {frames_before} -> {frames_after}"
    );

    let view = distributed_of(&replies[0])["view"].clone();
    assert_eq!(view.as_str().map(str::len), Some(16), "{view}");
    for ((reply, id), node) in replies.iter().zip(&ids).zip(nodes) {
        assert_explained_node(
            node.name(),
            reply,
            *id,
            &view,
            &owner_hexes,
            owners.contains(id),
        );
    }

    assert_eq!(
        read_series_of(&nodes).await,
        before,
        "explain moves no read series and creates none"
    );

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// Beside the previous release in a `Mode::Distributed` cluster, `explain`
/// on a current node probes the previous release's node with the fetch it
/// already answers: with two nodes and two owners, the previous release owns
/// every key, and its one probe reads `held`.
#[tokio::test]
async fn explain_probes_a_previous_release_owner_with_the_existing_fetch() {
    const CLUSTER: &str = "explain-mixed";
    const KEY: &str = "explained";
    require_containers!();

    assert!(
        sundog::wire::peer_supports(
            PREVIOUS_RELEASE_PROTOCOL,
            sundog::wire::PROTOCOL_DISTRIBUTED
        ),
        "the previous release answers a Distributed fetch"
    );
    let previous = build_previous_testnode();
    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "distributed"),
        ("SUNDOG_TESTNODE_OWNERS", "2"),
    ];
    let old = Node::spawn_binary(&net, CLUSTER, "n1", &[], &env, previous).await;
    let current = Node::spawn_with_env(&net, CLUSTER, "n2", &[&seed("n1")], &env).await;
    let nodes = [&old, &current];
    wait_for_peers(&nodes, 1).await;
    let old_hex = format!("{:016x}", old.node_id().await.expect("id"));
    let current_hex = format!("{:016x}", current.node_id().await.expect("id"));

    current.put(KEY, "v").await.expect("put");
    eventually_with_logs(Duration::from_secs(30), &nodes, || async {
        old.get(KEY).await == Ok(Some("v".to_string()))
            && current.get(KEY).await == Ok(Some("v".to_string()))
    })
    .await;
    eventually_reporting(
        "the current node names both nodes as owners",
        Duration::from_secs(30),
        &nodes,
        || async {
            let reply = current.explain(KEY).await?;
            let owners = owners_of_reply(&reply);
            if owners.len() == 2 && owners.contains(&old_hex) && owners.contains(&current_hex) {
                Ok(())
            } else {
                Err(format!("{reply}"))
            }
        },
    )
    .await;

    let reply = current.explain(KEY).await.expect("explain");
    let read = distributed_of(&reply);
    let probes = read["probes"].as_array().expect("probes is a list");
    assert_eq!(probes.len(), 1, "the one other owner is asked: {reply}");
    assert_eq!(probes[0]["node"], old_hex, "{reply}");
    assert_eq!(probes[0]["answer"], "held", "{reply}");
    assert_eq!(probes[0]["reads"], "value", "{reply}");
    assert_eq!(
        probes[0]["version"], reply["local"]["version"],
        "both nodes hold the one write: {reply}"
    );
    assert_eq!(read["residency"]["owns"], true, "{reply}");
    assert_eq!(reply["source"]["kind"], "local", "{reply}");

    old.stop().await.expect("old node stops");
    current.stop().await.expect("current node stops");
    net.close().await.expect("network closes");
}
