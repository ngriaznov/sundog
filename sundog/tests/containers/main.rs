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
    Node, PREVIOUS_RELEASE_PROTOCOL, build_previous_testnode, eventually_reporting,
    eventually_with_logs, seed, wait_for_peers,
};
use rightsize::Network;
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
mod loads;
mod previous_release;
mod replication;
mod scale;
mod spill;
mod support;

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
