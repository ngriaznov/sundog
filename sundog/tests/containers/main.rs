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
mod previous_release;
mod replication;
mod scale;
mod spill;
mod support;

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
