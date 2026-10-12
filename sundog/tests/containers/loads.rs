//! The loader and refresh-ahead scenarios.

use crate::container_util::{
    Node, PREVIOUS_RELEASE_PROTOCOL, build_previous_testnode, eventually_with_logs, seed,
    wait_for_peers,
};
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

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
