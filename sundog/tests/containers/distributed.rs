//! The distributed-mode scenarios and the fill and ownership helpers they share.

use crate::container_util::{
    DISTRIBUTED_RUST_LOG, Fleet, Node, eventually, eventually_with_logs, seed, wait_for_peers,
};
use crate::support::scrape_metric;
use futures::stream;
use futures::stream::StreamExt as _;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

/// Node ids for `nodes`, in the same order, read via each node's own `id`
/// control route.
pub(crate) async fn collect_node_ids(nodes: &[&Node]) -> Vec<u64> {
    let mut ids = Vec::with_capacity(nodes.len());
    for node in nodes {
        ids.push(
            node.node_id()
                .await
                .expect("id replies with this node's own NodeId as a decimal u64"),
        );
    }
    ids
}

/// [`fill`][Node::fill]'s deterministic `k{i}` -> `v{i}` pair for index `i`.
pub(crate) fn kv_entry(index: u32) -> (String, String) {
    (format!("k{index}"), format!("v{index}"))
}

/// [`kv_entry`] for every index in `0..count`: the full key/value set one
/// `fill(count)` call writes.
pub(crate) fn kv_range(count: u32) -> Vec<(String, String)> {
    (0..count).map(kv_entry).collect()
}

/// A uniform random sample of `sample_size` [`kv_entry`] pairs with indices
/// in `0..fill_keys`, from a fixed seed so a failing run's sample is
/// reproducible.
pub(crate) fn sample_kv_entries(
    seed: u64,
    fill_keys: u32,
    sample_size: usize,
) -> Vec<(String, String)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..sample_size)
        .map(|_| kv_entry(rng.random_range(0..fill_keys)))
        .collect()
}

/// True once `reader`'s `owners k` names exactly `owners` ids for every
/// `(key, value)` pair in `entries`, and exactly the nodes among `tagged`
/// (each paired with its own [`collect_node_ids`] id) whose id is named
/// answer `get k` with `value`, every other one answering `None`. Concurrent
/// over `entries`, so a poll of this against a few hundred keys stays fast
/// despite the container network's per-connection round trip.
pub(crate) async fn ownership_snapshot_matches(
    reader: &Node,
    tagged: &[(&Node, u64)],
    entries: &[(String, String)],
    owners: usize,
) -> bool {
    stream::iter(entries.iter())
        .map(|(key, value)| async move {
            let Ok(owner_ids) = reader.owners(key).await else {
                return false;
            };
            if owner_ids.len() != owners {
                return false;
            }
            for &(node, id) in tagged {
                let want = owner_ids.contains(&id).then(|| value.clone());
                if node.get(key).await != Ok(want) {
                    return false;
                }
            }
            true
        })
        .buffer_unordered(16)
        .all(|matched| async move { matched })
        .await
}

/// Concurrently reads every `(key, value)` in `entries` from every one of
/// `nodes` via [`Node::fetch`], returning a description of every reply that
/// wasn't `Ok(Some(value))` (empty on full agreement). Concurrent over the
/// full node-by-entry cross product for the same reason
/// [`ownership_snapshot_matches`] is: many container round trips otherwise
/// add up fast.
pub(crate) async fn fetch_mismatches(nodes: &[&Node], entries: &[(String, String)]) -> Vec<String> {
    let pairs: Vec<(&Node, &(String, String))> = nodes
        .iter()
        .flat_map(|&node| entries.iter().map(move |entry| (node, entry)))
        .collect();
    stream::iter(pairs)
        .map(|(node, (key, value))| async move {
            let got = node.fetch(key).await;
            (node.name().to_string(), key.clone(), value.clone(), got)
        })
        .buffer_unordered(64)
        .filter_map(|(name, key, expected, got)| async move {
            (got != Ok(Some(expected.clone())))
                .then(|| format!("{name}/{key}: expected Ok(Some({expected:?})), got {got:?}"))
        })
        .collect()
        .await
}

/// One line per node of `count`, `peers`, `sundog_owned_parts`, and the
/// rebalance in/out counters, for a convergence wait's progress report.
async fn describe_distributed_state(nodes: &[&Node]) -> String {
    let mut parts = Vec::with_capacity(nodes.len());
    for node in nodes {
        let count = node
            .count()
            .await
            .map_or_else(|_| "?".to_string(), |c| c.to_string());
        let peers = node
            .peers()
            .await
            .map_or_else(|_| "?".to_string(), |c| c.to_string());
        let owned = scrape_metric(node, "sundog_owned_parts", ("cache", "it")).await;
        let pulled = scrape_metric(node, "sundog_rebalance_parts_total", ("direction", "in")).await;
        let released =
            scrape_metric(node, "sundog_rebalance_parts_total", ("direction", "out")).await;
        parts.push(format!(
            "{}: count={count} peers={peers} owned={owned} in={pulled} out={released}",
            node.name()
        ));
    }
    parts.join("; ")
}

/// Polls until the summed `count` across `nodes` is `expected_sum`, printing
/// [`describe_distributed_state`] every few seconds while it is not, so a
/// timed-out wait's output says which node held what.
pub(crate) async fn wait_for_entry_sum(
    nodes: &[Node],
    expected_sum: usize,
    wait: Duration,
    iteration: u64,
) {
    let node_refs: Vec<&Node> = nodes.iter().collect();
    let node_refs = &node_refs;
    let mut polls = 0u32;
    eventually(wait, || {
        polls += 1;
        let report = polls.is_multiple_of(25);
        async move {
            let sum = sum_counts(node_refs).await;
            if report && sum != Some(expected_sum) {
                eprintln!(
                    "chaos[{iteration}]: waiting for {expected_sum} entries, {}",
                    describe_distributed_state(node_refs).await
                );
            }
            sum == Some(expected_sum)
        }
    })
    .await;
}

/// Sum of `count` across every node in `nodes`, `None` if any read fails.
/// The shared building block every distributed scenario's convergence poll
/// below sums to `owners * fill_keys`.
pub(crate) async fn sum_counts(nodes: &[&Node]) -> Option<usize> {
    let mut sum = 0usize;
    for node in nodes {
        sum += node.count().await.ok()?;
    }
    Some(sum)
}

/// Every `sundog-testnode` opens `"it"`'s `Mode::Distributed` part space,
/// `sundog::store::BUCKET_COUNT` buckets of `sundog::store::PART_COUNT`
/// parts; with `owners` owners per part, the summed `sundog_owned_parts`
/// gauge across every live node settles at this many part-ownership
/// assignments.
fn expected_owned_parts_sum(owners: u64) -> u64 {
    (sundog::store::BUCKET_COUNT * sundog::store::PART_COUNT) as u64 * owners
}

/// Five distributed nodes fill a shared keyspace from one writer, and every
/// key lands on exactly `OWNERS` owners: `owners k` from any node names
/// exactly two ids, exactly those nodes' `get k` answers with the value and
/// every other node answers `none`, every node's `fetch k` returns the
/// value, the summed local `count` is `OWNERS * FILL_KEYS`, and the summed
/// `sundog_owned_parts` gauge is every part assigned exactly `OWNERS`
/// times.
#[tokio::test]
async fn distributed_five_node_fill_and_convergence_with_every_key_on_exactly_k_owners() {
    const NODE_COUNT: usize = 5;
    const OWNERS: u8 = 2;
    const FILL_KEYS: u32 = 3_000;
    const SAMPLE_SIZE: usize = 200;
    const ALIASES: [&str; NODE_COUNT] = ["n1", "n2", "n3", "n4", "n5"];
    const CLUSTER: &str = "dist-fill-cluster";
    const CONVERGE_WAIT: Duration = Duration::from_secs(180);

    require_containers!();

    let net = Arc::new(Network::new_network());
    let log_env = [("RUST_LOG", DISTRIBUTED_RUST_LOG)];
    // `Fleet`, not a bare `Vec<Node>`: a prior CI run traced a leak to these
    // five never reaching `stop()` after a timeout; `Fleet::drop` stops them on unwind.
    let mut fleet = Fleet(Vec::with_capacity(NODE_COUNT));
    for (i, alias) in ALIASES.iter().enumerate() {
        let seeds: Vec<String> = ALIASES[..i].iter().map(|a| seed(a)).collect();
        let seed_refs: Vec<&str> = seeds.iter().map(String::as_str).collect();
        fleet.0.push(
            Node::spawn_distributed_with_env(
                &net,
                CLUSTER,
                alias,
                &seed_refs,
                Some(OWNERS),
                &log_env,
            )
            .await,
        );
    }
    let node_refs: Vec<&Node> = fleet.0.iter().collect();
    wait_for_peers(&node_refs, NODE_COUNT - 1).await;

    node_refs[0]
        .fill(FILL_KEYS)
        .await
        .expect("bulk fill succeeds");

    let ids = collect_node_ids(&node_refs).await;
    let tagged: Vec<(&Node, u64)> = node_refs.iter().copied().zip(ids).collect();
    let sample = sample_kv_entries(0xd157_fe11, FILL_KEYS, SAMPLE_SIZE);
    let expected_owned_sum = expected_owned_parts_sum(u64::from(OWNERS));

    eventually_with_logs(CONVERGE_WAIT, &node_refs, || async {
        let Some(sum) = sum_counts(&node_refs).await else {
            return false;
        };
        if sum != usize::from(OWNERS) * FILL_KEYS as usize {
            return false;
        }
        if !ownership_snapshot_matches(tagged[0].0, &tagged, &sample, usize::from(OWNERS)).await {
            return false;
        }
        let mut owned_sum = 0u64;
        for node in &node_refs {
            owned_sum += scrape_metric(node, "sundog_owned_parts", ("cache", "it")).await;
        }
        owned_sum == expected_owned_sum
    })
    .await;

    let all_entries = kv_range(FILL_KEYS);
    let mismatches = fetch_mismatches(&node_refs, &all_entries).await;
    assert!(
        mismatches.is_empty(),
        "every fill key must be fetchable with the right value from every node: {mismatches:?}"
    );

    for node in fleet.take() {
        node.stop().await.expect("node stops");
    }
    net.close().await.expect("network closes");
}

/// Crashing one owner never makes a key unfetchable, the surviving owner
/// keeps answering `fetch` through the dead peer's stale entry in the
/// ownership view, and once gossip notices the death and rebalance runs,
/// every key resettles on exactly `OWNERS` of the four survivors, with at
/// least one of them having pulled buckets in.
#[tokio::test]
async fn distributed_kill_one_owner_and_every_key_still_fetchable_then_re_owned() {
    const NODE_COUNT: usize = 5;
    const OWNERS: u8 = 2;
    const FILL_KEYS: u32 = 3_000;
    const SAMPLE_SIZE: usize = 100;
    const ALIASES: [&str; NODE_COUNT] = ["n1", "n2", "n3", "n4", "n5"];
    const CLUSTER: &str = "dist-kill-cluster";
    const FILL_WAIT: Duration = Duration::from_secs(60);
    const IMMEDIATE_CHECK_WINDOW: Duration = Duration::from_secs(10);
    const REOWNED_WAIT: Duration = Duration::from_secs(240);

    require_containers!();

    let net = Arc::new(Network::new_network());
    let log_env = [("RUST_LOG", DISTRIBUTED_RUST_LOG)];
    let mut nodes = Vec::with_capacity(NODE_COUNT);
    for (i, alias) in ALIASES.iter().enumerate() {
        let seeds: Vec<String> = ALIASES[..i].iter().map(|a| seed(a)).collect();
        let seed_refs: Vec<&str> = seeds.iter().map(String::as_str).collect();
        nodes.push(
            Node::spawn_distributed_with_env(
                &net,
                CLUSTER,
                alias,
                &seed_refs,
                Some(OWNERS),
                &log_env,
            )
            .await,
        );
    }
    wait_for_peers(&nodes.iter().collect::<Vec<_>>(), NODE_COUNT - 1).await;

    nodes[0].fill(FILL_KEYS).await.expect("bulk fill succeeds");
    // Every write during the fill landed straight on its real owners (a
    // non-owner write is forwarded, never applied locally), so the fill
    // itself converges as soon as the forwards land, well before any
    // rebalance is involved.
    eventually(FILL_WAIT, || async {
        sum_counts(&nodes.iter().collect::<Vec<_>>()).await
            == Some(usize::from(OWNERS) * FILL_KEYS as usize)
    })
    .await;

    let sample = sample_kv_entries(0xdead_b17e, FILL_KEYS, SAMPLE_SIZE);

    // Baselined before the crash: the survivors pull the moment gossip
    // drops the victim, the same moment `wait_for_peers` below returns.
    let mut in_before = Vec::with_capacity(nodes.len() - 1);
    for node in &nodes[1..] {
        in_before
            .push(scrape_metric(node, "sundog_rebalance_parts_total", ("direction", "in")).await);
    }

    let victim = nodes.remove(0);
    victim
        .crash()
        .await
        .expect("crashed node dies and is removed cleanly");
    let live: Vec<&Node> = nodes.iter().collect();

    // Immediately and repeatedly, before gossip has even had a chance to
    // notice the death: every key stays fetchable from every surviving
    // node, since fetch tries owners in order and the still-live one
    // answers.
    let immediate_deadline = tokio::time::Instant::now() + IMMEDIATE_CHECK_WINDOW;
    while tokio::time::Instant::now() < immediate_deadline {
        let mismatches = fetch_mismatches(&live, &sample).await;
        assert!(
            mismatches.is_empty(),
            "every key must stay fetchable through the surviving owner right after a crash: \
             {mismatches:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    wait_for_peers(&live, NODE_COUNT - 2).await;

    let ids = collect_node_ids(&live).await;
    let tagged: Vec<(&Node, u64)> = live.iter().copied().zip(ids).collect();

    eventually(REOWNED_WAIT, || async {
        if sum_counts(&live).await != Some(usize::from(OWNERS) * FILL_KEYS as usize) {
            return false;
        }
        ownership_snapshot_matches(tagged[0].0, &tagged, &sample, usize::from(OWNERS)).await
    })
    .await;

    let mut in_moved = false;
    for (node, before) in live.iter().zip(in_before.iter()) {
        let after = scrape_metric(node, "sundog_rebalance_parts_total", ("direction", "in")).await;
        if after > *before {
            in_moved = true;
        }
    }
    assert!(
        in_moved,
        "at least one surviving node should have pulled rebalanced parts in after the crash"
    );

    let mismatches = fetch_mismatches(&live, &sample).await;
    assert!(
        mismatches.is_empty(),
        "every sampled key must still be fetchable with the right value: {mismatches:?}"
    );

    for node in nodes {
        node.stop().await.expect("node stops");
    }
    net.close().await.expect("network closes");
}

/// Like [`fetch_mismatches`], but an honest "no reachable owner" is not a
/// mismatch: only a miss, a wrong value, or another error is.
async fn fetch_violations(nodes: &[&Node], entries: &[(String, String)]) -> Vec<String> {
    fetch_mismatches(nodes, entries)
        .await
        .into_iter()
        .filter(|line| !line.contains("found no reachable owner"))
        .collect()
}

/// An owner crashes while a newly joined node is still pulling its share.
/// The joiner's parts stay cold rather than answering from an empty store,
/// the surviving holders keep answering, and no read of a written key comes
/// back a miss or a wrong value, at worst "no reachable owner" for a moment;
/// every key is then readable again. The crash lands a random 0 to 1.5 s
/// after the joiner is ready, printed so a failure replays with
/// `SUNDOG_CRASH_DELAY_MS`.
#[tokio::test]
async fn distributed_owner_crash_during_a_joiners_pull_keeps_every_key_fetchable() {
    const OWNERS: u8 = 2;
    const FILL_KEYS: u32 = 3_000;
    const SAMPLE_SIZE: usize = 200;
    const ALIASES: [&str; 4] = ["j1", "j2", "j3", "j4"];
    const CLUSTER: &str = "dist-join-crash-cluster";
    const CHECK_WINDOW: Duration = Duration::from_secs(15);
    const READABLE_WAIT: Duration = Duration::from_secs(120);

    require_containers!();

    let delay_ms: u64 = std::env::var("SUNDOG_CRASH_DELAY_MS").map_or_else(
        |_| rand::rng().random_range(0..=1_500),
        |raw| raw.parse().expect("SUNDOG_CRASH_DELAY_MS is a u64"),
    );
    eprintln!("crash delay: replay with SUNDOG_CRASH_DELAY_MS={delay_ms}");

    let net = Arc::new(Network::new_network());
    let log_env = [("RUST_LOG", DISTRIBUTED_RUST_LOG)];
    let mut nodes = Vec::with_capacity(ALIASES.len() + 1);
    for (i, alias) in ALIASES.iter().enumerate() {
        let seeds: Vec<String> = ALIASES[..i].iter().map(|a| seed(a)).collect();
        let seed_refs: Vec<&str> = seeds.iter().map(String::as_str).collect();
        nodes.push(
            Node::spawn_distributed_with_env(
                &net,
                CLUSTER,
                alias,
                &seed_refs,
                Some(OWNERS),
                &log_env,
            )
            .await,
        );
    }
    wait_for_peers(&nodes.iter().collect::<Vec<_>>(), ALIASES.len() - 1).await;
    nodes[0].fill(FILL_KEYS).await.expect("bulk fill succeeds");
    eventually(Duration::from_secs(60), || async {
        sum_counts(&nodes.iter().collect::<Vec<_>>()).await
            == Some(usize::from(OWNERS) * FILL_KEYS as usize)
    })
    .await;
    let sample = sample_kv_entries(0x0101_c0de, FILL_KEYS, SAMPLE_SIZE);

    let joiner_seeds: Vec<String> = ALIASES.iter().map(|a| seed(a)).collect();
    let joiner_seed_refs: Vec<&str> = joiner_seeds.iter().map(String::as_str).collect();
    let joiner = Node::spawn_distributed_with_env(
        &net,
        CLUSTER,
        "j5",
        &joiner_seed_refs,
        Some(OWNERS),
        &log_env,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    let victim = nodes.remove(0);
    victim
        .crash()
        .await
        .expect("crashed node dies and is removed cleanly");
    nodes.push(joiner);
    let live: Vec<&Node> = nodes.iter().collect();

    let check_deadline = tokio::time::Instant::now() + CHECK_WINDOW;
    while tokio::time::Instant::now() < check_deadline {
        let violations = fetch_violations(&live, &sample).await;
        assert!(
            violations.is_empty(),
            "a read answered a miss or a wrong value {delay_ms} ms into the joiner's pull: \
             {violations:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    eventually(READABLE_WAIT, || async {
        fetch_mismatches(&live, &sample).await.is_empty()
    })
    .await;

    for node in nodes {
        node.stop().await.expect("node stops");
    }
    net.close().await.expect("network closes");
}

/// A fourth node joins a filled three-node distributed cluster: it pulls
/// roughly a quarter of the part-ownership assignments (`sundog_owned_parts`
/// near 65,536 × `OWNERS` / 4) and at least one original node's
/// `direction="out"` counter moves to match. Once the disown grace period
/// (`distributed_disown_grace_rounds` anti-entropy intervals) has passed, the
/// donors have dropped every key outside their own ownership, and every key
/// sits on exactly `OWNERS` of the four nodes, fetchable from all of them.
#[tokio::test]
async fn distributed_join_and_rebalance() {
    const OWNERS: u8 = 2;
    const FILL_KEYS: u32 = 3_000;
    const SAMPLE_SIZE: usize = 150;
    const ALIASES: [&str; 3] = ["n1", "n2", "n3"];
    const JOINER: &str = "n4";
    const CLUSTER: &str = "dist-join-cluster";
    /// Parts either side of 65,536 × `OWNERS` / 4, the even split a fresh
    /// joiner lands close to: rendezvous hashing balances parts closely
    /// rather than exactly, within about 1% at four nodes.
    const JOINER_PARTS_TOLERANCE: u64 = 1_000;
    const REBALANCE_WAIT: Duration = Duration::from_secs(240);
    // sundog-testnode sets ae_interval to 2s; distributed_disown_grace_
    // rounds defaults to 3, so a previous owner keeps serving for 6s past
    // losing a part. Generous past that for the anti-entropy round itself.
    const DISOWN_GRACE_WAIT: Duration = Duration::from_secs(16);
    const SETTLE_WAIT: Duration = Duration::from_secs(180);

    require_containers!();

    // Info-level everywhere, debug on the cluster state machine, for diagnostics on a timeout.
    let log_env = [("RUST_LOG", DISTRIBUTED_RUST_LOG)];
    let spawn = Node::spawn_distributed_with_env;

    let net = Arc::new(Network::new_network());
    // `Fleet`, not a bare `Vec<Node>`: a prior CI run traced leaked gossip to
    // a timed-out test's containers; `Fleet::drop` stops these on unwind too.
    let mut fleet = Fleet(Vec::with_capacity(4));
    for (i, alias) in ALIASES.iter().enumerate() {
        let seeds: Vec<String> = ALIASES[..i].iter().map(|a| seed(a)).collect();
        let seed_refs: Vec<&str> = seeds.iter().map(String::as_str).collect();
        let node = spawn(&net, CLUSTER, alias, &seed_refs, Some(OWNERS), &log_env).await;
        fleet.0.push(node);
    }
    wait_for_peers(&fleet.0.iter().collect::<Vec<_>>(), ALIASES.len() - 1).await;

    fleet.0[0]
        .fill(FILL_KEYS)
        .await
        .expect("bulk fill succeeds");
    let initial_node_refs: Vec<&Node> = fleet.0.iter().collect();
    eventually_with_logs(Duration::from_secs(60), &initial_node_refs, || async {
        sum_counts(&fleet.0.iter().collect::<Vec<_>>()).await
            == Some(usize::from(OWNERS) * FILL_KEYS as usize)
    })
    .await;

    let mut out_before = Vec::with_capacity(fleet.0.len());
    for node in &fleet.0 {
        out_before
            .push(scrape_metric(node, "sundog_rebalance_parts_total", ("direction", "out")).await);
    }

    let all_seeds: Vec<String> = ALIASES.iter().map(|a| seed(a)).collect();
    let all_seed_refs: Vec<&str> = all_seeds.iter().map(String::as_str).collect();
    let joiner_node = spawn(
        &net,
        CLUSTER,
        JOINER,
        &all_seed_refs,
        Some(OWNERS),
        &log_env,
    )
    .await;
    fleet.0.push(joiner_node);
    let node_refs: Vec<&Node> = fleet.0.iter().collect();
    wait_for_peers(&node_refs, ALIASES.len()).await;
    let joiner: &Node = node_refs
        .last()
        .expect("nodes always has the joiner as its last element");

    let expected_joiner_parts = expected_owned_parts_sum(u64::from(OWNERS)) / 4;
    eventually_with_logs(REBALANCE_WAIT, &node_refs, || async {
        let owned = scrape_metric(joiner, "sundog_owned_parts", ("cache", "it")).await;
        let pulled_in =
            scrape_metric(joiner, "sundog_rebalance_parts_total", ("direction", "in")).await > 0;
        pulled_in && owned.abs_diff(expected_joiner_parts) <= JOINER_PARTS_TOLERANCE
    })
    .await;

    // A previous owner keeps a lost part for the disown grace, hands it
    // to the joiner, and only then drops it: `direction="out"` moves at
    // that point, not when the joiner's pull lands.
    tokio::time::sleep(DISOWN_GRACE_WAIT).await;
    eventually_with_logs(SETTLE_WAIT, &node_refs, || async {
        for (node, before) in node_refs[..ALIASES.len()].iter().zip(out_before.iter()) {
            let after =
                scrape_metric(node, "sundog_rebalance_parts_total", ("direction", "out")).await;
            if after > *before {
                return true;
            }
        }
        false
    })
    .await;

    let ids = collect_node_ids(&node_refs).await;
    let tagged: Vec<(&Node, u64)> = node_refs.iter().copied().zip(ids).collect();
    let sample = sample_kv_entries(0x5ca1_ab1e, FILL_KEYS, SAMPLE_SIZE);

    eventually_with_logs(SETTLE_WAIT, &node_refs, || async {
        if sum_counts(&node_refs).await != Some(usize::from(OWNERS) * FILL_KEYS as usize) {
            return false;
        }
        ownership_snapshot_matches(tagged[0].0, &tagged, &sample, usize::from(OWNERS)).await
    })
    .await;

    let mismatches = fetch_mismatches(&node_refs, &sample).await;
    assert!(
        mismatches.is_empty(),
        "every sampled key must be fetchable with the right value from every node: {mismatches:?}"
    );

    for node in fleet.take() {
        node.stop().await.expect("node stops");
    }
    net.close().await.expect("network closes");
}
