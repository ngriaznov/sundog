//! The seeded chaos scenarios: crashes, churn and drops.

use crate::container_util::{Node, eventually, seed, wait_for_peers};
use crate::distributed::{
    collect_node_ids, fetch_mismatches, kv_range, ownership_snapshot_matches, sum_counts,
    wait_for_entry_sum,
};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

/// Resolves this run's chaos seed: `SUNDOG_CHAOS_SEED` if set, else 8 random
/// bytes read as a `u64`. Either way, prints a GitHub Actions notice so a
/// failing run can be replayed exactly.
fn chaos_seed(secs: u64) -> u64 {
    let seed = std::env::var("SUNDOG_CHAOS_SEED").map_or_else(
        |_| rand::rng().random::<u64>(),
        |raw| raw.parse().expect("SUNDOG_CHAOS_SEED is a u64"),
    );
    eprintln!(
        "::notice title=chaos seed::replay with SUNDOG_CHAOS_SEED={seed} \
         SUNDOG_CHAOS_SECS={secs}"
    );
    seed
}

/// One randomly chosen chaos action, and everything an iteration needs to
/// carry it out. Kept as a value (rather than performed inline in the
/// picking match) so the picking logic, the part that must stay
/// deterministic for a given seed, is a plain, independently testable
/// function with no `.await` in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChaosAction {
    /// Crash a random node and respawn it under the same alias.
    Crash { node: usize },
    /// Run `ops` churn operations on a random live node.
    Churn { node: usize, ops: u32 },
    /// Drop one fill key's local copy on a random live node.
    Drop { node: usize, key_index: u32 },
    /// Rewrite a further chunk of fill keys on a random live node.
    Fill { node: usize, count: u32 },
    /// A burst of distinct-key puts on a random live node.
    Burst { node: usize, count: u32 },
}

/// Picks one [`ChaosAction`] from a fixed weighted distribution over
/// `node_count` live nodes and `fill_keys` existing fill keys, crashes are
/// deliberately rare (10%) since they are the most disruptive action and the
/// scenario needs most of its run at steady churn, not mid-recovery; the
/// remaining 90% splits across churn (30%), drop (20%), fill (15%), and a
/// burst of puts (25%). Pure and deterministic: the same `rng` state always
/// picks the same action.
fn pick_chaos_action(
    rng: &mut StdRng,
    node_count: usize,
    fill_keys: u32,
    churn_ops: u32,
    fill_count: u32,
    burst_count: u32,
) -> ChaosAction {
    let node = rng.random_range(0..node_count);
    match rng.random_range(0..100u32) {
        0..=9 => ChaosAction::Crash { node },
        10..=39 => ChaosAction::Churn {
            node,
            ops: churn_ops,
        },
        40..=59 => ChaosAction::Drop {
            node,
            key_index: rng.random_range(0..fill_keys),
        },
        60..=74 => ChaosAction::Fill {
            node,
            count: fill_count,
        },
        _ => ChaosAction::Burst {
            node,
            count: burst_count,
        },
    }
}

/// Spawns one node under `alias`, seeded from `seeds`: `Mode::Distributed`
/// with `owners` owners per bucket when `owners` is `Some`, or
/// `Mode::Replicated` (a plain [`Node::spawn`]) when `None`. The one spawn
/// decision [`spawn_chaos_cluster_mode`] and [`crash_and_respawn_mode`] both
/// need, so a chaos cluster and its mid-run replacements always agree on
/// which mode they run.
async fn spawn_chaos_node(
    net: &Arc<Network>,
    cluster: &str,
    alias: &str,
    seeds: &[&str],
    owners: Option<u8>,
) -> Node {
    match owners {
        Some(owners) => Node::spawn_distributed(net, cluster, alias, seeds, Some(owners)).await,
        None => Node::spawn(net, cluster, alias, seeds).await,
    }
}

/// Spawns `aliases.len()` nodes on `cluster`, each seeded from the aliases
/// already spawned before it, in the mode [`spawn_chaos_node`] decides from
/// `owners`, and waits for all of them to see every other one as a peer.
async fn spawn_chaos_cluster_mode(
    net: &Arc<Network>,
    cluster: &str,
    aliases: &[&str],
    owners: Option<u8>,
) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(aliases.len());
    for (i, alias) in aliases.iter().enumerate() {
        let seeds: Vec<String> = aliases[..i].iter().map(|a| seed(a)).collect();
        let seed_refs: Vec<&str> = seeds.iter().map(String::as_str).collect();
        nodes.push(spawn_chaos_node(net, cluster, alias, &seed_refs, owners).await);
    }
    wait_for_peers(&nodes.iter().collect::<Vec<_>>(), aliases.len() - 1).await;
    nodes
}

/// [`spawn_chaos_cluster_mode`] with `owners: None`, `Mode::Replicated`
/// throughout: [`chaos_crashes_churn_and_drops_still_converge`]'s own,
/// unchanged entry point.
async fn spawn_chaos_cluster(net: &Arc<Network>, cluster: &str, aliases: &[&str]) -> Vec<Node> {
    spawn_chaos_cluster_mode(net, cluster, aliases, None).await
}

/// Crashes `nodes[idx]`, respawns it under the same alias seeded from the
/// other still-live aliases in the mode [`spawn_chaos_node`] decides from
/// `owners`, and waits for every node to see the rest of the cluster again
/// before returning, the point past which the next chaos iteration may pick
/// another node to crash.
async fn crash_and_respawn_mode(
    nodes: &mut Vec<Node>,
    net: &Arc<Network>,
    cluster: &str,
    aliases: &[&str],
    idx: usize,
    iteration: u64,
    owners: Option<u8>,
) {
    let alias = aliases[idx];
    eprintln!("chaos[{iteration}]: crashing {alias}");
    let dead = nodes.remove(idx);
    dead.crash()
        .await
        .expect("crashed node dies and is removed cleanly");

    let seeds: Vec<String> = aliases
        .iter()
        .enumerate()
        .filter(|&(j, _)| j != idx)
        .map(|(_, a)| seed(a))
        .collect();
    let seed_refs: Vec<&str> = seeds.iter().map(String::as_str).collect();
    nodes.insert(
        idx,
        spawn_chaos_node(net, cluster, alias, &seed_refs, owners).await,
    );

    wait_for_peers(&nodes.iter().collect::<Vec<_>>(), aliases.len() - 1).await;
    eprintln!(
        "chaos[{iteration}]: {alias} respawned and saw {} peers",
        aliases.len() - 1
    );
}

/// [`crash_and_respawn_mode`] with `owners: None`,
/// [`chaos_crashes_churn_and_drops_still_converge`]'s own, unchanged entry
/// point.
async fn crash_and_respawn(
    nodes: &mut Vec<Node>,
    net: &Arc<Network>,
    cluster: &str,
    aliases: &[&str],
    idx: usize,
    iteration: u64,
) {
    crash_and_respawn_mode(nodes, net, cluster, aliases, idx, iteration, None).await;
}

/// Runs one non-crash [`ChaosAction`] and logs it; crashes are handled
/// separately by [`crash_and_respawn`] since only they touch `nodes` itself.
async fn perform_chaos_action(nodes: &[Node], action: ChaosAction, iteration: u64, run_seed: u64) {
    match action {
        ChaosAction::Crash { .. } => unreachable!("crashes are handled by crash_and_respawn"),
        ChaosAction::Churn { node: idx, ops } => {
            eprintln!(
                "chaos[{iteration}]: churn {ops} ops on {}",
                nodes[idx].name()
            );
            nodes[idx].churn(ops).await.expect("churn completes");
        }
        ChaosAction::Drop {
            node: idx,
            key_index,
        } => {
            let key = format!("k{key_index}");
            eprintln!(
                "chaos[{iteration}]: dropping {key} on {}",
                nodes[idx].name()
            );
            nodes[idx].drop_key(&key).await.expect("drop succeeds");
        }
        ChaosAction::Fill { node: idx, count } => {
            eprintln!(
                "chaos[{iteration}]: refilling {count} keys on {}",
                nodes[idx].name()
            );
            nodes[idx].fill(count).await.expect("refill succeeds");
        }
        ChaosAction::Burst { node: idx, count } => {
            eprintln!(
                "chaos[{iteration}]: bursting {count} puts on {}",
                nodes[idx].name()
            );
            for j in 0..count {
                let key = format!("burst-{run_seed:x}-{iteration}-{j}");
                let value = format!("v-{run_seed:x}-{iteration}-{j}");
                nodes[idx]
                    .put(&key, &value)
                    .await
                    .expect("burst put succeeds");
            }
        }
    }
}

/// Waits for every node's `count` and `digest` to agree, then reads
/// `sample_size` random fill keys off every node and asserts they all match
/// [`fill`][Node::fill]'s deterministic `k{i}`/`v{i}` content. The `churn`
/// cache carries a short TTL by design, so both checks stay on `"it"` only.
async fn assert_converged(
    nodes: &[Node],
    rng: &mut StdRng,
    fill_keys: u32,
    sample_size: usize,
    wait: Duration,
) {
    eventually(wait, || async {
        let mut counts = Vec::with_capacity(nodes.len());
        let mut digests = Vec::with_capacity(nodes.len());
        for node in nodes {
            counts.push(node.count().await);
            digests.push(node.digest().await);
        }
        counts.iter().all(Result::is_ok)
            && counts.windows(2).all(|w| w[0] == w[1])
            && digests.iter().all(Result::is_ok)
            && digests.windows(2).all(|w| w[0] == w[1])
    })
    .await;
    eprintln!("chaos: every node converged to the same count and digest");

    for _ in 0..sample_size {
        let key_index = rng.random_range(0..fill_keys);
        let key = format!("k{key_index}");
        let expected = format!("v{key_index}");
        let mut values = Vec::with_capacity(nodes.len());
        for node in nodes {
            values.push(node.get(&key).await);
        }
        assert!(
            values
                .iter()
                .all(|v| v.as_ref() == Ok(&Some(expected.clone()))),
            "{key} disagrees across nodes after convergence: {values:?}"
        );
    }
    eprintln!("chaos: {sample_size} sampled fill keys read identically on every node");
}

/// Random crashes, churn, dropped keys, refills, and put bursts against a
/// four-node cluster for a bounded time, then checks every node converges to
/// the same `"it"` content, count, digest, and a key sample all agreeing.
/// Exercises the class of bug 0.3.1 fixed (an anti-entropy round landing
/// during a bulk fill) by never letting the cluster settle before the next
/// disruption lands.
///
/// Gated on `SUNDOG_CONTAINER_TESTS=1` *and* `SUNDOG_CHAOS_SECS` (the run
/// length in seconds) being set; `SUNDOG_CHAOS_SEED` pins the scenario's
/// random choices for a repeatable replay, otherwise a fresh seed is drawn
/// and logged. The cluster's own timing is never seeded and never
/// deterministic, that unpredictability is the point of a chaos lane.
#[tokio::test]
async fn chaos_crashes_churn_and_drops_still_converge() {
    const NODE_COUNT: usize = 4;
    const FILL_KEYS: u32 = 20_000;
    const FILL_WAIT: Duration = Duration::from_secs(120);
    const CHURN_OPS: u32 = 500;
    const REFILL_COUNT: u32 = 2_000;
    const BURST_COUNT: u32 = 50;
    const CONVERGENCE_WAIT: Duration = Duration::from_secs(120);
    const SAMPLE_SIZE: usize = 100;
    const ALIASES: [&str; NODE_COUNT] = ["n1", "n2", "n3", "n4"];
    const CLUSTER: &str = "chaos-cluster";

    require_containers!();
    let Ok(secs_raw) = std::env::var("SUNDOG_CHAOS_SECS") else {
        eprintln!("skipping: SUNDOG_CHAOS_SECS not set");
        return;
    };
    let secs: u64 = secs_raw
        .parse()
        .expect("SUNDOG_CHAOS_SECS is a u64 seconds count");

    let run_seed = chaos_seed(secs);
    let mut rng = StdRng::seed_from_u64(run_seed);

    let net = Arc::new(Network::new_network());
    let mut nodes = spawn_chaos_cluster(&net, CLUSTER, &ALIASES).await;

    nodes[0]
        .fill(FILL_KEYS)
        .await
        .expect("initial fill succeeds");
    for node in &nodes {
        eventually(FILL_WAIT, || async {
            node.count().await == Ok(FILL_KEYS as usize)
        })
        .await;
    }
    eprintln!("chaos: {FILL_KEYS} keys filled and present on all {NODE_COUNT} nodes");

    let mut crashes = 0u32;
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    let mut iteration = 0u64;
    while std::time::Instant::now() < deadline {
        iteration += 1;
        let action = pick_chaos_action(
            &mut rng,
            nodes.len(),
            FILL_KEYS,
            CHURN_OPS,
            REFILL_COUNT,
            BURST_COUNT,
        );
        if let ChaosAction::Crash { node: idx } = action {
            crashes += 1;
            crash_and_respawn(&mut nodes, &net, CLUSTER, &ALIASES, idx, iteration).await;
        } else {
            perform_chaos_action(&nodes, action, iteration, run_seed).await;
        }
    }
    eprintln!(
        "chaos: ran {iteration} actions over {secs}s, including {crashes} crash/respawn cycles \
         (every one already confirmed 3 peers before the next action ran)"
    );

    assert_converged(&nodes, &mut rng, FILL_KEYS, SAMPLE_SIZE, CONVERGENCE_WAIT).await;

    for node in nodes {
        node.stop().await.expect("node stops");
    }
    net.close().await.expect("network closes");
}

/// [`chaos_crashes_churn_and_drops_still_converge`]'s convergence check
/// (agreement on `count`/`digest`) assumes `Mode::Replicated`, where every
/// node holds everything; a distributed cluster instead settles with each
/// key on exactly `owners` nodes. `fill_keys` is `0..fill_keys`'s `k{i}`/
/// `v{i}` keyspace (surviving `Drop`, a dropped bucket's other owner, or an
/// anti-entropy repair, still has it, and `Fill`, which only ever rewrites
/// a prefix of it to the same deterministic values); `burst_entries` is
/// every distinct-key put [`perform_chaos_action`]'s `Burst` arm wrote,
/// collected by the caller since those keys carry no predictable index.
/// Polled by [`eventually`] the same way [`assert_converged`] is, since
/// ownership catching up after the last action needs a few anti-entropy
/// intervals same as anywhere else.
async fn assert_converged_distributed(
    nodes: &[Node],
    fill_keys: u32,
    burst_entries: &[(String, String)],
    owners: usize,
    wait: Duration,
) {
    let node_refs: Vec<&Node> = nodes.iter().collect();
    let ids = collect_node_ids(&node_refs).await;
    let tagged: Vec<(&Node, u64)> = node_refs.iter().copied().zip(ids).collect();
    let expected_sum = owners * (fill_keys as usize + burst_entries.len());

    let fill_sample = kv_range(fill_keys);
    eventually(wait, || async {
        if sum_counts(&node_refs).await != Some(expected_sum) {
            return false;
        }
        ownership_snapshot_matches(tagged[0].0, &tagged, &fill_sample, owners).await
            && ownership_snapshot_matches(tagged[0].0, &tagged, burst_entries, owners).await
    })
    .await;
    eprintln!(
        "chaos-distributed: every node converged, {fill_keys} fill keys and {} burst keys each \
         on exactly {owners} owners",
        burst_entries.len()
    );

    let mismatches = fetch_mismatches(&node_refs, &fill_sample).await;
    assert!(
        mismatches.is_empty(),
        "every fill key must be fetchable with the right value from every node: {mismatches:?}"
    );
    let burst_mismatches = fetch_mismatches(&node_refs, burst_entries).await;
    assert!(
        burst_mismatches.is_empty(),
        "every burst key must be fetchable with the right value from every node: \
         {burst_mismatches:?}"
    );
}

/// The distributed-mode counterpart to
/// [`chaos_crashes_churn_and_drops_still_converge`]: same seeded action mix
/// (crash+respawn, churn on the always-replicated `"churn"` cache, drop,
/// refill, put bursts) against a distributed `"it"`, converging on
/// [`assert_converged_distributed`]'s ownership-aware check instead of
/// content agreement across every node. Shares every helper the replicated
/// chaos test uses, [`ChaosAction`], [`pick_chaos_action`],
/// [`perform_chaos_action`], [`spawn_chaos_cluster_mode`],
/// [`crash_and_respawn_mode`], parametrized on `owners` rather than
/// duplicated.
///
/// Gated the same way: `SUNDOG_CONTAINER_TESTS=1` and `SUNDOG_CHAOS_SECS`;
/// `SUNDOG_CHAOS_SEED` pins the run for a repeatable replay. Named with the
/// `chaos_` prefix so `nightly-chaos.yml` picks it up alongside the
/// replicated one.
#[tokio::test]
async fn chaos_distributed_crashes_churn_and_drops_still_converge() {
    const NODE_COUNT: usize = 4;
    const OWNERS: u8 = 2;
    const FILL_KEYS: u32 = 2_000;
    const FILL_WAIT: Duration = Duration::from_secs(90);
    const CHURN_OPS: u32 = 500;
    const REFILL_COUNT: u32 = 500;
    const BURST_COUNT: u32 = 20;
    const CONVERGENCE_WAIT: Duration = Duration::from_secs(180);
    const RESETTLE_WAIT: Duration = Duration::from_secs(180);
    const ALIASES: [&str; NODE_COUNT] = ["n1", "n2", "n3", "n4"];
    const CLUSTER: &str = "chaos-distributed-cluster";

    require_containers!();
    let Ok(secs_raw) = std::env::var("SUNDOG_CHAOS_SECS") else {
        eprintln!("skipping: SUNDOG_CHAOS_SECS not set");
        return;
    };
    let secs: u64 = secs_raw
        .parse()
        .expect("SUNDOG_CHAOS_SECS is a u64 seconds count");

    let run_seed = chaos_seed(secs);
    let mut rng = StdRng::seed_from_u64(run_seed);

    let net = Arc::new(Network::new_network());
    let mut nodes = spawn_chaos_cluster_mode(&net, CLUSTER, &ALIASES, Some(OWNERS)).await;

    nodes[0]
        .fill(FILL_KEYS)
        .await
        .expect("initial fill succeeds");
    eventually(FILL_WAIT, || async {
        sum_counts(&nodes.iter().collect::<Vec<_>>()).await
            == Some(usize::from(OWNERS) * FILL_KEYS as usize)
    })
    .await;
    eprintln!("chaos-distributed: {FILL_KEYS} keys filled, each on exactly {OWNERS} owners");

    // Every burst key this run ever wrote, mirroring `perform_chaos_action`'s
    // own `Burst` arm's key/value format exactly, so the convergence check
    // below knows the full expected keyspace beyond `0..FILL_KEYS`.
    let mut burst_entries: Vec<(String, String)> = Vec::new();
    let mut unrepaired_drops = std::collections::HashSet::new();
    let mut crashes = 0u32;
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    let mut iteration = 0u64;
    while std::time::Instant::now() < deadline {
        iteration += 1;
        let action = pick_chaos_action(
            &mut rng,
            nodes.len(),
            FILL_KEYS,
            CHURN_OPS,
            REFILL_COUNT,
            BURST_COUNT,
        );
        if let ChaosAction::Crash { node: idx } = action {
            // A crash right after a burst can take the only copy of a key
            // whose fan-out has not left the node yet, and a crash right
            // after a drop takes the second copy of a key anti-entropy has
            // not repaired yet: two failures against two owners, which no
            // design with two owners survives. Every crash therefore waits
            // for the cluster to be settled first, exactly as it waits
            // afterwards.
            let expected_sum = usize::from(OWNERS) * (FILL_KEYS as usize + burst_entries.len());
            wait_for_entry_sum(&nodes, expected_sum, RESETTLE_WAIT, iteration).await;
            crashes += 1;
            crash_and_respawn_mode(
                &mut nodes,
                &net,
                CLUSTER,
                &ALIASES,
                idx,
                iteration,
                Some(OWNERS),
            )
            .await;
            // With two owners per bucket, a crash leaves every bucket the
            // victim owned on one live node until its new owner has pulled
            // it; a second crash inside that window can take the last copy.
            // The next action waits until every key is back on two owners.
            wait_for_entry_sum(&nodes, expected_sum, RESETTLE_WAIT, iteration).await;
            unrepaired_drops.clear();
            eprintln!("chaos[{iteration}]: every key is back on {OWNERS} owners");
        } else {
            if let ChaosAction::Drop { key_index, .. } = action
                && repeats_an_unrepaired_drop(&mut unrepaired_drops, key_index)
            {
                let expected_sum = usize::from(OWNERS) * (FILL_KEYS as usize + burst_entries.len());
                wait_for_entry_sum(&nodes, expected_sum, RESETTLE_WAIT, iteration).await;
            }
            if let ChaosAction::Burst { count, .. } = action {
                for j in 0..count {
                    let key = format!("burst-{run_seed:x}-{iteration}-{j}");
                    let value = format!("v-{run_seed:x}-{iteration}-{j}");
                    burst_entries.push((key, value));
                }
            }
            perform_chaos_action(&nodes, action, iteration, run_seed).await;
        }
    }
    eprintln!(
        "chaos-distributed: ran {iteration} actions over {secs}s, including {crashes} \
         crash/respawn cycles"
    );

    assert_converged_distributed(
        &nodes,
        FILL_KEYS,
        &burst_entries,
        usize::from(OWNERS),
        CONVERGENCE_WAIT,
    )
    .await;

    for node in nodes {
        node.stop().await.expect("node stops");
    }
    net.close().await.expect("network closes");
}

/// Records a drop of fill key `key_index` in `unrepaired`, the fill keys
/// dropped since the cluster last settled; true when it already held the
/// key, and the cluster must settle before this drop, which then leaves the
/// key the only one recorded. Chaos actions follow each other within
/// milliseconds, far inside one anti-entropy interval: dropping a key again
/// before its first drop is repaired can take its other copy, the same two
/// failures against two owners a crash waits out. Pure; unit tested
/// directly.
fn repeats_an_unrepaired_drop(
    unrepaired: &mut std::collections::HashSet<u32>,
    key_index: u32,
) -> bool {
    if unrepaired.insert(key_index) {
        return false;
    }
    unrepaired.clear();
    unrepaired.insert(key_index);
    true
}

#[test]
fn only_a_repeat_drop_since_the_last_settle_waits_for_one() {
    let mut unrepaired = std::collections::HashSet::new();
    assert!(!repeats_an_unrepaired_drop(&mut unrepaired, 1593));
    assert!(!repeats_an_unrepaired_drop(&mut unrepaired, 829));
    assert!(repeats_an_unrepaired_drop(&mut unrepaired, 1593));
    assert_eq!(unrepaired, std::collections::HashSet::from([1593]));
    assert!(!repeats_an_unrepaired_drop(&mut unrepaired, 829));
    unrepaired.clear();
    assert!(!repeats_an_unrepaired_drop(&mut unrepaired, 1593));
}
