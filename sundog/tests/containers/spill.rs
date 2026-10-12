//! The spill-tier scenarios.

use crate::container_util::{
    DISTRIBUTED_RUST_LOG, METRICS_PORT, Node, build_previous_testnode, build_testnode, eventually,
    seed, wait_for_peers,
};
use crate::distributed::{fetch_mismatches, kv_entry, sample_kv_entries, sum_counts};
use crate::support::{CONVERGE_WAIT, scrape_metric};
use futures::stream;
use futures::stream::StreamExt as _;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rightsize::{Network, Wait};
use std::sync::Arc;
use std::time::Duration;

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
