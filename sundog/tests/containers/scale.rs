//! The million-entry and hundred-thousand-entry cold-join and compaction scenarios.

use crate::container_util::{
    CRDT_RETIRE_AFTER_SECS_ENV, DISTRIBUTED_RUST_LOG, Node, eventually, seed, wait_for_peers,
};
use crate::support::{CONVERGE_WAIT, scrape_metric};
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

/// Bound for a CRDT compaction wait: the sweep's own cadence
/// (`ClusterConfig::crdt_sweep_period`) is `(crdt_retire_after / 4).max(30s)`,
/// so with the test node's 5s bound a writer retires on one 30s-floor tick,
/// folds on a later one, and its receipt prunes once it is older than the
/// receipt lifetime of two bounds plus two sweep periods (70s) counted from
/// the fold, on the tick after that: about 150s end to end, with headroom
/// for container boot and gossip jitter on top.
const CRDT_COMPACT_WAIT: Duration = Duration::from_secs(300);

/// The 100k scenario, an order of magnitude up: a cold node joins a
/// three-node cluster holding a million entries and must warm to a full
/// copy inside a bound that still reads as startup, not outage.
#[tokio::test]
async fn cold_join_warms_a_million_entry_cluster() {
    const ENTRIES: u32 = 1_000_000;

    require_containers!();

    let net = Arc::new(Network::new_network());
    let log_env = [("RUST_LOG", DISTRIBUTED_RUST_LOG)];
    let n1 = Node::spawn_with_env(&net, "million-cluster", "n1", &[], &log_env).await;
    let n2 = Node::spawn_with_env(&net, "million-cluster", "n2", &[&seed("n1")], &log_env).await;
    wait_for_peers(&[&n1, &n2], 1).await;

    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));
    // n2 warms via live fan-out, so the joiner has two full donors.
    eventually(Duration::from_secs(180), || async {
        n2.count().await == Ok(ENTRIES as usize)
    })
    .await;

    let started = std::time::Instant::now();
    let n3 = Node::spawn_with_env(
        &net,
        "million-cluster",
        "n3",
        &[&seed("n1"), &seed("n2")],
        &log_env,
    )
    .await;
    eventually(Duration::from_secs(300), || async {
        n3.count().await == Ok(ENTRIES as usize)
    })
    .await;
    let warm = started.elapsed();
    println!("cold join warmed {ENTRIES} entries in {warm:?} (incl. container boot)");
    assert!(
        warm < Duration::from_secs(120),
        "cold join took {warm:?}, past the million-entry bar"
    );
    assert_eq!(n3.get("k0").await, Ok(Some("v0".to_string())));
    assert_eq!(n3.get("k999999").await, Ok(Some("v999999".to_string())));

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// The million-entry cold-join scenario's counter analogue: three nodes
/// under `sundog::crdt::PnCounterResolver` (`sundog-testnode`'s `"pn"`
/// cache) each increment every one of a million counters once,
/// concurrently, then a fourth node joins cold. Every node must converge
/// to the exact total (`WRITERS`) on every counter, not merely to some
/// value every replica happens to agree on, and the cold-joining node's
/// state transfer must carry that merged total to every key, the same bar
/// [`cold_join_warms_a_million_entry_cluster`] sets for plain LWW entries.
#[tokio::test]
async fn cold_join_warms_a_million_counter_cluster_with_exact_totals() {
    const COUNTERS: u32 = 1_000_000;
    const WRITERS: i64 = 3;
    // Three million conflicting increments fold on every node before the
    // join starts; that settle takes longer than the file-level wait on a
    // four-core box, so this scenario sizes its own.
    const CONVERGE_WAIT: Duration = Duration::from_secs(180);

    require_containers!();

    let net = Arc::new(Network::new_network());
    let log_env = [("RUST_LOG", DISTRIBUTED_RUST_LOG)];
    let n1 = Node::spawn_with_env(&net, "pncounter-cluster", "n1", &[], &log_env).await;
    let n2 = Node::spawn_with_env(&net, "pncounter-cluster", "n2", &[&seed("n1")], &log_env).await;
    let n3 = Node::spawn_with_env(
        &net,
        "pncounter-cluster",
        "n3",
        &[&seed("n1"), &seed("n2")],
        &log_env,
    )
    .await;
    wait_for_peers(&[&n1, &n2, &n3], 2).await;

    // Every node increments every counter once, concurrently: `pn0..pn(COUNTERS
    // - 1)` must converge to a total of `WRITERS`, not whichever
    // increment's write happens to land last.
    let (r1, r2, r3) = tokio::join!(
        n1.pn_fill(COUNTERS),
        n2.pn_fill(COUNTERS),
        n3.pn_fill(COUNTERS)
    );
    r1.expect("n1 pnfill succeeds");
    r2.expect("n2 pnfill succeeds");
    r3.expect("n3 pnfill succeeds");

    let settle_started = std::time::Instant::now();
    for node in [&n1, &n2, &n3] {
        eventually(CONVERGE_WAIT, || async {
            node.pn_count().await == Ok(COUNTERS as usize)
        })
        .await;
    }
    let last_key = format!("pn{}", COUNTERS - 1);
    for node in [&n1, &n2, &n3] {
        eventually(CONVERGE_WAIT, || async {
            node.pn_get("pn0").await == Ok(Some(WRITERS))
                && node.pn_get(&last_key).await == Ok(Some(WRITERS))
        })
        .await;
    }
    let settle = settle_started.elapsed();
    println!("three writers settled {COUNTERS} counters to the exact total in {settle:?}");

    let (f1_before, b1_before) = n1.netstats().await.expect("n1 netstats before the join");
    let (f2_before, b2_before) = n2.netstats().await.expect("n2 netstats before the join");
    let (f3_before, b3_before) = n3.netstats().await.expect("n3 netstats before the join");

    let started = std::time::Instant::now();
    let n4 = Node::spawn_with_env(
        &net,
        "pncounter-cluster",
        "n4",
        &[&seed("n1"), &seed("n2"), &seed("n3")],
        &log_env,
    )
    .await;
    eventually(Duration::from_secs(300), || async {
        n4.pn_count().await == Ok(COUNTERS as usize)
    })
    .await;
    let warm = started.elapsed();

    let (f1_after, b1_after) = n1.netstats().await.expect("n1 netstats after the join");
    let (f2_after, b2_after) = n2.netstats().await.expect("n2 netstats after the join");
    let (f3_after, b3_after) = n3.netstats().await.expect("n3 netstats after the join");
    let frames_for_join = (f1_after - f1_before) + (f2_after - f2_before) + (f3_after - f3_before);
    let bytes_for_join = (b1_after - b1_before) + (b2_after - b2_before) + (b3_after - b3_before);
    let resident_entries = n4.pn_count().await.expect("pncount");
    println!(
        "cold join warmed {COUNTERS} counters in {warm:?} (incl. container boot); donors sent \
         {frames_for_join} frames / {bytes_for_join} bytes; n4 holds {resident_entries} \
         resident entries"
    );
    assert!(
        warm < Duration::from_secs(120),
        "cold join took {warm:?}, past the million-counter bar"
    );

    assert_eq!(n4.pn_get("pn0").await, Ok(Some(WRITERS)));
    assert_eq!(n4.pn_get(&last_key).await, Ok(Some(WRITERS)));

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    n4.stop().await.expect("n4 stops");
    net.close().await.expect("network closes");
}

/// Bounds CRDT metadata growth under sustained writer churn: a third node
/// is repeatedly joined under the same alias, writes to both the `"pn"`
/// `PnCounter` and `"os"` `OrSet<String>` caches, and is stopped again, a
/// fresh process mints a fresh membership incarnation and therefore a
/// fresh [`sundog::crdt::WriterId`], so this exercises genuine writer
/// churn, not repeated writes from one identity, over a window exceeding
/// a shortened `crdt_retire_after`. Asserts `pn`'s totals stay exact
/// despite the churn, `os`'s membership keeps every churned writer's
/// never-removed element (retirement blocks only a *future* add under a
/// retired writer's incarnation), and that every counter's resident
/// encoded size after the churn window and its compaction sweeps is no
/// larger than before any churn beyond the fold's own aggregate slot,
/// proving the two-stage fold bounds resident record size instead of it
/// growing with every replaced writer's incarnation. A fresh cold join at
/// the end carries the compacted records with their exact totals.
///
/// Uses [`CRDT_RETIRE_AFTER_SECS_ENV`] (see that constant's doc) to shorten
/// `ClusterConfig::crdt_retire_after` well below its 24h default, so the
/// compaction-metric wait below settles within the test's own timeout.
#[allow(clippy::too_many_lines, reason = "one scripted end-to-end scenario")]
#[tokio::test]
async fn churn_at_scale_bounds_crdt_record_size_across_writer_replacement() {
    const RETIRE_AFTER_SECS: u64 = 5;
    const CHURN_ROUNDS: u32 = 5;
    const PN_KEYS: u32 = 10;
    const OS_KEY: &str = "churn-set";
    /// A fully compacted counter encodes to the same size as one that
    /// never saw the churned writers, so this only leaves room for one
    /// fold receipt still waiting out its lifetime past the fold on a replica.
    const FOLD_SLACK_BYTES: u64 = 32;

    require_containers!();

    let retire_secs = RETIRE_AFTER_SECS.to_string();
    let env = [(CRDT_RETIRE_AFTER_SECS_ENV, retire_secs.as_str())];

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn_with_env(&net, "churn-scale-cluster", "n1", &[], &env).await;
    let n2 = Node::spawn_with_env(&net, "churn-scale-cluster", "n2", &[&seed("n1")], &env).await;
    wait_for_peers(&[&n1, &n2], 1).await;

    n1.pn_fill(PN_KEYS).await.expect("n1 pnfill");
    n2.pn_fill(PN_KEYS).await.expect("n2 pnfill");
    let mut expected_total: i64 = 2;
    let last_key = format!("pn{}", PN_KEYS - 1);
    for node in [&n1, &n2] {
        eventually(CONVERGE_WAIT, || async {
            node.pn_get("pn0").await == Ok(Some(expected_total))
                && node.pn_get(&last_key).await == Ok(Some(expected_total))
        })
        .await;
    }

    // "Before": every counter's resident encoded size with only n1's and
    // n2's two writers on it, no churn or compaction yet.
    let bytes_before = n1.pn_bytes(PN_KEYS).await.expect("n1 pnbytes before churn");
    let per_entry_before = bytes_before / u64::from(PN_KEYS);

    // Churn: a third writer joins, contributes to every pn key and adds a
    // never-removed os element, then is stopped, `CHURN_ROUNDS` times,
    // each under a fresh incarnation and therefore a fresh `WriterId`,
    // spanning well past `crdt_retire_after`.
    for round in 0..CHURN_ROUNDS {
        let churner = Node::spawn_with_env(
            &net,
            "churn-scale-cluster",
            "churner",
            &[&seed("n1"), &seed("n2")],
            &env,
        )
        .await;
        wait_for_peers(&[&n1, &n2, &churner], 2).await;

        churner.pn_fill(PN_KEYS).await.expect("churner pnfill");
        churner
            .os_add(OS_KEY, &format!("e{round}"))
            .await
            .expect("churner osadd");
        expected_total += 1;

        eventually(CONVERGE_WAIT, || async {
            n1.pn_get("pn0").await == Ok(Some(expected_total))
                && n2.pn_get("pn0").await == Ok(Some(expected_total))
        })
        .await;
        let expected_so_far: Vec<String> = (0..=round).map(|r| format!("e{r}")).collect();
        eventually(CONVERGE_WAIT, || async {
            n1.os_members(OS_KEY).await == Ok(Some(expected_so_far.clone()))
        })
        .await;

        churner.stop().await.expect("churner stops");
        wait_for_peers(&[&n1, &n2], 1).await;
    }

    // Both compaction stages, observed rather than slept for and logged
    // on every poll so a miss on CI shows how far the sweep got: stage
    // one retires every churned writer (cadence `(crdt_retire_after /
    // 4).max(30s)`, see `cluster.rs::crdt_compact_task`); stage two folds
    // them once the cache has been quiet for `2 * crdt_retire_after` and
    // prunes its fold receipts three bounds past each fold, each on the next tick.
    let bound = per_entry_before + FOLD_SLACK_BYTES;
    let started = std::time::Instant::now();
    let mut retired_done = false;
    let mut last_bytes = (0, 0);
    loop {
        let retired = (
            scrape_metric(&n1, "sundog_crdt_retired_writers_total", ("cache", "pn")).await,
            scrape_metric(&n2, "sundog_crdt_retired_writers_total", ("cache", "pn")).await,
        );
        let compactions = (
            scrape_metric(&n1, "sundog_crdt_compactions_total", ("cache", "pn")).await,
            scrape_metric(&n2, "sundog_crdt_compactions_total", ("cache", "pn")).await,
        );
        let bytes = (
            n1.pn_bytes(PN_KEYS)
                .await
                .expect("n1 pnbytes during compaction"),
            n2.pn_bytes(PN_KEYS)
                .await
                .expect("n2 pnbytes during compaction"),
        );
        let per_entry = bytes.0.max(bytes.1) / u64::from(PN_KEYS);
        println!(
            "compaction at {:?}: retired n1={} n2={}, compactions n1={} n2={}, pn bytes n1={} \
             n2={} ({per_entry}/entry, bound {bound})",
            started.elapsed(),
            retired.0,
            retired.1,
            compactions.0,
            compactions.1,
            bytes.0,
            bytes.1
        );
        if bytes != last_bytes {
            println!("  n1 pn0: {}", n1.pn_dump("pn0").await.expect("n1 pndump"));
            println!("  n2 pn0: {}", n2.pn_dump("pn0").await.expect("n2 pndump"));
            last_bytes = bytes;
        }
        // A writer retired on one replica reaches the other already
        // retired through anti-entropy, so only the sum across both sweeps
        // is guaranteed to reach the churned count.
        if !retired_done && retired.0 + retired.1 >= u64::from(CHURN_ROUNDS) {
            retired_done = true;
            println!("every churned writer retired on some replica");
        }
        if retired_done && per_entry <= bound {
            break;
        }
        assert!(
            started.elapsed() < CRDT_COMPACT_WAIT,
            "compaction did not settle within {CRDT_COMPACT_WAIT:?}: retired n1={} n2={} \
             (need {CHURN_ROUNDS} in total), pn bytes {per_entry}/entry against a bound of \
             {bound}",
            retired.0,
            retired.1
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // "After": the same size once every churned writer has been folded
    // away, despite `CHURN_ROUNDS` more distinct writer identities having
    // touched every counter than in the "before" measurement, read on both
    // replicas since each one compacts on its own sweep.
    let bytes_after_n1 = n1
        .pn_bytes(PN_KEYS)
        .await
        .expect("n1 pnbytes after compaction");
    let bytes_after_n2 = n2
        .pn_bytes(PN_KEYS)
        .await
        .expect("n2 pnbytes after compaction");
    let per_entry_after = bytes_after_n1.max(bytes_after_n2) / u64::from(PN_KEYS);
    println!(
        "record size per pn entry: {per_entry_before} bytes/entry before {CHURN_ROUNDS} \
         writers churned through, {per_entry_after} bytes/entry after they were all retired \
         and compacted"
    );
    assert!(
        per_entry_after <= bound,
        "resident record size grew from {per_entry_before} to {per_entry_after} bytes/entry \
         across {CHURN_ROUNDS} replaced writers; compaction should keep it bounded, not \
         growing with churn"
    );

    // Exact totals survive every retirement.
    for node in [&n1, &n2] {
        eventually(CONVERGE_WAIT, || async {
            node.pn_get("pn0").await == Ok(Some(expected_total))
                && node.pn_get(&last_key).await == Ok(Some(expected_total))
        })
        .await;
    }
    let expected_members: Vec<String> = (0..CHURN_ROUNDS).map(|r| format!("e{r}")).collect();
    assert_eq!(
        n1.os_members(OS_KEY).await,
        Ok(Some(expected_members)),
        "every churned writer's never-removed element must survive retirement"
    );

    // A fresh cold join carries the compacted records with exact totals.
    let probe2 = Node::spawn_with_env(
        &net,
        "churn-scale-cluster",
        "probe2",
        &[&seed("n1"), &seed("n2")],
        &env,
    )
    .await;
    eventually(CONVERGE_WAIT, || async {
        probe2.pn_count().await == Ok(PN_KEYS as usize)
    })
    .await;
    assert_eq!(probe2.pn_get("pn0").await, Ok(Some(expected_total)));

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    probe2.stop().await.expect("probe2 stops");
    net.close().await.expect("network closes");
}

/// A cold node joining a populated cluster warms via state transfer in
/// seconds, at 100k-entry scale; the printed duration is the number to watch.
#[tokio::test]
async fn cold_join_warms_a_hundred_thousand_entry_cluster_in_seconds() {
    const ENTRIES: u32 = 100_000;

    require_containers!();

    let net = Arc::new(Network::new_network());
    let log_env = [("RUST_LOG", DISTRIBUTED_RUST_LOG)];
    let n1 = Node::spawn_with_env(&net, "scale-cluster", "n1", &[], &log_env).await;
    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));

    let started = std::time::Instant::now();
    let n2 = Node::spawn_with_env(&net, "scale-cluster", "n2", &[&seed("n1")], &log_env).await;
    // Window is wider than the pass bar, so a slow run fails on duration.
    eventually(Duration::from_secs(120), || async {
        n2.count().await == Ok(ENTRIES as usize)
    })
    .await;
    let warm = started.elapsed();
    println!("cold join warmed {ENTRIES} entries in {warm:?} (incl. container boot)");
    assert!(
        warm < Duration::from_secs(30),
        "cold join took {warm:?}, past the warm-in-seconds bar"
    );
    assert_eq!(n2.get("k0").await, Ok(Some("v0".to_string())));
    assert_eq!(n2.get("k99999").await, Ok(Some("v99999".to_string())));

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    net.close().await.expect("network closes");
}
