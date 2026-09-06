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

mod container_util;

use std::sync::Arc;
use std::time::Duration;

use container_util::{
    METRICS_PORT, Node, build_previous_testnode, container_tests_enabled, eventually,
};
use futures::stream::{self, StreamExt as _};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rightsize::{Network, Wait};

/// Every `sundog-testnode` binds gossip on this fixed port; seed strings
/// below are `<alias>:<GOSSIP_PORT>`, resolved via DNS against the alias.
const GOSSIP_PORT: u16 = 7946;

const PEER_WAIT: Duration = Duration::from_secs(30);
const CONVERGE_WAIT: Duration = Duration::from_secs(20);

fn seed(alias: &str) -> String {
    format!("{alias}:{GOSSIP_PORT}")
}

async fn wait_for_peers(nodes: &[&Node], expected: usize) {
    for node in nodes {
        eventually(PEER_WAIT, || async { node.peers().await == Ok(expected) }).await;
    }
}

#[tokio::test]
async fn convergence_across_three_nodes_with_distinct_writers() {
    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "cvg-cluster", "n1", &[]).await;
    let n2 = Node::spawn(&net, "cvg-cluster", "n2", &[&seed("n1")]).await;
    let n3 = Node::spawn(&net, "cvg-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&n1, &n2, &n3], 2).await;

    let writers: [(&Node, &str); 3] = [(&n1, "n1"), (&n2, "n2"), (&n3, "n3")];
    for (node, label) in writers {
        for i in 0..5 {
            node.put(&format!("{label}-{i}"), &format!("val-{label}-{i}"))
                .await
                .expect("put succeeds");
        }
    }

    for node in [&n1, &n2, &n3] {
        eventually(CONVERGE_WAIT, || async { node.count().await == Ok(15) }).await;
    }

    // Spot-check gets on keys written by a different node than the reader.
    assert_eq!(
        n1.get("n3-2").await,
        Ok(Some("val-n3-2".to_string())),
        "n1 sees a key n3 wrote"
    );
    assert_eq!(
        n2.get("n1-4").await,
        Ok(Some("val-n1-4".to_string())),
        "n2 sees a key n1 wrote"
    );
    assert_eq!(
        n3.get("n2-0").await,
        Ok(Some("val-n2-0".to_string())),
        "n3 sees a key n2 wrote"
    );

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

#[tokio::test]
async fn tombstone_reaches_every_node() {
    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let a = Node::spawn(&net, "tomb-cluster", "n1", &[]).await;
    let b = Node::spawn(&net, "tomb-cluster", "n2", &[&seed("n1")]).await;
    let c = Node::spawn(&net, "tomb-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&a, &b, &c], 2).await;

    a.put("k", "v").await.expect("a puts");
    eventually(CONVERGE_WAIT, || async {
        b.get("k").await == Ok(Some("v".to_string()))
            && c.get("k").await == Ok(Some("v".to_string()))
    })
    .await;

    a.del("k").await.expect("a deletes");
    eventually(CONVERGE_WAIT, || async {
        b.get("k").await == Ok(None) && c.get("k").await == Ok(None)
    })
    .await;

    a.stop().await.expect("a stops");
    b.stop().await.expect("b stops");
    c.stop().await.expect("c stops");
    net.close().await.expect("network closes");
}

#[tokio::test]
async fn warm_join_state_transfer_with_no_new_writes() {
    const ENTRIES: usize = 500;

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "warm-cluster", "n1", &[]).await;
    let n2 = Node::spawn(&net, "warm-cluster", "n2", &[&seed("n1")]).await;
    let n3 = Node::spawn(&net, "warm-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&n1, &n2, &n3], 2).await;

    for i in 0..ENTRIES {
        n1.put(&format!("k{i}"), &format!("v{i}"))
            .await
            .expect("preload put succeeds");
    }
    for node in [&n1, &n2, &n3] {
        eventually(CONVERGE_WAIT, || async {
            node.count().await == Ok(ENTRIES)
        })
        .await;
    }

    // n1..n3 are already full; state transfer for n4 runs before its
    // control listener binds, so nothing writes after this point.
    let n4 = Node::spawn(
        &net,
        "warm-cluster",
        "n4",
        &[&seed("n1"), &seed("n2"), &seed("n3")],
    )
    .await;
    eventually(CONVERGE_WAIT, || async { n4.count().await == Ok(ENTRIES) }).await;
    assert_eq!(n4.get("k0").await, Ok(Some("v0".to_string())));
    assert_eq!(
        n4.get(&format!("k{}", ENTRIES - 1)).await,
        Ok(Some(format!("v{}", ENTRIES - 1)))
    );

    for node in [n1, n2, n3, n4] {
        node.stop().await.expect("node stops");
    }
    net.close().await.expect("network closes");
}

#[tokio::test]
async fn kill_one_node_and_replace_it_under_the_same_alias() {
    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "kill-cluster", "n1", &[]).await;
    let n2 = Node::spawn(&net, "kill-cluster", "n2", &[&seed("n1")]).await;
    let n3 = Node::spawn(&net, "kill-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&n1, &n2, &n3], 2).await;

    n1.put("before", "v-before").await.expect("n1 puts");
    eventually(CONVERGE_WAIT, || async {
        n2.get("before").await == Ok(Some("v-before".to_string()))
            && n3.get("before").await == Ok(Some("v-before".to_string()))
    })
    .await;

    n3.stop().await.expect("n3 stops");
    wait_for_peers(&[&n1, &n2], 1).await;

    n1.put("after", "v-after").await.expect("n1 puts");
    eventually(CONVERGE_WAIT, || async {
        n2.get("after").await == Ok(Some("v-after".to_string()))
    })
    .await;

    let replacement = Node::spawn(&net, "kill-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&n1, &n2, &replacement], 2).await;

    eventually(CONVERGE_WAIT, || async {
        replacement.get("before").await == Ok(Some("v-before".to_string()))
            && replacement.get("after").await == Ok(Some("v-after".to_string()))
    })
    .await;

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    replacement.stop().await.expect("replacement stops");
    net.close().await.expect("network closes");
}

/// `sundog-testnode`'s control protocol has no way to make a live member
/// miss one fan-out message, so stopping and restarting under the same
/// alias is the closest reachable equivalent: the restart's `open()` runs
/// state transfer and one anti-entropy round before `testnode-ready`
/// prints, exercising the same repair path anti-entropy exists for.
#[tokio::test]
async fn anti_entropy_repairs_a_gap_after_a_member_returns() {
    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "ae-cluster", "n1", &[]).await;
    let n2 = Node::spawn(&net, "ae-cluster", "n2", &[&seed("n1")]).await;
    let n3 = Node::spawn(&net, "ae-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&n1, &n2, &n3], 2).await;

    n1.put("steady", "v0").await.expect("n1 puts");
    eventually(CONVERGE_WAIT, || async {
        n2.get("steady").await == Ok(Some("v0".to_string()))
            && n3.get("steady").await == Ok(Some("v0".to_string()))
    })
    .await;

    n3.stop().await.expect("n3 stops");
    wait_for_peers(&[&n1, &n2], 1).await;

    n1.put("gap", "v1").await.expect("n1 puts while n3 is down");
    eventually(CONVERGE_WAIT, || async {
        n2.get("gap").await == Ok(Some("v1".to_string()))
    })
    .await;

    let n3 = Node::spawn(&net, "ae-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    eventually(Duration::from_secs(15), || async {
        n3.get("steady").await == Ok(Some("v0".to_string()))
            && n3.get("gap").await == Ok(Some("v1".to_string()))
    })
    .await;

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// Realistic value sizes: 4,096 entries of 64 KiB each, ~256 MiB of
/// payload per replica, proving live replication and a cold-join snapshot
/// both move that volume intact. The tail checks the frame-cap boundary
/// end to end: a near-cap value inserts fine, an over-cap value errors.
#[tokio::test]
async fn replication_and_cold_join_carry_realistic_value_sizes() {
    const ENTRIES: u32 = 4_096;
    const VALUE_BYTES: usize = 64 * 1024;
    const NEAR_CAP_BYTES: usize = 3 * 1024 * 1024;
    const OVER_CAP_BYTES: usize = 5 * 1024 * 1024;

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "bigval-cluster", "n1", &[]).await;
    let n2 = Node::spawn(&net, "bigval-cluster", "n2", &[&seed("n1")]).await;
    wait_for_peers(&[&n1, &n2], 1).await;

    n1.big_fill(ENTRIES, VALUE_BYTES)
        .await
        .expect("bulk large-value fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));
    eventually(Duration::from_secs(120), || async {
        n2.count().await == Ok(ENTRIES as usize)
    })
    .await;

    let spot_checks = [0, ENTRIES / 2, ENTRIES - 1];
    for index in spot_checks {
        assert_eq!(
            n2.big_check(index, VALUE_BYTES).await,
            Ok("ok".to_string()),
            "replicated value big{index} arrives byte-identical on n2"
        );
    }

    let started = std::time::Instant::now();
    let n3 = Node::spawn(&net, "bigval-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    eventually(Duration::from_secs(180), || async {
        n3.count().await == Ok(ENTRIES as usize)
    })
    .await;
    println!(
        "cold join warmed {ENTRIES} x {VALUE_BYTES}-byte entries in {:?} (incl. container boot)",
        started.elapsed()
    );
    for index in spot_checks {
        assert_eq!(
            n3.big_check(index, VALUE_BYTES).await,
            Ok("ok".to_string()),
            "state-transferred value big{index} arrives byte-identical on n3"
        );
    }

    assert_eq!(
        n1.big_put(NEAR_CAP_BYTES).await,
        Ok("ok".to_string()),
        "a single near-frame-cap value inserts cleanly"
    );
    eventually(CONVERGE_WAIT, || async {
        n2.big_verify(NEAR_CAP_BYTES).await == Ok("ok".to_string())
            && n3.big_verify(NEAR_CAP_BYTES).await == Ok("ok".to_string())
    })
    .await;

    let over_cap = n1
        .big_put(OVER_CAP_BYTES)
        .await
        .expect("control round trip succeeds");
    assert!(
        over_cap.starts_with("err"),
        "an over-frame-cap insert is rejected with an error, got {over_cap:?}"
    );

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// High-frequency entry lifecycle: three nodes hammer the same 512-key
/// space on a 2s-TTL cache, 100k operations each, three inserts to every
/// remove. Every replica must agree, drain to zero once writes stop, and
/// stay at zero across further anti-entropy rounds.
#[tokio::test]
async fn high_churn_of_adds_removes_and_ttl_expiry_drains_cleanly() {
    const OPS: u32 = 100_000;

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "churn-cluster", "n1", &[]).await;
    let n2 = Node::spawn(&net, "churn-cluster", "n2", &[&seed("n1")]).await;
    let n3 = Node::spawn(&net, "churn-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&n1, &n2, &n3], 2).await;

    let (r1, r2, r3) = tokio::join!(n1.churn(OPS), n2.churn(OPS), n3.churn(OPS));
    r1.expect("n1 churn completes");
    r2.expect("n2 churn completes");
    r3.expect("n3 churn completes");

    // Counts drift together as TTL expires what churn wrote; agreement at
    // a sampled instant is the invariant, not any particular value.
    eventually(CONVERGE_WAIT, || async {
        let (a, b, c) = (
            n1.churn_count().await,
            n2.churn_count().await,
            n3.churn_count().await,
        );
        a.is_ok() && a == b && b == c
    })
    .await;

    // With writers stopped, everything ages past the TTL and drains.
    eventually(Duration::from_secs(30), || async {
        n1.churn_count().await == Ok(0)
            && n2.churn_count().await == Ok(0)
            && n3.churn_count().await == Ok(0)
    })
    .await;

    // Several AE intervals later, still empty: nothing pulled anything back.
    tokio::time::sleep(Duration::from_secs(6)).await;
    for (node, name) in [(&n1, "n1"), (&n2, "n2"), (&n3, "n3")] {
        assert_eq!(
            node.churn_count().await,
            Ok(0),
            "{name} stays empty after the churn cache drains"
        );
    }

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// The 100k scenario, an order of magnitude up: a cold node joins a
/// three-node cluster holding a million entries and must warm to a full
/// copy inside a bound that still reads as startup, not outage.
#[tokio::test]
async fn cold_join_warms_a_million_entry_cluster() {
    const ENTRIES: u32 = 1_000_000;

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "million-cluster", "n1", &[]).await;
    let n2 = Node::spawn(&net, "million-cluster", "n2", &[&seed("n1")]).await;
    wait_for_peers(&[&n1, &n2], 1).await;

    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));
    // n2 warms via live fan-out, so the joiner has two full donors.
    eventually(Duration::from_secs(180), || async {
        n2.count().await == Ok(ENTRIES as usize)
    })
    .await;

    let started = std::time::Instant::now();
    let n3 = Node::spawn(&net, "million-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
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

/// A cold node joining a populated cluster warms via state transfer in
/// seconds, at 100k-entry scale; the printed duration is the number to watch.
#[tokio::test]
async fn cold_join_warms_a_hundred_thousand_entry_cluster_in_seconds() {
    const ENTRIES: u32 = 100_000;

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "scale-cluster", "n1", &[]).await;
    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));

    let started = std::time::Instant::now();
    let n2 = Node::spawn(&net, "scale-cluster", "n2", &[&seed("n1")]).await;
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

/// Waits until `node`'s cumulative sent-frame/byte counters stop changing
/// across a 1s sample, so a `netstats` snapshot taken right after this
/// returns isolates whatever traffic comes next from any trailing activity
/// (a state-transfer stream's last chunks, or the first post-join
/// anti-entropy round) still draining at the moment a peer's local count
/// first matches the expected total.
/// # Panics
///
/// Panics if the counters are still changing once `timeout` elapses.
async fn wait_for_quiescent_netstats(node: &Node, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = node.netstats().await.expect("netstats");
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let current = node.netstats().await.expect("netstats");
        if current == last {
            return;
        }
        last = current;
        assert!(
            tokio::time::Instant::now() < deadline,
            "{}'s netstats never settled within {timeout:?}",
            node.name()
        );
    }
}

/// At 500k entries, `sundog-testnode`'s 1,024 buckets hold about 488 entries
/// apiece, past `ClusterConfig::default`'s `ae_sketch_min_bucket` of 384:
/// the repair below runs through the IBLT sketch path, not a full listing.
/// n2 cold-joins and warms first, so both replicas start byte-identical;
/// dropping one key locally on n2 then leaves exactly one bucket mismatched
/// for anti-entropy to close.
#[tokio::test]
async fn anti_entropy_repairs_a_dropped_key_at_sketch_scale() {
    const ENTRIES: u32 = 500_000;
    const TARGET_KEY: &str = "k123456";
    const TARGET_VALUE: &str = "v123456";

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "ae-sketch-cluster", "n1", &[]).await;
    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));

    let n2 = Node::spawn(&net, "ae-sketch-cluster", "n2", &[&seed("n1")]).await;
    eventually(Duration::from_secs(200), || async {
        n2.count().await == Ok(ENTRIES as usize)
    })
    .await;
    assert_eq!(n2.get(TARGET_KEY).await, Ok(Some(TARGET_VALUE.to_string())));

    n2.drop_key(TARGET_KEY)
        .await
        .expect("drop succeeds, standing in for a lost Replicate");
    assert_eq!(
        n2.get(TARGET_KEY).await,
        Ok(None),
        "n2's copy is gone locally right after the drop"
    );

    // sundog-testnode sets `ae_interval` to 2s; generous past that plus the
    // digest pass and sketch build/peel/pull round trip over 500k entries.
    eventually(Duration::from_secs(60), || async {
        n2.get(TARGET_KEY).await == Ok(Some(TARGET_VALUE.to_string()))
    })
    .await;

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    net.close().await.expect("network closes");
}

/// At a 1,000,000-entry fill, `sundog-testnode`'s 1,024 buckets hold about
/// 976 entries apiece. With `SUNDOG_TESTNODE_AE_PART_MIN_BUCKET` lowered to
/// [`PART_MIN_BUCKET`], well past that, the mismatched bucket below is
/// answered with 64 part digests instead of a ~976-entry listing; only the
/// one part the dropped key falls into then answers with its own small
/// listing. n2 cold-joins and warms first, so both replicas start
/// byte-identical; dropping one key locally on n2 then leaves exactly one
/// bucket, and within it one part, mismatched. The wire-cost measurement
/// waits for [`wait_for_quiescent_netstats`] first: n2's local count reaches
/// ENTRIES slightly before its state-transfer stream and the first
/// post-join anti-entropy round finish draining, and that unrelated tail
/// would otherwise land inside the measured window.
#[tokio::test]
async fn anti_entropy_repairs_a_dropped_key_through_part_digests() {
    const ENTRIES: u32 = 1_000_000;
    const TARGET_KEY: &str = "k123456";
    const TARGET_VALUE: &str = "v123456";
    const PART_MIN_BUCKET: &str = "512";
    /// A full bucket listing at this scale runs about 22 KB; the part path
    /// is ~512 B of part digests, plus a small listing for the one
    /// differing part, plus the repaired record itself.
    const REPAIR_BYTES_BUDGET: u64 = 16 * 1024;

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let env = [("SUNDOG_TESTNODE_AE_PART_MIN_BUCKET", PART_MIN_BUCKET)];
    let n1 = Node::spawn_with_env(&net, "ae-parts-cluster", "n1", &[], &env).await;
    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));

    let n2 = Node::spawn_with_env(&net, "ae-parts-cluster", "n2", &[&seed("n1")], &env).await;
    eventually(Duration::from_secs(200), || async {
        n2.count().await == Ok(ENTRIES as usize)
    })
    .await;
    assert_eq!(n2.get(TARGET_KEY).await, Ok(Some(TARGET_VALUE.to_string())));

    // n2's local count reaches ENTRIES slightly before its state-transfer
    // stream and the first post-join anti-entropy round finish draining;
    // waiting for n1's own sent-byte counter to go quiet isolates the
    // repair's cost below from that unrelated tail.
    // A shared CI runner can keep n1's counters moving well past 30 s while
    // the million-entry transfer and fan-out drain.
    wait_for_quiescent_netstats(&n1, Duration::from_secs(90)).await;

    let (frames_before, bytes_before) = n1.netstats().await.expect("netstats before the drop");

    n2.drop_key(TARGET_KEY)
        .await
        .expect("drop succeeds, standing in for a lost Replicate");
    assert_eq!(
        n2.get(TARGET_KEY).await,
        Ok(None),
        "n2's copy is gone locally right after the drop"
    );

    // sundog-testnode sets `ae_interval` to 2s; generous past that plus the
    // digest pass, part-digest comparison, and pull round trip.
    eventually(Duration::from_secs(60), || async {
        n2.get(TARGET_KEY).await == Ok(Some(TARGET_VALUE.to_string()))
    })
    .await;

    let (frames_after, bytes_after) = n1.netstats().await.expect("netstats after the repair");
    let frames_for_repair = frames_after - frames_before;
    let bytes_for_repair = bytes_after - bytes_before;
    println!("part-digest repair: n1 sent {frames_for_repair} frames / {bytes_for_repair} bytes");
    assert!(
        bytes_for_repair < REPAIR_BYTES_BUDGET,
        "n1 sent {bytes_for_repair} bytes to repair one dropped key out of a {REPAIR_BYTES_BUDGET}-byte \
         budget; a full bucket listing at this scale runs about 22 KB, so the part-digest path \
         should cost a small fraction of that"
    );
    assert_eq!(n2.get(TARGET_KEY).await, Ok(Some(TARGET_VALUE.to_string())));

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    net.close().await.expect("network closes");
}

/// `wire::RecordHeader`'s exact fixed width per record ahead of its key and
/// value bytes: `wall_ms` (8) + `logical` (4) + `node` (8) + `expires_at_ms`
/// (8) + `key_len` (4) + `value_len` (4) + `flags` (1). Not reachable from
/// here (`wire::RECORD_HEADER_LEN` is `pub(crate)`), so restated as the
/// documented layout rather than a bare magic number.
const RECORD_HEADER_BYTES: u64 = 8 + 4 + 8 + 8 + 4 + 4 + 1;

/// One wire-sized copy of `fill`'s deterministic `k{i}`/`v{i}` entries: each
/// record's key/value bytes plus its fixed [`RECORD_HEADER_BYTES`] header,
/// the dominant cost at these key/value sizes. Omits the handful of
/// `RawFrameHeader`/cache-name/length-delimiter bytes shared across a whole
/// batch, negligible once amortized over thousands of records per batch.
/// [`bulk_fill_replicates_without_anti_entropy_duplicating_it`] checks its
/// measured bytes against a multiple of this.
fn fill_payload_bytes(count: u32) -> u64 {
    (0..count)
        .map(|i| RECORD_HEADER_BYTES + (format!("k{i}").len() + format!("v{i}").len()) as u64)
        .sum()
}

/// Pins the fan-out queue and the anti-entropy streaming skip together: a
/// bulk fill on a live three-node cluster must replicate as a handful of
/// batched frames fanned out once per peer, not one frame per record and not
/// a second copy from anti-entropy racing in behind it.
#[tokio::test]
async fn bulk_fill_replicates_without_anti_entropy_duplicating_it() {
    const ENTRIES: u32 = 100_000;

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let n1 = Node::spawn(&net, "fanout-cluster", "n1", &[]).await;
    let n2 = Node::spawn(&net, "fanout-cluster", "n2", &[&seed("n1")]).await;
    let n3 = Node::spawn(&net, "fanout-cluster", "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&n1, &n2, &n3], 2).await;

    let (frames_before, bytes_before) = n1.netstats().await.expect("netstats before the fill");

    n1.fill(ENTRIES).await.expect("bulk fill succeeds");
    assert_eq!(n1.count().await, Ok(ENTRIES as usize));
    eventually(Duration::from_secs(120), || async {
        n2.count().await == Ok(ENTRIES as usize) && n3.count().await == Ok(ENTRIES as usize)
    })
    .await;

    let (frames_after, bytes_after) = n1.netstats().await.expect("netstats after the fill");
    let frames_for_fill = frames_after - frames_before;
    let bytes_for_fill = bytes_after - bytes_before;

    let payload_estimate = fill_payload_bytes(ENTRIES);
    // Two peers receive every record, so one frame per record is
    // 2 * ENTRIES frames. The batched fan-out queue coalesces at least ten
    // records per frame however slowly the fill runs on a loaded machine;
    // an idle 4-core box sends a few hundred frames in total.
    let record_sends = u64::from(ENTRIES) * 2;
    eprintln!("bulk fill: n1 sent {frames_for_fill} frames for {record_sends} record sends");
    assert!(
        frames_for_fill < record_sends / 10,
        "n1 sent {frames_for_fill} frames for a {ENTRIES}-entry fill to 2 peers; the batched \
         fan-out queue should coalesce at least ten records per frame, not one frame per record"
    );
    assert!(
        bytes_for_fill < payload_estimate * 3,
        "n1 sent {bytes_for_fill} bytes for a {ENTRIES}-entry fill against an estimated \
         single-copy wire payload of {payload_estimate} bytes (each entry's `k{{i}}`/`v{{i}}` \
         bytes plus its fixed {RECORD_HEADER_BYTES}-byte record header); a normal 2-peer fan-out \
         costs about 2x that, so 3x leaves headroom for batch/frame overhead without also \
         covering anti-entropy re-sending a duplicate copy behind it"
    );

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

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
/// picking match) so the picking logic — the part that must stay
/// deterministic for a given seed — is a plain, independently testable
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
/// `node_count` live nodes and `fill_keys` existing fill keys — crashes are
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
/// before returning — the point past which the next chaos iteration may pick
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
/// the same `"it"` content — count, digest, and a key sample all agreeing.
/// Exercises the class of bug 0.3.1 fixed (an anti-entropy round landing
/// during a bulk fill) by never letting the cluster settle before the next
/// disruption lands.
///
/// Gated on `SUNDOG_CONTAINER_TESTS=1` *and* `SUNDOG_CHAOS_SECS` (the run
/// length in seconds) being set; `SUNDOG_CHAOS_SEED` pins the scenario's
/// random choices for a repeatable replay, otherwise a fresh seed is drawn
/// and logged. The cluster's own timing is never seeded and never
/// deterministic — that unpredictability is the point of a chaos lane.
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

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }
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

/// A rolling upgrade in miniature: the previous release's node and this
/// one's share a cluster, each donates to and repairs the other, and every
/// message the old node receives is one it can decode.
#[tokio::test]
async fn the_previous_release_and_this_one_interoperate_in_both_roles() {
    const ENTRIES: u32 = 5_000;

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

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

/// Finds `metric{...,label="value",...} <number>` in Prometheus
/// text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Mirrors `tests/spill_replication.rs`'s own
/// copy, kept local since integration test binaries don't share code beyond
/// `mod container_util`.
fn metric_value(body: &str, metric: &str, label: (&str, &str)) -> Option<f64> {
    let wanted = format!("{}=\"{}\"", label.0, label.1);
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (labels, value) = rest.split_once('}')?;
        if !labels.split(',').any(|pair| pair == wanted) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// Scrapes `node`'s `/metrics` and reads one `metric{label}` value, `0` if
/// the scrape fails or the series has never been touched (a metric with no
/// writes yet is absent from the exposition, not printed as zero). Every
/// `sundog_spill_*`/`sundog_ae_repaired_total` series this file reads is an
/// exact-integer count in practice, so rounding it to `u64` here sidesteps
/// `clippy::float_cmp` entirely: every comparison below compares `u64`s,
/// never `f64`s, mirroring `tests/spill_bench.rs`'s `metric_count`.
async fn scrape_metric(node: &Node, metric: &str, label: (&str, &str)) -> u64 {
    let value = node
        .metrics()
        .await
        .ok()
        .and_then(|body| metric_value(&body, metric, label));
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "every metric read here is a nonnegative counter or gauge"
    )]
    let count = value.unwrap_or(0.0).round() as u64;
    count
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
/// ample room for.
fn spill_node_env(
    ram_budget_bytes: u64,
    spill_capacity_bytes: u64,
    region_bytes: u64,
    flush_queue_bytes: u64,
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

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

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
    // instead of falling back to a delete, so this can no longer happen at
    // all, unlike before this policy existed.
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

    // Concurrent, not sequential: each `stop()` waits out the container's
    // ungraceful-shutdown grace period (`sundog-testnode` does not trap
    // `SIGTERM`), so stopping three nodes one after another would triple
    // that wait for no reason.
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

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let total_weight = fill_weight_bytes(FILL_COUNT);
    let ram_budget_bytes = total_weight / 3;
    let spill_capacity_bytes = (total_weight + u64::from(FILL_COUNT) * 64) * 2;
    let a_env = spill_node_env(
        ram_budget_bytes,
        spill_capacity_bytes,
        SPILL_REGION_BYTES,
        spill_capacity_bytes,
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
    // failing early is expected and `eventually` simply keeps retrying.
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

/// Node ids for `nodes`, in the same order, read via each node's own `id`
/// control route.
async fn collect_node_ids(nodes: &[&Node]) -> Vec<u64> {
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
fn kv_entry(index: u32) -> (String, String) {
    (format!("k{index}"), format!("v{index}"))
}

/// [`kv_entry`] for every index in `0..count`: the full key/value set one
/// `fill(count)` call writes.
fn kv_range(count: u32) -> Vec<(String, String)> {
    (0..count).map(kv_entry).collect()
}

/// A uniform random sample of `sample_size` [`kv_entry`] pairs with indices
/// in `0..fill_keys`, from a fixed seed so a failing run's sample is
/// reproducible.
fn sample_kv_entries(seed: u64, fill_keys: u32, sample_size: usize) -> Vec<(String, String)> {
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
async fn ownership_snapshot_matches(
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
async fn fetch_mismatches(nodes: &[&Node], entries: &[(String, String)]) -> Vec<String> {
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

/// Sum of `count` across every node in `nodes`, `None` if any read fails —
/// the shared building block every distributed scenario's convergence poll
/// below sums to `owners * fill_keys`.
async fn sum_counts(nodes: &[&Node]) -> Option<usize> {
    let mut sum = 0usize;
    for node in nodes {
        sum += node.count().await.ok()?;
    }
    Some(sum)
}

/// Every `sundog-testnode` opens `"it"`'s `Mode::Distributed` bucket space
/// over `sundog::store::BUCKET_COUNT` buckets; with `owners` owners per
/// bucket, the summed `sundog_owned_buckets` gauge across every live node
/// settles at this many bucket-ownership assignments.
fn expected_owned_buckets_sum(owners: u64) -> u64 {
    sundog::store::BUCKET_COUNT as u64 * owners
}

/// Five distributed nodes fill a shared keyspace from one writer, and every
/// key lands on exactly `OWNERS` owners: `owners k` from any node names
/// exactly two ids, exactly those nodes' `get k` answers with the value and
/// every other node answers `none`, every node's `fetch k` returns the
/// value, the summed local `count` is `OWNERS * FILL_KEYS`, and the summed
/// `sundog_owned_buckets` gauge is every bucket assigned exactly `OWNERS`
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

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let mut nodes = Vec::with_capacity(NODE_COUNT);
    for (i, alias) in ALIASES.iter().enumerate() {
        let seeds: Vec<String> = ALIASES[..i].iter().map(|a| seed(a)).collect();
        let seed_refs: Vec<&str> = seeds.iter().map(String::as_str).collect();
        nodes.push(Node::spawn_distributed(&net, CLUSTER, alias, &seed_refs, Some(OWNERS)).await);
    }
    let node_refs: Vec<&Node> = nodes.iter().collect();
    wait_for_peers(&node_refs, NODE_COUNT - 1).await;

    node_refs[0]
        .fill(FILL_KEYS)
        .await
        .expect("bulk fill succeeds");

    let ids = collect_node_ids(&node_refs).await;
    let tagged: Vec<(&Node, u64)> = node_refs.iter().copied().zip(ids).collect();
    let sample = sample_kv_entries(0xd157_fe11, FILL_KEYS, SAMPLE_SIZE);
    let expected_owned_sum = expected_owned_buckets_sum(u64::from(OWNERS));

    eventually(CONVERGE_WAIT, || async {
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
            owned_sum += scrape_metric(node, "sundog_owned_buckets", ("cache", "it")).await;
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

    for node in nodes {
        node.stop().await.expect("node stops");
    }
    net.close().await.expect("network closes");
}

/// Crashing one owner never makes a key unfetchable — the surviving owner
/// keeps answering `fetch` through the dead peer's stale entry in the
/// ownership view — and once gossip notices the death and rebalance runs,
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

    if !container_tests_enabled() {
        eprintln!("skipping: SUNDOG_CONTAINER_TESTS=1 not set");
        return;
    }

    let net = Arc::new(Network::new_network());
    let mut nodes = Vec::with_capacity(NODE_COUNT);
    for (i, alias) in ALIASES.iter().enumerate() {
        let seeds: Vec<String> = ALIASES[..i].iter().map(|a| seed(a)).collect();
        let seed_refs: Vec<&str> = seeds.iter().map(String::as_str).collect();
        nodes.push(Node::spawn_distributed(&net, CLUSTER, alias, &seed_refs, Some(OWNERS)).await);
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

    let mut in_before = Vec::with_capacity(live.len());
    for node in &live {
        in_before
            .push(scrape_metric(node, "sundog_rebalance_buckets_total", ("direction", "in")).await);
    }

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
        let after =
            scrape_metric(node, "sundog_rebalance_buckets_total", ("direction", "in")).await;
        if after > *before {
            in_moved = true;
        }
    }
    assert!(
        in_moved,
        "at least one surviving node should have pulled rebalanced buckets in after the crash"
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

