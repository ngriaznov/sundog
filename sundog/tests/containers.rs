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

use container_util::{Node, container_tests_enabled, eventually};
use rightsize::Network;

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
    assert!(
        frames_for_fill < 1_000,
        "n1 sent {frames_for_fill} frames for a {ENTRIES}-entry fill to 2 peers; the batched \
         fan-out queue should coalesce this into a few dozen `ReplicateBatch` frames per peer, \
         not one frame per record"
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
