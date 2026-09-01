//! Container-backed multi-node scenarios for everything that genuinely
//! needs real, separate processes on a real network: membership
//! convergence, replication, tombstones, state transfer, and anti-entropy —
//! exclusively through the `rightsize` crate, no docker CLI, no `bollard`.
//! See `tests/container_util` for the
//! harness and `sundog-testnode` for the control protocol every [`Node`]
//! drives.
//!
//! Gated on `SUNDOG_CONTAINER_TESTS=1` (checked first thing in every test,
//! `eprintln!` + early return otherwise) rather than `#[ignore]`, so a plain
//! `cargo test --workspace` run still compiles and "passes" this binary
//! without a container backend or the musl target installed. Run for real:
//!
//! ```text
//! SUNDOG_CONTAINER_TESTS=1 SUNDOG_TEST_BASE_IMAGE=rz-base:local RIGHTSIZE_BACKEND=docker \
//!     cargo test --release -p sundog --test containers -- --test-threads=1
//! ```

// Unix-only alongside the rightsize dev-dependencies themselves (see
// `Cargo.toml`): rightsize-docker reaches the daemon over a Unix socket, so
// neither it nor this binary builds on Windows — and running the suite needs
// a Unix docker host regardless.
#![cfg(unix)]

mod container_util;

use std::sync::Arc;
use std::time::Duration;

use container_util::{Node, container_tests_enabled, eventually};
use rightsize::Network;

/// Every `sundog-testnode` binds gossip on this fixed port (see its own
/// module doc) — seed strings below are `<alias>:<GOSSIP_PORT>`, resolved
/// via ordinary DNS against the alias a sibling container registered on the
/// shared [`Network`].
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
        "n1 must see a key n3 wrote"
    );
    assert_eq!(
        n2.get("n1-4").await,
        Ok(Some("val-n1-4".to_string())),
        "n2 must see a key n1 wrote"
    );
    assert_eq!(
        n3.get("n2-0").await,
        Ok(Some("val-n2-0".to_string())),
        "n3 must see a key n2 wrote"
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

    // Joins with all three existing members already known-full; open()
    // (which `Node::spawn`'s ready-wait blocks on) runs state transfer
    // before the control listener ever binds, so no write happens after
    // this point — the assertion below is purely state transfer's doing.
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

/// `sundog-testnode`'s control protocol has no `invalidate_local`-equivalent
/// (unlike the crate's own dev-only escape hatch), and rightsize's `Network`
/// exposes no partition primitive on either backend — so there is no way,
/// black-box, to make a *live* member miss one live-fan-out message the way
/// `tests/anti_entropy_repair.rs` (the in-process suite this replaces) once
/// did via `Cache::invalidate_local`. Stopping and restarting a member under
/// the same alias is the closest honest equivalent reachable through this
/// harness: the restarted node's `open()` runs state transfer *and* one
/// immediate anti-entropy round against its donor before `testnode-ready`
/// ever prints, so this exercises the same repair path
/// anti-entropy exists for — a member that missed writes catching back up —
/// even though it can't isolate the periodic scheduler from the join-time
/// sweep. The tight bound below (a handful of `sundog-testnode`'s 2s
/// `ae_interval`) is what actually distinguishes this from a cold join like
/// `warm_join_state_transfer_with_no_new_writes`: convergence here must be
/// fast, not just eventual.
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

/// Realistic value sizes: every other scenario replicates values a few bytes
/// long, so this is the one where *bytes*, not entry count, dominate — 4,096
/// entries of 64 KiB each, ~256 MiB of payload per replica. It proves live
/// replication and a bytes-heavy cold-join snapshot both move that volume,
/// and — via node-side regeneration — that the content arrives intact, not
/// merely counted. The tail asserts the frame-cap boundary end to end: a
/// single 3 MiB value replicates fine, and an over-cap value is rejected at
/// `insert` with an error rather than accepted locally and silently dropped
/// on the wire.
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
            "replicated value big{index} must arrive byte-identical on n2"
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
            "state-transferred value big{index} must arrive byte-identical on n3"
        );
    }

    assert_eq!(
        n1.big_put(NEAR_CAP_BYTES).await,
        Ok("ok".to_string()),
        "a single near-frame-cap value must insert cleanly"
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
        "an over-frame-cap insert must be rejected with an error, got {over_cap:?}"
    );

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// High-frequency entry lifecycle: three nodes concurrently hammer the same
/// 512-key space on a 2s-TTL replicated cache — 100k operations each, three
/// inserts to every remove, no pacing — so the same key is constantly
/// inserted, removed, re-inserted, and TTL-expired across writers, and some
/// records replicate after they are already dead on arrival. The end-state
/// claims are what make it a test rather than a stress toy: every replica
/// must agree, then drain to zero on its own once the writes stop (TTL
/// deadlines are absolute and travel with records), and must *stay* at zero
/// across further anti-entropy rounds — nothing may resurrect an expired
/// entry or a tombstone.
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

    // Counts drift downward together as TTL keeps expiring what the churn
    // wrote — agreement at a sampled instant is the invariant here, not any
    // particular value.
    eventually(CONVERGE_WAIT, || async {
        let (a, b, c) = (
            n1.churn_count().await,
            n2.churn_count().await,
            n3.churn_count().await,
        );
        a.is_ok() && a == b && b == c
    })
    .await;

    // With the writers stopped, everything ages past the 2s TTL and every
    // replica must empty out on its own.
    eventually(Duration::from_secs(30), || async {
        n1.churn_count().await == Ok(0)
            && n2.churn_count().await == Ok(0)
            && n3.churn_count().await == Ok(0)
    })
    .await;

    // Several of the testnode's 2s anti-entropy intervals later, still
    // empty: no round has pulled an expired entry or tombstone back.
    tokio::time::sleep(Duration::from_secs(6)).await;
    for (node, name) in [(&n1, "n1"), (&n2, "n2"), (&n3, "n3")] {
        assert_eq!(
            node.churn_count().await,
            Ok(0),
            "{name} must stay empty after the churn cache drains"
        );
    }

    n1.stop().await.expect("n1 stops");
    n2.stop().await.expect("n2 stops");
    n3.stop().await.expect("n3 stops");
    net.close().await.expect("network closes");
}

/// The 100k scenario, an order of magnitude up: a cold node joins a
/// three-node cluster already holding a million entries and must warm to a
/// full copy — and the donors must agree it stayed a full copy — inside a
/// bound that still reads as "startup", not "outage". A million small
/// entries is roughly 300 MiB of process footprint per node, so this also
/// exercises snapshot chunking and the anti-entropy top-up at a data volume
/// where any accidentally quadratic path would blow straight through the
/// window.
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
    // n2 warms from live replication fan-out, so the joiner below has two
    // donors holding the full set, not one.
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
/// seconds, at 100k-entry scale. The elapsed
/// bound below includes container boot and gossip convergence on top of the
/// transfer itself, so it is deliberately generous; the printed duration is
/// the number to watch.
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
    // Observation window is wider than the pass bar so a slow run fails on
    // the measured duration below, not on an opaque poll timeout.
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
