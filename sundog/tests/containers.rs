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
/// immediate anti-entropy round against its donor (plan §9) before
/// `testnode-ready` ever prints, so this exercises the same repair path
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
