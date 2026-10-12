//! The replicated-mode scenarios: convergence, tombstones, state transfer and churn.

use crate::container_util::{
    DISTRIBUTED_RUST_LOG, Node, eventually, seed, spawn_trio, wait_for_peers,
};
use crate::support::CONVERGE_WAIT;
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn convergence_across_three_nodes_with_distinct_writers() {
    require_containers!();

    let net = Arc::new(Network::new_network());
    let (n1, n2, n3) = spawn_trio(&net, "cvg-cluster").await;

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
    require_containers!();

    let net = Arc::new(Network::new_network());
    let (a, b, c) = spawn_trio(&net, "tomb-cluster").await;

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

    require_containers!();

    let net = Arc::new(Network::new_network());
    let log_env = [("RUST_LOG", DISTRIBUTED_RUST_LOG)];
    let n1 = Node::spawn_with_env(&net, "warm-cluster", "n1", &[], &log_env).await;
    let n2 = Node::spawn_with_env(&net, "warm-cluster", "n2", &[&seed("n1")], &log_env).await;
    let n3 = Node::spawn_with_env(
        &net,
        "warm-cluster",
        "n3",
        &[&seed("n1"), &seed("n2")],
        &log_env,
    )
    .await;
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
    let n4 = Node::spawn_with_env(
        &net,
        "warm-cluster",
        "n4",
        &[&seed("n1"), &seed("n2"), &seed("n3")],
        &log_env,
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
    require_containers!();

    let net = Arc::new(Network::new_network());
    let (n1, n2, n3) = spawn_trio(&net, "kill-cluster").await;

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

    require_containers!();

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

    require_containers!();

    let net = Arc::new(Network::new_network());
    let (n1, n2, n3) = spawn_trio(&net, "churn-cluster").await;

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
