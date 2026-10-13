//! The `Mode::Invalidation` scenario for a merging resolver: a write ships
//! the writer's merged record, a node holding the key folds it in, and a node
//! that does not hold the key ignores it.

use crate::container_util::{Node, eventually_with_logs, seed, wait_for_peers};
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

/// Time for a record B sent to reach A and be ignored there, before A writes
/// the key itself. Far above a container-network hop and an apply.
const DELIVERY_ROOM: Duration = Duration::from_secs(1);

/// One attempt on `key`: B puts `3`, A, which does not hold the key, puts
/// `5`; true when B then reads the sum `8` within a few seconds. False when
/// A's gossip view of B or B's view of A has not yet reached the protocol
/// that ships merged records, so the writes went out as plain invalidations
/// and B dropped its copy; the caller tries a fresh key.
async fn b_folds_a_write(a: &Node, b: &Node, key: &str) -> bool {
    b.put(key, "3").await.expect("b puts");
    tokio::time::sleep(DELIVERY_ROOM).await;
    a.put(key, "5").await.expect("a puts");
    for _ in 0..30 {
        if b.get(key).await == Ok(Some("8".to_string())) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Three current nodes, `"it"` in `Mode::Invalidation` with the interop-only
/// summing resolver. B holds a counter at `3`; A, which holds nothing for the
/// key, writes `5`. B folds A's merged record into its copy and reads the sum
/// `8` instead of dropping the key, A keeps its own `5`, and C, which never
/// held the key, ignores both records and reads a miss. The resolver is not
/// idempotent, so the test asserts a single fold only.
#[tokio::test]
async fn an_invalidation_write_folds_into_the_nodes_that_hold_the_key() {
    const CLUSTER: &str = "invalidation-fold";
    require_containers!();

    let net = Arc::new(Network::new_network());
    let env = [
        ("SUNDOG_TESTNODE_MODE", "invalidation"),
        ("SUNDOG_TESTNODE_RESOLVER", "sum_counter"),
    ];
    let a = Node::spawn_with_env(&net, CLUSTER, "n1", &[], &env).await;
    let b = Node::spawn_with_env(&net, CLUSTER, "n2", &[&seed("n1")], &env).await;
    let c = Node::spawn_with_env(&net, CLUSTER, "n3", &[&seed("n1")], &env).await;
    let nodes = [&a, &b, &c];
    wait_for_peers(&nodes, 2).await;

    let mut attempt = 0;
    let mut folded = String::new();
    eventually_with_logs(Duration::from_secs(120), &nodes, || {
        attempt += 1;
        let key = format!("counter{attempt}");
        folded.clone_from(&key);
        let (a, b) = (&a, &b);
        async move { b_folds_a_write(a, b, &key).await }
    })
    .await;

    assert_eq!(
        b.get(&folded).await,
        Ok(Some("8".to_string())),
        "B folded A's record into its copy"
    );
    assert_eq!(
        a.get(&folded).await,
        Ok(Some("5".to_string())),
        "A keeps its own write; B's record was ignored while A held nothing"
    );
    assert_eq!(
        c.get(&folded).await,
        Ok(None),
        "C never held the key, so neither record stored anything"
    );

    a.stop().await.expect("a stops");
    b.stop().await.expect("b stops");
    c.stop().await.expect("c stops");
    net.close().await.expect("network closes");
}
