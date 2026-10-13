//! The `Mode::Invalidation` scenario for a merging resolver: a write ships
//! the writer's merged record, a node holding the key folds it in, and a node
//! that does not hold the key ignores it.

use crate::container_util::{Node, eventually_with_logs, seed, wait_for_peers};
use rightsize::Network;
use std::sync::Arc;
use std::time::Duration;

/// The counter the scenario writes.
const KEY: &str = "counter";

/// How long a fold is given to show once a write that should fold is sent.
const FOLD_WAIT: Duration = Duration::from_secs(10);

/// Whether a write from `sender` folds into a copy `receiver` holds: the
/// receiver puts `1` under `key`, the sender puts `2`, and the receiver
/// reads the sum `3` within a few seconds. False when the sender's gossip
/// view of the receiver has not yet reached the protocol that ships merged
/// records, so the write went out as a plain invalidation and the receiver
/// dropped its copy.
async fn folds(sender: &Node, receiver: &Node, key: &str) -> bool {
    receiver.put(key, "1").await.expect("receiver puts");
    sender.put(key, "2").await.expect("sender puts");
    for _ in 0..30 {
        if receiver.get(key).await == Ok(Some("3".to_string())) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Waits until a write from `sender` folds into a copy `receiver` holds,
/// trying a fresh probe key each round. This is the readiness check for the
/// scenario below: once it returns, the sender ships merged records to the
/// receiver, and the scenario's own writes fold on their first attempt.
async fn wait_until_folds(nodes: &[&Node], sender: &Node, receiver: &Node, label: &str) {
    let mut attempt = 0;
    eventually_with_logs(Duration::from_secs(120), nodes, || {
        attempt += 1;
        let key = format!("probe-{label}-{attempt}");
        async move { folds(sender, receiver, &key).await }
    })
    .await;
}

/// Writes `key` from `sender` after a write the receiver should have
/// applied, and waits until `receiver`, which holds `key` at `1`, shows the
/// sum `3`. Frames from one node arrive in the order they were sent, so the
/// receiver has applied, or ignored, everything `sender` wrote before this
/// call by the time it shows the sum.
async fn barrier(sender: &Node, receiver: &Node, key: &str) {
    receiver
        .put(key, "1")
        .await
        .expect("receiver holds the barrier key");
    sender
        .put(key, "2")
        .await
        .expect("sender writes the barrier key");
    eventually_with_logs(FOLD_WAIT, &[sender, receiver], || async {
        receiver.get(key).await == Ok(Some("3".to_string()))
    })
    .await;
}

/// Three current nodes, `"it"` in `Mode::Invalidation` with the interop-only
/// summing resolver. B holds a counter at `3`; A, which holds nothing for the
/// key, writes `5`. B folds A's merged record into its copy and reads the sum
/// `8` instead of dropping the key, A keeps its own `5`, and C, which never
/// held the key, ignores both records and reads a miss. The resolver is not
/// idempotent, so the test asserts a single fold only.
///
/// Gossip readiness is checked first, for each direction a record travels,
/// so the scenario runs once. Each negative claim waits on a barrier key
/// written after the record it covers, rather than on elapsed time.
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

    wait_until_folds(&nodes, &a, &b, "a-to-b").await;
    wait_until_folds(&nodes, &a, &c, "a-to-c").await;
    wait_until_folds(&nodes, &b, &a, "b-to-a").await;

    // B holds the counter. A holds nothing, so B's record is ignored there;
    // the barrier shows A has seen it before A writes.
    b.put(KEY, "3").await.expect("b puts");
    barrier(&b, &a, "barrier-b-to-a").await;
    assert_eq!(
        a.get(KEY).await,
        Ok(None),
        "A ignored B's record while it held nothing"
    );

    a.put(KEY, "5").await.expect("a puts");
    barrier(&a, &c, "barrier-a-to-c").await;
    eventually_with_logs(FOLD_WAIT, &nodes, || async {
        b.get(KEY).await == Ok(Some("8".to_string()))
    })
    .await;

    assert_eq!(
        a.get(KEY).await,
        Ok(Some("5".to_string())),
        "A keeps its own write; B's record was ignored while A held nothing"
    );
    assert_eq!(
        c.get(KEY).await,
        Ok(None),
        "C never held the key, so neither record stored anything"
    );

    a.stop().await.expect("a stops");
    b.stop().await.expect("b stops");
    c.stop().await.expect("c stops");
    net.close().await.expect("network closes");
}
