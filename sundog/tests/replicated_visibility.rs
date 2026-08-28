//! Plan §11.3, layer 3, item 1: a 3-node `Mode::Replicated` cache. A put on
//! one node reaches the other two with a correctly attributed remote origin;
//! a remove tombstones the key everywhere.
//!
//! Real-transport-only: see `tests/anti_entropy_repair.rs`'s module doc for
//! why this excludes `sim`.

#![cfg(not(feature = "sim"))]

mod common;

use std::time::Duration;

use sundog::{Event, Mode, Origin};

#[tokio::test]
async fn replicated_put_and_remove_reach_every_node_with_correct_origin() {
    let nodes = common::spawn_cluster_group("it-replicated-3node", 3).await;
    let a = nodes[0].cluster.clone();
    let b = nodes[1].cluster.clone();
    let c = nodes[2].cluster.clone();
    let a_id = a.node_id();

    let cache_a = a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens");
    let cache_b = b
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("b opens");
    let cache_c = c
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("c opens");
    let mut events_c = cache_c.events();

    cache_a.insert(1, "hello".into()).await.expect("a inserts");

    let event = tokio::time::timeout(Duration::from_secs(10), events_c.recv())
        .await
        .expect("event arrives within the bound")
        .expect("event channel stays open");
    match event {
        Event::Created {
            key: 1,
            value,
            origin: Origin::Remote(node),
        } => {
            assert_eq!(value, "hello");
            assert_eq!(
                node, a_id,
                "event observed on C must attribute the write to A's node id"
            );
        }
        other => panic!("unexpected event: {other:?}"),
    }

    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.as_deref() == Some("hello")
    })
    .await;
    assert_eq!(cache_c.get(&1).await, Some("hello".to_string()));

    cache_b.remove(&1).await.expect("b removes");

    common::eventually(Duration::from_secs(10), || async {
        cache_a.get(&1).await.is_none()
    })
    .await;
    common::eventually(Duration::from_secs(10), || async {
        cache_c.get(&1).await.is_none()
    })
    .await;

    common::shutdown_all(nodes).await;
}
