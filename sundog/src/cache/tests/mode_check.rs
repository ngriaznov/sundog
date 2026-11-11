//! The mode check of `open`: what it sees of a peer that gossips right
//! after the cluster builds.

use std::collections::HashMap;

use super::*;
use crate::cache::open::{conflicting_mode, first_gossip_wait};
use crate::error::CacheError;

#[test]
fn conflicting_mode_finds_a_peer_advertising_another_mode_for_the_name() {
    let (a, b) = (NodeId::from(1), NodeId::from(2));
    let advertised = HashMap::from([
        (a, HashMap::from([("users".into(), Mode::Replicated)])),
        (b, HashMap::from([("other".into(), Mode::Invalidation)])),
    ]);
    let users = "users".into();
    assert_eq!(
        conflicting_mode(&advertised, &users, Mode::Invalidation),
        Some(Mode::Replicated)
    );
    assert_eq!(
        conflicting_mode(&advertised, &users, Mode::Replicated),
        None
    );
    assert_eq!(
        conflicting_mode(&advertised, &"absent".into(), Mode::Replicated),
        None
    );
    assert_eq!(
        conflicting_mode(&HashMap::new(), &users, Mode::Replicated),
        None
    );
}

#[test]
fn first_gossip_wait_is_five_intervals_and_never_over_the_cap() {
    assert_eq!(
        first_gossip_wait(Duration::from_millis(200)),
        Duration::from_secs(1)
    );
    assert_eq!(
        first_gossip_wait(Duration::from_secs(10)),
        Duration::from_secs(2)
    );
    assert_eq!(first_gossip_wait(Duration::ZERO), Duration::ZERO);
}

/// `b` seeds off `a` and opens a conflicting mode with no wait in between:
/// the open waits for the first gossip round and fails.
async fn open_right_after_build_conflicts(mode: Mode) {
    let name = format!("cache-it-open-mode-check-{mode:?}").replace(['{', '}', ' ', ':'], "");
    let a = Cluster::builder(&name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let _cache_a = a
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("a opens as Replicated");
    let b = Cluster::builder(&name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");

    let opened = b.cache::<u32, String>("users").mode(mode).open().await;
    match opened {
        Err(CacheError::ModeMismatch { local, remote, .. }) => {
            assert_eq!(local, mode);
            assert_eq!(remote, Mode::Replicated);
        }
        other => panic!("expected ModeMismatch, got {:?}", other.map(|_| ())),
    }
    assert!(
        b.health().caches.is_empty(),
        "the refused cache is not left open"
    );

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn an_invalidation_open_right_after_build_sees_a_replicated_peer() {
    open_right_after_build_conflicts(Mode::Invalidation).await;
}

#[tokio::test]
async fn a_distributed_open_right_after_build_sees_a_replicated_peer() {
    open_right_after_build_conflicts(Mode::Distributed {
        owners: NonZeroU8::new(2).expect("two owners"),
    })
    .await;
}

#[tokio::test]
async fn a_lone_node_opens_without_waiting_for_a_peer() {
    let cluster = Cluster::builder("cache-it-open-mode-check-lone")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("solo cluster builds");
    let start = Instant::now();
    let _cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("a lone node opens");
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "a node with no seeds has no first gossip round to wait for: {:?}",
        start.elapsed()
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_seeded_node_whose_seeds_are_down_opens_within_the_gossip_wait() {
    let cluster = Cluster::builder("cache-it-open-mode-check-dead-seed")
        .seeds([startup::dead_gossip_addr().await])
        .config(loopback_config())
        .build()
        .await
        .expect("cluster builds");
    let start = Instant::now();
    let _cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Invalidation)
        .open()
        .await
        .expect("a node with no live seed opens");
    assert!(
        start.elapsed()
            < first_gossip_wait(loopback_config().gossip_interval) + Duration::from_secs(1),
        "the open waits one bounded gossip wait: {:?}",
        start.elapsed()
    );
    cluster.shutdown().await;
}
