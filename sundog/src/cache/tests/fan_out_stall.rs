//! A `Distributed` fan-out toward owners that stay live in gossip but take nothing ends at `fan_out_stall_timeout`.

use super::*;
use crate::net::MsgClass;
use crate::wire::{Msg, WireRecord};

/// `a`'s `fan_out_stall_timeout` in these tests.
const STALL_TIMEOUT: Duration = Duration::from_millis(300);

/// Three `Distributed` nodes where `b` and `c`, the owners of the key
/// returned, crashed: `a`'s failure detector never drops them, so they stay
/// live there while every redial of their data plane fails, and `a`'s
/// one-slot outboxes toward them are full and no longer drained.
async fn owners_that_take_nothing(
    name: &'static str,
) -> (Cluster, Cache<u32, String>, u32, [crate::node::NodeId; 2]) {
    let a_config = ClusterConfig {
        outbox_capacity: 1,
        fan_out_stall_timeout: STALL_TIMEOUT,
        phi_threshold: 1_000.0,
        ..loopback_config()
    };
    let ((a, cache_a), (b, _cache_b), (c, _cache_c), key) =
        three_node_distributed_on(name, "prices", a_config, steady_config()).await;
    let owners = [b.node_id(), c.node_id()];
    tokio::join!(b.crash(), c.crash());
    // An outbox that refuses a frame, and still refuses one after its
    // writer has had time to find the link closed and start redialing, is
    // one that writer no longer drains.
    for owner in owners {
        for _ in 0..2 {
            wait_until(
                Duration::from_secs(5),
                "a's outbox toward a crashed owner fills",
                async || {
                    !a.mesh().send_many_enqueued(
                        owner,
                        MsgClass::Replicate,
                        [Msg::Replicate {
                            cache: SmolStr::new("prices"),
                            rec: record_for(&a, key, "filler"),
                        }],
                    )
                },
            )
            .await;
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
    }
    (a, cache_a, key, owners)
}

/// A record of `key` at `value`, written by `cluster` now.
fn record_for(cluster: &Cluster, key: u32, value: &str) -> WireRecord {
    WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(
            postcard::to_stdvec(&value.to_string())
                .expect("encodes")
                .into(),
        ),
        ver: crate::hlc::Hlc {
            wall_ms: now_ms(),
            logical: 0,
            node: cluster.node_id(),
        },
        expires_at_ms: None,
    }
}

/// `a` forwards a write for a key it does not own, as a non-owner does,
/// to two owners that take nothing: the batch ends at `a`'s
/// `fan_out_stall_timeout`, with both owners still live, and counts the
/// write lost.
#[tokio::test]
async fn a_forward_to_owners_that_take_nothing_ends_at_the_stall_timeout() {
    use crate::cluster::fan_out::fan_out_batch;
    use crate::store::FanOutItem;

    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (a, cache_a, key, owners) = owners_that_take_nothing("cache-it-fan-out-stall").await;

    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(5),
        fan_out_batch(
            &cache_a.shard,
            &a,
            &SmolStr::new("prices"),
            Mode::distributed(),
            vec![FanOutItem::Forward(record_for(&a, key, "forwarded"))],
        ),
    )
    .await
    .expect("the batch ends at the stall timeout, not when the owners leave");
    let took = started.elapsed();
    assert!(
        took >= STALL_TIMEOUT && took < crate::net::FAN_OUT_SEND_DEADLINE,
        "the batch ends at a's fan_out_stall_timeout: {took:?}"
    );
    assert!(
        owners.iter().all(|&owner| a.mesh().peer_is_live(owner)),
        "both owners are still live in a's peer table"
    );
    let lost: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == "sundog_fan_out_lost_writes_total")
        .map(|(_, _, _, value)| match value {
            metrics_util::debugging::DebugValue::Counter(n) => n,
            _ => 0,
        })
        .sum();
    assert_eq!(
        lost, 1,
        "the forward reached neither owner, its only copy gone"
    );

    a.shutdown().await;
}

/// `Cluster::shutdown` waits for every cache's fan-out task to send what it
/// holds, and gossip stops updating the peer table once shutdown starts, so
/// a fan-out that waited for as long as an owner stayed in that table never
/// ended. Bounded by `fan_out_stall_timeout`, the shutdown completes while
/// both owners of the writes it holds take nothing.
#[tokio::test]
async fn shutdown_completes_while_the_owners_of_queued_writes_take_nothing() {
    let (a, cache_a, key, _owners) =
        owners_that_take_nothing("cache-it-fan-out-stall-shutdown").await;
    for value in ["first", "second", "third"] {
        cache_a
            .insert(key, value.to_string())
            .await
            .expect("a non-owner forwards the write");
    }

    tokio::time::timeout(Duration::from_secs(30), a.shutdown())
        .await
        .expect("shutdown completes with the owners still taking nothing");
}
