//! A `Mode::Distributed` delete made while a co-owner is wrongly detected as
//! gone stays deleted once that co-owner comes back.
//!
//! Each node runs on its own runtime, as it would in its own process. Node
//! a's runtime has one worker, and a task blocking that worker freezes node
//! a whole: its gossip, data plane and timers stop, and its copy of every
//! key stays in memory, as in a process stopped with `SIGSTOP`. Node b's
//! failure detector drops it, so b's ownership view names b alone and b's
//! deletes reach no co-owner. The freeze outlasts `tombstone_ttl`. When a
//! resumes, it still holds every key's old value, and only b's tombstones
//! stand between that copy and anti-entropy pushing it back to b.

#![cfg(not(feature = "sim"))]

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
use std::sync::mpsc;
use std::time::Duration;

use sundog::{Cache, Cluster, ClusterConfig, Mode};

const CLUSTER_NAME: &str = "it-distributed-false-partition";
const CACHE_NAME: &str = "false-partition";
const KEYS: u32 = 50;
const TOMBSTONE_TTL: Duration = Duration::from_secs(3);

fn config() -> ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        c.tombstone_ttl = TOMBSTONE_TTL;
        c.tombstone_max_ttl = Duration::from_secs(120);
        c.distributed_disown_grace_rounds = 2;
    })
}

/// A runtime of its own for one node, as a separate process would have.
fn node_runtime(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("a node runtime builds")
}

async fn open(cluster: &Cluster) -> Cache<String, String> {
    cluster
        .cache::<String, String>(CACHE_NAME)
        .mode(Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("cache opens")
}

fn key(i: u32) -> String {
    format!("key-{i:03}")
}

#[test]
fn a_delete_during_a_false_partition_stays_deleted_after_the_heal() {
    let rt_a = node_runtime(1);
    let rt_b = node_runtime(2);

    let cluster_a = rt_a.block_on(async {
        Cluster::builder(CLUSTER_NAME)
            .seeds(std::iter::empty())
            .config(config())
            .build()
            .await
            .expect("a builds")
    });
    let cluster_b = rt_b.block_on(async {
        Cluster::builder(CLUSTER_NAME)
            .seeds([cluster_a.local_gossip_addr()])
            .config(config())
            .build()
            .await
            .expect("b builds")
    });
    rt_b.block_on(common::wait_for_peer_count(
        &cluster_b,
        1,
        Duration::from_secs(15),
    ));
    rt_a.block_on(common::wait_for_peer_count(
        &cluster_a,
        1,
        Duration::from_secs(15),
    ));
    let cache_a = rt_a.block_on(open(&cluster_a));
    let cache_b = rt_b.block_on(open(&cluster_b));

    // Two nodes, two owners: every key lives on both.
    rt_b.block_on(async {
        for i in 0..KEYS {
            cache_b
                .insert(key(i), format!("v1-{i}"))
                .await
                .expect("insert succeeds");
        }
        common::eventually(Duration::from_secs(15), || async {
            (0..KEYS).all(|i| cache_a.get_sync(&key(i)).is_some())
        })
        .await;
    });

    // Freezes node a: its one worker blocks until `thaw` sends.
    let (thaw, frozen) = mpsc::channel::<()>();
    rt_a.spawn(async move {
        frozen.recv().expect("the test thaws node a");
    });

    rt_b.block_on(async {
        // b's failure detector drops a, so b's view names b alone.
        common::eventually(Duration::from_secs(20), || async {
            cluster_b.peers().is_empty()
        })
        .await;
        for i in 0..KEYS {
            cache_b.remove(&key(i)).await.expect("remove succeeds");
        }
        // Past tombstone_ttl and at least one tombstone GC tick after it.
        tokio::time::sleep(TOMBSTONE_TTL + Duration::from_secs(3)).await;
    });
    assert!(
        (0..KEYS).all(|i| cache_a.get_sync(&key(i)).is_some()),
        "node a is frozen with its old copy of every key"
    );

    thaw.send(()).expect("node a's frozen task is waiting");
    rt_b.block_on(async {
        common::wait_for_peer_count(&cluster_b, 1, Duration::from_secs(20)).await;
        let held = || {
            let on = |cache: &Cache<String, String>| {
                (0..KEYS)
                    .filter(|&i| cache.get_sync(&key(i)).is_some())
                    .count()
            };
            (on(&cache_a), on(&cache_b))
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while held() != (0, 0) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "deleted keys still held after the heal (on a, on b): {:?}",
                held()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Past another tombstone_ttl, with every tombstone collectable: no
        // stale copy is left anywhere to bring a key back.
        tokio::time::sleep(TOMBSTONE_TTL * 2).await;
    });
    for i in 0..KEYS {
        assert_eq!(
            cache_a.get_sync(&key(i)),
            None,
            "{} stays deleted on a",
            key(i)
        );
        assert_eq!(
            cache_b.get_sync(&key(i)),
            None,
            "{} stays deleted on b",
            key(i)
        );
    }

    rt_b.block_on(cluster_b.shutdown());
    rt_a.block_on(cluster_a.shutdown());
}
