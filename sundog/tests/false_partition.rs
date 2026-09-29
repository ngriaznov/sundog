//! `Mode::Distributed` behavior while nodes are wrongly detected as gone.
//!
//! Each node runs on its own runtime, as it would in its own process. A
//! node's runtime has one worker, and a task blocking that worker freezes
//! the node whole: its gossip, data plane and timers stop, and its copy of
//! every key stays in memory, as in a process stopped with `SIGSTOP`. The
//! other nodes' failure detectors drop it, while it still holds everything
//! it owned.

#![cfg(not(feature = "sim"))]

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
use std::sync::mpsc;
use std::time::Duration;

use sundog::{Cache, CacheError, Cluster, ClusterConfig, Mode, NodeId};

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

/// A node on `rt` that seeds from `seeds`.
fn build(rt: &tokio::runtime::Runtime, seeds: &[SocketAddr], config: ClusterConfig) -> Cluster {
    rt.block_on(async {
        Cluster::builder(CLUSTER_NAME)
            .seeds(seeds.to_vec())
            .config(config)
            .build()
            .await
            .expect("node builds")
    })
}

/// Freezes the node on `rt`, a one-worker runtime: its only worker blocks
/// until the returned sender sends or drops.
fn freeze(rt: &tokio::runtime::Runtime) -> mpsc::Sender<()> {
    let (thaw, frozen) = mpsc::channel::<()>();
    rt.spawn(async move {
        let _ = frozen.recv();
    });
    thaw
}

/// Up to [`KEYS`] keys whose owners are exactly `only`, as every cache in
/// `caches` agrees: a view not yet naming every node gives none.
fn keys_owned_only_by(caches: &[&Cache<String, String>], only: &[NodeId]) -> Vec<String> {
    (0..1_000)
        .map(key)
        .filter(|k| {
            let mut owners = caches[0].owners_of(k);
            owners.sort_unstable();
            let mut want = only.to_vec();
            want.sort_unstable();
            owners == want
                && caches.iter().all(|cache| {
                    let mut theirs = cache.owners_of(k);
                    theirs.sort_unstable();
                    theirs == owners
                })
        })
        .take(KEYS as usize)
        .collect()
}

/// Node a is frozen past `tombstone_ttl` while node b deletes every key.
/// b's view names b alone, so its deletes reach no co-owner; when a resumes
/// it still holds every key's old value, and only b's tombstones stand
/// between that copy and anti-entropy pushing it back to b.
#[test]
fn a_delete_during_a_false_partition_stays_deleted_after_the_heal() {
    let rt_a = node_runtime(1);
    let rt_b = node_runtime(2);

    let cluster_a = build(&rt_a, &[], config());
    let cluster_b = build(&rt_b, &[cluster_a.local_gossip_addr()], config());
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

    let thaw = freeze(&rt_a);

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

/// Nodes b and c, the only owners of every key below, are frozen. Node a's
/// view then names a alone, so a owns every part, holding none of these
/// keys. A fetch there answers `FetchUnavailable` while b and c are absent
/// for less than `state_transfer_budget`, never a miss for a key they hold,
/// and the value once they return.
#[test]
fn a_node_that_seems_alone_never_answers_a_miss_for_a_key_absent_owners_hold() {
    // Longer than the checks below, so a keeps waiting for b and c; its
    // share for one donor bounds how long a pull against a frozen node
    // holds a's rebalance loop before the view naming a alone is handled.
    const BUDGET: Duration = Duration::from_secs(15);
    let config = || config().with(|c| c.state_transfer_budget = BUDGET);
    let rt_a = node_runtime(2);
    let rt_b = node_runtime(1);
    let rt_c = node_runtime(1);

    let cluster_a = build(&rt_a, &[], config());
    let seed = [cluster_a.local_gossip_addr()];
    let cluster_b = build(&rt_b, &seed, config());
    let cluster_c = build(&rt_c, &seed, config());
    for (rt, cluster) in [
        (&rt_a, &cluster_a),
        (&rt_b, &cluster_b),
        (&rt_c, &cluster_c),
    ] {
        rt.block_on(common::wait_for_peer_count(
            cluster,
            2,
            Duration::from_secs(15),
        ));
    }
    let cache_a = rt_a.block_on(open(&cluster_a));
    let cache_b = rt_b.block_on(open(&cluster_b));
    let cache_c = rt_c.block_on(open(&cluster_c));

    let a = cluster_a.node_id();
    let only_b_and_c = || {
        keys_owned_only_by(
            &[&cache_a, &cache_b, &cache_c],
            &[cluster_b.node_id(), cluster_c.node_id()],
        )
    };
    rt_a.block_on(common::eventually(Duration::from_secs(15), || async {
        only_b_and_c().len() == KEYS as usize
    }));
    let keys = only_b_and_c();
    rt_b.block_on(async {
        for k in &keys {
            cache_b
                .insert(k.clone(), format!("v-{k}"))
                .await
                .expect("insert succeeds");
        }
        common::eventually(Duration::from_secs(15), || async {
            keys.iter()
                .all(|k| cache_b.get_sync(k).is_some() && cache_c.get_sync(k).is_some())
        })
        .await;
    });

    // Past the disown grace and hand-off of every part the joins moved, so
    // no rebalance loop is mid-round when b and c freeze.
    std::thread::sleep(Duration::from_secs(3));

    let thaw = [freeze(&rt_b), freeze(&rt_c)];

    rt_a.block_on(async {
        common::eventually(Duration::from_secs(20), || async {
            cluster_a.peers().is_empty() && cache_a.owners_of(&keys[0]) == vec![a]
        })
        .await;
        // Through a's rebalance of the view naming it alone, and short of
        // BUDGET since b and c dropped out.
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for k in &keys {
                match cache_a.fetch(k).await {
                    Err(CacheError::FetchUnavailable { .. }) => {}
                    other => panic!(
                        "a fetch of {k} on a node that only seems alone answers \
                         FetchUnavailable, got {other:?}"
                    ),
                }
            }
        }
    });

    drop(thaw);
    rt_a.block_on(async {
        common::wait_for_peer_count(&cluster_a, 2, Duration::from_secs(20)).await;
        common::eventually(Duration::from_secs(20), || async {
            for k in &keys {
                if cache_a.fetch(k).await.ok().flatten() != Some(format!("v-{k}")) {
                    return false;
                }
            }
            true
        })
        .await;
    });

    rt_c.block_on(cluster_c.shutdown());
    rt_b.block_on(cluster_b.shutdown());
    rt_a.block_on(cluster_a.shutdown());
}
