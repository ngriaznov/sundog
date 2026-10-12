//! Unit tests for the cache: pure rules, builder validation and loopback clusters.

mod builder;
mod distributed_fetch;
mod explain_probes;
mod explain_reads;
mod observability;
mod rules;
#[cfg(feature = "spill")]
mod spill_gate;
mod startup;
mod warm_reconcile;

use std::net::{Ipv4Addr, SocketAddr};

use super::*;
use crate::cluster::Cluster;
use crate::cluster::test_support::wait_until;
use crate::explain::{
    LocalRead, LocalRecord, ProbeAnswer, ProbedRecord, Residency, ServeVerdict, Unreached,
};
use crate::store::bucket_of;
use crate::store::crdt::{PnCounter, PnCounterResolver};

fn loopback_config() -> ClusterConfig {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    ClusterConfig {
        gossip_bind_addr: loopback,
        data_bind_addr: loopback,
        ae_interval: Duration::from_millis(200),
        tombstone_ttl: Duration::from_secs(2),
        state_transfer_budget: Duration::from_secs(5),
        ..ClusterConfig::default()
    }
}

async fn wait_for_peer_count(cluster: &Cluster, expected: usize) {
    wait_until(
        Duration::from_secs(15),
        "peers converge within the bound",
        async || cluster.peers().len() >= expected,
    )
    .await;
}

/// A [`WireRecord`] for `key`/`value`, versioned at `node`, for seeding a shard directly.
fn test_record(key: u32, value: &str, node: NodeId) -> WireRecord {
    WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node,
        },
        expires_at_ms: None,
    }
}

/// Joins a fresh node onto `cluster_name`'s chitchat cluster (three
/// nodes total once this and the seed and a later joiner are all up, so
/// `owners = 2` leaves each bucket owned by exactly two of the three,
/// and every node fails to own a real share of buckets) and opens
/// `cache_name` as `Mode::distributed()`, two owners, on it.
async fn join_distributed(
    seed_addr: SocketAddr,
    cluster_name: &'static str,
    cache_name: &'static str,
    peer_count: usize,
) -> (Cluster, Cache<u32, String>) {
    let cluster = Cluster::builder(cluster_name)
        .seeds([seed_addr])
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    wait_for_peer_count(&cluster, peer_count).await;
    let cache = tokio::time::timeout(
        Duration::from_secs(20),
        cluster
            .cache::<u32, String>(cache_name)
            .mode(Mode::distributed())
            .open(),
    )
    .await
    .expect("open completes within the state-transfer budget")
    .expect("open succeeds");
    (cluster, cache)
}

/// A three-node `Mode::Distributed { owners: 2 }` cluster, all open on
/// `cache_name`, plus a key whose bucket the first node does not own
/// (its live owners are exactly the second and third nodes).
async fn three_node_distributed(
    cluster_name: &'static str,
    cache_name: &'static str,
) -> (
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    u32,
) {
    three_node_distributed_with_a(cluster_name, cache_name, loopback_config()).await
}

/// [`three_node_distributed`] with the first node built on `a_config`.
async fn three_node_distributed_with_a(
    cluster_name: &'static str,
    cache_name: &'static str,
    a_config: ClusterConfig,
) -> (
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    u32,
) {
    let a = Cluster::builder(cluster_name)
        .seeds(std::iter::empty())
        .config(a_config)
        .build()
        .await
        .expect("node a builds");
    let cache_a = a
        .cache::<u32, String>(cache_name)
        .mode(Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        })
        .open()
        .await
        .expect("a opens alone, owning everything");
    let seed = a.local_gossip_addr();

    let (b, cache_b) = join_distributed(seed, cluster_name, cache_name, 1).await;
    wait_for_peer_count(&a, 1).await;
    let (c, cache_c) = join_distributed(seed, cluster_name, cache_name, 2).await;
    wait_for_peer_count(&a, 2).await;
    wait_for_peer_count(&b, 2).await;

    // A key whose bucket `a` does not own: with three eligible nodes and
    // owners = 2, roughly a third of keys land here. `a`'s ownership
    // view only recomputes once its background refresh task has
    // observed both the membership change above and the gossiped
    // cache-mode advertisement `b`/`c` sent on their own `open()`,
    // strictly a separate, slightly later event than peer liveness,
    // so this polls rather than searching the instant peers converge.
    let unowned_key = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(key) =
                (0..10_000u32).find(|&k| !cache_a.owners_of(&k).contains(&a.node_id()))
            {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a's ownership view converges to include b and c within the bound");

    ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key)
}

/// `b` accepts any stamp; `a` bounds skew at one second. A record
/// stamped an hour ahead that `b` holds never reaches `a` through
/// anti-entropy, `a`'s clock never moves toward it, and `b`, whose
/// clock absorbed it, has its later writes refused by `a` too.
#[tokio::test]
async fn a_record_stamped_past_max_clock_skew_never_reaches_a_bounded_node() {
    let name = "cache-it-clock-skew";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config().with(|c| c.max_clock_skew = Some(Duration::from_secs(1))))
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config().with(|c| c.max_clock_skew = None))
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>("prices")
            .mode(Mode::Replicated)
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    cache_b.insert(2, "before".into()).await.expect("insert");
    wait_until(
        Duration::from_secs(10),
        "b's write stamped by its own clock reaches a",
        async || cache_a.get(&2).await.is_some(),
    )
    .await;

    let ahead = crate::hlc::Hlc {
        wall_ms: crate::store::now_ms() + 3_600_000,
        logical: 0,
        node: b.node_id(),
    };
    ShardOps::apply_remote(
        cache_b.shard.as_ref(),
        crate::wire::WireRecord {
            key: bytes::Bytes::from(postcard::to_stdvec(&1u32).expect("key encodes")),
            value: Some(bytes::Bytes::from(
                postcard::to_stdvec(&"ahead".to_string()).expect("value encodes"),
            )),
            ver: ahead,
            expires_at_ms: None,
        },
    )
    .await;
    assert_eq!(cache_b.get(&1).await.as_deref(), Some("ahead"));
    cache_b.insert(4, "after".into()).await.expect("insert");

    // Ten anti-entropy intervals: every round offers a both keys.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        cache_a.get(&1).await,
        None,
        "a refuses the record stamped an hour ahead"
    );
    assert_eq!(
        cache_a.get(&4).await,
        None,
        "b's clock absorbed the hour, so its next write is refused as well"
    );

    cache_a.insert(3, "local".into()).await.expect("insert");
    let key = bytes::Bytes::from(postcard::to_stdvec(&3u32).expect("key encodes"));
    let held = ShardOps::bucket_entries(cache_a.shard.as_ref(), crate::store::bucket_of(&key))
        .await
        .into_iter()
        .find(|entry| entry.key == key)
        .expect("a holds its own write");
    assert!(
        held.version.wall_ms < ahead.wall_ms - 3_000_000,
        "a's clock never took the refused stamp"
    );

    b.shutdown().await;
    a.shutdown().await;
}

/// The part `key`'s encoding falls in.
fn part_of(key: u32) -> PartId {
    PartId::of_key(&encode_key(&key).expect("u32 encodes"))
}

/// A `Mode::Distributed` cache of `cluster`'s whose shard decides under
/// `view`, with its residency set to mark parts in and the sender its
/// tracker reads, to publish a further view through.
fn cache_publishing_views(
    cluster: &Cluster,
    name: &str,
    view: &Arc<crate::ownership::OwnershipView>,
) -> (
    Cache<u32, String>,
    Arc<crate::ownership::ResidencySet>,
    watch::Sender<Arc<crate::ownership::OwnershipView>>,
) {
    let owners = std::num::NonZeroU8::MIN;
    let (tracker, tx) = crate::ownership::OwnershipTracker::seed(
        cluster.node_id(),
        &[],
        &std::collections::HashMap::new(),
        &SmolStr::new(name),
        owners,
    );
    tx.send(Arc::clone(view)).expect("receiver alive");
    let residency = Arc::new(crate::ownership::ResidencySet::new());
    let shard = Shard::<u32, String>::new(
        SmolStr::new(name),
        Mode::distributed(),
        cluster.node_id(),
        u64::MAX,
        None,
        None,
    )
    .with_ownership(tracker, Arc::clone(&residency));
    let cache = Cache {
        shard: Arc::new(shard),
        cluster: cluster.clone(),
        cancel: CancellationToken::new(),
        tasks: TaskTracker::new(),
    };
    (cache, residency, tx)
}

/// A `Mode::Distributed` cache of `cluster`'s whose shard decides under
/// `view`, with its residency set to mark parts in.
fn cache_under_view(
    cluster: &Cluster,
    name: &str,
    view: &Arc<crate::ownership::OwnershipView>,
) -> (Cache<u32, String>, Arc<crate::ownership::ResidencySet>) {
    let (cache, residency, _view_tx) = cache_publishing_views(cluster, name, view);
    (cache, residency)
}

#[tokio::test]
async fn expire_touch_persist_and_ttl_of_on_one_node() {
    let cluster = Cluster::builder("cache-it-ttl-surface")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("sessions")
        .mode(Mode::Local)
        .ttl(Duration::from_secs(600))
        .open()
        .await
        .expect("opens");
    cache.insert(1, "a".into()).await.expect("insert");
    let left = |cache: &Cache<u32, String>| match cache.ttl_of(&1) {
        Some(Ttl::Remaining(left)) => left,
        other => panic!("a remaining lifetime, got {other:?}"),
    };
    assert!(left(&cache) > Duration::from_secs(590));

    assert!(
        cache
            .expire(&1, Duration::from_secs(30))
            .await
            .expect("expire")
    );
    assert!(left(&cache) <= Duration::from_secs(30));
    assert!(cache.touch(&1).await.expect("touch"));
    assert!(
        left(&cache) > Duration::from_secs(590),
        "touch restarts the default"
    );
    assert!(cache.persist(&1).await.expect("persist"));
    assert_eq!(cache.ttl_of(&1), Some(Ttl::Never));
    assert_eq!(cache.get(&1).await, Some("a".to_string()));

    assert!(
        !cache
            .expire(&2, Duration::from_secs(1))
            .await
            .expect("expire")
    );
    assert_eq!(cache.ttl_of(&2), None);
    assert!(cache.expire(&1, Duration::ZERO).await.expect("expire"));
    assert_eq!(cache.get(&1).await, None);

    cluster.shutdown().await;
}

#[derive(Debug, thiserror::Error)]
#[error("source unavailable")]
struct SourceDown;

#[tokio::test]
async fn load_and_load_many_read_through_the_registered_loader() {
    let cluster = Cluster::builder("cache-it-load")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let calls = Arc::new(std::sync::Mutex::new(Vec::<Vec<u32>>::new()));
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Local)
        .ttl(Duration::from_secs(600))
        .batch_loader({
            let calls = Arc::clone(&calls);
            move |keys: Vec<u32>| {
                let calls = Arc::clone(&calls);
                async move {
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    calls.lock().expect("calls lock").push(sorted);
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .filter(|key| key.is_multiple_of(2))
                            .map(|key| (key, format!("v{key}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            }
        })
        .open()
        .await
        .expect("opens");

    assert_eq!(cache.load(&2).await.expect("load"), Some("v2".to_string()));
    assert_eq!(cache.get(&2).await, Some("v2".to_string()));
    assert!(
        matches!(cache.ttl_of(&2), Some(Ttl::Remaining(left)) if left > Duration::from_secs(590)),
        "a loaded value takes the cache's default TTL"
    );
    assert_eq!(cache.load(&3).await.expect("load"), None);
    assert_eq!(
        cache.load_many([2, 3, 4, 6]).await.expect("load_many"),
        HashMap::from([
            (2, "v2".to_string()),
            (4, "v4".to_string()),
            (6, "v6".to_string())
        ])
    );
    assert_eq!(
        calls.lock().expect("calls lock").clone(),
        vec![vec![2], vec![3], vec![3, 4, 6]],
        "a hit never reaches the loader; a key the source lacks is asked again"
    );

    let single = cluster
        .cache::<u32, String>("names")
        .mode(Mode::Local)
        .loader(|key: u32| async move {
            if key == 0 {
                Err(SourceDown)
            } else {
                Ok((key < 10).then(|| format!("n{key}")))
            }
        })
        .open()
        .await
        .expect("opens");
    assert_eq!(single.load(&1).await.expect("load"), Some("n1".to_string()));
    assert_eq!(single.load(&11).await.expect("load"), None);
    assert!(matches!(single.load(&0).await, Err(CacheError::Loader(_))));

    let plain = cluster
        .cache::<u32, String>("plain")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    assert!(matches!(
        plain.load(&1).await,
        Err(CacheError::NoLoader { cache }) if cache == "plain"
    ));
    assert!(matches!(
        cluster
            .cache::<u32, String>("windowed")
            .batch_window(Duration::from_millis(5), 10)
            .open()
            .await,
        Err(CacheError::InvalidLoadConfig { cache, .. }) if cache == "windowed"
    ));

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_batch_window_groups_misses_from_concurrent_loads() {
    let cluster = Cluster::builder("cache-it-load-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let calls = Arc::new(std::sync::Mutex::new(Vec::<Vec<u32>>::new()));
    let cache = cluster
        .cache::<u32, String>("prices")
        .mode(Mode::Local)
        .batch_loader({
            let calls = Arc::clone(&calls);
            move |keys: Vec<u32>| {
                let calls = Arc::clone(&calls);
                async move {
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    calls.lock().expect("calls lock").push(sorted);
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .map(|key| (key, format!("v{key}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            }
        })
        .batch_window(Duration::from_millis(200), 3)
        .open()
        .await
        .expect("opens");
    let (a, b, c) = tokio::join!(cache.load(&1), cache.load(&2), cache.load(&3));
    for (answer, key) in [(a, 1), (b, 2), (c, 3)] {
        assert_eq!(answer.expect("load"), Some(format!("v{key}")));
    }
    assert_eq!(
        calls.lock().expect("calls lock").clone(),
        vec![vec![1, 2, 3]],
        "three misses fill the batch and load in one call"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_loaded_value_replicates_to_every_replica() {
    let name = "cache-it-load-replicated";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>("prices")
            .mode(Mode::Replicated)
            .loader(|key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) })
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    assert_eq!(
        cache_a.load(&7).await.expect("load"),
        Some("v7".to_string())
    );
    wait_until(
        Duration::from_secs(10),
        "b holds the loaded value",
        async || cache_b.get(&7).await == Some("v7".to_string()),
    )
    .await;

    b.shutdown().await;
    a.shutdown().await;
}

/// Every loader call any node made: the node and the keys, sorted.
type LoadLog = Arc<std::sync::Mutex<Vec<(NodeId, Vec<u32>)>>>;

/// How long a test waits for a three-node `Mode::Distributed` cluster
/// to converge: views, loader adverts, and owners warming their parts.
/// Each node's ownership views cost seconds of CPU in a debug build, so
/// several such tests sharing a slow runner take well past ten seconds
/// to warm; a wait returns as soon as its condition holds.
const CONVERGE: Duration = Duration::from_secs(30);

/// Three nodes with `name` open under `mode`, each with a batch loader
/// that records its calls in `log`, takes 300ms, and holds `v{key}` for
/// every key; the loader on the node at index `fails` returns an error
/// instead. Returns once every node sees the other two advertise a
/// loader and, under `Mode::Distributed`, ranks all three in its
/// ownership view.
async fn three_loading_nodes(
    name: &'static str,
    mode: Mode,
    log: &LoadLog,
    fails: Option<usize>,
) -> Vec<(Cluster, Cache<u32, String>)> {
    let mut nodes: Vec<(Cluster, Cache<u32, String>)> = Vec::new();
    for index in 0..3 {
        // Every owner here stays up: a longer fetch timeout lets one
        // whose runtime is busy computing views still answer, and the
        // failure detector never marks down a node too busy to gossip,
        // which would move the view and cool the parts a test waits on.
        let builder = Cluster::builder(name).config(loopback_config().with(|c| {
            c.fetch_timeout = Duration::from_secs(5);
            c.phi_threshold = 1_000.0;
        }));
        let cluster = match nodes.first() {
            None => builder.seeds(std::iter::empty()),
            Some((seed, _)) => builder.seeds([seed.local_gossip_addr()]),
        }
        .build()
        .await
        .expect("node builds");
        wait_for_peer_count(&cluster, index).await;
        let node = cluster.node_id();
        let log = Arc::clone(log);
        let cache = cluster
            .cache::<u32, String>(name)
            .mode(mode)
            .batch_loader(move |keys: Vec<u32>| {
                let log = Arc::clone(&log);
                async move {
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    log.lock().expect("log lock").push((node, sorted));
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if fails == Some(index) {
                        return Err(SourceDown);
                    }
                    Ok(keys
                        .into_iter()
                        .map(|key| (key, format!("v{key}")))
                        .collect::<HashMap<_, _>>())
                }
            })
            .open()
            .await
            .expect("opens");
        nodes.push((cluster, cache));
    }
    wait_until(
        CONVERGE,
        "every node sees the other two advertise a loader",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.loader_peers().len() == 2)
        },
    )
    .await;
    wait_for_full_views(&nodes).await;
    nodes
}

/// Waits until every `Mode::Distributed` node's ownership view ranks
/// all of `nodes`. Loader adverts and the cache-mode adverts a view is
/// built from are separate gossip keys, and each node rebuilds its view
/// in its own task, so seeing every loader says nothing about the view.
async fn wait_for_full_views(nodes: &[(Cluster, Cache<u32, String>)]) {
    wait_until(
        CONVERGE,
        "every node's ownership view ranks every node",
        async || {
            nodes.iter().all(|(_, cache)| {
                cache
                    .shard
                    .ownership_view()
                    .is_none_or(|view| view.eligible().len() == nodes.len())
            })
        },
    )
    .await;
}

async fn shut_down_all(nodes: Vec<(Cluster, Cache<u32, String>)>) {
    for (cluster, _) in nodes.into_iter().rev() {
        cluster.shutdown().await;
    }
}

#[tokio::test]
async fn concurrent_invalidation_loads_of_a_key_gather_into_one_loader_call() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-load-gather", Mode::Invalidation, &log, None).await;
    let (answers, ()) = tokio::join!(
        futures::future::join_all(nodes.iter().map(|(_, cache)| cache.load(&7))),
        async {}
    );
    for answer in answers {
        assert_eq!(answer.expect("load"), Some("v7".to_string()));
    }
    assert_eq!(
        log.lock().expect("log lock").len(),
        1,
        "three nodes asking at once share one loader call: {:?}",
        log.lock().expect("log lock")
    );
    for (_, cache) in &nodes {
        assert_eq!(
            cache.get(&7).await,
            Some("v7".to_string()),
            "in Invalidation mode each node keeps what it loaded"
        );
    }
    let loads_before = log.lock().expect("log lock").len();
    let found = nodes[1].1.load_many([7, 8, 9]).await.expect("load_many");
    assert_eq!(found.len(), 3);
    assert_eq!(
        nodes[1].1.load(&7).await.expect("load"),
        Some("v7".to_string())
    );
    assert!(
        log.lock().expect("log lock").len() > loads_before,
        "the missing keys reach a loader"
    );
    shut_down_all(nodes).await;
}

/// Waits until every owner of `key` holds its part warm and verified, so
/// a fetch of it gets an answer rather than a decline.
async fn wait_for_warm_owners(nodes: &[(Cluster, Cache<u32, String>)], key: u32) {
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    let owners = nodes[0].1.owners_of(&key);
    wait_until(
        CONVERGE,
        "every owner holds the key's part warm",
        async || {
            nodes
                .iter()
                .filter(|(cluster, _)| owners.contains(&cluster.node_id()))
                .all(|(_, cache)| {
                    !cache.shard.is_cold_part(part) && !cache.shard.is_unverified_part(part)
                })
        },
    )
    .await;
}

/// A loopback address that accepts every connection and answers none: an
/// owner that is up and silent.
async fn silent_peer_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback tcp port for a silent peer");
    let addr = listener
        .local_addr()
        .expect("a freshly bound tcp listener reports a local address");
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    addr
}

/// The cache `node` runs in `nodes`.
fn cache_of(nodes: &[(Cluster, Cache<u32, String>)], node: NodeId) -> &Cache<u32, String> {
    &nodes
        .iter()
        .find(|(cluster, _)| cluster.node_id() == node)
        .expect("the node is one of the cluster's")
        .1
}

/// Waits until every cache of `caches` stores `key` as `want` says,
/// read from its engine: a tombstone is invisible to `Cache::get`.
async fn wait_for_local_record(
    caches: &[&Cache<u32, String>],
    key: u32,
    want: fn(&LocalRecord) -> bool,
) {
    let key_bytes = encode_key(&key).expect("u32 encodes");
    wait_until(
        Duration::from_secs(10),
        "every cache stores the key as wanted",
        async || {
            caches
                .iter()
                .all(|cache| want(&cache.shard.local_record(&key_bytes).1))
        },
    )
    .await;
}

#[tokio::test]
async fn a_distributed_load_runs_on_the_first_owner_and_stays_off_a_non_owner() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-load-owner", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    // A cold owner declines a load, which then runs on the next owner.
    wait_for_warm_owners(&nodes, key).await;

    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}"))
    );
    assert_eq!(
        log.lock().expect("log lock").clone(),
        vec![(owners[0], vec![key])],
        "the key's first owner runs the one load"
    );
    assert_eq!(cache_a.get(&key).await, None, "a non-owner keeps no copy");
    wait_until(CONVERGE, "both owners hold the loaded value", async || {
        let mut held = 0;
        for (cluster, cache) in &nodes {
            if owners.contains(&cluster.node_id()) && cache.get(&key).await.is_some() {
                held += 1;
            }
        }
        held == 2
    })
    .await;
    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}"))
    );
    assert_eq!(
        log.lock().expect("log lock").len(),
        1,
        "the owner answers the second read from its copy"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn a_failing_remote_loader_answers_with_a_remote_loader_error() {
    let log = LoadLog::default();
    let nodes = three_loading_nodes(
        "cache-it-load-remote-failure",
        Mode::Invalidation,
        &log,
        Some(1),
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let failing = nodes[1].0.node_id();
    let capable = cache_a.loader_peers();
    let key = (0..10_000u32)
        .find(|key| {
            let part = PartId::of_key(&encode_key(key).expect("u32 encodes"));
            load_target(Mode::Invalidation, part, a.node_id(), &[], &capable)
                == LoadTarget::Peer(failing)
        })
        .expect("a key whose loads gather on the failing node");
    let err = cache_a
        .load(&key)
        .await
        .expect_err("the remote loader fails");
    let CacheError::Loader(source) = &err else {
        panic!("a loader error, got {err:?}");
    };
    let remote = std::iter::successors(Some(source.as_ref() as &dyn std::error::Error), |err| {
        err.source()
    })
    .find_map(|err| err.downcast_ref::<RemoteLoaderError>())
    .expect("the error chain names the remote loader");
    assert_eq!(remote.node, failing);
    assert_eq!(remote.message, "source unavailable");
    assert_eq!(cache_a.get(&key).await, None);
    assert_eq!(
        log.lock().expect("log lock").clone(),
        vec![(failing, vec![key])],
        "a remote failure is not retried here"
    );
    shut_down_all(nodes).await;
}

/// A 10s TTL with refresh-ahead at 0.1: a key falls due 1s after its
/// write and expires 9s after that, so a stalled runner reads a due key
/// long before it expires.
const LONG_REFRESH: (Duration, f64) = (Duration::from_secs(10), 0.1);

/// `count` nodes with `name` open under `mode` with `lifetime`'s TTL and
/// refresh-ahead fraction, each with a batch loader that records its
/// calls in `log` and holds `v{key}.{n}`, `n` counting calls across all
/// nodes. Returns once every node sees the others advertise a loader
/// and, under `Mode::Distributed`, ranks them all in its ownership view.
async fn refreshing_nodes(
    name: &'static str,
    mode: Mode,
    count: usize,
    lifetime: (Duration, f64),
    log: &LoadLog,
) -> Vec<(Cluster, Cache<u32, String>)> {
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut nodes: Vec<(Cluster, Cache<u32, String>)> = Vec::new();
    for index in 0..count {
        let builder = Cluster::builder(name).config(loopback_config());
        let cluster = match nodes.first() {
            None => builder.seeds(std::iter::empty()),
            Some((seed, _)) => builder.seeds([seed.local_gossip_addr()]),
        }
        .build()
        .await
        .expect("node builds");
        wait_for_peer_count(&cluster, index).await;
        let node = cluster.node_id();
        let (log, calls) = (Arc::clone(log), Arc::clone(&calls));
        let cache = cluster
            .cache::<u32, String>(name)
            .mode(mode)
            .ttl(lifetime.0)
            .refresh_ahead(lifetime.1)
            .batch_loader(move |keys: Vec<u32>| {
                let (log, calls) = (Arc::clone(&log), Arc::clone(&calls));
                async move {
                    let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    let mut sorted = keys.clone();
                    sorted.sort_unstable();
                    log.lock().expect("log lock").push((node, sorted));
                    Ok::<_, SourceDown>(
                        keys.into_iter()
                            .map(|key| (key, format!("v{key}.{call}")))
                            .collect::<HashMap<_, _>>(),
                    )
                }
            })
            .open()
            .await
            .expect("opens");
        nodes.push((cluster, cache));
    }
    wait_until(
        Duration::from_secs(10),
        "every node sees the others advertise a loader",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.loader_peers().len() == count - 1)
        },
    )
    .await;
    wait_for_full_views(&nodes).await;
    nodes
}

#[tokio::test]
async fn refresh_ahead_keeps_a_read_key_alive_past_its_expiry_on_one_node() {
    let log = LoadLog::default();
    // A 5s TTL refreshed a fifth of the way in: a key falls due 1s
    // after its write and expires 4s after that, so a runner stalled
    // for up to 4s still reads it in time, and the 7s of reads below
    // outlive the lifetime.
    let lifetime = (Duration::from_secs(5), 0.2);
    let nodes = refreshing_nodes("cache-it-refresh-local", Mode::Local, 1, lifetime, &log).await;
    let cache = &nodes[0].1;
    assert_eq!(
        cache.load(&1).await.expect("load"),
        Some("v1.1".to_string())
    );
    let loaded_at = Instant::now();
    while loaded_at.elapsed() < Duration::from_secs(7) {
        assert!(
            cache.get(&1).await.is_some(),
            "a key read through its refresh window never expires"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A reload restarts the 5s lifetime, so the next refresh point is
    // 1s later: over 7s of reads, the load and about six reloads, fewer
    // when the runner stalls between reads.
    let loads = log.lock().expect("log lock").len();
    assert!(
        (2..=8).contains(&loads),
        "about one reload a refresh interval, not one per read: {loads} loads"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_reloads_a_replicated_key_once_on_its_refresher() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-refresh-replicated",
        Mode::Replicated,
        2,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let (b, cache_b) = &nodes[1];
    let key = 7u32;
    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some("v7.1".to_string())
    );
    wait_until(
        Duration::from_secs(5),
        "b holds the loaded value",
        async || cache_b.get(&key).await.is_some(),
    )
    .await;
    let part = PartId::of_key(&encode_key(&key).expect("u32 encodes"));
    let refresher = match load_target(
        Mode::Replicated,
        part,
        a.node_id(),
        &[],
        &[b.node_id()].into_iter().collect(),
    ) {
        LoadTarget::Here => a.node_id(),
        LoadTarget::Peer(peer) => peer,
    };

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for _ in 0..20 {
        assert!(cache_a.get(&key).await.is_some());
        assert!(cache_b.get(&key).await.is_some());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    wait_until(
        Duration::from_secs(5),
        "both nodes hold the reloaded value",
        async || {
            cache_a.get(&key).await == Some("v7.2".to_string())
                && cache_b.get(&key).await == Some("v7.2".to_string())
        },
    )
    .await;
    let log = log.lock().expect("log lock").clone();
    assert_eq!(
        log.len(),
        2,
        "one load and one reload for both nodes' reads: {log:?}"
    );
    assert_eq!(log[1], (refresher, vec![key]), "the refresher reloads");
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_reloads_a_distributed_key_a_non_owner_fetches_on_its_owner() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-refresh-distributed",
        Mode::distributed(),
        3,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    assert!(cache_a.load(&key).await.expect("load").is_some());
    let first = log.lock().expect("log lock").len();

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(cache_a.fetch(&key).await.expect("fetch").is_some());
    wait_until(
        Duration::from_secs(5),
        "the first owner reloads the key a non-owner read",
        async || log.lock().expect("log lock").len() > first,
    )
    .await;
    assert_eq!(
        log.lock().expect("log lock")[first],
        (owners[0], vec![key]),
        "the first owner reloads"
    );
    shut_down_all(nodes).await;
}

/// `explain` inside a key's refresh window is not a read: asked of every
/// node, the owners that hold the entry and the non-owner whose `fetch`
/// hints the owner included, it queues no reload and no loader runs,
/// where the `fetch` of the same key right after asks for one.
#[tokio::test]
async fn explain_does_not_start_a_refresh_inside_the_refresh_window() {
    let log = LoadLog::default();
    let nodes = refreshing_nodes(
        "cache-it-explain-refresh",
        Mode::distributed(),
        3,
        LONG_REFRESH,
        &log,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    assert!(cache_a.load(&key).await.expect("load").is_some());
    let loaded = log.lock().expect("log lock").len();
    wait_for_local_record(
        &[cache_of(&nodes, owners[0]), cache_of(&nodes, owners[1])],
        key,
        LocalRecord::is_live,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for (_, cache) in &nodes {
        let explanation = cache.explain(&key).await.expect("explains");
        assert!(
            explanation.source.returns_value(),
            "the entry is live: {explanation}"
        );
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        log.lock().expect("log lock").len(),
        loaded,
        "explain asks for no reload"
    );

    assert!(cache_a.fetch(&key).await.expect("fetch").is_some());
    wait_until(
        Duration::from_secs(5),
        "the first owner reloads the key a non-owner read",
        async || log.lock().expect("log lock").len() > loaded,
    )
    .await;
    assert_eq!(
        log.lock().expect("log lock")[loaded],
        (owners[0], vec![key]),
        "the first owner reloads"
    );
    shut_down_all(nodes).await;
}

#[tokio::test]
async fn refresh_ahead_needs_a_loader_a_ttl_and_a_mode_that_carries_values() {
    let cluster = Cluster::builder("cache-it-refresh-config")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let loader = |key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) };
    let reason = |result: Result<Cache<u32, String>, CacheError>| match result {
        Err(CacheError::InvalidLoadConfig { reason, .. }) => reason,
        other => panic!("an invalid load config, got {other:?}"),
    };
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("no-ttl")
                .loader(loader)
                .refresh_ahead(0.8)
                .open()
                .await
        ),
        "refresh_ahead needs a ttl"
    );
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("invalidation")
                .mode(Mode::Invalidation)
                .ttl(Duration::from_secs(1))
                .loader(loader)
                .refresh_ahead(0.8)
                .open()
                .await
        ),
        "refresh_ahead does not apply to Mode::Invalidation"
    );
    assert_eq!(
        reason(
            cluster
                .cache::<u32, String>("whole")
                .mode(Mode::Local)
                .ttl(Duration::from_secs(1))
                .loader(loader)
                .refresh_ahead(1.0)
                .open()
                .await
        ),
        "refresh_ahead takes a fraction strictly between 0 and 1"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_new_lifetime_replicates_to_every_replica() {
    let name = "cache-it-ttl-replicated";
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("a builds");
    let b = Cluster::builder(name)
        .seeds([a.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    wait_for_peer_count(&a, 1).await;
    wait_for_peer_count(&b, 1).await;
    let open = |cluster: &Cluster| {
        cluster
            .cache::<u32, String>("sessions")
            .mode(Mode::Replicated)
            .open()
    };
    let cache_a = open(&a).await.expect("a opens");
    let cache_b = open(&b).await.expect("b opens");

    cache_a.insert(1, "a".into()).await.expect("insert");
    wait_until(Duration::from_secs(10), "b holds the entry", async || {
        cache_b.ttl_of(&1) == Some(Ttl::Never)
    })
    .await;

    assert!(
        cache_a
            .expire(&1, Duration::from_secs(60))
            .await
            .expect("expire")
    );
    wait_until(
        Duration::from_secs(10),
        "b takes the new lifetime",
        async || {
            matches!(cache_b.ttl_of(&1), Some(Ttl::Remaining(left))
                if left <= Duration::from_secs(60) && left > Duration::from_secs(50))
        },
    )
    .await;
    assert!(cache_b.persist(&1).await.expect("persist"));
    wait_until(
        Duration::from_secs(10),
        "a takes b's persist back",
        async || cache_a.ttl_of(&1) == Some(Ttl::Never),
    )
    .await;
    assert_eq!(cache_a.get(&1).await, Some("a".to_string()));

    b.shutdown().await;
    a.shutdown().await;
}

/// `a` owns none of the key's part, so it asks an owner for the record
/// and forwards the re-arm: both owners take the lifetime and `a` still
/// holds nothing. A key no owner holds answers `false`.
#[tokio::test]
async fn expire_from_a_node_that_does_not_own_the_key_reaches_its_owners() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key) =
        three_node_distributed("cache-it-ttl-forward", "prices").await;
    cache_b
        .insert(unowned_key, "value".to_string())
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "both owners hold the value",
        async || cache_b.ttl_of(&unowned_key).is_some() && cache_c.ttl_of(&unowned_key).is_some(),
    )
    .await;

    assert!(
        cache_a
            .expire(&unowned_key, Duration::from_secs(60))
            .await
            .expect("expire")
    );
    let rearmed = |cache: &Cache<u32, String>| {
        matches!(cache.ttl_of(&unowned_key), Some(Ttl::Remaining(left))
            if left <= Duration::from_secs(60) && left > Duration::from_secs(50))
    };
    wait_until(
        Duration::from_secs(10),
        "both owners take the new lifetime",
        async || rearmed(&cache_b) && rearmed(&cache_c),
    )
    .await;
    assert_eq!(cache_a.ttl_of(&unowned_key), None, "a still holds no copy");
    assert_eq!(
        cache_a.fetch(&unowned_key).await.expect("fetch"),
        Some("value".to_string())
    );

    let absent = (unowned_key + 1..u32::MAX)
        .find(|k| !cache_a.owners_of(k).contains(&a.node_id()))
        .expect("another key a does not own");
    assert!(
        !cache_a.persist(&absent).await.expect("the owners answer"),
        "no owner holds the key"
    );

    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// Unlike a crash, a graceful leave hands a key on: when both of its
/// owners shut down one after the other, the remaining node inherits
/// the key and still reads it.
#[tokio::test]
async fn both_owners_leaving_gracefully_hand_the_key_to_the_remaining_node() {
    let ((a, cache_a), (b, cache_b), (c, _cache_c), unowned_key) =
        three_node_distributed("cache-it-graceful-hand-off", "prices").await;
    cache_b
        .insert(unowned_key, "value".to_string())
        .await
        .expect("insert");
    wait_until(
        Duration::from_secs(10),
        "the value reaches its owners",
        async || matches!(cache_a.fetch(&unowned_key).await, Ok(Some(value)) if value == "value"),
    )
    .await;

    b.shutdown().await;
    c.shutdown().await;

    wait_until(
        Duration::from_secs(10),
        "a owns the key alone and holds it",
        async || cache_a.owners_of(&unowned_key) == vec![a.node_id()],
    )
    .await;
    assert_eq!(
        cache_a.get(&unowned_key).await,
        Some("value".to_string()),
        "the hand-off put the value on a before its last owner left"
    );

    a.shutdown().await;
}

#[tokio::test]
async fn owners_of_matches_the_ownership_view_for_the_keys_bucket() {
    let ((a, cache_a), (b, _cache_b), (c, _cache_c), unowned_key) =
        three_node_distributed("cache-it-owners-of", "prices").await;

    let owners = cache_a.owners_of(&unowned_key);
    assert_eq!(owners.len(), 2, "owners = 2 for this cache");
    assert!(
        !owners.contains(&a.node_id()),
        "this key was chosen specifically because a does not own it"
    );
    // Every owner reported is a real, live node in this cluster.
    for owner in &owners {
        assert!(*owner == a.node_id() || *owner == b.node_id() || *owner == c.node_id());
    }

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

/// The end-to-end proof that the write forward, the fan-out group-by-
/// owner-set, and the fetch read path all compose: a write through a
/// node that does not own the key's bucket never becomes locally
/// visible on the writer, forwards to the real owners, and both the
/// writer and another non-owner read the value back correctly through
/// [`Cache::fetch`].
#[tokio::test]
async fn write_through_a_non_owner_composes_with_fetch_on_every_node() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), unowned_key) =
        three_node_distributed("cache-it-write-through-non-owner", "prices").await;

    // `a` was chosen specifically because it does not own this key's
    // bucket: this insert never touches a's engine, only forwards.
    cache_a
        .insert(unowned_key, "through-a".to_string())
        .await
        .expect("insert forwards");
    assert_eq!(
        cache_a.get(&unowned_key).await,
        None,
        "a forwarded write never becomes locally visible on the writer"
    );

    // Every node, owner or not, reads the same value back through
    // fetch: the writer itself, and whichever of b/c is not a real
    // owner either.
    let owners = cache_a.owners_of(&unowned_key);
    for cache in [&cache_a, &cache_b, &cache_c] {
        wait_until(
            Duration::from_secs(10),
            "fetch converges to the forwarded value on every node",
            async || matches!(cache.fetch(&unowned_key).await, Ok(Some(value)) if value == "through-a"),
        )
        .await;
    }
    assert_eq!(owners.len(), 2);

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn close_then_reopen_the_same_name_succeeds() {
    let cluster = Cluster::builder("cache-it-close-reopen")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, String>("scratch")
        .open()
        .await
        .expect("first open succeeds");
    cache.insert(1, "a".into()).await.expect("insert");
    cache.close().await;

    let reopened = cluster
        .cache::<u32, String>("scratch")
        .open()
        .await
        .expect("closing frees the name for a fresh open");
    assert_eq!(
        reopened.get(&1).await,
        None,
        "the reopened cache starts empty, not resuming the closed shard's state"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn close_returns_only_once_every_background_task_has_stopped() {
    let cluster = Cluster::builder("cache-it-close-waits")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, String>("orders")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("open succeeds");
    let tasks = cache.tasks.clone();
    assert!(
        !tasks.is_empty(),
        "a Replicated cache runs background tasks"
    );

    cache.close().await;

    assert!(tasks.is_closed() && tasks.is_empty());
    let caches = cluster.health().caches;
    assert!(caches.is_empty(), "{caches:?}");
    cluster.shutdown().await;
}

#[tokio::test]
async fn closing_one_clone_leaves_another_clone_usable() {
    let cluster = Cluster::builder("cache-it-close-clone")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let cache = cluster
        .cache::<u32, String>("shared")
        .open()
        .await
        .expect("open succeeds");
    let surviving = cache.clone();

    cache.close().await;

    surviving
        .insert(2, "still here".into())
        .await
        .expect("a surviving clone keeps writing locally after another clone closes");
    assert_eq!(
        surviving.get(&2).await,
        Some("still here".to_string()),
        "a surviving clone keeps reading locally after another clone closes"
    );
    assert!(
        surviving.shard.fan_out_queue().drain().is_empty(),
        "a detached clone queues nothing for a fan-out task that no longer runs"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn writer_id_pairs_the_node_with_the_cluster_incarnation() {
    let cluster = Cluster::builder("cache-it-writer-id")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");

    let writer = cache.writer_id();
    assert_eq!(
        writer,
        WriterId::new(cluster.node_id(), cluster.local_incarnation()),
        "Cache::writer_id pairs this node's id with the cluster's own \
         membership incarnation, the same pairing WriterId::new takes"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn writer_id_is_stable_across_clones_and_repeated_calls() {
    let cluster = Cluster::builder("cache-it-writer-id-stable")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");
    let clone = cache.clone();

    assert_eq!(
        cache.writer_id(),
        cache.writer_id(),
        "two calls on the same handle agree"
    );
    assert_eq!(
        cache.writer_id(),
        clone.writer_id(),
        "a clone shares the same underlying cluster membership, so it \
         reports the identical writer identity, never a fresh one"
    );

    cache.close().await;
    cluster.shutdown().await;
}

/// The end-to-end reason `Cache::writer_id` exists: a value merged under
/// it round-trips through the exact merging path CRDT compaction relies
/// on, exercised at the `Cache` layer rather than only unit-tested on
/// `WriterId` itself.
#[tokio::test]
async fn a_value_merged_under_writer_id_reads_back_correctly() {
    let cluster = Cluster::builder("cache-it-writer-id-merge")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");

    cache
        .merge(1, PnCounter::local_delta(cache.writer_id(), 5))
        .await
        .expect("merge");
    assert_eq!(cache.get(&1).await.map(|c| c.value()), Some(5));

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn merge_with_a_zero_window_applies_immediately() {
    let cluster = Cluster::builder("cache-it-merge-zero-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds");
    let writer = cache.writer_id();

    cache
        .merge(1, PnCounter::local_delta(writer, 1))
        .await
        .expect("merge");
    assert_eq!(
        cache.get(&1).await.map(|c| c.value()),
        Some(1),
        "a zero coalesce window (the default) applies every merge call at once, \
         like insert"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn merge_within_a_window_coalesces_into_one_apply_and_one_event() {
    let cluster = Cluster::builder("cache-it-merge-coalesce-window")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let window = Duration::from_millis(150);
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .merge_coalesce_window(window)
        .open()
        .await
        .expect("open succeeds");
    let writer = cache.writer_id();
    let mut events = cache.events();

    cache
        .merge(1, PnCounter::local_delta(writer, 1))
        .await
        .expect("merge 1");
    cache
        .merge(1, PnCounter::local_delta(writer, 2))
        .await
        .expect("merge 2");
    cache
        .merge(1, PnCounter::local_delta(writer, 3))
        .await
        .expect("merge 3");

    assert_eq!(
        cache.get(&1).await,
        None,
        "a pending coalesced fold is invisible to get until its window flushes"
    );
    assert!(
        events.try_recv().is_err(),
        "nothing applies, so nothing publishes, before the window elapses"
    );

    tokio::time::sleep(window * 3).await;

    assert_eq!(
        cache.get(&1).await.map(|c| c.value()),
        Some(3),
        "the coalesced fold of all three calls lands as one write once the window \
         elapses"
    );
    match events
        .recv()
        .await
        .expect("exactly one event for the whole window")
    {
        Event::Created { key, .. } => assert_eq!(key, 1),
        other => panic!("expected Event::Created for the window's one apply, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "three merge calls in one window publish exactly one event, not three"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn close_flushes_a_pending_coalesced_merge() {
    let cluster = Cluster::builder("cache-it-merge-close-flush")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");
    let cache = cluster
        .cache::<u32, PnCounter>("counters")
        .resolver(Arc::new(PnCounterResolver))
        .merge_coalesce_window(Duration::from_secs(60))
        .open()
        .await
        .expect("open succeeds");
    let survivor = cache.clone();
    let writer = cache.writer_id();

    cache
        .merge(1, PnCounter::local_delta(writer, 7))
        .await
        .expect("merge");
    assert_eq!(
        survivor.get(&1).await,
        None,
        "still pending: the 60s window has not elapsed"
    );

    cache.close().await;

    assert_eq!(
        survivor.get(&1).await.map(|c| c.value()),
        Some(7),
        "close flushes whatever was still pending, regardless of the window"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn forwarded_writes_reach_their_owners_when_the_writer_shuts_down_at_once() {
    // `a` forwards thousands of writes for buckets it does not own and
    // shuts down in the same breath: the fan-out task finishes the
    // batch, the mesh flushes it, and every key lands on the survivors,
    // which own everything between them once `a` is gone.
    let ((a, cache_a), (b, cache_b), (c, cache_c), _) =
        three_node_distributed("distributed-forward-flush", "forward-flush").await;
    let keys: Vec<u32> = (0..1_000_000u32)
        .filter(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .take(3_000)
        .collect();
    cache_a
        .insert_many(keys.iter().map(|&key| (key, format!("v{key}"))))
        .await
        .expect("insert_many forwards");
    cache_a.close().await;
    a.shutdown().await;

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut landed = 0usize;
            for key in &keys {
                let expected = format!("v{key}");
                if cache_b.get(key).await.as_deref() == Some(expected.as_str())
                    && cache_c.get(key).await.as_deref() == Some(expected.as_str())
                {
                    landed += 1;
                }
            }
            if landed == keys.len() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("every forwarded write lands on both survivors");

    cache_c.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn a_forwarded_write_after_close_fails_with_closed_instead_of_vanishing() {
    let ((a, cache_a), (b, cache_b), (c, cache_c), key_a_does_not_own) =
        three_node_distributed("distributed-write-after-close", "write-after-close").await;
    let owned_by_a = (0..1_000_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a owns some bucket");
    let late_handle = cache_a.clone();
    cache_a.close().await;
    assert!(
        matches!(
            late_handle
                .insert(key_a_does_not_own, "late".to_string())
                .await,
            Err(CacheError::Closed { .. })
        ),
        "a forwarded write after close is refused"
    );
    assert!(matches!(
        late_handle.remove(&key_a_does_not_own).await,
        Err(CacheError::Closed { .. })
    ));
    late_handle
        .insert(owned_by_a, "detached".to_string())
        .await
        .expect("a write this node applies itself still lands, detached");
    assert_eq!(
        late_handle.get(&owned_by_a).await.as_deref(),
        Some("detached")
    );

    cache_c.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

/// Three nodes under `config`, each with `name` open as a
/// `Mode::distributed()` cache and every peer count settled.
async fn three_node_distributed_with(
    name: &'static str,
    config: ClusterConfig,
) -> (
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
    (Cluster, Cache<u32, String>),
) {
    let a = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("node a builds");
    let cache_a = a
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("a opens alone");
    let seed = a.local_gossip_addr();
    let join = |peer_count: usize| {
        let config = config.clone();
        async move {
            let cluster = Cluster::builder(name)
                .seeds([seed])
                .config(config)
                .build()
                .await
                .expect("node builds");
            wait_for_peer_count(&cluster, peer_count).await;
            let cache = cluster
                .cache::<u32, String>(name)
                .mode(Mode::distributed())
                .open()
                .await
                .expect("node opens");
            (cluster, cache)
        }
    };
    let (b, cache_b) = join(1).await;
    wait_for_peer_count(&a, 1).await;
    let (c, cache_c) = join(2).await;
    wait_for_peer_count(&a, 2).await;
    wait_for_peer_count(&b, 2).await;
    ((a, cache_a), (b, cache_b), (c, cache_c))
}

/// A `WireRecord` for `key`/`value` as `node` would write it now.
fn wire_record_from(key: u32, value: &str, node: NodeId) -> crate::wire::WireRecord {
    crate::wire::WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock past the epoch")
                    .as_millis(),
            )
            .expect("fits"),
            logical: 0,
            node,
        },
        expires_at_ms: None,
    }
}

/// A write fanned out under a stale ownership view reaches only the
/// owners that view names. The receiver's inbound loop notices the
/// batch's view hash is not its own and re-forwards the records to
/// their owners under its view, so the owner the writer missed gets
/// the record without waiting for an anti-entropy round to pair the
/// two; a batch already re-forwarded once travels no further.
#[tokio::test]
async fn a_forward_batch_under_a_stale_view_reaches_the_owner_the_writer_missed() {
    use crate::net::OutFrame;
    use crate::wire::{Msg, WireRecord};

    // Anti-entropy is effectively off: it would repair the missing
    // copy and hide whether the re-forward did.
    let name = "distributed-stale-forward";
    let mut config = loopback_config();
    config.ae_interval = Duration::from_secs(3600);
    config.tombstone_ttl = config.bucket_release_window();
    let ((a, cache_a), (b, cache_b), (c, cache_c)) =
        three_node_distributed_with(name, config).await;

    // Two keys whose owners are exactly `b` and `c`, under every node's
    // settled view.
    let mut keys = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let found: Vec<u32> = (0..100_000u32)
                .filter(|key| {
                    let owners = cache_a.owners_of(key);
                    owners.len() == 2
                        && !owners.contains(&a.node_id())
                        && cache_b.owners_of(key) == owners
                        && cache_c.owners_of(key) == owners
                })
                .take(2)
                .collect();
            if found.len() == 2 {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every view settles on b and c owning some bucket");
    let hop_one_key = keys.pop().expect("two keys");
    let key = keys.pop().expect("two keys");
    let cache_name = SmolStr::new(name);
    let stale_hash = cache_b
        .shard
        .ownership_view_hash()
        .expect("b is distributed")
        ^ 1;
    let record = |key: u32, value: &str| wire_record_from(key, value, a.node_id());
    let forward = |hops: u8, rec: WireRecord| {
        let frame = OutFrame::new(Msg::ForwardBatch {
            cache: cache_name.clone(),
            view_hash: stale_hash,
            hops,
            recs: vec![rec],
        })
        .expect("encodes");
        let mesh = a.mesh().clone();
        let to = b.node_id();
        async move {
            mesh.send_frames_awaiting(to, vec![frame]).await;
        }
    };

    // As if `a` still believed `b` and a departed node owned the
    // bucket: the batch reaches `b` alone, under a hash `b` does not
    // recognise, and `b` passes it on to `c`.
    forward(0, record(key, "stale-routed")).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_c.get(&key).await.as_deref() != Some("stale-routed") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b re-forwards the batch to c, the owner a's stale view missed");
    assert_eq!(cache_b.get(&key).await.as_deref(), Some("stale-routed"));

    // The same batch already one hop past its writer stops at `b`.
    forward(1, record(hop_one_key, "hop-one")).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while cache_b.get(&hop_one_key).await.as_deref() != Some("hop-one") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("b applies the hop-one batch");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        cache_c.get(&hop_one_key).await,
        None,
        "a batch at the hop cap is applied where it lands and re-forwarded no further"
    );

    cache_c.close().await;
    cache_b.close().await;
    cache_a.close().await;
    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}
