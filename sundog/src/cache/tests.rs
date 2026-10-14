//! Unit tests for the cache: pure rules, builder validation and loopback clusters.

mod builder;
mod clear_nonowner;
mod distributed_fetch;
mod expiry_tombstone;
mod explain_probes;
mod explain_reads;
mod fan_out_departed;
mod forwarding;
mod forwarding_skew;
mod lifecycle;
mod loading;
mod merge_expiry;
mod nonowner_fill;
mod observability;
mod rules;
#[cfg(feature = "spill")]
mod spill_gate;
mod stale_close;
mod startup;
mod tti_clock;
mod warm_reconcile;
mod writes;

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
use std::time::Instant;

use tokio::sync::watch;

use super::*;
use crate::cache::load::{LoadTarget, load_target};
use crate::cache::open::{DistributedContext, trust_sole_owner_at_open};
use crate::cache::reconcile::reconcile_warm_buckets;
use crate::cluster::test_support::wait_until;
use crate::cluster::{Cluster, anti_entropy};
use crate::config::ClusterConfig;
use crate::explain::{
    LocalRead, LocalRecord, OwnerProbe, ProbeAnswer, Reads, ServeVerdict, Unreached,
};
use crate::ownership::OwnershipView;
use crate::store::crdt::{PnCounter, PnCounterResolver};
use crate::store::{bucket_of, now_ms};
use crate::wire::WireRecord;

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

#[derive(Debug, thiserror::Error)]
#[error("source unavailable")]
struct SourceDown;

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
