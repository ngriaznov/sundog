//! A `Mode::Distributed` load an owner declines or cannot take goes to the key's next owner with a loader, and only a key no owner takes loads on the requesting node.
//!
//! A real cluster cannot be made to decline on demand: an owner declines
//! while its part is cold, a window a test cannot hold open. These tests wire
//! three loading nodes and send the requesting node's load requests through
//! [`Cache::route_load_via`], the seam `route_load` sends over the mesh
//! through. It answers for the peers a test names with a decline or a
//! failure, and forwards every other request to the real peer, whose
//! `serve_load_encoded` loads, stores and fans out as in production.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::future::BoxFuture;

use super::*;
use crate::error::CodecError;
use crate::net::LoadOutcome;

/// How a test makes a peer refuse a load request.
#[derive(Clone, Copy)]
enum Refusal {
    /// The peer answers that it declines, without running its loader.
    Declines,
    /// The request fails before the peer answers.
    Unreachable,
}

/// A node of a three-node cluster, open on one `Mode::distributed()` cache
/// whose loader holds `v{key}` for every key and counts its calls.
struct LoadingNode {
    cluster: Cluster,
    cache: Cache<u32, String>,
    calls: Arc<AtomicUsize>,
}

impl LoadingNode {
    async fn open(cluster: Cluster, name: &'static str) -> Self {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let cache = tokio::time::timeout(
            Duration::from_secs(20),
            cluster
                .cache::<u32, String>(name)
                .mode(Mode::distributed())
                .loader(move |key: u32| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) }
                })
                .open(),
        )
        .await
        .expect("open completes within the state-transfer budget")
        .expect("open succeeds");
        Self {
            cluster,
            cache,
            calls,
        }
    }

    fn id(&self) -> NodeId {
        self.cluster.node_id()
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

/// Three loading nodes, the first the requester. Returns once the first
/// advertises a loader on both others.
async fn three_loading_nodes(name: &'static str) -> [LoadingNode; 3] {
    // Every node stays up: a failure detector that drops a node too busy to
    // gossip moves the views and cools the parts the tests wait on, and a
    // part a node loses keeps its cold mark.
    let build = |seed: Option<SocketAddr>| async move {
        Cluster::builder(name)
            .seeds(seed)
            .config(loopback_config().with(|c| c.phi_threshold = 1_000.0))
            .build()
            .await
            .expect("node builds")
    };
    let a = LoadingNode::open(build(None).await, name).await;
    let seed = a.cluster.local_gossip_addr();
    let b_cluster = build(Some(seed)).await;
    wait_for_peer_count(&b_cluster, 1).await;
    let b = LoadingNode::open(b_cluster, name).await;
    wait_for_peer_count(&a.cluster, 1).await;
    let c_cluster = build(Some(seed)).await;
    wait_for_peer_count(&c_cluster, 2).await;
    let c = LoadingNode::open(c_cluster, name).await;
    wait_for_peer_count(&a.cluster, 2).await;
    wait_for_peer_count(&b.cluster, 2).await;
    wait_until(
        CONVERGE,
        "the requester sees a loader on both peers",
        async || a.cache.loader_peers().len() == 2,
    )
    .await;
    // The loader adverts and the cache-mode adverts a view is built from are
    // separate gossip keys, so seeing both loaders says nothing about the
    // view.
    let nodes = [a, b, c];
    wait_until(
        CONVERGE,
        "every node's ownership view ranks every node",
        async || {
            nodes.iter().all(|node| {
                node.cache
                    .shard
                    .ownership_view()
                    .is_some_and(|view| view.eligible().len() == nodes.len())
            })
        },
    )
    .await;
    nodes
}

/// Waits until every node ranks each of `keys`' owners as the first does and
/// each owner has the key's part warm and verified, so no owner declines of
/// its own accord. A node that does not own the part is not waited on: it
/// keeps the cold mark an earlier view left on a part it has since lost, until
/// a view owns the part again (see `ResidencySet::mark_serving`), and it is
/// never asked to serve the key.
async fn wait_until_settled(nodes: &[LoadingNode; 3], keys: &[u32]) {
    wait_until(
        CONVERGE,
        "owners agree and hold their parts warm",
        async || {
            keys.iter().all(|key| {
                let part = part_of(*key);
                let owners = nodes[0].cache.owners_of(key);
                nodes.iter().all(|node| {
                    node.cache.owners_of(key) == owners
                        && (!owners.contains(&node.id())
                            || (!node.cache.shard.is_cold_part(part)
                                && !node.cache.shard.is_unverified_part(part)))
                })
            })
        },
    )
    .await;
}

/// Polls the first node's view until `pick` finds keys in it.
async fn wait_for_keys<T>(a: &LoadingNode, pick: impl Fn(&Cache<u32, String>) -> Option<T>) -> T {
    tokio::time::timeout(CONVERGE, async {
        loop {
            if let Some(found) = pick(&a.cache) {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the requester's view converges")
}

/// `keys` loaded through the requester's whole read path, with the load
/// requests to the peers in `refusals` refused as named and the others sent
/// to the real peer. Returns what the load answered and the peers asked, in
/// the order the requests were made.
async fn load_with_refusals(
    requester: &LoadingNode,
    keys: Vec<u32>,
    refusals: &[(NodeId, Refusal)],
) -> (HashMap<u32, String>, Vec<NodeId>) {
    let asked = Mutex::new(Vec::new());
    let name = SmolStr::new(requester.cache.shard.name());
    let ask = |peer: NodeId, request: Vec<Bytes>| -> BoxFuture<'_, _> {
        asked.lock().expect("lock").push(peer);
        let refusal = refusals
            .iter()
            .find(|(node, _)| *node == peer)
            .map(|(_, refusal)| *refusal);
        let name = name.clone();
        Box::pin(async move {
            match refusal {
                Some(Refusal::Declines) => Ok(LoadOutcome::Declined),
                Some(Refusal::Unreachable) => Err(CodecError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "refused by the test",
                ))),
                None => requester.cluster.mesh().load(peer, name, request).await,
            }
        })
    };
    let found = requester
        .cache
        .shard
        .load_many_from(keys, &|keys| {
            Box::pin(requester.cache.route_load_via(keys, &ask))
        })
        .await
        .expect("the load completes");
    let asked = asked.into_inner().expect("lock");
    (found, asked)
}

fn node_of(nodes: &[LoadingNode; 3], id: NodeId) -> &LoadingNode {
    nodes
        .iter()
        .find(|node| node.id() == id)
        .expect("a node of the cluster")
}

async fn shut_down(nodes: [LoadingNode; 3]) {
    for node in nodes {
        node.cluster.shutdown().await;
    }
}

/// The first owner refuses; the second, a real peer, loads the key once and
/// stores it; the requester, which owns nothing of the key, keeps no copy;
/// the refusing owner receives the value by fan-out from the loader.
async fn the_next_owner_loads_what_the_first_refused(name: &'static str, refusal: Refusal) {
    let nodes = three_loading_nodes(name).await;
    let key = wait_for_keys(&nodes[0], |cache| {
        (0..10_000u32).find(|k| !cache.owners_of(k).contains(&nodes[0].id()))
    })
    .await;
    wait_until_settled(&nodes, &[key]).await;
    let owners = nodes[0].cache.owners_of(&key);
    let (first, second) = (node_of(&nodes, owners[0]), node_of(&nodes, owners[1]));

    let (found, asked) = load_with_refusals(&nodes[0], vec![key], &[(first.id(), refusal)]).await;

    assert_eq!(found, HashMap::from([(key, format!("v{key}"))]));
    assert_eq!(
        asked,
        vec![first.id(), second.id()],
        "the owners are asked in rank order, each once"
    );
    assert_eq!(first.calls(), 0, "the refusing owner ran no loader");
    assert_eq!(second.calls(), 1, "the next owner ran the loader once");
    assert_eq!(nodes[0].calls(), 0, "the requester ran no loader");
    assert_eq!(
        second.cache.get(&key).await,
        Some(format!("v{key}")),
        "the owner that loaded stores the value"
    );
    wait_until(
        Duration::from_secs(10),
        "the loader's fan-out reaches the other owner",
        async || first.cache.get(&key).await.is_some(),
    )
    .await;
    assert_eq!(
        nodes[0].cache.get(&key).await,
        None,
        "a non-owner keeps no copy"
    );
    assert!(nodes[0].cache.keys().is_empty(), "no key is held");
    shut_down(nodes).await;
}

#[tokio::test]
async fn a_load_the_first_owner_declines_goes_to_the_next_owner() {
    the_next_owner_loads_what_the_first_refused("next-owner-declined", Refusal::Declines).await;
}

#[tokio::test]
async fn a_load_the_first_owner_cannot_take_goes_to_the_next_owner() {
    the_next_owner_loads_what_the_first_refused("next-owner-unreachable", Refusal::Unreachable)
        .await;
}

/// The requester owns the key and ranks after the refusing owner: it is the
/// next candidate, loads the key itself, stores it and fans it out.
#[tokio::test]
async fn a_load_the_first_owner_declines_goes_to_this_node_when_it_is_the_next_owner() {
    let nodes = three_loading_nodes("next-owner-here").await;
    let a = nodes[0].id();
    let key = wait_for_keys(&nodes[0], |cache| {
        (0..10_000u32).find(|k| cache.owners_of(k).get(1) == Some(&a))
    })
    .await;
    wait_until_settled(&nodes, &[key]).await;
    let first = node_of(&nodes, nodes[0].cache.owners_of(&key)[0]);

    let (found, asked) =
        load_with_refusals(&nodes[0], vec![key], &[(first.id(), Refusal::Declines)]).await;

    assert_eq!(found, HashMap::from([(key, format!("v{key}"))]));
    assert_eq!(asked, vec![first.id()], "only the first owner is asked");
    assert_eq!(nodes[0].calls(), 1, "the requester ran the loader once");
    assert_eq!(first.calls(), 0);
    assert_eq!(
        nodes[0].cache.get(&key).await,
        Some(format!("v{key}")),
        "an owner that loads stores the value"
    );
    wait_until(
        Duration::from_secs(10),
        "the requester's fan-out reaches the refusing owner",
        async || first.cache.get(&key).await.is_some(),
    )
    .await;
    shut_down(nodes).await;
}

/// Every owner refuses, one declining and one failing: the requester, which
/// owns nothing of the key, loads it once, answers and keeps no copy.
#[tokio::test]
async fn a_load_no_owner_takes_runs_once_on_the_non_owner_and_keeps_no_copy() {
    let nodes = three_loading_nodes("next-owner-none").await;
    let key = wait_for_keys(&nodes[0], |cache| {
        (0..10_000u32).find(|k| !cache.owners_of(k).contains(&nodes[0].id()))
    })
    .await;
    wait_until_settled(&nodes, &[key]).await;
    let owners = nodes[0].cache.owners_of(&key);

    let (found, asked) = load_with_refusals(
        &nodes[0],
        vec![key],
        &[
            (owners[0], Refusal::Declines),
            (owners[1], Refusal::Unreachable),
        ],
    )
    .await;

    assert_eq!(found, HashMap::from([(key, format!("v{key}"))]));
    assert_eq!(asked, owners, "each owner is asked once, in rank order");
    assert_eq!(nodes[0].calls(), 1, "the requester ran the loader once");
    assert_eq!(nodes[1].calls() + nodes[2].calls(), 0);
    assert_eq!(nodes[0].cache.get(&key).await, None, "no copy is kept");
    assert!(nodes[0].cache.keys().is_empty(), "no key is held");
    assert_eq!(nodes[1].cache.get(&key).await, None);
    assert_eq!(nodes[2].cache.get(&key).await, None);
    shut_down(nodes).await;
}

/// Two keys, one per ranking of the same two owners, in one load: the
/// declining first owner of one key hands it to the other, which also loads
/// the second key directly. Each key loads once.
#[tokio::test]
async fn a_batch_regroups_the_keys_a_declining_owner_hands_back() {
    let nodes = three_loading_nodes("next-owner-batch").await;
    let (b, c) = (nodes[1].id(), nodes[2].id());
    let (declined, direct) = wait_for_keys(&nodes[0], |cache| {
        let ranked = |first: NodeId| {
            (0..10_000u32).find(|k| {
                let owners = cache.owners_of(k);
                owners.len() == 2 && !owners.contains(&nodes[0].id()) && owners[0] == first
            })
        };
        Some((ranked(b)?, ranked(c)?))
    })
    .await;
    wait_until_settled(&nodes, &[declined, direct]).await;

    let (found, mut asked) =
        load_with_refusals(&nodes[0], vec![declined, direct], &[(b, Refusal::Declines)]).await;

    assert_eq!(
        found,
        HashMap::from([
            (declined, format!("v{declined}")),
            (direct, format!("v{direct}")),
        ])
    );
    asked.sort();
    let mut expected = vec![b, c, c];
    expected.sort();
    assert_eq!(
        asked, expected,
        "b is asked for the first key, c for the second, then c for the first"
    );
    assert_eq!(nodes[1].calls(), 0, "the declining owner ran no loader");
    assert_eq!(nodes[2].calls(), 2, "each key loaded once, on c");
    assert_eq!(nodes[0].calls(), 0);
    assert_eq!(
        nodes[2].cache.get(&declined).await,
        Some(format!("v{declined}"))
    );
    assert_eq!(
        nodes[2].cache.get(&direct).await,
        Some(format!("v{direct}"))
    );
    assert!(
        nodes[0].cache.keys().is_empty(),
        "the requester keeps nothing"
    );
    shut_down(nodes).await;
}
