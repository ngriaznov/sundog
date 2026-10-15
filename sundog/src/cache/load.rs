//! Load routing: picks the node a miss loads on, and the task that reloads entries under
//! refresh-ahead.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;

use super::Cache;
#[cfg(doc)]
use super::CacheBuilder;
use crate::cluster::Cluster;
use crate::error::{CodecError, RemoteLoaderError};
use crate::net::LoadOutcome;
use crate::node::NodeId;
use crate::store::{LoadError, Mode, PartId, RefreshRequest, Shard, ShardOps, Sourced, encode_key};

/// The live peers a key of cache `name`, open here under `mode`, may load
/// or refresh on: they speak [`crate::wire::PROTOCOL_LOAD`], advertise a
/// loader for `name`, and have it open under the same [`Mode`].
fn loader_peers(cluster: &Cluster, name: &str, mode: Mode) -> HashSet<NodeId> {
    let loaders = cluster.advertised_loaders();
    let modes = cluster.advertised_cache_modes();
    cluster
        .peers()
        .into_iter()
        .filter(|peer| {
            crate::wire::peer_supports(peer.protocol, crate::wire::PROTOCOL_LOAD)
                && loaders
                    .get(&peer.node)
                    .is_some_and(|caches| caches.contains(name))
                && modes.get(&peer.node).and_then(|caches| caches.get(name)) == Some(&mode)
        })
        .map(|peer| peer.node)
        .collect()
}

/// Whether [`Cache::load`] in `mode` gathers each key's loads on one node.
/// `Mode::Local` has no peers, and in `Mode::Replicated` a loaded value
/// reaches every node anyway, so each loads for itself. Pure; unit tested
/// directly.
pub(super) fn routes_loads(mode: Mode) -> bool {
    matches!(mode, Mode::Invalidation | Mode::Distributed { .. })
}

/// The node a key's loads gather on, from this node's side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LoadTarget {
    Here,
    Peer(NodeId),
}

/// The node `part`'s loads gather on, among this node `me` and the peers in
/// `capable` that can load for this cache, leaving out the peers in `tried`,
/// the ones that already declined or could not be reached for this key. In
/// `Mode::Distributed`, the first of the part's `owners`, in rank order, that
/// is `me` or a capable peer not in `tried`, and `me` when none is. In every
/// other mode, the rendezvous winner for `part` among `me` and `capable`,
/// ties to the lower [`NodeId`], so every node that agrees on who can load
/// picks the same one; once that winner has declined or failed, `me`, as
/// those modes keep what each node loads for itself. Pure; unit tested
/// directly.
pub(super) fn load_target(
    mode: Mode,
    part: PartId,
    me: NodeId,
    owners: &[NodeId],
    capable: &HashSet<NodeId>,
    tried: &HashSet<NodeId>,
) -> LoadTarget {
    let chosen = match mode {
        Mode::Distributed { .. } => owners
            .iter()
            .copied()
            .find(|&node| node == me || (capable.contains(&node) && !tried.contains(&node)))
            .unwrap_or(me),
        _ if !tried.is_empty() => me,
        _ => capable
            .iter()
            .copied()
            .chain(std::iter::once(me))
            .max_by_key(|&node| {
                (
                    crate::ownership::rendezvous_score(node, part.raw()),
                    std::cmp::Reverse(node),
                )
            })
            .unwrap_or(me),
    };
    if chosen == me {
        LoadTarget::Here
    } else {
        LoadTarget::Peer(chosen)
    }
}

/// A key a load routes: its encoding, and the peers that declined it or could
/// not be reached for it, which [`load_target`] skips.
pub(super) struct Routed<K> {
    key: K,
    key_bytes: bytes::Bytes,
    tried: HashSet<NodeId>,
}

impl<K> Routed<K> {
    /// A key no peer has been asked for yet.
    pub(super) fn new(key: K, key_bytes: bytes::Bytes) -> Self {
        Self {
            key,
            key_bytes,
            tried: HashSet::new(),
        }
    }
}

/// What asking a peer to load a batch of keys came to.
pub(super) enum Asked<K, V> {
    /// The peer loaded them, or answered that its loader failed.
    Answered(Sourced<K, V>),
    /// The peer declined or could not be reached; the keys, with the peer
    /// marked tried, go to the next node [`load_target`] picks.
    Passed(Vec<Routed<K>>),
}

/// How a load request reaches a peer: the keys' encodings in, the peer's
/// outcome out. [`Cache::route_load`] sends it over the mesh.
pub(super) type AskLoad<'a> = dyn Fn(NodeId, Vec<bytes::Bytes>) -> BoxFuture<'a, Result<LoadOutcome, CodecError>>
    + Send
    + Sync
    + 'a;

/// The most refresh requests [`refresh_task`] takes off its queue at once.
const REFRESH_BURST: usize = 1_024;

/// Drains the reloads reads ask for under [`CacheBuilder::refresh_ahead`].
/// A key this node refreshes, the node [`load_target`] picks for it, and a
/// key a peer's hint sent here reload in one [`Shard::refresh`] pass per
/// drained burst; a key another node refreshes goes to it as a
/// [`crate::wire::Msg::RefreshHint`], or reloads here when the hint cannot
/// be sent. Counts `sundog_refreshes_total{cache, outcome}` and
/// `sundog_refresh_hints_sent_total{cache}`.
pub(super) async fn refresh_task<K, V>(
    shard: Arc<Shard<K, V>>,
    cluster: Cluster,
    mut requests: tokio::sync::mpsc::Receiver<RefreshRequest<K>>,
    cancel: CancellationToken,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let name = SmolStr::new(shard.name());
    let mode = shard.mode();
    let outcome = |outcome: &'static str| metrics::counter!("sundog_refreshes_total", "cache" => name.to_string(), "outcome" => outcome);
    let (loaded, absent, failed) = (outcome("loaded"), outcome("absent"), outcome("failed"));
    let hints_sent =
        metrics::counter!("sundog_refresh_hints_sent_total", "cache" => name.to_string());
    let mut burst = Vec::new();
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            received = requests.recv_many(&mut burst, REFRESH_BURST) => {
                if received == 0 {
                    return;
                }
            }
        }
        let me = cluster.node_id();
        let capable = loader_peers(&cluster, &name, mode);
        let view = shard.ownership_view();
        let mut here = Vec::with_capacity(burst.len());
        for request in burst.drain(..) {
            let target = if request.hinted || matches!(mode, Mode::Local) {
                LoadTarget::Here
            } else {
                let part = PartId::of_key(&request.key_bytes);
                let owners = view.as_ref().map_or(&[][..], |view| view.owners_of(part));
                load_target(mode, part, me, owners, &capable, &HashSet::new())
            };
            match target {
                LoadTarget::Peer(peer)
                    if cluster.mesh().send_refresh_hint(
                        peer,
                        name.clone(),
                        request.key_bytes.clone(),
                        request.ver,
                    ) =>
                {
                    hints_sent.increment(1);
                }
                _ => here.push(request),
            }
        }
        if !here.is_empty() {
            let tally = shard.refresh(here).await;
            loaded.increment(tally.loaded);
            absent.increment(tally.absent);
            failed.increment(tally.failed);
        }
    }
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// The source [`Cache::load`] reads a miss from in `Mode::Invalidation`
    /// and `Mode::Distributed`: each key goes to the node its loads gather
    /// on, per [`load_target`], one request per node, and this node's own
    /// keys to its loader, all at once.
    ///
    /// In `Mode::Distributed`, a key whose owner declines or cannot be
    /// reached goes to the next owner of its part that is this node or has a
    /// loader, skipping every owner already tried, and so on down the
    /// owners; only a key no owner takes is loaded here. An owner that loads
    /// stores the value and fans it out to the other owners; a node that
    /// owns nothing of the key answers with the value and keeps no copy.
    /// In `Mode::Invalidation` a key whose peer declines or cannot be reached
    /// is loaded here. A peer that declines never ran its loader, so a
    /// declined key loads once. A request that fails after the peer received
    /// it can have run the peer's loader, which the requester cannot tell
    /// from one that never arrived, so such a key can load on that peer and
    /// again on the next node. A peer whose loader fails fails the batch with
    /// [`RemoteLoaderError`].
    pub(super) async fn route_load(&self, keys: Vec<K>) -> Result<Sourced<K, V>, LoadError> {
        let name = SmolStr::new(self.shard.name());
        self.route_load_via(keys, &|peer, request| {
            let name = name.clone();
            Box::pin(async move { self.cluster.mesh().load(peer, name, request).await })
        })
        .await
    }

    /// [`Cache::route_load`] with the requests to peers sent through `ask`.
    pub(super) async fn route_load_via(
        &self,
        keys: Vec<K>,
        ask: &AskLoad<'_>,
    ) -> Result<Sourced<K, V>, LoadError> {
        let mode = self.shard.mode();
        let me = self.cluster.node_id();
        let capable = self.loader_peers();
        let view = self.shard.ownership_view();
        let mut here = Vec::new();
        let mut pending = Vec::new();
        for key in keys {
            match encode_key(&key) {
                Ok(key_bytes) => pending.push(Routed::new(key, key_bytes)),
                Err(_) => here.push(key),
            }
        }
        let mut sourced = Sourced {
            store: HashMap::new(),
            answer: HashMap::new(),
        };
        loop {
            let mut remote: HashMap<NodeId, Vec<Routed<K>>> = HashMap::new();
            for routed in std::mem::take(&mut pending) {
                let part = PartId::of_key(&routed.key_bytes);
                let owners = view.as_ref().map_or(&[][..], |view| view.owners_of(part));
                match load_target(mode, part, me, owners, &capable, &routed.tried) {
                    LoadTarget::Here => here.push(routed.key),
                    LoadTarget::Peer(peer) => remote.entry(peer).or_default().push(routed),
                }
            }
            let local = async {
                let here = std::mem::take(&mut here);
                if here.is_empty() {
                    Ok(HashMap::new())
                } else {
                    self.shard.call_loader(here).await
                }
            };
            let remotes = futures::future::join_all(
                remote
                    .into_iter()
                    .map(|(peer, keys)| self.load_on(peer, keys, ask)),
            );
            let (local, remotes) = tokio::join!(local, remotes);
            sourced.store.extend(local?);
            for asked in remotes {
                match asked? {
                    Asked::Answered(answer) => {
                        sourced.store.extend(answer.store);
                        sourced.answer.extend(answer.answer);
                    }
                    Asked::Passed(passed) => pending.extend(passed),
                }
            }
            if pending.is_empty() {
                return Ok(sourced);
            }
        }
    }

    /// Asks `peer`, through `ask`, to load `keys`. A record it stores comes
    /// back to be stored here too in `Mode::Invalidation`, where each node
    /// caches its own working set, and only answered with in
    /// `Mode::Distributed`, where `peer` owns the key and this node may not.
    /// A peer that declines or cannot be reached is marked tried on each key,
    /// and the keys come back as [`Asked::Passed`] for the next node.
    pub(super) async fn load_on(
        &self,
        peer: NodeId,
        keys: Vec<Routed<K>>,
        ask: &AskLoad<'_>,
    ) -> Result<Asked<K, V>, LoadError> {
        let request = keys.iter().map(|routed| routed.key_bytes.clone()).collect();
        let (found, uncached) = match ask(peer, request).await {
            Ok(LoadOutcome::Loaded { found, uncached }) => (found, uncached),
            Ok(LoadOutcome::Failed(message)) => {
                return Err(Box::new(RemoteLoaderError {
                    node: peer,
                    message,
                }));
            }
            Ok(LoadOutcome::Declined) | Err(_) => {
                let passed = keys
                    .into_iter()
                    .map(|mut routed| {
                        routed.tried.insert(peer);
                        routed
                    })
                    .collect();
                return Ok(Asked::Passed(passed));
            }
        };
        let mut by_bytes: HashMap<bytes::Bytes, K> = keys
            .into_iter()
            .map(|routed| (routed.key_bytes, routed.key))
            .collect();
        let mut sourced = Sourced {
            store: HashMap::new(),
            answer: HashMap::new(),
        };
        let stores_here = matches!(self.shard.mode(), Mode::Invalidation);
        let records = found
            .into_iter()
            .filter_map(|rec| rec.value.map(|value| (rec.key, value, stores_here)));
        let values = uncached.into_iter().map(|(key, value)| (key, value, false));
        for (key_bytes, value_bytes, store) in records.chain(values) {
            let (Some(key), Ok(value)) = (
                by_bytes.remove(&key_bytes),
                postcard::from_bytes::<V>(&value_bytes),
            ) else {
                continue;
            };
            if store {
                sourced.store.insert(key, value);
            } else {
                sourced.answer.insert(key, value);
            }
        }
        Ok(Asked::Answered(sourced))
    }

    /// [`loader_peers`] for this cache.
    pub(super) fn loader_peers(&self) -> HashSet<NodeId> {
        loader_peers(&self.cluster, self.shard.name(), self.shard.mode())
    }
}
