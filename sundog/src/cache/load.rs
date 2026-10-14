//! Load routing: picks the node a miss loads on, and the task that reloads entries under
//! refresh-ahead.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;

use super::Cache;
#[cfg(doc)]
use super::CacheBuilder;
use crate::cluster::Cluster;
use crate::error::RemoteLoaderError;
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
/// `capable` that can load for this cache. In `Mode::Distributed`, the
/// first of the part's `owners`, in rank order, that is `me` or capable,
/// and `me` when none is. In every other mode, the rendezvous winner for
/// `part` among `me` and `capable`, ties to the lower [`NodeId`], so every
/// node that agrees on who can load picks the same one. Pure; unit tested
/// directly.
pub(super) fn load_target(
    mode: Mode,
    part: PartId,
    me: NodeId,
    owners: &[NodeId],
    capable: &HashSet<NodeId>,
) -> LoadTarget {
    let chosen = match mode {
        Mode::Distributed { .. } => owners
            .iter()
            .copied()
            .find(|&node| node == me || capable.contains(&node))
            .unwrap_or(me),
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
                load_target(mode, part, me, owners, &capable)
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
    /// keys to its loader, all at once. A peer that declines or cannot be
    /// reached has its keys loaded here instead, and so does a key no owner
    /// can load; a `Mode::Distributed` node that does not own such a key
    /// answers with the value and keeps no copy. A peer whose loader fails
    /// fails the batch with [`RemoteLoaderError`].
    pub(super) async fn route_load(&self, keys: Vec<K>) -> Result<Sourced<K, V>, LoadError> {
        let mode = self.shard.mode();
        let me = self.cluster.node_id();
        let capable = self.loader_peers();
        let view = self.shard.ownership_view();
        let mut here = Vec::new();
        let mut remote: HashMap<NodeId, Vec<(K, bytes::Bytes)>> = HashMap::new();
        for key in keys {
            let Ok(key_bytes) = encode_key(&key) else {
                here.push(key);
                continue;
            };
            let part = PartId::of_key(&key_bytes);
            let owners = view.as_ref().map_or(&[][..], |view| view.owners_of(part));
            match load_target(mode, part, me, owners, &capable) {
                LoadTarget::Here => here.push(key),
                LoadTarget::Peer(peer) => remote.entry(peer).or_default().push((key, key_bytes)),
            }
        }
        let local = async {
            if here.is_empty() {
                Ok(HashMap::new())
            } else {
                self.shard.call_loader(here).await
            }
        };
        let remotes = futures::future::join_all(
            remote
                .into_iter()
                .map(|(peer, keys)| self.load_on(peer, keys)),
        );
        let (local, remotes) = tokio::join!(local, remotes);
        let mut sourced = Sourced {
            store: local?,
            answer: HashMap::new(),
        };
        for answer in remotes {
            let answer = answer?;
            sourced.store.extend(answer.store);
            sourced.answer.extend(answer.answer);
        }
        Ok(sourced)
    }

    /// Asks `peer` to load `keys`. A record it stores comes back to be
    /// stored here too in `Mode::Invalidation`, where each node caches its
    /// own working set, and only answered with in `Mode::Distributed`,
    /// where `peer` owns the key and this node does not. Keys loaded here
    /// because `peer` declined or could not be reached come back to store;
    /// the shard stores them only where it owns them, so a `Mode::Distributed`
    /// non-owner answers with them and keeps no copy.
    pub(super) async fn load_on(
        &self,
        peer: NodeId,
        keys: Vec<(K, bytes::Bytes)>,
    ) -> Result<Sourced<K, V>, LoadError> {
        let request = keys
            .iter()
            .map(|(_, key_bytes)| key_bytes.clone())
            .collect();
        let outcome = self
            .cluster
            .mesh()
            .load(peer, SmolStr::new(self.shard.name()), request)
            .await;
        let (found, uncached) = match outcome {
            Ok(LoadOutcome::Loaded { found, uncached }) => (found, uncached),
            Ok(LoadOutcome::Failed(message)) => {
                return Err(Box::new(RemoteLoaderError {
                    node: peer,
                    message,
                }));
            }
            Ok(LoadOutcome::Declined) | Err(_) => {
                let keys = keys.into_iter().map(|(key, _)| key).collect();
                return Ok(Sourced {
                    store: self.shard.call_loader(keys).await?,
                    answer: HashMap::new(),
                });
            }
        };
        let mut by_bytes: HashMap<bytes::Bytes, K> = keys
            .into_iter()
            .map(|(key, key_bytes)| (key_bytes, key))
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
        Ok(sourced)
    }

    /// [`loader_peers`] for this cache.
    pub(super) fn loader_peers(&self) -> HashSet<NodeId> {
        loader_peers(&self.cluster, self.shard.name(), self.shard.mode())
    }
}
