//! The distribution-mode harness: nodes, ownership publishing and the owner-routed node loop.

// ---------------------------------------------------------------------
// Distribution mode (`Mode::Distributed`): deterministic turmoil
// simulations of ownership under churn, partition, owner loss, the
// non-owner write guard, and residency's disown-grace window.
//
// `OwnershipTracker`, `OwnershipView`, `ResidencySet`,
// `ownership::ownership_diff`, `Shard::with_ownership`, and a shard's
// fan-out queue are all `pub(crate)`; the pieces these scenarios need are
// re-exported under the `sim` feature the same way `cluster::anti_entropy`'s
// pieces are in `ae_round`: `sundog::{OwnershipTracker, OwnershipView,
// ResidencySet, ownership_diff}`, `Shard::with_ownership_for_sim`,
// `Shard::drain_fan_out_for_sim`, and `store::SimFanOut`.
//
// Ownership itself is driven the same way membership already is in the
// suite: hand-scripted from the test, via each node's own
// `watch::Sender<Arc<OwnershipView>>`, rather than through a live
// `refresh_task`/gossip loop this harness has no chitchat layer to run.
// `republish_view` mirrors `cluster::rebalance::rebalance_task`'s own
// reaction to a view change, mark newly lost buckets releasing, unmark
// newly regained ones, so residency behaves exactly as it does in
// production. Bucket transfer to a newly owning node goes through the same
// self-healing anti-entropy backstop `cluster::rebalance`'s own doc calls
// out, `ae_round_with_sketch` (in `ae_round`), rather than an eager
// `Mesh::request_buckets` pull (`pub(crate)`, out of reach here): once a
// gained bucket starts appearing in the new owner's own `ShardOps::digests`,
// the previous owner's own round shows a mismatch and the ordinary push
// path lands it.

mod guards;
mod rebalance;

use crate::ae_round::ae_round_with_sketch;
use crate::handler::{ShardHandler, dispatch_inbound};
use crate::support::{
    SimResult, TestShard, cache_name, key_bytes, part_of_u32, peer_list_of, value_of,
};
use smol_str::SmolStr;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use sundog::config::ClusterConfig;
use sundog::net::{InboundMsg, Mesh, MsgClass, RequestHandler};
use sundog::node::NodeId;
use sundog::store::{Mode, PartId, Shard, ShardOps, SimFanOut};
use sundog::wire::{Msg, WireRecord};
use sundog::{Granularity, OwnershipTracker, OwnershipView, ResidencySet, ownership_diff};
use tokio::sync::watch;
use turmoil::Sim;

/// A distribution-mode scenario's per-node handle to what the test driver
/// keeps outside any node's own async loop: the shard, its ownership
/// view's publish side, and its residency set. A real cluster keeps all
/// three behind `cluster::rebalance`'s task; here the test drives them
/// directly.
struct DistNode {
    node: NodeId,
    host: &'static str,
    port: u16,
    shard: Arc<TestShard>,
    tx: watch::Sender<Arc<OwnershipView>>,
    residency: Arc<ResidencySet>,
}

/// Builds one `Mode::Distributed` node: a fresh shard with an ownership
/// tracker and residency set attached via `Shard::with_ownership_for_sim`,
/// seeded to the solo view `OwnershipTracker::seed` always starts from;
/// see the `distributed_shard` fixture in `store/tests/mod.rs` for
/// the identical construction. The caller republishes the scenario's real
/// starting view immediately after, via `republish_view`/`republish_all`.
fn new_dist_node(node: NodeId, host: &'static str, port: u16, owners: u8) -> DistNode {
    let k = NonZeroU8::new(owners).expect("owners is nonzero");
    let (tracker, tx) = OwnershipTracker::seed(
        node,
        &[],
        &HashMap::<NodeId, HashMap<SmolStr, Mode>>::new(),
        &cache_name(),
        k,
    );
    let residency = Arc::new(ResidencySet::new());
    let shard = Arc::new(
        Shard::new(
            cache_name(),
            Mode::Distributed { owners: k },
            node,
            100_000,
            None,
            None,
        )
        .with_ownership_for_sim(tracker, Arc::clone(&residency)),
    );
    DistNode {
        node,
        host,
        port,
        shard,
        tx,
        residency,
    }
}

/// The `(NodeId, host, port)` entries of `roster` other than `self_node`,
/// for wiring one distribution-mode node's own peer list.
fn peers_excluding(
    roster: &[(NodeId, &'static str, u16)],
    self_node: NodeId,
) -> Vec<(NodeId, &'static str, u16)> {
    roster
        .iter()
        .copied()
        .filter(|&(node, _, _)| node != self_node)
        .collect()
}

/// The `Arc<TestShard>` for the node named `id` in `nodes`.
fn shard_of(nodes: &[DistNode], id: NodeId) -> &Arc<TestShard> {
    &nodes
        .iter()
        .find(|n| n.node == id)
        .expect("known node id")
        .shard
}

/// Recomputes and republishes one node's ownership view over `eligible`,
/// updating its residency set exactly as
/// `cluster::rebalance::rebalance_task` reacts to a real view change: a
/// newly lost bucket starts its disown-grace clock, a newly regained one
/// clears it, so a flap never accumulates toward release.
fn republish_view(
    self_node: NodeId,
    tx: &watch::Sender<Arc<OwnershipView>>,
    residency: &ResidencySet,
    eligible: Vec<NodeId>,
    k: NonZeroU8,
    granularity: Granularity,
) {
    let new_view = Arc::new(OwnershipView::compute_at(
        self_node,
        eligible,
        k,
        granularity,
    ));
    tx.send_if_modified(|current| {
        if current.view_hash() == new_view.view_hash() {
            return false;
        }
        let (gained, lost) = ownership_diff(current, &new_view);
        if !lost.is_empty() {
            residency.mark_releasing(&lost);
        }
        if !gained.is_empty() {
            residency.unmark(&gained);
        }
        *current = Arc::clone(&new_view);
        true
    });
}

/// [`republish_view`] for every node in `nodes` whose id is in `live`, all
/// against the view computed over exactly `live`: the hand-scripted
/// membership feed's counterpart to a real gossip round converging on a
/// new peer set. A node not in `live` is left untouched, it may be
/// crashed, in which case nothing reads its tracker until it bounces back
/// and this is called again with it included.
fn republish_all(nodes: &[DistNode], live: &[NodeId], k: NonZeroU8) {
    republish_all_at(nodes, live, k, Granularity::Bucket);
}

/// [`republish_all`] at `granularity`.
fn republish_all_at(nodes: &[DistNode], live: &[NodeId], k: NonZeroU8, granularity: Granularity) {
    for node in nodes {
        if live.contains(&node.node) {
            republish_view(
                node.node,
                &node.tx,
                &node.residency,
                live.to_vec(),
                k,
                granularity,
            );
        }
    }
}

/// Sends one `Mode::Distributed` record to its bucket's current owners
/// (`self_node` excluded), the per-write counterpart of
/// `cluster::group_by_owner_set`/`fan_out_by_owner_set`'s owner-set
/// grouping; [`fan_out_owned`] calls this once per `Applied` item, and
/// retries a `Forward` item across ticks the same way.
fn send_to_owners(mesh: &Mesh, view: &OwnershipView, self_node: NodeId, rec: &WireRecord) {
    for &owner in view.owners_of(PartId::of_key(rec.key.as_ref())) {
        if owner != self_node {
            mesh.send(
                owner,
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: cache_name(),
                    rec: rec.clone(),
                },
            );
        }
    }
}

/// Drains the shard's fan-out queue: an `Applied` item re-fetches its
/// current record via `ShardOps::records_for`, exactly as `Replicated`
/// mode's own [`fan_out`] helper in `handler` does, and is sent once (a lost send
/// still self-heals via anti-entropy, since the writing owner keeps its
/// own copy to push again next round); a `Forward` item already carries
/// its record, since a non-owner write is never applied to `engine` in the
/// first place, and is returned to the caller to retry across several
/// ticks via [`dist_node_loop`]'s own retry pool, a forwarding node keeps
/// no local copy once it has forwarded, so a lost send has no
/// anti-entropy backstop the way a lost `Applied` send does.
async fn fan_out_owned(
    shard: &TestShard,
    mesh: &Mesh,
    view: &OwnershipView,
    self_node: NodeId,
) -> Vec<WireRecord> {
    let mut forwards = Vec::new();
    for item in shard.drain_fan_out_for_sim() {
        match item {
            SimFanOut::Applied(key) => {
                if let Some(rec) = ShardOps::records_for(shard, vec![key_bytes(key)])
                    .await
                    .into_iter()
                    .next()
                {
                    send_to_owners(mesh, view, self_node, &rec);
                }
            }
            SimFanOut::Forward(rec) => forwards.push(rec),
        }
    }
    forwards
}

/// One scripted write for a distribution-mode node's own op list; see
/// [`DistNodeParams::ops`].
#[derive(Clone, Copy)]
enum DistOp {
    Insert(u32),
    Remove(u32),
}

/// One distribution-mode node's role: like [`NodeParams`] for
/// `Mode::Replicated`, but fanning writes out to a key's current owners
/// (via [`fan_out_owned`]) instead of broadcasting to every peer, gating
/// anti-entropy's peer choice through `ShardOps::ae_peer_filter`, and
/// releasing parts whose disown grace has elapsed from [`release_loop`],
/// mirroring `cluster::rebalance::rebalance_task`'s release half. The
/// gained half is left to anti-entropy's self-healing backstop; see this
/// section's own doc.
#[derive(Clone)]
struct DistNodeParams {
    node: NodeId,
    label: &'static str,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
    /// May be empty: a scenario that drives every write itself, directly on
    /// a node's `Arc<TestShard>` from outside this loop, still needs the
    /// loop running so its own `fan_out_tick`/`ae_tick` pick the write
    /// up. Shared, not owned outright: `sim.host`'s closure
    /// re-clones `DistNodeParams` on every restart (a bounce included), and
    /// an owned `Vec` would replay every already-issued op from scratch on
    /// each one, re-inserting an already-removed key with a fresh,
    /// LWW-winning `Hlc`, a real bug this harness hit once already. A
    /// shared queue keeps the harness's own restart honest: a bounced node
    /// resumes issuing its remaining ops, the way a real node resumes
    /// serving already-accepted API calls rather than an external caller
    /// replaying them.
    ops: Arc<StdMutex<VecDeque<DistOp>>>,
    write_period: Duration,
    /// Drains and dispatches the fan-out queue, decoupled from
    /// `write_period` so an externally issued write (never produced by this
    /// node's own `ops`) still gets picked up promptly.
    fan_out_period: Duration,
    ae_period: Duration,
    rebalance_period: Duration,
    disown_grace: Duration,
    ops_issued: Option<Arc<AtomicUsize>>,
}

/// How many `ae_tick`s a still-undelivered `Forward` item is retried for,
/// spread one attempt per tick rather than several copies back to back in
/// one tick, so each attempt gets an independent chance against a single
/// bad connection state instead of risking every copy failing together;
/// see [`fan_out_owned`]'s own doc for why a `Forward` item needs this and
/// an `Applied` one does not.
const FORWARD_RETRIES: u8 = 15;

/// Releases every part whose disown grace has elapsed, once per `period`.
/// Runs as its own task, as `cluster::rebalance::rebalance_task` does, so
/// an anti-entropy round waiting out network timeouts in
/// [`dist_node_loop`] never holds a release back.
async fn release_loop(
    shard: Arc<TestShard>,
    residency: Arc<ResidencySet>,
    period: Duration,
    grace: Duration,
) {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let due = residency.expired(grace);
        if !due.is_empty() {
            ShardOps::release_parts(shard.as_ref(), &due).await;
            residency.unmark(&due);
        }
    }
}

async fn dist_node_loop(
    params: DistNodeParams,
    shard: Arc<TestShard>,
    residency: Arc<ResidencySet>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], params.port));
    let (mesh, mut inbound) = Mesh::spawn(
        bind_addr,
        params.node,
        1,
        &ClusterConfig::default(),
        handler,
    )
    .await?;

    let peer_list = peer_list_of(&params.peers);
    mesh.update_peers(peer_list.clone());
    let peer_ids: Vec<NodeId> = peer_list.iter().map(|peer| peer.node).collect();

    // Forwarded writes not yet confirmed delivered, retried once per
    // `ae_tick` for `FORWARD_RETRIES` ticks each: see `fan_out_owned`'s own
    // doc for why a `Forward` item needs this and an `Applied` one does
    // not.
    let mut pending_forwards: Vec<(WireRecord, u8)> = Vec::new();

    let ops = params.ops;
    let mut write_tick = tokio::time::interval(params.write_period);
    write_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut fan_out_tick = tokio::time::interval(params.fan_out_period);
    fan_out_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ae_tick = tokio::time::interval(params.ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Dropped with the host's runtime on a crash; a bounce spawns a fresh one.
    tokio::spawn(release_loop(
        Arc::clone(&shard),
        residency,
        params.rebalance_period,
        params.disown_grace,
    ));

    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(shard.as_ref(), msg).await;
            }
            _ = write_tick.tick() => {
                let next_op = ops
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front();
                if let Some(op) = next_op {
                    match op {
                        DistOp::Insert(key) => {
                            let _ = shard.insert(key, format!("{}:{key}", params.label)).await;
                        }
                        DistOp::Remove(key) => {
                            let _ = shard.remove(&key).await;
                        }
                    }
                    if let Some(counter) = params.ops_issued.as_ref() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            _ = fan_out_tick.tick() => {
                if let Some(view) = ShardOps::ownership_view(shard.as_ref()) {
                    let new_forwards = fan_out_owned(shard.as_ref(), &mesh, &view, params.node).await;
                    for rec in &new_forwards {
                        send_to_owners(&mesh, &view, params.node, rec);
                    }
                    pending_forwards.extend(new_forwards.into_iter().map(|rec| (rec, FORWARD_RETRIES)));
                }
            }
            _ = ae_tick.tick() => {
                let (_, cohort) =
                    ShardOps::ae_peer_filter(shard.as_ref(), peer_ids.clone(), peer_ids.clone());
                for peer in cohort {
                    ae_round_with_sketch(&mesh, shard.as_ref(), peer, None).await;
                }
                // Retries for still-pending forwards, spaced at the AE
                // interval rather than the much tighter `fan_out_period`:
                // turmoil's `fail_rate` breaks the whole underlying
                // connection rather than dropping one frame with
                // retransmission, so several attempts fired close together
                // land on the same broken connection and all fail
                // together; spacing them out gives each one an independent
                // chance once the connection has had time to reconnect.
                if let Some(view) = ShardOps::ownership_view(shard.as_ref()) {
                    pending_forwards.retain_mut(|(rec, attempts_left)| {
                        send_to_owners(&mesh, &view, params.node, rec);
                        *attempts_left -= 1;
                        *attempts_left > 0
                    });
                }
            }
        }
    }
}

/// Whether every live node's local content is contained in its own current
/// owned (and, when `allow_releasing`, also releasing) bucket set. Shared by
/// the churn and non-owner-property scenarios in `rebalance` and `guards`.
fn assert_holds_only_owned_or_releasing(
    label: &str,
    nodes: &[DistNode],
    key_space: u32,
    allow_releasing: bool,
) {
    for n in nodes {
        let Some(view) = ShardOps::ownership_view(n.shard.as_ref()) else {
            continue;
        };
        for key in 0..key_space {
            if value_of(&n.shard, key).is_some() {
                let part = part_of_u32(key);
                let ok = view.owns(part) || (allow_releasing && n.residency.is_releasing(part));
                assert!(
                    ok,
                    "{label}: node {:?} holds key {key} (part {part}) it neither owns nor is releasing (is_releasing={})",
                    n.node,
                    n.residency.is_releasing(part)
                );
            }
        }
    }
}

/// Whether every key in `expected` is present on at least one node that
/// currently owns its bucket: the data-safety convergence signal churn
/// scenarios wait on, since distinct nodes legitimately hold different
/// buckets and a plain digest-equality check (as the replicated-mode
/// scenarios elsewhere in the suite use) does not apply.
fn dist_data_settled(nodes: &[DistNode], expected: &HashSet<u32>) -> bool {
    expected.iter().all(|&key| {
        let part = part_of_u32(key);
        nodes.iter().any(|n| {
            ShardOps::ownership_view(n.shard.as_ref()).is_some_and(|view| view.owns(part))
                && value_of(&n.shard, key).is_some()
        })
    })
}

/// [`dist_data_settled`]'s counterpart for a key removed rather than
/// surviving: every current owner of the key's bucket has forgotten it.
/// A removed key's own writer applies its tombstone
/// instantly, but the co-owner only learns of it via the ordinary
/// `Applied`-item fan-out (or, if that one send is lost, the next
/// anti-entropy round), [`dist_data_settled`] never looks at a removed
/// key at all, so this is the churn scenario's own explicit wait for that
/// second hop to land before treating the pre-churn baseline as settled.
fn dist_removals_settled(nodes: &[DistNode], removed: &HashSet<u32>) -> bool {
    removed.iter().all(|&key| {
        let part = part_of_u32(key);
        nodes.iter().all(|n| {
            !ShardOps::ownership_view(n.shard.as_ref()).is_some_and(|view| view.owns(part))
                || value_of(&n.shard, key).is_none()
        })
    })
}

/// Spawns `nodes[i]`'s `dist_node_loop` as a turmoil host for every `i`,
/// using the same `params` for each but for `node`/`port`/`peers`, which
/// come from the node itself.
fn spawn_dist_nodes(
    sim: &mut Sim<'_>,
    nodes: &[DistNode],
    mut params_for: impl FnMut(&DistNode) -> DistNodeParams,
) {
    for node in nodes {
        let params = params_for(node);
        let shard = Arc::clone(&node.shard);
        let residency = Arc::clone(&node.residency);
        sim.host(node.host, move || {
            let params = params.clone();
            let shard = Arc::clone(&shard);
            let residency = Arc::clone(&residency);
            async move { dist_node_loop(params, shard, residency).await }
        });
    }
}
