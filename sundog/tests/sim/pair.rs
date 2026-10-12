//! The two-node symmetric pair harness: `node_loop`, `NodeParams` and `spawn_symmetric_pair`.

use crate::ae_round::ae_round_with_sketch;
use crate::handler::{ShardHandler, dispatch_inbound, fan_out};
use crate::support::{SimResult, TestShard, peer_list_of};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use sundog::config::ClusterConfig;
use sundog::net::{InboundMsg, Mesh, RequestHandler};
use sundog::node::NodeId;
use turmoil::Sim;

/// One symmetric peer's whole role in scenarios 1 and 2: write its own key
/// range on a timer, fan each write out, run anti-entropy on a separate
/// timer, and dispatch inbound traffic. `remove_on_repeat` turns a key's
/// second occurrence into a remove; `ops_issued` counts every issued op.
#[derive(Clone)]
pub(crate) struct NodeParams {
    pub(crate) node: NodeId,
    pub(crate) label: &'static str,
    pub(crate) port: u16,
    pub(crate) peers: Vec<(NodeId, &'static str, u16)>,
    pub(crate) keys: Vec<u32>,
    pub(crate) write_period: Duration,
    pub(crate) ae_period: Duration,
    pub(crate) dup_factor: usize,
    pub(crate) ae_failures: Option<Arc<AtomicUsize>>,
    pub(crate) remove_on_repeat: bool,
    pub(crate) ops_issued: Option<Arc<AtomicUsize>>,
    /// Overrides the responder's `ae_sketch_min_bucket`; `None` keeps
    /// [`ClusterConfig::default`]'s value, past what any scenario but the
    /// sketch one ever populates a bucket to.
    pub(crate) ae_sketch_min_bucket: Option<usize>,
    /// Overrides the responder's `ae_part_min_bucket`; `None` keeps
    /// [`ClusterConfig::default`]'s value, past what any scenario but the
    /// part one ever populates a bucket to.
    pub(crate) ae_part_min_bucket: Option<usize>,
    /// Counts every `AeMismatch::Bucket` reply this node's rounds receive:
    /// a full bucket listing, the cost the part path exists to avoid. `None`
    /// for scenarios that don't care.
    pub(crate) bucket_listings: Option<Arc<AtomicUsize>>,
}

pub(crate) async fn node_loop(params: NodeParams, shard: Arc<TestShard>) -> SimResult {
    let defaults = ClusterConfig::default();
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::with_min_buckets(
        Arc::clone(&shard),
        params
            .ae_part_min_bucket
            .unwrap_or(defaults.ae_part_min_bucket),
        params
            .ae_sketch_min_bucket
            .unwrap_or(defaults.ae_sketch_min_bucket),
    ));
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

    let mut keys = params.keys.into_iter();
    let mut seen: HashSet<u32> = HashSet::new();
    let mut write_tick = tokio::time::interval(params.write_period);
    write_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ae_tick = tokio::time::interval(params.ae_period);
    ae_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            Some(InboundMsg { msg, .. }) = inbound.recv() => {
                dispatch_inbound(shard.as_ref(), msg).await;
            }
            _ = write_tick.tick() => {
                if let Some(key) = keys.next() {
                    if params.remove_on_repeat && !seen.insert(key) {
                        let _ = shard.remove(&key).await;
                    } else {
                        let value = format!("{}:{key}", params.label);
                        let _ = shard.insert(key, value).await;
                    }
                    fan_out(shard.as_ref(), &mesh, &peer_ids, key, params.dup_factor).await;
                    if let Some(counter) = params.ops_issued.as_ref() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            _ = ae_tick.tick() => {
                for &peer in &peer_ids {
                    if !ae_round_with_sketch(
                        &mesh,
                        shard.as_ref(),
                        peer,
                        params.bucket_listings.as_deref(),
                    )
                    .await
                        && let Some(counter) = params.ae_failures.as_ref()
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

/// A pair of symmetric peers' fixed setup: hosts, ids, key ranges, timing.
pub(crate) struct PairSpec {
    pub(crate) host_a: &'static str,
    pub(crate) host_b: &'static str,
    pub(crate) node_a: NodeId,
    pub(crate) node_b: NodeId,
    pub(crate) port: u16,
    pub(crate) keys_a: Vec<u32>,
    pub(crate) keys_b: Vec<u32>,
    pub(crate) write_period: Duration,
    pub(crate) ae_period: Duration,
    pub(crate) dup_factor: usize,
}

pub(crate) fn spawn_symmetric_pair(
    sim: &mut Sim<'_>,
    spec: PairSpec,
    shard_a: Arc<TestShard>,
    shard_b: Arc<TestShard>,
    ae_failures: Option<Arc<AtomicUsize>>,
) {
    let params_a = NodeParams {
        node: spec.node_a,
        label: "a",
        port: spec.port,
        peers: vec![(spec.node_b, spec.host_b, spec.port)],
        keys: spec.keys_a,
        write_period: spec.write_period,
        ae_period: spec.ae_period,
        dup_factor: spec.dup_factor,
        ae_failures: ae_failures.clone(),
        remove_on_repeat: false,
        ops_issued: None,
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: None,
        bucket_listings: None,
    };
    sim.host(spec.host_a, move || {
        let shard = Arc::clone(&shard_a);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: spec.node_b,
        label: "b",
        port: spec.port,
        peers: vec![(spec.node_a, spec.host_a, spec.port)],
        keys: spec.keys_b,
        write_period: spec.write_period,
        ae_period: spec.ae_period,
        dup_factor: spec.dup_factor,
        ae_failures,
        remove_on_repeat: false,
        ops_issued: None,
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: None,
        bucket_listings: None,
    };
    sim.host(spec.host_b, move || {
        let shard = Arc::clone(&shard_b);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });
}
