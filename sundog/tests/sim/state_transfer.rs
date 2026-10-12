//! The state-transfer scenario: a donor that crashes mid-transfer and the receiver that re-picks.

use crate::handler::{ShardHandler, dispatch_inbound};
use crate::support::{
    NET_TIMEOUT, SimResult, TICK, TestShard, cache_name, new_shard, peer_list_of, seed_shard,
    sim_seed, steps_for, value_of,
};
use futures::StreamExt as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use sundog::config::ClusterConfig;
use sundog::net::{InboundMsg, Mesh, RequestHandler};
use sundog::node::NodeId;
use sundog::store::ShardOps;
use turmoil::{Builder, Sim};

fn spawn_donor(
    sim: &mut Sim<'_>,
    host: &'static str,
    node: NodeId,
    port: u16,
    shard: Arc<TestShard>,
    peers: Vec<(NodeId, &'static str, u16)>,
) {
    sim.host(host, move || {
        let shard = Arc::clone(&shard);
        let peers = peers.clone();
        async move { donor_software(node, port, peers, shard).await }
    });
}

async fn donor_software(
    node: NodeId,
    port: u16,
    peers: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let (mesh, mut inbound) =
        Mesh::spawn(bind_addr, node, 1, &ClusterConfig::default(), handler).await?;
    mesh.update_peers(peer_list_of(&peers));
    loop {
        let Some(InboundMsg { msg, .. }) = inbound.recv().await else {
            return Ok(());
        };
        dispatch_inbound(shard.as_ref(), msg).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_receiver(
    sim: &mut Sim<'_>,
    host: &'static str,
    node: NodeId,
    port: u16,
    donors: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
    applied: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
) {
    sim.host(host, move || {
        let shard = Arc::clone(&shard);
        let donors = donors.clone();
        let applied = Arc::clone(&applied);
        let done = Arc::clone(&done);
        async move { receiver_software(node, port, donors, shard, applied, done).await }
    });
}

async fn receiver_software(
    node: NodeId,
    port: u16,
    donors: Vec<(NodeId, &'static str, u16)>,
    shard: Arc<TestShard>,
    applied: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
) -> SimResult {
    let handler: Arc<dyn RequestHandler> = Arc::new(ShardHandler::new(Arc::clone(&shard)));
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
    let (mesh, mut inbound) =
        Mesh::spawn(bind_addr, node, 1, &ClusterConfig::default(), handler).await?;
    mesh.update_peers(peer_list_of(&donors));
    let donor_ids: Vec<NodeId> = donors.iter().map(|&(id, _, _)| id).collect();

    let ok = warm_up(&mesh, shard.as_ref(), &donor_ids, &applied).await;
    done.store(ok, Ordering::Relaxed);

    loop {
        let Some(InboundMsg { msg, .. }) = inbound.recv().await else {
            return Ok(());
        };
        dispatch_inbound(shard.as_ref(), msg).await;
    }
}

/// A simplified stand-in for `cluster::state_transfer::run`: tries each
/// donor in order, applying records as they stream in, and falls through
/// to the next donor the moment the stream reports an error.
async fn warm_up(mesh: &Mesh, shard: &TestShard, donors: &[NodeId], applied: &AtomicUsize) -> bool {
    for &donor in donors {
        // A donor that declines, `Ok(None)`, is skipped like an unreachable one.
        let Ok(Ok(Some(mut stream))) =
            tokio::time::timeout(NET_TIMEOUT, mesh.request_state(donor, cache_name())).await
        else {
            continue;
        };
        let mut broke = false;
        loop {
            match tokio::time::timeout(NET_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    applied.fetch_add(chunk.len(), Ordering::Relaxed);
                    ShardOps::apply_remote_batch(shard, chunk).await;
                }
                Ok(Some(Err(_))) | Err(_) => {
                    broke = true;
                    break;
                }
                Ok(None) => break,
            }
        }
        if !broke {
            return true;
        }
    }
    false
}

#[test]
fn donor_crash_mid_state_transfer_repicks_and_completes() {
    let donor1 = NodeId::from(21);
    let donor2 = NodeId::from(22);
    let receiver = NodeId::from(23);
    let total_keys = 1_200u32;
    let port = 4200;

    let shard_d1 = Arc::new(seed_shard(donor1, 0..total_keys));
    let shard_d2 = Arc::new(seed_shard(donor2, 0..total_keys));
    let shard_r = Arc::new(new_shard(receiver));

    let applied = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xD0A0_5501))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(2))
        .max_message_latency(Duration::from_millis(30))
        .build();

    spawn_donor(
        &mut sim,
        "donor-1",
        donor1,
        port,
        Arc::clone(&shard_d1),
        vec![(receiver, "receiver", port)],
    );
    spawn_donor(
        &mut sim,
        "donor-2",
        donor2,
        port,
        Arc::clone(&shard_d2),
        vec![(receiver, "receiver", port)],
    );
    spawn_receiver(
        &mut sim,
        "receiver",
        receiver,
        port,
        vec![(donor1, "donor-1", port), (donor2, "donor-2", port)],
        Arc::clone(&shard_r),
        Arc::clone(&applied),
        Arc::clone(&done),
    );

    let mut crashed = false;
    let max_steps = steps_for(Duration::from_secs(20));
    for _ in 0..max_steps {
        sim.step().expect("turmoil step succeeds");
        if !crashed && applied.load(Ordering::Relaxed) >= 200 {
            sim.crash("donor-1");
            crashed = true;
        }
        if done.load(Ordering::Relaxed) {
            break;
        }
    }

    assert!(
        crashed,
        "test setup sanity: donor-1 is crashed mid-transfer"
    );
    assert!(
        done.load(Ordering::Relaxed),
        "receiver re-picks the surviving donor and completes warm-up within the step budget"
    );
    for key in [0u32, 599, total_keys - 1] {
        assert_eq!(
            value_of(&shard_r, key),
            value_of(&shard_d2, key),
            "receiver's warmed copy matches the surviving donor for key {key}"
        );
    }
}
