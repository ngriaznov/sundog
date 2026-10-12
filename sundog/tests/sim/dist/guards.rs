//! The distribution-mode guard scenarios: the non-owner write guard and releasing buckets.

use super::{
    DistNode, DistNodeParams, assert_holds_only_owned_or_releasing, new_dist_node, peers_excluding,
    republish_all, shard_of, spawn_dist_nodes,
};
use crate::handler::ShardHandler;
use crate::support::{
    TICK, block_on, cache_name, key_bytes, new_shard, part_of_u32, peer_list_of, run_steps,
    run_until, sim_seed, steps_for, value_of,
};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use sundog::OwnershipView;
use sundog::config::ClusterConfig;
use sundog::net::{Mesh, MsgClass, RequestHandler};
use sundog::node::NodeId;
use sundog::store::{PartId, ShardOps};
use sundog::wire::Msg;
use turmoil::Builder;

/// For a seeded, randomized sequence of membership toggles (crash/bounce,
/// never below two live nodes) and writes/removes on random live nodes,
/// checks after every step that every node's local content is contained in
/// its own current owned-or-releasing bucket set, equivalently, that a
/// node holding neither ownership nor a residency grace on a bucket holds
/// nothing in it.
#[test]
fn distributed_non_owner_never_applies_as_a_property() {
    const OWNERS: u8 = 2;
    const KEY_SPACE: u32 = 40;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5400;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(4001), "prop-node-a", port),
        (NodeId::from(4002), "prop-node-b", port),
        (NodeId::from(4003), "prop-node-c", port),
        (NodeId::from(4004), "prop-node-d", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x9A2C_0104))
        .tick_duration(TICK)
        .fail_rate(0.02)
        .repair_rate(0.8)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(40))
        .build();

    spawn_dist_nodes(&mut sim, &nodes, {
        let roster = roster.clone();
        move |node| DistNodeParams {
            node: node.node,
            label: "n",
            port: node.port,
            peers: peers_excluding(&roster, node.node),
            ops: Arc::new(StdMutex::new(VecDeque::new())),
            write_period: Duration::from_secs(3600),
            fan_out_period: Duration::from_millis(15),
            ae_period: Duration::from_millis(80),
            rebalance_period: Duration::from_millis(80),
            disown_grace: Duration::from_millis(150),
            ops_issued: None,
        }
    });

    let mut rng = StdRng::seed_from_u64(sim_seed(0xC0DE_5555));
    let mut live: Vec<NodeId> = node_ids.clone();

    for step in 0..40 {
        if rng.random_range(0..3u32) == 0 {
            let idx = rng.random_range(0..node_ids.len());
            let target = node_ids[idx];
            let host = nodes[idx].host;
            if live.contains(&target) {
                if live.len() > 2 {
                    live.retain(|&n| n != target);
                    republish_all(&nodes, &live, k);
                    sim.crash(host);
                }
            } else {
                sim.bounce(host);
                live.push(target);
                republish_all(&nodes, &live, k);
            }
        } else {
            let writer = live[rng.random_range(0..live.len())];
            let key = rng.random_range(0..KEY_SPACE);
            let shard = shard_of(&nodes, writer);
            if rng.random_range(0..4u32) == 0 {
                let _ = block_on(shard.remove(&key));
            } else {
                let _ = block_on(shard.insert(key, format!("v{step}:{key}")));
            }
        }
        run_steps(&mut sim, steps_for(Duration::from_millis(30)));
        assert_holds_only_owned_or_releasing(
            &format!("checkpoint {step}"),
            &nodes,
            KEY_SPACE,
            true,
        );
    }

    for &id in &node_ids {
        if !live.contains(&id) {
            sim.bounce(nodes.iter().find(|n| n.node == id).unwrap().host);
            live.push(id);
        }
    }
    republish_all(&nodes, &node_ids, k);
    run_steps(&mut sim, steps_for(Duration::from_secs(2)));
    assert_holds_only_owned_or_releasing("final", &nodes, KEY_SPACE, true);
}

/// Displaces one of a bucket's two owners with a phantom fourth node,
/// advertised into the eligible set but never run as a real host since
/// only the rendezvous computation, not its reachability, matters here,
/// so the departed owner enters disown-grace while its former co-owner
/// keeps the bucket. A one-shot prober dials the departed owner directly,
/// well within the grace window, proving it still answers a digest
/// mismatch (`Mesh::ae_round`) and an explicit entries listing
/// (`Mesh::ae_entries`) with real data, then sends it a fresh `Replicate`
/// for a new key in the same bucket, proving the inbound-apply guard drops
/// it. The bucket's pre-existing data survives untouched through the grace
/// window, then is released once the grace period elapses.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario's setup, prober, and phased assertions read best kept together"
)]
fn distributed_releasing_bucket_still_answers_anti_entropy_but_never_accepts_a_fresh_apply() {
    const OWNERS: u8 = 2;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5500;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(5001), "grace-node-a", port),
        (NodeId::from(5002), "grace-node-b", port),
        (NodeId::from(5003), "grace-node-c", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();
    let target_part = PartId::from_raw(0);

    let initial_owners = OwnershipView::compute(node_ids[0], node_ids.clone(), k)
        .owners_of(target_part)
        .to_vec();
    assert_eq!(initial_owners.len(), 2);
    // `owners_of` is ordered by descending rendezvous score: index 0 is the
    // strictly higher-scoring owner. Adding one more eligible node can only
    // ever displace the *weaker* of the two from the top-2, the stronger
    // owner would have to be outscored by the newcomer to fall out, which
    // would put the newcomer in its place instead, not remove it outright,
    // so `leaving` must be the weaker owner for the phantom search below to
    // have a solution.
    let staying = initial_owners[0];
    let leaving = initial_owners[1];

    // A phantom fourth node: never run as a real host, chosen purely so
    // the rendezvous computation displaces `leaving` from `target_part`
    // without displacing `staying`.
    let phantom = (9_000_000u64..9_001_000)
        .map(NodeId::from)
        .find(|&candidate| {
            let mut eligible = node_ids.clone();
            eligible.push(candidate);
            let owners = OwnershipView::compute(candidate, eligible, k)
                .owners_of(target_part)
                .to_vec();
            owners.contains(&staying) && !owners.contains(&leaving)
        })
        .expect("some candidate id displaces the leaving owner alone");

    let all_bucket_keys: Vec<u32> = (0..5_000_000)
        .filter(|&key| part_of_u32(key) == target_part)
        .take(4)
        .collect();
    assert_eq!(
        all_bucket_keys.len(),
        4,
        "test setup sanity: enough keys map to the target bucket"
    );
    let bucket_keys = &all_bucket_keys[0..3];
    let fresh_key = all_bucket_keys[3];

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x9BAC_E505))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(20))
        .build();

    // Real time, like the grace clock: long enough that the probe phase's
    // virtual seconds, run in a debug build, finish inside it.
    let disown_grace = Duration::from_secs(2);
    spawn_dist_nodes(&mut sim, &nodes, {
        let roster = roster.clone();
        move |node| DistNodeParams {
            node: node.node,
            label: "n",
            port: node.port,
            peers: peers_excluding(&roster, node.node),
            ops: Arc::new(StdMutex::new(VecDeque::new())),
            write_period: Duration::from_secs(3600),
            fan_out_period: Duration::from_millis(15),
            ae_period: Duration::from_millis(60),
            rebalance_period: Duration::from_millis(60),
            disown_grace,
            ops_issued: None,
        }
    });

    for &key in bucket_keys {
        block_on(shard_of(&nodes, leaving).insert(key, format!("v:{key}"))).expect("insert");
    }
    run_until(&mut sim, steps_for(Duration::from_secs(5)), || {
        bucket_keys
            .iter()
            .all(|&key| value_of(shard_of(&nodes, staying), key).is_some())
    })
    .expect("both original owners hold the bucket's keys before the membership change");

    // Advertise the phantom fourth node: `leaving` loses `target_part`
    // and starts its disown-grace clock; `staying` keeps it.
    let mut eligible = node_ids.clone();
    eligible.push(phantom);
    republish_all(&nodes, &eligible, k);
    let released_at = Instant::now() + disown_grace;
    assert!(
        nodes
            .iter()
            .find(|n| n.node == leaving)
            .unwrap()
            .residency
            .is_releasing(target_part),
        "the departed owner marks the bucket releasing immediately on the view change"
    );

    // A legitimate record for a brand-new key in the same bucket, built
    // from the current owner's own applied copy rather than hand-rolling a
    // wire frame.
    block_on(shard_of(&nodes, staying).insert(fresh_key, "fresh".to_string())).expect("insert");
    let fresh_rec = block_on(ShardOps::records_for(
        shard_of(&nodes, staying).as_ref(),
        vec![key_bytes(fresh_key)],
    ))
    .into_iter()
    .next()
    .expect("fresh_key is inserted above");

    let leaving_host = nodes.iter().find(|n| n.node == leaving).unwrap().host;
    let leaving_port = nodes.iter().find(|n| n.node == leaving).unwrap().port;
    let prober_id = NodeId::from(5999);
    let ae_mismatch_count = Arc::new(AtomicUsize::new(0));
    let entries_count = Arc::new(AtomicUsize::new(0));
    let probe_done = Arc::new(AtomicBool::new(false));
    let ae_mismatch_count_probe = Arc::clone(&ae_mismatch_count);
    let entries_count_probe = Arc::clone(&entries_count);
    let probe_done_probe = Arc::clone(&probe_done);
    sim.client("grace-prober", async move {
        let handler: Arc<dyn RequestHandler> =
            Arc::new(ShardHandler::new(Arc::new(new_shard(prober_id))));
        let bind_addr = SocketAddr::from(([0, 0, 0, 0], 5599));
        let (mesh, _inbound) =
            Mesh::spawn(bind_addr, prober_id, 1, &ClusterConfig::default(), handler).await?;
        mesh.update_peers(peer_list_of(&[(leaving, leaving_host, leaving_port)]));

        // A deliberately wrong digest forces a mismatch reply carrying
        // real entries, proving the responder still answers.
        if let Ok(mismatched) = mesh
            .ae_round(leaving, cache_name(), vec![(target_part.bucket(), 0)])
            .await
        {
            ae_mismatch_count_probe.fetch_add(mismatched.len(), Ordering::Relaxed);
        }
        if let Ok(listing) = mesh
            .ae_entries(leaving, cache_name(), vec![target_part.bucket()])
            .await
        {
            let count: usize = listing.into_iter().map(|(_, entries)| entries.len()).sum();
            entries_count_probe.fetch_add(count, Ordering::Relaxed);
        }

        // A fresh key in the same bucket, sent as a live `Replicate`:
        // the inbound-apply guard must drop it.
        mesh.send(
            leaving,
            MsgClass::Replicate,
            Msg::Replicate {
                cache: cache_name(),
                rec: fresh_rec,
            },
        );
        probe_done_probe.store(true, Ordering::Relaxed);
        Ok(())
    });

    run_until(&mut sim, steps_for(Duration::from_secs(3)), || {
        probe_done.load(Ordering::Relaxed)
    })
    .expect("the prober completes within the budget");
    run_steps(&mut sim, steps_for(Duration::from_millis(100)));

    assert!(
        Instant::now() < released_at,
        "the probe phase finishes inside the real-time grace"
    );
    assert!(
        ae_mismatch_count.load(Ordering::Relaxed) > 0,
        "the releasing node still answers a digest mismatch with real entries"
    );
    assert!(
        entries_count.load(Ordering::Relaxed) > 0,
        "the releasing node still answers an explicit entries listing"
    );
    assert!(
        value_of(shard_of(&nodes, leaving), fresh_key).is_none(),
        "the releasing node drops a fresh inbound apply for its released bucket"
    );
    for &key in bucket_keys {
        assert!(
            value_of(shard_of(&nodes, leaving), key).is_some(),
            "existing data for a releasing bucket is not wiped before grace elapses"
        );
    }

    // Once the grace period fully elapses, the bucket is released.
    // `ResidencySet`'s grace clock is stamped from real `Instant::now()`,
    // not turmoil's virtual clock, so real time must pass; see
    // `distributed_rebalance_under_churn`'s identical comment.
    std::thread::sleep(released_at.saturating_duration_since(Instant::now()) + disown_grace / 2);
    run_steps(&mut sim, steps_for(disown_grace));
    for &key in bucket_keys {
        assert!(
            value_of(shard_of(&nodes, leaving), key).is_none(),
            "the bucket's content is dropped once disown grace elapses"
        );
    }
}
