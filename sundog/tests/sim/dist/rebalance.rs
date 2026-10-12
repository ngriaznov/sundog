//! The ownership rebalance scenarios: churn, part granularity, partition heal and owner loss.

use super::{
    DistNode, DistNodeParams, DistOp, assert_holds_only_owned_or_releasing, dist_data_settled,
    dist_removals_settled, new_dist_node, peers_excluding, republish_all, republish_all_at,
    shard_of, spawn_dist_nodes,
};
use crate::support::{
    TICK, block_on, part_of_u32, run_steps, run_until, sim_seed, steps_for, value_of,
};
use std::collections::{HashSet, VecDeque};
use std::num::NonZeroU8;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use sundog::node::NodeId;
use sundog::store::{PartId, ShardOps};
use sundog::{Granularity, OwnershipView};
use turmoil::Builder;

/// Nodes join and leave a distributed cache (`owners=2`) repeatedly under
/// message loss and reordering. Each of four nodes writes a disjoint key
/// range, then removes every fourth key of its own range, so the surviving
/// set is deliberately not "every key ever inserted." One node at a time is
/// crashed and bounced back (never taking the live count below three, so
/// every bucket always keeps a live owner), with the self-healing
/// anti-entropy backstop given time to rebalance between each crash and the
/// next. Once churn stops and every disown grace has elapsed, the union of
/// every live node's content equals exactly the surviving set, and every
/// live node holds only buckets it currently owns.
#[test]
fn distributed_rebalance_under_churn() {
    rebalance_under_churn(Granularity::Bucket);
}

/// [`distributed_rebalance_under_churn`] with every view ranking parts:
/// each node owns scattered parts of every bucket, so rebalance, release
/// and repair all work below bucket level.
#[test]
fn distributed_rebalance_under_churn_at_part_granularity() {
    rebalance_under_churn(Granularity::Part);
}

#[allow(
    clippy::too_many_lines,
    reason = "one scenario's full setup, churn schedule, and assertions read best kept together"
)]
fn rebalance_under_churn(granularity: Granularity) {
    const OWNERS: u8 = 2;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5100;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(1001), "churn-node-a", port),
        (NodeId::from(1002), "churn-node-b", port),
        (NodeId::from(1003), "churn-node-c", port),
        (NodeId::from(1004), "churn-node-d", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all_at(&nodes, &node_ids, k, granularity);

    // Message loss turns on only once the initial write/remove plans have
    // settled (below), so that churn, the thing this scenario tests, runs
    // under real loss and reordering without also fighting
    // the transport's own worst case for a one-shot forward: turmoil's
    // `fail_rate` breaks a link outright until *some* further traffic on
    // it happens to trigger a repair check, which a forward that only ever
    // needs to reach its bucket's two owners once may never do on its own.
    // Reordering (the latency spread below) is in effect throughout.
    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xD157_C001))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    let ranges: [std::ops::Range<u32>; 4] = [0..20, 20..40, 40..60, 60..80];
    let labels = ["a", "b", "c", "d"];
    let mut expected: HashSet<u32> = HashSet::new();
    let mut removed_keys: HashSet<u32> = HashSet::new();
    let ops_counters: Vec<Arc<AtomicUsize>> =
        (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let mut op_lens = vec![0usize; 4];
    let mut plans: Vec<Vec<DistOp>> = Vec::new();
    for (i, range) in ranges.iter().enumerate() {
        let removed: Vec<u32> = range.clone().step_by(4).collect();
        for key in range.clone() {
            if removed.contains(&key) {
                removed_keys.insert(key);
            } else {
                expected.insert(key);
            }
        }
        let mut ops: Vec<DistOp> = range.clone().map(DistOp::Insert).collect();
        ops.extend(removed.into_iter().map(DistOp::Remove));
        op_lens[i] = ops.len();
        plans.push(ops);
    }

    spawn_dist_nodes(&mut sim, &nodes, {
        let roster = roster.clone();
        let counters = ops_counters.clone();
        let mut plans = plans.into_iter();
        let mut idx = 0usize;
        move |node| {
            let i = idx;
            idx += 1;
            DistNodeParams {
                node: node.node,
                label: labels[i],
                port: node.port,
                peers: peers_excluding(&roster, node.node),
                ops: Arc::new(StdMutex::new(VecDeque::from(
                    plans.next().expect("one plan per node"),
                ))),
                write_period: Duration::from_millis(20),
                // Deliberately not equal to `write_period`: both this and
                // `write_tick`'s own handler body complete synchronously
                // (no network await), so an identical period keeps them
                // perpetually in lock step and `biased`'s deterministic
                // tie-break would starve this tick forever.
                fan_out_period: Duration::from_millis(13),
                ae_period: Duration::from_millis(150),
                rebalance_period: Duration::from_millis(150),
                disown_grace: Duration::from_millis(300),
                ops_issued: Some(Arc::clone(&counters[i])),
            }
        }
    });

    // Phase 1: every node finishes issuing its own plan, then replication
    // settles onto every current owner before churn starts.
    run_until(&mut sim, steps_for(Duration::from_secs(15)), || {
        ops_counters
            .iter()
            .zip(&op_lens)
            .all(|(c, &len)| c.load(Ordering::Relaxed) >= len)
    })
    .expect("every node finishes its write/remove plan within the budget");
    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        dist_data_settled(&nodes, &expected) && dist_removals_settled(&nodes, &removed_keys)
    })
    .expect("every surviving key lands on at least one owner and every removal has landed before churn starts");

    // Churn itself now runs under message loss, on top of the reordering
    // already in effect: see this test's own setup comment for why loss is
    // held off until the plans above have cleanly settled. `repair_rate`
    // keeps `Builder`'s own default (1.0): any further traffic on a
    // link heals it on the very next attempt, so churn's own repeated
    // anti-entropy rounds are always enough to recover, without needing a
    // one-shot forward to succeed on an attempt the fail rate might deny.
    sim.set_fail_rate(0.03);

    // Phase 2: churn. One node down at a time, never below three live, each
    // window long enough for the self-healing AE backstop to rebalance
    // before the next crash.
    let mut live: Vec<NodeId> = node_ids.clone();
    let settle = Duration::from_secs(6);
    for &idx in &[2usize, 3usize] {
        let victim = node_ids[idx];
        live.retain(|&n| n != victim);
        republish_all_at(&nodes, &live, k, granularity);
        sim.crash(nodes[idx].host);
        run_until(&mut sim, steps_for(settle), || {
            dist_data_settled(&nodes, &expected) && dist_removals_settled(&nodes, &removed_keys)
        })
        .expect("data safety holds immediately after a node leaves");

        sim.bounce(nodes[idx].host);
        live.push(victim);
        republish_all_at(&nodes, &live, k, granularity);
        run_until(&mut sim, steps_for(settle), || {
            dist_data_settled(&nodes, &expected) && dist_removals_settled(&nodes, &removed_keys)
        })
        .expect("data safety holds once the node rejoins");
    }

    // Phase 3: let every disown grace fully elapse. `ResidencySet`'s grace
    // clock is stamped from real `Instant::now()`, not turmoil's virtual
    // clock (matching tombstone retention's own real-time deadline
    // elsewhere in this file), so real time, not simulated steps, must
    // pass before a rebalance tick has anything to release; the
    // ticks themselves then need simulated time. Both are repeated until
    // no node has a bucket left mid grace, bounded, since a bounced
    // node's loop may need more than one window to reach its tick.
    let grace = Duration::from_millis(300);
    for _ in 0..10 {
        std::thread::sleep(grace);
        run_steps(&mut sim, steps_for(grace * 3));
        if nodes
            .iter()
            .all(|n| n.residency.expired(Duration::ZERO).is_empty())
        {
            break;
        }
    }

    let mut union: HashSet<u32> = HashSet::new();
    for key in 0..80u32 {
        if nodes.iter().any(|n| value_of(&n.shard, key).is_some()) {
            union.insert(key);
        }
    }
    assert_eq!(
        union, expected,
        "the union of every live node's content is exactly the surviving set"
    );
    assert_holds_only_owned_or_releasing("after churn settles", &nodes, 80, false);
}

/// Splits the cluster into two halves, writes on both sides, including a
/// conflicting write to the same key, side two's strictly later so its
/// `Hlc` wins, then heals. `owners=2` over a two-node eligible set
/// degenerates to both sides owning everything while split (see
/// `owners_of_bucket`'s doc), so each side's writes apply locally without
/// forwarding. After healing, every node's independently computed view has
/// the same `view_hash`, and every write survives by version: the
/// conflicting key resolves to side two's value everywhere it lands, and
/// each side's private write survives on every node that ends up owning it.
#[test]
fn distributed_partition_then_heal_reconciles_ownership() {
    const OWNERS: u8 = 2;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5200;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(2001), "part-node-a", port),
        (NodeId::from(2002), "part-node-b", port),
        (NodeId::from(2003), "part-node-c", port),
        (NodeId::from(2004), "part-node-d", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();
    let side1 = [node_ids[0], node_ids[1]];
    let side2 = [node_ids[2], node_ids[3]];

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x9A27_1102))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
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
            fan_out_period: Duration::from_millis(20),
            ae_period: Duration::from_millis(100),
            rebalance_period: Duration::from_millis(100),
            disown_grace: Duration::from_millis(300),
            ops_issued: None,
        }
    });

    // Everyone converges on the single four-node view before the split.
    run_steps(&mut sim, steps_for(Duration::from_millis(200)));

    for &a in &side1 {
        for &b in &side2 {
            let host_a = nodes.iter().find(|n| n.node == a).unwrap().host;
            let host_b = nodes.iter().find(|n| n.node == b).unwrap().host;
            sim.partition(host_a, host_b);
        }
    }
    republish_all(&nodes, &side1, k);
    republish_all(&nodes, &side2, k);
    run_steps(&mut sim, steps_for(Duration::from_millis(100)));

    let shared_key = 999u32;
    block_on(shard_of(&nodes, side1[0]).insert(shared_key, "side1".to_string())).expect("insert");
    // Real time passing, not turmoil's virtual clock, so side two's Hlc is
    // strictly later.
    std::thread::sleep(Duration::from_millis(5));
    block_on(shard_of(&nodes, side2[0]).insert(shared_key, "side2".to_string())).expect("insert");
    block_on(shard_of(&nodes, side1[0]).insert(111u32, "only-side1".to_string())).expect("insert");
    block_on(shard_of(&nodes, side2[0]).insert(222u32, "only-side2".to_string())).expect("insert");

    run_until(&mut sim, steps_for(Duration::from_secs(5)), || {
        value_of(shard_of(&nodes, side1[1]), shared_key).is_some()
            && value_of(shard_of(&nodes, side2[1]), shared_key).is_some()
            && value_of(shard_of(&nodes, side1[1]), 111).is_some()
            && value_of(shard_of(&nodes, side2[1]), 222).is_some()
    })
    .expect("each side replicates its own writes to its other member before healing");

    for &a in &side1 {
        for &b in &side2 {
            let host_a = nodes.iter().find(|n| n.node == a).unwrap().host;
            let host_b = nodes.iter().find(|n| n.node == b).unwrap().host;
            sim.repair(host_a, host_b);
        }
    }
    republish_all(&nodes, &node_ids, k);

    // Every node that currently owns a given key's bucket holds the
    // correct, fully reconciled value for it; a node that does not own it
    // is free to have already released it (residency's disown-grace is
    // shorter than this budget) but must not still be holding the losing
    // side's stale value while it waits to either self-correct or release.
    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        nodes.iter().all(|n| {
            let view =
                ShardOps::ownership_view(n.shard.as_ref()).expect("distributed shard has a view");
            value_of(&n.shard, shared_key).as_deref() != Some("side1")
                && (!view.owns(part_of_u32(shared_key))
                    || value_of(&n.shard, shared_key).as_deref() == Some("side2"))
                && (!view.owns(part_of_u32(111))
                    || value_of(&n.shard, 111).as_deref() == Some("only-side1"))
                && (!view.owns(part_of_u32(222))
                    || value_of(&n.shard, 222).as_deref() == Some("only-side2"))
        })
    })
    .expect("reconciliation lands the higher-version write and both private writes on every owner");

    let hashes: HashSet<u64> = nodes
        .iter()
        .map(|n| {
            ShardOps::ownership_view_hash(n.shard.as_ref())
                .expect("distributed shard has a view hash")
        })
        .collect();
    assert_eq!(hashes.len(), 1, "every node's view_hash agrees once healed");

    for n in &nodes {
        if let Some(value) = value_of(&n.shard, shared_key) {
            assert_eq!(
                value, "side2",
                "the conflicting write survives by version: the strictly later write wins"
            );
        }
    }
}

/// With `owners=2`, kills one of a chosen bucket's two owners. The
/// surviving owner keeps answering for every one of the bucket's keys
/// throughout the outage, checked repeatedly while the cluster
/// rebalances, not only once at the end, and once the vacated slot's new
/// third owner has pulled the bucket via the anti-entropy self-healing
/// backstop, its content for those keys matches the survivor's exactly.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario's full setup, kill, and rebalance-matching checks read best kept together"
)]
fn distributed_owner_loss_k_two_loses_nothing() {
    const OWNERS: u8 = 2;
    let k = NonZeroU8::new(OWNERS).expect("nonzero");
    let port = 5300;
    let roster: Vec<(NodeId, &'static str, u16)> = vec![
        (NodeId::from(3001), "loss-node-a", port),
        (NodeId::from(3002), "loss-node-b", port),
        (NodeId::from(3003), "loss-node-c", port),
        (NodeId::from(3004), "loss-node-d", port),
    ];
    let node_ids: Vec<NodeId> = roster.iter().map(|&(id, _, _)| id).collect();
    let target_part = PartId::from_raw(0);

    let initial_owners = OwnershipView::compute(node_ids[0], node_ids.clone(), k)
        .owners_of(target_part)
        .to_vec();
    assert_eq!(
        initial_owners.len(),
        2,
        "k=2 over four eligible nodes always gives two owners"
    );
    let leaving = initial_owners[0];
    let staying = initial_owners[1];

    let bucket_keys: Vec<u32> = (0..5_000_000)
        .filter(|&key| part_of_u32(key) == target_part)
        .take(5)
        .collect();
    assert!(
        !bucket_keys.is_empty(),
        "test setup sanity: some key maps to the target bucket"
    );

    let nodes: Vec<DistNode> = roster
        .iter()
        .map(|&(id, host, port)| new_dist_node(id, host, port, OWNERS))
        .collect();
    republish_all(&nodes, &node_ids, k);

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x1055_3001))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(30))
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
            disown_grace: Duration::from_millis(250),
            ops_issued: None,
        }
    });

    for &key in &bucket_keys {
        block_on(shard_of(&nodes, leaving).insert(key, format!("v:{key}"))).expect("insert");
    }
    run_until(&mut sim, steps_for(Duration::from_secs(5)), || {
        bucket_keys
            .iter()
            .all(|&key| value_of(shard_of(&nodes, staying), key).is_some())
    })
    .expect("the surviving owner has every key before the kill");

    sim.crash(nodes.iter().find(|n| n.node == leaving).unwrap().host);
    let live: Vec<NodeId> = node_ids.iter().copied().filter(|&n| n != leaving).collect();
    republish_all(&nodes, &live, k);

    let new_owners = OwnershipView::compute(staying, live, k)
        .owners_of(target_part)
        .to_vec();
    assert!(
        new_owners.contains(&staying),
        "the surviving original owner keeps the bucket"
    );
    assert!(
        !new_owners.contains(&leaving),
        "the killed node is not an eligible owner"
    );
    let new_third = *new_owners
        .iter()
        .find(|&&n| n != staying)
        .expect("k=2 gives a second owner");

    for _ in 0..5 {
        run_steps(&mut sim, steps_for(Duration::from_millis(200)));
        for &key in &bucket_keys {
            assert!(
                value_of(shard_of(&nodes, staying), key).is_some(),
                "the surviving owner still answers for key {key} while rebalancing"
            );
        }
    }

    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        bucket_keys.iter().all(|&key| {
            value_of(shard_of(&nodes, new_third), key) == value_of(shard_of(&nodes, staying), key)
        })
    })
    .expect("the new owner's content matches the survivor's within the budget");

    for &key in &bucket_keys {
        assert_eq!(
            value_of(shard_of(&nodes, new_third), key),
            value_of(shard_of(&nodes, staying), key),
            "the new owner's content matches the survivor's exactly for key {key}"
        );
    }
}
