//! The convergence scenarios: partitions, loss, reordering, duplication, flapping, latency and churn.

use crate::pair::{NodeParams, PairSpec, node_loop, spawn_symmetric_pair};
use crate::support::{
    TICK, TestShard, digests_of, new_shard, run_steps, run_until, sim_seed, steps_for, value_of,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use sundog::node::NodeId;
use turmoil::Builder;

#[test]
fn partition_during_writes_converges_within_five_ae_rounds() {
    let node_a = NodeId::from(9201);
    let node_b = NodeId::from(9202);
    let ae_period = Duration::from_millis(250);
    let keys_a: Vec<u32> = (0..5).collect();
    let keys_b: Vec<u32> = (100..105).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xA11C_E001))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "node-a",
            host_b: "node-b",
            node_a,
            node_b,
            port: 4000,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(50),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    // Partition before the write burst, then heal and bound convergence.
    sim.partition("node-a", "node-b");
    run_steps(&mut sim, steps_for(Duration::from_millis(750)));
    sim.repair("node-a", "node-b");

    let budget = steps_for(ae_period * 5 + Duration::from_millis(500));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "digests converge within five AE-round intervals of healing"
    );

    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert!(
            value_of(&shard_a, key).is_some(),
            "node-a missing key {key}"
        );
        assert!(
            value_of(&shard_b, key).is_some(),
            "node-b missing key {key}"
        );
    }
    assert_eq!(value_of(&shard_a, 100).as_deref(), Some("b:100"));
    assert_eq!(value_of(&shard_b, 0).as_deref(), Some("a:0"));
}

struct StormStats {
    steps_to_converge: usize,
    ae_failures: usize,
    keys_present_a: usize,
    keys_present_b: usize,
}

/// Message loss, latency spread, and a duplicate storm running concurrently
/// with live writes; `fail_rate` alone breaks and heals links throughout.
fn run_storm_scenario(seed: u64) -> StormStats {
    let node_a = NodeId::from(11);
    let node_b = NodeId::from(12);
    let keys_a: Vec<u32> = (0..8).collect();
    let keys_b: Vec<u32> = (200..208).collect();
    let total_keys = keys_a.len() + keys_b.len();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));
    let ae_failures = Arc::new(AtomicUsize::new(0));

    let write_period = Duration::from_millis(30);
    let mut sim = Builder::new()
        .rng_seed(seed)
        .tick_duration(TICK)
        // Turmoil's `fail_rate` has no TCP-segment retransmission, so a
        // dropped chunk can break a whole in-flight connection; kept low
        // enough that AE rounds still routinely succeed within a few retries.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "node-a",
            host_b: "node-b",
            node_a,
            node_b,
            port: 4100,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period,
            ae_period: Duration::from_millis(150),
            dup_factor: 4,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        Some(Arc::clone(&ae_failures)),
    );

    // Both sides write concurrently with AE, so digest equality can hold
    // trivially early. Wait explicitly for every key to exist on its own
    // origin shard before treating digest equality as real convergence.
    let own_writes_issued =
        |shard: &TestShard, keys: &[u32]| keys.iter().all(|&key| value_of(shard, key).is_some());
    run_until(&mut sim, steps_for(Duration::from_secs(10)), || {
        own_writes_issued(&shard_a, &keys_a) && own_writes_issued(&shard_b, &keys_b)
    })
    .expect("both sides finish issuing their own writes within the budget");

    let budget = steps_for(Duration::from_secs(20));
    let steps_to_converge = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    })
    .expect("anti-entropy converges despite loss/reorder/duplication within the budget");

    let present = |shard: &TestShard| {
        keys_a
            .iter()
            .chain(keys_b.iter())
            .filter(|&&key| value_of(shard, key).is_some())
            .count()
    };
    let keys_present_a = present(&shard_a);
    let keys_present_b = present(&shard_b);
    assert_eq!(
        keys_present_a, total_keys,
        "node-a holds every key once converged"
    );
    assert_eq!(
        keys_present_b, total_keys,
        "node-b holds every key once converged"
    );

    StormStats {
        steps_to_converge,
        ae_failures: ae_failures.load(Ordering::Relaxed),
        keys_present_a,
        keys_present_b,
    }
}

#[test]
fn loss_reorder_duplicate_storm_still_converges() {
    run_storm_scenario(sim_seed(0x5707_2201));
}

#[test]
fn simulation_is_reproducible_for_a_fixed_seed() {
    // Digest values embed each write's `Hlc`, stamped from real wall-clock
    // time, so they are not reproducible across runs. What is a pure
    // function of the seed is turmoil's own network schedule.
    let run1 = run_storm_scenario(0x5EED_0042);
    let run2 = run_storm_scenario(0x5EED_0042);

    assert_eq!(
        run1.steps_to_converge, run2.steps_to_converge,
        "the same seed converges at the same simulated step"
    );
    assert_eq!(
        run1.ae_failures, run2.ae_failures,
        "the same seed reproduces the same count of failed AE rounds"
    );
    assert_eq!(run1.keys_present_a, run2.keys_present_a);
    assert_eq!(run1.keys_present_b, run2.keys_present_b);
}

/// A link that flaps: six partition/heal cycles in quick succession, each
/// shorter than an AE interval, with both sides writing throughout. Once
/// flapping stops, convergence completes within the usual five-round bound.
#[test]
fn link_flapping_under_writes_converges_after_final_heal() {
    let node_a = NodeId::from(31);
    let node_b = NodeId::from(32);
    let ae_period = Duration::from_millis(250);
    let keys_a: Vec<u32> = (0..12).collect();
    let keys_b: Vec<u32> = (300..312).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xF1A9_9001))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "flap-a",
            host_b: "flap-b",
            node_a,
            node_b,
            port: 4500,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(40),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    // 6 x (300ms down + 200ms up); both write sequences finish mid-flap.
    for _ in 0..6 {
        sim.partition("flap-a", "flap-b");
        run_steps(&mut sim, steps_for(Duration::from_millis(300)));
        sim.repair("flap-a", "flap-b");
        run_steps(&mut sim, steps_for(Duration::from_millis(200)));
    }

    let budget = steps_for(ae_period * 5 + Duration::from_millis(500));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "digests converge within five AE-round intervals of the final heal"
    );
    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert!(
            value_of(&shard_a, key).is_some() && value_of(&shard_b, key).is_some(),
            "both sides hold key {key} after the flapping stops"
        );
    }
}

/// An asymmetric fault: `partition_oneway` drops everything node-a sends to
/// node-b while node-b's path to node-a keeps delivering. The healthy
/// direction must keep replicating during the fault, and the broken
/// direction's backlog must repair once the link heals.
#[test]
fn one_way_partition_delivers_the_healthy_direction_and_heals() {
    let node_a = NodeId::from(41);
    let node_b = NodeId::from(42);
    let ae_period = Duration::from_millis(200);
    let keys_a: Vec<u32> = (0..8).collect();
    let keys_b: Vec<u32> = (400..408).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x0E1A_A701))
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "oneway-a",
            host_b: "oneway-b",
            node_a,
            node_b,
            port: 4600,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(100),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    // Let connections establish and a few writes cross, then break the
    // a-to-b direction only; write sequences keep issuing past this point.
    run_steps(&mut sim, steps_for(Duration::from_millis(300)));
    sim.partition_oneway("oneway-a", "oneway-b");

    // The healthy direction keeps working regardless of the fault.
    let fault_budget = steps_for(Duration::from_secs(10));
    run_until(&mut sim, fault_budget, || {
        keys_b.iter().all(|&key| value_of(&shard_a, key).is_some())
    })
    .expect("node-b's writes keep replicating to node-a during the one-way fault");

    // The broken direction stays broken: the two sides still disagree.
    assert_ne!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "node-b is missing node-a's post-fault writes while a→b is down"
    );

    sim.repair_oneway("oneway-a", "oneway-b");
    let budget = steps_for(ae_period * 10 + Duration::from_millis(500));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "digests converge within ten AE-round intervals of repairing a→b"
    );
    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert!(
            value_of(&shard_a, key).is_some() && value_of(&shard_b, key).is_some(),
            "both sides hold key {key} after the one-way fault heals"
        );
    }
}

/// A permanently slow link, an order of magnitude above the other
/// scenarios, with live writes on both sides. Nothing is lost, only late:
/// replication and anti-entropy must still converge within a bounded budget.
#[test]
fn sustained_high_latency_still_converges() {
    let node_a = NodeId::from(51);
    let node_b = NodeId::from(52);
    let ae_period = Duration::from_millis(400);
    let keys_a: Vec<u32> = (0..8).collect();
    let keys_b: Vec<u32> = (500..508).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x51_0111))
        .tick_duration(TICK)
        .min_message_latency(Duration::from_millis(50))
        .max_message_latency(Duration::from_millis(150))
        .build();

    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "slow-a",
            host_b: "slow-b",
            node_a,
            node_b,
            port: 4700,
            keys_a: keys_a.clone(),
            keys_b: keys_b.clone(),
            write_period: Duration::from_millis(50),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    let budget = steps_for(Duration::from_secs(30));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
            && keys_a
                .iter()
                .chain(keys_b.iter())
                .all(|&key| value_of(&shard_a, key).is_some())
    });
    assert!(
        converged.is_some(),
        "a slow-but-lossless link still converges within the budget"
    );
    for &key in keys_a.iter().chain(keys_b.iter()) {
        assert_eq!(
            value_of(&shard_a, key),
            value_of(&shard_b, key),
            "both sides agree on key {key} under sustained high latency"
        );
    }
}

/// High-frequency entry lifecycle under loss: both nodes run overlapping
/// insert-then-remove schedules over a shared key range on a lossy,
/// reordering link. Every even key ends removed, every odd key's last
/// operation is an insert, so the converged state must be the correct one.
#[test]
fn add_remove_churn_under_loss_converges_to_the_correct_state() {
    let node_a = NodeId::from(61);
    let node_b = NodeId::from(62);
    let port = 4800;
    // First pass inserts the range, second pass removes its even keys.
    let plan_a: Vec<u32> = (0..16).chain((0..16).step_by(2)).collect();
    let plan_b: Vec<u32> = (8..24).chain((8..24).step_by(2)).collect();

    let shard_a = Arc::new(new_shard(node_a));
    let shard_b = Arc::new(new_shard(node_b));
    let ops_a = Arc::new(AtomicUsize::new(0));
    let ops_b = Arc::new(AtomicUsize::new(0));

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0xC4B4_A901))
        .tick_duration(TICK)
        // Same loss rate as the storm scenario: AE rounds routinely fail
        // and retry rather than packets occasionally vanishing.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    let params_a = NodeParams {
        node: node_a,
        label: "a",
        port,
        peers: vec![(node_b, "churn-b", port)],
        keys: plan_a.clone(),
        write_period: Duration::from_millis(25),
        ae_period: Duration::from_millis(150),
        dup_factor: 2,
        ae_failures: None,
        remove_on_repeat: true,
        ops_issued: Some(Arc::clone(&ops_a)),
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: None,
        bucket_listings: None,
    };
    let shard = Arc::clone(&shard_a);
    sim.host("churn-a", move || {
        let shard = Arc::clone(&shard);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: node_b,
        label: "b",
        port,
        peers: vec![(node_a, "churn-a", port)],
        keys: plan_b.clone(),
        write_period: Duration::from_millis(25),
        ae_period: Duration::from_millis(150),
        dup_factor: 2,
        ae_failures: None,
        remove_on_repeat: true,
        ops_issued: Some(Arc::clone(&ops_b)),
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: None,
        bucket_listings: None,
    };
    let shard = Arc::clone(&shard_b);
    sim.host("churn-b", move || {
        let shard = Arc::clone(&shard);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });

    run_until(&mut sim, steps_for(Duration::from_secs(15)), || {
        ops_a.load(Ordering::Relaxed) >= plan_a.len()
            && ops_b.load(Ordering::Relaxed) >= plan_b.len()
    })
    .expect("both churn sequences finish issuing within the budget");

    run_until(&mut sim, steps_for(Duration::from_secs(20)), || {
        digests_of(&shard_a) == digests_of(&shard_b)
    })
    .expect("churned shards converge despite loss within the budget");

    for key in (0..24u32).step_by(2) {
        assert_eq!(
            value_of(&shard_a, key),
            None,
            "removed key {key} stays removed on node-a"
        );
        assert_eq!(
            value_of(&shard_b, key),
            None,
            "removed key {key} stays removed on node-b"
        );
    }
    for key in (1..24u32).step_by(2) {
        let (on_a, on_b) = (value_of(&shard_a, key), value_of(&shard_b, key));
        assert!(
            on_a.is_some(),
            "surviving key {key} is present once converged"
        );
        assert_eq!(on_a, on_b, "both sides agree on surviving key {key}");
    }
}
