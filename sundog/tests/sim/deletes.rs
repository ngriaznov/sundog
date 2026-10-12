//! The delete scenarios: tombstone survival across a partition and tombstone deferral.

use crate::pair::{PairSpec, spawn_symmetric_pair};
use crate::support::{
    TICK, TestShard, block_on, cache_name, digests_of, key_bytes, run_steps, run_until, sim_seed,
    steps_for, value_of,
};
use std::sync::Arc;
use std::time::Duration;
use sundog::node::NodeId;
use sundog::store::{Mode, Shard, ShardOps};
use turmoil::Builder;

fn new_shard_with_tombstone_ttl(
    node: NodeId,
    tombstone_ttl: Duration,
    tombstone_max_ttl: Duration,
) -> TestShard {
    Shard::new(cache_name(), Mode::Replicated, node, 100_000, None, None)
        .with_tombstone_ttl(tombstone_ttl)
        .with_tombstone_max_ttl(tombstone_max_ttl)
}

/// Runs the scenario that motivates partition-aware tombstone retention: two
/// nodes converged on a key, a partition, the survivor deleting the key,
/// real time passing past `tombstone_ttl` while still partitioned, a heal,
/// then anti-entropy. Returns each side's value once digests converge, or
/// the budget runs out, plus whether they did.
///
/// `defer_while_absent` stands in for `should_defer_gc`'s decision, handed
/// straight to the real [`ShardOps::gc_tombstones`]. `true` defers
/// collecting node-b's tombstone while node-a stays absent; `false`
/// collects it unconditionally.
fn run_partition_delete_scenario(
    seed: u64,
    port: u16,
    defer_while_absent: bool,
) -> (Option<String>, Option<String>, bool) {
    let node_a = NodeId::from(u64::from(port) * 10 + 1);
    let node_b = NodeId::from(u64::from(port) * 10 + 2);
    let key = 555u32;
    let tombstone_ttl = Duration::from_millis(30);
    let tombstone_max_ttl = Duration::from_secs(60);

    let shard_a = Arc::new(new_shard_with_tombstone_ttl(
        node_a,
        tombstone_ttl,
        tombstone_max_ttl,
    ));
    let shard_b = Arc::new(new_shard_with_tombstone_ttl(
        node_b,
        tombstone_ttl,
        tombstone_max_ttl,
    ));

    block_on(shard_a.insert(key, "original".to_string())).expect("insert");
    let rec = block_on(ShardOps::records_for(
        shard_a.as_ref(),
        vec![key_bytes(key)],
    ))
    .into_iter()
    .next()
    .expect("the freshly inserted key has a record");
    block_on(ShardOps::apply_remote(shard_b.as_ref(), rec));
    assert_eq!(value_of(&shard_a, key), Some("original".to_string()));
    assert_eq!(value_of(&shard_b, key), Some("original".to_string()));
    assert_eq!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "both sides start converged"
    );

    let mut sim = Builder::new()
        .rng_seed(seed)
        .tick_duration(TICK)
        .max_message_latency(Duration::from_millis(20))
        .build();

    let ae_period = Duration::from_millis(100);
    spawn_symmetric_pair(
        &mut sim,
        PairSpec {
            host_a: "resurrect-a",
            host_b: "resurrect-b",
            node_a,
            node_b,
            port,
            keys_a: vec![],
            keys_b: vec![],
            // No automatic writes here; the delete below is applied directly.
            write_period: Duration::from_secs(3600),
            ae_period,
            dup_factor: 1,
        },
        Arc::clone(&shard_a),
        Arc::clone(&shard_b),
        None,
    );

    sim.partition("resurrect-a", "resurrect-b");
    run_steps(&mut sim, steps_for(Duration::from_millis(200)));

    block_on(shard_b.remove(&key)).expect("remove creates a tombstone");
    assert_eq!(
        value_of(&shard_b, key),
        None,
        "the survivor's own read reflects its delete immediately"
    );

    // Tombstone deadlines are stamped from real `SystemTime`, not turmoil's
    // virtual clock, so real time must pass past `tombstone_ttl`.
    std::thread::sleep(tombstone_ttl * 10);

    block_on(ShardOps::gc_tombstones(
        shard_b.as_ref(),
        defer_while_absent,
    ));

    sim.repair("resurrect-a", "resurrect-b");
    let budget = steps_for(ae_period * 10);
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    })
    .is_some();

    (value_of(&shard_a, key), value_of(&shard_b, key), converged)
}

/// Proves the semantic goal directly: a member absent past
/// `tombstone_ttl` must not resurrect a manually deleted entry on heal.
/// Deferral keeps node-b's tombstone alive until node-a is reachable again.
#[test]
fn partition_survivor_delete_does_not_resurrect_after_heal() {
    let (value_a, value_b, converged) =
        run_partition_delete_scenario(sim_seed(0x2E1E_7A01), 4400, true);
    assert!(
        converged,
        "digests converge within the AE-round budget after healing"
    );
    assert_eq!(
        value_a, None,
        "node-a does not resurrect the deleted key after heal + AE"
    );
    assert_eq!(value_b, None, "node-b keeps the key deleted");
}

/// The counter-case, proving deferral is load-bearing: the same scenario
/// with unconditional GC (`defer_while_absent: false`) lets node-b forget
/// the tombstone while node-a is still absent, so anti-entropy pulls the
/// stale value back once healed.
#[test]
fn tombstone_deferral_is_load_bearing_against_resurrection() {
    let (_, value_b_unconditional, converged_unconditional) =
        run_partition_delete_scenario(sim_seed(0x2E1E_7A02), 4410, false);
    assert!(
        converged_unconditional,
        "digests converge within the budget, onto the wrong, resurrected state"
    );
    assert_eq!(
        value_b_unconditional,
        Some("original".to_string()),
        "counter-case: collecting the tombstone unconditionally while node-a is still absent \
         lets anti-entropy resurrect the deleted key on node-b once healed"
    );

    let (deferred_value_a, deferred_value_b, converged_deferred) =
        run_partition_delete_scenario(sim_seed(0x2E1E_7A03), 4420, true);
    assert!(converged_deferred, "digests converge under deferral too");
    assert_eq!(
        deferred_value_a, None,
        "same scenario, deferred: node-a stays deleted"
    );
    assert_eq!(
        deferred_value_b, None,
        "same scenario, deferred: node-b stays deleted, closing exactly the gap \
         the unconditional case above demonstrated"
    );
}
