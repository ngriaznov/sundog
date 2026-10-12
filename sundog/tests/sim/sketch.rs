//! The sketch and part reconciliation scenarios under loss.

use crate::pair::{NodeParams, node_loop};
use crate::support::{
    TICK, block_on, dense_bucket_keys, digests_of, new_shard, run_until, seed_shard, sim_seed,
    steps_for, value_of,
};
use futures::StreamExt as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use sundog::node::NodeId;
use sundog::store::ShardOps;
use turmoil::Builder;

/// Two shards start byte-identical across 4,096 keys, dense enough that a
/// forced `ae_sketch_min_bucket` of 4 puts several hundred entries in some
/// buckets; the responder answers a mismatch there with `AeMismatch::Sketch`
/// rather than a listing. One key is then dropped locally on node-b, as if
/// its `Replicate` never arrived, and the two nodes run `ae_round_with_sketch`
/// against each other on a timer under turmoil packet loss and reordering.
/// Recovery goes through the full sketch machinery: `sundog::Iblt` built and
/// peeled locally, `sundog::diff_decoded`'s classification, and
/// `Mesh::ae_pull_hashes` for the pull, with `Mesh::ae_entries` available as
/// the undecodable fallback though this scenario's single-key diff always
/// peels clean.
#[test]
fn sketch_reconciliation_under_loss_converges() {
    const N: u32 = 4096;
    const MIN_BUCKET: usize = 4;
    let node_a = NodeId::from(71);
    let node_b = NodeId::from(72);
    let port = 4900;

    let shard_a = Arc::new(seed_shard(node_a, 0..N));
    let shard_b = Arc::new(new_shard(node_b));
    block_on(async {
        let mut chunks = ShardOps::snapshot_chunks(shard_a.as_ref());
        while let Some(chunk) = chunks.next().await {
            ShardOps::apply_remote_batch(shard_b.as_ref(), chunk).await;
        }
    });
    assert_eq!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "both sides start converged before the drop"
    );

    // A bucket dense enough that MIN_BUCKET's threshold answers it as an
    // IBLT sketch instead of a full listing.
    let bucket_keys = dense_bucket_keys(N, MIN_BUCKET + 1);
    let target_key = bucket_keys[0];
    block_on(shard_b.invalidate_local(&target_key));
    assert_eq!(
        value_of(&shard_b, target_key),
        None,
        "node-b's copy is dropped, as if a Replicate never arrived"
    );
    assert_ne!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "test setup sanity: the drop makes one bucket mismatch"
    );

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x5CE7_C401))
        .tick_duration(TICK)
        // Same loss/reorder shape as the storm scenario: AE rounds routinely
        // fail and retry rather than never losing a packet.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    let ae_period = Duration::from_millis(150);
    let base_params = NodeParams {
        node: node_a,
        label: "a",
        port,
        peers: vec![(node_b, "sketch-b", port)],
        keys: vec![],
        // No writes in this scenario: the fixed dataset plus the one drop
        // is the whole story.
        write_period: Duration::from_secs(3600),
        ae_period,
        dup_factor: 1,
        ae_failures: None,
        remove_on_repeat: false,
        ops_issued: None,
        ae_sketch_min_bucket: Some(MIN_BUCKET),
        ae_part_min_bucket: None,
        bucket_listings: None,
    };

    let params_a = base_params.clone();
    let shard = Arc::clone(&shard_a);
    sim.host("sketch-a", move || {
        let shard = Arc::clone(&shard);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: node_b,
        label: "b",
        peers: vec![(node_a, "sketch-a", port)],
        ..base_params
    };
    let shard = Arc::clone(&shard_b);
    sim.host("sketch-b", move || {
        let shard = Arc::clone(&shard);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });

    let budget = steps_for(ae_period * 20 + Duration::from_secs(5));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "sketch-decoded anti-entropy converges within a bounded number of rounds despite loss"
    );
    assert_eq!(
        value_of(&shard_b, target_key),
        value_of(&shard_a, target_key),
        "the dropped key is repaired via the sketch decode / pull-by-hash path"
    );
}

/// Two shards start byte-identical across 4,096 keys, dense enough that a
/// forced `ae_part_min_bucket` of 2 puts several entries in some buckets;
/// the responder answers a mismatch there with `AeMismatch::PartDigests`
/// rather than a full listing or sketch. One key is then dropped locally on
/// node-b, and the two nodes run `ae_round_with_sketch` against each other
/// on a timer under turmoil packet loss and reordering. Recovery goes
/// through the part machinery: `ShardOps::part_digests` compared via
/// `sundog::mismatched_parts`, `Mesh::ae_parts` for the differing parts, and
/// `diff_part`/`ShardOps::entries_for_parts` for the pull, with the
/// `bucket_listings` counter proving no `AeMismatch::Bucket`/`Msg::AeBucket`
/// listing ever carried the repair.
#[test]
fn part_reconciliation_repairs_one_key_under_loss() {
    const N: u32 = 4096;
    const MIN_PART_BUCKET: usize = 2;
    let node_a = NodeId::from(81);
    let node_b = NodeId::from(82);
    let port = 5000;

    let shard_a = Arc::new(seed_shard(node_a, 0..N));
    let shard_b = Arc::new(new_shard(node_b));
    block_on(async {
        let mut chunks = ShardOps::snapshot_chunks(shard_a.as_ref());
        while let Some(chunk) = chunks.next().await {
            ShardOps::apply_remote_batch(shard_b.as_ref(), chunk).await;
        }
    });
    assert_eq!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "both sides start converged before the drop"
    );

    // A bucket dense enough that MIN_PART_BUCKET's threshold answers it with
    // part digests instead of a full listing or sketch.
    let bucket_keys = dense_bucket_keys(N, MIN_PART_BUCKET + 1);
    let target_key = bucket_keys[0];
    block_on(shard_b.invalidate_local(&target_key));
    assert_eq!(
        value_of(&shard_b, target_key),
        None,
        "node-b's copy is dropped, as if a Replicate never arrived"
    );
    assert_ne!(
        digests_of(&shard_a),
        digests_of(&shard_b),
        "test setup sanity: the drop makes one bucket mismatch"
    );

    let mut sim = Builder::new()
        .rng_seed(sim_seed(0x9A27_7501))
        .tick_duration(TICK)
        // Same loss/reorder shape as the storm scenario: AE rounds routinely
        // fail and retry rather than never losing a packet.
        .fail_rate(0.03)
        .repair_rate(0.75)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(60))
        .build();

    let ae_period = Duration::from_millis(150);
    let bucket_listings_a = Arc::new(AtomicUsize::new(0));
    let bucket_listings_b = Arc::new(AtomicUsize::new(0));
    let base_params = NodeParams {
        node: node_a,
        label: "a",
        port,
        peers: vec![(node_b, "part-b", port)],
        keys: vec![],
        // No writes in this scenario: the fixed dataset plus the one drop
        // is the whole story.
        write_period: Duration::from_secs(3600),
        ae_period,
        dup_factor: 1,
        ae_failures: None,
        remove_on_repeat: false,
        ops_issued: None,
        ae_sketch_min_bucket: None,
        ae_part_min_bucket: Some(MIN_PART_BUCKET),
        bucket_listings: Some(Arc::clone(&bucket_listings_a)),
    };

    let params_a = base_params.clone();
    let shard = Arc::clone(&shard_a);
    sim.host("part-a", move || {
        let shard = Arc::clone(&shard);
        let params = params_a.clone();
        async move { node_loop(params, shard).await }
    });

    let params_b = NodeParams {
        node: node_b,
        label: "b",
        peers: vec![(node_a, "part-a", port)],
        bucket_listings: Some(Arc::clone(&bucket_listings_b)),
        ..base_params
    };
    let shard = Arc::clone(&shard_b);
    sim.host("part-b", move || {
        let shard = Arc::clone(&shard);
        let params = params_b.clone();
        async move { node_loop(params, shard).await }
    });

    let budget = steps_for(ae_period * 20 + Duration::from_secs(5));
    let converged = run_until(&mut sim, budget, || {
        digests_of(&shard_a) == digests_of(&shard_b)
    });
    assert!(
        converged.is_some(),
        "part-digest anti-entropy converges within a bounded number of rounds despite loss"
    );
    assert_eq!(
        value_of(&shard_b, target_key),
        value_of(&shard_a, target_key),
        "the dropped key is repaired via the part-digest / part-listing path"
    );
    for (label, counter) in [("a", &bucket_listings_a), ("b", &bucket_listings_b)] {
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "node-{label}'s rounds never carried a full bucket listing"
        );
    }
}
