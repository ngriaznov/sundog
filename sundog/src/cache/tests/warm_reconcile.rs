//! Tests of warm-reopen reconciliation: `reconcile_warm_buckets` against live co-owners.

use super::*;

/// A `Mode::Distributed` shard-and-context pair, `attach_ownership`'s
/// shape, for testing without a full `open()`.
fn distributed_context_for_test(
    cluster: &Cluster,
    name: &SmolStr,
) -> (Arc<dyn ShardOps>, DistributedContext) {
    let shard = Shard::<u32, String>::new(
        name.clone(),
        Mode::distributed(),
        cluster.node_id(),
        u64::MAX,
        None,
        None,
    );
    let (shard, distributed) = attach_ownership(shard, cluster, name, Mode::distributed());
    let distributed = distributed.expect("Mode::distributed always attaches a context");
    (Arc::new(shard), distributed)
}

#[tokio::test]
async fn reconcile_warm_buckets_leaves_a_bucket_cold_with_no_live_co_owner_to_verify_against() {
    let cluster = Cluster::builder("cache-it-reconcile-alone")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let name = SmolStr::new("scratch");
    let (shard_ops, distributed) = distributed_context_for_test(&cluster, &name);

    let warm_parts: Vec<PartId> = [0u16, 500, 1023]
        .into_iter()
        .flat_map(PartId::of_bucket)
        .collect();
    distributed.residency.mark_cold(&warm_parts);
    let reconciled = reconcile_warm_buckets(
        &cluster,
        &shard_ops,
        &name,
        &distributed.ownership,
        &distributed.residency,
        &warm_parts,
    )
    .await;

    assert!(
        reconciled.is_empty(),
        "a bucket with no live co-owner has nobody to verify its replayed state against, \
         so reconcile_warm_buckets itself leaves it cold rather than vacuously treating it \
         as reconciled -- it falls through to PullRequest::run's own \"owned alone\" case \
         next, which is the site that actually decides a co-owner-less bucket is servable"
    );
    for part in warm_parts {
        assert!(
            distributed.residency.is_cold(part),
            "part {part} stays cold with nobody for reconcile_warm_buckets itself to \
             reconcile against, falling through to the ordinary cold-pull path"
        );
    }

    cluster.shutdown().await;
}

/// A donor still pulling a part itself declines every pull as cold, yet
/// can hold writes that reached it meanwhile. A joiner whose only donor
/// stays cold converges with it by anti-entropy before serving, rather
/// than answering a miss for a key the donor holds. `ae_interval` is an
/// hour, so no periodic round repairs it instead.
#[tokio::test]
async fn a_joiner_whose_only_donor_stays_cold_converges_with_it_before_serving() {
    const TOTAL: u32 = 50;
    let name = SmolStr::new("cold-donor");
    let config = ClusterConfig {
        ae_interval: Duration::from_hours(1),
        tombstone_ttl: Duration::from_hours(720),
        ..loopback_config()
    };

    let b = Cluster::builder("cache-it-cold-donor")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every part");
    for key in 0..TOTAL {
        cache_b
            .insert(key, format!("v{key}"))
            .await
            .expect("b owns every part while alone");
    }
    let c = Cluster::builder("cache-it-cold-donor")
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    // Marked once b sees c: while b owns every part alone, a republished
    // view marks what it owns alone servable.
    cache_b
        .shard
        .residency()
        .expect("b is distributed")
        .mark_cold(&PartId::all().collect::<Vec<_>>());
    let cache_c = c
        .cache::<u32, String>(name)
        .mode(Mode::distributed())
        .open()
        .await
        .expect("c opens, co-owning every part with b");

    wait_until(
        Duration::from_secs(10),
        "c's pull settles against its cold donor",
        async || cache_c.entry_count().await == u64::from(TOTAL),
    )
    .await;
    for key in 0..TOTAL {
        assert_eq!(
            cache_c.get(&key).await,
            Some(format!("v{key}")),
            "key {key} converged from the cold donor"
        );
    }

    cache_c.close().await;
    cache_b.close().await;
    c.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn reconcile_warm_buckets_never_runs_a_round_for_an_empty_bucket_list() {
    let cluster = Cluster::builder("cache-it-reconcile-empty")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("build succeeds");

    let name = SmolStr::new("scratch");
    let (shard_ops, distributed) = distributed_context_for_test(&cluster, &name);

    let reconciled = reconcile_warm_buckets(
        &cluster,
        &shard_ops,
        &name,
        &distributed.ownership,
        &distributed.residency,
        &[],
    )
    .await;

    assert!(
        reconciled.is_empty(),
        "an empty warm-bucket list, the non-warm-reopen case, never runs a round"
    );

    cluster.shutdown().await;
}

/// Pins the core reconciliation mechanism against a real live co-owner:
/// `c` starts cold-seeded, pulls `b`'s real data via a genuine
/// anti-entropy round, and only then clears cold via `mark_serving`.
#[tokio::test]
async fn reconcile_warm_buckets_clears_cold_once_a_real_round_against_a_live_co_owner_reconciles() {
    const TOTAL: u32 = 200;
    let name = SmolStr::new("reconcile-real-round");

    let b = Cluster::builder("cache-it-reconcile-real-round")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    for key in 0..TOTAL {
        cache_b
            .insert(key, format!("v{key}"))
            .await
            .expect("b owns every bucket while alone");
    }

    let c = Cluster::builder("cache-it-reconcile-real-round")
        .seeds([b.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    // Bypasses `open()`'s own advertise call: without it b never learns c
    // has `name` open, so every round against it answers `Stale`.
    c.advertise_cache_mode(&name, Mode::distributed());

    // c's shard for `name`, built directly rather than through a real `open()`.
    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and c eligible at owners=2, c owns every bucket"
    );
    distributed_c.residency.mark_cold(&warm_parts);

    // b's tracker catches up to c joining on its own refresh cadence,
    // answering every round `Stale` until then, so this polls rather
    // than assuming one round suffices.
    let mut reconciled: HashSet<PartId> = HashSet::new();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round against it reconciles",
        async || {
            reconciled = reconcile_warm_buckets(
                &c,
                &shard_ops_c,
                &name,
                &distributed_c.ownership,
                &distributed_c.residency,
                &warm_parts,
            )
            .await;
            reconciled.len() == warm_parts.len()
        },
    )
    .await;

    for &bucket in &warm_parts {
        assert!(
            !distributed_c.residency.is_cold(bucket),
            "bucket {bucket} clears cold once its digest matches b's in a round against it"
        );
    }
    let recovered: usize = shard_ops_c
        .entries_for_buckets(buckets_of(&warm_parts))
        .await
        .into_iter()
        .map(|(_, entries)| entries.len())
        .sum();
    assert_eq!(
        recovered, TOTAL as usize,
        "the round actually pulled b's data in, not merely declared the buckets reconciled"
    );

    c.shutdown().await;
    b.shutdown().await;
}

/// Pins [`reconcile_warm_buckets`]'s per-peer processing: three real
/// nodes so `c`'s warm buckets split across two live co-owners (`b` and
/// `d`), and one call converges every bucket regardless of which peer
/// it's grouped under.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end three-node scenario (settle b/c/d, split c's warm buckets \
              across b and d, reconcile, assert both peers' shares converge): splitting it \
              would only scatter state (b, c, d, shard_ops_c, distributed_c, warm_parts) \
              across helper signatures"
)]
async fn reconcile_warm_buckets_converges_buckets_split_across_two_live_co_owners() {
    use crate::cluster::test_support::registered_shard;

    let name = SmolStr::new("reconcile-two-peers");

    let b = Cluster::builder("cache-it-reconcile-two-peers")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    b.cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");

    let seed = b.local_gossip_addr();
    let c = Cluster::builder("cache-it-reconcile-two-peers")
        .seeds([seed])
        .config(loopback_config())
        .build()
        .await
        .expect("c builds");
    let d = Cluster::builder("cache-it-reconcile-two-peers")
        .seeds([seed])
        .config(loopback_config())
        .build()
        .await
        .expect("d builds");
    wait_for_peer_count(&b, 2).await;
    wait_for_peer_count(&c, 2).await;
    wait_for_peer_count(&d, 2).await;

    // Without this, neither b nor d learns c has `name` open, and every round answers Stale.
    c.advertise_cache_mode(&name, Mode::distributed());
    d.cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("d opens alone of data, owning its own share of buckets");

    let node_b = b.node_id();
    let node_d = d.node_id();

    // Until gossip carries d's mode to c, c sees only b as a co-owner.
    // Rebuilding the context on each retry (not just the round) is what
    // waits out that convergence instead of freezing an early view.
    let mut built: Option<SplitWarmContext> = None;
    wait_until(
        Duration::from_secs(10),
        "c's own gossiped view learns d also has `name` open, so c's warm buckets split \
         across both b and d rather than naming b the only co-owner",
        async || {
            let (shard_ops, distributed) = distributed_context_for_test(&c, &name);
            let warm_parts: Vec<PartId> = distributed.ownership.current().owned_parts().collect();
            let view = distributed.ownership.current();
            let with_b = warm_parts
                .iter()
                .copied()
                .find(|&bucket| view.owners_of(bucket).contains(&node_b));
            let with_d = warm_parts
                .iter()
                .copied()
                .find(|&bucket| view.owners_of(bucket).contains(&node_d));
            match (with_b, with_d) {
                (Some(part_with_b), Some(part_with_d)) => {
                    built = Some(SplitWarmContext {
                        shard_ops,
                        distributed,
                        warm_parts,
                        part_with_b,
                        part_with_d,
                    });
                    true
                }
                _ => false,
            }
        },
    )
    .await;
    let SplitWarmContext {
        shard_ops: shard_ops_c,
        distributed: distributed_c,
        warm_parts,
        part_with_b,
        part_with_d,
    } = built.expect("wait_until only returns once the closure itself reported ready");
    distributed_c.residency.mark_cold(&warm_parts);

    // One planted key per peer, applied straight to each donor's shard,
    // proves one call pulls data from both groups, not just whichever runs first.
    let key_from_b = key_for_part(part_with_b);
    let key_from_d = key_for_part(part_with_d);
    registered_shard(&b, &name)
        .apply_remote_batch(vec![test_record(key_from_b, "from-b", node_b)])
        .await;
    registered_shard(&d, &name)
        .apply_remote_batch(vec![test_record(key_from_d, "from-d", node_d)])
        .await;

    // b's and d's trackers catch up to c joining on their own refresh
    // cadence, the same real-time delay the single-peer test polls around.
    let mut reconciled: HashSet<PartId> = HashSet::new();
    wait_until(
        Duration::from_secs(10),
        "b's and d's views catch up to c joining, so both peers' rounds reconcile",
        async || {
            reconciled = reconcile_warm_buckets(
                &c,
                &shard_ops_c,
                &name,
                &distributed_c.ownership,
                &distributed_c.residency,
                &warm_parts,
            )
            .await;
            reconciled.len() == warm_parts.len()
        },
    )
    .await;

    for &bucket in &warm_parts {
        assert!(
            !distributed_c.residency.is_cold(bucket),
            "bucket {bucket} clears cold once its digest matches its live co-owner's, \
             whether that co-owner is b or d"
        );
    }
    assert_record_value(&shard_ops_c, key_from_b, "from-b").await;
    assert_record_value(&shard_ops_c, key_from_d, "from-d").await;

    c.shutdown().await;
    d.shutdown().await;
    b.shutdown().await;
}

/// Raw-shard test context, rebuilt on each `wait_until` retry, plus
/// which of `warm_parts` landed with each of the two live co-owners.
struct SplitWarmContext {
    shard_ops: Arc<dyn ShardOps>,
    distributed: DistributedContext,
    warm_parts: Vec<PartId>,
    part_with_b: PartId,
    part_with_d: PartId,
}

/// The first `u32` key (bounded scan) whose part is `part`, for
/// planting a record at a known part.
fn key_for_part(part: PartId) -> u32 {
    (0u32..10_000_000)
        .find(|key| PartId::of_key(&encode_key(key).expect("u32 key encodes")) == part)
        .expect("some u32 key among the first ten million maps to every part")
}

/// The distinct buckets `parts` fall in, for the bucket-keyed
/// [`ShardOps`] reads.
fn buckets_of(parts: &[PartId]) -> Vec<u16> {
    let mut buckets: Vec<u16> = parts.iter().map(|part| part.bucket()).collect();
    buckets.sort_unstable();
    buckets.dedup();
    buckets
}

/// Asserts `shard`'s live record for `key` decodes to `expected`.
async fn assert_record_value(shard: &Arc<dyn ShardOps>, key: u32, expected: &str) {
    let recs = shard
        .records_for(vec![encode_key(&key).expect("u32 encodes")])
        .await;
    assert_eq!(
        recs.len(),
        1,
        "key {key} must have landed on the reconciled shard"
    );
    let value: String = recs[0]
        .value
        .as_ref()
        .map(|bytes| postcard::from_bytes(bytes).expect("string decodes"))
        .expect("a live record, not a tombstone");
    assert_eq!(
        value, expected,
        "key {key} must carry the value its donor planted"
    );
}

/// Guards CLAUDE.md's "deleted or expired entries never resurrect":
/// a warm reopen's on-disk replay can carry a stale live copy of a key
/// this node itself deleted before going down. `c` stands in for such a
/// replay, seeded with a live record older than a tombstone `b` holds;
/// `reconcile_warm_buckets` must correct it to the tombstone before the
/// bucket clears cold.
#[tokio::test]
async fn reconcile_warm_buckets_corrects_a_stale_replayed_record_against_a_live_co_owners_tombstone()
 {
    let name = SmolStr::new("reconcile-resurrection-guard");
    let key = 7u32;
    // The default loopback config's short tombstone_ttl would race this
    // test's wait_until against a real GC sweep; a generous ttl avoids that.
    let config = ClusterConfig {
        tombstone_ttl: Duration::from_secs(300),
        ..loopback_config()
    };

    let b = Cluster::builder("cache-it-reconcile-resurrection")
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    cache_b
        .insert(key, "v1".to_string())
        .await
        .expect("b owns every bucket while alone");
    cache_b
        .remove(&key)
        .await
        .expect("b deletes the key, stamping a tombstone newer than the stale replay below");

    let c = Cluster::builder("cache-it-reconcile-resurrection")
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    c.advertise_cache_mode(&name, Mode::distributed());

    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and c eligible at owners=2, c owns every bucket"
    );

    // Stands in for what a warm reopen's replay would install: a stale
    // live copy of `key`, versioned older than b's tombstone above,
    // applied via the same versioned-apply entry point replay uses.
    let stale = WireRecord {
        key: encode_key(&key).expect("u32 encodes"),
        value: Some(bytes::Bytes::from(
            postcard::to_stdvec(&"stale-v1".to_string()).expect("string encodes"),
        )),
        ver: crate::hlc::Hlc {
            wall_ms: 1,
            logical: 0,
            node: c.node_id(),
        },
        expires_at_ms: None,
    };
    shard_ops_c.apply_remote_batch(vec![stale]).await;
    distributed_c.residency.mark_cold(&warm_parts);

    // b's tracker catches up to c joining on its own refresh cadence, as above.
    let mut reconciled: HashSet<PartId> = HashSet::new();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round against it reconciles",
        async || {
            reconciled = reconcile_warm_buckets(
                &c,
                &shard_ops_c,
                &name,
                &distributed_c.ownership,
                &distributed_c.residency,
                &warm_parts,
            )
            .await;
            reconciled.len() == warm_parts.len()
        },
    )
    .await;

    for &bucket in &warm_parts {
        assert!(
            !distributed_c.residency.is_cold(bucket),
            "bucket {bucket} clears cold once its digest matches b's in a round against it"
        );
    }

    let recs = shard_ops_c
        .records_for(vec![encode_key(&key).expect("u32 encodes")])
        .await;
    assert_eq!(
        recs.len(),
        1,
        "the reconciliation round must have applied b's tombstone for the key"
    );
    assert!(
        recs[0].is_tombstone(),
        "the stale replayed value this node itself deleted before going down must never be \
         served: the converge-before-serving loop against b, who holds the tombstone, \
         corrects it before the bucket ever clears cold"
    );

    c.shutdown().await;
    b.shutdown().await;
}

/// Pins that when the only co-owner is unreachable, every round fails
/// and the warm bucket stays cold, falling through to the ordinary
/// cold-pull path.
#[tokio::test]
async fn reconcile_warm_buckets_leaves_a_bucket_cold_when_its_only_co_owners_round_fails() {
    let name = SmolStr::new("reconcile-real-round-failed");

    let b = Cluster::builder("cache-it-reconcile-real-round-failed")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let _cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone");

    let c = Cluster::builder("cache-it-reconcile-real-round-failed")
        .seeds([b.local_gossip_addr()])
        // Tightened so the test bounds itself instead of waiting out loopback_config's 5s.
        .config(loopback_config().with(|config| {
            config.state_transfer_budget = Duration::from_secs(1);
        }))
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    // Without this, b never learns c has `name` open, and every round answers Stale.
    c.advertise_cache_mode(&name, Mode::distributed());

    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(!warm_parts.is_empty(), "{warm_parts:?}");
    distributed_c.residency.mark_cold(&warm_parts);

    // Warms up to a first converged round first, so the round below
    // fails on b's unreachability specifically, not an unsettled view.
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so a warm-up round against it reconciles",
        async || {
            !reconcile_warm_buckets(
                &c,
                &shard_ops_c,
                &name,
                &distributed_c.ownership,
                &distributed_c.residency,
                &warm_parts,
            )
            .await
            .is_empty()
        },
    )
    .await;
    distributed_c.residency.mark_cold(&warm_parts);

    // c's gossip-derived live list still names b live briefly, so the
    // round below attempts and fails on the dial, not a no-co-owner skip.
    b.shutdown().await;

    let started = Instant::now();
    let reconciled = reconcile_warm_buckets(
        &c,
        &shard_ops_c,
        &name,
        &distributed_c.ownership,
        &distributed_c.residency,
        &warm_parts,
    )
    .await;

    assert!(
        reconciled.is_empty(),
        "a round against an unreachable co-owner never reconciles any bucket"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the retries stop at the 1s time budget or when b leaves the live list, never \
         running on to loopback_config's 5s: took {:?}",
        started.elapsed()
    );
    for &bucket in &warm_parts {
        assert!(
            distributed_c.residency.is_cold(bucket),
            "bucket {bucket} stays cold once its only co-owner's round fails, falling \
             through to the ordinary cold-pull path rather than serving unverified"
        );
    }

    c.shutdown().await;
}

/// Every part of a bucket keys `0..total` never touch, so both sides'
/// empty digests match once b's view stops answering `Stale`, untouched
/// by real data.
fn unused_bucket(total: u32) -> Vec<PartId> {
    let used: HashSet<u16> = (0..total)
        .map(|key| bucket_of(&encode_key(&key).expect("u32 key encodes")))
        .collect();
    let bucket = (0..=u16::MAX)
        .take(crate::store::BUCKET_COUNT)
        .find(|bucket| !used.contains(bucket))
        .expect("BUCKET_COUNT buckets is far more than a small test's `total` can fill");
    PartId::of_bucket(bucket).collect()
}

/// Polls `probe_bucket` (an [`unused_bucket`]) until a round against it
/// stops answering `Stale`: b's tracker catching up to c joining. An
/// untouched bucket avoids repairing the real data the caller tests.
async fn wait_for_live_co_owner_view_to_settle(
    c: &Cluster,
    shard_ops_c: &Arc<dyn ShardOps>,
    name: &SmolStr,
    node_b: NodeId,
    probe_bucket: &[PartId],
) {
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so a round against it stops answering Stale",
        async || {
            !anti_entropy::run_round_for_parts(c.mesh(), shard_ops_c, name, node_b, probe_bucket)
                .await
                .failed
        },
    )
    .await;
}

/// A single `reconcile_warm_buckets` call must itself loop more than one
/// round to close a real gap: a round reports a bucket `still_diverged`
/// from the mismatch list before the repair lands, so only a later
/// round's fresh digest exchange reports it `matched`.
/// [`RECONCILE_MAX_ROUNDS`] gives the loop that confirming round.
///
/// `b`'s view is warmed up first against an untouched bucket, isolating
/// the two-rounds-needed behavior from b's own refresh-cadence delay.
#[tokio::test]
async fn reconcile_warm_buckets_loops_a_bounded_number_of_times_when_one_round_cannot_close_the_gap()
 {
    const TOTAL: u32 = 50;
    let name = SmolStr::new("reconcile-needs-two-rounds");

    let b = Cluster::builder("cache-it-reconcile-needs-two-rounds")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let node_b = b.node_id();
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    for key in 0..TOTAL {
        cache_b
            .insert(key, format!("v{key}"))
            .await
            .expect("b owns every bucket while alone");
    }
    let probe_bucket = unused_bucket(TOTAL);
    let divergent_parts: HashSet<PartId> = (0..TOTAL)
        .map(|key| PartId::of_key(&encode_key(&key).expect("u32 key encodes")))
        .collect();

    let c = Cluster::builder("cache-it-reconcile-needs-two-rounds")
        .seeds([b.local_gossip_addr()])
        .config(loopback_config())
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    c.advertise_cache_mode(&name, Mode::distributed());

    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and c eligible at owners=2, c owns every bucket"
    );
    distributed_c.residency.mark_cold(&warm_parts);

    wait_for_live_co_owner_view_to_settle(&c, &shard_ops_c, &name, node_b, &probe_bucket).await;

    // A single call: if it ran only one round, a repaired bucket would
    // still report `still_diverged`.
    let reconciled = reconcile_warm_buckets(
        &c,
        &shard_ops_c,
        &name,
        &distributed_c.ownership,
        &distributed_c.residency,
        &warm_parts,
    )
    .await;

    assert_eq!(
        reconciled.len(),
        warm_parts.len(),
        "a single call's own internal loop, not an external retry, closes the gap: round \
         one repairs the divergence but reports it still diverged, round two's fresh digest \
         exchange confirms the match"
    );
    for &bucket in &divergent_parts {
        assert!(
            !distributed_c.residency.is_cold(bucket),
            "bucket {bucket} clears cold once the loop's confirming round finds no mismatch"
        );
    }
    let recovered: usize = shard_ops_c
        .entries_for_buckets(buckets_of(&warm_parts))
        .await
        .into_iter()
        .map(|(_, entries)| entries.len())
        .sum();
    assert_eq!(
        recovered, TOTAL as usize,
        "the loop's first round actually pulled b's data in"
    );

    c.shutdown().await;
    b.shutdown().await;
}

/// The other side of the loop-needs-two-rounds test: a byte budget too
/// low for a second round makes [`should_keep_reconciling`] refuse it,
/// so a repaired-but-unconfirmed bucket stays cold and unverified rather
/// than clearing via `mark_serving` on an unconfirmed repair. Untouched
/// buckets in the same round still converge immediately.
#[tokio::test]
async fn reconcile_warm_buckets_stops_at_the_byte_budget_and_leaves_the_bucket_cold_and_unverified()
{
    const TOTAL: u32 = 50;
    let name = SmolStr::new("reconcile-byte-budget-stop");

    let b = Cluster::builder("cache-it-reconcile-byte-budget-stop")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let node_b = b.node_id();
    let cache_b = b
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");
    for key in 0..TOTAL {
        cache_b
            .insert(key, format!("v{key}"))
            .await
            .expect("b owns every bucket while alone");
    }
    let probe_bucket = unused_bucket(TOTAL);
    let divergent_parts: HashSet<PartId> = (0..TOTAL)
        .map(|key| PartId::of_key(&encode_key(&key).expect("u32 key encodes")))
        .collect();

    // A budget so small the first repair byte exceeds it: one round only.
    let config = ClusterConfig {
        reconcile_byte_budget: 1,
        ..loopback_config()
    };
    let c = Cluster::builder("cache-it-reconcile-byte-budget-stop")
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    c.advertise_cache_mode(&name, Mode::distributed());

    let (shard_ops_c, distributed_c) = distributed_context_for_test(&c, &name);
    let warm_parts: Vec<PartId> = distributed_c.ownership.current().owned_parts().collect();
    assert!(
        !warm_parts.is_empty(),
        "with only b and c eligible at owners=2, c owns every bucket"
    );
    distributed_c.residency.mark_cold(&warm_parts);

    wait_for_live_co_owner_view_to_settle(&c, &shard_ops_c, &name, node_b, &probe_bucket).await;

    let reconciled = reconcile_warm_buckets(
        &c,
        &shard_ops_c,
        &name,
        &distributed_c.ownership,
        &distributed_c.residency,
        &warm_parts,
    )
    .await;

    assert!(
        divergent_parts
            .iter()
            .all(|bucket| !reconciled.contains(bucket)),
        "the byte budget stops the loop after its one repairing round, before the \
         confirming round that would have reconciled the genuinely divergent buckets"
    );
    for &bucket in &divergent_parts {
        assert!(
            distributed_c.residency.is_cold(bucket),
            "bucket {bucket} stays cold: the budget cut the loop off before a confirming \
             round ever ran for it, so no ResidencySet call was ever made for it"
        );
    }
    let recovered: usize = shard_ops_c
        .entries_for_buckets(buckets_of(&warm_parts))
        .await
        .into_iter()
        .map(|(_, entries)| entries.len())
        .sum();
    assert_eq!(
        recovered, TOTAL as usize,
        "the one round the budget allowed still genuinely repaired the divergence -- the \
         bucket is left unverified, not un-repaired"
    );

    c.shutdown().await;
    b.shutdown().await;
}
