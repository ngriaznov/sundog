//! Tests for rounds scoped to parts, over real transport.

use std::time::Duration;

use super::*;

/// The ownership part a u32 test key hashes into.
fn part_of_u32(key: u32) -> PartId {
    PartId::of_key(&crate::store::encode_key(&key).expect("u32 key encodes"))
}

/// Two real, joined `Mode::Distributed` nodes co-owning every bucket,
/// for `run_round_for_parts`'s real-transport tests. Real-transport only.
async fn two_distributed_co_owners(
    test_id: &str,
    name: &SmolStr,
) -> (
    Cluster,
    Cluster,
    NodeId,
    Arc<dyn ShardOps>,
    Arc<dyn ShardOps>,
) {
    two_distributed_co_owners_resolving(test_id, name, None).await
}

/// [`two_distributed_co_owners`] with both nodes opening the cache under
/// `resolver` when one is given.
async fn two_distributed_co_owners_resolving(
    test_id: &str,
    name: &SmolStr,
    resolver: Option<Arc<dyn crate::store::ConflictResolver>>,
) -> (
    Cluster,
    Cluster,
    NodeId,
    Arc<dyn ShardOps>,
    Arc<dyn ShardOps>,
) {
    two_distributed_co_owners_configured(
        test_id,
        name,
        resolver,
        crate::cluster::test_support::loopback_config(),
    )
    .await
}

/// [`two_distributed_co_owners_resolving`] with both nodes built under
/// `config`.
async fn two_distributed_co_owners_configured(
    test_id: &str,
    name: &SmolStr,
    resolver: Option<Arc<dyn crate::store::ConflictResolver>>,
    config: crate::config::ClusterConfig,
) -> (
    Cluster,
    Cluster,
    NodeId,
    Arc<dyn ShardOps>,
    Arc<dyn ShardOps>,
) {
    use crate::cluster::test_support::{registered_shard, wait_for_peer_count};
    use crate::store::Mode;

    let open = |cluster: &Cluster| {
        let mut builder = cluster
            .cache::<u32, String>(name.clone())
            .mode(Mode::distributed());
        if let Some(resolver) = &resolver {
            builder = builder.resolver(Arc::clone(resolver));
        }
        builder.open()
    };
    let b = Cluster::builder(format!("cluster-it-{test_id}"))
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("b builds");
    let node_b = b.node_id();
    open(&b).await.expect("b opens alone, owning every bucket");

    let c = Cluster::builder(format!("cluster-it-{test_id}"))
        .seeds([b.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("c builds");
    wait_for_peer_count(&b, 1).await;
    wait_for_peer_count(&c, 1).await;
    open(&c)
        .await
        .expect("c opens, co-owning every bucket alongside b");

    let shard_b = registered_shard(&b, name);
    let shard_c = registered_shard(&c, name);
    (b, c, node_b, shard_b, shard_c)
}

/// Pins that `still_diverged` names only the requested bucket even
/// when a second, un-requested bucket also diverges, proving the
/// request is scoped to `buckets`.
#[tokio::test]
async fn run_round_for_parts_reports_only_the_requested_buckets_still_diverged() {
    use crate::cluster::test_support::wait_until;

    let name = SmolStr::new("scoped-round-diverged");
    let (b, c, node_b, shard_b, shard_c) =
        two_distributed_co_owners("scoped-diverged", &name).await;

    let keys = keys_in_distinct_buckets(2);
    let (requested_key, _) = keys[0];
    let (extra_key, _) = keys[1];
    let requested_part = part_of_u32(requested_key);
    let extra_part = part_of_u32(extra_key);

    let (_, requested_rec) = encode_test_record(requested_key, "requested", node_b);
    shard_b.apply_remote(requested_rec).await;
    let (_, extra_rec) = encode_test_record(extra_key, "extra", node_b);
    shard_b.apply_remote(extra_rec).await;

    let mut outcome = PartRoundOutcome::default();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round runs instead of reporting stale",
        async || {
            outcome =
                run_round_for_parts(c.mesh(), &shard_c, &name, node_b, &[requested_part]).await;
            !outcome.failed
        },
    )
    .await;

    assert_eq!(
        outcome.still_diverged,
        HashSet::from([requested_part]),
        "the requested bucket's real divergence is reported"
    );
    assert!(
        outcome.matched.is_empty(),
        "the only requested bucket diverged, so nothing is reported matched"
    );
    assert!(
        !outcome.still_diverged.contains(&extra_part) && !outcome.matched.contains(&extra_part),
        "the un-requested bucket's real divergence never surfaces in this scoped round's outcome"
    );

    b.shutdown().await;
    c.shutdown().await;
}

/// Pins the view-disagreement case: `c` requests one bucket its
/// fresh view says `b` does not co-own. That bucket is never sent
/// to `b`, so it must land in `still_diverged`, never `matched`.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end three-node scenario (settle b/c/d, find a bucket dropped from \
              the request, run and assert the round): splitting it would only scatter state \
              (b, c, d, node_b, shard_c, the split buckets) across helper signatures"
)]
async fn run_round_for_parts_never_reports_matched_for_a_bucket_the_peer_does_not_co_own() {
    use crate::cluster::test_support::{
        loopback_config, registered_shard, wait_for_peer_count, wait_until,
    };
    use crate::store::Mode;

    let name = SmolStr::new("scoped-round-dropped-co-owner");
    let b = Cluster::builder("cluster-it-scoped-dropped")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("b builds");
    let node_b = b.node_id();
    b.cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("b opens alone, owning every bucket");

    let seed = b.local_gossip_addr();
    let c = Cluster::builder("cluster-it-scoped-dropped")
        .seeds([seed])
        .config(loopback_config())
        .build()
        .await
        .expect("c builds");
    let d = Cluster::builder("cluster-it-scoped-dropped")
        .seeds([seed])
        .config(loopback_config())
        .build()
        .await
        .expect("d builds");
    wait_for_peer_count(&b, 2).await;
    wait_for_peer_count(&c, 2).await;
    wait_for_peer_count(&d, 2).await;
    let (cache_c, cache_d) = tokio::join!(
        c.cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open(),
        d.cache::<u32, String>(name.clone())
            .mode(Mode::distributed())
            .open(),
    );
    cache_c.expect("c opens");
    cache_d.expect("d opens");

    let shard_c = registered_shard(&c, &name);

    // c's ownership view catches up to d's cache mode on its own
    // cadence, so this polls rather than assuming the first view is settled.
    let mut split: Option<(PartId, PartId)> = None;
    wait_until(
        Duration::from_secs(10),
        "c's own view catches up to d joining, splitting c's owned buckets across both b \
         and d instead of naming b the only co-owner",
        async || {
            let Some(view) = shard_c.ownership_view() else {
                return false;
            };
            let mut shared_with_b = None;
            let mut dropped_by_b = None;
            for part in view.owned_parts() {
                if view.owners_of(part).contains(&node_b) {
                    shared_with_b.get_or_insert(part);
                } else {
                    dropped_by_b.get_or_insert(part);
                }
                if shared_with_b.is_some() && dropped_by_b.is_some() {
                    break;
                }
            }
            match (shared_with_b, dropped_by_b) {
                (Some(shared), Some(dropped)) => {
                    split = Some((shared, dropped));
                    true
                }
                _ => false,
            }
        },
    )
    .await;
    let (shared_with_b, dropped_by_b) =
        split.expect("wait_until only returns once the closure itself reported ready");

    // b's own OwnershipTracker catches up to c/d joining on its own
    // cadence, so this polls rather than assuming a shared view hash.
    let mut outcome = PartRoundOutcome::default();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c and d joining, so the round against it runs instead of \
         reporting stale",
        async || {
            outcome = run_round_for_parts(
                c.mesh(),
                &shard_c,
                &name,
                node_b,
                &[shared_with_b, dropped_by_b],
            )
            .await;
            !outcome.failed
        },
    )
    .await;
    assert!(
        outcome.still_diverged.contains(&dropped_by_b),
        "a bucket c's own fresh view says b does not co-own was never sent to b this \
         round, so its digest was never compared -- it must count as still diverged"
    );
    assert!(
        !outcome.matched.contains(&dropped_by_b),
        "a bucket dropped from the outbound request must never be reported matched"
    );
    assert!(
        outcome.matched.contains(&shared_with_b),
        "the bucket genuinely co-owned with b and untouched by either side still matches \
         normally"
    );

    b.shutdown().await;
    c.shutdown().await;
    d.shutdown().await;
}

/// Pins that an untouched bucket matches on the first round: no
/// divergence, no bytes moved.
#[tokio::test]
async fn run_round_for_parts_marks_a_bucket_matched_once_its_digest_exchange_reports_no_mismatch() {
    use crate::cluster::test_support::wait_until;

    let name = SmolStr::new("scoped-round-matched");
    let (b, c, node_b, _shard_b, shard_c) =
        two_distributed_co_owners("scoped-matched", &name).await;

    let matched_part = part_of_u32(keys_in_distinct_buckets(1)[0].0);

    let mut outcome = PartRoundOutcome::default();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round runs instead of reporting stale",
        async || {
            outcome = run_round_for_parts(c.mesh(), &shard_c, &name, node_b, &[matched_part]).await;
            !outcome.failed
        },
    )
    .await;

    assert_eq!(
        outcome.matched,
        HashSet::from([matched_part]),
        "an untouched bucket's digests already agree, so it matches at once"
    );
    assert!(outcome.still_diverged.is_empty());
    assert_eq!(
        outcome.bytes_moved, 0,
        "nothing needed repair, so nothing moved"
    );

    b.shutdown().await;
    c.shutdown().await;
}

/// Pins that a round scoped to parts pulls under the limit kept for the
/// peer, as every other round does: it starts from a limit of one key and
/// the quick answers on loopback double it.
#[tokio::test]
async fn run_round_for_parts_pulls_under_the_limit_kept_for_the_peer() {
    use crate::cluster::test_support::wait_until;

    let name = SmolStr::new("scoped-round-pull-limit");
    // Background anti-entropy is effectively off: it would pull the keys
    // between two polls and leave the explicit round nothing to pull.
    let mut config = crate::cluster::test_support::loopback_config();
    config.ae_interval = Duration::from_secs(3600);
    config.tombstone_ttl = config.bucket_release_window();
    let (b, c, node_b, shard_b, shard_c) =
        two_distributed_co_owners_configured("scoped-pull-limit", &name, None, config).await;

    let keys = keys_in_distinct_buckets(3);
    for &(key, _) in &keys {
        let (_, record) = encode_test_record(key, "held by b", node_b);
        shard_b.apply_remote(record).await;
    }
    let parts: Vec<PartId> = keys.iter().map(|&(key, _)| part_of_u32(key)).collect();

    let mut outcome = PartRoundOutcome::default();
    wait_until(
        Duration::from_secs(10),
        "b's view catches up to c joining, so the round runs instead of reporting stale",
        async || {
            c.mesh().set_pull_limit(node_b, &name, 1);
            outcome = run_round_for_parts(c.mesh(), &shard_c, &name, node_b, &parts).await;
            !outcome.failed
        },
    )
    .await;

    assert!(
        c.mesh()
            .pull_limit(node_b, &name)
            .is_some_and(|limit| limit > 1),
        "the pull started at one key and grew with its quick answers"
    );
    for &(key, _) in &keys {
        let key = Bytes::from(postcard::to_stdvec(&key).expect("key encodes"));
        assert_eq!(
            shard_c.records_for(vec![key]).await.len(),
            1,
            "the round pulled every record b held"
        );
    }

    b.shutdown().await;
    c.shutdown().await;
}

/// Pins that an unknown peer fails the round: everything requested
/// lands in `still_diverged`.
#[tokio::test]
async fn run_round_for_parts_treats_every_requested_bucket_as_still_diverged_when_the_peer_is_unreachable()
 {
    use crate::cluster::test_support::{loopback_config, registered_shard};
    use crate::store::Mode;

    let name = SmolStr::new("scoped-round-unreachable");
    let cluster = Cluster::builder("cluster-it-scoped-unreachable")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("cluster builds alone");
    cluster
        .cache::<u32, String>(name.clone())
        .mode(Mode::distributed())
        .open()
        .await
        .expect("opens alone, owning every bucket");

    let shard = registered_shard(&cluster, &name);
    let unknown_peer = NodeId::from(u64::MAX);
    let requested = [1u16, 2, 3].map(PartId::from_raw);

    let outcome =
        run_round_for_parts(cluster.mesh(), &shard, &name, unknown_peer, &requested).await;

    assert!(
        outcome.failed,
        "an unknown peer's digest exchange cannot succeed"
    );
    assert_eq!(
        outcome.still_diverged,
        requested.iter().copied().collect::<HashSet<PartId>>(),
        "every requested bucket is treated as still diverged when the round cannot run"
    );
    assert!(outcome.matched.is_empty());
    assert_eq!(outcome.bytes_moved, 0);

    cluster.shutdown().await;
}

/// Keeps the longer value: a deterministic antisymmetric order that does not
/// follow versions.
#[derive(Debug, Clone, Copy)]
struct LongerValueWins;

impl crate::store::ConflictResolver for LongerValueWins {
    fn winner(
        &self,
        _key: &[u8],
        a: crate::store::RecordView<'_>,
        b: crate::store::RecordView<'_>,
    ) -> crate::store::Winner {
        let len_a = a.value.map_or(0, <[u8]>::len);
        let len_b = b.value.map_or(0, <[u8]>::len);
        if len_b > len_a {
            crate::store::Winner::B
        } else {
            crate::store::Winner::A
        }
    }
}

/// Pins that a part-scoped round sends the resolver's winner when it is held
/// at the lower version, from either side: the pull a releasing owner drops
/// is not needed, because the holder pushes.
#[tokio::test]
async fn run_round_for_parts_exchanges_both_records_for_a_value_reading_resolver() {
    use crate::cluster::test_support::wait_until;

    for (label, winner_holder_initiates) in [("winner-holder", true), ("loser-holder", false)] {
        let name = SmolStr::new("scoped-round-winner");
        let (b, c, node_b, shard_b, shard_c) = two_distributed_co_owners_resolving(
            &format!("scoped-winner-{label}"),
            &name,
            Some(Arc::new(LongerValueWins)),
        )
        .await;
        let (key, _) = keys_in_distinct_buckets(1)[0];
        let part = part_of_u32(key);
        let node_c = c.node_id();

        // b holds the winner at the lower version, c the loser at the
        // higher one; neither saw the other's write.
        let (_, long) = encode_test_record(key, "the longer value", node_b);
        shard_b.apply_remote(long).await;
        let (_, mut short) = encode_test_record(key, "short", node_c);
        short.ver.wall_ms = 2;
        shard_c.apply_remote(short).await;

        wait_until(
            Duration::from_secs(10),
            "the round runs once both views agree, and leaves the replicas equal",
            async || {
                let outcome = if winner_holder_initiates {
                    run_round_for_parts(b.mesh(), &shard_b, &name, node_c, &[part]).await
                } else {
                    run_round_for_parts(c.mesh(), &shard_c, &name, node_b, &[part]).await
                };
                !outcome.failed && shard_b.digests().await == shard_c.digests().await
            },
        )
        .await;
        let key_bytes = crate::store::encode_key(&key).expect("u32 key encodes");
        for (side, shard) in [("b", &shard_b), ("c", &shard_c)] {
            let records = shard.records_for(vec![key_bytes.clone()]).await;
            assert_eq!(
                records.len(),
                1,
                "{label}: {side} holds the key after the round"
            );
            assert_eq!(
                records[0].value,
                Some(Bytes::from(
                    postcard::to_stdvec("the longer value").expect("value encodes")
                )),
                "{label}: {side} holds the resolver's winner"
            );
        }

        b.shutdown().await;
        c.shutdown().await;
    }
}
