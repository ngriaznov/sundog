//! Stage two of the sweep by cache mode: a `Replicated` cache folds a retired
//! writer into its bounded total, an `Invalidation` cache keeps the retired
//! entry.

use super::*;

async fn open_counters_in(cluster: &Cluster, name: &str, mode: Mode) -> Cache<u32, PnCounter> {
    cluster
        .cache::<u32, PnCounter>(name)
        .mode(mode)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("open succeeds")
}

/// Two caches in one cluster see the same dead writer and the same quiet
/// membership; only the `Replicated` one folds it, and the `Invalidation`
/// one keeps the retired entry past twice the bound.
#[tokio::test]
async fn crdt_compact_tick_folds_a_replicated_cache_and_keeps_an_invalidation_cache_unfolded() {
    let retire_after = Duration::from_millis(100);
    let cluster = Cluster::builder("cluster-it-crdt-stage-two-by-mode")
        .seeds(std::iter::empty())
        .config(crdt_loopback_config(retire_after))
        .build()
        .await
        .expect("build succeeds");
    let dead_writer = writer(999, 1);
    let unseen = UnseenWriters::default();

    let mut caches = Vec::new();
    for (name, mode) in [
        ("replicated", Mode::Replicated),
        ("invalidating", Mode::Invalidation),
    ] {
        let cache = open_counters_in(&cluster, name, mode).await;
        cache
            .insert(1, PnCounter::local_delta(dead_writer, 5))
            .await
            .expect("seed write from the dead writer");
        caches.push((SmolStr::new(name), cache));
    }
    // The writer's node leaves just before the wait, so the tracker still
    // remembers it (it forgets a member gone for the receipt lifetime).
    let absence = absence::AbsenceTracker::default();
    absence.observe(&HashMap::from([(
        dead_writer.node(),
        membership::LiveFlags { departing: false },
    )]));
    absence.observe(&HashMap::new());
    tokio::time::sleep(retire_after + Duration::from_millis(50)).await;

    // Stage one retires the dead writer in both.
    for (name, _) in &caches {
        let shard = registered_shard(&cluster, name);
        let (retired, compacted) = crdt_compact_tick(
            shard.as_ref(),
            name,
            &cluster,
            &absence,
            &unseen,
            three_bounds(retire_after),
        )
        .await;
        assert_eq!(retired, vec![dead_writer], "{name}");
        assert_eq!(compacted, 1, "{name}");
    }

    // Past twice the bound, with no other member and no churn.
    tokio::time::sleep(retire_after * 2 + Duration::from_millis(50)).await;
    for (name, cache) in &caches {
        let shard = registered_shard(&cluster, name);
        let (_, compacted) = crdt_compact_tick(
            shard.as_ref(),
            name,
            &cluster,
            &absence,
            &unseen,
            three_bounds(retire_after),
        )
        .await;
        let expected = usize::from(name == "replicated");
        assert_eq!(
            compacted, expected,
            "{name}: only the replicated cache folds the retired writer"
        );
        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(5),
            "{name} keeps the exact total either way"
        );
    }
    cluster.shutdown().await;
}
