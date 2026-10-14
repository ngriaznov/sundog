//! The sweep's handling of a writer whose node this process never observed:
//! retired once the cluster has run for `crdt_retire_after` and a bound has
//! passed since the sweep first met the writer.

use super::*;

/// A cluster younger than `crdt_retire_after` retires nothing it never
/// observed: it cannot yet tell a writer gone for the bound from one it
/// simply has not met.
#[tokio::test]
async fn crdt_compact_tick_keeps_an_unobserved_writer_while_the_cluster_is_younger_than_retire_after()
 {
    let retire_after = Duration::from_secs(3600);
    let cluster = Cluster::builder("cluster-it-crdt-unobserved-young")
        .seeds(std::iter::empty())
        .config(crdt_loopback_config(retire_after))
        .build()
        .await
        .expect("build succeeds");
    let name = SmolStr::new("counters");
    let cache = open_counters_cache(&cluster, name.as_str()).await;
    let shard = registered_shard(&cluster, &name);
    let local = crdt::WriterId::new(cluster.node_id(), cluster.local_incarnation());
    cache
        .insert(1, PnCounter::local_delta(writer(999, 1), 5))
        .await
        .expect("seed write from a writer this node never observed");
    cache
        .insert(1, PnCounter::local_delta(local, 2))
        .await
        .expect("seed write from this node's own writer");
    let absence = absence::AbsenceTracker::default();
    let unseen = UnseenWriters::default();

    for _ in 0..2 {
        let (retired, compacted) = crdt_compact_tick(
            shard.as_ref(),
            &name,
            &cluster,
            &absence,
            &unseen,
            three_bounds(retire_after),
        )
        .await;
        assert!(retired.is_empty(), "retired {retired:?}");
        assert_eq!(compacted, 0, "nothing was rewritten");
    }
    assert_eq!(cache.get(&1).await.map(|c| c.value()), Some(7));
    cluster.shutdown().await;
}

/// An unobserved writer retires a whole bound after the sweep first met
/// it, on a cluster that has already run for longer than the bound: one
/// tick never retires a node that gossip may simply not have shown yet.
#[tokio::test]
async fn crdt_compact_tick_retires_an_unobserved_writer_a_bound_after_it_first_met_it() {
    let retire_after = Duration::from_millis(150);
    let cluster = Cluster::builder("cluster-it-crdt-unobserved-first-sight")
        .seeds(std::iter::empty())
        .config(crdt_loopback_config(retire_after))
        .build()
        .await
        .expect("build succeeds");
    let name = SmolStr::new("counters");
    let cache = open_counters_cache(&cluster, name.as_str()).await;
    let shard = registered_shard(&cluster, &name);
    let unknown = writer(999, 1);
    cache
        .insert(1, PnCounter::local_delta(unknown, 5))
        .await
        .expect("seed write from a writer this node never observed");
    let absence = absence::AbsenceTracker::default();
    let unseen = UnseenWriters::default();
    // The cluster is now older than the bound.
    tokio::time::sleep(retire_after + Duration::from_millis(50)).await;

    let (retired, compacted) = crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert!(
        retired.is_empty(),
        "the first sight of the writer retires nothing, got {retired:?}"
    );
    assert_eq!(compacted, 0);
    assert_eq!(unseen.len(), 1, "the sweep remembers the first sight");

    tokio::time::sleep(retire_after + Duration::from_millis(50)).await;
    let (retired, compacted) = crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert_eq!(retired, vec![unknown], "a bound after the first sight");
    assert_eq!(compacted, 1);
    assert_eq!(
        cache.get(&1).await.map(|c| c.value()),
        Some(5),
        "retiring the writer never changes the exact value"
    );

    // The retired slot is no longer a live slot, so the next tick does
    // not meet the writer and the sweep forgets it.
    crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert_eq!(unseen.len(), 0, "a retired writer's first sight is dropped");
    cluster.shutdown().await;
}

/// A tracked gone time outranks the cluster's start: a node watched
/// leaving a moment ago retires a bound after it left, not at once
/// because the cluster is old.
#[tokio::test]
async fn crdt_compact_tick_uses_the_tracked_gone_time_over_the_cluster_start() {
    let retire_after = Duration::from_millis(300);
    let cluster = Cluster::builder("cluster-it-crdt-unobserved-tracked")
        .seeds(std::iter::empty())
        .config(crdt_loopback_config(retire_after))
        .build()
        .await
        .expect("build succeeds");
    let name = SmolStr::new("counters");
    let cache = open_counters_cache(&cluster, name.as_str()).await;
    let shard = registered_shard(&cluster, &name);
    let leaver = writer(999, 1);
    cache
        .insert(1, PnCounter::local_delta(leaver, 5))
        .await
        .expect("seed write");
    tokio::time::sleep(retire_after + Duration::from_millis(50)).await;
    let absence = absence::AbsenceTracker::default();
    absence.observe(&HashMap::from([(
        leaver.node(),
        membership::LiveFlags { departing: false },
    )]));
    absence.observe(&HashMap::new());
    let unseen = UnseenWriters::default();

    let (retired, _) = crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert!(
        retired.is_empty(),
        "gone for a moment, in a cluster older than the bound: got {retired:?}"
    );
    assert_eq!(
        unseen.len(),
        0,
        "a tracked writer is never recorded as unseen"
    );

    tokio::time::sleep(retire_after + Duration::from_millis(50)).await;
    let (retired, _) = crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert_eq!(retired, vec![leaver], "gone for the bound");
    cluster.shutdown().await;
}

/// A writer first met as unknown that membership then tracks is judged
/// by its tracked time from then on, and its first sight is forgotten.
#[tokio::test]
async fn crdt_compact_tick_drops_the_first_sight_of_a_writer_membership_then_tracks() {
    let retire_after = Duration::from_secs(3600);
    let cluster = Cluster::builder("cluster-it-crdt-unobserved-then-tracked")
        .seeds(std::iter::empty())
        .config(crdt_loopback_config(retire_after))
        .build()
        .await
        .expect("build succeeds");
    let name = SmolStr::new("counters");
    let cache = open_counters_cache(&cluster, name.as_str()).await;
    let shard = registered_shard(&cluster, &name);
    let unknown = writer(999, 1);
    cache
        .insert(1, PnCounter::local_delta(unknown, 5))
        .await
        .expect("seed write");
    let absence = absence::AbsenceTracker::default();
    let unseen = UnseenWriters::default();
    crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert_eq!(unseen.len(), 1);

    absence.observe(&HashMap::from([(
        unknown.node(),
        membership::LiveFlags { departing: false },
    )]));
    absence.observe(&HashMap::new());
    crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert_eq!(unseen.len(), 0, "the writer is known now");
    cluster.shutdown().await;
}

/// An `OrSet` writer slot of an unobserved node retires the same way,
/// keeping its elements, and retirement is sticky: a later add from the
/// retired writer is rejected.
#[tokio::test]
async fn crdt_compact_tick_retires_an_unobserved_or_set_writer_a_bound_after_it_first_met_it() {
    use crate::store::crdt::{OrSet, OrSetResolver};
    let retire_after = Duration::from_millis(150);
    let cluster = Cluster::builder("cluster-it-crdt-unobserved-or-set")
        .seeds(std::iter::empty())
        .config(crdt_loopback_config(retire_after))
        .build()
        .await
        .expect("build succeeds");
    let name = SmolStr::new("sets");
    let cache: Cache<u32, OrSet<String>> = cluster
        .cache::<u32, OrSet<String>>(name.as_str())
        .mode(Mode::Replicated)
        .resolver(Arc::new(OrSetResolver::<String>::new()))
        .open()
        .await
        .expect("open succeeds");
    let shard = registered_shard(&cluster, &name);
    let unknown = writer(999, 1);
    cache
        .insert(1, OrSet::add(unknown, 1, "first".to_string()))
        .await
        .expect("seed add from a writer this node never observed");
    let absence = absence::AbsenceTracker::default();
    let unseen = UnseenWriters::default();
    tokio::time::sleep(retire_after + Duration::from_millis(50)).await;

    let (retired, _) = crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert!(
        retired.is_empty(),
        "first sight retires nothing: {retired:?}"
    );

    tokio::time::sleep(retire_after + Duration::from_millis(50)).await;
    let (retired, compacted) = crdt_compact_tick(
        shard.as_ref(),
        &name,
        &cluster,
        &absence,
        &unseen,
        three_bounds(retire_after),
    )
    .await;
    assert_eq!(retired, vec![unknown]);
    assert_eq!(compacted, 1);
    let set = cache.get(&1).await.expect("the set is resident");
    assert!(set.contains(&"first".to_string()), "retiring keeps the add");

    cache
        .insert(1, OrSet::add(unknown, 2, "second".to_string()))
        .await
        .expect("a late add from the retired writer");
    let set = cache.get(&1).await.expect("the set is resident");
    assert!(
        !set.contains(&"second".to_string()),
        "a retired writer adds nothing new"
    );
    assert!(set.contains(&"first".to_string()));
    cluster.shutdown().await;
}
