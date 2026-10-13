//! Merged values expire in every mode: a merged counter dies with its deadline and a merge after expiry starts a new value.
//!
//! Every node of a test cluster reads one clock the test moves, so a counter
//! can be expired and not yet swept when the next write arrives, and no test
//! waits out a lifetime.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::store::Ttl;

/// The counter's lifetime.
const LIFETIME: Duration = Duration::from_secs(6);

/// The clock every node of a test cluster reads, in epoch milliseconds.
type Clock = Arc<AtomicU64>;

/// A clock set an hour past the system time and moved only by [`advance`].
/// A fetch re-checks a record's deadline against the system clock, so a
/// deadline stays ahead of it however long the cluster takes to form.
fn manual_clock() -> Clock {
    Arc::new(AtomicU64::new(now_ms() + 3_600_000))
}

/// Moves `clock` forward by `by`.
fn advance(clock: &Clock, by: Duration) {
    let by = u64::try_from(by.as_millis()).expect("a short duration");
    clock.fetch_add(by, Ordering::SeqCst);
}

const KEY: u32 = 1;

type CounterNodes = Vec<(Cluster, Cache<u32, PnCounter>)>;

/// `count` nodes with a `PnCounter` cache `name` open under `mode`, every
/// value living for [`LIFETIME`] on `clock`. Returns once every node's
/// ownership view ranks every node.
async fn counter_nodes(
    name: &'static str,
    mode: Mode,
    count: usize,
    clock: &Clock,
) -> CounterNodes {
    let mut nodes: CounterNodes = Vec::new();
    for index in 0..count {
        let builder = Cluster::builder(name).config(loopback_config().with(|c| {
            c.fetch_timeout = Duration::from_secs(5);
            c.phi_threshold = 1_000.0;
        }));
        let cluster = match nodes.first() {
            None => builder.seeds(std::iter::empty()),
            Some((seed, _)) => builder.seeds([seed.local_gossip_addr()]),
        }
        .build()
        .await
        .expect("node builds");
        wait_for_peer_count(&cluster, index).await;
        let reader = Arc::clone(clock);
        let cache = cluster
            .cache::<u32, PnCounter>(name)
            .mode(mode)
            .clock(Arc::new(move || reader.load(Ordering::SeqCst)))
            .ttl(LIFETIME)
            .resolver(Arc::new(PnCounterResolver))
            .open()
            .await
            .expect("opens");
        nodes.push((cluster, cache));
    }
    wait_until(
        Duration::from_secs(30),
        "every node's ownership view ranks every node",
        async || {
            nodes.iter().all(|(_, cache)| {
                cache
                    .shard
                    .ownership_view()
                    .is_none_or(|view| view.eligible().len() == count)
            })
        },
    )
    .await;
    nodes
}

async fn shut_down(nodes: CounterNodes) {
    for (cluster, _) in nodes.into_iter().rev() {
        cluster.shutdown().await;
    }
}

/// Merges `by` into [`KEY`] from node `index`, under that node's own writer.
async fn add(nodes: &CounterNodes, index: usize, by: u64) {
    let cache = &nodes[index].1;
    cache
        .merge(KEY, PnCounter::local_delta(cache.writer_id(), by))
        .await
        .expect("merge");
}

/// What node `index` reads for [`KEY`]: a `Mode::Distributed` node asks the
/// owners, any other node reads its own copy.
async fn value(nodes: &CounterNodes, index: usize) -> Option<i128> {
    nodes[index]
        .1
        .fetch(&KEY)
        .await
        .expect("the owners answer")
        .map(|c| c.value())
}

/// Waits until every node reads `expected` for [`KEY`].
async fn wait_for_value(nodes: &CounterNodes, expected: Option<i128>, what: &str) {
    wait_until(Duration::from_secs(30), what, async || {
        for index in 0..nodes.len() {
            if value(nodes, index).await != expected {
                return false;
            }
        }
        true
    })
    .await;
}

/// Waits until every owner of [`KEY`] holds its part warm, so a read of it
/// gets an answer rather than a decline.
async fn wait_for_warm_owners(nodes: &CounterNodes) {
    let part = PartId::of_key(&encode_key(&KEY).expect("u32 encodes"));
    let owners = nodes[0].1.owners_of(&KEY);
    wait_until(
        Duration::from_secs(30),
        "every owner holds the key's part warm",
        async || {
            nodes
                .iter()
                .filter(|(cluster, _)| owners.contains(&cluster.node_id()))
                .all(|(_, cache)| {
                    !cache.shard.is_cold_part(part) && !cache.shard.is_unverified_part(part)
                })
        },
    )
    .await;
}

/// Asserts every node's sweep removes the expired entry: no node holds a
/// key that outlived its deadline. The count sweeps before it reads, so
/// this runs only once the test no longer needs an unswept entry.
async fn assert_swept(nodes: &CounterNodes) {
    for (index, (_, cache)) in nodes.iter().enumerate() {
        assert_eq!(
            cache.entry_count().await,
            0,
            "node {index}'s sweep removed the expired entry"
        );
    }
}

/// Moves `clock` past the deadline of everything written so far. Nothing
/// sweeps: an expired entry stays in place until [`assert_swept`] counts.
fn outlive_the_lifetime(clock: &Clock) {
    advance(clock, LIFETIME + Duration::from_millis(500));
}

#[tokio::test]
async fn a_merged_local_counter_expires_and_a_merge_after_expiry_starts_a_new_value() {
    let clock = manual_clock();
    let nodes = counter_nodes("cache-it-merge-expiry-local", Mode::Local, 1, &clock).await;
    add(&nodes, 0, 3).await;
    add(&nodes, 0, 7).await;
    assert_eq!(value(&nodes, 0).await, Some(7));
    assert!(
        matches!(nodes[0].1.ttl_of(&KEY), Some(Ttl::Remaining(_))),
        "the merged counter carries a deadline"
    );

    outlive_the_lifetime(&clock);
    assert_eq!(value(&nodes, 0).await, None, "the merged counter expired");
    add(&nodes, 0, 1).await;
    assert_eq!(
        value(&nodes, 0).await,
        Some(1),
        "the expired contributions do not fold into the new write"
    );
    assert_eq!(
        nodes[0].1.ttl_of(&KEY),
        Some(Ttl::Remaining(LIFETIME)),
        "the new value carries the full lifetime"
    );

    outlive_the_lifetime(&clock);
    assert_swept(&nodes).await;
    assert_eq!(value(&nodes, 0).await, None, "the new value expires too");
    shut_down(nodes).await;
}

#[tokio::test]
async fn a_merged_replicated_counter_expires_on_every_node_and_stays_dead() {
    let clock = manual_clock();
    let nodes = counter_nodes(
        "cache-it-merge-expiry-replicated",
        Mode::Replicated,
        3,
        &clock,
    )
    .await;
    add(&nodes, 0, 3).await;
    add(&nodes, 1, 4).await;
    wait_for_value(&nodes, Some(7), "every node converges to the merged total").await;
    for (_, cache) in &nodes {
        assert!(
            matches!(cache.ttl_of(&KEY), Some(Ttl::Remaining(_))),
            "every holder carries the merged deadline"
        );
    }

    outlive_the_lifetime(&clock);
    wait_for_value(&nodes, None, "the merged counter expires on every node").await;

    // Every node still holds the expired counter, unswept.
    add(&nodes, 2, 1).await;
    wait_for_value(&nodes, Some(1), "the new value replaces the expired one").await;
    // Anti-entropy rounds run every 200ms; none brings the old total back.
    tokio::time::sleep(Duration::from_millis(800)).await;
    wait_for_value(&nodes, Some(1), "the new value stays the only contribution").await;

    outlive_the_lifetime(&clock);
    assert_swept(&nodes).await;
    shut_down(nodes).await;
}

#[tokio::test]
async fn a_merged_distributed_counter_expires_on_its_owners_and_stays_dead() {
    let clock = manual_clock();
    let nodes = counter_nodes(
        "cache-it-merge-expiry-distributed",
        Mode::distributed(),
        3,
        &clock,
    )
    .await;
    wait_for_warm_owners(&nodes).await;
    add(&nodes, 0, 3).await;
    add(&nodes, 1, 4).await;
    wait_for_value(&nodes, Some(7), "every node reads the merged total").await;
    let owners = nodes[0].1.owners_of(&KEY);
    for (cluster, cache) in &nodes {
        if owners.contains(&cluster.node_id()) {
            assert!(
                matches!(cache.ttl_of(&KEY), Some(Ttl::Remaining(_))),
                "every owner carries the merged deadline"
            );
        }
    }

    outlive_the_lifetime(&clock);
    wait_for_value(&nodes, None, "the merged counter expires on its owners").await;

    // The owners still hold the expired counter, unswept.
    add(&nodes, 2, 1).await;
    wait_for_value(&nodes, Some(1), "the new value starts afresh").await;

    outlive_the_lifetime(&clock);
    assert_swept(&nodes).await;
    shut_down(nodes).await;
}

#[tokio::test]
async fn a_merged_invalidation_counter_expires_and_a_merge_after_expiry_starts_afresh() {
    let clock = manual_clock();
    let nodes = counter_nodes(
        "cache-it-merge-expiry-invalidation",
        Mode::Invalidation,
        3,
        &clock,
    )
    .await;
    add(&nodes, 0, 3).await;
    add(&nodes, 1, 4).await;
    assert!(
        value(&nodes, 1).await.is_some(),
        "the writer holds its own contribution"
    );

    outlive_the_lifetime(&clock);
    wait_for_value(&nodes, None, "no node still reads the counter").await;

    // Node 0 still holds the expired counter, unswept.
    add(&nodes, 0, 1).await;
    assert_eq!(
        value(&nodes, 0).await,
        Some(1),
        "the expired contributions do not fold into the new write"
    );
    assert_eq!(
        value(&nodes, 2).await,
        None,
        "a node that never held it misses"
    );
    wait_until(
        Duration::from_secs(30),
        "node 1, whose copy expired, ignores the record",
        async || value(&nodes, 1).await.is_none(),
    )
    .await;

    outlive_the_lifetime(&clock);
    assert_swept(&nodes).await;
    shut_down(nodes).await;
}

#[tokio::test]
async fn merge_with_ttl_overrides_the_cache_ttl_for_that_contribution() {
    let clock = manual_clock();
    let nodes = counter_nodes("cache-it-merge-with-ttl", Mode::Local, 1, &clock).await;
    let cache = &nodes[0].1;
    let writer = cache.writer_id();

    cache
        .merge_with_ttl(
            KEY,
            PnCounter::local_delta(writer, 3),
            Duration::from_secs(600),
        )
        .await
        .expect("merge_with_ttl");
    let left = |cache: &Cache<u32, PnCounter>| match cache.ttl_of(&KEY) {
        Some(Ttl::Remaining(left)) => left,
        other => panic!("a remaining lifetime, got {other:?}"),
    };
    assert!(
        left(cache) > LIFETIME + Duration::from_secs(500),
        "the call's TTL replaces the cache default"
    );

    cache
        .merge_with_ttl(
            KEY,
            PnCounter::local_delta(writer, 4),
            Duration::from_secs(10),
        )
        .await
        .expect("merge_with_ttl");
    assert!(
        left(cache) > Duration::from_secs(500),
        "the merged record keeps the later deadline"
    );
    assert_eq!(value(&nodes, 0).await, Some(4));

    cache
        .merge(2, PnCounter::local_delta(writer, 1))
        .await
        .expect("merge");
    assert!(
        matches!(cache.ttl_of(&2), Some(Ttl::Remaining(left)) if left <= LIFETIME),
        "merge keeps the cache's default TTL"
    );
    shut_down(nodes).await;
}

#[tokio::test]
async fn merge_with_ttl_reaches_a_replica_with_its_deadline() {
    let clock = manual_clock();
    let nodes = counter_nodes(
        "cache-it-merge-with-ttl-replica",
        Mode::Replicated,
        2,
        &clock,
    )
    .await;
    let cache = &nodes[0].1;
    cache
        .merge_with_ttl(
            KEY,
            PnCounter::local_delta(cache.writer_id(), 3),
            Duration::from_secs(600),
        )
        .await
        .expect("merge_with_ttl");
    wait_until(
        Duration::from_secs(30),
        "the replica holds the record with the call's deadline",
        async || {
            matches!(
                nodes[1].1.ttl_of(&KEY),
                Some(Ttl::Remaining(left)) if left > Duration::from_secs(500)
            )
        },
    )
    .await;
    shut_down(nodes).await;
}

#[tokio::test]
async fn merge_with_ttl_reaches_the_owners_of_a_distributed_cache_with_its_deadline() {
    let clock = manual_clock();
    let nodes = counter_nodes(
        "cache-it-merge-with-ttl-distributed",
        Mode::distributed(),
        3,
        &clock,
    )
    .await;
    wait_for_warm_owners(&nodes).await;
    // With three nodes and two owners per part, exactly one node is not an
    // owner of the key: its write takes the forwarding path.
    let owners = nodes[0].1.owners_of(&KEY);
    let writer = nodes
        .iter()
        .position(|(cluster, _)| !owners.contains(&cluster.node_id()))
        .expect("one node does not own the key");
    let cache = &nodes[writer].1;
    cache
        .merge_with_ttl(
            KEY,
            PnCounter::local_delta(cache.writer_id(), 3),
            Duration::from_secs(600),
        )
        .await
        .expect("merge_with_ttl");
    wait_until(
        Duration::from_secs(30),
        "every owner holds the record with the call's deadline",
        async || {
            nodes
                .iter()
                .filter(|(cluster, _)| owners.contains(&cluster.node_id()))
                .all(|(_, cache)| {
                    matches!(
                        cache.ttl_of(&KEY),
                        Some(Ttl::Remaining(left)) if left > Duration::from_secs(500)
                    )
                })
        },
    )
    .await;
    assert_eq!(
        cache.ttl_of(&KEY),
        None,
        "the writer, not an owner, holds no copy"
    );
    shut_down(nodes).await;
}

#[tokio::test]
async fn merge_with_ttl_folds_into_an_invalidation_holder_with_the_later_deadline() {
    let clock = manual_clock();
    let nodes = counter_nodes(
        "cache-it-merge-with-ttl-invalidation",
        Mode::Invalidation,
        2,
        &clock,
    )
    .await;
    let (holder, writer) = (&nodes[0].1, &nodes[1].1);
    holder
        .merge_with_ttl(
            KEY,
            PnCounter::local_delta(holder.writer_id(), 3),
            Duration::from_secs(60),
        )
        .await
        .expect("holder merges");
    writer
        .merge_with_ttl(
            KEY,
            PnCounter::local_delta(writer.writer_id(), 4),
            Duration::from_secs(600),
        )
        .await
        .expect("writer merges");

    wait_until(
        Duration::from_secs(30),
        "the holder folds the record and takes its later deadline",
        async || {
            holder
                .fetch(&KEY)
                .await
                .expect("local read")
                .is_some_and(|c| c.value() == 7)
                && matches!(
                    holder.ttl_of(&KEY),
                    Some(Ttl::Remaining(left)) if left > Duration::from_secs(500)
                )
        },
    )
    .await;
    shut_down(nodes).await;
}
