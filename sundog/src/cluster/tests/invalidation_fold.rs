//! Tests for an `Invalidation` cache with a merging resolver across nodes: a write ships the merged record, a holder folds it in, a non-holder ignores it, a removal still drops the copy, and a folded value expires with its deadline.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::Cache;
use crate::cluster::test_support::wait_until;
use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};
use crate::store::{Ttl, bucket_of, encode_key, now_ms};
use crate::wire;

/// Opens `counters` on `cluster` in `Mode::Invalidation` with
/// [`PnCounterResolver`].
async fn open_counters(cluster: &Cluster) -> Cache<u32, PnCounter> {
    cluster
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Invalidation)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("opens")
}

/// The lifetime of the counters in the expiry test.
const LIFETIME: Duration = Duration::from_secs(5);

/// The clock every node of the expiry test reads, in epoch milliseconds.
type Clock = Arc<AtomicU64>;

/// A clock set an hour past the system time and moved only by the test.
fn manual_clock() -> Clock {
    Arc::new(AtomicU64::new(now_ms() + 3_600_000))
}

/// [`open_counters`] with every value living for [`LIFETIME`] on `clock`.
async fn open_expiring_counters(cluster: &Cluster, clock: &Clock) -> Cache<u32, PnCounter> {
    let reader = Arc::clone(clock);
    cluster
        .cache::<u32, PnCounter>("counters")
        .mode(Mode::Invalidation)
        .clock(Arc::new(move || reader.load(Ordering::SeqCst)))
        .ttl(LIFETIME)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("opens")
}

fn delta(cluster: &Cluster, by: u64) -> PnCounter {
    PnCounter::local_delta(
        WriterId::new(cluster.node_id(), cluster.local_incarnation()),
        by,
    )
}

fn value(counter: Option<PnCounter>) -> Option<i128> {
    counter.map(|c| c.value())
}

/// Waits until every node of `clusters` gossips a peer set that all speak
/// the protocol a merged record travels under, so no write below goes out
/// as a bare invalidation.
async fn wait_for_fold_protocol(clusters: &[&Cluster]) {
    wait_until(
        Duration::from_secs(10),
        "every peer gossips the fold protocol",
        async || {
            clusters.iter().all(|cluster| {
                cluster.peers().iter().all(|peer| {
                    wire::peer_supports(peer.protocol, wire::PROTOCOL_INVALIDATION_FOLD)
                })
            })
        },
    )
    .await;
}

/// The key the tests write, and the barrier keys that follow it.
const KEY: u32 = 1;

/// The `n`th barrier key: one whose bucket sorts after [`KEY`]'s. A batch
/// applies its records bucket by bucket, so a barrier record never applies
/// ahead of a record for [`KEY`] that travels with it, and a record on the
/// wire applies in the order it was sent.
fn barrier_key(n: usize) -> u32 {
    let bucket = |key: u32| bucket_of(&encode_key(&key).expect("u32 encodes"));
    (KEY + 1..)
        .filter(|&key| bucket(key) > bucket(KEY))
        .nth(n)
        .expect("a later bucket is found quickly")
}

/// Makes `cache` hold barrier `key` with a total of 1 from its own node.
async fn hold(cluster: &Cluster, cache: &Cache<u32, PnCounter>, key: u32) {
    cache
        .merge(key, delta(cluster, 1))
        .await
        .expect("holds the barrier key");
}

/// Writes barrier `key` from `sender` with its running `total`, and waits
/// until `receiver`, the only other node holding `key`, at a total of 1,
/// shows it folded. A record `sender` wrote for [`KEY`] before this call has
/// then been applied, or ignored, at `receiver`: records from one node apply
/// in order, and the barrier record follows.
async fn barrier(
    sender: (&Cluster, &Cache<u32, PnCounter>),
    key: u32,
    total: u64,
    receiver: &Cache<u32, PnCounter>,
) {
    sender
        .1
        .merge(key, delta(sender.0, total))
        .await
        .expect("writes the barrier key");
    let expected = 1 + i128::from(total);
    wait_until(
        Duration::from_secs(10),
        "the barrier record is folded at the receiver",
        async || value(receiver.get(&key).await) == Some(expected),
    )
    .await;
}

#[tokio::test]
async fn a_merging_invalidation_write_folds_into_holders_and_is_ignored_elsewhere() {
    let (cluster_a, cluster_b, cluster_c) =
        three_node_cluster("cluster-it-invalidation-fold").await;
    wait_for_fold_protocol(&[&cluster_a, &cluster_b, &cluster_c]).await;
    let cache_a = open_counters(&cluster_a).await;
    let cache_b = open_counters(&cluster_b).await;
    let cache_c = open_counters(&cluster_c).await;
    let (b_to_a, b_to_c, a_to_c) = (barrier_key(0), barrier_key(1), barrier_key(2));
    hold(&cluster_a, &cache_a, b_to_a).await;
    hold(&cluster_c, &cache_c, b_to_c).await;
    hold(&cluster_c, &cache_c, a_to_c).await;

    // B holds the counter; A and C hold nothing, so B's record is
    // ignored at both.
    cache_b
        .merge(KEY, delta(&cluster_b, 3))
        .await
        .expect("b merges");
    barrier((&cluster_b, &cache_b), b_to_a, 1, &cache_a).await;
    barrier((&cluster_b, &cache_b), b_to_c, 1, &cache_c).await;
    assert_eq!(value(cache_a.get(&KEY).await), None, "A ignored B's record");
    assert_eq!(value(cache_c.get(&KEY).await), None, "C ignored B's record");

    // A writes the key it did not hold. B holds it, so B folds A's
    // record in rather than dropping its copy; C still holds nothing.
    cache_a
        .merge(KEY, delta(&cluster_a, 5))
        .await
        .expect("a merges");
    barrier((&cluster_a, &cache_a), a_to_c, 1, &cache_c).await;
    wait_until(
        Duration::from_secs(10),
        "b folds a's record into its copy",
        async || value(cache_b.get(&KEY).await) == Some(8),
    )
    .await;
    assert_eq!(
        value(cache_a.get(&KEY).await),
        Some(5),
        "A keeps its own write"
    );
    assert_eq!(
        value(cache_c.get(&KEY).await),
        None,
        "C holds nothing for the key"
    );

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

#[tokio::test]
async fn a_removal_in_a_merging_invalidation_cache_still_drops_the_copy() {
    let (cluster_a, cluster_b, cluster_c) =
        three_node_cluster("cluster-it-invalidation-remove").await;
    let cache_a = open_counters(&cluster_a).await;
    let cache_b = open_counters(&cluster_b).await;
    let cache_c = open_counters(&cluster_c).await;
    cache_b
        .merge(1, delta(&cluster_b, 3))
        .await
        .expect("b merges");
    cache_c
        .merge(1, delta(&cluster_c, 4))
        .await
        .expect("c merges");
    tokio::time::sleep(Duration::from_millis(5)).await;

    cache_a.remove(&1).await.expect("a removes");

    wait_until(
        Duration::from_secs(10),
        "b and c drop their copies",
        async || cache_b.get(&1).await.is_none() && cache_c.get(&1).await.is_none(),
    )
    .await;

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

#[tokio::test]
async fn a_folded_invalidation_counter_expires_on_its_holders_and_the_next_write_starts_afresh() {
    let (cluster_a, cluster_b, cluster_c) =
        three_node_cluster("cluster-it-invalidation-fold-expiry").await;
    wait_for_fold_protocol(&[&cluster_a, &cluster_b, &cluster_c]).await;
    let clock = manual_clock();
    let cache_a = open_expiring_counters(&cluster_a, &clock).await;
    let cache_b = open_expiring_counters(&cluster_b, &clock).await;
    let cache_c = open_expiring_counters(&cluster_c, &clock).await;
    let (a_to_b, b_to_c, b_to_a) = (barrier_key(0), barrier_key(1), barrier_key(2));
    hold(&cluster_b, &cache_b, a_to_b).await;
    hold(&cluster_c, &cache_c, b_to_c).await;

    // A holds the counter. B ignores A's record, so B's own write that
    // follows starts a separate contribution that A then folds in.
    cache_a
        .merge(KEY, delta(&cluster_a, 3))
        .await
        .expect("a merges");
    barrier((&cluster_a, &cache_a), a_to_b, 1, &cache_b).await;
    assert_eq!(value(cache_b.get(&KEY).await), None, "B ignored A's record");
    cache_b
        .merge(KEY, delta(&cluster_b, 4))
        .await
        .expect("b merges");
    barrier((&cluster_b, &cache_b), b_to_c, 1, &cache_c).await;
    wait_until(
        Duration::from_secs(10),
        "a folds b's record into its copy",
        async || value(cache_a.get(&KEY).await) == Some(7),
    )
    .await;
    assert_eq!(
        value(cache_b.get(&KEY).await),
        Some(4),
        "B keeps its own write"
    );
    assert_eq!(value(cache_c.get(&KEY).await), None, "C holds nothing");
    for (name, cache) in [("A", &cache_a), ("B", &cache_b)] {
        assert!(
            matches!(cache.ttl_of(&KEY), Some(Ttl::Remaining(_))),
            "{name} carries a deadline for the value it holds"
        );
    }

    clock.fetch_add(
        u64::try_from((LIFETIME + Duration::from_millis(500)).as_millis()).expect("short"),
        Ordering::SeqCst,
    );
    for (name, cache) in [("A", &cache_a), ("B", &cache_b), ("C", &cache_c)] {
        assert_eq!(value(cache.get(&KEY).await), None, "{name}'s copy expired");
    }
    // B's and C's sweeps remove their copies. A's expired copy stays in
    // place until the end: the write below meets it unswept.
    for (name, cache) in [("B", &cache_b), ("C", &cache_c)] {
        assert_eq!(
            cache.entry_count().await,
            0,
            "{name}'s sweep removed the entry"
        );
    }

    // B writes again. A's copy has expired but is still in place, so A
    // ignores the record rather than folding the expired 7 into it; B
    // starts a new value.
    hold(&cluster_a, &cache_a, b_to_a).await;
    cache_b
        .merge(KEY, delta(&cluster_b, 1))
        .await
        .expect("b merges again");
    barrier((&cluster_b, &cache_b), b_to_a, 1, &cache_a).await;
    assert_eq!(
        value(cache_b.get(&KEY).await),
        Some(1),
        "the expired contributions do not fold into the new write"
    );
    assert_eq!(value(cache_a.get(&KEY).await), None, "A ignored the record");
    assert_eq!(
        cache_a.entry_count().await,
        1,
        "A stored nothing for the key: its expired copy is swept, and only the barrier key is live"
    );
    assert_eq!(value(cache_c.get(&KEY).await), None, "C holds nothing");

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}

/// A sender ships its merged state, not only the contribution it just
/// wrote: a holder that missed a third writer's record gets it through the
/// sender's next write.
#[tokio::test]
async fn a_writer_ships_its_whole_merged_value_so_a_holder_that_missed_a_record_catches_up() {
    let (cluster_a, cluster_b, cluster_c) =
        three_node_cluster("cluster-it-invalidation-fold-state").await;
    wait_for_fold_protocol(&[&cluster_a, &cluster_b, &cluster_c]).await;
    let cache_a = open_counters(&cluster_a).await;
    let cache_b = open_counters(&cluster_b).await;
    let cache_c = open_counters(&cluster_c).await;
    let (b_to_a, b_to_c, c_to_a) = (barrier_key(0), barrier_key(1), barrier_key(2));
    hold(&cluster_a, &cache_a, b_to_a).await;
    hold(&cluster_c, &cache_c, b_to_c).await;
    hold(&cluster_a, &cache_a, c_to_a).await;

    // B holds the counter at 3. A and C hold nothing, so both ignore B's
    // record. C's write then folds into B's copy: B reads 7. A holds
    // nothing, so it ignores C's record too.
    cache_b
        .merge(KEY, delta(&cluster_b, 3))
        .await
        .expect("b merges");
    barrier((&cluster_b, &cache_b), b_to_a, 1, &cache_a).await;
    barrier((&cluster_b, &cache_b), b_to_c, 1, &cache_c).await;
    cache_c
        .merge(KEY, delta(&cluster_c, 4))
        .await
        .expect("c merges");
    barrier((&cluster_c, &cache_c), c_to_a, 1, &cache_a).await;
    wait_until(
        Duration::from_secs(10),
        "b folds c's record into its copy",
        async || value(cache_b.get(&KEY).await) == Some(7),
    )
    .await;
    assert_eq!(
        value(cache_a.get(&KEY).await),
        None,
        "A ignored both records"
    );

    // A writes its own contribution; B and C, holders, fold it.
    cache_a
        .merge(KEY, delta(&cluster_a, 1))
        .await
        .expect("a merges");
    wait_until(
        Duration::from_secs(10),
        "b and c fold a's record",
        async || {
            value(cache_b.get(&KEY).await) == Some(8) && value(cache_c.get(&KEY).await) == Some(5)
        },
    )
    .await;
    assert_eq!(
        value(cache_a.get(&KEY).await),
        Some(1),
        "A holds only its own contribution"
    );

    // B's next write ships B's whole value, which carries C's 4 that A never
    // received: A catches up to the full total.
    cache_b
        .merge(KEY, delta(&cluster_b, 5))
        .await
        .expect("b merges again");
    wait_until(
        Duration::from_secs(10),
        "a catches up to the whole value through b's write",
        async || {
            value(cache_a.get(&KEY).await) == Some(10) && value(cache_c.get(&KEY).await) == Some(10)
        },
    )
    .await;

    cluster_a.shutdown().await;
    cluster_b.shutdown().await;
    cluster_c.shutdown().await;
}
