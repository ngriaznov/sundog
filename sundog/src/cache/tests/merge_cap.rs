//! A merged value stays within one frame: an `OrSet` that keeps growing is
//! refused at `max_frame`, every merge entry point of a cache reports it, and
//! a value that shrinks takes the next add.
//!
//! The cluster tests run on the real clock. Each waits for its cluster and its
//! convergence with a real-time bound, and returns as soon as its condition
//! holds.

use super::*;
use crate::store::crdt::{OrSet, OrSetResolver};

type TagCache = Cache<u32, OrSet<String>>;

/// The frame cap of every cluster here: sixteen elements of 1 KiB each do
/// not fit it.
const MAX_FRAME: usize = 16 * 1_024;

/// An element of a kilobyte, distinct per `n`.
fn element(n: usize) -> String {
    format!("{n:04}{}", "x".repeat(1_000))
}

async fn tag_cache(name: &'static str, window: Duration) -> (Cluster, TagCache) {
    let cluster = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(loopback_config().with(|config| config.max_frame = MAX_FRAME))
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, OrSet<String>>("tags")
        .mode(Mode::Local)
        .resolver(Arc::new(OrSetResolver::<String>::new()))
        .merge_coalesce_window(window)
        .open()
        .await
        .expect("opens");
    (cluster, cache)
}

fn too_large(result: Result<(), CacheError>) {
    let error = result.expect_err("the merge is refused");
    assert!(
        matches!(error, CacheError::ValueTooLarge { limit, .. } if limit == MAX_FRAME),
        "expected ValueTooLarge at the frame cap, got {error:?}"
    );
}

/// Adds elements until a call is refused, and returns how many it took.
async fn fill(cache: &TagCache) -> usize {
    for n in 0.. {
        match cache
            .merge_sequenced(1, |writer, seq| OrSet::add(writer, seq, element(n)))
            .await
        {
            Ok(()) => {}
            Err(CacheError::ValueTooLarge { .. }) => return n,
            Err(other) => panic!("add {n} failed: {other:?}"),
        }
        assert!(
            n < 64,
            "a 16 KiB frame holds fewer than 64 kilobyte elements"
        );
    }
    unreachable!("the loop returns")
}

#[tokio::test]
async fn a_set_that_keeps_growing_is_refused_at_a_frame_and_every_merge_call_says_so() {
    let (cluster, cache) = tag_cache("cache-it-cap-grow", Duration::ZERO).await;

    let held = fill(&cache).await;

    assert!(
        (10..16).contains(&held),
        "about fifteen kilobyte elements fit: {held}"
    );
    let set = cache.get(&1).await.expect("stored");
    assert_eq!(set.iter().count(), held, "the refused add changed nothing");
    let writer = cache.writer_id();
    too_large(
        cache
            .merge(1, OrSet::add(writer, 1_000, element(900)))
            .await,
    );
    too_large(
        cache
            .merge_with_ttl(
                1,
                OrSet::add(writer, 1_001, element(901)),
                Duration::from_secs(60),
            )
            .await,
    );
    too_large(
        cache
            .merge_sequenced(1, |writer, seq| OrSet::add(writer, seq, element(902)))
            .await,
    );
    too_large(
        cache
            .merge_sequenced_with_ttl(
                1,
                |writer, seq| OrSet::add(writer, seq, element(903)),
                Duration::from_secs(60),
            )
            .await,
    );
    assert_eq!(
        cache.get(&1).await.expect("stored").iter().count(),
        held,
        "four refusals later it is as it was"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn an_insert_that_merges_into_a_full_set_is_refused_by_every_insert_call() {
    let (cluster, cache) = tag_cache("cache-it-cap-insert", Duration::ZERO).await;
    let held = fill(&cache).await;
    let writer = cache.writer_id();
    let add = |seq: u64, n: usize| OrSet::add(writer, seq, element(n));

    too_large(cache.insert(1, add(1_000, 900)).await);
    too_large(cache.insert_sync(1, add(1_001, 901)));
    too_large(
        cache
            .insert_with_ttl(1, add(1_002, 902), Duration::from_secs(60))
            .await,
    );
    too_large(cache.insert_many([(1, add(1_003, 903))]).await);
    too_large(
        cache
            .insert_many_with_ttl([(1, add(1_004, 904))], Duration::from_secs(60))
            .await,
    );

    assert_eq!(
        cache.get(&1).await.expect("stored").iter().count(),
        held,
        "five refusals later it is as it was"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn an_insert_many_applies_the_entries_around_a_refused_one_and_returns_the_refusal() {
    let (cluster, cache) = tag_cache("cache-it-cap-insert-many", Duration::ZERO).await;
    let held = fill(&cache).await;
    let writer = cache.writer_id();
    let add = |seq: u64, n: usize| OrSet::add(writer, seq, element(n));

    too_large(
        cache
            .insert_many([
                (2, add(2_000, 800)),
                (1, add(2_001, 801)),
                (3, add(2_002, 802)),
            ])
            .await,
    );

    assert_eq!(
        cache.get(&1).await.expect("stored").iter().count(),
        held,
        "the refused entry changed nothing"
    );
    assert!(
        cache
            .get(&2)
            .await
            .is_some_and(|set| set.contains(&element(800))),
        "the entry before it applied"
    );
    assert!(
        cache
            .get(&3)
            .await
            .is_some_and(|set| set.contains(&element(802))),
        "and so did the entry after it"
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_remove_shrinks_a_full_set_and_the_next_add_fits() {
    let (cluster, cache) = tag_cache("cache-it-cap-shrink", Duration::ZERO).await;
    let held = fill(&cache).await;
    let observed = cache.get(&1).await.expect("stored");

    cache
        .merge(1, OrSet::remove(&observed, &element(0)))
        .await
        .expect("a remove leaves the value no larger");
    cache
        .merge(
            1,
            OrSet::remove(&cache.get(&1).await.expect("stored"), &element(1)),
        )
        .await
        .expect("so does a second");
    cache
        .merge_sequenced(1, |writer, seq| OrSet::add(writer, seq, element(500)))
        .await
        .expect("room again");

    let set = cache.get(&1).await.expect("stored");
    assert_eq!(set.iter().count(), held - 1, "two out, one in");
    assert!(set.contains(&element(500)));
    assert!(!set.contains(&element(0)) && !set.contains(&element(1)));
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_windowed_add_that_overflows_its_own_pending_fold_gets_the_error() {
    let (cluster, cache) = tag_cache("cache-it-cap-window", Duration::from_secs(60)).await;

    let folded = fill(&cache).await;

    assert!(
        (10..16).contains(&folded),
        "the pending fold holds the same fifteen: {folded}"
    );
    too_large(
        cache
            .merge_sequenced(1, |writer, seq| OrSet::add(writer, seq, element(900)))
            .await,
    );
    // The fold stays pending for a minute; a close applies it.
    cluster.shutdown().await;
}
