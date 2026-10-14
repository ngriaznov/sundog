//! An `Invalidation` shard receiving a peer's merged record: it folds into a key the shard holds, is ignored for any other, and a tombstone record drops the copy.

use std::sync::atomic::AtomicU64;

use tokio::sync::broadcast::error::TryRecvError;

use super::*;
use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};
use crate::store::shard_serve::inbound_scope;

const T0: u64 = 1_000_000;

/// A shard of `mode` for `node` on a clock the test sets, merging through
/// [`PnCounterResolver`].
fn counter_shard(mode: Mode, node: u64, clock: &Arc<AtomicU64>) -> Shard<u32, PnCounter> {
    let reader = Arc::clone(clock);
    Shard::<u32, PnCounter>::new(
        SmolStr::new("test-invalidation-fold"),
        mode,
        NodeId::from(node),
        10_000,
        None,
        None,
    )
    .with_resolver(Arc::new(PnCounterResolver))
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
}

fn delta(node: u64, by: u64) -> PnCounter {
    PnCounter::local_delta(WriterId::new(NodeId::from(node), 1), by)
}

/// A peer's merged record for key `1`: a live counter holding `by` from
/// `node`, or a tombstone when `by` is `None`.
fn record(node: u64, by: Option<u64>, wall_ms: u64, expires_at_ms: Option<u64>) -> WireRecord {
    WireRecord {
        key: key_bytes(&1u32),
        value: by.map(|by| Bytes::from(delta(node, by).encode().expect("encodes"))),
        ver: hlc(wall_ms, node),
        expires_at_ms,
    }
}

fn counter_value(shard: &Shard<u32, PnCounter>) -> Option<i128> {
    shard.get_sync(&1).map(|c| c.value())
}

#[test]
fn only_an_invalidation_shard_stores_a_record_as_a_fold_into_a_held_key() {
    assert_eq!(
        inbound_scope(Mode::Invalidation),
        engine::ApplyScope::HeldOnly
    );
    for mode in [Mode::Local, Mode::Replicated, Mode::distributed()] {
        assert_eq!(inbound_scope(mode), engine::ApplyScope::Any, "{mode:?}");
    }
}

#[tokio::test]
async fn a_peers_record_folds_into_a_held_key() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);
    s.merge(1, delta(11, 5)).await.expect("merge");
    let mut events = s.events();

    ShardOps::apply_remote(&s, record(12, Some(3), T0 + 10, None)).await;

    assert_eq!(counter_value(&s), Some(8), "the two contributions sum");
    match events.try_recv() {
        Ok(Event::Updated { key: 1, origin, .. }) => {
            assert_eq!(origin, Origin::Remote(NodeId::from(12)));
        }
        other => panic!("a fold emits the event a remote apply emits: {other:?}"),
    }
}

#[tokio::test]
async fn a_batch_folds_each_held_key_and_skips_the_rest() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);
    s.merge(1, delta(11, 5)).await.expect("merge");
    let mut other = record(12, Some(2), T0 + 10, None);
    other.key = key_bytes(&2u32);

    ShardOps::apply_remote_batch(&s, vec![record(12, Some(3), T0 + 10, None), other]).await;

    assert_eq!(counter_value(&s), Some(8));
    assert_eq!(s.get_sync(&2), None, "key 2 is not held and stays absent");
    assert_eq!(s.entry_count().await, 1);
}

#[tokio::test]
async fn a_peers_record_for_an_unheld_key_is_ignored() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);
    let mut events = s.events();

    ShardOps::apply_remote(&s, record(12, Some(3), T0 + 10, None)).await;

    assert_eq!(counter_value(&s), None);
    assert_eq!(s.entry_count().await, 0, "nothing is stored");
    assert!(
        matches!(events.try_recv(), Err(TryRecvError::Empty)),
        "an ignored record emits no event"
    );
    assert!(
        s.records_for_typed(&[1]).await.is_empty(),
        "no tombstone or live record stands in for the key"
    );
}

#[tokio::test]
async fn a_peers_record_for_a_tombstoned_key_is_ignored() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);
    s.merge(1, delta(11, 5)).await.expect("merge");
    s.remove(&1).await.expect("remove");
    assert_eq!(counter_value(&s), None);

    ShardOps::apply_remote(&s, record(12, Some(3), T0 + 10, None)).await;

    assert_eq!(counter_value(&s), None, "the removal is not undone");
}

#[tokio::test]
async fn a_peers_record_for_an_expired_key_is_ignored() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);
    s.merge_with_ttl(1, delta(11, 5), Duration::from_secs(1))
        .await
        .expect("merge");
    clock.store(T0 + 2_000, Ordering::SeqCst);
    assert_eq!(counter_value(&s), None, "the counter has expired");

    ShardOps::apply_remote(&s, record(12, Some(3), T0 + 1_500, None)).await;

    assert_eq!(
        counter_value(&s),
        None,
        "an expired copy is not held, so the record neither folds nor replaces it"
    );
}

#[tokio::test]
async fn a_fold_keeps_the_later_deadline() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);
    s.merge_with_ttl(1, delta(11, 5), Duration::from_secs(10))
        .await
        .expect("merge");

    ShardOps::apply_remote(&s, record(12, Some(3), T0 + 10, Some(T0 + 60_000))).await;

    assert_eq!(counter_value(&s), Some(8));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(Duration::from_secs(60))));
}

#[tokio::test]
async fn a_tombstone_record_drops_the_held_copy() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);
    s.merge(1, delta(11, 5)).await.expect("merge");
    let mut events = s.events();

    ShardOps::apply_remote(&s, record(12, None, T0 + 10, None)).await;

    assert_eq!(counter_value(&s), None);
    assert_eq!(s.entry_count().await, 0);
    match events.try_recv() {
        Ok(Event::Removed { key: 1, origin }) => {
            assert_eq!(origin, Origin::Remote(NodeId::from(12)));
        }
        other => panic!("a drop emits the event an invalidation emits: {other:?}"),
    }
    assert!(
        s.records_for_typed(&[1]).await.is_empty(),
        "the drop writes no tombstone"
    );
}

#[tokio::test]
async fn a_tombstone_record_older_than_the_held_copy_leaves_it() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);
    s.merge(1, delta(11, 5)).await.expect("merge");

    ShardOps::apply_remote(&s, record(12, None, T0 - 10_000, None)).await;

    assert_eq!(counter_value(&s), Some(5));
}

#[tokio::test]
async fn a_tombstone_record_for_an_unheld_key_stores_nothing() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Invalidation, 1, &clock);

    ShardOps::apply_remote(&s, record(12, None, T0 + 10, None)).await;

    assert_eq!(s.entry_count().await, 0);
    assert_eq!(s.records_for_typed(&[1]).await.len(), 0);
}

#[tokio::test]
async fn a_replicated_shard_stores_a_peers_record_for_an_unheld_key() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(Mode::Replicated, 1, &clock);

    ShardOps::apply_remote(&s, record(12, Some(3), T0 + 10, None)).await;

    assert_eq!(
        counter_value(&s),
        Some(3),
        "only an Invalidation shard holds back a record for a key it lacks"
    );
}

#[derive(Debug, thiserror::Error)]
#[error("source unavailable")]
struct Down;

/// A record a peer ships for a key this shard is loading, and does not
/// hold, tells the load its read may predate the peer's write: the loaded
/// value answers the caller and is not cached.
#[tokio::test]
async fn a_peers_record_during_a_load_leaves_the_loaded_value_uncached() {
    let release = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(tokio::sync::Notify::new());
    let s = Shard::<u32, String>::new(
        SmolStr::new("test-invalidation-fold-load"),
        Mode::Invalidation,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_loader(Loader::batch({
        let release = Arc::clone(&release);
        let started = Arc::clone(&started);
        move |keys: Vec<u32>| {
            let release = Arc::clone(&release);
            let started = Arc::clone(&started);
            async move {
                started.notify_one();
                release.notified().await;
                Ok::<_, Down>(
                    keys.into_iter()
                        .map(|key| (key, "stale".to_string()))
                        .collect(),
                )
            }
        }
    }));
    let race = async {
        started.notified().await;
        let record = WireRecord {
            key: key_bytes(&2u32),
            value: Some(key_bytes(&"fresh".to_string())),
            ver: hlc(ahead_ms(), 2),
            expires_at_ms: None,
        };
        ShardOps::apply_remote(&s, record).await;
        release.notify_one();
    };

    let (loaded, ()) = tokio::join!(s.load(&2), race);

    assert_eq!(loaded.expect("load"), Some("stale".to_string()));
    assert_eq!(s.get(&2).await, None, "neither value is cached");
}

/// An `Invalidation` counter shard that coalesces `merge` calls for a minute.
fn coalescing_counter_shard(clock: &Arc<AtomicU64>) -> Shard<u32, PnCounter> {
    counter_shard(Mode::Invalidation, 1, clock).with_merge_coalesce_window(Duration::from_secs(60))
}

#[tokio::test]
async fn an_invalidation_newer_than_a_pending_merge_flushes_it_then_removes_the_copy() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = coalescing_counter_shard(&clock);
    s.merge(1, delta(11, 5)).await.expect("merge");
    let (merged_at, _) = s.pending_merge_snapshot(&1).expect("pending");

    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(T0 + 10, 12)).await;
    s.flush_all_pending_merges();

    assert_eq!(
        counter_value(&s),
        None,
        "the invalidation lands after the merge it follows, so no copy stays"
    );
    assert_eq!(
        applied_writes(s.fan_out_queue().drain()),
        vec![(1, merged_at)],
        "the merge replicated at its own version before the copy dropped: peers keep the contribution"
    );
}

#[tokio::test]
async fn an_invalidation_older_than_a_pending_merge_keeps_it() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = coalescing_counter_shard(&clock);
    s.merge(1, delta(11, 5)).await.expect("merge");

    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(T0 - 10, 12)).await;
    assert!(
        s.pending_merge_snapshot(&1).is_some(),
        "the fold is still pending"
    );
    s.flush_all_pending_merges();

    assert_eq!(counter_value(&s), Some(5));
}

#[tokio::test]
async fn a_tombstone_record_in_invalidation_mode_discards_a_pending_fold() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = coalescing_counter_shard(&clock);
    s.merge(1, delta(11, 5)).await.expect("merge");

    ShardOps::apply_remote(&s, record(12, None, T0 + 10, None)).await;
    s.flush_all_pending_merges();

    assert_eq!(
        counter_value(&s),
        None,
        "a known tombstone newer than the merge call outvotes it"
    );
    assert!(
        s.fan_out_queue().drain().is_empty(),
        "the discarded fold replicates nothing"
    );
}

#[tokio::test]
async fn a_tombstone_record_older_than_a_pending_fold_leaves_it_in_invalidation_mode() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = coalescing_counter_shard(&clock);
    s.merge(1, delta(11, 5)).await.expect("merge");

    ShardOps::apply_remote(&s, record(12, None, T0 - 10, None)).await;
    s.flush_all_pending_merges();

    assert_eq!(counter_value(&s), Some(5));
}
