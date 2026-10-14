//! A merge waiting in its coalesce window orders against removes, clears and tombstones as the call did, and a tombstone newer than the call discards it.

use super::merging::SumResolver;
use super::*;
use crate::store::PendingMerge;
use crate::store::shard_merge::{folded_pending_ver, pending_merge_superseded};

const WINDOW: Duration = Duration::from_secs(60);

fn coalescing_shard() -> Shard<u32, u32> {
    shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(WINDOW)
}

fn tombstone_record(key: u32, ver: Hlc) -> WireRecord {
    WireRecord {
        key: key_bytes(&key),
        value: None,
        ver,
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn a_remove_after_a_pending_merge_leaves_the_key_removed() {
    let s = coalescing_shard();
    let mut events = s.events();
    s.merge(1, 5).await.expect("merge");

    s.remove(&1).await.expect("remove");
    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        None,
        "a remove issued after the merge call outvotes the pending merge"
    );
    assert_eq!(
        s.next_pending_merge_deadline_ms(),
        None,
        "the remove took the pending fold with it"
    );
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event, Event::Created { .. } | Event::Updated { .. }),
            "the discarded fold publishes no write event: {event:?}"
        );
    }
}

#[tokio::test]
async fn remove_sync_after_a_pending_merge_leaves_the_key_removed() {
    let s = coalescing_shard();
    s.merge(1, 5).await.expect("merge");

    s.remove_sync(&1).expect("remove_sync");
    s.flush_all_pending_merges();

    assert_eq!(s.get(&1).await, None);
}

#[tokio::test]
async fn a_clear_after_a_pending_merge_leaves_the_key_removed() {
    let s = coalescing_shard();
    s.merge(1, 5).await.expect("merge");
    assert!(
        s.keys().is_empty(),
        "a pending-only key is not among the stored keys clear used to snapshot"
    );

    s.clear().await.expect("clear");
    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        None,
        "a clear issued after the merge call outvotes the pending merge"
    );
    assert_eq!(s.next_pending_merge_deadline_ms(), None);
}

#[tokio::test]
async fn a_remove_many_after_a_pending_merge_leaves_the_key_removed() {
    let s = coalescing_shard();
    s.merge(1, 5).await.expect("merge");
    s.merge(2, 6).await.expect("merge another key");

    s.remove_many([1u32]).await.expect("remove_many");
    s.flush_all_pending_merges();

    assert_eq!(s.get(&1).await, None, "the removed key stays removed");
    assert_eq!(
        s.get(&2).await,
        Some(6),
        "a key the remove did not name keeps its pending fold"
    );
}

#[tokio::test]
async fn a_newer_remote_tombstone_outvotes_a_pending_merge() {
    let s = coalescing_shard();
    s.merge(1, 5).await.expect("merge");

    ShardOps::apply_remote(&s, tombstone_record(1, hlc(ahead_ms(), 2))).await;
    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        None,
        "a newer remote tombstone outvotes the pending merge"
    );
    assert_eq!(s.next_pending_merge_deadline_ms(), None);
}

#[tokio::test]
async fn a_remote_tombstone_older_than_the_pending_fold_leaves_it() {
    let s = coalescing_shard();
    s.merge(1, 5).await.expect("merge");

    ShardOps::apply_remote(&s, tombstone_record(1, hlc(1, 2))).await;
    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        Some(5),
        "a tombstone the merge call outranks never reaches the fold"
    );
}

#[tokio::test]
async fn a_merge_after_a_remove_starts_a_fresh_fold() {
    let s = coalescing_shard();
    s.merge(1, 5).await.expect("merge before the remove");
    s.remove(&1).await.expect("remove");
    s.merge(1, 3).await.expect("merge after the remove");

    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        Some(3),
        "only the contribution made after the remove lands, not 5 + 3"
    );
}

#[tokio::test]
async fn a_clear_tombstones_a_key_once_when_it_is_stored_and_pending() {
    let s = coalescing_shard();
    s.insert(1, 10).await.expect("insert");
    s.merge(1, 5).await.expect("merge over the stored key");
    let mut events = s.events();

    s.clear().await.expect("clear");
    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        None,
        "neither the stored value nor the fold survives"
    );
    let mut removed = 0;
    while let Ok(event) = events.try_recv() {
        match event {
            Event::Removed { key: 1, .. } => removed += 1,
            other => panic!("only the one removal publishes: {other:?}"),
        }
    }
    assert_eq!(
        removed, 1,
        "a key that is stored and pending is removed once"
    );
}

#[tokio::test]
async fn a_clear_covers_stored_and_pending_keys_together() {
    let s = coalescing_shard();
    s.insert(1, 10).await.expect("stored key");
    s.merge(2, 5).await.expect("pending-only key");

    s.clear().await.expect("clear");
    s.flush_all_pending_merges();

    assert_eq!(s.get(&1).await, None);
    assert_eq!(s.get(&2).await, None);
}

#[tokio::test]
async fn an_insert_after_a_pending_merge_outranks_it_under_a_resolver_that_picks_a_side() {
    let s = shard::<u32, u32>(1).with_merge_coalesce_window(WINDOW);
    s.merge(1, 1).await.expect("merge");
    s.insert(1, 7).await.expect("insert after the merge call");

    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        Some(7),
        "the later call wins by version whether the earlier one flushed first or not"
    );
}

#[tokio::test]
async fn an_insert_after_a_pending_merge_folds_with_it_under_a_merging_resolver() {
    let s = coalescing_shard();
    s.merge(1, 5).await.expect("merge");
    s.insert(1, 7).await.expect("insert");

    s.flush_all_pending_merges();

    assert_eq!(s.get(&1).await, Some(12));
}

#[test]
fn pending_merge_superseded_orders_by_hlc() {
    let at = |wall_ms, logical, node| Hlc {
        wall_ms,
        logical,
        node: NodeId::from(node),
    };
    assert!(pending_merge_superseded(at(1, 0, 1), at(2, 0, 1)));
    assert!(pending_merge_superseded(at(5, 0, 1), at(5, 1, 1)));
    assert!(pending_merge_superseded(at(5, 0, 1), at(5, 0, 2)));
    assert!(
        !pending_merge_superseded(at(5, 0, 1), at(5, 0, 1)),
        "an equal version is the fold's own, not a newer tombstone"
    );
    assert!(!pending_merge_superseded(at(5, 1, 1), at(5, 0, 1)));
    assert!(!pending_merge_superseded(at(6, 0, 1), at(5, 9, 9)));
}

#[test]
fn folded_pending_ver_keeps_the_newer_stamp() {
    let at = |wall_ms, logical| Hlc {
        wall_ms,
        logical,
        node: NodeId::from(1),
    };
    assert_eq!(folded_pending_ver(at(1, 0), at(2, 0)), at(2, 0));
    assert_eq!(
        folded_pending_ver(at(2, 3), at(2, 1)),
        at(2, 3),
        "a call that stamped first but folded second does not lower the version"
    );
    assert_eq!(folded_pending_ver(at(2, 3), at(2, 3)), at(2, 3));
}

#[tokio::test]
async fn a_flushed_fold_carries_the_version_of_its_last_call() {
    let s = coalescing_shard();
    s.merge(1, 2).await.expect("merge 1");
    s.merge(1, 3).await.expect("merge 2");
    let (last_call, value) = s.pending_merge_snapshot(&1).expect("pending");
    assert_eq!(value, 5);

    s.flush_all_pending_merges();

    let record = s
        .engine
        .record_for(&key_bytes(&1u32), now_ms())
        .expect("the fold is stored");
    assert_eq!(
        record.ver, last_call,
        "the fold is stored at its last call's version"
    );
}

#[tokio::test]
async fn a_fold_stamped_before_a_tombstone_loses_to_it_even_when_it_inserts_after() {
    // The race a remove has with a merge: the merge stamped first, the
    // tombstone applied, and only then did the merge's fold reach the flush.
    let s = coalescing_shard();
    let call_ver = s.stamp_local();
    s.remove(&1).await.expect("remove");
    assert!(
        call_ver < s.stamp_local(),
        "the call stamped before the remove"
    );

    s.flush_one_pending_merge(
        1,
        PendingMerge {
            key_bytes: key_bytes(&1u32),
            value: 5,
            encoded: Bytes::from(postcard::to_stdvec(&5u32).expect("encode")),
            expires_at_ms: None,
            ver: call_ver,
            deadline_key: (0, 0),
        },
    );

    assert_eq!(
        s.get(&1).await,
        None,
        "the older fold loses to the tombstone"
    );
}

#[tokio::test]
async fn discarding_a_pending_merge_leaves_no_stale_deadline_row() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_clock(Arc::new(|| 0u64))
        .with_merge_coalesce_window(Duration::from_millis(100));
    s.merge(1, 5).await.expect("merge");
    assert_eq!(s.next_pending_merge_deadline_ms(), Some(100));

    s.remove(&1).await.expect("remove");
    assert_eq!(
        s.next_pending_merge_deadline_ms(),
        None,
        "the row left with the entry"
    );
    s.merge(1, 3).await.expect("merge after the remove");
    s.flush_due_pending_merges(99);
    assert_eq!(s.get(&1).await, None, "the new fold is not due early");
    s.flush_due_pending_merges(100);
    assert_eq!(s.get(&1).await, Some(3));
}

#[tokio::test]
async fn the_pending_count_is_zero_after_every_way_a_fold_leaves() {
    let s = coalescing_shard();
    assert_eq!(s.pending_merge_count(), 0);

    s.merge(1, 1).await.expect("merge");
    s.merge(1, 2)
        .await
        .expect("merge again folds into the same entry");
    s.merge(2, 1).await.expect("merge");
    s.merge(3, 1).await.expect("merge");
    s.merge(4, 1).await.expect("merge");
    s.merge(5, 1).await.expect("merge");
    assert_eq!(s.pending_merge_count(), 5);

    s.remove(&1).await.expect("remove");
    assert_eq!(s.pending_merge_count(), 4, "a remove");
    s.remove_many([2u32]).await.expect("remove_many");
    assert_eq!(s.pending_merge_count(), 3, "a remove_many");
    ShardOps::apply_remote(&s, tombstone_record(3, hlc(ahead_ms(), 2))).await;
    assert_eq!(s.pending_merge_count(), 2, "a newer remote tombstone");
    s.flush_due_pending_merges(u64::MAX);
    assert_eq!(s.pending_merge_count(), 0, "a due flush");

    s.merge(1, 1).await.expect("merge");
    s.merge(2, 1).await.expect("merge");
    s.clear().await.expect("clear");
    assert_eq!(s.pending_merge_count(), 0, "a clear");

    s.merge(1, 1).await.expect("merge");
    s.flush_all_pending_merges();
    assert_eq!(s.pending_merge_count(), 0, "a flush of everything");

    s.merge(9, 1).await.expect("merge");
    // Past the clock the remote tombstone above moved: stamped in the same
    // millisecond it would carry an older logical counter than the fold's
    // and lose to it.
    ShardOps::invalidate(&s, key_bytes(&9u32), hlc(ahead_ms() + 1_000, 2)).await;
    assert_eq!(
        s.pending_merge_count(),
        0,
        "an invalidation flushes the fold"
    );
}

#[tokio::test]
async fn a_remove_of_a_key_nothing_pends_for_leaves_other_folds_alone() {
    let s = coalescing_shard();
    s.merge(1, 5).await.expect("merge");

    s.remove(&2).await.expect("remove another key");

    assert_eq!(s.pending_merge_count(), 1);
    s.flush_all_pending_merges();
    assert_eq!(s.get(&1).await, Some(5));
}

/// A `Mode::Distributed` shard of five nodes that coalesces `merge` calls
/// through [`SumResolver`], with a key it owns and one it does not.
fn distributed_coalescing_shard() -> (Shard<u32, u32>, u32, u32) {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let (s, view, _tx) =
        distributed_shard::<u32, u32>(self_node, eligible, 2, Arc::new(ResidencySet::new()));
    let s = s
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(WINDOW);
    let owned = find_key_by_ownership(&view, true);
    let unowned = find_key_by_ownership(&view, false);
    (s, owned, unowned)
}

fn forwarded(items: Vec<FanOutItem<u32>>) -> Vec<WireRecord> {
    items
        .into_iter()
        .map(|item| match item {
            FanOutItem::Forward(rec) => rec,
            FanOutItem::Applied(key, _) => panic!("a non-owner queues only forwards: {key}"),
        })
        .collect()
}

#[tokio::test]
async fn a_non_owner_remove_forwards_only_the_tombstone_and_the_later_fold() {
    let (s, _owned, key) = distributed_coalescing_shard();
    s.merge(key, 2).await.expect("merge before the remove");
    s.remove(&key).await.expect("remove");
    s.merge(key, 3).await.expect("merge after the remove");
    s.flush_all_pending_merges();

    let recs = forwarded(s.fan_out_queue().drain());

    assert_eq!(
        recs.len(),
        2,
        "the tombstone, then the later fold: {recs:?}"
    );
    assert!(recs[0].is_tombstone(), "the remove goes first");
    assert_eq!(
        recs[1].value.as_deref(),
        Some(postcard::to_stdvec(&3u32).expect("encode").as_slice()),
        "the fold carries only the contribution made after the remove"
    );
    assert!(
        recs[1].ver > recs[0].ver,
        "the later fold is stamped after the tombstone"
    );
}

#[tokio::test]
async fn a_non_owner_clear_forwards_a_pending_fold_and_writes_no_tombstone() {
    let (s, _owned, key) = distributed_coalescing_shard();
    s.merge(key, 2).await.expect("merge");

    s.clear().await.expect("clear");
    s.flush_all_pending_merges();

    assert_eq!(s.pending_merge_count(), 0, "the clear settled the fold");
    let recs = forwarded(s.fan_out_queue().drain());
    assert_eq!(recs.len(), 1, "only the fold leaves: {recs:?}");
    assert!(
        !recs[0].is_tombstone(),
        "no tombstone reaches the owners: {recs:?}"
    );
    assert_eq!(recs[0].key, key_bytes(&key));
    assert_eq!(
        recs[0].value.as_deref(),
        Some(postcard::to_stdvec(&2u32).expect("encode").as_slice()),
        "the fold goes to the owners as the merge call made it"
    );
}

#[tokio::test]
async fn an_owner_clear_discards_a_pending_fold_and_tombstones_the_key() {
    let (s, key, unowned) = distributed_coalescing_shard();
    s.merge(key, 2).await.expect("merge an owned key");
    s.merge(unowned, 3).await.expect("merge an unowned key");

    s.clear().await.expect("clear");
    s.flush_all_pending_merges();

    assert_eq!(s.get(&key).await, None, "the owned key is removed");
    let queued = s.fan_out_queue().drain();
    let forwarded_keys: Vec<_> = queued
        .iter()
        .filter_map(|item| match item {
            FanOutItem::Forward(rec) => Some((rec.key.clone(), rec.is_tombstone())),
            FanOutItem::Applied(..) => None,
        })
        .collect();
    assert_eq!(
        forwarded_keys,
        [(key_bytes(&unowned), false)],
        "only the unowned key's fold is forwarded, as a value: {queued:?}"
    );
}

/// A key the node no longer owns but still stores, mid disown-grace, is held:
/// `clear` forwards its pending fold to the owners and then its tombstone, which
/// outranks the fold.
#[tokio::test]
async fn a_non_owner_clear_tombstones_a_disown_grace_copy_after_forwarding_its_fold() {
    let self_node = NodeId::from(1);
    let residency = Arc::new(ResidencySet::new());
    let (s, _view, tx) =
        distributed_shard::<u32, u32>(self_node, vec![self_node], 1, Arc::clone(&residency));
    let s = s
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(WINDOW);
    let moved = OwnershipView::compute(
        self_node,
        vec![self_node, NodeId::from(2)],
        NonZeroU8::new(1).expect("nonzero"),
    );
    let key = (0..1_000_000u32)
        .find(|&k| !moved.owns(PartId::of_key(&key_bytes(&k))))
        .expect("some bucket flips ownership once a second node joins");
    s.insert(key, 7)
        .await
        .expect("the sole owner stores the key");
    tx.send(Arc::new(moved)).expect("receiver still alive");
    residency.mark_releasing(&[PartId::of_key(&key_bytes(&key))]);
    s.merge(key, 2).await.expect("the non-owner's merge waits");
    assert_eq!(s.keys(), [key], "the grace copy is still stored here");
    assert_eq!(s.pending_merge_count(), 1);
    s.fan_out_queue().drain();

    s.clear().await.expect("clear");

    assert_eq!(s.pending_merge_count(), 0);
    let recs = forwarded(s.fan_out_queue().drain());
    assert_eq!(
        recs.len(),
        2,
        "the fold, then the key's tombstone: {recs:?}"
    );
    assert!(!recs[0].is_tombstone(), "the fold goes first, as a value");
    assert!(recs[1].is_tombstone(), "the grace copy is tombstoned");
    assert_eq!(recs[0].key, key_bytes(&key));
    assert_eq!(recs[1].key, key_bytes(&key));
    assert!(
        recs[1].ver > recs[0].ver,
        "the tombstone outranks the fold at the owners"
    );
}

#[tokio::test]
async fn an_owner_discards_its_pending_fold_for_a_forwarded_newer_tombstone() {
    let (s, key, _unowned) = distributed_coalescing_shard();
    s.merge(key, 5).await.expect("merge");

    ShardOps::apply_remote(&s, tombstone_record(key, hlc(ahead_ms(), 2))).await;
    s.flush_all_pending_merges();

    assert_eq!(s.get(&key).await, None);
    assert_eq!(s.pending_merge_count(), 0);
}

#[tokio::test]
async fn an_owner_holding_a_newer_tombstone_rejects_a_fold_stamped_before_it() {
    let (s, key, _unowned) = distributed_coalescing_shard();
    let call_ver = s.stamp_local();
    s.remove(&key).await.expect("remove");

    // What a non-owner's pending fold forwards once it flushes: the call's
    // own version, which the owner's later tombstone outranks.
    ShardOps::apply_remote(
        &s,
        WireRecord {
            key: key_bytes(&key),
            value: Some(Bytes::from(postcard::to_stdvec(&5u32).expect("encode"))),
            ver: call_ver,
            expires_at_ms: None,
        },
    )
    .await;

    assert_eq!(s.get(&key).await, None);
}

/// Runs `flush` on its own thread, parked between the take and the apply of
/// its first stripe until the returned sender fires. Returns once the flush is
/// parked there.
fn park_a_flush(
    s: &Arc<Shard<u32, u32>>,
    flush: impl FnOnce(&Shard<u32, u32>) + Send + 'static,
) -> (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let (parked_tx, parked_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    s.set_after_take_hook(move || {
        let _ = parked_tx.send(());
        let _ = release_rx.lock().expect("hook lock").recv();
    });
    let flusher = {
        let s = Arc::clone(s);
        std::thread::spawn(move || flush(&s))
    };
    parked_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the flush parks between its take and its apply");
    (release_tx, flusher)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clear_waits_for_a_flush_between_its_take_and_its_apply() {
    let s = Arc::new(coalescing_shard());
    s.merge(1, 5).await.expect("merge");
    let (release, flusher) = park_a_flush(&s, Shard::flush_all_pending_merges);
    assert_eq!(
        s.pending_merge_count(),
        0,
        "the parked flush has taken the fold: it is pending nowhere"
    );
    assert!(s.keys().is_empty(), "and stored nowhere");

    let clearing = {
        let s = Arc::clone(&s);
        tokio::spawn(async move { s.clear().await })
    };
    // Long enough that a clear which does not wait for the flush has
    // finished and listed nothing.
    tokio::time::sleep(Duration::from_millis(200)).await;
    release.send(()).expect("release the flush");
    flusher.join().expect("the flush finishes");
    clearing.await.expect("clear task").expect("clear succeeds");

    assert_eq!(
        s.get(&1).await,
        None,
        "the fold the flush had taken lands under the clear's tombstone"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clear_waits_for_a_due_flush_and_for_an_invalidation_flush() {
    let s = Arc::new(
        shard::<u32, u32>(1)
            .with_resolver(Arc::new(SumResolver))
            .with_clock(Arc::new(|| 0u64))
            .with_merge_coalesce_window(Duration::from_millis(100)),
    );
    s.merge(1, 5).await.expect("merge");
    let (release, flusher) = park_a_flush(&s, |s| s.flush_due_pending_merges(100));

    let clearing = {
        let s = Arc::clone(&s);
        tokio::spawn(async move { s.clear().await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    release.send(()).expect("release the flush");
    flusher.join().expect("the flush finishes");
    clearing.await.expect("clear task").expect("clear succeeds");
    assert_eq!(s.get(&1).await, None, "the due flush's fold is cleared");

    // A peer's bare invalidation applies the fold first, under the same gate.
    let s = Arc::new(coalescing_shard());
    s.merge(2, 5).await.expect("merge");
    let key_hash = engine::hash_key_bytes(key_bytes(&2u32).as_ref());
    let newer = hlc(ahead_ms(), 2);
    let (release, flusher) = park_a_flush(&s, move |s| {
        s.flush_pending_merge_before(&2, key_hash, newer);
    });
    let clearing = {
        let s = Arc::clone(&s);
        tokio::spawn(async move { s.clear().await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    release.send(()).expect("release the flush");
    flusher.join().expect("the flush finishes");
    clearing.await.expect("clear task").expect("clear succeeds");
    assert_eq!(
        s.get(&2).await,
        None,
        "the invalidation flush's fold is cleared"
    );
}

#[test]
fn a_clear_tombstones_only_the_pending_keys_it_owns() {
    let (owned, foreign) =
        crate::store::shard_merge::split_pending_for_clear([(1u32, true), (2, false), (3, true)]);
    assert_eq!(owned, [1, 3], "owned folds are discarded and tombstoned");
    assert_eq!(foreign, [2], "an unowned fold is applied, never tombstoned");

    let (owned, foreign) =
        crate::store::shard_merge::split_pending_for_clear(Vec::<(u32, bool)>::new());
    assert!(owned.is_empty() && foreign.is_empty());
}
