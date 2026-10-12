//! Merge resolution at the shard layer: stored merges, the coalesce window and pending-merge flushes.

use super::*;
use crate::store::shard_merge::{drain_due_deadlines, merge_deadline_ms, merge_window_elapsed};

#[tokio::test]
async fn merge_against_a_tombstone_is_never_consulted_and_falls_back_to_hlc_order() {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from(postcard::to_stdvec(&99u32).expect("encode")),
        expires_at_ms: None,
    }));
    // Subscribed before the setup write below: a receiver created after
    // it fires would never see it and `recv().await` would hang.
    let mut events = s.events();

    // A bare tombstone for a key that has never existed: `stored_ver` is
    // `None` going in, so this never consults `AlwaysMerge` at all: it
    // would otherwise see its own tombstone as a value-less "incoming"
    // and have the engine's guard skip `merge`, same as the colliding
    // write below, which is exactly what this test means to isolate
    // instead.
    ShardOps::apply_remote(
        &s,
        WireRecord {
            key: key_bytes(&1u32),
            value: None,
            ver: hlc(1, 1),
            expires_at_ms: None,
        },
    )
    .await;
    assert_eq!(
        s.get(&1).await,
        None,
        "tombstoned before the colliding write arrives"
    );
    let _ = events
        .recv()
        .await
        .expect("Removed for the setup tombstone");

    // Genuinely newer than the tombstone by `Hlc`: the tombstone's
    // value-less side means `merge` is never consulted no matter what
    // `merges()` claims, so this lands via the ordinary Hlc-order
    // `winner` fallback, with its own real value, never `AlwaysMerge`'s
    // synthesized 99.
    let incoming = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&42u32).expect("encode"))),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, incoming).await;

    assert_eq!(
        s.get(&1).await,
        Some(42),
        "the incoming write's own value lands via the ordinary winner fallback, never the \
         resolver's synthesized merge bytes, since a tombstone's value-less side means \
         merge is never consulted"
    );
    match events
        .recv()
        .await
        .expect("an event for the colliding write")
    {
        Event::Created { key, value, origin } => {
            assert_eq!(key, 1);
            assert_eq!(value, 42);
            assert_eq!(origin, Origin::Remote(NodeId::from(2)));
        }
        other => panic!("expected Event::Created for a write over a tombstone, got {other:?}"),
    }
}

/// A spilled stored side carries a real value once `apply_locked`
/// reads its bytes back off disk (see `engine::apply::read_spilled_for_conflict`):
/// `AlwaysMerge`'s `merge` always returns `Some`, and now that both
/// sides have real values, `resolve_conflict`'s guard has nothing to
/// stop it, so the merge applies exactly as it would against a resident
/// stored side. `merge_against_a_tombstone_is_never_consulted_and_falls_back_to_hlc_order`,
/// just above, is the guard's remaining case: a tombstone is genuinely
/// value-less, spilled or not, and `merge` is never consulted for it.
#[cfg(feature = "spill")]
#[tokio::test]
async fn merge_against_a_spilled_side_folds_once_its_bytes_are_read_back() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-merge-spill-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);

    let s = Shard::<u32, u32>::new(
        SmolStr::new("merge-spill-test"),
        Mode::Local,
        NodeId::from(1),
        1, // capacity 1: the second insert spills the first
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("the tier's directory and region files open")
    .with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from(postcard::to_stdvec(&999u32).expect("encode")),
        expires_at_ms: None,
    }));

    s.insert(1, 10).await.expect("insert 1");
    s.insert(2, 20).await.expect("insert 2");

    // Eviction to disk runs on the flusher thread, off this call's path,
    // so poll rather than assume it is already done. Polls with
    // `get_sync`, never `get`: `get` promotes a spilled entry back to
    // residency on a hit (see its docs), which would silently undo the
    // very spilled state this test needs in place when the colliding
    // write arrives below.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let spilled_key = loop {
        if s.get_sync(&1).is_none() {
            break 1u32;
        }
        if s.get_sync(&2).is_none() {
            break 2u32;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "neither key spilled within the poll bound"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    // `get_sync` never promotes, unlike `get`: reading the key here
    // would either promote the spilled entry or require touching disk,
    // either way risking exactly the promotion this test must avoid
    // before the colliding write below.
    assert!(
        s.get_sync(&spilled_key).is_none(),
        "the key must still be spilled when the colliding write arrives"
    );

    let incoming = WireRecord {
        key: key_bytes(&spilled_key),
        value: Some(Bytes::from(postcard::to_stdvec(&123u32).expect("encode"))),
        ver: hlc(ahead_ms(), 9),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, incoming).await;

    // `get` here, after the collision has already been resolved, reads
    // back whatever the merge actually stored.
    assert_eq!(
        s.get(&spilled_key).await,
        Some(999),
        "apply_locked reads the spilled stored side's real bytes back before consulting \
         the resolver, so a Merged reply against it applies exactly as it would against a \
         resident side, rather than degrading to the guard's value-less fallback"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn merge_with_bytes_that_fail_to_decode_rejects_the_incoming_record() {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from_static(&[0x80]), // truncated varint: never decodes as u32
        expires_at_ms: None,
    }));
    s.insert(1, 7).await.expect("insert");
    let (_, weight_before) = s.engine.debug_totals();

    let incoming = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&5u32).expect("encode"))),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, incoming).await;

    assert_eq!(
        s.get(&1).await,
        Some(7),
        "a Merged reply whose bytes fail to decode as V is rejected, not stored"
    );
    let (_, weight_after) = s.engine.debug_totals();
    assert_eq!(
        weight_before, weight_after,
        "a rejected decode failure leaves the engine's bookkeeping untouched"
    );
}

#[tokio::test]
async fn merged_outcome_stores_the_merged_bytes_under_a_merge_derived_version_and_publishes_one_event()
 {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from(postcard::to_stdvec(&42u32).expect("encode")),
        expires_at_ms: None,
    }));
    // Subscribed before the setup insert: a receiver created after it
    // fires would never see it and `recv().await` would hang.
    let mut events = s.events();
    s.insert(1, 7).await.expect("insert");
    let _ = events.recv().await.expect("Created for the setup insert");

    let incoming = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&5u32).expect("encode"))),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, incoming).await;

    assert_eq!(
        s.get(&1).await,
        Some(42),
        "the resolver's merged value lands, not either input's raw value"
    );

    match events.recv().await.expect("exactly one event is published") {
        Event::Updated { key, value, origin } => {
            assert_eq!(key, 1);
            assert_eq!(value, 42);
            assert_eq!(origin, Origin::Remote(NodeId::from(2)));
        }
        other => panic!("expected Event::Updated for a merge, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "a merge outcome publishes exactly one event, never a second for the same write"
    );

    let stored = s
        .engine
        .record_for(key_bytes(&1u32).as_ref(), 0)
        .expect("merged key is live");
    assert!(
        stored.ver.node.is_merge_derived(),
        "the merged bytes match neither side's own bytes, so the engine mints a fresh \
         version no real node's clock could produce"
    );
}

/// A resolver whose merge sums both sides as `u32`, so repeated folding,
/// [`Shard::merge`]'s in-memory coalescing included, is observable
/// as an actual running total rather than just "some merge happened".
/// Falls back to plain `Hlc` order when either side lacks a value,
/// matching every other merging resolver's tombstone/spill guard.
#[derive(Debug, Clone, Copy, Default)]
struct SumResolver;

impl ConflictResolver for SumResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return None;
        };
        let (Ok(x), Ok(y)) = (
            postcard::from_bytes::<u32>(av),
            postcard::from_bytes::<u32>(bv),
        ) else {
            return None;
        };
        Some(Merged {
            value: Bytes::from(postcard::to_stdvec(&(x + y)).expect("u32 always encodes")),
            expires_at_ms: None,
        })
    }
}

/// A resolver that keeps whichever side decodes to the greater `u32` and
/// never overrides [`ConflictResolver::merge`]: legal per
/// [`ConflictResolver::merges`]'s own doc ("`true` is always correct to
/// return, even for a resolver whose `merge` never actually returns
/// `Some`") and exactly the shape needed to drive [`Shard::merge`]'s
/// pending fold through a real `Winner::A`/`Winner::B` decision between
/// two real `Put` values, rather than only through the tombstone/decode-
/// failure fallback every other test resolver's `A`/`B` arm takes.
#[derive(Debug, Clone, Copy, Default)]
struct MaxWinsResolver;

impl ConflictResolver for MaxWinsResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return if a.ver >= b.ver { Winner::A } else { Winner::B };
        };
        match (
            postcard::from_bytes::<u32>(av),
            postcard::from_bytes::<u32>(bv),
        ) {
            (Ok(x), Ok(y)) if x >= y => Winner::A,
            (Ok(_), Ok(_)) => Winner::B,
            _ => {
                if a.ver >= b.ver {
                    Winner::A
                } else {
                    Winner::B
                }
            }
        }
    }

    fn merges(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn merge_a_losing_calls_own_version_never_overwrites_the_pending_entrys_version() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(MaxWinsResolver))
        .with_merge_coalesce_window(Duration::from_secs(60));

    s.merge(1, 10).await.expect("merge 1: the eventual winner");
    let (ver_after_win, _) = s
        .pending_merge_snapshot(&1)
        .expect("pending after the first call");

    // Decodes to a smaller `u32`, so `MaxWinsResolver` picks `Winner::A`
    // (the already-pending 10 over this incoming 3): a losing call.
    s.merge(1, 3).await.expect("merge 2: loses against 10");

    let (ver_now, value_now) = s
        .pending_merge_snapshot(&1)
        .expect("still pending: the window hasn't elapsed");
    assert_eq!(
        ver_now, ver_after_win,
        "a losing merge call must not advance the pending entry's version, mirroring \
         resolve_and_rebind's IncomingLoses no-op in the engine — the record's stored \
         version must keep describing its own content"
    );
    assert_eq!(
        value_now, 10,
        "nor may it touch the pending entry's content"
    );

    s.flush_all_pending_merges();
    assert_eq!(
        s.get(&1).await,
        Some(10),
        "the losing call never overwrote the winner's value once flushed either"
    );
}

/// [`Engine::apply_many`]'s pre-fold (`engine::prefold::prefold_batch`) collapses
/// several same-key records in one `apply_remote_batch` call into one
/// real apply, so a fan-out batch carrying distinct origin nodes'
/// writes to the same key publishes exactly one event, not one per
/// contributing record, the pre-pre-fold behavior, and that event
/// names only the run's last record's origin, documented on
/// [`Origin::Remote`] as an accepted cost of the fold.
#[tokio::test]
async fn apply_remote_batch_prefold_publishes_one_event_naming_the_runs_last_origin() {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(SumResolver));
    let mut events = s.events();

    let rec = |node: u64, wall_ms: u64, value: u32| WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec(&value).expect("encode"))),
        ver: hlc(wall_ms, node),
        expires_at_ms: None,
    };

    // Three records for one key, from three distinct origin nodes, in
    // one batch: the shape a peer's fan-out or anti-entropy-pull reply
    // bundling several nodes' writes to the same key takes.
    let recs = vec![rec(2, 1, 2), rec(3, 2, 3), rec(4, 3, 4)];
    ShardOps::apply_remote_batch(&s, recs).await;

    assert_eq!(
        s.get(&1).await,
        Some(9),
        "pre-fold still stores the full merge of every record in the run"
    );

    match events
        .recv()
        .await
        .expect("one event for the whole folded run")
    {
        Event::Created { key, value, origin } => {
            assert_eq!(key, 1);
            assert_eq!(value, 9);
            assert_eq!(
                origin,
                Origin::Remote(NodeId::from(4)),
                "the folded run's event names the run's last record's origin, not \
                 every contributor's"
            );
        }
        other => {
            panic!("expected exactly one Event::Created for the folded run, got {other:?}")
        }
    }
    assert!(
        events.try_recv().is_err(),
        "three same-key records in one fan-out batch publish exactly one event under \
         pre-fold, not three"
    );
}

#[test]
fn merge_deadline_ms_adds_the_window_to_the_start_time() {
    assert_eq!(merge_deadline_ms(1_000, Duration::from_millis(250)), 1_250);
    assert_eq!(
        merge_deadline_ms(u64::MAX, Duration::from_millis(1)),
        u64::MAX,
        "saturates rather than wrapping"
    );
}

#[test]
fn merge_window_elapsed_is_true_at_and_after_the_deadline_only() {
    assert!(!merge_window_elapsed(999, 1_000), "before the deadline");
    assert!(
        merge_window_elapsed(1_000, 1_000),
        "exactly at the deadline"
    );
    assert!(merge_window_elapsed(1_001, 1_000), "past the deadline");
}

#[test]
fn drain_due_deadlines_pops_only_the_entries_actually_due_in_deadline_order() {
    let mut by_deadline = BTreeMap::from([
        ((100, 0), "a"),
        ((50, 0), "b"),
        ((100, 1), "c"),
        ((200, 0), "d"),
    ]);

    let due = drain_due_deadlines(&mut by_deadline, 100);
    assert_eq!(
        due,
        vec!["b", "a", "c"],
        "every entry at or before the deadline, in ascending deadline order, seq breaking \
         the tie between the two sharing deadline 100"
    );
    assert_eq!(
        by_deadline,
        BTreeMap::from([((200, 0), "d")]),
        "an entry past the deadline is left untouched"
    );
}

#[test]
fn drain_due_deadlines_is_empty_when_nothing_is_due_or_pending() {
    let due = drain_due_deadlines(&mut BTreeMap::<(u64, u64), u32>::new(), 1_000);
    assert!(due.is_empty(), "{due:?}");

    let mut by_deadline = BTreeMap::from([((1_000, 0), 1u32)]);
    assert!(
        drain_due_deadlines(&mut by_deadline, 999).is_empty(),
        "nothing is due yet"
    );
    assert_eq!(by_deadline.len(), 1, "the not-yet-due entry stays put");
}

#[tokio::test]
async fn merge_with_a_zero_window_applies_at_once_like_insert() {
    let s = shard::<u32, u32>(1).with_resolver(Arc::new(SumResolver));
    let mut events = s.events();

    s.merge(1, 5).await.expect("merge");
    assert_eq!(
        s.get(&1).await,
        Some(5),
        "a zero coalesce window applies every merge call immediately"
    );
    match events
        .recv()
        .await
        .expect("one event for the immediate apply")
    {
        Event::Created { key, value, .. } => {
            assert_eq!(key, 1);
            assert_eq!(value, 5);
        }
        other => panic!("expected Event::Created, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_within_a_window_folds_in_memory_and_applies_once() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(Duration::from_secs(60));
    let mut events = s.events();

    s.merge(1, 2).await.expect("merge 1");
    s.merge(1, 3).await.expect("merge 2");
    s.merge(1, 4).await.expect("merge 3");

    assert_eq!(
        s.get(&1).await,
        None,
        "nothing has applied yet: the 60s window has not elapsed"
    );
    assert!(
        events.try_recv().is_err(),
        "nothing applies, so nothing publishes, before the flush"
    );

    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        Some(9),
        "the fold of all three calls (2 + 3 + 4) lands as one write"
    );
    match events
        .recv()
        .await
        .expect("exactly one event for the whole window")
    {
        Event::Created { key, value, .. } => {
            assert_eq!(key, 1);
            assert_eq!(value, 9);
        }
        other => panic!("expected one Event::Created for the fold, got {other:?}"),
    }
    assert!(
        events.try_recv().is_err(),
        "three merge calls in one window publish exactly one event, not three"
    );
}

#[tokio::test]
async fn merge_does_not_read_the_stored_value_before_the_window_flushes() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(Duration::from_secs(60));
    s.insert(1, 100).await.expect("setup insert");

    s.merge(1, 5).await.expect("merge");
    assert_eq!(
        s.get(&1).await,
        Some(100),
        "merge never reads the stored value before its window flushes: the pending \
         fold, not the sum, is what's in memory until then"
    );

    s.flush_all_pending_merges();
    assert_eq!(
        s.get(&1).await,
        Some(105),
        "the flush's ordinary apply folds the pending value against what was stored, \
         through the same resolver"
    );
}

#[tokio::test]
async fn flush_due_pending_merges_only_flushes_once_the_deadline_is_reached() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_clock(Arc::new(|| 0u64))
        .with_merge_coalesce_window(Duration::from_millis(100));

    s.merge(1, 1).await.expect("merge");
    let deadline = s.next_pending_merge_deadline_ms().expect("one key pending");
    assert_eq!(deadline, 100);

    s.flush_due_pending_merges(deadline - 1);
    assert_eq!(s.get(&1).await, None, "not due yet");

    s.flush_due_pending_merges(deadline);
    assert_eq!(s.get(&1).await, Some(1), "due exactly at the deadline");
}

#[tokio::test]
async fn merge_flushes_pending_folds_on_drop() {
    let s = shard::<u32, u32>(1)
        .with_resolver(Arc::new(SumResolver))
        .with_merge_coalesce_window(Duration::from_secs(60));
    // Subscribed before the merges below: a receiver created after they
    // fire would never see the flush's event.
    let mut events = s.events();

    s.merge(1, 2).await.expect("merge 1");
    s.merge(1, 3).await.expect("merge 2");
    assert!(
        events.try_recv().is_err(),
        "still pending: nothing has applied yet"
    );

    drop(s);

    match events.recv().await.expect("drop flushes the pending fold") {
        Event::Created { key, value, .. } => {
            assert_eq!(key, 1);
            assert_eq!(value, 5);
        }
        other => panic!("expected Event::Created from the drop flush, got {other:?}"),
    }
}

#[tokio::test]
async fn merge_falls_back_to_hlc_order_when_the_resolver_does_not_merge() {
    // `Shard::with_merge_coalesce_window` is a raw setter with no
    // validation of its own: `crate::cache::CacheBuilder` is the
    // validated entry point, so a non-merging resolver combined with a
    // window must still behave sanely: the pending fold degrades to
    // picking a side, never panicking.
    let s = shard::<u32, u32>(1).with_merge_coalesce_window(Duration::from_secs(60));

    s.merge(1, 1).await.expect("merge 1");
    s.merge(1, 2).await.expect("merge 2");
    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await,
        Some(2),
        "LwwResolver::winner picks a side by Hlc order; the later call's own stamp wins"
    );
}
