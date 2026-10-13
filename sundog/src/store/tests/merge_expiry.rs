//! Expiry of merged values at the shard layer: no resurrection, merged deadlines, `expire`/`touch`/`persist` and `merge_with_ttl`.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};

const T0: u64 = 1_000_000;

/// A `Mode::Replicated` counter shard for `node` on a clock the test sets,
/// merging through [`PnCounterResolver`].
fn counter_shard(
    node: u64,
    default_ttl: Option<Duration>,
    clock: &Arc<AtomicU64>,
) -> Shard<u32, PnCounter> {
    let reader = Arc::clone(clock);
    Shard::<u32, PnCounter>::new(
        SmolStr::new("test-merge-expiry"),
        Mode::Replicated,
        NodeId::from(node),
        10_000,
        default_ttl,
        None,
    )
    .with_resolver(Arc::new(PnCounterResolver))
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
}

fn writer(node: u64) -> WriterId {
    WriterId::new(NodeId::from(node), 1)
}

fn delta(node: u64, by: u64) -> PnCounter {
    PnCounter::local_delta(writer(node), by)
}

fn counter_value(shard: &Shard<u32, PnCounter>) -> Option<i128> {
    shard.get_sync(&1).map(|c| c.value())
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[tokio::test]
async fn a_merge_over_an_expired_unswept_counter_starts_a_new_value() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, Some(secs(1)), &clock);
    s.merge(1, delta(11, 5)).await.expect("merge");
    assert_eq!(counter_value(&s), Some(5));

    clock.store(T0 + 2_000, Ordering::SeqCst);
    assert_eq!(counter_value(&s), None, "the counter has expired");
    s.merge(1, delta(12, 1)).await.expect("merge");

    assert_eq!(
        counter_value(&s),
        Some(1),
        "the expired contribution does not fold into the new write"
    );
    assert_eq!(
        s.ttl_of(&1),
        Some(Ttl::Remaining(secs(1))),
        "the new value carries the default TTL"
    );
}

/// A record stamped before the stored one's deadline, delivered after this
/// node's clock passed that deadline, still never folds into the dead value.
#[tokio::test]
async fn a_remote_record_over_a_locally_expired_counter_starts_a_new_value() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, None, &clock);
    let record = |node: u64, by: u64, wall_ms: u64, expires_at_ms: u64| WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(delta(node, by).encode().expect("encodes"))),
        ver: hlc(wall_ms, node),
        expires_at_ms: Some(expires_at_ms),
    };
    ShardOps::apply_remote(&s, record(11, 5, T0, T0 + 1_000)).await;
    assert_eq!(counter_value(&s), Some(5));

    clock.store(T0 + 2_000, Ordering::SeqCst);
    assert_eq!(counter_value(&s), None);
    ShardOps::apply_remote(&s, record(12, 1, T0 + 500, T0 + 9_000)).await;

    assert_eq!(counter_value(&s), Some(1));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(7))));
}

/// A node whose clock stamps run ahead of its physical clock by more than the
/// TTL (it absorbed a fast peer's stamp, inside `max_clock_skew`) still folds
/// its writes: a deadline is minted past the write's own stamp.
#[tokio::test]
async fn a_merge_folds_when_the_stamp_clock_leads_the_physical_clock_by_more_than_the_ttl() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, Some(secs(10)), &clock);
    let fast_peer = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(delta(11, 5).encode().expect("encodes"))),
        ver: hlc(T0 + 30_000, 11),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, fast_peer).await;
    assert_eq!(counter_value(&s), Some(5));

    s.merge(1, delta(12, 1)).await.expect("merge");
    clock.store(T0 + 1_000, Ordering::SeqCst);
    s.merge(1, delta(13, 2)).await.expect("merge");

    assert_eq!(
        counter_value(&s),
        Some(8),
        "every write folds although its stamp leads the physical clock by 30 s"
    );
    let record = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert!(
        record[0].expires_at_ms.expect("a deadline") > record[0].ver.wall_ms,
        "the deadline lies past the stamp of the write that set it"
    );
}

#[tokio::test]
async fn a_merge_after_the_old_value_was_swept_starts_a_new_value() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, Some(secs(1)), &clock);
    s.merge(1, delta(11, 5)).await.expect("merge");

    clock.store(T0 + 2_000, Ordering::SeqCst);
    assert_eq!(
        s.entry_count().await,
        0,
        "the sweep drops the expired counter"
    );
    s.merge(1, delta(12, 1)).await.expect("merge");
    assert_eq!(counter_value(&s), Some(1));
}

#[tokio::test]
async fn a_key_merged_twice_under_a_default_ttl_expires() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, Some(secs(1)), &clock);
    s.merge(1, delta(11, 3)).await.expect("merge");
    clock.store(T0 + 500, Ordering::SeqCst);
    s.merge(1, delta(12, 4)).await.expect("merge");

    assert_eq!(counter_value(&s), Some(7));
    assert_eq!(
        s.ttl_of(&1),
        Some(Ttl::Remaining(secs(1))),
        "the merged record keeps the later deadline, the second write's"
    );

    clock.store(T0 + 1_500, Ordering::SeqCst);
    assert_eq!(counter_value(&s), None, "the merged value expires");
    assert_eq!(s.ttl_of(&1), None);
}

#[tokio::test]
async fn a_merge_never_shortens_the_stored_deadline() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, None, &clock);
    s.merge_with_ttl(1, delta(11, 3), secs(60))
        .await
        .expect("merge");
    s.merge_with_ttl(1, delta(12, 4), secs(10))
        .await
        .expect("merge");

    assert_eq!(counter_value(&s), Some(7));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(60))));
}

#[tokio::test]
async fn an_unset_side_defers_to_the_set_deadline() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, None, &clock);
    s.merge(1, delta(11, 3)).await.expect("merge");
    assert_eq!(s.ttl_of(&1), Some(Ttl::Never));

    s.merge_with_ttl(1, delta(12, 4), secs(30))
        .await
        .expect("merge");
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(30))));

    s.merge(1, delta(13, 5)).await.expect("merge");
    assert_eq!(
        s.ttl_of(&1),
        Some(Ttl::Remaining(secs(30))),
        "a write without a TTL leaves the deadline in place"
    );
    assert_eq!(counter_value(&s), Some(12));
}

#[tokio::test]
async fn expire_extends_a_merged_deadline_and_never_shortens_it() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, None, &clock);
    s.merge(1, delta(11, 3)).await.expect("merge");

    assert!(s.expire(&1, secs(60)).await.expect("expire"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(60))));
    assert!(s.expire(&1, secs(10)).await.expect("expire"));
    assert_eq!(
        s.ttl_of(&1),
        Some(Ttl::Remaining(secs(60))),
        "a shorter expire leaves the later deadline"
    );
    assert!(s.expire(&1, secs(120)).await.expect("expire"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(120))));
    assert_eq!(counter_value(&s), Some(3), "the value is untouched");
}

#[tokio::test]
async fn persist_leaves_a_merged_deadline_unchanged() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, Some(secs(10)), &clock);
    s.merge(1, delta(11, 3)).await.expect("merge");
    s.merge(1, delta(12, 4)).await.expect("merge");

    assert!(s.persist(&1).await.expect("persist"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(10))));
    assert_eq!(counter_value(&s), Some(7));

    clock.store(T0 + 11_000, Ordering::SeqCst);
    assert_eq!(counter_value(&s), None, "the value still expires");
}

#[tokio::test]
async fn touch_extends_a_merged_deadline_by_the_default_ttl() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, Some(secs(10)), &clock);
    s.merge(1, delta(11, 3)).await.expect("merge");
    s.merge(1, delta(12, 4)).await.expect("merge");

    clock.store(T0 + 4_000, Ordering::SeqCst);
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(6))));
    assert!(s.touch(&1).await.expect("touch"));
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(10))));
    assert_eq!(counter_value(&s), Some(7));
}

#[tokio::test]
async fn merge_with_ttl_overrides_the_default_ttl_for_that_contribution() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, Some(secs(10)), &clock);
    s.merge_with_ttl(1, delta(11, 3), secs(60))
        .await
        .expect("merge");
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(60))));

    s.merge(2_u32, delta(11, 3)).await.expect("merge");
    assert_eq!(
        s.ttl_of(&2),
        Some(Ttl::Remaining(secs(10))),
        "merge keeps the default TTL"
    );
}

#[tokio::test]
async fn merge_with_ttl_replicates_the_deadline_with_the_record() {
    let clock = Arc::new(AtomicU64::new(T0));
    let a = counter_shard(1, None, &clock);
    let b = counter_shard(2, None, &clock);
    a.merge_with_ttl(1, delta(11, 3), secs(30))
        .await
        .expect("merge");

    let records = ShardOps::records_for(&a, vec![key_bytes(&1u32)]).await;
    assert_eq!(records[0].expires_at_ms, Some(T0 + 30_000));
    ShardOps::apply_remote_batch(&b, records).await;
    assert_eq!(b.ttl_of(&1), Some(Ttl::Remaining(secs(30))));
    assert_eq!(counter_value(&b), Some(3));
}

/// A `Mode::Distributed` non-owner forwards a `merge_with_ttl` contribution
/// to the owners with its deadline and applies nothing locally.
#[tokio::test]
async fn merge_with_ttl_on_a_non_owner_forwards_the_record_with_its_deadline() {
    let clock = Arc::new(AtomicU64::new(T0));
    let reader = Arc::clone(&clock);
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let (s, view, _tx) =
        distributed_shard::<u32, PnCounter>(self_node, eligible, 2, Arc::new(ResidencySet::new()));
    let s = s
        .with_resolver(Arc::new(PnCounterResolver))
        .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)));
    let unowned_key = find_key_by_ownership(&view, false);

    s.merge_with_ttl(unowned_key, delta(11, 3), secs(30))
        .await
        .expect("merge_with_ttl");

    assert!(
        s.get_sync(&unowned_key).is_none(),
        "a non-owner applies nothing locally"
    );
    let forwarded = s.fan_out_queue().drain();
    let [FanOutItem::Forward(rec)] = forwarded.as_slice() else {
        panic!("one record is forwarded: {forwarded:?}");
    };
    assert_eq!(rec.key.as_ref(), key_bytes(&unowned_key).as_ref());
    assert_eq!(
        rec.expires_at_ms,
        Some(T0 + 30_000),
        "the forwarded record carries the call's deadline"
    );
    let forwarded_value = PnCounter::decode(rec.value.as_ref().expect("a live record"))
        .expect("the forwarded value decodes");
    assert_eq!(forwarded_value.value(), 3);
}

#[tokio::test]
async fn merge_with_ttl_reports_the_errors_merge_does() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, None, &clock).with_max_frame(8);
    let result = s.merge_with_ttl(1, delta(11, 3), secs(30)).await;
    assert!(
        matches!(result, Err(CacheError::ValueTooLarge { .. })),
        "a value over the frame cap is refused: {result:?}"
    );
    assert_eq!(counter_value(&s), None);
}

#[tokio::test]
async fn the_coalescing_window_keeps_the_later_deadline() {
    for (first, second) in [
        (Some(secs(30)), Some(secs(10))),
        (Some(secs(10)), Some(secs(30))),
        (None, Some(secs(30))),
        (Some(secs(30)), None),
    ] {
        let clock = Arc::new(AtomicU64::new(T0));
        let s = counter_shard(1, None, &clock).with_merge_coalesce_window(secs(3_600));
        for (node, ttl, by) in [(11, first, 3), (12, second, 4)] {
            match ttl {
                Some(ttl) => s.merge_with_ttl(1, delta(node, by), ttl).await,
                None => s.merge(1, delta(node, by)).await,
            }
            .expect("merge");
        }
        s.flush_all_pending_merges();

        assert_eq!(counter_value(&s), Some(7));
        assert_eq!(
            s.ttl_of(&1),
            Some(Ttl::Remaining(secs(30))),
            "pending deadlines {first:?} then {second:?}"
        );
    }
}

#[tokio::test]
async fn the_coalescing_window_replaces_a_pending_value_that_lapsed() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, None, &clock).with_merge_coalesce_window(secs(3_600));
    s.merge_with_ttl(1, delta(11, 3), secs(1))
        .await
        .expect("merge");
    clock.store(T0 + 2_000, Ordering::SeqCst);
    s.merge_with_ttl(1, delta(12, 4), secs(10))
        .await
        .expect("merge");
    s.flush_all_pending_merges();

    assert_eq!(
        counter_value(&s),
        Some(4),
        "the lapsed pending value does not fold into the new call"
    );
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(10))));
}

/// Two calls made in the same millisecond with a zero lifespan are each
/// lapsed for the other, so neither folds and the resolver's winner, here
/// the later version, is the pending value.
#[tokio::test]
async fn the_coalescing_window_lets_the_winner_decide_when_both_calls_are_lapsed() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, None, &clock).with_merge_coalesce_window(secs(3_600));
    s.merge_with_ttl(1, delta(11, 3), Duration::ZERO)
        .await
        .expect("merge");
    s.merge_with_ttl(1, delta(12, 4), Duration::ZERO)
        .await
        .expect("merge");

    let key_bytes = key_bytes(&1u32);
    let stripe = s.pending_merges
        [engine::stripe_index_from_hash(engine::hash_key_bytes(key_bytes.as_ref()))]
    .lock()
    .expect("lock");
    let pending = stripe.entries.get(&1).expect("a value is pending");
    assert_eq!(
        pending.value,
        delta(12, 4),
        "the winner replaces the pending value without folding it"
    );
    assert_eq!(pending.expires_at_ms, Some(T0));
}

#[tokio::test]
async fn the_coalescing_window_drops_a_call_that_is_already_lapsed() {
    let clock = Arc::new(AtomicU64::new(T0));
    let s = counter_shard(1, None, &clock).with_merge_coalesce_window(secs(3_600));
    s.merge_with_ttl(1, delta(11, 3), secs(30))
        .await
        .expect("merge");
    s.merge_with_ttl(1, delta(12, 4), Duration::ZERO)
        .await
        .expect("merge");
    s.flush_all_pending_merges();

    assert_eq!(
        counter_value(&s),
        Some(3),
        "a call whose deadline is the moment it was made adds nothing"
    );
    assert_eq!(s.ttl_of(&1), Some(Ttl::Remaining(secs(30))));
}
