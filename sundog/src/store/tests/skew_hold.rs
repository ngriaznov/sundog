//! The skew hold: a forwarded record a `Mode::Distributed` owner refuses for
//! clock skew is held and applied once the owner's clock is within the bound.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use super::distributed_fill::{crowded_view, key_moved_away};
use super::merging::SumResolver;
use super::*;
use crate::store::skew_hold::SKEW_HOLD_MAX_RECORDS;

const START_MS: u64 = 1_000_000;
const SKEW_MS: u64 = 1_000;
const TOMBSTONE_MAX_TTL: Duration = Duration::from_secs(600);

/// A writer that is no node of the five-node view, so it owns nothing.
const OUTSIDER: u64 = 99;

/// A clock the test sets, with a queue of readings served first: a
/// stand-in for a system clock that steps back between two reads.
#[derive(Default)]
struct TestClock {
    now_ms: AtomicU64,
    script: Mutex<VecDeque<u64>>,
}

impl TestClock {
    fn at(now_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            now_ms: AtomicU64::new(now_ms),
            script: Mutex::default(),
        })
    }

    fn set(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }

    /// The next reads return `readings` in order, then the set time again.
    fn script(&self, readings: impl IntoIterator<Item = u64>) {
        self.script.lock().expect("lock").extend(readings);
    }

    fn read(&self) -> u64 {
        self.script
            .lock()
            .expect("lock")
            .pop_front()
            .unwrap_or_else(|| self.now_ms.load(Ordering::SeqCst))
    }
}

/// A shard with the view it decides under, the sender to publish a further
/// view through and its residency set.
type SkewShard<V> = (
    Shard<u32, V>,
    Arc<OwnershipView>,
    watch::Sender<Arc<OwnershipView>>,
    Arc<ResidencySet>,
);

/// A five-node `Mode::Distributed` shard of node 1, two owners per part,
/// on `clock`, refusing a stamp more than [`SKEW_MS`] ahead of it.
fn skew_shard<V>(clock: &Arc<TestClock>) -> SkewShard<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let residency = Arc::new(ResidencySet::new());
    let (shard, view, tx) = distributed_shard::<u32, V>(
        NodeId::from(1),
        (1..=5u64).map(NodeId::from).collect(),
        2,
        Arc::clone(&residency),
    );
    let reader = Arc::clone(clock);
    let shard = shard
        .with_tombstone_max_ttl(TOMBSTONE_MAX_TTL)
        .with_max_clock_skew(Some(Duration::from_millis(SKEW_MS)))
        .with_clock(Arc::new(move || reader.read()));
    (shard, view, tx, residency)
}

/// A record the outsider forwards, stamped `lead_ms` ahead of [`START_MS`].
fn forward(key: u32, value: &str, lead_ms: u64) -> WireRecord {
    wire_record(key, value, hlc(START_MS + lead_ms, OUTSIDER))
}

fn tombstone_forward(key: u32, lead_ms: u64) -> WireRecord {
    WireRecord {
        value: None,
        ..forward(key, "", lead_ms)
    }
}

/// The key bytes' size in the hold, for a record of `value` (a `String`
/// value encodes as its length byte plus its bytes).
fn held_bytes(key: u32, value: &str) -> usize {
    key_bytes(&key).len() + 1 + value.len() + 64
}

/// Owned by node 1 and, as the outsider is no node of the view, never
/// owned by the writer.
fn owned_key(view: &OwnershipView) -> u32 {
    find_key_by_ownership(view, true)
}

/// A forward the owner's clock refuses is parked, applies when the clock
/// comes within the bound, and the clock then absorbs its stamp.
#[tokio::test]
async fn a_forwarded_record_past_the_bound_is_held_then_applied_within_it() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);

    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "skewed", 10_000)]).await;
    assert_eq!(s.get(&key).await, None, "the stamp is past the bound");
    assert_eq!(s.skew_held(), (1, held_bytes(key, "skewed")));

    clock.set(START_MS + 8_999);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await, None, "one millisecond before it is due");
    assert_eq!(s.skew_held().0, 1);

    clock.set(START_MS + 9_000);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await.as_deref(), Some("skewed"));
    assert_eq!(s.skew_held(), (0, 0));
    assert!(
        s.stamp_local() > hlc(START_MS + 10_000, OUTSIDER),
        "the clock absorbed the released stamp"
    );
}

/// The same record through the replication entry point is dropped, never
/// held: its sender still has it and offers it again.
#[tokio::test]
async fn a_record_replicated_past_the_bound_is_dropped_not_held() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);

    ShardOps::apply_remote_batch(&s, vec![forward(key, "skewed", 10_000)]).await;
    assert_eq!(s.skew_held(), (0, 0));
    clock.set(START_MS + 20_000);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await, None, "nothing was kept to apply");
}

/// A shard with no ownership view, or no bound, has nothing to hold with.
#[tokio::test]
async fn a_shard_without_an_ownership_view_or_a_bound_holds_nothing() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    assert!(s.holds_skewed_forwards());
    let unbounded = s.with_max_clock_skew(None);
    assert!(
        !unbounded.holds_skewed_forwards(),
        "no bound, nothing refused"
    );

    let reader = Arc::clone(&clock);
    let replicated = shard::<u32, String>(1)
        .with_max_clock_skew(Some(Duration::from_millis(SKEW_MS)))
        .with_clock(Arc::new(move || reader.read()));
    assert!(
        !replicated.holds_skewed_forwards(),
        "no ownership view, no forwards"
    );
    let key = owned_key(&view);
    ShardOps::apply_forwarded_batch(&replicated, vec![forward(key, "skewed", 10_000)]).await;
    assert_eq!(replicated.skew_held(), (0, 0));
}

/// The shard holds every forward it is handed through
/// `apply_forwarded_batch`, whichever node wrote it: whether the writer kept
/// a copy is the inbound loop's call (see `cluster::inbound::writer_kept`),
/// from the sender and the view the record came under. A writer that owns
/// the part under this node's view may have owned nothing under the view it
/// wrote in, and kept nothing.
#[tokio::test]
async fn a_forward_whose_writer_owns_the_part_here_is_held() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    let part = PartId::of_key(&key_bytes(&key));
    let co_owner = view
        .owners_of(part)
        .iter()
        .copied()
        .find(|&node| node != NodeId::from(1))
        .expect("a second owner");
    let rec = wire_record(
        key,
        "from an owner",
        Hlc {
            node: co_owner,
            ..hlc(START_MS + 10_000, 1)
        },
    );

    ShardOps::apply_forwarded_batch(&s, vec![rec]).await;
    assert_eq!(s.skew_held().0, 1, "held although its writer owns the part");
    clock.set(START_MS + 9_000);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await.as_deref(), Some("from an owner"));
}

/// A held record refused again on release is held again even when its writer
/// has become an owner of the part since: the hold took it as a record whose
/// writer kept nothing, and a later view does not change what the writer
/// kept.
#[tokio::test]
async fn a_held_record_is_held_again_after_its_writer_becomes_an_owner() {
    let clock = TestClock::at(START_MS);
    let (s, view, tx, _residency) = skew_shard::<String>(&clock);
    let joined = Arc::new(OwnershipView::compute(
        NodeId::from(1),
        (1..=5u64).chain([OUTSIDER]).map(NodeId::from).collect(),
        NonZeroU8::new(2).expect("nonzero"),
    ));
    let key = (0..1_000_000u32)
        .find(|key| {
            let part = PartId::of_key(&key_bytes(key));
            view.owns(part)
                && joined.owns(part)
                && joined.owners_of(part).contains(&NodeId::from(OUTSIDER))
        })
        .expect("a part node 1 keeps and the outsider joins as owner");
    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "joined", 10_000)]).await;
    assert_eq!(s.skew_held().0, 1);

    tx.send(joined).expect("receiver alive");
    clock.set(START_MS + 9_000);
    clock.script([START_MS + 9_000, START_MS + 100]);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await, None);
    assert_eq!(s.skew_held().0, 1, "held again, not dropped");

    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await.as_deref(), Some("joined"));
}

/// A forwarded remove the owner refuses deletes on release, over a live
/// copy, and the delete holds against the older write.
#[tokio::test]
async fn a_held_tombstone_deletes_the_live_entry_on_release_and_stays_deleted() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    let live = wire_record(key, "live", hlc(START_MS - 100, 3));
    ShardOps::apply_remote_batch(&s, vec![live.clone()]).await;
    assert_eq!(s.get(&key).await.as_deref(), Some("live"));

    ShardOps::apply_forwarded_batch(&s, vec![tombstone_forward(key, 10_000)]).await;
    assert_eq!(
        s.get(&key).await.as_deref(),
        Some("live"),
        "the key stays readable until the delete applies"
    );

    clock.set(START_MS + 9_000);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await, None);
    ShardOps::apply_remote_batch(&s, vec![live]).await;
    assert_eq!(
        s.get(&key).await,
        None,
        "the older write does not resurrect it"
    );
}

/// While a record is held the clock has not absorbed its stamp, so a remove
/// made meanwhile stamps below it, and the held write wins when it applies.
/// The same outcome as a replicated write from a fast clock.
#[tokio::test]
async fn a_remove_made_while_a_record_is_held_loses_to_it_on_release() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);

    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "held", 10_000)]).await;
    clock.set(START_MS + 500);
    s.insert(key, "local".to_string()).await.expect("insert");
    s.remove(&key).await.expect("remove");
    assert_eq!(s.get(&key).await, None);

    clock.set(START_MS + 9_000);
    s.release_due_skewed().await;
    assert_eq!(
        s.get(&key).await.as_deref(),
        Some("held"),
        "the held write is newer than the remove"
    );
}

/// A record due when its part has moved to other owners goes to the fan-out
/// as a forward for them, and is not applied here.
#[tokio::test]
async fn a_held_record_for_a_part_the_view_no_longer_owns_is_forwarded_on_release() {
    let clock = TestClock::at(START_MS);
    let (s, view, tx, _residency) = skew_shard::<String>(&clock);
    let crowded = crowded_view();
    let key = key_moved_away(&view, &crowded);
    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "moved", 10_000)]).await;
    assert_eq!(s.skew_held().0, 1);

    tx.send(crowded).expect("receiver alive");
    clock.set(START_MS + 9_000);
    s.release_due_skewed().await;

    assert_eq!(s.skew_held(), (0, 0));
    assert_eq!(s.get(&key).await, None, "not applied here");
    let items = s.fan_out_queue().drain();
    assert!(
        matches!(
            items.as_slice(),
            [FanOutItem::Forward(rec)] if rec.key == key_bytes(&key)
        ),
        "the record goes to the part's current owners: {items:?}"
    );
}

/// The chain through a releasing owner: it redirects a forward for a part it
/// is handing off, the new owner's clock refuses the stamp, and the new
/// owner holds it and applies it once its clock allows.
#[tokio::test]
async fn a_forward_redirected_by_a_releasing_owner_is_held_by_the_new_owner() {
    let clock = TestClock::at(START_MS);
    let (releasing, view, tx, residency) = skew_shard::<String>(&clock);
    let crowded = crowded_view();
    let key = key_moved_away(&view, &crowded);
    let part = PartId::of_key(&key_bytes(&key));
    tx.send(crowded).expect("receiver alive");
    residency.mark_releasing(&[part]);

    ShardOps::apply_forwarded_batch(&releasing, vec![forward(key, "chained", 10_000)]).await;
    assert_eq!(
        releasing.skew_held(),
        (0, 0),
        "the releasing owner holds nothing"
    );
    let redirected = match releasing.fan_out_queue().drain().as_slice() {
        [FanOutItem::Forward(rec)] => rec.clone(),
        other => panic!("one redirected forward expected: {other:?}"),
    };

    let (new_owner, new_view, _new_tx, _new_residency) = {
        let residency = Arc::new(ResidencySet::new());
        let owner = *view.owners_of(part).first().expect("an owner");
        let (shard, new_view, tx) = distributed_shard::<u32, String>(
            owner,
            (1..=5u64).map(NodeId::from).collect(),
            2,
            Arc::clone(&residency),
        );
        let reader = Arc::clone(&clock);
        let shard = shard
            .with_max_clock_skew(Some(Duration::from_millis(SKEW_MS)))
            .with_clock(Arc::new(move || reader.read()));
        (shard, new_view, tx, residency)
    };
    assert!(new_view.owns(part));
    ShardOps::apply_forwarded_batch(&new_owner, vec![redirected]).await;
    assert_eq!(
        new_owner.skew_held().0,
        1,
        "the new owner refuses and holds it"
    );

    clock.set(START_MS + 9_000);
    new_owner.release_due_skewed().await;
    assert_eq!(new_owner.get(&key).await.as_deref(), Some("chained"));
}

/// A clock that steps back between taking a record due and applying it
/// parks it again instead of losing it.
#[tokio::test]
async fn a_record_refused_again_on_release_is_held_again() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "stepped", 10_000)]).await;

    clock.set(START_MS + 9_000);
    // The release reads the clock to take what is due, then the apply reads
    // it to judge the stamp: the second read is a step back.
    clock.script([START_MS + 9_000, START_MS + 100]);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await, None);
    assert_eq!(s.skew_held().0, 1, "held again, not lost");
    assert!(
        s.next_skew_wait()
            .is_some_and(|wait| wait >= Duration::from_millis(1)),
        "the task waits for the new due time instead of spinning"
    );

    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await.as_deref(), Some("stepped"));
}

/// A held write that has expired by release time is dropped like any
/// expired arrival, and does not come back.
#[tokio::test]
async fn a_held_put_that_expired_before_release_is_not_stored() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    let mut rec = forward(key, "short-lived", 10_000);
    rec.expires_at_ms = Some(START_MS + 9_500);
    ShardOps::apply_forwarded_batch(&s, vec![rec]).await;

    clock.set(START_MS + 9_600);
    s.release_due_skewed().await;
    assert_eq!(s.skew_held(), (0, 0));
    assert_eq!(s.get(&key).await, None);
    assert!(!s.contains_key_sync(&key));
}

/// A flood of versions of one key from a fast writer holds the newest only,
/// the same record twice holds once, and the newest is what applies.
#[tokio::test]
async fn a_hot_key_holds_one_record_and_it_is_the_newest() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    let versions: Vec<WireRecord> = (0..200u64)
        .map(|n| forward(key, &format!("v{n}"), 5_000 + n))
        .collect();
    ShardOps::apply_forwarded_batch(&s, versions.clone()).await;
    ShardOps::apply_forwarded_batch(&s, versions).await;
    assert_eq!(s.skew_held().0, 1, "one record per key, no duplicate");

    clock.set(START_MS + 10_000);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await.as_deref(), Some("v199"));
}

/// A merging resolver needs every contribution, so the hold keeps each
/// distinct version of a key and the release merges them.
#[tokio::test]
async fn a_merging_resolver_holds_every_version_and_merges_them_on_release() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<u32>(&clock);
    let s = s.with_resolver(Arc::new(SumResolver));
    let key = owned_key(&view);
    let contribution = |amount: u32, lead_ms: u64| WireRecord {
        key: key_bytes(&key),
        value: Some(key_bytes(&amount)),
        ver: hlc(START_MS + lead_ms, OUTSIDER),
        expires_at_ms: None,
    };
    ShardOps::apply_forwarded_batch(
        &s,
        vec![
            contribution(3, 5_000),
            contribution(4, 6_000),
            contribution(3, 5_000),
        ],
    )
    .await;
    assert_eq!(s.skew_held().0, 2, "both contributions, the duplicate once");

    clock.set(START_MS + 10_000);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await, Some(7));
}

/// A full hold under a merging resolver refuses the contribution that does
/// not fit and counts it as `full`; it never evicts a contribution it holds,
/// so the release merges all of them.
#[tokio::test]
async fn a_full_merging_hold_drops_and_counts_the_new_contribution_and_keeps_the_held_ones() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<u32>(&clock);
    let s = s.with_resolver(Arc::new(SumResolver));
    let key = owned_key(&view);
    let contribution = |amount: u32, wall_ms: u64, writer: u64| WireRecord {
        key: key_bytes(&key),
        value: Some(key_bytes(&amount)),
        ver: hlc(wall_ms, writer),
        expires_at_ms: None,
    };
    let held: Vec<WireRecord> = (0..SKEW_HOLD_MAX_RECORDS as u64)
        .map(|n| contribution(1, START_MS + 5_000 + n, OUTSIDER))
        .collect();
    ShardOps::apply_forwarded_batch(&s, held).await;
    assert_eq!(s.skew_held().0, SKEW_HOLD_MAX_RECORDS);

    // Another writer's higher version finds the hold full.
    let late = contribution(1_000_000, START_MS + 500_000, OUTSIDER - 1);
    ShardOps::apply_forwarded_batch(&s, vec![late]).await;

    assert_eq!(
        s.skew_held().0,
        SKEW_HOLD_MAX_RECORDS,
        "nothing was evicted"
    );
    let dropped = |reason: &str| -> u64 {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, ..)| {
                key.key().name() == "sundog_clock_skew_hold_dropped_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "reason" && label.value() == reason)
            })
            .map(|(_, _, _, value)| match value {
                metrics_util::debugging::DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum()
    };
    assert_eq!(dropped("full"), 1, "the refused contribution is counted");

    clock.set(START_MS + 2_000_000);
    s.release_due_skewed().await;
    assert_eq!(
        s.get(&key).await,
        Some(u32::try_from(SKEW_HOLD_MAX_RECORDS).expect("fits")),
        "every held contribution merged, the refused one did not"
    );
}

/// A stamp further out than the horizon is a broken clock, not skew worth
/// waiting for: it is not held. One within it is.
#[tokio::test]
async fn a_record_due_beyond_the_horizon_is_not_held() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    let horizon_ms = u64::try_from(TOMBSTONE_MAX_TTL.as_millis()).expect("fits");

    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "far", SKEW_MS + horizon_ms + 1)]).await;
    assert_eq!(s.skew_held(), (0, 0), "one millisecond past the horizon");
    ShardOps::apply_forwarded_batch(&s, vec![tombstone_forward(key, u64::MAX - START_MS)]).await;
    assert_eq!(
        s.skew_held(),
        (0, 0),
        "a stamp saturated at the end of time"
    );
    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "near", SKEW_MS + horizon_ms)]).await;
    assert_eq!(s.skew_held().0, 1, "exactly at the horizon");
}

/// The hold stops taking records at its cap; the records it has stay.
#[tokio::test]
async fn a_full_hold_drops_the_record_that_does_not_fit() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let keys: Vec<u32> = (0..u32::MAX)
        .filter(|key| view.owns(PartId::of_key(&key_bytes(key))))
        .take(SKEW_HOLD_MAX_RECORDS + 1)
        .collect();
    let recs: Vec<WireRecord> = keys.iter().map(|&key| forward(key, "v", 5_000)).collect();
    ShardOps::apply_forwarded_batch(&s, recs).await;
    assert_eq!(s.skew_held().0, SKEW_HOLD_MAX_RECORDS);

    clock.set(START_MS + 10_000);
    s.release_due_skewed().await;
    assert_eq!(s.skew_held(), (0, 0));
    assert_eq!(
        s.get(keys.last().expect("some keys")).await,
        None,
        "the record that did not fit was dropped"
    );
    assert_eq!(s.get(&keys[0]).await.as_deref(), Some("v"));
}

/// `next_skew_wait` parks with nothing held, sleeps to the due time in poll
/// slices, and the wake future resolves when a record is held.
#[tokio::test]
async fn the_release_task_parks_wakes_on_a_hold_and_sleeps_in_slices() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    assert_eq!(
        s.next_skew_wait(),
        None,
        "nothing held, nothing to wake for"
    );

    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "near", 1_300)]).await;
    tokio::time::timeout(Duration::from_secs(5), s.skew_wake_notified())
        .await
        .expect("holding a record wakes the task");
    assert_eq!(s.next_skew_wait(), Some(Duration::from_millis(300)));

    clock.set(START_MS + 300);
    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "far", 3_600_000 + 300)]).await;
    assert!(s.next_skew_wait().is_some(), "still the near record");
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await.as_deref(), Some("near"));
}

/// A close hands the held records to the fan-out as forwards for the other
/// owners, and the hold takes and releases nothing after.
#[tokio::test]
async fn closing_hands_the_held_records_to_the_fan_out() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "held", 10_000)]).await;

    s.seal_for_close();
    assert_eq!(s.skew_held(), (0, 0));
    let items = s.fan_out_queue().drain();
    assert!(
        matches!(
            items.as_slice(),
            [FanOutItem::Forward(rec)] if rec.key == key_bytes(&key) && !rec.is_tombstone()
        ),
        "the held record is queued as a forward: {items:?}"
    );

    ShardOps::apply_forwarded_batch(&s, vec![forward(key, "late", 10_000)]).await;
    assert_eq!(s.skew_held(), (0, 0), "a closed hold takes nothing");
    clock.set(START_MS + 20_000);
    s.release_due_skewed().await;
    assert_eq!(s.get(&key).await, None);
}

/// A held record is detached from the frame it came in: the hold, and the
/// forward a close hands it to, keep the record's own bytes, so the hold's
/// byte cap bounds the memory it keeps alive.
#[tokio::test]
async fn a_held_record_is_detached_from_the_frame_it_came_in() {
    let clock = TestClock::at(START_MS);
    let (s, view, _tx, _residency) = skew_shard::<String>(&clock);
    let key = owned_key(&view);
    let rec = forward(key, "held", 10_000);
    // As decoded from a frame: the key and value are slices of its buffer.
    let mut frame = rec.key.to_vec();
    frame.extend_from_slice(rec.value.as_deref().expect("a value"));
    frame.resize(64 * 1024, 0);
    let frame = Bytes::from(frame);
    let value_len = rec.value.as_ref().map_or(0, Bytes::len);
    let sliced = WireRecord {
        key: frame.slice(..rec.key.len()),
        value: Some(frame.slice(rec.key.len()..rec.key.len() + value_len)),
        ..rec.clone()
    };
    ShardOps::apply_forwarded_batch(&s, vec![sliced]).await;
    assert_eq!(s.skew_held().0, 1);

    s.seal_for_close();
    let items = s.fan_out_queue().drain();
    let [FanOutItem::Forward(handed)] = items.as_slice() else {
        panic!("the held record is queued as a forward: {items:?}");
    };
    assert_eq!(handed, &rec);
    let within = |bytes: &Bytes| frame.as_ptr_range().contains(&bytes.as_ptr());
    assert!(!within(&handed.key) && !within(handed.value.as_ref().expect("a value")));
}

/// The `reason` label of `sundog_clock_skew_hold_dropped_total` for each way
/// a forward is dropped instead of held.
#[test]
fn a_dropped_forward_is_labelled_by_why() {
    use crate::store::shard_skew::DropReason;

    assert_eq!(DropReason::Horizon.label(), "horizon");
    assert_eq!(DropReason::Full.label(), "full");
}
