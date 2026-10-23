//! `Shard::merge_sequenced`: the numbers it hands to `make`, the lock it holds
//! while it does, and the order the contributions of one key apply, fold and
//! leave the node in.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::store::crdt::WriterId;

const WINDOW: Duration = Duration::from_secs(60);

fn writer() -> WriterId {
    WriterId::new(NodeId::from(1), 7)
}

/// A resolver whose merge appends the incoming `Vec<u64>` to the stored one,
/// so a stored value lists its contributions in the order they reached the
/// engine. It is not commutative: an out-of-order apply shows in the list.
#[derive(Debug, Clone, Copy, Default)]
struct AppendResolver;

impl ConflictResolver for AppendResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let mut list: Vec<u64> = postcard::from_bytes(a.value?).ok()?;
        list.extend(postcard::from_bytes::<Vec<u64>>(b.value?).ok()?);
        Some(Merged {
            value: Bytes::from(postcard::to_stdvec(&list).ok()?),
            expires_at_ms: None,
        })
    }
}

fn appending_shard() -> Shard<u32, Vec<u64>> {
    shard::<u32, Vec<u64>>(1).with_resolver(Arc::new(AppendResolver))
}

fn coalescing_shard(window: Duration) -> Shard<u32, Vec<u64>> {
    appending_shard().with_merge_coalesce_window(window)
}

/// Draws one number for `key` and stores it as a one-element list.
async fn add_one(s: &Shard<u32, Vec<u64>>, key: u32) {
    s.merge_sequenced(key, writer(), |_, seq| vec![seq])
        .await
        .expect("merge_sequenced");
}

/// `(key, record)` of every item in a non-owner's queue, as lists.
fn forwarded_lists(items: Vec<FanOutItem<u32>>) -> Vec<Vec<u64>> {
    items
        .into_iter()
        .map(|item| match item {
            FanOutItem::Forward(rec) => {
                postcard::from_bytes(rec.value.as_deref().expect("a forwarded value"))
                    .expect("a list")
            }
            FanOutItem::Applied(key, _) => panic!("a non-owner queues only forwards: {key}"),
        })
        .collect()
}

/// A non-owner's shard of a three-node `Mode::Distributed` cluster with the
/// append resolver and `window`, and a key it does not own.
fn non_owner_shard(window: Duration) -> (Shard<u32, Vec<u64>>, u32) {
    let self_node = NodeId::from(1);
    let eligible = (1..=5u64).map(NodeId::from).collect::<Vec<_>>();
    let (s, view, _tx) =
        distributed_shard::<u32, Vec<u64>>(self_node, eligible, 2, Arc::new(ResidencySet::new()));
    let s = s
        .with_resolver(Arc::new(AppendResolver))
        .with_merge_coalesce_window(window);
    (s, find_key_by_ownership(&view, false))
}

#[tokio::test]
async fn the_numbers_start_at_one_and_rise_by_one_with_the_writer_beside_them() {
    let s = shard::<u32, u64>(1);
    let handed = Mutex::new(Vec::new());
    for _ in 0..4 {
        s.merge_sequenced(1, writer(), |who, seq| {
            handed.lock().expect("lock").push((who, seq));
            seq
        })
        .await
        .expect("merge_sequenced");
    }

    assert_eq!(
        *handed.lock().expect("lock"),
        [(writer(), 1), (writer(), 2), (writer(), 3), (writer(), 4)],
        "make gets the writer it was called with and a number that starts at 1"
    );
    assert_eq!(s.get(&1).await, Some(4), "the last value is stored");
}

#[tokio::test]
async fn one_counter_runs_across_the_keys_of_a_shard() {
    let s = shard::<u32, u64>(1);
    for key in [1, 2, 1, 3] {
        s.merge_sequenced(key, writer(), |_, seq| seq)
            .await
            .expect("merge_sequenced");
    }

    assert_eq!(s.get(&1).await, Some(3));
    assert_eq!(s.get(&2).await, Some(2));
    assert_eq!(s.get(&3).await, Some(4));
}

#[tokio::test]
async fn each_shard_numbers_its_own_calls_and_a_new_writer_starts_again_at_one() {
    let first = shard::<u32, u64>(1);
    let restarted = shard::<u32, u64>(1);
    let next_incarnation = WriterId::new(NodeId::from(1), 8);
    for _ in 0..3 {
        first
            .merge_sequenced(1, writer(), |_, seq| seq)
            .await
            .expect("first shard");
    }
    let handed = Mutex::new(None);
    restarted
        .merge_sequenced(1, next_incarnation, |who, seq| {
            *handed.lock().expect("lock") = Some((who, seq));
            seq
        })
        .await
        .expect("the restarted shard");

    assert_eq!(
        *handed.lock().expect("lock"),
        Some((next_incarnation, 1)),
        "a shard built for a writer of a new incarnation counts from 1, whatever another shard drew"
    );
    assert_eq!(
        first.get(&1).await,
        Some(3),
        "the first shard is unaffected"
    );
}

#[tokio::test]
async fn shards_given_one_counter_draw_distinct_numbers_from_it() {
    let counter = Arc::new(AtomicU64::new(0));
    let first = shard::<u32, u64>(1).with_merge_seq_counter(Arc::clone(&counter));
    let second = shard::<u32, u64>(1).with_merge_seq_counter(Arc::clone(&counter));
    let handed = Mutex::new(Vec::new());
    let draw = async |s: &Shard<u32, u64>| {
        s.merge_sequenced(1, writer(), |_, seq| {
            handed.lock().expect("lock").push(seq);
            seq
        })
        .await
        .expect("merge_sequenced");
    };

    draw(&first).await;
    draw(&first).await;
    draw(&second).await;
    draw(&first).await;
    draw(&second).await;

    assert_eq!(
        *handed.lock().expect("lock"),
        [1, 2, 3, 4, 5],
        "a shard that replaces another under one cache name carries on its count"
    );
    assert_eq!(counter.load(Ordering::Relaxed), 5, "the counter is shared");
}

#[tokio::test]
async fn a_value_over_the_frame_cap_fails_after_its_draw_and_its_number_is_not_given_back() {
    let cap = wire::forward_frame_len("test".len(), 1, 8);
    let s = shard::<u32, String>(1).with_max_frame(cap);
    let handed = Mutex::new(Vec::new());
    for big in [false, true, false] {
        let result = s
            .merge_sequenced(1, writer(), |_, seq| {
                handed.lock().expect("lock").push(seq);
                if big {
                    "x".repeat(200)
                } else {
                    "a".to_string()
                }
            })
            .await;
        assert_eq!(result.is_err(), big);
        if big {
            assert!(
                matches!(result, Err(CacheError::ValueTooLarge { .. })),
                "the oversized value is refused: {result:?}"
            );
        }
    }

    assert_eq!(
        *handed.lock().expect("lock"),
        [1, 2, 3],
        "the refused call spent number 2; the next call gets 3"
    );
    assert_eq!(s.get(&1).await.as_deref(), Some("a"));
}

#[tokio::test]
async fn a_refusal_before_the_draw_runs_no_make_and_spends_no_number() {
    let s = shard::<u32, u64>(1).with_max_resident_bytes(1);
    s.merge_sequenced(1, writer(), |_, seq| seq)
        .await
        .expect("a shard under its ceiling accepts the call");
    assert_eq!(s.merge_seq.load(Ordering::Relaxed), 1);

    let ran = Mutex::new(false);
    let result = s
        .merge_sequenced(2, writer(), |_, seq| {
            *ran.lock().expect("lock") = true;
            seq
        })
        .await;

    assert!(
        matches!(result, Err(CacheError::OverMemoryCeiling { .. })),
        "a shard at its ceiling refuses the call: {result:?}"
    );
    assert!(!*ran.lock().expect("lock"), "make never ran");
    assert_eq!(
        s.merge_seq.load(Ordering::Relaxed),
        1,
        "no number was drawn for the refused call"
    );
}

#[tokio::test]
async fn dropping_the_call_before_it_is_polled_draws_nothing() {
    let s = shard::<u32, u64>(1);
    let ran = Mutex::new(false);
    drop(s.merge_sequenced(1, writer(), |_, seq| {
        *ran.lock().expect("lock") = true;
        seq
    }));
    assert!(!*ran.lock().expect("lock"), "make waits for the first poll");

    s.merge_sequenced(1, writer(), |_, seq| seq)
        .await
        .expect("merge_sequenced");
    assert_eq!(s.get(&1).await, Some(1), "the dropped call took no number");
}

/// What a `make` observes about the key's stripe lock: held by the call.
fn stripe_is_held<K, V>(s: &Shard<K, V>, key: &K) -> bool
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = engine::hash_key_bytes(encode_key(key).expect("key encodes").as_ref());
    matches!(
        s.pending_merges[engine::stripe_index_from_hash(hash)].try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    )
}

#[tokio::test]
async fn make_runs_with_the_lock_that_orders_the_keys_merges_held() {
    for window in [Duration::ZERO, WINDOW] {
        let s = shard::<u32, u64>(1).with_merge_coalesce_window(window);
        let held = Mutex::new(None);
        s.merge_sequenced(1, writer(), |_, seq| {
            *held.lock().expect("lock") = Some(stripe_is_held(&s, &1));
            seq
        })
        .await
        .expect("merge_sequenced");

        assert_eq!(
            *held.lock().expect("lock"),
            Some(true),
            "make runs under the key's stripe lock with window {window:?}"
        );
        assert!(
            !stripe_is_held(&s, &1),
            "the call has released the lock when it returns"
        );
    }
}

#[tokio::test]
async fn the_value_stored_after_the_last_number_holds_every_lower_one() {
    let s = appending_shard();
    for _ in 0..3 {
        add_one(&s, 1).await;
    }
    let writes = applied_writes(s.fan_out_queue().drain());
    let last = *writes.last().expect("a write per call");

    let sent = s.fan_out_records(&[last]).await;

    assert_eq!(sent.records.len(), 1);
    assert_eq!(sent.departed, Vec::<WireRecord>::new());
    assert_eq!(
        postcard::from_bytes::<Vec<u64>>(sent.records[0].value.as_deref().expect("a value"))
            .expect("a list"),
        [1, 2, 3],
        "the record a peer receives for the last write carries every number applied before it"
    );
}

#[tokio::test]
async fn calls_in_a_window_fold_in_call_order_into_one_apply_and_one_event() {
    let s = coalescing_shard(WINDOW);
    let mut events = s.events();
    for _ in 0..3 {
        add_one(&s, 1).await;
    }

    assert_eq!(s.pending_merge_count(), 1, "the calls fold into one");
    assert_eq!(s.get(&1).await, None, "the fold waits in its window");
    s.flush_all_pending_merges();

    assert_eq!(s.get(&1).await, Some(vec![1, 2, 3]));
    match events.try_recv() {
        Ok(Event::Created { key: 1, value, .. }) => assert_eq!(value, [1, 2, 3]),
        other => panic!("one Created event for the fold: {other:?}"),
    }
    assert!(events.try_recv().is_err(), "and no other");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_calls_on_one_key_apply_in_the_order_their_numbers_were_drawn() {
    const TASKS: u64 = 8;
    const CALLS: u64 = 250;
    let s = Arc::new(appending_shard());
    let start = Arc::new(tokio::sync::Barrier::new(
        usize::try_from(TASKS).expect("small"),
    ));
    let tasks: Vec<_> = (0..TASKS)
        .map(|_| {
            let (s, start) = (Arc::clone(&s), Arc::clone(&start));
            tokio::spawn(async move {
                start.wait().await;
                for _ in 0..CALLS {
                    add_one(&s, 1).await;
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("task");
    }

    let applied = s.get(&1).await.expect("the key is stored");
    assert_eq!(
        applied,
        (1..=TASKS * CALLS).collect::<Vec<_>>(),
        "the stored list is in the order the numbers were drawn, with no gap and no repeat"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_calls_fold_and_flush_in_order_while_flushes_race_each_other() {
    const TASKS: u64 = 6;
    const CALLS: u64 = 200;
    let s = Arc::new(
        appending_shard()
            .with_clock(Arc::new(now_ms))
            .with_merge_coalesce_window(Duration::from_millis(1)),
    );
    let key_hash = engine::hash_key_bytes(key_bytes(&1u32).as_ref());
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // The background sweep, a peer's invalidation and a close all flush, and
    // do so at the same time.
    let flushers: Vec<_> = (0..3)
        .map(|which| {
            let (s, done) = (Arc::clone(&s), Arc::clone(&done));
            std::thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    match which {
                        0 => s.flush_due_pending_merges(now_ms()),
                        1 => s.flush_pending_merge_before(&1, key_hash, hlc(now_ms() + 5_000, 9)),
                        _ => s.flush_all_pending_merges(),
                    }
                    std::thread::yield_now();
                }
            })
        })
        .collect();
    let start = Arc::new(tokio::sync::Barrier::new(
        usize::try_from(TASKS).expect("small"),
    ));
    let tasks: Vec<_> = (0..TASKS)
        .map(|_| {
            let (s, start) = (Arc::clone(&s), Arc::clone(&start));
            tokio::spawn(async move {
                start.wait().await;
                for _ in 0..CALLS {
                    add_one(&s, 1).await;
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("task");
    }
    done.store(true, Ordering::Release);
    for flusher in flushers {
        flusher.join().expect("flusher");
    }
    s.flush_all_pending_merges();

    assert_eq!(
        s.get(&1).await.expect("the key is stored"),
        (1..=TASKS * CALLS).collect::<Vec<_>>(),
        "folds applied in the order they were made, however the flushes interleaved"
    );
}

/// Runs `flush` on its own thread, parked between the take and the apply of
/// its first stripe until the returned sender fires. Returns once it is
/// parked there.
fn park_the_first_flush(
    s: &Arc<Shard<u32, Vec<u64>>>,
    flush: impl FnOnce(&Shard<u32, Vec<u64>>) + Send + 'static,
) -> (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let (parked_tx, parked_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Mutex::new(release_rx);
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
async fn a_second_flush_waits_to_take_the_next_fold_of_a_key_until_the_first_has_applied() {
    let s = Arc::new(coalescing_shard(WINDOW));
    let mut events = s.events();
    add_one(&s, 1).await;
    let (release, first) = park_the_first_flush(&s, Shard::flush_all_pending_merges);
    assert_eq!(s.pending_merge_count(), 0, "the parked flush has the fold");

    // A fold made after the first was taken, and a peer's invalidation that
    // flushes it before the first fold has applied.
    add_one(&s, 1).await;
    assert_eq!(s.pending_merge_count(), 1);
    let key_hash = engine::hash_key_bytes(key_bytes(&1u32).as_ref());
    let newer = hlc(ahead_ms(), 2);
    let second = {
        let s = Arc::clone(&s);
        std::thread::spawn(move || s.flush_pending_merge_before(&1, key_hash, newer))
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !second.is_finished(),
        "the second flush waits for the first to apply"
    );
    assert_eq!(
        s.pending_merge_count(),
        1,
        "and has not taken the later fold"
    );
    assert_eq!(s.get(&1).await, None, "nothing has applied");

    release.send(()).expect("release the first flush");
    first.join().expect("the first flush finishes");
    second.join().expect("the second flush finishes");

    assert_eq!(s.get(&1).await, Some(vec![1, 2]));
    match events.try_recv() {
        Ok(Event::Created { key: 1, value, .. }) => {
            assert_eq!(value, [1], "the first fold applies first");
        }
        other => panic!("Created for the first fold: {other:?}"),
    }
    match events.try_recv() {
        Ok(Event::Updated { key: 1, value, .. }) => {
            assert_eq!(value, [1, 2], "the second folds into it");
        }
        other => panic!("Updated for the second fold: {other:?}"),
    }
}

#[tokio::test]
async fn an_owner_queues_one_write_per_call_in_call_order() {
    let s = appending_shard();
    for _ in 0..3 {
        add_one(&s, 1).await;
    }

    let versions: Vec<Hlc> = applied_writes(s.fan_out_queue().drain())
        .into_iter()
        .map(|(key, ver)| {
            assert_eq!(key, 1);
            ver
        })
        .collect();

    assert_eq!(versions.len(), 3);
    assert!(
        versions.windows(2).all(|pair| pair[0] < pair[1]),
        "the writes are stamped and queued in the order the numbers were drawn: {versions:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_owners_forwards_leave_in_the_order_the_numbers_were_drawn() {
    const TASKS: u64 = 8;
    const CALLS: u64 = 100;
    let (s, key) = non_owner_shard(Duration::ZERO);
    let s = Arc::new(s);
    let start = Arc::new(tokio::sync::Barrier::new(
        usize::try_from(TASKS).expect("small"),
    ));
    let tasks: Vec<_> = (0..TASKS)
        .map(|_| {
            let (s, start) = (Arc::clone(&s), Arc::clone(&start));
            tokio::spawn(async move {
                start.wait().await;
                for _ in 0..CALLS {
                    add_one(&s, key).await;
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("task");
    }

    let queued: Vec<u64> = forwarded_lists(s.fan_out_queue().drain())
        .into_iter()
        .map(|list| {
            assert_eq!(list.len(), 1, "one contribution per forward");
            list[0]
        })
        .collect();
    assert_eq!(
        queued,
        (1..=TASKS * CALLS).collect::<Vec<_>>(),
        "the owners receive the forwards in the order the numbers were drawn"
    );
}

#[tokio::test]
async fn a_non_owners_window_forwards_the_calls_folded_in_call_order() {
    let (s, key) = non_owner_shard(WINDOW);
    for _ in 0..4 {
        add_one(&s, key).await;
    }
    assert!(s.fan_out_queue().drain().is_empty(), "the fold is pending");

    s.flush_all_pending_merges();

    assert_eq!(
        forwarded_lists(s.fan_out_queue().drain()),
        [vec![1, 2, 3, 4]],
        "one forward carries the whole fold, numbers in the order they were drawn"
    );
}

#[tokio::test]
async fn merge_sequenced_with_ttl_gives_the_contribution_its_own_lifespan() {
    let s = shard::<u32, u64>(1);
    s.merge_sequenced_with_ttl(1, writer(), |_, seq| seq, Duration::from_secs(600))
        .await
        .expect("merge_sequenced_with_ttl");
    s.merge_sequenced(2, writer(), |_, seq| seq)
        .await
        .expect("merge_sequenced");

    assert!(
        matches!(
            s.ttl_of(&1),
            Some(Ttl::Remaining(left))
                if left > Duration::from_secs(590) && left <= Duration::from_secs(600)
        ),
        "the call's lifespan applies: {:?}",
        s.ttl_of(&1)
    );
    assert_eq!(s.ttl_of(&2), Some(Ttl::Never), "a plain call has none here");
    assert_eq!(s.get(&1).await, Some(1));
    assert_eq!(s.get(&2).await, Some(2), "the counter runs across both");
}
