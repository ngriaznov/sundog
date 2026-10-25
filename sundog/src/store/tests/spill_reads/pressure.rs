//! Spill read path tests under flush-queue and memory pressure.

use super::*;

/// Pins that `reserve_for_batch` suspends `insert_many` while another
/// caller holds the tier's flush-queue budget, resuming
/// once it frees.
#[tokio::test]
async fn apply_grouped_suspends_under_a_saturated_queue_and_resumes_once_room_frees() {
    let dir = temp_dir("suspend-apply-grouped");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(64)
        .spill_wait_timeout(Duration::from_secs(5));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("suspend-apply-grouped"),
        Mode::Local,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));

    // Another caller holds the entire flush-queue budget already.
    let held = tier
        .reserve(64, Duration::from_secs(5))
        .await
        .expect("the whole (tiny) queue is free before this call");

    let insert_fut = shard.insert_many([(1u32, "a".to_string())]);
    tokio::pin!(insert_fut);
    let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut insert_fut).await;
    assert!(
        not_yet.is_err(),
        "insert_many's own apply_grouped call must still be waiting on reserve()"
    );

    drop(held);

    let result = tokio::time::timeout(Duration::from_secs(5), &mut insert_fut)
        .await
        .expect("insert_many resumes once the reservation frees room");
    assert!(
        result.is_ok(),
        "the write itself still succeeds: {result:?}"
    );
    assert_eq!(shard.get(&1).await, Some("a".to_string()));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that `apply_remote_batch` suspends and resumes identically to
/// `apply_grouped` under a saturated flush-queue.
#[tokio::test]
async fn apply_remote_batch_suspends_identically_under_a_saturated_queue() {
    let dir = temp_dir("suspend-apply-remote-batch");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(64)
        .spill_wait_timeout(Duration::from_secs(5));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("suspend-apply-remote-batch"),
        Mode::Local,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));

    // Another caller holds the entire flush-queue budget already.
    let held = tier
        .reserve(64, Duration::from_secs(5))
        .await
        .expect("the whole (tiny) queue is free before this call");

    let rec = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec("a").expect("encode"))),
        ver: hlc(1, 1),
        expires_at_ms: None,
    };
    let mut batch_fut = ShardOps::apply_remote_batch(&shard, vec![rec]);
    let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut batch_fut).await;
    assert!(
        not_yet.is_err(),
        "apply_remote_batch's own reserve_for_batch call must still be waiting on \
         reserve()"
    );

    drop(held);

    tokio::time::timeout(Duration::from_secs(5), &mut batch_fut)
        .await
        .expect("apply_remote_batch resumes once the reservation frees room");
    assert_eq!(shard.get(&1).await, Some("a".to_string()));

    let _ = std::fs::remove_dir_all(&dir);
}

/// An `Invalidation` shard stores a peer's record only as a fold into a key
/// it holds, so `apply_remote_batch` reserves no flush-queue budget for the
/// batch: a record for a key it does not hold completes at once while another
/// caller holds the whole budget, and stores nothing.
#[tokio::test]
async fn an_invalidation_batch_waits_for_no_flush_queue_budget() {
    let dir = temp_dir("invalidation-batch-no-reservation");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(64)
        .spill_wait_timeout(Duration::from_secs(30));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("invalidation-batch-no-reservation"),
        Mode::Invalidation,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
    let _held = tier
        .reserve(64, Duration::from_secs(5))
        .await
        .expect("the whole (tiny) queue is free before this call");

    let rec = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(postcard::to_stdvec("a").expect("encode"))),
        ver: hlc(1, 1),
        expires_at_ms: None,
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        ShardOps::apply_remote_batch(&shard, vec![rec]),
    )
    .await
    .expect("a batch of records this shard ignores reserves nothing and does not wait");
    assert_eq!(shard.get_sync(&1), None);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that `apply_remote_batch` groups by stripe before its one
/// `reserve_for_batch` suspension point, so the suspension cannot
/// reorder a batch: two same-key, same-`Hlc` puts must apply first-wins.
#[tokio::test]
async fn apply_remote_batch_preserves_per_bucket_order_once_reserve_can_suspend() {
    let dir = temp_dir("order-apply-remote-batch");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(64)
        .spill_wait_timeout(Duration::from_secs(5));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("order-apply-remote-batch"),
        Mode::Local,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));

    let held = tier
        .reserve(64, Duration::from_secs(5))
        .await
        .expect("the whole (tiny) queue is free before this call");

    let ver = hlc(1, 1);
    let first = WireRecord {
        key: key_bytes(&7u32),
        value: Some(Bytes::from(postcard::to_stdvec("first").expect("encode"))),
        ver,
        expires_at_ms: None,
    };
    let second = WireRecord {
        key: key_bytes(&7u32),
        value: Some(Bytes::from(postcard::to_stdvec("second").expect("encode"))),
        ver,
        expires_at_ms: None,
    };
    let mut batch_fut = ShardOps::apply_remote_batch(&shard, vec![first, second]);
    let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut batch_fut).await;
    assert!(
        not_yet.is_err(),
        "this batch's reserve_for_batch call must still be waiting on reserve()"
    );

    drop(held);

    tokio::time::timeout(Duration::from_secs(5), &mut batch_fut)
        .await
        .expect("apply_remote_batch resumes once the reservation frees room");
    assert_eq!(
        shard.get(&7).await,
        Some("first".to_string()),
        "the batch's own arrival order survives the suspension: \"first\" applies before \
         \"second\", so the equal-Hlc \"second\" write loses to it, exactly as it would \
         with no suspension at all"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins `retry_reservation_deficit`'s closing fallback end to end:
/// once its retry budget is exhausted with a real deficit outstanding,
/// it must call `Engine::enforce_capacity` itself rather than leave
/// `max_capacity` exceeded. The four keys collide into stripe 0,
/// the `start_bucket` both call sites pass.
#[tokio::test]
async fn retry_reservation_deficit_deletes_the_leftover_victim_once_its_own_budget_is_exhausted() {
    let dir = temp_dir("retry-deficit-shard-fallback");
    let value = "x".repeat(20);
    let encoded_len = postcard::to_stdvec(&value)
        .expect("test value encodes")
        .len();

    let mut keys = Vec::new();
    for k in 1..200_000u32 {
        let kb = key_bytes(&k);
        if engine::stripe_index_from_hash(engine::hash_key_bytes(kb.as_ref())) != 0 {
            continue;
        }
        keys.push(k);
        if keys.len() == 4 {
            break;
        }
    }
    assert_eq!(
        keys.len(),
        4,
        "1024 stripes; four keys landing in stripe 0 specifically are found quickly"
    );

    let record_len_0 = spill::spill_record_len(key_bytes(&keys[0]).len(), encoded_len);

    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        // One record's worth: the reservation clamps down to this
        // tiny tier total, covering at most one victim.
        .flush_queue_bytes(u64::from(record_len_0))
        .spill_wait_timeout(Duration::from_millis(150));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("retry-deficit-shard-fallback"),
        Mode::Local,
        NodeId::from(1u64),
        30,
        None,
        None,
    )
    .with_weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX))
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
    // Paused before this call enqueues anything, so nothing ever
    // frees a byte back.
    tier.pause_flusher();

    shard
        .insert_many(keys.iter().map(|&k| (k, value.clone())))
        .await
        .expect(
            "insert_many completes even though most of its own evictions are refused \
             for the whole run",
        );

    let mut resident_or_spilled = 0;
    for &k in &keys {
        if shard.get(&k).await.is_some() {
            resident_or_spilled += 1;
        }
    }
    assert!(
        resident_or_spilled < keys.len(),
        "keep_resident_when_refused defaults to false: retry_reservation_deficit's own \
         closing fallback must physically delete at least the victim a reservation \
         retry gave up on, not leave every key resident with max_capacity exceeded \
         forever because a background retry ran out of budget"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// `retry_reservation_deficit` leaves the batch's own entries for
/// last: its closing pass starts in stripe 0, where the batch wrote
/// a key, and evicts an older key there instead. The flusher stays
/// paused and the flush queue holds one record, so the batch's
/// reservation covers one hand-off and the retry times out.
#[tokio::test]
async fn retry_reservation_deficit_keeps_the_batchs_own_entries() {
    let dir = temp_dir("retry-deficit-keeps-the-batch");
    let value = "x".repeat(20);
    let encoded_len = postcard::to_stdvec(&value)
        .expect("test value encodes")
        .len();
    let stripe = |k: u32| engine::stripe_index_from_hash(engine::hash_key_bytes(&key_bytes(&k)));
    // Keys of two encoded bytes alike, so one record length fits each.
    let in_stripe_zero = (128..16_384u32)
        .find(|&k| stripe(k) == 0)
        .expect("a key in stripe 0");
    let others: Vec<u32> = (128..16_384u32)
        .filter(|&k| stripe(k) != 0)
        .take(3)
        .collect();
    let [older_a, older_b, elsewhere] = others[..] else {
        panic!("three keys outside stripe 0");
    };
    let record_len = spill::spill_record_len(key_bytes(&in_stripe_zero).len(), encoded_len);
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(u64::from(record_len))
        .spill_wait_timeout(Duration::from_millis(150));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("retry-deficit-keeps-the-batch"),
        Mode::Local,
        NodeId::from(1u64),
        40,
        None,
        None,
    )
    .with_weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX))
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
    tier.pause_flusher();
    for k in [older_a, older_b] {
        shard.insert(k, value.clone()).await.expect("insert");
    }

    shard
        .insert_many([(in_stripe_zero, value.clone()), (elsewhere, value.clone())])
        .await
        .expect("insert_many");

    assert_eq!(shard.get_sync(&in_stripe_zero), Some(value.clone()));
    assert_eq!(shard.get_sync(&elsewhere), Some(value.clone()));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that `retry_reservation_deficit`'s waits share what
/// `reserve_for_batch`'s wait left of the budget instead of opening a
/// second, fresh `spill_wait_timeout` window: the `apply_grouped` call's
/// waits stay within one timeout total. A background task holds the tier's
/// admission budget for `HOG_HOLD` first, so a shared budget (finishes
/// near `SPILL_WAIT_TIMEOUT`) is distinguishable from a fresh one
/// (`HOG_HOLD` + `SPILL_WAIT_TIMEOUT`).
#[tokio::test]
async fn apply_grouped_bounds_its_whole_reservation_wait_to_one_spill_wait_timeout() {
    const SPILL_WAIT_TIMEOUT: Duration = Duration::from_millis(300);
    /// Half the budget, leaving a clear gap between the "shared" and
    /// "fresh" budget totals so scheduling jitter cannot blur them.
    const HOG_HOLD: Duration = Duration::from_millis(150);

    let dir = temp_dir("retry-deficit-shared-budget");
    let value = "x".repeat(20);
    let encoded_len = postcard::to_stdvec(&value)
        .expect("test value encodes")
        .len();

    let mut keys = Vec::new();
    for k in 1..200_000u32 {
        let kb = key_bytes(&k);
        if engine::stripe_index_from_hash(engine::hash_key_bytes(kb.as_ref())) != 0 {
            continue;
        }
        keys.push(k);
        if keys.len() == 4 {
            break;
        }
    }
    assert_eq!(
        keys.len(),
        4,
        "1024 stripes; four keys landing in stripe 0 specifically are found quickly"
    );

    let record_len_0 = spill::spill_record_len(key_bytes(&keys[0]).len(), encoded_len);

    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(u64::from(record_len_0))
        .spill_wait_timeout(SPILL_WAIT_TIMEOUT);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("retry-deficit-shared-budget"),
        Mode::Local,
        NodeId::from(1u64),
        30,
        None,
        None,
    )
    .with_weigher(|_k: &u32, v: &String| u32::try_from(v.len()).unwrap_or(u32::MAX))
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
    tier.pause_flusher();

    // Holds the whole admission budget for `HOG_HOLD`, forcing
    // `reserve_for_batch` below to wait a known slice of its timeout.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let hog_tier = Arc::clone(&tier);
    let hog = tokio::spawn(async move {
        let reservation = hog_tier
            .reserve(record_len_0, Duration::from_secs(5))
            .await
            .expect("the whole (single-record) queue is free before this task ever runs");
        let _ = ready_tx.send(());
        tokio::time::sleep(HOG_HOLD).await;
        drop(reservation);
    });
    ready_rx
        .await
        .expect("the hog task reserves the whole queue before insert_many starts racing it");

    let started = std::time::Instant::now();
    shard
        .insert_many(keys.iter().map(|&k| (k, value.clone())))
        .await
        .expect(
            "insert_many completes even though most of its own evictions are refused \
             for the whole run",
        );
    let elapsed = started.elapsed();
    hog.await.expect("the hog task never panics");

    assert!(
        elapsed < SPILL_WAIT_TIMEOUT + Duration::from_millis(100),
        "reserve_for_batch's own wait ({HOG_HOLD:?}, out for the hog) plus \
         retry_reservation_deficit's own retry must share one \
         {SPILL_WAIT_TIMEOUT:?} budget, not a fresh one each: elapsed {elapsed:?} \
         should land around {SPILL_WAIT_TIMEOUT:?}, well short of \
         {HOG_HOLD:?} + {SPILL_WAIT_TIMEOUT:?}, which is what a second, independent \
         full-budget wait would cost"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The wait budget a replicated batch's evictions share bounds their waits
/// for room, not the batch's own work: a batch that is still applying when
/// `spill_wait_timeout` has passed since it began waits for the flusher
/// instead of refusing its remaining evictions. The pause stands in for that
/// work and outlasts the budget; the flusher stays paused a little past it,
/// so a budget measured from the batch's start would refuse the evictions
/// into the full queue as `deferred`.
#[tokio::test]
async fn a_batch_whose_own_work_outlasts_the_wait_budget_still_waits_for_room() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    const NAME: &str = "retry-work-not-wait";
    const KEYS: u32 = 20;
    // Keys 1000.. encode in two bytes and "v" x 10 in eleven.
    const FIRST_KEY: u32 = 1_000;
    const SPILL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
    const WORK: Duration = Duration::from_millis(2_500);
    const FLUSHER_PAUSED_PAST_WORK: Duration = Duration::from_millis(200);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let dir = temp_dir(NAME);
    let record_len = u64::from(crate::store::spill::spill_record_len(2, 11));
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(64 * 1024)
        .flush_queue_bytes(record_len * 4)
        .spill_wait_timeout(SPILL_WAIT_TIMEOUT);
    let shard = Shard::<u32, String>::new(
        SmolStr::new(NAME),
        Mode::Replicated,
        NodeId::from(1u64),
        2,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
    tier.pause_flusher();
    let resume = std::thread::spawn(move || {
        std::thread::sleep(WORK + FLUSHER_PAUSED_PAST_WORK);
        tier.resume_flusher();
    });

    crate::store::shard_write::RETRY_PAUSE.with(|pause| pause.set(Some(WORK)));
    let records = (FIRST_KEY..FIRST_KEY + KEYS)
        .map(|k| wire_record(k, "vvvvvvvvvv", hlc(u64::from(k), 2)))
        .collect();
    ShardOps::apply_remote_batch(&shard, records).await;
    crate::store::shard_write::RETRY_PAUSE.with(|pause| pause.set(None));
    resume.join().expect("the resume thread finishes");

    let (mut timeouts, mut dropped) = (0, 0);
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let of_this_cache = key
            .key()
            .labels()
            .any(|label| label.key() == "cache" && label.value() == NAME);
        if let (true, DebugValue::Counter(n)) = (of_this_cache, value) {
            match key.key().name() {
                "sundog_spill_wait_timeouts_total" => timeouts += n,
                "sundog_spill_dropped_total" => dropped += n,
                _ => {}
            }
        }
    }
    assert_eq!(timeouts, 0, "no wait for room runs out");
    assert_eq!(
        dropped, 0,
        "every eviction waits for the flusher and reaches disk, none refused"
    );
    for k in FIRST_KEY..FIRST_KEY + KEYS {
        assert!(
            shard.get(&k).await.is_some(),
            "key {k} is present, resident or spilled"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A tier whose channel takes no job it admits, as one whose flusher is gone,
/// grants a retry round its reservation at once, from the permits every
/// abandoned hand-off hands back, and the round frees nothing. The retries
/// end there and the closing pass runs, instead of the call spinning: the
/// flush queue holds three records, fewer than one eviction batch takes, and
/// stripe 0 holds forty entries over the cap, so every round leaves a deficit.
#[test]
fn a_retry_over_a_tier_that_takes_no_job_ends() {
    const NAME: &str = "retry-no-flusher";
    let dir = temp_dir(NAME);
    let value = "x".repeat(20);
    let encoded_len = postcard::to_stdvec(&value)
        .expect("test value encodes")
        .len();
    let stripe = |k: u32| engine::stripe_index_from_hash(engine::hash_key_bytes(&key_bytes(&k)));
    // Keys of three encoded bytes alike, so one record length fits each.
    let in_stripe_zero: Vec<u32> = (16_384..2_097_152u32)
        .filter(|&k| stripe(k) == 0)
        .take(41)
        .collect();
    assert_eq!(
        in_stripe_zero.len(),
        41,
        "41 three-byte keys land in stripe 0"
    );
    let record_len = spill::spill_record_len(key_bytes(&in_stripe_zero[0]).len(), encoded_len);
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(u64::from(record_len) * 3)
        .spill_wait_timeout(Duration::from_millis(500));
    let shard = Arc::new(Shard::<u32, String>::new(
        SmolStr::new(NAME),
        Mode::Local,
        NodeId::from(1u64),
        2,
        None,
        None,
    ));
    // Published with no flusher attached: the tier admits within its budget
    // and fails every enqueue, as one whose flusher is gone does.
    shard.engine.set_spill(Arc::new(
        spill::SpillTier::open(&cfg, NAME).expect("tier opens"),
    ));

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = {
        let shard = Arc::clone(&shard);
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime builds");
            runtime.block_on(async {
                for &k in &in_stripe_zero[..40] {
                    shard.insert(k, value.clone()).await.expect("insert");
                }
                shard
                    .insert_many([(in_stripe_zero[40], value.clone())])
                    .await
                    .expect("insert_many");
            });
            let _ = done_tx.send(());
        })
    };

    // A spinning call never yields, so it is watched from this thread.
    assert!(
        done_rx.recv_timeout(Duration::from_secs(30)).is_ok(),
        "the batch's retries end over a tier that takes no job"
    );
    worker.join().expect("the worker does not panic");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that `apply_grouped`'s batch-wide `Reservation` reaches a real
/// eviction pass: every write past `max_capacity` still lands, never a
/// plain delete.
#[tokio::test]
async fn apply_grouped_threads_a_real_reservation_through_a_real_eviction_pass() {
    let dir = temp_dir("reservation-through-real-eviction");
    // Generous budget: the point is that the reservation reaches
    // real eviction, not byte-budget precision.
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(1 << 16)
        .spill_wait_timeout(Duration::from_secs(5));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("reservation-through-real-eviction"),
        Mode::Local,
        NodeId::from(1u64),
        5, // weight 1 per entry with no custom weigher: caps at 5 live entries
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");

    shard
        .insert_many((0..10u32).map(|k| (k, "v".repeat(5))))
        .await
        .expect("insert_many");

    // Every write landed, resident or spilled, never a plain delete.
    for k in 0..10u32 {
        assert!(
            shard.get(&k).await.is_some(),
            "key {k} is present, whether resident or spilled"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// A batch whose evictions need more than the whole flush queue,
/// local (`insert_many`) or replicated (`apply_remote_batch`), hands
/// its first reservation's unspent budget back before the retry asks
/// for more. The retry, clamped to the whole queue, is then granted
/// as the flusher drains, rather than waiting out
/// `spill_wait_timeout` and evicting with no reservation at all.
///
/// Every record is the same length and the queue holds ten and a
/// half of them, so the first reservation always leaves half a
/// record unspent; the flusher stays paused through the per-bucket
/// loop, so the retry's deficit always exceeds the whole queue.
#[tokio::test]
async fn a_batch_past_a_clamped_reservation_never_waits_out_its_budget() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    const KEYS: u32 = 200;
    // Keys 1000.. encode in two bytes and "v" x 10 in eleven.
    const FIRST_KEY: u32 = 1_000;
    let record_len = u64::from(crate::store::spill::spill_record_len(2, 11));
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    for (name, remote) in [("clamped-local", false), ("clamped-remote", true)] {
        let dir = temp_dir(name);
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(64 * 1024)
            .flush_queue_bytes(record_len * 10 + record_len / 2)
            .spill_wait_timeout(Duration::from_secs(5));
        let shard = Shard::<u32, String>::new(
            SmolStr::new(name),
            Mode::Local,
            NodeId::from(1u64),
            5,
            None,
            None,
        )
        .with_spill(&cfg)
        .expect("tier opens");
        let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
        tier.pause_flusher();
        let resume = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            tier.resume_flusher();
        });

        let keys = FIRST_KEY..FIRST_KEY + KEYS;
        let started = std::time::Instant::now();
        if remote {
            let records = keys
                .clone()
                .map(|k| wire_record(k, "vvvvvvvvvv", hlc(u64::from(k), 2)))
                .collect();
            ShardOps::apply_remote_batch(&shard, records).await;
        } else {
            shard
                .insert_many(keys.clone().map(|k| (k, "v".repeat(10))))
                .await
                .expect("insert_many");
        }
        let took = started.elapsed();
        resume.join().expect("the resume thread finishes");

        let (mut timeouts, mut dropped) = (0, 0);
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let of_this_cache = key
                .key()
                .labels()
                .any(|label| label.key() == "cache" && label.value() == name);
            if let (true, DebugValue::Counter(n)) = (of_this_cache, value) {
                match key.key().name() {
                    "sundog_spill_wait_timeouts_total" => timeouts += n,
                    "sundog_spill_dropped_total" => dropped += n,
                    _ => {}
                }
            }
        }
        assert_eq!(
            timeouts, 0,
            "{name}: no reservation wait runs out ({took:?})"
        );
        assert_eq!(dropped, 0, "{name}: every eviction reaches disk");
        assert!(
            took < Duration::from_secs(4),
            "{name}: the batch finishes well inside its 5s budget, took {took:?}"
        );
        for k in keys {
            assert!(
                shard.get(&k).await.is_some(),
                "{name}: key {k} is present, resident or spilled"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A victim whose spill record needs more than the whole flush queue
/// can never be admitted. The first retry holds the whole queue and
/// covers nothing, so the batch goes straight to its closing
/// no-reservation pass rather than retrying, or waiting, until its
/// 5s budget runs out; that pass still brings the shard back to its
/// cap, dropping every victim as `queue_full` on this `Mode::Local`
/// tier.
#[tokio::test]
async fn a_victim_larger_than_the_flush_queue_ends_the_retries_at_once() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    const NAME: &str = "oversize-victims";
    const KEYS: u32 = 20;
    const CAP: u32 = 2;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let dir = temp_dir(NAME);
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(64 * 1024)
        .flush_queue_bytes(u64::from(crate::store::spill::spill_record_len(2, 11)))
        .spill_wait_timeout(Duration::from_secs(5));
    let shard = Shard::<u32, String>::new(
        SmolStr::new(NAME),
        Mode::Local,
        NodeId::from(1u64),
        u64::from(CAP),
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");

    let started = std::time::Instant::now();
    shard
        .insert_many((1_000..1_000 + KEYS).map(|k| (k, "v".repeat(200))))
        .await
        .expect("insert_many");
    let took = started.elapsed();

    // One snapshot for every counter: a snapshot drains them.
    let counters = snapshotter.snapshot().into_vec();
    let counter = |name: &str| -> u64 {
        counters
            .iter()
            .filter(|(key, ..)| key.key().name() == name)
            .map(|(.., value)| match value {
                DebugValue::Counter(n) => *n,
                _ => 0,
            })
            .sum()
    };
    let dropped_for = |reason: &str| -> u64 {
        counters
            .iter()
            .filter(|(key, ..)| {
                key.key().name() == "sundog_spill_dropped_total"
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "reason" && l.value() == reason)
            })
            .map(|(.., value)| match value {
                DebugValue::Counter(n) => *n,
                _ => 0,
            })
            .sum()
    };
    assert_eq!(
        counter("sundog_spill_wait_timeouts_total"),
        0,
        "no reservation wait runs out ({took:?})"
    );
    assert_eq!(
        shard.entry_count().await,
        u64::from(CAP),
        "the closing pass brings the shard back to its cap"
    );
    assert_eq!(
        dropped_for("queue_full"),
        u64::from(KEYS - CAP),
        "every victim the queue could never admit is dropped as queue_full"
    );
    assert_eq!(
        counter("sundog_spill_dropped_total"),
        dropped_for("queue_full"),
        "a Local write's drops record no other reason, `deferred` among them"
    );
    assert!(
        took < Duration::from_secs(2),
        "the batch ends its retries at once, took {took:?} of its 5s budget"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A read that promotes a spilled entry back to RAM makes room for
/// it first, through `get` and through `get_or_load`'s spilled-key
/// arm alike: reading every key of a shard spilled far past its cap
/// leaves at most the cap resident, every key still live, no loader
/// called, and the last key read in RAM. With 1024 stripes and a
/// cap of four, each key is alone in its stripe, so a pass that
/// sampled the promoted entry's own stripe after reinstalling it
/// would evict that entry straight back to disk.
///
/// Every hand-off installs before the last key is read. A last key
/// still in a hand-off reads as a RAM hit, and the flusher's later
/// install leaves it on disk however the read behaves.
#[tokio::test]
async fn reading_spilled_keys_keeps_the_shard_within_its_cap() {
    const KEYS: u32 = 24;
    const CAP: u32 = 4;
    let (shard, dir) = spill_shard("promote-within-cap", u64::from(CAP));
    shard
        .insert_many((0..KEYS).map(|k| (k, format!("value-{k}"))))
        .await
        .expect("insert_many");
    let resident =
        |shard: &Shard<u32, String>| (0..KEYS).filter(|k| shard.get_sync(k).is_some()).count();
    assert!(
        poll_until(POLL_TIMEOUT, || resident(&shard) <= CAP as usize).await,
        "the writes spill all but the cap"
    );

    let loads = AtomicUsize::new(0);
    for k in 0..KEYS {
        if k == KEYS - 1 {
            assert!(
                poll_until(POLL_TIMEOUT, || {
                    shard.engine.debug_pending_spill_weight() == 0
                })
                .await,
                "every hand-off installs before the last key is read"
            );
        }
        let value = if k % 2 == 0 {
            shard.get(&k).await
        } else {
            shard
                .get_or_load(&k, async |_| {
                    loads.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, std::io::Error>(String::new())
                })
                .await
                .ok()
        };
        assert_eq!(value, Some(format!("value-{k}")), "key {k} reads back");
    }
    assert_eq!(
        loads.load(Ordering::Relaxed),
        0,
        "every key reads back off disk"
    );
    assert!(
        poll_until(POLL_TIMEOUT, || resident(&shard) <= CAP as usize).await,
        "reads leave at most the cap resident, {} of {KEYS} are",
        resident(&shard)
    );
    let (live, weight) = shard.engine.debug_totals();
    assert_eq!(
        live,
        u64::from(KEYS),
        "every key stays live, in RAM or on disk"
    );
    assert!(
        weight <= u64::from(CAP),
        "resident weight {weight} is within the cap"
    );
    assert!(
        shard.get_sync(&(KEYS - 1)).is_some(),
        "the last key read stays in RAM"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A spilled entry heavier than the whole cap reads back off disk
/// on every read and stays there: no eviction makes room for it,
/// so reinstalling it would leave the shard over its cap.
#[tokio::test]
async fn a_spilled_entry_heavier_than_the_cap_reads_from_disk_and_stays_there() {
    const CAP: u64 = 10;
    let dir = temp_dir("oversize-promotion");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("oversize-promotion"),
        Mode::Local,
        NodeId::from(1u64),
        CAP,
        None,
        None,
    )
    .with_weigher(|_key: &u32, value: &String| u32::try_from(value.len()).unwrap_or(u32::MAX))
    .with_spill(&cfg)
    .expect("tier opens");
    let big = "x".repeat(64);
    shard.insert(1, big.clone()).await.expect("insert");
    assert!(
        poll_until(POLL_TIMEOUT, || shard.get_sync(&1).is_none()).await,
        "the write spills the entry heavier than the cap"
    );
    for read in 1..=3 {
        assert_eq!(shard.get(&1).await, Some(big.clone()), "read {read}");
        assert!(
            shard.get_sync(&1).is_none(),
            "read {read} leaves the entry on disk"
        );
    }
    assert_eq!(shard.engine.debug_totals(), (1, 0));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Spills all but the cap of `KEYS` keys in a shard of `mode`, takes
/// the whole flush queue, then reads every spilled key twice, first
/// with `get`, then with `get_or_load` and a loader that fails the
/// test if called: each read answers its value from disk, no entry
/// is deleted, the keys stay spilled for want of room, and the
/// refusals count as `deferred`, never as a delete reason: each read's
/// scan refuses the victim of each of the `CAP` resident stripes
/// once. With the queue free again, a read promotes.
async fn reads_behind_a_full_queue_delete_nothing(mode: Mode, name: &str) {
    use crate::store::spill::dropped_since_last_snapshot;
    const KEYS: u32 = 24;
    const CAP: u32 = 4;
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (shard, dir) = spill_shard_in(name, name, mode, u64::from(CAP));
    shard
        .insert_many((0..KEYS).map(|k| (k, format!("value-{k}"))))
        .await
        .expect("insert_many");
    let resident =
        |shard: &Shard<u32, String>| (0..KEYS).filter(|k| shard.get_sync(k).is_some()).count();
    assert!(
        poll_until(POLL_TIMEOUT, || {
            resident(&shard) <= CAP as usize && shard.engine.debug_pending_spill_weight() == 0
        })
        .await,
        "the writes spill all but the cap, every hand-off installed"
    );
    let spilled: Vec<u32> = (0..KEYS).filter(|k| shard.get_sync(k).is_none()).collect();
    assert_eq!(spilled.len(), (KEYS - CAP) as usize);
    assert!(
        dropped_since_last_snapshot(&snapshotter, name).is_empty(),
        "the writes drop nothing"
    );

    let hold = shard.debug_hold_flush_queue().await;
    let loads = AtomicUsize::new(0);
    for round in 1..=2 {
        for &key in &spilled {
            let value = if round == 1 {
                shard.get(&key).await
            } else {
                shard
                    .get_or_load(&key, async |_| {
                        loads.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, std::io::Error>(String::new())
                    })
                    .await
                    .ok()
            };
            assert_eq!(
                value,
                Some(format!("value-{key}")),
                "round {round}: key {key} answers from disk"
            );
        }
    }
    assert_eq!(loads.load(Ordering::Relaxed), 0, "no read calls the loader");

    assert_eq!(
        shard.engine.debug_totals(),
        (u64::from(KEYS), u64::from(CAP)),
        "every key stays live and the resident weight is unchanged"
    );
    for key in 0..KEYS {
        assert_eq!(
            shard.get_sync(&key).is_none(),
            spilled.contains(&key),
            "key {key} keeps its place, spilled or resident"
        );
    }
    let dropped = dropped_since_last_snapshot(&snapshotter, name);
    assert_eq!(
        dropped.keys().collect::<Vec<_>>(),
        ["deferred"],
        "the refusals count as deferred and nothing else: {dropped:?}"
    );
    assert_eq!(
        dropped["deferred"],
        u64::from(CAP) * 2 * spilled.len() as u64,
        "each read's scan refuses the victim of each resident stripe once"
    );

    drop(hold);
    let key = spilled[0];
    assert_eq!(shard.get(&key).await, Some(format!("value-{key}")));
    assert!(
        poll_until(POLL_TIMEOUT, || shard.get_sync(&key).is_some()).await,
        "with room in the queue the read promotes"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_local_shard_reads_spilled_keys_behind_a_full_queue_without_deleting_any() {
    reads_behind_a_full_queue_delete_nothing(Mode::Local, "reads-full-queue-local").await;
}

#[tokio::test]
async fn an_invalidation_shard_reads_spilled_keys_behind_a_full_queue_without_deleting_any() {
    reads_behind_a_full_queue_delete_nothing(Mode::Invalidation, "reads-full-queue-invalidation")
        .await;
}

/// Concurrent `get_or_load` calls of a spilled key that its read
/// cannot promote, one heavier than the whole cap, still collapse
/// to one disk read: the owner hands the value to every joined
/// waiter instead of leaving each to read the disk again.
///
/// The test holds every read permit until all the calls have
/// started, so the owner waits for a permit while the others join
/// it. Unheld, a blocking thread that finishes the read before the
/// owner first polls it ends the owner's load inside that poll, and
/// the next call starts a load of its own.
#[tokio::test]
async fn concurrent_loads_of_an_unpromotable_spilled_key_read_the_disk_once() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    const CALLS: usize = 16;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let dir = temp_dir("unpromotable-single-flight");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("unpromotable-single-flight"),
        Mode::Local,
        NodeId::from(1u64),
        10,
        None,
        None,
    )
    .with_weigher(|_key: &u32, value: &String| u32::try_from(value.len()).unwrap_or(u32::MAX))
    .with_spill(&cfg)
    .expect("tier opens");
    let big = "x".repeat(64);
    shard.insert(1, big.clone()).await.expect("insert");
    assert!(
        poll_until(POLL_TIMEOUT, || shard.get_sync(&1).is_none()).await,
        "the write spills the entry heavier than the cap"
    );
    let _ = snapshotter.snapshot();

    let held = hold_spill_reads(&shard).await;
    let loads = AtomicUsize::new(0);
    let mut calls = std::pin::pin!(futures::future::join_all((0..CALLS).map(|_| {
        shard.get_or_load(&1, async |_| {
            loads.fetch_add(1, Ordering::Relaxed);
            Ok::<_, std::io::Error>(String::new())
        })
    })));
    assert!(
        futures::poll!(calls.as_mut()).is_pending(),
        "the owner waits for a read permit and every other call joins its load"
    );
    drop(held);
    let answers = calls.await;
    assert!(
        answers
            .iter()
            .all(|answer| answer.as_ref().ok() == Some(&big)),
        "every call answers the spilled value"
    );
    assert_eq!(loads.load(Ordering::Relaxed), 0, "no call runs the loader");
    let disk_hits: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key().name() == "sundog_spill_reads_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "hit")
        })
        .map(|(.., value)| match value {
            DebugValue::Counter(n) => n,
            _ => 0,
        })
        .sum();
    assert_eq!(disk_hits, 1, "{CALLS} concurrent loads read the disk once");
    assert!(shard.get_sync(&1).is_none(), "the entry stays on disk");
    let _ = std::fs::remove_dir_all(&dir);
}

/// `load_many`'s counterpart of
/// [`concurrent_loads_of_an_unpromotable_spilled_key_read_the_disk_once`]:
/// concurrent calls of a spilled key too heavy to promote share the
/// owner's one disk read and never reach the loader. The read
/// permits are held while the calls start, for the same reason.
#[tokio::test]
async fn concurrent_load_manys_of_an_unpromotable_spilled_key_read_the_disk_once() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    const CALLS: usize = 16;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let dir = temp_dir("unpromotable-load-many");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let loads = Arc::new(AtomicUsize::new(0));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("unpromotable-load-many"),
        Mode::Local,
        NodeId::from(1u64),
        10,
        None,
        None,
    )
    .with_weigher(|_key: &u32, value: &String| u32::try_from(value.len()).unwrap_or(u32::MAX))
    .with_loader(crate::store::load::Loader::batch({
        let loads = Arc::clone(&loads);
        move |_keys: Vec<u32>| {
            loads.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, std::io::Error>(HashMap::new()) }
        }
    }))
    .with_spill(&cfg)
    .expect("tier opens");
    let big = "x".repeat(64);
    shard.insert(1, big.clone()).await.expect("insert");
    assert!(
        poll_until(POLL_TIMEOUT, || shard.get_sync(&1).is_none()).await,
        "the write spills the entry heavier than the cap"
    );
    let _ = snapshotter.snapshot();

    let held = hold_spill_reads(&shard).await;
    let mut calls = std::pin::pin!(futures::future::join_all(
        (0..CALLS).map(|_| shard.load_many([1u32]))
    ));
    assert!(
        futures::poll!(calls.as_mut()).is_pending(),
        "the owner waits for a read permit and every other call joins its load"
    );
    drop(held);
    let answers = calls.await;
    assert!(
        answers
            .iter()
            .all(|answer| answer.as_ref().ok().and_then(|found| found.get(&1)) == Some(&big)),
        "every call answers the spilled value"
    );
    assert_eq!(
        loads.load(Ordering::Relaxed),
        0,
        "no call reaches the loader"
    );
    let disk_hits: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key().name() == "sundog_spill_reads_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "hit")
        })
        .map(|(.., value)| match value {
            DebugValue::Counter(n) => n,
            _ => 0,
        })
        .sum();
    assert_eq!(
        disk_hits, 1,
        "{CALLS} concurrent load_manys read the disk once"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A `load_many` dropped while it waits to read a later key's
/// spilled value frees the in-flight entries of the keys it already
/// claimed: a later load of such a key runs rather than join a load
/// that never ends.
#[tokio::test]
async fn a_load_many_dropped_mid_claim_frees_the_keys_it_claimed() {
    let dir = temp_dir("load-many-dropped-mid-claim");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("load-many-dropped-mid-claim"),
        Mode::Local,
        NodeId::from(1u64),
        1,
        None,
        None,
    )
    .with_loader(crate::store::load::Loader::batch(
        |keys: Vec<u32>| async move {
            Ok::<_, std::io::Error>(
                keys.into_iter()
                    .map(|key| (key, format!("loaded-{key}")))
                    .collect::<HashMap<_, _>>(),
            )
        },
    ))
    .with_spill(&cfg)
    .expect("tier opens");
    shard.insert(2, "two".to_string()).await.expect("insert 2");
    shard
        .insert(3, "three".to_string())
        .await
        .expect("insert 3");
    assert!(
        poll_until(POLL_TIMEOUT, || {
            shard.get_sync(&2).is_none() || shard.get_sync(&3).is_none()
        })
        .await,
        "one of the two keys spills under the one-entry cap"
    );
    let spilled = if shard.get_sync(&2).is_none() { 2 } else { 3 };

    let held = hold_spill_reads(&shard).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), shard.load_many([1, spilled]))
            .await
            .is_err(),
        "the call claims key 1, then waits on the held read permits for the spilled key"
    );
    drop(held);

    let loaded = tokio::time::timeout(Duration::from_secs(5), shard.load(&1))
        .await
        .expect("a later load of the claimed key runs rather than hang");
    assert_eq!(loaded.expect("load"), Some("loaded-1".to_string()));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A write burst far past the cap settles within it once the
/// flusher installs its hand-offs: an eviction pass that found
/// nothing to evict leaves the rest to the flusher only when the
/// hand-offs in flight are all that hold the shard over its cap.
#[tokio::test]
async fn a_write_burst_past_the_cap_settles_within_it() {
    const KEYS: u32 = 8_000;
    const CAP: u64 = 2_000;
    let dir = temp_dir("write-burst-within-cap");
    // Generous past the 2s default, so a contended CI disk slows the
    // burst rather than dropping a victim at its reservation wait.
    let cfg = SpillConfig::new(&dir, 4 << 20)
        .region_bytes(256 * 1024)
        .spill_wait_timeout(Duration::from_secs(20));
    let shard = Shard::<u32, String>::new(
        SmolStr::new("write-burst-within-cap"),
        Mode::Local,
        NodeId::from(1u64),
        CAP,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    shard
        .insert_many((0..KEYS).map(|k| (k, format!("value-{k}"))))
        .await
        .expect("insert_many");
    let resident = || -> u64 {
        (0..KEYS)
            .filter(|k| shard.get_sync(k).is_some())
            .map(|_| 1)
            .sum()
    };
    assert!(
        poll_until(POLL_TIMEOUT, || resident() <= CAP).await,
        "the burst settles within the cap, {} of {KEYS} resident",
        resident()
    );
    let (live, weight) = shard.engine.debug_totals();
    assert_eq!(live, u64::from(KEYS), "every key stays live");
    assert!(weight <= CAP, "resident weight {weight} is within the cap");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Extends [`insert_sync_writes_a_value_with_no_async_runtime`]:
/// `insert_sync` completes with no runtime even while another task
/// holds the whole reservation.
#[test]
fn insert_sync_completes_synchronously_while_another_task_holds_the_whole_reservation() {
    let dir = temp_dir("insert-sync-under-reservation");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(64);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("insert-sync-under-reservation"),
        Mode::Local,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));

    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("a fresh runtime builds");
        rt.block_on(async move {
            let reservation = tier
                .reserve(64, Duration::from_secs(30))
                .await
                .expect("the whole queue is free before this call");
            ready_tx.send(()).expect("the main thread is listening");
            let _ = release_rx.recv();
            drop(reservation);
        });
    });
    ready_rx
        .recv()
        .expect("the holder thread reports it holds the reservation");

    // Runs with no async runtime here while the reservation stays
    // live on another thread.
    shard.insert_sync(1, "a".to_string()).expect("insert_sync");
    assert_eq!(shard.get_sync(&1), Some("a".to_string()));

    release_tx.send(()).expect("the holder thread is listening");
    holder.join().expect("the holder thread does not panic");
    let _ = std::fs::remove_dir_all(&dir);
}

/// With a flush queue large enough that a small record is charged its share
/// of a channel slot, a batch reserves what each of its hand-offs will cost,
/// not the record lengths.
#[tokio::test]
async fn a_batch_reservation_covers_the_floor_charge_of_small_values() {
    let dir = temp_dir("batch-reservation-floor");
    let budget = 64 << 20;
    let cfg = SpillConfig::new(&dir, 64 << 20)
        .region_bytes(1 << 20)
        .flush_queue_bytes(budget);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("batch-reservation-floor"),
        Mode::Local,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    let tier = Arc::clone(shard.engine.spill().expect("with_spill attaches a tier"));
    let floor = u64::from(tier.admit_charge(1, 1));
    assert_eq!(floor, 256, "a small record costs its slot share");
    assert_eq!(tier.queued_bytes(), 0);

    let entries: Vec<(Bytes, Incoming<String>)> = (0..3u32)
        .map(|key| {
            let value = "v".to_string();
            (
                key_bytes(&key),
                Incoming::Put {
                    encoded: Bytes::from(postcard::to_stdvec(&value).expect("encodes")),
                    value,
                    expires_at_ms: None,
                },
            )
        })
        .collect();
    let (reservation, wait_left) = shard
        .reserve_for_batch(entries.iter().map(|(key, incoming)| (key, incoming)))
        .await;

    assert!(reservation.is_some() && wait_left.is_some());
    assert_eq!(
        tier.queued_bytes(),
        3 * floor,
        "the reservation holds the charge of each hand-off"
    );
    drop(reservation);
    assert_eq!(tier.queued_bytes(), 0, "unspent budget returns on drop");

    let _ = std::fs::remove_dir_all(&dir);
}
