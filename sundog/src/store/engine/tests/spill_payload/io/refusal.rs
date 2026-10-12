//! Tests for how eviction and promotion treat a victim the flush queue refuses.

use super::*;

/// An engine capped at `cap` under a length weigher, over a
/// keep-resident tier whose one-record flush queue holds a
/// hand-off the paused flusher never writes, and one resident
/// 200-byte entry that no eviction can move while the queue
/// stays full. Returns the engine, its tier, the scratch
/// directory and the stuck entry's key.
fn engine_with_a_full_queue(
    label: &str,
    cap: u64,
) -> (
    Arc<Engine<u32, String>>,
    Arc<SpillTier>,
    std::path::PathBuf,
    u32,
) {
    engine_with_a_full_queue_on(label, cap, true)
}

/// [`engine_with_a_full_queue`] over a tier whose own policy is
/// `keep_resident`: `false` is a `Mode::Local` or
/// `Mode::Invalidation` tier, which deletes the victim a write's
/// eviction finds refused.
fn engine_with_a_full_queue_on(
    label: &str,
    cap: u64,
    keep_resident: bool,
) -> (
    Arc<Engine<u32, String>>,
    Arc<SpillTier>,
    std::path::PathBuf,
    u32,
) {
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(300);
    let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
    tier.set_keep_resident_when_refused(keep_resident);
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(cap, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let _ = put(
        &engine,
        1,
        key_bytes(1),
        "x".repeat(200),
        hlc(1, 1),
        None,
        0,
    );
    let first = stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()));
    engine.evict_one_sampled(first, 1);
    assert_eq!(
        engine.debug_pending_spill_weight(),
        200,
        "the first entry's hand-off fills the queue"
    );
    let _ = put(
        &engine,
        2,
        key_bytes(2),
        "y".repeat(200),
        hlc(2, 1),
        None,
        1,
    );
    assert_eq!(engine.debug_totals().1, 200);
    (engine, tier, dir, 2)
}

/// A spilled entry `key` with nothing resident for it, as a
/// flusher install leaves one, and its encoded `value`.
fn spilled_entry(engine: &Engine<u32, String>, key: u32, value: &str) -> Bytes {
    engine.debug_insert_spilled(
        &key_bytes(key),
        hlc(10, 1),
        None,
        super::loc(0, 0, 10, 0),
        2,
    );
    Bytes::from(postcard::to_stdvec(&value.to_string()).expect("string encodes"))
}

/// With no room to make, a full flush queue holding the only
/// resident entry in place, a promotion leaves the read's entry
/// on disk rather than take RAM past the cap.
#[test]
fn a_promotion_with_no_room_to_make_leaves_the_entry_on_disk() {
    let (engine, tier, dir, stuck) = engine_with_a_full_queue("promote-no-room", 50);
    let value = "z".repeat(10);
    let encoded = spilled_entry(&engine, 7, &value);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());

    assert!(!engine.promote_locked(kb.as_ref(), hash, hlc(10, 1), &value, &encoded, 3));
    assert!(is_spilled(&engine, &kb), "the read's entry stays on disk");
    assert_eq!(engine.debug_totals().1, 200, "total_weight is unchanged");
    assert_eq!(
        engine.get(&stuck, 3),
        Some("y".repeat(200)),
        "the stuck entry stays resident"
    );

    tier.resume_flusher();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A promotion's eviction never deletes the victim its tier
/// refuses, whatever the tier's own policy: behind a full flush
/// queue on a tier that deletes (`Mode::Local`,
/// `Mode::Invalidation`), every resident entry stays resident,
/// the promotion finds no room, and the refusal counts as
/// `deferred`, never `queue_full`.
#[test]
fn a_promotion_behind_a_full_queue_keeps_every_refused_victim_resident_on_a_tier_that_deletes() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let label = "promote-keeps-victims";
    let (engine, tier, dir, stuck) = engine_with_a_full_queue_on(label, 50, false);
    assert!(
        !tier.keep_resident_when_refused(),
        "the tier deletes on a write"
    );
    for key in [3u32, 4] {
        let _ = put(
            &engine,
            key,
            key_bytes(key),
            "w".repeat(200),
            hlc(u64::from(key), 1),
            None,
            1,
        );
    }
    let value = "z".repeat(10);
    let encoded = spilled_entry(&engine, 7, &value);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());
    let totals_before = engine.debug_totals();
    assert_eq!(totals_before.1, 600, "three resident 200-byte entries");

    assert!(!engine.promote_locked(kb.as_ref(), hash, hlc(10, 1), &value, &encoded, 3));

    assert_eq!(
        engine.debug_totals(),
        totals_before,
        "no entry was deleted and none was added"
    );
    assert!(is_spilled(&engine, &kb), "the read's entry stays on disk");
    for (key, ch) in [(stuck, 'y'), (3, 'w'), (4, 'w')] {
        assert_eq!(
            engine.get(&key, 3),
            Some(ch.to_string().repeat(200)),
            "key {key} stays resident"
        );
    }
    assert_eq!(
        crate::store::spill::dropped_since_last_snapshot(&snapshotter, label),
        std::collections::BTreeMap::from([("deferred".to_string(), 1)]),
        "the full queue refuses the one victim of the pass and ends it; \
         nothing counts as dropped under a delete reason"
    );

    tier.resume_flusher();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The write-side guard for
/// `a_promotion_behind_a_full_queue_keeps_every_refused_victim_resident_on_a_tier_that_deletes`:
/// on the same full queue and the same tier, a write's eviction
/// keeps the tier's policy, deleting the victim it finds
/// refused and counting it `queue_full`, never `deferred`.
#[test]
fn a_write_behind_a_full_queue_still_drops_its_refused_victim_as_queue_full_on_a_tier_that_deletes()
{
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let label = "write-drops-victim";
    let (engine, tier, dir, stuck) = engine_with_a_full_queue_on(label, 50, false);
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(stuck).as_ref()));

    engine.enforce_capacity(bucket, &mut Written::default(), 2);

    assert_eq!(
        engine.get(&stuck, 2),
        None,
        "the tier's policy deletes the refused victim"
    );
    assert_eq!(engine.debug_totals().1, 0);
    assert_eq!(
        crate::store::spill::dropped_since_last_snapshot(&snapshotter, label),
        std::collections::BTreeMap::from([("queue_full".to_string(), 1)]),
    );

    tier.resume_flusher();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The batch pass is the first and main path of a promotion whose
/// stripe holds other residents: two residents sharing the read
/// key's stripe make the pass sample the colder one, the full
/// queue refuses it, and a tier that deletes on a write keeps it
/// resident, counted `deferred`, with the pending hand-off
/// ending the pass before any scan.
#[test]
fn a_promotion_keeps_the_refused_victim_its_own_stripe_batch_samples() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let label = "promote-batch-keeps";
    let (engine, tier, dir, stuck) = engine_with_a_full_queue_on(label, 50, false);
    let value = "z".repeat(10);
    let encoded = spilled_entry(&engine, 7, &value);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());
    let read_stripe = stripe_index_from_hash(hash);
    let sharing: Vec<u32> = (100u32..)
        .filter(|&key| {
            stripe_index_from_hash(hash_key_bytes(key_bytes(key).as_ref())) == read_stripe
        })
        .take(2)
        .collect();
    for &key in &sharing {
        let _ = put(
            &engine,
            key,
            key_bytes(key),
            "w".repeat(200),
            hlc(u64::from(key), 1),
            None,
            1,
        );
    }
    let totals_before = engine.debug_totals();
    assert_eq!(totals_before.1, 600);

    assert!(!engine.promote_locked(kb.as_ref(), hash, hlc(10, 1), &value, &encoded, 3));

    assert_eq!(engine.debug_totals(), totals_before);
    assert!(is_spilled(&engine, &kb), "the read's entry stays on disk");
    for &key in &sharing {
        assert_eq!(engine.get(&key, 3), Some("w".repeat(200)));
    }
    assert_eq!(engine.get(&stuck, 3), Some("y".repeat(200)));
    assert_eq!(
        crate::store::spill::dropped_since_last_snapshot(&snapshotter, label),
        std::collections::BTreeMap::from([("deferred".to_string(), 1)]),
        "the batch evicts the colder half of two candidates, one victim, and the \
         refusal under a pending hand-off ends the pass"
    );

    tier.resume_flusher();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// [`Engine::evict_batch_sampled_with_reservation`] and
/// [`Engine::evict_one_sampled_with_reservation`] apply the
/// pass's [`OnRefusal`] to the victim a full queue refuses, on a
/// tier that deletes on a write: `KeepResident` leaves it
/// resident, reports the pass as refused and counts `deferred`;
/// `TierPolicy` removes it and counts `queue_full`.
#[test]
fn the_batch_and_single_passes_apply_the_on_refusal_policy_to_a_refused_victim() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    for (pass, batch) in [("batch", true), ("one", false)] {
        for (on_refusal, kept, reason) in [
            (OnRefusal::KeepResident, true, "deferred"),
            (OnRefusal::TierPolicy, false, "queue_full"),
        ] {
            let label = format!("on-refusal-{pass}-{reason}");
            let (engine, tier, dir, stuck) = engine_with_a_full_queue_on(&label, 50, false);
            let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(stuck).as_ref()));
            let written = Written::default();
            let outcome = if batch {
                engine.evict_batch_sampled_with_reservation(
                    bucket, 150, None, &written, false, on_refusal, 3,
                )
            } else {
                engine.evict_one_sampled_with_reservation(bucket, None, &written, on_refusal, 3)
            };
            let what = format!("{pass} pass, {on_refusal:?}");
            assert_eq!(outcome.refused, kept, "{what}");
            assert_eq!(outcome.removed_weight, if kept { 0 } else { 200 }, "{what}");
            assert_eq!(
                engine.get(&stuck, 3),
                kept.then(|| "y".repeat(200)),
                "{what}"
            );
            assert_eq!(
                crate::store::spill::dropped_since_last_snapshot(&snapshotter, &label),
                std::collections::BTreeMap::from([(reason.to_string(), 1)]),
                "{what}"
            );
            tier.resume_flusher();
            drop(engine);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

/// A hand-off a promotion's eviction is admitted for can still
/// fail at the channel, once its slots run out before the byte
/// budget does. Nothing is deleted, the abandoned victim stays
/// resident, and the failure counts `deferred` for a keep-resident
/// pass on a tier that deletes, `queue_full` for the tier's own
/// policy.
#[test]
fn a_hand_off_failing_at_the_channel_records_the_reason_of_the_pass() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    for (on_refusal, reason) in [
        (OnRefusal::KeepResident, "deferred"),
        (OnRefusal::TierPolicy, "queue_full"),
    ] {
        let label = format!("channel-full-{reason}");
        let dir = temp_dir(&label);
        let cfg = SpillConfig::new(&dir, 1 << 20)
            .region_bytes(4096)
            .flush_queue_bytes(1 << 20);
        let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
        let tier = Arc::new(SpillTier::open(&cfg, &label).expect("tier opens"));
        assert!(!tier.keep_resident_when_refused());
        tier.pause_flusher();
        let weigher: Weigher<u32, String> =
            Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
        let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
        engine.set_spill(Arc::clone(&tier));
        let engine = Arc::new(engine);
        tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));
        for i in 0..slots {
            let filler = SpillJob {
                stripe_idx: 0,
                hash: 0,
                key_bytes: Bytes::from(format!("filler-{i}")),
                ver: hlc(0, 0),
                expires_at_ms: None,
                encoded: Bytes::from_static(b"f"),
                weight: 1,
                admitted_bytes: 0,
            };
            tier.enqueue(OnRefusal::TierPolicy, filler)
                .unwrap_or_else(|_| panic!("channel has room for filler {i}"));
        }
        assert_eq!(
            crate::store::spill::dropped_since_last_snapshot(&snapshotter, &label),
            std::collections::BTreeMap::new(),
            "the fillers fit"
        );

        let key = 1u32;
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(key).as_ref()));
        let _ = put(
            &engine,
            key,
            key_bytes(key),
            "x".repeat(30),
            hlc(1, 1),
            None,
            0,
        );
        let _ = engine.evict_one_sampled_with_reservation(
            bucket,
            None,
            &Written::default(),
            on_refusal,
            0,
        );

        assert_eq!(engine.get(&key, 0), Some("x".repeat(30)), "{on_refusal:?}");
        assert_eq!(engine.debug_totals().1, 30, "{on_refusal:?}");
        assert_eq!(
            crate::store::spill::dropped_since_last_snapshot(&snapshotter, &label),
            std::collections::BTreeMap::from([(reason.to_string(), 1)]),
            "{on_refusal:?}"
        );
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A refusal no flusher install can resolve, a record larger
/// than a region with no hand-off in flight, leaves a
/// promotion's eviction nothing to wait for: its scan visits
/// every stripe once, defers each victim, and returns with
/// every entry resident and the read's entry still on disk,
/// on a tier that deletes as on one that keeps.
#[test]
fn a_promotion_whose_victims_are_too_large_for_a_region_ends_with_every_entry_resident() {
    let recorder = Arc::new(metrics_util::debugging::DebuggingRecorder::new());
    let snapshotter = recorder.snapshotter();
    let label = "promote-too-large";
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
    let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
    assert!(!tier.keep_resident_when_refused());
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(50, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let value = "z".repeat(10);
    let encoded = spilled_entry(&engine, 7, &value);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());
    // Three resident entries, each alone in a stripe other than
    // the read's: the pass's first batch finds an empty stripe,
    // and the scan refuses one victim per resident.
    let mut taken = vec![stripe_index_from_hash(hash)];
    let mut residents = Vec::new();
    for key in 100u32.. {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(key).as_ref()));
        if !taken.contains(&bucket) {
            taken.push(bucket);
            residents.push(key);
        }
        if residents.len() == 3 {
            break;
        }
    }
    for &key in &residents {
        let _ = put(
            &engine,
            key,
            key_bytes(key),
            "w".repeat(200),
            hlc(u64::from(key), 1),
            None,
            1,
        );
    }
    let totals_before = engine.debug_totals();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let promoting = Arc::clone(&engine);
    let (promote_kb, promote_value, promote_encoded) = (kb.clone(), value.clone(), encoded.clone());
    let thread_recorder = Arc::clone(&recorder);
    std::thread::spawn(move || {
        // The recorder is per thread.
        let _guard = metrics::set_default_local_recorder(&*thread_recorder);
        let promoted = promoting.promote_locked(
            promote_kb.as_ref(),
            hash,
            hlc(10, 1),
            &promote_value,
            &promote_encoded,
            3,
        );
        let _ = done_tx.send(promoted);
    });
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(10)),
        Ok(false),
        "the promotion returns without room rather than loop over the refusals"
    );

    assert_eq!(engine.debug_totals(), totals_before);
    assert!(is_spilled(&engine, &kb));
    for &key in &residents {
        assert_eq!(engine.get(&key, 3), Some("w".repeat(200)));
    }
    assert_eq!(
        crate::store::spill::dropped_since_last_snapshot(&snapshotter, label),
        std::collections::BTreeMap::from([("deferred".to_string(), 3)]),
        "one deferred victim per resident stripe the scan visits"
    );

    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A pass that starts on an empty stripe scans on to the stripe
/// whose victim the full queue refuses, and stops there: the
/// queue would refuse every other stripe's victim too, so
/// scanning all 1024 stripes, or sampling more of them, would
/// buy nothing.
#[test]
fn the_scan_stops_at_a_victim_the_full_queue_refuses() {
    let (engine, tier, dir, stuck) = engine_with_a_full_queue("scan-refusal-stop", 50);
    let stuck_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(stuck).as_ref()));
    let start = (stuck_bucket + BUCKET_COUNT - 1) % BUCKET_COUNT;
    let locks_before = engine.debug_eviction_lock_acquisitions();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let enforcing = Arc::clone(&engine);
    std::thread::spawn(move || {
        enforcing.enforce_capacity(start, &mut Written::default(), 2);
        let _ = done_tx.send(());
    });
    assert!(
        done_rx.recv_timeout(Duration::from_secs(10)).is_ok(),
        "enforce_capacity returns rather than loop over the refusal"
    );
    let locks = engine.debug_eviction_lock_acquisitions() - locks_before;
    assert!(
        locks <= 4,
        "the pass and its scan stop at the refused victim, took {locks} stripe locks"
    );
    assert_eq!(
        engine.debug_totals().1,
        200,
        "the refused victim stays resident"
    );

    tier.resume_flusher();
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two promotions racing for the one free slot under the cap,
/// with nothing evictable to make more: exactly one claims it,
/// and `total_weight` lands on the cap, never past it.
#[test]
fn concurrent_promotions_never_claim_the_same_room_twice() {
    const ROUNDS: u32 = 50;
    for round in 0..ROUNDS {
        let (engine, tier, dir, _) =
            engine_with_a_full_queue(&format!("promote-race-{round}"), 210);
        let value = "z".repeat(10);
        let encoded = spilled_entry(&engine, 7, &value);
        let _ = spilled_entry(&engine, 8, &value);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let promoted: Vec<bool> = [7u32, 8]
            .into_iter()
            .map(|key| {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                let (value, encoded) = (value.clone(), encoded.clone());
                std::thread::spawn(move || {
                    let kb = key_bytes(key);
                    let hash = hash_key_bytes(kb.as_ref());
                    barrier.wait();
                    engine.promote_locked(kb.as_ref(), hash, hlc(10, 1), &value, &encoded, 3)
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().expect("the promoting thread finishes"))
            .collect();
        assert_eq!(
            promoted.iter().filter(|&&won| won).count(),
            1,
            "round {round}: exactly one promotion claims the slot, got {promoted:?}"
        );
        assert_eq!(engine.debug_totals().1, 210, "round {round}: at the cap");

        tier.resume_flusher();
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
