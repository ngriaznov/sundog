//! Tests for eviction over a flush queue whose byte budget is larger than its channel can hold.

use super::*;

/// The flush queue byte budget of a default spill tier, one region.
const DEFAULT_QUEUE_BYTES: u64 = 64 << 20;

/// The most stripe locks a pass that ends takes: one batch, then one scan
/// of every stripe, with room for a second scan.
const SCAN_BOUND: u64 = 2 * BUCKET_COUNT as u64 + 2;

/// How long a pass gets to end before it counts as spinning.
const PASS_DEADLINE: Duration = Duration::from_secs(2);

/// A filler job of the smallest size, for saturating the queue through
/// admission.
fn filler(n: usize) -> SpillJob {
    SpillJob {
        stripe_idx: 0,
        hash: 0,
        key_bytes: Bytes::from(format!("filler-{n}")),
        ver: hlc(0, 0),
        expires_at_ms: None,
        encoded: Bytes::from_static(b"f"),
        weight: 1,
        admitted_bytes: 0,
    }
}

/// An engine capped at 150 under a length weigher, holding two
/// 100-byte entries, over a tier with the default 64 MiB flush queue
/// whose flusher is parked before it takes a job and whose channel is full
/// of fillers admitted through the byte budget. The tier's refusal policy is
/// `keep_resident`.
fn engine_over_a_saturated_default_queue(
    label: &str,
    keep_resident: bool,
) -> (Arc<Engine<u32, String>>, Arc<SpillTier>, std::path::PathBuf) {
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 64 << 20)
        .region_bytes(1 << 20)
        .flush_queue_bytes(DEFAULT_QUEUE_BYTES);
    let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
    assert_eq!(slots, 262_144, "the default budget clamps the slots");
    let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
    tier.set_keep_resident_when_refused(keep_resident);
    // Parked before the flusher exists, so it never takes a job off the
    // channel.
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(150, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let mut admitted = 0usize;
    while tier.try_spill(filler(admitted)) {
        admitted += 1;
        assert!(admitted <= slots, "admission never outruns the channel");
    }
    assert!(admitted > 0, "the queue took fillers");

    for key in [1u32, 2] {
        let _ = put(
            &engine,
            key,
            key_bytes(key),
            "x".repeat(100),
            hlc(u64::from(key), 1),
            None,
            0,
        );
    }
    assert_eq!(engine.debug_totals().1, 200, "200 bytes over a 150 cap");
    (engine, tier, dir)
}

/// Runs `pass` against `engine` on a helper thread and returns whether it
/// ended within [`PASS_DEADLINE`]. Frees a spinning pass by resuming the
/// flusher either way, so the test process winds down.
fn ends_in_time(
    engine: &Arc<Engine<u32, String>>,
    tier: &SpillTier,
    pass: impl FnOnce(&Engine<u32, String>) + Send + 'static,
) -> bool {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let evicting = Arc::clone(engine);
    std::thread::spawn(move || {
        pass(&evicting);
        let _ = done_tx.send(());
    });
    let finished = done_rx.recv_timeout(PASS_DEADLINE).is_ok();
    tier.resume_flusher();
    finished
}

fn first_bucket() -> usize {
    stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()))
}

#[test]
fn enforce_capacity_deletes_a_victim_when_small_hand_offs_fill_the_flush_queue() {
    let (engine, tier, dir) = engine_over_a_saturated_default_queue("slots-delete", false);
    let bucket = first_bucket();
    assert!(
        ends_in_time(&engine, &tier, move |e| {
            e.enforce_capacity(bucket, &mut Written::default(), 1);
        }),
        "a pass ends when the queue refuses a hand-off, instead of spinning"
    );
    assert_eq!(
        engine.debug_totals().1,
        100,
        "a tier that deletes on refusal drops one victim and is back under the cap"
    );
    assert!(
        engine.debug_eviction_lock_acquisitions() <= SCAN_BOUND,
        "the loop does not spin: {} lock acquisitions",
        engine.debug_eviction_lock_acquisitions()
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn enforce_capacity_keeps_both_entries_when_a_keep_resident_tier_finds_the_queue_full() {
    let (engine, tier, dir) = engine_over_a_saturated_default_queue("slots-keep", true);
    let bucket = first_bucket();
    assert!(
        ends_in_time(&engine, &tier, move |e| {
            e.enforce_capacity(bucket, &mut Written::default(), 1);
        }),
        "a pass ends when the queue refuses a hand-off, instead of spinning"
    );
    assert_eq!(
        engine.debug_totals(),
        (2, 200),
        "a Replicated or Distributed tier leaves both victims resident"
    );
    assert!(
        engine.debug_eviction_lock_acquisitions() <= SCAN_BOUND,
        "the loop does not spin: {} lock acquisitions",
        engine.debug_eviction_lock_acquisitions()
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_promotion_pass_ends_and_deletes_nothing_when_the_queue_is_full() {
    let (engine, tier, dir) = engine_over_a_saturated_default_queue("slots-promote", false);
    let bucket = first_bucket();
    assert!(
        ends_in_time(&engine, &tier, move |e| {
            let _ = e.evict_down_to(
                100,
                bucket,
                None,
                &mut Written::default(),
                OnRefusal::KeepResident,
                1,
            );
        }),
        "a promotion's eviction ends when the queue refuses a hand-off"
    );
    assert_eq!(
        engine.debug_totals(),
        (2, 200),
        "a read never deletes an entry, in any mode"
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_abandoned_hand_off_is_not_progress_so_a_pass_over_a_tier_with_no_flusher_ends() {
    // A tier with no flusher attached admits within its budget and then
    // fails every enqueue, the same way a dead flusher does.
    let label = "slots-no-flusher";
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(150, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    for key in [1u32, 2] {
        let _ = put(
            &engine,
            key,
            key_bytes(key),
            "x".repeat(100),
            hlc(u64::from(key), 1),
            None,
            0,
        );
    }

    let bucket = first_bucket();
    assert!(
        ends_in_time(&engine, &tier, move |e| {
            e.enforce_capacity(bucket, &mut Written::default(), 1);
        }),
        "a hand-off the channel cannot take is not eviction progress"
    );
    assert_eq!(
        engine.debug_totals(),
        (2, 200),
        "the abandoned victims stay resident at their full weight"
    );
    assert_eq!(
        engine.pending_spill_weight_or_zero(),
        0,
        "no hand-off is left pending"
    );
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_abandoned_hand_off_is_not_progress_in_a_batch_either() {
    let label = "slots-no-flusher-batch";
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(150, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    for key in [1u32, 2] {
        let _ = put(
            &engine,
            key,
            key_bytes(key),
            "x".repeat(100),
            hlc(u64::from(key), 1),
            None,
            0,
        );
    }

    let stripe_a = stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()));
    let stripe_b = stripe_index_from_hash(hash_key_bytes(key_bytes(2).as_ref()));
    let outcomes = [stripe_a, stripe_b].map(|bucket| engine.evict_batch_sampled(bucket, 50, 1));

    for outcome in outcomes {
        assert_eq!(
            outcome.removed_weight, 0,
            "an abandoned hand-off frees nothing"
        );
    }
    assert!(
        outcomes.iter().any(|outcome| outcome.refused),
        "the stripe that held a victim reports the hand-off as refused"
    );
    assert_eq!(
        engine.debug_totals(),
        (2, 200),
        "the weight an abandoned hand-off restores is not lost or counted twice"
    );
    assert_eq!(engine.pending_spill_weight_or_zero(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}
