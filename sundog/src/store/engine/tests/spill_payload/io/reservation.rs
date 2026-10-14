//! Tests for byte reservations across the hand-off, admission and the capacity passes.

use super::*;

#[test]
fn finish_spill_handoff_err_releases_the_jobs_admitted_bytes() {
    let dir = temp_dir("release-admitted-bytes-on-err");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(1 << 20);
    let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
    let tier = Arc::new(SpillTier::open(&cfg, "release-on-err").expect("tier opens"));
    // Paused before the flusher spawns, so the channel stays
    // completely full when the real eviction's enqueue needs it.
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

    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb.clone(), "x".repeat(30), hlc(1, 1), None, 0);

    let queued_before = tier.queued_bytes();
    let outcome = engine.evict_one_sampled(bucket, 0);
    assert_eq!(
        outcome.removed_weight, 0,
        "the abandoned hand-off frees nothing, so the pass reports no progress"
    );
    let queued_after = tier.queued_bytes();

    assert_eq!(
        queued_after, queued_before,
        "the abandoned job's admitted bytes return to admit: queued_bytes ends \
         where it started, not permanently inflated by a job that never reached \
         the flusher's channel"
    );
    assert_eq!(
        engine.get(&key, 0),
        Some("x".repeat(30)),
        "abandon restores full residency"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_sufficient_reservation_never_falls_back_to_the_global_admit_check() {
    let dir = temp_dir("reservation-never-falls-back");
    let value = "x".repeat(20);
    // The real postcard-encoded byte length, what
    // spill_record_len/would_accept see, not the char count.
    let encoded_len = postcard::to_stdvec(&value)
        .expect("test value encodes")
        .len();
    let key = 1u32;
    let kb = key_bytes(key);
    let record_len = spill_record_len(kb.len(), encoded_len);
    // A flush queue sized to exactly one record, so the next
    // admission is refused once admit is drained.
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(u64::from(record_len));
    let tier = Arc::new(SpillTier::open(&cfg, "reservation-never").expect("tier opens"));
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb.clone(), value.clone(), hlc(1, 1), None, 0);

    // Drains admit entirely into a caller-held Reservation
    // before eviction ever samples the victim.
    let mut reservation = tier
        .reserve(record_len, Duration::from_secs(5))
        .await
        .expect("the whole (single-record) queue is free before this call");
    assert_eq!(
        tier.would_accept(None, kb.len(), encoded_len),
        Admission::RefusedFinal,
        "with no reservation threaded through, admit's own fallback refuses: \
         fully drained by the reservation above"
    );

    let outcome = engine.evict_one_sampled_with_reservation(
        bucket,
        Some(&mut reservation),
        &Written::default(),
        OnRefusal::TierPolicy,
        0,
    );
    assert_eq!(
        outcome.removed_weight,
        value.len() as u64,
        "the reservation's own pre-paid budget admits the victim; admit itself \
         never needed to grant anything"
    );
    assert_eq!(
        engine.get(&key, 0),
        Some(value),
        "still fully resident: a hand-off, not a removal"
    );

    drop(reservation);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn committed_bytes_returned_match_the_sum_of_admitted_job_lengths() {
    let dir = temp_dir("admitted-bytes-sum");
    let value_a = "x".repeat(20);
    let value_b = "y".repeat(20);
    // The real postcard-encoded byte lengths, what
    // spill_record_len/would_accept see.
    let encoded_len_a = postcard::to_stdvec(&value_a)
        .expect("test value encodes")
        .len();
    let encoded_len_b = postcard::to_stdvec(&value_b)
        .expect("test value encodes")
        .len();
    let key_a = 1u32;
    let kb_a = key_bytes(key_a);
    let hash_a = hash_key_bytes(kb_a.as_ref());
    let bucket = stripe_index_from_hash(hash_a);
    let record_len_a = spill_record_len(kb_a.len(), encoded_len_a);

    let mut key_b = None;
    for k in 2..100_000u32 {
        let kb = key_bytes(k);
        if stripe_index_from_hash(hash_key_bytes(kb.as_ref())) == bucket {
            key_b = Some(k);
            break;
        }
    }
    let key_b = key_b.expect("a second key collides into the same bucket quickly");
    let kb_b = key_bytes(key_b);
    let record_len_b = spill_record_len(kb_b.len(), encoded_len_b);

    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(u64::from(record_len_a) + u64::from(record_len_b));
    let tier = Arc::new(SpillTier::open(&cfg, "admitted-bytes").expect("tier opens"));
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let _ = put(
        &engine,
        key_a,
        kb_a.clone(),
        value_a.clone(),
        hlc(1, 1),
        None,
        0,
    );
    let _ = put(
        &engine,
        key_b,
        kb_b.clone(),
        value_b.clone(),
        hlc(2, 1),
        None,
        1,
    );

    // A reservation covering exactly one victim's record: the
    // colder key spends it fully; the second falls back to admit.
    let mut reservation = tier
        .reserve(record_len_a, Duration::from_secs(5))
        .await
        .expect("the whole queue is free before this call");

    let first = engine.evict_one_sampled_with_reservation(
        bucket,
        Some(&mut reservation),
        &Written::default(),
        OnRefusal::TierPolicy,
        2,
    );
    assert_eq!(
        first.removed_weight,
        value_a.len() as u64,
        "the colder key hands off from the reservation's own budget"
    );
    let second = engine.evict_one_sampled_with_reservation(
        bucket,
        Some(&mut reservation),
        &Written::default(),
        OnRefusal::TierPolicy,
        2,
    );
    assert_eq!(
        second.removed_weight,
        value_b.len() as u64,
        "the second key hands off from admit's own fallback, the reservation's \
         budget already spent"
    );

    drop(reservation);

    assert_eq!(
        tier.queued_bytes(),
        u64::from(record_len_a) + u64::from(record_len_b),
        "every byte admit ever handed out across this call, whether through the \
         caller's own reservation or its own try_acquire_many fallback, is \
         accounted for by exactly these two committed jobs"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that [`Engine::enforce_capacity_with_reservation`]
/// reports a deficit once both budgets are exhausted mid-batch.
#[tokio::test]
async fn enforce_capacity_with_reservation_returns_the_deficit_once_both_budgets_are_exhausted() {
    let dir = temp_dir("enforce-capacity-deficit");
    let value = "x".repeat(20);
    let encoded_len = postcard::to_stdvec(&value)
        .expect("test value encodes")
        .len();

    let mut bucket = None;
    let mut keys = Vec::new();
    for k in 1..200_000u32 {
        let kb = key_bytes(k);
        let b = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
        match bucket {
            Some(target) if b != target => continue,
            None => bucket = Some(b),
            Some(_) => {}
        }
        keys.push(k);
        if keys.len() == 4 {
            break;
        }
    }
    assert_eq!(
        keys.len(),
        4,
        "1024 stripes; four collisions are found quickly"
    );
    let bucket = bucket.expect("set once the first key fixed the target stripe");

    let record_len_0 = spill_record_len(key_bytes(keys[0]).len(), encoded_len);
    let record_len_1 = spill_record_len(key_bytes(keys[1]).len(), encoded_len);

    // Sized to exactly the coldest victim's record, so reserve
    // drains the tier's whole admit budget into the reservation.
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(u64::from(record_len_0));
    let tier = Arc::new(SpillTier::open(&cfg, "enforce-capacity-deficit").expect("tier opens"));
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    // Four 20-byte values against a 30-unit cap attempts the
    // second victim within this same lock hold.
    let engine = Engine::<u32, String>::new(30, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    for (i, &k) in keys.iter().enumerate() {
        let now_ms = u64::try_from(i).expect("four keys, fits comfortably");
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            value.clone(),
            hlc(now_ms + 1, 1),
            None,
            now_ms,
        );
    }

    let mut reservation = tier
        .reserve(record_len_0, Duration::from_secs(5))
        .await
        .expect("the whole (single-record) queue is free before this call");

    let deficit = engine.enforce_capacity_with_reservation(
        bucket,
        Some(&mut reservation),
        &mut Written::default(),
        10,
    );
    assert_eq!(
        deficit,
        u64::from(record_len_1),
        "keys[1]'s own record length: right now, neither the reservation's \
         remaining budget nor admit's own headroom covers it"
    );

    assert_eq!(
        engine.get(&keys[0], 0),
        Some(value.clone()),
        "the coldest victim hands off cleanly, spent from the reservation"
    );
    assert_eq!(engine.debug_pending_spill_weight(), 20);
    assert_eq!(
        engine.get(&keys[1], 0),
        Some(value.clone()),
        "left fully resident: a pending refusal, not a removal and not a \
         keep-resident deferral either, no drop of any kind recorded for it"
    );
    assert_eq!(
        engine.get(&keys[2], 0),
        Some(value.clone()),
        "never even sampled: the deficit stopped the pass before a second batch \
         or scan could reach it"
    );
    assert_eq!(engine.get(&keys[3], 0), Some(value));
    let (_, total_after) = engine.debug_totals();
    assert_eq!(
        total_after, 60,
        "keys[1..4] still fully resident and counted: three victims' worth"
    );

    drop(reservation);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that the closing `enforce_capacity` call physically
/// deletes a deficit-left victim once retries are exhausted.
#[tokio::test]
async fn enforce_capacity_deletes_a_deficit_left_victim_once_reservation_retries_give_up() {
    let dir = temp_dir("enforce-capacity-deficit-fallback");
    let value = "x".repeat(20);
    let encoded_len = postcard::to_stdvec(&value)
        .expect("test value encodes")
        .len();

    let mut bucket = None;
    let mut keys = Vec::new();
    for k in 1..200_000u32 {
        let kb = key_bytes(k);
        let b = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
        match bucket {
            Some(target) if b != target => continue,
            None => bucket = Some(b),
            Some(_) => {}
        }
        keys.push(k);
        if keys.len() == 4 {
            break;
        }
    }
    assert_eq!(
        keys.len(),
        4,
        "1024 stripes; four collisions are found quickly"
    );
    let bucket = bucket.expect("set once the first key fixed the target stripe");

    let record_len_0 = spill_record_len(key_bytes(keys[0]).len(), encoded_len);
    let record_len_1 = spill_record_len(key_bytes(keys[1]).len(), encoded_len);

    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(u64::from(record_len_0));
    let tier =
        Arc::new(SpillTier::open(&cfg, "enforce-capacity-deficit-fallback").expect("tier opens"));
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(30, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    for (i, &k) in keys.iter().enumerate() {
        let now_ms = u64::try_from(i).expect("four keys, fits comfortably");
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            value.clone(),
            hlc(now_ms + 1, 1),
            None,
            now_ms,
        );
    }

    let mut reservation = tier
        .reserve(record_len_0, Duration::from_secs(5))
        .await
        .expect("the whole (single-record) queue is free before this call");

    let deficit = engine.enforce_capacity_with_reservation(
        bucket,
        Some(&mut reservation),
        &mut Written::default(),
        10,
    );
    assert_eq!(deficit, u64::from(record_len_1));
    assert_eq!(
        engine.get(&keys[1], 0),
        Some(value.clone()),
        "left fully resident by the first pass, exactly like the sibling test above"
    );

    // The reservation returns its spent remainder on drop, so
    // nothing is left for the closing call below to spend.
    drop(reservation);

    // The closing call falls back to the ordinary
    // `reservation: None` path and keeps evicting.
    engine.enforce_capacity(bucket, &mut Written::default(), 10);

    assert_eq!(
        engine.get(&keys[1], 0),
        None,
        "keep_resident_when_refused defaults to false: the closing fallback must \
         physically delete the leftover victim, not leave max_capacity exceeded \
         forever because a reservation retry gave up on it"
    );
    // The fallback stops once keys[0]'s pending hand-off is all
    // that holds the engine over its cap, or nothing is left to
    // evict.
    let (_, total_after_fallback) = engine.debug_totals();
    assert!(
        total_after_fallback < 60,
        "the closing fallback must make real progress against the cap, not just \
         record a metric: total={total_after_fallback}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
