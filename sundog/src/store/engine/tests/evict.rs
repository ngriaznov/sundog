//! Tests for capacity eviction: sampling, scans, batches, deferral to the flusher and the eviction helpers.

use std::collections::HashSet;

use super::*;

#[test]
fn weighted_capacity_eviction_stays_within_bound_and_evicts_colder_first() {
    // "Coldest first" holds only within one bucket. Find several keys in
    // the same bucket so a single sampling pass sees all of them.
    let target_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(0).as_ref()));
    let mut same_bucket_keys = vec![0u32];
    let mut candidate = 1u32;
    while same_bucket_keys.len() < 5 {
        if stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref())) == target_bucket {
            same_bucket_keys.push(candidate);
        }
        candidate += 1;
    }

    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    // Five 5-unit entries under a 20-unit cap: exactly one goes.
    let engine = Engine::<u32, String>::new(20, None, Some(weigher), None);
    for (i, &k) in same_bucket_keys.iter().enumerate() {
        let now = u64::try_from(i).expect("small") * 100;
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            "x".repeat(5),
            hlc(u64::from(k) + 1, 1),
            None,
            now,
        );
    }
    let (entries_before, weight_before) = engine.debug_totals();
    assert_eq!(entries_before, 5);
    assert_eq!(weight_before, 25);

    engine.enforce_capacity(target_bucket, &mut Written::default(), 500);

    let (entries_after, weight_after) = engine.debug_totals();
    assert!(
        weight_after <= 20,
        "total weight {weight_after} stays within the 20-unit cap"
    );
    assert_eq!(
        entries_after, 4,
        "exactly one 5-unit entry is evicted to clear a 5-unit overage"
    );
    assert_eq!(
        engine.get(&same_bucket_keys[0], 1_000),
        None,
        "the coldest (first-inserted) entry is the one evicted"
    );
    for &k in &same_bucket_keys[1..] {
        assert!(engine.get(&k, 1_000).is_some(), "hotter entries survive");
    }
}

/// `n` keys, each in a stripe of its own, none in `taken`; their
/// stripes join `taken`.
fn keys_in_distinct_stripes(n: usize, taken: &mut HashSet<usize>) -> Vec<u32> {
    let mut keys = Vec::new();
    let mut candidate = 0u32;
    while keys.len() < n {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()));
        if taken.insert(bucket) {
            keys.push(candidate);
        }
        candidate += 1;
    }
    keys
}

/// An engine at its cap of `cap` unit-weight entries, stored the way
/// [`put`] stores, which never enforces the cap: `cap` keys, each in a
/// stripe of its own, written at times `0..cap`.
fn full_engine(cap: u64, taken: &mut HashSet<usize>) -> (Engine<u32, String>, Vec<u32>) {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(cap, None, Some(weigher), None);
    let keys = keys_in_distinct_stripes(usize::try_from(cap).expect("small"), taken);
    for (i, &k) in keys.iter().enumerate() {
        let now = u64::try_from(i).expect("small");
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            k.to_string(),
            hlc(now + 1, 1),
            None,
            now,
        );
    }
    (engine, keys)
}

#[test]
fn a_write_never_evicts_its_own_entry_while_another_is_left() {
    let mut taken = HashSet::new();
    let (engine, older) = full_engine(3, &mut taken);
    // A key alone in its stripe: the eviction this write triggers
    // starts there, with nothing in the stripe but the write itself.
    let fresh = keys_in_distinct_stripes(1, &mut taken)[0];
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(fresh).as_ref()));

    let _ = engine.apply_many(
        bucket,
        vec![put_entry(fresh, "fresh", hlc(100, 1), None)],
        &LwwResolver,
        60_000,
        600_000,
        100,
    );

    assert_eq!(engine.debug_totals(), (3, 3), "back within the cap");
    assert_eq!(
        engine.get(&fresh, 100),
        Some("fresh".to_string()),
        "the write keeps its own entry"
    );
    let evicted = older
        .iter()
        .filter(|&&k| engine.get(&k, 100).is_none())
        .count();
    assert_eq!(evicted, 1, "an older entry made the room");
}

#[test]
fn a_batch_evicts_its_own_entries_last_and_still_fits_the_cap() {
    let mut taken = HashSet::new();
    let (engine, older) = full_engine(3, &mut taken);
    // Four keys of one stripe no older key is in: one batch.
    let bucket = (0..BUCKET_COUNT)
        .find(|b| !taken.contains(b))
        .expect("a free stripe");
    let batch: Vec<u32> = (1_000u32..)
        .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == bucket)
        .take(4)
        .collect();

    let _ = engine.apply_many(
        bucket,
        batch
            .iter()
            .map(|&k| put_entry(k, "new", hlc(100, 1), None))
            .collect(),
        &LwwResolver,
        60_000,
        600_000,
        100,
    );

    assert_eq!(engine.debug_totals(), (3, 3), "the cap holds");
    assert!(
        older.iter().all(|&k| engine.get(&k, 100).is_none()),
        "every older entry goes before any of the batch's own"
    );
    let kept = batch
        .iter()
        .filter(|&&k| engine.get(&k, 100).is_some())
        .count();
    assert_eq!(kept, 3, "the batch's own entries fill the cap");
}

/// A write heavier than the whole cap never fits, so its eviction
/// samples it like any other entry: alone in its stripe, where the
/// eviction starts, it goes first and the older entry stays.
#[test]
fn a_write_heavier_than_the_whole_cap_goes_and_the_older_entry_stays() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(5, None, Some(weigher), None);
    let mut taken = HashSet::new();
    let [small, heavy] = keys_in_distinct_stripes(2, &mut taken)[..] else {
        panic!("two keys");
    };
    let _ = put(
        &engine,
        small,
        key_bytes(small),
        "x".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(heavy).as_ref()));

    let _ = engine.apply_many(
        bucket,
        vec![put_entry(heavy, &"x".repeat(10), hlc(100, 1), None)],
        &LwwResolver,
        60_000,
        600_000,
        100,
    );

    assert_eq!(engine.debug_totals(), (1, 1), "the cap holds");
    assert_eq!(engine.get(&heavy, 100), None, "the heavy write goes");
    assert_eq!(
        engine.get(&small, 100),
        Some("x".to_string()),
        "the older entry stays"
    );
}

/// A scan that finds nothing but the batch's own entries clears
/// `written` and goes back to the write's stripe, so that group evicts
/// one of its own there, and the batch's next stripe group evicts at
/// once rather than scan every stripe again.
#[test]
fn a_scan_finding_only_written_entries_clears_them_for_the_next_group() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(2, None, Some(weigher), None);
    let mut taken = HashSet::new();
    let late = keys_in_distinct_stripes(1, &mut taken)[0];
    let late_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(late).as_ref()));
    // The stripe a scan from `late`'s visits next.
    let early_bucket = (late_bucket + 1) % BUCKET_COUNT;
    let early: Vec<u32> = (1_000u32..)
        .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == early_bucket)
        .take(3)
        .collect();
    let mut written = Written::default();

    let first_group = engine.debug_eviction_lock_acquisitions();
    let _ = engine.apply_many_with_reservation(
        early_bucket,
        early
            .iter()
            .map(|&k| put_entry(k, "early", hlc(100, 1), None))
            .collect(),
        &LwwResolver,
        60_000,
        600_000,
        100,
        None,
        &mut written,
    );
    assert_eq!(engine.debug_totals(), (2, 2), "the cap holds");
    assert!(
        written.is_empty(),
        "the scan found nothing but the batch's own entries"
    );
    assert_eq!(
        engine.debug_eviction_lock_acquisitions() - first_group,
        1 + BUCKET_COUNT as u64 + 1,
        "a sample of the group's stripe, a scan of every stripe, then the group's stripe again"
    );

    let before = engine.debug_eviction_lock_acquisitions();
    let _ = engine.apply_many_with_reservation(
        late_bucket,
        vec![put_entry(late, "late", hlc(101, 1), None)],
        &LwwResolver,
        60_000,
        600_000,
        101,
        None,
        &mut written,
    );

    assert_eq!(engine.debug_totals(), (2, 2), "the cap holds");
    assert_eq!(engine.get(&late, 101), Some("late".to_string()));
    assert_eq!(
        engine.debug_eviction_lock_acquisitions() - before,
        3,
        "a sample of `late`'s stripe, then a scan of it and the next"
    );
}

/// Once a scan finds a victim, the next pass samples the stripe it
/// found rather than a random one: a sparse cache clears an overage in
/// a far stripe with one scan, not one scan per victim.
#[test]
fn the_pass_after_a_scan_stays_in_the_stripe_it_found() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(10, None, Some(weigher), None);
    let far = 7;
    let keys: Vec<u32> = (0..100_000u32)
        .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == far)
        .take(19)
        .collect();
    for (i, &k) in keys.iter().enumerate() {
        let now = u64::try_from(i).expect("small");
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            k.to_string(),
            hlc(now + 1, 1),
            None,
            now,
        );
    }
    assert_eq!(engine.debug_totals(), (19, 19));
    let before = engine.debug_eviction_lock_acquisitions();

    engine.enforce_capacity(far + 1, &mut Written::default(), 100);

    assert_eq!(engine.debug_totals(), (10, 10), "back within the cap");
    assert_eq!(
        engine.debug_eviction_lock_acquisitions() - before,
        1 + BUCKET_COUNT as u64 + 1,
        "the empty start stripe, a scan around to the far stripe, then one batch there"
    );
}

/// A batch that evicts only from a dense stripe takes nothing from one
/// holding fewer than [`EVICTION_SAMPLE`] candidates, and the colder
/// half of one holding that many.
#[test]
fn a_batch_only_if_dense_spares_a_sparse_stripe() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let (sparse, dense) = (7, 9);
    for (stripe, count) in [(sparse, EVICTION_SAMPLE - 1), (dense, EVICTION_SAMPLE)] {
        let keys = (0..100_000u32)
            .filter(|&k| stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref())) == stripe)
            .take(count);
        for (i, k) in keys.enumerate() {
            let now = u64::try_from(i).expect("small");
            let _ = put(
                &engine,
                k,
                key_bytes(k),
                k.to_string(),
                hlc(now + 1, 1),
                None,
                now,
            );
        }
    }
    let batch = |stripe, only_if_dense| {
        engine.evict_batch_sampled_with_reservation(
            stripe,
            100,
            None,
            &Written::default(),
            only_if_dense,
            OnRefusal::TierPolicy,
            100,
        )
    };

    assert!(
        batch(sparse, true).made_no_progress(),
        "a sparse stripe gives up nothing"
    );
    assert_eq!(
        batch(dense, true).removed_weight,
        4,
        "a dense stripe gives up its colder half"
    );
    assert_eq!(
        batch(sparse, false).removed_weight,
        4,
        "without the flag, a sparse stripe gives up its colder half too"
    );
}

#[test]
fn a_rearm_keeps_its_own_entry_when_it_enforces_the_cap() {
    let mut taken = HashSet::new();
    let (engine, older) = full_engine(3, &mut taken);
    // Over the cap by one more stored the way `put` stores: the rearm
    // is the first write to enforce the cap.
    let rearmed = keys_in_distinct_stripes(1, &mut taken)[0];
    let _ = put(
        &engine,
        rearmed,
        key_bytes(rearmed),
        "kept".to_string(),
        hlc(50, 1),
        None,
        50,
    );
    assert_eq!(engine.debug_totals(), (4, 4));

    let outcome = engine.apply_if_current(
        put_entry(rearmed, "kept", hlc(60, 1), Some(60_000)),
        hlc(50, 1),
        &LwwResolver,
        60_000,
        600_000,
        60,
    );

    assert!(matches!(outcome, Some(ApplyOutcome::Put { .. })));
    assert_eq!(engine.debug_totals(), (3, 3), "back within the cap");
    assert_eq!(engine.get(&rearmed, 60), Some("kept".to_string()));
    let evicted = older
        .iter()
        .filter(|&&k| engine.get(&k, 60).is_none())
        .count();
    assert_eq!(evicted, 1, "an older entry made the room");
}

#[test]
fn the_sample_and_the_scan_pass_over_the_written_entries() {
    let (first, second, bucket) = same_bucket_pair(100_000);
    let engine = engine_u32_string(u64::MAX, None);
    for k in [first, second] {
        let _ = put(&engine, k, key_bytes(k), k.to_string(), hlc(1, 1), None, 0);
    }
    let hash_of = |k: u32| hash_key_bytes(key_bytes(k).as_ref());
    let sampled = |written: &Written| -> Vec<Bytes> {
        let stripe = engine.stripe_lock(bucket).read();
        engine
            .sample_candidates(&stripe, EVICTION_BATCH_SAMPLE, written)
            .map(|live| record_key_bytes(&live.record))
            .collect()
    };
    assert_eq!(sampled(&Written::default()).len(), 2);
    assert_eq!(
        sampled(&Written::of(hash_of(first))),
        vec![key_bytes(second)],
        "the written entry is not in the window"
    );
    let mut both = Written::of(hash_of(first));
    both.insert(hash_of(second));
    assert_eq!(sampled(&both), Vec::<Bytes>::new());

    assert!(
        engine
            .evict_one_scanning_with_reservation(bucket, None, &both, OnRefusal::TierPolicy, 0)
            .is_none(),
        "a scan finds nothing but written entries to evict"
    );
    assert!(
        engine
            .evict_one_scanning_with_reservation(
                bucket,
                None,
                &Written::of(hash_of(first)),
                OnRefusal::TierPolicy,
                0,
            )
            .is_some()
    );
    assert_eq!(engine.get(&first, 0), Some(first.to_string()));
    assert_eq!(engine.get(&second, 0), None, "the unwritten entry went");
}

#[test]
fn capacity_eviction_rotates_past_an_empty_start_bucket_into_other_stripes() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, String>::new(3, None, Some(weigher), None);

    // 8 keys landing in 8 distinct, non-empty stripes.
    let mut other_keys = Vec::new();
    let mut used_buckets = std::collections::HashSet::new();
    let mut candidate = 0u32;
    while other_keys.len() < 8 {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()));
        if used_buckets.insert(bucket) {
            other_keys.push(candidate);
        }
        candidate += 1;
    }
    for (i, &k) in other_keys.iter().enumerate() {
        let now = u64::try_from(i).expect("small");
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            k.to_string(),
            hlc(u64::from(k) + 1, 1),
            None,
            now,
        );
    }

    // A key landing in a stripe none of the above touched: the eviction
    // start point, but with only one entry to give up.
    let start_key = loop {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref()));
        if !used_buckets.contains(&bucket) {
            break candidate;
        }
        candidate += 1;
    };
    let start_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(start_key).as_ref()));
    let _ = put(
        &engine,
        start_key,
        key_bytes(start_key),
        start_key.to_string(),
        hlc(1_000, 1),
        None,
        100,
    );

    let (entries_before, weight_before) = engine.debug_totals();
    assert_eq!(entries_before, 9);
    assert_eq!(weight_before, 9);

    engine.enforce_capacity(start_bucket, &mut Written::default(), 500);

    let (entries_after, weight_after) = engine.debug_totals();
    assert!(
        weight_after <= 3,
        "total weight {weight_after} stays within the 3-unit cap"
    );
    assert!(
        entries_after < entries_before - 1,
        "the start stripe alone (1 entry) cannot account for a {}-entry eviction: \
         enforce_capacity rotated into other stripes",
        entries_before - entries_after
    );
    assert_eq!(
        engine.get(&start_key, 100),
        None,
        "the start stripe's own entry is evicted too"
    );
}

#[test]
fn eviction_sampling_starts_from_rotating_offsets() {
    let engine = engine_u32_string(u64::MAX, None);
    let offsets: std::collections::HashSet<usize> =
        (0..64).map(|_| engine.sample_offset(40)).collect();
    assert!(
        offsets.iter().all(|&o| o < 40),
        "an offset always lies inside the stripe"
    );
    assert!(
        offsets.iter().any(|&o| o >= EVICTION_SAMPLE),
        "sampling reaches past the first {EVICTION_SAMPLE} slots: {offsets:?}"
    );
    assert_eq!(engine.sample_offset(0), 0, "an empty stripe has one offset");
}

#[test]
fn enforce_capacity_clears_an_overage_needing_thousands_of_evictions() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(10_000, None, Some(weigher), None);
    // 6,000 one-unit entries, then one 9,500-unit write: back under the
    // cap only after more than 5,500 evictions of older ones, since the
    // write's own entry goes only once nothing else is left.
    for k in 1..=6_000u32 {
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            "x".into(),
            hlc(u64::from(k), 1),
            None,
            0,
        );
    }
    let big = 7_000u32;
    let big_bytes = key_bytes(big);
    let big_hash = hash_key_bytes(big_bytes.as_ref());
    let start_bucket = stripe_index_from_hash(big_hash);
    let _ = put(
        &engine,
        big,
        big_bytes,
        "y".repeat(9_500),
        hlc(10_000, 1),
        None,
        1,
    );
    assert_eq!(engine.debug_totals().1, 15_500);

    engine.enforce_capacity(start_bucket, &mut Written::of(big_hash), 100);

    let (entries, weight) = engine.debug_totals();
    assert!(
        weight <= 10_000,
        "total weight {weight} is back under the cap"
    );
    assert!(
        entries <= 501,
        "{entries} entries remain; the big write was evicted instead of older ones"
    );
}

#[test]
fn defer_to_flusher_only_while_a_hand_off_is_in_flight_and_eviction_cannot_help() {
    assert!(
        !defer_to_flusher(false, 10, 0, 10),
        "nothing pending: enforce_capacity's ordinary scanning fallback runs"
    );
    assert!(
        !defer_to_flusher(true, 11, 0, 10),
        "a refusal with nothing pending is no full queue: scan for another victim"
    );
    assert!(
        defer_to_flusher(false, 10, 1, 10),
        "within the limit once the pending hand-off installs: trust the flusher"
    );
    assert!(defer_to_flusher(false, 0, u64::MAX, 0));
    assert!(
        !defer_to_flusher(false, 11, 1, 10),
        "still over the limit after every pending hand-off installs: keep evicting"
    );
    assert!(!defer_to_flusher(false, u64::MAX, u64::MAX, 10));
    assert!(
        defer_to_flusher(true, 11, 1, 10),
        "a victim refused while a hand-off is in flight: the queue is full for every stripe"
    );
    assert!(defer_to_flusher(true, u64::MAX, u64::MAX, 0));
}

#[cfg(feature = "spill")]
#[test]
fn claimed_total_adds_a_weight_only_within_the_cap() {
    assert_eq!(claimed_total(3, 1, 4), Some(4));
    assert_eq!(claimed_total(0, 0, 0), Some(0));
    assert_eq!(
        claimed_total(4, 1, 4),
        None,
        "no room left: the entry stays on disk"
    );
    assert_eq!(claimed_total(0, 5, 4), None);
    assert_eq!(claimed_total(u64::MAX, 1, u64::MAX), None, "never wraps");
    assert_eq!(claimed_total(u64::MAX - 1, 1, u64::MAX), Some(u64::MAX));
}

#[cfg(feature = "spill")]
#[test]
fn promotion_fits_any_entry_no_heavier_than_the_whole_cap() {
    assert!(promotion_fits(1, 1));
    assert!(promotion_fits(0, 0));
    assert!(promotion_fits(u32::MAX, u64::MAX));
    assert!(
        !promotion_fits(2, 1),
        "no eviction makes room for an entry heavier than the whole cap"
    );
    assert!(!promotion_fits(u32::MAX, u64::from(u32::MAX) - 1));
}

#[test]
fn deficit_once_still_over_capacity_zeroes_a_deficit_the_cap_no_longer_needs() {
    assert_eq!(
        deficit_once_still_over_capacity(35, 30, 15),
        15,
        "still 5 units over the cap: the deficit is still owed"
    );
    assert_eq!(
        deficit_once_still_over_capacity(25, 30, 15),
        0,
        "already under the cap, by the batch's own other progress or an unrelated \
         concurrent flusher install: the stale deficit is dropped rather than paid down \
         with an avoidable extra reserve() wait"
    );
    assert_eq!(
        deficit_once_still_over_capacity(30, 30, 15),
        0,
        "exactly at the cap counts as satisfied, matching enforce_capacity_with_reservation's \
         own loop-top `current <= max_capacity` check"
    );
    assert_eq!(
        deficit_once_still_over_capacity(31, 30, 15),
        15,
        "one unit over is still over: the deficit survives unchanged"
    );
}

#[test]
fn eviction_batch_size_takes_the_fewest_entries_that_clear_the_overage() {
    assert_eq!(
        eviction_batch_size(0, &[3, 3, 3]),
        0,
        "no overage evicts nothing"
    );
    assert_eq!(
        eviction_batch_size(2, &[5, 5, 5]),
        1,
        "the first sampled entry alone already clears a 2-unit overage"
    );
    assert_eq!(
        eviction_batch_size(6, &[3, 3, 3]),
        2,
        "3 clears none of a 6-unit overage, 3+3 clears all of it"
    );
    assert_eq!(
        eviction_batch_size(100, &[1, 1, 1]),
        2,
        "a large overage takes only the colder half of the sample"
    );
    assert_eq!(
        eviction_batch_size(100, &[1; 32]),
        EVICTION_BATCH,
        "a full sample evicts at most EVICTION_BATCH"
    );
    assert_eq!(
        eviction_batch_size(100, &[7]),
        1,
        "a lone sampled entry is evicted, as the single-entry path did"
    );
    assert_eq!(
        eviction_batch_size(5, &[]),
        0,
        "nothing sampled means nothing to evict"
    );
}

#[test]
fn enforce_capacity_batches_evictions_under_a_burst_ten_thousand_over_the_cap() {
    let weigher: Weigher<u32, String> = Box::new(|_k, _v| 1);
    // A 40,000-unit cap left dense after eviction (~39 entries/stripe)
    // so a random probe rarely lands on an already-empty stripe; the
    // point here is measuring the batch size, not the scanning
    // fallback's cost on a nearly-drained table.
    let engine = Engine::<u32, String>::new(40_000, None, Some(weigher), None);
    // 50,000 one-unit entries: 10,000 over the cap.
    for k in 1..=50_000u32 {
        let _ = put(
            &engine,
            k,
            key_bytes(k),
            "x".into(),
            hlc(u64::from(k), 1),
            None,
            0,
        );
    }
    let (entries_before, weight_before) = engine.debug_totals();
    assert_eq!(entries_before, 50_000);
    assert_eq!(weight_before, 50_000);

    engine.enforce_capacity(0, &mut Written::default(), 0);

    let (entries_after, weight_after) = engine.debug_totals();
    assert!(
        weight_after <= 40_000,
        "total weight {weight_after} is back under the cap"
    );
    let evicted = entries_before - entries_after;
    let acquisitions = engine.debug_eviction_lock_acquisitions();
    assert!(
        acquisitions * 2 < evicted,
        "{acquisitions} lock acquisitions to evict {evicted} entries: batching should need \
         far fewer acquisitions than entries evicted"
    );
}

#[test]
fn enforce_capacity_stops_once_every_stripe_is_empty() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(5, None, Some(weigher), None);
    // One entry heavier than the whole cap: evicting it is all there is.
    let _ = put(&engine, 1, key_bytes(1), "z".repeat(50), hlc(1, 1), None, 0);
    engine.enforce_capacity(0, &mut Written::default(), 0);
    assert_eq!(engine.debug_totals(), (0, 0));
    assert!(
        engine.evict_one_sampled(0, 0).made_no_progress(),
        "an empty stripe evicts nothing"
    );
    assert_eq!(engine.evict_one_scanning(0, 0), None);
}

#[test]
fn evict_outcome_made_no_progress_only_when_nothing_was_freed() {
    assert!(EvictOutcome::default().made_no_progress());
    assert!(
        !EvictOutcome {
            removed_weight: 1,
            deficit: 0,
            refused: false,
        }
        .made_no_progress()
    );
}

#[test]
fn a_pending_spill_entry_is_never_sampled_as_a_victim() {
    // A weight-0 Resident entry, exactly what a successful hand-off
    // leaves behind while the flusher's install is still in flight,
    // must never be picked a second time: not by the single-victim
    // sampler, and not by the batch one.
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb.clone(), "x".repeat(5), hlc(1, 1), None, 0);
    {
        let mut stripe = engine.stripe_lock(bucket).write();
        let live = stripe
            .live
            .find_mut(hash, |l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        live.weight = 0;
    }

    assert!(
        engine.evict_one_sampled(bucket, 0).made_no_progress(),
        "the only entry in this stripe is pending; single-victim sampling finds nothing"
    );
    assert!(
        engine
            .evict_batch_sampled(bucket, 100, 0)
            .made_no_progress(),
        "the only entry in this stripe is pending; batch sampling finds nothing either"
    );
    assert_eq!(
        engine.get(&key, 0),
        Some("x".repeat(5)),
        "the pending entry is untouched: still resident, still readable"
    );
}

#[test]
fn sampled_eviction_still_picks_the_coldest_entry_across_a_last_access_rollover() {
    // key_old's touch predates a touch_stamp rollover; a naive
    // min_by_key over raw stamps would wrongly call key_new colder.
    let target_bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(0).as_ref()));
    let key_old = 0u32;
    let mut candidate = 1u32;
    let key_new = loop {
        if stripe_index_from_hash(hash_key_bytes(key_bytes(candidate).as_ref())) == target_bucket {
            break candidate;
        }
        candidate += 1;
    };

    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    let before_rollover = (1u64 << 32) - 100;
    let after_rollover = (1u64 << 32) + 50;
    let _ = put(
        &engine,
        key_old,
        key_bytes(key_old),
        "old".to_string(),
        hlc(1, 1),
        None,
        before_rollover,
    );
    let _ = put(
        &engine,
        key_new,
        key_bytes(key_new),
        "new".to_string(),
        hlc(2, 1),
        None,
        after_rollover,
    );

    let now_ms = after_rollover + 10;
    let outcome = engine.evict_one_sampled(target_bucket, now_ms);
    assert_eq!(outcome.removed_weight, 1, "one entry evicted");
    assert_eq!(
        engine.get(&key_old, now_ms),
        None,
        "key_old, touched longest ago in true time, is the one evicted"
    );
    assert_eq!(
        engine.get(&key_new, now_ms),
        Some("new".to_string()),
        "key_new survives: its truncated stamp is smaller, but it is the true warmer entry"
    );
}

#[test]
fn evict_one_sampled_breaks_a_last_access_tie_by_arena_order() {
    // key_first/key_second share a bucket and the same now_ms, so
    // idle_elapsed_ms ties; the winner comes down to arena order in
    // sample_candidates feeding max_by_key, which returns the last
    // equally-maximal element. Pins today's resolution as a regression
    // guard, not a documented contract.
    let (key_first, key_second, bucket) = same_bucket_pair(100_000);
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    let now = 0u64;
    let _ = put(
        &engine,
        key_first,
        key_bytes(key_first),
        "first".to_string(),
        hlc(1, 1),
        None,
        now,
    );
    let _ = put(
        &engine,
        key_second,
        key_bytes(key_second),
        "second".to_string(),
        hlc(2, 1),
        None,
        now,
    );

    let outcome = engine.evict_one_sampled(bucket, now);
    assert_eq!(outcome.removed_weight, 1, "one entry evicted");
    assert_eq!(
        engine.get(&key_first, now),
        None,
        "today's concrete tie-break: the first-inserted key is evicted"
    );
    assert_eq!(
        engine.get(&key_second, now),
        Some("second".to_string()),
        "today's concrete tie-break: the second-inserted key survives"
    );
}
