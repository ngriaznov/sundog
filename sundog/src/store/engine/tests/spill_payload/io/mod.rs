//! Tests for the engine over a real spill tier on disk: hand-off, install, eviction refusal and reservations.

use std::time::{Duration, Instant};

use super::*;

mod flush_slots;
mod held_only;
mod refusal;
mod reservation;
mod tombstone_arrival;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sundog-engine-spill-test-{label}-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Polls `cond` until it returns `true` or `timeout` elapses,
/// returning the final result either way. Never a fixed sleep.
fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    loop {
        if cond() {
            return true;
        }
        if start.elapsed() >= timeout {
            return cond();
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

const POLL_TIMEOUT: Duration = Duration::from_secs(5);

fn is_spilled<V>(engine: &Engine<u32, V>, kb: &Bytes) -> bool
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let stripe = engine.stripe_lock(bucket).read();
    stripe
        .live
        .iter()
        .any(|l| record_key(&l.record) == kb.as_ref() && matches!(l.state, EntryState::Spilled(_)))
}

#[test]
fn set_spill_makes_it_visible_via_spill_accessor() {
    let dir = temp_dir("set-spill-accessor");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "accessor").expect("tier opens"));
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    assert!(engine.spill().is_none());
    engine.set_spill(Arc::clone(&tier));
    assert!(engine.spill().is_some());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn eviction_hands_off_weight_immediately_and_install_never_double_subtracts_it() {
    let dir = temp_dir("evict");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "evict").expect("tier opens"));
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let key = 1u32;
    let kb = key_bytes(key);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    let _ = put(&engine, key, kb.clone(), "x".repeat(20), hlc(1, 1), None, 0);

    let digest_before = engine.digests();
    let (live_count_before, weight_before) = engine.debug_totals();

    // `evict_one_sampled` is the real caller: it folds
    // `evict_victim_locked`'s freed weight into `total_weight`
    // right there, synchronously, with no dependency on the
    // flusher thread ever running.
    let pass_outcome = engine.evict_one_sampled(bucket, 0);
    assert_eq!(
        pass_outcome.removed_weight, 20,
        "a spill hand-off's freed weight counts the same as a physical removal's"
    );
    let (_, weight_at_handoff) = engine.debug_totals();
    assert_eq!(
        weight_at_handoff,
        weight_before - 20,
        "hand-off zeroes the victim's weight and frees it from total_weight \
         immediately, before any disk write or install has happened"
    );
    // The flusher may already have installed the victim by now;
    // either way its weight is 0 from the hand-off on.
    assert_eq!(
        {
            let stripe = engine.stripe_lock(bucket).read();
            stripe
                .live
                .iter()
                .find(|l| record_key(&l.record) == kb.as_ref())
                .map(|l| l.weight)
        },
        Some(0),
        "the victim keeps weight 0 from the hand-off, resident or spilled"
    );

    // A second sampling pass, called before the flusher's
    // install is known to have landed, must not pick the same
    // key again: whether it is still the pending hand-off or
    // has already been flipped to `Spilled` by now, either way
    // `is_spill_candidate` excludes it.
    assert!(
        engine.evict_one_sampled(bucket, 0).made_no_progress(),
        "the only entry in this stripe is either still pending or already \
         spilled; double-sampling before install must find nothing"
    );

    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );

    assert_eq!(
        engine.digests(),
        digest_before,
        "spilling and installing never touch the digest"
    );
    let (live_count_after, weight_after) = engine.debug_totals();
    assert_eq!(
        live_count_after, live_count_before,
        "spilling and installing never touch live_count"
    );
    assert_eq!(
        weight_after, weight_at_handoff,
        "install only flips the payload to Spilled; the weight was already zeroed \
         and freed at hand-off, so total_weight does not move again here"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn install_new_creates_a_fresh_spilled_entry_readable_via_read_at_and_folds_the_digest() {
    let dir = temp_dir("install-new");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "install-new").expect("tier opens"));

    // A throwaway producer spills one record to disk to get a
    // genuine SpillLoc.
    let producer: Arc<Engine<u32, String>> = Arc::new(Engine::new(u64::MAX, None, None, None));
    producer.set_spill(Arc::clone(&tier));
    tier.attach(Arc::downgrade(
        &(Arc::clone(&producer) as Arc<dyn SpillSink>),
    ));
    let key = 7u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(1, 1);
    let _ = put(
        &producer,
        key,
        kb.clone(),
        "spilled-value".to_string(),
        ver,
        Some(999),
        0,
    );
    let _ = producer.evict_one_sampled(bucket, 0);
    assert!(poll_until(POLL_TIMEOUT, || is_spilled(&producer, &kb)));
    let loc = {
        let stripe = producer.stripe_lock(bucket).read();
        let live = stripe
            .live
            .iter()
            .find(|l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        match live.state {
            EntryState::Spilled(loc) => loc,
            EntryState::Resident => {
                panic!("poll_until above confirms the producer's entry is spilled")
            }
        }
    };

    let target = Engine::<u32, String>::new(u64::MAX, None, None, None);
    let digest_before = target.digests();

    let inserted = SpillSink::install_new(&target, bucket, &kb, hash, ver, Some(999), loc, 1);
    assert!(inserted);

    // Resident-free: `get` reads nothing, since no value ever
    // landed in RAM on the target engine.
    assert_eq!(target.get(&key, 0), None);
    {
        let stripe = target.stripe_lock(bucket).read();
        let live = stripe
            .live
            .iter()
            .find(|l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        assert!(
            matches!(live.state, EntryState::Spilled(l) if l == loc),
            "Spilled(loc)-only, pointing at the exact same on-disk location"
        );
        assert_eq!(live.weight, 0, "a Spilled entry always carries weight 0");
    }

    // Readable via read_at, since install_new never re-writes
    // or copies the value bytes itself.
    let read_back = tier
        .read_at(loc)
        .expect("read_at succeeds")
        .expect("the record is still there");
    assert_eq!(read_back.ver, ver);
    assert_eq!(read_back.expires_at_ms, Some(999));
    assert_eq!(decode_value::<String>(&read_back.encoded), "spilled-value");

    let (live_count, weight) = target.debug_totals();
    assert_eq!(live_count, 1);
    assert_eq!(
        weight, 0,
        "a spilled entry never counts toward total_weight"
    );

    let mut expected_digest = digest_before;
    expected_digest[bucket].1 ^= entry_fingerprint(kb.as_ref(), ver);
    assert_eq!(
        target.digests(),
        expected_digest,
        "install_new folds the fresh entry's fingerprint into the bucket digest"
    );
    assert_eq!(
        target.digests(),
        target.recompute_digests_paired(),
        "the incrementally folded digest matches a full recompute from scratch"
    );

    tier.close();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn drop_unreadable_spilled_drops_only_the_entry_still_spilled_at_that_pointer() {
    // No tier is read: the entry points wherever install_new says it does.
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    let key = 7u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(1, 1);
    let loc = SpillLoc {
        region: 0,
        offset: 64,
        len: 32,
        generation: 1,
    };
    let digest_before = engine.digests();
    assert!(SpillSink::install_new(
        &engine, bucket, &kb, hash, ver, None, loc, 1
    ));
    assert_eq!(engine.debug_totals().0, 1);
    assert_eq!(engine.debug_spill_entries_count(), 1);

    let moved = SpillLoc { offset: 96, ..loc };
    assert!(
        !engine.drop_unreadable_spilled(kb.as_ref(), hash, moved),
        "an entry spilled again elsewhere since the read is kept"
    );
    let rotated = SpillLoc {
        generation: 2,
        ..loc
    };
    assert!(
        !engine.drop_unreadable_spilled(kb.as_ref(), hash, rotated),
        "a pointer into a later generation of the region names another record"
    );
    let other = key_bytes(8);
    assert!(
        !engine.drop_unreadable_spilled(other.as_ref(), hash, loc),
        "another key's read never drops this entry"
    );
    assert_eq!(engine.debug_totals().0, 1, "nothing dropped yet");

    assert!(engine.drop_unreadable_spilled(kb.as_ref(), hash, loc));
    assert_eq!(
        engine.debug_totals(),
        (0, 0),
        "the entry leaves the live count"
    );
    assert_eq!(engine.debug_spill_entries_count(), 0, "and the spill gauge");
    assert_eq!(
        engine.digests(),
        digest_before,
        "and the digest, so a peer's copy no longer compares equal"
    );
    assert_eq!(
        engine.digests(),
        engine.recompute_digests_paired(),
        "the incremental digest matches a full recompute"
    );
    assert!(!engine.contains_key(&key, 0), "the key is gone");
    assert_eq!(
        engine.tombstone_count(),
        0,
        "with no tombstone to delete a peer's copy"
    );
    assert!(
        !engine.drop_unreadable_spilled(kb.as_ref(), hash, loc),
        "a second drop finds nothing"
    );

    // A resident entry at the key is never an unreadable spilled one.
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "resident".to_string(),
        hlc(2, 0),
        None,
        0,
    );
    assert!(!engine.drop_unreadable_spilled(kb.as_ref(), hash, loc));
    assert_eq!(engine.get(&key, 0), Some("resident".to_string()));
}

#[test]
fn evict_one_sampled_abandons_the_victim_when_the_flush_queue_channel_is_full() {
    let dir = temp_dir("enqueue-err-abandon");
    // A generous flush_queue_bytes exercises the channel's
    // slot-count limit, not the byte bound.
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(1 << 20);
    let slots = crate::store::spill::flush_queue_slots(cfg.flush_queue_bytes_value());
    let tier = Arc::new(SpillTier::open(&cfg, "channel-full").expect("tier opens"));
    // Paused before the flusher thread is even spawned, so it
    // never gets a chance to drain any of the filler jobs
    // below before the real eviction's own `enqueue` needs the
    // channel to still be completely full.
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    // Fill the flusher's channel to its exact slot capacity
    // with throwaway jobs, so the real eviction below finds no
    // room: the one way `finish_spill_handoff` reaches
    // `abandon` rather than `install`.
    for i in 0..slots {
        let filler = SpillJob {
            stripe_idx: 0,
            hash: 0,
            key_bytes: Bytes::from(format!("filler-{i}")),
            ver: hlc(0, 0),
            expires_at_ms: None,
            encoded: Bytes::from_static(b"f"),
            weight: 1,
            // These fillers bypass would_accept, sent straight
            // to enqueue to exercise the channel's slot limit.
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

    let outcome = engine.evict_one_sampled(bucket, 0);
    assert_eq!(
        outcome.removed_weight, 0,
        "the hand-off frees the weight from total_weight right away, but abandon \
         puts it back once the enqueue that follows fails, so the pass reports no \
         progress"
    );
    assert!(outcome.refused, "the abandoned hand-off reads as a refusal");

    assert_eq!(
        engine.get(&key, 0),
        Some("x".repeat(30)),
        "abandon restores full residency: the value is still there and readable"
    );
    let (_, weight_after) = engine.debug_totals();
    assert_eq!(
        weight_after, 30,
        "abandon restores the victim's weight to total_weight"
    );
    assert_eq!(
        engine.debug_pending_spill_weight(),
        0,
        "abandon moves the weight back out of pending_spill_weight too"
    );
    let stripe = engine.stripe_lock(bucket).read();
    let live = stripe
        .live
        .iter()
        .find(|l| record_key(&l.record) == kb.as_ref())
        .expect("entry is present");
    assert_eq!(
        live.weight, 30,
        "the entry's own weight field is restored, not just the total"
    );
    drop(stripe);

    let _ = std::fs::remove_dir_all(&dir);
}

// The mirror of `enforce_capacity_leaves_a_refused_victim_resident_
// and_retries_once_the_queue_drains` below: `keep_resident_when_
// refused` is left at its default of `false` (never set), so a
// refused hand-off falls back to the ordinary delete exactly as
// every mode did before that policy existed.
#[test]
fn enforce_capacity_via_real_hand_off_bounds_ram_and_falls_back_to_queue_full() {
    let dir = temp_dir("real-backlog");
    // A flush queue that holds one small record but not two, so
    // `would_accept` refuses a second hand-off while the first
    // is still queued: the flusher is paused, so nothing ever
    // drains it.
    let record_len_upper_bound = 300u64;
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len_upper_bound);
    let tier = Arc::new(SpillTier::open(&cfg, "backlog").expect("tier opens"));
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(250, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    // Two keys landing in the same stripe, so a single bucket's
    // sampling sees both, colder one first.
    let mut same_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
    let mut keys = Vec::new();
    for k in 1..100_000u32 {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
        let group = same_bucket.entry(bucket).or_default();
        group.push(k);
        if group.len() == 2 {
            keys = group.clone();
            break;
        }
    }
    assert_eq!(
        keys.len(),
        2,
        "1024 stripes; two collisions are found quickly"
    );
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(keys[0]).as_ref()));

    let _ = put(
        &engine,
        keys[0],
        key_bytes(keys[0]),
        "x".repeat(200),
        hlc(1, 1),
        None,
        0,
    );
    let _ = put(
        &engine,
        keys[1],
        key_bytes(keys[1]),
        "y".repeat(200),
        hlc(2, 1),
        None,
        1,
    );

    // First eviction: the colder key (`now_ms: 0`) hands off
    // cleanly, with the queue empty.
    engine.evict_one_sampled(bucket, 2);
    assert_eq!(
        engine.get(&keys[0], 0),
        Some("x".repeat(200)),
        "still fully resident: a hand-off, not a removal"
    );
    assert_eq!(engine.debug_pending_spill_weight(), 200);
    let combined_after_first = engine.debug_totals().1 + engine.debug_pending_spill_weight();
    assert!(
        combined_after_first <= 250 + 200,
        "combined weight {combined_after_first} exceeds the cap by more than one \
         victim's worth"
    );

    // Second eviction: the queue already holds one record's
    // worth and the flusher is paused, so `would_accept` now
    // refuses; the victim falls back to an ordinary delete
    // instead of piling up a second fully-resident value.
    engine.evict_one_sampled(bucket, 2);
    assert_eq!(
        engine.get(&keys[1], 0),
        None,
        "queue_full falls back to the ordinary delete-and-XOR path"
    );
    let (total_after, pending_after) =
        (engine.debug_totals().1, engine.debug_pending_spill_weight());
    assert_eq!(total_after, 0);
    assert_eq!(pending_after, 200);
    assert!(
        total_after + pending_after <= 250 + 200,
        "combined weight never exceeds the cap by more than one victim's worth"
    );

    // The public entry point sees the same combined accounting
    // and returns immediately: nothing left to evict in this
    // bucket, and total_weight alone is within the cap, so the
    // pending hand-off's install is all that is left.
    engine.enforce_capacity(bucket, &mut Written::default(), 2);

    // Once the flusher is allowed to run, the one queued job
    // installs and pending_spill_weight drains back to zero.
    tier.resume_flusher();
    assert!(poll_until(POLL_TIMEOUT, || engine
        .debug_pending_spill_weight()
        == 0));
    assert!(poll_until(POLL_TIMEOUT, || is_spilled(
        &engine,
        &key_bytes(keys[0])
    )));

    let _ = std::fs::remove_dir_all(&dir);
}

/// The `Mode::Replicated` mirror of
/// `enforce_capacity_via_real_hand_off_bounds_ram_and_falls_back_to_queue_full`:
/// identical setup, `tier.set_keep_resident_when_refused(true)`
/// the only difference, so the second victim's refused hand-off
/// leaves it fully resident instead of falling back to a
/// delete, and a later `enforce_capacity` pass, once the queue
/// has room again, retries and spills it.
#[test]
fn enforce_capacity_leaves_a_refused_victim_resident_and_retries_once_the_queue_drains() {
    let dir = temp_dir("real-backlog-keep-resident");
    let record_len_upper_bound = 300u64;
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len_upper_bound);
    let tier = Arc::new(SpillTier::open(&cfg, "backlog-keep-resident").expect("tier opens"));
    tier.set_keep_resident_when_refused(true);
    tier.pause_flusher();
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    // A cap well under either single entry's own weight (200),
    // unlike the flag-off mirror's 250: that test only needs
    // one hand-off to land to fall back under its cap, but this
    // one means to keep the deferred victim's weight alone
    // over the cap even after the first hand-off fully
    // resolves, so the later retry has something left to do.
    let engine = Engine::<u32, String>::new(50, None, Some(weigher), None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let mut same_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
    let mut keys = Vec::new();
    for k in 1..100_000u32 {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
        let group = same_bucket.entry(bucket).or_default();
        group.push(k);
        if group.len() == 2 {
            keys = group.clone();
            break;
        }
    }
    assert_eq!(
        keys.len(),
        2,
        "1024 stripes; two collisions are found quickly"
    );
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(keys[0]).as_ref()));

    let _ = put(
        &engine,
        keys[0],
        key_bytes(keys[0]),
        "x".repeat(200),
        hlc(1, 1),
        None,
        0,
    );
    let _ = put(
        &engine,
        keys[1],
        key_bytes(keys[1]),
        "y".repeat(200),
        hlc(2, 1),
        None,
        1,
    );

    // First eviction: the colder key hands off cleanly, with
    // the queue empty.
    engine.evict_one_sampled(bucket, 2);
    assert_eq!(engine.debug_pending_spill_weight(), 200);

    // Second eviction: the queue already holds one record's
    // worth and the flusher is paused, so `would_accept`
    // refuses again, but this tier's `keep_resident_when_
    // refused` is set, so the victim is left fully resident
    // instead of deleted.
    engine.evict_one_sampled(bucket, 2);
    assert_eq!(
        engine.get(&keys[1], 0),
        Some("y".repeat(200)),
        "a Mode::Replicated victim refused by the tier stays fully resident, never \
         deleted"
    );
    let (total_after, pending_after) =
        (engine.debug_totals().1, engine.debug_pending_spill_weight());
    assert_eq!(
        total_after, 200,
        "the deferred victim's weight is untouched, still counted in total_weight"
    );
    assert_eq!(pending_after, 200, "unchanged from the first hand-off");
    assert!(
        total_after + pending_after > 50,
        "combined weight is still over the cap: nothing was freed by deferring"
    );
    {
        let stripe = engine.stripe_lock(bucket).read();
        let live = stripe
            .live
            .iter()
            .find(|l| record_key(&l.record) == key_bytes(keys[1]).as_ref())
            .expect("the deferred entry is still present");
        assert_eq!(live.weight, 200, "its own weight field is untouched too");
    }

    // The public entry point sees the same combined accounting,
    // finds its victim refused by the full queue while a
    // hand-off is pending, and returns after that one stripe
    // rather than scan every other stripe for a victim the
    // queue would refuse too.
    let locks_before = engine.debug_eviction_lock_acquisitions();
    engine.enforce_capacity(bucket, &mut Written::default(), 2);
    assert!(
        engine.debug_eviction_lock_acquisitions() - locks_before <= 2,
        "a full queue ends the pass at once, took {} stripe locks",
        engine.debug_eviction_lock_acquisitions() - locks_before
    );
    assert_eq!(
        engine.debug_totals().1,
        200,
        "enforce_capacity made no further change: still over the cap"
    );

    // Once the flusher drains the first job, `would_accept` has
    // room again for the deferred victim, and the next
    // `enforce_capacity` pass spills it.
    tier.resume_flusher();
    assert!(poll_until(POLL_TIMEOUT, || tier.queued_bytes() == 0));
    engine.enforce_capacity(bucket, &mut Written::default(), 2);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &key_bytes(keys[1]))),
        "the retried victim is spilled once the tier has room again"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// `resolve_conflict`'s tombstone/spill guard only ever skips
/// calling a misbehaving resolver's own `merge`; a well-behaved
/// value-aware resolver's *own* fallback (`winner` returning
/// `A`/`B` by `Hlc` order whenever it is handed a value-less
/// side) must never fire here merely because the stored side
/// is spilled rather than genuinely value-less. This
/// pins that `apply_locked` reads a spilled stored record's real
/// bytes back off disk before consulting the resolver, so a
/// merge against it folds exactly as it would against a resident
/// record, rather than discarding whichever side loses the
/// outright `Hlc` fallback.
#[test]
fn merge_against_a_spilled_counter_folds_the_stored_side_instead_of_dropping_it() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let dir = temp_dir("merge-spilled");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "merge-spilled").expect("tier opens"));
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let resolver = PnCounterResolver;
    let key = 1u32;
    let kb = key_bytes(key);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    let node_a = crate::store::crdt::WriterId::new(NodeId::from(11), 1);
    let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);

    let _ = put_with_resolver(
        &engine,
        key,
        kb.clone(),
        PnCounter::local_delta(node_a, 3),
        hlc(1, 1),
        None,
        0,
        &resolver,
    );

    let _ = engine.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );

    // Colliding write from a second, disjoint writer: before the
    // spilled-value read existed, the stored side's value-less
    // view would send `PnCounterResolver` to its plain-`Hlc`
    // fallback, and this strictly newer incoming version would
    // win outright, discarding node_a's spilled contribution
    // instead of folding it in.
    let outcome = put_with_resolver(
        &engine,
        key,
        kb.clone(),
        PnCounter::local_delta(node_b, 4),
        hlc(5, 2),
        None,
        0,
        &resolver,
    );

    match outcome {
        ApplyOutcome::Put { value, .. } => {
            assert_eq!(
                value.value(),
                7,
                "merging against a spilled stored record folds its contribution \
                 instead of dropping it"
            );
        }
        ApplyOutcome::Rejected => {
            panic!("expected a Put outcome carrying the merged counter, got Rejected")
        }
        ApplyOutcome::Tombstoned { .. } => {
            panic!("expected a Put outcome carrying the merged counter, got Tombstoned")
        }
        ApplyOutcome::TooLarge { .. } => {
            panic!("expected a Put outcome carrying the merged counter, got TooLarge")
        }
    }
    assert_eq!(
        engine.get(&key, 0).map(|c| c.value()),
        Some(7),
        "the merged counter reads back resident, carrying both sides' \
         contributions"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A spilled stored record that has expired by `apply_locked`'s clock
/// never folds into a merge, and its bytes are not read: the write over it
/// starts a new value.
#[test]
fn merge_against_an_expired_spilled_counter_starts_a_new_value() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let dir = temp_dir("merge-spilled-expired");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "merge-spilled-expired").expect("tier opens"));
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let resolver = PnCounterResolver;
    let key = 1u32;
    let kb = key_bytes(key);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    let node_a = crate::store::crdt::WriterId::new(NodeId::from(11), 1);
    let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);

    let _ = put_with_resolver(
        &engine,
        key,
        kb.clone(),
        PnCounter::local_delta(node_a, 3),
        hlc(1, 1),
        Some(100),
        0,
        &resolver,
    );
    let _ = engine.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );

    // Stamped before the spilled record's deadline, applied after it.
    let outcome = put_with_resolver(
        &engine,
        key,
        kb.clone(),
        PnCounter::local_delta(node_b, 4),
        hlc(50, 2),
        None,
        500,
        &resolver,
    );
    assert!(
        matches!(outcome, ApplyOutcome::Put { created: true, .. }),
        "a write over an expired spilled record creates"
    );
    assert_eq!(
        engine.get(&key, 500).map(|c| c.value()),
        Some(4),
        "the expired spilled contribution does not fold in"
    );
    assert!(
        !is_spilled(&engine, &kb),
        "the new value is resident; the spilled record is displaced"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A resolver that never merges and reads value bytes, recording the stored
/// side's bytes every [`ConflictResolver::winner`] call is shown. The
/// incoming side always wins.
#[derive(Default)]
struct RecordingResolver {
    stored_seen: std::sync::Mutex<Vec<Option<Vec<u8>>>>,
}

impl ConflictResolver for RecordingResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, _b: RecordView<'_>) -> Winner {
        self.stored_seen
            .lock()
            .expect("lock")
            .push(a.value.map(<[u8]>::to_vec));
        Winner::B
    }
}

/// A resolver that does not merge sees the value bytes of a spilled stored
/// record that has expired, as it does for a resident one: the skipped read
/// is the merging resolvers' alone.
#[test]
fn a_non_merging_resolver_reads_an_expired_spilled_record() {
    let dir = temp_dir("recording-spilled-expired");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "recording-spilled-expired").expect("tier opens"));
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let key = 1u32;
    let kb = key_bytes(key);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "aaaa".to_owned(),
        hlc(1, 1),
        Some(100),
        0,
    );
    let _ = engine.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );

    let resolver = RecordingResolver::default();
    let outcome = put_with_resolver(
        &engine,
        key,
        kb.clone(),
        "b".to_owned(),
        hlc(5, 2),
        None,
        500,
        &resolver,
    );
    assert!(matches!(outcome, ApplyOutcome::Put { created: true, .. }));
    assert_eq!(
        *resolver.stored_seen.lock().expect("lock"),
        vec![Some(postcard::to_stdvec("aaaa").expect("encodes"))],
        "the resolver is shown the expired spilled bytes"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// `read_stored_live` returns no bytes for a spilled record that is not
/// visible when the resolver merges, whether or not the prefetch holds them,
/// and returns them for a visible record and for a resolver that does not
/// merge.
#[test]
fn read_stored_live_skips_an_invisible_spilled_record_for_a_merging_resolver_only() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let dir = temp_dir("read-stored-live");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "read-stored-live").expect("tier opens"));
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let merging = PnCounterResolver;
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let writer = crate::store::crdt::WriterId::new(NodeId::from(11), 1);
    let _ = put_with_resolver(
        &engine,
        key,
        kb.clone(),
        PnCounter::local_delta(writer, 3),
        hlc(1, 1),
        Some(100),
        0,
        &merging,
    );
    let _ = engine.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );

    // Prefetched while the record is visible, so the map holds its bytes
    // for every read below.
    let prefetch = |resolver: &dyn ConflictResolver, now_ms: u64| {
        prefetch_spilled_conflict_bytes(
            engine.stripe_lock(bucket),
            std::iter::once((hash, &kb)),
            resolver,
            engine.spill().map(Arc::as_ref),
            None,
            now_ms,
        )
    };
    let read = |resolver: &dyn ConflictResolver, now_ms: u64| {
        let prefetched = prefetch(resolver, 50);
        assert_eq!(prefetched.len(), 1, "the prefetch holds the spilled bytes");
        let ctx = ApplyCtx {
            prefetched_spilled: &prefetched,
            spill: engine.spill().map(Arc::as_ref),
            ..test_ctx(&engine, bucket, hash, resolver, 60_000, 600_000, now_ms)
        };
        let stripe = engine.stripe_lock(bucket).read();
        let stored = read_stored_live(&stripe, &ctx, hash, kb.as_ref()).expect("a live entry");
        (stored.visible, stored.encoded.is_some())
    };
    assert_eq!(read(&merging, 50), (true, true), "a visible record is read");
    assert_eq!(
        prefetch(&merging, 500).len(),
        0,
        "the prefetch skips an expired record for a merging resolver"
    );
    assert_eq!(
        prefetch(&RecordingResolver::default(), 500).len(),
        1,
        "and reads it for a resolver that does not merge"
    );
    assert_eq!(
        read(&merging, 500),
        (false, false),
        "an expired record is not read for a merging resolver, prefetched or not"
    );
    assert_eq!(
        read(&RecordingResolver::default(), 500),
        (false, true),
        "a resolver that does not merge still reads it"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A batch over an expired spilled record starts a new value.
#[test]
fn an_apply_many_over_an_expired_spilled_counter_starts_a_new_value() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let dir = temp_dir("apply-many-expired-spilled");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "apply-many-expired-spilled").expect("tier opens"));
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let resolver = PnCounterResolver;
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let node_a = crate::store::crdt::WriterId::new(NodeId::from(11), 1);
    let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);
    let _ = put_with_resolver(
        &engine,
        key,
        kb.clone(),
        PnCounter::local_delta(node_a, 3),
        hlc(1, 1),
        Some(100),
        0,
        &resolver,
    );
    let _ = engine.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );

    let delta = PnCounter::local_delta(node_b, 4);
    let encoded = Bytes::from(postcard::to_stdvec(&delta).expect("encodes"));
    let outcomes = engine.apply_many(
        Arrival::Peer,
        bucket,
        vec![(
            hash,
            key,
            kb.clone(),
            hlc(50, 2),
            Incoming::Put {
                value: delta,
                expires_at_ms: None,
                encoded,
            },
        )],
        &resolver,
        60_000,
        600_000,
        500,
    );
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::Put { created: true, .. }
    ));
    assert_eq!(
        engine.get(&key, 500).map(|c| c.value()),
        Some(4),
        "the expired spilled contribution does not fold in"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A batch over an expired spilled seed leaves the record sequential
/// application does, including a run entry stamped below the seed's version.
#[test]
fn prefold_over_an_expired_spilled_seed_matches_sequential_application() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let resolver = PnCounterResolver;
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let node_p_id = NodeId::from(1);
    let node_p = crate::store::crdt::WriterId::new(node_p_id, 1);
    let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);
    let node_c = crate::store::crdt::WriterId::new(NodeId::from(33), 1);
    let p_ver = Hlc {
        wall_ms: 60,
        logical: 5,
        node: node_p_id,
    };
    let e0 = (hlc(5, 2), PnCounter::local_delta(node_b, 4));
    let e1 = (hlc(70, 3), PnCounter::local_delta(node_c, 9));

    let sequential = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    let _ = put_with_resolver(
        &sequential,
        key,
        kb.clone(),
        PnCounter::local_delta(node_p, 3),
        p_ver,
        Some(100),
        0,
        &resolver,
    );
    for (ver, value) in [e0.clone(), e1.clone()] {
        let _ = put_with_resolver(
            &sequential,
            key,
            kb.clone(),
            value,
            ver,
            None,
            500,
            &resolver,
        );
    }
    let (sequential_ver, sequential_value) = stored_ver_and_value(&sequential, key, &kb, bucket);

    let dir = temp_dir("prefold-expired-spilled-seed");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, "prefold-expired-spilled-seed").expect("tier opens"));
    let batched = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    batched.set_spill(Arc::clone(&tier));
    let batched = Arc::new(batched);
    tier.attach(Arc::downgrade(
        &(Arc::clone(&batched) as Arc<dyn SpillSink>),
    ));
    let _ = put_with_resolver(
        &batched,
        key,
        kb.clone(),
        PnCounter::local_delta(node_p, 3),
        p_ver,
        Some(100),
        0,
        &resolver,
    );
    let _ = batched.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&batched, &kb)),
        "the flusher installs the spilled entry"
    );
    assert!(batched.prefold_enabled());

    let entry = |(ver, value): (Hlc, PnCounter)| {
        let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encodes"));
        (
            hash,
            key,
            kb.clone(),
            ver,
            Incoming::Put {
                value,
                expires_at_ms: None,
                encoded,
            },
        )
    };
    let _ = batched.apply_many(
        Arrival::Peer,
        bucket,
        vec![entry(e0), entry(e1)],
        &resolver,
        60_000,
        600_000,
        500,
    );
    let (batched_ver, batched_value) = stored_ver_and_value(&batched, key, &kb, bucket);

    assert_eq!(batched_value, 13, "the expired seed's 3 does not fold in");
    assert_eq!(batched_value, sequential_value);
    assert_eq!(batched_ver, sequential_ver);

    let _ = std::fs::remove_dir_all(&dir);
}

/// `peek_stored_seed`'s prefold seeding is spill-aware exactly
/// like `apply_locked`'s own stored-side lookup: a same-key,
/// multi-entry batch landing on an already-spilled stored record
/// folds `P` in *first*, ahead of the batch's own entries, the
/// same left-to-right order sequential per-record application
/// uses, so the version the batched fold mints matches
/// sequential application's exactly, not only its content.
/// Seeding from a spilled `P` this way is what this test pins:
/// treating a spilled `P` as absent, seeding the fold with the
/// batch's own first entry instead, would still mint correct
/// content (`apply_locked`'s own final call still re-folds the
/// real `P` in) but not the identical `Hlc` sequential
/// application mints.
/// This test's own `(version, value)` reader, shared by its
/// sequential-reference and batched-under-test engines.
fn stored_ver_and_value(
    engine: &Engine<u32, crate::store::crdt::PnCounter>,
    key: u32,
    kb: &Bytes,
    bucket: usize,
) -> (Hlc, i128) {
    let ver = engine
        .collect_buckets(&[u16::try_from(bucket).expect("bucket fits u16")], 0)
        .into_iter()
        .flat_map(|(_, entries)| entries)
        .find(|kv| kv.key.as_ref() == kb.as_ref())
        .map(|kv| kv.version)
        .expect("the key is live");
    let value = engine
        .get(&key, 0)
        .expect("the key reads back resident")
        .value();
    (ver, value)
}

/// A fresh engine, seeded with one `(seed, seed_ver)` record at
/// `key`, then evicted and confirmed spilled: this test's own
/// `P`, shared by its batched-under-test engine. Returns the temp
/// dir (removed by the caller once done) alongside the engine.
fn spilled_pn_counter_engine(
    dir_name: &str,
    key: u32,
    kb: &Bytes,
    bucket: usize,
    seed: crate::store::crdt::PnCounter,
    seed_ver: Hlc,
    resolver: &dyn ConflictResolver,
) -> (
    std::path::PathBuf,
    Arc<Engine<u32, crate::store::crdt::PnCounter>>,
) {
    let dir = temp_dir(dir_name);
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, dir_name).expect("tier opens"));
    let engine = Engine::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));
    let _ = put_with_resolver(&engine, key, kb.clone(), seed, seed_ver, None, 0, resolver);
    let _ = engine.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, kb)),
        "the flusher installs the spilled entry"
    );
    (dir, engine)
}

#[test]
fn prefold_seeds_a_spilled_stored_record_so_the_batch_matches_sequential_application() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let resolver = PnCounterResolver;
    let key = 1u32;
    let kb = key_bytes(key);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    let node_p_id = NodeId::from(1);
    let node_p = crate::store::crdt::WriterId::new(node_p_id, 1);
    let node_b = crate::store::crdt::WriterId::new(NodeId::from(22), 1);
    let node_c = crate::store::crdt::WriterId::new(NodeId::from(33), 1);
    // A nonzero `logical` simulates `P` already carrying prior
    // merge history (a realistic starting point for a spilled
    // counter): with an all-fresh, all-zero-`logical` `P`, the
    // mint arm's `max(..) + 1` chain lands on the identical
    // `(wall_ms, logical)` regardless of fold order (both paths
    // always cost exactly two total merge steps for three
    // leaves), which would let this test pass even with the bug
    // it's meant to catch.
    let p_ver = Hlc {
        wall_ms: 1,
        logical: 5,
        node: node_p_id,
    };
    let e0 = (hlc(5, 2), PnCounter::local_delta(node_b, 4));
    let e1 = (hlc(6, 3), PnCounter::local_delta(node_c, 9));

    // The reference: `P`, then each batch entry, one real
    // `apply_many` call at a time against a plain resident
    // engine, never spilled, since spilling is this test's own
    // artifact, not part of the CRDT history being compared.
    let sequential = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    for (value, ver) in [
        (PnCounter::local_delta(node_p, 3), p_ver),
        (e0.1.clone(), e0.0),
        (e1.1.clone(), e1.0),
    ] {
        let _ = put_with_resolver(&sequential, key, kb.clone(), value, ver, None, 0, &resolver);
    }
    let (sequential_ver, sequential_value) = stored_ver_and_value(&sequential, key, &kb, bucket);

    // The batch under test: `P` applied and spilled exactly like
    // the sibling test above, then `e0` and `e1` folded through
    // one real `apply_many` call, prefold-eligible since both
    // share a key and `PnCounterResolver::merges()` is `true`.
    let (dir, batched) = spilled_pn_counter_engine(
        "prefold-seed-spilled",
        key,
        &kb,
        bucket,
        PnCounter::local_delta(node_p, 3),
        p_ver,
        &resolver,
    );
    assert!(
        batched.prefold_enabled(),
        "prefold is on by default: this batch must actually exercise it"
    );

    let hash = hash_key_bytes(kb.as_ref());
    let e0_encoded = Bytes::from(postcard::to_stdvec(&e0.1).expect("encodes"));
    let e1_encoded = Bytes::from(postcard::to_stdvec(&e1.1).expect("encodes"));
    let outcomes = batched.apply_many(
        Arrival::Peer,
        bucket,
        vec![
            (
                hash,
                key,
                kb.clone(),
                e0.0,
                Incoming::Put {
                    value: e0.1,
                    expires_at_ms: None,
                    encoded: e0_encoded,
                },
            ),
            (
                hash,
                key,
                kb.clone(),
                e1.0,
                Incoming::Put {
                    value: e1.1,
                    expires_at_ms: None,
                    encoded: e1_encoded,
                },
            ),
        ],
        &resolver,
        60_000,
        600_000,
        0,
    );
    assert_eq!(
        outcomes.len(),
        2,
        "one outcome per entry given, pre-fold or not"
    );

    let (batched_ver, batched_value) = stored_ver_and_value(&batched, key, &kb, bucket);

    assert_eq!(
        batched_value, sequential_value,
        "both paths fold every side's contribution into the same total"
    );
    assert_eq!(
        batched_ver, sequential_ver,
        "seeding the fold with the real (spilled) stored record first mints the \
         exact version sequential per-record application would have, not merely \
         the same content"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
