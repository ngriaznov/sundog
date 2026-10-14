//! Tests for the engine over a spill tier: spilled reads, conflicts, promotion and the hand-off to the flusher.

use super::*;
use crate::store::spill::{Admission, ReclaimRef, SpillJob, SpillSink, spill_record_len};
#[cfg(not(feature = "sim"))]
use crate::store::spill::{SpillConfig, SpillTier};

fn loc(region: u32, offset: u32, len: u32, generation: u32) -> SpillLoc {
    SpillLoc {
        region,
        offset,
        len,
        generation,
    }
}

/// [`spilled_read_in_flight`]'s engine, key bytes, hash, the
/// owner's in-flight entry and the joined waiter's receiver.
type SpilledReadInFlight = (
    Engine<u32, String>,
    Bytes,
    u64,
    Arc<Inflight<String>>,
    watch::Receiver<bool>,
);

/// An engine holding key 7 spilled at `hlc(10, 1)`, unexpiring or
/// expiring at `expires_at_ms`, with an in-flight read of it that
/// one owner registered and one waiter joined.
fn spilled_read_in_flight(expires_at_ms: Option<u64>) -> SpilledReadInFlight {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());
    engine.debug_insert_spilled(&kb, hlc(10, 1), expires_at_ms, loc(0, 0, 10, 0), 0);
    let JoinOutcome::Owner(inflight) = engine.miss_or_join(&kb, hash, 1) else {
        panic!("a spilled key's first miss owns the read");
    };
    let JoinOutcome::Join(_, done) = engine.miss_or_join(&kb, hash, 1) else {
        panic!("a second miss joins the read in flight");
    };
    (engine, kb, hash, inflight, done)
}

#[test]
fn a_spilled_read_hands_its_value_to_waiters_while_the_key_still_holds_it() {
    let (engine, kb, hash, inflight, done) = spilled_read_in_flight(None);
    engine.finish_spilled_read(&kb, hash, &inflight, hlc(10, 1), "v".to_string(), 2);
    assert_eq!(inflight.value.get(), Some(&"v".to_string()));
    assert!(*done.borrow(), "every waiter wakes");
    assert!(
        matches!(engine.miss_or_join(&kb, hash, 3), JoinOutcome::Owner(_)),
        "the in-flight entry is gone"
    );
}

#[test]
fn a_spilled_read_hands_nothing_to_waiters_once_the_key_moved_on() {
    // Removed after the read.
    let (engine, kb, hash, inflight, done) = spilled_read_in_flight(None);
    tombstone(&engine, 7, kb.clone(), hlc(11, 1), 2);
    engine.finish_spilled_read(&kb, hash, &inflight, hlc(10, 1), "v".to_string(), 2);
    assert_eq!(
        inflight.value.get(),
        None,
        "a removal leaves waiters to read again"
    );
    assert!(*done.borrow());

    // Overwritten after the read.
    let (engine, kb, hash, inflight, _done) = spilled_read_in_flight(None);
    let _ = put(
        &engine,
        7,
        kb.clone(),
        "newer".to_string(),
        hlc(11, 1),
        None,
        2,
    );
    engine.finish_spilled_read(&kb, hash, &inflight, hlc(10, 1), "v".to_string(), 2);
    assert_eq!(
        inflight.value.get(),
        None,
        "a newer write leaves waiters to read again"
    );

    // Expired after the read.
    let (engine, kb, hash, inflight, _done) = spilled_read_in_flight(Some(20));
    engine.finish_spilled_read(&kb, hash, &inflight, hlc(10, 1), "v".to_string(), 30);
    assert_eq!(
        inflight.value.get(),
        None,
        "an expiry leaves waiters to read again"
    );
    assert!(
        matches!(engine.miss_or_join(&kb, hash, 31), JoinOutcome::Owner(_)),
        "the in-flight entry is gone either way"
    );
}

#[test]
fn a_claimed_promotion_that_fails_its_recheck_gives_the_claim_back() {
    let engine = engine_u32_string(100, None);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());
    let encoded = Bytes::from(postcard::to_stdvec(&"v".to_string()).expect("string encodes"));
    engine.debug_insert_spilled(&kb, hlc(10, 1), None, loc(0, 0, 10, 0), 0);
    tombstone(&engine, 7, kb.clone(), hlc(11, 1), 1);

    assert!(engine.claim_weight(1));
    assert_eq!(engine.debug_totals().1, 1, "the claim counts at once");
    assert!(
        !engine.reinstall_claimed(kb.as_ref(), hash, hlc(10, 1), 1, &encoded),
        "a removal since the read fails the recheck"
    );
    assert_eq!(
        engine.debug_totals().1,
        0,
        "the failed recheck gives the claim back"
    );

    let kb = key_bytes(8);
    let hash = hash_key_bytes(kb.as_ref());
    engine.debug_insert_spilled(&kb, hlc(10, 1), None, loc(0, 0, 10, 0), 0);
    assert!(engine.claim_weight(1));
    assert!(engine.reinstall_claimed(kb.as_ref(), hash, hlc(10, 1), 1, &encoded));
    assert_eq!(engine.debug_totals().1, 1, "a reinstall keeps its claim");
    assert_eq!(
        engine.get(&8, 2),
        Some("v".to_string()),
        "and the entry is resident"
    );
}

#[test]
fn release_buckets_decrements_the_spilled_entries_gauge_for_a_departing_spilled_entry() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
    assert_eq!(engine.debug_spill_entries_count(), 1);

    let removed = engine.release_buckets(&[u16::try_from(bucket).expect("invariant: fits")]);

    assert_eq!(
        removed, 1,
        "the spilled entry counts toward the removed total same as a resident one"
    );
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "release_buckets corrects the spilled-entries gauge for a departing spilled entry"
    );
    assert_eq!(engine.live_entry_count(), 0);
}

#[test]
fn snapshot_spilled_reports_pointers_for_spilled_entries_only() {
    let engine = engine_u32_string(u64::MAX, None);
    let key1 = 1u32;
    let kb1 = key_bytes(key1);
    let l = loc(0, 0, 4, 0);
    engine.debug_insert_spilled(&kb1, hlc(1, 1), Some(500), l, 0);
    let key2 = 2u32;
    let kb2 = key_bytes(key2);
    let _ = put(
        &engine,
        key2,
        kb2.clone(),
        "resident".to_string(),
        hlc(1, 1),
        None,
        0,
    );

    let spilled = engine.snapshot_spilled(0);
    assert_eq!(spilled.len(), 1, "only the spilled key is reported here");
    let (k, ver, expires_at_ms, reported_loc) = &spilled[0];
    assert_eq!(k.as_ref(), kb1.as_ref());
    assert_eq!(*ver, hlc(1, 1));
    assert_eq!(*expires_at_ms, Some(500));
    assert_eq!(*reported_loc, l);

    let resident = engine.snapshot_records(0);
    assert_eq!(
        resident.len(),
        1,
        "snapshot_records reports only the resident key"
    );
    assert_eq!(resident[0].key.as_ref(), kb2.as_ref());
}

#[test]
fn records_for_or_spilled_splits_a_resident_and_a_spilled_key() {
    let engine = engine_u32_string(u64::MAX, None);
    let spilled_key = 1u32;
    let kb_spilled = key_bytes(spilled_key);
    let l = loc(0, 0, 4, 0);
    let spilled_ver = hlc(1, 1);
    engine.debug_insert_spilled(&kb_spilled, spilled_ver, Some(500), l, 0);

    let resident_key = 2u32;
    let kb_resident = key_bytes(resident_key);
    let _ = put(
        &engine,
        resident_key,
        kb_resident.clone(),
        "resident".to_string(),
        hlc(2, 1),
        None,
        0,
    );

    let (records, spilled) =
        engine.records_for_or_spilled(&[kb_resident.clone(), kb_spilled.clone()], 0);

    assert_eq!(
        records.len(),
        1,
        "only the resident key comes back as a WireRecord"
    );
    assert_eq!(records[0].key.as_ref(), kb_resident.as_ref());
    assert_eq!(
        records[0].value.as_deref(),
        Some(
            postcard::to_stdvec(&"resident".to_string())
                .expect("test value encodes")
                .as_slice()
        )
    );
    assert_eq!(records[0].ver, hlc(2, 1));

    assert_eq!(
        spilled.len(),
        1,
        "only the spilled key comes back as a pointer"
    );
    let (k, ver, expires_at_ms, reported_loc) = &spilled[0];
    assert_eq!(k.as_ref(), kb_spilled.as_ref());
    assert_eq!(*ver, spilled_ver);
    assert_eq!(*expires_at_ms, Some(500));
    assert_eq!(*reported_loc, l);
}

#[test]
fn get_by_bytes_and_record_for_return_none_for_a_spilled_entry() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);

    assert_eq!(
        engine.get(&key, 0),
        None,
        "a spilled entry never answers get with a value"
    );
    assert_eq!(engine.get_by_bytes(kb.as_ref(), hash, 0), None);
    assert!(
        engine.contains_key(&key, 0),
        "existence doesn't need the value bytes"
    );
    assert!(
        engine.record_for(kb.as_ref(), 0).is_none(),
        "record_for skips a spilled entry; fan-out simply drops it, repaired later by AE"
    );
}

#[test]
fn miss_or_join_never_returns_hit_for_a_spilled_entry() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);

    match engine.miss_or_join(&kb, hash, 0) {
        JoinOutcome::Hit(_) => panic!("a spilled entry has no resident value to hit on"),
        JoinOutcome::Owner(_) | JoinOutcome::Join(..) => {}
    }
}

#[test]
fn inspect_reports_a_spilled_entry_and_leaves_it_spilled() {
    let engine = Engine::<u32, String>::new(10, None, None, None);
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let ver = hlc(1, 1);
    let l = loc(2, 8, 4, 1);
    engine.debug_insert_spilled(&kb, ver, Some(5_000), l, 0);

    assert_eq!(
        engine.inspect(kb.as_ref(), 100),
        LocalRecord::Live {
            version: ver,
            expires_at_ms: Some(5_000),
            spilled: true,
        }
    );
    assert_eq!(last_access(&engine, 1), 0, "an inspect is no access");
    assert_eq!(
        engine.spilled_loc(kb.as_ref(), hash, 100),
        Some((ver, l)),
        "the entry stays spilled at the same location"
    );
}

#[test]
fn spilled_loc_snapshots_the_pointer_and_touches_last_access() {
    let engine = Engine::<u32, String>::new(10, None, None, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let ver = hlc(1, 1);
    let l = loc(2, 8, 4, 1);
    engine.debug_insert_spilled(&kb, ver, None, l, 0);

    assert_eq!(engine.spilled_loc(kb.as_ref(), hash, 100), Some((ver, l)));

    let bucket = stripe_index_from_hash(hash);
    let stripe = engine.stripe_lock(bucket).read();
    let live = stripe
        .live
        .iter()
        .find(|live| record_key(&live.record) == kb.as_ref())
        .expect("the entry is present");
    assert_eq!(
        live.last_access_ms.load(Ordering::Relaxed),
        100,
        "spilled_loc touches last_access for a capacity-tracking engine"
    );
    drop(stripe);

    let missing = key_bytes(999);
    assert_eq!(
        engine.spilled_loc(missing.as_ref(), hash_key_bytes(missing.as_ref()), 100),
        None,
        "an absent key yields None"
    );

    let key2 = 2u32;
    let kb2 = key_bytes(key2);
    let _ = put(
        &engine,
        key2,
        kb2.clone(),
        "resident".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(
        engine.spilled_loc(kb2.as_ref(), hash_key_bytes(kb2.as_ref()), 100),
        None,
        "a resident entry yields None too"
    );
}

#[test]
fn promote_locked_restores_residency_without_touching_the_digest_or_live_count() {
    let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let ver = hlc(5, 1);
    engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 10, 0), 0);

    let digest_before = engine.digests();
    let (live_count_before, weight_before) = engine.debug_totals();
    assert_eq!(weight_before, 0, "a spilled entry contributes zero weight");

    let promoted = engine.promote_locked(
        kb.as_ref(),
        hash,
        ver,
        &"restored".to_string(),
        &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
        0,
    );
    assert!(
        promoted,
        "the version matches and nothing displaced it: promotion succeeds"
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes()),
        "promotion adds the restored value's heap bytes to the resident total"
    );

    assert_eq!(engine.get(&key, 0), Some("restored".to_string()));
    assert_eq!(
        engine.digests(),
        digest_before,
        "promotion never touches the digest"
    );
    let (live_count_after, weight_after) = engine.debug_totals();
    assert_eq!(
        live_count_after, live_count_before,
        "promotion never touches live_count"
    );
    assert_eq!(
        weight_after, 1,
        "promotion adds the freshly weighed entry's weight back to total_weight"
    );
}

#[test]
fn promote_locked_adds_the_restored_value_heap_bytes_to_resident_bytes() {
    let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let ver = hlc(5, 1);
    engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 10, 0), 0);
    assert_eq!(
        engine.resident_bytes(),
        Some(ENTRY_OVERHEAD_BYTES),
        "a spilled entry keeps only its inline key-only record resident"
    );
    let restored = "r".repeat(100);
    let encoded = Bytes::from(postcard::to_stdvec(&restored).expect("string encodes"));
    assert!(engine.promote_locked(kb.as_ref(), hash, ver, &restored, &encoded, 0));
    assert_eq!(
        engine.resident_bytes(),
        Some(ENTRY_OVERHEAD_BYTES + (1 + kb.len() + encoded.len()) as u64),
        "promotion brings the whole record back onto the heap"
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes())
    );
}

#[test]
fn promote_locked_is_a_noop_once_already_resident() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let ver = hlc(5, 1);
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "already-here".to_string(),
        ver,
        None,
        0,
    );

    let promoted = engine.promote_locked(
        kb.as_ref(),
        hash,
        ver,
        &"stale-read".to_string(),
        &Bytes::from_static(b"stale"),
        0,
    );
    assert!(
        !promoted,
        "nothing to promote: the entry is already resident"
    );
    assert_eq!(engine.get(&key, 0), Some("already-here".to_string()));
}

#[test]
fn promote_locked_invokes_the_weigher_with_the_original_key_and_value() {
    let calls: Arc<std::sync::Mutex<Vec<(u32, String)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = Arc::clone(&calls);
    let weigher: Weigher<u32, String> = Box::new(move |k, v| {
        recorded.lock().expect("lock").push((*k, v.clone()));
        1
    });
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let ver = hlc(5, 1);
    engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 10, 0), 0);

    let promoted = engine.promote_locked(
        kb.as_ref(),
        hash,
        ver,
        &"restored".to_string(),
        &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
        0,
    );
    assert!(promoted);

    let calls = calls.lock().expect("lock");
    assert_eq!(
        calls.as_slice(),
        &[(key, "restored".to_string())],
        "the weigher sees the original key, decoded from key_bytes, and the original \
         value passed in, not a stale or spilled-pointer substitute"
    );
}

#[test]
fn flusher_install_is_a_noop_when_a_newer_write_lands_first() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let old_ver = hlc(1, 1);
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "old".to_string(),
        old_ver,
        None,
        0,
    );
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "new".to_string(),
        hlc(2, 1),
        None,
        0,
    );

    let installed = SpillSink::install(&engine, bucket, &kb, hash, old_ver, loc(0, 0, 4, 0), 0);
    assert!(
        !installed,
        "a stale flush is discarded once a newer write has landed"
    );
    assert_eq!(
        engine.get(&key, 0),
        Some("new".to_string()),
        "the newer write is untouched"
    );
}

#[test]
fn flusher_install_is_a_noop_when_a_tombstone_lands_first() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(1, 1);
    let _ = put(&engine, key, kb.clone(), "old".to_string(), ver, None, 0);
    tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);

    let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 0);
    assert!(
        !installed,
        "a stale flush is discarded once a tombstone has landed"
    );
    assert_eq!(
        engine.get(&key, 0),
        None,
        "the key stays deleted; a late flush never resurrects it"
    );
}

#[test]
fn region_reclaim_purges_a_key_still_pointing_at_the_reclaimed_generation() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(3, 0, 10, 0), 0);

    let removed = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
    assert_eq!(
        removed, 1,
        "the key's pointer still names this exact region and generation"
    );
    assert_eq!(engine.get(&key, 0), None);
    let (live_count, weight) = engine.debug_totals();
    assert_eq!((live_count, weight), (0, 0));
}

#[test]
fn region_reclaim_of_a_long_ttl_key_clears_its_side_table_row() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let long_deadline = MAX_INLINE_TTL_MS + 5_000;
    engine.debug_insert_spilled(&kb, hlc(1, 1), Some(long_deadline), loc(3, 0, 10, 0), 0);
    assert!(
        engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "a long TTL spilled entry leaves its absolute deadline in the side table"
    );

    let removed = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
    assert_eq!(
        removed, 1,
        "the key's pointer still names this exact region and generation"
    );
    assert!(
        !engine
            .stripe_lock(bucket)
            .read()
            .long_ttl
            .contains_key(kb.as_ref()),
        "reclaiming the entry clears its side-table row; no row outlives its entry"
    );
}

#[test]
fn region_reclaim_skips_a_key_that_was_promoted_since_being_recorded() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(1, 1);
    engine.debug_insert_spilled(&kb, ver, None, loc(3, 0, 10, 0), 0);
    assert!(engine.promote_locked(
        kb.as_ref(),
        hash,
        ver,
        &"restored".to_string(),
        &Bytes::from(postcard::to_stdvec(&"restored".to_string()).expect("string encodes")),
        0,
    ));

    let digest_before = engine.digests();
    let removed = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
    assert_eq!(
        removed, 0,
        "a key promoted back to resident survives its old region's reclaim"
    );
    assert_eq!(engine.get(&key, 0), Some("restored".to_string()));
    assert_eq!(engine.digests(), digest_before);
}

/// A spilled entry at the expected version takes a gated put like a
/// resident one, and comes back resident with the put's expiry; its
/// lifetime reads from RAM before and after, with no disk read.
#[test]
fn apply_if_current_takes_a_spilled_entry_at_the_expected_version() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(7);
    let hash = hash_key_bytes(kb.as_ref());
    let base = hlc(1_000, 1);
    engine.debug_insert_spilled(&kb, base, Some(5_000), loc(0, 0, 10, 0), 0);
    assert_eq!(
        engine.ttl_of(kb.as_ref(), hash, 2_000),
        Some(remaining(3_000))
    );

    let encoded = Bytes::from(postcard::to_stdvec("v").expect("encodes"));
    let outcome = engine.apply_if_current(
        (
            hash,
            7,
            kb.clone(),
            hlc(1_000, 2),
            Incoming::Put {
                value: "v".to_string(),
                expires_at_ms: Some(9_000),
                encoded,
            },
        ),
        base,
        &LwwResolver,
        60_000,
        600_000,
        2_000,
    );
    assert!(matches!(outcome, Some(ApplyOutcome::Put { .. })));
    assert_eq!(
        engine.ttl_of(kb.as_ref(), hash, 2_000),
        Some(remaining(7_000))
    );
    assert_eq!(engine.get(&7, 2_000), Some("v".to_string()), "now resident");
    assert_eq!(engine.debug_spill_entries_count(), 0);
}

/// Location, not the key hash, decides what a reclaim removes: a row
/// naming another offset of the same region and generation leaves
/// the entry alone, and a row naming another stripe removes
/// nothing.
#[test]
fn region_reclaim_removes_only_the_entry_at_the_listed_offset() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(3, 40, 10, 0), 0);

    let elsewhere = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
    assert_eq!(
        elsewhere, 0,
        "a row for offset 0 leaves the entry that now sits at offset 40"
    );
    let other_stripe = (bucket + 1) % crate::store::BUCKET_COUNT;
    let unknown = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(other_stripe, hash, 40)]);
    assert_eq!(unknown, 0, "a row naming another stripe removes nothing");
    assert_eq!(
        engine.spilled_loc(kb.as_ref(), hash, 0).map(|(_, at)| at),
        Some(loc(3, 40, 10, 0)),
        "the entry still points at offset 40"
    );

    let here = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 40)]);
    assert_eq!(here, 1, "the row naming the entry's own offset removes it");
    assert_eq!(engine.get(&key, 0), None);
}

#[test]
fn region_reclaim_skips_a_key_that_was_overwritten_since_being_recorded() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(3, 0, 10, 0), 0);
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "fresh".to_string(),
        hlc(2, 1),
        None,
        0,
    );

    let removed = SpillSink::reclaim(&engine, 3, 0, &[ReclaimRef::new(bucket, hash, 0)]);
    assert_eq!(
        removed, 0,
        "an overwritten key's stale reverse-index row is left alone"
    );
    assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
}

/// Puts `live.weight` at `hash`/`kb` back to `0`, moves `weight` out
/// of `total_weight` and into `pending_spill_weight`, exactly the
/// state a successful hand-off to a spill tier leaves behind while
/// the flusher's write is still in flight. Lets a test drive
/// `SpillSink::abandon`/`SpillSink::install` directly, the same way
/// this module already drives `reclaim` directly, with no real disk
/// or flusher thread needed.
fn simulate_pending_handoff<K, V>(
    engine: &Engine<K, V>,
    bucket: usize,
    hash: u64,
    kb: &Bytes,
    weight: u32,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    {
        let mut stripe = engine.stripe_lock(bucket).write();
        let live = stripe
            .live
            .find_mut(hash, |l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        live.weight = 0;
    }
    engine
        .total_weight
        .fetch_sub(u64::from(weight), Ordering::Relaxed);
    engine
        .pending_spill_weight
        .fetch_add(u64::from(weight), Ordering::Relaxed);
}

#[test]
fn abandon_restores_the_weight_of_a_still_pending_entry() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(1, 1);
    let _ = put(&engine, key, kb.clone(), "x".repeat(7), ver, None, 0);
    let (_, weight_before) = engine.debug_totals();
    assert_eq!(weight_before, 7);

    simulate_pending_handoff(&engine, bucket, hash, &kb, 7);
    let (_, weight_pending) = engine.debug_totals();
    assert_eq!(weight_pending, 0);
    assert_eq!(
        engine.debug_pending_spill_weight(),
        7,
        "the hand-off's weight moved into pending_spill_weight"
    );

    SpillSink::abandon(&engine, bucket, &kb, hash, ver, 7);

    let (_, weight_after) = engine.debug_totals();
    assert_eq!(
        weight_after, 7,
        "abandon recomputes the weight through the weigher and adds it back to \
         total_weight"
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
        live.weight, 7,
        "the entry's own weight field is restored too, not just the total"
    );
}

#[test]
fn abandon_is_a_noop_once_the_keys_stored_state_has_changed() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let old_ver = hlc(1, 1);
    let _ = put(&engine, key, kb.clone(), "x".repeat(7), old_ver, None, 0);
    simulate_pending_handoff(&engine, bucket, hash, &kb, 7);

    // A fresh write lands before the abandoned job is ever noticed:
    // this is exactly what `apply_put`'s own weight bookkeeping
    // already resolved correctly, so abandon must leave it alone.
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "fresh".to_string(),
        hlc(2, 1),
        None,
        0,
    );
    let (_, weight_before_abandon) = engine.debug_totals();
    assert_eq!(weight_before_abandon, 5, "\"fresh\".len() == 5");

    SpillSink::abandon(&engine, bucket, &kb, hash, old_ver, 7);

    let (_, weight_after) = engine.debug_totals();
    assert_eq!(
        weight_after, weight_before_abandon,
        "a key whose stored state changed since hand-off is left alone"
    );
    assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
    assert_eq!(
        engine.debug_pending_spill_weight(),
        0,
        "the job's weight leaves pending_spill_weight even though the key moved on"
    );
}

#[test]
fn abandon_on_a_key_that_is_no_longer_present_is_a_noop() {
    let engine = engine_u32_string(u64::MAX, None);
    let kb = key_bytes(999);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let (_, weight_before) = engine.debug_totals();

    SpillSink::abandon(&engine, bucket, &kb, hash, hlc(1, 1), 0);

    let (_, weight_after) = engine.debug_totals();
    assert_eq!(weight_after, weight_before);
}

#[test]
fn install_releases_its_share_of_pending_spill_weight() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(1, 1);
    let _ = put(&engine, key, kb.clone(), "x".repeat(9), ver, None, 0);
    simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
    assert_eq!(engine.debug_pending_spill_weight(), 9);

    let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 9);
    assert!(installed);

    assert_eq!(
        engine.debug_pending_spill_weight(),
        0,
        "install moves the hand-off's weight out of pending_spill_weight, not just \
         the entry's own payload"
    );
    let (_, weight_after) = engine.debug_totals();
    assert_eq!(
        weight_after, 0,
        "install never adds to total_weight; the entry stays at weight 0, Spilled"
    );
}

#[test]
fn install_copies_the_key_into_a_fresh_allocation_instead_of_slicing() {
    // A key over INLINE_CAP forces the heap path, where a
    // pointer-identity check is meaningful.
    let engine = Engine::<String, String>::new(u64::MAX, None, None, None).count_resident_bytes();
    let key = "k".repeat(40);
    let kb = string_key_bytes(&key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(1, 1);
    let _ = put(&engine, key, kb.clone(), "value".to_string(), ver, None, 0);
    simulate_pending_handoff(&engine, bucket, hash, &kb, 1);

    let record_ptr_before = {
        let stripe = engine.stripe_lock(bucket).read();
        let live = stripe
            .live
            .iter()
            .find(|l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        live.record.as_ref().as_ptr()
    };

    let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 1);
    assert!(installed);

    let record_ptr_after = {
        let stripe = engine.stripe_lock(bucket).read();
        let live = stripe
            .live
            .iter()
            .find(|l| record_key(&l.record) == kb.as_ref())
            .expect("entry is present");
        live.record.as_ref().as_ptr()
    };

    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes()),
        "install drops the value's heap bytes from the resident total"
    );
    assert_ne!(
        record_ptr_before, record_ptr_after,
        "install builds a fresh key-only allocation rather than slicing the old \
         resident record: a slice would share its refcount and keep the value bytes \
         physically resident in RAM"
    );
}

#[test]
fn install_of_an_inline_record_reuses_the_slot_with_no_heap_allocation() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(1, 1);
    let _ = put(&engine, key, kb.clone(), "value".to_string(), ver, None, 0);
    simulate_pending_handoff(&engine, bucket, hash, &kb, 1);

    let installed = SpillSink::install(&engine, bucket, &kb, hash, ver, loc(0, 0, 4, 0), 1);
    assert!(installed);

    let stripe = engine.stripe_lock(bucket).read();
    let live = stripe
        .live
        .iter()
        .find(|l| record_key(&l.record) == kb.as_ref())
        .expect("entry is present");
    assert!(
        matches!(live.record, Record::Inline { .. }),
        "a key-only record for a small u32 key stays inline: no heap allocation"
    );
    assert_eq!(record_key(&live.record), kb.as_ref());
    assert!(
        record_value(&live.record).is_empty(),
        "no old value bytes are retained anywhere"
    );
}

#[test]
fn install_new_never_resurrects_a_tombstoned_key() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    tombstone(&engine, key, kb.clone(), hlc(1, 1), 0);

    let inserted = SpillSink::install_new(
        &engine,
        bucket,
        &kb,
        hash,
        hlc(0, 0),
        None,
        loc(0, 0, 4, 0),
        1,
    );

    assert!(
        !inserted,
        "install_new never resurrects a key a tombstone already covers"
    );
    assert_eq!(engine.get(&key, 0), None);
    let (live_count, _) = engine.debug_totals();
    assert_eq!(live_count, 0);
}

#[test]
fn install_new_stamped_ahead_of_the_clock_stays_live_under_tti() {
    let engine = engine_u32_string(u64::MAX, Some(Duration::from_millis(100)));
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);

    // A warm reopen stamps the entry's last access with its write version's
    // wall time, which a restarted clock can trail.
    let inserted = SpillSink::install_new(
        &engine,
        bucket,
        &kb,
        hash,
        hlc(1_010, 1),
        None,
        loc(0, 0, 4, 0),
        1,
    );

    assert!(inserted);
    assert!(
        matches!(
            engine.inspect(kb.as_ref(), 1_005),
            LocalRecord::Live { spilled: true, .. }
        ),
        "an entry stamped ahead of the reader's clock is zero idle, not lapsed"
    );
}

#[test]
fn install_new_never_overwrites_an_already_present_live_entry() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let ver = hlc(2, 1);
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "resident".to_string(),
        ver,
        None,
        0,
    );
    let digest_before = engine.digests();

    let inserted = SpillSink::install_new(
        &engine,
        bucket,
        &kb,
        hash,
        hlc(1, 1),
        None,
        loc(0, 0, 4, 0),
        1,
    );

    assert!(
        !inserted,
        "install_new never overwrites a key that is already present, even at an \
         older version"
    );
    assert_eq!(engine.get(&key, 0), Some("resident".to_string()));
    assert_eq!(
        engine.digests(),
        digest_before,
        "a refused install_new never touches the digest"
    );
}

#[test]
fn install_releases_pending_weight_even_when_a_newer_write_displaced_the_key() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let old_ver = hlc(1, 1);
    let _ = put(&engine, key, kb.clone(), "x".repeat(9), old_ver, None, 0);
    simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
    let _ = put(
        &engine,
        key,
        kb.clone(),
        "fresh".to_string(),
        hlc(2, 1),
        None,
        0,
    );

    let installed = SpillSink::install(&engine, bucket, &kb, hash, old_ver, loc(0, 0, 4, 0), 9);

    assert!(!installed, "the newer write wins; nothing flips to Spilled");
    assert_eq!(
        engine.debug_pending_spill_weight(),
        0,
        "the job's weight leaves pending_spill_weight even though the key moved on"
    );
    let (_, weight_after) = engine.debug_totals();
    assert_eq!(
        weight_after, 5,
        "total_weight holds only the fresh value's weight"
    );
}

#[test]
fn abandon_releases_pending_weight_even_when_a_tombstone_displaced_the_key() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let old_ver = hlc(1, 1);
    let _ = put(&engine, key, kb.clone(), "x".repeat(9), old_ver, None, 0);
    simulate_pending_handoff(&engine, bucket, hash, &kb, 9);
    tombstone(&engine, key, kb.clone(), hlc(2, 1), 0);

    SpillSink::abandon(&engine, bucket, &kb, hash, old_ver, 9);

    assert_eq!(
        engine.debug_pending_spill_weight(),
        0,
        "the job's weight leaves pending_spill_weight although the key is tombstoned"
    );
    let (_, weight_after) = engine.debug_totals();
    assert_eq!(weight_after, 0, "nothing is restored onto a tombstoned key");
}

#[test]
fn enforce_capacity_counts_pending_spill_weight_toward_the_cap_and_defers_to_the_flusher() {
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(50, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb.clone(), "x".repeat(80), hlc(1, 1), None, 0);
    simulate_pending_handoff(&engine, bucket, hash, &kb, 80);
    // total_weight alone (0) already fits the 50-unit cap; only
    // adding pending_spill_weight (80) back in shows this bucket is
    // still, in effect, 30 units over.
    assert_eq!(engine.debug_totals().1, 0);
    assert_eq!(engine.debug_pending_spill_weight(), 80);

    engine.enforce_capacity(bucket, &mut Written::default(), 0);

    assert_eq!(
        engine.debug_eviction_lock_acquisitions(),
        1,
        "one batch pass finds nothing else to evict in this bucket; the stop rule \
         returns immediately rather than paying for a full-stripe scan"
    );
    assert_eq!(
        engine.get(&key, 0),
        Some("x".repeat(80)),
        "the pending entry itself is left alone: still resident, still readable"
    );
    assert_eq!(
        engine.debug_pending_spill_weight(),
        80,
        "still nobody resolved it"
    );
}

#[test]
fn insert_over_a_spilled_key_replaces_it_with_a_fresh_resident_entry() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
    let (live_count_before, weight_before) = engine.debug_totals();
    assert_eq!(weight_before, 0);
    assert_eq!(
        engine.debug_spill_entries_count(),
        1,
        "debug_insert_spilled counts against sundog_spill_entries like a real install"
    );

    let outcome = put(
        &engine,
        key,
        kb.clone(),
        "fresh".to_string(),
        hlc(2, 1),
        None,
        0,
    );
    assert!(
        matches!(outcome, ApplyOutcome::Put { created: false, .. }),
        "a spilled key is still live: a write over it is an update, not a creation"
    );
    assert_eq!(engine.get(&key, 0), Some("fresh".to_string()));
    let (live_count_after, weight_after) = engine.debug_totals();
    assert_eq!(
        live_count_after, live_count_before,
        "one spilled entry is replaced by one resident one: no net live_count change"
    );
    assert_eq!(weight_after, 1);
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "an overwrite of a spilled key must decrement sundog_spill_entries, the same as \
         a promotion would"
    );
}

#[test]
fn tombstone_over_a_spilled_key_removes_it_and_corrects_weight_and_live_count() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
    assert_eq!(engine.debug_spill_entries_count(), 1);

    {
        let mut stripe = engine.stripe_lock(bucket).write();
        let resolver = LwwResolver;
        let apply_ctx = test_ctx(&engine, bucket, hash, &resolver, 60_000, 600_000, 0);
        let (outcome, displaced_spilled) = apply_locked(
            &mut stripe,
            &apply_ctx,
            EntryKey {
                hash,
                key,
                key_bytes: kb.clone(),
            },
            hlc(2, 1),
            Incoming::Tombstone,
        );
        assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
        assert!(
            displaced_spilled,
            "apply_locked reports that the tombstoned entry was spilled"
        );
        engine.note_spill_departure(displaced_spilled);
    }
    assert_eq!(engine.get(&key, 0), None);
    let (live_count, weight) = engine.debug_totals();
    assert_eq!((live_count, weight), (0, 0));
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "a tombstone over a spilled key must decrement sundog_spill_entries"
    );
}

#[test]
fn invalidate_removes_a_spilled_key_at_a_newer_version() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let ver = hlc(1, 1);
    engine.debug_insert_spilled(&kb, ver, None, loc(0, 0, 4, 0), 0);
    assert_eq!(engine.debug_spill_entries_count(), 1);

    let removed_ver = engine.invalidate(kb.as_ref(), hash, hlc(2, 1));
    assert_eq!(removed_ver, Some(ver));
    assert_eq!(engine.get(&key, 0), None);
    let (live_count, weight) = engine.debug_totals();
    assert_eq!((live_count, weight), (0, 0));
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "invalidate of a spilled key must decrement sundog_spill_entries"
    );
}

#[test]
fn invalidate_local_removes_a_spilled_key_unconditionally() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
    assert_eq!(engine.debug_spill_entries_count(), 1);

    engine.invalidate_local(kb.as_ref(), hash);
    assert_eq!(engine.get(&key, 0), None);
    let (live_count, weight) = engine.debug_totals();
    assert_eq!((live_count, weight), (0, 0));
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "invalidate_local of a spilled key must decrement sundog_spill_entries"
    );
}

#[test]
fn sweep_removes_an_expired_spilled_key_and_corrects_the_digest() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    engine.debug_insert_spilled(&kb, hlc(1, 1), Some(50), loc(0, 0, 4, 0), 0);
    assert_eq!(engine.debug_spill_entries_count(), 1);

    engine.sweep(100);
    assert_eq!(engine.get(&key, 100), None);
    let (live_count, weight) = engine.debug_totals();
    assert_eq!((live_count, weight), (0, 0));
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "an expiry sweep of a spilled key must decrement sundog_spill_entries"
    );
}

#[test]
fn complete_fresh_load_over_a_spilled_key_decrements_spill_entries() {
    // A rare race: `get_spilled_by_bytes` already failed to promote
    // this key, since a concurrent tombstone or newer write raced
    // its read, yet the entry, sampled independently right here,
    // is still `Spilled` when the loader's fill lands.
    // `complete_fresh_load` replaces it, being newer, and must
    // still keep `sundog_spill_entries` correct.
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
    assert_eq!(engine.debug_spill_entries_count(), 1);

    let inflight = Arc::new(Inflight::<String>::new());
    let encoded = Bytes::from(postcard::to_stdvec(&"loaded".to_string()).unwrap());
    let outcome = engine.complete_fresh_load(
        FreshLoad {
            key: &key,
            key_bytes: &kb,
            hash,
            ver: hlc(2, 1),
            value: "loaded".to_string(),
            encoded,
            expires_at_ms: None,
        },
        0,
        &inflight,
    );
    assert_eq!(
        outcome,
        FillOutcome::Installed { had_live: true },
        "the spilled entry counted as already-live for complete_fresh_load's purposes"
    );
    assert_eq!(engine.get(&key, 0), Some("loaded".to_string()));
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "complete_fresh_load displacing a spilled key must decrement \
         sundog_spill_entries"
    );
}

#[test]
fn gc_tombstones_never_touches_a_spilled_live_entry() {
    let engine = engine_u32_string(u64::MAX, None);
    let key = 1u32;
    let kb = key_bytes(key);
    engine.debug_insert_spilled(&kb, hlc(1, 1), None, loc(0, 0, 4, 0), 0);
    let digest_before = engine.digests();

    engine.gc_tombstones(true, u64::MAX);

    assert_eq!(
        engine.digests(),
        digest_before,
        "gc_tombstones only ever touches stripe.tombstones"
    );
    let (live_count, _) = engine.debug_totals();
    assert_eq!(live_count, 1, "the spilled live entry is untouched");
}

#[test]
fn is_spill_candidate_excludes_a_weight_zeroed_entry_across_a_longer_pending_window() {
    // A `Reservation` wait suspends longer than a non-blocking pass
    // leaves open.
    let weigher: Weigher<u32, String> =
        Box::new(|_k, v| u32::try_from(v.len()).unwrap_or(u32::MAX));
    let engine = Engine::<u32, String>::new(u64::MAX, None, Some(weigher), None);
    let key = 1u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let _ = put(&engine, key, kb.clone(), "x".repeat(9), hlc(1, 1), None, 0);
    simulate_pending_handoff(&engine, bucket, hash, &kb, 9);

    for _ in 0..1_000 {
        assert!(
            engine.evict_one_sampled(bucket, 0).made_no_progress(),
            "a weight-zeroed pending entry is never re-selected, however long it \
             stays pending"
        );
    }
    assert_eq!(
        engine.get(&key, 0),
        Some("x".repeat(9)),
        "still resident and readable throughout"
    );
}

// --- Real disk: the flusher's actual eviction+install lifecycle.
// Never combined with `sim`: its virtual clock gives no determinism
// over real filesystem I/O or the flusher's OS thread.
#[cfg(not(feature = "sim"))]
mod io;
