//! Tests for the engine over a spill tier: spilled reads, conflicts, promotion and the hand-off to the flusher.

use super::*;
use crate::store::spill::{ReclaimRef, SpillSink};
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
mod io {
    use std::time::{Duration, Instant};

    use super::*;

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
        stripe.live.iter().any(|l| {
            record_key(&l.record) == kb.as_ref() && matches!(l.state, EntryState::Spilled(_))
        })
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
            outcome.removed_weight, 30,
            "the hand-off still commits and frees the weight from total_weight \
             right away, exactly as a physical removal would; abandon only puts it \
             back once the enqueue that follows is found to have failed"
        );

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
    fn a_promotion_behind_a_full_queue_keeps_every_refused_victim_resident_on_a_tier_that_deletes()
    {
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
        let (promote_kb, promote_value, promote_encoded) =
            (kb.clone(), value.clone(), encoded.clone());
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
            outcome.removed_weight, 30,
            "the hand-off still commits and frees the weight right away"
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
    async fn enforce_capacity_with_reservation_returns_the_deficit_once_both_budgets_are_exhausted()
    {
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
        let tier = Arc::new(
            SpillTier::open(&cfg, "enforce-capacity-deficit-fallback").expect("tier opens"),
        );
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
        }
        assert_eq!(
            engine.get(&key, 0).map(|c| c.value()),
            Some(7),
            "the merged counter reads back resident, carrying both sides' \
             contributions"
        );

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
        let (sequential_ver, sequential_value) =
            stored_ver_and_value(&sequential, key, &kb, bucket);

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
}
