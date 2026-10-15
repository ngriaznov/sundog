//! Tests for a tombstone arriving over a spilled holder: one a peer sends
//! past its retention is ignored without reading the spilled bytes, and
//! leaves the spilled older value in place.

use super::*;

/// An engine over a real spill tier holding key 1 as the spilled value "old"
/// at `hlc(500, 2)`. Returns the directory to remove, the tier, the engine
/// and the key's bytes.
fn spilled_holder(
    label: &str,
) -> (
    std::path::PathBuf,
    Arc<SpillTier>,
    Arc<Engine<u32, String>>,
    Bytes,
) {
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let kb = key_bytes(1);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    let _ = put(
        &engine,
        1,
        kb.clone(),
        "old".to_owned(),
        hlc(500, 2),
        None,
        1_000,
    );
    let _ = engine.evict_one_sampled(bucket, 1_000);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );
    engine.gc_tombstones(false, 0);
    (dir, tier, engine, kb)
}

/// Applies a delete of key 1 stamped `hlc(1_000, 1)` at `now_ms` as `arrival`,
/// with a 60 s retention and a 600 s cap.
fn apply_delete(
    engine: &Engine<u32, String>,
    kb: &Bytes,
    arrival: Arrival,
    now_ms: u64,
) -> (ApplyOutcome<u32, String>, u64) {
    let hash = hash_key_bytes(kb.as_ref());
    let mut written = Written::default();
    let outcome = engine
        .apply_many_with_reservation(
            arrival,
            stripe_index_from_hash(hash),
            vec![(hash, 1, kb.clone(), hlc(1_000, 1), Incoming::DELETE)],
            // Reads the stored side's value bytes, as a value-aware resolver
            // does, so a read of the spilled holder shows in the tier's count.
            &RecordingResolver::default(),
            60_000,
            600_000,
            now_ms,
            None,
            &mut written,
        )
        .0
        .remove(0);
    (outcome, written.ignored_tombstones())
}

#[test]
fn a_lapsed_peer_delete_reads_nothing_from_the_tier_and_leaves_the_spilled_value() {
    let (dir, tier, engine, kb) = spilled_holder("lapsed-delete-spilled");
    let reads = tier.read_calls();
    let (outcome, ignored) = apply_delete(&engine, &kb, Arrival::Peer, 700_000);

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(ignored, 1);
    assert_eq!(tier.read_calls(), reads, "nothing is read off disk");
    assert!(is_spilled(&engine, &kb), "the older value stays spilled");
    assert_eq!(engine.tombstone_count(), 0);
    assert_eq!(
        engine.inspect(kb.as_ref(), 700_000),
        LocalRecord::Live {
            version: hlc(500, 2),
            expires_at_ms: None,
            spilled: true
        },
        "and is still the live entry"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let _ = std::fs::remove_dir_all(&dir);
}

/// The control for the test above: a delete within its retention does read
/// the spilled holder, and deletes it, so the read counter that test pins
/// at zero moves when a read happens.
#[test]
fn a_peer_delete_within_its_retention_over_a_spilled_value_reads_the_tier_and_deletes_it() {
    let (dir, tier, engine, kb) = spilled_holder("fresh-delete-spilled");
    let reads = tier.read_calls();
    let (outcome, ignored) = apply_delete(&engine, &kb, Arrival::Peer, 30_000);

    assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
    assert_eq!(ignored, 0);
    assert!(tier.read_calls() > reads, "the stored side is read");
    assert_eq!(
        engine.inspect(kb.as_ref(), 30_000),
        LocalRecord::Tombstone {
            version: hlc(1_000, 1)
        }
    );
    assert_eq!(engine.tombstone_count(), 1);
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_local_delete_over_a_spilled_value_is_stored_however_late_it_applies() {
    let (dir, _tier, engine, kb) = spilled_holder("local-delete-spilled");
    let (outcome, ignored) = apply_delete(&engine, &kb, Arrival::Local, 700_000);

    assert!(matches!(outcome, ApplyOutcome::Tombstoned { .. }));
    assert_eq!(ignored, 0);
    assert_eq!(
        engine.inspect(kb.as_ref(), 700_000),
        LocalRecord::Tombstone {
            version: hlc(1_000, 1)
        }
    );
    assert_eq!(engine.tombstone_count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}
