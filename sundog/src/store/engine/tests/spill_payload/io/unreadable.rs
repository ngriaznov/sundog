//! Tests for a conflict resolution over a spilled stored side whose record no read can use: the conflict read tells such a record from a value and from a read a later one can get past, and the apply drops the entry under its write lock, so the incoming write meets no stored side.

use std::path::Path;

use super::super::super::super::apply::{ApplyScope, read_spilled_for_conflict};
use super::*;

/// A resolver that reads both records' values, as a value-aware one does,
/// and keeps the greater version.
struct ReadsValues;

impl ConflictResolver for ReadsValues {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }
}

/// An engine over a fresh tier in `dir`, holding key 1 as `value` spilled at
/// `hlc(5, 1)`, with its tier, the key's bytes and the entry's pointer.
fn spilled_at_five(
    dir: &Path,
    label: &str,
    value: &str,
) -> (Arc<Engine<u32, String>>, Arc<SpillTier>, Bytes, SpillLoc) {
    spilled_at_five_expiring(dir, label, value, None)
}

/// [`spilled_at_five`] with the entry expiring at `expires_at_ms`.
fn spilled_at_five_expiring(
    dir: &Path,
    label: &str,
    value: &str,
    expires_at_ms: Option<u64>,
) -> (Arc<Engine<u32, String>>, Arc<SpillTier>, Bytes, SpillLoc) {
    let (engine, tier) = engine_over_a_tier::<String>(dir, label);
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let _ = put(
        &engine,
        1,
        kb.clone(),
        value.to_owned(),
        hlc(5, 1),
        expires_at_ms,
        0,
    );
    let _ = engine.evict_one_sampled(stripe_index_from_hash(hash), 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );
    let (_, loc) = engine
        .spilled_loc(kb.as_ref(), hash, 0)
        .expect("a spilled entry");
    (engine, tier, kb, loc)
}

/// An engine with no capacity bound over a fresh tier in `dir`, and the tier.
fn engine_over_a_tier<V>(dir: &Path, label: &str) -> (Arc<Engine<u32, V>>, Arc<SpillTier>)
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let cfg = SpillConfig::new(dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
    let engine = Engine::<u32, V>::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));
    (engine, tier)
}

/// Flips one byte of `needle`, a value's encoding, in the one region file
/// under `dir` that holds it, leaving every length field intact, so the
/// record still frames and only its checksum fails.
fn damage(dir: &Path, needle: &[u8]) {
    let mut flipped = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).expect("read dir").flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let mut bytes = std::fs::read(&p).expect("read file");
            if let Some(at) = bytes.windows(needle.len()).position(|w| w == needle) {
                bytes[at] ^= 0x01;
                std::fs::write(&p, &bytes).expect("write file");
                flipped += 1;
            }
        }
    }
    assert_eq!(flipped, 1, "exactly one file holds the spilled value");
}

/// What every drop of the one entry leaves behind: `live` entries counted,
/// none spilled, digests that match a recompute, and `tombstones` tombstones.
fn assert_dropped(engine: &Engine<u32, String>, live: u64, tombstones: u64) {
    assert_eq!(engine.debug_totals().0, live, "the live count");
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "the spill gauge counts the dropped entry out"
    );
    assert_eq!(
        engine.digests(),
        engine.recompute_digests_paired(),
        "the incremental digest matches a full recompute"
    );
    assert_eq!(engine.tombstone_count(), tombstones, "the tombstones");
}

#[test]
fn read_spilled_for_conflict_tells_a_value_from_a_record_no_read_can_use() {
    let dir = temp_dir("conflict-read-outcomes");
    let (_engine, tier, _kb, loc) = spilled_at_five(&dir, "conflict-read-outcomes", "stored-value");
    let encoded = Bytes::from(postcard::to_stdvec("stored-value").expect("encodes"));

    assert_eq!(
        read_spilled_for_conflict::<String>(&tier, loc),
        Ok(Some(encoded)),
        "an intact record's value"
    );
    assert_eq!(
        read_spilled_for_conflict::<bool>(&tier, loc),
        Err(Unreadable::Undecodable),
        "bytes that do not decode as the value type cannot be used"
    );
    let rotated = SpillLoc {
        generation: loc.generation + 1,
        ..loc
    };
    assert_eq!(
        read_spilled_for_conflict::<String>(&tier, rotated),
        Ok(None),
        "a pointer its region rotated past holds nothing to fold"
    );
    tier.fail_reads(true);
    assert_eq!(
        read_spilled_for_conflict::<String>(&tier, loc),
        Ok(None),
        "an I/O error leaves the record for a later read"
    );
    tier.fail_reads(false);
    damage(&dir, b"stored-value");
    assert_eq!(
        read_spilled_for_conflict::<String>(&tier, loc),
        Err(Unreadable::Damaged),
        "a record that fails its checksum cannot be used"
    );

    tier.close();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A write below the version of a spilled entry whose record fails its
/// checksum lands: the apply drops the entry, which would otherwise win the
/// collision with no value to show, and the write meets no stored side.
#[test]
fn a_write_over_a_spilled_record_that_fails_its_checksum_drops_it_and_lands() {
    let dir = temp_dir("conflict-drop-put");
    let (engine, tier, kb, _loc) = spilled_at_five(&dir, "conflict-drop-put", "stored-value");
    damage(&dir, b"stored-value");
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));

    let outcomes = engine.apply_many(
        Arrival::Peer,
        bucket,
        vec![put_entry(1, "older", hlc(3, 2), None)],
        &ReadsValues,
        60_000,
        600_000,
        0,
    );
    assert!(
        matches!(
            outcomes.as_slice(),
            [ApplyOutcome::Put { created: true, .. }]
        ),
        "the write creates the key"
    );
    assert_eq!(engine.get(&1, 0), Some("older".to_owned()));
    assert_dropped(&engine, 1, 0);

    tier.close();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A tombstone below the version of a spilled entry whose record fails its
/// checksum is stored over the dropped entry.
#[test]
fn a_tombstone_over_a_spilled_record_that_fails_its_checksum_drops_it_and_lands() {
    let dir = temp_dir("conflict-drop-tombstone");
    let (engine, tier, kb, _loc) = spilled_at_five(&dir, "conflict-drop-tombstone", "stored-value");
    damage(&dir, b"stored-value");
    let hash = hash_key_bytes(kb.as_ref());

    let outcomes = engine.apply_many(
        Arrival::Peer,
        stripe_index_from_hash(hash),
        vec![(
            hash,
            1,
            kb.clone(),
            hlc(3, 2),
            Incoming::Tombstone {
                expired_at_ms: None,
            },
        )],
        &ReadsValues,
        60_000,
        600_000,
        0,
    );
    assert!(
        matches!(outcomes.as_slice(), [ApplyOutcome::Tombstoned { .. }]),
        "the tombstone is stored"
    );
    assert_eq!(engine.get(&1, 0), None);
    assert_dropped(&engine, 0, 1);

    tier.close();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A held-only write over a spilled entry whose record fails its checksum
/// folds into nothing and is refused, and the entry is dropped all the same.
#[test]
fn a_held_only_write_over_a_spilled_record_that_fails_its_checksum_drops_it() {
    let dir = temp_dir("conflict-drop-held-only");
    let (engine, tier, kb, _loc) = spilled_at_five(&dir, "conflict-drop-held-only", "stored-value");
    damage(&dir, b"stored-value");
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));

    let (outcomes, _) = engine.apply_many_in(
        ApplyScope::HeldOnly,
        Arrival::Peer,
        bucket,
        vec![put_entry(1, "newer", hlc(7, 2), None)],
        &ReadsValues,
        60_000,
        600_000,
        0,
        None,
        &mut Written::default(),
    );
    assert!(
        matches!(outcomes.as_slice(), [ApplyOutcome::Rejected]),
        "a held-only write has no held entry to fold into"
    );
    assert_eq!(engine.get(&1, 0), None);
    assert_dropped(&engine, 0, 0);

    tier.close();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A spilled entry that has already expired keeps its version when its record
/// fails its checksum: a value-reading resolver that keeps the greater
/// version still refuses an older write over it, and the entry stays for the
/// sweep, which tombstones an expired entry at its version where the mode
/// asks for one.
#[test]
fn an_older_write_over_an_expired_spilled_record_that_fails_its_checksum_is_refused() {
    let dir = temp_dir("conflict-expired-damaged");
    let (engine, tier, kb, _loc) =
        spilled_at_five_expiring(&dir, "conflict-expired-damaged", "stored-value", Some(100));
    damage(&dir, b"stored-value");
    let digests = engine.digests();

    let outcomes = engine.apply_many(
        Arrival::Peer,
        stripe_index_from_hash(hash_key_bytes(kb.as_ref())),
        vec![put_entry(1, "older", hlc(3, 2), None)],
        &ReadsValues,
        60_000,
        600_000,
        500,
    );
    assert!(
        matches!(outcomes.as_slice(), [ApplyOutcome::Rejected]),
        "the expired entry's version still orders the write"
    );
    assert_eq!(engine.debug_totals().0, 1, "the entry stays for the sweep");
    assert_eq!(engine.debug_spill_entries_count(), 1);
    assert_eq!(engine.digests(), digests);

    tier.close();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A batch of a counter's deltas below the version of a spilled counter
/// whose record fails its checksum folds as it would over no stored side:
/// the pre-fold finds no bytes for the stored side, so the run applies one
/// write at a time, and the first drops the entry.
#[test]
fn a_merged_batch_over_a_spilled_counter_that_fails_its_checksum_starts_a_new_value() {
    use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};

    let dir = temp_dir("conflict-drop-merge");
    let (engine, tier) = engine_over_a_tier::<PnCounter>(&dir, "conflict-drop-merge");
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let stored = PnCounter::local_delta(WriterId::new(NodeId::from(0x5EED_5EED_5EED_5EED), 1), 3);
    let _ = put_with_resolver(
        &engine,
        1,
        kb.clone(),
        stored.clone(),
        hlc(5, 1),
        None,
        0,
        &PnCounterResolver,
    );
    let _ = engine.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );
    damage(&dir, &postcard::to_stdvec(&stored).expect("encodes"));

    let writer = WriterId::new(NodeId::from(12), 1);
    let delta = |cumulative: u64, ver: Hlc| {
        let value = PnCounter::local_delta(writer, cumulative);
        let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encodes"));
        (
            hash,
            1,
            kb.clone(),
            ver,
            Incoming::Put {
                value,
                expires_at_ms: None,
                encoded,
            },
        )
    };
    let outcomes = engine.apply_many(
        Arrival::Peer,
        bucket,
        vec![delta(2, hlc(2, 2)), delta(5, hlc(3, 2))],
        &PnCounterResolver,
        60_000,
        600_000,
        0,
    );
    assert!(
        matches!(
            outcomes.as_slice(),
            [
                ApplyOutcome::Put { created: true, .. },
                ApplyOutcome::Put { created: false, .. }
            ]
        ),
        "the first delta creates the key and the second folds into it"
    );
    assert_eq!(
        engine.get(&1, 0).map(|counter| counter.value()),
        Some(5),
        "the counter holds the batch's deltas alone"
    );
    assert_eq!(engine.debug_totals().0, 1);
    assert_eq!(
        engine.debug_spill_entries_count(),
        0,
        "the spill gauge counts the dropped entry out"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());

    tier.close();
    let _ = std::fs::remove_dir_all(&dir);
}
