//! Tests for the held-only batch apply over a spilled holder: a peer's merged record folds into a spilled counter that is visible, and is rejected for one that has expired.

use super::super::super::super::apply::ApplyScope;
use super::*;
use crate::store::crdt::{PnCounter, PnCounterResolver, WriterId};

/// An engine over a real spill tier holding key 1 as a spilled counter of 3
/// from writer 11, expiring at `expires_at_ms`. Returns the directory to
/// remove, the engine and the key's bytes.
fn spilled_holder(
    label: &str,
    expires_at_ms: Option<u64>,
) -> (
    std::path::PathBuf,
    Arc<Engine<u32, PnCounter>>,
    Bytes,
    usize,
) {
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = Arc::new(SpillTier::open(&cfg, label).expect("tier opens"));
    let engine = Engine::<u32, PnCounter>::new(u64::MAX, None, None, None);
    engine.set_spill(Arc::clone(&tier));
    let engine = Arc::new(engine);
    tier.attach(Arc::downgrade(&(Arc::clone(&engine) as Arc<dyn SpillSink>)));

    let kb = key_bytes(1);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    let _ = put_with_resolver(
        &engine,
        1,
        kb.clone(),
        PnCounter::local_delta(WriterId::new(NodeId::from(11), 1), 3),
        hlc(1, 1),
        expires_at_ms,
        0,
        &PnCounterResolver,
    );
    let _ = engine.evict_one_sampled(bucket, 0);
    assert!(
        poll_until(POLL_TIMEOUT, || is_spilled(&engine, &kb)),
        "the flusher installs the spilled entry"
    );
    (dir, engine, kb, bucket)
}

/// The held-only apply of a peer's record of 4 from writer 22 for key 1,
/// stamped `ver`, at `now_ms`.
fn apply_peer_record(
    engine: &Engine<u32, PnCounter>,
    kb: &Bytes,
    bucket: usize,
    ver: Hlc,
    now_ms: u64,
) -> ApplyOutcome<u32, PnCounter> {
    let value = PnCounter::local_delta(WriterId::new(NodeId::from(22), 1), 4);
    let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encodes"));
    engine
        .apply_many_in(
            ApplyScope::HeldOnly,
            bucket,
            vec![(
                hash_key_bytes(kb.as_ref()),
                1,
                kb.clone(),
                ver,
                Incoming::Put {
                    value,
                    expires_at_ms: None,
                    encoded,
                },
            )],
            &PnCounterResolver,
            60_000,
            600_000,
            now_ms,
            None,
            &mut Written::default(),
        )
        .0
        .remove(0)
}

#[test]
fn a_spilled_held_counter_folds_a_peers_record() {
    let (dir, engine, kb, bucket) = spilled_holder("held-only-spilled-fold", None);

    let outcome = apply_peer_record(&engine, &kb, bucket, hlc(5, 2), 10);

    assert!(
        matches!(outcome, ApplyOutcome::Put { created: false, .. }),
        "a fold over a held spilled counter is an update"
    );
    assert_eq!(
        engine.get(&1, 10).map(|c| c.value()),
        Some(7),
        "both contributions are in the folded counter"
    );
    assert_eq!(engine.digests(), engine.recompute_digests_paired());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_expired_spilled_counter_rejects_a_peers_record_and_stays_spilled() {
    let (dir, engine, kb, bucket) = spilled_holder("held-only-spilled-expired", Some(100));
    let digests = engine.digests();

    let outcome = apply_peer_record(&engine, &kb, bucket, hlc(50, 2), 500);

    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert!(is_spilled(&engine, &kb), "nothing is stored over it");
    assert_eq!(engine.digests(), digests);

    let _ = std::fs::remove_dir_all(&dir);
}
