//! A shard with a spill tier attached: eviction onto disk, checkpointed close and warm reopen.

use super::*;

#[cfg(feature = "spill")]
#[tokio::test]
async fn with_spill_lets_eviction_extend_capacity_onto_disk() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-with-spill-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);

    let s = Shard::<u32, String>::new(
        SmolStr::new("test-spill"),
        Mode::Local,
        NodeId::from(1),
        3, // a tiny weight cap: at most 3 one-weight entries fit resident
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("the tier's directory and region files open");

    for k in 0..20u32 {
        s.insert(k, format!("value-{k}")).await.expect("insert");
    }

    assert_eq!(
        s.entry_count().await,
        20,
        "eviction demotes the coldest entries onto disk instead of deleting them once a \
         spill tier is configured, so live_count never shrinks below what was written"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Shard-level counterpart to `engine::tests::spill_payload::io::
/// merge_against_a_spilled_counter_folds_the_stored_side_instead_of_dropping_it`:
/// a merging resolver's stored side spills between two colliding
/// `Shard::merge` calls, and the second still folds against it instead
/// of the outright win/loss a value-less view would produce.
#[cfg(feature = "spill")]
#[tokio::test]
async fn merge_against_a_spilled_counter_folds_instead_of_dropping_a_side() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};

    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-merge-spill-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);

    let s = Shard::<u32, PnCounter>::new(
        SmolStr::new("test-merge-spill"),
        Mode::Local,
        NodeId::from(1),
        1, // a tiny weight cap: the weigher below always exceeds it
        None,
        None,
    )
    .with_resolver(Arc::new(PnCounterResolver))
    .with_weigher(|_key: &u32, _value: &PnCounter| 2)
    .with_spill(&cfg)
    .expect("the tier's directory and region files open");

    let writer_a = crdt::WriterId::new(NodeId::from(11), 1);
    let writer_b = crdt::WriterId::new(NodeId::from(22), 1);

    s.merge(1, PnCounter::local_delta(writer_a, 3))
        .await
        .expect("first increment");

    let spilled = tokio::time::timeout(Duration::from_secs(5), async {
        while s.get_sync(&1).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        spilled,
        "the single over-weight entry spills once eviction runs"
    );

    s.merge(1, PnCounter::local_delta(writer_b, 4))
        .await
        .expect("second increment, colliding with the spilled record");

    assert_eq!(
        s.get(&1).await.map(|c| c.value()),
        Some(7),
        "merging into a spilled record folds both sides' contributions instead of \
         dropping the one that was on disk"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that a warm reopen leaves a reloaded bucket cold until the
/// caller's own reconciliation clears it; the record stays readable meanwhile.
#[cfg(feature = "spill")]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end two-life scenario: splitting it would only scatter state \
              (dir, cfg, view, key, bucket) across helper signatures"
)]
async fn attach_spill_via_a_warm_reopen_leaves_the_reloaded_buckets_cold_until_reconciliation() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-warm-reopen-cold-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);

    let self_node = NodeId::from(201u64);
    let peer = NodeId::from(202u64);
    let owners = NonZeroU8::new(2).expect("nonzero");
    let view = Arc::new(OwnershipView::compute(
        self_node,
        vec![self_node, peer],
        owners,
    ));
    let key = find_key_by_ownership(&view, true);
    let bucket = bucket_of(&key_bytes(&key));

    // First life: a tiny weight cap forces the entry to disk before the
    // checkpointed close.
    let (tracker1, tx1) = OwnershipTracker::seed(
        self_node,
        &[],
        &HashMap::new(),
        &SmolStr::new("warm-cold"),
        owners,
    );
    tx1.send(Arc::clone(&view)).expect("receiver still alive");
    let residency1 = Arc::new(ResidencySet::new());
    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("warm-cold"),
        Mode::Distributed { owners },
        self_node,
        1, // a tiny weight cap: the one entry spills
        None,
        None,
    )
    .with_ownership(tracker1, residency1);
    let warm0 = shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");
    assert!(
        warm0.warm_parts.is_empty(),
        "a brand-new directory has nothing to warm-reopen the first time"
    );

    shard1.insert(key, "v".to_string()).await.expect("insert");
    // Eviction samples within one bucket at a time; filling `key`'s bucket
    // with newer entries concentrates eviction onto `key`, the oldest one there.
    let fillers: Vec<u32> = (0..1_000_000u32)
        .filter(|&k| k != key && bucket_of(&key_bytes(&k)) == bucket)
        .take(20)
        .collect();
    assert!(
        fillers.len() >= 8,
        "at least an eviction sample's worth of same-bucket fillers is found quickly"
    );
    for filler in fillers {
        shard1
            .insert(filler, "filler".to_string())
            .await
            .expect("insert filler");
    }
    let spilled = tokio::time::timeout(Duration::from_secs(5), async {
        while shard1.get_sync(&key).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        spilled,
        "the target key spills to disk once eviction runs past the tiny weight cap"
    );
    shard1.close_spill_checkpointed().await;

    // Second life: same directory, fresh tracker/residency, bucket marked
    // cold up front.
    let (tracker2, tx2) = OwnershipTracker::seed(
        self_node,
        &[],
        &HashMap::new(),
        &SmolStr::new("warm-cold"),
        owners,
    );
    tx2.send(Arc::clone(&view)).expect("receiver still alive");
    let residency2 = Arc::new(ResidencySet::new());
    residency2.mark_cold(&whole(bucket));
    let shard2 = Shard::<u32, String>::new(
        SmolStr::new("warm-cold"),
        Mode::Distributed { owners },
        self_node,
        1,
        None,
        None,
    )
    .with_ownership(tracker2, Arc::clone(&residency2));

    let warm = shard2
        .attach_spill(&cfg)
        .expect("a checkpoint snapshot within tombstone_ttl reopens warm");
    assert_eq!(
        warm.warm_parts
            .iter()
            .map(|part| part.bucket())
            .collect::<HashSet<u16>>(),
        HashSet::from([bucket]),
        "the second life's attach_spill warm-reloads parts of exactly the one bucket the \
         replayed records belong to"
    );
    assert!(warm.warm_parts.contains(&PartId::of_key(&key_bytes(&key))));
    assert!(
        residency2.is_cold(PartId::of_key(&key_bytes(&key))),
        "attach_spill alone never clears cold: only the caller's eager reconciliation does"
    );
    assert_eq!(
        shard2.get(&key).await,
        Some("v".to_string()),
        "the warm-reloaded record is still readable even while its bucket stays marked cold"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that a checkpointed close recovers every live entry, resident or
/// already spilled, and never resurrects a key tombstoned before the close.
#[cfg(feature = "spill")]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end three-key scenario (resident, spilled, tombstoned): splitting \
              it would only scatter state (dir, cfg, node, the three keys) across helper \
              signatures"
)]
async fn a_checkpointed_close_replays_every_live_entry_and_never_a_tombstoned_key() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-checkpoint-replay-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let node = NodeId::from(301u64);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("checkpoint-replay"),
        Mode::Local,
        node,
        2, // room for exactly two resident entries at once
        None,
        None,
    );
    let warm0 = shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");
    assert!(warm0.warm_parts.is_empty());

    let resident_key = 1_000_001u32;
    let spilled_key = 1_000_002u32;
    let tombstoned_key = 1_000_003u32;

    shard1
        .insert(resident_key, "resident-value".to_string())
        .await
        .expect("insert resident");
    shard1
        .insert(spilled_key, "spilled-value".to_string())
        .await
        .expect("insert spilled");

    // Filling `spilled_key`'s bucket concentrates eviction there,
    // leaving `resident_key`'s different bucket untouched.
    let spilled_bucket = bucket_of(&key_bytes(&spilled_key));
    let resident_bucket = bucket_of(&key_bytes(&resident_key));
    assert_ne!(
        spilled_bucket, resident_bucket,
        "the two keys must land in different buckets for this test to isolate eviction \
         pressure onto only one of them"
    );
    let fillers: Vec<u32> = (2_000_000..3_000_000u32)
        .filter(|&k| bucket_of(&key_bytes(&k)) == spilled_bucket)
        .take(20)
        .collect();
    assert!(
        fillers.len() >= 8,
        "at least an eviction sample's worth of same-bucket fillers is found quickly"
    );
    for filler in fillers {
        shard1
            .insert(filler, "filler".to_string())
            .await
            .expect("insert filler");
    }
    let spilled = tokio::time::timeout(Duration::from_secs(5), async {
        while shard1.get_sync(&spilled_key).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        spilled,
        "the target key spills to disk once eviction runs past the weight cap"
    );

    shard1
        .insert(tombstoned_key, "gone".to_string())
        .await
        .expect("insert tombstoned");
    shard1
        .remove(&tombstoned_key)
        .await
        .expect("remove tombstoned");

    shard1.close_spill_checkpointed().await;

    let shard2 = Shard::<u32, String>::new(
        SmolStr::new("checkpoint-replay"),
        Mode::Local,
        node,
        2,
        None,
        None,
    );
    let warm = shard2
        .attach_spill(&cfg)
        .expect("a checkpoint snapshot within tombstone_ttl reopens warm");
    assert!(
        warm.warm_parts
            .iter()
            .any(|part| part.bucket() == resident_bucket),
        "the still-resident key's bucket replayed too, not only the spilled one"
    );
    assert!(
        warm.warm_parts
            .iter()
            .any(|part| part.bucket() == spilled_bucket)
    );

    assert_eq!(
        shard2.get(&resident_key).await,
        Some("resident-value".to_string()),
        "a key never spilled during the first life still survives the checkpoint"
    );
    assert_eq!(
        shard2.get(&spilled_key).await,
        Some("spilled-value".to_string()),
        "a key already spilled when the tier closed survives the checkpoint too"
    );
    assert_eq!(
        shard2.get(&tombstoned_key).await,
        None,
        "a key tombstoned before the close never resurrects via checkpoint replay"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins the simplest resurrection case: a spilled-then-deleted key must
/// never come back on a checkpointed reopen, with no peer or GC involved.
#[cfg(feature = "spill")]
#[tokio::test]
async fn a_deleted_spilled_key_never_resurrects_across_a_checkpointed_reopen() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-no-resurrection-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let node = NodeId::from(302u64);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("no-resurrection"),
        Mode::Local,
        node,
        1, // a tiny weight cap: the one entry spills
        None,
        None,
    );
    shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");

    let key = 42u32;
    shard1.insert(key, "v".to_string()).await.expect("insert");
    let bucket = bucket_of(&key_bytes(&key));
    let fillers: Vec<u32> = (0..1_000_000u32)
        .filter(|&k| k != key && bucket_of(&key_bytes(&k)) == bucket)
        .take(20)
        .collect();
    assert!(fillers.len() >= 8);
    for filler in fillers {
        shard1
            .insert(filler, "filler".to_string())
            .await
            .expect("insert filler");
    }
    let spilled = tokio::time::timeout(Duration::from_secs(5), async {
        while shard1.get_sync(&key).is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        spilled,
        "the key spills to disk once eviction runs past the tiny weight cap"
    );

    // Deleted while still spilled: a delete never rewrites the on-disk
    // region bytes (see `engine::apply_tombstone`), so a scan-based
    // reopen would still find them.
    shard1.remove(&key).await.expect("remove the spilled key");

    shard1.close_spill_checkpointed().await;

    // Reopens well inside the default ten-minute tombstone_ttl gate.
    let shard2 = Shard::<u32, String>::new(
        SmolStr::new("no-resurrection"),
        Mode::Local,
        node,
        1,
        None,
        None,
    );
    // `bucket` still warm-reloads its live filler keys; only the deleted
    // key is checked below.
    let _warm = shard2
        .attach_spill(&cfg)
        .expect("a checkpoint snapshot within tombstone_ttl reopens warm");
    assert_eq!(
        shard2.get(&key).await,
        None,
        "the deleted key never resurrects: no peer, no anti-entropy round, no tombstone \
         GC, just replay from the checkpoint snapshot alone"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins the exact race `Engine::finalize_checkpoint_writes` closes: a
/// key deleted between resident-capture and disk-write must not survive
/// the checkpoint.
#[cfg(feature = "spill")]
#[tokio::test]
async fn close_spill_checkpoint_drops_a_key_deleted_between_capture_and_disk_write() {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-close-spill-race-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let node = NodeId::from(303u64);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("close-spill-race"),
        Mode::Local,
        node,
        1 << 20, // generous cap: this test drives the checkpoint's own
        // engine methods directly, not a real eviction
        None,
        None,
    );
    shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");

    let key = 77u32;
    shard1.insert(key, "v".to_string()).await.expect("insert");

    let now = shard1.now_ms();
    // Phase 1: capture every resident live entry before anything else changes.
    let resident = shard1.engine.checkpoint_resident_entries(now);
    assert_eq!(resident.len(), 1, "the one resident key is captured");

    // The race: a second clone deletes the key between capture and the write below.
    shard1
        .remove(&key)
        .await
        .expect("remove races the checkpoint");

    let tier = Arc::clone(shard1.engine.spill().expect("attach_spill attaches a tier"));
    let sink = Arc::clone(&shard1.engine) as Arc<dyn spill::SpillSink>;
    // Phase 2: writes the now-stale bytes to disk regardless of the race.
    let flush_outcome = tier.checkpoint_flush(sink.as_ref(), resident);
    let newly_written = flush_outcome.written;
    assert_eq!(
        newly_written.len(),
        1,
        "checkpoint_flush writes the captured entry to disk with no version check, \
         exactly as its own docs describe -- this is the vulnerable write the next phase \
         must correct for"
    );

    // Phase 3, the fix: re-validates against live state and drops the
    // now-tombstoned entry.
    let (survivors, finalize_counts) = shard1.engine.finalize_checkpoint_writes(newly_written, now);
    assert!(
        survivors.is_empty(),
        "a key deleted between the capture and the checkpoint write must never survive \
         finalize_checkpoint_writes"
    );
    assert_eq!(
        finalize_counts.dropped_tombstoned, 1,
        "the race drops the entry under the tombstoned reason specifically"
    );

    let mut entries = shard1.engine.snapshot_spilled(now);
    entries.extend(survivors);
    assert!(
        entries.is_empty(),
        "the deleted key must be absent from the final checkpoint snapshot"
    );

    tier.write_checkpoint_snapshot(&entries, now);
    tier.close();

    let shard2 = Shard::<u32, String>::new(
        SmolStr::new("close-spill-race"),
        Mode::Local,
        node,
        1 << 20,
        None,
        None,
    );
    shard2
        .attach_spill(&cfg)
        .expect("a checkpoint snapshot within tombstone_ttl reopens warm");
    assert_eq!(
        shard2.get(&key).await,
        None,
        "the key deleted mid-checkpoint never resurrects on reopen"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that `finalize_checkpoint_writes` calls `note_spill_arrival`
/// itself, since it flips `Resident` to `Spilled` without going through
/// `SpillSink::install`.
#[cfg(feature = "spill")]
#[tokio::test]
async fn close_spill_checkpoint_counts_a_surviving_resident_entry_toward_the_spill_entries_metric()
{
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-checkpoint-gauge-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let node = NodeId::from(304u64);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new("checkpoint-gauge"),
        Mode::Local,
        node,
        1 << 20, // generous: nothing is ever evicted by ordinary means
        None,
        None,
    )
    .with_max_resident_bytes(1 << 40);
    shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");
    shard1.insert(1u32, "v".repeat(40)).await.expect("insert");
    assert_eq!(
        shard1.engine.debug_spill_entries_count(),
        0,
        "a resident entry never counts toward sundog_spill_entries before any checkpoint"
    );
    let resident_before = shard1.resident_bytes().expect("a capped shard counts");

    shard1.close_spill_checkpointed().await;
    assert!(
        shard1.resident_bytes() < Some(resident_before),
        "the checkpoint's flip to Spilled drops the value's heap bytes"
    );
    assert_eq!(
        shard1.resident_bytes(),
        Some(shard1.engine.recount_resident_bytes())
    );

    assert_eq!(
        shard1.engine.debug_spill_entries_count(),
        1,
        "close_spill_checkpointed's checkpoint flips the one resident entry to Spilled via \
         Engine::finalize_checkpoint_writes, which must call note_spill_arrival itself \
         since it never goes through SpillSink::install"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Captures `sundog_spill_reopen_total{outcome,reason}` increments for one
/// cache name.
#[cfg(feature = "spill")]
#[derive(Clone, Default)]
struct ReopenCounts(Arc<StdMutex<HashMap<(String, String), u64>>>);

#[cfg(feature = "spill")]
impl ReopenCounts {
    fn get(&self, outcome: &str, reason: &str) -> u64 {
        *self
            .0
            .lock()
            .unwrap()
            .get(&(outcome.to_string(), reason.to_string()))
            .unwrap_or(&0)
    }
}

#[cfg(feature = "spill")]
struct ReopenOutcomeCounter {
    outcome_reason: (String, String),
    counts: ReopenCounts,
}

#[cfg(feature = "spill")]
impl metrics::CounterFn for ReopenOutcomeCounter {
    fn increment(&self, value: u64) {
        *self
            .counts
            .0
            .lock()
            .unwrap()
            .entry(self.outcome_reason.clone())
            .or_insert(0) += value;
    }

    fn absolute(&self, value: u64) {
        *self
            .counts
            .0
            .lock()
            .unwrap()
            .entry(self.outcome_reason.clone())
            .or_insert(0) = value;
    }
}

#[cfg(feature = "spill")]
struct ReopenRecorder {
    cache: String,
    counts: ReopenCounts,
}

#[cfg(feature = "spill")]
impl metrics::Recorder for ReopenRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        let this_cache = key
            .labels()
            .any(|l| l.key() == "cache" && l.value() == self.cache);
        if key.name() != "sundog_spill_reopen_total" || !this_cache {
            return metrics::Counter::noop();
        }
        let outcome = key
            .labels()
            .find(|l| l.key() == "outcome")
            .map(|l| l.value().to_string())
            .unwrap_or_default();
        let reason = key
            .labels()
            .find(|l| l.key() == "reason")
            .map(|l| l.value().to_string())
            .unwrap_or_default();
        metrics::Counter::from_arc(Arc::new(ReopenOutcomeCounter {
            outcome_reason: (outcome, reason),
            counts: self.counts.clone(),
        }))
    }

    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// Pins that `attach_spill` records a `stale_snapshot` cold fallback
/// under `sundog_spill_reopen_total`, the one reopen reason the exporter
/// test cannot reach.
#[cfg(feature = "spill")]
#[test]
fn attach_spill_records_a_stale_snapshot_cold_fallback_under_its_metric() {
    const CACHE: &str = "attach-spill-stale-snapshot-metric";
    let counts = ReopenCounts::default();
    // The global recorder slot is process-wide; if another test already
    // won it, this one observes nothing and skips its assertions.
    let installed = metrics::set_global_recorder(ReopenRecorder {
        cache: CACHE.to_string(),
        counts: counts.clone(),
    })
    .is_ok();

    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-stale-snapshot-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let region_bytes = 4096u64;
    let capacity_bytes = 1u64 << 20;
    let cfg = spill::SpillConfig::new(&dir, capacity_bytes)
        .region_bytes(region_bytes)
        .warm_reopen(true);
    let cache_dir = dir.join(CACHE);
    std::fs::create_dir_all(&cache_dir).expect("scratch tier directory creates");
    let region_count = spill::region_count_for(capacity_bytes, region_bytes).max(2);
    spill::write_stale_snapshot_for_test(&cache_dir, region_bytes, region_count, 1_000);

    let s = Shard::<u32, String>::new(
        SmolStr::new(CACHE),
        Mode::Local,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    let warm = s
        .attach_spill(&cfg)
        .expect("the cold fallback still succeeds even against a stale snapshot");
    assert!(
        warm.warm_parts.is_empty(),
        "a stale snapshot never reopens warm, so nothing is in warm_parts"
    );

    if installed {
        assert_eq!(
            counts.get("cold_fallback", "stale_snapshot"),
            1,
            "sundog_spill_reopen_total{{outcome=\"cold_fallback\", \
             reason=\"stale_snapshot\"}} records the stale-snapshot cold fallback"
        );
        assert_eq!(
            counts.get("warm", ""),
            0,
            "a stale snapshot never records a warm reopen"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pins that reattaching with no clean close in between (a crash) falls
/// back cold with reason `"no_snapshot"`.
#[cfg(feature = "spill")]
#[test]
fn attach_spill_twice_with_no_close_between_falls_back_cold_with_reason_no_snapshot() {
    const CACHE: &str = "attach-spill-twice-no-close";
    let counts = ReopenCounts::default();
    // The global recorder slot is process-wide; if already won, this
    // test skips its assertions.
    let installed = metrics::set_global_recorder(ReopenRecorder {
        cache: CACHE.to_string(),
        counts: counts.clone(),
    })
    .is_ok();

    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-twice-no-close-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);

    let shard1 = Shard::<u32, String>::new(
        SmolStr::new(CACHE),
        Mode::Local,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    let warm1 = shard1
        .attach_spill(&cfg)
        .expect("a brand-new directory opens cold");
    assert!(warm1.warm_parts.is_empty());
    // Life 1 ends with no checkpointed close: the tier drops, writing no
    // snapshot.
    drop(shard1);

    let shard2 = Shard::<u32, String>::new(
        SmolStr::new(CACHE),
        Mode::Local,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    let warm2 = shard2
        .attach_spill(&cfg)
        .expect("the cold fallback still succeeds against a directory with no snapshot");
    assert!(
        warm2.warm_parts.is_empty(),
        "no snapshot exists to warm-reopen from"
    );

    if installed {
        assert_eq!(
            counts.get("cold_fallback", "no_snapshot"),
            2,
            "sundog_spill_reopen_total{{outcome=\"cold_fallback\", \
             reason=\"no_snapshot\"}} records both lives' cold fallback: the first life's \
             directory starts out with no snapshot exactly as much as the second life's \
             does after a close with no checkpoint"
        );
        assert_eq!(counts.get("warm", ""), 0, "no snapshot ever reopens warm");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
