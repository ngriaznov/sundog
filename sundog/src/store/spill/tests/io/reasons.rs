//! Tests for the documented drop reasons and the keep-resident refusal policy.

use super::*;

#[test]
fn write_segment_failure_abandons_every_job_in_the_failed_segment() {
    // One region reopened read-only, so writing fails for every
    // job; DROP_REASONS_CACHE filters this tier's drops.
    let counts = DropCounts::default();
    let installed = metrics::set_global_recorder(DropRecorder {
        counts: counts.clone(),
    })
    .is_ok();

    let dir = temp_dir("write-fails");
    let record_len = HEADER_LEN as u64 + 6 + 4; // fixed-width key/value
    let region_bytes = record_len * 4;
    let cfg = SpillConfig::new(&dir, region_bytes * 2).region_bytes(region_bytes);
    let tier =
        SpillTier::open_with_readonly_regions_for_test(&cfg, DROP_REASONS_CACHE, &[0]).unwrap();
    let sink = Arc::new(RecordingSink::default());

    let jobs: Vec<SpillJob> = (0..3u32)
        .map(|i| job(&format!("key-{i:02}"), b"1234", hlc(u64::from(i) + 1, 0)))
        .collect();
    flush_batch(&tier.inner, &*sink, jobs);

    assert_eq!(
        sink.install_count(),
        0,
        "the failed write never installs anything"
    );
    assert_eq!(
        sink.abandon_count(),
        3,
        "every job in the failed segment reaches abandon, not just the first"
    );
    if installed {
        assert_eq!(
            counts.get("disk_error"),
            3,
            "every job in the failed segment is also counted under \
             sundog_spill_dropped_total{{reason=\"disk_error\"}}, even though \
             abandon's restore-to-resident behavior above is unchanged"
        );
    }

    let _ = fs::remove_dir_all(&dir);
}

/// A [`SpillSink`] whose `install` always rejects the flush, so
/// `flush_one` counts `sundog_spill_dropped_total{reason="obsolete"}`
/// instead of `sundog_spill_writes_total`.
struct RejectingSink;

impl SpillSink for RejectingSink {
    fn install(&self, _: usize, _: &Bytes, _: u64, _: Hlc, _: SpillLoc, _: u32) -> bool {
        false
    }

    #[allow(clippy::too_many_arguments)]
    fn install_new(
        &self,
        _: usize,
        _: &Bytes,
        _: u64,
        _: Hlc,
        _: Option<u64>,
        _: SpillLoc,
        _: u32,
    ) -> bool {
        false
    }

    fn reclaim(&self, _: u32, _: u32, _: &[ReclaimRef]) -> usize {
        0
    }

    fn abandon(&self, _: usize, _: &Bytes, _: u64, _: Hlc, _: u32) {}
}

/// Captures `sundog_spill_dropped_total{reason}` increments by
/// `reason`, ignoring every other metric.
#[derive(Clone, Default)]
struct DropCounts(Arc<StdMutex<StdHashMap<String, u64>>>);

impl DropCounts {
    fn get(&self, reason: &str) -> u64 {
        *self.0.lock().unwrap().get(reason).unwrap_or(&0)
    }
}

struct ReasonCounter {
    reason: String,
    counts: DropCounts,
}

impl metrics::CounterFn for ReasonCounter {
    fn increment(&self, value: u64) {
        *self
            .counts
            .0
            .lock()
            .unwrap()
            .entry(self.reason.clone())
            .or_insert(0) += value;
    }

    fn absolute(&self, value: u64) {
        *self
            .counts
            .0
            .lock()
            .unwrap()
            .entry(self.reason.clone())
            .or_insert(0) = value;
    }
}

struct DropRecorder {
    counts: DropCounts,
}

impl metrics::Recorder for DropRecorder {
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
            .any(|l| l.key() == "cache" && l.value() == DROP_REASONS_CACHE);
        if key.name() != "sundog_spill_dropped_total" || !this_cache {
            return metrics::Counter::noop();
        }
        let reason = key
            .labels()
            .find(|l| l.key() == "reason")
            .map(|l| l.value().to_string())
            .unwrap_or_default();
        metrics::Counter::from_arc(Arc::new(ReasonCounter {
            reason,
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

/// The cache name only this test opens, so the process-global
/// recorder ignores drops from every other test's tier.
const DROP_REASONS_CACHE: &str = "drop-reasons-only";

#[test]
fn try_spill_and_flush_record_the_documented_drop_reason_for_each_case() {
    let counts = DropCounts::default();
    // `metrics::set_global_recorder` is a single process-global
    // slot: if another test in this binary already won it, this one
    // silently observes nothing and skips its assertions, the same
    // way `tests/prometheus_exporter.rs`'s own tests tolerate
    // losing that race rather than assuming they run first.
    let installed = metrics::set_global_recorder(DropRecorder {
        counts: counts.clone(),
    })
    .is_ok();
    if !installed {
        return;
    }

    // too_large: a record too big for the tier's own region size.
    let dir = temp_dir("reasons-too-large");
    let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
    let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
    let sink: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&sink));
    assert!(!tier.try_spill(job("k", &[0u8; 100], hlc(1, 0))));
    assert_eq!(counts.get("too_large"), 1);
    assert_eq!(counts.get("closed"), 0);
    let _ = fs::remove_dir_all(&dir);

    // closed: try_spill after SpillTier::close.
    let dir = temp_dir("reasons-closed");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
    let sink: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&sink));
    tier.close();
    assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));
    assert_eq!(counts.get("closed"), 1);
    let _ = fs::remove_dir_all(&dir);

    // queue_full: never attached, so there is no channel to send on.
    let dir = temp_dir("reasons-queue-full");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
    assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));
    assert_eq!(counts.get("queue_full"), 1);
    let _ = fs::remove_dir_all(&dir);

    // obsolete: the flusher writes the record, but the sink rejects
    // installing it.
    let dir = temp_dir("reasons-obsolete");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
    let sink: Arc<dyn SpillSink> = Arc::new(RejectingSink);
    tier.attach(Arc::downgrade(&sink));
    assert!(tier.try_spill(job("k", b"v", hlc(1, 0))));
    assert!(poll_until(POLL_TIMEOUT, || counts.get("obsolete") == 1));
    let _ = fs::remove_dir_all(&dir);

    // deferred: the same three refusals, but recorded under
    // "deferred" instead once `set_keep_resident_when_refused(true)`
    // is in effect, and the specific reason's own count stays put.
    let dir = temp_dir("reasons-deferred-too-large");
    let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
    let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
    tier.set_keep_resident_when_refused(true);
    let sink: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&sink));
    assert!(!tier.try_spill(job("k", &[0u8; 100], hlc(1, 0))));
    assert_eq!(counts.get("deferred"), 1);
    assert_eq!(
        counts.get("too_large"),
        1,
        "unchanged from the earlier case"
    );
    let _ = fs::remove_dir_all(&dir);

    let dir = temp_dir("reasons-deferred-closed");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
    tier.set_keep_resident_when_refused(true);
    let sink: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&sink));
    tier.close();
    assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));
    assert_eq!(counts.get("deferred"), 2);
    assert_eq!(counts.get("closed"), 1, "unchanged from the earlier case");
    let _ = fs::remove_dir_all(&dir);

    let dir = temp_dir("reasons-deferred-queue-full");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, DROP_REASONS_CACHE).unwrap();
    tier.set_keep_resident_when_refused(true);
    assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));
    assert_eq!(counts.get("deferred"), 3);
    assert_eq!(
        counts.get("queue_full"),
        1,
        "unchanged from the earlier case"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A keep-resident pass records every refusal as `deferred` on a
/// tier that deletes, since nothing is dropped; the tier's own
/// policy records the specific reason, and `would_accept` is that
/// policy.
#[test]
fn would_accept_under_records_deferred_for_a_keep_resident_pass_on_a_tier_that_deletes() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let counts = |cache: &str, pairs: &[(&str, u64)]| {
        assert_eq!(
            dropped_since_last_snapshot(&snapshotter, cache),
            pairs
                .iter()
                .map(|&(reason, n)| (reason.to_string(), n))
                .collect::<std::collections::BTreeMap<_, _>>()
        );
    };

    // A queue that holds one record, taken by an admission that is
    // never enqueued.
    let dir = temp_dir("under-queue-full-closed");
    let record_len = HEADER_LEN as u64 + 4 + 4;
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, "under-a").unwrap();
    assert!(!tier.keep_resident_when_refused());
    assert_eq!(tier.would_accept(None, 4, 4), Admission::Accepted);
    counts("under-a", &[]);
    let under = |on_refusal| tier.would_accept_under(on_refusal, None, 4, 4);
    assert_eq!(under(OnRefusal::KeepResident), Admission::RefusedFinal);
    counts("under-a", &[("deferred", 1)]);
    assert_eq!(under(OnRefusal::TierPolicy), Admission::RefusedFinal);
    counts("under-a", &[("queue_full", 1)]);
    assert_eq!(
        tier.would_accept(None, 4, 4),
        Admission::RefusedFinal,
        "would_accept is the tier-policy pass"
    );
    counts("under-a", &[("queue_full", 1)]);

    tier.close();
    assert_eq!(under(OnRefusal::KeepResident), Admission::RefusedFinal);
    counts("under-a", &[("deferred", 1)]);
    assert_eq!(under(OnRefusal::TierPolicy), Admission::RefusedFinal);
    counts("under-a", &[("closed", 1)]);
    let _ = fs::remove_dir_all(&dir);

    // A record over the region size.
    let dir = temp_dir("under-too-large");
    let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
    let tier = SpillTier::open(&cfg, "under-b").unwrap();
    assert_eq!(
        tier.would_accept_under(OnRefusal::KeepResident, None, 1, 100),
        Admission::RefusedFinal
    );
    counts("under-b", &[("deferred", 1)]);
    assert_eq!(
        tier.would_accept_under(OnRefusal::TierPolicy, None, 1, 100),
        Admission::RefusedFinal
    );
    counts("under-b", &[("too_large", 1)]);
    let _ = fs::remove_dir_all(&dir);

    // A tier that keeps records `deferred` under either pass.
    let dir = temp_dir("under-keeps");
    let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
    let tier = SpillTier::open(&cfg, "under-c").unwrap();
    tier.set_keep_resident_when_refused(true);
    assert_eq!(
        tier.would_accept_under(OnRefusal::TierPolicy, None, 1, 100),
        Admission::RefusedFinal
    );
    assert_eq!(
        tier.would_accept_under(OnRefusal::KeepResident, None, 1, 100),
        Admission::RefusedFinal
    );
    counts("under-c", &[("deferred", 2)]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn keep_resident_when_refused_defaults_to_false_and_reflects_the_setter() {
    let dir = temp_dir("keep-resident-flag");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    assert!(!tier.keep_resident_when_refused());
    tier.set_keep_resident_when_refused(true);
    assert!(tier.keep_resident_when_refused());
    tier.set_keep_resident_when_refused(false);
    assert!(!tier.keep_resident_when_refused());
    let _ = fs::remove_dir_all(&dir);
}
