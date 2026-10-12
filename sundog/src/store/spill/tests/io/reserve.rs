//! Tests for `SpillTier` admission: `would_accept`, `reserve`, `release` and the wait metrics.

use super::*;

#[test]
fn would_accept_refuses_once_the_flush_queue_bytes_bound_would_be_exceeded() {
    let dir = temp_dir("byte-bound");
    let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    // Paused before the flusher thread is even spawned, so it never
    // gets a chance to race this test and drain the job the
    // assertions below depend on staying queued.
    tier.pause_flusher();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    // A standalone check here would double-count: try_spill's own
    // would_accept call below already commits the bytes on success.
    assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));
    assert_eq!(tier.queued_bytes(), record_len);
    assert_eq!(
        tier.would_accept(None, 4, 4),
        Admission::RefusedFinal,
        "the queue already holds one record's worth; a same-size second job would \
         push it past flush_queue_bytes"
    );

    tier.resume_flusher();
    assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 1));
    assert!(
        poll_until(POLL_TIMEOUT, || tier.queued_bytes() == 0),
        "queued_bytes drains back to zero once the flusher takes the job"
    );
    assert_eq!(
        tier.would_accept(None, 4, 4),
        Admission::Accepted,
        "room again once the backlog has drained"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn would_accept_never_refuses_when_the_caller_pre_reserved_enough_bytes() {
    // A pre-reserved second job succeeds via Reservation::spend alone.
    let dir = temp_dir("byte-bound-reserved");
    let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
    let record_len_u32 = u32::try_from(record_len).unwrap();
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    tier.pause_flusher();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));
    assert_eq!(tier.queued_bytes(), record_len, "admit is fully drained");
    assert_eq!(
        tier.would_accept(None, 4, 4),
        Admission::RefusedFinal,
        "the ordinary fallback still refuses with no reservation and no room"
    );

    let mut reservation = Reservation {
        admit: &tier.inner.admit,
        remaining: record_len_u32,
    };
    assert_eq!(
        tier.would_accept(Some(&mut reservation), 4, 4),
        Admission::Accepted,
        "a sufficient reservation admits the record with admit still fully drained"
    );
    drop(reservation);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn would_accept_returns_refused_pending_when_a_reservation_is_in_play_but_both_budgets_are_exhausted()
 {
    // Insufficient reservation + drained admit: RefusedPending, not a drop.
    let dir = temp_dir("byte-bound-pending");
    let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    tier.pause_flusher();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));
    assert_eq!(tier.queued_bytes(), record_len, "admit is fully drained");

    let mut reservation = Reservation {
        admit: &tier.inner.admit,
        remaining: 1,
    };
    assert_eq!(
        tier.would_accept(Some(&mut reservation), 4, 4),
        Admission::RefusedPending,
        "a reservation short of the record's own length, with admit also fully \
         drained, is a pending refusal, not a final one"
    );
    assert_eq!(
        reservation.remaining, 1,
        "the insufficient reservation is left untouched, not partially spent"
    );
    assert_eq!(
        tier.queued_bytes(),
        record_len,
        "admit's own accounting is unaffected by a pending refusal: no permits \
         acquired, none released"
    );
    drop(reservation);

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn reserve_resolves_once_the_flusher_frees_room() {
    let dir = temp_dir("reserve-resolves");
    let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
    let record_len_u32 = u32::try_from(record_len).unwrap();
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    // Paused before the flusher spawns, so admit stays drained.
    tier.pause_flusher();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));
    assert_eq!(tier.queued_bytes(), record_len, "admit is fully drained");

    // Pinned since a Reservation borrows this tier; a timed-out
    // poll keeps the future's registration with admit.
    let reserve_fut = tier.reserve(record_len_u32, Duration::from_secs(5));
    tokio::pin!(reserve_fut);

    // Not resolved yet: the queue is still fully drained.
    let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut reserve_fut).await;
    assert!(
        not_yet.is_err(),
        "no room yet; reserve must still be waiting"
    );

    tier.resume_flusher();
    let reservation = tokio::time::timeout(Duration::from_secs(5), &mut reserve_fut)
        .await
        .expect("reserve resolves once the flusher frees room")
        .expect("reserve succeeds once the flusher frees room");
    // queued_bytes() can't tell this apart from a job still queued.
    assert_eq!(tier.queued_bytes(), record_len);
    drop(reservation);
    assert_eq!(
        tier.queued_bytes(),
        0,
        "dropping the reservation returns its unspent bytes to admit"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn reserve_times_out_when_the_flusher_never_resumes() {
    let dir = temp_dir("reserve-times-out");
    let record_len = HEADER_LEN as u64 + 4 + 4;
    let record_len_u32 = u32::try_from(record_len).unwrap();
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    tier.pause_flusher();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));

    let start = Instant::now();
    let short_timeout = Duration::from_millis(100);
    let result = tier.reserve(record_len_u32, short_timeout).await;
    let elapsed = start.elapsed();

    assert!(matches!(result, Err(SpillWaitTimedOut)));
    assert!(
        elapsed >= short_timeout,
        "reserve must not return before its own timeout elapses: waited {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "reserve must not hang past its own timeout: waited {elapsed:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn reserve_clamps_a_request_larger_than_total_permits() {
    let dir = temp_dir("reserve-clamps");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(64);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();

    // Larger than admit could ever hold: must clamp, not hang.
    let reservation = tokio::time::timeout(
        Duration::from_secs(5),
        tier.reserve(u32::MAX, Duration::from_secs(5)),
    )
    .await
    .expect("reserve resolves promptly once clamped")
    .expect("reserve succeeds against the tier's whole, empty queue");
    drop(reservation);

    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unspent_reservation_bytes_return_on_drop() {
    let dir = temp_dir("reservation-drop");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(100);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();

    let mut reservation = tier
        .reserve(100, Duration::from_secs(5))
        .await
        .expect("the whole queue is free");
    assert_eq!(tier.queued_bytes(), 100, "the full reservation is held");

    assert!(
        reservation.spend(40),
        "40 of the 100 reserved bytes spend cleanly"
    );
    drop(reservation);

    assert_eq!(
        tier.queued_bytes(),
        40,
        "only the 40 spent bytes stay charged against admit; the 60 never spent \
         return to it on drop"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn spill_tier_exposes_its_configured_spill_wait_timeout() {
    let dir = temp_dir("tier-timeout-accessor");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .spill_wait_timeout(Duration::from_secs(7));
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    assert_eq!(tier.spill_wait_timeout_value(), Duration::from_secs(7));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn release_returns_bytes_to_admit() {
    let dir = temp_dir("release");
    let record_len = HEADER_LEN as u64 + 4 + 4;
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    assert_eq!(
        tier.would_accept(None, 4, 4),
        Admission::Accepted,
        "admits the one record's worth"
    );
    assert_eq!(tier.queued_bytes(), record_len);

    tier.release(u32::try_from(record_len).unwrap());
    assert_eq!(
        tier.queued_bytes(),
        0,
        "release returns the given bytes to admit, exactly the way \
         finish_spill_handoff's Err branch relies on for a job that never reaches the \
         flusher's channel"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn flusher_loop_credits_a_dequeued_jobs_bytes_even_when_the_sink_has_already_died() {
    let dir = temp_dir("flusher-loop-dead-sink");
    let record_len = HEADER_LEN as u64 + 4 + 4; // "aaaa"/"bbbb"
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    assert_eq!(tier.queued_bytes(), 0, "nothing queued yet");

    // Debits admit first so crediting it back is observable.
    assert_eq!(
        tier.would_accept(None, 4, 4),
        Admission::Accepted,
        "admits the one record's worth"
    );
    assert_eq!(tier.queued_bytes(), record_len, "admit is fully drained");

    // One job already on the channel, as finish_spill_handoff leaves it.
    let (tx, rx) = mpsc::sync_channel(1);
    tx.send(job("aaaa", b"bbbb", hlc(1, 0))).unwrap();
    // Dropped so a second recv() returns Err, forcing one dequeue.
    drop(tx);

    // Strong reference already gone before upgrade().
    let sink: Weak<dyn SpillSink> = {
        let strong: Arc<dyn SpillSink> = Arc::new(RecordingSink::default());
        Arc::downgrade(&strong)
    };

    flusher_loop(&tier.inner, &rx, &sink);

    assert_eq!(
        tier.queued_bytes(),
        0,
        "a job dequeued off the channel must credit its bytes back to `admit` even \
         when the sink has already died, or this tier's admission budget shrinks \
         permanently by that job's own bytes for the rest of this tier's lifetime"
    );

    let _ = fs::remove_dir_all(&dir);
}

/// Captures wait-related metrics for one dedicated cache name, so
/// concurrent tests never perturb these counts.
#[derive(Clone, Default)]
struct WaitMetrics {
    waiters: Arc<StdMutex<f64>>,
    wait_seconds_total: Arc<StdMutex<u64>>,
    wait_timeouts_total: Arc<StdMutex<u64>>,
}

struct WaitCounter {
    field: Arc<StdMutex<u64>>,
}

impl metrics::CounterFn for WaitCounter {
    fn increment(&self, value: u64) {
        *self.field.lock().unwrap() += value;
    }

    fn absolute(&self, value: u64) {
        *self.field.lock().unwrap() = value;
    }
}

struct WaitGauge {
    current: Arc<StdMutex<f64>>,
}

impl metrics::GaugeFn for WaitGauge {
    fn increment(&self, value: f64) {
        *self.current.lock().unwrap() += value;
    }

    fn decrement(&self, value: f64) {
        *self.current.lock().unwrap() -= value;
    }

    fn set(&self, value: f64) {
        *self.current.lock().unwrap() = value;
    }
}

struct WaitMetricsRecorder {
    metrics: WaitMetrics,
}

impl metrics::Recorder for WaitMetricsRecorder {
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
            .any(|l| l.key() == "cache" && l.value() == WAIT_METRICS_CACHE);
        if !this_cache {
            return metrics::Counter::noop();
        }
        match key.name() {
            "sundog_spill_wait_seconds_total" => {
                metrics::Counter::from_arc(Arc::new(WaitCounter {
                    field: self.metrics.wait_seconds_total.clone(),
                }))
            }
            "sundog_spill_wait_timeouts_total" => {
                metrics::Counter::from_arc(Arc::new(WaitCounter {
                    field: self.metrics.wait_timeouts_total.clone(),
                }))
            }
            _ => metrics::Counter::noop(),
        }
    }

    fn register_gauge(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        let this_cache = key
            .labels()
            .any(|l| l.key() == "cache" && l.value() == WAIT_METRICS_CACHE);
        if !this_cache || key.name() != "sundog_spill_waiters" {
            return metrics::Gauge::noop();
        }
        metrics::Gauge::from_arc(Arc::new(WaitGauge {
            current: self.metrics.waiters.clone(),
        }))
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
/// recorder ignores waiter/wait activity from every other test.
const WAIT_METRICS_CACHE: &str = "wait-metrics-only";

#[tokio::test]
async fn reserve_tracks_waiters_wait_seconds_and_wait_timeouts() {
    let metrics = WaitMetrics::default();
    let installed = metrics::set_global_recorder(WaitMetricsRecorder {
        metrics: metrics.clone(),
    })
    .is_ok();
    if !installed {
        // Another test already holds the global recorder slot.
        return;
    }

    // waiters rises while pending; wait_seconds accumulates real time.
    let dir = temp_dir("wait-metrics-resolves");
    let record_len = HEADER_LEN as u64 + 4 + 4;
    let record_len_u32 = u32::try_from(record_len).unwrap();
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, WAIT_METRICS_CACHE).unwrap();
    tier.pause_flusher();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));
    assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));

    let reserve_fut = tier.reserve(record_len_u32, Duration::from_secs(10));
    tokio::pin!(reserve_fut);
    let not_yet = tokio::time::timeout(Duration::from_millis(100), &mut reserve_fut).await;
    assert!(
        not_yet.is_err(),
        "no room yet; reserve must still be pending"
    );
    assert!(
        (*metrics.waiters.lock().unwrap() - 1.0).abs() < f64::EPSILON,
        "sundog_spill_waiters counts this one pending reserve call"
    );

    // Lower-bounded so the whole-second truncation is observable.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    tier.resume_flusher();
    let reservation = tokio::time::timeout(Duration::from_secs(5), &mut reserve_fut)
        .await
        .expect("reserve resolves once the flusher frees room")
        .expect("reserve succeeds once the flusher frees room");
    drop(reservation);

    assert!(
        metrics.waiters.lock().unwrap().abs() < f64::EPSILON,
        "the guard decrements sundog_spill_waiters back to zero once reserve resolves"
    );
    assert!(
        *metrics.wait_seconds_total.lock().unwrap() >= 1,
        "sundog_spill_wait_seconds_total accumulates at least the one whole second this \
         call was genuinely blocked"
    );
    assert_eq!(
        *metrics.wait_timeouts_total.lock().unwrap(),
        0,
        "this call resolved on its own; it never hit its own timeout"
    );
    let _ = fs::remove_dir_all(&dir);

    // An elapsed timeout increments wait_timeouts_total once.
    let dir = temp_dir("wait-metrics-timeout");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(record_len);
    let tier = SpillTier::open(&cfg, WAIT_METRICS_CACHE).unwrap();
    tier.pause_flusher();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));
    assert!(tier.try_spill(job("aaaa", b"bbbb", hlc(1, 0))));

    let result = tier
        .reserve(record_len_u32, Duration::from_millis(100))
        .await;
    assert!(matches!(result, Err(SpillWaitTimedOut)));
    assert_eq!(
        *metrics.wait_timeouts_total.lock().unwrap(),
        1,
        "the elapsed timeout increments sundog_spill_wait_timeouts_total exactly once"
    );
    assert!(
        metrics.waiters.lock().unwrap().abs() < f64::EPSILON,
        "the guard decrements sundog_spill_waiters on the timeout path too"
    );

    let _ = fs::remove_dir_all(&dir);
}
