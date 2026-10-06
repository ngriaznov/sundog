//! Sampled timing of [`Shard::get`](super::Shard::get) and
//! [`Shard::get_sync`](super::Shard::get_sync) for
//! `sundog_read_duration_seconds{cache,outcome}`.
//!
//! A resident read takes well under a microsecond, and the Prometheus
//! recorder buffers every histogram sample until its next upkeep, so timing
//! every read would cost more than the read and hold megabytes of samples on
//! a busy node. One read in [`READ_SAMPLE_EVERY`] on each thread is timed
//! instead, counted in a thread-local cell that no two cores ever share. The
//! histogram's count is therefore a sample count; the read rate is
//! `sundog_cache_hits_total` and `sundog_cache_misses_total`.

use std::cell::Cell;
use std::time::Instant;

/// One read in this many, per thread, is timed.
pub(crate) const READ_SAMPLE_EVERY: u32 = 256;

thread_local! {
    /// Reads this thread has started, wrapping.
    static READS: Cell<u32> = const { Cell::new(0) };
}

/// Whether a thread's `read`th read, counting from 1, is timed. Pure; unit
/// tested directly.
pub(crate) const fn read_sample_due(read: u32) -> bool {
    read.is_multiple_of(READ_SAMPLE_EVERY)
}

/// Counts one read on this thread and, when it is due a sample, the instant
/// it started.
pub(crate) fn start_read() -> Option<Instant> {
    READS.with(|reads| {
        let read = reads.get().wrapping_add(1);
        reads.set(read);
        read_sample_due(read).then(Instant::now)
    })
}

/// `sundog_read_duration_seconds{cache,outcome}`'s two handles, resolved
/// once when a shard is built.
pub(crate) struct ReadDurations {
    hit: metrics::Histogram,
    miss: metrics::Histogram,
}

impl ReadDurations {
    pub(crate) fn new(cache: &str) -> Self {
        let histogram = |outcome: &'static str| {
            metrics::histogram!(
                "sundog_read_duration_seconds",
                "cache" => cache.to_owned(),
                "outcome" => outcome,
            )
        };
        Self {
            hit: histogram("hit"),
            miss: histogram("miss"),
        }
    }

    /// Records a read [`start_read`] sampled at `started`; a no-op for an
    /// unsampled read.
    pub(crate) fn record(&self, started: Option<Instant>, hit: bool) {
        if let Some(started) = started {
            let histogram = if hit { &self.hit } else { &self.miss };
            histogram.record(started.elapsed());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_read_in_every_sample_interval_is_due() {
        let due = (1..=READ_SAMPLE_EVERY * 3)
            .filter(|&read| read_sample_due(read))
            .collect::<Vec<_>>();
        assert_eq!(
            due,
            vec![
                READ_SAMPLE_EVERY,
                READ_SAMPLE_EVERY * 2,
                READ_SAMPLE_EVERY * 3
            ]
        );
        assert!(read_sample_due(u32::MAX.wrapping_add(1)), "the count wraps");
    }

    #[test]
    fn a_fresh_thread_times_exactly_its_due_reads() {
        let sampled = std::thread::spawn(|| {
            (0..READ_SAMPLE_EVERY * 2)
                .filter(|_| start_read().is_some())
                .count()
        })
        .join()
        .expect("the thread finishes");
        assert_eq!(sampled, 2);
    }

    #[test]
    fn only_a_sampled_read_is_recorded() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let durations = ReadDurations::new("timed");
            durations.record(None, true);
            durations.record(Some(Instant::now()), true);
            durations.record(Some(Instant::now()), false);
        });
        let mut samples: Vec<(String, usize)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| {
                let outcome = key
                    .key()
                    .labels()
                    .find(|label| label.key() == "outcome")
                    .map(|label| label.value().to_owned())
                    .unwrap_or_default();
                let count = match value {
                    metrics_util::debugging::DebugValue::Histogram(values) => values.len(),
                    _ => 0,
                };
                (outcome, count)
            })
            .collect();
        samples.sort();
        assert_eq!(samples, vec![("hit".into(), 1), ("miss".into(), 1)]);
    }
}
