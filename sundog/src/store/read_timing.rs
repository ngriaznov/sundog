//! Sampled timing of [`Shard::get`](super::Shard::get) and
//! [`Shard::get_sync`](super::Shard::get_sync) for
//! `sundog_read_duration_seconds{cache,outcome}`.
//!
//! A resident read takes well under a microsecond, and the Prometheus
//! recorder buffers every histogram sample until its next upkeep, so timing
//! every read would cost more than the read and hold megabytes of samples on
//! a busy node. About one read in [`READ_SAMPLE_EVERY`] on each thread is
//! timed instead, counted down in a thread-local cell that no two cores ever
//! share. The stride to the next timed read is drawn uniformly from
//! [`STRIDE_MIN`] to [`STRIDE_MAX`] reads, so a thread whose reads cycle
//! through several caches in a fixed pattern still times each of them. The
//! histogram's count is a sample count; the read rate is
//! `sundog_cache_hits_total` and `sundog_cache_misses_total`.

use std::cell::Cell;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

/// The mean stride between timed reads on one thread.
pub(crate) const READ_SAMPLE_EVERY: u32 = 256;

/// The shortest stride between timed reads.
pub(crate) const STRIDE_MIN: u32 = READ_SAMPLE_EVERY / 2;

/// The longest stride between timed reads.
pub(crate) const STRIDE_MAX: u32 = READ_SAMPLE_EVERY * 3 / 2;

/// One thread's sampling state: reads left until the next timed one, and
/// the xorshift state its strides are drawn from. A zero `rng` is a thread
/// that has not read yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sampler {
    until_next: u32,
    rng: u32,
}

impl Sampler {
    /// A sampler seeded with `seed`, its first timed read one drawn stride
    /// away. A zero seed is replaced, since xorshift never leaves zero.
    pub(crate) fn seeded(seed: u32) -> Self {
        let (rng, stride) = next_stride(if seed == 0 { 0x9E37_79B9 } else { seed });
        Self {
            until_next: stride,
            rng,
        }
    }

    /// Counts one read: the sampler after it, and whether this read is
    /// timed. Pure; unit tested directly.
    pub(crate) fn advance(self) -> (Self, bool) {
        if self.until_next > 1 {
            return (
                Self {
                    until_next: self.until_next - 1,
                    ..self
                },
                false,
            );
        }
        let (rng, stride) = next_stride(self.rng);
        (
            Self {
                until_next: stride,
                rng,
            },
            true,
        )
    }
}

/// One xorshift32 step from a nonzero `state`, and the stride it draws,
/// uniform in [`STRIDE_MIN`]..=[`STRIDE_MAX`]. Pure; unit tested directly.
pub(crate) const fn next_stride(state: u32) -> (u32, u32) {
    let mut x = state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    (x, STRIDE_MIN + x % (STRIDE_MAX - STRIDE_MIN + 1))
}

/// Seeds for each new thread's sampler, a golden-ratio step apart.
static NEXT_SEED: AtomicU32 = AtomicU32::new(0x2545_F491);

thread_local! {
    static SAMPLER: Cell<Sampler> = const { Cell::new(Sampler { until_next: 0, rng: 0 }) };
}

/// Counts one read on this thread and, when it is timed, the instant it
/// started.
pub(crate) fn start_read() -> Option<Instant> {
    SAMPLER.with(|cell| {
        let mut sampler = cell.get();
        if sampler.rng == 0 {
            sampler = Sampler::seeded(NEXT_SEED.fetch_add(0x9E37_79B9, Ordering::Relaxed));
        }
        let (next, timed) = sampler.advance();
        cell.set(next);
        timed.then(Instant::now)
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

    /// The read indexes, counting from 1, that `sampler` times over `reads`
    /// reads.
    fn timed_reads(mut sampler: Sampler, reads: u32) -> Vec<u32> {
        let mut timed = Vec::new();
        for read in 1..=reads {
            let (next, due) = sampler.advance();
            sampler = next;
            if due {
                timed.push(read);
            }
        }
        timed
    }

    #[test]
    fn strides_stay_in_range_and_average_the_sample_interval() {
        let mut state = 0x1234_5678;
        let mut strides = Vec::new();
        for _ in 0..100_000 {
            let (next, stride) = next_stride(state);
            state = next;
            strides.push(stride);
        }
        assert!(
            strides
                .iter()
                .all(|&s| (STRIDE_MIN..=STRIDE_MAX).contains(&s))
        );
        let mean = strides.iter().map(|&s| f64::from(s)).sum::<f64>() / 100_000.0;
        assert!((mean - f64::from(READ_SAMPLE_EVERY)).abs() < 2.0, "{mean}");
        let distinct = strides.iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(distinct.len(), (STRIDE_MAX - STRIDE_MIN + 1) as usize);
    }

    #[test]
    fn timed_reads_fall_one_stride_apart() {
        let timed = timed_reads(Sampler::seeded(7), 100_000);
        assert!(
            (STRIDE_MIN..=STRIDE_MAX).contains(&timed[0]),
            "{}",
            timed[0]
        );
        assert!(
            timed
                .windows(2)
                .all(|pair| (STRIDE_MIN..=STRIDE_MAX).contains(&(pair[1] - pair[0])))
        );
        let expected = 100_000 / READ_SAMPLE_EVERY;
        assert!(
            timed.len().abs_diff(expected as usize) < 20,
            "{}",
            timed.len()
        );
    }

    #[test]
    fn reads_cycling_through_four_caches_time_every_cache() {
        let timed = timed_reads(Sampler::seeded(11), 400_000);
        let mut per_cache = [0usize; 4];
        for read in &timed {
            per_cache[(read % 4) as usize] += 1;
        }
        let each = timed.len() / 4;
        assert!(
            per_cache.iter().all(|&n| n.abs_diff(each) < each / 5),
            "{per_cache:?}"
        );
    }

    #[test]
    fn a_zero_seed_still_samples() {
        assert_eq!(Sampler::seeded(0), Sampler::seeded(0x9E37_79B9));
        let first = timed_reads(Sampler::seeded(0), STRIDE_MAX)[0];
        assert!((STRIDE_MIN..=STRIDE_MAX).contains(&first), "{first}");
    }

    #[test]
    fn a_fresh_thread_times_one_read_a_stride() {
        let timed = std::thread::spawn(|| {
            (0..STRIDE_MAX * 2)
                .filter(|_| start_read().is_some())
                .count()
        })
        .join()
        .expect("the thread finishes");
        assert!((2..=6).contains(&timed), "{timed}");
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
