//! Latency histograms and their summaries.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

/// Highest latency a histogram records, one minute in nanoseconds. A slower
/// operation is clamped to it.
const MAX_NANOS: u64 = 60_000_000_000;

/// A fresh histogram recording nanoseconds at three significant digits.
///
/// # Panics
///
/// Never: the bounds are constants hdrhistogram accepts.
#[must_use]
pub fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, MAX_NANOS, 3).expect("constant histogram bounds are valid")
}

/// Records one operation's latency in nanoseconds.
pub fn record(histogram: &mut Histogram<u64>, nanos: u64) {
    histogram.saturating_record(nanos.clamp(1, MAX_NANOS));
}

/// One operation kind's latency, in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    pub count: u64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
}

impl Summary {
    /// The summary of `histogram`, all zeros for an empty one.
    #[must_use]
    pub fn of(histogram: &Histogram<u64>) -> Self {
        if histogram.is_empty() {
            return Self {
                count: 0,
                mean_us: 0.0,
                p50_us: 0.0,
                p99_us: 0.0,
                p999_us: 0.0,
                max_us: 0.0,
            };
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "latencies stay far below 2^52 nanoseconds"
        )]
        let micros = |nanos: u64| nanos as f64 / 1_000.0;
        Self {
            count: histogram.len(),
            mean_us: histogram.mean() / 1_000.0,
            p50_us: micros(histogram.value_at_quantile(0.50)),
            p99_us: micros(histogram.value_at_quantile(0.99)),
            p999_us: micros(histogram.value_at_quantile(0.999)),
            max_us: micros(histogram.max()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_histogram_summarizes_to_zeros() {
        let summary = Summary::of(&histogram());
        assert_eq!(summary.count, 0);
        assert!(summary.p99_us.abs() < f64::EPSILON);
    }

    #[test]
    fn quantiles_come_out_in_microseconds() {
        let mut h = histogram();
        for _ in 0..99 {
            record(&mut h, 1_000);
        }
        record(&mut h, 500_000);
        let summary = Summary::of(&h);
        assert_eq!(summary.count, 100);
        assert!((summary.p50_us - 1.0).abs() < 0.01, "{summary:?}");
        assert!((summary.p99_us - 1.0).abs() < 0.01, "{summary:?}");
        assert!((summary.max_us - 500.0).abs() < 1.0, "{summary:?}");
    }

    #[test]
    fn out_of_range_latencies_are_clamped() {
        let mut h = histogram();
        record(&mut h, 0);
        record(&mut h, u64::MAX);
        assert_eq!(h.len(), 2);
        // hdrhistogram reports a bucket's upper edge, a hair above the clamp.
        assert!(Summary::of(&h).max_us <= 60_100_000.0);
    }
}
