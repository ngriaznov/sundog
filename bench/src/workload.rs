//! The workload every target runs: keys drawn from a zipf distribution over
//! a fixed key space, a read/write mix, and fixed-size values.

use serde::{Deserialize, Serialize};

/// One run's shape, identical for every target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workload {
    /// Distinct keys, all loaded before measuring.
    pub keys: usize,
    /// Bytes in every value.
    pub value_bytes: usize,
    /// Measured operations across all workers.
    pub ops: usize,
    /// Operations each target runs unmeasured first, to warm connections
    /// and caches.
    pub warmup_ops: usize,
    /// Concurrent workers, each with its own client.
    pub concurrency: usize,
    /// Fraction of operations that are reads, from 0 to 1.
    pub read_ratio: f64,
    /// The zipf exponent: 0 is uniform, and higher values concentrate on
    /// fewer keys.
    pub zipf_exponent: f64,
    /// Seeds every worker's random stream, so two runs draw the same keys.
    pub seed: u64,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            keys: 100_000,
            value_bytes: 100,
            ops: 200_000,
            warmup_ops: 20_000,
            concurrency: 16,
            read_ratio: 0.9,
            zipf_exponent: 0.99,
            seed: 7,
        }
    }
}

/// A zipf distribution over ranks `0..n`, rank 0 the most frequent,
/// sampled by inverting a precomputed cumulative distribution.
#[derive(Debug, Clone)]
pub struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    /// A distribution over `n` ranks with the given exponent.
    ///
    /// # Panics
    ///
    /// Panics if `n` is zero.
    #[must_use]
    pub fn new(n: usize, exponent: f64) -> Self {
        assert!(n > 0, "a zipf distribution needs at least one rank");
        let mut cdf = Vec::with_capacity(n);
        let mut total = 0.0;
        for rank in 1..=n {
            #[expect(
                clippy::cast_precision_loss,
                reason = "ranks stay far below 2^52, where f64 is exact"
            )]
            let weight = 1.0 / (rank as f64).powf(exponent);
            total += weight;
            cdf.push(total);
        }
        for point in &mut cdf {
            *point /= total;
        }
        Self { cdf }
    }

    /// The rank a uniform draw `u` in `[0, 1)` maps to.
    #[must_use]
    pub fn sample(&self, u: f64) -> usize {
        self.cdf
            .partition_point(|&point| point <= u)
            .min(self.cdf.len() - 1)
    }

    /// The probability of drawing `rank`.
    #[cfg(test)]
    #[must_use]
    pub fn probability(&self, rank: usize) -> f64 {
        let below = if rank == 0 { 0.0 } else { self.cdf[rank - 1] };
        self.cdf[rank] - below
    }
}

/// The key for index `i`: fixed width, so every key costs the same bytes.
#[must_use]
pub fn key(i: usize) -> String {
    format!("key:{i:010}")
}

/// A value of `len` bytes that differs per key, so no server can
/// deduplicate values across keys.
#[must_use]
pub fn value(i: usize, len: usize) -> Vec<u8> {
    let seed = i.to_le_bytes();
    (0..len)
        .map(|at| seed[at % seed.len()].wrapping_add(u8::try_from(at % 251).unwrap_or(0)))
        .collect()
}

/// Whether a uniform draw `u` in `[0, 1)` makes this operation a read.
#[must_use]
pub fn is_read(u: f64, read_ratio: f64) -> bool {
    u < read_ratio
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zipf_probabilities_sum_to_one_and_fall_with_rank() {
        let zipf = Zipf::new(1_000, 0.99);
        let total: f64 = (0..1_000).map(|rank| zipf.probability(rank)).sum();
        assert!((total - 1.0).abs() < 1e-9, "total {total}");
        assert!(zipf.probability(0) > zipf.probability(1));
        assert!(zipf.probability(1) > zipf.probability(999));
    }

    #[test]
    fn zipf_samples_stay_in_range_at_both_ends() {
        let zipf = Zipf::new(10, 1.2);
        assert_eq!(zipf.sample(0.0), 0);
        assert_eq!(zipf.sample(0.999_999_999), 9);
        assert!(zipf.sample(0.5) < 10);
    }

    #[test]
    fn a_zero_exponent_is_uniform() {
        let zipf = Zipf::new(4, 0.0);
        for rank in 0..4 {
            assert!((zipf.probability(rank) - 0.25).abs() < 1e-12);
        }
        assert_eq!(zipf.sample(0.3), 1);
    }

    #[test]
    fn keys_are_fixed_width_and_values_differ_per_key() {
        assert_eq!(key(7), "key:0000000007");
        assert_eq!(key(7).len(), key(123_456).len());
        assert_eq!(value(1, 100).len(), 100);
        assert_ne!(value(1, 16), value(2, 16));
        assert!(value(3, 0).is_empty());
    }

    #[test]
    fn the_read_ratio_splits_draws() {
        assert!(is_read(0.2, 0.9));
        assert!(!is_read(0.95, 0.9));
        assert!(!is_read(0.0, 0.0), "a zero ratio never reads");
        assert!(is_read(0.999, 1.0), "a ratio of one always reads");
    }
}
