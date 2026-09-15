//! `--report-json <PATH>`: one JSON object summarizing a headless run's
//! shape, RSS, fetch latency, the sample check, convergence, and the
//! summed `sundog_*` totals the metrics module already computes.
//! `--gate <PATH>` reads a threshold file of the same shape and checks the
//! report against it with the pure [`check`], so the gate itself never
//! touches a live cluster and is exercised from a plain [`Report`] value.

use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

/// One headless run's shape, sizing, and outcome, written as JSON by
/// `--report-json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Report {
    pub(crate) nodes: usize,
    pub(crate) keys: usize,
    pub(crate) owners: u8,
    pub(crate) value_bytes: usize,
    pub(crate) max_entries: Option<u64>,
    pub(crate) spill: bool,
    pub(crate) duration_secs: f64,
    pub(crate) preload_keys_per_sec: f64,
    pub(crate) preload_rss_bytes: u64,
    /// The last status sample recorded before the midpoint kill.
    pub(crate) steady_rss_bytes: u64,
    /// The largest RSS reading seen across every sample this run took.
    pub(crate) peak_rss_bytes: u64,
    pub(crate) copies_expected: u64,
    pub(crate) bytes_per_copy: f64,
    pub(crate) fetch_p50_us: u64,
    pub(crate) fetch_p99_us: u64,
    pub(crate) fetch_misses: u64,
    pub(crate) fetch_errors: u64,
    pub(crate) sample_checked: usize,
    pub(crate) sample_ok: usize,
    pub(crate) converged: bool,
    /// `sundog_spill_dropped_total{reason="deferred"}`, summed across
    /// caches: refusals kept resident rather than actually dropped.
    pub(crate) spill_dropped_deferred: u64,
    /// `sundog_rebalance_pull_timeouts_total`, summed across caches.
    pub(crate) pull_timeouts: u64,
    pub(crate) spill_writes: u64,
    pub(crate) ae_repaired: u64,
    /// `sundog_rebalance_buckets_total{direction="in"}`, summed across
    /// caches.
    pub(crate) rebalance_in: u64,
    /// `sundog_rebalance_buckets_total{direction="out"}`, summed across
    /// caches.
    pub(crate) rebalance_out: u64,
}

impl Report {
    /// Renders `self` as pretty-printed JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails, which `Report`'s all-plain
    /// fields never trigger in practice.
    pub(crate) fn to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string_pretty(self).context("report serializes to JSON")
    }

    /// Writes [`Self::to_json`]'s rendering to `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or the write fails.
    pub(crate) fn write_to(&self, path: &Path) -> anyhow::Result<()> {
        let json = self.to_json()?;
        std::fs::write(path, json)
            .with_context(|| format!("writing the report to {}", path.display()))
    }
}

/// `owners * surviving_keys`: the entry count a fully converged cluster's
/// live nodes sum to.
#[must_use]
pub(crate) fn copies_expected(owners: u8, surviving_keys: usize) -> u64 {
    u64::from(owners).saturating_mul(u64::try_from(surviving_keys).unwrap_or(u64::MAX))
}

/// `steady_rss_bytes / copies_expected`, `0.0` when nothing is expected
/// rather than dividing by zero.
#[must_use]
pub(crate) fn bytes_per_copy(steady_rss_bytes: u64, copies_expected: u64) -> f64 {
    if copies_expected == 0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss, reason = "reporting only")]
    {
        steady_rss_bytes as f64 / copies_expected as f64
    }
}

/// Folds one more RSS `sample` into a running `peak`: the larger of the
/// two, so a sequence of samples reduces to the maximum any of them read.
#[must_use]
pub(crate) fn track_peak(peak: u64, sample: u64) -> u64 {
    peak.max(sample)
}

/// A `--gate <PATH>` file's thresholds: [`check`] reports a line for every
/// one `report` violates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Gate {
    pub(crate) max_steady_rss_bytes: u64,
    pub(crate) max_peak_rss_bytes: u64,
    pub(crate) max_spill_dropped_deferred: u64,
    pub(crate) max_pull_timeouts: u64,
    pub(crate) max_fetch_p99_us: u64,
    pub(crate) require_converged: bool,
    pub(crate) require_full_sample: bool,
}

/// Reads and parses a `--gate <PATH>` file.
///
/// # Errors
///
/// Returns an error if the file can't be read or doesn't parse as a
/// [`Gate`].
pub(crate) fn read_gate(path: &Path) -> anyhow::Result<Gate> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading the gate file {}", path.display()))?;
    serde_json::from_str(&text)
        .with_context(|| format!("parsing the gate file {} as JSON", path.display()))
}

/// Whether the sample check ran and every key it checked matched: the same
/// pass condition `headless::run`'s own exit code uses.
#[must_use]
fn sample_fully_passed(report: &Report) -> bool {
    report.sample_checked > 0 && report.sample_checked == report.sample_ok
}

/// Checks `report` against every threshold in `gate`, returning one
/// human-readable line per threshold `report` violates. Pure: every input
/// is a plain value, so a passing and a failing case need no live cluster.
#[must_use]
pub(crate) fn check(report: &Report, gate: &Gate) -> Vec<String> {
    let mut violations = Vec::new();

    if report.steady_rss_bytes > gate.max_steady_rss_bytes {
        violations.push(format!(
            "steady_rss_bytes {} exceeds max_steady_rss_bytes {}",
            report.steady_rss_bytes, gate.max_steady_rss_bytes
        ));
    }
    if report.peak_rss_bytes > gate.max_peak_rss_bytes {
        violations.push(format!(
            "peak_rss_bytes {} exceeds max_peak_rss_bytes {}",
            report.peak_rss_bytes, gate.max_peak_rss_bytes
        ));
    }
    if report.spill_dropped_deferred > gate.max_spill_dropped_deferred {
        violations.push(format!(
            "spill_dropped_deferred {} exceeds max_spill_dropped_deferred {}",
            report.spill_dropped_deferred, gate.max_spill_dropped_deferred
        ));
    }
    if report.pull_timeouts > gate.max_pull_timeouts {
        violations.push(format!(
            "pull_timeouts {} exceeds max_pull_timeouts {}",
            report.pull_timeouts, gate.max_pull_timeouts
        ));
    }
    if report.fetch_p99_us > gate.max_fetch_p99_us {
        violations.push(format!(
            "fetch_p99_us {} exceeds max_fetch_p99_us {}",
            report.fetch_p99_us, gate.max_fetch_p99_us
        ));
    }
    if gate.require_converged && !report.converged {
        violations.push("converged is false but require_converged is set".to_owned());
    }
    if gate.require_full_sample && !sample_fully_passed(report) {
        violations.push(format!(
            "sample check {}/{} did not fully pass but require_full_sample is set",
            report.sample_ok, report.sample_checked
        ));
    }
    violations
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> Report {
        Report {
            nodes: 3,
            keys: 4_000_000,
            owners: 2,
            value_bytes: 256,
            max_entries: Some(800_000),
            spill: true,
            duration_secs: 300.0,
            preload_keys_per_sec: 700_000.0,
            preload_rss_bytes: 2_600_000_000,
            steady_rss_bytes: 3_970_000_000,
            peak_rss_bytes: 5_014_000_000,
            copies_expected: 8_000_000,
            bytes_per_copy: 496.25,
            fetch_p50_us: 138,
            fetch_p99_us: 43_800,
            fetch_misses: 0,
            fetch_errors: 0,
            sample_checked: 2_000,
            sample_ok: 2_000,
            converged: true,
            spill_dropped_deferred: 49_815,
            pull_timeouts: 0,
            spill_writes: 1_200_000,
            ae_repaired: 10,
            rebalance_in: 4_000,
            rebalance_out: 4_000,
        }
    }

    fn passing_gate() -> Gate {
        Gate {
            max_steady_rss_bytes: 4_500_000_000,
            max_peak_rss_bytes: 5_600_000_000,
            max_spill_dropped_deferred: 100_000,
            max_pull_timeouts: 0,
            max_fetch_p99_us: 100_000,
            require_converged: true,
            require_full_sample: true,
        }
    }

    #[test]
    fn copies_expected_multiplies_owners_by_surviving_keys() {
        assert_eq!(copies_expected(2, 4_000_000), 8_000_000);
        assert_eq!(copies_expected(3, 0), 0);
    }

    #[test]
    fn bytes_per_copy_divides_steady_rss_by_copies_expected() {
        assert!((bytes_per_copy(1_000, 10) - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn bytes_per_copy_is_zero_with_nothing_expected_rather_than_dividing_by_zero() {
        assert!(bytes_per_copy(1_000, 0).abs() < f64::EPSILON);
    }

    #[test]
    fn track_peak_keeps_the_larger_of_current_and_sample() {
        assert_eq!(track_peak(100, 250), 250);
        assert_eq!(track_peak(250, 100), 250);
        assert_eq!(track_peak(0, 0), 0);
    }

    #[test]
    fn report_round_trips_through_json() {
        let report = sample_report();
        let json = report.to_json().expect("report serializes");
        let parsed: Report = serde_json::from_str(&json).expect("report deserializes");
        assert_eq!(parsed, report);
    }

    #[test]
    fn check_reports_no_violations_for_a_report_within_every_threshold() {
        assert_eq!(
            check(&sample_report(), &passing_gate()),
            Vec::<String>::new()
        );
    }

    #[test]
    fn check_reports_one_line_per_violated_threshold() {
        let report = Report {
            steady_rss_bytes: 9_000_000_000,
            peak_rss_bytes: 9_500_000_000,
            spill_dropped_deferred: 200_000,
            pull_timeouts: 2,
            fetch_p99_us: 200_000,
            converged: false,
            sample_checked: 2_000,
            sample_ok: 1_999,
            ..sample_report()
        };
        let violations = check(&report, &passing_gate());
        assert_eq!(violations.len(), 7, "{violations:?}");
        assert!(violations.iter().any(|v| v.contains("steady_rss_bytes")));
        assert!(violations.iter().any(|v| v.contains("peak_rss_bytes")));
        assert!(
            violations
                .iter()
                .any(|v| v.contains("spill_dropped_deferred"))
        );
        assert!(violations.iter().any(|v| v.contains("pull_timeouts")));
        assert!(violations.iter().any(|v| v.contains("fetch_p99_us")));
        assert!(violations.iter().any(|v| v.contains("require_converged")));
        assert!(violations.iter().any(|v| v.contains("require_full_sample")));
    }

    #[test]
    fn check_skips_converged_and_sample_thresholds_when_the_gate_does_not_require_them() {
        let report = Report {
            converged: false,
            sample_checked: 2_000,
            sample_ok: 1_999,
            ..sample_report()
        };
        let gate = Gate {
            require_converged: false,
            require_full_sample: false,
            ..passing_gate()
        };
        assert_eq!(check(&report, &gate), Vec::<String>::new());
    }
}
