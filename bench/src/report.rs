//! The run's report: JSON for machines, Markdown for people.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::summary::Summary;
use crate::workload::Workload;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub workload: Workload,
    pub results: Vec<TargetResult>,
}

/// One target's outcome. A target that failed to run carries `error` and
/// zeroed figures.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetResult {
    pub target: String,
    pub image: Option<String>,
    pub transport: String,
    pub reads: Summary,
    pub writes: Summary,
    pub throughput_ops_per_s: f64,
    pub read_misses: u64,
    pub errors: u64,
    /// Memory per stored copy of an entry; `None` where the target reports
    /// no memory figure.
    pub bytes_per_entry: Option<f64>,
    /// Copies of each entry the target holds.
    pub copies: u64,
    pub error: Option<String>,
}

impl TargetResult {
    /// A result for a target that never produced figures.
    #[must_use]
    pub fn failed(target: &str, transport: &str, error: String) -> Self {
        let zero = Summary {
            count: 0,
            mean_us: 0.0,
            p50_us: 0.0,
            p99_us: 0.0,
            p999_us: 0.0,
            max_us: 0.0,
        };
        Self {
            target: target.to_string(),
            image: None,
            transport: transport.to_string(),
            reads: zero,
            writes: zero,
            throughput_ops_per_s: 0.0,
            read_misses: 0,
            errors: 0,
            bytes_per_entry: None,
            copies: 0,
            error: Some(error),
        }
    }
}

impl Report {
    /// The report as a Markdown section: the workload, one table row per
    /// target, and how each target is reached.
    #[must_use]
    pub fn markdown(&self) -> String {
        let w = &self.workload;
        let mut out = String::new();
        let _ = writeln!(out, "## Benchmark\n");
        let _ = writeln!(
            out,
            "{} keys of {} bytes, zipf exponent {}, {:.0}% reads, {} measured operations \
             after {} warm-up operations, {} concurrent workers.\n",
            w.keys,
            w.value_bytes,
            w.zipf_exponent,
            w.read_ratio * 100.0,
            w.ops,
            w.warmup_ops,
            w.concurrency
        );
        let _ = writeln!(
            out,
            "| Target | Read p50 | Read p99 | Write p50 | Write p99 | Throughput | Bytes per entry |"
        );
        let _ = writeln!(out, "|---|---:|---:|---:|---:|---:|---:|");
        for r in &self.results {
            if let Some(error) = &r.error {
                let _ = writeln!(
                    out,
                    "| {} | failed: {} | | | | | |",
                    r.target,
                    one_line(error)
                );
                continue;
            }
            let bytes = r
                .bytes_per_entry
                .map_or_else(|| "not reported".to_string(), |b| format!("{b:.0}"));
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} | {:.0} ops/s | {} |",
                r.target,
                micros(r.reads.p50_us),
                micros(r.reads.p99_us),
                micros(r.writes.p50_us),
                micros(r.writes.p99_us),
                r.throughput_ops_per_s,
                bytes
            );
        }
        let _ = writeln!(out, "\n| Target | Reached through | Image |");
        let _ = writeln!(out, "|---|---|---|");
        for r in &self.results {
            let _ = writeln!(
                out,
                "| {} | {} | {} |",
                r.target,
                r.transport,
                r.image.as_deref().unwrap_or("in this process")
            );
        }
        out
    }
}

fn micros(us: f64) -> String {
    if us < 10.0 {
        format!("{us:.2} µs")
    } else {
        format!("{us:.0} µs")
    }
}

fn one_line(text: &str) -> String {
    text.lines().next().unwrap_or_default().replace('|', "/")
}

#[cfg(test)]
fn summary(p50: f64, p99: f64) -> Summary {
    Summary {
        count: 10,
        mean_us: p50,
        p50_us: p50,
        p99_us: p99,
        p999_us: p99,
        max_us: p99,
    }
}

#[cfg(test)]
pub(crate) fn sample_report() -> Report {
    Report {
        workload: Workload::default(),
        results: vec![
            TargetResult {
                target: "sundog-local".into(),
                image: None,
                transport: "in-process".into(),
                reads: summary(0.4, 1.2),
                writes: summary(0.9, 3.0),
                throughput_ops_per_s: 2_000_000.0,
                read_misses: 0,
                errors: 0,
                bytes_per_entry: Some(180.4),
                copies: 1,
                error: None,
            },
            TargetResult {
                target: "hazelcast".into(),
                image: Some("hazelcast/hazelcast:5.5".into()),
                transport: "loopback TCP".into(),
                reads: summary(80.0, 400.0),
                writes: summary(90.0, 500.0),
                throughput_ops_per_s: 90_000.0,
                read_misses: 0,
                errors: 0,
                bytes_per_entry: None,
                copies: 1,
                error: None,
            },
            TargetResult::failed(
                "dragonfly",
                "loopback TCP",
                "image pull | denied\nmore".into(),
            ),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_lists_every_target_with_units_and_gaps() {
        let md = sample_report().markdown();
        assert!(
            md.contains(
                "| sundog-local | 0.40 µs | 1.20 µs | 0.90 µs | 3.00 µs | 2000000 ops/s | 180 |"
            ),
            "{md}"
        );
        assert!(
            md.contains(
                "| hazelcast | 80 µs | 400 µs | 90 µs | 500 µs | 90000 ops/s | not reported |"
            ),
            "{md}"
        );
        assert!(
            md.contains("| dragonfly | failed: image pull / denied |"),
            "{md}"
        );
        assert!(
            md.contains("| hazelcast | loopback TCP | hazelcast/hazelcast:5.5 |"),
            "{md}"
        );
        assert!(
            md.contains("| sundog-local | in-process | in this process |"),
            "{md}"
        );
    }

    #[test]
    fn the_report_round_trips_through_json() {
        let report = sample_report();
        let json = serde_json::to_string(&report).expect("serialize");
        assert_eq!(
            serde_json::from_str::<Report>(&json).expect("parse"),
            report
        );
    }
}
