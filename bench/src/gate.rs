//! Thresholds a run must meet, read from a JSON file like
//! `ops/bench-gate.json`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::report::Report;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Gate {
    /// Thresholds per target name. A gated target missing from the report,
    /// or one that failed to run, is a violation.
    pub targets: BTreeMap<String, TargetGate>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[expect(
    clippy::struct_field_names,
    reason = "the fields are the gate file's JSON keys, where `max_` reads as the bound it is"
)]
pub struct TargetGate {
    pub max_read_p99_us: Option<f64>,
    pub max_write_p99_us: Option<f64>,
    pub max_errors: Option<u64>,
}

impl Gate {
    /// Every threshold `report` breaks, one line each; empty when it passes.
    #[must_use]
    pub fn violations(&self, report: &Report) -> Vec<String> {
        let mut out = Vec::new();
        for (name, gate) in &self.targets {
            let Some(result) = report.results.iter().find(|r| &r.target == name) else {
                out.push(format!("{name}: gated but not in the report"));
                continue;
            };
            if let Some(error) = &result.error {
                out.push(format!("{name}: failed to run: {error}"));
                continue;
            }
            if let Some(max) = gate.max_read_p99_us
                && result.reads.p99_us > max
            {
                out.push(format!(
                    "{name}: read p99 {:.2} µs exceeds {max} µs",
                    result.reads.p99_us
                ));
            }
            if let Some(max) = gate.max_write_p99_us
                && result.writes.p99_us > max
            {
                out.push(format!(
                    "{name}: write p99 {:.2} µs exceeds {max} µs",
                    result.writes.p99_us
                ));
            }
            if let Some(max) = gate.max_errors
                && result.errors > max
            {
                out.push(format!("{name}: {} errors exceed {max}", result.errors));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::sample_report;

    fn gate(entries: &[(&str, TargetGate)]) -> Gate {
        Gate {
            targets: entries
                .iter()
                .map(|(name, g)| ((*name).to_string(), g.clone()))
                .collect(),
        }
    }

    #[test]
    fn a_report_inside_every_threshold_passes() {
        let g = gate(&[(
            "sundog-local",
            TargetGate {
                max_read_p99_us: Some(1_000.0),
                max_write_p99_us: Some(1_000.0),
                max_errors: Some(0),
            },
        )]);
        assert!(g.violations(&sample_report()).is_empty());
    }

    #[test]
    fn each_broken_threshold_is_named() {
        let g = gate(&[(
            "sundog-local",
            TargetGate {
                max_read_p99_us: Some(1.0),
                max_write_p99_us: Some(1.0),
                max_errors: None,
            },
        )]);
        let v = g.violations(&sample_report());
        assert_eq!(v.len(), 2, "{v:?}");
        assert!(v[0].contains("read p99 1.20"), "{v:?}");
        assert!(v[1].contains("write p99 3.00"), "{v:?}");
    }

    #[test]
    fn missing_and_failed_targets_are_violations() {
        let g = gate(&[
            ("sundog-replicated", TargetGate::default()),
            ("dragonfly", TargetGate::default()),
        ]);
        let v = g.violations(&sample_report());
        assert!(
            v.iter().any(|l| l.starts_with("dragonfly: failed to run")),
            "{v:?}"
        );
        assert!(
            v.iter()
                .any(|l| l.starts_with("sundog-replicated: gated but not")),
            "{v:?}"
        );
    }

    #[test]
    fn the_shipped_gate_file_parses_and_gates_the_in_ram_targets() {
        let text = include_str!("../../ops/bench-gate.json");
        let g: Gate = serde_json::from_str(text).expect("ops/bench-gate.json parses");
        for name in ["sundog-local", "sundog-replicated"] {
            let t = &g.targets[name];
            assert!(t.max_read_p99_us.is_some_and(|us| us <= 1_000.0), "{name}");
            assert!(t.max_write_p99_us.is_some_and(|us| us <= 1_000.0), "{name}");
        }
    }
}
