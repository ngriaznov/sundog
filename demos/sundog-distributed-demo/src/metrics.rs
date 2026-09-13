//! In-process metrics capture for a headless run: installs the `metrics`
//! recorder every node in this process reports into, and reduces its
//! Prometheus text rendering to per-metric totals and a one-line summary.
//!
//! Every in-process node shares one recorder and names its cache the same,
//! so a counter sums across nodes while a gauge shows whichever node set it
//! last. The summary line therefore pairs the gauges with the demo's own
//! per-node entry counts and the spill directories' sizes on disk.

use std::collections::BTreeMap;
use std::path::Path;

/// The process-wide recorder handle, installed once before any node opens.
pub(crate) struct Metrics {
    #[cfg(feature = "prometheus")]
    handle: sundog::PrometheusHandle,
}

impl Metrics {
    /// Installs the recorder. Call before the first node opens: a metric
    /// registered earlier stays on the no-op recorder and never reports.
    ///
    /// # Errors
    ///
    /// Returns an error if the recorder is already installed, or, in a
    /// build without the `prometheus` feature, always.
    pub(crate) fn install() -> anyhow::Result<Self> {
        #[cfg(feature = "prometheus")]
        {
            use anyhow::Context as _;
            let handle =
                sundog::prometheus_handle().context("failed to install the metrics recorder")?;
            Ok(Self { handle })
        }
        #[cfg(not(feature = "prometheus"))]
        {
            anyhow::bail!("--metrics needs the demo built with --features prometheus")
        }
    }

    /// The recorder's current Prometheus text rendering.
    #[must_use]
    #[cfg_attr(
        not(feature = "prometheus"),
        allow(
            clippy::unused_self,
            reason = "no recorder to read without the feature"
        )
    )]
    pub(crate) fn render(&self) -> String {
        #[cfg(feature = "prometheus")]
        {
            self.handle.render()
        }
        #[cfg(not(feature = "prometheus"))]
        {
            String::new()
        }
    }

    /// Every `sundog_*` metric summed across its label sets.
    #[must_use]
    pub(crate) fn totals(&self) -> BTreeMap<String, f64> {
        totals(&self.render())
    }

    /// Every `sundog_*` sample line, sorted, for a milestone dump.
    #[must_use]
    pub(crate) fn dump(&self) -> String {
        let mut lines: Vec<String> = self
            .render()
            .lines()
            .filter(|line| line.starts_with("sundog_"))
            .map(str::to_owned)
            .collect();
        lines.sort_unstable();
        lines.join("\n")
    }
}

/// Sums every `sundog_*` sample in Prometheus text-exposition `body` by
/// metric name, across label sets. `#` comment lines and other metrics are
/// skipped.
#[must_use]
pub(crate) fn totals(body: &str) -> BTreeMap<String, f64> {
    let mut sums = BTreeMap::new();
    for line in body.lines() {
        let Some(rest) = line.strip_prefix("sundog_") else {
            continue;
        };
        let name_end = rest.find(['{', ' ']).unwrap_or(rest.len());
        let name = format!("sundog_{}", &rest[..name_end]);
        let Some(value) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) else {
            continue;
        };
        *sums.entry(name).or_insert(0.0) += value;
    }
    sums
}

/// The metrics a spill run is watched by, in the order the summary line
/// prints them, with the short label each carries there.
const SUMMARY: &[(&str, &str)] = &[
    ("sundog_spill_entries", "spill_entries"),
    ("sundog_spill_bytes_used", "spill_bytes"),
    ("sundog_spill_writes_total", "spill_writes"),
    ("sundog_spill_reads_total", "spill_reads"),
    ("sundog_spill_promotions_total", "promotions"),
    ("sundog_spill_dropped_total", "spill_dropped"),
    ("sundog_spill_region_reclaims_total", "reclaims"),
    ("sundog_state_transfer_records_total", "st_records"),
    ("sundog_rebalance_buckets_total", "rebalance"),
    ("sundog_ae_repaired_total", "ae_repaired"),
    ("sundog_forwarded_writes_total", "forwarded"),
    ("sundog_cache_hits_total", "hits"),
    ("sundog_cache_misses_total", "misses"),
];

/// One line of the watched totals, `label=value` pairs in [`SUMMARY`]
/// order; a metric nothing has reported yet prints as `0`.
#[must_use]
pub(crate) fn summary_line(totals: &BTreeMap<String, f64>) -> String {
    SUMMARY
        .iter()
        .map(|(metric, label)| {
            let value = totals.get(*metric).copied().unwrap_or(0.0);
            format!("{label}={value:.0}")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Bytes on disk of every regular file under `dir`, recursively; `0` when
/// the directory does not exist yet. On Unix this counts allocated blocks,
/// so a preallocated but still empty region file counts what the
/// filesystem has really given it, not its nominal length.
#[must_use]
pub(crate) fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                dir_bytes(&path)
            } else {
                entry.metadata().map_or(0, |m| allocated_bytes(&m))
            }
        })
        .sum()
}

#[cfg(unix)]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt as _;
    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}

/// `bytes` as a human-scaled string.
#[must_use]
pub(crate) fn format_bytes(bytes: u64) -> String {
    #[allow(clippy::cast_precision_loss, reason = "display only")]
    let b = bytes as f64;
    if bytes >= 1 << 30 {
        format!("{:.2} GiB", b / f64::from(1u32 << 30))
    } else if bytes >= 1 << 20 {
        format!("{:.1} MiB", b / f64::from(1u32 << 20))
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = "# HELP sundog_spill_writes_total x\n\
        # TYPE sundog_spill_writes_total counter\n\
        sundog_spill_writes_total{cache=\"demo\"} 12\n\
        sundog_spill_writes_total{cache=\"other\"} 3\n\
        sundog_live_peers 2\n\
        other_metric{cache=\"demo\"} 99\n\
        sundog_cache_entries{cache=\"demo\"} 4.5\n";

    #[test]
    fn totals_sum_across_label_sets_and_skip_foreign_lines() {
        let sums = totals(BODY);
        assert_eq!(sums.get("sundog_spill_writes_total"), Some(&15.0));
        assert_eq!(sums.get("sundog_live_peers"), Some(&2.0));
        assert_eq!(sums.get("sundog_cache_entries"), Some(&4.5));
        assert!(!sums.contains_key("other_metric"));
        assert_eq!(sums.len(), 3);
    }

    #[test]
    fn summary_line_prints_every_watched_metric_zero_filled() {
        let line = summary_line(&totals(BODY));
        assert!(
            line.starts_with("spill_entries=0 spill_bytes=0 spill_writes=15 "),
            "{line}"
        );
        assert!(line.ends_with(" hits=0 misses=0"), "{line}");
    }

    #[test]
    fn dir_bytes_sums_files_recursively_and_tolerates_a_missing_dir() {
        let root =
            std::env::temp_dir().join(format!("sundog-demo-dir-bytes-{}", std::process::id()));
        let nested = root.join("a").join("b");
        std::fs::create_dir_all(&nested).expect("temp dirs create");
        std::fs::write(root.join("x"), [1u8; 10]).expect("file writes");
        std::fs::write(nested.join("y"), [1u8; 5]).expect("file writes");
        let total = dir_bytes(&root);
        // Allocated blocks round each file up to the filesystem's block
        // size, so the sum is at least the bytes written and covers both
        // files.
        assert!(total >= 15, "{total}");
        assert_eq!(dir_bytes(&root.join("missing")), 0);
        std::fs::remove_dir_all(&root).expect("temp dir removes");
    }

    #[test]
    fn bytes_format_by_scale() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(3 << 20), "3.0 MiB");
        assert_eq!(format_bytes(5 << 30), "5.00 GiB");
    }
}
