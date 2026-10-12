//! Wait bounds and the Prometheus scrape helper the container scenarios share.

use crate::container_util::Node;
use std::time::Duration;

pub(crate) const CONVERGE_WAIT: Duration = Duration::from_secs(20);

/// Finds `metric{...,label="value",...} <number>` in Prometheus
/// text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Mirrors `tests/spill_replication.rs`'s own
/// copy, kept local since integration test binaries don't share code beyond
/// `mod container_util`.
fn metric_value(body: &str, metric: &str, label: (&str, &str)) -> Option<f64> {
    let wanted = format!("{}=\"{}\"", label.0, label.1);
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (labels, value) = rest.split_once('}')?;
        if !labels.split(',').any(|pair| pair == wanted) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

pub(crate) async fn scrape_metric(node: &Node, metric: &str, label: (&str, &str)) -> u64 {
    let value = node
        .metrics()
        .await
        .ok()
        .and_then(|body| metric_value(&body, metric, label));
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "every metric read here is a nonnegative counter or gauge"
    )]
    let count = value.unwrap_or(0.0).round() as u64;
    count
}
