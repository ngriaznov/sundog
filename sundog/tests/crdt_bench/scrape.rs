//! The process-global Prometheus handle and the counter-family snapshot helpers built on it.

#[cfg(feature = "prometheus")]
use std::sync::OnceLock;
#[cfg(feature = "prometheus")]
use sundog::PrometheusHandle;

// ---------------------------------------------------------------------
// `sundog_ae_repaired_total` scrape, cfg-gated on `prometheus`: this
// binary's one claim on the process-global Prometheus recorder slot,
// installed lazily on first use, before the first benchmark in a run opens
// its first cache, a cache binds its per-cache metric handles when it
// opens, so the recorder has to exist first or those handles stay on the
// no-op recorder and every scrape reads back zero.
// ---------------------------------------------------------------------

#[cfg(feature = "prometheus")]
static METRICS_HANDLE: OnceLock<Option<PrometheusHandle>> = OnceLock::new();

#[cfg(feature = "prometheus")]
pub(crate) fn metrics_handle() -> Option<&'static PrometheusHandle> {
    METRICS_HANDLE
        .get_or_init(|| sundog::prometheus_handle().ok())
        .as_ref()
}

/// Finds `metric{label1="value1",...} <number>` in Prometheus
/// text-exposition `body`, tolerant of label ordering and
/// integer-vs-float rendering. Mirrors `spill_bench.rs`'s own
/// `scraped_metric`, kept local since integration test binaries don't
/// share code beyond `mod common`.
#[cfg(feature = "prometheus")]
fn scraped_metric(body: &str, metric: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let wanted: Vec<String> = labels
        .iter()
        .map(|&(k, v)| format!("{k}=\"{v}\""))
        .collect();
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(metric)?;
        let rest = rest.strip_prefix('{')?;
        let (line_labels, value) = rest.split_once('}')?;
        let line_labels: Vec<&str> = line_labels.split(',').collect();
        if !wanted.iter().all(|w| line_labels.contains(&w.as_str())) {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

/// Defines a Prometheus counter family's `_total` scrape fn plus its
/// `_snapshot` and `_field` `#[cfg(feature = "prometheus")]`/
/// `#[cfg(not(...))]` pairs. The `_snapshot` fn re-reads `_total` for a
/// before/after delta around one repetition's writes (or join, or
/// compaction wait), the pattern every scenario uses for
/// `frames_sent_total`/`bytes_sent_total` too, since every repetition of a
/// scenario shares one process-global counter for a given `cache_name`.
/// The `_field` fn renders a `_snapshot` delta's median (across a
/// scenario's repetitions) as a `BENCH`-line field, empty under a build
/// without `prometheus`. A second arm covers `sketch_outcome`, whose metric
/// carries a second label and whose field renders two counts as three
/// named sub-fields instead of one.
macro_rules! metric_family {
    (
        metric: $metric:literal,
        label: $label:literal,
        total_fn: $total_fn:ident,
        total_doc: $total_doc:literal,
        snapshot_fn: $snapshot_fn:ident,
        snapshot_doc: $snapshot_doc:literal,
        field_fn: $field_fn:ident,
        field_name: $field_name:literal,
        field_doc: $field_doc:literal $(,)?
    ) => {
        #[doc = $total_doc]
        #[cfg(feature = "prometheus")]
        fn $total_fn(cache_name: &str) -> u64 {
            let value = metrics_handle()
                .and_then(|h| scraped_metric(&h.render(), $metric, &[($label, cache_name)]));
            #[allow(
                clippy::cast_sign_loss,
                clippy::cast_possible_truncation,
                reason = "a Prometheus counter is nonnegative"
            )]
            let count = value.unwrap_or(0.0).round() as u64;
            count
        }

        #[doc = $snapshot_doc]
        #[cfg(feature = "prometheus")]
        pub(crate) fn $snapshot_fn(cache_name: &str) -> u64 {
            $total_fn(cache_name)
        }

        #[cfg(not(feature = "prometheus"))]
        pub(crate) fn $snapshot_fn(_cache_name: &str) -> u64 {
            0
        }

        #[doc = $field_doc]
        #[cfg(feature = "prometheus")]
        pub(crate) fn $field_fn(median: u64) -> String {
            format!(concat!(" ", $field_name, "={}"), median)
        }

        #[cfg(not(feature = "prometheus"))]
        pub(crate) fn $field_fn(_median: u64) -> String {
            String::new()
        }
    };

    (
        metric: $metric:literal,
        labels: ($label1:literal, $label2:literal),
        total_fn: $total_fn:ident,
        total_doc: $total_doc:literal,
        snapshot_fn: $snapshot_fn:ident,
        snapshot_doc: $snapshot_doc:literal,
        field_fn: $field_fn:ident,
        field_doc: $field_doc:literal $(,)?
    ) => {
        #[doc = $total_doc]
        #[cfg(feature = "prometheus")]
        fn $total_fn(cache_name: &str, outcome: &str) -> u64 {
            let value = metrics_handle().and_then(|h| {
                scraped_metric(
                    &h.render(),
                    $metric,
                    &[($label1, cache_name), ($label2, outcome)],
                )
            });
            #[allow(
                clippy::cast_sign_loss,
                clippy::cast_possible_truncation,
                reason = "a Prometheus counter is nonnegative"
            )]
            let count = value.unwrap_or(0.0).round() as u64;
            count
        }

        #[doc = $snapshot_doc]
        #[cfg(feature = "prometheus")]
        pub(crate) fn $snapshot_fn(cache_name: &str, outcome: &str) -> u64 {
            $total_fn(cache_name, outcome)
        }

        #[cfg(not(feature = "prometheus"))]
        pub(crate) fn $snapshot_fn(_cache_name: &str, _outcome: &str) -> u64 {
            0
        }

        #[doc = $field_doc]
        #[cfg(feature = "prometheus")]
        pub(crate) fn $field_fn(decoded: u64, fallback: u64) -> String {
            format!(
                " sketch_rounds={} sketch_peeled={decoded} sketch_fallback={fallback}",
                decoded + fallback
            )
        }

        #[cfg(not(feature = "prometheus"))]
        pub(crate) fn $field_fn(_decoded: u64, _fallback: u64) -> String {
            String::new()
        }
    };
}

metric_family! {
    metric: "sundog_ae_repaired_total",
    label: "cache",
    total_fn: ae_repaired_total,
    total_doc: "The current value of `sundog_ae_repaired_total{cache=cache_name}`, or 0 \
                if the recorder never installed or the counter never incremented.",
    snapshot_fn: ae_repaired_snapshot,
    snapshot_doc: "A per-repetition snapshot of [`ae_repaired_total`], for a before/after \
                   delta around one repetition's writes and convergence wait.",
    field_fn: ae_repaired_field,
    field_name: "ae_repaired_total",
    field_doc: "Renders `median`, the median across a scenario's repetitions of each one's \
                own before/after [`ae_repaired_snapshot`] delta, as a `BENCH`-line field.",
}

/// [`ae_repaired_field`]'s counterpart for a `BENCH` line reporting more
/// than one cache's repair count under distinct labels.
#[cfg(feature = "prometheus")]
pub(crate) fn ae_repaired_field_named(label: &str, median: u64) -> String {
    format!(" ae_repaired_total_{label}={median}")
}

#[cfg(not(feature = "prometheus"))]
pub(crate) fn ae_repaired_field_named(_label: &str, _median: u64) -> String {
    String::new()
}

metric_family! {
    metric: "sundog_ae_sketch_total",
    labels: ("cache", "outcome"),
    total_fn: sketch_outcome_total,
    total_doc: "The current value of `sundog_ae_sketch_total{cache=cache_name,outcome=outcome}`, \
                scenario 13's own peel-success (`outcome=\"decoded\"`) and fallback \
                (`outcome=\"fallback\"`) counts, the metric \
                `cluster::anti_entropy::handle_sketch_mismatch` already emits, reused here \
                rather than adding a new one. Returns 0 if the recorder never installed or \
                the counter never incremented.",
    snapshot_fn: sketch_outcome_snapshot,
    snapshot_doc: "[`ae_repaired_snapshot`]'s counterpart for [`sketch_outcome_total`], for a \
                   before/after delta around one repetition's writes and convergence wait.",
    field_fn: sketch_outcome_field,
    field_doc: "Renders scenario 13's `sketch_rounds`/`sketch_peeled`/`sketch_fallback` \
                fields. `sketch_rounds` is `decoded + fallback`, the number of times this \
                scenario's buckets answer a mismatch through the sketch mechanism, \
                successfully or not.",
}

metric_family! {
    metric: "sundog_state_transfer_records_total",
    label: "cache",
    total_fn: entries_received_total,
    total_doc: "The current value of `sundog_state_transfer_records_total{cache=cache_name}`, \
                the count of entries a state transfer applies to `cache_name` on this \
                process, incremented once per donor a `Mode::Replicated` cache opens \
                against, or 0 if the recorder never installed or no transfer has completed \
                yet.",
    snapshot_fn: entries_received_snapshot,
    snapshot_doc: "[`ae_repaired_snapshot`]'s counterpart for [`entries_received_total`], for \
                   a before/after delta around scenario 7's join.",
    field_fn: entries_received_field,
    field_name: "entries_received",
    field_doc: "[`ae_repaired_field`]'s counterpart for a `BENCH` line reporting scenario 7's \
                join-time [`entries_received_snapshot`] delta.",
}

metric_family! {
    metric: "sundog_crdt_retired_writers_total",
    label: "cache",
    total_fn: crdt_retired_writers_total,
    total_doc: "The current value of `sundog_crdt_retired_writers_total{cache=cache_name}`, \
                the per-writer retirement counter incremented once per `WriterId` the \
                compaction sweep folds out of `cache_name`'s live state, or 0 if the \
                recorder never installed or nothing has retired yet.",
    snapshot_fn: crdt_retired_writers_snapshot,
    snapshot_doc: "[`ae_repaired_snapshot`]'s counterpart for [`crdt_retired_writers_total`], \
                   for a before/after delta around the churn scenario's compaction wait.",
    field_fn: crdt_retired_writers_field,
    field_name: "crdt_retired_writers_total",
    field_doc: "[`ae_repaired_field`]'s counterpart for the churn scenario's \
                [`crdt_retired_writers_snapshot`] delta.",
}

metric_family! {
    metric: "sundog_crdt_compactions_total",
    label: "cache",
    total_fn: crdt_compactions_total,
    total_doc: "The current value of `sundog_crdt_compactions_total{cache=cache_name}`, the \
                per-tick record-compaction counter, or 0 if the recorder never installed or \
                nothing has compacted yet.",
    snapshot_fn: crdt_compactions_snapshot,
    snapshot_doc: "[`ae_repaired_snapshot`]'s counterpart for [`crdt_compactions_total`], for \
                   a before/after delta around the churn scenario's compaction wait.",
    field_fn: crdt_compactions_field,
    field_name: "crdt_compactions_total",
    field_doc: "[`ae_repaired_field`]'s counterpart for the churn scenario's \
                [`crdt_compactions_snapshot`] delta.",
}
