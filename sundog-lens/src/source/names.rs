//! Every `sundog_*` metric name the lens reads. Tests check each name against
//! `docs/src/operations.md`, against the registrations in `sundog/src` and
//! against the captured `fixtures/metrics.prom`, so a renamed or dropped metric
//! fails `cargo test`.

/// Gauge: live peers a node counts.
pub const LIVE_PEERS: &str = "sundog_live_peers";
/// Gauge: caches a node has open.
pub const OPEN_CACHES: &str = "sundog_open_caches";
/// Gauge, per cache: parts a node owns at any rank.
pub const OWNED_PARTS: &str = "sundog_owned_parts";
/// Gauge, per cache: buckets a node owns.
pub const OWNED_BUCKETS: &str = "sundog_owned_buckets";
/// Gauge, per cache: entries held.
pub const CACHE_ENTRIES: &str = "sundog_cache_entries";
/// Gauge, per cache: bytes held.
pub const CACHE_BYTES: &str = "sundog_cache_bytes";
/// Counter, per cache: reads that hit.
pub const CACHE_HITS: &str = "sundog_cache_hits_total";
/// Counter, per cache: reads that missed.
pub const CACHE_MISSES: &str = "sundog_cache_misses_total";
/// Counter, per cache and outcome: `fetch` calls (`local`, `remote`, `miss`, `error`).
pub const FETCH: &str = "sundog_fetch_total";
/// Counter, per cache: writes sent on to a part's owners.
pub const FORWARDED_WRITES: &str = "sundog_forwarded_writes_total";
/// Counter: frames sent to peers.
pub const FRAMES_SENT: &str = "sundog_frames_sent_total";
/// Counter: bytes sent to peers.
pub const BYTES_SENT: &str = "sundog_bytes_sent_total";
/// Gauge, per cache: frames waiting in the fan-out backlog.
pub const FAN_OUT_BACKLOG: &str = "sundog_fan_out_backlog";
/// Counter, per peer: frames dropped because the peer left the mesh.
pub const BACKLOG_DROPPED: &str = "sundog_backlog_dropped_total";
/// Counter, per peer: whole seconds writers waited for a full outbox.
pub const FAN_OUT_WAIT_SECONDS: &str = "sundog_fan_out_wait_seconds_total";
/// Counter, per cache and direction: parts pulled `in`, released `out` or `served`.
pub const REBALANCE_PARTS: &str = "sundog_rebalance_parts_total";
/// Counter, per cache: part pulls that timed out repeatedly.
pub const REBALANCE_PULL_TIMEOUTS: &str = "sundog_rebalance_pull_timeouts_total";
/// Counter, per cache: entries repaired by anti-entropy.
pub const AE_REPAIRED: &str = "sundog_ae_repaired_total";
/// Counter, per cache: records received in a state transfer.
pub const STATE_TRANSFER_RECORDS: &str = "sundog_state_transfer_records_total";
/// Counter, per cache: anti-entropy rounds a peer declined over a different view.
pub const STALE_VIEW: &str = "sundog_stale_view_total";
/// Gauge, per cache: bytes the spill tier uses.
pub const SPILL_BYTES_USED: &str = "sundog_spill_bytes_used";
/// Gauge, per cache: entries the spill tier holds.
pub const SPILL_ENTRIES: &str = "sundog_spill_entries";

/// Every name above.
pub const ALL: &[&str] = &[
    LIVE_PEERS,
    OPEN_CACHES,
    OWNED_PARTS,
    OWNED_BUCKETS,
    CACHE_ENTRIES,
    CACHE_BYTES,
    CACHE_HITS,
    CACHE_MISSES,
    FETCH,
    FORWARDED_WRITES,
    FRAMES_SENT,
    BYTES_SENT,
    FAN_OUT_BACKLOG,
    BACKLOG_DROPPED,
    FAN_OUT_WAIT_SECONDS,
    REBALANCE_PARTS,
    REBALANCE_PULL_TIMEOUTS,
    AE_REPAIRED,
    STATE_TRANSFER_RECORDS,
    STALE_VIEW,
    SPILL_BYTES_USED,
    SPILL_ENTRIES,
];

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn every_name_is_a_distinct_sundog_metric() {
        let distinct: HashSet<_> = ALL.iter().collect();
        assert_eq!(distinct.len(), ALL.len());
        for name in ALL {
            assert!(name.starts_with("sundog_"), "{name}");
            assert!(
                name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{name}"
            );
        }
    }

    #[test]
    fn every_name_appears_in_the_operations_guide() {
        let guide = include_str!("../../../docs/src/operations.md");
        for name in ALL {
            assert!(
                guide.contains(name),
                "{name} is not documented in docs/src/operations.md"
            );
        }
    }

    /// Names a healthy, spill-free node does not export in the capture: the
    /// exporter creates each series on its first event, and these need a
    /// resident-bytes ceiling, a dropped frame, a full outbox, a timed-out
    /// pull, a repair, a declined round or the spill tier.
    const ABSENT_FROM_THE_CAPTURE: &[&str] = &[
        CACHE_BYTES,
        BACKLOG_DROPPED,
        FAN_OUT_WAIT_SECONDS,
        REBALANCE_PULL_TIMEOUTS,
        AE_REPAIRED,
        STALE_VIEW,
        SPILL_BYTES_USED,
        SPILL_ENTRIES,
    ];

    fn capture_names() -> HashSet<String> {
        crate::source::expo::parse(include_str!("../../tests/fixtures/metrics.prom"))
            .into_iter()
            .map(|sample| sample.name)
            .collect()
    }

    #[test]
    fn the_capture_exports_every_name_a_healthy_node_exports() {
        let exported = capture_names();
        for name in ALL {
            if ABSENT_FROM_THE_CAPTURE.contains(name) {
                continue;
            }
            assert!(
                exported.contains(*name),
                "{name} is missing from tests/fixtures/metrics.prom: the exporter renamed \
                 it, or the capture needs retaking"
            );
        }
    }

    #[test]
    fn the_names_absent_from_the_capture_are_in_the_name_list_and_still_absent() {
        let exported = capture_names();
        for name in ABSENT_FROM_THE_CAPTURE {
            assert!(ALL.contains(name), "{name} is not a name the lens reads");
            assert!(
                !exported.contains(*name),
                "{name} is in the capture now: take it off ABSENT_FROM_THE_CAPTURE"
            );
        }
    }

    /// The `.rs` files under `dir`, recursively.
    fn rust_sources(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(dir).expect("the sundog source directory reads") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                found.extend(rust_sources(&path));
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                found.push(path);
            }
        }
        found
    }

    #[test]
    fn the_sundog_source_registers_every_name() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../sundog/src");
        let sources: Vec<String> = rust_sources(&root)
            .iter()
            .map(|path| std::fs::read_to_string(path).expect("a source file reads"))
            .collect();
        assert!(sources.len() > 10, "the walk found sundog's sources");
        for name in ALL {
            let literal = format!("\"{name}\"");
            assert!(
                sources.iter().any(|source| source.contains(&literal)),
                "{name} is not registered anywhere in sundog/src: the exporter renamed or \
                 dropped it"
            );
        }
    }
}
