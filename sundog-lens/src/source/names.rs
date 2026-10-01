//! Every `sundog_*` metric name the lens reads. A test checks each against
//! `docs/src/operations.md`, so a renamed metric fails the build.

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
}
