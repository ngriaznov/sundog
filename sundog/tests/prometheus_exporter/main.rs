//! The `prometheus` feature installs a real Prometheus recorder, either
//! serving `GET /metrics` itself
//! ([`sundog::ClusterBuilder::prometheus_listen`]) or handing back a
//! [`sundog::PrometheusHandle`] for a caller's own HTTP stack
//! ([`sundog::prometheus_handle`]).
//! `metrics::set_global_recorder` is a single process-global slot: whichever
//! test in this binary installs a recorder first wins it, so the second
//! test below tolerates losing that race instead of assuming it always
//! runs first.

#![cfg(all(feature = "prometheus", not(feature = "sim")))]

#[path = "../common/mod.rs"]
mod common;
mod pins;
mod seeds;
#[cfg(feature = "spill")]
mod spill;
mod support;

use crate::pins::{
    SKEW_HOLD_PIN_CAPACITY, TOMBSTONE_PIN_COUNT, clock_skew_pin_metrics, count_hits_and_misses,
    fan_out_wait_timeout_pin_metrics, loader_calls_pin_metric, memory_ceiling_pin_metrics,
    read_durations_pin_metrics, refresh_hints_pin_metric, refresh_outcomes_pin_metrics,
    skew_hold_pin_bytes, skew_hold_pin_metrics, tombstone_ignored_pin_metrics,
    tombstone_pin_metrics,
};
use crate::seeds::{
    seed_crdt_compaction_metrics, seed_distributed_metrics, seed_part_mismatch,
    seed_pull_timeout_metric, seed_sketch_mismatch,
};
#[cfg(feature = "spill")]
use crate::spill::{
    disk_error_and_reserve_wait_pin_metrics, reserve_timeout_pin_metric, spill_reopen_pins_metrics,
    spill_writes_and_promotes_pin_metrics,
};
use crate::support::{
    node_config, reserve_tcp_addr, scrape_metrics, scrape_status, scraped_metric_value,
};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use sundog::{Cluster, Mode};

#[allow(
    clippy::too_many_lines,
    reason = "folds in the spill metrics pin behind feature = \"spill\"; see \
              spill_writes_and_promotes_pin_metrics's own doc for why it can't be a separate \
              #[tokio::test]"
)]
// Multi-threaded: reserve_timeout_pin_metric's genuine timeout race needs
// the flusher's OS thread and this test's tasks to run with real
// parallelism, or the flusher tends to keep `admit` refilled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_serves_sundog_metrics_after_cache_ops() {
    let metrics_addr = reserve_tcp_addr().await;

    let cluster = Cluster::builder("it-prometheus-exporter")
        .seeds(std::iter::empty())
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
        .prometheus_listen(metrics_addr)
        .build()
        .await
        .expect("cluster builds with a prometheus listener");
    // A peer for `users` to replicate to and reconcile against; only the
    // first node serves metrics, since the recorder is process-global.
    let peer = Cluster::builder("it-prometheus-exporter")
        .seeds([cluster.local_gossip_addr()])
        .config(node_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
        .build()
        .await
        .expect("peer builds");
    common::wait_for_peer_count(&cluster, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&peer, 1, Duration::from_secs(15)).await;

    seed_sketch_mismatch(&cluster, &peer).await;
    seed_part_mismatch(&cluster, &peer).await;
    count_hits_and_misses(&cluster).await;
    read_durations_pin_metrics(&cluster).await;
    fan_out_wait_timeout_pin_metrics().await;
    clock_skew_pin_metrics().await;
    tombstone_ignored_pin_metrics().await;
    let ceiling_pin_bytes = f64::from(
        u32::try_from(memory_ceiling_pin_metrics(&cluster).await).expect("one entry's bytes fit"),
    );
    assert!(ceiling_pin_bytes > 0.0);
    loader_calls_pin_metric(&cluster).await;
    let tombstone_node = tombstone_pin_metrics().await;
    let (skew_hold_node, skew_hold_peer) = skew_hold_pin_metrics().await;
    refresh_outcomes_pin_metrics(&cluster).await;
    refresh_hints_pin_metric(&cluster, &peer).await;
    #[cfg(feature = "spill")]
    let mut spill_dirs = spill_writes_and_promotes_pin_metrics(&cluster).await;
    #[cfg(feature = "spill")]
    let (disk_error_immutable, disk_error_dropped) = {
        let (dir, immutable, observed) =
            disk_error_and_reserve_wait_pin_metrics(&cluster, metrics_addr).await;
        spill_dirs.push(dir);
        (immutable, observed)
    };
    #[cfg(feature = "spill")]
    {
        spill_dirs.push(reserve_timeout_pin_metric().await);
    }
    #[cfg(feature = "spill")]
    spill_dirs.extend(spill_reopen_pins_metrics(&cluster).await);
    // Independent of `peer`/`third`/`fourth`: fully retires its own
    // scenario-local nodes before returning.
    seed_crdt_compaction_metrics(&cluster, metrics_addr).await;
    // Also independent: donor and victim are both shut down before it returns.
    seed_pull_timeout_metric(cluster.local_gossip_addr(), metrics_addr).await;
    // Runs last: it shuts down two of its own scenario-local nodes once it
    // is done with them, and `peer` isn't touched by anything after it.
    seed_distributed_metrics(&cluster, &peer).await;

    // `sundog_open_caches` comes from a periodic background routine and the
    // sketch/parts counters from an anti-entropy round, so poll until every
    // metric checked below has been published once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let body = loop {
        if let Some(body) = scrape_metrics(metrics_addr).await
            && body.contains("sundog_open_caches")
            && body.contains("sundog_live_peers")
            && scraped_metric_value(&body, "sundog_ae_parts_total", &[("outcome", "listing")])
                .is_some()
            && body.contains("sundog_cache_entries")
            // Refreshed every 5 seconds, so it may first read the open-time 0.
            && scraped_metric_value(&body, "sundog_cache_bytes", &[("cache", "ceiling-pin")])
                == Some(ceiling_pin_bytes)
            && scraped_metric_value(&body, "sundog_ae_sketch_total", &[("outcome", "decoded")])
                .is_some()
            && scraped_metric_value(&body, "sundog_tombstones", &[("cache", "tombstone-pin")])
                == Some(TOMBSTONE_PIN_COUNT)
            // The raw peer's forwards are applied in order, so the horizon
            // drop at the end of its stream is the last to count.
            && scraped_metric_value(
                &body,
                "sundog_clock_skew_hold_dropped_total",
                &[("cache", "skew-hold-pin"), ("reason", "horizon")],
            ) == Some(1.0)
            && (cfg!(not(feature = "spill"))
                || scraped_metric_value(
                    &body,
                    "sundog_spill_writes_total",
                    &[("cache", "spilled")],
                )
                .is_some_and(|writes| writes >= 2.0))
        {
            break body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "metrics endpoint never served sundog_open_caches, sundog_live_peers, \
             sundog_cache_entries, sundog_cache_bytes, sundog_tombstones, a decoded sundog_ae_sketch_total, and (feature = \"spill\") \
             sundog_spill_writes_total within the bound"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_hits_total", &[("cache", "counted")]),
        Some(8.0),
        "expected 8 hits on the 'counted' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_misses_total", &[("cache", "counted")]),
        Some(5.0),
        "expected 5 misses on the 'counted' cache; got body:\n{body}"
    );
    // `explain` of `counted` counts no fetch: the cache never fetches, so it
    // exports neither a fetch count nor a fetch timing.
    for metric in ["sundog_fetch_total", "sundog_fetch_duration_seconds_count"] {
        assert_eq!(
            scraped_metric_value(&body, metric, &[("cache", "counted")]),
            None,
            "explain counts no fetch on the 'counted' cache; got body:\n{body}"
        );
    }
    for outcome in ["hit", "miss"] {
        let labels = [("cache", "timed-reads"), ("outcome", outcome)];
        let timed = scraped_metric_value(&body, "sundog_read_duration_seconds_count", &labels);
        assert!(
            timed.is_some_and(|count| (2.0..=6.0).contains(&count)),
            "768 reads with outcome {outcome} time two to six, got {timed:?}; body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_read_duration_seconds_bucket",
                &[labels[0], labels[1], ("le", "10")]
            ),
            timed,
            "every timed {outcome} lands under 10s; got body:\n{body}"
        );
    }
    assert!(
        scraped_metric_value(&body, "sundog_cache_entries", &[("cache", "counted")]).is_some(),
        "expected a sundog_cache_entries line for the 'counted' cache; got body:\n{body}"
    );

    // sundog_tombstones: pinned by tombstone_pin_metrics, two deletes and one
    // swept expired entry (the wait above), beside two live entries that
    // `sundog_cache_entries` counts.
    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_entries", &[("cache", "tombstone-pin")]),
        Some(2.0),
        "expected two live entries on the 'tombstone-pin' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_tombstones", &[("cache", "counted")]),
        Some(0.0),
        "a cache that deleted nothing holds no tombstone; got body:\n{body}"
    );

    // An uncapped cache counts no bytes, so it exports neither series.
    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_bytes", &[("cache", "counted")]),
        None,
        "an uncapped cache publishes no byte gauge; got body:\n{body}"
    );
    assert!(
        !body.contains("sundog_ceiling_refusals_total{cache=\"counted\""),
        "an uncapped cache registers no refusal counters; got body:\n{body}"
    );
    // sundog_ceiling_refusals_total: pinned by memory_ceiling_pin_metrics,
    // two refused writes and one uncached fill. sundog_cache_bytes is
    // pinned to Cache::resident_bytes by the wait above.
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_ceiling_refusals_total",
            &[("cache", "ceiling-pin"), ("kind", "write")]
        ),
        Some(2.0),
        "expected two refused writes on the 'ceiling-pin' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_ceiling_refusals_total",
            &[("cache", "ceiling-pin"), ("kind", "fill")]
        ),
        Some(1.0),
        "expected one uncached fill on the 'ceiling-pin' cache; got body:\n{body}"
    );
    // sundog_loader_calls_total: pinned by loader_calls_pin_metric and
    // refresh_outcomes_pin_metrics; a cache without a loader has no series.
    assert_eq!(
        scraped_metric_value(&body, "sundog_loader_calls_total", &[("cache", "load-pin")]),
        Some(2.0),
        "expected two loader calls on the 'load-pin' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_loader_calls_total",
            &[("cache", "refresh-pin")]
        ),
        Some(4.0),
        "expected four loader calls on the 'refresh-pin' cache; got body:\n{body}"
    );
    assert!(
        !body.contains("sundog_loader_calls_total{cache=\"counted\""),
        "a cache with no loader counts no loader calls; got body:\n{body}"
    );
    // sundog_refreshes_total: pinned by refresh_outcomes_pin_metrics, one
    // reload of each outcome.
    for outcome in ["loaded", "absent", "failed"] {
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_refreshes_total",
                &[("cache", "refresh-pin"), ("outcome", outcome)]
            ),
            Some(1.0),
            "expected one {outcome} reload on the 'refresh-pin' cache; got body:\n{body}"
        );
    }
    // sundog_refresh_hints_sent_total: pinned by refresh_hints_pin_metric,
    // one hint for each of 16 keys, from whichever node is not its refresher.
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_refresh_hints_sent_total",
            &[("cache", "refresh-hint-pin")]
        ),
        Some(16.0),
        "expected sixteen refresh hints on the 'refresh-hint-pin' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_refreshes_total",
            &[("cache", "refresh-hint-pin"), ("outcome", "loaded")]
        )
        .map(|reloads| reloads >= 16.0),
        Some(true),
        "every key reloads on its refresher; got body:\n{body}"
    );
    // sundog_clock_skew_rejected_total: pinned by the bare-Shard scenario
    // above, one record an hour ahead refused and one a second ahead applied.
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_clock_skew_rejected_total",
            &[("cache", "clock-skew-pin")]
        ),
        Some(1.0),
        "expected exactly one refused record on the 'clock-skew-pin' cache; got body:\n{body}"
    );
    // sundog_tombstones_ignored_total: pinned by the bare-Shard scenario
    // above, three deletes stamped an hour ago ignored and one stamped a
    // second ago stored.
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_tombstones_ignored_total",
            &[("cache", "tombstone-ignored-pin")]
        ),
        Some(3.0),
        "expected exactly three ignored tombstones on the 'tombstone-ignored-pin' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_tombstones_ignored_total",
            &[("cache", "tombstone-pin")]
        ),
        None,
        "a cache that ignored nothing exports no series; got body:\n{body}"
    );
    // The skew hold: pinned by skew_hold_pin_metrics. A raw peer that is no
    // owner forwards 16,385 distinct keys stamped an hour ahead and one
    // stamped three days ahead to a cache that owns every part.
    let skew_hold_cache = [("cache", "skew-hold-pin")];
    assert_eq!(
        scraped_metric_value(&body, "sundog_clock_skew_held", &skew_hold_cache),
        Some(f64::from(SKEW_HOLD_PIN_CAPACITY)),
        "the hold keeps its capacity of records; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_clock_skew_held_bytes", &skew_hold_cache),
        Some(skew_hold_pin_bytes()),
        "each held record costs its key, its value and 64 bytes; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_clock_skew_hold_dropped_total",
            &[("cache", "skew-hold-pin"), ("reason", "full")]
        ),
        Some(1.0),
        "the record past the cap is dropped as full; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_clock_skew_hold_dropped_total",
            &[("cache", "skew-hold-pin"), ("reason", "horizon")]
        ),
        Some(1.0),
        "the record three days ahead is dropped as past the horizon; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_clock_skew_rejected_total", &skew_hold_cache),
        Some(f64::from(SKEW_HOLD_PIN_CAPACITY) + 2.0),
        "every forwarded record is refused once, held or not; got body:\n{body}"
    );
    // sundog_fan_out_wait_timeouts_total / sundog_fan_out_backlog: pinned
    // by the bare-Shard scenario above; nothing drains it, so both keys
    // sit in the backlog against a capacity of one.
    assert!(
        scraped_metric_value(
            &body,
            "sundog_fan_out_wait_timeouts_total",
            &[("cache", "fan-out-timeout-pin")]
        )
        .is_some_and(|count| count >= 1.0),
        "expected at least one genuine fan-out wait timeout on the 'fan-out-timeout-pin' \
         cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_fan_out_backlog",
            &[("cache", "fan-out-timeout-pin")]
        ),
        Some(2.0),
        "both keys landed over the configured capacity of one, with nothing to drain them; \
         got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_ae_sketch_total", &[("cache", "users")])
            .is_some_and(|decoded| decoded >= 1.0),
        "expected at least one decoded sketch on the 'users' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_ae_parts_total", &[("cache", "parts")])
            .is_some_and(|listings| listings >= 1.0),
        "expected at least one part listing on the 'parts' cache; got body:\n{body}"
    );

    let owned_parts = scraped_metric_value(&body, "sundog_owned_parts", &[("cache", "prices")]);
    assert!(
        owned_parts.is_some_and(|owned| owned > 0.0),
        "expected a positive sundog_owned_parts for the distributed 'prices' cache; got \
         body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_owned_buckets", &[("cache", "prices")]),
        owned_parts.map(|parts| parts / 64.0),
        "sundog_owned_buckets is the owned part count over 64; got body:\n{body}"
    );
    for outcome in ["local", "remote", "miss", "error"] {
        assert!(
            scraped_metric_value(
                &body,
                "sundog_fetch_total",
                &[("cache", "prices"), ("outcome", outcome)]
            )
            .is_some_and(|count| count >= 1.0),
            "expected sundog_fetch_total{{cache=\"prices\",outcome=\"{outcome}\"}} >= 1; got \
             body:\n{body}"
        );
    }
    // Every remote, miss and error fetch here asks an owner (the error
    // one dials owners that crashed), so each is timed.
    for outcome in ["remote", "miss", "error"] {
        let labels = [("cache", "prices"), ("outcome", outcome)];
        assert_eq!(
            scraped_metric_value(&body, "sundog_fetch_duration_seconds_count", &labels),
            scraped_metric_value(&body, "sundog_fetch_total", &labels),
            "every {outcome} fetch that asked an owner is timed; got body:\n{body}"
        );
    }
    assert!(
        scraped_metric_value(
            &body,
            "sundog_forwarded_writes_total",
            &[("cache", "prices")]
        )
        .is_some_and(|count| count >= 1.0),
        "expected at least one forwarded write on the 'prices' cache; got body:\n{body}"
    );
    for direction in ["in", "out", "served"] {
        assert!(
            scraped_metric_value(
                &body,
                "sundog_rebalance_parts_total",
                &[("cache", "prices"), ("direction", direction)]
            )
            .is_some_and(|count| count >= 1.0),
            "expected sundog_rebalance_parts_total{{cache=\"prices\",direction=\"{direction}\"}} \
             >= 1; got body:\n{body}"
        );
    }

    // `seed_pull_timeout_metric`'s victim gives up on one warm-up pass,
    // ever, once its ownership view moves on and its cache is warm.
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_rebalance_pull_timeouts_total",
            &[("cache", "delayed")]
        ),
        Some(1.0),
        "expected exactly one rebalance pull timeout on the 'delayed' cache; got body:\n{body}"
    );

    // `seed_crdt_compaction_metrics` retires exactly one dead writer, ever
    // (stage-one retirement never calls `retire` on it again once moved),
    // so this pins exactly. `sundog_crdt_compactions_total` only gets a
    // lower bound: that same writer's retired slot may or may not have
    // already aged into a second, real stage-two fold by the time this
    // scrape lands (see `seed_crdt_compaction_metrics`'s own doc).
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_crdt_retired_writers_total",
            &[("cache", "counters")]
        ),
        Some(1.0),
        "expected exactly one retired writer on the 'counters' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(
            &body,
            "sundog_crdt_compactions_total",
            &[("cache", "counters")]
        )
        .is_some_and(|count| count >= 1.0),
        "expected at least one crdt compaction on the 'counters' cache; got body:\n{body}"
    );

    #[cfg(feature = "spill")]
    {
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_writes_total", &[("cache", "spilled")]),
            Some(2.0),
            "expected exactly two spill installs, the second making room for the promotion; \
             got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reads_total",
                &[("cache", "spilled"), ("outcome", "hit")]
            ),
            Some(1.0),
            "expected exactly one disk hit; got body:\n{body}"
        );
        let disk_reads: f64 = ["hit", "stale", "io_error"]
            .iter()
            .filter_map(|&outcome| {
                scraped_metric_value(
                    &body,
                    "sundog_spill_reads_total",
                    &[("cache", "spilled"), ("outcome", outcome)],
                )
            })
            .sum();
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_read_duration_seconds_count",
                &[("cache", "spilled")]
            ),
            Some(disk_reads),
            "every disk read is timed; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_promotions_total",
                &[("cache", "spilled")]
            ),
            Some(1.0),
            "expected exactly one promotion; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_entries", &[("cache", "spilled")]),
            Some(1.0),
            "the promoted key is resident again and the key spilled to make room for it is \
             on disk, so one currently-spilled entry remains; got body:\n{body}"
        );

        // An overwrite of a spilled key decrements sundog_spill_entries the
        // same way a promotion does, via apply_put rather than a disk read.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_writes_total",
                &[("cache", "spill-overwrite")]
            ),
            Some(1.0),
            "the overwrite itself never spills anything new; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_entries",
                &[("cache", "spill-overwrite")]
            ),
            Some(0.0),
            "the overwritten key is resident again, so zero currently-spilled entries remain; \
             got body:\n{body}"
        );

        // A remove of a spilled key decrements sundog_spill_entries via
        // apply_tombstone.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_writes_total",
                &[("cache", "spill-remove")]
            ),
            Some(1.0),
            "the remove itself never spills anything new; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(&body, "sundog_spill_entries", &[("cache", "spill-remove")]),
            Some(0.0),
            "the removed key is gone, so zero currently-spilled entries remain; got body:\n{body}"
        );

        // sundog_spill_reopen_total/records_total: "spilled"'s first
        // open, warm_reopen defaulted false, takes the disabled cold
        // fallback.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reopen_total",
                &[
                    ("cache", "spilled"),
                    ("outcome", "cold_fallback"),
                    ("reason", "disabled")
                ]
            ),
            Some(1.0),
            "expected exactly one disabled cold fallback on the 'spilled' cache's first open; \
             got body:\n{body}"
        );
        // spill_reopen_pins_metrics's scenarios: a real warm reopen
        // counting both the resident and the already-spilled key.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reopen_total",
                &[
                    ("cache", "spill-reopen-warm"),
                    ("outcome", "warm"),
                    ("reason", "")
                ]
            ),
            Some(1.0),
            "expected exactly one warm reopen on 'spill-reopen-warm'; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reopen_records_total",
                &[("cache", "spill-reopen-warm")]
            ),
            Some(2.0),
            "expected the warm reopen to have installed both live entries, the one still \
             resident and the one already spilled when the tier closed; got body:\n{body}"
        );
        // sundog_spill_checkpoint_entries_total{cache,stage}: the same
        // close, staged. max_capacity(1) leaves one key resident, so the
        // checkpoint captures/writes/keeps one entry; the post-checkpoint
        // scan then lists both it and the already-spilled key, so listed
        // and snapshot total two, not one.
        for (stage, expected) in [
            ("captured", 1.0),
            ("written", 1.0),
            ("kept", 1.0),
            ("listed", 2.0),
            ("snapshot", 2.0),
        ] {
            assert_eq!(
                scraped_metric_value(
                    &body,
                    "sundog_spill_checkpoint_entries_total",
                    &[("cache", "spill-reopen-warm"), ("stage", stage)]
                ),
                Some(expected),
                "expected sundog_spill_checkpoint_entries_total{{stage=\"{stage}\"}} = \
                 {expected} on 'spill-reopen-warm'; got body:\n{body}"
            );
        }
        // sundog_spill_reopen_entries_total{cache,stage}: the matching
        // reopen; both snapshot entries are read and installed, none refused.
        for (stage, expected) in [("read", 2.0), ("installed", 2.0), ("refused_present", 0.0)] {
            assert_eq!(
                scraped_metric_value(
                    &body,
                    "sundog_spill_reopen_entries_total",
                    &[("cache", "spill-reopen-warm"), ("stage", stage)]
                ),
                Some(expected),
                "expected sundog_spill_reopen_entries_total{{stage=\"{stage}\"}} = {expected} \
                 on 'spill-reopen-warm'; got body:\n{body}"
            );
        }
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reopen_total",
                &[
                    ("cache", "spill-reopen-no-snapshot"),
                    ("outcome", "cold_fallback"),
                    ("reason", "no_snapshot")
                ]
            ),
            Some(1.0),
            "expected exactly one no_snapshot cold fallback on 'spill-reopen-no-snapshot'; got \
             body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reopen_total",
                &[
                    ("cache", "spill-reopen-config-mismatch"),
                    ("outcome", "cold_fallback"),
                    ("reason", "config_mismatch")
                ]
            ),
            Some(1.0),
            "expected exactly one config_mismatch cold fallback on \
             'spill-reopen-config-mismatch'; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reopen_total",
                &[
                    ("cache", "spill-reopen-downtime-exceeded"),
                    ("outcome", "cold_fallback"),
                    ("reason", "downtime_exceeded")
                ]
            ),
            Some(1.0),
            "expected exactly one downtime_exceeded cold fallback on \
             'spill-reopen-downtime-exceeded'; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_reopen_total",
                &[
                    ("cache", "spill-reopen-bad-region"),
                    ("outcome", "cold_fallback"),
                    ("reason", "bad_region")
                ]
            ),
            Some(1.0),
            "expected exactly one bad_region cold fallback on 'spill-reopen-bad-region'; got \
             body:\n{body}"
        );

        // disk_error_and_reserve_wait_pin_metrics's insert_many resolves
        // its one reservation against an empty flush queue: no wait, no
        // timeout, waiters back at zero.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_wait_seconds_total",
                &[("cache", "disk-error-pin")]
            ),
            Some(0.0),
            "reserve() resolved against an empty queue, so this whole-second counter has \
             nothing to accumulate yet; got body:\n{body}"
        );
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_waiters",
                &[("cache", "disk-error-pin")]
            ),
            Some(0.0),
            "the RAII guard decrements sundog_spill_waiters back to zero once reserve() \
             resolves; got body:\n{body}"
        );
        // sundog_spill_wait_timeouts_total: pinned at the unit level
        // (`store::spill`) via `pause_flusher`, unreachable here. This tiny
        // scenario never saturates its flush queue, so the series is
        // correctly never registered; reserve_timeout_pin_metric pins
        // a genuine nonzero count instead.
        assert_eq!(
            scraped_metric_value(
                &body,
                "sundog_spill_wait_timeouts_total",
                &[("cache", "disk-error-pin")]
            ),
            None,
            "reserve() resolved well inside its own timeout, so this series was never \
             registered; got body:\n{body}"
        );
        // reserve_timeout_pin_metric: a real donor/joiner pull against a
        // tiny flush queue and a 1us timeout forces a reservation to lose
        // its race against a genuine wait.
        assert!(
            scraped_metric_value(
                &body,
                "sundog_spill_wait_timeouts_total",
                &[("cache", "reserve-timeout-pin")]
            )
            .is_some_and(|count| count >= 1.0),
            "expected at least one genuine reserve() timeout on the 'reserve-timeout-pin' \
             cache; got body:\n{body}"
        );

        // disk_error: only pinned when chattr_dir_entries could set up
        // the fault (root/sudo, ext2/3/4); otherwise skipped. Once set up,
        // None is a real failure.
        if disk_error_immutable {
            assert!(
                disk_error_dropped.is_some_and(|count| count >= 1.0),
                "the evictions insert_many forces land in the region chattr_dir_entries made \
                 immutable, so every job in the failing segment counts as a disk_error drop; \
                 how many jobs share that segment depends on flusher timing, so at least one; \
                 got body:\n{body}"
            );
        }
    }

    // sundog_fan_out_wait_seconds_total{peer}: only bumps when a
    // per-peer outbox stays full past a whole deadline slice; every real
    // peer here drains loopback traffic far faster than that, so this
    // series is correctly never registered. `net/tests/fan_out.rs`
    // pin the actual increment against an outbox held full by construction.
    assert!(
        !body.contains("sundog_fan_out_wait_seconds_total"),
        "no peer in this scenario ever stalls its outbox for a whole deadline slice; got \
         body:\n{body}"
    );

    // `users` warmed during `seed_sketch_mismatch`: `is_ready()` and
    // `/readyz` on the same listener must both already agree.
    assert!(cluster.is_ready(), "the open Replicated caches are warm");
    let readyz_status = scrape_status(metrics_addr, "/readyz")
        .await
        .expect("readyz answers once the cluster is up");
    assert!(
        readyz_status.contains("200"),
        "expected /readyz to answer 200 once warm; got {readyz_status}"
    );
    let healthz_status = scrape_status(metrics_addr, "/healthz")
        .await
        .expect("healthz answers once the cluster is up");
    assert!(
        healthz_status.contains("200"),
        "expected /healthz to always answer 200 while the process serves; got {healthz_status}"
    );

    peer.shutdown().await;
    tombstone_node.shutdown().await;
    drop(skew_hold_peer);
    skew_hold_node.shutdown().await;
    cluster.shutdown().await;
    #[cfg(feature = "spill")]
    for dir in &spill_dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// [`sundog::prometheus_handle`], the no-listener install, for a caller that
/// serves `/metrics` from its own HTTP stack. Whichever test in this binary
/// installs the process-global recorder first wins it: if the recorder
/// above already claimed the slot, `prometheus_handle` still runs (and this
/// test still exercises it), it cannot hand back a usable handle, so
/// the render-based assertion below only applies when this test wins the
/// race.
#[tokio::test]
async fn prometheus_handle_exposes_cache_hits_without_a_listener() {
    let cluster = Cluster::builder("it-prometheus-handle")
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("cluster builds");

    let cache = cluster
        .cache::<u32, String>("handle-metrics")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    cache.insert(1, "a".into()).await.expect("insert");
    assert_eq!(cache.get(&1).await, Some("a".to_string()));

    if let Ok(handle) = sundog::prometheus_handle() {
        let body = handle.render();
        assert!(
            body.contains("sundog_cache_hits_total"),
            "prometheus_handle's own recorder captures cache hits; got body:\n{body}"
        );
    }
    // Else: another test in this binary installed the process-global
    // recorder first; there is no handle to render from here.

    cluster.shutdown().await;
}
