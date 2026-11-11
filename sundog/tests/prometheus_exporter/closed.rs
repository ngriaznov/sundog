//! The gauges of a closed cache and of a shut-down cluster read 0.

use crate::support::{scrape_metrics, scraped_metric_value, scraped_unlabelled_value};
use std::net::SocketAddr;
use std::time::Duration;
use sundog::{Cache, Cluster, Mode};

/// Scrapes until `metric{cache}` reads `want`, panicking after `bound`.
async fn await_cache_gauge(
    metrics_addr: SocketAddr,
    metric: &str,
    cache: &str,
    want: f64,
    bound: Duration,
) {
    let deadline = tokio::time::Instant::now() + bound;
    loop {
        let body = scrape_metrics(metrics_addr).await.unwrap_or_default();
        let value = scraped_metric_value(&body, metric, &[("cache", cache)]);
        if value == Some(want) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{metric}{{cache=\"{cache}\"}} never read {want}, last {value:?}; body:\n{body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Closes a capped `Local` cache and a `Distributed` one on `cluster`, and
/// pins that each per-cache gauge (`sundog_cache_entries`,
/// `sundog_tombstones`, `sundog_cache_bytes`, `sundog_owned_parts`,
/// `sundog_owned_buckets`) reads 0 afterwards, and that a cache reopened
/// under the same name reports its own entries again.
pub(crate) async fn closed_cache_gauge_pins(cluster: &Cluster, metrics_addr: SocketAddr) {
    // The entry gauges refresh every five seconds.
    let refresh = Duration::from_secs(15);
    let open_local = async || {
        cluster
            .cache::<u32, String>("closed-gauge-pin")
            .mode(Mode::Local)
            .max_resident_bytes(1 << 30)
            .open()
            .await
            .expect("cache opens")
    };
    let local = open_local().await;
    for key in 0..3 {
        local.insert(key, "value".into()).await.expect("insert");
    }
    await_cache_gauge(
        metrics_addr,
        "sundog_cache_entries",
        "closed-gauge-pin",
        3.0,
        refresh,
    )
    .await;
    let body = scrape_metrics(metrics_addr).await.expect("scrape");
    assert!(
        scraped_metric_value(
            &body,
            "sundog_cache_bytes",
            &[("cache", "closed-gauge-pin")]
        )
        .is_some_and(|bytes| bytes > 0.0),
        "an open capped cache counts its bytes; body:\n{body}"
    );

    let distributed = cluster
        .cache::<u32, String>("closed-gauge-owned")
        .mode(Mode::distributed())
        .open()
        .await
        .expect("distributed cache opens");
    let body = scrape_metrics(metrics_addr).await.expect("scrape");
    assert!(
        scraped_metric_value(
            &body,
            "sundog_owned_parts",
            &[("cache", "closed-gauge-owned")]
        )
        .is_some_and(|parts| parts > 0.0),
        "an open distributed cache owns parts; body:\n{body}"
    );

    local.close().await;
    distributed.close().await;
    let body = scrape_metrics(metrics_addr).await.expect("scrape");
    for (metric, cache) in [
        ("sundog_cache_entries", "closed-gauge-pin"),
        ("sundog_tombstones", "closed-gauge-pin"),
        ("sundog_cache_bytes", "closed-gauge-pin"),
        ("sundog_cache_entries", "closed-gauge-owned"),
        ("sundog_owned_parts", "closed-gauge-owned"),
        ("sundog_owned_buckets", "closed-gauge-owned"),
    ] {
        assert_eq!(
            scraped_metric_value(&body, metric, &[("cache", cache)]),
            Some(0.0),
            "{metric}{{cache=\"{cache}\"}} reads 0 once the cache is closed; body:\n{body}"
        );
    }

    closed_fan_out_gauge_pins(cluster, metrics_addr, &body).await;
    #[cfg(feature = "spill")]
    closed_spill_gauge_pins(cluster, metrics_addr).await;

    let reopened = open_local().await;
    reopened.insert(7, "value".into()).await.expect("insert");
    await_cache_gauge(
        metrics_addr,
        "sundog_cache_entries",
        "closed-gauge-pin",
        1.0,
        refresh,
    )
    .await;
    reopened.close().await;
}

/// After every node of this test has shut down, the cluster gauges read 0.
pub(crate) async fn shut_down_cluster_gauge_pins(metrics_addr: SocketAddr) {
    let body = scrape_metrics(metrics_addr).await.expect("scrape");
    for metric in ["sundog_open_caches", "sundog_live_peers"] {
        assert_eq!(
            scraped_unlabelled_value(&body, metric),
            Some(0.0),
            "{metric} reads 0 after shutdown; body:\n{body}"
        );
    }
}

/// Closes a `Replicated` cache that queued writes for its peer, and pins
/// `sundog_fan_out_backlog` at 0 for it. `local_body`, scraped after the
/// `Local` cache `closed-gauge-pin` closed, pins that a cache that never
/// queued a key for a peer has no series to zero.
///
/// The caches of this file's pins carry names no other node of the test
/// opens: a gauge is one series for every cache of that name in the process,
/// so a second live cache of the name would hold it off the zero.
async fn closed_fan_out_gauge_pins(cluster: &Cluster, metrics_addr: SocketAddr, local_body: &str) {
    assert_eq!(
        scraped_metric_value(
            local_body,
            "sundog_fan_out_backlog",
            &[("cache", "closed-gauge-pin")]
        ),
        None,
        "a Local cache queues nothing for a peer and exports no backlog series; body:\n{local_body}"
    );
    let replicated = cluster
        .cache::<u32, String>("closed-gauge-fan-out")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("replicated cache opens");
    for key in 0..8 {
        replicated
            .insert(key, "value".into())
            .await
            .expect("insert");
    }
    let body = scrape_metrics(metrics_addr).await.expect("scrape");
    assert!(
        scraped_metric_value(
            &body,
            "sundog_fan_out_backlog",
            &[("cache", "closed-gauge-fan-out")]
        )
        .is_some(),
        "a cache that queued writes for its peer publishes a backlog; body:\n{body}"
    );
    replicated.close().await;
    let body = scrape_metrics(metrics_addr).await.expect("scrape");
    assert_eq!(
        scraped_metric_value(
            &body,
            "sundog_fan_out_backlog",
            &[("cache", "closed-gauge-fan-out")]
        ),
        Some(0.0),
        "sundog_fan_out_backlog reads 0 once the cache is closed; body:\n{body}"
    );
}

/// Closes a spill cache that holds spilled entries and pins
/// `sundog_spill_entries` and `sundog_spill_bytes_used` at 0, and that a
/// cache reopened under the name counts its own entries from there.
#[cfg(feature = "spill")]
async fn closed_spill_gauge_pins(cluster: &Cluster, metrics_addr: SocketAddr) {
    let dir = crate::spill::fresh_spill_dir("closed-gauges");
    let name = "closed-gauge-spill";
    let open = async || {
        cluster
            .cache::<u32, String>(name)
            .mode(Mode::Local)
            .max_capacity(3)
            .spill(sundog::SpillConfig::new(&dir, 1 << 20).region_bytes(4096))
            .open()
            .await
            .expect("spill cache opens")
    };
    let cache = open().await;
    for key in 0..20 {
        cache.insert(key, "value".into()).await.expect("insert");
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let body = scrape_metrics(metrics_addr).await.unwrap_or_default();
        let entries = scraped_metric_value(&body, "sundog_spill_entries", &[("cache", name)]);
        let bytes = scraped_metric_value(&body, "sundog_spill_bytes_used", &[("cache", name)]);
        if entries.is_some_and(|n| n > 0.0) && bytes.is_some_and(|n| n > 0.0) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "entries spill; last {entries:?} entries, {bytes:?} bytes"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cache.close().await;
    let body = scrape_metrics(metrics_addr).await.expect("scrape");
    for metric in ["sundog_spill_entries", "sundog_spill_bytes_used"] {
        assert_eq!(
            scraped_metric_value(&body, metric, &[("cache", name)]),
            Some(0.0),
            "{metric} reads 0 once the cache is closed; body:\n{body}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Closes the cache holding the skew hold's records and pins the hold's
/// gauges at 0.
pub(crate) async fn closed_skew_hold_gauge_pins(
    cache: Cache<u32, String>,
    metrics_addr: SocketAddr,
) {
    cache.close().await;
    let body = scrape_metrics(metrics_addr).await.expect("scrape");
    for metric in ["sundog_clock_skew_held", "sundog_clock_skew_held_bytes"] {
        assert_eq!(
            scraped_metric_value(&body, metric, &[("cache", "skew-hold-pin")]),
            Some(0.0),
            "{metric} reads 0 once the cache is closed; body:\n{body}"
        );
    }
}
