//! The gauges of a closed cache and of a shut-down cluster read 0.

use crate::support::{scrape_metrics, scraped_metric_value, scraped_unlabelled_value};
use std::net::SocketAddr;
use std::time::Duration;
use sundog::{Cluster, Mode};

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
