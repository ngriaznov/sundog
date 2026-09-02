//! The `prometheus` feature installs a real Prometheus recorder and serves
//! `GET /metrics` on the address given to
//! [`sundog::ClusterBuilder::prometheus_listen`].
//!
//! `metrics::set_global_recorder` is a single process-global slot (see
//! `sundog::telemetry`'s module docs) — this must stay the *only* test, in
//! this or any other `tests/*.rs` binary (each its own process), that
//! installs a Prometheus recorder, or the second install fails.

#![cfg(all(feature = "prometheus", not(feature = "sim")))]

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use sundog::{Cluster, Mode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Reserves a loopback TCP port the same way `cluster.rs`'s own
/// `reserve_data_bind_addr` does: probe-bind an ephemeral port, read it back,
/// then drop the listener so the Prometheus exporter's own bind (inside
/// `prometheus_listen`) can claim the same address moments later.
async fn reserve_tcp_addr() -> SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback tcp port to reserve a metrics address");
    listener
        .local_addr()
        .expect("a just-bound tcp listener reports a local address")
}

/// A minimal raw-socket `GET /metrics` — no HTTP client dependency needed for
/// one request against a text-exposition endpoint. Returns `None` if the
/// listener isn't accepting connections yet (the exporter's own background
/// task may not have started serving by the time this first runs).
async fn scrape_metrics(addr: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    stream
        .write_all(
            format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.ok()?;
    Some(String::from_utf8_lossy(&body).into_owned())
}

/// Finds `metric{label="value"} <number>` in Prometheus text-exposition
/// `body`, tolerant of the exporter's label ordering and of whether the
/// trailing value is rendered as an integer or a float (`"4"` vs `"4.0"`).
/// Returns the parsed value of the first matching line, skipping `#`
/// comment/type lines.
fn scraped_metric_value(body: &str, metric: &str, label: (&str, &str)) -> Option<f64> {
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

#[tokio::test]
async fn metrics_endpoint_serves_sundog_metrics_after_cache_ops() {
    let metrics_addr = reserve_tcp_addr().await;

    let cluster = Cluster::builder("it-prometheus-exporter")
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .prometheus_listen(metrics_addr)
        .build()
        .await
        .expect("cluster builds with a prometheus listener");

    let cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("cache opens");
    cache.insert(1, "hello".into()).await.expect("insert");
    cache.remove(&1).await.expect("remove");

    // A known hit/miss sequence on its own cache, so its counters are exact
    // rather than shared with `users`' own traffic above:
    // 2 inserts, 3 gets that hit, 2 gets that miss, one `get_or_load` miss
    // that fills, one `get_or_load` hit on the now-filled key ->
    // hits = 3 + 1 = 4, misses = 2 + 1 = 3.
    let counted = cluster
        .cache::<u32, String>("counted")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    counted.insert(1, "a".into()).await.expect("insert");
    counted.insert(2, "b".into()).await.expect("insert");
    assert_eq!(counted.get(&1).await, Some("a".to_string()));
    assert_eq!(counted.get(&2).await, Some("b".to_string()));
    assert_eq!(counted.get(&1).await, Some("a".to_string()));
    assert_eq!(counted.get(&3).await, None);
    assert_eq!(counted.get(&4).await, None);
    let filled = counted
        .get_or_load(&5, async |_key| {
            Ok::<_, std::convert::Infallible>("loaded".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(filled, "loaded");
    let cached = counted
        .get_or_load(&5, async |_key| {
            Ok::<_, std::convert::Infallible>("loaded".to_string())
        })
        .await
        .expect("loader succeeds");
    assert_eq!(cached, "loaded");

    // `sundog_open_caches` is only set by a periodic background task (see
    // `cluster::open_cache_gauge_task`'s docs on why it can't be
    // event-driven), so this polls until every metric checked below has been
    // published at least once, rather than stopping at the first reachable
    // scrape.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let body = loop {
        if let Some(body) = scrape_metrics(metrics_addr).await
            && body.contains("sundog_open_caches")
            && body.contains("sundog_live_peers")
            && body.contains("sundog_cache_entries")
        {
            break body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "metrics endpoint never served sundog_open_caches, sundog_live_peers, and \
             sundog_cache_entries within the bound"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_hits_total", ("cache", "counted")),
        Some(4.0),
        "expected 4 hits on the 'counted' cache; got body:\n{body}"
    );
    assert_eq!(
        scraped_metric_value(&body, "sundog_cache_misses_total", ("cache", "counted")),
        Some(3.0),
        "expected 3 misses on the 'counted' cache; got body:\n{body}"
    );
    assert!(
        scraped_metric_value(&body, "sundog_cache_entries", ("cache", "counted")).is_some(),
        "expected a sundog_cache_entries line for the 'counted' cache; got body:\n{body}"
    );

    cluster.shutdown().await;
}
