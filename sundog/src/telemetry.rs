//! Prometheus metrics export, behind a `prometheus` feature flag, off by
//! default. The `metrics::counter!`, `gauge!` and `histogram!` calls
//! spread across the crate are unconditional; without this feature they
//! fall through to `metrics`'s no-op default recorder. This module wires an
//! actual Prometheus recorder into the process, two ways:
//!
//! - [`crate::cluster::ClusterBuilder::prometheus_listen`] installs a recorder
//!   and serves `GET /metrics`, `GET /readyz`, and `GET /healthz` itself.
//! - [`prometheus_handle`] installs a recorder with no listener, for a process
//!   that serves `/metrics` from its own HTTP server.
//!
//! Both call `metrics::set_global_recorder`, a single process-global slot:
//! whichever runs second fails rather than replacing the first recorder.
//! Neither panics on that failure; see each function's `# Errors`.
//!
//! Both install the same recorder: each of [`DURATION_HISTOGRAMS`] renders
//! as Prometheus histogram buckets from [`LATENCY_BUCKETS`], which aggregate
//! across nodes in a Prometheus query, rather than as per-node quantiles,
//! which do not. The buckets match those names exactly, so a histogram the
//! application records through the same recorder renders as the exporter's
//! default summary.
//!
//! Install the recorder before opening a cache. A cache resolves its
//! per-cache handles (`sundog_cache_hits_total{cache}`,
//! `sundog_spill_entries{cache}`, and the like) once, when it opens, against
//! whichever recorder is global at that moment; a cache opened before the
//! real recorder installs keeps reporting to the no-op recorder.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

pub use metrics_exporter_prometheus::{BuildError, PrometheusHandle};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The upper bounds, in seconds, of the buckets of each of
/// [`DURATION_HISTOGRAMS`]: 1µs to 10s in 1-2.5-5 steps. A resident read
/// lands in the first bucket, and a fetch that waits out its timeout in the
/// last ones.
pub const LATENCY_BUCKETS: &[f64] = &[
    0.000_001,
    0.000_002_5,
    0.000_005,
    0.000_01,
    0.000_025,
    0.000_05,
    0.000_1,
    0.000_25,
    0.000_5,
    0.001,
    0.002_5,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
];

/// Every histogram the crate records, each bucketed by [`LATENCY_BUCKETS`];
/// `sundog_spill_read_duration_seconds` is recorded only with the `spill`
/// feature.
pub const DURATION_HISTOGRAMS: &[&str] = &[
    "sundog_read_duration_seconds",
    "sundog_fetch_duration_seconds",
    "sundog_spill_read_duration_seconds",
];

/// The recorder both install paths use: [`LATENCY_BUCKETS`] for each of
/// [`DURATION_HISTOGRAMS`], matched by full name.
fn builder() -> PrometheusBuilder {
    DURATION_HISTOGRAMS
        .iter()
        .fold(PrometheusBuilder::new(), |builder, &name| {
            builder
                .set_buckets_for_metric(Matcher::Full(name.to_owned()), LATENCY_BUCKETS)
                .expect("invariant: LATENCY_BUCKETS is not empty")
        })
}

/// What `install_listener`'s `GET /readyz` route reports.
pub(crate) trait ReadinessSource: Send + Sync + 'static {
    /// Mirrors [`crate::cluster::Cluster::is_ready`].
    fn is_ready(&self) -> bool;
}

/// Installs a Prometheus recorder and serves `GET /metrics`, `GET /readyz`
/// (200 once `readiness` reports ready, 503 otherwise), and `GET /healthz`
/// on `addr`.
///
/// # Errors
///
/// Returns [`BuildError`] if `addr` cannot be bound, or a `metrics`
/// recorder is already installed.
pub(crate) fn install_listener(
    addr: SocketAddr,
    readiness: Arc<dyn ReadinessSource>,
) -> Result<(), BuildError> {
    let handle = builder().install_recorder()?;
    let std_listener = std::net::TcpListener::bind(addr)
        .and_then(|listener| {
            listener.set_nonblocking(true)?;
            Ok(listener)
        })
        .map_err(|error| BuildError::FailedToCreateHTTPListener(error.to_string()))?;
    let listener = TcpListener::from_std(std_listener)
        .map_err(|error| BuildError::FailedToCreateHTTPListener(error.to_string()))?;
    tokio::spawn(upkeep(handle.clone()));
    tokio::spawn(serve(listener, handle, readiness));
    Ok(())
}

/// Drains histogram buckets on the exporter's default cadence, the task its
/// built-in listener would otherwise run.
async fn upkeep(handle: PrometheusHandle) {
    loop {
        tokio::time::sleep(UPKEEP_INTERVAL).await;
        handle.run_upkeep();
    }
}

const UPKEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Accepts connections on `listener` forever, answering each on its own task.
async fn serve(
    listener: TcpListener,
    handle: PrometheusHandle,
    readiness: Arc<dyn ReadinessSource>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        };
        let handle = handle.clone();
        let readiness = Arc::clone(&readiness);
        tokio::spawn(async move {
            if let Err(error) = respond(stream, &handle, readiness.as_ref()).await {
                tracing::debug!(%error, "telemetry http connection ended early");
            }
        });
    }
}

/// Answers one request on `stream`: `/metrics`, `/readyz`, `/healthz` (and
/// its `/health` alias from the exporter's own listener), 404 otherwise.
async fn respond(
    mut stream: TcpStream,
    handle: &PrometheusHandle,
    readiness: &dyn ReadinessSource,
) -> io::Result<()> {
    let mut buf = [0u8; 512];
    let read = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..read]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, body) = match path {
        "/metrics" => ("200 OK", handle.render()),
        "/readyz" if readiness.is_ready() => ("200 OK", "ready\n".to_string()),
        "/readyz" => ("503 Service Unavailable", "not ready\n".to_string()),
        "/healthz" | "/health" => ("200 OK", "ok\n".to_string()),
        _ => ("404 Not Found", String::new()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: \
         {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

/// Installs a Prometheus recorder with no listener, for a process that
/// serves `GET /metrics` via [`PrometheusHandle::render`] on its own HTTP
/// stack. The caller must call [`PrometheusHandle::run_upkeep`] on the
/// returned handle at a regular interval; this installs no loop for it.
///
/// # Errors
///
/// Returns [`BuildError`] if a `metrics` recorder is already installed.
pub fn prometheus_handle() -> Result<PrometheusHandle, BuildError> {
    builder().install_recorder()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_buckets_rise_from_a_microsecond_to_ten_seconds() {
        assert_eq!(LATENCY_BUCKETS.first(), Some(&0.000_001));
        assert_eq!(LATENCY_BUCKETS.last(), Some(&10.0));
        assert!(LATENCY_BUCKETS.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn the_builder_buckets_each_crate_histogram_and_no_other() {
        let recorder = builder().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            for &name in DURATION_HISTOGRAMS {
                metrics::histogram!(name).record(0.003);
            }
            metrics::histogram!("app_request_duration_seconds").record(0.003);
        });
        let body = handle.render();
        for &name in DURATION_HISTOGRAMS {
            assert!(body.contains(&format!("# TYPE {name} histogram")), "{body}");
            assert!(
                body.contains(&format!("{name}_bucket{{le=\"0.005\"}} 1")),
                "{body}"
            );
            assert!(
                body.contains(&format!("{name}_bucket{{le=\"0.0025\"}} 0")),
                "{body}"
            );
        }
        assert!(
            body.contains("# TYPE app_request_duration_seconds summary"),
            "an application histogram keeps the exporter's default: {body}"
        );
        assert!(
            !body.contains("app_request_duration_seconds_bucket"),
            "{body}"
        );
    }

    /// Every string literal in the crate's `src/` naming a `sundog_` metric
    /// that ends in `_duration_seconds` is in [`DURATION_HISTOGRAMS`], so a
    /// duration histogram added under such a name without buckets fails
    /// here.
    #[test]
    fn every_histogram_the_crate_records_is_bucketed() {
        fn rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("src/ reads") {
                let path = entry.expect("a directory entry").path();
                if path.is_dir() {
                    rust_sources(&path, out);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    out.push(path);
                }
            }
        }
        let mut files = Vec::new();
        rust_sources(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut files,
        );
        let mut named = std::collections::BTreeSet::new();
        for file in files {
            let source = std::fs::read_to_string(&file).expect("a source file reads");
            for (start, _) in source.match_indices("\"sundog_") {
                let rest = &source[start + 1..];
                let name = &rest[..rest.find('"').expect("a closing quote")];
                let metric_name = name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
                if metric_name && name.ends_with("_duration_seconds") {
                    named.insert(name.to_owned());
                }
            }
        }
        assert_eq!(
            named,
            DURATION_HISTOGRAMS
                .iter()
                .map(|&name| name.to_owned())
                .collect(),
            "every recorded histogram is in DURATION_HISTOGRAMS"
        );
    }
}
