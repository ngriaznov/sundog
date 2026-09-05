//! Prometheus metrics export, behind a `prometheus` feature flag, off by
//! default. The `metrics::counter!`/`gauge!` calls spread across the crate
//! are unconditional; without this feature they fall through to `metrics`'s
//! no-op default recorder. This module wires an actual Prometheus recorder
//! into the process, two ways:
//!
//! - [`crate::cluster::ClusterBuilder::prometheus_listen`] installs a recorder
//!   and serves `GET /metrics`, `GET /readyz`, and `GET /healthz` itself.
//! - [`prometheus_handle`] installs a recorder with no listener, for a process
//!   that serves `/metrics` from its own HTTP server.
//!
//! Both call `metrics::set_global_recorder`, a single process-global slot:
//! whichever runs second fails rather than replacing the first recorder.
//! Neither panics on that failure; see each function's `# Errors`.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusBuilder;
pub use metrics_exporter_prometheus::{BuildError, PrometheusHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
    let handle = PrometheusBuilder::new().install_recorder()?;
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
    PrometheusBuilder::new().install_recorder()
}
