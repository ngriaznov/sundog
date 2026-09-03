//! Prometheus metrics export, behind a `prometheus` feature flag, off by
//! default. The `metrics::counter!`/`gauge!` calls spread across the crate
//! are unconditional; without this feature they fall through to `metrics`'s
//! no-op default recorder. This module wires an actual Prometheus recorder
//! into the process, two ways:
//!
//! - [`crate::cluster::ClusterBuilder::prometheus_listen`] installs a recorder
//!   and serves `GET /metrics` itself.
//! - [`prometheus_handle`] installs a recorder with no listener, for a process
//!   that serves `/metrics` from its own HTTP server.
//!
//! Both call `metrics::set_global_recorder`, a single process-global slot:
//! whichever runs second fails rather than replacing the first recorder.
//! Neither panics on that failure; see each function's `# Errors`.

use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;
pub use metrics_exporter_prometheus::{BuildError, PrometheusHandle};

/// Installs a Prometheus recorder and serves `GET /metrics` (and `/health`)
/// on `addr`, spawning the exporter's own upkeep loop.
///
/// # Errors
///
/// Returns [`BuildError`] if `addr` cannot be bound, or a `metrics`
/// recorder is already installed.
pub(crate) fn install_listener(addr: SocketAddr) -> Result<(), BuildError> {
    PrometheusBuilder::new().with_http_listener(addr).install()
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
