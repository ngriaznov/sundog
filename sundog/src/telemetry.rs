//! Prometheus metrics export, behind a `prometheus` feature flag, off by
//! default.
//!
//! Metric emission — the `metrics::counter!`/`metrics::gauge!` calls spread
//! across the crate (`sundog_backlog_dropped_total`, `sundog_live_peers`,
//! `sundog_open_caches`, `sundog_cache_hits_total`,
//! `sundog_cache_misses_total`, `sundog_cache_entries`,
//! `sundog_ae_repaired_total`, `sundog_ae_sketch_total`, ...) is
//! unconditional: without this feature those calls fall through to
//! `metrics`'s no-op default recorder. This module wires an actual
//! Prometheus recorder into the process, two ways:
//!
//! - [`crate::cluster::ClusterBuilder::prometheus_listen`] installs a
//!   recorder and serves `GET /metrics` itself.
//! - [`prometheus_handle`] installs a recorder with no listener, for a
//!   process that serves `/metrics` from its own HTTP server.
//!
//! Both call `metrics::set_global_recorder`, a single process-global slot:
//! whichever of these runs second — a second cluster's `prometheus_listen`,
//! or a mix of `prometheus_listen` and `prometheus_handle` in the same
//! process — fails rather than replacing the first recorder. Neither path
//! panics on that failure; see each function's `# Errors`.

use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;
pub use metrics_exporter_prometheus::{BuildError, PrometheusHandle};

/// Installs a Prometheus recorder and serves `GET /metrics` (and `/health`)
/// on `addr` for the life of the process, spawning the exporter's own upkeep
/// loop. Called from [`crate::cluster::ClusterBuilder::build`] when
/// [`crate::cluster::ClusterBuilder::prometheus_listen`] was configured.
///
/// # Errors
///
/// Returns [`BuildError`] if `addr` cannot be bound, or if a `metrics`
/// recorder is already installed in this process.
pub(crate) fn install_listener(addr: SocketAddr) -> Result<(), BuildError> {
    PrometheusBuilder::new().with_http_listener(addr).install()
}

/// Installs a Prometheus recorder with no listener, for a process that
/// serves `GET /metrics` on its own HTTP stack via [`PrometheusHandle::render`].
///
/// The caller must call [`PrometheusHandle::run_upkeep`] on the returned
/// handle at a regular interval (a few seconds is typical); unlike
/// `prometheus_listen`, this installs no background loop to do it for you.
///
/// # Errors
///
/// Returns [`BuildError`] if a `metrics` recorder is already installed in
/// this process.
pub fn prometheus_handle() -> Result<PrometheusHandle, BuildError> {
    PrometheusBuilder::new().install_recorder()
}
