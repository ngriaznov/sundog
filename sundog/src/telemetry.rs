//! Prometheus metrics export, behind a `prometheus` feature flag, off by
//! default.
//!
//! Metric *emission* — the `metrics::counter!`/`metrics::gauge!` calls spread
//! across the crate (`sundog_backlog_dropped_total`, `sundog_live_peers`,
//! `sundog_open_caches`, ...) — is unconditional and feature-independent: a
//! build without this feature simply never installs a recorder, so those
//! calls fall through to `metrics`'s own no-op default. This module only
//! wires an actual Prometheus recorder into the process, two ways:
//!
//! - [`crate::cluster::ClusterBuilder::prometheus_listen`] installs a
//!   recorder *and* serves `GET /metrics` itself — the common case.
//! - [`prometheus_handle`] installs a recorder with no listener of its own,
//!   for a process that already runs its own HTTP server and wants to add a
//!   `/metrics` route to it (`PrometheusBuilder::install_recorder` in the
//!   underlying crate, as opposed to `with_http_listener`).
//!
//! Both ultimately call `metrics::set_global_recorder`, a single
//! process-global slot: **whichever of these runs second — a second
//! cluster's `prometheus_listen`, or a mix of `prometheus_listen` and
//! `prometheus_handle` in the same process — fails rather than silently
//! replacing the first recorder.** Neither path here panics on that failure;
//! see each function's `# Errors`.

use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;
pub use metrics_exporter_prometheus::{BuildError, PrometheusHandle};

/// Installs a Prometheus recorder and serves `GET /metrics` (and `/health`)
/// on `addr` for the life of the process — the exporter crate's own upkeep
/// task (draining internal histogram-bucket bookkeeping) is spawned
/// automatically as part of this. Called from
/// [`crate::cluster::ClusterBuilder::build`] when
/// [`crate::cluster::ClusterBuilder::prometheus_listen`] was configured.
///
/// # Errors
///
/// Returns the exporter's [`BuildError`] if `addr` cannot be bound, or if a
/// `metrics` recorder is already installed in this process — see the module
/// docs' process-global-recorder constraint.
pub(crate) fn install_listener(addr: SocketAddr) -> Result<(), BuildError> {
    PrometheusBuilder::new().with_http_listener(addr).install()
}

/// Installs a Prometheus recorder with no listener of its own, for a process
/// that serves `GET /metrics` on its own HTTP stack: render text-exposition
/// output on demand with [`PrometheusHandle::render`] (or
/// `PrometheusHandle::render_protobuf` behind the exporter crate's own
/// `protobuf` feature, not enabled here).
///
/// The caller is responsible for calling [`PrometheusHandle::run_upkeep`] on
/// the returned handle at a regular interval (a few seconds is typical) —
/// `prometheus_listen`'s own listener does this from a background task it
/// owns, but a bare handle has no task of its own to do it from, and the
/// underlying crate does not spawn one implicitly on a caller's behalf.
///
/// # Errors
///
/// Returns the exporter's [`BuildError`] if a `metrics` recorder is already
/// installed in this process — see the module docs' process-global-recorder
/// constraint.
pub fn prometheus_handle() -> Result<PrometheusHandle, BuildError> {
    PrometheusBuilder::new().install_recorder()
}
