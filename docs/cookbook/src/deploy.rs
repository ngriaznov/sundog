//! Getting started and deployment: the first cluster, fixed ports for a VPC
//! or Kubernetes, the readiness hook and a graceful shutdown.

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use serde::{Deserialize, Serialize};
use sundog::discovery::dns::DnsSrv;
use sundog::{Cache, CacheError, Cluster, ClusterConfig, Mode};
use tokio::net::TcpListener;

// ANCHOR: types
#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct UserId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Profile {
    pub name: String,
}
// ANCHOR_END: types

/// Builds a node with every default: mDNS discovery and ephemeral ports.
///
/// # Errors
///
/// Returns an error if the node cannot bind its sockets.
// ANCHOR: first_cluster
pub async fn first_cluster() -> anyhow::Result<Cluster> {
    let cluster = Cluster::builder("demo").build().await?;
    Ok(cluster)
}
// ANCHOR_END: first_cluster

/// Opens the `users` cache and runs one write, one read and one removal.
///
/// # Errors
///
/// Returns an error if the cache cannot open or a write fails.
///
/// # Panics
///
/// Panics if a read disagrees with the write before it.
// ANCHOR: first_cache
pub async fn use_users(cluster: &Cluster) -> Result<(), CacheError> {
    let users = cluster
        .cache::<UserId, Profile>("users")
        .mode(Mode::Replicated)
        .ttl(Duration::from_secs(600))
        .open()
        .await?;

    let ada = Profile {
        name: "Ada".to_string(),
    };
    users.insert(UserId(1), ada.clone()).await?;
    assert_eq!(users.get(&UserId(1)).await, Some(ada));

    users.remove(&UserId(1)).await?;
    assert_eq!(users.get(&UserId(1)).await, None);
    Ok(())
}
// ANCHOR_END: first_cache

/// The configuration a VPC or Kubernetes deployment needs: fixed ports a
/// security group or network policy can allow.
// ANCHOR: fixed_ports
#[must_use]
pub fn fixed_ports() -> ClusterConfig {
    ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = SocketAddr::from(([0, 0, 0, 0], 7946));
        c.data_bind_addr = SocketAddr::from(([0, 0, 0, 0], 7947));
    })
}
// ANCHOR_END: fixed_ports

/// Joins a VPC cluster through two stable seed addresses.
///
/// # Errors
///
/// Returns an error if the node cannot bind its fixed ports.
// ANCHOR: vpc
pub async fn join_vpc() -> anyhow::Result<Cluster> {
    let cluster = Cluster::builder("prod")
        .config(fixed_ports())
        .seeds([
            SocketAddr::from(([10, 0, 1, 10], 7946)),
            SocketAddr::from(([10, 0, 2, 10], 7946)),
        ])
        .build()
        .await?;
    Ok(cluster)
}
// ANCHOR_END: vpc

/// Joins a Kubernetes cluster through a headless Service.
///
/// # Errors
///
/// Returns an error if the node cannot bind its fixed ports.
// ANCHOR: kubernetes
pub async fn join_kubernetes() -> anyhow::Result<Cluster> {
    let cluster = Cluster::builder("prod")
        .config(fixed_ports())
        .discovery(DnsSrv::new(
            "myservice-gossip.my-ns.svc.cluster.local.",
            7946,
        ))
        .build()
        .await?;
    Ok(cluster)
}
// ANCHOR_END: kubernetes

// ANCHOR: readiness
/// The service's own readiness route, holding traffic off this pod until
/// every `Replicated` cache has pulled its snapshot.
pub async fn readyz(State(cluster): State<Cluster>) -> StatusCode {
    if cluster.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

pub fn health_routes(cluster: Cluster) -> Router {
    Router::new()
        .route("/readyz", get(readyz))
        .with_state(cluster)
}
// ANCHOR_END: readiness

/// Serves `app` until SIGTERM or Ctrl-C, then leaves the cluster.
///
/// # Errors
///
/// Returns an error if the HTTP server fails.
// ANCHOR: shutdown
pub async fn serve(cluster: Cluster, app: Router, listener: TcpListener) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    // Peers learn this node left on purpose, not by failing, and a spill
    // tier writes its checkpoint.
    cluster.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install the SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
// ANCHOR_END: shutdown

/// A bounded per-node cache: `Invalidation` mode, where `max_capacity`
/// needs no spill tier.
///
/// # Errors
///
/// Returns an error if the cache cannot open.
// ANCHOR: bounded
pub async fn open_bounded(cluster: &Cluster) -> Result<Cache<UserId, Profile>, CacheError> {
    cluster
        .cache::<UserId, Profile>("profiles")
        .mode(Mode::Invalidation)
        .max_capacity(200_000)
        .ttl(Duration::from_secs(600))
        .open()
        .await
}
// ANCHOR_END: bounded

/// A `Distributed` cache: each key on two owners, read with `fetch`.
///
/// # Errors
///
/// Returns an error if the cache cannot open, a write fails, or no owner
/// answers the read.
// ANCHOR: distributed
pub async fn price_lookup(cluster: &Cluster) -> Result<Option<u64>, CacheError> {
    let prices = cluster
        .cache::<String, u64>("prices")
        .mode(Mode::distributed())
        .open()
        .await?;

    // Lands on the key's two owners, forwarded if this node is not one.
    prices.insert("sku-42".to_string(), 999).await?;

    // Local when this node owns the key's bucket, one round trip otherwise.
    prices.fetch(&"sku-42".to_string()).await
}
// ANCHOR_END: distributed

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt as _;

    use super::*;
    use crate::test_support::solo_cluster;

    #[tokio::test]
    async fn the_first_cache_writes_reads_and_removes() {
        let cluster = solo_cluster("cookbook-first-cache").await;
        use_users(&cluster)
            .await
            .expect("the first-cache recipe runs");
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn a_bounded_invalidation_cache_opens_without_a_spill_tier() {
        let cluster = solo_cluster("cookbook-bounded").await;
        let profiles = open_bounded(&cluster)
            .await
            .expect("the bounded cache opens");
        profiles
            .insert(
                UserId(7),
                Profile {
                    name: "Grace".to_string(),
                },
            )
            .await
            .expect("insert");
        assert_eq!(profiles.entry_count().await, 1);
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn a_replicated_cache_with_a_capacity_and_no_spill_tier_is_refused() {
        let cluster = solo_cluster("cookbook-replicated-capacity").await;
        let refused = cluster
            .cache::<UserId, Profile>("users")
            .mode(Mode::Replicated)
            .max_capacity(200_000)
            .open()
            .await
            .expect_err("a replicated cache refuses a local capacity bound");
        assert!(
            matches!(refused, CacheError::ReplicatedWithLocalEviction { .. }),
            "got {refused:?}"
        );
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn a_distributed_write_is_fetchable() {
        let cluster = solo_cluster("cookbook-distributed").await;
        assert_eq!(
            price_lookup(&cluster).await.expect("fetch answers"),
            Some(999)
        );
        cluster.shutdown().await;
    }

    #[test]
    fn fixed_ports_bind_every_interface_on_the_documented_ports() {
        let config = fixed_ports();
        assert_eq!(
            config.gossip_bind_addr,
            SocketAddr::from(([0, 0, 0, 0], 7946))
        );
        assert_eq!(
            config.data_bind_addr,
            SocketAddr::from(([0, 0, 0, 0], 7947))
        );
    }

    #[tokio::test]
    async fn readiness_answers_ok_once_every_replicated_cache_is_warm() {
        let cluster = solo_cluster("cookbook-readiness").await;
        use_users(&cluster).await.expect("open a replicated cache");
        let response = health_routes(cluster.clone())
            .oneshot(
                Request::get("/readyz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("route answers");
        assert_eq!(response.status(), StatusCode::OK);
        cluster.shutdown().await;
    }
}
