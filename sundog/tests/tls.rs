//! Feature `tls`: mutual TLS on the data-plane mesh. Two throwaway-CA
//! scenarios: nodes sharing a root CA replicate normally over TLS; nodes
//! whose certificates chain to *different* CAs never manage to fan a write
//! out to each other, even though they still see each other via gossip
//! membership (chitchat's UDP plane never touches TLS).
//!
//! Certs are generated fresh per test with `rcgen` (dev-dependency) — no
//! files on disk, no expiry to manage. This binary duplicates the small
//! cert-generation helpers `net::tls`'s own unit tests use (documented
//! duplication, same as `tests/sim.rs`'s note on `cluster::anti_entropy`/
//! `state_transfer`: those helpers are private to `sundog`'s `src/`, so an
//! external integration-test binary can't import them).

#![cfg(all(feature = "tls", not(feature = "sim")))]

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sundog::{Cluster, ClusterConfig, Mode, TlsConfig};

struct Ca {
    der: CertificateDer<'static>,
    issuer: Issuer<'static, KeyPair>,
}

fn generate_ca() -> Ca {
    let key = KeyPair::generate().expect("generate ca key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let cert = params.self_signed(&key).expect("self-sign ca");
    let der = cert.der().clone();
    let issuer = Issuer::new(params, key);
    Ca { der, issuer }
}

/// A fresh leaf cert/key signed by `ca`, carrying the mesh's required fixed
/// SAN (`sundog::net::MESH_SERVER_NAME` — see `net::tls`'s docs on why every
/// node cert needs it, mTLS having no per-node hostname to check instead).
fn generate_node_tls(ca: &Ca) -> TlsConfig {
    let key = KeyPair::generate().expect("generate node key");
    let params = CertificateParams::new(vec![sundog::net::MESH_SERVER_NAME.to_string()])
        .expect("node params");
    let cert = params.signed_by(&key, &ca.issuer).expect("sign node cert");
    let private_key: PrivateKeyDer<'static> =
        key.serialize_der().try_into().expect("valid pkcs8 der");
    TlsConfig {
        cert_chain: vec![cert.der().clone()],
        private_key: Arc::new(private_key),
        root_ca_certs: vec![ca.der.clone()],
    }
}

/// Mirrors `common`'s own (private) `reserve_gossip_addr`: the only way,
/// from outside the crate, to learn a concrete gossip address before a node
/// exists — both nodes in a `Static`-seeded pair need each other's up front.
async fn reserve_gossip_addr() -> SocketAddr {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind an ephemeral loopback udp port to reserve a gossip address");
    socket
        .local_addr()
        .expect("a just-bound udp socket reports a local address")
}

fn tls_node_config(gossip_bind_addr: SocketAddr, tls: TlsConfig) -> ClusterConfig {
    common::fast_config().with(|c| {
        c.gossip_bind_addr = gossip_bind_addr;
        c.tls = Some(tls);
    })
}

#[tokio::test]
async fn nodes_sharing_a_ca_replicate_a_put_over_tls() {
    let ca = generate_ca();
    let gossip_a = reserve_gossip_addr().await;
    let gossip_b = reserve_gossip_addr().await;

    let cluster_a = Cluster::builder("it-tls-shared-ca")
        .seeds([gossip_b])
        .config(tls_node_config(gossip_a, generate_node_tls(&ca)))
        .build()
        .await
        .expect("node a builds with tls");
    let cluster_b = Cluster::builder("it-tls-shared-ca")
        .seeds([gossip_a])
        .config(tls_node_config(gossip_b, generate_node_tls(&ca)))
        .build()
        .await
        .expect("node b builds with tls");

    common::wait_for_peer_count(&cluster_a, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster_b, 1, Duration::from_secs(15)).await;

    let (cache_a, cache_b) = tokio::join!(
        cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");

    cache_a
        .insert(1, "hello".into())
        .await
        .expect("a inserts over tls");

    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.as_deref() == Some("hello")
    })
    .await;

    common::shutdown_all(vec![
        common::Node {
            cluster: cluster_a,
            gossip_addr: gossip_a,
        },
        common::Node {
            cluster: cluster_b,
            gossip_addr: gossip_b,
        },
    ])
    .await;
}

#[tokio::test]
async fn nodes_with_certs_from_different_cas_never_replicate() {
    let ca_a = generate_ca();
    let ca_b = generate_ca();
    let gossip_a = reserve_gossip_addr().await;
    let gossip_b = reserve_gossip_addr().await;

    // Every state-transfer attempt in this test is doomed (that's the
    // point), so shrink the budget each `open()` burns failing TLS
    // handshakes from the 20s default — which also exercises the knob:
    // `open()` must return once *this* budget elapses, not the default.
    let doomed_transfer = |config: sundog::ClusterConfig| {
        config.with(|c| c.state_transfer_budget = Duration::from_secs(2))
    };
    let cluster_a = Cluster::builder("it-tls-mismatched-ca")
        .seeds([gossip_b])
        .config(doomed_transfer(tls_node_config(
            gossip_a,
            generate_node_tls(&ca_a),
        )))
        .build()
        .await
        .expect("node a builds with tls");
    let cluster_b = Cluster::builder("it-tls-mismatched-ca")
        .seeds([gossip_a])
        .config(doomed_transfer(tls_node_config(
            gossip_b,
            generate_node_tls(&ca_b),
        )))
        .build()
        .await
        .expect("node b builds with tls");

    // Membership itself is UDP gossip, never TLS-wrapped — the two nodes see
    // each other here regardless, proving the negative result below is
    // specifically about the TCP data plane, not a total node failure.
    common::wait_for_peer_count(&cluster_a, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster_b, 1, Duration::from_secs(15)).await;

    // Concurrent: each side's own state-transfer attempt against the other
    // (Mode::Replicated always tries a donor on open) burns its own
    // `state_transfer_budget` failing every TLS handshake — run them in
    // parallel rather than paying that cost twice over.
    let (cache_a, cache_b) = tokio::join!(
        cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens despite a stalled state transfer");
    let cache_b = cache_b.expect("b opens despite a stalled state transfer");

    cache_a
        .insert(1, "hello".into())
        .await
        .expect("a inserts locally");

    // No positive bound to wait on here — a mismatched-CA mesh never
    // delivers, so this asserts absence stays true across the same window
    // the positive test uses to observe convergence.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        cache_b.get(&1).await,
        None,
        "a write must never cross a TLS mesh between nodes with unrelated root CAs"
    );

    common::shutdown_all(vec![
        common::Node {
            cluster: cluster_a,
            gossip_addr: gossip_a,
        },
        common::Node {
            cluster: cluster_b,
            gossip_addr: gossip_b,
        },
    ])
    .await;
}
