//! Feature `tls`: mutual TLS on the data-plane mesh. Nodes sharing a root
//! CA replicate normally over TLS; nodes whose certificates chain to
//! different CAs never fan a write out, though chitchat's gossip
//! membership over UDP still sees them.
//!
//! Certs are generated fresh per test with `rcgen`, no files on disk. This
//! binary duplicates the small cert-generation helpers `net::tls`'s own
//! unit tests use, private to `sundog`'s `src/`.

#![cfg(all(feature = "tls", not(feature = "sim")))]

mod common;

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
/// SAN (`sundog::net::MESH_SERVER_NAME`), mTLS having no per-node hostname.
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

fn tls_node_config(tls: TlsConfig) -> ClusterConfig {
    common::fast_config().with(|c| c.tls = Some(tls))
}

#[tokio::test]
async fn nodes_sharing_a_ca_replicate_a_put_over_tls() {
    let ca = generate_ca();

    // Enabled via `ClusterBuilder::tls`, not `ClusterConfig::tls` directly,
    // so this test covers both entry points to the same setting.
    let cluster_a = Cluster::builder("it-tls-shared-ca")
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .tls(generate_node_tls(&ca))
        .build()
        .await
        .expect("node a builds with tls");
    let cluster_b = Cluster::builder("it-tls-shared-ca")
        .seeds([cluster_a.local_gossip_addr()])
        .config(common::fast_config())
        .tls(generate_node_tls(&ca))
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

    common::shutdown_all(vec![cluster_a, cluster_b]).await;
}

/// `ClusterBuilder::tls` before `ClusterBuilder::config` leaves a node on TLS.
/// Node c takes its TLS setting from its config, and a TLS node and a
/// plaintext node refuse each other's connections, so a write reaches c from
/// a and b only if both run TLS.
#[tokio::test]
async fn nodes_with_tls_set_before_config_replicate_over_tls() {
    let ca = generate_ca();

    let cluster_a = Cluster::builder("it-tls-before-config")
        .seeds(std::iter::empty())
        .tls(generate_node_tls(&ca))
        .config(common::fast_config())
        .build()
        .await
        .expect("node a builds with tls before config");
    let cluster_b = Cluster::builder("it-tls-before-config")
        .seeds([cluster_a.local_gossip_addr()])
        .tls(generate_node_tls(&ca))
        .config(common::fast_config())
        .build()
        .await
        .expect("node b builds with tls before config");
    let cluster_c = Cluster::builder("it-tls-before-config")
        .seeds([cluster_a.local_gossip_addr()])
        .config(tls_node_config(generate_node_tls(&ca)))
        .build()
        .await
        .expect("node c builds with tls in its config");

    common::wait_for_peer_count(&cluster_a, 2, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster_b, 2, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster_c, 2, Duration::from_secs(15)).await;

    let (cache_a, cache_b, cache_c) = tokio::join!(
        cluster_a
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
        cluster_b
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
        cluster_c
            .cache::<u32, String>("users")
            .mode(Mode::Replicated)
            .open(),
    );
    let cache_a = cache_a.expect("a opens");
    let cache_b = cache_b.expect("b opens");
    let cache_c = cache_c.expect("c opens");

    cache_a
        .insert(1, "hello".into())
        .await
        .expect("a inserts over tls");
    cache_c
        .insert(2, "from c".into())
        .await
        .expect("c inserts over tls");

    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.as_deref() == Some("hello")
            && cache_c.get(&1).await.as_deref() == Some("hello")
            && cache_a.get(&2).await.as_deref() == Some("from c")
            && cache_b.get(&2).await.as_deref() == Some("from c")
    })
    .await;

    common::shutdown_all(vec![cluster_a, cluster_b, cluster_c]).await;
}

/// A raw socket that opens the data-plane port and stalls inside a
/// `ClientHello` holds a connection that has not authenticated. The nodes'
/// own links, which complete their handshakes, replicate beside it.
#[tokio::test]
async fn a_stalled_client_hello_does_not_stop_two_tls_nodes_replicating() {
    use tokio::io::AsyncWriteExt as _;

    let ca = generate_ca();
    let data_addr = common::reserve_tcp_addr().await;
    let cluster_a = Cluster::builder("it-tls-stalled-client-hello")
        .seeds(std::iter::empty())
        .config(tls_node_config(generate_node_tls(&ca)).with(|c| c.data_bind_addr = data_addr))
        .build()
        .await
        .expect("node a builds with tls");
    let cluster_b = Cluster::builder("it-tls-stalled-client-hello")
        .seeds([cluster_a.local_gossip_addr()])
        .config(tls_node_config(generate_node_tls(&ca)))
        .build()
        .await
        .expect("node b builds with tls");
    common::wait_for_peer_count(&cluster_a, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster_b, 1, Duration::from_secs(15)).await;

    // The 5-byte record header of a ClientHello, and nothing after it.
    let mut stalled = tokio::net::TcpStream::connect(data_addr)
        .await
        .expect("the data-plane port accepts");
    stalled
        .write_all(&[0x16, 0x03, 0x01, 0x01, 0x00])
        .await
        .expect("the start of a ClientHello");

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
        .insert(1, "from a".into())
        .await
        .expect("a inserts over tls");
    cache_b
        .insert(2, "from b".into())
        .await
        .expect("b inserts over tls");
    common::eventually(Duration::from_secs(10), || async {
        cache_b.get(&1).await.as_deref() == Some("from a")
            && cache_a.get(&2).await.as_deref() == Some("from b")
    })
    .await;

    drop(stalled);
    common::shutdown_all(vec![cluster_a, cluster_b]).await;
}

#[tokio::test]
async fn nodes_with_certs_from_different_cas_never_replicate() {
    let ca_a = generate_ca();
    let ca_b = generate_ca();

    // Every state-transfer attempt here is doomed, so shrink the budget
    // each `open()` burns failing TLS handshakes from the 20s default.
    let doomed_transfer = |config: sundog::ClusterConfig| {
        config.with(|c| c.state_transfer_budget = Duration::from_secs(2))
    };
    let cluster_a = Cluster::builder("it-tls-mismatched-ca")
        .seeds(std::iter::empty())
        .config(doomed_transfer(tls_node_config(generate_node_tls(&ca_a))))
        .build()
        .await
        .expect("node a builds with tls");
    let cluster_b = Cluster::builder("it-tls-mismatched-ca")
        .seeds([cluster_a.local_gossip_addr()])
        .config(doomed_transfer(tls_node_config(generate_node_tls(&ca_b))))
        .build()
        .await
        .expect("node b builds with tls");

    // Membership is UDP gossip, never TLS-wrapped, so the nodes see each
    // other here regardless of the TCP data plane's failure below.
    common::wait_for_peer_count(&cluster_a, 1, Duration::from_secs(15)).await;
    common::wait_for_peer_count(&cluster_b, 1, Duration::from_secs(15)).await;

    // Concurrent: each side's own state-transfer attempt burns its own
    // budget failing TLS handshakes, so run them in parallel.
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

    // No positive bound: a mismatched-CA mesh never delivers.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        cache_b.get(&1).await,
        None,
        "a write never crosses a TLS mesh between nodes with unrelated root CAs"
    );

    common::shutdown_all(vec![cluster_a, cluster_b]).await;
}
