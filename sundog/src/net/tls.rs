//! Mutual TLS for the data plane (feature `tls`; house rules "Future plans
//! pulled into v1": plan §14's "mTLS on the data plane (rustls)"). Every
//! persistent and short-lived connection — accepted and dialed alike, so the
//! request/response ones (state transfer, anti-entropy) too — is wrapped
//! once this node's [`ClusterConfig::tls`](crate::config::ClusterConfig::tls)
//! is set; the dialing side also presents a certificate, verified against
//! the same root CA as the accepting side (mutual auth).
//!
//! Peer identity here comes entirely from chain-of-trust to the shared root
//! CA, not from hostname verification: nodes are addressed by ephemeral
//! `ip:port` (mDNS/gossip-discovered), not stable DNS names, so there is no
//! meaningful per-node hostname to check either side of the handshake
//! against. Every certificate this mesh accepts must instead carry the
//! fixed [`MESH_SERVER_NAME`] as a DNS Subject Alternative Name — documented
//! loudly here because it is easy to trip over when hand-rolling test or
//! ops certificates.
//!
//! A TLS-configured node and a plaintext node cannot join the same mesh: the
//! plaintext side never speaks the TLS record layer the other expects, so
//! the connection fails outright rather than silently downgrading —
//! retried by [`super::conn::run_peer_writer`]'s backoff on the dial side,
//! silently dropped by [`super::conn::accept_loop`] on the accept side,
//! exactly like any other connection failure.
//!
//! Applies only to the real-tokio transport: this module, and every call
//! into it, is compiled only under `not(feature = "sim")` — a turmoil-hosted
//! simulation build stays plaintext regardless of `ClusterConfig::tls` (see
//! `net`'s `TlsCtx`/`MeshStream` aliases).

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::pki_types::ServerName;
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::tcp::TcpStream;
use crate::config::TlsConfig;

/// The fixed DNS name every certificate on a TLS-enabled mesh must carry as
/// a Subject Alternative Name — see this module's docs for why a per-node
/// hostname isn't the right check here.
pub const MESH_SERVER_NAME: &str = "sundog-mesh.internal";

/// A dialed or accepted mesh connection, plaintext or TLS-wrapped: the type
/// every [`super::conn`] framing function operates over once this feature is
/// active, so call sites never need to know which they got.
pub(crate) enum MeshStream {
    Plain(TcpStream),
    // Boxed: `tokio_rustls::TlsStream` carries a full client/server session
    // state machine, dwarfing a bare `TcpStream` — leaving it unboxed here
    // would blow up every `MeshStream` to the size of its biggest variant.
    Tls(Box<tokio_rustls::TlsStream<TcpStream>>),
}

impl AsyncRead for MeshStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MeshStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// This node's rustls connector/acceptor pair, built once from its
/// [`TlsConfig`] and shared by every dial and accept for the life of the
/// owning [`super::Mesh`].
pub(crate) struct MeshTls {
    connector: TlsConnector,
    acceptor: TlsAcceptor,
}

impl MeshTls {
    /// # Errors
    ///
    /// Returns [`rustls::Error`] if `config`'s certificate chain, key, or
    /// root CA material is malformed or internally inconsistent (e.g. the
    /// private key doesn't match the leaf certificate).
    pub(crate) fn new(config: &TlsConfig) -> Result<Self, rustls::Error> {
        let mut roots = RootCertStore::empty();
        for ca in &config.root_ca_certs {
            roots.add(ca.clone())?;
        }
        let roots = Arc::new(roots);

        let client_verifier = WebPkiClientVerifier::builder(Arc::clone(&roots))
            .build()
            .map_err(|source| rustls::Error::General(source.to_string()))?;
        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(config.cert_chain.clone(), config.private_key.clone_key())?;

        let client_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(config.cert_chain.clone(), config.private_key.clone_key())?;

        Ok(Self {
            connector: TlsConnector::from(Arc::new(client_config)),
            acceptor: TlsAcceptor::from(Arc::new(server_config)),
        })
    }

    pub(crate) async fn connect(&self, stream: TcpStream) -> io::Result<MeshStream> {
        let name = ServerName::try_from(MESH_SERVER_NAME)
            .expect("invariant: MESH_SERVER_NAME is a valid DNS name literal");
        let stream = self.connector.connect(name, stream).await?;
        Ok(MeshStream::Tls(Box::new(stream.into())))
    }

    pub(crate) async fn accept(&self, stream: TcpStream) -> io::Result<MeshStream> {
        let stream = self.acceptor.accept(stream).await?;
        Ok(MeshStream::Tls(Box::new(stream.into())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;

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

    fn generate_node_cert(ca: &Issuer<'static, KeyPair>) -> (CertificateDer<'static>, KeyPair) {
        let key = KeyPair::generate().expect("generate node key");
        let params =
            CertificateParams::new(vec![MESH_SERVER_NAME.to_string()]).expect("node params");
        let cert = params.signed_by(&key, ca).expect("sign node cert");
        (cert.der().clone(), key)
    }

    fn tls_config(
        ca_der: &CertificateDer<'static>,
        leaf: CertificateDer<'static>,
        key: &KeyPair,
    ) -> TlsConfig {
        let private_key: PrivateKeyDer<'static> =
            key.serialize_der().try_into().expect("valid pkcs8 der");
        TlsConfig {
            cert_chain: vec![leaf],
            private_key: StdArc::new(private_key),
            root_ca_certs: vec![ca_der.clone()],
        }
    }

    #[tokio::test]
    async fn mutual_tls_handshake_succeeds_between_certs_from_the_same_ca() {
        let ca = generate_ca();
        let (server_cert, server_key) = generate_node_cert(&ca.issuer);
        let (client_cert, client_key) = generate_node_cert(&ca.issuer);
        let server_tls = MeshTls::new(&tls_config(&ca.der, server_cert, &server_key))
            .expect("server tls config");
        let client_tls = MeshTls::new(&tls_config(&ca.der, client_cert, &client_key))
            .expect("client tls config");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut tls = server_tls.accept(stream).await.expect("server handshake");
            let mut buf = [0u8; 5];
            tls.read_exact(&mut buf).await.expect("read from client");
            buf
        });

        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("dial loopback");
        let mut tls = client_tls.connect(stream).await.expect("client handshake");
        tls.write_all(b"hello").await.expect("write to server");

        let got = server.await.expect("server task did not panic");
        assert_eq!(&got, b"hello");
    }

    #[tokio::test]
    async fn a_client_certificate_from_a_different_ca_is_rejected() {
        let server_ca = generate_ca();
        let other_ca = generate_ca();
        let (server_cert, server_key) = generate_node_cert(&server_ca.issuer);
        let (client_cert, client_key) = generate_node_cert(&other_ca.issuer);
        let server_tls = MeshTls::new(&tls_config(&server_ca.der, server_cert, &server_key))
            .expect("server tls config");
        // The client trusts the server's CA (so it accepts the server's
        // certificate), but presents a leaf issued by a different CA — the
        // side of mutual auth this test exercises.
        let client_tls = MeshTls::new(&tls_config(&server_ca.der, client_cert, &client_key))
            .expect("client tls config");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener has a local addr");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            server_tls.accept(stream).await
        });

        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("dial loopback");
        let client_result = client_tls.connect(stream).await;
        let server_result = server.await.expect("server task did not panic");

        assert!(
            client_result.is_err() || server_result.is_err(),
            "a client cert from an untrusted CA must fail the handshake on at least one side"
        );
    }
}
