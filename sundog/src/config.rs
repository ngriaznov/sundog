//! Cluster-wide tunables. A plain, `Clone`-able settings struct consumed by
//! the `Cluster` builder (`src/cluster.rs`) — this module owns only the
//! values and their defaults, not the builder API.

use std::net::SocketAddr;
use std::time::Duration;

#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::wire::MAX_FRAME;

/// Mutual-TLS material for the data-plane mesh (feature `tls`; house rules
/// "Future plans pulled into v1": plan §14's "mTLS on the data plane
/// (rustls)"). Set [`ClusterConfig::tls`] (or use
/// [`crate::cluster::ClusterBuilder::tls`]) to wrap every accepted and
/// dialed data-plane connection — including the short-lived
/// request/response ones (state transfer, anti-entropy) — in TLS; client
/// certificates are verified against `root_ca_certs` too (mutual auth).
///
/// A node with `tls: None` and a node with `tls: Some(_)` cannot join the
/// same mesh: the plaintext side never speaks the TLS record layer the
/// other expects, so every connection between them fails outright rather
/// than silently downgrading — see the crate's internal `net::tls` module
/// docs for the full failure story, including why every certificate must
/// carry [`crate::net::MESH_SERVER_NAME`] as a DNS SAN. Applies only to the
/// real-tokio transport: a `sim`-feature build stays plaintext regardless of
/// this field.
#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// This node's certificate chain, leaf first, DER-encoded.
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// This node's private key, matching `cert_chain`'s leaf certificate.
    /// `Arc`-wrapped so `TlsConfig` stays cheaply `Clone` despite
    /// [`PrivateKeyDer`] itself not being `Clone`.
    pub private_key: Arc<PrivateKeyDer<'static>>,
    /// DER-encoded root CA certificate(s) trusted for verifying peers on
    /// both sides of the handshake: a server verifying a dialing client's
    /// certificate, and a dialing client verifying the server it connects
    /// to.
    pub root_ca_certs: Vec<CertificateDer<'static>>,
}

/// Tunable knobs for a running cluster. Every field has a sane zeroconf
/// default; `Cluster::builder` exposes setters only for the ones worth
/// overriding day to day.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterConfig {
    /// Base interval between a node's anti-entropy rounds. The actual delay
    /// is jittered around this value to avoid thundering-herd rounds across
    /// the cluster.
    pub ae_interval: Duration,
    /// How long a tombstone is retained before garbage collection. Must be at
    /// least `3 * ae_interval` (documented rule, plan §4) so a lagging peer
    /// gets at least a few anti-entropy rounds to observe the deletion before
    /// it is forgotten.
    pub tombstone_ttl: Duration,
    /// Bounded capacity of each per-peer outbox (`mpsc`) on the data plane.
    pub outbox_capacity: usize,
    /// Hard cap on a single wire frame, in bytes.
    pub max_frame: usize,
    /// Bind address for the gossip (membership) UDP socket. Port `0` picks an
    /// ephemeral port, which is the zeroconf default.
    pub gossip_bind_addr: SocketAddr,
    /// Bind address for the data-plane TCP listener. Port `0` picks an
    /// ephemeral port, which is the zeroconf default.
    pub data_bind_addr: SocketAddr,
    /// Cadence of chitchat's own SWIM gossip rounds — distinct from
    /// [`ae_interval`](Self::ae_interval), which paces this crate's own
    /// anti-entropy, not chitchat's membership gossip. Chosen for sub-5s
    /// failure detection on a LAN per plan §12 M1's acceptance bar.
    pub gossip_interval: Duration,
    /// Phi-accrual failure-detector suspicion threshold: a peer is flagged
    /// faulty once its accrual value crosses this. Higher tolerates more
    /// gossip jitter before suspecting a peer, at the cost of slower
    /// detection; see chitchat's own docs for the trade-off.
    pub phi_threshold: f64,
    /// Sample window size behind the phi-accrual calculation.
    pub phi_sampling_window_size: usize,
    /// Upper bound on the failure detector's inter-heartbeat interval;
    /// heartbeats spaced further apart than this are dropped from the
    /// sample window.
    pub phi_max_interval: Duration,
    /// Initial assumed heartbeat interval, used before the failure detector
    /// has enough samples of its own to adapt.
    pub phi_initial_interval: Duration,
    /// How long a dead node's chitchat state is retained before this node
    /// forgets it entirely. Bounded well below chitchat's own 24h default so
    /// a churning cluster doesn't grow memory unboundedly.
    pub dead_node_grace_period: Duration,
    /// Grace period for tombstoned *chitchat key-values* (distinct from this
    /// crate's own cache tombstones, [`tombstone_ttl`](Self::tombstone_ttl)
    /// above) before chitchat garbage-collects them.
    pub kv_tombstone_grace_period: Duration,
    /// Mutual-TLS material for the data-plane mesh (feature `tls`); `None`
    /// (the default) means the mesh runs plaintext. See [`TlsConfig`]'s own
    /// docs for what setting this implies.
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
}

impl ClusterConfig {
    /// Returns `true` if `tombstone_ttl` satisfies the `>= 3 * ae_interval`
    /// rule from plan §4.
    #[must_use]
    pub fn tombstone_ttl_is_safe(&self) -> bool {
        self.tombstone_ttl >= self.ae_interval.saturating_mul(3)
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            ae_interval: Duration::from_secs(30),
            tombstone_ttl: Duration::from_mins(10),
            outbox_capacity: 8_192,
            max_frame: MAX_FRAME,
            gossip_bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            data_bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            gossip_interval: Duration::from_millis(200),
            phi_threshold: 6.0,
            phi_sampling_window_size: 1_000,
            phi_max_interval: Duration::from_secs(5),
            phi_initial_interval: Duration::from_millis(500),
            dead_node_grace_period: Duration::from_secs(600),
            kv_tombstone_grace_period: Duration::from_mins(15),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_satisfy_the_tombstone_ttl_rule() {
        assert!(ClusterConfig::default().tombstone_ttl_is_safe());
    }

    #[test]
    fn defaults_bind_ephemeral_on_any_interface() {
        let config = ClusterConfig::default();
        assert_eq!(config.gossip_bind_addr.port(), 0);
        assert_eq!(config.data_bind_addr.port(), 0);
    }

    #[test]
    fn defaults_disable_tls() {
        #[cfg(feature = "tls")]
        assert!(ClusterConfig::default().tls.is_none());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tls_config_is_cheaply_cloneable_and_comparable() {
        use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

        let cert = CertificateDer::from(vec![1, 2, 3]);
        let key: PrivateKeyDer<'static> = PrivatePkcs8KeyDer::from(vec![4, 5, 6]).into();
        let tls = TlsConfig {
            cert_chain: vec![cert.clone()],
            private_key: Arc::new(key),
            root_ca_certs: vec![cert],
        };
        assert_eq!(tls.clone(), tls);
    }
}
