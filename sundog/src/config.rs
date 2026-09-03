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

/// Mutual-TLS material for the data-plane mesh (feature `tls`). Set
/// [`ClusterConfig::tls`] (or use
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
///
/// `#[non_exhaustive]`: a field added here in a future release must not
/// break code that only overrides a few knobs. Construct one with
/// [`ClusterConfig::default`] and [`ClusterConfig::with`] to change a subset
/// of fields — the fully-public fields remain directly readable and
/// writable on an existing value.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ClusterConfig {
    /// Base interval between a node's anti-entropy rounds. The actual delay
    /// is jittered around this value to avoid thundering-herd rounds across
    /// the cluster.
    pub ae_interval: Duration,
    /// How long a tombstone is retained before garbage collection, once
    /// every recently-known cluster member is accounted for. Must be at
    /// least `3 * ae_interval` so a lagging peer gets at least a few
    /// anti-entropy rounds to observe the deletion before it is forgotten.
    ///
    /// While a member is absent from the live peer set, a `Replicated`-mode
    /// cache defers collection past this point — up to
    /// [`tombstone_max_ttl`](Self::tombstone_max_ttl) — so that member can't
    /// resurrect the deleted entry via anti-entropy once it returns. The
    /// trade-off: during a member outage, tombstones for deletes accumulate
    /// (tens of bytes each) until the member returns or `tombstone_max_ttl`
    /// expires.
    pub tombstone_ttl: Duration,
    /// Hard cap on tombstone retention: once a tombstone is older than this,
    /// it is garbage-collected regardless of any member's absence. Bounds
    /// the memory trade described on [`tombstone_ttl`](Self::tombstone_ttl)
    /// against a member that never comes back.
    pub tombstone_max_ttl: Duration,
    /// Bounded capacity of each per-peer outbox (`mpsc`) on the data plane.
    pub outbox_capacity: usize,
    /// Hard cap on a single wire frame, in bytes. Must not exceed
    /// [`crate::wire::MAX_FRAME`] (the wire codec's own hard-coded cap) —
    /// [`crate::cluster::ClusterBuilder::build`] rejects a config that sets
    /// this higher with [`crate::error::JoinError::InvalidConfig`].
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
    /// failure detection on a LAN.
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
    /// Wall-clock budget for the state transfer a [`Mode::Replicated`] cache
    /// runs inside `open()` (and inside the deferred warm-up when `open()`
    /// raced gossip convergence): how long a joining node keeps pulling a
    /// snapshot from donors before giving up and proceeding with whatever it
    /// has. A startup-latency bound, not a correctness one — anti-entropy
    /// repairs whatever the cut-off transfer didn't deliver — so size it for
    /// how long you are willing to have `open()` block, not for safety.
    /// Transfer moves roughly tens of thousands of small entries per second
    /// on a LAN; the default comfortably covers caches into the
    /// hundreds of thousands of entries, while a million-entry cache warms
    /// fully inside `open()` only if this is raised to match. Zero is
    /// honored: `open()` skips waiting entirely and leaves all warming to
    /// anti-entropy.
    ///
    /// [`Mode::Replicated`]: crate::Mode::Replicated
    pub state_transfer_budget: Duration,
    /// Local entry-list length above which an anti-entropy responder answers
    /// a mismatched bucket with an IBLT sketch (`Msg::AeSketch`) instead of
    /// the bucket's full `(key, version)` listing (`Msg::AeBucket`). Below
    /// this point a listing is already cheap on the wire and a fixed-cost
    /// sketch reply buys nothing; past it, a sketch's wire cost stops
    /// growing with the bucket while a listing's keeps climbing, so the
    /// sketch increasingly wins the larger the bucket gets regardless of how
    /// large the actual diff turns out to be.
    pub ae_sketch_min_bucket: usize,
    /// Cell count of the IBLT sketch an anti-entropy responder builds for a
    /// bucket past [`ae_sketch_min_bucket`](Self::ae_sketch_min_bucket).
    /// Rated (see `cluster::sketch::RATED_CAPACITY`, whose own docs cover
    /// why this is a statistical rather than absolute guarantee) to decode
    /// any symmetric difference up to 40 elements with overwhelming
    /// probability; a larger true difference — or, on the rare documented
    /// hash-collision case, sometimes even a smaller one — falls back to
    /// `Msg::AeEntries`'s full listing rather than ever risking a wrong
    /// decode.
    pub ae_sketch_cells: usize,
    /// Mutual-TLS material for the data-plane mesh (feature `tls`); `None`
    /// (the default) means the mesh runs plaintext. See [`TlsConfig`]'s own
    /// docs for what setting this implies.
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
}

impl ClusterConfig {
    /// Returns `true` if `tombstone_ttl` satisfies the `>= 3 * ae_interval`
    /// rule.
    #[must_use]
    pub fn tombstone_ttl_is_safe(&self) -> bool {
        self.tombstone_ttl >= self.ae_interval.saturating_mul(3)
    }

    /// Applies `f` to a mutable borrow of `self` and returns it — since
    /// `ClusterConfig` is `#[non_exhaustive]`, this (rather than struct-update
    /// syntax) is how code outside this crate overrides a subset of fields on
    /// top of [`ClusterConfig::default`] without breaking when a new field is
    /// added.
    #[must_use]
    pub fn with(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            ae_interval: Duration::from_secs(30),
            tombstone_ttl: Duration::from_mins(10),
            tombstone_max_ttl: Duration::from_hours(24),
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
            state_transfer_budget: Duration::from_secs(20),
            ae_sketch_min_bucket: 256,
            ae_sketch_cells: 951,
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
    fn default_tombstone_max_ttl_bounds_the_default_tombstone_ttl() {
        let config = ClusterConfig::default();
        assert!(config.tombstone_max_ttl > config.tombstone_ttl);
    }

    #[test]
    fn with_overrides_only_the_touched_fields() {
        let config = ClusterConfig::default().with(|c| {
            c.ae_interval = Duration::from_millis(1);
        });
        assert_eq!(config.ae_interval, Duration::from_millis(1));
        assert_eq!(
            config.outbox_capacity,
            ClusterConfig::default().outbox_capacity
        );
    }

    #[test]
    fn default_ae_sketch_knobs_match_the_rated_sketch_shape() {
        let config = ClusterConfig::default();
        assert_eq!(config.ae_sketch_min_bucket, 256);
        assert_eq!(config.ae_sketch_cells, 951);
    }

    #[test]
    fn default_state_transfer_budget_is_twenty_seconds() {
        assert_eq!(
            ClusterConfig::default().state_transfer_budget,
            Duration::from_secs(20)
        );
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
