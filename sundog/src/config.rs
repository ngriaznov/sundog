//! Cluster-wide tunables: the values and their defaults, consumed by the
//! `Cluster` builder.

use std::net::SocketAddr;
use std::time::Duration;

#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::wire::MAX_FRAME;

/// Mutual-TLS material for the data-plane mesh (feature `tls`). Set
/// [`ClusterConfig::tls`] to wrap every data-plane connection in TLS;
/// client certificates are verified against `root_ca_certs` too.
///
/// A node with `tls: None` and one with `tls: Some(_)` cannot join the same
/// mesh: every connection between them fails outright rather than silently
/// downgrading. Every certificate must carry [`crate::net::MESH_SERVER_NAME`]
/// as a DNS SAN. A `sim`-feature build stays plaintext regardless of this
/// field.
#[cfg(feature = "tls")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// This node's certificate chain, leaf first, DER-encoded.
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// This node's private key, matching `cert_chain`'s leaf certificate.
    /// `Arc`-wrapped so `TlsConfig` stays cheaply `Clone`.
    pub private_key: Arc<PrivateKeyDer<'static>>,
    /// DER-encoded root CA certificate(s) trusted on both sides of the
    /// handshake.
    pub root_ca_certs: Vec<CertificateDer<'static>>,
}

/// Tunable knobs for a running cluster. Every field has a sane zeroconf
/// default; `Cluster::builder` exposes setters only for the ones worth
/// overriding day to day.
///
/// `#[non_exhaustive]`: a field added here in a future release must not
/// break code that only overrides a few knobs. Use [`ClusterConfig::default`]
/// and [`ClusterConfig::with`] to change a subset of fields.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ClusterConfig {
    /// Base interval between a node's anti-entropy rounds. The actual delay
    /// is jittered around this value to avoid thundering-herd rounds.
    pub ae_interval: Duration,
    /// How long a tombstone is retained before garbage collection. Must be
    /// at least `3 * ae_interval` so a lagging peer gets a few anti-entropy
    /// rounds to observe the deletion first.
    ///
    /// While a member is absent, a `Replicated`-mode cache defers collection
    /// past this point, up to [`tombstone_max_ttl`](Self::tombstone_max_ttl),
    /// so that member can't resurrect the entry via anti-entropy on return.
    pub tombstone_ttl: Duration,
    /// Hard cap on tombstone retention regardless of any member's absence.
    /// Bounds [`tombstone_ttl`](Self::tombstone_ttl)'s deferral against a
    /// member that never comes back.
    pub tombstone_max_ttl: Duration,
    /// Bounded capacity of each per-peer outbox (`mpsc`) on the data plane.
    pub outbox_capacity: usize,
    /// Hard cap on a single wire frame, in bytes. Must not exceed
    /// [`crate::wire::MAX_FRAME`]; `build()` rejects a higher value with
    /// [`crate::error::JoinError::InvalidConfig`].
    pub max_frame: usize,
    /// Bind address for the gossip (membership) UDP socket. Port `0` picks the
    /// zeroconf default.
    pub gossip_bind_addr: SocketAddr,
    /// Bind address for the data-plane TCP listener. Port `0` picks the
    /// zeroconf default.
    pub data_bind_addr: SocketAddr,
    /// Cadence of chitchat's own SWIM gossip rounds, distinct from
    /// [`ae_interval`](Self::ae_interval). Chosen for sub-5s failure
    /// detection on a LAN.
    pub gossip_interval: Duration,
    /// Phi-accrual failure-detector suspicion threshold: a peer is flagged
    /// faulty once its accrual value crosses this. Higher tolerates more
    /// jitter at the cost of slower detection.
    pub phi_threshold: f64,
    /// Sample window size behind the phi-accrual calculation.
    pub phi_sampling_window_size: usize,
    /// Upper bound on the failure detector's inter-heartbeat interval; wider
    /// gaps are dropped from the sample window.
    pub phi_max_interval: Duration,
    /// Initial assumed heartbeat interval, used before the failure detector
    /// has enough samples of its own to adapt.
    pub phi_initial_interval: Duration,
    /// How long a dead node's chitchat state is retained before this node
    /// forgets it. Bounded well below chitchat's own 24h default.
    pub dead_node_grace_period: Duration,
    /// Grace period for tombstoned chitchat key-values, distinct from this
    /// crate's own cache tombstones, before chitchat garbage-collects them.
    pub kv_tombstone_grace_period: Duration,
    /// Wall-clock budget for the state transfer a [`Mode::Replicated`] cache
    /// runs inside `open()`: how long a joining node pulls a snapshot from
    /// donors before giving up and proceeding with whatever it has. A
    /// startup-latency bound, not a correctness one: anti-entropy repairs
    /// whatever the cut-off transfer didn't deliver. A fifth of it is the
    /// grace a node with no peer in sight waits for gossip to show one
    /// before it opens as the cluster's origin. A cache whose transfer times
    /// out opens cold, declining to donate, and keeps pulling in the
    /// background every `ae_interval`; after three timed-out pulls it opens
    /// warm with what landed. Zero is honored: `open()` skips the transfer
    /// entirely and the cache is warm with what it has.
    ///
    /// [`Mode::Replicated`]: crate::Mode::Replicated
    pub state_transfer_budget: Duration,
    /// Bucket size past which an anti-entropy responder answers a mismatch
    /// with its 64 part digests instead of a listing or sketch, narrowing
    /// the mismatch to whichever parts actually differ before either side
    /// sends anything at bucket scale. Each mismatched part is then answered
    /// by the same rule [`ae_sketch_min_bucket`](Self::ae_sketch_min_bucket)
    /// applies at bucket scale: a sketch past that many entries, a listing
    /// otherwise. Effectively a three-tier responder rule: part digests,
    /// then sketch, then listing.
    pub ae_part_min_bucket: usize,
    /// Bucket size past which an anti-entropy responder answers a mismatch
    /// with an IBLT sketch instead of the bucket's full listing; the same
    /// rule applies to a mismatched part once
    /// [`ae_part_min_bucket`](Self::ae_part_min_bucket) has narrowed a
    /// bucket to it. A sketch costs a fixed ~9 KB at the default
    /// `ae_sketch_cells`; a listing costs ~23 bytes per entry, so 384 is the
    /// crossover for small keys.
    pub ae_sketch_min_bucket: usize,
    /// Cell count of the IBLT sketch built for a bucket past
    /// [`ae_sketch_min_bucket`](Self::ae_sketch_min_bucket). The default of
    /// 240 decodes a difference of up to 100 elements in at least 99% of
    /// cases; an undecodable one falls back to a full listing. A count whose
    /// sketch cannot fit in one [`max_frame`](Self::max_frame) frame fails
    /// [`ClusterBuilder::build`](crate::ClusterBuilder::build).
    pub ae_sketch_cells: usize,
    /// Mutual-TLS material for the data-plane mesh; `None` (the default)
    /// means the mesh runs plaintext. See [`TlsConfig`].
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

    /// Applies `f` to a mutable borrow of `self` and returns it: how code
    /// outside this crate overrides a subset of fields on
    /// [`ClusterConfig::default`] without breaking when a field is added.
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
            ae_part_min_bucket: 4_096,
            ae_sketch_min_bucket: 384,
            ae_sketch_cells: 240,
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
        assert_eq!(config.ae_sketch_min_bucket, 384);
        assert_eq!(config.ae_sketch_cells, 240);
    }

    #[test]
    fn default_ae_part_min_bucket_is_four_thousand_ninety_six() {
        assert_eq!(ClusterConfig::default().ae_part_min_bucket, 4_096);
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
