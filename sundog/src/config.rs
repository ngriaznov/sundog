//! Cluster-wide tunables. A plain, `Clone`-able settings struct consumed by
//! the `Cluster` builder (`src/cluster.rs`) — this module owns only the
//! values and their defaults, not the builder API.

use std::net::SocketAddr;
use std::time::Duration;

use crate::wire::MAX_FRAME;

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
}
