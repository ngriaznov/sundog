//! Discovery: "who might be out there" — the `JGroups` `PING` layer analog.
//! Continuously produces candidate gossip addresses that feed
//! [`crate::membership::Membership::spawn`]'s seed stream.

pub mod dns;
pub mod mdns;
pub mod statics;

use std::io;
use std::net::SocketAddr;

use futures::future::BoxFuture;
use futures::stream::BoxStream;

/// A source of candidate cluster peers.
///
/// Object-safe by construction (`BoxStream`/`BoxFuture` return types rather
/// than `impl Stream`/`impl Future`) so a `Cluster` can hold
/// `Box<dyn Discovery>` and the builder can accept any implementation
/// uniformly — RPITIT return types are not object-safe, so this trait
/// returns boxed futures and streams instead.
pub trait Discovery: Send + Sync + 'static {
    /// A continuous stream of candidate peer gossip addresses. Duplicates are
    /// fine and expected; the stream must never terminate on its own — a
    /// fully restarted cluster relies on continuous (not one-shot) discovery
    /// to re-find itself.
    fn candidates(&self) -> BoxStream<'static, SocketAddr>;

    /// Makes this node findable by other instances of the same discovery
    /// mechanism (e.g. registers an mDNS service record). A no-op for
    /// discovery sources with nothing to announce (`Static`, `DnsSrv`).
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] if the announce mechanism fails to start.
    fn announce(&self, gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>>;
}

/// The discovery source a [`crate::cluster::ClusterBuilder`] holds: one of
/// the three built-in mechanisms, or a caller-supplied implementation.
/// Implements [`Discovery`] itself by delegating to whichever variant it
/// holds, so callers never need to box a built-in source to store it
/// alongside a custom one.
pub enum DiscoveryKind {
    /// The zeroconf default: `mdns-sd`-based LAN discovery.
    Mdns(mdns::Mdns),
    /// A fixed seed list, from the builder and/or `SUNDOG_SEEDS`.
    Static(statics::Static),
    /// SRV-record discovery against a headless service name. Boxed: a
    /// `TokioResolver` makes this variant far larger than its siblings.
    DnsSrv(Box<dns::DnsSrv>),
    /// A caller-supplied discovery mechanism.
    Custom(Box<dyn Discovery>),
}

impl Discovery for DiscoveryKind {
    fn candidates(&self) -> BoxStream<'static, SocketAddr> {
        match self {
            Self::Mdns(discovery) => discovery.candidates(),
            Self::Static(discovery) => discovery.candidates(),
            Self::DnsSrv(discovery) => discovery.candidates(),
            Self::Custom(discovery) => discovery.candidates(),
        }
    }

    fn announce(&self, gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>> {
        match self {
            Self::Mdns(discovery) => discovery.announce(gossip_addr),
            Self::Static(discovery) => discovery.announce(gossip_addr),
            Self::DnsSrv(discovery) => discovery.announce(gossip_addr),
            Self::Custom(discovery) => discovery.announce(gossip_addr),
        }
    }
}

impl From<mdns::Mdns> for DiscoveryKind {
    fn from(discovery: mdns::Mdns) -> Self {
        Self::Mdns(discovery)
    }
}

impl From<statics::Static> for DiscoveryKind {
    fn from(discovery: statics::Static) -> Self {
        Self::Static(discovery)
    }
}

impl From<dns::DnsSrv> for DiscoveryKind {
    fn from(discovery: dns::DnsSrv) -> Self {
        Self::DnsSrv(Box::new(discovery))
    }
}

impl From<Box<dyn Discovery>> for DiscoveryKind {
    fn from(discovery: Box<dyn Discovery>) -> Self {
        Self::Custom(discovery)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_built_in_source_is_usable_as_a_trait_object() {
        let sources: Vec<Box<dyn Discovery>> = vec![
            Box::new(statics::Static::new(std::iter::empty())),
            Box::new(mdns::Mdns::new("test-cluster", "test-node")),
            Box::new(dns::DnsSrv::new("_sundog._tcp.local.", 7946)),
        ];
        for source in &sources {
            let _candidates = source.candidates();
        }
    }

    #[tokio::test]
    async fn discovery_kind_delegates_to_the_held_variant() {
        let via_static: DiscoveryKind = statics::Static::new(std::iter::empty()).into();
        let via_dns: DiscoveryKind = dns::DnsSrv::new("_sundog._tcp.local.", 7946).into();
        let boxed: Box<dyn Discovery> = Box::new(statics::Static::new(std::iter::empty()));
        let via_custom: DiscoveryKind = boxed.into();

        for kind in [via_static, via_dns, via_custom] {
            let _candidates = kind.candidates();
        }
    }
}
