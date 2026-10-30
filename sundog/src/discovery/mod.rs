//! Discovery: "who might be out there," the `JGroups` `PING` layer analog.
//! Feeds [`crate::membership::Membership::spawn`]'s seed stream.

pub mod dns;
pub mod mdns;
pub mod statics;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt as _};

/// A source of candidate cluster peers. Object-safe by construction
/// (`BoxStream`/`BoxFuture` returns rather than `impl Stream`/`impl
/// Future`), so a `Cluster` can hold `Box<dyn Discovery>`.
pub trait Discovery: Send + Sync + 'static {
    /// A continuous stream of candidate peer gossip addresses. Duplicates
    /// are expected; the stream must never terminate, so a restarted
    /// cluster can re-find itself.
    fn candidates(&self) -> BoxStream<'static, SocketAddr>;

    /// Makes this node findable by other instances of the same mechanism.
    /// A no-op for sources with nothing to announce (`Static`, `DnsSrv`).
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] if the announce mechanism fails to start.
    fn announce(&self, gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>>;
}

/// The discovery source a [`crate::cluster::ClusterBuilder`] holds: one of
/// the three built-in mechanisms, or a caller-supplied implementation.
pub enum DiscoveryKind {
    /// The zeroconf default: `mdns-sd`-based LAN discovery.
    Mdns(mdns::Mdns),
    /// A fixed seed list, from the builder and/or `SUNDOG_SEEDS`.
    Static(statics::Static),
    /// SRV-record discovery against a headless service name. Boxed since a
    /// `TokioResolver` dwarfs the other variants.
    DnsSrv(Box<dns::DnsSrv>),
    /// A caller-supplied discovery mechanism.
    Custom(Box<dyn Discovery>),
}

impl DiscoveryKind {
    /// Whether this discovery source names a fixed, nonempty seed list:
    /// only [`Self::Static`] ever does, and only when its own
    /// [`statics::Static::has_seeds`] says so. `Mdns`, `DnsSrv`, and a
    /// caller-supplied [`Self::Custom`] source all find peers on their own
    /// schedule rather than naming any upfront, so a lone node under any of
    /// them is a legitimate standalone cluster, not evidence of a
    /// membership race `Cache::open` should wait out.
    pub(crate) fn has_seeds(&self) -> bool {
        matches!(self, Self::Static(discovery) if discovery.has_seeds())
    }
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

/// `source`'s [`Discovery::candidates`], holding `source` for as long as the
/// stream lives. A source whose mechanism runs only while it exists, as an
/// [`mdns::Mdns`] daemon does, then runs exactly as long as the gossip loop
/// that reads its candidates, and stops when that loop drops the stream. A
/// custom source whose candidates end, against [`Discovery`]'s contract,
/// leaves a stream that answers `None` from then on, however often the
/// initial seed collection and the gossip loop poll it.
pub(crate) fn candidates_holding(source: Arc<DiscoveryKind>) -> BoxStream<'static, SocketAddr> {
    let candidates = source.candidates();
    stream::unfold(
        (candidates, source),
        |(mut candidates, source)| async move {
            let addr = candidates.next().await?;
            Some((addr, (candidates, source)))
        },
    )
    .fuse()
    .boxed()
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
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// A source that never yields and records when it drops.
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    impl Discovery for Dropped {
        fn candidates(&self) -> BoxStream<'static, SocketAddr> {
            stream::pending().boxed()
        }

        fn announce(&self, _gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn candidates_holding_keeps_its_source_until_the_stream_drops() {
        let dropped = Arc::new(AtomicBool::new(false));
        let boxed: Box<dyn Discovery> = Box::new(Dropped(Arc::clone(&dropped)));
        let source = Arc::new(DiscoveryKind::from(boxed));

        let candidates = candidates_holding(Arc::clone(&source));
        drop(source);
        assert!(
            !dropped.load(Ordering::SeqCst),
            "the stream holds the source"
        );
        drop(candidates);
        assert!(
            dropped.load(Ordering::SeqCst),
            "and releases it when the stream drops"
        );
    }

    /// A source whose candidates end, which the [`Discovery`] contract
    /// forbids and a custom source can still do.
    struct Ending(Vec<SocketAddr>);

    impl Discovery for Ending {
        fn candidates(&self) -> BoxStream<'static, SocketAddr> {
            stream::iter(self.0.clone()).boxed()
        }

        fn announce(&self, _gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn candidates_holding_answers_none_for_good_once_its_source_ends() {
        let addr: SocketAddr = "127.0.0.1:4000".parse().expect("valid addr");
        let boxed: Box<dyn Discovery> = Box::new(Ending(vec![addr]));
        let mut candidates = candidates_holding(Arc::new(DiscoveryKind::from(boxed)));
        assert_eq!(candidates.next().await, Some(addr));
        assert_eq!(candidates.next().await, None);
        assert_eq!(
            candidates.next().await,
            None,
            "a stream polled again after it ended answers None, as the gossip loop polls it \
             after the initial seed collection saw the end"
        );
    }

    #[tokio::test]
    async fn candidates_holding_yields_the_source_candidates() {
        let seeds: Vec<SocketAddr> = vec![
            "127.0.0.1:4000".parse().expect("valid addr"),
            "127.0.0.1:4001".parse().expect("valid addr"),
        ];
        let source = Arc::new(DiscoveryKind::from(statics::Static::new(seeds.clone())));
        let first: Vec<SocketAddr> = candidates_holding(source).take(seeds.len()).collect().await;
        assert_eq!(first, seeds);
    }

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

    #[test]
    fn has_seeds_is_true_only_for_a_static_source_with_a_nonempty_seed_list() {
        let seed_addr: SocketAddr = "127.0.0.1:4000".parse().expect("valid addr");
        let seeded: DiscoveryKind = statics::Static::new([seed_addr]).into();
        let seedless: DiscoveryKind = statics::Static::new(std::iter::empty()).into();
        let mdns: DiscoveryKind = mdns::Mdns::new("test-cluster", "test-node").into();
        let dns: DiscoveryKind = dns::DnsSrv::new("_sundog._tcp.local.", 7946).into();

        assert!(seeded.has_seeds());
        assert!(!seedless.has_seeds());
        assert!(!mdns.has_seeds());
        assert!(!dns.has_seeds());
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
