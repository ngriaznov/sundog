//! `DnsSrv`: resolves a headless-service name on an interval via
//! `hickory-resolver`. The Kubernetes answer, equivalent to `JGroups`'
//! `DNS_PING`.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use hickory_resolver::proto::rr::RData;
use hickory_resolver::{ResolverBuilder, TokioResolver};

use super::Discovery;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);

/// SRV-record discovery against a headless service name, e.g.
/// `_sundog._tcp.my-cluster.svc.cluster.local.` on Kubernetes. Falls back to
/// an A/AAAA lookup of `service_name`/`fallback_port` with no SRV records,
/// so a bare hostname works too. Polls on an interval to pick up rolling
/// DNS updates without a restart.
pub struct DnsSrv {
    resolver: Option<TokioResolver>,
    service_name: String,
    fallback_port: u16,
    interval: Duration,
}

impl DnsSrv {
    /// Builds a resolver against the system DNS configuration. A failed
    /// resolver degrades to a permanently empty candidate stream.
    #[must_use]
    pub fn new(service_name: impl Into<String>, fallback_port: u16) -> Self {
        let resolver = TokioResolver::builder_tokio()
            .and_then(ResolverBuilder::build)
            .inspect_err(|err| {
                tracing::warn!(%err, "DNS resolver failed to initialize; DnsSrv discovery disabled");
            })
            .ok();
        Self {
            resolver,
            service_name: service_name.into(),
            fallback_port,
            interval: DEFAULT_INTERVAL,
        }
    }

    /// Overrides the default poll interval of 30s.
    #[must_use]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

/// Lookup seam between [`resolve_once`] and the real resolver, swapped for a
/// fake in tests. `srv` returns each SRV answer as `(target host, port)`;
/// `ips` resolves one host to its A/AAAA addresses.
pub(crate) trait SrvLookup {
    async fn srv(&self, service: &str) -> io::Result<Vec<(String, u16)>>;
    async fn ips(&self, host: &str) -> io::Result<Vec<IpAddr>>;
}

impl SrvLookup for TokioResolver {
    async fn srv(&self, service: &str) -> io::Result<Vec<(String, u16)>> {
        let lookup = self.srv_lookup(service).await.map_err(io::Error::other)?;
        Ok(lookup
            .answers()
            .iter()
            .filter_map(|record| {
                let RData::SRV(srv) = &record.data else {
                    return None;
                };
                Some((srv.target.to_string(), srv.port))
            })
            .collect())
    }

    async fn ips(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        let ips = self.lookup_ip(host).await.map_err(io::Error::other)?;
        Ok(ips.iter().collect())
    }
}

/// One resolution round: SRV first, falling back to A/AAAA. Failures are
/// logged and yield an empty result, never an error. An SRV port of `0`
/// falls back to `fallback_port`: no real service is ever intentionally
/// advertised on port 0, so treat it as "unset" rather than dialing it.
async fn resolve_once<L: SrvLookup>(
    lookup: &L,
    service_name: &str,
    fallback_port: u16,
) -> Vec<SocketAddr> {
    match lookup.srv(service_name).await {
        Ok(targets) => {
            let mut addrs = Vec::new();
            for (target, port) in targets {
                let port = if port == 0 { fallback_port } else { port };
                match lookup.ips(&target).await {
                    Ok(ips) => addrs.extend(ips.into_iter().map(|ip| SocketAddr::new(ip, port))),
                    Err(err) => {
                        tracing::warn!(target = %target, %err, "SRV target failed to resolve");
                    }
                }
            }
            addrs
        }
        Err(err) => {
            tracing::debug!(%err, service_name, "SRV lookup failed, falling back to A/AAAA");
            match lookup.ips(service_name).await {
                Ok(ips) => ips
                    .into_iter()
                    .map(|ip| SocketAddr::new(ip, fallback_port))
                    .collect(),
                Err(err) => {
                    tracing::warn!(%err, service_name, "DNS discovery lookup failed");
                    Vec::new()
                }
            }
        }
    }
}

/// Builds the on-interval candidate stream over `lookup`, generic so tests
/// can drive it with a fake. [`DnsSrv::candidates`] instantiates this with
/// the real resolver and boxes the result.
fn candidate_ticks<L: SrvLookup>(
    lookup: L,
    service_name: String,
    fallback_port: u16,
    interval: Duration,
) -> impl futures::Stream<Item = SocketAddr> {
    let ticker = tokio::time::interval(interval);
    stream::unfold(
        (lookup, service_name, fallback_port, ticker),
        |(lookup, service_name, fallback_port, mut ticker)| async move {
            ticker.tick().await;
            let addrs = resolve_once(&lookup, &service_name, fallback_port).await;
            Some((
                stream::iter(addrs),
                (lookup, service_name, fallback_port, ticker),
            ))
        },
    )
    .flatten()
}

impl Discovery for DnsSrv {
    fn candidates(&self) -> BoxStream<'static, SocketAddr> {
        let Some(resolver) = self.resolver.clone() else {
            return stream::empty().boxed();
        };
        candidate_ticks(
            resolver,
            self.service_name.clone(),
            self.fallback_port,
            self.interval,
        )
        .boxed()
    }

    fn announce(&self, _gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn construction_carries_the_configured_service_name_and_port() {
        let discovery = DnsSrv::new("_sundog._tcp.svc.cluster.local.", 7946);
        assert_eq!(discovery.service_name, "_sundog._tcp.svc.cluster.local.");
        assert_eq!(discovery.fallback_port, 7946);
        assert_eq!(discovery.interval, DEFAULT_INTERVAL);
    }

    #[test]
    fn with_interval_overrides_the_default() {
        let discovery = DnsSrv::new("seed.local.", 4000).with_interval(Duration::from_secs(5));
        assert_eq!(discovery.interval, Duration::from_secs(5));
    }

    #[test]
    fn resolver_builds_against_the_system_configuration() {
        // On any sane Unix CI box `/etc/resolv.conf` exists, so resolver
        // construction itself must succeed even without a real query here.
        let discovery = DnsSrv::new("_sundog._tcp.svc.cluster.local.", 7946);
        assert!(discovery.resolver.is_some());
    }

    #[tokio::test]
    async fn announce_is_a_no_op_that_resolves_ok() {
        let discovery = DnsSrv::new("_sundog._tcp.svc.cluster.local.", 7946);
        let gossip_addr: SocketAddr = "127.0.0.1:1".parse().expect("valid addr");
        assert!(discovery.announce(gossip_addr).await.is_ok());
    }

    /// A fake [`SrvLookup`]: `srv` is `None` to simulate an SRV lookup
    /// error, `ips` maps a host to `None` to simulate that host's A/AAAA
    /// lookup failing. A host absent from `ips` is also treated as a
    /// failure, so a test only needs to list the hosts it cares about.
    #[derive(Clone, Default)]
    struct FakeLookup {
        srv: Option<Vec<(String, u16)>>,
        ips: HashMap<String, Option<Vec<IpAddr>>>,
    }

    impl SrvLookup for FakeLookup {
        // No `.await` inside: these are in-memory fakes, not real I/O. Kept
        // `async fn` anyway to match the `SrvLookup` trait's signature.
        #[allow(clippy::unused_async_trait_impl)]
        async fn srv(&self, _service: &str) -> io::Result<Vec<(String, u16)>> {
            self.srv
                .clone()
                .ok_or_else(|| io::Error::other("fake SRV lookup failed"))
        }

        #[allow(clippy::unused_async_trait_impl)]
        async fn ips(&self, host: &str) -> io::Result<Vec<IpAddr>> {
            match self.ips.get(host) {
                Some(Some(ips)) => Ok(ips.clone()),
                Some(None) | None => Err(io::Error::other(format!(
                    "fake IP lookup failed for {host}"
                ))),
            }
        }
    }

    #[tokio::test]
    async fn resolve_once_returns_every_ip_from_every_srv_target() {
        let ip1: IpAddr = "10.0.0.1".parse().expect("valid ip");
        let ip2: IpAddr = "10.0.0.2".parse().expect("valid ip");
        let ip3: IpAddr = "10.0.0.3".parse().expect("valid ip");
        let lookup = FakeLookup {
            srv: Some(vec![
                ("a.svc.local.".to_string(), 7000),
                ("b.svc.local.".to_string(), 7001),
            ]),
            ips: HashMap::from([
                ("a.svc.local.".to_string(), Some(vec![ip1, ip2])),
                ("b.svc.local.".to_string(), Some(vec![ip3])),
            ]),
        };
        let addrs = resolve_once(&lookup, "svc.local.", 9999).await;
        assert_eq!(
            addrs,
            vec![
                SocketAddr::new(ip1, 7000),
                SocketAddr::new(ip2, 7000),
                SocketAddr::new(ip3, 7001),
            ]
        );
    }

    #[tokio::test]
    async fn resolve_once_falls_back_to_the_fallback_port_when_srv_port_is_zero() {
        let ip: IpAddr = "10.0.0.9".parse().expect("valid ip");
        let lookup = FakeLookup {
            srv: Some(vec![("z.svc.local.".to_string(), 0)]),
            ips: HashMap::from([("z.svc.local.".to_string(), Some(vec![ip]))]),
        };
        let addrs = resolve_once(&lookup, "svc.local.", 4242).await;
        assert_eq!(addrs, vec![SocketAddr::new(ip, 4242)]);
    }

    #[tokio::test]
    async fn resolve_once_yields_no_candidates_when_srv_and_fallback_both_fail() {
        let lookup = FakeLookup {
            srv: None,
            ips: HashMap::new(), // fallback A/AAAA lookup of the service name also fails
        };
        let addrs = resolve_once(&lookup, "svc.local.", 4242).await;
        assert!(
            addrs.is_empty(),
            "an SRV lookup failure must degrade to no candidates, not a panic"
        );
    }

    #[tokio::test]
    async fn resolve_once_skips_only_the_target_whose_ip_lookup_fails() {
        let good_ip: IpAddr = "10.0.0.5".parse().expect("valid ip");
        let lookup = FakeLookup {
            srv: Some(vec![
                ("good.svc.local.".to_string(), 7000),
                ("bad.svc.local.".to_string(), 7001),
            ]),
            ips: HashMap::from([
                ("good.svc.local.".to_string(), Some(vec![good_ip])),
                ("bad.svc.local.".to_string(), None),
            ]),
        };
        let addrs = resolve_once(&lookup, "svc.local.", 4242).await;
        assert_eq!(
            addrs,
            vec![SocketAddr::new(good_ip, 7000)],
            "the target whose IP lookup failed must be skipped, not abort the whole round"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn candidates_stream_through_the_fake_across_multiple_ticks() {
        let ip1: IpAddr = "10.0.0.1".parse().expect("valid ip");
        let ip2: IpAddr = "10.0.0.2".parse().expect("valid ip");
        let lookup = FakeLookup {
            srv: Some(vec![("t.svc.local.".to_string(), 7000)]),
            ips: HashMap::from([("t.svc.local.".to_string(), Some(vec![ip1, ip2]))]),
        };
        let interval = Duration::from_secs(10);
        let mut stream = Box::pin(candidate_ticks(
            lookup,
            "svc.local.".to_string(),
            9999,
            interval,
        ));

        let mut first_tick = Vec::new();
        first_tick.push(stream.next().await.expect("first tick yields an addr"));
        first_tick.push(stream.next().await.expect("first tick yields an addr"));
        assert_eq!(
            first_tick,
            vec![SocketAddr::new(ip1, 7000), SocketAddr::new(ip2, 7000)]
        );

        let too_soon = tokio::time::timeout(Duration::from_millis(1), stream.next()).await;
        assert!(
            too_soon.is_err(),
            "must not re-poll the fake before the interval elapses"
        );

        tokio::time::advance(interval).await;

        let mut second_tick = Vec::new();
        second_tick.push(stream.next().await.expect("second tick yields an addr"));
        second_tick.push(stream.next().await.expect("second tick yields an addr"));
        assert_eq!(
            second_tick,
            vec![SocketAddr::new(ip1, 7000), SocketAddr::new(ip2, 7000)],
            "the stream keeps polling the fake on every tick, not just the first"
        );
    }
}
