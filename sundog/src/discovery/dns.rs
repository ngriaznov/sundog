//! `DnsSrv`: resolves a headless-service name on an interval via
//! `hickory-resolver`. The Kubernetes answer, equivalent to `JGroups`'
//! `DNS_PING`.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use hickory_resolver::proto::rr::RData;
use hickory_resolver::{ResolverBuilder, TokioResolver};

use super::Discovery;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);

/// SRV-record discovery against a headless service name, e.g.
/// `_sundog._tcp.my-cluster.svc.cluster.local.` on Kubernetes.
///
/// Falls back to a plain A/AAAA lookup of `service_name` (paired with
/// `fallback_port`) when it has no SRV records, so a bare hostname works
/// too. Polls on an interval rather than once, so a rolling DNS update
/// (pod restarts, a scaled headless service) is picked up without a
/// restart.
pub struct DnsSrv {
    resolver: Option<TokioResolver>,
    service_name: String,
    fallback_port: u16,
    interval: Duration,
}

impl DnsSrv {
    /// Builds a resolver against the system DNS configuration
    /// (`/etc/resolv.conf` on Unix). If the resolver fails to initialize,
    /// this degrades to a permanently empty candidate stream rather than
    /// failing the whole discovery source.
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

    /// Overrides the default 30s poll interval.
    #[must_use]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

/// One resolution round: SRV first, falling back to A/AAAA on
/// `service_name` itself when no SRV records exist. Failures are logged and
/// yield an empty (not error) result — a transient DNS hiccup must not end
/// the candidate stream.
async fn resolve_once(
    resolver: &TokioResolver,
    service_name: &str,
    fallback_port: u16,
) -> Vec<SocketAddr> {
    match resolver.srv_lookup(service_name).await {
        Ok(lookup) => {
            let mut targets = Vec::new();
            for record in lookup.answers() {
                let RData::SRV(srv) = &record.data else {
                    continue;
                };
                match resolver.lookup_ip(srv.target.to_string()).await {
                    Ok(ips) => targets.extend(ips.iter().map(|ip| SocketAddr::new(ip, srv.port))),
                    Err(err) => {
                        tracing::warn!(target = %srv.target, %err, "SRV target failed to resolve");
                    }
                }
            }
            targets
        }
        Err(err) => {
            tracing::debug!(%err, service_name, "SRV lookup failed, falling back to A/AAAA");
            match resolver.lookup_ip(service_name).await {
                Ok(ips) => ips
                    .iter()
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

impl Discovery for DnsSrv {
    fn candidates(&self) -> BoxStream<'static, SocketAddr> {
        let Some(resolver) = self.resolver.clone() else {
            return stream::empty().boxed();
        };
        let service_name = self.service_name.clone();
        let fallback_port = self.fallback_port;
        let ticker = tokio::time::interval(self.interval);
        stream::unfold(
            (resolver, service_name, fallback_port, ticker),
            |(resolver, service_name, fallback_port, mut ticker)| async move {
                ticker.tick().await;
                let addrs = resolve_once(&resolver, &service_name, fallback_port).await;
                Some((
                    stream::iter(addrs),
                    (resolver, service_name, fallback_port, ticker),
                ))
            },
        )
        .flatten()
        .boxed()
    }

    fn announce(&self, _gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
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
        // construction itself must succeed even though we never issue a
        // real query here.
        let discovery = DnsSrv::new("_sundog._tcp.svc.cluster.local.", 7946);
        assert!(discovery.resolver.is_some());
    }
}
