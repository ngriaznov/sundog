//! `Mdns`: the zeroconf default. Registers `_sundog._udp.local.` (cluster
//! name as a TXT property, instance = node id) and browses continuously via
//! `mdns-sd`. Plan §5. Does not cross the default Docker bridge — compose
//! demos use `Static` instead (plan §13).

use std::io;
use std::net::{IpAddr, SocketAddr};

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use smol_str::SmolStr;

use super::Discovery;

const SERVICE_TYPE: &str = "_sundog._udp.local.";
const CLUSTER_TXT_KEY: &str = "cluster";

/// Zeroconf discovery over `_sundog._udp.local.` via `mdns-sd`. Registers
/// this node's instance (TXT `cluster=<name>`) on [`Discovery::announce`]
/// and browses continuously for others, filtering out any advertising a
/// different cluster name.
///
/// `ServiceDaemon::new` does not itself open the multicast sockets — per its
/// own docs those open lazily on the daemon thread, so an environment
/// without multicast (many CI containers) surfaces no error here, from
/// `browse`, or from `register`; the candidate stream simply never yields
/// anything. A hard daemon-startup failure (rare) is logged and degrades the
/// same way, so this type never panics regardless of multicast availability.
pub struct Mdns {
    cluster_name: SmolStr,
    instance_name: String,
    daemon: Option<ServiceDaemon>,
}

impl Mdns {
    /// `instance_name` becomes the mDNS instance label (plan §5: the node
    /// name); `cluster_name` is both the TXT filter applied to browse
    /// results and the value this node advertises on announce.
    #[must_use]
    pub fn new(cluster_name: impl Into<SmolStr>, instance_name: impl Into<String>) -> Self {
        let daemon = ServiceDaemon::new()
            .inspect_err(
                |err| tracing::warn!(%err, "mDNS daemon failed to start; Mdns discovery disabled"),
            )
            .ok();
        Self {
            cluster_name: cluster_name.into(),
            instance_name: instance_name.into(),
            daemon,
        }
    }
}

impl Discovery for Mdns {
    fn candidates(&self) -> BoxStream<'static, SocketAddr> {
        let Some(daemon) = self.daemon.clone() else {
            return stream::empty().boxed();
        };
        let cluster_name = self.cluster_name.clone();
        let events = match daemon.browse(SERVICE_TYPE) {
            Ok(events) => events,
            Err(err) => {
                tracing::warn!(%err, "mDNS browse failed to start");
                return stream::empty().boxed();
            }
        };
        events
            .into_stream()
            .filter_map(move |event| {
                let addr = resolved_addr(&event, &cluster_name);
                async move { addr }
            })
            .boxed()
    }

    fn announce(&self, gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            let Some(daemon) = &self.daemon else {
                return Ok(());
            };
            let service_info =
                build_service_info(&self.cluster_name, &self.instance_name, gossip_addr)
                    .map_err(io::Error::other)?;
            daemon.register(service_info).map_err(io::Error::other)
        })
    }
}

fn build_service_info(
    cluster_name: &str,
    instance_name: &str,
    gossip_addr: SocketAddr,
) -> mdns_sd::Result<ServiceInfo> {
    let host_name = format!("{instance_name}.local.");
    let properties = [(CLUSTER_TXT_KEY, cluster_name)];
    ServiceInfo::new(
        SERVICE_TYPE,
        instance_name,
        &host_name,
        (),
        gossip_addr.port(),
        &properties[..],
    )
    .map(ServiceInfo::enable_addr_auto)
}

/// Extracts a candidate address from a resolved event, filtering out
/// services from any other cluster. Prefers an IPv4 address (plan §5:
/// "handle IPv4 primary"), falling back to whatever the peer advertised.
fn resolved_addr(event: &ServiceEvent, cluster_name: &str) -> Option<SocketAddr> {
    let ServiceEvent::ServiceResolved(resolved) = event else {
        return None;
    };
    if resolved
        .txt_properties
        .get_property_val_str(CLUSTER_TXT_KEY)
        != Some(cluster_name)
    {
        return None;
    }
    let ip = resolved
        .addresses
        .iter()
        .map(mdns_sd::ScopedIp::to_ip_addr)
        .find(IpAddr::is_ipv4)
        .or_else(|| {
            resolved
                .addresses
                .iter()
                .next()
                .map(mdns_sd::ScopedIp::to_ip_addr)
        })?;
    Some(SocketAddr::new(ip, resolved.port))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn build_service_info_embeds_cluster_txt_and_gossip_port() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().expect("valid addr");
        let info = build_service_info("demo", "node1-aaaa", addr).expect("service info builds");
        assert_eq!(info.get_port(), addr.port());
        assert_eq!(info.get_property_val_str(CLUSTER_TXT_KEY), Some("demo"));
    }

    #[test]
    fn constructing_never_panics_even_without_multicast() {
        // Exercises the graceful-degradation path this module documents:
        // whatever the sandbox's multicast support, `new` must not panic.
        let _discovery = Mdns::new("test-cluster", "test-node");
    }

    #[tokio::test]
    #[ignore = "requires a working multicast loopback, absent in most CI containers"]
    async fn register_and_browse_round_trip() {
        let announcer = Mdns::new("smoke-test-cluster", "announcer-node");
        let addr: SocketAddr = "127.0.0.1:19999".parse().expect("valid addr");
        announcer.announce(addr).await.expect("announce succeeds");

        let browser = Mdns::new("smoke-test-cluster", "browser-node");
        let mut candidates = browser.candidates();
        let found = tokio::time::timeout(Duration::from_secs(5), candidates.next()).await;
        assert!(
            found.is_ok(),
            "expected to discover the announced peer within 5s"
        );
    }
}
