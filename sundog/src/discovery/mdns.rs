//! `Mdns`: the zeroconf default. Registers `_sundog._udp.local.` (cluster
//! name as a TXT property, instance = node id) and browses continuously.
//! Does not cross the default Docker bridge; compose demos use `Static`.

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
/// and browses continuously, filtering out any other cluster name.
///
/// The multicast sockets open lazily on the daemon thread, so an
/// environment without multicast surfaces no error anywhere here; the
/// candidate stream never yields anything instead. This type never panics.
pub struct Mdns {
    cluster_name: SmolStr,
    instance_name: String,
    daemon: Option<ServiceDaemon>,
}

impl Mdns {
    /// `instance_name` becomes the mDNS instance label. `cluster_name` is
    /// both the browse-result TXT filter and the announced value.
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
/// services from any other cluster. Prefers IPv4, falling back to whatever
/// the peer advertised.
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
    let ip = select_ip(resolved.addresses.iter().map(mdns_sd::ScopedIp::to_ip_addr))?;
    Some(SocketAddr::new(ip, resolved.port))
}

/// Picks the best candidate IP out of a resolved service's advertised
/// addresses: the first IPv4 address, or, absent one, whatever address was
/// advertised first. Pure and independent of `mdns_sd`'s own types, so it's
/// unit-testable with hand-built addresses.
fn select_ip(addresses: impl IntoIterator<Item = IpAddr>) -> Option<IpAddr> {
    let mut first = None;
    for ip in addresses {
        if ip.is_ipv4() {
            return Some(ip);
        }
        first.get_or_insert(ip);
    }
    first
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
    fn select_ip_prefers_ipv4_over_an_earlier_ipv6_address() {
        let v6: IpAddr = "fe80::1".parse().expect("valid addr");
        let v4: IpAddr = "10.0.0.1".parse().expect("valid addr");
        assert_eq!(select_ip([v6, v4]), Some(v4));
    }

    #[test]
    fn select_ip_falls_back_to_the_first_address_absent_any_ipv4() {
        let first: IpAddr = "fe80::1".parse().expect("valid addr");
        let second: IpAddr = "fe80::2".parse().expect("valid addr");
        assert_eq!(select_ip([first, second]), Some(first));
    }

    #[test]
    fn select_ip_on_no_addresses_is_none() {
        assert_eq!(select_ip(std::iter::empty()), None);
    }

    #[test]
    fn constructing_never_panics_even_without_multicast() {
        // `new` must not panic regardless of the sandbox's multicast support.
        let _discovery = Mdns::new("test-cluster", "test-node");
    }

    #[tokio::test]
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
