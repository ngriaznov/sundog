//! The gossip observer's feed: `sundog::observe::Observer` snapshots as
//! [`Update::Snapshot`].

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use sundog::ClusterConfig;
use sundog::observe::{ClusterSnapshot, Observer};
use tokio::sync::{mpsc, watch};

use super::Update;
use crate::cli::Seed;

/// The gossip settings of the observer: where it binds and which address it
/// advertises. Every other setting keeps the cluster default, so the observer
/// marks a member down when members running the defaults do.
#[must_use]
pub fn gossip_config(bind: SocketAddr, advertise: Option<IpAddr>) -> ClusterConfig {
    ClusterConfig::default().with(|config| {
        config.gossip_bind_addr = bind;
        config.advertise_ip = advertise;
    })
}

/// The socket addresses `seeds` name: a literal address as itself, a host
/// name as every address it resolves to.
///
/// # Errors
///
/// Returns an error naming the seed when a host name does not resolve.
pub async fn resolve_seeds(seeds: &[Seed]) -> anyhow::Result<Vec<SocketAddr>> {
    let mut resolved = Vec::new();
    for seed in seeds {
        match seed {
            Seed::Addr(addr) => resolved.push(*addr),
            Seed::Host(host, port) => {
                let addrs = tokio::net::lookup_host((host.as_str(), *port))
                    .await
                    .with_context(|| format!("resolving seed {seed}"))?;
                resolved.extend(addrs);
            }
        }
    }
    Ok(resolved)
}

/// Joins the cluster's gossip as an observer. With no seeds it finds the
/// cluster the way a node does: the `SUNDOG_SEEDS` seeds, then mDNS.
///
/// # Errors
///
/// Returns an error when a seed does not resolve or the observer cannot bind
/// its gossip socket or start gossip.
pub async fn start(
    cluster: &str,
    seeds: &[Seed],
    bind: SocketAddr,
    advertise: Option<IpAddr>,
) -> anyhow::Result<Observer> {
    let mut builder = Observer::builder(cluster).config(gossip_config(bind, advertise));
    if !seeds.is_empty() {
        builder = builder.seeds(resolve_seeds(seeds).await?);
    }
    builder
        .build()
        .await
        .with_context(|| format!("joining cluster {cluster:?} as an observer"))
}

/// Sends the current snapshot, then every later one, as
/// [`Update::Snapshot`]. Returns when the observer stops or `updates` closes.
///
/// With a `relay`, each snapshot is also published on it once its update is
/// queued, so a consumer of the relay never acts on a snapshot before the
/// model has been sent it.
pub async fn forward(
    mut snapshots: watch::Receiver<Arc<ClusterSnapshot>>,
    updates: mpsc::Sender<Update>,
    relay: Option<watch::Sender<Arc<ClusterSnapshot>>>,
) {
    loop {
        let snapshot = Arc::clone(&snapshots.borrow_and_update());
        if updates
            .send(Update::Snapshot(Arc::clone(&snapshot), Instant::now()))
            .await
            .is_err()
        {
            return;
        }
        if let Some(relay) = &relay {
            relay.send_replace(snapshot);
        }
        if snapshots.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::model::testkit;

    #[test]
    fn the_gossip_config_binds_and_advertises_as_asked() {
        let bind: SocketAddr = "127.0.0.1:7946".parse().unwrap();
        let advertise: IpAddr = "10.1.2.3".parse().unwrap();
        let config = gossip_config(bind, Some(advertise));
        assert_eq!(config.gossip_bind_addr, bind);
        assert_eq!(config.advertise_ip, Some(advertise));
        assert_eq!(gossip_config(bind, None).advertise_ip, None);
        assert_eq!(
            config.gossip_interval,
            ClusterConfig::default().gossip_interval
        );
    }

    #[tokio::test]
    async fn literal_seeds_resolve_to_themselves_in_order() {
        let first: SocketAddr = "127.0.0.11:7946".parse().unwrap();
        let second: SocketAddr = "[::1]:7947".parse().unwrap();
        let resolved = resolve_seeds(&[Seed::Addr(first), Seed::Addr(second)])
            .await
            .unwrap();
        assert_eq!(resolved, [first, second]);
        assert!(resolve_seeds(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_host_seed_resolves_through_the_resolver() {
        let resolved = resolve_seeds(&[Seed::Host("localhost".into(), 7946)])
            .await
            .unwrap();
        assert!(!resolved.is_empty());
        assert!(resolved.iter().all(|addr| addr.port() == 7946));
        assert!(resolved.iter().all(|addr| addr.ip().is_loopback()));
    }

    #[tokio::test]
    async fn an_unresolvable_seed_names_itself_in_the_error() {
        let error = resolve_seeds(&[Seed::Host("no-such-host.invalid".into(), 1)])
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("no-such-host.invalid:1"));
    }

    #[tokio::test]
    async fn forward_sends_the_current_snapshot_then_each_change() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(1)));
        let (updates_tx, mut updates) = mpsc::channel(8);
        let task = tokio::spawn(forward(rx, updates_tx, None));

        let first = updates.recv().await.unwrap();
        assert!(matches!(&first, Update::Snapshot(s, _) if s.members.len() == 1));
        tx.send(Arc::new(testkit::snapshot(2))).unwrap();
        let second = updates.recv().await.unwrap();
        assert!(matches!(&second, Update::Snapshot(s, _) if s.members.len() == 2));

        drop(tx);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("forward ends when the observer stops")
            .unwrap();
        assert!(updates.recv().await.is_none());
    }

    #[tokio::test]
    async fn forward_relays_a_snapshot_only_after_its_update_is_queued() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(1)));
        let (relay_tx, mut relay) = watch::channel(Arc::new(testkit::snapshot(0)));
        let (updates_tx, mut updates) = mpsc::channel(8);
        let task = tokio::spawn(forward(rx, updates_tx, Some(relay_tx)));

        for members in [1usize, 2, 3] {
            if members > 1 {
                tx.send(Arc::new(testkit::snapshot(u8::try_from(members).unwrap())))
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), relay.changed())
                .await
                .expect("the relay publishes")
                .unwrap();
            assert_eq!(relay.borrow_and_update().members.len(), members);
            let queued = updates.try_recv().expect("the update was queued first");
            assert!(matches!(&queued, Update::Snapshot(s, _) if s.members.len() == members));
        }
        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn forward_ends_when_the_receiver_is_gone() {
        let (tx, rx) = watch::channel(Arc::new(testkit::snapshot(1)));
        let (updates_tx, updates) = mpsc::channel(1);
        drop(updates);
        tokio::time::timeout(Duration::from_secs(5), forward(rx, updates_tx, None))
            .await
            .expect("forward ends when nobody listens");
        drop(tx);
    }

    #[tokio::test]
    async fn start_joins_an_empty_cluster_on_loopback() {
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let nobody = Seed::Addr("127.0.0.1:9".parse().unwrap());
        let observer = start("lens-observer-unit", &[nobody], bind, None)
            .await
            .expect("the observer starts");
        assert!(observer.local_gossip_addr().ip().is_loopback());
        assert!(observer.snapshot().members.is_empty());
        observer.shutdown().await;
    }
}
