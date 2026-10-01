//! Where the model's input comes from: the gossip observer, the ownership
//! worker and the metrics scraper, all feeding one [`Update`] stream.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use smol_str::SmolStr;
use sundog::observe::{ClusterSnapshot, Observer};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::cli::{Seed, WatchArgs};
use crate::model::ownership::OwnershipDigest;

pub mod expo;
pub mod http;
pub mod names;
pub mod observer;
pub mod ownership;
pub mod scrape;
pub mod targets;

pub use scrape::ScrapeReport;

/// One input to the [`Model`](crate::model::Model).
#[derive(Debug, Clone)]
pub enum Update {
    /// The observer published a snapshot, seen at the given instant.
    Snapshot(Arc<ClusterSnapshot>, Instant),
    /// The ownership worker computed a cache's ownership. The digest carries
    /// the owner slots moved since the previous digest of that cache.
    Ownership(OwnershipDigest),
    /// No cache of this name has ownership any more: the ownership worker
    /// holds none for it, so the model drops what it holds.
    OwnershipGone(SmolStr),
    /// One scrape of one node's exporter finished.
    Scrape(ScrapeReport),
}

/// How many updates wait for the consumer before the producers wait.
const UPDATE_BUFFER: usize = 256;

/// What a [`Feed`] watches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedConfig {
    /// The cluster name.
    pub cluster: String,
    /// The gossip seeds; empty means `SUNDOG_SEEDS`, then mDNS.
    pub seeds: Vec<Seed>,
    /// The observer's gossip bind address.
    pub bind: SocketAddr,
    /// The address the observer advertises, when not the bound one.
    pub advertise: Option<IpAddr>,
}

impl FeedConfig {
    /// A configuration for `cluster` with no seeds, binding every interface on
    /// a free port.
    #[must_use]
    pub fn new(cluster: impl Into<String>) -> Self {
        Self {
            cluster: cluster.into(),
            seeds: Vec::new(),
            bind: SocketAddr::from(([0, 0, 0, 0], 0)),
            advertise: None,
        }
    }
}

impl From<&WatchArgs> for FeedConfig {
    fn from(args: &WatchArgs) -> Self {
        Self {
            cluster: args.cluster.clone(),
            seeds: args.seeds.clone(),
            bind: args.bind,
            advertise: args.advertise,
        }
    }
}

/// The running sources: the gossip observer and the ownership worker, both
/// feeding one stream of [`Update`]s.
#[derive(Debug)]
pub struct Feed {
    observer: Observer,
    updates: mpsc::Receiver<Update>,
    tasks: Vec<JoinHandle<()>>,
}

impl Feed {
    /// Joins the cluster's gossip and starts the sources.
    ///
    /// # Errors
    ///
    /// Returns an error when a seed does not resolve or the observer cannot
    /// start; see [`observer::start`].
    pub async fn spawn(config: FeedConfig) -> anyhow::Result<Self> {
        let observer = observer::start(
            &config.cluster,
            &config.seeds,
            config.bind,
            config.advertise,
        )
        .await?;
        let (tx, updates) = mpsc::channel(UPDATE_BUFFER);
        // The worker reads the relay, which carries a snapshot only after its
        // update is queued, so the model sees a snapshot before any ownership
        // computed from it.
        let (relay, relayed) = watch::channel(Arc::new(ClusterSnapshot::new(
            config.cluster.as_str(),
            Vec::new(),
            0,
        )));
        let tasks = vec![
            tokio::spawn(observer::forward(
                observer.subscribe(),
                tx.clone(),
                Some(relay),
            )),
            tokio::spawn(ownership::run(relayed, tx)),
        ];
        Ok(Self {
            observer,
            updates,
            tasks,
        })
    }

    /// The next update; `None` once every source has stopped and the stream is
    /// drained.
    pub async fn recv(&mut self) -> Option<Update> {
        self.updates.recv().await
    }

    /// The next update if one is waiting.
    pub fn try_recv(&mut self) -> Option<Update> {
        self.updates.try_recv().ok()
    }

    /// The gossip address the observer advertises.
    #[must_use]
    pub fn observer_addr(&self) -> SocketAddr {
        self.observer.local_gossip_addr()
    }

    /// The observer.
    #[must_use]
    pub const fn observer(&self) -> &Observer {
        &self.observer
    }

    /// Leaves gossip and stops the sources.
    pub async fn shutdown(self) {
        let Self {
            observer, tasks, ..
        } = self;
        observer.shutdown().await;
        for task in tasks {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_new_config_binds_every_interface_on_a_free_port_with_no_seeds() {
        let config = FeedConfig::new("c");
        assert_eq!(config.cluster, "c");
        assert!(config.seeds.is_empty());
        assert_eq!(config.bind, SocketAddr::from(([0, 0, 0, 0], 0)));
        assert_eq!(config.advertise, None);
    }

    #[test]
    fn a_config_takes_its_fields_from_the_watch_arguments() {
        let command = crate::cli::parse([
            "watch",
            "prod",
            "--seed",
            "10.0.0.1:7946",
            "--bind",
            "127.0.0.1:7000",
            "--advertise",
            "10.0.0.9",
        ])
        .unwrap();
        let crate::cli::Command::Watch(args) = command else {
            panic!("a watch command");
        };
        let config = FeedConfig::from(&args);
        assert_eq!(config.cluster, "prod");
        assert_eq!(config.seeds, [Seed::Addr("10.0.0.1:7946".parse().unwrap())]);
        assert_eq!(config.bind, "127.0.0.1:7000".parse().unwrap());
        assert_eq!(config.advertise, Some("10.0.0.9".parse().unwrap()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_feed_starts_streams_the_first_snapshot_and_shuts_down() {
        let mut config = FeedConfig::new("lens-feed-unit");
        config.bind = "127.0.0.1:0".parse().unwrap();
        config.seeds = vec![Seed::Addr("127.0.0.1:9".parse().unwrap())];
        let mut feed = Feed::spawn(config).await.expect("the feed starts");
        assert!(feed.observer_addr().ip().is_loopback());
        assert_eq!(feed.observer().local_gossip_addr(), feed.observer_addr());
        let first = tokio::time::timeout(Duration::from_secs(10), feed.recv())
            .await
            .expect("an update arrives")
            .expect("the stream is open");
        assert!(matches!(first, Update::Snapshot(s, _) if s.members.is_empty()));
        assert!(feed.try_recv().is_none());
        tokio::time::timeout(Duration::from_secs(10), feed.shutdown())
            .await
            .expect("the feed shuts down");
    }
}
