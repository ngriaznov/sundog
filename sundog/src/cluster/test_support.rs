//! Shared helpers for `cluster`'s and `cache`'s real-transport test modules,
//! neither of which runs under the `sim` feature.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;

use super::{Cluster, ShardRegistryExt};
use crate::config::ClusterConfig;
use crate::discovery::Discovery;
use crate::store::ShardOps;

/// Loopback-only config: skips the outbound-interface probe and keeps
/// anti-entropy/tombstone timing tight for fast, deterministic tests.
pub(crate) fn loopback_config() -> ClusterConfig {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    ClusterConfig {
        gossip_bind_addr: loopback,
        data_bind_addr: loopback,
        ae_interval: Duration::from_millis(200),
        tombstone_ttl: Duration::from_secs(2),
        // A one-second first-peer grace, so a lone node opens fast.
        state_transfer_budget: Duration::from_secs(5),
        ..ClusterConfig::default()
    }
}

pub(crate) async fn wait_for_peer_count(cluster: &Cluster, expected: usize) {
    let mut peers = cluster.inner.membership.peers();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if peers.borrow().len() >= expected {
                return;
            }
            if peers.changed().await.is_err() {
                return;
            }
        }
    })
    .await
    .expect("peers converge within the bound");
}

/// Waits until `cluster`'s live peer set is empty, for a departure or
/// crash scenario the failure detector needs a little time to notice.
pub(crate) async fn wait_for_no_peers(cluster: &Cluster) {
    let mut peers = cluster.inner.membership.peers();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if peers.borrow().is_empty() {
                return;
            }
            if peers.changed().await.is_err() {
                return;
            }
        }
    })
    .await
    .expect("the peer disappears from the live set within the bound");
}

/// `cluster`'s registered shard for `cache`, as
/// [`super::anti_entropy::run_round_against`] takes it: the same handle
/// `Cache::open` installs in the registry.
pub(crate) fn registered_shard(cluster: &Cluster, cache: &SmolStr) -> Arc<dyn ShardOps> {
    cluster
        .shards()
        .read_shards()
        .get(cache)
        .cloned()
        .expect("the cache was opened, so its shard is registered")
}

/// Polls `cond` every 20ms until it returns `true`, or panics with `msg` once
/// `budget` elapses.
pub(crate) async fn wait_until(budget: Duration, msg: &str, mut cond: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(budget, async {
        loop {
            if cond().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect(msg);
}

/// `subscriber` as this thread's default dispatcher while the guard lives,
/// beside a second registered dispatcher that enables nothing.
///
/// tracing-core caches each callsite's interest process-wide. While at most
/// one dispatcher is registered, it takes that interest from the default
/// dispatcher of whichever thread reaches the callsite first, so a callsite
/// another test reaches first on a thread with no subscriber caches as
/// never wanted, and this thread's subscriber never sees it. With two or
/// more registered, it asks every live one, and each thread's own default
/// decides what that thread records.
pub(crate) fn scoped_subscriber(
    subscriber: impl tracing::Subscriber + Send + Sync + 'static,
) -> ScopedSubscriber {
    let other = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    ScopedSubscriber {
        _default: tracing::subscriber::set_default(subscriber),
        _other: other,
    }
}

/// [`scoped_subscriber`]'s guard: restores the thread's previous default
/// dispatcher on drop.
pub(crate) struct ScopedSubscriber {
    _default: tracing::subscriber::DefaultGuard,
    _other: tracing::Dispatch,
}

/// A discovery whose candidate stream yields its addresses, waits `lag` and
/// then ends. The [`Discovery`] contract forbids that, and a custom source
/// can still do it.
pub(crate) struct EndingDiscovery {
    pub(crate) addrs: Vec<SocketAddr>,
    pub(crate) lag: Duration,
}

impl Discovery for EndingDiscovery {
    fn candidates(&self) -> BoxStream<'static, SocketAddr> {
        let lag = self.lag;
        futures::stream::iter(self.addrs.clone())
            .chain(
                futures::stream::once(async move { tokio::time::sleep(lag).await })
                    .filter_map(|()| async { None }),
            )
            .boxed()
    }

    fn announce(&self, _gossip_addr: SocketAddr) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    /// Counts every event it sees.
    struct CountEvents(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountEvents {
        fn on_event(
            &self,
            _event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One callsite, reached from whichever thread calls it.
    fn probe() {
        tracing::info!("scoped subscriber probe");
    }

    #[test]
    fn a_scoped_subscriber_sees_a_callsite_another_thread_reached_first() {
        let seen = Arc::new(AtomicUsize::new(0));
        let _guard =
            scoped_subscriber(tracing_subscriber::registry().with(CountEvents(Arc::clone(&seen))));
        std::thread::spawn(probe)
            .join()
            .expect("the probe thread finishes");
        assert_eq!(
            seen.load(Ordering::Relaxed),
            0,
            "the other thread has no subscriber"
        );
        probe();
        assert_eq!(seen.load(Ordering::Relaxed), 1);
    }
}
