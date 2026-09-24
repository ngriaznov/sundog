//! sundog as a benchmark target: in-process nodes on loopback. Every worker
//! reads and writes through node 0's cache handle, the way one service
//! instance uses its own embedded cache.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use sundog::{Cache, Cluster, ClusterConfig, Mode};

/// The three ways a sundog cache runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SundogMode {
    /// One node, no cluster traffic.
    Local,
    /// Three nodes, every node holding every entry.
    Replicated,
    /// Three nodes, each entry on two owners; reads go through `fetch`.
    Distributed,
}

impl SundogMode {
    fn nodes(self) -> usize {
        match self {
            Self::Local => 1,
            Self::Replicated | Self::Distributed => 3,
        }
    }

    /// How many copies of each entry the cluster holds.
    #[must_use]
    pub fn copies(self) -> u64 {
        match self {
            Self::Local => 1,
            Self::Replicated => 3,
            Self::Distributed => 2,
        }
    }

    fn cache_mode(self) -> Mode {
        match self {
            Self::Local => Mode::Local,
            Self::Replicated => Mode::Replicated,
            Self::Distributed => Mode::distributed(),
        }
    }
}

pub struct SundogTarget {
    mode: SundogMode,
    clusters: Vec<Cluster>,
    caches: Vec<Cache<String, Vec<u8>>>,
}

#[derive(Clone)]
pub struct SundogClient {
    mode: SundogMode,
    cache: Cache<String, Vec<u8>>,
}

impl SundogTarget {
    /// Starts the nodes, waits until they see each other, and opens the
    /// benchmark cache on each.
    ///
    /// # Errors
    ///
    /// Returns an error if a node cannot bind, the nodes do not find each
    /// other in 30 seconds, or a cache fails to open.
    pub async fn start(mode: SundogMode) -> anyhow::Result<Self> {
        let nodes = mode.nodes();
        let mut gossip_addrs = Vec::with_capacity(nodes);
        for _ in 0..nodes {
            gossip_addrs.push(reserve_udp_port().await?);
        }
        let mut clusters = Vec::with_capacity(nodes);
        for (i, &gossip) in gossip_addrs.iter().enumerate() {
            let seeds: Vec<SocketAddr> = gossip_addrs
                .iter()
                .enumerate()
                .filter(|&(j, _)| j != i)
                .map(|(_, &addr)| addr)
                .collect();
            let cluster = Cluster::builder("sundog-bench")
                .config(ClusterConfig::default().with(|c| {
                    c.gossip_bind_addr = gossip;
                    c.data_bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
                    c.state_transfer_budget = Duration::from_secs(2);
                }))
                .seeds(seeds)
                .build()
                .await
                .with_context(|| format!("node {i} builds"))?;
            clusters.push(cluster);
        }
        wait_until(
            Duration::from_secs(30),
            "every node sees every peer",
            || clusters.iter().all(|c| c.peers().len() + 1 >= nodes),
        )
        .await?;
        let mut caches = Vec::with_capacity(nodes);
        for cluster in &clusters {
            caches.push(
                cluster
                    .cache::<String, Vec<u8>>("bench")
                    .mode(mode.cache_mode())
                    .open()
                    .await
                    .context("the bench cache opens")?,
            );
        }
        let target = Self {
            mode,
            clusters,
            caches,
        };
        if mode == SundogMode::Distributed {
            target.await_ownership_agreement().await?;
        }
        Ok(target)
    }

    /// Waits until every node computes the same owners for a sample of keys
    /// and every node owns some of them. A node that opened the cache before
    /// its peers advertised it briefly owns every bucket alone, and a write
    /// it accepts then reaches the real owners only when it hands the
    /// bucket over; loading before the views agree would measure that
    /// hand-off instead of steady state.
    async fn await_ownership_agreement(&self) -> anyhow::Result<()> {
        let sample: Vec<String> = (0..256).map(crate::workload::key).collect();
        let ids: Vec<_> = self.clusters.iter().map(Cluster::node_id).collect();
        wait_until(
            Duration::from_secs(30),
            "every node agrees on ownership",
            || {
                let views: Vec<Vec<_>> = self
                    .caches
                    .iter()
                    .map(|cache| sample.iter().map(|key| cache.owners_of(key)).collect())
                    .collect();
                let agree = views.windows(2).all(|pair| pair[0] == pair[1]);
                let owners_each = views[0].iter().all(|owners| owners.len() == 2);
                let everyone_owns = ids
                    .iter()
                    .all(|id| views[0].iter().any(|owners| owners.contains(id)));
                agree && owners_each && everyone_owns
            },
        )
        .await
    }

    #[must_use]
    pub fn client(&self) -> SundogClient {
        SundogClient {
            mode: self.mode,
            cache: self.caches[0].clone(),
        }
    }

    #[must_use]
    pub fn copies(&self) -> u64 {
        self.mode.copies()
    }

    /// Waits until a load of `keys` entries has landed: every copy on a
    /// `Replicated` cluster, and every key readable through `fetch` on a
    /// `Distributed` one, so memory is read once replication is done.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster does not settle in two minutes.
    pub async fn settle(&self, keys: usize) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(120);
        if self.mode == SundogMode::Distributed {
            let client = self.client();
            let mut pending: Vec<usize> = (0..keys).collect();
            while !pending.is_empty() {
                let mut still = Vec::new();
                for i in pending {
                    if !client.get(&crate::workload::key(i)).await? {
                        still.push(i);
                    }
                }
                pending = still;
                if !pending.is_empty() && Instant::now() > deadline {
                    bail!(
                        "{} of {keys} keys are unreadable after two minutes",
                        pending.len()
                    );
                }
                if !pending.is_empty() {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            return Ok(());
        }
        let want = keys as u64 * self.copies();
        loop {
            let mut held = 0;
            for cache in &self.caches {
                held += cache.entry_count().await;
            }
            if held >= want {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("the cluster holds {held} of {want} entries after two minutes");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn shutdown(self) {
        drop(self.caches);
        for cluster in self.clusters {
            cluster.shutdown().await;
        }
    }
}

impl SundogClient {
    /// Reads `key`: `get` on this node for `Local` and `Replicated`, `fetch`
    /// from an owner for `Distributed`. Returns whether it was a hit.
    ///
    /// # Errors
    ///
    /// Returns an error if a `Distributed` fetch finds no owner.
    pub async fn get(&self, key: &String) -> anyhow::Result<bool> {
        match self.mode {
            SundogMode::Local | SundogMode::Replicated => Ok(self.cache.get(key).await.is_some()),
            SundogMode::Distributed => Ok(self.cache.fetch(key).await?.is_some()),
        }
    }

    /// Writes `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the value exceeds the frame cap.
    pub async fn set(&self, key: String, value: Vec<u8>) -> anyhow::Result<()> {
        self.cache.insert(key, value).await?;
        Ok(())
    }
}

async fn reserve_udp_port() -> anyhow::Result<SocketAddr> {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .context("reserve a loopback gossip port")?;
    Ok(socket.local_addr()?)
}

async fn wait_until(
    timeout: Duration,
    what: &str,
    mut done: impl FnMut() -> bool,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    while !done() {
        if Instant::now() > deadline {
            bail!("timed out waiting until {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// Bytes this process holds from jemalloc and has not freed, from jemalloc's own
/// counter: the figure Redis reports as `used_memory`, so the two compare
/// like for like. `None` where jemalloc is not the allocator.
#[must_use]
pub fn allocated_bytes() -> Option<u64> {
    #[cfg(not(target_env = "msvc"))]
    {
        tikv_jemalloc_ctl::epoch::advance().ok()?;
        tikv_jemalloc_ctl::stats::allocated::read()
            .ok()
            .and_then(|bytes| u64::try_from(bytes).ok())
    }
    #[cfg(target_env = "msvc")]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn allocated_bytes_grow_with_a_live_allocation() {
        let before = allocated_bytes().expect("jemalloc reports allocated bytes");
        let held = vec![7u8; 8 * 1024 * 1024];
        let after = allocated_bytes().expect("jemalloc reports allocated bytes");
        // The counter is process-wide, and tests running at the same time
        // take and free memory too, so allow a margin below the full 8 MiB.
        assert!(after >= before + 7 * 1024 * 1024, "{before} -> {after}");
        drop(held);
    }

    #[test]
    fn each_mode_reports_its_node_and_copy_counts() {
        assert_eq!(
            (SundogMode::Local.nodes(), SundogMode::Local.copies()),
            (1, 1)
        );
        assert_eq!(
            (
                SundogMode::Replicated.nodes(),
                SundogMode::Replicated.copies()
            ),
            (3, 3)
        );
        assert_eq!(
            (
                SundogMode::Distributed.nodes(),
                SundogMode::Distributed.copies()
            ),
            (3, 2)
        );
    }
}
