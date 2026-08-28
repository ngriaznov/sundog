//! `Static`: a fixed seed list, from the builder or `SUNDOG_SEEDS=host:port,…`.
//! The escape hatch and the test-suite workhorse. Plan §5.

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};

use super::Discovery;

const SUNDOG_SEEDS_ENV: &str = "SUNDOG_SEEDS";
const DEFAULT_REDISCOVER_INTERVAL: Duration = Duration::from_secs(30);

/// A fixed seed list: explicit addresses from the builder, merged with the
/// `SUNDOG_SEEDS=host:port,host:port` environment variable if it is set.
///
/// Entries may be hostnames as well as literal addresses — each is
/// re-resolved through the OS resolver on every rediscovery tick, so DNS
/// changes are picked up. The candidate stream never ends: it re-yields the
/// whole (de-duplicated) seed set on a slow interval, which is what lets a
/// fully restarted cluster re-find itself (plan §5).
pub struct Static {
    specs: Arc<[String]>,
    rediscover_interval: Duration,
}

impl Static {
    /// Builds a seed list from explicit addresses, merged with
    /// `SUNDOG_SEEDS` if it is set. Duplicate entries (by their `host:port`
    /// text form) are collapsed to one.
    #[must_use]
    pub fn new(seeds: impl IntoIterator<Item = SocketAddr>) -> Self {
        let explicit = seeds.into_iter().map(|addr| addr.to_string());
        Self::from_specs(explicit.chain(env_seed_specs()))
    }

    /// Builds a seed list from `SUNDOG_SEEDS` alone, with no explicit
    /// addresses. Equivalent to `Static::new(std::iter::empty())`.
    #[must_use]
    pub fn from_env() -> Self {
        Self::new(std::iter::empty())
    }

    /// Overrides the default 30s rediscovery interval.
    #[must_use]
    pub fn with_rediscover_interval(mut self, interval: Duration) -> Self {
        self.rediscover_interval = interval;
        self
    }

    fn from_specs(specs: impl IntoIterator<Item = String>) -> Self {
        let mut seen = HashSet::new();
        let deduped: Vec<String> = specs
            .into_iter()
            .filter(|spec| seen.insert(spec.clone()))
            .collect();
        Self {
            specs: Arc::from(deduped),
            rediscover_interval: DEFAULT_REDISCOVER_INTERVAL,
        }
    }
}

fn env_seed_specs() -> Vec<String> {
    std::env::var(SUNDOG_SEEDS_ENV)
        .ok()
        .as_deref()
        .map(parse_seed_list)
        .unwrap_or_default()
}

fn parse_seed_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|spec| !spec.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Resolves every spec via the OS resolver, skipping (and logging) any that
/// fail rather than failing the whole round — one bad seed must never stop
/// the rest from being discovered.
async fn resolve_specs(specs: &[String]) -> Vec<SocketAddr> {
    let mut resolved = Vec::new();
    for spec in specs {
        match tokio::net::lookup_host(spec.as_str()).await {
            Ok(addrs) => resolved.extend(addrs),
            Err(err) => tracing::warn!(spec, %err, "static seed failed to resolve"),
        }
    }
    resolved
}

impl Discovery for Static {
    fn candidates(&self) -> BoxStream<'static, SocketAddr> {
        let specs = Arc::clone(&self.specs);
        let ticker = tokio::time::interval(self.rediscover_interval);
        stream::unfold((specs, ticker), |(specs, mut ticker)| async move {
            ticker.tick().await;
            let resolved = resolve_specs(&specs).await;
            Some((stream::iter(resolved), (specs, ticker)))
        })
        .flatten()
        .boxed()
    }

    fn announce(&self, _gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn parse_seed_list_trims_whitespace_and_skips_blanks() {
        let specs = parse_seed_list(" host1:4000 ,host2:4001,, ,host3:4002");
        assert_eq!(specs, vec!["host1:4000", "host2:4001", "host3:4002"]);
    }

    #[test]
    fn explicit_and_derived_duplicate_specs_collapse_to_one() {
        let a: SocketAddr = "127.0.0.1:4000".parse().expect("valid addr");
        let discovery =
            Static::from_specs([a.to_string(), a.to_string(), "127.0.0.1:4001".to_string()]);
        assert_eq!(discovery.specs.len(), 2);
    }

    #[tokio::test]
    async fn resolves_hostname_specs_via_the_os_resolver() {
        let resolved = resolve_specs(&[String::from("localhost:4321")]).await;
        assert!(
            resolved
                .iter()
                .any(|addr| addr.port() == 4321 && addr.ip().is_loopback()),
            "expected localhost:4321 to resolve to a loopback address, got {resolved:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stream_deduplicates_and_repeats_forever_without_ending() {
        let a: SocketAddr = "127.0.0.1:5000".parse().expect("valid addr");
        let b: SocketAddr = "127.0.0.1:5001".parse().expect("valid addr");
        let discovery = Static::new([a, a, b]).with_rediscover_interval(Duration::from_secs(30));
        let mut candidates = discovery.candidates();

        let mut first_batch = HashSet::new();
        first_batch.insert(candidates.next().await.expect("first batch item"));
        first_batch.insert(candidates.next().await.expect("first batch item"));
        assert_eq!(first_batch, HashSet::from([a, b]));

        let too_soon = tokio::time::timeout(Duration::from_millis(1), candidates.next()).await;
        assert!(
            too_soon.is_err(),
            "must not re-yield before the rediscovery interval elapses"
        );

        tokio::time::advance(Duration::from_secs(30)).await;

        let mut second_batch = HashSet::new();
        second_batch.insert(candidates.next().await.expect("second batch item"));
        second_batch.insert(candidates.next().await.expect("second batch item"));
        assert_eq!(second_batch, HashSet::from([a, b]));
    }
}
