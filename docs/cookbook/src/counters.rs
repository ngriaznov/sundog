//! A replicated counter that never loses an increment: a `PnCounter` value
//! under its merge resolver, written with `Cache::merge`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cache, CacheError, Cluster, Mode};

// ANCHOR: counter
/// Page views, counted on every node and merged to the exact total.
#[derive(Clone)]
pub struct PageViews {
    cache: Cache<String, PnCounter>,
    /// This process's own running total for each page would live in a map;
    /// one page keeps the example short.
    local_total: Arc<AtomicU64>,
}

impl PageViews {
    /// Opens the counter cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache cannot open.
    pub async fn open(cluster: &Cluster) -> Result<Self, CacheError> {
        let cache = cluster
            .cache::<String, PnCounter>("page-views")
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .open()
            .await?;
        Ok(Self {
            cache,
            local_total: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Counts one view. No read happens first: the write carries this
    /// writer's cumulative total, and the resolver merges it with every
    /// other writer's.
    ///
    /// # Errors
    ///
    /// Returns an error if the value fails to encode.
    pub async fn record(&self, page: &str) -> Result<(), CacheError> {
        let total = self.local_total.fetch_add(1, Ordering::SeqCst) + 1;
        let delta = PnCounter::local_delta(self.cache.writer_id(), total);
        self.cache.merge(page.to_string(), delta).await
    }

    /// The merged total across every writer this node has heard from.
    pub async fn views(&self, page: &str) -> i128 {
        self.cache
            .get(&page.to_string())
            .await
            .map_or(0, |counter| counter.value())
    }
}
// ANCHOR_END: counter

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::solo_cluster;

    #[tokio::test]
    async fn every_recorded_view_counts() {
        let cluster = solo_cluster("cookbook-counters").await;
        let views = PageViews::open(&cluster).await.expect("cache opens");
        for _ in 0..5 {
            views.record("/home").await.expect("record");
        }
        assert_eq!(views.views("/home").await, 5);
        assert_eq!(views.views("/about").await, 0);
        cluster.shutdown().await;
    }
}
