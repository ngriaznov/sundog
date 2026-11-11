//! A replicated counter that never loses an increment: a `PnCounter` value
//! under its merge resolver, written with `Cache::merge`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use sundog::crdt::{PnCounter, PnCounterResolver};
use sundog::{Cache, CacheError, Cluster, Mode};

// ANCHOR: counter
/// Page views, counted on every node and merged to the exact total.
#[derive(Clone)]
pub struct PageViews {
    cache: Cache<String, PnCounter>,
    /// This process's running total for each page. A write carries the
    /// writer's cumulative total for its own page, so every page keeps a
    /// total of its own.
    local_totals: Arc<Mutex<HashMap<String, u64>>>,
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
            local_totals: Arc::default(),
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
        let total = {
            let mut totals = self
                .local_totals
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let total = totals.entry(page.to_string()).or_insert(0);
            *total += 1;
            *total
        };
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

    #[tokio::test]
    async fn each_page_counts_its_own_views() {
        let cluster = solo_cluster("cookbook-counters-pages").await;
        let views = PageViews::open(&cluster).await.expect("cache opens");
        for _ in 0..5 {
            views.record("/home").await.expect("record");
        }
        views.record("/about").await.expect("record");
        assert_eq!(views.views("/home").await, 5);
        assert_eq!(views.views("/about").await, 1);
        cluster.shutdown().await;
    }
}
