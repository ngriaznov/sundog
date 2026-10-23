//! Tags on pages, added and removed from any node without losing an add: an
//! `OrSet` value under its merge resolver, added to with
//! `Cache::merge_sequenced`.

use std::sync::Arc;

use sundog::crdt::{OrSet, OrSetResolver};
use sundog::{Cache, CacheError, Cluster, Mode};

// ANCHOR: tags
/// The tags on each page, replicated to every node.
#[derive(Clone)]
pub struct PageTags {
    cache: Cache<String, OrSet<String>>,
}

impl PageTags {
    /// Opens the tag cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache cannot open.
    pub async fn open(cluster: &Cluster) -> Result<Self, CacheError> {
        let cache = cluster
            .cache::<String, OrSet<String>>("page-tags")
            .mode(Mode::Replicated)
            .resolver(Arc::new(OrSetResolver::<String>::new()))
            .open()
            .await?;
        Ok(Self { cache })
    }

    /// Tags `page` with `tag`. The cache draws this writer's next sequence
    /// number and applies the add under one lock, so concurrent calls on this
    /// node apply in the order they were numbered.
    ///
    /// # Errors
    ///
    /// Returns an error if the value fails to encode.
    pub async fn add(&self, page: &str, tag: &str) -> Result<(), CacheError> {
        self.cache
            .merge_sequenced(page.to_string(), |writer, seq| {
                OrSet::add(writer, seq, tag.to_string())
            })
            .await
    }

    /// Takes `tag` off `page`: the adds of it this node has read. An add of
    /// the same tag that another node made since survives.
    ///
    /// # Errors
    ///
    /// Returns an error if the value fails to encode.
    pub async fn remove(&self, page: &str, tag: &str) -> Result<(), CacheError> {
        let Some(observed) = self.cache.get(&page.to_string()).await else {
            return Ok(());
        };
        self.cache
            .merge(page.to_string(), OrSet::remove(&observed, &tag.to_string()))
            .await
    }

    /// The tags on `page`, in order.
    pub async fn tags(&self, page: &str) -> Vec<String> {
        let mut tags: Vec<String> = self
            .cache
            .get(&page.to_string())
            .await
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        tags.sort();
        tags
    }
}
// ANCHOR_END: tags

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::solo_cluster;

    #[tokio::test]
    async fn a_removed_tag_stays_off_and_an_add_after_it_stays_on() {
        let cluster = solo_cluster("cookbook-tags").await;
        let pages = PageTags::open(&cluster).await.expect("cache opens");
        for tag in ["rust", "cache", "crdt"] {
            pages.add("/home", tag).await.expect("add");
        }
        assert_eq!(pages.tags("/home").await, ["cache", "crdt", "rust"]);

        pages.remove("/home", "cache").await.expect("remove");
        pages
            .add("/home", "docs")
            .await
            .expect("add after the remove");
        pages
            .remove("/about", "rust")
            .await
            .expect("a remove on a page with no tags");

        assert_eq!(pages.tags("/home").await, ["crdt", "docs", "rust"]);
        assert_eq!(pages.tags("/about").await, Vec::<String>::new());
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_adds_on_one_node_all_land() {
        let cluster = solo_cluster("cookbook-tags-concurrent").await;
        let pages = PageTags::open(&cluster).await.expect("cache opens");
        let tasks: Vec<_> = (0..8)
            .map(|task| {
                let pages = pages.clone();
                tokio::spawn(async move {
                    for index in 0..10 {
                        pages
                            .add("/home", &format!("t{task}-{index}"))
                            .await
                            .expect("add");
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.expect("task");
        }

        assert_eq!(pages.tags("/home").await.len(), 80);
        cluster.shutdown().await;
    }
}
