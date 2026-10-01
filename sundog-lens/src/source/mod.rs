//! Where the model's input comes from: the gossip observer, the ownership
//! worker and the metrics scraper, all feeding one [`Update`] stream.

use std::sync::Arc;
use std::time::Instant;

use sundog::observe::ClusterSnapshot;

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
    /// One scrape of one node's exporter finished.
    Scrape(ScrapeReport),
}
