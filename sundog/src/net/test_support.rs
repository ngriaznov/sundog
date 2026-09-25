//! Shared real-transport test helpers for other modules' tests that need a
//! live [`Mesh`] pair, notably `cluster::rebalance`'s bucket-pull tests:
//! `net::mod`'s own `mod tests` builds a richer `FixtureHandler` for its own
//! use, but that module is private to `net`, so a cross-module test gets its
//! own minimal fixture here instead of duplicating `net`'s.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use tokio::sync::mpsc;

use super::{InboundMsg, Mesh, RequestHandler};
use crate::config::ClusterConfig;
use crate::membership::Peer;
use crate::node::{NodeId, NodeName};
use crate::store::{
    BucketDigest, BucketEntries, BucketLen, BucketPart, BucketPartDigests, KeyVersion, PartEntries,
};
use crate::wire::{self, WireRecord};

/// Binds a `Mesh` on loopback with `handler` answering requests: the donor
/// or requester side of a real two-mesh test, mirroring `net::mod`'s own
/// private `spawn_mesh` test helper.
pub(crate) async fn spawn_mesh(
    node: NodeId,
    handler: Arc<dyn RequestHandler>,
) -> (Mesh, mpsc::Receiver<InboundMsg>) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    Mesh::spawn(addr, node, 1, &ClusterConfig::default(), handler)
        .await
        .expect("bind loopback")
}

/// A `Peer` entry for `node` at `addr`, speaking the current build's
/// protocol: for `Mesh::update_peers` in a real two-mesh test.
pub(crate) fn peer_at(node: NodeId, addr: SocketAddr) -> Peer {
    Peer {
        node,
        name: NodeName::new("test", node),
        gossip_addr: addr,
        data_addr: addr,
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
    }
}

/// A [`RequestHandler`] that serves an `StBuckets` pull deterministically:
/// `st_buckets_available` answers `view_hash == self.view_hash`, and
/// `st_bucket_chunks` replays `chunks` (already bucket-tagged) verbatim,
/// optionally stalling forever afterward. Every other lookup answers empty,
/// never exercised by a bucket-pull test. Used by `cluster::rebalance`'s
/// tests, which need to control exactly which bucket a chunk belongs to and
/// whether a bucket's stream ever completes, without reimplementing the
/// rest of `RequestHandler`'s surface at that call site.
pub(crate) struct BucketPullHandler {
    pub(crate) view_hash: u64,
    pub(crate) chunks: Vec<(u16, Vec<WireRecord>)>,
    /// When `true`, the stream hangs forever once `chunks` is exhausted
    /// instead of ending: a donor stalled mid-group, for a partial-group
    /// test.
    pub(crate) stall_after: bool,
    /// Every id list `st_bucket_chunks` was asked for, in call order.
    pub(crate) requested: std::sync::Mutex<Vec<Vec<u16>>>,
}

impl RequestHandler for BucketPullHandler {
    fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        Box::pin(futures::stream::empty())
    }

    fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<BucketDigest>> {
        Box::pin(async { Vec::new() })
    }

    fn bucket_entries(&self, _cache: SmolStr, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        Box::pin(async { Vec::new() })
    }

    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, BucketEntries> {
        Box::pin(async { Vec::new() })
    }

    fn records_for(&self, _cache: SmolStr, _keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        Box::pin(async { Vec::new() })
    }

    fn bucket_lens(&self, _cache: SmolStr, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        Box::pin(async { Vec::new() })
    }

    fn part_digests(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        Box::pin(async { Vec::new() })
    }

    fn entries_for_parts(
        &self,
        _cache: SmolStr,
        _parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, PartEntries> {
        Box::pin(async { Vec::new() })
    }

    fn st_buckets_available(&self, _cache: SmolStr, view_hash: u64) -> BoxFuture<'_, bool> {
        let available = view_hash == self.view_hash;
        Box::pin(async move { available })
    }

    fn st_bucket_chunks(
        &self,
        _cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxStream<'static, (u16, Vec<WireRecord>)> {
        self.requested
            .lock()
            .expect("invariant: fixture mutex is never poisoned")
            .push(buckets);
        let chunks = futures::stream::iter(self.chunks.clone());
        if self.stall_after {
            Box::pin(chunks.chain(futures::stream::pending()))
        } else {
            Box::pin(chunks)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bucket_pull_handler_replays_its_chunks_then_stalls_when_asked() {
        let rec = WireRecord {
            key: Bytes::from_static(b"k"),
            value: Some(Bytes::from_static(b"v")),
            ver: crate::hlc::Hlc {
                wall_ms: 1,
                logical: 0,
                node: NodeId::from(1),
            },
            expires_at_ms: None,
        };
        let handler = BucketPullHandler {
            view_hash: 7,
            chunks: vec![(0, vec![rec.clone()])],
            stall_after: true,
            requested: Default::default(),
        };
        assert!(handler.st_buckets_available(SmolStr::new("c"), 7).await);
        assert!(!handler.st_buckets_available(SmolStr::new("c"), 8).await);

        let mut stream = handler.st_bucket_chunks(SmolStr::new("c"), vec![0]);
        assert_eq!(stream.next().await, Some((0u16, vec![rec])));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "stall_after keeps the stream pending once chunks is exhausted"
        );
    }

    #[tokio::test]
    async fn spawn_mesh_and_peer_at_produce_a_dialable_pair() {
        let handler: Arc<dyn RequestHandler> = Arc::new(BucketPullHandler {
            view_hash: 0,
            chunks: Vec::new(),
            stall_after: false,
            requested: Default::default(),
        });
        let (mesh_a, _inbound_a) = spawn_mesh(NodeId::from(1), Arc::clone(&handler)).await;
        let (mesh_b, _inbound_b) = spawn_mesh(NodeId::from(2), handler).await;
        mesh_b.update_peers(vec![peer_at(NodeId::from(1), mesh_a.local_addr())]);
        mesh_a.shutdown().await;
        mesh_b.shutdown().await;
    }
}
