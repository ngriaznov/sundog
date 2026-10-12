//! Unit tests for the data plane: the shared fixtures and the topic modules.

use std::sync::Mutex;
use std::time::Duration;

use futures::StreamExt as _;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::LengthDelimitedCodec;

use futures::SinkExt as _;

use super::*;
use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::membership::Peer;
use crate::node::NodeName;
use crate::store::PartMask;
use crate::store::{BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion};
use crate::wire::WireRecord;
use crate::wire::{self, MAX_FRAME};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::io;
use xxhash_rust::xxh3::xxh3_64;

mod anti_entropy;
mod distribution;
mod fan_out;
mod peers;
mod state_transfer;

/// `(key, version)` tuples to [`KeyVersion`]s, for a fixture written the
/// terser tuple way.
fn kv(entries: Vec<(Bytes, Hlc)>) -> Vec<KeyVersion> {
    entries
        .into_iter()
        .map(|(key, version)| KeyVersion { key, version })
        .collect()
}

/// [`FixtureHandler::fetch_fixture`]'s controllable outcome, one
/// variant per [`FetchServe`] shape.
enum FetchFixture {
    Found(Option<WireRecord>),
    Stale(u64),
    Unavailable,
}

/// [`FixtureHandler::load_fixture`]'s controllable outcome, one variant
/// per [`LoadServe`] shape.
enum LoadFixture {
    Loaded(Vec<WireRecord>, Vec<(Bytes, Bytes)>),
    Failed(&'static str),
    Unavailable,
}

/// [`FixtureHandler::ae_scoped_fixture`]'s controllable outcome, one
/// variant per [`AeServeOutcome`] shape.
enum AeScopedFixture {
    Digests(Vec<BucketDigest>),
    Stale(u64),
    Unavailable,
}

struct FixtureHandler {
    records: Vec<WireRecord>,
    digests: Vec<(u16, u64)>,
    bucket_entries: Vec<(Bytes, Hlc)>,
    pulled: Mutex<Vec<(SmolStr, Vec<Bytes>)>>,
    bucket_lens: Vec<(u16, usize)>,
    part_digests: Vec<(u16, Vec<u64>)>,
    part_entries: crate::store::PartEntries,
    ae_part_min_bucket: usize,
    /// Declines every state-transfer request, the way a node still
    /// warming does.
    snapshot_unavailable: bool,
    ownership_view_hash: Option<u64>,
    fetch_fixture: FetchFixture,
    load_fixture: LoadFixture,
    ae_scoped_fixture: AeScopedFixture,
    buckets_available: bool,
    buckets_cold: bool,
    bucket_chunks: Vec<Vec<WireRecord>>,
    /// Per-`bucket_chunks` entry bucket tag; empty tags every chunk `0`.
    bucket_chunk_tags: Vec<u16>,
}

impl Default for FixtureHandler {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            digests: Vec::new(),
            bucket_entries: Vec::new(),
            pulled: Mutex::new(Vec::new()),
            bucket_lens: Vec::new(),
            part_digests: Vec::new(),
            part_entries: Vec::new(),
            ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
            snapshot_unavailable: false,
            ownership_view_hash: None,
            fetch_fixture: FetchFixture::Unavailable,
            load_fixture: LoadFixture::Unavailable,
            ae_scoped_fixture: AeScopedFixture::Unavailable,
            buckets_available: false,
            buckets_cold: false,
            bucket_chunks: Vec::new(),
            bucket_chunk_tags: Vec::new(),
        }
    }
}

impl RequestHandler for FixtureHandler {
    fn snapshot_available(&self, _cache: SmolStr) -> bool {
        !self.snapshot_unavailable
    }

    fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        Box::pin(futures::stream::iter(vec![self.records.clone()]))
    }

    fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<BucketDigest>> {
        let digests = self
            .digests
            .iter()
            .map(|&(bucket, digest)| BucketDigest { bucket, digest })
            .collect();
        Box::pin(async { digests })
    }

    fn bucket_entries(&self, _cache: SmolStr, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        let entries = self
            .bucket_entries
            .iter()
            .cloned()
            .map(|(key, version)| KeyVersion { key, version })
            .collect();
        Box::pin(async { entries })
    }

    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, crate::store::BucketEntries> {
        let bucket_entries: Vec<KeyVersion> = self
            .bucket_entries
            .iter()
            .cloned()
            .map(|(key, version)| KeyVersion { key, version })
            .collect();
        Box::pin(async move {
            buckets
                .into_iter()
                .map(|bucket| (bucket, bucket_entries.clone()))
                .collect()
        })
    }

    fn records_for(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        self.pulled
            .lock()
            .expect("invariant: fixture mutex is never poisoned")
            .push((cache, keys));
        Box::pin(async { self.records.clone() })
    }

    fn bucket_lens(&self, _cache: SmolStr, buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        Box::pin(async move {
            let fixture = &self.bucket_lens;
            buckets
                .into_iter()
                .map(|bucket| {
                    let len = fixture
                        .iter()
                        .find(|(b, _)| *b == bucket)
                        .map_or(0, |(_, len)| *len);
                    BucketLen { bucket, len }
                })
                .collect()
        })
    }

    fn part_digests(
        &self,
        _cache: SmolStr,
        buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        Box::pin(async move {
            let fixture = &self.part_digests;
            buckets
                .into_iter()
                .map(|bucket| {
                    let digests = fixture
                        .iter()
                        .find(|(b, _)| *b == bucket)
                        .map_or_else(Vec::new, |(_, d)| d.clone());
                    BucketPartDigests { bucket, digests }
                })
                .collect()
        })
    }

    fn entries_for_parts(
        &self,
        _cache: SmolStr,
        parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        Box::pin(async move {
            let fixture = &self.part_entries;
            parts
                .into_iter()
                .map(|key| {
                    let entries = fixture
                        .iter()
                        .find(|(k, _)| *k == key)
                        .map_or_else(Vec::new, |(_, e)| e.clone());
                    (key, entries)
                })
                .collect()
        })
    }

    fn ae_part_min_bucket(&self) -> usize {
        self.ae_part_min_bucket
    }

    fn ownership_view_hash(&self, _cache: SmolStr) -> Option<u64> {
        self.ownership_view_hash
    }

    fn fetch(&self, _cache: SmolStr, _key: Bytes, _view_hash: u64) -> BoxFuture<'_, FetchServe> {
        Box::pin(async move {
            match &self.fetch_fixture {
                FetchFixture::Found(rec) => FetchServe::Found(rec.clone()),
                FetchFixture::Stale(responder_view_hash) => FetchServe::Stale {
                    responder_view_hash: *responder_view_hash,
                },
                FetchFixture::Unavailable => FetchServe::Unavailable,
            }
        })
    }

    fn load(&self, _cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, LoadServe> {
        Box::pin(async move {
            match &self.load_fixture {
                LoadFixture::Loaded(found, uncached) => LoadServe::Loaded {
                    found: found
                        .iter()
                        .filter(|rec| keys.contains(&rec.key))
                        .cloned()
                        .collect(),
                    uncached: uncached
                        .iter()
                        .filter(|(key, _)| keys.contains(key))
                        .cloned()
                        .collect(),
                },
                LoadFixture::Failed(message) => LoadServe::Failed((*message).to_string()),
                LoadFixture::Unavailable => LoadServe::Unavailable,
            }
        })
    }

    fn ae_digest_scoped(
        &self,
        _cache: SmolStr,
        _view_hash: u64,
        _buckets: Vec<BucketDigest>,
    ) -> BoxFuture<'_, AeServeOutcome<Vec<BucketDigest>>> {
        Box::pin(async move {
            match &self.ae_scoped_fixture {
                AeScopedFixture::Digests(digests) => AeServeOutcome::Digests(digests.clone()),
                AeScopedFixture::Stale(responder_view_hash) => AeServeOutcome::Stale {
                    responder_view_hash: *responder_view_hash,
                },
                AeScopedFixture::Unavailable => AeServeOutcome::Unavailable,
            }
        })
    }

    /// Answers from `part_digests`, folded over each requested mask.
    fn ae_digest_masked(
        &self,
        _cache: SmolStr,
        _view_hash: u64,
        buckets: Vec<(u16, u64, u64)>,
    ) -> BoxFuture<'_, AeServeOutcome<Vec<BucketPartDigests>>> {
        Box::pin(async move {
            match &self.ae_scoped_fixture {
                AeScopedFixture::Digests(_) => AeServeOutcome::Digests(
                    self.part_digests
                        .iter()
                        .filter(|(bucket, digests)| {
                            buckets.iter().any(|&(b, mask, digest)| {
                                b == *bucket && PartMask::from_bits(mask).fold(digests) != digest
                            })
                        })
                        .map(|(bucket, digests)| BucketPartDigests {
                            bucket: *bucket,
                            digests: digests.clone(),
                        })
                        .collect(),
                ),
                AeScopedFixture::Stale(responder_view_hash) => AeServeOutcome::Stale {
                    responder_view_hash: *responder_view_hash,
                },
                AeScopedFixture::Unavailable => AeServeOutcome::Unavailable,
            }
        })
    }

    fn st_buckets_available(&self, _cache: SmolStr, _view_hash: u64) -> BoxFuture<'_, bool> {
        Box::pin(async { self.buckets_available })
    }

    fn st_buckets_cold(&self, _cache: SmolStr, _buckets: Vec<u16>) -> BoxFuture<'_, bool> {
        Box::pin(async { self.buckets_cold })
    }

    fn st_bucket_chunks(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> BoxStream<'static, (u16, Vec<WireRecord>)> {
        // Empty tags default every chunk to bucket 0.
        let tags = if self.bucket_chunk_tags.is_empty() {
            vec![0u16; self.bucket_chunks.len()]
        } else {
            self.bucket_chunk_tags.clone()
        };
        Box::pin(
            futures::stream::iter(self.bucket_chunks.clone().into_iter().zip(tags))
                .map(|(recs, bucket)| (bucket, recs)),
        )
    }
}

fn sample_record(n: u8) -> WireRecord {
    WireRecord {
        key: Bytes::from(vec![n]),
        value: Some(Bytes::from(vec![n, n])),
        ver: Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: NodeId::from(u64::from(n)),
        },
        expires_at_ms: None,
    }
}

async fn spawn_mesh(
    node: NodeId,
    handler: Arc<dyn RequestHandler>,
) -> (Mesh, mpsc::Receiver<InboundMsg>) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    Mesh::spawn(addr, node, 1, &ClusterConfig::default(), handler)
        .await
        .expect("bind loopback")
}

fn empty_handler() -> Arc<dyn RequestHandler> {
    Arc::new(FixtureHandler {
        records: Vec::new(),
        digests: Vec::new(),
        bucket_entries: Vec::new(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    })
}

fn peer_at(node: NodeId, addr: SocketAddr) -> Peer {
    Peer {
        node,
        name: NodeName::new("test", node),
        gossip_addr: addr,
        data_addr: addr,
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
    }
}

/// Builds a `Peer` speaking `protocol` instead of the current build's,
/// for testing distribution-mode's protocol gate.
fn peer_at_protocol(node: NodeId, addr: SocketAddr, protocol: u16) -> Peer {
    Peer {
        protocol,
        ..peer_at(node, addr)
    }
}
