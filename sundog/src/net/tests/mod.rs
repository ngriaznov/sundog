use std::sync::Mutex;
use std::time::Duration;

use futures::StreamExt as _;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::LengthDelimitedCodec;

use futures::SinkExt as _;

use super::*;
use crate::node::NodeName;
use crate::store::PartMask;
use crate::wire::{self, MAX_FRAME};

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

#[tokio::test]
async fn a_bound_listener_holds_its_port_and_spawn_on_accepts_on_it() {
    let bound = BoundListener::bind("127.0.0.1:0".parse().expect("valid loopback addr"))
        .await
        .expect("bind loopback");
    let addr = bound.local_addr();
    assert_ne!(addr.port(), 0, "port 0 resolves to the assigned port");
    let err = std::net::TcpListener::bind(addr)
        .expect_err("the bound listener keeps its port from any other socket");
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse);

    let greeting = Greeting {
        node: NodeId::from(1),
        incarnation: 1,
        cluster: wire::UNNAMED_CLUSTER,
    };
    let (mesh, _inbound) =
        Mesh::spawn_on(bound, greeting, &ClusterConfig::default(), empty_handler())
            .expect("spawn on the bound listener");
    assert_eq!(mesh.local_addr(), addr);
    tokio::net::TcpStream::connect(addr)
        .await
        .expect("the mesh accepts on the port the listener held");
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

#[tokio::test]
async fn hello_is_sent_first_on_a_new_persistent_connection() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let fake_peer_addr = listener.local_addr().expect("listener has a local addr");

    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), fake_peer_addr)]);

    let (stream, _) = listener.accept().await.expect("accept dial-in");
    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(stream);
    let frame = framed
        .next()
        .await
        .expect("frame arrives")
        .expect("no io error");
    let msg = wire::decode(&frame.freeze()).expect("decodes");
    assert_eq!(
        msg,
        Msg::Hello {
            node: NodeId::from(1),
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        }
    );
}

#[tokio::test]
async fn invalidate_overflow_drops_oldest_not_newest() {
    let handler = empty_handler();
    let config = ClusterConfig {
        outbox_capacity: 2,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, handler)
        .await
        .expect("bind loopback");

    // A peer with no listener: the writer spins on connection failures, so
    // its outbox never drains.
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    let key_of = |n: u8| Bytes::from(vec![n]);
    let msg = |n: u8| Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: key_of(n),
        ver: Hlc {
            wall_ms: u64::from(n),
            logical: 0,
            node: NodeId::from(1),
        },
    };
    mesh.send(NodeId::from(2), MsgClass::Invalidate, msg(1));
    mesh.send(NodeId::from(2), MsgClass::Invalidate, msg(2));
    mesh.send(NodeId::from(2), MsgClass::Invalidate, msg(3)); // 1 must be dropped

    let invalidate = {
        let table = mesh.inner.peers.read().expect("lock");
        Arc::clone(
            &table
                .get(&NodeId::from(2))
                .expect("peer registered")
                .invalidate,
        )
    };
    let first = invalidate.pop().await;
    let second = invalidate.pop().await;
    assert_eq!(first.msg, msg(2));
    assert_eq!(second.msg, msg(3));
}

#[tokio::test]
async fn replicate_overflow_drops_newest_and_marks_peer_dirty() {
    let handler = empty_handler();
    let config = ClusterConfig {
        outbox_capacity: 1,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, handler)
        .await
        .expect("bind loopback");

    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    mesh.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    let rec = |n: u8| Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(n),
    };
    mesh.send(NodeId::from(2), MsgClass::Replicate, rec(1));
    assert!(mesh.take_dirty_peers().is_empty(), "no overflow yet");

    mesh.send(NodeId::from(2), MsgClass::Replicate, rec(2)); // dropped: outbox full

    let dirty = mesh.take_dirty_peers();
    assert_eq!(dirty, vec![NodeId::from(2)]);
    assert!(
        mesh.take_dirty_peers().is_empty(),
        "dirty mark is cleared once taken"
    );
}

#[tokio::test]
async fn send_to_an_unknown_peer_is_a_silent_no_op() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.send(
        NodeId::from(99),
        MsgClass::Invalidate,
        Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: Bytes::new(),
            ver: Hlc {
                wall_ms: 0,
                logical: 0,
                node: NodeId::from(1),
            },
        },
    );
}

#[tokio::test]
async fn state_transfer_roundtrip() {
    let records = vec![sample_record(1), sample_record(2)];
    let handler = Arc::new(FixtureHandler {
        records: records.clone(),
        digests: Vec::new(),
        bucket_entries: Vec::new(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let mut stream = requester
        .request_state(NodeId::from(1), SmolStr::new("users"))
        .await
        .expect("request accepted")
        .expect("a warm donor streams");
    let mut got = Vec::new();
    while let Some(chunk) = stream.next().await {
        got.extend(chunk.expect("chunk decodes"));
    }
    assert_eq!(got, records);
}

#[tokio::test]
async fn request_state_reports_a_donor_that_declines_and_keeps_its_connection() {
    let handler = Arc::new(FixtureHandler {
        records: vec![sample_record(1)],
        snapshot_unavailable: true,
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    for attempt in 0..2 {
        let declined = requester
            .request_state(NodeId::from(1), SmolStr::new("users"))
            .await
            .expect("request accepted");
        assert!(
            declined.is_none(),
            "attempt {attempt}: a donor that is not warm declines instead of streaming"
        );
    }
    // The declined connection went back to the pool: an unrelated
    // request on the same peer still works.
    let digests = requester
        .ae_round(NodeId::from(1), SmolStr::new("users"), Vec::new())
        .await
        .expect("the pooled connection serves the next request");
    assert!(digests.is_empty(), "{digests:?}");
}

#[tokio::test]
async fn state_transfer_stream_reports_an_error_on_truncated_connection() {
    // A hand-rolled "donor" that drops the connection mid-stream,
    // without a `done: true` chunk: the requester must see an error.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");

    let donor = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME)
            .new_framed(stream);
        let _hello = framed.next().await.expect("hello arrives");
        let _request = framed.next().await.expect("st request arrives");
        let chunk = Msg::StChunk {
            cache: SmolStr::new("users"),
            recs: vec![sample_record(1)],
            done: false,
        };
        let encoded = wire::encode(&chunk).expect("encodes");
        futures::SinkExt::send(&mut framed, encoded)
            .await
            .expect("send partial chunk");
        drop(framed); // connection closes: no `done: true` chunk ever sent
    });

    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), addr)]);

    let mut stream = requester
        .request_state(NodeId::from(1), SmolStr::new("users"))
        .await
        .expect("request accepted")
        .expect("a warm donor streams");
    let first = stream.next().await.expect("first chunk arrives");
    assert_eq!(first.expect("decodes"), vec![sample_record(1)]);
    let second = stream
        .next()
        .await
        .expect("stream ends with an error, not silently");
    assert!(second.is_err(), "truncated stream must surface as an error");

    donor.await.expect("donor did not panic");
}

#[test]
fn ae_part_reply_names_its_bucket_and_part_in_either_shape() {
    let listing = AePartReply::Listing {
        bucket: 7,
        part: 3,
        entries: Vec::new(),
    };
    let sketch = AePartReply::Sketch {
        bucket: 9,
        part: 63,
        cells: Vec::new(),
    };
    assert_eq!((listing.bucket(), listing.part()), (7, 3));
    assert_eq!((sketch.bucket(), sketch.part()), (9, 63));
}

#[tokio::test]
async fn ae_round_returns_only_mismatched_buckets() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let handler = Arc::new(FixtureHandler {
        records: Vec::new(),
        digests: vec![(0, 111), (1, 222)],
        bucket_entries: entries.clone(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    // bucket 0 matches, bucket 1 mismatches, bucket 2 is requester-only.
    let local_buckets = vec![(0, 111), (1, 999)];
    let result = client
        .ae_round(NodeId::from(1), SmolStr::new("users"), local_buckets)
        .await
        .expect("ae round succeeds");

    assert_eq!(result, vec![AeMismatch::Bucket(1, kv(entries))]);
}

#[tokio::test]
async fn ae_round_returns_part_digests_for_a_bucket_past_the_part_threshold() {
    let part_digests: Vec<u64> = (0..64u64).collect();
    let handler = Arc::new(FixtureHandler {
        digests: vec![(0, 111), (1, 222)],
        bucket_lens: vec![(1, 500)],
        part_digests: vec![(1, part_digests.clone())],
        ae_part_min_bucket: 100,
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    // bucket 0 matches (under threshold anyway); bucket 1 mismatches and
    // its 500-entry fixture length passes ae_part_min_bucket, so the
    // responder answers with its part digests instead of a listing.
    let local_buckets = vec![(0, 111), (1, 999)];
    let result = client
        .ae_round(NodeId::from(1), SmolStr::new("users"), local_buckets)
        .await
        .expect("ae round succeeds");

    assert_eq!(result, vec![AeMismatch::PartDigests(1, part_digests)]);
}

/// Accepts connections off `listener` forever, one task per connection:
/// reads `Hello`, then serves at most one `AeDigest` with an empty
/// `ReqDone`-only reply and closes right after, standing in for the
/// server's `REQ_CONN_IDLE_TIMEOUT` without waiting out the real 60s
/// bound. `Mesh::update_peers` also opens its own persistent writer
/// connection to the same address, which sends `Hello` and then
/// nothing else; that connection's task blocks forever on its
/// second read and is otherwise harmless, since every connection is
/// served independently. `served` fires once per request answered,
/// after its connection is already closed.
fn spawn_serve_one_digest_per_connection(
    listener: TcpListener,
    served: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            let served = Arc::clone(&served);
            tokio::spawn(async move {
                let mut framed = LengthDelimitedCodec::builder()
                    .max_frame_length(MAX_FRAME)
                    .new_framed(stream);
                let Some(Ok(_hello)) = framed.next().await else {
                    return;
                };
                let Some(Ok(_digest)) = framed.next().await else {
                    return;
                };
                let reply = wire::encode(&Msg::ReqDone).expect("encodes");
                framed.send(reply).await.expect("reply sends");
                drop(framed); // closes the connection, as the idle timeout would
                served.notify_one();
            });
        }
    })
}

/// A connection `Mesh::acquire_conn` reuses from the pool after the
/// server already closed it (its idle timeout, forced here to fire
/// after exactly one request rather than after the real
/// `REQ_CONN_IDLE_TIMEOUT`) must not fail the request it carries: the
/// stale reply is caught as an immediate `EOF` and the request retries
/// on a fresh dial instead.
#[tokio::test]
async fn ae_round_retries_a_pooled_connection_the_server_already_closed() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let served = Arc::new(tokio::sync::Notify::new());
    let _server = spawn_serve_one_digest_per_connection(listener, Arc::clone(&served));

    let (client, _client_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(2), addr)]);

    let first = client
        .ae_round(NodeId::from(2), SmolStr::new("users"), Vec::new())
        .await
        .expect("first round succeeds and checks its connection into the pool");
    assert!(first.is_empty(), "{first:?}");

    served.notified().await;

    let second = client
        .ae_round(NodeId::from(2), SmolStr::new("users"), Vec::new())
        .await
        .expect(
            "a pooled connection the server already closed must not fail the request: \
             acquire_conn retries it on a fresh dial",
        );
    assert!(second.is_empty(), "{second:?}");
}

/// A raw request connection to `addr` that has already sent `hello`, for
/// speaking as a peer of any protocol version, including the ones this
/// build's own `Mesh` doesn't send.
async fn raw_request_conn(
    addr: SocketAddr,
    hello: Bytes,
) -> tokio_util::codec::Framed<tokio::net::TcpStream, tokio_util::codec::LengthDelimitedCodec> {
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to the mesh listener");
    let mut framed = tokio_util::codec::LengthDelimitedCodec::builder()
        .max_frame_length(wire::MAX_FRAME)
        .new_framed(stream);
    framed.send(hello).await.expect("hello sends");
    framed
}

/// A protocol-1 hello: an unnamed protocol-2 encoding minus its
/// trailing `protocol` and `cluster` bytes, one each, checked to decode
/// back as protocol 1.
fn legacy_hello(node: NodeId) -> Bytes {
    let full = wire::encode(&Msg::Hello {
        node,
        incarnation: 1,
        protocol: 2,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let legacy = full.slice(..full.len() - 2);
    assert!(matches!(
        wire::decode(&legacy),
        Ok(Msg::Hello { protocol: 1, .. })
    ));
    legacy
}

async fn replies_until_req_done(
    framed: &mut tokio_util::codec::Framed<
        tokio::net::TcpStream,
        tokio_util::codec::LengthDelimitedCodec,
    >,
) -> Vec<Msg> {
    let mut replies = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), framed.next())
            .await
            .expect("a reply arrives")
            .expect("connection stays open")
            .expect("frame reads");
        let msg = wire::decode(&frame.freeze()).expect("reply decodes");
        if matches!(msg, Msg::ReqDone) {
            return replies;
        }
        replies.push(msg);
    }
}

#[tokio::test]
async fn a_protocol_one_peer_never_receives_part_digests() {
    let handler = Arc::new(FixtureHandler {
        digests: vec![(1, 222)],
        bucket_lens: vec![(1, 500)],
        part_digests: vec![(1, (0..64u64).collect())],
        bucket_entries: vec![(Bytes::from_static(b"k1"), sample_record(1).ver)],
        ae_part_min_bucket: 100,
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let digest = wire::encode(&Msg::AeDigest {
        cache: SmolStr::new("users"),
        buckets: vec![(1, 999)],
    })
    .expect("encodes");

    let mut old_peer = raw_request_conn(server.local_addr(), legacy_hello(NodeId::from(2))).await;
    old_peer.send(digest.clone()).await.expect("digest sends");
    let replies = replies_until_req_done(&mut old_peer).await;
    assert!(
        replies
            .iter()
            .all(|msg| matches!(msg, Msg::AeBucket { .. } | Msg::AeSketch { .. })),
        "a protocol-1 peer gets only the shapes it understands: {replies:?}"
    );
    assert!(!replies.is_empty(), "{replies:?}");

    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(3),
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let mut new_peer = raw_request_conn(server.local_addr(), hello).await;
    new_peer.send(digest).await.expect("digest sends");
    let replies = replies_until_req_done(&mut new_peer).await;
    assert!(
        replies
            .iter()
            .any(|msg| matches!(msg, Msg::AePartDigests { .. })),
        "the same bucket answers a current peer with part digests: {replies:?}"
    );
}

#[tokio::test]
async fn a_cold_donor_serves_a_protocol_one_peer_and_declines_a_current_one() {
    let handler = Arc::new(FixtureHandler {
        records: vec![sample_record(1)],
        snapshot_unavailable: true,
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let request = wire::encode(&Msg::StRequest {
        cache: SmolStr::new("users"),
    })
    .expect("encodes");

    let mut old_peer = raw_request_conn(server.local_addr(), legacy_hello(NodeId::from(2))).await;
    old_peer.send(request.clone()).await.expect("request sends");
    let frame = tokio::time::timeout(Duration::from_secs(5), old_peer.next())
        .await
        .expect("a reply arrives")
        .expect("connection stays open")
        .expect("frame reads");
    assert!(
        matches!(wire::decode(&frame.freeze()), Ok(Msg::StChunk { .. })),
        "a peer that cannot be declined is served"
    );

    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(3),
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    let mut new_peer = raw_request_conn(server.local_addr(), hello).await;
    new_peer.send(request).await.expect("request sends");
    let frame = tokio::time::timeout(Duration::from_secs(5), new_peer.next())
        .await
        .expect("a reply arrives")
        .expect("connection stays open")
        .expect("frame reads");
    assert!(matches!(
        wire::decode(&frame.freeze()),
        Ok(Msg::StUnavailable { .. })
    ));
}

#[tokio::test]
async fn ae_parts_returns_listings_and_sketches_per_the_threshold() {
    let small_entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    // Past ClusterConfig::default's ae_sketch_min_bucket (384), so this
    // part answers with a sketch instead of a listing.
    let big_entries: Vec<(Bytes, Hlc)> = (0..400u32)
        .map(|i| {
            (
                Bytes::from(i.to_le_bytes().to_vec()),
                Hlc {
                    wall_ms: u64::from(i) + 1,
                    logical: 0,
                    node: NodeId::from(1),
                },
            )
        })
        .collect();
    let handler = Arc::new(FixtureHandler {
        part_entries: vec![
            (BucketPart { bucket: 1, part: 2 }, kv(small_entries.clone())),
            (BucketPart { bucket: 1, part: 3 }, kv(big_entries)),
        ],
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let got = client
        .ae_parts(NodeId::from(1), SmolStr::new("users"), vec![(1, 2), (1, 3)])
        .await
        .expect("ae_parts succeeds");

    assert_eq!(got.len(), 2, "one reply per requested part");
    assert!(
        got.iter().any(|reply| matches!(
            reply,
            AePartReply::Listing { bucket: 1, part: 2, entries } if *entries == kv(small_entries.clone())
        )),
        "the small part answers with a listing: {got:?}"
    );
    assert!(
        got.iter().any(|reply| matches!(
            reply,
            AePartReply::Sketch {
                bucket: 1,
                part: 3,
                ..
            }
        )),
        "the large part answers with a sketch: {got:?}"
    );
}

#[test]
fn defer_ae_digest_only_with_frames_queued_and_a_bounded_number_of_times() {
    assert!(!defer_ae_digest(0, 0), "an empty outbox is served");
    assert!(defer_ae_digest(1, 0));
    assert!(defer_ae_digest(5, MAX_AE_DEFERRALS - 1));
    assert!(
        !defer_ae_digest(5, MAX_AE_DEFERRALS),
        "the bound serves a round even with frames still queued"
    );
}

#[cfg(feature = "tls")]
fn no_tls() -> super::TlsCtx {
    None
}
#[cfg(not(feature = "tls"))]
fn no_tls() -> super::TlsCtx {
    TlsCtx
}

#[test]
fn record_bucket_ack_is_readable_by_cache_bucket_and_requester() {
    let mesh = MeshInner::for_tests(no_tls(), CancellationToken::new());
    mesh.record_bucket_ack(SmolStr::new("prices"), 7, NodeId::from(3), 42);
    assert!(
        mesh.acked_buckets
            .read()
            .expect("lock is never poisoned")
            .contains_key(&(SmolStr::new("prices"), 7, NodeId::from(3))),
        "the ack lands under its (cache, bucket, requester) key"
    );
    assert!(
        !mesh
            .acked_buckets
            .read()
            .expect("lock is never poisoned")
            .contains_key(&(SmolStr::new("prices"), 8, NodeId::from(3))),
        "a different bucket is not recorded"
    );
}

#[test]
fn acked_owners_only_counts_a_due_bucket_within_the_window() {
    let mesh = MeshInner::for_tests(no_tls(), CancellationToken::new());
    mesh.record_bucket_ack(SmolStr::new("prices"), 7, NodeId::from(3), 42);
    mesh.record_bucket_ack(SmolStr::new("prices"), 9, NodeId::from(4), 42);
    mesh.record_bucket_ack(SmolStr::new("other"), 7, NodeId::from(5), 42);
    let owners = mesh.acked_owners(&SmolStr::new("prices"), &[7], Duration::from_secs(60), 42);
    assert_eq!(
        owners,
        HashMap::from([(7u16, HashSet::from([NodeId::from(3)]))]),
        "only the acked owner of a bucket actually in `due`, for the right cache, counts, \
         keyed by the bucket it acked"
    );
    // Sleeps briefly so the ack is reliably older than a zero window.
    std::thread::sleep(Duration::from_millis(5));
    let none = mesh.acked_owners(&SmolStr::new("prices"), &[7], Duration::ZERO, 42);
    assert!(
        none.is_empty(),
        "an ack older than the window does not count as reconciled"
    );
}

#[test]
fn an_ack_carrying_a_stale_view_hash_does_not_count_as_reconciled() {
    let mesh = MeshInner::for_tests(no_tls(), CancellationToken::new());
    // Acked under view hash 1; this node's current view has moved to 2.
    mesh.record_bucket_ack(SmolStr::new("prices"), 7, NodeId::from(3), 1);
    let stale = mesh.acked_owners(&SmolStr::new("prices"), &[7], Duration::from_secs(60), 2);
    assert!(
        stale.is_empty(),
        "an ack from before a view change does not count as reconciled"
    );
    let current = mesh.acked_owners(&SmolStr::new("prices"), &[7], Duration::from_secs(60), 1);
    assert_eq!(
        current,
        HashMap::from([(7u16, HashSet::from([NodeId::from(3)]))]),
        "the same ack counts once the requested view hash matches the ack's own"
    );
}

#[tokio::test]
async fn peer_protocol_is_zero_for_an_unknown_peer_and_otherwise_the_gossiped_value() {
    let mesh_handle = Mesh {
        local_addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
        inner: MeshInner::for_tests(no_tls(), CancellationToken::new()),
    };
    assert_eq!(mesh_handle.inner.peer_protocol(NodeId::from(9)), 0);
    mesh_handle.update_peers(vec![peer_at_protocol(
        NodeId::from(9),
        "127.0.0.1:1".parse().expect("valid loopback addr"),
        3,
    )]);
    assert_eq!(mesh_handle.inner.peer_protocol(NodeId::from(9)), 3);
}

#[tokio::test]
async fn mesh_acked_owners_delegates_to_the_inner_table() {
    let mesh_handle = Mesh {
        local_addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
        inner: MeshInner::for_tests(no_tls(), CancellationToken::new()),
    };
    mesh_handle
        .inner
        .record_bucket_ack(SmolStr::new("prices"), 7, NodeId::from(3), 42);
    assert_eq!(
        mesh_handle.acked_owners(&SmolStr::new("prices"), &[7], Duration::from_secs(60), 42),
        HashMap::from([(7u16, HashSet::from([NodeId::from(3)]))])
    );
}

#[tokio::test]
async fn ae_digest_from_a_peer_with_replicate_frames_queued_is_answered_empty() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let handler = Arc::new(FixtureHandler {
        records: Vec::new(),
        digests: vec![(0, 111), (1, 222)],
        bucket_entries: entries.clone(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);
    // The server knows the client at an address nobody listens on, so a
    // replicate frame toward it stays queued: a stream in motion, from
    // the server's side, for as long as this test runs.
    let unreachable = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    server.update_peers(vec![peer_at(NodeId::from(2), unreachable)]);
    let frame = OutFrame::new(Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(1),
    })
    .expect("encodes");
    server.send_frames(NodeId::from(2), MsgClass::Replicate, [frame]);

    let round = || async {
        client
            .ae_round(
                NodeId::from(1),
                SmolStr::new("users"),
                vec![(0, 111), (1, 999)],
            )
            .await
            .expect("ae round succeeds")
    };
    for attempt in 0..MAX_AE_DEFERRALS {
        let result = round().await;
        assert!(
            result.is_empty(),
            "round {attempt}: bucket 1 mismatches, but the stream in motion covers it: {result:?}"
        );
    }
    assert_eq!(
        round().await,
        vec![AeMismatch::Bucket(1, kv(entries.clone()))],
        "the bound serves the next round, frames queued or not"
    );
    assert!(round().await.is_empty(), "a served round resets the bound");
    assert!(
        !server.inner.defers_ae_digest_from(NodeId::from(3)),
        "a peer nothing streams to is served as usual"
    );
}

#[tokio::test]
async fn ae_round_times_out_against_a_peer_that_accepts_but_never_responds() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let _stalled_peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME)
            .new_framed(stream);
        let _hello = framed.next().await.expect("hello arrives");
        let _digest = framed.next().await.expect("ae digest arrives");
        // Accepts and reads, then goes silent: the failure mode
        // `REQUEST_TIMEOUT` bounds.
        std::future::pending::<()>().await;
    });

    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), addr)]);

    let result = tokio::time::timeout(
        super::REQUEST_TIMEOUT + Duration::from_secs(5),
        mesh.ae_round(NodeId::from(2), SmolStr::new("users"), Vec::new()),
    )
    .await
    .expect(
        "ae_round must give up on its own internal REQUEST_TIMEOUT, well inside this \
         generous outer bound, not hang forever",
    );
    assert!(
        result.is_err(),
        "a peer that accepts but never responds must surface as an error, not hang"
    );
}

#[tokio::test]
async fn ae_pull_returns_requested_records_as_replicate_messages() {
    let records = vec![sample_record(9)];
    let handler = Arc::new(FixtureHandler {
        records: records.clone(),
        digests: Vec::new(),
        bucket_entries: Vec::new(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let keys = vec![Bytes::from_static(b"k9")];
    let got = client
        .ae_pull(NodeId::from(1), SmolStr::new("users"), keys)
        .await
        .expect("pull succeeds");
    assert_eq!(got, records);
}

#[tokio::test]
async fn ae_entries_returns_full_listings_for_the_requested_buckets() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let handler = Arc::new(FixtureHandler {
        records: Vec::new(),
        digests: Vec::new(),
        bucket_entries: entries.clone(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let got = client
        .ae_entries(NodeId::from(1), SmolStr::new("users"), vec![3, 7])
        .await
        .expect("ae_entries succeeds");

    assert_eq!(got, vec![(3, kv(entries.clone())), (7, kv(entries))]);
}

#[tokio::test]
async fn ae_pull_hashes_resolves_only_the_requested_hash_to_records() {
    // Two entries, one hash requested: the default `records_for_hashes`
    // must filter to the matching key, not hand every key to `records_for`.
    let key_a = Bytes::from_static(b"a");
    let key_b = Bytes::from_static(b"b");
    let ver = Hlc {
        wall_ms: 1,
        logical: 0,
        node: NodeId::from(1),
    };
    let handler = Arc::new(FixtureHandler {
        records: vec![sample_record(9)],
        digests: Vec::new(),
        bucket_entries: vec![(key_a.clone(), ver), (key_b, ver)],
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let handler_for_asserts = Arc::clone(&handler);
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let hash_a = xxh3_64(&key_a);
    let got = client
        .ae_pull_hashes(NodeId::from(1), SmolStr::new("users"), 3, vec![hash_a])
        .await
        .expect("ae_pull_hashes succeeds");
    assert_eq!(got, vec![sample_record(9)]);

    let pulled = handler_for_asserts
        .pulled
        .lock()
        .expect("invariant: fixture mutex is never poisoned");
    assert_eq!(
        pulled.as_slice(),
        [(SmolStr::new("users"), vec![key_a])],
        "the resolved hash must pull exactly key_a's bytes, never key_b's"
    );
}

#[test]
fn batch_replicate_splits_by_count_and_budget_and_keeps_a_singleton_plain() {
    let cache = SmolStr::new("users");
    let tiny = |i: u32| WireRecord {
        key: Bytes::from(i.to_le_bytes().to_vec()),
        value: Some(Bytes::from_static(b"v")),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    };
    assert!(matches!(
        batch_replicate(&cache, vec![tiny(0)]).as_slice(),
        [Msg::Replicate { .. }]
    ));
    let msgs = batch_replicate(
        &cache,
        (0..u32::try_from(REPLICATE_BATCH_COUNT + 1).expect("fits"))
            .map(tiny)
            .collect(),
    );
    assert_eq!(
        msgs.len(),
        2,
        "one record past the count cap starts a second batch"
    );
    assert!(
        matches!(&msgs[0], Msg::ReplicateBatch { recs, .. } if recs.len() == REPLICATE_BATCH_COUNT)
    );
    assert!(matches!(&msgs[1], Msg::Replicate { .. }));
    let big = |i: u32| WireRecord {
        value: Some(Bytes::from(vec![0u8; REPLICATE_BATCH_BUDGET / 3])),
        ..tiny(i)
    };
    let msgs = batch_replicate(&cache, (0..4).map(big).collect());
    assert_eq!(
        msgs.len(),
        2,
        "the byte budget splits four third-budget records two and two"
    );
}

#[test]
fn batch_forward_stamps_every_chunk_and_keeps_a_singleton_a_forward_batch() {
    let cache = SmolStr::new("users");
    let tiny = |i: u32| WireRecord {
        key: Bytes::from(i.to_le_bytes().to_vec()),
        value: Some(Bytes::from_static(b"v")),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    };
    assert!(
        matches!(
            batch_forward(&cache, 9, 1, vec![tiny(0)]).as_slice(),
            [Msg::ForwardBatch { view_hash: 9, hops: 1, recs, .. }] if recs.len() == 1
        ),
        "a lone record still travels as a ForwardBatch, never a plain Replicate"
    );
    let msgs = batch_forward(
        &cache,
        9,
        0,
        (0..u32::try_from(REPLICATE_BATCH_COUNT + 1).expect("fits"))
            .map(tiny)
            .collect(),
    );
    assert_eq!(msgs.len(), 2, "the count cap splits like batch_replicate");
    assert!(msgs.iter().all(|msg| matches!(
        msg,
        Msg::ForwardBatch {
            view_hash: 9,
            hops: 0,
            ..
        }
    )));
    assert!(
        matches!(&msgs[0], Msg::ForwardBatch { recs, .. } if recs.len() == REPLICATE_BATCH_COUNT)
    );
}

#[tokio::test]
async fn ae_pull_of_thousands_of_records_returns_every_record_through_batched_frames() {
    let records: Vec<WireRecord> = (0..3000u32)
        .map(|i| WireRecord {
            key: Bytes::from(i.to_le_bytes().to_vec()),
            value: Some(Bytes::from(vec![7u8; 16])),
            ver: Hlc {
                wall_ms: u64::from(i) + 1,
                logical: 0,
                node: NodeId::from(1),
            },
            expires_at_ms: None,
        })
        .collect();
    let handler = Arc::new(FixtureHandler {
        records: records.clone(),
        digests: Vec::new(),
        bucket_entries: Vec::new(),
        pulled: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let bytes_before = bytes_sent_total();
    let got = client
        .ae_pull(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![Bytes::from_static(b"any")],
        )
        .await
        .expect("ae_pull succeeds");
    let bytes = bytes_sent_total() - bytes_before;
    assert_eq!(got, records, "every pulled record arrives, in order");
    // The reply is `batch_replicate`'s frames plus `ReqDone`, counted
    // from the batching itself: the process-wide frame counter also
    // takes every concurrently running test's frames.
    let frames = batch_replicate(&SmolStr::new("users"), records).len() + 1;
    assert!(
        frames < 100,
        "a 3000-record pull reply travels as a few batch frames, not one per record: {frames}"
    );
    assert!(
        bytes > 0,
        "sending 3000 records' worth of frames must grow the byte counter"
    );
}

#[tokio::test]
async fn replicate_in_flight_reflects_queued_and_recent_frames() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    // A peer nobody listens on: an enqueued frame stays queued forever.
    let unreachable = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    mesh.update_peers(vec![peer_at(NodeId::from(2), unreachable)]);
    let window = Duration::from_millis(500);
    assert!(
        !mesh.replicate_in_flight(NodeId::from(2), window),
        "nothing sent yet"
    );
    assert!(
        !mesh.replicate_in_flight(NodeId::from(9), window),
        "an unknown peer is never in flight"
    );
    let frame = OutFrame::new(Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(1),
    })
    .expect("encodes");
    mesh.send_frames(NodeId::from(2), MsgClass::Replicate, [frame]);
    assert!(
        mesh.replicate_in_flight(NodeId::from(2), window),
        "a queued frame counts as in flight"
    );
    assert!(
        mesh.replicate_in_flight(NodeId::from(2), Duration::ZERO),
        "queued frames count regardless of the window"
    );
}

#[tokio::test]
async fn send_many_enqueued_answers_whether_every_message_reached_the_outbox() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let config = ClusterConfig {
        outbox_capacity: 2,
        ..ClusterConfig::default()
    };
    let (mesh, _inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    let replicate = |key: u8| Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: sample_record(key),
    };
    assert!(
        !mesh.send_many_enqueued(NodeId::from(2), MsgClass::Replicate, [replicate(1)]),
        "a peer missing from the table receives nothing"
    );

    // A peer nobody listens on: its writer never drains the outbox.
    let unreachable = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    mesh.update_peers(vec![peer_at(NodeId::from(2), unreachable)]);
    assert!(
        mesh.send_many_enqueued(NodeId::from(2), MsgClass::Replicate, [replicate(1)]),
        "a known peer's outbox takes the message"
    );
    assert!(
        !mesh.send_many_enqueued(NodeId::from(2), MsgClass::Replicate, (2..=4).map(replicate)),
        "a message that meets a full outbox is dropped"
    );
}

#[tokio::test]
async fn mark_dirty_shows_up_in_the_next_take() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    mesh.update_peers(vec![peer_at(
        NodeId::from(2),
        SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1)),
    )]);
    let dirty = mesh.take_dirty_peers();
    assert!(dirty.is_empty(), "{dirty:?}");
    mesh.mark_dirty(NodeId::from(2));
    assert_eq!(mesh.take_dirty_peers(), vec![NodeId::from(2)]);
    assert!(mesh.take_dirty_peers().is_empty(), "taking clears the mark");
    mesh.mark_dirty(NodeId::from(42));
    assert!(
        mesh.take_dirty_peers().is_empty(),
        "an unknown peer is ignored"
    );
}

/// Builds a `Peer` speaking `protocol` instead of the current build's,
/// for testing distribution-mode's protocol gate.
fn peer_at_protocol(node: NodeId, addr: SocketAddr, protocol: u16) -> Peer {
    Peer {
        protocol,
        ..peer_at(node, addr)
    }
}

/// The kind of a [`CodecError::Io`]; any other error shape fails the test.
fn io_kind(err: &CodecError) -> io::ErrorKind {
    let CodecError::Io(source) = err else {
        panic!("expected a CodecError::Io, got {err:?}");
    };
    source.kind()
}

#[tokio::test]
async fn fetch_returns_the_responders_record_when_found() {
    let rec = sample_record(1);
    let handler = Arc::new(FixtureHandler {
        fetch_fixture: FetchFixture::Found(Some(rec.clone())),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            42,
        )
        .await
        .expect("fetch succeeds");
    assert_eq!(outcome, FetchOutcome::Found(Some(rec)));
}

#[tokio::test]
async fn load_returns_the_responders_records_and_uncached_values() {
    let rec = sample_record(1);
    let handler = Arc::new(FixtureHandler {
        load_fixture: LoadFixture::Loaded(
            vec![rec.clone()],
            vec![(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))],
        ),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .load(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![
                rec.key.clone(),
                Bytes::from_static(b"k2"),
                Bytes::from_static(b"k3"),
            ],
        )
        .await
        .expect("load succeeds");
    assert_eq!(
        outcome,
        LoadOutcome::Loaded {
            found: vec![rec],
            uncached: vec![(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))],
        }
    );
}

#[tokio::test]
async fn load_reports_the_responders_loader_failure() {
    let handler = Arc::new(FixtureHandler {
        load_fixture: LoadFixture::Failed("source unavailable"),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .load(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![Bytes::from_static(b"k1")],
        )
        .await
        .expect("the exchange succeeds");
    assert_eq!(
        outcome,
        LoadOutcome::Failed("source unavailable".to_string())
    );
}

#[tokio::test]
async fn load_is_declined_against_the_default_handler_body() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .load(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![Bytes::from_static(b"k1")],
        )
        .await
        .expect("the exchange succeeds");
    assert_eq!(outcome, LoadOutcome::Declined);
}

#[tokio::test]
async fn load_refuses_to_dial_a_peer_older_than_protocol_load() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        server.local_addr(),
        wire::PROTOCOL_LOAD - 1,
    )]);

    let result = client
        .load(
            NodeId::from(1),
            SmolStr::new("users"),
            vec![Bytes::from_static(b"k1")],
        )
        .await;
    assert!(
        result.is_err(),
        "a peer older than PROTOCOL_LOAD is never sent a load request"
    );
}

#[tokio::test]
async fn fetch_returns_a_definitive_miss() {
    let handler = Arc::new(FixtureHandler {
        fetch_fixture: FetchFixture::Found(None),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            42,
        )
        .await
        .expect("fetch succeeds");
    assert_eq!(outcome, FetchOutcome::Found(None));
}

#[tokio::test]
async fn fetch_reports_stale_when_the_responders_view_hash_differs() {
    let handler = Arc::new(FixtureHandler {
        fetch_fixture: FetchFixture::Stale(999),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            1,
        )
        .await
        .expect("fetch succeeds");
    assert_eq!(
        outcome,
        FetchOutcome::Stale {
            responder_view_hash: 999
        }
    );
}

#[tokio::test]
async fn fetch_is_declined_against_the_default_handler_body() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let outcome = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            1,
        )
        .await
        .expect("fetch succeeds even against a handler that never overrode it");
    assert_eq!(
        outcome,
        FetchOutcome::Declined,
        "the default RequestHandler::fetch body declines, so the requester tries the next owner"
    );
}

#[tokio::test]
async fn fetch_refuses_to_dial_a_peer_that_does_not_speak_distribution_mode() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        server.local_addr(),
        wire::PROTOCOL_DISTRIBUTED - 1,
    )]);

    let result = client
        .fetch(
            NodeId::from(1),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            1,
        )
        .await;
    let err = result.expect_err(
        "a peer that does not speak PROTOCOL_DISTRIBUTED must never be dialed for a fetch",
    );
    assert_eq!(
        io_kind(&err),
        io::ErrorKind::Unsupported,
        "the refusal is the kind that marks an owner too old for a distribution-mode fetch"
    );
}

/// A fetch to a node the peer table does not hold fails before any dial
/// with a `CodecError::Io` of kind `NotFound`, the kind `Cache::explain`
/// reports as an owner that is not a member.
#[tokio::test]
async fn fetch_to_an_unknown_peer_fails_with_not_found() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;

    let err = mesh
        .fetch(
            NodeId::from(42),
            SmolStr::new("users"),
            Bytes::from_static(b"k1"),
            1,
        )
        .await
        .expect_err("a node outside the peer table is never dialed for a fetch");
    assert_eq!(
        io_kind(&err),
        io::ErrorKind::NotFound,
        "an unknown owner is told apart from an unreachable one"
    );
    mesh.shutdown().await;
}

/// A fetch to a peer that accepts and never answers gives up on the mesh's
/// own request timeout with a `CodecError::Io` of kind `TimedOut`, the
/// kind `Cache::explain` reports as an owner that timed out.
#[tokio::test]
async fn fetch_times_out_with_the_mesh_backstop_kind() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener has a local addr");
    let stalled_peer = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept");
            // Every connection reads and never answers, whether it is the
            // persistent writer's or the fetch's: the failure mode
            // `REQUEST_TIMEOUT` bounds.
            tokio::spawn(async move {
                let mut framed = LengthDelimitedCodec::builder()
                    .max_frame_length(MAX_FRAME)
                    .new_framed(stream);
                while framed.next().await.is_some() {}
            });
        }
    });

    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), addr)]);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        with_request_timeout(
            Duration::from_millis(100),
            client.fetch(
                NodeId::from(1),
                SmolStr::new("users"),
                Bytes::from_static(b"k1"),
                1,
            ),
        ),
    )
    .await
    .expect(
        "fetch must give up on its own short request timeout, well inside this generous \
         outer bound, not hang forever",
    );
    let err = result.expect_err("a peer that never answers yields no reply");
    assert_eq!(
        io_kind(&err),
        io::ErrorKind::TimedOut,
        "the mesh's request timeout surfaces as the TimedOut kind"
    );
    client.shutdown().await;
    stalled_peer.abort();
}

#[tokio::test]
async fn ae_round_scoped_returns_only_mismatched_buckets() {
    let entries = vec![(
        Bytes::from_static(b"k1"),
        Hlc {
            wall_ms: 5,
            logical: 0,
            node: NodeId::from(1),
        },
    )];
    let handler = Arc::new(FixtureHandler {
        ae_scoped_fixture: AeScopedFixture::Digests(vec![
            BucketDigest {
                bucket: 0,
                digest: 111,
            },
            BucketDigest {
                bucket: 1,
                digest: 222,
            },
        ]),
        bucket_entries: entries.clone(),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let result = client
        .ae_round_scoped(
            NodeId::from(1),
            SmolStr::new("users"),
            1,
            vec![(0, 111), (1, 999)],
        )
        .await
        .expect("scoped ae round succeeds");
    assert_eq!(
        result,
        AeRoundOutcome::Mismatches(vec![AeMismatch::Bucket(1, kv(entries))])
    );
}

#[tokio::test]
async fn ae_round_scoped_reports_stale_when_the_responders_view_hash_differs() {
    let handler = Arc::new(FixtureHandler {
        ae_scoped_fixture: AeScopedFixture::Stale(777),
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let result = client
        .ae_round_scoped(NodeId::from(1), SmolStr::new("users"), 1, Vec::new())
        .await
        .expect("scoped ae round succeeds");
    assert_eq!(
        result,
        AeRoundOutcome::Stale {
            responder_view_hash: 777
        }
    );
}

#[tokio::test]
async fn ae_round_scoped_reports_no_mismatches_against_the_default_handler_body() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let result = client
        .ae_round_scoped(NodeId::from(1), SmolStr::new("users"), 1, vec![(0, 111)])
        .await
        .expect("scoped ae round succeeds even against a handler that never overrode it");
    assert_eq!(
        result,
        AeRoundOutcome::Mismatches(Vec::new()),
        "the default RequestHandler::ae_digest_scoped body degrades to no mismatches"
    );
}

#[tokio::test]
async fn ae_round_masked_answers_each_bucket_whose_masked_fold_differs_with_its_part_digests() {
    let digests: Vec<u64> = (1..=64).collect();
    let handler = Arc::new(FixtureHandler {
        ae_scoped_fixture: AeScopedFixture::Digests(Vec::new()),
        part_digests: vec![(0, digests.clone()), (1, digests.clone())],
        ..Default::default()
    });
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let result = client
        .ae_round_masked(
            NodeId::from(1),
            SmolStr::new("users"),
            1,
            vec![(0, 0b11, 1 ^ 2), (1, 0b1, 999)],
        )
        .await
        .expect("masked ae round succeeds");
    assert_eq!(
        result,
        AeRoundOutcome::Mismatches(vec![AeMismatch::PartDigests(1, digests)]),
        "bucket 0's fold over parts 0 and 1 matches; bucket 1's does not"
    );
}

#[tokio::test]
async fn ae_round_masked_reports_stale_and_degrades_against_the_default_handler_body() {
    let handler = Arc::new(FixtureHandler {
        ae_scoped_fixture: AeScopedFixture::Stale(777),
        ..Default::default()
    });
    let (stale, _stale_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (plain, _plain_inbound) = spawn_mesh(NodeId::from(3), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![
        peer_at(NodeId::from(1), stale.local_addr()),
        peer_at(NodeId::from(3), plain.local_addr()),
    ]);

    let round = |peer| {
        client.ae_round_masked(
            NodeId::from(peer),
            SmolStr::new("users"),
            1,
            vec![(0, 1, 5)],
        )
    };
    assert_eq!(
        round(1).await.expect("masked ae round succeeds"),
        AeRoundOutcome::Stale {
            responder_view_hash: 777
        }
    );
    assert_eq!(
        round(3).await.expect("masked ae round succeeds"),
        AeRoundOutcome::Mismatches(Vec::new()),
        "the default RequestHandler::ae_digest_masked body degrades to no mismatches"
    );
}

#[tokio::test]
async fn ae_round_masked_refuses_to_dial_a_peer_that_does_not_speak_part_ownership() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        server.local_addr(),
        wire::PROTOCOL_PART_OWNERSHIP - 1,
    )]);

    let result = client
        .ae_round_masked(NodeId::from(1), SmolStr::new("users"), 1, Vec::new())
        .await;
    assert!(
        result.is_err(),
        "a peer that does not speak PROTOCOL_PART_OWNERSHIP is never sent a masked round"
    );
}

#[tokio::test]
async fn ae_round_scoped_refuses_to_dial_a_peer_that_does_not_speak_distribution_mode() {
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        server.local_addr(),
        wire::PROTOCOL_DISTRIBUTED - 1,
    )]);

    let result = client
        .ae_round_scoped(NodeId::from(1), SmolStr::new("users"), 1, Vec::new())
        .await;
    assert!(
        result.is_err(),
        "a peer that does not speak PROTOCOL_DISTRIBUTED must never be dialed for a scoped ae round"
    );
}

/// Records carried by every `Replicate`/`ReplicateBatch` message read
/// off `inbound` within `wait`.
async fn count_replicated(inbound: &mut mpsc::Receiver<InboundMsg>, wait: Duration) -> usize {
    let deadline = tokio::time::Instant::now() + wait;
    let mut count = 0usize;
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, inbound.recv()).await {
        match msg.msg {
            Msg::Replicate { .. } => count += 1,
            Msg::ReplicateBatch { recs, .. } => count += recs.len(),
            _ => {}
        }
    }
    count
}

fn replicate_msgs(count: usize) -> Vec<Msg> {
    (0..count)
        .map(|i| Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: WireRecord {
                key: Bytes::from(format!("k{i}")),
                value: Some(Bytes::from_static(b"v")),
                ver: Hlc {
                    wall_ms: 1,
                    logical: u32::try_from(i).expect("small"),
                    node: NodeId::from(1),
                },
                expires_at_ms: None,
            },
        })
        .collect()
}

#[tokio::test]
async fn shutdown_flushes_replicate_frames_queued_just_before_it() {
    let (receiver, mut inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    let (sender, _sender_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    sender.update_peers(vec![peer_at(NodeId::from(2), receiver.local_addr())]);

    sender.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(2_000));
    sender.shutdown().await;

    let got = count_replicated(&mut inbound, Duration::from_secs(3)).await;
    assert_eq!(
        got, 2_000,
        "every frame queued before shutdown reaches the peer"
    );
    receiver.shutdown().await;
}

/// A mesh that names `cluster` in its `Hello` and checks every accepted
/// one against it, as `Cluster::build` spawns it.
async fn spawn_named_mesh(node: NodeId, cluster: &str) -> (Mesh, mpsc::Receiver<InboundMsg>) {
    let bound = BoundListener::bind("127.0.0.1:0".parse().expect("valid loopback addr"))
        .await
        .expect("bind loopback");
    let greeting = Greeting {
        node,
        incarnation: 1,
        cluster: wire::cluster_id(cluster),
    };
    Mesh::spawn_on(bound, greeting, &ClusterConfig::default(), empty_handler())
        .expect("spawn on the bound listener")
}

/// A node of another cluster that dials this node's address, left over
/// from a node of its own that held it before, gets nothing through:
/// its replication never reaches this cluster's inbound. A node of the
/// same cluster, and an unnamed peer standing in for one older than
/// protocol 6, still do.
#[tokio::test]
async fn an_accepted_connection_from_another_cluster_delivers_nothing() {
    let (receiver, mut inbound) = spawn_named_mesh(NodeId::from(2), "prices").await;
    let receiver_peer = peer_at(NodeId::from(2), receiver.local_addr());

    let (stranger, _) = spawn_named_mesh(NodeId::from(3), "sessions").await;
    stranger.update_peers(vec![receiver_peer.clone()]);
    stranger.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(10));
    assert_eq!(
        count_replicated(&mut inbound, Duration::from_secs(1)).await,
        0,
        "another cluster's replication is dropped at the handshake"
    );

    let (sibling, _) = spawn_named_mesh(NodeId::from(4), "prices").await;
    sibling.update_peers(vec![receiver_peer.clone()]);
    sibling.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(10));
    assert_eq!(
        count_replicated(&mut inbound, Duration::from_secs(3)).await,
        10
    );

    let (unnamed, _) = spawn_mesh(NodeId::from(5), empty_handler()).await;
    unnamed.update_peers(vec![receiver_peer]);
    unnamed.send_many(NodeId::from(2), MsgClass::Replicate, replicate_msgs(10));
    assert_eq!(
        count_replicated(&mut inbound, Duration::from_secs(3)).await,
        10,
        "a peer that names no cluster is accepted, as an older release is"
    );

    for mesh in [stranger, sibling, unnamed, receiver] {
        mesh.shutdown().await;
    }
}

#[tokio::test]
async fn send_frames_awaiting_waits_for_outbox_space_instead_of_dropping() {
    let (receiver, mut inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let config = ClusterConfig {
        outbox_capacity: 4,
        ..ClusterConfig::default()
    };
    let (sender, _sender_inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");
    sender.update_peers(vec![peer_at(NodeId::from(2), receiver.local_addr())]);

    let frames: Vec<OutFrame> = replicate_msgs(200)
        .into_iter()
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect();
    sender.send_frames_awaiting(NodeId::from(2), frames).await;
    sender.shutdown().await;

    let got = count_replicated(&mut inbound, Duration::from_secs(3)).await;
    assert_eq!(
        got, 200,
        "a four-slot outbox carries two hundred frames when the sender waits for space"
    );
    receiver.shutdown().await;
}

#[test]
fn fan_out_keep_waiting_only_for_a_live_peer() {
    assert!(
        fan_out_keep_waiting(true),
        "a live peer's frame is never dropped: keep waiting"
    );
    assert!(
        !fan_out_keep_waiting(false),
        "a peer gone from the table falls back to today's drop-and-count path"
    );
}

/// Pins that a live peer's stalled outbox keeps `send_frames_awaiting`
/// waiting past one `FAN_OUT_SEND_DEADLINE` instead of dropping frames.
#[tokio::test]
async fn send_frames_awaiting_waits_past_one_deadline_for_a_still_live_peer() {
    let config = ClusterConfig {
        outbox_capacity: 1,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _sender_inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");

    // No listener behind this peer, so its outbox never drains.
    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    sender.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    // Fills the one-slot outbox so the call below has to wait.
    sender.send(
        NodeId::from(2),
        MsgClass::Replicate,
        Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: sample_record(0),
        },
    );

    let frames: Vec<OutFrame> = replicate_msgs(3)
        .into_iter()
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect();
    let started = tokio::time::Instant::now();
    // Bounded past one deadline slice; the call should still be waiting.
    let outcome = tokio::time::timeout(
        FAN_OUT_SEND_DEADLINE * 2,
        sender.send_frames_awaiting(NodeId::from(2), frames),
    )
    .await;
    assert!(
        outcome.is_err(),
        "a live peer's frames are never dropped, so the wait outlives one deadline slice"
    );
    assert!(
        started.elapsed() >= FAN_OUT_SEND_DEADLINE,
        "the call keeps waiting past at least one full slice"
    );
    assert!(
        sender.take_dirty_peers().is_empty(),
        "a live peer's stalled outbox is never marked dirty, so nothing was dropped"
    );
}

/// Pins that removing the peer mid-wait drops its remaining frames
/// instead of waiting forever.
#[tokio::test]
async fn send_frames_awaiting_drops_once_the_peer_leaves_the_table_mid_wait() {
    let config = ClusterConfig {
        outbox_capacity: 1,
        ..ClusterConfig::default()
    };
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let (sender, _sender_inbound) = Mesh::spawn(addr, NodeId::from(1), 1, &config, empty_handler())
        .await
        .expect("bind loopback");

    let dead_peer: SocketAddr = "127.0.0.1:1".parse().expect("valid unroutable addr");
    sender.update_peers(vec![peer_at(NodeId::from(2), dead_peer)]);

    // take_dirty_peers only reports peers still in the table, so this
    // reads the dirty flag directly off the handle instead.
    let dirty_flag = {
        let table = sender.inner.peers.read().expect("lock");
        Arc::clone(&table.get(&NodeId::from(2)).expect("peer registered").dirty)
    };

    sender.send(
        NodeId::from(2),
        MsgClass::Replicate,
        Msg::Replicate {
            cache: SmolStr::new("users"),
            rec: sample_record(0),
        },
    );

    let frames: Vec<OutFrame> = replicate_msgs(3)
        .into_iter()
        .map(|msg| OutFrame::new(msg).expect("encodes"))
        .collect();
    let wait_fut = sender.send_frames_awaiting(NodeId::from(2), frames);
    tokio::pin!(wait_fut);

    // Confirms the call is still pending before the peer is removed.
    let not_yet = tokio::time::timeout(Duration::from_millis(200), &mut wait_fut).await;
    assert!(
        not_yet.is_err(),
        "the one-slot outbox is still full; the call must still be waiting"
    );
    assert!(
        !dirty_flag.load(Ordering::Relaxed),
        "nothing dropped yet while the peer is still live"
    );

    sender.update_peers(vec![]);

    tokio::time::timeout(Duration::from_secs(3), &mut wait_fut)
        .await
        .expect("send_frames_awaiting returns once the peer is gone");

    // `dirty` is set only alongside the drop counter, so this proves
    // the remaining frames were dropped rather than waited on.
    assert!(
        dirty_flag.load(Ordering::Relaxed),
        "a peer that leaves mid-wait drops its remaining frames and is marked dirty"
    );
}

#[tokio::test]
async fn request_buckets_streams_multiple_chunks_to_done() {
    let chunk_a = vec![sample_record(1)];
    let chunk_b = vec![sample_record(2), sample_record(3)];
    let handler = Arc::new(FixtureHandler {
        buckets_available: true,
        bucket_chunks: vec![chunk_a.clone(), chunk_b.clone()],
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let BucketPull::Stream(mut stream) = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0, 1], 42)
        .await
        .expect("request accepted")
    else {
        panic!("an available donor streams");
    };
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        if let BucketStreamItem::Chunk(recs) = item.expect("item decodes") {
            got.extend(recs);
        }
    }
    let mut expected = chunk_a;
    expected.extend(chunk_b);
    assert_eq!(got, expected);
}

#[tokio::test]
async fn request_buckets_yields_bucket_done_once_per_bucket_before_the_stream_ends() {
    let handler = Arc::new(FixtureHandler {
        buckets_available: true,
        bucket_chunks: vec![vec![sample_record(1)], vec![sample_record(2)]],
        bucket_chunk_tags: vec![0, 1],
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let BucketPull::Stream(mut stream) = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0, 1], 42)
        .await
        .expect("request accepted")
    else {
        panic!("an available donor streams");
    };
    let mut dones = Vec::new();
    while let Some(item) = stream.next().await {
        if let BucketStreamItem::BucketDone(bucket) = item.expect("item decodes") {
            dones.push(bucket);
        }
    }
    assert_eq!(
        dones,
        vec![0, 1],
        "each bucket's done arrives once, in request order, ahead of the stream's own end"
    );
}

#[tokio::test]
async fn request_buckets_declines_when_the_donor_is_unavailable() {
    let handler = Arc::new(FixtureHandler {
        buckets_available: false,
        ownership_view_hash: Some(123),
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let declined = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0], 999)
        .await
        .expect("request accepted");
    assert!(
        matches!(declined, BucketPull::Stale),
        "a donor whose view has diverged declines with StaleView"
    );
    // The declined connection went back to the pool: an unrelated
    // request on the same peer still works.
    let digests = requester
        .ae_round(NodeId::from(1), SmolStr::new("users"), Vec::new())
        .await
        .expect("the pooled connection serves the next request");
    assert!(digests.is_empty(), "{digests:?}");
}

#[tokio::test]
async fn request_buckets_declines_cold_when_the_donor_has_not_pulled_the_buckets() {
    let handler = Arc::new(FixtureHandler {
        buckets_available: true,
        buckets_cold: true,
        ownership_view_hash: Some(42),
        ..Default::default()
    });
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let declined = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0], 42)
        .await
        .expect("request accepted");
    assert!(
        matches!(declined, BucketPull::Cold),
        "a donor that has not pulled a requested bucket declines as cold, not stale"
    );
    let digests = requester
        .ae_round(NodeId::from(1), SmolStr::new("users"), Vec::new())
        .await
        .expect("the pooled connection serves the next request");
    assert!(digests.is_empty(), "{digests:?}");
}

#[tokio::test]
async fn request_buckets_declines_against_the_default_handler_body() {
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

    let declined = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0], 1)
        .await
        .expect("request accepted even against a handler that never overrode it");
    assert!(
        matches!(declined, BucketPull::Stale),
        "the default RequestHandler::st_buckets_available body degrades to false"
    );
}

#[tokio::test]
async fn request_buckets_refuses_to_dial_a_peer_that_does_not_speak_distribution_mode() {
    let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    requester.update_peers(vec![peer_at_protocol(
        NodeId::from(1),
        donor.local_addr(),
        wire::PROTOCOL_DISTRIBUTED - 1,
    )]);

    let result = requester
        .request_buckets(NodeId::from(1), SmolStr::new("users"), vec![0], 1)
        .await;
    assert!(
        result.is_err(),
        "a peer that does not speak PROTOCOL_DISTRIBUTED must never be dialed for a bucket pull"
    );
}

/// A handler whose `entries_for_buckets` never resolves, for
/// [`ae_entries_times_out_promptly_under_a_short_override`].
struct HangingHandler;
impl RequestHandler for HangingHandler {
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
    ) -> BoxFuture<'_, crate::store::BucketEntries> {
        Box::pin(std::future::pending())
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
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        Box::pin(async { Vec::new() })
    }
}

#[tokio::test]
async fn ae_entries_times_out_promptly_under_a_short_override() {
    // `AeEntries` is served by `entries_for_buckets`, which never
    // resolves here: `ae_entries` must give up on its own internal
    // timeout rather than hang forever waiting on a stuck peer.
    let (server, _server_inbound) = spawn_mesh(NodeId::from(1), Arc::new(HangingHandler)).await;
    let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
    client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

    let short = Duration::from_millis(100);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        with_request_timeout(
            short,
            client.ae_entries(NodeId::from(1), SmolStr::new("users"), vec![3]),
        ),
    )
    .await
    .expect(
        "ae_entries must give up on its own short REQUEST_TIMEOUT override, well inside \
         this generous outer bound, not hang forever",
    );
    assert!(
        result.is_err(),
        "a handler whose entries never resolve must time out, not hang"
    );
}

#[tokio::test]
async fn request_to_an_unknown_peer_errors_instead_of_hanging() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let err = mesh
        .ae_pull(NodeId::from(42), SmolStr::new("users"), Vec::new())
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn inbound_broadcast_traffic_reaches_the_mpsc_receiver() {
    let (server, mut inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let addr = server.local_addr();

    let sender = TcpStream::connect(addr).await.expect("connect");
    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(sender);
    let hello = wire::encode(&Msg::Hello {
        node: NodeId::from(2),
        incarnation: 1,
        protocol: wire::PROTOCOL_VERSION,
        cluster: wire::UNNAMED_CLUSTER,
    })
    .expect("encodes");
    futures::SinkExt::send(&mut framed, hello)
        .await
        .expect("send hello");

    let invalidate = Msg::Invalidate {
        cache: SmolStr::new("users"),
        key: Bytes::from_static(b"k1"),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(2),
        },
    };
    let encoded = wire::encode(&invalidate).expect("encodes");
    futures::SinkExt::send(&mut framed, encoded)
        .await
        .expect("send invalidate");

    let got = inbound.recv().await.expect("message forwarded");
    assert_eq!(
        got,
        InboundMsg {
            from: NodeId::from(2),
            msg: invalidate
        }
    );
}

#[tokio::test]
async fn update_peers_cancels_the_writer_of_a_dropped_peer() {
    let (mesh, _inbound) = spawn_mesh(NodeId::from(1), empty_handler()).await;
    let unreachable = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1));
    mesh.update_peers(vec![peer_at(NodeId::from(2), unreachable)]);

    let cancel = {
        let table = mesh.inner.peers.read().expect("lock");
        table
            .get(&NodeId::from(2))
            .expect("peer registered")
            .cancel
            .clone()
    };
    assert!(!cancel.is_cancelled(), "not cancelled while still a peer");

    // Dropping peer 2 from the incoming set removes its handle and
    // must cancel the writer task spawned for it.
    mesh.update_peers(Vec::new());
    assert!(
        cancel.is_cancelled(),
        "removing a peer must cancel its writer's token"
    );
    assert!(
        mesh.inner
            .peers
            .read()
            .expect("lock")
            .get(&NodeId::from(2))
            .is_none(),
        "the dropped peer's handle must leave the table"
    );
}
