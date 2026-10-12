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

mod anti_entropy;
mod distribution;
mod fan_out;
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
