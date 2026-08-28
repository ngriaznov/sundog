//! Data plane: a lazily-dialed TCP mesh between live peers, carrying
//! invalidations, replications, state transfer, and anti-entropy. Losing a
//! message here is acceptable by design — anti-entropy repairs it. Plan §3, §6.
//!
//! Every live peer gets one persistent, unidirectional writer connection
//! (`Hello` then a stream of `Invalidate`/`Replicate`, drained from bounded
//! per-class outboxes) plus, on demand, a fresh short-lived connection per
//! request/response exchange (state transfer, anti-entropy) — kept off the
//! broadcast path so a slow snapshot never backs up live traffic. On the
//! accept side, both kinds of connection share one listener: every
//! connection starts with `Hello`, and the message that follows decides
//! whether the connection is served once (a request) or looped indefinitely
//! (the persistent link), per plan "own streams" for request/response.

mod conn;
mod outbox;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use smol_str::SmolStr;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::ClusterConfig;
use crate::error::{CodecError, JoinError};
use crate::hlc::Hlc;
use crate::membership::Peer;
use crate::node::NodeId;
use crate::wire::{Msg, WireRecord};
use outbox::DropOldestQueue;

/// Capacity of the inbound-message channel and of each per-peer, per-class
/// outbox absent a caller override — mirrors [`ClusterConfig::outbox_capacity`]'s
/// default so a `Mesh` built directly against a default `ClusterConfig`
/// behaves identically everywhere.
const DEFAULT_CHANNEL_CAPACITY: usize = 8_192;

/// Backpressure class for a fan-out message, selecting the per-class drop
/// policy on outbox overflow (plan §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MsgClass {
    /// Overflow drops the oldest queued invalidation for that peer: an
    /// invalidation storm on a dead peer must never stall writers.
    Invalidate,
    /// Overflow drops the new message and marks the peer dirty so the next
    /// anti-entropy round targets it first.
    Replicate,
}

/// One inbound message, tagged with the peer it arrived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMsg {
    /// The peer that sent this message.
    pub from: NodeId,
    /// The message itself.
    pub msg: Msg,
}

/// What the net layer needs from the local shard registry to answer another
/// node's state-transfer or anti-entropy request, without depending on
/// `store` directly. An implementation typically looks `cache` up in the
/// `Cluster` shard registry and delegates to its `ShardOps`; an unknown
/// cache name should degrade to an empty result rather than an error — a
/// donor that doesn't (yet) have the cache is a normal race, not a fault.
pub trait RequestHandler: Send + Sync + 'static {
    /// Streams a full snapshot of `cache` in write-sized chunks, for state
    /// transfer on join (plan §9).
    fn snapshot_chunks(&self, cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>>;
    /// Returns `cache`'s current per-bucket digest array (plan §8).
    fn digests(&self, cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>>;
    /// Returns the live key/version listing for one bucket of `cache`.
    fn bucket_entries(&self, cache: SmolStr, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>>;
    /// Returns full records for `keys` in `cache` that this node holds.
    fn records_for(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>>;
}

struct PeerHandle {
    data_addr: SocketAddr,
    invalidate: Arc<DropOldestQueue>,
    replicate_tx: mpsc::Sender<Msg>,
    dirty: Arc<AtomicBool>,
    cancel: CancellationToken,
}

struct MeshInner {
    node: NodeId,
    incarnation: u64,
    outbox_capacity: usize,
    peers: RwLock<HashMap<NodeId, PeerHandle>>,
    accept_cancel: CancellationToken,
}

/// A cheap-to-clone handle onto the running data-plane mesh.
#[derive(Clone)]
pub struct Mesh {
    local_addr: SocketAddr,
    inner: Arc<MeshInner>,
}

impl Mesh {
    /// Binds the data-plane TCP listener and starts the mesh's background
    /// accept/dial tasks, returning the handle together with the single
    /// receiver of inbound messages (invalidations, replications, and
    /// unsolicited traffic — request/response traffic is returned inline by
    /// [`Mesh::request_state`], [`Mesh::ae_round`], and [`Mesh::ae_pull`]
    /// instead of flowing through this channel).
    ///
    /// `incarnation` is embedded in every `Hello` this node sends; `handler`
    /// answers `StRequest`/`AeDigest`/`AePull` from peers dialing in.
    ///
    /// Deviates from the original stub signature (`spawn(bind_addr, node)`):
    /// adds `incarnation`, `config` (for `outbox_capacity`), and `handler`,
    /// all needed to serve the request/response side and drive the drop
    /// policy — see this crate's build-agent report for the rationale.
    ///
    /// # Errors
    ///
    /// Returns [`JoinError::Bind`] if `bind_addr` cannot be bound.
    pub async fn spawn(
        bind_addr: SocketAddr,
        node: NodeId,
        incarnation: u64,
        config: &ClusterConfig,
        handler: Arc<dyn RequestHandler>,
    ) -> Result<(Self, mpsc::Receiver<InboundMsg>), JoinError> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|source| JoinError::Bind {
                addr: bind_addr,
                source,
            })?;
        let local_addr = listener.local_addr().map_err(|source| JoinError::Bind {
            addr: bind_addr,
            source,
        })?;

        let outbox_capacity = if config.outbox_capacity == 0 {
            DEFAULT_CHANNEL_CAPACITY
        } else {
            config.outbox_capacity
        };
        let (inbound_tx, inbound_rx) = mpsc::channel(outbox_capacity);
        let accept_cancel = CancellationToken::new();
        tokio::spawn(conn::accept_loop(
            listener,
            inbound_tx,
            handler,
            accept_cancel.clone(),
        ));

        let inner = Arc::new(MeshInner {
            node,
            incarnation,
            outbox_capacity,
            peers: RwLock::new(HashMap::new()),
            accept_cancel,
        });
        Ok((Self { local_addr, inner }, inbound_rx))
    }

    /// The address the data-plane listener is actually bound to (relevant
    /// when `bind_addr` used the ephemeral port `0`).
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Refreshes the set of peers the mesh dials and fans traffic out to:
    /// spawns a writer task (and fresh outboxes) for each newly seen peer,
    /// and cancels the task for any peer no longer present or whose
    /// `data_addr` changed (a restart on a fresh ephemeral port looks the
    /// same as a departure-and-rejoin here). Called whenever
    /// [`crate::membership::Membership::peers`] publishes a change.
    ///
    /// # Panics
    ///
    /// Panics if the internal peer-table lock is poisoned, which only
    /// happens if an earlier call already panicked while holding it.
    pub fn update_peers(&self, peers: Vec<Peer>) {
        let incoming: HashMap<NodeId, SocketAddr> = peers
            .into_iter()
            .filter(|peer| peer.node != self.inner.node)
            .map(|peer| (peer.node, peer.data_addr))
            .collect();

        let mut table = self
            .inner
            .peers
            .write()
            .expect("invariant: peers lock is never poisoned");
        table.retain(|node, handle| {
            let keep = incoming.get(node) == Some(&handle.data_addr);
            if !keep {
                handle.cancel.cancel();
            }
            keep
        });
        for (node, data_addr) in incoming {
            table
                .entry(node)
                .or_insert_with(|| self.spawn_peer_handle(data_addr));
        }
    }

    fn spawn_peer_handle(&self, data_addr: SocketAddr) -> PeerHandle {
        let invalidate = Arc::new(DropOldestQueue::new(self.inner.outbox_capacity));
        let (replicate_tx, replicate_rx) = mpsc::channel(self.inner.outbox_capacity);
        let cancel = CancellationToken::new();
        tokio::spawn(conn::run_peer_writer(
            self.inner.node,
            self.inner.incarnation,
            data_addr,
            Arc::clone(&invalidate),
            replicate_rx,
            cancel.clone(),
        ));
        PeerHandle {
            data_addr,
            invalidate,
            replicate_tx,
            dirty: Arc::new(AtomicBool::new(false)),
            cancel,
        }
    }

    /// Best-effort, non-blocking fan-out of `msg` to `peer` on the outbox
    /// selected by `class`. Never blocks the caller; overflow is handled per
    /// `class`'s drop policy (plan §6) rather than propagated as an error. A
    /// `peer` the mesh doesn't currently know about (not live, or not yet
    /// reconciled by [`Mesh::update_peers`]) is silently a no-op.
    ///
    /// # Panics
    ///
    /// Panics if the internal peer-table lock is poisoned, which only
    /// happens if an earlier call already panicked while holding it.
    pub fn send(&self, peer: NodeId, class: MsgClass, msg: Msg) {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        let Some(handle) = table.get(&peer) else {
            return;
        };
        match class {
            MsgClass::Invalidate => handle.invalidate.push(msg),
            MsgClass::Replicate => {
                if let Err(mpsc::error::TrySendError::Full(_)) = handle.replicate_tx.try_send(msg) {
                    handle.dirty.store(true, Ordering::Relaxed);
                    metrics::counter!("sundog_backlog_dropped_total", "peer" => peer.to_string())
                        .increment(1);
                }
            }
        }
    }

    /// Returns every peer whose `Replicate` outbox has dropped a message
    /// since the last call, clearing their dirty mark — the anti-entropy
    /// scheduler should target these peers before picking randomly (plan §8).
    ///
    /// # Panics
    ///
    /// Panics if the internal peer-table lock is poisoned, which only
    /// happens if an earlier call already panicked while holding it.
    #[must_use]
    pub fn take_dirty_peers(&self) -> Vec<NodeId> {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        table
            .iter()
            .filter(|(_, handle)| handle.dirty.swap(false, Ordering::Relaxed))
            .map(|(node, _)| *node)
            .collect()
    }

    fn peer_addr(&self, peer: NodeId) -> Result<SocketAddr, CodecError> {
        self.inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned")
            .get(&peer)
            .map(|handle| handle.data_addr)
            .ok_or_else(|| {
                CodecError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("peer {peer} is not a known mesh member"),
                ))
            })
    }

    /// Requests a full snapshot of `cache` from `donor` (state transfer on
    /// join, plan §9) and returns a stream of its `StChunk` records, read
    /// lazily off a fresh connection as the caller polls.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `donor` is not a known peer or the request
    /// cannot be sent.
    pub async fn request_state(
        &self,
        donor: NodeId,
        cache: SmolStr,
    ) -> Result<BoxStream<'static, Result<WireRecord, CodecError>>, CodecError> {
        let addr = self.peer_addr(donor)?;
        let mut framed =
            conn::dial_with_hello(addr, self.inner.node, self.inner.incarnation).await?;
        conn::send_msg(&mut framed, &Msg::StRequest { cache }).await?;
        Ok(conn::state_stream(framed))
    }

    /// Runs one anti-entropy digest exchange against `peer`: sends
    /// `local_buckets` and returns the peer's entries for every bucket whose
    /// digest mismatched.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is not a known peer or the
    /// request/response exchange fails.
    pub async fn ae_round(
        &self,
        peer: NodeId,
        cache: SmolStr,
        local_buckets: Vec<(u16, u64)>,
    ) -> Result<Vec<(u16, Vec<(Bytes, Hlc)>)>, CodecError> {
        let addr = self.peer_addr(peer)?;
        let mut framed =
            conn::dial_with_hello(addr, self.inner.node, self.inner.incarnation).await?;
        conn::send_msg(
            &mut framed,
            &Msg::AeDigest {
                cache,
                buckets: local_buckets,
            },
        )
        .await?;
        conn::collect_ae_buckets(framed).await
    }

    /// Pulls full records for `keys` from `peer` (the `AePull` step of
    /// anti-entropy: entries the requester is missing or holds stale).
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] if `peer` is not a known peer or the
    /// request/response exchange fails.
    pub async fn ae_pull(
        &self,
        peer: NodeId,
        cache: SmolStr,
        keys: Vec<Bytes>,
    ) -> Result<Vec<WireRecord>, CodecError> {
        let addr = self.peer_addr(peer)?;
        let mut framed =
            conn::dial_with_hello(addr, self.inner.node, self.inner.incarnation).await?;
        conn::send_msg(&mut framed, &Msg::AePull { cache, keys }).await?;
        conn::collect_pulled_records(framed).await
    }

    /// Shuts down the mesh: stops accepting, and cancels every per-peer
    /// writer task. Background tasks are detached (spawned, not joined) —
    /// this signals them to stop but doesn't wait for the sockets to close.
    ///
    /// # Panics
    ///
    /// Panics if the internal peer-table lock is poisoned, which only
    /// happens if an earlier call already panicked while holding it.
    pub async fn shutdown(self) {
        self.inner.accept_cancel.cancel();
        let table = std::mem::take(
            &mut *self
                .inner
                .peers
                .write()
                .expect("invariant: peers lock is never poisoned"),
        );
        for handle in table.into_values() {
            handle.cancel.cancel();
        }
        // Yield once so cancelled writer/accept tasks get a chance to
        // observe the token before this handle is dropped.
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::StreamExt as _;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::LengthDelimitedCodec;

    use super::*;
    use crate::node::NodeName;
    use crate::wire::{self, MAX_FRAME};

    struct FixtureHandler {
        records: Vec<WireRecord>,
        digests: Vec<(u16, u64)>,
        bucket_entries: Vec<(Bytes, Hlc)>,
        pulled: Mutex<Vec<(SmolStr, Vec<Bytes>)>>,
    }

    impl RequestHandler for FixtureHandler {
        fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
            Box::pin(futures::stream::iter(vec![self.records.clone()]))
        }

        fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<(u16, u64)>> {
            Box::pin(async { self.digests.clone() })
        }

        fn bucket_entries(
            &self,
            _cache: SmolStr,
            _bucket: u16,
        ) -> BoxFuture<'_, Vec<(Bytes, Hlc)>> {
            Box::pin(async { self.bucket_entries.clone() })
        }

        fn records_for(&self, cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
            self.pulled
                .lock()
                .expect("invariant: fixture mutex is never poisoned")
                .push((cache, keys));
            Box::pin(async { self.records.clone() })
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
        })
    }

    fn peer_at(node: NodeId, addr: SocketAddr) -> Peer {
        Peer {
            node,
            name: NodeName::new("test", node),
            gossip_addr: addr,
            data_addr: addr,
            incarnation: 1,
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
        let msg = wire::decode(&frame).expect("decodes");
        assert_eq!(
            msg,
            Msg::Hello {
                node: NodeId::from(1),
                incarnation: 1
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

        // A peer with no listener behind it: the writer task will spin on
        // connection failures forever, so its outbox never drains — exactly
        // the "invalidation storm on a dead peer" scenario the policy targets.
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
        assert_eq!(first, msg(2));
        assert_eq!(second, msg(3));
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
        });
        let (donor, _donor_inbound) = spawn_mesh(NodeId::from(1), handler).await;
        let (requester, _req_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
        requester.update_peers(vec![peer_at(NodeId::from(1), donor.local_addr())]);

        let mut stream = requester
            .request_state(NodeId::from(1), SmolStr::new("users"))
            .await
            .expect("request accepted");
        let mut got = Vec::new();
        while let Some(rec) = stream.next().await {
            got.push(rec.expect("record decodes"));
        }
        assert_eq!(got, records);
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
        });
        let (server, _server_inbound) = spawn_mesh(NodeId::from(1), handler).await;
        let (client, _client_inbound) = spawn_mesh(NodeId::from(2), empty_handler()).await;
        client.update_peers(vec![peer_at(NodeId::from(1), server.local_addr())]);

        // bucket 0 matches (same digest), bucket 1 mismatches, bucket 2 is
        // one the requester has that the server doesn't report locally.
        let local_buckets = vec![(0, 111), (1, 999)];
        let result = client
            .ae_round(NodeId::from(1), SmolStr::new("users"), local_buckets)
            .await
            .expect("ae round succeeds");

        assert_eq!(result, vec![(1, entries)]);
    }

    #[tokio::test]
    async fn ae_pull_returns_requested_records_as_replicate_messages() {
        let records = vec![sample_record(9)];
        let handler = Arc::new(FixtureHandler {
            records: records.clone(),
            digests: Vec::new(),
            bucket_entries: Vec::new(),
            pulled: Mutex::new(Vec::new()),
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
        })
        .expect("encodes");
        futures::SinkExt::send(&mut framed, Bytes::from(hello))
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
        futures::SinkExt::send(&mut framed, Bytes::from(encoded))
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
}
