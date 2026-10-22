//! The broadcast side of [`Mesh`].

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    BoundListener, DEFAULT_CHANNEL_CAPACITY, FAN_OUT_SEND_DEADLINE, Greeting, InboundMsg, Mesh,
    MeshInner, MsgClass, OUTBOX_FLUSH_DEADLINE, OUTBOX_FLUSH_TAIL, OutFrame, PeerHandle,
    RequestHandler, build_tls_ctx, conn, fan_out_keep_waiting, mono_ms,
};
use crate::config::ClusterConfig;
use crate::error::JoinError;
use crate::membership::Peer;
use crate::net::outbox::DropOldestQueue;
use crate::node::NodeId;
use crate::wire::{self, Msg};

impl Mesh {
    /// Binds the data-plane TCP listener and starts the mesh's background
    /// accept/dial tasks, returning the handle with the receiver of
    /// inbound invalidation/replication traffic; request/response traffic
    /// returns inline from [`Mesh::request_state`], [`Mesh::ae_round`],
    /// and [`Mesh::ae_pull`] instead. `incarnation` is embedded in every
    /// `Hello` this node sends; `handler` answers requests from peers
    /// dialing in; `config.outbox_capacity` sizes the inbound channel and
    /// outboxes.
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
        let listener = BoundListener::bind(bind_addr).await?;
        let greeting = Greeting {
            node,
            incarnation,
            cluster: wire::UNNAMED_CLUSTER,
        };
        Self::spawn_on(listener, greeting, config, handler)
    }

    /// [`Mesh::spawn`] over a listener already bound by
    /// [`BoundListener::bind`], so the port a caller advertised before
    /// spawning is the one the mesh accepts on, and naming the cluster in
    /// `greeting`: the mesh drops an accepted connection whose `Hello`
    /// names another.
    #[cfg_attr(
        not(all(feature = "tls", not(feature = "sim"))),
        expect(
            clippy::unnecessary_wraps,
            reason = "only building the TLS context can fail"
        )
    )]
    pub(crate) fn spawn_on(
        listener: BoundListener,
        greeting: Greeting,
        config: &ClusterConfig,
        handler: Arc<dyn RequestHandler>,
    ) -> Result<(Self, mpsc::Receiver<InboundMsg>), JoinError> {
        let BoundListener {
            listener,
            local_addr,
        } = listener;
        #[cfg(all(feature = "tls", not(feature = "sim")))]
        let tls = build_tls_ctx(config, local_addr)?;
        #[cfg(not(all(feature = "tls", not(feature = "sim"))))]
        let tls = build_tls_ctx(config);

        let outbox_capacity = if config.outbox_capacity == 0 {
            DEFAULT_CHANNEL_CAPACITY
        } else {
            config.outbox_capacity
        };
        let (inbound_tx, inbound_rx) = mpsc::channel(outbox_capacity);
        let inner = Arc::new(MeshInner {
            node: greeting.node,
            incarnation: greeting.incarnation,
            cluster: greeting.cluster,
            outbox_capacity,
            peers: RwLock::new(HashMap::new()),
            accept_cancel: CancellationToken::new(),
            tls,
            admission: conn::Admission::new(),
            acked_buckets: RwLock::new(HashMap::new()),
        });
        tokio::spawn(conn::accept_loop(
            listener,
            inbound_tx,
            handler,
            Arc::clone(&inner),
        ));
        Ok((Self { local_addr, inner }, inbound_rx))
    }

    /// The address the data-plane listener is bound to, relevant when
    /// `bind_addr` used port `0`.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The greeting this mesh opens every connection with, for the tests
    /// that check what `Cluster::build` named in it.
    #[cfg(all(test, not(feature = "sim")))]
    pub(crate) fn greeting(&self) -> Greeting {
        self.inner.greeting()
    }

    /// Refreshes the set of peers the mesh dials and fans traffic out to:
    /// spawns a writer for each newly seen peer, cancels it for any that
    /// departed or changed `data_addr`. The IPs of the peers' data addresses
    /// are the sources the cap on connections waiting for their `Hello`
    /// never refuses. Called on every
    /// [`crate::membership::Membership::peers`] change.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub fn update_peers(&self, peers: Vec<Peer>) {
        // `incoming_peers` keeps `peers`' own order (minus self) so the
        // spawn loop below creates each newly seen peer's writer in that
        // deterministic order; `incoming` exists only for the O(1) lookups
        // `retain` below needs, and is never itself iterated -- a `HashMap`
        // iterated directly would order those spawns by this process's own
        // per-instance hash-key randomization, not by anything about the
        // peers themselves, letting two runs seeded identically schedule
        // this node's writer tasks in a different relative order.
        let incoming_peers: Vec<Peer> = peers
            .into_iter()
            .filter(|peer| peer.node != self.inner.node)
            .collect();
        let incoming: HashMap<NodeId, (SocketAddr, u16)> = incoming_peers
            .iter()
            .map(|peer| (peer.node, (peer.data_addr, peer.protocol)))
            .collect();

        let mut table = self
            .inner
            .peers
            .write()
            .expect("invariant: peers lock is never poisoned");
        table.retain(|node, handle| {
            let Some(&(data_addr, protocol)) = incoming.get(node) else {
                handle.cancel.cancel();
                return false;
            };
            if data_addr != handle.data_addr {
                handle.cancel.cancel();
                return false;
            }
            // The peer is unchanged; its gossiped protocol may still have
            // moved (a rolling upgrade), so it is refreshed regardless.
            handle.protocol.store(protocol, Ordering::Relaxed);
            true
        });
        for peer in incoming_peers {
            table.entry(peer.node).or_insert_with(|| {
                self.spawn_peer_handle(peer.node, peer.data_addr, peer.protocol)
            });
        }
        self.inner
            .admission
            .set_known_sources(table.values().map(|handle| handle.data_addr.ip()));
    }

    fn spawn_peer_handle(&self, peer: NodeId, data_addr: SocketAddr, protocol: u16) -> PeerHandle {
        let invalidate = Arc::new(DropOldestQueue::new(self.inner.outbox_capacity));
        let (replicate_tx, replicate_rx) = mpsc::channel(self.inner.outbox_capacity);
        let cancel = CancellationToken::new();
        let dirty = Arc::new(AtomicBool::new(false));
        tokio::spawn(conn::run_peer_writer(
            conn::PeerTarget {
                peer,
                greeting: self.inner.greeting(),
                addr: data_addr,
                dirty: Arc::clone(&dirty),
                tls: self.inner.tls.clone(),
            },
            Arc::clone(&invalidate),
            replicate_rx,
            cancel.clone(),
        ));
        PeerHandle {
            data_addr,
            invalidate,
            replicate_tx,
            dirty,
            last_replicate_enqueued: Arc::new(AtomicU64::new(0)),
            ae_deferrals: AtomicU32::new(0),
            cancel,
            req_pool: Arc::new(conn::ReqPool::new()),
            protocol: AtomicU16::new(protocol),
        }
    }

    /// Best-effort, non-blocking fan-out of `msg` to `peer` on the outbox
    /// selected by `class`. Overflow is handled per `class`'s drop policy.
    /// A `peer` the mesh doesn't know about is a silent no-op.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub fn send(&self, peer: NodeId, class: MsgClass, msg: Msg) {
        self.send_many(peer, class, std::iter::once(msg));
    }

    /// [`Mesh::send`] for many messages, resolving the peer-table lock
    /// once for the batch. Each message still gets its own overflow
    /// decision. For a multi-peer fan-out sharing content, use
    /// `Mesh::send_frames` instead.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub fn send_many(&self, peer: NodeId, class: MsgClass, msgs: impl IntoIterator<Item = Msg>) {
        self.send_many_enqueued(peer, class, msgs);
    }

    /// [`Mesh::send_many`], answering whether every message reached
    /// `peer`'s outbox: `false` when `peer` is missing from the peer table,
    /// which a failure-detector flap causes as well as a departure, or when
    /// a message failed to encode or met a full outbox. Enqueued is not
    /// delivered: the outbox can still lose a frame to a broken connection.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn send_many_enqueued(
        &self,
        peer: NodeId,
        class: MsgClass,
        msgs: impl IntoIterator<Item = Msg>,
    ) -> bool {
        let mut unencoded = 0usize;
        let frames = msgs.into_iter().filter_map(|msg| match OutFrame::new(msg) {
            Ok(frame) => Some(frame),
            Err(error) => {
                tracing::warn!(%error, "failed to encode outbound message; dropped");
                unencoded += 1;
                None
            }
        });
        let enqueued = self.send_frames(peer, class, frames);
        self.note_unsent(peer, unencoded);
        unencoded == 0 && enqueued
    }

    /// Marks `peer` dirty and counts `frames` in
    /// `sundog_backlog_dropped_total{peer}`: for messages a caller could not
    /// encode, which never reach an outbox. A `peer` the mesh doesn't know
    /// about, and zero `frames`, are no-ops.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn note_unsent(&self, peer: NodeId, frames: usize) {
        if frames == 0 {
            return;
        }
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        if let Some(handle) = table.get(&peer) {
            handle.dirty.store(true, Ordering::Relaxed);
            metrics::counter!("sundog_backlog_dropped_total", "peer" => peer.to_string())
                .increment(u64::try_from(frames).unwrap_or(u64::MAX));
        }
    }

    /// [`Mesh::send_many`], but for already-encoded [`OutFrame`]s, so
    /// broadcast content is encoded once regardless of peer count.
    /// Otherwise identical to [`Mesh::send_many`]. Answers whether every
    /// frame reached `peer`'s outbox, as [`Mesh::send_many_enqueued`] does.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn send_frames(
        &self,
        peer: NodeId,
        class: MsgClass,
        frames: impl IntoIterator<Item = OutFrame>,
    ) -> bool {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        let Some(handle) = table.get(&peer) else {
            return false;
        };
        let mut enqueued_replicate = false;
        let mut enqueued_all = true;
        for frame in frames {
            match class {
                MsgClass::Invalidate => {
                    // The new frame is queued; the oldest one it displaced is lost.
                    if handle.invalidate.push(frame) {
                        handle.dirty.store(true, Ordering::Relaxed);
                        metrics::counter!(
                            "sundog_backlog_dropped_total",
                            "peer" => peer.to_string()
                        )
                        .increment(1);
                    }
                }
                MsgClass::Replicate => {
                    enqueued_replicate = true;
                    if let Err(mpsc::error::TrySendError::Full(_)) =
                        handle.replicate_tx.try_send(frame)
                    {
                        enqueued_all = false;
                        handle.dirty.store(true, Ordering::Relaxed);
                        metrics::counter!(
                            "sundog_backlog_dropped_total",
                            "peer" => peer.to_string()
                        )
                        .increment(1);
                    }
                }
            }
        }
        if enqueued_replicate {
            handle
                .last_replicate_enqueued
                .store(mono_ms() + 1, Ordering::Relaxed);
        }
        enqueued_all
    }

    /// [`Mesh::send_frames`] for records the sender keeps no copy of: a
    /// `Mode::Distributed` fan-out, where a forwarded write's only copy is
    /// the frame itself, so a frame for a peer still live in the peer table
    /// is never dropped. Waits for outbox space in
    /// [`FAN_OUT_SEND_DEADLINE`] slices, logging and counting each timed-out
    /// one in `sundog_fan_out_wait_seconds_total{peer}`. Once `peer` leaves
    /// the table or its outbox closes, remaining frames are dropped,
    /// counted in `sundog_backlog_dropped_total`, and the peer marked
    /// dirty. An unknown `peer` is a silent no-op.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) async fn send_frames_awaiting(&self, peer: NodeId, frames: Vec<OutFrame>) {
        let (tx, dirty, enqueued_stamp) = {
            let table = self
                .inner
                .peers
                .read()
                .expect("invariant: peers lock is never poisoned");
            let Some(handle) = table.get(&peer) else {
                return;
            };
            (
                handle.replicate_tx.clone(),
                Arc::clone(&handle.dirty),
                Arc::clone(&handle.last_replicate_enqueued),
            )
        };
        let mut queue: VecDeque<OutFrame> = frames.into();
        let mut dropped = 0u64;
        while let Some(frame) = queue.pop_front() {
            let slice_deadline = tokio::time::Instant::now() + FAN_OUT_SEND_DEADLINE;
            match tokio::time::timeout_at(slice_deadline, tx.reserve()).await {
                Ok(Ok(permit)) => permit.send(frame),
                Ok(Err(_)) => {
                    // The channel closed: this peer's writer already ended.
                    dropped += 1 + u64::try_from(queue.len()).unwrap_or(u64::MAX);
                    queue.clear();
                }
                Err(_) => {
                    if fan_out_keep_waiting(self.peer_is_live(peer)) {
                        metrics::counter!(
                            "sundog_fan_out_wait_seconds_total",
                            "peer" => peer.to_string()
                        )
                        .increment(FAN_OUT_SEND_DEADLINE.as_secs());
                        tracing::debug!(
                            %peer,
                            "outbox still full past one fan-out wait slice; peer is still \
                             live, waiting again"
                        );
                        queue.push_front(frame);
                    } else {
                        dropped += 1 + u64::try_from(queue.len()).unwrap_or(u64::MAX);
                        queue.clear();
                    }
                }
            }
        }
        enqueued_stamp.store(mono_ms() + 1, Ordering::Relaxed);
        if dropped > 0 {
            dirty.store(true, Ordering::Relaxed);
            metrics::counter!("sundog_backlog_dropped_total", "peer" => peer.to_string())
                .increment(dropped);
            tracing::warn!(%peer, dropped, "outbox stayed full past the fan-out deadline; frames dropped");
        }
    }

    /// Whether `peer` is still in the peer table:
    /// [`Mesh::send_frames_awaiting`]'s liveness check between wait slices,
    /// and what decides whether the inbound loop's re-forward lane toward
    /// `peer` is opened or retired.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn peer_is_live(&self, peer: NodeId) -> bool {
        self.inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned")
            .contains_key(&peer)
    }

    /// Waits, up to `deadline`, until every peer's replicate and invalidate
    /// outboxes have been handed to its writer ([`outboxes_pending`]), then
    /// a moment more for the writer's last frames to leave: what
    /// [`Mesh::shutdown`] runs before cancelling the writers, so a frame
    /// accepted right before shutdown still goes out.
    async fn flush_outboxes(&self, deadline: tokio::time::Instant) {
        loop {
            let pending = {
                let table = self
                    .inner
                    .peers
                    .read()
                    .expect("invariant: peers lock is never poisoned");
                table.values().any(PeerHandle::outboxes_pending)
            };
            if !pending || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(OUTBOX_FLUSH_TAIL).await;
    }

    /// Whether replicate traffic toward `peer` is still in motion: frames
    /// queued in its outbox, or a frame enqueued within the last `window`.
    /// Anti-entropy skips a round against such a peer.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn replicate_in_flight(&self, peer: NodeId, window: Duration) -> bool {
        self.inner.replicate_in_flight(peer, window)
    }

    /// Marks `peer` dirty for the next [`Mesh::take_dirty_peers`], for
    /// when the scheduler skips a round it already took the mark for.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) fn mark_dirty(&self, peer: NodeId) {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        if let Some(handle) = table.get(&peer) {
            handle.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// Returns every peer marked dirty since the last call, clearing their
    /// mark: a peer whose outbox dropped a message, or whose frames a writer
    /// could not encode, and, once its redial succeeds, a peer whose
    /// connection broke after carrying frames or while frames waited to be
    /// resent. The next anti-entropy round targets a dirty peer first.
    /// That repairs a `Replicated` write and an owner-to-owner write; a
    /// `Distributed` non-owner's forwarded write is held by no replica, so
    /// only the writer's resend protects it.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
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

    /// Shuts down the mesh: stops accepting, waits up to half a second for
    /// the replicate and invalidate frames queued for each peer to reach its
    /// writer, then cancels every per-peer writer. A frame still queued then
    /// is discarded, and a `warn` per peer logs how many. Signals the
    /// spawned background work to stop without waiting on sockets.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub async fn shutdown(self) {
        self.inner.accept_cancel.cancel();
        self.flush_outboxes(tokio::time::Instant::now() + OUTBOX_FLUSH_DEADLINE)
            .await;
        let table = std::mem::take(
            &mut *self
                .inner
                .peers
                .write()
                .expect("invariant: peers lock is never poisoned"),
        );
        for (peer, handle) in table {
            let discarded = handle.queued_frames();
            if discarded > 0 {
                tracing::warn!(
                    %peer,
                    frames = discarded,
                    "shutdown discards the frames still queued for the peer"
                );
            }
            handle.cancel.cancel();
        }
        // Yield once so cancelled tasks observe the token before this handle
        // drops.
        tokio::task::yield_now().await;
    }
}
