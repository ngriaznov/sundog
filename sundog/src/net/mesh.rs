//! The broadcast side of [`Mesh`].

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    BoundListener, DEFAULT_CHANNEL_CAPACITY, Greeting, InboundMsg, Mesh, MeshInner, MsgClass,
    NoRoom, OUTBOX_FLUSH_DEADLINE, OUTBOX_FLUSH_TAIL, OutFrame, OutboxFull, OutboxPermit, OutboxTx,
    PeerHandle, RequestHandler, RoomWait, SendBudget, SendEnd, build_tls_ctx, conn,
    fan_out_no_room, fan_out_room_wait, idle_deadline, latch_mark, mono_ms, replicate_outbox,
    stall_latch, taken_since_latch, waits_out_latch,
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
            outbox_bytes: config.outbox_bytes,
            peers: RwLock::new(HashMap::new()),
            accept_cancel: CancellationToken::new(),
            tls,
            keepalive: config.mesh_keepalive,
            admission: conn::Admission::new(),
            acked_buckets: RwLock::new(HashMap::new()),
            ack_window: config.rebalance_ack_window_value(),
            ack_swept_at: Mutex::new(Instant::now()),
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
    /// departed or changed `data_addr`, and drops the bucket acks of every
    /// requester that is no longer a peer. The IPs of the peers' data
    /// addresses are the sources the cap on connections waiting for their
    /// `Hello` never refuses. Called on every
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
        // The table now holds exactly `incoming`'s peers. The ack sweep runs
        // after the peer-table lock is released: it walks every ack row, and
        // the fan-out paths read that table.
        drop(table);
        self.inner.forget_acks_of_departed(&incoming);
    }

    fn spawn_peer_handle(&self, peer: NodeId, data_addr: SocketAddr, protocol: u16) -> PeerHandle {
        let invalidate = Arc::new(DropOldestQueue::new(
            self.inner.outbox_capacity,
            self.inner.outbox_bytes,
        ));
        let (replicate_tx, replicate_rx) =
            replicate_outbox(self.inner.outbox_capacity, self.inner.outbox_bytes);
        let cancel = CancellationToken::new();
        let dirty = Arc::new(AtomicBool::new(false));
        tokio::spawn(conn::run_peer_writer(
            conn::PeerTarget {
                peer,
                greeting: self.inner.greeting(),
                addr: data_addr,
                dirty: Arc::clone(&dirty),
                tls: self.inner.tls.clone(),
                keepalive: self.inner.keepalive,
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
            stalled: Arc::new(AtomicU64::new(0)),
            pull_limits: Mutex::new(HashMap::new()),
            pull_warned: Mutex::new(HashSet::new()),
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
                    // The new frame is queued; the oldest ones it displaced are lost.
                    let evicted = handle.invalidate.push(frame);
                    if evicted > 0 {
                        handle.dirty.store(true, Ordering::Relaxed);
                        metrics::counter!(
                            "sundog_backlog_dropped_total",
                            "peer" => peer.to_string()
                        )
                        .increment(evicted);
                    }
                }
                MsgClass::Replicate => {
                    enqueued_replicate = true;
                    if let Err(OutboxFull::Full) = handle.replicate_tx.try_send(frame) {
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

    /// [`Mesh::send_frames_within`] with no budget: waits for as long as
    /// `peer` stays in the peer table, for a send whose frames have no
    /// other copy and no bound of their own, as the inbound loop's
    /// re-forward lanes do.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) async fn send_frames_awaiting(&self, peer: NodeId, frames: Vec<OutFrame>) {
        self.send_frames_within(peer, frames, SendBudget::Unbounded)
            .await;
    }

    /// [`Mesh::send_frames`] for records the sender keeps no copy of, a
    /// `Mode::Distributed` fan-out's, which waits for outbox space instead
    /// of dropping the newest: in [`FAN_OUT_SEND_DEADLINE`] slices, the
    /// seconds of each one that finds no room counted in
    /// `sundog_fan_out_wait_seconds_total{peer}`, while `peer` stays in the
    /// peer table and its writer takes a frame from the outbox within
    /// `budget`'s idle time ([`idle_deadline`], renewed by every frame the
    /// writer takes and every frame placed). A frame keeps its place in the
    /// queue for the outbox's bytes from one slice to the next, so a heavy
    /// frame is not passed over by lighter ones. Once `peer` leaves the
    /// table, or its outbox closes or takes nothing for that long, the frames
    /// not yet placed are dropped, counted in
    /// `sundog_backlog_dropped_total{peer}`, and the peer marked dirty. A
    /// bounded send whose peer took nothing latches the peer stalled
    /// ([`stall_latch`]), counted once in `sundog_fan_out_stalls_total`:
    /// every bounded send to it then takes the room that is free, without
    /// waiting, until its writer takes a frame again ([`waits_out_latch`]),
    /// and the latch clears, marking the peer dirty again, once a send then
    /// places all its frames. Returns how many of `frames`, from the front,
    /// went in; an unknown `peer` takes none.
    ///
    /// [`FAN_OUT_SEND_DEADLINE`]: super::FAN_OUT_SEND_DEADLINE
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock is poisoned.
    pub(crate) async fn send_frames_within(
        &self,
        peer: NodeId,
        frames: Vec<OutFrame>,
        budget: SendBudget,
    ) -> usize {
        if frames.is_empty() {
            return 0;
        }
        let (tx, dirty, enqueued_stamp, stalled) = {
            let table = self
                .inner
                .peers
                .read()
                .expect("invariant: peers lock is never poisoned");
            let Some(handle) = table.get(&peer) else {
                return 0;
            };
            (
                handle.replicate_tx.clone(),
                Arc::clone(&handle.dirty),
                Arc::clone(&handle.last_replicate_enqueued),
                Arc::clone(&handle.stalled),
            )
        };
        let mut idle = IdleWait {
            budget,
            deadline: idle_deadline(budget, tokio::time::Instant::now()),
            last_taken: tx.taken(),
        };
        let bounded = idle.deadline.is_some();
        let mut queue: VecDeque<OutFrame> = frames.into();
        let mut placed = 0usize;
        let mut dropped = 0u64;
        let mut end = SendEnd::Placed;
        while let Some(frame) = queue.pop_front() {
            let rest = 1 + u64::try_from(queue.len()).unwrap_or(u64::MAX);
            match self
                .room_for(peer, &tx, frame.resident_bytes(), &mut idle, &stalled)
                .await
            {
                Ok(permit) => {
                    permit.send(frame);
                    placed += 1;
                    idle.renew(tokio::time::Instant::now());
                }
                Err(outcome) => {
                    dropped += rest;
                    queue.clear();
                    end = if outcome == NoRoom::Stalled {
                        SendEnd::Stalled
                    } else {
                        SendEnd::Departed
                    };
                }
            }
        }
        enqueued_stamp.store(mono_ms() + 1, Ordering::Relaxed);
        if dropped > 0 {
            dirty.store(true, Ordering::Relaxed);
            metrics::counter!("sundog_backlog_dropped_total", "peer" => peer.to_string())
                .increment(dropped);
        }
        if bounded {
            settle_stall_latch(peer, end, dropped, tx.taken(), &stalled, &dirty);
        } else if dropped > 0 {
            tracing::warn!(%peer, dropped, "a departed peer's fan-out frames dropped");
        }
        placed
    }

    /// Waits for room for a frame of `len` bytes in `peer`'s replicate outbox
    /// `tx`, in [`FAN_OUT_SEND_DEADLINE`] slices, through one reservation, so
    /// the frame keeps its place in the queue for the outbox's bytes from one
    /// slice to the next. Renews `idle` whenever the writer has taken a frame,
    /// counts each slice that found no room in
    /// `sundog_fan_out_wait_seconds_total{peer}`, and answers the room or why
    /// the send ends ([`fan_out_no_room`]).
    ///
    /// [`FAN_OUT_SEND_DEADLINE`]: super::FAN_OUT_SEND_DEADLINE
    async fn room_for<'t>(
        &self,
        peer: NodeId,
        tx: &'t OutboxTx,
        len: usize,
        idle: &mut IdleWait,
        stalled: &AtomicU64,
    ) -> Result<OutboxPermit<'t>, NoRoom> {
        let latched = |taken: u64| waits_out_latch(stalled.load(Ordering::Relaxed), taken);
        let reserve = tx.reserve(len);
        tokio::pin!(reserve);
        loop {
            let started = tokio::time::Instant::now();
            let wait = fan_out_room_wait(idle.deadline, latched(tx.taken()), started);
            let reserved = match wait {
                RoomWait::TryOnly => tx.try_reserve(len),
                RoomWait::Until(until) => tokio::time::timeout_at(until, &mut reserve)
                    .await
                    .unwrap_or(Err(OutboxFull::Full)),
            };
            match reserved {
                Ok(permit) => return Ok(permit),
                // This peer's writer already ended.
                Err(OutboxFull::Closed) => return Err(NoRoom::Departed),
                Err(OutboxFull::Full) => {}
            }
            let now = tokio::time::Instant::now();
            let taken = tx.taken();
            idle.note_taken(taken, now);
            let outcome = fan_out_no_room(
                self.peer_is_live(peer),
                latched(taken),
                wait,
                idle.deadline,
                now,
            );
            let waited = now.saturating_duration_since(started).as_secs();
            if waited > 0 {
                metrics::counter!(
                    "sundog_fan_out_wait_seconds_total",
                    "peer" => peer.to_string()
                )
                .increment(waited);
            }
            if outcome != NoRoom::WaitAgain {
                return Err(outcome);
            }
            tracing::debug!(
                %peer,
                "outbox still full past one fan-out wait slice; peer is still live, waiting \
                 again"
            );
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
    /// outboxes have been handed to its writer ([`super::outboxes_pending`]), then
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

    /// The most keys or hashes an anti-entropy pull from `peer` asks for in
    /// `cache`, once a pull to it timed out and shrank the batch; `None` for
    /// a peer that never shrank and for one the mesh does not know. The
    /// limit lives with the peer's handle, so it goes when
    /// [`Mesh::update_peers`] drops the peer.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock or the peer's limit lock is poisoned.
    pub(crate) fn pull_limit(&self, peer: NodeId, cache: &str) -> Option<usize> {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        let limits = table
            .get(&peer)?
            .pull_limits
            .lock()
            .expect("invariant: pull limit lock is never poisoned");
        limits.get(cache).copied()
    }

    /// Records `limit` as the most an anti-entropy pull from `peer` asks for
    /// in `cache`. A peer the mesh does not know keeps nothing.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock or the peer's limit lock is poisoned.
    pub(crate) fn set_pull_limit(&self, peer: NodeId, cache: &str, limit: usize) {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        if let Some(handle) = table.get(&peer) {
            handle
                .pull_limits
                .lock()
                .expect("invariant: pull limit lock is never poisoned")
                .insert(SmolStr::new(cache), limit);
        }
    }

    /// Whether this is the first time `cache`'s pull limit to `peer` reached
    /// one key: `true` once for each peer and cache, `false` after, and
    /// `false` for a peer the mesh does not know. The mark lives with the
    /// peer's handle, so a peer that left and came back warns again.
    ///
    /// # Panics
    ///
    /// Panics if the peer-table lock or the peer's warning lock is poisoned.
    pub(crate) fn first_one_key_warning(&self, peer: NodeId, cache: &str) -> bool {
        let table = self
            .inner
            .peers
            .read()
            .expect("invariant: peers lock is never poisoned");
        table.get(&peer).is_some_and(|handle| {
            handle
                .pull_warned
                .lock()
                .expect("invariant: pull warning lock is never poisoned")
                .insert(SmolStr::new(cache))
        })
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
    /// is discarded, and a `warn` per peer logs how many. A burst a writer
    /// has already taken, in flight or held for a redial, is outside both the
    /// wait and the count. Signals the spawned background work to stop
    /// without waiting on sockets.
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

/// The idle time a bounded [`Mesh::send_frames_within`] waits by: its
/// deadline ([`idle_deadline`] of `budget`), renewed whenever the peer's
/// writer has taken a frame since `last_taken` and whenever the send places
/// one.
struct IdleWait {
    budget: SendBudget,
    deadline: Option<tokio::time::Instant>,
    last_taken: u64,
}

impl IdleWait {
    fn renew(&mut self, now: tokio::time::Instant) {
        self.deadline = idle_deadline(self.budget, now);
    }

    /// Renews the deadline if the writer has taken `taken` frames, more
    /// than when last noted: the peer reads.
    fn note_taken(&mut self, taken: u64, now: tokio::time::Instant) {
        if taken != self.last_taken {
            self.last_taken = taken;
            self.renew(now);
        }
    }
}

/// Moves `peer`'s stall latch (`stalled`) as a bounded send that ended at
/// `end`, having dropped `dropped` frames, sets it ([`stall_latch`]), its
/// writer having taken `taken` frames so far: sets it to [`latch_mark`] of
/// `taken`, counting a newly set latch in `sundog_fan_out_stalls_total` with
/// a warning, and clears it, marking the peer `dirty` again, once the writer
/// has taken a frame since it was set ([`taken_since_latch`]) and the send
/// placed every frame.
pub(super) fn settle_stall_latch(
    peer: NodeId,
    end: SendEnd,
    dropped: u64,
    taken: u64,
    stalled: &AtomicU64,
    dirty: &AtomicBool,
) {
    let latched_at = stalled.load(Ordering::Relaxed);
    match stall_latch(end, taken_since_latch(latched_at, taken)) {
        Some(true) => {
            if stalled.swap(latch_mark(taken), Ordering::Relaxed) == 0 {
                metrics::counter!("sundog_fan_out_stalls_total").increment(1);
                tracing::warn!(
                    %peer,
                    dropped,
                    "a peer live in gossip took no fan-out frame for fan_out_stall_timeout; \
                     dropped the rest and marked it dirty, and sends to it no longer wait until \
                     it reads again"
                );
            } else {
                tracing::debug!(%peer, dropped, "fan-out frames dropped for a stalled peer");
            }
        }
        Some(false) => {
            if stalled
                .compare_exchange(latched_at, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                dirty.store(true, Ordering::Relaxed);
                tracing::info!(%peer, "a stalled peer reads fan-out frames again");
            }
        }
        None if end == SendEnd::Departed => {
            tracing::warn!(%peer, dropped, "a departed peer's fan-out frames dropped");
        }
        None => {}
    }
}
