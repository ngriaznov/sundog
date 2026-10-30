//! Tests for the per-peer writer: the frames it holds across a failed send,
//! the read arm, the paced redial and the dirty mark.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::Bytes;
use smol_str::SmolStr;
use tokio::time::Instant;

use super::tests::{out_frame, record};
use super::writer::{
    RECONNECT_BACKOFF, RecentTail, dirty_mark_due, mark_deadline, owes_repair, reconnect_delay,
};
use crate::node::NodeId;
use crate::wire::Msg;

fn replicate(n: u8) -> Msg {
    Msg::Replicate {
        cache: SmolStr::new("users"),
        rec: record(n),
    }
}

fn frame_of(n: u8) -> Bytes {
    out_frame(replicate(n)).frame
}

#[test]
fn reconnect_delay_is_the_backoff_minus_the_connection_lifetime() {
    assert_eq!(reconnect_delay(Duration::ZERO), RECONNECT_BACKOFF);
    assert_eq!(
        reconnect_delay(Duration::from_millis(200)),
        Duration::from_millis(300)
    );
    assert_eq!(reconnect_delay(RECONNECT_BACKOFF), Duration::ZERO);
    assert_eq!(reconnect_delay(Duration::from_secs(5)), Duration::ZERO);
}

#[test]
fn owes_repair_follows_held_frames_and_a_connection_that_carried_frames_and_lasted() {
    let short = Duration::from_millis(100);
    let long = RECONNECT_BACKOFF;
    assert!(
        !owes_repair(false, false, long),
        "an idle connection that closes loses nothing"
    );
    assert!(
        owes_repair(true, false, long),
        "a lasting connection that carried frames may have lost some"
    );
    assert!(
        !owes_repair(true, false, short),
        "a peer that accepts and closes at once does not steer anti-entropy"
    );
    assert!(owes_repair(false, true, short), "held frames are owed");
    assert!(owes_repair(true, true, long));
}

#[test]
fn dirty_marks_are_spaced() {
    let start = Instant::now();
    assert!(dirty_mark_due(None, start), "the first mark is due");
    assert!(!dirty_mark_due(Some(start), start));
    assert!(!dirty_mark_due(
        Some(start),
        start + Duration::from_millis(4_999)
    ));
    assert!(dirty_mark_due(Some(start), start + Duration::from_secs(5)));
}

#[test]
fn an_owed_mark_is_due_at_the_end_of_the_spacing_and_never_dropped() {
    let start = Instant::now();
    assert_eq!(mark_deadline(false, None, start), None, "nothing is owed");
    assert_eq!(
        mark_deadline(false, Some(start), start),
        None,
        "nothing is owed after a mark either"
    );
    assert_eq!(
        mark_deadline(true, None, start),
        Some(start),
        "the first mark is due at once"
    );
    let later = start + Duration::from_secs(2);
    assert_eq!(
        mark_deadline(true, Some(start), later),
        Some(start + Duration::from_secs(5)),
        "a mark the spacing defers is due when the spacing ends"
    );
    let much_later = start + Duration::from_secs(60);
    let deadline = mark_deadline(true, Some(start), much_later).expect("owed");
    assert!(
        dirty_mark_due(Some(start), deadline),
        "the deadline is an instant at which the mark is allowed"
    );
}

#[test]
fn the_recent_tail_keeps_frames_within_the_window_only() {
    let start = Instant::now();
    let mut tail = RecentTail::default();
    tail.record(start, &[frame_of(1), frame_of(2)]);
    tail.record(start + Duration::from_millis(600), &[frame_of(3)]);
    assert_eq!(tail.len(), 3);
    let taken = tail.take(start + Duration::from_millis(1_200));
    assert_eq!(
        taken,
        vec![frame_of(3)],
        "the first two are older than the one-second window"
    );
    assert_eq!(tail.len(), 0, "taking empties the tail");
    assert_eq!(
        tail.take(start + Duration::from_millis(1_300)).len(),
        0,
        "nothing is left to take"
    );
}

#[test]
fn the_recent_tail_drops_its_oldest_frames_past_the_byte_cap() {
    let start = Instant::now();
    let mut tail = RecentTail::default();
    let big = |n: u8| Bytes::from(vec![n; 400 * 1024]);
    tail.record(start, &[big(1), big(2)]);
    assert_eq!(tail.len(), 2, "800 KiB fits under the 1 MiB cap");
    tail.record(start, &[big(3)]);
    assert_eq!(
        tail.take(start),
        vec![big(2), big(3)],
        "the oldest frame goes first once 1200 KiB exceeds the cap"
    );
}

mod scripted {
    //! `drive_connection` over a scripted in-memory link.

    use std::collections::VecDeque;
    use std::io;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll};

    use bytes::BytesMut;
    use futures::{Sink, Stream};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::super::writer::{LinkEnd, WriterState, coalesce_replicate, drive_connection};
    use super::*;
    use crate::net::OutFrame;
    use crate::net::outbox::DropOldestQueue;

    /// A link that records what it is sent, can fail or stall a flush, and
    /// yields scripted inbound events.
    #[derive(Default)]
    struct ScriptedLink {
        staged: Vec<Bytes>,
        accepted: Arc<Mutex<Vec<Bytes>>>,
        flushes: u32,
        fail_flush_at: Option<u32>,
        stall_flush: bool,
        inbound: VecDeque<Option<BytesMut>>,
    }

    impl ScriptedLink {
        fn accepted(&self) -> Vec<Bytes> {
            self.accepted.lock().expect("lock").clone()
        }
    }

    impl Stream for ScriptedLink {
        type Item = io::Result<BytesMut>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.inbound.pop_front() {
                Some(Some(frame)) => Poll::Ready(Some(Ok(frame))),
                Some(None) => Poll::Ready(None),
                None => Poll::Pending,
            }
        }
    }

    impl Sink<Bytes> for ScriptedLink {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> io::Result<()> {
            self.staged.push(item);
            Ok(())
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.stall_flush {
                return Poll::Pending;
            }
            self.flushes += 1;
            if self.fail_flush_at == Some(self.flushes) {
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
            }
            let staged = std::mem::take(&mut self.staged);
            self.accepted.lock().expect("lock").extend(staged);
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct Rig {
        state: WriterState,
        dirty: Arc<AtomicBool>,
        invalidate: Arc<DropOldestQueue<OutFrame>>,
        replicate_tx: mpsc::Sender<OutFrame>,
        replicate_rx: mpsc::Receiver<OutFrame>,
        cancel: CancellationToken,
    }

    impl Rig {
        fn new() -> Self {
            let (replicate_tx, replicate_rx) = mpsc::channel(16);
            let dirty = Arc::new(AtomicBool::new(false));
            Self {
                state: WriterState::new(NodeId::from(2), Arc::clone(&dirty)),
                dirty,
                invalidate: Arc::new(DropOldestQueue::new(16)),
                replicate_tx,
                replicate_rx,
                cancel: CancellationToken::new(),
            }
        }

        async fn drive(&mut self, link: &mut ScriptedLink) -> LinkEnd {
            drive_connection(
                link,
                &mut self.state,
                &self.invalidate,
                &mut self.replicate_rx,
                &self.cancel,
            )
            .await
        }

        /// Drives `link` for a second of paused time, which ends only if
        /// the link ends.
        async fn drive_for_a_second(&mut self, link: &mut ScriptedLink) -> Option<LinkEnd> {
            tokio::time::timeout(Duration::from_secs(1), self.drive(link))
                .await
                .ok()
        }

        async fn queue_replicate(&self, n: u8) {
            self.replicate_tx
                .send(out_frame(replicate(n)))
                .await
                .expect("receiver alive");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn drive_connection_keeps_a_burst_whose_flush_failed_and_reports_closed() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink {
            fail_flush_at: Some(1),
            ..ScriptedLink::default()
        };
        for n in 1..=3 {
            rig.queue_replicate(n).await;
        }
        let expected = coalesce_replicate((1..=3).map(|n| out_frame(replicate(n))).collect());

        assert_eq!(rig.drive(&mut link).await, LinkEnd::Closed);

        assert_eq!(rig.state.held, expected, "the coalesced burst stays held");
        assert!(!rig.state.sent_any, "nothing reached the link");
        assert_eq!(link.accepted().len(), 0, "nothing reached the link");
    }

    #[tokio::test(start_paused = true)]
    async fn drive_connection_resends_held_frames_first_on_the_next_link() {
        let mut rig = Rig::new();
        let mut broken = ScriptedLink {
            fail_flush_at: Some(1),
            ..ScriptedLink::default()
        };
        rig.queue_replicate(1).await;
        assert_eq!(rig.drive(&mut broken).await, LinkEnd::Closed);
        rig.queue_replicate(2).await;

        let mut next = ScriptedLink::default();
        assert_eq!(
            rig.drive_for_a_second(&mut next).await,
            None,
            "a healthy link keeps running"
        );

        assert_eq!(
            next.accepted(),
            vec![frame_of(1), frame_of(2)],
            "the held frame goes out before the one queued later"
        );
        assert_eq!(rig.state.held.len(), 0, "the burst went out");
        assert!(rig.state.sent_any);
    }

    #[tokio::test(start_paused = true)]
    async fn drive_connection_holds_an_invalidate_burst_the_same_way() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink {
            fail_flush_at: Some(1),
            ..ScriptedLink::default()
        };
        let invalidate = |n: u8| Msg::Invalidate {
            cache: SmolStr::new("users"),
            key: Bytes::from(vec![n]),
            ver: record(n).ver,
        };
        rig.invalidate.push(out_frame(invalidate(1)));
        rig.invalidate.push(out_frame(invalidate(2)));

        assert_eq!(rig.drive(&mut link).await, LinkEnd::Closed);

        assert_eq!(
            rig.state.held,
            vec![
                out_frame(invalidate(1)).frame,
                out_frame(invalidate(2)).frame
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drive_connection_ends_on_peer_eof_without_draining_the_queues() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink {
            inbound: VecDeque::from([None]),
            ..ScriptedLink::default()
        };
        rig.queue_replicate(1).await;

        assert_eq!(rig.drive(&mut link).await, LinkEnd::Closed);

        assert!(
            rig.state.held.is_empty(),
            "nothing was drained into the dead link"
        );
        assert!(
            rig.replicate_rx.try_recv().is_ok(),
            "the queued frame is still in its channel for the next connection"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drive_connection_closes_on_a_frame_a_broadcast_link_never_carries() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink {
            inbound: VecDeque::from([Some(BytesMut::from(&b"unexpected"[..]))]),
            ..ScriptedLink::default()
        };
        rig.queue_replicate(1).await;

        assert_eq!(rig.drive(&mut link).await, LinkEnd::Closed);

        assert_eq!(link.accepted().len(), 0, "nothing reached the link");
        assert!(
            rig.replicate_rx.try_recv().is_ok(),
            "the queue is untouched"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drive_connection_returns_peer_gone_when_the_replicate_sender_drops() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink::default();
        let Rig { replicate_tx, .. } = &mut rig;
        // The sender is the rig's only one: dropping a clone is not enough.
        drop(std::mem::replace(replicate_tx, mpsc::channel(1).0));
        assert_eq!(rig.drive(&mut link).await, LinkEnd::PeerGone);
    }

    #[tokio::test(start_paused = true)]
    async fn drive_connection_returns_cancelled_on_cancel() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink::default();
        rig.cancel.cancel();
        assert_eq!(rig.drive(&mut link).await, LinkEnd::Cancelled);
    }

    #[tokio::test(start_paused = true)]
    async fn drive_connection_returns_soon_after_a_cancel_during_a_stalled_flush() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink {
            stall_flush: true,
            ..ScriptedLink::default()
        };
        rig.queue_replicate(1).await;
        let cancel = rig.cancel.clone();
        let canceller = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel.cancel();
        });
        let started = Instant::now();

        let end = tokio::time::timeout(Duration::from_secs(30), rig.drive(&mut link))
            .await
            .expect("a stalled write does not outlast the cancel");

        canceller.await.expect("canceller ran");
        assert_eq!(end, LinkEnd::Cancelled);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_written_burst_is_resent_from_the_recent_tail_after_the_link_closes() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink::default();
        rig.queue_replicate(1).await;
        assert_eq!(rig.drive_for_a_second(&mut link).await, None);
        assert_eq!(link.accepted(), vec![frame_of(1)]);
        assert!(rig.state.held.is_empty(), "the burst went out");

        // The peer's FIN reached us after the write: the tail is held again.
        rig.state.requeue_recent(Instant::now());
        assert_eq!(rig.state.held, vec![frame_of(1)]);

        let mut next = ScriptedLink::default();
        assert_eq!(rig.drive_for_a_second(&mut next).await, None);
        assert_eq!(
            next.accepted(),
            vec![frame_of(1)],
            "resent on the next link"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_mark_the_spacing_deferred_is_set_on_the_live_connection_once_the_spacing_ends() {
        let mut rig = Rig::new();
        let start = Instant::now();
        // A mark was set 1 s ago, and a connection that ended since owes one.
        rig.state.last_mark = Some(start - Duration::from_secs(1));
        rig.state.sent_any = true;
        rig.state.note_connection_end(RECONNECT_BACKOFF);
        assert!(rig.state.owes_mark);
        rig.state.apply_mark_if_due(start);
        assert!(
            !rig.dirty.load(Ordering::Relaxed),
            "the redial lands inside the spacing, so the mark waits"
        );
        assert!(rig.state.owes_mark, "the debt stays");

        // The next connection stays up and idle.
        let mut link = ScriptedLink::default();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rig.drive(&mut link))
                .await
                .ok(),
            None
        );
        assert!(
            !rig.dirty.load(Ordering::Relaxed),
            "4 s after the last mark is inside the spacing"
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), rig.drive(&mut link))
                .await
                .ok(),
            None
        );
        assert!(
            rig.dirty.load(Ordering::Relaxed),
            "the connection that lives on sets the owed mark once 5 s have passed"
        );
        assert!(!rig.state.owes_mark, "the debt is paid");
        assert_eq!(
            rig.state.last_mark.map(|at| at.duration_since(start)),
            Some(Duration::from_secs(4)),
            "the mark is stamped when the spacing ended"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_connection_that_owes_nothing_sets_no_mark() {
        let mut rig = Rig::new();
        let mut link = ScriptedLink::default();
        assert_eq!(rig.drive_for_a_second(&mut link).await, None);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(rig.drive_for_a_second(&mut link).await, None);
        assert!(!rig.dirty.load(Ordering::Relaxed));
        assert!(rig.state.last_mark.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn requeue_recent_puts_the_older_tail_ahead_of_the_held_burst() {
        let mut state = WriterState::new(NodeId::from(2), Arc::new(AtomicBool::new(false)));
        let now = Instant::now();
        state.recent.record(now, &[frame_of(1), frame_of(2)]);
        state.held = vec![frame_of(3)];

        state.requeue_recent(now);

        assert_eq!(state.held, vec![frame_of(1), frame_of(2), frame_of(3)]);
        assert_eq!(state.recent.len(), 0);
    }
}

#[cfg(not(feature = "sim"))]
mod sockets {
    //! `run_peer_writer` against a raw listener.

    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::StreamExt as _;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tokio_util::codec::{Framed, LengthDelimitedCodec};
    use tokio_util::sync::CancellationToken;

    use super::super::tests::no_tls;
    use super::*;
    use crate::net::outbox::DropOldestQueue;
    use crate::net::{Greeting, OutFrame};
    use crate::wire::{self, MAX_FRAME};

    type Conn = Framed<TcpStream, LengthDelimitedCodec>;

    struct Writer {
        listener: TcpListener,
        replicate_tx: mpsc::Sender<OutFrame>,
        dirty: Arc<AtomicBool>,
        cancel: CancellationToken,
        handle: JoinHandle<()>,
    }

    /// A loopback listener on a port below every default ephemeral range
    /// (Linux starts at 32768, macOS and Windows at 49152), so no socket
    /// bound to port 0 is handed the port while the listener is closed. The
    /// port is the first in 20000..30000, from an offset derived from the
    /// process id, that binds.
    async fn low_port_listener() -> TcpListener {
        const FIRST: u16 = 20_000;
        const SPAN: u16 = 10_000;
        let offset = u16::try_from(std::process::id() % u32::from(SPAN)).expect("below SPAN");
        for step in 0..SPAN {
            let port = FIRST + (offset + step) % SPAN;
            if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)).await {
                return listener;
            }
        }
        panic!("a free port in 20000..30000");
    }

    /// Binds `addr` again once the listener that held it is dropped. The port
    /// is one [`low_port_listener`] returned: below every ephemeral range, so
    /// the OS hands it to no socket bound to port 0 while it is free. A
    /// listener another test process picked from the same range can still hold
    /// it for a moment, so the bind retries every 10 ms for up to 5 s.
    async fn rebind(addr: std::net::SocketAddr) -> TcpListener {
        const RETRY_EVERY: Duration = Duration::from_millis(10);
        const GIVE_UP_AFTER: Duration = Duration::from_secs(5);
        let started = Instant::now();
        loop {
            match TcpListener::bind(addr).await {
                Ok(listener) => return listener,
                Err(err) if started.elapsed() >= GIVE_UP_AFTER => {
                    panic!("{addr} was not free again within {GIVE_UP_AFTER:?}: {err}");
                }
                Err(_) => tokio::time::sleep(RETRY_EVERY).await,
            }
        }
    }

    impl Writer {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback");
            Self::start_on(listener)
        }

        /// Starts a writer that dials `listener`.
        fn start_on(listener: TcpListener) -> Self {
            let addr = listener.local_addr().expect("listener has a local addr");
            let (replicate_tx, replicate_rx) = mpsc::channel(8);
            let dirty = Arc::new(AtomicBool::new(false));
            let cancel = CancellationToken::new();
            let handle = tokio::spawn(super::super::writer::run_peer_writer(
                super::super::PeerTarget {
                    peer: NodeId::from(2),
                    greeting: Greeting {
                        node: NodeId::from(1),
                        incarnation: 1,
                        cluster: wire::UNNAMED_CLUSTER,
                    },
                    addr,
                    dirty: Arc::clone(&dirty),
                    tls: no_tls(),
                    keepalive: None,
                },
                Arc::new(DropOldestQueue::new(4)),
                replicate_rx,
                cancel.clone(),
            ));
            Self {
                listener,
                replicate_tx,
                dirty,
                cancel,
                handle,
            }
        }

        /// Accepts the writer's next connection and reads its `Hello`.
        async fn accept(&self) -> Conn {
            let (stream, _) = self.listener.accept().await.expect("accept");
            let mut conn = Framed::new(
                stream,
                LengthDelimitedCodec::builder()
                    .max_frame_length(MAX_FRAME)
                    .new_codec(),
            );
            let hello = conn.next().await.expect("hello").expect("no io error");
            assert!(matches!(
                wire::decode(&hello.freeze()).expect("decodes"),
                Msg::Hello { .. }
            ));
            conn
        }

        async fn queue(&self, n: u8) {
            self.replicate_tx
                .send(out_frame(replicate(n)))
                .await
                .expect("writer alive");
        }

        fn is_dirty(&self) -> bool {
            self.dirty.load(Ordering::Relaxed)
        }

        async fn stop(self) {
            self.cancel.cancel();
            tokio::time::timeout(Duration::from_secs(2), self.handle)
                .await
                .expect("writer exits promptly once cancelled")
                .expect("writer task did not panic");
        }
    }

    async fn next_msg(conn: &mut Conn) -> Msg {
        let frame = conn.next().await.expect("frame").expect("no io error");
        wire::decode(&frame.freeze()).expect("decodes")
    }

    /// Reads from `conn` until a `Replicate` of `n` arrives, within 3 s.
    async fn reads_replicate(conn: &mut Conn, n: u8) -> bool {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if next_msg(conn).await == replicate(n) {
                    return;
                }
            }
        })
        .await
        .is_ok()
    }

    async fn becomes_dirty(writer: &Writer, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if writer.is_dirty() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        writer.is_dirty()
    }

    #[tokio::test]
    async fn run_peer_writer_redelivers_a_frame_whose_send_failed() {
        let writer = Writer::start().await;
        let conn1 = writer.accept().await;
        drop(conn1);
        // F1 goes into the dead socket (the first write after a peer close
        // succeeds locally), which provokes an RST.
        writer.queue(1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        // F2's write fails; the writer must not lose it.
        writer.queue(2).await;

        let mut conn2 = writer.accept().await;
        let delivered = reads_replicate(&mut conn2, 2).await;
        writer.stop().await;
        assert!(
            delivered,
            "the frame whose send failed must be redelivered on the reconnect"
        );
    }

    #[tokio::test]
    async fn run_peer_writer_sends_the_recent_tail_again_after_the_peer_closes() {
        let writer = Writer::start().await;
        let mut conn1 = writer.accept().await;
        writer.queue(1).await;
        assert!(reads_replicate(&mut conn1, 1).await, "conn 1 carries F1");
        drop(conn1);

        let mut conn2 = writer.accept().await;
        let delivered = reads_replicate(&mut conn2, 1).await;
        writer.stop().await;
        assert!(
            delivered,
            "a frame written a moment before the close may have died with the link, so it \
             goes out again"
        );
    }

    #[tokio::test]
    async fn run_peer_writer_redials_when_the_peer_closes_without_a_write() {
        let writer = Writer::start().await;
        let conn1 = writer.accept().await;
        let closed_at = Instant::now();
        drop(conn1);

        let conn2 = writer.accept().await;
        let waited = closed_at.elapsed();
        drop(conn2);
        writer.stop().await;
        assert!(
            waited >= Duration::from_millis(350),
            "a connection that lived a moment is redialed after the backoff, not at once: \
             {waited:?}"
        );
        assert!(waited < Duration::from_secs(3), "{waited:?}");
    }

    #[tokio::test]
    async fn run_peer_writer_does_not_hot_loop_against_a_peer_that_closes_every_connection() {
        let writer = Writer::start().await;
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        let listener = writer.listener;
        let acceptor = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                counter.fetch_add(1, Ordering::Relaxed);
                drop(stream);
            }
        });

        tokio::time::sleep(Duration::from_secs(2)).await;

        writer.cancel.cancel();
        acceptor.abort();
        let seen = accepts.load(Ordering::Relaxed);
        assert!(seen >= 2, "the writer keeps redialing: {seen}");
        assert!(seen <= 8, "dials are paced by the backoff: {seen}");
    }

    #[tokio::test]
    async fn run_peer_writer_marks_dirty_once_it_redials_after_a_connection_that_carried_frames() {
        let writer = Writer::start().await;
        let mut conn1 = writer.accept().await;
        writer.queue(1).await;
        assert!(reads_replicate(&mut conn1, 1).await);
        // The link outlives the backoff, so the frame it carried counts.
        tokio::time::sleep(RECONNECT_BACKOFF + Duration::from_millis(100)).await;
        assert!(!writer.is_dirty(), "a live connection owes nothing");
        drop(conn1);

        let _conn2 = writer.accept().await;
        let marked = becomes_dirty(&writer, Duration::from_secs(3)).await;
        writer.stop().await;
        assert!(
            marked,
            "the redial after a broken, frame-carrying link marks the peer"
        );
    }

    #[tokio::test]
    async fn rebind_waits_for_a_port_that_frees_within_its_bound() {
        let held = low_port_listener().await;
        let addr = held.local_addr().expect("addr");
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            drop(held);
        });
        let started = Instant::now();

        let again = rebind(addr).await;

        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "the first binds were refused while the listener held the port"
        );
        assert_eq!(again.local_addr().expect("addr"), addr);
        release.await.expect("the release task ends");
    }

    #[tokio::test]
    async fn run_peer_writer_marks_dirty_only_after_the_redial_succeeds() {
        let listener = low_port_listener().await;
        let addr = listener.local_addr().expect("addr");
        let writer = Writer::start_on(listener);
        let mut conn1 = writer.accept().await;
        writer.queue(1).await;
        assert!(reads_replicate(&mut conn1, 1).await);
        tokio::time::sleep(RECONNECT_BACKOFF + Duration::from_millis(100)).await;
        let Writer {
            listener,
            replicate_tx,
            dirty,
            cancel,
            handle,
        } = writer;
        // The peer goes away: the link breaks and the redials are refused.
        drop(conn1);
        drop(listener);
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(
            !dirty.load(Ordering::Relaxed),
            "a mark set while the peer is unreachable would be spent on a failed round"
        );

        let listener = rebind(addr).await;
        let writer = Writer {
            listener,
            replicate_tx,
            dirty,
            cancel,
            handle,
        };
        let _conn2 = writer.accept().await;
        let marked = becomes_dirty(&writer, Duration::from_secs(3)).await;
        writer.stop().await;
        assert!(marked, "the mark follows the successful redial");
    }

    #[tokio::test]
    async fn run_peer_writer_does_not_mark_dirty_when_an_idle_connection_closes() {
        let writer = Writer::start().await;
        drop(writer.accept().await);
        let _conn2 = writer.accept().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let dirty = writer.is_dirty();
        writer.stop().await;
        assert!(!dirty, "an idle close loses nothing");
    }

    #[tokio::test]
    async fn run_peer_writer_exits_promptly_when_cancelled_during_the_reconnect_pacing_sleep() {
        let writer = Writer::start().await;
        drop(writer.accept().await);
        // The writer has seen the close and waits out the backoff.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        writer.stop().await;
        assert!(
            started.elapsed() < Duration::from_millis(400),
            "{:?}",
            started.elapsed()
        );
    }
}
