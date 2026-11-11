//! The bounds on a connection after its `Hello`: the wait for a first frame
//! from an unknown source, and the wait inside a frame whose length prefix
//! has arrived. Every socket test runs on the paused clock and moves it with
//! `advance`, which lets the handler read what the test wrote before time
//! passes.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use tokio::io::AsyncWriteExt as _;

use super::{Accepted, Dropped, hello, invalidate, send_each, spawn_accepted};
use crate::net::conn::accept::{ReadTimeout, frame_idle_timeout, read_timeout_reason, read_wait};
use crate::net::conn::admit::{FIRST_FRAME_TIMEOUT, FRAME_STALL_TIMEOUT};
use crate::net::conn::framing::frame_started;
use crate::net::conn::pool::REQ_CONN_IDLE_TIMEOUT;
use crate::net::conn::tests::no_tls;

const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

#[test]
fn inside_a_frame_the_wait_is_the_stall_timeout_whatever_the_idle_wait() {
    for idle in [
        None,
        Some(Duration::from_secs(1)),
        Some(REQ_CONN_IDLE_TIMEOUT),
    ] {
        assert_eq!(read_wait(true, idle), Some(FRAME_STALL_TIMEOUT), "{idle:?}");
        assert_eq!(
            read_wait(false, idle),
            idle,
            "outside a frame the idle wait"
        );
    }
}

#[test]
fn only_an_unknown_source_with_no_frame_yet_waits_a_bounded_time_for_its_first() {
    // A connection that served a request is a pooling candidate.
    for unknown in [false, true] {
        for frames in [1, 7] {
            assert_eq!(
                frame_idle_timeout(1, frames, unknown),
                Some(REQ_CONN_IDLE_TIMEOUT)
            );
        }
    }
    assert_eq!(
        frame_idle_timeout(0, 0, true),
        Some(FIRST_FRAME_TIMEOUT),
        "an unknown source that has sent no frame"
    );
    assert_eq!(
        frame_idle_timeout(0, 0, false),
        None,
        "a peer's idle broadcast link is never cut"
    );
    assert_eq!(
        frame_idle_timeout(0, 3, true),
        None,
        "a source that has sent a broadcast frame is a link, bounded by the cap"
    );
    assert_eq!(frame_idle_timeout(0, 3, false), None);
}

#[test]
fn a_read_that_gave_up_counts_under_its_reason_except_an_idle_pooled_connection() {
    assert_eq!(
        read_timeout_reason(ReadTimeout::Idle, 0),
        Some("first_frame_timeout")
    );
    assert_eq!(read_timeout_reason(ReadTimeout::Idle, 3), None);
    for served in [0, 3] {
        assert_eq!(
            read_timeout_reason(ReadTimeout::Stalled, served),
            Some("frame_stall")
        );
    }
}

#[test]
fn a_codec_stands_inside_a_frame_from_its_length_prefix_until_it_returns_the_frame() {
    // Nothing buffered, nothing read: idle.
    assert!(!frame_started(false, false, 0, 0));
    // The prefix was taken and left the buffer empty: inside a frame.
    assert!(frame_started(false, false, 4, 0));
    // Part of a prefix or of a body is buffered.
    assert!(frame_started(false, false, 2, 2));
    assert!(frame_started(false, false, 0, 9));
    // A decode that touches nothing keeps the state.
    assert!(frame_started(true, false, 0, 0));
    assert!(!frame_started(false, false, 0, 0));
    // A returned frame ends it, whatever is left in the buffer.
    assert!(!frame_started(true, true, 90, 0));
    assert!(!frame_started(true, true, 90, 3));
}

/// Lets the handler read what the client wrote: a few tiny steps of the
/// paused clock, each of which runs the handler before time moves on.
async fn settle() {
    for _ in 0..3 {
        tokio::time::advance(Duration::from_millis(1)).await;
    }
}

/// Moves the paused clock by `by` and lets the handler react.
async fn pass(by: Duration) {
    tokio::time::advance(by).await;
    settle().await;
}

const SECOND: Duration = Duration::from_secs(1);

/// An accepted connection from an unknown source whose hello was read.
async fn greeted() -> Accepted {
    let mut accepted = spawn_accepted(no_tls()).await;
    send_each(&mut accepted.client, &[hello()]).await;
    settle().await;
    assert_eq!(accepted.mesh.admission.in_flight(), 0, "the hello decoded");
    accepted
}

/// The same from a source that is a peer's IP.
async fn greeted_peer() -> Accepted {
    let mesh =
        crate::net::MeshInner::for_tests(no_tls(), tokio_util::sync::CancellationToken::new());
    mesh.admission.set_known_sources([LOOPBACK]);
    let mut accepted = super::spawn_handler(super::loopback_pair().await, &mesh);
    send_each(&mut accepted.client, &[hello()]).await;
    settle().await;
    accepted
}

#[tokio::test(start_paused = true)]
async fn an_unknown_source_that_sends_no_frame_after_its_hello_is_closed_at_the_first_frame_timeout()
 {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let accepted = greeted().await;
    assert_eq!(accepted.mesh.admission.open(), 1);

    pass(FIRST_FRAME_TIMEOUT.saturating_sub(2 * SECOND)).await;
    assert!(!accepted.task.is_finished(), "still inside the timeout");
    assert_eq!(dropped.total("first_frame_timeout"), None);

    pass(3 * SECOND).await;
    assert!(accepted.task.is_finished(), "closed at the timeout");
    assert_eq!(dropped.total("first_frame_timeout"), Some(1));
    assert_eq!(dropped.total("hello_timeout"), None);
    assert_eq!(accepted.mesh.admission.open(), 0, "the slot is free");
}

#[tokio::test(start_paused = true)]
async fn a_peers_idle_link_before_its_first_frame_is_never_closed() {
    let mut accepted = greeted_peer().await;

    pass(Duration::from_hours(1)).await;
    assert!(!accepted.task.is_finished(), "an hour idle, still open");

    send_each(&mut accepted.client, &[invalidate(1)]).await;
    settle().await;
    assert_eq!(
        accepted.inbound.try_recv().expect("served").msg,
        invalidate(1)
    );
    pass(Duration::from_hours(1)).await;
    assert!(!accepted.task.is_finished(), "idle between frames too");
}

#[tokio::test(start_paused = true)]
async fn an_unknown_source_that_has_sent_a_frame_is_a_link_and_waits_on() {
    let mut accepted = spawn_accepted(no_tls()).await;
    send_each(&mut accepted.client, &[hello(), invalidate(1)]).await;
    settle().await;
    assert_eq!(
        accepted.inbound.try_recv().expect("served").msg,
        invalidate(1)
    );

    pass(Duration::from_hours(1)).await;
    assert!(!accepted.task.is_finished());
}

/// A frame head that declares `body` bytes, then `sent` of them.
fn partial_frame(body: u32, sent: usize) -> Vec<u8> {
    let mut bytes = body.to_be_bytes().to_vec();
    bytes.extend(std::iter::repeat_n(7u8, sent));
    bytes
}

#[tokio::test(start_paused = true)]
async fn a_peer_that_stops_after_a_length_prefix_is_closed_at_the_stall_timeout() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let mut accepted = greeted_peer().await;
    accepted
        .client
        .write_all(&partial_frame(4 << 20, 0))
        .await
        .expect("a prefix that declares 4 MiB");
    settle().await;

    pass(FRAME_STALL_TIMEOUT.saturating_sub(2 * SECOND)).await;
    assert!(!accepted.task.is_finished(), "inside the stall timeout");

    pass(3 * SECOND).await;
    assert!(accepted.task.is_finished(), "closed at the stall timeout");
    assert_eq!(dropped.total("frame_stall"), Some(1));
    assert_eq!(dropped.total("first_frame_timeout"), None);
}

#[tokio::test(start_paused = true)]
async fn an_unknown_source_that_stops_inside_a_frame_is_closed_at_the_stall_timeout() {
    let mut accepted = greeted().await;
    accepted
        .client
        .write_all(&partial_frame(100, 10))
        .await
        .expect("a prefix and part of the body");
    settle().await;

    pass(FRAME_STALL_TIMEOUT + SECOND).await;
    assert!(
        accepted.task.is_finished(),
        "the stall bound is shorter than the first-frame bound"
    );
    assert_eq!(accepted.mesh.admission.open(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_frame_that_keeps_arriving_is_not_cut_and_completes() {
    let mut accepted = greeted_peer().await;
    let frame = super::framed(&invalidate(1));
    let (head, rest) = frame.split_at(frame.len() - 6);
    let (a, b) = rest.split_at(3);

    accepted.client.write_all(head).await.expect("most of it");
    settle().await;
    pass(FRAME_STALL_TIMEOUT.saturating_sub(5 * SECOND)).await;
    accepted.client.write_all(a).await.expect("more of it");
    settle().await;
    pass(FRAME_STALL_TIMEOUT.saturating_sub(5 * SECOND)).await;
    assert!(
        !accepted.task.is_finished(),
        "{FRAME_STALL_TIMEOUT:?} passed in two pauses, but each ended with progress"
    );
    accepted.client.write_all(b).await.expect("the rest");
    settle().await;

    assert_eq!(
        accepted
            .inbound
            .try_recv()
            .expect("the frame completed")
            .msg,
        invalidate(1)
    );
    assert!(!accepted.task.is_finished());
}
