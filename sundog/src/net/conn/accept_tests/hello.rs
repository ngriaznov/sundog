//! A connection that has not sent its `Hello`: the deadline and the frame limit.

use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{
    Accepted, Warnings, dropped, framed, hello, invalidate, peer, replicate_of, send_each,
    spawn_accepted, spawn_accepted_in,
};
use crate::net::MeshInner;
use crate::net::conn::admit::HELLO_TIMEOUT;
use crate::net::conn::framing::HELLO_MAX_FRAME;
use crate::net::conn::tests::no_tls;
use crate::wire::{self, Msg};

/// Real-time bound on an event that happens at once. Well under
/// `HELLO_TIMEOUT`, so a close inside it is not the deadline.
const AT_ONCE: Duration = Duration::from_secs(5);

/// The slack a deadline close may add to `HELLO_TIMEOUT` on the paused clock,
/// which advances exactly to the timer.
const DEADLINE_SLACK: Duration = Duration::from_millis(50);

/// Waits for every handler task and asserts they all ended at the deadline:
/// on the paused clock, `HELLO_TIMEOUT` after the tasks started together.
async fn all_end_at_the_deadline(accepted: &mut [Accepted]) {
    let started = Instant::now();
    for one in accepted {
        (&mut one.task)
            .await
            .expect("the handler task does not panic");
    }
    let elapsed = started.elapsed();
    assert!(
        (HELLO_TIMEOUT..HELLO_TIMEOUT + DEADLINE_SLACK).contains(&elapsed),
        "the handlers ended after {elapsed:?}, not at the {HELLO_TIMEOUT:?} deadline"
    );
}

async fn ends_at_the_deadline(accepted: &mut Accepted) {
    all_end_at_the_deadline(std::slice::from_mut(accepted)).await;
}

/// Waits (on the real clock) for the handler task to end at once.
async fn ends_at_once(accepted: &mut Accepted) {
    tokio::time::timeout(AT_ONCE, &mut accepted.task)
        .await
        .expect("the handler ends long before the Hello deadline")
        .expect("the handler task does not panic");
}

/// Asserts the client end reads EOF or a reset: the server closed the link.
async fn assert_closed_by_server(accepted: &mut Accepted) {
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(AT_ONCE, accepted.client.read(&mut byte))
        .await
        .expect("the closed link reads at once");
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "the server closed the link: {read:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_silent_connection_closes_at_the_hello_deadline() {
    let mut accepted = spawn_accepted(no_tls()).await;
    ends_at_the_deadline(&mut accepted).await;
}

#[tokio::test(start_paused = true)]
async fn a_partial_length_prefix_closes_at_the_hello_deadline() {
    let mut accepted = spawn_accepted(no_tls()).await;
    accepted
        .client
        .write_all(&[0, 0])
        .await
        .expect("a partial length prefix");
    ends_at_the_deadline(&mut accepted).await;
}

#[tokio::test(start_paused = true)]
async fn a_partial_hello_body_closes_at_the_hello_deadline() {
    let mut accepted = spawn_accepted(no_tls()).await;
    let frame = framed(&hello());
    accepted
        .client
        .write_all(&frame[..frame.len() - 3])
        .await
        .expect("a hello missing its last bytes");
    ends_at_the_deadline(&mut accepted).await;
}

#[tokio::test(start_paused = true)]
async fn each_connection_closed_at_the_deadline_is_counted_and_the_warning_is_rate_limited() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (warnings, _log) = Warnings::capture();
    let mesh = MeshInner::for_tests(no_tls(), CancellationToken::new());
    let mut silent = Vec::new();
    for _ in 0..3 {
        silent.push(spawn_accepted_in(&mesh).await);
    }
    assert_eq!(dropped(&snapshotter, "hello_timeout"), None);

    all_end_at_the_deadline(&mut silent).await;

    assert_eq!(dropped(&snapshotter, "hello_timeout"), Some(3));
    assert_eq!(dropped(&snapshotter, "over_cap"), None);
    let logged = warnings.taken();
    assert_eq!(
        logged.len(),
        1,
        "one warning inside the spacing: {logged:?}"
    );
    assert!(logged[0].contains("hello"), "{logged:?}");
}

#[tokio::test]
async fn an_oversized_first_frame_closes_at_once_and_is_not_a_deadline_close() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let mut accepted = spawn_accepted(no_tls()).await;
    accepted
        .client
        .write_all(&(2u32 << 20).to_be_bytes())
        .await
        .expect("a length prefix declaring 2 MiB");

    ends_at_once(&mut accepted).await;

    assert_closed_by_server(&mut accepted).await;
    assert_eq!(dropped(&snapshotter, "hello_timeout"), None);
}

#[tokio::test]
async fn a_hello_frame_of_exactly_the_limit_is_read_and_one_byte_more_closes() {
    let hello_bytes = wire::encode(&hello()).expect("encodes").to_vec();
    let padded = |len: usize| {
        let mut body = hello_bytes.clone();
        body.resize(len, 0);
        let mut frame = u32::try_from(len).expect("fits u32").to_be_bytes().to_vec();
        frame.extend(body);
        frame
    };

    let mut accepted = spawn_accepted(no_tls()).await;
    accepted
        .client
        .write_all(&padded(HELLO_MAX_FRAME))
        .await
        .expect("a hello padded to the limit");
    send_each(&mut accepted.client, &[invalidate(1)]).await;
    let delivered = tokio::time::timeout(AT_ONCE, accepted.inbound.recv())
        .await
        .expect("a hello of exactly the limit is served")
        .expect("the inbound channel is open");
    assert_eq!(delivered.msg, invalidate(1));

    let mut over = spawn_accepted(no_tls()).await;
    over.client
        .write_all(&padded(HELLO_MAX_FRAME + 1))
        .await
        .expect("a hello one byte past the limit");
    ends_at_once(&mut over).await;
}

#[tokio::test]
async fn a_frame_past_the_hello_limit_is_accepted_after_the_hello_in_separate_writes() {
    let mut accepted = spawn_accepted(no_tls()).await;
    // The small message arrives behind the hello, so the hello has decoded
    // and the limit has lifted by the time the large frame's head is read.
    send_each(&mut accepted.client, &[hello(), invalidate(1)]).await;
    let first = tokio::time::timeout(AT_ONCE, accepted.inbound.recv())
        .await
        .expect("the small message is served")
        .expect("the inbound channel is open");
    assert_eq!(first.msg, invalidate(1));

    let large = replicate_of(2 << 20);
    send_each(&mut accepted.client, std::slice::from_ref(&large)).await;
    let second = tokio::time::timeout(AT_ONCE, accepted.inbound.recv())
        .await
        .expect("a 2 MiB frame is served after the hello")
        .expect("the inbound channel is open");
    assert_eq!(second.msg, large);
}

#[tokio::test]
async fn a_frame_past_the_hello_limit_is_accepted_after_the_hello_in_one_write() {
    let mut accepted = spawn_accepted(no_tls()).await;
    let large = replicate_of(2 << 20);
    let mut both = framed(&hello());
    both.extend(framed(&large));
    accepted
        .client
        .write_all(&both)
        .await
        .expect("the hello and the large frame in one write");

    let delivered = tokio::time::timeout(AT_ONCE, accepted.inbound.recv())
        .await
        .expect("the frame behind the hello is served")
        .expect("the inbound channel is open");
    assert_eq!(delivered.msg, large);
}

#[tokio::test]
async fn a_non_hello_first_message_closes_the_connection_at_once() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let mut accepted = spawn_accepted(no_tls()).await;
    let mut both = framed(&Msg::ReqDone);
    both.extend(framed(&hello()));
    accepted
        .client
        .write_all(&both)
        .await
        .expect("a first message that is not a hello, then a hello");

    ends_at_once(&mut accepted).await;

    assert_closed_by_server(&mut accepted).await;
    assert!(
        accepted.inbound.try_recv().is_err(),
        "nothing is forwarded from a connection that did not open with a hello"
    );
    assert_eq!(dropped(&snapshotter, "hello_timeout"), None);
}

#[tokio::test]
async fn cancel_ends_a_pending_hello_without_counting_a_timeout() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let mut accepted = spawn_accepted(no_tls()).await;

    accepted.cancel.cancel();

    ends_at_once(&mut accepted).await;
    assert_eq!(dropped(&snapshotter, "hello_timeout"), None);
}

#[tokio::test]
async fn a_hello_and_a_request_in_one_write_are_both_served() {
    let mut accepted = spawn_accepted(no_tls()).await;
    let mut both = framed(&hello());
    both.extend(framed(&invalidate(2)));
    accepted
        .client
        .write_all(&both)
        .await
        .expect("a hello and its first message in one write");

    let delivered = tokio::time::timeout(AT_ONCE, accepted.inbound.recv())
        .await
        .expect("the message behind the hello is served")
        .expect("the inbound channel is open");
    assert_eq!(delivered.msg, invalidate(2));
    assert_eq!(delivered.from, peer());
}
