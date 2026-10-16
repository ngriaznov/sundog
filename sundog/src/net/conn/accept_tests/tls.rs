//! A TLS node's connection that has not sent its `Hello`: one deadline spans
//! the handshake and the first message.

use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::time::Instant;

use super::{Accepted, Dropped, framed, hello, invalidate, peer, spawn_accepted};
use crate::net::conn::admit::HELLO_TIMEOUT;
use crate::net::conn::framing::{new_framed, send_msg};
use crate::net::conn::tests::no_tls;
use crate::net::tls::fixtures::mesh_tls_pair;

/// Real-time bound on an event that happens at once.
const AT_ONCE: Duration = Duration::from_secs(5);

/// The slack a deadline close may add to `HELLO_TIMEOUT` on the paused clock.
const DEADLINE_SLACK: Duration = Duration::from_millis(50);

/// The five bytes of a TLS record header announcing a 256-byte handshake
/// record, which never follows.
const STALLED_CLIENT_HELLO: [u8; 5] = [0x16, 0x03, 0x01, 0x01, 0x00];

#[tokio::test(start_paused = true)]
async fn tls_a_silent_connection_closes_at_the_hello_deadline() {
    let (server_tls, _client_tls) = mesh_tls_pair();
    let mut accepted = spawn_accepted(Some(server_tls)).await;
    let started = Instant::now();

    (&mut accepted.task).await.expect("the handler ends");

    let elapsed = started.elapsed();
    assert!(
        (HELLO_TIMEOUT..HELLO_TIMEOUT + DEADLINE_SLACK).contains(&elapsed),
        "{elapsed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn tls_a_stalled_client_hello_closes_at_the_hello_deadline() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (server_tls, _client_tls) = mesh_tls_pair();
    let mut accepted = spawn_accepted(Some(server_tls)).await;
    accepted
        .client
        .write_all(&STALLED_CLIENT_HELLO)
        .await
        .expect("the start of a ClientHello");
    let started = Instant::now();

    (&mut accepted.task).await.expect("the handler ends");

    let elapsed = started.elapsed();
    assert!(
        (HELLO_TIMEOUT..HELLO_TIMEOUT + DEADLINE_SLACK).contains(&elapsed),
        "{elapsed:?}"
    );
    assert_eq!(dropped.total("hello_timeout"), Some(1));
}

#[tokio::test]
async fn tls_a_finished_handshake_without_a_hello_closes_at_the_hello_deadline() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (server_tls, client_tls) = mesh_tls_pair();
    let Accepted {
        client, mut task, ..
    } = spawn_accepted(Some(server_tls)).await;

    // The handshake runs on the real clock.
    let _link = client_tls
        .connect(client)
        .await
        .expect("the handshake completes");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!task.is_finished(), "the handler waits for the hello");

    // Then the clock freezes and jumps past the deadline.
    tokio::time::pause();
    let started = Instant::now();
    tokio::time::advance(HELLO_TIMEOUT + Duration::from_secs(1)).await;
    (&mut task).await.expect("the handler ends");

    assert!(started.elapsed() >= HELLO_TIMEOUT);
    assert_eq!(dropped.total("hello_timeout"), Some(1));
}

#[tokio::test]
async fn tls_a_hello_over_a_finished_handshake_is_served() {
    let (server_tls, client_tls) = mesh_tls_pair();
    let Accepted {
        client,
        mut inbound,
        ..
    } = spawn_accepted(Some(server_tls)).await;

    let stream = client_tls
        .connect(client)
        .await
        .expect("the handshake completes");
    let mut link = new_framed(stream);
    send_msg(&mut link, &hello()).await.expect("hello");
    send_msg(&mut link, &invalidate(1)).await.expect("message");

    let delivered = tokio::time::timeout(AT_ONCE, inbound.recv())
        .await
        .expect("the message behind the hello is served")
        .expect("the inbound channel is open");
    assert_eq!(delivered.msg, invalidate(1));
    assert_eq!(delivered.from, peer());
}

#[tokio::test]
async fn tls_the_slot_is_held_through_the_handshake_and_freed_at_the_hello() {
    let (server_tls, client_tls) = mesh_tls_pair();
    let Accepted {
        client,
        mut inbound,
        mesh,
        ..
    } = spawn_accepted(Some(server_tls)).await;
    assert_eq!(mesh.admission.in_flight(), 1, "waiting for the handshake");

    let stream = client_tls
        .connect(client)
        .await
        .expect("the handshake completes");
    let mut link = new_framed(stream);
    assert_eq!(
        mesh.admission.in_flight(),
        1,
        "the handshake is not a hello"
    );
    send_msg(&mut link, &hello()).await.expect("hello");
    send_msg(&mut link, &invalidate(1)).await.expect("message");
    tokio::time::timeout(AT_ONCE, inbound.recv())
        .await
        .expect("the message behind the hello is served")
        .expect("the inbound channel is open");

    assert_eq!(mesh.admission.in_flight(), 0, "the hello released the slot");
}

#[tokio::test]
async fn tls_a_plaintext_client_is_dropped_at_the_handshake_without_a_timeout_count() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (server_tls, _client_tls) = mesh_tls_pair();
    let mut accepted = spawn_accepted(Some(server_tls)).await;
    accepted
        .client
        .write_all(&framed(&hello()))
        .await
        .expect("a plaintext hello");

    tokio::time::timeout(AT_ONCE, &mut accepted.task)
        .await
        .expect("a failed handshake ends the handler at once")
        .expect("the handler does not panic");

    assert_eq!(dropped.total("hello_timeout"), None);
    // The node may send a TLS alert before it closes the link.
    let mut alert = Vec::new();
    // EOF and a reset both end the read.
    let _ = tokio::time::timeout(AT_ONCE, accepted.client.read_to_end(&mut alert))
        .await
        .expect("the node closes the link");
}

#[tokio::test]
async fn tls_a_client_against_a_plaintext_node_fails_at_once() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let dropped = Dropped::new(recorder.snapshotter());
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (_server_tls, client_tls) = mesh_tls_pair();
    let Accepted {
        client, mut task, ..
    } = spawn_accepted(no_tls()).await;

    let connected = tokio::time::timeout(AT_ONCE, client_tls.connect(client))
        .await
        .expect("the handshake fails at once");
    assert!(connected.is_err(), "no TLS handshake with a plaintext node");
    tokio::time::timeout(AT_ONCE, &mut task)
        .await
        .expect("the plaintext node drops the connection at once")
        .expect("the handler does not panic");

    assert_eq!(dropped.total("hello_timeout"), None);
}
