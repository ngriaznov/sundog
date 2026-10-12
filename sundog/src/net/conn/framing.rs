//! Framed IO primitives and TLS layering.

#[cfg(doc)]
use crate::net::OutFrame;

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;

use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::net::tcp::TcpStream;
use crate::net::{MeshStream, TlsCtx};
use crate::store::KeyVersion;
use crate::wire::{self, MAX_FRAME, Msg};

pub(in crate::net) type PeerFramed = Framed<MeshStream, LengthDelimitedCodec>;

/// Disables Nagle's algorithm on a freshly dialed/accepted socket. Every
/// wire message here is already a deliberately-sized batch, and Nagle's
/// interaction with delayed ACKs is a classic multi-millisecond stall.
/// Best-effort: a failure here surfaces on the next real read/write instead.
pub(super) fn disable_nagle(stream: &TcpStream) {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(%error, "set_nodelay failed; leaving Nagle's algorithm enabled");
    }
}

pub(super) fn new_framed(stream: MeshStream) -> PeerFramed {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_framed(stream)
}

/// Layers TLS onto a freshly dialed `stream` per `tls`, a no-op passthrough
/// whenever TLS isn't compiled in for this transport.
#[cfg(all(feature = "tls", not(feature = "sim")))]
pub(super) async fn establish_dial(stream: TcpStream, tls: &TlsCtx) -> std::io::Result<MeshStream> {
    match tls {
        Some(ctx) => ctx.connect(stream).await,
        None => Ok(MeshStream::Plain(stream)),
    }
}
// A genuine `.await`, not `Ok(stream)` alone, keeps this `async fn` like
// the TLS-active branch above, so every caller writes `.await` unconditionally.
#[cfg(not(all(feature = "tls", not(feature = "sim"))))]
pub(super) async fn establish_dial(
    stream: TcpStream,
    _tls: &TlsCtx,
) -> std::io::Result<MeshStream> {
    std::future::ready(Ok(stream)).await
}

/// Layers TLS onto a freshly accepted `stream`, the accept-side counterpart of
/// [`establish_dial`].
#[cfg(all(feature = "tls", not(feature = "sim")))]
pub(super) async fn establish_accept(
    stream: TcpStream,
    tls: &TlsCtx,
) -> std::io::Result<MeshStream> {
    match tls {
        Some(ctx) => ctx.accept(stream).await,
        None => Ok(MeshStream::Plain(stream)),
    }
}
#[cfg(not(all(feature = "tls", not(feature = "sim"))))]
pub(super) async fn establish_accept(
    stream: TcpStream,
    _tls: &TlsCtx,
) -> std::io::Result<MeshStream> {
    std::future::ready(Ok(stream)).await
}

pub(in crate::net) async fn send_msg(framed: &mut PeerFramed, msg: &Msg) -> Result<(), CodecError> {
    let frame = wire::encode(msg)?;
    let len = frame.len();
    framed.send(frame).await.map_err(CodecError::Io)?;
    crate::net::record_frame_sent(len);
    Ok(())
}

/// Writes `msg` into `framed`'s buffer without flushing it: the caller
/// flushes once for a run of messages.
pub(super) async fn feed_msg(framed: &mut PeerFramed, msg: &Msg) -> Result<(), CodecError> {
    let frame = wire::encode(msg)?;
    let len = frame.len();
    framed.feed(frame).await.map_err(CodecError::Io)?;
    crate::net::record_frame_sent(len);
    Ok(())
}

/// Sends every message in `msgs` and flushes once at the end. Used for
/// control-plane replies and `Hello`-plus-first-request dials, which build
/// their own `Msg`s rather than reusing an [`OutFrame`] like [`send_frames`].
pub(super) async fn send_batch(framed: &mut PeerFramed, msgs: &[Msg]) -> Result<(), CodecError> {
    for msg in msgs {
        let frame = wire::encode(msg)?;
        let len = frame.len();
        framed.feed(frame).await.map_err(CodecError::Io)?;
        crate::net::record_frame_sent(len);
    }
    framed.flush().await.map_err(CodecError::Io)?;
    Ok(())
}

/// [`send_batch`], but for already-encoded frames: the per-peer writer's
/// broadcast-class drain feeds these straight onto the wire. The frame is
/// built once, upstream, and shared as a `Bytes` clone per peer.
pub(super) async fn send_frames(
    framed: &mut PeerFramed,
    encoded: impl IntoIterator<Item = Bytes>,
) -> Result<(), CodecError> {
    for frame in encoded {
        let len = frame.len();
        framed.feed(frame).await.map_err(CodecError::Io)?;
        crate::net::record_frame_sent(len);
    }
    framed.flush().await.map_err(CodecError::Io)?;
    Ok(())
}

/// [`send_or_cancelled`], but for a whole in-memory reply rather than one
/// message at a time: feeds every message in `msgs` and flushes once,
/// racing the whole batch against `cancel`. Returns `true` when this
/// connection is done.
pub(super) async fn send_batch_or_cancelled(
    framed: &mut PeerFramed,
    msgs: &[Msg],
    cancel: &CancellationToken,
) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        sent = send_batch(framed, msgs) => sent.is_err(),
    }
}

pub(in crate::net) async fn recv_msg(framed: &mut PeerFramed) -> Option<Result<Msg, CodecError>> {
    match framed.next().await {
        Some(Ok(bytes)) => Some(wire::decode(&bytes.freeze())),
        Some(Err(source)) => Some(Err(CodecError::Io(source))),
        None => None,
    }
}

/// Sends `msg`, racing the write against `cancel`. Returns `true` when this
/// connection is done.
pub(super) async fn send_or_cancelled(
    framed: &mut PeerFramed,
    msg: &Msg,
    cancel: &CancellationToken,
) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        sent = send_msg(framed, msg) => sent.is_err(),
    }
}

/// [`KeyVersion`]s to the wire's own `(key, version)` tuple shape, for a
/// [`Msg::AeBucket`]/[`Msg::AePart`] reply.
pub(super) fn key_versions_to_wire(entries: Vec<KeyVersion>) -> Vec<(Bytes, Hlc)> {
    entries.into_iter().map(|kv| (kv.key, kv.version)).collect()
}

/// The wire's `(key, version)` tuple shape back to [`KeyVersion`]s, decoding
/// a [`Msg::AeBucket`]/[`Msg::AePart`] reply.
pub(super) fn wire_to_key_versions(entries: Vec<(Bytes, Hlc)>) -> Vec<KeyVersion> {
    entries
        .into_iter()
        .map(|(key, version)| KeyVersion { key, version })
        .collect()
}
