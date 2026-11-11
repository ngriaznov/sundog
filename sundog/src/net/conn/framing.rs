//! Framed IO primitives and TLS layering.

#[cfg(doc)]
use crate::net::OutFrame;

use bytes::Bytes;
use bytes::BytesMut;
use futures::{Sink, SinkExt as _, StreamExt as _};
use tokio_util::codec::{Decoder, Encoder, Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;

#[cfg(not(feature = "sim"))]
use crate::config::{MESH_KEEPALIVE_MAX, MESH_KEEPALIVE_MIN};
use crate::error::CodecError;
use crate::hlc::Hlc;
use crate::net::tcp::TcpStream;
use crate::net::{MeshStream, TlsCtx};
use crate::store::KeyVersion;
use crate::wire::{self, MAX_FRAME, Msg};

pub(in crate::net) type PeerFramed = Framed<MeshStream, PeerCodec>;

/// The codec of every mesh connection: a [`LengthDelimitedCodec`] that also
/// reports whether it stands inside a frame, so a reader can bound a
/// connection that sent a frame's length prefix and then stopped. The wire
/// format is the length-delimited codec's.
pub(in crate::net) struct PeerCodec {
    inner: LengthDelimitedCodec,
    /// Whether the last decode left a frame started and not complete.
    mid_frame: bool,
}

impl PeerCodec {
    fn new(max_frame: usize) -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(max_frame)
                .new_codec(),
            mid_frame: false,
        }
    }

    /// Sets the largest frame the codec accepts.
    pub(in crate::net) fn set_max_frame_length(&mut self, max_frame: usize) {
        self.inner.set_max_frame_length(max_frame);
    }

    /// Whether the codec has taken the start of a frame, its length prefix
    /// or more, and waits for the rest. The prefix leaves the read buffer
    /// when it is taken, so the buffer alone cannot say.
    pub(in crate::net) fn mid_frame(&self) -> bool {
        self.mid_frame
    }
}

impl Decoder for PeerCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, Self::Error> {
        let before = src.len();
        let decoded = self.inner.decode(src)?;
        self.mid_frame = frame_started(self.mid_frame, decoded.is_some(), before, src.len());
        Ok(decoded)
    }
}

impl Encoder<Bytes> for PeerCodec {
    type Error = std::io::Error;

    fn encode(&mut self, frame: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(frame, dst)
    }
}

/// Whether a codec stands inside a frame after one decode call, given
/// whether it stood in one before (`was_mid_frame`), whether the call
/// returned a frame, and the read buffer's length before and after. A
/// returned frame ends the frame; an unfinished decode is inside one when it
/// consumed bytes (the length prefix) or leaves some buffered, and a decode
/// that touched nothing keeps the state it had. Pure; unit tested directly.
pub(super) const fn frame_started(
    was_mid_frame: bool,
    returned_frame: bool,
    len_before: usize,
    len_after: usize,
) -> bool {
    if returned_frame {
        return false;
    }
    was_mid_frame || len_after != len_before || len_after > 0
}

/// When the kernel probes a silent connection and how often it gives up:
/// [`keepalive_timing`]'s result for one `ClusterConfig::mesh_keepalive`.
#[cfg(not(feature = "sim"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::net) struct KeepaliveTiming {
    /// Silence before the first probe.
    pub(in crate::net) idle: std::time::Duration,
    /// The gap between two unanswered probes.
    pub(in crate::net) interval: std::time::Duration,
    /// Unanswered probes before the kernel drops the connection.
    pub(in crate::net) retries: u32,
}

/// The keepalive timing for an `idle` of silence: probes every third of
/// `idle`, three times, so a peer that stopped answering is dropped about
/// `2 * idle` after it went silent. The idle and the interval are held
/// within [`MESH_KEEPALIVE_MIN`] and [`MESH_KEEPALIVE_MAX`], the whole
/// seconds a kernel accepts for either. Pure; unit tested directly.
#[cfg(not(feature = "sim"))]
pub(in crate::net) fn keepalive_timing(idle: std::time::Duration) -> KeepaliveTiming {
    let bounded = |d: std::time::Duration| d.clamp(MESH_KEEPALIVE_MIN, MESH_KEEPALIVE_MAX);
    let idle = bounded(idle);
    KeepaliveTiming {
        idle,
        interval: bounded(idle / 3),
        retries: 3,
    }
}

/// Readies a freshly dialed or accepted socket. Disables Nagle's
/// algorithm: every wire message here is already a deliberately-sized
/// batch, and Nagle's interaction with delayed ACKs is a classic
/// multi-millisecond stall. Turns on TCP keepalive at `keepalive`'s timing
/// ([`keepalive_timing`]), so a connection whose peer vanished without
/// closing it errors instead of waiting forever; `None` leaves it off, as
/// does the `sim` transport, which has no kernel socket. Best-effort: a
/// setting the platform refuses is logged, and the socket keeps what the
/// system applied.
pub(super) fn tune_socket(stream: &TcpStream, keepalive: Option<std::time::Duration>) {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(%error, "set_nodelay failed; leaving Nagle's algorithm enabled");
    }
    #[cfg(not(feature = "sim"))]
    if let Some(idle) = keepalive
        && let Err(error) = enable_keepalive(stream, keepalive_timing(idle))
    {
        tracing::debug!(
            %error,
            "TCP keepalive not fully set; the socket keeps the system's settings for the rest"
        );
    }
    #[cfg(feature = "sim")]
    let _ = keepalive;
}

/// Turns on TCP keepalive at `timing` on `stream`, setting the probe
/// interval and count where the platform lets a socket choose them.
#[cfg(not(feature = "sim"))]
fn enable_keepalive(stream: &TcpStream, timing: KeepaliveTiming) -> std::io::Result<()> {
    let params = socket2::TcpKeepalive::new().with_time(timing.idle);
    // The platforms socket2 sets an interval and a probe count on.
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "emscripten",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        target_os = "nuttx",
        all(target_os = "wasi", not(target_env = "p1")),
    ))]
    let params = params
        .with_interval(timing.interval)
        .with_retries(timing.retries);
    socket2::SockRef::from(stream).set_tcp_keepalive(&params)
}

pub(super) fn new_framed(stream: MeshStream) -> PeerFramed {
    Framed::new(stream, PeerCodec::new(MAX_FRAME))
}

/// The largest frame an accepted connection may send before its `Hello`
/// decodes. A `Hello` encodes to a few dozen bytes. The codec reserves the
/// declared length in its read buffer as soon as it reads a frame's length
/// prefix, so the limit keeps a connection that has said nothing from
/// committing the buffer to [`MAX_FRAME`] bytes.
pub(super) const HELLO_MAX_FRAME: usize = 1024;

/// [`new_framed`] for an accepted connection that has not sent its `Hello`:
/// frames are capped at [`HELLO_MAX_FRAME`] until the caller lifts the cap
/// with `set_max_frame_length(MAX_FRAME)` on the codec.
pub(super) fn new_framed_pre_hello(stream: MeshStream) -> PeerFramed {
    Framed::new(stream, PeerCodec::new(HELLO_MAX_FRAME))
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
/// built once, upstream, and shared as a `Bytes` clone per peer. Generic over
/// the link so the writer's tests can script a link that fails a flush.
pub(super) async fn send_frames<L>(
    link: &mut L,
    encoded: impl IntoIterator<Item = Bytes>,
) -> Result<(), CodecError>
where
    L: Sink<Bytes, Error = std::io::Error> + Unpin,
{
    for frame in encoded {
        let len = frame.len();
        link.feed(frame).await.map_err(CodecError::Io)?;
        crate::net::record_frame_sent(len);
    }
    link.flush().await.map_err(CodecError::Io)?;
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
