//! `fwdskew`: a forward stamped ahead of every clock, sent to the other
//! members as a writer that is not in the cluster would send it.
//!
//! A `Mode::Distributed` write a non-owner makes travels as a
//! `Msg::ForwardBatch` the writer keeps no copy of, and an owner whose clock
//! is behind its stamp by more than `max_clock_skew` holds or drops it. The
//! library gives a cache no clock to set, so the command builds the frames
//! itself: a `Hello` naming a writer id no member uses, then one forward
//! under view hash 0, which no member's view carries.

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use sundog::wire::{self, Msg, WireRecord};
use sundog::{Hlc, NodeId};
use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpStream;

/// The writer id of the forwards `fwdskew` sends: in no member's view.
pub(crate) const FORWARD_WRITER: u64 = 0x5eed_0002;

/// The bytes of one connection that greets as [`FORWARD_WRITER`] and sends
/// one forward of `key` and `value` for `cache`, stamped at `wall_ms`, each
/// frame length-prefixed as the data plane frames them.
///
/// # Errors
///
/// Returns the codec's error text when a message does not encode.
pub(crate) fn forward_stream(
    cache: &str,
    key: &str,
    value: &str,
    wall_ms: u64,
) -> Result<Vec<u8>, String> {
    let encode = |text: &str| postcard::to_stdvec(text).map_err(|error| error.to_string());
    let record = WireRecord {
        key: Bytes::from(encode(key)?),
        value: Some(Bytes::from(encode(value)?)),
        ver: Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(FORWARD_WRITER),
        },
        expires_at_ms: None,
    };
    let messages = [
        Msg::Hello {
            node: NodeId::from(FORWARD_WRITER),
            incarnation: 1,
            protocol: wire::PROTOCOL_VERSION,
            cluster: wire::UNNAMED_CLUSTER,
        },
        Msg::ForwardBatch {
            cache: cache.into(),
            view_hash: 0,
            hops: 0,
            recs: vec![record],
        },
    ];
    let mut stream = Vec::new();
    for message in &messages {
        let frame = wire::encode(message).map_err(|error| error.to_string())?;
        let length = u32::try_from(frame.len()).map_err(|error| error.to_string())?;
        stream.extend_from_slice(&length.to_be_bytes());
        stream.extend_from_slice(&frame);
    }
    Ok(stream)
}

/// The arguments of `fwdskew <ahead-ms> <key> <value>`: `ahead` is the first
/// word of the line after the command and `rest` the remainder, which holds
/// the key, one space, and the value (which may contain spaces). `None` for a
/// missing or malformed argument. Pure; unit tested directly.
pub(crate) fn parse_fwdskew<'a>(
    ahead: Option<&str>,
    rest: Option<&'a str>,
) -> Option<(u64, &'a str, &'a str)> {
    let ahead = ahead?.parse().ok()?;
    let (key, value) = rest?.split_once(' ')?;
    Some((ahead, key, value))
}

/// The wall clock in epoch milliseconds.
fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis()),
    )
    .unwrap_or(u64::MAX)
}

/// Sends the forward of `key` and `value` to each of `targets`, stamped
/// `ahead_ms` ahead of this node's clock. Answers how many accepted it.
///
/// # Errors
///
/// Returns the first error text when the frames do not encode; a target that
/// refuses the connection is not counted.
pub(crate) async fn send_forwards(
    targets: &[SocketAddr],
    cache: &str,
    key: &str,
    value: &str,
    ahead_ms: u64,
) -> Result<usize, String> {
    let stream = forward_stream(cache, key, value, now_ms().saturating_add(ahead_ms))?;
    let mut sent = 0;
    for target in targets {
        let Ok(mut connection) = TcpStream::connect(target).await else {
            continue;
        };
        if connection.write_all(&stream).await.is_ok() && connection.shutdown().await.is_ok() {
            sent += 1;
        }
    }
    Ok(sent)
}
