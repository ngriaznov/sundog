//! Transport seam: the concrete `TcpListener`/`TcpStream` types the rest of
//! `net` binds and dials through — real `tokio::net` normally, `turmoil::net`
//! under the `sim` feature. Turmoil's networking types mirror `tokio::net`'s
//! shapes (`bind`/`connect` taking anything `ToSocketAddrs`-like, `accept`,
//! `local_addr`, and both implement `AsyncRead`/`AsyncWrite`, so
//! `tokio_util`'s `LengthDelimitedCodec` framing in `net::conn` works
//! unmodified against either), so picking the alias here is the only
//! difference between the two builds: every call site elsewhere in `net`
//! names `TcpListener`/`TcpStream` unconditionally and never sees which
//! backend it got. With the `sim` feature off, this module is nothing but a
//! re-export of `tokio::net`'s own types.

#[cfg(not(feature = "sim"))]
pub(super) use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "sim")]
pub(super) use turmoil::net::{TcpListener, TcpStream};
