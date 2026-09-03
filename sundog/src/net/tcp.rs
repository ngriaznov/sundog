//! Transport seam: the concrete `TcpListener`/`TcpStream` types `net` binds
//! and dials through. Real `tokio::net` normally, `turmoil::net` under the
//! `sim` feature.
//!
//! Turmoil's types mirror `tokio::net`'s shapes, so call sites elsewhere in
//! `net` name `TcpListener`/`TcpStream` unconditionally, never knowing which
//! backend they got.

#[cfg(not(feature = "sim"))]
pub(super) use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "sim")]
pub(super) use turmoil::net::{TcpListener, TcpStream};
