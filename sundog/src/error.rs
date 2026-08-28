//! Domain error types. One `thiserror` enum per fallible area, per house style.

use std::io;
use std::net::SocketAddr;

use smol_str::SmolStr;

use crate::node::NodeId;

/// Errors from encoding or decoding a [`crate::wire::Msg`] or [`crate::wire::WireRecord`]
/// on the wire.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The frame exceeded [`crate::wire::MAX_FRAME`].
    #[error("frame of {size} bytes exceeds the {limit}-byte cap")]
    FrameTooLarge {
        /// The offending frame's size in bytes.
        size: usize,
        /// The configured cap it exceeded.
        limit: usize,
    },
    /// postcard failed to serialize or deserialize a value.
    #[error("postcard codec error")]
    Postcard(#[from] postcard::Error),
    /// The underlying I/O stream failed.
    #[error("i/o error on the data-plane connection")]
    Io(#[from] io::Error),
}

/// Errors from forming, joining, or maintaining cluster membership.
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    /// No discovery source produced any candidate address before its deadline.
    #[error("no seed peers discovered before the join deadline")]
    NoSeedsFound,
    /// Binding the gossip or data-plane listening socket failed.
    #[error("failed to bind {addr}")]
    Bind {
        /// The address that failed to bind.
        addr: SocketAddr,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// The membership backend failed to start or reported a fatal error.
    #[error("membership backend failed")]
    Membership(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// A zeroconf announce/browse operation failed.
    #[error("discovery error")]
    Discovery(#[source] io::Error),
}

/// Errors from operating on a named [`crate::cache::Cache`].
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// A value (or its postcard-encoded key) exceeded the configured max frame size
    /// and was rejected at the API boundary rather than fragmented.
    #[error("value for cache {cache:?} is {size} bytes, exceeding the {limit}-byte frame cap")]
    ValueTooLarge {
        /// The cache the oversized write targeted.
        cache: SmolStr,
        /// The encoded size in bytes.
        size: usize,
        /// The configured cap it exceeded.
        limit: usize,
    },
    /// The named cache was opened locally with a [`crate::store::Mode`] that conflicts
    /// with how another live node already has it configured.
    #[error("cache {cache:?} mode mismatch: local {local:?}, cluster has {remote:?}")]
    ModeMismatch {
        /// The cache name.
        cache: SmolStr,
        /// The mode requested locally.
        local: crate::store::Mode,
        /// The mode observed elsewhere in the cluster.
        remote: crate::store::Mode,
    },
    /// The named cache is already open in this process. Reopening under a
    /// different key/value type would be unsound (the registry is
    /// type-erased), so a second `open()` for the same name is always
    /// rejected, even when the type matches.
    #[error("cache {cache:?} is already open in this process")]
    AlreadyOpen {
        /// The cache name.
        cache: SmolStr,
    },
    /// A read-through loader returned an error.
    #[error("read-through loader failed")]
    Loader(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Encoding or decoding a key or value for the wire failed.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// State transfer for a newly opened cache failed.
    #[error("state transfer from donor {donor} failed")]
    StateTransfer {
        /// The donor node that was streaming the snapshot.
        donor: NodeId,
        /// The underlying cause.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_carry_useful_display_text() {
        let err = CodecError::FrameTooLarge {
            size: 5_000_000,
            limit: 4 * 1024 * 1024,
        };
        assert!(err.to_string().contains("5000000"));
    }
}
