//! Domain error types. One `thiserror` enum per fallible area, per house style.

use std::io;
use std::net::SocketAddr;

use smol_str::SmolStr;

use crate::node::NodeId;

/// Errors from encoding or decoding a [`crate::wire::Msg`] or
/// [`crate::wire::WireRecord`].
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The frame exceeded [`crate::wire::MAX_FRAME`].
    #[error("frame of {size} bytes exceeds the {limit}-byte cap")]
    FrameTooLarge { size: usize, limit: usize },
    /// postcard failed to serialize or deserialize a value.
    #[error("postcard codec error")]
    Postcard(#[from] postcard::Error),
    /// A raw-record frame (`Replicate`/`ReplicateBatch`/`StChunk`) failed to
    /// parse: a header didn't fit, a length pointed past the frame's end, or
    /// the cache name wasn't valid UTF-8.
    #[error("malformed record frame: {0}")]
    MalformedFrame(&'static str),
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
    /// A [`crate::config::ClusterConfig`] field failed validation.
    #[error("invalid cluster config: {0}")]
    InvalidConfig(String),
}

/// Errors from operating on a named [`crate::cache::Cache`].
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// A value or key exceeded the configured max frame size and was
    /// rejected at the API boundary rather than fragmented.
    #[error("value for cache {cache:?} is {size} bytes, exceeding the {limit}-byte frame cap")]
    ValueTooLarge {
        /// The cache the oversized write targeted.
        cache: SmolStr,
        size: usize,
        /// The configured cap it exceeded.
        limit: usize,
    },
    /// The named cache was opened locally with a [`crate::store::Mode`] that
    /// conflicts with how another live node already has it configured. This
    /// check is best-effort: two nodes opening the same name at nearly the
    /// same moment can both pass it, so `cluster` re-checks on every
    /// membership view and logs whatever this check missed.
    #[error("cache {cache:?} mode mismatch: local {local:?}, cluster has {remote:?}")]
    ModeMismatch {
        cache: SmolStr,
        /// The mode requested locally.
        local: crate::store::Mode,
        /// The mode observed elsewhere.
        remote: crate::store::Mode,
    },
    /// The cache was opened as [`crate::store::Mode::Replicated`] with a
    /// finite `max_capacity` or `tti`. Every `Replicated` node holds every
    /// entry, so anti-entropy would silently re-pull an evicted entry back.
    /// Use [`crate::store::Mode::Invalidation`] for a bounded local cache.
    #[error(
        "cache {cache:?} combines Mode::Replicated with a finite max_capacity or tti, which anti-entropy would silently defeat"
    )]
    ReplicatedWithLocalEviction { cache: SmolStr },
    /// The named cache is already open in this process. The registry is
    /// type-erased, so a second `open()` is always rejected, even when the
    /// key/value types match.
    #[error("cache {cache:?} is already open in this process")]
    AlreadyOpen { cache: SmolStr },
    /// A read-through loader returned an error.
    #[error("read-through loader failed")]
    Loader(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Encoding or decoding a key or value for the wire failed.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// State transfer for a newly opened cache failed.
    #[error("state transfer from donor {donor} failed")]
    StateTransfer {
        donor: NodeId,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// The cache's [`crate::store::spill::SpillConfig`] failed validation at
    /// `open()`: `region_bytes` was zero, or `capacity_bytes` was less than
    /// two regions.
    #[cfg(feature = "spill")]
    #[error("cache {cache:?} has an invalid spill config: {reason}")]
    InvalidSpillConfig {
        cache: SmolStr,
        /// Why the config was rejected.
        reason: &'static str,
    },
    /// The cache's spill directory could not be created, or a region file
    /// under it could not be created.
    #[cfg(feature = "spill")]
    #[error("cache {cache:?} could not open its spill directory")]
    SpillUnavailable {
        cache: SmolStr,
        #[source]
        source: io::Error,
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
