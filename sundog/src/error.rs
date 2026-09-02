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
    /// A raw-record frame (`Replicate`/`ReplicateBatch`/`StChunk`, see
    /// `crate::wire`'s module docs) failed to parse: a header didn't fit,
    /// a length field pointed past the frame's end, the cache name wasn't
    /// valid UTF-8, or a length exceeded what the layout's fixed-width
    /// fields can hold.
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
    ///
    /// Every opened cache gossips its own name and [`crate::store::Mode`] as
    /// a `cache:<name>` chitchat key (`membership`'s cache-mode
    /// fingerprint). [`crate::cache::CacheBuilder::open`] checks the
    /// requested mode against every live peer's advertised fingerprint for
    /// the same name before
    /// registering the shard, and returns this error on a mismatch. That
    /// check only sees gossip that has already converged, so two nodes
    /// opening the same name under different modes at nearly the same
    /// moment can both pass it — `cluster`'s membership-change handling
    /// re-checks the live peer set against this node's shard registry on
    /// every view change as a background backstop, logging (not tearing
    /// down) whatever this constructor-time check missed.
    #[error("cache {cache:?} mode mismatch: local {local:?}, cluster has {remote:?}")]
    ModeMismatch {
        /// The cache name.
        cache: SmolStr,
        /// The mode requested locally.
        local: crate::store::Mode,
        /// The mode observed elsewhere in the cluster.
        remote: crate::store::Mode,
    },
    /// The cache was opened as [`crate::store::Mode::Replicated`] with a
    /// finite `max_capacity` or `tti`. Replicated mode expects every node to
    /// hold every entry; anti-entropy would silently re-pull back any entry
    /// evicted locally for capacity or idle reasons from a peer that still
    /// holds it, defeating the bound. Use [`crate::store::Mode::Invalidation`]
    /// for a bounded local cache, or leave `max_capacity`/`tti` unset for a
    /// `Replicated` one.
    #[error(
        "cache {cache:?} combines Mode::Replicated with a finite max_capacity or tti, which anti-entropy would silently defeat"
    )]
    ReplicatedWithLocalEviction {
        /// The cache name.
        cache: SmolStr,
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
