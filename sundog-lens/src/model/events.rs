//! The events the model raises as the cluster changes.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::store::Mode;

/// One event: what happened and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The wall-clock time of the update that raised the event.
    pub at: SystemTime,
    /// What happened.
    pub kind: EventKind,
}

/// What happened. A node is named by its id and gossip address; the view
/// resolves them to a slot label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    /// `JOIN`: a new member appeared.
    Join {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
        /// Its wire protocol.
        protocol: u16,
        /// The caches it advertises.
        caches: BTreeMap<SmolStr, Mode>,
    },
    /// `LEAVE`: a member gossips a graceful departure.
    Leave {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
    },
    /// `LEFT`: a departing member is gone.
    Left {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
    },
    /// `DOWN`: a live member dropped with no departure.
    Down {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
        /// How long its exporter had been silent, when known.
        exporter_silent: Option<Duration>,
    },
    /// `REJOIN`: a new node id at a known gossip address.
    Rejoin {
        /// The new node.
        node: NodeId,
        /// The shared gossip address.
        addr: SocketAddr,
        /// The node that held the address before.
        previous: NodeId,
    },
    /// `RESTART`: the same node id at a higher incarnation.
    Restart {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
    },
    /// `CACHE+`: a live member opened a cache.
    CacheAdded {
        /// The member.
        node: NodeId,
        /// The cache name.
        cache: SmolStr,
        /// Its mode.
        mode: Mode,
    },
    /// `CACHE-`: a live member closed a cache.
    CacheRemoved {
        /// The member.
        node: NodeId,
        /// The cache name.
        cache: SmolStr,
    },
    /// `CONFLICT`: live members advertise different modes for one cache.
    Conflict {
        /// The cache name.
        cache: SmolStr,
        /// Each live advertiser and its mode.
        modes: Vec<(NodeId, Mode)>,
    },
    /// `PROTO`: a member speaks a protocol other than the lens's, or below
    /// the minimum.
    Proto {
        /// The member.
        node: NodeId,
        /// Its protocol.
        protocol: u16,
    },
    /// `VIEW`: a cache's ownership view changed.
    View {
        /// The cache name.
        cache: SmolStr,
        /// The previous view hash, if there was one.
        from: Option<u64>,
        /// The new view hash.
        to: u64,
        /// Owner slots that changed hands.
        moved: usize,
        /// Each node's change in parts owned.
        deltas: Vec<(NodeId, i64)>,
    },
    /// `SETTLED`: a cache settled after a view change.
    Settled {
        /// The cache name.
        cache: SmolStr,
        /// Time from the view change to settling.
        took: Duration,
    },
    /// `XFER`: a node's state-transfer rate became nonzero.
    Xfer {
        /// The node.
        node: NodeId,
    },
    /// `DROP`: a node dropped frames bound for a peer.
    Drop {
        /// The sending node.
        node: NodeId,
        /// The peer's node id as the exporter labels it.
        peer: SmolStr,
        /// How many frames.
        frames: u64,
    },
    /// `READY`: a node's `/readyz` turned ready.
    Ready {
        /// The node.
        node: NodeId,
    },
    /// `UNREADY`: a node's `/readyz` turned not ready.
    Unready {
        /// The node.
        node: NodeId,
    },
    /// `UNREACHABLE`: scrapes fail while gossip lists the node live.
    Unreachable {
        /// The node.
        node: NodeId,
    },
    /// `EXPORTER`: an exporter came or went, or its mapping is wrong.
    Exporter {
        /// The gossip address of the mapped member.
        addr: SocketAddr,
        /// What happened.
        detail: String,
    },
}
