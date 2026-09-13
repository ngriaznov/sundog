//! Constructors shared by the store's test modules: `engine`, `crdt::pn_counter`,
//! and `crdt::or_set` each built their own `hlc`/`wid` before this module
//! existed; `pn_counter` and `or_set` now share the ones here.

use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::store::crdt::WriterId;

/// An [`Hlc`] at `wall_ms`/`logical`, node fixed to `1`.
pub(crate) fn hlc(wall_ms: u64, logical: u32) -> Hlc {
    Hlc {
        wall_ms,
        logical,
        node: NodeId::from(1),
    }
}

/// A [`WriterId`] for `node` at `incarnation`.
pub(crate) fn wid(node: u64, incarnation: u64) -> WriterId {
    WriterId::new(NodeId::from(node), incarnation)
}
