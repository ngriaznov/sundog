//! Writer identity for the CRDT types in [`super`].
//!
//! A counter slot ([`super::PnCounter`]) or an OR-set tag ([`super::OrSet`])
//! is keyed by a [`WriterId`], not by [`NodeId`] alone: pairing the node
//! with its current membership incarnation means a restarted node writes
//! from a fresh slot starting at zero, and a retired incarnation can never
//! receive a later write from its own process. The incarnation is the one
//! membership already tracks (`now_incarnation_ms()` in
//! `crate::membership`), so this is a new key shape over existing data,
//! not a new wire field.

use serde::{Deserialize, Serialize};

use crate::node::NodeId;

/// The identity of one writer to a [`super::PnCounter`] slot or
/// [`super::OrSet`] tag: a node paired with the membership incarnation it
/// wrote under. Two `WriterId`s with the same [`NodeId`] but different
/// incarnations are different writers for every purpose the CRDT types
/// care about, so a restarted node never resumes or corrupts what it
/// wrote before restarting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WriterId {
    node: NodeId,
    incarnation: u64,
}

impl WriterId {
    /// A writer identity for `node` under `incarnation`.
    #[must_use]
    pub fn new(node: NodeId, incarnation: u64) -> Self {
        Self { node, incarnation }
    }

    /// The node this writer identity belongs to.
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The membership incarnation this writer identity is minted under.
    #[must_use]
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use super::WriterId;
    use crate::node::NodeId;

    #[test]
    fn constructor_and_accessors_round_trip() {
        let w = WriterId::new(NodeId::from(7), 42);
        assert_eq!(w.node(), NodeId::from(7));
        assert_eq!(w.incarnation(), 42);
    }

    #[test]
    fn equality_requires_both_node_and_incarnation_to_match() {
        let base = WriterId::new(NodeId::from(1), 10);
        assert_eq!(base, WriterId::new(NodeId::from(1), 10));
        assert_ne!(
            base,
            WriterId::new(NodeId::from(1), 11),
            "same node, different incarnation must be a different writer"
        );
        assert_ne!(
            base,
            WriterId::new(NodeId::from(2), 10),
            "same incarnation, different node must be a different writer"
        );
    }

    #[test]
    fn is_copy() {
        let w = WriterId::new(NodeId::from(3), 5);
        let copy = w;
        // Using both after the copy proves `Copy`, not only `Clone`: a move
        // would make this a compile error.
        assert_eq!(w, copy);
    }

    #[test]
    fn ord_compares_node_before_incarnation() {
        let a = WriterId::new(NodeId::from(1), 999);
        let b = WriterId::new(NodeId::from(2), 0);
        assert!(a < b, "lower node id sorts first regardless of incarnation");

        let c = WriterId::new(NodeId::from(1), 1);
        let d = WriterId::new(NodeId::from(1), 2);
        assert!(c < d, "same node: lower incarnation sorts first");
    }

    #[test]
    fn usable_as_a_map_key() {
        let mut set: BTreeSet<WriterId> = BTreeSet::new();
        set.insert(WriterId::new(NodeId::from(1), 0));
        set.insert(WriterId::new(NodeId::from(1), 1));
        set.insert(WriterId::new(NodeId::from(1), 0)); // duplicate
        assert_eq!(set.len(), 2);

        let mut hash_set: HashSet<WriterId> = HashSet::new();
        hash_set.insert(WriterId::new(NodeId::from(2), 5));
        hash_set.insert(WriterId::new(NodeId::from(2), 5));
        assert_eq!(hash_set.len(), 1);
    }

    #[test]
    fn serde_round_trips_through_postcard() {
        let w = WriterId::new(NodeId::from(99), 123_456);
        let bytes = postcard::to_stdvec(&w).expect("encodes");
        let decoded: WriterId = postcard::from_bytes(&bytes).expect("decodes");
        assert_eq!(w, decoded);
    }

    #[test]
    fn a_restarted_node_gets_a_distinct_writer_id_from_a_fresh_incarnation() {
        // Same node, new incarnation after a restart: a different writer.
        let before_restart = WriterId::new(NodeId::from(4), 1_000);
        let after_restart = WriterId::new(NodeId::from(4), 2_000);
        assert_ne!(before_restart, after_restart);
        assert_eq!(before_restart.node(), after_restart.node());
    }
}
