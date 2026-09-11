//! A PN-Counter: increments and decrements from any number of writers merge
//! to the exact total, with no read before write and no lost updates under
//! concurrent apply.
//!
//! Each node owns exactly one slot in `p` (its cumulative increments) and
//! one in `n` (its cumulative decrements) — the standard PN-Counter
//! decomposition into two G-Counters, each merging by pointwise maximum. A
//! writer's successive writes must each carry its own cumulative total
//! ([`PnCounter::local_delta`]), never a one-shot increment: two independent
//! "+1" deltas from the same node merge to `1` under `max`, silently
//! dropping the second. A writer tracks its own running total (an
//! `AtomicU64` is typical) and calls `local_delta` with the new cumulative
//! value on every local write.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::node::NodeId;
use crate::store::{ConflictResolver, RecordView, Winner};

/// A PN-Counter: per-node cumulative increments and decrements that merge by
/// pointwise maximum, converging to the exact total regardless of apply
/// order, duplication, or how many nodes have ever written to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PnCounter {
    p: BTreeMap<NodeId, u64>,
    n: BTreeMap<NodeId, u64>,
}

impl PnCounter {
    /// The counter's current value: total increments minus total
    /// decrements, across every node that has ever written to it.
    #[must_use]
    pub fn value(&self) -> i64 {
        let increments: u64 = self.p.values().sum();
        let decrements: u64 = self.n.values().sum();
        i64::try_from(increments).unwrap_or(i64::MAX)
            - i64::try_from(decrements).unwrap_or(i64::MAX)
    }

    /// A blind write from `node` carrying its new cumulative increment
    /// total. The caller tracks its own running total and calls this on
    /// every local increment; no read of the shard is needed, and no other
    /// writer's slot is touched.
    #[must_use]
    pub fn local_delta(node: NodeId, cumulative_increment: u64) -> Self {
        Self {
            p: BTreeMap::from([(node, cumulative_increment)]),
            n: BTreeMap::new(),
        }
    }

    /// A blind decrement, the `n`-side counterpart to [`Self::local_delta`]:
    /// `node`'s new cumulative decrement total.
    #[must_use]
    pub fn local_decrement(node: NodeId, cumulative_decrement: u64) -> Self {
        Self {
            p: BTreeMap::new(),
            n: BTreeMap::from([(node, cumulative_decrement)]),
        }
    }

    /// Folds `other` into a new counter: each node's slot in `p` and in `n`
    /// becomes the pointwise maximum of the two sides' value for that node.
    /// Commutative, associative, and idempotent because `max` is, over each
    /// of two totally ordered domains.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            p: pointwise_max(&self.p, &other.p),
            n: pointwise_max(&self.n, &other.n),
        }
    }

    /// Postcard-encodes this counter. `BTreeMap`'s deterministic iteration
    /// order makes the encoding canonical: two counters with the same
    /// content always encode to the same bytes.
    ///
    /// # Errors
    ///
    /// Returns the codec's error if encoding fails.
    pub fn encode(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    /// Decodes a counter from postcard bytes, as produced by [`Self::encode`].
    ///
    /// # Errors
    ///
    /// Returns the codec's error for truncated or malformed bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Slot-wise maximum of two node-keyed totals: the join operation both of
/// [`PnCounter`]'s `p`/`n` sides use.
fn pointwise_max(a: &BTreeMap<NodeId, u64>, b: &BTreeMap<NodeId, u64>) -> BTreeMap<NodeId, u64> {
    let mut merged = a.clone();
    for (&node, &value) in b {
        merged
            .entry(node)
            .and_modify(|existing| *existing = (*existing).max(value))
            .or_insert(value);
    }
    merged
}

/// A [`ConflictResolver`] that merges two [`PnCounter`] values via
/// [`PnCounter::merge`] instead of picking a winner, so concurrent increments
/// and decrements from any number of writers converge to the exact total
/// with no lost updates.
///
/// Falls back to plain [`crate::Hlc`]-order `A`/`B` whenever either side is a
/// tombstone or spill-degraded view (no value to merge) or fails to decode
/// as a [`PnCounter`] — a corrupt or foreign-format record degrades to
/// last-writer-wins rather than stalling replication.
#[derive(Debug, Clone, Copy, Default)]
pub struct PnCounterResolver;

impl ConflictResolver for PnCounterResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return if a.ver >= b.ver { Winner::A } else { Winner::B };
        };
        match (PnCounter::decode(av), PnCounter::decode(bv)) {
            (Ok(pa), Ok(pb)) => match pa.merge(&pb).encode() {
                Ok(bytes) => Winner::Merged {
                    value: Bytes::from(bytes),
                    // A merged counter's slots only ever accumulate, so the
                    // counter as a whole never expires on its own: a TTL
                    // policy for it, if any, belongs to whichever explicit
                    // write set one.
                    expires_at_ms: None,
                },
                Err(_) => {
                    if a.ver >= b.ver {
                        Winner::A
                    } else {
                        Winner::B
                    }
                }
            },
            _ => {
                if a.ver >= b.ver {
                    Winner::A
                } else {
                    Winner::B
                }
            }
        }
    }

    fn merges(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::hlc::Hlc;

    fn hlc(wall_ms: u64, logical: u32) -> Hlc {
        Hlc {
            wall_ms,
            logical,
            node: NodeId::from(1),
        }
    }

    /// A small, mostly-colliding domain for generated counters' node ids, so
    /// merges routinely combine two counters that share slots instead of
    /// trivially union-ing disjoint ones.
    fn node_id() -> impl Strategy<Value = NodeId> {
        (0u64..4).prop_map(NodeId::from)
    }

    fn counter() -> impl Strategy<Value = PnCounter> {
        (
            proptest::collection::btree_map(node_id(), 0u64..1_000, 0..4),
            proptest::collection::btree_map(node_id(), 0u64..1_000, 0..4),
        )
            .prop_map(|(p, n)| PnCounter { p, n })
    }

    #[test]
    fn local_delta_and_value_round_trip() {
        let c = PnCounter::local_delta(NodeId::from(1), 5);
        assert_eq!(c.value(), 5);
    }

    #[test]
    fn local_decrement_lowers_value() {
        let c = PnCounter::local_delta(NodeId::from(1), 5)
            .merge(&PnCounter::local_decrement(NodeId::from(1), 2));
        assert_eq!(c.value(), 3);
    }

    #[test]
    fn encode_decode_round_trips() {
        let c = PnCounter::local_delta(NodeId::from(7), 42);
        let bytes = c.encode().expect("encodes");
        assert_eq!(PnCounter::decode(&bytes).expect("decodes"), c);
    }

    #[test]
    fn a_second_one_shot_delta_from_the_same_node_is_dropped_by_max_merge() {
        // Documents the intentional CRDT semantics `local_delta`'s doc
        // comment warns about: two independent "+1" deltas from the same
        // node, rather than each carrying its cumulative total, merge to 1.
        let first = PnCounter::local_delta(NodeId::from(1), 1);
        let second = PnCounter::local_delta(NodeId::from(1), 1);
        assert_eq!(first.merge(&second).value(), 1);
    }

    proptest! {
        #[test]
        fn merge_is_commutative(a in counter(), b in counter()) {
            prop_assert_eq!(
                a.merge(&b).encode().expect("encodes"),
                b.merge(&a).encode().expect("encodes")
            );
        }

        #[test]
        fn merge_is_idempotent_at_the_byte_level(a in counter()) {
            prop_assert_eq!(
                a.merge(&a).encode().expect("encodes"),
                a.encode().expect("encodes")
            );
        }

        /// Associativity across every one of the six orderings a
        /// three-replica pairwise fold could apply `a`, `b`, `c` in — not
        /// just the two groupings of a single ordering. `apply_locked`
        /// only ever folds one collision at a time, so an N-way concurrent
        /// write converges only if arbitrary fold order and grouping give
        /// the same result; this is the property that would catch a merge
        /// combinator that happens to be associative for one grouping but
        /// not under permutation (e.g. an order-dependent tie-break sneaking
        /// into what should be a pure join).
        #[test]
        fn merge_is_associative_three_way_all_orderings(
            a in counter(), b in counter(), c in counter()
        ) {
            let orderings: [(&PnCounter, &PnCounter, &PnCounter); 6] = [
                (&a, &b, &c),
                (&a, &c, &b),
                (&b, &a, &c),
                (&b, &c, &a),
                (&c, &a, &b),
                (&c, &b, &a),
            ];
            let mut encodings = orderings
                .iter()
                .map(|&(x, y, z)| x.merge(y).merge(z).encode().expect("encodes"));
            let first = encodings.next().expect("six orderings");
            for other in encodings {
                prop_assert_eq!(&first, &other);
            }
        }
    }

    #[test]
    fn resolver_merges_two_decodable_counters_regardless_of_argument_order() {
        let ca = PnCounter::local_delta(NodeId::from(1), 3);
        let cb = PnCounter::local_delta(NodeId::from(2), 4);
        let ba = ca.encode().expect("encodes");
        let bb = cb.encode().expect("encodes");

        let av = RecordView {
            value: Some(&ba),
            ver: hlc(1, 0),
            expires_at_ms: None,
        };
        let bv = RecordView {
            value: Some(&bb),
            ver: hlc(2, 0),
            expires_at_ms: None,
        };

        let resolver = PnCounterResolver;
        let Winner::Merged {
            value: merged_ab, ..
        } = resolver.winner(b"k", av, bv)
        else {
            panic!("expected Winner::Merged when both sides decode");
        };
        let Winner::Merged {
            value: merged_ba, ..
        } = resolver.winner(b"k", bv, av)
        else {
            panic!("expected Winner::Merged when both sides decode");
        };
        assert_eq!(
            merged_ab, merged_ba,
            "merge is commutative in argument order"
        );
        assert_eq!(PnCounter::decode(&merged_ab).expect("decodes").value(), 7);
    }

    #[test]
    fn resolver_falls_back_to_lww_when_a_side_is_a_tombstone() {
        let c = PnCounter::local_delta(NodeId::from(1), 3);
        let encoded = c.encode().expect("encodes");
        let value = RecordView {
            value: Some(&encoded),
            ver: hlc(1, 0),
            expires_at_ms: None,
        };
        let tombstone = RecordView {
            value: None,
            ver: hlc(2, 0),
            expires_at_ms: None,
        };

        // Neither side has both values, so the resolver never merges; it
        // degrades to plain `Hlc` order, and the tombstone (the strictly
        // newer version in both calls below) wins either way.
        let resolver = PnCounterResolver;
        assert_eq!(resolver.winner(b"k", value, tombstone), Winner::B);
        assert_eq!(resolver.winner(b"k", tombstone, value), Winner::A);
    }

    #[test]
    fn resolver_falls_back_to_lww_on_decode_failure() {
        let c = PnCounter::local_delta(NodeId::from(1), 3);
        let encoded = c.encode().expect("encodes");
        let malformed = [0xffu8; 6];

        let good = RecordView {
            value: Some(&encoded),
            ver: hlc(1, 0),
            expires_at_ms: None,
        };
        let bad = RecordView {
            value: Some(&malformed),
            ver: hlc(2, 0),
            expires_at_ms: None,
        };

        let resolver = PnCounterResolver;
        // `bad` fails to decode as a `PnCounter`, so the resolver degrades
        // to plain `Hlc` order rather than merging or panicking; `bad`'s
        // strictly newer version wins.
        assert_eq!(resolver.winner(b"k", good, bad), Winner::B);
        assert_eq!(resolver.winner(b"k", bad, good), Winner::A);
    }

    #[test]
    fn needs_value_bytes_is_true() {
        assert!(PnCounterResolver.needs_value_bytes());
    }

    #[test]
    fn merges_is_true() {
        assert!(
            PnCounterResolver.merges(),
            "PnCounterResolver returns Winner::Merged, so it must advertise merges()"
        );
    }
}
