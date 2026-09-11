//! An observed-remove set: [`OrSet`] and the [`OrSetResolver`] that merges
//! it through [`crate::store::Winner::Merged`].
//!
//! Built on `BTreeMap`/`BTreeSet` rather than a hash-based collection, so
//! its postcard encoding is canonical: two logically equal sets always
//! encode to identical bytes. That property is what lets the merge be
//! checked for idempotence and associativity at the byte level.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::node::NodeId;
use crate::store::{ConflictResolver, RecordView, Winner};

/// A tag uniquely identifying one [`OrSet::add`]: the writer's node id
/// paired with a sequence number local to that writer. No two adds, from
/// the same or different writers, ever share a tag.
type Tag = (NodeId, u64);

/// An observed-remove set: adds and removes from any number of writers
/// merge to the set reflecting every add whose tag no remover has
/// observed. A concurrent add of an element survives a concurrent remove
/// of that same element, because the remove can only tombstone tags it has
/// actually seen — a fresh tag from a concurrent add was never among them.
///
/// Removing an element is an ordinary mutation of the stored value, not a
/// whole-key delete: [`OrSet::remove`] produces a tombstone-only delta
/// meant to be written back through an ordinary `Put` (a normal
/// `Cache::insert`), the same way a [`super::PnCounter`] delta is. It never
/// goes through `Cache::remove`, which discards the entire logical set as a
/// real tombstone — a value [`crate::store::Winner::Merged`] can never be computed
/// against, since the engine rejects any merge where either side carries no
/// value. A real per-key delete stays a real delete, decided by plain
/// last-writer-wins, never by this resolver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrSet<T>
where
    T: Ord + Clone,
{
    adds: BTreeMap<Tag, T>,
    tombstones: BTreeSet<Tag>,
}

impl<T> OrSet<T>
where
    T: Ord + Clone + Serialize + DeserializeOwned,
{
    /// The set's live members: every added element whose tag has not been
    /// tombstoned by an observed [`Self::remove`].
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.adds
            .iter()
            .filter(|(tag, _)| !self.tombstones.contains(tag))
            .map(|(_, element)| element)
    }

    /// True when `element` has at least one live (non-tombstoned) add tag.
    #[must_use]
    pub fn contains(&self, element: &T) -> bool {
        self.iter().any(|live| live == element)
    }

    /// A blind add: `node` tags `element` with its own next sequence number
    /// (`seq`), unique across every add this writer has ever made. The
    /// caller tracks its own running sequence counter (an `AtomicU64` is
    /// typical) and calls this on every local add; no read of the shard is
    /// needed.
    #[must_use]
    pub fn add(node: NodeId, seq: u64, element: T) -> Self {
        Self {
            adds: BTreeMap::from([((node, seq), element)]),
            tombstones: BTreeSet::new(),
        }
    }

    /// An observed-remove: tombstones every tag in `observed` — a snapshot
    /// of the set this replica has actually read — that backs `element`.
    /// Because only tags this replica has seen are ever tombstoned, a
    /// concurrent add of the same element on another replica, whose fresh
    /// tag this replica could not have observed, survives the merge: the
    /// defining add-wins property of an observed-remove set.
    ///
    /// Like [`Self::add`], the result is a delta meant to be written back
    /// as an ordinary `Put`, merged into the stored set by
    /// [`OrSetResolver`] — never a whole-key `Cache::remove`.
    #[must_use]
    pub fn remove(observed: &Self, element: &T) -> Self {
        let dead: BTreeSet<Tag> = observed
            .adds
            .iter()
            .filter(|(_, e)| *e == element)
            .map(|(&tag, _)| tag)
            .collect();
        Self {
            adds: BTreeMap::new(),
            tombstones: dead,
        }
    }

    /// Folds `other` into a new set: the union of both sides' adds and both
    /// sides' tombstones. Commutative, associative, and idempotent because
    /// map/set union is. A tag present on both sides always carries the
    /// same element (tags are unique per add, minted once, and never
    /// reused), so which side's clone of the element survives the union is
    /// immaterial.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        let mut adds = self.adds.clone();
        for (tag, element) in &other.adds {
            adds.entry(*tag).or_insert_with(|| element.clone());
        }
        let tombstones = &self.tombstones | &other.tombstones;
        Self { adds, tombstones }
    }

    /// Postcard-encodes this set. `BTreeMap`/`BTreeSet`'s deterministic
    /// iteration order makes the encoding canonical: two sets with the same
    /// content always encode to the same bytes.
    ///
    /// # Errors
    ///
    /// Returns the codec's error if encoding fails.
    pub fn encode(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    /// Decodes a set from postcard bytes, as produced by [`Self::encode`].
    ///
    /// # Errors
    ///
    /// Returns the codec's error for truncated or malformed bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// A [`ConflictResolver`] that merges two [`OrSet<T>`] values via
/// [`OrSet::merge`] instead of picking a winner, so concurrent adds and
/// removes from any number of writers converge to the add-wins
/// observed-remove result.
///
/// Falls back to plain [`crate::Hlc`]-order `A`/`B` whenever either side is
/// a tombstone or spill-degraded view (no value to merge) or fails to
/// decode as an [`OrSet<T>`] — a corrupt or foreign-format record degrades
/// to last-writer-wins rather than stalling replication.
///
/// Carries no state of its own — `T` only selects which element type this
/// resolver decodes as — so it is `Send`/`Sync`/`Copy` regardless of `T`.
pub struct OrSetResolver<T> {
    element_type: PhantomData<fn() -> T>,
}

impl<T> OrSetResolver<T> {
    /// Builds a resolver for `OrSet<T>`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            element_type: PhantomData,
        }
    }
}

impl<T> Default for OrSetResolver<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for OrSetResolver<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for OrSetResolver<T> {}

impl<T> fmt::Debug for OrSetResolver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrSetResolver").finish()
    }
}

impl<T> ConflictResolver for OrSetResolver<T>
where
    T: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return if a.ver >= b.ver { Winner::A } else { Winner::B };
        };
        match (OrSet::<T>::decode(av), OrSet::<T>::decode(bv)) {
            (Ok(sa), Ok(sb)) => match sa.merge(&sb).encode() {
                Ok(bytes) => Winner::Merged {
                    value: Bytes::from(bytes),
                    // A merged set's tags only ever accumulate (adds and
                    // tombstones alike), so the set as a whole never
                    // expires on its own: a TTL policy for it, if any,
                    // belongs to whichever explicit write set one.
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

    /// A small, mostly-colliding domain for generated tags' node ids, so
    /// merges routinely combine two sets that share tags instead of
    /// trivially union-ing disjoint ones.
    fn node_id() -> impl Strategy<Value = NodeId> {
        (0u64..4).prop_map(NodeId::from)
    }

    fn tag() -> impl Strategy<Value = Tag> {
        (node_id(), 0u64..4)
    }

    /// The element a given tag adds, in the generated test data — a
    /// deterministic function of the tag alone, so any two independently
    /// generated sets that happen to share a tag necessarily agree on its
    /// element, matching the real invariant (a tag is minted once, for one
    /// element, and never reused for another). Without this, a randomly
    /// generated pair of conflicting elements at the same tag would make
    /// `merge`'s tag-collision tie-break order-dependent, and the
    /// commutativity/associativity properties below would fail for a
    /// reason that has nothing to do with `merge`'s actual correctness.
    fn tagged_element(tag: Tag) -> String {
        match tag.1 % 3 {
            0 => "a",
            1 => "b",
            _ => "c",
        }
        .to_string()
    }

    fn adds_map() -> impl Strategy<Value = BTreeMap<Tag, String>> {
        proptest::collection::btree_set(tag(), 0..4)
            .prop_map(|tags| tags.into_iter().map(|t| (t, tagged_element(t))).collect())
    }

    fn tombstones_set() -> impl Strategy<Value = BTreeSet<Tag>> {
        proptest::collection::btree_set(tag(), 0..4)
    }

    fn or_set() -> impl Strategy<Value = OrSet<String>> {
        (adds_map(), tombstones_set()).prop_map(|(adds, tombstones)| OrSet { adds, tombstones })
    }

    #[test]
    fn add_and_contains() {
        let s = OrSet::add(NodeId::from(1), 0, "x".to_string());
        assert!(s.contains(&"x".to_string()));
        assert!(!s.contains(&"y".to_string()));
    }

    #[test]
    fn iter_returns_live_members_only() {
        let s = OrSet::add(NodeId::from(1), 0, "x".to_string());
        let removed = OrSet::remove(&s, &"x".to_string());
        let merged = s.merge(&removed);
        assert_eq!(merged.iter().collect::<Vec<_>>(), Vec::<&String>::new());
    }

    #[test]
    fn remove_only_tombstones_observed_tags() {
        // Two independent adds of the same element, from two different
        // writers, get two different tags. Observing only the first add
        // and removing it must not affect the second's tag.
        let first = OrSet::add(NodeId::from(1), 0, "x".to_string());
        let second = OrSet::add(NodeId::from(2), 0, "x".to_string());
        let both = first.merge(&second);

        let removed = OrSet::remove(&first, &"x".to_string());
        let merged = both.merge(&removed);
        assert!(
            merged.contains(&"x".to_string()),
            "the second writer's untouched tag keeps `x` live"
        );
    }

    #[test]
    fn merge_unions_adds_and_tombstones() {
        let a = OrSet::add(NodeId::from(1), 0, "x".to_string());
        let b = OrSet::add(NodeId::from(2), 0, "y".to_string());
        let merged = a.merge(&b);
        assert!(merged.contains(&"x".to_string()));
        assert!(merged.contains(&"y".to_string()));
    }

    #[test]
    fn encode_decode_round_trips() {
        let s = OrSet::add(NodeId::from(7), 3, "x".to_string());
        let bytes = s.encode().expect("encodes");
        assert_eq!(OrSet::decode(&bytes).expect("decodes"), s);
    }

    proptest! {
        #[test]
        fn merge_is_commutative(a in or_set(), b in or_set()) {
            prop_assert_eq!(
                a.merge(&b).encode().expect("encodes"),
                b.merge(&a).encode().expect("encodes")
            );
        }

        #[test]
        fn merge_is_idempotent_at_the_byte_level(a in or_set()) {
            prop_assert_eq!(
                a.merge(&a).encode().expect("encodes"),
                a.encode().expect("encodes")
            );
        }

        /// Associativity across every one of the six orderings a
        /// three-replica pairwise fold could apply `a`, `b`, `c` in — not
        /// just the two groupings of a single ordering. A real apply path
        /// only ever folds one collision at a time, so an N-way concurrent
        /// write converges only if arbitrary fold order and grouping give
        /// the same result.
        #[test]
        fn merge_is_associative_three_way_all_orderings(
            a in or_set(), b in or_set(), c in or_set()
        ) {
            let orderings: [(&OrSet<String>, &OrSet<String>, &OrSet<String>); 6] = [
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

        /// The add-wins observed-remove property itself: a remove can only
        /// tombstone tags it has actually observed, so a concurrent add of
        /// the same element under a fresh, unobserved tag survives the
        /// merge regardless of apply order.
        #[test]
        fn concurrent_add_of_the_same_element_survives_a_concurrent_remove(
            node_a in node_id(), seq_a in 0u64..1_000,
            node_b in node_id(), seq_b in 0u64..1_000,
        ) {
            prop_assume!((node_a, seq_a) != (node_b, seq_b));
            let element = "x".to_string();

            let base = OrSet::add(node_a, seq_a, element.clone());
            // The remover observes only `base` — never the concurrent add.
            let removed = OrSet::remove(&base, &element);
            let concurrent_add = OrSet::add(node_b, seq_b, element.clone());

            let merged_remove_then_add = base.merge(&removed).merge(&concurrent_add);
            let merged_add_then_remove = base.merge(&concurrent_add).merge(&removed);

            prop_assert!(merged_remove_then_add.contains(&element));
            prop_assert!(merged_add_then_remove.contains(&element));
            prop_assert_eq!(
                merged_remove_then_add.encode().expect("encodes"),
                merged_add_then_remove.encode().expect("encodes")
            );
        }
    }

    #[test]
    fn resolver_merges_two_decodable_sets_regardless_of_argument_order() {
        let sa = OrSet::add(NodeId::from(1), 0, "x".to_string());
        let sb = OrSet::add(NodeId::from(2), 0, "y".to_string());
        let ba = sa.encode().expect("encodes");
        let bb = sb.encode().expect("encodes");

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

        let resolver = OrSetResolver::<String>::new();
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
        let merged = OrSet::<String>::decode(&merged_ab).expect("decodes");
        assert!(merged.contains(&"x".to_string()));
        assert!(merged.contains(&"y".to_string()));
    }

    #[test]
    fn resolver_falls_back_to_lww_when_a_side_is_a_tombstone() {
        let s = OrSet::add(NodeId::from(1), 0, "x".to_string());
        let encoded = s.encode().expect("encodes");
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

        let resolver = OrSetResolver::<String>::default();
        assert_eq!(resolver.winner(b"k", value, tombstone), Winner::B);
        assert_eq!(resolver.winner(b"k", tombstone, value), Winner::A);
    }

    #[test]
    fn resolver_falls_back_to_lww_on_decode_failure() {
        let s = OrSet::add(NodeId::from(1), 0, "x".to_string());
        let encoded = s.encode().expect("encodes");
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

        let resolver = OrSetResolver::<String>::new();
        assert_eq!(resolver.winner(b"k", good, bad), Winner::B);
        assert_eq!(resolver.winner(b"k", bad, good), Winner::A);
    }

    #[test]
    fn needs_value_bytes_is_true() {
        assert!(OrSetResolver::<String>::new().needs_value_bytes());
    }
}
