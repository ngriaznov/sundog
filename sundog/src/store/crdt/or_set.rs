//! An observed-remove set: [`OrSet`] and the [`OrSetResolver`] that merges
//! it through [`crate::store::ConflictResolver::merge`].
//!
//! Built on `BTreeMap`/`BTreeSet`, so its postcard encoding is canonical:
//! two logically equal sets always encode to identical bytes, letting the
//! merge be checked for idempotence and associativity at the byte level.
//!
//! Every tag is keyed by a [`WriterId`] (node plus membership incarnation),
//! so a restarted node's fresh incarnation never collides with its prior
//! one's tag sequence. [`OrSet::remove`] records removal as a per-writer
//! `seen` watermark, not a per-tag tombstone. Compaction retires a writer
//! in two stages: stage one marks it `retired`; stage two, once quiet and
//! retired long enough, drops `seen`/`retired` and leaves a `folded_at`
//! receipt so [`OrSet::merge`] can tell an already-folded writer from mere
//! silence about one. A tag from a retired or folded writer is rejected
//! outright; retirement is sticky here, unlike [`super::PnCounter`].
//! `OrSet::prune_receipts` drops a receipt past `receipt_ttl_ms`, under the
//! same trust boundary `tombstone_max_ttl` accepts for a long-gone member.
//!
//! A tag `(w, s)` absent from a side's own `adds` is dead to that side if
//! `w` is retired or folded there, or `s` does not exceed that side's
//! `seen` watermark for `w`; a writer with no `seen` entry is never
//! treated as an implicit watermark of `0`, since sequences legitimately
//! start there too.
//!
//! [`OrSet::encode`]/[`OrSet::decode`] are thin postcard wrappers around
//! this type's one wire layout; a future change to it is versioned.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::WriterId;
use crate::store::{CompactionBounds, ConflictResolver, Merged, RecordView, Winner};

/// A tag uniquely identifying one [`OrSet::add`]: the writer's identity
/// paired with a sequence number local to that writer's own incarnation. No
/// two adds, from the same or different writers, ever share a tag.
type Tag = (WriterId, u64);

/// An observed-remove set: adds and removes from any number of writers
/// merge to the set reflecting every add whose tag no remover has
/// observed, since a remove only marks dead the tags it has seen. Use
/// [`OrSet::remove`] through `Cache::insert`, never `Cache::remove`'s
/// whole-key tombstone; see the module doc for two-stage retirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrSet<T>
where
    T: Ord + Clone,
{
    adds: BTreeMap<Tag, T>,
    /// Per-writer version-vector watermark: every tag `(w, s)` with
    /// `s <= seen[w]` that this replica does not itself hold in `adds` is
    /// dead. Populated by [`OrSet::remove`], and left untouched by stage one
    /// of [`OrSet::compact`]; only stage two, once a retired writer has
    /// aged out, drops its entry.
    seen: BTreeMap<WriterId, u64>,
    /// Writers this replica has retired, keyed by the wall-clock
    /// millisecond retirement happened (stage one). A writer in this map
    /// can never contribute a *new* live tag to this replica or any replica
    /// this one merges into; see the module doc's deadness rule. Stage two
    /// drops an entry (leaving a `folded_at` receipt behind) once it has
    /// aged past twice `crdt_retire_after` and the cache is quiet.
    retired: BTreeMap<WriterId, u64>,
    /// A per-writer receipt: `w`'s own `since_ms` the moment this replica
    /// drops `w`'s `seen`/`retired` entries in stage two. [`Self::merge`]
    /// trusts a side's silence about `w` as "already folded" only once its
    /// own `folded_at` names `w` with a `since_ms` at least as late as the
    /// merged `retired` entry's. Dropped once old enough that every
    /// reachable replica has independently folded the same writer too; see
    /// [`Self::compact`].
    folded_at: BTreeMap<WriterId, u64>,
}

impl<T> OrSet<T>
where
    T: Ord + Clone + Serialize + DeserializeOwned,
{
    /// The set's live members: every added element whose tag is not dead,
    /// i.e. not covered by any writer's `seen` watermark. Since `compact`
    /// never touches `adds` (see the module doc), this is every tag this
    /// replica's own `adds` still holds, independent of retirement.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.adds.values()
    }

    /// True when `element` has at least one live add tag.
    #[must_use]
    pub fn contains(&self, element: &T) -> bool {
        self.iter().any(|live| live == element)
    }

    /// A blind add: `writer` tags `element` with its own next sequence
    /// number (`seq`), unique across every add its incarnation has ever
    /// made. The caller tracks its own running sequence counter (an
    /// `AtomicU64` is typical, reset on every fresh incarnation); no read
    /// of the shard is needed. A retired `writer` can still call this, but
    /// the tag never becomes live wherever it merges into a replica that
    /// has retired `writer`; see the module doc's deadness rule.
    #[must_use]
    pub fn add(writer: WriterId, seq: u64, element: T) -> Self {
        Self {
            adds: BTreeMap::from([((writer, seq), element)]),
            seen: BTreeMap::new(),
            retired: BTreeMap::new(),
            folded_at: BTreeMap::new(),
        }
    }

    /// An observed-remove: raises each touched writer's `seen` watermark to
    /// cover every tag in `observed` backing `element`, so only an
    /// observed tag is marked dead, and re-asserts that writer's other
    /// tags as live adds, since a watermark cannot spare a lower sequence.
    /// Write back via `Cache::insert`, never `Cache::remove`.
    #[must_use]
    pub fn remove(observed: &Self, element: &T) -> Self {
        let touched: BTreeSet<WriterId> = observed
            .adds
            .iter()
            .filter(|(_, e)| *e == element)
            .map(|(&(writer, _), _)| writer)
            .collect();

        let mut seen: BTreeMap<WriterId, u64> = BTreeMap::new();
        let mut adds: BTreeMap<Tag, T> = BTreeMap::new();
        for (&(writer, seq), e) in &observed.adds {
            if !touched.contains(&writer) {
                continue;
            }
            seen.entry(writer)
                .and_modify(|m: &mut u64| *m = (*m).max(seq))
                .or_insert(seq);
            if e != element {
                adds.insert((writer, seq), e.clone());
            }
        }
        Self {
            adds,
            seen,
            retired: BTreeMap::new(),
            folded_at: BTreeMap::new(),
        }
    }

    /// Folds `other` into a new set. `seen` merges by pointwise maximum;
    /// `retired` takes, for each writer either side retired, the
    /// *earliest* `since_ms` recorded; `folded_at` is the union, taking the
    /// *later* `since_ms` for a writer both sides have a receipt for. A
    /// writer named by the merged `retired` map is dropped from
    /// `seen`/`retired` entirely, rather than resurrected, once the
    /// *other* side's own `folded_at` names it with a `since_ms` at least
    /// as late; see the module doc for why a receipt, not mere silence, is
    /// what licenses that.
    ///
    /// A tag survives if either side's own `adds` still holds it, or it is
    /// live by the module doc's deadness rule evaluated against the
    /// *other* operand's pre-merge state. `merge_is_commutative`,
    /// `merge_is_idempotent_at_the_byte_level`, and
    /// `merge_never_resurrects_a_removed_element_solely_because_an_unrelated_writer_folded_later`
    /// prove this stays exact and lattice-lawful.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        let mut retired: BTreeMap<WriterId, u64> = self.retired.clone();
        for (&writer, &since_ms) in &other.retired {
            retired
                .entry(writer)
                .and_modify(|m| *m = (*m).min(since_ms))
                .or_insert(since_ms);
        }

        let mut seen: BTreeMap<WriterId, u64> = BTreeMap::new();
        for writer in self.seen.keys().chain(other.seen.keys()) {
            let m = self
                .seen
                .get(writer)
                .copied()
                .unwrap_or(0)
                .max(other.seen.get(writer).copied().unwrap_or(0));
            seen.insert(*writer, m);
        }

        let mut folded_at: BTreeMap<WriterId, u64> = self.folded_at.clone();
        for (&writer, &since_ms) in &other.folded_at {
            folded_at
                .entry(writer)
                .and_modify(|m| *m = (*m).max(since_ms))
                .or_insert(since_ms);
        }

        // A side that has already folded `w` away: no `seen`, no `retired`,
        // and a per-writer receipt naming `w` specifically, with no
        // `since_ms` comparison needed. A `WriterId` names one specific
        // membership incarnation, and an incarnation dies at most once
        // ever, so a receipt for `w` and any `retired`/`seen` evidence of
        // `w` can only ever refer to that same, singular death, never a
        // different, later one the receipt would need to be "caught up"
        // to.
        let dropped_by = |side: &Self, w: WriterId| -> bool {
            !side.seen.contains_key(&w)
                && !side.retired.contains_key(&w)
                && side.folded_at.contains_key(&w)
        };
        retired.retain(|&w, _since_ms| {
            let drop = dropped_by(self, w) || dropped_by(other, w);
            if drop {
                seen.remove(&w);
            }
            !drop
        });

        let is_dead_by = |side: &Self, writer: WriterId, seq: u64, in_side_adds: bool| -> bool {
            !in_side_adds
                && (side.retired.contains_key(&writer)
                    || side.folded_at.contains_key(&writer)
                    || side
                        .seen
                        .get(&writer)
                        .is_some_and(|&watermark| seq <= watermark))
        };

        let mut adds = BTreeMap::new();
        for (&(writer, seq), element) in self.adds.iter().chain(other.adds.iter()) {
            if adds.contains_key(&(writer, seq)) {
                continue;
            }
            let in_self = self.adds.contains_key(&(writer, seq));
            let in_other = other.adds.contains_key(&(writer, seq));
            let keep = if in_self && in_other {
                true
            } else if in_self {
                !is_dead_by(other, writer, seq, in_other)
            } else {
                debug_assert!(in_other, "tag came from self.adds or other.adds");
                !is_dead_by(self, writer, seq, in_self)
            };
            if keep {
                adds.insert((writer, seq), element.clone());
            }
        }

        Self {
            adds,
            seen,
            retired,
            folded_at,
        }
    }

    /// Drops every `folded_at` receipt older than `receipt_ttl_ms`
    /// ([`CompactionBounds::receipt_ttl_ms`]), the point past which no
    /// replica's stale copy of the writer is reconciled any more; `None` if
    /// no receipt is that old. Called by
    /// [`Self::compact`] on the sweep, and by the resolver on every merge
    /// apply through [`ConflictResolver::settle`], so a receipt one replica
    /// has already pruned cannot ride back in from a peer that has not.
    #[must_use]
    pub(crate) fn prune_receipts(&self, now_ms: u64, receipt_ttl_ms: u64) -> Option<Self> {
        if !self
            .folded_at
            .values()
            .any(|&since_ms| now_ms.saturating_sub(since_ms) > receipt_ttl_ms)
        {
            return None;
        }
        let mut out = self.clone();
        out.folded_at
            .retain(|_, &mut since_ms| now_ms.saturating_sub(since_ms) <= receipt_ttl_ms);
        Some(out)
    }

    /// Runs both retirement stages for the writers `retire` accepts,
    /// returning the compacted set when anything changed, `None`
    /// otherwise.
    ///
    /// Stage one stamps a `retired` entry at `now_ms` for a writer
    /// `retire` accepts, leaving `seen` untouched. Stage two, only when
    /// `quiet`, drops `seen`/`retired` for a writer retired more than
    /// `2 * retire_after_ms` ago and leaves a `folded_at` receipt; a
    /// pre-existing receipt older than `3 * retire_after_ms` is pruned
    /// first, against `self`'s state before that drop, so a writer
    /// advances at most one stage per call, keeping [`Self::merge`]'s
    /// reconciliation always finding a fresh receipt. Never touches
    /// `adds`.
    #[must_use]
    pub(crate) fn compact(
        &self,
        now_ms: u64,
        bounds: CompactionBounds,
        retire: &dyn Fn(WriterId) -> bool,
        quiet: bool,
    ) -> Option<Self> {
        let mut out = self.clone();
        let mut changed = false;

        let writers: BTreeSet<WriterId> = self
            .adds
            .keys()
            .map(|&(writer, _)| writer)
            .chain(self.seen.keys().copied())
            .collect();
        for writer in writers {
            if retire(writer) && !out.retired.contains_key(&writer) {
                out.retired.insert(writer, now_ms);
                changed = true;
            }
        }

        if quiet {
            let double_bound = bounds.retire_after_ms.saturating_mul(2);
            let mut aged_any = false;

            // Prune pre-existing receipts *before* folding anything new
            // this call; see the doc comment above for why the ordering
            // matters.
            if let Some(pruned) = out.prune_receipts(now_ms, bounds.receipt_ttl_ms) {
                out = pruned;
                aged_any = true;
            }

            // Stage two: drop a `retired` entry (and its `seen` watermark,
            // if any) once aged past the bound, leaving a receipt behind.
            let seen = &mut out.seen;
            let folded_at = &mut out.folded_at;
            out.retired.retain(|&writer, &mut since_ms| {
                if now_ms.saturating_sub(since_ms) > double_bound {
                    seen.remove(&writer);
                    folded_at.insert(writer, since_ms);
                    aged_any = true;
                    false
                } else {
                    true
                }
            });

            changed |= aged_any;
        }

        changed.then_some(out)
    }

    /// Postcard-encodes this set via its own [`Serialize`] derive.
    /// `BTreeMap`/`BTreeSet`'s deterministic iteration order makes the
    /// encoding canonical: two sets with the same content always encode to
    /// the same bytes.
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
/// [`OrSet::merge`] instead of picking a winner, falling back to plain
/// [`crate::Hlc`]-order `A`/`B` when either side is value-less or fails to
/// decode as an [`OrSet<T>`]. Carries no state of its own, so it is
/// `Send`/`Sync`/`Copy` regardless of `T`.
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
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return None;
        };
        let (Ok(sa), Ok(sb)) = (OrSet::<T>::decode(av), OrSet::<T>::decode(bv)) else {
            return None;
        };
        let bytes = sa.merge(&sb).encode().ok()?;
        Some(Merged {
            value: Bytes::from(bytes),
            // A merged set's tags only ever accumulate (adds and
            // seen/retired bookkeeping alike), so the set as a whole never
            // expires on its own: a TTL policy for it, if any, belongs to
            // whichever explicit write set one.
            expires_at_ms: None,
        })
    }

    fn compact(
        &self,
        _key: &[u8],
        value: &[u8],
        now_ms: u64,
        retire: &dyn Fn(WriterId) -> bool,
        quiet: bool,
        bounds: CompactionBounds,
    ) -> Option<Bytes> {
        let set = OrSet::<T>::decode(value).ok()?;
        let compacted = set.compact(now_ms, bounds, retire, quiet)?;
        let bytes = compacted.encode().ok()?;
        Some(Bytes::from(bytes))
    }

    fn settle(&self, _key: &[u8], value: &[u8], now_ms: u64, receipt_ttl_ms: u64) -> Option<Bytes> {
        let set = OrSet::<T>::decode(value).ok()?;
        let pruned = set.prune_receipts(now_ms, receipt_ttl_ms)?;
        let bytes = pruned.encode().ok()?;
        Some(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proptest::prelude::*;

    use super::*;
    use crate::hlc::Hlc;
    use crate::node::NodeId;

    fn hlc(wall_ms: u64, logical: u32) -> Hlc {
        Hlc {
            wall_ms,
            logical,
            node: NodeId::from(1),
        }
    }

    fn wid(node: u64, incarnation: u64) -> WriterId {
        WriterId::new(NodeId::from(node), incarnation)
    }

    // ---------------------------------------------------------------
    // The tombstone-based oracle kept as ground truth for the property
    // suite below: a plain `OrSet` with no writer identity concept and no
    // retirement, used only to check that ordinary add/remove/merge
    // behavior is unchanged by introducing `seen`/`retired` and two-stage
    // compaction.
    // ---------------------------------------------------------------

    type OracleTag = (u64, u64);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct OrSetOracle<T: Ord + Clone> {
        adds: BTreeMap<OracleTag, T>,
        tombstones: BTreeSet<OracleTag>,
    }

    impl<T: Ord + Clone> OrSetOracle<T> {
        fn empty() -> Self {
            Self {
                adds: BTreeMap::new(),
                tombstones: BTreeSet::new(),
            }
        }

        fn add(&mut self, writer: u64, seq: u64, element: T) {
            self.adds.insert((writer, seq), element);
        }

        fn remove(&mut self, element: &T) {
            let dead: Vec<OracleTag> = self
                .adds
                .iter()
                .filter(|(_, e)| *e == element)
                .map(|(&tag, _)| tag)
                .collect();
            self.tombstones.extend(dead);
        }

        fn live(&self) -> BTreeSet<T>
        where
            T: Clone,
        {
            self.adds
                .iter()
                .filter(|(tag, _)| !self.tombstones.contains(tag))
                .map(|(_, e)| e.clone())
                .collect()
        }
    }

    // ---------------------------------------------------------------
    // Type-level proptest generators
    // ---------------------------------------------------------------

    /// A small, mostly-colliding domain for generated tags' writer ids, so
    /// merges routinely combine two sets that share tags instead of
    /// trivially union-ing disjoint ones.
    fn writer_id() -> impl Strategy<Value = WriterId> {
        (0u64..3, 0u64..2).prop_map(|(n, i)| wid(n, i))
    }

    fn tag() -> impl Strategy<Value = Tag> {
        (writer_id(), 0u64..4)
    }

    /// The element a given tag adds, in the generated test data: a
    /// deterministic function of the tag alone, so any two independently
    /// generated sets that happen to share a tag necessarily agree on its
    /// element, matching the real invariant (a tag is minted once, for one
    /// element, and never reused for another).
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

    fn seen_map() -> impl Strategy<Value = BTreeMap<WriterId, u64>> {
        proptest::collection::btree_map(writer_id(), 0u64..4, 0..3)
    }

    fn retired_map() -> impl Strategy<Value = BTreeMap<WriterId, u64>> {
        proptest::collection::btree_map(writer_id(), 0u64..1000, 0..2)
    }

    fn folded_at_map() -> impl Strategy<Value = BTreeMap<WriterId, u64>> {
        proptest::collection::btree_map(writer_id(), 0u64..1000, 0..2)
    }

    /// Any shape: adds, `seen` watermarks, `retired` entries, and
    /// `folded_at` receipts, with the merge invariant (a writer's `retired`
    /// entry and its `folded_at` receipt mutually exclusive) maintained by
    /// construction.
    fn or_set() -> impl Strategy<Value = OrSet<String>> {
        (adds_map(), seen_map(), retired_map(), folded_at_map()).prop_map(
            |(adds, seen, retired, mut folded_at)| {
                for w in retired.keys() {
                    folded_at.remove(w);
                }
                OrSet {
                    adds,
                    seen,
                    retired,
                    folded_at,
                }
            },
        )
    }

    /// Only ever adds and `seen` watermarks: no writer has ever been
    /// retired on either side, and no fold has ever run.
    fn plain_or_set() -> impl Strategy<Value = OrSet<String>> {
        (adds_map(), seen_map()).prop_map(|(adds, seen)| OrSet {
            adds,
            seen,
            retired: BTreeMap::new(),
            folded_at: BTreeMap::new(),
        })
    }

    // ---------------------------------------------------------------
    // Basic behavior
    // ---------------------------------------------------------------

    #[test]
    fn add_and_contains() {
        let s = OrSet::add(wid(1, 0), 0, "x".to_string());
        assert!(s.contains(&"x".to_string()));
        assert!(!s.contains(&"y".to_string()));
    }

    #[test]
    fn iter_returns_live_members_only() {
        let s = OrSet::add(wid(1, 0), 0, "x".to_string());
        let removed = OrSet::remove(&s, &"x".to_string());
        let merged = s.merge(&removed);
        assert_eq!(merged.iter().collect::<Vec<_>>(), Vec::<&String>::new());
    }

    /// `add` then `remove` then `merge` removes the element.
    /// Guards `is_dead_by`'s argument order: swapped, a remove's watermark
    /// is never reachable and the element stays live.
    #[test]
    fn orset_remove_actually_removes() {
        let added = OrSet::add(wid(1, 0), 0, "x".to_string());
        let removed = OrSet::remove(&added, &"x".to_string());
        let merged = added.merge(&removed);
        assert!(
            !merged.contains(&"x".to_string()),
            "a remove observing the only add of `x` must actually remove it"
        );
    }

    #[test]
    fn remove_only_tombstones_observed_tags() {
        // Two independent adds of the same element, from two different
        // writers, get two different tags. Observing only the first add
        // and removing it must not affect the second's tag.
        let first = OrSet::add(wid(1, 0), 0, "x".to_string());
        let second = OrSet::add(wid(2, 0), 0, "x".to_string());
        let both = first.merge(&second);

        let removed = OrSet::remove(&first, &"x".to_string());
        let merged = both.merge(&removed);
        assert!(
            merged.contains(&"x".to_string()),
            "the second writer's untouched tag keeps `x` live"
        );
    }

    #[test]
    fn merge_unions_adds_and_raises_seen_watermarks() {
        let a = OrSet::add(wid(1, 0), 0, "x".to_string());
        let b = OrSet::add(wid(2, 0), 0, "y".to_string());
        let merged = a.merge(&b);
        assert!(merged.contains(&"x".to_string()));
        assert!(merged.contains(&"y".to_string()));
    }

    #[test]
    fn encode_decode_round_trips_an_add_only_set() {
        let s = OrSet::add(wid(7, 3), 3, "x".to_string());
        let bytes = s.encode().expect("encodes");
        assert_eq!(OrSet::decode(&bytes).expect("decodes"), s);
    }

    #[test]
    fn encode_decode_round_trips_a_set_with_several_adds() {
        let mut s = OrSet::<String>::add(wid(0, 0), 0, "e0".to_string());
        for i in 1..7u64 {
            s = s.merge(&OrSet::add(wid(i, 0), 0, format!("e{i}")));
        }
        assert_eq!(s.adds.len(), 7);
        let bytes = s.encode().expect("encodes");
        assert_eq!(OrSet::decode(&bytes).expect("decodes"), s);
    }

    #[test]
    fn encode_decode_round_trips_a_set_with_a_remove() {
        let s = OrSet::add(wid(7, 3), 3, "x".to_string());
        let removed = OrSet::remove(&s, &"x".to_string());
        let merged = s.merge(&removed);
        assert!(!merged.seen.is_empty(), "a remove always populates seen");
        let bytes = merged.encode().expect("encodes");
        assert_eq!(OrSet::decode(&bytes).expect("decodes"), merged);
    }

    /// A writer adds an element, it is never removed, and the writer is
    /// retired via `compact`: the element stays a live member afterward
    /// and survives further merges.
    #[test]
    fn orset_compact_preserves_live_elements_of_retired_writer() {
        let w = wid(1, 0);
        let s = OrSet::add(w, 0, "x".to_string());
        let compacted = s
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .expect("w is newly eligible, so compact must report a change");
        assert!(
            compacted.contains(&"x".to_string()),
            "retiring a writer must never remove its own never-removed elements"
        );

        // Survives a further merge with an independent, unrelated set too.
        let other = OrSet::add(wid(2, 0), 0, "y".to_string());
        let merged = compacted.merge(&other);
        assert!(merged.contains(&"x".to_string()));
        assert!(merged.contains(&"y".to_string()));
    }

    /// Confirms retirement stays sticky but scoped: retiring a
    /// writer blocks only a *future* add under its old identity, via a
    /// delta merge (the shape a real redelivered/late add would take).
    #[test]
    fn orset_compact_blocks_only_future_adds() {
        let w = wid(1, 0);
        let s = OrSet::add(w, 0, "x".to_string());
        let compacted = s
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .expect("newly eligible");

        let late_add = OrSet::add(w, 1, "z".to_string());
        let merged = compacted.merge(&late_add);
        assert!(
            !merged.contains(&"z".to_string()),
            "a new add from a retired writer must never become live"
        );
        assert!(
            merged.contains(&"x".to_string()),
            "the pre-retirement element is unaffected"
        );
    }

    #[test]
    fn compact_returns_none_when_no_writer_is_eligible() {
        let s = OrSet::add(wid(1, 0), 0, "x".to_string());
        assert_eq!(
            s.compact(1_000, CompactionBounds::three_bounds(100), &|_| false, true),
            None
        );
    }

    #[test]
    fn prune_receipts_drops_only_receipts_older_than_the_ttl() {
        let mut set = OrSet::add(wid(3, 1), 0, "x".to_string());
        set.folded_at.insert(wid(1, 1), 1_000);
        set.folded_at.insert(wid(2, 1), 5_000);
        let pruned = set
            .prune_receipts(5_000, 3_000)
            .expect("the old receipt is past the ttl");
        assert_eq!(
            pruned.folded_at.keys().copied().collect::<Vec<_>>(),
            vec![wid(2, 1)]
        );
        assert!(
            pruned.contains(&"x".to_string()),
            "pruning a receipt never touches membership"
        );
        assert!(set.prune_receipts(3_900, 3_000).is_none());
    }

    #[test]
    fn compact_is_idempotent() {
        let w = wid(1, 0);
        let s = OrSet::add(w, 0, "x".to_string());
        let once = s
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .unwrap();
        assert_eq!(
            once.compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true
            ),
            None
        );
    }

    /// Stage two: a retired writer's `seen`/`retired` entries age out once
    /// quiet and past twice the retirement bound, but never before either
    /// condition holds.
    #[test]
    fn compact_stage_two_drops_seen_and_retired_once_aged_and_quiet() {
        let w = wid(1, 0);
        let s = OrSet::add(w, 0, "x".to_string());
        let removed = OrSet::remove(&s, &"x".to_string());
        let merged = s.merge(&removed);
        let retired = merged
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .expect("newly eligible");
        assert!(retired.seen.contains_key(&w));
        assert!(retired.retired.contains_key(&w));

        // Not yet past 2x the bound: no change.
        assert_eq!(
            retired.compact(1_150, CompactionBounds::three_bounds(100), &|_| false, true),
            None,
            "150ms < 2x100ms bound: too soon to age out"
        );

        // Past the bound, but not quiet: no change.
        assert_eq!(
            retired.compact(
                1_250,
                CompactionBounds::three_bounds(100),
                &|_| false,
                false
            ),
            None,
            "aged past the bound, but the cache is not quiet"
        );

        // Past 2x the bound and quiet: both entries drop, leaving a receipt.
        let aged_out = retired
            .compact(1_250, CompactionBounds::three_bounds(100), &|_| false, true)
            .expect("aged past the bound and quiet");
        assert!(!aged_out.seen.contains_key(&w));
        assert!(!aged_out.retired.contains_key(&w));
        assert_eq!(
            aged_out.folded_at.get(&w),
            Some(&1_000),
            "a receipt for w specifically, naming w's own since_ms"
        );
        assert!(
            !aged_out.contains(&"x".to_string()),
            "x was removed before w's retirement; it stays absent"
        );
    }

    /// A `folded_at` receipt is pruned only once it is old enough *and* the
    /// cache is quiet, counting only from before the call that created
    /// it: a single `compact` call that folds a writer never also prunes
    /// that same brand-new receipt (see [`OrSet::compact`]'s doc for why),
    /// so this needs a further, separate call once the receipt itself has
    /// aged past three times the bound.
    #[test]
    fn a_folded_at_receipt_is_pruned_once_aged_past_three_times_the_bound_but_not_before() {
        let w = wid(1, 0);
        let s = OrSet::add(w, 0, "x".to_string());
        let removed = OrSet::remove(&s, &"x".to_string());
        let folded = s
            .merge(&removed)
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .expect("newly eligible")
            .compact(1_250, CompactionBounds::three_bounds(100), &|_| false, true)
            .expect("stage two fires");
        assert!(folded.folded_at.contains_key(&w));

        assert_eq!(
            folded.compact(1_299, CompactionBounds::three_bounds(100), &|_| false, true),
            None,
            "299ms since the receipt was written < 3x100ms bound: too soon to prune"
        );
        assert_eq!(
            folded.compact(
                1_400,
                CompactionBounds::three_bounds(100),
                &|_| false,
                false
            ),
            None,
            "aged past the bound, but the cache is not quiet"
        );

        let pruned = folded
            .compact(1_400, CompactionBounds::three_bounds(100), &|_| false, true)
            .expect("the receipt ages out: quiet and past 3x the bound");
        assert!(!pruned.folded_at.contains_key(&w));
        assert!(
            !pruned.contains(&"x".to_string()),
            "x was removed before w's retirement; it stays absent"
        );
    }

    /// After stage two ages a writer's `seen`/`retired` entries out, the
    /// live-membership content is otherwise unaffected: aging out is pure
    /// bookkeeping cleanup, not a membership change.
    #[test]
    fn compact_stage_two_never_changes_live_membership() {
        let w = wid(1, 0);
        let s = OrSet::add(w, 0, "x".to_string()).merge(&OrSet::add(wid(2, 0), 0, "y".to_string()));
        let retired = s
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .expect("newly eligible");
        let aged_out = retired
            .compact(1_500, CompactionBounds::three_bounds(100), &|_| false, true)
            .expect("aged past the bound and quiet");
        assert_eq!(
            aged_out.iter().collect::<BTreeSet<_>>(),
            retired.iter().collect::<BTreeSet<_>>()
        );
    }

    /// Replica A retires `w` before merging in a late tag from `w` that
    /// replica B already held live at retirement time. Since `compact`
    /// never touches `adds`, A's own copy is unaffected either way, and
    /// merging A's retired state into B does not remove B's already-live
    /// tag from the merged result (it is in both sides' `adds`).
    #[test]
    fn retiring_a_writer_on_one_replica_does_not_disturb_a_tag_the_other_already_holds_live() {
        let w = wid(1, 0);
        let shared = OrSet::add(w, 0, "x".to_string());
        let replica_a = shared
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .expect("newly eligible");
        let replica_b = shared.clone();

        let merged = replica_a.merge(&replica_b);
        assert!(
            merged.contains(&"x".to_string()),
            "a tag both replicas already held live before retirement stays live"
        );
    }

    /// A writer misclassified as absent by one replica, while it keeps
    /// writing through another, loses adds made during the
    /// misclassification window once that replica's retirement
    /// propagates. This is the existing contract, the same one
    /// `tombstone_max_ttl` already documents for GC'd tombstones, not a
    /// bug. This test documents the exact loss rather than asserting it
    /// away.
    #[test]
    fn a_writer_misclassified_as_retired_loses_adds_made_during_the_window() {
        let w = wid(1, 0);
        let base = OrSet::add(w, 0, "x".to_string());

        // Replica X never sees w's later activity and retires it.
        let x = base
            .clone()
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .expect("newly eligible");

        // Meanwhile w, still alive under the same incarnation, keeps
        // writing through another replica that never retired it.
        let new_tag_seen_elsewhere = base.merge(&OrSet::add(w, 1, "z".to_string()));
        assert!(new_tag_seen_elsewhere.contains(&"z".to_string()));

        // Once X's retirement propagates via merge, the new tag is lost:
        // documented, bounded loss, not resurrection of a real remove.
        let merged = x.merge(&new_tag_seen_elsewhere);
        assert!(
            !merged.contains(&"z".to_string()),
            "a new tag from a writer one side has already retired never survives a merge with that side"
        );
        assert!(
            merged.contains(&"x".to_string()),
            "the tag both sides already held before the misclassification is unaffected"
        );
    }

    // ---------------------------------------------------------------
    // Lattice laws, at the byte level
    // ---------------------------------------------------------------

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
        /// three-replica pairwise fold could apply `a`, `b`, `c` in, not
        /// only the two groupings of a single ordering. Holds
        /// unconditionally only while nothing has ever been retired on any
        /// side: [`OrSet::merge`]'s watermark-based fold-agreement rule is
        /// order-sensitive by design once retirement is involved, exactly
        /// like [`super::super::PnCounter::merge`]'s own stage two, so this
        /// is deliberately restricted to the plain adds-and-removes-only
        /// case.
        #[test]
        fn merge_is_associative_three_way_all_orderings(
            a in plain_or_set(), b in plain_or_set(), c in plain_or_set()
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
        /// mark dead the tags it has observed, so a concurrent add
        /// of the same element under a fresh, unobserved tag survives the
        /// merge regardless of apply order.
        #[test]
        fn concurrent_add_of_the_same_element_survives_a_concurrent_remove(
            node_a in 0u64..3, inc_a in 0u64..2, seq_a in 0u64..1_000,
            node_b in 0u64..3, inc_b in 0u64..2, seq_b in 0u64..1_000,
        ) {
            let writer_a = wid(node_a, inc_a);
            let writer_b = wid(node_b, inc_b);
            // Genuinely different writers: a single writer's own sequence
            // numbers are causally ordered (never issued out of order), so
            // "the same writer, a lower sequence" is not a real concurrent
            // scenario at all, only a malformed input this property is not
            // about.
            prop_assume!(writer_a != writer_b);
            let element = "x".to_string();

            let base = OrSet::add(writer_a, seq_a, element.clone());
            // The remover observes only `base`, never the concurrent add.
            let removed = OrSet::remove(&base, &element);
            let concurrent_add = OrSet::add(writer_b, seq_b, element.clone());

            let merged_remove_then_add = base.merge(&removed).merge(&concurrent_add);
            let merged_add_then_remove = base.merge(&concurrent_add).merge(&removed);

            prop_assert!(merged_remove_then_add.contains(&element));
            prop_assert!(merged_add_then_remove.contains(&element));
            prop_assert_eq!(
                merged_remove_then_add.encode().expect("encodes"),
                merged_add_then_remove.encode().expect("encodes")
            );
        }

        /// Byte-level round-trip: `decode(encode(x))` is exact for any
        /// generated shape.
        #[test]
        fn encode_decode_round_trips(a in or_set()) {
            let bytes = a.encode().expect("encodes");
            let decoded = OrSet::decode(&bytes).expect("decodes");
            prop_assert_eq!(&decoded, &a);
        }
    }

    // ---------------------------------------------------------------
    // Membership equals the tombstone-based oracle
    // ---------------------------------------------------------------

    /// One step of a generated write/remove/retire/redeliver/merge
    /// scenario, run in lockstep against the tombstone-based oracle.
    #[derive(Debug, Clone)]
    enum Op {
        Add {
            writer: u64,
            element: &'static str,
        },
        Remove {
            element: &'static str,
        },
        Retire {
            writer: u64,
        },
        /// Re-delivers the current accumulated state to itself (a no-op
        /// merge with its own current value), exercising idempotence
        /// inline in a scenario rather than only as its own dedicated
        /// property.
        Redeliver,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0u64..3, prop_oneof![Just("a"), Just("b"), Just("c")])
                .prop_map(|(writer, element)| Op::Add { writer, element }),
            prop_oneof![Just("a"), Just("b"), Just("c")].prop_map(|element| Op::Remove { element }),
            (0u64..3).prop_map(|writer| Op::Retire { writer }),
            Just(Op::Redeliver),
        ]
    }

    /// Replays `ops` against both the real (compaction-capable) `OrSet`
    /// and the tombstone-based oracle, asserting their live membership
    /// matches after every step. A writer's sequence counter is local to
    /// its current *incarnation*, matching the real system: this harness
    /// never restarts a writer mid-scenario, so each writer keeps one
    /// incarnation (0) throughout; restart/incarnation-change behavior is
    /// covered separately by the dedicated `WriterId`-specific tests
    /// above. Once a writer is retired, a further `Add` from it is skipped
    /// identically on both sides (real and oracle) rather than generated
    /// at all, since the real system's sticky-retirement rejection of a
    /// *new* post-retirement add is already covered as its own dedicated
    /// property (`orset_compact_blocks_only_future_adds`); this harness
    /// instead checks that everything retirement does *not* touch, every
    /// pre-existing live element, and ordinary add/remove/merge/redeliver
    /// behavior, stays exactly in step with a system that never retires
    /// anyone at all.
    fn replay_and_compare(ops: &[Op]) {
        let mut real = OrSet::<String>::add(wid(999, 0), 0, "seed".to_string());
        real = OrSet::remove(&real, &"seed".to_string()).merge(&real);
        // `real` now has an empty, seen-non-empty baseline so subsequent
        // shape doesn't matter for the comparison below (only membership
        // does).
        let mut oracle = OrSetOracle::<String>::empty();
        oracle.add(999, 0, "seed".to_string());
        oracle.remove(&"seed".to_string());

        let mut retired: HashSet<u64> = HashSet::new();
        let mut next_seq: BTreeMap<u64, u64> = BTreeMap::new();
        let now_ms = 1_000;

        for step in ops {
            match *step {
                Op::Add { writer, element } => {
                    if retired.contains(&writer) {
                        continue; // see the doc comment above
                    }
                    let seq = next_seq.entry(writer).or_insert(0);
                    let this_seq = *seq;
                    *seq += 1;
                    real = real.merge(&OrSet::add(wid(writer, 0), this_seq, element.to_string()));
                    oracle.add(writer, this_seq, element.to_string());
                }
                Op::Remove { element } => {
                    let delta = OrSet::remove(&real, &element.to_string());
                    real = real.merge(&delta);
                    oracle.remove(&element.to_string());
                }
                Op::Retire { writer } => {
                    let w = wid(writer, 0);
                    if let Some(compacted) = real.compact(
                        now_ms,
                        CompactionBounds::three_bounds(100),
                        &|candidate| candidate == w,
                        true,
                    ) {
                        real = compacted;
                    }
                    retired.insert(writer);
                }
                Op::Redeliver => {
                    real = real.merge(&real.clone());
                }
            }

            let real_live: BTreeSet<String> = real.iter().cloned().collect();
            assert_eq!(
                real_live,
                oracle.live(),
                "live membership must match the no-retirement oracle at every step \
                 (compaction must never remove a live, non-retired-add-blocking element)"
            );
        }
    }

    proptest! {
        #[test]
        fn membership_matches_the_oracle_under_any_generated_scenario(
            ops in proptest::collection::vec(op(), 0..40)
        ) {
            replay_and_compare(&ops);
        }
    }

    #[test]
    fn no_resurrection_of_a_removed_element_via_a_redelivered_stale_copy() {
        let w = wid(1, 0);
        let added = OrSet::add(w, 0, "x".to_string());
        let removed = OrSet::remove(&added, &"x".to_string());
        let after_remove = added.merge(&removed);
        assert!(!after_remove.contains(&"x".to_string()));

        // A stale redelivery of the pre-remove state (e.g. a retried
        // anti-entropy message carrying the old add alone) must not
        // resurrect it once merged back in.
        let redelivered = after_remove.merge(&added);
        assert!(
            !redelivered.contains(&"x".to_string()),
            "a redelivered stale add must never resurrect an already-removed tag"
        );
    }

    /// Redelivery/merge ordering across a small multi-replica scenario: two
    /// replicas apply the same operations through different message
    /// orderings (including a duplicate redelivery), then converge to the
    /// same membership once merged, matching the oracle for the same
    /// sequence.
    #[test]
    fn redelivered_and_reordered_messages_still_converge() {
        let w1 = wid(1, 0);
        let w2 = wid(2, 0);

        let add1 = OrSet::add(w1, 0, "x".to_string());
        let add2 = OrSet::add(w2, 0, "y".to_string());
        let remove1 = OrSet::remove(&add1, &"x".to_string());

        // Replica A: add1, remove1 (duplicated), add2.
        let replica_a = add1
            .merge(&remove1)
            .merge(&remove1) // redelivered duplicate
            .merge(&add2);
        // Replica B: add2, add1, remove1, a different order, no duplicate.
        let replica_b = add2.merge(&add1).merge(&remove1);

        let merged_ab = replica_a.merge(&replica_b);
        let merged_ba = replica_b.merge(&replica_a);
        assert_eq!(merged_ab.encode().unwrap(), merged_ba.encode().unwrap());
        assert!(!merged_ab.contains(&"x".to_string()));
        assert!(merged_ab.contains(&"y".to_string()));
    }

    /// A small explicit multi-replica churn scenario (redeliveries,
    /// retirements happening independently on different replicas, then a
    /// full merge), replayed against the oracle for the same op sequence
    /// with retirement ignored, extending the single-writer stale-slot
    /// case above to churn across several writers at once.
    #[test]
    fn independent_retirement_on_two_replicas_then_merge_matches_the_oracle() {
        let w1 = wid(1, 0);
        let w2 = wid(2, 0);
        let base = OrSet::add(w1, 0, "x".to_string()).merge(&OrSet::add(w2, 0, "y".to_string()));

        // Replica A retires w1 only; replica B retires w2 only, a stale
        // view of each other's retirement decision, exactly the cross-stale
        // shape `PnCounter`'s own merge tests exercise for the counter type.
        let replica_a = base
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|w| w == w1,
                true,
            )
            .expect("w1 newly eligible on A");
        let replica_b = base
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|w| w == w2,
                true,
            )
            .expect("w2 newly eligible on B");

        let merged = replica_a.merge(&replica_b);
        assert!(merged.retired.contains_key(&w1));
        assert!(merged.retired.contains_key(&w2));
        // Neither writer's pre-existing element is lost by either side's
        // independent retirement decision.
        assert!(merged.contains(&"x".to_string()));
        assert!(merged.contains(&"y".to_string()));

        let mut oracle = OrSetOracle::<String>::empty();
        oracle.add(1, 0, "x".to_string());
        oracle.add(2, 0, "y".to_string());
        assert_eq!(
            merged.iter().cloned().collect::<BTreeSet<_>>(),
            oracle.live()
        );
    }

    /// `since_ms` merges by minimum: whichever replica observed a writer's
    /// retirement first sets the pace for stage two on the merged result.
    #[test]
    fn retired_since_ms_merges_by_minimum() {
        let w = wid(1, 0);
        let base = OrSet::add(w, 0, "x".to_string());
        let retired_early = base
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|c| c == w,
                true,
            )
            .unwrap();
        let retired_late = base
            .compact(
                5_000,
                CompactionBounds::three_bounds(100),
                &|c| c == w,
                true,
            )
            .unwrap();

        let merged = retired_early.merge(&retired_late);
        assert_eq!(merged.retired.get(&w), Some(&1_000));

        let merged_reverse = retired_late.merge(&retired_early);
        assert_eq!(merged_reverse.retired.get(&w), Some(&1_000));
    }

    /// The same writer, folded to stage two independently by three
    /// replicas at three different times, converges to the same live
    /// membership however the merges are ordered: the watermark rule in
    /// [`OrSet::merge`] lets each side recognize the others' fold as
    /// legitimate instead of resurrecting `seen`/`retired` bookkeeping
    /// from whichever side hasn't folded yet.
    #[test]
    fn independent_folding_of_the_same_writer_at_different_times_converges() {
        let w = wid(1, 0);
        let base = OrSet::add(w, 0, "x".to_string());
        let fold_at = |since_ms: u64| {
            base.compact(
                since_ms,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                false,
            )
            .expect("stage one fires")
            .compact(
                since_ms + 201,
                CompactionBounds::three_bounds(100),
                &|_| false,
                true,
            )
            .expect("stage two fires")
        };
        let a = fold_at(0);
        let b = fold_at(500);
        let c = fold_at(1_000);
        for folded in [&a, &b, &c] {
            assert!(folded.retired.is_empty());
            assert!(folded.seen.is_empty());
            assert!(
                folded.contains(&"x".to_string()),
                "w's own never-removed element survives its own fold"
            );
        }

        let merged = a.merge(&b).merge(&c);
        assert!(merged.contains(&"x".to_string()));
        assert_eq!(
            merged.encode().expect("encodes"),
            c.merge(&a).merge(&b).encode().expect("encodes"),
            "every merge ordering of the three independently-folded copies agrees"
        );
    }

    /// A properly-removed element must never resurrect merely because some
    /// *other*, unrelated writer folds to stage two: replica C folding an
    /// unrelated writer `w3` must never "vouch" for writer `w`'s retirement
    /// on replica D too, when C has never seen `w` at all. Because
    /// `folded_at` names the specific writer it vouches for, `w`'s exact
    /// `seen` watermark survives a merge against a side that has only ever
    /// folded `w3`, closing the door to resurrecting `w`'s already-dead tag
    /// from a stale, pre-removal copy: the removed element stays removed.
    #[test]
    fn merge_never_resurrects_a_removed_element_solely_because_an_unrelated_writer_folded_later() {
        let w = wid(1, 0);
        let w3 = wid(3, 0);

        // Replica D: w adds "x", it is removed, then w is stage-one-retired
        // only, never folded further.
        let added = OrSet::add(w, 0, "x".to_string());
        let removed = OrSet::remove(&added, &"x".to_string());
        let d = added
            .merge(&removed)
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                true,
            )
            .expect("w newly eligible on D");
        assert!(!d.contains(&"x".to_string()));

        // Replica C: an entirely different writer w3 adds "y", independently
        // retired and folded to stage two (its own `folded_at` receipt for
        // w3 specifically), legitimately advancing C's own bookkeeping. C
        // has never heard of w or "x" at all.
        let c = OrSet::add(w3, 0, "y".to_string())
            .compact(
                5_000,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w3,
                true,
            )
            .expect("w3 newly eligible on C")
            .compact(
                5_000 + 201,
                CompactionBounds::three_bounds(100),
                &|_| false,
                true,
            )
            .expect("w3 folds to stage two on C, receipt intact");
        assert!(c.contains(&"y".to_string()));

        assert!(
            !c.merge(&d).contains(&"x".to_string()),
            "x must stay removed after merging in a side that only ever folded a different \
             writer"
        );

        // A stale, pre-removal redelivery of the original add must still
        // never resurrect x once merged into the properly-reconciled result.
        let merged = c.merge(&d);
        assert!(
            !merged.merge(&added).contains(&"x".to_string()),
            "w's exact seen watermark must survive the merge to keep rejecting a stale \
             redelivered add"
        );
    }

    /// A replica that discovers a dead writer's death *later* than a peer
    /// who has already folded that exact same writer must never defeat the
    /// peer's own receipt: an incarnation dies at most once ever, so a
    /// receipt for `w` and any `retired`/`seen` evidence of `w` on any
    /// replica can only ever refer to that one, singular death, never a
    /// different, later one. This is not a hypothetical: it is the normal
    /// shape of two independently-ticking replicas discovering the same
    /// supersession-based death at different real times (no absence wait
    /// gates this discovery, so both discover it "immediately" from their
    /// own, differently-phased point of view), well within the trust
    /// window this module's docs otherwise bound stale data to.
    #[test]
    fn merge_never_resurrects_when_a_peer_discovers_the_same_death_after_a_fold() {
        let w = wid(1, 0);
        let bound_ms: u64 = 2_000;

        // Writer discovers `w` dead at t=0 and, entirely on its own (no
        // merge against the observer in between), progresses all the way
        // through stage two: `w`'s tag drops from `seen`/`retired`, leaving
        // only a `folded_at` receipt behind.
        let base = OrSet::add(w, 0, "x".to_string());
        let writer_folded = base
            .compact(
                0,
                CompactionBounds::three_bounds(bound_ms),
                &|writer| writer == w,
                false,
            )
            .expect("writer stage one fires at t=0")
            .compact(
                2 * bound_ms + 1,
                CompactionBounds::three_bounds(bound_ms),
                &|_| false,
                true,
            )
            .expect("writer stage two fires once aged past 2x, still alone");

        // Observer discovers the exact same dead writer much later,
        // independently retiring it at its own, later discovery time.
        let observer_retired = base
            .compact(
                2 * bound_ms + 1,
                CompactionBounds::three_bounds(bound_ms),
                &|writer| writer == w,
                false,
            )
            .expect("observer stage one fires late, independently");

        // Now they finally sync, in both directions: `x`'s own live
        // membership is unaffected by any of this either way (`compact`
        // never touches `adds`), so the receipt's own job, keeping `w`'s
        // `retired` entry from persisting forever once one side has
        // already folded it, is what this checks.
        for merged in [
            writer_folded.merge(&observer_retired),
            observer_retired.merge(&writer_folded),
        ] {
            assert!(merged.contains(&"x".to_string()));
            assert!(
                merged.retired.is_empty(),
                "the writer's already-folded receipt must still vouch for the observer's \
                 later-discovered, still-retired entry for the exact same writer — instead \
                 it came back: {merged:?}"
            );
        }
    }

    /// Merging a compaction's own result against its pre-fold predecessor,
    /// the same lineage, recovers the exact fully-folded shape: the
    /// pre-fold side's `retired`/`seen` entries name exactly the writer the
    /// post-fold side's `folded_at` receipt already accounts for, so
    /// nothing is resurrected and an untouched writer's element is
    /// unaffected. `ShardOps::compact_pass` itself never merges a
    /// compaction result this way (`Engine::compact_replace_if_current`
    /// replaces the resident bytes directly instead); this pins the
    /// underlying exactness property `merge` itself still needs for two
    /// *distinct* replicas to reconcile correctly.
    #[test]
    fn a_compaction_result_merged_against_its_own_pre_fold_predecessor_is_exact() {
        let w = wid(1, 0);
        let resolver = OrSetResolver::<String>::new();

        let base =
            OrSet::add(w, 0, "x".to_string()).merge(&OrSet::add(wid(2, 0), 0, "y".to_string()));
        let removed = OrSet::remove(&base, &"x".to_string());
        let with_remove = base.merge(&removed);
        let pre_fold = with_remove
            .compact(
                0,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                false,
            )
            .expect("stage one fires");
        let folded = pre_fold
            .compact(201, CompactionBounds::three_bounds(100), &|_| false, true)
            .expect("stage two fires");

        let pre_fold_bytes = pre_fold.encode().expect("encodes");
        let stage_two_bytes = folded.encode().expect("encodes");
        let pre_fold_view = RecordView {
            value: Some(&pre_fold_bytes),
            ver: hlc(1, 0),
            expires_at_ms: None,
        };
        let folded_view = RecordView {
            value: Some(&stage_two_bytes),
            ver: hlc(1, 0),
            expires_at_ms: None,
        };

        let merged = resolver
            .merge(b"k", pre_fold_view, folded_view)
            .expect("both decode");
        let decoded = OrSet::<String>::decode(&merged.value).expect("decodes");
        assert!(!decoded.contains(&"x".to_string()), "x stays removed");
        assert!(
            decoded.contains(&"y".to_string()),
            "the untouched writer's element is unaffected"
        );
        assert_eq!(
            merged.value.as_ref(),
            folded.encode().expect("encodes"),
            "reconciling the pre-fold predecessor against its own fold recovers exactly the \
             fully-folded shape, with nothing resurrected"
        );
    }

    /// A redelivered pre-fold copy of a record, merged in after the record
    /// has already been folded, is a no-op: it never resurrects the
    /// per-writer `seen`/`retired` bookkeeping stage two already dropped.
    #[test]
    fn a_redelivered_pre_fold_record_merged_in_after_the_fold_is_a_no_op() {
        let w = wid(1, 0);
        let base = OrSet::add(w, 0, "x".to_string());
        let pre_fold = base
            .compact(
                0,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                false,
            )
            .expect("stage one fires");
        let folded = pre_fold
            .compact(201, CompactionBounds::three_bounds(100), &|_| false, true)
            .expect("stage two fires");

        let merged = folded.merge(&pre_fold);
        assert_eq!(
            merged.encode().expect("encodes"),
            folded.encode().expect("encodes"),
            "redelivering the stale pre-fold copy changes nothing once folded"
        );
        assert!(
            merged.contains(&"x".to_string()),
            "x was never removed, so it stays live throughout"
        );
    }

    /// The documented bounded loss outside the trust window, `OrSet`'s own
    /// counterpart to `PnCounter`'s: once every replica that once tracked
    /// `w`'s retirement has folded it and then had its `folded_at` receipt
    /// pruned, nothing is left to reject a fresh tag `w` writes afterward:
    /// the same trust boundary `tombstone_max_ttl` already documents for a
    /// member gone that long, not a new failure mode introduced by
    /// compaction. While `w`'s receipt is still present (stage two), its
    /// sticky retirement still holds: a late tag is rejected exactly as it
    /// would be at stage one.
    #[test]
    fn documents_the_bounded_loss_when_a_writer_writes_again_after_everyone_folded_it() {
        let w = wid(1, 0);
        let base = OrSet::add(w, 0, "x".to_string());
        let removed = OrSet::remove(&base, &"x".to_string());
        let with_remove = base.merge(&removed);
        let pre_fold = with_remove
            .compact(
                0,
                CompactionBounds::three_bounds(100),
                &|writer| writer == w,
                false,
            )
            .expect("stage one fires");
        let at_stage_two = pre_fold
            .compact(201, CompactionBounds::three_bounds(100), &|_| false, true)
            .expect("stage two fires");
        assert!(
            !at_stage_two.contains(&"x".to_string()),
            "x's removal is unaffected by folding"
        );

        let late_tag = OrSet::add(w, 1, "z".to_string());
        assert!(
            !at_stage_two.merge(&late_tag).contains(&"z".to_string()),
            "still sticky at stage two: w's retirement is an exact, writer-scoped fact, not \
             yet a bare watermark"
        );

        let folded = at_stage_two
            .compact(301, CompactionBounds::three_bounds(100), &|_| false, true)
            .expect("the receipt is pruned, past its own trust boundary");
        assert!(
            !folded.contains(&"x".to_string()),
            "x's removal is still unaffected once fully folded"
        );
        let merged = folded.merge(&late_tag);
        assert!(
            merged.contains(&"z".to_string()),
            "once folded all the way through stage three, nothing is left anywhere to reject a \
             writer's later tag — the documented, bounded contract, not a new failure mode"
        );
    }

    // ---------------------------------------------------------------
    // Resolver unit tests
    // ---------------------------------------------------------------

    #[test]
    fn resolver_merges_two_decodable_sets_regardless_of_argument_order() {
        let sa = OrSet::add(wid(1, 0), 0, "x".to_string());
        let sb = OrSet::add(wid(2, 0), 0, "y".to_string());
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
        let Some(Merged {
            value: merged_ab, ..
        }) = resolver.merge(b"k", av, bv)
        else {
            panic!("expected Some(Merged) when both sides decode");
        };
        let Some(Merged {
            value: merged_ba, ..
        }) = resolver.merge(b"k", bv, av)
        else {
            panic!("expected Some(Merged) when both sides decode");
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
    fn resolver_compacted_bytes_round_trip_through_merge() {
        let w = wid(1, 0);
        let s = OrSet::add(w, 0, "x".to_string());
        let removed = OrSet::remove(&s, &"x".to_string());
        let compacted = s
            .merge(&removed)
            .compact(
                1_000,
                CompactionBounds::three_bounds(100),
                &|c| c == w,
                true,
            )
            .expect("newly eligible");
        let encoded = compacted.encode().expect("encodes");

        let other = OrSet::add(wid(2, 0), 0, "y".to_string());
        let other_bytes = other.encode().expect("encodes");

        let a = RecordView {
            value: Some(&encoded),
            ver: hlc(1, 0),
            expires_at_ms: None,
        };
        let b = RecordView {
            value: Some(&other_bytes),
            ver: hlc(2, 0),
            expires_at_ms: None,
        };
        let resolver = OrSetResolver::<String>::new();
        let merged = resolver.merge(b"k", a, b).expect("both sides decode");
        let decoded = OrSet::<String>::decode(&merged.value).expect("decodes");
        assert!(decoded.contains(&"y".to_string()));
        assert!(!decoded.contains(&"x".to_string()));
    }

    #[test]
    fn resolver_falls_back_to_lww_when_a_side_is_a_tombstone() {
        let s = OrSet::add(wid(1, 0), 0, "x".to_string());
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
    fn resolver_merge_declines_on_decode_failure_and_winner_falls_back_to_lww() {
        let s = OrSet::add(wid(1, 0), 0, "x".to_string());
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
        assert!(resolver.merge(b"k", good, bad).is_none());
        assert!(resolver.merge(b"k", bad, good).is_none());
        assert_eq!(resolver.winner(b"k", good, bad), Winner::B);
        assert_eq!(resolver.winner(b"k", bad, good), Winner::A);
    }

    #[test]
    fn needs_value_bytes_is_true() {
        assert!(OrSetResolver::<String>::new().needs_value_bytes());
    }

    #[test]
    fn merges_is_true() {
        assert!(
            OrSetResolver::<String>::new().merges(),
            "OrSetResolver's merge can return Some, so it must advertise merges()"
        );
    }
}
