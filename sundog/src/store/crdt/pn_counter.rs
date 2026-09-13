//! A PN-Counter: increments and decrements from any number of writers merge
//! to the exact total, with no read before write and no lost updates under
//! concurrent apply.
//!
//! Each writer owns one slot in `p` (cumulative increments) and one in `n`
//! (cumulative decrements), the standard PN-Counter decomposition into two
//! G-Counters merging by pointwise maximum. A writer's successive writes
//! carry its own cumulative total ([`PnCounter::local_delta`]), never a
//! one-shot increment, since two independent "+1" deltas from the same
//! writer merge to `1` under `max`. A slot is keyed by [`WriterId`], not
//! [`NodeId`] alone, so a restarted node's fresh incarnation never resumes
//! or clobbers its pre-restart total; see [`WriterId`]'s doc.
//!
//! Compaction retires a long-gone writer in two stages
//! ([`PnCounter::compact`]): stage one moves its slot into a `retired`
//! entry; stage two, once quiet and retired long enough, folds it into a
//! bounded `folded_p`/`folded_n` accumulator and leaves a `folded_at`
//! receipt so [`PnCounter::merge`] can tell an already-folded writer from
//! mere silence about one. `PnCounter::prune_receipts` drops a receipt
//! past `receipt_ttl_ms`, under the same trust boundary `tombstone_max_ttl`
//! accepts for a long-gone member.
//!
//! [`PnCounter::encode`]/[`PnCounter::decode`] are thin postcard wrappers
//! around this type's one wire layout; a future change to it is versioned.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::WriterId;
use crate::store::{CompactionBounds, ConflictResolver, Merged, RecordView, Winner};

/// A writer's contribution at (and after) the moment it is retired: its
/// `p`/`n` slots as they stand then, and when retirement happens. Stays
/// disjoint from `PnCounter::p`/`PnCounter::n` for the same writer at all
/// times (the invariant [`PnCounter::merge`] and [`PnCounter::compact`] both
/// maintain).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Retired {
    p: u64,
    n: u64,
    /// Epoch milliseconds this writer's slot moves into `retired`.
    /// Merges by minimum, so every replica ages a writer from the earliest
    /// retirement anyone recorded.
    since_ms: u64,
}

/// A PN-Counter: per-writer cumulative increments and decrements that merge
/// by pointwise maximum, converging to the exact total regardless of apply
/// order, duplication, or how many writers have ever written to it, plus
/// the bounded, two-stage retirement state described in the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PnCounter {
    p: BTreeMap<WriterId, u64>,
    n: BTreeMap<WriterId, u64>,
    retired: BTreeMap<WriterId, Retired>,
    /// This replica's own bounded stage-two fold: a single running total,
    /// not one slot per replica. [`Self::merge`] never sums two sides'
    /// `folded_p` together, since a writer's contribution must be counted
    /// exactly once; it instead takes whichever side's bound already
    /// accounts for at least as much, deciding that via [`Self::folded_at`].
    folded_p: u64,
    folded_n: u64,
    /// A per-writer receipt: `w`'s own `since_ms` the moment this replica
    /// folds `w` into `folded_p`/`folded_n`. [`Self::merge`] trusts a
    /// side's silence about `w` as "already folded" only once its own
    /// `folded_at` names `w` with a `since_ms` at least as late as the
    /// merged `retired` entry's. Dropped once old enough that every
    /// reachable replica has independently folded the same writer too;
    /// see [`Self::compact`].
    folded_at: BTreeMap<WriterId, u64>,
}

impl PnCounter {
    /// The counter's current value: total increments minus total
    /// decrements, across every writer that has ever contributed to it,
    /// live or retired.
    #[must_use]
    pub fn value(&self) -> i128 {
        let (retired_p, retired_n) = self.retired.values().fold((0u64, 0u64), |(p, n), r| {
            (p.saturating_add(r.p), n.saturating_add(r.n))
        });
        let pos = self
            .p
            .values()
            .copied()
            .fold(0u64, u64::saturating_add)
            .saturating_add(retired_p)
            .saturating_add(self.folded_p);
        let neg = self
            .n
            .values()
            .copied()
            .fold(0u64, u64::saturating_add)
            .saturating_add(retired_n)
            .saturating_add(self.folded_n);
        i128::from(pos) - i128::from(neg)
    }

    /// A blind write from `writer` carrying its new cumulative increment
    /// total. The caller tracks its own running total and calls this on
    /// every local increment; no read of the shard is needed, and no other
    /// writer's slot is touched.
    #[must_use]
    pub fn local_delta(writer: WriterId, cumulative_increment: u64) -> Self {
        Self {
            p: BTreeMap::from([(writer, cumulative_increment)]),
            ..Self::default()
        }
    }

    /// A blind decrement, the `n`-side counterpart to [`Self::local_delta`]:
    /// `writer`'s new cumulative decrement total.
    #[must_use]
    pub fn local_decrement(writer: WriterId, cumulative_decrement: u64) -> Self {
        Self {
            n: BTreeMap::from([(writer, cumulative_decrement)]),
            ..Self::default()
        }
    }

    /// Folds `other` into a new counter in two stages. Takes no clock or
    /// bound: every timing decision is already made by whichever
    /// `compact` call produced `self`/`other`'s state, carried in
    /// `folded_at`.
    ///
    /// Stage one rebuilds `retired` as the union of both sides', taking
    /// each writer's max `p`/`n` (over both sides' retired and still-live
    /// slots) and minimum `since_ms`, which keeps the cross-stale-retirement
    /// case exact. Stage two drops a writer from `retired` once either
    /// side's `folded_at` receipt for it is at least as recent as the
    /// merged entry, crediting its bound into that side's
    /// `folded_p`/`folded_n` first; the merged bound is the greater of the
    /// two, never their sum. `p`/`n` are the pointwise maximum of both
    /// sides, excluding every writer now `retired`.
    ///
    /// `merge_is_commutative_for_any_shape` and
    /// `merge_is_idempotent_for_any_shape` hold unconditionally;
    /// `merge_is_associative_three_way_all_orderings_with_no_retired_state`
    /// only once nothing is retired.
    /// `merge_never_double_counts_when_a_peer_discovers_the_same_death_after_a_fold`
    /// pins the receipt exactness this relies on.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        let mut retired: BTreeMap<WriterId, Retired> = BTreeMap::new();
        for &w in self.retired.keys().chain(other.retired.keys()) {
            if retired.contains_key(&w) {
                continue;
            }
            let p = self
                .retired
                .get(&w)
                .map_or(0, |r| r.p)
                .max(other.retired.get(&w).map_or(0, |r| r.p))
                .max(self.p.get(&w).copied().unwrap_or(0))
                .max(other.p.get(&w).copied().unwrap_or(0));
            let n = self
                .retired
                .get(&w)
                .map_or(0, |r| r.n)
                .max(other.retired.get(&w).map_or(0, |r| r.n))
                .max(self.n.get(&w).copied().unwrap_or(0))
                .max(other.n.get(&w).copied().unwrap_or(0));
            let since_ms = match (self.retired.get(&w), other.retired.get(&w)) {
                (Some(a), Some(b)) => a.since_ms.min(b.since_ms),
                (Some(a), None) | (None, Some(a)) => a.since_ms,
                (None, None) => unreachable!("w was drawn from one of the two retired maps"),
            };
            retired.insert(w, Retired { p, n, since_ms });
        }

        // Whether `side` already folded `w` away: no live slot, no retired
        // entry, and a per-writer receipt naming `w` specifically, with no
        // `since_ms` comparison needed. A `WriterId` names one specific
        // membership incarnation, and an incarnation dies at most once
        // ever, so a receipt for `w` and any `retired`/live evidence of
        // `w` can only ever refer to that same, singular death, never a
        // different, later one the receipt would need to be "caught up"
        // to.
        let already_folded = |side: &Self, w: WriterId| -> bool {
            !side.retired.contains_key(&w)
                && !side.p.contains_key(&w)
                && !side.n.contains_key(&w)
                && side.folded_at.contains_key(&w)
        };

        // Extra credit toward each side's own bound: the (p, n) totals of
        // writers the *other* side has already folded but this side
        // hasn't, so comparing `self.folded_{p,n} + credit` against
        // `other.folded_{p,n} + credit` is a fair race between the two
        // sides' bounds even when only one side's `retired` map still
        // names the writer.
        let mut credit_self = (0u64, 0u64);
        let mut credit_other = (0u64, 0u64);
        retired.retain(|&w, r| {
            let by_self = already_folded(self, w);
            let by_other = already_folded(other, w);
            if by_other && !by_self {
                credit_self.0 = credit_self.0.saturating_add(r.p);
                credit_self.1 = credit_self.1.saturating_add(r.n);
            }
            if by_self && !by_other {
                credit_other.0 = credit_other.0.saturating_add(r.p);
                credit_other.1 = credit_other.1.saturating_add(r.n);
            }
            !(by_self || by_other)
        });

        let p = pointwise_max_excluding(&self.p, &other.p, &retired);
        let n = pointwise_max_excluding(&self.n, &other.n, &retired);

        let bound_self = (
            self.folded_p.saturating_add(credit_self.0),
            self.folded_n.saturating_add(credit_self.1),
        );
        let bound_other = (
            other.folded_p.saturating_add(credit_other.0),
            other.folded_n.saturating_add(credit_other.1),
        );

        let mut folded_at = self.folded_at.clone();
        for (&w, &t) in &other.folded_at {
            folded_at
                .entry(w)
                .and_modify(|m| *m = (*m).max(t))
                .or_insert(t);
        }

        Self {
            p,
            n,
            retired,
            folded_p: bound_self.0.max(bound_other.0),
            folded_n: bound_self.1.max(bound_other.1),
            folded_at,
        }
    }

    /// Retires writers `retire` accepts out of `p`/`n`, in two stages,
    /// returning the changed value, `None` if nothing changed.
    ///
    /// Stage one moves any writer's live `p`/`n` slot that `retire`
    /// accepts into a `retired` entry stamped `now_ms`. Stage two, only
    /// while `quiet`, folds a `retired` entry older than
    /// `2 * bounds.retire_after_ms` into `folded_p`/`folded_n` and leaves
    /// a `folded_at` receipt; a pre-existing receipt older than
    /// `bounds.receipt_ttl_ms` is pruned first, against `self`'s state
    /// before that fold, so a writer advances at most one stage per call,
    /// keeping [`Self::merge`]'s reconciliation always finding a fresh
    /// receipt.
    #[must_use]
    pub(crate) fn compact(
        &self,
        now_ms: u64,
        retire: &dyn Fn(WriterId) -> bool,
        quiet: bool,
        bounds: CompactionBounds,
    ) -> Option<Self> {
        let mut out = self.clone();
        let mut changed = false;

        // Deduplicated first so `retire` (a caller-supplied closure that
        // may not be cheap) is asked about each candidate writer once,
        // never once per slot it happens to hold in both `p` and `n`; the
        // borrow it releases is also what lets `out.p`/`out.n` be mutated
        // in the loop below.
        let candidates: BTreeSet<WriterId> = out.p.keys().chain(out.n.keys()).copied().collect();
        for w in candidates.into_iter().filter(|&w| retire(w)) {
            let p = out.p.remove(&w).unwrap_or(0);
            let n = out.n.remove(&w).unwrap_or(0);
            out.retired.insert(
                w,
                Retired {
                    p,
                    n,
                    since_ms: now_ms,
                },
            );
            changed = true;
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

            let (mut folded_p, mut folded_n) = (out.folded_p, out.folded_n);
            let folded_at = &mut out.folded_at;
            out.retired.retain(|&w, r| {
                if now_ms.saturating_sub(r.since_ms) > double_bound {
                    folded_p = folded_p.saturating_add(r.p);
                    folded_n = folded_n.saturating_add(r.n);
                    folded_at.insert(w, r.since_ms);
                    aged_any = true;
                    false
                } else {
                    true
                }
            });
            out.folded_p = folded_p;
            out.folded_n = folded_n;
            changed |= aged_any;
        }

        changed.then_some(out)
    }

    /// Drops every `folded_at` receipt older than `receipt_ttl_ms`
    /// ([`CompactionBounds::receipt_ttl_ms`]), the point past which no
    /// replica's stale copy of the writer is reconciled any more (see the
    /// module doc); `None` if no receipt is that old. Called
    /// by [`Self::compact`] on the sweep, and by the resolver on every
    /// merge apply through [`ConflictResolver::settle`], so a receipt one
    /// replica has already pruned cannot ride back in from a peer that has
    /// not: the merge unions it, the settle drops it again, and the two
    /// copies agree byte for byte.
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

    /// Postcard-encodes this counter.
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
    /// Returns the codec's error for truncated or malformed bytes, and for
    /// a writer both live in `p`/`n` and present in `retired`: a shape no
    /// constructor, merge, or compaction produces, whose first merge would
    /// change its bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        let counter: Self = postcard::from_bytes(bytes)?;
        let live_and_retired = counter
            .p
            .keys()
            .chain(counter.n.keys())
            .any(|w| counter.retired.contains_key(w));
        if live_and_retired {
            return Err(postcard::Error::DeserializeBadEncoding);
        }
        Ok(counter)
    }
}

/// Slot-wise maximum of two keyed totals: the join operation [`PnCounter`]'s
/// `p`/`n` sides use, keyed by [`WriterId`]. Safe whenever each key's own
/// value only ever grows at its source: true for a writer's own
/// cumulative delta, the only thing this crate ever merges this way.
fn pointwise_max<K: Ord + Copy>(a: &BTreeMap<K, u64>, b: &BTreeMap<K, u64>) -> BTreeMap<K, u64> {
    let mut merged = a.clone();
    for (&key, &value) in b {
        merged
            .entry(key)
            .and_modify(|existing| *existing = (*existing).max(value))
            .or_insert(value);
    }
    merged
}

/// [`pointwise_max`], then drops every writer now present in `excluding`,
/// the invariant that keeps a writer's live slot and its retired entry
/// mutually exclusive.
fn pointwise_max_excluding(
    a: &BTreeMap<WriterId, u64>,
    b: &BTreeMap<WriterId, u64>,
    excluding: &BTreeMap<WriterId, Retired>,
) -> BTreeMap<WriterId, u64> {
    let mut merged = pointwise_max(a, b);
    merged.retain(|w, _| !excluding.contains_key(w));
    merged
}

/// A [`ConflictResolver`] that merges two [`PnCounter`] values via
/// [`PnCounter::merge`] instead of picking a winner, falling back to plain
/// [`crate::Hlc`]-order `A`/`B` when either side is value-less or fails to
/// decode as a [`PnCounter`]. A unit struct, constructible as a bare value
/// like every other resolver in this crate.
#[derive(Debug, Clone, Copy, Default)]
pub struct PnCounterResolver;

impl ConflictResolver for PnCounterResolver {
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
        let (Ok(pa), Ok(pb)) = (PnCounter::decode(av), PnCounter::decode(bv)) else {
            return None;
        };
        // Deterministic given `a`/`b` alone (each side's own state already
        // carries everything its own `compact` calls decided), so `merge`
        // stays a pure function of `(key, a, b)` and symmetric under
        // argument swap.
        let bytes = pa.merge(&pb).encode().ok()?;
        Some(Merged {
            value: Bytes::from(bytes),
            // A merged counter's slots only ever accumulate, so the
            // counter as a whole never expires on its own: a TTL
            // policy for it, if any, belongs to whichever explicit
            // write set one.
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
        let counter = PnCounter::decode(value).ok()?;
        let compacted = counter.compact(now_ms, retire, quiet, bounds)?;
        let bytes = compacted.encode().ok()?;
        Some(Bytes::from(bytes))
    }

    fn settle(&self, _key: &[u8], value: &[u8], now_ms: u64, receipt_ttl_ms: u64) -> Option<Bytes> {
        let counter = PnCounter::decode(value).ok()?;
        let pruned = counter.prune_receipts(now_ms, receipt_ttl_ms)?;
        let bytes = pruned.encode().ok()?;
        Some(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
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

    /// A replica that discovers a dead writer's death *later* than a peer
    /// who has already folded that exact same writer must never defeat the
    /// peer's own receipt: an incarnation dies at most once ever, so a
    /// receipt for `w` and any `retired`/live evidence of `w` on any
    /// replica can only ever refer to that one, singular death, never a
    /// different, later one. This is not a hypothetical: it is the normal
    /// shape of two independently-ticking replicas discovering the same
    /// supersession-based death at different real times (no absence wait
    /// gates this discovery, so both discover it "immediately" from their
    /// own, differently-phased point of view), well within the trust
    /// window this module's docs otherwise bound stale data to.
    #[test]
    fn merge_never_double_counts_when_a_peer_discovers_the_same_death_after_a_fold() {
        let w = wid(1, 0);
        let bound_ms: u64 = 2_000;

        // Writer discovers `w` dead at t=0 and, entirely on its own (no
        // merge against the observer in between), progresses all the way
        // through stage two.
        let base = PnCounter::local_delta(w, 10);
        let writer_folded = base
            .compact(
                0,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("writer stage one fires at t=0")
            .compact(
                2 * bound_ms + 1,
                &|_| false,
                true,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("writer stage two fires once aged past 2x, still alone");

        // Observer discovers the exact same dead writer much later, e.g.
        // gossip propagation delay, or its own tick landing later,
        // independently retiring it at its own, later discovery time.
        let observer_retired = base
            .compact(
                2 * bound_ms + 1,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("observer stage one fires late, independently");

        // Now they finally sync, in both directions.
        for merged in [
            writer_folded.merge(&observer_retired),
            observer_retired.merge(&writer_folded),
        ] {
            assert_eq!(merged.value(), 10, "value must stay exact: {merged:?}");
            assert!(
                merged.retired.is_empty(),
                "the writer's already-folded receipt must still vouch for the observer's \
                 later-discovered, still-retired entry for the exact same writer — instead \
                 it came back: {merged:?}"
            );
        }
    }

    /// A small, mostly-colliding domain for generated counters' writer ids,
    /// so merges routinely combine two counters that share slots instead of
    /// trivially union-ing disjoint ones.
    fn writer_id() -> impl Strategy<Value = WriterId> {
        (0u64..4, 0u64..3).prop_map(|(node, inc)| WriterId::new(NodeId::from(node), inc))
    }

    fn retired_map() -> impl Strategy<Value = BTreeMap<WriterId, Retired>> {
        proptest::collection::btree_map(
            writer_id(),
            (0u64..500, 0u64..500, 0u64..100_000).prop_map(|(p, n, since_ms)| Retired {
                p,
                n,
                since_ms,
            }),
            0..3,
        )
    }

    fn folded_at_map() -> impl Strategy<Value = BTreeMap<WriterId, u64>> {
        proptest::collection::btree_map(writer_id(), 0u64..100_000, 0..3)
    }

    /// Any shape: live slots, retired entries, folded scalars, and
    /// `folded_at` receipts, with the merge invariant (retired disjoint
    /// from live) maintained by construction.
    fn counter() -> impl Strategy<Value = PnCounter> {
        (
            proptest::collection::btree_map(writer_id(), 0u64..1_000, 0..4),
            proptest::collection::btree_map(writer_id(), 0u64..1_000, 0..4),
            retired_map(),
            0u64..1_000,
            0u64..1_000,
            folded_at_map(),
        )
            .prop_map(|(mut p, mut n, retired, folded_p, folded_n, folded_at)| {
                for w in retired.keys() {
                    p.remove(w);
                    n.remove(w);
                }
                PnCounter {
                    p,
                    n,
                    retired,
                    folded_p,
                    folded_n,
                    folded_at,
                }
            })
    }

    /// Only ever a live G-counter pair: no writer has ever been retired on
    /// either side.
    fn plain_counter() -> impl Strategy<Value = PnCounter> {
        (
            proptest::collection::btree_map(writer_id(), 0u64..1_000, 0..4),
            proptest::collection::btree_map(writer_id(), 0u64..1_000, 0..4),
        )
            .prop_map(|(p, n)| PnCounter {
                p,
                n,
                ..PnCounter::default()
            })
    }

    #[test]
    fn encode_decode_round_trips_a_counter_with_several_writers() {
        let mut c = PnCounter::default();
        for node in 0..7u64 {
            c = c.merge(&PnCounter::local_delta(wid(node, 0), node));
        }
        assert_eq!(c.p.len(), 7);
        let bytes = c.encode().expect("encodes");
        assert_eq!(PnCounter::decode(&bytes).expect("decodes"), c);
    }

    #[test]
    fn local_delta_and_value_round_trip() {
        let c = PnCounter::local_delta(wid(1, 0), 5);
        assert_eq!(c.value(), 5);
    }

    #[test]
    fn local_decrement_lowers_value() {
        let w = wid(1, 0);
        let c = PnCounter::local_delta(w, 5).merge(&PnCounter::local_decrement(w, 2));
        assert_eq!(c.value(), 3);
    }

    #[test]
    fn default_is_the_empty_identity_element() {
        let empty = PnCounter::default();
        assert_eq!(empty.value(), 0);
    }

    #[test]
    fn encode_decode_round_trips() {
        let c = PnCounter::local_delta(wid(7, 0), 42);
        let bytes = c.encode().expect("encodes");
        assert_eq!(PnCounter::decode(&bytes).expect("decodes"), c);
    }

    #[test]
    fn a_second_one_shot_delta_from_the_same_writer_is_dropped_by_max_merge() {
        // Documents the intentional CRDT semantics `local_delta`'s doc
        // comment warns about: two independent "+1" deltas from the same
        // writer, rather than each carrying its cumulative total, merge to
        // 1.
        let w = wid(1, 0);
        let first = PnCounter::local_delta(w, 1);
        let second = PnCounter::local_delta(w, 1);
        assert_eq!(first.merge(&second).value(), 1);
    }

    #[test]
    fn a_restart_gets_a_fresh_slot_that_coexists_with_the_pre_restart_total() {
        let node = NodeId::from(9);
        let before_restart = WriterId::new(node, 1);
        let after_restart = WriterId::new(node, 2);
        let merged = PnCounter::local_delta(before_restart, 40)
            .merge(&PnCounter::local_delta(after_restart, 15));
        assert_eq!(
            merged.value(),
            55,
            "a restart starts a new slot, never resuming or overwriting the pre-restart one"
        );
    }

    #[test]
    fn merge_takes_the_minimum_since_ms_when_both_sides_have_retired_the_same_writer() {
        let w = wid(1, 0);
        let a = PnCounter {
            retired: BTreeMap::from([(
                w,
                Retired {
                    p: 5,
                    n: 0,
                    since_ms: 9_000,
                },
            )]),
            ..PnCounter::default()
        };
        let b = PnCounter {
            retired: BTreeMap::from([(
                w,
                Retired {
                    p: 5,
                    n: 0,
                    since_ms: 3_000,
                },
            )]),
            ..PnCounter::default()
        };
        let merged = a.merge(&b);
        assert_eq!(merged.retired[&w].since_ms, 3_000);
    }

    /// The classic stage-one exactness case a scalar-only bound would
    /// undercount: A retires `w1` at a stale value while B still holds
    /// `w1` live and current, and vice versa for `w2`; neither side has
    /// folded either writer past a per-writer `retired` entry, so this
    /// exercises stage one alone.
    #[test]
    fn merge_recovers_the_exact_total_under_cross_stale_independent_retirement() {
        let w1 = wid(1, 0);
        let w2 = wid(2, 0);
        let a = PnCounter {
            p: BTreeMap::from([(w2, 50)]),
            retired: BTreeMap::from([(
                w1,
                Retired {
                    p: 3,
                    n: 0,
                    since_ms: 0,
                },
            )]),
            ..PnCounter::default()
        };
        let b = PnCounter {
            p: BTreeMap::from([(w1, 10)]),
            retired: BTreeMap::from([(
                w2,
                Retired {
                    p: 20,
                    n: 0,
                    since_ms: 0,
                },
            )]),
            ..PnCounter::default()
        };
        let merged = a.merge(&b);
        assert_eq!(merged.value(), 60, "10 (w1's true total) + 50 (w2's)");
    }

    /// The same writer, folded to stage two independently by three
    /// replicas at three different times, converges to the exact total
    /// however the merges are ordered: since every replica's fold names
    /// the very same writer and total, taking the max of their bounds
    /// (never a sum) is exact. Taking the max of independent per-replica
    /// bounds, never a sum, keeps N independent folds of the same writer
    /// counted once.
    #[test]
    fn merge_recovers_the_exact_total_when_n_replicas_independently_fold_the_same_writer_at_different_times()
     {
        let w = wid(1, 0);
        let bound_ms = 1_000;
        let fold_at = |since_ms: u64| {
            PnCounter::local_delta(w, 77)
                .compact(
                    since_ms,
                    &|writer| writer == w,
                    false,
                    CompactionBounds::three_bounds(bound_ms),
                )
                .expect("stage one fires")
                .compact(
                    since_ms + 2 * bound_ms + 1,
                    &|_| false,
                    true,
                    CompactionBounds::three_bounds(bound_ms),
                )
                .expect("stage two fires")
        };
        let a = fold_at(0);
        let b = fold_at(100);
        let c = fold_at(250);
        assert_eq!((a.value(), b.value(), c.value()), (77, 77, 77));

        let merged = a.merge(&b).merge(&c);
        assert_eq!(
            merged.value(),
            77,
            "the same writer's total is counted once, not three times"
        );
        // Every ordering agrees, since this is a pure `max` over identical
        // per-side bounds.
        assert_eq!(
            merged.encode().unwrap(),
            c.merge(&a).merge(&b).encode().unwrap()
        );
    }

    /// Merging a compaction's own result against its pre-fold predecessor,
    /// the same lineage, not an independently-folded copy, recovers the
    /// exact pre-fold value: the pre-fold side's per-writer `retired` entry
    /// names exactly the writer the post-fold side's `folded_at` receipt
    /// already accounts for, so the two are reconciled without
    /// double-counting. `ShardOps::compact_pass` itself never merges a
    /// compaction result this way (`Engine::compact_replace_if_current`
    /// replaces the resident bytes directly instead, precisely because a
    /// merge here can restore what compaction pruned); this pins the
    /// underlying exactness property `merge` itself still needs to hold for
    /// two *distinct* replicas, one still holding a writer live or
    /// retired, the other having already folded it away, to reconcile
    /// correctly.
    #[test]
    fn a_compaction_result_merged_against_its_own_pre_fold_predecessor_is_exact() {
        let w = wid(1, 0);
        let bound_ms = 1_000;
        let resolver = PnCounterResolver;

        let base = PnCounter::local_delta(w, 42);
        let pre_fold = base
            .compact(
                0,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("stage one fires");
        let folded = pre_fold
            .compact(
                2 * bound_ms + 1,
                &|_| false,
                true,
                CompactionBounds::three_bounds(bound_ms),
            )
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
        assert_eq!(
            PnCounter::decode(&merged.value).expect("decodes").value(),
            42,
            "merging a compacted record against its own pre-fold predecessor is exact"
        );
    }

    /// A redelivered pre-fold copy of a record, merged in after the record
    /// has already been folded, is a no-op: it never resurrects the
    /// per-writer `retired` entry or double-counts the writer's total.
    #[test]
    fn a_redelivered_pre_fold_record_merged_in_after_the_fold_is_a_no_op() {
        let w = wid(1, 0);
        let bound_ms = 1_000;

        let base = PnCounter::local_delta(w, 42);
        let pre_fold = base
            .compact(
                0,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("stage one fires");
        let folded = pre_fold
            .compact(
                2 * bound_ms + 1,
                &|_| false,
                true,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("stage two fires");

        let merged = folded.merge(&pre_fold);
        assert_eq!(
            merged.encode().expect("encodes"),
            folded.encode().expect("encodes"),
            "redelivering the stale pre-fold copy changes nothing once folded"
        );
        assert_eq!(merged.value(), 42);
    }

    /// The documented bounded loss outside the trust window: a writer keeps
    /// writing through one replica after another has already folded it all
    /// the way to stage three's bare scalar, with no per-writer record left
    /// anywhere to reconcile the two. This is the same trust boundary
    /// `tombstone_max_ttl` already documents ("a member gone longer than
    /// this may resurrect data"), not a new failure mode introduced by
    /// compaction: the merge cannot know its own stale fold and the live
    /// slot name the same writer, so it counts both, bounded by exactly
    /// the stale snapshot, never unbounded, and only once a writer has
    /// reached stage three, unlike the unconditional loss a coincidental
    /// cross-writer watermark would cause at any staleness at all.
    #[test]
    fn documents_the_bounded_loss_when_a_writers_slot_outlives_everyones_fold_of_it() {
        let w = wid(1, 0);

        let already_folded = PnCounter {
            folded_p: 100,
            ..PnCounter::default()
        };
        let still_live = PnCounter {
            p: BTreeMap::from([(w, 150)]),
            ..PnCounter::default()
        };

        let merged = already_folded.merge(&still_live);

        assert_eq!(already_folded.value(), 100);
        assert_eq!(still_live.value(), 150);
        assert_eq!(
            merged.value(),
            250,
            "with no retired entry anywhere linking the two sides' view of `w`, the merge \
             counts both the already-folded total and the still-live slot: an exact, bounded \
             double count of `w`'s stale snapshot, never unbounded drift"
        );
    }

    /// Two entirely different writers, retired independently, whose
    /// retirement times happen to differ: a side that folds the
    /// *later*-retiring writer (`w3`) to stage two must never thereby
    /// "vouch" for a completely unrelated, still-exact, earlier-retiring
    /// writer (`w1`) that side has never even seen. Because `folded_at`
    /// names the specific writer it vouches for, `w1`'s exact `retired`
    /// entry survives a merge against a side that has only ever folded
    /// `w3`, and the merged total stays exact.
    #[test]
    fn merge_never_drops_a_writer_the_other_side_folded_solely_because_an_unrelated_writer_folded_later()
     {
        let w1 = wid(1, 0);
        let w3 = wid(3, 0);
        let bound_ms = 1_000;

        // Replica D: w1 is stage-one-retired only, never folded further.
        let d = PnCounter::local_delta(w1, 100)
            .compact(
                1_000,
                &|w| w == w1,
                false,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("w1 newly eligible on D");
        assert_eq!(d.value(), 100);

        // Replica C: an entirely different writer w3, independently
        // retired and folded to stage two, legitimately advancing C's own
        // bookkeeping (and its `folded_at` receipt for w3 specifically).
        // C has never heard of w1 at all.
        let c = PnCounter::local_delta(w3, 1_000)
            .compact(
                5_000,
                &|w| w == w3,
                false,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("w3 newly eligible on C")
            .compact(
                5_000 + 2 * bound_ms + 1,
                &|_| false,
                true,
                CompactionBounds::three_bounds(bound_ms),
            )
            .expect("w3 folds to stage two on C, receipt intact");
        assert_eq!(c.value(), 1_000);

        let true_total = 1_100;
        assert_eq!(
            c.merge(&d).value(),
            true_total,
            "w1's exact, still-tracked contribution must survive merging against a side that \
             has only ever folded a different writer"
        );
        assert_eq!(
            d.merge(&c).value(),
            true_total,
            "merge stays commutative while also being exact"
        );
    }

    proptest! {
        #[test]
        fn stage_one_is_exact_under_arbitrary_independent_compaction(
            writers in proptest::collection::vec(writer_id(), 1..5),
            deltas in proptest::collection::vec((0u64..1_000, 0u64..1_000), 1..5),
            retire_a in proptest::collection::vec(any::<bool>(), 1..5),
            retire_b in proptest::collection::vec(any::<bool>(), 1..5),
        ) {
            let mut base = PnCounter::default();
            for (w, &(p, n)) in writers.iter().zip(&deltas) {
                base = base
                    .merge(&PnCounter::local_delta(*w, p))
                    .merge(&PnCounter::local_decrement(*w, n));
            }
            let oracle = base.value();

            let a_retired: BTreeSet<WriterId> = writers.iter().zip(&retire_a)
                .filter(|&(_, &r)| r).map(|(&w, _)| w).collect();
            let b_retired: BTreeSet<WriterId> = writers.iter().zip(&retire_b)
                .filter(|&(_, &r)| r).map(|(&w, _)| w).collect();

            // A huge bound keeps stage two from ever firing, in `compact`
            // or in the following `merge`, isolating stage one under two
            // independently, arbitrarily different retirement decisions.
            let a = base.compact(1_000, &|w| a_retired.contains(&w), false, CompactionBounds::three_bounds(1_000_000))
                .unwrap_or_else(|| base.clone());
            let b = base.compact(1_000, &|w| b_retired.contains(&w), false, CompactionBounds::three_bounds(1_000_000))
                .unwrap_or_else(|| base.clone());

            let merged = a.merge(&b);
            prop_assert_eq!(merged.value(), oracle);
        }

        #[test]
        fn merge_is_commutative_for_any_shape(a in counter(), b in counter()) {
            prop_assert_eq!(
                a.merge(&b).encode().expect("encodes"),
                b.merge(&a).encode().expect("encodes")
            );
        }

        #[test]
        fn merge_is_idempotent_for_any_shape(a in counter()) {
            prop_assert_eq!(
                a.merge(&a).encode().expect("encodes"),
                a.encode().expect("encodes")
            );
        }

        /// Associativity holds unconditionally only while nothing has ever
        /// been retired on any side: stage two's fold is order-sensitive
        /// by design once retirement is involved (see
        /// `documents_the_bounded_loss_when_a_writers_slot_outlives_everyones_fold_of_it`
        /// above), so this is deliberately restricted to the plain
        /// live-slots-only case, matching the pre-compaction G-counter
        /// behavior this type has always had.
        #[test]
        fn merge_is_associative_three_way_all_orderings_with_no_retired_state(
            a in plain_counter(), b in plain_counter(), c in plain_counter(),
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

        #[test]
        fn encode_decode_round_trips_for_any_shape(c in counter()) {
            let bytes = c.encode().expect("encodes");
            prop_assert_eq!(PnCounter::decode(&bytes).expect("decodes"), c);
        }
    }

    #[test]
    fn compact_stage_one_moves_a_retired_writers_slots_without_changing_the_value() {
        let w = wid(1, 0);
        let c = PnCounter::local_delta(w, 30).merge(&PnCounter::local_decrement(w, 5));
        let before = c.value();

        let compacted = c
            .compact(
                1_000,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(10_000),
            )
            .expect("w is eligible");

        assert_eq!(compacted.value(), before);
        assert!(!compacted.p.contains_key(&w));
        assert!(!compacted.n.contains_key(&w));
        assert_eq!(
            compacted.retired[&w],
            Retired {
                p: 30,
                n: 5,
                since_ms: 1_000
            }
        );
    }

    #[test]
    fn compact_returns_none_when_no_writer_is_retirement_eligible() {
        let c = PnCounter::local_delta(wid(1, 0), 1);
        assert!(
            c.compact(
                1_000,
                &|_| false,
                true,
                CompactionBounds::three_bounds(10_000)
            )
            .is_none()
        );
    }

    #[test]
    fn compact_stage_two_needs_both_quiet_and_twice_the_bound() {
        let w = wid(1, 0);
        let c = PnCounter::local_delta(w, 8)
            .compact(
                0,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(1_000),
            )
            .expect("stage one fires");

        assert!(
            c.compact(
                1_500,
                &|_| false,
                true,
                CompactionBounds::three_bounds(1_000)
            )
            .is_none(),
            "1_500ms hasn't reached twice the 1_000ms bound yet"
        );
        assert!(
            c.compact(
                2_001,
                &|_| false,
                false,
                CompactionBounds::three_bounds(1_000)
            )
            .is_none(),
            "past the bound but not quiet"
        );

        let folded = c
            .compact(
                2_001,
                &|_| false,
                true,
                CompactionBounds::three_bounds(1_000),
            )
            .expect("stage two fires: quiet and past 2x the bound");
        assert!(folded.retired.is_empty());
        assert_eq!(folded.folded_p, 8);
        assert_eq!(
            folded.folded_at.get(&w),
            Some(&0),
            "a receipt for w specifically, naming w's own since_ms"
        );
        assert_eq!(folded.value(), 8);
    }

    /// A `folded_at` receipt is pruned only once it is old enough *and* the
    /// cache is quiet, counting only from before the call that created
    /// it: a single `compact` call that folds a writer never also prunes
    /// that same brand-new receipt (see [`PnCounter::compact`]'s doc for
    /// why), so this needs a further, separate call once the receipt itself
    /// has aged past three times the bound.
    #[test]
    fn a_folded_at_receipt_is_pruned_once_aged_past_three_times_the_bound_but_not_before() {
        let w = wid(1, 0);
        let folded = PnCounter::local_delta(w, 8)
            .compact(
                0,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(1_000),
            )
            .expect("stage one fires")
            .compact(
                2_001,
                &|_| false,
                true,
                CompactionBounds::three_bounds(1_000),
            )
            .expect("stage two fires");
        assert!(folded.folded_at.contains_key(&w));

        assert!(
            folded
                .compact(
                    2_999,
                    &|_| false,
                    true,
                    CompactionBounds::three_bounds(1_000)
                )
                .is_none(),
            "999ms since the receipt was written < 3x1_000ms bound: too soon to prune"
        );
        assert!(
            folded
                .compact(
                    3_001,
                    &|_| false,
                    false,
                    CompactionBounds::three_bounds(1_000)
                )
                .is_none(),
            "aged past the bound, but the cache is not quiet"
        );

        let pruned = folded
            .compact(
                3_001,
                &|_| false,
                true,
                CompactionBounds::three_bounds(1_000),
            )
            .expect("the receipt ages out: quiet and past 3x the bound");
        assert!(!pruned.folded_at.contains_key(&w));
        assert_eq!(
            pruned.folded_p, 8,
            "pruning the receipt never touches the scalar it already folded into"
        );
        assert_eq!(pruned.value(), 8);
    }

    #[test]
    fn decode_refuses_a_writer_both_live_and_retired() {
        let w = wid(1, 1);
        let mut counter = PnCounter::local_delta(w, 3);
        counter.retired.insert(
            w,
            Retired {
                p: 1,
                n: 0,
                since_ms: 5,
            },
        );
        let bytes = counter.encode().expect("encode never validates");
        assert!(PnCounter::decode(&bytes).is_err());
        let well_formed = PnCounter::local_delta(w, 3)
            .compact(10, &|_| true, false, CompactionBounds::three_bounds(1))
            .expect("retires w");
        let round_trip =
            PnCounter::decode(&well_formed.encode().expect("encode")).expect("decodes");
        assert_eq!(round_trip, well_formed);
    }

    #[test]
    fn prune_receipts_drops_only_receipts_older_than_the_ttl() {
        let w_old = wid(1, 1);
        let w_young = wid(2, 1);
        let mut counter = PnCounter::local_delta(wid(3, 1), 4);
        counter.folded_at.insert(w_old, 1_000);
        counter.folded_at.insert(w_young, 5_000);
        counter.folded_p = 9;
        let receipt_ttl_ms = 3_000;

        let pruned = counter
            .prune_receipts(5_000, receipt_ttl_ms)
            .expect("the old receipt is past three bounds");
        assert_eq!(
            pruned.folded_at.keys().copied().collect::<Vec<_>>(),
            vec![w_young]
        );
        assert_eq!(
            pruned.folded_p, 9,
            "pruning a receipt never touches the folded total"
        );
        assert_eq!(pruned.value(), counter.value());
    }

    #[test]
    fn prune_receipts_is_none_when_no_receipt_is_old_enough() {
        let mut counter = PnCounter::local_delta(wid(3, 1), 4);
        counter.folded_at.insert(wid(1, 1), 4_000);
        assert!(counter.prune_receipts(5_000, 3_000).is_none());
        assert!(
            PnCounter::local_delta(wid(3, 1), 1)
                .prune_receipts(u64::MAX, 1)
                .is_none()
        );
    }

    #[test]
    fn compact_is_idempotent() {
        let w = wid(1, 0);
        let c = PnCounter::local_delta(w, 8);
        let once = c
            .compact(
                0,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(1_000),
            )
            .expect("fires once");
        assert!(
            once.compact(
                0,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(1_000)
            )
            .is_none(),
            "re-applying with the same retire predicate changes nothing"
        );
    }

    #[test]
    fn resolver_merges_two_decodable_counters_regardless_of_argument_order() {
        let ca = PnCounter::local_delta(wid(1, 0), 3);
        let cb = PnCounter::local_delta(wid(2, 0), 4);
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
        assert_eq!(PnCounter::decode(&merged_ab).expect("decodes").value(), 7);
    }

    #[test]
    fn resolver_falls_back_to_lww_when_a_side_is_a_tombstone() {
        let c = PnCounter::local_delta(wid(1, 0), 3);
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
    fn resolver_merge_declines_on_decode_failure_and_winner_falls_back_to_lww() {
        let c = PnCounter::local_delta(wid(1, 0), 3);
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
        // `bad` fails to decode as a `PnCounter`, so `merge` declines rather
        // than merging or panicking, and `winner` degrades to plain `Hlc`
        // order; `bad`'s strictly newer version wins.
        assert!(resolver.merge(b"k", good, bad).is_none());
        assert!(resolver.merge(b"k", bad, good).is_none());
        assert_eq!(resolver.winner(b"k", good, bad), Winner::B);
        assert_eq!(resolver.winner(b"k", bad, good), Winner::A);
    }

    #[test]
    fn resolver_compact_returns_none_when_nothing_is_eligible() {
        let c = PnCounter::local_delta(wid(1, 0), 3);
        let bytes = c.encode().expect("encodes");
        assert!(
            PnCounterResolver
                .compact(
                    b"k",
                    &bytes,
                    0,
                    &|_| false,
                    true,
                    CompactionBounds::three_bounds(1_000)
                )
                .is_none()
        );
    }

    #[test]
    fn resolver_compact_folds_an_eligible_writer_and_is_idempotent() {
        let w = wid(1, 0);
        let c = PnCounter::local_delta(w, 9);
        let bytes = c.encode().expect("encodes");

        let once = PnCounterResolver
            .compact(
                b"k",
                &bytes,
                0,
                &|writer| writer == w,
                false,
                CompactionBounds::three_bounds(1_000),
            )
            .expect("w is eligible");
        assert_eq!(PnCounter::decode(&once).expect("decodes").value(), 9);

        assert!(
            PnCounterResolver
                .compact(
                    b"k",
                    &once,
                    0,
                    &|writer| writer == w,
                    false,
                    CompactionBounds::three_bounds(1_000)
                )
                .is_none(),
            "re-compacting an already-retired writer with the same predicate is a no-op"
        );
    }

    #[test]
    fn needs_value_bytes_is_true() {
        assert!(PnCounterResolver.needs_value_bytes());
    }

    #[test]
    fn merges_is_true() {
        assert!(
            PnCounterResolver.merges(),
            "PnCounterResolver's merge can return Some, so it must advertise merges()"
        );
    }

    /// Writer-slot growth: one `p` slot per distinct writer, one writer id
    /// and one `u64` total apiece, so encoded size grows with how many
    /// writers have ever incremented the counter, never with how many times
    /// any one of them has. Prints each size (`cargo test -p sundog --lib
    /// pn_counter:: -- --nocapture`) rather than pinning an exact byte
    /// count, since postcard's varint encoding of both the writer id and
    /// the cumulative total depends on their magnitude; asserts only that
    /// size grows with writer count and that the slot count itself is
    /// exact.
    #[test]
    fn encoded_size_at_3_10_and_100_distinct_writers() {
        let mut sizes = Vec::new();
        for writers in [3u64, 10, 100] {
            let mut counter = PnCounter::local_delta(wid(0, 0), 1);
            for node in 1..writers {
                counter = counter.merge(&PnCounter::local_delta(wid(node, 0), 1));
            }
            assert_eq!(
                counter.p.len(),
                usize::try_from(writers).expect("writers is a small literal, always fits"),
                "one p slot per distinct writer, none shared"
            );
            let bytes = counter.encode().expect("encodes");
            println!(
                "MEASURE pn_counter_encoded_size writers={writers} bytes={}",
                bytes.len()
            );
            sizes.push(bytes.len());
        }
        assert!(
            sizes[0] < sizes[1] && sizes[1] < sizes[2],
            "encoded size strictly grows with distinct writer count: {sizes:?}"
        );
    }
}
