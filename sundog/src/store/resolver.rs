//! The conflict-resolution contract: the record view a resolver compares, the
//! merged result it returns, and the default last-write-wins resolver.

use bytes::Bytes;

use super::{CompactionBounds, Quiescence, crdt};
use crate::hlc::Hlc;

/// One side of a [`ConflictResolver::winner`] comparison: everything a resolver
/// needs to pick a winner, at the wire level. Carries postcard-encoded value
/// bytes rather than the typed value, matching this module's "local reads never
/// deserialize" boundary.
#[derive(Debug, Clone, Copy)]
pub struct RecordView<'a> {
    /// The record's postcard-encoded value bytes, or `None` for a tombstone.
    pub value: Option<&'a [u8]>,
    /// The record's version.
    pub ver: Hlc,
    /// Absolute expiry in epoch milliseconds, or `None` for no TTL/no value.
    pub expires_at_ms: Option<u64>,
}

/// The outcome of a [`ConflictResolver::winner`] call: which argument record
/// wins, by position rather than role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Winner {
    /// The first record (`a`) wins; `b` is discarded.
    A,
    /// The second record (`b`) wins; `a` is discarded.
    B,
}

/// The outcome of a [`ConflictResolver::merge`] call: the folded value and
/// its own expiry, decided explicitly by the resolver; the engine never infers
/// one from `a`/`b`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merged {
    /// The merged record's postcard-encoded value bytes.
    pub value: Bytes,
    /// The merged record's absolute expiry in epoch milliseconds, or `None`
    /// for no expiry. The shipped resolvers set it to [`later_deadline`] of
    /// the two sides' deadlines.
    pub expires_at_ms: Option<u64>,
}

/// The merged expiry of two records: the later deadline, with an unset side
/// deferring to the other. `(Some(x), Some(y))` is the larger of the two,
/// `(Some(x), None)` and `(None, Some(x))` are `Some(x)`, and `(None, None)`
/// is `None`. Commutative, associative and idempotent, so it is a valid join
/// for [`Merged::expires_at_ms`]; the shipped merging resolvers use it.
///
/// A merged value therefore keeps the longest lifetime any contribution
/// asked for: [`crate::cache::Cache::persist`] leaves its deadline unchanged,
/// [`crate::cache::Cache::expire`] extends it but never shortens it, and
/// [`crate::cache::Cache::touch`] moves it to the default TTL from now when
/// that is later.
#[must_use]
pub fn later_deadline(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    a.max(b)
}

/// Picks a winner between two differently-versioned records for the same
/// key; a resolver never merges on its own, since a synthesized value
/// would make `Shard::apply`'s outcome depend on which two versions
/// happened to collide locally.
///
/// # Correctness contract
///
/// Convergence requires `winner` to be deterministic, antisymmetric
/// (`winner(key, a, b) == A` iff `winner(key, b, a) == B`), and a strict
/// total order over one key's distinct-version records. The order need not
/// follow versions: a resolver may prefer a record with a lower version.
/// Anti-entropy exchanges both records of a version-mismatched key for every
/// resolver that reads values ([`ConflictResolver::needs_value_bytes`] is
/// `true`), so the replica that holds the winner at the lower version sends
/// it, and each side's `winner` decides what it keeps; the adopting replica
/// stores the winner under its own, lower, version. A resolver that returns
/// `false` from `needs_value_bytes` promises that `winner` prefers the
/// greater version, since only the greater side travels for it. A tombstone
/// and a value never reach `winner`: the engine settles that pair by
/// version alone for every resolver, the higher version winning and the
/// stored side staying on a tie, so a resolver cannot revive a deleted
/// key. A resolver whose
/// [`ConflictResolver::merges`] is `true` additionally commits
/// [`ConflictResolver::merge`] to the join-semilattice laws (commutative,
/// associative across repeated pairwise folds, idempotent) over the merged
/// value and over the merged expiry: [`Merged::expires_at_ms`] must itself be
/// a join of the two deadlines, which [`later_deadline`] is and the shipped
/// resolvers use. `merge` runs only when both sides carry a value, so
/// [`ConflictResolver::needs_value_bytes`] must stay `true` whenever
/// `merges` does. The engine never folds a lapsed side (a record whose
/// deadline is at or before the other side's write, or a stored entry that
/// reads as absent on the applying node), so a resolver need not check
/// deadlines itself. Dropping a lapsed side is not a join, so replicas that
/// apply the same writes in different orders can differ once a deadline
/// falls between them: a merged record keeps the longest lifetime among its
/// parts, so a longer-lived record can carry a lapsed contribution back to a
/// replica that dropped it, and the merge coalescing window changes which
/// contributions survive. The engine, never the resolver, mints the merge
/// outcome's version. Configuration is local per `Shard`, never
/// wire-negotiated: every node must run an identical resolver for a given
/// cache.
pub trait ConflictResolver: Send + Sync + 'static {
    /// Decides which of `a`, `b`, two different versions of the record stored
    /// at `key`'s wire-encoded bytes, wins. See the trait docs for the
    /// correctness contract.
    fn winner(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner;

    /// Whether `winner` reads `RecordView::value`. Defaults to `true`. Override
    /// to `false` for a resolver that, like [`LwwResolver`], only compares
    /// `ver`, so the versioned apply skips encoding both records' values on
    /// every apply. Never override to `false` for a resolver whose
    /// [`ConflictResolver::merges`] returns `true`: see the trait docs.
    ///
    /// The answer selects anti-entropy's repair direction. Returning `false`
    /// is a promise that `winner` prefers the greater version, so
    /// anti-entropy repairs one way, the greater record travelling to the
    /// replica that holds the lesser. A resolver that returns `false` yet
    /// prefers a lower version, or compares only `expires_at_ms`, leaves
    /// replicas apart after a lost replicate. Returning `false` from a
    /// version-ordered resolver is also a bandwidth optimisation, since a
    /// value-reading resolver has each mismatched key's two records both
    /// travel.
    fn needs_value_bytes(&self) -> bool {
        true
    }

    /// Whether this resolver's [`ConflictResolver::merge`] can ever return
    /// `Some`. Defaults to `false`, matching [`LwwResolver`] and any other
    /// pick-a-side-only resolver. A resolver that overrides `merge` to
    /// return `Some` must override this to `true`: the engine runs `merge`
    /// only while this is `true`, and nothing enforces the override.
    ///
    /// The answer also decides what an `Invalidation` cache sends: a
    /// merging resolver's writes travel as records that a holding peer
    /// folds in, any other resolver's as invalidations. Return `true` only
    /// for a resolver whose `merge` can return `Some`.
    ///
    /// Anti-entropy reads the answer too. When `merges` or
    /// [`ConflictResolver::needs_value_bytes`] is `true`, it exchanges both
    /// records on a version mismatch, each side folding the other's record
    /// into its own, so both sides mint (or adopt) the identical result in
    /// the same round: see `merge_version`'s doc for why the mint arm is
    /// symmetric in its two input stamps. A resolver with
    /// `needs_value_bytes() == false` instead has only the greater record
    /// travel, which costs a merging one two rounds per divergent key; that
    /// pairing is a contract violation.
    fn merges(&self) -> bool {
        false
    }

    /// Folds `a` and `b` into a third record. `None` falls back to
    /// [`ConflictResolver::winner`]. Only consulted when
    /// [`ConflictResolver::merges`] is `true` and both sides carry a value.
    /// See the trait docs for the correctness contract a resolver that
    /// returns `Some` commits to.
    fn merge(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let _ = (key, a, b);
        None
    }

    /// Folds writer identities out of `value`'s live state into a bounded,
    /// already-retired representation, returning freshly encoded bytes
    /// when anything changed; `None` when nothing is retirement-eligible or
    /// this resolver doesn't compact (the default). Only called while
    /// [`ConflictResolver::merges`] is `true`; `now_ms` is the sweep's
    /// timestamp, threaded through for a pure decision.
    ///
    /// `retire(w)` says whether writer `w` is dead: absent from the
    /// cluster longer than [`crate::config::ClusterConfig::crdt_retire_after`],
    /// or on a different incarnation than `w`'s own. A resolver's first
    /// stage moves every writer `retire` accepts into a per-writer retired
    /// entry stamped with `now_ms`, exact under any inter-replica
    /// staleness since a slot and its retired entry are always maxed
    /// together at merge time. `quiet` and `bounds` gate an optional
    /// second stage that folds a retired entry older than `2 *
    /// bounds.retire_after_ms` on a quiet cache into a bounded
    /// accumulator, trading per-writer exactness for a bounded record
    /// size under the same trust boundary
    /// [`crate::config::ClusterConfig::tombstone_max_ttl`] documents; see
    /// [`crate::crdt::PnCounter`]'s docs for how its own second stage stays
    /// exact under this trade. A resolver with no second stage ignores
    /// `quiet`/`bounds`.
    fn compact(
        &self,
        key: &[u8],
        value: &[u8],
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
    ) -> Option<Bytes> {
        let _ = (key, value, now_ms, retire, quiet, bounds);
        None
    }

    /// Drops the metadata in `value`, `key`'s freshly merged record, whose
    /// reconciliation window has closed as of `now_ms`, returning the
    /// settled bytes, or `None` when nothing in it is that old. Defaults
    /// to `None`. Called on every merge apply, right after
    /// [`ConflictResolver::merge`], with the `receipt_ttl_ms` of the same
    /// [`CompactionBounds`] as [`ConflictResolver::compact`]: for the
    /// resolvers this crate ships it prunes fold receipts more than that
    /// past their fold, and receipts stamped that far ahead of `now_ms`, so a
    /// receipt one replica's sweep already dropped cannot ride back in
    /// from a peer whose sweep has not reached it yet. Without this step
    /// two replicas re-import each other's receipts through anti-entropy
    /// indefinitely, since a merge only ever unions them.
    fn settle(&self, key: &[u8], value: &[u8], now_ms: u64, receipt_ttl_ms: u64) -> Option<Bytes> {
        let _ = (key, value, now_ms, receipt_ttl_ms);
        None
    }
}

/// Whether anti-entropy sends both records of a version-mismatched key,
/// rather than only the greater one to the replica holding the lesser, for a
/// resolver with these answers: when it merges or reads values, because only
/// such a resolver can prefer the lower version. `merges` without
/// `needs_value_bytes` breaks [`ConflictResolver`]'s contract; the answer
/// stays `true` so the replicas still converge.
pub(crate) const fn exchanges_both_records(merges: bool, needs_value_bytes: bool) -> bool {
    merges || needs_value_bytes
}

/// The default resolver: last-write-wins by [`Hlc`], ignoring value bytes and
/// `expires_at_ms` entirely.
#[derive(Debug, Clone, Copy, Default)]
pub struct LwwResolver;

impl ConflictResolver for LwwResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn needs_value_bytes(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{exchanges_both_records, later_deadline};

    #[test]
    fn exchanges_both_records_is_the_or_of_merging_and_value_reading() {
        assert!(!exchanges_both_records(false, false), "version-only");
        assert!(exchanges_both_records(false, true), "reads values");
        assert!(exchanges_both_records(true, true), "merges");
        assert!(
            exchanges_both_records(true, false),
            "a merging resolver that declares no value reads still converges"
        );
    }

    #[test]
    fn later_deadline_takes_the_larger_of_two_set_deadlines() {
        assert_eq!(later_deadline(Some(5), Some(9)), Some(9));
        assert_eq!(later_deadline(Some(9), Some(5)), Some(9));
        assert_eq!(later_deadline(Some(7), Some(7)), Some(7));
    }

    #[test]
    fn later_deadline_defers_to_the_set_side() {
        assert_eq!(later_deadline(Some(5), None), Some(5));
        assert_eq!(later_deadline(None, Some(5)), Some(5));
    }

    #[test]
    fn later_deadline_of_two_unset_sides_is_unset() {
        assert_eq!(later_deadline(None, None), None);
    }

    #[test]
    fn later_deadline_is_a_join() {
        let cases = [None, Some(0), Some(3), Some(u64::MAX)];
        for a in cases {
            assert_eq!(later_deadline(a, a), a, "idempotent");
            for b in cases {
                assert_eq!(later_deadline(a, b), later_deadline(b, a), "commutative");
                for c in cases {
                    assert_eq!(
                        later_deadline(later_deadline(a, b), c),
                        later_deadline(a, later_deadline(b, c)),
                        "associative"
                    );
                }
            }
        }
    }
}
