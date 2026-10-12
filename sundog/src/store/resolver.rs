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
/// its own TTL, decided explicitly by the resolver; the engine never infers
/// one from `a`/`b`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merged {
    /// The merged record's postcard-encoded value bytes.
    pub value: Bytes,
    /// The merged record's own TTL; never derived from `a`/`b`.
    pub expires_at_ms: Option<u64>,
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
/// total order over one key's distinct-version records. A resolver whose
/// [`ConflictResolver::merges`] is `true` additionally commits
/// [`ConflictResolver::merge`] to the join-semilattice laws (commutative,
/// associative across repeated pairwise folds, idempotent); it runs only
/// when both sides carry a value, so
/// [`ConflictResolver::needs_value_bytes`] must stay `true` whenever
/// `merges` does. The engine, never the resolver, mints the merge
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
    /// `ver`/`expires_at_ms`, so the versioned apply skips encoding both
    /// records' values on every apply. Never override to `false` for a
    /// resolver whose [`ConflictResolver::merges`] returns `true`: see the
    /// trait docs.
    fn needs_value_bytes(&self) -> bool {
        true
    }

    /// Whether this resolver's [`ConflictResolver::merge`] can ever return
    /// `Some`. Defaults to `false`, matching [`LwwResolver`] and any other
    /// pick-a-side-only resolver. A resolver that overrides `merge` to
    /// return `Some` must override this to `true`; nothing enforces the
    /// override, but `ShardOps::merges` (via `Shard::with_resolver`)
    /// forwards it straight into anti-entropy's direction rule, so a
    /// resolver claiming `false` while `merge` returns `Some` only loses the
    /// optimization below, never correctness.
    ///
    /// Anti-entropy's `diff_bucket`/`diff_decoded` push only the greater of
    /// two differently-versioned records to the lesser side by default,
    /// costing a merging resolver two rounds per divergent key: the lesser
    /// side merges and mints a version above both inputs, and only the next
    /// round carries the fuller content back (see `merge_version`'s doc for
    /// why the mint always dominates both inputs). When `merges` is `true`,
    /// anti-entropy instead exchanges both records on a version mismatch,
    /// each side folding the other's record into its own, so both sides
    /// mint (or adopt) the identical result in the same round: see
    /// `merge_version`'s doc for why the mint arm is symmetric in its two
    /// input stamps. `true` is always correct to return, even for a
    /// resolver whose `merge` never returns `Some`; it only ever costs an
    /// extra exchange, never a correctness issue.
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
    /// resolvers this crate ships it prunes fold receipts older than that,
    /// so a receipt one replica's sweep already dropped cannot ride back in
    /// from a peer whose sweep has not reached it yet. Without this step
    /// two replicas re-import each other's receipts through anti-entropy
    /// indefinitely, since a merge only ever unions them.
    fn settle(&self, key: &[u8], value: &[u8], now_ms: u64, receipt_ttl_ms: u64) -> Option<Bytes> {
        let _ = (key, value, now_ms, receipt_ttl_ms);
        None
    }
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
