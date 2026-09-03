//! A partitioned invertible Bloom lookup table (IBLT) over anti-entropy
//! bucket elements — `(key_hash, version)` pairs, `key_hash = xxh3_64(key
//! bytes)` (the same hash [`crate::store::bucket_of`] takes the low bits
//! of) — used by [`super::anti_entropy`] to reconcile a large mismatched
//! bucket without shipping its full entry list.
//!
//! # How this differs from a Bloom filter
//!
//! A Bloom filter answers "is this element a member?"; an IBLT answers "what
//! is the *symmetric difference* between two sets?" without either side ever
//! sending its actual elements. Each side inserts its own bucket's elements
//! into an [`Iblt`] of the same shape, one side sends its sketch to the
//! other, the receiver [`Iblt::subtract`]s the two, and [`Iblt::peel`]s the
//! result: a per-cell XOR/count structure that, so long as the two sets
//! don't differ by more than the sketch's rated capacity
//! ([`RATED_CAPACITY`]), can be unwound one element at a time back into the
//! exact list of elements only on one side or the other. Unlike a bucket's
//! full listing (O(bucket size) on the wire), a sketch's wire cost is fixed
//! by its cell count regardless of how many elements went into it — cheap
//! exactly when the bucket is large and the actual diff is small, which is
//! the anti-entropy common case.
//!
//! # Structure
//!
//! [`IBLT_PARTITIONS`] (3) equal-length partitions of [`Cell`]s. An
//! element's cell in partition `p` is `mix(placement_hash, p) %
//! partition_len` — a distinct mix per partition, so one element always
//! lands in exactly one cell per partition (three cells total, generally
//! distinct). This is the "hedge" against a single bad hash collision:
//! partitioning trades a little locality for a correctness property the
//! peel loop below leans on — an element's three cells are never all three
//! the *same* cell.
//!
//! `placement_hash` is [`check`]'s hash of the *whole* element — `key_hash`
//! folded together with its version — not `key_hash` alone. Placing by
//! `key_hash` alone was the first thing tried here, and it does not work:
//! two different versions of the *same* key would then always occupy the
//! same three cells regardless of version, so subtracting one side's insert
//! from the other's always cancels their `count` contribution to zero
//! (`1 - 1`) while still leaving their differing version bytes `XOR`ed
//! together in the cell's sums — a residual that can never become a
//! verified-pure cell and permanently blocks that cell from ever
//! decoding, for *any* sketch size. Since the whole point of anti-entropy
//! is reconciling the same key at different versions, that failure mode
//! isn't a rare edge case, it's most of what this module exists to handle
//! — hence hashing the whole element for placement, so two versions of one
//! key are just two independent elements as far as placement is concerned.
//!
//! # Never wrong, only sometimes undecodable
//!
//! [`Iblt::peel`] only ever returns [`Decoded`] when it has *verified*, cell
//! by cell, that a candidate element's checksum matches its claimed
//! `key_hash`/version — a "pure" cell (`count` of exactly `1` or `-1`) whose
//! `check_sum` doesn't match is left alone rather than trusted. If the
//! difference is too large for the sketch to resolve fully, some cells are
//! left non-zero at the end and this returns [`Undecodable`] — a decode
//! failure never surfaces a wrong element, only "try the fallback path"
//! (`AeEntries`/`Msg::AeBucket`, `super::anti_entropy`'s job once this
//! returns [`Undecodable`]).

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::hlc::Hlc;
use crate::node::NodeId;

/// Number of independent hash partitions each element is inserted into —
/// see this module's docs for why partitioning (rather than `k` independent
/// hashes into one flat table) is the shape used here.
pub(crate) const IBLT_PARTITIONS: usize = 3;

/// Symmetric-difference size the crate's default sketch shape
/// ([`crate::config::ClusterConfig::ae_sketch_cells`]'s default of 951
/// cells, 317 per partition at [`IBLT_PARTITIONS`] = 3) is rated for.
///
/// This is a *statistical*, not absolute, guarantee: two elements that
/// happen to land in the exact same cell in all three partitions (an
/// [`IBLT_PARTITIONS`]-way hash collision, roughly `1 / partition_len^3` per
/// pair at random) leave that trio of cells non-pure and unresolvable —
/// vanishingly unlikely for any one pair, but with up to
/// `RATED_CAPACITY / 2` elements possibly pairwise-colliding, not literally
/// zero. At this constant's value, direct Monte Carlo sampling (tens of
/// thousands of random trials at this exact sketch shape, well beyond what
/// the property tests below run on every `cargo test`) puts the
/// per-difference-pair failure rate low enough that a full 1024-case
/// property-test run turning up even one `Undecodable` inside the rated
/// capacity is itself a rare event — not a mathematical impossibility, so a
/// red run here first re-runs before assuming a real regression, but not
/// one to expect in normal use. Past this size, decode failures become the
/// norm rather than an outlier (still never a *wrong* answer, only
/// `Undecodable` — see this module's docs); this constant is the boundary
/// the property tests hold that near-certain "always exact" property to,
/// not a hard cutoff enforced anywhere at runtime.
///
/// 317 is prime, not a round/highly-divisible number like 240 or 320 — this
/// matters because proptest's own integer shrinker narrows a failing case by
/// bisection, which tends to land two colliding elements' hash values a
/// suspiciously round (power-of-two-ish) distance apart; a highly composite
/// `partition_len` (say, one sharing factors of 2 and 5) lets that shrunk
/// distance stay a multiple of it far more often than genuine random luck
/// would predict, turning one real but rare statistical collision into a
/// *permanently reproducible* one once proptest saves the shrunk case to
/// `proptest-regressions`. A prime `partition_len` has (almost) no small
/// factors in common with a shrinker-produced delta, so this class of
/// self-inflicted flakiness — distinct from the genuine, inherent
/// `1 / partition_len^3` collision risk above — stays as unlikely as
/// intended.
// Only read by the property tests (`prop_tests`, `#[cfg(test)]`) that pin
// it; a non-test build never evaluates it, hence the otherwise-unused
// warning this silences.
#[allow(dead_code)]
pub(crate) const RATED_CAPACITY: usize = 40;

/// Distinct per-partition salts folded into [`mix`] — arbitrary odd 64-bit
/// constants (borrowed from well-known mixing constants), not secret; their
/// only job is making the three partitions' hash functions independent of
/// each other.
const PARTITION_SALTS: [u64; IBLT_PARTITIONS] = [
    0x9E37_79B9_7F4A_7C15,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
];

/// The per-partition hash used to place a `placement_hash` (this module's
/// docs — [`check`]'s hash of a whole element) in partition `p`: salting
/// then re-hashing rather than reusing `placement_hash` directly keeps the
/// three partitions' placements independent (a hash that happens to
/// collide with another element's in one partition is very unlikely to
/// collide with it in the other two).
fn mix(placement_hash: u64, partition: usize) -> u64 {
    xxh3_64(&(placement_hash ^ PARTITION_SALTS[partition]).to_le_bytes())
}

/// `xxh3_64` over the concatenated little-endian bytes of `(key_hash,
/// wall_ms, logical, node)` — the fingerprint [`Iblt::peel`] checks a
/// candidate "pure" cell's accumulated sums against before trusting it as a
/// real, single element rather than an accidental combination of several.
fn check(key_hash: u64, wall_ms: u64, logical: u32, node: u64) -> u64 {
    let mut buf = [0u8; 8 + 8 + 4 + 8];
    buf[0..8].copy_from_slice(&key_hash.to_le_bytes());
    buf[8..16].copy_from_slice(&wall_ms.to_le_bytes());
    buf[16..20].copy_from_slice(&logical.to_le_bytes());
    buf[20..28].copy_from_slice(&node.to_le_bytes());
    xxh3_64(&buf)
}

/// One IBLT cell: `count`, and XOR sums of every accumulated element's
/// `key_hash` and version fields, plus `check_sum` — the fingerprint used to
/// verify a "pure" cell genuinely holds exactly one element (this module's
/// docs, "Never wrong, only sometimes undecodable"). `Default` is the empty
/// cell (every field zero), what every cell in a freshly built `Iblt`
/// starts as.
///
/// Reachable outside `cluster::sketch` only because
/// [`crate::wire::Msg::AeSketch`] has to carry a `Vec<Cell>` on the wire
/// (re-exported as [`crate::wire::Cell`]) — every field stays private to
/// this module; nothing outside it constructs, inspects, or matches on one.
/// Serde derives postcard-encode this as five varints plus a signed varint
/// for `count` (zigzag-encoded), so an empty cell costs six single bytes on
/// the wire — cheap, but not the typical cell in the sketch a responder
/// actually sends: `Msg::AeSketch` carries the *un-subtracted* sketch built
/// from every entry the responder's own bucket holds (not the diff, which
/// only the initiator ever computes, after subtracting), so how many cells
/// stay empty depends on the bucket's population against
/// [`crate::config::ClusterConfig::ae_sketch_cells`], not on
/// `RATED_CAPACITY`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cell {
    count: i32,
    key_sum: u64,
    wall_sum: u64,
    logical_sum: u32,
    node_sum: u64,
    check_sum: u64,
}

impl Cell {
    const fn is_zero(self) -> bool {
        self.count == 0
            && self.key_sum == 0
            && self.wall_sum == 0
            && self.logical_sum == 0
            && self.node_sum == 0
            && self.check_sum == 0
    }

    /// `true` if this cell's `count` claims exactly one net element *and*
    /// its accumulated sums check out as a real, single `(key_hash, ver)`
    /// rather than an accidental combination — the only condition under
    /// which [`Iblt::peel`] trusts a cell enough to peel it.
    fn is_pure_and_valid(self) -> bool {
        (self.count == 1 || self.count == -1)
            && self.check_sum == check(self.key_sum, self.wall_sum, self.logical_sum, self.node_sum)
    }

    /// The element a pure cell claims to hold, read straight off its
    /// accumulated sums. Only meaningful when [`Cell::is_pure_and_valid`]
    /// holds — callers only ever call this after checking that.
    fn claimed_elem(self) -> Elem {
        Elem {
            key_hash: self.key_sum,
            ver: Hlc {
                wall_ms: self.wall_sum,
                logical: self.logical_sum,
                node: NodeId::from(self.node_sum),
            },
        }
    }
}

/// One IBLT element: a key's `xxh3_64` hash paired with its version — never
/// the key's actual bytes, since the whole point of a sketch is decoding
/// without either side shipping its elements. A pulled-only element is
/// therefore always pulled by hash ([`crate::wire::Msg::AePullHashes`]),
/// never by key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Elem {
    pub(crate) key_hash: u64,
    pub(crate) ver: Hlc,
}

/// [`Iblt::peel`]'s success case: every element present on one side of the
/// `subtract` but not the other, split by which side. `only_left` is what
/// the sketch that called `subtract` had and the other didn't; `only_right`
/// is the reverse.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Decoded {
    pub(crate) only_left: Vec<Elem>,
    pub(crate) only_right: Vec<Elem>,
}

/// [`Iblt::peel`]'s failure case: the symmetric difference between the two
/// sketches was too large (or, vanishingly unlikely, an accidental
/// checksum collision blocked a real cell) for peeling to fully resolve
/// every cell back to zero. Never means a *wrong* result was returned —
/// only that no result could be, this time; the caller's fallback path
/// (`AeEntries`, `super::anti_entropy`'s job) is exact regardless of size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Undecodable;

/// A partitioned IBLT over `(key_hash, version)` elements — see this
/// module's docs for the overall scheme.
#[derive(Debug, Clone)]
pub(crate) struct Iblt {
    cells: Vec<Cell>,
    partition_len: usize,
}

impl Iblt {
    /// Builds an empty sketch with (approximately) `cells` total cells,
    /// split evenly across [`IBLT_PARTITIONS`] partitions (each
    /// `(cells / IBLT_PARTITIONS).max(1)` long, so a tiny or zero `cells`
    /// still yields a well-formed, if uselessly small, sketch rather than
    /// an empty one — this does not require `cells` to make the resulting
    /// `partition_len` prime, but [`RATED_CAPACITY`]'s docs are why the
    /// crate's own default does). Pass
    /// [`crate::config::ClusterConfig::ae_sketch_cells`] for the responder's
    /// own outbound sketch; the initiator instead builds its comparison
    /// sketch with [`Iblt::new`] over the *received* sketch's own
    /// `cells.len()` (via [`Iblt::from_cells`]'s companion sizing), so the
    /// two are always shape-compatible regardless of config drift between
    /// nodes.
    pub(crate) fn new(cells: usize) -> Self {
        let partition_len = (cells / IBLT_PARTITIONS).max(1);
        Self {
            cells: vec![Cell::default(); partition_len * IBLT_PARTITIONS],
            partition_len,
        }
    }

    /// Rebuilds a sketch from cells received off the wire
    /// ([`crate::wire::Msg::AeSketch`]'s payload) — `partition_len` is
    /// re-derived from `cells.len()` the same way [`Iblt::new`] derives it
    /// from a requested cell count, so a sketch built with [`Iblt::new`]
    /// and one rebuilt with this from its own `into_cells()` output always
    /// agree on shape.
    pub(crate) fn from_cells(cells: Vec<Cell>) -> Self {
        let partition_len = (cells.len() / IBLT_PARTITIONS).max(1);
        Self {
            cells,
            partition_len,
        }
    }

    /// Unwraps this sketch's cells for the wire
    /// ([`crate::wire::Msg::AeSketch`]'s payload).
    pub(crate) fn into_cells(self) -> Vec<Cell> {
        self.cells
    }

    /// The three cell indices a `placement_hash` (this module's docs —
    /// [`check`]'s hash of a whole element, not a bare `key_hash`) maps to,
    /// one per partition — always three distinct offsets *within* their own
    /// partition, so always three cells overall (never the same cell
    /// twice: each partition owns a disjoint slice of `self.cells`).
    fn locations(&self, placement_hash: u64) -> [usize; IBLT_PARTITIONS] {
        let partition_len_u64 =
            u64::try_from(self.partition_len).expect("invariant: partition_len fits in u64");
        std::array::from_fn(|p| {
            let offset = mix(placement_hash, p) % partition_len_u64;
            let offset = usize::try_from(offset)
                .expect("invariant: a value taken mod partition_len always fits in usize");
            p * self.partition_len + offset
        })
    }

    /// Folds `(key_hash, ver)` into this sketch with `sign` (`1` to insert,
    /// `-1` to remove) at each of its three cells, placed by the whole
    /// element's hash (this module's docs — never by `key_hash` alone).
    fn apply(&mut self, key_hash: u64, ver: Hlc, sign: i32) {
        let c = check(key_hash, ver.wall_ms, ver.logical, ver.node.as_u64());
        for idx in self.locations(c) {
            let cell = &mut self.cells[idx];
            cell.count += sign;
            cell.key_sum ^= key_hash;
            cell.wall_sum ^= ver.wall_ms;
            cell.logical_sum ^= ver.logical;
            cell.node_sum ^= ver.node.as_u64();
            cell.check_sum ^= c;
        }
    }

    /// Inserts one `(key_hash, ver)` element.
    pub(crate) fn insert(&mut self, key_hash: u64, ver: Hlc) {
        self.apply(key_hash, ver, 1);
    }

    /// The symmetric-difference sketch: cell-wise `count` subtraction and
    /// XOR of every other field. An element present (with the same version)
    /// on both sides cancels out to a zero cell in every one of its three
    /// locations, exactly as if neither side had ever inserted it; an
    /// element present on only one side, or at different versions on each,
    /// survives as a nonzero contribution [`Iblt::peel`] can later resolve.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` have a different cell count — the two
    /// sketches being compared are always shape-compatible by construction
    /// (see [`Iblt::new`]'s docs), so this is an invariant violation, not a
    /// case this crate expects to hit.
    pub(crate) fn subtract(&self, other: &Self) -> Self {
        assert_eq!(
            self.cells.len(),
            other.cells.len(),
            "invariant: subtracting IBLTs of different cell counts"
        );
        let cells = self
            .cells
            .iter()
            .zip(&other.cells)
            .map(|(a, b)| Cell {
                count: a.count - b.count,
                key_sum: a.key_sum ^ b.key_sum,
                wall_sum: a.wall_sum ^ b.wall_sum,
                logical_sum: a.logical_sum ^ b.logical_sum,
                node_sum: a.node_sum ^ b.node_sum,
                check_sum: a.check_sum ^ b.check_sum,
            })
            .collect();
        Self {
            cells,
            partition_len: self.partition_len,
        }
    }

    /// Unwinds this (already-subtracted) sketch back into the exact list of
    /// elements only on the `self` ("left") side and only on the `other`
    /// ("right") side of that subtraction — see this module's docs for the
    /// "never wrong, only sometimes undecodable" guarantee this upholds.
    ///
    /// Standard IBLT peeling: repeatedly finds a cell that is *verified*
    /// pure (`Cell::is_pure_and_valid`), records the element it claims,
    /// subtracts that element back out of all three of its cells (which
    /// zeroes the cell just peeled, and may turn one of the other two pure
    /// in turn), and repeats via a work queue until no more verified-pure
    /// cells remain. If every cell has returned to zero at that point, the
    /// decode is exact and complete; otherwise the remaining nonzero cells
    /// mean the difference was too large (or an astronomically unlikely
    /// checksum collision blocked a real cell) and this returns
    /// [`Undecodable`] instead of a partial or guessed result.
    pub(crate) fn peel(mut self) -> Result<Decoded, Undecodable> {
        let mut only_left = Vec::new();
        let mut only_right = Vec::new();
        let mut queue: std::collections::VecDeque<usize> = (0..self.cells.len())
            .filter(|&idx| self.cells[idx].is_pure_and_valid())
            .collect();

        while let Some(idx) = queue.pop_front() {
            let cell = self.cells[idx];
            if !cell.is_pure_and_valid() {
                continue; // touched again since it was enqueued; re-checked below if it re-purifies
            }
            let elem = cell.claimed_elem();
            let sign = cell.count;
            // `cell.check_sum` — already verified equal to `check(key_sum,
            // wall_sum, logical_sum, node_sum)` by `is_pure_and_valid` above
            // — *is* this element's placement hash (this module's docs), so
            // re-deriving it from `elem`'s fields would just recompute the
            // same value.
            let placement_hash = cell.check_sum;
            for target in self.locations(placement_hash) {
                let c = &mut self.cells[target];
                c.count -= sign;
                c.key_sum ^= elem.key_hash;
                c.wall_sum ^= elem.ver.wall_ms;
                c.logical_sum ^= elem.ver.logical;
                c.node_sum ^= elem.ver.node.as_u64();
                c.check_sum ^= placement_hash;
                if self.cells[target].is_pure_and_valid() {
                    queue.push_back(target);
                }
            }
            if sign == 1 {
                only_left.push(elem);
            } else {
                only_right.push(elem);
            }
        }

        if self.cells.iter().all(|c| c.is_zero()) {
            Ok(Decoded {
                only_left,
                only_right,
            })
        } else {
            Err(Undecodable)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn ver(wall_ms: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(1),
        }
    }

    fn sketch_of(elems: &[(u64, Hlc)], cells: usize) -> Iblt {
        let mut iblt = Iblt::new(cells);
        for &(key_hash, v) in elems {
            iblt.insert(key_hash, v);
        }
        iblt
    }

    #[test]
    fn empty_sketches_subtract_and_peel_to_nothing() {
        let a = Iblt::new(240);
        let b = Iblt::new(240);
        let decoded = a.subtract(&b).peel().expect("empty diff always decodes");
        assert!(decoded.only_left.is_empty());
        assert!(decoded.only_right.is_empty());
    }

    #[test]
    fn identical_sketches_cancel_exactly() {
        let elems = [(1u64, ver(10)), (2, ver(20)), (3, ver(30))];
        let a = sketch_of(&elems, 240);
        let b = sketch_of(&elems, 240);
        let decoded = a.subtract(&b).peel().expect("identical sets decode");
        assert!(decoded.only_left.is_empty());
        assert!(decoded.only_right.is_empty());
    }

    #[test]
    fn one_sided_insert_decodes_as_only_left() {
        let a = sketch_of(&[(1, ver(10))], 240);
        let b = Iblt::new(240);
        let decoded = a.subtract(&b).peel().expect("small diff decodes");
        assert_eq!(
            decoded.only_left,
            vec![Elem {
                key_hash: 1,
                ver: ver(10)
            }]
        );
        assert!(decoded.only_right.is_empty());
    }

    #[test]
    fn one_sided_insert_on_the_other_side_decodes_as_only_right() {
        let a = Iblt::new(240);
        let b = sketch_of(&[(7, ver(10))], 240);
        let decoded = a.subtract(&b).peel().expect("small diff decodes");
        assert!(decoded.only_left.is_empty());
        assert_eq!(
            decoded.only_right,
            vec![Elem {
                key_hash: 7,
                ver: ver(10)
            }]
        );
    }

    #[test]
    fn same_key_different_version_appears_on_both_sides() {
        let a = sketch_of(&[(1, ver(20))], 240);
        let b = sketch_of(&[(1, ver(10))], 240);
        let decoded = a.subtract(&b).peel().expect("small diff decodes");
        assert_eq!(
            decoded.only_left,
            vec![Elem {
                key_hash: 1,
                ver: ver(20)
            }]
        );
        assert_eq!(
            decoded.only_right,
            vec![Elem {
                key_hash: 1,
                ver: ver(10)
            }]
        );
    }

    #[test]
    fn a_large_symmetric_difference_is_either_exact_or_undecodable_never_wrong() {
        let cells = 24; // deliberately small, to force undecodable outcomes
        let left: Vec<(u64, Hlc)> = (0..200u64).map(|k| (k, ver(k))).collect();
        let right: Vec<(u64, Hlc)> = (100..300u64).map(|k| (k, ver(k + 1))).collect();
        let a = sketch_of(&left, cells);
        let b = sketch_of(&right, cells);
        // `Err(Undecodable)` is also an acceptable outcome here (an
        // oversubscribed sketch reporting it can't decode, never a wrong
        // answer) — only a successful decode has anything further to check.
        if let Ok(decoded) = a.subtract(&b).peel() {
            let left_set: HashSet<Elem> = left
                .iter()
                .filter(|&&(k, _)| !(100..200).contains(&k))
                .map(|&(key_hash, v)| Elem { key_hash, ver: v })
                .collect();
            let expected_left: HashSet<Elem> = decoded.only_left.iter().copied().collect();
            assert_eq!(expected_left, left_set, "a real decode must be exact");
            let right_set: HashSet<Elem> = right
                .iter()
                .filter(|&&(k, _)| !(100..200).contains(&k))
                .map(|&(key_hash, v)| Elem { key_hash, ver: v })
                .collect();
            let expected_right: HashSet<Elem> = decoded.only_right.iter().copied().collect();
            assert_eq!(expected_right, right_set, "a real decode must be exact");
        }
    }

    #[test]
    fn from_cells_round_trips_through_into_cells() {
        let mut iblt = Iblt::new(240);
        iblt.insert(5, ver(1));
        let cells = iblt.into_cells();
        let rebuilt = Iblt::from_cells(cells.clone());
        assert_eq!(rebuilt.into_cells(), cells);
    }

    #[test]
    fn tiny_sketch_still_builds_a_well_formed_shape() {
        let iblt = Iblt::new(0);
        assert_eq!(iblt.into_cells().len(), IBLT_PARTITIONS);
    }
}

#[cfg(test)]
mod prop_tests;
