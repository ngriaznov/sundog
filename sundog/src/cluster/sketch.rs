//! A partitioned invertible Bloom lookup table (IBLT) over one anti-entropy
//! bucket's `(key_hash, version)` elements. [`super::anti_entropy`] uses it
//! to reconcile a large bucket without shipping the bucket's entry list.
//!
//! Each side inserts its bucket into an [`Iblt`] of the same shape. One side
//! sends its sketch; the other [`Iblt::subtract`]s the two and
//! [`Iblt::peel`]s the result into the elements present on only one side.
//! The wire size is fixed by cell count, and decoding succeeds whenever the
//! two sides differ by no more than roughly [`RATED_CAPACITY`] elements.
//!
//! # Structure
//!
//! [`IBLT_PARTITIONS`] equal partitions of [`Cell`]s. An element lands in one
//! cell per partition, at `mix(placement_hash, p) % partition_len`, always
//! distinct. `placement_hash` is `check`'s hash of the whole element, key and
//! version together; placing by key alone would put two versions of one key
//! in the same cells, where they cancel and can never become pure.
//!
//! # Never wrong
//!
//! [`Iblt::peel`] accepts a cell only when its count is `1` or `-1` and its
//! checksum matches. A difference too large to resolve leaves cells
//! non-zero and returns [`Undecodable`]; the caller falls back to a listing.

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::hlc::Hlc;
use crate::node::NodeId;

/// Number of independent hash partitions each element is inserted into. See
/// this module's docs for why partitioning is the shape used here.
pub(crate) const IBLT_PARTITIONS: usize = 3;

/// Symmetric-difference size the default sketch shape (240 cells) decodes in
/// at least 99% of cases. Decoding is a probability, not a guarantee: it
/// peels cells holding exactly one element, and a difference whose elements
/// overlap in every cell they touch stalls the peel. A failed decode costs
/// one listing fallback, never a wrong answer.
// Only read by the tests that pin it; a non-test build never evaluates it.
#[allow(dead_code)]
pub(crate) const RATED_CAPACITY: usize = 100;

/// Distinct per-partition salts folded into [`mix`]: arbitrary odd 64-bit
/// constants keeping the three partitions' hash functions independent.
const PARTITION_SALTS: [u64; IBLT_PARTITIONS] = [
    0x9E37_79B9_7F4A_7C15,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
];

/// The per-partition hash placing a `placement_hash` in partition `p`:
/// salting then re-hashing keeps the three partitions' placements
/// independent, so a collision in one is unlikely to recur in the others.
fn mix(placement_hash: u64, partition: usize) -> u64 {
    xxh3_64(&(placement_hash ^ PARTITION_SALTS[partition]).to_le_bytes())
}

/// `xxh3_64` over `(key_hash, wall_ms, logical, node)`: the fingerprint
/// [`Iblt::peel`] checks a candidate "pure" cell against before trusting it
/// as a real, single element rather than an accidental combination.
fn check(key_hash: u64, wall_ms: u64, logical: u32, node: u64) -> u64 {
    let mut buf = [0u8; 8 + 8 + 4 + 8];
    buf[0..8].copy_from_slice(&key_hash.to_le_bytes());
    buf[8..16].copy_from_slice(&wall_ms.to_le_bytes());
    buf[16..20].copy_from_slice(&logical.to_le_bytes());
    buf[20..28].copy_from_slice(&node.to_le_bytes());
    xxh3_64(&buf)
}

/// One IBLT cell: `count`, XOR sums of every accumulated element's
/// `key_hash` and version fields, and `check_sum`, the fingerprint that
/// verifies a "pure" cell holds exactly one element. `Default` is the empty
/// cell, what every cell in a freshly built `Iblt` starts as.
///
/// Reachable outside `cluster::sketch` only because
/// [`crate::wire::Msg::AeSketch`] carries a `Vec<Cell>` on the wire,
/// re-exported as [`crate::wire::Cell`]; every field stays private here.
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

    /// `true` if this cell's `count` claims exactly one net element and its
    /// sums check out as a real `(key_hash, ver)`, the only condition under
    /// which [`Iblt::peel`] trusts a cell enough to peel it.
    fn is_pure_and_valid(self) -> bool {
        (self.count == 1 || self.count == -1)
            && self.check_sum == check(self.key_sum, self.wall_sum, self.logical_sum, self.node_sum)
    }

    /// The element a pure cell claims to hold, read off its accumulated
    /// sums. Meaningful only when [`Cell::is_pure_and_valid`] holds.
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

/// One IBLT element: a key's `xxh3_64` hash paired with its version, never
/// the key's actual bytes. A pulled-only element is always pulled by hash
/// ([`crate::wire::Msg::AePullHashes`]), never by key.
///
/// Reachable outside `cluster::sketch` only because `crate::diff_decoded`
/// (a `feature = "sim"`-gated re-export `tests/sim.rs` drives directly)
/// carries [`Decoded`], which carries this; every field stays crate-visible
/// in spirit even though `pub` is what the re-export requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Elem {
    pub(crate) key_hash: u64,
    pub(crate) ver: Hlc,
}

/// [`Iblt::peel`]'s success case: every element on one side of `subtract`
/// but not the other. `only_left` is what the caller had and the other
/// didn't; `only_right` is the reverse.
///
/// Reachable outside `cluster::sketch` only because [`Iblt::peel`] returns
/// it and `crate::diff_decoded` takes it, both re-exported (the latter
/// under its `crate::` path) for `tests/sim.rs` under `feature = "sim"`;
/// see this module's docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Decoded {
    pub(crate) only_left: Vec<Elem>,
    pub(crate) only_right: Vec<Elem>,
}

/// [`Iblt::peel`]'s failure case: the symmetric difference was too large
/// for peeling to resolve every cell back to zero. Never a wrong result,
/// only no result; the caller's `AeEntries` fallback is exact regardless.
///
/// Reachable outside `cluster::sketch` only because [`Iblt::peel`]'s
/// `Result` names it; see [`Decoded`]'s doc for why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Undecodable;

/// A partitioned IBLT over `(key_hash, version)` elements; see this
/// module's docs for the overall scheme.
///
/// Reachable outside `cluster::sketch` only because
/// [`crate::wire::Msg::AeSketch`]'s peer needs the same construction on the
/// initiator's side; `tests/sim.rs` re-exports it as `crate::Iblt` under
/// `feature = "sim"` to build and peel its own comparison sketch, the same
/// way `net::conn::serve_ae_digest` does on the responder's side.
#[derive(Debug, Clone)]
pub struct Iblt {
    cells: Vec<Cell>,
    partition_len: usize,
}

impl Iblt {
    /// Builds an empty sketch with approximately `cells` total cells, split
    /// evenly across [`IBLT_PARTITIONS`] partitions. A tiny or zero `cells`
    /// still yields a well-formed, if uselessly small, sketch.
    #[must_use]
    pub fn new(cells: usize) -> Self {
        let partition_len = (cells / IBLT_PARTITIONS).max(1);
        Self {
            cells: vec![Cell::default(); partition_len * IBLT_PARTITIONS],
            partition_len,
        }
    }

    /// Rebuilds a sketch from cells received off the wire; `partition_len`
    /// is re-derived the same way [`Iblt::new`] derives it.
    #[must_use]
    pub fn from_cells(cells: Vec<Cell>) -> Self {
        let partition_len = (cells.len() / IBLT_PARTITIONS).max(1);
        Self {
            cells,
            partition_len,
        }
    }

    /// Unwraps this sketch's cells for the wire.
    #[must_use]
    pub fn into_cells(self) -> Vec<Cell> {
        self.cells
    }

    /// The three cell indices a `placement_hash` maps to, one per
    /// partition; always distinct, since partitions own disjoint slices.
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
    /// `-1` to remove), placed by the whole element's hash.
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
    pub fn insert(&mut self, key_hash: u64, ver: Hlc) {
        self.apply(key_hash, ver, 1);
    }

    /// The symmetric-difference sketch: cell-wise `count` subtraction and
    /// XOR of every other field. An element at the same version on both
    /// sides cancels to zero; one differing or on only one side survives
    /// for [`Iblt::peel`] to resolve.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `other` have a different cell count.
    #[must_use]
    pub fn subtract(&self, other: &Self) -> Self {
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

    /// Unwinds this already-subtracted sketch into the exact list of
    /// elements only on the `self` ("left") side and only on `other`
    /// ("right"). Upholds the "never wrong, only sometimes undecodable"
    /// guarantee described in this module's docs.
    ///
    /// Standard IBLT peeling: repeatedly finds a verified-pure cell, records
    /// its element, subtracts it back out of all three of its cells, and
    /// requeues any cell that newly purifies. If every cell returns to zero,
    /// the decode is exact; otherwise this returns [`Undecodable`].
    ///
    /// # Errors
    ///
    /// Returns [`Undecodable`] if the symmetric difference was too large
    /// for every cell to peel back to zero; the caller falls back to a full
    /// listing.
    pub fn peel(mut self) -> Result<Decoded, Undecodable> {
        let mut only_left = Vec::new();
        let mut only_right = Vec::new();
        let mut queue: std::collections::VecDeque<usize> = (0..self.cells.len())
            .filter(|&idx| self.cells[idx].is_pure_and_valid())
            .collect();

        while let Some(idx) = queue.pop_front() {
            let cell = self.cells[idx];
            if !cell.is_pure_and_valid() {
                continue; // touched again since it was enqueued
            }
            let elem = cell.claimed_elem();
            let sign = cell.count;
            // `cell.check_sum` is already this element's placement hash,
            // verified by `is_pure_and_valid` above.
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

    /// Every element on both sides of a difference like this one's is at a
    /// distinct `(key_hash, ver)`, even where the key ranges overlap: the
    /// overlap keys carry a bumped version on `right`, so nothing cancels
    /// and the full symmetric difference is `left` plus `right`, whole.
    fn expect_elems(side: &[(u64, Hlc)]) -> HashSet<Elem> {
        side.iter()
            .map(|&(key_hash, v)| Elem { key_hash, ver: v })
            .collect()
    }

    #[test]
    fn a_large_symmetric_difference_is_either_exact_or_undecodable_never_wrong() {
        // A modest difference against a full-size sketch: small enough that
        // peeling always succeeds, and exactly, since the overlap's bumped
        // versions never cancel.
        let left: Vec<(u64, Hlc)> = (0..20u64).map(|k| (k, ver(k))).collect();
        let right: Vec<(u64, Hlc)> = (10..30u64).map(|k| (k, ver(k + 1))).collect();
        let decoded = sketch_of(&left, 240)
            .subtract(&sketch_of(&right, 240))
            .peel()
            .expect("a modest diff against 240 cells always decodes");
        assert_eq!(
            decoded.only_left.iter().copied().collect::<HashSet<_>>(),
            expect_elems(&left),
            "a real decode must be exact"
        );
        assert_eq!(
            decoded.only_right.iter().copied().collect::<HashSet<_>>(),
            expect_elems(&right),
            "a real decode must be exact"
        );

        // The same shape of difference, scaled up against a deliberately
        // tiny sketch (8 cells per partition): too large to peel, so it
        // must fail closed rather than decode wrong.
        let left: Vec<(u64, Hlc)> = (0..200u64).map(|k| (k, ver(k))).collect();
        let right: Vec<(u64, Hlc)> = (100..300u64).map(|k| (k, ver(k + 1))).collect();
        assert_eq!(
            sketch_of(&left, 24).subtract(&sketch_of(&right, 24)).peel(),
            Err(Undecodable)
        );
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
