//! Property tests for the IBLT sketch: within [`RATED_CAPACITY`],
//! `subtract` + `peel` decodes the *exact* symmetric difference with
//! overwhelming probability — see [`RATED_CAPACITY`]'s own docs for why that
//! can never be an absolute guarantee, only one made statistically
//! negligible by this shape's cell count — and past it, arbitrarily large
//! differences either decode exactly or report `Undecodable`, never a wrong
//! answer.

use std::collections::{HashMap, HashSet};

use proptest::prelude::*;

use super::{Elem, IBLT_PARTITIONS, Iblt, RATED_CAPACITY};
use crate::hlc::Hlc;
use crate::node::NodeId;

/// The default sketch size ([`crate::config::ClusterConfig::ae_sketch_cells`]'s
/// own default) — [`RATED_CAPACITY`] is rated against exactly this shape.
const DEFAULT_CELLS: usize = 951;

#[derive(Debug, Clone, Copy)]
enum Role {
    LeftOnly,
    RightOnly,
    /// Present on both sides, at (possibly, if the two generated `Hlc`s
    /// happen to collide, *not* actually) different versions.
    Differing,
}

fn role_strategy() -> impl Strategy<Value = Role> {
    prop_oneof![
        Just(Role::LeftOnly),
        Just(Role::RightOnly),
        Just(Role::Differing),
    ]
}

fn hlc_strategy() -> impl Strategy<Value = Hlc> {
    (any::<u64>(), any::<u32>()).prop_map(|(wall_ms, logical)| Hlc {
        wall_ms,
        logical,
        node: NodeId::from(1),
    })
}

fn item_strategy() -> impl Strategy<Value = (u64, Role, Hlc, Hlc)> {
    (
        any::<u64>(),
        role_strategy(),
        hlc_strategy(),
        hlc_strategy(),
    )
}

/// Builds the two sketches and the expected exact decode from a list of
/// `(key_hash, role, ver_left, ver_right)` items — a `Differing` item whose
/// two generated `Hlc`s happen to be equal degenerates to canceling
/// entirely, exactly as a real identical-version element would, so the
/// computed expectation always matches what `peel` should return regardless.
/// Colliding `key_hash`es across items are deduplicated (last write wins),
/// which only ever shrinks the real difference, never grows it past the
/// caller's own bound.
fn build(items: Vec<(u64, Role, Hlc, Hlc)>) -> (Iblt, Iblt, HashSet<Elem>, HashSet<Elem>) {
    let mut by_hash: HashMap<u64, (Role, Hlc, Hlc)> = HashMap::new();
    for (key_hash, role, left_ver, right_ver) in items {
        by_hash.insert(key_hash, (role, left_ver, right_ver));
    }

    let mut left = Iblt::new(DEFAULT_CELLS);
    let mut right = Iblt::new(DEFAULT_CELLS);
    let mut expected_left = HashSet::new();
    let mut expected_right = HashSet::new();
    for (key_hash, (role, left_ver, right_ver)) in by_hash {
        match role {
            Role::LeftOnly => {
                left.insert(key_hash, left_ver);
                expected_left.insert(Elem {
                    key_hash,
                    ver: left_ver,
                });
            }
            Role::RightOnly => {
                right.insert(key_hash, right_ver);
                expected_right.insert(Elem {
                    key_hash,
                    ver: right_ver,
                });
            }
            Role::Differing => {
                left.insert(key_hash, left_ver);
                right.insert(key_hash, right_ver);
                if left_ver != right_ver {
                    expected_left.insert(Elem {
                        key_hash,
                        ver: left_ver,
                    });
                    expected_right.insert(Elem {
                        key_hash,
                        ver: right_ver,
                    });
                }
            }
        }
    }
    (left, right, expected_left, expected_right)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// Every `Differing` item costs at most 2 toward the true symmetric
    /// difference, every `LeftOnly`/`RightOnly` item at most 1 — so capping
    /// the item count at `RATED_CAPACITY / 2` bounds the true difference at
    /// or below `RATED_CAPACITY` regardless of which roles proptest picks.
    /// Within that bound, decode succeeds with overwhelming probability —
    /// [`RATED_CAPACITY`]'s own docs on why a hash-based sketch can never
    /// make that an absolute guarantee, and on how rare a red run here
    /// should be even so.
    #[test]
    fn within_rated_capacity_always_decodes_exactly(
        items in proptest::collection::vec(item_strategy(), 0..=(RATED_CAPACITY / 2))
    ) {
        let (left, right, expected_left, expected_right) = build(items);
        let decoded = left
            .subtract(&right)
            .peel()
            .expect(
                "a symmetric difference within the rated capacity decodes except on the rare, \
                 documented hash-collision case (RATED_CAPACITY's own docs) - re-run once before \
                 treating this as a regression",
            );
        let got_left: HashSet<Elem> = decoded.only_left.into_iter().collect();
        let got_right: HashSet<Elem> = decoded.only_right.into_iter().collect();
        prop_assert_eq!(got_left, expected_left);
        prop_assert_eq!(got_right, expected_right);
    }

    /// No size cap this time — the difference may well exceed the sketch's
    /// rated capacity. Either `peel` decodes (and, if it does, the result
    /// must still be exact) or it reports `Undecodable`; it must never
    /// return a wrong answer.
    #[test]
    fn beyond_rated_capacity_is_exact_or_undecodable_never_wrong(
        items in proptest::collection::vec(item_strategy(), 0..300)
    ) {
        let (left, right, expected_left, expected_right) = build(items);
        // `Err(Undecodable)` is also an acceptable outcome here (an
        // oversubscribed sketch, no wrong answer returned) — only a
        // successful decode has anything further to check.
        if let Ok(decoded) = left.subtract(&right).peel() {
            let got_left: HashSet<Elem> = decoded.only_left.into_iter().collect();
            let got_right: HashSet<Elem> = decoded.only_right.into_iter().collect();
            prop_assert_eq!(got_left, expected_left);
            prop_assert_eq!(got_right, expected_right);
        }
    }

    /// A sketch subtracted from an exact copy of itself always cancels to
    /// nothing, regardless of how many elements it holds — every element's
    /// contribution is deterministic and self-inverse, so this holds even
    /// far past `RATED_CAPACITY` (unlike the two properties above, this one
    /// carries no size bound at all).
    #[test]
    fn a_sketch_subtracted_from_itself_always_cancels_to_nothing(
        elems in proptest::collection::hash_map(any::<u64>(), hlc_strategy(), 0..500)
    ) {
        let mut a = Iblt::new(DEFAULT_CELLS);
        for (&key_hash, &v) in &elems {
            a.insert(key_hash, v);
        }
        let b = a.clone();
        let decoded = a
            .subtract(&b)
            .peel()
            .expect("a sketch subtracted from an identical copy always cancels to nothing");
        prop_assert!(decoded.only_left.is_empty());
        prop_assert!(decoded.only_right.is_empty());
    }

    /// `Iblt::new` always yields a well-formed shape: a positive multiple of
    /// [`IBLT_PARTITIONS`], regardless of the requested cell count.
    #[test]
    fn new_always_yields_a_shape_thats_a_multiple_of_partitions(cells in 0usize..10_000) {
        let iblt = Iblt::new(cells);
        let len = iblt.into_cells().len();
        prop_assert!(len > 0);
        prop_assert_eq!(len % IBLT_PARTITIONS, 0);
    }
}
