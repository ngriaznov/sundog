//! Property tests for the IBLT sketch. Whenever `subtract` and `peel`
//! decode, the result is the exact symmetric difference at any size, and
//! at [`RATED_CAPACITY`] the default shape decodes at least 98% of random
//! differences.

use std::collections::{HashMap, HashSet};

use proptest::prelude::*;
use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

use super::{Elem, IBLT_PARTITIONS, Iblt, RATED_CAPACITY};
use crate::hlc::Hlc;
use crate::node::NodeId;

/// The default sketch size, the shape [`RATED_CAPACITY`] rates against.
const DEFAULT_CELLS: usize = 240;

#[derive(Debug, Clone, Copy)]
enum Role {
    LeftOnly,
    RightOnly,
    /// Present on both sides, usually at different versions.
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

/// Builds two sketches and the expected exact decode from
/// `(key_hash, role, ver_left, ver_right)` items. Colliding `key_hash`es
/// are deduplicated, which only shrinks the real difference.
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

    /// No size cap: `peel` either decodes exactly or reports
    /// `Undecodable`, never a wrong answer.
    #[test]
    fn beyond_rated_capacity_is_exact_or_undecodable_never_wrong(
        items in proptest::collection::vec(item_strategy(), 0..300)
    ) {
        let (left, right, expected_left, expected_right) = build(items);
        // `Undecodable` is acceptable; only a decode needs checking.
        if let Ok(decoded) = left.subtract(&right).and_then(Iblt::peel) {
            let got_left: HashSet<Elem> = decoded.only_left.into_iter().collect();
            let got_right: HashSet<Elem> = decoded.only_right.into_iter().collect();
            prop_assert_eq!(got_left, expected_left);
            prop_assert_eq!(got_right, expected_right);
        }
    }

    /// A sketch subtracted from an exact copy of itself always cancels to
    /// nothing, at any size, since every element's contribution is self-inverse.
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
            .and_then(Iblt::peel)
            .expect("a sketch subtracted from an identical copy always cancels to nothing");
        prop_assert!(decoded.only_left.is_empty());
        prop_assert!(decoded.only_right.is_empty());
    }

    /// `Iblt::new` always yields a positive multiple of [`IBLT_PARTITIONS`].
    #[test]
    fn new_always_yields_a_shape_thats_a_multiple_of_partitions(cells in 0usize..10_000) {
        let iblt = Iblt::new(cells);
        let len = iblt.into_cells().len();
        prop_assert!(len > 0);
        prop_assert_eq!(len % IBLT_PARTITIONS, 0);
    }
}

/// A seeded sample of `RATED_CAPACITY`-sized differences at the default
/// shape, pinning the decode rate `RATED_CAPACITY`'s docs state.
#[test]
fn rated_capacity_decodes_at_least_ninety_eight_percent() {
    const TRIALS: u32 = 500;
    let mut rng = StdRng::seed_from_u64(0x5EED);
    let mut decoded = 0u32;
    for _ in 0..TRIALS {
        let mut left = Iblt::new(DEFAULT_CELLS);
        let mut right = Iblt::new(DEFAULT_CELLS);
        let mut expected_left = HashSet::new();
        let mut expected_right = HashSet::new();
        for _ in 0..1000 {
            let key_hash: u64 = rng.random();
            let ver = Hlc {
                wall_ms: rng.random_range(1..1_000_000_000),
                logical: 0,
                node: NodeId::from(rng.random_range(1..8u64)),
            };
            left.insert(key_hash, ver);
            right.insert(key_hash, ver);
        }
        for i in 0..RATED_CAPACITY {
            let key_hash: u64 = rng.random();
            let ver = Hlc {
                wall_ms: rng.random_range(1..1_000_000_000),
                logical: 0,
                node: NodeId::from(1),
            };
            let elem = Elem { key_hash, ver };
            if i % 2 == 0 {
                left.insert(key_hash, ver);
                expected_left.insert(elem);
            } else {
                right.insert(key_hash, ver);
                expected_right.insert(elem);
            }
        }
        if let Ok(result) = left.subtract(&right).and_then(Iblt::peel) {
            let got_left: HashSet<Elem> = result.only_left.into_iter().collect();
            let got_right: HashSet<Elem> = result.only_right.into_iter().collect();
            assert_eq!(got_left, expected_left, "a decode is always exact");
            assert_eq!(got_right, expected_right, "a decode is always exact");
            decoded += 1;
        }
    }
    let rate = f64::from(decoded) / f64::from(TRIALS);
    assert!(
        rate >= 0.98,
        "{decoded}/{TRIALS} differences of {RATED_CAPACITY} decoded at {DEFAULT_CELLS} cells"
    );
}
