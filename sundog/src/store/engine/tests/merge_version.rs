//! Property tests for `merge_version`: the mint arm dominates and the other arms return the stored or incoming stamp.

use proptest::prelude::*;

use super::super::merge::{MergedVersion, merge_version, mint_stamp};
use super::*;

/// A real single-writer stamp: `wall_ms`/`logical` unconstrained,
/// `node` drawn from the lower half of the `u64` range only,
/// everything [`NodeId::random`] can ever produce. A generated pair
/// here can never itself be merge-derived, so any `Store` arm that
/// reproduces one of `sv`/`ver` verbatim carries a real node id, and
/// only the mint arm ever introduces one that isn't.
fn arb_real_hlc() -> impl Strategy<Value = Hlc> {
    (any::<u64>(), any::<u32>(), 0..(1u64 << 63)).prop_map(|(wall_ms, logical, node)| Hlc {
        wall_ms,
        logical,
        node: NodeId::from(node),
    })
}

/// Short byte strings, generated independently for `stored`,
/// `incoming`, and `merged` so the mint-arm tests below can assume
/// pairwise inequality without biasing the distribution.
fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..8)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// (a) + (d): whenever the merged bytes match neither side's own
    /// bytes, the mint arm fires unconditionally (none of the other
    /// three arms' equality preconditions can hold), and its result
    /// both strictly dominates `sv` and `ver` under `Hlc`'s `Ord`
    /// and is recognizable as a mint, never a real node's stamp.
    #[test]
    fn mint_arm_dominates_both_inputs_and_is_always_merge_derived(
        sv in arb_real_hlc(),
        ver in arb_real_hlc(),
        stored in arb_bytes(),
        incoming in arb_bytes(),
        merged in arb_bytes(),
    ) {
        prop_assume!(merged != stored);
        prop_assume!(merged != incoming);
        let MergedVersion::Store(minted) =
            merge_version(sv, ver, &stored, &incoming, &merged)
        else {
            panic!("merged bytes matching neither side always mints a version");
        };
        prop_assert!(minted > sv);
        prop_assert!(minted > ver);
        prop_assert!(minted.node.is_merge_derived());
    }

    /// (b): two mints for merged bytes that differ from each other
    /// (and from both sides, so both trigger the mint arm) mint
    /// different node components: the property that keeps two
    /// nodes minting for genuinely different content from ever
    /// colliding on the same version.
    #[test]
    fn mint_arm_different_merged_bytes_mint_different_nodes(
        sv in arb_real_hlc(),
        ver in arb_real_hlc(),
        stored in arb_bytes(),
        incoming in arb_bytes(),
        merged_a in arb_bytes(),
        merged_b in arb_bytes(),
    ) {
        prop_assume!(merged_a != stored && merged_a != incoming);
        prop_assume!(merged_b != stored && merged_b != incoming);
        prop_assume!(merged_a != merged_b);
        let MergedVersion::Store(a) =
            merge_version(sv, ver, &stored, &incoming, &merged_a)
        else {
            panic!("merged bytes matching neither side always mints a version");
        };
        let MergedVersion::Store(b) =
            merge_version(sv, ver, &stored, &incoming, &merged_b)
        else {
            panic!("merged bytes matching neither side always mints a version");
        };
        prop_assert_ne!(a.node, b.node);
    }

    /// (c), first arm: identical content on both sides is pure
    /// version reconciliation: adopt whichever of `sv`/`ver` is
    /// greater, or do nothing if `sv` already is.
    #[test]
    fn identical_content_arm_adopts_the_greater_version_or_no_ops(
        sv in arb_real_hlc(),
        ver in arb_real_hlc(),
        content in arb_bytes(),
    ) {
        prop_assume!(sv != ver);
        let result = merge_version(sv, ver, &content, &content, &content);
        let greater = sv.max(ver);
        if greater == sv {
            prop_assert_eq!(result, MergedVersion::NoOp);
        } else {
            prop_assert_eq!(result, MergedVersion::Store(greater));
        }
    }

    /// (c), second arm: the merge reducing to incoming's own bytes,
    /// with incoming genuinely newer, adopts incoming's `(ver,
    /// merged)` pair verbatim.
    #[test]
    fn reduces_to_incoming_arm_adopts_incoming_verbatim(
        sv in arb_real_hlc(),
        ver in arb_real_hlc(),
        stored in arb_bytes(),
        incoming in arb_bytes(),
    ) {
        prop_assume!(ver > sv);
        prop_assume!(stored != incoming);
        let result = merge_version(sv, ver, &stored, &incoming, &incoming);
        prop_assert_eq!(result, MergedVersion::Store(ver));
    }

    /// (c), third arm: the merge reducing to stored's own bytes,
    /// with stored genuinely newer, is a no-op: incoming is fully
    /// absorbed already.
    #[test]
    fn reduces_to_stored_arm_is_a_no_op(
        sv in arb_real_hlc(),
        ver in arb_real_hlc(),
        stored in arb_bytes(),
        incoming in arb_bytes(),
    ) {
        prop_assume!(sv > ver);
        prop_assume!(stored != incoming);
        let result = merge_version(sv, ver, &stored, &incoming, &stored);
        prop_assert_eq!(result, MergedVersion::NoOp);
    }
}

/// (a) at the one corner `arb_real_hlc`'s random `logical` values
/// have negligible odds of ever hitting: both inputs' `logical` is
/// already `u32::MAX`, tied on `wall_ms` too. The `+ 1` has no room
/// to grow `logical`, so it must carry into `wall_ms` instead for
/// the mint to still strictly dominate both inputs.
#[test]
fn mint_arm_dominates_on_a_logical_overflow_at_a_wall_ms_tie() {
    let sv = Hlc {
        wall_ms: 1_000,
        logical: u32::MAX,
        node: NodeId::from(1u64),
    };
    let ver = Hlc {
        wall_ms: 1_000,
        logical: u32::MAX,
        node: NodeId::from(2u64),
    };
    let stored = b"stored".to_vec();
    let incoming = b"incoming".to_vec();
    let merged = b"merged".to_vec();
    let MergedVersion::Store(minted) = merge_version(sv, ver, &stored, &incoming, &merged) else {
        panic!("merged bytes matching neither side always mints a version");
    };
    assert!(minted > sv, "{minted:?} must dominate {sv:?}");
    assert!(minted > ver, "{minted:?} must dominate {ver:?}");
    assert_eq!(
        minted.wall_ms, 1_001,
        "the overflow must carry into wall_ms"
    );
    assert_eq!(minted.logical, 0, "logical resets once it carries");
    assert!(minted.node.is_merge_derived());
}

/// With `logical` and `wall_ms` both at their maximum there is no
/// larger stamp to mint: the mint holds the maximum instead of
/// wrapping below its inputs, and only the node can differ.
#[test]
fn mint_stamp_holds_the_maximum_instead_of_wrapping_below_its_inputs() {
    let node = NodeId::merge_derived(7);
    let top = |n: u64| Hlc {
        wall_ms: u64::MAX,
        logical: u32::MAX,
        node: NodeId::from(n),
    };
    let minted = mint_stamp(top(1), top(2), node);
    assert_eq!((minted.wall_ms, minted.logical), (u64::MAX, u32::MAX));
    assert_eq!(minted.node, node);

    let below = Hlc {
        wall_ms: u64::MAX - 1,
        logical: u32::MAX,
        node: NodeId::from(3u64),
    };
    let carried = mint_stamp(below, below, node);
    assert_eq!((carried.wall_ms, carried.logical), (u64::MAX, 0));
    assert!(carried > below);
}
