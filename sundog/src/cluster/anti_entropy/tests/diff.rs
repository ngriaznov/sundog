//! Tests for the decoded-sketch diff, the part-mismatch scan and the bucket and part settlement rules.

use crate::cluster::sketch::Elem;

use super::*;

fn hlc(wall_ms: u64) -> Hlc {
    Hlc {
        wall_ms,
        logical: 0,
        node: NodeId::from(1),
    }
}

fn entry(key: &[u8]) -> (Bytes, u64) {
    (Bytes::copy_from_slice(key), xxh3_64(key))
}

#[test]
fn diff_decoded_pushes_the_same_key_when_the_local_version_is_newer() {
    let (key, hash) = entry(b"k1");
    let local_entries = vec![(key.clone(), hlc(20))];
    let decoded = Decoded {
        only_left: vec![Elem {
            key_hash: hash,
            ver: hlc(20),
        }],
        only_right: vec![Elem {
            key_hash: hash,
            ver: hlc(10),
        }],
    };
    let (mut push, mut pull) = (Vec::new(), Vec::new());
    diff_decoded(&local_entries, &decoded, &mut push, &mut pull, false);
    assert_eq!(push, vec![key]);
    assert!(pull.is_empty(), "{pull:?}");
}

#[test]
fn diff_decoded_pulls_by_hash_when_the_remote_version_is_newer() {
    let (key, hash) = entry(b"k1");
    let local_entries = vec![(key, hlc(10))];
    let decoded = Decoded {
        only_left: vec![Elem {
            key_hash: hash,
            ver: hlc(10),
        }],
        only_right: vec![Elem {
            key_hash: hash,
            ver: hlc(20),
        }],
    };
    let (mut push, mut pull) = (Vec::new(), Vec::new());
    diff_decoded(&local_entries, &decoded, &mut push, &mut pull, false);
    assert!(push.is_empty(), "{push:?}");
    assert_eq!(pull, vec![hash]);
}

#[test]
fn diff_decoded_pushes_a_local_only_key() {
    let (key, hash) = entry(b"k2");
    let local_entries = vec![(key.clone(), hlc(5))];
    let decoded = Decoded {
        only_left: vec![Elem {
            key_hash: hash,
            ver: hlc(5),
        }],
        only_right: Vec::new(),
    };
    let (mut push, mut pull) = (Vec::new(), Vec::new());
    diff_decoded(&local_entries, &decoded, &mut push, &mut pull, false);
    assert_eq!(push, vec![key]);
    assert!(pull.is_empty(), "{pull:?}");
}

#[test]
fn diff_decoded_queues_both_directions_for_a_both_sides_mismatch_when_merging() {
    let (key, hash) = entry(b"k1");
    let local_entries = vec![(key.clone(), hlc(10))];
    let decoded = Decoded {
        only_left: vec![Elem {
            key_hash: hash,
            ver: hlc(10),
        }],
        only_right: vec![Elem {
            key_hash: hash,
            ver: hlc(20),
        }],
    };
    let (mut push, mut pull) = (Vec::new(), Vec::new());
    diff_decoded(&local_entries, &decoded, &mut push, &mut pull, true);
    assert_eq!(
        push,
        vec![key],
        "merging queues the push even though the peer's version is greater"
    );
    assert_eq!(
        pull,
        vec![hash],
        "merging keeps the pull the non-merging rule already queues"
    );
}

#[test]
fn diff_decoded_leaves_a_one_sided_key_alone_regardless_of_merging() {
    // Present only on this node's side of the peel: not a differing
    // version present on both sides, so `merging` changes nothing.
    let (key, hash) = entry(b"k2");
    let local_entries = vec![(key.clone(), hlc(5))];
    let decoded = Decoded {
        only_left: vec![Elem {
            key_hash: hash,
            ver: hlc(5),
        }],
        only_right: Vec::new(),
    };
    for merging in [false, true] {
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_decoded(&local_entries, &decoded, &mut push, &mut pull, merging);
        assert_eq!(push, vec![key.clone()]);
        assert!(pull.is_empty(), "{pull:?}");
    }
}

fn mismatch_of(cells: Vec<Cell>, local_entries: &[(Bytes, Hlc)]) -> RepairPlan {
    let mut plan = RepairPlan::default();
    handle_sketch_mismatch(
        &SmolStr::new("users"),
        7,
        cells,
        local_entries,
        &mut plan,
        false,
    );
    plan
}

#[test]
fn a_well_formed_sketch_decodes_into_hashes_to_pull() {
    let mut remote = Iblt::new(240);
    remote.insert(xxh3_64(b"k1"), hlc(5));
    let out = mismatch_of(remote.into_cells(), &[]);
    assert!(out.push_keys.is_empty(), "{:?}", out.push_keys);
    assert_eq!(out.pull_hashes, vec![(7, vec![xxh3_64(b"k1")])]);
    assert!(
        out.undecodable_buckets.is_empty(),
        "{:?}",
        out.undecodable_buckets
    );
}

#[test]
fn an_empty_sketch_off_the_wire_falls_back_to_a_full_listing() {
    let out = mismatch_of(Vec::new(), &[(Bytes::from_static(b"k1"), hlc(5))]);
    assert!(out.push_keys.is_empty(), "{:?}", out.push_keys);
    assert!(out.pull_hashes.is_empty(), "{:?}", out.pull_hashes);
    assert_eq!(out.undecodable_buckets, vec![7]);
}

#[test]
fn a_sketch_of_an_unreproducible_shape_falls_back_to_a_full_listing() {
    // 100 cells: `Iblt::new(100)` builds 99, so the two never line up.
    let out = mismatch_of(
        vec![Cell::default(); 100],
        &[(Bytes::from_static(b"k1"), hlc(5))],
    );
    assert!(out.push_keys.is_empty(), "{:?}", out.push_keys);
    assert!(out.pull_hashes.is_empty(), "{:?}", out.pull_hashes);
    assert_eq!(out.undecodable_buckets, vec![7]);
}

/// A merging bucket above the threshold that routes it to the sketch
/// path (`ClusterConfig::ae_sketch_min_bucket`) converges in this one
/// round: every one of its mismatched keys is a two-sided version
/// mismatch, the shape `sketch.rs`'s own
/// `two_sided_version_mismatches_decode_at_the_default_shape` pins as
/// decodable at this element count, so the peel here succeeds with no
/// fallback, and `merging` queues both directions for every key.
#[test]
fn a_merging_bucket_above_the_threshold_converges_through_the_sketch_path() {
    const KEYS: u64 = 40;
    let local_entries: Vec<(Bytes, Hlc)> = (0..KEYS)
        .map(|i| bucket_entry(format!("k{i}").as_bytes(), 2 * i + 1))
        .collect();
    let mut remote = Iblt::new(240);
    for i in 0..KEYS {
        remote.insert(xxh3_64(format!("k{i}").as_bytes()), hlc(2 * i));
    }

    let mut plan = RepairPlan::default();
    handle_sketch_mismatch(
        &SmolStr::new("users"),
        7,
        remote.into_cells(),
        &local_entries,
        &mut plan,
        true,
    );

    assert!(
        plan.undecodable_buckets.is_empty(),
        "a two-sided diff this size peels at the default sketch shape, no fallback"
    );
    assert_eq!(
        plan.push_keys.len(),
        usize::try_from(KEYS).expect("KEYS is a small literal, always fits"),
        "merging pushes every mismatched key, not only the greater-version side"
    );
    let (bucket, hashes) = plan
        .pull_hashes
        .first()
        .expect("one bucket entry carries every pulled hash");
    assert_eq!(*bucket, 7);
    assert_eq!(
        hashes.len(),
        usize::try_from(KEYS).expect("KEYS is a small literal, always fits"),
        "merging pulls every mismatched key's hash too, converging both directions in one round"
    );
}

#[test]
fn mismatched_parts_reports_only_the_differing_indices() {
    let local = vec![1u64, 2, 3, 4];
    let remote = vec![1u64, 9, 3, 8];
    assert_eq!(mismatched_parts(&local, &remote), vec![1, 3]);
}

#[test]
fn mismatched_parts_is_empty_for_identical_digests() {
    let digests: Vec<u64> = (0..64).collect();
    let mismatched = mismatched_parts(&digests, &digests);
    assert!(mismatched.is_empty(), "{mismatched:?}");
}

#[test]
fn mismatched_parts_treats_a_ragged_pair_as_mismatched_at_the_missing_index() {
    let local = vec![1u64, 2, 3];
    let remote = vec![1u64, 2];
    assert_eq!(
        mismatched_parts(&local, &remote),
        vec![2],
        "an index only one side has counts as mismatched, not skipped"
    );
}

fn part_mismatch_of(cells: Vec<Cell>, local_entries: &[(Bytes, Hlc)]) -> RepairPlan {
    let mut plan = RepairPlan::default();
    handle_part_sketch_mismatch(
        &SmolStr::new("users"),
        7,
        cells,
        local_entries,
        &mut plan,
        false,
    );
    plan
}

#[test]
fn a_well_formed_part_sketch_decodes_into_hashes_to_pull() {
    let mut remote = Iblt::new(240);
    remote.insert(xxh3_64(b"k1"), hlc(5));
    let out = part_mismatch_of(remote.into_cells(), &[]);
    assert!(out.push_keys.is_empty(), "{:?}", out.push_keys);
    assert_eq!(out.pull_hashes, vec![(7, vec![xxh3_64(b"k1")])]);
    assert!(
        out.undecodable_buckets.is_empty(),
        "{:?}",
        out.undecodable_buckets
    );
}

#[test]
fn an_undecodable_part_sketch_queues_its_bucket_for_the_whole_bucket_fallback() {
    let out = part_mismatch_of(
        vec![Cell::default(); 100],
        &[(Bytes::from_static(b"k1"), hlc(5))],
    );
    assert!(out.push_keys.is_empty(), "{:?}", out.push_keys);
    assert!(out.pull_hashes.is_empty(), "{:?}", out.pull_hashes);
    assert_eq!(out.undecodable_buckets, vec![7]);
}

#[test]
fn settle_bucket_parts_drops_a_bucket_with_an_undecodable_part_and_queues_it_once() {
    let mut gathered: HashMap<u16, RepairPlan> = HashMap::new();
    gathered.insert(
        3,
        RepairPlan {
            push_keys: vec![Bytes::from_static(b"p3")],
            pull_keys: vec![Bytes::from_static(b"q3")],
            pull_hashes: vec![(3, vec![33])],
            undecodable_buckets: vec![3],
            ..RepairPlan::default()
        },
    );
    gathered.insert(
        5,
        RepairPlan {
            push_keys: vec![Bytes::from_static(b"p5")],
            pull_keys: Vec::new(),
            pull_hashes: vec![(5, vec![55])],
            undecodable_buckets: Vec::new(),
            ..RepairPlan::default()
        },
    );
    let mut plan = RepairPlan::default();
    settle_bucket_parts(gathered, &mut plan);
    assert_eq!(plan.push_keys, vec![Bytes::from_static(b"p5")]);
    assert!(plan.pull_keys.is_empty(), "{:?}", plan.pull_keys);
    assert_eq!(plan.pull_hashes, vec![(5, vec![55])]);
    assert_eq!(
        plan.undecodable_buckets,
        vec![3],
        "the bucket falls back once, its part work dropped"
    );
}

#[test]
fn diff_decoded_pulls_a_remote_only_key_by_hash_alone() {
    // This node never held the key; `local_entries` is empty, so the
    // only thing to queue is the hash the peeled sketch reported.
    let hash = xxh3_64(b"k3");
    let local_entries: Vec<(Bytes, Hlc)> = Vec::new();
    let decoded = Decoded {
        only_left: Vec::new(),
        only_right: vec![Elem {
            key_hash: hash,
            ver: hlc(7),
        }],
    };
    let (mut push, mut pull) = (Vec::new(), Vec::new());
    diff_decoded(&local_entries, &decoded, &mut push, &mut pull, false);
    assert!(push.is_empty(), "{push:?}");
    assert_eq!(pull, vec![hash]);
}

fn bucket_entry(key: &[u8], wall_ms: u64) -> (Bytes, Hlc) {
    (Bytes::copy_from_slice(key), hlc(wall_ms))
}

#[test]
fn diff_bucket_pushes_only_the_greater_local_version_when_not_merging() {
    let local = vec![bucket_entry(b"k1", 20)];
    let peer = vec![bucket_entry(b"k1", 10)];
    let (mut push, mut pull) = (Vec::new(), Vec::new());
    diff_bucket(&local, &peer, &mut push, &mut pull, false);
    assert_eq!(push, vec![Bytes::from_static(b"k1")]);
    assert!(pull.is_empty(), "{pull:?}");
}

#[test]
fn diff_bucket_pulls_only_the_greater_peer_version_when_not_merging() {
    let local = vec![bucket_entry(b"k1", 10)];
    let peer = vec![bucket_entry(b"k1", 20)];
    let (mut push, mut pull) = (Vec::new(), Vec::new());
    diff_bucket(&local, &peer, &mut push, &mut pull, false);
    assert!(push.is_empty(), "{push:?}");
    assert_eq!(pull, vec![Bytes::from_static(b"k1")]);
}

#[test]
fn diff_bucket_queues_both_directions_for_a_mismatch_when_merging() {
    let local = vec![bucket_entry(b"k1", 10)];
    let peer = vec![bucket_entry(b"k1", 20)];
    let (mut push, mut pull) = (Vec::new(), Vec::new());
    diff_bucket(&local, &peer, &mut push, &mut pull, true);
    assert_eq!(
        push,
        vec![Bytes::from_static(b"k1")],
        "merging pushes this side's record even though the peer's version is greater"
    );
    assert_eq!(
        pull,
        vec![Bytes::from_static(b"k1")],
        "merging keeps the pull the non-merging rule already queues"
    );
}

#[test]
fn diff_bucket_agreeing_versions_queue_neither_direction_regardless_of_merging() {
    let local = vec![bucket_entry(b"k1", 10)];
    let peer = vec![bucket_entry(b"k1", 10)];
    for merging in [false, true] {
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_bucket(&local, &peer, &mut push, &mut pull, merging);
        assert!(push.is_empty(), "{push:?}");
        assert!(pull.is_empty(), "{pull:?}");
    }
}

#[test]
fn diff_bucket_a_key_only_one_side_holds_is_unaffected_by_merging() {
    let local_only = vec![bucket_entry(b"local-key", 1)];
    let peer_only = vec![bucket_entry(b"peer-key", 1)];
    for merging in [false, true] {
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        diff_bucket(&local_only, &peer_only, &mut push, &mut pull, merging);
        assert_eq!(push, vec![Bytes::from_static(b"local-key")]);
        assert_eq!(pull, vec![Bytes::from_static(b"peer-key")]);
    }
}
