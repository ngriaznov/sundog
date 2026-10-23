//! The frame cap on a merge: the pure rule, and [`Engine::apply_many`]
//! refusing a merge that grows a value past it, with and without the
//! pre-fold.

use std::collections::BTreeSet;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;

use proptest::prelude::*;

use super::merge::merge_fits;
use super::*;
use crate::wire::{self, WireRecord};

type SetEngine = Engine<u32, BTreeSet<String>>;
type SetEntry = BatchEntry<u32, BTreeSet<String>>;

/// The cache name length every test engine's cap assumes.
const CACHE_LEN: usize = 4;

/// The postcard length of a set of `elems` two-byte elements, each costing a
/// length byte and the two bytes.
const fn set_len(elems: usize) -> usize {
    1 + 3 * elems
}

/// An engine that keeps merged values to `elems` two-byte elements under a
/// one-byte key, and sets it as the cap.
fn capped_engine(elems: usize) -> SetEngine {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    engine.set_merge_cap(
        CACHE_LEN,
        wire::forward_frame_len(CACHE_LEN, 1, set_len(elems)),
    );
    engine
}

/// The elements `e0`, `e1`, ... numbered from `first`, `count` of them.
fn elems(first: usize, count: usize) -> BTreeSet<String> {
    (first..first + count).map(|n| format!("e{n}")).collect()
}

/// A `Put` batch entry for `key` of `value` at `ver`.
fn entry_of(key: u32, ver: Hlc, value: &BTreeSet<String>) -> SetEntry {
    let kb = key_bytes(key);
    let encoded = Bytes::from(postcard::to_stdvec(value).expect("test value encodes"));
    (
        hash_key_bytes(kb.as_ref()),
        key,
        kb,
        ver,
        Incoming::Put {
            value: value.clone(),
            expires_at_ms: None,
            encoded,
        },
    )
}

/// Applies `entries` of one key's stripe through `engine`, as a peer batch
/// at clock `now_ms`.
fn apply(
    engine: &SetEngine,
    entries: Vec<SetEntry>,
    now_ms: u64,
) -> Vec<ApplyOutcome<u32, BTreeSet<String>>> {
    let bucket = stripe_index_from_hash(entries[0].0);
    engine.apply_many(
        Arrival::Peer,
        bucket,
        entries,
        &UnionSetResolver,
        60_000,
        600_000,
        now_ms,
    )
}

/// The value stored for `key`, decoded.
fn stored(engine: &SetEngine, key: u32) -> Option<BTreeSet<String>> {
    engine
        .record_for(key_bytes(key).as_ref(), 0)
        .and_then(|rec| rec.value)
        .map(|bytes| postcard::from_bytes(&bytes).expect("stored value decodes"))
}

/// The record stored for `key`.
fn record(engine: &SetEngine, key: u32) -> Option<WireRecord> {
    engine.record_for(key_bytes(key).as_ref(), 0)
}

#[test]
fn a_merge_fits_when_the_result_fits_or_is_no_larger_than_the_record_it_replaces() {
    // (merged, stored, limit, fits)
    let table = [
        (100, 50, 100, true),
        (101, 50, 100, false),
        (101, 101, 100, true),
        (101, 102, 100, true),
        (102, 101, 100, false),
        (0, 0, 0, true),
        (1, 0, 0, false),
        (usize::MAX, usize::MAX, 0, true),
        (usize::MAX, usize::MAX - 1, usize::MAX - 1, false),
        (usize::MAX, 0, usize::MAX, true),
    ];
    for (merged, stored, limit, expected) in table {
        assert_eq!(
            merge_fits(merged, stored, limit),
            expected,
            "merged {merged}, stored {stored}, limit {limit}"
        );
    }
}

proptest! {
    /// A merge fits exactly when its result is within the larger of the limit
    /// and the record it replaces.
    #[test]
    fn merge_fits_is_the_larger_of_the_limit_and_the_stored_length(
        merged in 0usize..200,
        stored in 0usize..200,
        limit in 0usize..200,
    ) {
        prop_assert_eq!(merge_fits(merged, stored, limit), merged <= limit.max(stored));
    }
}

#[test]
fn the_cap_counts_frames_so_the_cache_name_and_the_key_take_their_share() {
    let limit = wire::forward_frame_len(CACHE_LEN, 1, 10);
    let cap = MergeCap {
        cache_len: CACHE_LEN,
        limit,
    };
    assert!(cap.admits(1, 10, None), "exactly the limit");
    assert!(!cap.admits(1, 11, None), "one byte past it");
    assert!(
        !cap.admits(2, 10, None),
        "a longer key takes a byte of the frame"
    );
    let longer_name = MergeCap {
        cache_len: CACHE_LEN + 1,
        ..cap
    };
    assert!(
        !longer_name.admits(1, 10, None),
        "a longer cache name does too"
    );
    assert!(
        cap.admits(1, 11, Some(11)),
        "no larger than the record it replaces"
    );
    assert!(cap.admits(1, 11, Some(12)), "smaller than it");
    assert!(!cap.admits(1, 12, Some(11)), "one byte larger than it");
    assert!(
        !cap.admits(1, 11, Some(0)),
        "a replaced record of no bytes is not larger than the result"
    );
}

#[test]
fn the_cap_counts_the_forward_header_so_a_value_only_a_replicate_holds_is_refused() {
    // A `Distributed` owner fans a record out to its co-owners as a forward,
    // which is nine bytes wider than a replicate of the same record.
    let limit = wire::replicate_frame_len(CACHE_LEN, 1, 10);
    let cap = MergeCap {
        cache_len: CACHE_LEN,
        limit,
    };
    assert!(cap.admits(1, 1, None), "the widest value a forward holds");
    assert!(
        !cap.admits(1, 2, None),
        "one byte more fits a replicate only"
    );
    assert!(
        !cap.admits(1, 10, None),
        "and so does the replicate's limit"
    );
    assert!(
        cap.admits(1, 10, Some(10)),
        "a record no larger is still accepted"
    );
}

#[test]
fn a_contribution_is_counted_as_the_replicate_frame_it_travelled_in() {
    let limit = wire::replicate_frame_len(CACHE_LEN, 1, 10);
    let cap = MergeCap {
        cache_len: CACHE_LEN,
        limit,
    };
    assert!(cap.admits_contribution(1, 10, None), "exactly the limit");
    assert!(!cap.admits_contribution(1, 11, None), "one byte past it");
    assert!(
        !cap.admits_contribution(2, 10, None),
        "a longer key takes a byte of the frame"
    );
    assert!(
        !cap.admits(1, 10, None),
        "a built value of the same length is a forward, nine bytes wider"
    );
    assert!(cap.admits(1, 1, None) && cap.admits_contribution(1, 1, None));
    assert!(
        cap.admits_contribution(1, 11, Some(11)),
        "no larger than the record it replaces"
    );
    assert!(
        !cap.admits_contribution(1, 12, Some(11)),
        "one byte larger than it"
    );
}

#[test]
fn an_engine_has_no_cap_until_one_is_set() {
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    assert_eq!(engine.merge_cap(), None);

    engine.set_merge_cap(7, 1_000);
    assert_eq!(
        engine.merge_cap(),
        Some(MergeCap {
            cache_len: 7,
            limit: 1_000
        })
    );

    engine.set_merge_cap(7, usize::MAX);
    assert_eq!(engine.merge_cap(), None, "an unbounded limit is no cap");
}

#[test]
fn a_refused_outcome_names_no_key_and_no_version() {
    let outcome: ApplyOutcome<u32, u32> = ApplyOutcome::TooLarge {
        key_bytes: key_bytes(1),
        size: 9,
    };
    assert_eq!(outcome.too_large(), Some(9));
    assert_eq!(outcome.key(), None);
    assert_eq!(outcome.version(), None);
    let rejected: ApplyOutcome<u32, u32> = ApplyOutcome::Rejected;
    assert_eq!(rejected.too_large(), None);
}

#[test]
fn a_merge_past_the_cap_is_refused_and_leaves_the_stored_record_whole() {
    let engine = capped_engine(3);
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 3))], 0);
    let before = record(&engine, 1).expect("stored");
    let digests = engine.recompute_digests();

    let outcomes = apply(&engine, vec![entry_of(1, hlc(20, 2), &elems(3, 1))], 0);

    assert!(
        matches!(outcomes[..], [ApplyOutcome::TooLarge { size, .. }] if size == set_len(4)),
        "the merge would hold four elements"
    );
    assert_eq!(
        record(&engine, 1),
        Some(before),
        "version and bytes unchanged"
    );
    assert_eq!(engine.recompute_digests(), digests, "so is the digest");
}

#[test]
fn a_merge_that_fits_the_cap_is_stored() {
    let engine = capped_engine(3);
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 2))], 0);

    let outcomes = apply(&engine, vec![entry_of(1, hlc(20, 2), &elems(2, 1))], 0);

    assert!(matches!(outcomes[..], [ApplyOutcome::Put { .. }]));
    assert_eq!(stored(&engine, 1), Some(elems(0, 3)), "exactly at the cap");
}

#[test]
fn a_merge_that_adds_nothing_is_accepted_even_beside_a_record_over_the_cap() {
    let engine = capped_engine(3);
    // Stored before a cap applied: five elements, two past it.
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 5))], 0);

    let outcomes = apply(&engine, vec![entry_of(1, hlc(20, 2), &elems(0, 2))], 0);

    assert!(
        !matches!(outcomes[..], [ApplyOutcome::TooLarge { .. }]),
        "the merged value is no larger than the stored one: {:?}",
        outcomes.first().map(ApplyOutcome::too_large)
    );
    assert_eq!(stored(&engine, 1), Some(elems(0, 5)), "nothing was lost");
}

#[test]
fn a_record_over_the_cap_cannot_grow_by_one_element() {
    let engine = capped_engine(3);
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 5))], 0);
    let before = record(&engine, 1);

    let outcomes = apply(&engine, vec![entry_of(1, hlc(20, 2), &elems(5, 1))], 0);

    assert!(matches!(outcomes[..], [ApplyOutcome::TooLarge { size, .. }] if size == set_len(6)));
    assert_eq!(record(&engine, 1), before);
}

/// A resolver whose merge keeps the shorter side's bytes, the way a merge
/// that removes content does.
struct ShorterResolver;

impl ConflictResolver for ShorterResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let (a_bytes, b_bytes) = (a.value?, b.value?);
        let kept = if b_bytes.len() < a_bytes.len() {
            b_bytes
        } else {
            a_bytes
        };
        Some(Merged {
            value: Bytes::copy_from_slice(kept),
            expires_at_ms: later_deadline(a.expires_at_ms, b.expires_at_ms),
        })
    }
}

#[test]
fn a_merge_that_shrinks_a_record_over_the_cap_is_accepted() {
    let engine = capped_engine(3);
    let apply_shorter = |value: &BTreeSet<String>, ver: Hlc| {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()));
        engine.apply_many(
            Arrival::Peer,
            bucket,
            vec![entry_of(1, ver, value)],
            &ShorterResolver,
            60_000,
            600_000,
            0,
        )
    };
    // The first store is not a merge: it keeps seven elements, four past the cap.
    apply_shorter(&elems(0, 7), hlc(10, 1));
    assert_eq!(stored(&engine, 1), Some(elems(0, 7)));

    // Five elements are still two past it, and fewer than were stored.
    let outcomes = apply_shorter(&elems(0, 5), hlc(20, 2));
    assert!(matches!(outcomes[..], [ApplyOutcome::Put { .. }]));
    assert_eq!(stored(&engine, 1), Some(elems(0, 5)));

    // A longer contribution merges to the stored value itself: no larger.
    let outcomes = apply_shorter(&elems(0, 9), hlc(30, 3));
    assert!(!matches!(outcomes[..], [ApplyOutcome::TooLarge { .. }]));
    assert_eq!(stored(&engine, 1), Some(elems(0, 5)));
}

#[test]
fn the_first_store_of_a_key_is_not_a_merge_and_meets_no_cap() {
    let engine = capped_engine(3);

    let outcomes = apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 9))], 0);

    assert!(matches!(
        outcomes[..],
        [ApplyOutcome::Put { created: true, .. }]
    ));
    assert_eq!(stored(&engine, 1), Some(elems(0, 9)));
}

#[test]
fn a_resolver_that_does_not_merge_is_not_capped() {
    let engine: Engine<u32, String> = Engine::new(u64::MAX, None, None, None);
    engine.set_merge_cap(CACHE_LEN, wire::forward_frame_len(CACHE_LEN, 1, 4));
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()));
    let put_of = |value: &str, ver: Hlc| -> BatchEntry<u32, String> {
        let kb = key_bytes(1);
        let encoded = Bytes::from(postcard::to_stdvec(value).expect("encodes"));
        (
            hash_key_bytes(kb.as_ref()),
            1,
            kb,
            ver,
            Incoming::Put {
                value: value.to_string(),
                expires_at_ms: None,
                encoded,
            },
        )
    };

    engine.apply_many(
        Arrival::Peer,
        bucket,
        vec![put_of("a", hlc(10, 1))],
        &LwwResolver,
        60_000,
        600_000,
        0,
    );
    let outcomes = engine.apply_many(
        Arrival::Peer,
        bucket,
        vec![put_of("a much longer value than the cap", hlc(20, 2))],
        &LwwResolver,
        60_000,
        600_000,
        0,
    );

    assert!(
        matches!(outcomes[..], [ApplyOutcome::Put { .. }]),
        "an outright win is the contribution itself, which its writer checked"
    );
}

#[test]
fn an_expired_stored_record_is_displaced_so_the_contribution_alone_meets_the_cap() {
    let engine = capped_engine(3);
    let mut expiring = entry_of(1, hlc(10, 1), &elems(0, 2));
    if let Incoming::Put { expires_at_ms, .. } = &mut expiring.4 {
        *expires_at_ms = Some(100);
    }
    apply(&engine, vec![expiring], 0);

    // At 500 the stored record reads as absent. The contribution replaces
    // it, with nothing to be larger than, and is counted as the replicate
    // frame its writer checked, which is nine bytes narrower than the forward
    // the cap counts a built value as: the three elements those bytes hold
    // fit, a fourth does not.
    let outcomes = apply(&engine, vec![entry_of(1, hlc(500, 2), &elems(2, 7))], 500);
    assert!(matches!(outcomes[..], [ApplyOutcome::TooLarge { size, .. }] if size == set_len(7)));

    let outcomes = apply(&engine, vec![entry_of(1, hlc(600, 2), &elems(2, 6))], 600);
    assert!(matches!(outcomes[..], [ApplyOutcome::Put { .. }]));
    assert_eq!(stored(&engine, 1), Some(elems(2, 6)));
}

/// A resolver whose merge keeps the longer side's bytes whole, so that a
/// merge's result is one of its two inputs.
struct LongerResolver;

impl ConflictResolver for LongerResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let (a_bytes, b_bytes) = (a.value?, b.value?);
        let kept = if b_bytes.len() > a_bytes.len() {
            b_bytes
        } else {
            a_bytes
        };
        Some(Merged {
            value: Bytes::copy_from_slice(kept),
            expires_at_ms: later_deadline(a.expires_at_ms, b.expires_at_ms),
        })
    }
}

#[test]
fn a_merge_whose_result_is_the_contribution_is_held_to_the_replicate_frame_it_travelled_in() {
    // A cap that admits ten bytes of value as a replicate frame: a forward of
    // the same record is nine bytes wider.
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    engine.set_merge_cap(
        CACHE_LEN,
        wire::replicate_frame_len(CACHE_LEN, 1, set_len(3)),
    );
    let apply_with = |resolver: &dyn ConflictResolver, value: &BTreeSet<String>, ver: Hlc| {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()));
        engine.apply_many(
            Arrival::Peer,
            bucket,
            vec![entry_of(1, ver, value)],
            resolver,
            60_000,
            600_000,
            0,
        )
    };
    apply_with(&LongerResolver, &elems(0, 1), hlc(10, 1));

    // Three elements fit the cap as the replicate they arrived in, and the
    // resolver keeps them whole: they are stored, though they are larger than
    // the stored value and a forward of them would not fit.
    let outcomes = apply_with(&LongerResolver, &elems(0, 3), hlc(20, 2));
    assert!(matches!(outcomes[..], [ApplyOutcome::Put { .. }]));
    assert_eq!(stored(&engine, 1), Some(elems(0, 3)));

    // Four are past the cap as a replicate, and larger than the stored value.
    let outcomes = apply_with(&LongerResolver, &elems(0, 4), hlc(30, 3));
    assert!(matches!(outcomes[..], [ApplyOutcome::TooLarge { size, .. }] if size == set_len(4)));

    // A union the resolver builds is counted as a forward. Two elements fit a
    // cap of two elements' replicate frame, and their forward does not.
    let engine: SetEngine = Engine::new(u64::MAX, None, None, None);
    engine.set_merge_cap(
        CACHE_LEN,
        wire::replicate_frame_len(CACHE_LEN, 1, set_len(2)),
    );
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 1))], 0);
    let outcomes = apply(&engine, vec![entry_of(1, hlc(20, 2), &elems(1, 1))], 0);
    assert!(matches!(outcomes[..], [ApplyOutcome::TooLarge { size, .. }] if size == set_len(2)));
    assert_eq!(stored(&engine, 1), Some(elems(0, 1)));
}

#[test]
fn a_pre_folded_batch_refuses_the_step_the_cap_stops_and_folds_on_with_the_rest() {
    let engine = capped_engine(3);
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 1))], 0);

    let outcomes = apply(
        &engine,
        vec![
            entry_of(1, hlc(20, 2), &elems(1, 1)),
            entry_of(1, hlc(21, 2), &elems(2, 1)),
            entry_of(1, hlc(22, 2), &elems(3, 1)),
            entry_of(1, hlc(23, 2), &elems(0, 1)),
        ],
        0,
    );

    let refused = outcomes.iter().filter(|o| o.too_large().is_some()).count();
    assert_eq!(refused, 1, "only the fourth element is refused");
    assert_eq!(
        stored(&engine, 1),
        Some(elems(0, 3)),
        "the first three fit, and the fourth contribution adds nothing"
    );
}

/// A union resolver that counts the merges it computes, and how many of them
/// run while the calling thread holds the write lock of the stripe the key
/// lives in: a merge under that lock stalls every reader and writer of the
/// stripe for as long as it takes.
struct LockProbe {
    engine: OnceLock<Arc<SetEngine>>,
    bucket: usize,
    merges: AtomicUsize,
    under_lock: AtomicUsize,
}

impl LockProbe {
    fn over(engine: &Arc<SetEngine>, key: u32) -> Self {
        let probe = Self {
            engine: OnceLock::new(),
            bucket: stripe_index_from_hash(hash_key_bytes(key_bytes(key).as_ref())),
            merges: AtomicUsize::new(0),
            under_lock: AtomicUsize::new(0),
        };
        probe.engine.set(Arc::clone(engine)).ok();
        probe
    }

    fn counts(&self) -> (usize, usize) {
        (
            self.merges.load(Ordering::SeqCst),
            self.under_lock.load(Ordering::SeqCst),
        )
    }
}

impl ConflictResolver for LockProbe {
    fn winner(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        UnionSetResolver.winner(key, a, b)
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        self.merges.fetch_add(1, Ordering::SeqCst);
        let engine = self.engine.get().expect("the probe is wired to its engine");
        // Nothing else touches the stripe in these tests, so a write lock
        // that cannot be taken is the one this very call holds.
        if engine.stripes[self.bucket].try_write().is_none() {
            self.under_lock.fetch_add(1, Ordering::SeqCst);
        }
        UnionSetResolver.merge(key, a, b)
    }
}

#[test]
fn a_run_of_refused_steps_against_a_record_at_the_cap_merges_nothing_under_the_stripe_lock() {
    let engine = Arc::new(capped_engine(3));
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 3))], 0);
    let before = record(&engine, 1);
    let probe = LockProbe::over(&engine, 1);
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()));

    let outcomes = engine.apply_many(
        Arrival::Peer,
        bucket,
        (0..6)
            .map(|n| {
                entry_of(
                    1,
                    hlc(20 + n, 2),
                    &elems(3 + usize::try_from(n).unwrap(), 1),
                )
            })
            .collect(),
        &probe,
        60_000,
        600_000,
        0,
    );

    assert_eq!(outcomes.len(), 6, "one outcome for each entry given");
    assert!(
        outcomes
            .iter()
            .all(|o| o.too_large() == Some(set_len(4)) && o.key().is_none()),
        "every step is refused at its own index, with the size it would leave"
    );
    assert_eq!(record(&engine, 1), before, "the stored record is whole");
    assert_eq!(
        probe.counts(),
        (6, 0),
        "each step merged once, in the fold, and none under the stripe lock"
    );
}

#[test]
fn a_run_goes_on_folding_past_a_refused_step_and_each_outcome_stays_at_its_index() {
    let engine = Arc::new(capped_engine(3));
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 2))], 0);
    let probe = LockProbe::over(&engine, 1);
    let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(1).as_ref()));

    // Two new elements would make four: refused. One new element makes
    // three: folds. Another would make four again: refused, and last.
    let outcomes = engine.apply_many(
        Arrival::Peer,
        bucket,
        vec![
            entry_of(1, hlc(20, 2), &elems(2, 2)),
            entry_of(1, hlc(21, 2), &elems(2, 1)),
            entry_of(1, hlc(22, 2), &elems(4, 1)),
        ],
        &probe,
        60_000,
        600_000,
        0,
    );

    assert!(matches!(
        outcomes[..],
        [
            ApplyOutcome::TooLarge { size: first, .. },
            ApplyOutcome::Put { .. },
            ApplyOutcome::TooLarge { size: last, .. },
        ] if first == set_len(4) && last == set_len(4)
    ));
    assert_eq!(stored(&engine, 1), Some(elems(0, 3)));
    assert_eq!(
        probe.counts(),
        (4, 1),
        "three merges ran in the fold, and the survivor's one against the stored record under the lock"
    );
}

#[test]
fn a_refusal_in_a_pre_folded_run_leaves_the_same_store_and_outcomes_as_sequential_apply() {
    let batch = || {
        vec![
            entry_of(1, hlc(20, 2), &elems(2, 2)),
            entry_of(1, hlc(21, 3), &elems(2, 1)),
            entry_of(1, hlc(22, 2), &elems(4, 1)),
            entry_of(1, hlc(23, 3), &elems(0, 1)),
        ]
    };
    let on = capped_engine(3);
    let off = capped_engine(3);
    off.set_prefold_enabled(false);
    for engine in [&on, &off] {
        apply(engine, vec![entry_of(1, hlc(10, 1), &elems(0, 2))], 0);
    }

    let on_outcomes = apply(&on, batch(), 0);
    let off_outcomes = apply(&off, batch(), 0);

    assert_eq!(record(&on, 1), record(&off, 1), "version and bytes");
    let refused = |outcomes: &[ApplyOutcome<u32, BTreeSet<String>>]| {
        let mut sizes: Vec<_> = outcomes
            .iter()
            .filter_map(ApplyOutcome::too_large)
            .collect();
        sizes.sort_unstable();
        sizes
    };
    assert_eq!(
        refused(&on_outcomes),
        refused(&off_outcomes),
        "the same contributions are refused, with the same sizes"
    );
    assert_eq!(refused(&on_outcomes), [set_len(4), set_len(4)]);
}

#[test]
fn a_batch_stores_the_same_with_and_without_the_pre_fold_under_a_cap() {
    let batch = || {
        vec![
            entry_of(1, hlc(20, 2), &elems(1, 2)),
            entry_of(1, hlc(21, 3), &elems(3, 2)),
            entry_of(1, hlc(22, 2), &elems(5, 1)),
            entry_of(1, hlc(23, 3), &elems(1, 1)),
        ]
    };
    let on = capped_engine(4);
    let off = capped_engine(4);
    off.set_prefold_enabled(false);
    for engine in [&on, &off] {
        apply(engine, vec![entry_of(1, hlc(10, 1), &elems(0, 1))], 0);
    }

    let on_outcomes = apply(&on, batch(), 0);
    let off_outcomes = apply(&off, batch(), 0);

    assert_eq!(record(&on, 1), record(&off, 1));
    assert_eq!(
        on_outcomes
            .iter()
            .filter(|o| o.too_large().is_some())
            .count(),
        off_outcomes
            .iter()
            .filter(|o| o.too_large().is_some())
            .count(),
        "the same contributions are refused"
    );
    // The two-element contribution after the first fits; the next, which
    // would make five elements, is refused whole; the single element after
    // it fits again.
    assert_eq!(
        stored(&on, 1),
        Some(["e0", "e1", "e2", "e5"].map(String::from).into())
    );
}

#[test]
fn a_current_only_apply_meets_the_cap_too() {
    let engine = capped_engine(3);
    apply(&engine, vec![entry_of(1, hlc(10, 1), &elems(0, 3))], 0);
    let (hash, key, kb, ver, incoming) = entry_of(1, hlc(20, 2), &elems(3, 1));

    let outcome = engine.apply_if_current(
        (hash, key, kb, ver, incoming),
        hlc(10, 1),
        &UnionSetResolver,
        60_000,
        600_000,
        0,
    );

    assert!(matches!(outcome, Some(ApplyOutcome::TooLarge { size, .. }) if size == set_len(4)));
    assert_eq!(stored(&engine, 1), Some(elems(0, 3)));
}

/// An element from a small alphabet: batches of them overflow a small cap and
/// still collide often.
fn arb_elem() -> impl Strategy<Value = String> {
    (0u8..8).prop_map(|n| format!("e{n}"))
}

/// A key, a version and a one-element value.
type Spec = (u32, u64, u64, String);

fn arb_spec() -> impl Strategy<Value = Spec> {
    (0u32..3, 0u64..12, 1u64..4, arb_elem())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The pre-fold changes only how many applies a batch costs, with a cap
    /// as without one: the same `(version, bytes)` per key is stored, the
    /// same keys report a real outcome and the same keys have a contribution
    /// refused. Four elements is the cap, so a batch of up to eight
    /// elements crosses it.
    #[test]
    fn prefold_on_and_off_store_the_same_under_a_cap(
        setup in proptest::collection::vec(arb_spec(), 0..6),
        main in proptest::collection::vec(arb_spec(), 1..30),
    ) {
        let build = |specs: &[Spec]| -> Vec<SetEntry> {
            specs
                .iter()
                .map(|(key, wall_ms, node, elem)| {
                    entry_of(*key, hlc(*wall_ms, *node), &BTreeSet::from([elem.clone()]))
                })
                .collect()
        };
        let apply_all = |engine: &SetEngine, entries: Vec<SetEntry>| {
            let mut by_bucket: HashMap<usize, Vec<SetEntry>> = HashMap::new();
            for entry in entries {
                by_bucket.entry(stripe_index_from_hash(entry.0)).or_default().push(entry);
            }
            let mut outcomes = Vec::new();
            for (bucket, group) in by_bucket {
                outcomes.extend(engine.apply_many(
                    Arrival::Peer,
                    bucket,
                    group,
                    &UnionSetResolver,
                    60_000,
                    600_000,
                    0,
                ));
            }
            outcomes
        };
        let on = capped_engine(4);
        let off = capped_engine(4);
        off.set_prefold_enabled(false);
        apply_all(&on, build(&setup));
        apply_all(&off, build(&setup));

        let on_outcomes = apply_all(&on, build(&main));
        let off_outcomes = apply_all(&off, build(&main));

        for key in 0u32..3 {
            prop_assert_eq!(record(&on, key), record(&off, key));
            if let Some(held) = stored(&on, key) {
                prop_assert!(held.len() <= 4, "every value starts at one element and merges to the cap at most");
            }
        }
        let on_keys: BTreeSet<u32> = on_outcomes.iter().filter_map(ApplyOutcome::key).copied().collect();
        let off_keys: BTreeSet<u32> = off_outcomes.iter().filter_map(ApplyOutcome::key).copied().collect();
        prop_assert_eq!(on_keys, off_keys);
        let refused = |outcomes: &[ApplyOutcome<u32, BTreeSet<String>>]| {
            outcomes.iter().filter(|o| o.too_large().is_some()).count()
        };
        prop_assert_eq!(refused(&on_outcomes), refused(&off_outcomes));
    }
}
