//! The arrival rule for tombstones: a delete counts its retention from its
//! stamp; a tombstone a peer sends that arrives past its retention is
//! ignored whatever the key holds, in any order, and without reading the
//! key; this node's own delete is never ignored.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::super::apply::{ArrivalRule, EntryKey};
use super::*;
use crate::store::Tombstone;

const TTL: u64 = 60_000;
const MAX: u64 = 600_000;

fn tombstone_state(engine: &Engine<u32, String>, key: u32) -> Option<Tombstone> {
    let kb = key_bytes(key);
    let bucket = stripe_index_from_hash(hash_key_bytes(kb.as_ref()));
    engine
        .stripe_lock(bucket)
        .read()
        .tombstones
        .get(kb.as_ref())
        .copied()
}

pub(super) fn put_of(value: &str) -> Incoming<String> {
    let value = value.to_owned();
    Incoming::Put {
        encoded: Bytes::from(postcard::to_stdvec(&value).expect("encodes")),
        value,
        expires_at_ms: None,
    }
}

/// A record to apply, which can be built into a batch entry any number of times.
#[derive(Debug, Clone, Copy)]
enum Spec {
    Put(&'static str, Hlc),
    Delete(Hlc),
    Expired(Hlc, u64),
}

impl Spec {
    fn entry(self) -> BatchEntry<u32, String> {
        match self {
            Self::Put(value, ver) => entry(1, ver, put_of(value)),
            Self::Delete(ver) => entry(1, ver, Incoming::DELETE),
            Self::Expired(ver, expired_at_ms) => entry(
                1,
                ver,
                Incoming::Tombstone {
                    expired_at_ms: Some(expired_at_ms),
                },
            ),
        }
    }
}

pub(super) fn entry(key: u32, ver: Hlc, incoming: Incoming<String>) -> BatchEntry<u32, String> {
    let kb = key_bytes(key);
    (hash_key_bytes(kb.as_ref()), key, kb, ver, incoming)
}

/// Applies `records` as one batch through [`Engine::apply_many_with_reservation`],
/// grouped by bucket in order, with `ttl_ms` and `MAX` as the retentions.
/// Returns the outcomes and the number of tombstones the batch ignored.
pub(super) fn apply_with_ttl(
    engine: &Engine<u32, String>,
    arrival: Arrival,
    ttl_ms: u64,
    now_ms: u64,
    records: Vec<BatchEntry<u32, String>>,
) -> (Vec<ApplyOutcome<u32, String>>, u64) {
    let mut by_bucket: BTreeMap<usize, Vec<BatchEntry<u32, String>>> = BTreeMap::new();
    for record in records {
        by_bucket
            .entry(stripe_index_from_hash(record.0))
            .or_default()
            .push(record);
    }
    let mut written = Written::default();
    let mut outcomes = Vec::new();
    for (bucket, group) in by_bucket {
        outcomes.extend(
            engine
                .apply_many_with_reservation(
                    arrival,
                    bucket,
                    group,
                    &LwwResolver,
                    ttl_ms,
                    MAX,
                    now_ms,
                    None,
                    &mut written,
                )
                .0,
        );
    }
    (outcomes, written.ignored_tombstones())
}

/// [`apply_with_ttl`] at the retentions `TTL` and `MAX`.
pub(super) fn apply_records(
    engine: &Engine<u32, String>,
    arrival: Arrival,
    now_ms: u64,
    records: Vec<BatchEntry<u32, String>>,
) -> (Vec<ApplyOutcome<u32, String>>, u64) {
    apply_with_ttl(engine, arrival, TTL, now_ms, records)
}

/// What the key holds before a tombstone arrives.
#[derive(Debug, Clone, Copy)]
enum Held {
    Nothing,
    OlderValue,
    OlderTombstone,
}

/// An engine whose key 1 holds `held` at version `hlc(500, 2)`, told by a
/// collection at clock zero whether it `deferring`.
fn engine_holding(held: Held, deferring: bool) -> Engine<u32, String> {
    let engine = engine_u32_string(u64::MAX, None);
    match held {
        Held::Nothing => {}
        Held::OlderValue => {
            let _ = put(
                &engine,
                1,
                key_bytes(1),
                "old".into(),
                hlc(500, 2),
                None,
                1_000,
            );
        }
        Held::OlderTombstone => {
            let _ = tombstone(&engine, 1, key_bytes(1), hlc(500, 2), 1_000);
        }
    }
    engine.gc_tombstones(deferring, 0, 60_000);
    engine
}

fn rule(from_peer: bool, deferring: bool, now_ms: u64) -> ArrivalRule {
    ArrivalRule {
        from_peer,
        deferring,
        ttl_ms: TTL,
        max_ttl_ms: MAX,
        now_ms,
    }
}

#[test]
fn arrival_rule_deadlines_anchor_a_delete_on_its_stamp_and_a_put_has_none() {
    let at_5_000 = rule(true, false, 5_000);
    assert_eq!(
        at_5_000.deadlines(&Incoming::<String>::DELETE, hlc(1_000, 1)),
        Some((61_000, 601_000))
    );
    assert_eq!(
        at_5_000.deadlines(
            &Incoming::<String>::Tombstone {
                expired_at_ms: Some(2_000)
            },
            hlc(1_000, 1),
        ),
        Some((62_000, 602_000)),
        "an expiry tombstone counts from the expiry"
    );
    assert_eq!(
        at_5_000.deadlines(&Incoming::<String>::DELETE, hlc(9_000, 1)),
        Some((65_000, 605_000)),
        "capped at now"
    );
    assert_eq!(at_5_000.deadlines(&put_of("v"), hlc(1_000, 1)), None);
}

#[test]
fn only_a_peer_arrival_is_from_a_peer() {
    assert!(Arrival::Peer.is_peer());
    assert!(!Arrival::Local.is_peer());
}

#[test]
fn arrival_rule_ignores_only_a_lapsed_tombstone_from_a_peer() {
    let deadlines = Some((100, 1_000));
    assert!(rule(true, false, 100).ignores(deadlines));
    assert!(!rule(true, false, 99).ignores(deadlines));
    assert!(!rule(true, true, 999).ignores(deadlines));
    assert!(rule(true, true, 1_000).ignores(deadlines));
    assert!(!rule(false, false, 5_000).ignores(deadlines));
    assert!(
        !rule(true, false, u64::MAX).ignores(None),
        "a put has no deadlines and is never ignored"
    );
}

#[test]
fn a_peer_delete_arrives_anchored_on_its_stamp() {
    for arrival in [Arrival::Peer, Arrival::Local] {
        let engine = engine_u32_string(u64::MAX, None);
        let (outcomes, ignored) = apply_records(
            &engine,
            arrival,
            5_000,
            vec![entry(1, hlc(1_000, 1), Incoming::DELETE)],
        );
        assert!(
            matches!(outcomes[0], ApplyOutcome::Tombstoned { .. }),
            "{arrival:?}"
        );
        assert_eq!(ignored, 0);
        let t = tombstone_state(&engine, 1).expect("stored");
        assert_eq!(
            (t.ttl_deadline_ms, t.max_deadline_ms),
            (61_000, 601_000),
            "{arrival:?}: counted from the stamp at 1,000, not the receipt at 5,000"
        );
        assert_eq!(t.expired_at_ms, None);
        assert_eq!(engine.digests(), engine.recompute_digests_paired());
    }
}

#[test]
fn a_delete_stamped_ahead_of_the_clock_anchors_on_its_receipt() {
    for arrival in [Arrival::Peer, Arrival::Local] {
        let engine = engine_u32_string(u64::MAX, None);
        let _ = apply_records(
            &engine,
            arrival,
            5_000,
            vec![entry(1, hlc(9_000_000, 1), Incoming::DELETE)],
        );
        let t = tombstone_state(&engine, 1).expect("stored");
        assert_eq!(
            (t.ttl_deadline_ms, t.max_deadline_ms),
            (65_000, 605_000),
            "{arrival:?}: a stamp ahead never extends the hold"
        );
    }
}

#[test]
fn an_expiry_tombstone_anchors_on_its_expiry_not_its_stamp() {
    let engine = engine_u32_string(u64::MAX, None);
    let (outcomes, _) = apply_records(
        &engine,
        Arrival::Peer,
        5_000,
        vec![entry(
            1,
            hlc(3_000, 1),
            Incoming::Tombstone {
                expired_at_ms: Some(2_000),
            },
        )],
    );
    assert!(matches!(outcomes[0], ApplyOutcome::Tombstoned { .. }));
    let t = tombstone_state(&engine, 1).expect("stored");
    assert_eq!((t.ttl_deadline_ms, t.max_deadline_ms), (62_000, 602_000));
}

/// A peer delete stamped at 1,000 has its `ttl` deadline at 61,000 and its
/// cap at 601,000. Whatever the key holds, it is stored before the deadline
/// that applies, and ignored from it on.
#[test]
fn a_lapsed_peer_delete_is_ignored_whatever_the_key_holds() {
    for held in [Held::Nothing, Held::OlderValue, Held::OlderTombstone] {
        for (deferring, now_ms, stored) in [
            (false, 60_999, true),
            (false, 61_000, false),
            (false, 700_000, false),
            (true, 61_000, true),
            (true, 600_999, true),
            (true, 601_000, false),
            (true, u64::MAX, false),
        ] {
            let engine = engine_holding(held, deferring);
            let before_digests = engine.digests();
            let before_tombstone = tombstone_state(&engine, 1).map(|t| t.ver);
            let before_value = engine.get(&1, 1_000);
            let label = format!("{held:?}, deferring {deferring}, now {now_ms}");

            let (outcomes, ignored) = apply_records(
                &engine,
                Arrival::Peer,
                now_ms,
                vec![entry(1, hlc(1_000, 1), Incoming::DELETE)],
            );

            if stored {
                assert!(
                    matches!(outcomes[0], ApplyOutcome::Tombstoned { .. }),
                    "{label}"
                );
                assert_eq!(ignored, 0, "{label}");
                assert_eq!(
                    tombstone_state(&engine, 1).map(|t| t.ver),
                    Some(hlc(1_000, 1)),
                    "{label}"
                );
                assert_eq!(engine.get(&1, 1_000), None, "{label}: the older value goes");
                assert_ne!(engine.digests(), before_digests, "{label}");
            } else {
                assert!(matches!(outcomes[0], ApplyOutcome::Rejected), "{label}");
                assert_eq!(ignored, 1, "{label}");
                assert_eq!(
                    tombstone_state(&engine, 1).map(|t| t.ver),
                    before_tombstone,
                    "{label}: the key's tombstone is untouched"
                );
                assert_eq!(engine.get(&1, 1_000), before_value, "{label}");
                assert_eq!(engine.digests(), before_digests, "{label}");
            }
            assert_eq!(engine.digests(), engine.recompute_digests_paired());
        }
    }
}

/// After a lapsed delete is ignored, the older value it would have buried
/// still lands, the outcome of the writes that were not ignored.
#[test]
fn an_ignored_delete_leaves_the_older_value_free_to_arrive() {
    let engine = engine_holding(Held::Nothing, false);
    let (outcomes, ignored) = apply_records(
        &engine,
        Arrival::Peer,
        700_000,
        vec![
            entry(1, hlc(1_000, 1), Incoming::DELETE),
            entry(1, hlc(500, 2), put_of("old")),
        ],
    );
    assert!(matches!(outcomes[0], ApplyOutcome::Rejected));
    assert!(matches!(outcomes[1], ApplyOutcome::Put { .. }));
    assert_eq!(ignored, 1);
    assert_eq!(engine.get(&1, 700_000), Some("old".into()));
    assert_eq!(engine.tombstone_count(), 0);
}

#[test]
fn an_ignored_tombstone_counts_once_per_record_across_a_batch() {
    let engine = engine_holding(Held::Nothing, false);
    let (_, ignored) = apply_records(
        &engine,
        Arrival::Peer,
        700_000,
        vec![
            entry(1, hlc(1_000, 1), Incoming::DELETE),
            entry(
                2,
                hlc(1_000, 1),
                Incoming::Tombstone {
                    expired_at_ms: Some(1_500),
                },
            ),
            entry(3, hlc(699_999, 1), Incoming::DELETE),
            entry(4, hlc(500, 2), put_of("p")),
        ],
    );
    assert_eq!(
        ignored, 2,
        "the lapsed delete and the lapsed expiry tombstone; the fresh delete and the put are stored"
    );
    assert_eq!(engine.tombstone_count(), 1);
}

#[test]
fn a_lapsed_delete_and_an_older_value_converge_in_either_order() {
    for (now_ms, tombstoned) in [(100_000, false), (30_000, true)] {
        let records = [Spec::Delete(hlc(2_000, 4)), Spec::Put("old", hlc(500, 2))];
        let states: Vec<_> = [false, true]
            .into_iter()
            .map(|reversed| {
                let engine = engine_holding(Held::Nothing, false);
                let mut order = records.to_vec();
                if reversed {
                    order.reverse();
                }
                for record in order {
                    let _ = apply_records(&engine, Arrival::Peer, now_ms, vec![record.entry()]);
                }
                assert_eq!(
                    engine.get(&1, now_ms),
                    (!tombstoned).then(|| "old".to_string()),
                    "now {now_ms}, reversed {reversed}"
                );
                assert_eq!(
                    engine.tombstone_count(),
                    u64::from(tombstoned),
                    "now {now_ms}, reversed {reversed}"
                );
                assert_eq!(engine.digests(), engine.recompute_digests_paired());
                engine.digests()
            })
            .collect();
        assert_eq!(
            states[0], states[1],
            "now {now_ms}: the same state either way"
        );
    }
}

/// Every permutation of a key's four records: two values, the tombstone of
/// an expired entry and a delete, the last two lapsed. Applied as one batch
/// or one record at a time, they leave one state.
#[test]
fn a_lapsed_expiry_tombstone_and_a_lapsed_delete_converge_in_all_orders_of_a_mixed_batch() {
    fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut all = Vec::new();
        for i in 0..items.len() {
            let mut rest = items.to_vec();
            let head = rest.remove(i);
            for mut tail in permutations(&rest) {
                tail.insert(0, head.clone());
                all.push(tail);
            }
        }
        all
    }

    let records = [
        Spec::Put("a", hlc(400, 2)),
        Spec::Put("b", hlc(900, 3)),
        Spec::Expired(hlc(1_000, 1), 1_500),
        Spec::Delete(hlc(2_000, 4)),
    ];
    let all = permutations(&records);
    assert_eq!(all.len(), 24);

    // Not deferring, both tombstones are lapsed and ignored, so the newer
    // value stands. Deferring, both are within their cap and stored, so the
    // delete, the newest record, wins.
    for (deferring, live, tombstone_ver) in [
        (false, Some("b".to_string()), None),
        (true, None, Some(hlc(2_000, 4))),
    ] {
        let mut states = Vec::new();
        for batched in [true, false] {
            for order in &all {
                let engine = engine_holding(Held::Nothing, deferring);
                if batched {
                    let batch = order.iter().map(|spec| spec.entry()).collect();
                    let _ = apply_records(&engine, Arrival::Peer, 100_000, batch);
                } else {
                    for spec in order {
                        let _ = apply_records(&engine, Arrival::Peer, 100_000, vec![spec.entry()]);
                    }
                }
                assert_eq!(engine.get(&1, 100_000), live, "deferring {deferring}");
                assert_eq!(
                    tombstone_state(&engine, 1).map(|t| t.ver),
                    tombstone_ver,
                    "deferring {deferring}"
                );
                assert_eq!(engine.digests(), engine.recompute_digests_paired());
                states.push(engine.digests());
            }
        }
        assert!(
            states.windows(2).all(|pair| pair[0] == pair[1]),
            "deferring {deferring}: one state in all {} runs",
            states.len()
        );
    }
}

/// The exemption: this node's own delete is stored however late it applies,
/// and even with a zero retention. The same call from a peer is ignored,
/// which is what the arrival changes.
#[test]
fn a_local_delete_is_never_ignored() {
    for (ttl_ms, now_ms) in [(TTL, 9_000_000), (0, 2_000), (0, 1_000)] {
        for arrival in [Arrival::Local, Arrival::Peer] {
            let engine = engine_holding(Held::OlderValue, false);
            let (outcomes, ignored) = apply_with_ttl(
                &engine,
                arrival,
                ttl_ms,
                now_ms,
                vec![entry(1, hlc(1_000, 1), Incoming::DELETE)],
            );
            let label = format!("{arrival:?}, ttl {ttl_ms}, now {now_ms}");
            match arrival {
                Arrival::Local => {
                    assert!(
                        matches!(outcomes[0], ApplyOutcome::Tombstoned { .. }),
                        "{label}"
                    );
                    assert_eq!(ignored, 0, "{label}");
                    assert_eq!(engine.get(&1, now_ms), None, "{label}");
                    assert_eq!(engine.tombstone_count(), 1, "{label}");
                }
                Arrival::Peer => {
                    assert!(matches!(outcomes[0], ApplyOutcome::Rejected), "{label}");
                    assert_eq!(ignored, 1, "{label}");
                    assert_eq!(engine.get(&1, now_ms), Some("old".into()), "{label}");
                    assert_eq!(engine.tombstone_count(), 0, "{label}");
                }
            }
        }
    }
}

/// A resolver that counts every call made to it.
#[derive(Default)]
struct CountingResolver {
    calls: AtomicUsize,
}

impl ConflictResolver for CountingResolver {
    fn winner(&self, _key: &[u8], _a: RecordView<'_>, _b: RecordView<'_>) -> Winner {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Winner::B
    }

    fn needs_value_bytes(&self) -> bool {
        self.calls.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn merges(&self) -> bool {
        self.calls.fetch_add(1, Ordering::SeqCst);
        false
    }
}

/// `apply_locked` of `incoming` at `ver` for key 1, from a peer, with the
/// clock at `now_ms` and no deferral; returns the outcome and whether the
/// counter saw it ignored.
fn apply_locked_from_peer(
    engine: &Engine<u32, String>,
    resolver: &dyn ConflictResolver,
    ver: Hlc,
    incoming: Incoming<String>,
    now_ms: u64,
) -> (ApplyOutcome<u32, String>, u64) {
    let kb = key_bytes(1);
    let hash = hash_key_bytes(kb.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let sink = AtomicU64::new(0);
    let ctx = ApplyCtx {
        from_peer: true,
        ignored: &sink,
        ..test_ctx(engine, bucket, hash, resolver, TTL, MAX, now_ms)
    };
    let mut stripe = engine.stripes[bucket].write();
    let (outcome, _) = apply_locked(
        &mut stripe,
        &ctx,
        EntryKey {
            hash,
            key: 1,
            key_bytes: kb,
        },
        ver,
        incoming,
    );
    (outcome, sink.load(Ordering::SeqCst))
}

/// A dropped arrival never reaches the resolver, for any state the key is
/// in; the counter works, as a put over the same state does call it.
#[test]
fn a_dropped_tombstone_makes_no_resolver_call() {
    for held in [Held::OlderValue, Held::OlderTombstone, Held::Nothing] {
        let engine = engine_holding(held, false);
        let resolver = CountingResolver::default();
        let (outcome, ignored) =
            apply_locked_from_peer(&engine, &resolver, hlc(1_000, 1), Incoming::DELETE, 700_000);
        assert!(matches!(outcome, ApplyOutcome::Rejected), "{held:?}");
        assert_eq!(ignored, 1, "{held:?}");
        assert_eq!(
            resolver.calls.load(Ordering::SeqCst),
            0,
            "{held:?}: a dropped arrival consults nothing"
        );
    }

    let engine = engine_holding(Held::OlderValue, false);
    let resolver = CountingResolver::default();
    let (outcome, ignored) =
        apply_locked_from_peer(&engine, &resolver, hlc(1_000, 1), put_of("new"), 700_000);
    assert!(matches!(outcome, ApplyOutcome::Put { .. }));
    assert_eq!(ignored, 0);
    assert!(
        resolver.calls.load(Ordering::SeqCst) > 0,
        "a put over a stored value does consult the resolver"
    );
}

/// The verdict does not depend on the key: a key that already holds a
/// tombstone newer than the incoming delete, which would reject it on
/// version alone, still counts the delete as ignored for being lapsed.
#[test]
fn a_dropped_tombstone_is_ignored_even_where_the_key_would_reject_it_anyway() {
    let engine = engine_holding(Held::Nothing, false);
    let _ = tombstone(&engine, 1, key_bytes(1), hlc(5_000, 2), 5_000);
    let before = engine.digests();
    let resolver = CountingResolver::default();
    let (outcome, ignored) =
        apply_locked_from_peer(&engine, &resolver, hlc(1_000, 1), Incoming::DELETE, 700_000);
    assert!(matches!(outcome, ApplyOutcome::Rejected));
    assert_eq!(
        ignored, 1,
        "counted although the stored tombstone is newer and would reject it"
    );
    assert_eq!(engine.digests(), before);
}
