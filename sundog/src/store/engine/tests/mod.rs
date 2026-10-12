//! Unit tests for the engine: the CRDT write path, reads, expiry, eviction, loads and the spill tier.

use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use super::*;
use crate::hlc::HlcClock;
use crate::node::NodeId;
use crate::store::LwwResolver;

fn engine_u32_string(max_capacity: u64, tti: Option<Duration>) -> Engine<u32, String> {
    Engine::new(max_capacity, tti, None, None)
}

fn key_bytes(key: u32) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("u32 encodes"))
}

/// [`key_bytes`]'s `String`-keyed counterpart, for a key long enough to
/// force [`Record`]'s heap path.
#[cfg(feature = "spill")]
fn string_key_bytes(key: &str) -> Bytes {
    Bytes::from(postcard::to_stdvec(key).expect("a string key encodes"))
}

fn hlc(wall_ms: u64, node: u64) -> Hlc {
    Hlc {
        wall_ms,
        logical: 0,
        node: NodeId::from(node),
    }
}

/// Always empty: every raw `apply_locked` test site below skips real spill prefetch.
#[cfg(feature = "spill")]
fn empty_prefetched() -> &'static HashMap<SpillLoc, Bytes> {
    static EMPTY: std::sync::OnceLock<HashMap<SpillLoc, Bytes>> = std::sync::OnceLock::new();
    EMPTY.get_or_init(HashMap::new)
}

/// An [`ApplyCtx`] against `engine`'s own weigher/tti, no spill prefetch or tier, for a test driving [`apply_locked`] directly.
fn test_ctx<'a, K, V>(
    engine: &'a Engine<K, V>,
    bucket: usize,
    hash: u64,
    resolver: &'a dyn ConflictResolver,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
    now_ms: u64,
) -> ApplyCtx<'a, K, V> {
    ApplyCtx {
        digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
        total_weight: &engine.total_weight,
        live_count: &engine.live_count,
        weigher: engine.weigher.as_ref(),
        tti_ms: engine.tti_ms,
        resolver,
        #[cfg(feature = "spill")]
        prefetched_spilled: empty_prefetched(),
        #[cfg(feature = "spill")]
        spill: None,
        tombstone_ttl_ms,
        tombstone_max_ttl_ms,
        now_ms,
    }
}

fn put<K, V>(
    engine: &Engine<K, V>,
    key: K,
    key_bytes: Bytes,
    value: V,
    ver: Hlc,
    expires_at_ms: Option<u64>,
    now_ms: u64,
) -> ApplyOutcome<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    put_with_resolver(
        engine,
        key,
        key_bytes,
        value,
        ver,
        expires_at_ms,
        now_ms,
        &LwwResolver,
    )
}

/// [`put`]'s tombstone counterpart, for a test that needs a real
/// tombstone in place rather than a live entry.
fn tombstone<K, V>(engine: &Engine<K, V>, key: K, key_bytes: Bytes, ver: Hlc, now_ms: u64)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = hash_key_bytes(key_bytes.as_ref());
    let bucket = stripe_index_from_hash(hash);
    let resolver = LwwResolver;
    #[cfg(feature = "spill")]
    let prefetched_spilled = prefetch_spilled_conflict_bytes(
        &engine.stripes[bucket],
        std::iter::once((hash, &key_bytes)),
        &resolver,
        engine.spill().map(Arc::as_ref),
    );
    let mut stripe = engine.stripes[bucket].write();
    let ctx = ApplyCtx {
        digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
        total_weight: &engine.total_weight,
        live_count: &engine.live_count,
        weigher: engine.weigher.as_ref(),
        tti_ms: engine.tti_ms,
        resolver: &resolver,
        #[cfg(feature = "spill")]
        prefetched_spilled: &prefetched_spilled,
        #[cfg(feature = "spill")]
        spill: engine.spill().map(Arc::as_ref),
        tombstone_ttl_ms: 60_000,
        tombstone_max_ttl_ms: 600_000,
        now_ms,
    };
    let _ = apply_locked(
        &mut stripe,
        &ctx,
        EntryKey {
            hash,
            key,
            key_bytes,
        },
        ver,
        Incoming::Tombstone,
    );
}

/// [`put`]'s counterpart for a test that needs a resolver other than
/// [`LwwResolver`], in particular one whose [`ConflictResolver::merge`]
/// can return `Some`.
#[allow(clippy::too_many_arguments)]
fn put_with_resolver<K, V>(
    engine: &Engine<K, V>,
    key: K,
    key_bytes: Bytes,
    value: V,
    ver: Hlc,
    expires_at_ms: Option<u64>,
    now_ms: u64,
    resolver: &dyn ConflictResolver,
) -> ApplyOutcome<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = hash_key_bytes(key_bytes.as_ref());
    let encoded = Bytes::from(postcard::to_stdvec(&value).expect("test value encodes"));
    let bucket = stripe_index_from_hash(hash);
    // Mirrors `Engine::apply_many`'s own prefetch (see `put` above), so
    // a merge against an already-spilled stored side, exercised
    // directly through this helper, reads its bytes back off-lock
    // exactly as the real write path does.
    #[cfg(feature = "spill")]
    let prefetched_spilled = prefetch_spilled_conflict_bytes(
        &engine.stripes[bucket],
        std::iter::once((hash, &key_bytes)),
        resolver,
        engine.spill().map(Arc::as_ref),
    );
    let ctx = ApplyCtx {
        digest_bucket: &engine.digest[digest_slot(bucket, part_index_from_hash(hash))],
        total_weight: &engine.total_weight,
        live_count: &engine.live_count,
        weigher: engine.weigher.as_ref(),
        tti_ms: engine.tti_ms,
        resolver,
        #[cfg(feature = "spill")]
        prefetched_spilled: &prefetched_spilled,
        #[cfg(feature = "spill")]
        spill: engine.spill().map(Arc::as_ref),
        tombstone_ttl_ms: 60_000,
        tombstone_max_ttl_ms: 600_000,
        now_ms,
    };
    let (outcome, displaced_spilled) = {
        let mut stripe = engine.stripes[bucket].write();
        apply_locked(
            &mut stripe,
            &ctx,
            EntryKey {
                hash,
                key,
                key_bytes,
            },
            ver,
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            },
        )
    };
    engine.note_spill_departure(displaced_spilled);
    outcome
}

/// A one-entry [`BatchEntry`] putting `value` at `ver`, expiring at
/// `expires_at_ms`.
fn put_entry(
    key: u32,
    value: &str,
    ver: Hlc,
    expires_at_ms: Option<u64>,
) -> BatchEntry<u32, String> {
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let encoded = Bytes::from(postcard::to_stdvec(value).expect("test value encodes"));
    (
        hash,
        key,
        kb,
        ver,
        Incoming::Put {
            value: value.to_string(),
            expires_at_ms,
            encoded,
        },
    )
}

fn remaining(ms: u64) -> Ttl {
    Ttl::Remaining(Duration::from_millis(ms))
}

/// `last_access_ms` of key `key`'s live entry.
fn last_access(engine: &Engine<u32, String>, key: u32) -> u32 {
    let kb = key_bytes(key);
    let stripe = engine
        .stripe_lock(stripe_index_from_hash(hash_key_bytes(kb.as_ref())))
        .read();
    stripe
        .live
        .iter()
        .find(|live| record_key(&live.record) == kb.as_ref())
        .expect("the entry is present")
        .last_access_ms
        .load(Ordering::Relaxed)
}

/// Test-only resolver: merges by set union of postcard-decoded
/// `BTreeSet<String>` values, a trivial join-semilattice standing in for
/// a real CRDT like [`super::super::crdt::PnCounter`] or
/// [`super::super::crdt::OrSet`]: content that grows whenever either
/// side contributes an element the other lacks, unlike
/// [`MaxStringResolver`]'s `max`, which can reproduce one side's bytes
/// exactly and so cannot exercise the case below. `merges()` is `true`:
/// besides the mint-arm test below, this is also `apply_many_prefold`'s
/// resolver, since set union is a real join-semilattice (commutative,
/// associative, idempotent) and so a fold-order-independence property
/// test can lean on it.
struct UnionSetResolver;

impl ConflictResolver for UnionSetResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return None;
        };
        let da: std::collections::BTreeSet<String> =
            postcard::from_bytes(av).expect("test value decodes");
        let db: std::collections::BTreeSet<String> =
            postcard::from_bytes(bv).expect("test value decodes");
        let merged: std::collections::BTreeSet<String> = da.union(&db).cloned().collect();
        Some(Merged {
            value: Bytes::from(postcard::to_stdvec(&merged).expect("merged value encodes")),
            expires_at_ms: None,
        })
    }
}

fn string_set(elems: &[&str]) -> std::collections::BTreeSet<String> {
    elems.iter().map(|s| (*s).to_string()).collect()
}

/// A pair of `u32` keys, scanned from `1` up to `limit`, that land in
/// the same bucket, plus that bucket's own index.
fn same_bucket_pair(limit: u32) -> (u32, u32, usize) {
    let mut by_bucket: HashMap<usize, Vec<u32>> = HashMap::new();
    for k in 1..limit {
        let bucket = stripe_index_from_hash(hash_key_bytes(key_bytes(k).as_ref()));
        let group = by_bucket.entry(bucket).or_default();
        group.push(k);
        if group.len() == 2 {
            return (group[0], group[1], bucket);
        }
    }
    panic!("no colliding pair found within the first {limit} keys");
}

#[tokio::test]
async fn concurrent_inserts_across_stripes_all_land() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let keys: Vec<u32> = (0..64).collect();
    let mut tasks = tokio::task::JoinSet::new();
    for k in keys.clone() {
        let engine = Arc::clone(&engine);
        tasks.spawn(async move {
            let kb = key_bytes(k);
            let _ = put(
                &engine,
                k,
                kb,
                k.to_string(),
                hlc(u64::from(k) + 1, 1),
                None,
                0,
            );
        });
    }
    while let Some(r) = tasks.join_next().await {
        r.expect("spawned call does not panic");
    }
    for k in keys {
        assert_eq!(engine.get(&k, 0), Some(k.to_string()));
    }
}

#[test]
fn two_different_stripes_lock_independently() {
    let engine = Arc::new(engine_u32_string(u64::MAX, None));
    let (bucket_a, bucket_b) = (0usize, 1usize);

    let held = Arc::clone(&engine);
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let _guard = held.stripe_lock(bucket_a).write();
        tx.send(()).expect("send");
        std::thread::sleep(Duration::from_millis(150));
    });
    rx.recv().expect("lock holder signals it has the lock");

    assert!(
        engine.stripe_lock(bucket_b).try_write().is_some(),
        "a different stripe's lock is not blocked"
    );
    assert!(
        engine.stripe_lock(bucket_a).try_write().is_none(),
        "the held stripe's lock is still contended"
    );
    handle.join().expect("lock holder thread does not panic");
    assert!(
        engine.stripe_lock(bucket_a).try_write().is_some(),
        "released after the holder finishes"
    );
}

/// [`digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions`]'s
/// shared step: applies `incoming` directly through [`apply_locked`],
/// under a fresh stripe lock, with a 1s/10s tombstone deadline and an
/// [`LwwResolver`].
fn apply_direct<K, V>(
    engine: &Engine<K, V>,
    bucket: usize,
    entry: EntryKey<K>,
    ver: Hlc,
    incoming: Incoming<V>,
    now_ms: u64,
) where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let hash = entry.hash;
    let mut stripe = engine.stripe_lock(bucket).write();
    let resolver = LwwResolver;
    let apply_ctx = test_ctx(engine, bucket, hash, &resolver, 1_000, 10_000, now_ms);
    let _ = apply_locked(&mut stripe, &apply_ctx, entry, ver, incoming);
}

#[test]
fn digest_matches_full_recompute_after_random_ops_including_sweeps_and_evictions() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let weigher: Weigher<u32, u64> = Box::new(|_k, _v| 1);
    let engine = Engine::<u32, u64>::new(12, None, Some(weigher), None);
    let mut rng = StdRng::seed_from_u64(0x5EED);
    let mut clock = HlcClock::new(NodeId::from(1));

    for i in 0..300u64 {
        let key = rng.random_range(0..24u32);
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let now = i * 10;
        match rng.random_range(0..4u32) {
            0 => {
                let ver = clock.now(now);
                let value = u64::from(key) * 31;
                let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                apply_direct(
                    &engine,
                    bucket,
                    EntryKey {
                        hash,
                        key,
                        key_bytes: kb,
                    },
                    ver,
                    Incoming::Put {
                        value,
                        expires_at_ms: Some(now + 500),
                        encoded,
                    },
                    now,
                );
                engine.enforce_capacity(bucket, &mut Written::default(), now);
            }
            1 => {
                let ver = clock.now(now);
                apply_direct(
                    &engine,
                    bucket,
                    EntryKey {
                        hash,
                        key,
                        key_bytes: kb,
                    },
                    ver,
                    Incoming::Tombstone,
                    now,
                );
            }
            2 => engine.sweep(now),
            _ => engine.gc_tombstones(false, now),
        }
        assert_eq!(
            engine.live_entry_count(),
            engine.recompute_live_entry_count(),
            "iteration {i}: the incrementally maintained live count diverged from a full \
             recount"
        );
        if i % 15 == 0 {
            assert_eq!(
                engine.digests(),
                engine.recompute_digests_paired(),
                "iteration {i}"
            );
            assert_eq!(
                engine.recompute_digests(),
                (0..BUCKET_COUNT)
                    .flat_map(|b| (0..PART_COUNT).map(move |p| (b, p)))
                    .map(|(b, p)| engine.part_digests(u16::try_from(b).expect("fits"))[p])
                    .collect::<Vec<u64>>(),
                "iteration {i}: part digests match the full recompute, not only their \
                 bucket aggregate"
            );
        }
    }
    assert_eq!(engine.digests(), engine.recompute_digests_paired());
}

#[test]
fn resident_bytes_is_none_and_slabs_count_nothing_until_counting_is_turned_on() {
    let engine = engine_u32_string(u64::MAX, None);
    let _ = put(
        &engine,
        1,
        key_bytes(1),
        "v".repeat(100),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(engine.resident_bytes(), None);
    assert!(
        engine
            .stripes
            .iter()
            .all(|stripe| stripe.read().live.resident_bytes.is_none()),
        "an uncapped engine's slabs carry no counter to move"
    );
    let counted = engine_u32_string(u64::MAX, None).count_resident_bytes();
    assert_eq!(counted.resident_bytes(), Some(0));
    assert!(
        counted
            .stripes
            .iter()
            .all(|stripe| stripe.read().live.resident_bytes.is_some())
    );
}

#[test]
fn resident_bytes_counts_each_entry_overhead_plus_its_heap_record() {
    let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
    let (short, long) = (1u32, 2u32);
    let _ = put(
        &engine,
        short,
        key_bytes(short),
        "s".to_string(),
        hlc(1, 1),
        None,
        0,
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(ENTRY_OVERHEAD_BYTES),
        "an inline record adds only the entry overhead"
    );
    let long_value = "v".repeat(100);
    let _ = put(
        &engine,
        long,
        key_bytes(long),
        long_value.clone(),
        hlc(2, 1),
        None,
        0,
    );
    // One LEN prefix byte, then the key and value bytes.
    let long_record =
        1 + key_bytes(long).len() + postcard::to_stdvec(&long_value).expect("encode").len();
    assert_eq!(
        engine.resident_bytes(),
        Some(2 * ENTRY_OVERHEAD_BYTES + long_record as u64),
        "a heap record adds its whole length on top of the overhead"
    );
    let _ = put(
        &engine,
        long,
        key_bytes(long),
        "t".to_string(),
        hlc(3, 1),
        None,
        0,
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(2 * ENTRY_OVERHEAD_BYTES),
        "overwriting with an inline value releases the old heap bytes"
    );
    for (key, wall_ms) in [(short, 4), (long, 5)] {
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        apply_direct(
            &engine,
            stripe_index_from_hash(hash),
            EntryKey {
                hash,
                key,
                key_bytes: kb,
            },
            hlc(wall_ms, 1),
            Incoming::Tombstone,
            wall_ms,
        );
    }
    assert_eq!(
        engine.resident_bytes(),
        Some(0),
        "removals release every byte"
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes())
    );
}

#[test]
fn resident_bytes_matches_a_full_recount_after_random_ops_including_sweeps_and_evictions() {
    use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

    let engine = engine_u32_string(12, None).count_resident_bytes();
    let mut rng = StdRng::seed_from_u64(0xB17E5);
    let mut clock = HlcClock::new(NodeId::from(1));

    for i in 0..400u64 {
        let key = rng.random_range(0..24u32);
        let kb = key_bytes(key);
        let hash = hash_key_bytes(kb.as_ref());
        let bucket = stripe_index_from_hash(hash);
        let now = i * 10;
        let entry = EntryKey {
            hash,
            key,
            key_bytes: kb,
        };
        match rng.random_range(0..4u32) {
            0 => {
                // Lengths on both sides of INLINE_CAP, so records move
                // between the inline and heap variants.
                let value = "x".repeat(rng.random_range(0..64usize));
                let encoded = Bytes::from(postcard::to_stdvec(&value).expect("encode"));
                let incoming = Incoming::Put {
                    value,
                    expires_at_ms: Some(now + 500),
                    encoded,
                };
                apply_direct(&engine, bucket, entry, clock.now(now), incoming, now);
                engine.enforce_capacity(bucket, &mut Written::default(), now);
            }
            1 => {
                let ver = clock.now(now);
                apply_direct(&engine, bucket, entry, ver, Incoming::Tombstone, now);
            }
            2 => engine.sweep(now),
            _ => engine.gc_tombstones(false, now),
        }
        assert_eq!(
            engine.resident_bytes(),
            Some(engine.recount_resident_bytes()),
            "iteration {i}: the maintained resident byte total diverged from a full recount"
        );
    }
}

#[test]
fn compact_replace_moves_resident_bytes_by_the_heap_delta() {
    let engine = engine_u32_string(u64::MAX, None).count_resident_bytes();
    let key = 7u32;
    let kb = key_bytes(key);
    let hash = hash_key_bytes(kb.as_ref());
    let _ = put(&engine, key, kb.clone(), "a".repeat(80), hlc(1, 1), None, 0);
    let before = engine.resident_bytes().expect("counting is on");
    let compacted = "a".repeat(30);
    assert!(engine.compact_replace_if_current(CompactReplace {
        key: &key,
        key_bytes: kb.as_ref(),
        hash,
        expected_ver: hlc(1, 1),
        new_ver: hlc(2, 1),
        value: compacted.clone(),
        encoded: Bytes::from(postcard::to_stdvec(&compacted).expect("encode")),
    }));
    assert_eq!(
        before - engine.resident_bytes().expect("counting is on"),
        50,
        "the record shrinks by exactly the 50 value bytes compaction dropped"
    );
    assert_eq!(
        engine.resident_bytes(),
        Some(engine.recount_resident_bytes())
    );
}

trait PairedDigests {
    fn recompute_digests_paired(&self) -> Vec<(u16, u64)>;
}
impl<K, V> PairedDigests for Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn recompute_digests_paired(&self) -> Vec<(u16, u64)> {
        let parts = self.recompute_digests();
        (0..BUCKET_COUNT)
            .map(|bucket| {
                let digest =
                    (0..PART_COUNT).fold(0u64, |acc, part| acc ^ parts[digest_slot(bucket, part)]);
                (u16::try_from(bucket).expect("fits"), digest)
            })
            .collect()
    }
}

#[test]
fn is_resident_true_for_resident_false_for_spilled() {
    let resident = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        1,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(is_resident(&resident));

    #[cfg(feature = "spill")]
    {
        let spilled = Live::<u32, String>::new(
            build_key_only_record(key_bytes(1).as_ref()),
            hlc(1, 1),
            NEVER_EXPIRES,
            0,
            0,
            EntryState::Spilled(SpillLoc {
                region: 0,
                offset: 0,
                len: 1,
                generation: 0,
            }),
        );
        assert!(!is_resident(&spilled));
    }
}

#[test]
fn is_spill_candidate_true_only_for_a_resident_entry_with_nonzero_weight() {
    let resident_hot = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        3,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(is_spill_candidate(&resident_hot));

    let resident_pending = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        0,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert!(
        !is_spill_candidate(&resident_pending),
        "weight zero means a hand-off to the spill tier is already in flight"
    );

    #[cfg(feature = "spill")]
    {
        let spilled = Live::<u32, String>::new(
            build_key_only_record(key_bytes(1).as_ref()),
            hlc(1, 1),
            NEVER_EXPIRES,
            0,
            0,
            EntryState::Spilled(SpillLoc {
                region: 0,
                offset: 0,
                len: 1,
                generation: 0,
            }),
        );
        assert!(!is_spill_candidate(&spilled));
    }
}

#[test]
fn build_resident_record_round_trips_key_and_value() {
    for (key_bytes, value_bytes) in [
        (&b""[..], &b""[..]),
        (&b"k"[..], &b"v"[..]),
        (&b"a-longer-key"[..], &b"a-longer-value-too"[..]),
    ] {
        let record = build_resident_record(key_bytes, value_bytes);
        assert_eq!(record_key(&record), key_bytes);
        assert_eq!(record_key_bytes(&record).as_ref(), key_bytes);
        assert_eq!(record_value_bytes(&record).as_ref(), value_bytes);
    }
}

#[test]
fn split_record_recovers_the_value_half_at_every_key_length_boundary() {
    // 127 and below encode LEN in one postcard varint byte; 128 and
    // above spill into a second, crossing the continuation-bit boundary.
    for key_len in [0usize, 1, 127, 128, 129] {
        let key_bytes = vec![b'k'; key_len];
        let value_bytes = b"value".to_vec();
        let record = build_resident_record(&key_bytes, &value_bytes);
        let (key_half, value_half) = split_record(&record);
        assert_eq!(key_half, key_bytes.as_slice(), "key_len = {key_len}");
        assert_eq!(value_half, value_bytes.as_slice(), "key_len = {key_len}");
    }
}

#[test]
fn build_key_only_record_produces_an_empty_value_half() {
    let record = build_key_only_record(b"a-key");
    let (key_half, value_half) = split_record(&record);
    assert_eq!(key_half, b"a-key");
    assert!(value_half.is_empty(), "{value_half:?}");
}

#[test]
fn record_value_matches_split_record_and_is_zero_copy() {
    for (key_bytes, value_bytes) in [
        (&b""[..], &b""[..]),
        (&b"k"[..], &b"v"[..]),
        (&b"a-longer-key"[..], &b"a-longer-value-too"[..]),
    ] {
        let record = build_resident_record(key_bytes, value_bytes);
        let value = record_value(&record);
        assert_eq!(value, split_record(&record).1);
        assert_eq!(value, value_bytes);
    }
}

#[test]
fn engine_capacity_hint_presizes_each_stripe_to_ceil_hint_over_bucket_count() {
    // Not a multiple of BUCKET_COUNT, so every stripe's own share only
    // divides evenly once rounded up.
    let hint = 3_000u64;
    let expected = usize::try_from(hint.div_ceil(BUCKET_COUNT as u64))
        .expect("a hint this small divided by BUCKET_COUNT fits in usize");
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, Some(hint));
    assert_eq!(
        engine.stripe_capacities(),
        vec![expected; BUCKET_COUNT],
        "every stripe presizes to ceil(hint / BUCKET_COUNT), none left at zero or grown past it"
    );
}

#[test]
fn engine_no_capacity_hint_allocates_nothing_until_first_write() {
    let engine = Engine::<u32, String>::new(u64::MAX, None, None, None);
    assert_eq!(
        engine.stripe_capacities(),
        vec![0; BUCKET_COUNT],
        "no hint: every stripe starts exactly as Stripe::new leaves it, zero allocation"
    );
    let _ = put(&engine, 1, key_bytes(1), "a".into(), hlc(1, 1), None, 0);
    let total: usize = engine.stripe_capacities().into_iter().sum();
    assert!(
        total > 0,
        "the first write grows only the one stripe its key lands in, matching today's \
         unhinted behavior exactly"
    );
}

#[test]
fn decode_key_and_decode_value_round_trip_postcard_bytes() {
    let key_bytes = postcard::to_stdvec(&42u32).expect("u32 encodes");
    let value_bytes = postcard::to_stdvec(&"a value".to_string()).expect("string encodes");
    assert_eq!(decode_key::<u32>(&key_bytes), 42u32);
    assert_eq!(decode_value::<String>(&value_bytes), "a value".to_string());
}

/// Pins `Live`'s non-spill size: `record` (24) + `wall_ms` (8) +
/// `node` (8) + four 4-byte fields (16) = 56, no padding.
#[test]
#[cfg(not(feature = "spill"))]
fn live_size_is_56_bytes_without_spill() {
    assert_eq!(std::mem::size_of::<Live<String, String>>(), 56);
}

/// Pins `Live`'s spill-enabled size: `EntryState` adds a 1-byte tag
/// plus padding, 76 bytes rounded up to 80 for 8-byte alignment.
#[test]
#[cfg(feature = "spill")]
fn live_size_is_80_bytes_with_spill() {
    assert_eq!(std::mem::size_of::<Live<String, String>>(), 80);
}

#[test]
fn live_ver_round_trips_through_set_ver() {
    let mut live = Live::<u32, String>::new(
        build_resident_record(key_bytes(1).as_ref(), b"v"),
        hlc(1, 1),
        NEVER_EXPIRES,
        1,
        0,
        #[cfg(feature = "spill")]
        EntryState::Resident,
    );
    assert_eq!(live.ver(), hlc(1, 1));
    let new_ver = Hlc {
        wall_ms: 9,
        logical: 3,
        node: NodeId::merge_derived(7),
    };
    live.set_ver(new_ver);
    assert_eq!(live.ver(), new_ver);
}

#[cfg(feature = "spill")]
mod spill_payload;

/// Coverage for [`Engine::compact`]: how its cursor rotates across
/// calls, that it never locks more of the engine than its budget needs,
/// and that a spilled entry is never handed to the resolver.
/// [`crate::store::crdt::PnCounter`]/[`crate::store::crdt::PnCounterResolver`]
/// (real types, not a stub) exercise the resolver-hook plumbing end to
/// end, since a fake resolver would only prove this method calls
/// *something*, not that it hands the real hook real arguments.
mod compact_sweep;

/// Property coverage for [`merge_version`] alone: every claim its doc
/// comment's convergence argument rests on is checked here rather than
/// assumed from the construction, namely that the mint arm strictly
/// dominates both inputs, that different merged bytes mint different
/// nodes, that each of the other three arms returns exactly what the
/// rule says, and that a mint is always recognizable as one (never a
/// real node's id).
mod merge_version;

/// Coverage for [`Engine::apply_many`]'s pre-fold: that a non-merging
/// resolver never triggers it, that a run never crosses a tombstone,
/// and, the property [`prefold_batch`]'s and [`fold_run`]'s docs argue
/// for from the resolver's join-semilattice contract, that
/// pre-fold changes only how many `apply_locked` calls a batch costs,
/// never the `(version, bytes)` it leaves stored or the set of keys it
/// reports a real outcome for.
mod apply_many_prefold;

mod apply;
mod evict;
mod expiry;
mod housekeeping;
mod inflight;
mod read;
mod release;
mod serve;
