//! Property tests for the store. [`permutation_convergence`] applies the
//! same multiset of versioned writes, in any order with any duplication,
//! to multiple shards and checks they converge to byte-identical state.
//! Other properties cover a tombstone/put race, incremental digest against
//! full recompute, TTL and sweep under a manual clock, and
//! [`shard_matches_the_reference_model_under_arbitrary_op_sequences`], which
//! runs the [`model::run`] driver over proptest-generated op sequences.

use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;
use proptest_arbitrary_interop::arb;
use rand::seq::SliceRandom as _;
use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

use super::*;

/// Origin nodes a generated workload is spread across.
const NUM_NODES: u8 = 3;
/// Fresh shards the multiset replays into under independent permutations.
const NUM_REPLICAS: usize = 4;
/// Keyspace size, small enough that writes from different origins collide.
const KEYSPACE: u8 = 8;
/// Upper bound on gossip rounds the fixed-point loop may take before the
/// test treats non-convergence as a failure of the merge rule itself.
const MAX_GOSSIP_ROUNDS: usize = 20;

#[derive(Debug, Clone, Copy)]
enum OpKind {
    Put(u8, u16),
    Remove(u8),
}

fn op_strategy() -> impl Strategy<Value = (u8, OpKind)> {
    (
        0..NUM_NODES,
        prop_oneof![
            (0..KEYSPACE, any::<u16>()).prop_map(|(k, v)| OpKind::Put(k, v)),
            (0..KEYSPACE).prop_map(OpKind::Remove),
        ],
    )
}

/// Turns (origin, op) pairs into the [`WireRecord`] multiset a live cluster
/// would produce: each origin stamps with its own [`HlcClock`], observed by
/// the next node in rotation, mimicking gossip interleaving HLC stamps.
fn build_records(ops: &[(u8, OpKind)]) -> Vec<WireRecord> {
    let mut clocks: Vec<HlcClock> = (0..NUM_NODES)
        .map(|i| HlcClock::new(NodeId::from(u64::from(i) + 1)))
        .collect();
    let mut physical_ms: u64 = 1_700_000_000_000;

    ops.iter()
        .map(|(origin, op)| {
            physical_ms += 1;
            let origin_idx = usize::from(*origin);
            let ver = clocks[origin_idx].now(physical_ms);
            let gossip_idx = (origin_idx + 1) % clocks.len();
            clocks[gossip_idx].observe(physical_ms, ver);

            let key = match op {
                OpKind::Put(k, _) | OpKind::Remove(k) => *k,
            };
            let key_bytes = Bytes::from(postcard::to_stdvec(&key).expect("u8 key encodes"));
            match op {
                OpKind::Put(_, value) => WireRecord {
                    key: key_bytes,
                    value: Some(Bytes::from(
                        postcard::to_stdvec(value).expect("u16 value encodes"),
                    )),
                    ver,
                    expires_at_ms: None,
                },
                OpKind::Remove(_) => WireRecord {
                    key: key_bytes,
                    value: None,
                    ver,
                    expires_at_ms: None,
                },
            }
        })
        .collect()
}

/// Duplicates every record 1-3 times and shuffles under `seed`,
/// deterministic so proptest shrinking stays reproducible.
fn shuffled_with_duplicates(records: &[WireRecord], seed: u64) -> Vec<WireRecord> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut expanded = Vec::with_capacity(records.len() * 2);
    for rec in records {
        let copies = rng.random_range(1..=3u32);
        for _ in 0..copies {
            expanded.push(rec.clone());
        }
    }
    expanded.shuffle(&mut rng);
    expanded
}

/// A shard's full observable state, sorted by key so two converged shards
/// compare equal regardless of internal iteration order.
type CanonicalState = (Vec<(Bytes, Bytes, Hlc)>, Vec<(Bytes, Hlc)>, Vec<u64>);

fn canonical_state<K, V>(shard: &Shard<K, V>) -> CanonicalState
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let (mut live, mut tomb) = shard.engine.debug_snapshot();
    live.sort_by(|a, b| a.0.cmp(&b.0));
    tomb.sort_by(|a, b| a.0.cmp(&b.0));
    let digest: Vec<u64> = shard.engine.digests().into_iter().map(|(_, d)| d).collect();
    (live, tomb, digest)
}

/// Applies permuted `records` as a mix of single and batch applies:
/// `seed`-derived run lengths decide [`ShardOps::apply_remote`] vs
/// [`ShardOps::apply_remote_batch`], preserving record order either way.
async fn apply_mixed<K, V>(shard: &Shard<K, V>, records: Vec<WireRecord>, seed: u64)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let mut rng = StdRng::seed_from_u64(seed ^ 0xBA7C_11ED);
    let mut records = records.into_iter();
    loop {
        let run_len = rng.random_range(1..=4usize);
        let run: Vec<WireRecord> = records.by_ref().take(run_len).collect();
        match run.len() {
            0 => break,
            1 => {
                ShardOps::apply_remote(shard, run.into_iter().next().expect("len checked")).await;
            }
            _ => ShardOps::apply_remote_batch(shard, run).await,
        }
    }
}

/// Applies permuted `records` via real concurrent scheduling: splits into
/// partitions driven through [`tokio::spawn`] at once, each partition
/// applied via [`apply_mixed`]. Per-key stripe locking still converges.
async fn apply_concurrent<K, V>(shard: &Arc<Shard<K, V>>, records: Vec<WireRecord>, seed: u64)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    const PARTITIONS: u64 = 4;
    let mut rng = StdRng::seed_from_u64(seed ^ 0xC0FF_EE01);
    let mut parts: Vec<Vec<WireRecord>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
    for rec in records {
        let idx = usize::try_from(rng.random_range(0..PARTITIONS)).expect("small");
        parts[idx].push(rec);
    }
    let handles: Vec<_> = parts
        .into_iter()
        .enumerate()
        .map(|(i, part)| {
            let shard = Arc::clone(shard);
            let part_seed = seed ^ (i as u64).wrapping_mul(0x9E37_79B9);
            tokio::spawn(async move { apply_mixed(&shard, part, part_seed).await })
        })
        .collect();
    for handle in handles {
        handle.await.expect("apply_mixed call does not panic");
    }
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("invariant: a current-thread runtime always builds")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// The permutation-convergence property: the same multiset of writes and
    /// removes, applied to several shards under independent permutation and
    /// duplication and alternating [`apply_mixed`] with [`apply_concurrent`],
    /// converges to byte-identical state everywhere.
    #[test]
    fn permutation_convergence(
        ops in proptest::collection::vec(op_strategy(), 4..40),
        seeds in proptest::collection::vec(any::<u64>(), NUM_REPLICAS),
    ) {
        let records = build_records(&ops);
        let rt = current_thread_runtime();

        rt.block_on(async {
            let mut states = Vec::with_capacity(seeds.len());
            for &seed in &seeds {
                let shard = Arc::new(Shard::<u8, u16>::new(
                    SmolStr::new("perm-conv"),
                    Mode::Replicated,
                    NodeId::from(1000),
                    10_000,
                    None,
                    None,
                ));
                let permuted = shuffled_with_duplicates(&records, seed);
                if seed % 2 == 0 {
                    apply_mixed(&shard, permuted, seed).await;
                } else {
                    apply_concurrent(&shard, permuted, seed).await;
                }
                states.push(canonical_state(&shard));
            }
            for state in &states[1..] {
                assert_eq!(
                    &states[0], state,
                    "permutation, duplication, and mixing single/batch/concurrent applies do \
                     not change the converged state"
                );
            }
        });
    }

    /// Focused tombstone/put race: the newer [`Hlc`] wins at a shared key,
    /// and that outcome and the full converged state hold under any order.
    #[test]
    fn tombstone_put_race_converges_regardless_of_order(
        put_wall in 0u64..10_000,
        put_node in 0u8..4,
        tomb_wall in 0u64..10_000,
        tomb_node in 0u8..4,
        put_value in any::<u16>(),
        seeds in proptest::collection::vec(any::<u64>(), NUM_REPLICAS),
    ) {
        // Disjoint node ranges keep put and tombstone stamps from tying, so
        // the race always has a definitive winner.
        let put_ver = Hlc {
            wall_ms: put_wall,
            logical: 0,
            node: NodeId::from(u64::from(put_node) + 1),
        };
        let tomb_ver = Hlc {
            wall_ms: tomb_wall,
            logical: 0,
            node: NodeId::from(u64::from(tomb_node) + 100),
        };
        let key: u8 = 7;
        let key_bytes = Bytes::from(postcard::to_stdvec(&key).expect("key encodes"));
        let records = vec![
            WireRecord {
                key: key_bytes.clone(),
                value: Some(Bytes::from(
                    postcard::to_stdvec(&put_value).expect("value encodes"),
                )),
                ver: put_ver,
                expires_at_ms: None,
            },
            WireRecord {
                key: key_bytes,
                value: None,
                ver: tomb_ver,
                expires_at_ms: None,
            },
        ];
        let expected_live = if tomb_ver > put_ver { None } else { Some(put_value) };

        let rt = current_thread_runtime();
        rt.block_on(async {
            let mut states = Vec::with_capacity(seeds.len());
            for &seed in &seeds {
                let shard = Shard::<u8, u16>::new(
                    SmolStr::new("race"),
                    Mode::Replicated,
                    NodeId::from(1000),
                    1_000,
                    None,
                    None,
                );
                apply_mixed(&shard, shuffled_with_duplicates(&records, seed), seed).await;
                assert_eq!(
                    shard.get(&key).await,
                    expected_live,
                    "the newer of {{put, tombstone}} wins regardless of application order or \
                     whether it went through a single or batch apply"
                );
                states.push(canonical_state(&shard));
            }
            for state in &states[1..] {
                assert_eq!(&states[0], state);
            }
        });
    }
}

/// Compares the incrementally maintained bucket digests, and the part digests
/// beneath them, against a full recompute.
fn digest_matches_full_recompute<K, V>(shard: &Shard<K, V>) -> bool
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let expected_parts = shard.engine.recompute_digests();
    let buckets_match = shard.engine.digests().into_iter().all(|(bucket, digest)| {
        let expected = (0..PART_COUNT).fold(0u64, |acc, part| {
            acc ^ expected_parts[usize::from(bucket) * PART_COUNT + part]
        });
        expected == digest
    });
    let parts_match = (0..BUCKET_COUNT).all(|bucket| {
        let bucket_u16 = u16::try_from(bucket).expect("invariant: bucket < BUCKET_COUNT");
        let actual = shard.engine.part_digests(bucket_u16);
        (0..PART_COUNT).all(|part| actual[part] == expected_parts[bucket * PART_COUNT + part])
    });
    buckets_match && parts_match
}

/// Whether the incrementally maintained live-entry count agrees with a full
/// recount over every stripe.
fn live_count_matches_full_recount<K, V>(shard: &Shard<K, V>) -> bool
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    shard.engine.live_entry_count() == shard.engine.recompute_live_entry_count()
}

#[derive(Debug, Clone, Copy)]
enum DigestOp {
    Insert(u8, u16),
    Remove(u8),
    ApplyRemote(u8, Option<u16>, u8),
    Invalidate(u8, u8),
    Gc,
}

fn digest_op_strategy() -> impl Strategy<Value = DigestOp> {
    prop_oneof![
        (0..KEYSPACE, any::<u16>()).prop_map(|(k, v)| DigestOp::Insert(k, v)),
        (0..KEYSPACE).prop_map(DigestOp::Remove),
        (0..KEYSPACE, proptest::option::of(any::<u16>()), 0..3u8)
            .prop_map(|(k, v, o)| DigestOp::ApplyRemote(k, v, o)),
        (0..KEYSPACE, 0..3u8).prop_map(|(k, o)| DigestOp::Invalidate(k, o)),
        Just(DigestOp::Gc),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// After arbitrary local writes, remote applies, invalidations, and
    /// tombstone GC, the incremental digest always equals a full recompute,
    /// and a GC pass with `tombstone_ttl` zeroed clears every tombstone.
    #[test]
    fn digest_matches_recompute_after_arbitrary_ops_and_gc(
        ops in proptest::collection::vec(digest_op_strategy(), 4..60),
    ) {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let shard = Shard::<u8, u16>::new(
                SmolStr::new("digest-prop"),
                Mode::Replicated,
                NodeId::from(999),
                10_000,
                None,
                None,
            )
            .with_tombstone_ttl(Duration::ZERO);
            let mut remote_clocks: Vec<HlcClock> = (0u8..3)
                .map(|i| HlcClock::new(NodeId::from(u64::from(i) + 1)))
                .collect();
            let mut physical_ms: u64 = 1_700_000_000_000;

            for op in &ops {
                match *op {
                    DigestOp::Insert(k, v) => {
                        let _ = shard.insert(k, v).await;
                    }
                    DigestOp::Remove(k) => {
                        let _ = shard.remove(&k).await;
                    }
                    DigestOp::ApplyRemote(k, v, origin) => {
                        let idx = usize::from(origin) % remote_clocks.len();
                        physical_ms += 1;
                        let ver = remote_clocks[idx].now(physical_ms);
                        let rec = WireRecord {
                            key: Bytes::from(postcard::to_stdvec(&k).expect("u8 key encodes")),
                            value: v.map(|value| {
                                Bytes::from(postcard::to_stdvec(&value).expect("u16 value encodes"))
                            }),
                            ver,
                            expires_at_ms: None,
                        };
                        ShardOps::apply_remote(&shard, rec).await;
                    }
                    DigestOp::Invalidate(k, origin) => {
                        let idx = usize::from(origin) % remote_clocks.len();
                        physical_ms += 1;
                        let ver = remote_clocks[idx].now(physical_ms);
                        let key_bytes = Bytes::from(postcard::to_stdvec(&k).expect("u8 key encodes"));
                        ShardOps::invalidate(&shard, key_bytes, ver).await;
                    }
                    DigestOp::Gc => {
                        ShardOps::gc_tombstones(&shard, false).await;
                        assert!(
                            shard.engine.debug_snapshot().1.is_empty(),
                            "zero tombstone_ttl means every tombstone is GC-eligible immediately"
                        );
                    }
                }
                assert!(
                    digest_matches_full_recompute(&shard),
                    "incremental digest diverged from full recompute after {op:?}"
                );
                assert!(
                    live_count_matches_full_recount(&shard),
                    "incremental live count diverged from a full recount after {op:?}"
                );
            }
        });
    }
}

/// One op in the clock-driven workload: a TTL'd insert, a remove, a remote
/// apply with its own deadline, a clock advance, a sweep, or a GC pass.
#[derive(Debug, Clone, Copy)]
enum ClockOp {
    InsertTtl(u8, u16, u16),
    Remove(u8),
    ApplyRemote(u8, Option<u16>, u8, u32),
    Advance(u16),
    Sweep,
    Gc,
}

fn clock_op_strategy() -> impl Strategy<Value = ClockOp> {
    prop_oneof![
        (0..KEYSPACE, any::<u16>(), 0u16..500)
            .prop_map(|(k, v, ttl_ms)| ClockOp::InsertTtl(k, v, ttl_ms)),
        (0..KEYSPACE).prop_map(ClockOp::Remove),
        (
            0..KEYSPACE,
            proptest::option::of(any::<u16>()),
            0..3u8,
            0u32..500
        )
            .prop_map(|(k, v, o, ttl_ms)| ClockOp::ApplyRemote(k, v, o, ttl_ms)),
        (0u16..300).prop_map(ClockOp::Advance),
        Just(ClockOp::Sweep),
        Just(ClockOp::Gc),
    ]
}

/// [`ShardOps::digests`] equals a full recompute over
/// [`ShardOps::entries_for_buckets`], the same check anti-entropy makes.
async fn digest_matches_entries_for_buckets(shard: &Shard<u8, u16>) -> bool {
    let all_buckets: Vec<u16> = (0..u16::try_from(BUCKET_COUNT).expect("fits")).collect();
    let mut expected = vec![0u64; BUCKET_COUNT];
    for (bucket, entries) in ShardOps::entries_for_buckets(shard, all_buckets).await {
        for (key_bytes, ver) in entries {
            expected[usize::from(bucket)] ^= entry_fingerprint(&key_bytes, ver);
        }
    }
    let actual: Vec<u64> = ShardOps::digests(shard)
        .await
        .into_iter()
        .map(|(_, d)| d)
        .collect();
    actual == expected
}

/// [`ShardOps::part_digests`] equals a full recompute over
/// [`ShardOps::entries_for_parts`], the part-grained counterpart of
/// [`digest_matches_entries_for_buckets`].
async fn digest_matches_entries_for_parts(shard: &Shard<u8, u16>) -> bool {
    let all_parts: Vec<(u16, u8)> = (0..u16::try_from(BUCKET_COUNT).expect("fits"))
        .flat_map(|b| (0..u8::try_from(PART_COUNT).expect("fits")).map(move |p| (b, p)))
        .collect();
    let mut expected = vec![0u64; BUCKET_COUNT * PART_COUNT];
    for ((bucket, part), entries) in ShardOps::entries_for_parts(shard, all_parts).await {
        for (key_bytes, ver) in entries {
            expected[usize::from(bucket) * PART_COUNT + usize::from(part)] ^=
                entry_fingerprint(&key_bytes, ver);
        }
    }
    let all_buckets: Vec<u16> = (0..u16::try_from(BUCKET_COUNT).expect("fits")).collect();
    let actual = ShardOps::part_digests(shard, all_buckets).await;
    actual.into_iter().all(|(bucket, digests)| {
        digests
            .iter()
            .enumerate()
            .all(|(part, &d)| d == expected[usize::from(bucket) * PART_COUNT + part])
    })
}

/// For every key currently readable, its own record's `expires_at_ms` is
/// not yet past `now_ms`.
async fn assert_no_readable_entry_is_expired(shard: &Shard<u8, u16>, now_ms: u64) {
    for k in 0..KEYSPACE {
        if shard.get(&k).await.is_none() {
            continue;
        }
        let key_bytes = Bytes::from(postcard::to_stdvec(&k).expect("u8 key encodes"));
        let recs = ShardOps::records_for(shard, vec![key_bytes]).await;
        let rec = recs
            .first()
            .expect("a key that reads as present has a record");
        if let Some(deadline) = rec.expires_at_ms {
            assert!(
                deadline > now_ms,
                "key {k} reads as present but its own record's deadline {deadline} \
                 is already past the current clock ({now_ms})"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Drives a shard through random TTL'd inserts, remote applies, removes,
    /// clock advances, sweeps, and GC, all timestamped by a manual clock.
    /// After every [`ClockOp::Sweep`], the digest matches a full recompute
    /// and no readable entry is past its deadline.
    #[test]
    fn clock_driven_sweep_keeps_digest_and_readability_consistent(
        ops in proptest::collection::vec(clock_op_strategy(), 4..80),
    ) {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let now = Arc::new(AtomicU64::new(1_000_000));
            let reader = Arc::clone(&now);
            let clock_fn: Arc<dyn Fn() -> u64 + Send + Sync> =
                Arc::new(move || reader.load(Ordering::Relaxed));
            let shard = Shard::<u8, u16>::new(
                SmolStr::new("clock-prop"),
                Mode::Replicated,
                NodeId::from(500),
                10_000,
                None,
                None,
            )
            .with_tombstone_ttl(Duration::from_millis(200))
            .with_clock(Arc::clone(&clock_fn));
            let mut remote_clocks: Vec<HlcClock> = (0u8..3)
                .map(|i| HlcClock::new(NodeId::from(u64::from(i) + 1)))
                .collect();

            for op in &ops {
                match *op {
                    ClockOp::InsertTtl(k, v, ttl_ms) => {
                        let _ = shard
                            .insert_with_ttl(k, v, Duration::from_millis(u64::from(ttl_ms)))
                            .await;
                    }
                    ClockOp::Remove(k) => {
                        let _ = shard.remove(&k).await;
                    }
                    ClockOp::ApplyRemote(k, v, origin, ttl_ms) => {
                        let idx = usize::from(origin) % remote_clocks.len();
                        let current = now.load(Ordering::Relaxed);
                        let ver = remote_clocks[idx].now(current);
                        let rec = WireRecord {
                            key: Bytes::from(postcard::to_stdvec(&k).expect("u8 key encodes")),
                            value: v.map(|value| {
                                Bytes::from(postcard::to_stdvec(&value).expect("u16 value encodes"))
                            }),
                            ver,
                            expires_at_ms: Some(current + u64::from(ttl_ms)),
                        };
                        ShardOps::apply_remote(&shard, rec).await;
                    }
                    ClockOp::Advance(delta_ms) => {
                        now.fetch_add(u64::from(delta_ms), Ordering::Relaxed);
                    }
                    ClockOp::Sweep => {
                        ShardOps::run_pending_tasks(&shard).await;
                        assert!(
                            digest_matches_entries_for_buckets(&shard).await,
                            "digest diverged from a full recompute over entries_for_buckets \
                             after a sweep"
                        );
                        assert!(
                            digest_matches_entries_for_parts(&shard).await,
                            "part digest diverged from a full recompute over entries_for_parts \
                             after a sweep"
                        );
                        assert_no_readable_entry_is_expired(&shard, now.load(Ordering::Relaxed)).await;
                    }
                    ClockOp::Gc => {
                        ShardOps::gc_tombstones(&shard, false).await;
                    }
                }
            }
        });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Runs [`model::run`], the driver `sundog-fuzz`'s apply-path targets
    /// use, over `arbitrary`-generated `model::Op` sequences sampled by
    /// `proptest::collection::vec` for length control. A divergence is a
    /// real bug in versioned apply, digests, retention, expiry, or resolver.
    #[test]
    fn shard_matches_the_reference_model_under_arbitrary_op_sequences(
        ops in proptest::collection::vec(arb::<model::Op>(), 1..128),
    ) {
        let (shard, mut model) = model::new_shard_and_model("model-prop", 1);
        model::run(&ops, &shard, &mut model);
    }
}

use crdt::{PnCounter, PnCounterResolver};

/// Builds one [`WireRecord`] per origin, each carrying a
/// [`PnCounter::local_delta`] write for the same fixed key, staggered and
/// gossip-observed exactly like [`build_records`] so the resulting versions
/// interleave the way a live cluster's would. `deltas[i]` is the cumulative
/// increment origin `i` writes.
fn build_pn_counter_records(deltas: &[u64]) -> Vec<WireRecord> {
    let mut clocks: Vec<HlcClock> = (0..deltas.len())
        .map(|i| HlcClock::new(NodeId::from(u64::try_from(i).expect("small") + 1)))
        .collect();
    let mut physical_ms: u64 = 1_700_000_000_000;
    let key_bytes = Bytes::from(postcard::to_stdvec(&0u8).expect("u8 key encodes"));

    deltas
        .iter()
        .enumerate()
        .map(|(origin, &delta)| {
            physical_ms += 1;
            let node = NodeId::from(u64::try_from(origin).expect("small") + 1);
            let ver = clocks[origin].now(physical_ms);
            let gossip_idx = (origin + 1) % clocks.len();
            clocks[gossip_idx].observe(physical_ms, ver);
            let counter = PnCounter::local_delta(node, delta);
            WireRecord {
                key: key_bytes.clone(),
                value: Some(Bytes::from(
                    counter.encode().expect("PnCounter always encodes"),
                )),
                ver,
                expires_at_ms: None,
            }
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Shard-level convergence under [`PnCounterResolver`]: a multiset of
    /// concurrent per-origin increments to one shared key, replayed with
    /// arbitrary permutation, duplication (redelivery), and mixed
    /// single/batch/concurrent apply order across several shards, converges
    /// everywhere to byte-identical state — and to the exact sum of every
    /// generated increment, the "no lost updates" oracle a digest match
    /// alone doesn't check.
    #[test]
    fn pn_counter_merge_converges_to_the_exact_sum_under_any_order(
        deltas in proptest::collection::vec(0u64..1_000, usize::from(NUM_NODES)),
        seeds in proptest::collection::vec(any::<u64>(), NUM_REPLICAS),
    ) {
        let records = build_pn_counter_records(&deltas);
        let expected_total = i64::try_from(deltas.iter().sum::<u64>()).expect("fits");
        let rt = current_thread_runtime();

        rt.block_on(async {
            let mut states = Vec::with_capacity(seeds.len());
            for &seed in &seeds {
                let shard = Arc::new(
                    Shard::<u8, PnCounter>::new(
                        SmolStr::new("pn-counter-conv"),
                        Mode::Replicated,
                        NodeId::from(1000),
                        10_000,
                        None,
                        None,
                    )
                    .with_resolver(Arc::new(PnCounterResolver)),
                );
                let permuted = shuffled_with_duplicates(&records, seed);
                if seed % 2 == 0 {
                    apply_mixed(&shard, permuted, seed).await;
                } else {
                    apply_concurrent(&shard, permuted, seed).await;
                }

                let converged = shard
                    .get(&0u8)
                    .await
                    .expect("at least one origin's write landed");
                assert_eq!(
                    converged.value(),
                    expected_total,
                    "converged value must equal the exact sum of every generated increment, \
                     with no lost updates, under seed {seed}"
                );

                states.push(canonical_state(&shard));
            }
            for state in &states[1..] {
                assert_eq!(
                    &states[0], state,
                    "shards diverged under the same PnCounterResolver despite permutation, \
                     duplication, and mixed apply order"
                );
            }
        });
    }
}

#[cfg(feature = "spill")]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// [`pn_counter_merge_converges_to_the_exact_sum_under_any_order`]'s own
    /// workload, replayed against a shard with a real spill tier attached
    /// and a weight cap tight enough that its one counter key spills and
    /// re-residents repeatedly under capacity pressure as the workload
    /// replays: a merge that lands while the stored side is spilled must
    /// fold against it exactly as it would resident, so convergence to the
    /// exact sum, with no lost updates, still holds.
    /// `Engine::debug_snapshot` (`canonical_state`'s source) panics once a
    /// spill tier is attached, so this checks convergence via `Shard::get`
    /// alone, without the byte-identical cross-replica compare the
    /// non-spill property makes.
    #[test]
    fn pn_counter_merge_converges_to_the_exact_sum_with_a_spill_tier_under_capacity_pressure(
        deltas in proptest::collection::vec(0u64..1_000, usize::from(NUM_NODES)),
        seeds in proptest::collection::vec(any::<u64>(), NUM_REPLICAS),
    ) {
        let records = build_pn_counter_records(&deltas);
        let expected_total = i64::try_from(deltas.iter().sum::<u64>()).expect("fits");
        let rt = current_thread_runtime();

        rt.block_on(async {
            for &seed in &seeds {
                let dir = std::env::temp_dir().join(format!(
                    "sundog-prop-merge-spill-{}-{:?}-{seed}",
                    std::process::id(),
                    std::thread::current().id(),
                ));
                let _ = std::fs::remove_dir_all(&dir);
                let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
                let shard = Arc::new(
                    Shard::<u8, PnCounter>::new(
                        SmolStr::new("pn-counter-conv-spill"),
                        Mode::Replicated,
                        NodeId::from(1000),
                        1, // a tiny weight cap: the weigher below always exceeds it
                        None,
                        None,
                    )
                    .with_resolver(Arc::new(PnCounterResolver))
                    .with_weigher(|_key: &u8, _value: &PnCounter| 2)
                    .with_spill(&cfg)
                    .expect("the tier's directory and region files open"),
                );

                let permuted = shuffled_with_duplicates(&records, seed);
                if seed % 2 == 0 {
                    apply_mixed(&shard, permuted, seed).await;
                } else {
                    apply_concurrent(&shard, permuted, seed).await;
                }

                let converged = shard
                    .get(&0u8)
                    .await
                    .expect("at least one origin's write landed");
                assert_eq!(
                    converged.value(),
                    expected_total,
                    "converged value must equal the exact sum of every generated increment, \
                     with no lost updates, under seed {seed}, with a spill tier attached under \
                     capacity pressure"
                );

                let _ = std::fs::remove_dir_all(&dir);
            }
        });
    }
}

/// Targeted example test: a redelivery of an input already folded into a
/// prior merge is a byte-for-byte, version-for-version no-op. This is the
/// property that stops a redelivered record from re-broadcasting forever —
/// a merged version never equals either input's own version (so the
/// `sv == ver` fast path never fires on redelivery), so the resolver runs
/// again on every redelivery and must recognize the result as unchanged.
#[tokio::test]
async fn pn_counter_redelivery_after_merge_is_a_no_op() {
    let shard = Shard::<u8, PnCounter>::new(
        SmolStr::new("pn-counter-redelivery"),
        Mode::Replicated,
        NodeId::from(1000),
        10_000,
        None,
        None,
    )
    .with_resolver(Arc::new(PnCounterResolver));

    let key_bytes = Bytes::from(postcard::to_stdvec(&0u8).expect("u8 key encodes"));
    let a = WireRecord {
        key: key_bytes.clone(),
        value: Some(Bytes::from(
            PnCounter::local_delta(NodeId::from(1), 3)
                .encode()
                .expect("encodes"),
        )),
        ver: Hlc {
            wall_ms: 100,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    };
    let b = WireRecord {
        key: key_bytes.clone(),
        value: Some(Bytes::from(
            PnCounter::local_delta(NodeId::from(2), 4)
                .encode()
                .expect("encodes"),
        )),
        ver: Hlc {
            wall_ms: 200,
            logical: 0,
            node: NodeId::from(2),
        },
        expires_at_ms: None,
    };

    ShardOps::apply_remote(&shard, a).await;
    ShardOps::apply_remote(&shard, b.clone()).await;

    assert_eq!(
        shard.get(&0u8).await.map(|c| c.value()),
        Some(7),
        "two concurrent increments merge to their sum"
    );
    let merged = ShardOps::records_for(&shard, vec![key_bytes.clone()])
        .await
        .into_iter()
        .next()
        .expect("the merged key has a stored record");
    assert!(
        merged.ver.node.is_merge_derived(),
        "a real merge that grows content mints a version, never either input's own node"
    );

    // Redeliver `b`: already fully absorbed into the merge above, so this
    // must change nothing — no event, and nothing queued for re-fan-out.
    let _ = shard.fan_out.drain();
    let mut events = shard.events();

    ShardOps::apply_remote(&shard, b).await;

    assert_eq!(
        shard.get(&0u8).await.map(|c| c.value()),
        Some(7),
        "redelivering an already-absorbed input does not change the converged value"
    );
    let after = ShardOps::records_for(&shard, vec![key_bytes])
        .await
        .into_iter()
        .next()
        .expect("still stored");
    assert_eq!(
        after.ver, merged.ver,
        "a redelivery no-op leaves the stored version exactly as it was, never re-stamped"
    );
    assert!(
        shard.fan_out.drain().is_empty(),
        "a redelivery no-op must not be queued for re-fan-out, or it would re-broadcast forever"
    );
    assert!(
        events.try_recv().is_err(),
        "a redelivery no-op must not publish an Event"
    );
}

/// Applies, for every ordered shard pair, the anti-entropy direction rule
/// `diff_bucket` uses: the side holding the strictly greater [`Hlc`] version
/// for `key_bytes` pushes its record to the lesser side (a side missing the
/// key entirely counts as lesser). Repeats full rounds until one changes
/// nothing, and returns the round count that took. Callers bound that count
/// to turn "does gossip converge" into a property a hang can't dodge.
async fn gossip_until_fixed_point(
    shards: &[Arc<Shard<u8, PnCounter>>],
    key_bytes: &Bytes,
) -> usize {
    for round in 0..MAX_GOSSIP_ROUNDS {
        let mut changed = false;
        for i in 0..shards.len() {
            for j in 0..shards.len() {
                if i == j {
                    continue;
                }
                let from = ShardOps::records_for(shards[i].as_ref(), vec![key_bytes.clone()])
                    .await
                    .into_iter()
                    .next();
                let Some(from) = from else { continue };
                let to = ShardOps::records_for(shards[j].as_ref(), vec![key_bytes.clone()])
                    .await
                    .into_iter()
                    .next();
                let should_push = match &to {
                    Some(to) => from.ver > to.ver,
                    None => true,
                };
                if should_push {
                    ShardOps::apply_remote(shards[j].as_ref(), from).await;
                    changed = true;
                }
            }
        }
        if !changed {
            return round;
        }
    }
    panic!(
        "gossip did not reach a fixed point within {MAX_GOSSIP_ROUNDS} rounds; the merge \
         rule may be failing to converge"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Shard-level gossip-emulation property: `NUM_NODES` shards, each
    /// seeded with only its own origin's concurrent write to a shared key
    /// (redelivered and reordered per [`shuffled_with_duplicates`] and
    /// [`apply_mixed`]), then driven to a fixed point by
    /// [`gossip_until_fixed_point`] -- the same "greater version flows to
    /// the lesser side" rule anti-entropy's `diff_bucket` applies. Every
    /// shard must end at the identical `(version, bytes)` pair and the
    /// exact sum of every generated increment, with no lost updates, within
    /// a bounded number of rounds.
    #[test]
    fn pn_counter_gossip_converges_to_the_exact_sum_within_bounded_rounds(
        deltas in proptest::collection::vec(0u64..1_000, usize::from(NUM_NODES)),
        seeds in proptest::collection::vec(any::<u64>(), usize::from(NUM_NODES)),
    ) {
        let records = build_pn_counter_records(&deltas);
        let expected_total = i64::try_from(deltas.iter().sum::<u64>()).expect("fits");
        let key_bytes = records[0].key.clone();
        let rt = current_thread_runtime();

        rt.block_on(async {
            let shards: Vec<Arc<Shard<u8, PnCounter>>> = (0..records.len())
                .map(|i| {
                    Arc::new(
                        Shard::<u8, PnCounter>::new(
                            SmolStr::new("pn-counter-gossip"),
                            Mode::Replicated,
                            NodeId::from(1000 + u64::try_from(i).expect("small")),
                            10_000,
                            None,
                            None,
                        )
                        .with_resolver(Arc::new(PnCounterResolver)),
                    )
                })
                .collect();

            // Each shard starts having seen only its own origin's write --
            // possibly redelivered, in a generated order -- so every other
            // origin's increment can only reach it through gossip.
            for ((shard, own_record), &seed) in shards.iter().zip(&records).zip(&seeds) {
                let redelivered = shuffled_with_duplicates(std::slice::from_ref(own_record), seed);
                apply_mixed(shard, redelivered, seed).await;
            }

            let rounds = gossip_until_fixed_point(&shards, &key_bytes).await;
            assert!(
                rounds <= MAX_GOSSIP_ROUNDS,
                "gossip took {rounds} rounds, exceeding the {MAX_GOSSIP_ROUNDS}-round bound"
            );

            let mut final_records = Vec::with_capacity(shards.len());
            for shard in &shards {
                let rec = ShardOps::records_for(shard.as_ref(), vec![key_bytes.clone()])
                    .await
                    .into_iter()
                    .next()
                    .expect("every shard holds the key once gossip has reached a fixed point");
                let counter = PnCounter::decode(
                    rec.value.as_deref().expect("a live PnCounter record carries a value"),
                )
                .expect("PnCounter always decodes");
                assert_eq!(
                    counter.value(),
                    expected_total,
                    "every shard must converge to the exact sum of every generated increment, \
                     with no lost updates"
                );
                final_records.push((rec.ver, rec.value));
            }
            for pair in final_records.windows(2) {
                assert_eq!(
                    pair[0], pair[1],
                    "gossip must converge every shard to the identical (version, bytes) pair"
                );
            }
        });
    }
}

/// One full pass of the bidirectional exchange anti-entropy's
/// `merging = true` path applies (`cluster::anti_entropy`'s
/// `diff_bucket`/`diff_decoded` with `merging` set), emulated the same way
/// [`gossip_until_fixed_point`] emulates the unidirectional rule: for every
/// unordered pair of `shards`, each side's *current* record for `key_bytes`
/// is applied to the other, rather than only the greater version pushing to
/// the lesser side. A single call processes every pair exactly once.
///
/// This converges the whole set in that one pass, not merely each pair in
/// isolation: `engine::merge_version`'s doc proves any two replicas that
/// trade records this way land on an identical `(version, bytes)` pair in
/// one exchange regardless of which arm fires (the content merge and the
/// `wall_ms`/`logical` max are both symmetric in the two inputs, and the
/// minted `node` is a function of the merged bytes alone), and since the
/// underlying `PnCounter` join is associative and idempotent, chaining that
/// pairwise guarantee across every pair in one pass (in any order) carries
/// every origin's write to every shard by the last pair that touches it.
async fn bidirectional_gossip_pass(shards: &[Arc<Shard<u8, PnCounter>>], key_bytes: &Bytes) {
    for i in 0..shards.len() {
        for j in (i + 1)..shards.len() {
            let from_i = ShardOps::records_for(shards[i].as_ref(), vec![key_bytes.clone()])
                .await
                .into_iter()
                .next();
            let from_j = ShardOps::records_for(shards[j].as_ref(), vec![key_bytes.clone()])
                .await
                .into_iter()
                .next();
            if let Some(rec) = from_j {
                ShardOps::apply_remote(shards[i].as_ref(), rec).await;
            }
            if let Some(rec) = from_i {
                ShardOps::apply_remote(shards[j].as_ref(), rec).await;
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Bidirectional analog of
    /// [`pn_counter_gossip_converges_to_the_exact_sum_within_bounded_rounds`]:
    /// the same per-origin seeding (each shard starts having seen only its
    /// own origin's, possibly redelivered and reordered, write), but driven
    /// through exactly one [`bidirectional_gossip_pass`] instead of a
    /// bounded loop of unidirectional rounds. Every pair of shards must hold
    /// an identical `(version, bytes)` pair after that one pass — the
    /// one-round convergence property `engine::merge_version`'s doc proves
    /// for a symmetric bidirectional exchange — and that shared value must
    /// be the exact sum of every generated increment, with no lost updates.
    #[test]
    fn pn_counter_bidirectional_gossip_converges_in_one_round(
        deltas in proptest::collection::vec(0u64..1_000, usize::from(NUM_NODES)),
        seeds in proptest::collection::vec(any::<u64>(), usize::from(NUM_NODES)),
    ) {
        let records = build_pn_counter_records(&deltas);
        let expected_total = i64::try_from(deltas.iter().sum::<u64>()).expect("fits");
        let key_bytes = records[0].key.clone();
        let rt = current_thread_runtime();

        rt.block_on(async {
            let shards: Vec<Arc<Shard<u8, PnCounter>>> = (0..records.len())
                .map(|i| {
                    Arc::new(
                        Shard::<u8, PnCounter>::new(
                            SmolStr::new("pn-counter-bidi-gossip"),
                            Mode::Replicated,
                            NodeId::from(4000 + u64::try_from(i).expect("small")),
                            10_000,
                            None,
                            None,
                        )
                        .with_resolver(Arc::new(PnCounterResolver)),
                    )
                })
                .collect();

            // Each shard starts having seen only its own origin's write --
            // possibly redelivered, in a generated order -- exactly as
            // `pn_counter_gossip_converges_to_the_exact_sum_within_bounded_rounds`
            // seeds it, so the one pass below is the only thing carrying
            // every other origin's increment to a given shard.
            for ((shard, own_record), &seed) in shards.iter().zip(&records).zip(&seeds) {
                let redelivered = shuffled_with_duplicates(std::slice::from_ref(own_record), seed);
                apply_mixed(shard, redelivered, seed).await;
            }

            bidirectional_gossip_pass(&shards, &key_bytes).await;

            let mut final_records = Vec::with_capacity(shards.len());
            for shard in &shards {
                let rec = ShardOps::records_for(shard.as_ref(), vec![key_bytes.clone()])
                    .await
                    .into_iter()
                    .next()
                    .expect("every shard holds the key after one bidirectional pass");
                let counter = PnCounter::decode(
                    rec.value.as_deref().expect("a live PnCounter record carries a value"),
                )
                .expect("PnCounter always decodes");
                assert_eq!(
                    counter.value(),
                    expected_total,
                    "one bidirectional pass must converge every shard to the exact sum of \
                     every generated increment, with no lost updates"
                );
                final_records.push((rec.ver, rec.value));
            }
            for i in 0..final_records.len() {
                for j in (i + 1)..final_records.len() {
                    assert_eq!(
                        final_records[i], final_records[j],
                        "every pair of shards must hold an identical (version, bytes) pair \
                         after exactly one bidirectional pass"
                    );
                }
            }
        });
    }
}
