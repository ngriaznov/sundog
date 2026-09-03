//! Property tests for the store. [`permutation_convergence`] is the direct
//! test of the license for the whole loss-tolerant design: applying the
//! same multiset of versioned writes, in any order, with any duplication,
//! converges to byte-identical state. Alongside it: a focused tombstone/put
//! race property, incremental-digest-vs-full-recompute across arbitrary op
//! sequences including anti-entropy-style repairs and tombstone GC, a
//! clock-driven property that puts [`Shard::with_clock`] through TTL expiry
//! and sweep correctness on a manual clock instead of real time, and
//! [`shard_matches_the_reference_model_under_arbitrary_op_sequences`], which
//! runs the [`model::run`] driver `sundog-fuzz`'s stateful apply-path
//! targets use, over proptest-generated op sequences instead of
//! libFuzzer-guided ones.

use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;
use proptest_arbitrary_interop::arb;
use rand::seq::SliceRandom as _;
use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

use super::*;

/// Origin nodes a generated workload is spread across.
const NUM_NODES: u8 = 3;
/// Fresh shards the same multiset is replayed into, each under its own
/// sampled permutation and duplication.
const NUM_REPLICAS: usize = 4;
/// Keyspace size: small on purpose, so puts/removes from different origins
/// collide on the same key and race — the case that exercises versioned-apply
/// convergence.
const KEYSPACE: u8 = 8;

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

/// Turns a sequence of (origin, op) pairs into the actual multiset of
/// [`WireRecord`]s a live cluster would have produced: each origin stamps
/// with its own [`HlcClock`], and every write is observed by the next node
/// in rotation immediately after — a stand-in for the gossip/replication
/// exchange that makes real HLC stamps interleave across nodes instead of
/// only advancing independently.
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

/// Duplicates every record a random number of times (1–3) and shuffles the
/// result under `seed` — a sampled permutation with random duplication,
/// deterministic given the seed so proptest shrinking stays reproducible.
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

/// The full observable state of a shard, canonicalized (sorted by key) so two
/// shards that converged to the same content compare equal regardless of
/// internal (per-stripe) iteration order: live entries as `(key, value,
/// ver)`, the tombstone set as `(key, ver)`, and all [`BUCKET_COUNT`]
/// digests.
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

/// Applies `records` (already permuted and duplicated by
/// [`shuffled_with_duplicates`]) as a mix of single and batch applies:
/// `seed`-derived contiguous run lengths (1..=4) decide, for each run,
/// whether it goes through [`ShardOps::apply_remote`] (length 1) or
/// [`ShardOps::apply_remote_batch`] (length > 1, one lock acquisition for
/// the whole run) — mirroring how live traffic arrives (a coalesced
/// `Msg::ReplicateBatch` next to individual `Msg::Replicate`s).
/// Consuming `records` in order and only choosing how to *group* the calls
/// keeps the effective application order identical to always calling
/// `apply_remote` one record at a time, so this converges to the same state
/// the single-apply path always has.
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

/// Applies `records` (already permuted and duplicated by
/// [`shuffled_with_duplicates`]) via real concurrent scheduling rather than
/// [`apply_mixed`]'s sequential run-grouping: splits into a handful of
/// partitions and drives them all through [`tokio::spawn`] at once (each
/// partition itself applied via [`apply_mixed`]'s single/batch mix). Two
/// records for the same key landing in different partitions is exactly the
/// stress case per-key stripe serialization exists for — the stripe's own
/// lock, not program order, keeps concurrently scheduled applies to that key
/// from racing — so this still converges to the identical state
/// [`apply_mixed`] alone always has.
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

    /// The permutation-convergence property: apply the same multiset of
    /// writes/removes across several interleaved-clock origin nodes to
    /// multiple fresh shards, each in its own sampled permutation with
    /// random duplication — the converged state (live entries, tombstones,
    /// and all 1024 digests) is byte-identical everywhere. Alternates, by
    /// seed parity, between [`apply_mixed`]'s sequential single/batch mix and
    /// [`apply_concurrent`]'s real concurrent scheduling across stripes, so
    /// the property holds under real scheduling too, not only sampled
    /// sequential orderings.
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

    /// Focused tombstone/put race: whichever of a put and a tombstone at the
    /// same key carries the newer [`Hlc`] wins, and that outcome — plus the
    /// full converged state — is identical across every permutation and
    /// duplication of the two records.
    #[test]
    fn tombstone_put_race_converges_regardless_of_order(
        put_wall in 0u64..10_000,
        put_node in 0u8..4,
        tomb_wall in 0u64..10_000,
        tomb_node in 0u8..4,
        put_value in any::<u16>(),
        seeds in proptest::collection::vec(any::<u64>(), NUM_REPLICAS),
    ) {
        // Disjoint node-id ranges: put and tombstone always come from
        // distinct nodes, so their stamps never tie and the race always has
        // a definitive winner.
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

/// Compares the engine's incrementally-maintained digest against
/// [`engine::Engine::recompute_digests`]'s full pass over every stripe.
fn digest_matches_full_recompute<K, V>(shard: &Shard<K, V>) -> bool
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let expected = shard.engine.recompute_digests();
    let actual: Vec<u64> = shard.engine.digests().into_iter().map(|(_, d)| d).collect();
    actual == expected
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

    /// After an arbitrary sequence of local writes, remote applies,
    /// invalidations, and tombstone GC, the incrementally-maintained digest
    /// always equals a full recompute from the live entries and un-GC'd
    /// tombstones — and immediately after a GC pass (with `tombstone_ttl`
    /// zeroed), every tombstone is gone.
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
            }
        });
    }
}

/// One op in [`clock_driven_sweep_keeps_digest_and_readability_consistent`]'s
/// workload: a TTL'd local insert, a remove, a remote apply carrying its own
/// (possibly already-past) absolute deadline, a manual-clock advance, a
/// sweep, or a tombstone GC pass.
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
/// [`ShardOps::entries_for_buckets`] — the same check anti-entropy itself
/// makes, using only the public trait surface (no reach into engine
/// internals, unlike [`digest_matches_full_recompute`] above).
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

/// For every key in the small keyspace that currently reads as present, its
/// own record's `expires_at_ms` (fetched through the public
/// [`ShardOps::records_for`], not engine internals) is not yet past
/// `now_ms` — the direct statement of "no readable entry is expired".
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

    /// Drives a shard through random TTL'd inserts, remote applies (each
    /// carrying its own absolute, possibly already-past deadline), removes,
    /// manual-clock advances, sweeps, and tombstone GC — all timestamped by
    /// a manual clock installed via [`Shard::with_clock`], never real time,
    /// so this runs fast and deterministically. After every
    /// [`ClockOp::Sweep`], the digest equals a full recompute over
    /// [`ShardOps::entries_for_buckets`], and no readable entry is past its
    /// own deadline.
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

    /// Runs [`model::run`] — the driver `sundog-fuzz`'s `apply_model` and
    /// `apply_permutation` targets use — over a sequence of `model::Op`,
    /// each one `arbitrary`-generated and bridged into a proptest strategy
    /// by `proptest-arbitrary-interop`, with `proptest::collection::vec`
    /// controlling the sequence length. Asking `arb` for the whole
    /// `Vec<Op>` at once would hand length control to `arbitrary`'s own
    /// geometric continuation instead, which averages under two ops per
    /// sequence — far too short to exercise cross-op interactions. The
    /// model and the driver's assertions live once, in `store::model`; this
    /// test and the fuzz targets differ only in how they sample op
    /// sequences, so a divergence either finds is a real bug in versioned
    /// apply, digest bookkeeping, tombstone retention, expiry, or resolver —
    /// not in this test, which has no assertions of its own.
    #[test]
    fn shard_matches_the_reference_model_under_arbitrary_op_sequences(
        ops in proptest::collection::vec(arb::<model::Op>(), 1..128),
    ) {
        let (shard, mut model) = model::new_shard_and_model("model-prop", 1);
        model::run(&ops, &shard, &mut model);
    }
}
