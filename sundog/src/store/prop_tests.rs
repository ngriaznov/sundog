//! Property tests for the store (plan §11.1). This is the highest-value
//! suite in the project: [`permutation_convergence`] is the direct test of
//! the license for the whole loss-tolerant design (plan §4) — applying the
//! same multiset of versioned writes, in any order, with any duplication,
//! converges to byte-identical state. Alongside it: a focused tombstone/put
//! race property, and incremental-digest-vs-full-recompute across arbitrary
//! op sequences including anti-entropy-style repairs and tombstone GC.

use proptest::prelude::*;
use rand::seq::SliceRandom as _;
use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};

use super::*;

/// Origin nodes a generated workload is spread across.
const NUM_NODES: u8 = 3;
/// Fresh shards the same multiset is replayed into, each under its own
/// sampled permutation and duplication.
const NUM_REPLICAS: usize = 4;
/// Keyspace size: small on purpose, so puts/removes from different origins
/// collide on the same key and race — the case that matters for plan §4.
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
/// exchange that makes real HLC stamps interleave across nodes, not just
/// advance independently.
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
/// internal (e.g. moka iteration) order: live entries as `(key, value, ver)`,
/// the tombstone set as `(key, ver)`, and all [`BUCKET_COUNT`] digests.
type CanonicalState = (Vec<(Bytes, Bytes, Hlc)>, Vec<(Bytes, Hlc)>, Vec<u64>);

async fn canonical_state<K, V>(shard: &Shard<K, V>) -> CanonicalState
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let mut live: Vec<(Bytes, Bytes, Hlc)> = shard
        .cache
        .iter()
        .map(|(key, stored)| {
            let key_bytes = Bytes::from(postcard::to_stdvec(&*key).expect("test key encodes"));
            let value_bytes =
                Bytes::from(postcard::to_stdvec(&stored.value).expect("test value encodes"));
            (key_bytes, value_bytes, stored.ver)
        })
        .collect();
    live.sort_by(|a, b| a.0.cmp(&b.0));

    let mut tomb: Vec<(Bytes, Hlc)> = shard
        .tombstones
        .lock()
        .await
        .iter()
        .map(|(key_bytes, t)| (key_bytes.clone(), t.ver))
        .collect();
    tomb.sort_by(|a, b| a.0.cmp(&b.0));

    let digest: Vec<u64> = shard
        .digest
        .iter()
        .map(|d| d.load(Ordering::Relaxed))
        .collect();
    (live, tomb, digest)
}

/// Applies `records` (already permuted and duplicated by
/// [`shuffled_with_duplicates`]) as a mix of single and batch applies:
/// `seed`-derived contiguous run lengths (1..=4) decide, for each run,
/// whether it goes through [`ShardOps::apply_remote`] (length 1) or
/// [`ShardOps::apply_remote_batch`] (length > 1, one lock acquisition for
/// the whole run) — mirroring how live traffic actually arrives (a
/// coalesced `Msg::ReplicateBatch` next to individual `Msg::Replicate`s).
/// Consuming `records` in order and only choosing how to *group* the calls
/// keeps the effective application order identical to always calling
/// `apply_remote` one record at a time, so this must converge to the same
/// state the single-apply path always has.
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

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("invariant: a current-thread runtime always builds")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// The permutation-convergence property (plan §4, §11.1): apply the same
    /// multiset of writes/removes across several interleaved-clock origin
    /// nodes to multiple fresh shards, each in its own sampled permutation
    /// with random duplication — the converged state (live entries,
    /// tombstones, and all 1024 digests) must be byte-identical everywhere.
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
                let shard = Shard::<u8, u16>::new(
                    SmolStr::new("perm-conv"),
                    Mode::Replicated,
                    NodeId::from(1000),
                    10_000,
                    None,
                    None,
                );
                apply_mixed(&shard, shuffled_with_duplicates(&records, seed), seed).await;
                states.push(canonical_state(&shard).await);
            }
            for state in &states[1..] {
                assert_eq!(
                    &states[0], state,
                    "permutation, duplication, and mixing single/batch applies must not change \
                     the converged state"
                );
            }
        });
    }

    /// Focused tombstone/put race: whichever of a put and a tombstone at the
    /// same key carries the newer [`Hlc`] wins, and that outcome — plus the
    /// full converged state — must be identical across every permutation and
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
                    "the newer of {{put, tombstone}} must win regardless of application order \
                     or whether it went through a single or batch apply"
                );
                states.push(canonical_state(&shard).await);
            }
            for state in &states[1..] {
                assert_eq!(&states[0], state);
            }
        });
    }
}

/// One full pass over live entries + tombstones — same technique as the
/// hand-rolled version in `mod tests`, duplicated here since that helper is
/// private to its own sibling module.
async fn digest_matches_full_recompute<K, V>(shard: &Shard<K, V>) -> bool
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let mut expected = vec![0u64; BUCKET_COUNT];
    for (key, stored) in &shard.cache {
        let key_bytes = postcard::to_stdvec(&*key).expect("test key encodes");
        expected[usize::from(bucket_of(&key_bytes))] ^= entry_fingerprint(&key_bytes, stored.ver);
    }
    for (key_bytes, t) in shard.tombstones.lock().await.iter() {
        expected[usize::from(bucket_of(key_bytes))] ^= entry_fingerprint(key_bytes, t.ver);
    }
    let actual: Vec<u64> = shard
        .digest
        .iter()
        .map(|d| d.load(Ordering::Relaxed))
        .collect();
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
    /// must always equal a full recompute from the live entries and
    /// un-GC'd tombstones (plan §8, §11.1) — and immediately after a GC pass
    /// (with `tombstone_ttl` zeroed), every tombstone must actually be gone.
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
                        ShardOps::gc_tombstones(&shard).await;
                        assert!(
                            shard.tombstones.lock().await.is_empty(),
                            "zero tombstone_ttl means every tombstone is GC-eligible immediately"
                        );
                    }
                }
                assert!(
                    digest_matches_full_recompute(&shard).await,
                    "incremental digest diverged from full recompute after {op:?}"
                );
            }
        });
    }
}
