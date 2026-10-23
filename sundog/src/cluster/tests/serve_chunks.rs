//! Tests for serve ordering and state-transfer chunking against recording shards.

use super::*;
use crate::cluster::handler::{bucket_runs, serve_order, st_bucket_key_sub_batches};
use crate::hlc::Hlc;
use crate::ownership::Granularity;
use crate::store::{BucketDigest, BucketLen, BucketPart, BucketPartDigests, KeyVersion};
use crate::wire::{self, WireRecord};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt as _};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn group_in_order_keeps_first_seen_key_order_and_input_value_order() {
    let groups = group_in_order([('b', [1]), ('a', [2]), ('b', [3])]);
    assert_eq!(groups, vec![('b', vec![1, 3]), ('a', vec![2])]);
    let merged = group_in_order([("k", vec![1, 2]), ("k", vec![]), ("k", vec![3])]);
    assert_eq!(merged, vec![("k", vec![1, 2, 3])]);
    let groups = group_in_order(Vec::<(u8, [u8; 0])>::new());
    assert!(groups.is_empty(), "{groups:?}");
}

#[test]
fn serve_order_sorts_bucket_ids_and_groups_part_ids_by_bucket() {
    assert_eq!(serve_order(Granularity::Bucket, vec![9, 3, 9]), vec![3, 9]);
    let (a, b, c) = (PartId::new(5, 2), PartId::new(3, 9), PartId::new(5, 0));
    assert_eq!(
        serve_order(Granularity::Part, vec![a.raw(), b.raw(), c.raw(), a.raw()]),
        vec![b.raw(), c.raw(), a.raw()],
        "bucket 3 first, then bucket 5's parts in part order, each once"
    );
}

#[test]
fn bucket_runs_groups_consecutive_part_ids_of_one_bucket() {
    let runs = |granularity, ids: &[u16]| -> Vec<Vec<u16>> {
        bucket_runs(granularity, ids).map(<[u16]>::to_vec).collect()
    };
    assert_eq!(
        runs(Granularity::Bucket, &[3, 9]),
        vec![vec![3], vec![9]],
        "a bucket id is a run of its own"
    );
    let ids = serve_order(
        Granularity::Part,
        vec![
            PartId::new(5, 2).raw(),
            PartId::new(3, 9).raw(),
            PartId::new(5, 0).raw(),
        ],
    );
    assert_eq!(
        runs(Granularity::Part, &ids),
        vec![
            vec![PartId::new(3, 9).raw()],
            vec![PartId::new(5, 0).raw(), PartId::new(5, 2).raw()],
        ]
    );
    let no_runs = runs(Granularity::Part, &[]);
    assert!(no_runs.is_empty(), "{no_runs:?}");
}

#[test]
fn st_bucket_key_sub_batches_of_no_keys_is_empty() {
    assert_eq!(
        st_bucket_key_sub_batches(Vec::new(), 1024),
        Vec::<Vec<Bytes>>::new()
    );
}

#[test]
fn st_bucket_key_sub_batches_splits_by_estimated_size() {
    let keys: Vec<Bytes> = (0u8..5).map(|n| Bytes::from(vec![n])).collect();
    let per_key = (wire::RECORD_HEADER_LEN + 1) as u64;
    let sub_batches = st_bucket_key_sub_batches(keys.clone(), per_key * 2);
    assert_eq!(
        sub_batches,
        vec![
            vec![keys[0].clone(), keys[1].clone()],
            vec![keys[2].clone(), keys[3].clone()],
            vec![keys[4].clone()],
        ],
        "two one-byte keys fit the two-key budget exactly before a group closes"
    );
}

#[test]
fn st_bucket_key_sub_batches_gives_an_oversized_key_its_own_batch() {
    let small = Bytes::from_static(b"a");
    let big = Bytes::from(vec![0u8; 4096]);
    // Fits `small`; nowhere near enough for `big`.
    let budget = (wire::RECORD_HEADER_LEN + 1) as u64;
    let sub_batches = st_bucket_key_sub_batches(vec![small.clone(), big.clone()], budget);
    assert_eq!(
        sub_batches,
        vec![vec![small], vec![big]],
        "a key whose own estimate exceeds the budget still gets a batch of its own"
    );
}

/// A [`ShardOps`] test double recording every `records_for` call's key
/// list. Every method but `entries_for_buckets`, `records_for`, and
/// `close_spill` panics if called, since that would be a test bug.
struct RecordingShard {
    entries: Vec<(u16, Vec<KeyVersion>)>,
    held: Vec<u16>,
    records: HashMap<Bytes, WireRecord>,
    records_for_calls: Mutex<Vec<Vec<Bytes>>>,
    close_spill_calls: AtomicUsize,
}

impl ShardOps for RecordingShard {
    fn apply_remote(&self, _rec: WireRecord) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls apply_remote")
    }

    fn apply_remote_batch(&self, _recs: Vec<WireRecord>) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls apply_remote_batch")
    }

    fn invalidate(&self, _key: Bytes, _ver: Hlc) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls invalidate")
    }

    fn digests(&self) -> BoxFuture<'_, Vec<BucketDigest>> {
        unimplemented!("st_bucket_chunks never calls digests")
    }

    fn bucket_entries(&self, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        unimplemented!("st_bucket_chunks calls entries_for_buckets, not bucket_entries")
    }

    fn entries_for_buckets(&self, buckets: Vec<u16>) -> BoxFuture<'_, crate::store::BucketEntries> {
        let entries = self.entries.clone();
        Box::pin(async move {
            entries
                .into_iter()
                .filter(|(bucket, _)| buckets.contains(bucket))
                .collect()
        })
    }

    fn bucket_lens(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        unimplemented!("st_bucket_chunks never calls bucket_lens")
    }

    fn part_digests(&self, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        unimplemented!("st_bucket_chunks never calls part_digests")
    }

    fn entries_for_parts(
        &self,
        _parts: Vec<BucketPart>,
    ) -> BoxFuture<'_, crate::store::PartEntries> {
        unimplemented!("st_bucket_chunks never calls entries_for_parts")
    }

    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        self.records_for_calls
            .lock()
            .expect("invariant: fixture mutex is never poisoned")
            .push(keys.clone());
        Box::pin(async move {
            keys.into_iter()
                .filter_map(|k| self.records.get(&k).cloned())
                .collect()
        })
    }

    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>> {
        unimplemented!("st_bucket_chunks never calls snapshot_chunks")
    }

    fn gc_tombstones(&self, _any_member_absent: bool) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls gc_tombstones")
    }

    fn run_pending_tasks(&self) -> BoxFuture<'_, ()> {
        unimplemented!("st_bucket_chunks never calls run_pending_tasks")
    }

    fn close_spill(&self) {
        self.close_spill_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn held_buckets(&self) -> BoxFuture<'_, Vec<u16>> {
        let held = self.held.clone();
        Box::pin(async move { held })
    }
}

/// `ShardOps`' part-granular defaults, on a stub that implements only
/// the bucket forms: held parts are every part of each held bucket,
/// nothing releases or reads cold or unverified, and a stub with no
/// view has no scoped digests.
#[tokio::test]
async fn shard_ops_part_defaults_follow_the_bucket_forms() {
    let stub = RecordingShard {
        entries: Vec::new(),
        held: vec![3, 900],
        records: HashMap::new(),
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    };
    let held = stub.held_parts().await;
    assert_eq!(held.len(), 2 * crate::store::PART_COUNT);
    assert!(held.iter().all(|part| [3, 900].contains(&part.bucket())));
    assert_eq!(stub.release_parts(&held).await, 0);
    assert!(!stub.is_cold_part(held[0]));
    assert!(!stub.is_unverified_part(held[0]));
    assert!(stub.ae_scoped_digests_for(NodeId::from(2)).await.is_none());
}

#[tokio::test]
async fn st_bucket_chunks_never_materializes_more_than_rebalance_chunk_bytes_for_a_bucket_larger_than_the_budget()
 {
    let bucket: u16 = 7;
    let key_count = 10u8;
    let keys: Vec<Bytes> = (0..key_count).map(|n| Bytes::from(vec![n])).collect();
    let ver = Hlc {
        wall_ms: 1,
        logical: 0,
        node: NodeId::from(1),
    };
    let entries: Vec<KeyVersion> = keys
        .iter()
        .map(|key| KeyVersion {
            key: key.clone(),
            version: ver,
        })
        .collect();
    let records: HashMap<Bytes, WireRecord> = keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                WireRecord {
                    key: key.clone(),
                    value: Some(Bytes::from_static(b"v")),
                    ver,
                    expires_at_ms: None,
                },
            )
        })
        .collect();

    // A budget of three keys' worth forces the ten-key bucket into
    // more than one `records_for` call.
    let per_key_size = (wire::RECORD_HEADER_LEN + 1) as u64;
    let chunk_bytes = per_key_size * 3;

    let recording = Arc::new(RecordingShard {
        entries: vec![(bucket, entries)],
        held: Vec::new(),
        records,
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    });
    let shard: Arc<dyn ShardOps> = recording.clone();
    let mut registry = HashMap::new();
    registry.insert(SmolStr::new("cache"), shard);
    let handler = ClusterRequestHandler {
        shards: Arc::new(RwLock::new(registry)),
        warmth: Arc::new(Warmth::default()),
        ae_part_min_bucket: ClusterConfig::default().ae_part_min_bucket,
        ae_sketch_min_bucket: ClusterConfig::default().ae_sketch_min_bucket,
        ae_sketch_cells: ClusterConfig::default().ae_sketch_cells,
        rebalance_chunk_bytes: chunk_bytes,
    };

    let mut got_keys: Vec<Bytes> = Vec::new();
    let mut chunks = handler.st_bucket_chunks(SmolStr::new("cache"), vec![bucket]);
    while let Some((got_bucket, chunk)) = chunks.next().await {
        assert_eq!(
            got_bucket, bucket,
            "every yielded chunk is tagged with the bucket"
        );
        got_keys.extend(chunk.into_iter().map(|r| r.key));
    }
    got_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    let mut expected_keys = keys.clone();
    expected_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    assert_eq!(
        got_keys, expected_keys,
        "every key's record is served exactly once, regardless of sub-batching"
    );

    let calls = recording
        .records_for_calls
        .lock()
        .expect("invariant: fixture mutex is never poisoned");
    assert!(
        calls.len() > 1,
        "a bucket larger than the budget is fetched over more than one records_for call, not all at once"
    );
    for call in calls.iter() {
        let call_size: u64 = call
            .iter()
            .map(|k| (wire::RECORD_HEADER_LEN + k.len()) as u64)
            .sum();
        assert!(
            call_size <= chunk_bytes,
            "one records_for call never exceeds the rebalance_chunk_bytes estimate: {call_size} > {chunk_bytes}"
        );
    }
    let total_keys: usize = calls.iter().map(Vec::len).sum();
    assert_eq!(
        total_keys, key_count as usize,
        "every key is fetched exactly once across sub-batches"
    );
}

/// [`ShardOps::close_spill_checkpointed`]'s default, for an implementor
/// that never overrides it, calls the sync [`ShardOps::close_spill`]
/// once.
#[tokio::test]
async fn close_spill_checkpointed_default_resolves_and_calls_the_sync_close() {
    let recording = Arc::new(RecordingShard {
        entries: Vec::new(),
        held: Vec::new(),
        records: HashMap::new(),
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    });
    let shard: Arc<dyn ShardOps> = recording.clone();

    shard.close_spill_checkpointed().await;

    assert_eq!(
        recording.close_spill_calls.load(Ordering::Relaxed),
        1,
        "the default close_spill_checkpointed calls the sync close_spill exactly once"
    );
}

/// A shard that does not override [`ShardOps::skip_unframeable`] has nothing
/// it cannot ship, so the default hands back every record, in order.
#[test]
fn skip_unframeable_default_hands_back_every_record_unchanged() {
    let shard = RecordingShard {
        entries: Vec::new(),
        held: Vec::new(),
        records: HashMap::new(),
        records_for_calls: Mutex::new(Vec::new()),
        close_spill_calls: AtomicUsize::new(0),
    };
    let record = |key: &'static [u8]| WireRecord {
        key: Bytes::from_static(key),
        value: Some(Bytes::from(vec![0u8; wire::MAX_FRAME])),
        ver: Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1),
        },
        expires_at_ms: None,
    };
    let records = vec![record(b"a"), record(b"b")];

    assert_eq!(shard.skip_unframeable(records.clone()), records);
}
