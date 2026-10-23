//! A round's pulls in batches: the byte cap beside the key cap, and a failed
//! batch that does not end the rest.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use proptest::prelude::*;

use super::*;
use crate::net::RequestHandler;
use crate::net::test_support::{peer_at, spawn_mesh};
use crate::store::{BucketEntries, BucketLen, BucketPartDigests, KeyVersion, Mode, PartEntries};

fn key_of(len: usize, fill: u8) -> Bytes {
    Bytes::from(vec![fill; len])
}

#[test]
fn a_batch_ends_at_the_key_count_or_the_key_bytes_whichever_comes_first() {
    let keys: Vec<Bytes> = (0..7).map(|n| key_of(10, n)).collect();

    let by_count = pull_batches(&keys, 3, 1_000);
    assert_eq!(
        by_count.iter().map(|b| b.len()).collect::<Vec<_>>(),
        [3, 3, 1],
        "the count caps a batch"
    );

    let by_bytes = pull_batches(&keys, 100, 25);
    assert_eq!(
        by_bytes.iter().map(|b| b.len()).collect::<Vec<_>>(),
        [2, 2, 2, 1],
        "two ten-byte keys fit twenty-five bytes, three do not"
    );

    let exact = pull_batches(&keys, 100, 30);
    assert_eq!(
        exact.iter().map(|b| b.len()).collect::<Vec<_>>(),
        [3, 3, 1],
        "a batch may fill the byte cap exactly"
    );
}

#[test]
fn a_key_longer_than_the_byte_cap_is_a_batch_of_its_own_and_nothing_is_lost() {
    let keys = vec![key_of(5, 1), key_of(500, 2), key_of(5, 3), key_of(5, 4)];

    let batches = pull_batches(&keys, 100, 20);

    assert_eq!(
        batches.iter().map(|b| b.len()).collect::<Vec<_>>(),
        [1, 1, 2],
        "the long key neither joins its neighbours nor is dropped"
    );
    assert_eq!(batches.concat(), keys);
}

#[test]
fn no_keys_make_no_batches() {
    assert_eq!(pull_batches(&[], 4, 100), Vec::<&[Bytes]>::new());
}

proptest! {
    /// Every key lands in exactly one batch, in order, and a batch is within
    /// both caps unless it is one key alone.
    #[test]
    fn batches_cover_the_keys_in_order_within_both_caps(
        lens in proptest::collection::vec(0usize..40, 0..60),
        max_keys in 1usize..8,
        max_bytes in 1usize..80,
    ) {
        let keys: Vec<Bytes> = lens.iter().enumerate().map(|(n, &len)| key_of(len, u8::try_from(n % 250).unwrap_or(0))).collect();

        let batches = pull_batches(&keys, max_keys, max_bytes);

        prop_assert_eq!(batches.concat(), keys.clone());
        for batch in &batches {
            prop_assert!(!batch.is_empty());
            prop_assert!(batch.len() <= max_keys);
            let bytes: usize = batch.iter().map(Bytes::len).sum();
            prop_assert!(bytes <= max_bytes || batch.len() == 1);
        }
    }
}

#[test]
fn a_round_gives_up_on_its_pulls_after_two_failed_requests_in_a_row() {
    assert!(!pull_gives_up(0));
    assert!(!pull_gives_up(1));
    assert!(pull_gives_up(2));
    assert!(pull_gives_up(3));
}

/// A peer that answers pulls from a script: the calls named in `failing`
/// close the connection mid-reply, by offering a record no frame can hold,
/// and every other call answers one small record named after its number.
struct ScriptedPeer {
    failing: Vec<usize>,
    calls: AtomicUsize,
    requested: Mutex<Vec<usize>>,
}

impl ScriptedPeer {
    fn new(failing: Vec<usize>) -> Arc<Self> {
        Arc::new(Self {
            failing,
            calls: AtomicUsize::new(0),
            requested: Mutex::new(Vec::new()),
        })
    }

    fn answer(&self, requested: usize) -> Vec<WireRecord> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requested.lock().expect("lock").push(requested);
        if self.failing.contains(&call) {
            return vec![WireRecord {
                key: Bytes::from_static(b"poison"),
                value: Some(Bytes::from(vec![0u8; wire::MAX_FRAME])),
                ver: scripted_ver(),
                expires_at_ms: None,
            }];
        }
        let (key, rec) = super::encode_test_record(
            u32::try_from(call).expect("a few calls"),
            "pulled",
            NodeId::from(2),
        );
        let _ = key;
        vec![rec]
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn scripted_ver() -> Hlc {
    Hlc {
        wall_ms: 1,
        logical: 0,
        node: NodeId::from(2),
    }
}

impl RequestHandler for ScriptedPeer {
    fn snapshot_chunks(&self, _cache: SmolStr) -> BoxStream<'static, Vec<WireRecord>> {
        Box::pin(futures::stream::empty())
    }

    fn digests(&self, _cache: SmolStr) -> BoxFuture<'_, Vec<crate::store::BucketDigest>> {
        Box::pin(async { Vec::new() })
    }

    fn bucket_entries(&self, _cache: SmolStr, _bucket: u16) -> BoxFuture<'_, Vec<KeyVersion>> {
        Box::pin(async { Vec::new() })
    }

    fn entries_for_buckets(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, BucketEntries> {
        Box::pin(async { Vec::new() })
    }

    fn records_for(&self, _cache: SmolStr, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>> {
        let answer = self.answer(keys.len());
        Box::pin(async move { answer })
    }

    fn records_for_hashes(
        &self,
        _cache: SmolStr,
        _bucket: u16,
        hashes: Vec<u64>,
    ) -> BoxFuture<'_, Vec<WireRecord>> {
        let answer = self.answer(hashes.len());
        Box::pin(async move { answer })
    }

    fn bucket_lens(&self, _cache: SmolStr, _buckets: Vec<u16>) -> BoxFuture<'_, Vec<BucketLen>> {
        Box::pin(async { Vec::new() })
    }

    fn part_digests(
        &self,
        _cache: SmolStr,
        _buckets: Vec<u16>,
    ) -> BoxFuture<'_, Vec<BucketPartDigests>> {
        Box::pin(async { Vec::new() })
    }

    fn entries_for_parts(
        &self,
        _cache: SmolStr,
        _parts: Vec<crate::store::BucketPart>,
    ) -> BoxFuture<'_, PartEntries> {
        Box::pin(async { Vec::new() })
    }
}

/// A requester mesh that knows a responder answering from `script`, and a
/// shard for the records it pulls.
async fn requester(script: &Arc<ScriptedPeer>) -> (Mesh, Mesh, Arc<dyn ShardOps>, SmolStr) {
    let handler: Arc<dyn RequestHandler> = Arc::clone(script) as Arc<dyn RequestHandler>;
    let (responder, _inbound) = spawn_mesh(NodeId::from(2), handler).await;
    let (mesh, _inbound) = spawn_mesh(
        NodeId::from(1),
        Arc::new(crate::net::test_support::BucketPullHandler::default()),
    )
    .await;
    mesh.update_peers(vec![peer_at(NodeId::from(2), responder.local_addr())]);
    let name = SmolStr::new("pulled");
    let shard: Arc<dyn ShardOps> = Arc::new(crate::store::Shard::<u32, String>::new(
        name.clone(),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    ));
    (mesh, responder, shard, name)
}

/// Keys of 300 KB each: the byte cap puts one in a batch.
fn long_keys(count: u8) -> Vec<Bytes> {
    (0..count).map(|n| key_of(300 * 1_024, n)).collect()
}

/// `count` batches' worth of hashes for one bucket.
fn hash_batches(count: usize) -> Vec<(u16, Vec<u64>)> {
    vec![(
        0,
        (0..u64::try_from(count * REPAIR_BATCH).expect("fits")).collect(),
    )]
}

async fn pull(
    mesh: &Mesh,
    shard: &Arc<dyn ShardOps>,
    name: &SmolStr,
    keys: Vec<Bytes>,
    hashes: Vec<(u16, Vec<u64>)>,
) -> (HashMap<Bytes, Hlc>, bool) {
    let mut tally = RepairTally::default();
    pull_repairs(mesh, shard, name, NodeId::from(2), keys, hashes, &mut tally).await
}

#[tokio::test]
async fn a_failed_key_batch_does_not_end_the_batches_after_it() {
    let script = ScriptedPeer::new(vec![0]);
    let (mesh, responder, shard, name) = requester(&script).await;

    let (pulled, complete) = pull(&mesh, &shard, &name, long_keys(3), Vec::new()).await;

    assert!(!complete, "a pull failed");
    assert_eq!(script.calls(), 3, "all three batches were asked for");
    assert_eq!(
        *script.requested.lock().expect("lock"),
        [1, 1, 1],
        "one long key each"
    );
    assert_eq!(pulled.len(), 2, "the second and third batches landed");
    for call in [1u32, 2] {
        assert!(
            shard
                .records_for(vec![Bytes::from(
                    postcard::to_stdvec(&call).expect("encodes")
                )])
                .await
                .len()
                == 1,
            "the record of call {call} is applied"
        );
    }
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn a_failed_hash_batch_does_not_end_the_batches_after_it() {
    let script = ScriptedPeer::new(vec![0]);
    let (mesh, responder, shard, name) = requester(&script).await;

    let (pulled, complete) = pull(&mesh, &shard, &name, Vec::new(), hash_batches(3)).await;

    assert!(!complete);
    assert_eq!(script.calls(), 3);
    assert_eq!(pulled.len(), 2);
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn the_hash_batches_follow_the_key_batches_when_one_fails() {
    let script = ScriptedPeer::new(vec![1]);
    let (mesh, responder, shard, name) = requester(&script).await;

    let (pulled, complete) = pull(&mesh, &shard, &name, long_keys(2), hash_batches(1)).await;

    assert!(!complete);
    assert_eq!(script.calls(), 3, "two key batches, then the hashes");
    assert_eq!(pulled.len(), 2);
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn two_failures_in_a_row_end_the_pulls() {
    let script = ScriptedPeer::new(vec![0, 1]);
    let (mesh, responder, shard, name) = requester(&script).await;

    let (pulled, complete) = pull(&mesh, &shard, &name, long_keys(4), Vec::new()).await;

    assert!(!complete);
    assert_eq!(script.calls(), 2, "the third batch is not asked for");
    assert!(pulled.is_empty());
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn a_success_between_two_failures_resets_the_count() {
    let script = ScriptedPeer::new(vec![0, 2]);
    let (mesh, responder, shard, name) = requester(&script).await;

    let (pulled, complete) = pull(&mesh, &shard, &name, long_keys(4), Vec::new()).await;

    assert!(!complete);
    assert_eq!(
        script.calls(),
        4,
        "failures that alternate with answers do not stop the round"
    );
    assert_eq!(pulled.len(), 2);
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn every_batch_answered_is_a_complete_pull() {
    let script = ScriptedPeer::new(Vec::new());
    let (mesh, responder, shard, name) = requester(&script).await;

    let (pulled, complete) = pull(&mesh, &shard, &name, long_keys(2), hash_batches(1)).await;

    assert!(complete);
    assert_eq!(pulled.len(), 3);
    mesh.shutdown().await;
    responder.shutdown().await;
}
