//! A round's pulls in batches: the byte cap beside the key cap, a failed
//! batch that does not end the rest, and a batch that times out going out
//! again smaller.

use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use proptest::prelude::*;

use super::*;
use crate::net::test_support::{peer_at, spawn_mesh};
use crate::net::{RequestHandler, with_request_timeout};
use crate::store::{BucketEntries, BucketLen, BucketPartDigests, KeyVersion, Mode, PartEntries};

fn key_of(len: usize, fill: u8) -> Bytes {
    Bytes::from(vec![fill; len])
}

/// Cuts `keys` into the batches [`pull_batch_len`] gives under fixed caps, in
/// order.
fn pull_batches(keys: &[Bytes], max_keys: usize, max_bytes: usize) -> Vec<&[Bytes]> {
    let mut batches = Vec::new();
    let mut rest = keys;
    while !rest.is_empty() {
        let (batch, tail) = rest.split_at(pull_batch_len(rest, max_keys, max_bytes));
        batches.push(batch);
        rest = tail;
    }
    batches
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
async fn requester<H: RequestHandler>(script: &Arc<H>) -> (Mesh, Mesh, Arc<dyn ShardOps>, SmolStr) {
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

#[test]
fn a_batch_is_cut_where_the_next_key_would_pass_a_cap() {
    let keys: Vec<Bytes> = (0..7).map(|n| key_of(10, n)).collect();

    assert_eq!(pull_batch_len(&[], 4, 100), 0, "no keys, no batch");
    assert_eq!(pull_batch_len(&keys, 100, 1_000), 7, "everything fits");
    assert_eq!(pull_batch_len(&keys, 3, 1_000), 3, "the count caps it");
    assert_eq!(pull_batch_len(&keys, 100, 25), 2, "the key bytes cap it");
    assert_eq!(
        pull_batch_len(&keys, 100, 30),
        3,
        "the bytes may fill the cap"
    );
    assert_eq!(pull_batch_len(&keys[4..], 100, 1_000), 3, "from any start");
    assert_eq!(
        pull_batch_len(&keys, 1, 1_000),
        1,
        "a limit of one key is one key"
    );
    assert_eq!(
        pull_batch_len(&[key_of(500, 1), key_of(5, 2)], 100, 20),
        1,
        "a key over the byte cap is a batch of its own"
    );
}

#[test]
fn a_timeout_halves_the_limit_down_to_one() {
    let timed_out = PullOutcome::TimedOut;
    assert_eq!(next_pull_limit(REPAIR_BATCH, timed_out), REPAIR_BATCH / 2);
    assert_eq!(next_pull_limit(100, timed_out), 50);
    assert_eq!(next_pull_limit(5, timed_out), 2);
    assert_eq!(next_pull_limit(3, timed_out), 1);
    assert_eq!(next_pull_limit(2, timed_out), 1);
    assert_eq!(next_pull_limit(1, timed_out), 1, "never below one");
}

#[test]
fn a_quick_answer_doubles_the_limit_up_to_the_largest_batch() {
    let quick = PullOutcome::Answered { quick: true };
    assert_eq!(next_pull_limit(1, quick), 2);
    assert_eq!(next_pull_limit(100, quick), 200);
    assert_eq!(next_pull_limit(REPAIR_BATCH / 2, quick), REPAIR_BATCH);
    assert_eq!(next_pull_limit(3_000, quick), REPAIR_BATCH, "capped");
    assert_eq!(next_pull_limit(REPAIR_BATCH, quick), REPAIR_BATCH);
    assert_eq!(next_pull_limit(usize::MAX, quick), REPAIR_BATCH);
}

#[test]
fn a_slower_answer_and_any_other_failure_keep_the_limit() {
    for limit in [1, 2, 100, REPAIR_BATCH] {
        assert_eq!(
            next_pull_limit(limit, PullOutcome::Answered { quick: false }),
            limit
        );
        assert_eq!(next_pull_limit(limit, PullOutcome::Failed), limit);
    }
}

#[test]
fn only_an_io_timeout_is_a_timed_out_pull() {
    let io = |kind| CodecError::Io(io::Error::new(kind, "pull"));

    assert!(pull_timed_out(&io(io::ErrorKind::TimedOut)));
    for kind in [
        io::ErrorKind::UnexpectedEof,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::NotFound,
        io::ErrorKind::Unsupported,
        io::ErrorKind::WouldBlock,
    ] {
        assert!(!pull_timed_out(&io(kind)), "{kind:?}");
    }
    assert!(!pull_timed_out(&CodecError::FrameTooLarge {
        size: 2,
        limit: 1
    }));
    assert!(!pull_timed_out(&CodecError::MalformedFrame("short")));
}

#[test]
fn an_exchange_is_quick_up_to_a_quarter_of_the_timeout() {
    let timeout = Duration::from_secs(10);

    assert!(pull_was_quick(Duration::ZERO, timeout));
    assert!(pull_was_quick(Duration::from_secs(2), timeout));
    assert!(pull_was_quick(Duration::from_millis(2_500), timeout));
    assert!(!pull_was_quick(Duration::from_millis(2_501), timeout));
    assert!(!pull_was_quick(timeout, timeout));
}

#[test]
fn a_reply_and_its_time_decide_the_outcome() {
    let timeout = Duration::from_secs(8);
    let answered: Result<(), CodecError> = Ok(());
    let timed_out: Result<(), CodecError> = Err(CodecError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        "anti-entropy pull did not complete",
    )));
    let failed: Result<(), CodecError> = Err(CodecError::Io(io::Error::from(
        io::ErrorKind::UnexpectedEof,
    )));

    assert_eq!(
        pull_outcome(&answered, Duration::from_secs(1), timeout),
        PullOutcome::Answered { quick: true }
    );
    assert_eq!(
        pull_outcome(&answered, Duration::from_secs(3), timeout),
        PullOutcome::Answered { quick: false }
    );
    assert_eq!(
        pull_outcome(&timed_out, Duration::from_secs(8), timeout),
        PullOutcome::TimedOut
    );
    assert_eq!(
        pull_outcome(&timed_out, Duration::ZERO, timeout),
        PullOutcome::TimedOut,
        "the error decides, not the time"
    );
    assert_eq!(
        pull_outcome(&failed, Duration::from_secs(1), timeout),
        PullOutcome::Failed
    );
}

#[test]
fn a_timed_out_batch_of_several_stays_and_halves_from_its_own_size() {
    let step = |limit, sent, outcome| pull_step(limit, sent, outcome);
    let timed_out = PullOutcome::TimedOut;

    assert_eq!(
        step(8, 8, timed_out),
        PullStep {
            limit: 4,
            consumed: false
        },
        "a full batch halves the limit"
    );
    assert_eq!(
        step(REPAIR_BATCH, 3, timed_out),
        PullStep {
            limit: 1,
            consumed: false
        },
        "a short batch halves its own size, not the limit above it"
    );
    assert_eq!(
        step(REPAIR_BATCH, 2, timed_out),
        PullStep {
            limit: 1,
            consumed: false
        }
    );
}

#[test]
fn a_timed_out_batch_of_one_is_consumed() {
    assert_eq!(
        pull_step(1, 1, PullOutcome::TimedOut),
        PullStep {
            limit: 1,
            consumed: true
        }
    );
    assert_eq!(
        pull_step(8, 1, PullOutcome::TimedOut),
        PullStep {
            limit: 1,
            consumed: true
        },
        "a key the batch cut short of the limit is one that timed out alone"
    );
}

#[test]
fn every_batch_that_ends_any_other_way_is_consumed() {
    let consumed = |limit| PullStep {
        limit,
        consumed: true,
    };

    assert_eq!(pull_step(8, 8, PullOutcome::Failed), consumed(8));
    assert_eq!(pull_step(8, 1, PullOutcome::Failed), consumed(8));
    assert_eq!(
        pull_step(8, 8, PullOutcome::Answered { quick: false }),
        consumed(8)
    );
    assert_eq!(
        pull_step(8, 3, PullOutcome::Answered { quick: true }),
        consumed(16)
    );
    assert_eq!(
        pull_step(REPAIR_BATCH, 3, PullOutcome::Answered { quick: true }),
        consumed(REPAIR_BATCH)
    );
}

/// The request timeout of the slow-peer tests: short enough that a timeout is
/// quick to wait out, long enough that a loopback answer never reaches it.
const SHORT_TIMEOUT: Duration = Duration::from_millis(500);

/// The request timeout of the tests where the answer after a timed-out key
/// has to be quick, at most a quarter of it, or the batch does not grow: a
/// loaded runner has 500 ms for a fresh dial and the answer, not 125 ms.
const ROOMY_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a slow answer waits: past every timeout above, and dropped with
/// the test's runtime, never waited for.
const SLOW_ANSWER: Duration = Duration::from_secs(5);

/// The key `n` is stored under in the pulled shard.
fn numbered_key(n: u64) -> Bytes {
    Bytes::from(postcard::to_stdvec(&u32::try_from(n).expect("a small key")).expect("encodes"))
}

fn numbered_keys(range: std::ops::Range<u64>) -> Vec<Bytes> {
    range.map(numbered_key).collect()
}

/// A small record under `key`.
fn small_record(key: Bytes) -> WireRecord {
    WireRecord {
        key,
        value: Some(Bytes::from(postcard::to_stdvec("pulled").expect("encodes"))),
        ver: scripted_ver(),
        expires_at_ms: None,
    }
}

/// A peer that answers every pull with one small record for each entry asked
/// for. A request waits `delay` first when it asks for more entries than
/// `threshold`, or when it is a call named in `stalls`, counting from 0,
/// whatever its size.
struct SlowPeer {
    threshold: usize,
    stalls: Vec<usize>,
    delay: Duration,
    calls: AtomicUsize,
    requested: Mutex<Vec<usize>>,
}

impl SlowPeer {
    fn new(threshold: usize, stalls: Vec<usize>, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            threshold,
            stalls,
            delay,
            calls: AtomicUsize::new(0),
            requested: Mutex::new(Vec::new()),
        })
    }

    /// Slow for any request over `threshold` entries.
    fn over(threshold: usize) -> Arc<Self> {
        Self::new(threshold, Vec::new(), SLOW_ANSWER)
    }

    /// Slow for none.
    fn never() -> Arc<Self> {
        Self::over(usize::MAX)
    }

    /// Answers `records` after the wait the request earns.
    fn answer(&self, records: Vec<WireRecord>) -> BoxFuture<'_, Vec<WireRecord>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requested.lock().expect("lock").push(records.len());
        let slow = records.len() > self.threshold || self.stalls.contains(&call);
        let delay = self.delay;
        Box::pin(async move {
            if slow {
                tokio::time::sleep(delay).await;
            }
            records
        })
    }

    /// The number of entries each request asked for, in order.
    fn requested(&self) -> Vec<usize> {
        self.requested.lock().expect("lock").clone()
    }
}

impl RequestHandler for SlowPeer {
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
        self.answer(keys.into_iter().map(small_record).collect())
    }

    fn records_for_hashes(
        &self,
        _cache: SmolStr,
        _bucket: u16,
        hashes: Vec<u64>,
    ) -> BoxFuture<'_, Vec<WireRecord>> {
        self.answer(
            hashes
                .into_iter()
                .map(|hash| small_record(numbered_key(hash)))
                .collect(),
        )
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

/// How many of `keys` the shard holds.
async fn held(shard: &Arc<dyn ShardOps>, keys: &[Bytes]) -> usize {
    shard.records_for(keys.to_vec()).await.len()
}

/// [`pull`] under a request timeout of `timeout`.
async fn pull_within(
    timeout: Duration,
    mesh: &Mesh,
    shard: &Arc<dyn ShardOps>,
    name: &SmolStr,
    keys: Vec<Bytes>,
    hashes: Vec<(u16, Vec<u64>)>,
) -> (HashMap<Bytes, Hlc>, bool) {
    with_request_timeout(timeout, pull(mesh, shard, name, keys, hashes)).await
}

#[tokio::test]
async fn a_key_pull_that_times_out_goes_out_again_in_halves() {
    let peer = SlowPeer::over(8);
    let (mesh, responder, shard, name) = requester(&peer).await;
    let keys = numbered_keys(0..16);

    let (pulled, complete) = pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        keys.clone(),
        Vec::new(),
    )
    .await;

    assert_eq!(
        peer.requested(),
        [16, 8, 8],
        "the sixteen time out, then the same keys go in halves"
    );
    assert_eq!(pulled.len(), 16, "every key landed in the one call");
    assert_eq!(held(&shard, &keys).await, 16);
    assert!(!complete, "the first request failed");
    assert!(
        mesh.pull_limit(NodeId::from(2), &name)
            .is_some_and(|limit| limit < REPAIR_BATCH),
        "the shrunk limit is kept for the peer"
    );
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn a_hash_pull_that_times_out_goes_out_again_in_halves() {
    let peer = SlowPeer::over(8);
    let (mesh, responder, shard, name) = requester(&peer).await;
    let hashes: Vec<u64> = (0..16).collect();

    let (pulled, complete) = pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        Vec::new(),
        vec![(0, hashes.clone())],
    )
    .await;

    assert_eq!(peer.requested(), [16, 8, 8]);
    assert_eq!(pulled.len(), 16);
    assert_eq!(held(&shard, &numbered_keys(0..16)).await, 16);
    assert!(!complete);
    assert!(mesh.pull_limit(NodeId::from(2), &name).is_some());
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn a_later_pull_starts_at_the_limit_the_last_one_shrank_to() {
    let peer = SlowPeer::over(3);
    let (mesh, responder, shard, name) = requester(&peer).await;

    let (_, complete) = pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        numbered_keys(0..8),
        Vec::new(),
    )
    .await;
    assert!(!complete);
    assert_eq!(peer.requested(), [8, 4], "both time out and end the pulls");
    assert_eq!(
        mesh.pull_limit(NodeId::from(2), &name),
        Some(2),
        "the second timeout halved four"
    );

    let later = numbered_keys(100..104);
    let (pulled, complete) = pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        later.clone(),
        Vec::new(),
    )
    .await;

    assert_eq!(
        peer.requested(),
        [8, 4, 2, 2],
        "the second call's first request is the shrunk limit, not four thousand"
    );
    assert!(complete, "both of its requests were answered");
    assert_eq!(pulled.len(), 4);
    assert_eq!(held(&shard, &later).await, 4);
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn quick_answers_double_the_limit_back_toward_the_largest_batch() {
    let peer = SlowPeer::never();
    let (mesh, responder, shard, name) = requester(&peer).await;
    mesh.set_pull_limit(NodeId::from(2), &name, 4);

    let (pulled, complete) = pull_within(
        Duration::from_secs(20),
        &mesh,
        &shard,
        &name,
        numbered_keys(0..100),
        Vec::new(),
    )
    .await;

    assert_eq!(
        peer.requested(),
        [4, 8, 16, 32, 40],
        "each quick answer doubles the next batch"
    );
    assert!(complete);
    assert_eq!(pulled.len(), 100);
    assert_eq!(mesh.pull_limit(NodeId::from(2), &name), Some(128));
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn the_limit_grows_no_further_than_the_largest_batch() {
    let peer = SlowPeer::never();
    let (mesh, responder, shard, name) = requester(&peer).await;
    mesh.set_pull_limit(NodeId::from(2), &name, REPAIR_BATCH - 1);

    let (pulled, complete) = pull_within(
        Duration::from_secs(20),
        &mesh,
        &shard,
        &name,
        numbered_keys(0..REPAIR_BATCH as u64),
        Vec::new(),
    )
    .await;

    assert_eq!(peer.requested(), [REPAIR_BATCH - 1, 1]);
    assert!(complete);
    assert_eq!(pulled.len(), REPAIR_BATCH);
    assert_eq!(mesh.pull_limit(NodeId::from(2), &name), Some(REPAIR_BATCH));
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn an_answer_slower_than_a_quarter_of_the_timeout_keeps_the_limit() {
    // Every request waits half a second: past a quarter of the one-second
    // timeout, well inside it.
    let peer = SlowPeer::new(0, Vec::new(), Duration::from_millis(500));
    let (mesh, responder, shard, name) = requester(&peer).await;
    mesh.set_pull_limit(NodeId::from(2), &name, 4);

    let (pulled, complete) = pull_within(
        Duration::from_secs(1),
        &mesh,
        &shard,
        &name,
        numbered_keys(0..10),
        Vec::new(),
    )
    .await;

    assert_eq!(peer.requested(), [4, 4, 2], "the batch never grows");
    assert!(complete, "slow is not failed");
    assert_eq!(pulled.len(), 10);
    assert_eq!(mesh.pull_limit(NodeId::from(2), &name), Some(4));
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn a_key_that_times_out_alone_is_skipped_and_the_keys_after_it_land() {
    let peer = SlowPeer::new(usize::MAX, vec![0], SLOW_ANSWER);
    let (mesh, responder, shard, name) = requester(&peer).await;
    mesh.set_pull_limit(NodeId::from(2), &name, 1);
    let keys = numbered_keys(0..4);

    let (pulled, complete) = pull_within(
        ROOMY_TIMEOUT,
        &mesh,
        &shard,
        &name,
        keys.clone(),
        Vec::new(),
    )
    .await;

    assert_eq!(
        peer.requested(),
        [1, 1, 2],
        "the first key alone, and it is not asked for again"
    );
    assert!(!complete);
    assert_eq!(pulled.len(), 3);
    assert_eq!(held(&shard, &keys[..1]).await, 0, "the first key waits");
    assert_eq!(held(&shard, &keys[1..]).await, 3, "the rest landed");
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn a_hash_that_times_out_alone_is_skipped_and_the_hashes_after_it_land() {
    let peer = SlowPeer::new(usize::MAX, vec![0], SLOW_ANSWER);
    let (mesh, responder, shard, name) = requester(&peer).await;
    mesh.set_pull_limit(NodeId::from(2), &name, 1);

    let (pulled, complete) = pull_within(
        ROOMY_TIMEOUT,
        &mesh,
        &shard,
        &name,
        Vec::new(),
        vec![(0, (0..4).collect())],
    )
    .await;

    assert_eq!(peer.requested(), [1, 1, 2]);
    assert!(!complete);
    assert_eq!(pulled.len(), 3);
    let keys = numbered_keys(0..4);
    assert_eq!(held(&shard, &keys[..1]).await, 0);
    assert_eq!(held(&shard, &keys[1..]).await, 3);
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn a_peer_that_never_answers_in_time_costs_two_requests() {
    let peer = SlowPeer::over(0);
    let (mesh, responder, shard, name) = requester(&peer).await;

    let (pulled, complete) = pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        numbered_keys(0..8),
        vec![(3, (0..8).collect())],
    )
    .await;

    assert_eq!(
        peer.requested(),
        [8, 4],
        "the eight, then the same keys in four, and no request after the second timeout"
    );
    assert!(!complete);
    assert!(pulled.is_empty());
    assert_eq!(mesh.pull_limit(NodeId::from(2), &name), Some(2));
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn the_limit_is_kept_per_cache_for_the_peer() {
    let peer = SlowPeer::over(0);
    let (mesh, responder, shard, name) = requester(&peer).await;

    pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        numbered_keys(0..8),
        Vec::new(),
    )
    .await;

    assert_eq!(mesh.pull_limit(NodeId::from(2), &name), Some(2));
    assert_eq!(
        mesh.pull_limit(NodeId::from(2), "another cache"),
        None,
        "a cache whose pulls never timed out starts at the largest batch"
    );
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn a_pull_that_fails_without_a_timeout_keeps_the_limit() {
    let script = ScriptedPeer::new(vec![0]);
    let (mesh, responder, shard, name) = requester(&script).await;

    let (pulled, complete) = pull(&mesh, &shard, &name, long_keys(3), Vec::new()).await;

    assert!(!complete);
    assert_eq!(
        *script.requested.lock().expect("lock"),
        [1, 1, 1],
        "the failed batch is consumed, not asked for again"
    );
    assert_eq!(pulled.len(), 2);
    assert_eq!(
        mesh.pull_limit(NodeId::from(2), &name),
        None,
        "nothing shrank the batch"
    );
    mesh.shutdown().await;
    responder.shutdown().await;
}

#[tokio::test]
async fn both_orders_of_a_round_pull_under_the_limit_kept_for_the_peer() {
    for both_ways in [false, true] {
        let peer = SlowPeer::never();
        let (mesh, responder, shard, name) = requester(&peer).await;
        mesh.set_pull_limit(NodeId::from(2), &name, 2);

        let repairs = with_request_timeout(
            Duration::from_secs(20),
            apply_repairs(
                &mesh,
                &shard,
                &name,
                NodeId::from(2),
                Vec::new(),
                numbered_keys(0..6),
                Vec::new(),
                both_ways,
            ),
        )
        .await;

        assert!(repairs.complete, "both_ways: {both_ways}");
        assert_eq!(peer.requested(), [2, 4], "both_ways: {both_ways}");
        mesh.shutdown().await;
        responder.shutdown().await;
    }
}

/// A tracing layer that keeps the level and the fields of every event.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<(tracing::Level, String)>>>);

/// Collects the fields of one event as `name=value` words.
struct Fields(String);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        write!(self.0, "{}={value:?}", field.name()).expect("a string takes the write");
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Logs {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0
            .lock()
            .expect("lock")
            .push((*event.metadata().level(), fields.0));
    }
}

impl Logs {
    /// The events of `level` that carry `text`.
    fn matching(&self, level: tracing::Level, text: &str) -> Vec<String> {
        self.0
            .lock()
            .expect("lock")
            .iter()
            .filter(|(seen, fields)| *seen == level && fields.contains(text))
            .map(|(_, fields)| fields.clone())
            .collect()
    }
}

#[tokio::test]
async fn a_shrink_logs_at_debug_and_the_limit_reaching_one_warns_once() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let logs = Logs::default();
    let _log = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(logs.clone()),
    );
    let peer = SlowPeer::over(0);
    let (mesh, responder, shard, name) = requester(&peer).await;
    mesh.set_pull_limit(NodeId::from(2), &name, 4);

    pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        numbered_keys(0..4),
        Vec::new(),
    )
    .await;

    assert_eq!(peer.requested(), [4, 2]);
    let shrinks = logs.matching(tracing::Level::DEBUG, "shrinking the batch");
    assert_eq!(shrinks.len(), 2, "four to two, two to one: {shrinks:?}");
    assert!(shrinks[0].contains("limit=2"), "{shrinks:?}");
    assert!(shrinks[1].contains("limit=1"), "{shrinks:?}");
    assert!(
        shrinks[1].contains(&format!("peer={}", NodeId::from(2))),
        "{shrinks:?}"
    );
    assert!(shrinks[1].contains("cache=pulled"), "{shrinks:?}");
    let warnings = logs.matching(tracing::Level::WARN, "one key at a time");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains(&format!("peer={}", NodeId::from(2))),
        "{warnings:?}"
    );
    assert!(warnings[0].contains("cache=pulled"), "{warnings:?}");

    pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        numbered_keys(10..11),
        Vec::new(),
    )
    .await;

    assert_eq!(
        logs.matching(tracing::Level::WARN, "one key at a time")
            .len(),
        1,
        "a limit already at one warns no more"
    );
    assert_eq!(
        logs.matching(tracing::Level::DEBUG, "shrinking the batch")
            .len(),
        2
    );

    mesh.set_pull_limit(NodeId::from(2), &name, 4);
    pull_within(
        SHORT_TIMEOUT,
        &mesh,
        &shard,
        &name,
        numbered_keys(20..24),
        Vec::new(),
    )
    .await;

    assert_eq!(peer.requested(), [4, 2, 1, 4, 2]);
    assert_eq!(mesh.pull_limit(NodeId::from(2), &name), Some(1));
    assert_eq!(
        logs.matching(tracing::Level::DEBUG, "shrinking the batch")
            .len(),
        4,
        "a limit that grew back shrinks to one again, and each shrink logs"
    );
    assert_eq!(
        logs.matching(tracing::Level::WARN, "one key at a time")
            .len(),
        1,
        "the warning comes the first time the limit reaches one for the peer and cache"
    );
    mesh.shutdown().await;
    responder.shutdown().await;
}
