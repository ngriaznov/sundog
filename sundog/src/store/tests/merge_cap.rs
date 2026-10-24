//! A merged value stays within one frame: a merge that would push a value
//! past the shard's `max_frame` is refused and changes nothing, however it
//! reaches the shard, and a record too large to ship stays out of a sync.

use std::sync::{Arc, Mutex};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
#[cfg(not(feature = "sim"))]
use tracing_subscriber::layer::SubscriberExt as _;

use super::*;
#[cfg(not(feature = "sim"))]
use crate::cluster::test_support::{ScopedSubscriber, scoped_subscriber};
use crate::store::crdt::{OrSet, OrSetResolver, WriterId};
use crate::store::shard_oversize::{
    OVERSIZE_WARN_SPACING_MS, first_refusal_in_call_order, key_for_log, oversize_warn_due,
    split_unframeable,
};
use crate::wire;

const WINDOW: Duration = Duration::from_secs(60);

/// A resolver over byte vectors whose merge appends the incoming vector to
/// the stored one, and clears the value when the incoming one is empty: a
/// merge that can grow a value, leave it as it is, and shrink it.
#[derive(Debug, Clone, Copy, Default)]
struct ConcatResolver;

impl ConflictResolver for ConcatResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let mut left: Vec<u8> = postcard::from_bytes(a.value?).ok()?;
        let right: Vec<u8> = postcard::from_bytes(b.value?).ok()?;
        if right.is_empty() {
            left.clear();
        } else {
            left.extend(right);
        }
        Some(Merged {
            value: Bytes::from(postcard::to_stdvec(&left).ok()?),
            expires_at_ms: later_deadline(a.expires_at_ms, b.expires_at_ms),
        })
    }
}

/// The frame cap that admits a value of `value_bytes` encoded bytes under a
/// one-byte key in the cache `shard` names, `test`.
fn cap_for(value_bytes: usize) -> usize {
    wire::forward_frame_len("test".len(), 1, value_bytes)
}

/// A shard whose values stay within `value_bytes` encoded bytes. A vector of
/// `n` bytes under 128 encodes to `n + 1`.
fn capped_shard(value_bytes: usize) -> Shard<u32, Vec<u8>> {
    shard::<u32, Vec<u8>>(1)
        .with_resolver(Arc::new(ConcatResolver))
        .with_max_frame(cap_for(value_bytes))
}

/// Four bytes: five encoded, so two of them merge to nine, past a cap of
/// eight.
fn four(byte: u8) -> Vec<u8> {
    vec![byte; 4]
}

/// A record for key 1 carrying `value` at `ver`.
fn bytes_record(value: &[u8], ver: Hlc) -> WireRecord {
    WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(
            postcard::to_stdvec(&value.to_vec()).expect("test value encodes"),
        )),
        ver,
        expires_at_ms: None,
    }
}

/// The record stored for key 1.
async fn stored_record(s: &Shard<u32, Vec<u8>>) -> Option<WireRecord> {
    ShardOps::records_for(s, vec![key_bytes(&1u32)])
        .await
        .into_iter()
        .next()
}

/// Reads the counters a recorder holds. A snapshot resets the counters it
/// reads, so this keeps running totals.
struct Counted {
    snapshotter: Snapshotter,
    totals: Mutex<HashMap<String, u64>>,
}

impl Counted {
    fn new(recorder: &DebuggingRecorder) -> Self {
        Self {
            snapshotter: recorder.snapshotter(),
            totals: Mutex::new(HashMap::new()),
        }
    }

    /// The total of `name{cache="test", reason}` so far, 0 while the series
    /// does not exist.
    fn total(&self, name: &str, reason: &str) -> u64 {
        let mut totals = self.totals.lock().expect("lock");
        for (key, _, _, value) in self.snapshotter.snapshot().into_vec() {
            let labelled = |label: &str, expected: &str| {
                key.key()
                    .labels()
                    .any(|l| l.key() == label && l.value() == expected)
            };
            if key.key().name() != name || !labelled("cache", "test") || !labelled("reason", reason)
            {
                continue;
            }
            let DebugValue::Counter(n) = value else {
                panic!("a counter, got {value:?}");
            };
            *totals.entry(name.to_owned()).or_default() += n;
        }
        totals.get(name).copied().unwrap_or(0)
    }

    fn refused(&self) -> u64 {
        self.total("sundog_merge_refused_total", "too_large")
    }

    fn skipped(&self) -> u64 {
        self.total("sundog_sync_skipped_total", "too_large")
    }
}

/// A tracing layer that keeps the message and fields of every `WARN` event.
/// A `sim` build has no cluster test support to install it with, so it
/// captures nothing and its tests skip the assertions on the log.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

#[cfg(feature = "sim")]
type ScopedSubscriber = ();

struct Message(String);

impl tracing::field::Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        } else {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warnings {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0.lock().expect("lock").push(message.0);
    }
}

impl Warnings {
    /// Whether this build installs the layer.
    const CAPTURES: bool = cfg!(not(feature = "sim"));

    fn capture() -> (Self, ScopedSubscriber) {
        let warnings = Self::default();
        #[cfg(not(feature = "sim"))]
        let guard = scoped_subscriber(tracing_subscriber::registry().with(warnings.clone()));
        #[cfg(feature = "sim")]
        let guard = ();
        (warnings, guard)
    }

    fn logged(&self) -> Vec<String> {
        self.0.lock().expect("lock").clone()
    }

    /// Asserts that exactly one warning was logged and that it holds each of
    /// `parts`, its message or a field as `name="value"`. A build that
    /// captures nothing skips the check.
    fn assert_one(&self, parts: &[&str]) {
        if !Self::CAPTURES {
            return;
        }
        let logged = self.logged();
        assert_eq!(
            logged.len(),
            1,
            "one warning inside the spacing: {logged:?}"
        );
        for part in parts {
            assert!(logged[0].contains(part), "{part} is in {logged:?}");
        }
    }
}

fn too_large(result: Result<(), CacheError>) -> (usize, usize) {
    match result {
        Err(CacheError::ValueTooLarge { size, limit, .. }) => (size, limit),
        other => panic!("expected ValueTooLarge, got {other:?}"),
    }
}

#[test]
fn a_warning_is_due_at_first_and_then_once_per_spacing() {
    assert!(oversize_warn_due(None, 0));
    assert!(!oversize_warn_due(Some(1_000), 1_000), "the same instant");
    assert!(!oversize_warn_due(
        Some(1_000),
        1_000 + OVERSIZE_WARN_SPACING_MS - 1
    ));
    assert!(oversize_warn_due(
        Some(1_000),
        1_000 + OVERSIZE_WARN_SPACING_MS
    ));
    assert!(
        oversize_warn_due(Some(5_000), 4_999),
        "the clock stepped back"
    );
}

#[test]
fn a_key_shows_in_a_log_as_the_hash_and_length_of_its_encoded_bytes() {
    // The xxh3 of the empty input and of the single byte 0x01.
    assert_eq!(key_for_log(&[]), ("2d06800538d394c2".to_owned(), 0));
    let (hash, len) = key_for_log(&[0x01]);
    assert_eq!(hash, format!("{:016x}", engine::hash_key_bytes(&[0x01])));
    assert_eq!(len, 1);
    assert_eq!(hash.len(), 16, "always sixteen digits");

    let long = [0xab; 1_000];
    let (long_hash, long_len) = key_for_log(&long);
    assert_eq!(long_len, 1_000, "the length is the whole key's");
    assert_eq!(long_hash.len(), 16, "and the hash stays as short");
    assert_ne!(
        long_hash,
        key_for_log(&long[..999]).0,
        "it covers every byte"
    );
}

#[test]
fn a_batch_reports_the_refusal_of_the_entry_given_first_whatever_stripe_applies_first() {
    // The stripe of each entry in the order given, each refusing stripe's
    // first refused entry as (stripe, index in its group, size), and the size
    // reported.
    type Case<'a> = (&'a [usize], &'a [(usize, usize, usize)], Option<usize>);
    let cases: &[Case<'_>] = &[
        (&[], &[], None),
        (&[4, 9, 4], &[], None),
        (&[5, 3, 5], &[(5, 1, 40)], Some(40)),
        // Stripe 2 applies before stripe 7; stripe 7's entry was given first.
        (&[7, 2], &[(2, 0, 10), (7, 0, 20)], Some(20)),
        (&[7, 2, 7, 2], &[(2, 0, 10), (7, 1, 20)], Some(10)),
        // The second entry of stripe 3 comes before the only one of stripe 8.
        (&[3, 3, 8], &[(3, 1, 30), (8, 0, 50)], Some(30)),
    ];
    for (stripe_of, first_refused, want) in cases {
        assert_eq!(
            first_refusal_in_call_order(stripe_of, first_refused),
            *want,
            "stripes {stripe_of:?}, refusals {first_refused:?}"
        );
    }
}

/// The two fields a log line holds for key `key` under `name`, as
/// `Warnings::assert_one` reads them: `key` for a refused merge,
/// `largest_key` for a sync skip.
fn key_fields(name: &str, key: u32) -> [String; 2] {
    let bytes = key_bytes(&key);
    [
        format!(
            "{name}_hash=\"{:016x}\"",
            engine::hash_key_bytes(bytes.as_ref())
        ),
        format!("{name}_len={}", bytes.len()),
    ]
}

#[test]
fn a_record_fits_a_frame_up_to_max_frame_and_not_one_byte_past_it() {
    let record = |key_len: usize, value_len: usize| WireRecord {
        key: Bytes::from(vec![0u8; key_len]),
        value: Some(Bytes::from(vec![0u8; value_len])),
        ver: hlc(1, 1),
        expires_at_ms: None,
    };
    let cache_len = 4;
    let room = wire::MAX_FRAME - wire::replicate_frame_len(cache_len, 1, 0);

    let (kept, largest) = split_unframeable(
        cache_len,
        vec![
            record(1, 3),
            record(1, room),
            record(1, room + 1),
            record(2, room),
        ],
    );

    assert_eq!(
        kept.iter()
            .map(|r| (r.key.len(), r.value.as_ref().map_or(0, Bytes::len)))
            .collect::<Vec<_>>(),
        [(1, 3), (1, room)],
        "the lone record at the limit stays, in order; a longer key tips one over"
    );
    assert_eq!(
        largest,
        Some((wire::MAX_FRAME + 1, Bytes::from(vec![0u8; 1]))),
        "the frame length and key of the largest one left out, the first of two that tie"
    );
    let (all, none) = split_unframeable(cache_len, vec![record(1, 3)]);
    assert_eq!((all.len(), none), (1, None));
}

#[tokio::test]
async fn a_growing_merge_past_the_cap_fails_with_value_too_large_and_changes_nothing() {
    let s = capped_shard(8);
    let mut events = s.events();
    s.merge(1, four(1)).await.expect("five bytes fit");
    events
        .try_recv()
        .expect("the first merge published its event");
    let before = stored_record(&s).await.expect("stored");
    s.fan_out_queue().drain();

    let (size, limit) = too_large(s.merge(1, four(2)).await);

    assert_eq!(size, 9, "the merged vector would encode to nine bytes");
    assert_eq!(limit, cap_for(8), "against the shard's frame cap");
    assert_eq!(
        stored_record(&s).await,
        Some(before),
        "the stored record is whole: same version, same bytes"
    );
    assert!(events.try_recv().is_err(), "no event for a refused merge");
    assert!(
        s.fan_out_queue().drain().is_empty(),
        "nothing queued to replicate"
    );
    assert_digest_matches_full_recompute(&s).await;
}

/// A resolver over byte vectors whose merge keeps the longer side whole: a
/// merge whose result is one of its two inputs, the incoming one when that is
/// the longer.
#[derive(Debug, Clone, Copy, Default)]
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
        let longer = if b_bytes.len() > a_bytes.len() {
            b_bytes
        } else {
            a_bytes
        };
        Some(Merged {
            value: Bytes::copy_from_slice(longer),
            expires_at_ms: later_deadline(a.expires_at_ms, b.expires_at_ms),
        })
    }
}

/// The frame cap that admits a value of ten encoded bytes under a one-byte
/// key as a replicate frame: a forward of the same record is nine bytes wider
/// and does not fit it.
fn replicate_cap() -> usize {
    wire::replicate_frame_len("test".len(), 1, 10)
}

/// Nine bytes, which encode to ten.
fn widest() -> Vec<u8> {
    vec![7; 9]
}

#[tokio::test]
async fn every_merge_call_accepts_the_value_a_replicate_frame_holds_as_insert_does() {
    // The default resolver does not merge, and nothing in a `Local` or
    // `Replicated` cache forwards a record.
    let s = shard::<u32, Vec<u8>>(1).with_max_frame(replicate_cap());
    s.insert(1, widest()).await.expect("insert");
    s.insert_sync(2, widest()).expect("insert_sync");
    s.insert_many([(3, widest())]).await.expect("insert_many");

    s.merge(4, widest()).await.expect("merge");
    s.merge_with_ttl(5, widest(), Duration::from_secs(60))
        .await
        .expect("merge_with_ttl");
    let writer = WriterId::new(NodeId::from(1), 1);
    s.merge_sequenced(6, writer, |_, _| widest())
        .await
        .expect("merge_sequenced");
    s.merge_sequenced_with_ttl(7, writer, |_, _| widest(), Duration::from_secs(60))
        .await
        .expect("merge_sequenced_with_ttl");

    for key in 1..=7 {
        assert_eq!(s.get(&key).await, Some(widest()), "key {key}");
    }
    let one_more = vec![7u8; 10];
    too_large(s.insert(8, one_more.clone()).await);
    too_large(s.merge(8, one_more).await);
}

#[tokio::test]
async fn a_merging_resolver_that_keeps_the_contribution_stores_it_as_an_empty_key_would() {
    let s = shard::<u32, Vec<u8>>(1)
        .with_resolver(Arc::new(LongerResolver))
        .with_max_frame(replicate_cap());
    s.insert(1, vec![1]).await.expect("a small value");
    s.insert(2, vec![1]).await.expect("another");
    s.merge(3, vec![1]).await.expect("and another");

    s.insert(1, widest())
        .await
        .expect("insert over a smaller value stores the longer, as at an empty key");
    s.merge(2, widest())
        .await
        .expect("merge keeps the call's value");
    s.merge(3, widest()).await.expect("and so does the third");

    for key in 1..=3 {
        assert_eq!(s.get(&key).await, Some(widest()), "key {key}");
    }
}

#[tokio::test]
async fn a_windowed_fold_that_is_the_calls_own_value_meets_no_cap_beyond_the_one_on_the_call() {
    let s = shard::<u32, Vec<u8>>(1)
        .with_resolver(Arc::new(LongerResolver))
        .with_merge_coalesce_window(WINDOW)
        .with_max_frame(replicate_cap());

    s.merge(1, vec![1]).await.expect("opens the fold");
    s.merge(1, widest())
        .await
        .expect("the fold is the call's own value, which fits as a replicate");
    s.flush_all_pending_merges();

    assert_eq!(s.get(&1).await, Some(widest()));
}

#[tokio::test]
async fn every_local_merge_entry_point_refuses_the_same_growth() {
    let s = capped_shard(8);
    s.merge(1, four(1)).await.expect("fits");

    too_large(s.merge_with_ttl(1, four(2), Duration::from_secs(60)).await);
    too_large(
        s.merge_sequenced(1, WriterId::new(NodeId::from(1), 1), |_, _| four(3))
            .await,
    );
    too_large(
        s.merge_sequenced_with_ttl(
            1,
            WriterId::new(NodeId::from(1), 1),
            |_, _| four(4),
            Duration::from_secs(60),
        )
        .await,
    );

    assert_eq!(s.get(&1).await, Some(four(1)), "nothing grew");
}

#[tokio::test]
async fn a_merge_sequenced_call_that_is_refused_spends_its_number() {
    let s = capped_shard(8);
    let writer = WriterId::new(NodeId::from(1), 1);
    let handed = Mutex::new(Vec::new());
    let call = |value: Vec<u8>| {
        let handed = &handed;
        async {
            s.merge_sequenced(1, writer, |_, seq| {
                handed.lock().expect("lock").push(seq);
                value
            })
            .await
        }
    };

    call(four(1)).await.expect("fits");
    too_large(call(four(2)).await);
    call(Vec::new())
        .await
        .expect("a clearing merge always fits");

    assert_eq!(
        *handed.lock().expect("lock"),
        [1, 2, 3],
        "the refused call drew number 2 and the next call gets 3"
    );
    assert_eq!(s.get(&1).await, Some(Vec::new()));
}

#[tokio::test]
async fn a_merge_that_shrinks_or_keeps_an_oversized_value_succeeds_and_one_that_grows_it_does_not()
{
    let s = capped_shard(8);
    // Stored before the cap applied: twenty bytes, twelve past it. A peer's
    // record is how a shard comes to hold it.
    ShardOps::apply_remote(&s, bytes_record(&[7u8; 20], hlc(ahead_ms(), 2))).await;
    assert_eq!(
        s.get(&1).await,
        Some(vec![7u8; 20]),
        "it keeps serving reads"
    );

    let (size, _) = too_large(s.merge(1, vec![1]).await);
    assert_eq!(size, 22, "one more byte, and its length byte, is refused");
    assert_eq!(s.get(&1).await, Some(vec![7u8; 20]));

    s.merge(1, Vec::new()).await.expect("a clear shrinks it");
    assert_eq!(s.get(&1).await, Some(Vec::new()));
    s.merge(1, four(1)).await.expect("and it fits again");
    assert_eq!(s.get(&1).await, Some(four(1)));
}

#[tokio::test]
async fn an_insert_that_merges_into_a_stored_value_meets_the_cap_and_so_does_insert_many() {
    let s = capped_shard(8);
    s.insert(1, four(1)).await.expect("the first insert stores");
    s.insert(3, four(3)).await.expect("so does another key");

    too_large(s.insert(1, four(2)).await);
    assert_eq!(s.get(&1).await, Some(four(1)));

    let result = s.insert_many([(1, four(2)), (2, four(5))]).await;
    too_large(result);
    assert_eq!(
        s.get(&1).await,
        Some(four(1)),
        "the refused entry changed nothing"
    );
    assert_eq!(
        s.get(&2).await,
        Some(four(5)),
        "the entries around it applied, as a batch that is not a transaction does"
    );
}

#[tokio::test]
async fn the_default_cap_is_one_wire_frame() {
    let s = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(ConcatResolver));
    let three_mib = vec![1u8; 3 * 1024 * 1024];
    s.merge(1, three_mib.clone()).await.expect("fits a frame");

    let (_, limit) = too_large(s.merge(1, three_mib).await);

    assert_eq!(limit, wire::MAX_FRAME);
    assert_eq!(s.get(&1).await.map(|v| v.len()), Some(3 * 1024 * 1024));
}

#[tokio::test]
async fn the_cap_survives_the_builders_that_rebuild_the_engine() {
    for capped_last in [false, true] {
        let base = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(ConcatResolver));
        let s = if capped_last {
            base.with_capacity_hint(16).with_max_frame(cap_for(8))
        } else {
            base.with_max_frame(cap_for(8)).with_capacity_hint(16)
        };
        s.merge(1, four(1)).await.expect("fits");

        too_large(s.merge(1, four(2)).await);
    }
    let weighed = shard::<u32, Vec<u8>>(1)
        .with_resolver(Arc::new(ConcatResolver))
        .with_max_frame(cap_for(8))
        .with_weigher(|_, v: &Vec<u8>| u32::try_from(v.len()).unwrap_or(u32::MAX));
    weighed.merge(1, four(1)).await.expect("fits");
    too_large(weighed.merge(1, four(2)).await);
}

#[tokio::test]
async fn a_remote_merge_past_the_cap_is_refused_counted_and_logged_once_per_spacing() {
    let recorder = DebuggingRecorder::new();
    let counted = Counted::new(&recorder);
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (warnings, _log) = Warnings::capture();
    let s = capped_shard(8);
    ShardOps::apply_remote(&s, bytes_record(&four(1), hlc(ahead_ms(), 2))).await;
    let before = stored_record(&s).await.expect("stored");
    s.fan_out_queue().drain();
    let mut events = s.events();

    ShardOps::apply_remote(&s, bytes_record(&four(2), hlc(ahead_ms() + 1, 3))).await;
    ShardOps::apply_remote_batch(&s, vec![bytes_record(&four(3), hlc(ahead_ms() + 2, 4))]).await;

    assert_eq!(
        stored_record(&s).await,
        Some(before),
        "the stored value is as it was"
    );
    assert!(events.try_recv().is_err(), "no event");
    assert!(
        s.fan_out_queue().drain().is_empty(),
        "and nothing to fan out"
    );
    assert_eq!(counted.refused(), 2, "each refusal counts");
    let [hash, len] = key_fields("key", 1);
    warnings.assert_one(&["frame limit", &hash, &len]);
}

#[tokio::test]
async fn a_local_refusal_counts_but_does_not_log_since_the_caller_gets_the_error() {
    let recorder = DebuggingRecorder::new();
    let counted = Counted::new(&recorder);
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (warnings, _log) = Warnings::capture();
    let s = capped_shard(8);
    s.merge(1, four(1)).await.expect("fits");

    too_large(s.merge(1, four(2)).await);
    too_large(s.insert(1, four(3)).await);

    assert_eq!(counted.refused(), 2);
    assert!(warnings.logged().is_empty(), "{:?}", warnings.logged());
}

#[tokio::test]
async fn an_insert_many_of_one_key_counts_every_refused_entry_and_changes_nothing() {
    let recorder = DebuggingRecorder::new();
    let counted = Counted::new(&recorder);
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let s = capped_shard(8);
    s.insert(1, four(1)).await.expect("fits");
    let before = stored_record(&s).await;

    // Three entries of one key fold into a run before they apply, and each is
    // refused on its own: none grows the value past the cap and out of it.
    let (size, _) = too_large(
        s.insert_many([(1, four(2)), (1, four(3)), (1, four(4))])
            .await,
    );

    assert_eq!(size, 9, "each would leave nine encoded bytes");
    assert_eq!(counted.refused(), 3, "one count for each refused entry");
    assert_eq!(stored_record(&s).await, before, "the record is whole");
    assert_digest_matches_full_recompute(&s).await;
}

/// The stripe a `u32` key lives in.
fn stripe_of(key: u32) -> usize {
    engine::stripe_index_from_hash(engine::hash_key_bytes(key_bytes(&key).as_ref()))
}

/// Two keys, the first in a higher stripe than the second: a batch given in
/// this order reaches the second's stripe first.
fn keys_in_descending_stripes() -> (u32, u32) {
    let first = (1u32..1_000)
        .find(|key| stripe_of(*key) > 0)
        .expect("a key outside stripe 0");
    let second = (1u32..1_000)
        .find(|key| stripe_of(*key) < stripe_of(first))
        .expect("a key in a lower stripe");
    (first, second)
}

#[tokio::test]
async fn an_insert_many_returns_the_refusal_of_the_first_entry_given_not_the_first_stripe() {
    let s = capped_shard(8);
    let (first, second) = keys_in_descending_stripes();
    s.insert(first, four(1)).await.expect("fits");
    s.insert(second, four(1)).await.expect("fits");

    // `first` would leave nine encoded bytes, `second` ten. The batch is
    // given with `first` first and applies stripe by stripe, `second`'s
    // stripe before `first`'s.
    let (size, _) = too_large(
        s.insert_many([(first, four(2)), (second, vec![2; 5])])
            .await,
    );

    assert_eq!(size, 9, "the first entry given, not the first stripe");
    assert_eq!(s.get(&first).await, Some(four(1)));
    assert_eq!(s.get(&second).await, Some(four(1)));
}

#[tokio::test]
async fn an_insert_many_returns_the_first_refusal_of_a_stripe_not_its_last() {
    // Each entry of key 1 is refused on its own against the stored four
    // bytes: four more leave nine encoded bytes, five more leave ten.
    for (batch, first) in [([four(2), vec![2; 5]], 9), ([vec![2; 5], four(2)], 10)] {
        let s = capped_shard(8);
        s.insert(1, four(1)).await.expect("fits");

        let (size, _) = too_large(s.insert_many(batch.map(|value| (1, value))).await);

        assert_eq!(size, first, "the refusal of the entry given first");
        assert_eq!(s.get(&1).await, Some(four(1)));
    }
}

/// Two keys in one stripe other than stripe 0, and a third key in a lower
/// stripe: a batch reaches the third key's stripe before the pair's.
fn a_pair_in_one_stripe_above_a_third_key() -> (u32, u32, u32) {
    let (a, b) = (1u32..1_000)
        .filter(|key| stripe_of(*key) > 0)
        .find_map(|a| {
            (a + 1..100_000)
                .find(|b| stripe_of(*b) == stripe_of(a))
                .map(|b| (a, b))
        })
        .expect("two keys in one stripe");
    let c = (1u32..1_000)
        .find(|key| stripe_of(*key) < stripe_of(a))
        .expect("a key in a lower stripe");
    (a, b, c)
}

#[tokio::test]
async fn an_insert_many_finds_a_refusal_by_its_place_in_its_stripe() {
    let s = capped_shard(8);
    let (a, b, c) = a_pair_in_one_stripe_above_a_third_key();
    s.insert(b, four(1)).await.expect("fits");
    s.insert(c, four(1)).await.expect("fits");

    // `a` is new and fits. `c`, given second, would leave ten encoded bytes;
    // `b`, given third and second in its stripe, nine.
    let (size, _) = too_large(
        s.insert_many([(a, four(2)), (c, vec![2; 5]), (b, four(2))])
            .await,
    );

    assert_eq!(size, 10, "the refusal of the entry given first");
    assert_eq!(s.get(&a).await, Some(four(2)));
    assert_eq!(s.get(&b).await, Some(four(1)));
    assert_eq!(s.get(&c).await, Some(four(1)));
}

#[tokio::test]
async fn an_insert_many_whose_entry_is_over_the_frame_ends_there_and_returns_that_error() {
    let recorder = DebuggingRecorder::new();
    let counted = Counted::new(&recorder);
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let s = capped_shard(8);
    s.insert(1, four(1)).await.expect("fits");
    s.insert(3, four(1)).await.expect("fits");

    // Entry 1 merges past the cap; entry 2 is a contribution no frame holds,
    // which ends the call before entry 3 is prepared.
    let (size, _) = too_large(
        s.insert_many([(1, four(2)), (2, vec![9; 40]), (3, four(3))])
            .await,
    );

    assert_eq!(size, 41, "the contribution's error, not the merge's nine");
    assert_eq!(s.get(&1).await, Some(four(1)), "the refused entry is whole");
    assert_eq!(s.get(&2).await, None);
    assert_eq!(
        s.get(&3).await,
        Some(four(1)),
        "the entries after the contribution are not applied"
    );
    assert_eq!(counted.refused(), 1, "the merge refused before it counts");
}

#[tokio::test]
async fn a_log_line_names_a_key_by_hash_and_length_never_by_its_bytes() {
    use std::fmt::Write as _;

    let (warnings, _log) = Warnings::capture();
    let s = shard::<String, Vec<u8>>(1)
        .with_resolver(Arc::new(ConcatResolver))
        .with_max_frame(cap_for(8));
    let key = "alice@example.com".to_owned();
    let record = |value: &[u8], ver: Hlc| WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from(
            postcard::to_stdvec(&value.to_vec()).expect("test value encodes"),
        )),
        ver,
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, record(&four(1), hlc(ahead_ms(), 2))).await;

    ShardOps::apply_remote(&s, record(&four(2), hlc(ahead_ms() + 1, 3))).await;

    let encoded = key_bytes(&key);
    warnings.assert_one(&[
        "frame limit",
        &format!(
            "key_hash=\"{:016x}\"",
            engine::hash_key_bytes(encoded.as_ref())
        ),
        &format!("key_len={}", encoded.len()),
    ]);
    if Warnings::CAPTURES {
        let line = warnings.logged().join("\n");
        let hex = encoded.iter().fold(String::new(), |mut acc, byte| {
            let _ = write!(acc, "{byte:02x}");
            acc
        });
        assert!(!line.contains("alice"), "{line}");
        assert!(!line.contains(&hex), "{line}");
    }
}

#[tokio::test]
async fn a_windowed_call_that_overflows_its_own_pending_fold_gets_the_error() {
    let recorder = DebuggingRecorder::new();
    let counted = Counted::new(&recorder);
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let s = capped_shard(8).with_merge_coalesce_window(WINDOW);
    s.merge(1, four(1)).await.expect("opens a fold");
    let pending = s.pending_merge_snapshot(&1).expect("pending");

    let (size, _) = too_large(s.merge(1, four(2)).await);

    assert_eq!(size, 9, "the fold of the two would hold eight bytes");
    assert_eq!(
        s.pending_merge_snapshot(&1),
        Some(pending),
        "the pending fold is as it was, version included"
    );
    assert_eq!(counted.refused(), 1);
    s.merge(1, Vec::new()).await.expect("a clear still folds");
    s.flush_all_pending_merges();
    assert_eq!(s.get(&1).await, Some(Vec::new()));
}

#[tokio::test]
async fn a_fold_that_overflows_the_stored_value_is_refused_at_its_flush_where_it_is_logged() {
    let recorder = DebuggingRecorder::new();
    let counted = Counted::new(&recorder);
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (warnings, _log) = Warnings::capture();
    let s = capped_shard(8).with_merge_coalesce_window(WINDOW);
    s.merge(1, four(1)).await.expect("fold");
    s.flush_all_pending_merges();
    let stored = stored_record(&s).await.expect("stored");

    s.merge(1, four(2))
        .await
        .expect("the call returns before the fold meets the stored value");
    assert_eq!(counted.refused(), 0, "not refused yet");
    s.flush_all_pending_merges();

    assert_eq!(
        stored_record(&s).await,
        Some(stored),
        "the stored value is as it was"
    );
    assert_eq!(counted.refused(), 1);
    let [hash, len] = key_fields("key", 1);
    warnings.assert_one(&["frame limit", &hash, &len]);
}

/// The value bytes of the record `unshippable_record` builds are a vector of
/// this many bytes under a four-byte length prefix.
const UNSHIPPABLE_PREFIX: usize = 4;

/// A record for key 2 whose frame is one byte past [`wire::MAX_FRAME`].
fn unshippable_record(s: &Shard<u32, Vec<u8>>) -> WireRecord {
    let key = key_bytes(&2u32);
    let base = wire::replicate_frame_len(s.name.len(), key.len(), 0);
    let value_len = wire::MAX_FRAME - base + 1;
    let value = postcard::to_stdvec(&vec![9u8; value_len - UNSHIPPABLE_PREFIX])
        .expect("test value encodes");
    assert_eq!(
        value.len(),
        value_len,
        "a four-byte prefix for a vector this long"
    );
    WireRecord {
        key,
        value: Some(Bytes::from(value)),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn a_record_too_large_for_a_frame_stays_here_and_out_of_every_sync() {
    let recorder = DebuggingRecorder::new();
    let counted = Counted::new(&recorder);
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (warnings, _log) = Warnings::capture();
    let s = capped_shard(8);
    let big = unshippable_record(&s);
    assert_eq!(
        wire::replicate_frame_len(
            s.name.len(),
            big.key.len(),
            big.value.as_ref().map_or(0, Bytes::len)
        ),
        wire::MAX_FRAME + 1
    );
    // Stored before the cap applied, as a peer's record, below the public API.
    ShardOps::apply_remote(&s, big.clone()).await;
    ShardOps::apply_remote(&s, bytes_record(&four(1), hlc(ahead_ms(), 2))).await;
    assert_eq!(
        s.get(&2).await.map(|v| v.len()),
        Some(big.value.as_ref().map_or(0, Bytes::len) - UNSHIPPABLE_PREFIX),
        "the local copy keeps serving"
    );

    let keys = vec![key_bytes(&1u32), key_bytes(&2u32)];
    let read = ShardOps::records_for(&s, keys).await;
    assert_eq!(
        read.len(),
        2,
        "a read answers both: only a sync leaves one out"
    );
    let shipped = ShardOps::skip_unframeable(&s, read);
    assert_eq!(
        shipped.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        [key_bytes(&1u32)]
    );
    assert_eq!(counted.skipped(), 1);

    let snapshot: Vec<Vec<WireRecord>> = ShardOps::snapshot_chunks(&s).collect().await;
    let snapshot_keys: Vec<Bytes> = snapshot.into_iter().flatten().map(|r| r.key).collect();
    assert_eq!(
        snapshot_keys,
        [key_bytes(&1u32)],
        "a state transfer skips it too"
    );
    assert_eq!(counted.skipped(), 2, "each skip counts");
    let [hash, len] = key_fields("largest_key", 2);
    warnings.assert_one(&["larger than one frame", &hash, &len]);
}

/// A set of `count` adds by `writer`, numbered from 1.
fn set_of(writer: WriterId, count: u32) -> OrSet<u32> {
    (2..=count).fold(OrSet::add(writer, 1, 1), |set, n| {
        set.merge(&OrSet::add(writer, u64::from(n), n))
    })
}

/// A shard of `OrSet`s that holds `set` under key 1 and keeps values to
/// exactly that size, so no merge has room to add a byte.
async fn shard_full_of(set: &OrSet<u32>) -> Shard<u32, OrSet<u32>> {
    let encoded = set.encode().expect("encodes");
    let s = shard::<u32, OrSet<u32>>(1)
        .with_resolver(Arc::new(OrSetResolver::<u32>::new()))
        .with_max_frame(wire::forward_frame_len("test".len(), 1, encoded.len()));
    s.merge(1, set.clone()).await.expect("the set fits exactly");
    s
}

#[tokio::test]
async fn a_compaction_that_would_grow_a_value_past_the_cap_is_refused_counted_and_logged() {
    let recorder = DebuggingRecorder::new();
    let counted = Counted::new(&recorder);
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let (warnings, _log) = Warnings::capture();
    let dead = WriterId::new(NodeId::from(11), 1);
    let retire = move |w: WriterId| w == dead;
    let bounds = CompactionBounds::three_bounds(1_000_000_000);
    let set = set_of(dead, 12);
    let grown = set
        .compact(1_000, bounds, &retire, Quiescence::Settled)
        .expect("a dead writer's first stage adds a retirement");
    assert!(
        grown.encode().expect("encodes").len() > set.encode().expect("encodes").len(),
        "the premise: a first stage grows the value"
    );
    let s = shard_full_of(&set).await;
    let before = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;

    let outcome =
        ShardOps::compact_pass(&s, 1_000, &retire, Quiescence::Settled, bounds, 100).await;

    assert_eq!(outcome.compacted, 0, "nothing is rewritten");
    assert_eq!(
        ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await,
        before,
        "the stored record keeps its bytes and its version"
    );
    assert_eq!(
        counted.refused(),
        1,
        "the refusal counts as a refused merge"
    );
    let [hash, len] = key_fields("key", 1);
    warnings.assert_one(&["a compaction pass", "frame limit", &hash, &len]);

    ShardOps::compact_pass(&s, 2_000, &retire, Quiescence::Settled, bounds, 100).await;
    assert_eq!(counted.refused(), 2, "each pass that meets it counts again");
    assert!(
        warnings.logged().len() <= 1,
        "and logs at most once a minute: {:?}",
        warnings.logged()
    );
}

#[tokio::test]
async fn a_compaction_that_shrinks_a_value_at_the_cap_is_applied() {
    let dead = WriterId::new(NodeId::from(11), 1);
    let retire = move |w: WriterId| w == dead;
    let bounds = CompactionBounds::three_bounds(1_000);
    // A remove leaves a watermark for the writer, which the second stage drops.
    let base = set_of(dead, 12);
    let base = base.merge(&OrSet::remove(&base, &12));
    let retired = base
        .compact(1_000, bounds, &retire, Quiescence::Churning)
        .expect("the first stage");
    let folded = retired
        .compact(3_001, bounds, &retire, Quiescence::Settled)
        .expect("the second stage");
    assert!(
        folded.encode().expect("encodes").len() < retired.encode().expect("encodes").len(),
        "the premise: the second stage leaves the value smaller"
    );
    let s = shard_full_of(&retired).await;

    let outcome =
        ShardOps::compact_pass(&s, 3_001, &retire, Quiescence::Settled, bounds, 100).await;

    assert_eq!(outcome.compacted, 1, "a rewrite that fits is applied");
    assert_eq!(
        s.get(&1).await.map(|set| set.iter().count()),
        Some(11),
        "and the elements stay"
    );
}

/// A record for key 1 of `set`, as a peer sends it.
fn set_record(set: &OrSet<u32>) -> WireRecord {
    WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(set.encode().expect("encodes"))),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    }
}

/// Shards of `OrSet`s that keep values to `cap` bytes under a one-byte key.
fn set_shard(cap: usize) -> Shard<u32, OrSet<u32>> {
    shard::<u32, OrSet<u32>>(1)
        .with_resolver(Arc::new(OrSetResolver::<u32>::new()))
        .with_max_frame(cap)
}

/// The value bytes the `OrSet` shards below hold at most.
const SET_CAP: usize = 256;

#[tokio::test]
async fn a_remove_fits_a_frame_when_the_writers_it_touches_hold_few_tags() {
    // A hundred writers, one element each: a set past the cap whose remove
    // carries one writer's tags and so fits a frame.
    let many_writers = (0..100u64)
        .map(|n| {
            let writer = WriterId::new(NodeId::from(100 + n), 1);
            OrSet::add(writer, 1, u32::try_from(n).expect("small"))
        })
        .reduce(|set, add| set.merge(&add))
        .expect("a hundred adds");
    let stored_len = many_writers.encode().expect("encodes").len();
    assert!(
        stored_len > SET_CAP,
        "the premise: the set is over the cap: {stored_len}"
    );
    let s = set_shard(wire::forward_frame_len("test".len(), 1, SET_CAP));
    ShardOps::apply_remote(&s, set_record(&many_writers)).await;

    s.merge(1, OrSet::remove(&many_writers, &7))
        .await
        .expect("a remove that fits a frame shrinks a set past the cap");

    let set = s.get(&1).await.expect("stored");
    assert_eq!(set.iter().count(), 99, "one element out");
    assert!(!set.contains(&7));
}

#[tokio::test]
async fn a_remove_against_a_set_one_writer_filled_is_as_large_as_the_set_and_is_refused() {
    let writer = WriterId::new(NodeId::from(11), 1);
    let one_writer = set_of(writer, 300);
    let stored_len = one_writer.encode().expect("encodes").len();
    assert!(
        stored_len > SET_CAP,
        "the premise: the set is over the cap: {stored_len}"
    );
    let cap = wire::forward_frame_len("test".len(), 1, SET_CAP);
    let s = set_shard(cap);
    ShardOps::apply_remote(&s, set_record(&one_writer)).await;
    let remove = OrSet::remove(&one_writer, &7);
    assert!(
        remove.encode().expect("encodes").len() > SET_CAP,
        "the remove carries the writer's 299 other tags"
    );

    let (size, limit) = too_large(s.merge(1, remove).await);

    assert!(size > SET_CAP);
    assert_eq!(limit, cap);
    assert_eq!(
        s.get(&1).await.map(|set| set.iter().count()),
        Some(300),
        "nothing changed"
    );
    s.remove(&1).await.expect("Cache::remove is the way");
    assert_eq!(s.get(&1).await, None);
}
