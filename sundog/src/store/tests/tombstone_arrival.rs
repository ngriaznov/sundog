//! The arrival rule at the shard: a tombstone a peer sends past its
//! retention is ignored, counted and, when it came by forward, logged; this
//! node's own delete is stored however late it applies.

use std::sync::atomic::{AtomicU64, Ordering};

use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use super::*;
use crate::store::shard_ignored::{IGNORED_FORWARD_WARN_SPACING_MS, ignored_forward_warn_due};

pub(super) const START_MS: u64 = 10_000_000;
const TTL: Duration = Duration::from_secs(60);
const TTL_MS: u64 = 60_000;
const MAX_TTL: Duration = Duration::from_secs(600);

pub(super) fn manual_clock() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(START_MS))
}

/// A `Mode::Replicated` shard on `clock` with a 60 s retention and a 600 s
/// cap.
pub(super) fn clocked(clock: &Arc<AtomicU64>, ttl: Duration) -> Shard<u32, String> {
    let reader = Arc::clone(clock);
    Shard::<u32, String>::new(
        SmolStr::new("arrival"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    )
    .with_tombstone_ttl(ttl)
    .with_tombstone_max_ttl(MAX_TTL)
    .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
}

pub(super) fn delete_record(key: u32, wall_ms: u64) -> WireRecord {
    WireRecord {
        key: key_bytes(&key),
        value: None,
        ver: hlc(wall_ms, 2),
        expires_at_ms: None,
    }
}

/// How much the counter `sundog_tombstones_ignored_total` for the cache
/// `arrival` has risen since the last call: a snapshot drains the recorder.
fn ignored_since_last_read(snapshotter: &metrics_util::debugging::Snapshotter) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == "sundog_tombstones_ignored_total")
        .map(|(key, _, _, value)| {
            assert!(
                key.key()
                    .labels()
                    .any(|label| label.key() == "cache" && label.value() == "arrival"),
                "labelled with the cache"
            );
            match value {
                DebugValue::Counter(n) => n,
                other => panic!("a counter, got {other:?}"),
            }
        })
        .sum()
}

// --- the pure rule ---

#[test]
fn a_forward_warning_is_due_at_the_first_and_then_once_per_spacing() {
    assert!(ignored_forward_warn_due(None, 0));
    assert!(!ignored_forward_warn_due(Some(1_000), 1_000));
    assert!(!ignored_forward_warn_due(
        Some(1_000),
        1_000 + IGNORED_FORWARD_WARN_SPACING_MS - 1
    ));
    assert!(ignored_forward_warn_due(
        Some(1_000),
        1_000 + IGNORED_FORWARD_WARN_SPACING_MS
    ));
    assert!(
        ignored_forward_warn_due(Some(1_000), 999),
        "a clock that stepped back warns again"
    );
}

// --- a node's own delete is never ignored ---

/// `tombstone_ttl` zero makes a delete lapsed at its own stamp. Each of the
/// three local deletes still stores its tombstone.
#[tokio::test]
async fn shard_remove_stores_its_tombstone_though_it_is_lapsed_at_its_stamp() {
    let clock = manual_clock();
    let s = clocked(&clock, Duration::ZERO);
    s.insert(1, "a".into()).await.expect("insert");
    ShardOps::gc_tombstones(&s, false).await;

    s.remove(&1).await.expect("remove");

    assert_eq!(s.get(&1).await, None);
    assert_eq!(s.tombstone_count(), 1, "the delete is stored, not ignored");
}

#[tokio::test]
async fn shard_remove_many_stores_its_tombstones_though_they_are_lapsed_at_their_stamp() {
    let clock = manual_clock();
    let s = clocked(&clock, Duration::ZERO);
    for key in 1..=3 {
        s.insert(key, "a".into()).await.expect("insert");
    }
    ShardOps::gc_tombstones(&s, false).await;

    s.remove_many([1, 2, 3]).await.expect("remove_many");

    for key in 1..=3 {
        assert_eq!(s.get(&key).await, None);
    }
    assert_eq!(s.tombstone_count(), 3);
}

#[tokio::test]
async fn shard_clear_stores_its_tombstones_though_they_are_lapsed_at_their_stamp() {
    let clock = manual_clock();
    let s = clocked(&clock, Duration::ZERO);
    for key in 1..=3 {
        s.insert(key, "a".into()).await.expect("insert");
    }
    ShardOps::gc_tombstones(&s, false).await;

    s.clear().await.expect("clear");

    for key in 1..=3 {
        assert_eq!(s.get(&key).await, None);
    }
    assert_eq!(s.tombstone_count(), 3);
}

/// The twin of the three: the same delete from a peer is ignored.
#[tokio::test]
async fn a_peer_delete_lapsed_at_its_stamp_is_ignored_where_a_local_one_is_stored() {
    let clock = manual_clock();
    let s = clocked(&clock, Duration::ZERO);
    ShardOps::apply_remote(&s, delete_record(1, START_MS - 1)).await;
    assert_eq!(
        s.tombstone_count(),
        1,
        "a fresh shard defers, so the cap is the horizon"
    );
    ShardOps::gc_tombstones(&s, false).await;
    assert_eq!(s.tombstone_count(), 0, "collected at once");

    ShardOps::apply_remote(&s, delete_record(2, START_MS - 1)).await;
    assert_eq!(s.tombstone_count(), 0, "now it is ignored");
}

// --- a peer's lapsed delete ---

#[tokio::test]
async fn a_lapsed_peer_delete_leaves_a_live_value_and_counts() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let clock = manual_clock();
    let s = clocked(&clock, TTL);
    ShardOps::gc_tombstones(&s, false).await;
    // The value is older than the delete that follows.
    ShardOps::apply_remote(&s, wire_record(1, "kept", hlc(START_MS - 200_000, 3))).await;

    // Stamped a minute before it arrives.
    let stamp = START_MS - TTL_MS;
    ShardOps::apply_remote(&s, delete_record(1, stamp)).await;

    assert_eq!(s.get(&1).await.as_deref(), Some("kept"));
    assert_eq!(s.tombstone_count(), 0);
    assert_eq!(ignored_since_last_read(&snapshotter), 1);

    // One millisecond inside its retention it lands and deletes.
    ShardOps::apply_remote(&s, delete_record(1, stamp + 1)).await;
    assert_eq!(s.get(&1).await, None);
    assert_eq!(s.tombstone_count(), 1);
    assert_eq!(
        ignored_since_last_read(&snapshotter),
        0,
        "a stored delete is not counted"
    );
}

#[tokio::test]
async fn an_ignored_delete_does_not_stop_the_older_value_arriving_later() {
    let clock = manual_clock();
    let s = clocked(&clock, TTL);
    ShardOps::gc_tombstones(&s, false).await;
    ShardOps::apply_remote(&s, delete_record(1, START_MS - 120_000)).await;
    ShardOps::apply_remote(&s, wire_record(1, "old", hlc(START_MS - 130_000, 3))).await;
    assert_eq!(s.get(&1).await.as_deref(), Some("old"));
    assert_eq!(s.tombstone_count(), 0);
}

#[tokio::test]
async fn a_lapsed_expiry_tombstone_counts_as_ignored_too() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let clock = manual_clock();
    let s = clocked(&clock, TTL);
    ShardOps::gc_tombstones(&s, false).await;
    let mut tombstone = delete_record(1, START_MS - 1_000);
    tombstone.expires_at_ms = Some(START_MS - 120_000);
    ShardOps::apply_remote(&s, tombstone).await;
    assert_eq!(s.tombstone_count(), 0);
    assert_eq!(ignored_since_last_read(&snapshotter), 1);
}

#[tokio::test]
async fn an_older_value_losing_to_a_stored_version_is_not_counted_as_ignored() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let clock = manual_clock();
    let s = clocked(&clock, TTL);
    s.insert(1, "new".into()).await.expect("insert");
    ShardOps::apply_remote(&s, wire_record(1, "stale", hlc(START_MS - 5_000, 3))).await;
    ShardOps::apply_remote(&s, delete_record(1, START_MS - 4_000)).await;
    assert_eq!(s.get(&1).await.as_deref(), Some("new"));
    assert_eq!(
        ignored_since_last_read(&snapshotter),
        0,
        "losing on version is not lapsing"
    );
}

// --- a forwarded delete ---

/// Tests that read the log, which the `sim` build's cluster test support
/// does not provide a subscriber for.
#[cfg(not(feature = "sim"))]
pub(super) mod logged {
    use std::sync::Mutex;

    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    /// A tracing layer that keeps the message of every `WARN` event.
    #[derive(Clone, Default)]
    pub(in crate::store::tests) struct Warnings(Arc<Mutex<Vec<String>>>);

    /// Collects the `message` field of one event.
    struct Message(String);
    impl tracing::field::Visit for Message {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = format!("{value:?}");
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
        pub(in crate::store::tests) fn taken(&self) -> Vec<String> {
            std::mem::take(&mut *self.0.lock().expect("lock"))
        }
    }

    #[tokio::test]
    async fn a_forwarded_delete_that_arrives_past_its_ttl_is_ignored_counted_and_warned() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _metrics = metrics::set_default_local_recorder(&recorder);
        let warnings = Warnings::default();
        let _log = crate::cluster::test_support::scoped_subscriber(
            tracing_subscriber::registry().with(warnings.clone()),
        );
        let clock = manual_clock();
        let s = clocked(&clock, TTL);
        ShardOps::gc_tombstones(&s, false).await;
        s.insert(1, "kept".into()).await.expect("insert");
        s.insert(2, "kept".into()).await.expect("insert");

        let lapsed = START_MS - 2 * TTL_MS;
        ShardOps::apply_forwarded_batch(&s, vec![delete_record(1, lapsed)]).await;
        assert_eq!(s.get(&1).await.as_deref(), Some("kept"), "ignored");
        assert_eq!(ignored_since_last_read(&snapshotter), 1, "counted");
        let logged = warnings.taken();
        assert_eq!(logged.len(), 1, "warned once: {logged:?}");
        assert!(logged[0].contains("forwarded"), "{logged:?}");

        // A second one inside the spacing counts and does not warn again.
        ShardOps::apply_forwarded_batch(&s, vec![delete_record(2, lapsed)]).await;
        assert_eq!(ignored_since_last_read(&snapshotter), 1);
        assert!(warnings.taken().is_empty(), "rate limited");

        // After the spacing it warns again. The clock steps a gc period at a
        // time with a collection at each, as the gc task would: a node that
        // collects that often is not paused.
        let mut at = START_MS;
        while at < START_MS + IGNORED_FORWARD_WARN_SPACING_MS {
            at += 15_000;
            clock.store(at, Ordering::SeqCst);
            ShardOps::gc_tombstones(&s, false).await;
        }
        assert_eq!(at, START_MS + IGNORED_FORWARD_WARN_SPACING_MS);
        ShardOps::apply_forwarded_batch(&s, vec![delete_record(2, lapsed)]).await;
        assert_eq!(ignored_since_last_read(&snapshotter), 1);
        assert_eq!(warnings.taken().len(), 1);
    }

    #[tokio::test]
    async fn a_replicated_lapsed_delete_counts_but_does_not_warn() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _metrics = metrics::set_default_local_recorder(&recorder);
        let warnings = Warnings::default();
        let _log = crate::cluster::test_support::scoped_subscriber(
            tracing_subscriber::registry().with(warnings.clone()),
        );
        let clock = manual_clock();
        let s = clocked(&clock, TTL);
        ShardOps::gc_tombstones(&s, false).await;

        ShardOps::apply_remote(&s, delete_record(1, START_MS - 2 * TTL_MS)).await;

        assert_eq!(ignored_since_last_read(&snapshotter), 1);
        assert!(
            warnings.taken().is_empty(),
            "a replicated delete has its sender's copy to offer again"
        );
    }
}
