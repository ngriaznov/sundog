//! Applying remote records: version order, clock-skew refusal and the events a write emits.

use super::*;

#[tokio::test]
async fn newer_remote_tombstone_beats_older_local_put() {
    let s = shard::<u32, String>(1);
    s.insert(1, "a".into()).await.expect("insert");
    assert_eq!(s.get(&1).await, Some("a".into()));

    let rec = WireRecord {
        key: key_bytes(&1u32),
        value: None,
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, rec).await;
    assert_eq!(s.get(&1).await, None);
}

#[tokio::test]
async fn newer_remote_put_beats_older_local_tombstone() {
    let s = shard::<u32, String>(1);
    s.insert(1, "a".into()).await.expect("insert");
    s.remove(&1).await.expect("remove");
    assert_eq!(s.get(&1).await, None);

    let rec = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from_static(b"\x01b")),
        ver: hlc(ahead_ms(), 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, rec).await;
    assert_eq!(s.get(&1).await, Some("b".into()));
}

#[tokio::test]
async fn stale_remote_writes_are_rejected_idempotently() {
    let s = shard::<u32, String>(1);
    let winner = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from_static(b"\x01x")),
        ver: hlc(1_000_000, 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, winner.clone()).await;
    assert_eq!(s.get(&1).await, Some("x".into()));

    let stale = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from_static(b"\x01y")),
        ver: hlc(1, 3),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, stale).await;
    assert_eq!(
        s.get(&1).await,
        Some("x".into()),
        "an older write does not overwrite a newer one"
    );

    // Re-applying the exact same winning record is also a no-op.
    ShardOps::apply_remote(&s, winner).await;
    assert_eq!(s.get(&1).await, Some("x".into()));
}

/// A shard on a fixed clock at `NOW_MS` with a one-minute skew bound.
fn skew_bounded_shard() -> Shard<u32, String> {
    const NOW_MS: u64 = 1_700_000_000_000;
    shard::<u32, String>(1)
        .with_clock(Arc::new(|| NOW_MS))
        .with_max_clock_skew(Some(Duration::from_mins(1)))
}

/// The version `shard` holds for `key`, read from its bucket listing.
async fn held_version(shard: &Shard<u32, String>, key: u32) -> Option<Hlc> {
    let key_bytes = key_bytes(&key);
    ShardOps::bucket_entries(shard, bucket_of(&key_bytes))
        .await
        .into_iter()
        .find(|entry| entry.key == key_bytes)
        .map(|entry| entry.version)
}

fn put(key: u32, value: &str, ver: Hlc) -> WireRecord {
    WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from(
            postcard::to_stdvec(&value.to_string()).expect("value encodes"),
        )),
        ver,
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn a_remote_record_stamped_past_max_clock_skew_is_refused_and_moves_no_clock() {
    let s = skew_bounded_shard();
    let now = s.now_ms();
    let within = now + 60_000;
    let past = now + 60_001;
    ShardOps::apply_remote_batch(
        &s,
        vec![
            put(1, "within", hlc(within, 2)),
            put(2, "past", hlc(past, 2)),
        ],
    )
    .await;
    assert_eq!(s.get(&1).await.as_deref(), Some("within"));
    assert_eq!(s.get(&2).await, None, "a stamp past the bound is refused");

    s.insert(3, "local".into()).await.expect("insert");
    let local = held_version(&s, 3).await.expect("the local write is held");
    assert_eq!(
        local.wall_ms, within,
        "the clock took the accepted stamp and never the refused one"
    );

    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(past, 9)).await;
    assert_eq!(
        s.get(&1).await.as_deref(),
        Some("within"),
        "an invalidation stamped past the bound is refused too"
    );
    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(within, 9)).await;
    assert_eq!(s.get(&1).await, None);
}

#[tokio::test]
async fn a_refused_record_applies_once_the_clock_comes_within_the_bound() {
    let now = Arc::new(std::sync::atomic::AtomicU64::new(1_700_000_000_000));
    let clock = Arc::clone(&now);
    let s = shard::<u32, String>(1)
        .with_clock(Arc::new(move || clock.load(Ordering::Relaxed)))
        .with_max_clock_skew(Some(Duration::from_mins(1)));
    let stamp = hlc(now.load(Ordering::Relaxed) + 3_600_000, 2);
    ShardOps::apply_remote(&s, put(1, "early", stamp)).await;
    assert_eq!(s.get(&1).await, None);

    now.fetch_add(3_600_000 - 60_000, Ordering::Relaxed);
    ShardOps::apply_remote(&s, put(1, "early", stamp)).await;
    assert_eq!(s.get(&1).await.as_deref(), Some("early"));
}

#[tokio::test]
async fn no_max_clock_skew_accepts_a_stamp_any_distance_ahead() {
    let s = skew_bounded_shard().with_max_clock_skew(None);
    let far = hlc(s.now_ms() + 365 * 24 * 3_600_000, 2);
    ShardOps::apply_remote(&s, put(1, "far", far)).await;
    assert_eq!(s.get(&1).await.as_deref(), Some("far"));
    s.insert(2, "local".into()).await.expect("insert");
    assert!(
        held_version(&s, 2).await.expect("held") > far,
        "an unbounded shard's clock follows the far stamp"
    );
}

#[test]
fn with_cluster_config_takes_the_configured_max_clock_skew() {
    let config = ClusterConfig::default().with(|c| c.max_clock_skew = Some(Duration::from_secs(5)));
    assert_eq!(
        shard::<u32, String>(1)
            .with_cluster_config(&config)
            .max_clock_skew_ms,
        Some(5_000)
    );
    let config = ClusterConfig::default().with(|c| c.max_clock_skew = None);
    assert_eq!(
        shard::<u32, String>(1)
            .with_cluster_config(&config)
            .max_clock_skew_ms,
        None
    );
}

#[tokio::test]
async fn invalidate_respects_newer_local_write() {
    let s = shard::<u32, String>(1);
    s.insert(1, "fresh".into()).await.expect("insert");

    // An invalidation for an old version does not evict a newer local
    // write.
    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(1, 9)).await;
    assert_eq!(s.get(&1).await, Some("fresh".into()));

    // A newer invalidation does evict it, and writes no tombstone.
    ShardOps::invalidate(&s, key_bytes(&1u32), hlc(ahead_ms(), 9)).await;
    assert_eq!(s.get(&1).await, None);
    let records = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert!(records.is_empty(), "{records:?}");
}

#[tokio::test]
async fn insert_update_remove_emit_matching_events() {
    let s = shard::<u32, String>(1);
    let mut events = s.events();

    s.insert(1, "a".into()).await.expect("insert");
    match events.recv().await.expect("created") {
        Event::Created {
            key: 1,
            value,
            origin: Origin::Local,
        } => assert_eq!(value, "a"),
        other => panic!("unexpected event: {other:?}"),
    }

    s.insert(1, "b".into()).await.expect("insert");
    match events.recv().await.expect("updated") {
        Event::Updated {
            key: 1,
            value,
            origin: Origin::Local,
        } => assert_eq!(value, "b"),
        other => panic!("unexpected event: {other:?}"),
    }

    s.remove(&1).await.expect("remove");
    match events.recv().await.expect("removed") {
        Event::Removed {
            key: 1,
            origin: Origin::Local,
        } => {}
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn with_clock_drives_every_shard_timestamp() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let fake_now = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&fake_now);
    let clock_fn: Arc<dyn Fn() -> u64 + Send + Sync> =
        Arc::new(move || reader.load(Ordering::SeqCst));
    let s = Shard::<u32, String>::new(
        SmolStr::new("test"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        Some(Duration::from_millis(100)),
        None,
    )
    .with_clock(Arc::clone(&clock_fn));

    s.insert(1, "a".into()).await.expect("insert");
    let recs = ShardOps::records_for(&s, vec![key_bytes(&1u32)]).await;
    assert_eq!(
        recs[0].ver.wall_ms, 1_000,
        "the HLC stamp reads the injected clock, not the system clock"
    );
    assert_eq!(
        recs[0].expires_at_ms,
        Some(1_100),
        "the expiry deadline is computed from the injected clock"
    );
    assert_eq!(
        s.get(&1).await,
        Some("a".into()),
        "not yet past the injected clock's TTL"
    );

    // Advance the fake clock, not real time, past the TTL.
    fake_now.store(1_300, Ordering::SeqCst);
    assert_eq!(
        s.get(&1).await,
        None,
        "reads are TTL-blind but still honor the injected clock for expiry"
    );
    ShardOps::run_pending_tasks(&s).await;
    assert_eq!(
        s.entry_count().await,
        0,
        "the sweep ran against the injected clock, not real time"
    );
}

/// What `shard` holds for `key` as wire records, tombstones and expiries
/// included.
async fn held_records<V>(shard: &Shard<u32, V>, key: u32) -> Vec<WireRecord>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    ShardOps::records_for(shard, vec![key_bytes(&key)]).await
}

/// Applies `rec` twice and answers the records held after each.
async fn held_after_each_apply<V>(
    shard: &Shard<u32, V>,
    key: u32,
    rec: WireRecord,
) -> (Vec<WireRecord>, Vec<WireRecord>)
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    ShardOps::apply_remote_batch(shard, vec![rec.clone()]).await;
    let once = held_records(shard, key).await;
    ShardOps::apply_remote_batch(shard, vec![rec]).await;
    (once, held_records(shard, key).await)
}

#[tokio::test]
async fn a_resent_put_changes_nothing() {
    let s = shard::<u32, String>(1);
    let (once, twice) = held_after_each_apply(&s, 1, put(1, "a", hlc(1_000, 2))).await;
    assert_eq!(once.len(), 1, "the first delivery stores the record");
    assert_eq!(
        twice, once,
        "the second delivery leaves the value and version alone"
    );
    assert_eq!(s.get(&1).await.as_deref(), Some("a"));
}

#[tokio::test]
async fn a_resent_put_with_an_expiry_keeps_its_expiry() {
    let s = shard::<u32, String>(1);
    let rec = WireRecord {
        expires_at_ms: Some(s.now_ms() + 60_000),
        ..put(1, "a", hlc(s.now_ms(), 2))
    };
    let (once, twice) = held_after_each_apply(&s, 1, rec.clone()).await;
    assert_eq!(once[0].expires_at_ms, rec.expires_at_ms);
    assert_eq!(twice, once);
}

#[tokio::test]
async fn a_resent_tombstone_changes_nothing_and_does_not_resurrect_the_key() {
    let s = shard::<u32, String>(1);
    // Stamps just behind the shard's clock, so the tombstone arrives within
    // its retention.
    let base = now_ms() - 10_000;
    ShardOps::apply_remote(&s, put(1, "a", hlc(base + 1_000, 2))).await;
    let tombstone = WireRecord {
        value: None,
        ..put(1, "a", hlc(base + 2_000, 2))
    };
    let (once, twice) = held_after_each_apply(&s, 1, tombstone).await;
    assert_eq!(twice, once);
    assert_eq!(once.len(), 1, "the tombstone is stored, not ignored");
    assert_eq!(once[0].value, None);
    assert_eq!(s.get(&1).await, None);
    // The put that preceded the tombstone, resent late, stays dead.
    ShardOps::apply_remote(&s, put(1, "a", hlc(base + 1_000, 2))).await;
    assert_eq!(s.get(&1).await, None, "a deleted entry never resurrects");
}

#[tokio::test]
async fn a_resent_merged_record_changes_nothing() {
    use crate::store::crdt::{PnCounter, PnCounterResolver};
    let source = shard::<u32, PnCounter>(1).with_resolver(Arc::new(PnCounterResolver));
    let writer = crdt::WriterId::new(NodeId::from(1), 1);
    source
        .merge(1, PnCounter::local_delta(writer, 5))
        .await
        .expect("merge");
    let rec = held_records(&source, 1).await.remove(0);

    let target = shard::<u32, PnCounter>(2).with_resolver(Arc::new(PnCounterResolver));
    let (once, twice) = held_after_each_apply(&target, 1, rec).await;
    assert_eq!(twice, once);
    assert_eq!(target.get(&1).await.map(|c| c.value()), Some(5));
}
