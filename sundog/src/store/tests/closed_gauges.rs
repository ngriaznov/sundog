//! The gauges a shard publishes read 0 once its cache closes, and a cache
//! that never published one gets no series.

use super::skew_hold::{TestClock, forward, owned_key, skew_shard};
use super::*;
use crate::store::test_support::Gauges;

#[tokio::test]
async fn a_closed_shard_reads_no_held_forwards() {
    let recorder = Gauges::default();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let clock = TestClock::at(1_000_000);
    let (shard, view, _tx, _residency) = skew_shard::<String>(&clock);
    let name = shard.name().to_string();
    let key = owned_key(&view);

    ShardOps::apply_forwarded_batch(&shard, vec![forward(key, "skewed", 10_000)]).await;
    assert_eq!(
        recorder.get("sundog_clock_skew_held", Some(name.as_str())),
        Some(1.0)
    );
    assert!(
        recorder
            .get("sundog_clock_skew_held_bytes", Some(name.as_str()))
            .is_some_and(|b| b > 0.0),
        "the hold counts its bytes"
    );

    shard.seal_for_close();

    assert_eq!(
        recorder.get("sundog_clock_skew_held", Some(name.as_str())),
        Some(0.0)
    );
    assert_eq!(
        recorder.get("sundog_clock_skew_held_bytes", Some(name.as_str())),
        Some(0.0)
    );
}

#[tokio::test]
async fn a_shard_that_never_held_a_forward_gets_no_skew_series_when_it_closes() {
    let recorder = Gauges::default();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let clock = TestClock::at(1_000_000);
    let (shard, _view, _tx, _residency) = skew_shard::<String>(&clock);
    let name = shard.name().to_string();

    shard.seal_for_close();

    assert_eq!(
        recorder.get("sundog_clock_skew_held", Some(name.as_str())),
        None
    );
    assert_eq!(
        recorder.get("sundog_clock_skew_held_bytes", Some(name.as_str())),
        None
    );
}

#[tokio::test]
async fn a_closed_fan_out_queue_reads_no_backlog_and_a_local_one_never_publishes() {
    let recorder = Gauges::default();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let replicated = Shard::<u32, String>::new(
        SmolStr::new("closed-backlog-replicated"),
        Mode::Replicated,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    replicated.insert(1, "a".into()).await.expect("insert");
    assert_eq!(
        recorder.get("sundog_fan_out_backlog", Some("closed-backlog-replicated")),
        Some(1.0)
    );
    replicated.fan_out_queue().close();
    assert_eq!(
        recorder.get("sundog_fan_out_backlog", Some("closed-backlog-replicated")),
        Some(0.0)
    );

    let local = Shard::<u32, String>::new(
        SmolStr::new("closed-backlog-local"),
        Mode::Local,
        NodeId::from(1),
        u64::MAX,
        None,
        None,
    );
    local.insert(1, "a".into()).await.expect("insert");
    local.fan_out_queue().close();
    assert_eq!(
        recorder.get("sundog_fan_out_backlog", Some("closed-backlog-local")),
        None
    );
}

/// A closed spill cache reads no spilled entries and no spill bytes, and a
/// spilled key a straggling read promotes afterwards moves neither gauge.
#[cfg(feature = "spill")]
#[tokio::test]
async fn a_closed_spill_cache_reads_no_spilled_entries_or_bytes() {
    let recorder = Gauges::default();
    let _metrics = metrics::set_default_local_recorder(&recorder);
    let dir = std::env::temp_dir().join(format!(
        "sundog-closed-gauges-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = spill::SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let name = "closed-spill-gauges";
    let shard = Shard::<u32, String>::new(
        SmolStr::new(name),
        Mode::Local,
        NodeId::from(1),
        3,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("the tier opens");
    for k in 0..20u32 {
        shard.insert(k, format!("value-{k}")).await.expect("insert");
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while recorder
        .get("sundog_spill_entries", Some(name))
        .is_none_or(|entries| entries < 1.0)
    {
        assert!(std::time::Instant::now() < deadline, "an entry spills");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The flusher thread has no recorder of its own; publish from here.
    shard.engine.spill().expect("a tier").publish_bytes_used();
    assert!(
        recorder
            .get("sundog_spill_bytes_used", Some(name))
            .is_some_and(|bytes| bytes > 0.0)
    );

    shard.seal_for_close();
    shard.close_spill_checkpointed().await;

    assert_eq!(recorder.get("sundog_spill_entries", Some(name)), Some(0.0));
    assert_eq!(
        recorder.get("sundog_spill_bytes_used", Some(name)),
        Some(0.0)
    );
    // A read of a spilled key after the close finds the tier closed or
    // promotes it; either way the gauge stays at the zero.
    for k in 0..20u32 {
        let _ = shard.get(&k).await;
    }
    assert_eq!(recorder.get("sundog_spill_entries", Some(name)), Some(0.0));
    let _ = std::fs::remove_dir_all(&dir);
}
