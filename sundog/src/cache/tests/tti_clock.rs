//! A clock behind the last read never idles out a recently read entry under `tti`.
//!
//! The cache reads a clock the test moves, so a backward step is exact and no
//! test waits out an idle timeout.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::store::crdt::WriterId;

/// How far the test steps the clock back.
const STEP_BACK_MS: u64 = 500;

/// The idle timeout, far longer than the step so only the step could idle an entry out.
const TTI: Duration = Duration::from_secs(30);

const KEY: u32 = 1;

type Clock = Arc<AtomicU64>;

fn manual_clock() -> Clock {
    Arc::new(AtomicU64::new(now_ms() + 3_600_000))
}

fn step_back(clock: &Clock) {
    clock.fetch_sub(STEP_BACK_MS, Ordering::SeqCst);
}

async fn solo(name: &'static str) -> Cluster {
    Cluster::builder(name)
        .config(loopback_config())
        .seeds(std::iter::empty())
        .build()
        .await
        .expect("node builds")
}

fn reading(clock: &Clock) -> Arc<dyn Fn() -> u64 + Send + Sync> {
    let reader = Arc::clone(clock);
    Arc::new(move || reader.load(Ordering::SeqCst))
}

#[tokio::test]
async fn a_backward_clock_step_keeps_a_recently_read_entry_under_tti() {
    let clock = manual_clock();
    let cluster = solo("cache-it-tti-clock-step-get").await;
    let cache = cluster
        .cache::<u32, String>("cache-it-tti-clock-step-get")
        .mode(Mode::Local)
        .clock(reading(&clock))
        .tti(TTI)
        .open()
        .await
        .expect("opens");
    cache.insert(KEY, "v".to_string()).await.expect("insert");
    assert_eq!(cache.get(&KEY).await, Some("v".to_string()));

    step_back(&clock);

    assert_eq!(
        cache.get(&KEY).await,
        Some("v".to_string()),
        "get after the step"
    );
    assert!(
        cache.contains_key(&KEY).await,
        "contains_key after the step"
    );
    assert!(
        cache.contains_key_sync(&KEY),
        "contains_key_sync after the step"
    );
    assert_eq!(cache.entry_count().await, 1, "entry_count after the step");
    assert_eq!(cache.keys(), vec![KEY], "keys after the step");
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_backward_clock_step_does_not_reset_a_merged_counter_under_tti() {
    let clock = manual_clock();
    let cluster = solo("cache-it-tti-clock-step-merge").await;
    let cache = cluster
        .cache::<u32, PnCounter>("cache-it-tti-clock-step-merge")
        .mode(Mode::Local)
        .clock(reading(&clock))
        .tti(TTI)
        .resolver(Arc::new(PnCounterResolver))
        .open()
        .await
        .expect("opens");
    cache
        .merge(KEY, PnCounter::local_delta(cache.writer_id(), 3))
        .await
        .expect("merge");
    assert_eq!(cache.get(&KEY).await.map(|c| c.value()), Some(3));

    step_back(&clock);

    let other = WriterId::new(NodeId::from(77u64), 1);
    cache
        .merge(KEY, PnCounter::local_delta(other, 4))
        .await
        .expect("merge after the step");
    assert_eq!(
        cache.get(&KEY).await.map(|c| c.value()),
        Some(7),
        "both contributions survive: the counter just read is not idle"
    );
    cluster.shutdown().await;
}
