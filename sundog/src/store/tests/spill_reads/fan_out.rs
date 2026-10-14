//! What the fan-out drain reads for a write whose entry spilled before it was sent.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use super::*;

/// Drained queue, one newest write per key.
fn drained(shard: &Shard<u32, String>) -> Vec<(u32, Hlc)> {
    newest_per_key(applied_writes(shard.fan_out_queue().drain()))
}

/// Two writes under `max_capacity == 1`, so one spills; the shard, the
/// spilled key, the resident key, the drained writes and the scratch
/// directory.
async fn two_writes_one_spilled(
    label: &str,
    mode: Mode,
) -> (Shard<u32, String>, u32, u32, Vec<(u32, Hlc)>, PathBuf) {
    let (shard, dir) = spill_shard_in(label, "spill-fan-out", mode, 1);
    shard.insert(1, "one".to_string()).await.expect("insert 1");
    shard.insert(2, "two".to_string()).await.expect("insert 2");
    assert!(
        poll_until(POLL_TIMEOUT, || {
            shard
                .engine
                .spill()
                .is_some_and(|tier| tier.bytes_used() > 0)
        })
        .await,
        "eviction spills one of the two keys within the poll bound"
    );
    let (spilled, resident) = if shard.get_sync(&1).is_none() {
        (1, 2)
    } else {
        (2, 1)
    };
    let writes = drained(&shard);
    (shard, spilled, resident, writes, dir)
}

fn encoded(value: &str) -> Vec<u8> {
    postcard::to_stdvec(&value.to_string()).expect("test value encodes")
}

#[tokio::test]
async fn a_spilled_written_key_fans_out_its_record_read_from_disk() {
    let (shard, spilled, resident, writes, dir) =
        two_writes_one_spilled("fan-out-spilled-record", Mode::Replicated).await;
    let queued = |key: u32| writes.iter().find(|w| w.0 == key).expect("queued").1;

    let fan = shard.fan_out_records(&writes).await;

    assert!(fan.departed.is_empty(), "nothing departed");
    assert_eq!(fan.records.len(), 2);
    let record_of = |key: u32| {
        fan.records
            .iter()
            .find(|rec| rec.key == key_bytes(&key))
            .expect("a record for the key")
    };
    let spilled_value = if spilled == 1 { "one" } else { "two" };
    assert_eq!(
        record_of(spilled).value.as_deref(),
        Some(encoded(spilled_value).as_slice()),
        "the spilled value is read off disk"
    );
    assert_eq!(record_of(spilled).ver, queued(spilled));
    assert_eq!(record_of(resident).ver, queued(resident));
    assert_eq!(
        shard.get_sync(&spilled),
        None,
        "the fan-out never promotes a spilled entry"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_non_merging_invalidation_cache_reads_no_disk_for_a_spilled_write() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (shard, spilled, resident, writes, dir) =
        two_writes_one_spilled("fan-out-invalidation-spilled", Mode::Invalidation).await;

    let fan = shard.fan_out_records(&writes).await;

    assert_eq!(fan.records.len(), 1, "only the resident write is a record");
    assert_eq!(fan.records[0].key, key_bytes(&resident));
    assert_eq!(fan.departed.len(), 1, "the spilled write invalidates peers");
    assert_eq!(fan.departed[0].key, key_bytes(&spilled));
    assert_eq!(
        fan.departed[0].ver,
        writes.iter().find(|w| w.0 == spilled).expect("queued").1
    );
    let reads: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| match (key.key().name(), value) {
            ("sundog_spill_reads_total", DebugValue::Counter(n)) => Some(n),
            _ => None,
        })
        .sum();
    assert_eq!(reads, 0, "a value nobody receives is not read");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_spilled_write_that_cannot_be_read_back_is_departed() {
    let (shard, spilled, resident, writes, dir) =
        two_writes_one_spilled("fan-out-unreadable", Mode::Replicated).await;
    shard
        .spill_read
        .get()
        .expect("a spill tier")
        .semaphore
        .close();

    let fan = shard.fan_out_records(&writes).await;

    assert_eq!(fan.records.len(), 1);
    assert_eq!(fan.records[0].key, key_bytes(&resident));
    assert_eq!(fan.departed.len(), 1, "the unreadable write invalidates");
    assert_eq!(fan.departed[0].key, key_bytes(&spilled));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_lapsed_spilled_write_is_departed_without_a_disk_read() {
    let clock = Arc::new(AtomicU64::new(1_000_000));
    let reader = Arc::clone(&clock);
    let dir = temp_dir("fan-out-lapsed-spilled");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let shard = Shard::<u32, String>::new(
        SmolStr::new("spill-fan-out-lapsed"),
        Mode::Replicated,
        NodeId::from(1u64),
        1,
        None,
        None,
    )
    .with_clock(Arc::new(move || reader.load(AtomicOrdering::SeqCst)))
    .with_spill(&cfg)
    .expect("tier opens");
    shard
        .insert_with_ttl(1, "short".to_string(), Duration::from_millis(50))
        .await
        .expect("insert");
    shard.insert(2, "two".to_string()).await.expect("insert");
    assert!(
        poll_until(POLL_TIMEOUT, || {
            shard
                .engine
                .spill()
                .is_some_and(|tier| tier.bytes_used() > 0)
        })
        .await
    );
    assert_eq!(shard.get_sync(&1), None, "key 1 spilled");
    let writes = drained(&shard);
    clock.store(1_000_100, AtomicOrdering::SeqCst);

    let fan = shard.fan_out_records(&writes).await;

    assert_eq!(fan.records.len(), 1, "only the live write is a record");
    assert_eq!(fan.records[0].key, key_bytes(&2u32));
    assert_eq!(fan.departed.len(), 1);
    assert_eq!(fan.departed[0].key, key_bytes(&1u32));
    let _ = std::fs::remove_dir_all(&dir);
}
