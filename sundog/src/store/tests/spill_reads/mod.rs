//! Shard-layer spill read path tests that go through a real spill tier.

mod fan_out;
mod pressure;

use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use super::*;
use crate::store::spill::{SpillConfig, SpillLoc, SpillSink};

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sundog-shard-spill-test-{label}-{}-{:?}",
        std::process::id(),
        std::thread::current().id(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Polls `cond` until it returns `true` or `timeout` elapses,
/// returning the final result either way. Never a fixed sleep.
async fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return cond();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

const POLL_TIMEOUT: Duration = Duration::from_secs(5);

/// Takes every one of `shard`'s spill read permits, so no disk read
/// of its own starts until the returned permit drops.
async fn hold_spill_reads(shard: &Shard<u32, String>) -> tokio::sync::OwnedSemaphorePermit {
    let semaphore = Arc::clone(&shard.spill_read.get().expect("a spill tier").semaphore);
    let permits = u32::try_from(semaphore.available_permits()).expect("a small count");
    semaphore
        .acquire_many_owned(permits)
        .await
        .expect("the read semaphore is open")
}

/// A `Mode::Local` shard with a real spill tier attached, under
/// `max_capacity`. Returns the shard and its scratch directory,
/// cleaned up by the caller.
fn spill_shard(label: &str, max_capacity: u64) -> (Shard<u32, String>, PathBuf) {
    spill_shard_in(label, "spill-test", Mode::Local, max_capacity)
}

/// [`spill_shard`] named `name`, which labels its metrics, under
/// `mode`.
fn spill_shard_in(
    label: &str,
    name: &str,
    mode: Mode,
    max_capacity: u64,
) -> (Shard<u32, String>, PathBuf) {
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let shard = Shard::<u32, String>::new(
        SmolStr::new(name),
        mode,
        NodeId::from(1u64),
        max_capacity,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    (shard, dir)
}

/// `Shard::attach_spill` sets the tier's `keep_resident_when_refused`
/// policy from the shard's own `Mode`: `true` for `Mode::Replicated`
/// and `Mode::Distributed`, since every peer, or every co-owner,
/// still holds the entry and a local delete would just have
/// anti-entropy, or a rebalance donor pull, repair it back in,
/// `false` for `Mode::Local`, which has no peer to repair from and
/// so deletes a refused victim on a write.
#[test]
fn with_spill_sets_keep_resident_when_refused_from_mode() {
    let dir = temp_dir("keep-resident-replicated");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let replicated = Shard::<u32, String>::new(
        SmolStr::new("spill-test-replicated"),
        Mode::Replicated,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    assert!(
        replicated
            .engine
            .spill()
            .expect("with_spill attaches a tier")
            .keep_resident_when_refused(),
        "Mode::Replicated sets the tier's keep_resident_when_refused policy"
    );
    let _ = std::fs::remove_dir_all(&dir);

    let dir = temp_dir("keep-resident-distributed");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let distributed = Shard::<u32, String>::new(
        SmolStr::new("spill-test-distributed"),
        Mode::Distributed {
            owners: NonZeroU8::new(2).expect("nonzero"),
        },
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    assert!(
        distributed
            .engine
            .spill()
            .expect("with_spill attaches a tier")
            .keep_resident_when_refused(),
        "Mode::Distributed sets the tier's keep_resident_when_refused policy too"
    );
    let _ = std::fs::remove_dir_all(&dir);

    let dir = temp_dir("keep-resident-local");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let local = Shard::<u32, String>::new(
        SmolStr::new("spill-test-local"),
        Mode::Local,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    )
    .with_spill(&cfg)
    .expect("tier opens");
    assert!(
        !local
            .engine
            .spill()
            .expect("with_spill attaches a tier")
            .keep_resident_when_refused(),
        "Mode::Local leaves the tier's keep_resident_when_refused policy at its default"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn release_buckets_corrects_the_spilled_entries_gauge_on_a_shard_with_spill() {
    let (s, dir) = spill_shard("release-buckets-spill", 1);
    s.insert(1, "one".to_string()).await.expect("insert 1");
    s.insert(2, "two".to_string()).await.expect("insert 2");
    assert!(
        poll_until(POLL_TIMEOUT, || s.engine.debug_spill_entries_count() > 0).await,
        "one of the two entries spills under a capacity of 1"
    );

    let bucket1 = bucket_of(&key_bytes(&1u32));
    let bucket2 = bucket_of(&key_bytes(&2u32));
    let removed = ShardOps::release_buckets(&s, &[bucket1, bucket2]).await;

    assert_eq!(
        removed, 2,
        "both entries removed, whichever one had spilled"
    );
    assert_eq!(
        s.engine.debug_spill_entries_count(),
        0,
        "release_buckets corrects the spilled-entries gauge for its own bucket set"
    );
    assert!(s.get(&1).await.is_none());
    assert!(s.get(&2).await.is_none());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Inserts two entries under `max_capacity == 1`, forcing eviction to
/// spill one of them, then waits up to a bound for the flusher to
/// install it. This is observed through the tier's own
/// `bytes_used`, so it never depends on which of the two keys the
/// sampler picked. Returns `(shard, spilled_key, spilled_value,
/// resident_key, resident_value, dir)`.
async fn shard_with_one_spilled_entry(
    label: &str,
) -> (Shard<u32, String>, u32, String, u32, String, PathBuf) {
    let (shard, dir) = spill_shard(label, 1);
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
        "eviction spills exactly one of the two keys within the poll bound"
    );
    if shard.get_sync(&1).is_none() {
        (shard, 1, "one".to_string(), 2, "two".to_string(), dir)
    } else {
        (shard, 2, "two".to_string(), 1, "one".to_string(), dir)
    }
}

/// `local_record` of a spilled key reports it from its pointer: no
/// spill read is counted or timed, and the entry stays spilled, so
/// `get_sync` still misses it; a `get` of it then reads it from disk
/// and is counted.
#[tokio::test]
async fn local_record_of_a_spilled_key_reads_no_disk_and_does_not_promote() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (shard, spilled_key, spilled_value, resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("local-record-spilled").await;
    // Disk reads counted and timed since the last call: a snapshot
    // drains both.
    let reads = || {
        let (mut counted, mut timed) = (0, 0);
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            match (key.key().name(), value) {
                ("sundog_spill_reads_total", DebugValue::Counter(n)) => counted += n,
                ("sundog_spill_read_duration_seconds", DebugValue::Histogram(v)) => {
                    timed += v.len();
                }
                _ => {}
            }
        }
        (counted, timed)
    };
    assert_eq!(reads(), (0, 0));

    let spilled = shard.local_record(&key_bytes(&spilled_key)).1;
    let resident = shard.local_record(&key_bytes(&resident_key)).1;
    assert!(
        matches!(spilled, LocalRecord::Live { spilled: true, .. }),
        "{spilled:?}"
    );
    assert!(
        matches!(resident, LocalRecord::Live { spilled: false, .. }),
        "{resident:?}"
    );
    assert_eq!(reads(), (0, 0), "no disk read");
    assert_eq!(
        shard.get_sync(&spilled_key),
        None,
        "the key is still spilled"
    );

    assert_eq!(shard.get(&spilled_key).await, Some(spilled_value));
    assert_eq!(reads(), (1, 1), "a get of it reads the disk");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A write past the cap spills an older entry, never itself: under
/// a cap of one, the second key stays resident and the first spills.
#[tokio::test]
async fn a_write_past_the_cap_spills_an_older_entry_rather_than_itself() {
    let (shard, dir) = spill_shard("spills-the-older", 1);
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
        "one key spills within the poll bound"
    );
    assert_eq!(
        shard.get_sync(&2),
        Some("two".to_string()),
        "the write stays resident"
    );
    assert_eq!(shard.get_sync(&1), None, "the older key spilled");
    assert_eq!(shard.get(&1).await, Some("one".to_string()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn get_sync_misses_a_spilled_key_while_get_promotes_it() {
    let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("get-sync-vs-get").await;

    assert_eq!(
        shard.get_sync(&spilled_key),
        None,
        "get_sync never touches disk; a spilled key reads as a miss"
    );
    assert_eq!(
        shard.get(&spilled_key).await,
        Some(spilled_value.clone()),
        "get reads the spilled value back off disk"
    );
    assert_eq!(
        shard.get_sync(&spilled_key),
        Some(spilled_value),
        "the disk read promoted the key back to residency"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The re-arm is a write over the capacity of one entry, so the
/// eviction it triggers can spill the entry straight back: the value
/// stays readable either way and keeps the new lifetime.
#[tokio::test]
async fn expire_reads_a_spilled_value_back_and_ttl_of_needs_no_disk_read() {
    let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("expire-spilled").await;

    assert_eq!(shard.ttl_of(&spilled_key), Some(Ttl::Never));
    assert_eq!(
        shard.get_sync(&spilled_key),
        None,
        "reading the lifetime left the entry spilled"
    );
    assert!(
        shard
            .expire(&spilled_key, Duration::from_secs(60))
            .await
            .expect("expire")
    );
    assert!(matches!(
        shard.ttl_of(&spilled_key),
        Some(Ttl::Remaining(left)) if left <= Duration::from_secs(60)
            && left > Duration::from_secs(50)
    ));
    assert_eq!(
        shard.get(&spilled_key).await,
        Some(spilled_value),
        "the value survives the re-arm, resident or spilled again"
    );
    assert!(matches!(
        shard.ttl_of(&spilled_key),
        Some(Ttl::Remaining(left)) if left <= Duration::from_secs(60)
            && left > Duration::from_secs(50)
    ));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn contains_key_and_contains_key_sync_both_see_a_spilled_key_with_zero_disk_reads() {
    let (shard, spilled_key, _spilled_value, _resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("contains-key").await;

    assert!(
        shard.contains_key_sync(&spilled_key),
        "existence doesn't need the value bytes"
    );
    assert!(shard.contains_key(&spilled_key).await);
    // Neither existence check triggered a promotion: still spilled.
    assert_eq!(
        shard.get_sync(&spilled_key),
        None,
        "contains_key/contains_key_sync never read the disk"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn get_or_load_promotes_on_hit_without_a_duplicate_loader_call() {
    let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("get-or-load").await;

    let calls = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&calls);
    let loaded = shard
        .get_or_load(
            &spilled_key,
            async move |_key: &u32| -> Result<String, std::convert::Infallible> {
                c.fetch_add(1, Ordering::SeqCst);
                Ok("should-not-run".to_string())
            },
        )
        .await
        .expect("resolves via the spilled-read path");
    assert_eq!(loaded, spilled_value.clone());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the loader never runs once the disk read already found the key"
    );
    assert_eq!(
        shard.get_sync(&spilled_key),
        Some(spilled_value),
        "the owner's disk read promoted the key back to residency"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn get_or_insert_with_promotes_on_hit() {
    let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("get-or-insert-with").await;

    let made = shard
        .get_or_insert_with(&spilled_key, async move |_key: &u32| {
            "should-not-run".to_string()
        })
        .await
        .expect("resolves via the spilled-read path");
    assert_eq!(made, spilled_value.clone());
    assert_eq!(shard.get_sync(&spilled_key), Some(spilled_value));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Each disk read is counted and timed once, a batched read
/// (`snapshot_chunks`) and a read of its own (`get`) alike.
#[tokio::test]
async fn every_disk_read_is_counted_and_timed_once() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (shard, spilled_key, spilled_value, _resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("timed-reads").await;
    // Disk reads counted and timed since the last call: a snapshot
    // drains both.
    let reads = || {
        let (mut counted, mut timed) = (0, 0);
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            match (key.key().name(), value) {
                ("sundog_spill_reads_total", DebugValue::Counter(n)) => counted += n,
                ("sundog_spill_read_duration_seconds", DebugValue::Histogram(v)) => {
                    timed += v.len();
                }
                _ => {}
            }
        }
        (counted, timed)
    };
    assert_eq!(reads(), (0, 0));

    let records: Vec<WireRecord> = ShardOps::snapshot_chunks(&shard)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(
        records.len(),
        2,
        "both records, the spilled one read from disk"
    );
    assert_eq!(reads(), (1, 1), "the batched read is counted and timed");

    assert_eq!(shard.get(&spilled_key).await, Some(spilled_value));
    assert_eq!(reads(), (1, 1), "the get's own read is counted and timed");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn snapshot_chunks_folds_in_the_spilled_value_from_disk_without_promoting_it() {
    let (shard, spilled_key, spilled_value, resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("snapshot-chunks").await;

    // The state-transfer donor path: a peer joining fresh drives
    // this to pull every record this node holds, spilled ones
    // included.
    let chunks: Vec<Vec<WireRecord>> = ShardOps::snapshot_chunks(&shard).collect().await;
    let records: Vec<WireRecord> = chunks.into_iter().flatten().collect();

    let spilled_key_bytes = encode_key(&spilled_key).expect("key encodes");
    let record = records
        .iter()
        .find(|r| r.key.as_ref() == spilled_key_bytes.as_ref())
        .expect("the donor snapshot includes the spilled key's record");
    assert_eq!(
        record.value.as_deref(),
        Some(
            postcard::to_stdvec(&spilled_value)
                .expect("test value encodes")
                .as_slice()
        ),
        "the chunk carries the spilled value, read from disk"
    );

    let resident_key_bytes = encode_key(&resident_key).expect("key encodes");
    assert!(
        records
            .iter()
            .any(|r| r.key.as_ref() == resident_key_bytes.as_ref()),
        "the resident key's record is included too"
    );

    assert_eq!(
        shard.get_sync(&spilled_key),
        None,
        "the donor path never promotes: the entry stays Spilled after being read for \
         the snapshot"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn insert_on_a_spilled_key_discards_the_pending_spill() {
    let (shard, spilled_key, _old_value, resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("insert-over-spilled").await;

    // Frees the other live key's weight first, so the capacity-1
    // shard has room for the fresh write below without immediately
    // sampling it back out again: this test is about what one write
    // installs, not a second race against `enforce_capacity`.
    shard.remove(&resident_key).await.expect("remove");

    shard
        .insert(spilled_key, "fresh".to_string())
        .await
        .expect("insert");
    assert_eq!(
        shard.get_sync(&spilled_key),
        Some("fresh".to_string()),
        "a fresh write always installs resident, discarding whatever was spilled"
    );
    assert!(shard.contains_key_sync(&spilled_key));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn remove_on_a_spilled_key_prevents_a_late_flush_from_resurrecting_it() {
    // No real tier needed here: this drives `SpillSink::install`
    // directly, the call a real flusher makes, to deterministically
    // simulate a flush of the pre-removal value landing *after*
    // the remove. This is the narrow race a real disk's timing
    // cannot be made to land on demand.
    let shard = Shard::<u32, String>::new(
        SmolStr::new("spill-remove-race"),
        Mode::Local,
        NodeId::from(1u64),
        u64::MAX,
        None,
        None,
    );
    shard
        .insert(1, "original".to_string())
        .await
        .expect("insert");
    shard.remove(&1).await.expect("remove");
    assert_eq!(shard.get_sync(&1), None);

    let key_bytes = Bytes::from(postcard::to_stdvec(&1u32).expect("key encodes"));
    let hash = engine::hash_key_bytes(key_bytes.as_ref());
    let bucket = engine::stripe_index_from_hash(hash);
    let installed = shard.engine.install(
        bucket,
        &key_bytes,
        hash,
        Hlc {
            wall_ms: 1,
            logical: 0,
            node: NodeId::from(1u64),
        },
        SpillLoc {
            region: 0,
            offset: 0,
            len: 4,
            generation: 0,
        },
        0,
    );
    assert!(!installed, "a late flush never resurrects a removed key");
    assert_eq!(shard.get_sync(&1), None, "the tombstone still wins");
    assert!(!shard.contains_key_sync(&1));
}

#[tokio::test]
async fn invalidate_local_on_a_spilled_key_removes_it_unconditionally() {
    let (shard, spilled_key, _spilled_value, resident_key, resident_value, dir) =
        shard_with_one_spilled_entry("invalidate-local").await;

    shard.invalidate_local(&spilled_key).await;
    assert_eq!(shard.get_sync(&spilled_key), None);
    assert!(!shard.contains_key_sync(&spilled_key));
    // The untouched, still-resident key survives.
    assert_eq!(shard.get_sync(&resident_key), Some(resident_value));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn clear_tombstones_a_spilled_key() {
    let (shard, spilled_key, _spilled_value, resident_key, _resident_value, dir) =
        shard_with_one_spilled_entry("clear").await;

    shard.clear().await.expect("clear");
    assert_eq!(shard.get_sync(&spilled_key), None);
    assert!(!shard.contains_key_sync(&spilled_key));
    assert_eq!(shard.get_sync(&resident_key), None);
    assert!(!shard.contains_key_sync(&resident_key));

    let _ = std::fs::remove_dir_all(&dir);
}
