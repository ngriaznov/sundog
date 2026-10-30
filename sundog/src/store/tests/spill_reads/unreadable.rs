//! Tests for a spilled entry whose record a read cannot use: one that fails its checksum, lies past the end of its region file or no longer decodes is dropped, so the node stops counting and advertising it, and one behind any other I/O error is kept for the next read.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use tracing_subscriber::layer::SubscriberExt as _;

use super::*;

/// A `Mode::Replicated` shard over a fresh tier, holding `1 => "one"` and
/// `2 => "two"` with exactly one of them spilled, and the spilled one.
async fn replicated_with_one_spilled(label: &str) -> (Shard<u32, String>, u32, String, PathBuf) {
    let (shard, dir) = spill_shard_in(label, "unreadable", Mode::Replicated, 1);
    let (shard, key, value) = with_one_spilled(shard).await;
    (shard, key, value, dir)
}

/// `shard`, a spilling shard that holds one entry in RAM, holding `1 =>
/// "one"` and `2 => "two"` with exactly one of them spilled, and the spilled
/// one.
async fn with_one_spilled(shard: Shard<u32, String>) -> (Shard<u32, String>, u32, String) {
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
        "one key spills"
    );
    if shard.get_sync(&1).is_none() {
        (shard, 1, "one".to_string())
    } else {
        (shard, 2, "two".to_string())
    }
}

/// Every region file under `dir`.
fn region_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).expect("read dir").flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|ext| ext == "reg") {
                files.push(p);
            }
        }
    }
    files
}

/// Flips one byte of `value`'s encoding in the only region file under `dir`
/// that holds it, leaving every length field intact, so the record still
/// frames and only its checksum fails.
fn corrupt_on_disk(dir: &Path, value: &str) {
    let needle = value.as_bytes();
    let mut flipped = 0;
    for path in region_files(dir) {
        let mut bytes = std::fs::read(&path).expect("read region");
        if let Some(at) = bytes.windows(needle.len()).position(|w| w == needle) {
            bytes[at] ^= 0x01;
            std::fs::write(&path, &bytes).expect("write region");
            flipped += 1;
        }
    }
    assert_eq!(
        flipped, 1,
        "exactly one region file holds the spilled value"
    );
}

/// `sundog_spill_reads_total` by `outcome` since the last snapshot, from one
/// snapshot: taking one resets every counter.
fn reads(snapshotter: &Snapshotter) -> HashMap<String, u64> {
    let mut by_outcome = HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        if key.key().name() != "sundog_spill_reads_total" {
            continue;
        }
        let DebugValue::Counter(n) = value else {
            panic!("a counter, not {value:?}");
        };
        let outcome = key
            .key()
            .labels()
            .find(|label| label.key() == "outcome")
            .map(|label| label.value().to_string())
            .expect("every read has an outcome");
        *by_outcome.entry(outcome).or_insert(0) += n;
    }
    by_outcome.retain(|_, n| *n > 0);
    by_outcome
}

/// `outcome => count` pairs, for comparing with [`reads`].
fn counts(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
    pairs
        .iter()
        .map(|&(outcome, n)| (outcome.to_string(), n))
        .collect()
}

#[tokio::test]
async fn a_get_of_a_spilled_record_that_fails_its_checksum_drops_the_entry() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (shard, key, value, dir) = replicated_with_one_spilled("checksum-get").await;
    let resident = if key == 1 { 2 } else { 1 };
    corrupt_on_disk(&dir, &value);
    let digests_before = shard.engine.digests();
    let (count_before, _) = shard.engine.debug_totals();
    let _ = snapshotter.snapshot();

    assert_eq!(shard.get(&key).await, None, "a corrupt record is a miss");
    assert_eq!(reads(&snapshotter), counts(&[("corrupt", 1)]));
    assert!(
        !shard.contains_key(&key).await,
        "the node no longer counts the key it cannot serve"
    );
    assert_eq!(shard.engine.debug_totals().0, count_before - 1);
    assert_ne!(
        shard.engine.digests(),
        digests_before,
        "nor advertises it, so a peer's copy no longer compares equal"
    );
    assert_eq!(
        shard.engine.tombstone_count(),
        0,
        "and no tombstone deletes that copy"
    );
    assert_eq!(shard.get(&key).await, None);
    assert!(
        reads(&snapshotter).is_empty(),
        "a later get reads nothing off disk"
    );
    assert!(
        shard.get(&resident).await.is_some(),
        "the other key is untouched"
    );

    shard
        .insert(key, "again".to_string())
        .await
        .expect("insert");
    assert_eq!(
        shard.get(&key).await,
        Some("again".to_string()),
        "the key takes a write as any absent key does"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_batch_read_of_a_record_that_fails_its_checksum_leaves_it_out_and_drops_the_entry() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (shard, key, value, dir) = replicated_with_one_spilled("checksum-batch").await;
    corrupt_on_disk(&dir, &value);
    let (count_before, _) = shard.engine.debug_totals();
    let _ = snapshotter.snapshot();

    // The state-transfer donor path reads spilled records in one batch, as
    // anti-entropy replies and fetch answers do.
    let records: Vec<WireRecord> = ShardOps::snapshot_chunks(&shard)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect();
    let key_bytes = encode_key(&key).expect("key encodes");
    assert!(
        records.iter().all(|r| r.key.as_ref() != key_bytes.as_ref()),
        "the unreadable record is not sent"
    );
    assert_eq!(records.len(), 1, "the readable one is");
    assert_eq!(reads(&snapshotter), counts(&[("corrupt", 1)]));
    assert!(!shard.contains_key(&key).await, "the entry is dropped");
    assert_eq!(shard.engine.debug_totals().0, count_before - 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_spilled_record_past_the_end_of_a_region_cut_short_drops_the_entry() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (shard, key, _value, dir) = replicated_with_one_spilled("cut-short").await;
    for path in region_files(&dir) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open region")
            .set_len(0)
            .expect("truncate region");
    }
    let _ = snapshotter.snapshot();

    assert_eq!(shard.get(&key).await, None, "the record is not there");
    assert_eq!(
        reads(&snapshotter),
        counts(&[("corrupt", 1)]),
        "a later read would not find it either, so it is corrupt, not an I/O error"
    );
    assert!(!shard.contains_key(&key).await, "the entry is dropped");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_spilled_record_behind_an_io_error_is_kept_for_the_next_read() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (shard, key, value, dir) = replicated_with_one_spilled("io-error").await;
    let tier = Arc::clone(shard.engine.spill().expect("a tier"));
    tier.fail_reads(true);
    let digests_before = shard.engine.digests();
    let _ = snapshotter.snapshot();

    assert_eq!(shard.get(&key).await, None, "a failed read is a miss");
    assert_eq!(reads(&snapshotter), counts(&[("io_error", 1)]));
    assert!(
        shard.contains_key(&key).await,
        "an I/O error may pass, so the entry stays for the next read"
    );
    assert_eq!(shard.engine.digests(), digests_before);

    tier.fail_reads(false);
    assert_eq!(
        shard.get(&key).await,
        Some(value),
        "and the next read finds it"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A tier config for a warm reopen in a fresh directory, and the directory.
fn warm_config(label: &str) -> (SpillConfig, PathBuf) {
    let dir = temp_dir(label);
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    (cfg, dir)
}

/// The shard `name` reopened warm as `Shard<u32, String>` over `cfg` after a
/// clean close as `Shard<u32, Vec<u8>>` holding `keys`, each with bytes that
/// are a fine `Vec<u8>` and not valid UTF-8, so not a `String`.
async fn reopened_across_a_value_type_change(
    cfg: &SpillConfig,
    name: &str,
    keys: &[u32],
) -> Shard<u32, String> {
    let node = NodeId::from(41u64);
    let before = Shard::<u32, Vec<u8>>::new(
        SmolStr::new(name),
        Mode::Replicated,
        node,
        1 << 20,
        None,
        None,
    );
    before
        .attach_spill(cfg)
        .expect("a brand-new directory opens cold");
    for &key in keys {
        before.insert(key, vec![0xFF, 0xFE]).await.expect("insert");
    }
    before.close_spill_checkpointed().await;
    let after = Shard::<u32, String>::new(
        SmolStr::new(name),
        Mode::Replicated,
        node,
        1 << 20,
        None,
        None,
    );
    after
        .attach_spill(cfg)
        .expect("the checkpoint reopens warm");
    for key in keys {
        assert!(
            after.contains_key(key).await,
            "the replayed entry is counted"
        );
    }
    after
}

#[tokio::test]
async fn a_get_after_a_warm_reopen_across_a_value_type_change_drops_the_entry() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (cfg, dir) = warm_config("value-type-get");
    let after = reopened_across_a_value_type_change(&cfg, "type-change-get", &[5]).await;
    let _ = snapshotter.snapshot();

    assert_eq!(
        after.get(&5).await,
        None,
        "its value does not decode as a String"
    );
    assert_eq!(reads(&snapshotter), counts(&[("corrupt", 1)]));
    assert!(
        !after.contains_key(&5).await,
        "the entry is dropped, so a peer's copy of the new type can replace it"
    );
    assert_eq!(after.engine.debug_totals().0, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_batch_read_after_a_warm_reopen_across_a_value_type_change_drops_the_entry() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (cfg, dir) = warm_config("value-type-batch");
    let after = reopened_across_a_value_type_change(&cfg, "type-change-batch", &[5]).await;
    let _ = snapshotter.snapshot();

    let records: Vec<WireRecord> = ShardOps::snapshot_chunks(&after)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect();
    assert!(
        records.is_empty(),
        "a peer is not sent bytes it cannot decode"
    );
    assert_eq!(reads(&snapshotter), counts(&[("corrupt", 1)]));
    assert!(!after.contains_key(&5).await, "the entry is dropped");
    let _ = std::fs::remove_dir_all(&dir);
}

/// One logged event: its level and its fields, `message` among them.
#[derive(Debug)]
struct Logged {
    level: tracing::Level,
    fields: HashMap<String, String>,
}

/// A tracing layer that keeps every event it sees.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<Logged>>>);

/// Collects every field of one event, formatted.
struct Fields(HashMap<String, String>);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Log {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = Fields(HashMap::new());
        event.record(&mut fields);
        self.0.lock().expect("lock").push(Logged {
            level: *event.metadata().level(),
            fields: fields.0,
        });
    }
}

#[tokio::test]
async fn the_first_unreadable_drop_in_a_cache_warns_and_later_ones_log_at_debug() {
    let log = Log::default();
    let _subscriber = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(log.clone()),
    );
    let (cfg, dir) = warm_config("unreadable-log");
    let keys = [5u32, 6];
    let after = reopened_across_a_value_type_change(&cfg, "unreadable-log", &keys).await;
    log.0.lock().expect("lock").clear();

    for key in keys {
        assert_eq!(after.get(&key).await, None);
    }
    assert_eq!(after.get(&5).await, None, "a dropped key is not read again");
    let drops: Vec<Logged> = std::mem::take(&mut *log.0.lock().expect("lock"))
        .into_iter()
        .filter(|event| {
            event
                .fields
                .get("message")
                .is_some_and(|message| message.contains("spilled entry"))
        })
        .collect();
    let levels: Vec<tracing::Level> = drops.iter().map(|event| event.level).collect();
    assert_eq!(
        levels,
        [tracing::Level::WARN, tracing::Level::DEBUG],
        "one warning for the cache, then debug lines: {drops:?}"
    );
    let key_len = encode_key(&5u32).expect("key encodes").len().to_string();
    for event in &drops {
        assert_eq!(
            event.fields.get("key_len"),
            Some(&key_len),
            "each names the key's length"
        );
        assert!(
            !event.fields.contains_key("key"),
            "and never the key: {event:?}"
        );
        assert_eq!(
            event.fields.get("why"),
            Some(&format!(
                "{:?}",
                "does not decode as the cache's value type"
            )),
            "and the reason"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A resolver that reads both records' values, as a value-aware one does,
/// and keeps the greater version, so a conflict resolution reads a spilled
/// stored side off disk.
struct ReadsValues;

impl ConflictResolver for ReadsValues {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }
}

/// `key`'s version on `shard`, where it is spilled.
fn spilled_ver<V>(shard: &Shard<u32, V>, key: u32) -> Hlc
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let kb = key_bytes(&key);
    shard
        .engine
        .spilled_loc(kb.as_ref(), engine::hash_key_bytes(&kb), shard.now_ms())
        .expect("the key is spilled")
        .0
}

/// A peer's record of `key` holding `value`, one millisecond older than
/// `ver`.
fn older_record(key: u32, value: &str, ver: Hlc) -> WireRecord {
    WireRecord {
        key: key_bytes(&key),
        value: Some(Bytes::from(postcard::to_stdvec(value).expect("encodes"))),
        ver: Hlc {
            wall_ms: ver.wall_ms - 1,
            ..ver
        },
        expires_at_ms: None,
    }
}

/// The reason, the `why` field, of each `warn` event in `log` that reports a
/// dropped spilled entry.
fn drop_warnings(log: &Log) -> Vec<String> {
    log.0
        .lock()
        .expect("lock")
        .iter()
        .filter(|event| {
            event.level == tracing::Level::WARN
                && event
                    .fields
                    .get("message")
                    .is_some_and(|message| message.contains("spilled entry"))
        })
        .map(|event| event.fields.get("why").cloned().unwrap_or_default())
        .collect()
}

#[tokio::test]
async fn a_peer_write_below_a_spilled_record_that_fails_its_checksum_drops_it_and_lands() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let log = Log::default();
    let _subscriber = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(log.clone()),
    );
    let (shard, dir) = spill_shard_in("checksum-conflict", "unreadable", Mode::Replicated, 1);
    let (shard, key, value) = with_one_spilled(shard.with_resolver(Arc::new(ReadsValues))).await;
    corrupt_on_disk(&dir, &value);
    let ver = spilled_ver(&shard, key);
    let _ = snapshotter.snapshot();

    ShardOps::apply_remote_batch(&shard, vec![older_record(key, "older", ver)]).await;
    assert_eq!(
        reads(&snapshotter),
        HashMap::new(),
        "a conflict resolution's read is not counted"
    );
    assert_eq!(
        drop_warnings(&log),
        [format!("{:?}", "is damaged or cut short on disk")],
        "the drop is logged once, with its reason"
    );
    assert_eq!(
        shard.get(&key).await,
        Some("older".to_string()),
        "the peer's write lands, where the record no read can use would win"
    );
    assert_eq!(shard.engine.debug_totals().0, 2, "each key counts once");
    assert_digest_matches_full_recompute(&shard).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_peer_write_below_a_spilled_record_of_another_value_type_drops_it_and_lands() {
    let (cfg, dir) = warm_config("value-type-conflict");
    let after = reopened_across_a_value_type_change(&cfg, "type-change-conflict", &[5])
        .await
        .with_resolver(Arc::new(ReadsValues));
    let ver = spilled_ver(&after, 5);

    ShardOps::apply_remote_batch(&after, vec![older_record(5, "older", ver)]).await;
    assert_eq!(
        after.get(&5).await,
        Some("older".to_string()),
        "the peer's write lands, where the record of the old type would win"
    );
    assert_eq!(after.engine.debug_totals().0, 1);
    assert_eq!(
        after.engine.debug_spill_entries_count(),
        0,
        "the spill gauge counts the dropped entry out"
    );
    assert_digest_matches_full_recompute(&after).await;
    let _ = std::fs::remove_dir_all(&dir);
}
