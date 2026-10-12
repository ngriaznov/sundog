//! Real-disk tests of the spill tier: flush, read, rotation, close and the shared recorders.

use std::collections::HashMap as StdHashMap;
use std::sync::mpsc;
use std::sync::{Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};

use super::flusher::{flush_batch, flusher_loop};
use super::record::pwrite_all;
use super::snapshot::{
    SNAPSHOT_FORMAT_VERSION, build_snapshot_bytes, parse_snapshot, snapshot_path,
    snapshot_tmp_path, write_snapshot_atomic,
};
use super::*;

mod reasons;
mod reopen;
mod reserve;

/// Polls `cond` until it returns `true` or `timeout` elapses,
/// returning the final result either way. Never a fixed sleep: every
/// timing-sensitive assertion in this module goes through this.
fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    loop {
        if cond() {
            return true;
        }
        if start.elapsed() >= timeout {
            return cond();
        }
        thread::sleep(Duration::from_millis(5));
    }
}

const POLL_TIMEOUT: Duration = Duration::from_secs(5);

/// One `reclaim` call: the region and generation being reused, and
/// the reverse-index rows to purge.
type ReclaimCall = (u32, u32, Vec<ReclaimRef>);

/// One `install_new` call: `install`'s fields plus `expires_at_ms`.
type InstallNewCall = (usize, Bytes, Hlc, Option<u64>, SpillLoc);

#[derive(Default)]
struct RecordingSink {
    installs: StdMutex<Vec<(usize, Bytes, Hlc, SpillLoc)>>,
    install_news: StdMutex<Vec<InstallNewCall>>,
    reclaims: StdMutex<Vec<ReclaimCall>>,
    live: StdMutex<StdHashMap<Bytes, (Hlc, SpillLoc)>>,
    abandons: StdMutex<Vec<(usize, Bytes, u64, Hlc)>>,
}

impl RecordingSink {
    fn install_count(&self) -> usize {
        self.installs.lock().unwrap().len()
    }

    fn install_new_count(&self) -> usize {
        self.install_news.lock().unwrap().len()
    }

    fn abandon_count(&self) -> usize {
        self.abandons.lock().unwrap().len()
    }

    /// The keys this sink installed at the locations `rows` name in
    /// `region` at `generation`, sorted.
    fn keys_at(&self, region: u32, generation: u32, rows: &[ReclaimRef]) -> Vec<String> {
        let installed = self.installs.lock().unwrap();
        let mut keys: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                installed.iter().find(|(_, _, _, loc)| {
                    loc.region == region && loc.generation == generation && loc.offset == row.offset
                })
            })
            .map(|(_, key, _, _)| String::from_utf8(key.to_vec()).unwrap())
            .collect();
        keys.sort();
        keys
    }
}

impl SpillSink for RecordingSink {
    fn install(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        _hash: u64,
        ver: Hlc,
        loc: SpillLoc,
        _weight: u32,
    ) -> bool {
        self.installs
            .lock()
            .unwrap()
            .push((stripe_idx, key_bytes.clone(), ver, loc));
        self.live
            .lock()
            .unwrap()
            .insert(key_bytes.clone(), (ver, loc));
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn install_new(
        &self,
        stripe_idx: usize,
        key_bytes: &Bytes,
        _hash: u64,
        ver: Hlc,
        expires_at_ms: Option<u64>,
        loc: SpillLoc,
        _weight: u32,
    ) -> bool {
        let mut live = self.live.lock().unwrap();
        if live.contains_key(key_bytes) {
            return false;
        }
        live.insert(key_bytes.clone(), (ver, loc));
        drop(live);
        self.install_news.lock().unwrap().push((
            stripe_idx,
            key_bytes.clone(),
            ver,
            expires_at_ms,
            loc,
        ));
        true
    }

    fn reclaim(&self, region: u32, generation: u32, rows: &[ReclaimRef]) -> usize {
        self.reclaims
            .lock()
            .unwrap()
            .push((region, generation, rows.to_vec()));
        let mut live = self.live.lock().unwrap();
        let before = live.len();
        live.retain(|_, (_, loc)| {
            !(loc.region == region
                && loc.generation == generation
                && rows.iter().any(|row| row.offset == loc.offset))
        });
        before - live.len()
    }

    fn abandon(&self, stripe_idx: usize, key_bytes: &Bytes, hash: u64, ver: Hlc, _weight: u32) {
        // The engine-level weight-restoring behavior itself is
        // covered directly in `store::engine`'s tests; this test
        // double only records that the call happened, for this
        // module's own write-failure test.
        self.abandons
            .lock()
            .unwrap()
            .push((stripe_idx, key_bytes.clone(), hash, ver));
    }
}

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sundog-spill-test-{label}-{}-{:?}",
        std::process::id(),
        thread::current().id(),
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn job(key: &str, value: &[u8], ver: Hlc) -> SpillJob {
    SpillJob {
        stripe_idx: 0,
        hash: 0,
        key_bytes: Bytes::copy_from_slice(key.as_bytes()),
        ver,
        expires_at_ms: Some(999),
        encoded: Bytes::copy_from_slice(value),
        weight: 1,
        admitted_bytes: 0,
    }
}

#[test]
fn flushed_job_installs_and_read_at_returns_the_same_bytes_ver_and_expiry() {
    let dir = temp_dir("roundtrip");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    let ver = hlc(42, 3);
    let j = job("hello", b"world-value", ver);
    assert!(tier.try_spill(j));

    assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 1));
    let loc = sink.installs.lock().unwrap()[0].3;

    let bytes = tier
        .read_at(loc)
        .unwrap()
        .expect("record should be present");
    assert_eq!(bytes.ver, ver);
    assert_eq!(bytes.expires_at_ms, Some(999));
    assert_eq!(bytes.encoded.as_ref(), b"world-value");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn read_at_returns_none_for_a_stale_generation() {
    // Each region holds one record, so writing a third job rotates
    // region 0 out from under the first job's pointer. Waiting for
    // each job's install before sending the next keeps this
    // deterministic under the default flush_queue_bytes bound
    // too, one region's worth here: only one record is ever
    // queued at a time, and the rotation this test depends on is
    // unaffected by writes landing one at a time versus batched.
    let dir = temp_dir("stale-gen");
    let record_len = HEADER_LEN as u64 + 1 + 1; // 1-byte key, 1-byte value
    let cfg = SpillConfig::new(&dir, 2 * record_len).region_bytes(record_len);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    assert!(tier.try_spill(job("a", b"1", hlc(1, 0))));
    assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 1));
    assert!(tier.try_spill(job("b", b"2", hlc(2, 0))));
    assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 2));
    assert!(tier.try_spill(job("c", b"3", hlc(3, 0))));
    assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 3));

    let loc_a = sink.installs.lock().unwrap()[0].3;
    assert_eq!(tier.read_at(loc_a).unwrap(), None);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn read_at_returns_none_for_a_corrupted_record() {
    let dir = temp_dir("corrupt");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    assert!(tier.try_spill(job("k", b"original-value", hlc(1, 0))));
    assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 1));
    let loc = sink.installs.lock().unwrap()[0].3;

    // Flip a byte inside the value payload, after the header and key,
    // leaving every length field intact so parsing proceeds and only
    // the checksum fails.
    let region_path = dir.join("cache-a").join(region_file_name(loc.region));
    let file = OpenOptions::new().write(true).open(&region_path).unwrap();
    let corrupt_offset = u64::from(loc.offset) + HEADER_LEN as u64 + 1 /* key len */;
    pwrite_all(&file, b"!", corrupt_offset).unwrap();

    assert_eq!(tier.read_at(loc).unwrap(), None);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn rotation_reclaims_the_oldest_region_and_reports_its_keys() {
    // region_bytes fits two records; five writes force a rotation
    // into region 1, empty, and then back into region 0, which by
    // then holds the first two jobs' keys. Waiting for each job's
    // own install before sending the next keeps this deterministic
    // under the default flush_queue_bytes bound, one region's
    // worth here (room for two records): the rotations this test
    // depends on are unaffected by writes landing one at a time
    // versus batched.
    let dir = temp_dir("rotation");
    let record_len = HEADER_LEN as u64 + 6 + 4; // fixed-width key/value
    let region_bytes = record_len * 2;
    let cfg = SpillConfig::new(&dir, region_bytes * 2).region_bytes(region_bytes);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    for i in 0..5u32 {
        let key = format!("key-{i:02}");
        assert!(tier.try_spill(job(&key, b"1234", hlc(u64::from(i) + 1, 0))));
        let installed_so_far = i as usize + 1;
        assert!(poll_until(POLL_TIMEOUT, || {
            sink.install_count() == installed_so_far
        }));
    }
    assert!(poll_until(POLL_TIMEOUT, || sink
        .reclaims
        .lock()
        .unwrap()
        .len()
        == 2));

    let reclaims = sink.reclaims.lock().unwrap();
    let (region, generation, rows) = &reclaims[1];
    assert_eq!(*region, 0);
    assert_eq!(*generation, 0);
    assert_eq!(
        sink.keys_at(*region, *generation, rows),
        vec!["key-00", "key-01"]
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn flush_batch_spanning_a_rotation_installs_correctly_on_both_sides() {
    // Two records exactly fill one region. Handing all four jobs to
    // one direct `flush_batch` call, rather than through `try_spill`
    // and the flusher thread, makes the rotation deterministic: the
    // batch writes the first two, rotates once into the never-yet-
    // used second region, and writes the last two there, a record
    // never straddling the two.
    let dir = temp_dir("batch-rotation");
    let record_len = HEADER_LEN as u64 + 6 + 4; // fixed-width key/value
    let region_bytes = record_len * 2;
    let cfg = SpillConfig::new(&dir, region_bytes * 2).region_bytes(region_bytes);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = Arc::new(RecordingSink::default());

    let jobs: Vec<SpillJob> = (0..4u32)
        .map(|i| job(&format!("key-{i:02}"), b"1234", hlc(u64::from(i) + 1, 0)))
        .collect();
    flush_batch(&tier.inner, &*sink, jobs);

    assert_eq!(sink.install_count(), 4, "every job in the batch installs");
    assert_eq!(
        sink.reclaims.lock().unwrap().len(),
        1,
        "one rotation, into the never-yet-used second region"
    );

    let installs = sink.installs.lock().unwrap().clone();
    let regions: Vec<u32> = installs.iter().map(|(_, _, _, loc)| loc.region).collect();
    assert_eq!(
        regions,
        vec![0, 0, 1, 1],
        "the batch's four records land two per side of the one rotation"
    );
    for (idx, (_, key_bytes, _, loc)) in installs.iter().enumerate() {
        assert_eq!(key_bytes.as_ref(), format!("key-{idx:02}").as_bytes());
        let bytes = tier
            .read_at(*loc)
            .unwrap()
            .expect("record present on both sides of the rotation");
        assert_eq!(bytes.encoded.as_ref(), b"1234");
    }

    let _ = fs::remove_dir_all(&dir);
}

/// `checkpoint_write_resident`'s self-collision guard: a
/// wraparound overwrites a region this call already wrote, and
/// only surviving entries' `SpillLoc`s come back.
#[test]
fn checkpoint_write_resident_drops_entries_its_own_wraparound_overwrites_and_reopen_replays_the_survivors()
 {
    let dir = temp_dir("checkpoint-wraparound");
    let record_len = HEADER_LEN as u64 + 5 + 4; // fixed-width "xxx-N"/4-byte value
    let region_bytes = record_len;
    let cfg = SpillConfig::new(&dir, region_bytes * 2)
        .region_bytes(region_bytes)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = RecordingSink::default();

    // Ordinary flush: old-0 lands in region 0, so its reverse index
    // knows about it.
    flush_batch(&tier.inner, &sink, vec![job("old-0", b"1234", hlc(1, 0))]);
    assert_eq!(sink.install_count(), 1);

    // Three entries into a two-record ring: res-0, then res-1
    // (reclaiming old-0), then res-2 wraps back and overwrites
    // res-0.
    let entries: Vec<(Bytes, Hlc, Option<u64>, Bytes)> = ["res-0", "res-1", "res-2"]
        .iter()
        .enumerate()
        .map(|(i, key)| {
            (
                Bytes::from(key.as_bytes().to_vec()),
                hlc(u64::try_from(i).unwrap_or(u64::MAX) + 10, 0),
                None,
                Bytes::from_static(b"1234"),
            )
        })
        .collect();
    let newly_written = tier.checkpoint_flush(&sink, entries).written;

    assert_eq!(
        sink.reclaims.lock().unwrap().len(),
        3,
        "three rotations: into the empty region 1, back into region 0, then into \
         region 1 a second time"
    );
    let reclaimed_old = sink
        .reclaims
        .lock()
        .unwrap()
        .iter()
        .any(|(region, generation, rows)| {
            *region == 0
                && *generation == 0
                && sink
                    .keys_at(*region, *generation, rows)
                    .iter()
                    .any(|key| key == "old-0")
        });
    assert!(
        reclaimed_old,
        "the reclaim-during-checkpoint path actually ran against the pre-existing entry"
    );

    let written_keys: Vec<&[u8]> = newly_written.iter().map(|(k, ..)| k.as_ref()).collect();
    assert_eq!(
        written_keys,
        vec![b"res-1".as_slice(), b"res-2".as_slice()],
        "res-0's bytes were overwritten by res-2 within this same checkpoint call, so \
         only the entries whose bytes survived to the end of the call come back -- no \
         location in the result points at bytes this same call already reclaimed"
    );

    tier.write_checkpoint_snapshot(&newly_written, 1_000);
    tier.close();

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("reopen against a cleanly closed tier succeeds");
    assert!(
        outcome.warm,
        "the surviving entries' own on-disk bytes must still validate cleanly; reason: \
         {:?}",
        outcome.reason
    );
    assert_eq!(outcome.records_installed, 2);
    let mut installed_keys: Vec<Bytes> = fresh_sink
        .install_news
        .lock()
        .unwrap()
        .iter()
        .map(|(_, key, ..)| key.clone())
        .collect();
    installed_keys.sort();
    assert_eq!(
        installed_keys,
        vec![Bytes::from_static(b"res-1"), Bytes::from_static(b"res-2")],
        "reopen replays exactly the entries whose bytes survived the checkpoint, no \
         more (no old-0, reclaimed mid-checkpoint) and no less (no res-0, overwritten \
         mid-checkpoint)"
    );

    let _ = fs::remove_dir_all(&dir);
}

/// Captures `sundog_spill_writes_total{cache}` increments for one
/// dedicated cache name, ignoring every other metric: the
/// write-count counterpart to [`DropCounts`]/[`DropRecorder`] below.
#[derive(Clone, Default)]
struct WriteCounts(Arc<StdMutex<u64>>);

impl WriteCounts {
    fn get(&self) -> u64 {
        *self.0.lock().unwrap()
    }
}

struct WriteCounter {
    counts: WriteCounts,
}

impl metrics::CounterFn for WriteCounter {
    fn increment(&self, value: u64) {
        *self.counts.0.lock().unwrap() += value;
    }

    fn absolute(&self, value: u64) {
        *self.counts.0.lock().unwrap() = value;
    }
}

struct WriteRecorder {
    counts: WriteCounts,
}

impl metrics::Recorder for WriteRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        let this_cache = key
            .labels()
            .any(|l| l.key() == "cache" && l.value() == WRITE_COUNTS_CACHE);
        if key.name() != "sundog_spill_writes_total" || !this_cache {
            return metrics::Counter::noop();
        }
        metrics::Counter::from_arc(Arc::new(WriteCounter {
            counts: self.counts.clone(),
        }))
    }

    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// The cache name only this test opens, so the process-global
/// recorder ignores writes from every other test's tier.
const WRITE_COUNTS_CACHE: &str = "write-counts-only";

#[test]
fn flusher_batches_two_thousand_records_and_installs_every_one_byte_exact() {
    const COUNT: u32 = 2_000;

    let write_counts = WriteCounts::default();
    // Same single-process-global-slot race every other metrics-
    // reading test in this module tolerates: if another test already
    // won the slot, this one skips the counter assertion below.
    let recorder_installed = metrics::set_global_recorder(WriteRecorder {
        counts: write_counts.clone(),
    })
    .is_ok();

    let dir = temp_dir("batch-2000");
    let cfg = SpillConfig::new(&dir, 1 << 24).region_bytes(1 << 20);
    let tier = SpillTier::open(&cfg, WRITE_COUNTS_CACHE).unwrap();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    let expected: Vec<(String, Vec<u8>)> = (0..COUNT)
        .map(|i| (format!("key-{i:05}"), format!("value-{i:05}").into_bytes()))
        .collect();
    for i in 0..COUNT {
        let (key, value) = &expected[i as usize];
        let ver = hlc(u64::from(i) + 1, 0);
        assert!(tier.try_spill(job(key, value, ver)));
    }

    assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == COUNT as usize));

    let installs = sink.installs.lock().unwrap().clone();
    assert_eq!(installs.len(), COUNT as usize);
    for (idx, (expected_key, expected_value)) in expected.iter().enumerate() {
        let (_, key_bytes, _, loc) = &installs[idx];
        assert_eq!(
            key_bytes.as_ref(),
            expected_key.as_bytes(),
            "installs land in the order jobs were sent, batched or not"
        );
        let bytes = tier
            .read_at(*loc)
            .unwrap()
            .expect("every installed record reads back");
        assert_eq!(bytes.encoded.as_ref(), expected_value.as_slice());
    }

    if recorder_installed {
        assert_eq!(
            write_counts.get(),
            u64::from(COUNT),
            "the writes counter equals the install count, batched or not"
        );
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn bytes_used_reflects_confirmed_installs_and_drops_on_reclaim() {
    // Same layout as the rotation test: two records per region.
    // Waiting for each job's own install before sending the next
    // keeps this deterministic under the default
    // flush_queue_bytes bound, one region's worth here.
    let dir = temp_dir("bytes-used");
    let record_len = HEADER_LEN as u64 + 6 + 4;
    let region_bytes = record_len * 2;
    let cfg = SpillConfig::new(&dir, region_bytes * 2).region_bytes(region_bytes);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    for i in 0..4u32 {
        let key = format!("key-{i:02}");
        assert!(tier.try_spill(job(&key, b"1234", hlc(u64::from(i) + 1, 0))));
        let installed_so_far = i as usize + 1;
        assert!(poll_until(POLL_TIMEOUT, || {
            sink.install_count() == installed_so_far
        }));
    }
    assert!(poll_until(POLL_TIMEOUT, || tier.bytes_used() == record_len * 4));

    // A fifth job rotates region 0 out from under the first two
    // records, reclaiming them and freeing their bytes.
    assert!(tier.try_spill(job("key-04", b"1234", hlc(5, 0))));
    assert!(poll_until(POLL_TIMEOUT, || tier.bytes_used() == record_len * 3));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn try_spill_rejects_a_job_larger_than_region_bytes() {
    let dir = temp_dir("too-large");
    let cfg = SpillConfig::new(&dir, 128).region_bytes(64);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    let oversized = job("k", &[0u8; 100], hlc(1, 0));
    assert!(!tier.try_spill(oversized));
    assert_eq!(sink.install_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn try_spill_returns_false_before_attach_and_after_close() {
    let dir = temp_dir("before-after");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();

    assert!(!tier.try_spill(job("k", b"v", hlc(1, 0))));

    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));
    assert!(tier.try_spill(job("k2", b"v", hlc(2, 0))));

    tier.close();
    assert!(!tier.try_spill(job("k3", b"v", hlc(3, 0))));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn open_recreates_region_files_and_removes_stale_reg_files() {
    let dir = temp_dir("recreate");
    let cache_dir = dir.join("cache-a");
    fs::create_dir_all(&cache_dir).unwrap();
    fs::write(
        cache_dir.join("garbage.reg"),
        b"leftover-from-a-crashed-run",
    )
    .unwrap();
    write_snapshot_atomic(&cache_dir, b"leftover-from-a-crashed-run");
    fs::write(cache_dir.join("keep-me.txt"), b"not a region file").unwrap();

    let cfg = SpillConfig::new(&dir, 256).region_bytes(64);
    let _tier = SpillTier::open(&cfg, "cache-a").unwrap();

    assert!(!cache_dir.join("garbage.reg").exists());
    assert!(
        !snapshot_path(&cache_dir).exists(),
        "a snapshot left over from a prior incarnation never survives a fresh, \
         fully-recreated set of regions"
    );
    assert!(cache_dir.join("keep-me.txt").exists());
    for idx in 0..region_count_for(256, 64) {
        let path = cache_dir.join(region_file_name(idx));
        assert!(path.exists());
        assert_eq!(fs::metadata(&path).unwrap().len(), 64);
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn close_drains_the_queue() {
    let dir = temp_dir("drain");
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let sink = Arc::new(RecordingSink::default());
    tier.attach(Arc::downgrade(&(Arc::clone(&sink) as Arc<dyn SpillSink>)));

    for i in 0..5u32 {
        let key = format!("k{i}");
        assert!(tier.try_spill(job(&key, b"v", hlc(u64::from(i) + 1, 0))));
    }
    tier.close();

    assert!(poll_until(POLL_TIMEOUT, || sink.install_count() == 5));

    let _ = fs::remove_dir_all(&dir);
}
