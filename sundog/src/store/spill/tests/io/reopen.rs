//! Tests for snapshot serialization, `install_new` and warm `reopen`.

use super::*;

// --- Checkpoint snapshot serialization, no fsync ---

#[test]
fn snapshot_bytes_round_trip_through_parse_snapshot() {
    let loc_a = SpillLoc {
        region: 1,
        offset: 0,
        len: 40,
        generation: 0,
    };
    let loc_b = SpillLoc {
        region: 2,
        offset: 40,
        len: 41,
        generation: 3,
    };
    let entries = vec![
        (Bytes::from_static(b"a"), hlc(1, 0), Some(999), loc_a),
        (Bytes::from_static(b"bb"), hlc(2, 5), None, loc_b),
    ];

    let bytes = build_snapshot_bytes(&entries, 1 << 20, 7, 123_456);
    let (header, parsed) = parse_snapshot(&bytes).expect("a freshly built snapshot parses");

    assert_eq!(header.region_bytes, 1 << 20);
    assert_eq!(header.region_count, 7);
    assert_eq!(header.closed_at_ms, 123_456);
    assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].key.as_ref(), b"a");
    assert_eq!(parsed[0].ver, hlc(1, 0));
    assert_eq!(parsed[0].expires_at_ms, Some(999));
    assert_eq!(parsed[0].loc, loc_a);
    assert_eq!(parsed[1].key.as_ref(), b"bb");
    assert_eq!(parsed[1].ver, hlc(2, 5));
    assert_eq!(parsed[1].expires_at_ms, None);
    assert_eq!(parsed[1].loc, loc_b);
}

#[test]
fn write_snapshot_atomic_then_snapshot_eligibility_round_trips() {
    let dir = temp_dir("snapshot-roundtrip");
    fs::create_dir_all(&dir).unwrap();

    let bytes = build_snapshot_bytes(&[], 4096, 3, 1_000);
    write_snapshot_atomic(&dir, &bytes);

    assert!(
        !snapshot_tmp_path(&dir).exists(),
        "the temporary file is renamed away, never left behind on success"
    );
    let (header, entries) = snapshot_eligibility(&dir, 4096, 3, 1_500, 600_000)
        .expect("a freshly written snapshot within tombstone_ttl is eligible");
    assert_eq!(header.closed_at_ms, 1_000);
    assert!(entries.is_empty(), "{entries:?}");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_truncated_snapshot_falls_back_to_open_cleanly() {
    // root plays SpillConfig::dir; open() computes the same
    // root.join(cache_name) this test writes into.
    let root = temp_dir("snapshot-truncated");
    let cache_name = "cache-a";
    let cache_dir = root.join(cache_name);
    fs::create_dir_all(&cache_dir).unwrap();
    let bytes = build_snapshot_bytes(&[], 4096, 3, 123_456);
    write_snapshot_atomic(&cache_dir, &bytes[..bytes.len() / 2]);

    assert!(
        parse_snapshot(&fs::read(snapshot_path(&cache_dir)).unwrap()).is_none(),
        "a truncated snapshot is never trusted"
    );

    // open() never reads the snapshot, so a corrupt one can't
    // block it.
    let cfg = SpillConfig::new(&root, 1 << 20).region_bytes(4096);
    assert!(SpillTier::open(&cfg, cache_name).is_ok());

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn a_snapshot_with_a_flipped_checksum_bit_reads_as_none() {
    let dir = temp_dir("snapshot-corrupt");
    fs::create_dir_all(&dir).unwrap();
    let bytes = build_snapshot_bytes(&[], 1 << 20, 7, 123_456);
    write_snapshot_atomic(&dir, &bytes);

    let path = snapshot_path(&dir);
    let mut bytes = fs::read(&path).unwrap();
    // Flips a bit past the checksum field, so only it catches this.
    let flip_at = size_of::<u64>();
    bytes[flip_at] ^= 0x01;
    fs::write(&path, &bytes).unwrap();

    assert!(
        parse_snapshot(&bytes).is_none(),
        "a bit-flipped snapshot fails its checksum"
    );

    let _ = fs::remove_dir_all(&dir);
}

// --- SpillSink::install_new ---

#[test]
fn install_new_inserts_a_fresh_key_into_the_sinks_live_map() {
    let sink = RecordingSink::default();
    let kb = Bytes::from_static(b"fresh-key");
    let ver = hlc(1, 0);
    let loc = SpillLoc {
        region: 0,
        offset: 0,
        len: 16,
        generation: 0,
    };

    let inserted = sink.install_new(0, &kb, 0, ver, Some(999), loc, 1);

    assert!(inserted);
    assert_eq!(sink.install_new_count(), 1);
    assert_eq!(sink.live.lock().unwrap().get(&kb), Some(&(ver, loc)));
}

#[test]
fn install_new_never_overwrites_an_already_present_key() {
    let sink = RecordingSink::default();
    let kb = Bytes::from_static(b"already-there");
    let first_ver = hlc(1, 0);
    let loc = SpillLoc {
        region: 0,
        offset: 0,
        len: 16,
        generation: 0,
    };
    assert!(sink.install_new(0, &kb, 0, first_ver, None, loc, 1));

    let second_ver = hlc(2, 0);
    let second_loc = SpillLoc {
        region: 1,
        offset: 0,
        len: 16,
        generation: 0,
    };
    let inserted = sink.install_new(0, &kb, 0, second_ver, None, second_loc, 1);

    assert!(
        !inserted,
        "install_new never overwrites a key that is already present"
    );
    assert_eq!(sink.install_new_count(), 1);
    assert_eq!(
        sink.live.lock().unwrap().get(&kb),
        Some(&(first_ver, loc)),
        "the original entry is untouched"
    );
}

// --- SpillTier::reopen ---

/// Builds a checkpoint snapshot on disk from `sink`'s recorded
/// live entries, mirroring what a real checkpoint writes.
fn write_snapshot_for_test(
    cache_dir: &Path,
    sink: &RecordingSink,
    region_bytes: u64,
    region_count: u32,
    closed_at_ms: u64,
    expires_at_ms: Option<u64>,
) {
    let entries: Vec<(Bytes, Hlc, Option<u64>, SpillLoc)> = sink
        .live
        .lock()
        .unwrap()
        .iter()
        .map(|(key, (ver, loc))| (key.clone(), *ver, expires_at_ms, *loc))
        .collect();
    let bytes = build_snapshot_bytes(&entries, region_bytes, region_count, closed_at_ms);
    write_snapshot_atomic(cache_dir, &bytes);
}

#[test]
fn reopen_warm_reload_recovers_a_record_written_before_a_clean_close() {
    let dir = temp_dir("reopen-warm-basic");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    let ver = hlc(1, 0);
    flush_batch(&tier.inner, &*sink, vec![job("hello", b"world", ver)]);
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, None);

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("reopen against a cleanly closed tier succeeds");

    assert!(
        outcome.warm,
        "a valid snapshot within tombstone_ttl reopens warm"
    );
    assert_eq!(outcome.reason, None);
    assert_eq!(outcome.records_installed, 1);
    assert_eq!(
        outcome.warm_parts,
        HashSet::from([crate::store::PartId::of_key(b"hello")]),
        "warm_parts names exactly the one part a record was actually replayed for"
    );
    assert_eq!(fresh_sink.install_new_count(), 1);
    let (recovered_ver, loc) = *fresh_sink
        .live
        .lock()
        .unwrap()
        .get(&Bytes::from_static(b"hello"))
        .expect("the record replayed");
    assert_eq!(recovered_ver, ver);
    let bytes = outcome
        .tier
        .read_at(loc)
        .unwrap()
        .expect("the reopened tier still serves the record's original on-disk bytes");
    assert_eq!(bytes.encoded.as_ref(), b"world");
    assert!(
        !snapshot_path(&cache_dir).exists(),
        "a successful warm reopen deletes the snapshot it just replayed"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_falls_back_cold_when_a_snapshot_entrys_record_fails_to_decode() {
    let dir = temp_dir("reopen-bad-record");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(&tier.inner, &*sink, vec![job("a", b"1", hlc(1, 0))]);
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

    // Corrupts the pointed-at record; the flip must land inside
    // its bytes, not the zero-filled padding.
    let record_len = HEADER_LEN + 1 + 1;
    let region_path = cache_dir.join(region_file_name(0));
    let mut bytes = fs::read(&region_path).unwrap();
    bytes[record_len - 1] ^= 0x01;
    fs::write(&region_path, &bytes).unwrap();

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("the cold fallback still succeeds");

    assert!(
        !outcome.warm,
        "one bad entry falls the whole reopen back cold, conservatively"
    );
    assert_eq!(outcome.reason, Some("bad_region"));
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_falls_back_cold_when_a_snapshot_entry_points_past_its_region() {
    let dir = temp_dir("reopen-past-region");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let _tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let region_count = region_count_for(1 << 20, 4096);
    let bogus_loc = SpillLoc {
        region: 0,
        offset: 4090,
        len: 16,
        generation: 0,
    };
    let entries = vec![(Bytes::from_static(b"k"), hlc(1, 0), Some(999), bogus_loc)];
    let bytes = build_snapshot_bytes(&entries, 4096, region_count, 1_000);
    write_snapshot_atomic(&cache_dir, &bytes);

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("the cold fallback still succeeds");

    assert!(!outcome.warm);
    assert_eq!(outcome.reason, Some("bad_region"));
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_falls_back_cold_when_the_snapshot_checksum_is_corrupt() {
    let dir = temp_dir("reopen-corrupt-checksum");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

    let path = snapshot_path(&cache_dir);
    let mut bytes = fs::read(&path).unwrap();
    let flip_at = size_of::<u64>();
    bytes[flip_at] ^= 0x01;
    fs::write(&path, &bytes).unwrap();

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("the cold fallback still succeeds");

    assert!(!outcome.warm);
    assert_eq!(
        outcome.reason,
        Some("no_snapshot"),
        "a checksum failure collapses to the same \"nothing to trust\" outcome a \
         missing snapshot gets"
    );
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_with_no_snapshot_falls_back_to_open_with_reason_no_snapshot() {
    // A crash: region files exist but nothing ever closed cleanly,
    // so no snapshot was written.
    let dir = temp_dir("reopen-no-snapshot");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let _tier = SpillTier::open(&cfg, "cache-a").unwrap();

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("the cold fallback still succeeds");

    assert!(!outcome.warm);
    assert_eq!(outcome.reason, Some("no_snapshot"));
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_refuses_past_tombstone_ttl_and_falls_back_to_open() {
    let dir = temp_dir("reopen-ttl-exceeded");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    // A deterministic closed_at_ms so the tombstone_ttl comparison
    // is exact, independent of test timing.
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

    let fresh_sink = RecordingSink::default();
    let tombstone_ttl_ms = 600_000;
    let now_ms = 1_000 + tombstone_ttl_ms + 1;
    let outcome = SpillTier::reopen(
        &cfg,
        "cache-a",
        &fresh_sink,
        now_ms,
        tombstone_ttl_ms,
        |_| true,
    )
    .expect("the cold fallback still succeeds");

    assert!(!outcome.warm);
    assert_eq!(outcome.reason, Some("downtime_exceeded"));
    assert_eq!(outcome.records_installed, 0);
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_refuses_on_a_stale_snapshot_format_version_and_falls_back_to_open() {
    let dir = temp_dir("reopen-stale-snapshot");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let _tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");

    // Everything but format_version checks out.
    let region_count = region_count_for(1 << 20, 4096);
    write_stale_snapshot_for_test(&cache_dir, 4096, region_count, 1_000);

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_000, 600_000, |_| true)
        .expect("the cold fallback still succeeds");

    assert!(!outcome.warm);
    assert_eq!(outcome.reason, Some("stale_snapshot"));
    assert_eq!(outcome.records_installed, 0);
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_refuses_on_a_region_bytes_or_region_count_mismatch_and_falls_back_to_open() {
    let dir = temp_dir("reopen-config-mismatch");
    let region_len = HEADER_LEN as u64 + 1 + 1;
    let cfg = SpillConfig::new(&dir, 2 * region_len)
        .region_bytes(region_len)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
    tier.close();
    let region_count = region_count_for(2 * region_len, region_len).max(2);
    write_snapshot_for_test(
        &cache_dir,
        &sink,
        region_len,
        region_count,
        1_000,
        Some(999),
    );

    // A different region_bytes than the snapshot was written under.
    let resized_cfg = SpillConfig::new(&dir, 4 * region_len)
        .region_bytes(2 * region_len)
        .warm_reopen(true);
    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&resized_cfg, "cache-a", &fresh_sink, 1_000, 600_000, |_| {
        true
    })
    .expect("the cold fallback still succeeds");

    assert!(!outcome.warm);
    assert_eq!(outcome.reason, Some("config_mismatch"));
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_is_disabled_by_default_even_against_an_otherwise_eligible_snapshot() {
    let dir = temp_dir("reopen-disabled");
    // `warm_reopen` left at its default, `false`.
    let cfg = SpillConfig::new(&dir, 1 << 20).region_bytes(4096);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("the cold fallback still succeeds");

    assert!(!outcome.warm);
    assert_eq!(outcome.reason, Some("disabled"));
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_falls_back_to_open_when_a_region_file_is_missing() {
    let dir = temp_dir("reopen-missing-region");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

    // The snapshot names this dir eligible, but a region file is gone.
    fs::remove_file(cache_dir.join(region_file_name(1))).unwrap();

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("the cold fallback still succeeds");

    assert!(!outcome.warm);
    assert_eq!(outcome.reason, Some("bad_region"));
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_filters_out_a_bucket_this_node_no_longer_owns() {
    let dir = temp_dir("reopen-owned-filter");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, None);

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| false)
        .expect("reopen against a cleanly closed tier succeeds");

    assert!(outcome.warm, "the tier itself still reopens warm");
    assert_eq!(
        outcome.records_installed, 0,
        "every record is filtered out by an owned-bucket predicate that owns nothing"
    );
    assert!(
        outcome.warm_parts.is_empty(),
        "no bucket was actually replayed into, so warm_parts names none"
    );
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_warm_buckets_names_only_the_buckets_a_record_survived_the_owned_filter_for() {
    // Two keys in different buckets; only one passes the owned
    // predicate, so warm_parts must name only that one.
    let dir = temp_dir("reopen-owned-filter-partial");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(
        &tier.inner,
        &*sink,
        vec![job("k", b"1", hlc(1, 0)), job("a", b"2", hlc(1, 0))],
    );
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, None);

    let kept_bucket = crate::store::PartId::of_key(b"k");
    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |p| {
        p == kept_bucket
    })
    .expect("reopen against a cleanly closed tier succeeds");

    assert!(outcome.warm);
    assert_eq!(
        outcome.warm_parts,
        HashSet::from([kept_bucket]),
        "only the owned part's key survives the filter and lands in warm_parts"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_drops_an_already_expired_record() {
    let dir = temp_dir("reopen-expired");
    let cfg = SpillConfig::new(&dir, 1 << 20)
        .region_bytes(4096)
        .warm_reopen(true);
    let tier = SpillTier::open(&cfg, "cache-a").unwrap();
    let cache_dir = dir.join("cache-a");
    let sink = Arc::new(RecordingSink::default());
    flush_batch(&tier.inner, &*sink, vec![job("k", b"1", hlc(1, 0))]);
    tier.close();
    let region_count = region_count_for(1 << 20, 4096);
    // `now_ms` past this expiry when reopen runs, below.
    write_snapshot_for_test(&cache_dir, &sink, 4096, region_count, 1_000, Some(999));

    let fresh_sink = RecordingSink::default();
    let outcome = SpillTier::reopen(&cfg, "cache-a", &fresh_sink, 1_500, 600_000, |_| true)
        .expect("reopen against a cleanly closed tier succeeds");

    assert!(outcome.warm);
    assert_eq!(
        outcome.records_installed, 0,
        "an already-expired record is dropped"
    );
    assert_eq!(fresh_sink.install_new_count(), 0);

    let _ = fs::remove_dir_all(&dir);
}
