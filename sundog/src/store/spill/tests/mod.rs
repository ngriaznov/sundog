//! Unit tests for the spill tier: pure rules, then the on-disk I/O tests in `io`.

use crate::node::NodeId;
use crate::store::engine::OnRefusal;

use super::record::{build_header, split_record};
use super::rules::{
    FLUSH_QUEUE_CAPACITY, FLUSH_QUEUE_SLOTS_MAX, next_region_index, record_fits, record_fits_queue,
    record_too_large, records_fitting_region, refusal_drop_reason,
};
use super::*;

// --- Pure functions: no I/O, no tokio, safe under every feature combo. ---

#[test]
fn a_reclaim_row_is_sixteen_bytes_and_keeps_its_stripe() {
    assert_eq!(size_of::<ReclaimRef>(), 16);
    let row = ReclaimRef::new(crate::store::BUCKET_COUNT - 1, 0xDEAD_BEEF, 4096);
    assert_eq!(usize::from(row.stripe), crate::store::BUCKET_COUNT - 1);
    assert_eq!((row.hash, row.offset), (0xDEAD_BEEF, 4096));
}

#[test]
fn region_count_for_returns_at_least_one_region() {
    assert_eq!(region_count_for(0, 64), 1);
    assert_eq!(region_count_for(63, 64), 1);
    assert_eq!(region_count_for(64, 64), 1);
    assert_eq!(region_count_for(128, 64), 2);
    assert_eq!(region_count_for(200, 64), 3);
}

#[test]
fn slot_count_exceeds_the_old_fixed_capacity_for_small_records() {
    // Enough 256-byte records to fill FLUSH_QUEUE_CAPACITY well before
    // this byte budget.
    let flush_queue_bytes = 8192 * 256;
    assert!(flush_queue_slots(flush_queue_bytes) > FLUSH_QUEUE_CAPACITY);
}

#[test]
fn flush_queue_slots_stays_at_the_floor_for_a_tiny_byte_budget() {
    assert_eq!(flush_queue_slots(0), FLUSH_QUEUE_CAPACITY);
    assert_eq!(flush_queue_slots(1), FLUSH_QUEUE_CAPACITY);
}

#[test]
fn flush_queue_slots_clamps_at_the_ceiling_for_a_huge_byte_budget() {
    assert_eq!(flush_queue_slots(u64::MAX), FLUSH_QUEUE_SLOTS_MAX);
}

#[test]
fn record_fits_at_the_exact_boundary_and_one_byte_over() {
    assert!(record_fits(60, 64, 4));
    assert!(!record_fits(61, 64, 4));
    assert!(record_fits(0, 64, 64));
    assert!(!record_fits(0, 64, 65));
}

#[test]
fn record_fits_never_wraps_on_a_pathological_record_len() {
    assert!(!record_fits(u32::MAX - 1, 64, u32::MAX));
    assert!(!record_fits(10, u32::MAX, u32::MAX));
}

#[test]
fn records_fitting_region_takes_every_record_when_they_all_fit() {
    // Four 16-byte records exactly fill a 64-byte region starting at 0.
    assert_eq!(records_fitting_region(0, 64, &[16, 16, 16, 16]), 4);
}

#[test]
fn records_fitting_region_stops_at_the_first_record_that_does_not_fit() {
    // 60 bytes are free; a 4-byte record lands exactly on the boundary
    // and is taken, a following 1-byte record is not.
    assert_eq!(records_fitting_region(60, 64, &[4, 1]), 1);
    // The same 4-byte record one byte later does not fit at all.
    assert_eq!(records_fitting_region(61, 64, &[4]), 0);
}

#[test]
fn records_fitting_region_an_empty_batch_takes_nothing() {
    assert_eq!(records_fitting_region(0, 64, &[]), 0);
}

#[test]
fn records_fitting_region_a_batch_spanning_a_rotation_splits_in_two() {
    // A region with 20 bytes free; a four-record batch of 8 bytes each
    // fits two before the third would overrun, exactly the split
    // `flush_batch` uses to write the first segment, rotate, and retry
    // the remainder against a freshly emptied region (write_cursor 0).
    let lens = [8u32, 8, 8, 8];
    let first_segment = records_fitting_region(0, 20, &lens);
    assert_eq!(first_segment, 2);
    let remainder = &lens[first_segment..];
    // After a rotation the new region starts at cursor 0, where every
    // remaining record fits.
    assert_eq!(records_fitting_region(0, 20, remainder), remainder.len());
}

#[test]
fn record_too_large_rejects_over_region_bytes_and_accepts_exact_fit() {
    assert!(!record_too_large(64, 64));
    assert!(record_too_large(65, 64));
    assert!(!record_too_large(0, 0));
}

#[test]
fn record_fits_queue_at_the_exact_boundary_and_one_byte_over() {
    assert!(record_fits_queue(60, 4, 64));
    assert!(!record_fits_queue(61, 4, 64));
    assert!(record_fits_queue(0, 64, 64));
    assert!(!record_fits_queue(0, 65, 64));
}

#[test]
fn record_fits_queue_never_wraps_on_a_pathological_record_len() {
    assert!(!record_fits_queue(u64::MAX - 1, u64::MAX, 64));
    assert!(!record_fits_queue(10, u64::MAX, u64::MAX));
}

#[test]
fn spill_record_len_is_header_plus_key_plus_value() {
    let header_len = u32::try_from(HEADER_LEN).unwrap();
    assert_eq!(spill_record_len(4, 4), header_len + 8);
    assert_eq!(spill_record_len(0, 0), header_len);
}

#[test]
fn spill_record_len_saturates_instead_of_wrapping_on_a_pathological_input() {
    assert_eq!(spill_record_len(usize::MAX, usize::MAX), u32::MAX);
}

#[test]
fn refusal_drop_reason_passes_through_the_specific_reason_by_default() {
    assert_eq!(refusal_drop_reason(false, "too_large"), "too_large");
    assert_eq!(refusal_drop_reason(false, "closed"), "closed");
    assert_eq!(refusal_drop_reason(false, "queue_full"), "queue_full");
}

#[test]
fn refusal_drop_reason_reports_deferred_when_keep_resident_when_refused_is_set() {
    assert_eq!(refusal_drop_reason(true, "too_large"), "deferred");
    assert_eq!(refusal_drop_reason(true, "closed"), "deferred");
    assert_eq!(refusal_drop_reason(true, "queue_full"), "deferred");
}

#[test]
fn keeps_refused_victim_resident_combines_the_pass_policy_with_the_tier_flag() {
    assert!(
        keeps_refused_victim_resident(OnRefusal::KeepResident, true),
        "a keep-resident pass on a keep-resident tier"
    );
    assert!(
        keeps_refused_victim_resident(OnRefusal::KeepResident, false),
        "a keep-resident pass overrides a tier that deletes"
    );
    assert!(
        keeps_refused_victim_resident(OnRefusal::TierPolicy, true),
        "the tier's flag decides, set"
    );
    assert!(
        !keeps_refused_victim_resident(OnRefusal::TierPolicy, false),
        "the tier's flag decides, unset"
    );
}

#[test]
fn next_region_index_wraps_from_the_last_region_to_the_first() {
    assert_eq!(next_region_index(0, 3), 1);
    assert_eq!(next_region_index(1, 3), 2);
    assert_eq!(next_region_index(2, 3), 0);
    assert_ne!(next_region_index(0, 2), 0);
    assert_ne!(next_region_index(1, 2), 1);
}

fn hlc(wall_ms: u64, logical: u32) -> Hlc {
    Hlc {
        wall_ms,
        logical,
        node: NodeId::from(7u64),
    }
}

#[test]
fn spilled_is_current_true_when_live_version_matches_and_no_tombstone() {
    let v = hlc(10, 0);
    assert!(spilled_is_current(None, Some(v), v));
}

#[test]
fn spilled_is_current_false_when_tombstoned() {
    let v = hlc(10, 0);
    assert!(!spilled_is_current(Some(hlc(5, 0)), Some(v), v));
    assert!(!spilled_is_current(Some(v), Some(v), v));
}

#[test]
fn spilled_is_current_false_when_live_version_differs() {
    let spilled = hlc(10, 0);
    assert!(!spilled_is_current(None, Some(hlc(11, 0)), spilled));
    assert!(!spilled_is_current(None, Some(hlc(9, 0)), spilled));
}

#[test]
fn spilled_is_current_false_when_nothing_live() {
    assert!(!spilled_is_current(None, None, hlc(10, 0)));
}

/// Builds one record's raw on-disk bytes for
/// [`decode_record_with_key`] tests.
fn raw_record(key: &[u8], value: &[u8], ver: Hlc, expires_at_ms: Option<u64>) -> Vec<u8> {
    let job = SpillJob {
        stripe_idx: 0,
        hash: 0,
        key_bytes: Bytes::copy_from_slice(key),
        ver,
        expires_at_ms,
        encoded: Bytes::copy_from_slice(value),
        weight: 1,
        admitted_bytes: 0,
    };
    let key_len = u32::try_from(key.len()).unwrap();
    let value_len = u32::try_from(value.len()).unwrap();
    let header = build_header(&job, key_len, value_len);
    let mut buf = Vec::new();
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

// --- decode_record_with_key ---

#[test]
fn decode_record_with_key_round_trips_the_key_alongside_the_value() {
    let ver = hlc(7, 2);
    let buf = raw_record(b"a-key", b"a-value", ver, Some(555));

    let (key, sb) = decode_record_with_key(&buf).expect("a well-formed record decodes");
    assert_eq!(key.as_ref(), b"a-key");
    assert_eq!(sb.ver, ver);
    assert_eq!(sb.expires_at_ms, Some(555));
    assert_eq!(sb.encoded.as_ref(), b"a-value");
}

#[test]
fn decode_record_with_key_rejects_a_bit_flipped_checksum() {
    let mut buf = raw_record(b"k", b"v", hlc(1, 0), None);
    let last = buf.len() - 1;
    buf[last] ^= 0x01;
    assert!(decode_record_with_key(&buf).is_none());
}

#[test]
fn split_record_accounts_for_every_byte_and_refuses_a_bad_length() {
    let buf = raw_record(b"key", b"value", hlc(1, 0), None);
    let (header, key, value) = split_record(&buf).expect("a well-formed record splits");
    assert_eq!((key, value), (&b"key"[..], &b"value"[..]));
    assert_eq!(header.magic, SPILL_MAGIC);
    assert!(split_record(&buf[..buf.len() - 1]).is_none());
    assert!(split_record(&buf[..HEADER_LEN - 1]).is_none());
    let mut longer = buf.clone();
    longer.push(0);
    assert!(split_record(&longer).is_none());
}

#[test]
fn snapshot_loc_end_accepts_only_a_location_inside_a_real_region() {
    let loc = |region, offset, len| SpillLoc {
        region,
        offset,
        len,
        generation: 0,
    };
    assert_eq!(snapshot_loc_end(loc(0, 10, 20), 100, 2), Some(30));
    assert_eq!(snapshot_loc_end(loc(1, 80, 20), 100, 2), Some(100));
    assert_eq!(snapshot_loc_end(loc(2, 0, 1), 100, 2), None);
    assert_eq!(snapshot_loc_end(loc(0, 90, 20), 100, 2), None);
    assert_eq!(snapshot_loc_end(loc(0, u32::MAX, 1), u32::MAX, 2), None);
}

#[test]
fn decode_record_still_discards_the_key_exactly_as_before() {
    let buf = raw_record(b"k", b"v", hlc(1, 0), None);
    let sb = decode_record(&buf).expect("a well-formed record decodes");
    assert_eq!(sb.encoded.as_ref(), b"v");
}

// --- SpillConfig / validate ---

#[test]
fn spill_config_defaults_are_64_mib_regions_16_way_read_concurrency_and_one_region_of_flush_queue()
{
    let cfg = SpillConfig::new("/tmp/does-not-matter", 1 << 30);
    assert_eq!(cfg.region_bytes_value(), 64 * 1024 * 1024);
    assert_eq!(cfg.read_concurrency_value(), 16);
    assert_eq!(cfg.flush_queue_bytes_value(), 64 * 1024 * 1024);
}

#[test]
fn spill_config_builder_methods_override_the_defaults() {
    let cfg = SpillConfig::new("/tmp/does-not-matter", 1 << 30)
        .region_bytes(4096)
        .read_concurrency(4)
        .flush_queue_bytes(8192);
    assert_eq!(cfg.region_bytes_value(), 4096);
    assert_eq!(cfg.read_concurrency_value(), 4);
    assert_eq!(cfg.flush_queue_bytes_value(), 8192);
}

#[test]
fn validate_rejects_zero_region_bytes() {
    let cfg = SpillConfig::new("/tmp/x", 1024).region_bytes(0);
    assert!(cfg.validate().is_err());
}

#[test]
fn validate_rejects_region_bytes_over_u32_max() {
    let cfg = SpillConfig::new("/tmp/x", u64::MAX).region_bytes(u64::from(u32::MAX) + 1);
    assert!(cfg.validate().is_err());
}

#[test]
fn validate_rejects_capacity_under_twice_region_bytes() {
    let cfg = SpillConfig::new("/tmp/x", 100).region_bytes(64);
    assert!(cfg.validate().is_err());
    let ok = SpillConfig::new("/tmp/x", 128).region_bytes(64);
    assert!(ok.validate().is_ok());
}

#[test]
fn validate_rejects_flush_queue_bytes_under_one_record() {
    let cfg = SpillConfig::new("/tmp/x", 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(HEADER_LEN as u64 - 1);
    assert!(cfg.validate().is_err());
    let ok = SpillConfig::new("/tmp/x", 1 << 20)
        .region_bytes(4096)
        .flush_queue_bytes(HEADER_LEN as u64);
    assert!(ok.validate().is_ok());
}

#[test]
fn validate_rejects_flush_queue_bytes_over_capacity_bytes() {
    let cfg = SpillConfig::new("/tmp/x", 1024)
        .region_bytes(64)
        .flush_queue_bytes(1025);
    assert!(cfg.validate().is_err());
    let ok = SpillConfig::new("/tmp/x", 1024)
        .region_bytes(64)
        .flush_queue_bytes(1024);
    assert!(ok.validate().is_ok());
}

#[test]
fn validate_rejects_an_absurd_spill_wait_timeout() {
    let cfg = SpillConfig::new("/tmp/x", 1 << 20)
        .region_bytes(4096)
        .spill_wait_timeout(Duration::from_secs(60));
    assert!(cfg.validate().is_err());
    let ok = SpillConfig::new("/tmp/x", 1 << 20)
        .region_bytes(4096)
        .spill_wait_timeout(Duration::from_secs(59));
    assert!(ok.validate().is_ok());
}

#[test]
fn validate_accepts_spill_wait_timeout_zero_as_an_explicit_opt_out() {
    let cfg = SpillConfig::new("/tmp/x", 1 << 20)
        .region_bytes(4096)
        .spill_wait_timeout(Duration::ZERO);
    assert!(cfg.validate().is_ok());
    assert_eq!(cfg.spill_wait_timeout_value(), Duration::ZERO);
}

#[test]
fn spill_wait_timeout_defaults_to_two_seconds() {
    let cfg = SpillConfig::new("/tmp/x", 1 << 20);
    assert_eq!(cfg.spill_wait_timeout_value(), Duration::from_secs(2));
}

#[test]
fn spill_wait_timeout_builder_overrides_the_default() {
    let cfg = SpillConfig::new("/tmp/x", 1 << 20).spill_wait_timeout(Duration::from_secs(5));
    assert_eq!(cfg.spill_wait_timeout_value(), Duration::from_secs(5));
}

#[test]
fn default_warm_reopen_is_false() {
    let cfg = SpillConfig::new("/tmp/x", 1 << 20);
    assert!(!cfg.warm_reopen_value());
}

#[test]
fn warm_reopen_builder_overrides_the_default() {
    let cfg = SpillConfig::new("/tmp/x", 1 << 20).warm_reopen(true);
    assert!(cfg.warm_reopen_value());
}

// --- SpillTier: I/O tests. Real disk, real thread; never combined with
// `sim` since its virtual clock gives no determinism over real
// filesystem I/O or the flusher's OS thread.
#[cfg(not(feature = "sim"))]
mod io;
