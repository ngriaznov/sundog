//! Kani proofs over the spill tier's sizing arithmetic.

use super::open::snapshot_loc_end;
use super::record::split_record;
use super::rules::{
    FLUSH_QUEUE_CAPACITY, FLUSH_QUEUE_SLOTS_MAX, admission_charge, flush_queue_slots, record_fits,
    region_count_for, slot_charge_floor,
};
use super::*;

/// Any capacity and region size give at least one region without panicking.
#[kani::proof]
fn region_count_is_at_least_one_for_any_sizes() {
    let capacity_bytes: u64 = kani::any();
    let region_bytes: u64 = kani::any();
    assert!(region_count_for(capacity_bytes, region_bytes) >= 1);
}

/// Any flush queue byte size lands between the slot floor and ceiling.
#[kani::proof]
fn flush_queue_slots_stay_within_the_clamp() {
    let flush_queue_bytes: u64 = kani::any();
    let slots = flush_queue_slots(flush_queue_bytes);
    assert!(slots >= FLUSH_QUEUE_CAPACITY);
    assert!(slots <= FLUSH_QUEUE_SLOTS_MAX);
}

/// The byte budget admits no more jobs than the channel has slots: every
/// record is at least a header long, so a job costs at least a header and at
/// least the budget's share of a slot, and the budget divided by that cost
/// is within the slot count. Holds short of the floor saturating to
/// `u32::MAX`.
#[kani::proof]
fn the_admission_charge_bounds_admitted_jobs_by_the_slot_count() {
    let flush_queue_bytes: u64 = kani::any();
    let record_len: u32 = kani::any();
    kani::assume(record_len as usize >= HEADER_LEN);
    kani::assume(flush_queue_bytes <= u64::from(u32::MAX) * FLUSH_QUEUE_SLOTS_MAX as u64);
    let charge = admission_charge(record_len, slot_charge_floor(flush_queue_bytes));
    assert!(charge > 0);
    assert!(flush_queue_bytes / u64::from(charge) <= flush_queue_slots(flush_queue_bytes) as u64);
}

/// A record fits exactly when its end, taken without wrapping, is
/// inside the region.
#[kani::proof]
fn a_record_fits_exactly_when_it_ends_inside_the_region() {
    let (write_cursor, region_bytes, record_len): (u32, u32, u32) =
        (kani::any(), kani::any(), kani::any());
    let end = u64::from(write_cursor) + u64::from(record_len);
    assert_eq!(
        record_fits(write_cursor, region_bytes, record_len),
        end <= u64::from(region_bytes)
    );
}

/// A snapshot location is accepted only inside a real region, and its
/// end is exactly `offset + len`, taken without wrapping.
#[kani::proof]
fn a_snapshot_location_is_accepted_only_inside_a_real_region() {
    let loc = SpillLoc {
        region: kani::any(),
        offset: kani::any(),
        len: kani::any(),
        generation: kani::any(),
    };
    let (region_bytes, region_count): (u32, u32) = (kani::any(), kani::any());
    let end = u64::from(loc.offset) + u64::from(loc.len);
    match snapshot_loc_end(loc, region_bytes, region_count) {
        Some(accepted) => {
            assert!(loc.region < region_count);
            assert_eq!(u64::from(accepted), end);
            assert!(accepted <= region_bytes);
        }
        None => {
            assert!(loc.region >= region_count || end > u64::from(region_bytes));
        }
    }
}

/// Any buffer off disk up to three bytes past a header splits without
/// panicking, and a split accounts for every byte.
#[kani::proof]
fn any_buffer_off_disk_splits_without_panicking() {
    const MAX: usize = HEADER_LEN + 3;
    let bytes: [u8; MAX] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= MAX);
    let buf = &bytes[..len];
    if let Some((header, key, value)) = split_record(buf) {
        assert_eq!(header.magic, SPILL_MAGIC);
        assert_eq!(key.len(), header.key_len as usize);
        assert_eq!(value.len(), header.value_len as usize);
        assert_eq!(HEADER_LEN + key.len() + value.len(), buf.len());
    }
}
