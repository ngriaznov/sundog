//! Pure sizing and admission rules for the spill tier: region counts, queue slots, record fit and refusal policy.

use crate::hlc::Hlc;
use crate::store::engine::OnRefusal;

use super::HEADER_LEN;

/// Floor on the flusher's job queue slot count. [`SpillTier::attach`]
/// sizes the channel from `flush_queue_bytes_value() / HEADER_LEN`,
/// clamped between this and [`FLUSH_QUEUE_SLOTS_MAX`].
pub(crate) const FLUSH_QUEUE_CAPACITY: usize = 8192;
/// Ceiling [`SpillTier::attach`]'s byte-derived slot count clamps to,
/// guarding a huge `flush_queue_bytes` paired with tiny records from
/// allocating an oversized channel buffer.
pub(crate) const FLUSH_QUEUE_SLOTS_MAX: usize = 262_144;

/// Region count for a capacity/region-size pair. Pure; unit-tested directly.
/// [`SpillConfig::validate`] additionally requires the result be at least
/// 2, since a lone region would be both the active writer and the only
/// candidate for FIFO reclaim. This function itself stays a simple
/// division; the floor is enforced by the caller, [`SpillTier::open`].
pub(crate) fn region_count_for(capacity_bytes: u64, region_bytes: u64) -> u32 {
    u32::try_from((capacity_bytes / region_bytes.max(1)).max(1)).unwrap_or(u32::MAX)
}

/// Slot count for [`SpillTier::attach`]'s flush channel:
/// `flush_queue_bytes` divided by the header size, clamped to
/// `[FLUSH_QUEUE_CAPACITY, FLUSH_QUEUE_SLOTS_MAX]`, so larger records get
/// more slots than a fixed count would. Pure; unit tested directly.
pub(crate) fn flush_queue_slots(flush_queue_bytes: u64) -> usize {
    let slots = (flush_queue_bytes / HEADER_LEN as u64)
        .clamp(FLUSH_QUEUE_CAPACITY as u64, FLUSH_QUEUE_SLOTS_MAX as u64);
    // Bounded well within `usize` by the clamp; the fallback keeps this
    // total instead of panicking.
    usize::try_from(slots).unwrap_or(FLUSH_QUEUE_SLOTS_MAX)
}

/// Whether `record_len` more bytes fit in a region of `region_bytes` bytes
/// whose write cursor already sits at `write_cursor`. Checked arithmetic: a
/// pathological `record_len` never wraps into a false "yes".
pub(crate) fn record_fits(write_cursor: u32, region_bytes: u32, record_len: u32) -> bool {
    write_cursor
        .checked_add(record_len)
        .is_some_and(|end| end <= region_bytes)
}

/// Whether a record of `record_len` bytes, header plus key plus value,
/// could ever fit in *any* region of `region_bytes` bytes. `try_spill`
/// rejects a record that fails this before it is ever queued. No rotation
/// would help it.
pub(crate) fn record_too_large(record_len: u64, region_bytes: u64) -> bool {
    record_len > region_bytes
}

/// Whether a `record_len`-byte record fits in a `flush_queue_bytes`-byte
/// budget already holding `queued_bytes`. Test-only: pins the boundary and
/// overflow arithmetic `Inner::admit` performs internally. Pure.
#[cfg(test)]
pub(crate) fn record_fits_queue(
    queued_bytes: u64,
    record_len: u64,
    flush_queue_bytes: u64,
) -> bool {
    queued_bytes
        .checked_add(record_len)
        .is_some_and(|total| total <= flush_queue_bytes)
}

/// Total on-disk length of a `key_len`+`value_len`-byte record: header
/// plus key plus value, as a `u32`. Called by both `would_accept`'s
/// admission check and `try_spill_victim`'s bookkeeping so they agree on
/// the exact byte count. Saturates to `u32::MAX` on overflow. Pure; unit
/// tested directly.
pub(crate) fn spill_record_len(key_len: usize, value_len: usize) -> u32 {
    let total = (HEADER_LEN as u64)
        .saturating_add(key_len as u64)
        .saturating_add(value_len as u64);
    u32::try_from(total).unwrap_or(u32::MAX)
}

/// The `sundog_spill_dropped_total` reason a refusal is recorded under:
/// `"deferred"` when `keeps_resident` is set
/// ([`keeps_refused_victim_resident`]), since the caller leaves the victim
/// resident and retries it on a later eviction pass rather than deleting
/// it, so an operator sees backpressure instead of a
/// delete reason that fails to describe what happened; `specific`
/// (`"too_large"`, `"closed"`, or `"queue_full"`) unchanged otherwise. Pure;
/// unit tested directly.
pub(crate) fn refusal_drop_reason(keeps_resident: bool, specific: &'static str) -> &'static str {
    if keeps_resident { "deferred" } else { specific }
}

/// Whether an eviction pass leaves a victim its tier refuses resident,
/// rather than deleting it: always under [`OnRefusal::KeepResident`], and
/// under [`OnRefusal::TierPolicy`] exactly when `tier_keeps_resident`, the
/// tier's own flag ([`SpillTier::set_keep_resident_when_refused`]), is set.
/// Pure; unit tested directly.
pub(crate) fn keeps_refused_victim_resident(
    on_refusal: OnRefusal,
    tier_keeps_resident: bool,
) -> bool {
    match on_refusal {
        OnRefusal::KeepResident => true,
        OnRefusal::TierPolicy => tier_keeps_resident,
    }
}

/// The flusher's batch-splitting rule: how many of `record_lens`, taken in
/// order, fit consecutively in a region of `region_bytes` bytes whose write
/// cursor already sits at `write_cursor`, before the first one that does
/// not. Returns `record_lens.len()` when every record fits. [`flush_batch`]
/// calls this once per region a batch touches: it writes the leading
/// `records_fitting_region(..)` records in one buffer, rotates, then calls
/// this again with `write_cursor` reset to `0` and the unconsumed remainder,
/// so a batch that spans a rotation still never lets a record straddle two
/// regions. Pure; unit-tested directly, including the exact-boundary and
/// spans-a-rotation cases.
pub(crate) fn records_fitting_region(
    write_cursor: u32,
    region_bytes: u32,
    record_lens: &[u32],
) -> usize {
    let mut cursor = write_cursor;
    for (taken, &len) in record_lens.iter().enumerate() {
        if !record_fits(cursor, region_bytes, len) {
            return taken;
        }
        // `record_fits` above already ruled out overflow for this add.
        cursor += len;
    }
    record_lens.len()
}

/// The next region in FIFO round-robin order after `current`. Pure, total.
/// Never returns `current` when `region_count >= 2`, enforced by
/// [`SpillConfig::validate`]. That keeps the active-write region and the
/// next-to-reclaim region always distinct.
pub(crate) fn next_region_index(current: u32, region_count: u32) -> u32 {
    (current + 1) % region_count.max(1)
}

/// Whether a record spilled at `spilled_ver` still describes the key's
/// current state: no tombstone, and a live entry at that version. Used by
/// the flusher's install and by promotion; both are no-ops when this is
/// `false`. A key missing from `live`, `stored_live_ver == None`, is never
/// re-added, and a tombstone or a differing live version always wins over
/// the stale flush.
pub(crate) fn spilled_is_current(
    stored_tombstone_ver: Option<Hlc>,
    stored_live_ver: Option<Hlc>,
    spilled_ver: Hlc,
) -> bool {
    stored_tombstone_ver.is_none() && stored_live_ver == Some(spilled_ver)
}
