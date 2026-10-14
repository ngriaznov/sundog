//! The hold a `Mode::Distributed` owner keeps for a forwarded write it
//! refused for clock skew.
//!
//! A non-owner that forwards a write keeps no copy of it, so the frame is the
//! only one there is. An owner whose clock runs more than `max_clock_skew`
//! behind the write's stamp refuses to apply it, and without a hold the write
//! is gone: an insert never appears, a remove leaves the key live. The owner
//! parks the record here instead, in memory and bounded, and applies it once
//! its own clock is within the bound of the stamp.
//!
//! Everything in this file is pure: the data structure and the rules that
//! decide what it takes and when it releases. `Shard` drives it (see
//! `shard_skew.rs`).

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use bytes::Bytes;

use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::wire::WireRecord;

/// The most records one shard holds for clock skew.
pub(super) const SKEW_HOLD_MAX_RECORDS: usize = 16_384;

/// The most bytes (keys, values and a fixed overhead per record) one shard
/// holds for clock skew.
pub(super) const SKEW_HOLD_MAX_BYTES: usize = 32 * 1024 * 1024;

/// The longest a record is held, whatever `tombstone_max_ttl` allows: a
/// record that would wait longer (a stamp ahead by more than `max_clock_skew`
/// plus this) is a broken clock, not skew worth waiting out.
pub(super) const SKEW_HOLD_MAX_HORIZON_MS: u64 = 24 * 60 * 60 * 1000;

/// The longest the release task sleeps between looks at the hold. Due times
/// are wall-clock and tokio's timer is monotonic, so a clock step or a
/// suspend would otherwise strand a release for the whole wait.
pub(super) const SKEW_RELEASE_POLL: Duration = Duration::from_secs(1);

/// Bytes a held record costs beyond its key and value.
const HELD_RECORD_OVERHEAD: usize = 64;

/// The earliest physical time, in epoch milliseconds, at which a stamp at
/// `wall_ms` is within `max_skew_ms` of the clock: the first instant
/// [`crate::hlc::exceeds_skew`] is false for it. Saturates at zero.
pub(super) const fn skew_due_ms(wall_ms: u64, max_skew_ms: u64) -> u64 {
    wall_ms.saturating_sub(max_skew_ms)
}

/// How long a record may be held: `tombstone_max_ttl_ms`, no more than
/// [`SKEW_HOLD_MAX_HORIZON_MS`]. A held tombstone older than the longest a
/// tombstone lives would be gone from every peer anyway.
pub(super) fn skew_hold_horizon_ms(tombstone_max_ttl_ms: u64) -> u64 {
    tombstone_max_ttl_ms.min(SKEW_HOLD_MAX_HORIZON_MS)
}

/// Whether a record due at `due_ms` lies further past `now_ms` than
/// `horizon_ms`. A stamp saturated at `u64::MAX` always does.
pub(super) const fn beyond_horizon(due_ms: u64, now_ms: u64, horizon_ms: u64) -> bool {
    due_ms.saturating_sub(now_ms) > horizon_ms
}

/// How long the release task sleeps with `next_due_ms` the earliest due
/// time in the hold: `None` with nothing held, so the task parks on its
/// wake-up alone. Otherwise the time until it, at least a millisecond (a
/// frozen clock cannot spin the task) and at most [`SKEW_RELEASE_POLL`].
pub(super) fn skew_hold_wait(next_due_ms: Option<u64>, now_ms: u64) -> Option<Duration> {
    next_due_ms.map(|due_ms| {
        Duration::from_millis(due_ms.saturating_sub(now_ms).max(1)).min(SKEW_RELEASE_POLL)
    })
}

/// Whether a refused record is held. Only a record from a forward is: a
/// replication, anti-entropy or state-transfer refusal leaves the sender
/// holding the record, and so does an owner's forward to a co-owner, since
/// the stamp's writer kept the record in its own engine and anti-entropy
/// repairs the refusal. Only a writer that is not among the part's `owners`
/// kept nothing.
pub(super) fn should_hold(hold_skewed: bool, owners: &[NodeId], writer: NodeId) -> bool {
    hold_skewed && !owners.contains(&writer)
}

/// What [`SkewHold::hold`] did with a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HoldOutcome {
    /// The record is held.
    Held,
    /// A record with this key and version is held already.
    Duplicate,
    /// A newer record for this key is held, and the resolver keeps one
    /// version per key.
    Superseded,
    /// Holding the record would pass the record or byte cap. A hold never
    /// evicts another record to make room for a merging key's contribution.
    Full,
    /// The hold was closed by [`SkewHold::close`].
    Closed,
}

/// Where a held record sits: its due time, then its arrival number.
type Slot = (u64, u64);

/// The records an owner refused for skew, ordered by when they come due.
///
/// A resolver that merges needs every distinct `(key, version)`: collapsing a
/// key to its newest record would drop merge inputs. Any other resolver
/// settles a key on its highest version, so the hold keeps that one record
/// per key and a hot key from a fast writer cannot crowd out its own newest
/// value.
pub(super) struct SkewHold {
    entries: BTreeMap<Slot, WireRecord>,
    by_key: HashMap<Bytes, Vec<(Slot, Hlc)>>,
    seq: u64,
    bytes: usize,
    max_records: usize,
    max_bytes: usize,
    closed: bool,
}

/// What `rec` costs the byte cap.
fn held_size(rec: &WireRecord) -> usize {
    rec.key.len() + rec.value.as_ref().map_or(0, Bytes::len) + HELD_RECORD_OVERHEAD
}

impl SkewHold {
    pub(super) fn new(max_records: usize, max_bytes: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            by_key: HashMap::new(),
            seq: 0,
            bytes: 0,
            max_records,
            max_bytes,
            closed: false,
        }
    }

    /// Holds `rec`, due when the clock is within `max_skew_ms` of its stamp.
    /// `keep_every_version` is the resolver's `merges()`. A refusal changes
    /// nothing. A key that keeps one version gives up its held record for a
    /// newer `rec`. A key that keeps every version never gives one up: at a
    /// cap `rec` is refused as [`HoldOutcome::Full`], because a version from
    /// another writer does not subsume the one held.
    pub(super) fn hold(
        &mut self,
        rec: WireRecord,
        max_skew_ms: u64,
        keep_every_version: bool,
    ) -> HoldOutcome {
        if self.closed {
            return HoldOutcome::Closed;
        }
        let size = held_size(&rec);
        let held_for_key: &[(Slot, Hlc)] = self.by_key.get(&rec.key).map_or(&[], Vec::as_slice);
        if held_for_key.iter().any(|&(_, ver)| ver == rec.ver) {
            return HoldOutcome::Duplicate;
        }
        let mut evict: Vec<Slot> = Vec::new();
        if !keep_every_version {
            if held_for_key.iter().any(|&(_, ver)| ver > rec.ver) {
                return HoldOutcome::Superseded;
            }
            evict.extend(held_for_key.iter().map(|&(slot, _)| slot));
        }
        if !self.fits(&evict, size) {
            return HoldOutcome::Full;
        }
        for slot in evict {
            self.remove(slot);
        }
        let slot = (skew_due_ms(rec.ver.wall_ms, max_skew_ms), self.seq);
        self.seq += 1;
        self.bytes += size;
        self.by_key
            .entry(rec.key.clone())
            .or_default()
            .push((slot, rec.ver));
        self.entries.insert(slot, rec);
        HoldOutcome::Held
    }

    /// Whether one more record of `size` bytes fits once the records in
    /// `evict` are gone.
    fn fits(&self, evict: &[Slot], size: usize) -> bool {
        let freed: usize = evict
            .iter()
            .filter_map(|slot| self.entries.get(slot))
            .map(held_size)
            .sum();
        self.entries.len() - evict.len() < self.max_records
            && self.bytes - freed + size <= self.max_bytes
    }

    /// Takes the record at `slot` out of both indexes.
    fn remove(&mut self, slot: Slot) -> Option<WireRecord> {
        let rec = self.entries.remove(&slot)?;
        self.bytes -= held_size(&rec);
        if let Some(held) = self.by_key.get_mut(&rec.key) {
            held.retain(|&(held_slot, _)| held_slot != slot);
            if held.is_empty() {
                self.by_key.remove(&rec.key);
            }
        }
        Some(rec)
    }

    /// Removes and returns every record due by `now_ms`, earliest due first,
    /// arrival order within a due time. Nothing once the hold is closed.
    pub(super) fn take_due(&mut self, now_ms: u64) -> Vec<WireRecord> {
        let due: Vec<Slot> = self
            .entries
            .range(..=(now_ms, u64::MAX))
            .map(|(&slot, _)| slot)
            .collect();
        due.into_iter()
            .filter_map(|slot| self.remove(slot))
            .collect()
    }

    /// Closes the hold and returns everything in it, in due order: it takes
    /// no record from now on and releases none.
    pub(super) fn close(&mut self) -> Vec<WireRecord> {
        self.closed = true;
        self.by_key.clear();
        self.bytes = 0;
        std::mem::take(&mut self.entries).into_values().collect()
    }

    /// The earliest due time held.
    pub(super) fn next_due_ms(&self) -> Option<u64> {
        self.entries.keys().next().map(|&(due_ms, _)| due_ms)
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::exceeds_skew;

    fn node(n: u64) -> NodeId {
        NodeId::from(n)
    }

    fn rec(key: u8, wall_ms: u64, logical: u32, value: usize) -> WireRecord {
        WireRecord {
            key: Bytes::from(vec![key]),
            value: Some(Bytes::from(vec![0u8; value])),
            ver: Hlc {
                wall_ms,
                logical,
                node: node(9),
            },
            expires_at_ms: None,
        }
    }

    fn new_hold() -> SkewHold {
        SkewHold::new(SKEW_HOLD_MAX_RECORDS, SKEW_HOLD_MAX_BYTES)
    }

    #[test]
    fn skew_due_ms_is_the_first_instant_the_stamp_is_within_the_bound() {
        let (wall_ms, max_skew_ms) = (100_000, 1_000);
        let due = skew_due_ms(wall_ms, max_skew_ms);
        assert!(exceeds_skew(due - 1, wall_ms, max_skew_ms));
        assert!(!exceeds_skew(due, wall_ms, max_skew_ms));
        assert_eq!(skew_due_ms(500, 1_000), 0, "a small stamp saturates");
    }

    #[test]
    fn the_horizon_is_tombstone_max_ttl_capped_at_a_day() {
        assert_eq!(skew_hold_horizon_ms(60_000), 60_000);
        assert_eq!(
            skew_hold_horizon_ms(30 * 24 * 60 * 60 * 1000),
            SKEW_HOLD_MAX_HORIZON_MS
        );
        assert_eq!(skew_hold_horizon_ms(0), 0);
    }

    #[test]
    fn beyond_horizon_is_strictly_past_it_and_catches_a_saturated_stamp() {
        assert!(!beyond_horizon(1_000, 1_000, 0), "due now is within");
        assert!(!beyond_horizon(2_000, 1_000, 1_000));
        assert!(beyond_horizon(2_001, 1_000, 1_000));
        assert!(!beyond_horizon(500, 1_000, 0), "already due is within");
        assert!(beyond_horizon(u64::MAX, 1_000, SKEW_HOLD_MAX_HORIZON_MS));
    }

    #[test]
    fn the_horizon_bounds_the_wait_so_a_stamp_may_lead_by_the_skew_bound_plus_it() {
        let (now_ms, max_skew_ms, horizon_ms) = (1_000_000, 60_000, 600_000);
        let due = |lead_ms: u64| skew_due_ms(now_ms + lead_ms, max_skew_ms);
        assert!(
            !beyond_horizon(due(max_skew_ms + horizon_ms), now_ms, horizon_ms),
            "a lead of the bound plus the horizon waits exactly the horizon"
        );
        assert!(
            beyond_horizon(due(max_skew_ms + horizon_ms + 1), now_ms, horizon_ms),
            "one millisecond more waits past it"
        );
        assert!(
            !beyond_horizon(due(horizon_ms), now_ms, horizon_ms),
            "a lead of the horizon alone waits less than it"
        );
    }

    #[test]
    fn skew_hold_wait_parks_when_empty_and_polls_in_slices() {
        assert_eq!(skew_hold_wait(None, 5_000), None);
        assert_eq!(
            skew_hold_wait(Some(5_300), 5_000),
            Some(Duration::from_millis(300))
        );
        assert_eq!(
            skew_hold_wait(Some(5_000), 5_000),
            Some(Duration::from_millis(1)),
            "a due record still waits a millisecond"
        );
        assert_eq!(
            skew_hold_wait(Some(1_000), 5_000),
            Some(Duration::from_millis(1)),
            "a past due time cannot spin the task"
        );
        assert_eq!(
            skew_hold_wait(Some(5_000 + 3_600_000), 5_000),
            Some(SKEW_RELEASE_POLL),
            "a long wait is cut to a poll slice"
        );
    }

    #[test]
    fn should_hold_needs_a_forward_from_a_writer_that_is_not_an_owner() {
        let owners = [node(1), node(2)];
        assert!(should_hold(true, &owners, node(3)));
        assert!(
            !should_hold(true, &owners, node(2)),
            "an owner kept the record in its own engine"
        );
        assert!(
            !should_hold(false, &owners, node(3)),
            "only a forward is held"
        );
        assert!(should_hold(true, &[], node(3)), "no owners, no retention");
    }

    #[test]
    fn records_come_back_in_due_then_arrival_order() {
        let mut hold = new_hold();
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(2, 3_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(3, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(hold.next_due_ms(), Some(2_000));
        assert_eq!(hold.len(), 3);
        assert_eq!(hold.take_due(1_999).len(), 0, "nothing is due yet");
        let keys: Vec<u8> = hold.take_due(4_000).iter().map(|r| r.key[0]).collect();
        assert_eq!(keys, [2, 1, 3], "due first, arrival within a due time");
        assert_eq!(hold.len(), 0);
        assert_eq!(hold.bytes(), 0);
        assert_eq!(hold.next_due_ms(), None);
    }

    #[test]
    fn take_due_leaves_what_is_not_yet_due() {
        let mut hold = new_hold();
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(2, 9_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(hold.take_due(4_000).len(), 1);
        assert_eq!(hold.next_due_ms(), Some(8_000));
        assert_eq!(hold.len(), 1);
        assert_eq!(hold.bytes(), 1 + 1 + HELD_RECORD_OVERHEAD);
    }

    #[test]
    fn the_same_key_and_version_is_held_once() {
        let mut hold = new_hold();
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        let bytes = hold.bytes();
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Duplicate
        );
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, true),
            HoldOutcome::Duplicate,
            "a merging resolver holds each distinct version once as well"
        );
        assert_eq!((hold.len(), hold.bytes()), (1, bytes));
    }

    #[test]
    fn a_resolver_that_does_not_merge_keeps_the_highest_version_per_key() {
        let mut hold = new_hold();
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(1, 6_000, 0, 10), 1_000, false),
            HoldOutcome::Held,
            "a newer version replaces the held one"
        );
        assert_eq!(hold.len(), 1);
        assert_eq!(hold.bytes(), 1 + 10 + HELD_RECORD_OVERHEAD);
        assert_eq!(
            hold.hold(rec(1, 5_500, 0, 1), 1_000, false),
            HoldOutcome::Superseded,
            "an older version is not held beside it"
        );
        assert_eq!(
            hold.hold(rec(1, 6_000, 1, 1), 1_000, false),
            HoldOutcome::Held,
            "a higher logical counter is a newer version"
        );
        let taken = hold.take_due(10_000);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].ver.logical, 1);
    }

    #[test]
    fn a_merging_resolver_keeps_every_distinct_version() {
        let mut hold = new_hold();
        for wall_ms in [5_000, 6_000, 5_500] {
            assert_eq!(
                hold.hold(rec(1, wall_ms, 0, 1), 1_000, true),
                HoldOutcome::Held
            );
        }
        assert_eq!(hold.len(), 3);
        assert_eq!(hold.take_due(10_000).len(), 3);
    }

    #[test]
    fn a_full_hold_refuses_without_changing() {
        let mut hold = SkewHold::new(2, SKEW_HOLD_MAX_BYTES);
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(2, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        let bytes = hold.bytes();
        assert_eq!(
            hold.hold(rec(3, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Full
        );
        assert_eq!((hold.len(), hold.bytes()), (2, bytes));
        assert_eq!(hold.next_due_ms(), Some(4_000));
    }

    #[test]
    fn the_byte_cap_refuses_a_record_that_does_not_fit() {
        let one = 1 + 100 + HELD_RECORD_OVERHEAD;
        let mut hold = SkewHold::new(SKEW_HOLD_MAX_RECORDS, one * 2);
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 100), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(2, 5_000, 0, 100), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(3, 5_000, 0, 100), 1_000, false),
            HoldOutcome::Full
        );
        assert_eq!(hold.bytes(), one * 2);
        // Taking one frees its bytes exactly.
        assert_eq!(hold.take_due(10_000).len(), 2);
        assert_eq!(hold.bytes(), 0);
        assert_eq!(
            hold.hold(rec(3, 5_000, 0, 100), 1_000, false),
            HoldOutcome::Held
        );
    }

    #[test]
    fn a_replacement_too_big_for_the_byte_cap_leaves_the_held_record() {
        let mut hold = SkewHold::new(SKEW_HOLD_MAX_RECORDS, 200);
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 10), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(1, 6_000, 0, 200), 1_000, false),
            HoldOutcome::Full
        );
        let taken = hold.take_due(10_000);
        assert_eq!(taken.len(), 1);
        assert_eq!(
            taken[0].ver.wall_ms, 5_000,
            "the older record is still held"
        );
    }

    /// `rec` as written by `writer`.
    fn by_writer(mut rec: WireRecord, writer: u64) -> WireRecord {
        rec.ver.node = node(writer);
        rec
    }

    #[test]
    fn a_full_merging_hold_refuses_a_newer_contribution_and_evicts_nothing() {
        let mut hold = SkewHold::new(2, SKEW_HOLD_MAX_BYTES);
        assert_eq!(
            hold.hold(by_writer(rec(1, 5_000, 0, 1), 7), 1_000, true),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(by_writer(rec(1, 5_500, 0, 1), 8), 1_000, true),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(by_writer(rec(1, 6_000, 0, 1), 9), 1_000, true),
            HoldOutcome::Full,
            "a higher version from another writer does not subsume a held one"
        );
        assert_eq!(
            hold.hold(by_writer(rec(1, 6_000, 0, 1), 7), 1_000, true),
            HoldOutcome::Full,
            "nor does a higher version from the same writer, which an OrSet add is not"
        );
        let kept: Vec<(u64, NodeId)> = hold
            .take_due(10_000)
            .iter()
            .map(|r| (r.ver.wall_ms, r.ver.node))
            .collect();
        assert_eq!(kept, [(5_000, node(7)), (5_500, node(8))]);
    }

    #[test]
    fn a_full_merging_hold_refuses_a_version_below_everything_held_for_the_key() {
        let mut hold = SkewHold::new(2, SKEW_HOLD_MAX_BYTES);
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, true),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(1, 5_500, 0, 1), 1_000, true),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(1, 4_000, 0, 1), 1_000, true),
            HoldOutcome::Full
        );
        assert_eq!(
            hold.hold(rec(2, 9_000, 0, 1), 1_000, true),
            HoldOutcome::Full,
            "a full hold does not evict for another key"
        );
        assert_eq!(hold.len(), 2);
    }

    #[test]
    fn a_hot_key_cannot_crowd_out_its_own_newest_value() {
        let mut hold = SkewHold::new(4, SKEW_HOLD_MAX_BYTES);
        for wall_ms in 5_000..5_100 {
            let outcome = hold.hold(rec(1, wall_ms, 0, 1), 1_000, false);
            assert_eq!(outcome, HoldOutcome::Held, "version {wall_ms}");
        }
        assert_eq!(hold.len(), 1);
        assert_eq!(hold.take_due(10_000)[0].ver.wall_ms, 5_099);
    }

    #[test]
    fn a_closed_hold_takes_and_releases_nothing_and_hands_back_what_it_held() {
        let mut hold = new_hold();
        assert_eq!(
            hold.hold(rec(2, 9_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        assert_eq!(
            hold.hold(rec(1, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Held
        );
        let handed: Vec<u8> = hold.close().iter().map(|r| r.key[0]).collect();
        assert_eq!(handed, [1, 2], "in due order");
        assert_eq!(
            hold.hold(rec(3, 5_000, 0, 1), 1_000, false),
            HoldOutcome::Closed
        );
        assert_eq!(
            hold.take_due(u64::MAX).len(),
            0,
            "a closed hold releases nothing"
        );
        assert_eq!((hold.len(), hold.bytes(), hold.next_due_ms()), (0, 0, None));
    }
}
