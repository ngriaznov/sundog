//! Expiry encoding for `Live` entries, the idle-touch arithmetic and the absence check.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::explain::Lapse;
use crate::store::Mode;

use super::Live;

/// Sentinel [`Live::expiry`] marking "never expires".
pub(super) const NEVER_EXPIRES: u32 = u32::MAX;

/// Sentinel [`Live::expiry`] for a deadline past [`MAX_INLINE_TTL_MS`]
/// above `wall_ms`; the real deadline then lives in `Stripe::long_ttl`.
pub(super) const LONG_TTL: u32 = u32::MAX - 1;

/// The largest inline delta [`encode_expiry`] stores above `wall_ms`
/// (`u32::MAX - 2`, ~49.71 days); past this, [`LONG_TTL`] applies instead.
pub(super) const MAX_INLINE_TTL_MS: u64 = (u32::MAX - 2) as u64;

/// Encodes a write's `expires_at_ms` relative to `wall_ms`, for
/// [`Live::expiry`]: [`NEVER_EXPIRES`] for `None`, an exact delta within
/// [`MAX_INLINE_TTL_MS`], or [`LONG_TTL`] with the deadline as the pair's
/// `Some` second element, stored in `Stripe::long_ttl`.
pub(super) fn encode_expiry(wall_ms: u64, expires_at_ms: Option<u64>) -> (u32, Option<u64>) {
    match expires_at_ms {
        None => (NEVER_EXPIRES, None),
        Some(exp) => match exp.checked_sub(wall_ms) {
            // `exp < wall_ms` happens (a dead-on-arrival TTL, or a remote
            // write predating its own wall_ms); `expiry` only ever adds to
            // `wall_ms`, so this always takes the exact side-table path.
            Some(delta) if delta <= MAX_INLINE_TTL_MS => (
                u32::try_from(delta).expect("delta <= MAX_INLINE_TTL_MS < u32::MAX fits in a u32"),
                None,
            ),
            _ => (LONG_TTL, Some(exp)),
        },
    }
}

/// The sole reader of a stored [`Live::expiry`]: `None`, `long_ttl`'s
/// value, or `wall_ms` plus the inline delta, per the sentinel.
pub(super) fn decode_expiry(
    wall_ms: u64,
    expiry: u32,
    key_bytes: &[u8],
    long_ttl: &HashMap<Bytes, u64>,
) -> Option<u64> {
    match expiry {
        NEVER_EXPIRES => None,
        LONG_TTL => long_ttl.get(key_bytes).copied(),
        delta => Some(wall_ms + u64::from(delta)),
    }
}

/// The part of the 32-bit stamp ring reserved for a stamp that leads the
/// observer's `now_ms` (24 hours): a reader that sampled its clock before a
/// later touch, or a wall clock that stepped back, sees the stamp ahead of
/// it by at most this much, and reads zero idle.
pub(super) const MAX_STAMP_LEAD_MS: u64 = 86_400_000;

/// The largest idle gap [`idle_elapsed_ms`] reports and the ceiling
/// [`clamp_tti_ms`] enforces: the stamp ring less [`MAX_STAMP_LEAD_MS`]
/// (~48.71 days). Gaps and leads share one ring, so they cannot both own all
/// of it.
pub(super) const MAX_TTI_MS: u64 = (u32::MAX as u64) - MAX_STAMP_LEAD_MS;

const _: () = assert!(
    MAX_TTI_MS + MAX_STAMP_LEAD_MS == u32::MAX as u64 && MAX_TTI_MS <= MAX_INLINE_TTL_MS,
    "the idle gaps and the lead window split the stamp ring exactly, inside the inline ttl range"
);

/// Truncates `now_ms` to [`Live::last_access_ms`]'s stored form (the low 32
/// bits); [`idle_elapsed_ms`] recovers the true gap from this alone.
#[allow(
    clippy::cast_possible_truncation,
    reason = "the truncation is the point: idle_elapsed_ms recovers the true elapsed \
              duration from the low 32 bits alone via a wrapping subtraction"
)]
pub(super) fn touch_stamp(now_ms: u64) -> u32 {
    now_ms as u32
}

/// Milliseconds since an entry was last touched at
/// [`touch_stamp`]`(earlier now)`. Exact for a true gap up to
/// [`MAX_TTI_MS`], the ceiling [`clamp_tti_ms`] enforces. A stamp ahead of
/// `now_ms` by up to [`MAX_STAMP_LEAD_MS`] reads as zero idle: the reader
/// sampled its clock before a later touch, or the wall clock stepped back.
/// A lead past that window, or an unswept entry idle past [`MAX_TTI_MS`],
/// reads as a gap in the other region.
#[allow(
    clippy::cast_possible_truncation,
    reason = "matches touch_stamp's own truncation of now_ms before the wrapping subtraction"
)]
pub(super) fn idle_elapsed_ms(now_ms: u64, last_access_ms: u32) -> u64 {
    let gap = u64::from((now_ms as u32).wrapping_sub(last_access_ms));
    if gap > MAX_TTI_MS { 0 } else { gap }
}

/// Clamps a configured time-to-idle to [`MAX_TTI_MS`], past which
/// [`idle_elapsed_ms`] cannot tell an idle entry from a freshly touched one.
pub(super) fn clamp_tti_ms(tti_ms: u64) -> u64 {
    tti_ms.min(MAX_TTI_MS)
}

/// Whether a record view hides an entry that has run past its expiry.
/// Idleness is local to a node and never travels with a record, so both
/// views hide an idle entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lapsed {
    /// The readable view: an entry past its expiry or idle reads as absent.
    Hidden,
    /// The written view the fan-out reads: an entry past its expiry is
    /// returned with its (past) deadline, so a peer learns the write
    /// lapsed. An idle entry is still hidden.
    Shown,
}

/// Whether `live` has gone idle for `tti_ms` or longer at `now_ms`. Always
/// `false` with no time-to-idle configured.
fn idle_at<K, V>(live: &Live<K, V>, tti_ms: Option<u64>, now_ms: u64) -> bool {
    tti_ms.is_some_and(|tti| {
        idle_elapsed_ms(now_ms, live.last_access_ms.load(Ordering::Relaxed)) >= tti
    })
}

/// Whether a read of `live` at `now_ms` sees nothing: past its expiry, or
/// idle for `tti_ms` or longer. Lazy expiry and idle eviction both hinge on
/// this; a sweep only reclaims what it already reports absent.
pub(super) fn absent_at<K, V>(
    live: &Live<K, V>,
    key_bytes: &[u8],
    long_ttl: &HashMap<Bytes, u64>,
    tti_ms: Option<u64>,
    now_ms: u64,
) -> bool {
    absent_under(live, key_bytes, long_ttl, tti_ms, now_ms, Lapsed::Hidden)
}

/// [`absent_at`] under a record view: [`Lapsed::Shown`] leaves out the
/// expiry test, so only an idle entry is absent.
pub(super) fn absent_under<K, V>(
    live: &Live<K, V>,
    key_bytes: &[u8],
    long_ttl: &HashMap<Bytes, u64>,
    tti_ms: Option<u64>,
    now_ms: u64,
    lapsed: Lapsed,
) -> bool {
    if lapsed == Lapsed::Hidden
        && decode_expiry(live.wall_ms, live.expiry, key_bytes, long_ttl)
            .is_some_and(|exp| now_ms >= exp)
    {
        return true;
    }
    idle_at(live, tti_ms, now_ms)
}

/// Why an entry [`absent_at`] reports absent no longer reads: its expiry
/// passed at `now_ms`, or else it went idle. Pure; unit tested directly.
pub(super) fn lapse_cause(expires_at_ms: Option<u64>, now_ms: u64) -> Lapse {
    if expires_at_ms.is_some_and(|expires| now_ms >= expires) {
        Lapse::Expired
    } else {
        Lapse::Idle
    }
}

/// How long [`Engine::sweep`](super::Engine::sweep) keeps the tombstone of an
/// entry it removes for having expired: the same two retentions a delete's
/// tombstone gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TombstoneRetention {
    pub(crate) ttl_ms: u64,
    pub(crate) max_ttl_ms: u64,
}

/// What a sweep does with one live entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SweepFate {
    /// The entry is readable: it stays.
    Keep,
    /// The entry is removed and leaves nothing behind.
    Drop,
    /// The entry is removed and leaves a tombstone at its version, so an
    /// older copy of the key cannot come back after it.
    Tombstone,
}

/// A sweep's verdict on an entry. `absent` is what [`absent_at`] says;
/// `tombstones_expired` is whether this sweep leaves a tombstone for an
/// entry that expired. Only an expiry deadline that has passed leaves one:
/// idleness is local to a node, and a tombstone would delete the key on its
/// peers. The verdict reads the expiry alone, never the idle clock, so an
/// entry both expired and idle still leaves one. Pure; unit tested directly.
pub(super) fn sweep_fate(
    absent: bool,
    expires_at_ms: Option<u64>,
    now_ms: u64,
    tombstones_expired: bool,
) -> SweepFate {
    if !absent {
        SweepFate::Keep
    } else if tombstones_expired && expires_at_ms.is_some_and(|exp| now_ms >= exp) {
        SweepFate::Tombstone
    } else {
        SweepFate::Drop
    }
}

/// The instant a tombstone counts its retention from: the instant every
/// replica agrees on, capped at `now_ms`. That is the expiry
/// (`expired_at_ms`) for the tombstone of an expired entry and the delete's
/// own version stamp (`ver_wall_ms`) for a delete (`expired_at_ms` `None`).
/// The cap keeps a stamp from a clock ahead of this node's from extending
/// the hold: such a tombstone counts from this node's clock instead. Every replica
/// that learns of a tombstone after its agreed instant derives the same
/// anchor, whenever it learns. Pure; unit tested directly.
pub(super) fn tombstone_anchor_ms(
    expired_at_ms: Option<u64>,
    ver_wall_ms: u64,
    now_ms: u64,
) -> u64 {
    expired_at_ms.unwrap_or(ver_wall_ms).min(now_ms)
}

/// The two GC deadlines of a tombstone anchored at `anchor_ms`, as
/// `(ttl_deadline_ms, max_deadline_ms)`: the anchor plus the two
/// retentions, saturating. Pure; unit tested directly.
pub(super) fn tombstone_deadlines(anchor_ms: u64, ttl_ms: u64, max_ttl_ms: u64) -> (u64, u64) {
    (
        anchor_ms.saturating_add(ttl_ms),
        anchor_ms.saturating_add(max_ttl_ms),
    )
}

/// The deadlines of the tombstone a sweep at `now_ms` leaves for an entry
/// that expired at `expired_at_ms`: [`tombstone_deadlines`] from the expiry
/// capped at `now_ms`, and never earlier than `now_ms`, so the tombstone
/// outlives the sweep that made it even when the sweep runs long after the
/// expiry. Pure; unit tested directly.
pub(super) fn swept_tombstone_deadlines(
    expired_at_ms: u64,
    now_ms: u64,
    retention: TombstoneRetention,
) -> (u64, u64) {
    let (ttl, max) = tombstone_deadlines(
        tombstone_anchor_ms(Some(expired_at_ms), 0, now_ms),
        retention.ttl_ms,
        retention.max_ttl_ms,
    );
    (ttl.max(now_ms), max.max(now_ms))
}

/// Whether a tombstone with these deadlines is already past the retention
/// the next collection would apply at `now_ms`: its `ttl_deadline_ms`, or,
/// while collection is deferred (`deferring`), its `max_deadline_ms`. Pure;
/// unit tested directly.
pub(super) fn tombstone_lapsed(
    ttl_deadline_ms: u64,
    max_deadline_ms: u64,
    now_ms: u64,
    deferring: bool,
) -> bool {
    now_ms
        >= if deferring {
            max_deadline_ms
        } else {
            ttl_deadline_ms
        }
}

/// Whether [`apply_locked`](super::apply::apply_locked) drops a tombstone
/// instead of storing it: one that comes from a peer (`from_peer`) and has
/// already lapsed, past its `ttl_deadline_ms` or, while collection is
/// deferred, its `max_deadline_ms` (see [`tombstone_lapsed`]). A node's own
/// delete is never dropped. Whatever the key holds does not matter: the
/// outcome depends on the tombstone, the clock and the arrival alone, never
/// on which record reached the node first, so replicas that receive the same
/// records in any order store the same ones. A replica that collected such a
/// tombstone would otherwise take it back from a peer that has not collected
/// yet, with fresh deadlines, and the peers would hand it back and forth for
/// good. Pure; unit tested directly.
pub(super) fn drops_lapsed_tombstone(
    from_peer: bool,
    deadlines: (u64, u64),
    now_ms: u64,
    deferring: bool,
) -> bool {
    from_peer && tombstone_lapsed(deadlines.0, deadlines.1, now_ms, deferring)
}

/// The period of the tombstone gc task for a `tombstone_ttl` of `ttl_ms`: a
/// quarter of it, at least one second. The task ticks on this period and the
/// pause guard measures its gap against it ([`pause_gap_ms`]), so the two
/// cannot drift apart. Pure; unit tested directly.
pub(crate) fn gc_period_ms(ttl_ms: u64) -> u64 {
    (ttl_ms / 4).max(1_000)
}

/// How long a node may go without a tombstone collection before it counts as
/// paused: two gc periods. A healthy task collects once per period, so the
/// second period is the room for scheduler jitter. The gap is half the
/// `tombstone_ttl` for a ttl of 4 s or more; below that the one-second floor
/// of the period dominates. Pure; unit tested directly.
pub(crate) fn pause_gap_ms(ttl_ms: u64) -> u64 {
    gc_period_ms(ttl_ms).saturating_mul(2)
}

/// Whether the wall clock shows this node did not collect tombstones for
/// longer than [`pause_gap_ms`] before `now_ms`, given the reading of its
/// last collection (`last_gc_ms`, `0` before the first). A process that was
/// stopped, a VM that was suspended and a starved gc task all leave such a
/// gap, and none of them can have received the deletes its peers made
/// meanwhile. The comparison is strict and saturating, so a clock that stepped
/// back never reads as a pause. Pure; unit tested directly.
pub(crate) fn paused_since(last_gc_ms: u64, now_ms: u64, ttl_ms: u64) -> bool {
    last_gc_ms != 0 && now_ms.saturating_sub(last_gc_ms) > pause_gap_ms(ttl_ms)
}

/// The instant a pause hold lasts to after a collection at `now_ms`: a node
/// that `paused` defers for another `ttl_ms` from now, and a later pause
/// extends an earlier hold, never shortens it. Pure; unit tested directly.
pub(crate) fn pause_hold_after(hold_until_ms: u64, paused: bool, now_ms: u64, ttl_ms: u64) -> u64 {
    if paused {
        hold_until_ms.max(now_ms.saturating_add(ttl_ms))
    } else {
        hold_until_ms
    }
}

/// Whether a collection or an arrival at `now_ms` defers, as
/// `(effective, entered_hold)`: it defers when the gc task says so
/// (`defer`: an absent member, a returned member's settle window, the first
/// `tombstone_ttl` after the task starts, or the previous collection's
/// decision), when the node is `paused`, or while the hold from an earlier
/// pause (`hold_until_ms`) lasts. `entered_hold` is whether the pause starts
/// a hold: one that was not already inside one. Pure; unit tested directly.
pub(crate) fn gc_defer_decision(
    defer: bool,
    paused: bool,
    hold_until_ms: u64,
    now_ms: u64,
) -> (bool, bool) {
    let holding = now_ms < hold_until_ms;
    (defer || paused || holding, paused && !holding)
}

/// The retention a shard's sweep leaves the tombstone of an expired entry
/// under, or `None` when it leaves none: in a mode that runs no
/// anti-entropy ([`Mode::tombstones_expired_entries`]), or under a resolver
/// that merges. A merging cache already reads an expired stored side as
/// absent on every node, swept or not, so a write older than the expired
/// value starts a new value there rather than being an overwritten value
/// coming back; a tombstone would make that depend on whether the receiving
/// node has swept. Pure; unit tested directly.
pub(crate) fn expiry_tombstone_retention(
    mode: Mode,
    resolver_merges: bool,
    tombstone_ttl_ms: u64,
    tombstone_max_ttl_ms: u64,
) -> Option<TombstoneRetention> {
    (mode.tombstones_expired_entries() && !resolver_merges).then_some(TombstoneRetention {
        ttl_ms: tombstone_ttl_ms,
        max_ttl_ms: tombstone_max_ttl_ms,
    })
}
