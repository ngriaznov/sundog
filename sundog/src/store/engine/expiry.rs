//! Expiry encoding for `Live` entries, the idle-touch arithmetic and the absence check.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::explain::Lapse;

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
/// [`touch_stamp`]`(earlier now)`, correct as long as the true gap is under
/// ~49.71 days, the ceiling [`clamp_tti_ms`] enforces.
#[allow(
    clippy::cast_possible_truncation,
    reason = "matches touch_stamp's own truncation of now_ms before the wrapping subtraction"
)]
pub(super) fn idle_elapsed_ms(now_ms: u64, last_access_ms: u32) -> u64 {
    u64::from((now_ms as u32).wrapping_sub(last_access_ms))
}

/// Clamps a configured time-to-idle to [`MAX_INLINE_TTL_MS`], past which
/// [`idle_elapsed_ms`] never fires and the entry stays readable forever.
pub(super) fn clamp_tti_ms(tti_ms: u64) -> u64 {
    tti_ms.min(MAX_INLINE_TTL_MS)
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
    if let Some(exp) = decode_expiry(live.wall_ms, live.expiry, key_bytes, long_ttl)
        && now_ms >= exp
    {
        return true;
    }
    if let Some(tti) = tti_ms {
        let last = live.last_access_ms.load(Ordering::Relaxed);
        if idle_elapsed_ms(now_ms, last) >= tti {
            return true;
        }
    }
    false
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
