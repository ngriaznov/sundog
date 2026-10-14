//! Reference CRDT value types and the [`super::ConflictResolver`]s that merge
//! them through [`super::ConflictResolver::merge`].
//!
//! Every type here is built on `BTreeMap`/`BTreeSet` rather than a
//! hash-based collection, so its postcard encoding is canonical: two
//! logically equal values always encode to identical bytes. That property is
//! what lets a resolver's merge be checked for idempotence and associativity
//! at the byte level.

pub mod or_set;
mod pn_counter;
mod writer;

pub use or_set::{OrSet, OrSetResolver};
pub use pn_counter::{PnCounter, PnCounterResolver};
pub use writer::WriterId;

/// Whether a fold receipt stamped `stamp_ms` is past its lifetime at
/// `now_ms`: more than `ttl_ms` after the fold, or stamped more than `ttl_ms`
/// ahead of `now_ms`. The receipt stamp is a wall-clock instant read off the
/// folding replica's clock and merged by maximum, so one replica whose clock
/// runs far ahead would otherwise keep a receipt, and spread it, long after
/// every honest replica dropped theirs; a stamp a whole lifetime ahead of
/// this clock is wrong by more than any clock this crate trusts, so it
/// expires instead.
pub(super) fn receipt_expired(now_ms: u64, stamp_ms: u64, ttl_ms: u64) -> bool {
    now_ms.saturating_sub(stamp_ms) > ttl_ms || stamp_ms.saturating_sub(now_ms) > ttl_ms
}

#[cfg(test)]
mod tests {
    use super::receipt_expired;

    #[test]
    fn a_receipt_expires_a_lifetime_after_its_stamp() {
        assert!(!receipt_expired(1_000, 1_000, 500), "just stamped");
        assert!(!receipt_expired(1_500, 1_000, 500), "exactly one lifetime");
        assert!(receipt_expired(1_501, 1_000, 500), "past one lifetime");
    }

    #[test]
    fn a_receipt_expires_when_stamped_a_lifetime_ahead_of_now() {
        assert!(
            !receipt_expired(1_000, 1_500, 500),
            "exactly one lifetime ahead"
        );
        assert!(
            receipt_expired(1_000, 1_501, 500),
            "past one lifetime ahead"
        );
        assert!(receipt_expired(0, u64::MAX, 500), "a stamp at the far end");
    }

    #[test]
    fn a_zero_lifetime_keeps_only_a_receipt_stamped_now() {
        assert!(!receipt_expired(7, 7, 0));
        assert!(receipt_expired(8, 7, 0));
        assert!(receipt_expired(7, 8, 0));
    }
}
