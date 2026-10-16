//! Admission of accepted data-plane connections: the wait between failed
//! accepts and the rate-limited warning that reports them.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// The wait after the first failed accept in a row.
const ACCEPT_BACKOFF_MIN: Duration = Duration::from_millis(5);

/// The longest wait between two accept attempts while accepts keep failing.
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// The least time between two warnings of one kind.
const WARN_SPACING_MS: u64 = 60_000;

/// The wait after the `consecutive_failures`-th failed accept in a row:
/// [`ACCEPT_BACKOFF_MIN`] doubling per failure up to [`ACCEPT_BACKOFF_MAX`].
/// No failure, no wait.
pub(super) fn accept_backoff(consecutive_failures: u32) -> Duration {
    match consecutive_failures {
        0 => Duration::ZERO,
        n => ACCEPT_BACKOFF_MIN
            .saturating_mul(2u32.saturating_pow(n - 1))
            .min(ACCEPT_BACKOFF_MAX),
    }
}

/// Whether a failed accept of error kind `kind` makes the listener wait
/// before the next one. A connection that was reset or aborted before the
/// accept returned concerns that connection alone, and the next one is
/// ready at once. Every other kind, a process out of descriptors among them,
/// persists until something frees a resource, so retrying without a wait
/// spins.
pub(super) const fn accept_error_backs_off(kind: io::ErrorKind) -> bool {
    !matches!(
        kind,
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    )
}

/// Whether a warning is due at `now_ms`, given the instant of the last one
/// (`None` before the first). A clock that stepped back makes the next one
/// due.
const fn warn_due(last_warn_ms: Option<u64>, now_ms: u64) -> bool {
    match last_warn_ms {
        None => true,
        Some(last) => now_ms < last || now_ms - last >= WARN_SPACING_MS,
    }
}

/// One rate-limited warning slot: [`WarnLimiter::claim`] succeeds once per
/// [`WARN_SPACING_MS`], whichever thread asks.
struct WarnLimiter {
    /// The `crate::net::mono_ms` instant of the last warning, `u64::MAX`
    /// before the first.
    last_ms: AtomicU64,
}

impl WarnLimiter {
    const fn new() -> Self {
        Self {
            last_ms: AtomicU64::new(u64::MAX),
        }
    }

    /// Whether the caller may warn at `now_ms`. Of several callers racing
    /// for one due warning, one wins.
    fn claim(&self, now_ms: u64) -> bool {
        let last = self.last_ms.load(Ordering::Relaxed);
        warn_due((last != u64::MAX).then_some(last), now_ms)
            && self
                .last_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
    }
}

/// What a mesh keeps to admit the connections its listener accepts.
pub(in crate::net) struct Admission {
    warn_accept_error: WarnLimiter,
}

impl Admission {
    pub(in crate::net) const fn new() -> Self {
        Self {
            warn_accept_error: WarnLimiter::new(),
        }
    }

    /// Reports one failed accept: `consecutive_failures` backing-off
    /// failures in a row, the listener waiting `retry_in` before the next
    /// attempt. A failure that waits is warned about at most once a minute,
    /// on a limiter of its own, so a stream of per-connection failures,
    /// which wait for nothing and log at debug, never hides it.
    pub(super) fn note_accept_error(
        &self,
        error: &io::Error,
        consecutive_failures: u32,
        retry_in: Duration,
    ) {
        if retry_in.is_zero() {
            tracing::debug!(%error, "accepting a data-plane connection failed; the connection is skipped");
        } else if self.warn_accept_error.claim(crate::net::mono_ms()) {
            tracing::warn!(
                %error,
                os_error = error.raw_os_error(),
                consecutive_failures,
                retry_in_ms = u64::try_from(retry_in.as_millis()).unwrap_or(u64::MAX),
                "accepting a data-plane connection failed; a process out of file descriptors \
                 (EMFILE, ENFILE) fails this way until it frees one"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::sync::Arc;

    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn accept_backoff_doubles_from_five_milliseconds_to_one_second() {
        let waits: Vec<Duration> = (0..=10).map(accept_backoff).collect();
        assert_eq!(
            waits,
            [0, 5, 10, 20, 40, 80, 160, 320, 640, 1000, 1000].map(ms)
        );
    }

    #[test]
    fn accept_backoff_never_overflows_and_stays_within_its_bounds() {
        assert_eq!(accept_backoff(u32::MAX), ACCEPT_BACKOFF_MAX);
        let mut previous = Duration::ZERO;
        for failures in 0..=100 {
            let wait = accept_backoff(failures);
            assert!(wait >= previous, "non-decreasing at {failures}");
            if failures >= 1 {
                assert!(wait >= ACCEPT_BACKOFF_MIN, "at least the minimum");
            }
            assert!(wait <= ACCEPT_BACKOFF_MAX, "at most the maximum");
            previous = wait;
        }
    }

    #[test]
    fn the_backoff_bounds_are_five_milliseconds_and_one_second() {
        assert_eq!(ACCEPT_BACKOFF_MIN, ms(5));
        assert_eq!(ACCEPT_BACKOFF_MAX, Duration::from_secs(1));
    }

    #[test]
    fn only_a_per_connection_accept_error_skips_the_backoff() {
        for kind in [
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionReset,
            ErrorKind::Interrupted,
        ] {
            assert!(
                !accept_error_backs_off(kind),
                "{kind:?} concerns one connection"
            );
        }
        for kind in [
            ErrorKind::Other,
            ErrorKind::OutOfMemory,
            ErrorKind::PermissionDenied,
            ErrorKind::TimedOut,
            ErrorKind::ConnectionRefused,
            ErrorKind::BrokenPipe,
            ErrorKind::InvalidInput,
            ErrorKind::WouldBlock,
            ErrorKind::Unsupported,
            ErrorKind::AddrInUse,
        ] {
            assert!(accept_error_backs_off(kind), "{kind:?} persists");
        }
    }

    #[test]
    fn a_warning_is_due_before_the_first_after_the_spacing_and_after_a_clock_step_back() {
        assert!(warn_due(None, 0));
        assert!(!warn_due(Some(1_000), 1_000), "the same instant");
        assert!(!warn_due(Some(1_000), 1_000 + WARN_SPACING_MS - 1));
        assert!(warn_due(Some(1_000), 1_000 + WARN_SPACING_MS));
        assert!(warn_due(Some(5_000), 4_999), "the clock stepped back");
    }

    #[test]
    fn a_warn_limiter_grants_one_claim_per_spacing() {
        let limiter = WarnLimiter::new();
        assert!(limiter.claim(10), "the first claim wins");
        assert!(!limiter.claim(11), "an immediate second loses");
        assert!(!limiter.claim(10 + WARN_SPACING_MS - 1));
        assert!(
            limiter.claim(10 + WARN_SPACING_MS),
            "one after the spacing wins"
        );
        assert!(!limiter.claim(11 + WARN_SPACING_MS));
    }

    #[test]
    fn racing_claimants_win_one_warning_per_spacing_window() {
        let limiter = Arc::new(WarnLimiter::new());
        for window in 0..3u64 {
            let now = window * WARN_SPACING_MS;
            let winners: usize = (0..8)
                .map(|_| {
                    let limiter = Arc::clone(&limiter);
                    std::thread::spawn(move || (0..1_000).filter(|_| limiter.claim(now)).count())
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|thread| thread.join().expect("claimant does not panic"))
                .sum();
            assert_eq!(winners, 1, "window {window}");
        }
    }

    #[test]
    fn accept_errors_warn_on_their_own_limiter_and_per_connection_errors_never_claim_it() {
        let admission = Admission::new();
        let aborted = io::Error::from(ErrorKind::ConnectionAborted);
        for _ in 0..3 {
            admission.note_accept_error(&aborted, 0, Duration::ZERO);
        }
        assert_eq!(
            admission.warn_accept_error.last_ms.load(Ordering::Relaxed),
            u64::MAX,
            "a per-connection error leaves the warning slot untouched"
        );
        let exhausted = io::Error::other("too many open files");
        admission.note_accept_error(&exhausted, 1, ms(5));
        assert_ne!(
            admission.warn_accept_error.last_ms.load(Ordering::Relaxed),
            u64::MAX,
            "a backing-off error claims it"
        );
    }
}
