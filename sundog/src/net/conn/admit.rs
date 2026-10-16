//! Admission of accepted data-plane connections: the deadline and the cap on
//! connections that have not sent their `Hello`, the wait between failed
//! accepts, and the counters and rate-limited warnings that report them.

use std::collections::HashSet;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How long an accepted connection has, from the moment its handler first
/// runs, to finish its TLS handshake (on a node that runs TLS) and send a
/// complete `Hello`. A connection that has not by then is closed, so the
/// real bound is this plus the time the handler waits to be scheduled.
///
/// Equal to the request timeout: a dialer abandons a request exchange, its
/// connection and handshake included, after that long, so the acceptor never
/// cuts a dial that is still waited on.
pub(crate) const HELLO_TIMEOUT: Duration = crate::net::request::REQUEST_TIMEOUT;

/// How many connections from sources that are not mesh peers wait for their
/// `Hello` at once. A connection from such a source that arrives with this
/// many already waiting is closed as it is accepted.
pub(super) const MAX_PRE_HELLO_CONNS: usize = 512;

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

/// What the accept loop does with one accepted connection.
pub(super) enum Admit {
    /// The source is a mesh peer's address: no slot, and the cap never
    /// refuses it. The `Hello` deadline and frame limit still apply.
    Known,
    /// Any other source with room under the cap: the connection holds
    /// `permit` until its `Hello` decodes or it closes.
    Waiting(OwnedSemaphorePermit),
    /// Any other source with the cap full: closed at once.
    OverCap,
}

/// What a mesh keeps to admit the connections its listener accepts.
pub(in crate::net) struct Admission {
    /// One permit per connection from an unknown source waiting for its
    /// `Hello`.
    gate: Arc<Semaphore>,
    cap: usize,
    /// The IPs of the data addresses of the mesh's current peers, kept by
    /// `Mesh::update_peers`. A peer behind NAT, or whose advertised address
    /// differs from the one it dials from, is not in it and takes the gate
    /// like any other source; so does every source before the first peer
    /// table.
    known_sources: RwLock<HashSet<IpAddr>>,
    warn_over_cap: WarnLimiter,
    warn_hello_timeout: WarnLimiter,
    warn_accept_error: WarnLimiter,
}

impl Admission {
    pub(in crate::net) fn new() -> Self {
        Self::with_cap(MAX_PRE_HELLO_CONNS)
    }

    pub(in crate::net) fn with_cap(cap: usize) -> Self {
        Self {
            gate: Arc::new(Semaphore::new(cap)),
            cap,
            known_sources: RwLock::new(HashSet::new()),
            warn_over_cap: WarnLimiter::new(),
            warn_hello_timeout: WarnLimiter::new(),
            warn_accept_error: WarnLimiter::new(),
        }
    }

    /// Replaces the set of source addresses the cap never refuses with
    /// `sources`. An IPv4 address mapped into IPv6 counts as the IPv4
    /// address, as a dual-stack listener reports it.
    pub(in crate::net) fn set_known_sources(&self, sources: impl IntoIterator<Item = IpAddr>) {
        let sources = sources.into_iter().map(|ip| ip.to_canonical()).collect();
        *self
            .known_sources
            .write()
            .expect("invariant: known_sources lock is never poisoned") = sources;
    }

    /// Whether `source` is the IP of a current peer's data address.
    pub(in crate::net) fn is_known_source(&self, source: IpAddr) -> bool {
        self.known_sources
            .read()
            .expect("invariant: known_sources lock is never poisoned")
            .contains(&source.to_canonical())
    }

    /// Decides the fate of a connection accepted from `source`.
    pub(super) fn admit(&self, source: IpAddr) -> Admit {
        if self.is_known_source(source) {
            return Admit::Known;
        }
        match Arc::clone(&self.gate).try_acquire_owned() {
            Ok(permit) => Admit::Waiting(permit),
            Err(_no_permits) => Admit::OverCap,
        }
    }

    /// The connections from unknown sources waiting for their `Hello`.
    #[cfg(test)]
    pub(in crate::net) fn in_flight(&self) -> usize {
        self.cap - self.gate.available_permits()
    }

    #[cfg(test)]
    pub(in crate::net) const fn cap(&self) -> usize {
        self.cap
    }

    /// Counts a connection closed because the cap was full in
    /// `sundog_accept_dropped_total{reason="over_cap"}` and warns about it at
    /// most once a minute.
    pub(super) fn note_over_cap(&self, peer: SocketAddr) {
        metrics::counter!("sundog_accept_dropped_total", "reason" => "over_cap").increment(1);
        if self.warn_over_cap.claim(crate::net::mono_ms()) {
            tracing::warn!(
                %peer,
                cap = self.cap,
                "closed a data-plane connection: the cap on connections from unknown sources \
                 that wait for their hello is full; the data-plane port is for cluster \
                 members only, and sundog_accept_dropped_total{{reason=\"over_cap\"}} counts \
                 every such close"
            );
        }
    }

    /// Counts a connection closed at [`HELLO_TIMEOUT`] in
    /// `sundog_accept_dropped_total{reason="hello_timeout"}` and warns about
    /// it at most once a minute.
    pub(super) fn note_hello_timeout(&self, peer: Option<SocketAddr>) {
        metrics::counter!("sundog_accept_dropped_total", "reason" => "hello_timeout").increment(1);
        if self.warn_hello_timeout.claim(crate::net::mono_ms()) {
            tracing::warn!(
                peer = peer.map(|addr| addr.to_string()),
                timeout_secs = HELLO_TIMEOUT.as_secs(),
                "closed a data-plane connection that sent no complete hello in time; the \
                 data-plane port is for cluster members only, and \
                 sundog_accept_dropped_total{{reason=\"hello_timeout\"}} counts every such close"
            );
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
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::sync::Arc;

    use super::*;
    use crate::net::conn::framing::HELLO_MAX_FRAME;
    use crate::node::NodeId;
    use crate::wire::{self, Msg};

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

    fn ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))
    }

    /// The permit of an admission that must wait under the cap.
    fn waiting(admission: &Admission, source: IpAddr) -> OwnedSemaphorePermit {
        match admission.admit(source) {
            Admit::Waiting(permit) => permit,
            Admit::Known => panic!("{source} is not a known source"),
            Admit::OverCap => panic!("{source} is refused under the cap"),
        }
    }

    #[test]
    fn the_cap_is_512_connections_from_unknown_sources() {
        assert_eq!(MAX_PRE_HELLO_CONNS, 512);
        let admission = Admission::new();
        assert_eq!(admission.cap(), 512);
        assert_eq!(admission.in_flight(), 0);
    }

    #[test]
    fn unknown_sources_are_admitted_up_to_the_cap_and_the_next_is_refused() {
        let admission = Admission::with_cap(3);
        let permits: Vec<_> = (0..3).map(|n| waiting(&admission, ip(n))).collect();
        assert_eq!(admission.in_flight(), 3);
        assert!(matches!(admission.admit(ip(9)), Admit::OverCap));
        assert!(matches!(admission.admit(ip(0)), Admit::OverCap));
        assert_eq!(admission.in_flight(), 3, "a refusal takes no slot");
        drop(permits);
        assert_eq!(admission.in_flight(), 0);
    }

    #[test]
    fn a_dropped_permit_frees_its_slot_for_the_next_connection() {
        let admission = Admission::with_cap(1);
        let first = waiting(&admission, ip(1));
        assert!(matches!(admission.admit(ip(2)), Admit::OverCap));
        drop(first);
        let _second = waiting(&admission, ip(2));
        assert_eq!(admission.in_flight(), 1);
    }

    #[test]
    fn a_cap_of_zero_admits_no_unknown_source() {
        let admission = Admission::with_cap(0);
        assert!(matches!(admission.admit(ip(1)), Admit::OverCap));
        admission.set_known_sources([ip(1)]);
        assert!(matches!(admission.admit(ip(1)), Admit::Known));
    }

    #[test]
    fn a_known_source_takes_no_slot_and_is_admitted_with_the_gate_full() {
        let admission = Admission::with_cap(2);
        admission.set_known_sources([ip(7)]);
        let _held: Vec<_> = (0..2).map(|n| waiting(&admission, ip(n))).collect();
        assert!(matches!(admission.admit(ip(9)), Admit::OverCap));

        for _ in 0..10 {
            assert!(matches!(admission.admit(ip(7)), Admit::Known));
        }
        assert_eq!(admission.in_flight(), 2, "a known source holds no slot");
    }

    #[test]
    fn every_source_takes_the_general_gate_until_a_peer_is_known() {
        let admission = Admission::with_cap(1);
        assert!(!admission.is_known_source(ip(1)));
        let _held = waiting(&admission, ip(1));
        assert!(matches!(admission.admit(ip(1)), Admit::OverCap));
    }

    #[test]
    fn setting_the_known_sources_replaces_the_set() {
        let admission = Admission::with_cap(1);
        admission.set_known_sources([ip(1), ip(2)]);
        assert!(admission.is_known_source(ip(1)));
        assert!(admission.is_known_source(ip(2)));
        admission.set_known_sources([ip(2), ip(3)]);
        assert!(
            !admission.is_known_source(ip(1)),
            "a departed peer is dropped"
        );
        assert!(admission.is_known_source(ip(2)));
        assert!(admission.is_known_source(ip(3)));
        admission.set_known_sources([]);
        assert!(!admission.is_known_source(ip(2)));
    }

    #[test]
    fn an_ipv4_address_mapped_into_ipv6_matches_its_ipv4_peer() {
        let admission = Admission::with_cap(1);
        let mapped = IpAddr::V6(Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped());
        admission.set_known_sources([ip(1)]);
        assert!(admission.is_known_source(mapped));
        admission.set_known_sources([mapped]);
        assert!(admission.is_known_source(ip(1)));
        assert!(!admission.is_known_source(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn racing_connections_never_exceed_the_cap() {
        let admission = Arc::new(Admission::with_cap(64));
        let permits: Vec<OwnedSemaphorePermit> = (0..8)
            .map(|_| {
                let admission = Arc::clone(&admission);
                std::thread::spawn(move || {
                    (0..1_000u32)
                        .filter_map(|n| {
                            match admission.admit(ip(u8::try_from(n % 200).expect("under 200"))) {
                                Admit::Waiting(permit) => Some(permit),
                                Admit::Known | Admit::OverCap => None,
                            }
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|thread| thread.join().expect("the thread does not panic"))
            .collect();
        assert_eq!(permits.len(), 64);
        assert_eq!(admission.in_flight(), 64);
    }

    #[test]
    fn over_cap_closes_are_counted_by_reason() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _metrics = metrics::set_default_local_recorder(&recorder);
        let admission = Admission::with_cap(1);
        let peer: SocketAddr = "10.0.0.1:4000".parse().expect("valid addr");

        admission.note_over_cap(peer);
        admission.note_over_cap(peer);
        admission.note_hello_timeout(Some(peer));

        // One snapshot: reading resets the counters it reads.
        let counted: std::collections::HashMap<String, metrics_util::debugging::DebugValue> =
            snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .filter(|(key, ..)| key.key().name() == "sundog_accept_dropped_total")
                .filter_map(|(key, _, _, value)| {
                    let reason = key.key().labels().find(|l| l.key() == "reason")?;
                    Some((reason.value().to_owned(), value))
                })
                .collect();
        assert_eq!(
            counted["over_cap"],
            metrics_util::debugging::DebugValue::Counter(2)
        );
        assert_eq!(
            counted["hello_timeout"],
            metrics_util::debugging::DebugValue::Counter(1)
        );
        assert_eq!(counted.len(), 2, "no other reason: {:?}", counted.keys());
    }

    #[test]
    fn the_hello_deadline_is_ten_seconds_and_no_shorter_than_a_request() {
        assert_eq!(HELLO_TIMEOUT, Duration::from_secs(10));
        assert!(HELLO_TIMEOUT >= crate::net::request::REQUEST_TIMEOUT);
    }

    /// A `Hello` never grows past 128 bytes; anything larger goes in a later
    /// message gated on the peer's protocol. An acceptor of every release
    /// that carries the pre-Hello frame limit closes a connection whose
    /// first frame exceeds it, so a `Hello` that outgrew `HELLO_MAX_FRAME`
    /// would be refused by every such peer, and raising the limit in the
    /// release that grows the `Hello` does not reach them.
    #[test]
    fn the_largest_hello_encodes_to_at_most_128_bytes() {
        let largest = Msg::Hello {
            node: NodeId::from(u64::MAX),
            incarnation: u64::MAX,
            protocol: u16::MAX,
            cluster: u64::MAX,
        };
        let len = wire::encode(&largest).expect("encodes").len();
        assert!(len <= 128, "the largest hello encodes to {len} bytes");
        const {
            assert!(
                128 * 8 <= HELLO_MAX_FRAME,
                "the pre-hello frame limit leaves eightfold room over a hello"
            );
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
