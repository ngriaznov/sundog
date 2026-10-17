//! Prometheus metrics export, behind a `prometheus` feature flag, off by
//! default. The `metrics::counter!`, `gauge!` and `histogram!` calls
//! spread across the crate are unconditional; without this feature they
//! fall through to `metrics`'s no-op default recorder. This module wires an
//! actual Prometheus recorder into the process, two ways:
//!
//! - [`crate::cluster::ClusterBuilder::prometheus_listen`] installs a recorder
//!   and serves `GET /metrics`, `GET /readyz`, and `GET /healthz` itself.
//! - [`prometheus_handle`] installs a recorder with no listener, for a process
//!   that serves `/metrics` from its own HTTP server.
//!
//! Both call `metrics::set_global_recorder`, a single process-global slot:
//! whichever runs second fails rather than replacing the first recorder.
//! Neither panics on that failure; see each function's `# Errors`.
//!
//! The listener answers `GET` and `HEAD` for `/metrics`, `/readyz`,
//! `/healthz` and its `/health` alias, and ignores a query string. `HEAD`
//! answers with the headers of the `GET` and no body, and any other method on
//! those paths gets `405 Method Not Allowed` with `Allow: GET, HEAD`. Any
//! other path gets `404 Not Found`, whatever the method. A client has
//! `REQUEST_TIMEOUT` (5 seconds) for the whole exchange: to send its request
//! head, the request line and the headers, of at most `MAX_REQUEST_HEAD`
//! (8 KiB), and to take the response. A connection that has not finished by
//! then is closed, and a head that has not arrived gets no response. A head
//! over the size limit, or a request line that is not `METHOD target
//! HTTP/version`, gets `400 Bad Request`. At most `MAX_CONNECTIONS` (64)
//! connections are served at once, and a connection over that is closed at
//! once.
//!
//! Both install the same recorder: each of [`DURATION_HISTOGRAMS`] renders
//! as Prometheus histogram buckets from [`LATENCY_BUCKETS`], which aggregate
//! across nodes in a Prometheus query, rather than as per-node quantiles,
//! which do not. The buckets match those names exactly, so a histogram the
//! application records through the same recorder renders as the exporter's
//! default summary.
//!
//! Install the recorder before opening a cache. A cache resolves its
//! per-cache handles (`sundog_cache_hits_total{cache}`,
//! `sundog_spill_entries{cache}`, and the like) once, when it opens, against
//! whichever recorder is global at that moment; a cache opened before the
//! real recorder installs keeps reporting to the no-op recorder.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub use metrics_exporter_prometheus::{BuildError, PrometheusHandle};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// The upper bounds, in seconds, of the buckets of each of
/// [`DURATION_HISTOGRAMS`]: 1µs to 10s in 1-2.5-5 steps. A resident read
/// lands in the first bucket, and a fetch that waits out its timeout in the
/// last ones.
pub const LATENCY_BUCKETS: &[f64] = &[
    0.000_001,
    0.000_002_5,
    0.000_005,
    0.000_01,
    0.000_025,
    0.000_05,
    0.000_1,
    0.000_25,
    0.000_5,
    0.001,
    0.002_5,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
];

/// Every histogram the crate records, each bucketed by [`LATENCY_BUCKETS`];
/// `sundog_spill_read_duration_seconds` is recorded only with the `spill`
/// feature.
pub const DURATION_HISTOGRAMS: &[&str] = &[
    "sundog_read_duration_seconds",
    "sundog_fetch_duration_seconds",
    "sundog_spill_read_duration_seconds",
];

/// The recorder both install paths use: [`LATENCY_BUCKETS`] for each of
/// [`DURATION_HISTOGRAMS`], matched by full name.
fn builder() -> PrometheusBuilder {
    DURATION_HISTOGRAMS
        .iter()
        .fold(PrometheusBuilder::new(), |builder, &name| {
            builder
                .set_buckets_for_metric(Matcher::Full(name.to_owned()), LATENCY_BUCKETS)
                .expect("invariant: LATENCY_BUCKETS is not empty")
        })
}

/// What `install_listener`'s `GET /readyz` route reports.
pub(crate) trait ReadinessSource: Send + Sync + 'static {
    /// Mirrors [`crate::cluster::Cluster::is_ready`].
    fn is_ready(&self) -> bool;
}

/// How long a client has to complete one request once it connects: send its
/// whole request head, request line and headers, and take the response. The
/// listener closes a connection that has not finished by then.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The most bytes of request line and headers the listener reads. A head
/// that has not ended within them gets `400 Bad Request`.
pub(crate) const MAX_REQUEST_HEAD: usize = 8 * 1024;

/// The most connections the listener serves at once. A connection over that
/// is closed as soon as it is accepted.
pub(crate) const MAX_CONNECTIONS: usize = 64;

/// The methods a known path answers, as the `Allow` header of a `405`.
const ALLOWED_METHODS: &str = "GET, HEAD";

/// The bounds `serve` enforces. `install_listener` passes `Limits::SERVED`;
/// the tests pass smaller ones.
#[derive(Clone, Copy, Debug)]
struct Limits {
    /// How long a client has to complete its exchange: send its whole request
    /// head and take the response.
    request_timeout: Duration,
    /// The most bytes of request line and headers read from a client.
    max_request_head: usize,
    /// The most connections served at once.
    max_connections: usize,
}

impl Limits {
    /// `REQUEST_TIMEOUT`, `MAX_REQUEST_HEAD` and `MAX_CONNECTIONS`: the bounds
    /// the listener runs under.
    const SERVED: Self = Self {
        request_timeout: REQUEST_TIMEOUT,
        max_request_head: MAX_REQUEST_HEAD,
        max_connections: MAX_CONNECTIONS,
    };
}

/// Installs a Prometheus recorder and serves `GET` and `HEAD` for
/// `/metrics`, `/readyz` (200 once `readiness` reports ready, 503 otherwise),
/// and `/healthz` on `addr`. Each request is bounded by `REQUEST_TIMEOUT`,
/// `MAX_REQUEST_HEAD` and `MAX_CONNECTIONS`; see the module docs.
///
/// # Errors
///
/// Returns [`BuildError`] if `addr` cannot be bound, or a `metrics`
/// recorder is already installed.
pub(crate) fn install_listener(
    addr: SocketAddr,
    readiness: Arc<dyn ReadinessSource>,
) -> Result<(), BuildError> {
    let handle = builder().install_recorder()?;
    let std_listener = std::net::TcpListener::bind(addr)
        .and_then(|listener| {
            listener.set_nonblocking(true)?;
            Ok(listener)
        })
        .map_err(|error| BuildError::FailedToCreateHTTPListener(error.to_string()))?;
    let listener = TcpListener::from_std(std_listener)
        .map_err(|error| BuildError::FailedToCreateHTTPListener(error.to_string()))?;
    tokio::spawn(upkeep(handle.clone()));
    tokio::spawn(serve(listener, handle, readiness, Limits::SERVED));
    Ok(())
}

/// Drains histogram buckets on the exporter's default cadence, the task its
/// built-in listener would otherwise run.
async fn upkeep(handle: PrometheusHandle) {
    loop {
        tokio::time::sleep(UPKEEP_INTERVAL).await;
        handle.run_upkeep();
    }
}

const UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

/// Accepts connections on `listener` forever, answering each on its own task.
/// A connection that arrives while `limits.max_connections` are being served
/// is dropped before anything is read from it. A connection's slot is held by
/// its task until the connection is closed, at the latest
/// `limits.request_timeout` after it arrives.
async fn serve(
    listener: TcpListener,
    handle: PrometheusHandle,
    readiness: Arc<dyn ReadinessSource>,
    limits: Limits,
) {
    let slots = Arc::new(Semaphore::new(limits.max_connections));
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
            tracing::debug!(
                %peer,
                max = limits.max_connections,
                "telemetry http connection closed over the connection cap"
            );
            continue;
        };
        let handle = handle.clone();
        let readiness = Arc::clone(&readiness);
        tokio::spawn(async move {
            let _slot = slot;
            if let Err(error) = respond(stream, &handle, readiness.as_ref(), limits).await {
                tracing::debug!(%peer, %error, "telemetry http connection ended early");
            }
        });
    }
}

/// Answers one request on `stream`, then closes it. The whole exchange, the
/// request head and the response, must finish within `limits.request_timeout`:
/// a connection that has not is closed, with no response if the head had not
/// arrived. A head the client abandons is closed without a response, and a head
/// over `limits.max_request_head` or with a malformed request line gets `400`.
async fn respond(
    mut stream: TcpStream,
    handle: &PrometheusHandle,
    readiness: &dyn ReadinessSource,
    limits: Limits,
) -> io::Result<()> {
    tokio::time::timeout(
        limits.request_timeout,
        answer(&mut stream, handle, readiness, limits.max_request_head),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "request not finished in time"))?
}

/// Reads one request head from `stream` and writes the response to it. The
/// caller bounds the time.
async fn answer(
    stream: &mut TcpStream,
    handle: &PrometheusHandle,
    readiness: &dyn ReadinessSource,
    max_request_head: usize,
) -> io::Result<()> {
    let head = read_head(&mut *stream, max_request_head).await?;
    let reply = match head {
        Head::Complete(line) => match parse_request_line(&line) {
            Some((method, target)) => {
                reply_for(route(method, target, readiness.is_ready()), handle)
            }
            None => Reply::empty("400 Bad Request"),
        },
        Head::TooLong => Reply::empty("400 Bad Request"),
        Head::Incomplete => return Err(io::ErrorKind::UnexpectedEof.into()),
    };
    stream.write_all(reply.render().as_bytes()).await?;
    stream.shutdown().await
}

/// What `read_head` found in the bytes a client sent.
#[derive(Debug, PartialEq, Eq)]
enum Head {
    /// The request line of a head that ended at its empty line, or at the
    /// client closing its sending side after the request line, even inside a
    /// header line.
    Complete(Vec<u8>),
    /// The head ran past the byte limit without ending.
    TooLong,
    /// The client closed its sending side before the request line ended, or
    /// before it sent anything.
    Incomplete,
}

/// Reads a request head from `stream` line by line, up to and including the
/// empty line that ends it, and reads no more than `max` bytes. A client that
/// closes its sending side after the request line, even inside a header line,
/// ends the head as well. The caller bounds the time.
async fn read_head<R: AsyncRead + Unpin>(stream: R, max: usize) -> io::Result<Head> {
    let limit = u64::try_from(max).unwrap_or(u64::MAX);
    let mut reader = BufReader::new(stream.take(limit));
    let mut request_line: Option<Vec<u8>> = None;
    let mut line = Vec::new();
    loop {
        line.clear();
        reader.read_until(b'\n', &mut line).await?;
        if line.last() != Some(&b'\n') {
            // The reader ended inside a line, or between two: the limit ran
            // out with the head unfinished, or the client closed.
            return Ok(match request_line {
                _ if reader.get_ref().limit() == 0 => Head::TooLong,
                Some(request_line) => Head::Complete(request_line),
                None => Head::Incomplete,
            });
        }
        let content = trim_line_end(&line);
        match request_line {
            None => request_line = Some(content.to_vec()),
            Some(done) if content.is_empty() => return Ok(Head::Complete(done)),
            Some(_) => {}
        }
    }
}

/// `line` without its `\n` or `\r\n` terminator.
fn trim_line_end(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// The method and target of a request line of the form `METHOD SP target SP
/// version`, where each part is one or more printable ASCII characters and the
/// version starts with `HTTP/`. `None` for any other line.
fn parse_request_line(line: &[u8]) -> Option<(&str, &str)> {
    let line = std::str::from_utf8(line).ok()?;
    let mut parts = line.split(' ');
    let (method, target, version) = (parts.next()?, parts.next()?, parts.next()?);
    let word = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_graphic());
    (parts.next().is_none()
        && word(method)
        && word(target)
        && version.strip_prefix("HTTP/").is_some_and(word))
    .then_some((method, target))
}

/// A body the listener serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Page {
    /// The rendered metrics.
    Metrics,
    /// `/readyz` on a ready node.
    Ready,
    /// `/readyz` on a node that is not ready.
    NotReady,
    /// `/healthz`, and its `/health` alias.
    Health,
}

/// What a request gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    /// A `GET` or `HEAD` of a known path.
    Serve {
        /// The body, which a `HEAD` leaves unsent.
        page: Page,
        /// Whether the request is a `HEAD`.
        head_only: bool,
    },
    /// A known path with a method other than `GET` or `HEAD`.
    MethodNotAllowed,
    /// A path the listener does not serve, whatever the method.
    NotFound,
}

/// Routes a request line's `method` and `target`. The path is the target
/// without its `?query` and `#fragment`. `ready` is what `/readyz` reports.
fn route(method: &str, target: &str, ready: bool) -> Route {
    let path = target.split(['?', '#']).next().unwrap_or_default();
    let page = match path {
        "/metrics" => Page::Metrics,
        "/readyz" if ready => Page::Ready,
        "/readyz" => Page::NotReady,
        "/healthz" | "/health" => Page::Health,
        _ => return Route::NotFound,
    };
    match method {
        "GET" => Route::Serve {
            page,
            head_only: false,
        },
        "HEAD" => Route::Serve {
            page,
            head_only: true,
        },
        _ => Route::MethodNotAllowed,
    }
}

/// One response: a status, a body, and which of the two go on the wire.
struct Reply {
    status: &'static str,
    /// Whether the response carries `Allow: GET, HEAD`.
    allow: bool,
    body: String,
    /// `false` for a `HEAD`: the headers advertise the body and omit it.
    send_body: bool,
}

impl Reply {
    /// A response with no body.
    fn empty(status: &'static str) -> Self {
        Self {
            status,
            allow: false,
            body: String::new(),
            send_body: true,
        }
    }

    /// The response bytes: headers, then the body unless `send_body` is
    /// `false`. `Content-Length` is the body's length either way.
    fn render(&self) -> String {
        let allow = if self.allow {
            format!("Allow: {ALLOWED_METHODS}\r\n")
        } else {
            String::new()
        };
        let body = if self.send_body { &self.body } else { "" };
        format!(
            "HTTP/1.1 {}\r\n{allow}Content-Type: text/plain; charset=utf-8\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{body}",
            self.status,
            self.body.len(),
        )
    }
}

/// The response `route` calls for.
fn reply_for(route: Route, handle: &PrometheusHandle) -> Reply {
    match route {
        Route::Serve { page, head_only } => {
            let (status, body) = match page {
                Page::Metrics => ("200 OK", handle.render()),
                Page::Ready => ("200 OK", "ready\n".to_string()),
                Page::NotReady => ("503 Service Unavailable", "not ready\n".to_string()),
                Page::Health => ("200 OK", "ok\n".to_string()),
            };
            Reply {
                status,
                allow: false,
                body,
                send_body: !head_only,
            }
        }
        Route::MethodNotAllowed => Reply {
            allow: true,
            ..Reply::empty("405 Method Not Allowed")
        },
        Route::NotFound => Reply::empty("404 Not Found"),
    }
}

/// Installs a Prometheus recorder with no listener, for a process that
/// serves `GET /metrics` via [`PrometheusHandle::render`] on its own HTTP
/// stack. The caller must call [`PrometheusHandle::run_upkeep`] on the
/// returned handle at a regular interval; this installs no loop for it.
///
/// # Errors
///
/// Returns [`BuildError`] if a `metrics` recorder is already installed.
pub fn prometheus_handle() -> Result<PrometheusHandle, BuildError> {
    builder().install_recorder()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use tokio::net::TcpSocket;

    #[test]
    fn latency_buckets_rise_from_a_microsecond_to_ten_seconds() {
        assert_eq!(LATENCY_BUCKETS.first(), Some(&0.000_001));
        assert_eq!(LATENCY_BUCKETS.last(), Some(&10.0));
        assert!(LATENCY_BUCKETS.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn the_builder_buckets_each_crate_histogram_and_no_other() {
        let recorder = builder().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            for &name in DURATION_HISTOGRAMS {
                metrics::histogram!(name).record(0.003);
            }
            metrics::histogram!("app_request_duration_seconds").record(0.003);
        });
        let body = handle.render();
        for &name in DURATION_HISTOGRAMS {
            assert!(body.contains(&format!("# TYPE {name} histogram")), "{body}");
            assert!(
                body.contains(&format!("{name}_bucket{{le=\"0.005\"}} 1")),
                "{body}"
            );
            assert!(
                body.contains(&format!("{name}_bucket{{le=\"0.0025\"}} 0")),
                "{body}"
            );
        }
        assert!(
            body.contains("# TYPE app_request_duration_seconds summary"),
            "an application histogram keeps the exporter's default: {body}"
        );
        assert!(
            !body.contains("app_request_duration_seconds_bucket"),
            "{body}"
        );
    }

    /// Every string literal in the crate's `src/` naming a `sundog_` metric
    /// that ends in `_duration_seconds` is in [`DURATION_HISTOGRAMS`], so a
    /// duration histogram added under such a name without buckets fails
    /// here.
    #[test]
    fn every_histogram_the_crate_records_is_bucketed() {
        fn rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("src/ reads") {
                let path = entry.expect("a directory entry").path();
                if path.is_dir() {
                    rust_sources(&path, out);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    out.push(path);
                }
            }
        }
        let mut files = Vec::new();
        rust_sources(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut files,
        );
        let mut named = std::collections::BTreeSet::new();
        for file in files {
            let source = std::fs::read_to_string(&file).expect("a source file reads");
            for (start, _) in source.match_indices("\"sundog_") {
                let rest = &source[start + 1..];
                let name = &rest[..rest.find('"').expect("a closing quote")];
                let metric_name = name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
                if metric_name && name.ends_with("_duration_seconds") {
                    named.insert(name.to_owned());
                }
            }
        }
        assert_eq!(
            named,
            DURATION_HISTOGRAMS
                .iter()
                .map(|&name| name.to_owned())
                .collect(),
            "every recorded histogram is in DURATION_HISTOGRAMS"
        );
    }

    struct Ready(bool);
    impl ReadinessSource for Ready {
        fn is_ready(&self) -> bool {
            self.0
        }
    }

    /// The most a test waits on a socket before it fails: a guard on the real
    /// clock, so a listener that never closes fails a test instead of
    /// hanging it.
    const GUARD: Duration = Duration::from_secs(5);

    /// The size of the `/metrics` body of the tests that need a response
    /// larger than the socket buffers on both ends.
    const LARGE_METRICS: usize = 8 * 1024 * 1024;

    /// The request timeout of the tests that watch a deadline pass.
    const SHORT: Duration = Duration::from_millis(300);

    /// The least time a connection with a [`SHORT`] deadline lives: the
    /// deadline less the slack of the timer and the clock reads.
    const SHORT_FLOOR: Duration = Duration::from_millis(250);

    /// Serves on a free loopback port under `limits`. `GET /metrics` renders
    /// what `record` records, and `/readyz` reports `ready`.
    async fn start_listener_recording(
        limits: Limits,
        ready: bool,
        record: impl FnOnce(),
    ) -> SocketAddr {
        let recorder = builder().build_recorder();
        metrics::with_local_recorder(&recorder, record);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(serve(
            listener,
            recorder.handle(),
            Arc::new(Ready(ready)),
            limits,
        ));
        addr
    }

    /// Serves on a free loopback port under `limits`. `GET /metrics` renders
    /// one counter, and `/readyz` reports `ready`.
    async fn start_listener(limits: Limits, ready: bool) -> SocketAddr {
        start_listener_recording(limits, ready, || {
            metrics::counter!("telemetry_test_requests_total").increment(1);
        })
        .await
    }

    /// What a client saw before the server closed the connection.
    struct Seen {
        /// The bytes received.
        text: String,
        /// Whether the server closed within [`GUARD`].
        closed: bool,
        /// Time from the connect to the close.
        elapsed: Duration,
    }

    impl Seen {
        /// The status line and headers.
        fn head(&self) -> &str {
            self.text.split("\r\n\r\n").next().unwrap_or_default()
        }

        /// What follows the headers.
        fn body(&self) -> &str {
            self.text
                .split_once("\r\n\r\n")
                .map_or("", |(_, body)| body)
        }

        /// The value of header `name`.
        fn header(&self, name: &str) -> Option<&str> {
            self.head()
                .lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
        }
    }

    /// Connects and writes each of `parts`, pausing after each so the server
    /// reads them separately.
    async fn send(addr: SocketAddr, parts: &[&str]) -> (TcpStream, Instant) {
        let started = Instant::now();
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        for part in parts {
            if stream.write_all(part.as_bytes()).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        (stream, started)
    }

    /// Reads until the server closes the connection, or [`GUARD`] passes.
    async fn read_until_closed(mut stream: TcpStream, started: Instant) -> Seen {
        let mut bytes = Vec::new();
        // A reset ends the read as a close does; what arrived before it is
        // in `bytes`.
        let closed = tokio::time::timeout(GUARD, stream.read_to_end(&mut bytes))
            .await
            .is_ok();
        Seen {
            text: String::from_utf8_lossy(&bytes).into_owned(),
            closed,
            elapsed: started.elapsed(),
        }
    }

    /// One exchange: writes `parts`, then reads the answer to the close.
    async fn exchange(addr: SocketAddr, parts: &[&str]) -> Seen {
        let (stream, started) = send(addr, parts).await;
        read_until_closed(stream, started).await
    }

    /// One `GET` of `path` on a listener under the default limits.
    async fn get_once(path: &str) -> Seen {
        let addr = start_listener(Limits::SERVED, true).await;
        exchange(addr, &[&format!("GET {path} HTTP/1.1\r\n\r\n")]).await
    }

    #[test]
    fn the_limits_are_five_seconds_eight_kib_and_sixty_four_connections() {
        assert_eq!(REQUEST_TIMEOUT, Duration::from_secs(5));
        assert_eq!(MAX_REQUEST_HEAD, 8192);
        assert_eq!(MAX_CONNECTIONS, 64);
        let served = Limits::SERVED;
        assert_eq!(served.request_timeout, Duration::from_secs(5));
        assert_eq!(served.max_request_head, 8192);
        assert_eq!(served.max_connections, 64);
    }

    #[test]
    fn a_known_path_answers_get_and_head_and_refuses_other_methods() {
        use Page::{Health, Metrics, NotReady, Ready};
        let served = |page, head_only| Route::Serve { page, head_only };
        let table = [
            ("GET", "/metrics", true, served(Metrics, false)),
            ("HEAD", "/metrics", true, served(Metrics, true)),
            ("GET", "/healthz", true, served(Health, false)),
            ("GET", "/health", true, served(Health, false)),
            ("HEAD", "/health", false, served(Health, true)),
            ("GET", "/readyz", true, served(Ready, false)),
            ("GET", "/readyz", false, served(NotReady, false)),
            ("HEAD", "/readyz", false, served(NotReady, true)),
            ("POST", "/metrics", true, Route::MethodNotAllowed),
            ("PUT", "/readyz", false, Route::MethodNotAllowed),
            ("DELETE", "/healthz", true, Route::MethodNotAllowed),
            ("OPTIONS", "/health", true, Route::MethodNotAllowed),
            ("get", "/metrics", true, Route::MethodNotAllowed),
        ];
        for (method, target, ready, expected) in table {
            assert_eq!(
                route(method, target, ready),
                expected,
                "{method} {target} ready={ready}"
            );
        }
    }

    #[test]
    fn the_path_is_the_target_without_its_query_and_fragment() {
        use Page::{Health, Metrics, Ready};
        let served = |page| Route::Serve {
            page,
            head_only: false,
        };
        let table = [
            ("/metrics?x=1", served(Metrics)),
            ("/metrics?", served(Metrics)),
            ("/readyz?probe=1&a=/healthz", served(Ready)),
            ("/healthz#top", served(Health)),
            ("/healthz?x=1#top", served(Health)),
            ("/metrics/", Route::NotFound),
            ("/metrics/extra?x=1", Route::NotFound),
            ("/Metrics", Route::NotFound),
            ("/", Route::NotFound),
            ("/?/metrics", Route::NotFound),
            ("metrics", Route::NotFound),
            ("*", Route::NotFound),
            ("", Route::NotFound),
        ];
        for (target, expected) in table {
            assert_eq!(route("GET", target, true), expected, "GET {target}");
        }
    }

    #[test]
    fn an_unknown_path_is_not_found_for_every_method() {
        for method in ["GET", "HEAD", "POST", "PUT", "DELETE", "BREW"] {
            assert_eq!(route(method, "/nope", true), Route::NotFound, "{method}");
        }
    }

    #[test]
    fn a_request_line_is_a_method_a_target_and_a_version_split_by_single_spaces() {
        type Parsed<'a> = Option<(&'a str, &'a str)>;
        let table: [(&[u8], Parsed); 14] = [
            (b"GET /metrics HTTP/1.1", Some(("GET", "/metrics"))),
            (b"GET /metrics HTTP/1.0", Some(("GET", "/metrics"))),
            (
                b"HEAD /readyz?probe=1 HTTP/1.1",
                Some(("HEAD", "/readyz?probe=1")),
            ),
            (b"POST * HTTP/2", Some(("POST", "*"))),
            (b"GET /metrics", None),
            (b"GET  /metrics HTTP/1.1", None),
            (b"GET /metrics  HTTP/1.1", None),
            (b"GET /metrics HTTP/1.1 extra", None),
            (b"GET /metrics HTTP/", None),
            (b"GET /metrics FTP/1.1", None),
            (b"GARBAGE", None),
            (b"", None),
            (b"GET /m\xc3\xa9trics HTTP/1.1", None),
            (b"GET /metrics HTTP/1.1\t", None),
        ];
        for (line, expected) in table {
            assert_eq!(
                parse_request_line(line),
                expected,
                "{:?}",
                String::from_utf8_lossy(line)
            );
        }
        assert_eq!(parse_request_line(b"GET /m\xff HTTP/1.1"), None);
    }

    #[tokio::test]
    async fn a_head_report_covers_complete_and_abandoned_heads() {
        fn complete(line: &str) -> Head {
            Head::Complete(line.as_bytes().to_vec())
        }
        let line = "GET /metrics HTTP/1.1";
        let table: [(&[u8], Head); 13] = [
            (b"GET /metrics HTTP/1.1\r\n\r\n", complete(line)),
            (b"GET /metrics HTTP/1.1\n\n", complete(line)),
            (
                b"GET /metrics HTTP/1.1\r\nHost: a\r\nAccept: */*\r\n\r\n",
                complete(line),
            ),
            // Bytes after the head are not read as part of it.
            (
                b"GET /metrics HTTP/1.1\r\n\r\nGET /x HTTP/1.1\r\n\r\n",
                complete(line),
            ),
            // The client closed after the request line, after a header, or
            // inside a header.
            (b"GET /metrics HTTP/1.1\r\n", complete(line)),
            (b"GET /metrics HTTP/1.1\r\nHost: a\r\n", complete(line)),
            (b"GET /metrics HTTP/1.1\r\nHost: a", complete(line)),
            // The client closed inside the request line, or before it.
            (b"GET /metrics HTT", Head::Incomplete),
            (b"GET /metrics HTTP/1.1", Head::Incomplete),
            (b"", Head::Incomplete),
            // An empty first line is a request line with nothing in it.
            (b"\r\n\r\n", complete("")),
            (b"\r\nGET /metrics HTTP/1.1\r\n\r\n", complete("")),
            // A header may hold any bytes.
            (
                b"GET /metrics HTTP/1.1\r\nX: \xff\xfe\r\n\r\n",
                complete(line),
            ),
        ];
        for (bytes, expected) in table {
            let head = read_head(bytes, MAX_REQUEST_HEAD).await.expect("reads");
            assert_eq!(head, expected, "{:?}", String::from_utf8_lossy(bytes));
        }
    }

    #[tokio::test]
    async fn a_head_of_exactly_the_limit_ends_and_one_byte_more_is_too_long() {
        let head = b"GET /metrics HTTP/1.1\r\nHost: a\r\n\r\n";
        let request_line = Head::Complete(b"GET /metrics HTTP/1.1".to_vec());
        let exact = read_head(&head[..], head.len()).await.expect("reads");
        assert_eq!(exact, request_line);
        let over = read_head(&head[..], head.len() - 1).await.expect("reads");
        assert_eq!(over, Head::TooLong);
        // The limit falls on a line boundary, with the empty line still to come.
        let boundary = read_head(&head[..], head.len() - 2).await.expect("reads");
        assert_eq!(boundary, Head::TooLong);
    }

    #[test]
    fn a_head_request_keeps_the_content_length_and_drops_the_body() {
        let reply = Reply {
            status: "200 OK",
            allow: false,
            body: "ok\n".to_string(),
            send_body: false,
        };
        let text = reply.render();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "{text}");
        assert!(text.contains("\r\nContent-Length: 3\r\n"), "{text}");
        assert!(
            text.ends_with("\r\n\r\n"),
            "no body follows the headers: {text}"
        );
        let get = Reply {
            send_body: true,
            ..reply
        };
        assert_eq!(get.render(), format!("{text}ok\n"));
    }

    #[test]
    fn a_refused_method_names_the_methods_a_path_answers() {
        let text = reply_for(
            Route::MethodNotAllowed,
            &builder().build_recorder().handle(),
        )
        .render();
        assert_eq!(
            text,
            "HTTP/1.1 405 Method Not Allowed\r\nAllow: GET, HEAD\r\nContent-Type: text/plain; \
             charset=utf-8\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let text = Reply::empty("404 Not Found").render();
        assert!(!text.contains("Allow:"), "{text}");
    }

    #[tokio::test]
    async fn a_query_string_is_ignored_when_routing() {
        let with_query = get_once("/readyz?x=1").await;
        assert!(
            with_query.text.starts_with("HTTP/1.1 200 OK"),
            "{}",
            with_query.text
        );
        assert_eq!(with_query.body(), "ready\n");
    }

    #[tokio::test]
    async fn a_request_line_split_across_writes_is_served() {
        let addr = start_listener(Limits::SERVED, true).await;
        let seen = exchange(addr, &["GET /rea", "dyz HTTP/1.1\r\n\r\n"]).await;
        assert!(seen.text.starts_with("HTTP/1.1 200 OK"), "{}", seen.text);
        assert_eq!(seen.body(), "ready\n");
    }

    #[tokio::test]
    async fn headers_split_across_writes_are_served() {
        let addr = start_listener(Limits::SERVED, true).await;
        let seen = exchange(
            addr,
            &[
                "GET /healthz HTTP/1.1\r\n",
                "Host: example\r\nAccept: */*\r\n",
                "User-Agent: scraper\r",
                "\n\r",
                "\n",
            ],
        )
        .await;
        assert!(seen.text.starts_with("HTTP/1.1 200 OK"), "{}", seen.text);
        assert_eq!(seen.body(), "ok\n");
    }

    #[tokio::test]
    async fn a_post_to_a_known_path_is_refused() {
        let addr = start_listener(Limits::SERVED, true).await;
        for path in ["/metrics", "/readyz", "/healthz", "/health"] {
            let seen = exchange(addr, &[&format!("POST {path} HTTP/1.1\r\n\r\n")]).await;
            assert!(
                seen.text.starts_with("HTTP/1.1 405 Method Not Allowed"),
                "POST {path}: {}",
                seen.text
            );
            assert_eq!(seen.header("Allow"), Some("GET, HEAD"));
            assert_eq!(seen.header("Connection"), Some("close"));
            assert_eq!(seen.header("Content-Length"), Some("0"));
            assert_eq!(seen.body(), "");
        }
    }

    #[tokio::test]
    async fn an_unknown_path_is_not_found_whatever_the_method() {
        let addr = start_listener(Limits::SERVED, true).await;
        for method in ["GET", "HEAD", "POST"] {
            let seen = exchange(addr, &[&format!("{method} /nope HTTP/1.1\r\n\r\n")]).await;
            assert!(
                seen.text.starts_with("HTTP/1.1 404 Not Found"),
                "{method} /nope: {}",
                seen.text
            );
            assert_eq!(seen.body(), "");
        }
    }

    #[tokio::test]
    async fn a_head_request_has_the_headers_of_a_get_and_no_body() {
        let addr = start_listener(Limits::SERVED, true).await;
        for path in ["/healthz", "/health", "/readyz", "/metrics"] {
            let get = exchange(addr, &[&format!("GET {path} HTTP/1.1\r\n\r\n")]).await;
            let head = exchange(addr, &[&format!("HEAD {path} HTTP/1.1\r\n\r\n")]).await;
            assert!(get.text.starts_with("HTTP/1.1 200 OK"), "{}", get.text);
            assert!(!get.body().is_empty(), "GET {path} has a body");
            assert_eq!(
                head.head(),
                get.head(),
                "HEAD {path} has the headers of GET"
            );
            assert_eq!(head.body(), "", "HEAD {path} sends no body");
            assert_eq!(
                head.header("Content-Length"),
                Some(get.body().len().to_string().as_str()),
                "HEAD {path} advertises the body GET sends"
            );
        }
    }

    #[tokio::test]
    async fn a_node_that_is_not_ready_answers_503_to_get_and_head() {
        let addr = start_listener(Limits::SERVED, false).await;
        let get = exchange(addr, &["GET /readyz HTTP/1.1\r\n\r\n"]).await;
        assert!(
            get.text.starts_with("HTTP/1.1 503 Service Unavailable"),
            "{}",
            get.text
        );
        assert_eq!(get.body(), "not ready\n");
        let head = exchange(addr, &["HEAD /readyz?probe=1 HTTP/1.1\r\n\r\n"]).await;
        assert_eq!(head.head(), get.head());
        assert_eq!(head.body(), "");
        let health = exchange(addr, &["GET /healthz HTTP/1.1\r\n\r\n"]).await;
        assert!(health.text.starts_with("HTTP/1.1 200 OK"), "liveness holds");
    }

    #[tokio::test]
    async fn the_client_closing_after_the_request_line_still_gets_an_answer() {
        let addr = start_listener(Limits::SERVED, true).await;
        let (mut stream, started) = send(addr, &["GET /healthz HTTP/1.0\r\n"]).await;
        stream.shutdown().await.expect("half close");
        let seen = read_until_closed(stream, started).await;
        assert!(seen.text.starts_with("HTTP/1.1 200 OK"), "{}", seen.text);
        assert_eq!(seen.body(), "ok\n");
    }

    #[tokio::test]
    async fn a_malformed_request_line_gets_400() {
        let addr = start_listener(Limits::SERVED, true).await;
        for request in [
            "GARBAGE\r\n\r\n",
            "GET /metrics\r\n\r\n",
            "GET  /metrics HTTP/1.1\r\n\r\n",
            "\r\n\r\n",
        ] {
            let seen = exchange(addr, &[request]).await;
            assert!(
                seen.text.starts_with("HTTP/1.1 400 Bad Request"),
                "{request:?}: {}",
                seen.text
            );
            assert_eq!(seen.header("Connection"), Some("close"));
            assert_eq!(seen.body(), "");
        }
    }

    #[tokio::test]
    async fn an_idle_connection_is_closed_by_the_server() {
        let addr = start_listener(
            Limits {
                request_timeout: SHORT,
                ..Limits::SERVED
            },
            true,
        )
        .await;
        let seen = exchange(addr, &[]).await;
        assert!(seen.closed, "the idle connection stayed open for {GUARD:?}");
        assert_eq!(
            seen.text, "",
            "no response goes to a client that sent nothing"
        );
    }

    #[tokio::test]
    async fn a_partial_head_is_closed_at_the_deadline_without_a_response() {
        let addr = start_listener(
            Limits {
                request_timeout: SHORT,
                ..Limits::SERVED
            },
            true,
        )
        .await;
        let seen = exchange(addr, &["GET /healthz HTTP/1.1\r\nHost: example\r\n"]).await;
        assert!(seen.closed, "the partial head stayed open for {GUARD:?}");
        assert_eq!(seen.text, "", "no response goes to a head that never ends");
        assert!(
            seen.elapsed >= SHORT_FLOOR,
            "closed after {:?}, before the {SHORT_FLOOR:?} floor of the {SHORT:?} deadline",
            seen.elapsed
        );
    }

    #[tokio::test]
    async fn a_head_that_trickles_in_is_closed_at_the_deadline_of_the_whole_head() {
        let addr = start_listener(
            Limits {
                request_timeout: SHORT,
                ..Limits::SERVED
            },
            true,
        )
        .await;
        let started = Instant::now();
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        // A header every 100 ms for a second: each write lands well inside
        // the deadline, and the head never ends. The server closes at the
        // deadline of the whole head, which a deadline that restarted on
        // every read would not reach.
        let mut closed_at = None;
        let mut buf = [0u8; 64];
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\n")
            .await
            .expect("write");
        for _ in 0..10 {
            if stream.write_all(b"X: 1\r\n").await.is_err() {
                closed_at = Some(started.elapsed());
                break;
            }
            match tokio::time::timeout(Duration::from_millis(100), stream.read(&mut buf)).await {
                Err(_) => {}
                Ok(read) => {
                    assert!(matches!(read, Ok(0) | Err(_)), "a response came: {read:?}");
                    closed_at = Some(started.elapsed());
                    break;
                }
            }
        }
        let closed_at = closed_at.expect("the server closed before the client's last header");
        assert!(
            closed_at >= SHORT_FLOOR,
            "closed after {closed_at:?}, before the {SHORT:?} deadline"
        );
    }

    #[tokio::test]
    async fn a_head_over_the_limit_gets_400() {
        let limits = Limits {
            max_request_head: 256,
            ..Limits::SERVED
        };
        let addr = start_listener(limits, true).await;
        // Exactly the limit, unfinished: the server has read all of it.
        let mut request = String::from("GET /healthz HTTP/1.1\r\nX-Pad: ");
        request.push_str(&"a".repeat(256 - request.len()));
        let seen = exchange(addr, &[&request]).await;
        assert!(
            seen.text.starts_with("HTTP/1.1 400 Bad Request"),
            "{}",
            seen.text
        );
        assert_eq!(seen.header("Connection"), Some("close"));
        assert_eq!(seen.body(), "");
        assert!(seen.closed);
    }

    #[tokio::test]
    async fn a_head_of_exactly_the_limit_is_served() {
        let limits = Limits {
            max_request_head: 256,
            ..Limits::SERVED
        };
        let addr = start_listener(limits, true).await;
        let mut request = String::from("GET /healthz HTTP/1.1\r\nX-Pad: ");
        request.push_str(&"a".repeat(256 - request.len() - 4));
        request.push_str("\r\n\r\n");
        assert_eq!(request.len(), 256);
        let seen = exchange(addr, &[&request]).await;
        assert!(seen.text.starts_with("HTTP/1.1 200 OK"), "{}", seen.text);
    }

    #[tokio::test]
    async fn connections_over_the_cap_are_closed_at_once_and_slots_free_at_the_deadline() {
        let timeout = Duration::from_secs(1);
        let timeout_floor = Duration::from_millis(900);
        let limits = Limits {
            request_timeout: timeout,
            max_connections: 2,
            ..Limits::SERVED
        };
        let addr = start_listener(limits, true).await;
        // Two idle connections hold both slots until their deadline.
        let (idle_a, started_a) = send(addr, &[]).await;
        let (idle_b, started_b) = send(addr, &[]).await;
        // The listener accepts in the order of arrival, so the third arrives
        // with both slots taken.
        let third = exchange(addr, &["GET /healthz HTTP/1.1\r\n\r\n"]).await;
        assert!(third.closed, "the third connection stayed open");
        assert_eq!(third.text, "", "a connection over the cap gets no response");
        assert!(
            third.elapsed < timeout,
            "closed after {:?}, not at once",
            third.elapsed
        );
        for (idle, started) in [(idle_a, started_a), (idle_b, started_b)] {
            let seen = read_until_closed(idle, started).await;
            assert!(seen.closed, "an idle connection outlived the deadline");
            assert_eq!(seen.text, "");
            assert!(seen.elapsed >= timeout_floor, "{:?}", seen.elapsed);
        }
        // The deadline freed both slots.
        for _ in 0..3 {
            let served = exchange(addr, &["GET /healthz HTTP/1.1\r\n\r\n"]).await;
            assert!(
                served.text.starts_with("HTTP/1.1 200 OK"),
                "{}",
                served.text
            );
        }
    }

    #[tokio::test]
    async fn a_client_that_does_not_read_its_response_loses_its_slot_at_the_deadline() {
        let limits = Limits {
            request_timeout: SHORT,
            max_connections: 1,
            ..Limits::SERVED
        };
        let addr = start_listener_recording(limits, true, || {
            metrics::counter!("telemetry_test_padded_total", "padding" => "x".repeat(LARGE_METRICS))
                .increment(1);
        })
        .await;
        // A small receive buffer on the client: the response outgrows it and
        // the listener's send buffer, so the listener cannot finish writing.
        let socket = TcpSocket::new_v4().expect("socket");
        socket.set_recv_buffer_size(4096).expect("receive buffer");
        let started = Instant::now();
        let mut stream = socket.connect(addr).await.expect("connect");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\n\r\n")
            .await
            .expect("write");
        // The client never reads, and keeps the stream until the test ends.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let over_the_cap = exchange(addr, &["GET /healthz HTTP/1.1\r\n\r\n"]).await;
        assert_eq!(
            over_the_cap.text, "",
            "the unread response holds the only slot, or the metrics fit the socket buffers"
        );
        let mut served_at = None;
        while started.elapsed() < GUARD {
            let seen = exchange(addr, &["GET /healthz HTTP/1.1\r\n\r\n"]).await;
            if seen.text.starts_with("HTTP/1.1 200 OK") {
                served_at = Some(started.elapsed());
                break;
            }
        }
        let served_at = served_at.expect("the unread response held the slot past the deadline");
        assert!(
            served_at >= SHORT_FLOOR,
            "the slot freed after {served_at:?}, before the {SHORT:?} deadline"
        );
    }

    #[tokio::test]
    async fn a_finished_request_frees_its_slot() {
        let limits = Limits {
            max_connections: 1,
            ..Limits::SERVED
        };
        let addr = start_listener(limits, true).await;
        for round in 0..5 {
            let served = exchange(addr, &["GET /healthz HTTP/1.1\r\n\r\n"]).await;
            assert!(
                served.text.starts_with("HTTP/1.1 200 OK"),
                "request {round}: {}",
                served.text
            );
        }
    }
}
