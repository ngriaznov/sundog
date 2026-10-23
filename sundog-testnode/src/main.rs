//! Container test-node: embeds the sundog library behind a tiny line-based
//! control protocol, so the rightsize harness (`sundog/tests/container_util`)
//! can drive a real cluster member from outside its container. Built as a
//! static musl binary with no libc dependency.
//!
//! Usage: `sundog-testnode <cluster-name>`; every other setting comes from
//! `SUNDOG_*` environment variables (seeds, anti-entropy bucket overrides,
//! cache mode and capacity, spill tier, conflict resolver): see each
//! variable's read site in [`run`] for its meaning and default.
//! `SUNDOG_TESTNODE_MAX_ENTRIES` and the `_SPILL_*_MB` knobs mirror
//! `sundog-distributed-demo`'s own sizing flags; each has an older
//! byte-denominated counterpart that wins when both are set.
//!
//! Line protocol: one command per line on `CONTROL_PORT`, one
//! line-terminated reply each; each command's argument shape and reply
//! string is documented on the match arm or handler that implements it, in
//! [`TestNode::dispatch`] and the `*_command` functions it calls.
//!
//! Built with the `prometheus` feature, every member run also serves
//! `GET /metrics` (and `/readyz`, `/healthz`) on `METRICS_PORT`; an observer
//! serves none. Built with the `spill`
//! feature, `"it"` can open a `SpillConfig` disk tier; see
//! `spill_config_from_env`. A SIGTERM, sent by a container stop, leaves the
//! cluster and exits 0, checkpointing a warm-reopen spill tier; `quit` and
//! `crash` exit without leaving.
//!
//! `SUNDOG_TESTNODE_ROLE=observer` runs a [`sundog::observe::Observer`]
//! instead of a cluster member: it binds gossip on `{bind_ip}:7946`, joins
//! through `SUNDOG_SEEDS`, opens no cache and serves three control lines,
//! `members`, `share <cache> <owners> <node-id>` and `quit`; see
//! `ObserverNode::dispatch`.
//!
//! `explain <key>` answers with one line of JSON, `Cache::explain`'s report
//! on `"it"` for that key: the record this node stores, and in a
//! `Mode::Distributed` cache the ownership view, the owners, the residency
//! marks, how a fetch decides and what each other owner answers. The line's
//! shape is the `explain_reply` module's, and the four files under
//! `sundog-lens/tests/fixtures/explain/` hold the encoder's output for four
//! replies, which the lens parses in its own tests.
//!
//! `SUNDOG_TESTNODE_LOADER_DELAY_MS` registers a loader on `"it"`: each call
//! waits that long, then answers `src-{key}.{n}` for every key, `n`
//! counting this node's loader calls, which the `loads` line reports.
//! `SUNDOG_TESTNODE_TTL_MS` gives `"it"` a default TTL and
//! `SUNDOG_TESTNODE_REFRESH_AHEAD` its refresh-ahead fraction; see
//! `loading_from_env`.
//!
//! The ports are fixed unless overridden: `SUNDOG_TESTNODE_GOSSIP_PORT`
//! (default 7946), `SUNDOG_TESTNODE_CONTROL_PORT` (8080) and, for a member
//! built with the `prometheus` feature, `SUNDOG_TESTNODE_METRICS_PORT`
//! (9090). Several nodes then share one address, each on its own ports. An
//! observer honors the gossip and control overrides. A value that is not a
//! port number stops the process with an error.

use std::env;
use std::io::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "spill")]
use sundog::SpillConfig;
use sundog::crdt::{OrSet, OrSetResolver, PnCounter, PnCounterResolver, WriterId};
use sundog::observe::{ClusterSnapshot, MemberStatus, Observer};
use sundog::{
    Cache, CacheBuilder, Cluster, ClusterConfig, ConflictResolver, Merged, Mode, NodeId,
    RecordView, Winner,
};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, TcpStream};
use xxhash_rust::xxh3::xxh3_64;

mod explain_reply;

const GOSSIP_PORT: u16 = 7946;
const CONTROL_PORT: u16 = 8080;
/// Bound only with the `prometheus` feature: `GET /metrics`/`/readyz`/
/// `/healthz` via `sundog::ClusterBuilder::prometheus_listen`.
#[cfg(feature = "prometheus")]
const METRICS_PORT: u16 = 9090;
const CACHE_NAME: &str = "it";
const CHURN_CACHE_NAME: &str = "churn";
/// The dedicated `PnCounter` cache `pnfill`/`pncount`/`pnget` operate on,
/// always merging under [`PnCounterResolver`].
const PN_CACHE_NAME: &str = "pn";
/// The dedicated `OrSet<String>` cache `osadd`/`osremove`/`osmembers`
/// operate on, always merging under [`OrSetResolver`].
const OS_CACHE_NAME: &str = "os";
/// Short enough that early `churn` writes expire while the run continues.
const CHURN_TTL: Duration = Duration::from_secs(2);
/// `churn` wraps keys modulo this, so churners on different nodes collide.
const CHURN_KEYSPACE: u32 = 512;
/// The fixed key `bigput`/`bigverify` operate on.
const BIG_ONE_KEY: &str = "bigone";
const BIG_ONE_INDEX: u32 = u32::MAX;

/// Deterministic large-value content: `index`'s hex digits cycled to `len`
/// bytes, so any node can regenerate and byte-compare it locally.
fn big_value(index: u32, len: usize) -> String {
    format!("{index:08x}").chars().cycle().take(len).collect()
}

/// `ok` if `stored` matches [`big_value`]`(index, len)`, `bad` otherwise.
fn verdict(stored: &str, index: u32, len: usize) -> String {
    if stored == big_value(index, len) {
        "ok".to_string()
    } else {
        format!("bad len={} want={len}", stored.len())
    }
}

/// An order-independent digest of `cache`'s live content: XORs
/// `xxh3_64(key ++ 0x00 ++ value)` (both as UTF-8 bytes, `0x00` never
/// appearing in either since `fill`/`put`/`churn` never write it) over every
/// live entry. XOR makes the fold order-independent, so two nodes holding the
/// same set of entries compute the same digest regardless of insertion
/// order or [`Cache::keys`]'s iteration order; any single entry differing in
/// key, value, or presence changes the result.
async fn digest_it(cache: &Cache<String, String>) -> u64 {
    let mut digest = 0u64;
    for key in cache.keys() {
        let Some(value) = cache.get(&key).await else {
            // Expired or removed between `keys()` and `get`: no contribution,
            // matching a digest taken once it's already gone.
            continue;
        };
        let mut buf = Vec::with_capacity(key.len() + 1 + value.len());
        buf.extend_from_slice(key.as_bytes());
        buf.push(0);
        buf.extend_from_slice(value.as_bytes());
        digest ^= xxh3_64(&buf);
    }
    digest
}

/// jemalloc instead of the platform allocator (not on MSVC): glibc retains
/// about twice the resident set live entries need under bulk ingest.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    install_tracing();
    if let Err(error) = run().await {
        eprintln!("sundog-testnode: {error}");
        std::process::exit(1);
    }
}

/// The `EnvFilter` directive [`install_tracing`] builds from: `raw`
/// verbatim when it parses, `"warn"` when unset or invalid.
fn log_filter_directive(raw: Option<&str>) -> &str {
    match raw {
        Some(value) if tracing_subscriber::EnvFilter::try_new(value).is_ok() => value,
        _ => "warn",
    }
}

/// Installs the process-wide `tracing` subscriber: stderr only, uncolored,
/// filtered by [`log_filter_directive`]'s reading of `RUST_LOG`. Leaves
/// stdout untouched for the harness's control protocol.
fn install_tracing() {
    let raw_log = env::var("RUST_LOG").ok();
    let directive = log_filter_directive(raw_log.as_deref());
    let filter = tracing_subscriber::EnvFilter::try_new(directive)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_target(true)
        .with_env_filter(filter)
        .init();
}

/// Parses an env override's raw string into a `usize`, `None` for an absent
/// or unparsable value: the pure decision [`usize_env`] wraps around a real
/// `std::env::var` read, for [`run`]'s `ae_part_min_bucket`/
/// `ae_sketch_min_bucket` overrides.
fn parse_usize_override(raw: Option<&str>) -> Option<usize> {
    raw?.parse().ok()
}

/// Reads `name` as a `usize` env override via [`parse_usize_override`].
fn usize_env(name: &str) -> Option<usize> {
    parse_usize_override(env::var(name).ok().as_deref())
}

/// [`parse_usize_override`] for a `u64`, backing
/// `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES`/`SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES`/
/// `SUNDOG_TESTNODE_SPILL_REGION_BYTES`.
fn parse_u64_override(raw: Option<&str>) -> Option<u64> {
    raw?.parse().ok()
}

/// Reads `name` as a `u64` env override via [`parse_u64_override`].
fn u64_env(name: &str) -> Option<u64> {
    parse_u64_override(env::var(name).ok().as_deref())
}

/// Parses `raw` as a boolean env override: `"true"` is `true`, else
/// `false`. Backs `SUNDOG_TESTNODE_WARM_REOPEN`, default `false`.
#[cfg(feature = "spill")]
fn parse_bool_override(raw: Option<&str>) -> bool {
    raw == Some("true")
}

/// Reads `name` as a `bool` env override via [`parse_bool_override`].
#[cfg(feature = "spill")]
fn bool_env(name: &str) -> bool {
    parse_bool_override(env::var(name).ok().as_deref())
}

/// Decides `"it"`'s [`Mode`] from `SUNDOG_TESTNODE_MODE`/`SUNDOG_TESTNODE_OWNERS`'s
/// already-read values: `mode` absent or `"replicated"` is always
/// `Mode::Replicated` (`owners` is only meaningful in distributed mode, so
/// it is ignored here rather than rejected); `"distributed"` is
/// `Mode::distributed()` when `owners` is absent, or `Mode::Distributed`
/// with that count when present and at least 2. Any other `mode` string, or
/// an `owners` under 2, is an `Err` naming the problem.
/// The IP every listener binds, `SUNDOG_TESTNODE_BIND_IP`: all interfaces
/// by default, as a container wants, or one loopback address apiece when
/// many test nodes share a host.
fn bind_ip_from_env(raw: Option<&str>) -> Result<IpAddr, String> {
    raw.map_or(Ok(IpAddr::from([0, 0, 0, 0])), |raw| {
        raw.parse()
            .map_err(|error| format!("SUNDOG_TESTNODE_BIND_IP must be an IP address: {error}"))
    })
}

/// A listener port from the env override `name`, whose already-read value is
/// `raw`: `default` when absent, an `Err` naming `name` when the value is not
/// a port number. Backs `SUNDOG_TESTNODE_GOSSIP_PORT`,
/// `SUNDOG_TESTNODE_CONTROL_PORT` and `SUNDOG_TESTNODE_METRICS_PORT`.
fn port_from_env(name: &str, raw: Option<&str>, default: u16) -> Result<u16, String> {
    raw.map_or(Ok(default), |raw| {
        raw.parse()
            .map_err(|error| format!("{name} must be a port number, got {raw:?}: {error}"))
    })
}

/// Reads the port override `name` through [`port_from_env`].
fn port_env(name: &str, default: u16) -> Result<u16, String> {
    port_from_env(name, env::var(name).ok().as_deref(), default)
}

/// Whether the replicated side caches the `churn`, `pn*` and `os*` commands
/// drive open, `SUNDOG_TESTNODE_SIDE_CACHES`: on unless it reads `off`, so
/// a scale run exercises `"it"` alone.
fn side_caches_from_env(raw: Option<&str>) -> Result<bool, String> {
    match raw {
        None | Some("on") => Ok(true),
        Some("off") => Ok(false),
        Some(other) => Err(format!(
            "SUNDOG_TESTNODE_SIDE_CACHES must be \"on\" or \"off\", got {other:?}"
        )),
    }
}

fn mode_from_env(mode: Option<&str>, owners: Option<u8>) -> Result<Mode, String> {
    match mode {
        None | Some("replicated") => Ok(Mode::Replicated),
        Some("invalidation") => Ok(Mode::Invalidation),
        Some("distributed") => match owners {
            None => Ok(Mode::distributed()),
            Some(owners) => std::num::NonZeroU8::new(owners)
                .filter(|owners| owners.get() >= 2)
                .map(|owners| Mode::Distributed { owners })
                .ok_or_else(|| {
                    format!(
                        "SUNDOG_TESTNODE_OWNERS must be at least 2, got {owners}: a single owner \
                         is a lost bucket the instant it leaves"
                    )
                }),
        },
        Some(other) => Err(format!(
            "SUNDOG_TESTNODE_MODE must be \"replicated\", \"invalidation\" or \"distributed\", \
             got {other:?}"
        )),
    }
}

/// How many times this node's `"it"` loader has run, for the `loads` line.
static LOADER_CALLS: AtomicU64 = AtomicU64::new(0);

/// `"it"`'s loading options, from `SUNDOG_TESTNODE_LOADER_DELAY_MS`,
/// `SUNDOG_TESTNODE_TTL_MS` and `SUNDOG_TESTNODE_REFRESH_AHEAD`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Loading {
    /// A loader that waits this long a call; `None`, no loader.
    loader_delay: Option<Duration>,
    ttl: Option<Duration>,
    refresh_ahead: Option<f64>,
}

/// Parses `"it"`'s loading options from those three variables' raw values,
/// each optional; an unparsable value is an error. Pure; unit tested
/// directly.
fn loading_from_env(
    loader_delay_ms: Option<&str>,
    ttl_ms: Option<&str>,
    refresh_ahead: Option<&str>,
) -> Result<Loading, String> {
    let millis = |name: &str, raw: Option<&str>| {
        raw.map(|raw| {
            raw.parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|error| format!("{name} must be a u64 of milliseconds: {error}"))
        })
        .transpose()
    };
    Ok(Loading {
        loader_delay: millis("SUNDOG_TESTNODE_LOADER_DELAY_MS", loader_delay_ms)?,
        ttl: millis("SUNDOG_TESTNODE_TTL_MS", ttl_ms)?,
        refresh_ahead: refresh_ahead
            .map(|raw| {
                raw.parse::<f64>().map_err(|error| {
                    format!("SUNDOG_TESTNODE_REFRESH_AHEAD must be a fraction: {error}")
                })
            })
            .transpose()?,
    })
}

/// Applies `loading` to `"it"`'s builder: the TTL, a loader answering
/// `src-{key}.{n}` after its delay, and refresh-ahead.
fn apply_loading(
    mut builder: CacheBuilder<String, String>,
    loading: Loading,
) -> CacheBuilder<String, String> {
    if let Some(ttl) = loading.ttl {
        builder = builder.ttl(ttl);
    }
    if let Some(delay) = loading.loader_delay {
        builder = builder.batch_loader(move |keys: Vec<String>| async move {
            let call = LOADER_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::time::sleep(delay).await;
            Ok::<_, std::convert::Infallible>(
                keys.into_iter()
                    .map(|key| {
                        let value = format!("src-{key}.{call}");
                        (key, value)
                    })
                    .collect::<std::collections::HashMap<_, _>>(),
            )
        });
    }
    if let Some(fraction) = loading.refresh_ahead {
        builder = builder.refresh_ahead(fraction);
    }
    builder
}

/// Weighs one entry by its UTF-8 byte length, key plus value: the
/// byte-counting weigher `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` installs on
/// `"it"` so its `max_capacity` bounds bytes rather than entry count.
/// Saturates at `u32::MAX` rather than panicking on a pathologically large
/// single entry.
fn byte_weight(key: &str, value: &str) -> u32 {
    (key.len() + value.len()).try_into().unwrap_or(u32::MAX)
}

/// Which unit `"it"`'s optional `CacheBuilder::max_capacity` bounds, from
/// `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES`/`SUNDOG_TESTNODE_MAX_ENTRIES`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapacityCap {
    /// Bounds total UTF-8 bytes across key and value, via [`byte_weight`].
    Bytes(u64),
    /// Bounds live entry count via the default per-entry weigher.
    Entries(u64),
}

/// Decides [`CapacityCap`] from the two env knobs: a byte budget wins when
/// both are set (the older knob); `None` leaves `"it"` unbounded.
fn capacity_cap_from_env(
    max_capacity_bytes: Option<u64>,
    max_entries: Option<u64>,
) -> Option<CapacityCap> {
    max_capacity_bytes
        .map(CapacityCap::Bytes)
        .or(max_entries.map(CapacityCap::Entries))
}

/// One mebibyte, in bytes: `sundog-distributed-demo`'s own `MIB`, backing
/// the `_MB`-suffixed spill sizing knobs below.
#[cfg(feature = "spill")]
const MIB: u64 = 1024 * 1024;

/// The byte value for one spill sizing knob: `bytes` if set, otherwise
/// `mib` converted (saturating on overflow), else `None`. The byte
/// variable always wins when both are set.
#[cfg(feature = "spill")]
fn resolve_byte_budget(bytes: Option<u64>, mib: Option<u64>) -> Option<u64> {
    bytes.or_else(|| mib.map(|mib| mib.saturating_mul(MIB)))
}

/// Builds `"it"`'s optional spill tier from
/// `SUNDOG_TESTNODE_SPILL_DIR`/`SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES`/
/// `SUNDOG_TESTNODE_SPILL_REGION_BYTES`/
/// `SUNDOG_TESTNODE_SPILL_FLUSH_QUEUE_BYTES`/`SUNDOG_TESTNODE_WARM_REOPEN`'s
/// already-parsed values (byte knobs folded with their `_MB` counterpart):
/// `None` when no spill dir is set. `warm_reopen` defaults to `false`.
///
/// # Panics
///
/// Panics if `dir` is set but `capacity_bytes` is not: a spill dir with no
/// budget is a misconfigured test run, not a state to open a node in.
#[cfg(feature = "spill")]
fn spill_config_from_env(
    dir: Option<String>,
    capacity_bytes: Option<u64>,
    region_bytes: Option<u64>,
    flush_queue_bytes: Option<u64>,
    warm_reopen: bool,
) -> Option<SpillConfig> {
    let dir = dir?;
    let capacity_bytes = capacity_bytes.expect(
        "SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES must be set alongside SUNDOG_TESTNODE_SPILL_DIR",
    );
    let mut cfg = SpillConfig::new(dir, capacity_bytes).warm_reopen(warm_reopen);
    if let Some(region_bytes) = region_bytes {
        cfg = cfg.region_bytes(region_bytes);
    }
    if let Some(flush_queue_bytes) = flush_queue_bytes {
        cfg = cfg.flush_queue_bytes(flush_queue_bytes);
    }
    Some(cfg)
}

/// Which `ConflictResolver` `"it"` opens with, decided from
/// `SUNDOG_TESTNODE_RESOLVER`'s raw string by [`resolver_kind_from_env`]; the
/// actual `Arc<dyn ConflictResolver>` this maps to is built separately in
/// [`run`], mirroring [`mode_from_env`]'s own split between deciding and
/// constructing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolverKind {
    /// `"it"` opens with no resolver override, keeping `CacheBuilder`'s
    /// default `LwwResolver`.
    Lww,
    /// `"it"` opens with [`SumCounterResolver`] installed.
    SumCounter,
}

/// Decides [`ResolverKind`] from `SUNDOG_TESTNODE_RESOLVER`'s already-read
/// value: absent or `"lww"` keeps the default resolver, `"sum_counter"`
/// installs [`SumCounterResolver`]. Any other value is an `Err` naming the
/// problem.
fn resolver_kind_from_env(raw: Option<&str>) -> Result<ResolverKind, String> {
    match raw {
        None | Some("lww") => Ok(ResolverKind::Lww),
        Some("sum_counter") => Ok(ResolverKind::SumCounter),
        Some(other) => Err(format!(
            "SUNDOG_TESTNODE_RESOLVER must be \"lww\" or \"sum_counter\", got {other:?}"
        )),
    }
}

/// Decodes a postcard-encoded `String` and parses it as a decimal `u64`
/// counter; `None` if either step fails.
fn decode_counter(bytes: &[u8]) -> Option<u64> {
    postcard::from_bytes::<String>(bytes)
        .ok()
        .and_then(|value| value.parse().ok())
}

/// The merged counter's postcard-encoded decimal-string bytes when both `a`
/// and `b` parse as `u64` counters via [`decode_counter`], `None` otherwise:
/// the pure decision [`SumCounterResolver::merge`] wraps around a real
/// `RecordView` pair.
fn merge_counter_bytes(a: &[u8], b: &[u8]) -> Option<Vec<u8>> {
    let (a, b) = (decode_counter(a)?, decode_counter(b)?);
    postcard::to_stdvec(&(a + b).to_string()).ok()
}

/// Merges two decimal-string counters by addition on a genuine version
/// conflict, rather than picking whichever write is most recent, so a real
/// `ConflictResolver::merge` outcome, and the sentinel-stamped `Hlc` sundog
/// stamps it with, reaches the wire, falling back to plain `Hlc` order
/// whenever either side fails to parse as a `u64` (a tombstone, a spilled
/// view, or a non-numeric value) to match `sundog::crdt`'s own resolvers'
/// decode-failure fallback: summing on every merge is not idempotent
/// (`merge(a, a) != a`), so this is not a general-purpose CRDT resolver the
/// way `sundog::crdt`'s are, existing only to drive one interop scenario in
/// the container test suite, which triggers exactly one merge on one key
/// and never redelivers it.
struct SumCounterResolver;

impl ConflictResolver for SumCounterResolver {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn merge(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Option<Merged> {
        let (Some(av), Some(bv)) = (a.value, b.value) else {
            return None;
        };
        let merged = merge_counter_bytes(av, bv)?;
        Some(Merged {
            value: Bytes::from(merged),
            expires_at_ms: None,
        })
    }
}

/// Applies `cap` (from [`capacity_cap_from_env`]) to `builder`: a byte
/// budget installs the byte weigher with `max_capacity`; an entry count
/// installs `max_capacity` and `capacity_hint` together. No cap is a no-op.
fn apply_capacity_cap(
    mut builder: CacheBuilder<String, String>,
    cap: Option<CapacityCap>,
) -> CacheBuilder<String, String> {
    match cap {
        Some(CapacityCap::Bytes(max_capacity_bytes)) => {
            builder = builder
                .max_capacity(max_capacity_bytes)
                .weigher(|key: &String, value: &String| byte_weight(key, value));
        }
        Some(CapacityCap::Entries(max_entries)) => {
            builder = builder.max_capacity(max_entries).capacity_hint(max_entries);
        }
        None => {}
    }
    builder
}

/// Opens `"it"` with `resolver` and its sizing knobs: the RAM cap and,
/// with `spill`, the spill tier. Split out of [`run`] to keep it under
/// clippy's line-count lint.
async fn open_it_cache(
    cluster: &Cluster,
    mode: Mode,
    resolver: ResolverKind,
) -> Result<Cache<String, String>, Box<dyn std::error::Error>> {
    let max_capacity_bytes = u64_env("SUNDOG_TESTNODE_MAX_CAPACITY_BYTES");
    let max_entries = u64_env("SUNDOG_TESTNODE_MAX_ENTRIES");
    let mut it_builder = cluster.cache::<String, String>(CACHE_NAME).mode(mode);
    if resolver == ResolverKind::SumCounter {
        it_builder = it_builder.resolver(Arc::new(SumCounterResolver));
    }
    let it_builder = apply_capacity_cap(
        it_builder,
        capacity_cap_from_env(max_capacity_bytes, max_entries),
    );
    let it_builder = apply_loading(
        it_builder,
        loading_from_env(
            env::var("SUNDOG_TESTNODE_LOADER_DELAY_MS").ok().as_deref(),
            env::var("SUNDOG_TESTNODE_TTL_MS").ok().as_deref(),
            env::var("SUNDOG_TESTNODE_REFRESH_AHEAD").ok().as_deref(),
        )?,
    );
    #[cfg(feature = "spill")]
    let it_builder = {
        let spill_cfg = spill_config_from_env(
            env::var("SUNDOG_TESTNODE_SPILL_DIR").ok(),
            resolve_byte_budget(
                u64_env("SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES"),
                u64_env("SUNDOG_TESTNODE_SPILL_CAPACITY_MB"),
            ),
            resolve_byte_budget(
                u64_env("SUNDOG_TESTNODE_SPILL_REGION_BYTES"),
                u64_env("SUNDOG_TESTNODE_SPILL_REGION_MB"),
            ),
            resolve_byte_budget(
                u64_env("SUNDOG_TESTNODE_SPILL_FLUSH_QUEUE_BYTES"),
                u64_env("SUNDOG_TESTNODE_SPILL_FLUSH_QUEUE_MB"),
            ),
            bool_env("SUNDOG_TESTNODE_WARM_REOPEN"),
        );
        match spill_cfg {
            Some(spill_cfg) => it_builder.spill(spill_cfg),
            None => it_builder,
        }
    };
    Ok(it_builder.open().await?)
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cluster_name = env::args()
        .nth(1)
        .ok_or("usage: sundog-testnode <cluster-name>")?;
    let seeds = resolve_seeds(&env::var("SUNDOG_SEEDS").unwrap_or_default()).await;
    let role = role_from_env(env::var("SUNDOG_TESTNODE_ROLE").ok().as_deref())?;
    let bind_ip = bind_ip_from_env(env::var("SUNDOG_TESTNODE_BIND_IP").ok().as_deref())?;
    let gossip_port = port_env("SUNDOG_TESTNODE_GOSSIP_PORT", GOSSIP_PORT)?;
    let control_port = port_env("SUNDOG_TESTNODE_CONTROL_PORT", CONTROL_PORT)?;
    if role == Role::Observer {
        return run_observer(cluster_name, seeds, bind_ip, gossip_port, control_port).await;
    }
    #[cfg(feature = "prometheus")]
    let metrics_port = port_env("SUNDOG_TESTNODE_METRICS_PORT", METRICS_PORT)?;
    let ae_part_min_bucket = usize_env("SUNDOG_TESTNODE_AE_PART_MIN_BUCKET");
    let ae_sketch_min_bucket = usize_env("SUNDOG_TESTNODE_AE_SKETCH_MIN_BUCKET");
    let crdt_retire_after_secs = u64_env("SUNDOG_TESTNODE_CRDT_RETIRE_AFTER_SECS");
    let owners = match env::var("SUNDOG_TESTNODE_OWNERS").ok() {
        None => None,
        Some(raw) => Some(
            raw.parse::<u8>()
                .map_err(|error| format!("SUNDOG_TESTNODE_OWNERS must be a u8: {error}"))?,
        ),
    };
    let it_mode = mode_from_env(env::var("SUNDOG_TESTNODE_MODE").ok().as_deref(), owners)?;
    let it_resolver = resolver_kind_from_env(env::var("SUNDOG_TESTNODE_RESOLVER").ok().as_deref())?;

    let side_caches =
        side_caches_from_env(env::var("SUNDOG_TESTNODE_SIDE_CACHES").ok().as_deref())?;
    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = SocketAddr::new(bind_ip, gossip_port);
        c.data_bind_addr = SocketAddr::new(bind_ip, 0);
        // Faster than the default so container tests converge in seconds.
        c.ae_interval = Duration::from_secs(2);
        // Covers the 16 s bucket release window a distributed cache needs.
        c.tombstone_ttl = Duration::from_secs(20);
        if let Some(min_bucket) = ae_part_min_bucket {
            c.ae_part_min_bucket = min_bucket;
        }
        if let Some(min_bucket) = ae_sketch_min_bucket {
            c.ae_sketch_min_bucket = min_bucket;
        }
        if let Some(secs) = crdt_retire_after_secs {
            c.crdt_retire_after = Duration::from_secs(secs);
        }
    });

    #[cfg_attr(not(feature = "prometheus"), allow(unused_mut))]
    let mut builder = Cluster::builder(cluster_name).seeds(seeds).config(config);
    #[cfg(feature = "prometheus")]
    {
        builder = builder.prometheus_listen(SocketAddr::new(bind_ip, metrics_port));
    }
    let cluster = builder.build().await?;

    let cache = open_it_cache(&cluster, it_mode, it_resolver).await?;
    let side = if side_caches {
        Some(SideCaches::open(&cluster).await?)
    } else {
        None
    };
    let node = TestNode {
        cache,
        side,
        cluster,
    };
    serve_until_stopped(bind_ip, control_port, Control::Node(node)).await
}

/// The `SUNDOG_TESTNODE_ROLE=observer` process: an [`Observer`] bound to
/// `{bind_ip}:{gossip_port}` and seeded from `seeds`, serving the control
/// lines of [`ObserverNode::dispatch`] on `{bind_ip}:{control_port}`.
async fn run_observer(
    cluster_name: String,
    seeds: Vec<SocketAddr>,
    bind_ip: IpAddr,
    gossip_port: u16,
    control_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = SocketAddr::new(bind_ip, gossip_port);
    });
    let observer = Observer::builder(cluster_name)
        .seeds(seeds)
        .config(config)
        .build()
        .await?;
    serve_until_stopped(
        bind_ip,
        control_port,
        Control::Observer(ObserverNode { observer }),
    )
    .await
}

/// Serves `control` on `{bind_ip}:{control_port}` after printing
/// `testnode-ready`, and leaves gossip and exits 0 on the stop signal.
async fn serve_until_stopped(
    bind_ip: IpAddr,
    control_port: u16,
    control: Control,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind((bind_ip, control_port)).await?;
    // A container stop sends SIGTERM: leave the cluster and checkpoint a
    // warm-reopen spill tier; `quit`/`crash` exit with no leave.
    let stop = stop_requested();
    tokio::pin!(stop);
    println!("testnode-ready");
    let _ = std::io::stdout().flush();

    loop {
        tokio::select! {
            biased;
            requested = &mut stop => {
                requested?;
                control.clone().shutdown().await;
                std::process::exit(0);
            }
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                tokio::spawn(serve(socket, control.clone()));
            }
        }
    }
}

/// Resolves once the process is asked to stop: SIGTERM on Unix (what a
/// container stop sends), Ctrl-C elsewhere. A shutdown that outlasts the
/// stop's grace period is killed mid-way; the next spill open falls back
/// cold.
async fn stop_requested() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        terminate.recv().await;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

/// Resolves each `host:port` entry via DNS; a seed that fails to resolve
/// is logged and skipped rather than failing startup.
async fn resolve_seeds(spec: &str) -> Vec<SocketAddr> {
    let mut seeds = Vec::new();
    for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match tokio::net::lookup_host(entry).await {
            Ok(mut addrs) => match addrs.next() {
                Some(addr) => seeds.push(addr),
                None => eprintln!("sundog-testnode: seed {entry:?} resolved to no addresses"),
            },
            Err(error) => eprintln!("sundog-testnode: seed {entry:?} failed to resolve: {error}"),
        }
    }
    seeds
}

enum Reply {
    Line(String),
    Quit,
    Crash,
}

/// What a process is, `SUNDOG_TESTNODE_ROLE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// A cluster member with open caches: the default.
    Node,
    /// A gossip-only [`Observer`] that is never a peer.
    Observer,
}

/// The process role, `SUNDOG_TESTNODE_ROLE`: `node` by default, or
/// `observer`. Any other value is an `Err` naming the allowed ones.
fn role_from_env(raw: Option<&str>) -> Result<Role, String> {
    match raw {
        None | Some("node") => Ok(Role::Node),
        Some("observer") => Ok(Role::Observer),
        Some(other) => Err(format!(
            "SUNDOG_TESTNODE_ROLE must be \"node\" or \"observer\", got {other:?}"
        )),
    }
}

/// What a control connection drives: a cluster member or an observer.
#[derive(Clone)]
enum Control {
    Node(TestNode),
    Observer(ObserverNode),
}

impl Control {
    async fn dispatch(&self, line: &str) -> Reply {
        match self {
            Self::Node(node) => node.dispatch(line).await,
            Self::Observer(observer) => observer.dispatch(line).await,
        }
    }

    /// Leaves the cluster's gossip.
    async fn shutdown(self) {
        match self {
            Self::Node(node) => node.cluster.shutdown().await,
            Self::Observer(observer) => observer.observer.shutdown().await,
        }
    }
}

/// The observer role's handle: no cache, no data plane.
#[derive(Clone)]
struct ObserverNode {
    observer: Observer,
}

impl ObserverNode {
    /// Parses and runs one observer control line, returning its reply.
    async fn dispatch(&self, line: &str) -> Reply {
        let mut parts = line.trim().splitn(2, ' ');
        let command = parts.next().unwrap_or_default();
        match command {
            // members -> live=<n> departing=<n> left=<n> down=<n> anon=<n>.
            "members" => Reply::Line(members_line(&self.observer.snapshot())),
            // share <cache> <owners> <node-id> -> the parts <node-id> owns in
            // <cache> at <owners> owners per part, or none.
            "share" => {
                let Some((cache, owners, node)) = parse_share_args(parts.next().unwrap_or(""))
                else {
                    return Reply::Line(
                        "err share needs a cache, a nonzero u8 owner count and a decimal node id"
                            .to_string(),
                    );
                };
                let snapshot = self.observer.snapshot();
                // About 65,536 hashes per eligible member: off the async workers.
                let reply = tokio::task::spawn_blocking(move || {
                    share_reply(&snapshot, &cache, owners, node)
                })
                .await
                .unwrap_or_else(|error| format!("err {error}"));
                Reply::Line(reply)
            }
            // quit -> exits 0, no reply written.
            "quit" => Reply::Quit,
            other => Reply::Line(format!("err unknown command {other:?}")),
        }
    }
}

/// The `members` reply: how many members `snapshot` shows in each status,
/// and how many live gossip participants have no node state:
/// `live=<n> departing=<n> left=<n> down=<n> anon=<n>`.
fn members_line(snapshot: &ClusterSnapshot) -> String {
    let count = |status| {
        snapshot
            .members
            .iter()
            .filter(|member| member.status == status)
            .count()
    };
    format!(
        "live={} departing={} left={} down={} anon={}",
        count(MemberStatus::Live),
        count(MemberStatus::Departing),
        count(MemberStatus::Left),
        count(MemberStatus::Down),
        snapshot.anonymous,
    )
}

/// Parses the arguments of `share`: `<cache> <owners> <node-id>`, the owner
/// count nonzero and the node id a decimal `u64`.
fn parse_share_args(args: &str) -> Option<(String, std::num::NonZeroU8, NodeId)> {
    let mut parts = args.split_whitespace();
    let cache = parts.next()?;
    let owners = std::num::NonZeroU8::new(parse_arg(parts.next())?)?;
    let node = NodeId::from(parse_arg::<u64>(parts.next())?);
    parts
        .next()
        .is_none()
        .then(|| (cache.to_string(), owners, node))
}

/// The `share` reply: how many of the 65,536 parts `node` owns in `cache` at
/// `owners` owners per part, or `none` when no member is eligible.
fn share_reply(
    snapshot: &ClusterSnapshot,
    cache: &str,
    owners: std::num::NonZeroU8,
    node: NodeId,
) -> String {
    snapshot.ownership(cache, owners).map_or_else(
        || "none".to_string(),
        |shares| shares.parts_owned_by(node).to_string(),
    )
}

/// Parses one control-line argument via `FromStr`; `None` if absent or unparsable.
fn parse_arg<T: std::str::FromStr>(raw: Option<&str>) -> Option<T> {
    raw?.parse().ok()
}

/// Renders `Ok(())` as `ok`, `Err(error)` as `err <error>`.
fn ok_reply(result: Result<(), impl std::fmt::Display>) -> String {
    match result {
        Ok(()) => "ok".to_string(),
        Err(error) => format!("err {error}"),
    }
}

/// One control connection's handles to every open cache and the cluster.
#[derive(Clone)]
struct TestNode {
    cache: Cache<String, String>,
    side: Option<SideCaches>,
    cluster: Cluster,
}

/// The replicated caches beside `"it"`, absent under
/// `SUNDOG_TESTNODE_SIDE_CACHES=off`.
#[derive(Clone)]
struct SideCaches {
    churn: Cache<String, String>,
    pn: Cache<String, PnCounter>,
    os: Cache<String, OrSet<String>>,
}

impl SideCaches {
    async fn open(cluster: &Cluster) -> Result<Self, Box<dyn std::error::Error>> {
        let churn = cluster
            .cache::<String, String>(CHURN_CACHE_NAME)
            .mode(Mode::Replicated)
            .ttl(CHURN_TTL)
            .open()
            .await?;
        let pn = cluster
            .cache::<String, PnCounter>(PN_CACHE_NAME)
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .open()
            .await?;
        let os = cluster
            .cache::<String, OrSet<String>>(OS_CACHE_NAME)
            .mode(Mode::Replicated)
            .resolver(Arc::new(OrSetResolver::<String>::new()))
            .open()
            .await?;
        Ok(Self { churn, pn, os })
    }

    /// Runs one side-cache command: `churn`, `ccount`, `pn*` or `os*`.
    async fn dispatch(&self, command: &str, parts: &mut std::str::SplitN<'_, char>) -> Reply {
        match command {
            // churn n -> ok | err <e>, running n ops (3:1 insert:remove) over CHURN_KEYSPACE keys.
            "churn" => {
                let Some(ops) = parse_arg::<u32>(parts.next()) else {
                    return Reply::Line("err churn needs a u32 op count".to_string());
                };
                for i in 0..ops {
                    let key = format!("c{}", i % CHURN_KEYSPACE);
                    let result = if i % 4 == 3 {
                        self.churn.remove(&key).await
                    } else {
                        self.churn.insert(key, format!("v{i}")).await
                    };
                    if let Err(error) = result {
                        return Reply::Line(format!("err {error}"));
                    }
                }
                Reply::Line("ok".to_string())
            }
            // ccount -> <n>, "churn"'s live-entry count.
            "ccount" => Reply::Line(self.churn.entry_count().await.to_string()),
            "pnfill" | "pncount" | "pnget" | "pnbytes" | "pndump" => {
                pn_command(&self.pn, self.pn.writer_id(), command, parts.next()).await
            }
            _ => os_command(&self.os, command, parts.next(), parts.next()).await,
        }
    }
}

impl TestNode {
    /// Parses and runs one control line, returning its reply.
    async fn dispatch(&self, line: &str) -> Reply {
        let mut parts = line.trim().splitn(3, ' ');
        let command = parts.next().unwrap_or_default();
        match command {
            // put k v -> ok | err <e>.
            "put" => {
                let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
                    return Reply::Line("err put needs a key and a value".to_string());
                };
                Reply::Line(ok_reply(
                    self.cache.insert(key.to_string(), value.to_string()).await,
                ))
            }
            // get k -> val <v> | none.
            "get" => {
                let Some(key) = parts.next() else {
                    return Reply::Line("err get needs a key".to_string());
                };
                Reply::Line(match self.cache.get(&key.to_string()).await {
                    Some(value) => format!("val {value}"),
                    None => "none".to_string(),
                })
            }
            // del k -> ok | err <e>.
            "del" => {
                let Some(key) = parts.next() else {
                    return Reply::Line("err del needs a key".to_string());
                };
                Reply::Line(ok_reply(self.cache.remove(&key.to_string()).await))
            }
            // count -> <n>, "it"'s live-entry count.
            "count" => Reply::Line(self.cache.entry_count().await.to_string()),
            // load k -> val <v> | none | err <e>, reading k through "it"'s loader.
            "load" => {
                let Some(key) = parts.next() else {
                    return Reply::Line("err load needs a key".to_string());
                };
                Reply::Line(match self.cache.load(&key.to_string()).await {
                    Ok(Some(value)) => format!("val {value}"),
                    Ok(None) => "none".to_string(),
                    Err(error) => format!("err {error}"),
                })
            }
            // loads -> <n>, how many times this node's "it" loader has run.
            "loads" => Reply::Line(LOADER_CALLS.load(Ordering::SeqCst).to_string()),
            // fill n -> ok | err <e>, bulk-inserting k0..kn = v0..vn.
            "fill" => {
                let Some(count) = parse_arg::<u32>(parts.next()) else {
                    return Reply::Line("err fill needs a u32 count".to_string());
                };
                let entries = (0..count).map(|i| (format!("k{i}"), format!("v{i}")));
                Reply::Line(ok_reply(self.cache.insert_many(entries).await))
            }
            // The replicated side caches' commands; see `SideCaches::dispatch`.
            "churn" | "ccount" | "pnfill" | "pncount" | "pnget" | "pnbytes" | "pndump"
            | "osadd" | "osremove" | "osmembers" => {
                let Some(side) = &self.side else {
                    return Reply::Line("err side caches are off".to_string());
                };
                side.dispatch(command, &mut parts).await
            }
            "bigfill" | "bigcheck" | "bigput" | "bigverify" => {
                big_command(&self.cache, command, &mut parts).await
            }
            // drop k -> ok, dropping k's local copy with no tombstone.
            "drop" => {
                let Some(key) = parts.next() else {
                    return Reply::Line("err drop needs a key".to_string());
                };
                self.cache.invalidate_local(&key.to_string()).await;
                Reply::Line("ok".to_string())
            }
            // explain k -> one line of JSON, why a read of k answers what it answers on this
            // node | err <e>.
            "explain" => explain_command(&self.cache, parts.next()).await,
            "fetch" | "owners" => ownership_command(&self.cache, command, parts.next()).await,
            // id -> this node's own NodeId as a decimal u64.
            "id" => Reply::Line(self.cluster.node_id().as_u64().to_string()),
            // netstats -> <frames> <bytes>, this process's total wire frames/bytes sent.
            "netstats" => Reply::Line(format!(
                "{} {}",
                sundog::net::frames_sent_total(),
                sundog::net::bytes_sent_total()
            )),
            // peers -> <n>.
            "peers" => Reply::Line(self.cluster.peers().len().to_string()),
            // digest -> <hex u64>, an order-independent digest of "it"'s live content.
            "digest" => Reply::Line(format!("{:016x}", digest_it(&self.cache).await)),
            // quit -> exits 0, no reply written.
            "quit" => Reply::Quit,
            // crash -> ok, then exits status 3 without leaving the cluster gracefully.
            "crash" => Reply::Crash,
            other => Reply::Line(format!("err unknown command {other:?}")),
        }
    }
}

/// The `big*` command family; each variant's contract is on its match arm.
async fn big_command(
    cache: &Cache<String, String>,
    command: &str,
    parts: &mut std::str::SplitN<'_, char>,
) -> Reply {
    let index = if matches!(command, "bigfill" | "bigcheck") {
        match parse_arg::<u32>(parts.next()) {
            Some(index) => index,
            None => return Reply::Line(format!("err {command} needs a u32 before the size")),
        }
    } else {
        BIG_ONE_INDEX
    };
    let Some(bytes) = parse_arg::<usize>(parts.next()) else {
        return Reply::Line(format!("err {command} needs a usize size"));
    };

    Reply::Line(match command {
        // bigfill n bytes -> ok | err <e>, bulk-inserting big0..bign.
        "bigfill" => {
            let entries = (0..index).map(|i| (format!("big{i}"), big_value(i, bytes)));
            ok_reply(cache.insert_many(entries).await)
        }
        // bigput bytes -> ok | err <e>, the point for over-cap sizes.
        "bigput" => ok_reply(
            cache
                .insert(BIG_ONE_KEY.to_string(), big_value(BIG_ONE_INDEX, bytes))
                .await,
        ),
        // bigcheck i bytes / bigverify bytes -> ok | bad | none, via verdict.
        _ => {
            let key = if command == "bigcheck" {
                format!("big{index}")
            } else {
                BIG_ONE_KEY.to_string()
            };
            match cache.get(&key).await {
                Some(value) => verdict(&value, index, bytes),
                None => "none".to_string(),
            }
        }
    })
}

/// The `pnfill n` entries: `pn0..pn(n-1)`, each a blind
/// [`PnCounter::local_delta`] of `1` from `writer`. Pure so the fill's
/// shape, key names, one writer, one increment each, is testable without
/// writing through a real cache.
fn pn_fill_entries(writer: WriterId, count: u32) -> Vec<(String, PnCounter)> {
    (0..count)
        .map(|i| (format!("pn{i}"), PnCounter::local_delta(writer, 1)))
        .collect()
}

/// Renders `"pn"`'s [`Cache::get`] result as the `pndump` reply body: the
/// counter's `Debug` form for a present key, `none` for an absent one.
/// Pure so the reply formatting is testable without a real cluster.
fn pn_dump_reply(counter: Option<&PnCounter>) -> String {
    counter.map_or_else(|| "none".to_string(), |counter| format!("{counter:?}"))
}

/// Renders `"pn"`'s [`Cache::get`] result as the `pnget` reply body:
/// `val <n>` for a present counter's [`PnCounter::value`], `none` for an
/// absent key. Pure so the reply formatting is testable without a real
/// cluster.
fn pn_get_reply(counter: Option<&PnCounter>) -> String {
    match counter {
        Some(counter) => format!("val {}", counter.value()),
        None => "none".to_string(),
    }
}

/// The summed encoded length of `counters`, the resident record size a
/// cold join or a state transfer carries for them. Pure so the sum is
/// testable without a real cache.
/// # Errors
///
/// Returns `Err` if any counter fails to encode.
fn pn_encoded_bytes<'a>(
    counters: impl IntoIterator<Item = &'a PnCounter>,
) -> Result<usize, String> {
    counters
        .into_iter()
        .map(|counter| counter.encode().map(|bytes| bytes.len()))
        .try_fold(0, |total, len| len.map(|len| total + len))
        .map_err(|error| error.to_string())
}

/// The `pn*` command family on `"pn"`; each variant's contract is on its match arm.
async fn pn_command(
    pn: &Cache<String, PnCounter>,
    writer: WriterId,
    command: &str,
    arg: Option<&str>,
) -> Reply {
    match command {
        // pnfill n -> ok | err <e>, bulk-incrementing pn0..pn(n-1) by one.
        "pnfill" => {
            let Some(count) = parse_arg::<u32>(arg) else {
                return Reply::Line("err pnfill needs a u32 count".to_string());
            };
            Reply::Line(ok_reply(
                pn.insert_many(pn_fill_entries(writer, count)).await,
            ))
        }
        // pncount -> <n>, "pn"'s live-entry count.
        "pncount" => Reply::Line(pn.entry_count().await.to_string()),
        // pnbytes n -> <b>, summed encoded length of pn0..pn(n-1).
        "pnbytes" => {
            let Some(count) = parse_arg::<u32>(arg) else {
                return Reply::Line("err pnbytes needs a u32 count".to_string());
            };
            let mut counters = Vec::with_capacity(count as usize);
            for i in 0..count {
                if let Some(counter) = pn.get(&format!("pn{i}")).await {
                    counters.push(counter);
                }
            }
            Reply::Line(match pn_encoded_bytes(&counters) {
                Ok(bytes) => bytes.to_string(),
                Err(error) => format!("err {error}"),
            })
        }
        // pndump k -> key k's resident counter in Debug form, or none.
        "pndump" => {
            let Some(key) = arg else {
                return Reply::Line("err pndump needs a key".to_string());
            };
            Reply::Line(pn_dump_reply(pn.get(&key.to_string()).await.as_ref()))
        }
        // pnget k -> val <n> | none, key k's current value.
        _ => {
            let Some(key) = arg else {
                return Reply::Line("err pnget needs a key".to_string());
            };
            Reply::Line(pn_get_reply(pn.get(&key.to_string()).await.as_ref()))
        }
    }
}

/// Parses `osadd`/`osremove`'s required key and element; `Err` names which
/// command needs both. Pure so the validation is testable without a real cache.
fn parse_key_and_element<'a>(
    command: &str,
    key: Option<&'a str>,
    elem: Option<&'a str>,
) -> Result<(&'a str, &'a str), String> {
    match (key, elem) {
        (Some(key), Some(elem)) => Ok((key, elem)),
        _ => Err(format!("err {command} needs a key and an element")),
    }
}

/// The `osadd k e` delta: `e` tagged with `writer`'s next sequence number
/// (`seq`), a blind [`OrSet::add`]. Pure so the delta's shape is testable
/// without writing through a real cache.
fn osadd_delta(writer: WriterId, seq: u64, elem: &str) -> OrSet<String> {
    OrSet::add(writer, seq, elem.to_string())
}

/// The `osremove k e` delta against `observed`, key `k`'s currently read
/// set: a blind [`OrSet::remove`]. Pure so the delta's shape is testable
/// without writing through a real cache.
fn osremove_delta(observed: &OrSet<String>, elem: &str) -> OrSet<String> {
    OrSet::remove(observed, &elem.to_string())
}

/// Renders `"os"`'s [`Cache::get`] result as the `osmembers` reply body:
/// every live element of a present set, alphabetically sorted and
/// space-separated so two replicas holding the same membership always
/// render identically regardless of each element's underlying tag order,
/// `none` for a key that has never been written. Pure so the reply
/// formatting is testable without a real cluster.
fn os_members_reply(set: Option<&OrSet<String>>) -> String {
    match set {
        Some(set) => {
            let mut members: Vec<&String> = set.iter().collect();
            members.sort();
            members
                .into_iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        }
        None => "none".to_string(),
    }
}

/// The `os*` command family on `"os"`; each variant's contract is on its
/// match arm.
async fn os_command(
    os: &Cache<String, OrSet<String>>,
    command: &str,
    key: Option<&str>,
    elem: Option<&str>,
) -> Reply {
    match command {
        // osadd k e -> ok | err <e>, merging a blind OrSet::add into key k.
        // Control connections run concurrently, so the cache draws the
        // sequence number under the lock that applies the add.
        "osadd" => {
            let (key, elem) = match parse_key_and_element(command, key, elem) {
                Ok(pair) => pair,
                Err(error) => return Reply::Line(error),
            };
            Reply::Line(ok_reply(
                os.merge_sequenced(key.to_string(), |writer, seq| {
                    osadd_delta(writer, seq, elem)
                })
                .await,
            ))
        }
        // osremove k e -> ok | err <e>, merging OrSet::remove against the observed set.
        "osremove" => {
            let (key, elem) = match parse_key_and_element(command, key, elem) {
                Ok(pair) => pair,
                Err(error) => return Reply::Line(error),
            };
            let Some(observed) = os.get(&key.to_string()).await else {
                return Reply::Line("ok".to_string());
            };
            let delta = osremove_delta(&observed, elem);
            Reply::Line(ok_reply(os.merge(key.to_string(), delta).await))
        }
        // osmembers k -> sorted space-separated live elements, or none.
        _ => {
            let Some(key) = key else {
                return Reply::Line("err osmembers needs a key".to_string());
            };
            Reply::Line(os_members_reply(os.get(&key.to_string()).await.as_ref()))
        }
    }
}

/// `fetch`/`owners`, the two routes reading `"it"`'s ownership state.
async fn ownership_command(
    cache: &Cache<String, String>,
    command: &str,
    key: Option<&str>,
) -> Reply {
    let Some(key) = key else {
        return Reply::Line(format!("err {command} needs a key"));
    };
    let key = key.to_string();
    Reply::Line(match command {
        // fetch k -> val <v> | none | err <e>.
        "fetch" => match cache.fetch(&key).await {
            Ok(Some(value)) => format!("val {value}"),
            Ok(None) => "none".to_string(),
            Err(error) => format!("err {error}"),
        },
        // owners k -> k's owning node ids, space-separated decimal u64s.
        _ => cache
            .owners_of(&key)
            .into_iter()
            .map(|id| id.as_u64().to_string())
            .collect::<Vec<_>>()
            .join(" "),
    })
}

/// `explain`, `Cache::explain` of a key of `"it"` as one line of JSON, or
/// `err <e>` when `Cache::explain` fails. It is not a read: it counts no
/// hit, miss or fetch and calls no loader.
async fn explain_command(cache: &Cache<String, String>, key: Option<&str>) -> Reply {
    let Some(key) = key else {
        return Reply::Line("err explain needs a key".to_string());
    };
    Reply::Line(match cache.explain(&key.to_string()).await {
        Ok(explanation) => explain_reply::to_line(&explain_reply::from_explanation(&explanation)),
        Err(error) => format!("err {error}"),
    })
}

async fn serve(socket: TcpStream, control: Control) {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    loop {
        let Ok(Some(line)) = lines.next_line().await else {
            return;
        };
        match control.dispatch(&line).await {
            Reply::Line(reply) => {
                if writer
                    .write_all(format!("{reply}\n").as_bytes())
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Reply::Quit => std::process::exit(0),
            Reply::Crash => {
                // Reply first so the harness's `crash` round trip completes,
                // then exit uncleanly: no cluster leave, the way a killed
                // process dies.
                let _ = writer.write_all(b"ok\n").await;
                let _ = writer.flush().await;
                std::process::exit(3);
            }
        }
    }
}

#[cfg(test)]
mod tests;
