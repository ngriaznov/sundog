//! Container test-node: embeds the sundog library behind a tiny line-based
//! control protocol, so the rightsize harness (`sundog/tests/container_util`)
//! can drive a real cluster member from outside its container. Built as a
//! static musl binary with no libc dependency.
//!
//! Usage: `sundog-testnode <cluster-name>`, with `SUNDOG_SEEDS` a
//! comma-separated list of `host:port` gossip seeds,
//! `SUNDOG_TESTNODE_AE_PART_MIN_BUCKET`/`SUNDOG_TESTNODE_AE_SKETCH_MIN_BUCKET`
//! optional `usize` overrides for `ClusterConfig::ae_part_min_bucket`/
//! `ae_sketch_min_bucket`, and `SUNDOG_TESTNODE_CRDT_RETIRE_AFTER_SECS` an
//! optional `u64` override for `ClusterConfig::crdt_retire_after` (absent
//! means each knob's built-in default). Opens one `Mode::Replicated` cache
//! named `"it"` and prints `testnode-ready` once the control listener is up.
//!
//! `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES`, an optional `u64`, bounds `"it"`
//! with a byte-counting weigher (`byte_weight`) instead of the default
//! unbounded entry count. Combined with `SUNDOG_TESTNODE_SPILL_DIR` (a
//! filesystem path) and `SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES` (a `u64` byte
//! budget), both required together, `"it"` opens with a `SpillConfig` tier
//! under that directory and capacity; `SUNDOG_TESTNODE_SPILL_REGION_BYTES`,
//! an optional `u64`, overrides its default region size, and
//! `SUNDOG_TESTNODE_SPILL_FLUSH_QUEUE_BYTES`, also optional, overrides the
//! flush-queue byte bound (default: one region's worth) independently of
//! `region_bytes`. These four spill variables only exist when this binary is
//! built with sundog's `spill` feature; setting
//! `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` alone, with no spill dir, opens
//! `Mode::Replicated` with a finite `max_capacity` and no spill tier, which
//! `open()` rejects — a test never does this. With none of these set, `"it"`
//! opens exactly as it always has: unbounded, no weigher, no spill.
//!
//! Built with the `prometheus` feature, every run also serves `GET /metrics`
//! (and `/readyz`, `/healthz`) on `METRICS_PORT` via
//! `sundog::ClusterBuilder::prometheus_listen`, exposing every
//! `sundog_spill_*`/`sundog_ae_repaired_total` counter and gauge the store
//! emits for `"it"`.
//!
//! Control protocol, one command per line, one line-terminated reply each:
//! `put k v` -> `ok`; `get k` -> `val <v>` | `none`; `del k` -> `ok`;
//! `count` -> `<n>`; `fill n` -> `ok`, bulk-inserting `k0..kn` = `v0..vn`;
//! `drop k` -> `ok`, dropping `k`'s local copy with no tombstone, standing
//! in for a lost `Replicate`; `netstats` -> `<frames> <bytes>`, this
//! process's total wire frames and bytes sent; `peers` -> `<n>`; `quit` ->
//! exits 0.
//!
//! A second `Mode::Replicated` cache named `"churn"` carries a short TTL
//! (`CHURN_TTL`): `churn n` -> `ok` runs `n` operations (3:1 insert:remove)
//! over `CHURN_KEYSPACE` keys; `ccount` -> `<n>` reads its live-entry count.
//!
//! Large-value commands work on `"it"` with deterministic content
//! ([`big_value`]), verified without crossing the control connection:
//! `bigfill n bytes` -> `ok`; `bigcheck i bytes` -> `ok` | `bad` | `none`;
//! `bigput bytes` -> `ok` | `err ...`, the point for over-cap sizes;
//! `bigverify bytes` -> `ok` | `bad` | `none`.
//!
//! `digest` -> `<hex u64>`: an order-independent digest of `"it"`'s live
//! content, computed as [`digest_it`] describes. `crash` -> `ok`, then the
//! process exits with status 3 without leaving the cluster gracefully, the
//! closest a control command can get to a process actually being killed.
//!
//! `SUNDOG_TESTNODE_MODE` selects `"it"`'s clustering mode: `"replicated"`
//! (the default, byte-for-byte the behavior above) or `"distributed"`,
//! which opens `"it"` as `Mode::Distributed` instead. `SUNDOG_TESTNODE_OWNERS`,
//! an optional `u8` at least 2, sets the owners-per-bucket count in
//! distributed mode; absent, distributed mode uses `Mode::distributed()`'s
//! default of two. Either variable set to anything else fails startup with
//! a clear message. `"churn"` always stays `Mode::Replicated` regardless of
//! this setting.
//!
//! Three more control routes, meaningful on any mode but only ever
//! interesting on a distributed `"it"`: `fetch k` -> `val <v>` | `none` |
//! `err <e>` ([`Cache::fetch`]); `owners k` -> `k`'s owning node ids, as
//! space-separated decimal `u64`s in rendezvous score order
//! ([`Cache::owners_of`]; on a non-distributed cache this is just this
//! node's own id); `id` -> this node's own [`sundog::NodeId`] as a decimal
//! `u64`.
//!
//! `SUNDOG_TESTNODE_RESOLVER` selects `"it"`'s `ConflictResolver`: absent or
//! `"lww"` keeps the default; `"sum_counter"` installs [`SumCounterResolver`],
//! which merges two decimal-string counters by addition on a genuine version
//! conflict instead of picking the most recent write. It exists to drive the
//! mixed-version container test's sentinel-stamped-merge scenario: a merge on
//! this node's `"it"` stamps its version with sundog's reserved merge-version
//! node id, and the container test installs this resolver on the current
//! release's node to confirm the previous release's node stores and serves
//! what it receives.
//!
//! A third `Mode::Replicated` cache named `"pn"` carries real
//! [`sundog::crdt::PnCounter`] values under [`sundog::crdt::PnCounterResolver`]
//! unconditionally, rather than gating it behind `SUNDOG_TESTNODE_RESOLVER`
//! the way `"it"` does: [`SumCounterResolver`]'s decimal-string wire shape
//! cannot stand in for it, since summing on every merge is not idempotent and
//! a real anti-entropy repair can redeliver the same merge more than once,
//! where `PnCounter::merge`'s pointwise-max join can absorb a redelivery for
//! free. `pnfill n` -> `ok`, bulk-incrementing `pn0..pn(n-1)` by one from this
//! node's [`Cache::writer_id`] — a blind
//! [`sundog::crdt::PnCounter::local_delta`] write, no read needed; `pncount`
//! -> `<n>`, `"pn"`'s live-entry count; `pnget k` -> `val <n>` | `none`, key
//! `k`'s current [`sundog::crdt::PnCounter::value`]. It drives the
//! million-counter cold-join container scenario: three nodes each increment
//! every one of a million counters once, concurrently, and a cold-joining
//! fourth node's state transfer must carry every counter's exact merged
//! total, not any one writer's last write.
//!
//! A fourth `Mode::Replicated` cache named `"os"` carries real
//! [`sundog::crdt::OrSet`]`<String>` values under
//! [`sundog::crdt::OrSetResolver`], one independent set per key. `osadd k e`
//! -> `ok`, adding element `e` to key `k`'s set via a blind
//! [`sundog::crdt::OrSet::add`] tagged with this node's own `writer_id` and
//! its own next per-process sequence number — no read needed. `osremove k e`
//! -> `ok`, reading key `k`'s currently observed set (a no-op if the key has
//! never been written on this node, since there is then nothing to observe)
//! and merging in an [`sundog::crdt::OrSet::remove`] delta against it.
//! `osmembers k` -> every live element of key `k`'s set, alphabetically
//! sorted and space-separated, or `none` if the key has never been written.

use std::env;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "spill")]
use sundog::SpillConfig;
use sundog::crdt::{OrSet, OrSetResolver, PnCounter, PnCounterResolver, WriterId};
use sundog::{Cache, Cluster, ClusterConfig, ConflictResolver, Merged, Mode, RecordView, Winner};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, TcpStream};
use xxhash_rust::xxh3::xxh3_64;

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
            // matching a digest taken after it was already gone.
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

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sundog-testnode: {error}");
        std::process::exit(1);
    }
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

/// Decides `"it"`'s [`Mode`] from `SUNDOG_TESTNODE_MODE`/`SUNDOG_TESTNODE_OWNERS`'s
/// already-read values: `mode` absent or `"replicated"` is always
/// `Mode::Replicated` (`owners` is only meaningful in distributed mode, so
/// it is ignored here rather than rejected); `"distributed"` is
/// `Mode::distributed()` when `owners` is absent, or `Mode::Distributed`
/// with that count when present and at least 2. Any other `mode` string, or
/// an `owners` under 2, is an `Err` naming the problem.
fn mode_from_env(mode: Option<&str>, owners: Option<u8>) -> Result<Mode, String> {
    match mode {
        None | Some("replicated") => Ok(Mode::Replicated),
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
            "SUNDOG_TESTNODE_MODE must be \"replicated\" or \"distributed\", got {other:?}"
        )),
    }
}

/// Weighs one entry by its UTF-8 byte length, key plus value: the
/// byte-counting weigher `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` installs on
/// `"it"` so its `max_capacity` bounds bytes rather than entry count.
/// Saturates at `u32::MAX` rather than panicking on a pathologically large
/// single entry.
fn byte_weight(key: &str, value: &str) -> u32 {
    (key.len() + value.len()).try_into().unwrap_or(u32::MAX)
}

/// Builds `"it"`'s optional spill tier from
/// `SUNDOG_TESTNODE_SPILL_DIR`/`SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES`/
/// `SUNDOG_TESTNODE_SPILL_REGION_BYTES`/
/// `SUNDOG_TESTNODE_SPILL_FLUSH_QUEUE_BYTES`'s already-parsed values: `None`
/// when no spill dir is set, so the cache opens spill-free exactly as it
/// always has.
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
) -> Option<SpillConfig> {
    let dir = dir?;
    let capacity_bytes = capacity_bytes.expect(
        "SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES must be set alongside SUNDOG_TESTNODE_SPILL_DIR",
    );
    let mut cfg = SpillConfig::new(dir, capacity_bytes);
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
/// `ConflictResolver::merge` outcome — and the sentinel-stamped `Hlc` sundog
/// stamps it with — reaches the wire. Falls back to plain `Hlc` order
/// whenever either side fails to parse as a `u64` (a tombstone, a spilled
/// view, or a non-numeric value), matching `sundog::crdt`'s own resolvers'
/// decode-failure fallback.
///
/// Summing on every merge is not idempotent (`merge(a, a) != a`), so this is
/// not a general-purpose CRDT resolver the way `sundog::crdt`'s are: it
/// exists only to drive one interop scenario in the container test suite,
/// which triggers exactly one merge on one key and never redelivers it.
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

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cluster_name = env::args()
        .nth(1)
        .ok_or("usage: sundog-testnode <cluster-name>")?;
    let seeds = resolve_seeds(&env::var("SUNDOG_SEEDS").unwrap_or_default()).await;
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

    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = SocketAddr::from(([0, 0, 0, 0], GOSSIP_PORT));
        // Faster than the default so container tests converge in seconds.
        c.ae_interval = Duration::from_secs(2);
        c.tombstone_ttl = Duration::from_secs(10);
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
        builder = builder.prometheus_listen(SocketAddr::from(([0, 0, 0, 0], METRICS_PORT)));
    }
    let cluster = builder.build().await?;

    let max_capacity_bytes = u64_env("SUNDOG_TESTNODE_MAX_CAPACITY_BYTES");
    let mut it_builder = cluster.cache::<String, String>(CACHE_NAME).mode(it_mode);
    if it_resolver == ResolverKind::SumCounter {
        it_builder = it_builder.resolver(Arc::new(SumCounterResolver));
    }
    if let Some(max_capacity_bytes) = max_capacity_bytes {
        it_builder = it_builder
            .max_capacity(max_capacity_bytes)
            .weigher(|key: &String, value: &String| byte_weight(key, value));
    }
    #[cfg(feature = "spill")]
    {
        let spill_cfg = spill_config_from_env(
            env::var("SUNDOG_TESTNODE_SPILL_DIR").ok(),
            u64_env("SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES"),
            u64_env("SUNDOG_TESTNODE_SPILL_REGION_BYTES"),
            u64_env("SUNDOG_TESTNODE_SPILL_FLUSH_QUEUE_BYTES"),
        );
        if let Some(spill_cfg) = spill_cfg {
            it_builder = it_builder.spill(spill_cfg);
        }
    }
    let cache = it_builder.open().await?;
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
    // This process's next `OrSet::add` sequence number: shared across every
    // control connection and every key, since it only has to be unique per
    // writer incarnation, not per key. Starts at zero every process start,
    // matching a fresh incarnation's fresh tag space.
    let os_seq = Arc::new(AtomicU64::new(0));

    let listener = TcpListener::bind(("0.0.0.0", CONTROL_PORT)).await?;
    println!("testnode-ready");
    let _ = std::io::stdout().flush();

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(serve(
            socket,
            cache.clone(),
            churn.clone(),
            pn.clone(),
            os.clone(),
            Arc::clone(&os_seq),
            cluster.clone(),
        ));
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

async fn dispatch(
    cache: &Cache<String, String>,
    churn: &Cache<String, String>,
    pn: &Cache<String, PnCounter>,
    os: &Cache<String, OrSet<String>>,
    os_seq: &AtomicU64,
    cluster: &Cluster,
    line: &str,
) -> Reply {
    let mut parts = line.trim().splitn(3, ' ');
    let command = parts.next().unwrap_or_default();
    match command {
        "put" => {
            let (Some(key), Some(value)) = (parts.next(), parts.next()) else {
                return Reply::Line("err put needs a key and a value".to_string());
            };
            Reply::Line(
                match cache.insert(key.to_string(), value.to_string()).await {
                    Ok(()) => "ok".to_string(),
                    Err(error) => format!("err {error}"),
                },
            )
        }
        "get" => {
            let Some(key) = parts.next() else {
                return Reply::Line("err get needs a key".to_string());
            };
            Reply::Line(match cache.get(&key.to_string()).await {
                Some(value) => format!("val {value}"),
                None => "none".to_string(),
            })
        }
        "del" => {
            let Some(key) = parts.next() else {
                return Reply::Line("err del needs a key".to_string());
            };
            Reply::Line(match cache.remove(&key.to_string()).await {
                Ok(()) => "ok".to_string(),
                Err(error) => format!("err {error}"),
            })
        }
        "count" => Reply::Line(cache.entry_count().await.to_string()),
        "fill" => {
            let Some(count) = parts.next().and_then(|raw| raw.parse::<u32>().ok()) else {
                return Reply::Line("err fill needs a u32 count".to_string());
            };
            let entries = (0..count).map(|i| (format!("k{i}"), format!("v{i}")));
            Reply::Line(match cache.insert_many(entries).await {
                Ok(()) => "ok".to_string(),
                Err(error) => format!("err {error}"),
            })
        }
        "churn" => {
            let Some(ops) = parts.next().and_then(|raw| raw.parse::<u32>().ok()) else {
                return Reply::Line("err churn needs a u32 op count".to_string());
            };
            for i in 0..ops {
                let key = format!("c{}", i % CHURN_KEYSPACE);
                let result = if i % 4 == 3 {
                    churn.remove(&key).await
                } else {
                    churn.insert(key, format!("v{i}")).await
                };
                if let Err(error) = result {
                    return Reply::Line(format!("err {error}"));
                }
            }
            Reply::Line("ok".to_string())
        }
        "ccount" => Reply::Line(churn.entry_count().await.to_string()),
        "bigfill" | "bigcheck" | "bigput" | "bigverify" => {
            big_command(cache, command, &mut parts).await
        }
        "pnfill" | "pncount" | "pnget" => {
            pn_command(pn, pn.writer_id(), command, parts.next()).await
        }
        "osadd" | "osremove" | "osmembers" => {
            os_command(
                os,
                os.writer_id(),
                os_seq,
                command,
                parts.next(),
                parts.next(),
            )
            .await
        }
        "drop" => {
            let Some(key) = parts.next() else {
                return Reply::Line("err drop needs a key".to_string());
            };
            cache.invalidate_local(&key.to_string()).await;
            Reply::Line("ok".to_string())
        }
        "fetch" | "owners" => ownership_command(cache, command, parts.next()).await,
        "id" => Reply::Line(cluster.node_id().as_u64().to_string()),
        "netstats" => Reply::Line(format!(
            "{} {}",
            sundog::net::frames_sent_total(),
            sundog::net::bytes_sent_total()
        )),
        "peers" => Reply::Line(cluster.peers().len().to_string()),
        "digest" => Reply::Line(format!("{:016x}", digest_it(cache).await)),
        "quit" => Reply::Quit,
        "crash" => Reply::Crash,
        other => Reply::Line(format!("err unknown command {other:?}")),
    }
}

/// The `big*` command family: every variant parses a trailing `usize` size,
/// `bigfill`/`bigcheck` an index or count before it.
async fn big_command(
    cache: &Cache<String, String>,
    command: &str,
    parts: &mut std::str::SplitN<'_, char>,
) -> Reply {
    let index = if matches!(command, "bigfill" | "bigcheck") {
        match parts.next().and_then(|raw| raw.parse::<u32>().ok()) {
            Some(index) => index,
            None => return Reply::Line(format!("err {command} needs a u32 before the size")),
        }
    } else {
        BIG_ONE_INDEX
    };
    let Some(bytes) = parts.next().and_then(|raw| raw.parse::<usize>().ok()) else {
        return Reply::Line(format!("err {command} needs a usize size"));
    };

    Reply::Line(match command {
        "bigfill" => {
            let entries = (0..index).map(|i| (format!("big{i}"), big_value(i, bytes)));
            match cache.insert_many(entries).await {
                Ok(()) => "ok".to_string(),
                Err(error) => format!("err {error}"),
            }
        }
        "bigput" => match cache
            .insert(BIG_ONE_KEY.to_string(), big_value(BIG_ONE_INDEX, bytes))
            .await
        {
            Ok(()) => "ok".to_string(),
            Err(error) => format!("err {error}"),
        },
        // `bigcheck`/`bigverify`'s fixed key: regenerate and byte-compare.
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
/// [`PnCounter::local_delta`] of `1` from `writer`. Pure so the fill's shape
/// — key names, one writer, one increment each — is testable without
/// writing through a real cache.
fn pn_fill_entries(writer: WriterId, count: u32) -> Vec<(String, PnCounter)> {
    (0..count)
        .map(|i| (format!("pn{i}"), PnCounter::local_delta(writer, 1)))
        .collect()
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

/// The `pn*` command family on `"pn"`: `pnfill n` bulk-increments
/// `pn0..pn(n-1)` by one from `writer` via [`pn_fill_entries`] (no read
/// needed); `pncount` reads `"pn"`'s live-entry count; `pnget k` reads key
/// `k`'s current value via [`pn_get_reply`].
async fn pn_command(
    pn: &Cache<String, PnCounter>,
    writer: WriterId,
    command: &str,
    arg: Option<&str>,
) -> Reply {
    match command {
        "pnfill" => {
            let Some(count) = arg.and_then(|raw| raw.parse::<u32>().ok()) else {
                return Reply::Line("err pnfill needs a u32 count".to_string());
            };
            Reply::Line(match pn.insert_many(pn_fill_entries(writer, count)).await {
                Ok(()) => "ok".to_string(),
                Err(error) => format!("err {error}"),
            })
        }
        "pncount" => Reply::Line(pn.entry_count().await.to_string()),
        // "pnget"
        _ => {
            let Some(key) = arg else {
                return Reply::Line("err pnget needs a key".to_string());
            };
            Reply::Line(pn_get_reply(pn.get(&key.to_string()).await.as_ref()))
        }
    }
}

/// Parses `osadd`/`osremove`'s two required arguments — a key and an
/// element — out of the line's already-split tokens: `Err` names which
/// command needs both, `Ok` the pair unwrapped. Pure so the validation is
/// testable without a real cache.
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

/// The `osremove k e` delta against `observed` — key `k`'s currently read
/// set — a blind [`OrSet::remove`]. Pure so the delta's shape is testable
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

/// The `os*` command family on `"os"`: `osadd k e` -> `ok`, merging
/// [`osadd_delta`] tagged with `writer`'s own next sequence number
/// (`next_seq`, shared across every key and connection, reset to zero at
/// process start since a fresh process gets a fresh incarnation and so a
/// fresh tag space) into key `k`; `osremove k e` -> `ok`, reading key `k`'s
/// currently observed set — a no-op, since there is then nothing to observe
/// or remove, if the key has never been written — and merging in
/// [`osremove_delta`] against it; `osmembers k` -> [`os_members_reply`] of
/// key `k`'s current set.
async fn os_command(
    os: &Cache<String, OrSet<String>>,
    writer: WriterId,
    next_seq: &AtomicU64,
    command: &str,
    key: Option<&str>,
    elem: Option<&str>,
) -> Reply {
    match command {
        "osadd" => {
            let (key, elem) = match parse_key_and_element(command, key, elem) {
                Ok(pair) => pair,
                Err(error) => return Reply::Line(error),
            };
            let seq = next_seq.fetch_add(1, Ordering::Relaxed);
            let delta = osadd_delta(writer, seq, elem);
            Reply::Line(match os.merge(key.to_string(), delta).await {
                Ok(()) => "ok".to_string(),
                Err(error) => format!("err {error}"),
            })
        }
        "osremove" => {
            let (key, elem) = match parse_key_and_element(command, key, elem) {
                Ok(pair) => pair,
                Err(error) => return Reply::Line(error),
            };
            let Some(observed) = os.get(&key.to_string()).await else {
                return Reply::Line("ok".to_string());
            };
            let delta = osremove_delta(&observed, elem);
            Reply::Line(match os.merge(key.to_string(), delta).await {
                Ok(()) => "ok".to_string(),
                Err(error) => format!("err {error}"),
            })
        }
        // "osmembers"
        _ => {
            let Some(key) = key else {
                return Reply::Line("err osmembers needs a key".to_string());
            };
            Reply::Line(os_members_reply(os.get(&key.to_string()).await.as_ref()))
        }
    }
}

/// `fetch`/`owners`, the two routes that read `"it"`'s ownership state
/// rather than its content: `fetch` answers via [`Cache::fetch`], `owners`
/// via [`Cache::owners_of`] rendered as space-separated decimal ids.
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
        "fetch" => match cache.fetch(&key).await {
            Ok(Some(value)) => format!("val {value}"),
            Ok(None) => "none".to_string(),
            Err(error) => format!("err {error}"),
        },
        _ => cache
            .owners_of(&key)
            .into_iter()
            .map(|id| id.as_u64().to_string())
            .collect::<Vec<_>>()
            .join(" "),
    })
}

async fn serve(
    socket: TcpStream,
    cache: Cache<String, String>,
    churn: Cache<String, String>,
    pn: Cache<String, PnCounter>,
    os: Cache<String, OrSet<String>>,
    os_seq: Arc<AtomicU64>,
    cluster: Cluster,
) {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    loop {
        let Ok(Some(line)) = lines.next_line().await else {
            return;
        };
        match dispatch(&cache, &churn, &pn, &os, &os_seq, &cluster, &line).await {
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
mod tests {
    use sundog::{Hlc, NodeId};

    use super::*;

    #[test]
    fn parse_usize_override_reads_a_valid_number() {
        assert_eq!(parse_usize_override(Some("512")), Some(512));
        assert_eq!(parse_usize_override(Some("0")), Some(0));
    }

    #[test]
    fn parse_usize_override_is_none_for_absent_or_unparsable_input() {
        assert_eq!(parse_usize_override(None), None);
        assert_eq!(parse_usize_override(Some("")), None);
        assert_eq!(parse_usize_override(Some("not-a-number")), None);
        assert_eq!(
            parse_usize_override(Some("-1")),
            None,
            "usize rejects a negative value"
        );
    }

    #[test]
    fn big_value_is_deterministic_and_regenerable() {
        let a = big_value(7, 20);
        let b = big_value(7, 20);
        assert_eq!(a, b);
        assert_eq!(a.len(), 20);
        assert!(a.starts_with("00000007"));
    }

    #[test]
    fn verdict_matches_only_the_exact_regenerated_value() {
        let value = big_value(3, 16);
        assert_eq!(verdict(&value, 3, 16), "ok");
        assert_ne!(verdict("wrong", 3, 16), "ok");
    }

    #[test]
    fn parse_u64_override_reads_a_valid_number() {
        assert_eq!(parse_u64_override(Some("1073741824")), Some(1_073_741_824));
        assert_eq!(parse_u64_override(Some("0")), Some(0));
    }

    #[test]
    fn parse_u64_override_is_none_for_absent_or_unparsable_input() {
        assert_eq!(parse_u64_override(None), None);
        assert_eq!(parse_u64_override(Some("")), None);
        assert_eq!(parse_u64_override(Some("not-a-number")), None);
        assert_eq!(
            parse_u64_override(Some("-1")),
            None,
            "u64 rejects a negative value"
        );
    }

    #[test]
    fn mode_from_env_defaults_to_replicated() {
        assert_eq!(mode_from_env(None, None), Ok(Mode::Replicated));
    }

    #[test]
    fn mode_from_env_replicated_is_explicit_too() {
        assert_eq!(
            mode_from_env(Some("replicated"), None),
            Ok(Mode::Replicated)
        );
    }

    #[test]
    fn mode_from_env_distributed_defaults_to_two_owners() {
        assert_eq!(
            mode_from_env(Some("distributed"), None),
            Ok(Mode::distributed())
        );
    }

    #[test]
    fn mode_from_env_distributed_takes_an_explicit_owners_count() {
        assert_eq!(
            mode_from_env(Some("distributed"), Some(4)),
            Ok(Mode::Distributed {
                owners: std::num::NonZeroU8::new(4).expect("4 is nonzero")
            })
        );
    }

    #[test]
    fn mode_from_env_rejects_a_single_owner() {
        let error = mode_from_env(Some("distributed"), Some(1))
            .expect_err("one owner is a lost bucket the instant it leaves");
        assert!(
            error.contains("at least 2"),
            "error should explain the minimum: {error:?}"
        );
    }

    #[test]
    fn mode_from_env_rejects_an_unknown_mode() {
        let error = mode_from_env(Some("gossiped"), None)
            .expect_err("an unrecognized mode string fails startup");
        assert!(
            error.contains("gossiped"),
            "error should name the bad value: {error:?}"
        );
    }

    #[test]
    fn byte_weight_sums_key_and_value_utf8_lengths() {
        assert_eq!(byte_weight("k0", "v0"), 4);
        assert_eq!(byte_weight("", ""), 0);
        assert_eq!(byte_weight("abc", "de"), 5);
    }

    #[test]
    fn resolver_kind_from_env_defaults_to_lww() {
        assert_eq!(resolver_kind_from_env(None), Ok(ResolverKind::Lww));
        assert_eq!(resolver_kind_from_env(Some("lww")), Ok(ResolverKind::Lww));
    }

    #[test]
    fn resolver_kind_from_env_reads_sum_counter() {
        assert_eq!(
            resolver_kind_from_env(Some("sum_counter")),
            Ok(ResolverKind::SumCounter)
        );
    }

    #[test]
    fn resolver_kind_from_env_rejects_an_unknown_resolver() {
        let error = resolver_kind_from_env(Some("crdt"))
            .expect_err("an unrecognized resolver string fails startup");
        assert!(
            error.contains("crdt"),
            "error should name the bad value: {error:?}"
        );
    }

    #[test]
    fn decode_counter_parses_a_postcard_encoded_decimal_string() {
        let bytes = postcard::to_stdvec(&"42".to_string()).expect("encodes");
        assert_eq!(decode_counter(&bytes), Some(42));
    }

    #[test]
    fn decode_counter_is_none_for_non_numeric_or_undecodable_bytes() {
        let non_numeric = postcard::to_stdvec(&"not-a-number".to_string()).expect("encodes");
        assert_eq!(decode_counter(&non_numeric), None);
        assert_eq!(
            decode_counter(b"\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
            None
        );
    }

    #[test]
    fn merge_counter_bytes_sums_two_decodable_counters() {
        let a = postcard::to_stdvec(&"3".to_string()).expect("encodes");
        let b = postcard::to_stdvec(&"5".to_string()).expect("encodes");
        let merged = merge_counter_bytes(&a, &b).expect("both sides decode");
        assert_eq!(
            postcard::from_bytes::<String>(&merged).expect("merged bytes decode"),
            "8"
        );
    }

    #[test]
    fn merge_counter_bytes_is_none_when_either_side_is_not_a_counter() {
        let counter = postcard::to_stdvec(&"3".to_string()).expect("encodes");
        let not_a_counter = postcard::to_stdvec(&"hello".to_string()).expect("encodes");
        assert_eq!(merge_counter_bytes(&counter, &not_a_counter), None);
        assert_eq!(merge_counter_bytes(&not_a_counter, &counter), None);
    }

    /// Postcard-encodes `a_value`/`b_value` as the decimal-string counters
    /// [`SumCounterResolver::winner`]'s tests below build `RecordView`s from.
    fn counter_bytes(a_value: &str, b_value: &str) -> (Vec<u8>, Vec<u8>) {
        (
            postcard::to_stdvec(&a_value.to_string()).expect("encodes"),
            postcard::to_stdvec(&b_value.to_string()).expect("encodes"),
        )
    }

    fn hlc_at(wall_ms: u64) -> Hlc {
        Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::random(),
        }
    }

    #[test]
    fn sum_counter_resolver_merges_two_decodable_counters() {
        let (a_bytes, b_bytes) = counter_bytes("3", "5");
        let a = RecordView {
            value: Some(&a_bytes),
            ver: hlc_at(1),
            expires_at_ms: None,
        };
        let b = RecordView {
            value: Some(&b_bytes),
            ver: hlc_at(2),
            expires_at_ms: None,
        };
        match SumCounterResolver.merge(b"key", a, b) {
            Some(Merged {
                value,
                expires_at_ms,
            }) => {
                assert_eq!(
                    postcard::from_bytes::<String>(&value).expect("merged bytes decode"),
                    "8"
                );
                assert_eq!(expires_at_ms, None);
            }
            other => panic!("expected Some(Merged), got {other:?}"),
        }
    }

    #[test]
    fn merges_is_true() {
        assert!(
            SumCounterResolver.merges(),
            "SumCounterResolver's merge can return Some, so it must advertise merges()"
        );
    }

    #[test]
    fn sum_counter_resolver_falls_back_to_hlc_order_on_a_tombstone() {
        let (_, b_bytes) = counter_bytes("3", "5");
        let a = RecordView {
            value: None,
            ver: hlc_at(1),
            expires_at_ms: None,
        };
        let b = RecordView {
            value: Some(&b_bytes),
            ver: hlc_at(2),
            expires_at_ms: None,
        };
        assert_eq!(SumCounterResolver.winner(b"key", a, b), Winner::B);
        assert_eq!(SumCounterResolver.winner(b"key", b, a), Winner::A);
    }

    #[test]
    fn sum_counter_resolver_falls_back_to_hlc_order_on_a_non_numeric_value() {
        let a_bytes = postcard::to_stdvec(&"not-a-number".to_string()).expect("encodes");
        let (_, b_bytes) = counter_bytes("3", "5");
        let a = RecordView {
            value: Some(&a_bytes),
            ver: hlc_at(1),
            expires_at_ms: None,
        };
        let b = RecordView {
            value: Some(&b_bytes),
            ver: hlc_at(2),
            expires_at_ms: None,
        };
        assert_eq!(SumCounterResolver.winner(b"key", a, b), Winner::B);
    }

    fn writer(node: u64, incarnation: u64) -> WriterId {
        WriterId::new(NodeId::from(node), incarnation)
    }

    #[test]
    fn pn_fill_entries_builds_one_local_delta_per_index() {
        let writer = writer(1, 100);
        let entries = pn_fill_entries(writer, 3);
        assert_eq!(entries.len(), 3);
        for (i, (key, counter)) in entries.iter().enumerate() {
            assert_eq!(*key, format!("pn{i}"));
            assert_eq!(counter.value(), 1);
        }
    }

    #[test]
    fn pn_fill_entries_is_empty_for_a_zero_count() {
        assert!(pn_fill_entries(writer(1, 100), 0).is_empty());
    }

    #[test]
    fn pn_get_reply_reports_the_counters_value() {
        let counter = PnCounter::local_delta(writer(1, 100), 5);
        assert_eq!(pn_get_reply(Some(&counter)), "val 5");
    }

    #[test]
    fn pn_get_reply_is_none_for_an_absent_key() {
        assert_eq!(pn_get_reply(None), "none");
    }

    #[test]
    fn parse_key_and_element_accepts_both_arguments() {
        assert_eq!(
            parse_key_and_element("osadd", Some("k"), Some("e")),
            Ok(("k", "e"))
        );
    }

    #[test]
    fn parse_key_and_element_rejects_a_missing_key_or_element() {
        let missing_both =
            parse_key_and_element("osadd", None, None).expect_err("neither argument is present");
        assert!(missing_both.contains("osadd"));
        assert!(missing_both.contains("key"));
        assert!(missing_both.contains("element"));
        assert!(parse_key_and_element("osremove", Some("k"), None).is_err());
        assert!(parse_key_and_element("osremove", None, Some("e")).is_err());
    }

    #[test]
    fn osadd_delta_tags_the_element_with_the_writer_and_seq() {
        let set = osadd_delta(writer(1, 100), 7, "hello");
        assert!(set.contains(&"hello".to_string()));
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![&"hello".to_string()]);
    }

    #[test]
    fn osremove_delta_removes_only_the_named_element() {
        let a = osadd_delta(writer(1, 100), 0, "keep");
        let b = osadd_delta(writer(1, 100), 1, "drop");
        let observed = a.merge(&b);
        assert!(observed.contains(&"keep".to_string()));
        assert!(observed.contains(&"drop".to_string()));

        let delta = osremove_delta(&observed, "drop");
        let after = observed.merge(&delta);
        assert!(after.contains(&"keep".to_string()));
        assert!(
            !after.contains(&"drop".to_string()),
            "osremove_delta must remove exactly the named element"
        );
    }

    #[test]
    fn os_members_reply_sorts_and_space_joins_live_members() {
        let a = osadd_delta(writer(1, 100), 0, "banana");
        let b = osadd_delta(writer(2, 100), 0, "apple");
        let set = a.merge(&b);
        assert_eq!(os_members_reply(Some(&set)), "apple banana");
    }

    #[test]
    fn os_members_reply_is_empty_string_for_a_present_but_fully_removed_set() {
        let a = osadd_delta(writer(1, 100), 0, "only");
        let removed = a.merge(&osremove_delta(&a, "only"));
        assert_eq!(os_members_reply(Some(&removed)), "");
    }

    #[test]
    fn os_members_reply_is_none_for_an_absent_key() {
        assert_eq!(os_members_reply(None), "none");
    }

    #[cfg(feature = "spill")]
    mod spill_env {
        use super::*;

        #[test]
        fn spill_config_from_env_is_none_without_a_dir() {
            assert!(spill_config_from_env(None, None, None, None).is_none());
            assert!(
                spill_config_from_env(None, Some(1 << 20), None, None).is_none(),
                "a capacity with no dir still opens spill-free"
            );
        }

        #[test]
        fn spill_config_from_env_builds_from_a_dir_and_capacity() {
            let cfg =
                spill_config_from_env(Some("/tmp/spill-it".to_string()), Some(4096), None, None)
                    .expect("dir plus capacity builds a config");
            assert_eq!(cfg.dir, std::path::PathBuf::from("/tmp/spill-it"));
            assert_eq!(cfg.capacity_bytes, 4096);
        }

        #[test]
        fn spill_config_from_env_applies_the_region_override() {
            let default_region =
                spill_config_from_env(Some("/tmp/spill-it".to_string()), Some(4096), None, None)
                    .expect("builds")
                    .region_bytes_value();
            let cfg = spill_config_from_env(
                Some("/tmp/spill-it".to_string()),
                Some(4096),
                Some(512),
                None,
            )
            .expect("builds");
            assert_eq!(cfg.region_bytes_value(), 512);
            assert_ne!(
                cfg.region_bytes_value(),
                default_region,
                "the override actually changes the region size from the default"
            );
        }

        #[test]
        fn spill_config_from_env_applies_the_flush_queue_bytes_override() {
            let default_flush_queue =
                spill_config_from_env(Some("/tmp/spill-it".to_string()), Some(4096), None, None)
                    .expect("builds")
                    .flush_queue_bytes_value();
            let cfg = spill_config_from_env(
                Some("/tmp/spill-it".to_string()),
                Some(4096),
                None,
                Some(1024),
            )
            .expect("builds");
            assert_eq!(cfg.flush_queue_bytes_value(), 1024);
            assert_ne!(
                cfg.flush_queue_bytes_value(),
                default_flush_queue,
                "the override actually changes the flush-queue bound from the default"
            );
        }

        #[test]
        #[should_panic(expected = "SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES")]
        fn spill_config_from_env_panics_when_the_dir_is_set_without_a_capacity() {
            let _ = spill_config_from_env(Some("/tmp/spill-it".to_string()), None, None, None);
        }
    }
}
