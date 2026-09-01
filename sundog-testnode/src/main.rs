//! Container test-node: embeds the sundog library behind a tiny line-based
//! control protocol, so the rightsize harness (`sundog/tests/container_util`)
//! can drive a real cluster member from outside its container. Built as a
//! static musl binary — no libc dependency inside the guest image.
//!
//! Usage: `sundog-testnode <cluster-name>`, with `SUNDOG_SEEDS` a
//! comma-separated list of `host:port` gossip seeds (Docker network aliases
//! resolve here via ordinary DNS). Opens one `Mode::Replicated` cache named
//! `"it"` and prints `testnode-ready` once the control listener is up — the
//! harness's `Wait::for_log_message` target.
//!
//! Control protocol, one command per line, one line-terminated reply each
//! (`quit` excepted): `put k v` -> `ok`; `get k` -> `val <v>` | `none`;
//! `del k` -> `ok`; `count` -> `<n>` (live local entries, read from the
//! store); `fill n` -> `ok` (bulk-inserts `k0..kn` = `v0..vn` locally);
//! `peers` -> `<n>`; `quit` -> exits 0.
//!
//! A second `Mode::Replicated` cache named `"churn"` carries a short TTL
//! (`CHURN_TTL`) for high-frequency lifecycle tests: `churn n` -> `ok` runs
//! `n` back-to-back operations over a fixed `CHURN_KEYSPACE`-key space —
//! three inserts to every remove, full speed, no pacing — and `ccount` ->
//! `<n>` reads that cache's live-entry count.
//!
//! Large-value commands work on the `"it"` cache with deterministic content
//! ([`big_value`]), so a value is generated on the writing node and verified
//! on a reading node without ever crossing the control connection:
//! `bigfill n bytes` -> `ok` bulk-inserts `big0..bign` with `bytes`-sized
//! values; `bigcheck i bytes` -> `ok` | `bad` | `none` regenerates and
//! compares `bigi`'s value; `bigput bytes` -> `ok` | `err …` inserts one
//! `bytes`-sized value under a fixed key (the `err` reply is the point for
//! over-frame-cap sizes); `bigverify bytes` -> `ok` | `bad` | `none` checks
//! that fixed key.

use std::env;
use std::io::Write as _;
use std::net::SocketAddr;
use std::time::Duration;

use sundog::{Cache, Cluster, ClusterConfig, Mode};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, TcpStream};

const GOSSIP_PORT: u16 = 7946;
const CONTROL_PORT: u16 = 8080;
const CACHE_NAME: &str = "it";
const CHURN_CACHE_NAME: &str = "churn";
/// Short enough that entries written early in a `churn` run expire while the
/// run is still writing — replication of already-expired records is part of
/// what the churn suite exists to exercise.
const CHURN_TTL: Duration = Duration::from_secs(2);
/// `churn` wraps keys modulo this, so concurrent churners on different nodes
/// keep colliding on the same keys instead of writing disjoint ranges.
const CHURN_KEYSPACE: u32 = 512;
/// The fixed key `bigput`/`bigverify` operate on, with [`BIG_ONE_INDEX`] as
/// its content seed.
const BIG_ONE_KEY: &str = "bigone";
const BIG_ONE_INDEX: u32 = u32::MAX;

/// Deterministic large-value content: `index`'s hex digits cycled out to
/// `len` bytes. Any node can regenerate and byte-compare a value locally, so
/// content-integrity checks never ship the value itself over the control
/// connection.
fn big_value(index: u32, len: usize) -> String {
    format!("{index:08x}").chars().cycle().take(len).collect()
}

/// `ok` if `stored` matches [`big_value`]`(index, len)` exactly, `bad` (with
/// a length hint) otherwise.
fn verdict(stored: &str, index: u32, len: usize) -> String {
    if stored == big_value(index, len) {
        "ok".to_string()
    } else {
        format!("bad len={} want={len}", stored.len())
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sundog-testnode: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cluster_name = env::args()
        .nth(1)
        .ok_or("usage: sundog-testnode <cluster-name>")?;
    let seeds = resolve_seeds(&env::var("SUNDOG_SEEDS").unwrap_or_default()).await;

    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = SocketAddr::from(([0, 0, 0, 0], GOSSIP_PORT));
        // Faster than the library default so container tests converge in
        // seconds, not the production 30s/10min cadence.
        c.ae_interval = Duration::from_secs(2);
        c.tombstone_ttl = Duration::from_secs(10);
    });

    let cluster = Cluster::builder(cluster_name)
        .seeds(seeds)
        .config(config)
        .build()
        .await?;
    let cache = cluster
        .cache::<String, String>(CACHE_NAME)
        .mode(Mode::Replicated)
        .open()
        .await?;
    let churn = cluster
        .cache::<String, String>(CHURN_CACHE_NAME)
        .mode(Mode::Replicated)
        .ttl(CHURN_TTL)
        .open()
        .await?;

    let listener = TcpListener::bind(("0.0.0.0", CONTROL_PORT)).await?;
    println!("testnode-ready");
    let _ = std::io::stdout().flush();

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(serve(socket, cache.clone(), churn.clone(), cluster.clone()));
    }
}

/// Resolves each `host:port` entry via DNS (Docker network aliases included);
/// a seed that fails to resolve is logged and skipped rather than failing
/// startup — a lone first node with no reachable seeds is a healthy
/// single-node cluster, exactly as `Cluster::builder` treats it.
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
}

async fn dispatch(
    cache: &Cache<String, String>,
    churn: &Cache<String, String>,
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
        "peers" => Reply::Line(cluster.peers().len().to_string()),
        "quit" => Reply::Quit,
        other => Reply::Line(format!("err unknown command {other:?}")),
    }
}

/// The `big*` command family (see the module docs): every variant parses a
/// trailing `usize` size, `bigfill`/`bigcheck` an index or count before it.
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
        // `bigcheck big{index}` or `bigverify`'s fixed key: regenerate and
        // byte-compare locally.
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

async fn serve(
    socket: TcpStream,
    cache: Cache<String, String>,
    churn: Cache<String, String>,
    cluster: Cluster,
) {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    loop {
        let Ok(Some(line)) = lines.next_line().await else {
            return;
        };
        match dispatch(&cache, &churn, &cluster, &line).await {
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
        }
    }
}
