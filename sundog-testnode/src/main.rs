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
//! `del k` -> `ok`; `count` -> `<n>`; `peers` -> `<n>`; `quit` -> exits 0.

use std::collections::HashSet;
use std::env;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sundog::{Cache, Cluster, ClusterConfig, Event, Mode};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const GOSSIP_PORT: u16 = 7946;
const CONTROL_PORT: u16 = 8080;
const CACHE_NAME: &str = "it";

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

    let known_keys = KnownKeys::default();
    tokio::spawn(track_keys(cache.events(), known_keys.clone()));

    let listener = TcpListener::bind(("0.0.0.0", CONTROL_PORT)).await?;
    println!("testnode-ready");
    let _ = std::io::stdout().flush();

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(serve(
            socket,
            cache.clone(),
            cluster.clone(),
            known_keys.clone(),
        ));
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

/// Mirrors the cache's live key set from its own event stream: `Cache` has no
/// public entry-count accessor (`docs/INTERFACES.md`), but the control
/// protocol's `count` command needs one that reflects replicated writes too.
#[derive(Clone, Default)]
struct KnownKeys(Arc<Mutex<HashSet<String>>>);

impl KnownKeys {
    fn count(&self) -> usize {
        self.0
            .lock()
            .expect("invariant: never held across an await point")
            .len()
    }
}

async fn track_keys(mut events: broadcast::Receiver<Event<String, String>>, known: KnownKeys) {
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return,
        };
        let mut keys = known
            .0
            .lock()
            .expect("invariant: never held across an await point");
        match event {
            Event::Created { key, .. } | Event::Updated { key, .. } => {
                keys.insert(key);
            }
            Event::Removed { key, .. } => {
                keys.remove(&key);
            }
        }
    }
}

enum Reply {
    Line(String),
    Quit,
}

async fn dispatch(
    cache: &Cache<String, String>,
    cluster: &Cluster,
    known: &KnownKeys,
    line: &str,
) -> Reply {
    let mut parts = line.trim().splitn(3, ' ');
    match parts.next().unwrap_or_default() {
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
        "count" => Reply::Line(known.count().to_string()),
        "peers" => Reply::Line(cluster.peers().len().to_string()),
        "quit" => Reply::Quit,
        other => Reply::Line(format!("err unknown command {other:?}")),
    }
}

async fn serve(
    socket: TcpStream,
    cache: Cache<String, String>,
    cluster: Cluster,
    known: KnownKeys,
) {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    loop {
        let Ok(Some(line)) = lines.next_line().await else {
            return;
        };
        match dispatch(&cache, &cluster, &known, &line).await {
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
