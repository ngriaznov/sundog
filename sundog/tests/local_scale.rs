//! A `Mode::Distributed` cluster of `SUNDOG_SCALE_NODES` local
//! `sundog-testnode` processes, each on its own loopback address
//! (`127.0.0.2` upward) with only the distributed `"it"` cache open, filled
//! with `SUNDOG_SCALE_KEYS` keys (1,000 by default). Skipped unless
//! `SUNDOG_SCALE_NODES` is set, so no CI lane runs it.
//!
//! Run with:
//! `SUNDOG_SCALE_NODES=100 cargo test --release -p sundog --test local_scale -- --nocapture`

#![cfg(target_os = "linux")]
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "report-only arithmetic on counts far inside f64's exact range"
)]

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt as _};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

const OWNERS: usize = 2;
const CLUSTER: &str = "local-scale";
const GOSSIP_PORT: u16 = 7946;
const CONTROL_PORT: u16 = 8080;
const METRICS_PORT: u16 = 9090;
const SEEDS: usize = 3;
/// Nodes that leave, and then join, mid-run.
const CHURN: usize = 5;
const SETTLE_WAIT: Duration = Duration::from_secs(600);
const IDLE_WINDOW: Duration = Duration::from_secs(30);
/// Every part assigned once per owner, across the cluster.
const OWNED_PARTS: u64 = 65_536 * OWNERS as u64;

/// One `sundog-testnode` process and the loopback address it binds.
struct Node {
    ip: Ipv4Addr,
    child: Child,
}

impl Node {
    /// Starts node `index` on `127.0.0.{index + 2}`, seeded on the first
    /// [`SEEDS`] nodes, and waits for its ready line.
    async fn spawn(bin: &Path, index: usize) -> Self {
        let ip = node_ip(index);
        let seeds: Vec<String> = (0..SEEDS.min(index))
            .map(|seed| format!("{}:{GOSSIP_PORT}", node_ip(seed)))
            .collect();
        let mut child = Command::new(bin)
            .arg(CLUSTER)
            .env("SUNDOG_TESTNODE_BIND_IP", ip.to_string())
            .env("SUNDOG_TESTNODE_MODE", "distributed")
            .env("SUNDOG_TESTNODE_OWNERS", OWNERS.to_string())
            .env("SUNDOG_TESTNODE_SIDE_CACHES", "off")
            .env("SUNDOG_SEEDS", seeds.join(","))
            .env("RUST_LOG", "warn")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("sundog-testnode starts");
        let stdout = child.stdout.take().expect("stdout is piped");
        let mut lines = BufReader::new(stdout).lines();
        tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(line) = lines.next_line().await.expect("stdout reads") {
                if line == "testnode-ready" {
                    return;
                }
            }
            panic!("{ip} exited before it was ready");
        })
        .await
        .unwrap_or_else(|_| panic!("{ip} is ready within a minute"));
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        Self { ip, child }
    }

    /// Sends one control line and returns its reply.
    async fn command(&self, line: &str) -> Result<String, String> {
        let mut stream = TcpStream::connect((self.ip, CONTROL_PORT))
            .await
            .map_err(|error| format!("{}: connect: {error}", self.ip))?;
        stream
            .write_all(format!("{line}\n").as_bytes())
            .await
            .map_err(|error| format!("{}: write: {error}", self.ip))?;
        let mut reply = String::new();
        BufReader::new(stream)
            .read_line(&mut reply)
            .await
            .map_err(|error| format!("{}: read: {error}", self.ip))?;
        Ok(reply.trim_end().to_string())
    }

    async fn number<T: std::str::FromStr>(&self, line: &str) -> Result<T, String> {
        let reply = self.command(line).await?;
        reply
            .parse()
            .map_err(|_| format!("{}: {line} replied {reply:?}", self.ip))
    }

    async fn peers(&self) -> Result<usize, String> {
        self.number("peers").await
    }

    async fn count(&self) -> Result<usize, String> {
        self.number("count").await
    }

    async fn id(&self) -> Result<u64, String> {
        self.number("id").await
    }

    /// Wire bytes this process has sent.
    async fn bytes_sent(&self) -> Result<u64, String> {
        let reply = self.command("netstats").await?;
        reply
            .split_once(' ')
            .and_then(|(_, bytes)| bytes.parse().ok())
            .ok_or_else(|| format!("{}: netstats replied {reply:?}", self.ip))
    }

    async fn owners(&self, key: &str) -> Result<Vec<u64>, String> {
        let reply = self.command(&format!("owners {key}")).await?;
        reply
            .split_whitespace()
            .map(|id| {
                id.parse()
                    .map_err(|_| format!("{}: owners replied {reply:?}", self.ip))
            })
            .collect()
    }

    async fn get(&self, key: &str) -> Result<Option<String>, String> {
        value_reply(self.command(&format!("get {key}")).await?)
    }

    async fn fetch(&self, key: &str) -> Result<Option<String>, String> {
        value_reply(self.command(&format!("fetch {key}")).await?)
    }

    /// This process's `sundog_owned_parts{cache="it"}` gauge.
    async fn owned_parts(&self) -> Result<u64, String> {
        let mut stream = TcpStream::connect((self.ip, METRICS_PORT))
            .await
            .map_err(|error| format!("{}: metrics connect: {error}", self.ip))?;
        stream
            .write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
            .await
            .map_err(|error| format!("{}: metrics write: {error}", self.ip))?;
        let mut body = String::new();
        stream
            .read_to_string(&mut body)
            .await
            .map_err(|error| format!("{}: metrics read: {error}", self.ip))?;
        body.lines()
            .find(|line| line.starts_with("sundog_owned_parts{") && line.contains("cache=\"it\""))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse::<f64>().ok())
            .map(|value| value as u64)
            .ok_or_else(|| format!("{}: no sundog_owned_parts gauge", self.ip))
    }

    /// Resident memory and user plus system CPU time, from `/proc`.
    fn usage(&self) -> Option<(u64, Duration)> {
        let pid = self.child.id()?;
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        let rss_kib: u64 = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()?;
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
        let ticks: u64 =
            fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
        Some((rss_kib * 1024, Duration::from_millis(ticks * 10)))
    }

    /// Exits at once without leaving the cluster.
    async fn crash(mut self) {
        let _ = self.command("crash").await;
        let _ = self.child.wait().await;
    }

    /// Leaves the cluster on SIGTERM and waits for the exit.
    async fn stop(mut self) {
        if let Some(pid) = self.child.id() {
            let _ = Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .await;
        }
        let _ = tokio::time::timeout(Duration::from_secs(30), self.child.wait()).await;
    }
}

/// `val v` as `Some(v)`, `none` as `None`.
fn value_reply(reply: String) -> Result<Option<String>, String> {
    if reply == "none" {
        return Ok(None);
    }
    reply
        .strip_prefix("val ")
        .map(|value| Some(value.to_string()))
        .ok_or(reply)
}

/// Node `index`'s loopback address: `127.0.0.2` upward.
fn node_ip(index: usize) -> Ipv4Addr {
    let offset = u32::try_from(index + 2).expect("the node count fits a loopback /8");
    Ipv4Addr::from(u32::from(Ipv4Addr::LOCALHOST) & 0xFF00_0000 | offset)
}

/// Builds the native `sundog-testnode` once, with `spill` and `prometheus`,
/// and returns its path.
fn testnode() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the crate sits in the workspace root")
            .to_path_buf();
        let status = std::process::Command::new(env!("CARGO"))
            .args([
                "build",
                "--release",
                "-p",
                "sundog-testnode",
                "--features",
                "spill,prometheus",
            ])
            .current_dir(&root)
            .status()
            .expect("cargo runs");
        assert!(status.success(), "sundog-testnode builds");
        root.join("target/release/sundog-testnode")
    })
}

async fn eventually<F, Fut>(timeout: Duration, what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !cond().await {
        assert!(Instant::now() < deadline, "{what} within {timeout:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Starts nodes `range`, eight at a time.
async fn spawn_nodes(bin: &Path, range: std::ops::Range<usize>) -> Vec<Node> {
    stream::iter(range)
        .map(|index| Node::spawn(bin, index))
        .buffered(8)
        .collect()
        .await
}

/// Waits until every one of `nodes` sees every other one as a peer.
async fn settle_peers(nodes: &[&Node]) {
    let expected = nodes.len() - 1;
    eventually(SETTLE_WAIT, "every node sees every other", || async {
        stream::iter(nodes)
            .map(|node| async move { node.peers().await == Ok(expected) })
            .buffer_unordered(32)
            .all(|seen| async move { seen })
            .await
    })
    .await;
}

/// Every one of `nodes`' owned-part gauge.
async fn part_shares(nodes: &[&Node]) -> Vec<u64> {
    stream::iter(nodes)
        .map(|node| async move { node.owned_parts().await.unwrap_or(0) })
        .buffered(32)
        .collect()
        .await
}

/// Whether `sample` is placed on exactly [`OWNERS`] nodes: `reader` names
/// that many owners, exactly they hold each key, and nobody else does.
async fn placed(reader: &Node, tagged: &[(&Node, u64)], sample: &[(String, String)]) -> bool {
    stream::iter(sample)
        .map(|(key, value)| async move {
            let Ok(owners) = reader.owners(key).await else {
                return false;
            };
            if owners.len() != OWNERS {
                return false;
            }
            for &(node, id) in tagged {
                let want = owners.contains(&id).then(|| value.clone());
                if node.get(key).await != Ok(want) {
                    return false;
                }
            }
            true
        })
        .buffer_unordered(8)
        .all(|ok| async move { ok })
        .await
}

/// Waits until `nodes` hold `keys` keys [`OWNERS`] times over, own every
/// part [`OWNERS`] times over, and place `sample` exactly.
async fn settle_placement(nodes: &[&Node], keys: usize, sample: &[(String, String)]) {
    let ids: Vec<u64> = stream::iter(nodes)
        .map(|node| async move { node.id().await.expect("id replies") })
        .buffered(32)
        .collect()
        .await;
    let tagged: Vec<(&Node, u64)> = nodes.iter().copied().zip(ids).collect();
    eventually(
        SETTLE_WAIT,
        "every key and part on exactly its owners",
        || async {
            let held: usize = stream::iter(nodes)
                .map(|node| async move { node.count().await.unwrap_or(0) })
                .buffered(32)
                .fold(0, |sum, count| async move { sum + count })
                .await;
            held == OWNERS * keys
                && part_shares(nodes).await.iter().sum::<u64>() == OWNED_PARTS
                && placed(tagged[0].0, &tagged, sample).await
        },
    )
    .await;
}

/// Prints the part-share spread against an even split and holds it within
/// a fifth of even.
async fn report_shares(when: &str, nodes: &[&Node]) {
    let shares = part_shares(nodes).await;
    let even = OWNED_PARTS as f64 / shares.len() as f64;
    let (min, max) = (
        *shares.iter().min().expect("nodes"),
        *shares.iter().max().expect("nodes"),
    );
    let (low, high) = (min as f64 / even, max as f64 / even);
    eprintln!("scale: {when}: parts per node {min}..{max}, even {even:.0} ({low:.2}..{high:.2})");
    assert!(
        low > 0.8 && high < 1.2,
        "{when}: shares within a fifth of even"
    );
}

/// Asserts every one of `nodes` fetches every entry of `entries`.
async fn assert_fetchable(nodes: &[&Node], entries: &[(String, String)]) {
    let pairs: Vec<(&Node, &(String, String))> = nodes
        .iter()
        .flat_map(|&node| entries.iter().map(move |entry| (node, entry)))
        .collect();
    let misses: Vec<String> = stream::iter(pairs)
        .map(|(node, (key, value))| async move {
            let got = node.fetch(key).await;
            (got != Ok(Some(value.clone()))).then(|| format!("{}/{key}: {got:?}", node.ip))
        })
        .buffer_unordered(64)
        .filter_map(|miss| async move { miss })
        .collect()
        .await;
    assert!(
        misses.is_empty(),
        "{} fetches missed: {:?}",
        misses.len(),
        &misses[..misses.len().min(10)]
    );
}

/// Mean wire bytes per second, CPU share and resident memory per node
/// across an idle `window`.
async fn report_idle(nodes: &[&Node]) {
    let sent = || async {
        stream::iter(nodes)
            .map(|node| async move { node.bytes_sent().await.unwrap_or(0) })
            .buffered(32)
            .fold(0u64, |sum, bytes| async move { sum + bytes })
            .await
    };
    let cpu = || {
        nodes
            .iter()
            .filter_map(|node| node.usage())
            .map(|(_, cpu)| cpu)
            .sum::<Duration>()
    };
    let (bytes_before, cpu_before) = (sent().await, cpu());
    tokio::time::sleep(IDLE_WINDOW).await;
    let (bytes_after, cpu_after) = (sent().await, cpu());
    let n = nodes.len() as f64;
    let window = IDLE_WINDOW.as_secs_f64();
    let rss: Vec<u64> = nodes
        .iter()
        .filter_map(|node| node.usage())
        .map(|(rss, _)| rss)
        .collect();
    let mib = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
    eprintln!(
        "scale: idle over {IDLE_WINDOW:?}: {:.0} B/s sent and {:.1}% of a core per node; RSS {:.1} MiB mean, {:.1} MiB max",
        (bytes_after - bytes_before) as f64 / n / window,
        cpu_after.saturating_sub(cpu_before).as_secs_f64() / n / window * 100.0,
        mib(rss.iter().sum::<u64>()) / rss.len() as f64,
        mib(*rss.iter().max().unwrap_or(&0)),
    );
}

/// The whole scenario: form, fill, idle, lose [`CHURN`] nodes one at a
/// time (two crashed), gain [`CHURN`] fresh ones at once, and check
/// placement and reads after each step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_node_cluster_owns_every_part_k_times_through_leaves_and_joins() {
    let Some(node_count) = std::env::var("SUNDOG_SCALE_NODES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
    else {
        eprintln!("skipping: SUNDOG_SCALE_NODES not set");
        return;
    };
    let keys: usize = std::env::var("SUNDOG_SCALE_KEYS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(1_000);
    assert!(
        node_count > CHURN + SEEDS,
        "more than {} nodes",
        CHURN + SEEDS
    );
    let bin = testnode();
    let entries: Vec<(String, String)> = (0..keys)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();
    let sample: Vec<(String, String)> = entries
        .iter()
        .step_by((keys / 40).max(1))
        .cloned()
        .collect();

    let started = Instant::now();
    let mut nodes = Vec::with_capacity(node_count + CHURN);
    for index in 0..SEEDS {
        nodes.push(Node::spawn(bin, index).await);
    }
    nodes.extend(spawn_nodes(bin, SEEDS..node_count).await);
    eprintln!(
        "scale: {node_count} processes up in {:?}",
        started.elapsed()
    );

    let phase = Instant::now();
    settle_peers(&nodes.iter().collect::<Vec<_>>()).await;
    eprintln!(
        "scale: every node sees {} peers after {:?}",
        node_count - 1,
        phase.elapsed()
    );

    let phase = Instant::now();
    assert_eq!(
        nodes[0].command(&format!("fill {keys}")).await,
        Ok("ok".to_string())
    );
    let all: Vec<&Node> = nodes.iter().collect();
    settle_placement(&all, keys, &sample).await;
    eprintln!(
        "scale: {keys} keys on exactly {OWNERS} owners after {:?}",
        phase.elapsed()
    );
    report_shares("formed", &all).await;
    assert_fetchable(&all[..all.len().min(10)], &entries).await;
    report_idle(&all).await;

    // Two owners survive one failure at a time, so nodes leave one by one,
    // each after the last one's rebalance settles: two crash, three stop.
    for leave in 0..CHURN {
        let phase = Instant::now();
        let node = nodes.pop().expect("more nodes than leave");
        let how = if leave < 2 {
            node.crash().await;
            "crashed"
        } else {
            node.stop().await;
            "stopped"
        };
        let survivors: Vec<&Node> = nodes.iter().collect();
        settle_peers(&survivors).await;
        settle_placement(&survivors, keys, &sample).await;
        eprintln!(
            "scale: a node {how}, {} survivors re-own every part after {:?}",
            survivors.len(),
            phase.elapsed()
        );
    }
    let survivors: Vec<&Node> = nodes.iter().collect();
    report_shares("after leaves", &survivors).await;
    assert_fetchable(&survivors[..survivors.len().min(10)], &entries).await;

    let phase = Instant::now();
    nodes.extend(spawn_nodes(bin, node_count..node_count + CHURN).await);
    let everyone: Vec<&Node> = nodes.iter().collect();
    settle_peers(&everyone).await;
    settle_placement(&everyone, keys, &sample).await;
    eprintln!(
        "scale: {CHURN} joined, {} nodes settled after {:?}",
        everyone.len(),
        phase.elapsed()
    );
    report_shares("after joins", &everyone).await;
    assert_fetchable(&everyone[everyone.len() - CHURN..], &entries).await;
    report_idle(&everyone).await;
    eprintln!("scale: whole run {:?}", started.elapsed());

    stream::iter(nodes)
        .for_each_concurrent(16, Node::stop)
        .await;
}
