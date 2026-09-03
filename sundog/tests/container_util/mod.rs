//! Dev-only harness for driving `sundog-testnode` inside real containers,
//! exclusively through the `rightsize` crate, never the docker CLI or
//! `bollard`.
//!
//! `RIGHTSIZE_BACKEND=docker` is required: sundog's gossip is UDP, and
//! rightsize's microsandbox network emulation relays TCP only. Every CI job
//! running `tests/container_*` sets it; see `.github/workflows`.
#![allow(dead_code)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rightsize::{Container, ContainerGuard, MountableFile, Network, Wait};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;

const CONTROL_PORT: u16 = 8080;
const READY_LOG: &str = "testnode-ready";

/// Gate for `tests/containers.rs`: `false` unless `SUNDOG_CONTAINER_TESTS=1`,
/// so a plain `cargo test --workspace` stays hermetic.
#[must_use]
pub fn container_tests_enabled() -> bool {
    std::env::var("SUNDOG_CONTAINER_TESTS").as_deref() == Ok("1")
}

/// Base image for test-node containers, `SUNDOG_TEST_BASE_IMAGE` overrides
/// it locally where registry pulls are blocked. Any image works as long as
/// it can run a static musl binary.
fn base_image() -> String {
    std::env::var("SUNDOG_TEST_BASE_IMAGE").unwrap_or_else(|_| "alpine:3.22".to_string())
}

/// Builds `sundog-testnode` for the musl target, once per test process, and
/// returns its release binary path. `chitchat` pulls `zstd-sys`, which needs
/// `CC_x86_64_unknown_linux_musl` to point at a musl-capable `cc`.
/// # Panics
///
/// Panics if the build command cannot be spawned or exits non-zero.
pub fn build_testnode() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let status = Command::new("cargo")
            .args([
                "build",
                "--release",
                "--target",
                "x86_64-unknown-linux-musl",
                "-p",
                "sundog-testnode",
            ])
            .env("CC_x86_64_unknown_linux_musl", "musl-gcc")
            .current_dir(workspace_root())
            .status()
            .expect("invariant: cargo is on PATH in every environment this harness runs in");
        assert!(status.success(), "sundog-testnode musl build failed");
        workspace_root().join("target/x86_64-unknown-linux-musl/release/sundog-testnode")
    })
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("invariant: sundog/ is a workspace member with a workspace-root parent")
        .to_path_buf()
}

/// One running test-node container plus a control-protocol client bound to
/// its mapped control port.
pub struct Node {
    guard: ContainerGuard,
    control_port: u16,
}

impl Node {
    /// Starts one `sundog-testnode` container named `alias` on `net`, in
    /// cluster `cluster_name`, seeded from `seeds`. Waits for the
    /// `testnode-ready` log line before returning.
    /// # Panics
    ///
    /// Panics if the container fails to start or never becomes ready.
    pub async fn spawn(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
    ) -> Node {
        Self::spawn_with_env(net, cluster_name, alias, seeds, &[]).await
    }

    /// [`Node::spawn`] with additional container environment variables, for
    /// `SUNDOG_TESTNODE_AE_PART_MIN_BUCKET`/`SUNDOG_TESTNODE_AE_SKETCH_MIN_BUCKET`
    /// overrides a test needs a node to start with.
    /// # Panics
    ///
    /// Panics if the container fails to start or never becomes ready.
    pub async fn spawn_with_env(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        extra_env: &[(&str, &str)],
    ) -> Node {
        rightsize_modules::register_default_backends();
        let bin = build_testnode();

        let mut container = Container::new(&base_image())
            .with_network(net)
            .with_network_aliases(&[alias])
            .with_exposed_ports(&[CONTROL_PORT])
            .with_copy_file_to_container(
                MountableFile::for_host_path(&bin.to_string_lossy()),
                "/sundog-testnode",
            )
            .with_env("SUNDOG_SEEDS", &seeds.join(","))
            .with_command(&["/sundog-testnode", cluster_name]);
        for &(key, value) in extra_env {
            container = container.with_env(key, value);
        }
        let guard = container
            .waiting_for(Wait::for_log_message(READY_LOG, 1))
            .start()
            .await
            .expect("test-node container starts and becomes ready");

        let control_port = guard
            .get_mapped_port(CONTROL_PORT)
            .expect("invariant: control port was declared via with_exposed_ports");
        Node {
            guard,
            control_port,
        }
    }

    /// This node's Docker network alias / rightsize-assigned name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.guard.name()
    }

    /// `put k v`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not `ok`.
    pub async fn put(&self, key: &str, value: &str) -> Result<(), String> {
        match self.command(&format!("put {key} {value}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(format!("unexpected reply to put: {other}")),
        }
    }

    /// `get k`, returning `Some(value)` on `val <v>` and `None` on `none`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply matches neither.
    pub async fn get(&self, key: &str) -> Result<Option<String>, String> {
        match self.command(&format!("get {key}")).await? {
            reply if reply == "none" => Ok(None),
            reply => reply
                .strip_prefix("val ")
                .map(str::to_string)
                .map(Some)
                .ok_or(reply),
        }
    }

    /// `del k`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not `ok`.
    pub async fn del(&self, key: &str) -> Result<(), String> {
        match self.command(&format!("del {key}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(format!("unexpected reply to del: {other}")),
        }
    }

    /// `count`, the node's live-entry count including replicated writes.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not numeric.
    pub async fn count(&self) -> Result<usize, String> {
        self.command("count")
            .await?
            .parse()
            .map_err(|error| format!("bad count reply: {error}"))
    }

    /// `fill n`, bulk-inserting `k0..kn` locally with no control round trip
    /// per entry.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the node reports an error.
    pub async fn fill(&self, count: u32) -> Result<(), String> {
        match self.command(&format!("fill {count}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(other.to_string()),
        }
    }

    /// `churn n`, running `n` back-to-back insert/remove operations (3:1
    /// mix) on the node's short-TTL `"churn"` cache over a fixed key space.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or an operation fails mid-run.
    pub async fn churn(&self, ops: u32) -> Result<(), String> {
        match self.command(&format!("churn {ops}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(other.to_string()),
        }
    }

    /// `ccount`, the live-entry count of the node's short-TTL `"churn"` cache.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not numeric.
    pub async fn churn_count(&self) -> Result<usize, String> {
        self.command("ccount")
            .await?
            .parse()
            .map_err(|error| format!("bad ccount reply: {error}"))
    }

    /// `bigfill n bytes`, bulk-inserting `big0..bign` with a deterministic
    /// `bytes`-sized value each.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the node reports an error.
    pub async fn big_fill(&self, count: u32, bytes: usize) -> Result<(), String> {
        match self
            .command(&format!("bigfill {count} {bytes}"))
            .await?
            .as_str()
        {
            "ok" => Ok(()),
            other => Err(other.to_string()),
        }
    }

    /// `bigcheck i bytes`: the node regenerates `bigi`'s expected value and
    /// byte-compares it, replying `ok`, `bad ...`, or `none`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails.
    pub async fn big_check(&self, index: u32, bytes: usize) -> Result<String, String> {
        self.command(&format!("bigcheck {index} {bytes}")).await
    }

    /// `bigput bytes`, inserting one `bytes`-sized value under the fixed
    /// large-value key, replying `ok` or `err ...`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails.
    pub async fn big_put(&self, bytes: usize) -> Result<String, String> {
        self.command(&format!("bigput {bytes}")).await
    }

    /// `bigverify bytes`, content-checking the fixed large-value key the way
    /// [`Node::big_check`] does for `bigi` keys.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails.
    pub async fn big_verify(&self, bytes: usize) -> Result<String, String> {
        self.command(&format!("bigverify {bytes}")).await
    }

    /// `drop k`, dropping `k`'s local copy with no tombstone and no fan-out,
    /// as if a `Replicate` for it never arrived.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not `ok`.
    pub async fn drop_key(&self, key: &str) -> Result<(), String> {
        match self.command(&format!("drop {key}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(format!("unexpected reply to drop: {other}")),
        }
    }

    /// `netstats`, this node's total wire frames and bytes sent since start.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply isn't `<frames>
    /// <bytes>`.
    pub async fn netstats(&self) -> Result<(u64, u64), String> {
        let reply = self.command("netstats").await?;
        let (frames, bytes) = reply
            .split_once(' ')
            .ok_or_else(|| format!("bad netstats reply: {reply:?}"))?;
        let frames = frames
            .parse()
            .map_err(|error| format!("bad netstats frames: {error}"))?;
        let bytes = bytes
            .parse()
            .map_err(|error| format!("bad netstats bytes: {error}"))?;
        Ok((frames, bytes))
    }

    /// `peers`, the node's live peer count as membership currently reports it.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not numeric.
    pub async fn peers(&self) -> Result<usize, String> {
        self.command("peers")
            .await?
            .parse()
            .map_err(|error| format!("bad peers reply: {error}"))
    }

    /// Sends one control-protocol line and returns its single-line reply,
    /// over a fresh connection per call.
    async fn command(&self, line: &str) -> Result<String, String> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.control_port))
            .await
            .map_err(|error| format!("connect to {}: {error}", self.control_port))?;
        stream
            .write_all(format!("{line}\n").as_bytes())
            .await
            .map_err(|error| format!("write: {error}"))?;
        let mut reply = String::new();
        BufReader::new(stream)
            .read_line(&mut reply)
            .await
            .map_err(|error| format!("read: {error}"))?;
        Ok(reply.trim_end().to_string())
    }

    /// Stops and removes the container, ahead of `ContainerGuard`'s own
    /// cleanup so a failure's output stays free of unrelated containers.
    /// # Errors
    ///
    /// Returns `Err` if the backend's stop or remove call fails.
    pub async fn stop(self) -> Result<(), String> {
        self.guard.stop().await.map_err(|error| error.to_string())
    }
}

/// Polls `cond` on a short fixed cadence until it returns `true`, or panics
/// once `timeout` elapses.
/// # Panics
///
/// Panics if `cond` has not returned `true` by `timeout`.
pub async fn eventually<F, Fut>(timeout: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
