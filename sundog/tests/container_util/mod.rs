//! Dev-only harness for driving `sundog-testnode` inside real containers,
//! exclusively through the `rightsize` crate — no docker CLI, no `bollard`.
//!
//! `RIGHTSIZE_BACKEND=docker` is required for the multi-node suite: sundog's
//! gossip is UDP, and rightsize's microsandbox network emulation relays TCP
//! only (msb's `install_network_links`). Not enforced here — rightsize
//! itself resolves `RIGHTSIZE_BACKEND` from the real environment — but every
//! CI job that runs `tests/container_*` must set it; see `.github/workflows`.
//!
//! Each `tests/*.rs` binary that needs containers writes `mod container_util;`
//! and pulls in only the helpers it needs — unused ones are expected, hence
//! the blanket `dead_code` allow.
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
/// so a plain `cargo test --workspace` run stays hermetic (no docker daemon,
/// no musl toolchain required) while still compiling and "passing" the
/// gated tests trivially.
#[must_use]
pub fn container_tests_enabled() -> bool {
    std::env::var("SUNDOG_CONTAINER_TESTS").as_deref() == Ok("1")
}

/// Base image for test-node containers. Defaults to a normal, registry-pulled
/// image; overridden locally (`SUNDOG_TEST_BASE_IMAGE=rz-base:local`) where
/// registry pulls are blocked and a minimal placeholder image is pre-seeded
/// in the local daemon instead. Any image works as long as it can run a
/// static musl binary — `sundog-testnode` needs no libc, no shell.
fn base_image() -> String {
    std::env::var("SUNDOG_TEST_BASE_IMAGE").unwrap_or_else(|_| "alpine:3.22".to_string())
}

/// Builds `sundog-testnode` for the musl target, once per test process, and
/// returns its release binary path. `chitchat` pulls `zstd-sys`, which needs
/// a C compiler for the musl target — `CC_x86_64_unknown_linux_musl` must
/// point at a musl-capable `cc` (`musl-gcc`; CI installs `musl-tools`).
///
/// # Panics
///
/// Panics if the build command cannot be spawned or exits non-zero — a build
/// failure here means every container test in the binary is unusable, so
/// there is no useful degraded path to fall back to.
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
    /// cluster `cluster_name`, seeded from `seeds` (peer aliases, e.g.
    /// `["n1:7946"]`). Waits for the `testnode-ready` log line before
    /// returning, so every `Node` this returns is immediately usable.
    ///
    /// # Panics
    ///
    /// Panics if the container fails to start or never becomes ready — see
    /// [`build_testnode`] for why there's no useful fallback here either.
    pub async fn spawn(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
    ) -> Node {
        rightsize_modules::register_default_backends();
        let bin = build_testnode();

        let guard = Container::new(&base_image())
            .with_network(net)
            .with_network_aliases(&[alias])
            .with_exposed_ports(&[CONTROL_PORT])
            .with_copy_file_to_container(
                MountableFile::for_host_path(&bin.to_string_lossy()),
                "/sundog-testnode",
            )
            .with_env("SUNDOG_SEEDS", &seeds.join(","))
            .with_command(&["/sundog-testnode", cluster_name])
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

    /// `put k v` — `Ok(())` iff the node replied `ok`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the control connection fails, or the node's reply
    /// isn't `ok`.
    pub async fn put(&self, key: &str, value: &str) -> Result<(), String> {
        match self.command(&format!("put {key} {value}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(format!("unexpected reply to put: {other}")),
        }
    }

    /// `get k` — `Some(value)` on `val <v>`, `None` on `none`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the control connection fails, or the node's reply
    /// matches neither `val <v>` nor `none`.
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

    /// `del k` — `Ok(())` iff the node replied `ok`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the control connection fails, or the node's reply
    /// isn't `ok`.
    pub async fn del(&self, key: &str) -> Result<(), String> {
        match self.command(&format!("del {key}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(format!("unexpected reply to del: {other}")),
        }
    }

    /// `count` — the node's live-entry count (replicated writes included).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the control connection fails, or the reply doesn't
    /// parse as a number.
    pub async fn count(&self) -> Result<usize, String> {
        self.command("count")
            .await?
            .parse()
            .map_err(|error| format!("bad count reply: {error}"))
    }

    /// `fill n` — bulk-inserts `k0..kn` locally on the node, without paying a
    /// control round trip per entry.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the control connection fails or the node reports an
    /// insert error.
    pub async fn fill(&self, count: u32) -> Result<(), String> {
        match self.command(&format!("fill {count}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(other.to_string()),
        }
    }

    /// `peers` — the node's live peer count, as membership currently reports it.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the control connection fails, or the reply doesn't
    /// parse as a number.
    pub async fn peers(&self) -> Result<usize, String> {
        self.command("peers")
            .await?
            .parse()
            .map_err(|error| format!("bad peers reply: {error}"))
    }

    /// Sends one control-protocol line and returns its single-line reply.
    /// Opens a fresh connection per call — the protocol is stateless and this
    /// keeps the client trivial; a test issuing many commands pays a
    /// reconnect each time, which is cheap on loopback.
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

    /// Stops and removes the container. Not required for cleanliness —
    /// `ContainerGuard`'s `Drop` reclaims it via rightsize's own cleanup
    /// thread — but explicit teardown keeps a test's failure output free of
    /// unrelated containers from earlier cases in the same run.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the backend's stop/remove calls fail.
    pub async fn stop(self) -> Result<(), String> {
        self.guard.stop().await.map_err(|error| error.to_string())
    }
}

/// Polls `cond` on a short fixed cadence until it returns `true`, or panics
/// once `timeout` elapses — the container-test analog of `tests/common`'s
/// in-process `eventually`.
///
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
