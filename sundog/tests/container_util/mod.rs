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

use rightsize::{Container, ContainerGuard, MountableFile, Network, Wait, WaitStrategy};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;

const CONTROL_PORT: u16 = 8080;
/// `sundog-testnode`'s `GET /metrics` port, served once it is built with
/// sundog's `prometheus` feature (see [`build_testnode`]); unused, but
/// harmlessly exposed, on a [`build_previous_testnode`] binary that predates
/// it. `pub` so a caller can build its own [`rightsize::WaitStrategy`]
/// against it, e.g. [`Wait::for_http`] for a wait that does not require the
/// control port to be up yet.
pub const METRICS_PORT: u16 = 9090;
const READY_LOG: &str = "testnode-ready";
/// Bound on [`Node::crash`]'s wait for the backend to confirm the container
/// process died.
const CRASH_WAIT: Duration = Duration::from_secs(30);

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
/// `CC_x86_64_unknown_linux_musl` to point at a musl-capable `cc`. Built with
/// sundog's `spill` and `prometheus` features on, so every container run has
/// both the spill tier (`SUNDOG_TESTNODE_SPILL_DIR` and friends) and a
/// `GET /metrics` endpoint available, whether or not a given test uses them.
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
                "--features",
                "spill,prometheus",
            ])
            .env("CC_x86_64_unknown_linux_musl", "musl-gcc")
            .current_dir(workspace_root())
            .status()
            .expect("invariant: cargo is on PATH in every environment this harness runs in");
        assert!(status.success(), "sundog-testnode musl build failed");
        workspace_root().join("target/x86_64-unknown-linux-musl/release/sundog-testnode")
    })
}

/// The release whose test node [`build_previous_testnode`] builds: the one
/// this checkout must interoperate with across a rolling upgrade.
pub const PREVIOUS_RELEASE_TAG: &str = "v0.6.0";

/// Env var a container test passes via [`Node::spawn_with_env`] to override
/// `ClusterConfig::crdt_retire_after` (`u64` seconds) down from its 24h
/// default to something a test can reasonably wait out, the same override
/// shape as `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` and friends (this file's
/// module doc names the pattern; `sundog-testnode`'s own crate doc lists
/// every `SUNDOG_TESTNODE_*` knob it currently reads). `sundog-testnode`
/// wires this straight into `ClusterConfig::crdt_retire_after`, the same
/// way `SUNDOG_TESTNODE_AE_PART_MIN_BUCKET` etc. already are.
pub const CRDT_RETIRE_AFTER_SECS_ENV: &str = "SUNDOG_TESTNODE_CRDT_RETIRE_AFTER_SECS";

/// `RUST_LOG` value every `distributed_*` container test, plus the
/// `cold_join_*`/`warm_join_*` ones, passes to its spawned nodes as
/// `("RUST_LOG", DISTRIBUTED_RUST_LOG)` in `extra_env`: `info` broadly, and
/// `debug` on exactly the cluster state-machine internals a rebalance,
/// state-transfer, or anti-entropy timeout needs visible in the log dump
/// `eventually_with_logs` prints, plus `sundog::net=debug` for the
/// connection-level detail those hand off through. `chitchat=warn` keeps
/// gossip's own per-round chatter out of that dump; `eventually_with_logs`
/// also filters its cross-cluster "addressed to a different cluster"/"wrong
/// cluster" lines by content, since a previous test's still-running
/// containers can still emit them at `warn`.
pub const DISTRIBUTED_RUST_LOG: &str = "info,sundog::cluster::rebalance=debug,\
     sundog::cluster::state_transfer=debug,sundog::cluster::anti_entropy=debug,\
     sundog::net=debug,sundog::ownership=debug,chitchat=warn";

/// Builds the previous release's `sundog-testnode` from its git tag, once per
/// test process, into `target/prev-release/` and returns the musl binary
/// path. The tag is fetched if the clone lacks it, as a shallow CI checkout
/// does.
/// # Panics
///
/// Panics if the tag cannot be fetched or checked out, or the build fails.
pub fn build_previous_testnode() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let root = workspace_root();
        let base = root.join("target").join("prev-release");
        let src = base.join(format!("src-{PREVIOUS_RELEASE_TAG}"));
        let target_dir = base.join("target");
        if !src.join("Cargo.toml").exists() {
            let has_tag = Command::new("git")
                .args([
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    &format!("refs/tags/{PREVIOUS_RELEASE_TAG}"),
                ])
                .current_dir(&root)
                .status()
                .expect("git runs")
                .success();
            if !has_tag {
                let fetched = Command::new("git")
                    .args([
                        "fetch",
                        "--depth",
                        "1",
                        "origin",
                        "tag",
                        PREVIOUS_RELEASE_TAG,
                    ])
                    .current_dir(&root)
                    .status()
                    .expect("git runs");
                assert!(
                    fetched.success(),
                    "fetching tag {PREVIOUS_RELEASE_TAG} succeeds"
                );
            }
            std::fs::create_dir_all(&base).expect("target/prev-release is creatable");
            // A restored CI cache can leave the directory behind without its
            // worktree metadata or sources; `git worktree add` refuses a
            // path that exists, so clear both before adding.
            if src.exists() {
                std::fs::remove_dir_all(&src).expect("a stale prev-release checkout is removable");
            }
            let _ = Command::new("git")
                .args(["worktree", "prune"])
                .current_dir(&root)
                .status();
            let added = Command::new("git")
                .args(["worktree", "add", "--detach"])
                .arg(&src)
                .arg(PREVIOUS_RELEASE_TAG)
                .current_dir(&root)
                .status()
                .expect("git runs");
            assert!(
                added.success(),
                "checking out {PREVIOUS_RELEASE_TAG} succeeds"
            );
        }
        let status = Command::new("cargo")
            .args([
                "build",
                "--release",
                "--target",
                "x86_64-unknown-linux-musl",
                "-p",
                "sundog-testnode",
                "--target-dir",
            ])
            .arg(&target_dir)
            .env("CC_x86_64_unknown_linux_musl", "musl-gcc")
            .current_dir(&src)
            .status()
            .expect("cargo build of the previous release spawns");
        assert!(status.success(), "the previous release's test node builds");
        target_dir.join("x86_64-unknown-linux-musl/release/sundog-testnode")
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
    metrics_port: u16,
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
        Self::spawn_binary(net, cluster_name, alias, seeds, extra_env, build_testnode()).await
    }

    /// [`Node::spawn`] for a `Mode::Distributed` `"it"`:
    /// `SUNDOG_TESTNODE_MODE=distributed`, plus `SUNDOG_TESTNODE_OWNERS` set
    /// to `owners` when given (absent means the two-owner default).
    /// # Panics
    ///
    /// Panics if the container fails to start or never becomes ready.
    pub async fn spawn_distributed(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        owners: Option<u8>,
    ) -> Node {
        Self::spawn_distributed_with_env(net, cluster_name, alias, seeds, owners, &[]).await
    }

    /// [`Node::spawn_distributed`] with additional container environment
    /// variables layered on top of the `Mode::Distributed` defaults, the
    /// same shape [`Node::spawn_with_env`] adds to [`Node::spawn`].
    /// # Panics
    ///
    /// Panics if the container fails to start or never becomes ready.
    pub async fn spawn_distributed_with_env(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        owners: Option<u8>,
        extra_env: &[(&str, &str)],
    ) -> Node {
        let owners_str;
        let mut env = vec![("SUNDOG_TESTNODE_MODE", "distributed")];
        if let Some(owners) = owners {
            owners_str = owners.to_string();
            env.push(("SUNDOG_TESTNODE_OWNERS", owners_str.as_str()));
        }
        env.extend_from_slice(extra_env);
        Self::spawn_with_env(net, cluster_name, alias, seeds, &env).await
    }

    /// [`Node::spawn_with_env`] running `bin` instead of this checkout's
    /// test node: [`build_previous_testnode`] for a mixed-version cluster.
    /// # Panics
    ///
    /// Panics if the container fails to start or never becomes ready.
    pub async fn spawn_binary(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        extra_env: &[(&str, &str)],
        bin: &Path,
    ) -> Node {
        Self::spawn_binary_with_wait(
            net,
            cluster_name,
            alias,
            seeds,
            extra_env,
            &[],
            bin,
            Wait::for_log_message(READY_LOG, 1),
        )
        .await
    }

    /// [`Node::spawn_with_env_and_wait`] plus `mounts`, each a
    /// `(host_path, guest_path)` pair bind-mounted read-write into the
    /// container: on both backends a guest write reaches the host path, so
    /// the same `host_path` mounted again under the same alias after a
    /// [`Node::stop`] sees whatever the previous container's process left
    /// behind there, unlike every other path inside the container's own
    /// otherwise-fresh filesystem. The way to test a restart that is
    /// expected to find its spill directory preserved, `sundog-testnode`'s
    /// `SUNDOG_TESTNODE_SPILL_DIR` pointed at a mount's `guest_path`.
    /// # Panics
    ///
    /// Panics if the container fails to start or never satisfies `wait`.
    pub async fn spawn_with_env_mounts_and_wait(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        extra_env: &[(&str, &str)],
        mounts: &[(&str, &str)],
        wait: impl WaitStrategy + 'static,
    ) -> Node {
        Self::spawn_binary_with_wait(
            net,
            cluster_name,
            alias,
            seeds,
            extra_env,
            mounts,
            build_testnode(),
            wait,
        )
        .await
    }

    /// [`Node::spawn_with_env_mounts_and_wait`] running `bin` instead of
    /// this checkout's test node: [`build_previous_testnode`] for a mixed-
    /// version cluster whose restarting node also needs a bind-mounted
    /// spill directory, the combination
    /// [`Node::spawn_binary`]/[`Node::spawn_with_env_mounts_and_wait`] each
    /// cover only one half of.
    /// # Panics
    ///
    /// Panics if the container fails to start or never satisfies `wait`.
    #[expect(
        clippy::too_many_arguments,
        reason = "a thin wrapper over spawn_binary_with_wait, itself already carrying the \
                  same too-many-arguments allowance for the same reason: every parameter is \
                  independent container-boot context"
    )]
    pub async fn spawn_binary_with_env_mounts_and_wait(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        extra_env: &[(&str, &str)],
        mounts: &[(&str, &str)],
        bin: &Path,
        wait: impl WaitStrategy + 'static,
    ) -> Node {
        Self::spawn_binary_with_wait(
            net,
            cluster_name,
            alias,
            seeds,
            extra_env,
            mounts,
            bin,
            wait,
        )
        .await
    }

    /// [`Node::spawn_with_env`], but returns as soon as `wait` reports ready
    /// instead of waiting for the `testnode-ready` log line, for a caller
    /// that wants the guard back before the node has finished its cache
    /// warm-up, e.g. to observe `/metrics` the moment it starts serving,
    /// well before a `Mode::Replicated` cache's state transfer runs. A
    /// `Node` returned this way may not have a live control port yet: its
    /// own [`Node::command`] connections fail until `sundog-testnode`'s
    /// listener binds, same as connecting too early to any other port.
    /// # Panics
    ///
    /// Panics if the container fails to start or never satisfies `wait`.
    pub async fn spawn_with_env_and_wait(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        extra_env: &[(&str, &str)],
        wait: impl WaitStrategy + 'static,
    ) -> Node {
        Self::spawn_binary_with_wait(
            net,
            cluster_name,
            alias,
            seeds,
            extra_env,
            &[],
            build_testnode(),
            wait,
        )
        .await
    }

    /// The actual container-boot logic every `spawn*` constructor shares,
    /// parametrized on the readiness check so [`Node::spawn_with_env_and_wait`]
    /// can substitute its own without duplicating the rest. `mounts`, each a
    /// `(host_path, guest_path)` pair, is bind-mounted read-write into the
    /// container alongside the test-node binary itself; see
    /// [`Node::spawn_with_env_mounts_and_wait`].
    /// # Panics
    ///
    /// Panics if the container fails to start or never satisfies `wait`.
    #[expect(
        clippy::too_many_arguments,
        reason = "every parameter is independent container-boot context every spawn* \
                  constructor shares; grouping any subset into a struct would only rename the \
                  same eight pieces of state"
    )]
    async fn spawn_binary_with_wait(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        extra_env: &[(&str, &str)],
        mounts: &[(&str, &str)],
        bin: &Path,
        wait: impl WaitStrategy + 'static,
    ) -> Node {
        rightsize_modules::register_default_backends();

        let mut container = Container::new(&base_image())
            .with_network(net)
            .with_network_aliases(&[alias])
            .with_exposed_ports(&[CONTROL_PORT, METRICS_PORT])
            .with_copy_file_to_container(
                MountableFile::for_host_path(&bin.to_string_lossy()),
                "/sundog-testnode",
            )
            .with_env("SUNDOG_SEEDS", &seeds.join(","))
            .with_command(&["/sundog-testnode", cluster_name]);
        for &(host_path, guest_path) in mounts {
            container = container
                .with_copy_file_to_container(MountableFile::for_host_path(host_path), guest_path);
        }
        for &(key, value) in extra_env {
            container = container.with_env(key, value);
        }
        let guard = container
            .waiting_for(wait)
            .start()
            .await
            .expect("test-node container starts and becomes ready");

        let control_port = guard
            .get_mapped_port(CONTROL_PORT)
            .expect("invariant: control port was declared via with_exposed_ports");
        let metrics_port = guard
            .get_mapped_port(METRICS_PORT)
            .expect("invariant: metrics port was declared via with_exposed_ports");
        Node {
            guard,
            control_port,
            metrics_port,
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

    /// `fetch k`, returning `Some(value)` on `val <v>` and `None` on `none`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails, or the reply is `err ...` or
    /// matches neither `val <v>` nor `none`.
    pub async fn fetch(&self, key: &str) -> Result<Option<String>, String> {
        match self.command(&format!("fetch {key}")).await? {
            reply if reply == "none" => Ok(None),
            reply => reply
                .strip_prefix("val ")
                .map(str::to_string)
                .map(Some)
                .ok_or(reply),
        }
    }

    /// `owners k`, the key's owning node ids in rendezvous score order.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or any id fails to parse.
    pub async fn owners(&self, key: &str) -> Result<Vec<u64>, String> {
        let reply = self.command(&format!("owners {key}")).await?;
        reply
            .split_whitespace()
            .map(|id| {
                id.parse()
                    .map_err(|error| format!("bad owner id {id:?}: {error}"))
            })
            .collect()
    }

    /// `id`, this node's own `NodeId` as a decimal `u64`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not numeric.
    pub async fn node_id(&self) -> Result<u64, String> {
        self.command("id")
            .await?
            .parse()
            .map_err(|error| format!("bad id reply: {error}"))
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

    /// `pnfill n`, bulk-incrementing `pn0..pn(n-1)` by one from this node on
    /// the `"pn"` `PnCounter` cache, no control round trip per entry.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the node reports an error.
    pub async fn pn_fill(&self, count: u32) -> Result<(), String> {
        match self.command(&format!("pnfill {count}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(other.to_string()),
        }
    }

    /// `pncount`, the `"pn"` cache's live-entry count.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not numeric.
    pub async fn pn_count(&self) -> Result<usize, String> {
        self.command("pncount")
            .await?
            .parse()
            .map_err(|error| format!("bad pncount reply: {error}"))
    }

    /// `pnbytes n`, the summed resident encoded size of `pn0..pn(n-1)` on
    /// this node: the record bytes a cold join or a state transfer carries
    /// for those counters.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not numeric.
    pub async fn pn_bytes(&self, count: u32) -> Result<u64, String> {
        self.command(&format!("pnbytes {count}"))
            .await?
            .parse()
            .map_err(|error| format!("bad pnbytes reply: {error}"))
    }

    /// `pndump k`, key `k`'s resident counter in its `Debug` form, or
    /// `none`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails.
    pub async fn pn_dump(&self, key: &str) -> Result<String, String> {
        self.command(&format!("pndump {key}")).await
    }

    /// `pnget k`, returning `Some(value)` on `val <v>` and `None` on `none`.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply matches neither.
    pub async fn pn_get(&self, key: &str) -> Result<Option<i64>, String> {
        match self.command(&format!("pnget {key}")).await? {
            reply if reply == "none" => Ok(None),
            reply => reply
                .strip_prefix("val ")
                .and_then(|value| value.parse::<i64>().ok())
                .map(Some)
                .ok_or(reply),
        }
    }

    /// `osadd k e`, merging an [`sundog::crdt::OrSet::add`] delta tagged
    /// with this node's own current writer identity into key `k` of the
    /// `"os"` cache.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not `ok`.
    pub async fn os_add(&self, key: &str, elem: &str) -> Result<(), String> {
        match self.command(&format!("osadd {key} {elem}")).await?.as_str() {
            "ok" => Ok(()),
            other => Err(other.to_string()),
        }
    }

    /// `osmembers k`: `None` for a key never written, `Some(members)`,
    /// alphabetically sorted per `sundog-testnode`'s own rendering, for a
    /// key that has been written, `Some(vec![])` included if every element
    /// has since been removed (a written, currently empty set is not the
    /// same as an unwritten key).
    /// # Errors
    ///
    /// Returns `Err` if the connection fails.
    pub async fn os_members(&self, key: &str) -> Result<Option<Vec<String>>, String> {
        let reply = self.command(&format!("osmembers {key}")).await?;
        if reply == "none" {
            return Ok(None);
        }
        if reply.is_empty() {
            return Ok(Some(Vec::new()));
        }
        Ok(Some(reply.split_whitespace().map(str::to_string).collect()))
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

    /// `digest`, this node's order-independent xxh3 digest of `"it"`'s live
    /// content (see `sundog-testnode`'s module docs for the exact formula):
    /// two nodes holding identical content return the same digest, and any
    /// single differing, missing, or extra entry changes it.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply isn't 16 hex digits.
    pub async fn digest(&self) -> Result<u64, String> {
        let reply = self.command("digest").await?;
        u64::from_str_radix(&reply, 16).map_err(|error| format!("bad digest reply: {error}"))
    }

    /// `GET /metrics` on this node's mapped `METRICS_PORT`, returning the raw
    /// Prometheus text-exposition body: `sundog-testnode` built with
    /// sundog's `prometheus` feature serves it via
    /// `ClusterBuilder::prometheus_listen`. A fresh connection per call, like
    /// [`Node::command`].
    /// # Errors
    ///
    /// Returns `Err` if the connection fails, or the response has no
    /// `\r\n\r\n` header/body separator.
    pub async fn metrics(&self) -> Result<String, String> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.metrics_port))
            .await
            .map_err(|error| format!("connect to {}: {error}", self.metrics_port))?;
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .map_err(|error| format!("write: {error}"))?;
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .map_err(|error| format!("read: {error}"))?;
        response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .ok_or_else(|| format!("no header/body separator in metrics response: {response:?}"))
    }

    /// This node's captured container logs (stdout/stderr), for a timeout
    /// failure's diagnostics. Never fails: a backend error comes back as
    /// part of the returned string instead of `Err`, since the only caller,
    /// [`eventually_with_logs`], is already mid-panic over a different
    /// failure and has nothing useful to do with a second one.
    #[must_use]
    pub async fn logs(&self) -> String {
        self.guard
            .logs()
            .await
            .unwrap_or_else(|error| format!("<failed to fetch logs for {}: {error}>", self.name()))
    }

    /// `crash`: sends the command, waits for the backend to confirm the
    /// container process died, then removes the (already-dead)
    /// container so `name()`'s alias is free for a fresh [`Node::spawn`].
    ///
    /// `ContainerGuard::is_running` only tracks whether this guard's own
    /// `stop()` has run, so it has no way to observe a death this process
    /// didn't itself cause; death is detected the way the backend
    /// itself would notice: `docker exec` against an exited container
    /// fails, so polling `exec` until it errors is that confirmation.
    /// # Errors
    ///
    /// Returns `Err` if the container has not died within
    /// [`CRASH_WAIT`], or if the cleanup `stop()`/remove afterward fails.
    pub async fn crash(self) -> Result<(), String> {
        // The reply is flushed before the process exits, but the connection
        // can still race the exit on a loaded runner, so a failed round trip
        // here is not itself a failure to crash.
        let _ = self.command("crash").await;

        let deadline = tokio::time::Instant::now() + CRASH_WAIT;
        while self.guard.exec(&["true"]).await.is_ok() {
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "{} did not stop within {CRASH_WAIT:?} of crash",
                    self.name()
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        self.stop().await
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

/// Substrings [`eventually_with_logs`] drops from a timeout's log dump: a
/// previous test's containers, still gossiping into this one's shared
/// network because a panic never reached their `stop()` calls, spam these
/// at `warn` regardless of `RUST_LOG`, and at the volume seen in practice
/// they crowd the capped tail out of everything else. Counted and
/// summarized instead of silently dropped, so the dump still says a leak
/// happened.
const CHITCHAT_MARKERS: [&str; 2] = [
    "addressed to a different cluster",
    "message rejected by peer: wrong cluster",
];

/// [`eventually`], but on a timeout, prints every one of `nodes`' captured
/// logs (its alias, then its last 1500 lines, [`CHITCHAT_MARKERS`] filtered
/// out and counted first so the cap holds state-machine tracing rather than
/// another test's stale gossip) to stderr before panicking, so a CI job's
/// log carries enough of the cluster's own state-machine tracing to
/// diagnose the failure without reproducing it locally. Follows each node's
/// log dump with its `sundog_`-prefixed Prometheus metric lines (`# HELP`/
/// `# TYPE` lines and histogram bucket lines omitted), since a previous-
/// release node installs no tracing subscriber and so has nothing in its
/// log dump, but every test node still serves `/metrics`; a metrics fetch
/// error prints in place of the metric lines.
/// # Panics
///
/// Panics if `cond` has not returned `true` by `timeout`.
pub async fn eventually_with_logs<F, Fut>(timeout: Duration, nodes: &[&Node], mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    const TAIL_LINES: usize = 1500;

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            for (index, node) in nodes.iter().enumerate() {
                let logs = node.logs().await;
                let mut chitchat_lines = 0usize;
                let kept: Vec<&str> = logs
                    .lines()
                    .filter(|line| {
                        let is_chitchat =
                            CHITCHAT_MARKERS.iter().any(|marker| line.contains(*marker));
                        if is_chitchat {
                            chitchat_lines += 1;
                        }
                        !is_chitchat
                    })
                    .collect();
                let tail: Vec<&str> = kept.iter().rev().take(TAIL_LINES).copied().collect();
                eprintln!(
                    "----- node[{index}] {} (last {} lines) -----",
                    node.name(),
                    tail.len()
                );
                if chitchat_lines > 0 {
                    eprintln!(
                        "  ({chitchat_lines} cross-cluster gossip chitchat line(s) filtered out \
                         of this node's log, likely from a previous test's still-running \
                         containers)"
                    );
                }
                for line in tail.into_iter().rev() {
                    eprintln!("{line}");
                }
                eprintln!("----- node[{index}] {} metrics -----", node.name());
                match node.metrics().await {
                    Ok(body) => {
                        for line in body.lines() {
                            if line.starts_with("# HELP") || line.starts_with("# TYPE") {
                                continue;
                            }
                            let metric_name = line
                                .split(|c: char| c == '{' || c.is_whitespace())
                                .next()
                                .unwrap_or("");
                            if !metric_name.starts_with("sundog_")
                                || metric_name.ends_with("_bucket")
                            {
                                continue;
                            }
                            eprintln!("{line}");
                        }
                    }
                    Err(error) => eprintln!("{error}"),
                }
            }
            panic!("condition not met within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Every `sundog-testnode` binds gossip on this fixed port; a seed string
/// is `<alias>:<GOSSIP_PORT>`, resolved via DNS against the alias.
pub const GOSSIP_PORT: u16 = 7946;

/// A seed string for `alias`, in the `<alias>:<GOSSIP_PORT>` shape every
/// `sundog-testnode` resolves via DNS.
pub fn seed(alias: &str) -> String {
    format!("{alias}:{GOSSIP_PORT}")
}

/// Bound on [`wait_for_peers`]' wait for `Node::peers` to reach the
/// expected count.
const PEER_WAIT: Duration = Duration::from_secs(30);

/// Waits for every one of `nodes` to report `expected` peers.
pub async fn wait_for_peers(nodes: &[&Node], expected: usize) {
    for node in nodes {
        eventually(PEER_WAIT, || async { node.peers().await == Ok(expected) }).await;
    }
}

/// Spawns a 3-node cluster under `cluster_name`: `n1` with no seeds, `n2`
/// seeded on `n1`, `n3` seeded on `n1` and `n2`, then waits for every node
/// to see both peers before returning.
/// # Panics
///
/// Panics if any node fails to start or become ready, or if any node has
/// not converged on 2 peers within [`PEER_WAIT`].
pub async fn spawn_trio(net: &Arc<Network>, cluster_name: &str) -> (Node, Node, Node) {
    let n1 = Node::spawn(net, cluster_name, "n1", &[]).await;
    let n2 = Node::spawn(net, cluster_name, "n2", &[&seed("n1")]).await;
    let n3 = Node::spawn(net, cluster_name, "n3", &[&seed("n1"), &seed("n2")]).await;
    wait_for_peers(&[&n1, &n2, &n3], 2).await;
    (n1, n2, n3)
}

/// A `Vec<Node>` that stops every node it still holds, in a blocking
/// [`Drop`], when the `Fleet` itself is dropped: a panic-only backstop for
/// a multi-node container test, so its containers stop gossiping into the
/// next test's network instead of lingering on `rightsize::ContainerGuard`'s
/// own, slower, backgrounded teardown (see that type's `Drop` impl: it
/// enqueues the actual backend stop/remove call onto a dedicated cleanup
/// thread and returns immediately, rather than waiting for it).
///
/// [`Node::stop`] both consumes `self` and is `async`, so nothing already
/// available can stop a node from a borrowed, synchronous `Drop`:
/// `ContainerGuard` (the type `Node` wraps) exposes no `kill`, and its only
/// public `stop` also consumes `self`. A test using `Fleet` pushes every
/// node it spawns onto `fleet.0` and, on the success path, calls
/// [`Fleet::take`] to get them back and `stop()` each one explicitly
/// exactly as before (plus `net.close()`); `Fleet::drop` only has anything
/// to do when a panic skipped that, in which case the `Vec` it takes is
/// still full.
pub struct Fleet(pub Vec<Node>);

impl Fleet {
    /// Empties the fleet and returns its nodes, for the success path to
    /// `stop()` explicitly (in whatever order/grouping the test wants,
    /// e.g. alongside a `net.close()`), leaving `Fleet::drop` with nothing
    /// left to do.
    pub fn take(&mut self) -> Vec<Node> {
        std::mem::take(&mut self.0)
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        let nodes = std::mem::take(&mut self.0);
        if nodes.is_empty() {
            return; // the success path already took them; nothing to stop.
        }
        // `Node::stop` is async, and this `Drop` can run while unwinding a
        // panic on a thread already inside a Tokio runtime (the `#[tokio::
        // test]` task itself), where `Handle::block_on` panics rather than
        // nesting. A dedicated OS thread with its own throwaway
        // current-thread runtime has no such conflict; the only blocking
        // call back on this thread is `JoinHandle::join`, an ordinary
        // synchronous thread join, not a nested `block_on`.
        let spawned = std::thread::Builder::new()
            .name("fleet-panic-stop".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("invariant: a throwaway current-thread runtime always builds");
                runtime.block_on(async move {
                    for node in nodes {
                        let name = node.name().to_string();
                        if let Err(error) = node.stop().await {
                            eprintln!("Fleet::drop: best-effort stop of {name} failed: {error}");
                        }
                    }
                });
            });
        match spawned {
            Ok(handle) => {
                if handle.join().is_err() {
                    eprintln!("Fleet::drop: the panic-stop thread itself panicked");
                }
            }
            Err(error) => {
                eprintln!("Fleet::drop: failed to spawn the panic-stop thread: {error}");
            }
        }
    }
}
