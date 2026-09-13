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
/// process actually died.
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
/// default to something a test can actually wait out, the same override
/// shape as `SUNDOG_TESTNODE_MAX_CAPACITY_BYTES` and friends (this file's
/// module doc names the pattern; `sundog-testnode`'s own crate doc lists
/// every `SUNDOG_TESTNODE_*` knob it currently reads). `sundog-testnode`
/// wires this straight into `ClusterConfig::crdt_retire_after`, the same
/// way `SUNDOG_TESTNODE_AE_PART_MIN_BUCKET` etc. already are.
pub const CRDT_RETIRE_AFTER_SECS_ENV: &str = "SUNDOG_TESTNODE_CRDT_RETIRE_AFTER_SECS";

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
        let owners_str;
        let mut env = vec![("SUNDOG_TESTNODE_MODE", "distributed")];
        if let Some(owners) = owners {
            owners_str = owners.to_string();
            env.push(("SUNDOG_TESTNODE_OWNERS", owners_str.as_str()));
        }
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
            bin,
            Wait::for_log_message(READY_LOG, 1),
        )
        .await
    }

    /// [`Node::spawn_with_env`], but returns as soon as `wait` reports ready
    /// instead of waiting for the `testnode-ready` log line — for a caller
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
            build_testnode(),
            wait,
        )
        .await
    }

    /// The actual container-boot logic every `spawn*` constructor shares,
    /// parametrized on the readiness check so [`Node::spawn_with_env_and_wait`]
    /// can substitute its own without duplicating the rest.
    /// # Panics
    ///
    /// Panics if the container fails to start or never satisfies `wait`.
    async fn spawn_binary_with_wait(
        net: &Arc<Network>,
        cluster_name: &str,
        alias: &str,
        seeds: &[&str],
        extra_env: &[(&str, &str)],
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

    /// `osremove k e`, merging an [`sundog::crdt::OrSet::remove`] delta
    /// against this node's own currently observed copy of key `k` into it.
    /// # Errors
    ///
    /// Returns `Err` if the connection fails or the reply is not `ok`.
    pub async fn os_remove(&self, key: &str, elem: &str) -> Result<(), String> {
        match self
            .command(&format!("osremove {key} {elem}"))
            .await?
            .as_str()
        {
            "ok" => Ok(()),
            other => Err(other.to_string()),
        }
    }

    /// `osmembers k`: `None` for a key never written, `Some(members)` —
    /// alphabetically sorted, per `sundog-testnode`'s own rendering — for a
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

    /// Cuts this node off the network entirely — `ip link set <iface> down`
    /// inside the container, via [`ContainerGuard::exec`], which reaches the
    /// container through the backend directly rather than over the network
    /// this severs, so it (and [`Node::heal`]) keep working on an already
    /// partitioned node — without stopping its process. Unlike
    /// [`Node::stop`]/[`Node::crash`], the process keeps running and never
    /// gets a chance to gossip anything, gracefully or otherwise, so every
    /// peer sees exactly a genuine network partition: the node drops out of
    /// the live set with no departure signal at all, distinct from a
    /// confirmed-gone member the same way CRDT compaction's own
    /// partitioned-vs-retired distinction
    /// (`crate::cluster::crdt_compact_tick`'s dead/quiet predicates)
    /// requires.
    ///
    /// The interface name is detected at call time (`eth0` first, falling
    /// back to the first non-loopback interface `ip -o link show` reports),
    /// so this only requires the container image's busybox to provide the
    /// `ip` applet — true of `alpine:3.22`, this file's default base image —
    /// not a full `iproute2`/`iptables` install.
    /// # Errors
    ///
    /// Returns `Err` if the exec call itself fails, or the in-container
    /// command exits non-zero (e.g. no `ip` applet in a non-default base
    /// image).
    pub async fn partition(&self) -> Result<(), String> {
        self.set_interface_state("down").await
    }

    /// Reverses [`Node::partition`], restoring this node's network
    /// connectivity without having ever stopped its process.
    /// # Errors
    ///
    /// Returns `Err` if the exec call itself fails, or the in-container
    /// command exits non-zero.
    pub async fn heal(&self) -> Result<(), String> {
        self.set_interface_state("up").await
    }

    /// The `ip link set <iface> {up,down}` `sh -c` script both
    /// [`Node::partition`] and [`Node::heal`] run.
    async fn set_interface_state(&self, state: &str) -> Result<(), String> {
        let script = format!(
            "set -e; \
             iface=eth0; \
             if ! ip link show \"$iface\" >/dev/null 2>&1; then \
                 iface=$(ip -o link show | awk -F': ' '$2 != \"lo\" {{print $2; exit}}'); \
             fi; \
             ip link set \"$iface\" {state}"
        );
        let result = self
            .guard
            .exec(&["sh", "-c", script.as_str()])
            .await
            .map_err(|error| {
                format!("exec failed setting this node's interface {state}: {error}")
            })?;
        if result.exit_code == 0 {
            Ok(())
        } else {
            Err(format!(
                "setting this node's interface {state} exited {}: stdout={:?} stderr={:?}",
                result.exit_code, result.stdout, result.stderr
            ))
        }
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

    /// `crash`: sends the command, waits for the backend to confirm the
    /// container process actually died, then removes the (already-dead)
    /// container so `name()`'s alias is free for a fresh [`Node::spawn`].
    ///
    /// `ContainerGuard::is_running` only tracks whether this guard's own
    /// `stop()` has run — it has no way to observe a death this process
    /// didn't itself cause — so death is detected the way the backend
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
