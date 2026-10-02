//! Child processes: spawning test nodes and signalling them.
//!
//! A test node is the `sundog-testnode` binary. [`spawn`] starts one with its
//! environment, waits for the `testnode-ready` line it prints once its
//! cluster and caches are open, and keeps draining its standard output so a
//! later write never blocks. Every function that decides something from
//! values, rather than from the machine, is pure and tested without a
//! process.

use std::fs::File;
use std::io;
use std::net::Ipv4Addr;
use std::num::NonZeroU8;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use smol_str::SmolStr;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::watch;

use super::SlotInfo;
use super::layout::Layout;

/// The gossip port of every test node.
pub const GOSSIP_PORT: u16 = 7946;

/// The control port of every test node.
pub const CONTROL_PORT: u16 = 8080;

/// The exporter port of every test node.
pub const METRICS_PORT: u16 = 9090;

/// How far a node's exporter port lies above its gossip port, in every fleet
/// layout: the scrape template relies on it.
pub const METRICS_OFFSET: u16 = METRICS_PORT - GOSSIP_PORT;

/// What a test node prints when its cluster and caches are open.
pub const READY_LINE: &str = "testnode-ready";

/// How long a test node has to print [`READY_LINE`]. A lone first node waits
/// the first-peer grace of every cache it opens, about four seconds each, so
/// it is ready some sixteen seconds after it starts; a node that joins a
/// cluster is ready in a second or two.
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The command that builds a test node the demo can use.
pub const BUILD_HINT: &str = "cargo build --release -p sundog-testnode --features prometheus";

/// The file name of the test node binary.
pub const BINARY: &str = "sundog-testnode";

/// How many lines of a node's log an error quotes.
const LOG_TAIL_LINES: usize = 12;

/// The address of slot `slot` (1-based): the `slot`th address counting from
/// `base`. `None` past the end of the IPv4 space.
#[must_use]
pub fn slot_ip(base: Ipv4Addr, slot: usize) -> Option<Ipv4Addr> {
    let offset = u32::try_from(slot.checked_sub(1)?).ok()?;
    u32::from(base).checked_add(offset).map(Ipv4Addr::from)
}

/// The label of slot `slot` (1-based): `n1`, `n2`, ...
#[must_use]
pub fn slot_label(slot: usize) -> SmolStr {
    SmolStr::from(format!("n{slot}"))
}

/// The slot (1-based) a label names: `n3` is 3. `None` for anything that is
/// not `n` and a positive number.
#[must_use]
pub fn parse_label(label: &str) -> Option<usize> {
    let digits = label.strip_prefix('n')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok().filter(|&slot| slot >= 1)
}

/// The `SUNDOG_SEEDS` value of every node: the gossip addresses of the first
/// two slots, so a node joins through either. A layout with a single slot
/// address repeats the first.
#[must_use]
pub fn seed_list(layout: Layout) -> String {
    let seeds = layout.seeds();
    let first = seeds.first().copied();
    let second = seeds.get(1).copied().or(first);
    [first, second]
        .into_iter()
        .flatten()
        .map(|addr| addr.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// The environment one test node starts with. In the shared layout the node
/// also gets its three ports, which the fixed defaults would otherwise give
/// every slot alike; in the per-address layout it keeps the defaults.
#[must_use]
pub fn node_env(
    layout: Layout,
    info: &SlotInfo,
    seeds: &str,
    owners: NonZeroU8,
) -> Vec<(&'static str, String)> {
    let mut env = vec![
        ("SUNDOG_TESTNODE_BIND_IP", info.ip.to_string()),
        ("SUNDOG_SEEDS", seeds.to_owned()),
        ("SUNDOG_TESTNODE_MODE", "distributed".to_owned()),
        ("SUNDOG_TESTNODE_OWNERS", owners.get().to_string()),
        ("SUNDOG_TESTNODE_SIDE_CACHES", "on".to_owned()),
        ("RUST_LOG", "warn".to_owned()),
    ];
    if layout.is_shared() {
        env.extend([
            (
                "SUNDOG_TESTNODE_GOSSIP_PORT",
                info.gossip.port().to_string(),
            ),
            (
                "SUNDOG_TESTNODE_CONTROL_PORT",
                info.control.port().to_string(),
            ),
            (
                "SUNDOG_TESTNODE_METRICS_PORT",
                info.metrics.port().to_string(),
            ),
        ]);
    }
    env
}

/// Where the standard error of the `seq`th start of slot `slot` goes.
#[must_use]
pub fn log_path(dir: &Path, slot: usize, seq: u32) -> PathBuf {
    dir.join(format!("n{slot}-{seq}.log"))
}

/// Whether `line` is the ready line.
#[must_use]
pub fn is_ready_line(line: &str) -> bool {
    line.trim() == READY_LINE
}

/// The places to look for the test node binary, in order: the explicit path,
/// the environment's, the directory of the running executable, then the
/// release and debug build directories under `target`.
#[must_use]
pub fn candidate_paths(
    explicit: Option<&Path>,
    from_env: Option<&Path>,
    exe_dir: Option<&Path>,
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    paths.extend(explicit.map(Path::to_path_buf));
    paths.extend(from_env.map(Path::to_path_buf));
    paths.extend(exe_dir.map(|dir| dir.join(BINARY)));
    paths.push(Path::new("target/release").join(BINARY));
    paths.push(Path::new("target/debug").join(BINARY));
    paths
}

/// The first of `candidates` that is a file.
#[must_use]
pub fn first_existing(candidates: &[PathBuf]) -> Option<&PathBuf> {
    candidates.iter().find(|path| path.is_file())
}

/// Finds the test node binary: `explicit` (the `--testnode` flag), then
/// `$SUNDOG_TESTNODE`, then beside the running executable, then under
/// `target`.
///
/// # Errors
///
/// Returns an error that lists the places looked at and prints [`BUILD_HINT`]
/// when none holds a file. An explicit path that is not a file is an error on
/// its own: it is never silently replaced by another binary.
pub fn find_testnode(explicit: Option<&Path>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        if path.is_file() {
            return Ok(path.to_path_buf());
        }
        bail!("--testnode {} is not a file", path.display());
    }
    let from_env = std::env::var_os("SUNDOG_TESTNODE").map(PathBuf::from);
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    let candidates = candidate_paths(None, from_env.as_deref(), exe_dir.as_deref());
    if let Some(found) = first_existing(&candidates) {
        return Ok(found.clone());
    }
    let looked: Vec<String> = candidates
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    Err(anyhow!(
        "no {BINARY} found (looked at {}); build one with `{BUILD_HINT}` or pass --testnode",
        looked.join(", ")
    ))
}

/// The last lines of a node's log, for an error message.
#[must_use]
pub fn log_tail(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    let from = lines.len().saturating_sub(LOG_TAIL_LINES);
    lines[from..].join("\n")
}

/// How far a started node has come.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    /// The node has not printed [`READY_LINE`] yet.
    Starting,
    /// The node is ready.
    Ready,
    /// The node closed its output before it was ready.
    Failed,
}

/// A running test node.
#[derive(Debug)]
pub struct NodeProc {
    /// The child process.
    pub child: Child,
    /// Its process id.
    pub pid: u32,
    /// The file its standard error goes to.
    pub log: PathBuf,
    ready: watch::Receiver<Readiness>,
}

impl NodeProc {
    /// How far the node has come.
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        self.ready.borrow().clone()
    }

    /// A handle that waits for the node to be ready.
    #[must_use]
    pub fn ready_handle(&self) -> ReadyHandle {
        ReadyHandle {
            ready: self.ready.clone(),
            log: self.log.clone(),
        }
    }
}

/// Waits for a started node to be ready. Cheap to clone.
#[derive(Debug, Clone)]
pub struct ReadyHandle {
    ready: watch::Receiver<Readiness>,
    log: PathBuf,
}

impl ReadyHandle {
    /// Waits until the node is ready.
    ///
    /// # Errors
    ///
    /// Returns an error when the node closes its output before it is ready,
    /// or when it takes longer than `timeout`. The error quotes the end of
    /// the node's log.
    pub async fn wait(mut self, timeout: Duration) -> anyhow::Result<()> {
        let outcome = tokio::time::timeout(
            timeout,
            self.ready.wait_for(|state| *state != Readiness::Starting),
        )
        .await;
        let reason = match outcome {
            Ok(Ok(state)) if *state == Readiness::Ready => return Ok(()),
            Ok(_) => "the node exited or closed its output before it was ready".to_owned(),
            Err(_) => format!("the node was not ready within {timeout:?}"),
        };
        bail!(
            "{reason}; its log {}:\n{}",
            self.log.display(),
            log_tail(&self.log)
        )
    }
}

/// Reads the node's standard output until [`READY_LINE`], reports it, and
/// keeps draining the stream so that a later write never blocks the node.
async fn watch_output(stdout: ChildStdout, state: watch::Sender<Readiness>) {
    let mut lines = BufReader::new(stdout).lines();
    let mut ready = false;
    while let Ok(Some(line)) = lines.next_line().await {
        if !ready && is_ready_line(&line) {
            ready = true;
            state.send_replace(Readiness::Ready);
        }
    }
    if !ready {
        state.send_replace(Readiness::Failed);
    }
}

/// How many times a start is retried while the binary is busy.
const BUSY_RETRIES: u32 = 100;

/// The wait between those retries.
const BUSY_WAIT: Duration = Duration::from_millis(10);

/// Starts `command`, retrying for up to a second while the system refuses to
/// run the file because a writer still holds it open (`ETXTBSY`, which Linux
/// returns and macOS does not): a binary that is being written, or whose
/// writer another process has inherited across a `fork` and not yet closed.
fn spawn_when_idle(command: &mut Command) -> io::Result<Child> {
    let mut tries = 0;
    loop {
        match command.spawn() {
            Err(error)
                if error.kind() == io::ErrorKind::ExecutableFileBusy && tries < BUSY_RETRIES =>
            {
                tries += 1;
                std::thread::sleep(BUSY_WAIT);
            }
            other => return other,
        }
    }
}

/// Starts the test node at `testnode` for `cluster` with `env`. It returns
/// as soon as the process runs; [`NodeProc::ready_handle`] waits for the node
/// to be ready. The node's standard error goes to `log`. It runs in its own
/// process group, so a Ctrl-C at the terminal reaches the lens and not the
/// node, and it is killed if the handle drops.
///
/// # Errors
///
/// Returns an error when the log cannot be created or the binary cannot
/// start.
pub fn spawn(
    testnode: &Path,
    cluster: &str,
    env: &[(&'static str, String)],
    log: &Path,
) -> anyhow::Result<NodeProc> {
    if let Some(dir) = log.parent() {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating the log directory {}", dir.display()))?;
    }
    let log_file =
        File::create(log).with_context(|| format!("creating the node log {}", log.display()))?;
    let mut command = Command::new(testnode);
    command
        .arg(cluster)
        .envs(env.iter().map(|(key, value)| (key, value)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log_file))
        .kill_on_drop(true);
    command.process_group(0);
    let mut child = spawn_when_idle(&mut command)
        .with_context(|| format!("starting {}", testnode.display()))?;
    let pid = child.id().context("the node has no process id")?;
    let stdout = child.stdout.take().context("the node has no output pipe")?;
    let (state, ready) = watch::channel(Readiness::Starting);
    tokio::spawn(watch_output(stdout, state));
    Ok(NodeProc {
        child,
        pid,
        log: log.to_path_buf(),
        ready,
    })
}

/// Sends SIGTERM to `pid`: the test node leaves the cluster and exits.
///
/// # Errors
///
/// Returns the system error when the signal cannot be sent, for instance
/// because the process is gone.
pub fn terminate(pid: u32) -> io::Result<()> {
    use rustix::process::{Pid, Signal, kill_process};
    let raw = i32::try_from(pid).map_err(|_| io::Error::other("the pid is out of range"))?;
    let pid = Pid::from_raw(raw).ok_or_else(|| io::Error::other("the pid is not a process"))?;
    kill_process(pid, Signal::TERM)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_count_up_from_the_base_address() {
        let base = Ipv4Addr::new(127, 0, 0, 11);
        assert_eq!(slot_ip(base, 1), Some(Ipv4Addr::new(127, 0, 0, 11)));
        assert_eq!(slot_ip(base, 6), Some(Ipv4Addr::new(127, 0, 0, 16)));
        assert_eq!(slot_ip(base, 0), None);
        assert_eq!(slot_ip(Ipv4Addr::BROADCAST, 2), None);
        assert_eq!(
            slot_ip(Ipv4Addr::new(127, 0, 0, 255), 2),
            Some(Ipv4Addr::new(127, 0, 1, 0))
        );
    }

    #[test]
    fn labels_round_trip_and_reject_what_is_not_a_slot() {
        assert_eq!(slot_label(3), "n3");
        assert_eq!(parse_label("n3"), Some(3));
        assert_eq!(parse_label("n12"), Some(12));
        for bad in ["", "n", "n0", "3", "m3", "n-1", "n3x", "N3", "n 3", "n+3"] {
            assert_eq!(parse_label(bad), None, "{bad:?}");
        }
        for slot in 1..50 {
            assert_eq!(parse_label(&slot_label(slot)), Some(slot));
        }
    }

    #[test]
    fn seeds_are_the_first_two_gossip_addresses() {
        assert_eq!(
            seed_list(Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 11))),
            "127.0.0.11:7946,127.0.0.12:7946"
        );
        assert_eq!(
            seed_list(Layout::PerAddress(Ipv4Addr::new(10, 1, 2, 255))),
            "10.1.2.255:7946,10.1.3.0:7946"
        );
        // Past the end of the space the second seed repeats the first.
        assert_eq!(
            seed_list(Layout::PerAddress(Ipv4Addr::BROADCAST)),
            "255.255.255.255:7946,255.255.255.255:7946"
        );
    }

    #[test]
    fn shared_seeds_are_the_first_two_ports_of_one_address() {
        assert_eq!(seed_list(Layout::Shared), "127.0.0.1:7946,127.0.0.1:7947");
    }

    #[test]
    fn the_exporter_lies_a_fixed_distance_above_gossip() {
        assert_eq!(METRICS_OFFSET, 1144);
        assert_eq!(GOSSIP_PORT + METRICS_OFFSET, METRICS_PORT);
    }

    fn env_value<'a>(env: &'a [(&'static str, String)], key: &str) -> Option<&'a str> {
        env.iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn a_node_starts_distributed_with_side_caches_and_quiet_logs() {
        let layout = Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 11));
        let env = node_env(
            layout,
            &SlotInfo::new(layout, 3).unwrap(),
            "127.0.0.11:7946,127.0.0.12:7946",
            NonZeroU8::new(2).unwrap(),
        );
        let get = |key: &str| env_value(&env, key);
        assert_eq!(get("SUNDOG_TESTNODE_BIND_IP"), Some("127.0.0.13"));
        assert_eq!(get("SUNDOG_SEEDS"), Some("127.0.0.11:7946,127.0.0.12:7946"));
        assert_eq!(get("SUNDOG_TESTNODE_MODE"), Some("distributed"));
        assert_eq!(get("SUNDOG_TESTNODE_OWNERS"), Some("2"));
        assert_eq!(get("SUNDOG_TESTNODE_SIDE_CACHES"), Some("on"));
        assert_eq!(get("RUST_LOG"), Some("warn"));
        // The fixed ports are the node's own defaults: no port variable.
        assert_eq!(env.len(), 6);
        for name in [
            "SUNDOG_TESTNODE_GOSSIP_PORT",
            "SUNDOG_TESTNODE_CONTROL_PORT",
            "SUNDOG_TESTNODE_METRICS_PORT",
        ] {
            assert_eq!(get(name), None, "{name}");
        }
        let owners = node_env(
            layout,
            &SlotInfo::new(layout, 1).unwrap(),
            "",
            NonZeroU8::new(3).unwrap(),
        );
        assert!(owners.contains(&("SUNDOG_TESTNODE_OWNERS", "3".to_owned())));
    }

    #[test]
    fn a_shared_node_gets_its_own_ports_on_the_shared_address() {
        let layout = Layout::Shared;
        let env = node_env(
            layout,
            &SlotInfo::new(layout, 2).unwrap(),
            "127.0.0.1:7946,127.0.0.1:7947",
            NonZeroU8::new(2).unwrap(),
        );
        let get = |key: &str| env_value(&env, key);
        assert_eq!(get("SUNDOG_TESTNODE_BIND_IP"), Some("127.0.0.1"));
        assert_eq!(get("SUNDOG_SEEDS"), Some("127.0.0.1:7946,127.0.0.1:7947"));
        assert_eq!(get("SUNDOG_TESTNODE_GOSSIP_PORT"), Some("7947"));
        assert_eq!(get("SUNDOG_TESTNODE_CONTROL_PORT"), Some("8081"));
        assert_eq!(get("SUNDOG_TESTNODE_METRICS_PORT"), Some("9091"));
        assert_eq!(get("SUNDOG_TESTNODE_MODE"), Some("distributed"));
        assert_eq!(get("SUNDOG_TESTNODE_OWNERS"), Some("2"));
        assert_eq!(get("SUNDOG_TESTNODE_SIDE_CACHES"), Some("on"));
        assert_eq!(get("RUST_LOG"), Some("warn"));
        assert_eq!(env.len(), 9);
    }

    #[test]
    fn the_log_of_each_start_has_its_own_file() {
        assert_eq!(
            log_path(Path::new("target/lens-demo"), 3, 2),
            Path::new("target/lens-demo/n3-2.log")
        );
    }

    #[test]
    fn only_the_ready_line_is_ready() {
        assert!(is_ready_line("testnode-ready"));
        assert!(is_ready_line("testnode-ready\r"));
        assert!(!is_ready_line("testnode-readyx"));
        assert!(!is_ready_line("starting"));
        assert!(!is_ready_line(""));
    }

    #[test]
    fn candidates_are_ordered_explicit_env_executable_release_debug() {
        let all = candidate_paths(
            Some(Path::new("/a/node")),
            Some(Path::new("/b/node")),
            Some(Path::new("/c")),
        );
        assert_eq!(
            all,
            [
                PathBuf::from("/a/node"),
                PathBuf::from("/b/node"),
                PathBuf::from("/c/sundog-testnode"),
                PathBuf::from("target/release/sundog-testnode"),
                PathBuf::from("target/debug/sundog-testnode"),
            ]
        );
        assert_eq!(candidate_paths(None, None, None).len(), 2);
    }

    #[test]
    fn the_first_candidate_that_is_a_file_wins() {
        let here = PathBuf::from(file!());
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let candidates = [PathBuf::from("/no/such/node"), manifest.clone(), here];
        assert_eq!(first_existing(&candidates), Some(&manifest));
        assert_eq!(first_existing(&[PathBuf::from("/no/such/node")]), None);
        assert_eq!(first_existing(&[]), None);
    }

    #[test]
    fn an_explicit_path_that_is_not_a_file_is_an_error_not_a_fallback() {
        let error = find_testnode(Some(Path::new("/no/such/node"))).unwrap_err();
        assert!(error.to_string().contains("/no/such/node"), "{error}");
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        assert_eq!(find_testnode(Some(&manifest)).unwrap(), manifest);
    }

    #[test]
    fn the_build_command_names_the_prometheus_feature() {
        assert!(BUILD_HINT.contains("--features prometheus"));
        assert!(BUILD_HINT.contains("sundog-testnode"));
    }

    #[test]
    fn a_log_tail_keeps_the_last_lines() {
        let dir = scratch_dir("tail");
        let path = dir.join("n1-1.log");
        let mut text = String::new();
        for i in 1..=30 {
            use std::fmt::Write as _;
            let _ = writeln!(text, "line {i}");
        }
        std::fs::write(&path, text).unwrap();
        let tail = log_tail(&path);
        assert_eq!(tail.lines().count(), LOG_TAIL_LINES);
        assert!(tail.ends_with("line 30"));
        assert!(!tail.contains("line 18\n"));
        assert_eq!(log_tail(&dir.join("missing.log")), "");
    }

    /// A fresh directory under the system temporary directory.
    fn scratch_dir(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "sundog-lens-proc-{}-{name}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Whether a process with this id exists, by signal 0.
    fn process_exists(pid: u32) -> bool {
        use rustix::process::{Pid, test_kill_process};
        Pid::from_raw(i32::try_from(pid).unwrap()).is_some_and(|pid| test_kill_process(pid).is_ok())
    }

    /// Writes an executable shell script and returns its path.
    fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[tokio::test]
    async fn a_node_that_prints_ready_is_started_and_signalled_to_leave() {
        let dir = scratch_dir("ready");
        let node = script(
            &dir,
            "node.sh",
            "echo \"cluster=$1 ip=$SUNDOG_TESTNODE_BIND_IP\" >&2\n\
             echo booting\n\
             echo testnode-ready\n\
             trap 'exit 0' TERM\n\
             while true; do sleep 0.05; done",
        );
        let layout = Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 77));
        let env = node_env(
            layout,
            &SlotInfo::new(layout, 1).unwrap(),
            "127.0.0.77:7946",
            NonZeroU8::new(2).unwrap(),
        );
        let log = log_path(&dir, 1, 1);
        let mut proc = spawn(&node, "lens-test", &env, &log).expect("the node starts");
        assert!(proc.pid > 0);
        assert_eq!(proc.log, log);
        proc.ready_handle()
            .wait(Duration::from_secs(10))
            .await
            .expect("the node becomes ready");
        assert_eq!(proc.readiness(), Readiness::Ready);
        assert!(proc.child.try_wait().unwrap().is_none());
        let written = std::fs::read_to_string(&log).unwrap();
        assert!(
            written.contains("cluster=lens-test ip=127.0.0.77"),
            "{written}"
        );
        terminate(proc.pid).expect("the signal is sent");
        let status = tokio::time::timeout(Duration::from_secs(10), proc.child.wait())
            .await
            .expect("the node exits")
            .unwrap();
        assert!(status.success());
        assert!(terminate(proc.pid).is_err(), "the process is gone");
    }

    #[tokio::test]
    async fn a_node_that_exits_before_it_is_ready_is_an_error_with_its_log() {
        let dir = scratch_dir("early");
        let node = script(
            &dir,
            "node.sh",
            "echo 'bind failed: address in use' >&2\nexit 1",
        );
        let log = log_path(&dir, 2, 1);
        let proc = spawn(&node, "c", &[], &log).unwrap();
        let error = proc
            .ready_handle()
            .wait(Duration::from_secs(10))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("before it was ready"), "{error}");
        assert!(error.contains("bind failed: address in use"), "{error}");
        assert_eq!(proc.readiness(), Readiness::Failed);
    }

    // Linux refuses to run a file a writer holds open; macOS runs it at once,
    // so the start has nothing to wait for there.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_binary_that_a_writer_still_holds_open_starts_once_the_writer_closes_it() {
        use std::os::unix::fs::OpenOptionsExt as _;
        let dir = scratch_dir("busy");
        let node = script(&dir, "node.sh", "echo testnode-ready\nsleep 30");
        // While a writer holds the file, Linux refuses to run it.
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .mode(0o755)
            .open(&node)
            .unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            drop(writer);
        });
        let log = log_path(&dir, 7, 1);
        let started = std::time::Instant::now();
        let proc = spawn(&node, "c", &[], &log).expect("the start waits for the writer");
        assert!(started.elapsed() >= Duration::from_millis(100));
        proc.ready_handle()
            .wait(Duration::from_secs(10))
            .await
            .unwrap();
        release.join().unwrap();
    }

    #[tokio::test]
    async fn a_node_that_is_never_ready_times_out_and_is_killed_with_its_handle() {
        let dir = scratch_dir("slow");
        let node = script(&dir, "node.sh", "echo 'still starting' >&2\nsleep 30");
        let log = log_path(&dir, 3, 1);
        let proc = spawn(&node, "c", &[], &log).unwrap();
        assert_eq!(proc.readiness(), Readiness::Starting);
        let started = std::time::Instant::now();
        let error = proc
            .ready_handle()
            .wait(Duration::from_millis(300))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("was not ready within"), "{error}");
        assert!(error.contains("still starting"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(10));
        let pid = proc.pid;
        drop(proc);
        for _ in 0..100 {
            if !process_exists(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the node outlived its handle");
    }

    #[tokio::test]
    async fn a_node_prints_more_after_ready_without_blocking() {
        let dir = scratch_dir("chatty");
        let node = script(
            &dir,
            "node.sh",
            "echo testnode-ready\n\
             i=0\n\
             while [ $i -lt 20000 ]; do echo \"line $i\"; i=$((i+1)); done\n\
             echo finished >&2",
        );
        let log = log_path(&dir, 4, 1);
        let mut proc = spawn(&node, "c", &[], &log).unwrap();
        proc.ready_handle()
            .wait(Duration::from_secs(10))
            .await
            .unwrap();
        let status = tokio::time::timeout(Duration::from_secs(10), proc.child.wait())
            .await
            .expect("a node that talks is drained, not blocked")
            .unwrap();
        assert!(status.success());
    }

    #[tokio::test]
    async fn a_binary_that_does_not_exist_is_an_error_naming_it() {
        let dir = scratch_dir("missing");
        let log = log_path(&dir, 1, 1);
        let error = spawn(Path::new("/no/such/testnode"), "c", &[], &log).unwrap_err();
        assert!(
            format!("{error:#}").contains("/no/such/testnode"),
            "{error:#}"
        );
    }

    #[test]
    fn a_pid_that_is_not_a_process_is_an_error() {
        assert!(terminate(0).is_err());
        assert!(terminate(u32::MAX).is_err());
    }
}
