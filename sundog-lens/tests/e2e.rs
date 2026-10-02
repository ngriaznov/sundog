//! End to end with real processes: the demo's headless run and the
//! `cluster` command watched by `watch --once --json`.
//!
//! Every test starts `sundog-testnode` processes on `127.0.0.11` and up, and
//! the nodes' ports are fixed, so the tests run one at a time and are
//! `#[ignore]`d. They need a test node built with the exporter, in the
//! profile the tests run in:
//!
//! ```text
//! cargo build -p sundog-testnode --features prometheus
//! cargo test -p sundog-lens --test e2e -- --ignored --test-threads=1
//! ```
//!
//! The tests use `$SUNDOG_TESTNODE` when it is set, else the `sundog-testnode`
//! beside the `sundog-lens` binary, else the workspace's release build, and
//! pass the path to every lens they start with `--testnode`. The nodes are
//! stopped when a test ends, whichever way it ends.

#![cfg(unix)]

use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sundog_lens::fleet::proc::terminate;

const LENS: &str = env!("CARGO_BIN_EXE_sundog-lens");

/// The tests share fixed ports: one at a time.
static PORTS: Mutex<()> = Mutex::new(());

/// A fresh scratch directory for one test.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sundog-lens-e2e-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("the scratch directory is created");
    dir
}

/// The pids of the test nodes of cluster `cluster`, from the process table.
#[cfg(target_os = "linux")]
fn testnode_pids(cluster: &str) -> Vec<u32> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let pid: u32 = entry.file_name().to_str()?.parse().ok()?;
            let cmdline = std::fs::read(entry.path().join("cmdline")).ok()?;
            let words: Vec<&[u8]> = cmdline.split(|&b| b == 0).collect();
            let binary = words.first()?;
            let is_node = binary.ends_with(b"sundog-testnode");
            (is_node && words.get(1) == Some(&cluster.as_bytes())).then_some(pid)
        })
        .collect()
}

#[cfg(not(target_os = "linux"))]
fn testnode_pids(_cluster: &str) -> Vec<u32> {
    Vec::new()
}

/// Kills every test node of `cluster` and waits until none is left.
fn sweep(cluster: &str) {
    for pid in testnode_pids(cluster) {
        let _ = terminate(pid);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while !testnode_pids(cluster).is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    for pid in testnode_pids(cluster) {
        let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
    }
}

/// Stops a lens process and the nodes of its cluster when dropped.
struct Guard {
    child: Child,
    cluster: &'static str,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = terminate(self.child.id());
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        sweep(self.cluster);
    }
}

/// The places the test node is looked for, in order.
fn testnode_candidates() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    paths.extend(std::env::var_os("SUNDOG_TESTNODE").map(PathBuf::from));
    paths.extend(
        Path::new(LENS)
            .parent()
            .map(|dir| dir.join("sundog-testnode")),
    );
    paths.push(Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/release/sundog-testnode"));
    paths
}

/// The test node every test starts, or a clear failure.
fn require_testnode() -> PathBuf {
    let candidates = testnode_candidates();
    candidates
        .iter()
        .find(|path| path.is_file())
        .cloned()
        .unwrap_or_else(|| {
            panic!(
                "no sundog-testnode found (looked at {candidates:?}); build one with \
                 `cargo build -p sundog-testnode --features prometheus` or set SUNDOG_TESTNODE"
            )
        })
}

/// Sends SIGINT to `child`, as Ctrl-C at a terminal does.
#[cfg(target_os = "linux")]
fn interrupt(child: &Child) {
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("kill runs");
    assert!(status.success(), "SIGINT is sent");
}

/// Waits for `child` to exit, killing it past `limit`. Returns the exit code
/// and the elapsed time.
fn wait_within(child: &mut Child, limit: Duration) -> (Option<i32>, Duration) {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("the child can be polled") {
            return (status.code(), started.elapsed());
        }
        if started.elapsed() > limit {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the process did not exit within {limit:?}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Runs the demo headless on the scenario `text` for cluster `cluster` and
/// returns its exit code, its standard output and how long it took.
fn demo_headless(
    testnode: &Path,
    cluster: &'static str,
    text: &str,
    limit: Duration,
) -> (Option<i32>, String, Duration) {
    let dir = scratch(cluster);
    let scenario = dir.join("scenario.txt");
    std::fs::write(&scenario, text).expect("the scenario is written");
    let marks = dir.join("marks.txt");
    let mut child = Command::new(LENS)
        .args(["demo", "--headless", "--name", cluster, "--scenario"])
        .arg(&scenario)
        .arg("--marks")
        .arg(&marks)
        .arg("--logs")
        .arg(dir.join("logs"))
        .arg("--testnode")
        .arg(testnode)
        .args(["--rate", "300"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the lens starts");
    let stdout = child.stdout.take().expect("a stdout pipe");
    let reader = std::thread::spawn(move || {
        BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
            .collect::<Vec<_>>()
            .join("\n")
    });
    let (code, took) = wait_within(&mut child, limit);
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = std::io::Read::read_to_string(&mut pipe, &mut stderr);
    }
    let stdout = reader.join().expect("the reader finishes");
    let marked = std::fs::read_to_string(&marks).unwrap_or_default();
    println!("--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}\n--- marks ---\n{marked}");
    (code, format!("{stdout}\n{stderr}"), took)
}

#[test]
#[ignore = "starts sundog-testnode processes on 127.0.0.11 and up"]
fn the_demo_runs_an_inline_scenario_headless_and_leaves_no_node_behind() {
    let _ports = PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let testnode = require_testnode();
    let cluster = "lens-e2e-demo";
    let (code, output, took) = demo_headless(
        &testnode,
        cluster,
        "spawn 3 stagger 1s\n\
         await members 3 within 30s\n\
         fill 2000\n\
         load start\n\
         pause 2s\n\
         kill n3\n\
         await down n3 within 20s\n\
         await settled it within 30s\n\
         quit\n",
        Duration::from_secs(120),
    );
    assert_eq!(code, Some(0), "the run passes:\n{output}");
    assert!(took < Duration::from_secs(95), "{took:?}");
    for want in [
        "JOIN     n1",
        "JOIN     n3",
        "DOWN     n3",
        "no departure seen",
        "VIEW     it",
        "SETTLED  it",
        "scenario done: 9 steps, 0 timeouts, 0 failures",
        "headless run passed",
    ] {
        assert!(output.contains(want), "missing {want:?}:\n{output}");
    }
    assert!(
        !output.contains("LEFT     n3"),
        "a kill is a crash, not a departure:\n{output}"
    );
    assert!(testnode_pids(cluster).is_empty(), "every node is stopped");
}

#[test]
#[ignore = "starts sundog-testnode processes on 127.0.0.11 and up"]
fn a_headless_await_that_times_out_exits_one_and_stops_the_nodes() {
    let _ports = PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let testnode = require_testnode();
    let cluster = "lens-e2e-timeout";
    let (code, output, _) = demo_headless(
        &testnode,
        cluster,
        "spawn 2 stagger 1s\nawait members 5 within 4s\nquit\n",
        Duration::from_secs(120),
    );
    assert_eq!(code, Some(1), "{output}");
    assert!(output.contains("timed out"), "{output}");
    assert!(output.contains("line 2"), "{output}");
    assert!(testnode_pids(cluster).is_empty(), "every node is stopped");
}

#[test]
#[ignore = "starts sundog-testnode processes on 127.0.0.11 and up"]
fn watch_once_json_reports_the_cluster_command_s_nodes_and_ownership() {
    let _ports = PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let testnode = require_testnode();
    let cluster = "lens-e2e-cluster";
    let dir = scratch(cluster);
    let mut child = Command::new(LENS)
        .args([
            "cluster", "--name", cluster, "--nodes", "3", "--keys", "2000", "--rate", "300",
        ])
        .arg("--logs")
        .arg(dir.join("logs"))
        .arg("--testnode")
        .arg(&testnode)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("the cluster command starts");
    let stdout = child.stdout.take().expect("a stdout pipe");
    let guard = Guard { child, cluster };

    // The command prints the line that watches it once the nodes are ready.
    let (lines_tx, lines_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut printed = Vec::new();
    let watch_line = loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = lines_rx
            .recv_timeout(left)
            .unwrap_or_else(|_| panic!("the cluster command printed no watch line:\n{printed:#?}"));
        printed.push(line.clone());
        if line.starts_with("sundog-lens watch ") {
            break line;
        }
    };
    assert_eq!(
        watch_line,
        format!(
            "sundog-lens watch {cluster} --seed 127.0.0.11:7946 --metrics 'http://{{ip}}:9090/metrics'"
        )
    );

    let report = Command::new(LENS)
        .args(["watch", cluster, "--seed", "127.0.0.11:7946"])
        .args(["--metrics", "http://{ip}:9090/metrics", "--once", "--json"])
        .output()
        .expect("the watch command runs");
    assert!(
        report.status.success(),
        "watch --once failed: {}",
        String::from_utf8_lossy(&report.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&report.stdout).expect("the report is one JSON object");
    assert_eq!(json["cluster"], cluster);
    assert_eq!(json["live"], 3, "{json:#}");
    assert_eq!(json["departing"], 0);
    assert_eq!(json["down"], 0);
    let members = json["members"].as_array().expect("members is a list");
    assert_eq!(members.len(), 3);
    for member in members {
        assert_eq!(member["status"], "live", "{member}");
        assert_eq!(member["caches"]["it"], "distributed:2", "{member}");
        assert_eq!(member["exporter"]["live_peers"], 2.0, "{member}");
    }
    let it = json["caches"]
        .as_array()
        .expect("caches is a list")
        .iter()
        .find(|cache| cache["name"] == "it")
        .expect("the it cache is reported");
    let ownership = &it["ownership"];
    assert_eq!(ownership["eligible"], 3);
    assert_eq!(ownership["parts_total"], 131_072);
    assert_eq!(
        ownership["agree"], 3,
        "every node reports what is computed: {ownership:#}"
    );
    assert_eq!(ownership["reporting"], 3);
    let shares: usize = ownership["shares"]
        .as_array()
        .expect("shares is a list")
        .iter()
        .map(|share| usize::try_from(share["parts"].as_u64().unwrap()).unwrap())
        .sum();
    assert_eq!(shares, 131_072);

    drop(guard);
    assert!(testnode_pids(cluster).is_empty(), "every node is stopped");
}

/// Waits until `count` test nodes of `cluster` run, or panics after `limit`.
#[cfg(target_os = "linux")]
fn wait_for_nodes(cluster: &str, count: usize, limit: Duration) {
    let deadline = Instant::now() + limit;
    while testnode_pids(cluster).len() < count {
        assert!(
            Instant::now() < deadline,
            "{count} nodes of {cluster} did not start within {limit:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
#[ignore = "starts sundog-testnode processes on 127.0.0.11 and up"]
#[cfg(target_os = "linux")]
fn a_sigint_to_a_headless_demo_stops_its_nodes() {
    let _ports = PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let testnode = require_testnode();
    let cluster = "lens-e2e-sigint-demo";
    let dir = scratch(cluster);
    let scenario = dir.join("scenario.txt");
    std::fs::write(
        &scenario,
        "spawn 3 stagger 1s\nawait members 3 within 30s\npause 120s\nquit\n",
    )
    .expect("the scenario is written");
    let child = Command::new(LENS)
        .args(["demo", "--headless", "--name", cluster, "--scenario"])
        .arg(&scenario)
        .arg("--logs")
        .arg(dir.join("logs"))
        .arg("--testnode")
        .arg(&testnode)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the lens starts");
    let mut guard = Guard { child, cluster };
    wait_for_nodes(cluster, 3, Duration::from_secs(60));
    interrupt(&guard.child);
    let (code, took) = wait_within(&mut guard.child, Duration::from_secs(30));
    assert_eq!(code, Some(1), "a handled signal ends the run with an error");
    assert!(took < Duration::from_secs(30), "{took:?}");
    assert!(testnode_pids(cluster).is_empty(), "every node is stopped");
}

/// Starts `command` with its stdout piped, reads one line of it, closes the
/// pipe and waits for the process to exit. Returns the exit code and what it
/// wrote to stderr.
#[cfg(target_os = "linux")]
fn run_with_a_reader_that_leaves(
    mut command: Command,
    cluster: &'static str,
    wanted_nodes: usize,
) -> (Option<i32>, String) {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the lens starts");
    let stdout = child.stdout.take().expect("a stdout pipe");
    let stderr = child.stderr.take().expect("a stderr pipe");
    let errors = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = std::io::Read::read_to_string(&mut BufReader::new(stderr), &mut text);
        text
    });
    let mut guard = Guard { child, cluster };
    wait_for_nodes(cluster, wanted_nodes, Duration::from_secs(60));
    let mut first = String::new();
    BufReader::new(stdout)
        .read_line(&mut first)
        .expect("a first line arrives");
    assert!(!first.is_empty(), "the lens wrote a line");
    // The reader is gone: the next write to stdout fails.
    let (code, took) = wait_within(&mut guard.child, Duration::from_secs(60));
    assert!(took < Duration::from_secs(60), "{took:?}");
    let stderr = errors.join().expect("the stderr reader finishes");
    (code, stderr)
}

#[test]
#[ignore = "starts sundog-testnode processes on 127.0.0.11 and up"]
#[cfg(target_os = "linux")]
fn a_headless_demo_whose_reader_leaves_stops_its_nodes_without_a_panic() {
    let _ports = PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let testnode = require_testnode();
    let cluster = "lens-e2e-pipe-demo";
    let dir = scratch(cluster);
    let scenario = dir.join("scenario.txt");
    std::fs::write(
        &scenario,
        "spawn 3 stagger 1s\nawait members 3 within 30s\npause 120s\nquit\n",
    )
    .expect("the scenario is written");
    let mut command = Command::new(LENS);
    command
        .args(["demo", "--headless", "--name", cluster, "--scenario"])
        .arg(&scenario)
        .arg("--logs")
        .arg(dir.join("logs"))
        .arg("--testnode")
        .arg(&testnode);
    let (code, stderr) = run_with_a_reader_that_leaves(command, cluster, 1);
    assert_ne!(code, Some(101), "no panic:\n{stderr}");
    assert_eq!(
        code,
        Some(1),
        "a closed log ends the run with an error:\n{stderr}"
    );
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(stderr.contains("stopping the nodes"), "{stderr}");
    assert!(testnode_pids(cluster).is_empty(), "every node is stopped");
}

#[test]
#[ignore = "starts sundog-testnode processes on 127.0.0.11 and up"]
#[cfg(target_os = "linux")]
fn a_cluster_command_whose_reader_leaves_stops_its_nodes_without_a_panic() {
    let _ports = PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let testnode = require_testnode();
    let cluster = "lens-e2e-pipe-cluster";
    let dir = scratch(cluster);
    let mut command = Command::new(LENS);
    command
        .args([
            "cluster", "--name", cluster, "--nodes", "3", "--keys", "2000",
        ])
        .arg("--logs")
        .arg(dir.join("logs"))
        .arg("--testnode")
        .arg(&testnode);
    let (code, stderr) = run_with_a_reader_that_leaves(command, cluster, 3);
    assert_ne!(code, Some(101), "no panic:\n{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(testnode_pids(cluster).is_empty(), "every node is stopped");
}

#[test]
#[ignore = "starts sundog-testnode processes on 127.0.0.11 and up"]
#[cfg(target_os = "linux")]
fn a_sigint_during_the_cluster_startup_stops_the_nodes() {
    let _ports = PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let testnode = require_testnode();
    let cluster = "lens-e2e-sigint-cluster";
    let dir = scratch(cluster);
    let child = Command::new(LENS)
        .args([
            "cluster", "--name", cluster, "--nodes", "3", "--keys", "2000",
        ])
        .arg("--logs")
        .arg(dir.join("logs"))
        .arg("--testnode")
        .arg(&testnode)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the cluster command starts");
    let mut guard = Guard { child, cluster };
    // The nodes run, but have not all opened their caches: the fill that ends
    // the startup is still ahead.
    wait_for_nodes(cluster, 2, Duration::from_secs(30));
    interrupt(&guard.child);
    let (code, took) = wait_within(&mut guard.child, Duration::from_secs(30));
    assert_eq!(code, Some(0), "a handled signal is a clean stop");
    assert!(took < Duration::from_secs(30), "{took:?}");
    assert!(testnode_pids(cluster).is_empty(), "every node is stopped");
}

#[test]
#[ignore = "binds 127.0.0.11 and runs the interface in a pseudo-terminal"]
#[cfg(target_os = "linux")]
fn the_interface_demo_logs_a_timed_out_await_to_the_log_file() {
    let _ports = PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let testnode = require_testnode();
    let cluster = "lens-e2e-uilog";
    let dir = scratch(cluster);
    let scenario = dir.join("scenario.txt");
    // No node is ever started, so the await cannot pass.
    std::fs::write(&scenario, "await members 9 within 1s\npause 1s\nquit\n")
        .expect("the scenario is written");
    let log = dir.join("lens.log");
    // `script` gives the interface a terminal; `stty` gives it a size.
    let command = format!(
        "stty cols 140 rows 40; exec {LENS} demo --name {cluster} --scenario {} --log {} \
         --logs {} --testnode {} --no-anim",
        scenario.display(),
        log.display(),
        dir.join("logs").display(),
        testnode.display(),
    );
    let child = Command::new("script")
        .args(["-qec", &command, "/dev/null"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("script starts");
    let mut guard = Guard { child, cluster };
    let (code, took) = wait_within(&mut guard.child, Duration::from_secs(60));
    assert_eq!(code, Some(0), "the interface ends at the scenario's quit");
    assert!(took < Duration::from_secs(30), "{took:?}");
    let logged = std::fs::read_to_string(&log).expect("the log file is written");
    println!("--- log ---\n{logged}");
    assert!(logged.contains("await timed out after 1."), "{logged}");
    assert!(logged.contains("await members 9 within 1s"), "{logged}");
    assert!(testnode_pids(cluster).is_empty(), "no node was started");
}
