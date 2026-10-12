//! Lifecycle tests of `Fleet` and `FleetStage` against a fake test node and real child processes.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use super::*;

/// A node that is ready at once and exits 0 on SIGTERM. The crash test
/// serves its control port separately and stops it there.
fn fake_node(dir: &Path) -> PathBuf {
    let path = dir.join("fake-testnode.sh");
    std::fs::write(
        &path,
        "#!/bin/sh\ntrap 'exit 0' TERM\necho testnode-ready\nwhile true; do sleep 0.05; done\n",
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sundog-lens-fleet-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn fleet(name: &str, base: Ipv4Addr) -> Fleet {
    let dir = scratch(name);
    Fleet::new(FleetConfig {
        cluster: "lens-fleet-test".to_owned(),
        layout: Layout::PerAddress(base),
        testnode: fake_node(&dir),
        owners: NonZeroU8::new(2).unwrap(),
        logs: dir.join("logs"),
    })
}

fn alive(pid: u32) -> bool {
    // Linux's process table; the module runs on Linux only.
    Path::new(&format!("/proc/{pid}")).exists()
}

#[tokio::test]
async fn slots_start_in_order_and_each_stop_leaves_no_process() {
    let mut fleet = fleet("lifecycle", Ipv4Addr::new(127, 0, 0, 81));
    let (n1, ready1) = fleet.spawn_next().unwrap();
    let (n2, ready2) = fleet.spawn_next().unwrap();
    ready1.wait(Duration::from_secs(10)).await.unwrap();
    ready2.wait(Duration::from_secs(10)).await.unwrap();
    assert_eq!((n1.label.as_str(), n2.label.as_str()), ("n1", "n2"));
    assert_eq!(fleet.running().len(), 2);
    assert_eq!(fleet.phase("n1"), Some(Phase::Running));
    let pid1 = fleet.slots[0].node.as_ref().unwrap().pid;
    let pid2 = fleet.slots[1].node.as_ref().unwrap().pid;
    assert!(alive(pid1) && alive(pid2));

    // A restart of a running node is refused.
    assert!(
        fleet
            .restart("n1")
            .await
            .unwrap_err()
            .to_string()
            .contains("still running")
    );

    // SIGKILL ends n1 at once and frees its slot.
    fleet.kill("n1").await.unwrap();
    assert_eq!(fleet.phase("n1"), Some(Phase::Stopped));
    assert!(!alive(pid1));
    assert!(
        fleet
            .kill("n1")
            .await
            .unwrap_err()
            .to_string()
            .contains("not running")
    );

    // SIGTERM is a leave: the node exits by itself.
    fleet.leave("n2").unwrap();
    assert_eq!(fleet.phase("n2"), Some(Phase::Leaving));
    fleet.wait_exit(1, Duration::from_secs(5)).await;
    assert_eq!(fleet.phase("n2"), Some(Phase::Stopped));
    assert!(!alive(pid2));

    // A stopped slot restarts at its address, as a new process.
    let again = fleet.restart("n1").await.unwrap().0;
    assert_eq!(again.gossip, n1.gossip);
    let pid_again = fleet.slots[0].node.as_ref().unwrap().pid;
    assert_ne!(pid_again, pid1);
    assert_eq!(fleet.slots[0].starts, 2);
    assert!(proc::log_path(&fleet.config.logs, 1, 2).exists());

    fleet.stop_all().await;
    let running = fleet.running();
    assert!(running.is_empty(), "{running:?}");
    assert!(!alive(pid_again));
}

#[tokio::test]
async fn stopping_nodes_that_ignore_sigterm_takes_one_grace_period() {
    let dir = scratch("stubborn");
    let path = dir.join("stubborn-testnode.sh");
    std::fs::write(
        &path,
        "#!/bin/sh\ntrap '' TERM\necho testnode-ready\nwhile true; do sleep 0.05; done\n",
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut fleet = Fleet::new(FleetConfig {
        cluster: "c".to_owned(),
        layout: Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 191)),
        testnode: path,
        owners: NonZeroU8::new(2).unwrap(),
        logs: dir.join("logs"),
    });
    let (_, ready1) = fleet.spawn_next().unwrap();
    let (_, ready2) = fleet.spawn_next().unwrap();
    ready1.wait(Duration::from_secs(10)).await.unwrap();
    ready2.wait(Duration::from_secs(10)).await.unwrap();
    let pids: Vec<u32> = fleet
        .slots
        .iter()
        .map(|slot| slot.node.as_ref().unwrap().pid)
        .collect();
    let started = std::time::Instant::now();
    fleet.stop_all().await;
    let took = started.elapsed();
    assert!(took >= STOP_GRACE, "{took:?}");
    assert!(took < STOP_GRACE + Duration::from_secs(2), "{took:?}");
    let running = fleet.running();
    assert!(running.is_empty(), "{running:?}");
    assert!(pids.iter().all(|pid| !alive(*pid)));
}

#[tokio::test]
async fn restarting_a_node_that_is_still_leaving_waits_for_it_to_exit() {
    let mut fleet = fleet("leaving", Ipv4Addr::new(127, 0, 0, 91));
    fleet.spawn_next().unwrap();
    fleet.leave("n1").unwrap();
    let info = fleet.restart("n1").await.unwrap().0;
    assert_eq!(info.label, "n1");
    assert_eq!(fleet.phase("n1"), Some(Phase::Running));
    fleet.stop_all().await;
}

#[tokio::test]
async fn unknown_and_unstarted_labels_are_errors() {
    let mut fleet = fleet("labels", Ipv4Addr::new(127, 0, 0, 101));
    fleet.spawn_next().unwrap();
    for label in ["x", "n0", "n2", "n9"] {
        assert!(fleet.kill(label).await.is_err(), "{label}");
        assert!(fleet.leave(label).is_err(), "{label}");
        assert!(fleet.crash(label).await.is_err(), "{label}");
        assert!(fleet.restart(label).await.is_err(), "{label}");
    }
    fleet.stop_all().await;
}

#[tokio::test]
async fn the_seventh_node_is_refused() {
    let mut fleet = fleet("seventh", Ipv4Addr::new(127, 0, 0, 111));
    for _ in 0..MAX_SLOTS {
        fleet.spawn_next().unwrap();
    }
    let error = fleet.spawn_next().unwrap_err();
    assert!(error.to_string().contains("at most 6"), "{error}");
    assert_eq!(fleet.infos().len(), MAX_SLOTS);
    fleet.stop_all().await;
}

#[tokio::test]
async fn a_node_that_never_becomes_ready_is_reported_by_its_handle_with_its_log() {
    let dir = scratch("failing");
    let path = dir.join("failing.sh");
    std::fs::write(&path, "#!/bin/sh\necho 'no luck' >&2\nexit 1\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut fleet = Fleet::new(FleetConfig {
        cluster: "c".to_owned(),
        layout: Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 121)),
        testnode: path,
        owners: NonZeroU8::new(2).unwrap(),
        logs: dir.join("logs"),
    });
    let (info, ready) = fleet.spawn_next().unwrap();
    assert_eq!(info.label, "n1");
    let error = ready.wait(Duration::from_secs(10)).await.unwrap_err();
    assert!(format!("{error:#}").contains("no luck"), "{error:#}");
}

#[tokio::test]
async fn a_binary_that_cannot_start_leaves_no_slot_behind() {
    let dir = scratch("nobinary");
    let mut fleet = Fleet::new(FleetConfig {
        cluster: "c".to_owned(),
        layout: Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 122)),
        testnode: dir.join("missing"),
        owners: NonZeroU8::new(2).unwrap(),
        logs: dir.join("logs"),
    });
    let error = fleet.spawn_next().unwrap_err();
    assert!(format!("{error:#}").contains("missing"), "{error:#}");
    let infos = fleet.infos();
    assert!(infos.is_empty(), "{infos:?}");
}

#[tokio::test]
async fn dropping_a_fleet_kills_its_nodes() {
    let mut fleet = fleet("dropped", Ipv4Addr::new(127, 0, 0, 131));
    fleet.spawn_next().unwrap();
    let pid = fleet.slots[0].node.as_ref().unwrap().pid;
    assert!(alive(pid));
    drop(fleet);
    for _ in 0..100 {
        if !alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the node outlived its fleet");
}

/// A node that is ready at once and logs the port variables it got.
fn port_echo_node(dir: &Path) -> PathBuf {
    let path = dir.join("port-echo-testnode.sh");
    std::fs::write(
        &path,
        "#!/bin/sh\n\
         echo \"ports ${SUNDOG_TESTNODE_GOSSIP_PORT-unset} ${SUNDOG_TESTNODE_CONTROL_PORT-unset} \
         ${SUNDOG_TESTNODE_METRICS_PORT-unset} ip=$SUNDOG_TESTNODE_BIND_IP \
         seeds=$SUNDOG_SEEDS\" >&2\n\
         trap 'exit 0' TERM\necho testnode-ready\nwhile true; do sleep 0.05; done\n",
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

async fn logged_ports(layout: Layout, name: &str) -> Vec<String> {
    let dir = scratch(name);
    let mut fleet = Fleet::new(FleetConfig {
        cluster: "lens-ports-test".to_owned(),
        layout,
        testnode: port_echo_node(&dir),
        owners: NonZeroU8::new(2).unwrap(),
        logs: dir.join("logs"),
    });
    let (_, ready1) = fleet.spawn_next().unwrap();
    let (_, ready2) = fleet.spawn_next().unwrap();
    ready1.wait(Duration::from_secs(10)).await.unwrap();
    ready2.wait(Duration::from_secs(10)).await.unwrap();
    fleet.stop_all().await;
    (1..=2)
        .map(|slot| {
            std::fs::read_to_string(proc::log_path(&dir.join("logs"), slot, 1))
                .unwrap()
                .trim()
                .to_owned()
        })
        .collect()
}

#[tokio::test]
async fn a_shared_fleet_starts_its_nodes_on_one_address_with_their_own_ports() {
    let logged = logged_ports(Layout::Shared, "shared-env").await;
    assert_eq!(
        logged,
        [
            "ports 7946 8080 9090 ip=127.0.0.1 seeds=127.0.0.1:7946,127.0.0.1:7947",
            "ports 7947 8081 9091 ip=127.0.0.1 seeds=127.0.0.1:7946,127.0.0.1:7947",
        ]
    );
}

#[tokio::test]
async fn a_per_address_fleet_leaves_the_ports_to_the_node() {
    let logged = logged_ports(
        Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 61)),
        "per-address-env",
    )
    .await;
    assert_eq!(
        logged,
        [
            "ports unset unset unset ip=127.0.0.61 seeds=127.0.0.61:7946,127.0.0.62:7946",
            "ports unset unset unset ip=127.0.0.62 seeds=127.0.0.61:7946,127.0.0.62:7946",
        ]
    );
}

#[tokio::test]
async fn the_preflight_names_a_blocked_port() {
    let blocker = TcpListener::bind("127.0.0.141:8080").await;
    let Ok(_blocker) = blocker else {
        // The host does not give this process the address: nothing to test.
        return;
    };
    let fleet = fleet("preflight", Ipv4Addr::new(127, 0, 0, 141));
    let error = fleet.preflight().await.unwrap_err().to_string();
    assert!(error.contains("127.0.0.141:8080"), "{error}");
    assert!(error.contains("blocks the fleet"), "{error}");
}

#[tokio::test]
async fn the_preflight_passes_on_a_free_loopback_address() {
    let fleet = fleet("preflight-ok", Ipv4Addr::new(127, 0, 0, 151));
    fleet.preflight().await.unwrap();
}

#[tokio::test]
async fn a_node_without_an_exporter_fails_the_check_with_the_build_command() {
    let mut fleet = fleet("noexporter", Ipv4Addr::new(127, 0, 0, 161));
    let short = Duration::from_millis(300);
    assert!(fleet.verify_exporter(short).await.is_err(), "no node yet");
    fleet.spawn_next().unwrap();
    let error = fleet.verify_exporter(short).await.unwrap_err().to_string();
    assert!(error.contains(BUILD_HINT), "{error}");
    fleet.stop_all().await;
}

/// Answers every connection on `addr` with `reply` for each line (or,
/// for HTTP, once) until the test ends, and records the lines it reads.
async fn serve(addr: SocketAddr, http: bool) -> Arc<std::sync::Mutex<Vec<String>>> {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    let listener = TcpListener::bind(addr)
        .await
        .expect("the test address binds");
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = Arc::clone(&seen);
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let record = Arc::clone(&record);
            tokio::spawn(async move {
                let (reader, mut writer) = socket.into_split();
                let mut lines = BufReader::new(reader).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if http {
                        if line.is_empty() {
                            let _ = writer
                                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                                .await;
                            return;
                        }
                    } else {
                        record.lock().unwrap().push(line);
                        let _ = writer.write_all(b"ok\n").await;
                    }
                }
            });
        }
    });
    seen
}

fn slow_node(dir: &Path, seconds: &str) -> PathBuf {
    let path = dir.join("slow-testnode.sh");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nsleep {seconds}\ntrap 'exit 0' TERM\necho testnode-ready\nwhile true; do sleep 0.05; done\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// A control server on `addr` that answers `ok` to every line and, for
/// `crash`, then stops the process `crash_pid` names, as a node that
/// crashes does.
async fn serve_control(
    addr: SocketAddr,
    crash_pid: Arc<std::sync::atomic::AtomicU32>,
) -> Arc<std::sync::Mutex<Vec<String>>> {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    let listener = TcpListener::bind(addr)
        .await
        .expect("the test address binds");
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = Arc::clone(&seen);
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let record = Arc::clone(&record);
            let crash_pid = Arc::clone(&crash_pid);
            tokio::spawn(async move {
                let (reader, mut writer) = socket.into_split();
                let mut lines = BufReader::new(reader).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    record.lock().unwrap().push(line.clone());
                    let _ = writer.write_all(b"ok\n").await;
                    if line == "crash" {
                        let pid = crash_pid.load(std::sync::atomic::Ordering::Relaxed);
                        let _ = proc::terminate(pid);
                    }
                }
            });
        }
    });
    seen
}

#[tokio::test]
async fn a_crash_asks_the_node_over_its_control_port_and_waits_for_it_to_go() {
    let dir = scratch("crash");
    let base = Ipv4Addr::new(127, 0, 0, 184);
    let mut fleet = Fleet::new(FleetConfig {
        cluster: "c".to_owned(),
        layout: Layout::PerAddress(base),
        testnode: fake_node(&dir),
        owners: NonZeroU8::new(2).unwrap(),
        logs: dir.join("logs"),
    });
    let (info, ready) = fleet.spawn_next().unwrap();
    ready.wait(Duration::from_secs(10)).await.unwrap();
    let pid = fleet.slots[0].node.as_ref().unwrap().pid;
    let crash_pid = Arc::new(std::sync::atomic::AtomicU32::new(pid));
    let seen = serve_control(info.control, crash_pid).await;
    assert!(alive(pid));
    fleet.crash("n1").await.unwrap();
    assert_eq!(seen.lock().unwrap().as_slice(), ["crash"]);
    assert_eq!(fleet.phase("n1"), Some(Phase::Stopped));
    assert!(!alive(pid));
    assert!(
        fleet.crash("n1").await.is_err(),
        "a stopped node cannot crash"
    );
}

#[tokio::test]
async fn the_director_acts_on_the_stage_through_its_trait() {
    let dir = scratch("trait");
    let base = Ipv4Addr::new(127, 0, 0, 185);
    let n1 = SlotInfo::new(Layout::PerAddress(base), 1).unwrap();
    let _exporter = serve(n1.metrics, true).await;
    let crash_pid = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let control = serve_control(n1.control, Arc::clone(&crash_pid)).await;
    let (load, handle) = load::Load::spawn(10, 10);
    let stage = FleetStage::new(
        Fleet::new(FleetConfig {
            cluster: "c".to_owned(),
            layout: Layout::PerAddress(base),
            testnode: fake_node(&dir),
            owners: NonZeroU8::new(2).unwrap(),
            logs: dir.join("logs"),
        }),
        handle,
    );
    let started = std::time::Instant::now();
    Stage::spawn(&stage, 2, Some(Duration::from_millis(400)))
        .await
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(stage.infos().await.len(), 2);
    stage.wait_ready_all().await.unwrap();

    Stage::fill(&stage, 77).await.unwrap();
    assert_eq!(control.lock().unwrap().as_slice(), ["fill 77"]);
    Stage::load(&stage, true);
    assert!(stage.load.is_running());
    Stage::load(&stage, false);
    assert!(!stage.load.is_running());

    Stage::leave(&stage, "n2").await.unwrap();
    Stage::restart(&stage, "n2").await.unwrap();
    stage.wait_ready("n2").await.unwrap();
    Stage::kill(&stage, "n2").await.unwrap();
    assert!(Stage::kill(&stage, "n2").await.is_err(), "already stopped");

    let pid = stage.fleet.lock().await.slots[0].node.as_ref().unwrap().pid;
    crash_pid.store(pid, std::sync::atomic::Ordering::Relaxed);
    Stage::crash(&stage, "n1").await.unwrap();
    assert!(!alive(pid));
    stage.stop_all().await;
    load.shutdown();
}

/// A control server on `addr` that answers every line with `reply` and
/// records the lines it reads.
async fn serve_replying(
    addr: SocketAddr,
    reply: &'static str,
) -> Arc<std::sync::Mutex<Vec<String>>> {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    let listener = TcpListener::bind(addr)
        .await
        .expect("the test address binds");
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = Arc::clone(&seen);
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let record = Arc::clone(&record);
            tokio::spawn(async move {
                let (reader, mut writer) = socket.into_split();
                let mut lines = BufReader::new(reader).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    record.lock().unwrap().push(line);
                    let _ = writer.write_all(format!("{reply}\n").as_bytes()).await;
                }
            });
        }
    });
    seen
}

#[tokio::test]
async fn an_explain_asks_every_running_node_over_its_control_port() {
    let dir = scratch("explain-stage");
    let layout = Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 186));
    let n1 = SlotInfo::new(layout, 1).unwrap();
    let n2 = SlotInfo::new(layout, 2).unwrap();
    let _exporter = serve(n1.metrics, true).await;
    let first = serve_replying(
        n1.control,
        include_str!("../../../tests/fixtures/explain/owner.json").trim(),
    )
    .await;
    let second = serve_replying(
        n2.control,
        include_str!("../../../tests/fixtures/explain/non_owner.json").trim(),
    )
    .await;
    let (load, handle) = load::Load::spawn(10, 10);
    let stage = FleetStage::new(
        Fleet::new(FleetConfig {
            cluster: "c".to_owned(),
            layout,
            testnode: fake_node(&dir),
            owners: NonZeroU8::new(2).unwrap(),
            logs: dir.join("logs"),
        }),
        handle,
    );
    let error = Stage::explain(&stage, "k1").await.unwrap_err();
    assert_eq!(error.to_string(), "no node is running to ask");

    Stage::spawn(&stage, 2, None).await.unwrap();
    let answers = Stage::explain(&stage, "k1").await.unwrap();
    assert_eq!(
        (answers.id, answers.key.as_str()),
        (crate::ask::SCRIPTED_ID, "k1")
    );
    let nodes: Vec<(&str, &str)> = answers
        .readings()
        .map(|(label, reading)| (label.as_str(), reading.node.as_str()))
        .collect();
    assert_eq!(
        nodes,
        [("n1", "6f3ac1e29d54b807"), ("n2", "1b88e5d0a7c64f92")]
    );
    assert_eq!(first.lock().unwrap().as_slice(), ["explain k1"]);
    assert_eq!(second.lock().unwrap().as_slice(), ["explain k1"]);

    // A node that no longer runs is not asked.
    Stage::kill(&stage, "n1").await.unwrap();
    let answers = stage.explain("k2").await.unwrap();
    let labels: Vec<&str> = answers.nodes.iter().map(|a| a.label.as_str()).collect();
    assert_eq!(labels, ["n2"]);
    assert_eq!(first.lock().unwrap().as_slice(), ["explain k1"]);
    assert_eq!(
        second.lock().unwrap().as_slice(),
        ["explain k1", "explain k2"]
    );

    stage.kill("n2").await.unwrap();
    assert!(stage.explain("k3").await.is_err(), "no node runs to ask");
    stage.stop_all().await;
    load.shutdown();
}

#[tokio::test]
async fn spawning_returns_before_the_node_is_ready_and_a_fill_waits_for_it() {
    let dir = scratch("readiness");
    let base = Ipv4Addr::new(127, 0, 0, 181);
    let info = SlotInfo::new(Layout::PerAddress(base), 1).unwrap();
    let _exporter = serve(info.metrics, true).await;
    let control = serve(info.control, false).await;
    let (load, handle) = load::Load::spawn(10, 10);
    let stage = FleetStage::new(
        Fleet::new(FleetConfig {
            cluster: "c".to_owned(),
            layout: Layout::PerAddress(base),
            testnode: slow_node(&dir, "1.5"),
            owners: NonZeroU8::new(2).unwrap(),
            logs: dir.join("logs"),
        }),
        handle,
    );
    let started = std::time::Instant::now();
    let spawned = stage.spawn_many(1, None).await.unwrap();
    assert_eq!(spawned[0].label, "n1");
    assert!(
        started.elapsed() < Duration::from_millis(1300),
        "{:?}",
        started.elapsed()
    );

    stage.fill(25).await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(control.lock().unwrap().as_slice(), ["fill 25"]);
    stage.wait_ready("n1").await.unwrap();
    stage.wait_ready_all().await.unwrap();
    assert!(stage.wait_ready("n9").await.is_err());
    let failures = stage.startup_failures();
    assert!(failures.is_empty(), "{failures:?}");
    stage.stop_all().await;
    load.shutdown();
}

#[tokio::test]
async fn a_node_that_never_becomes_ready_is_listed_and_fails_the_waits() {
    let dir = scratch("unready");
    let base = Ipv4Addr::new(127, 0, 0, 182);
    let info = SlotInfo::new(Layout::PerAddress(base), 1).unwrap();
    let _exporter = serve(info.metrics, true).await;
    let path = dir.join("failing.sh");
    std::fs::write(&path, "#!/bin/sh\necho 'no luck' >&2\nexit 1\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (load, handle) = load::Load::spawn(10, 10);
    let stage = FleetStage::new(
        Fleet::new(FleetConfig {
            cluster: "c".to_owned(),
            layout: Layout::PerAddress(base),
            testnode: path,
            owners: NonZeroU8::new(2).unwrap(),
            logs: dir.join("logs"),
        }),
        handle,
    );
    stage.spawn_one().await.unwrap();
    let error = stage.wait_ready_all().await.unwrap_err();
    assert!(format!("{error:#}").contains("no luck"), "{error:#}");
    assert!(stage.fill(1).await.is_err());
    for _ in 0..100 {
        if !stage.startup_failures().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let failures = stage.startup_failures();
    assert_eq!(failures.len(), 1);
    assert!(
        failures[0].starts_with("n1: ") && failures[0].contains("no luck"),
        "{failures:?}"
    );
    stage.stop_all().await;
    load.shutdown();
}

#[tokio::test]
async fn a_restart_is_tracked_again_and_a_stopped_node_is_refused_a_crash() {
    let dir = scratch("restart-track");
    let base = Ipv4Addr::new(127, 0, 0, 183);
    let info = SlotInfo::new(Layout::PerAddress(base), 1).unwrap();
    let _exporter = serve(info.metrics, true).await;
    let (load, handle) = load::Load::spawn(10, 10);
    let stage = FleetStage::new(
        Fleet::new(FleetConfig {
            cluster: "c".to_owned(),
            layout: Layout::PerAddress(base),
            testnode: fake_node(&dir),
            owners: NonZeroU8::new(2).unwrap(),
            logs: dir.join("logs"),
        }),
        handle,
    );
    stage.apply(FleetCmd::Spawn).await.unwrap();
    stage.wait_ready("n1").await.unwrap();
    stage.apply(FleetCmd::Leave("n1".into())).await.unwrap();
    stage.apply(FleetCmd::Restart("n1".into())).await.unwrap();
    stage.wait_ready("n1").await.unwrap();
    assert_eq!(stage.infos().await.len(), 1);
    stage.apply(FleetCmd::Kill("n1".into())).await.unwrap();
    assert!(
        stage.crash("n1").await.is_err(),
        "a stopped node cannot crash"
    );
    assert!(
        stage.fill(10).await.is_err(),
        "no node runs to fill through"
    );
    assert!(stage.apply(FleetCmd::Kill("n7".into())).await.is_err());
    assert!(stage.apply(FleetCmd::Leave("n7".into())).await.is_err());
    assert!(stage.apply(FleetCmd::Restart("n7".into())).await.is_err());

    stage.set_load(true);
    assert!(stage.load.is_running());
    stage.set_load(false);
    assert!(!stage.load.is_running());
    stage.stop_all().await;
    load.shutdown();
}

#[tokio::test]
async fn an_explain_request_is_not_a_fleet_action() {
    let dir = scratch("explain-not-applied");
    let (load, handle) = load::Load::spawn(10, 10);
    let stage = FleetStage::new(
        Fleet::new(FleetConfig {
            cluster: "c".to_owned(),
            layout: Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 185)),
            testnode: fake_node(&dir),
            owners: NonZeroU8::new(2).unwrap(),
            logs: dir.join("logs"),
        }),
        handle,
    );
    let request = crate::app::ExplainRequest {
        id: 1,
        key: "k1".to_owned(),
        targets: Vec::new(),
    };
    let error = stage
        .apply(FleetCmd::Explain(request))
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        "an explain request is answered by ask::answer, not applied"
    );
    assert!(stage.infos().await.is_empty(), "no node was started");
    stage.stop_all().await;
    load.shutdown();
}

#[tokio::test]
async fn without_an_exporter_the_first_spawn_fails_with_the_build_command() {
    let dir = scratch("stage-noexporter");
    let fleet = Fleet::new(FleetConfig {
        cluster: "c".to_owned(),
        layout: Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 171)),
        testnode: fake_node(&dir),
        owners: NonZeroU8::new(2).unwrap(),
        logs: dir.join("logs"),
    });
    let (load, handle) = load::Load::spawn(10, 10);
    let stage = FleetStage::new(fleet, handle).with_exporter_limit(Duration::from_millis(300));
    let error = stage.spawn_one().await.unwrap_err();
    assert!(error.to_string().contains(BUILD_HINT), "{error}");
    // The node that started is still the fleet's to stop.
    assert_eq!(stage.infos().await.len(), 1);
    stage.stop_all().await;
    load.shutdown();
}
