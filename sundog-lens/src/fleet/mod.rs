//! The local fleet of `sundog-testnode` processes.
//!
//! A fleet starts one test node per slot. A node's ports are fixed unless
//! the environment overrides them (gossip 7946, control 8080, exporter 9090),
//! so the [`Layout`] decides how the slots tell apart. In the per-address
//! layout each slot has its own loopback address (`127.0.0.11` for `n1`,
//! `127.0.0.12` for `n2`, and so on) and the fixed ports; Linux answers on all
//! of `127.0.0.0/8`, so that is its default. In the shared layout every slot
//! is on `127.0.0.1` and slot `i` has the fixed ports plus `i - 1`, which the
//! fleet passes through the node's port variables; that is the default
//! everywhere else, macOS included, and needs no setup. An explicit
//! `--base-ip` always picks the per-address layout. A fleet can
//! stop a node three ways: [`Fleet::leave`] sends SIGTERM and the node
//! gossips a graceful departure; [`Fleet::kill`] sends SIGKILL; and
//! [`Fleet::crash`] asks the node to exit without leaving. [`Fleet::restart`]
//! starts a stopped slot again at its address and ports, with a new identity. No
//! process outlives the fleet: [`Fleet::stop_all`] ends every node, and
//! dropping the fleet kills any that remain.
//!
//! [`FleetStage`] puts a fleet and a [load driver](load) behind the scenario
//! director's [`Stage`], and [`cluster_cmd`] is the `cluster` subcommand.

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU8;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use smol_str::SmolStr;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Mutex;

use crate::app::FleetCmd;
use crate::cli::FleetArgs;
use crate::scenario::director::Stage;
use crate::source::http;
use load::LoadHandle;
use proc::{
    BUILD_HINT, CONTROL_PORT, METRICS_OFFSET, METRICS_PORT, NodeProc, READY_TIMEOUT, ReadyHandle,
    parse_label, slot_label,
};

pub mod layout;
pub mod load;
pub mod proc;

/// The control client lives at the crate root, where every platform builds
/// it; this path keeps `fleet::control` resolving.
pub use crate::control;
pub use layout::{Layout, Ports};

/// How many nodes a fleet runs at most: one per color the interface has.
pub const MAX_SLOTS: usize = 6;

/// How long a node has to exit after SIGTERM before it is killed.
pub const STOP_GRACE: Duration = Duration::from_secs(5);

/// How long the first node's exporter has to answer `/healthz`, counted from
/// the start of the process: the exporter binds as soon as the cluster is
/// built, a second or so in.
pub const HEALTHZ_TIMEOUT: Duration = Duration::from_secs(5);

/// The time between `/healthz` probes.
const HEALTHZ_EVERY: Duration = Duration::from_millis(100);

/// How long a `fill` command may take.
pub const FILL_TIMEOUT: Duration = Duration::from_secs(60);

/// The directory node logs go to when `--logs` is not given.
pub const DEFAULT_LOGS: &str = "target/lens-demo";

/// What a fleet is made of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetConfig {
    /// The cluster name every node joins.
    pub cluster: String,
    /// How the slots share the loopback addresses and the ports.
    pub layout: Layout,
    /// The `sundog-testnode` binary.
    pub testnode: PathBuf,
    /// Owners per part.
    pub owners: NonZeroU8,
    /// Where node logs go.
    pub logs: PathBuf,
}

impl FleetConfig {
    /// The configuration the fleet flags ask for: the layout is
    /// [`Layout::for_host`] of `--base-ip`, and the test node binary is found
    /// as [`proc::find_testnode`] does.
    ///
    /// # Errors
    ///
    /// Returns an error when no test node binary is found.
    pub fn from_args(args: &FleetArgs) -> anyhow::Result<Self> {
        Ok(Self {
            cluster: args.name.clone(),
            layout: Layout::for_host(args.base_ip),
            testnode: proc::find_testnode(args.testnode.as_deref())?,
            owners: args.owners,
            logs: args
                .logs
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_LOGS)),
        })
    }

    /// The gossip addresses the observer joins through: the first two slots.
    #[must_use]
    pub fn seed_addrs(&self) -> Vec<SocketAddr> {
        self.layout.seeds()
    }
}

/// The addresses of one slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotInfo {
    /// The slot's number, from 1.
    pub slot: usize,
    /// The slot's label: `n1`.
    pub label: SmolStr,
    /// The slot's loopback address; every slot's in the shared layout.
    pub ip: Ipv4Addr,
    /// The gossip address.
    pub gossip: SocketAddr,
    /// The control address.
    pub control: SocketAddr,
    /// The exporter address.
    pub metrics: SocketAddr,
}

impl SlotInfo {
    /// The slot `slot` (1-based) of a fleet laid out as `layout`. `None` for
    /// slot 0 and when the layout has no address or port left for the slot.
    #[must_use]
    pub fn new(layout: Layout, slot: usize) -> Option<Self> {
        let ip = layout.ip(slot)?;
        let ports = layout.ports(slot)?;
        Some(Self {
            slot,
            label: slot_label(slot),
            ip,
            gossip: SocketAddr::from((ip, ports.gossip)),
            control: SocketAddr::from((ip, ports.control)),
            metrics: SocketAddr::from((ip, ports.metrics)),
        })
    }
}

/// Where a slot's process is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Started and not asked to stop.
    Running,
    /// Sent SIGTERM and not yet gone.
    Leaving,
    /// No process.
    Stopped,
}

#[derive(Debug)]
struct Slot {
    info: SlotInfo,
    starts: u32,
    node: Option<NodeProc>,
    phase: Phase,
}

/// The nodes of a local cluster.
#[derive(Debug)]
pub struct Fleet {
    config: FleetConfig,
    slots: Vec<Slot>,
}

impl Fleet {
    /// A fleet with no node started.
    #[must_use]
    pub const fn new(config: FleetConfig) -> Self {
        Self {
            config,
            slots: Vec::new(),
        }
    }

    /// The configuration.
    #[must_use]
    pub const fn config(&self) -> &FleetConfig {
        &self.config
    }

    /// Checks that the machine can run the fleet. In the per-address layout
    /// the loopback range answers on the first address and nothing holds the
    /// control or exporter port there. In the shared layout nothing holds
    /// any gossip, control or exporter port of the [`MAX_SLOTS`] slots on
    /// `127.0.0.1`.
    ///
    /// # Errors
    ///
    /// Returns an error that says what to change.
    pub async fn preflight(&self) -> anyhow::Result<()> {
        match self.config.layout {
            Layout::PerAddress(base) => {
                UdpSocket::bind((base, 0))
                    .await
                    .map_err(|error| anyhow!(address_error(base, &error)))?;
                for port in [CONTROL_PORT, METRICS_PORT] {
                    let addr = SocketAddr::from((base, port));
                    TcpListener::bind(addr).await.map_err(|error| {
                        anyhow!(port_error(self.config.layout, addr, "", &error))
                    })?;
                }
            }
            Layout::Shared => {
                for slot in 1..=MAX_SLOTS {
                    let info = SlotInfo::new(self.config.layout, slot)
                        .context("the shared layout has no ports left for another node")?;
                    UdpSocket::bind(info.gossip).await.map_err(|error| {
                        anyhow!(port_error(
                            self.config.layout,
                            info.gossip,
                            "gossip",
                            &error
                        ))
                    })?;
                    for (addr, role) in [(info.control, "control"), (info.metrics, "exporter")] {
                        TcpListener::bind(addr).await.map_err(|error| {
                            anyhow!(port_error(self.config.layout, addr, role, &error))
                        })?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Checks that the first node serves its exporter, which a node built
    /// without the `prometheus` feature does not. The probe repeats until
    /// the exporter answers or `limit` passes ([`HEALTHZ_TIMEOUT`] by
    /// default, see [`FleetStage::with_exporter_limit`]).
    ///
    /// # Errors
    ///
    /// Returns an error that prints the build command when `/healthz` does
    /// not answer `200` in time.
    pub async fn verify_exporter(&self, limit: Duration) -> anyhow::Result<()> {
        let slot = self.slots.first().context("no node is running")?;
        let url = format!("http://{}/healthz", slot.info.metrics);
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            let outcome = http::get(&url, limit).await;
            if matches!(outcome, Ok((200, _))) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "the node serves no exporter at {url} ({outcome:?}); rebuild it with \
                     `{BUILD_HINT}`"
                );
            }
            tokio::time::sleep(HEALTHZ_EVERY).await;
        }
    }

    /// The slots that have been started, in order.
    #[must_use]
    pub fn infos(&self) -> Vec<SlotInfo> {
        self.slots.iter().map(|slot| slot.info.clone()).collect()
    }

    /// The slots whose process runs.
    #[must_use]
    pub fn running(&self) -> Vec<SlotInfo> {
        self.slots
            .iter()
            .filter(|slot| slot.phase == Phase::Running)
            .map(|slot| slot.info.clone())
            .collect()
    }

    /// Where the process of `label` is.
    #[must_use]
    pub fn phase(&self, label: &str) -> Option<Phase> {
        self.slots
            .iter()
            .find(|slot| slot.info.label == label)
            .map(|slot| slot.phase)
    }

    fn index_of(&self, label: &str) -> anyhow::Result<usize> {
        let slot = parse_label(label).ok_or_else(|| anyhow!("{label:?} is not a node label"))?;
        if slot > self.slots.len() {
            bail!("{label} has not been started");
        }
        Ok(slot - 1)
    }

    /// Notes the slots whose process has exited by itself.
    fn reap(&mut self) {
        for slot in &mut self.slots {
            let exited = slot
                .node
                .as_mut()
                .is_some_and(|node| matches!(node.child.try_wait(), Ok(Some(_))));
            if exited {
                slot.node = None;
                slot.phase = Phase::Stopped;
            }
        }
    }

    fn start(&mut self, index: usize) -> anyhow::Result<(SlotInfo, ReadyHandle)> {
        let slot = &mut self.slots[index];
        slot.starts += 1;
        let log = proc::log_path(&self.config.logs, slot.info.slot, slot.starts);
        let env = proc::node_env(
            self.config.layout,
            &slot.info,
            &proc::seed_list(self.config.layout),
            self.config.owners,
        );
        let node = proc::spawn(&self.config.testnode, &self.config.cluster, &env, &log)
            .with_context(|| format!("starting {}", slot.info.label))?;
        let ready = node.ready_handle();
        slot.node = Some(node);
        slot.phase = Phase::Running;
        Ok((slot.info.clone(), ready))
    }

    /// Starts the next slot. It returns as soon as the process runs; the
    /// handle waits for the node to be ready.
    ///
    /// # Errors
    ///
    /// Returns an error when every slot is in use or the node does not start.
    pub fn spawn_next(&mut self) -> anyhow::Result<(SlotInfo, ReadyHandle)> {
        self.reap();
        let slot = self.slots.len() + 1;
        if slot > MAX_SLOTS {
            bail!("the fleet runs at most {MAX_SLOTS} nodes");
        }
        let info = SlotInfo::new(self.config.layout, slot)
            .context("the layout leaves no room for another node")?;
        self.slots.push(Slot {
            info,
            starts: 0,
            node: None,
            phase: Phase::Stopped,
        });
        self.start(slot - 1).inspect_err(|_| {
            self.slots.pop();
        })
    }

    /// Starts a stopped slot again at its address. The node comes back with a
    /// new identity. A node still leaving is given up to [`STOP_GRACE`] to
    /// exit first. Like [`Fleet::spawn_next`] it returns once the process
    /// runs.
    ///
    /// # Errors
    ///
    /// Returns an error when the label is unknown, the node still runs, or
    /// the new node does not start.
    pub async fn restart(&mut self, label: &str) -> anyhow::Result<(SlotInfo, ReadyHandle)> {
        let index = self.index_of(label)?;
        if self.slots[index].phase == Phase::Leaving {
            self.wait_exit(index, STOP_GRACE).await;
        }
        self.reap();
        if self.slots[index].phase != Phase::Stopped {
            bail!("{label} is still running");
        }
        self.start(index)
    }

    async fn wait_exit(&mut self, index: usize, grace: Duration) {
        let slot = &mut self.slots[index];
        if let Some(node) = slot.node.as_mut()
            && tokio::time::timeout(grace, node.child.wait())
                .await
                .is_err()
        {
            let _ = node.child.start_kill();
            let _ = node.child.wait().await;
        }
        slot.node = None;
        slot.phase = Phase::Stopped;
    }

    fn running_index(&mut self, label: &str) -> anyhow::Result<usize> {
        let index = self.index_of(label)?;
        self.reap();
        if self.slots[index].phase == Phase::Stopped {
            bail!("{label} is not running");
        }
        Ok(index)
    }

    /// Sends SIGTERM to `label`: its node gossips a graceful departure and
    /// exits.
    ///
    /// # Errors
    ///
    /// Returns an error when the node is not running or the signal fails.
    pub fn leave(&mut self, label: &str) -> anyhow::Result<()> {
        let index = self.running_index(label)?;
        let slot = &mut self.slots[index];
        let pid = slot
            .node
            .as_ref()
            .map(|node| node.pid)
            .context("no process")?;
        proc::terminate(pid).with_context(|| format!("signalling {label}"))?;
        slot.phase = Phase::Leaving;
        Ok(())
    }

    /// Sends SIGKILL to `label` and waits for the process to be gone.
    ///
    /// # Errors
    ///
    /// Returns an error when the node is not running.
    pub async fn kill(&mut self, label: &str) -> anyhow::Result<()> {
        let index = self.running_index(label)?;
        self.wait_exit(index, Duration::ZERO).await;
        Ok(())
    }

    /// Asks the node of `label` to crash: it exits without leaving.
    ///
    /// # Errors
    ///
    /// Returns an error when the node is not running or does not answer.
    pub async fn crash(&mut self, label: &str) -> anyhow::Result<()> {
        let index = self.running_index(label)?;
        let control = self.slots[index].info.control;
        control::request(control, "crash", Duration::from_secs(5))
            .await
            .with_context(|| format!("asking {label} to crash"))?;
        self.wait_exit(index, STOP_GRACE).await;
        Ok(())
    }

    /// Stops every node: SIGTERM to all at once, then SIGKILL for any that
    /// outlasts one shared [`STOP_GRACE`]. No process of the fleet remains.
    pub async fn stop_all(&mut self) {
        for slot in &self.slots {
            if let Some(node) = &slot.node {
                let _ = proc::terminate(node.pid);
            }
        }
        let deadline = tokio::time::Instant::now() + STOP_GRACE;
        for index in 0..self.slots.len() {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            self.wait_exit(index, left).await;
        }
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        for slot in &mut self.slots {
            if let Some(node) = slot.node.as_mut() {
                let _ = node.child.start_kill();
            }
        }
    }
}

/// What a [`FleetStage`] knows about how its nodes are starting.
#[derive(Debug, Default)]
struct Startup {
    /// The latest start of each slot, by label.
    ready: HashMap<SmolStr, ReadyHandle>,
    /// The nodes that never became ready, with why.
    failures: Vec<String>,
}

/// A fleet and its load behind one handle: what the scenario director and the
/// demo keys act on. Cheap to clone.
///
/// Starting a node returns once its process runs. A node is ready some
/// seconds later, when it has opened its caches; the stage watches each
/// start in the background, and the steps that need a ready node
/// ([`FleetStage::fill`], [`FleetStage::wait_ready_all`]) wait for it.
#[derive(Debug, Clone)]
pub struct FleetStage {
    fleet: Arc<Mutex<Fleet>>,
    load: LoadHandle,
    exporter_limit: Duration,
    startup: Arc<std::sync::Mutex<Startup>>,
}

impl FleetStage {
    /// A stage over `fleet` and `load`.
    #[must_use]
    pub fn new(fleet: Fleet, load: LoadHandle) -> Self {
        Self {
            fleet: Arc::new(Mutex::new(fleet)),
            load,
            exporter_limit: HEALTHZ_TIMEOUT,
            startup: Arc::default(),
        }
    }

    /// Gives the first node's exporter `limit` to answer, instead of
    /// [`HEALTHZ_TIMEOUT`].
    #[must_use]
    pub const fn with_exporter_limit(mut self, limit: Duration) -> Self {
        self.exporter_limit = limit;
        self
    }

    fn startup(&self) -> std::sync::MutexGuard<'_, Startup> {
        self.startup
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Notes a started node, points the load at it and watches for its
    /// readiness.
    fn track(&self, info: &SlotInfo, ready: ReadyHandle) {
        self.load
            .add(info.label.clone(), info.control, load::phase_of(info.slot));
        self.startup()
            .ready
            .insert(info.label.clone(), ready.clone());
        let stage = self.clone();
        let label = info.label.clone();
        tokio::spawn(async move {
            if let Err(error) = ready.wait(READY_TIMEOUT).await {
                tracing::warn!("{label} did not become ready: {error:#}");
                stage.startup().failures.push(format!("{label}: {error:#}"));
            }
        });
    }

    /// Starts the next node and, when it is the first, checks that it serves
    /// its exporter.
    async fn start_one(&self) -> anyhow::Result<SlotInfo> {
        let mut fleet = self.fleet.lock().await;
        let first = fleet.slots.is_empty();
        let (info, ready) = fleet.spawn_next()?;
        self.track(&info, ready);
        if first {
            fleet.verify_exporter(self.exporter_limit).await?;
        }
        Ok(info)
    }

    /// Starts `count` nodes, `stagger` apart. It returns once every process
    /// runs, without waiting for the nodes to be ready: a lone first node
    /// waits for a peer to show up, and the next node to start is that peer.
    ///
    /// # Errors
    ///
    /// Returns an error from a start or the exporter check. The nodes that
    /// started keep running; [`FleetStage::stop_all`] ends
    /// them.
    pub async fn spawn_many(
        &self,
        count: usize,
        stagger: Option<Duration>,
    ) -> anyhow::Result<Vec<SlotInfo>> {
        let mut started = Vec::with_capacity(count);
        for index in 0..count {
            if let Some(stagger) = stagger.filter(|_| index > 0) {
                tokio::time::sleep(stagger).await;
            }
            started.push(self.start_one().await?);
        }
        Ok(started)
    }

    /// Starts one node.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`FleetStage::spawn_many`].
    pub async fn spawn_one(&self) -> anyhow::Result<SlotInfo> {
        let mut infos = self.spawn_many(1, None).await?;
        infos.pop().context("no node started")
    }

    /// Starts a stopped node again at its address and points the load at it.
    /// It returns once the process runs.
    ///
    /// # Errors
    ///
    /// Returns an error from [`Fleet::restart`].
    pub async fn restart(&self, label: &str) -> anyhow::Result<()> {
        let (info, ready) = self.fleet.lock().await.restart(label).await?;
        self.track(&info, ready);
        Ok(())
    }

    /// Waits until the latest start of `label` is ready.
    ///
    /// # Errors
    ///
    /// Returns an error when the node was never started, exited before it was
    /// ready, or took longer than [`READY_TIMEOUT`].
    pub async fn wait_ready(&self, label: &str) -> anyhow::Result<()> {
        let ready = self
            .startup()
            .ready
            .get(label)
            .cloned()
            .with_context(|| format!("{label} has not been started"))?;
        ready.wait(READY_TIMEOUT).await
    }

    /// Waits until the latest start of every node is ready.
    ///
    /// # Errors
    ///
    /// Returns the first node that does not become ready.
    pub async fn wait_ready_all(&self) -> anyhow::Result<()> {
        let handles: Vec<ReadyHandle> = self.startup().ready.values().cloned().collect();
        futures::future::try_join_all(handles.into_iter().map(|ready| ready.wait(READY_TIMEOUT)))
            .await
            .map(drop)
    }

    /// The nodes that never became ready, each with why and the end of its
    /// log.
    #[must_use]
    pub fn startup_failures(&self) -> Vec<String> {
        self.startup().failures.clone()
    }

    /// SIGKILLs a node and drops it from the load.
    ///
    /// # Errors
    ///
    /// Returns an error from [`Fleet::kill`].
    pub async fn kill(&self, label: &str) -> anyhow::Result<()> {
        self.load.remove(label);
        self.fleet.lock().await.kill(label).await
    }

    /// SIGTERMs a node, which leaves gracefully, and drops it from the load.
    ///
    /// # Errors
    ///
    /// Returns an error from [`Fleet::leave`].
    pub async fn leave(&self, label: &str) -> anyhow::Result<()> {
        self.load.remove(label);
        self.fleet.lock().await.leave(label)
    }

    /// Asks a node to crash and drops it from the load.
    ///
    /// # Errors
    ///
    /// Returns an error from [`Fleet::crash`].
    pub async fn crash(&self, label: &str) -> anyhow::Result<()> {
        self.load.remove(label);
        self.fleet.lock().await.crash(label).await
    }

    /// Writes `keys` keys through the first running node, once that node is
    /// ready.
    ///
    /// # Errors
    ///
    /// Returns an error when no node runs, the node does not become ready or
    /// the fill fails.
    pub async fn fill(&self, keys: u64) -> anyhow::Result<()> {
        let target = self
            .fleet
            .lock()
            .await
            .running()
            .into_iter()
            .next()
            .context("no node is running to fill through")?;
        self.wait_ready(&target.label).await?;
        let control = target.control;
        control::request(control, &format!("fill {keys}"), FILL_TIMEOUT)
            .await
            .with_context(|| format!("filling {keys} keys"))?;
        Ok(())
    }

    /// Starts or stops the load.
    pub fn set_load(&self, on: bool) {
        if on {
            self.load.start();
        } else {
            self.load.stop();
        }
    }

    /// Does what a demo key asks.
    ///
    /// # Errors
    ///
    /// Returns the error of the action.
    pub async fn apply(&self, command: FleetCmd) -> anyhow::Result<()> {
        match command {
            FleetCmd::Spawn => self.spawn_one().await.map(drop),
            FleetCmd::Kill(label) => self.kill(&label).await,
            FleetCmd::Leave(label) => self.leave(&label).await,
            FleetCmd::Restart(label) => self.restart(&label).await,
        }
    }

    /// Stops the load and every node.
    pub async fn stop_all(&self) {
        self.load.stop();
        self.fleet.lock().await.stop_all().await;
    }

    /// The slots started so far.
    pub async fn infos(&self) -> Vec<SlotInfo> {
        self.fleet.lock().await.infos()
    }
}

impl Stage for FleetStage {
    async fn spawn(&self, count: usize, stagger: Option<Duration>) -> anyhow::Result<()> {
        self.spawn_many(count, stagger).await.map(drop)
    }

    async fn fill(&self, keys: u64) -> anyhow::Result<()> {
        Self::fill(self, keys).await
    }

    fn load(&self, on: bool) {
        self.set_load(on);
    }

    async fn kill(&self, label: &str) -> anyhow::Result<()> {
        Self::kill(self, label).await
    }

    async fn leave(&self, label: &str) -> anyhow::Result<()> {
        Self::leave(self, label).await
    }

    async fn crash(&self, label: &str) -> anyhow::Result<()> {
        Self::crash(self, label).await
    }

    async fn restart(&self, label: &str) -> anyhow::Result<()> {
        Self::restart(self, label).await
    }
}

/// The scrape URL template that reaches each node's exporter: the exporter
/// lies [`METRICS_OFFSET`] ports above the node's gossip port in every
/// layout, so the template needs no per-layout form.
#[must_use]
pub fn metrics_template() -> String {
    format!("http://{{ip}}:{{gossip_port+{METRICS_OFFSET}}}/metrics")
}

/// The command line that watches the fleet `config` starts.
#[must_use]
pub fn watch_line(config: &FleetConfig) -> String {
    format!(
        "sundog-lens watch {} --seed {} --metrics '{}'",
        config.cluster,
        config.layout.first_gossip(),
        metrics_template(),
    )
}

/// What `cluster` prints for a started node.
#[must_use]
pub fn started_line(info: &SlotInfo) -> String {
    format!("{} started at {}", info.label, info.gossip)
}

/// Why binding the loopback address `base` failed: the per-address layout
/// needs the machine to answer on it.
fn address_error(base: Ipv4Addr, error: &io::Error) -> String {
    format!(
        "cannot bind {base}: {error}; the per-address layout needs Linux's 127.0.0.0/8 \
         loopback; on macOS add `sudo ifconfig lo0 alias 127.0.0.1N up` for each node, or leave \
         out --base-ip to share 127.0.0.1"
    )
}

/// Why binding `addr`, the `role` port of a slot, failed. The text says which
/// ports the fleet of `layout` uses.
fn port_error(layout: Layout, addr: SocketAddr, role: &str, error: &io::Error) -> String {
    match layout {
        Layout::PerAddress(_) => format!(
            "cannot bind {addr}: {error}; a listener on {CONTROL_PORT} or {METRICS_PORT} blocks \
             the fleet"
        ),
        Layout::Shared => {
            let first = Ports::FIXED;
            let last = layout.ports(MAX_SLOTS).unwrap_or(first);
            format!(
                "cannot bind {addr} ({role}): {error}; the fleet shares 127.0.0.1 and uses gossip \
                 ports {}-{}, control ports {}-{} and exporter ports {}-{}, so a process holding \
                 one of them blocks it",
                first.gossip, last.gossip, first.control, last.control, first.metrics, last.metrics,
            )
        }
    }
}

/// Runs `cluster`: starts `args.nodes` nodes, fills `args.keys` keys, starts
/// the load, prints the line that watches the cluster and runs until it is
/// interrupted; then stops every node.
///
/// # Errors
///
/// Returns an error when the machine cannot run the fleet or a node does not
/// start. The nodes already started are stopped first.
pub async fn cluster_cmd(args: FleetArgs) -> anyhow::Result<()> {
    let config = FleetConfig::from_args(&args)?;
    let line = watch_line(&config);
    let fleet = Fleet::new(config);
    fleet.preflight().await?;
    let (load, handle) = load::Load::spawn(args.rate, args.keys);
    let stage = FleetStage::new(fleet, handle);
    // A signal at any point, the startup included, still reaches `stop_all`.
    let outcome = tokio::select! {
        outcome = run_cluster(&stage, &args, &line) => outcome,
        () = crate::watch::termination() => {
            writeln!(io::stdout(), "stopping the nodes").map_err(Into::into)
        }
    };
    stage.stop_all().await;
    load.shutdown();
    outcome
}

async fn run_cluster(stage: &FleetStage, args: &FleetArgs, line: &str) -> anyhow::Result<()> {
    let mut out = io::stdout();
    let started = stage
        .spawn_many(args.nodes, Some(Duration::from_secs(1)))
        .await?;
    for info in &started {
        writeln!(out, "{}", started_line(info))?;
    }
    writeln!(out, "waiting for the nodes to open their caches")?;
    stage.wait_ready_all().await?;
    stage.fill(args.keys).await?;
    writeln!(out, "filled {} keys", args.keys)?;
    stage.set_load(true);
    writeln!(
        out,
        "load running at about {} operations a second per node",
        args.rate
    )?;
    writeln!(out, "{line}")?;
    writeln!(out, "press Ctrl-C to stop the nodes")?;
    std::future::pending().await
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const PER_ADDRESS: Layout = Layout::PerAddress(Ipv4Addr::new(127, 0, 0, 11));

    fn config(testnode: &str) -> FleetConfig {
        FleetConfig {
            cluster: "lens-test".to_owned(),
            layout: PER_ADDRESS,
            testnode: PathBuf::from(testnode),
            owners: NonZeroU8::new(2).unwrap(),
            logs: PathBuf::from("target/lens-demo"),
        }
    }

    fn shared_config(testnode: &str) -> FleetConfig {
        FleetConfig {
            layout: Layout::Shared,
            ..config(testnode)
        }
    }

    #[test]
    fn a_slot_has_its_ports_on_its_own_address() {
        let info = SlotInfo::new(PER_ADDRESS, 3).unwrap();
        assert_eq!(info.slot, 3);
        assert_eq!(info.label, "n3");
        assert_eq!(info.ip, Ipv4Addr::new(127, 0, 0, 13));
        assert_eq!(info.gossip, "127.0.0.13:7946".parse().unwrap());
        assert_eq!(info.control, "127.0.0.13:8080".parse().unwrap());
        assert_eq!(info.metrics, "127.0.0.13:9090".parse().unwrap());
        assert_eq!(SlotInfo::new(PER_ADDRESS, 0), None);
        assert_eq!(
            SlotInfo::new(Layout::PerAddress(Ipv4Addr::BROADCAST), 2),
            None
        );
    }

    #[test]
    fn a_shared_slot_has_its_own_ports_on_the_shared_address() {
        let info = SlotInfo::new(Layout::Shared, 3).unwrap();
        assert_eq!(info.slot, 3);
        assert_eq!(info.label, "n3");
        assert_eq!(info.ip, Ipv4Addr::LOCALHOST);
        assert_eq!(info.gossip, "127.0.0.1:7948".parse().unwrap());
        assert_eq!(info.control, "127.0.0.1:8082".parse().unwrap());
        assert_eq!(info.metrics, "127.0.0.1:9092".parse().unwrap());
        let first = SlotInfo::new(Layout::Shared, 1).unwrap();
        assert_eq!(first.gossip, "127.0.0.1:7946".parse().unwrap());
        assert_eq!(first.control, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(first.metrics, "127.0.0.1:9090".parse().unwrap());
        assert_eq!(SlotInfo::new(Layout::Shared, 0), None);
        assert_eq!(SlotInfo::new(Layout::Shared, 70_000), None);
    }

    #[test]
    fn the_observer_seeds_are_the_first_two_slots() {
        assert_eq!(
            config("x").seed_addrs(),
            [
                "127.0.0.11:7946".parse().unwrap(),
                "127.0.0.12:7946".parse().unwrap()
            ]
        );
        let mut last = config("x");
        last.layout = Layout::PerAddress(Ipv4Addr::BROADCAST);
        assert_eq!(last.seed_addrs().len(), 1);
    }

    #[test]
    fn the_shared_observer_seeds_are_the_first_two_gossip_ports() {
        assert_eq!(
            shared_config("x").seed_addrs(),
            [
                "127.0.0.1:7946".parse().unwrap(),
                "127.0.0.1:7947".parse().unwrap()
            ]
        );
    }

    #[test]
    fn the_watch_line_names_the_cluster_the_first_seed_and_the_exporter() {
        assert_eq!(
            watch_line(&config("x")),
            "sundog-lens watch lens-test --seed 127.0.0.11:7946 \
             --metrics 'http://{ip}:{gossip_port+1144}/metrics'"
        );
        assert_eq!(
            watch_line(&shared_config("x")),
            "sundog-lens watch lens-test --seed 127.0.0.1:7946 \
             --metrics 'http://{ip}:{gossip_port+1144}/metrics'"
        );
    }

    #[test]
    fn the_metrics_template_reaches_each_nodes_exporter_in_either_layout() {
        use std::collections::BTreeMap;
        use std::time::SystemTime;

        use sundog::membership::Peer;
        use sundog::node::{NodeId, NodeName};
        use sundog::observe::{Member, MemberStatus};

        use crate::source::targets::UrlTemplate;

        let template = UrlTemplate::parse(&metrics_template()).unwrap();
        assert_eq!(metrics_template(), "http://{ip}:{gossip_port+1144}/metrics");
        for layout in [PER_ADDRESS, Layout::Shared] {
            for slot in 1..=MAX_SLOTS {
                let info = SlotInfo::new(layout, slot).unwrap();
                let node = NodeId::from(slot as u64);
                let member = Member::new(
                    Peer {
                        node,
                        name: NodeName::new("host", node),
                        gossip_addr: info.gossip,
                        data_addr: SocketAddr::from((info.ip, 40_000)),
                        incarnation: 1,
                        protocol: 6,
                    },
                    MemberStatus::Live,
                    SystemTime::UNIX_EPOCH,
                    BTreeMap::new(),
                );
                assert_eq!(
                    template.expand(&member).unwrap(),
                    format!("http://{}/metrics", info.metrics),
                    "{layout:?} slot {slot}"
                );
            }
        }
        // The second shared slot's exporter, as the fleet documents it.
        let second = SlotInfo::new(Layout::Shared, 2).unwrap();
        assert_eq!(second.metrics, "127.0.0.1:9091".parse().unwrap());
    }

    #[test]
    fn the_cluster_command_prints_where_each_node_started() {
        assert_eq!(
            started_line(&SlotInfo::new(PER_ADDRESS, 2).unwrap()),
            "n2 started at 127.0.0.12:7946"
        );
        assert_eq!(
            started_line(&SlotInfo::new(Layout::Shared, 2).unwrap()),
            "n2 started at 127.0.0.1:7947"
        );
    }

    #[test]
    fn the_address_error_keeps_the_macos_alias_hint_and_names_the_flag() {
        let error = io::Error::from(io::ErrorKind::AddrNotAvailable);
        let text = address_error(Ipv4Addr::new(127, 0, 0, 11), &error);
        assert!(text.starts_with("cannot bind 127.0.0.11: "), "{text}");
        assert!(text.contains("sudo ifconfig lo0 alias"), "{text}");
        assert!(text.contains("leave out --base-ip"), "{text}");
    }

    #[test]
    fn a_blocked_port_error_names_the_ports_of_its_layout() {
        let error = io::Error::from(io::ErrorKind::AddrInUse);
        let per_address = port_error(
            PER_ADDRESS,
            "127.0.0.11:8080".parse().unwrap(),
            "control",
            &error,
        );
        assert!(
            per_address.starts_with("cannot bind 127.0.0.11:8080: "),
            "{per_address}"
        );
        assert!(
            per_address.contains("a listener on 8080 or 9090 blocks the fleet"),
            "{per_address}"
        );
        assert!(!per_address.contains("ifconfig"), "{per_address}");

        let shared = port_error(
            Layout::Shared,
            "127.0.0.1:7948".parse().unwrap(),
            "gossip",
            &error,
        );
        assert!(
            shared.starts_with("cannot bind 127.0.0.1:7948 (gossip): "),
            "{shared}"
        );
        for ports in ["7946-7951", "8080-8085", "9090-9095"] {
            assert!(shared.contains(ports), "{ports}: {shared}");
        }
        assert!(!shared.contains("ifconfig"), "{shared}");
        assert!(!shared.contains("alias"), "{shared}");
    }

    #[test]
    fn the_config_follows_the_fleet_flags() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let args = FleetArgs {
            name: "other".to_owned(),
            testnode: Some(manifest.clone()),
            logs: Some(PathBuf::from("/tmp/logs")),
            owners: NonZeroU8::new(3).unwrap(),
            ..FleetArgs::default()
        };
        let config = FleetConfig::from_args(&args).unwrap();
        assert_eq!(config.cluster, "other");
        assert_eq!(config.testnode, manifest);
        assert_eq!(config.logs, PathBuf::from("/tmp/logs"));
        assert_eq!(config.owners.get(), 3);
        assert_eq!(config.layout, Layout::for_host(None));
        let default_logs = FleetConfig::from_args(&FleetArgs {
            testnode: Some(manifest),
            ..FleetArgs::default()
        })
        .unwrap();
        assert_eq!(default_logs.logs, PathBuf::from(DEFAULT_LOGS));
        assert!(
            FleetConfig::from_args(&FleetArgs {
                testnode: Some(PathBuf::from("/no/such/node")),
                ..FleetArgs::default()
            })
            .is_err()
        );
    }

    #[test]
    fn an_explicit_base_ip_gives_the_per_address_layout_on_any_host() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let base = Ipv4Addr::new(127, 0, 0, 21);
        let config = FleetConfig::from_args(&FleetArgs {
            testnode: Some(manifest),
            base_ip: Some(base),
            ..FleetArgs::default()
        })
        .unwrap();
        assert_eq!(config.layout, Layout::PerAddress(base));
    }

    #[tokio::test]
    async fn the_shared_preflight_checks_every_port_of_every_slot() {
        // One test, so no other test holds a port this one probes.
        let fleet = Fleet::new(shared_config("x"));
        fleet.preflight().await.unwrap();

        let blocked = |addr: &str| addr.parse::<SocketAddr>().unwrap();
        let udp = UdpSocket::bind(blocked("127.0.0.1:7949")).await.unwrap();
        let error = fleet.preflight().await.unwrap_err().to_string();
        assert!(error.contains("127.0.0.1:7949 (gossip)"), "{error}");
        assert!(error.contains("7946-7951"), "{error}");
        drop(udp);

        let control = TcpListener::bind(blocked("127.0.0.1:8083")).await.unwrap();
        let error = fleet.preflight().await.unwrap_err().to_string();
        assert!(error.contains("127.0.0.1:8083 (control)"), "{error}");
        drop(control);

        let metrics = TcpListener::bind(blocked("127.0.0.1:9095")).await.unwrap();
        let error = fleet.preflight().await.unwrap_err().to_string();
        assert!(error.contains("127.0.0.1:9095 (exporter)"), "{error}");
        assert!(!error.contains("ifconfig"), "{error}");
        drop(metrics);

        fleet.preflight().await.unwrap();
    }

    #[test]
    fn a_fleet_starts_with_no_slots() {
        let fleet = Fleet::new(config("x"));
        let infos = fleet.infos();
        assert!(infos.is_empty(), "{infos:?}");
        let running = fleet.running();
        assert!(running.is_empty(), "{running:?}");
        assert_eq!(fleet.phase("n1"), None);
        assert_eq!(fleet.config().cluster, "lens-test");
    }

    #[test]
    fn at_most_six_nodes_run() {
        assert_eq!(MAX_SLOTS, 6);
        assert!(MAX_SLOTS <= crate::ui::theme::NODE_COLORS.len());
    }
}

/// Lifecycle tests that run real child processes and bind loopback addresses
/// other than `127.0.0.1`, which only Linux gives a process without setup.
#[cfg(all(test, target_os = "linux"))]
mod process_tests {
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
        let dir =
            std::env::temp_dir().join(format!("sundog-lens-fleet-{}-{name}", std::process::id()));
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
}
