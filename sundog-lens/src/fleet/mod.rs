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
use crate::ask::{self, Target};
use crate::cli::FleetArgs;
use crate::explained::Explained;
use crate::scenario::director::Stage;
use crate::source::http;
use load::LoadHandle;
use proc::{
    BUILD_HINT, CONTROL_OFFSET, CONTROL_PORT, METRICS_OFFSET, METRICS_PORT, NodeProc,
    READY_TIMEOUT, ReadyHandle, parse_label, slot_label,
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

    /// Asks every running node about `key`, each allowed [`ask::TIMEOUT`], and
    /// returns the answers in slot order. A node that does not answer keeps
    /// its place with the reason in words. The request carries
    /// [`ask::SCRIPTED_ID`]: it is a scenario's, not the overlay's.
    ///
    /// # Errors
    ///
    /// Returns an error when no node runs to ask.
    pub async fn explain(&self, key: &str) -> anyhow::Result<Explained> {
        let targets = targets_of(&self.fleet.lock().await.running());
        if targets.is_empty() {
            bail!("no node is running to ask");
        }
        Ok(ask::ask_all(ask::SCRIPTED_ID, key, &targets, ask::TIMEOUT).await)
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
            FleetCmd::Explain(_) => {
                bail!("an explain request is answered by ask::answer, not applied")
            }
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

    async fn explain(&self, key: &str) -> anyhow::Result<Explained> {
        Self::explain(self, key).await
    }
}

/// The nodes `infos` name, as the targets of an ask: each slot's label and
/// control address, in the order given.
fn targets_of(infos: &[SlotInfo]) -> Vec<Target> {
    infos
        .iter()
        .map(|info| Target {
            label: info.label.clone(),
            addr: info.control,
        })
        .collect()
}

/// The scrape URL template that reaches each node's exporter: the exporter
/// lies [`METRICS_OFFSET`] ports above the node's gossip port in every
/// layout, so the template needs no per-layout form.
#[must_use]
pub fn metrics_template() -> String {
    format!("http://{{ip}}:{{gossip_port+{METRICS_OFFSET}}}/metrics")
}

/// The control address template that reaches each node's control port: the
/// control port lies [`CONTROL_OFFSET`] ports above the node's gossip port in
/// every layout, so the template needs no per-layout form.
#[must_use]
pub fn control_template() -> String {
    format!("{{ip}}:{{gossip_port+{CONTROL_OFFSET}}}")
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
mod tests;

/// Lifecycle tests that run real child processes and bind loopback addresses
/// other than `127.0.0.1`, which only Linux gives a process without setup.
#[cfg(all(test, target_os = "linux"))]
// The module name keeps the test paths `fleet::process_tests::*`.
#[path = "tests/process_tests.rs"]
mod process_tests;
