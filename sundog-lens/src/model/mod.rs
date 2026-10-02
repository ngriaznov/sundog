//! The model: everything the interface shows, folded from [`Update`]s.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::observe::{ClusterSnapshot, Member, MemberStatus};
use sundog::store::Mode;

use crate::source::{ScrapeReport, Update};
use derive::NodeProgress;
use series::{RING_LEN, Ring};

pub mod derive;
pub mod digest;
pub mod events;
pub mod exporter;
pub mod lifelines;
pub mod metrics;
pub mod ownership;
pub mod series;
pub mod slots;
#[doc(hidden)]
pub mod testkit;

pub use digest::{ModelDigest, digest};
pub use events::{Event, EventKind, EventLog, Filter, diff_snapshots};
pub use exporter::ExporterState;
pub use lifelines::Lifelines;
pub use metrics::NodeMetrics;
pub use ownership::OwnershipDigest;
pub use slots::{Slot, Slots};

/// `count` as a float: counts here (parts, screen columns, members) are far
/// below 2^52, so the conversion is exact.
#[expect(clippy::cast_precision_loss, reason = "counts here are far below 2^52")]
pub(crate) const fn count_to_f64(count: usize) -> f64 {
    count as f64
}

/// With no metrics, a cache counts as settled once its view has held this
/// long.
pub const GOSSIP_SETTLE: Duration = Duration::from_secs(3);

/// How long the live member set has to hold still before the lens stops
/// discovering the cluster. The observer finds members one at a time, so the
/// ownership it computes while the set grows is the baseline, not a view
/// change of the cluster.
pub const DISCOVERY_QUIET: Duration = Duration::from_secs(2);

/// How long a node's `sundog_live_peers` has to disagree with the observer's
/// count before the PEERS column turns amber.
pub const PEERS_GRACE: Duration = Duration::from_secs(3);

/// The scrape interval the cluster throughput series assumes until
/// [`Model::set_scrape_interval`] says otherwise.
const DEFAULT_SCRAPE_INTERVAL: Duration = Duration::from_secs(1);

/// A node's peer count against the observer's.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PeersView {
    /// The node's `sundog_live_peers`.
    pub reported: f64,
    /// The observer's live members, less the node itself.
    pub expected: usize,
    /// Whether the two have disagreed for [`PEERS_GRACE`].
    pub amber: bool,
}

/// Members the observer counts live: `Live` and `Departing`.
fn live_members(snapshot: &ClusterSnapshot) -> usize {
    snapshot
        .members
        .iter()
        .filter(|member| member.status.is_live())
        .count()
}

/// Whether the member `kind` names started at or after `started`: its
/// incarnation, the wall-clock milliseconds at its start, is not earlier.
fn arrived_after(
    snapshot: &ClusterSnapshot,
    kind: &EventKind,
    started: Option<SystemTime>,
) -> bool {
    let (EventKind::Join { node, .. }
    | EventKind::Rejoin { node, .. }
    | EventKind::CacheAdded { node, .. }) = kind
    else {
        return false;
    };
    let Some(started) = started else {
        return false;
    };
    let incarnation = snapshot
        .members
        .iter()
        .filter(|member| member.peer.node == *node)
        .map(|member| member.peer.incarnation)
        .max();
    incarnation.is_some_and(|ms| SystemTime::UNIX_EPOCH + Duration::from_millis(ms) >= started)
}

/// The state the interface draws. `Clone` so the frozen display can keep a
/// copy while collection continues.
#[derive(Debug, Clone, Default)]
pub struct Model {
    slots: Slots,
    snapshot: Option<Arc<ClusterSnapshot>>,
    ownership: BTreeMap<SmolStr, OwnershipDigest>,
    view_since: BTreeMap<SmolStr, Instant>,
    settle_pending: BTreeSet<SmolStr>,
    scrapes: BTreeMap<SocketAddr, ScrapeReport>,
    last_ok: BTreeMap<SocketAddr, Instant>,
    metrics: BTreeMap<SocketAddr, NodeMetrics>,
    exporters: BTreeMap<SocketAddr, ExporterState>,
    peers_differ_since: BTreeMap<SocketAddr, Instant>,
    log: EventLog,
    lifelines: Lifelines,
    now: Option<Instant>,
    wall: Option<SystemTime>,
    live_set: Vec<NodeId>,
    live_set_changed: Option<Instant>,
    discovered: bool,
    started: Option<SystemTime>,
    scrape_interval: Option<Duration>,
    cluster_ops: Ring<RING_LEN>,
    cluster_ops_at: Option<Instant>,
}

impl Model {
    /// An empty model.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds `update` in at monotonic time `now` and wall-clock time `wall`
    /// and returns the events it raises. The events also join the
    /// [`events`](Self::events) log and mark the [`lifelines`](Self::lifelines).
    ///
    /// A snapshot replaces the member view, gives every gossip address a slot
    /// and raises the events [`diff_snapshots`] finds. A `DOWN` carries the
    /// time since the node's exporter last answered when its latest scrape
    /// failed. An ownership digest replaces its cache's digest; a changed view
    /// hash or owner count raises `VIEW` and restarts the cache's settling
    /// clock, except while the lens is still [discovering](Self::discovering)
    /// the cluster: those digests only replace the held one, and the members
    /// and caches it finds raise no `JOIN`, `REJOIN` or `CACHE+` (they still mark the lifelines). The
    /// ownership worker alone decides what ownership the model holds:
    /// [`Update::OwnershipGone`] drops a cache's digest, settling clock and
    /// lifeline, and a snapshot never does.
    ///
    /// A scrape replaces the node's last report and marks the node suspect on
    /// a failure and live again on a success. A scrape that answers folds into
    /// the node's [`NodeMetrics`] and can raise `XFER` and `DROP`; any scrape
    /// can raise the exporter events of [`ExporterState::observe`]. A scrape
    /// of a node id other than the one the address held before starts that
    /// address's metrics and exporter state afresh. Any update raises
    /// `SETTLED` for a cache whose view has held long enough.
    pub fn apply(&mut self, update: Update, now: Instant, wall: SystemTime) -> Vec<Event> {
        self.now = Some(now);
        self.wall = Some(wall);
        self.started.get_or_insert(wall);
        self.refresh_discovery(now);
        self.sample_cluster(now);
        let mut baseline = Vec::new();
        let mut kinds = match update {
            Update::Snapshot(snapshot, _) => self.apply_snapshot(snapshot, now, &mut baseline),
            Update::Ownership(digest) => self.apply_ownership(digest, now),
            Update::OwnershipGone(cache) => {
                self.forget_cache(&cache);
                Vec::new()
            }
            Update::Scrape(report) => self.apply_scrape(report, now),
        };
        self.refresh_peers(now);
        kinds.extend(self.settled_events(now));
        for kind in &baseline {
            self.lifelines.record(kind, now);
        }
        self.finish(kinds, now, wall)
    }

    /// Advances the model's clocks to `now` and returns the events the passing
    /// time raises: `SETTLED` for a cache whose view has now held long enough.
    /// A tick before the first update leaves the wall clock unset.
    pub fn tick(&mut self, now: Instant) -> Vec<Event> {
        let wall = self.wall_at(now);
        self.now = Some(now);
        self.wall = self.wall.map(|_| wall);
        self.refresh_discovery(now);
        self.sample_cluster(now);
        let kinds = self.settled_events(now);
        self.finish(kinds, now, wall)
    }

    /// Whether the lens is still discovering the cluster: no member has been
    /// seen yet, or the live member set changed less than
    /// [`DISCOVERY_QUIET`] ago. Once the set has held still that long, the
    /// phase is over for good.
    #[must_use]
    pub const fn discovering(&self) -> bool {
        !self.discovered
    }

    /// Ends discovery once the live member set has held still for
    /// [`DISCOVERY_QUIET`].
    fn refresh_discovery(&mut self, now: Instant) {
        if !self.discovered
            && self
                .live_set_changed
                .is_some_and(|at| now.saturating_duration_since(at) >= DISCOVERY_QUIET)
        {
            self.discovered = true;
        }
    }

    /// Sets the wall-clock time the lens started looking. A member whose
    /// process started before it is baseline while the lens discovers the
    /// cluster; one that started at or after it is an arrival. Without a call,
    /// the wall time of the first update stands for it.
    pub const fn set_started(&mut self, started: SystemTime) {
        self.started = Some(started);
    }

    /// Sets the scrape interval, which paces the cluster throughput series:
    /// one sample per interval.
    pub const fn set_scrape_interval(&mut self, interval: Duration) {
        self.scrape_interval = Some(interval);
    }

    /// The scrape interval that paces the cluster throughput series.
    #[must_use]
    pub fn scrape_interval(&self) -> Duration {
        self.scrape_interval.unwrap_or(DEFAULT_SCRAPE_INTERVAL)
    }

    /// The cluster's reads and fetches per second, one sample per scrape
    /// interval, oldest first. Each sample sums the newest figure of every
    /// node that is live and answered its last scrape when the sample was
    /// taken, so a crash or a failed scrape changes later samples only.
    #[must_use]
    pub const fn cluster_ops(&self) -> &Ring<RING_LEN> {
        &self.cluster_ops
    }

    /// Takes the next [`cluster_ops`](Self::cluster_ops) sample when a scrape
    /// interval has passed since the last. It reads the state before the
    /// update that carries `now`, so the nodes of one scrape round count
    /// together. The first sample waits until every answering node has a
    /// figure, so the series never starts with a part of the cluster.
    fn sample_cluster(&mut self, now: Instant) {
        let interval = self.scrape_interval();
        let due = self
            .cluster_ops_at
            .is_none_or(|at| now.saturating_duration_since(at) >= interval);
        if !due {
            return;
        }
        let answering = self.answering_ops();
        if self.cluster_ops.is_empty() && (answering.is_empty() || answering.contains(&None)) {
            return;
        }
        self.cluster_ops.push(answering.iter().flatten().sum());
        // Keep the cadence while the clock runs on time; restart it after a
        // stall of several intervals.
        self.cluster_ops_at = Some(match self.cluster_ops_at {
            Some(at) if now.saturating_duration_since(at) < interval * 2 => at + interval,
            _ => now,
        });
    }

    /// The newest `ops` figure of each live member whose exporter answered
    /// its last scrape, `None` for a node that has none yet.
    fn answering_ops(&self) -> Vec<Option<f64>> {
        let Some(snapshot) = &self.snapshot else {
            return Vec::new();
        };
        snapshot
            .members
            .iter()
            .filter(|member| member.status.is_live())
            .filter_map(|member| {
                let addr = member.peer.gossip_addr;
                let node = member.peer.node;
                let metrics = self
                    .metrics
                    .get(&addr)
                    .filter(|metrics| metrics.node() == node)?;
                self.scrapes
                    .get(&addr)
                    .filter(|report| report.node == node && report.outcome.is_ok())?;
                Some(metrics.ops().last())
            })
            .collect()
    }

    /// Names the node at gossip address `addr` as `label`, for the slot that
    /// exists or will.
    pub fn set_label_hint(&mut self, addr: SocketAddr, label: impl Into<SmolStr>) {
        self.slots.hint(addr, label);
    }

    /// The latest member snapshot.
    #[must_use]
    pub fn snapshot(&self) -> Option<&Arc<ClusterSnapshot>> {
        self.snapshot.as_ref()
    }

    /// The node slots.
    #[must_use]
    pub const fn slots(&self) -> &Slots {
        &self.slots
    }

    /// The ownership digest of `cache`.
    #[must_use]
    pub fn ownership(&self, cache: &str) -> Option<&OwnershipDigest> {
        self.ownership.get(cache)
    }

    /// Every ownership digest, ascending by cache name.
    pub fn ownership_digests(&self) -> impl Iterator<Item = &OwnershipDigest> {
        self.ownership.values()
    }

    /// The last scrape report of the node at gossip address `addr`.
    #[must_use]
    pub fn scrape(&self, addr: SocketAddr) -> Option<&ScrapeReport> {
        self.scrapes.get(&addr)
    }

    /// The event log, oldest first.
    #[must_use]
    pub const fn events(&self) -> &EventLog {
        &self.log
    }

    /// The node and cache lifelines.
    #[must_use]
    pub const fn lifelines(&self) -> &Lifelines {
        &self.lifelines
    }

    /// The monotonic time of the last update or tick.
    #[must_use]
    pub const fn now(&self) -> Option<Instant> {
        self.now
    }

    /// The wall-clock time of the last update or tick. A tick advances it by
    /// the monotonic time that passed.
    #[must_use]
    pub const fn wall(&self) -> Option<SystemTime> {
        self.wall
    }

    /// The settling verdict for `cache`. Every eligible node that reports the
    /// cache votes. A node whose exporter has not answered since the view
    /// changed holds the verdict open until it answers or its exporter is
    /// unreachable, and a later failed scrape does not withdraw an answered
    /// vote. The cache is settled once each voter reports the parts the
    /// observer computes for it and has pulled none in for
    /// [`derive::QUIET_SCRAPES`] scrapes taken since the view changed. With no
    /// voter the verdict rests on gossip alone, and the cache is settled once
    /// its view has held for [`GOSSIP_SETTLE`]. `None` for a cache with no
    /// ownership digest.
    #[must_use]
    pub fn settle(&self, cache: &str) -> Option<derive::Settle> {
        let since = *self.view_since.get(cache)?;
        let now = self.now?;
        let digest = self.ownership.get(cache)?;
        let progress: Vec<_> = digest
            .eligible
            .iter()
            .map(|&node| self.progress(digest, node, since))
            .collect();
        Some(derive::settled(
            now.saturating_duration_since(since),
            &progress,
        ))
    }

    /// Whether `cache` has settled; see [`settle`](Self::settle).
    #[must_use]
    pub fn settled(&self, cache: &str) -> Option<bool> {
        self.settle(cache).map(|verdict| verdict.settled)
    }

    /// The metrics of the node at gossip address `addr`: what its exporter
    /// reported, folded over time. A new node id at the address starts afresh.
    #[must_use]
    pub fn metrics(&self, addr: SocketAddr) -> Option<&NodeMetrics> {
        self.metrics.get(&addr)
    }

    /// The exporter state of the node at gossip address `addr`.
    #[must_use]
    pub fn exporter(&self, addr: SocketAddr) -> Option<&ExporterState> {
        self.exporters.get(&addr)
    }

    /// How the node at `addr` counts its peers against the observer: its
    /// `sundog_live_peers` against the live members the observer sees, less
    /// the node itself. `None` without metrics from a node the observer lists
    /// live.
    #[must_use]
    pub fn peers(&self, addr: SocketAddr) -> Option<PeersView> {
        let snapshot = self.snapshot.as_ref()?;
        let metrics = self.metrics.get(&addr)?;
        let reported = metrics.live_peers()?;
        let listed = snapshot.members.iter().any(|member| {
            member.peer.gossip_addr == addr
                && member.peer.node == metrics.node()
                && member.status.is_live()
        });
        listed.then(|| PeersView {
            reported,
            expected: live_members(snapshot).saturating_sub(1),
            amber: self.peers_differ_since.get(&addr).is_some_and(|since| {
                self.now
                    .is_some_and(|now| now.saturating_duration_since(*since) >= PEERS_GRACE)
            }),
        })
    }

    /// The fraction of `cache`'s owner slots the eligible nodes report holding:
    /// see [`derive::coverage`]. `None` without a digest or a reporting node.
    #[must_use]
    pub fn coverage(&self, cache: &str) -> Option<f64> {
        let digest = self.ownership.get(cache)?;
        let reported: Vec<f64> = digest
            .eligible
            .iter()
            .filter_map(|&node| self.live_metrics_of(node)?.owned_parts(cache))
            .collect();
        if reported.is_empty() {
            return None;
        }
        derive::coverage(reported.iter().sum(), digest.k, digest.eligible.len())
    }

    /// The spread of entries across the live members that advertise `cache`
    /// as `Replicated` and report it: see [`derive::divergence`].
    #[must_use]
    pub fn divergence(&self, cache: &str) -> Option<f64> {
        let snapshot = self.snapshot.as_ref()?;
        let entries: Vec<f64> = snapshot
            .members
            .iter()
            .filter(|member| {
                member.status == MemberStatus::Live
                    && member.caches.get(cache) == Some(&Mode::Replicated)
            })
            .filter_map(|member| self.live_metrics_of(member.peer.node)?.entries(cache))
            .collect();
        derive::divergence(&entries)
    }

    /// The live member `node` and its metrics.
    fn live_member_metrics(&self, node: NodeId) -> Option<(&Member, &NodeMetrics)> {
        let member = self
            .snapshot
            .as_ref()?
            .members
            .iter()
            .find(|member| member.peer.node == node && member.status.is_live())?;
        let metrics = self
            .metrics
            .get(&member.peer.gossip_addr)
            .filter(|metrics| metrics.node() == node)?;
        Some((member, metrics))
    }

    /// The metrics of the live member `node`.
    fn live_metrics_of(&self, node: NodeId) -> Option<&NodeMetrics> {
        self.live_member_metrics(node).map(|(_, metrics)| metrics)
    }

    /// What the settling test knows about `node` for the view that began at
    /// `since`. A node that reports nothing for `digest.cache`, or whose
    /// exporter is unreachable and has not answered since `since`, has no
    /// vote. A node that reports the cache but has not answered since `since`
    /// holds the verdict open with a pending vote. A failed scrape leaves the
    /// vote as the last answer cast it.
    fn progress(&self, digest: &OwnershipDigest, node: NodeId, since: Instant) -> NodeProgress {
        let silent = NodeProgress {
            agrees: None,
            quiet_scrapes: 0,
        };
        let Some((member, metrics)) = self.live_member_metrics(node) else {
            return silent;
        };
        let Some(reported) = metrics.owned_parts(&digest.cache) else {
            return silent;
        };
        let addr = member.peer.gossip_addr;
        let fresh = self.last_ok.get(&addr).is_some_and(|at| *at >= since);
        if !fresh {
            let unreachable = self
                .exporters
                .get(&addr)
                .is_some_and(ExporterState::unreachable);
            return if unreachable {
                silent
            } else {
                NodeProgress {
                    agrees: Some(false),
                    quiet_scrapes: 0,
                }
            };
        }
        NodeProgress {
            agrees: Some(
                derive::agreement(Some(reported), digest.parts_owned_by(node))
                    == derive::Agreement::Match,
            ),
            quiet_scrapes: metrics.quiet_scrapes_since(&digest.cache, since),
        }
    }

    /// Starts or ends the clock of each node whose peer count disagrees with
    /// the observer's.
    fn refresh_peers(&mut self, now: Instant) {
        let Some(snapshot) = self.snapshot.clone() else {
            return;
        };
        let live = live_members(&snapshot);
        let differing: Vec<SocketAddr> = self
            .metrics
            .iter()
            .filter(|(addr, metrics)| {
                let listed = snapshot.members.iter().any(|member| {
                    member.peer.gossip_addr == **addr
                        && member.peer.node == metrics.node()
                        && member.status.is_live()
                });
                listed && derive::peers_agree(metrics.live_peers(), live) == Some(false)
            })
            .map(|(&addr, _)| addr)
            .collect();
        self.peers_differ_since
            .retain(|addr, _| differing.contains(addr));
        for addr in differing {
            self.peers_differ_since.entry(addr).or_insert(now);
        }
    }

    /// Folds a snapshot in and returns the events it raises. While the
    /// observer discovers the cluster, the members and caches it finds are the
    /// baseline, not arrivals: they joined before the lens looked. Their
    /// events go to `baseline`, which marks the lifelines and stays out of
    /// the log.
    fn apply_snapshot(
        &mut self,
        snapshot: Arc<ClusterSnapshot>,
        now: Instant,
        baseline: &mut Vec<EventKind>,
    ) -> Vec<EventKind> {
        let mut kinds = diff_snapshots(self.snapshot.as_deref(), &snapshot);
        if !self.discovered {
            let started = self.started;
            let (found, rest): (Vec<_>, Vec<_>) = kinds.into_iter().partition(|kind| {
                matches!(
                    kind,
                    EventKind::Join { .. }
                        | EventKind::Rejoin { .. }
                        | EventKind::CacheAdded { .. }
                ) && !arrived_after(&snapshot, kind, started)
            });
            if rest
                .iter()
                .any(|kind| matches!(kind, EventKind::Join { .. } | EventKind::Rejoin { .. }))
            {
                // A member that started after the lens looked is a real
                // arrival: discovery ends and the views that follow count.
                self.discovered = true;
            }
            *baseline = found;
            kinds = rest;
        }
        for kind in &mut kinds {
            if let EventKind::Down {
                addr,
                exporter_silent,
                ..
            } = kind
            {
                *exporter_silent = self.exporter_silence(*addr, now);
            }
        }
        // Members unseen so far take their slots in address order, so that a
        // baseline that lists them in any order labels them the same way.
        let mut unseen: Vec<SocketAddr> = snapshot
            .members
            .iter()
            .map(|member| member.peer.gossip_addr)
            .filter(|addr| self.slots.get(*addr).is_none())
            .collect();
        unseen.sort_unstable();
        unseen.dedup();
        for addr in unseen {
            self.slots.assign(addr);
        }
        let live_set: Vec<NodeId> = snapshot
            .members
            .iter()
            .filter(|member| member.status.is_live())
            .map(|member| member.peer.node)
            .collect();
        if live_set != self.live_set {
            if !live_set.is_empty() {
                self.live_set_changed = Some(now);
            }
            self.live_set = live_set;
        }
        self.snapshot = Some(snapshot);
        kinds
    }

    fn apply_ownership(&mut self, digest: OwnershipDigest, now: Instant) -> Vec<EventKind> {
        let held = self.ownership.get(&digest.cache);
        let mut kinds = Vec::new();
        if !self.discovered {
            // The baseline: the view the observer has found so far, which
            // grows as it finds members. Nothing moved in the cluster.
            if held.is_none_or(|held| (held.view_hash, held.k) != (digest.view_hash, digest.k)) {
                self.view_since.insert(digest.cache.clone(), now);
            }
            self.ownership.insert(digest.cache.clone(), digest);
            return kinds;
        }
        if held.is_none_or(|held| (held.view_hash, held.k) != (digest.view_hash, digest.k)) {
            let deltas = held.map_or_else(
                || {
                    digest
                        .counts
                        .iter()
                        .map(|&(node, count)| (node, i64::try_from(count).unwrap_or(i64::MAX)))
                        .collect()
                },
                |held| ownership::share_deltas(&held.shares, &digest.shares),
            );
            kinds.push(EventKind::View {
                cache: digest.cache.clone(),
                from: held.map(|held| held.view_hash),
                to: digest.view_hash,
                moved: digest.moved,
                deltas,
            });
            self.view_since.insert(digest.cache.clone(), now);
            self.settle_pending.insert(digest.cache.clone());
        }
        self.ownership.insert(digest.cache.clone(), digest);
        kinds
    }

    fn apply_scrape(&mut self, report: ScrapeReport, now: Instant) -> Vec<EventKind> {
        let (addr, node) = (report.addr, report.node);
        if self
            .exporters
            .get(&addr)
            .is_some_and(|state| state.node() != node)
        {
            self.exporters.remove(&addr);
            self.metrics.remove(&addr);
            self.last_ok.remove(&addr);
        }
        let listed_live = self.snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .members
                .iter()
                .any(|member| member.peer.node == node && member.status == MemberStatus::Live)
        });
        let mut kinds = self
            .exporters
            .entry(addr)
            .or_insert_with(|| ExporterState::new(node))
            .observe(&report, listed_live);
        match &report.outcome {
            Ok(samples) => {
                self.last_ok.insert(addr, report.at);
                self.lifelines.recovered(addr, now);
                let folded = self
                    .metrics
                    .entry(addr)
                    .or_insert_with(|| NodeMetrics::new(node))
                    .fold(report.at, samples);
                if folded.xfer_started {
                    kinds.push(EventKind::Xfer { node });
                }
                kinds.extend(folded.drops.into_iter().map(|edge| EventKind::Drop {
                    node,
                    peer: edge.peer,
                    frames: edge.frames,
                }));
            }
            Err(error) if !error.is_mapping() => {
                self.lifelines.suspect(addr, now);
                if let Some(metrics) = self.metrics.get_mut(&addr) {
                    metrics.mark_stale();
                }
            }
            Err(_) => {}
        }
        self.scrapes.insert(addr, report);
        kinds
    }

    /// Drops everything the model holds for `cache`: its ownership, its
    /// settling clock and its lifeline.
    fn forget_cache(&mut self, cache: &str) {
        self.ownership.remove(cache);
        self.view_since.remove(cache);
        self.settle_pending.remove(cache);
        self.lifelines.forget_cache(cache);
    }

    /// How long before `now` the exporter of the node at `addr` last answered,
    /// when its latest scrape failed.
    fn exporter_silence(&self, addr: SocketAddr, now: Instant) -> Option<Duration> {
        let failing = self.scrapes.get(&addr).is_some_and(|report| {
            report
                .outcome
                .as_ref()
                .is_err_and(|error| !error.is_mapping())
        });
        let last_ok = self.last_ok.get(&addr)?;
        failing.then(|| now.saturating_duration_since(*last_ok))
    }

    /// `SETTLED` for each cache that has a view change to settle and has now
    /// settled.
    fn settled_events(&mut self, now: Instant) -> Vec<EventKind> {
        let ready: Vec<SmolStr> = self
            .settle_pending
            .iter()
            .filter(|cache| self.settled(cache) == Some(true))
            .cloned()
            .collect();
        ready
            .into_iter()
            .map(|cache| {
                self.settle_pending.remove(&cache);
                let since = self.view_since.get(&cache).copied().unwrap_or(now);
                EventKind::Settled {
                    took: now.saturating_duration_since(since),
                    cache,
                }
            })
            .collect()
    }

    fn wall_at(&self, now: Instant) -> SystemTime {
        let (Some(wall), Some(then)) = (self.wall, self.now) else {
            return SystemTime::UNIX_EPOCH;
        };
        wall + now.saturating_duration_since(then)
    }

    fn finish(&mut self, kinds: Vec<EventKind>, now: Instant, wall: SystemTime) -> Vec<Event> {
        kinds
            .into_iter()
            .filter_map(|kind| {
                if let EventKind::CacheAdded { node, cache, mode } = &kind
                    && self.log.fold_cache(*node, cache, *mode, wall)
                {
                    return None;
                }
                self.lifelines.record(&kind, now);
                let event = Event { at: wall, kind };
                self.log.push(event.clone());
                Some(event)
            })
            .collect()
    }
}

#[cfg(test)]
mod scrape_tests;

#[cfg(test)]
mod tests {
    use sundog::observe::MemberStatus;

    use super::*;

    /// Later than every fixture incarnation, so fixture members predate the lens.
    fn wall0() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(100)
    }

    fn snapshot_update(snapshot: ClusterSnapshot, now: Instant) -> Update {
        Update::Snapshot(Arc::new(snapshot), now)
    }

    fn ownership_update(live: u8) -> Update {
        Update::Ownership(testkit::ownership_digest(&testkit::snapshot(live), "it").unwrap())
    }

    #[test]
    fn a_new_model_is_empty() {
        let model = Model::new();
        assert!(model.snapshot().is_none());
        assert!(model.slots().is_empty());
        assert!(model.ownership("it").is_none());
        assert_eq!(model.ownership_digests().count(), 0);
        assert!(model.now().is_none() && model.wall().is_none());
        assert_eq!(model.settled("it"), None);
    }

    #[test]
    fn a_snapshot_gives_each_gossip_address_a_slot() {
        let mut model = Model::new();
        let now = Instant::now();
        let events = model.apply(snapshot_update(testkit::snapshot(3), now), now, wall0());
        assert!(events.is_empty(), "the first snapshot is the baseline");
        assert_eq!(model.snapshot().unwrap().members.len(), 3);
        assert_eq!(model.slots().len(), 3);
        assert_eq!(
            model.slots().get(testkit::gossip_addr(2)).unwrap().label,
            "n2"
        );
        assert_eq!(model.wall(), Some(wall0()));
    }

    #[test]
    fn members_unseen_so_far_take_slots_in_address_order_whatever_the_snapshot_order() {
        let now = Instant::now();
        for order in [[3, 1, 2], [2, 3, 1], [1, 2, 3]] {
            let mut model = Model::new();
            let members = order
                .iter()
                .map(|&index| testkit::member(index, MemberStatus::Live))
                .collect();
            let snapshot = ClusterSnapshot::new("c", members, 0);
            model.apply(snapshot_update(snapshot, now), now, wall0());
            for index in 1..=3 {
                let slot = model.slots().get(testkit::gossip_addr(index)).unwrap();
                assert_eq!(slot.index, usize::from(index) - 1, "{order:?}");
                assert_eq!(slot.label, format!("n{index}"), "{order:?}");
            }
        }

        // A member that appears later takes the next slot after those held.
        let mut model = Model::new();
        let both = ClusterSnapshot::new(
            "c",
            vec![
                testkit::member(5, MemberStatus::Live),
                testkit::member(4, MemberStatus::Live),
            ],
            0,
        );
        model.apply(snapshot_update(both, now), now, wall0());
        let more = ClusterSnapshot::new(
            "c",
            vec![
                testkit::member(5, MemberStatus::Live),
                testkit::member(4, MemberStatus::Live),
                testkit::member(1, MemberStatus::Live),
            ],
            0,
        );
        model.apply(snapshot_update(more, now), now, wall0());
        let label = |index| {
            model
                .slots()
                .get(testkit::gossip_addr(index))
                .unwrap()
                .label
                .clone()
        };
        assert_eq!(
            [label(4), label(5), label(1)],
            [SmolStr::new("n1"), SmolStr::new("n2"), SmolStr::new("n3")]
        );
    }

    #[test]
    fn caches_a_node_opens_soon_after_it_joins_fold_into_its_join_row() {
        let mut model = Model::new();
        let now = Instant::now();
        let later = wall0() + Duration::from_secs(1);
        let first = ClusterSnapshot::new(
            "c",
            vec![testkit::member_with(1, 0, 1, MemberStatus::Live, &[])],
            0,
        );
        model.apply(snapshot_update(first, now), now, wall0());
        // The observer has found the cluster when node 2 arrives, with no
        // cache keys yet; they follow in gossip a moment later.
        let found = now + DISCOVERY_QUIET;
        model.tick(found);
        let side = Mode::Replicated;
        let with = |caches: &[(&str, Mode)]| {
            ClusterSnapshot::new(
                "c",
                vec![
                    testkit::member_with(1, 0, 1, MemberStatus::Live, &[]),
                    testkit::member_with(2, 0, 1, MemberStatus::Live, caches),
                ],
                0,
            )
        };
        model.apply(snapshot_update(with(&[]), found), found, later);
        let events = model.apply(
            snapshot_update(
                with(&[("it", testkit::distributed(2)), ("os", side)]),
                found,
            ),
            found,
            later + Duration::from_secs(1),
        );
        assert!(
            events.is_empty(),
            "the caches fold into the join: {events:?}"
        );
        let rows: Vec<&Event> = model.events().iter().collect();
        assert_eq!(rows.len(), 1, "{rows:?}");
        let EventKind::Join { caches, .. } = &rows[0].kind else {
            panic!("a join row, got {:?}", rows[0].kind);
        };
        assert_eq!(
            caches.keys().map(SmolStr::as_str).collect::<Vec<_>>(),
            ["it", "os"]
        );

        // A cache opened long after the join is a row of its own.
        let much_later = found + Duration::from_secs(60);
        let events = model.apply(
            snapshot_update(
                with(&[("it", testkit::distributed(2)), ("os", side), ("pn", side)]),
                much_later,
            ),
            much_later,
            later + Duration::from_secs(60),
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind.tag(), "CACHE+");
    }

    #[test]
    fn a_restarted_node_keeps_its_slot() {
        let mut model = Model::new();
        let now = Instant::now();
        let first = ClusterSnapshot::new(
            "c",
            vec![testkit::member_at(1, 0, 1, MemberStatus::Live)],
            0,
        );
        model.apply(snapshot_update(first, now), now, SystemTime::UNIX_EPOCH);
        let slot = model.slots().get(testkit::gossip_addr(1)).unwrap().clone();
        let second = ClusterSnapshot::new(
            "c",
            vec![testkit::member_at(1, 1, 2, MemberStatus::Live)],
            0,
        );
        model.apply(snapshot_update(second, now), now, SystemTime::UNIX_EPOCH);
        assert_eq!(model.slots().get(testkit::gossip_addr(1)), Some(&slot));
        assert_eq!(model.slots().len(), 1);
    }

    #[test]
    fn label_hints_name_slots_before_and_after_they_exist() {
        let mut model = Model::new();
        model.set_label_hint(testkit::gossip_addr(3), "n3");
        let now = Instant::now();
        model.apply(
            snapshot_update(testkit::snapshot(3), now),
            now,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(
            model.slots().get(testkit::gossip_addr(3)).unwrap().label,
            "n3"
        );
        model.set_label_hint(testkit::gossip_addr(1), "alpha");
        assert_eq!(
            model.slots().get(testkit::gossip_addr(1)).unwrap().label,
            "alpha"
        );
    }

    #[test]
    fn an_ownership_digest_replaces_its_cache_entry() {
        let mut model = Model::new();
        let now = Instant::now();
        model.apply(ownership_update(3), now, SystemTime::UNIX_EPOCH);
        let first = model.ownership("it").unwrap().view_hash;
        model.apply(ownership_update(4), now, SystemTime::UNIX_EPOCH);
        let second = model.ownership("it").unwrap().view_hash;
        assert_ne!(first, second);
        assert_eq!(model.ownership_digests().count(), 1);
    }

    #[test]
    fn a_cache_settles_after_its_view_holds_for_three_seconds() {
        let mut model = Model::new();
        let start = Instant::now();
        model.apply(ownership_update(3), start, SystemTime::UNIX_EPOCH);
        assert_eq!(model.settled("it"), Some(false));
        model.tick(start + Duration::from_millis(2999));
        assert_eq!(model.settled("it"), Some(false));
        model.tick(start + Duration::from_secs(3));
        assert_eq!(model.settled("it"), Some(true));
        assert_eq!(model.now(), Some(start + Duration::from_secs(3)));
    }

    #[test]
    fn an_unchanged_view_does_not_restart_the_settling_clock() {
        let mut model = Model::new();
        let start = Instant::now();
        model.apply(ownership_update(3), start, SystemTime::UNIX_EPOCH);
        model.apply(
            ownership_update(3),
            start + Duration::from_secs(4),
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(model.settled("it"), Some(true));
    }

    #[test]
    fn a_new_owner_count_over_the_same_members_raises_a_view_and_restarts_the_clock() {
        let (mut model, start) = testkit::past_discovery(Instant::now());
        let two = testkit::ownership_digest_with_owners(
            &testkit::snapshot_with_owners(3, 2),
            "it",
            owners(2),
            None,
        )
        .unwrap();
        let three = testkit::ownership_digest_with_owners(
            &testkit::snapshot_with_owners(3, 3),
            "it",
            owners(3),
            Some(&two),
        )
        .unwrap();
        assert_eq!(two.view_hash, three.view_hash);
        model.apply(Update::Ownership(two), start, SystemTime::UNIX_EPOCH);
        let later = start + Duration::from_secs(4);
        assert_eq!(model.settled("it"), Some(false));
        model.tick(later);
        assert_eq!(model.settled("it"), Some(true));
        let events = model.apply(Update::Ownership(three), later, SystemTime::UNIX_EPOCH);
        assert_eq!(tags(&events), ["VIEW"]);
        assert_eq!(model.ownership("it").unwrap().k, owners(3));
        assert_eq!(model.settled("it"), Some(false));
        model.tick(later + Duration::from_secs(3));
        assert_eq!(model.settled("it"), Some(true));
    }

    #[test]
    fn a_changed_view_restarts_the_settling_clock() {
        let mut model = Model::new();
        let start = Instant::now();
        model.apply(ownership_update(3), start, SystemTime::UNIX_EPOCH);
        model.apply(
            ownership_update(4),
            start + Duration::from_secs(4),
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(model.settled("it"), Some(false));
        model.tick(start + Duration::from_secs(7));
        assert_eq!(model.settled("it"), Some(true));
    }

    #[test]
    fn a_scrape_report_replaces_the_nodes_last_one() {
        let mut model = Model::new();
        let now = Instant::now();
        let addr = testkit::gossip_addr(1);
        let report = |ready| ScrapeReport {
            addr,
            node: testkit::node_id(1, 0),
            at: now,
            outcome: Ok(Vec::new()),
            ready,
        };
        assert!(model.scrape(addr).is_none());
        model.apply(
            Update::Scrape(report(Some(false))),
            now,
            SystemTime::UNIX_EPOCH,
        );
        model.apply(
            Update::Scrape(report(Some(true))),
            now,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(model.scrape(addr).unwrap().ready, Some(true));
        assert!(model.scrape(testkit::gossip_addr(2)).is_none());
    }

    #[test]
    fn tick_advances_the_clock_and_raises_no_events() {
        let mut model = Model::new();
        let now = Instant::now();
        let events = model.tick(now);
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(model.now(), Some(now));
        assert_eq!(model.wall(), None, "no update has given a wall clock yet");
        model.apply(ownership_update(1), now, SystemTime::UNIX_EPOCH);
        model.tick(now + Duration::from_secs(2));
        assert_eq!(
            model.wall(),
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2))
        );
    }

    #[test]
    fn a_model_clones_independently() {
        let mut model = Model::new();
        let now = Instant::now();
        model.apply(
            snapshot_update(testkit::snapshot(2), now),
            now,
            SystemTime::UNIX_EPOCH,
        );
        let frozen = model.clone();
        model.apply(
            snapshot_update(testkit::snapshot(3), now),
            now,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(frozen.snapshot().unwrap().members.len(), 2);
        assert_eq!(model.snapshot().unwrap().members.len(), 3);
    }

    fn owners(count: u8) -> std::num::NonZeroU8 {
        std::num::NonZeroU8::new(count).expect("owners is nonzero")
    }

    fn tags(events: &[Event]) -> Vec<&'static str> {
        events.iter().map(|event| event.kind.tag()).collect()
    }

    fn with_status(live: u8, index: u8, status: MemberStatus) -> ClusterSnapshot {
        let mut members = testkit::snapshot(live).members;
        members[usize::from(index - 1)].status = status;
        ClusterSnapshot::new("fixture", members, 0)
    }

    #[test]
    fn a_status_change_raises_its_event_and_the_log_keeps_it() {
        let mut model = Model::new();
        let t = Instant::now();
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        model.apply(snapshot_update(testkit::snapshot(3), t), t, wall);
        let events = model.apply(
            snapshot_update(with_status(3, 2, MemberStatus::Departing), t),
            t,
            wall,
        );
        assert_eq!(tags(&events), ["LEAVE"]);
        assert_eq!(events[0].at, wall);
        let events = model.apply(
            snapshot_update(with_status(3, 2, MemberStatus::Left), t),
            t,
            wall,
        );
        assert_eq!(tags(&events), ["LEFT"]);
        let logged: Vec<_> = model.events().iter().map(|e| e.kind.tag()).collect();
        assert_eq!(logged, ["LEAVE", "LEFT"], "the members found are no JOINs");
    }

    #[test]
    fn events_mark_the_lifeline_of_the_node_they_concern() {
        use lifelines::{MarkKind, PhaseKind};
        let mut model = Model::new();
        let t = Instant::now();
        model.apply(
            snapshot_update(testkit::snapshot(2), t),
            t,
            SystemTime::UNIX_EPOCH,
        );
        model.apply(
            snapshot_update(
                with_status(2, 1, MemberStatus::Down),
                t + Duration::from_secs(1),
            ),
            t + Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        );
        let first = model.lifelines().node(testkit::gossip_addr(1)).unwrap();
        let kinds: Vec<_> = first.marks().iter().map(|m| m.kind).collect();
        assert_eq!(kinds, [MarkKind::Join, MarkKind::Suspect, MarkKind::Down]);
        assert_eq!(first.current(), None);
        let second = model.lifelines().node(testkit::gossip_addr(2)).unwrap();
        assert_eq!(second.current(), Some(PhaseKind::Live));
    }

    #[test]
    fn a_down_carries_how_long_the_exporter_had_been_silent() {
        let mut model = Model::new();
        let t = Instant::now();
        let addr = testkit::gossip_addr(1);
        let node = testkit::node_id(1, 0);
        let report = |at, outcome| ScrapeReport {
            addr,
            node,
            at,
            outcome,
            ready: None,
        };
        model.apply(
            snapshot_update(testkit::snapshot(2), t),
            t,
            SystemTime::UNIX_EPOCH,
        );
        model.apply(
            Update::Scrape(report(t + Duration::from_secs(1), Ok(Vec::new()))),
            t + Duration::from_secs(1),
            SystemTime::UNIX_EPOCH,
        );
        let failure = Err(crate::source::scrape::ScrapeError::Timeout);
        model.apply(
            Update::Scrape(report(t + Duration::from_secs(2), failure)),
            t + Duration::from_secs(2),
            SystemTime::UNIX_EPOCH,
        );
        let down_at = t + Duration::from_millis(4_900);
        let events = model.apply(
            snapshot_update(with_status(2, 1, MemberStatus::Down), down_at),
            down_at,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(
            events[0].kind,
            EventKind::Down {
                node,
                addr,
                exporter_silent: Some(Duration::from_millis(3_900)),
                superseded: false,
            }
        );
        // The lifeline turned suspect at the first failure and ended at Down.
        let line = model.lifelines().node(addr).unwrap();
        let kinds: Vec<_> = line.marks().iter().map(|m| m.kind).collect();
        assert_eq!(
            kinds,
            [
                lifelines::MarkKind::Join,
                lifelines::MarkKind::Suspect,
                lifelines::MarkKind::Down
            ]
        );
    }

    #[test]
    fn a_down_with_an_answering_or_absent_exporter_carries_no_silence() {
        let t = Instant::now();
        let addr = testkit::gossip_addr(1);
        let silent = |model: &Model| {
            let event = &model
                .events()
                .newest_first(Filter::Membership)
                .find(|e| matches!(e.kind, EventKind::Down { .. }))
                .unwrap()
                .kind;
            let EventKind::Down {
                exporter_silent, ..
            } = event
            else {
                unreachable!()
            };
            *exporter_silent
        };

        // No scrape at all.
        let mut model = Model::new();
        model.apply(
            snapshot_update(testkit::snapshot(2), t),
            t,
            SystemTime::UNIX_EPOCH,
        );
        model.apply(
            snapshot_update(with_status(2, 1, MemberStatus::Down), t),
            t,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(silent(&model), None);

        // The exporter still answers: gossip and the exporter disagree.
        let mut model = Model::new();
        model.apply(
            snapshot_update(testkit::snapshot(2), t),
            t,
            SystemTime::UNIX_EPOCH,
        );
        model.apply(
            Update::Scrape(ScrapeReport {
                addr,
                node: testkit::node_id(1, 0),
                at: t,
                outcome: Ok(Vec::new()),
                ready: None,
            }),
            t,
            SystemTime::UNIX_EPOCH,
        );
        model.apply(
            snapshot_update(with_status(2, 1, MemberStatus::Down), t),
            t,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(silent(&model), None);
    }

    #[test]
    fn a_scrape_that_succeeds_again_lifts_the_suspicion() {
        use lifelines::PhaseKind;
        let mut model = Model::new();
        let t = Instant::now();
        let addr = testkit::gossip_addr(1);
        model.apply(
            snapshot_update(testkit::snapshot(1), t),
            t,
            SystemTime::UNIX_EPOCH,
        );
        let report = |outcome| ScrapeReport {
            addr,
            node: testkit::node_id(1, 0),
            at: t,
            outcome,
            ready: None,
        };
        model.apply(
            Update::Scrape(report(Err(crate::source::scrape::ScrapeError::Timeout))),
            t,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(
            model.lifelines().node(addr).unwrap().current(),
            Some(PhaseKind::Suspect)
        );
        model.apply(
            Update::Scrape(report(Ok(Vec::new()))),
            t,
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(
            model.lifelines().node(addr).unwrap().current(),
            Some(PhaseKind::Live)
        );
    }

    #[test]
    fn a_member_that_started_after_the_lens_joins_during_discovery_and_ends_it() {
        let mut model = Model::new();
        let t = Instant::now();
        let started = wall0();
        model.set_started(started);
        // Member 1 predates the lens: the baseline.
        let mut events = model.apply(snapshot_update(testkit::snapshot(1), t), t, started);
        events.extend(model.apply(ownership_update(1), t, started));
        assert!(events.is_empty(), "the baseline raises nothing: {events:?}");
        assert!(model.discovering());
        // Member 2's process started a second after the lens did.
        let t = t + Duration::from_millis(500);
        let ms = u64::try_from(
            (started + Duration::from_secs(1))
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let members = vec![
            testkit::member(1, MemberStatus::Live),
            testkit::member_at(2, 0, ms, MemberStatus::Live),
        ];
        let snapshot = ClusterSnapshot::new("fixture", members, 0);
        let digest = testkit::ownership_digest(&snapshot, "it").unwrap();
        let events = model.apply(snapshot_update(snapshot, t), t, started);
        assert!(
            matches!(
                events.as_slice(),
                [Event {
                    kind: EventKind::Join { .. },
                    ..
                }]
            ),
            "a JOIN for the arrival only: {events:?}"
        );
        assert!(!model.discovering(), "an arrival ends discovery");
        let events = model.apply(Update::Ownership(digest), t, started);
        assert!(
            events
                .iter()
                .any(|event| matches!(event.kind, EventKind::View { .. })),
            "the digest after the arrival raises VIEW: {events:?}"
        );
    }

    #[test]
    fn digests_found_while_the_observer_discovers_members_raise_no_view() {
        let mut model = Model::new();
        let t = Instant::now();
        let mut events = model.apply(snapshot_update(testkit::snapshot(1), t), t, wall0());
        events.extend(model.apply(ownership_update(1), t, wall0()));
        let t = t + Duration::from_millis(500);
        events.extend(model.apply(snapshot_update(testkit::snapshot(2), t), t, wall0()));
        events.extend(model.apply(ownership_update(2), t, wall0()));
        assert!(events.is_empty(), "no VIEW or JOIN in {events:?}");
        assert!(model.discovering());
        assert_eq!(
            model.ownership("it").unwrap().eligible.len(),
            2,
            "the latest digest is held as the baseline"
        );
        // The set held still for the quiet span: the baseline view settles
        // quietly on the gossip hold, with no SETTLED event for it.
        let end = t + DISCOVERY_QUIET;
        let events = model.tick(end);
        assert!(events.is_empty(), "{events:?}");
        assert!(!model.discovering());
        assert_eq!(model.settled("it"), Some(false));
        let settled = t + GOSSIP_SETTLE;
        assert!(
            model.tick(settled).is_empty(),
            "no SETTLED for the baseline"
        );
        assert_eq!(model.settled("it"), Some(true));
        // A real change after discovery is a view.
        let events = model.apply(ownership_update(3), settled, wall0());
        assert_eq!(tags(&events), ["VIEW"]);
    }

    #[test]
    fn members_and_caches_found_while_discovering_raise_no_arrival_event() {
        use lifelines::{MarkKind, PhaseKind};
        let mut model = Model::new();
        let t = Instant::now();
        let bare = |index| testkit::member_with(index, 0, 1, MemberStatus::Live, &[]);
        let events = model.apply(
            snapshot_update(ClusterSnapshot::new("c", vec![bare(1)], 0), t),
            t,
            wall0(),
        );
        assert!(events.is_empty(), "{events:?}");
        // A second member and a cache on the first arrive while still discovering.
        let t = t + Duration::from_millis(500);
        let found = vec![
            testkit::member_with(
                1,
                0,
                1,
                MemberStatus::Live,
                &[("it", testkit::distributed(2))],
            ),
            testkit::member_with(
                2,
                0,
                1,
                MemberStatus::Live,
                &[("it", testkit::distributed(2))],
            ),
        ];
        let events = model.apply(
            snapshot_update(ClusterSnapshot::new("c", found, 0), t),
            t,
            wall0(),
        );
        assert!(model.discovering());
        assert!(events.is_empty(), "no JOIN or CACHE+ in {events:?}");
        assert_eq!(model.events().len(), 0);
        // The lifelines still start: both nodes run from the first sight.
        for index in [1, 2] {
            let line = model.lifelines().node(testkit::gossip_addr(index)).unwrap();
            assert_eq!(line.current(), Some(PhaseKind::Live));
            assert!(line.marks().iter().any(|mark| mark.kind == MarkKind::Join));
        }
        // After the quiet span, a member that arrives is a JOIN again.
        let t = t + DISCOVERY_QUIET;
        model.tick(t);
        assert!(!model.discovering());
        let events = model.apply(snapshot_update(testkit::snapshot(3), t), t, wall0());
        assert_eq!(tags(&events), ["JOIN"]);
    }

    #[test]
    fn a_member_set_that_keeps_changing_keeps_the_observer_discovering() {
        let mut model = Model::new();
        let t = Instant::now();
        assert!(model.discovering());
        for count in 1u8..=4 {
            let at = t + Duration::from_millis(1500) * u32::from(count);
            model.apply(snapshot_update(testkit::snapshot(count), at), at, wall0());
            model.tick(at + Duration::from_millis(1400));
            assert!(model.discovering(), "member {count} arrived 1.5 s ago");
        }
        let last = t + Duration::from_millis(6000);
        model.tick(last + DISCOVERY_QUIET);
        assert!(!model.discovering());
        // The phase is over for good, even when the set changes again.
        let again = last + DISCOVERY_QUIET + Duration::from_secs(1);
        model.apply(snapshot_update(testkit::snapshot(2), again), again, wall0());
        assert!(!model.discovering());
    }

    #[test]
    fn the_first_digest_raises_a_view_with_each_nodes_share() {
        let (mut model, t) = testkit::past_discovery(Instant::now());
        let events = model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        assert_eq!(tags(&events), ["VIEW"]);
        let digest = model.ownership("it").unwrap();
        let EventKind::View {
            cache,
            from,
            to,
            moved,
            deltas,
        } = &events[0].kind
        else {
            panic!("a view event");
        };
        assert_eq!(cache, "it");
        assert_eq!(*from, None);
        assert_eq!(*to, digest.view_hash);
        assert_eq!(*moved, 0);
        assert_eq!(deltas.len(), 3);
        assert_eq!(deltas.iter().map(|&(_, d)| d).sum::<i64>(), 2 * 65_536);
        assert!(deltas.iter().all(|&(_, d)| d > 0));
    }

    #[test]
    fn a_changed_view_raises_a_view_with_the_parts_moved_and_each_nodes_change() {
        let (mut model, t) = testkit::past_discovery(Instant::now());
        let three = testkit::snapshot(3);
        let four = testkit::snapshot(4);
        let first = testkit::ownership_digest(&three, "it").unwrap();
        let second = testkit::ownership_digest_after(&four, "it", Some(&first)).unwrap();
        let (first_hash, second_hash, moved) = (first.view_hash, second.view_hash, second.moved);
        model.apply(Update::Ownership(first), t, SystemTime::UNIX_EPOCH);
        let events = model.apply(Update::Ownership(second), t, SystemTime::UNIX_EPOCH);
        let EventKind::View {
            from,
            to,
            moved: reported,
            deltas,
            ..
        } = &events[0].kind
        else {
            panic!("a view event");
        };
        assert_eq!((*from, *to), (Some(first_hash), second_hash));
        assert_eq!(*reported, moved);
        assert!(moved > 0);
        assert_eq!(deltas.len(), 4);
        assert_eq!(deltas.iter().map(|&(_, d)| d).sum::<i64>(), 0);
    }

    #[test]
    fn an_unchanged_view_raises_nothing() {
        let mut model = Model::new();
        let t = Instant::now();
        model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        let events = model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        assert!(events.is_empty(), "{events:?}");
    }

    #[test]
    fn a_view_settles_once_with_the_time_it_took() {
        let (mut model, t) = testkit::past_discovery(Instant::now());
        model.apply(
            ownership_update(3),
            t,
            SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        );
        let events = model.tick(t + Duration::from_millis(2_999));
        assert!(events.is_empty(), "{events:?}");
        let events = model.tick(t + Duration::from_secs(4));
        assert_eq!(tags(&events), ["SETTLED"]);
        assert_eq!(
            events[0].kind,
            EventKind::Settled {
                cache: "it".into(),
                took: Duration::from_secs(4),
            }
        );
        assert_eq!(
            events[0].at,
            SystemTime::UNIX_EPOCH + Duration::from_secs(14)
        );
        assert!(
            model.tick(t + Duration::from_secs(9)).is_empty(),
            "settles once"
        );
    }

    #[test]
    fn an_update_after_the_hold_raises_the_settled_event_too() {
        let (mut model, t) = testkit::past_discovery(Instant::now());
        model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        let events = model.apply(
            snapshot_update(testkit::snapshot(3), t + Duration::from_secs(5)),
            t + Duration::from_secs(5),
            SystemTime::UNIX_EPOCH,
        );
        assert!(tags(&events).contains(&"SETTLED"));
    }

    #[test]
    fn a_new_view_before_the_settle_restarts_it() {
        let (mut model, t) = testkit::past_discovery(Instant::now());
        model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        model.apply(
            ownership_update(4),
            t + Duration::from_secs(2),
            SystemTime::UNIX_EPOCH,
        );
        let events = model.tick(t + Duration::from_secs(4));
        assert!(events.is_empty(), "{events:?}");
        let events = model.tick(t + Duration::from_secs(5));
        assert_eq!(tags(&events), ["SETTLED"]);
        assert_eq!(
            events[0].kind,
            EventKind::Settled {
                cache: "it".into(),
                took: Duration::from_secs(3),
            }
        );
    }

    #[test]
    fn the_settle_verdict_says_it_rests_on_gossip_alone() {
        let mut model = Model::new();
        let t = Instant::now();
        assert_eq!(model.settle("it"), None);
        model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        let waiting = model.settle("it").unwrap();
        assert!(waiting.gossip_only && !waiting.settled);
        model.tick(t + GOSSIP_SETTLE);
        assert!(model.settle("it").unwrap().settled);
    }

    #[test]
    fn ownership_gone_drops_the_cache_and_a_snapshot_does_not() {
        let (mut model, t) = testkit::past_discovery(Instant::now());
        model.apply(
            snapshot_update(testkit::snapshot(2), t),
            t,
            SystemTime::UNIX_EPOCH,
        );
        model.apply(ownership_update(2), t, SystemTime::UNIX_EPOCH);
        assert!(model.ownership("it").is_some());
        assert!(model.lifelines().cache("it").is_some());

        // No snapshot drops ownership: the worker alone decides.
        let down = ClusterSnapshot::new(
            "fixture",
            vec![
                testkit::member(1, MemberStatus::Down),
                testkit::member(2, MemberStatus::Left),
            ],
            0,
        );
        model.apply(snapshot_update(down, t), t, SystemTime::UNIX_EPOCH);
        assert!(model.ownership("it").is_some());

        let events = model.apply(
            Update::OwnershipGone("it".into()),
            t,
            SystemTime::UNIX_EPOCH,
        );
        assert!(events.is_empty(), "{events:?}");
        assert!(model.ownership("it").is_none());
        assert_eq!(model.ownership_digests().count(), 0);
        assert_eq!(model.settled("it"), None);
        assert!(model.lifelines().cache("it").is_none());
        assert!(
            model.tick(t + Duration::from_secs(10)).is_empty(),
            "nothing left to settle"
        );
    }

    #[test]
    fn ownership_gone_for_an_unknown_cache_changes_nothing() {
        let mut model = Model::new();
        let t = Instant::now();
        model.apply(ownership_update(2), t, SystemTime::UNIX_EPOCH);
        model.apply(
            Update::OwnershipGone("other".into()),
            t,
            SystemTime::UNIX_EPOCH,
        );
        assert!(model.ownership("it").is_some());
    }

    #[test]
    fn a_digest_after_ownership_gone_is_a_first_view() {
        let (mut model, t) = testkit::past_discovery(Instant::now());
        model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        model.apply(
            Update::OwnershipGone("it".into()),
            t,
            SystemTime::UNIX_EPOCH,
        );
        let events = model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        assert_eq!(tags(&events), ["VIEW"]);
        assert!(matches!(events[0].kind, EventKind::View { from: None, .. }));
    }

    #[test]
    fn a_restart_then_the_old_incarnation_going_down_keeps_the_line_live() {
        use lifelines::PhaseKind;
        let mut model = Model::new();
        let t = Instant::now();
        let addr = testkit::gossip_addr(1);
        let inc = |incarnation, status| testkit::member_at(1, 0, incarnation, status);
        let step = |model: &mut Model, members: Vec<sundog::observe::Member>| {
            model.apply(
                snapshot_update(ClusterSnapshot::new("c", members, 0), t),
                t,
                SystemTime::UNIX_EPOCH,
            )
        };
        step(&mut model, vec![inc(1, MemberStatus::Live)]);
        let events = step(
            &mut model,
            vec![inc(1, MemberStatus::Live), inc(2, MemberStatus::Live)],
        );
        assert_eq!(tags(&events), ["RESTART"]);
        let events = step(
            &mut model,
            vec![inc(1, MemberStatus::Down), inc(2, MemberStatus::Live)],
        );
        assert_eq!(tags(&events), ["DOWN"]);
        assert_eq!(
            model.lifelines().node(addr).unwrap().current(),
            Some(PhaseKind::Live)
        );

        // The same for a new node id at the address.
        let mut model = Model::new();
        step(&mut model, vec![inc(1, MemberStatus::Live)]);
        model.tick(t + DISCOVERY_QUIET);
        let rejoined = testkit::member_at(1, 1, 5, MemberStatus::Live);
        let events = step(
            &mut model,
            vec![inc(1, MemberStatus::Live), rejoined.clone()],
        );
        assert_eq!(tags(&events), ["REJOIN"]);
        step(&mut model, vec![inc(1, MemberStatus::Left), rejoined]);
        assert_eq!(
            model.lifelines().node(addr).unwrap().current(),
            Some(PhaseKind::Live)
        );
    }

    #[test]
    fn a_member_that_comes_back_from_down_is_live_on_its_line_again() {
        use lifelines::PhaseKind;
        let mut model = Model::new();
        let t = Instant::now();
        let addr = testkit::gossip_addr(1);
        let step = |model: &mut Model, status| {
            model.apply(
                snapshot_update(with_status(1, 1, status), t),
                t,
                SystemTime::UNIX_EPOCH,
            )
        };
        step(&mut model, MemberStatus::Live);
        step(&mut model, MemberStatus::Down);
        assert_eq!(model.lifelines().node(addr).unwrap().current(), None);
        let events = step(&mut model, MemberStatus::Live);
        assert_eq!(tags(&events), ["UP"]);
        assert_eq!(
            model.lifelines().node(addr).unwrap().current(),
            Some(PhaseKind::Live)
        );
    }

    #[test]
    fn count_to_f64_is_exact_for_the_counts_the_lens_holds() {
        assert!(count_to_f64(0).abs() < f64::EPSILON);
        assert!((count_to_f64(65_536) - 65_536.0).abs() < f64::EPSILON);
        assert!((count_to_f64(2 * 65_536) - 131_072.0).abs() < f64::EPSILON);
    }

    #[test]
    fn the_fixture_model_orders_departure_view_and_left() {
        let model = testkit::fixture_model(Instant::now());
        let tags: Vec<_> = model.events().iter().map(|e| e.kind.tag()).collect();
        let last = |tag| tags.iter().rposition(|t| *t == tag).unwrap();
        assert!(last("LEAVE") < last("LEFT"));
        assert!(last("DOWN") > last("JOIN"));
        assert_eq!(model.events().len(), tags.len());
        assert!(model.lifelines().cache("it").is_some());
    }
}
