//! The model: everything the interface shows, folded from [`Update`]s.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::observe::{ClusterSnapshot, MemberStatus};
use sundog::store::Mode;

use crate::source::{ScrapeReport, Update};
use derive::NodeProgress;

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

/// How long a node's `sundog_live_peers` has to disagree with the observer's
/// count before the PEERS column turns amber.
pub const PEERS_GRACE: Duration = Duration::from_secs(3);

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
    /// clock. The
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
        let mut kinds = match update {
            Update::Snapshot(snapshot, _) => self.apply_snapshot(snapshot, now),
            Update::Ownership(digest) => self.apply_ownership(digest, now),
            Update::OwnershipGone(cache) => {
                self.forget_cache(&cache);
                Vec::new()
            }
            Update::Scrape(report) => self.apply_scrape(report, now),
        };
        self.refresh_peers(now);
        kinds.extend(self.settled_events(now));
        self.finish(kinds, now, wall)
    }

    /// Advances the model's clocks to `now` and returns the events the passing
    /// time raises: `SETTLED` for a cache whose view has now held long enough.
    /// A tick before the first update leaves the wall clock unset.
    pub fn tick(&mut self, now: Instant) -> Vec<Event> {
        let wall = self.wall_at(now);
        self.now = Some(now);
        self.wall = self.wall.map(|_| wall);
        let kinds = self.settled_events(now);
        self.finish(kinds, now, wall)
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

    /// The settling verdict for `cache`. Nodes whose exporter answered since
    /// the view changed vote: the cache is settled once each of them reports
    /// the parts the observer computes for it and has pulled none in for
    /// [`derive::QUIET_SCRAPES`] scrapes taken since the view changed. With no such node the verdict rests
    /// on gossip alone, and the cache is settled once its view has held for
    /// [`GOSSIP_SETTLE`]. `None` for a cache with no ownership digest.
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

    /// The metrics of the live member `node`.
    fn live_metrics_of(&self, node: NodeId) -> Option<&NodeMetrics> {
        let member = self
            .snapshot
            .as_ref()?
            .members
            .iter()
            .find(|member| member.peer.node == node && member.status.is_live())?;
        self.metrics
            .get(&member.peer.gossip_addr)
            .filter(|metrics| metrics.node() == node)
    }

    /// What the settling test knows about `node` for the view that began at
    /// `since`: nothing, unless the node's exporter answered after `since` and
    /// reports `digest.cache`.
    fn progress(&self, digest: &OwnershipDigest, node: NodeId, since: Instant) -> NodeProgress {
        let silent = NodeProgress {
            agrees: None,
            quiet_scrapes: 0,
        };
        let Some(metrics) = self.live_metrics_of(node) else {
            return silent;
        };
        let fresh = self
            .scrapes
            .values()
            .any(|report| report.node == node && report.outcome.is_ok() && report.at >= since);
        let Some(reported) = metrics.owned_parts(&digest.cache).filter(|_| fresh) else {
            return silent;
        };
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

    fn apply_snapshot(&mut self, snapshot: Arc<ClusterSnapshot>, now: Instant) -> Vec<EventKind> {
        let mut kinds = diff_snapshots(self.snapshot.as_deref(), &snapshot);
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
        for member in &snapshot.members {
            self.slots.assign(member.peer.gossip_addr);
        }
        self.snapshot = Some(snapshot);
        kinds
    }

    fn apply_ownership(&mut self, digest: OwnershipDigest, now: Instant) -> Vec<EventKind> {
        let held = self.ownership.get(&digest.cache);
        let mut kinds = Vec::new();
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
                .any(|member| member.peer.node == node && member.status.is_live())
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
            Err(error) if !error.is_mapping() => self.lifelines.suspect(addr, now),
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
            .map(|kind| {
                self.lifelines.record(&kind, now);
                let event = Event { at: wall, kind };
                self.log.push(event.clone());
                event
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
    fn a_snapshot_gives_each_gossip_address_a_slot_in_member_order() {
        let mut model = Model::new();
        let now = Instant::now();
        let events = model.apply(
            snapshot_update(testkit::snapshot(3), now),
            now,
            SystemTime::UNIX_EPOCH,
        );
        let tags: Vec<_> = events.iter().map(|event| event.kind.tag()).collect();
        assert_eq!(tags, ["JOIN", "JOIN", "JOIN"]);
        assert!(
            events
                .iter()
                .all(|event| event.at == SystemTime::UNIX_EPOCH)
        );
        assert_eq!(model.snapshot().unwrap().members.len(), 3);
        assert_eq!(model.slots().len(), 3);
        assert_eq!(
            model.slots().get(testkit::gossip_addr(2)).unwrap().label,
            "n2"
        );
        assert_eq!(model.wall(), Some(SystemTime::UNIX_EPOCH));
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
        let mut model = Model::new();
        let start = Instant::now();
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
        assert!(model.tick(now).is_empty());
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
        assert_eq!(logged, ["JOIN", "JOIN", "JOIN", "LEAVE", "LEFT"]);
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
        assert_eq!(kinds, [MarkKind::Join, MarkKind::Down]);
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
    fn the_first_digest_raises_a_view_with_each_nodes_share() {
        let mut model = Model::new();
        let t = Instant::now();
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
        let mut model = Model::new();
        let t = Instant::now();
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
        assert!(events.is_empty());
    }

    #[test]
    fn a_view_settles_once_with_the_time_it_took() {
        let mut model = Model::new();
        let t = Instant::now();
        model.apply(
            ownership_update(3),
            t,
            SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        );
        assert!(model.tick(t + Duration::from_millis(2_999)).is_empty());
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
        let mut model = Model::new();
        let t = Instant::now();
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
        let mut model = Model::new();
        let t = Instant::now();
        model.apply(ownership_update(3), t, SystemTime::UNIX_EPOCH);
        model.apply(
            ownership_update(4),
            t + Duration::from_secs(2),
            SystemTime::UNIX_EPOCH,
        );
        assert!(model.tick(t + Duration::from_secs(4)).is_empty());
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
        let mut model = Model::new();
        let t = Instant::now();
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
        assert!(events.is_empty());
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
        let mut model = Model::new();
        let t = Instant::now();
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
