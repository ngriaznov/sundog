//! The model: everything the interface shows, folded from [`Update`]s.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use smol_str::SmolStr;
use sundog::observe::ClusterSnapshot;

use crate::source::{ScrapeReport, Update};

pub mod derive;
pub mod digest;
pub mod events;
pub mod lifelines;
pub mod ownership;
pub mod series;
pub mod slots;
#[doc(hidden)]
pub mod testkit;

pub use digest::{ModelDigest, digest};
pub use events::{Event, EventKind};
pub use ownership::OwnershipDigest;
pub use slots::{Slot, Slots};

/// With no metrics, a cache counts as settled once its view has held this
/// long.
pub const GOSSIP_SETTLE: Duration = Duration::from_secs(3);

/// The state the interface draws. `Clone` so the frozen display can keep a
/// copy while collection continues.
#[derive(Debug, Clone, Default)]
pub struct Model {
    slots: Slots,
    snapshot: Option<Arc<ClusterSnapshot>>,
    ownership: BTreeMap<SmolStr, OwnershipDigest>,
    view_since: BTreeMap<SmolStr, Instant>,
    scrapes: BTreeMap<SocketAddr, ScrapeReport>,
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
    /// and returns the events it raises.
    ///
    /// A snapshot replaces the member view and gives every gossip address a
    /// slot. An ownership digest replaces its cache's digest and restarts the
    /// cache's settling clock when the view hash changed. A scrape replaces
    /// the node's last report. The model derives no events yet, so the list
    /// is empty.
    pub fn apply(&mut self, update: Update, now: Instant, wall: SystemTime) -> Vec<Event> {
        self.now = Some(now);
        self.wall = Some(wall);
        match update {
            Update::Snapshot(snapshot, _) => {
                for member in &snapshot.members {
                    self.slots.assign(member.peer.gossip_addr);
                }
                self.snapshot = Some(snapshot);
            }
            Update::Ownership(digest) => {
                let moved = self
                    .ownership
                    .get(&digest.cache)
                    .is_none_or(|held| held.view_hash != digest.view_hash);
                if moved {
                    self.view_since.insert(digest.cache.clone(), now);
                }
                self.ownership.insert(digest.cache.clone(), digest);
            }
            Update::Scrape(report) => {
                self.scrapes.insert(report.addr, report);
            }
        }
        Vec::new()
    }

    /// Advances the model's clock to `now` and returns the events that time
    /// alone raises: none yet.
    pub fn tick(&mut self, now: Instant) -> Vec<Event> {
        self.now = Some(now);
        Vec::new()
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

    /// The monotonic time of the last update or tick.
    #[must_use]
    pub const fn now(&self) -> Option<Instant> {
        self.now
    }

    /// The wall-clock time of the last update.
    #[must_use]
    pub const fn wall(&self) -> Option<SystemTime> {
        self.wall
    }

    /// Whether `cache`'s view has held for [`GOSSIP_SETTLE`], judged from
    /// gossip alone; `None` for a cache with no ownership digest.
    #[must_use]
    pub fn settled(&self, cache: &str) -> Option<bool> {
        let since = self.view_since.get(cache)?;
        let now = self.now?;
        Some(now.saturating_duration_since(*since) >= GOSSIP_SETTLE)
    }
}

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
        assert!(events.is_empty());
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
}
