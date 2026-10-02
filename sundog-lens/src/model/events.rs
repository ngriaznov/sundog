//! The events the model raises as the cluster changes.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::observe::{ClusterSnapshot, Member, MemberStatus};
use sundog::store::Mode;
use sundog::wire::PROTOCOL_VERSION;

/// How many events the log keeps.
pub const LOG_CAPACITY: usize = 2000;

/// One event: what happened and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The wall-clock time of the update that raised the event.
    pub at: SystemTime,
    /// What happened.
    pub kind: EventKind,
}

/// What happened. A node is named by its id and gossip address; the view
/// resolves them to a slot label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    /// `JOIN`: a new member appeared.
    Join {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
        /// Its wire protocol.
        protocol: u16,
        /// The caches it advertises.
        caches: BTreeMap<SmolStr, Mode>,
    },
    /// `LEAVE`: a member gossips a graceful departure.
    Leave {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
        /// Whether a later incarnation holds the address: the departure
        /// concerns an older process, not the node now at the address.
        superseded: bool,
    },
    /// `LEFT`: a departing member is gone.
    Left {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
        /// Whether a later incarnation holds the address.
        superseded: bool,
    },
    /// `DOWN`: a live member dropped with no departure.
    Down {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
        /// How long its exporter had been silent, when known.
        exporter_silent: Option<Duration>,
        /// Whether a later incarnation holds the address.
        superseded: bool,
    },
    /// `UP`: a member that was down is live again, as when chitchat revives a
    /// node whose heartbeat resumes after a stall or a partition.
    Up {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
    },
    /// `REJOIN`: a new node id at a known gossip address.
    Rejoin {
        /// The new node.
        node: NodeId,
        /// The shared gossip address.
        addr: SocketAddr,
        /// The node that held the address before.
        previous: NodeId,
        /// The caches it advertises.
        caches: BTreeMap<SmolStr, Mode>,
    },
    /// `RESTART`: the same node id at a higher incarnation.
    Restart {
        /// The member.
        node: NodeId,
        /// Its gossip address.
        addr: SocketAddr,
    },
    /// `CACHE+`: a live member opened a cache.
    CacheAdded {
        /// The member.
        node: NodeId,
        /// The cache name.
        cache: SmolStr,
        /// Its mode.
        mode: Mode,
    },
    /// `CACHE-`: a live member closed a cache.
    CacheRemoved {
        /// The member.
        node: NodeId,
        /// The cache name.
        cache: SmolStr,
    },
    /// `CONFLICT`: live members advertise different modes for one cache.
    Conflict {
        /// The cache name.
        cache: SmolStr,
        /// Each live advertiser and its mode.
        modes: Vec<(NodeId, Mode)>,
    },
    /// `PROTO`: a member speaks a protocol other than the lens's, or below
    /// the minimum.
    Proto {
        /// The member.
        node: NodeId,
        /// Its protocol.
        protocol: u16,
    },
    /// `VIEW`: a cache's ownership view changed. The hashes are equal when only
    /// the owner count changed.
    View {
        /// The cache name.
        cache: SmolStr,
        /// The previous view hash, if there was one.
        from: Option<u64>,
        /// The new view hash.
        to: u64,
        /// Owner slots that changed hands.
        moved: usize,
        /// Each node's change in parts owned.
        deltas: Vec<(NodeId, i64)>,
    },
    /// `SETTLED`: a cache settled after a view change.
    Settled {
        /// The cache name.
        cache: SmolStr,
        /// Time from the view change to settling.
        took: Duration,
    },
    /// `XFER`: a node's state-transfer rate became nonzero.
    Xfer {
        /// The node.
        node: NodeId,
    },
    /// `DROP`: a node dropped frames bound for a peer.
    Drop {
        /// The sending node.
        node: NodeId,
        /// The peer's node id as the exporter labels it.
        peer: SmolStr,
        /// How many frames.
        frames: u64,
    },
    /// `READY`: a node's `/readyz` turned ready.
    Ready {
        /// The node.
        node: NodeId,
    },
    /// `UNREADY`: a node's `/readyz` turned not ready.
    Unready {
        /// The node.
        node: NodeId,
    },
    /// `UNREACHABLE`: scrapes fail while gossip lists the node live.
    Unreachable {
        /// The node.
        node: NodeId,
    },
    /// `EXPORTER`: an exporter came or went, or its mapping is wrong.
    Exporter {
        /// The node the exporter is mapped to, as it was when the event
        /// happened.
        node: NodeId,
        /// The gossip address of the mapped member.
        addr: SocketAddr,
        /// What happened.
        detail: String,
    },
}

/// The group of events an [`EventKind`] belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Category {
    /// Members joining, leaving, restarting, and cache or protocol changes.
    Membership,
    /// Ownership view changes and settling.
    Ownership,
    /// State transfer and dropped frames.
    Traffic,
    /// Readiness and exporter health.
    Exporter,
}

impl EventKind {
    /// The upper-case tag the event log shows: `JOIN`, `VIEW`, `CACHE+`.
    #[must_use]
    pub const fn tag(&self) -> &'static str {
        match self {
            Self::Join { .. } => "JOIN",
            Self::Leave { .. } => "LEAVE",
            Self::Left { .. } => "LEFT",
            Self::Down { .. } => "DOWN",
            Self::Up { .. } => "UP",
            Self::Rejoin { .. } => "REJOIN",
            Self::Restart { .. } => "RESTART",
            Self::CacheAdded { .. } => "CACHE+",
            Self::CacheRemoved { .. } => "CACHE-",
            Self::Conflict { .. } => "CONFLICT",
            Self::Proto { .. } => "PROTO",
            Self::View { .. } => "VIEW",
            Self::Settled { .. } => "SETTLED",
            Self::Xfer { .. } => "XFER",
            Self::Drop { .. } => "DROP",
            Self::Ready { .. } => "READY",
            Self::Unready { .. } => "UNREADY",
            Self::Unreachable { .. } => "UNREACHABLE",
            Self::Exporter { .. } => "EXPORTER",
        }
    }

    /// The filter group of the event.
    #[must_use]
    pub const fn category(&self) -> Category {
        match self {
            Self::Join { .. }
            | Self::Leave { .. }
            | Self::Left { .. }
            | Self::Down { .. }
            | Self::Up { .. }
            | Self::Rejoin { .. }
            | Self::Restart { .. }
            | Self::CacheAdded { .. }
            | Self::CacheRemoved { .. }
            | Self::Conflict { .. }
            | Self::Proto { .. } => Category::Membership,
            Self::View { .. } | Self::Settled { .. } => Category::Ownership,
            Self::Xfer { .. } | Self::Drop { .. } => Category::Traffic,
            Self::Ready { .. }
            | Self::Unready { .. }
            | Self::Unreachable { .. }
            | Self::Exporter { .. } => Category::Exporter,
        }
    }

    /// The gossip address the event names, when it names one: the
    /// membership events and the exporter's own.
    #[must_use]
    pub const fn addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Join { addr, .. }
            | Self::Leave { addr, .. }
            | Self::Left { addr, .. }
            | Self::Down { addr, .. }
            | Self::Up { addr, .. }
            | Self::Rejoin { addr, .. }
            | Self::Restart { addr, .. }
            | Self::Exporter { addr, .. } => Some(*addr),
            _ => None,
        }
    }

    /// The node the event is about, when it is about exactly one.
    #[must_use]
    pub const fn node(&self) -> Option<NodeId> {
        match self {
            Self::Join { node, .. }
            | Self::Leave { node, .. }
            | Self::Left { node, .. }
            | Self::Down { node, .. }
            | Self::Up { node, .. }
            | Self::Rejoin { node, .. }
            | Self::Restart { node, .. }
            | Self::CacheAdded { node, .. }
            | Self::CacheRemoved { node, .. }
            | Self::Proto { node, .. }
            | Self::Xfer { node }
            | Self::Drop { node, .. }
            | Self::Ready { node }
            | Self::Unready { node }
            | Self::Unreachable { node }
            | Self::Exporter { node, .. } => Some(*node),
            Self::Conflict { .. } | Self::View { .. } | Self::Settled { .. } => None,
        }
    }
}

/// The event filter the `f` key cycles through.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Filter {
    /// Every event.
    #[default]
    All,
    /// [`Category::Membership`] events.
    Membership,
    /// [`Category::Ownership`] events.
    Ownership,
    /// [`Category::Traffic`] events.
    Traffic,
    /// [`Category::Exporter`] events.
    Exporter,
}

impl Filter {
    /// The filter after this one, wrapping from `Exporter` to `All`.
    #[must_use]
    pub const fn next(self) -> Self {
        match self {
            Self::All => Self::Membership,
            Self::Membership => Self::Ownership,
            Self::Ownership => Self::Traffic,
            Self::Traffic => Self::Exporter,
            Self::Exporter => Self::All,
        }
    }

    /// The lower-case name the filter bar shows.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Membership => "membership",
            Self::Ownership => "ownership",
            Self::Traffic => "traffic",
            Self::Exporter => "exporter",
        }
    }

    /// Whether an event of `kind` passes the filter.
    #[must_use]
    pub const fn matches(self, kind: &EventKind) -> bool {
        match self {
            Self::All => true,
            Self::Membership => matches!(kind.category(), Category::Membership),
            Self::Ownership => matches!(kind.category(), Category::Ownership),
            Self::Traffic => matches!(kind.category(), Category::Traffic),
            Self::Exporter => matches!(kind.category(), Category::Exporter),
        }
    }
}

/// How long after a node's `JOIN` or `REJOIN` a cache it opens still belongs
/// to that row: a node advertises each cache as it opens it, and opening them
/// and pulling their state takes it several seconds.
pub const FOLD_WITHIN: Duration = Duration::from_secs(15);

/// The most recent [`LOG_CAPACITY`] events, oldest dropped first.
#[derive(Debug, Clone, Default)]
pub struct EventLog {
    events: VecDeque<Event>,
}

impl EventLog {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends `event`, dropping the oldest when the log is full.
    pub fn push(&mut self, event: Event) {
        if self.events.len() == LOG_CAPACITY {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    /// Adds the cache `cache` opened in `mode` by `node` at `at` to the
    /// node's latest `JOIN` or `REJOIN` row, when that row is at most
    /// [`FOLD_WITHIN`] old and does not name the cache yet. Returns whether it
    /// did: a `CACHE+` that folds is part of the arrival and raises no row of
    /// its own.
    pub fn fold_cache(
        &mut self,
        node: NodeId,
        cache: &SmolStr,
        mode: Mode,
        at: SystemTime,
    ) -> bool {
        let Some(event) = self.events.iter_mut().rev().find(|event| {
            matches!(
                &event.kind,
                EventKind::Join { node: held, .. } | EventKind::Rejoin { node: held, .. }
                    if *held == node
            )
        }) else {
            return false;
        };
        if at.duration_since(event.at).unwrap_or_default() > FOLD_WITHIN {
            return false;
        }
        match &mut event.kind {
            EventKind::Join { caches, .. } | EventKind::Rejoin { caches, .. }
                if !caches.contains_key(cache) =>
            {
                caches.insert(cache.clone(), mode);
                true
            }
            _ => false,
        }
    }

    /// The events of the slot at gossip address `addr`, newest first: those
    /// that name the address and those about a node that held it, whichever
    /// process it was, so a restarted node's row shows its whole story
    /// (`JOIN`, `DOWN`, `REJOIN`). `current` is the node there now, which
    /// counts before any event names it.
    #[must_use]
    pub fn of_address(&self, addr: SocketAddr, current: NodeId) -> Vec<&Event> {
        let mut held: Vec<NodeId> = vec![current];
        for event in &self.events {
            if event.kind.addr() == Some(addr)
                && let Some(node) = event.kind.node()
                && !held.contains(&node)
            {
                held.push(node);
            }
        }
        self.events
            .iter()
            .rev()
            .filter(|event| {
                event.kind.addr() == Some(addr)
                    || event.kind.node().is_some_and(|node| held.contains(&node))
            })
            .collect()
    }

    /// How many events the log holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// The events, oldest first.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Event> + ExactSizeIterator {
        self.events.iter()
    }

    /// The events that pass `filter`, newest first.
    pub fn newest_first(&self, filter: Filter) -> impl Iterator<Item = &Event> {
        self.events
            .iter()
            .rev()
            .filter(move |event| filter.matches(&event.kind))
    }
}

/// The events that turn `prev` into `next`, in member order (ascending node id,
/// then incarnation) with the conflicts last.
///
/// A member that appears live or departing raises `JOIN`, or `RESTART` when
/// an older incarnation of its node id is in `prev`, or `REJOIN` when another
/// node id held its gossip address, followed by `PROTO` when its protocol is
/// not the lens's and `LEAVE` when it arrives departing. A member that was
/// already in `prev` raises `LEAVE` on `Live` to `Departing`, `LEFT` on
/// `Departing` to `Left`, both on `Live` straight to `Left`, `DOWN` on a
/// live status to `Down`, and `UP` on `Down` to `Live` (followed by `LEAVE`
/// when it turns `Departing`). A `LEAVE`, `LEFT` or `DOWN` is `superseded`
/// when `next` holds a later incarnation at the member's gossip address: the
/// older process is going while the node at the address lives. Between two `Live` snapshots of one member a cache
/// that appears raises `CACHE+` and a cache that disappears `CACHE-`. A cache
/// whose live members advertise different modes raises `CONFLICT` when the
/// disagreement is new or changes. A member that appears already `Left` or
/// `Down` and a member that leaves `prev` raise nothing. A `DOWN` carries no exporter silence: the model adds
/// it from the scrapes.
#[must_use]
pub fn diff_snapshots(prev: Option<&ClusterSnapshot>, next: &ClusterSnapshot) -> Vec<EventKind> {
    let before: &[Member] = prev.map_or(&[], |snapshot| snapshot.members.as_slice());
    let mut events = Vec::new();
    for member in &next.members {
        let peer = &member.peer;
        let old = before
            .iter()
            .find(|old| old.peer.node == peer.node && old.peer.incarnation == peer.incarnation);
        match old {
            None => arrival(before, member, next, &mut events),
            Some(old) => {
                transition(old, member, next, &mut events);
                cache_changes(old, member, &mut events);
            }
        }
    }
    let held = conflicts(prev);
    for (cache, modes) in conflicts(Some(next)) {
        if held.get(&cache) != Some(&modes) {
            events.push(EventKind::Conflict { cache, modes });
        }
    }
    events
}

fn arrival(
    before: &[Member],
    member: &Member,
    next: &ClusterSnapshot,
    events: &mut Vec<EventKind>,
) {
    if !member.status.is_live() {
        return;
    }
    let peer = &member.peer;
    let (node, addr) = (peer.node, peer.gossip_addr);
    let restarted = before
        .iter()
        .any(|old| old.peer.node == node && old.peer.incarnation < peer.incarnation);
    let previous = before
        .iter()
        .filter(|old| old.peer.gossip_addr == addr && old.peer.node != node)
        .max_by_key(|old| old.peer.incarnation)
        .map(|old| old.peer.node);
    events.push(if restarted {
        EventKind::Restart { node, addr }
    } else if let Some(previous) = previous {
        EventKind::Rejoin {
            node,
            addr,
            previous,
            caches: member.caches.clone(),
        }
    } else {
        EventKind::Join {
            node,
            addr,
            protocol: peer.protocol,
            caches: member.caches.clone(),
        }
    });
    if peer.protocol != PROTOCOL_VERSION {
        events.push(EventKind::Proto {
            node,
            protocol: peer.protocol,
        });
    }
    if member.status == MemberStatus::Departing {
        events.push(EventKind::Leave {
            node,
            addr,
            superseded: superseded(next, member),
        });
    }
}

fn transition(old: &Member, member: &Member, next: &ClusterSnapshot, events: &mut Vec<EventKind>) {
    let (node, addr) = (member.peer.node, member.peer.gossip_addr);
    let superseded = superseded(next, member);
    match (old.status, member.status) {
        (MemberStatus::Live, MemberStatus::Departing) => {
            events.push(EventKind::Leave {
                node,
                addr,
                superseded,
            });
        }
        (MemberStatus::Departing, MemberStatus::Left) => {
            events.push(EventKind::Left {
                node,
                addr,
                superseded,
            });
        }
        (MemberStatus::Live, MemberStatus::Left) => {
            events.push(EventKind::Leave {
                node,
                addr,
                superseded,
            });
            events.push(EventKind::Left {
                node,
                addr,
                superseded,
            });
        }
        (MemberStatus::Live | MemberStatus::Departing, MemberStatus::Down) => {
            events.push(EventKind::Down {
                node,
                addr,
                exporter_silent: None,
                superseded,
            });
        }
        (MemberStatus::Down, MemberStatus::Live) => events.push(EventKind::Up { node, addr }),
        (MemberStatus::Down, MemberStatus::Departing) => {
            events.push(EventKind::Up { node, addr });
            events.push(EventKind::Leave {
                node,
                addr,
                superseded,
            });
        }
        _ => {}
    }
}

/// Whether `next` holds a later record than `member` at its gossip address:
/// the greater (incarnation, node id), the order in which the newest record
/// stands for the address.
fn superseded(next: &ClusterSnapshot, member: &Member) -> bool {
    let peer = &member.peer;
    next.members.iter().any(|other| {
        other.peer.gossip_addr == peer.gossip_addr
            && (other.peer.incarnation, other.peer.node) > (peer.incarnation, peer.node)
    })
}

fn cache_changes(old: &Member, member: &Member, events: &mut Vec<EventKind>) {
    if old.status != MemberStatus::Live || member.status != MemberStatus::Live {
        return;
    }
    let node = member.peer.node;
    for (cache, mode) in &old.caches {
        if member.caches.get(cache) != Some(mode) {
            events.push(EventKind::CacheRemoved {
                node,
                cache: cache.clone(),
            });
        }
    }
    for (cache, mode) in &member.caches {
        if old.caches.get(cache) != Some(mode) {
            events.push(EventKind::CacheAdded {
                node,
                cache: cache.clone(),
                mode: *mode,
            });
        }
    }
}

/// Each cache the live members advertise under more than one mode, with every
/// live advertiser and its mode, ascending by node.
fn conflicts(snapshot: Option<&ClusterSnapshot>) -> BTreeMap<SmolStr, Vec<(NodeId, Mode)>> {
    let mut by_cache: BTreeMap<SmolStr, Vec<(NodeId, Mode)>> = BTreeMap::new();
    for member in snapshot.into_iter().flat_map(|s| &s.members) {
        if member.status != MemberStatus::Live {
            continue;
        }
        for (cache, mode) in &member.caches {
            by_cache
                .entry(cache.clone())
                .or_default()
                .push((member.peer.node, *mode));
        }
    }
    by_cache.retain(|_, modes| modes.iter().any(|&(_, mode)| mode != modes[0].1));
    for modes in by_cache.values_mut() {
        modes.sort_by_key(|&(node, _)| node);
    }
    by_cache
}

#[cfg(test)]
mod tests {
    use sundog::store::Mode;

    use crate::model::testkit::{self, distributed, member_with};

    use super::*;

    fn snapshot(members: Vec<Member>) -> ClusterSnapshot {
        ClusterSnapshot::new("c", members, 0)
    }

    fn live(index: u8) -> Member {
        testkit::member(index, MemberStatus::Live)
    }

    fn node(index: u8) -> NodeId {
        testkit::node_id(index, 0)
    }

    fn addr(index: u8) -> SocketAddr {
        testkit::gossip_addr(index)
    }

    #[test]
    fn a_member_in_the_first_snapshot_joins_with_its_protocol_and_caches() {
        let first = snapshot(vec![live(1), live(2)]);
        let events = diff_snapshots(None, &first);
        assert_eq!(
            events,
            vec![
                EventKind::Join {
                    node: node(1),
                    addr: addr(1),
                    protocol: PROTOCOL_VERSION,
                    caches: first.members[0].caches.clone(),
                },
                EventKind::Join {
                    node: node(2),
                    addr: addr(2),
                    protocol: PROTOCOL_VERSION,
                    caches: first.members[1].caches.clone(),
                },
            ]
        );
    }

    #[test]
    fn an_unchanged_snapshot_raises_nothing() {
        let both = snapshot(vec![live(1), live(2)]);
        assert!(diff_snapshots(Some(&both), &both.clone()).is_empty());
    }

    #[test]
    fn a_member_that_appears_gone_raises_nothing() {
        let first = snapshot(vec![
            live(1),
            testkit::member(2, MemberStatus::Down),
            testkit::member(3, MemberStatus::Left),
        ]);
        let events = diff_snapshots(None, &first);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EventKind::Join { .. }));
    }

    #[test]
    fn a_member_that_appears_departing_joins_and_leaves() {
        let next = snapshot(vec![testkit::member(1, MemberStatus::Departing)]);
        let tags: Vec<_> = diff_snapshots(None, &next)
            .iter()
            .map(EventKind::tag)
            .collect();
        assert_eq!(tags, ["JOIN", "LEAVE"]);
    }

    #[test]
    fn a_graceful_leave_raises_leave_then_left() {
        let live_one = snapshot(vec![live(1)]);
        let departing = snapshot(vec![testkit::member(1, MemberStatus::Departing)]);
        let left = snapshot(vec![testkit::member(1, MemberStatus::Left)]);
        assert_eq!(
            diff_snapshots(Some(&live_one), &departing),
            vec![EventKind::Leave {
                node: node(1),
                addr: addr(1),
                superseded: false
            }]
        );
        assert_eq!(
            diff_snapshots(Some(&departing), &left),
            vec![EventKind::Left {
                node: node(1),
                addr: addr(1),
                superseded: false
            }]
        );
    }

    #[test]
    fn a_live_member_that_turns_left_between_snapshots_leaves_then_is_left() {
        let before = snapshot(vec![live(1)]);
        let after = snapshot(vec![testkit::member(1, MemberStatus::Left)]);
        let tags: Vec<_> = diff_snapshots(Some(&before), &after)
            .iter()
            .map(EventKind::tag)
            .collect();
        assert_eq!(tags, ["LEAVE", "LEFT"]);
    }

    #[test]
    fn a_crash_raises_down_and_never_left() {
        let before = snapshot(vec![live(1)]);
        let after = snapshot(vec![testkit::member(1, MemberStatus::Down)]);
        assert_eq!(
            diff_snapshots(Some(&before), &after),
            vec![EventKind::Down {
                node: node(1),
                addr: addr(1),
                exporter_silent: None,
                superseded: false
            }]
        );
    }

    #[test]
    fn a_departing_member_that_goes_down_raises_down() {
        let before = snapshot(vec![testkit::member(1, MemberStatus::Departing)]);
        let after = snapshot(vec![testkit::member(1, MemberStatus::Down)]);
        let tags: Vec<_> = diff_snapshots(Some(&before), &after)
            .iter()
            .map(EventKind::tag)
            .collect();
        assert_eq!(tags, ["DOWN"]);
    }

    #[test]
    fn a_down_member_that_turns_live_comes_up() {
        let down = snapshot(vec![testkit::member(1, MemberStatus::Down)]);
        assert_eq!(
            diff_snapshots(Some(&down), &snapshot(vec![live(1)])),
            vec![EventKind::Up {
                node: node(1),
                addr: addr(1)
            }]
        );
    }

    #[test]
    fn a_down_member_that_turns_departing_comes_up_and_leaves() {
        let down = snapshot(vec![testkit::member(1, MemberStatus::Down)]);
        let departing = snapshot(vec![testkit::member(1, MemberStatus::Departing)]);
        assert_eq!(
            diff_snapshots(Some(&down), &departing),
            vec![
                EventKind::Up {
                    node: node(1),
                    addr: addr(1)
                },
                EventKind::Leave {
                    node: node(1),
                    addr: addr(1),
                    superseded: false
                },
            ]
        );
    }

    #[test]
    fn a_gone_member_that_vanishes_or_a_down_member_that_turns_left_raises_nothing() {
        let down = snapshot(vec![testkit::member(1, MemberStatus::Down)]);
        assert!(diff_snapshots(Some(&down), &snapshot(Vec::new())).is_empty());
        let left = snapshot(vec![testkit::member(1, MemberStatus::Left)]);
        assert!(diff_snapshots(Some(&down), &left).is_empty());
    }

    #[test]
    fn an_older_incarnation_going_while_a_newer_one_holds_the_address_is_superseded() {
        let old = |status| testkit::member_at(1, 0, 1, status);
        let new = || testkit::member_at(1, 0, 2, MemberStatus::Live);
        let before = snapshot(vec![old(MemberStatus::Live), new()]);
        let flags = |events: Vec<EventKind>| -> Vec<(&'static str, bool)> {
            events
                .into_iter()
                .map(|event| match event {
                    EventKind::Leave { superseded, .. }
                    | EventKind::Left { superseded, .. }
                    | EventKind::Down { superseded, .. } => (event.tag(), superseded),
                    other => (other.tag(), false),
                })
                .collect()
        };
        assert_eq!(
            flags(diff_snapshots(
                Some(&before),
                &snapshot(vec![old(MemberStatus::Down), new()])
            )),
            [("DOWN", true)]
        );
        assert_eq!(
            flags(diff_snapshots(
                Some(&before),
                &snapshot(vec![old(MemberStatus::Left), new()])
            )),
            [("LEAVE", true), ("LEFT", true)]
        );
        // A different node id at the address is superseded the same way.
        let rejoined = testkit::member_at(1, 1, 5, MemberStatus::Live);
        let before = snapshot(vec![old(MemberStatus::Live), rejoined.clone()]);
        assert_eq!(
            flags(diff_snapshots(
                Some(&before),
                &snapshot(vec![old(MemberStatus::Down), rejoined])
            )),
            [("DOWN", true)]
        );
        // The newest record at the address is not.
        let before = snapshot(vec![old(MemberStatus::Live), new()]);
        let after = snapshot(vec![
            old(MemberStatus::Live),
            testkit::member_at(1, 0, 2, MemberStatus::Down),
        ]);
        assert_eq!(
            flags(diff_snapshots(Some(&before), &after)),
            [("DOWN", false)]
        );
    }

    #[test]
    fn an_older_incarnation_arriving_departing_leaves_superseded() {
        let new = testkit::member_at(1, 0, 2, MemberStatus::Live);
        let old = testkit::member_at(1, 0, 1, MemberStatus::Departing);
        let events = diff_snapshots(
            Some(&snapshot(vec![new.clone()])),
            &snapshot(vec![old, new]),
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                EventKind::Leave {
                    superseded: true,
                    ..
                }
            )),
            "{events:?}"
        );
    }

    #[test]
    fn a_new_node_id_at_a_known_address_rejoins() {
        let before = snapshot(vec![testkit::member(1, MemberStatus::Down)]);
        let after = snapshot(vec![
            testkit::member(1, MemberStatus::Down),
            testkit::member_at(1, 1, 5, MemberStatus::Live),
        ]);
        assert_eq!(
            diff_snapshots(Some(&before), &after),
            vec![EventKind::Rejoin {
                node: testkit::node_id(1, 1),
                addr: addr(1),
                previous: node(1),
                caches: [("it".into(), testkit::distributed(2))].into(),
            }]
        );
    }

    fn wall(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn join_row(index: u8, at: u64, caches: &[(&str, Mode)]) -> Event {
        Event {
            at: wall(at),
            kind: EventKind::Join {
                node: node(index),
                addr: addr(index),
                protocol: 6,
                caches: caches.iter().map(|(n, m)| ((*n).into(), *m)).collect(),
            },
        }
    }

    fn joined_caches(log: &EventLog, row: usize) -> Vec<String> {
        match &log.iter().nth(row).unwrap().kind {
            EventKind::Join { caches, .. } | EventKind::Rejoin { caches, .. } => {
                caches.keys().map(ToString::to_string).collect()
            }
            other => panic!("not an arrival: {other:?}"),
        }
    }

    #[test]
    fn a_cache_a_node_opens_just_after_it_joins_folds_into_its_join_row() {
        let mut log = EventLog::new();
        log.push(join_row(1, 100, &[]));
        log.push(join_row(2, 100, &[("it", testkit::distributed(2))]));
        let side = Mode::Replicated;
        // Folds into node 1's row, not node 2's, and says so.
        assert!(log.fold_cache(node(1), &"os".into(), side, wall(102)));
        assert!(log.fold_cache(node(1), &"pn".into(), side, wall(100) + FOLD_WITHIN));
        assert_eq!(joined_caches(&log, 0), ["os", "pn"]);
        assert_eq!(joined_caches(&log, 1), ["it"]);
        assert_eq!(log.len(), 2, "a fold adds no row");
    }

    #[test]
    fn a_cache_does_not_fold_into_a_stale_row_a_named_cache_or_another_node() {
        let mut log = EventLog::new();
        log.push(join_row(1, 100, &[("it", testkit::distributed(2))]));
        let side = Mode::Replicated;
        // Past the window.
        let late = wall(100) + FOLD_WITHIN + Duration::from_secs(1);
        assert!(!log.fold_cache(node(1), &"os".into(), side, late));
        // The row names it already: a mode change stands on its own.
        assert!(!log.fold_cache(node(1), &"it".into(), side, wall(101)));
        // No arrival row for the node.
        assert!(!log.fold_cache(node(9), &"os".into(), side, wall(101)));
        assert!(!EventLog::new().fold_cache(node(1), &"os".into(), side, wall(1)));
        assert_eq!(joined_caches(&log, 0), ["it"]);
    }

    #[test]
    fn a_cache_folds_into_a_rejoin_row_and_into_the_latest_arrival_of_the_node() {
        let mut log = EventLog::new();
        log.push(join_row(1, 10, &[]));
        log.push(Event {
            at: wall(100),
            kind: EventKind::Rejoin {
                node: node(1),
                addr: addr(1),
                previous: node(7),
                caches: BTreeMap::new(),
            },
        });
        assert!(log.fold_cache(node(1), &"os".into(), Mode::Replicated, wall(101)));
        assert!(joined_caches(&log, 0).is_empty());
        assert_eq!(joined_caches(&log, 1), ["os"]);
    }

    #[test]
    fn the_events_of_an_address_include_every_process_that_held_it() {
        let old = node(1);
        let new = testkit::node_id(1, 1);
        let other = node(2);
        let at = |secs| wall(secs);
        let mut log = EventLog::new();
        let push = |log: &mut EventLog, secs, kind| log.push(Event { at: at(secs), kind });
        push(
            &mut log,
            1,
            EventKind::Join {
                node: old,
                addr: addr(1),
                protocol: 6,
                caches: BTreeMap::new(),
            },
        );
        push(
            &mut log,
            2,
            EventKind::Join {
                node: other,
                addr: addr(2),
                protocol: 6,
                caches: BTreeMap::new(),
            },
        );
        push(&mut log, 3, EventKind::Ready { node: old });
        push(
            &mut log,
            4,
            EventKind::Down {
                node: old,
                addr: addr(1),
                exporter_silent: None,
                superseded: false,
            },
        );
        push(&mut log, 5, EventKind::Ready { node: other });
        push(
            &mut log,
            6,
            EventKind::Rejoin {
                node: new,
                addr: addr(1),
                previous: old,
                caches: BTreeMap::new(),
            },
        );
        push(&mut log, 7, EventKind::Ready { node: new });
        push(
            &mut log,
            8,
            EventKind::View {
                cache: "it".into(),
                from: None,
                to: 1,
                moved: 0,
                deltas: Vec::new(),
            },
        );
        let tags = |events: Vec<&Event>| -> Vec<&'static str> {
            events.iter().map(|event| event.kind.tag()).collect()
        };
        // Newest first: the new process's READY and REJOIN, then the old
        // one's DOWN, READY and JOIN; node 2 and the cluster-wide VIEW stay out.
        assert_eq!(
            tags(log.of_address(addr(1), new)),
            ["READY", "REJOIN", "DOWN", "READY", "JOIN"]
        );
        assert_eq!(tags(log.of_address(addr(2), other)), ["READY", "JOIN"]);
        // An address nothing names, held by a node nothing mentions.
        assert!(log.of_address(addr(9), node(9)).is_empty());
        // The current node counts even when no event names its address: its
        // own events are the slot's.
        assert_eq!(tags(log.of_address(addr(7), new)), ["READY", "REJOIN"]);
    }

    #[test]
    fn an_event_names_its_gossip_address_when_it_has_one() {
        let n = node(1);
        let a = addr(1);
        assert_eq!(EventKind::Restart { node: n, addr: a }.addr(), Some(a));
        assert_eq!(EventKind::Up { node: n, addr: a }.addr(), Some(a));
        assert_eq!(
            EventKind::Exporter {
                node: n,
                addr: a,
                detail: String::new()
            }
            .addr(),
            Some(a)
        );
        assert_eq!(EventKind::Ready { node: n }.addr(), None);
        assert_eq!(
            EventKind::CacheRemoved {
                node: n,
                cache: "it".into()
            }
            .addr(),
            None
        );
    }

    #[test]
    fn a_higher_incarnation_of_the_same_node_restarts() {
        let before = snapshot(vec![testkit::member(1, MemberStatus::Down)]);
        let after = snapshot(vec![
            testkit::member(1, MemberStatus::Down),
            testkit::member_at(1, 0, 9, MemberStatus::Live),
        ]);
        assert_eq!(
            diff_snapshots(Some(&before), &after),
            vec![EventKind::Restart {
                node: node(1),
                addr: addr(1)
            }]
        );
    }

    #[test]
    fn a_node_that_restarts_on_another_address_still_restarts() {
        let before = snapshot(vec![testkit::member(1, MemberStatus::Down)]);
        let mut moved = testkit::member_at(1, 0, 9, MemberStatus::Live);
        moved.peer.gossip_addr = addr(7);
        let after = snapshot(vec![testkit::member(1, MemberStatus::Down), moved]);
        let events = diff_snapshots(Some(&before), &after);
        assert_eq!(
            events,
            vec![EventKind::Restart {
                node: node(1),
                addr: addr(7)
            }]
        );
    }

    #[test]
    fn a_cache_opened_or_closed_on_a_live_member_raises_cache_events() {
        let one = |caches: &[(&str, Mode)]| {
            snapshot(vec![member_with(1, 0, 1, MemberStatus::Live, caches)])
        };
        let before = one(&[("a", Mode::Replicated), ("b", Mode::Replicated)]);
        let after = one(&[("b", Mode::Replicated), ("c", distributed(2))]);
        assert_eq!(
            diff_snapshots(Some(&before), &after),
            vec![
                EventKind::CacheRemoved {
                    node: node(1),
                    cache: "a".into()
                },
                EventKind::CacheAdded {
                    node: node(1),
                    cache: "c".into(),
                    mode: distributed(2)
                },
            ]
        );
    }

    #[test]
    fn a_cache_whose_mode_changes_is_removed_and_added() {
        let one = |mode| {
            snapshot(vec![member_with(
                1,
                0,
                1,
                MemberStatus::Live,
                &[("a", mode)],
            )])
        };
        let events = diff_snapshots(Some(&one(Mode::Replicated)), &one(distributed(2)));
        let tags: Vec<_> = events.iter().map(EventKind::tag).collect();
        assert_eq!(tags, ["CACHE-", "CACHE+"]);
    }

    #[test]
    fn cache_changes_of_a_departing_member_raise_nothing() {
        let before = snapshot(vec![member_with(
            1,
            0,
            1,
            MemberStatus::Departing,
            &[("a", Mode::Replicated)],
        )]);
        let after = snapshot(vec![member_with(1, 0, 1, MemberStatus::Departing, &[])]);
        assert!(diff_snapshots(Some(&before), &after).is_empty());
    }

    #[test]
    fn live_members_with_different_modes_for_a_cache_conflict() {
        let before = snapshot(vec![
            member_with(1, 0, 1, MemberStatus::Live, &[("x", Mode::Replicated)]),
            member_with(2, 0, 1, MemberStatus::Live, &[("x", Mode::Replicated)]),
        ]);
        let after = snapshot(vec![
            member_with(1, 0, 1, MemberStatus::Live, &[("x", Mode::Replicated)]),
            member_with(2, 0, 1, MemberStatus::Live, &[("x", distributed(2))]),
        ]);
        let events = diff_snapshots(Some(&before), &after);
        let conflict = EventKind::Conflict {
            cache: "x".into(),
            modes: vec![(node(1), Mode::Replicated), (node(2), distributed(2))],
        };
        assert!(events.contains(&conflict), "{events:?}");
        assert_eq!(events.last(), Some(&conflict), "conflicts come last");
        // The same disagreement raises it only once.
        assert!(diff_snapshots(Some(&after), &after).is_empty());
    }

    #[test]
    fn a_departing_or_down_member_does_not_conflict() {
        let next = snapshot(vec![
            member_with(1, 0, 1, MemberStatus::Live, &[("x", Mode::Replicated)]),
            member_with(2, 0, 1, MemberStatus::Departing, &[("x", distributed(2))]),
            member_with(3, 0, 1, MemberStatus::Down, &[("x", distributed(3))]),
        ]);
        let events = diff_snapshots(None, &next);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, EventKind::Conflict { .. }))
        );
    }

    #[test]
    fn a_member_on_another_protocol_raises_proto_after_its_join() {
        let mut old = live(1);
        old.peer.protocol = PROTOCOL_VERSION - 1;
        let events = diff_snapshots(None, &snapshot(vec![old, live(2)]));
        let tags: Vec<_> = events.iter().map(EventKind::tag).collect();
        assert_eq!(tags, ["JOIN", "PROTO", "JOIN"]);
        assert_eq!(
            events[1],
            EventKind::Proto {
                node: node(1),
                protocol: PROTOCOL_VERSION - 1
            }
        );
        let mut newer = live(3);
        newer.peer.protocol = PROTOCOL_VERSION + 1;
        let tags: Vec<_> = diff_snapshots(None, &snapshot(vec![newer]))
            .iter()
            .map(EventKind::tag)
            .collect();
        assert_eq!(tags, ["JOIN", "PROTO"]);
    }

    #[test]
    fn events_follow_member_order() {
        let before = snapshot(vec![live(1), live(2)]);
        let after = snapshot(vec![
            testkit::member(1, MemberStatus::Down),
            testkit::member(2, MemberStatus::Departing),
            live(3),
        ]);
        let events = diff_snapshots(Some(&before), &after);
        let tags: Vec<_> = events.iter().map(EventKind::tag).collect();
        assert_eq!(tags, ["DOWN", "LEAVE", "JOIN"]);
    }

    fn event(kind: EventKind) -> Event {
        Event {
            at: SystemTime::UNIX_EPOCH,
            kind,
        }
    }

    fn view(to: u64) -> EventKind {
        EventKind::View {
            cache: "it".into(),
            from: None,
            to,
            moved: 0,
            deltas: Vec::new(),
        }
    }

    type Row = (EventKind, &'static str, Category, Option<NodeId>);

    /// One row per event kind: the kind, its tag, category and node.
    #[expect(clippy::too_many_lines, reason = "a table with one row per event kind")]
    fn kind_table() -> Vec<Row> {
        let n = node(1);
        let a = addr(1);
        vec![
            (
                EventKind::Join {
                    node: n,
                    addr: a,
                    protocol: 6,
                    caches: BTreeMap::new(),
                },
                "JOIN",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::Leave {
                    node: n,
                    addr: a,
                    superseded: false,
                },
                "LEAVE",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::Left {
                    node: n,
                    addr: a,
                    superseded: false,
                },
                "LEFT",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::Down {
                    node: n,
                    addr: a,
                    exporter_silent: None,
                    superseded: false,
                },
                "DOWN",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::Up { node: n, addr: a },
                "UP",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::Rejoin {
                    node: n,
                    addr: a,
                    previous: node(2),
                    caches: BTreeMap::new(),
                },
                "REJOIN",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::Restart { node: n, addr: a },
                "RESTART",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::CacheAdded {
                    node: n,
                    cache: "c".into(),
                    mode: Mode::Replicated,
                },
                "CACHE+",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::CacheRemoved {
                    node: n,
                    cache: "c".into(),
                },
                "CACHE-",
                Category::Membership,
                Some(n),
            ),
            (
                EventKind::Conflict {
                    cache: "c".into(),
                    modes: Vec::new(),
                },
                "CONFLICT",
                Category::Membership,
                None,
            ),
            (
                EventKind::Proto {
                    node: n,
                    protocol: 5,
                },
                "PROTO",
                Category::Membership,
                Some(n),
            ),
            (view(1), "VIEW", Category::Ownership, None),
            (
                EventKind::Settled {
                    cache: "it".into(),
                    took: Duration::ZERO,
                },
                "SETTLED",
                Category::Ownership,
                None,
            ),
            (
                EventKind::Xfer { node: n },
                "XFER",
                Category::Traffic,
                Some(n),
            ),
            (
                EventKind::Drop {
                    node: n,
                    peer: "ab".into(),
                    frames: 3,
                },
                "DROP",
                Category::Traffic,
                Some(n),
            ),
            (
                EventKind::Ready { node: n },
                "READY",
                Category::Exporter,
                Some(n),
            ),
            (
                EventKind::Unready { node: n },
                "UNREADY",
                Category::Exporter,
                Some(n),
            ),
            (
                EventKind::Unreachable { node: n },
                "UNREACHABLE",
                Category::Exporter,
                Some(n),
            ),
            (
                EventKind::Exporter {
                    node: n,
                    addr: a,
                    detail: "up".into(),
                },
                "EXPORTER",
                Category::Exporter,
                Some(n),
            ),
        ]
    }

    #[test]
    fn every_kind_has_a_tag_a_category_and_a_node() {
        let mut tags = std::collections::HashSet::new();
        for (kind, tag, category, who) in kind_table() {
            assert_eq!(kind.tag(), tag);
            assert_eq!(kind.category(), category, "{tag}");
            assert_eq!(kind.node(), who, "{tag}");
            assert!(tags.insert(tag), "{tag} twice");
        }
        assert_eq!(tags.len(), 19);
    }

    #[test]
    fn the_filter_cycles_through_every_group_and_back() {
        let mut filter = Filter::default();
        assert_eq!(filter, Filter::All);
        let mut seen = vec![filter.label()];
        for _ in 0..5 {
            filter = filter.next();
            seen.push(filter.label());
        }
        assert_eq!(
            seen,
            [
                "all",
                "membership",
                "ownership",
                "traffic",
                "exporter",
                "all"
            ]
        );
    }

    #[test]
    fn a_filter_passes_only_its_category() {
        let join = EventKind::Restart {
            node: node(1),
            addr: addr(1),
        };
        let settled = EventKind::Settled {
            cache: "it".into(),
            took: Duration::ZERO,
        };
        let xfer = EventKind::Xfer { node: node(1) };
        let ready = EventKind::Ready { node: node(1) };
        let passes =
            |filter: Filter| [&join, &settled, &xfer, &ready].map(|kind| filter.matches(kind));
        assert_eq!(passes(Filter::All), [true; 4]);
        assert_eq!(passes(Filter::Membership), [true, false, false, false]);
        assert_eq!(passes(Filter::Ownership), [false, true, false, false]);
        assert_eq!(passes(Filter::Traffic), [false, false, true, false]);
        assert_eq!(passes(Filter::Exporter), [false, false, false, true]);
    }

    #[test]
    fn the_log_keeps_the_newest_events_and_filters_newest_first() {
        let mut log = EventLog::new();
        assert!(log.is_empty());
        for index in 0..(LOG_CAPACITY as u64 + 5) {
            log.push(event(view(index)));
        }
        assert_eq!(log.len(), LOG_CAPACITY);
        let EventKind::View { to, .. } = log.iter().next().unwrap().kind else {
            panic!("a view");
        };
        assert_eq!(to, 5, "the oldest five dropped");
        let EventKind::View { to, .. } = log.newest_first(Filter::All).next().unwrap().kind else {
            panic!("a view");
        };
        assert_eq!(to, LOG_CAPACITY as u64 + 4);
    }

    #[test]
    fn the_log_filters_by_category() {
        let mut log = EventLog::new();
        log.push(event(view(1)));
        log.push(event(EventKind::Xfer { node: node(1) }));
        log.push(event(view(2)));
        let views: Vec<_> = log
            .newest_first(Filter::Ownership)
            .map(|e| e.kind.tag())
            .collect();
        assert_eq!(views, ["VIEW", "VIEW"]);
        assert_eq!(log.newest_first(Filter::Traffic).count(), 1);
        assert_eq!(log.newest_first(Filter::Exporter).count(), 0);
        assert_eq!(log.iter().len(), 3);
    }
}
