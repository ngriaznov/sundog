//! Watching a cluster from outside. An [`Observer`] joins a cluster's gossip
//! without a data plane and without caches, and publishes a
//! [`ClusterSnapshot`] of every member as gossip shows it: its [`Peer`]
//! record, its [`MemberStatus`], and the caches it advertises with their
//! [`Mode`]. Its gossip state carries no node id and no data address, so no
//! member counts it as a peer: it is never dialed, sent writes, asked for
//! state or made an owner. Members keep its dead chitchat entry for
//! `dead_node_grace_period` after it stops.
//! [`ClusterSnapshot::ownership`] ranks a [`Mode::Distributed`] cache's
//! parts the way its members do.
//!
//! ```no_run
//! # use sundog::Mode;
//! # use sundog::observe::Observer;
//! # async fn watch() -> Result<(), Box<dyn std::error::Error>> {
//! let observer = Observer::builder("prod")
//!     .seeds(["10.0.0.1:7946".parse()?])
//!     .build()
//!     .await?;
//! let snapshot = observer.snapshot();
//! if let Some(shares) = snapshot.ownership("sessions", Mode::DEFAULT_OWNERS) {
//!     for &node in shares.eligible() {
//!         println!("{node} owns {} parts", shares.parts_owned_by(node));
//!     }
//! }
//! observer.shutdown().await;
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use chitchat::{Chitchat, ChitchatHandle, ChitchatId, Heartbeat, NodeState, Version};
use futures::StreamExt as _;
use futures::stream::{BoxStream, FusedStream};
use smol_str::SmolStr;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::time::{self, MissedTickBehavior};

use crate::cluster::{local_hostname, resolve_discovery};
use crate::config::ClusterConfig;
use crate::discovery::statics::Static;
use crate::discovery::{Discovery, DiscoveryKind};
use crate::error::JoinError;
use crate::membership::{
    CacheModes, Peer, collect_initial_seeds, is_departing, now_incarnation_ms, parse_cache_modes,
    parse_peer, start_gossip,
};
use crate::node::{NodeId, NodeName};
use crate::ownership::{Granularity, OwnershipView, eligible_peers};
use crate::store::{Mode, PartId};

/// How often an observer re-reads gossip when no live-set change wakes it:
/// the cadence at which a failure-detector verdict and the collection of an
/// old entry reach a snapshot.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// A gossip-only member that watches a cluster. Cheap to clone; clones share
/// one gossip session. Dropping every clone stops it at once;
/// [`Observer::shutdown`] leaves gossip and waits for its loop to stop.
///
/// The observer's gossip state carries no node id, so no member counts it as
/// a peer. Each member keeps the observer's dead chitchat entry for
/// [`ClusterConfig::dead_node_grace_period`] after the observer stops.
#[derive(Clone)]
pub struct Observer {
    inner: Arc<ObserverInner>,
}

struct ObserverInner {
    cluster: SmolStr,
    gossip_addr: SocketAddr,
    /// A receiver only: the loop owns the sender, so the channel closes
    /// exactly when the loop returns.
    snapshot: watch::Receiver<Arc<ClusterSnapshot>>,
    commands: mpsc::UnboundedSender<Command>,
}

/// A request to the observer's gossip loop, the sole owner of the chitchat
/// handle.
enum Command {
    Stop(oneshot::Sender<()>),
}

impl fmt::Debug for Observer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Observer")
            .field("cluster", &self.inner.cluster)
            .field("gossip_addr", &self.inner.gossip_addr)
            .finish_non_exhaustive()
    }
}

/// Builds an [`Observer`]. [`ObserverBuilder::build`] alone finds the cluster
/// as a [`Cluster`](crate::Cluster) does: the `SUNDOG_SEEDS` seeds when that
/// variable is set, mDNS otherwise.
#[must_use]
pub struct ObserverBuilder {
    name: SmolStr,
    discovery: Option<DiscoveryKind>,
    config: ClusterConfig,
}

/// A point-in-time picture of a cluster as one observer's gossip shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClusterSnapshot {
    /// The cluster name.
    pub cluster: SmolStr,
    /// Every member gossip shows, live or recently gone, ascending by node id
    /// then incarnation. A process restarted with a persisted `NodeId`
    /// appears once per incarnation until the old one is reaped.
    pub members: Vec<Member>,
    /// Live gossip participants with no sundog node state: other observers,
    /// or a node whose first state has not arrived yet.
    pub anonymous: usize,
}

/// One member in a [`ClusterSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Member {
    /// Identity and addresses, as [`Cluster::peers`](crate::Cluster::peers)
    /// reports them.
    pub peer: Peer,
    /// Where the member is in its lifecycle.
    pub status: MemberStatus,
    /// When this observer first saw the member in `status`.
    pub since: SystemTime,
    /// Every cache the member advertises, with its mode. Kept through a
    /// departure, so a departing or gone member still shows what it held.
    pub caches: BTreeMap<SmolStr, Mode>,
}

/// A member's lifecycle as gossip shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MemberStatus {
    /// Live and serving.
    Live,
    /// Live and gossiping a graceful departure: it owns no part of any
    /// `Distributed` cache from the moment the departure spreads.
    Departing,
    /// Dropped by the failure detector after gossiping a departure. Final
    /// for an incarnation: a heartbeat relayed late can make the detector
    /// hold the node live again, and the snapshot keeps it `Left`.
    Left,
    /// Dropped by the failure detector with no departure: a crash, a stall
    /// past the detector's threshold, or a partition from this observer.
    Down,
}

/// One `Distributed` cache's ownership as its live members compute it.
#[derive(Debug, Clone)]
pub struct OwnershipShares {
    cache: SmolStr,
    owners: NonZeroU8,
    view: Arc<OwnershipView>,
    /// Parts owned at any rank, ascending by node id, one entry per eligible
    /// node.
    counts: Vec<(NodeId, usize)>,
}

/// One member as a read of gossip yields it, before its `since` is known.
type Observed = (Peer, MemberStatus, BTreeMap<SmolStr, Mode>);

impl Observer {
    /// Starts building an observer of the cluster named `cluster`.
    pub fn builder(cluster: impl Into<SmolStr>) -> ObserverBuilder {
        ObserverBuilder {
            name: cluster.into(),
            discovery: None,
            config: ClusterConfig::default(),
        }
    }

    /// The latest snapshot: a watch borrow and an `Arc` clone, no gossip
    /// lock.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ClusterSnapshot> {
        Arc::clone(&self.inner.snapshot.borrow())
    }

    /// Wakes on every published snapshot. A snapshot publishes only when a
    /// member, a status, a cache or the anonymous count changes, never on a
    /// heartbeat. The receiver starts with the current snapshot seen, and
    /// its `changed` returns an error once the observer has stopped, by
    /// [`Observer::shutdown`] or by dropping every clone.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<ClusterSnapshot>> {
        let mut updates = self.inner.snapshot.clone();
        updates.mark_unchanged();
        updates
    }

    /// The gossip address this observer advertises.
    #[must_use]
    pub fn local_gossip_addr(&self) -> SocketAddr {
        self.inner.gossip_addr
    }

    /// Leaves gossip and stops the loop. Members see the observer drop out
    /// of chitchat's live set; nothing else changes for them.
    pub async fn shutdown(self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.inner.commands.send(Command::Stop(reply_tx)).is_ok() {
            let _ = reply_rx.await;
        }
    }
}

impl ObserverBuilder {
    /// A fixed seed list; switches discovery to [`Static`].
    pub fn seeds(mut self, seeds: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.discovery = Some(DiscoveryKind::Static(Static::new(seeds)));
        self
    }

    /// A custom discovery mechanism; the observer reads its candidates and
    /// never announces itself.
    pub fn discovery(mut self, discovery: impl Discovery + 'static) -> Self {
        self.discovery = Some(DiscoveryKind::Custom(Box::new(discovery)));
        self
    }

    /// Gossip settings: `gossip_bind_addr`, `advertise_ip`, `gossip_interval`,
    /// the failure-detector fields and both grace periods. Other fields are
    /// unused. Pass the cluster's own gossip settings so the observer marks a
    /// member [`MemberStatus::Down`] when the members do.
    pub fn config(mut self, config: ClusterConfig) -> Self {
        self.config = config;
        self
    }

    /// Joins the cluster's gossip and starts watching it.
    ///
    /// # Errors
    ///
    /// Returns [`JoinError::Bind`] if a port-0 `gossip_bind_addr` cannot be
    /// probed for a free port, or [`JoinError::Membership`] if the gossip
    /// backend fails to start, including when a fixed `gossip_bind_addr` is
    /// already taken.
    pub async fn build(self) -> Result<Observer, JoinError> {
        let Self {
            name,
            discovery,
            config,
        } = self;
        let node_name = NodeName::new(&format!("observer.{}", local_hostname()), NodeId::random());
        let discovery = resolve_discovery(discovery, &name, &node_name);
        let mut seeds = discovery.candidates();
        let seed_nodes = collect_initial_seeds(&mut seeds).await;
        // The empty key list is the seam: without a `node_id` key no member's
        // `parse_peer` accepts this entry, so it is never a peer. No
        // announce either: the observer is not to be found.
        let (handle, chitchat_id, gossip_addr) = start_gossip(
            &name,
            &node_name,
            now_incarnation_ms(),
            &config,
            seed_nodes,
            Vec::new(),
        )
        .await?;

        let (snapshot_tx, snapshot_rx) =
            watch::channel(Arc::new(ClusterSnapshot::new(name.clone(), Vec::new(), 0)));
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        tokio::spawn(observe_loop(
            handle,
            seeds,
            chitchat_id,
            settle_window(config.gossip_interval),
            snapshot_tx,
            commands_rx,
        ));
        tracing::info!(cluster = %name, %gossip_addr, "observer started");

        Ok(Observer {
            inner: Arc::new(ObserverInner {
                cluster: name,
                gossip_addr,
                snapshot: snapshot_rx,
                commands: commands_tx,
            }),
        })
    }
}

impl ClusterSnapshot {
    /// A snapshot of `cluster` holding `members`, sorted as
    /// [`ClusterSnapshot::members`] is documented. For fixtures.
    #[must_use]
    pub fn new(cluster: impl Into<SmolStr>, mut members: Vec<Member>, anonymous: usize) -> Self {
        members.sort_by_key(|member| (member.peer.node, member.peer.incarnation));
        Self {
            cluster: cluster.into(),
            members,
            anonymous,
        }
    }

    /// `cache`'s ownership with `owners` owners per part, as every live
    /// member advertising `cache` under `Mode::Distributed { owners }`
    /// computes it once gossip converges. Eligible: [`MemberStatus::Live`]
    /// members on at least [`wire::PROTOCOL_DISTRIBUTED`](crate::wire::PROTOCOL_DISTRIBUTED)
    /// advertising that exact mode; a departing, left or down member never
    /// is. A node id held under two incarnations, as in a rolling upgrade
    /// with a persisted id, reads its caches from the later incarnation and
    /// its protocol from the earlier one, as its peers do. Ranks single parts when every eligible member speaks
    /// [`wire::PROTOCOL_PART_OWNERSHIP`](crate::wire::PROTOCOL_PART_OWNERSHIP),
    /// whole buckets otherwise. `None` when no member is eligible. Costs about
    /// 65,536 × eligible hashes: call it off async workers, and again only on
    /// a new snapshot.
    #[must_use]
    pub fn ownership(&self, cache: &str, owners: NonZeroU8) -> Option<OwnershipShares> {
        let cache = SmolStr::new(cache);
        // The inputs a member's own `membership::run` builds: every member in
        // chitchat's live set, a departing one with no advertised caches,
        // and a node id's last incarnation deciding its modes.
        let live: Vec<&Member> = self
            .members
            .iter()
            .filter(|member| member.status.is_live())
            .collect();
        let peers: Vec<Peer> = live.iter().map(|member| member.peer.clone()).collect();
        let mut modes: CacheModes = HashMap::new();
        for member in &live {
            let advertised = if member.status == MemberStatus::Departing {
                HashMap::new()
            } else {
                member
                    .caches
                    .iter()
                    .map(|(name, mode)| (name.clone(), *mode))
                    .collect()
            };
            modes.insert(member.peer.node, advertised);
        }
        let mut eligible = eligible_peers(&peers, &modes, &cache, owners);
        eligible.sort_unstable();
        eligible.dedup();
        let seat = *eligible.first()?;
        // A member resolves a node's protocol from the first of its peer
        // records for that node.
        let granularity = Granularity::for_members(eligible.iter().map(|node| {
            peers
                .iter()
                .find(|peer| peer.node == *node)
                .map_or(0, |peer| peer.protocol)
        }));
        // Ranking from an eligible seat adds nobody to the set.
        let view = OwnershipView::compute_at(seat, eligible.clone(), owners, granularity);
        let mut counts: Vec<(NodeId, usize)> = eligible.iter().map(|&node| (node, 0)).collect();
        for part in PartId::all() {
            for owner in view.owners_of(part) {
                if let Ok(index) = counts.binary_search_by_key(owner, |&(node, _)| node) {
                    counts[index].1 += 1;
                }
            }
        }
        Some(OwnershipShares {
            cache,
            owners,
            view: Arc::new(view),
            counts,
        })
    }
}

impl Member {
    /// A member with the given fields. For fixtures.
    #[must_use]
    pub fn new(
        peer: Peer,
        status: MemberStatus,
        since: SystemTime,
        caches: BTreeMap<SmolStr, Mode>,
    ) -> Self {
        Self {
            peer,
            status,
            since,
            caches,
        }
    }
}

impl MemberStatus {
    /// Whether the member is in chitchat's live set: `Live` or `Departing`.
    #[must_use]
    pub const fn is_live(self) -> bool {
        matches!(self, Self::Live | Self::Departing)
    }
}

impl OwnershipShares {
    /// The cache these shares describe.
    #[must_use]
    pub fn cache(&self) -> &str {
        &self.cache
    }

    /// How many owners each part has.
    #[must_use]
    pub const fn owners(&self) -> NonZeroU8 {
        self.owners
    }

    /// Equal to the view hash each eligible member's own view carries; it
    /// changes exactly when ownership moves.
    #[must_use]
    pub fn view_hash(&self) -> u64 {
        self.view.view_hash()
    }

    /// Whether the view ranks single parts rather than whole buckets.
    #[must_use]
    pub fn ranks_parts(&self) -> bool {
        self.view.granularity() == Granularity::Part
    }

    /// The eligible members, ascending.
    #[must_use]
    pub fn eligible(&self) -> &[NodeId] {
        self.view.eligible()
    }

    /// `part`'s owners, highest rendezvous score first: what
    /// [`Cache::owners_of`](crate::Cache::owners_of) answers on a member for
    /// a key in `part`.
    #[must_use]
    pub fn owners_of(&self, part: PartId) -> &[NodeId] {
        self.view.owners_of(part)
    }

    /// How many of the 65,536 parts `node` owns at any rank; 0 off the
    /// eligible set. The counts sum to 65,536 × min(owners, eligible).
    #[must_use]
    pub fn parts_owned_by(&self, node: NodeId) -> usize {
        self.counts
            .binary_search_by_key(&node, |&(member, _)| member)
            .map_or(0, |index| self.counts[index].1)
    }
}

/// A member's status from chitchat's two verdicts: whether the failure
/// detector holds it live, and whether it gossiped a departure.
fn status_of(live: bool, departing: bool) -> MemberStatus {
    match (live, departing) {
        (true, false) => MemberStatus::Live,
        (true, true) => MemberStatus::Departing,
        (false, true) => MemberStatus::Left,
        (false, false) => MemberStatus::Down,
    }
}

/// The status of a member whose live reading is `status` (`Live` or
/// `Departing`) once chitchat holds it dead.
fn dead_status(status: MemberStatus) -> MemberStatus {
    status_of(false, status == MemberStatus::Departing)
}

/// The member `id` and `state` describe, or `None` for a gossip participant
/// with no sundog node state. The caches are every `cache:<name>` key, kept
/// whether or not the member is departing.
fn observe_member(id: &ChitchatId, state: &NodeState, live: bool) -> Option<Observed> {
    let peer = parse_peer(id, state)?;
    let status = status_of(live, is_departing(state));
    let caches = parse_cache_modes(state).into_iter().collect();
    Some((peer, status, caches))
}

/// `observed` as members, ascending by node id then incarnation. A member
/// keeps its `since` from `prev` while its node, incarnation and status all
/// hold; otherwise `since` is `now`. A departure is final for an
/// incarnation: a reading of `Departing` for one `prev` holds as `Left`
/// stays `Left`, as when a heartbeat relayed late revives the node in the
/// failure detector.
fn next_members(prev: &[Member], observed: Vec<Observed>, now: SystemTime) -> Vec<Member> {
    let mut members: Vec<Member> = observed
        .into_iter()
        .map(|(peer, status, caches)| {
            let held = prev
                .iter()
                .find(|old| old.peer.node == peer.node && old.peer.incarnation == peer.incarnation);
            let status = match held {
                Some(old)
                    if old.status == MemberStatus::Left && status == MemberStatus::Departing =>
                {
                    MemberStatus::Left
                }
                _ => status,
            };
            let since = held
                .filter(|old| old.status == status)
                .map_or(now, |old| old.since);
            Member::new(peer, status, since, caches)
        })
        .collect();
    members.sort_by_key(|member| (member.peer.node, member.peer.incarnation));
    members
}

/// The snapshot that follows `current` once gossip reads as `observed` with
/// `anonymous` anonymous participants, or `None` when nothing a subscriber
/// sees changed.
fn next_snapshot(
    current: &ClusterSnapshot,
    observed: Vec<Observed>,
    anonymous: usize,
    now: SystemTime,
) -> Option<ClusterSnapshot> {
    let members = next_members(&current.members, observed, now);
    if members == current.members && anonymous == current.anonymous {
        return None;
    }
    Some(ClusterSnapshot {
        cluster: current.cluster.clone(),
        members,
        anonymous,
    })
}

/// How long a dead entry's heartbeat must hold still before the entry is
/// not a joining node: well past the interval at which a live node's
/// heartbeat reaches this observer.
fn settle_window(gossip_interval: Duration) -> Duration {
    REFRESH_INTERVAL.max(gossip_interval.saturating_mul(4))
}

/// Whether a dead entry is a member that stopped rather than one still
/// joining. chitchat holds a node dead until it has two heartbeat samples, so
/// every node enters the dead set on first sight with its heartbeat still
/// advancing. An entry is gone when a snapshot already holds it
/// (`seen_before`), or when its heartbeat has held still for `settle`.
fn dead_entry_settled(unchanged_for: Duration, seen_before: bool, settle: Duration) -> bool {
    seen_before || unchanged_for >= settle
}

/// `value` with the instant it took that value: `now` unless `previous`
/// already records it.
fn hold<T: PartialEq>(previous: Option<(T, Instant)>, value: T, now: Instant) -> (T, Instant) {
    match previous {
        Some((held, since)) if held == value => (held, since),
        _ => (value, now),
    }
}

/// What one read of gossip hands to the next.
#[derive(Default)]
struct Reader {
    /// Each node's parsed state, valid while its `max_version` holds: an
    /// unchanged state is neither re-parsed nor re-warned about.
    parsed: HashMap<ChitchatId, (Version, Option<Observed>)>,
    /// Each dead node's last heartbeat and the instant it took that value.
    dead: HashMap<ChitchatId, (Heartbeat, Instant)>,
    /// How many states have been parsed.
    #[cfg(test)]
    parses: usize,
}

impl Reader {
    /// `id`'s member as `state` shows it while live, parsed only when
    /// `state` changed since the last read.
    fn parse(&mut self, id: &ChitchatId, state: &NodeState) -> Option<Observed> {
        let version = state.max_version();
        if let Some((cached, observed)) = self.parsed.get(id)
            && *cached == version
        {
            return observed.clone();
        }
        #[cfg(test)]
        {
            self.parses += 1;
        }
        let observed = observe_member(id, state, true);
        self.parsed.insert(id.clone(), (version, observed.clone()));
        observed
    }

    /// Classifies gossip's `entries`, each a node, its state and whether the
    /// failure detector holds it live: the members they show and the count of
    /// live participants other than `self_id` with no sundog node state.
    /// `published` is the snapshot in force, `now` the instant of the read.
    ///
    /// - `self_id` is skipped.
    /// - A live entry with no sundog node state is anonymous; a dead one is
    ///   dropped.
    /// - A dead entry is reported when [`dead_entry_settled`] holds, and
    ///   otherwise skipped as a node still joining.
    fn read<'a>(
        &mut self,
        self_id: &ChitchatId,
        entries: impl IntoIterator<Item = (&'a ChitchatId, Option<&'a NodeState>, bool)>,
        published: &[Member],
        now: Instant,
        settle: Duration,
    ) -> (Vec<Observed>, usize) {
        let mut observed = Vec::new();
        let mut anonymous = 0;
        let mut present = HashSet::new();
        let mut dead = HashSet::new();
        for (id, state, live) in entries {
            if id == self_id {
                continue;
            }
            present.insert(id.clone());
            let member = state.and_then(|state| self.parse(id, state));
            let (Some(state), Some((peer, status, caches))) = (state, member) else {
                if live {
                    anonymous += 1;
                }
                continue;
            };
            if live {
                observed.push((peer, status, caches));
                continue;
            }
            dead.insert(id.clone());
            let held = hold(self.dead.get(id).copied(), state.heartbeat(), now);
            self.dead.insert(id.clone(), held);
            let seen_before = published.iter().any(|member| {
                member.peer.node == peer.node && member.peer.incarnation == peer.incarnation
            });
            if dead_entry_settled(now.saturating_duration_since(held.1), seen_before, settle) {
                observed.push((peer, dead_status(status), caches));
            }
        }
        self.parsed.retain(|id, _| present.contains(id));
        self.dead.retain(|id, _| dead.contains(id));
        (observed, anonymous)
    }
}

/// Reads gossip and publishes the snapshot it shows, when that differs from
/// the one published.
async fn publish(
    chitchat: &Mutex<Chitchat>,
    self_id: &ChitchatId,
    reader: &mut Reader,
    settle: Duration,
    snapshot: &watch::Sender<Arc<ClusterSnapshot>>,
) {
    let published = Arc::clone(&snapshot.borrow());
    let (observed, anonymous) = {
        let chitchat = chitchat.lock().await;
        let live = chitchat
            .live_nodes()
            .map(|id| (id, chitchat.node_state(id), true));
        let dead = chitchat
            .dead_nodes()
            .map(|id| (id, chitchat.node_state(id), false));
        reader.read(
            self_id,
            live.chain(dead),
            &published.members,
            Instant::now(),
            settle,
        )
    };
    let now = SystemTime::now();
    snapshot.send_if_modified(|current| {
        let Some(next) = next_snapshot(current, observed, anonymous, now) else {
            return false;
        };
        *current = Arc::new(next);
        true
    });
}

/// Owns the chitchat handle and the snapshot sender for one observer:
/// forwards discovered addresses into gossip and republishes the snapshot on
/// every live-set change and every [`REFRESH_INTERVAL`], until told to stop or
/// abandoned. The sender drops when the loop returns, which closes every
/// receiver.
async fn observe_loop(
    handle: ChitchatHandle,
    seeds: BoxStream<'static, SocketAddr>,
    self_id: ChitchatId,
    settle: Duration,
    snapshot: watch::Sender<Arc<ClusterSnapshot>>,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    let mut seeds = seeds.fuse();
    let chitchat = handle.chitchat();
    let mut live_nodes = chitchat.lock().await.live_nodes_watch_stream().fuse();
    let mut refresh = time::interval(REFRESH_INTERVAL);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut reader = Reader::default();
    loop {
        tokio::select! {
            biased;
            command = commands.recv() => {
                match command {
                    Some(Command::Stop(reply)) => {
                        if let Err(error) = handle.shutdown().await {
                            tracing::warn!(%error, "chitchat shutdown reported an error");
                        }
                        tracing::info!("observer stopped");
                        let _ = reply.send(());
                    }
                    // Every clone is gone with no `shutdown()` call.
                    None => handle.abort(),
                }
                return;
            }
            // The arm retires when the discovery stream ends. The contract
            // forbids that and a custom source can still do it;
            // `select_next_some` would panic on the poll after the end.
            Some(addr) = seeds.next(), if !seeds.is_terminated() => {
                if let Err(error) = handle.gossip(addr) {
                    tracing::debug!(%error, %addr, "failed to queue gossip with discovered peer");
                }
            }
            _ = live_nodes.select_next_some() => {
                publish(&chitchat, &self_id, &mut reader, settle, &snapshot).await;
            }
            _ = refresh.tick() => {
                publish(&chitchat, &self_id, &mut reader, settle, &snapshot).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;

// Real-transport-only: these build live `Cluster`s over loopback sockets,
// which panics under `sim` outside a driven `turmoil::Sim`.
#[cfg(all(test, not(feature = "sim")))]
mod socket_tests {
    use std::sync::Mutex as StdMutex;

    use super::tests::caches;
    use super::*;
    use crate::cache::Cache;
    use crate::cluster::Cluster;
    use crate::cluster::test_support::{
        EndingDiscovery, loopback_config, registered_shard, wait_for_peer_count, wait_until,
    };
    use crate::store::encode_key;

    /// How long a condition may take to hold. Failure detection at phi 6 and
    /// a 200 ms gossip interval takes several seconds.
    const BOUND: Duration = Duration::from_secs(30);

    async fn eventually(what: &str, cond: impl AsyncFnMut() -> bool) {
        wait_until(BOUND, what, cond).await;
    }

    /// Three clusters on loopback, the second and third seeded by the first,
    /// each seeing the other two.
    async fn trio(name: &str) -> [Cluster; 3] {
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("a builds");
        let mut rest = Vec::new();
        for label in ["b", "c"] {
            rest.push(
                Cluster::builder(name)
                    .seeds([a.local_gossip_addr()])
                    .config(loopback_config())
                    .build()
                    .await
                    .unwrap_or_else(|error| panic!("{label} builds: {error}")),
            );
        }
        let [b, c]: [Cluster; 2] = rest.try_into().expect("two clusters");
        for cluster in [&a, &b, &c] {
            wait_for_peer_count(cluster, 2).await;
        }
        [a, b, c]
    }

    async fn open(cluster: &Cluster, cache: &str, mode: Mode) -> Cache<u32, String> {
        tokio::time::timeout(
            Duration::from_secs(15),
            cluster.cache::<u32, String>(cache).mode(mode).open(),
        )
        .await
        .expect("the cache opens within the bound")
        .expect("the cache opens")
    }

    async fn observe(name: &str, seed: &Cluster) -> Observer {
        Observer::builder(name)
            .seeds([seed.local_gossip_addr()])
            .config(loopback_config())
            .build()
            .await
            .expect("the observer builds")
    }

    /// One live-peers receiver per member, marked seen. chitchat fires a
    /// member's live-set watch exactly when the set of live gossip
    /// participants changes, and the membership loop then publishes its peers
    /// again, so a receiver changes once the member itself has counted a
    /// participant in or out. That is the member's own verdict, where an
    /// observer's failure detector holds its own arrival times.
    fn member_watches(clusters: &[&Cluster]) -> Vec<watch::Receiver<Vec<Peer>>> {
        clusters
            .iter()
            .map(|cluster| {
                let mut peers = cluster.peers_watch();
                peers.borrow_and_update();
                peers
            })
            .collect()
    }

    /// Waits until every watched member has republished its peers since
    /// `member_watches` marked them seen, and checks each value it sees holds
    /// `expected` peers.
    async fn every_member_republished(
        what: &str,
        watches: &mut [watch::Receiver<Vec<Peer>>],
        expected: usize,
    ) {
        for peers in watches {
            tokio::time::timeout(BOUND, peers.changed())
                .await
                .unwrap_or_else(|_| panic!("{what}: a member's live set did not change"))
                .unwrap_or_else(|_| panic!("{what}: a member stopped"));
            assert_eq!(peers.borrow_and_update().len(), expected, "{what}");
        }
    }

    fn live_members(observer: &Observer) -> usize {
        observer
            .snapshot()
            .members
            .iter()
            .filter(|member| member.status == MemberStatus::Live)
            .count()
    }

    fn status_of_node(snapshot: &ClusterSnapshot, node: NodeId) -> Option<MemberStatus> {
        snapshot
            .members
            .iter()
            .find(|member| member.peer.node == node)
            .map(|member| member.status)
    }

    /// What [`record`] collects while an observer runs.
    struct Recorder {
        /// Every status each node passes through, in order.
        history: Arc<StdMutex<Vec<(NodeId, MemberStatus)>>>,
        /// The first snapshot that shows a departing member.
        departing: Arc<StdMutex<Option<Arc<ClusterSnapshot>>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Recorder {
        /// The statuses `node` passed through, in order.
        fn statuses(&self, node: NodeId) -> Vec<MemberStatus> {
            self.history
                .lock()
                .expect("history lock")
                .iter()
                .filter(|(member, _)| *member == node)
                .map(|&(_, status)| status)
                .collect()
        }
    }

    /// Follows `observer`'s snapshots from the current one on.
    fn record(observer: &Observer) -> Recorder {
        let history: Arc<StdMutex<Vec<(NodeId, MemberStatus)>>> = Arc::default();
        let departing: Arc<StdMutex<Option<Arc<ClusterSnapshot>>>> = Arc::default();
        let mut updates = observer.subscribe();
        let task = tokio::spawn({
            let (history, departing) = (Arc::clone(&history), Arc::clone(&departing));
            async move {
                loop {
                    let snapshot = Arc::clone(&updates.borrow_and_update());
                    {
                        let mut history = history.lock().expect("history lock");
                        for member in &snapshot.members {
                            let last = history
                                .iter()
                                .rev()
                                .find(|(node, _)| *node == member.peer.node)
                                .map(|&(_, status)| status);
                            if last != Some(member.status) {
                                history.push((member.peer.node, member.status));
                            }
                        }
                    }
                    if snapshot
                        .members
                        .iter()
                        .any(|member| member.status == MemberStatus::Departing)
                    {
                        departing
                            .lock()
                            .expect("departing lock")
                            .get_or_insert(snapshot);
                    }
                    if updates.changed().await.is_err() {
                        return;
                    }
                }
            }
        });
        Recorder {
            history,
            departing,
            task,
        }
    }

    /// A graceful leave: Live, Departing, then Left for good, never Down.
    fn assert_leave_history(history: &[MemberStatus]) {
        assert_eq!(history.first(), Some(&MemberStatus::Live));
        assert!(
            !history.contains(&MemberStatus::Down),
            "a leave is no crash: {history:?}"
        );
        let departing = history
            .iter()
            .position(|status| *status == MemberStatus::Departing)
            .expect("the departure was seen");
        let first_left = history
            .iter()
            .position(|status| *status == MemberStatus::Left)
            .expect("the leave ended Left");
        assert!(
            departing < first_left,
            "Departing comes before Left: {history:?}"
        );
        assert!(
            history[first_left..]
                .iter()
                .all(|status| *status == MemberStatus::Left),
            "Left is final: {history:?}"
        );
    }

    /// A crash: Live, then Down, never a departure.
    fn assert_crash_history(history: &[MemberStatus]) {
        assert_eq!(history.first(), Some(&MemberStatus::Live));
        assert!(history.contains(&MemberStatus::Down), "{history:?}");
        assert!(
            !history.contains(&MemberStatus::Left) && !history.contains(&MemberStatus::Departing),
            "a crash is never a leave: {history:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_observer_sees_members_their_caches_and_ownership() {
        let name = "observe-it-sees";
        let [a, b, c] = trio(name).await;
        let owners = Mode::DEFAULT_OWNERS;
        let mut distributed_caches = Vec::new();
        for cluster in [&a, &b, &c] {
            distributed_caches.push(open(cluster, "d", Mode::distributed()).await);
        }
        let _replicated = [
            open(&a, "r", Mode::Replicated).await,
            open(&b, "r", Mode::Replicated).await,
        ];
        let observer = observe(name, &a).await;

        let expected = |with_r: bool| {
            let mut advertised = vec![("d", Mode::distributed())];
            if with_r {
                advertised.push(("r", Mode::Replicated));
            }
            caches(&advertised)
        };
        eventually(
            "the observer shows three live members and their caches",
            async || {
                let snapshot = observer.snapshot();
                snapshot.members.len() == 3
                    && snapshot.members.iter().all(|member| {
                        member.status == MemberStatus::Live
                            && member.caches == expected(member.peer.node != c.node_id())
                    })
            },
        )
        .await;
        let snapshot = observer.snapshot();
        assert_eq!(snapshot.cluster, name);
        assert_eq!(snapshot.anonymous, 0);
        for cluster in [&a, &b, &c] {
            for peer in cluster.peers() {
                let seen = snapshot
                    .members
                    .iter()
                    .find(|member| member.peer.node == peer.node)
                    .expect("a peer of a member is a member");
                assert_eq!(seen.peer, peer, "the record Cluster::peers reports");
            }
        }

        let shares = snapshot.ownership("d", owners).expect("three advertisers");
        assert!(shares.ranks_parts());
        assert_eq!(shares.eligible().len(), 3);
        let total: usize = shares
            .eligible()
            .iter()
            .map(|&node| shares.parts_owned_by(node))
            .sum();
        assert_eq!(total, 131_072, "two owners of each of 65,536 parts");
        eventually("every member computes the observer's view", async || {
            [&a, &b, &c].iter().all(|cluster| {
                registered_shard(cluster, &SmolStr::new("d")).ownership_view_hash()
                    == Some(shares.view_hash())
            })
        })
        .await;
        for key in 0..512u32 {
            let part = PartId::of_key(&encode_key(&key).expect("a u32 encodes"));
            for cache in &distributed_caches {
                assert_eq!(
                    shares.owners_of(part),
                    cache.owners_of(&key).as_slice(),
                    "key {key}"
                );
            }
        }

        observer.shutdown().await;
        for cluster in [a, b, c] {
            cluster.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_observer_tells_a_graceful_leave_from_a_crash() {
        let name = "observe-it-leave-crash";
        let [stays, leaves, crashes] = trio(name).await;
        for cluster in [&stays, &leaves, &crashes] {
            open(cluster, "d", Mode::distributed()).await;
        }
        let observer = observe(name, &stays).await;
        eventually("the observer shows three live members", async || {
            live_members(&observer) == 3
                && observer
                    .snapshot()
                    .members
                    .iter()
                    .all(|member| member.caches.contains_key("d"))
        })
        .await;
        let (stays_id, leaves_id, crashes_id) =
            (stays.node_id(), leaves.node_id(), crashes.node_id());

        let recorder = record(&observer);

        leaves.shutdown().await;
        crashes.crash().await;
        eventually(
            "the observer shows the leave as Left and the crash as Down",
            async || {
                let snapshot = observer.snapshot();
                status_of_node(&snapshot, leaves_id) == Some(MemberStatus::Left)
                    && status_of_node(&snapshot, crashes_id) == Some(MemberStatus::Down)
            },
        )
        .await;
        eventually("the recorder has followed both", async || {
            recorder.statuses(leaves_id).last() == Some(&MemberStatus::Left)
                && recorder.statuses(crashes_id).last() == Some(&MemberStatus::Down)
        })
        .await;
        recorder.task.abort();

        // Properties, not exact sequences: chitchat revives a node whose
        // newer heartbeat a peer still relays, so a crash can read Down, Live,
        // Down. A departure is final, and a crash is never a departure.
        assert_leave_history(&recorder.statuses(leaves_id));
        assert_crash_history(&recorder.statuses(crashes_id));
        let stay_history = recorder.statuses(stays_id);
        assert_eq!(stay_history.first(), Some(&MemberStatus::Live));
        assert!(
            !stay_history.contains(&MemberStatus::Departing)
                && !stay_history.contains(&MemberStatus::Left),
            "a member that never stops never leaves: {stay_history:?}"
        );

        let seen_departing = recorder
            .departing
            .lock()
            .expect("departing lock")
            .clone()
            .expect("the departure was seen");
        let shares = seen_departing
            .ownership("d", Mode::DEFAULT_OWNERS)
            .expect("members still advertise");
        assert!(
            !shares.eligible().contains(&leaves_id),
            "a departing member owns nothing"
        );
        assert!(shares.eligible().contains(&stays_id));

        let shares = observer
            .snapshot()
            .ownership("d", Mode::DEFAULT_OWNERS)
            .expect("one member stays");
        assert_eq!(
            shares.eligible(),
            [stays_id],
            "neither the left nor the down member"
        );

        observer.shutdown().await;
        stays.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_observer_shows_a_joining_member_live_and_never_down() {
        let name = "observe-it-join";
        let [a, b, c] = trio(name).await;
        let observer = observe(name, &a).await;
        eventually("the observer shows three live members", async || {
            live_members(&observer) == 3
        })
        .await;
        let recorder = record(&observer);

        let joiner = Cluster::builder(name)
            .seeds([a.local_gossip_addr()])
            .config(loopback_config())
            .build()
            .await
            .expect("the joiner builds");
        let joiner_id = joiner.node_id();
        eventually("the observer shows four live members", async || {
            live_members(&observer) == 4
        })
        .await;
        // Several refresh ticks past the join, so a late verdict would show.
        tokio::time::sleep(REFRESH_INTERVAL * 3).await;
        recorder.task.abort();

        assert_eq!(
            recorder.statuses(joiner_id),
            [MemberStatus::Live],
            "chitchat holds a node dead until its second heartbeat; the observer does not report that"
        );
        for node in [a.node_id(), b.node_id(), c.node_id()] {
            assert_eq!(recorder.statuses(node), [MemberStatus::Live]);
        }

        observer.shutdown().await;
        for cluster in [a, b, c, joiner] {
            cluster.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_observer_is_invisible_to_every_member() {
        let name = "observe-it-invisible";
        let [a, b, c] = trio(name).await;
        let clusters = [&a, &b, &c];
        let mut distributed_caches = Vec::new();
        let mut replicated = Vec::new();
        for cluster in clusters {
            distributed_caches.push(open(cluster, "d", Mode::distributed()).await);
            replicated.push(open(cluster, "r", Mode::Replicated).await);
        }
        let owners_now = |caches: &[Cache<u32, String>]| -> Vec<Vec<Vec<NodeId>>> {
            caches
                .iter()
                .map(|cache| (0..256u32).map(|key| cache.owners_of(&key)).collect())
                .collect()
        };
        eventually(
            "every member has settled on one ownership view",
            async || {
                let hashes: Vec<Option<u64>> = clusters
                    .iter()
                    .map(|cluster| {
                        registered_shard(cluster, &SmolStr::new("d")).ownership_view_hash()
                    })
                    .collect();
                hashes[0].is_some() && hashes.iter().all(|hash| *hash == hashes[0])
            },
        )
        .await;
        let before = owners_now(&distributed_caches);

        let mut watches = member_watches(&clusters);
        let observer = observe(name, &a).await;
        eventually("the observer shows three members", async || {
            live_members(&observer) == 3
        })
        .await;
        every_member_republished("the first observer joined", &mut watches, 2).await;
        for _ in 0..10 {
            for cluster in clusters {
                assert_eq!(cluster.peers().len(), 2, "the observer is no peer");
                assert_eq!(cluster.health().live_peers, 2);
            }
            assert_eq!(observer.snapshot().anonymous, 0);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let mut watches = member_watches(&clusters);
        let second = observe(name, &b).await;
        eventually("each observer counts the other as anonymous", async || {
            observer.snapshot().anonymous == 1
                && second.snapshot().anonymous == 1
                && live_members(&second) == 3
        })
        .await;
        every_member_republished("the second observer joined", &mut watches, 2).await;
        for cluster in clusters {
            assert_eq!(cluster.peers().len(), 2);
            assert_eq!(cluster.health().live_peers, 2);
        }

        for key in 0..100u32 {
            replicated[0]
                .insert(key, key.to_string())
                .await
                .expect("insert");
        }
        eventually(
            "replicated inserts converge with observers present",
            async || {
                let mut counts = Vec::new();
                for cache in &replicated {
                    counts.push(cache.entry_count().await);
                }
                counts.iter().all(|&count| count == 100)
            },
        )
        .await;
        assert_eq!(owners_now(&distributed_caches), before, "observers joined");

        // Each observer's departure fires every member's live-set watch once
        // that member's own failure detector drops it, and the membership loop
        // then resends its peers. The test waits for that signal on every
        // member, one observer at a time so the first drop cannot stand in for
        // the second, and only then checks that nothing moved.
        for (leaving, label) in [
            (second, "the second observer"),
            (observer, "the first observer"),
        ] {
            let mut watches = member_watches(&clusters);
            leaving.shutdown().await;
            every_member_republished(&format!("{label} left"), &mut watches, 2).await;
        }
        for cluster in clusters {
            assert_eq!(cluster.peers().len(), 2);
            assert_eq!(cluster.health().live_peers, 2);
        }
        assert_eq!(owners_now(&distributed_caches), before, "observers left");

        for cluster in [a, b, c] {
            cluster.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn observer_subscribe_wakes_on_a_change_and_not_on_heartbeats() {
        let name = "observe-it-subscribe";
        let [a, b, c] = trio(name).await;
        let observer = observe(name, &a).await;
        eventually("the observer shows three members", async || {
            live_members(&observer) == 3
        })
        .await;

        let mut updates = observer.subscribe();
        assert!(
            !updates.has_changed().expect("the observer runs"),
            "a new subscription starts with the current snapshot seen"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            !updates.has_changed().expect("the observer runs"),
            "gossip heartbeats publish nothing"
        );

        open(&b, "fresh", Mode::Replicated).await;
        let b_id = b.node_id();
        tokio::time::timeout(BOUND, async {
            loop {
                updates
                    .changed()
                    .await
                    .expect("the observer runs while it is awaited");
                let snapshot = Arc::clone(&updates.borrow_and_update());
                let advertises = snapshot.members.iter().any(|member| {
                    member.peer.node == b_id
                        && member.caches == caches(&[("fresh", Mode::Replicated)])
                });
                if advertises {
                    return;
                }
            }
        })
        .await
        .expect("opening a cache publishes within the bound");

        observer.shutdown().await;
        for cluster in [a, b, c] {
            cluster.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn observer_builder_honors_discovery_and_config() {
        let name = "observe-it-builder";
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("a builds");

        let observer = Observer::builder(name)
            .discovery(Static::new([a.local_gossip_addr()]))
            .config(loopback_config())
            .build()
            .await
            .expect("the observer builds");

        let addr = observer.local_gossip_addr();
        assert!(addr.ip().is_loopback(), "gossip_bind_addr is honored");
        assert_ne!(addr.port(), 0, "port 0 resolves to a bound port");
        let rendered = format!("{observer:?}");
        assert!(rendered.contains(name) && rendered.contains(&addr.to_string()));
        eventually("the custom discovery finds the cluster", async || {
            live_members(&observer) == 1
        })
        .await;
        assert_eq!(observer.snapshot().members[0].peer.node, a.node_id());

        let unbindable = Observer::builder(name)
            .seeds([a.local_gossip_addr()])
            .config(ClusterConfig {
                gossip_bind_addr: addr,
                ..loopback_config()
            })
            .build()
            .await;
        assert!(
            matches!(unbindable, Err(JoinError::Membership(_))),
            "a taken fixed gossip_bind_addr is a membership error, not a probe failure: {unbindable:?}"
        );

        observer.shutdown().await;
        a.shutdown().await;
    }

    /// Builds an observer over a seed stream that ends `lag` after its one
    /// address, then checks it finds the member, still publishes a change
    /// once the stream has ended, and shuts down.
    async fn assert_observer_survives_ending_seeds(name: &str, lag: Duration) {
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("a builds");
        let observer = Observer::builder(name)
            .discovery(EndingDiscovery {
                addrs: vec![a.local_gossip_addr()],
                lag,
            })
            .config(loopback_config())
            .build()
            .await
            .expect("the observer builds");
        eventually("the observer finds the member", async || {
            live_members(&observer) == 1
        })
        .await;

        // Outwait the end of the seed stream, then change the cluster.
        time::sleep(lag + Duration::from_secs(1)).await;
        open(&a, "late", Mode::Replicated).await;
        eventually(
            "the observer sees a change after the stream ended",
            async || {
                observer
                    .snapshot()
                    .members
                    .iter()
                    .any(|member| member.caches.contains_key("late"))
            },
        )
        .await;

        let stopped = time::timeout(Duration::from_secs(10), observer.shutdown()).await;
        assert!(stopped.is_ok(), "shutdown completes after the stream ended");
        a.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_observer_keeps_watching_after_its_seed_stream_ends() {
        assert_observer_survives_ending_seeds("observe-it-ending-seeds", Duration::ZERO).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_observer_keeps_watching_after_its_seed_stream_ends_mid_run() {
        // Past `INITIAL_SEED_WINDOW`, so the observe loop itself sees the end.
        assert_observer_survives_ending_seeds(
            "observe-it-ending-seeds-late",
            Duration::from_millis(1_500),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn observer_shutdown_returns_and_a_fresh_observer_sees_every_member() {
        let name = "observe-it-shutdown";
        let [a, b, c] = trio(name).await;
        let first = observe(name, &a).await;
        eventually("the first observer shows three members", async || {
            live_members(&first) == 3
        })
        .await;

        let clone = first.clone();
        let mut updates = clone.subscribe();
        first.shutdown().await;
        assert_eq!(
            live_members(&clone),
            3,
            "the last snapshot stays readable after the loop stops"
        );
        let closed =
            tokio::time::timeout(BOUND, async { while updates.changed().await.is_ok() {} }).await;
        assert!(
            closed.is_ok(),
            "a receiver from a surviving clone learns that the observer stopped"
        );

        let second = observe(name, &b).await;
        // The first observer stays live in the members' gossip until the
        // failure detector drops it, and counts as anonymous until then.
        eventually(
            "a fresh observer shows every member and no stale observer",
            async || live_members(&second) == 3 && second.snapshot().anonymous == 0,
        )
        .await;

        second.shutdown().await;
        for cluster in [a, b, c] {
            cluster.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn clones_share_one_session_and_dropping_the_last_stops_it() {
        let name = "observe-it-drop";
        let a = Cluster::builder(name)
            .seeds(std::iter::empty())
            .config(loopback_config())
            .build()
            .await
            .expect("a builds");
        let observer = observe(name, &a).await;
        let clone = observer.clone();
        assert_eq!(clone.local_gossip_addr(), observer.local_gossip_addr());
        let mut updates = observer.subscribe();

        drop(observer);
        open(&a, "late", Mode::Replicated).await;
        eventually("a surviving clone keeps watching", async || {
            clone
                .snapshot()
                .members
                .iter()
                .any(|member| member.caches.contains_key("late"))
        })
        .await;

        drop(clone);
        // The loop holds the only sender; the channel closes when it stops.
        let closed = tokio::time::timeout(Duration::from_secs(10), async {
            while updates.changed().await.is_ok() {}
        })
        .await;
        assert!(closed.is_ok(), "the loop outlived every clone");

        a.shutdown().await;
    }
}
