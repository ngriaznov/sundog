//! What the views read from the model: the node rows, the cache rows and the
//! cluster-wide totals, derived once so every view agrees.
//!
//! Every function is pure over a [`Model`].

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::observe::{Member, MemberStatus};
use sundog::store::Mode;

use super::text;
use super::theme::{self, Rgb};
use crate::model::derive::{self, FetchMix};
use crate::model::metrics::NodeMetrics;
use crate::model::series::RING_LEN;
use crate::model::{Model, Slot};
use crate::source::names;

/// How long after a node first shows live its row reads as newly joined.
pub const JOINED_FOR: Duration = Duration::from_secs(10);

/// How long a rejoined node keeps its `↻` badge.
pub const REJOINED_FOR: Duration = Duration::from_secs(60);

/// One node on screen: the newest incarnation at one gossip address.
#[derive(Debug, Clone, Copy)]
pub struct NodeRow<'a> {
    /// The member.
    pub member: &'a Member,
    /// Its slot: label and color index.
    pub slot: &'a Slot,
    /// Whether an older incarnation or identity also sits at the address.
    pub rejoined: bool,
}

impl NodeRow<'_> {
    /// The node's color.
    #[must_use]
    pub fn color(&self) -> Rgb {
        theme::node_color(self.slot.index)
    }

    /// The slot label: `n1`.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.slot.label
    }

    /// The node id as 16 hex digits.
    #[must_use]
    pub fn full_id(&self) -> String {
        self.member.peer.node.to_string()
    }

    /// The first four hex digits of the node id.
    #[must_use]
    pub fn short_id(&self) -> String {
        text::short_id(&self.full_id()).to_owned()
    }

    /// The member's lifecycle status.
    #[must_use]
    pub fn status(&self) -> MemberStatus {
        self.member.status
    }

    /// Whether the member is `Down` or `Left`.
    #[must_use]
    pub fn is_gone(&self) -> bool {
        matches!(self.member.status, MemberStatus::Down | MemberStatus::Left)
    }

    /// How long ago the observer first saw the member in its status.
    #[must_use]
    pub fn status_age(&self, wall: SystemTime) -> Duration {
        wall.duration_since(self.member.since).unwrap_or_default()
    }
}

/// How a node is named in the event log: its label, short id and color.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTag {
    /// The slot label, or `??` for a node the model has not seen.
    pub label: SmolStr,
    /// The first four hex digits of the node id.
    pub short: String,
    /// The node's color; the muted color for an unknown node.
    pub color: Rgb,
}

/// The sort group of a status: live and departing first, then down, then
/// left.
const fn group(status: MemberStatus) -> u8 {
    match status {
        MemberStatus::Live | MemberStatus::Departing => 0,
        MemberStatus::Down => 1,
        MemberStatus::Left => 2,
        _ => 3,
    }
}

/// Every gossip address the model has seen, as its newest incarnation: live
/// and departing nodes first, then down, then left, each in slot order.
#[must_use]
pub fn all_node_rows(model: &Model) -> Vec<NodeRow<'_>> {
    let Some(snapshot) = model.snapshot() else {
        return Vec::new();
    };
    let mut newest: BTreeMap<SocketAddr, (&Member, usize)> = BTreeMap::new();
    for member in &snapshot.members {
        let entry = newest.entry(member.peer.gossip_addr).or_insert((member, 0));
        entry.1 += 1;
        if member.peer.incarnation >= entry.0.peer.incarnation {
            entry.0 = member;
        }
    }
    let mut rows: Vec<NodeRow<'_>> = newest
        .into_iter()
        .filter_map(|(addr, (member, entries))| {
            Some(NodeRow {
                member,
                slot: model.slots().get(addr)?,
                rejoined: entries > 1,
            })
        })
        .collect();
    rows.sort_by_key(|row| (group(row.status()), row.slot.index));
    rows
}

/// The rows to draw: a gone node shows until `forget_after` has passed since
/// it went, or always with `show_all`.
#[must_use]
pub fn node_rows(
    model: &Model,
    wall: SystemTime,
    show_all: bool,
    forget_after: Duration,
) -> Vec<NodeRow<'_>> {
    let mut rows = all_node_rows(model);
    rows.retain(|row| !row.is_gone() || show_all || row.status_age(wall) < forget_after);
    rows
}

/// The row of the node at gossip address `addr`.
#[must_use]
pub fn row_at(model: &Model, addr: SocketAddr) -> Option<NodeRow<'_>> {
    all_node_rows(model)
        .into_iter()
        .find(|row| row.member.peer.gossip_addr == addr)
}

/// The row of the slot labeled `label`.
#[must_use]
pub fn row_labeled<'a>(model: &'a Model, label: &str) -> Option<NodeRow<'a>> {
    all_node_rows(model)
        .into_iter()
        .find(|row| row.slot.label == label)
}

/// How `node` is named: by the slot of the address it was last seen at.
#[must_use]
pub fn tag_of(model: &Model, node: NodeId) -> NodeTag {
    let newest = model.snapshot().and_then(|snapshot| {
        snapshot
            .members
            .iter()
            .filter(|member| member.peer.node == node)
            .max_by_key(|member| member.peer.incarnation)
    });
    let full = node.to_string();
    let short = text::short_id(&full).to_owned();
    match newest.and_then(|member| model.slots().get(member.peer.gossip_addr)) {
        Some(slot) => NodeTag {
            label: slot.label.clone(),
            short,
            color: theme::node_color(slot.index),
        },
        None => NodeTag {
            label: SmolStr::new_static("??"),
            short,
            color: theme::MUTED,
        },
    }
}

/// How the node at gossip address `addr` is named, when it has a slot.
#[must_use]
pub fn tag_at(model: &Model, addr: SocketAddr) -> Option<NodeTag> {
    let slot = model.slots().get(addr)?;
    let newest = model
        .snapshot()?
        .members
        .iter()
        .filter(|member| member.peer.gossip_addr == addr)
        .max_by_key(|member| member.peer.incarnation)?;
    let full = newest.peer.node.to_string();
    Some(NodeTag {
        label: slot.label.clone(),
        short: text::short_id(&full).to_owned(),
        color: theme::node_color(slot.index),
    })
}

/// The tag of the node whose id the exporter labels as `peer` (16 hex
/// digits), if the model knows it.
#[must_use]
pub fn tag_of_hex(model: &Model, peer: &str) -> Option<NodeTag> {
    let node: NodeId = peer.parse().ok()?;
    let known = model
        .snapshot()?
        .members
        .iter()
        .any(|member| member.peer.node == node);
    known.then(|| tag_of(model, node))
}

/// The letter and owner count that name a mode: `D` with 2 for
/// `Distributed { owners: 2 }`, `R`, `I`, `L`; `?` for a mode this build does
/// not know.
#[must_use]
pub fn mode_letter(mode: Mode) -> (char, Option<u8>) {
    match mode {
        Mode::Local => ('L', None),
        Mode::Invalidation => ('I', None),
        Mode::Replicated => ('R', None),
        Mode::Distributed { owners } => ('D', Some(owners.get())),
        _ => ('?', None),
    }
}

/// A mode as the event log writes it: `D2`, `R`.
#[must_use]
pub fn mode_short(mode: Mode) -> String {
    match mode_letter(mode) {
        (letter, Some(owners)) => format!("{letter}{owners}"),
        (letter, None) => letter.to_string(),
    }
}

/// A mode as the tables write it: `D·2`, `R`.
#[must_use]
pub fn mode_dotted(mode: Mode) -> String {
    match mode_letter(mode) {
        (letter, Some(owners)) => format!("{letter}·{owners}"),
        (letter, None) => letter.to_string(),
    }
}

/// A mode as the one-shot report writes it: `distributed:2`, `replicated`.
#[must_use]
pub fn mode_token(mode: Mode) -> String {
    match mode {
        Mode::Local => "local".to_owned(),
        Mode::Invalidation => "invalidation".to_owned(),
        Mode::Replicated => "replicated".to_owned(),
        Mode::Distributed { owners } => format!("distributed:{owners}"),
        _ => "unknown".to_owned(),
    }
}

/// A mode spelled out: `distributed k=2`, `replicated`.
#[must_use]
pub fn mode_name(mode: Mode) -> String {
    match mode {
        Mode::Local => "local".to_owned(),
        Mode::Invalidation => "invalidation".to_owned(),
        Mode::Replicated => "replicated".to_owned(),
        Mode::Distributed { owners } => format!("distributed k={owners}"),
        _ => "unknown".to_owned(),
    }
}

/// One cache as gossip shows it across the live members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheRow {
    /// The cache name.
    pub name: SmolStr,
    /// The mode every advertiser agrees on; `None` while they disagree.
    pub mode: Option<Mode>,
    /// The live and departing members that advertise the cache.
    pub advertisers: Vec<NodeId>,
    /// Each advertiser's mode, in advertiser order.
    pub modes: Vec<(NodeId, Mode)>,
}

impl CacheRow {
    /// Whether the cache is `Distributed`.
    #[must_use]
    pub const fn is_distributed(&self) -> bool {
        matches!(self.mode, Some(Mode::Distributed { .. }))
    }

    /// Whether the advertisers disagree on the mode.
    #[must_use]
    pub const fn is_conflicted(&self) -> bool {
        self.mode.is_none()
    }
}

/// Every cache a live or departing member advertises. `Distributed` caches
/// come first, then the others by name.
#[must_use]
pub fn cache_rows(model: &Model) -> Vec<CacheRow> {
    let Some(snapshot) = model.snapshot() else {
        return Vec::new();
    };
    let mut by_name: BTreeMap<SmolStr, Vec<(NodeId, Mode)>> = BTreeMap::new();
    for member in snapshot
        .members
        .iter()
        .filter(|member| member.status.is_live())
    {
        for (name, &mode) in &member.caches {
            by_name
                .entry(name.clone())
                .or_default()
                .push((member.peer.node, mode));
        }
    }
    let mut rows: Vec<CacheRow> = by_name
        .into_iter()
        .map(|(name, modes)| {
            let first = modes.first().map(|&(_, mode)| mode);
            let agreed = first.filter(|first| modes.iter().all(|(_, mode)| mode == first));
            CacheRow {
                name,
                mode: agreed,
                advertisers: modes.iter().map(|&(node, _)| node).collect(),
                modes,
            }
        })
        .collect();
    rows.sort_by_key(|row| !row.is_distributed());
    rows
}

/// The names of the `Distributed` caches, in [`cache_rows`] order.
#[must_use]
pub fn distributed_caches(model: &Model) -> Vec<SmolStr> {
    cache_rows(model)
        .into_iter()
        .filter(CacheRow::is_distributed)
        .map(|row| row.name)
        .collect()
}

/// The `Distributed` cache the Ownership panel and the SHARE column show:
/// `preferred` when it is one, else the first.
#[must_use]
pub fn ownership_cache(model: &Model, preferred: Option<&str>) -> Option<SmolStr> {
    let caches = distributed_caches(model);
    preferred
        .and_then(|wanted| caches.iter().find(|name| *name == wanted))
        .or_else(|| caches.first())
        .cloned()
}

/// The members that are live or departing, as the count the header shows.
#[must_use]
pub fn live_count(model: &Model) -> usize {
    all_node_rows(model)
        .iter()
        .filter(|row| row.status().is_live())
        .count()
}

/// The metrics of the live member at `addr`, when its exporter has answered
/// for the node now at the address.
#[must_use]
pub fn metrics_of<'a>(model: &'a Model, row: &NodeRow<'_>) -> Option<&'a NodeMetrics> {
    model
        .metrics(row.member.peer.gossip_addr)
        .filter(|metrics| metrics.node() == row.member.peer.node && metrics.folds() > 0)
}

/// Cluster-wide traffic: the sum of every live node's metrics.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Throughput {
    /// Reads and fetches per second, one sample per second, oldest first.
    pub ops: Vec<f64>,
    /// Bytes sent per second now.
    pub tx_bytes: Option<f64>,
    /// Cache reads (hits and misses) per second now.
    pub reads: Option<f64>,
    /// Fetches per second now.
    pub fetch: Option<f64>,
    /// Forwarded writes per second now.
    pub forwarded: Option<f64>,
    /// Anti-entropy repairs per second now.
    pub repairs: Option<f64>,
    /// The share of reads that hit.
    pub hit_ratio: Option<f64>,
    /// How many live nodes contribute.
    pub nodes: usize,
}

/// Adds `series` into `total`, aligning their newest samples.
fn add_aligned(total: &mut Vec<f64>, series: &[f64]) {
    if series.len() > total.len() {
        let mut grown = vec![0.0; series.len() - total.len()];
        grown.append(total);
        *total = grown;
    }
    let offset = total.len() - series.len();
    for (slot, value) in total[offset..].iter_mut().zip(series) {
        *slot += value;
    }
}

/// The sum of an optional per-node rate over the nodes that report it.
fn sum_option(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    values.fold(None, |total, value| match (total, value) {
        (None, other) => other,
        (some, None) => some,
        (Some(a), Some(b)) => Some(a + b),
    })
}

/// The live nodes with metrics.
fn live_metrics(model: &Model) -> impl Iterator<Item = &NodeMetrics> {
    all_node_rows(model)
        .into_iter()
        .filter(|row| row.status().is_live())
        .filter_map(|row| metrics_of(model, &row))
}

/// The traffic totals of the live nodes with metrics.
#[must_use]
pub fn throughput(model: &Model) -> Throughput {
    let nodes: Vec<&NodeMetrics> = live_metrics(model).collect();
    let mut total = Throughput {
        nodes: nodes.len(),
        ..Throughput::default()
    };
    for metrics in &nodes {
        add_aligned(&mut total.ops, &metrics.ops().to_vec());
    }
    total.tx_bytes = sum_option(nodes.iter().map(|m| m.tx_bytes().last()));
    let hits = sum_option(nodes.iter().map(|m| m.rate_sum(names::CACHE_HITS)));
    let misses = sum_option(nodes.iter().map(|m| m.rate_sum(names::CACHE_MISSES)));
    total.reads = match (hits, misses) {
        (None, None) => None,
        (a, b) => Some(a.unwrap_or(0.0) + b.unwrap_or(0.0)),
    };
    total.hit_ratio = derive::hit_ratio(hits.unwrap_or(0.0), misses.unwrap_or(0.0));
    total.fetch = sum_option(nodes.iter().map(|m| m.rate_sum(names::FETCH)));
    total.forwarded = sum_option(nodes.iter().map(|m| m.rate_sum(names::FORWARDED_WRITES)));
    total.repairs = sum_option(nodes.iter().map(|m| m.rate_sum(names::AE_REPAIRED)));
    total
}

/// The relative change of the newest samples of `series` against those ten
/// samples earlier: `0.12` for 12% up. `None` with too little history or a
/// zero base.
#[must_use]
pub fn trend(series: &[f64]) -> Option<f64> {
    const WINDOW: usize = 3;
    const BACK: usize = 10;
    if series.len() < WINDOW + BACK {
        return None;
    }
    let mean = |slice: &[f64]| slice.iter().sum::<f64>() / crate::model::count_to_f64(slice.len());
    let end = series.len();
    let now = mean(&series[end - WINDOW..]);
    let before = mean(&series[end - WINDOW - BACK..end - BACK]);
    (before > 0.0).then(|| now / before - 1.0)
}

/// The mix of `cache` fetch outcomes across the live nodes; `None` at zero
/// traffic.
#[must_use]
pub fn cluster_fetch_mix(model: &Model, cache: &str) -> Option<FetchMix> {
    let outcome = |name: &str| -> f64 {
        live_metrics(model)
            .filter_map(|m| m.rate(names::FETCH, &[("cache", cache), ("outcome", name)]))
            .sum()
    };
    derive::fetch_mix(
        outcome("local"),
        outcome("remote"),
        outcome("miss"),
        outcome("error"),
    )
}

/// The parts of `cache` pulled in and released per second across the live
/// nodes, one sample per second, oldest first.
#[must_use]
pub fn rebalance_series(model: &Model, cache: &str) -> (Vec<f64>, Vec<f64>) {
    let mut into = Vec::new();
    let mut out = Vec::new();
    for metrics in live_metrics(model) {
        if let Some(ring) = metrics.rebalance_in(cache) {
            add_aligned(&mut into, &ring.to_vec());
        }
        if let Some(ring) = metrics.rebalance_out(cache) {
            add_aligned(&mut out, &ring.to_vec());
        }
    }
    (into, out)
}

/// The keys `cache` holds across the cluster, as the live nodes report them:
/// for a `Distributed` cache the entries summed over its `k` owners, divided
/// by `k`; for any other mode the largest count. `None` without a report.
#[must_use]
pub fn key_estimate(model: &Model, row: &CacheRow) -> Option<f64> {
    let entries: Vec<f64> = live_metrics(model)
        .filter(|metrics| row.advertisers.contains(&metrics.node()))
        .filter_map(|metrics| metrics.entries(&row.name))
        .collect();
    if entries.is_empty() {
        return None;
    }
    match row.mode {
        Some(Mode::Distributed { owners }) => {
            let owners = usize::from(owners.get()).min(row.advertisers.len().max(1));
            Some(entries.iter().sum::<f64>() / crate::model::count_to_f64(owners))
        }
        _ => Some(entries.iter().copied().fold(0.0, f64::max)),
    }
}

/// How a `Distributed` cache's ownership view stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewState {
    /// The view hash.
    pub hash: u64,
    /// Whether the view ranks single parts rather than whole buckets.
    pub ranks_parts: bool,
    /// Whether the cache has settled since its last view change.
    pub settled: bool,
    /// Whether the verdict rests on gossip alone.
    pub gossip_only: bool,
    /// The time since the last `VIEW` event of the cache, when the log holds
    /// one.
    pub since: Option<Duration>,
    /// The owner slots that event moved.
    pub moved: Option<usize>,
    /// The wall-clock time of that event.
    pub changed_at: Option<SystemTime>,
}

/// The view state of `cache` at wall-clock time `wall`; `None` without an
/// ownership digest.
#[must_use]
pub fn view_state(model: &Model, cache: &str, wall: SystemTime) -> Option<ViewState> {
    let digest = model.ownership(cache)?;
    let settle = model.settle(cache)?;
    let last = model
        .events()
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            crate::model::events::EventKind::View {
                cache: name, moved, ..
            } if name == cache => Some((event.at, *moved)),
            _ => None,
        });
    Some(ViewState {
        hash: digest.view_hash,
        ranks_parts: digest.ranks_parts,
        settled: settle.settled,
        gossip_only: settle.gossip_only,
        since: last.map(|(at, _)| wall.duration_since(at).unwrap_or_default()),
        moved: last.map(|(_, moved)| moved),
        changed_at: last.map(|(at, _)| at),
    })
}

/// How many live nodes have a working exporter, out of the live nodes.
/// `None` when no node has ever been scraped.
#[must_use]
pub fn exporter_summary(model: &Model) -> Option<(usize, usize)> {
    let rows = all_node_rows(model);
    let live: Vec<_> = rows.iter().filter(|row| row.status().is_live()).collect();
    let seen = rows
        .iter()
        .any(|row| model.exporter(row.member.peer.gossip_addr).is_some());
    seen.then(|| {
        let working = live
            .iter()
            .filter(|row| {
                metrics_of(model, row).is_some()
                    && !model
                        .exporter(row.member.peer.gossip_addr)
                        .is_some_and(crate::model::ExporterState::unreachable)
            })
            .count();
        (working, live.len())
    })
}

/// How many seconds of history a ring of `width` columns shows: two samples
/// per column, at most a full ring.
#[must_use]
pub fn window_seconds(width: usize) -> usize {
    (width * 2).min(RING_LEN)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use sundog::observe::ClusterSnapshot;

    use super::*;
    use crate::model::testkit;
    use crate::source::Update;

    fn fixture() -> Model {
        testkit::fixture_model(Instant::now())
    }

    #[test]
    fn rows_group_live_then_down_then_left_in_slot_order() {
        let model = fixture();
        let rows = all_node_rows(&model);
        let shape: Vec<_> = rows
            .iter()
            .map(|r| (r.label().to_owned(), r.status()))
            .collect();
        assert_eq!(
            shape,
            [
                ("n1".to_owned(), MemberStatus::Live),
                ("n2".to_owned(), MemberStatus::Live),
                ("n3".to_owned(), MemberStatus::Live),
                ("n4".to_owned(), MemberStatus::Live),
                ("n5".to_owned(), MemberStatus::Live),
                ("n6".to_owned(), MemberStatus::Departing),
                ("n7".to_owned(), MemberStatus::Down),
                ("n8".to_owned(), MemberStatus::Left),
            ]
        );
        assert_eq!(live_count(&model), 6);
        assert!(rows.iter().all(|r| !r.rejoined));
    }

    #[test]
    fn gone_rows_hide_after_the_forget_window_unless_all_are_shown() {
        let model = fixture();
        let wall = model.wall().unwrap();
        let visible = |show_all, forget| {
            node_rows(&model, wall, show_all, forget)
                .iter()
                .map(|r| r.label().to_owned())
                .collect::<Vec<_>>()
        };
        // The fixture's gone members were seen at the epoch, long before `wall`.
        assert_eq!(
            visible(false, Duration::from_secs(90)).len(),
            8,
            "wall is 20 s after the epoch: still inside the window"
        );
        let later = wall + Duration::from_secs(200);
        let hidden = node_rows(&model, later, false, Duration::from_secs(90));
        assert_eq!(hidden.len(), 6);
        assert!(hidden.iter().all(|r| !r.is_gone()));
        let shown = node_rows(&model, later, true, Duration::from_secs(90));
        assert_eq!(shown.len(), 8);
    }

    #[test]
    fn a_restarted_address_is_one_row_marked_rejoined() {
        let mut model = Model::new();
        let now = Instant::now();
        let snapshot = ClusterSnapshot::new(
            "c",
            vec![
                testkit::member_at(1, 0, 1, MemberStatus::Down),
                testkit::member_at(1, 1, 2, MemberStatus::Live),
            ],
            0,
        );
        model.apply(
            Update::Snapshot(std::sync::Arc::new(snapshot), now),
            now,
            SystemTime::UNIX_EPOCH,
        );
        let rows = all_node_rows(&model);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].rejoined);
        assert_eq!(rows[0].status(), MemberStatus::Live);
        assert_eq!(rows[0].member.peer.node, testkit::node_id(1, 1));
        assert_eq!(rows[0].full_id().len(), 16);
        assert_eq!(rows[0].short_id().len(), 4);
        assert_eq!(
            row_at(&model, testkit::gossip_addr(1)).unwrap().label(),
            "n1"
        );
        assert!(row_at(&model, testkit::gossip_addr(9)).is_none());
        assert_eq!(
            row_labeled(&model, "n1").unwrap().color(),
            theme::NODE_COLORS[0]
        );
        assert!(row_labeled(&model, "zz").is_none());
    }

    #[test]
    fn nodes_are_tagged_by_slot_or_marked_unknown() {
        let model = fixture();
        let tag = tag_of(&model, testkit::node_id(2, 0));
        assert_eq!(tag.label, "n2");
        assert_eq!(tag.color, theme::NODE_COLORS[1]);
        assert_eq!(tag.short.len(), 4);
        let unknown = tag_of(&model, NodeId::from(7));
        assert_eq!(unknown.label, "??");
        assert_eq!(unknown.color, theme::MUTED);
        assert_eq!(tag_at(&model, testkit::gossip_addr(3)).unwrap().label, "n3");
        assert!(tag_at(&model, testkit::gossip_addr(30)).is_none());
        let hex = testkit::node_id(4, 0).to_string();
        assert_eq!(tag_of_hex(&model, &hex).unwrap().label, "n4");
        assert!(tag_of_hex(&model, "zz").is_none());
        assert!(tag_of_hex(&model, &NodeId::from(7).to_string()).is_none());
    }

    #[test]
    fn modes_have_letters_and_names() {
        assert_eq!(mode_letter(Mode::Replicated), ('R', None));
        assert_eq!(mode_letter(Mode::Invalidation), ('I', None));
        assert_eq!(mode_letter(Mode::Local), ('L', None));
        assert_eq!(mode_letter(testkit::distributed(3)), ('D', Some(3)));
        assert_eq!(mode_short(testkit::distributed(2)), "D2");
        assert_eq!(mode_short(Mode::Replicated), "R");
        assert_eq!(mode_dotted(testkit::distributed(2)), "D·2");
        assert_eq!(mode_dotted(Mode::Invalidation), "I");
        assert_eq!(mode_name(testkit::distributed(2)), "distributed k=2");
        assert_eq!(mode_token(testkit::distributed(2)), "distributed:2");
        assert_eq!(mode_token(Mode::Replicated), "replicated");
        assert_eq!(mode_token(Mode::Local), "local");
        assert_eq!(mode_token(Mode::Invalidation), "invalidation");
        assert_eq!(mode_name(Mode::Local), "local");
        assert_eq!(mode_name(Mode::Invalidation), "invalidation");
        assert_eq!(mode_name(Mode::Replicated), "replicated");
    }

    #[test]
    fn caches_list_distributed_first_and_count_live_advertisers() {
        let model = fixture();
        let rows = cache_rows(&model);
        let names: Vec<_> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["it", "churn", "os", "pn"]);
        assert!(rows[0].is_distributed() && !rows[1].is_distributed());
        // Five live and one departing member advertise each cache.
        assert!(rows.iter().all(|r| r.advertisers.len() == 6));
        assert_eq!(distributed_caches(&model), ["it"]);
        assert_eq!(ownership_cache(&model, Some("it")).unwrap(), "it");
        assert_eq!(ownership_cache(&model, Some("churn")).unwrap(), "it");
        assert_eq!(ownership_cache(&model, None).unwrap(), "it");
        assert!(ownership_cache(&Model::new(), Some("it")).is_none());
        assert!(cache_rows(&Model::new()).is_empty());
    }

    #[test]
    fn disagreeing_modes_mark_the_cache_conflicted() {
        let mut model = Model::new();
        let now = Instant::now();
        let snapshot = ClusterSnapshot::new(
            "c",
            vec![
                testkit::member_with(1, 0, 1, MemberStatus::Live, &[("x", Mode::Replicated)]),
                testkit::member_with(
                    2,
                    0,
                    1,
                    MemberStatus::Live,
                    &[("x", testkit::distributed(2))],
                ),
            ],
            0,
        );
        model.apply(
            Update::Snapshot(std::sync::Arc::new(snapshot), now),
            now,
            SystemTime::UNIX_EPOCH,
        );
        let rows = cache_rows(&model);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_conflicted());
        assert!(!rows[0].is_distributed());
        assert_eq!(rows[0].modes.len(), 2);
    }

    #[test]
    fn aligned_sums_line_up_the_newest_samples() {
        let mut total = vec![1.0, 1.0];
        add_aligned(&mut total, &[10.0, 20.0, 30.0]);
        assert_eq!(total, [10.0, 21.0, 31.0]);
        add_aligned(&mut total, &[5.0]);
        assert_eq!(total, [10.0, 21.0, 36.0]);
        let mut empty = Vec::new();
        add_aligned(&mut empty, &[]);
        assert!(empty.is_empty());
    }

    #[test]
    fn optional_sums_skip_missing_nodes() {
        assert_eq!(sum_option([None, None].into_iter()), None);
        assert_eq!(
            sum_option([Some(1.0), None, Some(2.0)].into_iter()),
            Some(3.0)
        );
    }

    #[test]
    fn trend_compares_recent_samples_with_ten_earlier() {
        assert_eq!(trend(&[1.0; 12]), None);
        let flat = vec![100.0; 30];
        assert!((trend(&flat).unwrap()).abs() < 1e-12);
        let mut rising = vec![100.0; 30];
        rising[27..].fill(112.0);
        assert!((trend(&rising).unwrap() - 0.12).abs() < 1e-9);
        assert_eq!(trend(&[0.0; 30]), None);
    }

    #[test]
    fn a_model_without_metrics_has_no_traffic() {
        let model = fixture();
        let total = throughput(&model);
        assert_eq!(total, Throughput::default());
        assert_eq!(cluster_fetch_mix(&model, "it"), None);
        assert_eq!(rebalance_series(&model, "it"), (Vec::new(), Vec::new()));
        let row = &cache_rows(&model)[0];
        assert_eq!(key_estimate(&model, row), None);
        assert!(metrics_of(&model, &all_node_rows(&model)[0]).is_none());
    }

    #[test]
    fn windows_hold_two_samples_per_column_up_to_a_full_ring() {
        assert_eq!(window_seconds(10), 20);
        assert_eq!(window_seconds(56), 112);
        assert_eq!(window_seconds(500), RING_LEN);
    }

    #[test]
    fn status_age_counts_from_when_the_status_began() {
        let model = fixture();
        let rows = all_node_rows(&model);
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        assert_eq!(rows[0].status_age(wall), Duration::from_secs(30));
        assert_eq!(rows[0].status_age(SystemTime::UNIX_EPOCH), Duration::ZERO);
    }

    #[test]
    fn a_node_reads_as_joined_and_rejoined_for_the_documented_spans() {
        assert_eq!(JOINED_FOR, Duration::from_secs(10));
        assert_eq!(REJOINED_FOR, Duration::from_secs(60));
    }

    #[test]
    fn the_view_state_reports_the_hash_the_settle_verdict_and_the_last_view_change() {
        let model = fixture();
        let wall = model.wall().unwrap() + Duration::from_secs(5);
        let state = view_state(&model, "it", wall).expect("it has a digest");
        assert_eq!(state.hash, model.ownership("it").unwrap().view_hash);
        assert!(state.ranks_parts && state.settled && state.gossip_only);
        // The last VIEW event is at 10 s; the wall is 25 s.
        assert_eq!(state.since, Some(Duration::from_secs(15)));
        assert_eq!(
            state.changed_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10))
        );
        assert!(state.moved.is_some_and(|moved| moved > 0));
        assert_eq!(view_state(&model, "nope", wall), None);
        let live = testkit::fixture_model_with_metrics(Instant::now());
        let unsettled = view_state(&live, "it", live.wall().unwrap()).unwrap();
        assert!(!unsettled.settled && !unsettled.gossip_only);
    }

    #[test]
    fn the_exporter_summary_counts_the_nodes_whose_exporter_answers() {
        assert_eq!(exporter_summary(&fixture()), None);
        let live = testkit::fixture_model_with_metrics(Instant::now());
        assert_eq!(exporter_summary(&live), Some((6, 6)));
    }
}
