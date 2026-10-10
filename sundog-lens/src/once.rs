//! `watch --once`: one text or JSON report of the cluster.
//!
//! The run joins the cluster's gossip, waits until the member set has held
//! still for `--settle`, takes one scrape round, prints a report and exits.
//! [`build_report`] and [`render_text`] are pure over a [`Model`]. With
//! `--explain KEY` the report also names where that key lives:
//! [`explain_report`] computes its part and its owners from the model alone.

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, bail};
use serde::Serialize;
use sundog::observe::MemberStatus;

use crate::cli::{ExplainArgs, OnceArgs, WatchArgs};
use crate::key::printable;
use crate::locate::{LocateError, Located, locate};
use crate::model::Model;
use crate::model::derive::{self, Agreement, PART_SPACE};
use crate::source::{Feed, FeedConfig, Update};
use crate::ui::data::{self, NodeRow};
use crate::ui::eventlog::view_hash;
use crate::ui::text;
use crate::watch::init_logging;

/// How long [`collect`] waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// How long the run waits for the first member before it gives up.
    pub first_member: Duration,
    /// How long the run waits, after the members settle, for the ownership
    /// digests, the first scrape of every exporter and, with `--explain`, the
    /// settling of the key's view.
    pub extras: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            first_member: Duration::from_secs(15),
            extras: Duration::from_secs(8),
        }
    }
}

/// The report of one run.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OnceReport {
    /// The cluster name.
    pub cluster: String,
    /// The observer's gossip address.
    pub observer: String,
    /// Seconds the run observed before it printed.
    pub observed_secs: f64,
    /// Members that are live.
    pub live: usize,
    /// Members gossiping a departure.
    pub departing: usize,
    /// Members dropped with no departure.
    pub down: usize,
    /// Members dropped after a departure.
    pub left: usize,
    /// The wire protocols the live members speak, ascending.
    pub protocols: Vec<u16>,
    /// One entry per node.
    pub members: Vec<OnceMember>,
    /// One entry per cache.
    pub caches: Vec<OnceCache>,
    /// Where the `--explain` key lives; absent without `--explain`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<OnceExplain>,
}

/// One node in a [`OnceReport`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OnceMember {
    /// The slot label: `n1`.
    pub slot: String,
    /// The node id, 16 hex digits.
    pub node: String,
    /// The gossip address.
    pub gossip: String,
    /// The data-plane address.
    pub data: String,
    /// `live`, `departing`, `left` or `down`.
    pub status: &'static str,
    /// Seconds the process has run for a live node (from its incarnation),
    /// or since a departing node announced its departure; `null` for a node
    /// that is down or has left.
    pub up_secs: Option<f64>,
    /// The wire protocol.
    pub protocol: u16,
    /// Each advertised cache and its mode: `distributed:2`.
    pub caches: BTreeMap<String, String>,
    /// What the node's exporter reported, when one answered.
    pub exporter: Option<OnceExporter>,
}

/// What a node's exporter reported.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OnceExporter {
    /// The `/readyz` verdict, when probed.
    pub ready: Option<bool>,
    /// The node's `sundog_live_peers`.
    pub live_peers: Option<f64>,
    /// The node's `sundog_owned_parts` for each cache it reports.
    pub owned_parts: BTreeMap<String, f64>,
}

/// One cache in a [`OnceReport`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OnceCache {
    /// The cache name.
    pub name: String,
    /// The mode every advertiser agrees on; `conflict` when they disagree.
    pub mode: String,
    /// How many live members advertise the cache.
    pub advertisers: usize,
    /// The ownership the lens computed, for a `Distributed` cache.
    pub ownership: Option<OnceOwnership>,
}

/// The computed ownership of one `Distributed` cache.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OnceOwnership {
    /// Owners per part.
    pub owners: u8,
    /// How many nodes are eligible.
    pub eligible: usize,
    /// The first eight hex digits of the view hash.
    pub view: String,
    /// Whether the view ranks single parts.
    pub ranks_parts: bool,
    /// Parts owned across the eligible nodes: `65,536 × min(owners, eligible)`.
    pub parts_total: usize,
    /// Each eligible node's share.
    pub shares: Vec<OnceShare>,
    /// How many nodes report the parts computed for them.
    pub agree: usize,
    /// How many nodes report the cache at all.
    pub reporting: usize,
    /// Whether the cache has settled.
    pub settled: bool,
    /// Whether the verdict rests on gossip alone.
    pub gossip_only: bool,
}

/// One node's computed share of a cache.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OnceShare {
    /// The slot label.
    pub slot: String,
    /// Parts owned at any rank.
    pub parts: usize,
    /// The node's `sundog_owned_parts` for the cache, when it reports.
    pub reported: Option<f64>,
}

/// Where one key lives, computed from gossip: the part it hashes to and the
/// owners of that part in the order a fetch asks them. No node is asked, so
/// the report holds no node's own record, residency marks or probes; those
/// come from `Cache::explain` on the node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the view's rank unit, its settled verdict, what the verdict rests on and the mode conflict are independent facts, one JSON field each"
)]
pub struct OnceExplain {
    /// The cache.
    pub cache: String,
    /// The key as typed, and the bytes it hashes as.
    pub key: OnceKey,
    /// The key's part.
    pub part: OncePart,
    /// The ownership view hash, 16 hex digits.
    pub view: String,
    /// Owners per part in the view.
    pub owners_per_part: u8,
    /// Whether the view ranks single parts rather than whole buckets.
    pub ranks_parts: bool,
    /// The members the view ranks.
    pub eligible: usize,
    /// Whether the view has settled.
    pub settled: bool,
    /// Whether the verdict rests on gossip alone.
    pub gossip_only: bool,
    /// Whether the members that advertise the cache disagree on its mode.
    pub conflicted: bool,
    /// The part's owners in fetch order.
    pub owners: Vec<OnceOwner>,
}

/// The key of an [`OnceExplain`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OnceKey {
    /// The prefix that selects the encoding: `str`, `uint`, `int` or `hex`.
    pub kind: &'static str,
    /// The text after the prefix, as typed.
    pub text: String,
    /// The postcard bytes the cache hashes, as lowercase hex digits that
    /// `hex:` takes back.
    pub hex: String,
    /// The key as the text report writes it: `"k17" as String (4 bytes 03 6b
    /// 31 37)`.
    #[serde(skip)]
    echo: String,
}

/// A key's part, as `Cache::explain` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct OncePart {
    /// The part's bucket.
    pub bucket: u16,
    /// The part's position in its bucket.
    pub part: u8,
}

/// One owner of a key's part in an [`OnceExplain`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OnceOwner {
    /// The owner's place in fetch order: 1 is asked first.
    pub rank: usize,
    /// The slot label; `??` for a node the snapshot no longer lists.
    pub slot: String,
    /// The node id, 16 hex digits.
    pub node: String,
    /// `live`, `departing`, `left` or `down`; `null` for a node the snapshot
    /// no longer lists.
    pub status: Option<&'static str>,
    /// The gossip address; `null` for a node the snapshot no longer lists.
    pub gossip: Option<String>,
    /// The data-plane address; `null` for a node the snapshot no longer
    /// lists.
    pub data: Option<String>,
}

const fn status_name(status: MemberStatus) -> &'static str {
    match status {
        MemberStatus::Live => "live",
        MemberStatus::Departing => "departing",
        MemberStatus::Left => "left",
        MemberStatus::Down => "down",
        _ => "unknown",
    }
}

fn member_entry(model: &Model, row: &NodeRow<'_>, wall: SystemTime) -> OnceMember {
    let metrics = data::metrics_of(model, row);
    let exporter = metrics.map(|metrics| OnceExporter {
        ready: model
            .exporter(row.member.peer.gossip_addr)
            .and_then(crate::model::ExporterState::ready),
        live_peers: metrics.live_peers(),
        owned_parts: row
            .member
            .caches
            .keys()
            .filter_map(|cache| Some((cache.to_string(), metrics.owned_parts(cache)?)))
            .collect(),
    });
    OnceMember {
        slot: row.label().to_owned(),
        node: row.full_id(),
        gossip: row.member.peer.gossip_addr.to_string(),
        data: row.member.peer.data_addr.to_string(),
        status: status_name(row.status()),
        up_secs: match row.status() {
            MemberStatus::Live => row.uptime(wall),
            MemberStatus::Departing => Some(row.status_age(wall)),
            _ => None,
        }
        .map(|up| up.as_secs_f64()),
        protocol: row.member.peer.protocol,
        caches: row
            .member
            .caches
            .iter()
            .map(|(name, &mode)| (name.to_string(), data::mode_token(mode)))
            .collect(),
        exporter,
    }
}

fn cache_entry(model: &Model, row: &data::CacheRow) -> OnceCache {
    let ownership = model.ownership(&row.name).map(|digest| {
        let nodes = data::all_node_rows(model);
        let shares: Vec<OnceShare> = digest
            .counts
            .iter()
            .map(|&(node, parts)| {
                let node_row = nodes.iter().find(|r| r.member.peer.node == node);
                OnceShare {
                    slot: data::tag_of(model, node).label.to_string(),
                    parts,
                    reported: node_row
                        .and_then(|r| data::metrics_of(model, r))
                        .and_then(|m| m.owned_parts(&digest.cache)),
                }
            })
            .collect();
        let agree = shares
            .iter()
            .filter(|share| derive::agreement(share.reported, share.parts) == Agreement::Match)
            .count();
        let reporting = shares.iter().filter(|s| s.reported.is_some()).count();
        let verdict = model.settle(&digest.cache);
        OnceOwnership {
            owners: digest.k.get(),
            eligible: digest.eligible.len(),
            view: view_hash(digest.view_hash),
            ranks_parts: digest.ranks_parts,
            parts_total: PART_SPACE * usize::from(digest.k.get()).min(digest.eligible.len()),
            shares,
            agree,
            reporting,
            settled: verdict.is_some_and(|v| v.settled),
            gossip_only: verdict.is_some_and(|v| v.gossip_only),
        }
    });
    OnceCache {
        name: row.name.to_string(),
        mode: row
            .mode
            .map_or_else(|| "conflict".to_owned(), data::mode_token),
        advertisers: row.advertisers.len(),
        ownership,
    }
}

/// The report of `model` as of wall-clock time `wall`, after `elapsed` of
/// watching through the observer at `observer`.
#[must_use]
pub fn build_report(
    model: &Model,
    observer: &str,
    elapsed: Duration,
    wall: SystemTime,
) -> OnceReport {
    let rows = data::all_node_rows(model);
    let count = |status| rows.iter().filter(|row| row.status() == status).count();
    let mut protocols: Vec<u16> = rows
        .iter()
        .filter(|row| row.status() == MemberStatus::Live)
        .map(|row| row.member.peer.protocol)
        .collect();
    protocols.sort_unstable();
    protocols.dedup();
    OnceReport {
        cluster: model
            .snapshot()
            .map_or_else(String::new, |snapshot| snapshot.cluster.to_string()),
        observer: observer.to_owned(),
        observed_secs: elapsed.as_secs_f64(),
        live: count(MemberStatus::Live),
        departing: count(MemberStatus::Departing),
        down: count(MemberStatus::Down),
        left: count(MemberStatus::Left),
        protocols,
        members: rows
            .iter()
            .map(|row| member_entry(model, row, wall))
            .collect(),
        caches: data::cache_rows(model)
            .iter()
            .map(|row| cache_entry(model, row))
            .collect(),
        explain: None,
    }
}

/// `1 byte`, `3 bytes`.
fn byte_count(bytes: usize) -> String {
    format!("{bytes} byte{}", if bytes == 1 { "" } else { "s" })
}

fn explain_entry(located: &Located) -> OnceExplain {
    let key = &located.key;
    let bytes = key.bytes().len();
    OnceExplain {
        cache: located.cache.to_string(),
        key: OnceKey {
            kind: key.kind().token(),
            text: key.text().to_owned(),
            hex: key.hex().replace(' ', ""),
            echo: format!("{key} ({} {})", byte_count(bytes), key.hex()),
        },
        part: OncePart {
            bucket: located.part.bucket(),
            part: located.part.part(),
        },
        view: format!("{:016x}", located.view_hash),
        owners_per_part: located.owners_per_part.get(),
        ranks_parts: located.ranks_parts,
        eligible: located.eligible,
        settled: located.settle.settled,
        gossip_only: located.settle.gossip_only,
        conflicted: located.conflicted,
        owners: located
            .owners
            .iter()
            .map(|owner| OnceOwner {
                rank: owner.rank,
                slot: owner.slot.to_string(),
                node: owner.node.to_string(),
                status: owner.status.map(status_name),
                gossip: owner.gossip.map(|addr| addr.to_string()),
                data: owner.data.map(|addr| addr.to_string()),
            })
            .collect(),
    }
}

/// Where the key of `explain` lives in `model`: its part and its owners in
/// fetch order, from the ownership view the lens computed.
///
/// # Errors
///
/// Returns the [`LocateError`] when the cache is unknown, is not
/// `Distributed`, has no ownership digest yet, or is not named while several
/// `Distributed` caches are ranked. Its message names the remedy.
pub fn explain_report(model: &Model, explain: &ExplainArgs) -> Result<OnceExplain, LocateError> {
    locate(model, explain.cache.as_deref(), &explain.key).map(|located| explain_entry(&located))
}

/// What the view's settled verdict reads as: `settled (gossip only)`.
fn settle_text(explain: &OnceExplain) -> String {
    let verdict = if explain.settled {
        "settled"
    } else {
        "settling"
    };
    if explain.gossip_only {
        format!("{verdict} (gossip only)")
    } else {
        verdict.to_owned()
    }
}

/// The explain block of the text report: the cache and key, the part, the
/// view and the owners in fetch order.
#[must_use]
pub fn explain_lines(explain: &OnceExplain) -> Vec<String> {
    let owners = usize::from(explain.owners_per_part);
    let mut view = format!(
        "{} · {} · {} eligible · {owners} owner{} per part",
        explain.view,
        settle_text(explain),
        explain.eligible,
        if owners == 1 { "" } else { "s" },
    );
    if explain.conflicted {
        view.push_str(" · the advertisers disagree on the mode");
    }
    let mut lines = vec![
        format!(
            "explain {} · key {} · computed",
            printable(&explain.cache),
            explain.key.echo
        ),
        format!(
            "part   {}/{} · ranked per {}",
            explain.part.bucket,
            explain.part.part,
            if explain.ranks_parts {
                "part"
            } else {
                "bucket"
            }
        ),
        format!("view   {view}"),
    ];
    let rows: Vec<Vec<String>> = explain
        .owners
        .iter()
        .map(|owner| {
            let mut row = vec![
                owner.rank.to_string(),
                owner.slot.clone(),
                owner.node.clone(),
                owner.gossip.clone().unwrap_or_else(|| "—".to_owned()),
                owner
                    .data
                    .as_ref()
                    .map_or_else(|| "—".to_owned(), |addr| format!("data {addr}")),
            ];
            match owner.status {
                Some("live") => {}
                Some(status) => row.push(status.to_owned()),
                None => row.push("not listed".to_owned()),
            }
            row
        })
        .collect();
    lines.extend(table(&rows).into_iter().map(|row| format!("owner  {row}")));
    lines.push(
        "note   no node was asked; a node's record, residency marks and probes come from \
         Cache::explain on that node"
            .to_owned(),
    );
    lines
}

/// Pads the cells of each column to the widest, separated by two spaces; the
/// last column is not padded.
fn table(rows: &[Vec<String>]) -> Vec<String> {
    let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
    let widths: Vec<usize> = (0..columns)
        .map(|column| {
            rows.iter()
                .filter_map(|row| row.get(column))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    rows.iter()
        .map(|row| {
            let last = row.len().saturating_sub(1);
            let line: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(column, cell)| {
                    if column == last {
                        cell.clone()
                    } else {
                        text::pad_right(cell, widths[column])
                    }
                })
                .collect();
            line.join("  ")
        })
        .collect()
}

/// The summary line of the text report.
fn summary_line(report: &OnceReport) -> String {
    let mut counts = vec![format!("{} live", report.live)];
    for (count, label) in [
        (report.departing, "departing"),
        (report.down, "down"),
        (report.left, "left"),
    ] {
        if count > 0 {
            counts.push(format!("{count} {label}"));
        }
    }
    let protocol = if report.protocols.is_empty() {
        "no protocol".to_owned()
    } else {
        format!(
            "protocol {}",
            report
                .protocols
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join("·")
        )
    };
    format!(
        "{} · {} · {protocol} · observed {:.1} s via observer {}",
        report.cluster,
        counts.join(" · "),
        report.observed_secs,
        report.observer
    )
}

/// The members table of the text report.
fn members_table(report: &OnceReport) -> Vec<String> {
    let mut rows = vec![
        ["SLOT", "NODE", "GOSSIP", "STATUS", "UP", "P", "CACHES"]
            .map(str::to_owned)
            .to_vec(),
    ];
    for member in &report.members {
        let mut listed: Vec<_> = member.caches.iter().collect();
        listed.sort_by_key(|(_, mode)| !mode.starts_with("distributed"));
        let caches = listed
            .iter()
            .map(|(name, mode)| format!("{name}={mode}"))
            .collect::<Vec<_>>()
            .join(" ");
        rows.push(vec![
            member.slot.clone(),
            member.node.clone(),
            member.gossip.clone(),
            member.status.to_owned(),
            member.up_secs.map_or_else(
                || "—".to_owned(),
                |secs| text::uptime(Duration::from_secs_f64(secs.max(0.0))),
            ),
            member.protocol.to_string(),
            caches,
        ]);
    }
    table(&rows)
}

/// The caches table of the text report; empty when no cache is advertised.
fn caches_table(report: &OnceReport) -> Vec<String> {
    if report.caches.is_empty() {
        return Vec::new();
    }
    let mut rows = vec![
        ["CACHE", "MODE", "ELIGIBLE", "VIEW", "Σ PARTS", "REPORTED"]
            .map(str::to_owned)
            .to_vec(),
    ];
    for cache in &report.caches {
        rows.push(match &cache.ownership {
            Some(own) => vec![
                cache.name.clone(),
                cache.mode.clone(),
                own.eligible.to_string(),
                own.view.clone(),
                format!(
                    "{} ({}×{})",
                    text::thousands(u64::try_from(own.parts_total).unwrap_or(0)),
                    usize::from(own.owners).min(own.eligible),
                    text::thousands(u64::try_from(PART_SPACE).unwrap_or(0))
                ),
                reported_text(own),
            ],
            None => vec![
                cache.name.clone(),
                cache.mode.clone(),
                cache.advertisers.to_string(),
                "—".to_owned(),
                "—".to_owned(),
                "—".to_owned(),
            ],
        });
    }
    table(&rows)
}

/// The report as text: a summary line, the members and the caches, then, with
/// `--explain`, the explain block.
#[must_use]
pub fn render_text(report: &OnceReport) -> String {
    let mut out = summary_line(report);
    out.push('\n');
    for line in members_table(report)
        .into_iter()
        .chain(caches_table(report))
    {
        out.push_str(line.trim_end());
        out.push('\n');
    }
    if let Some(explain) = &report.explain {
        out.push('\n');
        for line in explain_lines(explain) {
            out.push_str(line.trim_end());
            out.push('\n');
        }
    }
    out
}

fn reported_text(own: &OnceOwnership) -> String {
    if own.reporting == 0 {
        "no metrics".to_owned()
    } else if own.agree == own.eligible {
        format!("✓ {}/{}", own.agree, own.eligible)
    } else {
        format!("↻ {}/{}", own.agree, own.eligible)
    }
}

/// The members as the member set shows them: node, incarnation and status.
/// The run waits for this to hold still.
#[must_use]
pub fn membership_key(model: &Model) -> Vec<(sundog::NodeId, u64, MemberStatus)> {
    model.snapshot().map_or_else(Vec::new, |snapshot| {
        snapshot
            .members
            .iter()
            .map(|member| (member.peer.node, member.peer.incarnation, member.status))
            .collect()
    })
}

/// Tracks how long the member set has held still.
#[derive(Debug, Clone, Default)]
pub struct Settler {
    key: Vec<(sundog::NodeId, u64, MemberStatus)>,
    since: Option<Instant>,
}

impl Settler {
    /// A settler that has seen nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Notes the member set of `model` at `now`.
    pub fn observe(&mut self, model: &Model, now: Instant) {
        let key = membership_key(model);
        if key != self.key || self.since.is_none() {
            self.key = key;
            self.since = Some(now);
        }
    }

    /// Whether the set is non-empty and has held still for `settle`.
    #[must_use]
    pub fn settled(&self, settle: Duration, now: Instant) -> bool {
        !self.key.is_empty()
            && self
                .since
                .is_some_and(|since| now.saturating_duration_since(since) >= settle)
    }
}

/// Whether the model holds what the report needs: an ownership digest for
/// every `Distributed` cache and, when scraping, a scrape of every live node.
#[must_use]
pub fn complete(model: &Model, scraping: bool) -> bool {
    let digests = data::distributed_caches(model)
        .iter()
        .all(|cache| model.ownership(cache).is_some());
    let scraped = !scraping
        || data::all_node_rows(model)
            .iter()
            .filter(|row| row.status() == MemberStatus::Live)
            .all(|row| model.scrape(row.member.peer.gossip_addr).is_some());
    digests && scraped
}

/// Whether the view an `--explain` key rests on has settled: its digest has
/// held, unreplaced, as long as a view needs to count as settled. The ownership
/// worker ranks on a blocking thread, so until then a newer snapshot may have a
/// ranking on its way. A key [`locate`] cannot answer for a reason a wait does
/// not fix is ready too, and the run reports the error; a `Distributed` cache
/// without a digest yet is not.
#[must_use]
pub fn explain_ready(model: &Model, explain: &ExplainArgs) -> bool {
    match locate(model, explain.cache.as_deref(), &explain.key) {
        Ok(located) => located.settle.settled,
        Err(LocateError::NotRankedYet { .. }) => false,
        Err(_) => true,
    }
}

/// Feeds `model` from `feed` until the member set has held still for the
/// settle time and the model is complete, or the extras limit passes. With
/// `--explain`, the model is complete only when [`explain_ready`] holds too.
///
/// # Errors
///
/// Returns an error when no member shows up within `limits.first_member` or
/// the feed stops.
pub async fn collect(
    feed: &mut Feed,
    model: &mut Model,
    once: &OnceArgs,
    scraping: bool,
    limits: Limits,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let mut settler = Settler::new();
    let mut settled_at: Option<Instant> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            update = feed.recv() => {
                let update: Update = update.context("the sources stopped")?;
                model.apply(update, Instant::now(), SystemTime::now());
            }
            _ = tick.tick() => {
                model.tick(Instant::now());
            }
        }
        let now = Instant::now();
        settler.observe(model, now);
        if settler.settled(once.settle, now) {
            let since = *settled_at.get_or_insert(now);
            let ready = complete(model, scraping)
                && once
                    .explain
                    .as_ref()
                    .is_none_or(|explain| explain_ready(model, explain));
            if ready || now.saturating_duration_since(since) >= limits.extras {
                return Ok(());
            }
        } else {
            settled_at = None;
            if membership_key(model).is_empty()
                && now.saturating_duration_since(start) >= limits.first_member
            {
                bail!(
                    "no member of the cluster showed up in {} s: check the cluster name and the seeds",
                    limits.first_member.as_secs().max(1)
                );
            }
        }
    }
}

/// Runs `watch --once`.
///
/// # Errors
///
/// Returns an error when the observer cannot start, no member shows up or
/// the `--explain` key cannot be located. The last prints nothing to
/// stdout.
pub async fn run(args: WatchArgs) -> anyhow::Result<()> {
    let once = args
        .once
        .clone()
        .context("watch --once needs the --once options")?;
    init_logging(args.log.as_deref())?;
    let scraping = !args.metrics.is_empty() || !args.scrape.is_empty();
    let config = FeedConfig::try_from(&args).context("reading --metrics")?;
    let lens_started = SystemTime::now();
    let mut feed = Feed::spawn(config).await?;
    let mut model = Model::new();
    model.set_started(lens_started);
    let started = Instant::now();
    let outcome = collect(&mut feed, &mut model, &once, scraping, Limits::default()).await;
    let observer = feed.observer_addr().to_string();
    feed.shutdown().await;
    outcome?;
    let explain = once
        .explain
        .as_ref()
        .map(|explain| explain_report(&model, explain))
        .transpose()?;
    let mut report = build_report(&model, &observer, started.elapsed(), SystemTime::now());
    report.explain = explain;
    if once.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{}", render_text(&report));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU8;
    use std::sync::Arc;

    use smol_str::SmolStr;
    use sundog::NodeId;
    use sundog::observe::ClusterSnapshot;
    use sundog::store::PartId;

    use super::*;
    use crate::key::KeySpec;
    use crate::locate::LocatedOwner;
    use crate::model::derive::Settle;
    use crate::model::{GOSSIP_SETTLE, testkit};

    fn report() -> OnceReport {
        let model = testkit::fixture_model(Instant::now());
        build_report(
            &model,
            "127.0.0.1:41733",
            Duration::from_millis(3000),
            model.wall().unwrap(),
        )
    }

    #[test]
    fn the_report_counts_each_status_and_lists_every_node() {
        let report = report();
        assert_eq!(report.cluster, "fixture");
        assert_eq!(
            (report.live, report.departing, report.down, report.left),
            (5, 1, 1, 1)
        );
        assert_eq!(report.protocols, [sundog::wire::PROTOCOL_VERSION]);
        assert_eq!(report.members.len(), 8);
        let first = &report.members[0];
        assert_eq!(first.slot, "n1");
        assert_eq!(first.status, "live");
        assert_eq!(first.node.len(), 16);
        assert_eq!(first.gossip, "127.0.0.11:7946");
        assert_eq!(first.caches["it"], "distributed:2");
        assert_eq!(first.caches["churn"], "replicated");
        // The fixture's incarnation is 1 ms past the epoch; its wall is 20 s.
        assert!((first.up_secs.unwrap() - 19.999).abs() < 1e-9);
        let ups: Vec<_> = report.members.iter().map(|m| m.up_secs.is_some()).collect();
        assert_eq!(
            ups,
            [true, true, true, true, true, true, false, false],
            "down and left nodes have no uptime"
        );
        assert!(first.exporter.is_none(), "no metrics in the fixture");
        let statuses: Vec<_> = report.members.iter().map(|m| m.status).collect();
        assert_eq!(
            statuses,
            [
                "live",
                "live",
                "live",
                "live",
                "live",
                "departing",
                "down",
                "left"
            ]
        );
    }

    #[test]
    fn the_report_computes_ownership_for_a_distributed_cache() {
        let report = report();
        let it = report.caches.iter().find(|c| c.name == "it").unwrap();
        assert_eq!(it.mode, "distributed:2");
        assert_eq!(it.advertisers, 6);
        let own = it.ownership.as_ref().unwrap();
        assert_eq!(own.owners, 2);
        assert_eq!(own.eligible, 5);
        assert_eq!(own.parts_total, 131_072);
        assert_eq!(own.shares.len(), 5);
        assert_eq!(own.shares.iter().map(|s| s.parts).sum::<usize>(), 131_072);
        assert_eq!(own.view.len(), 8);
        assert!(own.settled && own.gossip_only);
        assert_eq!((own.agree, own.reporting), (0, 0));
        let churn = report.caches.iter().find(|c| c.name == "churn").unwrap();
        assert!(churn.ownership.is_none());
        assert_eq!(churn.mode, "replicated");
    }

    #[test]
    fn the_text_report_follows_the_documented_shape() {
        let text = render_text(&report());
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines[0],
            format!(
                "fixture · 5 live · 1 departing · 1 down · 1 left · protocol {} · observed 3.0 s \
                 via observer 127.0.0.1:41733",
                sundog::wire::PROTOCOL_VERSION
            )
        );
        assert!(lines[1].starts_with("SLOT  NODE"), "{}", lines[1]);
        assert!(
            lines[1].contains("GOSSIP") && lines[1].ends_with("CACHES"),
            "{}",
            lines[1]
        );
        assert!(lines[2].starts_with("n1    "), "{}", lines[2]);
        assert!(
            lines[2].ends_with("it=distributed:2 churn=replicated os=replicated pn=replicated"),
            "{}",
            lines[2]
        );
        assert!(lines[2].contains("00:19"), "{}", lines[2]);
        for (slot, status) in [("n7", "down"), ("n8", "left")] {
            let gone = lines.iter().find(|l| l.starts_with(slot)).unwrap();
            let fields: Vec<_> = gone.split_whitespace().collect();
            assert_eq!(fields[3..5], [status, "—"], "{gone}");
        }
        let cache_header = lines.iter().position(|l| l.starts_with("CACHE")).unwrap();
        assert!(lines[cache_header].contains("Σ PARTS"));
        let it = lines[cache_header + 1];
        assert!(it.starts_with("it     distributed:2  5 "), "{it}");
        assert!(it.contains("131,072 (2×65,536)"), "{it}");
        assert!(it.ends_with("no metrics"), "{it}");
        let churn = lines[cache_header + 2];
        assert!(churn.starts_with("churn  replicated"), "{churn}");
        assert!(churn.contains("—"), "{churn}");
    }

    #[test]
    fn a_report_serializes_to_one_json_object_with_every_field() {
        let json = serde_json::to_value(report()).unwrap();
        for field in [
            "cluster",
            "observer",
            "observed_secs",
            "live",
            "departing",
            "down",
            "left",
            "protocols",
            "members",
            "caches",
        ] {
            assert!(json.get(field).is_some(), "{field}");
        }
        assert_eq!(json["members"][0]["slot"], "n1");
        assert_eq!(json["caches"][0]["name"], "it");
        assert_eq!(json["caches"][0]["ownership"]["parts_total"], 131_072);
        assert!(json["members"][0]["exporter"].is_null());
    }

    #[test]
    fn an_empty_model_reports_nothing_without_panicking() {
        let model = Model::new();
        let report = build_report(&model, "o", Duration::ZERO, SystemTime::UNIX_EPOCH);
        assert_eq!(report.members.len(), 0);
        let text = render_text(&report);
        assert!(text.starts_with(" · 0 live · no protocol · observed 0.0 s via observer o"));
    }

    #[test]
    fn the_reported_column_reads_agreement_or_the_lack_of_metrics() {
        let mut own = report().caches[0].ownership.clone().unwrap();
        assert_eq!(reported_text(&own), "no metrics");
        own.reporting = 5;
        own.agree = 5;
        assert_eq!(reported_text(&own), "✓ 5/5");
        own.agree = 3;
        assert_eq!(reported_text(&own), "↻ 3/5");
    }

    #[test]
    fn tables_pad_every_column_but_the_last() {
        let rows = vec![
            vec!["a".to_owned(), "bbb".to_owned(), "c".to_owned()],
            vec!["aaaa".to_owned(), "b".to_owned(), "cc".to_owned()],
        ];
        assert_eq!(table(&rows), ["a     bbb  c", "aaaa  b    cc"]);
        let rows = table(&[]);
        assert!(rows.is_empty(), "{rows:?}");
    }

    #[test]
    fn the_settler_waits_for_a_non_empty_set_that_holds_still() {
        let mut settler = Settler::new();
        let start = Instant::now();
        let settle = Duration::from_secs(3);
        let empty = Model::new();
        settler.observe(&empty, start);
        assert!(
            !settler.settled(settle, start + Duration::from_secs(60)),
            "empty never settles"
        );
        let mut model = Model::new();
        let apply = |model: &mut Model, live: u8, at: Instant| {
            model.apply(
                Update::Snapshot(std::sync::Arc::new(testkit::snapshot(live)), at),
                at,
                SystemTime::UNIX_EPOCH,
            );
        };
        apply(&mut model, 2, start);
        settler.observe(&model, start);
        assert!(!settler.settled(settle, start + Duration::from_secs(2)));
        assert!(settler.settled(settle, start + Duration::from_secs(3)));
        // A new member restarts the clock.
        apply(&mut model, 3, start + Duration::from_secs(4));
        settler.observe(&model, start + Duration::from_secs(4));
        assert!(!settler.settled(settle, start + Duration::from_secs(6)));
        assert!(settler.settled(settle, start + Duration::from_secs(7)));
        // The same set again does not.
        settler.observe(&model, start + Duration::from_secs(8));
        assert!(settler.settled(settle, start + Duration::from_secs(8)));
    }

    #[test]
    fn a_model_is_complete_once_its_digests_and_scrapes_are_in() {
        let model = testkit::fixture_model(Instant::now());
        assert!(complete(&model, false));
        assert!(!complete(&model, true), "no scrape of any live node yet");
        let mut bare = Model::new();
        let now = Instant::now();
        bare.apply(
            Update::Snapshot(std::sync::Arc::new(testkit::snapshot(2)), now),
            now,
            SystemTime::UNIX_EPOCH,
        );
        assert!(
            !complete(&bare, false),
            "the ownership digest has not arrived"
        );
    }

    #[test]
    fn an_explained_key_is_ready_once_its_view_has_held() {
        let base = Instant::now();
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let snapshot = testkit::snapshot_with_owners(3, 2);
        let mut model = Model::new();
        model.apply(
            Update::Snapshot(Arc::new(snapshot.clone()), base),
            base,
            wall,
        );
        let unnamed = args("k1", None);
        let named = args("k1", Some("it"));
        assert!(
            !explain_ready(&model, &unnamed) && !explain_ready(&model, &named),
            "the cache is advertised and its digest has not arrived"
        );

        let digest = testkit::ownership_digest(&snapshot, "it").expect("a member is eligible");
        model.apply(Update::Ownership(digest), base, wall);
        assert!(!explain_ready(&model, &named), "the view has not held yet");
        model.tick(base + GOSSIP_SETTLE.saturating_sub(Duration::from_millis(1)));
        assert!(!explain_ready(&model, &named));
        model.tick(base + GOSSIP_SETTLE);
        assert!(explain_ready(&model, &named) && explain_ready(&model, &unnamed));

        // No wait fixes an unknown cache or a cluster with no Distributed
        // cache: the run reports the error.
        assert!(explain_ready(&model, &args("k1", Some("nope"))));
        assert!(explain_ready(&Model::new(), &unnamed));

        // Measured nodes that still disagree with the computed view keep it
        // unsettled.
        let measured = testkit::fixture_model_with_metrics(base);
        assert!(!explain_ready(&measured, &unnamed));
        assert!(explain_ready(&testkit::fixture_model(base), &unnamed));
    }

    #[test]
    fn the_membership_key_lists_every_member_by_node_incarnation_and_status() {
        let model = testkit::fixture_model(Instant::now());
        let key = membership_key(&model);
        assert_eq!(key.len(), 8);
        assert_eq!(key[0], (testkit::node_id(1, 0), 1, MemberStatus::Live));
        assert_eq!(key[7].2, MemberStatus::Left);
        let key = membership_key(&Model::new());
        assert!(key.is_empty(), "{key:?}");
    }

    #[test]
    fn the_default_limits_wait_fifteen_seconds_for_a_member_and_eight_for_the_rest() {
        let limits = Limits::default();
        assert_eq!(limits.first_member, Duration::from_secs(15));
        assert_eq!(limits.extras, Duration::from_secs(8));
    }

    #[test]
    fn the_exporter_section_carries_what_the_scrape_reported() {
        let model = testkit::fixture_model_with_metrics(Instant::now());
        let report = build_report(&model, "o", Duration::ZERO, model.wall().unwrap());
        let n1 = &report.members[0];
        let exporter = n1.exporter.as_ref().expect("n1 is scraped");
        assert_eq!(exporter.ready, Some(true));
        assert_eq!(exporter.live_peers, Some(5.0));
        assert!(exporter.owned_parts["it"] > 20_000.0);
        let it = report.caches.iter().find(|c| c.name == "it").unwrap();
        let own = it.ownership.as_ref().unwrap();
        assert_eq!(
            (own.agree, own.reporting),
            (4, 5),
            "n3 reports 900 parts short"
        );
        assert!(!own.settled && !own.gossip_only);
        assert!(own.shares.iter().all(|share| share.reported.is_some()));
        let text = render_text(&report);
        assert!(text.contains("↻ 4/5"), "{text}");
        assert_eq!(report.members[6].exporter, None, "a down node has none");
    }

    fn args(key: &str, cache: Option<&str>) -> ExplainArgs {
        ExplainArgs {
            key: KeySpec::parse(key).expect("the key parses"),
            cache: cache.map(str::to_owned),
        }
    }

    /// The report of the fixture model with `key` explained.
    fn explained_report(key: &str) -> OnceReport {
        let model = testkit::fixture_model(Instant::now());
        let mut report = build_report(&model, "o", Duration::ZERO, model.wall().unwrap());
        report.explain = Some(explain_report(&model, &args(key, None)).expect("key is located"));
        report
    }

    /// A model of three live members that rank `it` with two owners, then
    /// drop member 3 from the snapshot, and a key whose owners include it.
    fn model_without_an_owner() -> (Model, KeySpec, NodeId) {
        let base = Instant::now();
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let full = testkit::snapshot_with_owners(3, 2);
        let mut model = Model::new();
        model.apply(Update::Snapshot(Arc::new(full.clone()), base), base, wall);
        let digest = testkit::ownership_digest(&full, "it").expect("a member is eligible");
        let gone = testkit::node_id(3, 0);
        let key = (0..10_000)
            .map(|n| KeySpec::parse(&format!("k{n}")).expect("text parses"))
            .find(|key| {
                digest
                    .shares
                    .owners_of(PartId::of_key(key.bytes()))
                    .contains(&gone)
            })
            .expect("member 3 owns some part");
        model.apply(Update::Ownership(digest), base, wall);
        let remaining = ClusterSnapshot::new(
            "fixture",
            full.members
                .iter()
                .filter(|member| member.peer.node != gone)
                .cloned()
                .collect(),
            0,
        );
        model.apply(Update::Snapshot(Arc::new(remaining), base), base, wall);
        (model, key, gone)
    }

    #[test]
    fn the_explain_section_carries_the_part_view_and_owners_of_the_key() {
        let model = testkit::fixture_model(Instant::now());
        let digest = model.ownership("it").expect("the fixture ranks it");
        let explain = explain_report(&model, &args("k1", None)).expect("k1 is located");

        let part = PartId::of_key(&[2, b'k', b'1']);
        assert_eq!(explain.cache, "it");
        assert_eq!((explain.key.kind, explain.key.text.as_str()), ("str", "k1"));
        assert_eq!(explain.key.hex, "026b31");
        assert_eq!(
            KeySpec::parse(&format!("hex:{}", explain.key.hex))
                .expect("the hex digits are a key")
                .bytes(),
            args("k1", None).key.bytes(),
            "hex: takes the digits back as the same key"
        );
        assert_eq!(
            explain.part,
            OncePart {
                bucket: part.bucket(),
                part: part.part()
            }
        );
        assert_eq!(explain.view, format!("{:016x}", digest.view_hash));
        assert_eq!(explain.view.len(), 16);
        assert_eq!(explain.owners_per_part, 2);
        assert_eq!(explain.ranks_parts, digest.ranks_parts);
        assert_eq!(explain.eligible, 5);
        assert!(explain.settled && explain.gossip_only && !explain.conflicted);

        let expected = digest.shares.owners_of(part);
        assert_eq!(
            explain
                .owners
                .iter()
                .map(|owner| owner.node.clone())
                .collect::<Vec<_>>(),
            expected.iter().map(NodeId::to_string).collect::<Vec<_>>(),
            "the owners in fetch order"
        );
        let snapshot = model.snapshot().expect("the fixture has a snapshot");
        for (index, owner) in explain.owners.iter().enumerate() {
            assert_eq!(owner.rank, index + 1);
            assert_eq!(owner.node.len(), 16);
            assert_eq!(
                owner.slot,
                crate::ui::data::tag_of(&model, expected[index]).label
            );
            assert_eq!(owner.status, Some("live"));
            let member = snapshot
                .members
                .iter()
                .find(|member| member.peer.node == expected[index])
                .expect("an owner is a member");
            assert_eq!(owner.gossip, Some(member.peer.gossip_addr.to_string()));
            assert_eq!(owner.data, Some(member.peer.data_addr.to_string()));
        }

        // The cache is named, and a key of another kind lands elsewhere.
        let named = explain_report(&model, &args("uint:300", Some("it"))).expect("located");
        assert_eq!((named.key.kind, named.key.hex.as_str()), ("uint", "ac02"));
        let part = PartId::of_key(&[0xAC, 0x02]);
        assert_eq!(
            (named.part.bucket, named.part.part),
            (part.bucket(), part.part())
        );
        assert_eq!(named.view, explain.view);
    }

    #[test]
    fn explain_entry_maps_each_fact_of_a_located_key() {
        let located = |ranks_parts, settled, gossip_only, conflicted| Located {
            cache: SmolStr::new("it"),
            key: KeySpec::parse("k1").expect("the key parses"),
            part: PartId::new(513, 9),
            owners_per_part: NonZeroU8::new(3).unwrap(),
            ranks_parts,
            view_hash: 0x5d69_e3db_4c1a_02f7,
            eligible: 4,
            settle: Settle {
                settled,
                gossip_only,
            },
            conflicted,
            owners: vec![LocatedOwner {
                rank: 1,
                node: testkit::node_id(2, 0),
                slot: SmolStr::new("n2"),
                status: Some(MemberStatus::Departing),
                gossip: Some("10.0.0.2:7946".parse().unwrap()),
                data: Some("10.0.0.2:7947".parse().unwrap()),
            }],
        };

        // Every pair of the four flags differs in one of the three cases, so a
        // swapped field fails.
        let first = explain_entry(&located(false, false, true, true));
        assert_eq!(
            (
                first.ranks_parts,
                first.settled,
                first.gossip_only,
                first.conflicted
            ),
            (false, false, true, true)
        );
        let second = explain_entry(&located(false, true, false, true));
        assert_eq!(
            (
                second.ranks_parts,
                second.settled,
                second.gossip_only,
                second.conflicted
            ),
            (false, true, false, true)
        );
        let third = explain_entry(&located(true, true, true, false));
        assert_eq!(
            (
                third.ranks_parts,
                third.settled,
                third.gossip_only,
                third.conflicted
            ),
            (true, true, true, false)
        );

        assert_eq!(first.cache, "it");
        assert_eq!(
            first.part,
            OncePart {
                bucket: 513,
                part: 9
            }
        );
        assert_eq!(first.view, "5d69e3db4c1a02f7");
        assert_eq!((first.owners_per_part, first.eligible), (3, 4));
        assert_eq!(
            first.owners,
            [OnceOwner {
                rank: 1,
                slot: "n2".to_owned(),
                node: testkit::node_id(2, 0).to_string(),
                status: Some("departing"),
                gossip: Some("10.0.0.2:7946".to_owned()),
                data: Some("10.0.0.2:7947".to_owned()),
            }]
        );
        let lines = explain_lines(&first);
        assert!(lines[3].ends_with("departing"), "{}", lines[3]);
        assert!(lines[2].contains("settling (gossip only)"), "{}", lines[2]);
        assert!(lines[2].contains("3 owners per part"), "{}", lines[2]);
    }

    #[test]
    fn a_plain_report_has_no_explain_field() {
        let report = report();
        assert_eq!(report.explain, None);
        let json = serde_json::to_value(&report).unwrap();
        assert!(json.get("explain").is_none(), "{json}");
        assert!(!render_text(&report).contains("explain"));
    }

    #[test]
    fn the_explain_section_serializes_with_every_field() {
        let report = explained_report("k1");
        let explain = report.explain.as_ref().expect("the report explains k1");
        let json = serde_json::to_value(&report).unwrap();
        let section = &json["explain"];
        let mut fields: Vec<_> = section.as_object().unwrap().keys().cloned().collect();
        fields.sort();
        assert_eq!(
            fields,
            [
                "cache",
                "conflicted",
                "eligible",
                "gossip_only",
                "key",
                "owners",
                "owners_per_part",
                "part",
                "ranks_parts",
                "settled",
                "view"
            ]
        );
        assert_eq!(section["cache"], "it");
        assert_eq!(
            section["key"],
            serde_json::json!({"kind": "str", "text": "k1", "hex": "026b31"}),
            "the text report's echo is not a JSON field"
        );
        assert_eq!(
            section["part"],
            serde_json::json!({"bucket": explain.part.bucket, "part": explain.part.part})
        );
        assert_eq!(section["view"], explain.view.as_str());
        assert_eq!(section["owners_per_part"], 2);
        assert_eq!(section["eligible"], 5);
        assert_eq!(
            (
                &section["settled"],
                &section["gossip_only"],
                &section["conflicted"]
            ),
            (&true.into(), &true.into(), &false.into())
        );
        assert!(section["ranks_parts"].is_boolean());
        let owners = section["owners"].as_array().unwrap();
        assert_eq!(owners.len(), 2);
        for (owner, rank) in owners.iter().zip(1..) {
            let mut fields: Vec<_> = owner.as_object().unwrap().keys().cloned().collect();
            fields.sort();
            assert_eq!(fields, ["data", "gossip", "node", "rank", "slot", "status"]);
            assert_eq!(owner["rank"], rank);
            assert_eq!(owner["status"], "live");
            assert_eq!(owner["node"].as_str().map(str::len), Some(16));
        }
        assert_eq!(owners[0]["node"], explain.owners[0].node.as_str());
    }

    #[test]
    fn the_text_report_prints_the_explain_block_after_the_caches() {
        let report = explained_report("k1");
        let explain = report.explain.clone().expect("the report explains k1");
        let text = render_text(&report);
        let mut plain = report.clone();
        plain.explain = None;
        let plain = render_text(&plain);
        let block = text
            .strip_prefix(&plain)
            .expect("the explain block follows the plain report");
        let lines: Vec<&str> = block.lines().collect();
        assert_eq!(lines[0], "", "a blank line sets the block apart");
        assert_eq!(
            lines[1],
            "explain it · key \"k1\" as String (3 bytes 02 6b 31) · computed"
        );
        assert_eq!(
            lines[2],
            format!(
                "part   {}/{} · ranked per part",
                explain.part.bucket, explain.part.part
            ),
            "the part is bucket/part"
        );
        assert_eq!(
            lines[3],
            format!(
                "view   {} · settled (gossip only) · 5 eligible · 2 owners per part",
                explain.view
            )
        );
        for (index, owner) in explain.owners.iter().enumerate() {
            let line = lines[4 + index];
            let prefix = format!("owner  {}  {}  {}  ", owner.rank, owner.slot, owner.node);
            assert!(line.starts_with(&prefix), "{line}");
            assert!(
                line.ends_with(&format!(
                    "{}  data {}",
                    owner.gossip.as_ref().unwrap(),
                    owner.data.as_ref().unwrap()
                )),
                "{line}"
            );
        }
        assert_eq!(
            lines[6],
            "note   no node was asked; a node's record, residency marks and probes come from \
             Cache::explain on that node"
        );
        assert_eq!(lines.len(), 7, "{block}");
    }

    #[test]
    fn explain_lines_name_the_rank_unit_the_verdict_and_an_owner_that_is_not_live() {
        let mut explain = explained_report("k1").explain.expect("k1 is explained");

        explain.ranks_parts = false;
        explain.settled = false;
        explain.gossip_only = false;
        explain.conflicted = true;
        explain.owners_per_part = 1;
        explain.owners[1].status = Some("down");
        explain.cache = "i\u{1b}t".to_owned();
        let lines = explain_lines(&explain);
        assert!(lines[0].starts_with("explain i·t · key "), "{}", lines[0]);
        assert!(lines[1].ends_with("· ranked per bucket"), "{}", lines[1]);
        assert!(
            lines[2].ends_with(
                " · settling · 5 eligible · 1 owner per part · the advertisers disagree on the mode"
            ),
            "{}",
            lines[2]
        );
        assert!(!lines[3].ends_with("down"), "{}", lines[3]);
        assert!(lines[4].ends_with("  down"), "{}", lines[4]);
        // The columns of the owner rows line up.
        let columns = |line: &str| line.find("data ").expect("a data column");
        assert_eq!(columns(&lines[3]), columns(&lines[4]));

        assert_eq!(settle_text(&explain), "settling");
        explain.gossip_only = true;
        assert_eq!(settle_text(&explain), "settling (gossip only)");
        explain.settled = true;
        assert_eq!(settle_text(&explain), "settled (gossip only)");
        explain.gossip_only = false;
        assert_eq!(settle_text(&explain), "settled");
    }

    #[test]
    fn the_explain_echo_states_the_bytes_hashed_for_each_kind_of_key() {
        let model = testkit::fixture_model(Instant::now());
        let echo = |key: &str| {
            explain_report(&model, &args(key, None))
                .expect("located")
                .key
                .echo
        };
        assert_eq!(echo("k17"), "\"k17\" as String (4 bytes 03 6b 31 37)");
        assert_eq!(echo("uint:300"), "300 as unsigned integer (2 bytes ac 02)");
        assert_eq!(echo("int:-1"), "-1 as signed integer (1 byte 01)");
        assert_eq!(
            echo("hex:07ac02"),
            "07 ac 02 as postcard bytes (3 bytes 07 ac 02)"
        );
        assert_eq!(echo("é"), "\"·\" as String (3 bytes 02 c3 a9)");
        assert_eq!(byte_count(0), "0 bytes");
        assert_eq!(byte_count(1), "1 byte");
    }

    #[test]
    fn a_departed_owner_has_no_status_or_address_in_the_explain_block() {
        let (model, key, gone) = model_without_an_owner();
        let explain = explain_report(
            &model,
            &ExplainArgs {
                key,
                cache: Some("it".to_owned()),
            },
        )
        .expect("located after the departure");
        let departed = explain
            .owners
            .iter()
            .find(|owner| owner.node == gone.to_string())
            .expect("the digest still ranks the departed owner");
        assert_eq!(departed.slot, "??");
        assert_eq!(
            (&departed.status, &departed.gossip, &departed.data),
            (&None, &None, &None)
        );
        let json = serde_json::to_value(departed).unwrap();
        assert!(json["status"].is_null() && json["gossip"].is_null() && json["data"].is_null());
        let line = explain_lines(&explain)
            .into_iter()
            .find(|line| line.contains(&gone.to_string()))
            .expect("the departed owner has a row");
        let cells: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(
            (cells[2], cells[4..].to_vec()),
            ("??", vec!["—", "—", "not", "listed"]),
            "{line}"
        );
        let listed = explain
            .owners
            .iter()
            .find(|owner| owner.node != gone.to_string())
            .expect("the other owner");
        assert_eq!(listed.status, Some("live"));
    }

    #[test]
    fn explain_report_turns_a_locate_error_into_the_error_run_returns() {
        let model = testkit::fixture_model(Instant::now());
        let error = explain_report(&model, &args("k1", Some("nope"))).expect_err("no such cache");
        assert!(matches!(&error, LocateError::UnknownCache { cache, .. } if cache == "nope"));
        let message = anyhow::Error::from(error).to_string();
        assert!(message.contains("nope"), "{message}");
        assert!(message.contains("name one of it, churn"), "{message}");

        let error = explain_report(&model, &args("k1", Some("churn"))).expect_err("replicated");
        assert!(error.to_string().contains("not Distributed"), "{error}");
        let error = explain_report(&Model::new(), &args("k1", None)).expect_err("no cache");
        assert_eq!(error, LocateError::NoDistributedCache);

        // Two ranked Distributed caches: without a name the run exits 1
        // naming `--cache`.
        let base = Instant::now();
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let caches = [
            ("ids", testkit::distributed(2)),
            ("it", testkit::distributed(2)),
        ];
        let snapshot = ClusterSnapshot::new(
            "fixture",
            (1..=3)
                .map(|index| testkit::member_with(index, 0, 1, MemberStatus::Live, &caches))
                .collect(),
            0,
        );
        let mut model = Model::new();
        model.apply(
            Update::Snapshot(Arc::new(snapshot.clone()), base),
            base,
            wall,
        );
        for (cache, _) in caches {
            let digest = testkit::ownership_digest(&snapshot, cache).expect("eligible");
            model.apply(Update::Ownership(digest), base, wall);
        }
        let error = explain_report(&model, &args("k1", None)).expect_err("two caches are ranked");
        assert!(
            error.to_string().contains("name one with --cache"),
            "{error}"
        );
        assert!(explain_report(&model, &args("k1", Some("ids"))).is_ok());
    }
}
