//! `watch --once`: one text or JSON report of the cluster.
//!
//! The run joins the cluster's gossip, waits until the member set has held
//! still for `--settle`, takes one scrape round, prints a report and exits.
//! [`build_report`] and [`render_text`] are pure over a [`Model`].

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, bail};
use serde::Serialize;
use sundog::observe::MemberStatus;

use crate::cli::{OnceArgs, WatchArgs};
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
    /// digests and the first scrape of every exporter.
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
    }
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

/// The report as text: a summary line, the members and the caches.
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

/// Feeds `model` from `feed` until the member set has held still for the
/// settle time and the model is complete, or the extras limit passes.
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
            if complete(model, scraping) || now.saturating_duration_since(since) >= limits.extras {
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
/// Returns an error when the observer cannot start or no member shows up.
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
    let report = build_report(&model, &observer, started.elapsed(), SystemTime::now());
    if once.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{}", render_text(&report));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::testkit;

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
        assert_eq!(report.protocols, [6]);
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
            "fixture · 5 live · 1 departing · 1 down · 1 left · protocol 6 · observed 3.0 s via observer 127.0.0.1:41733"
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
        assert!(table(&[]).is_empty());
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
    fn the_membership_key_lists_every_member_by_node_incarnation_and_status() {
        let model = testkit::fixture_model(Instant::now());
        let key = membership_key(&model);
        assert_eq!(key.len(), 8);
        assert_eq!(key[0], (testkit::node_id(1, 0), 1, MemberStatus::Live));
        assert_eq!(key[7].2, MemberStatus::Left);
        assert!(membership_key(&Model::new()).is_empty());
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
}
