//! The metrics scraper: it maps each live member to its exporter URL and
//! scrapes `/metrics` and `/readyz` on an interval, reporting each round as an
//! [`Update::Scrape`].
//!
//! [`plan_targets`] decides the mapping and [`diff_targets`] what to start and
//! stop; both are pure. [`run`] keeps one task per target, started and
//! stopped as snapshots change which members are live.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sundog::NodeId;
use sundog::observe::{ClusterSnapshot, Member};
use tokio::sync::{mpsc, watch};
use tokio::task::{AbortHandle, JoinSet};

use super::Update;
use super::expo::{self, Sample};
use super::http::{self, HttpError};
use super::targets::{TemplateError, UrlTemplate};
use crate::cli::{ScrapePin, WatchArgs};
use crate::model::Slots;

/// How long one request to an exporter may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_millis(800);

/// How often a target's `/readyz` is probed.
pub const READY_EVERY: Duration = Duration::from_secs(2);

/// Why a scrape failed, or why there is nothing to scrape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScrapeError {
    /// The exporter did not answer within the deadline.
    Timeout,
    /// The connection failed.
    Connect(String),
    /// The exporter answered `/metrics` with a status other than 200.
    Status(u16),
    /// The answer is not a well-formed HTTP response.
    Malformed(String),
    /// The member's exporter URL is also another member's, so neither is
    /// scraped.
    Collision(String),
    /// No `--metrics` template expands for the member.
    Template(String),
}

impl ScrapeError {
    /// Whether the error is about the mapping from member to URL rather than
    /// about the exporter: no scrape was tried.
    #[must_use]
    pub const fn is_mapping(&self) -> bool {
        matches!(self, Self::Collision(_) | Self::Template(_))
    }
}

impl From<HttpError> for ScrapeError {
    fn from(error: HttpError) -> Self {
        match error {
            HttpError::Timeout => Self::Timeout,
            HttpError::Io(message) => Self::Connect(message),
            other => Self::Malformed(other.to_string()),
        }
    }
}

impl fmt::Display for ScrapeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("timed out"),
            Self::Connect(message) | Self::Malformed(message) => f.write_str(message),
            Self::Status(status) => write!(f, "HTTP {status}"),
            Self::Collision(url) => write!(f, "{url} is the exporter URL of more than one member"),
            Self::Template(message) => write!(f, "no exporter URL: {message}"),
        }
    }
}

impl std::error::Error for ScrapeError {}

/// The outcome of one scrape round of one node.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrapeReport {
    /// The gossip address of the member the exporter is mapped to.
    pub addr: SocketAddr,
    /// The node id of that member when the scrape started.
    pub node: NodeId,
    /// When `/metrics` answered or failed: the instant of the counter samples.
    pub at: Instant,
    /// The `sundog_*` samples of `/metrics`, or why there are none.
    pub outcome: Result<Vec<Sample>, ScrapeError>,
    /// The `/readyz` verdict: `Some(true)` for 200, `Some(false)` for 503,
    /// `None` when not probed in this round or the probe failed.
    pub ready: Option<bool>,
}

/// How the scraper finds and polls exporters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrapeConfig {
    /// The `--metrics` templates, tried in order for a member no pin names.
    pub templates: Vec<UrlTemplate>,
    /// The `--scrape` pins, tried in order before the templates.
    pub pins: Vec<ScrapePin>,
    /// The time between rounds of one target.
    pub interval: Duration,
    /// The deadline of one request.
    pub timeout: Duration,
    /// The time between `/readyz` probes of one target.
    pub ready_every: Duration,
}

impl ScrapeConfig {
    /// A configuration with the default interval of 1 s, [`REQUEST_TIMEOUT`]
    /// and [`READY_EVERY`].
    #[must_use]
    pub fn new(templates: Vec<UrlTemplate>, pins: Vec<ScrapePin>) -> Self {
        Self {
            templates,
            pins,
            interval: Duration::from_secs(1),
            timeout: REQUEST_TIMEOUT,
            ready_every: READY_EVERY,
        }
    }

    /// The scrape configuration `args` ask for; `None` when they give neither
    /// a template nor a pin.
    ///
    /// # Errors
    ///
    /// Returns the first template that does not parse.
    pub fn from_args(args: &WatchArgs) -> Result<Option<Self>, TemplateError> {
        if args.metrics.is_empty() && args.scrape.is_empty() {
            return Ok(None);
        }
        let templates = args
            .metrics
            .iter()
            .map(|template| UrlTemplate::parse(template))
            .collect::<Result<Vec<_>, _>>()?;
        let mut config = Self::new(templates, args.scrape.clone());
        config.interval = args.interval;
        Ok(Some(config))
    }
}

/// One exporter to scrape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The gossip address of the member.
    pub addr: SocketAddr,
    /// The member's node id.
    pub node: NodeId,
    /// The `/metrics` URL.
    pub url: String,
}

/// A live member that has no exporter to scrape and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unmapped {
    /// The gossip address of the member.
    pub addr: SocketAddr,
    /// The member's node id.
    pub node: NodeId,
    /// A [`ScrapeError::Collision`] or a [`ScrapeError::Template`].
    pub error: ScrapeError,
}

/// The exporters to scrape and the live members that have none.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// One target per mapped live member, in member order.
    pub targets: Vec<Target>,
    /// Live members with a mapping error, in member order.
    pub unmapped: Vec<Unmapped>,
}

/// Whether `pin` names `member`: `pin.node` is the member's gossip `ip:port`,
/// its slot `label` or a prefix of its node id in hex. A prefix is
/// case-insensitive.
#[must_use]
pub fn pin_matches(pin: &ScrapePin, member: &Member, label: Option<&str>) -> bool {
    let peer = &member.peer;
    if pin
        .node
        .parse::<SocketAddr>()
        .is_ok_and(|addr| addr == peer.gossip_addr)
    {
        return true;
    }
    if label == Some(pin.node.as_str()) {
        return true;
    }
    !pin.node.is_empty()
        && pin.node.bytes().all(|byte| byte.is_ascii_hexdigit())
        && peer
            .node
            .to_string()
            .starts_with(&pin.node.to_ascii_lowercase())
}

/// The live members that are the newest record at their gossip address, in
/// member order. The newest is the greater (incarnation, node id), as in
/// `events::superseded`: while gossip still lists a restarted node's old
/// incarnation next to the new one, only the new one has an exporter.
fn newest_live_per_address(members: &[Member]) -> impl Iterator<Item = &Member> {
    let mut newest: BTreeMap<SocketAddr, (u64, NodeId)> = BTreeMap::new();
    for member in members.iter().filter(|member| member.status.is_live()) {
        let key = (member.peer.incarnation, member.peer.node);
        let entry = newest.entry(member.peer.gossip_addr).or_insert(key);
        *entry = (*entry).max(key);
    }
    members.iter().filter(move |member| {
        member.status.is_live()
            && newest.get(&member.peer.gossip_addr)
                == Some(&(member.peer.incarnation, member.peer.node))
    })
}

/// Maps every live member to an exporter URL. An older incarnation at an
/// address that a newer live record holds is not mapped.
///
/// The first pin that names a member gives its URL. Otherwise the first
/// template that expands gives it; with templates and none expanding the
/// member is [`Unmapped`] with a [`ScrapeError::Template`]. A member without
/// pin or templates has no exporter and appears nowhere. When several members
/// map to one URL none of them is scraped: each is [`Unmapped`] with a
/// [`ScrapeError::Collision`].
#[must_use]
pub fn plan_targets(members: &[Member], config: &ScrapeConfig, slots: &Slots) -> Plan {
    let mut mapped: Vec<(&Member, Result<String, ScrapeError>)> = Vec::new();
    for member in newest_live_per_address(members) {
        let label = slots
            .get(member.peer.gossip_addr)
            .map(|slot| slot.label.as_str());
        let pinned = config
            .pins
            .iter()
            .find(|pin| pin_matches(pin, member, label))
            .map(|pin| pin.url.clone());
        if let Some(url) = pinned {
            mapped.push((member, Ok(url)));
            continue;
        }
        let mut first_error = None;
        let mut url = None;
        for template in &config.templates {
            match template.expand(member) {
                Ok(expanded) => {
                    url = Some(expanded);
                    break;
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        match (url, first_error) {
            (Some(url), _) => mapped.push((member, Ok(url))),
            (None, Some(error)) => {
                mapped.push((member, Err(ScrapeError::Template(error.to_string()))));
            }
            (None, None) => {}
        }
    }
    let mut users: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, outcome) in &mapped {
        if let Ok(url) = outcome {
            *users.entry(url.as_str()).or_default() += 1;
        }
    }
    let mut plan = Plan::default();
    for (member, outcome) in &mapped {
        let (addr, node) = (member.peer.gossip_addr, member.peer.node);
        match outcome {
            Ok(url) if users[url.as_str()] > 1 => plan.unmapped.push(Unmapped {
                addr,
                node,
                error: ScrapeError::Collision(url.clone()),
            }),
            Ok(url) => plan.targets.push(Target {
                addr,
                node,
                url: url.clone(),
            }),
            Err(error) => plan.unmapped.push(Unmapped {
                addr,
                node,
                error: error.clone(),
            }),
        }
    }
    plan
}

/// The targets to start and the targets to stop to go from `running` to
/// `wanted`. A target whose address, node or URL changed is stopped and
/// started again.
#[must_use]
pub fn diff_targets(running: &[Target], wanted: &[Target]) -> (Vec<Target>, Vec<Target>) {
    let start = wanted
        .iter()
        .filter(|target| !running.contains(target))
        .cloned()
        .collect();
    let stop = running
        .iter()
        .filter(|target| !wanted.contains(target))
        .cloned()
        .collect();
    (start, stop)
}

/// The `/readyz` URL of the exporter whose `/metrics` URL is `url`.
///
/// # Errors
///
/// Returns [`HttpError::BadUrl`] when `url` is not an `http://` URL.
pub fn readyz_url(url: &str) -> Result<String, HttpError> {
    let split = http::split_url(url)?;
    Ok(format!("http://{}/readyz", split.host))
}

/// Fetches `url` and returns its `sundog_*` samples.
///
/// # Errors
///
/// Returns the transport error, or [`ScrapeError::Status`] for an answer
/// other than 200.
pub async fn fetch_samples(url: &str, timeout: Duration) -> Result<Vec<Sample>, ScrapeError> {
    let (status, body) = http::get(url, timeout).await?;
    if status != 200 {
        return Err(ScrapeError::Status(status));
    }
    Ok(expo::parse(&body))
}

/// Probes `/readyz` of the exporter at `url`: `Some(true)` for a 200,
/// `Some(false)` for a 503, `None` for any other answer or a failure.
pub async fn probe_ready(url: &str, timeout: Duration) -> Option<bool> {
    let ready = readyz_url(url).ok()?;
    match http::get(&ready, timeout).await.ok()?.0 {
        200 => Some(true),
        503 => Some(false),
        _ => None,
    }
}

/// One scrape round of `target`: `/metrics` and, when `probe` is set,
/// `/readyz` at the same time. The report's `at` is the moment `/metrics`
/// answered, however long `/readyz` takes.
pub async fn scrape_round(target: &Target, timeout: Duration, probe: bool) -> ScrapeReport {
    let metrics = async {
        let outcome = fetch_samples(&target.url, timeout).await;
        (outcome, Instant::now())
    };
    let ready = async {
        if probe {
            probe_ready(&target.url, timeout).await
        } else {
            None
        }
    };
    let ((outcome, at), ready) = tokio::join!(metrics, ready);
    ScrapeReport {
        addr: target.addr,
        node: target.node,
        at,
        outcome,
        ready,
    }
}

/// Whether the round at `now` probes `/readyz`: the first round does, and a
/// later one does once `every` has passed since the last probe.
#[must_use]
pub fn probe_due(last_probe: Option<Instant>, now: Instant, every: Duration) -> bool {
    last_probe.is_none_or(|at| now.saturating_duration_since(at) >= every)
}

/// Scrapes `target` every `interval` until `updates` closes, probing
/// `/readyz` every `ready_every`.
async fn scrape_target(
    target: Target,
    interval: Duration,
    timeout: Duration,
    ready_every: Duration,
    updates: mpsc::Sender<Update>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_probe: Option<Instant> = None;
    loop {
        ticker.tick().await;
        let probe = probe_due(last_probe, Instant::now(), ready_every);
        if probe {
            last_probe = Some(Instant::now());
        }
        let report = scrape_round(&target, timeout, probe).await;
        if updates.send(Update::Scrape(report)).await.is_err() {
            return;
        }
    }
}

/// Runs the scraper until the snapshot sender is dropped or `updates` closes.
///
/// Every snapshot re-plans the targets: a task starts for each newly mapped
/// live member, stops for a member that is no longer live, and restarts when
/// the member's node id or URL changes. A member with a mapping error gets one
/// report carrying it, and another whenever the error changes.
///
/// `labels` carries the slot labels `--scrape` pins name nodes by. They are
/// the labels of the model, and the sender publishes them before the snapshot
/// that gives them, as [`observer::Relay`](super::observer::Relay) does.
pub async fn run(
    config: ScrapeConfig,
    mut snapshots: watch::Receiver<Arc<ClusterSnapshot>>,
    labels: watch::Receiver<Arc<Slots>>,
    updates: mpsc::Sender<Update>,
) {
    let mut tasks = JoinSet::new();
    let mut running: Vec<(Target, AbortHandle)> = Vec::new();
    let mut told: BTreeMap<SocketAddr, (NodeId, ScrapeError)> = BTreeMap::new();
    loop {
        while tasks.try_join_next().is_some() {}
        let snapshot = Arc::clone(&snapshots.borrow_and_update());
        let slots = Arc::clone(&labels.borrow());
        let plan = plan_targets(&snapshot.members, &config, &slots);
        let held: Vec<Target> = running.iter().map(|(target, _)| target.clone()).collect();
        let (start, stop) = diff_targets(&held, &plan.targets);
        running.retain(|(target, handle)| {
            let keep = !stop.contains(target);
            if !keep {
                handle.abort();
            }
            keep
        });
        for target in start {
            let handle = tasks.spawn(scrape_target(
                target.clone(),
                config.interval,
                config.timeout,
                config.ready_every,
                updates.clone(),
            ));
            running.push((target, handle));
        }
        told.retain(|addr, _| plan.unmapped.iter().any(|unmapped| unmapped.addr == *addr));
        for unmapped in plan.unmapped {
            let news = (unmapped.node, unmapped.error.clone());
            if told.get(&unmapped.addr) == Some(&news) {
                continue;
            }
            told.insert(unmapped.addr, news);
            let report = ScrapeReport {
                addr: unmapped.addr,
                node: unmapped.node,
                at: Instant::now(),
                outcome: Err(unmapped.error),
                ready: None,
            };
            if updates.send(Update::Scrape(report)).await.is_err() {
                return;
            }
        }
        tokio::select! {
            changed = snapshots.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = updates.closed() => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use sundog::observe::{ClusterSnapshot, MemberStatus};

    use super::*;
    use crate::model::testkit;

    fn template(text: &str) -> UrlTemplate {
        UrlTemplate::parse(text).unwrap()
    }

    fn pin(node: &str, url: &str) -> ScrapePin {
        ScrapePin {
            node: node.to_owned(),
            url: url.to_owned(),
        }
    }

    fn config(templates: &[&str], pins: Vec<ScrapePin>) -> ScrapeConfig {
        ScrapeConfig::new(templates.iter().map(|t| template(t)).collect(), pins)
    }

    fn slots_for(snapshot: &ClusterSnapshot) -> Slots {
        let mut slots = Slots::new();
        for member in &snapshot.members {
            slots.assign(member.peer.gossip_addr);
        }
        slots
    }

    fn urls(plan: &Plan) -> Vec<&str> {
        plan.targets.iter().map(|t| t.url.as_str()).collect()
    }

    #[test]
    fn http_errors_map_to_scrape_errors() {
        assert_eq!(ScrapeError::from(HttpError::Timeout), ScrapeError::Timeout);
        assert_eq!(
            ScrapeError::from(HttpError::Io("refused".into())),
            ScrapeError::Connect("refused".into())
        );
        assert!(matches!(
            ScrapeError::from(HttpError::NoHeaderEnd),
            ScrapeError::Malformed(_)
        ));
        assert!(matches!(
            ScrapeError::from(HttpError::BadUrl("x".into())),
            ScrapeError::Malformed(_)
        ));
    }

    #[test]
    fn scrape_errors_display_a_reason() {
        assert_eq!(ScrapeError::Timeout.to_string(), "timed out");
        assert_eq!(ScrapeError::Status(503).to_string(), "HTTP 503");
        assert_eq!(
            ScrapeError::Connect("refused".into()).to_string(),
            "refused"
        );
        assert_eq!(ScrapeError::Malformed("bad".into()).to_string(), "bad");
        assert_eq!(
            ScrapeError::Collision("http://h/m".into()).to_string(),
            "http://h/m is the exporter URL of more than one member"
        );
        assert_eq!(
            ScrapeError::Template("port overflow".into()).to_string(),
            "no exporter URL: port overflow"
        );
    }

    #[test]
    fn only_collisions_and_template_errors_are_mapping_errors() {
        assert!(ScrapeError::Collision("u".into()).is_mapping());
        assert!(ScrapeError::Template("t".into()).is_mapping());
        assert!(!ScrapeError::Timeout.is_mapping());
        assert!(!ScrapeError::Connect("c".into()).is_mapping());
        assert!(!ScrapeError::Status(500).is_mapping());
        assert!(!ScrapeError::Malformed("m".into()).is_mapping());
    }

    #[test]
    fn a_config_takes_templates_pins_and_interval_from_the_arguments() {
        let command = crate::cli::parse([
            "watch",
            "prod",
            "--metrics",
            "http://{ip}:9090/metrics",
            "--scrape",
            "n1=http://h:1/metrics",
            "--interval",
            "250ms",
        ])
        .unwrap();
        let crate::cli::Command::Watch(args) = command else {
            panic!("a watch command");
        };
        let config = ScrapeConfig::from_args(&args).unwrap().unwrap();
        assert_eq!(config.templates, [template("http://{ip}:9090/metrics")]);
        assert_eq!(config.pins, [pin("n1", "http://h:1/metrics")]);
        assert_eq!(config.interval, Duration::from_millis(250));
        assert_eq!(config.timeout, REQUEST_TIMEOUT);
        assert_eq!(config.ready_every, READY_EVERY);
    }

    #[test]
    fn arguments_with_no_template_and_no_pin_ask_for_no_scraping() {
        let command = crate::cli::parse(["watch", "prod"]).unwrap();
        let crate::cli::Command::Watch(args) = command else {
            panic!("a watch command");
        };
        assert_eq!(ScrapeConfig::from_args(&args), Ok(None));
    }

    #[test]
    fn a_bad_template_fails_the_config() {
        let command = crate::cli::parse(["watch", "prod", "--metrics", "http://{host}/m"]).unwrap();
        let crate::cli::Command::Watch(args) = command else {
            panic!("a watch command");
        };
        assert_eq!(
            ScrapeConfig::from_args(&args),
            Err(TemplateError::UnknownPlaceholder("host".into()))
        );
    }

    #[test]
    fn a_pin_names_a_member_by_address_label_or_id_prefix() {
        let member = testkit::member(1, MemberStatus::Live);
        let id = member.peer.node.to_string();
        let by = |node: &str, label| pin_matches(&pin(node, "http://h/m"), &member, label);
        assert!(by("127.0.0.11:7946", None));
        assert!(!by("127.0.0.11:7947", None));
        assert!(!by("127.0.0.12:7946", None));
        assert!(by("n1", Some("n1")));
        assert!(!by("n1", Some("n2")));
        assert!(!by("n1", None));
        assert!(by(&id[..6], None));
        assert!(by(&id.to_ascii_uppercase()[..6], None));
        assert!(by(&id, None));
        assert!(!by("0000000000009999", None));
        assert!(!by("", None), "an empty prefix names nobody");
    }

    #[test]
    fn a_template_maps_every_live_member_in_member_order() {
        let snapshot = testkit::snapshot(3);
        let plan = plan_targets(
            &snapshot.members,
            &config(&["http://{ip}:9090/metrics"], Vec::new()),
            &slots_for(&snapshot),
        );
        assert_eq!(
            urls(&plan),
            [
                "http://127.0.0.11:9090/metrics",
                "http://127.0.0.12:9090/metrics",
                "http://127.0.0.13:9090/metrics",
            ]
        );
        assert!(plan.unmapped.is_empty(), "{:?}", plan.unmapped);
        assert_eq!(plan.targets[1].addr, testkit::gossip_addr(2));
        assert_eq!(plan.targets[1].node, testkit::node_id(2, 0));
    }

    #[test]
    fn only_live_and_departing_members_are_mapped() {
        let snapshot = testkit::mixed_snapshot();
        let plan = plan_targets(
            &snapshot.members,
            &config(&["http://{ip}:9090/metrics"], Vec::new()),
            &slots_for(&snapshot),
        );
        assert_eq!(plan.targets.len(), 6, "five live and one departing");
        assert!(
            plan.targets.iter().all(|t| {
                t.addr != testkit::gossip_addr(7) && t.addr != testkit::gossip_addr(8)
            })
        );
    }

    #[test]
    fn a_pin_beats_the_templates_and_the_first_expanding_template_wins() {
        let snapshot = testkit::snapshot(2);
        let slots = slots_for(&snapshot);
        let plan = plan_targets(
            &snapshot.members,
            &config(
                &["http://{ip}:{gossip_port-9000}/m", "http://{ip}:1/second"],
                vec![pin("n2", "http://pinned:2/metrics")],
            ),
            &slots,
        );
        assert_eq!(
            urls(&plan),
            ["http://127.0.0.11:1/second", "http://pinned:2/metrics"],
            "n1 falls to the second template because the first underflows"
        );
    }

    #[test]
    fn the_first_matching_pin_wins() {
        let snapshot = testkit::snapshot(1);
        let plan = plan_targets(
            &snapshot.members,
            &config(
                &[],
                vec![
                    pin("127.0.0.11:7946", "http://first:1/m"),
                    pin("n1", "http://second:2/m"),
                ],
            ),
            &slots_for(&snapshot),
        );
        assert_eq!(urls(&plan), ["http://first:1/m"]);
    }

    #[test]
    fn a_member_no_template_expands_for_is_unmapped_with_the_reason() {
        let snapshot = testkit::snapshot(1);
        let plan = plan_targets(
            &snapshot.members,
            &config(&["http://{ip}:{gossip_port-9000}/m"], Vec::new()),
            &slots_for(&snapshot),
        );
        assert!(plan.targets.is_empty(), "{:?}", plan.targets);
        let [unmapped] = plan.unmapped.as_slice() else {
            panic!("one unmapped member");
        };
        assert_eq!(unmapped.addr, testkit::gossip_addr(1));
        assert!(matches!(&unmapped.error, ScrapeError::Template(m) if m.contains("below 0")));
    }

    #[test]
    fn a_member_without_pin_or_template_has_no_exporter_and_no_entry() {
        let snapshot = testkit::snapshot(2);
        let plan = plan_targets(
            &snapshot.members,
            &config(&[], vec![pin("n1", "http://h:1/m")]),
            &slots_for(&snapshot),
        );
        assert_eq!(urls(&plan), ["http://h:1/m"]);
        assert!(plan.unmapped.is_empty(), "{:?}", plan.unmapped);
    }

    #[test]
    fn members_that_map_to_one_url_are_both_refused() {
        let snapshot = testkit::snapshot(3);
        let plan = plan_targets(
            &snapshot.members,
            &config(&["http://shared:9090/metrics"], Vec::new()),
            &slots_for(&snapshot),
        );
        assert!(plan.targets.is_empty(), "{:?}", plan.targets);
        assert_eq!(plan.unmapped.len(), 3);
        assert!(
            plan.unmapped.iter().all(|u| {
                u.error == ScrapeError::Collision("http://shared:9090/metrics".into())
            })
        );
    }

    #[test]
    fn a_collision_spares_the_members_with_their_own_url() {
        let snapshot = testkit::snapshot(3);
        let plan = plan_targets(
            &snapshot.members,
            &config(
                &["http://{ip}:9090/metrics"],
                vec![pin("n3", "http://127.0.0.11:9090/metrics")],
            ),
            &slots_for(&snapshot),
        );
        assert_eq!(urls(&plan), ["http://127.0.0.12:9090/metrics"]);
        let unmapped: Vec<_> = plan.unmapped.iter().map(|u| u.addr).collect();
        assert_eq!(unmapped, [testkit::gossip_addr(1), testkit::gossip_addr(3)]);
    }

    #[test]
    fn a_restarted_node_next_to_its_live_old_record_is_the_only_target() {
        // Gossip still lists the old incarnation live while the new one is up.
        let old = testkit::member_at(1, 0, 10, MemberStatus::Live);
        let new = testkit::member_at(1, 1, 20, MemberStatus::Live);
        let other = testkit::member_at(2, 0, 10, MemberStatus::Live);
        for members in [
            vec![old.clone(), new.clone(), other.clone()],
            vec![new.clone(), old.clone(), other.clone()],
        ] {
            let snapshot = ClusterSnapshot::new("fixture", members, 0);
            let plan = plan_targets(
                &snapshot.members,
                &config(&["http://{ip}:9090/metrics"], Vec::new()),
                &slots_for(&snapshot),
            );
            assert_eq!(plan.unmapped, [], "no collision with the old record");
            assert_eq!(
                plan.targets.len(),
                2,
                "one target per address: {:?}",
                plan.targets
            );
            assert!(
                plan.targets
                    .contains(&target(1, 1, "http://127.0.0.11:9090/metrics"))
            );
            assert!(
                plan.targets
                    .contains(&target(2, 0, "http://127.0.0.12:9090/metrics"))
            );
        }
    }

    #[test]
    fn records_at_one_address_with_one_incarnation_keep_the_greater_node_id() {
        let a = testkit::member_at(1, 0, 10, MemberStatus::Live);
        let b = testkit::member_at(1, 1, 10, MemberStatus::Live);
        let newer = a.peer.node.max(b.peer.node);
        let snapshot = ClusterSnapshot::new("fixture", vec![a, b], 0);
        let plan = plan_targets(
            &snapshot.members,
            &config(&["http://{ip}:9090/metrics"], Vec::new()),
            &slots_for(&snapshot),
        );
        assert_eq!(plan.unmapped, []);
        assert_eq!(plan.targets.len(), 1);
        assert_eq!(plan.targets[0].node, newer);
    }

    #[test]
    fn pins_follow_label_hints() {
        let snapshot = testkit::snapshot(2);
        let mut slots = Slots::new();
        slots.hint(testkit::gossip_addr(2), "alpha");
        for member in &snapshot.members {
            slots.assign(member.peer.gossip_addr);
        }
        let plan = plan_targets(
            &snapshot.members,
            &config(&[], vec![pin("alpha", "http://a:1/m")]),
            &slots,
        );
        assert_eq!(plan.targets.len(), 1);
        assert_eq!(plan.targets[0].addr, testkit::gossip_addr(2));
    }

    fn target(index: u8, generation: u16, url: &str) -> Target {
        Target {
            addr: testkit::gossip_addr(index),
            node: testkit::node_id(index, generation),
            url: url.to_owned(),
        }
    }

    #[test]
    fn diffing_targets_starts_the_new_and_stops_the_gone() {
        let a = target(1, 0, "http://a/m");
        let b = target(2, 0, "http://b/m");
        let c = target(3, 0, "http://c/m");
        let (start, stop) = diff_targets(&[a.clone(), b.clone()], &[b.clone(), c.clone()]);
        assert_eq!(start, [c]);
        assert_eq!(stop, [a]);
        let (start, stop) = diff_targets(std::slice::from_ref(&b), std::slice::from_ref(&b));
        assert!(start.is_empty() && stop.is_empty());
    }

    #[test]
    fn a_new_node_or_url_at_an_address_restarts_its_target() {
        let old = target(1, 0, "http://a/m");
        let reborn = target(1, 1, "http://a/m");
        let moved = target(1, 0, "http://b/m");
        for changed in [reborn, moved] {
            let (start, stop) =
                diff_targets(std::slice::from_ref(&old), std::slice::from_ref(&changed));
            assert_eq!(start, [changed]);
            assert_eq!(stop.as_slice(), std::slice::from_ref(&old));
        }
    }

    #[test]
    fn readiness_is_probed_on_the_first_round_and_then_once_per_period() {
        let start = Instant::now();
        let every = Duration::from_secs(2);
        assert!(probe_due(None, start, every));
        assert!(!probe_due(Some(start), start, every));
        assert!(!probe_due(
            Some(start),
            start + Duration::from_millis(1999),
            every
        ));
        assert!(probe_due(Some(start), start + every, every));
        assert!(probe_due(
            Some(start),
            start + Duration::from_secs(9),
            every
        ));
        assert!(
            !probe_due(Some(start + every), start, every),
            "a clock that went back is not due"
        );
    }

    #[test]
    fn the_readyz_url_keeps_the_authority_and_replaces_the_path() {
        assert_eq!(
            readyz_url("http://127.0.0.11:9090/metrics").unwrap(),
            "http://127.0.0.11:9090/readyz"
        );
        assert_eq!(
            readyz_url("http://[::1]:9090/some/metrics?x=1").unwrap(),
            "http://[::1]:9090/readyz"
        );
        assert_eq!(readyz_url("http://host").unwrap(), "http://host/readyz");
        assert!(readyz_url("https://h/metrics").is_err());
    }
}
