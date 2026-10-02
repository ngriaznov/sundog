//! What one node's exporter reports, folded over time: the latest value of
//! every series, the rate of every counter, and the rate histories the charts
//! draw.

use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use smol_str::SmolStr;
use sundog::NodeId;

use super::derive::{self, FetchMix, QUIET_SCRAPES};
use super::series::{CounterTrack, RING_LEN, Ring};
use crate::source::expo::Sample;
use crate::source::names;

/// One metric series: a name and its labels, sorted by label name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeriesKey {
    name: String,
    labels: Vec<(String, String)>,
}

impl SeriesKey {
    /// The series `sample` belongs to.
    #[must_use]
    pub fn of(sample: &Sample) -> Self {
        let mut labels = sample.labels.clone();
        labels.sort();
        Self {
            name: sample.name.clone(),
            labels,
        }
    }

    /// The metric name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The labels, sorted by name.
    #[must_use]
    pub fn labels(&self) -> &[(String, String)] {
        &self.labels
    }

    /// The value of label `key`, if the series has it.
    #[must_use]
    pub fn label(&self, key: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    /// Whether the series is metric `name` and carries every `(label, value)`
    /// pair in `filters`.
    #[must_use]
    pub fn matches(&self, name: &str, filters: &[(&str, &str)]) -> bool {
        self.name == name
            && filters
                .iter()
                .all(|(key, value)| self.label(key) == Some(*value))
    }
}

/// Whether metric `name` is a counter: the exporter names every counter with
/// the `_total` suffix.
#[must_use]
pub fn is_counter(name: &str) -> bool {
    name.ends_with("_total")
}

/// A drop of frames one scrape reveals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropEdge {
    /// The peer id as the exporter labels it.
    pub peer: SmolStr,
    /// Frames dropped since the previous scrape.
    pub frames: u64,
}

/// What folding one scrape into [`NodeMetrics`] reveals.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Folded {
    /// Peers whose `backlog_dropped_total` rose.
    pub drops: Vec<DropEdge>,
    /// Whether the state-transfer rate became nonzero with this scrape.
    pub xfer_started: bool,
}

/// The run of quiet folds one cache is on: the folds in which the node pulled
/// no part of it in.
#[derive(Debug, Clone, Default)]
struct Quiet {
    /// The folds in the run.
    count: u32,
    /// Where the rate window of each of the newest [`QUIET_SCRAPES`] quiet
    /// folds began: the instant of the fold before it.
    windows: VecDeque<Instant>,
}

impl Quiet {
    fn reset(&mut self) {
        self.count = 0;
        self.windows.clear();
    }

    fn extend(&mut self, window_start: Option<Instant>) {
        self.count += 1;
        let Some(start) = window_start else { return };
        if self.windows.len() >= QUIET_SCRAPES as usize {
            self.windows.pop_front();
        }
        self.windows.push_back(start);
    }
}

/// One node's metrics over time.
///
/// A fold takes the `sundog_*` samples of one successful scrape. Each fold
/// replaces the latest value of every series, so a series absent from a scrape
/// has no value until it reappears. A counter's rate needs two folds and is
/// absent after a counter reset.
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    node: NodeId,
    folds: u32,
    last_at: Option<Instant>,
    values: BTreeMap<SeriesKey, f64>,
    rates: BTreeMap<SeriesKey, f64>,
    tracks: BTreeMap<SeriesKey, CounterTrack>,
    dropped_seen: BTreeMap<SmolStr, f64>,
    xfer_active: bool,
    quiet: BTreeMap<SmolStr, Quiet>,
    ops: Ring<RING_LEN>,
    tx_bytes: Ring<RING_LEN>,
    tx_frames: Ring<RING_LEN>,
    rebalance_in: BTreeMap<SmolStr, Ring<RING_LEN>>,
    rebalance_out: BTreeMap<SmolStr, Ring<RING_LEN>>,
    entries_history: BTreeMap<SmolStr, Ring<RING_LEN>>,
    hit_history: BTreeMap<SmolStr, Ring<RING_LEN>>,
    backlog_history: BTreeMap<SmolStr, Ring<RING_LEN>>,
}

impl NodeMetrics {
    /// Metrics of `node`, with no fold yet.
    #[must_use]
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            folds: 0,
            last_at: None,
            values: BTreeMap::new(),
            rates: BTreeMap::new(),
            tracks: BTreeMap::new(),
            dropped_seen: BTreeMap::new(),
            xfer_active: false,
            quiet: BTreeMap::new(),
            ops: Ring::new(),
            tx_bytes: Ring::new(),
            tx_frames: Ring::new(),
            rebalance_in: BTreeMap::new(),
            rebalance_out: BTreeMap::new(),
            entries_history: BTreeMap::new(),
            hit_history: BTreeMap::new(),
            backlog_history: BTreeMap::new(),
        }
    }

    /// The node the metrics belong to.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }

    /// How many scrapes have been folded.
    #[must_use]
    pub const fn folds(&self) -> u32 {
        self.folds
    }

    /// Withdraws the rates after a failed scrape: a rate describes the span
    /// between two answers, so a node that stopped answering has none. The
    /// latest values, the counter tracks and the quiet counts stay, so the
    /// settle vote and the next rate span are unchanged.
    pub fn mark_stale(&mut self) {
        self.rates.clear();
    }

    /// Folds one scrape taken at `at` into the metrics and returns what it
    /// reveals.
    ///
    /// Each fold replaces the latest values and rates: a series absent from
    /// the scrape has no value until it reappears; its counter track is kept,
    /// so its next rate spans back to its last sample. A
    /// `backlog_dropped_total` series that is new after the first fold counts
    /// all its frames as dropped, because the exporter creates the series at
    /// the first drop; one that rose counts the rise; one that fell is a
    /// counter reset and counts nothing. Each cache the node reports owning
    /// parts of gets a quiet-scrape count: the consecutive folds in which no
    /// part was pulled in. The exporter creates the
    /// `rebalance_parts_total{direction="in"}` series at the first pull, so a
    /// fold that adds it with a positive value is a pull, at the rate of that
    /// value over the time since the previous fold.
    ///
    /// The reads history counts the cache hits and misses and the fetches that
    /// bypass the local read. A local fetch is a hit or a miss already; a miss
    /// in a cold owned part that goes on to a remote owner counts as a miss
    /// and as a fetch.
    pub fn fold(&mut self, at: Instant, samples: &[Sample]) -> Folded {
        let first = self.folds == 0;
        self.values.clear();
        self.rates.clear();
        let mut created = Vec::new();
        for sample in samples {
            let key = SeriesKey::of(sample);
            if is_counter(&sample.name) && sample.value.is_finite() {
                if !self.tracks.contains_key(&key) {
                    created.push(key.clone());
                }
                if let Some(rate) = self
                    .tracks
                    .entry(key.clone())
                    .or_default()
                    .observe(at, sample.value)
                {
                    self.rates.insert(key.clone(), rate);
                }
            }
            self.values.insert(key, sample.value);
        }
        let drops = self.fold_drops(first);
        let xfer = self.rate_sum(names::STATE_TRANSFER_RECORDS);
        let active = xfer.is_some_and(|rate| rate > 0.0);
        let xfer_started = active && !self.xfer_active;
        self.xfer_active = active;
        self.fold_caches(at, first, &created);
        self.fold_cache_histories(first);
        if !first {
            let bypassing: f64 = ["remote", "miss", "error"]
                .iter()
                .filter_map(|outcome| self.rate_sum_where(names::FETCH, &[("outcome", outcome)]))
                .sum();
            let reads = self.rate_sum(names::CACHE_HITS).unwrap_or(0.0)
                + self.rate_sum(names::CACHE_MISSES).unwrap_or(0.0)
                + bypassing;
            self.ops.push(reads);
            self.tx_bytes
                .push(self.rate_sum(names::BYTES_SENT).unwrap_or(0.0));
            self.tx_frames
                .push(self.rate_sum(names::FRAMES_SENT).unwrap_or(0.0));
        }
        self.folds += 1;
        self.last_at = Some(at);
        Folded {
            drops,
            xfer_started,
        }
    }

    fn fold_drops(&mut self, first: bool) -> Vec<DropEdge> {
        let mut edges = Vec::new();
        let current: Vec<(SmolStr, f64)> = self
            .values
            .iter()
            .filter(|(key, _)| key.name() == names::BACKLOG_DROPPED)
            .filter_map(|(key, &value)| Some((SmolStr::new(key.label("peer")?), value)))
            .collect();
        for (peer, value) in current {
            let rise = match self.dropped_seen.get(&peer) {
                Some(&seen) => (value > seen).then_some(value - seen),
                None => (!first && value > 0.0).then_some(value),
            };
            if let Some(rise) = rise {
                edges.push(DropEdge {
                    peer: peer.clone(),
                    frames: whole_frames(rise),
                });
            }
            self.dropped_seen.insert(peer, value);
        }
        edges
    }

    /// Pushes one sample per cache the node reports entries for: the entries
    /// and the fan-out backlog at every fold, and the hit percentage from the
    /// second fold on (0 at zero traffic).
    fn fold_cache_histories(&mut self, first: bool) {
        let caches: Vec<SmolStr> = self
            .values
            .keys()
            .filter(|key| key.name() == names::CACHE_ENTRIES)
            .filter_map(|key| key.label("cache").map(SmolStr::new))
            .collect();
        for cache in caches {
            let entries = self.cache_value(names::CACHE_ENTRIES, &cache);
            self.entries_history
                .entry(cache.clone())
                .or_default()
                .push(entries.unwrap_or(0.0));
            if let Some(backlog) = self.cache_value(names::FAN_OUT_BACKLOG, &cache) {
                self.backlog_history
                    .entry(cache.clone())
                    .or_default()
                    .push(backlog);
            }
            if !first {
                let hit = self.hit_ratio(&cache).map_or(0.0, |ratio| ratio * 100.0);
                self.hit_history.entry(cache).or_default().push(hit);
            }
        }
    }

    fn fold_caches(&mut self, at: Instant, first: bool, created: &[SeriesKey]) {
        let caches: Vec<SmolStr> = self
            .values
            .keys()
            .filter(|key| key.name() == names::OWNED_PARTS)
            .filter_map(|key| key.label("cache").map(SmolStr::new))
            .collect();
        let window_start = self.last_at;
        let span =
            window_start.map_or(0.0, |last| at.saturating_duration_since(last).as_secs_f64());
        for cache in caches {
            let into = [("cache", cache.as_str()), ("direction", "in")];
            let out = [("cache", cache.as_str()), ("direction", "out")];
            let present = self.value(names::REBALANCE_PARTS, &into);
            let appeared = !first
                && created
                    .iter()
                    .any(|key| key.matches(names::REBALANCE_PARTS, &into));
            let in_rate = match self.rate(names::REBALANCE_PARTS, &into) {
                Some(rate) => Some(rate),
                // The exporter creates the series at the first pull: the
                // parts it holds arrived since the previous fold.
                None if appeared => {
                    present.map(|value| if span > 0.0 { value / span } else { value })
                }
                // Until then nothing has been pulled in.
                None if present.is_none() && !first => Some(0.0),
                None => None,
            };
            let quiet = self.quiet.entry(cache.clone()).or_default();
            match in_rate {
                Some(rate) if rate > 0.0 => quiet.reset(),
                Some(_) => quiet.extend(window_start),
                None => {}
            }
            if !first {
                let out_rate = self.rate(names::REBALANCE_PARTS, &out).unwrap_or(0.0);
                self.rebalance_in
                    .entry(cache.clone())
                    .or_default()
                    .push(in_rate.unwrap_or(0.0));
                self.rebalance_out.entry(cache).or_default().push(out_rate);
            }
        }
    }

    /// The latest value of the first series of metric `name` that carries
    /// every `(label, value)` pair in `filters`.
    #[must_use]
    pub fn value(&self, name: &str, filters: &[(&str, &str)]) -> Option<f64> {
        self.values
            .iter()
            .find(|(key, _)| key.matches(name, filters))
            .map(|(_, &value)| value)
    }

    /// The latest value of metric `name` for `cache`.
    #[must_use]
    pub fn cache_value(&self, name: &str, cache: &str) -> Option<f64> {
        self.value(name, &[("cache", cache)])
    }

    /// The latest per-second rate of the first counter series of metric
    /// `name` that carries every pair in `filters`; `None` before two folds
    /// and after a reset.
    #[must_use]
    pub fn rate(&self, name: &str, filters: &[(&str, &str)]) -> Option<f64> {
        self.rates
            .iter()
            .find(|(key, _)| key.matches(name, filters))
            .map(|(_, &rate)| rate)
    }

    /// The summed rate of every series of counter `name`; `None` when no
    /// series has a rate.
    #[must_use]
    pub fn rate_sum(&self, name: &str) -> Option<f64> {
        self.rate_sum_where(name, &[])
    }

    /// The summed rate of every series of counter `name` that carries every
    /// pair in `filters`; `None` when no series has a rate.
    #[must_use]
    pub fn rate_sum_where(&self, name: &str, filters: &[(&str, &str)]) -> Option<f64> {
        let mut rates = self
            .rates
            .iter()
            .filter(|(key, _)| key.matches(name, filters))
            .map(|(_, &rate)| rate)
            .peekable();
        rates.peek()?;
        Some(rates.sum())
    }

    /// Every latest sample as `(series, value)`, ordered by series.
    pub fn samples(&self) -> impl Iterator<Item = (&SeriesKey, f64)> {
        self.values.iter().map(|(key, &value)| (key, value))
    }

    /// The parts of `cache` the node reports owning.
    #[must_use]
    pub fn owned_parts(&self, cache: &str) -> Option<f64> {
        self.cache_value(names::OWNED_PARTS, cache)
    }

    /// The peers the node reports seeing alive.
    #[must_use]
    pub fn live_peers(&self) -> Option<f64> {
        self.value(names::LIVE_PEERS, &[])
    }

    /// The entries the node holds in `cache`.
    #[must_use]
    pub fn entries(&self, cache: &str) -> Option<f64> {
        self.cache_value(names::CACHE_ENTRIES, cache)
    }

    /// The share of `cache` reads that hit; `None` at zero traffic or before a
    /// rate exists.
    #[must_use]
    pub fn hit_ratio(&self, cache: &str) -> Option<f64> {
        let hits = self.rate(names::CACHE_HITS, &[("cache", cache)])?;
        let misses = self.rate(names::CACHE_MISSES, &[("cache", cache)])?;
        derive::hit_ratio(hits, misses)
    }

    /// The mix of `cache` fetch outcomes; `None` at zero traffic or before a
    /// rate exists. An outcome with no series counts as zero.
    #[must_use]
    pub fn fetch_mix(&self, cache: &str) -> Option<FetchMix> {
        self.rate_sum_where(names::FETCH, &[("cache", cache)])?;
        let outcome = |name: &str| {
            self.rate(names::FETCH, &[("cache", cache), ("outcome", name)])
                .unwrap_or(0.0)
        };
        derive::fetch_mix(
            outcome("local"),
            outcome("remote"),
            outcome("miss"),
            outcome("error"),
        )
    }

    /// Consecutive folds in which the node pulled no part of `cache` in.
    #[must_use]
    pub fn quiet_scrapes(&self, cache: &str) -> u32 {
        self.quiet.get(cache).map_or(0, |quiet| quiet.count)
    }

    /// The quiet folds of the current run, newest first, whose rate window
    /// began at or after `since`: scrapes taken wholly inside a view that
    /// began at `since`. Counts at most [`QUIET_SCRAPES`].
    #[must_use]
    pub fn quiet_scrapes_since(&self, cache: &str, since: Instant) -> u32 {
        self.quiet.get(cache).map_or(0, |quiet| {
            let inside = quiet
                .windows
                .iter()
                .filter(|start| **start >= since)
                .count();
            u32::try_from(inside).unwrap_or(u32::MAX)
        })
    }

    /// Reads and fetches per second, one sample per fold after the first.
    #[must_use]
    pub const fn ops(&self) -> &Ring<RING_LEN> {
        &self.ops
    }

    /// Bytes sent per second, one sample per fold after the first.
    #[must_use]
    pub const fn tx_bytes(&self) -> &Ring<RING_LEN> {
        &self.tx_bytes
    }

    /// Frames sent per second, one sample per fold after the first.
    #[must_use]
    pub const fn tx_frames(&self) -> &Ring<RING_LEN> {
        &self.tx_frames
    }

    /// Parts of `cache` pulled in per second, one sample per fold after the
    /// first.
    #[must_use]
    pub fn rebalance_in(&self, cache: &str) -> Option<&Ring<RING_LEN>> {
        self.rebalance_in.get(cache)
    }

    /// Parts of `cache` released per second, one sample per fold after the
    /// first.
    #[must_use]
    pub fn rebalance_out(&self, cache: &str) -> Option<&Ring<RING_LEN>> {
        self.rebalance_out.get(cache)
    }

    /// The entries the node held in `cache`, one sample per fold.
    #[must_use]
    pub fn entries_history(&self, cache: &str) -> Option<&Ring<RING_LEN>> {
        self.entries_history.get(cache)
    }

    /// The percentage of `cache` reads that hit, one sample per fold after
    /// the first; 0 at zero traffic.
    #[must_use]
    pub fn hit_history(&self, cache: &str) -> Option<&Ring<RING_LEN>> {
        self.hit_history.get(cache)
    }

    /// The frames waiting in the fan-out backlog of `cache`, one sample per
    /// fold. A cache that has no backlog series has no history.
    #[must_use]
    pub fn backlog_history(&self, cache: &str) -> Option<&Ring<RING_LEN>> {
        self.backlog_history.get(cache)
    }
}

/// A frame count from a counter rise. The rise is a whole number far below
/// 2^53, so rounding it is exact.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a counter rise is a whole, positive number far below 2^53"
)]
fn whole_frames(rise: f64) -> u64 {
    rise.round().max(0.0) as u64
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::source::expo;

    const FIXTURE: &str = include_str!("../../tests/fixtures/metrics.prom");

    fn node() -> NodeId {
        NodeId::from(0x1001)
    }

    fn sample(name: &str, labels: &[(&str, &str)], value: f64) -> Sample {
        Sample {
            name: name.to_owned(),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            value,
        }
    }

    fn at(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    #[test]
    fn a_series_key_sorts_labels_and_matches_by_name_and_label() {
        let scraped = sample(
            names::REBALANCE_PARTS,
            &[("direction", "in"), ("cache", "it")],
            5.0,
        );
        let key = SeriesKey::of(&scraped);
        assert_eq!(key.name(), names::REBALANCE_PARTS);
        assert_eq!(key.label("cache"), Some("it"));
        assert_eq!(key.label("peer"), None);
        assert_eq!(
            key.labels(),
            [
                ("cache".to_owned(), "it".to_owned()),
                ("direction".to_owned(), "in".to_owned())
            ]
        );
        assert!(key.matches(names::REBALANCE_PARTS, &[]));
        assert!(key.matches(names::REBALANCE_PARTS, &[("direction", "in")]));
        assert!(!key.matches(names::REBALANCE_PARTS, &[("direction", "out")]));
        assert!(!key.matches(names::FETCH, &[]));
        let swapped = SeriesKey::of(&sample(
            names::REBALANCE_PARTS,
            &[("cache", "it"), ("direction", "in")],
            9.0,
        ));
        assert_eq!(key, swapped, "label order does not distinguish a series");
    }

    #[test]
    fn counters_are_the_total_suffixed_names() {
        assert!(is_counter(names::CACHE_HITS));
        assert!(is_counter(names::FRAMES_SENT));
        assert!(!is_counter(names::OWNED_PARTS));
        assert!(!is_counter(names::LIVE_PEERS));
    }

    #[test]
    fn the_first_fold_holds_values_and_no_rates() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        assert_eq!(metrics.node(), node());
        assert_eq!(metrics.folds(), 0);
        let folded = metrics.fold(base, &expo::parse(FIXTURE));
        assert_eq!(folded, Folded::default());
        assert_eq!(metrics.folds(), 1);
        assert_eq!(metrics.owned_parts("it"), Some(43_616.0));
        assert_eq!(metrics.live_peers(), Some(2.0));
        assert_eq!(metrics.entries("it"), Some(2040.0));
        assert_eq!(metrics.entries("nope"), None);
        assert_eq!(
            metrics.cache_value(names::CACHE_ENTRIES, "it"),
            Some(2040.0)
        );
        assert_eq!(metrics.cache_value(names::CACHE_ENTRIES, "nope"), None);
        assert_eq!(metrics.cache_value(names::LIVE_PEERS, "it"), None);
        assert_eq!(metrics.rate_sum(names::CACHE_HITS), None);
        assert!(metrics.ops().is_empty());
        assert_eq!(metrics.hit_ratio("it"), None);
        assert_eq!(metrics.samples().count(), expo::parse(FIXTURE).len());
    }

    #[test]
    fn a_second_fold_turns_counters_into_rates_and_pushes_the_histories() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        metrics.fold(
            base,
            &[
                sample(names::CACHE_HITS, &[("cache", "it")], 100.0),
                sample(names::CACHE_MISSES, &[("cache", "it")], 100.0),
                sample(names::BYTES_SENT, &[], 1000.0),
                sample(names::FRAMES_SENT, &[], 10.0),
                sample(names::OWNED_PARTS, &[("cache", "it")], 100.0),
                sample(names::FETCH, &[("cache", "it"), ("outcome", "local")], 0.0),
                sample(names::FETCH, &[("cache", "it"), ("outcome", "remote")], 0.0),
                sample(names::FETCH, &[("cache", "it"), ("outcome", "miss")], 0.0),
            ],
        );
        metrics.fold(
            at(base, 2),
            &[
                sample(names::CACHE_HITS, &[("cache", "it")], 160.0),
                sample(names::CACHE_MISSES, &[("cache", "it")], 120.0),
                sample(names::BYTES_SENT, &[], 5000.0),
                sample(names::FRAMES_SENT, &[], 30.0),
                sample(names::OWNED_PARTS, &[("cache", "it")], 100.0),
                sample(
                    names::FETCH,
                    &[("cache", "it"), ("outcome", "local")],
                    200.0,
                ),
                sample(
                    names::FETCH,
                    &[("cache", "it"), ("outcome", "remote")],
                    10.0,
                ),
                sample(names::FETCH, &[("cache", "it"), ("outcome", "miss")], 4.0),
            ],
        );
        let rate = |name| metrics.rate(name, &[("cache", "it")]).unwrap();
        assert!((rate(names::CACHE_HITS) - 30.0).abs() < 1e-9);
        assert!((rate(names::CACHE_MISSES) - 10.0).abs() < 1e-9);
        assert!((metrics.hit_ratio("it").unwrap() - 0.75).abs() < 1e-9);
        assert!((metrics.rate_sum(names::BYTES_SENT).unwrap() - 2000.0).abs() < 1e-9);
        // 30 hits and 10 misses a second, and 5 remote and 2 missed fetches a
        // second; the 100 local fetches a second are the hits and misses
        // already.
        assert_eq!(metrics.ops().to_vec(), [47.0]);
        assert_eq!(metrics.tx_bytes().to_vec(), [2000.0]);
        assert_eq!(metrics.tx_frames().to_vec(), [10.0]);
        assert_eq!(
            metrics.rate(names::OWNED_PARTS, &[]),
            None,
            "a gauge has no rate"
        );
    }

    #[test]
    fn a_counter_reset_has_no_rate_and_a_gauge_is_replaced() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        metrics.fold(base, &[sample(names::BYTES_SENT, &[], 9000.0)]);
        metrics.fold(at(base, 1), &[sample(names::BYTES_SENT, &[], 100.0)]);
        assert_eq!(metrics.rate_sum(names::BYTES_SENT), None);
        assert_eq!(metrics.value(names::BYTES_SENT, &[]), Some(100.0));
        metrics.fold(at(base, 2), &[sample(names::BYTES_SENT, &[], 300.0)]);
        assert!((metrics.rate_sum(names::BYTES_SENT).unwrap() - 200.0).abs() < 1e-9);
    }

    #[test]
    fn a_non_finite_counter_value_gets_no_rate() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        metrics.fold(base, &[sample(names::BYTES_SENT, &[], 10.0)]);
        metrics.fold(at(base, 1), &[sample(names::BYTES_SENT, &[], f64::NAN)]);
        assert_eq!(metrics.rate_sum(names::BYTES_SENT), None);
        metrics.fold(at(base, 3), &[sample(names::BYTES_SENT, &[], 40.0)]);
        assert!(
            (metrics.rate_sum(names::BYTES_SENT).unwrap() - 10.0).abs() < 1e-9,
            "the rate spans back to the last finite sample"
        );
    }

    #[test]
    fn the_fetch_mix_reads_each_outcome_and_missing_ones_count_zero() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let fetch = |local, remote| {
            vec![
                sample(
                    names::FETCH,
                    &[("cache", "it"), ("outcome", "local")],
                    local,
                ),
                sample(
                    names::FETCH,
                    &[("cache", "it"), ("outcome", "remote")],
                    remote,
                ),
            ]
        };
        metrics.fold(base, &fetch(0.0, 0.0));
        assert_eq!(metrics.fetch_mix("it"), None, "no rates yet");
        metrics.fold(at(base, 1), &fetch(30.0, 10.0));
        let mix = metrics.fetch_mix("it").unwrap();
        assert!((mix.local - 0.75).abs() < 1e-9);
        assert!((mix.remote - 0.25).abs() < 1e-9);
        assert!(mix.miss.abs() < 1e-9 && mix.error.abs() < 1e-9);
        let local_filter = [("cache", "it"), ("outcome", "local")];
        assert!((metrics.rate_sum_where(names::FETCH, &local_filter).unwrap() - 30.0).abs() < 1e-9);
        assert!(
            (metrics
                .rate_sum_where(names::FETCH, &[("cache", "it")])
                .unwrap()
                - 40.0)
                .abs()
                < 1e-9
        );
        assert_eq!(
            metrics.rate_sum_where(names::FETCH, &[("cache", "other")]),
            None
        );
        metrics.fold(at(base, 2), &fetch(30.0, 10.0));
        assert_eq!(metrics.fetch_mix("it"), None, "zero traffic has no mix");
        assert_eq!(metrics.fetch_mix("other"), None);
    }

    #[test]
    fn dropped_frames_are_counted_by_rise_and_a_new_series_counts_whole() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let dropped = |peer: &str, value| sample(names::BACKLOG_DROPPED, &[("peer", peer)], value);
        let folded = metrics.fold(base, &[dropped("aa", 40.0)]);
        assert!(
            folded.drops.is_empty(),
            "the first fold only sets the baseline"
        );
        let folded = metrics.fold(at(base, 1), &[dropped("aa", 40.0)]);
        assert!(folded.drops.is_empty(), "no rise");
        let folded = metrics.fold(at(base, 2), &[dropped("aa", 352.0), dropped("bb", 7.0)]);
        assert_eq!(
            folded.drops,
            [
                DropEdge {
                    peer: "aa".into(),
                    frames: 312
                },
                DropEdge {
                    peer: "bb".into(),
                    frames: 7
                },
            ]
        );
        let folded = metrics.fold(at(base, 3), &[dropped("aa", 5.0), dropped("bb", 7.0)]);
        assert!(folded.drops.is_empty(), "a fall is a counter reset");
        let folded = metrics.fold(at(base, 4), &[dropped("aa", 9.0), dropped("bb", 7.0)]);
        assert_eq!(
            folded.drops,
            [DropEdge {
                peer: "aa".into(),
                frames: 4
            }]
        );
    }

    #[test]
    fn a_series_with_zero_frames_that_appears_later_is_not_a_drop() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        metrics.fold(base, &[sample(names::LIVE_PEERS, &[], 2.0)]);
        let folded = metrics.fold(
            at(base, 1),
            &[sample(names::BACKLOG_DROPPED, &[("peer", "aa")], 0.0)],
        );
        assert!(folded.drops.is_empty(), "{:?}", folded.drops);
    }

    #[test]
    fn the_state_transfer_rate_starts_once_until_it_stops() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let xfer = |value| {
            [sample(
                names::STATE_TRANSFER_RECORDS,
                &[("cache", "it")],
                value,
            )]
        };
        assert!(!metrics.fold(base, &xfer(0.0)).xfer_started);
        assert!(!metrics.fold(at(base, 1), &xfer(0.0)).xfer_started);
        assert!(metrics.fold(at(base, 2), &xfer(500.0)).xfer_started);
        assert!(
            !metrics.fold(at(base, 3), &xfer(900.0)).xfer_started,
            "a running transfer is not a new one"
        );
        assert!(!metrics.fold(at(base, 4), &xfer(900.0)).xfer_started);
        assert!(metrics.fold(at(base, 5), &xfer(1200.0)).xfer_started);
    }

    #[test]
    fn quiet_scrapes_count_folds_without_parts_pulled_in() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let parts = |pulled: Option<f64>| {
            let mut samples = vec![sample(names::OWNED_PARTS, &[("cache", "it")], 100.0)];
            if let Some(value) = pulled {
                samples.push(sample(
                    names::REBALANCE_PARTS,
                    &[("cache", "it"), ("direction", "in")],
                    value,
                ));
            }
            samples
        };
        metrics.fold(base, &parts(Some(0.0)));
        assert_eq!(metrics.quiet_scrapes("it"), 0, "no rate on the first fold");
        metrics.fold(at(base, 1), &parts(Some(500.0)));
        assert_eq!(metrics.quiet_scrapes("it"), 0, "parts came in");
        metrics.fold(at(base, 2), &parts(Some(500.0)));
        assert_eq!(metrics.quiet_scrapes("it"), 1);
        metrics.fold(at(base, 3), &parts(Some(500.0)));
        assert_eq!(metrics.quiet_scrapes("it"), 2);
        metrics.fold(at(base, 4), &parts(Some(900.0)));
        assert_eq!(metrics.quiet_scrapes("it"), 0, "pulling again");
        assert_eq!(metrics.quiet_scrapes("other"), 0);
    }

    #[test]
    fn an_absent_pull_series_counts_as_quiet_after_the_first_fold() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let owned = [sample(names::OWNED_PARTS, &[("cache", "it")], 100.0)];
        metrics.fold(base, &owned);
        assert_eq!(metrics.quiet_scrapes("it"), 0);
        metrics.fold(at(base, 1), &owned);
        assert_eq!(metrics.quiet_scrapes("it"), 1);
        metrics.fold(at(base, 2), &owned);
        assert_eq!(metrics.quiet_scrapes("it"), 2);
    }

    #[test]
    fn a_pull_series_that_appears_with_parts_is_a_pull() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let owned = sample(names::OWNED_PARTS, &[("cache", "it")], 100.0);
        for secs in 0..3 {
            metrics.fold(at(base, secs), std::slice::from_ref(&owned));
        }
        assert_eq!(metrics.quiet_scrapes("it"), 2);
        // The exporter creates the series at the first pull: 5000 parts came
        // in during the last second.
        let pulled = sample(
            names::REBALANCE_PARTS,
            &[("cache", "it"), ("direction", "in")],
            5000.0,
        );
        metrics.fold(at(base, 3), &[owned.clone(), pulled.clone()]);
        assert_eq!(
            metrics.quiet_scrapes("it"),
            0,
            "a node that pulled is not quiet"
        );
        assert_eq!(
            metrics.rebalance_in("it").unwrap().to_vec(),
            [0.0, 0.0, 5000.0]
        );
        // No more parts: the series holds still and the quiet run starts over.
        metrics.fold(at(base, 4), &[owned.clone(), pulled.clone()]);
        assert_eq!(metrics.quiet_scrapes("it"), 1);
        assert_eq!(
            metrics.rebalance_in("it").unwrap().to_vec(),
            [0.0, 0.0, 5000.0, 0.0]
        );
    }

    #[test]
    fn a_pull_series_that_appears_empty_is_quiet_and_one_in_the_first_fold_is_a_baseline() {
        let base = Instant::now();
        let owned = sample(names::OWNED_PARTS, &[("cache", "it")], 100.0);
        let pulled = |value| {
            sample(
                names::REBALANCE_PARTS,
                &[("cache", "it"), ("direction", "in")],
                value,
            )
        };
        let mut metrics = NodeMetrics::new(node());
        metrics.fold(base, std::slice::from_ref(&owned));
        metrics.fold(at(base, 1), &[owned.clone(), pulled(0.0)]);
        assert_eq!(metrics.quiet_scrapes("it"), 1);
        let mut baseline = NodeMetrics::new(node());
        baseline.fold(base, &[owned.clone(), pulled(9000.0)]);
        baseline.fold(at(base, 1), &[owned, pulled(9000.0)]);
        assert_eq!(
            baseline.quiet_scrapes("it"),
            1,
            "the first fold sets the baseline"
        );
    }

    #[test]
    fn quiet_scrapes_since_counts_only_folds_taken_inside_the_view() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let owned = [sample(names::OWNED_PARTS, &[("cache", "it")], 100.0)];
        for secs in 0..=5 {
            metrics.fold(at(base, secs), &owned);
        }
        assert_eq!(metrics.quiet_scrapes("it"), 5);
        // A view that began at 4.5 s: the fold at 5 s, whose rate window
        // began at 4 s, is not inside it.
        let since = at(base, 4) + Duration::from_millis(500);
        assert_eq!(metrics.quiet_scrapes_since("it", since), 0);
        metrics.fold(at(base, 6), &owned);
        assert_eq!(metrics.quiet_scrapes_since("it", since), 1);
        metrics.fold(at(base, 7), &owned);
        assert_eq!(metrics.quiet_scrapes_since("it", since), 2);
        metrics.fold(at(base, 8), &owned);
        assert_eq!(
            metrics.quiet_scrapes_since("it", since),
            QUIET_SCRAPES,
            "it counts no more than the scrapes settling needs"
        );
        assert_eq!(
            metrics.quiet_scrapes_since("it", at(base, 0)),
            QUIET_SCRAPES
        );
        assert_eq!(metrics.quiet_scrapes_since("other", since), 0);
        // A pull ends the run.
        let pulled = sample(
            names::REBALANCE_PARTS,
            &[("cache", "it"), ("direction", "in")],
            50.0,
        );
        metrics.fold(at(base, 9), &[owned[0].clone(), pulled]);
        assert_eq!(metrics.quiet_scrapes_since("it", since), 0);
    }

    #[test]
    fn rebalance_histories_hold_the_in_and_out_rates() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let parts = |into, out| {
            vec![
                sample(names::OWNED_PARTS, &[("cache", "it")], 100.0),
                sample(
                    names::REBALANCE_PARTS,
                    &[("cache", "it"), ("direction", "in")],
                    into,
                ),
                sample(
                    names::REBALANCE_PARTS,
                    &[("cache", "it"), ("direction", "out")],
                    out,
                ),
            ]
        };
        metrics.fold(base, &parts(0.0, 0.0));
        assert!(metrics.rebalance_in("it").is_none());
        metrics.fold(at(base, 2), &parts(400.0, 100.0));
        assert_eq!(metrics.rebalance_in("it").unwrap().to_vec(), [200.0]);
        assert_eq!(metrics.rebalance_out("it").unwrap().to_vec(), [50.0]);
        assert!(metrics.rebalance_in("other").is_none());
    }

    #[test]
    fn cache_histories_hold_entries_backlog_and_hit_percentage() {
        let base = Instant::now();
        let mut metrics = NodeMetrics::new(node());
        let round = |entries: f64, hits: f64, misses: f64, backlog: f64| {
            vec![
                sample(names::CACHE_ENTRIES, &[("cache", "side")], entries),
                sample(names::CACHE_HITS, &[("cache", "side")], hits),
                sample(names::CACHE_MISSES, &[("cache", "side")], misses),
                sample(names::FAN_OUT_BACKLOG, &[("cache", "side")], backlog),
                sample(names::CACHE_ENTRIES, &[("cache", "bare")], 7.0),
            ]
        };
        metrics.fold(base, &round(100.0, 0.0, 0.0, 4.0));
        assert_eq!(metrics.entries_history("side").unwrap().to_vec(), [100.0]);
        assert_eq!(metrics.backlog_history("side").unwrap().to_vec(), [4.0]);
        assert!(metrics.hit_history("side").is_none(), "needs two folds");
        metrics.fold(at(base, 1), &round(150.0, 90.0, 10.0, 2.0));
        metrics.fold(at(base, 2), &round(160.0, 90.0, 10.0, 0.0));
        assert_eq!(
            metrics.entries_history("side").unwrap().to_vec(),
            [100.0, 150.0, 160.0]
        );
        assert_eq!(
            metrics.backlog_history("side").unwrap().to_vec(),
            [4.0, 2.0, 0.0]
        );
        // 90 hits and 10 misses in the second round, nothing in the third.
        assert_eq!(metrics.hit_history("side").unwrap().to_vec(), [90.0, 0.0]);
        assert_eq!(metrics.entries_history("bare").unwrap().len(), 3);
        assert!(metrics.backlog_history("bare").is_none());
        assert!(metrics.entries_history("nothing").is_none());
        assert!(metrics.hit_history("nothing").is_none());
    }

    #[test]
    fn the_real_capture_folds_twice_into_zero_rates() {
        let base = Instant::now();
        let samples = expo::parse(FIXTURE);
        let mut metrics = NodeMetrics::new(node());
        metrics.fold(base, &samples);
        metrics.fold(at(base, 1), &samples);
        assert_eq!(metrics.rate_sum(names::FRAMES_SENT), Some(0.0));
        assert_eq!(metrics.hit_ratio("it"), None, "a quiet node has no ratio");
        assert_eq!(metrics.quiet_scrapes("it"), 1);
        assert_eq!(metrics.ops().to_vec(), [0.0]);
    }

    #[test]
    fn whole_frames_rounds_and_never_goes_negative() {
        assert_eq!(whole_frames(312.0), 312);
        assert_eq!(whole_frames(311.6), 312);
        assert_eq!(whole_frames(-4.0), 0);
    }
}
