//! The state of one node's exporter as the scrapes show it: whether it
//! answers, whether the node is ready, and whether the URL maps to the right
//! node.

use sundog::NodeId;

use super::events::EventKind;
use crate::source::scrape::ScrapeReport;

/// Consecutive failed scrapes after which an exporter counts as not answering.
pub const FAILURES_UNREACHABLE: u32 = 2;

/// The detail of the `EXPORTER` event raised when an exporter first answers.
pub const ANSWERING: &str = "answering";

/// The detail of the `EXPORTER` event raised when an exporter answers again
/// after it stopped.
pub const ANSWERING_AGAIN: &str = "answering again";

/// Whether the `EXPORTER` event `detail` reports a healthy exporter.
#[must_use]
pub fn is_answering(detail: &str) -> bool {
    detail == ANSWERING || detail == ANSWERING_AGAIN
}

/// One node's exporter, as the scrapes of that node show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExporterState {
    node: NodeId,
    ever_answered: bool,
    failures: u32,
    ready: Option<bool>,
    mapping: Option<String>,
    mismatch: bool,
}

impl ExporterState {
    /// The state of the exporter of `node`, before any scrape.
    #[must_use]
    pub const fn new(node: NodeId) -> Self {
        Self {
            node,
            ever_answered: false,
            failures: 0,
            ready: None,
            mapping: None,
            mismatch: false,
        }
    }

    /// The node the exporter is mapped to.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        self.node
    }

    /// Consecutive failed scrapes; 0 after a scrape that answered.
    #[must_use]
    pub const fn failures(&self) -> u32 {
        self.failures
    }

    /// Whether the exporter has failed [`FAILURES_UNREACHABLE`] scrapes in a
    /// row.
    #[must_use]
    pub const fn unreachable(&self) -> bool {
        self.failures >= FAILURES_UNREACHABLE
    }

    /// The last `/readyz` verdict; `None` before the first answer.
    #[must_use]
    pub const fn ready(&self) -> Option<bool> {
        self.ready
    }

    /// Why the member has no exporter to scrape (a URL shared with another
    /// member, or a template that does not expand), when that is so.
    #[must_use]
    pub fn mapping_error(&self) -> Option<&str> {
        self.mapping.as_deref()
    }

    /// Whether the exporter reports the node's own id as a peer: the URL maps
    /// to another node's exporter (the `⚠ map` badge).
    #[must_use]
    pub const fn mismatch(&self) -> bool {
        self.mismatch
    }

    /// Folds one scrape report in and returns the events it raises:
    ///
    /// - `READY` or `UNREADY` when `/readyz` flips;
    /// - `UNREACHABLE` at the second failed scrape in a row when gossip still
    ///   lists the node `Live` (`listed_live`), `EXPORTER` when it does not,
    ///   a departing node included;
    /// - `EXPORTER` at the first answer, at the first answer after
    ///   unreachability, when a mapping error appears and when a node's own id
    ///   shows up as a peer label.
    ///
    /// A mapping error is no failed scrape: it leaves the failure count alone.
    pub fn observe(&mut self, report: &ScrapeReport, listed_live: bool) -> Vec<EventKind> {
        let (addr, node) = (report.addr, report.node);
        let mut kinds = Vec::new();
        match &report.outcome {
            Err(error) if error.is_mapping() => {
                let message = error.to_string();
                if self.mapping.as_deref() != Some(message.as_str()) {
                    kinds.push(EventKind::Exporter {
                        node,
                        addr,
                        detail: format!("not scraped: {message}"),
                    });
                    self.mapping = Some(message);
                }
                return kinds;
            }
            Err(error) => {
                self.mapping = None;
                self.failures += 1;
                if self.failures == FAILURES_UNREACHABLE {
                    kinds.push(if listed_live {
                        EventKind::Unreachable { node }
                    } else {
                        EventKind::Exporter {
                            node,
                            addr,
                            detail: format!("not answering: {error}"),
                        }
                    });
                }
            }
            Ok(samples) => {
                self.mapping = None;
                let again = self.unreachable();
                self.failures = 0;
                if !self.ever_answered || again {
                    kinds.push(EventKind::Exporter {
                        node,
                        addr,
                        detail: if again {
                            ANSWERING_AGAIN.to_owned()
                        } else {
                            ANSWERING.to_owned()
                        },
                    });
                }
                self.ever_answered = true;
                let own = node.to_string();
                let mismatch = samples
                    .iter()
                    .any(|sample| sample.label("peer") == Some(own.as_str()));
                if mismatch && !self.mismatch {
                    kinds.push(EventKind::Exporter {
                        node,
                        addr,
                        detail: format!(
                            "reports {own} as a peer, which is the node it is mapped to: \
                             the URL maps to the wrong exporter"
                        ),
                    });
                }
                self.mismatch = mismatch;
            }
        }
        if let Some(ready) = report.ready {
            if self.ready.is_some_and(|before| before != ready) {
                kinds.push(if ready {
                    EventKind::Ready { node }
                } else {
                    EventKind::Unready { node }
                });
            }
            self.ready = Some(ready);
        }
        kinds
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::model::testkit;
    use crate::source::expo::Sample;
    use crate::source::scrape::ScrapeError;

    fn report(outcome: Result<Vec<Sample>, ScrapeError>, ready: Option<bool>) -> ScrapeReport {
        ScrapeReport {
            addr: testkit::gossip_addr(1),
            node: testkit::node_id(1, 0),
            at: Instant::now(),
            outcome,
            ready,
        }
    }

    fn answering(ready: Option<bool>) -> ScrapeReport {
        report(Ok(Vec::new()), ready)
    }

    fn failing() -> ScrapeReport {
        report(Err(ScrapeError::Timeout), None)
    }

    fn peer_sample(peer: &str) -> Sample {
        Sample {
            name: "sundog_backlog_dropped_total".to_owned(),
            labels: vec![("peer".to_owned(), peer.to_owned())],
            value: 1.0,
        }
    }

    #[test]
    fn only_the_answering_details_report_a_healthy_exporter() {
        assert!(is_answering(ANSWERING));
        assert!(is_answering(ANSWERING_AGAIN));
        assert!(!is_answering("unreachable"));
        assert!(!is_answering(""));
    }

    fn state() -> ExporterState {
        ExporterState::new(testkit::node_id(1, 0))
    }

    fn tags(kinds: &[EventKind]) -> Vec<&'static str> {
        kinds.iter().map(EventKind::tag).collect()
    }

    #[test]
    fn a_new_state_knows_nothing() {
        let state = state();
        assert_eq!(state.node(), testkit::node_id(1, 0));
        assert_eq!(state.failures(), 0);
        assert!(!state.unreachable());
        assert_eq!(state.ready(), None);
        assert_eq!(state.mapping_error(), None);
        assert!(!state.mismatch());
    }

    #[test]
    fn the_first_answer_raises_one_exporter_event() {
        let mut state = state();
        assert_eq!(tags(&state.observe(&answering(None), true)), ["EXPORTER"]);
        let repeat_kinds = state.observe(&answering(None), true);
        assert!(repeat_kinds.is_empty(), "{repeat_kinds:?}");
    }

    #[test]
    fn readiness_raises_an_event_only_when_it_flips() {
        let mut state = state();
        state.observe(&answering(Some(false)), true);
        assert_eq!(state.ready(), Some(false));
        let kinds = state.observe(&answering(Some(true)), true);
        assert_eq!(tags(&kinds), ["READY"]);
        let repeat_kinds = state.observe(&answering(Some(true)), true);
        assert!(repeat_kinds.is_empty(), "{repeat_kinds:?}");
        let unprobed_kinds = state.observe(&answering(None), true);
        assert!(unprobed_kinds.is_empty(), "{unprobed_kinds:?}");
        assert_eq!(
            state.ready(),
            Some(true),
            "an unprobed round keeps the verdict"
        );
        let kinds = state.observe(&answering(Some(false)), true);
        assert_eq!(tags(&kinds), ["UNREADY"]);
    }

    #[test]
    fn the_first_readiness_verdict_raises_nothing() {
        let mut state = state();
        state.observe(&answering(None), true);
        let kinds = state.observe(&answering(Some(true)), true);
        assert!(kinds.is_empty(), "{kinds:?}");
    }

    #[test]
    fn readiness_flips_are_seen_while_metrics_fail() {
        let mut state = state();
        state.observe(&answering(Some(true)), true);
        let mut failed = failing();
        failed.ready = Some(false);
        assert_eq!(tags(&state.observe(&failed, true)), ["UNREADY"]);
    }

    #[test]
    fn two_failures_in_a_row_raise_unreachable_once_while_gossip_lists_the_node_live() {
        let mut state = state();
        state.observe(&answering(None), true);
        let first_failure_kinds = state.observe(&failing(), true);
        assert!(first_failure_kinds.is_empty(), "{first_failure_kinds:?}");
        assert_eq!(state.failures(), 1);
        assert!(!state.unreachable());
        let kinds = state.observe(&failing(), true);
        assert_eq!(
            kinds,
            [EventKind::Unreachable {
                node: testkit::node_id(1, 0)
            }]
        );
        assert!(state.unreachable());
        assert!(
            state.observe(&failing(), true).is_empty(),
            "once per streak"
        );
        assert_eq!(state.failures(), 3);
    }

    #[test]
    fn two_failures_of_a_node_gossip_no_longer_lists_live_raise_an_exporter_event() {
        let mut state = state();
        state.observe(&failing(), false);
        let kinds = state.observe(&failing(), false);
        assert_eq!(tags(&kinds), ["EXPORTER"]);
        let EventKind::Exporter { detail, .. } = &kinds[0] else {
            panic!("an exporter event");
        };
        assert_eq!(detail, "not answering: timed out");
    }

    #[test]
    fn an_answer_after_unreachability_raises_answering_again_and_resets_the_count() {
        let mut state = state();
        state.observe(&answering(None), true);
        state.observe(&failing(), true);
        state.observe(&failing(), true);
        let kinds = state.observe(&answering(None), true);
        let [EventKind::Exporter { detail, .. }] = kinds.as_slice() else {
            panic!("one exporter event, got {kinds:?}");
        };
        assert_eq!(detail, "answering again");
        assert_eq!(state.failures(), 0);
        state.observe(&failing(), true);
        assert!(
            state.observe(&answering(None), true).is_empty(),
            "one blip is no outage"
        );
        state.observe(&failing(), true);
        let kinds = state.observe(&failing(), true);
        assert_eq!(tags(&kinds), ["UNREACHABLE"], "a new streak raises again");
    }

    #[test]
    fn a_mapping_error_is_reported_once_and_is_no_failed_scrape() {
        let mut state = state();
        let collision = report(Err(ScrapeError::Collision("http://h/metrics".into())), None);
        let kinds = state.observe(&collision, true);
        let [EventKind::Exporter { detail, .. }] = kinds.as_slice() else {
            panic!("one exporter event, got {kinds:?}");
        };
        assert!(detail.starts_with("not scraped: "), "{detail}");
        assert!(detail.contains("http://h/metrics"));
        assert_eq!(state.failures(), 0);
        assert!(state.mapping_error().is_some());
        let kinds = state.observe(&collision, true);
        assert!(kinds.is_empty(), "{kinds:?}");
        let other = report(Err(ScrapeError::Template("port".into())), None);
        assert_eq!(tags(&state.observe(&other, true)), ["EXPORTER"]);
        state.observe(&answering(None), true);
        assert_eq!(state.mapping_error(), None);
    }

    #[test]
    fn a_peer_label_equal_to_the_nodes_own_id_flags_a_mismapped_exporter() {
        let own = testkit::node_id(1, 0).to_string();
        let mut state = state();
        state.observe(&answering(None), true);
        let mismapped = report(Ok(vec![peer_sample(&own)]), None);
        let kinds = state.observe(&mismapped, true);
        let [EventKind::Exporter { detail, .. }] = kinds.as_slice() else {
            panic!("one exporter event, got {kinds:?}");
        };
        assert!(
            detail.contains(&own) && detail.contains("wrong exporter"),
            "{detail}"
        );
        assert!(state.mismatch());
        assert!(state.observe(&mismapped, true).is_empty(), "raised once");
        let fine = report(Ok(vec![peer_sample("00000000000000aa")]), None);
        let kinds = state.observe(&fine, true);
        assert!(kinds.is_empty(), "{kinds:?}");
        assert!(!state.mismatch());
    }
}
