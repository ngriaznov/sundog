//! Asking test nodes about a key.
//!
//! [`ask_all`] sends `explain <key>` to the control port of every [`Target`]
//! at once and returns one [`NodeAnswer`] per target, in the order the
//! targets were given. A node that does not answer keeps its place with the
//! reason in words. [`outcome_of`] holds every decision: it turns a reply, or
//! the way a request failed, into an [`Outcome`]. [`answer`] runs an
//! [`ExplainRequest`] and wraps the answers in the [`UiCommand`] that carries
//! them back to the interface.
//!
//! The lens dials control ports only, the channel the fleet already uses for
//! `fill` and `crash`. It opens no connection to a data port.

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use futures::future::join_all;
use smol_str::SmolStr;

use crate::app::{ExplainRequest, UiCommand};
use crate::control;
use crate::explained::{self, Explained, NodeAnswer, Outcome};
use crate::key::printable;
use crate::ui::text;

/// How long a node has to answer. It exceeds a node's fetch timeout, which
/// bounds each probe the node makes before it replies, and the dial.
pub const TIMEOUT: Duration = Duration::from_secs(3);

/// What the interface says of a test node that predates the `explain` line:
/// the remedy for the person running the fleet.
pub const PREDATES: &str =
    "this test node predates explain; rebuild sundog-testnode and restart the fleet";

/// How many characters of a node's `err` line the interface shows.
const REPLY_CHARS: usize = 60;

/// One test node to ask: its slot label and control address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The node's slot label: `n1`.
    pub label: SmolStr,
    /// The node's control address.
    pub addr: SocketAddr,
}

/// What the answer of one node to `explain` comes to: its reading, or the
/// reason in words that it gave none.
///
/// `deadline` is the time the request was allowed, which a timeout names.
#[must_use]
pub fn outcome_of(reply: io::Result<String>, deadline: Duration) -> Outcome {
    match reply {
        Ok(line) => match explained::parse_reply(&line) {
            Ok(reading) => Outcome::Read(Box::new(reading)),
            Err(error) => Outcome::Failed(error.to_string()),
        },
        Err(error) => Outcome::Failed(failure_text(&error, deadline)),
    }
}

/// The words for a request that failed.
fn failure_text(error: &io::Error, deadline: Duration) -> String {
    let text = error.to_string();
    if text.contains("unknown command") {
        return PREDATES.to_owned();
    }
    match error.kind() {
        io::ErrorKind::TimedOut => format!("no answer within {}", span_text(deadline)),
        // `control::request` makes an `Other` error of an `err` reply.
        io::ErrorKind::Other => {
            let reply = text
                .split_once("`: ")
                .map_or(text.as_str(), |(_, reply)| reply);
            format!(
                "the node replied: {}",
                text::fit(&printable(reply), REPLY_CHARS)
            )
        }
        kind => format!("no answer ({kind})"),
    }
}

/// A span as whole seconds when it is, else as milliseconds: `3 s`, `200 ms`.
fn span_text(span: Duration) -> String {
    if span.subsec_millis() == 0 && span.as_secs() > 0 {
        format!("{} s", span.as_secs())
    } else {
        format!("{} ms", span.as_millis())
    }
}

/// Asks every target about `key` at once, each allowed `deadline`. The
/// answers keep the order of `targets`, whatever order the nodes reply in.
pub async fn ask_all(id: u64, key: &str, targets: &[Target], deadline: Duration) -> Explained {
    let line = format!("explain {key}");
    let asked = SystemTime::now();
    let nodes = join_all(targets.iter().map(|target| async {
        let reply = control::request(target.addr, &line, deadline).await;
        NodeAnswer {
            label: target.label.clone(),
            outcome: outcome_of(reply, deadline),
        }
    }))
    .await;
    Explained {
        id,
        key: key.to_owned(),
        asked,
        nodes,
    }
}

/// Runs `request` with [`TIMEOUT`] and returns the answers as the command
/// that hands them to the interface.
pub async fn answer(request: ExplainRequest) -> UiCommand {
    UiCommand::Explained(Box::new(
        ask_all(request.id, &request.key, &request.targets, TIMEOUT).await,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::net::TcpListener;
    use tokio::sync::Barrier;

    use super::*;
    use crate::model::testkit;

    const OWNER_LINE: &str = include_str!("../tests/fixtures/explain/owner.json");
    const NON_OWNER_LINE: &str = include_str!("../tests/fixtures/explain/non_owner.json");
    const CRASHED_OWNER_LINE: &str = include_str!("../tests/fixtures/explain/crashed_owner.json");

    /// The control lines a fake node has read.
    type Seen = Arc<Mutex<Vec<String>>>;

    /// A control server that answers every line it reads with `reply`. When
    /// it has a `gate`, it answers only after every party of the gate has
    /// read a line.
    async fn node(reply: &'static str, gate: Option<Arc<Barrier>>) -> (SocketAddr, Seen) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Seen::default();
        let log = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let log = Arc::clone(&log);
                let gate = gate.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        log.lock().unwrap().push(line);
                        if let Some(gate) = &gate {
                            gate.wait().await;
                        }
                        if writer
                            .write_all(format!("{reply}\n").as_bytes())
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });
        (addr, seen)
    }

    fn target(label: &str, addr: SocketAddr) -> Target {
        Target {
            label: label.into(),
            addr,
        }
    }

    fn failure(outcome: &Outcome) -> &str {
        match outcome {
            Outcome::Failed(why) => why,
            Outcome::Read(reading) => panic!("expected a failure, read {reading:?}"),
        }
    }

    /// The outcome of a node that answered with `line`.
    fn reading(line: &str) -> Outcome {
        Outcome::Read(Box::new(explained::parse_reply(line).unwrap()))
    }

    fn read_node(outcome: &Outcome) -> &str {
        match outcome {
            Outcome::Read(reading) => &reading.node,
            Outcome::Failed(why) => panic!("expected a reading, failed: {why}"),
        }
    }

    #[test]
    fn outcome_of_gives_each_failure_its_words() {
        let deadline = TIMEOUT;
        let read = outcome_of(Ok(OWNER_LINE.trim().to_owned()), deadline);
        assert_eq!(read_node(&read), "6f3ac1e29d54b807");

        // `control::request` turns an `err` reply into this error.
        let unknown = io::Error::other("`explain k1`: err unknown command \"explain\"");
        assert_eq!(
            failure(&outcome_of(Err(unknown), deadline)),
            "this test node predates explain; rebuild sundog-testnode and restart the fleet"
        );
        let refused = io::Error::from(io::ErrorKind::ConnectionRefused);
        assert_eq!(
            failure(&outcome_of(Err(refused), deadline)),
            "no answer (connection refused)"
        );
        let slow = io::Error::new(io::ErrorKind::TimedOut, "`explain k1` timed out");
        assert_eq!(
            failure(&outcome_of(Err(slow), deadline)),
            "no answer within 3 s"
        );
        let quick = io::Error::from(io::ErrorKind::TimedOut);
        assert_eq!(
            failure(&outcome_of(Err(quick), Duration::from_millis(200))),
            "no answer within 200 ms"
        );
        let hung_up = io::Error::from(io::ErrorKind::UnexpectedEof);
        assert_eq!(
            failure(&outcome_of(Err(hung_up), deadline)),
            "no answer (unexpected end of file)"
        );
        let bad = outcome_of(Ok("not json at all".to_owned()), deadline);
        assert_eq!(
            failure(&bad),
            "reply is not an explanation: not json at all"
        );
    }

    #[test]
    fn a_node_that_replies_err_is_quoted_in_words_that_are_safe_to_draw() {
        let refused = io::Error::other("`explain k1`: err explain needs a key");
        assert_eq!(
            failure(&outcome_of(Err(refused), TIMEOUT)),
            "the node replied: err explain needs a key"
        );
        let hostile = io::Error::other(format!("`explain k1`: err \u{1b}[31m{}", "x".repeat(200)));
        let shown = failure(&outcome_of(Err(hostile), TIMEOUT)).to_owned();
        assert!(
            shown.starts_with("the node replied: err ·[31mxxx"),
            "{shown}"
        );
        assert!(shown.ends_with('…'), "{shown}");
        assert!(shown.chars().all(|c| !c.is_control()), "{shown}");
        assert!(shown.chars().count() < 100, "{shown}");
    }

    #[tokio::test]
    async fn ask_all_asks_every_target_at_once_and_keeps_slot_order() {
        // No node answers before all three hold the request, so a lens that
        // asked one node after another would never get an answer.
        let gate = Arc::new(Barrier::new(3));
        let (first, first_seen) = node(OWNER_LINE.trim(), Some(Arc::clone(&gate))).await;
        let (second, second_seen) = node(NON_OWNER_LINE.trim(), Some(Arc::clone(&gate))).await;
        let (third, third_seen) = node(CRASHED_OWNER_LINE.trim(), Some(gate)).await;
        let targets = [
            target("n1", first),
            target("n2", second),
            target("n3", third),
        ];

        let before = SystemTime::now();
        let explained = ask_all(41, "k17", &targets, Duration::from_secs(5)).await;
        let after = SystemTime::now();

        assert_eq!(explained.id, 41);
        assert_eq!(explained.key, "k17");
        assert!(before <= explained.asked && explained.asked <= after);
        let labels: Vec<_> = explained.nodes.iter().map(|n| n.label.as_str()).collect();
        assert_eq!(labels, ["n1", "n2", "n3"]);
        let outcomes: Vec<_> = explained
            .nodes
            .iter()
            .map(|answer| answer.outcome.clone())
            .collect();
        assert_eq!(
            outcomes,
            [OWNER_LINE, NON_OWNER_LINE, CRASHED_OWNER_LINE].map(reading)
        );
        for seen in [first_seen, second_seen, third_seen] {
            assert_eq!(seen.lock().unwrap().as_slice(), ["explain k17"]);
        }
    }

    #[tokio::test]
    async fn ask_all_reports_a_refused_node_and_the_others_still_answer() {
        let (live, _) = node(OWNER_LINE.trim(), None).await;
        let targets = [target("n1", testkit::refusing_addr()), target("n2", live)];
        let explained = ask_all(1, "k17", &targets, Duration::from_secs(5)).await;
        assert_eq!(explained.nodes.len(), 2);
        assert_eq!(explained.nodes[0].label, "n1");
        assert_eq!(
            failure(&explained.nodes[0].outcome),
            "no answer (connection refused)"
        );
        assert_eq!(explained.nodes[1].label, "n2");
        assert_eq!(read_node(&explained.nodes[1].outcome), "6f3ac1e29d54b807");
    }

    #[tokio::test]
    async fn ask_all_words_the_err_reply_of_a_node_that_cannot_explain() {
        let (older, _) = node(r#"err unknown command "explain""#, None).await;
        let (strict, _) = node("err explain needs a key", None).await;
        let targets = [target("n1", older), target("n2", strict)];
        let explained = ask_all(3, "k17", &targets, Duration::from_secs(5)).await;
        assert_eq!(failure(&explained.nodes[0].outcome), PREDATES);
        assert_eq!(
            failure(&explained.nodes[1].outcome),
            "the node replied: err explain needs a key"
        );
    }

    #[tokio::test]
    async fn ask_all_gives_up_on_a_silent_node_after_the_timeout() {
        let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent_addr = silent.local_addr().unwrap();
        tokio::spawn(async move {
            let _held = silent.accept().await;
            std::future::pending::<()>().await;
        });
        let (live, _) = node(OWNER_LINE.trim(), None).await;
        let targets = [target("n1", silent_addr), target("n2", live)];

        let started = Instant::now();
        let explained = ask_all(2, "k17", &targets, Duration::from_millis(200)).await;
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(
            failure(&explained.nodes[0].outcome),
            "no answer within 200 ms"
        );
        assert_eq!(read_node(&explained.nodes[1].outcome), "6f3ac1e29d54b807");
    }

    #[tokio::test]
    async fn answer_wraps_a_request_into_a_ui_command_with_its_id() {
        let (addr, seen) = node(NON_OWNER_LINE.trim(), None).await;
        let request = ExplainRequest {
            id: 9,
            key: "k1".to_owned(),
            targets: vec![target("n4", addr)],
        };
        let UiCommand::Explained(explained) = answer(request).await else {
            panic!("an answer is an Explained command");
        };
        assert_eq!(explained.id, 9);
        assert_eq!(explained.key, "k1");
        assert_eq!(explained.nodes.len(), 1);
        assert_eq!(explained.nodes[0].label, "n4");
        assert_eq!(explained.nodes[0].outcome, reading(NON_OWNER_LINE));
        assert_eq!(seen.lock().unwrap().as_slice(), ["explain k1"]);

        let nobody = ExplainRequest {
            id: 10,
            key: "k1".to_owned(),
            targets: Vec::new(),
        };
        let UiCommand::Explained(none) = answer(nobody).await else {
            panic!("an answer is an Explained command");
        };
        assert_eq!((none.id, none.nodes.len()), (10, 0));
    }
}
