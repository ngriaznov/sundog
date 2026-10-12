//! Tests of the scenario director.

use std::sync::Mutex;
use std::time::SystemTime;

use sundog::observe::MemberStatus;

use super::*;
use crate::ask::SCRIPTED_ID;
use crate::explained::{Outcome, fixture};
use crate::model::digest::HeldShares;
use crate::model::testkit;
use crate::scenario::{AwaitedStatus, Entry, parse};

mod decisions;
mod explain_step;
mod playing;

const S: fn(u64) -> Duration = Duration::from_secs;

/// Held by the tests whose runs log a warning. `tracing` caches whether a
/// callsite is enabled when a thread first reaches it, so a test that
/// installs a subscriber must not race another test into the callsite.
static LOGGING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn digest(live: usize) -> ModelDigest {
    ModelDigest {
        live,
        ..ModelDigest::default()
    }
}

fn with_cache(mut digest: ModelDigest, view: u64, settled: bool) -> ModelDigest {
    digest.view_hash.insert("it".into(), view);
    digest.settled.insert("it".into(), settled);
    digest
}

/// A stage that records what it is asked and, for a spawn, moves the
/// digest the way a real cluster would: members join, the view changes,
/// and the cache settles a moment later.
struct Fake {
    calls: Mutex<Vec<String>>,
    digests: watch::Sender<ModelDigest>,
    fail_kill: bool,
    settle_after: Duration,
    /// After a kill or crash the node shows `Down` this much later.
    down_after: Option<Duration>,
    /// What the nodes answer to `explain` once the script has run out.
    nodes: Nodes,
    /// What the nodes answer to the first asks, in order.
    script: Mutex<std::collections::VecDeque<Nodes>>,
}

/// What the fake's nodes answer to `explain`: the five live nodes of the
/// fixture model, as a converged cluster gives them, unless it says
/// otherwise.
#[derive(Clone, Copy, Debug)]
enum Nodes {
    /// Every node holds the lens's view and owners.
    Agree,
    /// Every node holds a view the lens does not.
    Moved,
    /// `n3` refuses the connection.
    OneRefuses,
    /// The answer holds no node.
    Nobody,
    /// Every node agrees, after this long.
    Late(Duration),
    /// Nobody answers.
    Never,
    /// The stage cannot ask.
    Broken,
}

impl Fake {
    fn new(digests: watch::Sender<ModelDigest>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            digests,
            fail_kill: false,
            settle_after: Duration::from_millis(500),
            down_after: None,
            nodes: Nodes::Agree,
            script: Mutex::default(),
        }
    }

    fn note(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    /// The detector notices `label` `down_after` later, if set.
    fn goes_down(&self, label: &str) {
        let Some(after) = self.down_after else { return };
        let digests = self.digests.clone();
        let label = label.to_owned();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            digests.send_modify(|d| {
                d.statuses.insert(label.as_str().into(), MemberStatus::Down);
            });
        });
    }

    /// The cluster changes: `live` members, a new view, settled later.
    fn change(&self, live: usize) {
        let view = u64::try_from(live).unwrap() * 100;
        self.digests
            .send_replace(with_cache(digest(live), view, false));
        let digests = self.digests.clone();
        let after = self.settle_after;
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            digests.send_modify(|d| {
                d.settled.insert("it".into(), true);
            });
        });
    }
}

#[expect(clippy::unused_async_trait_impl, reason = "the fake never waits")]
impl Stage for Fake {
    async fn spawn(&self, count: usize, stagger: Option<Duration>) -> anyhow::Result<()> {
        self.note(format!("spawn {count} {stagger:?}"));
        let live = self.digests.borrow().live + count;
        self.change(live);
        Ok(())
    }

    async fn fill(&self, keys: u64) -> anyhow::Result<()> {
        self.note(format!("fill {keys}"));
        Ok(())
    }

    fn load(&self, on: bool) {
        self.note(format!("load {on}"));
    }

    async fn kill(&self, label: &str) -> anyhow::Result<()> {
        self.note(format!("kill {label}"));
        if self.fail_kill {
            anyhow::bail!("{label} is not running");
        }
        self.goes_down(label);
        Ok(())
    }

    async fn leave(&self, label: &str) -> anyhow::Result<()> {
        self.note(format!("leave {label}"));
        Ok(())
    }

    async fn crash(&self, label: &str) -> anyhow::Result<()> {
        self.note(format!("crash {label}"));
        self.goes_down(label);
        Ok(())
    }

    async fn restart(&self, label: &str) -> anyhow::Result<()> {
        self.note(format!("restart {label}"));
        Ok(())
    }

    async fn explain(&self, key: &str) -> anyhow::Result<Explained> {
        self.note(format!("explain {key}"));
        let nodes = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(self.nodes);
        let mut answers = converged_answers(key);
        match nodes {
            Nodes::Agree => {}
            Nodes::Moved => {
                for answer in &mut answers.nodes {
                    if let Outcome::Read(reading) = &mut answer.outcome {
                        reading
                            .distributed
                            .as_mut()
                            .expect("the fixture reads are distributed")
                            .view = "00c1a2f4e8b3195d".to_owned();
                    }
                }
            }
            Nodes::OneRefuses => {
                answers.nodes[2].outcome =
                    Outcome::Failed("no answer (connection refused)".to_owned());
            }
            Nodes::Nobody => answers.nodes.clear(),
            Nodes::Late(after) => tokio::time::sleep(after).await,
            Nodes::Never => std::future::pending().await,
            Nodes::Broken => anyhow::bail!("no node is running to ask"),
        }
        Ok(answers)
    }
}

/// The model of five live nodes ranking `it`, whose placement the fake's
/// answers and the digest's shares both come from. Ranking costs a few
/// hundred thousand hashes, so every test shares one.
fn cluster() -> &'static crate::model::Model {
    static CLUSTER: std::sync::OnceLock<crate::model::Model> = std::sync::OnceLock::new();
    CLUSTER.get_or_init(|| testkit::fixture_model(std::time::Instant::now()))
}

/// What five nodes that hold the lens's view answer about `key`.
fn converged_answers(key: &str) -> Explained {
    fixture::explained(cluster(), SCRIPTED_ID, key, SystemTime::UNIX_EPOCH)
}

/// `digest` with the ownership shares of the model of [`cluster`].
fn with_cluster_shares(mut digest: ModelDigest) -> ModelDigest {
    let ranked = cluster().ownership("it").expect("the fixture ranks it");
    digest.ownership.insert(
        "it".into(),
        HeldShares(std::sync::Arc::clone(&ranked.shares)),
    );
    digest
}

struct Rig {
    director: Director<Fake>,
    ui: mpsc::UnboundedReceiver<UiCommand>,
}

fn rig(options: Options, tweak: impl FnOnce(&mut Fake)) -> Rig {
    let (tx, rx) = watch::channel(ModelDigest::default());
    let mut fake = Fake::new(tx);
    tweak(&mut fake);
    let (ui_tx, ui_rx) = mpsc::unbounded_channel();
    Rig {
        director: Director::new(fake, rx, Some(ui_tx), options),
        ui: ui_rx,
    }
}

/// A rig whose digest holds the cluster's shares and whose nodes answer
/// `nodes`.
fn explaining_rig(options: Options, nodes: Nodes) -> Rig {
    let rig = rig(options, |fake| fake.nodes = nodes);
    rig.director
        .stage
        .digests
        .send_modify(|digest| *digest = with_cluster_shares(std::mem::take(digest)));
    rig
}

const HEADLESS: Options = Options {
    headless: true,
    captions: true,
};
const SHOW: Options = Options {
    headless: false,
    captions: true,
};

/// Collects what `tracing` events write.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Self;

    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// Collects what this thread's `tracing` events write, until the guard
/// drops. Held beside [`LOGGING`].
fn capture_logs() -> (Captured, tracing::subscriber::DefaultGuard) {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    // Other tests of this module hit the same callsites on their own
    // threads, with no subscriber: recompute what each callsite is
    // interested in now that this thread has one.
    tracing::callsite::rebuild_interest_cache();
    (captured, guard)
}
