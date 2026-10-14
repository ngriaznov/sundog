//! `rebalance_task` retries a view-change pull that ran out its budget, on the `ae_interval` tick, until it lands.

use super::*;
use crate::config::ClusterConfig;
use crate::ownership::Granularity;

const CACHE: &str = "prices";

/// The metric a timed-out pull counts under.
const TIMEOUTS: &str = "sundog_rebalance_pull_timeouts_total";

fn config(budget: Duration) -> ClusterConfig {
    ClusterConfig {
        gossip_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        data_bind_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        state_transfer_budget: budget,
        ae_interval: Duration::from_millis(100),
        ..ClusterConfig::default()
    }
}

/// The granularity the donor computes between two current-protocol nodes.
fn part_view(cluster: &Cluster, eligible: Vec<NodeId>, k: u8) -> OwnershipView {
    OwnershipView::compute_at(
        cluster.node_id(),
        eligible,
        NonZeroU8::new(k).expect("nonzero"),
        Granularity::Part,
    )
}

/// A node under test whose first view-change pull has been issued against a
/// donor that still declines it: the donor has the cache open but computes a
/// view without this node, so every pull attempt is declined as stale.
struct Rig {
    cluster: Cluster,
    donor: Cluster,
    /// Keeps the donor's cache registered.
    _donor_cache: crate::Cache<u32, u32>,
    mode: crate::Mode,
    name: SmolStr,
    residency: Arc<ResidencySet>,
    tx: tokio::sync::watch::Sender<Arc<OwnershipView>>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    /// A part the first view change gained from the donor.
    gained: PartId,
    /// The view the pull was issued under.
    full: Arc<OwnershipView>,
}

impl Rig {
    /// Builds both nodes, spawns `rebalance_task` on the first with
    /// `disown_grace`, and returns once the pull of `gained` is under way.
    async fn start(cluster_name: &'static str, budget: Duration, disown_grace: Duration) -> Self {
        let cluster = Cluster::builder(cluster_name)
            .seeds(std::iter::empty())
            .config(config(budget))
            .build()
            .await
            .expect("a builds");
        let donor = Cluster::builder(cluster_name)
            .seeds([cluster.local_gossip_addr()])
            .config(config(budget))
            .build()
            .await
            .expect("the donor builds");
        let k = NonZeroU8::new(2).expect("nonzero");
        let mode = crate::Mode::Distributed { owners: k };
        let donor_cache = donor
            .cache::<u32, u32>(CACHE)
            .mode(mode)
            .open()
            .await
            .expect("the donor opens the cache alone");
        tokio::time::timeout(Duration::from_secs(15), async {
            while !cluster
                .peers()
                .iter()
                .any(|peer| peer.node == donor.node_id())
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the donor is live in gossip");

        let name = SmolStr::new(CACHE);
        let (tracker, tx) = OwnershipTracker::seed(
            cluster.node_id(),
            &cluster.peers(),
            &cluster.advertised_cache_modes(),
            &name,
            k,
        );
        let residency = Arc::new(ResidencySet::new());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(rebalance_task(
            cluster.clone(),
            empty_shard(),
            tracker,
            Arc::clone(&residency),
            name.clone(),
            disown_grace,
            4,
            cancel.clone(),
        ));

        // Half the parts go to a phantom node, then come back co-owned
        // with the donor: the second view gains them and pulls from it.
        let phantom = NodeId::from(u64::MAX);
        tx.send(Arc::new(part_view(
            &cluster,
            vec![cluster.node_id(), phantom],
            1,
        )))
        .expect("receiver still alive");
        tokio::time::timeout(Duration::from_secs(5), async {
            while residency.expired(Duration::ZERO).is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the first view change is handled");
        let full = Arc::new(part_view(
            &cluster,
            vec![cluster.node_id(), donor.node_id()],
            2,
        ));
        let gained = full
            .owned_parts()
            .find(|&part| residency.is_releasing(part))
            .expect("a part the phantom took");
        tx.send(Arc::clone(&full)).expect("receiver still alive");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !residency.is_cold(gained) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the pull starts");
        Self {
            cluster,
            donor,
            _donor_cache: donor_cache,
            mode,
            name,
            residency,
            tx,
            cancel,
            task,
            gained,
            full,
        }
    }

    /// Lets the donor's view catch up with this node's, with no view
    /// published here: the pulls it declined as stale now succeed.
    async fn catch_the_donor_up(&self) {
        let donor_shard = crate::cluster::test_support::registered_shard(&self.donor, &self.name);
        self.cluster.advertise_cache_mode(&self.name, self.mode);
        tokio::time::timeout(Duration::from_secs(10), async {
            while donor_shard.ownership_view_hash() != Some(self.full.view_hash()) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the donor computes the same view");
    }

    async fn finish(self) {
        self.cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), self.task).await;
        self.donor.shutdown().await;
        self.cluster.shutdown().await;
    }
}

/// Reads the timeouts counted under the test's cache. The recorder's counters
/// reset when read, so the reads add up here.
struct Timeouts {
    snapshotter: metrics_util::debugging::Snapshotter,
    total: std::cell::Cell<u64>,
}

impl Timeouts {
    fn new(recorder: &metrics_util::debugging::DebuggingRecorder) -> Self {
        Self {
            snapshotter: recorder.snapshotter(),
            total: std::cell::Cell::new(0),
        }
    }

    /// The timeouts counted so far.
    fn count(&self) -> u64 {
        let new: u64 = self
            .snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, ..)| {
                key.key().name() == TIMEOUTS
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "cache" && label.value() == CACHE)
            })
            .map(|(_, _, _, value)| match value {
                metrics_util::debugging::DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum();
        self.total.set(self.total.get() + new);
        self.total.get()
    }
}

async fn eventually(budget: Duration, what: &str, mut cond: impl FnMut() -> bool) {
    tokio::time::timeout(budget, async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// A view-change pull that times out is pulled again every `ae_interval`
/// until it lands (`state_transfer_budget` docs), not only on the next
/// membership change. The first pull runs out its budget against the
/// donor's stale view and leaves the part cold; once the donor's view
/// catches up, with no further view published here, the gained part is warm
/// again.
#[tokio::test]
async fn rebalance_task_retries_a_timed_out_pull_every_ae_interval() {
    let rig = Rig::start(
        "rebalance-unit-test-retry",
        Duration::from_millis(600),
        Duration::from_secs(3600),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(
        rig.residency.is_cold(rig.gained),
        "the timed-out pull left it cold"
    );

    rig.catch_the_donor_up().await;
    eventually(
        Duration::from_secs(10),
        "a timed-out view-change pull is retried every ae_interval once the donor can serve; the part is still cold",
        || !rig.residency.is_cold(rig.gained),
    )
    .await;
    assert!(
        !rig.residency.is_unverified(rig.gained),
        "a landed pull clears the stale mark too"
    );

    // The task still answers a cancel.
    rig.cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), rig.task)
        .await
        .expect("the task ends within two seconds of cancel")
        .expect("the task does not panic");
    rig.donor.shutdown().await;
    rig.cluster.shutdown().await;
}

/// Every view-change pull and every retry that runs out its budget counts
/// under `sundog_rebalance_pull_timeouts_total{cache}`.
#[tokio::test]
async fn rebalance_task_counts_each_timed_out_pull() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let timeouts = Timeouts::new(&recorder);
    let _guard = metrics::set_default_local_recorder(&recorder);
    let before = timeouts.count();
    let rig = Rig::start(
        "rebalance-unit-test-retry-metric",
        Duration::from_millis(400),
        Duration::from_secs(3600),
    )
    .await;
    assert_eq!(
        timeouts.count(),
        before,
        "no pull has run out its budget yet"
    );

    eventually(Duration::from_secs(10), "the first pull times out", || {
        timeouts.count() > before
    })
    .await;
    eventually(
        Duration::from_secs(10),
        "a retry against the still-stale donor times out as well",
        || timeouts.count() >= before + 2,
    )
    .await;
    rig.finish().await;
}

/// A view published during a retry preempts it: the new view is handled long
/// before the retry's budget runs out, and the retry resumes on the next
/// tick if parts are still cold.
#[tokio::test]
async fn a_view_published_during_a_retry_preempts_it() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let timeouts = Timeouts::new(&recorder);
    let _guard = metrics::set_default_local_recorder(&recorder);
    let before = timeouts.count();
    let budget = Duration::from_secs(4);
    let rig = Rig::start(
        "rebalance-unit-test-retry-preempt",
        budget,
        Duration::from_secs(3600),
    )
    .await;
    eventually(budget * 2, "the first pull times out", || {
        timeouts.count() > before
    })
    .await;
    // A retry is running now, with most of `budget` left.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        timeouts.count(),
        before + 1,
        "the retry is still within its budget"
    );

    // The phantom takes `gained` back.
    let phantom = NodeId::from(u64::MAX);
    let started = std::time::Instant::now();
    rig.tx
        .send(Arc::new(part_view(
            &rig.cluster,
            vec![rig.cluster.node_id(), phantom],
            1,
        )))
        .expect("receiver still alive");
    eventually(
        Duration::from_secs(2),
        "the view change is handled while the retry runs",
        || rig.residency.is_releasing(rig.gained),
    )
    .await;
    assert!(
        started.elapsed() < budget / 2,
        "the retry gave way to the view change instead of running out its budget"
    );
    rig.finish().await;
}

/// The task still ends promptly on cancel while a retry is running.
#[tokio::test]
async fn rebalance_task_stops_mid_retry_when_cancelled() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let timeouts = Timeouts::new(&recorder);
    let _guard = metrics::set_default_local_recorder(&recorder);
    let before = timeouts.count();
    let budget = Duration::from_secs(4);
    let rig = Rig::start(
        "rebalance-unit-test-retry-cancel",
        budget,
        Duration::from_secs(3600),
    )
    .await;
    eventually(budget * 2, "the first pull times out", || {
        timeouts.count() > before
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    rig.cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), rig.task)
        .await
        .expect("the task ends within a second of cancel, not at the retry's budget")
        .expect("the task does not panic");
    rig.donor.shutdown().await;
    rig.cluster.shutdown().await;
}

/// A zero budget runs no pull at all, so nothing times out and the retry
/// never starts.
#[tokio::test]
async fn a_zero_budget_runs_no_retry() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let timeouts = Timeouts::new(&recorder);
    let _guard = metrics::set_default_local_recorder(&recorder);
    let before = timeouts.count();
    let rig = Rig::start(
        "rebalance-unit-test-retry-zero",
        Duration::ZERO,
        Duration::from_secs(3600),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(timeouts.count(), before, "no pull ran");
    rig.finish().await;
}

/// A part whose disown grace has run out is released on the tick that finds
/// it due, before a retry begins, so a pull that keeps timing out never
/// holds a release behind the retry's budget.
#[tokio::test]
async fn a_due_release_is_not_held_behind_a_retry() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let timeouts = Timeouts::new(&recorder);
    let _guard = metrics::set_default_local_recorder(&recorder);
    let before = timeouts.count();
    let budget = Duration::from_secs(3);
    let rig = Rig::start(
        "rebalance-unit-test-retry-release",
        budget,
        Duration::from_millis(150),
    )
    .await;
    // Two phantoms and the donor: this node loses the parts the phantoms
    // own between them, whose owners are unreachable, and keeps pulling
    // the parts it shares with the donor.
    let (phantom_a, phantom_b) = (NodeId::from(u64::MAX - 1), NodeId::from(u64::MAX - 2));
    let crowded = Arc::new(part_view(
        &rig.cluster,
        vec![
            rig.cluster.node_id(),
            rig.donor.node_id(),
            phantom_a,
            phantom_b,
        ],
        2,
    ));
    let lost = rig
        .full
        .owned_parts()
        .find(|&part| {
            !crowded.owns(part)
                && crowded
                    .owners_of(part)
                    .iter()
                    .all(|owner| *owner == phantom_a || *owner == phantom_b)
        })
        .expect("a part only the phantoms own in the crowded view");
    // The first pull is still running; its view change is handled when it
    // ends, so publish the crowded view now and let the task reach it.
    rig.tx
        .send(Arc::clone(&crowded))
        .expect("receiver still alive");
    eventually(budget * 3, "the crowded view is handled", || {
        rig.residency.is_releasing(lost)
    })
    .await;
    eventually(budget * 3, "a pull times out, so a retry follows", || {
        timeouts.count() > before
    })
    .await;
    let counted = timeouts.count();
    eventually(
        Duration::from_secs(2),
        "the part is released while the retry that follows the timeout runs",
        || !rig.residency.is_releasing(lost),
    )
    .await;
    assert_eq!(
        timeouts.count(),
        counted,
        "the release did not wait for a further timeout"
    );
    rig.finish().await;
}
