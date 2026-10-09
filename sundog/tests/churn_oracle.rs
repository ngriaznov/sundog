//! Reads every key from every live node, continuously, while `Mode::Distributed`
//! ownership moves under real clusters: a node joining while an owner
//! crashes mid-pull, a node joining and leaving inside the disown grace, a
//! graceful leave, and a join followed by a crash. The oracle knows each
//! key's value or its deletion. A read may answer
//! `CacheError::FetchUnavailable`, never a wrong value or a miss for a key a
//! live node holds. A deleted key's old value is stale, allowed only while
//! the live nodes' views disagree about the key's owners: a node that gave
//! the key's part up still holds its copy through the disown grace and
//! answers a reader whose view has not caught up. Once every view agrees, a
//! deleted key never reads back.
//!
//! The first violations carry the reading node's `Cache::explain` of
//! the key, taken right after the read, and a `settle` that times
//! out prints the same account for its first wrong reads.
//!
//! The churn schedule's delays come from a seed, printed on failure and
//! overridden with `SUNDOG_ORACLE_SEED`; `SUNDOG_ORACLE_RUNS` runs that many
//! seeds in a row.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::{RngExt as _, SeedableRng as _, rngs::StdRng};
use sundog::{Cache, CacheError, Cluster, ClusterConfig, Mode, NodeId};

use common::fast_config;

const CLUSTER: &str = "it-churn-oracle";
const CACHE: &str = "oracle";
const KEYS: u32 = 400;

/// Violations the oracle keeps. A later one counts as a read and is dropped.
const MAX_VIOLATIONS: usize = 50;

/// How many of the first violations carry the reading node's explanation.
const EXPLAINED_VIOLATIONS: usize = 5;

/// How many of the first wrong reads a [`settle_to`] timeout explains.
const EXPLAINED_UNSETTLED: usize = 3;

/// What a read of `key` must answer once written: `None` for the keys the
/// setup deletes, every fifth.
fn expected(key: u32) -> Option<String> {
    (!key.is_multiple_of(5)).then(|| format!("v{key}"))
}

/// A disown grace far longer than the whole schedule, so a displaced owner
/// still holds what it gave up throughout: every key always has a live
/// holder, and any miss is the cluster's fault, not lost data.
fn config() -> ClusterConfig {
    fast_config().with(|c| {
        c.distributed_disown_grace_rounds = 400;
        c.tombstone_ttl = c.bucket_release_window() + Duration::from_secs(10);
    })
}

struct Member {
    name: &'static str,
    cluster: Cluster,
    cache: Cache<u32, String>,
}

async fn join(name: &'static str, seeds: &[&Member]) -> Member {
    let cluster = Cluster::builder(CLUSTER)
        .seeds(seeds.iter().map(|m| m.cluster.local_gossip_addr()))
        .config(config())
        .build()
        .await
        .unwrap_or_else(|error| panic!("{name} builds: {error}"));
    let cache = cluster
        .cache::<u32, String>(CACHE)
        .mode(Mode::distributed())
        .open()
        .await
        .unwrap_or_else(|error| panic!("{name} opens: {error}"));
    Member {
        name,
        cluster,
        cache,
    }
}

/// Everything the reader found wrong, and how many reads it made.
#[derive(Default)]
struct Findings {
    violations: Mutex<Vec<String>>,
    reads: AtomicU64,
    unavailable: AtomicU64,
    /// Deleted keys read back while the views disagreed: allowed staleness.
    stale: AtomicU64,
}

/// Each live member's view of a key's owners, sorted so two views that
/// name the same owners compare equal whatever order they rank them in.
fn owner_sets(
    members: &[(&'static str, NodeId, Cache<u32, String>)],
    key: u32,
) -> Vec<Vec<NodeId>> {
    members
        .iter()
        .map(|(_, _, cache)| {
            let mut owners = cache.owners_of(&key);
            owners.sort_unstable();
            owners
        })
        .collect()
}

/// Whether the views in `before` and `after`, taken around one read, all
/// name the same owners for its key: no node can have answered from a
/// part it gave up to a reader that still thought it owned it.
fn views_agree(before: &[Vec<NodeId>], after: &[Vec<NodeId>]) -> bool {
    before
        .iter()
        .chain(after)
        .all(|owners| before.first().is_some_and(|first| owners == first))
}

/// What the oracle makes of one read.
#[derive(Debug, PartialEq, Eq)]
enum Judgement {
    /// The read answers what the oracle expects.
    Correct,
    /// The read answers `CacheError::FetchUnavailable`, which is allowed.
    Unavailable,
    /// A deleted key's old value while the views disagree about its owners:
    /// allowed staleness.
    Stale,
    /// The read breaks the contract, for the reason named.
    Violation(&'static str),
}

/// Classifies one read against `want`, the oracle's answer for its key.
/// `settled` is whether every live view named the same owners around the
/// read.
fn classify(
    read: &Result<Option<String>, CacheError>,
    want: Option<&str>,
    settled: bool,
) -> Judgement {
    match (read, want) {
        (Ok(got), want) if got.as_deref() == want => Judgement::Correct,
        (Err(CacheError::FetchUnavailable { .. }), _) => Judgement::Unavailable,
        (Ok(Some(_)), None) if !settled => Judgement::Stale,
        (Ok(Some(_)), None) => {
            Judgement::Violation("a deleted key came back once every view agreed")
        }
        (Ok(None), Some(_)) => Judgement::Violation("a miss for a key a live node holds"),
        (Ok(_), _) => Judgement::Violation("a wrong value"),
        (Err(_), _) => Judgement::Violation("an unexpected error"),
    }
}

/// The members' owner sets from [`owner_sets`] as `[id id] [id id]`, one
/// bracket per member in the order the reader lists them, each id in the hex
/// an explanation prints.
fn show_owners(views: &[Vec<NodeId>]) -> String {
    views
        .iter()
        .map(|owners| {
            let ids: Vec<String> = owners.iter().map(ToString::to_string).collect();
            format!("[{}]", ids.join(" "))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// `cache`'s [`Cache::explain`] of a read of `key`, as text for a failure
/// message, every line after the first indented under its label.
async fn explained(cache: &Cache<u32, String>, key: u32) -> String {
    match cache.explain(&key).await {
        Ok(explanation) => explanation.to_string().replace('\n', "\n   "),
        Err(error) => format!("not available: {error}"),
    }
}

/// Checks one read of `key` from `node`, a member whose cache is `cache`,
/// against the oracle. `before` and `after` are the live members' owner sets
/// for the key around the read; the read is settled when they all name the
/// same owners.
///
/// The first [`EXPLAINED_VIOLATIONS`] violations also record both owner sets
/// and `cache`'s explanation of the key, taken after the read. Only that path
/// awaits: every read the oracle accepts, and every violation past the first
/// few, returns on the first poll, so the reader's schedule is the one it has
/// without explanations.
async fn judge(
    findings: &Findings,
    node: &str,
    cache: &Cache<u32, String>,
    key: u32,
    read: &Result<Option<String>, CacheError>,
    before: &[Vec<NodeId>],
    after: &[Vec<NodeId>],
) {
    findings.reads.fetch_add(1, Ordering::Relaxed);
    let verdict = match classify(read, expected(key).as_deref(), views_agree(before, after)) {
        Judgement::Correct => return,
        Judgement::Unavailable => {
            findings.unavailable.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Judgement::Stale => {
            findings.stale.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Judgement::Violation(verdict) => verdict,
    };
    // The lock decides and is released before the await below: a `std` guard
    // held across it would make the reader's future not `Send`.
    let explain = {
        let violations = findings.violations.lock().expect("unpoisoned");
        if violations.len() >= MAX_VIOLATIONS {
            return;
        }
        violations.len() < EXPLAINED_VIOLATIONS
    };
    let mut lines = vec![format!("{node}/k{key}: {verdict}: {read:?}")];
    if explain {
        let explanation = explained(cache, key).await;
        lines.push(format!("owners before the read: {}", show_owners(before)));
        lines.push(format!("owners after the read: {}", show_owners(after)));
        lines.push(format!("explained after the read: {explanation}"));
    }
    let mut violations = findings.violations.lock().expect("unpoisoned");
    if violations.len() < MAX_VIOLATIONS {
        violations.push(lines.join("\n "));
    }
}

/// A live member as the reader and `settle` see it.
type Live = Mutex<Vec<(&'static str, NodeId, Cache<u32, String>)>>;

/// How far [`settle_to`] waits.
#[derive(Clone, Copy, PartialEq)]
enum Settled {
    /// Every read answers as the oracle expects, though an owner may still
    /// be receiving its copy and answering through another owner.
    Reads,
    /// Reads, and every current owner of a present key also holds it
    /// locally.
    Placed,
}

/// [`settle_to`] with [`Settled::Placed`].
async fn settle(live: &Live, what: &str) {
    settle_to(live, what, Settled::Placed).await;
}

/// A read [`settle_to`] found wrong: the member that made it, that member's
/// cache, the key, and what went wrong: `owner-lacks-copy`, or what the fetch
/// answered (`value`, `miss`, `unavailable`, `error`).
struct Wrong<'a> {
    name: &'static str,
    cache: &'a Cache<u32, String>,
    key: u32,
    kind: &'static str,
}

/// The panic message of a [`settle_to`] that timed out after `what`: the
/// wrong reads per node and kind, tallied, then the first
/// [`EXPLAINED_UNSETTLED`] of them with their cache's explanation of the key,
/// taken after the timeout.
async fn never_settled(what: &str, wrong: &[Wrong<'_>]) -> String {
    let mut tally: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for entry in wrong {
        *tally
            .entry(format!("{}:{}", entry.name, entry.kind))
            .or_default() += 1;
    }
    let mut lines = vec![format!(
        "never settled after {what}: wrong reads per node and kind {tally:?}"
    )];
    for entry in wrong.iter().take(EXPLAINED_UNSETTLED) {
        let explanation = explained(entry.cache, entry.key).await;
        lines.push(format!(
            "{}/k{} ({}), explained after the timeout: {explanation}",
            entry.name, entry.key, entry.kind
        ));
    }
    lines.join("\n ")
}

/// Waits until every live member answers every key as the oracle expects
/// and, for [`Settled::Placed`], every current owner of a present key holds
/// it locally. On timeout, panics with what each member still gets wrong,
/// tallied, and the first few wrong reads explained.
async fn settle_to(live: &Live, what: &str, level: Settled) {
    let members = live.lock().expect("unpoisoned").clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut wrong: Vec<Wrong<'_>> = Vec::new();
        for (name, node, cache) in &members {
            for key in 0..KEYS {
                if level == Settled::Placed
                    && expected(key).is_some()
                    && members[0].2.owners_of(&key).contains(node)
                    && cache.get(&key).await != expected(key)
                {
                    wrong.push(Wrong {
                        name,
                        cache,
                        key,
                        kind: "owner-lacks-copy",
                    });
                }
                let read = cache.fetch(&key).await;
                if read.as_ref().ok() != Some(&expected(key)) {
                    let kind = match &read {
                        Ok(Some(_)) => "value",
                        Ok(None) => "miss",
                        Err(CacheError::FetchUnavailable { .. }) => "unavailable",
                        Err(_) => "error",
                    };
                    wrong.push(Wrong {
                        name,
                        cache,
                        key,
                        kind,
                    });
                }
            }
        }
        if wrong.is_empty() {
            eprintln!("settled: {what}");
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            let report = never_settled(what, &wrong).await;
            panic!("{report}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn run(seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut pause = |max_ms: u64| Duration::from_millis(rng.random_range(0..=max_ms));

    let node_a = join("a", &[]).await;
    let node_b = join("b", &[&node_a]).await;
    let node_c = join("c", &[&node_a, &node_b]).await;
    common::wait_for_peer_count(&node_c.cluster, 2, Duration::from_secs(15)).await;
    for key in 0..KEYS {
        node_a
            .cache
            .insert(key, format!("v{key}"))
            .await
            .expect("insert");
    }
    for key in (0..KEYS).filter(|k| expected(*k).is_none()) {
        node_a.cache.remove(&key).await.expect("remove");
    }

    let live = Arc::new(Mutex::new(vec![
        ("a", node_a.cluster.node_id(), node_a.cache.clone()),
        ("b", node_b.cluster.node_id(), node_b.cache.clone()),
        ("c", node_c.cluster.node_id(), node_c.cache.clone()),
    ]));
    settle(&live, "initial fill").await;

    let findings = Arc::new(Findings::default());
    let stop = Arc::new(AtomicBool::new(false));
    let reader = tokio::spawn({
        let (live, findings, stop) = (Arc::clone(&live), Arc::clone(&findings), Arc::clone(&stop));
        async move {
            while !stop.load(Ordering::Relaxed) {
                let members = live.lock().expect("unpoisoned").clone();
                for (name, _, cache) in &members {
                    for key in 0..KEYS {
                        let before = owner_sets(&members, key);
                        let read = cache.fetch(&key).await;
                        let after = owner_sets(&members, key);
                        judge(&findings, name, cache, key, &read, &before, &after).await;
                    }
                    // A fetch served from a local copy completes without
                    // awaiting anything, so once every member holds every
                    // key this loop would never hand its worker back.
                    tokio::task::yield_now().await;
                }
            }
        }
    });
    let drop_from_reader = |name: &str| {
        live.lock()
            .expect("unpoisoned")
            .retain(|(n, _, _)| *n != name);
    };
    let add_to_reader = |m: &Member| {
        live.lock()
            .expect("unpoisoned")
            .push((m.name, m.cluster.node_id(), m.cache.clone()));
    };

    // A joins-while-an-owner-crashes window: d starts pulling its share,
    // and a, which owns much of it, dies partway.
    let node_d = join("d", &[&node_b, &node_c]).await;
    add_to_reader(&node_d);
    tokio::time::sleep(pause(800)).await;
    drop_from_reader("a");
    node_a.cluster.crash().await;
    settle(&live, "d joined, a crashed").await;

    // A join and a leave inside the disown grace: the parts e took come
    // back to the nodes that gave them up.
    let node_e = join("e", &[&node_b, &node_d]).await;
    add_to_reader(&node_e);
    tokio::time::sleep(pause(1_500)).await;
    drop_from_reader("e");
    node_e.cluster.shutdown().await;
    // Only reads settle before b leaves: an owner may still be receiving its
    // copy while b answers for it, so b's leave has to hand its copies off.
    settle_to(&live, "e joined and left", Settled::Reads).await;

    // A graceful leave.
    tokio::time::sleep(pause(500)).await;
    drop_from_reader("b");
    node_b.cluster.shutdown().await;
    settle(&live, "b left").await;

    // A join, then a crash of another owner while it pulls.
    let node_f = join("f", &[&node_c, &node_d]).await;
    add_to_reader(&node_f);
    tokio::time::sleep(pause(800)).await;
    drop_from_reader("c");
    node_c.cluster.crash().await;
    settle(&live, "f joined, c crashed").await;

    stop.store(true, Ordering::Relaxed);
    reader.await.expect("reader finishes");
    let violations = findings.violations.lock().expect("unpoisoned").clone();
    eprintln!(
        "seed {seed}: {} reads, {} unavailable, {} stale while views moved, {} violations",
        findings.reads.load(Ordering::Relaxed),
        findings.unavailable.load(Ordering::Relaxed),
        findings.stale.load(Ordering::Relaxed),
        violations.len()
    );
    assert!(
        violations.is_empty(),
        "reads broke the churn contract (replay with SUNDOG_ORACLE_SEED={seed}):\n{}",
        violations.join("\n")
    );

    node_d.cluster.shutdown().await;
    node_f.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_never_miss_resurrect_or_go_stale_while_distributed_ownership_moves() {
    let seed: u64 = std::env::var("SUNDOG_ORACLE_SEED").map_or(0x5EED_0AC1, |raw| {
        raw.parse().expect("SUNDOG_ORACLE_SEED is a u64")
    });
    let runs: u64 = std::env::var("SUNDOG_ORACLE_RUNS")
        .map_or(1, |raw| raw.parse().expect("SUNDOG_ORACLE_RUNS is a u64"));
    for run_index in 0..runs {
        let seed = seed.wrapping_add(run_index);
        eprintln!("churn oracle: seed {seed}");
        run(seed).await;
    }
}

#[test]
fn views_agree_only_when_every_view_names_the_same_owners_before_and_after() {
    let (a, b, c) = (NodeId::from(1), NodeId::from(2), NodeId::from(3));
    let ab = vec![a, b];
    assert!(views_agree(
        &[ab.clone(), ab.clone()],
        &[ab.clone(), ab.clone()]
    ));
    assert!(
        !views_agree(&[ab.clone(), vec![a, c]], &[ab.clone(), ab.clone()]),
        "two members disagree before the read"
    );
    assert!(
        !views_agree(&[ab.clone(), ab.clone()], &[vec![b, c], vec![b, c]]),
        "the views moved during the read, even though they agree after it"
    );
}

#[test]
fn classify_allows_unavailable_and_unsettled_stale_reads_and_flags_every_other_wrong_answer() {
    type Read = Result<Option<String>, CacheError>;
    let value = |text: &str| -> Read { Ok(Some(text.to_owned())) };
    let miss = || -> Read { Ok(None) };
    let unavailable = || -> Read {
        Err(CacheError::FetchUnavailable {
            cache: CACHE.into(),
        })
    };
    let closed = || -> Read {
        Err(CacheError::Closed {
            cache: CACHE.into(),
        })
    };
    let came_back = "a deleted key came back once every view agreed";
    let missed = "a miss for a key a live node holds";
    // (read, what the oracle expects, whether the views agreed, judgement)
    let cases = [
        (value("v1"), Some("v1"), true, Judgement::Correct),
        (value("v1"), Some("v1"), false, Judgement::Correct),
        (miss(), None, true, Judgement::Correct),
        (miss(), None, false, Judgement::Correct),
        (unavailable(), Some("v1"), true, Judgement::Unavailable),
        (unavailable(), None, false, Judgement::Unavailable),
        (value("v5"), None, false, Judgement::Stale),
        (value("v5"), None, true, Judgement::Violation(came_back)),
        (miss(), Some("v1"), true, Judgement::Violation(missed)),
        (miss(), Some("v1"), false, Judgement::Violation(missed)),
        (
            value("v2"),
            Some("v1"),
            true,
            Judgement::Violation("a wrong value"),
        ),
        (
            value("v2"),
            Some("v1"),
            false,
            Judgement::Violation("a wrong value"),
        ),
        (
            closed(),
            Some("v1"),
            true,
            Judgement::Violation("an unexpected error"),
        ),
        (
            closed(),
            None,
            false,
            Judgement::Violation("an unexpected error"),
        ),
    ];
    for (read, want, settled, judgement) in cases {
        assert_eq!(
            classify(&read, want, settled),
            judgement,
            "{read:?} against {want:?}, views agreed: {settled}"
        );
    }
}

#[test]
fn owner_sets_show_as_one_bracket_per_member_in_the_hex_an_explanation_prints() {
    let (a, b, c) = (NodeId::from(1), NodeId::from(2), NodeId::from(3));
    assert_eq!(
        show_owners(&[vec![a, b], vec![a, c]]),
        "[0000000000000001 0000000000000002] [0000000000000001 0000000000000003]"
    );
    assert_eq!(show_owners(&[vec![b]]), "[0000000000000002]");
    assert_eq!(show_owners(&[vec![], vec![c]]), "[] [0000000000000003]");
    assert_eq!(show_owners(&[]), "");
}

/// A violation the oracle records carries the reading node's own account of
/// the key, taken after the read: the key's part, the owners the node names,
/// its residency marks, and what its copy holds, which here contradicts the
/// fabricated miss. The two owner sets sit above it in the node's hex ids.
#[tokio::test]
async fn a_violation_is_reported_with_its_explanation() {
    let solo = join("solo", &[]).await;
    let node = solo.cluster.node_id();
    let key = 7;
    solo.cache
        .insert(key, expected(key).expect("key 7 is not deleted"))
        .await
        .expect("insert");
    assert_eq!(
        solo.cache.owners_of(&key),
        vec![node],
        "a solo node owns every key alone"
    );
    let part = solo.cache.explain(&key).await.expect("u32 encodes").part;
    let views = owner_sets(&[("solo", node, solo.cache.clone())], key);

    // The oracle expects "v7"; a miss breaks the contract whatever the views
    // say.
    let findings = Findings::default();
    judge(
        &findings,
        "solo",
        &solo.cache,
        key,
        &Ok(None),
        &views,
        &views,
    )
    .await;

    let violations = findings.violations.lock().expect("unpoisoned").clone();
    assert_eq!(violations.len(), 1, "one wrong read, one violation");
    let text = &violations[0];
    assert!(
        text.starts_with("solo/k7: a miss for a key a live node holds: Ok(None)\n "),
        "{text}"
    );
    for expected_text in [
        format!("owners before the read: [{node}]\n"),
        format!("owners after the read: [{node}]\n"),
        format!("explained after the read: cache {CACHE} on node {node}, "),
        format!("part {}/{}, at ", part.bucket(), part.part()),
        format!("owners: {node}\n"),
        "residency: owned\n".to_owned(),
        "local: live ".to_owned(),
        "source: this node, hit".to_owned(),
    ] {
        assert!(text.contains(&expected_text), "{expected_text:?} in {text}");
    }
    assert_eq!(findings.reads.load(Ordering::Relaxed), 1);

    solo.cluster.shutdown().await;
}

/// The oracle keeps its first fifty violations and explains only the first
/// five: the sixth is the one-line form, and a violation past the fiftieth is
/// counted as a read and dropped.
#[tokio::test]
async fn only_the_first_five_violations_are_explained_and_only_the_first_fifty_are_kept() {
    let solo = join("solo", &[]).await;
    let views = owner_sets(&[("solo", solo.cluster.node_id(), solo.cache.clone())], 7);

    // Every key reads a value the oracle never expects: a wrong value for a
    // key it holds, a resurrection for a deleted key once the views agree.
    let findings = Findings::default();
    let wrong = Ok(Some("wrong".to_owned()));
    for key in 0..60 {
        judge(&findings, "solo", &solo.cache, key, &wrong, &views, &views).await;
    }

    let violations = findings.violations.lock().expect("unpoisoned").clone();
    assert_eq!(violations.len(), 50, "the list is capped");
    assert_eq!(findings.reads.load(Ordering::Relaxed), 60);
    for (index, text) in violations.iter().enumerate() {
        assert!(
            text.starts_with(&format!("solo/k{index}: ")),
            "violations keep the order they happened in: {text}"
        );
        assert_eq!(
            text.contains("explained after the read"),
            index < 5,
            "violation {index}: {text}"
        );
        assert_eq!(text.contains('\n'), index < 5, "violation {index}: {text}");
    }

    solo.cluster.shutdown().await;
}

/// A `settle_to` that times out reports every wrong read tallied by node and
/// kind, and explains the first three through their own caches; a fourth
/// wrong read is tallied and not explained.
#[tokio::test]
async fn a_timeout_tallies_every_wrong_read_and_explains_the_first_three() {
    let solo = join("solo", &[]).await;
    let node = solo.cluster.node_id();
    let wrong: Vec<Wrong<'_>> = [
        (0, "miss"),
        (1, "miss"),
        (2, "owner-lacks-copy"),
        (3, "value"),
        (4, "miss"),
    ]
    .into_iter()
    .map(|(key, kind)| Wrong {
        name: "solo",
        cache: &solo.cache,
        key,
        kind,
    })
    .collect();

    let report = never_settled("a fixture", &wrong).await;

    assert!(
        report.starts_with(
            "never settled after a fixture: wrong reads per node and kind \
             {\"solo:miss\": 3, \"solo:owner-lacks-copy\": 1, \"solo:value\": 1}\n "
        ),
        "{report}"
    );
    assert_eq!(report.matches("explained after the timeout").count(), 3);
    for (key, kind) in [(0, "miss"), (1, "miss"), (2, "owner-lacks-copy")] {
        assert!(
            report.contains(&format!(
                "solo/k{key} ({kind}), explained after the timeout: "
            )),
            "{report}"
        );
    }
    assert!(!report.contains("solo/k3 "), "{report}");
    assert_eq!(
        report
            .matches(&format!("cache {CACHE} on node {node}, "))
            .count(),
        3,
        "each explanation comes from the cache that read"
    );

    solo.cluster.shutdown().await;
}
