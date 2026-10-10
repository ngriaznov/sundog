//! The drift guard for `key` and `locate`: a key typed as text hashes to the
//! part `Cache::explain` reports, and the owners the lens computes from
//! gossip are the owners every node's `Cache::owners_of` answers, on real
//! in-process clusters. The `sundog-lens` binary's `watch --once --explain`
//! reports the same part, view and owners as the nodes' own `Cache::explain`.
//!
//! The lens knows no cache's key type, so it assumes the encoding the cache
//! applies: postcard. These tests fail when `sundog` changes how it encodes a
//! key or hashes it into a part, or when the observer's ownership drifts from
//! the members'.

use std::hash::Hash;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant, SystemTime};

use serde::Serialize;
use serde::de::DeserializeOwned;
use sundog::store::{Mode, PartId};
use sundog::{Cache, Cluster, ClusterConfig, ReadExplanation};
use sundog_lens::cli::Seed;
use sundog_lens::key::KeySpec;
use sundog_lens::locate::{LocateError, locate};
use sundog_lens::model::Model;
use sundog_lens::source::{Feed, FeedConfig};

/// The bound on each wait: a Distributed cache opening on a joining node, and
/// the nodes and the lens agreeing on one ownership view. Three nodes spend
/// seconds of CPU on their views in a debug build, so a runner shared with
/// other tests takes well past ten seconds; a wait returns as soon as its
/// condition holds. A timeout is a bug to root-cause, not a flake.
const CONVERGE: Duration = Duration::from_secs(30);

fn loopback_config() -> ClusterConfig {
    ClusterConfig::default().with(|config| {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        config.gossip_bind_addr = loopback;
        config.data_bind_addr = loopback;
        config.ae_interval = Duration::from_millis(200);
    })
}

async fn node(name: &str, seed: Option<SocketAddr>) -> Cluster {
    Cluster::builder(name)
        .seeds(seed)
        .config(loopback_config())
        .build()
        .await
        .expect("the node builds")
}

async fn open<K, V>(cluster: &Cluster, name: &str, mode: Mode) -> Cache<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(CONVERGE, cluster.cache::<K, V>(name).mode(mode).open())
        .await
        .expect("the cache opens within the bound")
        .expect("the cache opens")
}

/// Asserts that `text`, parsed as a key, hashes to the part `cache` reports
/// for `key`.
async fn assert_part<K>(cache: &Cache<K, String>, key: &K, text: &str)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let spec = KeySpec::parse(text).unwrap_or_else(|error| panic!("{text:?} parses: {error}"));
    let explained = cache
        .explain(key)
        .await
        .unwrap_or_else(|error| panic!("{text:?} explains: {error}"));
    assert_eq!(
        PartId::of_key(spec.bytes()),
        explained.part,
        "{text:?} as {} hashes to the part {}/{} explain reports in {}",
        spec.kind().describe(),
        explained.part.bucket(),
        explained.part.part(),
        cache.name()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_keys_hash_to_the_part_cache_explain_reports() {
    // `Cache::explain` names the part before it looks at the mode, so a Local
    // cache, which opens at once, stands for any of them.
    let cluster = node("lens-locate-text", None).await;
    let cache: Cache<String, String> = open(&cluster, "text", Mode::Local).await;

    let keys = [
        String::new(),
        "k1".to_owned(),
        "user:42".to_owned(),
        "é".to_owned(),
        // The length prefix is one varint byte up to 127 bytes and two from 128.
        "a".repeat(127),
        "a".repeat(128),
        // 300 bytes: 150 two-byte characters.
        "é".repeat(150),
    ];
    for key in &keys {
        assert_part(&cache, key, key).await;
    }
    // A String that starts with a prefix is typed with the `str:` escape.
    for key in ["uint:1", "int:-1", "hex:6b", "str:x"] {
        assert_part(&cache, &key.to_owned(), &format!("str:{key}")).await;
    }

    cache.close().await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_keys_hash_to_the_part_cache_explain_reports() {
    let cluster = node("lens-locate-integers", None).await;

    let cache: Cache<u16, String> = open(&cluster, "u16", Mode::Local).await;
    for key in [0, 1, 127, 128, 300, u16::MAX] {
        assert_part(&cache, &key, &format!("uint:{key}")).await;
    }
    let cache: Cache<u32, String> = open(&cluster, "u32", Mode::Local).await;
    for key in [0, 1, 127, 128, 300, 70_000, u32::MAX] {
        assert_part(&cache, &key, &format!("uint:{key}")).await;
    }
    let cache: Cache<u64, String> = open(&cluster, "u64", Mode::Local).await;
    for key in [0, 1, 127, 128, 300, u64::from(u32::MAX) + 1, u64::MAX] {
        assert_part(&cache, &key, &format!("uint:{key}")).await;
    }

    let cache: Cache<i16, String> = open(&cluster, "i16", Mode::Local).await;
    for key in [0, 1, -1, 63, 64, -64, -65, 300, i16::MIN, i16::MAX] {
        assert_part(&cache, &key, &format!("int:{key}")).await;
    }
    let cache: Cache<i32, String> = open(&cluster, "i32", Mode::Local).await;
    for key in [0, 1, -1, 63, 64, -64, -65, 300, i32::MIN, i32::MAX] {
        assert_part(&cache, &key, &format!("int:{key}")).await;
    }
    let cache: Cache<i64, String> = open(&cluster, "i64", Mode::Local).await;
    for key in [0, 1, -1, 300, i64::from(i32::MAX) + 1, i64::MIN, i64::MAX] {
        assert_part(&cache, &key, &format!("int:{key}")).await;
    }

    // A u8 and an i8 are one raw byte, not a varint: `hex:` types them.
    let cache: Cache<u8, String> = open(&cluster, "u8", Mode::Local).await;
    for key in [0u8, 1, 127, 128, u8::MAX] {
        assert_part(&cache, &key, &format!("hex:{key:02x}")).await;
    }
    let cache: Cache<i8, String> = open(&cluster, "i8", Mode::Local).await;
    for (key, hex) in [(0i8, "00"), (-1, "ff"), (127, "7f"), (i8::MIN, "80")] {
        assert_part(&cache, &key, &format!("hex:{hex}")).await;
    }
    // So does any other key type: a tuple is its fields' postcard bytes in
    // order, here 07 and the varint of 300.
    let cache: Cache<(u8, u32), String> = open(&cluster, "pair", Mode::Local).await;
    assert_part(&cache, &(7, 300), "hex:07ac02").await;

    cluster.shutdown().await;
}

/// Applies the feed's updates to `model` until `ready` holds.
async fn watch_until(
    feed: &mut Feed,
    model: &mut Model,
    what: &str,
    ready: impl Fn(&Model) -> bool,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let settled = tokio::time::timeout(CONVERGE, async {
        loop {
            tokio::select! {
                update = feed.recv() => {
                    let update = update.expect("the sources run");
                    model.apply(update, Instant::now(), SystemTime::now());
                }
                _ = tick.tick() => {
                    model.tick(Instant::now());
                }
            }
            if ready(model) {
                return;
            }
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "the lens did not see {what} within {CONVERGE:?}"
    );
}

/// Waits until every cache in `caches` reports the ownership view with
/// `view_hash` for `probe`.
async fn wait_for_view<K>(caches: &[&Cache<K, String>], probe: &K, view_hash: u64)
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let converged = tokio::time::timeout(CONVERGE, async {
        loop {
            let mut views = Vec::new();
            for cache in caches {
                let view = cache
                    .explain(probe)
                    .await
                    .ok()
                    .and_then(|explained| explained.distributed)
                    .map(|read| read.view_hash);
                views.push(view);
            }
            if views.iter().all(|view| *view == Some(view_hash)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        converged.is_ok(),
        "the nodes did not reach the lens's view {view_hash:016x} of {} within {CONVERGE:?}",
        caches.first().map_or("", |cache| cache.name())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_spec_locates_the_owners_the_caches_report() {
    let name = "lens-locate-owners";
    let a = node(name, None).await;
    let b = node(name, Some(a.local_gossip_addr())).await;
    let c = node(name, Some(a.local_gossip_addr())).await;
    let mut ids: Vec<Cache<u32, String>> = Vec::new();
    let mut names: Vec<Cache<String, String>> = Vec::new();
    for cluster in [&a, &b, &c] {
        ids.push(open(cluster, "ids", Mode::distributed()).await);
        names.push(open(cluster, "names", Mode::distributed()).await);
    }

    let mut config = FeedConfig::new(name);
    config.bind = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    config.seeds = vec![Seed::Addr(a.local_gossip_addr())];
    let mut feed = Feed::spawn(config).await.expect("the feed starts");
    let mut model = Model::new();
    watch_until(
        &mut feed,
        &mut model,
        "both caches ranked over three nodes",
        |model| {
            ["ids", "names"].iter().all(|cache| {
                model
                    .ownership(cache)
                    .is_some_and(|digest| digest.eligible.len() == 3)
            })
        },
    )
    .await;
    // The lens and the nodes rank one view.
    let ids_view = model.ownership("ids").expect("ids is ranked").view_hash;
    let names_view = model.ownership("names").expect("names is ranked").view_hash;
    wait_for_view(&ids.iter().collect::<Vec<_>>(), &0, ids_view).await;
    wait_for_view(
        &names.iter().collect::<Vec<_>>(),
        &String::new(),
        names_view,
    )
    .await;

    for n in 0..256u32 {
        let spec = KeySpec::parse(&format!("uint:{n}")).expect("a number parses");
        let located = locate(&model, Some("ids"), &spec).expect("ids is located");
        assert_eq!(located.view_hash, ids_view);
        assert_eq!(located.owners.len(), 2);
        let lens: Vec<_> = located.owners.iter().map(|owner| owner.node).collect();
        for (index, cache) in ids.iter().enumerate() {
            assert_eq!(lens, cache.owners_of(&n), "key {n} on node {index}");
        }
    }
    for n in 0..64 {
        let key = format!("user:{n}");
        let spec = KeySpec::parse(&key).expect("text parses");
        let located = locate(&model, Some("names"), &spec).expect("names is located");
        assert_eq!(located.view_hash, names_view);
        let lens: Vec<_> = located.owners.iter().map(|owner| owner.node).collect();
        for (index, cache) in names.iter().enumerate() {
            assert_eq!(lens, cache.owners_of(&key), "key {key} on node {index}");
        }
    }

    // The part the lens names is the part a node explains, and the view it
    // names is the view the node used.
    let spec = KeySpec::parse("uint:7").expect("a number parses");
    let located = locate(&model, Some("ids"), &spec).expect("ids is located");
    let explained = ids[0].explain(&7).await.expect("explain answers");
    assert_eq!(located.part, explained.part);
    assert_eq!(
        explained
            .distributed
            .map(|read| (read.view_hash, read.owners)),
        Some((
            located.view_hash,
            located
                .owners
                .iter()
                .map(|owner| owner.node)
                .collect::<Vec<_>>()
        ))
    );

    // Two ranked Distributed caches: the lens asks for a name.
    assert_eq!(
        locate(&model, None, &spec),
        Err(LocateError::Ambiguous {
            caches: vec!["ids".into(), "names".into()]
        })
    );

    feed.shutdown().await;
    for cache in ids {
        cache.close().await;
    }
    for cache in names {
        cache.close().await;
    }
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

/// Waits until every node knows both other nodes and `caches` all report
/// the same ownership view and the same two owners for `probe`, and returns
/// the first node's explanation.
async fn wait_for_one_view<K>(
    clusters: &[&Cluster],
    caches: &[&Cache<K, String>],
    probe: &K,
) -> ReadExplanation
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let converged = tokio::time::timeout(CONVERGE, async {
        loop {
            let mut explained = Vec::new();
            for cache in caches {
                if let Ok(read) = cache.explain(probe).await {
                    explained.push(read);
                }
            }
            let views: Vec<_> = explained
                .iter()
                .filter_map(|read| read.distributed.as_ref())
                .map(|read| (read.view_hash, read.owners.clone()))
                .collect();
            let known = clusters
                .iter()
                .all(|cluster| cluster.peers().len() + 1 == clusters.len());
            if known
                && views.len() == caches.len()
                && views
                    .iter()
                    .all(|view| view.1.len() == 2 && *view == views[0])
            {
                return explained.swap_remove(0);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    converged.unwrap_or_else(|_| {
        panic!(
            "the nodes did not agree on one view and two owners of the key in {} within {CONVERGE:?}",
            caches.first().map_or("", |cache| cache.name())
        )
    })
}

/// Runs `sundog-lens watch CLUSTER --once --json` through `seed` with the
/// `extra` arguments and waits for it to exit.
async fn lens_once(cluster: &str, seed: SocketAddr, extra: &[&str]) -> std::process::Output {
    let run = tokio::process::Command::new(env!("CARGO_BIN_EXE_sundog-lens"))
        .args(["watch", cluster, "--seed", &seed.to_string()])
        .args([
            "--bind",
            "127.0.0.1:0",
            "--once",
            "--json",
            "--settle",
            "1s",
        ])
        .args(extra)
        .kill_on_drop(true)
        .output();
    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("the binary exits within the bound")
        .expect("the binary runs")
}

/// The JSON object a successful run printed.
fn report_of(output: &std::process::Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "exit {:?}: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout is one JSON object")
}

/// Asserts that a run failed with exit 1, printed nothing on stdout and named
/// `needle` on stderr.
fn assert_refused(output: &std::process::Output, needle: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(output.stdout.is_empty(), "{:?}", output.stdout);
    assert!(stderr.contains(needle), "{needle:?} is not in: {stderr}");
}

/// Asserts that `section`, a run's `explain` object, is what `read`, a
/// node's own explanation of the same key, says.
fn assert_agrees(section: &serde_json::Value, cache: &str, read: &ReadExplanation) {
    let distributed = read.distributed.as_ref().expect("the cache is Distributed");
    assert_eq!(section["cache"], cache);
    assert_eq!(section["part"]["bucket"], read.part.bucket());
    assert_eq!(section["part"]["part"], read.part.part());
    assert_eq!(
        section["view"],
        format!("{:016x}", distributed.view_hash).as_str()
    );
    assert_eq!(section["eligible"], 3);
    assert_eq!(section["owners_per_part"], 2);
    assert_eq!(
        section["settled"], true,
        "the run waits until the view it names has held"
    );
    let owners: Vec<String> = section["owners"]
        .as_array()
        .expect("owners is a list")
        .iter()
        .map(|owner| owner["node"].as_str().expect("a node id").to_owned())
        .collect();
    assert_eq!(
        owners,
        distributed
            .owners
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        "the owners in fetch order"
    );
    for (index, owner) in section["owners"].as_array().unwrap().iter().enumerate() {
        assert_eq!(owner["rank"], index + 1);
        assert_eq!(owner["status"], "live");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_once_explain_agrees_with_cache_explain_and_refuses_a_wrong_cache() {
    let name = "lens-once-explain";
    let a = node(name, None).await;
    let b = node(name, Some(a.local_gossip_addr())).await;
    let c = node(name, Some(a.local_gossip_addr())).await;
    let clusters = [&a, &b, &c];
    let mut its: Vec<Cache<String, String>> = Vec::new();
    let mut ids: Vec<Cache<u32, String>> = Vec::new();
    for cluster in clusters {
        its.push(open(cluster, "it", Mode::distributed()).await);
        ids.push(open(cluster, "ids", Mode::distributed()).await);
    }
    let it_read =
        wait_for_one_view(&clusters, &its.iter().collect::<Vec<_>>(), &"k1".to_owned()).await;
    let ids_read = wait_for_one_view(&clusters, &ids.iter().collect::<Vec<_>>(), &7).await;
    let seed = a.local_gossip_addr();

    // A String key in a named cache.
    let output = lens_once(name, seed, &["--explain", "k1", "--cache", "it"]).await;
    let json = report_of(&output);
    let section = &json["explain"];
    assert_agrees(section, "it", &it_read);
    assert_eq!(
        section["key"],
        serde_json::json!({"kind": "str", "text": "k1", "hex": "026b31"})
    );
    let own = json["caches"]
        .as_array()
        .and_then(|caches| caches.iter().find(|cache| cache["name"] == "it"))
        .expect("the report lists it");
    assert_eq!(
        section["view"].as_str().map(|view| &view[..8]),
        own["ownership"]["view"].as_str(),
        "the explain view is the view the caches table shows"
    );

    // An integer key in the other cache.
    let output = lens_once(name, seed, &["--explain", "uint:7", "--cache", "ids"]).await;
    let json = report_of(&output);
    assert_agrees(&json["explain"], "ids", &ids_read);
    assert_eq!(
        json["explain"]["key"],
        serde_json::json!({"kind": "uint", "text": "7", "hex": "07"})
    );

    // Two Distributed caches are ranked: a run without a name asks for one.
    let output = lens_once(name, seed, &["--explain", "k1"]).await;
    assert_refused(&output, "--cache");
    // A cache the cluster does not advertise is named in the refusal.
    let output = lens_once(name, seed, &["--explain", "k1", "--cache", "nope"]).await;
    assert_refused(&output, "nope");

    // A plain run carries no explain object.
    let output = lens_once(name, seed, &[]).await;
    let json = report_of(&output);
    assert!(json.get("explain").is_none(), "{json}");
    assert_eq!(json["live"], 3);

    for cache in its {
        cache.close().await;
    }
    for cache in ids {
        cache.close().await;
    }
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}
