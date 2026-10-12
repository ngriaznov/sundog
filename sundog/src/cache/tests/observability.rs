//! Tests of the fetch span and the read metrics, and that `explain` leaves both untouched.

use super::*;

/// One closed span's name and recorded fields.
type ClosedSpan = (&'static str, HashMap<&'static str, String>);

/// Every closed span, in close order.
#[derive(Clone, Default)]
struct FetchSpans(Arc<std::sync::Mutex<Vec<ClosedSpan>>>);

/// One span's recorded fields.
#[derive(Default)]
struct SpanFields(HashMap<&'static str, String>);

impl tracing::field::Visit for SpanFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name(), value.to_owned());
    }
}

impl<S> tracing_subscriber::Layer<S> for FetchSpans
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            let mut fields = SpanFields::default();
            attrs.record(&mut fields);
            span.extensions_mut().insert(fields);
        }
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id)
            && let Some(fields) = span.extensions_mut().get_mut::<SpanFields>()
        {
            values.record(fields);
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if let Some(span) = ctx.span(&id)
            && let Some(fields) = span.extensions_mut().remove::<SpanFields>()
        {
            self.0
                .lock()
                .expect("spans lock")
                .push((span.name(), fields.0));
        }
    }
}

impl FetchSpans {
    /// The fields of the last closed span named `name`, as sorted
    /// `(field, value)` pairs; `None` when no such span closed.
    fn last_named(&self, name: &str) -> Option<Vec<(&'static str, String)>> {
        let spans = self.0.lock().expect("spans lock");
        let (_, fields) = spans.iter().rev().find(|(span, _)| *span == name)?;
        let mut fields: Vec<_> = fields
            .iter()
            .map(|(field, value)| (*field, value.clone()))
            .collect();
        fields.sort();
        Some(fields)
    }

    /// The last closed `sundog.fetch` span's fields.
    fn last(&self) -> Vec<(&'static str, String)> {
        self.last_named("sundog.fetch")
            .expect("a fetch span closed")
    }
}

#[tokio::test]
async fn a_fetch_span_names_the_owner_asked_its_outcome_and_its_attempts() {
    use tracing_subscriber::layer::SubscriberExt as _;
    let spans = FetchSpans::default();
    let _guard = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(spans.clone()),
    );
    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-fetch-span", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let not_owned: Vec<u32> = (0..10_000u32)
        .filter(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .take(2)
        .collect();
    let (present, absent) = (not_owned[0], not_owned[1]);
    wait_for_warm_owners(&nodes, present).await;
    wait_for_warm_owners(&nodes, absent).await;
    let local_key = (0..10_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a owns");
    let fields = |owner: NodeId, outcome: &str, attempts: u32| {
        let mut fields = vec![
            ("attempts", attempts.to_string()),
            ("cache", "cache-it-fetch-span".to_owned()),
            ("outcome", outcome.to_owned()),
            ("owner", owner.to_string()),
        ];
        fields.sort();
        fields
    };

    let first_owner = cache_a.owners_of(&present)[0];
    cache_a
        .insert(present, "here".to_owned())
        .await
        .expect("insert");
    wait_until(CONVERGE, "the first owner holds the key", async || {
        nodes
            .iter()
            .find(|(cluster, _)| cluster.node_id() == first_owner)
            .expect("the owner is a node")
            .1
            .get(&present)
            .await
            .is_some()
    })
    .await;
    assert_eq!(
        cache_a.fetch(&present).await.expect("fetch"),
        Some("here".to_owned())
    );
    assert_eq!(spans.last(), fields(first_owner, "remote", 1));

    let absent_owner = cache_a.owners_of(&absent)[0];
    assert_eq!(cache_a.fetch(&absent).await.expect("fetch"), None);
    assert_eq!(spans.last(), fields(absent_owner, "miss", 1));

    let part = PartId::of_key(&encode_key(&local_key).expect("u32 encodes"));
    wait_until(CONVERGE, "a holds the local key's part warm", async || {
        !cache_a.shard.is_cold_part(part)
    })
    .await;
    assert_eq!(cache_a.fetch(&local_key).await.expect("fetch"), None);
    assert_eq!(spans.last(), fields(a.node_id(), "local", 0));
    shut_down_all(nodes).await;
}

/// A subscriber that leaves the debug-level `sundog.fetch` span disabled
/// sees nothing of a fetch or an `expire` on the span the caller runs it
/// in, even when that span declares fields of the same names: neither
/// on a cache with no ownership view, nor through a `Mode::Distributed`
/// cache's owner requests.
#[tokio::test]
async fn a_disabled_fetch_span_records_nothing_on_the_callers_span() {
    use tracing::Instrument as _;
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    let spans = FetchSpans::default();
    let _guard = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(
            spans
                .clone()
                .with_filter(tracing::level_filters::LevelFilter::INFO),
        ),
    );
    let cluster = Cluster::builder("cache-it-fetch-span-disabled")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>("cache-it-fetch-span-disabled")
        .open()
        .await
        .expect("opens");
    let caller = tracing::info_span!(
        "caller",
        owner = tracing::field::Empty,
        outcome = tracing::field::Empty,
        attempts = tracing::field::Empty,
    );
    assert_eq!(
        cache
            .fetch(&1)
            .instrument(caller.clone())
            .await
            .expect("fetch"),
        None
    );
    drop(caller);
    assert_eq!(spans.last_named("caller"), Some(Vec::new()));
    assert_eq!(spans.last_named("sundog.fetch"), None);
    cluster.shutdown().await;

    let log = LoadLog::default();
    let nodes = three_loading_nodes(
        "cache-it-fetch-span-disabled-dist",
        Mode::distributed(),
        &log,
        None,
    )
    .await;
    let (a, cache_a) = &nodes[0];
    let remote = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let local = (0..10_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a owns");
    // A fetch answers `FetchUnavailable` while every owner is cold.
    wait_for_warm_owners(&nodes, remote).await;
    wait_for_warm_owners(&nodes, local).await;
    cache_a
        .insert(remote, "there".to_owned())
        .await
        .expect("insert");
    let caller = tracing::info_span!(
        "caller",
        owner = tracing::field::Empty,
        outcome = tracing::field::Empty,
        attempts = tracing::field::Empty,
    );
    async {
        cache_a.fetch(&remote).await.expect("remote fetch");
        cache_a.fetch(&local).await.expect("local fetch");
        cache_a
            .expire(&remote, Duration::from_secs(60))
            .await
            .expect("expire");
    }
    .instrument(caller.clone())
    .await;
    drop(caller);
    assert_eq!(spans.last_named("caller"), Some(Vec::new()));
    assert_eq!(spans.last_named("sundog.fetch"), None);
    shut_down_all(nodes).await;
}

/// Per `(name, cache, outcome)` metric, a counter's value or a
/// histogram's sample count, drained from `snapshotter`.
fn drained(
    snapshotter: &metrics_util::debugging::Snapshotter,
) -> std::collections::BTreeMap<(String, String, String), u64> {
    use metrics_util::debugging::DebugValue;
    let mut values = std::collections::BTreeMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let label = |name: &str| {
            key.key()
                .labels()
                .find(|label| label.key() == name)
                .map(|label| label.value().to_owned())
                .unwrap_or_default()
        };
        let n = match value {
            DebugValue::Counter(n) => n,
            DebugValue::Histogram(samples) => samples.len() as u64,
            DebugValue::Gauge(_) => continue,
        };
        values.insert(
            (
                key.key().name().to_owned(),
                label("cache"),
                label("outcome"),
            ),
            n,
        );
    }
    values
}

/// [`drained`]'s map: per `(name, cache, outcome)`, a counter's value or a
/// histogram's sample count.
type Series = std::collections::BTreeMap<(String, String, String), u64>;

/// The series a read moves: hit and miss counts, sampled read timings,
/// fetch counts and timings, and loader calls.
const READ_SERIES: [&str; 6] = [
    "sundog_cache_hits_total",
    "sundog_cache_misses_total",
    "sundog_read_duration_seconds",
    "sundog_fetch_total",
    "sundog_fetch_duration_seconds",
    "sundog_loader_calls_total",
];

/// [`drained`] restricted to [`READ_SERIES`]. A call drains every series,
/// so each reports what ran since the one before.
fn read_series(snapshotter: &metrics_util::debugging::Snapshotter) -> Series {
    drained(snapshotter)
        .into_iter()
        .filter(|((name, ..), _)| READ_SERIES.contains(&name.as_str()))
        .collect()
}

/// The value of the series `name` of `cache` under `outcome` in `series`,
/// 0 when it has none.
fn series_count(series: &Series, name: &str, cache: &str, outcome: &str) -> u64 {
    series
        .get(&(name.to_owned(), cache.to_owned(), outcome.to_owned()))
        .copied()
        .unwrap_or(0)
}

/// Each call under its own cache label, so the test sees which calls
/// are timed, not only how many per outcome.
#[test]
fn only_a_fetch_that_asked_an_owner_is_timed() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let calls = [
        ("local", 0),
        ("local", 2),
        ("remote", 1),
        ("miss", 1),
        ("error", 0),
        ("error", 3),
    ];
    metrics::with_local_recorder(&recorder, || {
        let started = std::time::Instant::now();
        for (index, (outcome, attempts)) in calls.into_iter().enumerate() {
            let cache = format!("call{index}");
            record_fetch_outcome(&tracing::Span::none(), &cache, outcome, attempts, started);
        }
    });
    let values = drained(&snapshotter);
    for (index, (outcome, attempts)) in calls.into_iter().enumerate() {
        let key = |name: &str| (name.to_owned(), format!("call{index}"), outcome.to_owned());
        assert_eq!(
            values.get(&key("sundog_fetch_total")),
            Some(&1),
            "{outcome} with {attempts} owner requests is counted"
        );
        assert_eq!(
            values.get(&key("sundog_fetch_duration_seconds")),
            (attempts > 0).then_some(&1),
            "{outcome} with {attempts} owner requests is timed iff it asked an owner"
        );
    }
}

/// Through `Cache::fetch`: a fetch answered without asking an owner is
/// counted and not timed, on a cache with no ownership view and on a
/// `Mode::Distributed` key this node owns warm; a fetch that asks an
/// owner is counted and timed.
#[tokio::test]
async fn a_fetch_is_timed_only_when_it_asks_an_owner() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let fetched = |cache: &str, outcome: &str| {
        let values = drained(&snapshotter);
        let get = |name: &str| {
            values
                .get(&(name.to_owned(), cache.to_owned(), outcome.to_owned()))
                .copied()
        };
        (
            get("sundog_fetch_total"),
            get("sundog_fetch_duration_seconds"),
        )
    };

    let cluster = Cluster::builder("cache-it-fetch-timed")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let plain = cluster
        .cache::<u32, String>("fetch-timed-plain")
        .open()
        .await
        .expect("opens");
    assert_eq!(plain.fetch(&1).await.expect("fetch"), None);
    assert_eq!(fetched("fetch-timed-plain", "local"), (Some(1), None));
    cluster.shutdown().await;

    let log = LoadLog::default();
    let nodes = three_loading_nodes("cache-it-fetch-timed", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let local = (0..10_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a owns");
    let part = PartId::of_key(&encode_key(&local).expect("u32 encodes"));
    wait_until(CONVERGE, "a holds the owned key's part warm", async || {
        !cache_a.shard.is_cold_part(part)
    })
    .await;
    let remote = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    wait_for_warm_owners(&nodes, remote).await;
    let _ = drained(&snapshotter);
    assert_eq!(cache_a.fetch(&local).await.expect("fetch"), None);
    assert_eq!(fetched("cache-it-fetch-timed", "local"), (Some(1), None));
    assert_eq!(cache_a.fetch(&remote).await.expect("fetch"), None);
    assert_eq!(fetched("cache-it-fetch-timed", "miss"), (Some(1), Some(1)));
    shut_down_all(nodes).await;
}

/// `explain` of a `Mode::Local` cache counts no read: over a live key, a
/// deleted key and a key never written it moves no hit, miss, read
/// timing, fetch or loader series and creates none, where a `get` and a
/// `fetch` of the same cache move them.
#[tokio::test]
async fn explain_does_not_move_a_read_metric_on_a_local_cache() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let name = "explain-quiet-local";
    let cluster = Cluster::builder("cache-it-explain-quiet-local")
        .seeds(std::iter::empty())
        .config(loopback_config())
        .build()
        .await
        .expect("node builds");
    let cache = cluster
        .cache::<u32, String>(name)
        .mode(Mode::Local)
        .loader(|key: u32| async move { Ok::<_, SourceDown>(Some(format!("v{key}"))) })
        .open()
        .await
        .expect("opens");
    cache.insert(1, "live".to_string()).await.expect("insert");
    cache.insert(2, "gone".to_string()).await.expect("insert");
    cache.remove(&2).await.expect("remove");

    assert_eq!(cache.get(&1).await, Some("live".to_string()));
    assert_eq!(cache.get(&3).await, None);
    assert_eq!(
        cache.fetch(&1).await.expect("fetch"),
        Some("live".to_string())
    );
    let read = read_series(&snapshotter);
    assert_eq!(series_count(&read, "sundog_cache_hits_total", name, ""), 2);
    assert_eq!(
        series_count(&read, "sundog_cache_misses_total", name, ""),
        1
    );
    assert_eq!(series_count(&read, "sundog_fetch_total", name, "local"), 1);

    let before = read_series(&snapshotter);
    for (key, returns_value) in [(1, true), (2, false), (3, false)] {
        let explanation = cache.explain(&key).await.expect("explains");
        assert_eq!(
            explanation.source.returns_value(),
            returns_value,
            "{explanation}"
        );
        assert!(explanation.distributed.is_none());
    }
    assert_eq!(
        read_series(&snapshotter),
        before,
        "explain moves no read series and creates none"
    );

    assert_eq!(cache.get(&1).await, Some("live".to_string()));
    assert_eq!(
        series_count(
            &read_series(&snapshotter),
            "sundog_cache_hits_total",
            name,
            ""
        ),
        1,
        "a read after the explanations counts as it always does"
    );
    cluster.shutdown().await;
}

/// `explain` of a `Mode::Distributed` cache asks the owners and still
/// counts no read: over a key this node owns and a key it does not, each
/// stored and never written, asked of every node, it moves no hit, miss,
/// read timing, fetch, fetch timing or loader series, where the `fetch`
/// of the same four keys moves them.
#[tokio::test]
async fn explain_does_not_move_a_read_metric_on_a_distributed_cache() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let name = "cache-it-explain-quiet";
    let log = LoadLog::default();
    let nodes = three_loading_nodes(name, Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let owned: Vec<u32> = (0..10_000u32)
        .filter(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .take(2)
        .collect();
    let remote: Vec<u32> = (0..10_000u32)
        .filter(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .take(2)
        .collect();
    let [owned_stored, owned_absent] = owned[..] else {
        panic!("a owns some parts of a three-node view");
    };
    let [remote_stored, remote_absent] = remote[..] else {
        panic!("a does not own some parts of a three-node view");
    };
    let keys = [owned_stored, owned_absent, remote_stored, remote_absent];
    let is_stored = |key: u32| key == owned_stored || key == remote_stored;
    for key in keys {
        wait_for_warm_owners(&nodes, key).await;
    }
    for key in [owned_stored, remote_stored] {
        cache_a
            .insert(key, format!("v{key}"))
            .await
            .expect("insert");
        let holders: Vec<&Cache<u32, String>> = cache_a
            .owners_of(&key)
            .into_iter()
            .map(|owner| cache_of(&nodes, owner))
            .collect();
        wait_for_local_record(&holders, key, LocalRecord::is_live).await;
    }

    for key in keys {
        assert_eq!(
            cache_a.fetch(&key).await.expect("fetch"),
            is_stored(key).then(|| format!("v{key}"))
        );
    }
    let fetched = read_series(&snapshotter);
    let count = |series: &str, outcome: &str| series_count(&fetched, series, name, outcome);
    assert_eq!(count("sundog_fetch_total", "remote"), 1);
    assert_eq!(count("sundog_fetch_total", "miss"), 1);
    assert_eq!(count("sundog_fetch_total", "local"), 2);
    assert_eq!(count("sundog_fetch_duration_seconds", "remote"), 1);
    assert_eq!(count("sundog_fetch_duration_seconds", "miss"), 1);
    assert_eq!(count("sundog_fetch_duration_seconds", "local"), 0);
    assert_eq!(count("sundog_cache_hits_total", ""), 1);
    assert_eq!(count("sundog_cache_misses_total", ""), 1);

    let before = read_series(&snapshotter);
    for (_, cache) in &nodes {
        for key in keys {
            let explanation = cache.explain(&key).await.expect("explains");
            assert_eq!(
                explanation.source.returns_value(),
                is_stored(key),
                "{explanation}"
            );
        }
    }
    let asked = cache_a.explain(&remote_stored).await.expect("explains");
    let read = asked
        .distributed
        .as_ref()
        .expect("a Distributed cache explains its fetch");
    assert_eq!(read.probes.len(), 2, "explain asks both owners: {asked}");
    assert_eq!(
        read_series(&snapshotter),
        before,
        "explain moves no read series and creates none"
    );
    assert!(
        log.lock().expect("log lock").is_empty(),
        "explain calls no loader: {:?}",
        log.lock().expect("log lock")
    );
    shut_down_all(nodes).await;
}

/// `explain` of a key the source holds and no cache stores calls no
/// loader and stores nothing: every node still misses the key afterwards,
/// where `Cache::load` of it calls the first owner's loader once and the
/// owners keep the value.
#[tokio::test]
async fn explain_does_not_load_or_fill_a_missing_key() {
    let log = LoadLog::default();
    let nodes =
        three_loading_nodes("cache-it-explain-no-load", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let key = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    let owners = cache_a.owners_of(&key);
    wait_until(
        CONVERGE,
        "every node agrees on the key's owners",
        async || {
            nodes
                .iter()
                .all(|(_, cache)| cache.owners_of(&key) == owners)
        },
    )
    .await;
    wait_for_warm_owners(&nodes, key).await;

    for (_, cache) in &nodes {
        let explanation = cache.explain(&key).await.expect("explains");
        assert_eq!(explanation.local, LocalRecord::Absent);
        assert!(
            !explanation.source.returns_value(),
            "no cache stores the key: {explanation}"
        );
    }
    assert!(
        log.lock().expect("log lock").is_empty(),
        "explain calls no loader: {:?}",
        log.lock().expect("log lock")
    );
    for (_, cache) in &nodes {
        assert_eq!(cache.get(&key).await, None, "explain stores nothing");
        assert_eq!(cache.keys(), Vec::<u32>::new(), "explain fills no key");
    }
    assert_eq!(cache_a.fetch(&key).await.expect("fetch"), None);
    assert!(
        log.lock().expect("log lock").is_empty(),
        "a fetch loads nothing either"
    );

    assert_eq!(
        cache_a.load(&key).await.expect("load"),
        Some(format!("v{key}"))
    );
    assert_eq!(
        log.lock().expect("log lock").clone(),
        vec![(owners[0], vec![key])],
        "the key's first owner runs the one load"
    );
    wait_until(CONVERGE, "both owners hold the loaded value", async || {
        let mut held = 0;
        for (cluster, cache) in &nodes {
            if owners.contains(&cluster.node_id()) && cache.get(&key).await.is_some() {
                held += 1;
            }
        }
        held == 2
    })
    .await;
    shut_down_all(nodes).await;
}

/// `explain` opens no `sundog.fetch` span: not for a key this node owns,
/// a key it asks the owners for, or a cache with no ownership view,
/// where the `fetch` of the same three keys opens one each.
#[tokio::test]
async fn explain_does_not_open_a_fetch_span() {
    use tracing_subscriber::layer::SubscriberExt as _;
    let spans = FetchSpans::default();
    let _guard = crate::cluster::test_support::scoped_subscriber(
        tracing_subscriber::registry().with(spans.clone()),
    );
    let log = LoadLog::default();
    let nodes =
        three_loading_nodes("cache-it-explain-no-span", Mode::distributed(), &log, None).await;
    let (a, cache_a) = &nodes[0];
    let plain = a
        .cache::<u32, String>("cache-it-explain-no-span-plain")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    let local = (0..10_000u32)
        .find(|key| cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a owns");
    let remote = (0..10_000u32)
        .find(|key| !cache_a.owners_of(key).contains(&a.node_id()))
        .expect("a key a does not own");
    wait_for_warm_owners(&nodes, local).await;
    wait_for_warm_owners(&nodes, remote).await;
    let fetch_spans = || {
        spans
            .0
            .lock()
            .expect("spans lock")
            .iter()
            .filter(|(span, _)| *span == "sundog.fetch")
            .count()
    };

    let before = fetch_spans();
    for key in [local, remote] {
        cache_a.explain(&key).await.expect("explains");
    }
    plain.explain(&1).await.expect("explains");
    assert_eq!(fetch_spans(), before, "explain opens no fetch span");

    for key in [local, remote] {
        cache_a.fetch(&key).await.expect("fetch");
    }
    plain.fetch(&1).await.expect("fetch");
    assert_eq!(
        fetch_spans(),
        before + 3,
        "a fetch opens one span, on this node and on its owners alike"
    );
    shut_down_all(nodes).await;
}
