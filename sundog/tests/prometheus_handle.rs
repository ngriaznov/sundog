//! [`sundog::prometheus_handle`]'s recorder, in a test binary of its own so
//! it is the first recorder the process installs and always hands back a
//! handle.

#![cfg(all(feature = "prometheus", not(feature = "sim")))]

mod common;

use sundog::telemetry::DURATION_HISTOGRAMS;
use sundog::{Cluster, Mode};

#[tokio::test]
async fn prometheus_handle_buckets_the_crate_histograms_and_no_application_one() {
    let handle = sundog::prometheus_handle().expect("the first recorder in this binary installs");
    let cluster = Cluster::builder("it-prometheus-handle-buckets")
        .seeds(std::iter::empty())
        .config(common::fast_config())
        .build()
        .await
        .expect("cluster builds");
    let cache = cluster
        .cache::<u32, String>("handle-buckets")
        .mode(Mode::Local)
        .open()
        .await
        .expect("cache opens");
    cache.insert(1, "a".into()).await.expect("insert");
    std::thread::spawn(move || {
        for _ in 0..768 {
            assert_eq!(cache.get_sync(&1), Some("a".to_string()));
        }
    })
    .join()
    .expect("the reads finish");
    metrics::histogram!("app_request_duration_seconds").record(0.003);

    let body = handle.render();
    assert!(
        body.contains("# TYPE sundog_read_duration_seconds histogram"),
        "{body}"
    );
    assert!(
        body.lines().any(
            |line| line.starts_with("sundog_read_duration_seconds_bucket{")
                && line.contains("cache=\"handle-buckets\"")
                && line.contains("outcome=\"hit\"")
                && line.contains("le=\"0.000001\"")
        ),
        "the timed hits render in the first of LATENCY_BUCKETS; got body:\n{body}"
    );
    assert!(
        body.contains("# TYPE app_request_duration_seconds summary"),
        "an application histogram keeps the exporter's default rendering; got body:\n{body}"
    );
    assert!(
        !body.contains("app_request_duration_seconds_bucket"),
        "{body}"
    );
    assert!(DURATION_HISTOGRAMS.contains(&"sundog_read_duration_seconds"));

    cluster.shutdown().await;
}
