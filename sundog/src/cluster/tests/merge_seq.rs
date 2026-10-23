//! `Cluster::merge_seq_counter`: one sequence counter per cache name, kept for the cluster's life.

use std::sync::atomic::Ordering;

use super::*;

#[tokio::test]
async fn one_name_gets_one_counter_and_each_name_its_own() {
    let cluster = solo_cluster("cluster-it-merge-seq").await;

    let orders = cluster.merge_seq_counter("orders");
    let users = cluster.merge_seq_counter("users");

    assert!(
        Arc::ptr_eq(&orders, &cluster.merge_seq_counter("orders")),
        "every ask for a name gets the same counter"
    );
    assert!(
        !Arc::ptr_eq(&orders, &users),
        "another name has a counter of its own"
    );
    assert_eq!(orders.load(Ordering::Relaxed), 0, "a counter starts unused");
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_counter_outlives_the_cache_that_drew_from_it() {
    let name = SmolStr::new("users");
    let cluster = solo_cluster("cluster-it-merge-seq-forget").await;
    let cache = cluster
        .cache::<u32, u64>("users")
        .mode(Mode::Local)
        .open()
        .await
        .expect("opens");
    cache
        .merge_sequenced(1, |_, seq| seq)
        .await
        .expect("a number is drawn");
    let counter = cluster.merge_seq_counter("users");
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    cache.close().await;

    assert!(
        crate::cluster::test_support::registry_entry(&cluster, &name).is_none(),
        "the close freed the name"
    );
    assert!(
        Arc::ptr_eq(&counter, &cluster.merge_seq_counter("users")),
        "the freed name keeps its counter"
    );
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    cluster.shutdown().await;
}
