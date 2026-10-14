//! `Cluster::forget_cache` acts only on the shard the registry holds under the name.

use super::*;
use crate::cluster::test_support::{registered_shard, registry_entry};
use crate::node::NodeId;

fn shard() -> Arc<Shard<u32, String>> {
    Arc::new(Shard::new(
        SmolStr::new("users"),
        Mode::Replicated,
        NodeId::from(1),
        10_000,
        None,
        None,
    ))
}

#[test]
fn registry_holds_names_only_the_shard_registered_under_the_name() {
    let (first, second) = (shard(), shard());
    let registry: HashMap<SmolStr, Arc<dyn ShardOps>> = HashMap::from([(
        SmolStr::new("users"),
        Arc::clone(&first) as Arc<dyn ShardOps>,
    )]);

    assert!(registry_holds(&registry, "users", &first));
    assert!(
        registry_holds(&registry, "users", &Arc::clone(&first)),
        "a clone of the Arc is the same shard"
    );
    let erased: Arc<dyn ShardOps> = first;
    assert!(
        registry_holds(&registry, "users", &erased),
        "the same shard held as a trait object"
    );
    assert!(
        !registry_holds(&registry, "users", &second),
        "another shard of the same type and name is not the registered one"
    );
    assert!(
        !registry_holds(&registry, "orders", &erased),
        "a name nothing is registered under holds nothing"
    );
    assert!(!registry_holds(&HashMap::new(), "users", &erased));
}

#[tokio::test]
async fn forget_cache_forgets_the_registered_shard_and_nothing_for_another() {
    let name = SmolStr::new("users");
    let cluster = solo_cluster("cluster-it-forget-guard").await;
    let _cache = cluster
        .cache::<u32, String>("users")
        .mode(Mode::Replicated)
        .loader(
            |key: u32| async move { Ok::<_, std::convert::Infallible>(Some(format!("v{key}"))) },
        )
        .open()
        .await
        .expect("opens");
    let registered = registered_shard(&cluster, &name);
    let stranger = shard();
    let listed = || {
        cluster
            .health()
            .caches
            .iter()
            .any(|cache| cache.name == name)
    };
    assert!(listed() && cluster.is_warm(&name));

    assert!(
        !cluster.forget_cache("users", &stranger),
        "a shard the registry does not hold under the name forgets nothing"
    );
    assert!(registry_entry(&cluster, &name).is_some());
    assert!(listed(), "its mode stays");
    assert!(cluster.is_warm(&name), "its warmth stays");

    assert!(cluster.forget_cache("users", &registered));
    assert!(registry_entry(&cluster, &name).is_none());
    assert!(!listed(), "its mode is gone");
    assert!(!cluster.is_warm(&name), "its warmth is gone");

    assert!(
        !cluster.forget_cache("users", &registered),
        "forgetting twice changes nothing"
    );
    cluster.shutdown().await;
}
