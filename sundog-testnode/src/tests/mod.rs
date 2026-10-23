//! Unit tests for the test node binary: environment knobs, command dispatch, side caches, explain replies and spill settings.

use std::collections::BTreeMap;

use sundog::Hlc;
use sundog::NodeName;
use sundog::membership::Peer;
use sundog::observe::Member;

use super::*;

#[test]
fn parse_usize_override_reads_a_valid_number() {
    assert_eq!(parse_usize_override(Some("512")), Some(512));
    assert_eq!(parse_usize_override(Some("0")), Some(0));
}

#[test]
fn parse_usize_override_is_none_for_absent_or_unparsable_input() {
    assert_eq!(parse_usize_override(None), None);
    assert_eq!(parse_usize_override(Some("")), None);
    assert_eq!(parse_usize_override(Some("not-a-number")), None);
    assert_eq!(
        parse_usize_override(Some("-1")),
        None,
        "usize rejects a negative value"
    );
}

#[cfg(feature = "spill")]
#[test]
fn parse_bool_override_reads_true_and_defaults_everything_else_to_false() {
    assert!(parse_bool_override(Some("true")));
    assert!(!parse_bool_override(Some("false")));
    assert!(!parse_bool_override(None));
    assert!(!parse_bool_override(Some("")));
    assert!(!parse_bool_override(Some("TRUE")));
    assert!(!parse_bool_override(Some("not-a-bool")));
}

#[test]
fn log_filter_directive_defaults_to_warn_when_unset() {
    assert_eq!(log_filter_directive(None), "warn");
}

#[test]
fn log_filter_directive_uses_a_valid_value_verbatim() {
    assert_eq!(log_filter_directive(Some("info")), "info");
    assert_eq!(
        log_filter_directive(Some("sundog=debug,warn")),
        "sundog=debug,warn"
    );
}

#[test]
fn log_filter_directive_falls_back_to_warn_for_an_invalid_value() {
    assert_eq!(log_filter_directive(Some("sundog=nonsense_level")), "warn");
}

#[test]
fn parse_arg_reads_a_valid_number() {
    assert_eq!(parse_arg::<u32>(Some("42")), Some(42));
    assert_eq!(parse_arg::<usize>(Some("0")), Some(0));
}

#[test]
fn parse_arg_is_none_for_absent_or_unparsable_input() {
    assert_eq!(parse_arg::<u32>(None), None);
    assert_eq!(parse_arg::<u32>(Some("")), None);
    assert_eq!(parse_arg::<u32>(Some("not-a-number")), None);
}

#[test]
fn ok_reply_renders_ok_for_success() {
    assert_eq!(ok_reply(Ok::<(), String>(())), "ok");
}

#[test]
fn ok_reply_renders_the_error_for_failure() {
    assert_eq!(ok_reply(Err("boom")), "err boom");
}

#[test]
fn big_value_is_deterministic_and_regenerable() {
    let a = big_value(7, 20);
    let b = big_value(7, 20);
    assert_eq!(a, b);
    assert_eq!(a.len(), 20);
    assert!(a.starts_with("00000007"));
}

#[test]
fn verdict_matches_only_the_exact_regenerated_value() {
    let value = big_value(3, 16);
    assert_eq!(verdict(&value, 3, 16), "ok");
    assert_ne!(verdict("wrong", 3, 16), "ok");
}

#[test]
fn parse_u64_override_reads_a_valid_number() {
    assert_eq!(parse_u64_override(Some("1073741824")), Some(1_073_741_824));
    assert_eq!(parse_u64_override(Some("0")), Some(0));
}

#[test]
fn parse_u64_override_is_none_for_absent_or_unparsable_input() {
    assert_eq!(parse_u64_override(None), None);
    assert_eq!(parse_u64_override(Some("")), None);
    assert_eq!(parse_u64_override(Some("not-a-number")), None);
    assert_eq!(
        parse_u64_override(Some("-1")),
        None,
        "u64 rejects a negative value"
    );
}

#[test]
fn bind_ip_from_env_defaults_to_every_interface_and_reads_an_address() {
    assert_eq!(bind_ip_from_env(None), Ok(IpAddr::from([0, 0, 0, 0])));
    assert_eq!(
        bind_ip_from_env(Some("127.0.0.42")),
        Ok(IpAddr::from([127, 0, 0, 42]))
    );
    assert!(bind_ip_from_env(Some("n42")).is_err());
}

#[test]
fn port_from_env_keeps_the_default_when_unset() {
    assert_eq!(port_from_env("P", None, GOSSIP_PORT), Ok(7946));
    assert_eq!(port_from_env("P", None, CONTROL_PORT), Ok(8080));
    assert_eq!(port_from_env("P", None, 9090), Ok(9090));
}

#[test]
fn port_from_env_reads_a_port_number() {
    assert_eq!(port_from_env("P", Some("7947"), GOSSIP_PORT), Ok(7947));
    assert_eq!(port_from_env("P", Some("0"), CONTROL_PORT), Ok(0));
    assert_eq!(port_from_env("P", Some("65535"), 9090), Ok(65535));
}

#[test]
fn port_from_env_rejects_what_is_not_a_port_and_names_the_variable() {
    for bad in ["", "n1", "-1", "65536", "7946 ", "0x1f", "80.80"] {
        let error =
            port_from_env("SUNDOG_TESTNODE_GOSSIP_PORT", Some(bad), GOSSIP_PORT).expect_err(bad);
        assert!(error.contains("SUNDOG_TESTNODE_GOSSIP_PORT"), "{error}");
        assert!(error.contains(&format!("{bad:?}")), "{error}");
    }
}

#[test]
fn port_env_reads_the_process_environment() {
    // A name no other test or process sets.
    const NAME: &str = "SUNDOG_TESTNODE_PORT_ENV_TEST";
    assert_eq!(port_env(NAME, 4242), Ok(4242));
}

#[test]
fn side_caches_from_env_is_on_unless_off() {
    assert_eq!(side_caches_from_env(None), Ok(true));
    assert_eq!(side_caches_from_env(Some("on")), Ok(true));
    assert_eq!(side_caches_from_env(Some("off")), Ok(false));
    assert!(side_caches_from_env(Some("no")).is_err());
}

#[test]
fn mode_from_env_defaults_to_replicated() {
    assert_eq!(mode_from_env(None, None), Ok(Mode::Replicated));
}

#[test]
fn mode_from_env_replicated_is_explicit_too() {
    assert_eq!(
        mode_from_env(Some("replicated"), None),
        Ok(Mode::Replicated)
    );
}

#[test]
fn mode_from_env_reads_invalidation() {
    assert_eq!(
        mode_from_env(Some("invalidation"), None),
        Ok(Mode::Invalidation)
    );
}

#[test]
fn loading_from_env_reads_each_option_and_rejects_garbage() {
    assert_eq!(
        loading_from_env(None, None, None),
        Ok(Loading {
            loader_delay: None,
            ttl: None,
            refresh_ahead: None,
        })
    );
    assert_eq!(
        loading_from_env(Some("250"), Some("2000"), Some("0.5")),
        Ok(Loading {
            loader_delay: Some(Duration::from_millis(250)),
            ttl: Some(Duration::from_secs(2)),
            refresh_ahead: Some(0.5),
        })
    );
    assert!(loading_from_env(Some("soon"), None, None).is_err());
    assert!(loading_from_env(None, Some("-1"), None).is_err());
    assert!(loading_from_env(None, None, Some("most")).is_err());
}

#[test]
fn mode_from_env_distributed_defaults_to_two_owners() {
    assert_eq!(
        mode_from_env(Some("distributed"), None),
        Ok(Mode::distributed())
    );
}

#[test]
fn mode_from_env_distributed_takes_an_explicit_owners_count() {
    assert_eq!(
        mode_from_env(Some("distributed"), Some(4)),
        Ok(Mode::Distributed {
            owners: std::num::NonZeroU8::new(4).expect("4 is nonzero")
        })
    );
}

#[test]
fn mode_from_env_rejects_a_single_owner() {
    let error = mode_from_env(Some("distributed"), Some(1))
        .expect_err("one owner is a lost bucket the instant it leaves");
    assert!(
        error.contains("at least 2"),
        "error should explain the minimum: {error:?}"
    );
}

#[test]
fn mode_from_env_rejects_an_unknown_mode() {
    let error = mode_from_env(Some("gossiped"), None)
        .expect_err("an unrecognized mode string fails startup");
    assert!(
        error.contains("gossiped"),
        "error should name the bad value: {error:?}"
    );
}

#[test]
fn byte_weight_sums_key_and_value_utf8_lengths() {
    assert_eq!(byte_weight("k0", "v0"), 4);
    assert_eq!(byte_weight("", ""), 0);
    assert_eq!(byte_weight("abc", "de"), 5);
}

#[test]
fn capacity_cap_from_env_is_none_when_neither_knob_is_set() {
    assert_eq!(capacity_cap_from_env(None, None), None);
}

#[test]
fn capacity_cap_from_env_reads_a_byte_budget() {
    assert_eq!(
        capacity_cap_from_env(Some(4096), None),
        Some(CapacityCap::Bytes(4096))
    );
}

#[test]
fn capacity_cap_from_env_reads_an_entry_count() {
    assert_eq!(
        capacity_cap_from_env(None, Some(10)),
        Some(CapacityCap::Entries(10))
    );
}

#[test]
fn capacity_cap_from_env_prefers_the_byte_budget_when_both_are_set() {
    assert_eq!(
        capacity_cap_from_env(Some(4096), Some(10)),
        Some(CapacityCap::Bytes(4096)),
        "the older byte-denominated knob wins over the entry-count knob"
    );
}

#[tokio::test]
async fn apply_capacity_cap_entries_reaches_the_opened_caches_stripes() {
    let loopback = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0));
    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = loopback;
        c.data_bind_addr = loopback;
    });
    let cluster = Cluster::builder("testnode-apply-capacity-cap")
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("a single-node cluster builds with no seeds");

    let hint = 500u64;
    let expected = usize::try_from(hint.div_ceil(sundog::store::BUCKET_COUNT as u64))
        .expect("hint fits usize");
    let builder = apply_capacity_cap(
        cluster
            .cache::<String, String>(CACHE_NAME)
            .mode(Mode::Local),
        Some(CapacityCap::Entries(hint)),
    );
    let cache = builder.open().await.expect("open succeeds");
    assert_eq!(
        cache.stripe_capacities(),
        vec![expected; sundog::store::BUCKET_COUNT],
        "an entry-count cap reaches the opened cache's stripes through capacity_hint, \
         backing open_it_cache's forwarding of SUNDOG_TESTNODE_MAX_ENTRIES"
    );

    cache.close().await;
    cluster.shutdown().await;
}

#[test]
fn resolver_kind_from_env_defaults_to_lww() {
    assert_eq!(resolver_kind_from_env(None), Ok(ResolverKind::Lww));
    assert_eq!(resolver_kind_from_env(Some("lww")), Ok(ResolverKind::Lww));
}

#[test]
fn resolver_kind_from_env_reads_sum_counter() {
    assert_eq!(
        resolver_kind_from_env(Some("sum_counter")),
        Ok(ResolverKind::SumCounter)
    );
}

#[test]
fn resolver_kind_from_env_rejects_an_unknown_resolver() {
    let error = resolver_kind_from_env(Some("crdt"))
        .expect_err("an unrecognized resolver string fails startup");
    assert!(
        error.contains("crdt"),
        "error should name the bad value: {error:?}"
    );
}

#[test]
fn decode_counter_parses_a_postcard_encoded_decimal_string() {
    let bytes = postcard::to_stdvec(&"42".to_string()).expect("encodes");
    assert_eq!(decode_counter(&bytes), Some(42));
}

#[test]
fn decode_counter_is_none_for_non_numeric_or_undecodable_bytes() {
    let non_numeric = postcard::to_stdvec(&"not-a-number".to_string()).expect("encodes");
    assert_eq!(decode_counter(&non_numeric), None);
    assert_eq!(
        decode_counter(b"\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
        None
    );
}

#[test]
fn merge_counter_bytes_sums_two_decodable_counters() {
    let a = postcard::to_stdvec(&"3".to_string()).expect("encodes");
    let b = postcard::to_stdvec(&"5".to_string()).expect("encodes");
    let merged = merge_counter_bytes(&a, &b).expect("both sides decode");
    assert_eq!(
        postcard::from_bytes::<String>(&merged).expect("merged bytes decode"),
        "8"
    );
}

#[test]
fn merge_counter_bytes_is_none_when_either_side_is_not_a_counter() {
    let counter = postcard::to_stdvec(&"3".to_string()).expect("encodes");
    let not_a_counter = postcard::to_stdvec(&"hello".to_string()).expect("encodes");
    assert_eq!(merge_counter_bytes(&counter, &not_a_counter), None);
    assert_eq!(merge_counter_bytes(&not_a_counter, &counter), None);
}

/// Postcard-encodes `a_value`/`b_value` as the decimal-string counters
/// [`SumCounterResolver::winner`]'s tests below build `RecordView`s from.
fn counter_bytes(a_value: &str, b_value: &str) -> (Vec<u8>, Vec<u8>) {
    (
        postcard::to_stdvec(&a_value.to_string()).expect("encodes"),
        postcard::to_stdvec(&b_value.to_string()).expect("encodes"),
    )
}

fn hlc_at(wall_ms: u64) -> Hlc {
    Hlc {
        wall_ms,
        logical: 0,
        node: NodeId::random(),
    }
}

#[test]
fn sum_counter_resolver_merges_two_decodable_counters() {
    let (a_bytes, b_bytes) = counter_bytes("3", "5");
    let a = RecordView {
        value: Some(&a_bytes),
        ver: hlc_at(1),
        expires_at_ms: None,
    };
    let b = RecordView {
        value: Some(&b_bytes),
        ver: hlc_at(2),
        expires_at_ms: None,
    };
    match SumCounterResolver.merge(b"key", a, b) {
        Some(Merged {
            value,
            expires_at_ms,
        }) => {
            assert_eq!(
                postcard::from_bytes::<String>(&value).expect("merged bytes decode"),
                "8"
            );
            assert_eq!(expires_at_ms, None);
        }
        other => panic!("expected Some(Merged), got {other:?}"),
    }
}

#[test]
fn merges_is_true() {
    assert!(
        SumCounterResolver.merges(),
        "SumCounterResolver's merge can return Some, so it must advertise merges()"
    );
}

#[test]
fn sum_counter_resolver_falls_back_to_hlc_order_on_a_tombstone() {
    let (_, b_bytes) = counter_bytes("3", "5");
    let a = RecordView {
        value: None,
        ver: hlc_at(1),
        expires_at_ms: None,
    };
    let b = RecordView {
        value: Some(&b_bytes),
        ver: hlc_at(2),
        expires_at_ms: None,
    };
    assert_eq!(SumCounterResolver.winner(b"key", a, b), Winner::B);
    assert_eq!(SumCounterResolver.winner(b"key", b, a), Winner::A);
}

#[test]
fn sum_counter_resolver_falls_back_to_hlc_order_on_a_non_numeric_value() {
    let a_bytes = postcard::to_stdvec(&"not-a-number".to_string()).expect("encodes");
    let (_, b_bytes) = counter_bytes("3", "5");
    let a = RecordView {
        value: Some(&a_bytes),
        ver: hlc_at(1),
        expires_at_ms: None,
    };
    let b = RecordView {
        value: Some(&b_bytes),
        ver: hlc_at(2),
        expires_at_ms: None,
    };
    assert_eq!(SumCounterResolver.winner(b"key", a, b), Winner::B);
}

fn writer(node: u64, incarnation: u64) -> WriterId {
    WriterId::new(NodeId::from(node), incarnation)
}

#[test]
fn pn_fill_entries_builds_one_local_delta_per_index() {
    let writer = writer(1, 100);
    let entries = pn_fill_entries(writer, 3);
    assert_eq!(entries.len(), 3);
    for (i, (key, counter)) in entries.iter().enumerate() {
        assert_eq!(*key, format!("pn{i}"));
        assert_eq!(counter.value(), 1);
    }
}

#[test]
fn pn_fill_entries_is_empty_for_a_zero_count() {
    let entries = pn_fill_entries(writer(1, 100), 0);
    assert!(entries.is_empty(), "{entries:?}");
}

#[test]
fn pn_get_reply_reports_the_counters_value() {
    let counter = PnCounter::local_delta(writer(1, 100), 5);
    assert_eq!(pn_get_reply(Some(&counter)), "val 5");
}

#[test]
fn pn_get_reply_is_none_for_an_absent_key() {
    assert_eq!(pn_get_reply(None), "none");
}

#[test]
fn pn_dump_reply_renders_the_counters_debug_form() {
    let counter = PnCounter::local_delta(writer(1, 100), 5);
    assert_eq!(pn_dump_reply(Some(&counter)), format!("{counter:?}"));
    assert_eq!(pn_dump_reply(None), "none");
}

#[test]
fn pn_encoded_bytes_sums_each_counters_encoded_length() {
    let a = PnCounter::local_delta(writer(1, 100), 5);
    let b = a.merge(&PnCounter::local_delta(writer(2, 100), 7));
    let expected = a.encode().unwrap().len() + b.encode().unwrap().len();
    assert_eq!(pn_encoded_bytes([&a, &b]), Ok(expected));
    assert!(
        b.encode().unwrap().len() > a.encode().unwrap().len(),
        "a second writer's slot makes the record larger, so the sum is size-sensitive"
    );
}

#[test]
fn pn_encoded_bytes_is_zero_for_no_counters() {
    assert_eq!(pn_encoded_bytes(std::iter::empty()), Ok(0));
}

#[test]
fn parse_key_and_element_accepts_both_arguments() {
    assert_eq!(
        parse_key_and_element("osadd", Some("k"), Some("e")),
        Ok(("k", "e"))
    );
}

#[test]
fn parse_key_and_element_rejects_a_missing_key_or_element() {
    let missing_both =
        parse_key_and_element("osadd", None, None).expect_err("neither argument is present");
    assert!(missing_both.contains("osadd"));
    assert!(missing_both.contains("key"));
    assert!(missing_both.contains("element"));
    assert!(parse_key_and_element("osremove", Some("k"), None).is_err());
    assert!(parse_key_and_element("osremove", None, Some("e")).is_err());
}

#[test]
fn osadd_delta_tags_the_element_with_the_writer_and_seq() {
    let set = osadd_delta(writer(1, 100), 7, "hello");
    assert!(set.contains(&"hello".to_string()));
    assert_eq!(set.iter().collect::<Vec<_>>(), vec![&"hello".to_string()]);
}

#[test]
fn osremove_delta_removes_only_the_named_element() {
    let a = osadd_delta(writer(1, 100), 0, "keep");
    let b = osadd_delta(writer(1, 100), 1, "drop");
    let observed = a.merge(&b);
    assert!(observed.contains(&"keep".to_string()));
    assert!(observed.contains(&"drop".to_string()));

    let delta = osremove_delta(&observed, "drop");
    let after = observed.merge(&delta);
    assert!(after.contains(&"keep".to_string()));
    assert!(
        !after.contains(&"drop".to_string()),
        "osremove_delta must remove exactly the named element"
    );
}

#[test]
fn os_members_reply_sorts_and_space_joins_live_members() {
    let a = osadd_delta(writer(1, 100), 0, "banana");
    let b = osadd_delta(writer(2, 100), 0, "apple");
    let set = a.merge(&b);
    assert_eq!(os_members_reply(Some(&set)), "apple banana");
}

#[test]
fn os_members_reply_is_empty_string_for_a_present_but_fully_removed_set() {
    let a = osadd_delta(writer(1, 100), 0, "only");
    let removed = a.merge(&osremove_delta(&a, "only"));
    assert_eq!(os_members_reply(Some(&removed)), "");
}

#[test]
fn os_members_reply_is_none_for_an_absent_key() {
    assert_eq!(os_members_reply(None), "none");
}

/// A single-node [`TestNode`] on loopback, all four caches open as [`run`] opens them.
async fn test_node(name: &str) -> TestNode {
    let loopback = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0));
    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = loopback;
        c.data_bind_addr = loopback;
    });
    let cluster = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("a single-node cluster builds with no seeds");
    let cache = cluster
        .cache::<String, String>(CACHE_NAME)
        .mode(Mode::Replicated)
        .open()
        .await
        .expect("\"it\" opens");
    let side = SideCaches::open(&cluster)
        .await
        .expect("the side caches open");
    TestNode {
        cache,
        side: Some(side),
        cluster,
    }
}

/// Unwraps a [`Reply::Line`]; panics on `Quit`/`Crash`.
fn reply_line(reply: Reply) -> String {
    match reply {
        Reply::Line(line) => line,
        Reply::Quit | Reply::Crash => panic!("expected Reply::Line"),
    }
}

#[tokio::test]
async fn dispatch_through_test_node_put_get_and_count_round_trip() {
    let node = test_node("testnode-dispatch-put-get").await;
    assert_eq!(reply_line(node.dispatch("put k v").await), "ok");
    assert_eq!(reply_line(node.dispatch("get k").await), "val v");
    assert_eq!(reply_line(node.dispatch("count").await), "1");
    assert_eq!(reply_line(node.dispatch("get missing").await), "none");
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn dispatch_through_test_node_declines_side_cache_commands_with_the_side_caches_off() {
    let mut node = test_node("testnode-dispatch-side-off").await;
    assert_eq!(reply_line(node.dispatch("ccount").await), "0");
    node.side = None;
    for line in ["churn 4", "ccount", "pncount", "osmembers k"] {
        assert_eq!(
            reply_line(node.dispatch(line).await),
            "err side caches are off",
            "{line}"
        );
    }
    assert_eq!(
        reply_line(node.dispatch("count").await),
        "0",
        "\"it\" still answers"
    );
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn dispatch_through_test_node_reports_an_unknown_command() {
    let node = test_node("testnode-dispatch-unknown-command").await;
    assert_eq!(
        reply_line(node.dispatch("bogus").await),
        "err unknown command \"bogus\""
    );
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn dispatch_through_test_node_osadd_osremove_and_osmembers_round_trip() {
    let node = test_node("testnode-dispatch-os-round-trip").await;
    for elem in ["b", "a", "c"] {
        assert_eq!(
            reply_line(node.dispatch(&format!("osadd k {elem}")).await),
            "ok"
        );
    }
    assert_eq!(reply_line(node.dispatch("osmembers k").await), "a b c");

    assert_eq!(reply_line(node.dispatch("osremove k b").await), "ok");
    assert_eq!(reply_line(node.dispatch("osadd k d").await), "ok");

    assert_eq!(
        reply_line(node.dispatch("osmembers k").await),
        "a c d",
        "the remove takes out the element it read and the later add lands"
    );
    node.cluster.clone().shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_through_test_node_concurrent_osadds_all_land() {
    let node = Arc::new(test_node("testnode-dispatch-os-concurrent").await);
    let tasks: Vec<_> = (0..8)
        .map(|task| {
            let node = Arc::clone(&node);
            tokio::spawn(async move {
                for index in 0..15 {
                    let reply = node.dispatch(&format!("osadd k t{task}-{index}")).await;
                    assert_eq!(reply_line(reply), "ok");
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("osadd task");
    }

    let members = reply_line(node.dispatch("osmembers k").await);
    assert_eq!(members.split(' ').count(), 8 * 15, "every add lands");
    node.cluster.clone().shutdown().await;
}

/// A single-node [`TestNode`] on loopback whose `"it"` has a loader, as
/// `SUNDOG_TESTNODE_LOADER_DELAY_MS` registers one. No other test
/// registers a loader, so only this node moves `loads`.
async fn loading_node(name: &str) -> TestNode {
    let loopback = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0));
    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = loopback;
        c.data_bind_addr = loopback;
    });
    let cluster = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config)
        .build()
        .await
        .expect("a single-node cluster builds with no seeds");
    let builder = cluster
        .cache::<String, String>(CACHE_NAME)
        .mode(Mode::Replicated);
    let loading = Loading {
        loader_delay: Some(Duration::from_millis(1)),
        ttl: None,
        refresh_ahead: None,
    };
    let cache = apply_loading(builder, loading)
        .open()
        .await
        .expect("\"it\" opens");
    TestNode {
        cache,
        side: None,
        cluster,
    }
}

/// A loopback node with `"it"` open under two-owner `Distributed`,
/// joined to `seed` when there is one.
async fn distributed_node(name: &str, seed: Option<SocketAddr>) -> TestNode {
    let loopback = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0));
    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = loopback;
        c.data_bind_addr = loopback;
    });
    let cluster = Cluster::builder(name)
        .seeds(seed)
        .config(config)
        .build()
        .await
        .expect("a loopback node builds");
    let cache = cluster
        .cache::<String, String>(CACHE_NAME)
        .mode(Mode::Distributed {
            owners: two_owners(),
        })
        .open()
        .await
        .expect("\"it\" opens");
    TestNode {
        cache,
        side: None,
        cluster,
    }
}

/// What `node` reports for `key` through [`explain_reply`].
async fn explained(node: &TestNode, key: &str) -> explain_reply::ExplainReply {
    let explanation = node
        .cache
        .explain(&key.to_string())
        .await
        .expect("a String key encodes");
    explain_reply::from_explanation(&explanation)
}

/// `node`'s `explain key` reply, parsed.
async fn explain_json(node: &TestNode, key: &str) -> serde_json::Value {
    let line = reply_line(node.dispatch(&format!("explain {key}")).await);
    serde_json::from_str(&line).unwrap_or_else(|error| panic!("{error}: {line}"))
}

/// `node`'s id as the `explain` reply prints it.
fn id_text(node: &TestNode) -> String {
    node.cluster.node_id().to_string()
}

const EXPLAIN_WAIT: Duration = Duration::from_secs(30);

#[tokio::test]
async fn a_replicated_cache_reply_has_no_distributed_section() {
    let node = test_node("testnode-explain-replicated").await;
    let reply = explain_json(&node, "k17").await;
    assert_eq!(reply["cache"], "it");
    assert_eq!(reply["node"], id_text(&node));
    assert_eq!(reply["mode"], "replicated");
    assert_eq!(
        reply["distributed"],
        serde_json::Value::Null,
        "a Replicated cache has no ownership to explain"
    );
    let typed = explained(&node, "k17").await;
    assert_eq!(typed.mode, "replicated");
    assert_eq!(typed.distributed, None);
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn a_single_node_distributed_reply_names_the_node_as_the_only_owner_and_has_no_probes() {
    let node = distributed_node("testnode-explain-single", None).await;
    let me = id_text(&node);
    let deadline = tokio::time::Instant::now() + EXPLAIN_WAIT;
    let read = loop {
        let reply = explained(&node, "k17").await;
        if let Some(read) = reply.distributed
            && read.owners == [me.clone()]
        {
            break read;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the node did not become the only owner within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(read.residency.owns, "the only owner owns the part");
    assert_eq!(read.probes, Vec::new(), "no other owner is asked");
    assert_eq!(read.view.len(), 16);
    assert_eq!(read.view_moved_to, None);

    let reply = explain_json(&node, "k17").await;
    assert_eq!(reply["mode"], "distributed:2");
    assert_eq!(reply["distributed"]["owners"], serde_json::json!([me]));
    assert_eq!(reply["distributed"]["probes"], serde_json::json!([]));
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn a_written_key_reads_live_and_a_removed_key_reads_tombstone() {
    let node = test_node("testnode-explain-records").await;
    let me = id_text(&node);
    let part = sundog::store::PartId::of_key(
        &postcard::to_stdvec(&"k17".to_string()).expect("a String encodes"),
    );

    let absent = explained(&node, "k17").await;
    assert_eq!((absent.bucket, absent.part), (part.bucket(), part.part()));
    assert_eq!((absent.cache.as_str(), absent.node.as_str()), ("it", &*me));
    assert_eq!(absent.local.kind, "absent");
    assert_eq!(absent.local.version, None);
    assert_eq!(absent.source.kind, "local");
    assert_eq!(absent.source.hit, Some(false));

    assert_eq!(reply_line(node.dispatch("put k17 v").await), "ok");
    let live = explained(&node, "k17").await;
    assert_eq!(live.local.kind, "live");
    let live_version = live
        .local
        .version
        .clone()
        .expect("a live record has a version");
    let (stamp, writer) = live_version.split_once('@').expect("wall.logical@node");
    assert_eq!(writer, me, "this node wrote the record");
    let (wall, logical) = stamp.split_once('.').expect("wall.logical");
    wall.parse::<u64>().expect("the wall clock is a number");
    logical
        .parse::<u32>()
        .expect("the logical clock is a number");
    assert_eq!(live.local.expires_at_ms, explain_reply::Expiry::Never);
    assert_eq!(live.local.spilled, Some(false));
    assert_eq!(live.source.hit, Some(true));

    assert_eq!(reply_line(node.dispatch("del k17").await), "ok");
    let tombstone = explained(&node, "k17").await;
    assert_eq!(tombstone.local.kind, "tombstone");
    let tombstone_version = tombstone.local.version.expect("a tombstone has a version");
    assert_ne!(
        tombstone_version, live_version,
        "the delete is a newer version"
    );
    assert_eq!(
        tombstone.local.expires_at_ms,
        explain_reply::Expiry::Omitted
    );
    assert_eq!(tombstone.source.hit, Some(false));
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn an_expired_entry_the_sweep_has_not_reached_reads_lapsed() {
    let node = test_node("testnode-explain-lapsed").await;
    // The cache's entries gauge task sweeps every 5 s, so a sweep can land
    // between a key's expiry and its read. The 20 attempts span about 3 s,
    // which fits at most one tick: it removes that attempt's key, and the
    // next attempt's key lapses.
    let mut lapsed = None;
    for attempt in 0..20 {
        let key = format!("ttl{attempt}");
        node.cache
            .insert_with_ttl(key.clone(), "v".to_string(), Duration::from_millis(20))
            .await
            .expect("a TTL insert succeeds");
        tokio::time::sleep(Duration::from_millis(150)).await;
        let reply = explained(&node, &key).await;
        if reply.local.kind == "lapsed" {
            lapsed = Some(reply);
            break;
        }
    }
    let reply = lapsed.expect("an expired entry reads lapsed before the sweep removes it");
    assert!(reply.local.version.is_some(), "{reply:?}");
    assert_eq!(reply.local.cause.as_deref(), Some("expired"));
    let explain_reply::Expiry::At(expires_at_ms) = reply.local.expires_at_ms else {
        panic!("a lapsed entry carries its expiry: {reply:?}");
    };
    assert!(expires_at_ms <= reply.at_ms, "{reply:?}");
    assert_eq!(
        reply.source.hit,
        Some(false),
        "a lapsed entry reads as a miss"
    );
    node.cluster.clone().shutdown().await;
}

/// `node`'s explanation of `key`, once its `"it"` reports how a
/// `Distributed` fetch decides.
async fn distributed_explanation(node: &TestNode, key: &str) -> sundog::ReadExplanation {
    let deadline = tokio::time::Instant::now() + EXPLAIN_WAIT;
    loop {
        let explanation = node
            .cache
            .explain(&key.to_string())
            .await
            .expect("a String key encodes");
        if explanation.distributed.is_some() {
            return explanation;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the cache did not explain its ownership within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn each_residency_mark_and_the_moved_view_reach_the_reply_through_their_own_field() {
    let node = distributed_node("testnode-explain-marks", None).await;
    let mut explanation = distributed_explanation(&node, "k17").await;
    // Each mark alone, so a swap of two fields, a dropped field or a
    // constant fails in at least one case.
    for field in 0..6 {
        let residency = &mut explanation
            .distributed
            .as_mut()
            .expect("a Distributed cache")
            .residency;
        residency.owns = false;
        residency.releasing_for = None;
        residency.cold_marked = false;
        residency.unsettled = false;
        residency.unverified = false;
        residency.stale = false;
        let mut expected = explain_reply::Marks::default();
        match field {
            0 => (residency.owns, expected.owns) = (true, true),
            1 => {
                residency.releasing_for = Some(Duration::from_millis(4_200));
                expected.releasing_ms = Some(4_200);
            }
            2 => (residency.cold_marked, expected.cold_marked) = (true, true),
            3 => (residency.unsettled, expected.unsettled) = (true, true),
            4 => (residency.unverified, expected.unverified) = (true, true),
            _ => (residency.stale, expected.stale) = (true, true),
        }
        let reply = explain_reply::from_explanation(&explanation);
        let read = reply.distributed.expect("a Distributed cache");
        assert_eq!(read.residency, expected, "mark {field}");
    }
    // A view that moved during the call prints as 16 hex digits.
    let read = explanation
        .distributed
        .as_mut()
        .expect("a Distributed cache");
    read.view_moved_to = Some(0x71c0_a2f4_e8b3_195d);
    let reply = explain_reply::from_explanation(&explanation);
    let read = reply.distributed.expect("a Distributed cache");
    assert_eq!(read.view_moved_to.as_deref(), Some("71c0a2f4e8b3195d"));
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn a_record_in_the_spill_tier_reports_spilled() {
    use sundog::explain::LocalRecord;

    let node = test_node("testnode-explain-spilled").await;
    assert_eq!(reply_line(node.dispatch("put k17 v").await), "ok");
    let mut explanation = node
        .cache
        .explain(&"k17".to_string())
        .await
        .expect("a String key encodes");
    let spilled = |explanation: &mut sundog::ReadExplanation, to: bool| {
        let LocalRecord::Live { spilled, .. } = &mut explanation.local else {
            panic!("a written key reads live: {explanation:?}");
        };
        *spilled = to;
    };
    assert_eq!(
        explain_reply::from_explanation(&explanation).local.spilled,
        Some(false),
        "the test node keeps its entries in memory"
    );
    spilled(&mut explanation, true);
    assert_eq!(
        explain_reply::from_explanation(&explanation).local.spilled,
        Some(true)
    );
    spilled(&mut explanation, false);
    assert_eq!(
        explain_reply::from_explanation(&explanation).local.spilled,
        Some(false)
    );
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn two_nodes_explain_reports_the_other_owners_probe() {
    let a = distributed_node("testnode-explain-two", None).await;
    let b = distributed_node("testnode-explain-two", Some(a.cluster.local_gossip_addr())).await;
    let (a_id, b_id) = (id_text(&a), id_text(&b));
    let key = "k17".to_string();
    let deadline = tokio::time::Instant::now() + EXPLAIN_WAIT;
    while a.cache.owners_of(&key).len() != 2 || b.cache.owners_of(&key).len() != 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "both nodes did not own the key within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(reply_line(a.dispatch("put k17 v").await), "ok");

    let mut seen = Vec::new();
    for (node, other) in [(&a, &b_id), (&b, &a_id)] {
        let reply = loop {
            let reply = explained(node, "k17").await;
            if reply.local.kind == "live"
                && reply
                    .distributed
                    .as_ref()
                    .is_some_and(|read| read.probes.iter().any(|p| p.answer == "held"))
            {
                break reply;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} did not see the other owner hold the key within 30 s: {reply:?}",
                node.cluster.node_id()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let read = reply.distributed.as_ref().expect("a Distributed cache");
        assert_eq!(read.owners.len(), 2, "{reply:?}");
        assert!(read.owners.contains(&a_id) && read.owners.contains(&b_id));
        assert!(
            read.residency.owns,
            "two nodes at two owners own every part"
        );
        assert_eq!(read.local_read, "hit");
        assert_eq!(read.serves_peers, "serve");
        assert_eq!(read.view_moved_to, None);
        assert_eq!(read.probes.len(), 1, "the one other owner is asked");
        let probe = &read.probes[0];
        assert_eq!(&probe.node, other);
        assert_eq!(probe.answer, "held");
        assert_eq!(probe.reads.as_deref(), Some("value"));
        assert_eq!(probe.expires_at_ms, explain_reply::Expiry::Never);
        assert_eq!(
            probe.version, reply.local.version,
            "both owners hold the one write"
        );
        assert_eq!(reply.source.kind, "local");
        assert_eq!(reply.source.hit, Some(true));
        seen.push((read.view.clone(), read.owners.clone()));
    }
    assert_eq!(seen[0], seen[1], "both nodes explain from one view");
    a.cluster.clone().shutdown().await;
    b.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn dispatch_through_test_node_explain_replies_with_a_reading_and_touches_nothing() {
    let node = test_node("testnode-dispatch-explain").await;
    let reply = explain_json(&node, "k").await;
    assert_eq!(reply["cache"], "it");
    assert_eq!(reply["node"], id_text(&node));
    assert_eq!(reply["local"], serde_json::json!({ "kind": "absent" }));
    assert_eq!(reply["source"]["kind"], "local");
    assert_eq!(reply["source"]["hit"], false);
    assert_eq!(reply_line(node.dispatch("count").await), "0");
    assert_eq!(
        reply_line(node.dispatch("get k").await),
        "none",
        "explain stores nothing"
    );
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn explain_runs_no_loader_where_load_runs_it() {
    let node = loading_node("testnode-explain-loader").await;
    let loads = || async { reply_line(node.dispatch("loads").await) };
    let before = loads().await;

    let reply = explain_json(&node, "k").await;
    assert_eq!(reply["local"], serde_json::json!({ "kind": "absent" }));
    assert_eq!(
        loads().await,
        before,
        "explain of a missing key loads nothing"
    );
    assert_eq!(reply_line(node.dispatch("count").await), "0");

    let loaded = reply_line(node.dispatch("load k").await);
    assert!(loaded.starts_with("val src-k."), "{loaded}");
    assert_ne!(loads().await, before, "load runs the loader");
    assert_eq!(explain_json(&node, "k").await["local"]["kind"], "live");

    let after = loads().await;
    explain_json(&node, "k").await;
    explain_json(&node, "other").await;
    assert_eq!(loads().await, after, "explain never runs the loader");
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn dispatch_through_test_node_explain_needs_a_key() {
    let node = test_node("testnode-dispatch-explain-key").await;
    assert_eq!(
        reply_line(node.dispatch("explain").await),
        "err explain needs a key"
    );
    assert_eq!(
        reply_line(explain_command(&node.cache, None).await),
        "err explain needs a key"
    );
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn explain_is_not_answered_by_the_owners_arm() {
    let node = test_node("testnode-dispatch-explain-arm").await;
    let explain = reply_line(node.dispatch("explain k").await);
    let owners = reply_line(node.dispatch("owners k").await);
    assert!(explain.starts_with('{'), "{explain}");
    assert!(
        !owners.starts_with('{')
            && owners
                .split_whitespace()
                .all(|id| id.parse::<u64>().is_ok()),
        "owners stays a list of decimal ids: {owners:?}"
    );
    // The cache's clock is the one field two replies in a row can differ in.
    let without_clock = |line: &str| {
        let mut reply: serde_json::Value = serde_json::from_str(line).expect("a JSON reply");
        reply
            .as_object_mut()
            .expect("a JSON object")
            .remove("at_ms")
            .expect("every reply carries the cache's clock");
        reply
    };
    assert_eq!(
        without_clock(&reply_line(explain_command(&node.cache, Some("k")).await)),
        without_clock(&explain),
        "the arm and the handler give one reply"
    );
    node.cluster.clone().shutdown().await;
}

#[tokio::test]
async fn dispatch_through_test_node_quit_yields_no_reply_line() {
    let node = test_node("testnode-dispatch-quit").await;
    assert!(matches!(node.dispatch("quit").await, Reply::Quit));
    node.cluster.clone().shutdown().await;
}

/// Pins that `stop_requested` resolves on SIGTERM, retrying the raise
/// until the pending future observes one.
#[cfg(unix)]
#[tokio::test]
async fn stop_requested_resolves_on_sigterm() {
    let _guard = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("a SIGTERM listener registers");
    let mut requested = tokio::spawn(stop_requested());
    let pid = std::process::id().to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("kill runs");
        assert!(status.success(), "kill -TERM delivers to this process");
        tokio::select! {
            result = &mut requested => {
                result.expect("the task is not cancelled").expect("the stop request is Ok");
                return;
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "stop_requested did not resolve within 10 s of repeated SIGTERMs"
        );
    }
}

#[test]
fn role_from_env_defaults_to_node() {
    assert_eq!(role_from_env(None), Ok(Role::Node));
    assert_eq!(role_from_env(Some("node")), Ok(Role::Node));
}

#[test]
fn role_from_env_reads_observer() {
    assert_eq!(role_from_env(Some("observer")), Ok(Role::Observer));
}

#[test]
fn role_from_env_rejects_an_unknown_role_naming_the_allowed_values() {
    let error = role_from_env(Some("watcher")).expect_err("watcher is not a role");
    assert!(error.contains("SUNDOG_TESTNODE_ROLE"), "{error}");
    assert!(error.contains("\"node\""), "{error}");
    assert!(error.contains("\"observer\""), "{error}");
    assert!(error.contains("\"watcher\""), "{error}");
}

fn two_owners() -> std::num::NonZeroU8 {
    std::num::NonZeroU8::new(2).expect("nonzero")
}

/// A member of `status` advertising `"it"` under two-owner `Distributed`
/// at the current protocol.
fn distributed_member(node: u64, status: MemberStatus) -> Member {
    let peer = Peer {
        node: NodeId::from(node),
        name: NodeName::new("host", NodeId::from(node)),
        gossip_addr: SocketAddr::from(([127, 0, 0, 1], 7000)),
        data_addr: SocketAddr::from(([127, 0, 0, 1], 8000)),
        incarnation: 1,
        protocol: sundog::wire::PROTOCOL_VERSION,
    };
    let caches = BTreeMap::from([(
        "it".into(),
        Mode::Distributed {
            owners: two_owners(),
        },
    )]);
    Member::new(peer, status, std::time::UNIX_EPOCH, caches)
}

#[test]
fn members_line_is_all_zero_for_an_empty_snapshot() {
    let snapshot = ClusterSnapshot::new("c", Vec::new(), 0);
    assert_eq!(
        members_line(&snapshot),
        "live=0 departing=0 left=0 down=0 anon=0"
    );
}

#[test]
fn members_line_counts_each_status_and_the_anonymous_participants() {
    let members = vec![
        distributed_member(1, MemberStatus::Live),
        distributed_member(2, MemberStatus::Live),
        distributed_member(3, MemberStatus::Live),
        distributed_member(4, MemberStatus::Departing),
        distributed_member(5, MemberStatus::Left),
        distributed_member(6, MemberStatus::Left),
        distributed_member(7, MemberStatus::Down),
    ];
    let snapshot = ClusterSnapshot::new("c", members, 4);
    assert_eq!(
        members_line(&snapshot),
        "live=3 departing=1 left=2 down=1 anon=4"
    );
}

#[test]
fn parse_share_args_reads_a_cache_an_owner_count_and_a_node_id() {
    assert_eq!(
        parse_share_args("it 2 18446744073709551615"),
        Some(("it".to_string(), two_owners(), NodeId::from(u64::MAX)))
    );
}

#[test]
fn parse_share_args_rejects_missing_zero_unparsable_and_extra_arguments() {
    for args in [
        "",
        "it",
        "it 2",
        "it 0 7",
        "it 256 7",
        "it two 7",
        "it 2 seven",
        "it 2 -1",
        "it 2 7 extra",
    ] {
        assert_eq!(parse_share_args(args), None, "{args:?}");
    }
}

#[test]
fn share_reply_is_none_without_an_eligible_member() {
    let snapshot = ClusterSnapshot::new("c", vec![distributed_member(1, MemberStatus::Down)], 0);
    assert_eq!(
        share_reply(&snapshot, "it", two_owners(), NodeId::from(1)),
        "none"
    );
    assert_eq!(
        share_reply(&snapshot, "absent", two_owners(), NodeId::from(1)),
        "none"
    );
}

#[test]
fn share_reply_gives_each_member_its_parts_and_zero_off_the_eligible_set() {
    let members = vec![
        distributed_member(1, MemberStatus::Live),
        distributed_member(2, MemberStatus::Live),
        distributed_member(3, MemberStatus::Departing),
    ];
    let snapshot = ClusterSnapshot::new("c", members, 0);
    // Two eligible members at two owners: each owns every part.
    for node in [1, 2] {
        assert_eq!(
            share_reply(&snapshot, "it", two_owners(), NodeId::from(node)),
            "65536",
            "node {node}"
        );
    }
    assert_eq!(
        share_reply(&snapshot, "it", two_owners(), NodeId::from(3)),
        "0",
        "a departing member owns nothing"
    );
    assert_eq!(
        share_reply(&snapshot, "it", two_owners(), NodeId::from(9)),
        "0",
        "an unknown node owns nothing"
    );
}

/// A single-node cluster on loopback with `"it"` open under two-owner
/// `Distributed`, and an observer seeded from it.
async fn observed_cluster(name: &str) -> (TestNode, ObserverNode) {
    let loopback = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0));
    let config = ClusterConfig::default().with(|c| {
        c.gossip_bind_addr = loopback;
        c.data_bind_addr = loopback;
    });
    let cluster = Cluster::builder(name)
        .seeds(std::iter::empty())
        .config(config.clone())
        .build()
        .await
        .expect("a single-node cluster builds with no seeds");
    let cache = cluster
        .cache::<String, String>(CACHE_NAME)
        .mode(Mode::Distributed {
            owners: two_owners(),
        })
        .open()
        .await
        .expect("\"it\" opens");
    let observer = Observer::builder(name)
        .seeds([cluster.local_gossip_addr()])
        .config(config)
        .build()
        .await
        .expect("the observer builds");
    let member = TestNode {
        cache,
        side: None,
        cluster,
    };
    (member, ObserverNode { observer })
}

#[tokio::test]
async fn observer_dispatch_reports_members_and_shares_and_rejects_the_rest() {
    let (member, node) = observed_cluster("testnode-observer-dispatch").await;
    let id = member.cluster.node_id().as_u64();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while reply_line(node.dispatch("members").await) != "live=1 departing=0 left=0 down=0 anon=0"
        || reply_line(node.dispatch(&format!("share it 2 {id}")).await) != "65536"
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the observer did not show the member within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        reply_line(node.dispatch("share it 2 1").await),
        "0",
        "an unknown node owns nothing"
    );
    assert_eq!(reply_line(node.dispatch("share absent 2 1").await), "none");
    assert!(
        reply_line(node.dispatch("share it 0 1").await).starts_with("err share needs"),
        "a zero owner count is refused"
    );
    assert!(reply_line(node.dispatch("share").await).starts_with("err share needs"));
    assert_eq!(
        reply_line(node.dispatch("bogus").await),
        "err unknown command \"bogus\""
    );
    assert!(matches!(node.dispatch("quit").await, Reply::Quit));
    node.observer.shutdown().await;
    member.cluster.shutdown().await;
}

#[tokio::test]
async fn control_dispatch_routes_each_role_to_its_own_commands() {
    let (member, observer) = observed_cluster("testnode-control-routing").await;
    let observing = Control::Observer(observer);
    assert!(reply_line(observing.dispatch("members").await).starts_with("live="));
    assert_eq!(
        reply_line(observing.dispatch("count").await),
        "err unknown command \"count\"",
        "an observer has no cache commands"
    );
    assert_eq!(
        reply_line(observing.dispatch("explain k").await),
        "err unknown command \"explain\"",
        "an observer has no cache to explain"
    );
    observing.shutdown().await;

    let member = Control::Node(member);
    assert_eq!(reply_line(member.dispatch("count").await), "0");
    assert_eq!(
        reply_line(member.dispatch("members").await),
        "err unknown command \"members\"",
        "a member has no observer commands"
    );
    member.shutdown().await;
}

#[cfg(feature = "spill")]
mod spill_env;
