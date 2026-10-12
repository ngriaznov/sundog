//! Tests for the `spill` environment settings of the test node.

use super::*;

#[test]
fn spill_config_from_env_is_none_without_a_dir() {
    assert!(spill_config_from_env(None, None, None, None, false).is_none());
    assert!(
        spill_config_from_env(None, Some(1 << 20), None, None, false).is_none(),
        "a capacity with no dir still opens spill-free"
    );
}

#[test]
fn spill_config_from_env_builds_from_a_dir_and_capacity() {
    let cfg = spill_config_from_env(
        Some("/tmp/spill-it".to_string()),
        Some(4096),
        None,
        None,
        false,
    )
    .expect("dir plus capacity builds a config");
    assert_eq!(cfg.dir, std::path::PathBuf::from("/tmp/spill-it"));
    assert_eq!(cfg.capacity_bytes, 4096);
}

#[test]
fn spill_config_from_env_applies_the_region_override() {
    let default_region = spill_config_from_env(
        Some("/tmp/spill-it".to_string()),
        Some(4096),
        None,
        None,
        false,
    )
    .expect("builds")
    .region_bytes_value();
    let cfg = spill_config_from_env(
        Some("/tmp/spill-it".to_string()),
        Some(4096),
        Some(512),
        None,
        false,
    )
    .expect("builds");
    assert_eq!(cfg.region_bytes_value(), 512);
    assert_ne!(
        cfg.region_bytes_value(),
        default_region,
        "the override changes the region size from the default"
    );
}

#[test]
fn spill_config_from_env_applies_the_flush_queue_bytes_override() {
    let default_flush_queue = spill_config_from_env(
        Some("/tmp/spill-it".to_string()),
        Some(4096),
        None,
        None,
        false,
    )
    .expect("builds")
    .flush_queue_bytes_value();
    let cfg = spill_config_from_env(
        Some("/tmp/spill-it".to_string()),
        Some(4096),
        None,
        Some(1024),
        false,
    )
    .expect("builds");
    assert_eq!(cfg.flush_queue_bytes_value(), 1024);
    assert_ne!(
        cfg.flush_queue_bytes_value(),
        default_flush_queue,
        "the override changes the flush-queue bound from the default"
    );
}

#[test]
fn spill_config_from_env_defaults_warm_reopen_to_false() {
    let cfg = spill_config_from_env(
        Some("/tmp/spill-it".to_string()),
        Some(4096),
        None,
        None,
        false,
    )
    .expect("builds");
    assert!(!cfg.warm_reopen_value());
}

#[test]
fn spill_config_from_env_applies_the_warm_reopen_override() {
    let cfg = spill_config_from_env(
        Some("/tmp/spill-it".to_string()),
        Some(4096),
        None,
        None,
        true,
    )
    .expect("builds");
    assert!(cfg.warm_reopen_value());
}

#[test]
#[should_panic(expected = "SUNDOG_TESTNODE_SPILL_CAPACITY_BYTES")]
fn spill_config_from_env_panics_when_the_dir_is_set_without_a_capacity() {
    let _ = spill_config_from_env(Some("/tmp/spill-it".to_string()), None, None, None, false);
}

#[test]
fn resolve_byte_budget_is_none_when_neither_knob_is_set() {
    assert_eq!(resolve_byte_budget(None, None), None);
}

#[test]
fn resolve_byte_budget_reads_a_raw_byte_value() {
    assert_eq!(resolve_byte_budget(Some(4096), None), Some(4096));
}

#[test]
fn resolve_byte_budget_converts_a_mebibyte_value() {
    assert_eq!(resolve_byte_budget(None, Some(4)), Some(4 * MIB));
}

#[test]
fn resolve_byte_budget_prefers_the_byte_value_when_both_are_set() {
    assert_eq!(
        resolve_byte_budget(Some(4096), Some(4)),
        Some(4096),
        "the byte-denominated knob wins over its MiB counterpart"
    );
}

#[test]
fn resolve_byte_budget_saturates_instead_of_overflowing() {
    assert_eq!(resolve_byte_budget(None, Some(u64::MAX)), Some(u64::MAX));
}
