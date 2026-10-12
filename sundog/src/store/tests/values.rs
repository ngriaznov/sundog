//! Value types and pure functions: fingerprints, version functions, `Mode` tokens, `Origin` and the fan-out helpers.

use super::*;

/// `xxh3(key_bytes ‖ postcard(ver))` spelled out, the definition
/// [`entry_fingerprint`] computes.
fn fingerprint_by_definition(key_bytes: &[u8], ver: Hlc) -> u64 {
    let mut joined = key_bytes.to_vec();
    joined.extend_from_slice(&postcard::to_stdvec(&ver).expect("an Hlc encodes"));
    xxh3_64(&joined)
}

#[test]
fn entry_fingerprint_hashes_key_and_version_on_either_side_of_the_stack_buffer() {
    let widest = Hlc {
        wall_ms: u64::MAX,
        logical: u32::MAX,
        node: NodeId::from(u64::MAX),
    };
    let widest_len = postcard::to_stdvec(&widest).expect("an Hlc encodes").len();
    let edge = FINGERPRINT_STACK_BYTES - widest_len;
    for ver in [hlc(0, 0), hlc(1_727_000_000_000, 7), widest] {
        for len in [
            0,
            1,
            16,
            edge - 1,
            edge,
            edge + 1,
            FINGERPRINT_STACK_BYTES,
            300,
        ] {
            let key: Vec<u8> = (0..len)
                .map(|i| u8::try_from(i % 251).expect("below 251"))
                .collect();
            assert_eq!(
                entry_fingerprint(&key, ver),
                fingerprint_by_definition(&key, ver),
                "a {len}-byte key at {ver:?}"
            );
        }
    }
}

/// Nodes compare digests built from these fingerprints, so the value is
/// part of the protocol: pinned against the one release N-1 computes.
#[test]
fn entry_fingerprint_keeps_its_value_across_releases() {
    assert_eq!(
        entry_fingerprint(b"key-000000000042", hlc(1_727_000_000_000, 7)),
        16_473_212_977_212_482_666
    );
}

#[test]
fn compacted_version_is_the_stale_versions_successor_keyed_by_content() {
    let stale = hlc(5_000, 1);
    let minted = compacted_version(stale, b"folded");
    assert!(minted > stale);
    assert_eq!((minted.wall_ms, minted.logical), (5_000, stale.logical + 1));
    assert!(minted.node.is_merge_derived());
    assert_eq!(
        compacted_version(stale, b"folded"),
        minted,
        "the same stale version and bytes mint the same version on any replica"
    );
    assert_ne!(
        compacted_version(stale, b"folded differently"),
        minted,
        "different bytes from the same stale version mint different versions"
    );
}

#[test]
fn compacted_version_carries_wall_ms_into_logical_zero_on_overflow() {
    let stale = Hlc {
        wall_ms: 9_000,
        logical: u32::MAX,
        node: NodeId::from(1),
    };
    let minted = compacted_version(stale, b"x");
    assert_eq!((minted.wall_ms, minted.logical), (9_001, 0));
}

#[test]
fn mode_is_copy_and_comparable() {
    assert_eq!(Mode::Local, Mode::Local);
    assert_ne!(Mode::Local, Mode::Replicated);
}

#[test]
fn mode_distributed_defaults_to_two_owners_and_round_trips_its_token() {
    assert_eq!(Mode::DEFAULT_OWNERS.get(), 2);
    assert_eq!(
        Mode::distributed(),
        Mode::Distributed {
            owners: Mode::DEFAULT_OWNERS
        }
    );
    assert_eq!(Mode::distributed().as_token(), "distributed:2");
    assert_eq!(
        Mode::from_token(&Mode::distributed().as_token()),
        Some(Mode::distributed())
    );
}

#[test]
fn mode_as_token_and_from_token_round_trip_every_variant() {
    let distributed = Mode::Distributed {
        owners: NonZeroU8::new(3).expect("nonzero"),
    };
    for mode in [
        Mode::Local,
        Mode::Invalidation,
        Mode::Replicated,
        distributed,
    ] {
        assert_eq!(Mode::from_token(&mode.as_token()), Some(mode));
    }
}

#[test]
fn mode_as_token_distributed_carries_the_owners_count() {
    let mode = Mode::Distributed {
        owners: NonZeroU8::new(5).expect("nonzero"),
    };
    assert_eq!(mode.as_token().as_str(), "distributed:5");
}

#[test]
fn mode_from_token_rejects_zero_owners() {
    assert_eq!(Mode::from_token("distributed:0"), None);
}

#[test]
fn mode_from_token_rejects_a_non_numeric_owners_suffix() {
    assert_eq!(Mode::from_token("distributed:abc"), None);
}

#[test]
fn mode_from_token_rejects_a_missing_owners_suffix() {
    assert_eq!(Mode::from_token("distributed:"), None);
    assert_eq!(Mode::from_token("distributed"), None);
}

#[test]
fn mode_from_token_rejects_an_owners_count_past_u8() {
    assert_eq!(Mode::from_token("distributed:256"), None);
}

#[test]
fn mode_from_token_rejects_an_unrecognized_token() {
    assert_eq!(Mode::from_token("bogus"), None);
}

#[test]
fn origin_distinguishes_local_from_remote() {
    assert_ne!(Origin::Local, Origin::Remote(NodeId::from(1)));
}

#[test]
fn clock_jumped_flags_a_lead_past_the_bound_only() {
    assert!(!clock_jumped(60_000, Some(60_000)));
    assert!(clock_jumped(60_001, Some(60_000)));
    assert!(!clock_jumped(0, Some(0)));
    assert!(!clock_jumped(u64::MAX, None), "no bound never flags");
}

#[test]
fn fill_fans_out_everywhere_but_invalidation() {
    assert!(fill_fans_out(Mode::Replicated));
    assert!(fill_fans_out(Mode::distributed()));
    assert!(
        fill_fans_out(Mode::Local),
        "the fan-out task sends nothing for Local"
    );
    assert!(!fill_fans_out(Mode::Invalidation));
}

#[test]
fn rearmed_version_is_the_bases_successor_keyed_by_the_new_deadline() {
    let base = hlc(5_000, 1);
    let minted = rearmed_version(base, Some(9_000));
    assert!(minted > base);
    assert_eq!((minted.wall_ms, minted.logical), (5_000, base.logical + 1));
    assert!(minted.node.is_merge_derived());
    assert_eq!(
        rearmed_version(base, Some(9_000)),
        minted,
        "the same re-arm of the same record mints the same version on any node"
    );
    assert_ne!(rearmed_version(base, Some(9_001)), minted);
    assert_ne!(rearmed_version(base, None), minted);
    assert_ne!(
        rearmed_version(hlc(5_000, 2), Some(9_000)).node,
        minted.node,
        "another base record mints another node id"
    );
    assert!(
        minted < hlc(5_001, 1),
        "a write stamped a millisecond after the base outranks the re-arm"
    );
}

#[cfg(feature = "spill")]
#[test]
fn a_retry_round_is_futile_only_when_the_whole_queue_covered_nothing() {
    const WHOLE: u32 = 4096;
    assert!(
        retry_round_is_futile(WHOLE, WHOLE, WHOLE, 5000),
        "the whole queue, nothing spent, a deficit still owed: the victim exceeds the queue"
    );
    assert!(
        !retry_round_is_futile(300, WHOLE, 300, 700),
        "granted less than the queue and nothing spent: the next round asks for 700, \
         which the queue can grant"
    );
    assert!(
        !retry_round_is_futile(WHOLE, WHOLE, 96, 700),
        "the round spent from its reservation: progress, keep retrying"
    );
    assert!(
        !retry_round_is_futile(WHOLE, WHOLE, WHOLE, 0),
        "nothing owed: the loop ends on its own"
    );
}

#[test]
fn fan_out_over_capacity_at_or_above_the_cap_only() {
    assert!(!fan_out_over_capacity(4, 5), "under capacity: no wait");
    assert!(fan_out_over_capacity(5, 5), "at capacity: wait");
    assert!(fan_out_over_capacity(6, 5), "over capacity: wait");
    assert!(
        !fan_out_over_capacity(0, 1),
        "an empty queue is always under a nonzero capacity"
    );
}
