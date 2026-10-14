//! Custom [`ConflictResolver`]s: winner selection, merging and compaction defaults.

use super::*;

/// A resolver where the longer value wins, `Hlc` breaking ties on equal
/// length. `(len, ver)` compared lexicographically satisfies
/// `ConflictResolver::winner`'s contract.
#[derive(Debug, Clone, Copy)]
struct LongestValueWins;

impl ConflictResolver for LongestValueWins {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        let len_a = a.value.map_or(0, <[u8]>::len);
        let len_b = b.value.map_or(0, <[u8]>::len);
        match len_a.cmp(&len_b) {
            std::cmp::Ordering::Greater => Winner::A,
            std::cmp::Ordering::Less => Winner::B,
            std::cmp::Ordering::Equal => {
                if a.ver >= b.ver {
                    Winner::A
                } else {
                    Winner::B
                }
            }
        }
    }
}

#[test]
fn longest_value_wins_is_antisymmetric_over_sample_pairs() {
    let resolver = LongestValueWins;
    let samples = [
        RecordView {
            value: Some(b"a"),
            ver: hlc(1, 1),
            expires_at_ms: None,
        },
        RecordView {
            value: Some(b"abc"),
            ver: hlc(2, 2),
            expires_at_ms: None,
        },
        RecordView {
            value: None,
            ver: hlc(3, 1),
            expires_at_ms: None,
        },
        RecordView {
            value: Some(b"ab"),
            ver: hlc(1, 9),
            expires_at_ms: None,
        },
        // Same length as "ab" above, different version: the only pair
        // here that exercises the equal-length tie-break by `Hlc`.
        RecordView {
            value: Some(b"zz"),
            ver: hlc(5, 3),
            expires_at_ms: None,
        },
    ];
    for &a in &samples {
        for &b in &samples {
            if a.ver == b.ver {
                continue; // Shard::apply never asks about equal versions.
            }
            let ab = resolver.winner(b"k", a, b);
            let ba = resolver.winner(b"k", b, a);
            assert_ne!(ab, ba, "swapping the arguments swaps the winner");
        }
    }

    // `AlwaysA` is exactly the antisymmetry violation the trait docs
    // warn about: swapping the arguments does not swap the winner.
    let broken = AlwaysA;
    let (x, y) = (samples[0], samples[1]);
    assert_eq!(broken.winner(b"k", x, y), Winner::A);
    assert_eq!(broken.winner(b"k", y, x), Winner::A);
}

#[test]
fn merged_carries_the_resolvers_value_and_ttl() {
    let merged = Merged {
        value: Bytes::from_static(b"merged-bytes"),
        expires_at_ms: Some(42),
    };
    let Merged {
        value,
        expires_at_ms,
    } = merged;
    assert_eq!(value, Bytes::from_static(b"merged-bytes"));
    assert_eq!(expires_at_ms, Some(42));
}

#[test]
fn merged_equality_compares_value_and_ttl() {
    let a = Merged {
        value: Bytes::from_static(b"x"),
        expires_at_ms: None,
    };
    let b = Merged {
        value: Bytes::from_static(b"x"),
        expires_at_ms: None,
    };
    let different_value = Merged {
        value: Bytes::from_static(b"y"),
        expires_at_ms: None,
    };
    let different_ttl = Merged {
        value: Bytes::from_static(b"x"),
        expires_at_ms: Some(1),
    };
    assert_eq!(a, b);
    assert_ne!(a, different_value);
    assert_ne!(a, different_ttl);
}

#[test]
fn conflict_resolver_merges_defaults_to_false_and_is_overridable() {
    assert!(
        !LwwResolver.merges(),
        "the default resolver never returns Merged"
    );
    assert!(
        !LongestValueWins.merges(),
        "a custom A/B-only resolver inherits the false default without overriding it"
    );
    assert!(
        AlwaysMerge {
            value: Bytes::from_static(b"x"),
            expires_at_ms: None,
        }
        .merges(),
        "a resolver that returns Merged overrides merges() to true"
    );
}

#[test]
fn conflict_resolver_compact_defaults_to_none_and_is_overridable() {
    let retire_everyone = |_: crdt::WriterId| true;
    assert_eq!(
        LwwResolver.compact(
            b"k",
            b"v",
            0,
            &retire_everyone,
            Quiescence::Settled,
            CompactionBounds::three_bounds(0)
        ),
        None,
        "the default resolver never compacts, regardless of key/value/\
         now/retire/quiet/bound"
    );
    assert_eq!(
        LongestValueWins.compact(
            b"k",
            b"v",
            12_345,
            &retire_everyone,
            Quiescence::Churning,
            CompactionBounds::three_bounds(999)
        ),
        None,
        "a custom winner-only resolver inherits the None default without \
         overriding compact"
    );

    let retire_none = |_: crdt::WriterId| false;
    let out = AlwaysCompacts
        .compact(
            b"key",
            b"val",
            42,
            &retire_none,
            Quiescence::Settled,
            CompactionBounds::three_bounds(100),
        )
        .expect("a resolver that overrides compact can return Some");
    assert_eq!(
        out.as_ref(),
        b"key:val|42|false|Settled|100",
        "every parameter compact was called with reaches the resolver's \
         own implementation"
    );
}

#[tokio::test]
async fn shard_ops_compact_pass_defaults_to_a_no_op() {
    let s = shard::<u32, String>(1);
    s.insert(1, "v".to_string()).await.expect("insert");
    let writer = crdt::WriterId::new(NodeId::from(9), 1);
    let retire = move |w: crdt::WriterId| w == writer;

    let CompactPassOutcome {
        retired, compacted, ..
    } = ShardOps::compact_pass(
        &s,
        0,
        &retire,
        Quiescence::Settled,
        CompactionBounds::three_bounds(0),
        100,
    )
    .await;

    assert!(
        retired.is_empty(),
        "a shard with no resolver set (LwwResolver) never retires a writer"
    );
    assert_eq!(compacted, 0, "the default compact_pass compacts nothing");
    assert_eq!(
        s.get(&1).await,
        Some("v".to_string()),
        "compact_pass's no-op default never touches the resident value"
    );
}

#[tokio::test]
async fn shard_merges_reflects_the_resolvers_merges_flag() {
    let default_resolver = shard::<u32, Vec<u8>>(1);
    assert!(
        !ShardOps::merges(&default_resolver),
        "a shard with no resolver set carries LwwResolver's false"
    );

    let merging = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from_static(b"x"),
        expires_at_ms: None,
    }));
    assert!(
        ShardOps::merges(&merging),
        "a shard forwards its resolver's merges() through ShardOps"
    );
}

/// Prefers the greater version and declares `needs_value_bytes` `false`.
#[derive(Debug, Clone, Copy)]
struct GreaterVersionOnly;

impl ConflictResolver for GreaterVersionOnly {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn needs_value_bytes(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn shard_ae_exchanges_both_for_a_resolver_that_reads_values_only() {
    let default_resolver = shard::<u32, Vec<u8>>(1);
    assert!(
        !ShardOps::ae_exchanges_both(&default_resolver),
        "a shard with no resolver set repairs one way, like LwwResolver"
    );

    let reads_values = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(LongestValueWins));
    assert!(
        ShardOps::ae_exchanges_both(&reads_values),
        "a resolver left at needs_value_bytes = true can prefer the lower version"
    );
    assert!(
        !ShardOps::merges(&reads_values),
        "reading values is not merging: compaction and the Invalidation wire stay off"
    );

    let merging = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(AlwaysMerge {
        value: Bytes::from_static(b"x"),
        expires_at_ms: None,
    }));
    assert!(ShardOps::ae_exchanges_both(&merging), "a merging resolver");

    let version_only = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(GreaterVersionOnly));
    assert!(
        !ShardOps::ae_exchanges_both(&version_only),
        "a resolver declaring needs_value_bytes = false keeps the one-way repair"
    );
}

#[tokio::test]
async fn a_tombstone_and_a_value_settle_by_version_for_a_resolver_preferring_values() {
    let s = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(LongestValueWins));
    let put = |len: usize, ver: Hlc| WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(
            postcard::to_stdvec(&vec![0u8; len]).expect("encode"),
        )),
        ver,
        expires_at_ms: None,
    };
    let tombstone = |ver: Hlc| WireRecord {
        key: key_bytes(&1u32),
        value: None,
        ver,
        expires_at_ms: None,
    };

    // A newer tombstone buries a longer value, which `LongestValueWins`
    // would otherwise keep.
    ShardOps::apply_remote(&s, put(10, hlc(100, 2))).await;
    ShardOps::apply_remote(&s, tombstone(hlc(200, 3))).await;
    assert_eq!(s.get(&1).await, None, "the newer tombstone wins");

    // An older value cannot resurrect the key, however long it is.
    ShardOps::apply_remote(&s, put(50, hlc(150, 2))).await;
    assert_eq!(s.get(&1).await, None, "an older value never resurrects");

    // A newer value does, however short it is.
    ShardOps::apply_remote(&s, put(1, hlc(300, 2))).await;
    assert_eq!(s.get(&1).await, Some(vec![0u8; 1]), "a newer value lands");
}

#[tokio::test]
async fn custom_resolver_overrides_plain_hlc_order() {
    let s = shard::<u32, Vec<u8>>(1).with_resolver(Arc::new(LongestValueWins));

    let long_but_old = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(
            postcard::to_stdvec(&vec![0u8; 10]).expect("encode"),
        )),
        ver: hlc(100, 2),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, long_but_old).await;
    assert_eq!(s.get(&1).await, Some(vec![0u8; 10]));

    // Strictly newer by Hlc, but shorter: loses under LongestValueWins.
    let short_but_new = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(
            postcard::to_stdvec(&vec![0u8; 3]).expect("encode"),
        )),
        ver: hlc(200, 3),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, short_but_new).await;
    assert_eq!(
        s.get(&1).await,
        Some(vec![0u8; 10]),
        "the default Hlc-only rule does not apply once a custom resolver is installed"
    );

    // Longer *and* newer: wins outright.
    let long_and_new = WireRecord {
        key: key_bytes(&1u32),
        value: Some(Bytes::from(
            postcard::to_stdvec(&vec![0u8; 20]).expect("encode"),
        )),
        ver: hlc(300, 3),
        expires_at_ms: None,
    };
    ShardOps::apply_remote(&s, long_and_new).await;
    assert_eq!(s.get(&1).await, Some(vec![0u8; 20]));
}

/// The permutation-convergence property holds for a custom resolver too,
/// not only the default one.
#[tokio::test]
async fn custom_resolver_converges_across_permutations_and_duplicates() {
    let a = shard::<u32, Vec<u8>>(10).with_resolver(Arc::new(LongestValueWins));
    let b = shard::<u32, Vec<u8>>(20).with_resolver(Arc::new(LongestValueWins));

    let mut records = Vec::new();
    for i in 0..6u64 {
        let key = u32::try_from(i % 3).expect("small");
        let len = usize::try_from((i * 7) % 11).expect("small");
        let value = vec![u8::try_from(i).expect("small"); len];
        records.push(WireRecord {
            key: key_bytes(&key),
            value: Some(Bytes::from(postcard::to_stdvec(&value).expect("encode"))),
            ver: hlc(1_000 + i * 3, i % 4 + 1),
            expires_at_ms: None,
        });
    }
    records.push(WireRecord {
        key: key_bytes(&1u32),
        value: None,
        ver: hlc(1_100, 9),
        expires_at_ms: None,
    });

    for rec in &records {
        ShardOps::apply_remote(&a, rec.clone()).await;
    }
    // Reverse order, each record applied twice: reordering and
    // duplication together, the two hazards anti-entropy tolerates.
    for rec in records.iter().rev() {
        ShardOps::apply_remote(&b, rec.clone()).await;
        ShardOps::apply_remote(&b, rec.clone()).await;
    }

    for key in 0..3u32 {
        assert_eq!(
            a.get(&key).await,
            b.get(&key).await,
            "key {key} diverged across permutations under a shared custom resolver"
        );
    }
    assert_eq!(
        ShardOps::digests(&a).await,
        ShardOps::digests(&b).await,
        "digests diverged between replicas under the same custom resolver"
    );
}

/// A resolver whose `compact` always claims to have folded something,
/// echoing every parameter it was called with into the returned bytes
/// so a test can assert each one actually reached the implementation,
/// not merely that `Some`/`None` came back.
#[derive(Debug, Clone, Copy)]
struct AlwaysCompacts;

impl ConflictResolver for AlwaysCompacts {
    fn winner(&self, _key: &[u8], a: RecordView<'_>, b: RecordView<'_>) -> Winner {
        if a.ver >= b.ver { Winner::A } else { Winner::B }
    }

    fn merges(&self) -> bool {
        true
    }

    fn compact(
        &self,
        key: &[u8],
        value: &[u8],
        now_ms: u64,
        retire: &dyn Fn(crdt::WriterId) -> bool,
        quiet: Quiescence,
        bounds: CompactionBounds,
    ) -> Option<Bytes> {
        let retired_probe = retire(crdt::WriterId::new(NodeId::from(1), 1));
        Some(Bytes::from(format!(
            "{}|{now_ms}|{retired_probe}|{quiet:?}|{}",
            String::from_utf8_lossy(key).into_owned() + ":" + &String::from_utf8_lossy(value),
            bounds.retire_after_ms
        )))
    }
}
