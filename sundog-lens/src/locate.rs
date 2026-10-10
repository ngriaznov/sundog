//! Where a key lives, computed from the model alone.
//!
//! A `Distributed` cache routes a key by the hash of its postcard encoding:
//! the low 16 bits are its part, and the ownership view ranks the part's
//! owners, highest rendezvous score first. The lens holds that view for every
//! `Distributed` cache, computed with the code the nodes run, so
//! [`locate`] names a key's part and owners, in the order a fetch asks them,
//! without asking a node.
//!
//! The answer is the placement a converged cluster gives. A node that has not
//! converged to the lens's view reads the key differently; `Cache::explain`
//! on that node says how.

use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroU8;

use smol_str::SmolStr;
use sundog::NodeId;
use sundog::observe::MemberStatus;
use sundog::store::{Mode, PartId};

use crate::key::{KeySpec, printable};
use crate::model::Model;
use crate::model::derive::Settle;
use crate::model::ownership::OwnershipDigest;
use crate::ui::data::{self, CacheRow};

/// One owner of a located key's part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatedOwner {
    /// The owner's place in fetch order: 1 is the first owner, which a fetch
    /// asks first.
    pub rank: usize,
    /// The owner's node id.
    pub node: NodeId,
    /// The owner's slot label, or [`data::UNKNOWN_LABEL`] for a node the
    /// snapshot no longer lists.
    pub slot: SmolStr,
    /// The owner's lifecycle status; `None` for a node the snapshot no longer
    /// lists.
    pub status: Option<MemberStatus>,
    /// The owner's gossip address; `None` for a node the snapshot no longer
    /// lists.
    pub gossip: Option<SocketAddr>,
    /// The owner's data-plane address; `None` for a node the snapshot no
    /// longer lists.
    pub data: Option<SocketAddr>,
}

/// A key's part and owners in one cache's ownership view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Located {
    /// The cache.
    pub cache: SmolStr,
    /// The key.
    pub key: KeySpec,
    /// The key's part: the low 16 bits of the hash of its bytes.
    pub part: PartId,
    /// Owners per part in the view.
    pub owners_per_part: NonZeroU8,
    /// Whether the view ranks single parts rather than whole buckets.
    pub ranks_parts: bool,
    /// The hash of the ownership view.
    pub view_hash: u64,
    /// The members the view ranks.
    pub eligible: usize,
    /// Whether the view has settled, and what the verdict rests on.
    pub settle: Settle,
    /// Whether the live members that advertise the cache disagree on its
    /// mode. The view is then the one the `Distributed` advertisers compute.
    pub conflicted: bool,
    /// The part's owners in fetch order; fewer than
    /// [`owners_per_part`](Self::owners_per_part) when the view ranks fewer
    /// members.
    pub owners: Vec<LocatedOwner>,
}

impl Located {
    /// The part as `Cache::explain` prints it, `bucket/part`: `513/9`.
    #[must_use]
    pub fn part_text(&self) -> String {
        format!("{}/{}", self.part.bucket(), self.part.part())
    }
}

/// Why a key cannot be located.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocateError {
    /// No live member advertises a `Distributed` cache.
    NoDistributedCache,
    /// No live member advertises the cache.
    UnknownCache {
        /// The name asked for.
        cache: SmolStr,
        /// The caches the live members advertise, `Distributed` first.
        known: Vec<SmolStr>,
    },
    /// The cache is not `Distributed`, so no part has owners.
    NotDistributed {
        /// The cache.
        cache: SmolStr,
        /// The mode every advertiser agrees on; `None` when they disagree and
        /// none of them says `Distributed`.
        mode: Option<Mode>,
    },
    /// The cache is `Distributed` but the lens has not computed its
    /// ownership yet.
    NotRankedYet {
        /// The cache.
        cache: SmolStr,
    },
    /// Several `Distributed` caches are ranked and none was named.
    Ambiguous {
        /// The ranked caches.
        caches: Vec<SmolStr>,
    },
}

impl fmt::Display for LocateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDistributedCache => {
                f.write_str("no Distributed cache is advertised: explain needs part ownership")
            }
            Self::UnknownCache { cache, known } if known.is_empty() => write!(
                f,
                "no live node advertises a cache named {} and none advertises any cache: \
                 check the cluster name and the seeds",
                printable(cache)
            ),
            Self::UnknownCache { cache, known } => write!(
                f,
                "no live node advertises a cache named {}: name one of {}",
                printable(cache),
                join(known)
            ),
            Self::NotDistributed {
                cache,
                mode: Some(mode),
            } => write!(
                f,
                "{} is {}, not Distributed, so no part has owners: name a Distributed cache",
                printable(cache),
                data::mode_name(*mode)
            ),
            Self::NotDistributed { cache, mode: None } => write!(
                f,
                "the nodes that advertise {} disagree on its mode and none says Distributed: \
                 name a Distributed cache",
                printable(cache)
            ),
            Self::NotRankedYet { cache } => write!(
                f,
                "{} is Distributed but the lens has not ranked its parts yet: \
                 wait for its ownership and ask again",
                printable(cache)
            ),
            Self::Ambiguous { caches } => write!(
                f,
                "several Distributed caches are ranked ({}): name one with --cache",
                join(caches)
            ),
        }
    }
}

impl std::error::Error for LocateError {}

/// `names`, comma separated and fit to draw: a cache name comes from gossip.
fn join(names: &[SmolStr]) -> String {
    names
        .iter()
        .map(|name| printable(name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The part and owners of `key` in `cache`, as the model's ownership view
/// ranks them. Without a cache name, the one `Distributed` cache that has a
/// digest stands for it.
///
/// A digest outlives the members it ranks: an owner the snapshot no longer
/// lists keeps its rank, reads [`data::UNKNOWN_LABEL`] for its slot and has
/// no status or address.
///
/// # Errors
///
/// Returns a [`LocateError`] when the cache is unknown, is not `Distributed`
/// or has no digest yet, and, without a name, when no or several
/// `Distributed` caches have one.
pub fn locate(model: &Model, cache: Option<&str>, key: &KeySpec) -> Result<Located, LocateError> {
    let rows = data::cache_rows(model);
    let (digest, conflicted) = match cache {
        Some(name) => named(model, &rows, name)?,
        None => (unnamed(model, &rows)?, false),
    };
    let part = PartId::of_key(key.bytes());
    let members = model
        .snapshot()
        .map_or(&[][..], |snapshot| snapshot.members.as_slice());
    let owners = digest
        .shares
        .owners_of(part)
        .iter()
        .enumerate()
        .map(|(index, &node)| {
            let member = members
                .iter()
                .filter(|member| member.peer.node == node)
                .max_by_key(|member| member.peer.incarnation);
            LocatedOwner {
                rank: index + 1,
                node,
                slot: data::tag_of(model, node).label,
                status: member.map(|member| member.status),
                gossip: member.map(|member| member.peer.gossip_addr),
                data: member.map(|member| member.peer.data_addr),
            }
        })
        .collect();
    Ok(Located {
        cache: digest.cache.clone(),
        key: key.clone(),
        part,
        owners_per_part: digest.k,
        ranks_parts: digest.ranks_parts,
        view_hash: digest.view_hash,
        eligible: digest.eligible.len(),
        settle: model.settle(&digest.cache).unwrap_or(Settle {
            settled: false,
            gossip_only: true,
        }),
        conflicted,
        owners,
    })
}

/// The digest of the cache called `name`, and whether its advertisers
/// disagree on the mode.
fn named<'m>(
    model: &'m Model,
    rows: &[CacheRow],
    name: &str,
) -> Result<(&'m OwnershipDigest, bool), LocateError> {
    let Some(row) = rows.iter().find(|row| row.name == name) else {
        return Err(LocateError::UnknownCache {
            cache: SmolStr::new(name),
            known: rows.iter().map(|row| row.name.clone()).collect(),
        });
    };
    let not_ranked = || LocateError::NotRankedYet {
        cache: row.name.clone(),
    };
    match row.mode {
        Some(Mode::Distributed { .. }) => {
            Ok((model.ownership(name).ok_or_else(not_ranked)?, false))
        }
        Some(mode) => Err(LocateError::NotDistributed {
            cache: row.name.clone(),
            mode: Some(mode),
        }),
        // The advertisers disagree. The digest the lens holds is the one the
        // `Distributed` advertisers compute, and without one the cache is
        // either about to be ranked or never will be.
        None => match model.ownership(name) {
            Some(digest) => Ok((digest, true)),
            None if row
                .modes
                .iter()
                .any(|&(_, mode)| matches!(mode, Mode::Distributed { .. })) =>
            {
                Err(not_ranked())
            }
            None => Err(LocateError::NotDistributed {
                cache: row.name.clone(),
                mode: None,
            }),
        },
    }
}

/// The digest of the one `Distributed` cache that has one.
fn unnamed<'m>(model: &'m Model, rows: &[CacheRow]) -> Result<&'m OwnershipDigest, LocateError> {
    let distributed: Vec<&CacheRow> = rows.iter().filter(|row| row.is_distributed()).collect();
    let ranked: Vec<&OwnershipDigest> = distributed
        .iter()
        .filter_map(|row| model.ownership(&row.name))
        .collect();
    match (ranked.as_slice(), distributed.first()) {
        ([digest], _) => Ok(*digest),
        ([], Some(row)) => Err(LocateError::NotRankedYet {
            cache: row.name.clone(),
        }),
        ([], None) => Err(LocateError::NoDistributedCache),
        (several, _) => Err(LocateError::Ambiguous {
            caches: several.iter().map(|digest| digest.cache.clone()).collect(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime};

    use sundog::observe::ClusterSnapshot;

    use crate::model::testkit;
    use crate::source::Update;
    use crate::ui::eventlog::view_hash;
    use crate::ui::theme;

    use super::*;

    fn key(text: &str) -> KeySpec {
        KeySpec::parse(text).expect("the key parses")
    }

    /// A model that has seen `snapshot` and a digest, with the given owners
    /// per part, for each cache in `ranked`.
    fn model_of(snapshot: &ClusterSnapshot, ranked: &[(&str, u8)]) -> Model {
        let base = Instant::now();
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let mut model = Model::new();
        model.apply(
            Update::Snapshot(Arc::new(snapshot.clone()), base),
            base,
            wall,
        );
        for &(cache, owners) in ranked {
            let digest = testkit::ownership_digest_with_owners(
                snapshot,
                cache,
                NonZeroU8::new(owners).expect("owners is nonzero"),
                None,
            )
            .expect("a member is eligible");
            model.apply(Update::Ownership(digest), base, wall);
        }
        model
    }

    /// A key whose part `node` owns in `digest`.
    fn key_owned_by(digest: &OwnershipDigest, node: NodeId) -> KeySpec {
        (0..10_000)
            .map(|n| key(&format!("k{n}")))
            .find(|spec| {
                digest
                    .shares
                    .owners_of(PartId::of_key(spec.bytes()))
                    .contains(&node)
            })
            .expect("the node owns some part")
    }

    /// Live members `1..=live` advertising `caches`.
    fn snapshot_of(live: u8, caches: &[(&str, Mode)]) -> ClusterSnapshot {
        ClusterSnapshot::new(
            "fixture",
            (1..=live)
                .map(|index| testkit::member_with(index, 0, 1, MemberStatus::Live, caches))
                .collect(),
            0,
        )
    }

    #[test]
    fn locate_returns_the_owners_of_the_keys_part_in_fetch_order() {
        let model = testkit::fixture_model(Instant::now());
        let digest = model.ownership("it").expect("the fixture ranks it");
        let located = locate(&model, Some("it"), &key("k1")).expect("k1 is located");

        let part = PartId::of_key(&[2, b'k', b'1']);
        assert_eq!(located.part, part);
        assert_eq!(located.key, key("k1"));
        assert_eq!(located.cache, "it");
        assert_eq!(located.owners_per_part.get(), 2);
        assert_eq!(located.eligible, 5);
        assert_eq!(located.view_hash, digest.view_hash);
        assert_eq!(located.ranks_parts, digest.ranks_parts);
        assert!(!located.conflicted);

        let expected = digest.shares.owners_of(part);
        assert_eq!(expected.len(), 2);
        let nodes: Vec<_> = located.owners.iter().map(|owner| owner.node).collect();
        assert_eq!(nodes, expected, "the owners in fetch order");
        assert_eq!(
            located
                .owners
                .iter()
                .map(|owner| owner.rank)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        let snapshot = model.snapshot().expect("the fixture has a snapshot");
        for owner in &located.owners {
            assert_eq!(owner.slot, data::tag_of(&model, owner.node).label);
            let member = snapshot
                .members
                .iter()
                .find(|member| member.peer.node == owner.node)
                .expect("an owner is a member");
            assert_eq!(owner.gossip, Some(member.peer.gossip_addr));
            assert_eq!(owner.data, Some(member.peer.data_addr));
            assert_eq!(owner.status, Some(MemberStatus::Live));
            assert_ne!(owner.slot, data::UNKNOWN_LABEL);
        }
        assert_eq!(
            located.part_text(),
            format!("{}/{}", part.bucket(), part.part())
        );
    }

    #[test]
    fn locate_reads_the_view_and_settled_state_from_the_digest() {
        let base = Instant::now();
        let settled = testkit::fixture_model(base);
        let located = locate(&settled, Some("it"), &key("k1")).expect("k1 is located");
        assert_eq!(
            located.settle,
            Settle {
                settled: true,
                gossip_only: true
            },
            "the fixture has no metrics and its view has held"
        );
        assert_eq!(
            view_hash(located.view_hash),
            view_hash(settled.ownership("it").expect("it is ranked").view_hash)
        );

        let measured = testkit::fixture_model_with_metrics(base);
        let located = locate(&measured, Some("it"), &key("k1")).expect("k1 is located");
        assert_eq!(
            located.settle,
            Settle {
                settled: false,
                gossip_only: false
            },
            "n3 still reports fewer parts than the lens computes"
        );
        assert_eq!(
            located.view_hash,
            settled.ownership("it").expect("ranked").view_hash
        );
    }

    #[test]
    fn locate_picks_the_only_ranked_cache_and_asks_for_a_name_when_several_are() {
        let one = model_of(
            &snapshot_of(
                3,
                &[("it", testkit::distributed(2)), ("side", Mode::Replicated)],
            ),
            &[("it", 2)],
        );
        let located = locate(&one, None, &key("k1")).expect("the only ranked cache stands in");
        assert_eq!(located.cache, "it");

        let two = model_of(
            &snapshot_of(
                3,
                &[
                    ("ids", testkit::distributed(2)),
                    ("it", testkit::distributed(2)),
                ],
            ),
            &[("ids", 2), ("it", 2)],
        );
        let error = locate(&two, None, &key("k1")).expect_err("two caches are ranked");
        assert_eq!(
            error,
            LocateError::Ambiguous {
                caches: vec![SmolStr::new("ids"), SmolStr::new("it")]
            }
        );
        let named = locate(&two, Some("ids"), &key("k1")).expect("a name settles it");
        assert_eq!(named.cache, "ids");

        // One of two is ranked: it stands in.
        let half = model_of(
            &snapshot_of(
                3,
                &[
                    ("ids", testkit::distributed(2)),
                    ("it", testkit::distributed(2)),
                ],
            ),
            &[("it", 2)],
        );
        assert_eq!(
            locate(&half, None, &key("k1"))
                .expect("it is the ranked one")
                .cache,
            "it"
        );
    }

    #[test]
    fn locate_refuses_an_unknown_cache_a_replicated_cache_and_a_cluster_with_no_distributed_cache()
    {
        let model = testkit::fixture_model(Instant::now());
        assert_eq!(
            locate(&model, Some("nope"), &key("k1")),
            Err(LocateError::UnknownCache {
                cache: SmolStr::new("nope"),
                known: vec![
                    SmolStr::new("it"),
                    SmolStr::new("churn"),
                    SmolStr::new("os"),
                    SmolStr::new("pn")
                ],
            }),
            "the Distributed cache is listed first"
        );
        assert_eq!(
            locate(&model, Some("churn"), &key("k1")),
            Err(LocateError::NotDistributed {
                cache: SmolStr::new("churn"),
                mode: Some(Mode::Replicated),
            })
        );

        let replicated = model_of(&snapshot_of(3, &[("side", Mode::Replicated)]), &[]);
        assert_eq!(
            locate(&replicated, None, &key("k1")),
            Err(LocateError::NoDistributedCache)
        );
        assert_eq!(
            locate(&Model::new(), None, &key("k1")),
            Err(LocateError::NoDistributedCache),
            "a model that has seen no member"
        );
        assert_eq!(
            locate(&Model::new(), Some("it"), &key("k1")),
            Err(LocateError::UnknownCache {
                cache: SmolStr::new("it"),
                known: Vec::new(),
            })
        );
    }

    #[test]
    fn locate_says_not_ranked_yet_for_a_distributed_cache_without_a_digest() {
        let model = model_of(&snapshot_of(3, &[("it", testkit::distributed(2))]), &[]);
        let not_ranked = LocateError::NotRankedYet {
            cache: SmolStr::new("it"),
        };
        assert_eq!(
            locate(&model, Some("it"), &key("k1")),
            Err(not_ranked.clone())
        );
        assert_eq!(
            locate(&model, None, &key("k1")),
            Err(not_ranked),
            "advertised but unranked, with no name given"
        );
    }

    #[test]
    fn a_departed_owner_keeps_its_rank_and_loses_its_addresses() {
        let full = snapshot_of(3, &[("it", testkit::distributed(2))]);
        let mut model = model_of(&full, &[("it", 2)]);
        let digest = model.ownership("it").expect("it is ranked").clone();

        // A key whose owners include member 3, which the snapshot then drops.
        let gone = testkit::node_id(3, 0);
        let spec = key_owned_by(&digest, gone);
        let before = locate(&model, Some("it"), &spec).expect("located while listed");

        let remaining = ClusterSnapshot::new(
            "fixture",
            full.members
                .iter()
                .filter(|member| member.peer.node != gone)
                .cloned()
                .collect(),
            0,
        );
        let base = Instant::now();
        model.apply(
            Update::Snapshot(Arc::new(remaining), base),
            base,
            SystemTime::UNIX_EPOCH + Duration::from_secs(101),
        );
        let after = locate(&model, Some("it"), &spec).expect("located after the departure");
        assert_eq!(
            after
                .owners
                .iter()
                .map(|owner| (owner.rank, owner.node))
                .collect::<Vec<_>>(),
            before
                .owners
                .iter()
                .map(|owner| (owner.rank, owner.node))
                .collect::<Vec<_>>(),
            "the digest still ranks the departed owner"
        );
        let departed = after
            .owners
            .iter()
            .find(|owner| owner.node == gone)
            .expect("the departed owner stays");
        assert_eq!(departed.slot, data::UNKNOWN_LABEL);
        assert_eq!(
            (departed.status, departed.gossip, departed.data),
            (None, None, None)
        );
        let listed = after
            .owners
            .iter()
            .find(|owner| owner.node != gone)
            .expect("the other owner");
        assert_ne!(listed.slot, data::UNKNOWN_LABEL);
        assert!(listed.gossip.is_some() && listed.data.is_some());
    }

    #[test]
    fn an_owner_superseded_at_its_address_keeps_its_slot_status_and_addresses() {
        let caches = [("it", testkit::distributed(2))];
        let full = snapshot_of(3, &caches);
        let mut model = model_of(&full, &[("it", 2)]);
        let digest = model.ownership("it").expect("it is ranked").clone();
        let superseded = testkit::node_id(3, 0);
        let spec = key_owned_by(&digest, superseded);

        // Member 3 restarts under a new identity at its address. The snapshot
        // keeps the old identity as down, and the digest still ranks it.
        let down = testkit::member_with(3, 0, 1, MemberStatus::Down, &caches);
        let mut members: Vec<_> = full
            .members
            .iter()
            .filter(|member| member.peer.node != superseded)
            .cloned()
            .collect();
        members.push(down.clone());
        members.push(testkit::member_with(3, 1, 2, MemberStatus::Live, &caches));
        let base = Instant::now();
        model.apply(
            Update::Snapshot(Arc::new(ClusterSnapshot::new("fixture", members, 0)), base),
            base,
            SystemTime::UNIX_EPOCH + Duration::from_secs(101),
        );

        let located = locate(&model, Some("it"), &spec).expect("located after the restart");
        let owner = located
            .owners
            .iter()
            .find(|owner| owner.node == superseded)
            .expect("the digest still ranks the old identity");
        assert_eq!(owner.slot, data::tag_of(&model, superseded).label);
        assert_ne!(owner.slot, data::UNKNOWN_LABEL, "the snapshot lists it");
        assert_eq!(owner.status, Some(MemberStatus::Down));
        assert_eq!(owner.gossip, Some(down.peer.gossip_addr));
        assert_eq!(owner.data, Some(down.peer.data_addr));
    }

    #[test]
    fn a_cache_name_from_gossip_displays_inside_the_allowlist() {
        let hostile = SmolStr::new("it\u{1b}[2J\u{202e}x");
        let errors = [
            LocateError::UnknownCache {
                cache: hostile.clone(),
                known: vec![hostile.clone()],
            },
            LocateError::UnknownCache {
                cache: hostile.clone(),
                known: Vec::new(),
            },
            LocateError::NotDistributed {
                cache: hostile.clone(),
                mode: Some(Mode::Replicated),
            },
            LocateError::NotDistributed {
                cache: hostile.clone(),
                mode: None,
            },
            LocateError::NotRankedYet {
                cache: hostile.clone(),
            },
            LocateError::Ambiguous {
                caches: vec![hostile.clone(), SmolStr::new("ids")],
            },
        ];
        for error in errors {
            let text = error.to_string();
            assert!(text.contains("it·[2J·x"), "{text:?}");
            assert!(
                text.chars()
                    .all(|c| theme::is_allowed(c) && !c.is_control()),
                "{text:?}"
            );
        }
    }

    #[test]
    fn locate_flags_a_cache_whose_advertisers_disagree_on_the_mode() {
        // Members 1 and 2 say Distributed, member 3 says Replicated.
        let snapshot = ClusterSnapshot::new(
            "fixture",
            (1..=3)
                .map(|index| {
                    let mode = if index == 3 {
                        Mode::Replicated
                    } else {
                        testkit::distributed(2)
                    };
                    testkit::member_with(index, 0, 1, MemberStatus::Live, &[("it", mode)])
                })
                .collect(),
            0,
        );
        let model = model_of(&snapshot, &[("it", 2)]);
        assert!(
            data::distributed_caches(&model).is_empty(),
            "the cache is in conflict"
        );

        let located = locate(&model, Some("it"), &key("k1")).expect("the digest exists");
        assert!(located.conflicted);
        assert_eq!(
            located.eligible, 2,
            "only the Distributed advertisers are ranked"
        );
        assert_eq!(
            locate(&model, None, &key("k1")),
            Err(LocateError::NoDistributedCache),
            "a cache in conflict is not picked without a name"
        );

        // Without a digest: a node says Distributed, so the lens ranks the cache
        // once its ownership worker has computed the view.
        let undigested = model_of(
            &ClusterSnapshot::new(
                "fixture",
                (1..=2)
                    .map(|index| {
                        let mode = if index == 2 {
                            Mode::Replicated
                        } else {
                            testkit::distributed(2)
                        };
                        testkit::member_with(index, 0, 1, MemberStatus::Live, &[("it", mode)])
                    })
                    .collect(),
                0,
            ),
            &[],
        );
        assert_eq!(
            locate(&undigested, Some("it"), &key("k1")),
            Err(LocateError::NotRankedYet {
                cache: SmolStr::new("it")
            })
        );

        // Nobody says Distributed: none ever will.
        let never = model_of(
            &ClusterSnapshot::new(
                "fixture",
                (1..=2)
                    .map(|index| {
                        let mode = if index == 2 {
                            Mode::Local
                        } else {
                            Mode::Replicated
                        };
                        testkit::member_with(index, 0, 1, MemberStatus::Live, &[("it", mode)])
                    })
                    .collect(),
                0,
            ),
            &[],
        );
        assert_eq!(
            locate(&never, Some("it"), &key("k1")),
            Err(LocateError::NotDistributed {
                cache: SmolStr::new("it"),
                mode: None
            })
        );
    }

    #[test]
    fn each_locate_error_reads_as_one_sentence_naming_the_remedy() {
        let name = |text: &str| SmolStr::new(text);
        let cases = [
            (
                LocateError::NoDistributedCache,
                "no Distributed cache is advertised: explain needs part ownership",
            ),
            (
                LocateError::UnknownCache {
                    cache: name("nope"),
                    known: vec![name("it"), name("ids")],
                },
                "no live node advertises a cache named nope: name one of it, ids",
            ),
            (
                LocateError::UnknownCache {
                    cache: name("nope"),
                    known: Vec::new(),
                },
                "none advertises any cache: check the cluster name and the seeds",
            ),
            (
                LocateError::NotDistributed {
                    cache: name("churn"),
                    mode: Some(Mode::Replicated),
                },
                "churn is replicated, not Distributed, so no part has owners: name a Distributed cache",
            ),
            (
                LocateError::NotDistributed {
                    cache: name("churn"),
                    mode: None,
                },
                "disagree on its mode and none says Distributed: name a Distributed cache",
            ),
            (
                LocateError::NotRankedYet { cache: name("it") },
                "it is Distributed but the lens has not ranked its parts yet: wait for its ownership and ask again",
            ),
            (
                LocateError::Ambiguous {
                    caches: vec![name("ids"), name("it")],
                },
                "several Distributed caches are ranked (ids, it): name one with --cache",
            ),
        ];
        for (error, expected) in cases {
            let text = error.to_string();
            assert!(text.contains(expected), "{text}");
            assert!(!text.contains('\n') && !text.ends_with('.'), "{text}");
        }
    }
}
