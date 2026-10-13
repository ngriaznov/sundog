//! Conflict resolution for a colliding write: the resolver decision, the merged version and the stamp it mints.

use bytes::Bytes;
use serde::de::DeserializeOwned;
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::store::{ConflictResolver, Incoming, Merged, RecordView, Winner};

#[cfg(doc)]
use super::apply::apply_locked;
#[cfg(all(doc, feature = "spill"))]
use super::apply::read_spilled_for_conflict;

/// What consulting the resolver on a real key collision decided, the
/// [`ConflictResolver`]-consultation half of [`apply_locked`]'s decision. See
/// [`super::super::Shard::apply`]'s docs for the correctness contract.
pub(super) enum Resolution {
    /// `incoming` lost outright: nothing changes.
    IncomingLoses,
    /// `incoming` won outright: proceed with `incoming` exactly as given.
    IncomingWins,
    /// The collision settled on a content to store (or not, if
    /// [`merge_version`] finds nothing to do) under a version it decides
    /// from `sv`, `ver`, and how the content compares to each side's own:
    /// the resolver's fold of both sides, or the one side [`fold_sides`]
    /// keeps.
    Merged {
        value: Bytes,
        expires_at_ms: Option<u64>,
    },
}

/// Whether a record whose deadline is `expires_at_ms` had lapsed when the
/// write stamped `other_ver` was made: its deadline is set and at or before
/// that write's wall clock. Decided from the two stamps alone, so every
/// replica reaches the same answer for the same pair of records.
pub(super) fn lapsed_before(expires_at_ms: Option<u64>, other_ver: Hlc) -> bool {
    expires_at_ms.is_some_and(|deadline| deadline <= other_ver.wall_ms)
}

/// Which content a merging resolver's collision keeps, as [`fold_sides`]
/// decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fold {
    /// Both sides are alive: [`ConflictResolver::merge`] folds them.
    Both,
    /// Only the stored side is alive: the result is the stored record.
    Stored,
    /// Only the incoming side is alive: the result is the incoming record.
    Incoming,
    /// Neither side is alive: [`ConflictResolver::winner`] picks one.
    Winner,
}

/// [`Fold`] for a collision between the stored record (version `sv`,
/// deadline `stored_expires_at_ms`) and the incoming write (version `ver`,
/// deadline `incoming_expires_at_ms`). A side that [`lapsed_before`] the
/// other side's write never folds; `stored_absent` marks a stored entry that
/// reads as absent on this node (expired by the local clock, or idle past
/// `tti`), which never folds either.
pub(crate) fn fold_sides(
    stored_absent: bool,
    sv: Hlc,
    stored_expires_at_ms: Option<u64>,
    ver: Hlc,
    incoming_expires_at_ms: Option<u64>,
) -> Fold {
    let stored_dead = stored_absent || lapsed_before(stored_expires_at_ms, ver);
    let incoming_dead = lapsed_before(incoming_expires_at_ms, sv);
    match (stored_dead, incoming_dead) {
        (false, false) => Fold::Both,
        (true, false) => Fold::Incoming,
        (false, true) => Fold::Stored,
        (true, true) => Fold::Winner,
    }
}

/// Consults `resolver` on `incoming` at `ver` against whatever is stored at
/// `sv`. Returns [`Resolution::IncomingLoses`] on the equal-version fast
/// path (an already-applied record seen again) without calling `resolver`.
///
/// When [`ConflictResolver::merges`] is `true` and `incoming` is a put,
/// [`fold_sides`] decides which content survives. [`Fold::Both`] runs
/// [`ConflictResolver::merge`], which needs a real value on both sides: a
/// tombstone or an unreadable spilled side never reaches it, and
/// [`ConflictResolver::winner`] decides instead. [`Fold::Incoming`] keeps
/// the incoming record without reading the stored side's bytes, and
/// [`Fold::Stored`] keeps the stored record. A lapsed side never folds into
/// a merge, so no resolver merges a dead value into a live one. No side is
/// ever merged against a fabricated value: [`apply_locked`] reads a spilled
/// side's real bytes through [`read_spilled_for_conflict`] first, so a
/// readable spilled side merges like a resident one. `stored_absent` is
/// `true` when the stored entry reads as absent on this node, and `false`
/// for a tombstone.
#[allow(
    clippy::too_many_arguments,
    reason = "the two sides of a collision, each with its version, bytes and deadline, plus the resolver and key"
)]
pub(super) fn resolve_conflict<V>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    sv: Hlc,
    stored_encoded: Option<&[u8]>,
    stored_expires_at_ms: Option<u64>,
    stored_absent: bool,
    ver: Hlc,
    incoming: &Incoming<V>,
) -> Resolution {
    if sv == ver {
        return Resolution::IncomingLoses;
    }
    let needs_value_bytes = resolver.needs_value_bytes();
    let stored_view = RecordView {
        value: stored_encoded.filter(|_| needs_value_bytes),
        ver: sv,
        expires_at_ms: stored_expires_at_ms,
    };
    let (incoming_encoded, incoming_expires_at_ms) = match incoming {
        Incoming::Put {
            encoded,
            expires_at_ms,
            ..
        } => (Some(encoded), *expires_at_ms),
        Incoming::Tombstone => (None, None),
    };
    let incoming_view = RecordView {
        value: incoming_encoded
            .map(AsRef::as_ref)
            .filter(|_| needs_value_bytes),
        ver,
        expires_at_ms: incoming_expires_at_ms,
    };
    if resolver.merges()
        && let Some(incoming_bytes) = incoming_encoded
    {
        match fold_sides(
            stored_absent,
            sv,
            stored_expires_at_ms,
            ver,
            incoming_expires_at_ms,
        ) {
            Fold::Both => {
                if stored_view.value.is_some()
                    && incoming_view.value.is_some()
                    && let Some(Merged {
                        value,
                        expires_at_ms,
                    }) = resolver.merge(key_bytes, stored_view, incoming_view)
                {
                    return Resolution::Merged {
                        value,
                        expires_at_ms,
                    };
                }
            }
            Fold::Incoming => {
                return Resolution::Merged {
                    value: incoming_bytes.clone(),
                    expires_at_ms: incoming_expires_at_ms,
                };
            }
            Fold::Stored => {
                if let Some(stored_bytes) = stored_view.value {
                    return Resolution::Merged {
                        value: Bytes::copy_from_slice(stored_bytes),
                        expires_at_ms: stored_expires_at_ms,
                    };
                }
            }
            Fold::Winner => {}
        }
    }
    match resolver.winner(key_bytes, stored_view, incoming_view) {
        Winner::A => Resolution::IncomingLoses,
        Winner::B => Resolution::IncomingWins,
    }
}

/// What a resolver's [`ConflictResolver::merge`] reply becomes once [`merge_version`]
/// has weighed the two inputs' versions against how the merged content
/// compares to each side's own content.
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(super) enum MergedVersion {
    /// The merge changes nothing this node doesn't already have at a version
    /// at least as high: no write.
    NoOp,
    /// Store the merged content under this version.
    Store(Hlc),
}

/// A record's content as [`merge_version`] compares it: its value bytes and
/// its deadline. Two records with equal bytes and different deadlines are
/// different content.
pub(super) type Content<'a> = (&'a [u8], Option<u64>);

/// The 64-bit hash that names a minted version's node: `xxh3_64` of the
/// bytes alone when the record has no deadline, so a no-deadline merge mints
/// the same version on every release, and of the bytes followed by the
/// deadline's little-endian bytes when it has one.
pub(super) fn content_hash(bytes: &[u8], expires_at_ms: Option<u64>) -> u64 {
    match expires_at_ms {
        None => xxh3_64(bytes),
        Some(deadline) => {
            let mut hasher = Xxh3::new();
            hasher.update(bytes);
            hasher.update(&deadline.to_le_bytes());
            hasher.digest()
        }
    }
}

/// Decides the version and disposition of a [`ConflictResolver::merge`]
/// reply, from `sv`/`stored` (this stripe's own version and content) and
/// `ver`/`incoming` (the colliding write's) against the resolver's folded
/// `merged` content, each a `(bytes, deadline)` pair. `stored` is `None`
/// when its bytes are excluded or unreadable, so the merge never reduces to
/// it. Four outcomes: identical content everywhere adopts the greater of
/// `sv`/`ver` under [`Hlc`]'s `Ord`, or no-ops if `sv` already is; a merge
/// reducing to `incoming` with `ver > sv` stores its `(ver, merged)` pair
/// verbatim; one reducing to `stored` with `sv > ver` is a no-op; otherwise
/// mints a version, [`NodeId::merge_derived`] from [`content_hash`] of the
/// merged content, that strictly dominates both inputs under `Hlc`'s `Ord`
/// short of the maximum stamp [`mint_stamp`] describes.
/// `permutation_convergence` and
/// `pn_counter_bidirectional_gossip_converges_in_one_round` in
/// `store::prop_tests` prove this converges, including the one-round case
/// under `cluster::anti_entropy`'s bidirectional exchange.
pub(super) fn merge_version(
    sv: Hlc,
    ver: Hlc,
    stored: Option<Content<'_>>,
    incoming: Content<'_>,
    merged: Content<'_>,
) -> MergedVersion {
    merge_decision(sv, ver, stored == Some(merged), incoming == merged, || {
        NodeId::merge_derived(content_hash(merged.0, merged.1))
    })
}

/// [`merge_version`] over which inputs the merged content equals:
/// `is_stored` for `stored`, `is_incoming` for `incoming`. `mint_node`
/// names the minted version's node and runs only on the mint arm.
pub(super) fn merge_decision(
    sv: Hlc,
    ver: Hlc,
    is_stored: bool,
    is_incoming: bool,
    mint_node: impl FnOnce() -> NodeId,
) -> MergedVersion {
    if is_stored && is_incoming {
        let v = sv.max(ver);
        if v == sv {
            MergedVersion::NoOp
        } else {
            MergedVersion::Store(v)
        }
    } else if is_incoming && ver > sv {
        MergedVersion::Store(ver)
    } else if is_stored && sv > ver {
        MergedVersion::NoOp
    } else {
        MergedVersion::Store(mint_stamp(sv, ver, mint_node()))
    }
}

/// A version above both `sv` and `ver`: the larger `wall_ms` with one past
/// the larger `logical`, carrying into `wall_ms` when `logical` has no room
/// left. Where both are already at their maximum no stamp can be larger,
/// and the mint ties both inputs on `wall_ms` and `logical`; the clock-skew
/// guard keeps a real stamp nowhere near that.
pub(super) fn mint_stamp(sv: Hlc, ver: Hlc, node: NodeId) -> Hlc {
    let base_wall = sv.wall_ms.max(ver.wall_ms);
    let base_logical = sv.logical.max(ver.logical);
    let (wall_ms, logical) = match (base_logical.checked_add(1), base_wall.checked_add(1)) {
        (Some(logical), _) => (base_wall, logical),
        (None, Some(wall_ms)) => (wall_ms, 0),
        (None, None) => (u64::MAX, u32::MAX),
    };
    Hlc {
        wall_ms,
        logical,
        node,
    }
}

/// [`apply_locked`]'s full collision decision: consults [`resolve_conflict`]
/// and, on [`Resolution::Merged`], rebinds `ver`/`incoming` to the version
/// and content [`merge_version`] decides. Returns `None` for a rejected write
/// (outright loss, the engine-enforced tombstone/spill guard, a redelivered
/// already-absorbed merge, or bytes that fail to decode as `V`) and
/// `Some((ver, incoming))` otherwise: unchanged for an outright win, rebound
/// for a merge. `stored_absent` is [`resolve_conflict`]'s.
#[allow(
    clippy::too_many_arguments,
    reason = "the two sides of a collision, each with its version, bytes and deadline, plus the resolver and key"
)]
pub(super) fn resolve_and_rebind<V: DeserializeOwned>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    sv: Hlc,
    stored_encoded: Option<&[u8]>,
    stored_expires_at_ms: Option<u64>,
    stored_absent: bool,
    ver: Hlc,
    incoming: Incoming<V>,
) -> Option<(Hlc, Incoming<V>)> {
    match resolve_conflict(
        resolver,
        key_bytes,
        sv,
        stored_encoded,
        stored_expires_at_ms,
        stored_absent,
        ver,
        &incoming,
    ) {
        Resolution::IncomingLoses => None,
        Resolution::IncomingWins => Some((ver, incoming)),
        Resolution::Merged {
            value,
            expires_at_ms,
        } => {
            // `resolve_conflict` returns `Merged` only for a put, so this is
            // always `Put`; rejecting rather than trusting that invariant
            // blindly costs nothing.
            let Incoming::Put {
                encoded: incoming_encoded,
                expires_at_ms: incoming_expires_at_ms,
                ..
            } = &incoming
            else {
                return None;
            };
            let result_is_incoming =
                value == *incoming_encoded && expires_at_ms == *incoming_expires_at_ms;
            // An absent stored entry is displaced, never retained, so the
            // result is never "the stored record".
            let stored = stored_encoded
                .filter(|_| !stored_absent)
                .map(|bytes| (bytes, stored_expires_at_ms));
            let merged_version = merge_version(
                sv,
                ver,
                stored,
                (incoming_encoded.as_ref(), *incoming_expires_at_ms),
                (value.as_ref(), expires_at_ms),
            );
            match merged_version {
                MergedVersion::NoOp => None,
                // The result is the incoming record: its decoded value is
                // already in hand.
                MergedVersion::Store(new_ver) if result_is_incoming => Some((new_ver, incoming)),
                MergedVersion::Store(new_ver) => {
                    // Never trust a resolver's bytes blindly: reject, don't panic.
                    let Ok(decoded): Result<V, _> = postcard::from_bytes(&value) else {
                        return None;
                    };
                    Some((
                        new_ver,
                        Incoming::Put {
                            value: decoded,
                            expires_at_ms,
                            encoded: value,
                        },
                    ))
                }
            }
        }
    }
}
