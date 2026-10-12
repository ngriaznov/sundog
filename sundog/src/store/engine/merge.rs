//! Conflict resolution for a colliding write: the resolver decision, the merged version and the stamp it mints.

use bytes::Bytes;
use serde::de::DeserializeOwned;
use xxhash_rust::xxh3::xxh3_64;

use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::store::{ConflictResolver, Incoming, Merged, RecordView, Winner};

#[cfg(doc)]
use super::apply_locked;
#[cfg(all(doc, feature = "spill"))]
use super::read_spilled_for_conflict;

/// What consulting the resolver on a real key collision decided, the
/// [`ConflictResolver`]-consultation half of [`apply_locked`]'s decision. See
/// [`super::super::Shard::apply`]'s docs for the correctness contract.
pub(super) enum Resolution {
    /// `incoming` lost outright: nothing changes.
    IncomingLoses,
    /// `incoming` won outright: proceed with `incoming` exactly as given.
    IncomingWins,
    /// The resolver folded both sides into a new value, to be stored (or
    /// not, if [`merge_version`] finds nothing to do) under a version it
    /// decides from `sv`, `ver`, and how the merged bytes compare to each
    /// side's own bytes.
    Merged {
        value: Bytes,
        expires_at_ms: Option<u64>,
    },
}

/// Consults `resolver` on `incoming` at `ver` against whatever is stored at
/// `sv`. Returns [`Resolution::IncomingLoses`] on the equal-version fast
/// path (an already-applied record seen again) without calling `resolver`.
///
/// [`ConflictResolver::merge`] runs only when [`ConflictResolver::merges`]
/// is `true` and both sides carry a real value; a tombstone or an
/// unreadable spilled side never reaches it, and [`ConflictResolver::winner`]
/// decides instead. No side is ever merged against a fabricated value:
/// [`apply_locked`] reads a spilled side's real bytes through
/// [`read_spilled_for_conflict`] first, so a readable spilled side merges
/// like a resident one.
pub(super) fn resolve_conflict<V>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    sv: Hlc,
    stored_encoded: Option<&[u8]>,
    stored_expires_at_ms: Option<u64>,
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
        } => (Some(encoded.as_ref()), *expires_at_ms),
        Incoming::Tombstone => (None, None),
    };
    let incoming_view = RecordView {
        value: incoming_encoded.filter(|_| needs_value_bytes),
        ver,
        expires_at_ms: incoming_expires_at_ms,
    };
    if resolver.merges()
        && stored_view.value.is_some()
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
    match resolver.winner(key_bytes, stored_view, incoming_view) {
        Winner::A => Resolution::IncomingLoses,
        Winner::B => Resolution::IncomingWins,
    }
}

/// What a resolver's [`ConflictResolver::merge`] reply becomes once [`merge_version`]
/// has weighed the two inputs' versions against how the merged bytes compare
/// to each side's own bytes.
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(super) enum MergedVersion {
    /// The merge changes nothing this node doesn't already have at a version
    /// at least as high: no write.
    NoOp,
    /// Store the merged bytes under this version.
    Store(Hlc),
}

/// Decides the version and disposition of a [`ConflictResolver::merge`]
/// reply, from `sv`/`stored` (this stripe's own version and bytes) and
/// `ver`/`incoming` (the colliding write's) against the resolver's folded
/// `merged` bytes. Four outcomes: identical content everywhere adopts the
/// greater of `sv`/`ver` under [`Hlc`]'s `Ord`, or no-ops if `sv` already
/// is; a merge reducing to `incoming` with `ver > sv` stores its `(ver,
/// merged)` pair verbatim; one reducing to `stored` with `sv > ver` is a
/// no-op; otherwise mints a version, [`NodeId::merge_derived`] from the
/// merged bytes, that strictly dominates both inputs under `Hlc`'s `Ord`
/// short of the maximum stamp [`mint_stamp`] describes.
/// `permutation_convergence` and
/// `pn_counter_bidirectional_gossip_converges_in_one_round` in
/// `store::prop_tests` prove this converges, including the one-round case
/// under `cluster::anti_entropy`'s bidirectional exchange.
pub(super) fn merge_version(
    sv: Hlc,
    ver: Hlc,
    stored: &[u8],
    incoming: &[u8],
    merged: &[u8],
) -> MergedVersion {
    merge_decision(sv, ver, merged == stored, merged == incoming, || {
        NodeId::merge_derived(xxh3_64(merged))
    })
}

/// [`merge_version`] over which inputs the merged bytes equal:
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
/// and value [`merge_version`] decides. Returns `None` for a rejected write
/// (outright loss, the engine-enforced tombstone/spill guard, a redelivered
/// already-absorbed merge, or bytes that fail to decode as `V`) and
/// `Some((ver, incoming))` otherwise: unchanged for an outright win, rebound
/// for a merge.
pub(super) fn resolve_and_rebind<V: DeserializeOwned>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    sv: Hlc,
    stored_encoded: Option<&[u8]>,
    stored_expires_at_ms: Option<u64>,
    ver: Hlc,
    incoming: Incoming<V>,
) -> Option<(Hlc, Incoming<V>)> {
    match resolve_conflict(
        resolver,
        key_bytes,
        sv,
        stored_encoded,
        stored_expires_at_ms,
        ver,
        &incoming,
    ) {
        Resolution::IncomingLoses => None,
        Resolution::IncomingWins => Some((ver, incoming)),
        Resolution::Merged {
            value,
            expires_at_ms,
        } => {
            // `resolve_conflict`'s guard confirms both sides carry a real
            // value whenever it returns `Merged`, so these are always
            // `Some`/`Put`; falling back to rejecting rather than trusting
            // that invariant blindly costs nothing.
            let stored = stored_encoded?;
            let Incoming::Put {
                encoded: incoming_encoded,
                ..
            } = &incoming
            else {
                return None;
            };
            match merge_version(sv, ver, stored, incoming_encoded, &value) {
                MergedVersion::NoOp => None,
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
