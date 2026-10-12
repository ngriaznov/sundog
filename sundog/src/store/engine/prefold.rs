//! Pre-fold of an `apply_many` batch: grouping a key's consecutive puts into runs, seeding them from the stored record and folding each run into one survivor.

use std::collections::HashMap;

use bytes::Bytes;
use serde::de::DeserializeOwned;

use crate::hlc::Hlc;
#[cfg(feature = "spill")]
use crate::store::spill::SpillLoc;
use crate::store::{ConflictResolver, Incoming};

#[cfg(feature = "spill")]
use super::EntryState;
use super::Stripe;
use super::expiry::decode_expiry;
use super::merge::resolve_and_rebind;
#[cfg(doc)]
use super::merge::resolve_conflict;
#[cfg(all(doc, feature = "spill"))]
use super::prefetch_spilled_conflict_bytes;
use super::record::{record_key, record_value_bytes};
#[cfg(doc)]
use super::{ApplyOutcome, Engine, apply_locked};

/// One [`Engine::apply_many`] batch entry, before or after pre-fold:
/// precomputed hash, the typed key, its wire-encoded bytes, the write's
/// version, and the value or tombstone it carries. Named only to keep
/// [`prefold_batch`]'s and [`Engine::apply_many`]'s signatures under
/// clippy's type-complexity threshold; structurally identical to the plain
/// tuple every caller already builds.
pub(super) type BatchEntry<K, V> = (u64, K, Bytes, Hlc, Incoming<V>);

/// [`prefold_batch`]'s per-run fold: `seed_ver`/`seed_incoming` and every
/// subsequent `(ver, incoming)` in `rest`, in order, folded into one
/// survivor by repeatedly calling [`resolve_and_rebind`], the same function
/// [`apply_locked`] calls against real stored state, with the running
/// accumulator standing in for "stored". Every entry is a real
/// `Incoming::Put`: [`prefold_batch`] never lets a run cross an
/// `Incoming::Tombstone`. Runs entirely outside any stripe lock; see
/// [`prefold_batch`]'s doc for why seeding with the key's real stored
/// record, when there is one, keeps the minted version identical to
/// sequential application's, not only the bytes.
fn fold_run<V: DeserializeOwned>(
    resolver: &dyn ConflictResolver,
    key_bytes: &[u8],
    seed_ver: Hlc,
    seed_incoming: Incoming<V>,
    rest: Vec<(Hlc, Incoming<V>)>,
) -> (Hlc, Incoming<V>) {
    let mut acc_ver = seed_ver;
    let mut acc_incoming = seed_incoming;
    for (ver, incoming) in rest {
        let (stored_encoded, stored_expires_at_ms) = match &acc_incoming {
            Incoming::Put {
                encoded,
                expires_at_ms,
                ..
            } => (Some(encoded.as_ref()), *expires_at_ms),
            Incoming::Tombstone => (None, None),
        };
        if let Some((new_ver, new_incoming)) = resolve_and_rebind(
            resolver,
            key_bytes,
            acc_ver,
            stored_encoded,
            stored_expires_at_ms,
            ver,
            incoming,
        ) {
            acc_ver = new_ver;
            acc_incoming = new_incoming;
        }
    }
    (acc_ver, acc_incoming)
}

/// Groups `entries` by key bytes, preserving each key's own relative order
/// however its entries are interleaved with other keys' in the batch, and
/// splits each key's indices into maximal runs of consecutive
/// `Incoming::Put` entries. A run never crosses an `Incoming::Tombstone`: a
/// tombstone always starts a singleton run of its own, on both sides of it,
/// since `merge` is never consulted against a tombstone's value-less side
/// (see [`resolve_conflict`]), and pre-folding takes the same stance
/// rather than relying on that guard alone. Shared by [`prefold_batch`] and
/// [`Engine::apply_many`]'s seed lookup so both agree on exactly which runs
/// are long enough to fold.
pub(super) fn group_prefold_runs<K, V>(entries: &[BatchEntry<K, V>]) -> Vec<Vec<usize>> {
    let mut by_key: HashMap<Bytes, Vec<usize>> = HashMap::new();
    for (i, entry) in entries.iter().enumerate() {
        by_key.entry(entry.2.clone()).or_default().push(i);
    }
    let mut runs: Vec<Vec<usize>> = Vec::new();
    for indices in by_key.into_values() {
        let mut current: Vec<usize> = Vec::new();
        for idx in indices {
            match &entries[idx].4 {
                Incoming::Put { .. } => current.push(idx),
                Incoming::Tombstone => {
                    if !current.is_empty() {
                        runs.push(std::mem::take(&mut current));
                    }
                    runs.push(vec![idx]);
                }
            }
        }
        if !current.is_empty() {
            runs.push(current);
        }
    }
    runs
}

/// The key's real currently-stored record, in the same value-aware,
/// tombstone/spill-degraded shape [`apply_locked`] itself reads before
/// calling [`resolve_and_rebind`]: `None` against a tombstone, an absent
/// key, or a currently-spilled entry whose bytes weren't already prefetched
/// into `prefetched_spilled` (no value bytes to fold), `Some` with the
/// stored `Hlc`, encoded bytes, and TTL otherwise. Read under a brief stripe
/// read lock, before any decode or fold work runs; see
/// [`Engine::apply_many`]'s call site.
/// [`peek_stored_seed`]'s and [`peek_prefold_seeds`]'s result for one key:
/// the real stored `(version, encoded bytes, TTL)` to seed
/// [`prefold_batch`]'s fold with, or `None` when there is none to seed
/// with. Named only to keep the two functions' signatures under clippy's
/// type-complexity threshold.
pub(super) type PrefoldSeed = Option<(Hlc, Bytes, Option<u64>)>;

pub(super) fn peek_stored_seed<K, V>(
    stripe: &Stripe<K, V>,
    hash: u64,
    key_bytes: &[u8],
    #[cfg(feature = "spill")] prefetched_spilled: &HashMap<SpillLoc, Bytes>,
) -> PrefoldSeed {
    if stripe.tombstones.contains_key(key_bytes) {
        return None;
    }
    let live = stripe
        .live
        .find(hash, |l| record_key(&l.record) == key_bytes)?;
    // `prefetched_spilled` is read off-lock, before this stripe's lock, in
    // `prefetch_spilled_conflict_bytes`; a miss degrades to no seed.
    #[cfg(feature = "spill")]
    let encoded = match &live.state {
        EntryState::Resident => record_value_bytes(&live.record),
        EntryState::Spilled(loc) => prefetched_spilled.get(loc).cloned()?,
    };
    #[cfg(not(feature = "spill"))]
    let encoded = record_value_bytes(&live.record);
    Some((
        live.ver(),
        encoded,
        decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl),
    ))
}

/// [`Engine::apply_many`]'s seed lookup: for every `runs` entry long enough
/// to fold (`len() >= 2`), [`peek_stored_seed`]s that key once, keyed by
/// its wire bytes. Takes the stripe read lock for exactly this pass;
/// [`Engine::apply_many`] drops the guard immediately after this returns,
/// before any decode or fold work, which all runs lock-free afterward in
/// [`prefold_batch`]. `prefetched_spilled`, when the `spill` feature is
/// on, is [`prefetch_spilled_conflict_bytes`]'s result, computed before
/// this same read lock is taken.
pub(super) fn peek_prefold_seeds<K, V>(
    stripe: &Stripe<K, V>,
    entries: &[BatchEntry<K, V>],
    runs: &[Vec<usize>],
    #[cfg(feature = "spill")] prefetched_spilled: &HashMap<SpillLoc, Bytes>,
) -> HashMap<Bytes, PrefoldSeed> {
    let mut seeds = HashMap::new();
    for run in runs {
        if run.len() < 2 {
            continue;
        }
        let (hash, _, key_bytes, ..) = &entries[run[0]];
        seeds.entry(key_bytes.clone()).or_insert_with(|| {
            peek_stored_seed(
                stripe,
                *hash,
                key_bytes.as_ref(),
                #[cfg(feature = "spill")]
                prefetched_spilled,
            )
        });
    }
    seeds
}

/// [`Engine::apply_many`]'s pre-fold: run only when the resolver's
/// [`ConflictResolver::merges`] is `true` and pre-folding is on, folds
/// every maximal run [`group_prefold_runs`] found into one survivor with
/// [`fold_run`], outside any stripe lock. A run whose key has a real stored
/// record ([`peek_prefold_seeds`]) folds that record in first, matching
/// sequential application's order: `merge_version`'s content-join is
/// fold-order independent but its mint arm's running max is not, so seeding
/// first keeps the minted version identical too, not only the bytes. A run
/// with no stored record folds from its own first entry.
///
/// Returns one slot per original index: `Some` where [`apply_locked`] still
/// runs, `None` where a run absorbed the entry ([`Engine::apply_many`]
/// reports those as [`ApplyOutcome::Rejected`]).
pub(super) fn prefold_batch<K, V: DeserializeOwned>(
    entries: Vec<BatchEntry<K, V>>,
    runs: Vec<Vec<usize>>,
    resolver: &dyn ConflictResolver,
    stored_seeds: &HashMap<Bytes, PrefoldSeed>,
) -> Vec<Option<BatchEntry<K, V>>> {
    let mut slots: Vec<Option<BatchEntry<K, V>>> = entries.into_iter().map(Some).collect();
    for run in runs {
        if run.len() < 2 {
            // Alone for this run: nothing to fold, left exactly as given.
            continue;
        }
        let last = *run.last().expect("checked run.len() >= 2 above");
        let mut positions = run.into_iter();
        let seed_idx = positions.next().expect("checked run.len() >= 2 above");
        let (hash, key, key_bytes, seed_ver, seed_incoming) = slots[seed_idx]
            .take()
            .expect("each batch index belongs to at most one run");
        let rest: Vec<(Hlc, Incoming<V>)> = positions
            .map(|idx| {
                let (_, _, _, ver, incoming) = slots[idx]
                    .take()
                    .expect("each batch index belongs to at most one run");
                (ver, incoming)
            })
            .collect();

        // Decoding the peeked stored bytes never happens under the read
        // lock `peek_prefold_seeds` took: that lock is gone by here.
        let stored_decoded = stored_seeds
            .get(key_bytes.as_ref())
            .and_then(Option::as_ref)
            .and_then(|(sv, encoded, expires_at_ms)| {
                postcard::from_bytes::<V>(encoded)
                    .ok()
                    .map(|value| (*sv, value, encoded.clone(), *expires_at_ms))
            });

        let (folded_ver, folded_incoming) = match stored_decoded {
            Some((sv, value, encoded, expires_at_ms)) => {
                let stored_incoming = Incoming::Put {
                    value,
                    expires_at_ms,
                    encoded,
                };
                let mut whole_run = Vec::with_capacity(rest.len() + 1);
                whole_run.push((seed_ver, seed_incoming));
                whole_run.extend(rest);
                fold_run(resolver, key_bytes.as_ref(), sv, stored_incoming, whole_run)
            }
            None => fold_run(resolver, key_bytes.as_ref(), seed_ver, seed_incoming, rest),
        };
        slots[last] = Some((hash, key, key_bytes, folded_ver, folded_incoming));
    }
    slots
}
