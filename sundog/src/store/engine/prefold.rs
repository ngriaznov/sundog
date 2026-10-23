//! Pre-fold of an `apply_many` batch: grouping a key's consecutive puts into runs, seeding them from the stored record and folding each run into one survivor.

use std::collections::HashMap;

use bytes::Bytes;
use serde::de::DeserializeOwned;

use crate::hlc::Hlc;
#[cfg(feature = "spill")]
use crate::store::spill::SpillLoc;
use crate::store::{ConflictResolver, Incoming};

use super::ApplyOutcome;
#[cfg(doc)]
use super::Engine;
#[cfg(feature = "spill")]
use super::EntryState;
use super::Stripe;
#[cfg(doc)]
use super::apply::apply_locked;
#[cfg(all(doc, feature = "spill"))]
use super::apply::prefetch_spilled_conflict_bytes;
use super::expiry::{absent_at, decode_expiry};
#[cfg(doc)]
use super::merge::resolve_conflict;
use super::merge::{Fold, MergeCap, Rebound, fold_sides, resolve_and_rebind};
use super::record::{record_key, record_value_bytes};

/// One [`Engine::apply_many`] batch entry, before or after pre-fold:
/// precomputed hash, the typed key, its wire-encoded bytes, the write's
/// version, and the value or tombstone it carries. Named only to keep
/// [`prefold_batch`]'s and [`Engine::apply_many`]'s signatures under
/// clippy's type-complexity threshold; structurally identical to the plain
/// tuple every caller already builds.
pub(super) type BatchEntry<K, V> = (u64, K, Bytes, Hlc, Incoming<V>);

/// What a batch index becomes once [`prefold_batch`] has run.
pub(super) enum Slot<K, V> {
    /// The entry [`apply_locked`] still runs: a write no run touched, the
    /// survivor of a run, or an entry a run left for sequential application.
    Apply(BatchEntry<K, V>),
    /// Absorbed into its run's survivor, whose own [`apply_locked`] call is
    /// this entry's only real contribution. [`Engine::apply_many`] reports it
    /// [`ApplyOutcome::Rejected`].
    Absorbed,
    /// A merge the cap refused while its run folded, which would have left
    /// `size` value bytes under `key_bytes`: nothing of it is stored, and no
    /// [`apply_locked`] call is made for it. [`Engine::apply_many`] reports it
    /// [`ApplyOutcome::TooLarge`].
    Refused { key_bytes: Bytes, size: usize },
}

impl<K, V> Slot<K, V> {
    /// The entry [`apply_locked`] runs, or for an entry [`prefold_batch`]
    /// settled, the outcome [`Engine::apply_many`] reports for it.
    pub(super) fn into_entry(self) -> Result<BatchEntry<K, V>, ApplyOutcome<K, V>> {
        match self {
            Self::Apply(entry) => Ok(entry),
            Self::Absorbed => Err(ApplyOutcome::Rejected),
            Self::Refused { key_bytes, size } => Err(ApplyOutcome::TooLarge { key_bytes, size }),
        }
    }

    /// The entry of a slot [`prefold_batch`] has not touched yet.
    fn entry(&self) -> &BatchEntry<K, V> {
        let Self::Apply(entry) = self else {
            panic!("invariant: each batch index belongs to at most one run");
        };
        entry
    }

    /// Takes the entry out of a slot that holds one, leaving it absorbed.
    fn take(&mut self) -> BatchEntry<K, V> {
        let Self::Apply(entry) = std::mem::replace(self, Self::Absorbed) else {
            panic!("invariant: each batch index belongs to at most one run");
        };
        entry
    }
}

/// What [`fold_run`] leaves: the accumulator, the steps the cap refused, and
/// the entries it stopped before.
struct FoldedRun<V> {
    ver: Hlc,
    incoming: Incoming<V>,
    /// The steps the cap refused, each as its position among the entries
    /// [`fold_run`] was given and the value bytes the merge would have left,
    /// in order.
    refused: Vec<(usize, usize)>,
    /// The tail of the run [`fold_run`] did not consume, in order.
    remaining: Vec<(Hlc, Incoming<V>)>,
}

/// [`prefold_batch`]'s per-run fold: `seed_ver`/`seed_incoming` and every
/// subsequent `(ver, incoming)` in `rest`, in order, folded into one
/// survivor by repeatedly calling [`resolve_and_rebind`], the same function
/// [`apply_locked`] calls against real stored state, with the running
/// accumulator standing in for "stored"; `seed_absent` says the seed is a
/// stored entry, or a record already past its deadline at `now_ms`, that
/// reads as absent and never folds. Every entry is a real
/// `Incoming::Put`: [`prefold_batch`] never lets a run cross an
/// `Incoming::Tombstone`. Runs entirely outside any stripe lock; see
/// [`prefold_batch`]'s doc for why seeding with the key's real stored
/// record, when there is one, keeps the minted version identical to
/// sequential application's, not only the bytes.
///
/// `carries_stored` says the accumulator holds a live stored record's
/// content. Dropping a lapsed side is not a join, so once the accumulator
/// carries the stored record the fold stops before the first step that
/// [`fold_sides`] does not decide as [`Fold::Both`]: the survivor, applied
/// against that same stored record, would otherwise fold the content the
/// step dropped back in. The unconsumed tail is returned for sequential
/// application.
///
/// A step `cap` refuses ([`Rebound::TooLarge`]) changes nothing, as in
/// sequential application, where a refused merge leaves the stored record as
/// it is: the accumulator stays the content sequential application would have
/// stored, the fold goes on with the next step, and the refusal is returned in
/// [`FoldedRun::refused`]. A refused step is never handed to [`apply_locked`],
/// which would merge it again under the stripe's write lock, once for every
/// refused step of a run against a record at the cap.
#[allow(
    clippy::too_many_arguments,
    reason = "one caller; the seed, its absence, the clock and the run are each a distinct input"
)]
fn fold_run<V: DeserializeOwned>(
    resolver: &dyn ConflictResolver,
    cap: Option<MergeCap>,
    key_bytes: &[u8],
    seed_ver: Hlc,
    seed_incoming: Incoming<V>,
    seed_absent: bool,
    carries_stored: bool,
    now_ms: u64,
    rest: Vec<(Hlc, Incoming<V>)>,
) -> FoldedRun<V> {
    let mut acc_ver = seed_ver;
    let mut acc_incoming = seed_incoming;
    let mut acc_absent = seed_absent;
    let mut refused = Vec::new();
    let mut step = 0usize;
    let mut rest = rest.into_iter().peekable();
    while let Some((ver, incoming)) = rest.peek() {
        let (stored_encoded, stored_expires_at_ms) = match &acc_incoming {
            Incoming::Put {
                encoded,
                expires_at_ms,
                ..
            } => (Some(encoded.as_ref()), *expires_at_ms),
            Incoming::Tombstone { .. } => (None, None),
        };
        let incoming_expires_at_ms = match incoming {
            Incoming::Put { expires_at_ms, .. } => *expires_at_ms,
            Incoming::Tombstone { .. } => None,
        };
        if carries_stored
            && fold_sides(
                acc_absent,
                acc_ver,
                stored_expires_at_ms,
                *ver,
                incoming_expires_at_ms,
            ) != Fold::Both
        {
            break;
        }
        let (ver, incoming) = rest.next().expect("just peeked");
        match resolve_and_rebind(
            resolver,
            cap,
            key_bytes,
            acc_ver,
            stored_encoded,
            stored_expires_at_ms,
            acc_absent,
            matches!(acc_incoming, Incoming::Tombstone { .. }),
            ver,
            incoming,
        ) {
            Rebound::Store(new_ver, new_incoming) => {
                acc_ver = new_ver;
                acc_incoming = new_incoming;
                // The stored entry is displaced: every later step folds
                // against the running accumulator, which sequential
                // application would have stored, and which reads as absent
                // once `now_ms` reaches its deadline. A rejected step leaves
                // the stored entry, still absent, in place.
                acc_absent = reaches_deadline(&acc_incoming, now_ms);
            }
            Rebound::Lost => {}
            Rebound::TooLarge { size } => refused.push((step, size)),
        }
        step += 1;
    }
    FoldedRun {
        ver: acc_ver,
        incoming: acc_incoming,
        refused,
        remaining: rest.collect(),
    }
}

/// Whether `incoming` is a put whose deadline `now_ms` has reached: the
/// record reads as absent the moment sequential application stores it.
pub(super) fn reaches_deadline<V>(incoming: &Incoming<V>, now_ms: u64) -> bool {
    matches!(
        incoming,
        Incoming::Put { expires_at_ms: Some(deadline), .. } if now_ms >= *deadline
    )
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
///
/// `leading_run_only` cuts a key at its first tombstone: the tombstone and
/// every entry after it is a singleton run, which nothing folds. A later run
/// would fold from the record stored before the batch, but by the time it
/// applies, the key's earlier entries have changed that record, and a delete
/// among them has removed content the fold would put back. A cap decides each
/// merge against the record it meets, so that fold is refused at other steps
/// than sequential application refuses it. A key's leading run starts from
/// the stored record itself and folds exactly. [`Engine::apply_many`] sets it
/// when a cap is in force.
pub(super) fn group_prefold_runs<K, V>(
    entries: &[BatchEntry<K, V>],
    leading_run_only: bool,
) -> Vec<Vec<usize>> {
    let mut by_key: HashMap<Bytes, Vec<usize>> = HashMap::new();
    for (i, entry) in entries.iter().enumerate() {
        by_key.entry(entry.2.clone()).or_default().push(i);
    }
    let mut runs: Vec<Vec<usize>> = Vec::new();
    for indices in by_key.into_values() {
        let mut current: Vec<usize> = Vec::new();
        let mut past_leading_run = false;
        for idx in indices {
            if past_leading_run {
                runs.push(vec![idx]);
                continue;
            }
            match &entries[idx].4 {
                Incoming::Put { .. } => current.push(idx),
                Incoming::Tombstone { .. } => {
                    if !current.is_empty() {
                        runs.push(std::mem::take(&mut current));
                    }
                    runs.push(vec![idx]);
                    past_leading_run = leading_run_only;
                }
            }
        }
        if !current.is_empty() {
            runs.push(current);
        }
    }
    runs
}

/// What [`peek_stored_seed`] and [`peek_prefold_seeds`] find stored for one
/// key, as [`prefold_batch`] seeds a run's fold with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PrefoldSeed {
    /// The key has no stored entry: the run folds from its own first entry.
    Vacant,
    /// The key's real stored record: its version, encoded bytes, TTL, and
    /// whether it reads as absent at the batch's `now_ms` (expired, or idle
    /// for `tti_ms`).
    Stored(Hlc, Bytes, Option<u64>, bool),
    /// The stored side is a tombstone, or a spilled entry whose bytes were
    /// not prefetched: sequential application decides each of the run's
    /// entries against it on its own, which a fold of the run cannot
    /// reproduce, so the run is left to [`apply_locked`] entry by entry.
    Sequential,
}

/// The key's real currently-stored record, in the same value-aware,
/// tombstone/spill-degraded shape [`apply_locked`] itself reads before
/// calling [`resolve_and_rebind`]: [`PrefoldSeed::Sequential`] against a
/// tombstone or a currently-spilled entry whose bytes weren't already
/// prefetched into `prefetched_spilled` (no value bytes to fold),
/// [`PrefoldSeed::Vacant`] for an absent key, and [`PrefoldSeed::Stored`]
/// with the stored `Hlc`, encoded bytes, TTL, and whether the entry reads as
/// absent at `now_ms` (expired, or idle for `tti_ms`) otherwise. Read under
/// a brief stripe read lock, before any decode or fold work runs; see
/// [`Engine::apply_many`]'s call site.
pub(super) fn peek_stored_seed<K, V>(
    stripe: &Stripe<K, V>,
    hash: u64,
    key_bytes: &[u8],
    tti_ms: Option<u64>,
    now_ms: u64,
    #[cfg(feature = "spill")] prefetched_spilled: &HashMap<SpillLoc, Bytes>,
) -> PrefoldSeed {
    if stripe.tombstones.contains_key(key_bytes) {
        return PrefoldSeed::Sequential;
    }
    let Some(live) = stripe
        .live
        .find(hash, |l| record_key(&l.record) == key_bytes)
    else {
        return PrefoldSeed::Vacant;
    };
    // `prefetched_spilled` is read off-lock, before this stripe's lock, in
    // `prefetch_spilled_conflict_bytes`; a miss leaves the run sequential.
    #[cfg(feature = "spill")]
    let Some(encoded) = (match &live.state {
        EntryState::Resident => Some(record_value_bytes(&live.record)),
        EntryState::Spilled(loc) => prefetched_spilled.get(loc).cloned(),
    }) else {
        return PrefoldSeed::Sequential;
    };
    #[cfg(not(feature = "spill"))]
    let encoded = record_value_bytes(&live.record);
    PrefoldSeed::Stored(
        live.ver(),
        encoded,
        decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl),
        absent_at(live, key_bytes, &stripe.long_ttl, tti_ms, now_ms),
    )
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
    tti_ms: Option<u64>,
    now_ms: u64,
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
                tti_ms,
                now_ms,
                #[cfg(feature = "spill")]
                prefetched_spilled,
            )
        });
    }
    seeds
}

/// What one entry of a folded run becomes; see [`plan_run`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunSlot {
    /// Absorbed into the run's survivor.
    Absorbed,
    /// The run's survivor, which [`apply_locked`] still applies.
    Survivor,
    /// A merge the cap refused, which would have left this many value bytes.
    Refused(usize),
    /// An entry the fold stopped before, left for sequential application.
    Unconsumed,
}

/// What each of a run's `len` entries becomes once [`fold_run`] has consumed
/// the first `consumed` of them. `refused` is [`FoldedRun::refused`], whose
/// positions count from the first entry [`fold_run`] merged. That entry is
/// the run's first when a stored record seeds the accumulator (`first_step`
/// 0), and its second when the run's own first entry does (`first_step` 1).
/// The survivor takes the last consumed position no refusal took, and none
/// when the cap refused every step of a run against a stored record, which
/// leaves that record as the survivor and needs no apply. One pass over the
/// run, so a run of many refused steps costs no more than the fold that
/// refused them.
pub(super) fn plan_run(
    len: usize,
    consumed: usize,
    first_step: usize,
    refused: &[(usize, usize)],
) -> Vec<RunSlot> {
    let mut plan = vec![RunSlot::Absorbed; len];
    plan[consumed..].fill(RunSlot::Unconsumed);
    for &(step, size) in refused {
        plan[step + first_step] = RunSlot::Refused(size);
    }
    if let Some(last) = plan[..consumed]
        .iter()
        .rposition(|slot| *slot == RunSlot::Absorbed)
    {
        plan[last] = RunSlot::Survivor;
    }
    plan
}

/// [`Engine::apply_many`]'s pre-fold: run only when the resolver's
/// [`ConflictResolver::merges`] is `true` and pre-folding is on, folds
/// every maximal run [`group_prefold_runs`] found into one survivor with
/// [`fold_run`], outside any stripe lock. A run whose key has a real stored
/// record ([`peek_prefold_seeds`]) folds that record in first, matching
/// sequential application's order: `merge_version`'s content-join is
/// fold-order independent but its mint arm's running max is not, so seeding
/// first keeps the minted version identical too, not only the bytes. A run
/// with no stored entry folds from its own first entry. A run whose stored
/// side is a tombstone, or cannot be read, is not folded
/// ([`PrefoldSeed::Sequential`]): its entries stay in their own slots. A run that
/// [`fold_run`] stops early folds only its consumed prefix; the rest of its
/// entries stay in their own slots.
///
/// Returns one [`Slot`] per original index: [`Slot::Apply`] where
/// [`apply_locked`] still runs, [`Slot::Absorbed`] where a run absorbed the
/// entry, and [`Slot::Refused`] where `cap` refused the merge of the entry, at
/// the entry's own index. The survivor takes the last slot of the run's
/// consumed prefix that no refusal took; a run whose steps the cap all
/// refused against a stored record leaves that record as the survivor, which
/// needs no apply.
///
/// Each run folds from the record its key holds when the batch starts, which
/// only a key's leading run meets in sequential application. Under a cap
/// [`group_prefold_runs`] leaves the rest of a key's entries after its first
/// tombstone unfolded, so the folded batch refuses the same entries as
/// sequential application and stores the same record.
pub(super) fn prefold_batch<K, V: DeserializeOwned>(
    entries: Vec<BatchEntry<K, V>>,
    runs: Vec<Vec<usize>>,
    resolver: &dyn ConflictResolver,
    cap: Option<MergeCap>,
    stored_seeds: &HashMap<Bytes, PrefoldSeed>,
    now_ms: u64,
) -> Vec<Slot<K, V>> {
    let mut slots: Vec<Slot<K, V>> = entries.into_iter().map(Slot::Apply).collect();
    for run in runs {
        if run.len() < 2 {
            // Alone for this run: nothing to fold, left exactly as given.
            continue;
        }
        let key_seed = stored_seeds.get(slots[run[0]].entry().2.as_ref());
        // Decoding the peeked stored bytes never happens under the read
        // lock `peek_prefold_seeds` took: that lock is gone by here. Bytes
        // that do not decode leave the run to sequential application.
        let stored_decoded = match key_seed {
            Some(PrefoldSeed::Sequential) => continue,
            Some(PrefoldSeed::Stored(sv, encoded, expires_at_ms, absent)) => {
                let Ok(value) = postcard::from_bytes::<V>(encoded) else {
                    continue;
                };
                Some((*sv, value, encoded.clone(), *expires_at_ms, *absent))
            }
            Some(PrefoldSeed::Vacant) | None => None,
        };
        let mut parts: Vec<(u64, K, Bytes)> = Vec::with_capacity(run.len());
        let mut versioned: Vec<(Hlc, Incoming<V>)> = Vec::with_capacity(run.len());
        for &idx in &run {
            let (hash, key, key_bytes, ver, incoming) = slots[idx].take();
            parts.push((hash, key, key_bytes));
            versioned.push((ver, incoming));
        }
        let key_bytes = parts[0].2.clone();
        // The position among the run's entries of the first one the fold
        // merges: the first of all against a stored record, which seeds the
        // accumulator, and the second without one, where the run's own first
        // entry seeds it.
        let first_step = usize::from(stored_decoded.is_none());

        let folded = if let Some((sv, value, encoded, expires_at_ms, absent)) = stored_decoded {
            let stored_incoming = Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            };
            fold_run(
                resolver,
                cap,
                key_bytes.as_ref(),
                sv,
                stored_incoming,
                absent,
                !absent,
                now_ms,
                versioned,
            )
        } else {
            let mut versioned = versioned.into_iter();
            let (seed_ver, seed_incoming) = versioned.next().expect("checked run.len() >= 2 above");
            let seed_absent = reaches_deadline(&seed_incoming, now_ms);
            fold_run(
                resolver,
                cap,
                key_bytes.as_ref(),
                seed_ver,
                seed_incoming,
                seed_absent,
                false,
                now_ms,
                versioned.collect(),
            )
        };
        // The run's entries consumed so far, counting the unseeded fold's
        // own first entry, which seeds the accumulator.
        let consumed = run.len() - folded.remaining.len();
        let plan = plan_run(run.len(), consumed, first_step, &folded.refused);
        let mut survivor = Some((folded.ver, folded.incoming));
        let mut remaining = folded.remaining.into_iter();
        for ((&idx, (hash, key, key_bytes)), planned) in run.iter().zip(parts).zip(plan) {
            match planned {
                // The slot `take` emptied already reads absorbed.
                RunSlot::Absorbed => {}
                RunSlot::Survivor => {
                    let (ver, incoming) = survivor.take().expect("a run has one survivor");
                    slots[idx] = Slot::Apply((hash, key, key_bytes, ver, incoming));
                }
                RunSlot::Refused(size) => slots[idx] = Slot::Refused { key_bytes, size },
                RunSlot::Unconsumed => {
                    let (ver, incoming) = remaining
                        .next()
                        .expect("one remaining entry for each unconsumed index");
                    slots[idx] = Slot::Apply((hash, key, key_bytes, ver, incoming));
                }
            }
        }
    }
    slots
}
