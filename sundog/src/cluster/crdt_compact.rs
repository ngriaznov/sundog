//! CRDT writer retirement and compaction scheduling for a merging cache:
//! decides which writer slots are dead, whether the whole cache is quiet
//! enough for stage two folding, and runs the sweep on a timer.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;

use super::{Cluster, absence};
use crate::membership::{self, CacheModes, Peer};
use crate::node::NodeId;
use crate::store::{CompactionBounds, Quiescence, ShardOps, crdt, now_ms};

/// Whether writer `w` is eligible for CRDT retirement this tick: `w`'s
/// node is dead (see [`membership::incarnation_is_dead`]) and `w` isn't
/// `local`, this node's own current writer identity. The second guard is
/// defense in depth: a live node's own current incarnation is already
/// unreachable through `incarnation_is_dead`, but keeping the check
/// explicit keeps the invariant holding even if that stops being
/// incidentally true. A member with no live incarnation and no absent time
/// is not dead, so the caller supplies the absent time of a writer's node
/// it never observed ([`crdt_writer_absent_since`]).
fn crdt_writer_is_retirement_eligible(
    local: crdt::WriterId,
    w: crdt::WriterId,
    member: &membership::MemberView,
    now: Instant,
    bound: Duration,
) -> bool {
    w != local && membership::incarnation_is_dead(member, w.incarnation(), now, bound)
}

/// The instant a writer's node counts as absent when no live peer and no
/// tracked gone time describes it. A tracked gone time is exact. A node
/// this process never observed counts as absent from the later of two
/// instants: the cluster's start, because the process cannot have seen the
/// node live before it started, and the sweep's first sight of the writer,
/// so a live node that gossip has not yet shown to this process is not
/// retired on the strength of one tick: it is a peer long before the
/// retirement bound elapses, and then its live incarnation decides.
fn crdt_writer_absent_since(
    gone_since: Option<Instant>,
    first_unseen: Instant,
    node_started: Instant,
) -> Instant {
    gone_since.unwrap_or_else(|| first_unseen.max(node_started))
}

/// Whether one sweep has walked every stripe of the keyspace, so a writer
/// it did not meet is held by no resident record.
fn keyspace_walked(stripes_visited: usize) -> bool {
    stripes_visited >= crate::store::BUCKET_COUNT
}

/// Where the sweep first met each writer whose node it knows nothing about:
/// neither a live peer nor a tracked gone member. [`crdt_writer_absent_since`]
/// reads the instant as the start of the writer's absence.
///
/// An entry lives while the sweep keeps meeting the writer. After a sweep
/// that walks the whole keyspace, [`end_cycle`](Self::end_cycle) drops every
/// entry the sweep did not touch: the writer became known, was retired and
/// folded away, or no resident record holds it any more. A sweep that stops
/// short drops nothing, so an entry never goes because one pass reached
/// only part of the keyspace.
#[derive(Default)]
pub(crate) struct UnseenWriters {
    state: Mutex<UnseenState>,
}

#[derive(Default)]
struct UnseenState {
    first_seen: HashMap<crdt::WriterId, Instant>,
    touched: HashSet<crdt::WriterId>,
}

impl UnseenWriters {
    /// When the sweep first met `writer` as unknown, recorded at `now` on
    /// the first call; marks the writer as met in this cycle.
    fn first_sight(&self, writer: crdt::WriterId, now: Instant) -> Instant {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.touched.insert(writer);
        *state.first_seen.entry(writer).or_insert(now)
    }

    /// Ends a cycle over the whole keyspace: forgets every writer the cycle
    /// did not meet.
    fn end_cycle(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let UnseenState {
            first_seen,
            touched,
        } = &mut *state;
        first_seen.retain(|writer, _| touched.contains(writer));
        touched.clear();
    }

    /// How many writers are tracked.
    #[cfg(all(test, not(feature = "sim")))]
    fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first_seen
            .len()
    }
}

/// Whether an entire cache is quiet this tick: every relevant member has
/// settled (see [`membership::member_is_quiet`]), continuously present or
/// continuously gone for at least `bound`. Vacuously [`Quiescence::Settled`]
/// with no other known members. Only stage two of
/// [`crdt::PnCounter::compact`]/[`crdt::OrSet::compact`] reads this; stage
/// one runs unconditionally on `retire` alone.
fn crdt_cache_is_quiet(
    members: &[membership::MemberView],
    now: Instant,
    bound: Duration,
) -> Quiescence {
    Quiescence::from_settled(
        members
            .iter()
            .all(|member| membership::member_is_quiet(member, now, bound)),
    )
}

/// Every [`membership::MemberView`] the CRDT compaction sweep for `name`
/// treats as relevant this tick: every live peer that currently has `name`
/// open, plus every member `absence` currently tracks gone at all, whether
/// it crashed or departed gracefully. Live-holder scoped, gone-global:
/// absence is tracked per-member globally, not per-(member, cache), so one
/// gone member holds every merging cache's quiet check pending, including
/// the caches it never had open.
fn crdt_cache_members(
    peers: &[Peer],
    modes: &CacheModes,
    name: &SmolStr,
    absence: &absence::AbsenceTracker,
) -> Vec<membership::MemberView> {
    let live = peers
        .iter()
        .filter(|peer| {
            modes
                .get(&peer.node)
                .is_some_and(|caches| caches.contains_key(name))
        })
        .map(|peer| membership::MemberView {
            known_since: absence.known_since(peer.node),
            present_since: absence.present_since(peer.node),
            absent_since: None,
            live_incarnation: Some(peer.incarnation),
        });
    // `Duration::ZERO` makes every currently-tracked gone member match,
    // non-destructively (`AbsenceTracker::gone_longer_than`'s own doc):
    // this is a membership *listing*, not a retirement decision, so it must
    // never prune.
    let gone = absence
        .gone_longer_than(Duration::ZERO)
        .into_iter()
        .map(|node| membership::MemberView {
            known_since: absence.known_since(node),
            present_since: None,
            absent_since: absence.gone_since(node),
            live_incarnation: None,
        });
    live.chain(gone).collect()
}

/// One pass of the CRDT writer-retirement sweep for one merging cache. A
/// no-op unless the cache's resolver merges ([`ShardOps::merges`]);
/// otherwise builds this tick's retirement predicate and quiet flag from
/// the membership/absence snapshot and hands both to
/// [`ShardOps::compact_pass`], counting each retired writer and rewritten
/// record. A writer of a node that is neither a live peer nor tracked gone is
/// one this process never observed; `unseen` holds where the sweep first met
/// it. Returns that same result, so a test observes one pass directly;
/// split out from [`crdt_compact_task`]'s ticker loop for the same reason.
async fn crdt_compact_tick(
    shard: &dyn ShardOps,
    name: &SmolStr,
    cluster: &Cluster,
    absence: &absence::AbsenceTracker,
    unseen: &UnseenWriters,
    bounds: CompactionBounds,
) -> (Vec<crdt::WriterId>, usize) {
    if !shard.merges() {
        return (Vec::new(), 0);
    }
    let peers = cluster.peers();
    let modes = cluster.advertised_cache_modes();

    let retire_after = cluster.config().crdt_retire_after;
    let batch = cluster.config().crdt_compact_batch;
    absence.prune_gone_older_than(Duration::from_millis(bounds.receipt_ttl_ms));
    let now = Instant::now();
    let local = crdt::WriterId::new(cluster.node_id(), cluster.local_incarnation());
    // Seeded with this node's own current incarnation, in addition to its
    // peers': `Cluster::peers()` excludes self by construction, so without
    // this a node could never tell its *own* past incarnations (same
    // `NodeId`, a prior `local_incarnation()` from before a restart) apart
    // from a writer of a node it has never observed. A node's own current
    // incarnation is always known with certainty, no membership round-trip
    // required, so every one of its own past incarnations is
    // unconditionally eligible the moment it holds a newer one live, the
    // same "any difference counts" rule
    // [`membership::incarnation_is_dead`] already applies to every peer.
    // Without this, a node's own dead incarnations never fold locally: the
    // node keeps taking part in ordinary anti-entropy, so its perpetually
    // unfolded copy keeps re-merging its old writer slots back into every
    // peer's already-compacted copy of the same record.
    let mut live_incarnation: HashMap<NodeId, u64> = peers
        .iter()
        .map(|peer| (peer.node, peer.incarnation))
        .collect();
    live_incarnation.insert(cluster.node_id(), cluster.local_incarnation());

    let members = crdt_cache_members(&peers, &modes, name, absence);
    let quiet = crdt_cache_is_quiet(&members, now, retire_after);
    let started = cluster.started_at();

    // A writer's node that is neither a live peer nor tracked gone is one
    // this process never observed, as after a rolling replacement of
    // random node ids: it counts as absent from the later of this
    // process's start and the sweep's first sight of the writer
    // ([`crdt_writer_absent_since`]), so it retires once this process has
    // run for the bound, and a live node that gossip has not yet shown is
    // never retired on one tick's say-so.
    let retire = |w: crdt::WriterId| {
        let member = if let Some(&incarnation) = live_incarnation.get(&w.node()) {
            membership::MemberView {
                known_since: None,
                present_since: None,
                absent_since: None,
                live_incarnation: Some(incarnation),
            }
        } else {
            let gone_since = absence.gone_since(w.node());
            let first_unseen = if gone_since.is_some() {
                now
            } else {
                unseen.first_sight(w, now)
            };
            membership::MemberView {
                known_since: None,
                present_since: None,
                absent_since: Some(crdt_writer_absent_since(gone_since, first_unseen, started)),
                live_incarnation: None,
            }
        };
        crdt_writer_is_retirement_eligible(local, w, &member, now, retire_after)
    };

    // The whole keyspace, `batch` records per call and a yield between
    // calls so a large cache never pins the executor: one tick retires a
    // dead writer everywhere, rather than `batch` records per period.
    let mut retired: Vec<crdt::WriterId> = Vec::new();
    let mut compacted = 0usize;
    let mut stripes_visited = 0usize;
    loop {
        let outcome = shard
            .compact_pass(now_ms(), &retire, quiet, bounds, batch)
            .await;
        for writer in outcome.retired {
            if !retired.contains(&writer) {
                retired.push(writer);
            }
        }
        compacted += outcome.compacted;
        stripes_visited += outcome.stripes_visited;
        if sweep_is_complete(stripes_visited, outcome.stripes_visited) {
            break;
        }
        tokio::task::yield_now().await;
    }
    if keyspace_walked(stripes_visited) {
        unseen.end_cycle();
    }
    for _ in &retired {
        metrics::counter!("sundog_crdt_retired_writers_total", "cache" => name.to_string())
            .increment(1);
    }
    if compacted > 0 {
        metrics::counter!("sundog_crdt_compactions_total", "cache" => name.to_string())
            .increment(u64::try_from(compacted).unwrap_or(u64::MAX));
    }
    (retired, compacted)
}

/// Whether one tick's sweep has examined the whole keyspace: the stripes
/// its `compact_pass` calls walked add up to every stripe, or the last
/// call walked none (an empty budget, or a shard that never compacts).
fn sweep_is_complete(stripes_visited: usize, last_call_stripes: usize) -> bool {
    last_call_stripes == 0 || keyspace_walked(stripes_visited)
}

/// Periodically runs [`crdt_compact_tick`] for one merging cache, at a
/// quarter of `ClusterConfig::crdt_retire_after` (floored at 30s), firing
/// immediately on the first tick. Ticks need no alignment across replicas:
/// [`crate::store::ConflictResolver::settle`] drops a pruned fold receipt
/// again on every merge apply, so a receipt one replica pruned does not ride
/// back in from another. Replicas fold a writer at their own sweep instants
/// and stamp the receipt with it, so their records differ by those stamps
/// until one exchange merges them by maximum. The task owns the
/// [`UnseenWriters`] its ticks share.
pub(crate) async fn crdt_compact_task(
    shard: Arc<dyn ShardOps>,
    name: SmolStr,
    cluster: Cluster,
    absence: absence::AbsenceTracker,
    cancel: CancellationToken,
) {
    let period = cluster.config().crdt_sweep_period();
    let bounds = cluster.config().crdt_compaction_bounds();
    let unseen = UnseenWriters::default();
    let mut wait = Duration::ZERO;
    loop {
        if cancel
            .run_until_cancelled(tokio::time::sleep(wait))
            .await
            .is_none()
        {
            return;
        }
        crdt_compact_tick(shard.as_ref(), &name, &cluster, &absence, &unseen, bounds).await;
        wait = period;
    }
}

// Pure decision logic (`crdt_writer_is_retirement_eligible`,
// `crdt_writer_absent_since`, `keyspace_walked`, `UnseenWriters`,
// `crdt_cache_is_quiet`, `crdt_cache_members`, `sweep_is_complete`) needs no
// cluster; end-to-end passes drive a real `Cluster`/`Shard`, which panics
// under `sim` outside a driven `turmoil::Sim`.
#[cfg(all(test, not(feature = "sim")))]
mod tests {
    use super::*;
    use crate::cache::Cache;
    use crate::cluster::test_support::{
        loopback_config, registered_shard, wait_for_no_peers, wait_for_peer_count, wait_until,
    };
    use crate::config::ClusterConfig;
    use crate::node::NodeName;
    use crate::store::Mode;
    use crate::store::crdt::{PnCounter, PnCounterResolver};
    use crate::wire;

    mod stage_two;
    mod unobserved;

    // -----------------------------------------------------------------
    // crdt_compact_task: pure decision logic.
    // -----------------------------------------------------------------

    /// `secs` seconds before `now`, for building [`membership::MemberView`]
    /// fixtures without a real clock.
    fn ago(now: Instant, secs: u64) -> Instant {
        now.checked_sub(Duration::from_secs(secs))
            .expect("test duration fits before `now`")
    }

    fn writer(node: u64, incarnation: u64) -> crdt::WriterId {
        crdt::WriterId::new(NodeId::from(node), incarnation)
    }

    #[test]
    fn crdt_writer_is_retirement_eligible_is_false_for_the_local_writer_even_if_absent() {
        let now = Instant::now();
        let bound = Duration::from_secs(60);
        let local = writer(1, 1);
        // If `local` weren't excluded, this member view (absent well past
        // the bound) would otherwise make it eligible.
        let member = membership::MemberView {
            known_since: None,
            present_since: None,
            absent_since: Some(ago(now, 3600)),
            live_incarnation: None,
        };
        assert!(!crdt_writer_is_retirement_eligible(
            local, local, &member, now, bound
        ));
    }

    #[test]
    fn crdt_writer_is_retirement_eligible_is_true_for_a_writer_absent_past_the_bound() {
        let now = Instant::now();
        let bound = Duration::from_secs(60);
        let local = writer(1, 1);
        let w = writer(2, 1);
        let member = membership::MemberView {
            known_since: None,
            present_since: None,
            absent_since: Some(ago(now, 3600)),
            live_incarnation: None,
        };
        assert!(crdt_writer_is_retirement_eligible(
            local, w, &member, now, bound
        ));
    }

    #[test]
    fn crdt_writer_is_retirement_eligible_defers_for_a_writer_absent_less_than_the_bound() {
        let now = Instant::now();
        let bound = Duration::from_secs(3600);
        let local = writer(1, 1);
        let w = writer(2, 1);
        let member = membership::MemberView {
            known_since: None,
            present_since: None,
            absent_since: Some(ago(now, 5)),
            live_incarnation: None,
        };
        assert!(!crdt_writer_is_retirement_eligible(
            local, w, &member, now, bound
        ));
    }

    #[test]
    fn crdt_writer_is_retirement_eligible_is_true_for_a_live_writer_under_a_different_incarnation()
    {
        let now = Instant::now();
        let bound = Duration::from_secs(3600);
        let local = writer(1, 1);
        // node 2 is live, but under incarnation 9 now: incarnation 1's
        // process is gone regardless of how recently node 2 restarted.
        let old_w = writer(2, 1);
        let member = membership::MemberView {
            known_since: None,
            present_since: Some(now),
            absent_since: None,
            live_incarnation: Some(9),
        };
        assert!(crdt_writer_is_retirement_eligible(
            local, old_w, &member, now, bound
        ));
    }

    #[test]
    fn crdt_writer_is_retirement_eligible_is_false_for_a_never_observed_writer() {
        // With no live incarnation and no absent time the member is not
        // dead; the sweep supplies an absent time for a writer's node it
        // never observed (`crdt_writer_absent_since`).
        let now = Instant::now();
        let bound = Duration::from_secs(60);
        let local = writer(1, 1);
        let w = writer(2, 1);
        let member = membership::MemberView {
            known_since: None,
            present_since: None,
            absent_since: None,
            live_incarnation: None,
        };
        assert!(!crdt_writer_is_retirement_eligible(
            local, w, &member, now, bound
        ));
    }

    #[test]
    fn crdt_writer_absent_since_is_the_tracked_gone_time_even_when_later_than_the_start() {
        let now = Instant::now();
        let started = ago(now, 3600);
        let gone = ago(now, 5);
        assert_eq!(
            crdt_writer_absent_since(Some(gone), ago(now, 1800), started),
            gone,
            "a tracked gone time is exact: neither the start nor the first sight lowers or raises it"
        );
        let long_gone = ago(now, 7200);
        assert_eq!(
            crdt_writer_absent_since(Some(long_gone), now, started),
            long_gone,
            "and a tracked gone time earlier than the start is kept too"
        );
    }

    #[test]
    fn crdt_writer_absent_since_is_the_later_of_first_sight_and_start_for_an_unobserved_node() {
        let now = Instant::now();
        let started = ago(now, 3600);
        let first_seen = ago(now, 60);
        assert_eq!(
            crdt_writer_absent_since(None, first_seen, started),
            first_seen,
            "a writer first met after the start counts from the first sight"
        );
        assert_eq!(
            crdt_writer_absent_since(None, ago(now, 7200), started),
            started,
            "a first sight cannot precede the start"
        );
    }

    #[test]
    fn an_unobserved_writer_is_eligible_only_once_the_start_and_first_sight_are_a_bound_old() {
        let now = Instant::now();
        let local = writer(1, 1);
        let w = writer(2, 1);
        let eligible = |first_seen: Instant, started: Instant, bound: Duration| {
            let member = membership::MemberView {
                known_since: None,
                present_since: None,
                absent_since: Some(crdt_writer_absent_since(None, first_seen, started)),
                live_incarnation: None,
            };
            crdt_writer_is_retirement_eligible(local, w, &member, now, bound)
        };
        let minute = Duration::from_secs(60);
        assert!(
            eligible(ago(now, 3600), ago(now, 3600), minute),
            "met at the start of a process that has run for an hour"
        );
        assert!(
            !eligible(ago(now, 3600), ago(now, 5), Duration::from_secs(3600)),
            "a process younger than the bound retires nothing it never observed"
        );
        assert!(
            !eligible(ago(now, 5), ago(now, 3600), minute),
            "a writer first met a moment ago waits a bound from that sight, however old the process"
        );
    }

    #[test]
    fn keyspace_walked_is_true_from_one_full_pass_of_stripes() {
        assert!(!keyspace_walked(0));
        assert!(!keyspace_walked(crate::store::BUCKET_COUNT - 1));
        assert!(keyspace_walked(crate::store::BUCKET_COUNT));
        assert!(keyspace_walked(crate::store::BUCKET_COUNT + 9));
    }

    #[test]
    fn unseen_writers_keep_the_first_sight_across_later_sights() {
        let unseen = UnseenWriters::default();
        let now = Instant::now();
        let w = writer(7, 1);
        assert_eq!(unseen.first_sight(w, ago(now, 30)), ago(now, 30));
        assert_eq!(
            unseen.first_sight(w, now),
            ago(now, 30),
            "a later sight returns the first"
        );
        assert_eq!(unseen.len(), 1);
    }

    #[test]
    fn unseen_writers_end_a_cycle_by_dropping_what_the_cycle_did_not_meet() {
        let unseen = UnseenWriters::default();
        let now = Instant::now();
        let (met, passed) = (writer(7, 1), writer(8, 1));
        unseen.first_sight(met, ago(now, 30));
        unseen.first_sight(passed, ago(now, 30));
        unseen.end_cycle();
        assert_eq!(
            unseen.len(),
            2,
            "both were met in the cycle that just ended"
        );

        assert_eq!(unseen.first_sight(met, now), ago(now, 30));
        unseen.end_cycle();
        assert_eq!(unseen.len(), 1, "the writer the cycle skipped is forgotten");

        unseen.end_cycle();
        assert_eq!(unseen.len(), 0, "and one met in no cycle at all");
        assert_eq!(
            unseen.first_sight(met, now),
            now,
            "a forgotten writer starts over at its next sight"
        );
    }

    #[test]
    fn unseen_writers_keep_a_writer_through_a_cycle_that_has_not_ended() {
        let unseen = UnseenWriters::default();
        let now = Instant::now();
        let w = writer(7, 1);
        unseen.first_sight(w, ago(now, 30));
        unseen.end_cycle();
        assert_eq!(unseen.len(), 1);
        // No end_cycle: the sweep stopped short of the whole keyspace, so
        // nothing is dropped however many passes ran.
        assert_eq!(unseen.first_sight(writer(9, 1), now), now);
        assert_eq!(unseen.len(), 2);
    }

    #[test]
    fn crdt_cache_is_quiet_is_vacuously_true_with_no_members() {
        let now = Instant::now();
        assert!(crdt_cache_is_quiet(&[], now, Duration::from_secs(60)).is_settled());
    }

    #[test]
    fn crdt_cache_is_quiet_is_true_when_every_member_has_settled() {
        let now = Instant::now();
        let bound = Duration::from_secs(60);
        let members = [
            membership::MemberView {
                known_since: None,
                present_since: Some(ago(now, 120)),
                absent_since: None,
                live_incarnation: Some(1),
            },
            membership::MemberView {
                known_since: None,
                present_since: None,
                absent_since: Some(ago(now, 120)),
                live_incarnation: None,
            },
        ];
        assert!(crdt_cache_is_quiet(&members, now, bound).is_settled());
    }

    #[test]
    fn crdt_cache_is_quiet_is_false_when_one_member_has_not_settled() {
        let now = Instant::now();
        let bound = Duration::from_secs(60);
        let members = [
            membership::MemberView {
                known_since: None,
                present_since: Some(ago(now, 120)),
                absent_since: None,
                live_incarnation: Some(1),
            },
            // Recently returned/gone absent: not yet past the bound.
            membership::MemberView {
                known_since: None,
                present_since: None,
                absent_since: Some(ago(now, 5)),
                live_incarnation: None,
            },
        ];
        assert!(!crdt_cache_is_quiet(&members, now, bound).is_settled());
    }

    #[test]
    fn crdt_cache_members_scopes_live_to_cache_holders_and_absentees_globally() {
        let cache = SmolStr::new("counters");
        let other_cache = SmolStr::new("other");
        let holder = NodeId::from(2);
        let bystander = NodeId::from(3);
        let peers = vec![
            Peer {
                node: holder,
                name: NodeName::new("host", holder),
                gossip_addr: "127.0.0.1:0".parse().expect("valid addr"),
                data_addr: "127.0.0.1:0".parse().expect("valid addr"),
                incarnation: 7,
                protocol: wire::PROTOCOL_VERSION,
            },
            Peer {
                node: bystander,
                name: NodeName::new("host", bystander),
                gossip_addr: "127.0.0.1:0".parse().expect("valid addr"),
                data_addr: "127.0.0.1:0".parse().expect("valid addr"),
                incarnation: 3,
                protocol: wire::PROTOCOL_VERSION,
            },
        ];
        let mut modes: CacheModes = HashMap::new();
        modes
            .entry(holder)
            .or_default()
            .insert(cache.clone(), Mode::Replicated);
        modes
            .entry(bystander)
            .or_default()
            .insert(other_cache, Mode::Replicated);

        let absentee = NodeId::from(4);
        let absence = absence::AbsenceTracker::default();
        // `absentee` starts live, then drops out once `holder`/`bystander`
        // are the live set: the same tracker that reports presence for the
        // live peers also reports the absentee, exactly as it would from a
        // real membership feed.
        absence.observe(&HashMap::from([(
            absentee,
            membership::LiveFlags { departing: false },
        )]));
        absence.observe(&HashMap::from([
            (holder, membership::LiveFlags { departing: false }),
            (bystander, membership::LiveFlags { departing: false }),
        ]));

        let members = crdt_cache_members(&peers, &modes, &cache, &absence);
        assert_eq!(
            members.len(),
            2,
            "the live holder of `cache` and the global absentee, but not the \
             bystander holding a different cache"
        );
        assert!(
            members
                .iter()
                .any(|m| m.live_incarnation == Some(7) && m.present_since.is_some()),
            "the live holder's own incarnation is carried through"
        );
        assert!(
            members
                .iter()
                .any(|m| m.absent_since.is_some() && m.live_incarnation.is_none()),
            "the absentee is carried through even though it holds no cache at all"
        );
    }

    // -----------------------------------------------------------------
    // crdt_compact_task: end-to-end passes against a real cluster/shard,
    // driving `crdt_compact_tick` directly (rather than the real ticker,
    // which floors at 30s) with a fake `AbsenceTracker` for deterministic
    // absence state.
    // -----------------------------------------------------------------

    /// A loopback config with `retire_after` for tests that drive
    /// `crdt_compact_tick` by hand with their own tracker: the cache's
    /// own background sweep keeps the 30-second production floor, so it
    /// never races the hand-driven ticks within a test's lifetime, and
    /// the hand-driven ticks pass [`three_bounds`] themselves.
    fn crdt_loopback_config(retire_after: Duration) -> ClusterConfig {
        ClusterConfig {
            crdt_retire_after: retire_after,
            crdt_compact_batch: 100,
            ..loopback_config()
        }
    }

    /// The config for tests that let the real `crdt_compact_task` do the
    /// sweeping: a sweep every quarter of `retire_after` (at least 50ms),
    /// so a fold receipt lives a few bounds rather than the minute the
    /// production floor would give.
    fn crdt_task_loopback_config(retire_after: Duration) -> ClusterConfig {
        ClusterConfig {
            crdt_sweep_interval: Some((retire_after / 4).max(Duration::from_millis(50))),
            ..crdt_loopback_config(retire_after)
        }
    }

    /// The bounds a hand-driven tick passes: ticks in these tests are as
    /// frequent as the test sleeps, so the receipt lives its plain three
    /// bounds.
    fn three_bounds(retire_after: Duration) -> CompactionBounds {
        CompactionBounds::three_bounds(u64::try_from(retire_after.as_millis()).unwrap_or(u64::MAX))
    }

    /// One tick examines the whole keyspace however small the per-call
    /// batch: eight records, a batch of one, and every record's dead writer
    /// retired by a single `crdt_compact_tick`.
    #[tokio::test]
    async fn crdt_compact_tick_sweeps_the_whole_keyspace_however_small_the_batch() {
        let retire_after = Duration::from_millis(20);
        let cluster = Cluster::builder("cluster-it-crdt-whole-keyspace")
            .seeds(std::iter::empty())
            .config(ClusterConfig {
                crdt_compact_batch: 1,
                ..crdt_loopback_config(retire_after)
            })
            .build()
            .await
            .expect("build succeeds");
        let name = SmolStr::new("counters");
        let cache = open_counters_cache(&cluster, name.as_str()).await;
        let shard = registered_shard(&cluster, &name);
        let dead_writer = writer(999, 1);
        for key in 0..8u32 {
            cache
                .insert(key, PnCounter::local_delta(dead_writer, u64::from(key) + 1))
                .await
                .expect("seed write");
        }
        let absence = absence::AbsenceTracker::default();
        absence.observe(&HashMap::from([(
            dead_writer.node(),
            membership::LiveFlags { departing: false },
        )]));
        absence.observe(&HashMap::new());
        tokio::time::sleep(retire_after + Duration::from_millis(20)).await;

        let (retired, compacted) = crdt_compact_tick(
            shard.as_ref(),
            &name,
            &cluster,
            &absence,
            &UnseenWriters::default(),
            three_bounds(retire_after),
        )
        .await;
        assert_eq!(retired, vec![dead_writer]);
        assert_eq!(
            compacted, 8,
            "one tick rewrote all eight records, one per call"
        );
        for key in 0..8u32 {
            assert_eq!(
                cache.get(&key).await.map(|c| c.value()),
                Some(i128::from(key) + 1),
                "key {key} keeps its exact value"
            );
        }
        cluster.shutdown().await;
    }

    #[test]
    fn sweep_is_complete_once_every_stripe_was_walked_or_a_call_walked_none() {
        assert!(!sweep_is_complete(crate::store::BUCKET_COUNT - 1, 3));
        assert!(sweep_is_complete(crate::store::BUCKET_COUNT, 3));
        assert!(sweep_is_complete(crate::store::BUCKET_COUNT + 7, 7));
        assert!(
            sweep_is_complete(0, 0),
            "a call that walked nothing ends the tick"
        );
    }

    /// A member flapping faster than the bound never settles as present or
    /// gone; once known for two bounds it counts as settled, so it cannot
    /// hold stage two back for every writer in the cache indefinitely.
    #[test]
    fn crdt_cache_is_quiet_counts_a_member_known_for_two_bounds_as_settled() {
        let now = Instant::now();
        let bound = Duration::from_secs(10);
        let flapper = membership::MemberView {
            known_since: Some(now.checked_sub(Duration::from_secs(25)).expect("recent")),
            present_since: Some(now.checked_sub(Duration::from_secs(1)).expect("recent")),
            absent_since: None,
            live_incarnation: Some(3),
        };
        assert!(crdt_cache_is_quiet(&[flapper], now, bound).is_settled());
        let newcomer = membership::MemberView {
            known_since: Some(now.checked_sub(Duration::from_secs(15)).expect("recent")),
            ..flapper
        };
        assert!(!crdt_cache_is_quiet(&[newcomer], now, bound).is_settled());
    }

    /// A member gone for longer than the receipt lifetime is forgotten by the tick:
    /// no fold receipt reconciles a straggling copy of its writer any more,
    /// so keeping it could only make a later retirement double count, and
    /// forgetting it bounds the tracker under sustained restarts.
    #[tokio::test]
    async fn crdt_compact_tick_forgets_a_member_gone_past_the_receipt_lifetime() {
        let retire_after = Duration::from_millis(20);
        let cluster = Cluster::builder("cluster-it-crdt-forget-gone")
            .seeds(std::iter::empty())
            .config(crdt_loopback_config(retire_after))
            .build()
            .await
            .expect("build succeeds");
        let name = SmolStr::new("counters");
        let _cache = open_counters_cache(&cluster, name.as_str()).await;
        let shard = registered_shard(&cluster, &name);
        let absence = absence::AbsenceTracker::default();
        let gone = NodeId::from(4242);
        absence.observe(&HashMap::from([(
            gone,
            membership::LiveFlags { departing: false },
        )]));
        absence.observe(&HashMap::new());
        let a_minute = CompactionBounds {
            receipt_ttl_ms: 60_000,
            ..three_bounds(retire_after)
        };
        crdt_compact_tick(
            shard.as_ref(),
            &name,
            &cluster,
            &absence,
            &UnseenWriters::default(),
            a_minute,
        )
        .await;
        assert!(
            absence.gone_since(gone).is_some(),
            "gone within the receipt lifetime: still tracked"
        );
        tokio::time::sleep(retire_after * 4).await;
        crdt_compact_tick(
            shard.as_ref(),
            &name,
            &cluster,
            &absence,
            &UnseenWriters::default(),
            three_bounds(retire_after),
        )
        .await;
        assert!(
            absence.gone_since(gone).is_none(),
            "past the receipt lifetime: forgotten"
        );
        cluster.shutdown().await;
    }

    async fn open_counters_cache(cluster: &Cluster, name: &str) -> Cache<u32, PnCounter> {
        cluster
            .cache::<u32, PnCounter>(name)
            .mode(Mode::Replicated)
            .resolver(Arc::new(PnCounterResolver))
            .open()
            .await
            .expect("open succeeds")
    }

    /// The quiet-deferral variant: stage one (moving a dead writer's slot
    /// into its own per-writer retired entry) runs as soon as the writer is
    /// dead, but stage two (folding a long-aged retired entry into the
    /// bounded scalar) defers for as long as any other member of the cache
    /// hasn't settled, even once that entry is old enough on its own.
    #[tokio::test]
    async fn crdt_compact_tick_defers_stage_two_folding_while_the_cache_is_not_quiet() {
        let retire_after = Duration::from_millis(30);
        let cluster = Cluster::builder("cluster-it-crdt-quiet-deferral")
            .seeds(std::iter::empty())
            .config(crdt_loopback_config(retire_after))
            .build()
            .await
            .expect("build succeeds");

        let name = SmolStr::new("counters");
        let cache = open_counters_cache(&cluster, name.as_str()).await;
        let shard = registered_shard(&cluster, &name);

        let dead_writer = writer(999, 1);
        cache
            .insert(1, PnCounter::local_delta(dead_writer, 5))
            .await
            .expect("seed write");

        let absence = absence::AbsenceTracker::default();
        absence.observe(&HashMap::from([(
            dead_writer.node(),
            membership::LiveFlags { departing: false },
        )]));
        absence.observe(&HashMap::new());

        tokio::time::sleep(retire_after + Duration::from_millis(20)).await;

        // Stage one: with no other known members the cache is vacuously
        // quiet, so the dead writer's live slot moves into its own retired
        // entry the moment it's found dead.
        let (retired, compacted) = crdt_compact_tick(
            shard.as_ref(),
            &name,
            &cluster,
            &absence,
            &UnseenWriters::default(),
            three_bounds(retire_after),
        )
        .await;
        assert_eq!(retired, vec![dead_writer]);
        assert_eq!(compacted, 1);
        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(5),
            "stage one never changes the counter's value"
        );

        // Age the dead writer's retired entry well past the stage-two
        // bound (`2 * retire_after`) *before* introducing any other
        // member, so the only thing standing between it and stage two is
        // the cache's quiet flag, not its own age.
        tokio::time::sleep(retire_after * 2 + Duration::from_millis(30)).await;

        // A second, freshly-unsettled member, introduced right before the
        // next pass: its own absence age is close to zero, well under
        // `retire_after`, so it alone holds the cache's quiet flag false.
        // Absence is tracked globally, not per-cache, so this holds every
        // merging cache's quiet check pending, regardless of which cache
        // `flaky` ever had open.
        let flaky = NodeId::from(998);
        absence.observe(&HashMap::from([(
            flaky,
            membership::LiveFlags { departing: false },
        )]));
        absence.observe(&HashMap::new());

        // The dead writer's retired entry is now old enough for stage two,
        // but `flaky` hasn't settled yet, so the cache stays not-quiet and
        // stage two must defer.
        let (retired, compacted) = crdt_compact_tick(
            shard.as_ref(),
            &name,
            &cluster,
            &absence,
            &UnseenWriters::default(),
            three_bounds(retire_after),
        )
        .await;
        assert!(
            retired.is_empty(),
            "no writer newly enters stage one this pass"
        );
        assert_eq!(
            compacted, 0,
            "stage two defers while the cache is not quiet"
        );

        tokio::time::sleep(retire_after + Duration::from_millis(20)).await;

        // `flaky` has now itself been continuously absent past the bound,
        // settling it: the cache is quiet and stage two folds the
        // long-aged retired entry into the bounded scalar accumulator.
        let (retired, compacted) = crdt_compact_tick(
            shard.as_ref(),
            &name,
            &cluster,
            &absence,
            &UnseenWriters::default(),
            three_bounds(retire_after),
        )
        .await;
        assert!(retired.is_empty(), "{retired:?}");
        assert_eq!(
            compacted, 1,
            "stage two runs once the cache settles: this pass's own change is exactly \
             what was deferred above"
        );
        // `ShardOps::compact_pass` applies stage two's folded bytes via
        // `Engine::compact_replace_if_current`, a version-gated direct
        // replace, never a merge against the still-resident pre-fold
        // bytes, so the folded value this pass produces is exactly what
        // is now resident, with nothing to reconcile.
        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(5),
            "stage two's own fold never changes the counter's value"
        );

        cluster.shutdown().await;
    }

    /// A node's own past incarnation (same [`NodeId`], an incarnation
    /// earlier than [`Cluster::local_incarnation`]) retires with no peers
    /// and no absence tracking involved at all: `Cluster::peers()` excludes
    /// self, so `live_incarnation` must seed this node's own current
    /// incarnation directly for `retire` ever to see it as dead.
    ///
    /// Drives `crdt_compact_tick` directly, several times over, since the
    /// cluster's own `crdt_compact_task` races these manual calls for the
    /// same pass; only the converged outcome across every task is asserted.
    #[tokio::test]
    async fn crdt_compact_tick_retires_this_nodes_own_past_incarnation_with_no_peers_at_all() {
        let retire_after = Duration::from_millis(20);
        let cluster = Cluster::builder("cluster-it-crdt-self-past-incarnation")
            .seeds(std::iter::empty())
            .config(crdt_loopback_config(retire_after))
            .build()
            .await
            .expect("build succeeds");

        let name = SmolStr::new("counters");
        let cache = open_counters_cache(&cluster, name.as_str()).await;
        let shard = registered_shard(&cluster, &name);

        // A stale `WriterId` under this very node's own id, one incarnation
        // behind the current one: exactly what a fresh incarnation minted
        // after a restart under the same persisted `NodeId` leaves behind
        // in a still-resident record.
        let stale_self = crdt::WriterId::new(cluster.node_id(), cluster.local_incarnation() - 1);
        cache
            .insert(1, PnCounter::local_delta(stale_self, 7))
            .await
            .expect("seed write from this node's own past incarnation");
        let raw_len = cache
            .get(&1)
            .await
            .expect("the seed write is resident")
            .encode()
            .expect("PnCounter::encode never fails on a resident value")
            .len();

        // No absence tracker entries at all: with no peers and nothing
        // tracked absent, the cache is vacuously quiet, and this node needs
        // no absence information whatsoever to know its own past self is
        // dead.
        let absence = absence::AbsenceTracker::default();

        // Polls rather than counting a fixed number of passes: stage two
        // needs a pass strictly after a retirement its own age has cleared
        // `2 * bound_ms`, and stage three (pruning the `folded_at` receipt
        // stage two leaves behind) needs a *further* pass strictly after
        // that one, once the receipt's own age has cleared `3 * bound_ms`.
        // Reaching the fully-folded, minimal form takes several
        // sufficiently-spaced passes, not only enough total elapsed time.
        let folded_len = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                crdt_compact_tick(
                    shard.as_ref(),
                    &name,
                    &cluster,
                    &absence,
                    &UnseenWriters::default(),
                    three_bounds(retire_after),
                )
                .await;
                let len = cache
                    .get(&1)
                    .await
                    .expect("the record is still resident while folding")
                    .encode()
                    .expect("PnCounter::encode never fails on a resident value")
                    .len();
                if len < raw_len {
                    return len;
                }
                tokio::time::sleep(retire_after).await;
            }
        })
        .await
        .expect(
            "this node's own past incarnation folds away, bounding the record's metadata \
             instead of leaving it live (and growing every peer's already-compacted copy \
             back out on the next anti-entropy round) forever",
        );
        assert!(folded_len < raw_len, "raw={raw_len} folded={folded_len}");
        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(7),
            "retiring and folding this node's own past incarnation never changes the value"
        );

        cluster.shutdown().await;
    }

    /// `sundog_crdt_retired_writers_total`/`sundog_crdt_compactions_total`
    /// increment exactly once each for the pass that retires a writer, via
    /// the real `crdt_compact_task` entry point (not just `crdt_compact_tick`
    /// directly, so the ticker/task wiring itself is exercised too).
    #[tokio::test]
    async fn crdt_compact_task_retires_a_dead_writer_and_leaves_a_live_one_alone() {
        let retire_after = Duration::from_millis(20);
        let cluster = Cluster::builder("cluster-it-crdt-metrics")
            .seeds(std::iter::empty())
            .config(crdt_task_loopback_config(retire_after))
            .build()
            .await
            .expect("build succeeds");

        let name = SmolStr::new("counters");
        let cache = open_counters_cache(&cluster, name.as_str()).await;
        let shard = registered_shard(&cluster, &name);

        let local = crdt::WriterId::new(cluster.node_id(), cluster.local_incarnation());
        let dead_writer = writer(999, 1);
        cache
            .insert(1, PnCounter::local_delta(dead_writer, 5))
            .await
            .expect("seed write from the dead writer");
        cache
            .insert(1, PnCounter::local_delta(local, 2))
            .await
            .expect("seed write from this node's own current writer identity");

        let absence = absence::AbsenceTracker::default();
        absence.observe(&HashMap::from([(
            dead_writer.node(),
            membership::LiveFlags { departing: false },
        )]));
        absence.observe(&HashMap::new());
        tokio::time::sleep(retire_after + Duration::from_millis(20)).await;

        let cancel = CancellationToken::new();
        let task = tokio::spawn(crdt_compact_task(
            Arc::clone(&shard),
            name.clone(),
            cluster.clone(),
            absence.clone(),
            cancel.clone(),
        ));

        let seeded_len = cache
            .get(&1)
            .await
            .expect("counter 1 is resident")
            .encode()
            .expect("PnCounter::encode never fails on a resident value")
            .len();
        wait_until(
            Duration::from_secs(15),
            "the dead writer's slot moves into its retired entry, rewriting the record",
            async || {
                let len = cache
                    .get(&1)
                    .await
                    .expect("counter 1 is resident")
                    .encode()
                    .expect("PnCounter::encode never fails on a resident value")
                    .len();
                len != seeded_len
            },
        )
        .await;
        assert!(
            absence.gone_since(dead_writer.node()).is_some(),
            "the member stays tracked gone after retirement, for the records later ticks reach"
        );

        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(7),
            "retiring the dead writer never changes the counter's exact value"
        );

        cancel.cancel();
        task.await.expect("crdt_compact_task doesn't panic");
        cluster.shutdown().await;
    }

    /// A writer slot of a node this process never observed, in a record that
    /// arrived from an earlier deploy, retires once this process has run for
    /// `crdt_retire_after`: random node ids mean a rolling replacement leaves
    /// a fresh process no gone-time for any earlier writer, so the absence
    /// tracker alone never retires them and a merged value would grow by one
    /// slot per deploy.
    #[tokio::test]
    async fn crdt_compact_task_retires_a_writer_of_a_node_this_process_never_observed() {
        let retire_after = Duration::from_millis(20);
        let cluster = Cluster::builder("cluster-it-crdt-retire-unobserved")
            .seeds(std::iter::empty())
            .config(crdt_task_loopback_config(retire_after))
            .build()
            .await
            .expect("build succeeds");

        let name = SmolStr::new("counters");
        let cache = open_counters_cache(&cluster, name.as_str()).await;
        let shard = registered_shard(&cluster, &name);

        let local = crdt::WriterId::new(cluster.node_id(), cluster.local_incarnation());
        let unknown_writer = writer(999, 1);
        cache
            .insert(1, PnCounter::local_delta(unknown_writer, 5))
            .await
            .expect("seed write from a writer this node never observed");
        cache
            .insert(1, PnCounter::local_delta(local, 2))
            .await
            .expect("seed write from this node's own current writer identity");

        // Nothing observed: the tracker was never told the writer existed,
        // as after a rolling replacement of random node ids.
        let absence = absence::AbsenceTracker::default();
        tokio::time::sleep(retire_after + Duration::from_millis(20)).await;

        let seeded_len = cache
            .get(&1)
            .await
            .expect("counter 1 is resident")
            .encode()
            .expect("PnCounter::encode never fails on a resident value")
            .len();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(crdt_compact_task(
            Arc::clone(&shard),
            name.clone(),
            cluster.clone(),
            absence.clone(),
            cancel.clone(),
        ));
        wait_until(
            Duration::from_secs(5),
            "the unobserved writer's slot moves into its retired entry, rewriting the record",
            async || {
                let len = cache
                    .get(&1)
                    .await
                    .expect("counter 1 is resident")
                    .encode()
                    .expect("PnCounter::encode never fails on a resident value")
                    .len();
                len != seeded_len
            },
        )
        .await;
        assert_eq!(
            cache.get(&1).await.map(|c| c.value()),
            Some(7),
            "retiring the unobserved writer never changes the counter's exact value"
        );

        cancel.cancel();
        task.await.expect("crdt_compact_task doesn't panic");
        cluster.shutdown().await;
    }

    /// A writer's `NodeId` reused across three replacements (the churn
    /// shape `crdt_bench`'s benchmark drives at real cluster scale): each
    /// replacement's incarnation retires and folds away on the observer,
    /// and its `folded_at` receipt disappears for good, not merely once per
    /// stray tick. Pinned here because pruning alone, one node's own sweep
    /// against the other's differently-phased tick re-merging an unpruned
    /// copy back in, never converges; the resolver's `settle` step on every
    /// merge apply is what ends it, dropping a receipt three bounds past its
    /// fold again the moment a peer's copy carries it back in.
    #[tokio::test]
    async fn crdt_compact_task_prunes_a_folded_receipt_for_good_under_ongoing_two_node_replication()
    {
        let retire_after = Duration::from_secs(2);
        let observer = Cluster::builder("cluster-it-crdt-churn-receipt-prune")
            .seeds(std::iter::empty())
            .config(crdt_task_loopback_config(retire_after))
            .build()
            .await
            .expect("observer builds");
        let observer_addr = observer.inner.membership.local_peer().gossip_addr;
        let name = SmolStr::new("counters");
        let observer_cache = open_counters_cache(&observer, name.as_str()).await;

        let writer_node_id = NodeId::random();
        let mut last_writer_cluster: Option<Cluster> = None;
        for round in 0..3u64 {
            let writer = Cluster::builder("cluster-it-crdt-churn-receipt-prune")
                .node_id(writer_node_id)
                .seeds([observer_addr])
                .config(crdt_task_loopback_config(retire_after))
                .build()
                .await
                .unwrap_or_else(|e| panic!("writer round {round} builds: {e}"));
            wait_for_peer_count(&observer, 1).await;
            let writer_cache = open_counters_cache(&writer, name.as_str()).await;
            let wid = writer_cache.writer_id();
            writer_cache
                .insert(0, PnCounter::local_delta(wid, 10))
                .await
                .expect("writer inserts");
            let expected = i128::from(round + 1) * 10;
            wait_until(
                Duration::from_secs(10),
                "observer converges this round",
                async || observer_cache.get(&0).await.map(|c| c.value()) == Some(expected),
            )
            .await;
            if round + 1 == 3 {
                last_writer_cluster = Some(writer);
            } else {
                writer.shutdown().await;
            }
        }

        let raw_len = observer_cache
            .get(&0)
            .await
            .expect("counter 0 has landed on the observer")
            .encode()
            .expect("PnCounter::encode never fails on a resident value")
            .len();

        // Every earlier incarnation's fold-and-prune, end to end, within
        // a few 30-second-floored ticks, comfortably inside this budget:
        // a bound this test hits on every run, never just occasionally.
        let pruned_len = tokio::time::timeout(Duration::from_secs(100), async {
            loop {
                let len = observer_cache
                    .get(&0)
                    .await
                    .expect("counter 0 is still resident")
                    .encode()
                    .expect("PnCounter::encode never fails on a resident value")
                    .len();
                if len < raw_len {
                    return len;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .expect(
            "every earlier incarnation's folded_at receipt prunes for good under ongoing \
             two-node replication, shrinking the record below its raw, pre-compaction size",
        );
        assert!(pruned_len < raw_len, "raw={raw_len} pruned={pruned_len}");

        // Never bounces back over one further tick cycle on each side: a
        // receipt actually gone stays gone, it doesn't just dip below
        // `raw_len` for a moment before the next anti-entropy round
        // resurrects it.
        tokio::time::sleep(Duration::from_secs(35)).await;
        let settled_len = observer_cache
            .get(&0)
            .await
            .expect("counter 0 is still resident")
            .encode()
            .expect("PnCounter::encode never fails on a resident value")
            .len();
        assert!(
            settled_len <= pruned_len,
            "the pruned form must hold, not regrow: pruned={pruned_len} settled={settled_len}"
        );
        assert_eq!(
            observer_cache.get(&0).await.map(|c| c.value()),
            Some(30),
            "pruning three earlier incarnations' receipts never changes the counter's total"
        );

        if let Some(w) = last_writer_cluster {
            w.shutdown().await;
        }
        observer.shutdown().await;
    }

    /// A cold joiner seeded at `observer` warms counter 0 from the
    /// compacted record alone and holds it at `expected_len` bytes.
    async fn assert_cold_joiner_warms_compacted_counter(
        observer: &Cluster,
        retire_after: Duration,
        expected_len: usize,
    ) {
        let joiner = Cluster::builder("cluster-it-crdt-graceful-churn")
            .seeds([observer.inner.membership.local_peer().gossip_addr])
            .config(crdt_task_loopback_config(retire_after))
            .build()
            .await
            .expect("joiner builds");
        wait_for_peer_count(&joiner, 1).await;
        let joiner_cache = open_counters_cache(&joiner, "counters").await;
        let warmed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(counter) = joiner_cache.get(&0).await {
                    return counter;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the joiner warms the compacted counter");
        assert_eq!(
            warmed
                .encode()
                .expect("PnCounter::encode never fails on a resident value")
                .len(),
            expected_len,
            "the joiner's copy is the compacted, observer-only size"
        );
        joiner.shutdown().await;
    }

    /// A writer that leaves through `Cluster::shutdown` is retired and
    /// folded like one that crashed: three distinct nodes each join, write
    /// one counter, and leave gracefully, and the observer's record ends up
    /// back at the size it had with only the observer's own slot, holding
    /// the exact total, within two 30s-floored ticks of the last leave.
    #[tokio::test]
    async fn crdt_compact_task_retires_and_folds_gracefully_departed_writers() {
        let retire_after = Duration::from_secs(2);
        let observer = Cluster::builder("cluster-it-crdt-graceful-churn")
            .seeds(std::iter::empty())
            .config(crdt_task_loopback_config(retire_after))
            .build()
            .await
            .expect("observer builds");
        let observer_addr = observer.inner.membership.local_peer().gossip_addr;
        let observer_cache = open_counters_cache(&observer, "counters").await;
        observer_cache
            .insert(0, PnCounter::local_delta(observer_cache.writer_id(), 1))
            .await
            .expect("observer writes");
        let raw_len = observer_cache
            .get(&0)
            .await
            .expect("counter 0 is resident")
            .encode()
            .expect("PnCounter::encode never fails on a resident value")
            .len();

        for round in 1..=3i128 {
            let leaver = Cluster::builder("cluster-it-crdt-graceful-churn")
                .seeds([observer_addr])
                .config(crdt_task_loopback_config(retire_after))
                .build()
                .await
                .unwrap_or_else(|e| panic!("leaver {round} builds: {e}"));
            wait_for_peer_count(&observer, 1).await;
            let leaver_cache = open_counters_cache(&leaver, "counters").await;
            leaver_cache
                .merge(0, PnCounter::local_delta(leaver_cache.writer_id(), 10))
                .await
                .expect("leaver writes");
            let expected = 1 + round * 10;
            tokio::time::timeout(Duration::from_secs(10), async {
                while observer_cache.get(&0).await.map(|c| c.value()) != Some(expected) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("observer converges this round");
            leaver.shutdown().await;
            wait_for_no_peers(&observer).await;
        }
        let churned_len = observer_cache
            .get(&0)
            .await
            .expect("counter 0 is resident")
            .encode()
            .expect("PnCounter::encode never fails on a resident value")
            .len();
        assert!(
            churned_len > raw_len,
            "three more slots: raw={raw_len} churned={churned_len}"
        );

        let settled_len = tokio::time::timeout(Duration::from_secs(100), async {
            loop {
                let len = observer_cache
                    .get(&0)
                    .await
                    .expect("counter 0 is resident")
                    .encode()
                    .expect("PnCounter::encode never fails on a resident value")
                    .len();
                if len <= raw_len {
                    return len;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .expect("every gracefully departed writer retires, folds, and its receipt prunes");
        assert_eq!(settled_len, raw_len, "back to the observer-only size");

        assert_cold_joiner_warms_compacted_counter(&observer, retire_after, raw_len).await;
        assert_eq!(
            observer_cache.get(&0).await.map(|c| c.value()),
            Some(31),
            "folding three departed writers never changes the total"
        );
        observer.shutdown().await;
    }
}
