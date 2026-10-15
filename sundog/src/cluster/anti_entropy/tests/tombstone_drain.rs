//! Tombstones leave every replica: shards on one manual clock sweep and
//! collect on phases of their own while anti-entropy rounds run between
//! them, and a tombstone that one replica has collected and another still
//! holds must not keep coming back.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::store::{Mode, Shard};

const TOMBSTONE_TTL_MS: u64 = 60_000;
const TOMBSTONE_MAX_TTL_MS: u64 = 600_000;
/// `tombstone_gc_task` runs a quarter of `tombstone_ttl` apart.
const GC_PERIOD_STEPS: u64 = 15;
const AE_PERIOD_STEPS: u64 = 5;
const START_MS: u64 = 1_000_000;

fn key_of(key: u32) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("test key encodes"))
}

fn record(key: u32, value: Option<&str>, wall_ms: u64, expires_at_ms: Option<u64>) -> WireRecord {
    WireRecord {
        key: key_of(key),
        value: value.map(|v| Bytes::from(postcard::to_stdvec(v).expect("test value encodes"))),
        ver: Hlc {
            wall_ms,
            logical: 0,
            node: NodeId::from(99),
        },
        expires_at_ms,
    }
}

/// Replicas on one manual clock, each running its housekeeping on a phase
/// of its own.
struct Ring {
    clock: Arc<AtomicU64>,
    shards: Vec<Arc<crate::store::Shard<u32, String>>>,
    phases: Vec<u64>,
}

impl Ring {
    fn new(phases: &[u64]) -> Self {
        let clock = Arc::new(AtomicU64::new(START_MS));
        let shards = (1..=phases.len() as u64)
            .map(|node| Self::shard(&clock, node))
            .collect();
        Self {
            clock,
            shards,
            phases: phases.to_vec(),
        }
    }

    /// A fresh replica `node` on `clock`.
    fn shard(clock: &Arc<AtomicU64>, node: u64) -> Arc<Shard<u32, String>> {
        let reader = Arc::clone(clock);
        Arc::new(
            Shard::<u32, String>::new(
                SmolStr::new("drain"),
                Mode::Replicated,
                NodeId::from(node),
                10_000,
                None,
                None,
            )
            .with_clock(Arc::new(move || reader.load(Ordering::SeqCst)))
            .with_tombstone_ttl(Duration::from_millis(TOMBSTONE_TTL_MS))
            .with_tombstone_max_ttl(Duration::from_millis(TOMBSTONE_MAX_TTL_MS)),
        )
    }

    /// Adds a replica built now, on `phase`: it starts deferring collection
    /// until its first tick, as a restarted node does.
    fn join(&mut self, phase: u64) {
        let node = self.shards.len() as u64 + 1;
        self.shards.push(Self::shard(&self.clock, node));
        self.phases.push(phase);
    }

    fn now(&self) -> u64 {
        self.clock.load(Ordering::SeqCst)
    }

    fn ops(&self, node: usize) -> Arc<dyn ShardOps> {
        Arc::clone(&self.shards[node]) as Arc<dyn ShardOps>
    }

    /// Applies `rec` on every replica, as the fan-out delivers a write.
    async fn apply_everywhere(&self, rec: &WireRecord) {
        for shard in &self.shards {
            shard.apply_remote(rec.clone()).await;
        }
    }

    /// One anti-entropy exchange of `local` against `peer`, classified by
    /// the round's own [`diff_bucket`] over the buckets whose digests
    /// differ, then pulled and pushed through the records both sides serve.
    /// Returns the records the round moved.
    async fn exchange(&self, local: usize, peer: usize) -> usize {
        let (local, peer) = (self.ops(local), self.ops(peer));
        let (mine, theirs) = (local.digests().await, peer.digests().await);
        let (mut push, mut pull) = (Vec::new(), Vec::new());
        for (a, b) in mine.iter().zip(&theirs) {
            if a.digest != b.digest {
                let l = key_versions_to_tuples(local.bucket_entries(a.bucket).await);
                let p = key_versions_to_tuples(peer.bucket_entries(a.bucket).await);
                diff_bucket(&l, &p, &mut push, &mut pull, false);
            }
        }
        let moved = pull.len() + push.len();
        if !pull.is_empty() {
            local.apply_remote_batch(peer.records_for(pull).await).await;
        }
        if !push.is_empty() {
            peer.apply_remote_batch(local.records_for(push).await).await;
        }
        moved
    }

    /// Advances one second: each replica whose phase falls on this step
    /// sweeps and collects, and every `AE_PERIOD_STEPS` each ordered pair
    /// of replicas exchanges.
    async fn step(&self, step: u64) {
        self.clock.fetch_add(1_000, Ordering::SeqCst);
        for (node, phase) in self.phases.iter().enumerate() {
            if (step + phase).is_multiple_of(GC_PERIOD_STEPS) {
                let ops = self.ops(node);
                ops.run_pending_tasks().await;
                ops.gc_tombstones(false).await;
            }
        }
        if step.is_multiple_of(AE_PERIOD_STEPS) {
            for a in 0..self.shards.len() {
                for b in 0..self.shards.len() {
                    if a != b {
                        let _ = self.exchange(a, b).await;
                    }
                }
            }
        }
    }

    fn tombstones(&self) -> Vec<u64> {
        self.shards.iter().map(|s| s.tombstone_count()).collect()
    }

    /// Steps until `now() >= until_ms`, returning the step reached.
    async fn run_until(&self, until_ms: u64, mut step: u64) -> u64 {
        while self.now() < until_ms {
            step += 1;
            self.step(step).await;
        }
        step
    }
}

/// Steps a ring from `from_ms` until the tombstone retention and two
/// collection ticks have passed, and returns each replica's tombstone count
/// then.
async fn tombstones_after_retention(ring: &Ring, from_ms: u64) -> Vec<u64> {
    let until = from_ms + TOMBSTONE_TTL_MS + 2 * GC_PERIOD_STEPS * 1_000;
    ring.run_until(until, 0).await;
    ring.tombstones()
}

/// The tombstone an expired entry leaves drains from every replica, whatever
/// phase each one sweeps and collects on: a replica that collected it never
/// takes it back from one that has not.
#[tokio::test]
async fn an_expired_entry_leaves_every_replica_that_sweeps_on_its_own_phase() {
    for phases in [vec![0, 5], vec![0, 5, 11], vec![0, 1, 2, 3], vec![7, 0]] {
        let ring = Ring::new(&phases);
        let expires_at = START_MS + 20_000;
        ring.apply_everywhere(&record(1, Some("old"), START_MS, None))
            .await;
        ring.apply_everywhere(&record(1, Some("new"), START_MS + 1, Some(expires_at)))
            .await;
        assert!(
            ring.tombstones().iter().all(|&n| n == 0),
            "a live entry holds no tombstone: {phases:?}"
        );

        let counts = tombstones_after_retention(&ring, expires_at).await;
        assert_eq!(
            counts,
            vec![0; phases.len()],
            "the tombstone drains from every replica: {phases:?}"
        );
        let first = ring.ops(0).digests().await;
        for node in 1..phases.len() {
            assert_eq!(
                ring.ops(node).digests().await,
                first,
                "the replicas agree once it is gone: {phases:?}"
            );
        }
    }
}

/// Many entries expiring at different instants leave a population of
/// tombstones that is empty again once the last retention ends.
#[tokio::test]
async fn a_stream_of_expiring_entries_leaves_no_tombstones_behind() {
    let ring = Ring::new(&[0, 4, 9]);
    let mut last_expiry = START_MS;
    let mut step = 0;
    for key in 0..40u32 {
        let expires_at = ring.now() + 7_000 + u64::from(key % 5) * 3_000;
        last_expiry = last_expiry.max(expires_at);
        let overwrite = record(key, Some("v"), ring.now(), Some(expires_at));
        if key % 3 == 0 {
            // The last replica missed the overwrite and holds the value it
            // replaced, with no deadline.
            for shard in &ring.shards[..2] {
                shard.apply_remote(overwrite.clone()).await;
            }
            ring.shards[2]
                .apply_remote(record(key, Some("stale"), ring.now() - 1, None))
                .await;
        } else {
            ring.apply_everywhere(&overwrite).await;
        }
        step = ring.run_until(ring.now() + 2_000, step).await;
    }
    let peak = ring.tombstones();
    assert!(
        peak.iter().any(|&n| n > 0),
        "the stream leaves tombstones while it runs: {peak:?}"
    );

    ring.run_until(
        last_expiry + TOMBSTONE_TTL_MS + 2 * GC_PERIOD_STEPS * 1_000,
        step,
    )
    .await;
    assert_eq!(ring.tombstones(), vec![0; 3], "every tombstone drains");
    for (node, shard) in ring.shards.iter().enumerate() {
        assert_eq!(
            shard.entry_count().await,
            0,
            "the value an expired overwrite replaced does not come back on replica {node}"
        );
    }
}

/// Sweeps both replicas and collects on the ones in `collect`, all at the
/// ring's current instant.
async fn sweep_then_collect(ring: &Ring, collect: &[usize]) {
    for node in 0..ring.shards.len() {
        ring.ops(node).run_pending_tasks().await;
    }
    for &node in collect {
        ring.ops(node).gc_tombstones(false).await;
    }
}

/// Two replicas holding the same lapsed expiry tombstones move no record in
/// an exchange, before they collect and after both have. While only one has
/// collected, every exchange moves the whole set both ways round: the window
/// the gc grid shrinks to the replicas' clock skew, by collecting on both at
/// the same instant.
#[tokio::test]
async fn an_exchange_moves_no_record_once_both_replicas_collect_the_same_tombstones() {
    const KEYS: u32 = 20;
    let ring = Ring::new(&[0, 0]);
    let expires_at = START_MS + 20_000;
    for key in 0..KEYS {
        ring.apply_everywhere(&record(key, Some("old"), START_MS, None))
            .await;
        ring.apply_everywhere(&record(key, Some("new"), START_MS + 1, Some(expires_at)))
            .await;
    }
    ring.clock.store(expires_at + 1_000, Ordering::SeqCst);
    sweep_then_collect(&ring, &[]).await;
    assert_eq!(ring.tombstones(), vec![u64::from(KEYS); 2], "both swept");
    assert_eq!(
        ring.exchange(0, 1).await,
        0,
        "the same tombstones on both sides need no repair"
    );

    ring.clock
        .store(expires_at + TOMBSTONE_TTL_MS + 1_000, Ordering::SeqCst);
    ring.ops(0).gc_tombstones(false).await;
    assert_eq!(ring.tombstones(), vec![0, u64::from(KEYS)]);
    assert_eq!(
        ring.exchange(0, 1).await,
        KEYS as usize,
        "a tombstone one replica collected and the other holds is offered again"
    );
    assert_eq!(
        ring.exchange(1, 0).await,
        KEYS as usize,
        "and again by the next round, from either side"
    );

    ring.ops(1).gc_tombstones(false).await;
    assert_eq!(ring.tombstones(), vec![0, 0]);
    assert_eq!(ring.exchange(0, 1).await, 0, "both collected");
    assert_eq!(ring.exchange(1, 0).await, 0, "both collected");
}

/// The records moved by the exchanges after every step, from a ring on
/// `phases`, until its expiry tombstones have drained.
async fn records_moved_while_draining(phases: &[u64]) -> usize {
    const KEYS: u32 = 20;
    let ring = Ring::new(phases);
    let expires_at = START_MS + 20_000;
    for key in 0..KEYS {
        ring.apply_everywhere(&record(key, Some("old"), START_MS, None))
            .await;
        ring.apply_everywhere(&record(key, Some("new"), START_MS + 1, Some(expires_at)))
            .await;
    }
    let mut moved = 0;
    let mut step = 0;
    while ring.now() < expires_at + TOMBSTONE_TTL_MS + 2 * GC_PERIOD_STEPS * 1_000 {
        step += 1;
        ring.step(step).await;
        for a in 0..phases.len() {
            for b in 0..phases.len() {
                if a != b {
                    moved += ring.exchange(a, b).await;
                }
            }
        }
    }
    assert_eq!(
        ring.tombstones(),
        vec![0; phases.len()],
        "every tombstone drained"
    );
    moved
}

/// Replicas that collect at the same instants never offer a lapsed tombstone
/// the other has dropped, where replicas on phases of their own do.
#[tokio::test]
async fn replicas_collecting_on_one_grid_never_move_a_lapsed_tombstone() {
    assert_eq!(
        records_moved_while_draining(&[0, 0, 0]).await,
        0,
        "no exchange moved a record"
    );
    assert!(
        records_moved_while_draining(&[0, 5]).await > 0,
        "replicas on offset phases move the tombstones one has collected"
    );
}

/// The delete record of key 1, stamped at [`START_MS`].
fn delete_stamped_at_start() -> WireRecord {
    record(1, None, START_MS, None)
}

/// Each replica's digests agree with replica 0's.
async fn assert_digests_agree(ring: &Ring, label: &str) {
    let first = ring.ops(0).digests().await;
    for node in 1..ring.shards.len() {
        assert_eq!(
            ring.ops(node).digests().await,
            first,
            "the replicas agree: {label}"
        );
    }
}

/// A delete leaves every replica, whatever phase each collects on and
/// whenever each received it. Its tombstone counts its retention from the
/// delete's stamp, so every replica derives the same deadlines, and a
/// replica that collected it never takes it back from one that has not.
/// Only a plain delete, with no value beside it, is applied here: the case
/// the tests above, which apply values, do not reach.
#[tokio::test]
async fn a_delete_leaves_every_replica_whatever_phase_and_receipt_time() {
    for phases in [vec![0, 5], vec![0, 5, 11], vec![7, 0]] {
        let ring = Ring::new(&phases);
        ring.shards[0].apply_remote(delete_stamped_at_start()).await;
        // The second replica receives it 20 s later, between its ticks.
        ring.clock.fetch_add(20_000, Ordering::SeqCst);
        ring.shards[1].apply_remote(delete_stamped_at_start()).await;
        assert_eq!(ring.tombstones()[..2], [1, 1], "both hold it: {phases:?}");

        let counts = tombstones_after_retention(&ring, ring.now()).await;
        assert_eq!(
            counts,
            vec![0; phases.len()],
            "the delete drains from every replica: {phases:?}"
        );
        assert_digests_agree(&ring, &format!("{phases:?}")).await;

        // It stays gone over the rounds that follow.
        let mut step = 1_000;
        for round in 0..6 {
            step = ring
                .run_until(ring.now() + AE_PERIOD_STEPS * 1_000, step)
                .await;
            assert_eq!(
                ring.tombstones(),
                vec![0; phases.len()],
                "no replica takes it back, round {round}: {phases:?}"
            );
        }
        assert_digests_agree(&ring, &format!("{phases:?}, later")).await;
    }
}

/// A replica built after the delete, which defers collection until its first
/// tick, takes the tombstone from the others and drops it with them.
#[tokio::test]
async fn a_replica_that_starts_late_takes_a_delete_and_drops_it_with_the_others() {
    for join_phase in [0, 4, 11] {
        let mut ring = Ring::new(&[0, 5]);
        ring.shards[0].apply_remote(delete_stamped_at_start()).await;
        ring.clock.fetch_add(20_000, Ordering::SeqCst);
        ring.shards[1].apply_remote(delete_stamped_at_start()).await;
        ring.clock.fetch_add(20_000, Ordering::SeqCst);
        ring.join(join_phase);

        let counts = tombstones_after_retention(&ring, ring.now()).await;
        assert_eq!(
            counts,
            vec![0; 3],
            "the delete drains from every replica: late phase {join_phase}"
        );
        assert_digests_agree(&ring, &format!("late phase {join_phase}")).await;
        let step = ring
            .run_until(ring.now() + 6 * AE_PERIOD_STEPS * 1_000, 1_000)
            .await;
        assert!(step > 1_000);
        assert_eq!(ring.tombstones(), vec![0; 3], "and stays gone");
    }
}
