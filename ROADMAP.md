# Roadmap

Design sketches for what v1 excludes, each cut to keep the core
buildable and correct first. Every section states its cost and trigger, so
revisiting one is a decision made on evidence, not on itch.

Nothing here is scheduled: a section becomes code only once its trigger
condition shows up in a real deployment, not because it would be interesting
to build. The exceptions are under "Next": small, self-contained, and
already justified by the code as it stands.

## Next

### Clock-skew guard

`HlcClock::observe` absorbs any remote stamp. One node with a clock an hour
ahead wins every write cluster-wide and drags every other node's clock forward
with it, and nothing reports it. A `max_clock_skew` on `ClusterConfig`
rejects a remote stamp whose skew exceeds it, counts the rejection, and logs
a local clock jump once.

**Cost:** two days, with a skewed-node simulation scenario.

### Memory ceilings that refuse rather than diverge

`Replicated` mode has no capacity bound because evicting locally makes
replicas differ. A byte-accurate accounting of keys, values, and per-entry
overhead, a `sundog_cache_bytes{cache}` gauge, and a soft ceiling that
rejects writes with a typed error keep every replica identical under memory
pressure.

**Cost:** about a week, most of it the accounting's property coverage.

### Zone-aware donor and repair choice

Every replicated node holds every entry, so a write crosses every zone once
whatever the topology. That traffic is the floor. What is not the floor is
where a joiner pulls its snapshot from and which peer a node reconciles with:
both pick by node id today. A `zone` key in gossip state, set from
`ClusterConfig::zone`, lets a joiner prefer a warm donor in its own zone and
lets anti-entropy weight same-zone peers, which is where the bulk transfers
happen. `Mode::Distributed`'s rendezvous scoring has no such input today. The
same `zone` key would extend it to spread a bucket's owners across zones
instead of scoring every eligible peer the same regardless of where it runs.

**Cost:** a few hundred lines across membership, state transfer, and the
scheduler's peer choice.

**Trigger:** a multi-zone deployment measuring cross-zone egress from joins
or repairs.

### Merge resolvers

`ConflictResolver` no longer only picks one of two records via `winner`:
`ConflictResolver::merge`, a defaulted method returning `Option<Merged>`
(`Merged { value, expires_at_ms }`), lets a resolver fold the stored and
incoming records into a third value instead, consulted only when
`ConflictResolver::merges` is `true` and both sides carry a value. `None`
falls back to `winner`. The engine computes that value's version
from the merged bytes themselves, not only from the two inputs' versions:
when the merge reduces to one side outright it adopts that side's own
`(version, bytes)` pair (or does nothing, if that side is already what's
stored), and only when the merged bytes are new relative to both
sides does it mint a version: `wall_ms`/`logical` strictly ahead of both
inputs and a `node` computed from a hash of the merged bytes itself
(`NodeId::merge_derived`), so two nodes minting for the same bytes mint the
same id and two nodes minting for different bytes never collide on the same
version. A merge-derived id can never equal a real node's id (a private high
bit distinguishes the two), so a minted version never falls into the
equal-version fast path against a genuine single-writer stamp, and
redelivering an already-absorbed merge is a no-op rather than an unbounded
re-broadcast. `Winner` itself is untouched, still `A`/`B` as in every prior
release, and `merges`/`merge` are both defaulted, so no
existing `ConflictResolver` implementation changes. This shipped as a patch
rather than waiting on a minor. The `sundog::crdt`
module ships `PnCounter`/`PnCounterResolver` and `OrSet`/`OrSetResolver` as
reference types proving the mechanism: both merge commutatively,
associatively, and idempotently under any delivery order, and a three-node
cluster with `PnCounterResolver` installed converges concurrent, unpaced
blind increments to the exact sum with no lost updates, unlike the default
last-write-wins resolver on the same workload.

`ConflictResolver::merges` (`ShardOps::merges` forwards a shard's own
resolver's answer) tells `cluster::anti_entropy` whether a resolver can
ever return `Merged`, `true` on `PnCounterResolver` and `OrSetResolver`.
Anti-entropy's ordinary direction rule pushes only the greater of two
mismatched versions to the lesser side, so a merging resolver's minted
result needs a second round to reach the replica that mints behind the
other. When `merges` is `true`, a version-mismatched key exchanges in
both directions in the same round instead: `diff_bucket` and
`diff_decoded` queue it for both push and pull, so both replicas fold
each other's pre-round record into their own in that one round.
`merge_version`'s doc proves this converges in one round on every
outcome, mint included: the merge itself is commutative and the mint's
`wall_ms`/`logical`/`node` are each a function of the unordered pair of
inputs, not of which side calls which argument `sv` and which `ver`, so
both replicas mint (or adopt) byte-for-byte, `Hlc`-for-`Hlc` identical
results. The partition-heal sim, rebuilt to drive `run_round_against`
(sundog's real anti-entropy entry point, not a reimplementation), confirms
it end to end: `merged` repairs a fully-conflicting partition split in 3
anti-entropy rounds against `decomposed`'s 4, at both 2,000 and 20,000
keys, and a monotonicity pin
(`partition_heal_rounds_are_monotone_in_key_count`) holds both variants'
round counts non-decreasing from 2,000 through 40,000 keys. This supersedes
an earlier, since-replaced version of the sim that measured a round-count
inversion at 20,000 keys under a race-prone anti-entropy tick. The
"Redundant pulls at a partial conflict fraction" item below is the
byte-cost question that measurement did surface and that this rebuild
leaves open.

Two write-path levers sit on top of the same contract, both public API,
neither changing the version rule or the wire format. `Engine::apply_many`
groups a batch by key when the resolver's `merges()` is `true` and folds
each same-key run down to one survivor with the resolver, outside
the stripe lock, before applying the run's single survivor through the
ordinary path. A peer's replicated fan-out batch and a local
`Cache::insert_many` call are where a real run appears. A singleton `insert`
never contains one. `Cache::merge`/`CacheBuilder::merge_coalesce_window`
coalesce a run of client-side `merge` calls to one key, inside a bounded
window, into a single record applied once the window elapses. A zero
window (the default) applies at once, byte-for-byte like `insert` under a
merging resolver. A nonzero window trades a bounded read-side staleness
(`get` is blind to a pending fold until it flushes) for turning
`writers × calls` client calls into one applied record per key per window.

CRDT writer metadata no longer grows without bound under churn.
`crdt::WriterId` pairs a node with the membership incarnation it wrote
under (`now_incarnation_ms()`, already carried in `Msg::Hello`
since protocol 1), so `PnCounter::local_delta` and `OrSet::add` key a slot
by that pair instead of the bare node id, and a restarted node's fresh
incarnation never resumes or corrupts its pre-restart slot.
`ConflictResolver::compact` (defaulted to a no-op, backed by
`PnCounter::compact`/`OrSet::compact`) retires a writer in two stages.
Stage one runs the instant a writer is confirmed dead (gone past
`ClusterConfig::crdt_retire_after`, default: `tombstone_max_ttl`, 24h,
whether it crashed or left gracefully, or live again under a different
incarnation) and the cache is quiet: `membership::member_is_quiet` counts
a member as settled once it has been continuously present for
`crdt_retire_after`, continuously gone for `crdt_retire_after`, or merely
known for twice `crdt_retire_after` however often it has flapped between
the two in that time, safe because any copy such a flapping member can
still bring back is at most one bound old, and reconciles through the
retired entry the fold replaces or the fold receipt it leaves behind.
Stage one moves the writer's `p`/`n` slots or its `seen` watermark into
per-writer retired state that merges correctly under any cross-replica
staleness, since a writer's slot and its retired entry are never summed,
only maxed, and a retirement's timestamp merges by the earliest any
replica recorded. Stage two runs only once that retirement has itself
aged past twice `CompactionBounds::retire_after_ms` with the cache still
quiet: it folds `PnCounter`'s retired entry into a bounded scalar, or
drops `OrSet`'s `seen`/`retired` entries outright, and either way leaves a
per-writer receipt in `folded_at`: the folded writer's own `since_ms`,
recorded under its own key. `OrSet`'s own never-removed elements stay
ordinary set members forever, so retiring a writer blocks only its
*future* adds under that incarnation, never its past ones. `merge` carries
no clock or bound of its own: a side's silence about a folded writer (no
live slot, no retired entry) is trusted as "already folded" only once that
side's own `folded_at` names that writer with a `since_ms` at least as
late as the writer's retirement on the other side. That is an exact,
per-writer fact each side recorded for the writer itself, never inferred
from an unrelated writer's later retirement, so two records independently
folded by different replicas reconcile to the exact total without
double-counting.

A `folded_at` receipt is pruned once it is older than
`CompactionBounds::receipt_ttl_ms` (the longer of three bounds and two
bounds plus two sweep periods), past the point every reachable replica is
guaranteed to have independently folded that writer too.
`ConflictResolver::settle`, a new defaulted trait hook, drops an aged-out
receipt on every merge apply too, right after `ConflictResolver::merge`
(a shard's `SettlingResolver` wrapper is what actually calls it): without
this step a receipt one replica's sweep has already dropped would ride
back in from a peer's still-carrying copy on the very next merge, since a
merge only ever unions two sides' state. A record that still carries a
writer's slot or retired entry on one replica past
`2 * CompactionBounds::retire_after_ms` while another replica has already
folded the writer away, once the receipt that would otherwise reconcile
the two has itself been pruned past `receipt_ttl_ms` on both sides,
double-counts that writer's contribution when the two merge (an `OrSet`
writer's already-removed elements resurrect the same way), the same
trust boundary `tombstone_max_ttl` already accepts for a member gone
that long, not a new one.

Compaction changes a record's bytes with no wire write behind it, so it
mints its own version: `compacted_version` is the stale version's
successor under a content-hash-derived node tiebreaker, the same scheme
a merge's own minted version uses. Two replicas compacting identical
bytes at the same stale version mint identical versions, so anti-entropy
finds nothing to exchange for that key; two that compact to different
bytes (one has judged a writer dead the other has not yet) mint
different versions and anti-entropy does exchange them, converging
through the ordinary `merge`-plus-`settle` path exactly as it would any
other divergent write. Compaction is not invisible to anti-entropy; it
is merely redundant with it whenever replicas already agree.

A per-shard sweep drives both stages whenever the cache's resolver
merges at all, gated only by the per-writer dead and cache-quiet
predicates above, at `ClusterConfig::crdt_sweep_period()` cadence: the
new `crdt_sweep_interval: Option<Duration>` field if set, else a quarter
of `crdt_retire_after` floored at 30s, the same period a shorter override
also feeds into `CompactionBounds::receipt_ttl_ms`. Ticks need no
alignment across replicas (an immediate first tick, then a plain
interval): `settle` and the content-derived version above are what make
eventual agreement enough on their own. One tick now walks the whole
keyspace, calling `ShardOps::compact_pass` repeatedly at
`ClusterConfig::crdt_compact_batch` records per call (default 4,096) and
yielding between calls, so the batch size bounds the stall one call can
cause, not the work a whole tick does. `AbsenceTracker::prune_gone_older_than`,
called at the start of every tick, forgets a member gone past the fold
receipt lifetime, the same age past which its absence no longer reconciles
a writer's records either and at least two sweep periods, so a member that
left between ticks is seen gone before it is forgotten, bounding the
absence tracker under sustained
restarts, where every crashed-and-restarted process leaves behind a node
id (random per process) that never comes back. `PnCounter`'s and
`OrSet`'s record layouts are this crate's own, so there is no released
node to stay wire-compatible with and no protocol gate on when the sweep
may run. A future change to either layout bumps `wire::PROTOCOL_VERSION`
and is versioned then.
`sundog_crdt_retired_writers_total{cache}` counts writers the scan found
eligible for retirement, which can run ahead of
`sundog_crdt_compactions_total{cache}`'s count of records actually
rewritten: a writer counts the moment the scan judges it eligible, even
for a record the pass skips without rewriting (an unowned bucket, or one
that changed underneath the scan).

**What is still open:**

- **Redundant pulls at a partial conflict fraction.** A key one side's pull
  already converged can still mismatch the other side's sketch later in the
  same repair, costing a second pull for content anti-entropy already
  settled. At `conflict_fraction=0.5` and 20,000 keys this costs `merged`
  more total bytes than `decomposed` despite an equal round count; the
  per-key convergence argument bounds rounds for one divergent key, not
  bytes for a whole partition's mix of converged and still-diverging keys.
  Needs root-causing before the exchange's byte cost can be trusted at every
  conflict mix. Separately, the partition-heal sim still runs at a longer,
  race-free anti-entropy tick than production's 200ms default
  (`HEAL_AE_INTERVAL_MS` in `sundog/tests/sim.rs`); a production-cadence
  variant is still needed to confirm the round-count fix above holds there
  too, not only under this harness's generous timing margin.
- **No CRDT resolver ships for a map or a register**, only a counter and a
  set; a user needing either writes their own `ConflictResolver` against the
  same `Merged` contract, or a `MergeResolver<V, F>` generic adapter over an
  arbitrary join-semilattice `V` would remove the boilerplate both reference
  resolvers currently duplicate.
- **A resolver's bytes that fail to decode are rejected silently.** There is
  no dedicated counter for this path, so it is observable only as an absent
  write.
- **No metric for a coalesced fold.** `Cache::merge`'s pending-fold count and
  flush cadence are observable today only through `Cache::events()` and
  `entry_count`, not a dedicated counter.

**Trigger for the open items above:** none yet seen in a real
deployment; each is a known limitation of the mechanism as it stands, not a
scheduled fix.

## Distribution mode

Ships as `Mode::Distributed { owners }` / `Mode::distributed()`. Ownership
lives at the anti-entropy bucket, `xxh3(key) & 1023`, not the key: rendezvous
(highest-random-weight) hashing over the cache's live, protocol-3 peers
advertising the same cache under the same mode and owner count picks each
bucket's `k` owners as a pure function of the live peer set, recomputed from
gossip membership on every change, no ring to keep. `Cache::fetch` is the
network-aware read, local if this node owns the key's bucket and a request to
a live owner in rendezvous order otherwise; `Cache::get` stays local-only, and
a write for a bucket this node doesn't own is forwarded to its owners and
never applied locally. Rebalance pulls a gained bucket from its previous
owners and keeps a lost one resident for `distributed_disown_grace_rounds`
anti-entropy intervals before releasing it, so a new owner's pull has
somewhere to land. Anti-entropy scopes itself to the buckets two peers
co-own instead of pairing every live peer against every other, and every
ownership-scoped RPC carries the requester's view hash: a responder whose own
view disagrees declines with `Msg::StaleView` rather than answering against a
stale owner set.

### What is still open

- **Node identity resets on restart.** `NodeId` is generated fresh per
  process, so a rolling restart with no capacity change reshuffles nearly
  every bucket. A persisted node identity is a follow-up.
- **No quorum.** Gossip membership lets each side of a partition compute its
  own owner set and accept writes; the two sides converge by version alone,
  the same rule every other mode's conflicting writes settle by, once the
  partition heals.
- **Two owners is a narrow margin under back-to-back failures.** A bucket
  pull the view moves past is planned again against the current view, and a
  release hands a bucket to each new owner before dropping it, so two
  membership changes in a row lose nothing on their own. Two owners of the
  same bucket dying inside one rebalance window still take its last copy;
  `owners` above 2 is the only answer, and a pull from a node outside the
  current owner set is not attempted.

## QUIC data plane

The data plane is one `LengthDelimitedCodec`-framed TCP connection per peer,
carrying every message class, `Invalidate`/`Replicate` broadcast traffic,
state-transfer chunk streams, and anti-entropy digest/pull round-trips, over the
same stream, ordered by TCP's own head-of-line blocking. A `quinn`-based QUIC
transport would give each message class its own stream, keeping a large
state-transfer snapshot to a joining node from stalling a latency-sensitive
invalidation behind it.

**Cost:** a second transport implementation behind the existing `net::tcp` seam,
already the `sim`-feature swap point, so the shape exists; a TLS identity for
QUIC's mandatory encryption, sharing the mutual-TLS material the `tls` feature
wires into the TCP path; and connection-migration semantics that a same-LAN
deployment never needs.

**Trigger:** head-of-line blocking between state-transfer/anti-entropy traffic
and live broadcast traffic shows up as measured tail latency, not as a
theoretical concern. The per-class outbox split and request-response traffic
living outside the broadcast channel already remove the worst of this at the
application layer; QUIC would only matter for what's left after that.

## Tiered storage

A local SSD/NVMe tier behind the in-memory tables is the `spill` feature,
off by default. `CacheBuilder::spill(SpillConfig::new(dir,
capacity_bytes))` lets eviction demote cold entries onto a FIFO ring of
region files instead of discarding them, and a later read promotes a
spilled entry back into RAM. It composes with `Mode::Replicated`'s and
`Mode::Distributed`'s capacity bound alike: eviction demotes rather than
deletes, so anti-entropy does not need to silently re-pull an evicted entry
back. It does nothing about the reason a replicated cluster runs out of
memory: every node still holds every entry. `Mode::Distributed` removes that
reason, since a node only holds the buckets it owns. The spill tier only
extends how much of what a node does hold its disk, rather than its RAM, can
carry.

A spilled entry still keeps its full key resident. Only the value moves to
disk, so the fixed per-key bookkeeping stays in RAM regardless of how cold
the entry is: key, version, expiry, and disk pointer. That is deliberate:
it keeps a spilled entry a normal member of the live table, with no second
index to keep in sync. RAM per spilled entry does not shrink below one
key's worth, however small the value it replaced.

**Cost:** a real on-disk index keyed by a hash of the key instead of the key
itself, plus the collision handling that implies. A hash match must confirm
against the record's stored key before trusting it, so an occasional
false-positive disk read replaces a guaranteed-correct in-memory comparison.

**Trigger:** a `spill`-configured deployment whose per-node RAM is bound by
the number of spilled keys rather than by the resident values spilling was
built to move off-heap: many small values behind large keys, or a cold
working set large enough that a fixed ~100 bytes a key adds up. Not before.
Today's resident-key design is simpler and correct, and nothing seen
yet needs trading that away.

## Distributed locks and leader leases

A lock or a lease is a promise that at most one holder exists. sundog's
membership is gossip with no quorum, so under a partition each side computes
its own view and both sides can grant the lease. Every construction on top of
that either admits two holders or bolts on a consensus protocol, which is a
different system. The stance is a refusal: anyone who needs a lease needs
etcd or a database row, and sundog stays a cache.

Coordinator-free rate limiting and counters are a different question: they
are CRDTs, and they wait on merge resolvers above.
