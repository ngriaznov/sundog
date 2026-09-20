# Roadmap

Design sketches for what the crate leaves out, each cut to keep the core
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

`Replicated` mode bounds capacity only through the spill tier, since evicting
locally without one makes replicas differ. A byte-accurate accounting of
keys, values, and per-entry overhead, a `sundog_cache_bytes{cache}` gauge,
and a soft ceiling that rejects writes with a typed error keep every replica
identical under memory pressure without a disk behind it.

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

## Merge resolvers

`ConflictResolver::merge`, `Cache::merge`, the coalesce window, writer
retirement and the `sundog::crdt` reference types ship. What is still open:

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
  variant is still needed to confirm the round-count result holds there
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

**Trigger:** none yet seen in a real deployment; each is a known limitation
of the mechanism as it stands, not a scheduled fix.

## Distribution mode

`Mode::Distributed { owners }` ships. What is still open:

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

## Tiered storage: an on-disk key index

The `spill` feature ships. A spilled entry still keeps its full key resident:
only the value moves to disk, so the fixed per-key bookkeeping stays in RAM
regardless of how cold the entry is: key, version, expiry, and disk pointer.
That is deliberate: it keeps a spilled entry a normal member of the live
table, with no second index to keep in sync. RAM per spilled entry does not
shrink below one key's worth, however small the value it replaced.

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
are CRDTs, and the merge resolvers above cover them.
