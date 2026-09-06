# Roadmap

Design sketches for what v1 deliberately excludes, each cut to keep the core
buildable and correct first. Every section states its cost and trigger, so
revisiting one is a decision made on evidence, not on itch.

Nothing here is scheduled: a section becomes code only once its trigger
condition is observed in a real deployment, not because it would be interesting
to build. The exceptions are under "Next": small, self-contained, and
already justified by the code as it stands.

## Next

### Clock-skew guard

`HlcClock::observe` absorbs any remote stamp. One node with a clock an hour
ahead wins every write cluster-wide and drags every other node's clock forward
with it, and nothing reports it. A `max_clock_skew` on `ClusterConfig`
rejects a remote stamp further ahead than that, counts the rejection, and logs
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
whatever the topology; that traffic is the floor. What is not the floor is
where a joiner pulls its snapshot from and which peer a node reconciles with:
both pick by node id today. A `zone` key in gossip state, set from
`ClusterConfig::zone`, lets a joiner prefer a warm donor in its own zone and
lets anti-entropy weight same-zone peers, which is where the bulk transfers
happen. `Mode::Distributed`'s rendezvous scoring has no such input today; the
same `zone` key would extend it to spread a bucket's owners across zones
instead of scoring every eligible peer the same regardless of where it runs.

**Cost:** a few hundred lines; membership, state transfer, and the scheduler's
peer choice.

**Trigger:** a multi-zone deployment measuring cross-zone egress from joins
or repairs.

### Merge resolvers

`ConflictResolver::winner` picks one of two records; it cannot produce a
third. A merge outcome, the stored and incoming records folded into a new
value, is what a PN-counter or an observed-remove set needs to converge under
concurrent writes. The versioned apply already runs the resolver under the
stripe lock with both encoded values in hand, so the engine change is small;
the API change is a new `Winner` variant, which is a break on an exhaustive
enum and waits for the next major.

**Cost:** the variant, the apply path, and a CRDT property suite proving
merge is commutative, associative, and idempotent for the reference types.

**Trigger:** a user with a counter or set that concurrent writers clobber
under last-write-wins.

## Distribution mode

Ships as `Mode::Distributed { owners }` / `Mode::distributed()`. Ownership
lives at the anti-entropy bucket, `xxh3(key) & 1023`, not the key: rendezvous
(highest-random-weight) hashing over the cache's live, protocol-3 peers
advertising the same cache under the same mode and owner count picks each
bucket's `k` owners as a pure function of the live peer set, recomputed from
gossip membership on every change, no ring to maintain. `Cache::fetch` is the
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
  the same rule every other mode's conflicting writes resolve by, once the
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
reason, since a node only holds the buckets it owns; the spill tier only
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
Today's resident-key design is simpler and correct, and nothing observed
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
