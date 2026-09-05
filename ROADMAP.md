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
happen. For distribution mode, the same key places replicas across zones.

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

A consistent-hash ring over the live member set, `numOwners` primary and backup
replicas per key instead of every node holding every entry, and the hard part:
rebalancing the ring and streaming ownership transfers on every membership
change without dropping a write or serving a stale primary mid-transition.
`Replicated` mode sidesteps nearly all of this: every node is trivially owner of
everything, so join and leave need only a snapshot pull plus anti-entropy, never
a rebalance, which is why state transfer fits in a page of design instead of a
subsystem. A rebalancing coordinator, conflict resolution during rebalance, and
partial-rebalance-on-partition logic are the hardest parts of building a
distribution mode.

**Feasibility, assessed against the code:** feasible with restrictions.
Ownership belongs at the anti-entropy bucket, `xxh3(key) & 1023`, not the key:
the 1,024 buckets already have digests, entry enumeration, and chunked
streaming, so "who owns bucket b" is the only new question. Rendezvous hashing
answers it as a pure function of the live `Peer` list: no ring to maintain,
adding or losing one node moves about `1/n` of the buckets, and `owners = k` is
"the top k scores." The mesh's per-peer outboxes and pooled request-response
path carry a non-owner read as one more request shape. Fan-out groups records by
owner set instead of broadcasting; anti-entropy pairs only peers sharing a
bucket; state transfer runs on every membership change, scoped to newly owned
buckets, not once at `open()`; tombstone-GC deferral asks whether an *owner* of
the bucket is absent, not whether any member is.

Three gaps are real, not paperwork. `NodeId` is generated fresh per process, so
a rolling restart with no capacity change would rehash nearly every bucket: a
persisted node identity is a prerequisite. Gossip membership has no quorum, so a
partition lets each side compute its own owner set and both accept writes;
convergence after the heal is by version only, and the design must say so
plainly. Nothing tracks an ownership epoch, so a transfer can complete against a
stale owner set when membership changes twice mid-flight. Restrictions that keep
the mode honest: `owners >= 2` always, since a single lost owner is a lost
bucket; no finite `max_capacity` on a distributed cache, the same guard
`Replicated` has, for the same reason; and a non-owner read as a separately
typed operation, not a silent network hop inside `get`.

**Cost:** roughly 3,000-5,000 lines plus a comparable volume of sim and property
coverage, and a new invariant class for the test stack: the union across owners
converges, and a non-owner never applies a record it was not sent, alongside
rebalance-under-churn and partition-then-heal scenarios that reconcile
ownership, not only versions. On the order of 8-14 weeks for one engineer at
this repository's verification bar.

**Trigger:** replicated memory footprint, every node holding every entry,
becomes the binding constraint on cluster size or value volume, not that it
would be more efficient, but a deployment that has hit the ceiling `Replicated`
mode imposes. Until that's observed, `Invalidation` mode covers the common case
of bounding per-node memory while keeping cross-node correctness.

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

A local NVMe tier behind the in-memory tables, cold entries spilling to disk
and reading back in microseconds, is how a node holds more than its RAM. It is
also an index, a compaction schedule, an I/O path, and a second set of failure
modes, and it does nothing about the reason a replicated cluster runs out of
memory: every node holds every entry. Distribution mode removes that reason;
tiered storage only postpones it per node.

**Cost:** a storage engine of its own, comparable to distribution mode.

**Trigger:** a deployment already running distribution mode whose per-node
working set still exceeds RAM. Not before.

## Distributed locks and leader leases

A lock or a lease is a promise that at most one holder exists. sundog's
membership is gossip with no quorum, so under a partition each side computes
its own view and both sides can grant the lease. Every construction on top of
that either admits two holders or bolts on a consensus protocol, which is a
different system. The stance is a refusal: anyone who needs a lease needs
etcd or a database row, and sundog stays a cache.

Coordinator-free rate limiting and counters are a different question: they
are CRDTs, and they wait on merge resolvers above.
