# Roadmap

Design sketches for what v1 deliberately excludes, each cut to keep the core
buildable and correct first. Every section states its cost and trigger, so
revisiting one is a decision made on evidence, not on itch.

Nothing here is scheduled: a section becomes code only once its trigger
condition is observed in a real deployment, not because it would be interesting
to build.

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

## Cluster-wide max-idle

Touch-propagating max-idle (TTI) across a distributed cache, every read anywhere
resetting every replica's idle clock, sounds like a small feature but drags in a
specific set of anomalies: touch messages that themselves need to be reliable
and ordered against concurrent writes, idle timers that drift apart under any
message loss, and a feature that turns reading a cache into cluster-wide
chatter. sundog's stance is **local-only, permanently**: `.tti()` on a
`CacheBuilder` bounds only that node's own idle eviction and is never gossiped
or replicated. This is not a gap to close later; it's a deliberate refusal to
build a subsystem whose failure modes cost more than the feature is worth.

**Cost / trigger:** none tracked. Anyone who needs cluster-wide idle expiry
needs a different tool than an embedded cache library; see "remote thin clients"
below for the general shape of that advice.

## Remote thin clients vs. "run Valkey instead"

A thin-client protocol would let non-embedding clients talk to a cluster over a
small binary TCP protocol with topology hints, so a process that isn't a Rust
member of the cluster could still be a cache client. sundog is embedded-only by
design: the whole point is that every participant runs the identical stack with
no roles and no separate server tier. A thin-client protocol would mean
designing and versioning a second, external wire format alongside the internal
one, plus routing and topology awareness for clients that aren't cluster members
and can't watch chitchat's gossip.

**Cost:** effectively a second product: protocol design, client-side topology
tracking, versioning discipline for a wire format now consumed by code sundog
doesn't control the release cadence of.

**Trigger, and the honest caveat:** the moment a remote thin client is wanted,
the right first move is to compare against running [Valkey](https://valkey.io/),
or Redis, as a standalone cache server and using its already-mature,
already-multi-language client ecosystem. sundog's entire value proposition is
*embedded* zeroconf clustering for services that already share a process
boundary with their cache; a remote-client story gives that up in exchange for
reinventing a strictly worse Valkey. This item stays here as a sketch, not a
commitment, for exactly that reason.
