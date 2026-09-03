# Roadmap

Design sketches for what v1 deliberately does not do. Each cut here was made
to keep the core buildable and correct first; this document is where those
cuts get an honest paragraph on cost and trigger, so revisiting one is a
decision made on evidence, not on itch.

None of what follows is scheduled. A section moves from here into actual
code only once its trigger condition is observed in a real deployment, not
because it would be interesting to build.

## Bespoke store engine (sub-200ns reads)

The store rides on [`moka`](https://crates.io/crates/moka), which supplies
TTL/TTI expiry, size- and weight-bounded TinyLFU eviction, and the
stampede-collapsed `get_or_load` path — battle-tested semantics the whole
verification stack leans on. It also sets the performance floor: a `moka`
`get` costs ~600ns of the ~700ns a `Shard::get` measures end to end, and
its async insert path is the largest single slice of a ~3.4µs replicated
write. State-of-the-art concurrent cache reads (Caffeine-class designs)
land in the 50–150ns range, so a purpose-built engine — our own concurrent
map, TTL wheel, eviction policy, and stampede collapse behind the existing
`ShardOps` seam — is worth an estimated 5× on reads and 3× on writes, and
speeds up bulk remote apply in proportion. Nothing above the store (wire,
fan-out batching, anti-entropy) needs to change; the replication pipeline
already converges within a fraction of a second of the writes landing.

**Cost:** a multi-week engine build whose hardest parts are exactly the
semantics `moka` currently guarantees for free — expiry correctness under
concurrent writes, eviction that can't resurrect stale entries, and
stampede collapse without deadlock — all of which the permutation
proptests, TTL-guarantee ladder, and churn suites must hold green through
the swap.

**Trigger:** a benchmark against a competing embedded cache where the read
path is the measured, deciding gap — or read latency showing up as the
binding constraint in a real deployment. Until then the current numbers
already lead the embedded-replicated category, and the risk budget is
better spent on real-workload mileage.

## Distribution mode

A consistent-hash ring over the live member set, `numOwners` primary+backup
replicas per key instead of every node holding every entry, and — the hard
part — rebalancing the ring and streaming ownership transfers whenever
membership changes, without ever dropping a write or serving a stale
primary during the transition. sundog's `Replicated` mode sidesteps nearly
all of this: every node is trivially "owner" of everything, so join/leave
only ever needs a snapshot pull plus anti-entropy, never a rebalance. That
simplification is why state transfer fits in a page of design instead of a
subsystem — a rebalancing coordinator, conflict resolution during
rebalance, and partial-rebalance-on-partition logic are the hardest part of
building a distribution mode at all.

**Cost:** a consistent-hash ring, an owner-set computation that stays stable
under single-node churn, a rebalance protocol that can be interrupted by a
second membership change mid-flight, and a decision about what a read does
while its key's ownership is in motion. Every one of those interacts with
the anti-entropy and state-transfer machinery already built, not in addition
to it.

**Trigger:** replicated memory footprint (every node holding every entry)
becomes the binding constraint on cluster size or value volume — not "it
would be more efficient," but a deployment that has hit the ceiling
`Replicated` mode imposes. Until that's observed, `Invalidation` mode already
covers the common case of bounding per-node memory while keeping cross-node
correctness.

## QUIC data plane

The data plane is one `LengthDelimitedCodec`-framed TCP connection per peer,
carrying every message class — `Invalidate`/`Replicate` broadcast traffic,
state-transfer chunk streams, and anti-entropy digest/pull round-trips — over
the same stream, ordered by TCP's own head-of-line blocking. A `quinn`-based
QUIC transport would give each message class its own stream, so a large
state-transfer snapshot streaming to a joining node can no longer stall a
latency-sensitive invalidation behind it.

**Cost:** a second transport implementation behind the existing `net::tcp`
seam (already the `sim`-feature swap point, so the shape exists), a TLS
identity story for QUIC's mandatory encryption — which folds together with
the mutual-TLS identity material the `tls` feature already wires into the
TCP path (`net::tls`, `ClusterConfig::tls`) — and connection-migration
semantics that don't currently matter to a same-LAN deployment.

**Trigger:** head-of-line blocking between state-transfer/anti-entropy
traffic and live broadcast traffic shows up as measured tail latency in
practice, not as a theoretical concern. The per-class outbox split and
request/response traffic living outside the broadcast channel already
remove the worst of this at the application layer; QUIC would only matter
for what's left after that.

## Set reconciliation past ~10⁶ entries (IBLT / minisketch)

Anti-entropy compares 1,024 XOR-of-hash bucket digests per round — cheap,
unconditional, incrementally maintained in O(1) per write, no rescans. Its
cost is per-bucket, not per-entry, so it scales with cache *size* only
insofar as bucket collision rates rise: at some entry count, enough distinct
keys land in the same bucket that a single digest mismatch forces pulling
the whole bucket's key/version list to find the actual diff, even when only
one entry disagrees. Invertible Bloom lookup tables or minisketch-style set
reconciliation would let two peers exchange a sketch sized to the *diff*,
not the bucket, regardless of how many entries share a bucket.

**Cost:** a second reconciliation protocol alongside the existing digest
round, a decision about the sketch size (over-provisioned wastes bandwidth,
under-provisioned fails to decode and falls back anyway), and — the part
that matters — this only pays for itself once buckets are hot enough that
the fallback (full bucket pull) is happening routinely rather than as the
rare exception it's designed to be.

**Trigger:** measured anti-entropy bandwidth per round exceeds budget on a
cache in the ~10⁶-entries-and-up range, i.e. `BUCKET_COUNT` (1,024, fixed
today) is no longer coarse enough to keep bucket pulls rare. Below that scale
the current design is simpler and already convergence-bounded.

## Cluster-wide max-idle

Touch-propagating max-idle (TTI) across a distributed cache — every read
anywhere resets every replica's idle clock — sounds like a small feature
but drags in a specific set of anomalies: touch messages that themselves
need to be reliable and ordered against concurrent writes, idle timers that
drift apart under any message loss, and a feature that turns "read a cache"
into cluster-wide chatter. sundog's stance is **local-only, permanently**:
`.tti()` on a `CacheBuilder` bounds only that node's own idle eviction and is
never gossiped or replicated. This is not a gap to be closed later — it's a
deliberate refusal to build a subsystem whose failure modes cost more than
the feature is worth.

**Cost / trigger:** none tracked. Anyone who needs genuinely cluster-wide
idle expiry needs a different tool than an embedded cache library — see
"remote thin clients" below for the general shape of that advice.

## Remote thin clients vs. "just run Valkey"

A thin-client protocol would let non-embedding clients talk to a cluster
over a small binary TCP protocol with topology hints, so a process that
isn't a Rust member of the cluster could still be a cache client. sundog is
embedded-only by design — the whole point is that every participant runs the
identical stack with no roles and no separate server tier. A thin-client
protocol would mean designing and versioning a second, external wire format
alongside the internal one, plus routing/topology-awareness for clients that
aren't cluster members and can't watch chitchat's gossip.

**Cost:** effectively a second product — protocol design, client-side
topology tracking, versioning discipline for a wire format now consumed by
code sundog doesn't control the release cadence of.

**Trigger, and the honest caveat:** the moment a remote thin client is
wanted, the right first move is to compare against running
[Valkey](https://valkey.io/) (or Redis) as a standalone cache server and
using its already-mature, already-multi-language client ecosystem. sundog's
entire value proposition is *embedded* zeroconf clustering for services that
already share a process boundary with their cache; a remote-client story
gives that up in exchange for reinventing a strictly worse Valkey. This item
stays on the roadmap as a sketch, not a plan, for exactly that reason.
