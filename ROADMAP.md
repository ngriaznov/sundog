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

### Memory ceilings that refuse rather than diverge

`Replicated` mode bounds capacity only through the spill tier, since evicting
locally without one makes replicas differ. `max_capacity` counts entries, or
a `Weigher`'s value, and a weigher sees the decoded key and value, not their
encoded length, and counts payload alone: never the 56-byte entry, its index
slot, or the allocator's rounding. A built-in byte weigher over the encoded
record plus that per-entry overhead, a `sundog_cache_bytes{cache}` gauge fed
from the engine's existing weight total, and a soft ceiling that rejects
writes with a typed error keep every replica identical under memory pressure
without a disk behind it.

### TTL surface

Nothing on `Cache` changes an entry's lifetime after the write that set it
or reads how much of it remains. `expire` and `touch` are writes: a re-put
of the entry's stored value bytes under a fresh version and a new
`expires_at_ms`, riding the `Replicate` record that already carries all
three, so no wire change. `ttl_of` is a read that returns a duration and
takes none, which keeps reads TTL-blind.

### Batch reads

`insert_many`, `insert_many_with_ttl` and `remove_many` have no read
counterpart. `get_many` takes one stripe lock per distinct bucket instead of
one per key, and `fetch_many` groups keys by owner into one request per
owner instead of one round trip per key.

### Latency histograms and a resident-bytes gauge

Every `sundog_*` metric is a counter or a gauge. Latency exists only as two
accumulating sums, the fan-out and spill wait totals, with no buckets and no
timing at all for a hit, a miss, a fetch or a spill read. Histograms for
those four, plus the byte gauge above, are what a dashboard needs to show a
p99. The exporter test pins each one.

### A span on the fetch path

State transfer and anti-entropy rounds already carry `tracing` spans, and a
user's own OpenTelemetry subscriber already receives every event the crate
emits. `fetch` has no span, so an owner round trip is invisible in a trace.
One span with the owner, the outcome and the attempt count closes that.

### Cluster health with ownership, backlog and spill

`Cluster::peers()` and `Cluster::health()` exist. `Health` carries
readiness, the live-peer count and each cache's mode and warmth, and no
more. An operator asking which buckets this node owns, how deep the fan-out
backlog is, or how much of the spill tier is used reads a metric or
nothing. `Health` grows a per-cache ownership summary, the backlog depth and
the spill tier's bytes and entries, from state the crate already tracks.

### Drop the spill reverse index

Every spilled entry keeps its key twice in RAM: the key half of its record on
the entry, and a second copy in the region's reverse index, held so a region
rotating out can purge the keys still pointing into it. The on-disk record
header already stores the key, so reclaim can read the region's headers
sequentially instead, one 64 MB scan off the hot path. For a 16-byte key
that is 157 bytes of RAM per spilled entry today against about 85 without
the index. This is the first step of the on-disk key index below and stands
on its own.

## Proof

### A benchmark harness with competitors

The benches under `sundog/tests` measure sundog alone, and the README's only
competitor figure is memory per entry against Redis 7. Nothing measures
throughput or latency against anything else. A workload crate with a zipf
key distribution drives sundog, Redis, Dragonfly, Olric and Hazelcast
through the same operations, reports p50 and p99 for reads, writes and
fetch plus bytes per entry, and runs weekly next to the Scale workflow.
The scale gate's 100 ms fetch p99 covers a 3-node, 4M-key run whose fetch
crosses the network and the SSD, and stays. A separate in-RAM local bench
carries the sub-millisecond gate.

**Trigger:** any claim of a lead over another system. Without these
benchmarks no such claim has a number behind it.

### A hundred nodes in simulation

The README targets 2 to 30 nodes on a LAN. The largest scenario anywhere in
the repository runs 5: 4 in the turmoil suite and the chaos run, 5 in the
container suite. Nothing in gossip caps the count, but three things grow
with it: one pair of bounded outboxes and a connection pool per peer, a
replicated write fanning out to every peer, and gossip state growing as
nodes times caches. A turmoil scenario at 100 nodes with a rolling restart
proves or corrects the README's number, and the README says what it proves.

## Reach

### A RESP-speaking server

Every Redis client speaks RESP and none speaks sundog. A `sundog-server`
binary that opens a cache and answers GET, SET, DEL, EXPIRE and MGET over
RESP lets `redis-cli`, `memtier` and every client library talk to a cluster,
and gives the competitor benchmarks one driver for every system. MGET is
`get_many` above. EXPIRE is `expire` above.

### An observer

No CLI exists; `sundog-testnode` is a container test node driven by a line
protocol. A member that joins gossip and advertises no caches already owns
nothing under every mode, which is the seam. An observer binary joins that
way and dumps peers, ownership, digests and keys over the wire, through the
state-transfer and anti-entropy requests a donor already answers.

### Snapshot export and import

Persistence is the spill tier's checkpoint, behind the `spill` feature. A
`Replicated` node warms from a live donor, and a whole-cluster restart with
no donor leaves every node warm and empty. `Cache::export` streams the
shard's snapshot chunks to a writer and `Cache::import` applies them as
remote records, versions intact, so a restart from a file converges with a
peer the way a join does.

## Store

### Owner-side atomic update

`Cache::merge` combines values that commute. Everything else that needs
read-modify-write does a get and an insert, and a test in `cluster.rs`
documents that this loses updates under last-write-wins. In `Distributed`
mode a non-owner's write already forwards to the owners, so an
`update_with` closure forwards the same way and runs on the owner under the
stripe lock: compare-and-set, insert-if-absent and increments without a
resolver. A wire change with the usual bump.

### Admission by frequency

Capacity eviction is sampled LRU: the idlest of 8 sampled entries goes, or
the idlest 8 of 32 in a batch. Nothing counts how often a key is read, so
one scan of cold keys evicts a hot set. A count-min sketch of 4-bit
counters, sized as Caffeine sizes its own at about 8 bytes per entry of
capacity, and a doorkeeper filter in front of eviction admit a new entry
only when it is likelier to be read again than the victim.

### A byte arena for heap records

A record over 22 encoded bytes takes its own `Box<[u8]>`. On the 16-byte
key, 100-byte value shape the record is 119 bytes and lands in a 128-byte
allocator chunk; with the 56-byte entry and about 5 bytes of index that
accounts for 189 bytes, and the measured 209.6 leaves about 21 bytes of
fragmentation and slack per entry. A per-stripe arena of bump-allocated
pages holds those records back to back and reclaims holes on the tombstone
sweep, the tick that runs for every cache. Compaction runs only for a
merging cache and is the wrong host.

**Trigger:** the entry-diet bench on that shape above Redis 7's computed
184 bytes, which is where it sits today.

### Hot keys

Nothing tracks per-key access frequency. A sampled hot-key list, fed from
the admission sketch above once it exists, reports through `Health` and a
gauge which keys a node serves most.

### Per-record compression

No compression anywhere in the store. A replica caches a replicate frame's
bytes as its resident record, so compressed storage either decompresses
before every send or ships compressed bytes, and the second is a wire
change with a protocol bump and gated responders. Behind a feature, with a
per-cache trained dictionary and a size floor under which a record stays
raw.

**Trigger:** a deployment whose values are text or structured payloads and
whose RAM is bound by them.

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
- **Every node weighs the same.** Rendezvous scoring hashes the node id and
  the bucket and nothing else, so a node with twice the RAM owns the same
  share as one with half. A per-node weight in `ClusterConfig`, gossiped
  beside the node's other keys and folded into `view_hash`, since that hash
  rides in fetch and transfer frames as the epoch check, scales the score.
  A protocol bump.
- **A bucket is a coarse unit at a hundred nodes.** Ownership, pull and
  release move whole buckets, 1,024 of them. At 30 nodes each holds about
  34 with a 17 percent spread from hash variance alone; at 100 nodes about
  10 with a 31 percent spread. The 64 anti-entropy parts under each bucket
  give 65,536 units, about 655 per node at 100 with a 4 percent spread.
  Stripes stay at 1,024, since a part lives inside its bucket's stripe and
  part-scoped reads already exist. The bucket ids in every transfer and
  fetch message change meaning, so a protocol bump.

**Trigger** for the last two: a deployment past 30 nodes, or one with
heterogeneous machines, measuring the owned-entry spread across nodes.

## Zone-aware donor and repair choice

Every replicated node holds every entry, so a write crosses every zone once
whatever the topology. That traffic is the floor. What is not the floor is
where a joiner pulls its snapshot from and which peer a node reconciles with:
the joiner takes the live donor with the lowest node id, and anti-entropy
takes a dirty-marked peer first and a uniformly random live peer otherwise.
A `zone` key in gossip state, set from `ClusterConfig::zone`, lets a joiner
prefer a warm donor in its own zone and lets anti-entropy weight same-zone
peers, which is where the bulk transfers happen. `Mode::Distributed`'s
rendezvous scoring has no such input today. The same `zone` key would
extend it to spread a bucket's owners across zones instead of scoring every
eligible peer the same regardless of where it runs.

**Trigger:** a multi-zone deployment measuring cross-zone egress from joins
or repairs.

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
regardless of how cold the entry is: an 80-byte entry holding the key,
version, expiry, and disk pointer, plus its index slot. That is deliberate:
it keeps a spilled entry a normal member of the live table, with no second
index to keep in sync. RAM per spilled entry does not shrink below one key's
worth, however small the value it replaced.

A fingerprint index replaces that: a 64-bit hash of the key, the 16-byte
disk location, and the version and expiry a wire record needs, 48 bytes per
entry before table overhead, with the key itself confirmed against the
on-disk record header on every read. A hash match must confirm against the
record's stored key before trusting it, so an occasional false-positive
disk read replaces a guaranteed-correct in-memory comparison.

| Per spilled entry, 16-byte key | RAM |
|---|---:|
| Today | about 157 B |
| Reverse index dropped (under Next) | about 85 B |
| Fingerprint index | 48 B |
| 4 billion entries at 48 B | 192 GB before table overhead |

Reads stay bounded separately. A spilled read is one `spawn_blocking`
positional read behind a semaphore, 16 permits by default, and
`SpillConfig::read_concurrency` raises that with no cap. NVMe delivers its
random-read rate at queue depths far above 16, so the spill bench first
measures what raising the permit count alone recovers; only if the thread
hop itself is the bound does an io_uring submission ring under a
Linux-only feature pay for itself.

**Trigger:** a `spill`-configured deployment whose per-node RAM is bound by
the number of spilled keys rather than by the resident values spilling was
built to move off-heap: many small values behind large keys, or a cold
working set large enough that a fixed 85 bytes a key adds up. At a terabyte
of 256-byte values on disk that is 4 billion keys and 340 GB of index,
against 192 with fingerprints.

## Distributed locks and leader leases

A lock or a lease is a promise that at most one holder exists. sundog's
membership is gossip with no quorum, so under a partition each side computes
its own view and both sides can grant the lease. Every construction on top of
that either admits two holders or bolts on a consensus protocol, which is a
different system. The stance is a refusal: anyone who needs a lease needs
etcd or a database row, and sundog stays a cache.

Coordinator-free rate limiting and counters are a different question: they
are CRDTs, and the merge resolvers above cover them.
