# Roadmap

Design sketches for what the crate leaves out, each cut to keep the core
buildable and correct first. Every section states its cost and trigger, so
revisiting one is a decision made on evidence, not on itch.

Nothing here is scheduled: a section becomes code only once its trigger
condition shows up in a real deployment, not because it would be interesting
to build. The exceptions are under "Next": small, self-contained, and
already justified by the code as it stands.

## Next

### Batch reads

`insert_many`, `insert_many_with_ttl` and `remove_many` have no plain read
counterpart; `load_many` reads many keys only through a loader. `get_many`
takes one stripe lock per distinct bucket instead of one per key, and
`fetch_many` groups keys by owner into one request per owner instead of one
round trip per key.

### Cluster health with ownership, backlog and spill

`Cluster::peers()` and `Cluster::health()` exist. `Health` carries
readiness, the live-peer count and each cache's mode and warmth, and no
more. An operator asking which buckets this node owns, how deep the fan-out
backlog is, or how much of the spill tier is used reads a metric or
nothing. `Health` grows a per-cache ownership summary, the backlog depth and
the spill tier's bytes and entries, from state the crate already tracks.

## Reach

### A RESP-speaking server

Every Redis client speaks RESP and none speaks sundog. A `sundog-server`
binary that opens a cache and answers GET, SET, DEL, EXPIRE and MGET over
RESP lets `redis-cli`, `memtier` and every client library talk to a cluster,
and lets `sundog-bench` measure a networked sundog through the same RESP
client it uses for Redis and Valkey. MGET is
`get_many` above. EXPIRE, PERSIST and TTL are `Cache::expire`,
`Cache::persist` and `Cache::ttl_of`.

### An observer

`sundog::observe::Observer` joins a cluster's gossip without caches or a data
plane and returns a `ClusterSnapshot`: every member's status, protocol and
advertised caches, and the part ownership of each `Distributed` cache
computed from gossip. `sundog-lens watch --once` prints that snapshot as text
or JSON. A member that advertises no caches owns nothing under every mode, so
the observer is never a peer.

What remains is what gossip does not carry: per-part digests, the keys a
node holds and per-part residency (cold, unverified, releasing). An observer
reads them over the state-transfer and anti-entropy requests a donor already
answers, and the observer then dials nodes, which it does not do today.

### Explaining a read

When a read answers a miss or `FetchUnavailable`, nothing says why. The
answer depends on which nodes this node's view names as owners, whether its
copy of the key's part is cold, unverified, stale or releasing, which pull
or anti-entropy round last touched that part, and which owner answered.
`Cache::explain(&key)` returns that record. A bounded per-part ring of
residency events (gained, dropped, marked cold, pulled from a donor,
settled) feeds it, off by default or sampled, and one request asks each
owner for its own side. An observer would read the same request.

**Trigger:** an operator asking why a read missed, with nothing to read but
counters.

### A live cluster view

`sundog-lens watch` is a terminal UI over the observer that redraws
continuously: members joining, leaving gracefully and crashing, part
ownership moving between them as a mosaic with the parts each view change
moves, a settled state per cache, per-node rates, hit ratios and rebalance
traffic from each node's Prometheus exporter, and a lifeline per node.
`sundog-lens demo` plays a scripted tour of those events against a local
fleet and is the recording the README shows.

What remains needs data nothing exports yet. Pulls and repairs in flight per
part need the state-transfer and anti-entropy requests of the observer
above. The parts whose fetches are slowest or most frequent need per-part
counts: `sundog_fetch_duration_seconds` is per cache.

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

### Hot keys

Nothing tracks per-key access frequency. A sampled hot-key list, fed from
the admission sketch above once it exists, reports through `Health` and a
gauge which keys a node serves most.

### Hot-key read replicas

In `Distributed` mode every read of a key a node does not own is a round
trip to an owner, however often the key is read. A key the hot-key list
above names gets a short-lived copy on each node that reads it, under a
read lease from its owners: a write to the key invalidates every leased
copy before it lands, and a copy past its lease reads through to the owners
again. Owners stay the source of truth, so a sharded cache serves its
hottest keys at `Replicated` read latency. A wire change with the usual
bump.

**Trigger:** a `Distributed` deployment whose fetch rate is concentrated on
a few keys.

### Changing a cache's mode in place

A cache's mode is fixed at `open()`, and moving from `Replicated` to
`Distributed` when a cache outgrows one node's memory means closing it on
every node and refilling it. Rebalance already moves parts between owners.
A mode change advertised through gossip runs it from a view where every
node owns every part to a sharded one, with each node releasing the parts
it no longer owns through the disown grace, and back the other way by
pulling every part. Reads keep answering throughout, as they do during
ordinary churn.

**Trigger:** a deployment that needs to change a cache's mode without
taking it down.

### Per-record compression

No compression anywhere in the store. A replica caches a replicate frame's
bytes as its resident record, so compressed storage either decompresses
before every send or ships compressed bytes, and the second is a wire
change with a protocol bump and gated responders. Behind a feature, with a
per-cache trained dictionary and a size floor under which a record stays
raw.

**Trigger:** a deployment whose values are text or structured payloads and
whose RAM is bound by them.

## Reads

### Refresh-ahead in invalidation mode

`CacheBuilder::refresh_ahead` reloads a read key on the one node its loads
gather on, and the new value travels to every node that holds it. An
`Invalidation` cache sends no values, so each node holding a key would need
its own reload, gathered on the loader node without that node answering from
its own cached copy. That takes a load request that bypasses the responder's
cache: a new message kind with the usual bump.

**Trigger:** an `Invalidation` deployment whose hot keys expire under load
and that cannot move to `Replicated` or `Distributed`.

### Hedged fetches

A fetch asks one owner and waits for it. An owner that is slow but alive
holds the read for the whole round trip, so a fetch's tail latency is its
slowest owner's. A hedged fetch sends to a second owner once the first has
taken longer than the fetch latency's p95 and takes whichever answer
arrives first. The exported `sundog_fetch_duration_seconds` histogram is
not readable in process, so the threshold is either configured from it or
tracked by the node itself.

**Trigger:** a fetch p99 well above its p50 on a cluster whose owners are
all healthy.

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

## Distributed locks and leader leases

A lock or a lease is a promise that at most one holder exists. sundog's
membership is gossip with no quorum, so under a partition each side computes
its own view and both sides can grant the lease. Every construction on top of
that either admits two holders or bolts on a consensus protocol, which is a
different system. The stance is a refusal: anyone who needs a lease needs
etcd or a database row, and sundog stays a cache.

Coordinator-free rate limiting and counters are a different question: they
are CRDTs, and `sundog::crdt`'s merge resolvers cover them.
