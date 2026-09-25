<p align="center">
  <img src="assets/social-card.png" alt="sundog: an embedded, replicated cache for Rust" width="720">
</p>

[![CI](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml/badge.svg)](https://github.com/ngriaznov/sundog/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/sundog.svg)](https://crates.io/crates/sundog)
[![docs.rs](https://img.shields.io/docsrs/sundog)](https://docs.rs/sundog)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

# sundog

sundog is an embedded, replicated cache for Rust services. Every process on the
network finds the others, forms a cluster over gossip, and keeps named caches
coherent between them. No cache server, no coordinator, no config beyond a
cluster name. Caches run in one of four modes: invalidation, full replication,
distributed, or local-only.

It's named for the [parhelion](https://en.wikipedia.org/wiki/Sun_dog), the
optical effect where ice crystals produce extra copies of the sun next to the
real one. A replicated cache, drawn by the atmosphere.

Consistency is best-effort on purpose. Gossip membership and last-write-wins
skip the cost of a consensus protocol for cache data, and anti-entropy repairs
whatever gossip's fire-and-forget delivery drops.

A read comes from the process's own memory in under a microsecond,
hundreds of times faster than a round trip to Redis on the same machine.
[Speed](#speed) has the figures.

The [sundog book](https://ngriaznov.github.io/sundog/) covers guarantees per
mode, deployment on a LAN, a VPC or Kubernetes, sizing, tuning, an operations
runbook, and tested recipes for axum, sqlx and sessions.

## Getting it

```sh
cargo add sundog
```

or in `Cargo.toml`:

```toml
[dependencies]
sundog = "0.6"
```

sundog is async, on [tokio](https://tokio.rs). The examples below assume a tokio
runtime. [Feature flags](#feature-flags) are additive: `cargo add sundog
--features tls,prometheus`.

## Quick start

```rust
use std::time::Duration;

use sundog::{Cluster, Mode};

let cluster = Cluster::builder("demo")
    .build() // mDNS discovery, ephemeral ports, sane defaults
    .await?;

let users = cluster
    .cache::<UserId, Profile>("users")
    .mode(Mode::Replicated) // or Mode::Invalidation, the default, or Mode::Local
    .ttl(Duration::from_secs(600))
    .open()
    .await?; // triggers state transfer if the cache exists cluster-wide

users.insert(id.clone(), Profile).await?; // stamp HLC -> local apply -> fan out
let profile = users.get_or_load(&id, async |id| load_profile(id).await).await?;
users.remove(&id).await?; // tombstone write

// A cache is typed at open, so sessions get one of their own.
let sessions = cluster
    .cache::<Token, Session>("sessions")
    .mode(Mode::Replicated)
    .open()
    .await?;
sessions.insert_with_ttl(token, Session, Duration::from_secs(30)).await?; // this entry's own TTL

let mut events = users.events();
while let Ok(ev) = events.recv().await {
    // handle Event::{Created, Updated, Removed}, each tagged with its Origin
}

cluster.shutdown().await; // graceful leave
```

That's the whole API surface for the common case.
`Cluster::builder(name).build()` with nothing else chained works on a LAN. The
doctest in `sundog/src/lib.rs` compiles it on every CI run. For
bulk fills, `users.insert_many(entries).await?` gives every entry its own HLC
stamp and event under one lock acquisition instead of one per entry. The rest of
the surface:

- `contains_key`.
- `keys`, a local snapshot.
- `for_each_key`, the same scan as a visitor that never holds it all in
  memory at once.
- `get_or_insert_with`, an infallible `get_or_load`.
- `remove_many`.
- `clear`, which tombstones and fans out every key this node holds. In
  `Replicated` mode, that empties the whole cluster once the tombstones land.

`get_sync`, `contains_key_sync`, `insert_sync`, and `remove_sync` are the same
operations without an async runtime. `users.close().await` stops its
background tasks and frees the name for a fresh `open()`. A clone kept past
`close()` keeps working as a local, detached cache.

## Should you use this?

sundog is a cache, not a database. If you need durability or strong consistency,
this isn't that tool.

Writes are last-write-wins on a hybrid logical clock. If two nodes write the
same key at nearly the same time, one write silently loses: no conflict error,
no merge, the loser vanishes.

A merge resolver changes that for values that combine. A `ConflictResolver`
can implement `merge`, folding the stored and incoming records into a third
value, and every node converges on the same result whatever order the writes
arrive in. `sundog::crdt` ships `PnCounter` and `OrSet` with their resolvers.
`Cache::merge` writes through one without a read, and
`CacheBuilder::merge_coalesce_window` batches a writer's merges to one
applied record per key per window. A background sweep keeps their metadata
from growing forever under writer churn: a writer gone (crashed or shut
down, or superseded by a restart) longer than `ClusterConfig::crdt_retire_after` (default:
`tombstone_max_ttl`, 24h) gets retired into bounded per-writer state, and
folded away once that retirement itself has aged past a second
`crdt_retire_after` with every peer quiet, leaving a per-writer receipt
behind so a merge can tell a writer some other replica already folded from
one it just hasn't retired yet. `ConflictResolver::settle` prunes that
receipt on every merge apply once it outlives its own lifetime, so a
receipt one replica has already dropped can't ride back in from a peer
that hasn't caught up. A replica isolated for longer than that receipt
lifetime, still holding a live slot for a writer every other replica has
already folded away, double-counts that writer's contribution once it
reconnects (an `OrSet` writer's already-removed elements resurrect the
same way). That is the same trust boundary `tombstone_max_ttl` already
accepts for a member gone that long, applied here to one writer's
contribution rather than a whole entry.

Deletes and expiries differ. A TTL-expired entry never returns. Every record
carries its own absolute `expires_at_ms`, and once a key is past it no peer
accepts a stale copy back, partition or not. `.ttl(..)` sets a cache's default
lifespan. `insert_with_ttl` and `insert_many_with_ttl` override it per entry or
batch, and the override replicates like the default. Reads never touch expiry.

A manually removed key becomes a tombstone that skips the `tombstone_ttl` GC
schedule while any member gossip still remembers is absent. A partitioned node can't
return with pre-delete data and resurrect the key elsewhere. That deferral caps
at `tombstone_max_ttl`, 24 hours unless raised. Past it the tombstone is collected
regardless of who's missing. A member gone longer can resurrect the key on
return, bounded only by its stale copy's own `expires_at_ms`, and 24+ hours
already outlives most cache TTLs. Set a TTL and deletes stay deleted. Without
one, raise `tombstone_max_ttl` or treat sundog as the wrong layer.

Good fits: read-through caching in front of a slower store, session or profile
data that's fine being eventually consistent, or any per-instance cache whose
instances agree without standing up Redis. It targets small clusters of 2-30
nodes on a LAN, with no consistent-hashing or partitioning. Every replicated
node holds every entry.

The store is an in-crate engine with 1,024 lock-striped tables, one per
anti-entropy bucket. A read takes one read lock and one lookup with no
allocation. A write takes one guard with nothing awaited. Bucket enumeration
time is proportional to the bucket, not the cache. 447 ns for a local read, 1.0
µs for a replicated write, on one machine.

A burst of writes, `insert_many` or back-to-back `insert` calls, fans out as
coalesced `Replicate` batches. Replication throughput scales with the burst, not
per-message overhead. Frames encode and decode without copying key/value bytes.
Writes to different keys apply concurrently. Same-key writes still serialize.
Anti-entropy and state-transfer requests reuse pooled connections instead of
dialing fresh every round.

Each bucket also keeps 64 part digests, the next 6 hash bits below the bucket's
own 10. A round exchanges the 1,024 bucket digests first. A mismatched bucket
answers with a full `(key, version)` listing, or, past `ae_sketch_min_bucket`
entries, an IBLT sketch decoding up to ~100 differing elements, or, past the
larger `ae_part_min_bucket`, its 64 part digests instead of either, without ever
building the listing. A mismatched part then follows the same
listing-or-sketch rule at part scale. That third tier is what keeps repairing
one changed key in a 100M-entry cache cheap: a bucket-level listing there costs
megabytes, a part digest exchange costs a few hundred bytes.

## Speed

sundog answers a read from the calling process's memory, where a cache
server answers across a socket. On one machine, a `Local` or `Replicated`
read is more than 500 times faster than a Redis, Valkey or Dragonfly read, a
write more than 100 times faster, and throughput more than 50 times higher.

The figures below come from one run on a 4-core GitHub Actions runner:

- **Data**: 100,000 keys of 14 bytes, each with its own 100-byte value.
- **Mix**: 90% reads, keys drawn from a zipf distribution with exponent
  0.99.
- **Load**: 16 concurrent workers, 200,000 measured operations after
  20,000 warm-up ones.

| Target | Read p50 | Read p99 | Write p50 | Write p99 | Throughput |
|---|---:|---:|---:|---:|---:|
| sundog, `Local` | 0.25 µs | 0.80 µs | 1.02 µs | 1.76 µs | 6.52M ops/s |
| sundog, `Replicated`, 3 nodes | 0.26 µs | 0.93 µs | 1.18 µs | 2.48 µs | 5.13M ops/s |
| sundog, `Distributed`, 3 nodes | 0.58 µs | 257 µs | 1.23 µs | 2.38 µs | 415K ops/s |
| Redis 8 | 143 µs | 527 µs | 144 µs | 539 µs | 96.5K ops/s |
| Valkey 8 | 137 µs | 494 µs | 136 µs | 502 µs | 100K ops/s |
| Dragonfly | 223 µs | 596 µs | 225 µs | 591 µs | 65.3K ops/s |
| Olric | 208 µs | 717 µs | 209 µs | 744 µs | 67.6K ops/s |
| Hazelcast 5.5 | 337 µs | 876 µs | 340 µs | 901 µs | 43.4K ops/s |

The comparison is an embedded cache against a networked one, measured the
way a service sees each:

- **sundog**: the benchmark calls it in-process through one node's
  `Cache` handle. In a 3-node cluster, all three nodes run in the
  benchmark's process and talk over loopback.
- **The servers**: each runs in a container, reached over loopback TCP on
  the container's published port. That round trip is most of their
  latency. Across a real network it grows, while a sundog read stays in
  memory.
- **Replicated writes**: a `Replicated` write returns once the local copy
  is applied and the write is queued for its peers, so its latency leaves
  out the network.
- **`Distributed` reads**: a node owns about two thirds of the keys (two
  owners on three nodes). It reads those from memory, and each of the rest
  costs one round trip to an owner. Those remote reads set the 257 µs p99.

The servers hold each entry in less memory: Redis and Valkey used 166 to 168
bytes per entry, Dragonfly 131, and sundog 205 to 209 bytes per copy.
[Memory per entry](#memory-per-entry) breaks down sundog's layout.

## The four modes

| Mode | Each node stores | On write | On read | Pick this when |
|---|---|---|---|---|
| `Local` | its own data, nothing shared | nothing sent | local only | you want a fast in-process cache with TTL and bounded size, and no cluster traffic at all |
| `Invalidation` (default) | its own working set | broadcasts "this key changed" | local, may be momentarily stale | the dataset is big or expensive to hold everywhere, and each node mostly cares about its own hot keys |
| `Replicated` | a full copy of everything | broadcasts the value | always local, never waits on the network | the dataset is small enough to duplicate, and you want reads to never touch the network |
| `Distributed` | the parts it owns, `k` live owners per key (2 by default, via `Mode::distributed()`) | forwarded to the key's owners, applied only there | `get` is local-only; `fetch` asks an owner | the dataset is too big to hold on every node, but must survive a node loss |

`Invalidation` never sends values between nodes: a write on A tells B "your copy
of this key is stale," and B drops it or reloads it on next access. `Replicated`
alone runs state transfer on join, a new node pulling a full snapshot from an
existing peer that has finished its own, then reconciling with every other
peer once. It also keeps a background anti-entropy loop running while the
cache is open.

`Distributed` splits a cache into 65,536 parts (`xxh3(key) & 0xFFFF`: the
1,024 anti-entropy buckets, 64 parts each), each assigned to its `k` live
owners by rendezvous hashing over the peers that advertise the same cache
under the same mode and owner count. At 100 nodes and two owners the busiest
node holds about 8% more than an even share and the lightest about 11% less,
and a join moves only the parts the joiner takes. The view recomputes from gossip
membership, so ownership converges a few gossip intervals after a join or
leave, not instantly. A node that gains a part pulls it from the previous
owners. One that loses a part keeps serving it for
`distributed_disown_grace_rounds` anti-entropy intervals, then hands it to
each new owner in one anti-entropy round and drops it only once every owner
has answered. Until a gained part's pull lands, a `fetch` that misses it
locally asks the other owners first. A write for a part this node doesn't
own is forwarded to that part's owners and never applied locally, so nothing external needs to route it, though a local `get`
right after a forwarded write still misses, since only the owners hold it.
Every forwarded batch carries the writer's view hash. An owner whose own
view differs passes the batch on once more to the owners it knows, so a
write routed under a view that has since changed still lands on every
current owner.
`get` stays local-only everywhere, returning `None` off a non-owner. `fetch`
is the network-aware read, trying live owners in rendezvous order and
returning `Ok(None)` for a genuine miss or `CacheError::FetchUnavailable` once
every owner has timed out inside `fetch_timeout`, or while the part is still
cold on every owner that answered. `owners_of` reports a key's
current owners in that same order. `owners` must be 2 or more
(`CacheError::TooFewOwners` otherwise), a finite `max_capacity` needs a
`spill` tier the same way `Replicated` does, `tti` is rejected outright, and
two peers disagreeing on `owners` for the same cache name hit
`CacheError::ModeMismatch` like any other mode conflict.

```rust
let prices = cluster
    .cache::<Sku, Price>("prices")
    .mode(Mode::distributed()) // k = 2 owners per part
    .open()
    .await?;

prices.insert(sku.clone(), Price(999)).await?; // forwarded if this node isn't an owner
match prices.fetch(&sku).await? {
    Some(price) => { /* found: local, or read from an owner */ }
    None => { /* a genuine miss */ }
}
```

Every node gossips the mode of each cache it has open. Opening a name under a
mode that conflicts with a live peer fails with `CacheError::ModeMismatch`. TTL
and capacity are local knobs, free to differ.

## Rolling upgrades

Every node states its wire protocol version, `sundog::wire::PROTOCOL_VERSION`,
in the hello that opens each connection and in its gossip state. A node
answers a peer only with what that peer's version understands: an older peer
never receives a message kind its release cannot decode, and a newer peer
limits itself the same way. One release step interoperates, so a cluster
upgrades one node at a time with replication and repair running throughout.
The current release speaks protocol 5 and serves protocol 4, the release
before it. A container test runs the previous release's node against the
current one in both roles. Distribution mode's message kinds (`Fetch`,
`FetchReply`, `FetchDeclined`, `AeDigestScoped`, `StBuckets`,
`StBucketChunk`, `ForwardBatch`, and `StaleView`) are gated on protocol 3: a distributed cache forms only among protocol-3
peers advertising it, and a protocol-2 peer mid-rollout is never eligible to
own a part and never receives one of these messages at all.

Protocol 5 ranks each of a distributed cache's 65,536 parts on its own and
adds `AeDigestMasked`: anti-entropy under part ranking sends one digest per
bucket, folded over the parts both nodes own. A protocol-4 node ranks whole
buckets, so while any eligible node speaks protocol 4 every node keeps
ranking whole buckets. When the last protocol-4 node leaves, every node
switches to part ranking within a few gossip intervals. Most parts change owners at that moment: the switch runs as one
large rebalance, with every lost part served through its disown grace and
handed to its new owners before it is dropped. Upgrade outside peak load.

## How nodes find each other

| Mechanism | Default? | What it does | Use it for |
|---|---|---|---|
| `Mdns` | yes | registers `_sundog._udp.local.` and browses for it continuously, via `mdns-sd` | a real LAN, office network, or anywhere multicast works |
| `Static` | no, but wins over `Mdns` if either `.seeds(..)` is called or `SUNDOG_SEEDS=host:port,host:port` is set with no other discovery configured | a fixed seed list, re-resolved periodically | tests, and anywhere mDNS can't reach |
| `DnsSrv` | no, `.discovery(DnsSrv::new(..))` | polls SRV records for a service name, falls back to A/AAAA | Kubernetes: point it at a headless service and you're done |

Discovery keeps running after startup: if the whole cluster reboots at once and
nobody remembers anybody, continuous mDNS browsing lets it find itself again. A
node with no peers isn't broken. A single-node "cluster" is a normal, healthy
state.

**The Docker gotcha:** mDNS doesn't cross the default Docker bridge network,
and on most Wi-Fi networks AP isolation blocks it too, since multicast
doesn't route there. If
you're demoing this with `docker compose`, use `Static` seeds. Save `Mdns` for
host networking or bare-metal LANs.

**Behind NAT or a container port mapping**, the interface address a node finds
on its own (via its outbound-interface probe, or the `if-addrs` fallback
behind it) is not always the address peers must dial: a cloud instance's
public IP while the process binds its private one, or a container's
externally published port. Set `ClusterConfig::advertise_ip` to the address
peers should use. It covers both the gossip and data-plane addresses, and no
probe runs. Under Kubernetes host networking, or any setup where the bind
address is already correct, leave it unset.

**In a cloud VPC**, AWS, GCP or Azure, unicast routes and multicast does
not, so `Mdns` finds nobody. Two settings make a VPC work. First, discovery:
`Static` seeds at a few stable private addresses, via `.seeds(..)` or
`SUNDOG_SEEDS`, or `DnsSrv` against a name in a private zone. Seeds only
bootstrap; every node learns the rest through gossip. Second, fixed ports:
both bind addresses default to port `0`, a free port picked at startup, and a
security group cannot allow a random port. Set the gossip port (UDP) and the
data-plane port (TCP) and open both within the cluster's security group, a
self-referencing rule, plus the Prometheus port if you use one:

```rust
use sundog::{Cluster, ClusterConfig};

let config = ClusterConfig::default().with(|c| {
    c.gossip_bind_addr = "0.0.0.0:7946".parse().expect("a socket address");
    c.data_bind_addr = "0.0.0.0:7947".parse().expect("a socket address");
});
let cluster = Cluster::builder("prod")
    .config(config)
    .seeds(["10.0.1.10:7946".parse()?, "10.0.2.10:7946".parse()?])
    .build()
    .await?;
```

Seeds name the gossip port only; peers learn the data-plane port from gossip.
The advertised address needs nothing: the outbound-interface probe finds the
instance's private IP, which is the address peers dial. Mutual TLS is
optional and carries a fixed name in every certificate, so no per-node IP
SANs and no reissue when an instance's address changes. Several availability
zones behave as one LAN with a few milliseconds more latency. A peered VPC in
another region routes too, but the failure detector is tuned for
sub-5-second detection on a LAN: at tens of milliseconds of RTT raise
`gossip_interval`, `phi_threshold` and `fetch_timeout` before trusting it.

**On Kubernetes**, sundog runs inside your service's pod, and the same two
settings apply. Discovery is `DnsSrv` against a headless Service that
selects the same pods, with the gossip port as its fallback so plain A
records are enough. Ports are the same fixed pair, declared as container
ports next to the service's own:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: myservice-gossip
spec:
  clusterIP: None
  selector:
    app: myservice
  ports:
    - name: gossip
      port: 7946
      protocol: UDP
```

```rust
use sundog::discovery::dns::DnsSrv;

let cluster = Cluster::builder("prod")
    .config(config) // the fixed ports above
    .discovery(DnsSrv::new("myservice-gossip.my-ns.svc.cluster.local.", 7946))
    .build()
    .await?;
```

The pod IP is what the probe advertises, so nothing more to set. Wire the
readiness probe to `/readyz` if you enable `prometheus_listen`, or fold
`cluster.is_ready()` into the probe your service already serves, and call
`cluster.shutdown()` from its SIGTERM handler so peers see a departure
instead of a failure.

## Feature flags

| Flag | Default | What it adds |
|---|---|---|
| `tls` | off | mutual TLS on the data-plane mesh (`rustls`); set `ClusterConfig::tls` / `ClusterBuilder::tls` and every connection, including state-transfer and anti-entropy, gets wrapped |
| `prometheus` | off | a Prometheus exporter; `ClusterBuilder::prometheus_listen` serves `GET /metrics` directly, or grab a recorder via `telemetry::prometheus_handle` and mount it in your own server |
| `sim` | off | swaps the data-plane transport for `turmoil`'s, so the net layer can run inside a deterministic simulation; test-only, never enable it in a real deployment |
| `fuzzing` | off | exposes the reference model the apply-path fuzz targets drive against a real shard (`sundog::store::model`); changes no behavior |
| `spill` | off | a local SSD/NVMe spill tier; `CacheBuilder::spill(SpillConfig::new(dir, capacity_bytes))` lets eviction demote cold entries to disk instead of discarding them |

With `spill` in use, eviction writes cold entries to a FIFO ring of region
files on local disk instead of discarding them, so a cache's effective size
extends past its RAM budget. A later read promotes a spilled entry back into
RAM.

- `capacity_bytes`, the disk budget bounding the tier.
- `region_bytes`, the size of each region file in the ring (64 MiB default).
- `read_concurrency`, how many spilled-value reads run at once (16 default).
- `flush_queue_bytes`, the cap on how many queued-but-unwritten bytes a
  lagging flusher may hold in RAM before eviction falls back to an ordinary
  delete instead (one region's worth unless raised), which keeps a slow disk a
  plain-eviction problem rather than an unbounded-RSS one.

A `Replicated` cache keeps a refused victim resident, at its full weight, for
a later eviction pass to retry instead, since every peer still holds the
entry and a local delete would only have anti-entropy repair it back in. A
`Local` or `Invalidation` cache evicts it as described above.

With `SpillConfig::warm_reopen(true)` (default `false`, so a default tier's
open and close cost stay what they are without this setting), a
clean close checkpoints the tier: every currently-resident live record is
written to disk alongside every already-spilled one, and a snapshot next
to the region files lists every live entry's key, version, expiry, and
on-disk location. Tombstones and expired entries are never written into
it. A restart against the same directory then replays only that snapshot,
never scanning a region file for records it does not already know to look
for, filters what it recovers to buckets this node currently owns, drops
anything already expired, and folds every survivor straight into the
shard's digests with no value bytes read into RAM. For `Mode::Distributed`
on a cluster built with seeds, `CacheBuilder::open` waits briefly (bounded
by `min(ClusterConfig::state_transfer_budget, 5s)`) for a first known peer
before computing that owned-buckets filter at all, so a restart racing
gossip convergence never treats this node as the sole owner of every
bucket just because no peer has reported in yet. A crash, or a close
with `warm_reopen` off, leaves no snapshot, so the next open is cold; a
successful warm reopen deletes the snapshot it just replayed, so a second
open with no intervening clean close is cold too. A node down longer than
`tombstone_ttl` (10 minutes by default) also always falls back to the
ordinary cold, wipe-and-recreate open, since no live peer is still
guaranteed to hold the tombstones that would out-vote a resurrected stale
record. Either way, a warm-reloaded bucket starts both cold and
unverified: unlike an ordinary cold bucket, whose only possible content
before a live pull lands is already trustworthy (pulled from a donor or
replicated in live), a replayed bucket can hold a record a co-owner
deleted during this node's downtime, so `Cache::fetch` and a peer's
request for it both treat a local hit there the same as a miss, asking
the other owners instead of answering short. A `Mode::Distributed` cache
clears the unverified mark on the same decision, and at the same call,
that clears cold. Verification is preferred whenever a live co-owner
exists to check against: an eager anti-entropy round against every live
co-owner confirming the replayed data, or the ordinary cold-pull machinery
landing fresh data for the bucket, clears both marks together. When the
code instead decides to serve a bucket with no donor to verify against --
a bucket found to have no co-owner at all, or one whose only co-owners
never answer before the warm-up's attempts run out -- the replayed data,
already bounded by the `tombstone_ttl` downtime gate above, is the best
available answer, and it clears both marks too rather than refusing local
hits forever while already trusting local misses in the same bucket. A
bucket this node does not own any more is never served short regardless.

sundog emits these metrics regardless of features:
`sundog_cache_hits_total{cache}`, `sundog_cache_misses_total{cache}`,
`sundog_cache_entries{cache}`, `sundog_backlog_dropped_total{peer}`, frames
dropped only once a peer is gone from the mesh -- a peer that is merely slow
is never dropped for; `sundog_fan_out_wait_seconds_total{peer}`, whole
seconds spent instead waiting out such a live peer's full outbox,
`sundog_fan_out_wait_timeouts_total{cache}`, an async write (`insert`,
`insert_many`, `remove`, `merge`, and their siblings) whose own wait for
fan-out backlog room under `ClusterConfig::fan_out_backlog_capacity` ran out
its `ClusterConfig::fan_out_wait_timeout` and proceeded over capacity anyway
rather than ever dropping the write, and `sundog_fan_out_backlog{cache}`, a
cache's current not-yet-fanned-out backlog length,
`sundog_clock_skew_rejected_total{cache}`, records refused for a stamp
further ahead of this node's clock than `ClusterConfig::max_clock_skew`,
`sundog_live_peers`, `sundog_open_caches`, `sundog_ae_sketch_total{cache,
outcome}`, and `sundog_ae_parts_total{cache, outcome}`. The first of that pair
tags anti-entropy's IBLT-sketch reconciliation on large buckets, where
`outcome` is `decoded` or `fallback`. The second tags the part-digest path's
per-part reconciliation, where `outcome` is `listing`, `sketch`, or
`fallback`. A cache whose resolver merges (`sundog::crdt`'s `PnCounter` and
`OrSet`) also emits `sundog_crdt_retired_writers_total{cache}`, writers the
CRDT compaction sweep found eligible for retirement, and
`sundog_crdt_compactions_total{cache}`, records it rewrote in
their compacted form. The first can exceed the second: a writer counts as
retired the moment the sweep's scan judges it eligible, even for a record
the pass skips without rewriting (an unowned bucket, or one that changed
underneath the scan). Without
`prometheus` they fall into the `metrics` crate's no-op default recorder.
Install the recorder before opening a cache: a cache binds its per-cache
handles when it opens. A ready-made Grafana dashboard lives at
[`ops/grafana-dashboard.json`](ops/grafana-dashboard.json).

With `spill`, every cache open also emits
`sundog_spill_reopen_total{cache, outcome, reason}`, `outcome` `warm` for a
fast reopen straight from a checkpoint snapshot or `cold_fallback` for the
ordinary wipe-and-recreate path, with `reason` naming why for a fallback
(`disabled` when `SpillConfig::warm_reopen` is off, `no_snapshot`,
`stale_snapshot`, `config_mismatch`, `downtime_exceeded`, or `bad_region`;
empty for `warm`), and `sundog_spill_reopen_records_total{cache}`, how many
records a warm reopen replayed.

A `Mode::Distributed` cache adds eight more:

- `sundog_owned_parts{cache}`, this node's current part count.
- `sundog_owned_buckets{cache}`, the same share in buckets: owned parts over
  64, fractional once parts are ranked on their own.
- `sundog_rebalance_parts_total{cache, direction}`, parts rebalance pulled
  `in`, released `out`, or `served` to another node's pull, credited per
  part the moment its own pull lands or its own release fires, not batched
  behind the rest of a multi-part transfer, and per stream on the donor once
  the stream runs to its end.
- `sundog_rebalance_pull_timeouts_total{cache}`, warm-ups that gave up on a
  part pull timing out repeatedly and opened warm with whatever landed,
  leaving the rest to anti-entropy.
- `sundog_fetch_total{cache, outcome}`, each `Cache::fetch` call's outcome
  (`local`, `remote`, `miss`, or `error`).
- `sundog_forwarded_writes_total{cache}`, writes this node forwarded to a
  part's owners instead of applying, or passed on because they arrived
  under another node's view.
- `sundog_stale_view_total{cache}`, anti-entropy rounds a peer declined over
  a mismatched ownership view.
- `sundog_unowned_inbound_dropped_total{cache}`, inbound records dropped for
  a part this node neither owns nor is mid disown-grace on.

`Cluster::is_ready()` and `Cluster::health()` report whether every open
`Mode::Replicated` cache has finished its state transfer. A `Local` or
`Invalidation` cache is warm from the moment it opens, so it never holds
readiness back. With the `prometheus` feature, the same listener that serves
`GET /metrics` also serves `GET /readyz` (200 once ready, 503 otherwise) and
`GET /healthz` (200 for as long as the process serves), for a container
orchestrator's readiness and liveness probes.

## Testing

Six layers, cheapest and highest-signal first:

1. **Property tests** run via `proptest` in `hlc`, `wire`, and `store` under
   `sundog/src`. The one that matters most, `store`'s permutation-convergence
   property, applies a random batch of writes and removes in every sampled
   order, with drops and duplicates. Every run lands on the same final state,
   the property this loss-tolerant design rests on.
2. **Deterministic simulation** runs via `turmoil` in `sundog/tests/sim.rs`,
   behind the `sim` feature. It drives the real net layer and store against a
   scripted membership feed with no sockets involved. Scenarios:
   - Partition under load, heal, and check convergence inside a bounded
     number of rounds.
   - Message loss, reordering, and duplication.
   - A donor dying mid-state-transfer.
   - A forced low `ae_sketch_min_bucket` driving the IBLT sketch path
     itself, and a forced low `ae_part_min_bucket` driving the part-digest
     path, both under the same loss and reordering.
   - A `Mode::Distributed` cluster churning membership under loss and
     reordering, checking that every bucket's data converges across its
     current owners with no non-owner ever holding one.
3. **Container integration** runs via
   [`rightsize`](https://crates.io/crates/rightsize) in
   `sundog/tests/containers.rs`, no Docker CLI, no `bollard`. Multi-node
   scenarios run as separate processes on a real virtual network. They cover
   three-node convergence, tombstones reaching every node, and cold joins up to
   a million entries. They also cover a killed node's gap repaired by
   anti-entropy, a dropped key repaired the same way at 500k-entry sketch
   scale, the same repair again at 1M-entry part-digest scale with its wire
   cost pinned via `netstats` under a lowered `ae_part_min_bucket`, a bulk
   fill's wire cost pinned via `netstats` against the fan-out queue
   duplicating it, high-churn add/remove/TTL workloads draining to zero, and
   64 KiB values verified byte-for-byte, and, for `Mode::Distributed`, a
   five-node fill landing every key on two owners, one owner crashing
   with every key still fetchable and then re-owned, and a fourth node
   joining a filled cluster and taking its share. Each node is
   `sundog-testnode`, a tiny
   static/musl binary driven over a line-based control protocol, and reads
   `SUNDOG_TESTNODE_MODE=distributed` (with `SUNDOG_TESTNODE_OWNERS` to pick
   `owners`, default 2) to open `"it"` as a distributed cache instead of
   `Mode::Replicated`. It sits behind an env var, so plain `cargo test
   --workspace` still compiles without a container backend:

   ```sh
   SUNDOG_CONTAINER_TESTS=1 cargo test --release -p sundog --test containers -- --test-threads=1
   ```

   Set `RIGHTSIZE_BACKEND=docker`: sundog's gossip is UDP, and only
   rightsize's Docker backend carries it, not its lighter TCP-only microVM
   emulation. CI pulls a real base image over KVM and Docker. Locally, point
   `SUNDOG_TEST_BASE_IMAGE` at a pre-seeded image if registry pulls aren't
   available.

   - **Chaos lane**: `chaos_crashes_churn_and_drops_still_converge` runs a
     seeded random mix of crashes, churn, dropped keys, refills, and put
     bursts against a four-node cluster for `SUNDOG_CHAOS_SECS` seconds, then
     checks that every node converges to the same content.
     `chaos_distributed_crashes_churn_and_drops_still_converge` runs the same
     mix against a `Mode::Distributed` cluster instead, checking that every
     bucket converges across its current owners alone. `SUNDOG_CHAOS_SEED`
     pins a run for replay; `weekly-chaos.yml` runs it for ten minutes with a
     fresh seed, logged so a red run replays with
     `SUNDOG_CONTAINER_TESTS=1 SUNDOG_CHAOS_SEED=<seed> SUNDOG_CHAOS_SECS=<secs>
     RIGHTSIZE_BACKEND=docker cargo test --release -p sundog --test containers
     -- --test-threads=1 chaos_`.

   Scenarios needing only one node, or two on loopback with real UDP membership,
   run as ordinary `#[cfg(test)]` unit tests beside the code they exercise:
   `sundog::store`'s stampede-collapse and TTL tests, `sundog::cluster`'s
   two-node replication, invalidation, state-transfer, anti-entropy, and
   local-mode tests.
4. **Coverage-guided fuzzing** runs via `sundog/fuzz`, a `cargo-fuzz` crate
   outside the workspace, weekly via `.github/workflows/weekly-fuzz.yml`.
   `decode_never_panics` and `decode_encode_roundtrip` throw arbitrary bytes at
   the wire decoder: it must never panic, and any frame it accepts must
   re-encode to a fixed point. `apply_model` and `apply_permutation` cover
   everything after a successful decode. `apply_model` replays generated writes,
   remote applies, invalidations, tombstone GC, and clock advances against a
   real `Shard` and its reference model, the same property
   `shard_matches_the_reference_model_under_arbitrary_op_sequences` checks
   in-crate under libFuzzer instead of proptest. `apply_permutation` runs the
   same permutation-convergence property: a duplicated, twice-shuffled record
   set must converge two shards to identical digests and entry sets.
5. **Bounded model checking** runs `#[kani::proof]` harnesses under each
   module's `kani_proofs` with [Kani](https://github.com/model-checking/kani),
   which exhausts every input of a pure function where proptest samples and
   proves the absence of panics and overflow on the way: expiry packing and
   the touch stamp, hash-to-bucket indexing, the compaction shrink rule, the
   reconciliation retry bounds, the gossip bind retry, the anti-entropy skip
   rule, the hybrid logical clock, frame length arithmetic and
   spill sizing. `.github/workflows/weekly-kani.yml` runs them weekly;
   locally, `cargo install --locked kani-verifier && cargo kani setup &&
   cargo kani -p sundog --features spill`.
6. **Chaos demo** runs `sundog-demo` in headless mode, described in the Chaos demo section.

Four benchmark suites sit outside these six layers, each gated on
`SUNDOG_BENCH=1` so a plain `cargo test` never pays their wall-clock cost:
`sundog/tests/entry_diet_bench.rs` (memory per entry and local read
latency, covered in Memory per entry below), `sundog/tests/replication_bench.rs` (bulk write and read latency across a live
cluster), `sundog/tests/spill_bench.rs` (the optional SSD spill tier's
write path, RAM-hit versus tier-hit read latency, concurrent tier reads,
region reclaim, and its hit-ratio case against plain eviction), and
`sundog/tests/crdt_bench.rs` (merge resolvers against last-write-wins:
concurrent counters, per-apply cost, cold join, large-entity convergence,
and the sketch path, with `SUNDOG_BENCH_KEYS` scaling the entity count). Run
the spill suite with:

```sh
SUNDOG_BENCH=1 cargo test --release -p sundog --features spill,prometheus \
    --test spill_bench -- --test-threads=1 --nocapture
```

Plain `cargo test --workspace` runs everything except the `sim` and container
suites, which need their feature/env var explicitly:

```sh
cargo test --workspace
cargo test -p sundog --features sim --test 'sim*'
SUNDOG_CONTAINER_TESTS=1 RIGHTSIZE_BACKEND=docker \
    cargo test --release -p sundog --test containers -- --test-threads=1
```

`cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic` and
`cargo fmt --all --check` are both enforced in CI, and every CI lane builds with
`RUSTFLAGS="-D warnings"` and `RUSTDOCFLAGS="-D warnings"`: a compiler or
rustdoc warning fails the build.

`.github/workflows/scale.yml` runs weekly and on demand: it builds
`sundog-distributed-demo` with `--features spill,prometheus` and runs it
headless at 4M keys across three nodes with an 800k-entry RAM cap per node
over a spill tier, checking the resulting `--report-json` output against
[`ops/scale-gate.json`](ops/scale-gate.json)'s thresholds for steady and
peak RSS, deferred spill drops, pull timeouts, dropped replicate backlog
(`max_backlog_dropped_other_peers`, the `sundog_backlog_dropped_total`
frames dropped toward any peer other than the node the run kills, whose
departure drops the frames queued for it by design; `max_backlog_dropped`
bounds the total across every peer instead), fetch p99 latency, warm spill
reopens, convergence, and a passing sample check via `--gate`.
`workflow_dispatch` reruns the same shape on demand with its own key count,
duration, RAM cap, and runner inputs, for a one-off run at a different
scale. Both the report and the run log upload as workflow artifacts.

`bench`, the `sundog-bench` binary, runs one zipf workload against sundog in
each of its three modes and against Redis, Valkey, Dragonfly, Olric and
Hazelcast, each server in a container started through rightsize. It reports
read and write p50 and p99, throughput, and bytes per entry, and
[`ops/bench-gate.json`](ops/bench-gate.json) holds `sundog-local` and
`sundog-replicated` to a one-millisecond read and write p99.
`.github/workflows/weekly-bench.yml` runs it weekly and posts the report on
the run's summary page. The book's Benchmarks page covers the method:

```sh
RIGHTSIZE_BACKEND=docker cargo run --release -p sundog-bench -- \
    --report-md bench-report.md --gate ops/bench-gate.json
```

## Chaos demo

`demos/sundog-demo` spins up N in-process nodes on loopback and runs a background
write load against a shared key space. It lets you kill and restart nodes
interactively, watching replication and anti-entropy repair the damage in a
`ratatui` TUI:

```sh
cargo run -p sundog-demo -- --nodes 5
```

Flags: `--cluster <NAME>`, `--key-space <N>`, `--write-interval-ms <N>`,
`--gossip-base-port <PORT>`; `--help` lists everything. In the TUI: arrow keys
or `j`/`k` to move, `1`-`9`/Enter to pick a node, `K` to kill it, `R` to restart
it, `P` to pause the write load, `q` to quit.

`--headless <SECS>` runs the same thing with no terminal, for a fixed duration,
then prints a convergence report and exits nonzero if anything diverged. That's
the soak-test rig: run it for 24h and memory stays flat. It doubles as a
CI-friendly smoke check.

## Distributed demo

`demos/sundog-distributed-demo` is the same shape of demo built around
`Mode::Distributed` instead: it preloads a large key set across N in-process
nodes, then runs a steady write load and random `fetch` sampling against it
while you kill and restart nodes and watch buckets move:

```sh
cargo run --release -p sundog-distributed-demo -- --nodes 5 --keys 2000000
```

It preloads `--keys` keys (`k{i}` = `v{i}`, two million unless overridden) in
batches spread round-robin across the live nodes via `Cache::insert_many`
before the write load starts, printing preload throughput and this
process's RSS when the preload finishes. Flags: `--owners <N>` (live owners per bucket,
default 2), `--cluster <NAME>`, `--write-interval-ms <N>`,
`--gossip-base-port <PORT>`, `--value-bytes <N>` to pad every value,
`--max-entries <N>` with `--spill-dir <PATH>` for a per-node RAM cap over a
spill tier (built with `--features spill`), and `--metrics` for an
in-process `sundog_*` status line every interval during a headless run
(built with `--features prometheus`); `--help` lists everything. The TUI shows a
progress bar during preload, then per node: entry count, an estimated
owned-bucket share, warmth, and restarts, plus cluster-wide fetch hit/miss/
error counts and latency. Same keys as the chaos demo: arrow keys or `j`/`k`
to move, `1`-`9`/Enter to pick a node, `K` to kill it, `R` to restart it, `P`
to pause the load, `q` to quit.

Watch entries per node settle around `owners / N` of the key count; kill a
node and watch the survivors' owned-bucket counts and entry counts climb as
they pull its buckets; restart it and watch it take its share back.

`--headless <SECS>` preloads, runs the load for `SECS` seconds (killing one
node at the midpoint and restarting it after a downtime of `min(SECS / 4,
tombstone_ttl / 2)`, so the node comes back after half the tombstone TTL at
most and a spill run exercises the warm reopen instead of always falling
back cold, to exercise a real rebalance), then pauses it, polls the sum of
live nodes' entry counts against `owners * surviving keys` under a bound
wide enough for
`distributed_disown_grace_rounds` to run out, verifies a random sample of
surviving keys against their expected value, and prints one report line
each for the preload, the fetch counters, the sample check, and
convergence, exiting nonzero on either a divergence or a failed sample.
`--report-json <PATH>` writes that same run as a JSON summary (RSS, fetch
latency, the sample check, convergence, and the summed `sundog_*` totals)
instead of only printing it, and `--gate <PATH>` reads a JSON threshold
file of the same shape and checks the run against it, exiting nonzero and
listing every violated threshold; both need `--metrics`, and the Scale
workflow described in Testing runs with both set.

Both the demo and the test node set jemalloc as their global allocator on
every target but MSVC Windows. The library itself sets none, so a service
embedding it chooses: on a 4M-key three-node run the demo settles at 1.7
GiB under jemalloc against 3.2 GiB under glibc's default arenas, which
keep the preload's and anti-entropy's transient buffers resident, and
bulk ingest runs about twice as fast. Entries themselves cost 56 bytes
(80 with `spill`) plus the slab's index slot and, for a key and value
under 23 encoded bytes together, no allocation; see Memory per entry
below for the full accounting against Redis.

## Memory per entry

`Live<K, V>` (`sundog/src/store/engine.rs`) packs a key and value under 23
encoded bytes together, an absolute-millisecond HLC, the full 64-bit
`NodeId` and logical counter a merge needs bit-identical, a packed expiry,
and a packed last-access stamp into 56 bytes, no heap allocation, no
separate typed copy of the key or value. `Stripe::live` is a `Slab`, a
dense arena plus a `u32`-keyed hash index in place of a hash table holding
entries directly, so an idle index slot costs about 5 bytes against the
roughly 73 bytes an idle table slot holding a `Live` directly would cost
at the same load factor. `CacheBuilder::capacity_hint` presizes a shard's
stripes for its own expected local entry count up front, at `open()`,
instead of growing one insert at a time.

Measured with `SUNDOG_BENCH=1 cargo test --release -p sundog --test
entry_diet_bench -- --nocapture` on a 4-core Linux box, glibc, one node,
`Mode::Local`, settled resident set (`VmRSS`) divided by entry count:

| Shape | Entries | sundog, no hint | sundog, hinted | Redis 7 computed | Redis 7 practical |
|---|---:|---:|---:|---:|---:|
| 7-byte key, 8-byte value (`Record::Inline`) | 4,000,000 | 76.3 B/entry | 75.1 B/entry | 80 B/copy | 85-100 B/copy |
| 7-byte key, 8-byte value (`Record::Inline`) | 64,000,000 | 67.3 B/entry | 67.4 B/entry | 80 B/copy | 85-100 B/copy |
| 16-byte key, 100-byte value (`Record::Heap`) | 4,000,000 | 212.2 B/entry | not measured | 184 B/copy | 195-230 B/copy |

The Redis 7 figures for the 7-byte-key/8-byte-value shape are its own `dictEntry` (24
bytes) plus an `sdshdr8` key plus an `embstr`-encoded value sharing one
allocation with its `robj` header plus one bucket-array slot, computed;
for the 16/100-byte shape, past `embstr`'s threshold, the value takes its
own `raw`-encoded allocation instead. `used_memory / DBSIZE` from public
Redis benchmarks gives the practical range for both.

For the 7-byte-key/8-byte-value shape, every one of sundog's four figures sits under 90
bytes per entry and under Redis 7's own practical range, at both
4,000,000 and 64,000,000 entries; the byte cost drops further as the
entry count grows (fixed per-stripe overhead amortizing over more
entries) rather than staying flat or climbing. A stripe's entry array
grows by a quarter when it fills, so it stays at least four fifths full
whatever the entry count; `capacity_hint` saves a little at 4,000,000
entries (75.1 against 76.3 bytes per entry), where about half of 1024
stripes land a few keys over their hinted share and grow once by a
quarter, and hinted and unhinted land within 0.1 bytes of each other at
64,000,000 (67.4 against 67.3). The
16-byte-key/100-byte-value shape takes one heap allocation on both
engines, past sundog's 22-byte `Record::Inline` cap and Redis's `embstr`
threshold alike; sundog's target there is parity with Redis, not another
win, and it measures at 212.2 bytes per entry, inside Redis's 195-230
practical range and a little above its 184 computed figure.

Reproducing sundog's figures:

```sh
SUNDOG_BENCH=1 cargo test --release -p sundog --test entry_diet_bench \
    entry_diet_rss_budget -- --exact entry_diet_rss_budget --test-threads=1 --nocapture
SUNDOG_BENCH=1 cargo test --release -p sundog --test entry_diet_bench \
    entry_diet_rss_budget_64m -- --exact entry_diet_rss_budget_64m --test-threads=1 --nocapture
SUNDOG_BENCH=1 cargo test --release -p sundog --test entry_diet_bench \
    entry_diet_rss_budget_heap_shape -- --exact entry_diet_rss_budget_heap_shape --test-threads=1 --nocapture
```

Reproducing Redis 7's figures at the same widths, 7-byte keys and 8-byte values:

```sh
redis-server --daemonize yes --save '' --appendonly no
seq 1 4000000 | awk '{printf "SET %07d %08d\n", $1, $1}' | redis-cli --pipe
redis-cli info memory | grep used_memory:
redis-cli dbsize
```

And 16-byte keys, 100-byte values (`redis-cli flushall` first):

```sh
seq 1 4000000 | awk '{printf "SET %016d %0100d\n", $1, $1}' | redis-cli --pipe
redis-cli info memory | grep used_memory:
redis-cli dbsize
```

`used_memory` divided by `dbsize` is Redis's own bytes/copy in both
cases. `redis-cli shutdown nosave` when done.

## MSRV

Rust edition 2024, `rust-version = "1.97"`, resolver `3`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT
license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
