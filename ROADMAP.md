# Roadmap

Design sketches for what v1 deliberately does not do. Each cut in
`docs/BUILD_PLAN.md` §12/§14 was made to keep the core buildable and correct
first; this document is where those cuts get an honest paragraph on cost and
trigger, so revisiting one is a decision made on evidence, not on itch.

None of what follows is scheduled. A section moves from here into
`docs/BUILD_PLAN.md` and actual code only once its trigger condition is
observed in a real deployment, not because it would be interesting to build.

## Distribution mode

Infinispan's `DIST` mode: a consistent-hash ring over the live member set,
`numOwners` primary+backup replicas per key instead of every node holding
every entry, and — the hard part — rebalancing the ring and streaming
ownership transfers whenever membership changes, without ever dropping a
write or serving a stale primary during the transition. sundog's `Replicated`
mode sidesteps essentially all of this: every node is trivially "owner" of
everything, so join/leave only ever needs a snapshot pull plus anti-entropy,
never a rebalance. That simplification is *why* state transfer
(`docs/BUILD_PLAN.md` §9) fits in a page of design instead of a subsystem —
Infinispan's own rebalancing coordinator, conflict resolution during
rebalance, and partial-rebalance-on-partition logic are, by their own
documentation, the hardest 20% of the whole product.

**Cost:** a consistent-hash ring, an owner-set computation that stays stable
under single-node churn, a rebalance protocol that can be interrupted by a
second membership change mid-flight, and a decision about what a read does
while its key's ownership is in motion. Every one of those interacts with
the anti-entropy and state-transfer machinery already built, not in addition
to it.

**Trigger:** replicated memory footprint (every node holding every entry)
actually becomes the binding constraint on cluster size or value volume —
not "it would be more efficient," but a deployment that has hit the ceiling
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
the `tls` feature flag already declared in `Cargo.toml` but not yet wired
into the TCP path — and connection-migration semantics that don't currently
matter to a same-LAN deployment.

**Trigger:** head-of-line blocking between state-transfer/anti-entropy
traffic and live broadcast traffic shows up as measured tail latency in
practice, not as a theoretical concern. The per-class outbox split
(`docs/BUILD_PLAN.md` §6) and request/response traffic living outside the
broadcast channel already remove the worst of this at the application layer;
QUIC would only matter for what's left after that.

## Set reconciliation past ~10⁶ entries (IBLT / minisketch)

Anti-entropy compares 1,024 XOR-of-hash bucket digests per round
(`docs/BUILD_PLAN.md` §8) — cheap, unconditional, incrementally maintained
in O(1) per write, no rescans. Its cost is per-bucket, not per-entry, so it
scales with cache *size* only insofar as bucket collision rates rise: at
some entry count, enough distinct keys land in the same bucket that a single
digest mismatch forces pulling the whole bucket's key/version list to find
the actual diff, even when only one entry disagrees. Invertible Bloom lookup
tables or minisketch-style set reconciliation would let two peers exchange a
sketch sized to the *diff*, not the bucket, regardless of how many entries
share a bucket.

**Cost:** a second reconciliation protocol alongside the existing digest
round, a decision about the sketch size (over-provisioned wastes bandwidth,
under-provisioned fails to decode and falls back anyway), and — the part
that actually matters — this only pays for itself once buckets are hot
enough that the fallback (full bucket pull) is happening routinely rather
than as the rare exception it's designed to be.

**Trigger:** measured anti-entropy bandwidth per round exceeds budget on a
cache in the ~10⁶-entries-and-up range, i.e. `BUCKET_COUNT` (1,024, fixed
today) is no longer coarse enough to keep bucket pulls rare. Below that scale
the current design is simpler and already convergence-bounded.

## Cache-config fingerprint gossip

Every node in a sundog cluster opens caches independently — nothing stops
node A from opening `"users"` as `Mode::Replicated` while node B opens the
same name as `Mode::Invalidation`, or with a different TTL. Today that's
silently inconsistent: `CacheError::ModeMismatch` already exists in
`sundog/src/error.rs` as a reserved variant, but nothing in the cluster
constructs it — there is no code path that would ever detect the mismatch,
let alone reject or warn about it. The fix Infinispan-adjacent systems use is
gossiping a per-cache config fingerprint (name, mode, and whatever else needs
to agree) as part of membership state — chitchat already carries a
per-node key/value bag for `data_addr`/`incarnation`
(`docs/BUILD_PLAN.md` §6) that a `caches` entry could extend — and comparing
a locally-`open()`ed cache's fingerprint against what peers advertise.

**Cost:** a wire format for the fingerprint, a decision about what "mismatch"
means for fields it's fine to disagree on (`max_capacity` is a local knob;
`mode` is not) versus fields that must agree, and a decision about what
happens on mismatch — reject `open()`, or just warn loudly and let LWW/AE
paper over the semantic disagreement as best they can.

**Trigger:** a real incident, or even a near-miss, where two nodes ran a
cache under different modes without anyone noticing until behavior diverged.
This is cheap enough (a few gossiped bytes, checked at `open()`) that it may
get pulled forward even without one, but it's not blocking anything today.

## Cluster-wide max-idle

Infinispan supports touch-propagating max-idle (TTI) across a distributed
cache — every read anywhere resets every replica's idle clock — and
[documents at length](https://infinispan.org/docs/stable/titles/configuring/configuring.html)
the anomalies that fall out of it: touch messages that themselves need to be
reliable and ordered against concurrent writes, idle timers that drift apart
under any message loss, and a feature that turns "read a cache" into
cluster-wide chatter. sundog's stance is **local-only, permanently**: `.tti()`
on a `CacheBuilder` bounds only that node's own idle eviction and is never
gossiped or replicated (`docs/BUILD_PLAN.md` §7, §13). This is not a gap to
be closed later — it's a documented refusal to reproduce a subsystem
Infinispan's own docs describe as a source of pain, not a design to emulate.

**Cost / trigger:** none tracked. Anyone who needs genuinely cluster-wide
idle expiry is pointed at Infinispan directly rather than at a future sundog
release — see "remote thin clients" below for the general shape of that
advice.

## Remote thin clients vs. "just run Valkey"

Infinispan's Hot Rod protocol lets non-embedding clients talk to a cluster
over a small binary TCP protocol with topology hints, so a process that
isn't itself a JVM member can still be a cache client. sundog is
embedded-only by design — the whole point is that every participant runs the
identical stack with no roles and no separate server tier
(`docs/BUILD_PLAN.md` §3). A thin-client protocol would mean designing and
versioning a second, external wire format alongside the internal one, plus
routing/topology-awareness for clients that aren't cluster members and can't
just watch chitchat's gossip.

**Cost:** effectively a second product — protocol design, client-side
topology tracking, versioning discipline for a wire format now consumed by
code sundog doesn't control the release cadence of.

**Trigger, and the honest caveat:** the moment a remote thin client is
wanted, the right first move is to compare against just running
[Valkey](https://valkey.io/) (or Redis) as a standalone cache server and
using its already-mature, already-multi-language client ecosystem. sundog's
entire value proposition is *embedded* zeroconf clustering for services that
already share a process boundary with their cache; a remote-client story
gives that up in exchange for reinventing a strictly worse Valkey. This item
stays on the roadmap as a sketch, not a plan, for exactly that reason.
