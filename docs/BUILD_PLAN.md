# sundog — build plan

### An embedded, replicated, zeroconf cache for Rust, modeled on Infinispan's embedded mode

*Named for the parhelion: the sky rendering simultaneous copies of the sun in ice crystals — a replicated cache, drawn by the atmosphere.*

---

## 1. Goal and requirements

**Functional.** A library crate that any Rust service embeds. Instances of the service on the same network discover each other, form a cluster, and keep named caches coherent across nodes. Two clustered modes, mirroring Infinispan: **invalidation** (every node caches locally; writes broadcast an invalidate) and **replication** (every node holds every entry; writes broadcast the value). Read-through loading, per-entry TTL, size-bounded eviction, and cluster events (entry created/updated/removed, with origin node).

**Non-functional.** Set-and-forget: no operator action on node join, leave, crash, or network partition — the cluster reforms and converges on its own. Zeroconf on multicast-capable LANs; one env var of seeds everywhere else. AP semantics: the cache keeps serving during partitions and heals afterward. Target scale: 2–30 nodes, values ≤ a few MiB, LAN latencies.

**Non-goals for v1.** Distribution mode (consistent hashing, `numOwners`), transactions, queries, persistence, remote clients (Hot Rod equivalent), cross-DC, and transport security. Each is a deliberate cut; §12 says when to revisit.

**Toolchain.** Edition 2024, `rust-version = "1.97"`, resolver 3. `clippy::pedantic` clean in CI.

The one-sentence philosophy, and the reason this is buildable in weekends rather than years: **a cache is re-derivable data, so we buy availability and simplicity with eventual consistency** — last-write-wins on a hybrid logical clock, anti-entropy as the repair mechanism, and no consensus anywhere.

---

## 2. Infinispan → sundog map

This is the "when stuck, read the prior art" table. Infinispan solved every one of these; we borrow designs, not code.

| Infinispan concept | sundog equivalent | Reference when stuck |
|---|---|---|
| JGroups discovery (`MPING`, `DNS_PING`, `TCPPING`, `JDBC_PING`) | `Discovery` trait: `Mdns`, `DnsSrv`, `Static` | JGroups manual, "Discovery protocols" |
| JGroups membership + failure detection (`FD_ALL`, `VERIFY_SUSPECT`, views) | chitchat (SWIM-style gossip, phi-accrual failure detection) | SWIM paper; chitchat README |
| Reliable multicast (`NAKACK2`, `UNICAST3`) | none — TCP fan-out, losses repaired by anti-entropy | this substitution is the core simplification |
| Flow control (`MFC`/`UFC`) | bounded per-peer outboxes + drop policy (§6) | JGroups "flow control" chapter |
| Fragmentation (`FRAG4`) | frame size cap; oversized values rejected at the API | — |
| Cache modes `LOCAL` / `INVALIDATION` / `REPL_ASYNC` | `Mode::{Local, Invalidation, Replicated}` | ISPN user guide, "Clustered caches" |
| `DIST` mode, `ConsistentHash`, rebalancing | non-goal v1 | ISPN "Distribution" — read before ever attempting |
| Entry metadata / `SimpleClusteredVersion` | `Hlc` version stamp on every write | ISPN versioning docs |
| Expiration: lifespan / max-idle | TTL replicated as absolute expiry; TTI local-only (§7) | ISPN "Expiration" — note their cluster-wide max-idle pain |
| Eviction | moka `max_capacity` / weigher, local per node | — |
| State transfer on join | donor snapshot stream + one anti-entropy round (§9) | ISPN "State transfer" |
| Partition handling `ALLOW_READ_WRITES` + `MergePolicy` | AP always; LWW merge; pluggable `ConflictResolver` later | ISPN "Partition handling" |
| `@CacheEntryCreated` listeners | `cache.events()` → `broadcast::Receiver<Event>` | — |
| JMX statistics | `metrics` crate → Prometheus exporter | — |

---

## 3. Architecture overview

Three planes, deliberately decoupled so each is testable alone:

```
┌──────────────────────────── one service process ────────────────────────────┐
│                                                                             │
│  your code ──► Cache<K,V> handles (typed) ──► Shard registry (type-erased)  │
│                                                       │                     │
│  ┌─────────────┐   seeds   ┌──────────────┐  events   │   ┌──────────────┐  │
│  │ DISCOVERY   │──────────►│ MEMBERSHIP   │──────────►├──►│ DATA PLANE   │  │
│  │ mDNS / DNS  │           │ chitchat     │ live set  │   │ TCP mesh     │  │
│  │ / static    │           │ (UDP gossip) │           │   │ postcard     │  │
│  └─────────────┘           └──────────────┘           │   └──────┬───────┘  │
│                                                       │          │          │
│                            ┌──────────────────────────┴──────┐   │          │
│                            │ STORE: moka shards + HLC apply  │◄──┘          │
│                            │ + bucket digests + anti-entropy │              │
│                            └─────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
        every node runs the identical stack — no roles, no leader
```

- **Discovery** answers "who might be out there" and produces candidate socket addresses, continuously (not just at boot — a fully restarted cluster must re-find itself).
- **Membership** (chitchat) answers "who is alive right now" and gossips a tiny per-node key-value state; we use it to advertise each node's data-plane address and incarnation number. Its watch stream drives everything downstream.
- **Data plane** is a lazy full mesh of TCP connections carrying cache traffic: invalidations, replications, state transfer, anti-entropy. Losing a message here is acceptable by design — anti-entropy repairs it.
- **Store** is moka plus the versioned-apply logic that makes replication commutative, plus incrementally-maintained digests that make repair cheap.

### Crate bill of materials

| Concern | Crate | Notes |
|---|---|---|
| Runtime | `tokio`, `tokio-util` | codecs, `TaskTracker`, `broadcast` |
| Local cache | `moka` (async) | the Caffeine of Rust: TTL, TTI, weigher, `Expiry` trait |
| Membership | `chitchat` | Quickwit's production gossip; alt: `foca` (sans-io SWIM) if we ever need full transport control |
| Zeroconf | `mdns-sd` | DNS-SD service discovery, pure Rust |
| DNS seeds | `hickory-resolver` | SRV/A lookups for the K8s path |
| Wire format | `serde` + `postcard` | compact, deterministic for a fixed type — required for key hashing (§8) |
| Versioning | `uhlc` | hybrid logical clock, battle-tested in Zenoh |
| Hashing | `xxhash-rust` | bucket + digest hashing |
| Errors | `thiserror` | one enum per fallible domain |
| Observability | `tracing`, `metrics` | spans on join/ST/AE; Prometheus via exporter feature |
| Testing | `proptest`, `turmoil` | property tests + deterministic network simulation |

Workspace: keep it to two members for now — `sundog` (lib, with `discovery`, `membership`, `net`, `store`, `cache` modules) and `sundog-demo` (bin). Split crates only when a boundary proves itself; the module seams below are drawn so the split is mechanical later.

---

## 4. Consistency model — the decisions that shape everything

**AP, always.** Both sides of a partition keep reading and writing. This is Infinispan's `ALLOW_READ_WRITES` stance and the only stance compatible with "set-and-forget"; a quorum-based design (Raft) halts writes on partition and needs explicit membership — the opposite of the goal.

**Every write is stamped once, at its origin, with an HLC version** `(wall_ms, logical, node_id)`. Comparison is lexicographic. HLC gives us happens-before when clocks are sane and total order even when they aren't; NTP-level skew is absorbed by the logical component.

**Apply is a pure function and the heart of correctness:** an incoming record (local write, replicated write, state-transfer chunk, anti-entropy repair — all four go through the *same* path) is applied iff its version is greater than the stored one. Consequence: applying any set of records **in any order, any number of times, converges to the same state**. That property is what lets us drop reliable multicast, tolerate message loss, and interleave state transfer with live traffic. It is also directly testable (§11) — the permutation-convergence property test is the most important test in the project.

**Deletes are writes.** A remove stores a tombstone `(key, ver, deleted)` so that a stale replicated put can't resurrect the key. Tombstones are GC'd after `tombstone_ttl` (default 10 min, rule: ≥ 3 × anti-entropy interval). The documented hazard: a node partitioned away *longer than tombstone_ttl* may resurrect deleted entries on merge. For a cache this is acceptable (the entry re-expires or gets re-invalidated); if a key must never resurrect, it doesn't belong in a cache.

**Trade-offs accepted:** readers can observe stale values for up to replication latency (normal path) or partition duration + one AE round (worst case); LWW discards concurrent writers' losers silently. Both are inherent to the category — Infinispan's async replicated mode makes the same trades.

---

## 5. Discovery — the JGroups `PING` layer

```rust
pub trait Discovery: Send + Sync + 'static {
    /// Continuous stream of candidate peer gossip addresses. Duplicates fine.
    fn candidates(&self) -> impl Stream<Item = SocketAddr> + Send;
    /// Make ourselves findable (mDNS register; no-op for static/DNS).
    fn announce(&self, gossip_addr: SocketAddr) -> impl Future<Output = io::Result<()>> + Send;
}
```

Three implementations, mirroring the JGroups discovery stack:

1. **`Mdns`** (default) — registers `_sundog._udp.local.` with the cluster name as a TXT property and instance = node id; browses continuously. This is the zeroconf path and works on real LANs, office networks, edge boxes.
2. **`Static`** — seed list from the builder or `SUNDOG_SEEDS=host:port,host:port`. The escape hatch and the test-suite workhorse.
3. **`DnsSrv`** — resolve a headless-service name on an interval. The Kubernetes answer, equivalent to `DNS_PING`; "zeroconf" there means one line of config you already have.

Discovery feeds chitchat's seed set continuously. Corner case to design for from day one: **full-cluster cold restart** — all nodes reboot, nobody remembers anybody; continuous mDNS browsing (not a one-shot at boot) is what makes this heal.

Docker/Wi-Fi reality check (dragon §13): mDNS does not cross the default Docker bridge; compose demos use `Static`, host-network demos use `Mdns`.

---

## 6. Membership and the data-plane mesh

**chitchat** runs its own UDP loop; we give it: node id (`{hostname}-{short_uuid}`), the cluster name (as chitchat's cluster id — wrong-cluster gossip is rejected for free), seeds from Discovery, and our node-state entries: `data_addr` (the TCP port for the data plane, chosen ephemerally and advertised — zero ports to configure), `incarnation`, and later a `caches` fingerprint for config-mismatch warnings. Its live-nodes watch stream is our membership truth: no separate view/epoch machinery in v1 — the SWIM failure detector plus our idempotent apply rule make precise view synchrony unnecessary (that's the second big simplification vs JGroups).

**Data plane:** per live peer, a lazily-dialed TCP connection with `LengthDelimitedCodec` (frame cap 4 MiB — oversized `insert` fails fast with `Error::ValueTooLarge`; we do not fragment in v1) and a bounded outbox (`mpsc`, default 8 192 msgs). Backpressure policy per message class, stated explicitly because this is the JGroups-flow-control analog and the classic silent-failure spot:

- `Invalidate` overflow → drop oldest (an invalidation storm on a dead peer must never stall writers); correctness unaffected — worst case a peer serves stale until TTL/AE.
- `Replicate` overflow → drop and mark the peer *dirty*; the next anti-entropy round targets dirty peers first. Increment `sundog_backlog_dropped_total{peer}` — if that metric moves in steady state, the cluster is undersized and the metric is the honest signal.
- State-transfer and AE messages are request/response on their own streams and never queue behind the broadcast path.

Wire messages (postcard over serde; `WireRecord` = `{ key: Bytes, value: Option<Bytes>, ver: Hlc, expires_at_ms: Option<u64> }` — `value: None` is the tombstone):

```rust
enum Msg {
    Hello { node: NodeId, incarnation: u64 },
    Invalidate { cache: SmolStr, key: Bytes, ver: Hlc },
    Replicate  { cache: SmolStr, rec: WireRecord },
    StRequest  { cache: SmolStr },
    StChunk    { cache: SmolStr, recs: Vec<WireRecord>, done: bool },
    AeDigest   { cache: SmolStr, buckets: Vec<(u16, u64)> },
    AeBucket   { cache: SmolStr, bucket: u16, entries: Vec<(Bytes, Hlc)> },
    AePull     { cache: SmolStr, keys: Vec<Bytes> },
}
```

---

## 7. Store: shards, typed handles, expiry

A named cache is a **`Shard<K, V>`**: a moka `Cache<K, Arc<Stored<V>>>` (values wrapped in `Arc` — remote fan-out clones are pointer clones), the per-key version table folded into `Stored { value, ver }`, tombstones in a sibling small map with their own TTL, and the digest array (§8). The `Cluster` registry holds `HashMap<SmolStr, Arc<dyn ShardOps>>` where `ShardOps` is the type-erased surface the network layer drives: `apply_remote(WireRecord)`, `digests()`, `bucket_entries(u16)`, `snapshot_chunks()`. The typed `Cache<K, V>` handle the user holds is a thin wrapper over `Arc<Shard<K, V>>` — serialization happens only at the wire boundary; local reads never deserialize.

Bounds: `K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static`, same minus `Hash + Eq` for `V`. Postcard encoding of `K` doubles as the wire key and the digest-hash input, which is why deterministic encoding matters: no `HashMap`-typed keys (document it; debug-assert re-encode-equality in tests).

**Expiration.** Lifespan (TTL) is cluster-meaningful, so it travels as an **absolute** `expires_at_ms` computed at the origin; each replica converts to a remaining duration through a moka `Expiry` implementation. Max-idle (TTI) is **local-only** by design — Infinispan's cluster-wide max-idle requires touch-propagation chatter and still has documented anomalies; we skip that swamp knowingly. Eviction (capacity/weigher) is local per node, exactly as in Infinispan.

---

## 8. Anti-entropy — the set-and-forget insurance

Per shard: 1 024 buckets, `bucket(k) = xxh3(key_bytes) & 1023`. Each bucket keeps a running digest = **XOR of `xxh3(key_bytes ‖ ver)` over its live entries and un-GC'd tombstones**. XOR is order-independent and incrementally maintainable: every apply does `digest[b] ^= h(old_entry)` (if present) `^= h(new_entry)` — O(1), no rescans, ever.

The round, on a jittered interval (default 30 s), each node independently: pick one random live peer (dirty-marked peers first) → send all 1 024 `(bucket, u64)` pairs (~10 KiB — cheap enough to be unconditional) → peer replies `AeBucket` key/version lists for mismatched buckets only → both sides diff: entries the other lacks or holds older get pushed via the normal `Replicate` path; missing-newer entries get `AePull`ed. Convergence bound: any single lost update heals in O(rounds to random-pick that peer); a healed partition fully reconciles in a handful of rounds, and the permutation-convergence property guarantees the result is identical on both sides.

Known weakness, accepted: XOR-of-hashes can collide (birthday-bound on 64 bits, negligible against faults, weak against an adversary — but the trust model is "my own trusted LAN"). The upgrade path if bucket diffs ever get chatty at scale is set reconciliation (IBLT/minisketch) — parked in §12.

---

## 9. State transfer on join

A joining node (or a node opening a cache that already exists in the cluster) picks the **lowest node-id among live members** as donor (deterministic, no election), sends `StRequest`, and the donor streams `StChunk`s of ~500 records by iterating its moka shard. Moka iteration is weakly consistent — perfect here, because chunks flow through the same versioned apply as everything else, and live `Replicate` traffic interleaves safely: whatever the snapshot missed, concurrency delivered; whatever both delivered, the version check deduplicated. After `done: true`, run one immediate AE round against the donor as a belt-and-braces sweep. No cluster pause, no rebalance, no transfer coordinator — replicated-AP mode makes Infinispan's hardest subsystem almost trivial, which is exactly why DIST mode stays a non-goal.

Donor dies mid-stream? The receiver notices via membership, re-picks, re-requests. Idempotent apply makes the restart free.

---

## 10. Public API sketch

```rust
// Zeroconf happy path — this exact snippet is the acceptance test for the whole project:
let cluster = Cluster::builder("demo")
    .build()                       // mDNS discovery, ephemeral ports, sane defaults
    .await?;

let users: Cache<UserId, Profile> = cluster
    .cache("users")
    .mode(Mode::Replicated)        // or Mode::Invalidation (default), Mode::Local
    .max_capacity(200_000)
    .ttl(Duration::from_secs(600))
    .open()
    .await?;                       // triggers state transfer if the cache exists cluster-wide

users.insert(id, profile).await?;                 // stamp HLC → local apply → fan out
let p = users.get_or_load(&id, |id| async move {  // read-through; moka collapses stampedes
    db.load_profile(id).await
}).await?;
users.remove(&id).await?;                         // tombstone write

let mut events = users.events();                  // broadcast::Receiver<Event<K, V>>
while let Ok(ev) = events.recv().await {
    if let Event::Removed { key, origin: Origin::Remote(node), .. } = ev { /* … */ }
}

cluster.shutdown().await;                         // graceful leave (chitchat departs politely)
```

Design notes, per house style: builders are own-and-return with `#[must_use]`; `Mode` is an enum, not booleans; errors are one `thiserror` enum per domain (`JoinError`, `CacheError`) with `#[from]` conversions; the loader is `impl AsyncFnOnce(&K) -> Result<V, E>`; every handle is `Clone + Send + Sync` and cheap. `Cluster::builder(..).build()` with zero further calls **must** form a working LAN cluster — every added line of config is a defect against the project goal.

---

## 11. Testing strategy

Layered, cheapest first:

1. **Property tests (proptest)** on the pure core — the highest-value suite:
   - *Permutation convergence:* generate a random set of writes/removes across virtual nodes, apply every permutation (sampled) with duplications and drops → final states identical. This single property is the license for the whole loss-tolerant design.
   - HLC: monotonicity, tiebreak totality, skew absorption.
   - Digests: incremental maintenance ≡ full recompute after arbitrary op sequences; tombstone GC keeps digest and entry-set consistent.
2. **Deterministic simulation (turmoil)** for the data plane + AE with a scripted membership feed (the net layer is written against tokio's I/O traits precisely so turmoil can host it): partitions during write load on both sides → heal → assert convergence within N rounds; message loss/reorder/dup storms; donor crash mid-state-transfer.
3. **In-process integration** (`Static` discovery on loopback ephemeral ports): 3–5 real nodes in one test binary; kill/restart; assert invalidation and replication visibility with bounded waits. chitchat runs real UDP here — this layer is where membership itself gets exercised.
4. **Chaos demo bin**: `sundog-demo` runs N nodes with a TUI cluster view and injectable faults (drop node, pause node, partition via a toggleable transport filter). Doubles as the README screencast and the manual soak-test rig (24 h, memory flat, digests converged).

CI: fmt (style 2024), `clippy --all-targets -- -W clippy::pedantic`, tests, plus a scheduled job running the turmoil suite with fresh seeds and logging failing seeds for replay.

---

## 12. Milestones

Each milestone ends green, shippable, and demo-able; M3 is the first internally-useful artifact.

| # | Scope | Acceptance criterion | Est. |
|---|---|---|---|
| M0 | Workspace, config/env layer, tracing, CI skeleton | pipeline green; `sundog 0.1.0` skeleton published to crates.io — name reserved the legitimate way | 1 evening |
| M1 | `Discovery` trait + `Mdns`/`Static`; chitchat integration; membership watch | 3 terminals on a LAN converge on one member list with zero config; kill −1 → detected < 5 s; full restart re-forms | 1 weekend |
| M2 | TCP mesh, codec, outboxes + drop policy, `Hello` | echo bench across 3 nodes; overflow unit tests prove the per-class policy | 1 weekend |
| M3 | moka shards, typed `Cache<K,V>`, **invalidation mode**, `get_or_load`, events | put on A → stale entry on B invalidated; loader stampede collapsed; *ship it* | 1 weekend |
| M4 | HLC + versioned apply + **replication mode** + tombstones/GC | put on A readable on B; permutation-convergence proptest green | 1–2 weekends |
| M5 | State transfer | cold node joins 100 k-entry cluster → warm in seconds; donor-kill mid-ST recovers | 1 weekend |
| M6 | Anti-entropy | partition-and-heal turmoil test converges ≤ 5 rounds; digest bandwidth within budget | 1–2 weekends |
| M7 | Hardening: metrics, `DnsSrv`, docs, soak, demo polish | Prometheus dashboard; 24 h soak flat; README with the §10 snippet working verbatim | ongoing |

Sequencing note: M1–M3 contain zero distributed-systems risk — it's integration work with excellent crates, and the payoff (invalidation mode) already covers the most common Infinispan use case (keeping local caches honest). The novel-engineering budget is concentrated in M4 + M6, which is exactly where the property/simulation tests live.

---

## 13. Dragons — Infinispan's scars, pre-acknowledged

**Delete resurrection** if a node returns after `tombstone_ttl` — accepted for cache semantics, documented loudly, tunable. **Max-idle across the cluster** — deliberately not replicated; anyone who needs it gets pointed at Infinispan's own docs on why it hurts. **Slow consumers** — the bounded-outbox drop policy makes degradation loud (metrics) instead of silent (unbounded memory), which is the failure mode that actually pages people. **Large values** — hard frame cap; fragmentation is a feature request, not a default. **Clock chaos** — HLC absorbs skew for ordering, but *absolute TTL expiry* still trusts wall clocks to within seconds; state the NTP assumption in the README. **mDNS in containers/Wi-Fi isolation** — default Docker bridges and AP client isolation eat multicast; the demo compose file uses `Static` and says why. **Postcard determinism** — key types must encode canonically; forbid map-typed keys, assert in debug. **moka semantics** — iteration is weakly consistent and eviction is advisory-timed; both fine for the design, both worth a comment where relied upon.

---

## 14. Revisit as it grows

Distribution mode (consistent hash ring, `numOwners`, per-key primary) — only if replicated memory footprint actually becomes the constraint; it drags in rebalancing and ownership transfer, the hardest 20% of Infinispan. mTLS on the data plane (rustls) + gossip keying the moment this leaves a trusted network. QUIC (quinn) if head-of-line blocking between ST streams and broadcast traffic shows up in practice. Set-reconciliation digests (IBLT) if AE bucket diffs get heavy past ~10⁶ entries. A `ConflictResolver` trait if any consumer outgrows LWW. And if the project ever wants remote thin clients, that's the moment to steal the Hot Rod idea: a tiny TCP protocol with topology hints — but at that point, compare honestly against just running Valkey.
