# sundog — module interfaces

**Status: historical.** This document is the interface contract handed to
the four build agents for the parallel implementation phase — it froze the
public signatures below *before* `discovery`, `membership`, `net`, and
`store` had bodies, so every module could be implemented independently
without touching another's files. That phase is done: all four modules,
plus `cluster.rs` and `cache.rs`, are fully implemented, tested, and green
(`cargo check --workspace`, `cargo clippy --workspace --all-targets -W
clippy::pedantic`, `cargo test --workspace`) — none of the `todo!()` bodies
described below still exist. Kept as a reference for the signatures and
ownership seams the implementation was built against; for the current state
of the library, read the modules themselves (each carries `//!` docs) or
`README.md`.

---

The workspace scaffold is in place. `node`, `hlc`, `wire`, `error`, `config`
are **complete** (with unit tests) and stable — treat them as a library.
`discovery`, `membership`, `net`, `store` are **stubs**: precise public
signatures, `todo!()` bodies, `cargo check --workspace` green. Four agents
implement these in parallel, one module each, without touching any file
outside their own directory/file. `cluster.rs` and `cache.rs` are also stubs
(owned by whoever wires the four modules together last — not part of this
parallel phase) and show how the pieces are meant to compose.

**Own your files only.** Discovery owns `src/discovery/**`. Membership owns
`src/membership/mod.rs`. Net owns `src/net/mod.rs`. Store owns
`src/store/mod.rs`. Never edit `Cargo.toml`, `src/lib.rs`, another module's
files, or this document — if you need a new dependency or a contract change,
report it instead of making it.

**Before using a third-party crate API** (chitchat, mdns-sd, moka,
hickory-resolver, postcard), read its real source in
`~/.cargo/registry/src/*/` — `cargo fetch` first if it's not there. Every
public signature below was written after reading the corresponding crate's
API; do not assume anything about internal fields not shown here.

**Every module ships its own `#[cfg(test)] mod tests`.** For `membership` and
`net`, real behavior needs live UDP/TCP, so their in-file tests are
necessarily thin (pure helper logic only) — the real exercise happens later
at the in-process integration layer (plan §11 layer 3), once all four modules
land.

---

## Threading and ownership, once for all four modules

- Every public type is `Send + Sync + 'static`. Nothing here is `!Send`.
- Every handle (`Membership`, `Mesh`, `Cache<K, V>`, `Cluster`) is cheap
  `Clone`: an `Arc`-wrapped inner, clones share state, dropping every clone
  does **not** stop background work — an explicit `shutdown()` does.
- Background work runs as `tokio` tasks. Don't spawn your own executor,
  don't block a worker thread (`std::sync::Mutex` held across an `.await` is
  a bug; prefer `tokio::sync::Mutex` or keep std-mutex critical sections
  synchronous and short).
- Traits that cross a module boundary and are held as `Arc<dyn Trait>` or
  `Box<dyn Trait>` (`Discovery`, `ShardOps`) are written in an
  **object-safe, boxed-future/boxed-stream form** — `BoxFuture<'_, T>` /
  `BoxStream<'static, T>` from the `futures` crate, not `async fn` /
  `impl Future` in the trait. This is a deliberate deviation from how the
  plan sketches `Discovery` (plan §5 uses RPITIT, which isn't object-safe);
  `ShardOps` follows the same pattern for consistency. Free functions and
  inherent methods (not behind `dyn`) use plain `async fn` as usual.
- `Cluster`'s shard registry is `RwLock<HashMap<SmolStr, Arc<dyn ShardOps>>>`
  (plan §7 says `HashMap` explicitly) rather than a lock-free map crate — no
  new dependency was added for it.

---

## What's already built (read, don't reimplement)

| Module | Key items |
|---|---|
| `node` | `NodeId(u64)` (random, hex `Display`, ordered, hashable, serde); `NodeName` = `{hostname}-{nodeid-hex}` |
| `hlc` | `Hlc { wall_ms, logical, node }` (derived lexicographic `Ord`); `HlcClock::{new, now, observe}` |
| `wire` | `WireRecord`, the full `Msg` enum (plan §6), `encode`/`decode` (postcard, `MAX_FRAME` = 4 MiB cap enforced both ways) |
| `error` | `CodecError`, `JoinError`, `CacheError` (all `thiserror`, `#[from]` where sensible) |
| `config` | `ClusterConfig` with defaults (`ae_interval` 30 s, `tombstone_ttl` 10 min, `outbox_capacity` 8192, `max_frame` 4 MiB, both bind addrs `0.0.0.0:0`) |

---

## `discovery` (`src/discovery/{mod,statics,mdns,dns}.rs`)

```rust
pub trait Discovery: Send + Sync + 'static {
    fn candidates(&self) -> BoxStream<'static, SocketAddr>;
    fn announce(&self, gossip_addr: SocketAddr) -> BoxFuture<'_, io::Result<()>>;
}
```

Implement three types, one per submodule, each implementing `Discovery`:

- **`statics::Static`** — fixed seed list (builder-supplied `Vec<SocketAddr>`
  and/or `SUNDOG_SEEDS=host:port,host:port` env var). `candidates()` should
  repeat the list on an interval rather than emitting once and terminating —
  a `Stream` that ends looks the same to a consumer as one that's merely
  slow, and membership needs a *continuous* seed source (plan §5). `announce`
  is a no-op `Ok(())`.
- **`mdns::Mdns`** — registers `_sundog._udp.local.` via `mdns-sd`, cluster
  name as a TXT property, instance = node id; browses continuously.
  Read `~/.cargo/registry/src/*/mdns-sd-0.21.0/` before touching this —
  the crate's own `async` feature and its `ServiceDaemon` browse/register
  API are the whole implementation.
- **`dns::DnsSrv`** — resolves a headless-service name via `hickory-resolver`
  SRV lookups on an interval. Read
  `~/.cargo/registry/src/*/hickory-resolver-0.26.1/` first.

Consumed by: `ClusterBuilder::build` (in `cluster.rs`, not your file) passes
`Box<dyn Discovery>` candidates into `Membership::spawn`'s `seeds` parameter,
and calls `announce` once the data-plane bind address is known.

---

## `membership` (`src/membership/mod.rs`)

```rust
pub struct Peer {
    pub node: NodeId,
    pub name: NodeName,
    pub gossip_addr: SocketAddr,
    pub data_addr: SocketAddr,
    pub incarnation: u64,
}

#[derive(Clone)]
pub struct Membership { /* private */ }

impl Membership {
    pub async fn spawn(
        cluster_name: SmolStr,
        node: NodeId,
        hostname: &str,
        data_addr: SocketAddr,
        config: &ClusterConfig,
        seeds: BoxStream<'static, SocketAddr>,
    ) -> Result<Self, JoinError>;

    pub fn local_peer(&self) -> &Peer;
    pub fn peers(&self) -> watch::Receiver<Vec<Peer>>;
    pub async fn shutdown(self);
}
```

Backed by `chitchat` (`~/.cargo/registry/src/*/chitchat-0.13.0/`,
`spawn_chitchat` in `src/server.rs`, `ChitchatId` in `src/types.rs`). Node id
string passed to chitchat is `NodeName::new(hostname, node).to_string()`;
cluster id is `cluster_name`. Advertise `data_addr` and `incarnation` as
chitchat key/value state entries; every `Peer` in the `peers()` watch stream
is reconstructed from another live node's gossiped state. `seeds` feeds
chitchat's seed list — re-poll it continuously (don't drain once at spawn),
so a full-cluster cold restart still converges (plan §5).

`shutdown` should perform a graceful chitchat departure (mark self as
leaving, let that propagate) before dropping the gossip task.

Consumed by: `ClusterBuilder::build` calls `spawn` once, then clones
`peers()` into `net`'s `Mesh::update_peers` on every change (a `tokio::spawn`
loop over `.changed().await`) and into `store`'s state-transfer donor
selection (lowest live `node` id, plan §9).

---

## `net` (`src/net/mod.rs`)

```rust
pub enum MsgClass { Invalidate, Replicate }

pub struct InboundMsg { pub from: NodeId, pub msg: Msg }

#[derive(Clone)]
pub struct Mesh { /* private */ }

impl Mesh {
    pub async fn spawn(bind_addr: SocketAddr, node: NodeId)
        -> Result<(Self, mpsc::Receiver<InboundMsg>), JoinError>;

    pub fn local_addr(&self) -> SocketAddr;
    pub fn update_peers(&self, peers: Vec<Peer>);
    pub fn send(&self, peer: NodeId, class: MsgClass, msg: Msg);

    pub async fn request_state(&self, donor: NodeId, cache: SmolStr)
        -> Result<BoxStream<'static, Result<WireRecord, CodecError>>, CodecError>;
    pub async fn ae_round(&self, peer: NodeId, cache: SmolStr, local_buckets: Vec<(u16, u64)>)
        -> Result<Vec<(u16, Vec<(Bytes, Hlc)>)>, CodecError>;
    pub async fn ae_pull(&self, peer: NodeId, cache: SmolStr, keys: Vec<Bytes>)
        -> Result<Vec<WireRecord>, CodecError>;

    pub async fn shutdown(self);
}
```

One `spawn` call binds the data-plane `TcpListener` and starts the
accept/dial tasks; the returned `mpsc::Receiver<InboundMsg>` is the **single**
consumer of unsolicited inbound traffic (`Invalidate`, `Replicate`, and any
`Hello`s from newly dialed-in peers) — request/response traffic
(`StRequest`/`StChunk`, `AeDigest`/`AeBucket`, `AePull`) is handled inline by
the three request methods instead of flowing through that channel, so your
accept-loop needs to demux on the first message type per logical exchange.

Per peer: `tokio_util::codec::LengthDelimitedCodec` (read
`~/.cargo/registry/src/*/tokio-util-0.7.19/src/codec/`), frame cap
`MAX_FRAME`, one bounded outbox per `MsgClass` (`outbox_capacity` from
`ClusterConfig`). Drop policy on overflow (plan §6, not optional):

- `Invalidate` — drop the **oldest** queued message for that peer.
- `Replicate` — drop the **new** message, mark the peer dirty (a
  `sundog_backlog_dropped_total{peer}` `metrics::counter!` increment), so the
  next anti-entropy round targets dirty peers first.

`send` is fire-and-forget: non-blocking, no error return, because the drop
policy *is* the error handling. `update_peers` reconciles connection tasks
against the live set (dial new peers, close departed ones) — called from
`Membership::peers()`'s change loop by whoever wires `cluster.rs`.

Consumed by: `store::Shard::insert`/`remove` call `send` for fan-out;
`store`'s anti-entropy loop calls `ae_round`/`ae_pull`; state transfer
(wherever the joining side lives) calls `request_state`.

---

## `store` (`src/store/mod.rs`)

```rust
pub const BUCKET_COUNT: usize = 1024;

pub enum Mode { Local, Invalidation, Replicated }
pub enum Origin { Local, Remote(NodeId) }
pub enum Event<K, V> { Created { key: K, value: V, origin: Origin }, Updated { .. }, Removed { key: K, origin: Origin } }

pub trait ShardOps: Send + Sync {
    fn apply_remote(&self, rec: WireRecord) -> BoxFuture<'_, ()>;
    fn invalidate(&self, key: Bytes, ver: Hlc) -> BoxFuture<'_, ()>;
    fn digests(&self) -> BoxFuture<'_, Vec<(u16, u64)>>;
    fn bucket_entries(&self, bucket: u16) -> BoxFuture<'_, Vec<(Bytes, Hlc)>>;
    fn records_for(&self, keys: Vec<Bytes>) -> BoxFuture<'_, Vec<WireRecord>>;
    fn snapshot_chunks(&self) -> BoxStream<'static, Vec<WireRecord>>;
    fn gc_tombstones(&self) -> BoxFuture<'_, ()>;
}

pub struct Stored<V> { pub value: V, pub ver: Hlc }

pub struct Shard<K, V> { /* private; implements ShardOps */ }

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    pub fn new(name: SmolStr, mode: Mode, node: NodeId, max_capacity: u64, ttl: Option<Duration>, tti: Option<Duration>) -> Self;
    pub fn name(&self) -> &str;
    pub fn mode(&self) -> Mode;
    pub async fn get(&self, key: &K) -> Option<V>;
    pub async fn get_or_load<F, E>(&self, key: &K, loader: F) -> Result<V, CacheError>
        where F: AsyncFnOnce(&K) -> Result<V, E>, E: std::error::Error + Send + Sync + 'static;
    pub async fn insert(&self, key: K, value: V) -> Result<(), CacheError>;
    pub async fn remove(&self, key: &K) -> Result<(), CacheError>;
    pub async fn invalidate_local(&self, key: &K);
    pub fn events(&self) -> broadcast::Receiver<Event<K, V>>;
}
```

Backed by `moka::future::Cache<K, Arc<Stored<V>>>`
(`~/.cargo/registry/src/*/moka-0.12.16/src/future/`), plus a sibling
tombstone map (own `tombstone_ttl` expiry) and a `[u64; BUCKET_COUNT]` XOR
digest array (`bucket(k) = xxh3(postcard(k)) & (BUCKET_COUNT - 1)`, using
`xxhash-rust`'s `xxh3::xxh3_64`; every apply does
`digest[b] ^= h(old) ^ h(new)`, no rescans — plan §8).

`apply_remote`/`invalidate`/local `insert`/`remove` **all funnel through one
versioned-apply function**: apply iff the incoming `Hlc` is greater than the
stored one. This is the single most important invariant in the project
(plan §4) — it's what makes replaying any set of records, in any order, any
number of times, converge. `insert`/`remove` additionally: postcard-encode
the value and reject with `CacheError::ValueTooLarge` if it exceeds
`config.max_frame`; stamp a fresh `Hlc` via an owned `HlcClock`; publish an
`Event` on the `broadcast` channel; then, per `Mode`, call `net::Mesh::send`
with `MsgClass::Invalidate` (mode `Invalidation`) or `MsgClass::Replicate`
(mode `Replicated`) — nothing for `Mode::Local`.

`K`'s postcard encoding is both its wire form and its digest-hash input —
must be deterministic (no map-typed keys; plan §13 asks for a debug-assert
of re-encode equality somewhere in this module).

Consumed by: `net`'s inbound-message loop calls `apply_remote`/`invalidate`
on the shard named in the message (looked up in `Cluster`'s
`Arc<dyn ShardOps>` registry); `net`'s AE/state-transfer request handlers
call `digests`/`bucket_entries`/`records_for`/`snapshot_chunks`;
`cache::Cache<K, V>` is a thin `Arc<Shard<K, V>>` wrapper exposing the typed
methods 1:1.

---

## Composition sketch (for context, not part of this phase)

```
ClusterBuilder::build()
  → hostname + NodeId::random()
  → Discovery::announce(gossip_addr)
  → Membership::spawn(.., seeds: Discovery::candidates())
  → Mesh::spawn(data_bind_addr, node)
  → tokio::spawn: loop { membership.peers().changed().await; mesh.update_peers(..) }
  → tokio::spawn: loop { mesh inbound Msg → look up Arc<dyn ShardOps> by cache name → apply_remote/invalidate }
  → tokio::spawn: jittered ae_interval loop → Mesh::ae_round/ae_pull against a live peer (dirty-marked first)

CacheBuilder::open()
  → Shard::new(..)
  → register in Cluster's shard registry
  → if the cache exists cluster-wide: Mesh::request_state(lowest-node-id donor) → Shard::apply_remote per chunk → one immediate ae_round
```
