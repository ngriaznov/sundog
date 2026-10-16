# Guarantees

sundog is eventually consistent. Every node that stays connected converges
on the same content for a shared cache. There is no consensus round and no
leader, so a write returns once it applies locally, and peers learn of it
a moment later.

## Conflicting writes

Every write carries a hybrid logical clock stamp: wall-clock milliseconds,
a logical counter, and the writing node's id as a final tiebreak. By
default the higher stamp wins, everywhere, regardless of the order writes
arrive in. Two nodes writing one key at nearly the same instant both
succeed locally, and the older write disappears once the newer one
reaches it. No error reports the loser.

A custom `ConflictResolver` replaces that rule for two values. Its `winner`
may prefer a record with a lower version, and replicas still converge on
it: anti-entropy exchanges both records of a mismatched key whenever the
resolver reads values, so the replica that holds the winner at the lower
version sends it and the other adopts it. A tombstone and a value settle
by version for every resolver, the higher version winning, so a deleted key
stays deleted whatever a resolver prefers. A resolver that returns `false`
from `needs_value_bytes` promises that it prefers the greater version, and
anti-entropy repairs it one way.

Keep node clocks synchronized with NTP or your platform's time service. A
node whose clock runs ahead stamps its writes later than its peers' and
wins their conflicts. `ClusterConfig::max_clock_skew`, one minute by
default, caps how far: a node refuses a record stamped further ahead of its
own clock than that, whether it arrives by replication, anti-entropy or
state transfer, and does not let the stamp pull its clock forward. The
refused write stays on the node that made it and reaches the others through
anti-entropy once their clocks catch up with its stamp.
`sundog_clock_skew_rejected_total{cache}` counts refusals.

A `Distributed` write that a non-owner forwards is the exception: the writer
keeps no copy, so the frame is the only one. An owner that refuses its stamp
holds the record in memory and applies it once its own clock is within the
bound, about the stamp's lead minus `max_clock_skew` after the write.
Correcting the writer's clock afterwards does not shorten the wait, because
the stamp is fixed when the write is made. While a record is held, the
owner's clock has not absorbed its stamp, so a write or remove made in the
meantime, here or on a node with a correct clock, stamps below the held
record and loses to it when it applies: an acknowledged insert from a fast
clock can bring back a key removed during the wait, and a held remove can
delete a newer insert. Several held versions of one key reduce to the newest,
except for a merging resolver, which holds every contribution that fits. The
hold covers 16,384 records and 32 MiB per cache. A record that does not fit
is dropped and counted with reason `full`; a held record is never evicted to
make room, so under a merging resolver the contribution that arrives at a
full hold is the one lost. A record whose wait would pass the shorter of
`tombstone_max_ttl` and a day, a stamp ahead by more than `max_clock_skew`
plus that limit, is a broken clock rather than skew, and is dropped and
counted with reason `horizon`. Both count in
`sundog_clock_skew_hold_dropped_total{cache, reason}`. The hold is in memory:
a crash loses it, and a cache that closes hands its held records to the other
owners of their parts, which hold them in turn. A forward whose writer is an
owner of the part is not held, because that owner kept its record and
anti-entropy offers it again. Rebalance and state transfer do
not hold what they are refused: a record a receiver refuses for skew is not
applied there, and a donor that finishes its round can release the part.

A restart that replays a spill snapshot resumes the node's clock at or past
the replayed stamps, so a clock that steps back across a restart does not
make the node's new writes and deletes lose to its own old entries. Until
the system clock catches up, the node's writes stay ordered at the replayed
stamp, and peers may refuse them under `max_clock_skew` like any write
stamped ahead.

The floor covers the entries the snapshot holds. The snapshot holds no
tombstones, so a delete this node stamped ahead of its restarted clock is not
in it. In `Replicated` and `Distributed`, a peer keeps that tombstone for
`tombstone_ttl` and refuses the node's lower-stamped write of the same key;
anti-entropy then sends the tombstone back and the node deletes the value it
wrote. A tombstone the node receives from a peer pulls its clock past the
tombstone's stamp when that stamp is within `max_clock_skew`, so a write made
after the node has received it stamps past it. A write made before then, or
against a stamp ahead by more than `max_clock_skew`, is lost this way until
the system clock passes the tombstone's stamp.

### Values that merge

A value that combines, a counter or a set, does not have to lose writes. A
`ConflictResolver` that implements `merge` folds the stored and incoming
records into a third value, and every node reaches the same result in any
order, as long as no deadline lapses between the writes. `sundog::crdt`
ships two: `PnCounter` with `PnCounterResolver` and `OrSet` with
`OrSetResolver`. Every mode merges. [Choosing a resolver](resolvers.md) says
when a merging resolver fits, how to write its values, and how they expire.

```rust
{{#include ../cookbook/src/counters.rs:counter}}
```

`Cache::merge` writes through the resolver without a read.
`CacheBuilder::merge_coalesce_window` folds a writer's consecutive merges
(`merge` and `merge_with_ttl`) to one key into one applied record per
window, and a read does not see a fold still pending inside its window.

Each writer owns a slot in a merged value. A background sweep retires the
slot of a writer whose node has been absent longer than
`ClusterConfig::crdt_retire_after`, 24 hours by default, or has restarted
under a new incarnation. A node this node never saw as a member counts as
absent from the later of this node's start and the sweep's first sight of its
writer, so the slots that earlier deploys left retire once this node has run
for the bound; a fleet that replaces every node faster than that bound lowers
`crdt_retire_after`, or merged values keep a slot for every deploy
([Tuning](tuning.md)). A writer the sweep meets for the first time waits a
full bound before it retires, so a live node that gossip has not shown yet is
a peer by then and never retires. The sweep reads resident records only, so a
spilled record keeps its slots until it is read back.

A `Replicated` or `Distributed` cache folds a retirement away once it has
aged past twice that bound with every peer quiet. Each fold leaves a receipt
that every merge honors for three times `crdt_retire_after` past the fold,
longer when the sweep period is more than half that bound. The lifetime
starts at the fold, which waits for a quiet cache and trails the retirement by
at least twice the bound, so a receipt lives about five bounds past its
retirement at the defaults, however long churn delays the fold. Replicas fold
at their own sweeps and stamp the receipt with that instant, so their records
differ by the stamps until one anti-entropy exchange merges them, the later
stamp winning. A receipt stamped more than its lifetime ahead of a replica's
clock expires there instead of outliving the others. The replicas of a part
hold the same writers, so they fold nested sets of writers and the merge of
their folded totals is exact; anti-entropy keeps it so, except for a replica
cut off from before the fold until after the receipt expires, which still
holds the slot every other replica has folded and counts that writer's
contribution twice when it reconnects. Like `tombstone_max_ttl` for deletes,
below, the bound trusts that a replica gone this long no longer holds the
state.

An `Invalidation` cache retires a dead writer but never folds it. The nodes
of such a cache hold different writers, each the ones it saw, so two nodes can
fold disjoint sets, and merging their folded totals would keep the larger and
lose the smaller. A merged value there keeps one retired entry, a few bytes,
for every dead writer it has seen, and stays exact.

## Deletes

`remove` writes a tombstone that replicates like a value and outvotes
every older copy of the key. Tombstones stay for `tombstone_ttl`, 10
minutes by default, counted from the delete's version stamp, so every
replica collects one at the same grid tick. A tombstone a peer sends that
arrives after its stamp plus `tombstone_ttl` is ignored: it deletes nothing
and is not stored, and `sundog_tombstones_ignored_total{cache}` counts it. A
delete therefore has to reach each replica within `tombstone_ttl` of its stamp,
or that replica has to be deferring. Keep node clocks within seconds of each
other: a receiver whose clock runs ahead of a writer's by nearly
`tombstone_ttl` ignores that writer's fresh deletes, and a writer whose clock
runs behind by `tombstone_ttl` stamps deletes that arrive lapsed. A node's own
`remove`, `remove_many` and `clear` are never ignored. While a member that gossip remembers is absent, a
cache keeps its tombstones longer, up to `tombstone_max_ttl`, 24 hours by
default, and for another `tombstone_ttl` once that member is back, so a
node returning from a partition cannot bring a deleted key back. A
`Replicated` cache counts every absent member. A `Distributed` cache
counts an absent member that shares a part with this node once it is
eligible again: its ownership view drops the member, yet the member keeps
its copy of every part it owned, and a delete made meanwhile reaches only
this side. A graceful leave is not an absence: the leaver hands its parts
off and keeps nothing. A `Replicated` or `Distributed` cache also defers
collection for the first `tombstone_ttl` after it opens, whoever is absent:
a node that reopened warm can hold a copy that a peer's tombstone has to
delete, and a node that just started has no memory of which members were
away.

A pause defers collection too. A process that was stopped, a virtual machine
that was suspended and a starved gc task all miss the deletes their peers
make, and gossip does not reliably flag them: the monotonic clock the failure
detector reads does not advance across a suspend. A `Replicated` or
`Distributed` node compares the wall clock at each tombstone collection with
the one before. A gap of more than twice the collection period, a quarter of
`tombstone_ttl` and at least a second, counts as a pause. The node then
defers for another `tombstone_ttl` and logs one warning with the gap; a second
pause inside the hold extends it. The check also runs when a record arrives,
so the first delete after a resume is judged against `tombstone_max_ttl`
before the gc task wakes. The gap is half of `tombstone_ttl` for a
`tombstone_ttl` of 4 seconds or more; the one-second floor of the period makes
it 2 seconds below that. A pause shorter than the gap is not detected: the
deletes made meanwhile have the rest of `tombstone_ttl` from their stamp to
reach the node. A false positive, such as a clock stepped forward, only
lengthens the deferral.

The tombstone an expired entry leaves in `Replicated` and `Distributed` under
a non-merging resolver is kept by the same rules, counted from the entry's
expiry instead of from the delete's stamp. `sundog_tombstones{cache}` reports how many
a node holds.

A member gone longer than `tombstone_max_ttl` returns after the tombstone
is collected, and its stale copy can come back, limited by that copy's own
expiry. Give every entry a TTL and deletes stay deleted whatever the
downtime. Without TTLs, raise `tombstone_max_ttl` above the longest outage
you tolerate.

A remove or clear outvotes a merge still waiting in its
`merge_coalesce_window`: the pending fold is discarded, `clear` covers a key
that exists only as a pending fold, and a merge made after the remove opens
a fresh fold. A `clear` also covers a fold a flush is applying as the clear
begins: it waits for that flush, so the key is removed. In a `Distributed`
cache a node clears the keys it holds. A key it does not own and holds only as
a pending fold is not one: `clear` applies that fold, which forwards it to the
owners, and writes no tombstone, so a non-owner's `clear` never deletes a
value at the owners. A disown-grace copy of such a key is still stored here
and is tombstoned. A tombstone from a peer discards a pending fold made before
it. A tombstone stamped between two calls folded into one pending record,
which only a peer whose clock runs behind produces, leaves the whole fold in
place. A fold that flushes carries the version of its last `merge` call, so
every write and remove stamped after that call outranks it, as with no
window.

## Expiry

A TTL-expired entry never returns. Every record carries its absolute
expiry, so it expires at the same instant on every node, and no node
accepts a copy past its expiry from a peer. `CacheBuilder::ttl` sets a
cache's default lifetime, and `insert_with_ttl` and `insert_many_with_ttl`
set one entry's or one batch's. Reads never extend or accept a TTL.

`expire`, `touch` and `persist` give a live entry a new lifetime and keep
its value: a given duration, the cache's default again, or none. Each
writes back the value this node reads, stamped as that value's successor,
so a write or remove stamped in a later millisecond on any node supersedes
it. A new lifetime never brings back a removed key or an overwritten value;
only a write made in the same millisecond as the value, which counts as
concurrent with it, can lose to one. A `Distributed` node that does not
hold the key asks an owner for it, as `fetch` does. `ttl_of` reads the
lifetime left on this node's copy.

A write that has already expired when it is sent, a zero TTL included,
reaches every peer as an already-expired record, so the peers drop the value
it replaced. In `Invalidation` with a resolver that does not merge, the
peers get the invalidation instead. A write whose entry is gone by the time
it is sent, swept, disowned or idle, invalidates the older copy on every
peer that receives the frame; a frame lost to an overflowing outbox is not
resent. In `Replicated` and `Distributed` an evicted entry spills to disk
rather than disappearing, so a departed write there comes from a sweep or
from an ownership release. With a merging resolver a departed write sends
nothing, in every mode: a peer's merged copy is the best one left, and a bare
invalidation cannot say which contributions the writer's copy lacked.
A resolver that reads values may store a lower version than the write, such
as a longer value that anti-entropy delivered first. That record is the
resolver's winner, so the write sends it as a record, not as a departed
invalidation.

In `Replicated` and `Distributed` under a non-merging resolver, the sweep that
removes an expired entry keeps a tombstone at the entry's version. A replica
that missed the overwrite cannot bring the overwritten value back, whether or
not the sweep had run when the older copy arrived, and anti-entropy removes
that copy from the replica that still holds it. The tombstone counts its
retention from the entry's expiry, so every replica that learns of it, by
sweeping or from a peer, collects it at the same instant: `tombstone_ttl`, and
up to `tombstone_max_ttl` while collection defers, as for a delete. A
tombstone that arrives already past that retention is ignored, whatever the
node holds for the key: it deletes nothing and is not stored. A replica that
collected it never takes it back from one that has not, and the same records
applied in any order leave the same state. Within its first `tombstone_ttl` a
cache defers collection, so a tombstone past `tombstone_ttl` but not
`tombstone_max_ttl` is kept there and removes an older copy the node holds.
Shortening a lifetime takes the same path: once the shortened entry expires
and is swept, a node cut off for the whole lifetime loses its longer-lived
copy to anti-entropy instead of bringing the entry back. After the retention
ends, a copy that was cut off longer is accepted again, limited by its own
expiry, as after a delete. Nodes sweep on their own clocks, which differ by up
to `max_clock_skew`, so a peer can take a tombstone that much before its own
clock reaches the expiry.

A `Distributed` owner of a part it has not yet verified declines a peer's
fetch for a key it holds nothing for, and the peer asks the next owner. An
owner that swept an expired entry under a non-merging resolver holds its
tombstone, so it answers that fetch with a miss instead.

A cache with a merging resolver keeps no tombstone: it reads an expired side
as absent on every node, swept or not, so a write older than the expired
value starts a new value there. `Local` and `Invalidation` run no
anti-entropy and keep nothing either. In `Invalidation` a node cut off for
the whole lifetime of a shortened entry keeps its copy's old deadline and
can bring the entry back until that deadline passes; `remove` is the delete.

Under a merging resolver the resolver sets a merged record's expiry, and the
`crdt` resolvers keep the later of the two deadlines, an unset side
deferring to the other. A merged value therefore expires with its
longest-lived contribution: `persist` leaves its deadline, `expire` extends
it and never shortens it, and `touch` moves it to the default TTL from now
when that is later. A record whose deadline passed before the other side's
write was made never folds into the result, so a merge after expiry starts
a new value. On the node applying a write, a value that reads as absent
there, expired or idle, never folds either.

`CacheBuilder::tti`, an idle timeout, is local to one node and applies only
to `Local` and `Invalidation` caches. It is measured on the observing
node's clock. A read, a sweep or a merge that sampled its clock before a
later touch, or after the wall clock stepped back by up to 24 hours, counts
the entry as just used: it is never idle because of that, and eviction ranks
it hottest. A step back of more than 24 hours is not covered. A `tti` longer
than about 48.7 days is capped at that, and idle time, both for `tti` and for
the eviction ranking in every mode with a capacity, is exact only up to that
span: an entry that sat unswept for longer is not guaranteed to stay deleted.

## Per mode

| Mode | A reader sees | After a node's partition heals |
|---|---|---|
| `Local` | its own writes | nothing to converge |
| `Invalidation` | its own copy, dropped when any node writes the key, or under a merging resolver folded with that write | copies it missed invalidations for stay until their TTL |
| `Replicated` | its own copy of every entry, a moment behind a write elsewhere | anti-entropy reconciles every entry by version |
| `Distributed` | via `fetch`, the owners' copy | owners reconcile by version; each side of the partition accepted writes for its own view |

An `Invalidation` cache runs no anti-entropy and sends a value only when its
resolver merges, so a TTL is its bound on staleness. Set one on every
`Invalidation` cache.

A `Distributed` cache has no quorum. During a partition each side computes
its own owners and accepts writes, and the two sides settle by version
when it heals. With two owners, both owners of one part failing inside
one rebalance window lose that part; set `owners` to 3 or more where
that matters. A crash counts as a failure until the failure detector drops
the node from the live set. A graceful shutdown does not: the leaving node
first reconciles every part it owns with each live co-owner, within
`state_transfer_budget`, then announces its departure, and ownership moves
at the announcement.

### Reads while ownership moves

While `Distributed` nodes join, leave or crash, `fetch` answers one of
three things for a key:

- its current value, from an owner that holds it;
- `Ok(None)`, only when the key is absent or deleted;
- `CacheError::FetchUnavailable`, when no reachable owner can vouch for the
  key yet, for example while every owner is still pulling its part.

A read never returns a miss for a key a live owner holds. It can return a
stale value, an older one or a key's value from before its delete, while
the nodes' views of who owns the key disagree: a node that gave the key's
part up keeps its copy through the disown grace, and a reader whose view
has not caught up still asks it. Writes and deletes made meanwhile reach
only the new owners, so that copy lacks them. Nothing stale is written
back, the copy is dropped when the grace ends, and once every view agrees
the key reads its current value. sundog keeps answering here rather than
failing reads until membership settles. A part a node has just gained, or regained after losing it,
counts as cold until its pull lands, so the node asks the other owners
rather than answering a miss from an incomplete copy. A pull that runs out
`state_transfer_budget` is retried every anti-entropy interval until it
lands, and the part reads as cold until then. The same holds when
the failure detector drops a key's other owners and leaves a node owning its
part alone: an owner dropped that way can be stalled or across a partition
and still hold the key, so the part stays cold and the node answers
`FetchUnavailable` for up to `state_transfer_budget`, 20 seconds by default.
An owner back within that bound hands the part over as a join does; past
it, the node treats the owners as gone and serves what it holds. A regained part whose
old copy is still here does not answer a hit locally either, since writes
and deletes went to the other owners while the node did not own it. `get` reads only this node's
copy and makes none of these promises off an owner that is still pulling.
The [churn oracle](verification.md) checks exactly this list.
`Cache::explain` shows which of these cases a read of one key is in.

## What sundog does not promise

- **Durability.** A cache that loses every node loses its data. The spill
  tier's warm reopen keeps data across a clean restart of one node, not a
  crash.
- **Read-your-writes across nodes.** A write on one node reaches the others
  shortly after, not before the call returns.
- **Mutual exclusion.** Gossip membership has no quorum, so a lock or a
  lease built on sundog can have two holders. Use a consensus system for
  those.
