# A session store

Sessions in a `Replicated` cache: every node holds every session, so any
node answers any request from memory, and a logout on one node ends the
session everywhere.

```rust
{{#include ../../cookbook/src/sessions.rs:types}}
```

## The store

```rust
{{#include ../../cookbook/src/sessions.rs:store}}
```

- **Each session gets its lifetime at login** through `insert_with_ttl`,
  and it expires at the same instant on every node. `refresh` restarts it.
- **A logout is a tombstone.** It outvotes the login on every node, and a
  node that was partitioned away during the logout cannot bring the
  session back when it returns.
- **Memory is bounded by the login rate times the lifetime**, since a
  `Replicated` cache takes no `max_capacity` without a spill tier.

## Sliding sessions

`refresh` keeps an active user signed in: `expire` restarts the session's
lifetime without rewriting the session. It writes the stored session back
stamped as the successor of the login it read, so a logout stamped after
that login, on any node, still outranks it. A refresh racing a logout never
signs the user back in, and a refresh after the logout arrives returns
`false`.

Rewriting the session with `insert_with_ttl` instead stamps it with a
fresh version, newer than a logout on another node that has not reached
this one yet, and the rewrite wins. Extend a session with `expire`, never
with a second insert.

## Wiring it into a service

Read the session id from a cookie or header in an extractor or middleware,
call `session`, and reject the request when it returns `None`. Generate
ids from a cryptographically secure random source with at least 128 bits of
entropy.
