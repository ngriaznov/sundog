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
  and it expires at the same instant on every node.
- **A logout is a tombstone.** It outvotes the login on every node, and a
  node that was partitioned away during the logout cannot bring the
  session back when it returns.
- **Memory is bounded by the login rate times the lifetime**, since a
  `Replicated` cache takes no `max_capacity` without a spill tier.

## A fixed lifetime

The store never extends a session. Rewriting a session with a fresh TTL
would stamp it with a newer version than a logout on another node that
has not reached this one yet, and the rewrite would win. To keep a user
signed in longer, issue a new session id at a refresh point and log the
old one out.

## Wiring it into a service

Read the session id from a cookie or header in an extractor or middleware,
call `session`, and reject the request when it returns `None`. Generate
ids from a cryptographically secure random source with at least 128 bits of
entropy.
