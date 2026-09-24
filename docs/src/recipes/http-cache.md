# Caching HTTP responses in axum

A middleware that stores successful `GET` responses in a `Replicated`
cache. One node renders a page, and every node serves it from memory until
its TTL runs out or a write purges it.

The cached value holds what a replay needs:

```rust
{{#include ../../cookbook/src/http_cache.rs:types}}
```

## The cache

`Replicated` puts every rendered response on every node, so a page one
node renders is a hit on the others. The TTL bounds both staleness and
memory: the cache holds at most the distinct URLs requested within one
TTL.

```rust
{{#include ../../cookbook/src/http_cache.rs:open}}
```

## The middleware

The middleware keys on the request URI, path and query together. It
buffers only a `200 OK` body whose exact length is known and small, and
passes a streamed or large response through untouched.

```rust
{{#include ../../cookbook/src/http_cache.rs:middleware}}
```

Mount it with `from_fn_with_state`:

```rust
{{#include ../../cookbook/src/http_cache.rs:router}}
```

## Purging

A handler that changes a resource drops its cached page. The removal
reaches every node and outvotes any copy written before it:

```rust
{{#include ../../cookbook/src/http_cache.rs:purge}}
```

## Adapting it

- Responses that vary by user must not share a key. Add the user or the
  relevant headers to the key, or skip caching for authenticated requests.
- Arbitrary query strings leave the URL space without bound, and the cache
  grows by one TTL's worth of distinct URLs. Normalize the key or cache
  only known routes.
- For a response cache too large to replicate, open the cache in
  `Invalidation` mode with a `max_capacity`, and each node keeps only the
  pages it served.
