# sundog

An embedded, replicated, zeroconf cache for Rust, modeled on Infinispan's embedded mode. Instances of a service on the same network discover each other, form a cluster over gossip membership, and keep named caches coherent across nodes via a hybrid-logical-clock versioned last-write-wins model with anti-entropy repair — no consensus, no operator action on join, leave, crash, or partition.

**Under construction.** See `docs/BUILD_PLAN.md` for the design and `docs/HOUSE_RULES.md` for implementation conventions.
