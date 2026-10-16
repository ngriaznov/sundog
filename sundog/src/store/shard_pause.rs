//! The report of a pause hold a [`Shard`] enters.
//!
//! A shard that did not collect tombstones for longer than
//! `engine::expiry::pause_gap_ms` may hold values whose deletes it never saw,
//! so it defers collection and judges arrivals against the cap for
//! `tombstone_ttl` (see `Engine::guarding_pauses`). Each hold start logs one
//! warning with the gap.

use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Shard;

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Logs, at `warn`, that this shard did not collect tombstones for
    /// `gap_ms` and now defers collection for `tombstone_ttl`.
    pub(super) fn warn_paused(&self, gap_ms: u64) {
        tracing::warn!(
            cache = %self.name,
            gap_ms,
            tombstone_ttl_ms = self.tombstone_ttl_ms,
            "this node did not collect tombstones for {gap_ms} ms (a stopped or suspended \
             process, a clock step or a starved task): it defers tombstone collection and \
             judges arriving deletes against tombstone_max_ttl for tombstone_ttl"
        );
    }
}

#[cfg(all(test, not(feature = "sim")))]
impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// The wall-clock reading of the engine's last tombstone collection
    /// (`0` before the first) and the end of its pause hold (`0` when none).
    /// Only an engine with the pause guard on records either.
    pub(crate) fn pause_state(&self) -> (u64, u64) {
        (self.engine.last_gc_ms(), self.engine.pause_hold_until_ms())
    }
}
