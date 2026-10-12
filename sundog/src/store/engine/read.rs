//! Point reads on `Engine`: lookups by key or key bytes, lifetimes, key enumeration and the local record view.

use std::hash::Hash;
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::explain::LocalRecord;
use crate::hlc::Hlc;
use crate::store::Ttl;
use crate::wire::WireRecord;

#[cfg(all(doc, feature = "spill"))]
use super::EntryState;
use super::expiry::{decode_expiry, lapse_cause};
use super::keys::{encode_key_for_read, hash_key_bytes, stripe_index_from_hash};
use super::record::{decode_key, decode_value, record_key, record_value, record_value_bytes};
use super::{Engine, is_resident, is_spilled};

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Reads `key`: a stripe read lock, a hashbrown lookup by the key's
    /// postcard-encoded bytes, and a value decode. No mutation beyond the
    /// recency touch, when configured.
    pub(crate) fn get(&self, key: &K, now_ms: u64) -> Option<V> {
        let key_buf = encode_key_for_read(key).ok()?;
        let key_bytes = key_buf.as_slice();
        self.get_by_bytes(key_bytes, hash_key_bytes(key_bytes), now_ms)
    }

    /// [`Engine::get`] for a key already encoded and hashed, so a caller that
    /// holds both, as the `get_or_load` loop does, skips re-encoding.
    ///
    /// `None` for a currently-[`EntryState::Spilled`] entry too. A spill-aware
    /// caller, `get`/`get_or_load`, checks [`Engine::spilled_loc`] next; a
    /// spill-blind one, `get_sync`, treats this like a miss, per its
    /// documented contract.
    pub(crate) fn get_by_bytes(&self, key_bytes: &[u8], hash: u64, now_ms: u64) -> Option<V> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        if !is_resident(live) {
            return None;
        }
        self.touch(live, now_ms);
        Some(decode_value(record_value(live.record.as_slice())))
    }

    /// [`Engine::get_by_bytes`] that also returns the entry's version and
    /// absolute expiry, `None` for none: what refresh-ahead checks a read
    /// against.
    pub(crate) fn get_with_lifetime(
        &self,
        key_bytes: &[u8],
        hash: u64,
        now_ms: u64,
    ) -> Option<(V, Hlc, Option<u64>)> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) || !is_resident(live) {
            return None;
        }
        self.touch(live, now_ms);
        let deadline = decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl);
        Some((
            decode_value(record_value(live.record.as_slice())),
            live.ver(),
            deadline,
        ))
    }

    /// The version and absolute expiry of the key's live entry, resident or
    /// spilled, and `None` with no live entry. Not an access.
    pub(crate) fn lifetime_of(
        &self,
        key_bytes: &[u8],
        hash: u64,
        now_ms: u64,
    ) -> Option<(Hlc, Option<u64>)> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        Some((
            live.ver(),
            decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl),
        ))
    }

    /// What this engine stores for `key_bytes` at `now_ms`: a tombstone,
    /// a live entry resident or spilled, an entry a read no longer returns
    /// and a sweep has not reclaimed, or nothing. One stripe read lock, no
    /// recency touch, no value decode, no disk read.
    pub(crate) fn inspect(&self, key_bytes: &[u8], now_ms: u64) -> LocalRecord {
        let hash = hash_key_bytes(key_bytes);
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        if let Some(t) = stripe.tombstones.get(key_bytes) {
            return LocalRecord::Tombstone { version: t.ver };
        }
        let Some(live) = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)
        else {
            return LocalRecord::Absent;
        };
        let version = live.ver();
        let expires_at_ms = decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl);
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            LocalRecord::Lapsed {
                version,
                expires_at_ms,
                cause: lapse_cause(expires_at_ms, now_ms),
            }
        } else {
            LocalRecord::Live {
                version,
                expires_at_ms,
                spilled: is_spilled(live),
            }
        }
    }

    /// Whether `key` has a live, unexpired, non-idle entry.
    pub(crate) fn contains_key(&self, key: &K, now_ms: u64) -> bool {
        let Ok(key_buf) = encode_key_for_read(key) else {
            return false;
        };
        let key_bytes = key_buf.as_slice();
        let hash = hash_key_bytes(key_bytes);
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let Some(live) = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)
        else {
            return false;
        };
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return false;
        }
        self.touch(live, now_ms);
        true
    }

    /// Every live, unexpired, non-idle key. O(entries): a full pass over every
    /// stripe.
    pub(crate) fn keys(&self, now_ms: u64) -> Vec<K> {
        let mut out = Vec::new();
        for stripe_lock in &self.stripes {
            let stripe = stripe_lock.read();
            out.extend(
                stripe
                    .live
                    .iter()
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .map(|live| decode_key(record_key(&live.record))),
            );
        }
        out
    }

    /// [`Engine::keys`] one stripe at a time: each stripe's live keys are
    /// cloned under its read lock, then `f` runs on them with no lock held.
    pub(crate) fn for_each_key(&self, now_ms: u64, mut f: impl FnMut(K)) {
        for stripe_lock in &self.stripes {
            let stripe_keys: Vec<K> = {
                let stripe = stripe_lock.read();
                stripe
                    .live
                    .iter()
                    .filter(|live| {
                        !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                    })
                    .map(|live| decode_key(record_key(&live.record)))
                    .collect()
            };
            for key in stripe_keys {
                f(key);
            }
        }
    }

    /// The full [`WireRecord`] for `key_bytes`, present entry or tombstone
    /// alike. `None` for a currently-spilled entry: the fan-out records path
    /// this feeds skips it, correct because a peer's next anti-entropy
    /// round repairs it.
    pub(crate) fn record_for(&self, key_bytes: &[u8], now_ms: u64) -> Option<WireRecord> {
        let hash = hash_key_bytes(key_bytes);
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        if let Some(t) = stripe.tombstones.get(key_bytes) {
            return Some(WireRecord {
                key: Bytes::copy_from_slice(key_bytes),
                value: None,
                ver: t.ver,
                expires_at_ms: None,
            });
        }
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        is_resident(live).then(|| WireRecord {
            key: Bytes::copy_from_slice(key_bytes),
            value: Some(record_value_bytes(&live.record)),
            ver: live.ver(),
            expires_at_ms: decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl),
        })
    }

    /// The lifetime left at `now_ms` on the key's live entry, resident or
    /// spilled, and `None` with no live entry. Not an access: the recency
    /// stamp stays as it was.
    pub(crate) fn ttl_of(&self, key_bytes: &[u8], hash: u64, now_ms: u64) -> Option<Ttl> {
        let stripe = self.stripes[stripe_index_from_hash(hash)].read();
        let live = stripe
            .live
            .find(hash, |l| record_key(&l.record) == key_bytes)?;
        if self.is_absent(live, key_bytes, &stripe.long_ttl, now_ms) {
            return None;
        }
        Some(
            match decode_expiry(live.wall_ms, live.expiry, key_bytes, &stripe.long_ttl) {
                None => Ttl::Never,
                Some(deadline) => {
                    Ttl::Remaining(Duration::from_millis(deadline.saturating_sub(now_ms)))
                }
            },
        )
    }
}
