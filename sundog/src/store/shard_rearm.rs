//! Rearming an entry's lifetime: `expire`, `touch`, `persist`, `ttl_of` and the local rearm
//! they share.

use std::hash::Hash;
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{
    Incoming, Lifetime, LocalRearm, Origin, PartId, Shard, Ttl, encode_key, engine, rearmed_version,
};
use crate::error::{CacheError, CodecError};
use crate::explain::LocalRead;
use crate::hlc::Hlc;
use crate::verdict;
use crate::wire::WireRecord;

#[allow(
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    reason = "most methods here keep async fn even though this backend never awaits"
)]
impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Gives `key`'s live entry a lifespan of `ttl` from now and keeps its
    /// value. The write re-puts the value this shard holds, stamped as its
    /// successor: a write or remove stamped in a later millisecond than that
    /// value supersedes it. A spilled entry's value is read back from disk
    /// first. Returns whether a live entry was here. A `Mode::Distributed`
    /// shard answers `false` for a key in a part it does not hold warm, as
    /// [`Shard::get`] misses there; `Cache::expire` asks the owners instead.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to encode or the stored
    /// value fails to decode.
    pub async fn expire(&self, key: &K, ttl: Duration) -> Result<bool, CacheError> {
        let rearmed = self.rearm_local(key, Lifetime::For(ttl)).await?;
        Ok(rearmed == LocalRearm::Rearmed)
    }

    /// [`Shard::expire`] with the shard's default TTL: the entry's lifespan
    /// restarts from now, and on a shard with no default it stops expiring.
    ///
    /// # Errors
    ///
    /// As [`Shard::expire`].
    pub async fn touch(&self, key: &K) -> Result<bool, CacheError> {
        let rearmed = self.rearm_local(key, Lifetime::Default).await?;
        Ok(rearmed == LocalRearm::Rearmed)
    }

    /// [`Shard::expire`] with no lifespan: the entry stops expiring.
    ///
    /// # Errors
    ///
    /// As [`Shard::expire`].
    pub async fn persist(&self, key: &K) -> Result<bool, CacheError> {
        let rearmed = self.rearm_local(key, Lifetime::Unbounded).await?;
        Ok(rearmed == LocalRearm::Rearmed)
    }

    /// The remaining lifetime of `key`'s live entry in this shard, resident
    /// or spilled, with no disk read and no recency touch. `None` when no
    /// live entry is here, an entry idle past `tti` included. The lifetime
    /// is the TTL's alone: a `tti` idle timeout that ends the entry sooner
    /// is not reflected.
    #[must_use]
    pub fn ttl_of(&self, key: &K) -> Option<Ttl> {
        let key_bytes = encode_key(key).ok()?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        self.engine.ttl_of(key_bytes.as_ref(), hash, self.now_ms())
    }

    /// The absolute expiry `lifetime` stamps now.
    fn deadline_for(&self, lifetime: Lifetime) -> Option<u64> {
        match lifetime {
            Lifetime::For(ttl) => self.expiry_for(Some(ttl)),
            Lifetime::Default => self.expiry_for(None),
            Lifetime::Unbounded => None,
        }
    }

    /// Re-arms `key`'s entry from this shard's own copy: reads the live
    /// value, resident or spilled, and applies it back with `lifetime`'s
    /// deadline at [`rearmed_version`], only while the entry is still at
    /// the version read. An entry that moves on in between has taken a
    /// write stamped after the read, which supersedes the re-arm anyway, so
    /// that still counts as [`LocalRearm::Rearmed`].
    pub(crate) async fn rearm_local(
        &self,
        key: &K,
        lifetime: Lifetime,
    ) -> Result<LocalRearm, CacheError> {
        let key_bytes = encode_key(key)?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let part = PartId::of_key(key_bytes.as_ref());
        if !verdict::reads_local(&self.current_residency(part)) {
            return Ok(LocalRearm::NotHeld);
        }
        self.wait_for_fan_out_room().await;
        // Read again past the wait and before the record, so the decision
        // sees ownership as it is now, and a pull landing during the read
        // cannot clear a cold mark the decision then trusts.
        let residency = self.current_residency(part);
        if !verdict::reads_local(&residency) {
            return Ok(LocalRearm::NotHeld);
        }
        let resident = self
            .engine
            .record_for(key_bytes.as_ref(), self.now_ms())
            .and_then(|rec| rec.value.map(|encoded| (rec.ver, encoded)));
        let base = match resident {
            Some((ver, encoded)) => {
                let value: V = postcard::from_bytes(&encoded).map_err(CodecError::from)?;
                Some((ver, value, encoded))
            }
            None => self.read_spilled_for_rearm(key_bytes.as_ref(), hash).await,
        };
        let Some((base_ver, value, encoded)) = base else {
            return Ok(match verdict::local_verdict(&residency, false) {
                LocalRead::Miss => LocalRearm::Absent,
                _ => LocalRearm::NotHeld,
            });
        };
        let expires_at_ms = self.deadline_for(lifetime);
        let ver = rearmed_version(base_ver, expires_at_ms);
        let applied = self.engine.apply_if_current(
            (
                hash,
                key.clone(),
                key_bytes,
                ver,
                Incoming::Put {
                    value,
                    expires_at_ms,
                    encoded,
                },
            ),
            base_ver,
            self.resolver.as_ref(),
            self.tombstone_ttl_ms,
            self.tombstone_max_ttl_ms,
            self.now_ms(),
        );
        if let Some(outcome) = applied {
            self.observe_derived(ver);
            self.handle_apply_outcome(outcome, Origin::Local, true);
        }
        Ok(LocalRearm::Rearmed)
    }

    /// The key's spilled value, read back for a re-arm. Always `None`
    /// without the `spill` feature.
    #[cfg_attr(
        not(feature = "spill"),
        allow(
            clippy::unused_async,
            clippy::unused_self,
            reason = "the spill build reads the value off disk here"
        )
    )]
    async fn read_spilled_for_rearm(&self, key_bytes: &[u8], hash: u64) -> Option<(Hlc, V, Bytes)> {
        #[cfg(feature = "spill")]
        {
            self.read_spilled_by_bytes(key_bytes, hash).await
        }
        #[cfg(not(feature = "spill"))]
        {
            let _ = (key_bytes, hash);
            None
        }
    }

    /// Re-arms `key` from `base`, a record an owner answered with, when
    /// this shard holds no warm copy of its own: the same write
    /// [`Shard::rearm_local`] makes, applied here if this shard owns the
    /// key's part and forwarded to the owners otherwise. Returns `false`
    /// for a tombstone or an expired record, which has nothing to re-arm.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Codec`] if `key` fails to encode or `base`'s
    /// value fails to decode, and [`CacheError::Closed`] if the shard can no
    /// longer forward.
    pub(crate) async fn forward_rearm(
        &self,
        key: K,
        base: WireRecord,
        lifetime: Lifetime,
    ) -> Result<bool, CacheError> {
        let Some(encoded) = base.value else {
            return Ok(false);
        };
        if base
            .expires_at_ms
            .is_some_and(|deadline| deadline <= self.now_ms())
        {
            return Ok(false);
        }
        let key_bytes = encode_key(&key)?;
        let value: V = postcard::from_bytes(&encoded).map_err(CodecError::from)?;
        let expires_at_ms = self.deadline_for(lifetime);
        let ver = rearmed_version(base.ver, expires_at_ms);
        self.wait_for_fan_out_room().await;
        self.apply_or_forward(
            key,
            key_bytes,
            ver,
            Incoming::Put {
                value,
                expires_at_ms,
                encoded,
            },
        )?;
        Ok(true)
    }
}
