//! Anti-entropy serving on `Engine`: bucket and part listings, digests and the resident snapshot.

use std::hash::Hash;
use std::sync::atomic::Ordering;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::store::{BUCKET_COUNT, BucketEntries, BucketPart, KeyVersion, PART_COUNT, PartEntries};
use crate::wire::WireRecord;

use super::expiry::decode_expiry;
use super::keys::{digest_slot, hash_key_bytes, part_index_from_hash};
use super::record::{record_key, record_key_bytes, record_value_bytes};
use super::{Engine, is_resident};

impl<K, V> Engine<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// [`Engine::record_for`] for every requested bucket, one stripe lock each:
    /// O(bucket size) per bucket, not O(shard size). A bucket at or past
    /// [`BUCKET_COUNT`], which only a misbehaving peer names, yields nothing.
    pub(crate) fn collect_buckets(&self, wanted: &[u16], now_ms: u64) -> BucketEntries {
        wanted
            .iter()
            .filter(|&&bucket| usize::from(bucket) < BUCKET_COUNT)
            .map(|&bucket| {
                let stripe = self.stripes[usize::from(bucket)].read();
                let mut entries = Vec::with_capacity(stripe.live.len() + stripe.tombstones.len());
                entries.extend(
                    stripe
                        .live
                        .iter()
                        .filter(|live| {
                            !self.is_absent(
                                live,
                                record_key(&live.record),
                                &stripe.long_ttl,
                                now_ms,
                            )
                        })
                        .map(|live| KeyVersion {
                            key: record_key_bytes(&live.record),
                            version: live.ver(),
                        }),
                );
                entries.extend(stripe.tombstones.iter().map(|(key_bytes, t)| KeyVersion {
                    key: key_bytes.clone(),
                    version: t.ver,
                }));
                (bucket, entries)
            })
            .collect()
    }

    /// [`Engine::collect_buckets`] at part granularity: a [`KeyVersion`] for
    /// every live entry and un-GC'd tombstone in each requested
    /// [`BucketPart`], one stripe read lock per distinct bucket in `wanted`.
    /// An out-of-range bucket ([`BUCKET_COUNT`] or past) or part
    /// ([`PART_COUNT`] or past) is skipped rather than indexed.
    pub(crate) fn collect_parts(&self, wanted: &[BucketPart], now_ms: u64) -> PartEntries {
        let mut by_bucket: std::collections::BTreeMap<u16, Vec<u8>> =
            std::collections::BTreeMap::new();
        for &BucketPart { bucket, part } in wanted {
            if usize::from(bucket) >= BUCKET_COUNT || usize::from(part) >= PART_COUNT {
                continue;
            }
            by_bucket.entry(bucket).or_default().push(part);
        }
        let mut out = Vec::new();
        for (bucket, mut parts) in by_bucket {
            parts.sort_unstable();
            parts.dedup();
            // One pass over the stripe, hashing each key once, routing every
            // entry to its part's slot; a bucket's 64 parts cost one listing.
            let mut slot_of_part = [usize::MAX; PART_COUNT];
            for (slot, &part) in parts.iter().enumerate() {
                slot_of_part[usize::from(part)] = slot;
            }
            let mut per_part: Vec<Vec<KeyVersion>> = vec![Vec::new(); parts.len()];
            let stripe = self.stripes[usize::from(bucket)].read();
            let live_entries = stripe
                .live
                .iter()
                .filter(|live| {
                    !self.is_absent(live, record_key(&live.record), &stripe.long_ttl, now_ms)
                })
                .map(|live| (record_key_bytes(&live.record), live.ver()));
            let tombstone_entries = stripe
                .tombstones
                .iter()
                .map(|(key_bytes, t)| (key_bytes.clone(), t.ver));
            for (key_bytes, ver) in live_entries.chain(tombstone_entries) {
                let slot = slot_of_part[part_index_from_hash(hash_key_bytes(key_bytes.as_ref()))];
                if slot != usize::MAX {
                    per_part[slot].push(KeyVersion {
                        key: key_bytes.clone(),
                        version: ver,
                    });
                }
            }
            drop(stripe);
            out.extend(
                parts
                    .into_iter()
                    .zip(per_part)
                    .map(|(part, entries)| (BucketPart { bucket, part }, entries)),
            );
        }
        out
    }

    /// This engine's current per-bucket XOR digests, `(bucket, digest)` for all
    /// buckets. Each bucket digest is the XOR of its [`PART_COUNT`] part
    /// digests, computed on demand.
    pub(crate) fn digests(&self) -> Vec<(u16, u64)> {
        (0..BUCKET_COUNT)
            .map(|bucket| {
                let idx =
                    u16::try_from(bucket).expect("invariant: index < BUCKET_COUNT fits in u16");
                let digest = (0..PART_COUNT).fold(0u64, |acc, part| {
                    acc ^ self.digest[digest_slot(bucket, part)].load(Ordering::Relaxed)
                });
                (idx, digest)
            })
            .collect()
    }

    /// The number of live entries plus un-GC'd tombstones in `bucket`,
    /// without cloning or enumerating any of them: `O(1)` past the stripe's
    /// read lock. A bucket at or past [`BUCKET_COUNT`] yields `0`. Lets an
    /// anti-entropy responder decide the part-digest threshold without
    /// paying to materialize a bucket's full listing first.
    pub(crate) fn bucket_len(&self, bucket: u16) -> usize {
        if usize::from(bucket) >= BUCKET_COUNT {
            return 0;
        }
        let stripe = self.stripes[usize::from(bucket)].read();
        stripe.live.len() + stripe.tombstones.len()
    }

    /// Every stripe's current `live` arena capacity, in stripe order.
    pub(crate) fn stripe_capacities(&self) -> Vec<usize> {
        self.stripes
            .iter()
            .map(|s| s.read().live.capacity())
            .collect()
    }

    /// This engine's current part digests for `bucket`: [`PART_COUNT`] values,
    /// one per part, in ascending part order. A bucket at or past
    /// [`BUCKET_COUNT`], which only a misbehaving peer names, yields an empty
    /// vec.
    pub(crate) fn part_digests(&self, bucket: u16) -> Vec<u64> {
        if usize::from(bucket) >= BUCKET_COUNT {
            return Vec::new();
        }
        (0..PART_COUNT)
            .map(|part| self.digest[digest_slot(usize::from(bucket), part)].load(Ordering::Relaxed))
            .collect()
    }

    /// Every resident live entry and tombstone as [`WireRecord`]s, for
    /// [`super::super::ShardOps::snapshot_chunks`]. A currently-spilled entry is
    /// never included here: see [`Engine::snapshot_spilled`], its sibling,
    /// for the pointers a spill-aware caller reads off-lock and folds in.
    ///
    pub(crate) fn snapshot_records(&self, now_ms: u64) -> Vec<WireRecord> {
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
                    .filter(|&live| is_resident(live))
                    .map(|live| WireRecord {
                        key: record_key_bytes(&live.record),
                        value: Some(record_value_bytes(&live.record)),
                        ver: live.ver(),
                        expires_at_ms: decode_expiry(
                            live.wall_ms,
                            live.expiry,
                            record_key(&live.record),
                            &stripe.long_ttl,
                        ),
                    }),
            );
            out.extend(stripe.tombstones.iter().map(|(key_bytes, t)| WireRecord {
                key: key_bytes.clone(),
                value: None,
                ver: t.ver,
                expires_at_ms: None,
            }));
        }
        out
    }
}
