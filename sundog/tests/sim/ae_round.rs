//! The anti-entropy round driver: sketch decoding, mismatch classification and per-round resolution.

use crate::support::{NET_TIMEOUT, cache_name, kv_to_tuples};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use sundog::hlc::Hlc;
use sundog::net::{AeMismatch, AePartReply, Mesh, MsgClass};
use sundog::node::NodeId;
use sundog::store::{BucketPart, KeyVersion, ShardOps};
use sundog::wire::Msg;
use xxhash_rust::xxh3::xxh3_64;

/// Decodes one sketch (bucket-scoped or part-scoped alike) against
/// `local_entries`: on success, classifies the peel via
/// [`sundog::diff_decoded`] into `push_keys`/`pull_hashes`; on failure,
/// queues `bucket` for the `Mesh::ae_entries` listing fallback. Shared by
/// [`classify_ae_mismatches`]'s `AeMismatch::Sketch` arm and
/// [`resolve_wanted_parts`]'s `AePartReply::Sketch` arm.
fn decode_sketch_into(
    bucket: u16,
    cells: Vec<sundog::wire::Cell>,
    local_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
    merging: bool,
) {
    let mut local_sketch = sundog::Iblt::new(cells.len());
    for (key, ver) in local_entries {
        local_sketch.insert(xxh3_64(key), *ver);
    }
    match local_sketch
        .subtract(&sundog::Iblt::from_cells(cells))
        .and_then(sundog::Iblt::peel)
    {
        Ok(decoded) => {
            let mut hashes = Vec::new();
            sundog::diff_decoded(local_entries, &decoded, push_keys, &mut hashes, merging);
            if !hashes.is_empty() {
                pull_hashes.push((bucket, hashes));
            }
        }
        Err(_) => undecodable_buckets.push(bucket),
    }
}

/// Classifies one round's `AeMismatch` replies into push/pull keys, hash
/// pulls, undecodable buckets, and wanted `(bucket, part)` pairs: the match
/// [`ae_round_with_sketch`] runs over `mismatched`, split out to keep that
/// function's own line count down. `bucket_listings` counts every
/// `AeMismatch::Bucket` reply seen.
#[allow(clippy::too_many_arguments)]
async fn classify_ae_mismatches<S: ShardOps>(
    shard: &S,
    mismatched: Vec<AeMismatch>,
    bucket_listings: Option<&AtomicUsize>,
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
    wanted_parts: &mut Vec<(u16, u8)>,
    merging: bool,
) {
    for mismatch in mismatched {
        match mismatch {
            AeMismatch::Bucket(bucket, peer_entries) => {
                if let Some(counter) = bucket_listings {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                diff_bucket(
                    shard,
                    bucket,
                    &kv_to_tuples(peer_entries),
                    push_keys,
                    pull_keys,
                    merging,
                )
                .await;
            }
            AeMismatch::Sketch(bucket, cells) => {
                let local_entries = kv_to_tuples(ShardOps::bucket_entries(shard, bucket).await);
                decode_sketch_into(
                    bucket,
                    cells,
                    &local_entries,
                    push_keys,
                    pull_hashes,
                    undecodable_buckets,
                    merging,
                );
            }
            AeMismatch::PartDigests(bucket, digests) => {
                let local_parts = ShardOps::part_digests(shard, vec![bucket])
                    .await
                    .into_iter()
                    .find(|p| p.bucket == bucket)
                    .map_or_else(Vec::new, |p| p.digests);
                for part in sundog::mismatched_parts(&local_parts, &digests) {
                    wanted_parts.push((bucket, part));
                }
            }
            // `AeMismatch` is `#[non_exhaustive]`: a reply shape this
            // scenario does not model is left to the full listing.
            other => undecodable_buckets.push(other.bucket()),
        }
    }
}

/// Requests every `(bucket, part)` pair in `wanted_parts` via
/// `Mesh::ae_parts` and classifies each reply, the part-path counterpart of
/// [`classify_ae_mismatches`]'s bucket-scoped arms, split out for the same
/// reason. Returns `false` on a failed exchange, mirroring
/// `run_round_against`'s own give-up-this-round handling.
#[allow(clippy::too_many_arguments)]
async fn resolve_wanted_parts<S: ShardOps>(
    mesh: &Mesh,
    shard: &S,
    peer: NodeId,
    wanted_parts: Vec<(u16, u8)>,
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    pull_hashes: &mut Vec<(u16, Vec<u64>)>,
    undecodable_buckets: &mut Vec<u16>,
    merging: bool,
) -> bool {
    let wanted_bucket_parts: Vec<BucketPart> = wanted_parts
        .iter()
        .map(|&(bucket, part)| BucketPart { bucket, part })
        .collect();
    match tokio::time::timeout(
        NET_TIMEOUT,
        mesh.ae_parts(peer, cache_name(), wanted_parts.clone()),
    )
    .await
    {
        Ok(Ok(replies)) => {
            let local_part_entries = ShardOps::entries_for_parts(shard, wanted_bucket_parts).await;
            let local_by_part: HashMap<(u16, u8), Vec<(Bytes, Hlc)>> = local_part_entries
                .into_iter()
                .map(|(bp, entries)| ((bp.bucket, bp.part), kv_to_tuples(entries)))
                .collect();
            for reply in replies {
                match reply {
                    AePartReply::Listing {
                        bucket,
                        part,
                        entries: peer_entries,
                    } => {
                        let local_entries = local_by_part
                            .get(&(bucket, part))
                            .cloned()
                            .unwrap_or_default();
                        diff_part(
                            &local_entries,
                            &kv_to_tuples(peer_entries),
                            push_keys,
                            pull_keys,
                            merging,
                        );
                    }
                    AePartReply::Sketch {
                        bucket,
                        part,
                        cells,
                    } => {
                        let local_entries = local_by_part
                            .get(&(bucket, part))
                            .cloned()
                            .unwrap_or_default();
                        decode_sketch_into(
                            bucket,
                            cells,
                            &local_entries,
                            push_keys,
                            pull_hashes,
                            undecodable_buckets,
                            merging,
                        );
                    }
                    // `AePartReply` is `#[non_exhaustive]`: an unmodelled
                    // reply shape is left to the whole-bucket fallback.
                    other => undecodable_buckets.push(other.bucket()),
                }
            }
            true
        }
        Ok(Err(_)) | Err(_) => false,
    }
}

/// One anti-entropy round against `peer`, reimplementing
/// `run_round_against`'s digest-exchange logic over the same public calls,
/// including the sketch path: a bucket answered as `AeMismatch::Sketch`
/// gets a local comparison sketch built from `bucket_entries`, subtracted
/// against the received one, and peeled via [`sundog::Iblt`]; a decode
/// classifies into pushes and hash-pulls via [`sundog::diff_decoded`], the
/// same function `cluster::anti_entropy::handle_sketch_mismatch` calls, and
/// an `Undecodable` one queues for the `Mesh::ae_entries` listing fallback;
/// and the part path, a bucket answered as `AeMismatch::PartDigests` is
/// compared against this node's own `ShardOps::part_digests` for the same
/// bucket via [`sundog::mismatched_parts`], the differing `(bucket, part)`
/// pairs requested in one `Mesh::ae_parts` call, and each reply classified
/// the same way as the bucket path above but scoped to
/// `ShardOps::entries_for_parts`. `bucket_listings` counts every
/// `AeMismatch::Bucket`/`Msg::AeBucket`-shaped reply this round receives, so
/// a scenario can assert the part path never carries one. Exactly mirrors
/// `run_round_against`'s own shape.
#[allow(
    clippy::too_many_lines,
    reason = "one round's whole digest-exchange-through-repair sequence reads best kept together, \
              mirroring cluster::anti_entropy::run_round_against's own allow"
)]
pub(crate) async fn ae_round_with_sketch<S: ShardOps>(
    mesh: &Mesh,
    shard: &S,
    peer: NodeId,
    bucket_listings: Option<&AtomicUsize>,
) -> bool {
    // Read once per round, exactly as `run_round_against` reads
    // `shard.merges()`: a merging resolver has both sides exchange a
    // mismatched key in the same round instead of only the greater version
    // pushing to the lesser side.
    let merging = ShardOps::merges(shard);
    let local_buckets: Vec<(u16, u64)> = ShardOps::digests(shard)
        .await
        .into_iter()
        .map(|bd| (bd.bucket, bd.digest))
        .collect();
    let Ok(Ok(mismatched)) = tokio::time::timeout(
        NET_TIMEOUT,
        mesh.ae_round(peer, cache_name(), local_buckets),
    )
    .await
    else {
        return false;
    };

    let mut push_keys = Vec::new();
    let mut pull_keys = Vec::new();
    let mut pull_hashes: Vec<(u16, Vec<u64>)> = Vec::new();
    let mut undecodable_buckets = Vec::new();
    let mut wanted_parts: Vec<(u16, u8)> = Vec::new();

    classify_ae_mismatches(
        shard,
        mismatched,
        bucket_listings,
        &mut push_keys,
        &mut pull_keys,
        &mut pull_hashes,
        &mut undecodable_buckets,
        &mut wanted_parts,
        merging,
    )
    .await;

    if !wanted_parts.is_empty()
        && !resolve_wanted_parts(
            mesh,
            shard,
            peer,
            wanted_parts,
            &mut push_keys,
            &mut pull_keys,
            &mut pull_hashes,
            &mut undecodable_buckets,
            merging,
        )
        .await
    {
        return false;
    }

    if !undecodable_buckets.is_empty() {
        match tokio::time::timeout(
            NET_TIMEOUT,
            mesh.ae_entries(peer, cache_name(), undecodable_buckets),
        )
        .await
        {
            Ok(Ok(fallback)) => {
                for (bucket, peer_entries) in fallback {
                    diff_bucket(
                        shard,
                        bucket,
                        &kv_to_tuples(peer_entries),
                        &mut push_keys,
                        &mut pull_keys,
                        merging,
                    )
                    .await;
                }
            }
            Ok(Err(_)) | Err(_) => return false,
        }
    }

    if !push_keys.is_empty() {
        for rec in ShardOps::records_for(shard, push_keys).await {
            mesh.send(
                peer,
                MsgClass::Replicate,
                Msg::Replicate {
                    cache: cache_name(),
                    rec,
                },
            );
        }
    }
    let mut ok = true;
    if !pull_keys.is_empty() {
        match tokio::time::timeout(NET_TIMEOUT, mesh.ae_pull(peer, cache_name(), pull_keys)).await {
            Ok(Ok(records)) => {
                for rec in records {
                    ShardOps::apply_remote(shard, rec).await;
                }
            }
            Ok(Err(_)) | Err(_) => ok = false,
        }
    }
    for (bucket, hashes) in pull_hashes {
        match tokio::time::timeout(
            NET_TIMEOUT,
            mesh.ae_pull_hashes(peer, cache_name(), bucket, hashes),
        )
        .await
        {
            Ok(Ok(records)) => {
                for rec in records {
                    ShardOps::apply_remote(shard, rec).await;
                }
            }
            Ok(Err(_)) | Err(_) => ok = false,
        }
    }
    ok
}

/// Mirrors `cluster::anti_entropy::diff_bucket`: `merging` is
/// `ShardOps::merges(shard)`, read once per round by [`ae_round_with_sketch`]
/// and threaded through every classification call in this file the same way
/// `run_round_against` threads it through its own. `false` keeps the
/// greater-version-only push/pull rule; `true` additionally queues the
/// other direction too for a key present on both sides under different
/// versions, so a merging resolver's two replicas exchange records in one
/// round instead of needing a second round to carry a minted result back.
async fn diff_bucket<S: ShardOps>(
    shard: &S,
    bucket: u16,
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    merging: bool,
) {
    let peer_by_key: HashMap<Bytes, Hlc> = peer_entries.iter().cloned().collect();
    let mut local_keys = HashSet::with_capacity(peer_by_key.len());

    for KeyVersion {
        key,
        version: local_ver,
    } in ShardOps::bucket_entries(shard, bucket).await
    {
        local_keys.insert(key.clone());
        match peer_by_key.get(&key) {
            Some(&peer_ver) if local_ver != peer_ver => {
                if local_ver > peer_ver || merging {
                    push_keys.push(key.clone());
                }
                if local_ver < peer_ver || merging {
                    pull_keys.push(key.clone());
                }
            }
            Some(_) => {}
            None => push_keys.push(key),
        }
    }
    for (key, _) in peer_entries {
        if !local_keys.contains(key) {
            pull_keys.push(key.clone());
        }
    }
}

/// [`diff_bucket`]'s comparison, but over an already-fetched local entry
/// list rather than a fresh `ShardOps::bucket_entries` call: the part path's
/// counterpart, since `AePartReply::Listing`'s local side comes from one
/// batched `ShardOps::entries_for_parts` call up front. `merging` is
/// [`diff_bucket`]'s same flag.
fn diff_part(
    local_entries: &[(Bytes, Hlc)],
    peer_entries: &[(Bytes, Hlc)],
    push_keys: &mut Vec<Bytes>,
    pull_keys: &mut Vec<Bytes>,
    merging: bool,
) {
    let peer_by_key: HashMap<&Bytes, Hlc> = peer_entries.iter().map(|(k, v)| (k, *v)).collect();
    let mut local_keys = HashSet::with_capacity(local_entries.len());

    for (key, local_ver) in local_entries {
        local_keys.insert(key);
        match peer_by_key.get(key) {
            Some(&peer_ver) if *local_ver != peer_ver => {
                if *local_ver > peer_ver || merging {
                    push_keys.push(key.clone());
                }
                if *local_ver < peer_ver || merging {
                    pull_keys.push(key.clone());
                }
            }
            Some(_) => {}
            None => push_keys.push(key.clone()),
        }
    }
    for (key, _) in peer_entries {
        if !local_keys.contains(key) {
            pull_keys.push(key.clone());
        }
    }
}
