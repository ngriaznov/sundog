//! Checkpoint snapshot files: the on-disk constants, serialization, atomic write and the warm-reopen eligibility check.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use xxhash_rust::xxh3::Xxh3Default;

use crate::hlc::Hlc;
use crate::node::NodeId;

use super::{SnapshotEntry, SnapshotHeader, SpillLoc};

/// Corruption/format-skew guard at the front of every [`SnapshotHeader`],
/// distinct from [`SPILL_MAGIC`] so a snapshot and a region file can't be
/// confused even if misnamed.
const SNAPSHOT_MAGIC: u32 = u32::from_le_bytes(*b"SPLS");
/// Format version stamped into every snapshot, bumped when its binary
/// layout changes. A mismatch is `"stale_snapshot"`, distinct from
/// `"no_snapshot"` (fails to parse at all).
pub(super) const SNAPSHOT_FORMAT_VERSION: u32 = 1;
/// File name a tier's checkpoint snapshot is written under, inside
/// `cfg.dir.join(cache_name)`.
const SNAPSHOT_FILE_NAME: &str = "snapshot";
/// Name [`write_snapshot_atomic`] writes bytes under before renaming to
/// [`SNAPSHOT_FILE_NAME`]; a crash in between leaves this file and no real
/// one, so the next reopen sees `"no_snapshot"`.
const SNAPSHOT_TMP_FILE_NAME: &str = "snapshot.tmp";
/// Byte length of every [`SnapshotHeader`]: `checksum`(8) + `magic`(4) +
/// `format_version`(4) + `region_bytes`(8) + `region_count`(4) +
/// `closed_at_ms`(8) + `entry_count`(8).
const SNAPSHOT_HEADER_LEN: usize = 44;
/// Byte length of one [`SnapshotEntry`]'s fixed portion, before its
/// variable-length key bytes: `wall_ms`(8) + `logical`(4) + `node`(8) +
/// `expires_at_ms`(8) + `region`(4) + `offset`(4) + `len`(4) +
/// `generation`(4) + `key_len`(4).
const SNAPSHOT_ENTRY_FIXED_LEN: usize = 48;

/// Removes a leftover checkpoint snapshot, temp name included, since a
/// cold open recreates every region from scratch. A missing file is not
/// an error.
pub(super) fn remove_stale_snapshot_files(dir: &Path) -> io::Result<()> {
    for path in [snapshot_path(dir), snapshot_tmp_path(dir)] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// The checkpoint snapshot's path inside a tier's own directory.
pub(super) fn snapshot_path(dir: &Path) -> PathBuf {
    dir.join(SNAPSHOT_FILE_NAME)
}

/// [`write_snapshot_atomic`]'s temporary-name counterpart to
/// [`snapshot_path`].
pub(super) fn snapshot_tmp_path(dir: &Path) -> PathBuf {
    dir.join(SNAPSHOT_TMP_FILE_NAME)
}

/// `xxh3_64` over every byte but the leading `checksum` field, mirroring
/// [`SpillRecordHeader`]'s checksum reasoning.
fn snapshot_checksum(buf: &[u8]) -> u64 {
    let mut hasher = Xxh3Default::new();
    hasher.update(&buf[size_of::<u64>()..]);
    hasher.digest()
}

/// Serializes `entries` (live, un-tombstoned, un-expired pointers) into
/// one snapshot buffer: a fixed [`SnapshotHeader`] naming
/// `region_bytes`/`region_count`/`closed_at_ms`, followed by one
/// variable-length record per entry. Manual serialization, not
/// `zerocopy`, since keys vary in length. `checksum` is computed last and
/// patched into the first eight bytes.
pub(super) fn build_snapshot_bytes(
    entries: &[(Bytes, Hlc, Option<u64>, SpillLoc)],
    region_bytes: u64,
    region_count: u32,
    closed_at_ms: u64,
) -> Vec<u8> {
    let mut buf =
        Vec::with_capacity(SNAPSHOT_HEADER_LEN + entries.len() * (SNAPSHOT_ENTRY_FIXED_LEN + 16));
    buf.extend_from_slice(&0u64.to_le_bytes()); // checksum placeholder
    buf.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&region_bytes.to_le_bytes());
    buf.extend_from_slice(&region_count.to_le_bytes());
    buf.extend_from_slice(&closed_at_ms.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    debug_assert_eq!(buf.len(), SNAPSHOT_HEADER_LEN);
    for (key, ver, expires_at_ms, loc) in entries {
        buf.extend_from_slice(&ver.wall_ms.to_le_bytes());
        buf.extend_from_slice(&ver.logical.to_le_bytes());
        buf.extend_from_slice(&ver.node.as_u64().to_le_bytes());
        buf.extend_from_slice(&expires_at_ms.unwrap_or(u64::MAX).to_le_bytes());
        buf.extend_from_slice(&loc.region.to_le_bytes());
        buf.extend_from_slice(&loc.offset.to_le_bytes());
        buf.extend_from_slice(&loc.len.to_le_bytes());
        buf.extend_from_slice(&loc.generation.to_le_bytes());
        buf.extend_from_slice(&(u32::try_from(key.len()).unwrap_or(u32::MAX)).to_le_bytes());
        buf.extend_from_slice(key);
    }
    let checksum = snapshot_checksum(&buf);
    buf[..size_of::<u64>()].copy_from_slice(&checksum.to_le_bytes());
    buf
}

/// Writes `bytes` to a temp file, then renames it to [`snapshot_path`]: a
/// crash in between leaves only the temp file, so reopen sees
/// `"no_snapshot"`. No fsync. A failed write or rename is logged and
/// ignored.
pub(super) fn write_snapshot_atomic(dir: &Path, bytes: &[u8]) {
    let tmp = snapshot_tmp_path(dir);
    if let Err(err) = fs::write(&tmp, bytes) {
        tracing::warn!(
            dir = %dir.display(),
            error = %err,
            "sundog spill: checkpoint snapshot write failed",
        );
        return;
    }
    if let Err(err) = fs::rename(&tmp, snapshot_path(dir)) {
        tracing::warn!(
            dir = %dir.display(),
            error = %err,
            "sundog spill: checkpoint snapshot rename failed",
        );
    }
}

/// Removes `dir`'s checkpoint snapshot after a successful reopen, so a
/// second open with no intervening close takes the cold path instead of
/// replaying against overwritten regions.
pub(super) fn delete_snapshot(dir: &Path) {
    match fs::remove_file(snapshot_path(dir)) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(
                dir = %dir.display(),
                error = %err,
                "sundog spill: checkpoint snapshot delete failed after a warm reopen",
            );
        }
    }
}

/// Parses `buf`, a whole snapshot file's bytes, into a [`SnapshotHeader`]
/// and its [`SnapshotEntry`] list, or `None` if it doesn't check out (too
/// short, bad checksum/magic, truncated). `format_version` isn't checked
/// here, since a future format may not even parse;
/// [`snapshot_eligibility`] checks it after parsing succeeds.
pub(super) fn parse_snapshot(buf: &[u8]) -> Option<(SnapshotHeader, Vec<SnapshotEntry>)> {
    if buf.len() < SNAPSHOT_HEADER_LEN {
        return None;
    }
    let checksum = u64::from_le_bytes(buf[0..8].try_into().ok()?);
    if snapshot_checksum(buf) != checksum {
        return None;
    }
    let magic = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    if magic != SNAPSHOT_MAGIC {
        return None;
    }
    let format_version = u32::from_le_bytes(buf[12..16].try_into().ok()?);
    let region_bytes = u64::from_le_bytes(buf[16..24].try_into().ok()?);
    let region_count = u32::from_le_bytes(buf[24..28].try_into().ok()?);
    let closed_at_ms = u64::from_le_bytes(buf[28..36].try_into().ok()?);
    let entry_count = u64::from_le_bytes(buf[36..44].try_into().ok()?);

    let mut pos = SNAPSHOT_HEADER_LEN;
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        if buf.len() < pos + SNAPSHOT_ENTRY_FIXED_LEN {
            return None;
        }
        let wall_ms = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let logical = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let node = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let expires_raw = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let region = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let offset = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let generation = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let key_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if buf.len() < pos + key_len {
            return None;
        }
        let key = Bytes::copy_from_slice(&buf[pos..pos + key_len]);
        pos += key_len;
        entries.push(SnapshotEntry {
            key,
            ver: Hlc {
                wall_ms,
                logical,
                node: NodeId::from(node),
            },
            expires_at_ms: (expires_raw != u64::MAX).then_some(expires_raw),
            loc: SpillLoc {
                region,
                offset,
                len,
                generation,
            },
        });
    }

    Some((
        SnapshotHeader {
            region_bytes,
            closed_at_ms,
            format_version,
            region_count,
        },
        entries,
    ))
}

/// Test-only: writes `dir` a snapshot stale by one format version,
/// everything else intact, so only [`snapshot_eligibility`]'s
/// `"stale_snapshot"` check can fail against it. Used to prove
/// `Shard::attach_spill`'s metric recording hits `reason="stale_snapshot"`
/// through the real call site.
#[cfg(test)]
pub(crate) fn write_stale_snapshot_for_test(
    dir: &Path,
    region_bytes: u64,
    region_count: u32,
    closed_at_ms: u64,
) {
    let mut bytes = build_snapshot_bytes(&[], region_bytes, region_count, closed_at_ms);
    bytes[12..16].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.wrapping_add(1).to_le_bytes());
    let checksum = snapshot_checksum(&bytes);
    bytes[..8].copy_from_slice(&checksum.to_le_bytes());
    write_snapshot_atomic(dir, &bytes);
}

/// [`SpillTier::reopen`]'s eligibility check: a snapshot must be present
/// and parse cleanly (`"no_snapshot"` otherwise), name the current
/// [`SNAPSHOT_FORMAT_VERSION`] (`"stale_snapshot"`), match
/// `region_bytes`/`region_count` (`"config_mismatch"`), and have closed
/// within `tombstone_ttl_ms` of `now_ms` (`"downtime_exceeded"`). `Ok`
/// names the parsed snapshot; `Err` names the [`ReopenOutcome::reason`]
/// to record.
pub(super) fn snapshot_eligibility(
    dir: &Path,
    region_bytes: u64,
    region_count: u32,
    now_ms: u64,
    tombstone_ttl_ms: u64,
) -> Result<(SnapshotHeader, Vec<SnapshotEntry>), &'static str> {
    let bytes = fs::read(snapshot_path(dir)).map_err(|_| "no_snapshot")?;
    let (header, entries) = parse_snapshot(&bytes).ok_or("no_snapshot")?;
    if header.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err("stale_snapshot");
    }
    if header.region_bytes != region_bytes || header.region_count != region_count {
        return Err("config_mismatch");
    }
    if header.closed_at_ms == 0 || now_ms.saturating_sub(header.closed_at_ms) > tombstone_ttl_ms {
        return Err("downtime_exceeded");
    }
    Ok((header, entries))
}
