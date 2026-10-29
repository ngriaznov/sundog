//! Spill record codec: record checksum and header, positional file I/O, and region file names.

use std::fs::{self, File};
use std::io;
use std::path::Path;

use bytes::Bytes;
use xxhash_rust::xxh3::Xxh3Default;
use zerocopy::FromBytes;

use crate::hlc::Hlc;
use crate::node::NodeId;

#[cfg(doc)]
use super::SpillTier;
use super::{SPILL_MAGIC, SpillJob, SpillRecordHeader, SpilledBytes};

pub(super) fn record_checksum(key_bytes: &[u8], value_bytes: &[u8]) -> u64 {
    let mut hasher = Xxh3Default::new();
    hasher.update(key_bytes);
    hasher.update(value_bytes);
    hasher.digest()
}

pub(super) fn build_header(job: &SpillJob, key_len: u32, value_len: u32) -> SpillRecordHeader {
    SpillRecordHeader {
        checksum: record_checksum(&job.key_bytes, &job.encoded),
        expires_at_ms: job.expires_at_ms.unwrap_or(u64::MAX),
        wall_ms: job.ver.wall_ms,
        node: job.ver.node.as_u64(),
        magic: SPILL_MAGIC,
        key_len,
        value_len,
        logical: job.ver.logical,
    }
}

/// Parses `buf`, one record's bytes as read off disk, into its key bytes
/// alongside [`SpilledBytes`], or `None` for a bad buffer, magic, length,
/// or checksum. [`decode_record`] wraps this, discarding the key, for
/// [`SpillTier::read_at`]'s caller, which already knows the key; reopen's
/// region scan does not.
pub(super) fn decode_record_with_key(buf: &[u8]) -> Option<(Bytes, SpilledBytes)> {
    let (header, key_bytes, value_bytes) = split_record(buf)?;
    if record_checksum(key_bytes, value_bytes) != header.checksum {
        return None;
    }
    let expires_at_ms = (header.expires_at_ms != u64::MAX).then_some(header.expires_at_ms);
    Some((
        Bytes::copy_from_slice(key_bytes),
        SpilledBytes {
            ver: Hlc {
                wall_ms: header.wall_ms,
                logical: header.logical,
                node: NodeId::from(header.node),
            },
            expires_at_ms,
            encoded: Bytes::copy_from_slice(value_bytes),
        },
    ))
}

/// Splits `buf` into its header, key and value, or `None` for a short
/// buffer, a bad magic, or lengths that don't add up to exactly the bytes
/// after the header. Checks no checksum.
pub(super) fn split_record(buf: &[u8]) -> Option<(SpillRecordHeader, &[u8], &[u8])> {
    let (header, rest) = SpillRecordHeader::read_from_prefix(buf).ok()?;
    if header.magic != SPILL_MAGIC {
        return None;
    }
    let key_len = usize::try_from(header.key_len).ok()?;
    let value_len = usize::try_from(header.value_len).ok()?;
    if key_len.checked_add(value_len)? != rest.len() {
        return None;
    }
    let (key_bytes, value_bytes) = rest.split_at(key_len);
    Some((header, key_bytes, value_bytes))
}

/// Parses `buf`, one record's bytes as read off disk, into
/// [`SpilledBytes`], or `None` for anything that doesn't check out: a short
/// buffer, a bad magic, a length mismatch, or a checksum mismatch. Every
/// one of these is treated identically: the record cannot be used. Whether
/// that means it was never written or it was damaged is the caller's to
/// tell, as [`SpillTier::read_record`] does from the pointer's generation.
pub(super) fn decode_record(buf: &[u8]) -> Option<SpilledBytes> {
    decode_record_with_key(buf).map(|(_, sb)| sb)
}

#[cfg(unix)]
pub(super) fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(windows)]
pub(super) fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut read = 0usize;
    while read < buf.len() {
        let n = file.seek_read(&mut buf[read..], offset + read as u64)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        read += n;
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

#[cfg(windows)]
pub(super) fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        written += n;
    }
    Ok(())
}

pub(super) fn region_file_name(idx: u32) -> String {
    format!("spill-{idx:08x}.reg")
}

pub(super) fn remove_stale_region_files(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("reg") {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}
