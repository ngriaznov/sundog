//! A `Mode::Distributed` cache's unit of ownership: one of the
//! [`PART_COUNT`] parts of one of the [`BUCKET_COUNT`] buckets, 65,536 in
//! all. A key's part is the low 16 bits of its hash: the bucket in the low
//! ten, the part in the six above them, the same bits
//! [`super::engine::stripe_index_from_hash`] and
//! [`super::engine::part_index_from_hash`] read.
//!
//! A view that owns whole buckets gives every part of a bucket the same
//! owners, so rebalance, residency and release keep one vocabulary
//! whichever granularity the view has.

use super::{BUCKET_COUNT, BucketPart, PART_COUNT, engine};

/// Every part in the key space: [`BUCKET_COUNT`] × [`PART_COUNT`].
pub(crate) const PART_SPACE: usize = BUCKET_COUNT * PART_COUNT;

/// One part: `bucket | part << 10`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartId(u16);

impl PartId {
    /// The part a key hash falls in: its low 16 bits.
    #[must_use]
    pub const fn from_hash(hash: u64) -> Self {
        Self((hash & 0xFFFF) as u16)
    }

    /// The part `key_bytes` hashes into.
    #[must_use]
    pub fn of_key(key_bytes: &[u8]) -> Self {
        Self::from_hash(engine::hash_key_bytes(key_bytes))
    }

    /// Part `part` of `bucket`. `bucket` wraps modulo [`BUCKET_COUNT`] and
    /// `part` modulo [`PART_COUNT`].
    #[must_use]
    pub const fn new(bucket: u16, part: u8) -> Self {
        Self((bucket & 0x3FF) | (((part & 0x3F) as u16) << 10))
    }

    /// The part at `index` in `0..PART_SPACE`, the inverse of
    /// [`PartId::index`]. `index` wraps modulo [`PART_SPACE`].
    #[must_use]
    pub const fn from_index(index: usize) -> Self {
        Self((index & 0xFFFF) as u16)
    }

    /// The raw 16-bit id, as the wire carries it.
    #[must_use]
    pub const fn raw(self) -> u16 {
        self.0
    }

    /// The part a raw wire id names.
    #[must_use]
    pub const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }

    /// The bucket this part belongs to, which is also its store stripe.
    #[must_use]
    pub const fn bucket(self) -> u16 {
        self.0 & 0x3FF
    }

    /// This part's position within its bucket, `0..PART_COUNT`.
    #[must_use]
    pub const fn part(self) -> u8 {
        (self.0 >> 10) as u8
    }

    /// A dense index in `0..PART_SPACE`, for bitsets and tables.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    /// The same part as a [`BucketPart`], the anti-entropy part-digest
    /// vocabulary.
    #[must_use]
    pub const fn bucket_part(self) -> BucketPart {
        BucketPart {
            bucket: self.bucket(),
            part: self.part(),
        }
    }

    /// Every part in the key space, in index order.
    pub fn all() -> impl Iterator<Item = Self> {
        (0..PART_SPACE).map(Self::from_index)
    }

    /// The [`PART_COUNT`] parts of `bucket`, in part order.
    pub fn of_bucket(bucket: u16) -> impl Iterator<Item = Self> {
        (0..PART_COUNT).map(move |part| {
            Self::new(
                bucket,
                u8::try_from(part).expect("invariant: PART_COUNT fits u8"),
            )
        })
    }
}

/// `bucket.part`, as logs and test messages name a part.
impl std::fmt::Display for PartId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.bucket(), self.part())
    }
}

impl From<BucketPart> for PartId {
    fn from(bp: BucketPart) -> Self {
        Self::new(bp.bucket, bp.part)
    }
}

/// A set of parts as a 65,536-bit bitset: 8 KiB whatever it holds.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PartSet {
    bits: Box<[u64]>,
    len: usize,
}

impl std::fmt::Debug for PartSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartSet").field("len", &self.len).finish()
    }
}

impl Default for PartSet {
    fn default() -> Self {
        Self::new()
    }
}

impl PartSet {
    /// An empty set.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            bits: vec![0u64; PART_SPACE / 64].into_boxed_slice(),
            len: 0,
        }
    }

    /// Adds `part`; `true` if it was not already present.
    pub(crate) fn insert(&mut self, part: PartId) -> bool {
        let (word, bit) = (part.index() / 64, 1u64 << (part.index() % 64));
        let fresh = self.bits[word] & bit == 0;
        if fresh {
            self.bits[word] |= bit;
            self.len += 1;
        }
        fresh
    }

    /// Removes `part`; `true` if it was present.
    pub(crate) fn remove(&mut self, part: PartId) -> bool {
        let (word, bit) = (part.index() / 64, 1u64 << (part.index() % 64));
        let present = self.bits[word] & bit != 0;
        if present {
            self.bits[word] &= !bit;
            self.len -= 1;
        }
        present
    }

    /// Whether `part` is in the set.
    #[must_use]
    pub(crate) fn contains(&self, part: PartId) -> bool {
        self.bits[part.index() / 64] & (1u64 << (part.index() % 64)) != 0
    }

    /// How many parts the set holds.
    #[must_use]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// Removes every part.
    pub(crate) fn clear(&mut self) {
        self.bits.fill(0);
        self.len = 0;
    }

    /// Every part in the set, in index order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = PartId> + '_ {
        self.bits.iter().enumerate().flat_map(|(word_idx, &word)| {
            let mut rest = word;
            std::iter::from_fn(move || {
                if rest == 0 {
                    return None;
                }
                let bit = rest.trailing_zeros() as usize;
                rest &= rest - 1;
                Some(PartId::from_index(word_idx * 64 + bit))
            })
        })
    }
}

impl FromIterator<PartId> for PartSet {
    fn from_iter<I: IntoIterator<Item = PartId>>(iter: I) -> Self {
        let mut set = Self::new();
        set.extend(iter);
        set
    }
}

impl Extend<PartId> for PartSet {
    fn extend<I: IntoIterator<Item = PartId>>(&mut self, iter: I) {
        for part in iter {
            self.insert(part);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_part_is_its_bucket_and_part_index_from_the_same_hash() {
        for key in [&b"a"[..], b"key:0000000001", b"", &[0xFF; 40]] {
            let hash = engine::hash_key_bytes(key);
            let part = PartId::of_key(key);
            assert_eq!(
                usize::from(part.bucket()),
                engine::stripe_index_from_hash(hash)
            );
            assert_eq!(usize::from(part.part()), engine::part_index_from_hash(hash));
            assert_eq!(part.bucket(), super::super::bucket_of(key));
        }
    }

    #[test]
    fn part_id_round_trips_through_every_representation() {
        for part in [PartId::new(0, 0), PartId::new(1023, 63), PartId::new(5, 7)] {
            assert_eq!(PartId::new(part.bucket(), part.part()), part);
            assert_eq!(PartId::from_index(part.index()), part);
            assert_eq!(PartId::from_raw(part.raw()), part);
            assert_eq!(PartId::from(part.bucket_part()), part);
        }
        assert_eq!(PartId::all().count(), PART_SPACE);
        assert_eq!(
            PartId::all().map(PartId::index).collect::<Vec<_>>(),
            (0..PART_SPACE).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_part_displays_as_bucket_dot_part() {
        assert_eq!(PartId::new(5, 7).to_string(), "5.7");
        assert_eq!(PartId::new(1023, 63).to_string(), "1023.63");
    }

    #[test]
    fn of_bucket_lists_the_bucket_s_parts_once_each() {
        let parts: Vec<PartId> = PartId::of_bucket(9).collect();
        assert_eq!(parts.len(), PART_COUNT);
        assert!(parts.iter().all(|p| p.bucket() == 9));
        let distinct: std::collections::HashSet<u8> = parts.iter().map(|p| p.part()).collect();
        assert_eq!(distinct.len(), PART_COUNT);
    }

    #[test]
    fn part_set_tracks_membership_length_and_order() {
        let mut set = PartSet::new();
        assert_eq!(set.len(), 0);
        let a = PartId::new(3, 1);
        let b = PartId::new(1000, 63);
        assert!(set.insert(b));
        assert!(set.insert(a));
        assert!(!set.insert(a), "a second insert is not fresh");
        assert_eq!(set.len(), 2);
        assert!(set.contains(a) && set.contains(b));
        assert!(!set.contains(PartId::new(3, 2)));
        let order: Vec<PartId> = set.iter().collect();
        assert_eq!(order, {
            let mut sorted = vec![a, b];
            sorted.sort_by_key(|p| p.index());
            sorted
        });
        assert!(set.remove(a));
        assert!(!set.remove(a));
        assert_eq!(set.len(), 1);
        set.clear();
        assert_eq!(set.len(), 0);
        assert_eq!(set.iter().count(), 0);
    }

    #[test]
    fn part_set_collects_and_extends() {
        let set: PartSet = PartId::of_bucket(2).collect();
        assert_eq!(set.len(), PART_COUNT);
        let mut more = set.clone();
        more.extend(PartId::of_bucket(2).chain(PartId::of_bucket(3)));
        assert_eq!(more.len(), 2 * PART_COUNT);
        let every: PartSet = PartId::all().collect();
        assert_eq!(every.len(), PART_SPACE);
        assert_eq!(every.iter().count(), PART_SPACE);
    }
}
