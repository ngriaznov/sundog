//! Node identity: a per-process-incarnation random id and the derived name used
//! as chitchat's cluster-membership identifier.

use std::fmt;

use rand::RngExt as _;
use serde::{Deserialize, Serialize};

/// A compact, random identifier for one running instance of the process.
///
/// Random per incarnation unless [`crate::ClusterBuilder::node_id`] supplies
/// a persisted one: with a fresh id a restarted process is a new node for
/// membership and HLC tie-breaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    /// Partitions the `u64` id space in two: a real node's id always has
    /// this bit clear, and the engine's merge-version combinator always sets
    /// it when minting a version for a resolver's [`crate::store::ConflictResolver::merge`]
    /// reply. The two halves never overlap, so a minted version can never
    /// collide with a real single-writer stamp: [`Self::random`] clears the
    /// bit unconditionally, and the explicit-id path
    /// ([`crate::ClusterBuilder::node_id`]) rejects any id with the bit set.
    const MERGE_BIT: u64 = 1 << 63;

    /// Generates a new random node id.
    ///
    /// Clears the top bit unconditionally, so a real node's id always falls
    /// in the lower half of the `u64` range and can never collide with a
    /// merge-derived id, minted only by the engine's merge-version
    /// combinator.
    #[must_use]
    pub fn random() -> Self {
        Self(rand::rng().random::<u64>() & !Self::MERGE_BIT)
    }

    /// Builds the node id a minted merge version is stamped with: `hash`
    /// (the merged bytes' `xxh3_64`, in the engine's actual use) with
    /// [`Self::MERGE_BIT`] set.
    ///
    /// Setting the bit puts the result in the half of the id space
    /// [`Self::random`] never draws from and the explicit-id builder path
    /// never accepts, so it can never collide with a real node's identity;
    /// keeping `hash` as the rest of the value makes the id a deterministic
    /// function of the merged content, so two nodes minting a version for
    /// the same merged bytes mint the same id.
    #[must_use]
    pub(crate) const fn merge_derived(hash: u64) -> Self {
        Self(hash | Self::MERGE_BIT)
    }

    /// Whether this id names a merge the engine minted, never a member of
    /// the cluster. See [`Self::merge_derived`].
    #[must_use]
    pub(crate) const fn is_merge_derived(self) -> bool {
        self.0 & Self::MERGE_BIT != 0
    }

    /// Returns the raw numeric value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for NodeId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for NodeId {
    /// Renders as lowercase hex, e.g. `a1b2c3d4e5f60718`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl std::str::FromStr for NodeId {
    type Err = std::num::ParseIntError;

    /// Parses the hex text [`NodeId`]'s `Display` produces, so a persisted
    /// id (see [`crate::cluster::ClusterBuilder::node_id`]) round-trips
    /// through a file across a restart.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        u64::from_str_radix(s, 16).map(Self)
    }
}

/// The human-readable, cluster-unique name derived from a node's hostname and
/// [`NodeId`]: `{hostname}-{nodeid-hex}`. This is the string chitchat uses as
/// its node id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeName(String);

impl NodeName {
    /// Builds the canonical `{hostname}-{nodeid-hex}` name.
    #[must_use]
    pub fn new(hostname: &str, node_id: NodeId) -> Self {
        Self(format!("{hostname}-{node_id}"))
    }

    /// Returns the name as a plain string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for NodeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_ids_differ() {
        assert_ne!(NodeId::random(), NodeId::random());
    }

    #[test]
    fn random_never_produces_a_merge_derived_id() {
        for _ in 0..10_000 {
            assert!(
                !NodeId::random().is_merge_derived(),
                "a real node id must never carry the merge bit"
            );
        }
    }

    #[test]
    fn merge_derived_always_sets_the_merge_bit() {
        for hash in [0u64, 1, 42, u64::MAX / 2, u64::MAX] {
            assert!(
                NodeId::merge_derived(hash).is_merge_derived(),
                "merge_derived({hash}) must be recognized as merge-derived"
            );
        }
    }

    #[test]
    fn a_real_and_a_merge_derived_id_never_collide() {
        for hash in [0u64, 1, 42, u64::MAX] {
            let real = NodeId::random();
            let derived = NodeId::merge_derived(hash);
            assert_ne!(
                real, derived,
                "a real node id and a merge-derived id live in disjoint halves of the id space"
            );
        }
    }

    #[test]
    fn display_is_lowercase_hex_16_chars() {
        let id = NodeId::from(0xdead_beef_cafe_babe);
        let text = id.to_string();
        assert_eq!(text, "deadbeefcafebabe");
        assert_eq!(text.len(), 16);
        assert!(
            text.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn node_name_format() {
        let id = NodeId::from(1);
        let name = NodeName::new("host1", id);
        assert_eq!(name.as_str(), "host1-0000000000000001");
    }

    #[test]
    fn ordering_matches_numeric_value() {
        let a = NodeId::from(1);
        let b = NodeId::from(2);
        assert!(a < b);
    }

    #[test]
    fn roundtrips_through_postcard() {
        let id = NodeId::random();
        let bytes = postcard::to_stdvec(&id).expect("invariant: NodeId always encodes");
        let decoded: NodeId =
            postcard::from_bytes(&bytes).expect("invariant: freshly encoded bytes decode");
        assert_eq!(id, decoded);
    }

    #[test]
    fn roundtrips_through_display_and_from_str() {
        let id = NodeId::random();
        let parsed: NodeId = id.to_string().parse().expect("Display output is valid hex");
        assert_eq!(id, parsed);
    }

    #[test]
    fn roundtrips_through_a_persisted_file() {
        let id = NodeId::from(0xabc_def);
        let path = std::env::temp_dir().join(format!("sundog-node-id-test-{id}"));
        std::fs::write(&path, id.to_string()).expect("write persists the id");
        let read_back = std::fs::read_to_string(&path).expect("read back the persisted file");
        std::fs::remove_file(&path).ok();
        let restarted: NodeId = read_back
            .trim()
            .parse()
            .expect("persisted text is valid hex");
        assert_eq!(id, restarted, "a restarted node reads back the same id");
    }
}
