//! How a `Mode::Distributed` cache decides a read of a key: this node's
//! residency marks for the key's part, what its own copy makes of a fetch,
//! and what it answers a peer's fetch.

use std::time::Duration;

/// This node's residency marks for one part of a `Mode::Distributed`
/// cache, read once, in the order the marks' writers take their locks.
/// Each mark is read on its own, so a mark a writer moves during the read
/// can show its state from just before or just after the move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent residency marks; the verdict tests exhaust their product"
)]
pub struct Residency {
    /// Whether this node's ownership view names it an owner of the part.
    pub owns: bool,
    /// How long ago the part's disown grace began, while this node still
    /// holds its copy of a part it no longer owns.
    pub releasing_for: Option<Duration>,
    /// Whether rebalance marked the part cold: owned, not yet pulled from a
    /// co-owner.
    pub cold_marked: bool,
    /// Whether the part is owned since the last settled view without a
    /// pull or verification since, the window between a published view
    /// and rebalance marking its gained parts cold.
    pub unsettled: bool,
    /// Whether a warm spill-tier reopen replayed the part from disk with no
    /// co-owner check yet.
    pub unverified: bool,
    /// Whether this node owns the part again while it holds a copy from
    /// owning it before, which lacks what changed while it was not an
    /// owner.
    pub stale: bool,
}

impl Residency {
    /// Marks for a part with none set: owned or not, with nothing cold,
    /// distrusted or releasing.
    pub(crate) const fn new(owns: bool) -> Self {
        Self {
            owns,
            releasing_for: None,
            cold_marked: false,
            unsettled: false,
            unverified: false,
            stale: false,
        }
    }

    /// Whether a local miss in the part says nothing about the key: marked
    /// cold, or owned and unsettled.
    #[must_use]
    pub const fn cold(&self) -> bool {
        self.cold_marked || (self.unsettled && self.owns)
    }

    /// Whether the part counts as cold only for being owned and unsettled,
    /// before rebalance marks it.
    #[must_use]
    pub const fn implicitly_cold(&self) -> bool {
        !self.cold_marked && self.unsettled && self.owns
    }

    /// Whether a local hit in the part is not trusted: replayed and not yet
    /// verified, or stale.
    #[must_use]
    pub const fn distrusted(&self) -> bool {
        self.unverified || self.stale
    }

    /// Whether this node serves the part's records to a peer: owned, or
    /// mid disown grace.
    #[must_use]
    pub const fn resident(&self) -> bool {
        self.owns || self.releasing_for.is_some()
    }
}

/// What this node's own copy of a `Mode::Distributed` key's part makes of a
/// fetch, before any owner is asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LocalRead {
    /// Owned, trusted and holding a live entry: the fetch answers it here.
    Hit,
    /// Owned, trusted, warm and holding no live entry: the fetch answers
    /// `None` here.
    Miss,
    /// Not an owner of the part: the fetch asks the owners.
    NotOwner,
    /// Owned, but a local hit is not trusted: the fetch asks the other
    /// owners whatever the local copy holds.
    Distrusted,
    /// Owned and trusted but cold, with no live entry: the miss says
    /// nothing, so the fetch asks the other owners.
    ColdMiss,
}

impl LocalRead {
    /// Whether the fetch answers here, without asking an owner.
    #[must_use]
    pub const fn answers(self) -> bool {
        matches!(self, Self::Hit | Self::Miss)
    }
}

/// What this node answers a peer's `Fetch` for a key of a
/// `Mode::Distributed` cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServeVerdict {
    /// Sends the record it holds, live or a tombstone, whatever the two
    /// views say.
    Serve,
    /// Holds no record and the asker's view differs from this node's: the
    /// asker refreshes its view and asks again.
    Stale,
    /// Holds no record, on an equal view, in a warm part: a definitive
    /// miss.
    Miss,
    /// Declines: the part is distrusted, so even a held record is not sent.
    DeclineDistrusted,
    /// Declines: the part is cold and holds no record, so a miss says
    /// nothing.
    DeclineCold,
}
