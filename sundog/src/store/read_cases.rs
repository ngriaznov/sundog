//! Every residency state a `Mode::Distributed` read decision tells apart,
//! built as a real shard per state: the equivalence tests pin the read
//! paths' decisions over all of them.

use std::collections::HashMap;
use std::num::NonZeroU8;
use std::sync::Arc;

use bytes::Bytes;
use smol_str::SmolStr;
use tokio::sync::watch;

use super::{Mode, PartId, Shard, ShardOps};
use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::ownership::{OwnershipTracker, OwnershipView, ResidencySet};
use crate::wire::WireRecord;

/// What the case's shard holds for its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Held {
    Nothing,
    Live,
    Tombstone,
}

/// One residency state of the case key's part, and what the shard holds
/// for the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent residency marks; every_read_case exhausts their product"
)]
pub(crate) struct ReadCase {
    pub(crate) owns: bool,
    pub(crate) cold_marked: bool,
    pub(crate) unsettled: bool,
    pub(crate) stale: bool,
    pub(crate) unverified: bool,
    pub(crate) releasing: bool,
    pub(crate) held: Held,
}

impl ReadCase {
    /// `ShardOps::is_cold_part`'s answer for the case.
    pub(crate) const fn cold(self) -> bool {
        self.cold_marked || (self.unsettled && self.owns)
    }

    /// `ShardOps::is_unverified_part`'s answer for the case.
    pub(crate) const fn distrusted(self) -> bool {
        self.unverified || self.stale
    }

    /// Whether the shard holds a live entry for the key.
    pub(crate) const fn live(self) -> bool {
        matches!(self.held, Held::Live)
    }

    /// Whether `ShardOps::records_for` returns the key's record: held, in a
    /// part owned or mid disown grace.
    #[cfg_attr(
        feature = "sim",
        allow(
            dead_code,
            reason = "the fetch responder's test is not built under sim"
        )
    )]
    pub(crate) const fn served(self) -> bool {
        !matches!(self.held, Held::Nothing) && (self.owns || self.releasing)
    }
}

/// Every [`ReadCase`]: each mark on and off, `unverified` only with the
/// `spill` feature that can set it, times each held record.
pub(crate) fn every_read_case() -> Vec<ReadCase> {
    let unverified_states: &[bool] = if cfg!(feature = "spill") {
        &[false, true]
    } else {
        &[false]
    };
    let mut cases = Vec::new();
    for bits in 0u8..32 {
        for &unverified in unverified_states {
            for held in [Held::Nothing, Held::Live, Held::Tombstone] {
                cases.push(ReadCase {
                    owns: bits & 1 != 0,
                    cold_marked: bits & 2 != 0,
                    unsettled: bits & 4 != 0,
                    stale: bits & 8 != 0,
                    unverified,
                    releasing: bits & 16 != 0,
                    held,
                });
            }
        }
    }
    cases
}

/// A shard built in one [`ReadCase`]'s state.
pub(crate) struct CaseShard {
    pub(crate) shard: Shard<u32, String>,
    pub(crate) residency: Arc<ResidencySet>,
    pub(crate) key: u32,
    pub(crate) key_bytes: Bytes,
    pub(crate) part: PartId,
    /// The view the shard decides under: node 1 among nodes 1 and 2.
    pub(crate) view: Arc<OwnershipView>,
    /// Kept so a test can publish a further view.
    pub(crate) view_tx: watch::Sender<Arc<OwnershipView>>,
}

fn key_bytes(key: u32) -> Bytes {
    Bytes::from(postcard::to_stdvec(&key).expect("u32 encodes"))
}

/// Builds `case`'s shard, named `name`, as node 1 of nodes 1 and 2 at one
/// owner per part. Its record is installed under a view where node 1 owns
/// every part, before the two-node view is published, so a part node 1
/// does not own can still hold a copy. A part is unsettled when the last
/// settled view is node 3's, which never owns the key's part, and settled
/// when it is the all-owning one.
pub(crate) async fn case_shard(name: &str, case: &ReadCase) -> CaseShard {
    let (n1, n2, n3) = (NodeId::from(1u64), NodeId::from(2u64), NodeId::from(3u64));
    let owners = NonZeroU8::new(1).expect("nonzero");
    let name = SmolStr::new(name);
    let (tracker, view_tx) = OwnershipTracker::seed(n1, &[], &HashMap::new(), &name, owners);
    let solo = Arc::new(OwnershipView::compute(n1, vec![n1], owners));
    view_tx
        .send(Arc::clone(&solo))
        .expect("the tracker holds a receiver");
    let residency = Arc::new(ResidencySet::new());
    let shard =
        Shard::<u32, String>::new(name, Mode::Distributed { owners }, n1, u64::MAX, None, None)
            .with_ownership(tracker, Arc::clone(&residency));

    let view = Arc::new(OwnershipView::compute(n1, vec![n1, n2], owners));
    let third = OwnershipView::compute(n3, vec![n1, n2, n3], owners);
    let key = (0..u32::MAX)
        .find(|&k| {
            let part = PartId::of_key(&key_bytes(k));
            view.owns(part) == case.owns && !third.owns(part)
        })
        .expect("a key in the wanted part is found quickly");
    let key_bytes = key_bytes(key);
    let part = PartId::of_key(&key_bytes);
    let value = match case.held {
        Held::Nothing => None,
        Held::Live => Some(Some(Bytes::from(
            postcard::to_stdvec(&"held".to_string()).expect("a string encodes"),
        ))),
        Held::Tombstone => Some(None),
    };
    if let Some(value) = value {
        ShardOps::apply_remote_batch(
            &shard,
            vec![WireRecord {
                key: key_bytes.clone(),
                value,
                ver: Hlc {
                    wall_ms: super::now_ms(),
                    logical: 1,
                    node: n2,
                },
                expires_at_ms: None,
            }],
        )
        .await;
    }
    view_tx
        .send(Arc::clone(&view))
        .expect("the tracker holds a receiver");

    residency.settle(if case.unsettled { &third } else { &solo });
    if case.cold_marked {
        residency.mark_cold(&[part]);
    }
    if case.stale {
        residency.mark_stale(&[part]);
    }
    #[cfg(feature = "spill")]
    if case.unverified {
        residency.mark_unverified(&[part]);
    }
    if case.releasing {
        residency.mark_releasing(&[part]);
    }
    CaseShard {
        shard,
        residency,
        key,
        key_bytes,
        part,
        view,
        view_tx,
    }
}
