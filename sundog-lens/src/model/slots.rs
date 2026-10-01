//! Stable node slots: a label and a color index for each gossip address, in
//! the order the addresses first appear.

use std::collections::HashMap;
use std::net::SocketAddr;

use smol_str::SmolStr;

/// One node's slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slot {
    /// The gossip address the slot belongs to.
    pub addr: SocketAddr,
    /// The first-seen order, from 0. The slot's color is
    /// `NODE_COLORS[index % 8]`.
    pub index: usize,
    /// The label drawn for the node: `n1`, `n2`, ... unless a hint names it.
    pub label: SmolStr,
}

/// The slots of every gossip address seen so far. A restarted node that comes
/// back at the same address, with a new identity, keeps its slot, its label
/// and its color.
#[derive(Debug, Clone, Default)]
pub struct Slots {
    entries: Vec<Slot>,
    by_addr: HashMap<SocketAddr, usize>,
    hints: HashMap<SocketAddr, SmolStr>,
}

impl Slots {
    /// No slots.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The slot for `addr`, created at the next index on first sight.
    pub fn assign(&mut self, addr: SocketAddr) -> &Slot {
        let index = if let Some(&index) = self.by_addr.get(&addr) {
            index
        } else {
            let index = self.entries.len();
            let label = self
                .hints
                .get(&addr)
                .cloned()
                .unwrap_or_else(|| self.default_label(index));
            self.by_addr.insert(addr, index);
            self.entries.push(Slot { addr, index, label });
            index
        };
        &self.entries[index]
    }

    /// Names `addr` as `label`, overriding the default. It applies at once to
    /// a slot that exists and when the address first appears otherwise.
    pub fn hint(&mut self, addr: SocketAddr, label: impl Into<SmolStr>) {
        let label = label.into();
        if let Some(&index) = self.by_addr.get(&addr) {
            self.entries[index].label.clone_from(&label);
        }
        self.hints.insert(addr, label);
    }

    /// The slot for `addr`, if it has one.
    #[must_use]
    pub fn get(&self, addr: SocketAddr) -> Option<&Slot> {
        self.by_addr.get(&addr).map(|&index| &self.entries[index])
    }

    /// The slot labeled `label`, if any.
    #[must_use]
    pub fn by_label(&self, label: &str) -> Option<&Slot> {
        self.entries.iter().find(|slot| slot.label == label)
    }

    /// Every slot in first-seen order.
    pub fn iter(&self) -> impl Iterator<Item = &Slot> {
        self.entries.iter()
    }

    /// How many slots exist.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no slot exists.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `n{index + 1}`, or the next free `n{k}` when a hint or an earlier slot
    /// already uses it.
    fn default_label(&self, index: usize) -> SmolStr {
        let taken = |label: &str| {
            self.entries.iter().any(|slot| slot.label == label)
                || self.hints.values().any(|hint| hint == label)
        };
        // More candidates than there are labels in use, so one is free.
        (index + 1..=index + 1 + self.entries.len() + self.hints.len())
            .map(|k| SmolStr::from(format!("n{k}")))
            .find(|label| !taken(label))
            .expect("a range longer than the labels in use has a free label")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, last], 7946))
    }

    #[test]
    fn slots_follow_first_seen_order() {
        let mut slots = Slots::new();
        assert!(slots.is_empty());
        assert_eq!(slots.assign(addr(12)).index, 0);
        assert_eq!(slots.assign(addr(11)).index, 1);
        assert_eq!(slots.assign(addr(13)).index, 2);
        assert_eq!(slots.len(), 3);
        let labels: Vec<_> = slots.iter().map(|s| s.label.as_str()).collect();
        assert_eq!(labels, ["n1", "n2", "n3"]);
    }

    #[test]
    fn assigning_an_address_again_keeps_its_slot_and_color() {
        let mut slots = Slots::new();
        let first = slots.assign(addr(11)).clone();
        slots.assign(addr(12));
        assert_eq!(slots.assign(addr(11)), &first);
        assert_eq!(slots.len(), 2);
    }

    #[test]
    fn a_hint_overrides_the_label_of_an_existing_slot() {
        let mut slots = Slots::new();
        slots.assign(addr(13));
        slots.hint(addr(13), "n3");
        assert_eq!(slots.get(addr(13)).unwrap().label, "n3");
        assert_eq!(slots.get(addr(13)).unwrap().index, 0);
    }

    #[test]
    fn a_hint_before_first_sight_applies_on_assignment() {
        let mut slots = Slots::new();
        slots.hint(addr(14), "n4");
        assert!(slots.get(addr(14)).is_none());
        let slot = slots.assign(addr(14));
        assert_eq!((slot.index, slot.label.as_str()), (0, "n4"));
    }

    #[test]
    fn a_default_label_skips_labels_that_hints_or_slots_hold() {
        let mut slots = Slots::new();
        slots.hint(addr(11), "n1");
        slots.hint(addr(12), "n2");
        // First seen is .13: index 0 would be n1, which a hint holds.
        assert_eq!(slots.assign(addr(13)).label, "n3");
        assert_eq!(slots.assign(addr(11)).label, "n1");
        assert_eq!(slots.assign(addr(12)).label, "n2");
        assert_eq!(slots.assign(addr(15)).label, "n4");
    }

    #[test]
    fn lookups_by_address_and_label() {
        let mut slots = Slots::new();
        slots.assign(addr(11));
        slots.assign(addr(12));
        assert_eq!(slots.by_label("n2").unwrap().addr, addr(12));
        assert!(slots.by_label("n9").is_none());
        assert!(slots.get(addr(99)).is_none());
    }

    #[test]
    fn the_same_ip_on_another_port_is_another_node() {
        let mut slots = Slots::new();
        slots.assign(addr(11));
        let other = SocketAddr::from(([127, 0, 0, 11], 7947));
        assert_eq!(slots.assign(other).index, 1);
    }
}
