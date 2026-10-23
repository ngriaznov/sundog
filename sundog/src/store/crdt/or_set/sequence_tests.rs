//! One writer's adds applied in sequence order: a remove takes out the adds it
//! observed and no other, whatever order the removes and the other writers'
//! adds arrive in.
//!
//! A remove raises a writer's `seen` watermark to the highest sequence it
//! observed, so it relies on every lower sequence of that writer having been
//! observed too. Applying a writer's adds in the order it numbered them, which
//! `Cache::merge_sequenced` does, is what makes that hold.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use super::*;
use crate::store::test_support::wid;

const ELEMENTS: [&str; 3] = ["a", "b", "c"];

fn element(index: usize) -> String {
    ELEMENTS[index % ELEMENTS.len()].to_string()
}

/// A set with nothing in it, not even a watermark.
fn empty() -> OrSet<String> {
    OrSet {
        adds: BTreeMap::new(),
        seen: BTreeMap::new(),
        retired: BTreeMap::new(),
        folded_at: BTreeMap::new(),
    }
}

/// What one generated remove does: which element it removes, how many of each
/// writer's adds the remover had read, and which earlier removes the read
/// included.
#[derive(Debug, Clone)]
struct RemoveSpec {
    element: usize,
    read_prefix: Vec<usize>,
    includes_earlier: Vec<bool>,
}

fn remove_spec() -> impl Strategy<Value = RemoveSpec> {
    (
        0usize..3,
        proptest::collection::vec(0usize..8, 3),
        proptest::collection::vec(any::<bool>(), 6),
    )
        .prop_map(|(element, read_prefix, includes_earlier)| RemoveSpec {
            element,
            read_prefix,
            includes_earlier,
        })
}

/// `deltas`, streams that each keep their own order, merged into one set by
/// taking the next delta of the stream `picks` names, round after round,
/// until every stream is empty.
fn deliver(streams: &[Vec<OrSet<String>>], picks: &[u8]) -> OrSet<String> {
    let mut cursors = vec![0usize; streams.len()];
    let mut merged = empty();
    let mut round = 0usize;
    loop {
        let live: Vec<usize> = (0..streams.len())
            .filter(|&stream| cursors[stream] < streams[stream].len())
            .collect();
        if live.is_empty() {
            return merged;
        }
        let pick = usize::from(picks[round % picks.len()]);
        let stream = live[pick % live.len()];
        merged = merged.merge(&streams[stream][cursors[stream]]);
        cursors[stream] += 1;
        round += 1;
    }
}

type Tag = (WriterId, u64);

/// A generated scenario, built: the add streams of every writer, each in
/// sequence order, the remove deltas, and the tags the removes observed.
struct Scenario {
    streams: Vec<Vec<OrSet<String>>>,
    every_tag: BTreeSet<Tag>,
    observed_and_removed: BTreeSet<Tag>,
    observed_at_all: BTreeSet<Tag>,
}

fn build(adds: &[Vec<usize>], removes: &[RemoveSpec]) -> Scenario {
    let writers: Vec<WriterId> = (0u64..).take(adds.len()).map(|w| wid(w, 0)).collect();
    let mut streams: Vec<Vec<OrSet<String>>> = Vec::new();
    let mut every_tag = BTreeSet::new();
    for (writer, elements) in writers.iter().zip(adds) {
        let mut stream = Vec::new();
        for (index, &which) in elements.iter().enumerate() {
            let seq = u64::try_from(index).expect("small") + 1;
            stream.push(OrSet::add(*writer, seq, element(which)));
            every_tag.insert((*writer, seq));
        }
        streams.push(stream);
    }
    let mut deltas: Vec<OrSet<String>> = Vec::new();
    let mut observed_and_removed = BTreeSet::new();
    let mut observed_at_all = BTreeSet::new();
    for spec in removes {
        let mut observed = empty();
        for (stream, prefix) in streams.iter().zip(&spec.read_prefix) {
            for add in stream.iter().take(*prefix) {
                observed = observed.merge(add);
            }
        }
        for (earlier, included) in deltas.iter().zip(&spec.includes_earlier) {
            if *included {
                observed = observed.merge(earlier);
            }
        }
        let target = element(spec.element);
        for (&tag, held) in &observed.adds {
            observed_at_all.insert(tag);
            if *held == target {
                observed_and_removed.insert(tag);
            }
        }
        deltas.push(OrSet::remove(&observed, &target));
    }
    streams.extend(deltas.into_iter().map(|delta| vec![delta]));
    Scenario {
        streams,
        every_tag,
        observed_and_removed,
        observed_at_all,
    }
}

proptest! {
    /// The add-wins property for one writer's ordered adds: after every
    /// delta has arrived, the live tags are every add except the ones some
    /// remove observed under its own element, so an add no remove observed
    /// is never lost, whatever order the removes and the writers' adds
    /// interleave in, and every interleaving reaches the same bytes.
    #[test]
    fn no_add_that_a_remove_did_not_observe_is_lost(
        adds in proptest::collection::vec(proptest::collection::vec(0usize..3, 0..6), 1..=3),
        removes in proptest::collection::vec(remove_spec(), 0..6),
        picks in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..40), 3),
    ) {
        let scenario = build(&adds, &removes);
        let expected: BTreeSet<Tag> = scenario
            .every_tag
            .difference(&scenario.observed_and_removed)
            .copied()
            .collect();
        let unobserved: BTreeSet<Tag> = scenario
            .every_tag
            .difference(&scenario.observed_at_all)
            .copied()
            .collect();

        let mut encodings = Vec::new();
        for order in &picks {
            let merged = deliver(&scenario.streams, order);
            let live: BTreeSet<Tag> = merged.adds.keys().copied().collect();
            prop_assert!(
                unobserved.is_subset(&live),
                "an add no remove observed was lost: lost {:?}",
                unobserved.difference(&live).collect::<Vec<_>>()
            );
            prop_assert_eq!(&live, &expected);
            encodings.push(merged.encode().expect("encodes"));
        }
        for encoding in &encodings[1..] {
            prop_assert_eq!(encoding, &encodings[0]);
        }
    }
}

/// A writer adds `a` (1) and `b` (2); a node reads both and removes `b`; the
/// writer then adds `c` (3). Each delta arrives at a second replica in every
/// order that keeps the writer's adds in sequence order, and `a` and `c` live
/// in all of them.
#[test]
fn a_remove_of_the_newest_add_keeps_the_lower_add_and_a_later_one() {
    let w = wid(1, 0);
    let one = OrSet::add(w, 1, "a".to_string());
    let two = OrSet::add(w, 2, "b".to_string());
    let observed = one.merge(&two);
    let remove = OrSet::remove(&observed, &"b".to_string());
    let three = OrSet::add(w, 3, "c".to_string());
    let deltas = [one, two, three, remove];

    // The remove lands before, between and after the writer's adds.
    for slot in 0..=3 {
        let mut order = vec![0usize, 1, 2];
        order.insert(slot, 3);
        let merged = order
            .iter()
            .fold(empty(), |acc, &index| acc.merge(&deltas[index]));
        assert!(merged.contains(&"a".to_string()), "slot {slot}: a lives");
        assert!(!merged.contains(&"b".to_string()), "slot {slot}: b is gone");
        assert!(merged.contains(&"c".to_string()), "slot {slot}: c lives");
    }
}
