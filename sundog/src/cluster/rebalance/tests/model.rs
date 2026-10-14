//! A property test of the residency bookkeeping against a reference model, under views published and pulls superseded in any order.

use proptest::prelude::*;

use super::*;
use crate::ownership::Granularity;
use crate::store::part::PART_SPACE;

/// One published view, or one rebalance step over the latest one.
#[derive(Debug, Clone)]
enum Step {
    /// Publish the view over `self` plus these others (bit i set
    /// means node `i + 2` is eligible).
    Publish(u8),
    /// Rebalance the latest published view. A superseded or timed-out
    /// pull lands only the parts `landed` selects; a superseded one does
    /// not settle, a timed-out one does.
    Rebalance { pull: Pull, landed: u64 },
    /// Retry the parts a pull planned and did not land, on the tick:
    /// only once the latest published view is the one the task settled.
    Retry { landed: u64 },
}

/// How a rebalance pull ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pull {
    Landed,
    Superseded,
    TimedOut,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        (1u8..16).prop_map(Step::Publish),
        (
            prop_oneof![
                Just(Pull::Landed),
                Just(Pull::Superseded),
                Just(Pull::TimedOut)
            ],
            any::<u64>()
        )
            .prop_map(|(pull, landed)| Step::Rebalance { pull, landed }),
        any::<u64>().prop_map(|landed| Step::Retry { landed }),
    ]
}

fn view_for(mask: u8, granularity: Granularity) -> Arc<OwnershipView> {
    let me = NodeId::from(1);
    let mut eligible = vec![me];
    eligible.extend(
        (0..4u64)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| NodeId::from(i + 2)),
    );
    Arc::new(OwnershipView::compute_at(
        me,
        eligible,
        NonZeroU8::new(2).expect("nonzero"),
        granularity,
    ))
}

/// The `state_transfer_budget` the model's retries run under.
const RETRY_BUDGET: Duration = Duration::from_secs(1);

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Every part this node owns is either complete (it holds
    /// every write made to it) or reads as cold, so a local miss
    /// there is never taken as an answer; and once a view's pull
    /// completes with nothing published after it, every owned part
    /// serves.
    #[test]
    fn a_part_owned_but_missing_writes_always_reads_as_cold(
        start in 1u8..16,
        steps in proptest::collection::vec(step(), 1..14),
        part_granularity in any::<bool>(),
    ) {
        let granularity = if part_granularity { Granularity::Part } else { Granularity::Bucket };
        let residency = ResidencySet::new();
        let mut published = view_for(start, granularity);
        residency.settle(&published);
        // Every part the starting view owns is complete; every other
        // part holds nothing, so it is complete only while unowned.
        let mut complete: Vec<bool> = vec![true; PART_SPACE];
        let mut prev = Arc::clone(&published);
        let mut pulled = Arc::clone(&published);
        let mut unfinished = PartSet::new();

        let check = |published: &OwnershipView, complete: &[bool]| {
            for part in published.owned_parts() {
                if !complete[part.index()] {
                    prop_assert!(
                        residency.is_cold(part) || residency.is_unsettled(part),
                        "part {part} is owned and misses writes, yet reads as servable"
                    );
                }
            }
            Ok(())
        };

        for step in steps {
            match step {
                Step::Publish(mask) => {
                    let next = view_for(mask, granularity);
                    residency.record_published(&published, &next);
                    // Writes to a part this view does not own go to
                    // its other owners, not here.
                    for part in PartId::all().filter(|&p| !next.owns(p)) {
                        complete[part.index()] = false;
                    }
                    published = next;
                }
                Step::Rebalance { pull, landed } => {
                    let new = Arc::clone(&published);
                    let mut repull = residency.take_lost();
                    repull.extend(unfinished.iter());
                    let plan = plan_view_change(&prev, &pulled, &new, &[], &repull);
                    residency.mark_cold(&plan.to_pull);
                    let landing: Vec<PartId> = if pull == Pull::Landed {
                        plan.to_pull.clone()
                    } else {
                        plan.to_pull
                            .iter()
                            .copied()
                            .filter(|part| landed & (1 << (part.index() % 64)) != 0)
                            .collect()
                    };
                    residency.mark_serving(&landing);
                    for part in &landing {
                        complete[part.index()] = true;
                    }
                    unfinished = still_cold(plan.to_pull.iter().copied(), |part| {
                        residency.is_cold(part)
                    });
                    if pull != Pull::Superseded {
                        residency.settle(&new);
                        pulled = Arc::clone(&new);
                    }
                    prev = new;
                }
                Step::Retry { landed } => {
                    // The task retries only on a tick, and a published view
                    // it has not yet handled is handled first.
                    if Arc::ptr_eq(&pulled, &published) {
                        let retry = retry_parts(&unfinished, &published, RETRY_BUDGET, |part| {
                            residency.is_cold(part)
                        });
                        let landing: Vec<PartId> = retry
                            .into_iter()
                            .filter(|part| landed & (1 << (part.index() % 64)) != 0)
                            .collect();
                        residency.mark_serving(&landing);
                        for part in &landing {
                            complete[part.index()] = true;
                        }
                        unfinished = still_cold(unfinished.iter(), |part| residency.is_cold(part));
                    }
                }
            }
            check(&published, &complete)?;
        }

        // Liveness: retries alone complete what a timed-out pull left, with
        // no further view published.
        if Arc::ptr_eq(&pulled, &published) {
            let retry = retry_parts(&unfinished, &published, RETRY_BUDGET, |part| {
                residency.is_cold(part)
            });
            residency.mark_serving(&retry);
            for part in &retry {
                complete[part.index()] = true;
            }
            for part in published.owned_parts() {
                prop_assert!(complete[part.index()], "part {part} still misses writes after a retry");
                prop_assert!(
                    !residency.is_cold(part) && !residency.is_unsettled(part),
                    "part {part} still reads as cold after a retry"
                );
            }
        }

        // Liveness: one completed rebalance of the latest view leaves
        // every owned part complete and serving.
        let mut repull = residency.take_lost();
        repull.extend(unfinished.iter());
        let plan = plan_view_change(&prev, &pulled, &published, &[], &repull);
        residency.mark_cold(&plan.to_pull);
        residency.mark_serving(&plan.to_pull);
        for part in &plan.to_pull {
            complete[part.index()] = true;
        }
        residency.settle(&published);
        for part in published.owned_parts() {
            prop_assert!(complete[part.index()], "part {part} still misses writes after a completed pull");
            prop_assert!(
                !residency.is_cold(part) && !residency.is_unsettled(part),
                "part {part} still reads as cold after a completed pull"
            );
        }
    }
}
