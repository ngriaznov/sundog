//! The pure decisions: `next_action`, `condition_met`, `deadline`, `judge` and `placement_in`.

use super::*;

fn state(started: Instant) -> DirectorState {
    DirectorState {
        step_started: started,
        baseline: BTreeMap::new(),
    }
}

fn settled_step(within: Option<Duration>) -> Step {
    Step::AwaitSettled {
        cache: "it".into(),
        within,
    }
}

#[test]
fn a_pause_is_done_when_its_time_has_passed() {
    let t0 = Instant::now();
    let step = Step::Pause(S(3));
    let d = ModelDigest::default();
    assert_eq!(next_action(&step, &state(t0), &d, t0), DirectorAction::Wait);
    assert_eq!(
        next_action(&step, &state(t0), &d, t0 + Duration::from_millis(2999)),
        DirectorAction::Wait
    );
    assert_eq!(
        next_action(&step, &state(t0), &d, t0 + S(3)),
        DirectorAction::Done
    );
}

#[test]
fn steps_that_do_not_wait_are_done_at_once() {
    let t0 = Instant::now();
    let d = ModelDigest::default();
    for step in [
        Step::Caption("x".into()),
        Step::Spawn {
            count: 3,
            stagger: None,
        },
        Step::Fill(10),
        Step::Load(true),
        Step::Kill("n1".into()),
        Step::Tab(crate::ui::View::Node),
        Step::Select("n1".into()),
        Step::Help(true),
        Step::Explain {
            key: "k1".into(),
            within: Some(S(5)),
        },
        Step::ExplainClose,
        Step::Quit,
    ] {
        assert_eq!(
            next_action(&step, &state(t0), &d, t0),
            DirectorAction::Done,
            "{step}"
        );
    }
}

#[test]
fn await_members_counts_live_members_exactly() {
    let t0 = Instant::now();
    let step = Step::AwaitMembers {
        count: 3,
        within: Some(S(20)),
    };
    for (live, want) in [
        (0, DirectorAction::Wait),
        (2, DirectorAction::Wait),
        (3, DirectorAction::Done),
        (4, DirectorAction::Wait),
    ] {
        assert_eq!(
            next_action(&step, &state(t0), &digest(live), t0),
            want,
            "{live}"
        );
    }
}

#[test]
fn await_a_status_reads_the_slot_label_and_the_status() {
    let t0 = Instant::now();
    let mut d = digest(2);
    d.statuses.insert("n3".into(), MemberStatus::Live);
    d.statuses.insert("n2".into(), MemberStatus::Departing);
    let step = |status, label: &str| Step::AwaitStatus {
        status,
        label: label.into(),
        within: Some(S(5)),
    };
    let at = |step: &Step, d: &ModelDigest| next_action(step, &state(t0), d, t0);
    assert_eq!(
        at(&step(AwaitedStatus::Departing, "n2"), &d),
        DirectorAction::Done
    );
    assert_eq!(
        at(&step(AwaitedStatus::Left, "n2"), &d),
        DirectorAction::Wait
    );
    assert_eq!(
        at(&step(AwaitedStatus::Down, "n3"), &d),
        DirectorAction::Wait
    );
    assert_eq!(
        at(&step(AwaitedStatus::Down, "n9"), &d),
        DirectorAction::Wait
    );
    d.statuses.insert("n3".into(), MemberStatus::Down);
    assert_eq!(
        at(&step(AwaitedStatus::Down, "n3"), &d),
        DirectorAction::Done
    );
}

#[test]
fn a_settled_await_does_not_pass_on_the_view_that_held_before() {
    let t0 = Instant::now();
    let step = settled_step(Some(S(25)));
    let mut st = state(t0);
    st.baseline.insert("it".into(), 7);

    // The view has not moved from the baseline, however settled it is.
    let same = with_cache(digest(3), 7, true);
    assert_eq!(next_action(&step, &st, &same, t0), DirectorAction::Wait);
    // It moved but has not settled.
    let moving = with_cache(digest(4), 8, false);
    assert_eq!(next_action(&step, &st, &moving, t0), DirectorAction::Wait);
    // It moved and settled.
    let done = with_cache(digest(4), 8, true);
    assert_eq!(next_action(&step, &st, &done, t0), DirectorAction::Done);
}

#[test]
fn a_settled_await_with_no_baseline_needs_a_view_and_settling() {
    let t0 = Instant::now();
    let step = settled_step(Some(S(25)));
    let st = state(t0);
    assert_eq!(
        next_action(&step, &st, &ModelDigest::default(), t0),
        DirectorAction::Wait
    );
    let mut no_view = digest(3);
    no_view.settled.insert("it".into(), true);
    assert_eq!(next_action(&step, &st, &no_view, t0), DirectorAction::Wait);
    assert_eq!(
        next_action(&step, &st, &with_cache(digest(3), 1, false), t0),
        DirectorAction::Wait
    );
    assert_eq!(
        next_action(&step, &st, &with_cache(digest(3), 1, true), t0),
        DirectorAction::Done
    );
}

#[test]
fn an_await_times_out_at_its_deadline_and_the_condition_wins_a_tie() {
    let t0 = Instant::now();
    let step = Step::AwaitMembers {
        count: 3,
        within: Some(S(20)),
    };
    let st = state(t0);
    assert_eq!(
        next_action(&step, &st, &digest(2), t0 + Duration::from_millis(19_999)),
        DirectorAction::Wait
    );
    assert_eq!(
        next_action(&step, &st, &digest(2), t0 + S(20)),
        DirectorAction::TimedOut
    );
    assert_eq!(
        next_action(&step, &st, &digest(3), t0 + S(20)),
        DirectorAction::Done
    );
}

#[test]
fn an_await_without_within_waits_the_default() {
    let t0 = Instant::now();
    let step = Step::AwaitMembers {
        count: 3,
        within: None,
    };
    assert_eq!(deadline(&step), Some(DEFAULT_WITHIN));
    assert_eq!(
        next_action(&step, &state(t0), &digest(0), t0 + DEFAULT_WITHIN),
        DirectorAction::TimedOut
    );
    assert_eq!(deadline(&Step::Pause(S(1))), None);
    assert_eq!(deadline(&Step::Quit), None);
    assert_eq!(deadline(&settled_step(Some(S(7)))), Some(S(7)));
}

#[test]
fn an_explain_waits_for_its_answers_up_to_its_within_or_the_default() {
    let explain = |within| Step::Explain {
        key: "k1".into(),
        within,
    };
    assert_eq!(deadline(&explain(Some(S(4)))), Some(S(4)));
    assert_eq!(deadline(&explain(None)), Some(DEFAULT_WITHIN));
    assert_eq!(deadline(&Step::ExplainClose), None);
}

#[test]
fn only_steps_that_change_the_cluster_are_fleet_actions() {
    for line in ["spawn 2", "kill n1", "leave n1", "crash n1", "restart n1"] {
        let step = parse(line).unwrap().steps.remove(0).step;
        assert!(is_fleet_action(&step), "{line}");
    }
    for line in [
        "fill 10",
        "load start",
        "pause 1s",
        "tab node",
        "await members 1",
        "explain k1",
        "explain close",
        "quit",
        "caption \"x\"",
    ] {
        let step = parse(line).unwrap().steps.remove(0).step;
        assert!(!is_fleet_action(&step), "{line}");
    }
}

#[test]
fn condition_met_holds_for_each_await_and_for_non_awaits() {
    let members = Step::AwaitMembers {
        count: 3,
        within: None,
    };
    let none = BTreeMap::new();
    assert!(condition_met(&members, &none, &digest(3)));
    assert!(!condition_met(&members, &none, &digest(2)));

    let mut d = digest(2);
    d.statuses.insert("n2".into(), MemberStatus::Departing);
    let departing = Step::AwaitStatus {
        status: AwaitedStatus::Departing,
        label: "n2".into(),
        within: None,
    };
    let left = Step::AwaitStatus {
        status: AwaitedStatus::Left,
        label: "n2".into(),
        within: None,
    };
    assert!(condition_met(&departing, &none, &d));
    assert!(!condition_met(&left, &none, &d));

    let settled = settled_step(None);
    let base: BTreeMap<SmolStr, u64> = [("it".into(), 7)].into();
    // Same view as the baseline: not met, however settled.
    assert!(!condition_met(
        &settled,
        &base,
        &with_cache(digest(3), 7, true)
    ));
    // A different view, settled: met.
    assert!(condition_met(
        &settled,
        &base,
        &with_cache(digest(3), 8, true)
    ));
    assert!(!condition_met(
        &settled,
        &base,
        &with_cache(digest(3), 8, false)
    ));
    // No baseline: any view counts once it has settled.
    assert!(condition_met(
        &settled,
        &none,
        &with_cache(digest(3), 1, true)
    ));
    // No view at all.
    assert!(!condition_met(&settled, &base, &digest(3)));

    // A step that is not an await has no condition and holds.
    for step in [
        Step::Quit,
        Step::Pause(S(1)),
        Step::Fill(1),
        Step::Load(true),
        Step::ExplainClose,
        Step::Explain {
            key: "k1".into(),
            within: None,
        },
    ] {
        assert!(
            condition_met(&step, &none, &ModelDigest::default()),
            "{step}"
        );
    }
}

/// The nodes' answers about `k1`, and the lens's placement of it.
fn k1() -> (Explained, Placement) {
    let model = cluster();
    let ranked = model.ownership("it").expect("the fixture ranks it");
    let spec = KeySpec::parse("k1").unwrap();
    (
        fixture::explained(model, SCRIPTED_ID, "k1", SystemTime::UNIX_EPOCH),
        Placement::of(&ranked.shares, &spec),
    )
}

/// The reading of node `at` of `answers`.
fn reading_mut(answers: &mut Explained, at: usize) -> &mut crate::explained::Reading {
    match &mut answers.nodes[at].outcome {
        Outcome::Read(reading) => reading,
        Outcome::Failed(why) => panic!("node {at} failed: {why}"),
    }
}

#[test]
fn answers_pass_only_when_every_node_read_the_key_and_all_agree_with_the_lens() {
    let (agreeing, placement) = k1();
    let judged = |answers: &Explained| judge(answers, Some(&placement));
    assert_eq!(
        judged(&agreeing),
        Ok("5 nodes agree, the lens computes the same".to_owned())
    );

    let mut nobody = agreeing.clone();
    nobody.nodes.clear();
    assert_eq!(judged(&nobody), Err("no node was asked".to_owned()));

    let mut silent = agreeing.clone();
    silent.nodes[1].outcome = Outcome::Failed("no answer within 3 s".to_owned());
    silent.nodes[3].outcome = Outcome::Failed("no answer (connection refused)".to_owned());
    assert_eq!(
        judged(&silent),
        Err(
            "2 of 5 nodes gave no explanation: n2: no answer within 3 s; \
             n4: no answer (connection refused)"
                .to_owned()
        )
    );

    let mut lone = agreeing.clone();
    lone.nodes.truncate(1);
    lone.nodes[0].outcome = Outcome::Failed("no answer (connection refused)".to_owned());
    assert_eq!(
        judged(&lone),
        Err("1 of 1 node gave no explanation: n1: no answer (connection refused)".to_owned())
    );

    // Every node holds a view the lens no longer has.
    let mut moved = agreeing.clone();
    for at in 0..5 {
        reading_mut(&mut moved, at)
            .distributed
            .as_mut()
            .unwrap()
            .view = "00c1a2f4e8b3195d".to_owned();
    }
    let why = judged(&moved).unwrap_err();
    assert!(why.starts_with("view moved 00c1a2f4 -> "), "{why}");
    assert!(!why.contains("Enter"), "no key to press in a run: {why}");

    // One node holds another view than the rest.
    let mut split = agreeing.clone();
    reading_mut(&mut split, 2)
        .distributed
        .as_mut()
        .unwrap()
        .view = "00c1a2f4e8b3195d".to_owned();
    let why = judged(&split).unwrap_err();
    assert!(why.starts_with("n1 n2 n4 n5 hold "), "{why}");
    assert!(why.ends_with(", n3 holds 00c1a2f4"), "{why}");

    // One node lists the owners in another order.
    let mut apart = agreeing.clone();
    reading_mut(&mut apart, 4)
        .distributed
        .as_mut()
        .unwrap()
        .owners
        .reverse();
    let why = judged(&apart).unwrap_err();
    assert!(
        why.starts_with("n5 differs from the lens, which holds view "),
        "{why}"
    );

    // One node's cache is not Distributed, so it compares nothing.
    let mut mixed = agreeing.clone();
    reading_mut(&mut mixed, 0).distributed = None;
    assert_eq!(
        judged(&mixed),
        Err("only 4 of 5 nodes gave a Distributed reading".to_owned())
    );

    // The lens has no placement, or places the key in another cache.
    assert_eq!(
        judge(&agreeing, None),
        Err("the lens has no placement of the key to compare with".to_owned())
    );
    let mut elsewhere = placement.clone();
    elsewhere.cache = "other".into();
    assert_eq!(
        judge(&agreeing, Some(&elsewhere)),
        Err(
            "the nodes explained cache it and the lens places the key in cache other: \
             nothing to compare"
                .to_owned()
        )
    );
}

#[test]
fn a_key_is_placed_in_the_cache_the_nodes_explained() {
    let (answers, expected) = k1();
    let digest = with_cluster_shares(ModelDigest::default());
    let spec = KeySpec::parse("k1").unwrap();
    assert_eq!(
        placement_in(&digest, &answers, &spec),
        Some(expected.clone())
    );

    // Another key lands on its own part in the same view.
    let k2 = KeySpec::parse("k2").unwrap();
    let other = placement_in(&digest, &answers, &k2).expect("k2 is placed");
    assert_eq!(other.part, sundog::store::PartId::of_key(k2.bytes()));
    assert_eq!(
        (other.cache.as_str(), other.view_hash),
        ("it", expected.view_hash)
    );

    // No shares in the digest, no reading, or a cache the digest does not
    // hold: nothing to place the key in.
    assert_eq!(placement_in(&ModelDigest::default(), &answers, &spec), None);
    let mut failed = answers.clone();
    for answer in &mut failed.nodes {
        answer.outcome = Outcome::Failed("no answer".to_owned());
    }
    assert_eq!(placement_in(&digest, &failed, &spec), None);
    let mut elsewhere = answers.clone();
    for at in 0..5 {
        reading_mut(&mut elsewhere, at).cache = "other".to_owned();
    }
    assert_eq!(placement_in(&digest, &elsewhere, &spec), None);
}
