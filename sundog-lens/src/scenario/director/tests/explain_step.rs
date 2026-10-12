//! The `explain` step: asking, asking again, timing out and failing.

use super::*;

/// What reached the interface, in order.
fn drain(ui: &mut mpsc::UnboundedReceiver<UiCommand>) -> Vec<UiCommand> {
    let mut commands = Vec::new();
    while let Ok(command) = ui.try_recv() {
        commands.push(command);
    }
    commands
}

/// The overlays the commands open: the key typed, and the answers.
fn overlays(commands: &[UiCommand]) -> Vec<(&str, &Explained)> {
    commands
        .iter()
        .filter_map(|command| match command {
            UiCommand::Explain { key, answers } => Some((key.as_str(), answers.as_ref())),
            _ => None,
        })
        .collect()
}

/// The commands as one word each.
fn kinds(commands: &[UiCommand]) -> Vec<&'static str> {
    commands
        .iter()
        .map(|command| match command {
            UiCommand::Explain { .. } => "explain",
            UiCommand::CloseExplain => "close",
            UiCommand::Caption(_) => "caption",
            UiCommand::Quit => "quit",
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn an_explain_step_asks_every_node_and_hands_the_overlay_the_answers() {
    let mut rig = explaining_rig(HEADLESS, Nodes::Agree);
    let scenario = parse(
        "explain k1 within 5s\n\
         pause 1s\n\
         explain close\n\
         explain str:close\n\
         explain close\n\
         quit",
    )
    .unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 6);
    assert!(outcome.timeouts.is_empty() && outcome.failures.is_empty());
    // The nodes are asked about the key after the overlay's prefix.
    assert_eq!(rig.director.stage.calls(), ["explain k1", "explain close"]);

    let commands = drain(&mut rig.ui);
    assert_eq!(
        kinds(&commands),
        ["explain", "close", "explain", "close", "quit"]
    );
    let shown = overlays(&commands);
    assert_eq!((shown[0].0, shown[0].1.key.as_str()), ("k1", "k1"));
    assert_eq!(
        (shown[1].0, shown[1].1.key.as_str()),
        ("str:close", "close"),
        "the overlay types the key as the scenario wrote it"
    );
    for (_, answers) in &shown {
        assert_eq!(answers.id, SCRIPTED_ID);
        let labels: Vec<&str> = answers.nodes.iter().map(|a| a.label.as_str()).collect();
        assert_eq!(labels, ["n1", "n2", "n3", "n4", "n5"]);
    }
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("] explain k1 within 5s"), "{log}");
    assert!(
        log.contains("ok after 0.0s: 5 nodes agree, the lens computes the same"),
        "{log}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_explain_needs_no_interface_to_ask_and_judge() {
    let (tx, rx) = watch::channel(with_cluster_shares(ModelDigest::default()));
    let none = Director::new(Fake::new(tx), rx, None, HEADLESS);
    let scenario = parse("explain k1\nexplain close").unwrap();
    let mut out = Vec::new();
    let outcome = none
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 2);
    assert_eq!(none.stage.calls(), ["explain k1"]);
    let log = String::from_utf8(out).unwrap();
    assert!(
        log.contains("5 nodes agree, the lens computes the same"),
        "{log}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_explain_asks_again_until_every_node_answers_and_agrees() {
    // Two nodes still opening their caches, then a view still moving,
    // then the cluster agrees.
    let mut rig = explaining_rig(HEADLESS, Nodes::Agree);
    rig.director.stage.script.lock().unwrap().extend([
        Nodes::OneRefuses,
        Nodes::OneRefuses,
        Nodes::Moved,
    ]);
    let scenario = parse("explain k1 within 5s\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert!(outcome.timeouts.is_empty() && outcome.failures.is_empty());
    assert_eq!(rig.director.stage.calls(), ["explain k1"; 4]);
    // One look at the digest after another, a quarter second apart.
    assert!(outcome.elapsed >= ASK_AGAIN * 3, "{:?}", outcome.elapsed);
    assert!(outcome.elapsed < ASK_AGAIN * 4, "{:?}", outcome.elapsed);
    let log = String::from_utf8(out).unwrap();
    // Each new reason is logged once, and the pass ends the wait.
    assert_eq!(log.matches("not yet after").count(), 2, "{log}");
    assert!(
        log.contains(
            "not yet after 0.0s: 1 of 5 nodes gave no explanation: \
             n3: no answer (connection refused)"
        ),
        "{log}"
    );
    assert!(log.contains("not yet after 0.5s: view moved "), "{log}");
    assert!(
        log.contains("ok after 0.8s: 5 nodes agree, the lens computes the same")
            || log.contains("ok after 0.7s: 5 nodes agree, the lens computes the same"),
        "{log}"
    );
    // The overlay opens once, on the answers that passed.
    let commands = drain(&mut rig.ui);
    assert_eq!(kinds(&commands), ["explain", "quit"]);
    let shown = overlays(&commands);
    assert_eq!(shown[0].1.readings().count(), 5);
}

#[test]
fn an_explain_asks_again_every_quarter_second_until_its_deadline() {
    assert_eq!(ask_again_in(S(0), S(5)), Some(ASK_AGAIN));
    assert_eq!(ask_again_in(S(4), S(5)), Some(ASK_AGAIN));
    // Less than a quarter second left: wait out what is left.
    let near = Duration::from_millis(4900);
    assert_eq!(ask_again_in(near, S(5)), Some(Duration::from_millis(100)));
    // At the deadline and past it: no more asks.
    assert_eq!(ask_again_in(S(5), S(5)), None);
    assert_eq!(ask_again_in(S(6), S(5)), None);
    assert_eq!(ask_again_in(S(0), S(0)), None);
    // A deadline no instant can hold is a wait that does not end.
    assert_eq!(ask_again_in(S(1_000_000), Duration::MAX), Some(ASK_AGAIN));
    assert_eq!(ask_again_in(Duration::MAX, Duration::MAX), None);
}

#[tokio::test(start_paused = true)]
async fn an_explain_with_a_deadline_no_instant_can_hold_asks_until_the_answers_pass() {
    let mut rig = explaining_rig(HEADLESS, Nodes::Agree);
    rig.director
        .stage
        .script
        .lock()
        .unwrap()
        .extend([Nodes::OneRefuses, Nodes::OneRefuses]);
    let scenario = parse("explain k1 within 18446744073709551615s\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert!(outcome.timeouts.is_empty() && outcome.failures.is_empty());
    assert_eq!(rig.director.stage.calls(), ["explain k1"; 3]);
    assert!(outcome.elapsed >= ASK_AGAIN * 2, "{:?}", outcome.elapsed);
    assert_eq!(kinds(&drain(&mut rig.ui)), ["explain", "quit"]);
    let log = String::from_utf8(out).unwrap();
    assert!(
        log.contains("5 nodes agree, the lens computes the same"),
        "{log}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_explain_waits_for_slow_answers_and_gives_up_at_its_deadline() {
    let rig = explaining_rig(HEADLESS, Nodes::Late(S(3)));
    let scenario = parse("explain k1 within 5s").unwrap();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    assert!(outcome.elapsed >= S(3), "{:?}", outcome.elapsed);
    assert!(outcome.elapsed < S(4), "{:?}", outcome.elapsed);

    let rig = explaining_rig(HEADLESS, Nodes::Late(S(3)));
    let scenario = parse("explain k1 within 2s").unwrap();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap_err();
    assert!(matches!(error, PlayError::TimedOut(_)), "{error}");
}

#[tokio::test(start_paused = true)]
async fn an_explain_nobody_answers_times_out_like_an_await() {
    // Headless, it is an error that names the line.
    let rig = explaining_rig(HEADLESS, Nodes::Never);
    let scenario = parse("pause 1s\nexplain k1 within 2s\nquit").unwrap();
    let mut out = Vec::new();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap_err();
    match &error {
        PlayError::TimedOut(Timeout { line, step }) => {
            assert_eq!(*line, 2);
            assert_eq!(step, "explain k1 within 2s");
        }
        other => panic!("not a timeout: {other}"),
    }
    assert_eq!(
        error.to_string(),
        "line 2: `explain k1 within 2s` timed out"
    );
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("timed out after 2.0s")
    );

    // Without a `within` it waits the default.
    let rig = explaining_rig(HEADLESS, Nodes::Never);
    let mut out = Vec::new();
    rig.director
        .play(
            &parse("explain k1").unwrap(),
            Instant::now(),
            &mut out,
            None,
        )
        .await
        .unwrap_err();
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("timed out after 30.0s"), "{log}");

    // With an interface, the run warns, captions it and goes on.
    let _logging = LOGGING.lock().await;
    let (captured, _guard) = capture_logs();
    let mut rig = explaining_rig(SHOW, Nodes::Never);
    let scenario = parse("explain k1 within 2s\nfill 5\nquit").unwrap();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 3);
    assert_eq!(
        outcome.timeouts,
        [Timeout {
            line: 1,
            step: "explain k1 within 2s".into()
        }]
    );
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    assert_eq!(rig.director.stage.calls(), ["explain k1", "fill 5"]);
    assert_eq!(
        drain(&mut rig.ui),
        [
            UiCommand::Caption(Some(TIMED_OUT_CAPTION.into())),
            UiCommand::Quit
        ]
    );
    let text = captured.text();
    assert!(text.contains("WARN"), "{text}");
    assert!(text.contains("await timed out after 2.0s"), "{text}");
    assert!(text.contains("explain k1 within 2s"), "{text}");
}

#[tokio::test(start_paused = true)]
async fn answers_that_disagree_with_the_lens_fail_the_step_and_the_overlay_shows_them() {
    let mut rig = explaining_rig(HEADLESS, Nodes::Moved);
    let scenario = parse("fill 1\nexplain k1 within 5s\nexplain close\nquit").unwrap();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap_err();
    let PlayError::Step { line, step, source } = &error else {
        panic!("not a failed step: {error}");
    };
    assert_eq!((*line, step.as_str()), (2, "explain k1 within 5s"));
    let why = source.to_string();
    assert!(why.starts_with("view moved 00c1a2f4 -> "), "{why}");
    assert!(!why.contains("Enter"), "no key to press in a run: {why}");
    assert_eq!(
        error.to_string(),
        format!("line 2: `explain k1 within 5s` failed: {why}")
    );
    // The nodes were asked again until the deadline, the run stopped
    // there, and the overlay was handed the last answers once.
    let calls = rig.director.stage.calls();
    assert_eq!(calls[0], "fill 1");
    assert!(calls.len() > 3, "{calls:?}");
    assert!(
        calls[1..].iter().all(|call| call == "explain k1"),
        "{calls:?}"
    );
    let commands = drain(&mut rig.ui);
    assert_eq!(kinds(&commands), ["explain"]);

    // With an interface the run warns of the failure and goes on.
    let _logging = LOGGING.lock().await;
    let (captured, _guard) = capture_logs();
    let mut rig = explaining_rig(SHOW, Nodes::Moved);
    let scenario = parse("explain k1\nexplain close\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 3);
    assert!(outcome.timeouts.is_empty(), "{:?}", outcome.timeouts);
    assert_eq!(outcome.failures, [(1, why)]);
    assert_eq!(
        kinds(&drain(&mut rig.ui)),
        ["explain", "close", "quit"],
        "the overlay opens and closes as scripted"
    );
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("failed: view moved 00c1a2f4 -> "), "{log}");
    let text = captured.text();
    assert!(text.contains("WARN"), "{text}");
    assert!(
        text.contains("step failed: view moved 00c1a2f4 -> "),
        "{text}"
    );
    assert!(text.contains("explain k1"), "{text}");
}

#[tokio::test(start_paused = true)]
async fn an_explain_fails_when_a_node_gives_no_explanation_or_nobody_is_asked() {
    for (nodes, expected) in [
        (
            Nodes::OneRefuses,
            "1 of 5 nodes gave no explanation: n3: no answer (connection refused)",
        ),
        (Nodes::Nobody, "no node was asked"),
        (Nodes::Broken, "no node is running to ask"),
    ] {
        let mut rig = explaining_rig(HEADLESS, nodes);
        let error = rig
            .director
            .play(
                &parse("explain k1").unwrap(),
                Instant::now(),
                &mut Vec::new(),
                None,
            )
            .await
            .unwrap_err();
        let PlayError::Step { source, .. } = &error else {
            panic!("{nodes:?}: not a failed step: {error}");
        };
        assert_eq!(source.to_string(), expected, "{nodes:?}");
        // A stage that cannot ask has no answers for the overlay.
        let opened = overlays(&drain(&mut rig.ui)).len();
        assert_eq!(opened, usize::from(!matches!(nodes, Nodes::Broken)));
    }
}

#[tokio::test(start_paused = true)]
async fn an_explain_without_a_placement_or_a_key_to_ask_fails() {
    // The digest holds no shares of the cache the nodes explained.
    let rig = rig(HEADLESS, |_| {});
    let error = rig
        .director
        .play(
            &parse("explain k1").unwrap(),
            Instant::now(),
            &mut Vec::new(),
            None,
        )
        .await
        .unwrap_err();
    let PlayError::Step { source, .. } = &error else {
        panic!("not a failed step: {error}");
    };
    assert_eq!(
        source.to_string(),
        "the lens has no placement of the key to compare with"
    );
    let calls = rig.director.stage.calls();
    assert!(calls.len() > 3, "asked again until the deadline: {calls:?}");

    // The parser refuses a key a test node cannot be asked, so a step
    // built by hand is the director's to refuse, before it asks anyone.
    let rig = explaining_rig(HEADLESS, Nodes::Agree);
    let scenario = Scenario {
        steps: vec![Entry {
            line: 7,
            step: Step::Explain {
                key: "uint:5".into(),
                within: None,
            },
        }],
    };
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap_err();
    let PlayError::Step { line, source, .. } = &error else {
        panic!("not a failed step: {error}");
    };
    assert_eq!(*line, 7);
    assert!(source.to_string().contains("cannot be asked"), "{source}");
    let calls = rig.director.stage.calls();
    assert!(calls.is_empty(), "{calls:?}");
}
