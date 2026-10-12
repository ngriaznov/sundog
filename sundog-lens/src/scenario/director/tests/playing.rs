//! Whole runs of `Director::play` against a fake stage, and the built-in tour.

use super::*;

#[tokio::test(start_paused = true)]
async fn a_scenario_plays_in_order_and_awaits_what_it_changed() {
    let mut rig = rig(HEADLESS, |_| {});
    let scenario = parse(
        "caption \"hello\"\n\
         pause 2s\n\
         spawn 3 stagger 1s\n\
         await members 3 within 20s\n\
         fill 100\n\
         load start\n\
         await settled it within 25s\n\
         tab caches\n\
         select n2\n\
         cache it\n\
         help on\n\
         quit\n",
    )
    .unwrap();
    let mut out = Vec::new();
    let mut marks = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, Some(&mut marks))
        .await
        .unwrap();
    assert_eq!(outcome.steps, 12);
    assert!(outcome.timeouts.is_empty() && outcome.failures.is_empty());
    assert!(outcome.elapsed >= S(2));
    assert_eq!(
        rig.director.stage.calls(),
        ["spawn 3 Some(1s)", "fill 100", "load true"]
    );
    let mut commands = Vec::new();
    while let Ok(command) = rig.ui.try_recv() {
        commands.push(command);
    }
    assert_eq!(
        commands,
        [
            UiCommand::Caption(Some("hello".into())),
            UiCommand::Tab(crate::ui::View::Caches),
            UiCommand::Select("n2".into()),
            UiCommand::Cache("it".into()),
            UiCommand::Help(true),
            UiCommand::Quit,
        ]
    );
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("spawn 3 stagger 1s"), "{log}");
    assert!(log.contains("ok after"), "{log}");
    assert!(
        log.contains("scenario done: 12 steps, 0 timeouts, 0 failures"),
        "{log}"
    );
    let marks = String::from_utf8(marks).unwrap();
    assert_eq!(marks.lines().count(), 12);
    let second: Vec<&str> = marks.lines().nth(2).unwrap().splitn(2, ' ').collect();
    assert!(second[0].parse::<f64>().unwrap() >= 2.0, "{marks}");
    assert_eq!(second[1], "spawn 3 stagger 1s");
}

#[tokio::test(start_paused = true)]
async fn a_settled_await_waits_for_the_view_to_move_and_settle() {
    let rig = rig(HEADLESS, |fake| fake.settle_after = S(4));
    let scenario = parse("spawn 1\nawait settled it within 25s\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    // The cache settled four seconds after the spawn, so the await took
    // that long and no less.
    assert!(outcome.elapsed >= S(4), "{:?}", outcome.elapsed);
    assert!(outcome.elapsed < S(6), "{:?}", outcome.elapsed);
}

#[tokio::test(start_paused = true)]
async fn the_baseline_is_taken_before_the_action_so_a_fast_view_change_still_counts() {
    // The view changes and settles inside the spawn itself: a baseline
    // taken after the action would equal the new view and never pass.
    let rig = rig(HEADLESS, |fake| fake.settle_after = Duration::ZERO);
    let scenario =
        parse("spawn 1\nawait settled it within 5s\nspawn 1\nawait settled it within 5s").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert!(outcome.timeouts.is_empty(), "{:?}", outcome.timeouts);
    assert_eq!(outcome.steps, 4);
}

#[tokio::test(start_paused = true)]
async fn a_headless_run_that_times_out_is_an_error_naming_the_line() {
    let rig = rig(HEADLESS, |_| {});
    let scenario = parse("pause 1s\nawait members 9 within 2s\nquit").unwrap();
    let mut out = Vec::new();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap_err();
    match &error {
        PlayError::TimedOut(Timeout { line, step }) => {
            assert_eq!(*line, 2);
            assert_eq!(step, "await members 9 within 2s");
        }
        other => panic!("not a timeout: {other}"),
    }
    assert_eq!(
        error.to_string(),
        "line 2: `await members 9 within 2s` timed out"
    );
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("timed out after 2.0s")
    );
}

#[tokio::test(start_paused = true)]
async fn a_run_with_an_interface_notes_a_timeout_shows_a_caption_and_goes_on() {
    let _logging = LOGGING.lock().await;
    let mut rig = rig(SHOW, |_| {});
    let scenario = parse("await members 9 within 2s\nfill 5\nquit").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 3);
    assert_eq!(
        outcome.timeouts,
        [Timeout {
            line: 1,
            step: "await members 9 within 2s".into()
        }]
    );
    assert_eq!(rig.director.stage.calls(), ["fill 5"]);
    assert_eq!(
        rig.ui.try_recv().unwrap(),
        UiCommand::Caption(Some(TIMED_OUT_CAPTION.into()))
    );
    assert_eq!(rig.ui.try_recv().unwrap(), UiCommand::Quit);
}

#[tokio::test(start_paused = true)]
async fn a_failed_step_ends_a_headless_run_and_is_noted_otherwise() {
    let _logging = LOGGING.lock().await;
    let headless = rig(HEADLESS, |fake| fake.fail_kill = true);
    let scenario = parse("fill 1\nkill n7\nfill 2").unwrap();
    let mut out = Vec::new();
    let error = headless
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "line 2: `kill n7` failed: n7 is not running"
    );
    assert_eq!(headless.director.stage.calls(), ["fill 1", "kill n7"]);

    let shown = rig(SHOW, |fake| fake.fail_kill = true);
    let mut out = Vec::new();
    let outcome = shown
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 3);
    assert_eq!(outcome.failures, [(2, "n7 is not running".to_owned())]);
    assert_eq!(
        shown.director.stage.calls(),
        ["fill 1", "kill n7", "fill 2"]
    );
    assert!(
        String::from_utf8(out)
            .unwrap()
            .contains("failed: n7 is not running")
    );
}

#[tokio::test(start_paused = true)]
async fn captions_can_be_turned_off_and_a_run_without_an_interface_is_fine() {
    let mut quiet = rig(
        Options {
            headless: true,
            captions: false,
        },
        |_| {},
    );
    let scenario = parse("caption \"hidden\"\ntab node\n").unwrap();
    let mut out = Vec::new();
    quiet
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(
        quiet.ui.try_recv().unwrap(),
        UiCommand::Tab(crate::ui::View::Node)
    );
    // The scenario has no `quit` step, so the director ends it.
    assert_eq!(quiet.ui.try_recv().unwrap(), UiCommand::Quit);
    assert!(quiet.ui.try_recv().is_err());

    let (tx, rx) = watch::channel(ModelDigest::default());
    let none = Director::new(Fake::new(tx), rx, None, HEADLESS);
    let mut out = Vec::new();
    let outcome = none
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 2);
}

#[test]
fn a_failure_to_write_the_log_is_a_play_error_that_says_so() {
    let error = PlayError::from(io::Error::other("disk full"));
    assert!(matches!(error, PlayError::Io(_)));
    assert_eq!(error.to_string(), "writing the scenario log: disk full");
}

/// A writer that fails.
struct Broken;

impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("broken pipe"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn a_log_that_cannot_be_written_ends_the_run_with_an_error() {
    let rig = rig(HEADLESS, |_| {});
    let scenario = parse("fill 1\nquit").unwrap();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut Broken, None)
        .await
        .unwrap_err();
    assert!(matches!(error, PlayError::Io(_)), "{error}");
    let mut out = Vec::new();
    let error = rig
        .director
        .play(&scenario, Instant::now(), &mut out, Some(&mut Broken))
        .await
        .unwrap_err();
    assert!(matches!(error, PlayError::Io(_)), "{error}");
}

#[tokio::test(start_paused = true)]
async fn a_scenario_stops_at_quit_and_an_empty_scenario_is_done_at_once() {
    let rig = rig(HEADLESS, |_| {});
    let scenario = parse("fill 1\nquit\nfill 2").unwrap();
    let mut out = Vec::new();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 2);
    assert_eq!(rig.director.stage.calls(), ["fill 1"]);

    let empty = rig
        .director
        .play(&Scenario::default(), Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(empty.steps, 0);
}

/// The stamps of the `[ 12.3s]` lines of a log, in order.
fn stamps(log: &str) -> Vec<f64> {
    log.lines()
        .filter_map(|line| {
            let inner = line.strip_prefix('[')?.split_once("s]")?.0;
            inner.trim().parse().ok()
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn every_stamp_counts_from_the_origin_the_caller_gives_and_never_goes_back() {
    let rig = rig(HEADLESS, |fake| fake.settle_after = S(2));
    let scenario = parse(
        "pause 1s\nspawn 1\nawait members 1 within 5s\n\
         await settled it within 25s\npause 1s\nquit",
    )
    .unwrap();
    // The run began 10 s before the director took over.
    let start = Instant::now() - S(10);
    let mut out = Vec::new();
    let mut marks = Vec::new();
    rig.director
        .play(&scenario, start, &mut out, Some(&mut marks))
        .await
        .unwrap();
    let log = String::from_utf8(out).unwrap();
    let stamps = stamps(&log);
    assert!(stamps.len() >= 9, "{log}");
    assert!(stamps[0] >= 10.0, "{log}");
    assert!(stamps.windows(2).all(|pair| pair[0] <= pair[1]), "{log}");
    let marks = String::from_utf8(marks).unwrap();
    let first: f64 = marks.split(' ').next().unwrap().parse().unwrap();
    assert!(first >= 10.0, "{marks}");
}

#[tokio::test(start_paused = true)]
async fn a_scenario_without_quit_tells_the_interface_to_quit_after_its_last_step() {
    let mut rig = rig(SHOW, |_| {});
    let scenario = parse("tab node\nfill 1").unwrap();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, 2);
    assert_eq!(
        rig.ui.try_recv().unwrap(),
        UiCommand::Tab(crate::ui::View::Node)
    );
    assert_eq!(rig.ui.try_recv().unwrap(), UiCommand::Quit);
    assert!(rig.ui.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn a_run_with_an_interface_logs_a_warning_for_a_timeout_and_a_failed_step() {
    let _logging = LOGGING.lock().await;
    let (captured, _guard) = capture_logs();
    let scenario = parse("await members 9 within 2s\nkill n7\nquit").unwrap();

    let shown = rig(SHOW, |fake| fake.fail_kill = true);
    shown
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    let text = captured.text();
    assert!(text.contains("WARN"), "{text}");
    assert!(text.contains("await timed out after 2.0s"), "{text}");
    assert!(text.contains("await members 9 within 2s"), "{text}");
    assert!(text.contains("step failed: n7 is not running"), "{text}");

    // A headless run reports through its error and stays out of the log.
    let before = captured.text();
    let headless = rig(HEADLESS, |_| {});
    let timeout = parse("await members 9 within 2s").unwrap();
    headless
        .director
        .play(&timeout, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap_err();
    assert_eq!(captured.text(), before);
}

#[test]
fn only_a_kill_and_a_crash_leave_without_a_goodbye() {
    assert_eq!(
        abrupt_exit(&Step::Kill("n3".into())),
        Some(("n3", "SIGKILL"))
    );
    assert_eq!(
        abrupt_exit(&Step::Crash("n2".into())),
        Some(("n2", "crash"))
    );
    for step in [
        Step::Leave("n1".into()),
        Step::Restart("n1".into()),
        Step::Quit,
    ] {
        assert_eq!(abrupt_exit(&step), None, "{step}");
    }
    assert_eq!(
        detected_caption("n3", "SIGKILL", Duration::from_millis(840)),
        "n3 marked down 0.8 s after the SIGKILL"
    );
}

#[tokio::test(start_paused = true)]
async fn the_caption_after_a_kill_says_how_long_the_detector_took() {
    let _logging = LOGGING.lock().await;
    let mut rig = rig(SHOW, |fake| {
        fake.down_after = Some(Duration::from_millis(800));
    });
    let scenario = parse(
        "kill n3\nawait down n3 within 5s\nkill n2\nawait down n9 within 1s\ncrash n1\nawait down n1 within 5s",
    )
    .unwrap();
    let outcome = rig
        .director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    // `await down n9` times out: no caption about it.
    assert_eq!(outcome.timeouts.len(), 1);
    let mut captions = Vec::new();
    while let Ok(command) = rig.ui.try_recv() {
        if let UiCommand::Caption(Some(text)) = command {
            captions.push(text);
        }
    }
    assert_eq!(
        captions,
        [
            "n3 marked down 0.8 s after the SIGKILL".to_owned(),
            TIMED_OUT_CAPTION.to_owned(),
            "n1 marked down 0.8 s after the crash".to_owned(),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn no_detection_caption_without_captions_or_without_an_interface() {
    let quiet = Options {
        headless: false,
        captions: false,
    };
    let mut rig = rig(quiet, |fake| {
        fake.down_after = Some(Duration::from_millis(300));
    });
    let scenario = parse("kill n3\nawait down n3 within 5s").unwrap();
    rig.director
        .play(&scenario, Instant::now(), &mut Vec::new(), None)
        .await
        .unwrap();
    while let Ok(command) = rig.ui.try_recv() {
        assert!(
            !matches!(command, UiCommand::Caption(_)),
            "no caption: {command:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_built_in_tour_runs_against_a_fake_cluster() {
    // A fake that follows the tour: it counts live members and moves the
    // view on every spawn, kill, leave and restart.
    struct Tour {
        digests: watch::Sender<ModelDigest>,
    }
    impl Tour {
        fn bump(&self, f: impl FnOnce(&mut ModelDigest)) {
            self.digests.send_modify(|d| {
                f(d);
                let next = d.view_hash.get("it").copied().unwrap_or(0) + 1;
                d.view_hash.insert("it".into(), next);
                d.settled.insert("it".into(), false);
            });
            let digests = self.digests.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(800)).await;
                digests.send_modify(|d| {
                    d.settled.insert("it".into(), true);
                });
            });
        }
        fn status(&self, label: &str, status: MemberStatus, live_delta: isize) {
            let label = label.to_owned();
            self.bump(|d| {
                d.statuses.insert(label.as_str().into(), status);
                d.live = d.live.saturating_add_signed(live_delta);
            });
        }
    }
    #[expect(clippy::unused_async_trait_impl, reason = "the fake never waits")]
    impl Stage for Tour {
        async fn spawn(&self, count: usize, _: Option<Duration>) -> anyhow::Result<()> {
            for _ in 0..count {
                let n = self.digests.borrow().statuses.len() + 1;
                self.status(&format!("n{n}"), MemberStatus::Live, 1);
            }
            Ok(())
        }
        async fn fill(&self, _: u64) -> anyhow::Result<()> {
            Ok(())
        }
        fn load(&self, _: bool) {}
        async fn kill(&self, label: &str) -> anyhow::Result<()> {
            self.status(label, MemberStatus::Down, -1);
            Ok(())
        }
        async fn leave(&self, label: &str) -> anyhow::Result<()> {
            self.status(label, MemberStatus::Departing, -1);
            let digests = self.digests.clone();
            let label = label.to_owned();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
                digests.send_modify(|d| {
                    d.statuses.insert(label.as_str().into(), MemberStatus::Left);
                });
            });
            Ok(())
        }
        async fn crash(&self, label: &str) -> anyhow::Result<()> {
            self.status(label, MemberStatus::Down, -1);
            Ok(())
        }
        async fn restart(&self, label: &str) -> anyhow::Result<()> {
            self.status(label, MemberStatus::Live, 1);
            Ok(())
        }
        async fn explain(&self, key: &str) -> anyhow::Result<Explained> {
            Ok(converged_answers(key))
        }
    }
    let (tx, rx) = watch::channel(with_cluster_shares(ModelDigest::default()));
    let director = Director::new(Tour { digests: tx }, rx, None, HEADLESS);
    let scenario = parse(crate::scenario::TOUR).unwrap();
    let mut out = Vec::new();
    let outcome = director
        .play(&scenario, Instant::now(), &mut out, None)
        .await
        .unwrap();
    assert_eq!(outcome.steps, scenario.steps.len());
    assert!(outcome.timeouts.is_empty(), "{:?}", outcome.timeouts);
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    // The tour's explain scene asks and passes.
    let log = String::from_utf8(out).unwrap();
    assert!(log.contains("] explain k1 within 10s"), "{log}");
    assert!(
        log.contains("5 nodes agree, the lens computes the same"),
        "{log}"
    );
}
