//! The popup frame, the verdict line, changed and frozen views, mono mode and answers about another cache.

use ratatui::style::Color;

use super::*;

#[test]
fn a_moved_view_dims_the_result_and_says_enter_asks_again() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        for answer in &mut answers.nodes {
            if let Outcome::Read(reading) = &mut answer.outcome {
                reading.distributed.as_mut().unwrap().view = "00c1a2f4e8b3195d".to_owned();
            }
        }
    });
    let digest = model.ownership("it").unwrap();
    let lens = eventlog::view_hash(digest.view_hash);
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(
            &rows,
            &format!("view moved 00c1a2f4 -> {lens}: Enter asks again")
        ),
        "{rows:?}"
    );
    let ctx = ctx_of(&model);
    let scene = Scene {
        app: &app,
        model: &model,
        ctx: &ctx,
        look: app.look(),
        kind: LayoutKind::Full,
    };
    let state = app.explain.as_ref().unwrap();
    let drawn = lines(&scene, state, 116, 40);
    let dimmed = |line: &Line<'_>| {
        line.spans
            .iter()
            .filter(|span| !span.content.trim().is_empty())
            .all(|span| span.style.add_modifier.contains(Modifier::DIM))
    };
    let node_rows: Vec<_> = drawn
        .iter()
        .filter(|line| text_of(line).contains(" hit "))
        .collect();
    assert_eq!(node_rows.len(), 5);
    assert!(node_rows.iter().all(|line| dimmed(line)));
    let detail = drawn
        .iter()
        .find(|line| text_of(line).contains("record  "))
        .unwrap();
    assert!(dimmed(detail));
    let summary = drawn
        .iter()
        .find(|line| text_of(line).contains("Enter asks again"))
        .unwrap();
    assert!(!dimmed(summary), "the verdict itself stays bright");
}

#[test]
fn a_frozen_display_is_named() {
    let model = model();
    let mut app = opened(&model, "k1");
    let rows = drawn(&app, &model, 140, 37);
    assert!(!has(&rows, "display frozen"), "{rows:?}");
    // `p` types while the overlay is open; the display freezes by the
    // model the overlay was drawn from.
    app.explain = None;
    press(&mut app, &model, KeyCode::Char('p'));
    press(&mut app, &model, KeyCode::Char('e'));
    assert!(app.is_frozen());
    let rows = drawn(&app, &model, 140, 37);
    assert!(
        has(&rows, "Explain a read · it · computed (display frozen)"),
        "{rows:?}"
    );
}

#[test]
fn the_popup_is_a_rounded_box_centered_in_the_body() {
    let model = model();
    let app = opened(&model, "k1");
    let rows = drawn(&app, &model, 140, 37);
    let top = rows.iter().position(|row| row.contains('╭')).unwrap();
    let bottom = rows.iter().rposition(|row| row.contains('╯')).unwrap();
    assert!(top >= 1 && bottom <= 35, "{top} {bottom}");
    let chars: Vec<char> = rows[top].chars().collect();
    let left = chars.iter().position(|c| *c == '╭').unwrap();
    let right = chars.iter().rposition(|c| *c == '╮').unwrap();
    assert!(left.abs_diff(140 - 1 - right) <= 1, "{left} {right}");
    assert_eq!(
        right - left + 1,
        usize::from(MAX_WIDTH),
        "the popup is MAX_WIDTH wide"
    );
    // Nothing fits a body under 12 columns or 3 rows: nothing is drawn.
    for (w, h) in [(10, 30), (140, 4), (0, 0)] {
        assert!(
            drawn(&app, &model, w, h)
                .iter()
                .all(|row| row.trim().is_empty())
        );
    }
}

#[test]
fn mono_mode_carries_the_verdict_in_words() {
    let model = model();
    let mono = Look::with_mode(&crate::cli::DisplayArgs::default(), ColorMode::Mono);
    let mut app = app_in(mono, Some(control()));
    typed(&mut app, &model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        answers.nodes[1].outcome = Outcome::Failed("no answer (connection refused)".to_owned());
    });
    let rendered = |app: &App| {
        let ctx = ctx_of(&model);
        let scene = Scene {
            app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        lines(&scene, app.explain.as_ref().unwrap(), 116, 40)
    };
    let lines_now = rendered(&app);
    let words: Vec<String> = lines_now.iter().map(text_of).collect();
    assert!(
        has(&words, "✔ 4 nodes agree, the lens computes the same"),
        "{words:?}"
    );
    assert!(has(&words, "no answer (connection refused)"), "{words:?}");
    assert!(has(&words, "owner  1 ◆"), "{words:?}");
    for line in &lines_now {
        for span in &line.spans {
            for color in [span.style.fg, span.style.bg].into_iter().flatten() {
                assert_eq!(color, Color::Reset, "{span:?}");
            }
        }
    }
    // A split verdict reads the same without color.
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        if let Outcome::Read(reading) = &mut answers.nodes[0].outcome {
            reading.distributed.as_mut().unwrap().view = "71c0a2f4e8b3195d".to_owned();
        }
    });
    let words: Vec<String> = rendered(&app).iter().map(text_of).collect();
    assert!(
        has(&words, "↻ n1 holds 71c0a2f4, n2 n3 n4 n5 hold"),
        "{words:?}"
    );
}

#[test]
fn the_age_of_the_answers_is_counted_from_the_frame_clock() {
    let model = model();
    let wall = model.wall().expect("the model has a wall clock");
    let asked_ago = |ago: Duration, later: bool| {
        let mut app = opened(&model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            answers.asked = if later { wall + ago } else { wall - ago };
        });
        find(&shown(&app, &model, 116, 40), "nodes agree").to_owned()
    };
    assert!(asked_ago(Duration::from_millis(3_400), false).ends_with("asked 3.4 s ago"));
    assert!(asked_ago(Duration::from_secs(250), false).ends_with("asked 4m ago"));
    assert!(asked_ago(Duration::from_secs(7_300), false).ends_with("asked 2h ago"));
    // A frame clock that has not reached the answers reads no negative
    // age.
    assert!(asked_ago(Duration::from_secs(5), true).ends_with("asked 0.0 s ago"));
}

#[test]
fn answers_with_no_placement_to_compare_with_are_listed_without_a_verdict() {
    // A cluster the lens has not ranked yet: the nodes still answer.
    let snapshot = testkit::snapshot_with_owners(3, 2);
    let model = model_of(&snapshot);
    let mut app = opened(&model, "k1");
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(
            &rows,
            "is Distributed but the lens has not ranked its parts yet"
        ),
        "{rows:?}"
    );
    let request = ask(&mut app, &model);
    let reading = crate::explained::parse_reply(include_str!(
        "../../../../tests/fixtures/explain/owner.json"
    ))
    .expect("the fixture parses");
    let answers = Explained {
        id: request.id,
        key: request.key.clone(),
        asked: SystemTime::UNIX_EPOCH,
        nodes: vec![NodeAnswer {
            label: "n1".into(),
            outcome: Outcome::Read(Box::new(reading)),
        }],
    };
    app.apply_director(UiCommand::Explained(Box::new(answers)), &model);
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(
            &rows,
            "· the lens has no placement of the key to compare with"
        ),
        "{rows:?}"
    );
    assert!(has(&rows, "n1 "), "the node's row stays");
    // Nodes that all failed leave a Distributed cache with nothing to
    // compare either.
    let model = self::model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        for answer in &mut answers.nodes {
            answer.outcome = Outcome::Failed("no answer within 3 s".to_owned());
        }
    });
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(&rows, "· no node gave a Distributed reading to compare"),
        "{rows:?}"
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r.contains("no answer within 3 s"))
            .count(),
        5 + 1
    );
}

/// Five live nodes, all advertising `it` as `Distributed` with two
/// owners; n1 to n4 also advertise `other` as `Distributed` with
/// `other_owners`. Both caches are ranked.
fn two_distributed_caches(other_owners: u8) -> Model {
    use sundog::observe::{ClusterSnapshot, MemberStatus};

    let snapshot = ClusterSnapshot::new(
        "fixture",
        (1..=5)
            .map(|index| {
                let mut caches = vec![("it", testkit::distributed(2))];
                if index <= 4 {
                    caches.push(("other", testkit::distributed(other_owners)));
                }
                testkit::member_with(index, 0, 1, MemberStatus::Live, &caches)
            })
            .collect(),
        0,
    );
    let mut model = model_of(&snapshot);
    for (cache, owners) in [("it", 2), ("other", other_owners)] {
        let digest = testkit::ownership_digest_with_owners(
            &snapshot,
            cache,
            std::num::NonZeroU8::new(owners).expect("owners is nonzero"),
            None,
        )
        .expect("a live member advertises the cache");
        model.apply(
            Update::Ownership(digest),
            Instant::now(),
            SystemTime::UNIX_EPOCH + Duration::from_secs(100),
        );
    }
    model
}

#[test]
fn answers_about_another_cache_are_not_compared_with_the_block_above() {
    let model = two_distributed_caches(3);
    let mut app = app_in(Look::default(), Some(control()));
    // The test nodes always explain `it`; the block places the key in the
    // cache the Ownership panel shows.
    app.apply_director(UiCommand::Cache("other".into()), &model);
    typed(&mut app, &model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |_| {});
    let note = "the nodes explained cache it and the block above is cache other: \
                nothing to compare";
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(&rows, &format!("· {note} · asked 3.0 s ago")),
        "{rows:?}"
    );
    assert!(!has(&rows, "view moved"), "{rows:?}");
    assert!(!has(&rows, "nodes agree"), "{rows:?}");
    let title = drawn(&app, &model, 140, 40);
    assert!(
        has(&title, "Explain a read · other · computed · asked"),
        "{title:?}"
    );
    // Nothing moved, so nothing dims and Enter has nothing to redo.
    let ctx = ctx_of(&model);
    let scene = Scene {
        app: &app,
        model: &model,
        ctx: &ctx,
        look: app.look(),
        kind: LayoutKind::Full,
    };
    let drawn = lines(&scene, app.explain.as_ref().unwrap(), 116, 40);
    let dimmed = |line: &Line<'_>| {
        line.spans
            .iter()
            .any(|span| span.style.add_modifier.contains(Modifier::DIM))
    };
    let node_rows: Vec<_> = drawn
        .iter()
        .filter(|line| text_of(line).contains(" hit "))
        .collect();
    assert_eq!(node_rows.len(), 5);
    assert!(node_rows.iter().all(|line| !dimmed(line)));

    // Nodes that hold the very placement the lens computes for `other`
    // still did not explain it.
    let located = locate(&model, Some("other"), &KeySpec::parse("k1").unwrap()).unwrap();
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        for answer in &mut answers.nodes {
            if let Outcome::Read(reading) = &mut answer.outcome {
                reading.bucket = located.part.bucket();
                reading.part = located.part.part();
                let dist = reading.distributed.as_mut().unwrap();
                dist.view = format!("{:016x}", located.view_hash);
                dist.owners = located.owners.iter().map(|o| o.node.to_string()).collect();
            }
        }
    });
    let rows = shown(&app, &model, 116, 40);
    assert!(has(&rows, note), "{rows:?}");
    assert!(!has(&rows, "agree"), "{rows:?}");

    // The cache the nodes did explain is compared as before.
    app.apply_director(UiCommand::Cache("it".into()), &model);
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |_| {});
    let rows = shown(&app, &model, 116, 40);
    assert!(has(&rows, "✔ 5 nodes agree"), "{rows:?}");
    assert!(!has(&rows, "nothing to compare"), "{rows:?}");
}

#[test]
fn the_name_of_a_cache_from_a_node_is_drawn_through_the_allowlist() {
    let model = two_distributed_caches(3);
    let mut app = app_in(Look::default(), Some(control()));
    app.apply_director(UiCommand::Cache("other".into()), &model);
    typed(&mut app, &model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        for answer in &mut answers.nodes {
            if let Outcome::Read(reading) = &mut answer.outcome {
                reading.cache = "\u{1b}[2Jit".to_owned();
            }
        }
    });
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(&rows, "the nodes explained cache ·[2Jit and"),
        "{rows:?}"
    );
    for row in &rows {
        assert!(
            row.chars().all(|c| theme::is_allowed(c) && !c.is_control()),
            "{row}"
        );
    }
}

/// An overlay on a frozen display, asked about `k1`, whose answers carry
/// a view the frozen digest does not, asked `after` the freeze instant.
fn frozen_moved(model: &Model, after: Option<Duration>) -> App {
    let wall = model.wall().expect("the model has a wall clock");
    let mut app = app_in(Look::default(), Some(control()));
    press(&mut app, model, KeyCode::Char('p'));
    typed(&mut app, model, "k1");
    assert!(app.is_frozen());
    let request = ask(&mut app, model);
    answered(&mut app, model, &request, |answers| {
        answers.asked = after.map_or(wall - Duration::from_secs(3), |gap| wall + gap);
        for answer in &mut answers.nodes {
            if let Outcome::Read(reading) = &mut answer.outcome {
                reading.distributed.as_mut().unwrap().view = "00c1a2f4e8b3195d".to_owned();
            }
        }
    });
    app
}

#[test]
fn a_moved_view_under_a_frozen_display_names_the_unfreeze_not_asking_again() {
    let model = model();
    let lens = eventlog::view_hash(model.ownership("it").unwrap().view_hash);
    // Asked 30 s after the freeze: the nodes answer for the live cluster
    // and the display holds the frozen digest, so asking again changes
    // nothing.
    let app = frozen_moved(&model, Some(Duration::from_secs(30)));
    let rows = shown(&app, &model, 116, 40);
    let summary = find(&rows, "Esc, p unfreezes");
    assert!(
        summary.contains(&format!(
            "the nodes hold 00c1a2f4, the frozen display holds {lens}: Esc, p unfreezes"
        )),
        "{summary}"
    );
    assert!(summary.ends_with("asked after the freeze"), "{summary}");
    assert!(!has(&rows, "Enter asks again"), "{rows:?}");
    assert!(!has(&rows, "0.0 s ago"), "{rows:?}");
    assert!(!has(&rows, "view moved"), "{rows:?}");
    // Answers from before the freeze still move with a fresh ask.
    let app = frozen_moved(&model, None);
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(
            &rows,
            &format!("view moved 00c1a2f4 -> {lens}: Enter asks again · asked 3.0 s ago")
        ),
        "{rows:?}"
    );
    assert!(!has(&rows, "unfreezes"), "{rows:?}");
    // A display that is not frozen keeps the plain remedy and the clamped
    // age, whenever the answers were asked.
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    let wall = model.wall().unwrap();
    answered(&mut app, &model, &request, |answers| {
        answers.asked = wall + Duration::from_secs(30);
        for answer in &mut answers.nodes {
            if let Outcome::Read(reading) = &mut answer.outcome {
                reading.distributed.as_mut().unwrap().view = "00c1a2f4e8b3195d".to_owned();
            }
        }
    });
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(
            &rows,
            &format!("view moved 00c1a2f4 -> {lens}: Enter asks again · asked 0.0 s ago")
        ),
        "{rows:?}"
    );
}
