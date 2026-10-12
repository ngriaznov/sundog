//! Tests of key handling, the explain overlay's keys, director commands and motion.

use crossterm::event::KeyEventState;

use super::*;
use crate::model::testkit;

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn ch(c: char) -> KeyEvent {
    let modifiers = if c.is_ascii_uppercase() {
        KeyModifiers::SHIFT
    } else {
        KeyModifiers::NONE
    };
    KeyEvent::new(KeyCode::Char(c), modifiers)
}

fn model() -> Model {
    testkit::fixture_model(Instant::now())
}

fn app() -> App {
    App::new(AppConfig::default())
}

fn demo_app() -> App {
    App::new(AppConfig {
        demo: true,
        ..AppConfig::default()
    })
}

fn selected_label(app: &App, model: &Model) -> String {
    let wall = model.wall().unwrap();
    let addr = app.selected_addr(model, wall).unwrap();
    data::row_at(model, addr).unwrap().label().to_owned()
}

#[test]
fn digits_tab_and_backtab_switch_views() {
    let model = model();
    let mut app = app();
    let table = [
        (key(KeyCode::Char('2')), View::Caches),
        (key(KeyCode::Char('3')), View::Node),
        (key(KeyCode::Char('4')), View::Timeline),
        (key(KeyCode::Char('1')), View::Overview),
        (key(KeyCode::Tab), View::Caches),
        (key(KeyCode::Tab), View::Node),
        (key(KeyCode::BackTab), View::Caches),
        (key(KeyCode::BackTab), View::Overview),
        (key(KeyCode::BackTab), View::Timeline),
        (key(KeyCode::Tab), View::Overview),
    ];
    for (event, view) in table {
        assert_eq!(app.handle_key(event, &model), Action::Redraw);
        assert_eq!(app.view, view, "{event:?}");
    }
}

#[test]
fn q_and_ctrl_c_quit_from_any_state() {
    let model = model();
    for event in [
        ch('q'),
        KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
    ] {
        let mut plain = app();
        assert_eq!(plain.handle_key(event, &model), Action::Quit);
        let mut helped = app();
        helped.help = true;
        assert_eq!(helped.handle_key(event, &model), Action::Quit);
    }
    // A bare `c` is the cache key, not a quit.
    let mut app = app();
    assert_eq!(app.handle_key(ch('c'), &model), Action::Redraw);
}

#[test]
fn released_keys_do_nothing() {
    let model = model();
    let mut app = app();
    let release = KeyEvent {
        kind: KeyEventKind::Release,
        state: KeyEventState::NONE,
        ..key(KeyCode::Char('2'))
    };
    assert_eq!(app.handle_key(release, &model), Action::None);
    assert_eq!(app.view, View::Overview);
    let quit = KeyEvent {
        kind: KeyEventKind::Release,
        ..ch('q')
    };
    assert_eq!(app.handle_key(quit, &model), Action::None);
}

#[test]
fn arrows_j_k_g_shift_g_select_members_and_clamp() {
    let model = model();
    let mut app = app();
    assert_eq!(selected_label(&app, &model), "n1");
    app.handle_key(key(KeyCode::Up), &model);
    assert_eq!(selected_label(&app, &model), "n1", "up clamps at the first");
    app.handle_key(key(KeyCode::Down), &model);
    app.handle_key(ch('j'), &model);
    assert_eq!(selected_label(&app, &model), "n3");
    app.handle_key(ch('k'), &model);
    assert_eq!(selected_label(&app, &model), "n2");
    app.handle_key(ch('G'), &model);
    assert_eq!(selected_label(&app, &model), "n8");
    app.handle_key(key(KeyCode::Down), &model);
    assert_eq!(
        selected_label(&app, &model),
        "n8",
        "down clamps at the last"
    );
    app.handle_key(ch('g'), &model);
    assert_eq!(selected_label(&app, &model), "n1");
}

#[test]
fn the_node_view_selects_members_too() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('3'), &model);
    app.handle_key(ch('j'), &model);
    assert_eq!(selected_label(&app, &model), "n2");
}

#[test]
fn selection_falls_back_to_the_first_row_when_the_chosen_node_is_hidden() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('G'), &model);
    assert_eq!(selected_label(&app, &model), "n8");
    // 200 s later the left node n8 has aged out of the rows.
    let later = model.wall().unwrap() + Duration::from_secs(200);
    let addr = app.selected_addr(&model, later).unwrap();
    assert_eq!(data::row_at(&model, addr).unwrap().label(), "n1");
    assert_eq!(app.selected_addr(&Model::new(), later), None);
}

#[test]
fn enter_opens_the_node_view_and_escape_goes_back() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('j'), &model);
    app.handle_key(ch('j'), &model);
    assert_eq!(app.handle_key(key(KeyCode::Enter), &model), Action::Redraw);
    assert_eq!(app.view, View::Node);
    assert_eq!(selected_label(&app, &model), "n3");
    assert_eq!(app.handle_key(key(KeyCode::Enter), &model), Action::None);
    assert_eq!(app.handle_key(key(KeyCode::Esc), &model), Action::Redraw);
    assert_eq!(app.view, View::Overview);
    assert_eq!(app.handle_key(key(KeyCode::Esc), &model), Action::None);
    assert_eq!(app.view, View::Overview, "escape never quits");
}

#[test]
fn enter_without_members_does_nothing() {
    let mut app = app();
    assert_eq!(
        app.handle_key(key(KeyCode::Enter), &Model::new()),
        Action::None
    );
    assert_eq!(app.view, View::Overview);
}

#[test]
fn escape_closes_help_first_then_raw_then_goes_back() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('3'), &model);
    app.handle_key(ch('r'), &model);
    assert!(app.raw);
    app.handle_key(ch('?'), &model);
    assert!(app.help);
    app.handle_key(key(KeyCode::Esc), &model);
    assert!(!app.help && app.raw && app.view == View::Node);
    app.handle_key(key(KeyCode::Esc), &model);
    assert!(!app.raw && app.view == View::Node);
    app.handle_key(key(KeyCode::Esc), &model);
    assert_eq!(app.view, View::Overview);
}

fn control() -> UrlTemplate {
    UrlTemplate::parse("{ip}:{gossip_port+134}").expect("the template parses")
}

fn explaining_app() -> App {
    App::new(AppConfig {
        control: Some(control()),
        ..AppConfig::default()
    })
}

fn control_key(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
}

/// Types `text` into the open overlay.
fn type_into(app: &mut App, model: &Model, text: &str) {
    for c in text.chars() {
        assert_eq!(app.handle_key(ch(c), model), Action::Redraw, "{c:?}");
    }
}

fn typed(app: &App) -> &str {
    &app.explain.as_ref().expect("the overlay is open").text
}

fn request_of(action: Action) -> ExplainRequest {
    match action {
        Action::Fleet(FleetCmd::Explain(request)) => request,
        other => panic!("expected an explain request, got {other:?}"),
    }
}

/// An answer to request `id` about `key` that holds no node's reading.
fn answers(id: u64, key: &str) -> Explained {
    Explained {
        id,
        key: key.to_owned(),
        asked: SystemTime::UNIX_EPOCH,
        nodes: Vec::new(),
    }
}

#[test]
fn e_opens_the_overlay_in_every_mode_seeded_with_k1_only_in_demo() {
    let model = model();
    for view in View::ALL {
        for (demo, seed) in [(false, ""), (true, SEED_KEY)] {
            let mut app = App::new(AppConfig {
                demo,
                ..AppConfig::default()
            });
            app.go(view);
            assert_eq!(app.handle_key(ch('e'), &model), Action::Redraw);
            let state = app.explain.as_ref().expect("e opens the overlay");
            assert_eq!(state.text, seed, "{view:?} demo={demo}");
            assert_eq!(app.view, view, "e leaves the view alone");
            assert_eq!(
                (state.pending, &state.result, &state.note),
                (None, &None, &None)
            );
        }
    }
    assert_eq!(SEED_KEY, "k1");
    // It opens over an empty model too.
    let mut empty = app();
    assert_eq!(empty.handle_key(ch('e'), &Model::new()), Action::Redraw);
    assert!(empty.explain.is_some());
}

#[test]
fn e_is_swallowed_while_help_is_open() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('?'), &model);
    assert_eq!(app.handle_key(ch('e'), &model), Action::None);
    assert!(app.explain.is_none() && app.help);
    app.handle_key(ch('?'), &model);
    assert_eq!(app.handle_key(ch('e'), &model), Action::Redraw);
    assert!(app.explain.is_some());
}

#[test]
fn q_and_every_hotkey_type_inside_the_overlay_but_ctrl_c_quits() {
    let model = model();
    let mut app = demo_app();
    app.handle_key(ch('e'), &model);
    app.handle_key(control_key('u'), &model);
    let before = (app.view, app.filter, app.anim, app.show_gone, app.raw);
    let hotkeys = "qcCftpar?1234jkgGSKLRe";
    type_into(&mut app, &model, hotkeys);
    assert_eq!(typed(&app), hotkeys);
    assert_eq!(
        (app.view, app.filter, app.anim, app.show_gone, app.raw),
        before,
        "no hotkey acted"
    );
    assert!(!app.help && !app.is_frozen());
    assert_eq!(app.handle_key(key(KeyCode::Tab), &model), Action::None);
    assert_eq!(app.handle_key(key(KeyCode::Left), &model), Action::None);
    // Ctrl-C quits from inside the overlay, and Ctrl-U is no letter.
    assert_eq!(app.handle_key(control_key('c'), &model), Action::Quit);
    assert_eq!(app.handle_key(control_key('x'), &model), Action::None);
    let alt = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::ALT);
    assert_eq!(app.handle_key(alt, &model), Action::None);
    assert_eq!(typed(&app), hotkeys);
    // Outside the overlay the same keys act again.
    app.handle_key(key(KeyCode::Esc), &model);
    assert_eq!(app.handle_key(ch('q'), &model), Action::Quit);
}

#[test]
fn esc_closes_help_drawn_over_the_overlay_then_the_overlay_then_raw_then_back() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('3'), &model);
    app.handle_key(ch('r'), &model);
    app.handle_key(ch('?'), &model);
    // The director can open help over the overlay; help is drawn on top,
    // so the first Esc closes it and leaves the overlay.
    app.handle_key(ch('?'), &model);
    app.handle_key(ch('e'), &model);
    app.apply_director(UiCommand::Help(true), &model);
    assert!(app.explain.is_some() && app.help && app.raw && app.view == View::Node);
    assert_eq!(app.handle_key(key(KeyCode::Esc), &model), Action::Redraw);
    assert!(app.explain.is_some() && !app.help && app.raw);
    app.handle_key(key(KeyCode::Esc), &model);
    assert!(app.explain.is_none() && !app.help && app.raw && app.view == View::Node);
    app.handle_key(key(KeyCode::Esc), &model);
    assert!(!app.raw && app.view == View::Node);
    app.handle_key(key(KeyCode::Esc), &model);
    assert_eq!(app.view, View::Overview);
}

#[test]
fn help_drawn_over_the_overlay_takes_the_keys_its_card_lists() {
    let model = model();
    let mut app = explaining_app();
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "k1");
    app.apply_director(UiCommand::Help(true), &model);
    let pending = app.explain.as_ref().unwrap().pending;
    // Every key the card does not list is ignored, and none types.
    for event in [
        ch('x'),
        ch('1'),
        ch('e'),
        key(KeyCode::Enter),
        key(KeyCode::Up),
        key(KeyCode::Down),
        key(KeyCode::Backspace),
        control_key('u'),
    ] {
        assert_eq!(app.handle_key(event, &model), Action::None, "{event:?}");
        assert_eq!(typed(&app), "k1", "{event:?}");
        assert_eq!(app.explain.as_ref().unwrap().pending, pending);
        assert!(app.help && app.selected.is_none(), "{event:?}");
    }
    // `?` closes help and leaves the overlay and its text as they were.
    assert_eq!(app.handle_key(ch('?'), &model), Action::Redraw);
    assert!(!app.help && app.explain.is_some());
    assert_eq!(typed(&app), "k1");
    // The overlay types again once help is closed.
    type_into(&mut app, &model, "7");
    assert_eq!(typed(&app), "k17");
    // `q` quits from under help, as the card says.
    app.apply_director(UiCommand::Help(true), &model);
    assert_eq!(app.handle_key(ch('q'), &model), Action::Quit);
    assert_eq!(app.handle_key(control_key('c'), &model), Action::Quit);
    assert_eq!(typed(&app), "k17");
}

#[test]
fn a_frame_too_small_to_draw_the_overlay_leaves_it_no_keys() {
    let model = model();
    let mut app = explaining_app();
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "k1");
    app.note_too_small(true);
    // The overlay is not drawn: `q` quits as it does without one.
    assert_eq!(app.handle_key(ch('q'), &model), Action::Quit);
    assert_eq!(typed(&app), "k1");
    // Esc still closes it, and then `e` opens nothing while the notice
    // is all the screen shows.
    assert_eq!(app.handle_key(key(KeyCode::Esc), &model), Action::Redraw);
    assert!(app.explain.is_none());
    assert_eq!(app.handle_key(ch('e'), &model), Action::None);
    assert!(app.explain.is_none());
    // A frame big enough again gives the overlay its keys back.
    app.note_too_small(false);
    assert_eq!(app.handle_key(ch('e'), &model), Action::Redraw);
    assert!(app.explain.is_some());
    assert_eq!(app.handle_key(ch('q'), &model), Action::Redraw);
    assert_eq!(typed(&app), "k1q");
    app.note_too_small(true);
    assert_eq!(app.handle_key(control_key('c'), &model), Action::Quit);
}

#[test]
fn backspace_and_ctrl_u_edit_the_key_and_the_text_is_capped() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "k17");
    assert_eq!(
        app.handle_key(key(KeyCode::Backspace), &model),
        Action::Redraw
    );
    assert_eq!(typed(&app), "k1");
    assert_eq!(app.handle_key(control_key('u'), &model), Action::Redraw);
    assert_eq!(typed(&app), "");
    // Backspace on nothing is a redraw, not a panic.
    assert_eq!(
        app.handle_key(key(KeyCode::Backspace), &model),
        Action::Redraw
    );
    assert_eq!(typed(&app), "");
    // Any character types, a multibyte one included; the cap counts
    // characters.
    type_into(&mut app, &model, "é k");
    assert_eq!(typed(&app), "é k");
    app.handle_key(control_key('u'), &model);
    type_into(&mut app, &model, &"é".repeat(key::MAX_CHARS + 20));
    assert_eq!(typed(&app).chars().count(), key::MAX_CHARS);
    app.handle_key(key(KeyCode::Backspace), &model);
    assert_eq!(typed(&app).chars().count(), key::MAX_CHARS - 1);
}

#[test]
fn the_last_key_is_kept() {
    let model = model();
    let mut app = demo_app();
    app.handle_key(ch('e'), &model);
    assert_eq!(typed(&app), "k1");
    type_into(&mut app, &model, "2");
    app.handle_key(key(KeyCode::Esc), &model);
    assert!(app.explain.is_none());
    app.handle_key(ch('e'), &model);
    assert_eq!(typed(&app), "k12", "the text is kept, not the seed again");
    // An emptied key stays empty.
    app.handle_key(control_key('u'), &model);
    app.handle_key(key(KeyCode::Esc), &model);
    app.handle_key(ch('e'), &model);
    assert_eq!(typed(&app), "");
}

#[test]
fn enter_without_control_ports_asks_nobody_and_says_so() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "k1");
    assert_eq!(app.handle_key(key(KeyCode::Enter), &model), Action::Redraw);
    let state = app.explain.as_ref().unwrap();
    assert_eq!(state.pending, None);
    let note = state.note.as_deref().expect("the note says why");
    assert!(
        note.starts_with("nobody is asked:") && note.contains("control ports"),
        "{note}"
    );
    // Editing the key retires the note.
    type_into(&mut app, &model, "7");
    assert_eq!(app.explain.as_ref().unwrap().note, None);
}

#[test]
fn enter_sends_an_explain_request_with_the_live_targets_and_records_the_pending_id() {
    let model = model();
    let mut app = explaining_app();
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "k1");
    let first = request_of(app.handle_key(key(KeyCode::Enter), &model));
    assert_eq!(first.id, 1);
    assert_ne!(first.id, crate::ask::SCRIPTED_ID, "ids count from 1");
    assert_eq!(first.key, "k1");
    // The five live nodes, in slot order, at their control ports: the
    // departing, down and left members are not asked.
    let asked: Vec<(String, String)> = first
        .targets
        .iter()
        .map(|target| (target.label.to_string(), target.addr.to_string()))
        .collect();
    let expected: Vec<(String, String)> = (1..=5)
        .map(|n| (format!("n{n}"), format!("127.0.0.{}:8080", 10 + n)))
        .collect();
    assert_eq!(asked, expected);
    let state = app.explain.as_ref().unwrap();
    assert_eq!(state.pending, Some(1));
    assert_eq!(state.note.as_deref(), Some("asking n1 n2 n3 n4 n5"));
    assert_eq!(state.result, None);
    // A second Enter replaces the pending request.
    let second = request_of(app.handle_key(key(KeyCode::Enter), &model));
    assert_eq!(second.id, 2);
    assert_eq!(app.explain.as_ref().unwrap().pending, Some(2));
    // `str:` sends the text after the prefix.
    app.handle_key(control_key('u'), &model);
    type_into(&mut app, &model, "str:uint:1");
    let escaped = request_of(app.handle_key(key(KeyCode::Enter), &model));
    assert_eq!((escaped.id, escaped.key.as_str()), (3, "uint:1"));
}

#[test]
fn a_key_of_another_kind_is_computed_but_never_asked() {
    let model = model();
    for text in ["uint:5", "int:-5", "hex:6b", "a b", ""] {
        let mut app = explaining_app();
        app.handle_key(ch('e'), &model);
        type_into(&mut app, &model, text);
        assert_eq!(
            app.handle_key(key(KeyCode::Enter), &model),
            Action::Redraw,
            "{text:?}"
        );
        let state = app.explain.as_ref().unwrap();
        assert_eq!(state.pending, None, "{text:?}");
        let note = state.note.as_deref().expect("a note says why");
        assert!(note.starts_with("nobody is asked: "), "{note}");
        let why = key::not_askable(&KeySpec::parse(text).unwrap()).unwrap();
        assert!(note.ends_with(why), "{note}");
    }
}

#[test]
fn enter_with_no_live_node_keeps_the_overlay_and_says_so() {
    let mut app = explaining_app();
    app.handle_key(ch('e'), &Model::new());
    type_into(&mut app, &Model::new(), "k1");
    assert_eq!(
        app.handle_key(key(KeyCode::Enter), &Model::new()),
        Action::Redraw
    );
    let state = app.explain.as_ref().expect("the overlay stays open");
    assert_eq!(state.pending, None);
    assert_eq!(
        state.note.as_deref(),
        Some("nobody is asked: no live node has a control port to ask")
    );
    // A template that gives no node an address is the same.
    let model = model();
    let mut app = App::new(AppConfig {
        control: Some(UrlTemplate::parse("{ip}:{gossip_port+65500}").unwrap()),
        ..AppConfig::default()
    });
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "k1");
    app.handle_key(key(KeyCode::Enter), &model);
    assert!(
        app.explain
            .as_ref()
            .unwrap()
            .note
            .as_deref()
            .unwrap()
            .contains("no live node")
    );
}

#[test]
fn the_note_names_the_nodes_asked_and_the_nodes_left_out() {
    let target = |label: &str| Target {
        label: label.into(),
        addr: "127.0.0.1:8080".parse().unwrap(),
    };
    let mut targets = data::ControlTargets {
        asked: vec![target("n1"), target("n2")],
        skipped: Vec::new(),
    };
    assert_eq!(asking_note(&targets), "asking n1 n2");
    targets.skipped = vec![("n3".into(), "why".into()), ("n4".into(), "why".into())];
    assert_eq!(asking_note(&targets), "asking n1 n2 · not asked: n3 n4");
}

#[test]
fn a_reply_for_the_pending_id_lands() {
    let model = model();
    let mut app = explaining_app();
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "k1");
    let request = request_of(app.handle_key(key(KeyCode::Enter), &model));
    app.apply_director(
        UiCommand::Explained(Box::new(answers(request.id, "k1"))),
        &model,
    );
    let state = app.explain.as_ref().unwrap();
    assert_eq!(state.pending, None);
    assert_eq!(state.note, None);
    assert_eq!(state.result.as_deref(), Some(&answers(request.id, "k1")));
    // The same reply twice lands once: nothing is pending any more.
    app.apply_director(
        UiCommand::Explained(Box::new(answers(request.id, "other"))),
        &model,
    );
    assert_eq!(
        app.explain.as_ref().unwrap().result.as_deref(),
        Some(&answers(request.id, "k1"))
    );
}

#[test]
fn a_late_reply_after_esc_is_dropped() {
    let model = model();
    let mut app = explaining_app();
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "k1");
    let request = request_of(app.handle_key(key(KeyCode::Enter), &model));
    app.handle_key(key(KeyCode::Esc), &model);
    app.apply_director(
        UiCommand::Explained(Box::new(answers(request.id, "k1"))),
        &model,
    );
    assert!(app.explain.is_none());
    // Reopened, the overlay waits for nothing: the old reply is not
    // adopted, and a reply to a request replaced by a newer one is
    // dropped too.
    app.handle_key(ch('e'), &model);
    app.apply_director(
        UiCommand::Explained(Box::new(answers(request.id, "k1"))),
        &model,
    );
    assert_eq!(app.explain.as_ref().unwrap().result, None);
    let old = request_of(app.handle_key(key(KeyCode::Enter), &model));
    let new = request_of(app.handle_key(key(KeyCode::Enter), &model));
    assert_ne!(old.id, new.id);
    app.apply_director(
        UiCommand::Explained(Box::new(answers(old.id, "k1"))),
        &model,
    );
    assert_eq!(app.explain.as_ref().unwrap().result, None);
    assert_eq!(app.explain.as_ref().unwrap().pending, Some(new.id));
}

#[test]
fn accepts_decides_by_id() {
    let state = |pending| ExplainState {
        text: "k1".to_owned(),
        note: None,
        pending,
        result: None,
    };
    assert!(accepts(&Some(state(Some(4))), &answers(4, "k1")));
    assert!(!accepts(&Some(state(Some(4))), &answers(5, "k1")));
    assert!(!accepts(&Some(state(None)), &answers(4, "k1")));
    assert!(!accepts(&None, &answers(4, "k1")));
}

#[test]
fn a_result_shows_only_for_the_key_it_answered() {
    let state = |text: &str, key: &str| ExplainState {
        text: text.to_owned(),
        note: None,
        pending: None,
        result: Some(Box::new(answers(1, key))),
    };
    assert!(result_for(&state("k1", "k1")).is_some());
    assert!(result_for(&state("k12", "k1")).is_none());
    assert!(result_for(&state("", "k1")).is_none());
    // The text after `str:` is the key that was sent.
    assert!(result_for(&state("str:uint:1", "uint:1")).is_some());
    assert!(result_for(&state("uint:1", "uint:1")).is_none());
    // A text that is no key shows nothing.
    assert!(result_for(&state("hex:zz", "hex:zz")).is_none());
    assert!(
        result_for(&ExplainState {
            result: None,
            ..state("k1", "k1")
        })
        .is_none()
    );
}

#[test]
fn asked_key_is_the_text_a_test_node_is_sent() {
    assert_eq!(asked_key("k1").as_deref(), Some("k1"));
    assert_eq!(asked_key("str:uint:1").as_deref(), Some("uint:1"));
    for text in ["", "a b", "uint:5", "hex:6b", "hex:zz", "é"] {
        assert_eq!(asked_key(text), None, "{text:?}");
    }
}

#[test]
fn arrows_move_the_selected_node_in_every_view() {
    let model = model();
    for view in View::ALL {
        let mut app = app();
        app.go(view);
        app.handle_key(ch('e'), &model);
        let before = app.view;
        assert_eq!(selected_label(&app, &model), "n1");
        assert_eq!(app.handle_key(key(KeyCode::Down), &model), Action::Redraw);
        assert_eq!(app.handle_key(key(KeyCode::Down), &model), Action::Redraw);
        assert_eq!(selected_label(&app, &model), "n3", "{view:?}");
        assert_eq!(app.handle_key(key(KeyCode::Up), &model), Action::Redraw);
        assert_eq!(selected_label(&app, &model), "n2", "{view:?}");
        assert_eq!(app.view, before, "the arrows do not leave the view");
        assert_eq!(app.selected_cache(&model), model_first_cache(&model));
        assert_eq!(typed(&app), "", "the arrows do not type");
    }
}

fn model_first_cache(model: &Model) -> Option<SmolStr> {
    data::cache_rows(model).first().map(|row| row.name.clone())
}

#[test]
fn help_swallows_other_keys_and_toggles_with_question_mark() {
    let model = model();
    let mut app = app();
    assert_eq!(app.handle_key(ch('?'), &model), Action::Redraw);
    assert!(app.help);
    assert_eq!(app.handle_key(ch('2'), &model), Action::None);
    assert_eq!(app.view, View::Overview);
    assert_eq!(app.handle_key(ch('?'), &model), Action::Redraw);
    assert!(!app.help);
}

#[test]
fn c_and_shift_c_cycle_distributed_caches() {
    let mut snapshot_members = vec![];
    for index in 1..=3 {
        snapshot_members.push(testkit::member_with(
            index,
            0,
            1,
            sundog::observe::MemberStatus::Live,
            &[
                ("a", testkit::distributed(2)),
                ("b", testkit::distributed(3)),
                ("r", sundog::store::Mode::Replicated),
            ],
        ));
    }
    let snapshot = sundog::observe::ClusterSnapshot::new("c", snapshot_members, 0);
    let mut model = Model::new();
    let now = Instant::now();
    model.apply(
        crate::source::Update::Snapshot(std::sync::Arc::new(snapshot), now),
        now,
        SystemTime::UNIX_EPOCH,
    );
    let mut app = app();
    assert_eq!(app.ownership_cache(&model).unwrap(), "a");
    app.handle_key(ch('c'), &model);
    assert_eq!(app.ownership_cache(&model).unwrap(), "b");
    app.handle_key(ch('c'), &model);
    assert_eq!(app.ownership_cache(&model).unwrap(), "a");
    app.handle_key(ch('C'), &model);
    assert_eq!(app.ownership_cache(&model).unwrap(), "b");
    // The replicated cache is never the ownership cache.
    app.apply_director(UiCommand::Cache("r".into()), &model);
    assert_eq!(app.ownership_cache(&model).unwrap(), "a");
    // No distributed cache, no cycling.
    let mut empty = App::new(AppConfig::default());
    assert_eq!(empty.handle_key(ch('c'), &Model::new()), Action::Redraw);
    assert_eq!(empty.ownership_cache(&Model::new()), None);
}

#[test]
fn the_caches_view_selects_among_every_cache() {
    let model = model();
    let mut app = app();
    app.handle_key(ch('2'), &model);
    assert_eq!(app.selected_cache(&model).unwrap(), "it");
    app.handle_key(ch('j'), &model);
    assert_eq!(app.selected_cache(&model).unwrap(), "churn");
    app.handle_key(ch('G'), &model);
    assert_eq!(app.selected_cache(&model).unwrap(), "pn");
    app.handle_key(ch('j'), &model);
    assert_eq!(app.selected_cache(&model).unwrap(), "pn");
    app.handle_key(ch('g'), &model);
    assert_eq!(app.selected_cache(&model).unwrap(), "it");
    assert_eq!(app.handle_key(key(KeyCode::Enter), &model), Action::None);
}

#[test]
fn f_cycles_the_filter_t_the_gone_rows_a_the_animation() {
    let model = model();
    let mut app = app();
    assert_eq!(app.filter, Filter::All);
    app.handle_key(ch('f'), &model);
    assert_eq!(app.filter, Filter::Membership);
    for _ in 0..4 {
        app.handle_key(ch('f'), &model);
    }
    assert_eq!(app.filter, Filter::All);
    assert!(!app.show_gone);
    app.handle_key(ch('t'), &model);
    assert!(app.show_gone);
    assert!(app.anim);
    app.handle_key(ch('a'), &model);
    assert!(!app.anim);
    app.handle_key(ch('a'), &model);
    assert!(app.anim);
}

#[test]
fn r_toggles_raw_samples_only_in_the_node_view() {
    let model = model();
    let mut app = app();
    assert_eq!(app.handle_key(ch('r'), &model), Action::None);
    assert!(!app.raw);
    app.handle_key(ch('3'), &model);
    assert_eq!(app.handle_key(ch('r'), &model), Action::Redraw);
    assert!(app.raw);
    app.handle_key(ch('r'), &model);
    assert!(!app.raw);
}

#[test]
fn p_freezes_a_copy_of_the_model_and_unfreezes() {
    let model = model();
    let mut app = app();
    assert!(!app.is_frozen());
    app.handle_key(ch('p'), &model);
    assert!(app.is_frozen());
    let live = Model::new();
    assert_eq!(
        app.shown(&live).snapshot().unwrap().members.len(),
        8,
        "the frozen copy is shown while the live model is empty"
    );
    app.handle_key(ch('p'), &model);
    assert!(!app.is_frozen());
    assert!(app.shown(&live).snapshot().is_none());
}

#[test]
fn the_timeline_scrolls_and_pins_the_log_until_shift_g() {
    let model = model();
    let len = model.events().len();
    assert!(len > 3);
    let mut app = app();
    app.handle_key(ch('4'), &model);
    assert!(!app.is_pinned());
    assert_eq!(app.log_offset(len), 0);
    app.handle_key(key(KeyCode::Up), &model);
    assert!(app.is_pinned());
    assert_eq!(app.log_offset(len), 1);
    app.handle_key(ch('k'), &model);
    assert_eq!(app.log_offset(len), 2);
    app.handle_key(key(KeyCode::Down), &model);
    assert_eq!(app.log_offset(len), 1);
    // New events do not move a pinned log.
    assert_eq!(app.log_offset(len + 3), 4);
    app.handle_key(ch('g'), &model);
    assert_eq!(app.log_offset(len), len - 1);
    app.handle_key(ch('G'), &model);
    assert_eq!(app.log_offset(len), 0);
    assert!(!app.is_pinned(), "G returns to the live end");
    app.handle_key(key(KeyCode::Up), &model);
    app.handle_key(key(KeyCode::Down), &model);
    assert!(!app.is_pinned(), "scrolling down to the newest unpins");
}

#[test]
fn scrolling_counts_the_events_the_filter_shows() {
    let model = model();
    let total = model.events().len();
    let filtered = model.events().newest_first(Filter::Ownership).count();
    assert!(filtered >= 2 && filtered < total, "{filtered} of {total}");
    let mut app = app();
    app.handle_key(ch('4'), &model);
    while app.filter != Filter::Ownership {
        app.handle_key(ch('f'), &model);
    }
    app.handle_key(ch('g'), &model);
    assert_eq!(app.log_offset(filtered), filtered - 1);
    app.handle_key(ch('j'), &model);
    assert_eq!(
        app.log_offset(filtered),
        filtered - 2,
        "one press moves the view one row"
    );
    // A matching event arriving while pinned holds the view in place.
    let before = app.log_offset(filtered);
    assert_eq!(app.log_offset(filtered + 1), before + 1);
    app.handle_key(ch('f'), &model);
    assert!(!app.is_pinned(), "a new filter unpins the log");
    assert_eq!(app.log_offset(filtered), 0);
}

#[test]
fn demo_keys_act_only_in_demo_mode() {
    let model = model();
    for letter in ['S', 'K', 'L', 'R'] {
        let mut watch = app();
        assert_eq!(
            watch.handle_key(ch(letter), &model),
            Action::None,
            "{letter}"
        );
    }
    let mut demo = demo_app();
    assert_eq!(
        demo.handle_key(ch('S'), &model),
        Action::Fleet(FleetCmd::Spawn)
    );
    demo.handle_key(ch('j'), &model);
    assert_eq!(
        demo.handle_key(ch('K'), &model),
        Action::Fleet(FleetCmd::Kill("n2".into()))
    );
    assert_eq!(
        demo.handle_key(ch('L'), &model),
        Action::Fleet(FleetCmd::Leave("n2".into()))
    );
    assert_eq!(
        demo.handle_key(ch('R'), &model),
        Action::Fleet(FleetCmd::Restart("n2".into()))
    );
    assert_eq!(demo.handle_key(ch('K'), &Model::new()), Action::None);
}

#[test]
fn unbound_keys_do_nothing() {
    let model = model();
    let mut app = app();
    for code in [
        KeyCode::Char('z'),
        KeyCode::F(5),
        KeyCode::Left,
        KeyCode::Char('!'),
    ] {
        assert_eq!(app.handle_key(key(code), &model), Action::None, "{code:?}");
    }
}

#[test]
fn the_director_drives_views_selection_help_and_captions() {
    let model = model();
    let mut app = app();
    app.apply_director(UiCommand::Tab(View::Caches), &model);
    assert_eq!(app.view, View::Caches);
    app.apply_director(UiCommand::Select("n4".into()), &model);
    assert_eq!(selected_label(&app, &model), "n4");
    app.apply_director(UiCommand::Select("zz".into()), &model);
    assert_eq!(
        selected_label(&app, &model),
        "n4",
        "an unknown label keeps the selection"
    );
    app.apply_director(UiCommand::Cache("pn".into()), &model);
    assert_eq!(app.selected_cache(&model).unwrap(), "pn");
    app.apply_director(UiCommand::Help(true), &model);
    assert!(app.help);
    app.apply_director(UiCommand::Help(false), &model);
    assert!(!app.help);
    app.apply_director(UiCommand::Caption(Some("hello".into())), &model);
    assert_eq!(app.caption.as_deref(), Some("hello"));
    app.apply_director(UiCommand::Caption(None), &model);
    assert_eq!(app.caption, None);
}

#[test]
fn a_scripted_explain_opens_the_overlay_on_its_key_with_the_answers_in() {
    let model = model();
    let mut app = explaining_app();
    app.apply_director(UiCommand::Tab(View::Node), &model);
    app.apply_director(UiCommand::Caption(Some("kept".into())), &model);
    let scripted = || answers(crate::ask::SCRIPTED_ID, "k1");
    app.apply_director(
        UiCommand::Explain {
            key: "k1".to_owned(),
            answers: Box::new(scripted()),
        },
        &model,
    );
    let state = app.explain.as_ref().expect("the command opens the overlay");
    assert_eq!(state.text, "k1");
    assert_eq!(state.result.as_deref(), Some(&scripted()));
    assert_eq!((state.pending, &state.note), (None, &None));
    assert_eq!(result_for(state), Some(&scripted()), "the answers show");
    // Nothing but the overlay changed.
    assert_eq!(app.view, View::Node);
    assert_eq!(app.caption.as_deref(), Some("kept"));
    assert!(!app.help);
    // The overlay takes keys as if the person had opened it: typing edits
    // the key, and the answers stop showing for another key.
    type_into(&mut app, &model, "2");
    assert_eq!(typed(&app), "k12");
    assert_eq!(result_for(app.explain.as_ref().unwrap()), None);
    assert_eq!(
        app.handle_key(key(KeyCode::Esc), &model),
        Action::Redraw,
        "Esc closes it"
    );
    assert!(app.explain.is_none());

    // A key spelled with its prefix shows the answers to the key it names.
    app.apply_director(
        UiCommand::Explain {
            key: "str:close".to_owned(),
            answers: Box::new(answers(crate::ask::SCRIPTED_ID, "close")),
        },
        &model,
    );
    let state = app.explain.as_ref().unwrap();
    assert_eq!(state.text, "str:close");
    assert!(result_for(state).is_some_and(|result| result.key == "close"));
}

#[test]
fn a_scripted_explain_replaces_an_overlay_that_is_open() {
    let model = model();
    let mut app = explaining_app();
    app.handle_key(ch('e'), &model);
    type_into(&mut app, &model, "x");
    let request = request_of(app.handle_key(key(KeyCode::Enter), &model));
    assert_eq!(app.explain.as_ref().unwrap().pending, Some(request.id));
    app.apply_director(
        UiCommand::Explain {
            key: "k1".to_owned(),
            answers: Box::new(answers(crate::ask::SCRIPTED_ID, "k1")),
        },
        &model,
    );
    let state = app.explain.as_ref().unwrap();
    assert_eq!(state.text, "k1", "the typed key is replaced");
    assert_eq!((state.pending, &state.note), (None, &None));
    // The answer to the request the command replaced is dropped.
    app.apply_director(
        UiCommand::Explained(Box::new(answers(request.id, "x"))),
        &model,
    );
    assert_eq!(
        app.explain.as_ref().unwrap().result.as_deref(),
        Some(&answers(crate::ask::SCRIPTED_ID, "k1"))
    );
}

#[test]
fn closing_a_scripted_explain_keeps_its_key_for_the_next_opening() {
    let model = model();
    let mut app = explaining_app();
    // Closing what is not open changes nothing.
    app.apply_director(UiCommand::CloseExplain, &model);
    assert!(app.explain.is_none());
    app.handle_key(ch('?'), &model);
    app.apply_director(UiCommand::CloseExplain, &model);
    assert!(app.help, "the help overlay is not the explain overlay");
    app.handle_key(ch('?'), &model);

    app.apply_director(
        UiCommand::Explain {
            key: "k1".to_owned(),
            answers: Box::new(answers(crate::ask::SCRIPTED_ID, "k1")),
        },
        &model,
    );
    assert!(app.explain.is_some());
    app.apply_director(UiCommand::CloseExplain, &model);
    assert!(app.explain.is_none());
    app.handle_key(ch('e'), &model);
    let state = app.explain.as_ref().unwrap();
    assert_eq!(state.text, "k1", "the text stays between openings");
    assert_eq!((&state.result, state.pending), (&None, None));
}

#[test]
fn an_explained_command_changes_no_view_state() {
    let model = model();
    let mut app = app();
    app.apply_director(UiCommand::Tab(View::Node), &model);
    app.apply_director(UiCommand::Caption(Some("kept".into())), &model);
    let explained = Explained {
        id: 3,
        key: "k1".to_owned(),
        asked: SystemTime::UNIX_EPOCH,
        nodes: Vec::new(),
    };
    app.apply_director(UiCommand::Explained(Box::new(explained)), &model);
    assert_eq!(app.view, View::Node);
    assert_eq!(app.caption.as_deref(), Some("kept"));
    assert!(!app.help);
}

#[test]
fn the_quit_command_is_the_loops_to_handle_and_changes_nothing_here() {
    let model = model();
    let mut app = app();
    app.apply_director(UiCommand::Tab(View::Node), &model);
    app.apply_director(UiCommand::Caption(Some("kept".into())), &model);
    app.apply_director(UiCommand::Quit, &model);
    assert_eq!(app.view, View::Node);
    assert_eq!(app.caption.as_deref(), Some("kept"));
}

#[test]
fn steps_move_and_clamp() {
    assert_eq!(Step::Next.apply(Some(0), 3), Some(1));
    assert_eq!(Step::Next.apply(Some(2), 3), Some(2));
    assert_eq!(Step::Prev.apply(Some(0), 3), Some(0));
    assert_eq!(Step::Prev.apply(Some(2), 3), Some(1));
    assert_eq!(Step::First.apply(Some(2), 3), Some(0));
    assert_eq!(Step::Last.apply(None, 3), Some(2));
    assert_eq!(Step::Next.apply(None, 3), Some(0));
    assert_eq!(Step::Next.apply(Some(0), 0), None);
}

fn digest_of(live: u8) -> OwnershipDigest {
    testkit::ownership_digest(&testkit::snapshot(live), "it").unwrap()
}

#[test]
fn remapping_a_lead_follows_the_node_not_the_index() {
    let three = digest_of(3);
    let four = digest_of(4);
    let remapped = remap_lead(&three, &four);
    for (bucket, &lead) in three.lead.iter().enumerate() {
        let node = three.eligible[usize::from(lead)];
        assert_eq!(
            four.eligible[usize::from(remapped[bucket])],
            node,
            "bucket {bucket}"
        );
    }
    // Going the other way, buckets led by the dropped node have no lead.
    let back = remap_lead(&four, &three);
    let gone = four
        .eligible
        .iter()
        .position(|n| !three.eligible.contains(n))
        .unwrap();
    for (bucket, &lead) in four.lead.iter().enumerate() {
        if usize::from(lead) == gone {
            assert_eq!(back[bucket], NO_LEAD);
        } else {
            assert_ne!(back[bucket], NO_LEAD);
        }
    }
}

fn apply_digest(model: &mut Model, digest: OwnershipDigest, now: Instant) {
    model.apply(
        crate::source::Update::Ownership(digest),
        now,
        SystemTime::UNIX_EPOCH,
    );
}

#[test]
fn a_view_change_flashes_the_moved_buckets_for_1200_ms() {
    let mut app = app();
    let (mut model, start) = testkit::past_discovery(Instant::now());
    apply_digest(&mut model, digest_of(3), start);
    app.observe(&model, start);
    assert!(
        app.flash("it", start).is_none(),
        "the first view has nothing to compare"
    );
    apply_digest(&mut model, digest_of(4), start);
    app.observe(&model, start);
    let (prev, intensity) = app.flash("it", start).expect("a flash starts");
    assert!((intensity - 1.0).abs() < 1e-9);
    let new_lead = &model.ownership("it").unwrap().lead;
    assert!(prev.iter().zip(new_lead.iter()).any(|(a, b)| a != b));
    let mid = app
        .flash("it", start + Duration::from_millis(600))
        .unwrap()
        .1;
    assert!((mid - 0.5).abs() < 1e-6);
    assert!(app.flash("it", start + MOSAIC_FLASH).is_none());
    assert!(app.flash("none", start).is_none());
}

#[test]
fn a_view_found_while_discovering_the_cluster_does_not_flash() {
    let mut model = Model::new();
    let mut app = app();
    let start = Instant::now();
    let snapshot = |count| {
        crate::source::Update::Snapshot(std::sync::Arc::new(testkit::snapshot(count)), start)
    };
    model.apply(
        snapshot(1),
        start,
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    );
    apply_digest(&mut model, digest_of(1), start);
    app.observe(&model, start);
    model.apply(
        snapshot(3),
        start,
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    );
    apply_digest(&mut model, digest_of(3), start);
    assert!(model.discovering());
    app.observe(&model, start);
    assert!(
        app.flash("it", start).is_none(),
        "the observer finding members moves no bucket"
    );
    let later = start + crate::model::DISCOVERY_QUIET;
    model.tick(later);
    apply_digest(&mut model, digest_of(4), later);
    app.observe(&model, later);
    assert!(app.flash("it", later).is_some(), "a real change flashes");
}

#[test]
fn no_animation_means_no_flash_and_snapped_shares() {
    let mut model = Model::new();
    let mut app = App::new(AppConfig {
        anim: false,
        ..AppConfig::default()
    });
    let start = Instant::now();
    apply_digest(&mut model, digest_of(3), start);
    app.observe(&model, start);
    apply_digest(&mut model, digest_of(4), start);
    app.observe(&model, start);
    assert!(app.flash("it", start).is_none());
    let digest = model.ownership("it").unwrap();
    let (node, count) = digest.counts[0];
    let target = crate::model::derive::share_fraction(count);
    assert!((app.share("it", node, 0.0) - target).abs() < 1e-12);
    assert!(!app.animating(&model, start, SystemTime::UNIX_EPOCH));
}

#[test]
fn share_bars_grow_from_zero_toward_the_computed_share() {
    let mut model = Model::new();
    let mut app = app();
    let start = Instant::now();
    apply_digest(&mut model, digest_of(3), start);
    app.observe(&model, start);
    let (node, count) = model.ownership("it").unwrap().counts[0];
    let target = crate::model::derive::share_fraction(count);
    assert!(
        app.share("it", node, target).abs() < 1e-12,
        "starts at zero"
    );
    assert!(app.animating(&model, start, SystemTime::UNIX_EPOCH));
    for _ in 0..40 {
        app.step(Duration::from_millis(50));
    }
    assert!((app.share("it", node, 0.0) - target).abs() < 0.01);
    app.snap();
    assert!((app.share("it", node, 0.0) - target).abs() < 1e-9);
    assert!(!app.animating(
        &model,
        start + Duration::from_secs(60),
        SystemTime::UNIX_EPOCH + Duration::from_secs(60)
    ));
    // An unknown cache falls back to the target given.
    assert!((app.share("other", node, 0.25) - 0.25).abs() < 1e-12);
}

#[test]
fn a_dropped_node_loses_its_tween() {
    let mut model = Model::new();
    let mut app = app();
    let start = Instant::now();
    apply_digest(&mut model, digest_of(4), start);
    app.observe(&model, start);
    let dropped = model.ownership("it").unwrap().eligible[3];
    apply_digest(&mut model, digest_of(3), start);
    app.observe(&model, start);
    let remaining = model.ownership("it").unwrap().eligible.clone();
    if !remaining.contains(&dropped) {
        assert!((app.share("it", dropped, 0.7) - 0.7).abs() < 1e-12);
    }
}

#[test]
fn a_frozen_display_neither_observes_nor_steps() {
    let mut model = Model::new();
    let mut app = app();
    let start = Instant::now();
    apply_digest(&mut model, digest_of(3), start);
    app.observe(&model, start);
    app.handle_key(ch('p'), &model);
    let (node, count) = model.ownership("it").unwrap().counts[0];
    let target = crate::model::derive::share_fraction(count);
    let before = app.share("it", node, target);
    app.step(Duration::from_secs(5));
    assert!((app.share("it", node, target) - before).abs() < 1e-12);
    apply_digest(&mut model, digest_of(4), start);
    app.observe(&model, start);
    assert!(app.flash("it", start).is_none());
    assert!(!app.animating(&model, start, SystemTime::UNIX_EPOCH));
}

#[test]
fn fresh_events_and_departing_rows_keep_the_screen_animating() {
    let model = model();
    let mut app = app();
    app.observe(&model, Instant::now());
    app.snap();
    let wall = model.wall().unwrap();
    // The fixture's departing member blinks forever.
    assert!(app.animating(&model, Instant::now(), wall));
}

#[test]
fn the_config_is_readable_and_the_observer_can_be_set() {
    let mut app = app();
    assert_eq!(app.config().forget_after, Duration::from_secs(90));
    assert!(app.config().observer.is_none());
    let addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();
    app.set_observer(addr);
    assert_eq!(app.config().observer, Some(addr));
    assert_eq!(app.look(), Look::default());
}

#[test]
fn motion_lasts_as_long_as_the_flash_the_pulse_and_the_hold_say() {
    use sundog::observe::{ClusterSnapshot, MemberStatus};
    let wall_at = |secs| SystemTime::UNIX_EPOCH + Duration::from_secs(100) + secs;
    let mut app = app();
    let start = Instant::now();
    let one = |status, since: SystemTime| {
        let mut model = Model::new();
        // A process's incarnation is the wall clock at its start.
        let started = since.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let incarnation = u64::try_from(started.as_millis()).unwrap();
        let member = testkit::member_since(1, 0, incarnation, status, since, &[]);
        let snapshot = ClusterSnapshot::new("c", vec![member], 0);
        model.apply(
            crate::source::Update::Snapshot(std::sync::Arc::new(snapshot), start),
            start,
            wall_at(Duration::ZERO),
        );
        model
    };
    let seen = wall_at(Duration::ZERO);
    // A member that just went live pulses for JOIN_PULSE.
    let live = one(MemberStatus::Live, seen);
    app.observe(&live, start);
    assert!(app.animating(
        &live,
        start,
        wall_at(JOIN_PULSE.saturating_sub(Duration::from_millis(1)))
    ));
    assert!(!app.animating(&live, start, wall_at(JOIN_PULSE)));
    // A down member holds red for DOWN_HOLD.
    let down = one(MemberStatus::Down, seen);
    assert!(app.animating(
        &down,
        start,
        wall_at(DOWN_HOLD.saturating_sub(Duration::from_millis(1)))
    ));
    assert!(!app.animating(&down, start, wall_at(DOWN_HOLD)));
    // A left member does not move, a departing one blinks for good.
    assert!(!app.animating(
        &one(MemberStatus::Left, seen),
        start,
        wall_at(Duration::ZERO)
    ));
    let departing = one(MemberStatus::Departing, seen);
    assert!(app.animating(&departing, start, wall_at(Duration::from_secs(3600))));
    // A new event flashes for EVENT_FLASH, even when no member is moving.
    let old = wall_at(Duration::ZERO) - Duration::from_secs(100);
    let (mut model, found) = testkit::past_discovery(start);
    let mut members = testkit::snapshot(1).members;
    members.push(testkit::member_since(2, 0, 1, MemberStatus::Live, old, &[]));
    let snapshot = ClusterSnapshot::new("c", members, 0);
    model.apply(
        crate::source::Update::Snapshot(std::sync::Arc::new(snapshot), found),
        found,
        wall_at(Duration::ZERO),
    );
    assert_eq!(model.events().len(), 1, "the join");
    let mut calm = App::new(AppConfig::default());
    calm.observe(&model, start);
    assert!(calm.animating(
        &model,
        start,
        wall_at(EVENT_FLASH.saturating_sub(Duration::from_millis(1)))
    ));
    assert!(!calm.animating(&model, start, wall_at(EVENT_FLASH)));
}
