//! Tests of the explain overlay.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use sundog::store::{Mode, PartId};

use super::*;
use crate::app::{Action, App, AppConfig, ExplainRequest, FleetCmd, UiCommand};
use crate::explained::fixture;
use crate::model::Model;
use crate::model::testkit;
use crate::source::Update;
use crate::source::targets::UrlTemplate;
use crate::ui::look::Look;
use crate::ui::panel::row_text;
use crate::ui::theme::{self, ColorMode};
use crate::ui::{Ctx, LayoutKind};

mod rows_and_detail;

mod verdict_line;

fn model() -> Model {
    testkit::fixture_model(Instant::now())
}

fn control() -> UrlTemplate {
    UrlTemplate::parse("{ip}:{gossip_port+134}").expect("the template parses")
}

fn app_in(look: Look, control: Option<UrlTemplate>) -> App {
    App::new(AppConfig {
        look,
        control,
        ..AppConfig::default()
    })
}

fn press(app: &mut App, model: &Model, code: KeyCode) -> Action {
    app.handle_key(KeyEvent::new(code, KeyModifiers::NONE), model)
}

/// An app with the overlay open on `text`, whose control ports are known.
fn opened(model: &Model, text: &str) -> App {
    let mut app = app_in(Look::default(), Some(control()));
    typed(&mut app, model, text);
    app
}

/// Opens the overlay, if it is closed, and types `text` over its key.
fn typed(app: &mut App, model: &Model, text: &str) {
    if app.explain.is_none() {
        press(app, model, KeyCode::Char('e'));
    }
    let ctrl_u = KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL);
    app.handle_key(ctrl_u, model);
    for c in text.chars() {
        press(app, model, KeyCode::Char(c));
    }
}

fn ask(app: &mut App, model: &Model) -> ExplainRequest {
    match press(app, model, KeyCode::Enter) {
        Action::Fleet(FleetCmd::Explain(request)) => request,
        other => panic!("Enter asks the nodes, not {other:?}"),
    }
}

/// The nodes' answers to `request` as a converged cluster gives them,
/// changed by `edit`, delivered to the overlay.
fn answered(
    app: &mut App,
    model: &Model,
    request: &ExplainRequest,
    edit: impl FnOnce(&mut Explained),
) {
    let asked = model.wall().expect("the model has a wall clock") - Duration::from_secs(3);
    let mut answers = fixture::explained(model, request.id, &request.key, asked);
    edit(&mut answers);
    app.apply_director(UiCommand::Explained(Box::new(answers)), model);
}

fn ctx_of(model: &Model) -> Ctx {
    Ctx {
        now: model.now().expect("the model has a clock"),
        wall: model.wall().expect("the model has a wall clock"),
        elapsed: Duration::ZERO,
    }
}

fn text_of(line: &Line<'_>) -> String {
    line.spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect()
}

/// The overlay's lines as text.
fn shown(app: &App, model: &Model, width: usize, height: usize) -> Vec<String> {
    let ctx = ctx_of(model);
    let scene = Scene {
        app,
        model: app.shown(model),
        ctx: &ctx,
        look: app.look(),
        kind: LayoutKind::Full,
    };
    let state = app.explain.as_ref().expect("the overlay is open");
    lines(&scene, state, width, height)
        .iter()
        .map(text_of)
        .collect()
}

/// The overlay drawn into a `width` by `height` body: its rows as text.
fn drawn(app: &App, model: &Model, width: u16, height: u16) -> Vec<String> {
    let ctx = ctx_of(model);
    let scene = Scene {
        app,
        model: app.shown(model),
        ctx: &ctx,
        look: app.look(),
        kind: LayoutKind::Full,
    };
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    render(
        &scene,
        app.explain.as_ref().expect("the overlay is open"),
        area,
        &mut buf,
    );
    (0..height).map(|y| row_text(&buf, y)).collect()
}

fn find<'a>(rows: &'a [String], needle: &str) -> &'a str {
    rows.iter()
        .map(String::as_str)
        .find(|row| row.contains(needle))
        .unwrap_or_else(|| panic!("no line holds {needle:?} in\n{}", rows.join("\n")))
}

fn has(rows: &[String], needle: &str) -> bool {
    rows.iter().any(|row| row.contains(needle))
}

/// A model that has seen `snapshot` and no ownership.
fn model_of(snapshot: &sundog::observe::ClusterSnapshot) -> Model {
    let base = Instant::now();
    let mut model = Model::new();
    model.apply(
        Update::Snapshot(Arc::new(snapshot.clone()), base),
        base,
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    );
    model
}

#[test]
fn the_overlay_echoes_the_key_kind_bytes_and_part() {
    let model = model();
    let app = opened(&model, "k1");
    let rows = shown(&app, &model, 100, 30);
    assert_eq!(rows[0], "key    k1▌");
    assert_eq!(rows[1], "       \"k1\" as String · 3 bytes 02 6b 31");
    assert!(
        rows[2].contains("prefix uint: int: hex: str:"),
        "{}",
        rows[2]
    );
    let part = PartId::of_key(&[2, b'k', b'1']);
    let line = find(&rows, "part   ");
    assert_eq!(
        line,
        format!(
            "part   {}/{} · bucket {}, part {}",
            part.bucket(),
            part.part(),
            part.bucket(),
            part.part()
        )
    );
    for (typed, echo) in [
        ("uint:7", "7 as unsigned integer · 1 byte 07"),
        ("int:-1", "-1 as signed integer · 1 byte 01"),
        ("hex:ff00", "ff 00 as postcard bytes · 2 bytes ff 00"),
    ] {
        let app = opened(&model, typed);
        let rows = shown(&app, &model, 100, 30);
        assert_eq!(rows[1].trim(), echo, "{typed}");
    }
}

#[test]
fn owners_are_in_fetch_order_with_slot_labels_and_the_first_owner_mark() {
    let model = model();
    let app = opened(&model, "k1");
    let rows = shown(&app, &model, 100, 30);
    let located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
    let first = &located.owners[0];
    let second = &located.owners[1];
    let tag = |owner: &crate::locate::LocatedOwner| data::tag_of(&model, owner.node);
    let first_row = find(&rows, "owner  1 ◆");
    assert!(
        first_row.contains(&format!("{} {}  live", tag(first).label, tag(first).short)),
        "{first_row}"
    );
    assert!(first_row.contains("first owner: a fetch asks it first"));
    let second_row = find(&rows, &format!("2   {}", tag(second).label));
    assert!(second_row.starts_with(NO_LABEL_TEXT), "{second_row}");
    assert!(second_row.contains("live"), "{second_row}");
    assert!(!second_row.contains('◆'), "{second_row}");
    let at = |row: &str| rows.iter().position(|r| r == row).unwrap();
    assert!(at(first_row) < at(second_row));
}

/// The text of a label column with no label.
const NO_LABEL_TEXT: &str = "       ";

#[test]
fn the_view_line_names_the_view_the_verdict_and_what_it_rests_on() {
    let model = model();
    let app = opened(&model, "k1");
    let rows = shown(&app, &model, 100, 30);
    let digest = model.ownership("it").unwrap();
    let view = find(&rows, "view   ");
    assert!(
        view.starts_with(&format!(
            "view   {} · k=2 · ranks parts · ",
            eventlog::view_hash(digest.view_hash)
        )),
        "{view}"
    );
    let settled = model.settle("it").unwrap();
    assert_eq!(view.contains("✔ settled"), settled.settled, "{view}");
    assert_eq!(
        view.contains("(gossip only)"),
        settled.gossip_only,
        "{view}"
    );
    let unsettled = testkit::fixture_model_with_metrics(Instant::now());
    assert!(!unsettled.settle("it").unwrap().settled);
    let rows = shown(&opened(&unsettled, "k1"), &unsettled, 100, 30);
    let view = find(&rows, "view   ");
    assert!(view.contains("↻ settling"), "{view}");
    assert!(!view.contains("✔ settled"), "{view}");
}

#[test]
fn a_cluster_with_no_distributed_cache_says_so() {
    let snapshot = sundog::observe::ClusterSnapshot::new(
        "fixture",
        (1..=3)
            .map(|index| {
                testkit::member_with(
                    index,
                    0,
                    1,
                    sundog::observe::MemberStatus::Live,
                    &[("churn", Mode::Replicated)],
                )
            })
            .collect(),
        0,
    );
    let model = model_of(&snapshot);
    let app = opened(&model, "k1");
    let rows = shown(&app, &model, 100, 30);
    assert_eq!(rows[0], "key    k1▌");
    assert!(
        has(
            &rows,
            "no Distributed cache is advertised: explain needs part ownership"
        ),
        "{rows:?}"
    );
    assert!(!has(&rows, "part   "), "{rows:?}");
    assert!(!has(&rows, "owner  "), "{rows:?}");
    let title = drawn(&app, &model, 100, 30);
    assert!(find(&title, "Explain a read").contains("╭ Explain a read ─"));
    assert!(!find(&title, "Explain a read").contains("computed"));
}

#[test]
fn discovering_marks_the_block_provisional() {
    let snapshot = testkit::snapshot_with_owners(3, 2);
    let mut model = model_of(&snapshot);
    let base = Instant::now();
    let digest = testkit::ownership_digest(&snapshot, "it").unwrap();
    model.apply(
        Update::Ownership(digest),
        base,
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    );
    assert!(model.discovering());
    let rows = shown(&opened(&model, "k1"), &model, 100, 30);
    assert!(has(&rows, "provisional: still discovering"), "{rows:?}");
    assert!(has(&rows, "part   "), "the placement still shows");
    let settled = self::model();
    assert!(!settled.discovering());
    let rows = shown(&opened(&settled, "k1"), &settled, 100, 30);
    assert!(!has(&rows, "provisional"), "{rows:?}");
}

#[test]
fn a_bad_key_shows_its_error_in_place_of_the_block() {
    let model = model();
    for (typed, error) in [
        ("uint:x", "uint: takes decimal digits"),
        ("uint:-1", "uint: takes no sign"),
        ("hex:abc", "hex: takes pairs of hex digits and this has 3"),
        ("int:", "int: takes an optional minus sign"),
    ] {
        let app = opened(&model, typed);
        let rows = shown(&app, &model, 100, 30);
        assert_eq!(rows[0], format!("key    {typed}▌"));
        assert!(
            rows[1].starts_with(NO_LABEL_TEXT) && rows[1].contains(error),
            "{rows:?}"
        );
        for gone in ["part   ", "view   ", "owner", "bytes"] {
            assert!(!has(&rows, gone), "{gone} in {rows:?}");
        }
        let title = drawn(&app, &model, 100, 30);
        assert!(!has(&title, "computed"), "{title:?}");
    }
    assert!(has(
        &shown(&opened(&model, "hex:zz"), &model, 100, 30),
        "fix the key to ask the nodes"
    ));
    // The note of an Enter on a bad key names the error and asks nobody.
    let mut app = opened(&model, "uint:x");
    assert_eq!(press(&mut app, &model, KeyCode::Enter), Action::Redraw);
    let rows = shown(&app, &model, 100, 30);
    assert!(
        has(&rows, "nobody is asked: uint: takes decimal digits"),
        "{rows:?}"
    );
}

#[test]
fn hostile_text_stays_in_the_allowlist() {
    use crate::explained::{Probe, Reads};

    let model = model();
    let mut app = opened(&model, "k\u{1b}[31m\u{202e}é\u{7}");
    let hostile = "\u{1b}[2J\u{7}boom \u{202e}é\u{85}";
    let dotted = printable(hostile);
    assert_ne!(dotted, hostile);
    assert!(!dotted.chars().any(char::is_control), "{dotted:?}");
    let allowed = |rows: &[String]| {
        for row in rows {
            for c in row.chars() {
                assert!(theme::is_allowed(c) && !c.is_control(), "{c:?} in {row}");
            }
        }
    };
    for check in [false, true] {
        allowed(&shown(&app, &model, 100, 30));
        if check {
            break;
        }
        typed(&mut app, &model, "k1");
        let request = ask(&mut app, &model);
        answered(&mut app, &model, &request, |answers| {
            answers.nodes[1].outcome = Outcome::Failed(hostile.to_owned());
            answers.nodes[2].label = SmolStr::new("n\u{1b}3\u{202e}");
            if let Outcome::Read(reading) = &mut answers.nodes[3].outcome {
                reading.local = Local::Other(hostile.to_owned());
                reading.source = Source::Other(hostile.to_owned());
                if let Some(dist) = &mut reading.distributed {
                    dist.probes = vec![
                        Probe {
                            node: hostile.to_owned(),
                            answer: Answer::StaleView {
                                responder_view: hostile.to_owned(),
                            },
                        },
                        Probe {
                            node: "ab".to_owned(),
                            answer: Answer::Held {
                                version: format!("{hostile}@{hostile}"),
                                expires_at_ms: None,
                                reads: Reads::Value,
                            },
                        },
                    ];
                    dist.view = hostile.to_owned();
                    dist.view_moved_to = Some(hostile.to_owned());
                }
            }
            if let Outcome::Read(reading) = &mut answers.nodes[4].outcome {
                reading.local = Local::Live {
                    version: format!("{hostile}@{hostile}"),
                    expires_at_ms: None,
                    spilled: false,
                };
                reading.source = Source::Owner {
                    node: hostile.to_owned(),
                    hit: true,
                };
            }
        });
    }
    allowed(&drawn(&app, &model, 140, 40));
    // The detail block of each node that carries hostile text, drawn
    // as the allowlist lets it and still saying what it holds. Down
    // moves to n2, whose reply is the text.
    let selected = |app: &mut App, steps: usize, label: &str| {
        for _ in 0..steps {
            press(app, &model, KeyCode::Down);
        }
        let rows = shown(app, &model, 116, 60);
        allowed(&rows);
        allowed(&drawn(app, &model, 140, 40));
        assert!(has(&rows, &format!("{label} in detail")), "{rows:?}");
        rows
    };
    let rows = selected(&mut app, 1, "n2");
    assert!(find(&rows, "reply   ").contains(&dotted), "{rows:?}");
    // n3 answers under a label no slot has, so n4 is three Downs from
    // the first.
    let rows = selected(&mut app, 2, "n4");
    let moved = format!("{dotted} · moved to {dotted} during the call");
    assert!(find(&rows, "  view    ").contains(&moved), "{rows:?}");
    assert!(find(&rows, "record  ").contains(&dotted), "{rows:?}");
    assert!(find(&rows, "source  ").contains(&dotted), "{rows:?}");
    assert!(
        find(&rows, "probes  ").contains(&format!("its view is {dotted}")),
        "{rows:?}"
    );
    assert!(has(&rows, &format!("{dotted}@")), "{rows:?}");
    let rows = selected(&mut app, 1, "n5");
    assert!(
        find(&rows, "record  ").contains(&format!("live {dotted}@")),
        "{rows:?}"
    );
    assert!(
        find(&rows, "source  ").contains("the first owner to answer is"),
        "{rows:?}"
    );
}

#[test]
fn asking_names_the_nodes() {
    let model = model();
    let mut app = opened(&model, "k1");
    let rows = shown(&app, &model, 100, 30);
    assert!(has(&rows, "Enter asks 5 nodes about this key"), "{rows:?}");
    ask(&mut app, &model);
    let rows = shown(&app, &model, 100, 30);
    assert!(has(&rows, "asking n1 n2 n3 n4 n5"), "{rows:?}");
    assert!(!has(&rows, "NODE"), "no table before the answers: {rows:?}");
}

#[test]
fn what_enter_does_is_stated_before_and_after_it_is_pressed() {
    let model = model();
    let mut app = opened(&model, "uint:5");
    let rows = shown(&app, &model, 100, 30);
    assert!(has(&rows, "test nodes take String keys"), "{rows:?}");
    assert_eq!(press(&mut app, &model, KeyCode::Enter), Action::Redraw);
    let rows = shown(&app, &model, 100, 30);
    assert!(
        has(&rows, "nobody is asked: test nodes take String keys"),
        "{rows:?}"
    );
    // A lens without control ports says what asks need.
    let mut plain = app_in(Look::default(), None);
    typed(&mut plain, &model, "k1");
    let rows = shown(&plain, &model, 100, 30);
    assert!(
        has(&rows, "asked: test nodes answer over their control ports"),
        "{rows:?}"
    );
    assert!(
        !has(&rows, "Enter ask"),
        "no Enter hint without ports: {rows:?}"
    );
    assert_eq!(
        rows.last().unwrap(),
        "Ctrl-U clear  Esc close  Ctrl-C quit",
        "the prompt types q, so the hints name the key that quits"
    );
    press(&mut plain, &model, KeyCode::Enter);
    let rows = shown(&plain, &model, 100, 30);
    assert!(
        has(&rows, "nobody is asked: this lens does not know"),
        "{rows:?}"
    );
    // The key hints with ports.
    let rows = shown(&opened(&model, "k1"), &model, 100, 30);
    assert_eq!(
        rows.last().unwrap(),
        "Enter ask  ↑↓ node  Ctrl-U clear  Esc close  Ctrl-C quit"
    );
}

#[test]
fn an_old_result_gives_way_to_the_prompt_once_the_key_changes() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |_| {});
    assert!(has(&shown(&app, &model, 116, 40), "NODE"));
    press(&mut app, &model, KeyCode::Char('0'));
    let rows = shown(&app, &model, 116, 40);
    assert!(!has(&rows, "NODE"), "{rows:?}");
    assert!(has(&rows, "Enter asks 5 nodes about this key"), "{rows:?}");
    press(&mut app, &model, KeyCode::Backspace);
    assert!(
        has(&shown(&app, &model, 116, 40), "NODE"),
        "the same key shows its answers again"
    );
}

#[test]
fn a_long_key_shows_its_end_beside_the_cursor() {
    let model = model();
    let text = format!("{}end", "x".repeat(100));
    let app = opened(&model, &text);
    let rows = shown(&app, &model, 60, 30);
    assert!(rows[0].ends_with("xend▌"), "{}", rows[0]);
    assert!(rows[0].chars().count() <= 60, "{}", rows[0]);
    assert!(
        rows[1].chars().count() <= 60 && rows[1].ends_with('…'),
        "{}",
        rows[1]
    );
}

#[test]
fn a_frozen_display_asks_the_live_nodes_and_says_so_before_it_asks() {
    let model = model();
    let mut app = app_in(Look::default(), Some(control()));
    press(&mut app, &model, KeyCode::Char('p'));
    typed(&mut app, &model, "k1");
    let rows = shown(&app, &model, 100, 30);
    assert!(
        has(&rows, "Enter asks the live nodes about this key"),
        "{rows:?}"
    );
    assert!(!has(&rows, "Enter asks 5 nodes"), "{rows:?}");
    // Unfrozen, it counts the nodes it lists.
    press(&mut app, &model, KeyCode::Esc);
    press(&mut app, &model, KeyCode::Char('p'));
    typed(&mut app, &model, "k1");
    assert!(!app.is_frozen());
    let rows = shown(&app, &model, 100, 30);
    assert!(has(&rows, "Enter asks 5 nodes about this key"), "{rows:?}");
}

#[test]
fn one_live_node_is_asked_in_the_singular() {
    let snapshot = testkit::snapshot_with_owners(1, 2);
    let mut model = model_of(&snapshot);
    let digest = testkit::ownership_digest(&snapshot, "it").unwrap();
    model.apply(
        Update::Ownership(digest),
        Instant::now(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    );
    let rows = shown(&opened(&model, "k1"), &model, 100, 30);
    assert!(has(&rows, "Enter asks 1 node about this key"), "{rows:?}");
    assert!(!has(&rows, "1 nodes"), "{rows:?}");
}

#[test]
fn a_view_that_ranks_no_member_says_so_in_place_of_the_owners() {
    let model = model();
    let app = opened(&model, "k1");
    let mut located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
    located.owners.clear();
    let ctx = ctx_of(&model);
    let scene = Scene {
        app: &app,
        model: &model,
        ctx: &ctx,
        look: app.look(),
        kind: LayoutKind::Full,
    };
    let owners: Vec<String> = owner_lines(&scene, &located).iter().map(text_of).collect();
    assert_eq!(owners, ["owner  none: the view ranks no member"]);
    // With owners there is a line for each, in fetch order.
    let located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
    assert_eq!(owner_lines(&scene, &located).len(), located.owners.len());
}

#[test]
fn a_cluster_with_no_distributed_cache_wraps_its_remedy_under_the_key() {
    let snapshot = sundog::observe::ClusterSnapshot::new(
        "fixture",
        (1..=3)
            .map(|index| {
                testkit::member_with(
                    index,
                    0,
                    1,
                    sundog::observe::MemberStatus::Live,
                    &[("churn", Mode::Replicated)],
                )
            })
            .collect(),
        0,
    );
    let model = model_of(&snapshot);
    let app = opened(&model, "k1");
    let error = LocateError::NoDistributedCache.to_string();
    // 72 cells is what the 80-column screen leaves a line of the popup.
    for width in [72, 100] {
        let rows = shown(&app, &model, width, 30);
        let message: Vec<&String> = rows[3..]
            .iter()
            .take_while(|row| !row.trim().is_empty())
            .collect();
        assert!(
            message.iter().all(|row| row.chars().count() <= width
                && row.starts_with(NO_LABEL_TEXT)
                && !row.ends_with('…')),
            "{width}: {message:?}"
        );
        let whole = message
            .iter()
            .map(|row| row.trim())
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(whole, error, "{width}: the whole sentence shows");
        assert!(
            message.len() > 1,
            "{width}: the sentence takes several lines"
        );
    }
}

#[test]
fn wrap_breaks_at_spaces_and_keeps_a_long_word_whole() {
    assert_eq!(wrap("a bb ccc dddd", 6), ["a bb", "ccc", "dddd"]);
    assert_eq!(wrap("a bb ccc", 8), ["a bb ccc"]);
    assert_eq!(wrap("  spaced   out  ", 20), ["spaced out"]);
    assert_eq!(wrap("abcdefgh ij", 4), ["abcdefgh", "ij"]);
    assert_eq!(wrap("", 10), Vec::<String>::new());
    assert_eq!(wrap("é é é", 3), ["é é", "é"], "width counts characters");
}
