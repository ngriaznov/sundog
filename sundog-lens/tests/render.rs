//! Renders every view of the interface through a buffer and checks the
//! frames: landmarks at four screen sizes, no panic at any size, the glyph
//! allowlist, the color modes and a golden Overview.
//!
//! The clock and the spinner are injected, so every frame is the same on
//! every run. To retake the golden after a deliberate change to the Overview:
//!
//! ```sh
//! UPDATE_GOLDEN=1 cargo test -p sundog-lens --test render
//! ```

use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Color;
use sundog_lens::app::{Action, App, AppConfig, FleetCmd, UiCommand};
use sundog_lens::cli::DisplayArgs;
use sundog_lens::explained::fixture;
use sundog_lens::model::Model;
use sundog_lens::model::testkit;
use sundog_lens::source::targets::UrlTemplate;
use sundog_lens::ui::look::Look;
use sundog_lens::ui::theme::{self, ColorMode};
use sundog_lens::ui::{self, Ctx, View};

const SIZES: [(u16, u16); 4] = [(140, 40), (120, 36), (100, 30), (80, 24)];

const VIEWS: [View; 4] = [View::Overview, View::Caches, View::Node, View::Timeline];

fn config(mode: ColorMode) -> AppConfig {
    AppConfig {
        look: Look::with_mode(&DisplayArgs::default(), mode),
        cluster: "fixture".to_owned(),
        seeds: vec!["127.0.0.11:7946".to_owned(), "127.0.0.12:7946".to_owned()],
        observer: Some("127.0.0.1:41733".parse().unwrap()),
        scrape_interval: Some(Duration::from_secs(1)),
        ..AppConfig::default()
    }
}

fn ctx_of(model: &Model) -> Ctx {
    Ctx {
        now: model.now().unwrap_or_else(Instant::now),
        wall: model.wall().unwrap_or(std::time::UNIX_EPOCH),
        elapsed: Duration::ZERO,
    }
}

fn app_for(model: &Model, view: View) -> App {
    let mut app = App::new(config(ColorMode::Truecolor));
    app.observe(model, ctx_of(model).now);
    app.snap();
    app.apply_director(UiCommand::Tab(view), model);
    app
}

fn render(app: &App, model: &Model, width: u16, height: u16) -> Buffer {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    ui::render(&mut buf, area, app, model, &ctx_of(model));
    buf
}

fn rows(buf: &Buffer) -> Vec<String> {
    (0..buf.area.height)
        .map(|y| {
            let mut row = String::new();
            for x in 0..buf.area.width {
                row.push_str(buf[(x, y)].symbol());
            }
            row.trim_end().to_owned()
        })
        .collect()
}

fn text(buf: &Buffer) -> String {
    rows(buf).join("\n")
}

fn bare() -> Model {
    testkit::fixture_model(Instant::now())
}

fn live() -> Model {
    testkit::fixture_model_with_metrics(Instant::now())
}

fn press(app: &mut App, model: &Model, c: char) {
    app.handle_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE), model);
}

/// Where the fixture nodes' control ports are: 134 above their gossip ports.
fn control() -> UrlTemplate {
    UrlTemplate::parse("{ip}:{gossip_port+134}").expect("the template parses")
}

/// The states the explain overlay is drawn in.
#[derive(Clone, Copy, Debug)]
enum Stage {
    /// Open on an empty key, before anything is asked.
    Prompt,
    /// `Enter` pressed, no answer back.
    Asking,
    /// The nodes' answers drawn.
    Answered,
}

const STAGES: [Stage; 3] = [Stage::Prompt, Stage::Asking, Stage::Answered];

/// An app on `view` with the explain overlay in `stage` for the key `k1`,
/// whose control ports are known.
fn explaining(model: &Model, view: View, stage: Stage, mode: ColorMode) -> App {
    let mut app = App::new(AppConfig {
        control: Some(control()),
        ..config(mode)
    });
    app.observe(model, ctx_of(model).now);
    app.snap();
    app.apply_director(UiCommand::Tab(view), model);
    press(&mut app, model, 'e');
    let ctrl_u = KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL);
    app.handle_key(ctrl_u, model);
    if matches!(stage, Stage::Prompt) {
        return app;
    }
    press(&mut app, model, 'k');
    press(&mut app, model, '1');
    let enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    let Action::Fleet(FleetCmd::Explain(request)) = app.handle_key(enter, model) else {
        // A cluster with no live node has nobody to ask.
        return app;
    };
    if matches!(stage, Stage::Answered) {
        let asked = ctx_of(model).wall - Duration::from_secs(3);
        let answers = fixture::explained(model, request.id, &request.key, asked);
        app.apply_director(UiCommand::Explained(Box::new(answers)), model);
    }
    app
}

#[test]
fn every_view_renders_at_every_size_with_its_landmarks() {
    for model in [bare(), live()] {
        for (width, height) in SIZES {
            for view in VIEWS {
                let app = app_for(&model, view);
                let shown = text(&render(&app, &model, width, height));
                let context = format!("{view:?} at {width}x{height}\n{shown}");
                assert!(shown.contains("sundog lens"), "{context}");
                assert!(shown.contains("fixture"), "{context}");
                assert!(
                    shown.contains("1 Overview") && shown.contains("4 Timeline"),
                    "{context}"
                );
                assert!(shown.contains("quit") || shown.contains("? q"), "{context}");
                match view {
                    View::Overview => {
                        assert!(shown.contains("Members"), "{context}");
                        assert!(shown.contains("◐") || shown.contains("◒"), "{context}");
                        assert!(shown.contains("Ownership · it"), "{context}");
                        assert!(shown.contains("Events"), "{context}");
                    }
                    View::Caches => {
                        assert!(shown.contains("Caches · gossip"), "{context}");
                        assert!(shown.contains("it · distributed k=2"), "{context}");
                    }
                    View::Node => {
                        assert!(shown.contains("● n1"), "{context}");
                        assert!(shown.contains("caches on n1"), "{context}");
                        assert!(shown.contains("events for n1"), "{context}");
                    }
                    View::Timeline => {
                        assert!(shown.contains("Lifelines · last"), "{context}");
                        assert!(shown.contains("[all]"), "{context}");
                    }
                }
            }
        }
    }
}

#[test]
fn the_explain_overlay_draws_over_every_view_at_every_size() {
    for model in [bare(), live()] {
        for (width, height) in SIZES {
            for view in VIEWS {
                for stage in STAGES {
                    let app = explaining(&model, view, stage, ColorMode::Truecolor);
                    let shown = text(&render(&app, &model, width, height));
                    let context = format!("{stage:?} over {view:?} at {width}x{height}\n{shown}");
                    // The header, tabs and footer stay visible around it.
                    assert!(shown.contains("sundog lens"), "{context}");
                    assert!(shown.contains("1 Overview"), "{context}");
                    assert!(
                        shown.contains("e explain") || shown.contains("e ? q"),
                        "{context}"
                    );
                    for landmark in [
                        "Explain a read · it · computed",
                        "key    k1▌",
                        "part   ",
                        "owner  1 ◆",
                        "Esc close",
                    ] {
                        if matches!(stage, Stage::Prompt) && landmark == "key    k1▌" {
                            continue;
                        }
                        assert!(shown.contains(landmark), "{landmark}: {context}");
                    }
                    let stage_landmarks: &[&str] = match stage {
                        Stage::Prompt => &["type a key: Enter asks"],
                        Stage::Asking => &["asking n1 n2 n3 n4 n5"],
                        Stage::Answered => &["✔ 5 nodes agree", "NODE"],
                    };
                    for landmark in stage_landmarks {
                        assert!(shown.contains(landmark), "{landmark}: {context}");
                    }
                }
            }
        }
    }
}

#[test]
fn a_frozen_display_names_itself_in_the_overlay_and_help_draws_over_it() {
    let model = bare();
    let mut app = explaining(
        &model,
        View::Overview,
        Stage::Answered,
        ColorMode::Truecolor,
    );
    app.apply_director(UiCommand::Help(true), &model);
    let shown = text(&render(&app, &model, 140, 40));
    assert!(shown.contains("sundog lens · keys"), "{shown}");
    app.apply_director(UiCommand::Help(false), &model);
    // `p` types while the overlay is open: freeze before opening it.
    let mut frozen = App::new(config(ColorMode::Truecolor));
    frozen.observe(&model, ctx_of(&model).now);
    press(&mut frozen, &model, 'p');
    press(&mut frozen, &model, 'e');
    let shown = text(&render(&frozen, &model, 140, 40));
    assert!(shown.contains("(display frozen)"), "{shown}");
    assert!(shown.contains("‖ frozen"), "{shown}");
}

#[test]
fn the_full_overview_with_metrics_shows_throughput_and_agreement() {
    let model = live();
    let app = app_for(&model, View::Overview);
    let shown = text(&render(&app, &model, 140, 40));
    for landmark in [
        "Throughput · metrics",
        "ops/s",
        "hit ",
        "reads ",
        "metrics 6/6 · 1 s",
        "reported ↻ 4/5",
        "↻",
        "✓",
    ] {
        assert!(shown.contains(landmark), "{landmark} in\n{shown}");
    }
    assert!(!shown.contains("no exporter mapped"), "{shown}");
}

#[test]
fn the_help_overlay_splash_and_too_small_notice_render() {
    let model = bare();
    let mut app = app_for(&model, View::Overview);
    press(&mut app, &model, '?');
    let help = text(&render(&app, &model, 140, 40));
    assert!(help.contains("sundog lens · keys"), "{help}");
    assert!(help.contains("next/prev Distributed cache"), "{help}");
    assert!(
        help.contains("Members"),
        "the overview stays behind the popup:\n{help}"
    );

    let empty = Model::new();
    let app = App::new(config(ColorMode::Truecolor));
    let splash = text(&render(&app, &empty, 140, 40));
    assert!(
        splash.contains("listening for gossip from \"fixture\""),
        "{splash}"
    );
    assert!(
        splash.contains("seeds 127.0.0.11:7946, 127.0.0.12:7946 · observer 127.0.0.1:41733"),
        "{splash}"
    );
    assert!(splash.contains("never a peer"), "{splash}");

    let small = text(&render(&app, &empty, 72, 20));
    assert_eq!(small.trim(), "sundog-lens needs 80×24 (now 72×20)");
}

#[test]
fn a_frozen_display_says_so_and_a_demo_has_a_caption_row() {
    let model = bare();
    let mut app = app_for(&model, View::Overview);
    press(&mut app, &model, 'p');
    assert!(text(&render(&app, &model, 140, 40)).contains("‖ frozen"));

    let mut demo = App::new(AppConfig {
        demo: true,
        ..config(ColorMode::Truecolor)
    });
    demo.apply_director(UiCommand::Caption(Some("A graceful leave".into())), &model);
    let rows = rows(&render(&demo, &model, 140, 40));
    assert!(
        rows[38].contains("▶ 0:00  A graceful leave"),
        "{}",
        rows[38]
    );
    assert!(
        rows[39].contains("demo: S spawn  K kill  L leave  R restart"),
        "{}",
        rows[39]
    );
}

#[test]
fn selection_and_filter_show_on_screen() {
    let model = bare();
    let mut app = app_for(&model, View::Overview);
    press(&mut app, &model, 'j');
    press(&mut app, &model, 'j');
    let overview = rows(&render(&app, &model, 140, 40));
    let selected = overview
        .iter()
        .find(|r| r.contains('▌'))
        .expect("a selected row");
    assert!(selected.contains("n3"), "{selected}");
    press(&mut app, &model, 'f');
    press(&mut app, &model, 'f');
    let filtered = text(&render(&app, &model, 140, 40));
    assert!(filtered.contains("ownership · "), "{filtered}");
    assert!(!filtered.contains(" JOIN "), "{filtered}");
}

#[test]
fn no_size_from_1x1_to_200x60_panics() {
    let bare = bare();
    let live = live();
    let empty = Model::new();
    for model in [&bare, &live, &empty] {
        for view in VIEWS {
            for help in [false, true] {
                let mut app = app_for(model, view);
                app.help = help;
                let mut width = 1;
                while width <= 200 {
                    let mut height = 1;
                    while height <= 60 {
                        let _ = render(&app, model, width, height);
                        height += 7;
                    }
                    width += 7;
                }
            }
            // The overlay does not depend on the view beneath it, and help
            // draws over it: help joins the answered overlay only.
            let overlays: &[(Stage, bool)] = if view == View::Overview {
                &[
                    (Stage::Prompt, false),
                    (Stage::Asking, false),
                    (Stage::Answered, false),
                    (Stage::Answered, true),
                ]
            } else {
                &[]
            };
            for &(stage, help) in overlays {
                let mut app = explaining(model, view, stage, ColorMode::Truecolor);
                app.help = help;
                // The overlay needs 80x24; below that the screen is a
                // notice, which the sweep above covers.
                let widths = (71..=200).step_by(7).chain([79, 80]);
                for width in widths {
                    for height in (22..=60).step_by(7).chain([23, 24]) {
                        let _ = render(&app, model, width, height);
                    }
                }
            }
        }
    }
}

#[test]
fn a_frame_too_small_for_the_overlay_hands_its_keys_back() {
    let model = bare();
    let mut app = explaining(&model, View::Overview, Stage::Prompt, ColorMode::Truecolor);
    let q = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    // A frame that draws the overlay leaves it the keys, `q` included.
    let shown = text(&render(&app, &model, 80, 24));
    assert!(shown.contains("Explain a read"), "{shown}");
    assert_eq!(app.handle_key(q, &model), Action::Redraw);
    // A frame that is only the size notice draws no overlay, so `q` quits.
    let small = text(&render(&app, &model, 72, 20));
    assert!(!small.contains("Explain a read"), "{small}");
    assert_eq!(app.handle_key(q, &model), Action::Quit);
    // The next frame that fits gives the overlay its keys back.
    let shown = text(&render(&app, &model, 80, 24));
    assert!(shown.contains("Explain a read"), "{shown}");
    assert_eq!(app.handle_key(q, &model), Action::Redraw);
}

fn assert_allowed(buf: &Buffer, context: &str) {
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            for c in buf[(x, y)].symbol().chars() {
                assert!(
                    theme::is_allowed(c),
                    "{c:?} (U+{:04X}) at ({x}, {y}) is not in the allowlist: {context}",
                    u32::from(c)
                );
            }
        }
    }
}

#[test]
fn every_rendered_glyph_is_in_the_allowlist() {
    for model in [bare(), live()] {
        for (width, height) in SIZES {
            for view in VIEWS {
                for braille in [true, false] {
                    let mut app = App::new(AppConfig {
                        look: Look {
                            braille,
                            ..Look::default()
                        },
                        ..config(ColorMode::Truecolor)
                    });
                    app.observe(&model, ctx_of(&model).now);
                    app.snap();
                    app.apply_director(UiCommand::Tab(view), &model);
                    let context = format!("{view:?} {width}x{height} braille={braille}");
                    assert_allowed(&render(&app, &model, width, height), &context);
                    app.help = true;
                    assert_allowed(
                        &render(&app, &model, width, height),
                        &format!("help {context}"),
                    );
                    for stage in STAGES {
                        let mut explain = explaining(&model, view, stage, ColorMode::Truecolor);
                        explain.apply_director(UiCommand::Help(false), &model);
                        assert_allowed(
                            &render(&explain, &model, width, height),
                            &format!("explain {stage:?} {context}"),
                        );
                    }
                }
            }
        }
    }
    let empty = Model::new();
    let app = App::new(config(ColorMode::Truecolor));
    assert_allowed(&render(&app, &empty, 140, 40), "splash");
    assert_allowed(&render(&app, &empty, 72, 20), "too small");
}

#[test]
fn the_banned_glyphs_never_appear() {
    let model = live();
    for view in VIEWS {
        let mut app = app_for(&model, view);
        for help in [false, true] {
            app.help = help;
            let shown = text(&render(&app, &model, 140, 40));
            for banned in theme::BANNED.chars() {
                assert!(!shown.contains(banned), "{banned} in {view:?}");
            }
        }
        for stage in STAGES {
            let app = explaining(&model, view, stage, ColorMode::Truecolor);
            let shown = text(&render(&app, &model, 140, 40));
            for banned in theme::BANNED.chars() {
                assert!(!shown.contains(banned), "{banned} in {view:?} {stage:?}");
            }
        }
    }
}

fn colors(buf: &Buffer) -> Vec<Color> {
    let mut all = Vec::new();
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            all.push(buf[(x, y)].fg);
            all.push(buf[(x, y)].bg);
        }
    }
    all
}

#[test]
fn truecolor_256_and_mono_use_their_own_color_spaces() {
    let model = live();
    for view in VIEWS {
        let draw = |mode| {
            let mut app = App::new(config(mode));
            app.observe(&model, ctx_of(&model).now);
            app.snap();
            app.apply_director(UiCommand::Tab(view), &model);
            colors(&render(&app, &model, 140, 40))
        };
        let truecolor = draw(ColorMode::Truecolor);
        assert!(
            truecolor.iter().any(|c| matches!(c, Color::Rgb(..))),
            "{view:?}"
        );
        assert!(
            truecolor
                .iter()
                .all(|c| matches!(c, Color::Rgb(..) | Color::Reset)),
            "{view:?}"
        );
        let indexed = draw(ColorMode::Ansi256);
        assert!(
            indexed.iter().any(|c| matches!(c, Color::Indexed(_))),
            "{view:?}"
        );
        assert!(
            indexed
                .iter()
                .all(|c| matches!(c, Color::Indexed(_) | Color::Reset)),
            "{view:?}"
        );
        let mono = draw(ColorMode::Mono);
        assert!(
            mono.iter().all(|c| *c == Color::Reset),
            "{view:?}: {:?}",
            mono.iter().find(|c| **c != Color::Reset)
        );
    }
}

#[test]
fn the_explain_overlay_uses_each_color_space_and_mono_uses_none() {
    let model = live();
    for stage in STAGES {
        let draw = |mode| {
            let app = explaining(&model, View::Overview, stage, mode);
            colors(&render(&app, &model, 140, 40))
        };
        assert!(
            draw(ColorMode::Truecolor)
                .iter()
                .all(|c| matches!(c, Color::Rgb(..) | Color::Reset)),
            "{stage:?}"
        );
        assert!(
            draw(ColorMode::Ansi256)
                .iter()
                .all(|c| matches!(c, Color::Indexed(_) | Color::Reset)),
            "{stage:?}"
        );
        let mono = draw(ColorMode::Mono);
        assert!(
            mono.iter().all(|c| *c == Color::Reset),
            "{stage:?}: {:?}",
            mono.iter().find(|c| **c != Color::Reset)
        );
    }
}

#[test]
fn the_background_is_painted_under_every_cell_unless_turned_off() {
    let model = bare();
    let app = app_for(&model, View::Overview);
    let painted = render(&app, &model, 140, 40);
    let bg = Color::Rgb(0x12, 0x11, 0x0F);
    let surface = Color::Rgb(0x1B, 0x1A, 0x17);
    let background = |x: u16, y: u16| painted[(x, y)].bg;
    // The body is bg, the header and footer rows surface; no cell is left
    // on the terminal's own background.
    for y in 0..40 {
        for x in 0..140 {
            assert_ne!(background(x, y), Color::Reset, "({x}, {y})");
        }
    }
    assert_eq!(background(0, 0), surface);
    assert_eq!(background(0, 39), surface);
    assert_eq!(background(139, 20), bg);

    let mut bare_app = App::new(AppConfig {
        look: Look {
            paint_bg: false,
            ..Look::default()
        },
        ..config(ColorMode::Truecolor)
    });
    bare_app.observe(&model, ctx_of(&model).now);
    bare_app.snap();
    let unpainted = render(&bare_app, &model, 140, 40);
    assert_eq!(unpainted[(139, 20)].bg, Color::Reset);
}

#[test]
fn the_selected_row_and_the_focused_panel_use_the_amber_accent() {
    let model = bare();
    let app = app_for(&model, View::Overview);
    let buf = render(&app, &model, 140, 40);
    let amber = Color::Rgb(0xF2, 0xB5, 0x44);
    let overview = rows(&buf);
    let selected_y = overview.iter().position(|r| r.contains('▌')).unwrap();
    let bar_x = overview[selected_y].chars().position(|c| c == '▌').unwrap();
    assert_eq!(
        buf[(
            u16::try_from(bar_x).unwrap(),
            u16::try_from(selected_y).unwrap()
        )]
            .fg,
        amber
    );
    // The Members panel is focused: its border is amber; Ownership's is not.
    assert_eq!(buf[(0, 2)].fg, amber);
    let ownership_y = overview
        .iter()
        .position(|r| r.starts_with("╭ Ownership"))
        .unwrap();
    assert_ne!(buf[(0, u16::try_from(ownership_y).unwrap())].fg, amber);
}

#[test]
fn the_mosaic_paints_each_bucket_in_its_leads_node_color() {
    let model = bare();
    let app = app_for(&model, View::Overview);
    let buf = render(&app, &model, 140, 40);
    let overview = rows(&buf);
    let top = overview
        .iter()
        .position(|r| r.starts_with("╭ Ownership"))
        .unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for y in top + 1..top + 9 {
        for x in 1..=64u16 {
            let cell = &buf[(x + 1, u16::try_from(y).unwrap())];
            assert_eq!(cell.symbol(), "▀");
            for color in [cell.fg, cell.bg] {
                if let Color::Rgb(r, g, b) = color {
                    seen.insert((r, g, b));
                }
            }
        }
    }
    let palette: Vec<_> = theme::NODE_COLORS.iter().map(|c| (c.0, c.1, c.2)).collect();
    assert!(seen.len() >= 4, "{seen:?}");
    assert!(seen.iter().all(|c| palette.contains(c)), "{seen:?}");
}

/// Compares `shown` with `tests/golden/<name>.txt`, or retakes the file when
/// `UPDATE_GOLDEN` is set.
fn assert_golden(name: &str, shown: &str) {
    let path = format!("{}/tests/golden/{name}.txt", env!("CARGO_MANIFEST_DIR"));
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, shown).expect("the golden is written");
        return;
    }
    let golden = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("tests/golden/{name}.txt is unreadable: {error}"))
        .replace("\r\n", "\n");
    if golden != shown {
        let mismatch = golden
            .lines()
            .zip(shown.lines())
            .position(|(a, b)| a != b)
            .unwrap_or_else(|| golden.lines().count().min(shown.lines().count()));
        panic!(
            "{name} differs from its golden at line {}\n golden: {:?}\n  shown: {:?}\nretake it with UPDATE_GOLDEN=1",
            mismatch + 1,
            golden.lines().nth(mismatch),
            shown.lines().nth(mismatch)
        );
    }
}

fn frame_text(app: &App, model: &Model) -> String {
    format!("{}\n", rows(&render(app, model, 140, 40)).join("\n"))
}

#[test]
fn the_overview_matches_the_golden() {
    let model = live();
    let app = app_for(&model, View::Overview);
    assert_golden("overview_140x40", &frame_text(&app, &model));
}

#[test]
fn the_caches_view_matches_the_golden() {
    let model = live();
    let app = app_for(&model, View::Caches);
    assert_golden("caches_140x40", &frame_text(&app, &model));
}

#[test]
fn the_node_view_matches_the_golden() {
    let model = live();
    let mut app = app_for(&model, View::Node);
    app.apply_director(UiCommand::Select("n3".into()), &model);
    assert_golden("node_140x40", &frame_text(&app, &model));
}

#[test]
fn the_timeline_matches_the_golden() {
    let model = live();
    let app = app_for(&model, View::Timeline);
    assert_golden("timeline_140x40", &frame_text(&app, &model));
}

#[test]
fn the_help_overlay_matches_the_golden() {
    let model = live();
    let mut app = app_for(&model, View::Overview);
    app.help = true;
    assert_golden("help_140x40", &frame_text(&app, &model));
}

#[test]
fn the_splash_matches_the_golden() {
    let empty = Model::new();
    let app = App::new(config(ColorMode::Truecolor));
    assert_golden("splash_140x40", &frame_text(&app, &empty));
}

#[test]
fn the_explain_overlay_matches_the_golden() {
    let model = live();
    let app = explaining(
        &model,
        View::Overview,
        Stage::Answered,
        ColorMode::Truecolor,
    );
    assert_golden("explain_140x40", &frame_text(&app, &model));
}

/// Prints one frame, for looking at a layout while changing it:
///
/// ```sh
/// DUMP_VIEW=overview DUMP_SIZE=100x30 DUMP_METRICS=1 DUMP_EXPLAIN=answered \
///     cargo test -p sundog-lens --test render -- --ignored --nocapture dump_a_frame
/// ```
#[test]
#[ignore = "a development aid that prints a frame"]
fn dump_a_frame() {
    let view = std::env::var("DUMP_VIEW")
        .ok()
        .and_then(|name| View::from_name(&name))
        .unwrap_or(View::Overview);
    let (width, height) = std::env::var("DUMP_SIZE")
        .ok()
        .and_then(|size| {
            let (w, h) = size.split_once('x')?;
            Some((w.parse().ok()?, h.parse().ok()?))
        })
        .unwrap_or((140, 40));
    let model = if std::env::var_os("DUMP_METRICS").is_some() {
        live()
    } else {
        bare()
    };
    let app = match std::env::var("DUMP_EXPLAIN").as_deref() {
        Ok("prompt") => explaining(&model, view, Stage::Prompt, ColorMode::Truecolor),
        Ok("asking") => explaining(&model, view, Stage::Asking, ColorMode::Truecolor),
        Ok(_) => explaining(&model, view, Stage::Answered, ColorMode::Truecolor),
        Err(_) => app_for(&model, view),
    };
    println!("{}", rows(&render(&app, &model, width, height)).join("\n"));
}
