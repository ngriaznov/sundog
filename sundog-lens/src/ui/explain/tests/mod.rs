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

mod computed_block;
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
