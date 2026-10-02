//! Event rows: one line per event, in the format the Overview and the
//! Timeline share.

use std::fmt::Write as _;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::Widget;
use sundog::wire::PROTOCOL_VERSION;

use super::data::{self, NodeTag};
use super::look::Token;
use super::panel::gap;
use super::theme::{self, Rgb};
use super::{Scene, anim, text};
use crate::app::EVENT_FLASH;
use crate::model::derive::PART_SPACE;
use crate::model::events::{Event, EventKind};
use crate::model::exporter::is_answering;

/// The width of the tag column.
const TAG_WIDTH: usize = 8;

/// The glyph of an event and the token it is drawn in.
#[must_use]
pub fn glyph(kind: &EventKind) -> (char, Token) {
    match kind {
        EventKind::Join { .. } => ('✚', Token::Info),
        EventKind::Leave { .. } => ('◐', Token::Warn),
        EventKind::Left { .. } => ('○', Token::Muted),
        EventKind::Down { .. } | EventKind::Drop { .. } => ('✖', Token::Bad),
        EventKind::Up { .. } | EventKind::Rejoin { .. } | EventKind::Restart { .. } => {
            ('↻', Token::Info)
        }
        EventKind::CacheAdded { .. } | EventKind::CacheRemoved { .. } => ('▸', Token::Info),
        EventKind::Conflict { .. } => ('⚠', Token::Bad),
        EventKind::Proto { .. } | EventKind::Unreachable { .. } => ('⚠', Token::Warn),
        EventKind::View { .. } => ('⇄', Token::Move),
        EventKind::Settled { .. } => ('✔', Token::Ok),
        EventKind::Xfer { .. } => ('⇣', Token::Info),
        EventKind::Ready { .. } => ('✓', Token::Ok),
        EventKind::Unready { .. } => ('…', Token::Warn),
        EventKind::Exporter { detail, .. } if is_answering(detail) => ('✓', Token::Ok),
        EventKind::Exporter { .. } => ('⚠', Token::Muted),
    }
}

/// The tag as the log shows it: `UNREACHABLE` shortens to fit the column.
#[must_use]
pub fn tag(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::Unreachable { .. } => "UNREACH",
        other => other.tag(),
    }
}

/// The first eight hex digits of a view hash.
#[must_use]
pub fn view_hash(hash: u64) -> String {
    format!("{hash:016x}")[..8].to_owned()
}

/// A node as the log names it: its label in its color, then its short id.
fn node(scene: &Scene<'_>, tag: &NodeTag) -> Vec<Span<'static>> {
    let look = scene.look;
    vec![
        Span::styled(
            tag.label.to_string(),
            look.node(tag.color).add_modifier(Modifier::BOLD),
        ),
        gap(1),
        look.span(tag.short.clone(), Token::Muted),
    ]
}

fn plain(scene: &Scene<'_>, content: impl Into<String>) -> Span<'static> {
    scene.look.span(content, Token::Text)
}

fn muted(scene: &Scene<'_>, content: impl Into<String>) -> Span<'static> {
    scene.look.span(content, Token::Muted)
}

/// A signed part count: `+13,107`, `−6,500`.
fn signed(delta: i64) -> String {
    let magnitude = text::thousands(delta.unsigned_abs());
    if delta < 0 {
        format!("−{magnitude}")
    } else {
        format!("+{magnitude}")
    }
}

/// A node, then `text` in the `token` style.
fn said(
    scene: &Scene<'_>,
    id: sundog::NodeId,
    text: impl Into<String>,
    token: Token,
) -> Vec<Span<'static>> {
    let mut spans = node(scene, &data::tag_of(scene.model, id));
    spans.push(scene.look.span(text, token));
    spans
}

/// A cache's mode in its short form, `Distributed` caches first.
fn listed_modes(
    caches: &std::collections::BTreeMap<smol_str::SmolStr, sundog::store::Mode>,
) -> String {
    let mut listed: Vec<_> = caches.iter().collect();
    listed.sort_by_key(|(_, mode)| !matches!(mode, sundog::store::Mode::Distributed { .. }));
    let mut text = String::new();
    for (name, mode) in listed {
        let _ = write!(text, " · {name} {}", data::mode_short(*mode));
    }
    text
}

/// The text of an event after its tag, as spans.
#[must_use]
pub fn body(scene: &Scene<'_>, kind: &EventKind) -> Vec<Span<'static>> {
    body_fit(scene, kind, usize::MAX)
}

/// The text of an event after its tag in `room` cells: a `VIEW` shortens its
/// per-node changes to fit, the other kinds ignore the room.
#[must_use]
pub fn body_fit(scene: &Scene<'_>, kind: &EventKind, room: usize) -> Vec<Span<'static>> {
    match kind {
        EventKind::Join { .. }
        | EventKind::Leave { .. }
        | EventKind::Left { .. }
        | EventKind::Down { .. }
        | EventKind::Up { .. }
        | EventKind::Rejoin { .. }
        | EventKind::Restart { .. } => lifecycle_body(scene, kind),
        EventKind::CacheAdded { .. }
        | EventKind::CacheRemoved { .. }
        | EventKind::Conflict { .. }
        | EventKind::Proto { .. } => cache_body(scene, kind),
        EventKind::View {
            cache,
            from,
            to,
            moved,
            deltas,
        } => view_body(scene, cache, *from, *to, *moved, deltas, room),
        EventKind::Settled { cache, took } => vec![
            Span::styled(
                cache.to_string(),
                scene.look.style(Token::Text).add_modifier(Modifier::BOLD),
            ),
            plain(scene, format!(" settled in {}", text::seconds(*took))),
        ],
        EventKind::Xfer { .. }
        | EventKind::Drop { .. }
        | EventKind::Ready { .. }
        | EventKind::Unready { .. }
        | EventKind::Unreachable { .. }
        | EventKind::Exporter { .. } => exporter_body(scene, kind),
    }
}

/// Joins, departures, drops, returns and restarts.
fn lifecycle_body(scene: &Scene<'_>, kind: &EventKind) -> Vec<Span<'static>> {
    match kind {
        EventKind::Join {
            node: id,
            addr,
            protocol,
            caches,
        } => said(
            scene,
            *id,
            format!(
                "  {} · protocol {protocol}{}",
                addr.ip(),
                listed_modes(caches)
            ),
            Token::Muted,
        ),
        EventKind::Leave {
            node: id,
            superseded,
            ..
        } => said(
            scene,
            *id,
            if *superseded {
                "  an earlier process announced departure"
            } else {
                "  announced departure; ownership moves now"
            },
            Token::Text,
        ),
        EventKind::Left {
            node: id,
            superseded,
            ..
        } => said(
            scene,
            *id,
            if *superseded {
                "  an earlier process is gone"
            } else {
                "  gone after its departure"
            },
            Token::Muted,
        ),
        EventKind::Down {
            node: id,
            exporter_silent,
            superseded,
            ..
        } => {
            let tail = match (superseded, exporter_silent) {
                (true, _) => "  an earlier process dropped".to_owned(),
                (false, Some(silent)) => format!(
                    "  no departure seen · exporter silent {} before",
                    text::seconds(*silent)
                ),
                (false, None) => "  no departure seen (crash, stall or partition)".to_owned(),
            };
            said(scene, *id, tail, Token::Text)
        }
        EventKind::Rejoin {
            node: id,
            addr,
            previous,
            caches,
        } => {
            let was = previous.to_string();
            said(
                scene,
                *id,
                format!(
                    "  new identity at {} (was {}…){}",
                    addr.ip(),
                    text::short_id(&was),
                    listed_modes(caches)
                ),
                Token::Text,
            )
        }
        EventKind::Restart { node: id, addr } => said(
            scene,
            *id,
            format!("  restarted at {}", addr.ip()),
            Token::Text,
        ),
        EventKind::Up { node: id, .. } => said(scene, *id, "  heartbeat resumed", Token::Text),
        _ => Vec::new(),
    }
}

/// Cache opens and closes, mode conflicts and protocol mismatches.
fn cache_body(scene: &Scene<'_>, kind: &EventKind) -> Vec<Span<'static>> {
    match kind {
        EventKind::CacheAdded {
            node: id,
            cache,
            mode,
        } => said(
            scene,
            *id,
            format!("  opened {cache} as {}", data::mode_short(*mode)),
            Token::Text,
        ),
        EventKind::CacheRemoved { node: id, cache } => {
            said(scene, *id, format!("  closed {cache}"), Token::Text)
        }
        EventKind::Conflict { cache, modes } => {
            let mut spans = vec![plain(scene, format!("{cache}  modes disagree: "))];
            for (index, (id, mode)) in modes.iter().enumerate() {
                if index > 0 {
                    spans.push(muted(scene, " · "));
                }
                let tag = data::tag_of(scene.model, *id);
                spans.push(scene.look.node_span(tag.label.to_string(), tag.color));
                spans.push(plain(scene, format!(" {}", data::mode_short(*mode))));
            }
            spans
        }
        EventKind::Proto { node: id, protocol } => said(
            scene,
            *id,
            format!("  protocol {protocol} (lens {PROTOCOL_VERSION})"),
            Token::Text,
        ),
        _ => Vec::new(),
    }
}

/// State transfers, dropped frames and the exporter's own events.
fn exporter_body(scene: &Scene<'_>, kind: &EventKind) -> Vec<Span<'static>> {
    match kind {
        EventKind::Xfer { node: id } => said(scene, *id, "  state transfer active", Token::Text),
        EventKind::Drop {
            node: id,
            peer,
            frames,
        } => {
            let mut spans = said(scene, *id, " → ", Token::Text);
            match data::tag_of_hex(scene.model, peer) {
                Some(tag) => spans.push(scene.look.node_span(tag.label.to_string(), tag.color)),
                None => spans.push(muted(scene, text::short_id(peer).to_owned())),
            }
            spans.push(plain(
                scene,
                format!(" · {} frames", text::thousands(*frames)),
            ));
            spans
        }
        EventKind::Ready { node: id } => said(scene, *id, "  ready", Token::Text),
        EventKind::Unready { node: id } => said(scene, *id, "  not ready", Token::Text),
        EventKind::Unreachable { node: id } => said(
            scene,
            *id,
            "  exporter not answering while gossip lists it live",
            Token::Text,
        ),
        EventKind::Exporter {
            node: id,
            addr,
            detail,
        } => {
            let tag = data::tag_of(scene.model, *id);
            let mut spans = if tag.label == data::UNKNOWN_LABEL {
                vec![muted(scene, addr.to_string())]
            } else {
                node(scene, &tag)
            };
            spans.push(muted(scene, format!("  {detail}")));
            spans
        }
        _ => Vec::new(),
    }
}

/// A signed part count in the compact form: `+21.9k`, `−43.6k`.
fn signed_compact(delta: i64) -> String {
    let magnitude = text::count(crate::model::count_to_f64(
        usize::try_from(delta.unsigned_abs()).unwrap_or(usize::MAX),
    ));
    if delta < 0 {
        format!("−{magnitude}")
    } else {
        format!("+{magnitude}")
    }
}

/// How a `VIEW` row is written: whether it keeps the percentage and the
/// compact per-node changes, and how many of those it lists.
#[derive(Debug, Clone, Copy)]
struct ViewShape {
    percent: bool,
    compact: bool,
    nodes: usize,
}

/// The shapes of a `VIEW` row from the fullest to the barest: first the
/// per-node changes shorten, then the percentage goes, then whole changes
/// drop from the end. No shape cuts inside a number.
const VIEW_SHAPES: [ViewShape; 6] = [
    ViewShape {
        percent: true,
        compact: false,
        nodes: 3,
    },
    ViewShape {
        percent: true,
        compact: true,
        nodes: 3,
    },
    ViewShape {
        percent: false,
        compact: true,
        nodes: 3,
    },
    ViewShape {
        percent: false,
        compact: true,
        nodes: 2,
    },
    ViewShape {
        percent: false,
        compact: true,
        nodes: 1,
    },
    ViewShape {
        percent: false,
        compact: true,
        nodes: 0,
    },
];

fn view_body(
    scene: &Scene<'_>,
    cache: &str,
    from: Option<u64>,
    to: u64,
    moved: usize,
    deltas: &[(sundog::NodeId, i64)],
    room: usize,
) -> Vec<Span<'static>> {
    let mut chosen = Vec::new();
    for shape in VIEW_SHAPES {
        chosen = view_spans(scene, cache, from, to, moved, deltas, shape);
        if super::panel::width_of(&chosen) <= room {
            break;
        }
    }
    chosen
}

fn view_spans(
    scene: &Scene<'_>,
    cache: &str,
    from: Option<u64>,
    to: u64,
    moved: usize,
    deltas: &[(sundog::NodeId, i64)],
    shape: ViewShape,
) -> Vec<Span<'static>> {
    let model = scene.model;
    let look = scene.look;
    let mut spans = vec![Span::styled(
        cache.to_owned(),
        look.style(Token::Text).add_modifier(Modifier::BOLD),
    )];
    match from {
        Some(from) if from == to => spans.push(plain(scene, " owner count changed")),
        Some(from) => spans.push(plain(
            scene,
            format!(" {} → {}", view_hash(from), view_hash(to)),
        )),
        None => spans.push(plain(scene, format!(" view {}", view_hash(to)))),
    }
    if moved > 0 {
        let mut text = format!(
            " · {} parts move",
            text::thousands(u64::try_from(moved).unwrap_or(u64::MAX))
        );
        if shape.percent {
            let slots = model.ownership(cache).map_or(PART_SPACE, |digest| {
                PART_SPACE * usize::from(digest.k.get()).min(digest.eligible.len().max(1))
            });
            let fraction = crate::model::count_to_f64(moved) / crate::model::count_to_f64(slots);
            let _ = write!(text, " ({})", text::percent(fraction, 1));
        }
        spans.push(plain(scene, text));
    }
    let mut ranked: Vec<_> = deltas.iter().filter(|(_, delta)| *delta != 0).collect();
    ranked.sort_by_key(|(_, delta)| std::cmp::Reverse(delta.unsigned_abs()));
    for (id, delta) in ranked.into_iter().take(shape.nodes) {
        let tag = data::tag_of(model, *id);
        spans.push(muted(scene, " · "));
        spans.push(look.node_span(tag.label.to_string(), tag.color));
        let change = if shape.compact {
            signed_compact(*delta)
        } else {
            signed(*delta)
        };
        spans.push(plain(scene, format!(" {change}")));
    }
    spans
}

/// How strongly the row of `event` flashes at the frame's time, 0 to 1.
#[must_use]
pub fn flash(scene: &Scene<'_>, event: &Event) -> f64 {
    if !scene.app.anim {
        return 0.0;
    }
    let age = scene.ctx.wall.duration_since(event.at).unwrap_or_default();
    anim::pulse(age, EVENT_FLASH)
}

/// The row of one event: time, glyph, tag and text. A fresh event's row
/// flashes its color as a background.
#[must_use]
pub fn line(scene: &Scene<'_>, event: &Event) -> Line<'static> {
    line_fit(scene, event, usize::MAX)
}

/// [`line()`] in `width` cells: a `VIEW` shortens its per-node changes to fit.
#[must_use]
pub fn line_fit(scene: &Scene<'_>, event: &Event, width: usize) -> Line<'static> {
    let look = scene.look;
    let (mark, token) = glyph(&event.kind);
    let mut spans = vec![
        look.span(text::clock(event.at), Token::Muted),
        gap(2),
        look.span(mark.to_string(), token),
        gap(1),
        Span::styled(
            text::pad_right(tag(&event.kind), TAG_WIDTH),
            look.style(token).add_modifier(Modifier::BOLD),
        ),
        gap(1),
    ];
    let room = width.saturating_sub(super::panel::width_of(&spans));
    spans.extend(body_fit(scene, &event.kind, room));
    let mut row = Line::from(spans);
    let intensity = flash(scene, event);
    if intensity > 0.0 {
        let color: Rgb = anim::blend(theme::BG, token.rgb(), 0.30 * intensity);
        row = row.style(look.bg(color));
    }
    row
}

/// Draws `events` (newest first) into `area`, one row each, starting
/// `offset` events in. The rows keep a cell of padding on each side.
pub fn render(scene: &Scene<'_>, events: &[&Event], offset: usize, area: Rect, buf: &mut Buffer) {
    let area = super::panel::padded(area);
    for (row, event) in events
        .iter()
        .skip(offset)
        .take(usize::from(area.height))
        .enumerate()
    {
        let Ok(dy) = u16::try_from(row) else { break };
        let mut shown = line_fit(scene, event, usize::from(area.width));
        shown.spans = super::panel::clip(shown.spans, usize::from(area.width));
        shown.render(Rect::new(area.x, area.y + dy, area.width, 1), buf);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::app::{App, AppConfig};
    use crate::model::Model;
    use crate::model::events::Filter;
    use crate::model::testkit;
    use crate::ui::{Ctx, LayoutKind};

    fn ctx(model: &Model) -> Ctx {
        Ctx {
            now: model.now().unwrap(),
            wall: model.wall().unwrap(),
            elapsed: Duration::ZERO,
        }
    }

    fn with_scene<R>(model: &Model, f: impl FnOnce(&Scene<'_>) -> R) -> R {
        let app = App::new(AppConfig::default());
        let ctx = ctx(model);
        let scene = Scene {
            app: &app,
            model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        f(&scene)
    }

    fn text_of(spans: &[Span<'_>]) -> String {
        spans.iter().map(|s| s.content.as_ref()).collect()
    }

    fn row(model: &Model, tag: &str) -> String {
        with_scene(model, |scene| {
            let event = model
                .events()
                .newest_first(Filter::All)
                .find(|event| event.kind.tag() == tag)
                .unwrap_or_else(|| panic!("no {tag} event"));
            text_of(&line(scene, event).spans)
        })
    }

    #[test]
    fn every_kind_has_a_glyph_and_a_tag_that_fits_the_column() {
        let model = testkit::fixture_model(Instant::now());
        for event in model.events().iter() {
            let (mark, _) = glyph(&event.kind);
            assert!(theme::is_allowed(mark), "{mark}");
            assert!(tag(&event.kind).chars().count() <= TAG_WIDTH);
        }
        assert_eq!(
            tag(&EventKind::Unreachable {
                node: sundog::NodeId::from(1)
            }),
            "UNREACH"
        );
    }

    #[test]
    fn a_healthy_exporter_row_is_a_check_and_a_failing_one_a_warning() {
        let exporter = |detail: &str| EventKind::Exporter {
            node: sundog::NodeId::from(1),
            addr: testkit::gossip_addr(1),
            detail: detail.to_owned(),
        };
        assert_eq!(glyph(&exporter("answering")), ('✓', Token::Ok));
        assert_eq!(glyph(&exporter("answering again")), ('✓', Token::Ok));
        assert_eq!(
            glyph(&exporter("not answering: timed out")),
            ('⚠', Token::Muted)
        );
        assert_eq!(
            glyph(&exporter("not scraped: two members map to one URL")),
            ('⚠', Token::Muted)
        );
    }

    #[test]
    fn a_rejoin_row_lists_the_caches_the_node_advertises() {
        let model = testkit::fixture_model(Instant::now());
        let scene_app = App::new(AppConfig::default());
        let ctx = Ctx {
            now: Instant::now(),
            wall: std::time::SystemTime::UNIX_EPOCH,
            elapsed: std::time::Duration::ZERO,
        };
        let scene = Scene {
            app: &scene_app,
            model: &model,
            ctx: &ctx,
            look: scene_app.look(),
            kind: LayoutKind::Full,
        };
        let kind = EventKind::Rejoin {
            node: testkit::node_id(2, 1),
            addr: testkit::gossip_addr(2),
            previous: testkit::node_id(2, 0),
            caches: [
                ("os".into(), sundog::store::Mode::Replicated),
                ("it".into(), testkit::distributed(2)),
            ]
            .into(),
        };
        let shown = text_of(&body(&scene, &kind));
        assert!(shown.contains("new identity at 127.0.0.12"), "{shown}");
        assert!(shown.ends_with(" · it D2 · os R"), "{shown}");
    }

    #[test]
    fn an_exporter_row_keeps_the_identity_it_was_raised_for_after_the_address_rejoins() {
        use sundog::observe::{ClusterSnapshot, MemberStatus};

        use crate::source::{ScrapeReport, Update};

        let at = Instant::now();
        let wall = std::time::SystemTime::UNIX_EPOCH;
        let mut model = Model::new();
        let old = testkit::member_at(2, 0, 1, MemberStatus::Live);
        let (old_node, addr) = (old.peer.node, old.peer.gossip_addr);
        let snapshot = |members| {
            Update::Snapshot(
                std::sync::Arc::new(ClusterSnapshot::new("c", members, 0)),
                at,
            )
        };
        model.apply(snapshot(vec![old.clone()]), at, wall);
        model.apply(
            Update::Scrape(ScrapeReport {
                addr,
                node: old_node,
                at,
                outcome: Ok(Vec::new()),
                ready: None,
            }),
            at,
            wall,
        );
        // The observer has found the cluster when the process restarts under
        // another id at the same address.
        let later = at + crate::model::DISCOVERY_QUIET;
        model.tick(later);
        let mut new = testkit::member_at(2, 0, 2, MemberStatus::Live);
        new.peer.node = testkit::node_id(5, 0);
        let mut gone = old;
        gone.status = MemberStatus::Down;
        model.apply(snapshot(vec![gone, new.clone()]), later, wall);
        let old_short = text::short_id(&old_node.to_string()).to_owned();
        let new_short = text::short_id(&new.peer.node.to_string()).to_owned();
        assert_ne!(old_short, new_short);
        let exporter = row(&model, "EXPORTER");
        assert!(exporter.contains(&old_short), "{exporter}");
        assert!(!exporter.contains(&new_short), "{exporter}");
        let join = row(&model, "REJOIN");
        assert!(join.contains(&new_short), "{join}");
    }

    #[test]
    fn a_view_row_that_does_not_fit_shortens_its_changes_and_never_cuts_a_number() {
        let model = testkit::fixture_model(Instant::now());
        let (n1, n2, n3) = (
            testkit::node_id(1, 0),
            testkit::node_id(2, 0),
            testkit::node_id(3, 0),
        );
        let kind = EventKind::View {
            cache: "it".into(),
            from: Some(0x1111_1111_0000_0000),
            to: 0x2222_2222_0000_0000,
            moved: 43_650,
            deltas: vec![(n2, -43_650), (n3, 21_825), (n1, 21_825)],
        };
        with_scene(&model, |scene| {
            let width = |room: usize| text_of(&body_fit(scene, &kind, room));
            let full = width(usize::MAX);
            assert!(
                full.starts_with("it 11111111 → 22222222 · 43,650 parts move (33.3%) · n2 −43,650"),
                "{full}"
            );
            assert!(full.ends_with("n1 +21,825"), "{full}");
            assert_eq!(full, text_of(&body(scene, &kind)));
            // One cell short: the changes go compact, the percentage stays.
            let compact = width(full.chars().count() - 1);
            assert!(compact.contains("(33.3%)"), "{compact}");
            assert!(
                compact.contains("n2 −43.6k") || compact.contains("n2 −43.7k"),
                "{compact}"
            );
            assert!(
                compact.contains("n3 +21.8k") || compact.contains("n3 +21.9k"),
                "{compact}"
            );
            // Then the percentage goes, then changes drop from the end.
            let mut kept = 3;
            for room in (0..=compact.chars().count()).rev() {
                let shown = width(room);
                let segments: Vec<_> = shown.split(" · ").collect();
                let changes = segments.len().saturating_sub(2);
                assert!(changes <= kept, "{room}: {shown}");
                kept = changes;
                for change in &segments[segments.len().min(2)..] {
                    let (node, number) = change.split_once(' ').unwrap();
                    assert!(["n1", "n2", "n3"].contains(&node), "{shown}");
                    assert!(
                        number.starts_with(['+', '−'])
                            && number[number.char_indices().nth(1).unwrap().0..]
                                .trim_end_matches('k')
                                .chars()
                                .all(|c| c.is_ascii_digit() || c == '.'),
                        "a whole number in {shown}"
                    );
                }
                if room >= 60 {
                    assert!(shown.chars().count() <= room, "{room}: {shown}");
                }
            }
            let bare = width(0);
            assert_eq!(bare, "it 11111111 → 22222222 · 43,650 parts move");
            let two = width(70);
            assert!(
                !two.contains('%') && two.matches(" · n").count() >= 1,
                "{two}"
            );
        });
    }

    #[test]
    fn view_hashes_show_eight_digits() {
        assert_eq!(view_hash(0x3f9a_2c1e_0000_0001), "3f9a2c1e");
        assert_eq!(view_hash(1), "00000000");
    }

    #[test]
    fn signed_counts_use_a_true_minus() {
        assert_eq!(signed(13_107), "+13,107");
        assert_eq!(signed(-6_500), "−6,500");
        assert_eq!(signed(0), "+0");
    }

    #[test]
    fn a_join_row_names_the_node_address_protocol_and_caches() {
        let model = testkit::fixture_model(Instant::now());
        let row = row(&model, "JOIN");
        assert!(row.contains("JOIN"), "{row}");
        assert!(row.contains("127.0.0.1"), "{row}");
        assert!(row.contains("· protocol 6"), "{row}");
        assert!(row.contains("it D2"), "{row}");
        assert!(row.contains("churn R"), "{row}");
        assert!(row.starts_with("00:00:"), "{row}");
        assert!(row.contains('✚'));
    }

    #[test]
    fn membership_rows_say_what_happened() {
        let model = testkit::fixture_model(Instant::now());
        assert!(row(&model, "LEAVE").contains("announced departure; ownership moves now"));
        assert!(row(&model, "LEFT").contains("gone after its departure"));
        assert!(row(&model, "DOWN").contains("no departure seen (crash, stall or partition)"));
    }

    #[test]
    fn a_view_row_shows_the_hashes_the_moved_parts_and_the_biggest_changes() {
        let model = testkit::fixture_model(Instant::now());
        let latest = row(&model, "VIEW");
        assert!(latest.contains("it "), "{latest}");
        assert!(latest.contains(" → "), "{latest}");
        assert!(latest.contains("parts move ("), "{latest}");
        assert!(latest.contains('%'), "{latest}");
        assert!(latest.contains(" +") || latest.contains(" −"), "{latest}");
        // A cache whose ownership was dropped and comes back has a first
        // view with no predecessor.
        let (mut model, now) = testkit::past_discovery(Instant::now());
        let digest = testkit::ownership_digest(&testkit::snapshot(3), "it").unwrap();
        let events = model.apply(
            crate::source::Update::Ownership(digest),
            now,
            std::time::SystemTime::UNIX_EPOCH,
        );
        let first = events
            .iter()
            .find(|event| matches!(event.kind, EventKind::View { from: None, .. }))
            .expect("the first view has no predecessor");
        with_scene(&model, |scene| {
            let text = text_of(&body(scene, &first.kind));
            assert!(text.starts_with("it view "), "{text}");
            assert!(!text.contains("parts move"), "{text}");
        });
    }

    #[test]
    fn a_settled_row_reports_the_time_it_took() {
        let model = testkit::fixture_model(Instant::now());
        let settled = row(&model, "SETTLED");
        assert!(settled.contains("it settled in "), "{settled}");
        assert!(settled.contains(" s"), "{settled}");
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one entry for each event kind")]
    fn the_remaining_kinds_render_without_panicking() {
        let model = testkit::fixture_model(Instant::now());
        let node = testkit::node_id(2, 0);
        let other = testkit::node_id(3, 0);
        let addr = testkit::gossip_addr(2);
        let kinds = [
            EventKind::Up { node, addr },
            EventKind::Rejoin {
                node,
                addr,
                previous: other,
                caches: std::collections::BTreeMap::new(),
            },
            EventKind::Restart { node, addr },
            EventKind::CacheAdded {
                node,
                cache: "x".into(),
                mode: testkit::distributed(3),
            },
            EventKind::CacheRemoved {
                node,
                cache: "x".into(),
            },
            EventKind::Conflict {
                cache: "x".into(),
                modes: vec![
                    (node, sundog::store::Mode::Replicated),
                    (other, testkit::distributed(2)),
                ],
            },
            EventKind::Proto { node, protocol: 5 },
            EventKind::Xfer { node },
            EventKind::Drop {
                node,
                peer: other.to_string().into(),
                frames: 312,
            },
            EventKind::Drop {
                node,
                peer: "ffffffffffffffff".into(),
                frames: 1,
            },
            EventKind::Ready { node },
            EventKind::Unready { node },
            EventKind::Unreachable { node },
            EventKind::Exporter {
                node,
                addr,
                detail: "scrape up".into(),
            },
            EventKind::Exporter {
                node: sundog::NodeId::from(40),
                addr: testkit::gossip_addr(40),
                detail: "scrape up".into(),
            },
            EventKind::Leave {
                node,
                addr,
                superseded: true,
            },
            EventKind::Left {
                node,
                addr,
                superseded: true,
            },
            EventKind::Down {
                node,
                addr,
                exporter_silent: Some(Duration::from_millis(3900)),
                superseded: false,
            },
            EventKind::View {
                cache: "it".into(),
                from: Some(5),
                to: 5,
                moved: 10,
                deltas: vec![(node, -500), (other, 500)],
            },
        ];
        with_scene(&model, |scene| {
            for kind in &kinds {
                let text = text_of(&body(scene, kind));
                assert!(!text.is_empty(), "{kind:?}");
            }
            let drop = text_of(&body(scene, &kinds[8]));
            assert!(
                drop.contains(" → n3") && drop.contains("312 frames"),
                "{drop}"
            );
            let unknown = text_of(&body(scene, &kinds[9]));
            assert!(unknown.contains("ffff"), "{unknown}");
            let down = text_of(&body(scene, &kinds[17]));
            assert!(
                down.contains("no departure seen · exporter silent 3.9 s before"),
                "{down}"
            );
            let owners = text_of(&body(scene, &kinds[18]));
            assert!(owners.contains("owner count changed"), "{owners}");
            assert!(owners.contains("−500"), "{owners}");
            let conflict = text_of(&body(scene, &kinds[5]));
            assert!(conflict.contains("n2 R · n3 D2"), "{conflict}");
        });
    }

    #[test]
    fn a_fresh_event_flashes_its_color_and_an_old_one_does_not() {
        let model = testkit::fixture_model(Instant::now());
        let event = model.events().iter().last().unwrap().clone();
        with_scene(&model, |scene| {
            let at = event.at;
            let mut fresh = *scene.ctx;
            fresh.wall = at;
            let scene_now = Scene {
                ctx: &fresh,
                ..*scene
            };
            assert!((flash(&scene_now, &event) - 1.0).abs() < 1e-9);
            assert!(line(&scene_now, &event).style.bg.is_some());
            let mut old = *scene.ctx;
            old.wall = at + Duration::from_secs(5);
            let scene_old = Scene {
                ctx: &old,
                ..*scene
            };
            assert!(flash(&scene_old, &event).abs() < 1e-9);
            assert!(line(&scene_old, &event).style.bg.is_none());
        });
    }

    #[test]
    fn no_animation_means_no_flash() {
        let model = testkit::fixture_model(Instant::now());
        let app = App::new(AppConfig {
            anim: false,
            ..AppConfig::default()
        });
        let mut ctx = ctx(&model);
        let event = model.events().iter().last().unwrap().clone();
        ctx.wall = event.at;
        let scene = Scene {
            app: &app,
            model: &model,
            ctx: &ctx,
            look: app.look(),
            kind: LayoutKind::Full,
        };
        assert!(flash(&scene, &event).abs() < 1e-9);
    }

    #[test]
    fn rows_render_newest_first_from_an_offset_and_clip_to_the_area() {
        let model = testkit::fixture_model(Instant::now());
        let events: Vec<&Event> = model.events().newest_first(Filter::All).collect();
        assert!(events.len() > 4);
        with_scene(&model, |scene| {
            let area = Rect::new(0, 0, 100, 3);
            let mut buf = Buffer::empty(area);
            render(scene, &events, 1, area, &mut buf);
            let second = crate::ui::panel::row_text(&buf, 0);
            let direct = text_of(&line_fit(scene, events[1], 98).spans);
            assert!(second.starts_with(' '), "{second:?}");
            let stem = second
                .trim_start()
                .strip_suffix('…')
                .unwrap_or(second.trim_start());
            assert!(
                direct.trim_end().starts_with(stem),
                "{second:?} vs {direct:?}"
            );
            assert!(second.chars().count() <= 100);
            let text = crate::ui::panel::row_text(&buf, 2);
            assert!(!text.is_empty(), "{text:?}");
        });
    }
}
