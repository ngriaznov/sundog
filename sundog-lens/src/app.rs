//! The interface state: the selected view, member and cache, and key
//! handling.
//!
//! [`App::handle_key`] is pure over the state and the model: it changes the
//! state and names what the loop does next. The views read the state and draw;
//! they never change it. Motion lives here too: the share-bar tweens and the
//! mosaic flash that follow a view change.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use smol_str::SmolStr;
use sundog::NodeId;

use crate::model::Model;
use crate::model::events::Filter;
use crate::model::ownership::{BUCKETS, NO_LEAD, OwnershipDigest};
use crate::ui::View;
use crate::ui::anim::{self, Tween};
use crate::ui::data;
use crate::ui::look::Look;
use crate::ui::timeline;

/// How long the mosaic flashes the buckets whose lead moved.
pub const MOSAIC_FLASH: Duration = Duration::from_millis(1200);

/// How long a new event row flashes its color.
pub const EVENT_FLASH: Duration = Duration::from_millis(1500);

/// How long a joined row pulses in its node color.
pub const JOIN_PULSE: Duration = Duration::from_secs(2);

/// How long a down row stays red before it fades to a ghost.
pub const DOWN_HOLD: Duration = Duration::from_secs(8);

/// What the loop does after a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Nothing changed.
    None,
    /// The state changed: draw again.
    Redraw,
    /// Leave the interface.
    Quit,
    /// Ask the fleet to act (demo only).
    Fleet(FleetCmd),
}

/// What the demo keys ask the fleet to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetCmd {
    /// Start the next slot.
    Spawn,
    /// SIGKILL the node with this label.
    Kill(SmolStr),
    /// SIGTERM the node with this label: a graceful leave.
    Leave(SmolStr),
    /// Start the node with this label again at its address.
    Restart(SmolStr),
}

/// What the scenario director asks of the interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UiCommand {
    /// Show a view.
    Tab(View),
    /// Select the node with this label.
    Select(SmolStr),
    /// Select the cache with this name.
    Cache(SmolStr),
    /// Show or hide the help overlay.
    Help(bool),
    /// Show a caption, or clear it.
    Caption(Option<String>),
    /// End the interface: the scenario is over.
    Quit,
}

/// What an [`App`] is set up with.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// How the views style text.
    pub look: Look,
    /// How long a gone node keeps its row.
    pub forget_after: Duration,
    /// Whether motion starts on.
    pub anim: bool,
    /// Whether the demo keys and the caption line exist.
    pub demo: bool,
    /// Whether the demo shows captions.
    pub captions: bool,
    /// The scrape interval, when metrics are scraped.
    pub scrape_interval: Option<Duration>,
    /// The cluster name the splash waits for.
    pub cluster: String,
    /// The seeds the splash names.
    pub seeds: Vec<String>,
    /// The observer's gossip address, once known.
    pub observer: Option<SocketAddr>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            look: Look::default(),
            forget_after: Duration::from_secs(90),
            anim: true,
            demo: false,
            captions: true,
            scrape_interval: None,
            cluster: String::new(),
            seeds: Vec::new(),
            observer: None,
        }
    }
}

/// The buckets whose lead changed at the last view change.
#[derive(Debug, Clone)]
struct MosaicFlash {
    /// The previous leads, as indices into the new eligible list;
    /// [`NO_LEAD`] for a node that is gone.
    prev: Box<[u8; BUCKETS]>,
    /// When the view changed.
    since: Instant,
}

/// The interface's motion state.
#[derive(Debug, Clone, Default)]
struct Motion {
    shares: BTreeMap<(SmolStr, NodeId), Tween>,
    flashes: BTreeMap<SmolStr, MosaicFlash>,
    previous: BTreeMap<SmolStr, OwnershipDigest>,
}

/// The interface state.
#[derive(Debug, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "help, raw, gone rows, motion and the log pin are independent toggles the user flips with one key each"
)]
pub struct App {
    /// The view on screen.
    pub view: View,
    /// The event filter.
    pub filter: Filter,
    /// Whether the help overlay is open.
    pub help: bool,
    /// Whether rows that went long ago show too.
    pub show_gone: bool,
    /// Whether motion is on.
    pub anim: bool,
    /// The demo caption, if any.
    pub caption: Option<String>,
    /// Whether the Node view lists the raw samples.
    pub raw: bool,
    prev_view: Option<View>,
    selected: Option<SocketAddr>,
    cache: Option<SmolStr>,
    frozen: Option<Model>,
    pinned: bool,
    scroll: usize,
    pin_len: usize,
    config: AppConfig,
    motion: Motion,
    lifeline_window: Cell<Duration>,
}

/// `lead` indices of `prev` translated into the eligible order of `next`: a
/// bucket led by a node that `next` also ranks keeps that node's new index;
/// one led by a node that is gone becomes [`NO_LEAD`]. Comparing the result
/// with `next.lead` finds the buckets whose lead moved.
#[must_use]
pub fn remap_lead(prev: &OwnershipDigest, next: &OwnershipDigest) -> Box<[u8; BUCKETS]> {
    let mut remapped = Box::new([NO_LEAD; BUCKETS]);
    for (slot, &lead) in remapped.iter_mut().zip(prev.lead.iter()) {
        *slot = prev
            .eligible
            .get(usize::from(lead))
            .filter(|_| lead != NO_LEAD)
            .and_then(|&node| next.position(node))
            .and_then(|index| u8::try_from(index).ok())
            .unwrap_or(NO_LEAD);
    }
    remapped
}

impl App {
    /// A fresh interface on the Overview.
    #[must_use]
    pub fn new(config: AppConfig) -> Self {
        Self {
            view: View::Overview,
            filter: Filter::All,
            help: false,
            show_gone: false,
            anim: config.anim,
            caption: None,
            raw: false,
            prev_view: None,
            selected: None,
            cache: None,
            frozen: None,
            pinned: false,
            scroll: 0,
            pin_len: 0,
            config,
            motion: Motion::default(),
            lifeline_window: Cell::new(Duration::ZERO),
        }
    }

    /// The configuration the interface was set up with.
    #[must_use]
    pub const fn config(&self) -> &AppConfig {
        &self.config
    }

    /// The time the Timeline's lifelines cover when the oldest is `span` old.
    /// The window grows with the history and holds while the view stays
    /// open; leaving the view resets it.
    pub fn lifeline_window(&self, span: Duration) -> Duration {
        let window = timeline::window_for(span, self.lifeline_window.get());
        self.lifeline_window.set(window);
        window
    }

    /// Records the observer's gossip address, once the feed is up.
    pub fn set_observer(&mut self, addr: SocketAddr) {
        self.config.observer = Some(addr);
    }

    /// How the views style text.
    #[must_use]
    pub const fn look(&self) -> Look {
        self.config.look
    }

    /// Whether the display is frozen.
    #[must_use]
    pub const fn is_frozen(&self) -> bool {
        self.frozen.is_some()
    }

    /// The model to draw: the frozen copy while frozen, else `live`.
    #[must_use]
    pub fn shown<'a>(&'a self, live: &'a Model) -> &'a Model {
        self.frozen.as_ref().unwrap_or(live)
    }

    /// Whether the event log is pinned (scrolled off the live end).
    #[must_use]
    pub const fn is_pinned(&self) -> bool {
        self.pinned
    }

    /// How many events from the newest the log view starts at, given that the
    /// log now shows `len` events (those that pass the filter). 0 while the log follows the live end.
    #[must_use]
    pub fn log_offset(&self, len: usize) -> usize {
        if !self.pinned {
            return 0;
        }
        let grown = len.saturating_sub(self.pin_len);
        (self.scroll + grown).min(len.saturating_sub(1))
    }

    /// The selected node's gossip address: the one chosen, else the first
    /// visible row's.
    #[must_use]
    pub fn selected_addr(&self, live: &Model, wall: SystemTime) -> Option<SocketAddr> {
        let rows = self.rows(live, wall);
        self.selected
            .filter(|addr| rows.iter().any(|row| row.member.peer.gossip_addr == *addr))
            .or_else(|| rows.first().map(|row| row.member.peer.gossip_addr))
    }

    /// The selected cache's name: the one chosen, else the first listed.
    #[must_use]
    pub fn selected_cache(&self, live: &Model) -> Option<SmolStr> {
        let caches = data::cache_rows(self.shown(live));
        self.cache
            .as_ref()
            .filter(|name| caches.iter().any(|row| row.name == **name))
            .or_else(|| caches.first().map(|row| &row.name))
            .cloned()
    }

    /// The `Distributed` cache the Ownership panel and the SHARE column show.
    #[must_use]
    pub fn ownership_cache(&self, live: &Model) -> Option<SmolStr> {
        data::ownership_cache(self.shown(live), self.cache.as_deref())
    }

    /// The node rows the views show.
    #[must_use]
    pub fn rows<'a>(&'a self, live: &'a Model, wall: SystemTime) -> Vec<data::NodeRow<'a>> {
        data::node_rows(
            self.shown(live),
            wall,
            self.show_gone,
            self.config.forget_after,
        )
    }

    fn wall_of(&self, live: &Model) -> SystemTime {
        self.shown(live).wall().unwrap_or(SystemTime::UNIX_EPOCH)
    }

    fn go(&mut self, view: View) {
        if self.view != view {
            self.prev_view = Some(self.view);
            self.view = view;
            self.lifeline_window.set(Duration::ZERO);
        }
    }

    fn select_row(&mut self, live: &Model, step: Step) {
        let wall = self.wall_of(live);
        let addrs: Vec<SocketAddr> = self
            .rows(live, wall)
            .iter()
            .map(|row| row.member.peer.gossip_addr)
            .collect();
        let current = self
            .selected_addr(live, wall)
            .and_then(|addr| addrs.iter().position(|a| *a == addr));
        self.selected = step.apply(current, addrs.len()).map(|index| addrs[index]);
    }

    fn select_cache(&mut self, live: &Model, step: Step) {
        let names: Vec<SmolStr> = data::cache_rows(self.shown(live))
            .into_iter()
            .map(|row| row.name)
            .collect();
        let current = self
            .selected_cache(live)
            .and_then(|name| names.iter().position(|n| *n == name));
        self.cache = step.apply(current, names.len()).map(|i| names[i].clone());
    }

    fn scroll_log(&mut self, live: &Model, step: Step) {
        let len = self.shown(live).events().newest_first(self.filter).count();
        if !self.pinned {
            self.pinned = true;
            self.pin_len = len;
            self.scroll = 0;
        }
        let current = self.log_offset(len);
        let target = match step {
            Step::Prev => current + 1,
            Step::Next => current.saturating_sub(1),
            Step::First => len.saturating_sub(1),
            Step::Last => 0,
        };
        // At the live end the log follows new events again.
        self.pinned = target > 0;
        self.pin_len = len;
        self.scroll = target.min(len.saturating_sub(1));
    }

    fn cycle_ownership_cache(&mut self, live: &Model, forward: bool) {
        let caches = data::distributed_caches(self.shown(live));
        if caches.is_empty() {
            return;
        }
        let current = self
            .ownership_cache(live)
            .and_then(|name| caches.iter().position(|n| *n == name))
            .unwrap_or(0);
        let next = if forward {
            (current + 1) % caches.len()
        } else {
            (current + caches.len() - 1) % caches.len()
        };
        self.cache = Some(caches[next].clone());
    }

    fn fleet_target(&self, live: &Model, make: impl FnOnce(SmolStr) -> FleetCmd) -> Action {
        let wall = self.wall_of(live);
        let shown = self.shown(live);
        let label = self
            .selected_addr(live, wall)
            .and_then(|addr| data::row_at(shown, addr))
            .map(|row| row.slot.label.clone());
        label.map_or(Action::None, |label| Action::Fleet(make(label)))
    }

    /// Handles one key press and returns what the loop does next.
    ///
    /// Keys follow the table in the help overlay. `Esc` closes the help
    /// overlay, else the raw-sample list, else returns to the previous view;
    /// it never quits. While help is open every key but `?`, `Esc`, `q` and
    /// `Ctrl-C` is ignored. The demo keys `S K L R` act only in demo mode.
    pub fn handle_key(&mut self, key: KeyEvent, live: &Model) -> Action {
        if key.kind == KeyEventKind::Release {
            return Action::None;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('c') if ctrl => return Action::Quit,
            KeyCode::Char('q') => return Action::Quit,
            _ => {}
        }
        if self.help {
            return match key.code {
                KeyCode::Char('?') | KeyCode::Esc => {
                    self.help = false;
                    Action::Redraw
                }
                _ => Action::None,
            };
        }
        match key.code {
            KeyCode::Char('1') => self.go(View::Overview),
            KeyCode::Char('2') => self.go(View::Caches),
            KeyCode::Char('3') => self.go(View::Node),
            KeyCode::Char('4') => self.go(View::Timeline),
            KeyCode::Tab => self.go(self.view.next()),
            KeyCode::BackTab => self.go(self.view.prev()),
            KeyCode::Up | KeyCode::Char('k') => return self.move_selection(live, Step::Prev),
            KeyCode::Down | KeyCode::Char('j') => return self.move_selection(live, Step::Next),
            KeyCode::Char('g') => return self.move_selection(live, Step::First),
            KeyCode::Char('G') => return self.move_selection(live, Step::Last),
            KeyCode::Enter => return self.enter(live),
            KeyCode::Esc => return self.escape(),
            KeyCode::Char('c') => self.cycle_ownership_cache(live, true),
            KeyCode::Char('C') => self.cycle_ownership_cache(live, false),
            KeyCode::Char('f') => {
                self.filter = self.filter.next();
                // A scroll offset in one filter means nothing in another.
                self.pinned = false;
                self.scroll = 0;
            }
            KeyCode::Char('t') => self.show_gone = !self.show_gone,
            KeyCode::Char('r') if self.view == View::Node => self.raw = !self.raw,
            KeyCode::Char('p') => {
                self.frozen = match self.frozen {
                    Some(_) => None,
                    None => Some(live.clone()),
                };
            }
            KeyCode::Char('a') => {
                self.anim = !self.anim;
                if !self.anim {
                    self.snap();
                }
            }
            KeyCode::Char('?') => self.help = true,
            KeyCode::Char('S') if self.config.demo => return Action::Fleet(FleetCmd::Spawn),
            KeyCode::Char('K') if self.config.demo => {
                return self.fleet_target(live, FleetCmd::Kill);
            }
            KeyCode::Char('L') if self.config.demo => {
                return self.fleet_target(live, FleetCmd::Leave);
            }
            KeyCode::Char('R') if self.config.demo => {
                return self.fleet_target(live, FleetCmd::Restart);
            }
            _ => return Action::None,
        }
        Action::Redraw
    }

    fn move_selection(&mut self, live: &Model, step: Step) -> Action {
        match self.view {
            View::Caches => self.select_cache(live, step),
            View::Timeline => self.scroll_log(live, step),
            View::Overview | View::Node => self.select_row(live, step),
        }
        Action::Redraw
    }

    fn enter(&mut self, live: &Model) -> Action {
        match self.view {
            View::Overview | View::Timeline => {
                let wall = self.wall_of(live);
                if let Some(addr) = self.selected_addr(live, wall) {
                    self.selected = Some(addr);
                    self.go(View::Node);
                    return Action::Redraw;
                }
                Action::None
            }
            // The Caches view always shows the selected cache's detail, and
            // the Node view is already the detail.
            View::Caches | View::Node => Action::None,
        }
    }

    fn escape(&mut self) -> Action {
        if self.help {
            self.help = false;
        } else if self.raw {
            self.raw = false;
        } else if let Some(previous) = self.prev_view.take() {
            self.view = previous;
            self.lifeline_window.set(Duration::ZERO);
        } else {
            return Action::None;
        }
        Action::Redraw
    }

    /// Applies one command of the scenario director.
    pub fn apply_director(&mut self, command: UiCommand, live: &Model) {
        match command {
            UiCommand::Tab(view) => self.go(view),
            UiCommand::Select(label) => {
                if let Some(row) = data::row_labeled(self.shown(live), &label) {
                    self.selected = Some(row.member.peer.gossip_addr);
                }
            }
            UiCommand::Cache(name) => self.cache = Some(name),
            UiCommand::Help(open) => self.help = open,
            UiCommand::Caption(text) => self.caption = text,
            UiCommand::Quit => {}
        }
    }

    /// Notes the model's new state: starts a mosaic flash for each cache
    /// whose view changed and retargets the share-bar tweens. A frozen
    /// display notes nothing.
    pub fn observe(&mut self, model: &Model, now: Instant) {
        if self.frozen.is_some() {
            return;
        }
        let animate = self.anim;
        // Digests found while the observer is still discovering the cluster
        // are the baseline: no bucket moved.
        let flash = animate && !model.discovering();
        for digest in model.ownership_digests() {
            let changed = self
                .motion
                .previous
                .get(&digest.cache)
                .is_none_or(|prev| (prev.view_hash, prev.k) != (digest.view_hash, digest.k));
            if changed {
                if let Some(prev) = self.motion.previous.get(&digest.cache)
                    && flash
                {
                    self.motion.flashes.insert(
                        digest.cache.clone(),
                        MosaicFlash {
                            prev: remap_lead(prev, digest),
                            since: now,
                        },
                    );
                }
                self.motion
                    .previous
                    .insert(digest.cache.clone(), digest.clone());
            }
            for &(node, count) in &digest.counts {
                let target = crate::model::derive::share_fraction(count);
                let tween = self
                    .motion
                    .shares
                    .entry((digest.cache.clone(), node))
                    .or_insert_with(|| Tween::new(if animate { 0.0 } else { target }));
                tween.set(target);
                if !animate {
                    *tween = Tween::new(target);
                }
            }
        }
        let live: Vec<(SmolStr, Vec<NodeId>)> = model
            .ownership_digests()
            .map(|d| (d.cache.clone(), d.eligible.clone()))
            .collect();
        self.motion.shares.retain(|(cache, node), _| {
            live.iter()
                .any(|(name, eligible)| name == cache && eligible.contains(node))
        });
        self.motion
            .previous
            .retain(|cache, _| live.iter().any(|(name, _)| name == cache));
        self.motion
            .flashes
            .retain(|cache, _| live.iter().any(|(name, _)| name == cache));
    }

    /// Advances the share-bar tweens by `dt`.
    pub fn step(&mut self, dt: Duration) {
        if self.frozen.is_some() {
            return;
        }
        for tween in self.motion.shares.values_mut() {
            tween.step(dt);
        }
    }

    /// Puts every tween on its target and ends every flash.
    pub fn snap(&mut self) {
        for tween in self.motion.shares.values_mut() {
            tween.step(Duration::from_secs(3600));
        }
        self.motion.flashes.clear();
    }

    /// The animated share of `node` in `cache`, falling back to `target`
    /// before the interface has observed the digest.
    #[must_use]
    pub fn share(&self, cache: &str, node: NodeId, target: f64) -> f64 {
        self.motion
            .shares
            .get(&(SmolStr::new(cache), node))
            .map_or(target, Tween::value)
    }

    /// The mosaic flash of `cache` at `now`: the previous leads and the
    /// intensity, while the flash lasts.
    #[must_use]
    pub fn flash(&self, cache: &str, now: Instant) -> Option<(&[u8; BUCKETS], f64)> {
        if !self.anim {
            return None;
        }
        let flash = self.motion.flashes.get(cache)?;
        let intensity = anim::pulse(now.saturating_duration_since(flash.since), MOSAIC_FLASH);
        (intensity > 0.0).then_some((&*flash.prev, intensity))
    }

    /// Whether anything on screen is moving at `now`: a tween or a flash in
    /// progress, a blinking departing row, a fresh event or join, or a down
    /// row inside its hold.
    #[must_use]
    pub fn animating(&self, live: &Model, now: Instant, wall: SystemTime) -> bool {
        if !self.anim || self.frozen.is_some() {
            return false;
        }
        if self.motion.shares.values().any(|tween| !tween.is_done()) {
            return true;
        }
        if self
            .motion
            .flashes
            .values()
            .any(|flash| now.saturating_duration_since(flash.since) < MOSAIC_FLASH)
        {
            return true;
        }
        let shown = self.shown(live);
        let fresh =
            |at: SystemTime, span: Duration| wall.duration_since(at).is_ok_and(|age| age < span);
        shown
            .events()
            .iter()
            .rev()
            .take(8)
            .any(|event| fresh(event.at, EVENT_FLASH))
            || data::all_node_rows(shown).iter().any(|row| {
                use sundog::observe::MemberStatus::{Departing, Down, Live};
                match row.status() {
                    Departing => true,
                    Live => row.uptime(wall).is_some_and(|up| up < JOIN_PULSE),
                    Down => fresh(row.member.since, DOWN_HOLD),
                    _ => false,
                }
            })
    }
}

/// A move of a selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// One earlier (up).
    Prev,
    /// One later (down).
    Next,
    /// The first.
    First,
    /// The last.
    Last,
}

impl Step {
    /// The index selected after the step, from `current`, over `len` items.
    /// `None` when there are none.
    fn apply(self, current: Option<usize>, len: usize) -> Option<usize> {
        if len == 0 {
            return None;
        }
        let last = len - 1;
        Some(match self {
            Self::First => 0,
            Self::Last => last,
            Self::Prev => current.map_or(0, |index| index.saturating_sub(1)),
            Self::Next => current.map_or(0, |index| (index + 1).min(last)),
        })
    }
}

#[cfg(test)]
mod tests {
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
}
