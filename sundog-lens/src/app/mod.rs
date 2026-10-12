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

use crate::ask::Target;
use crate::explained::Explained;
use crate::key::{self, KeySpec};
use crate::model::Model;
use crate::model::events::Filter;
use crate::model::ownership::{BUCKETS, NO_LEAD, OwnershipDigest};
use crate::source::targets::UrlTemplate;
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
    /// Send a request to the demo's fleet: an action for a demo key, or an
    /// explain request (demo only).
    Fleet(FleetCmd),
}

/// What the interface asks of the demo: a fleet action for a demo key, or a
/// request to ask test nodes about a key.
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
    /// Ask test nodes about a key. The demo answers it with
    /// [`crate::ask::answer`]; the fleet never applies it.
    Explain(ExplainRequest),
}

/// A request to ask test nodes what they make of a key. [`crate::ask::answer`]
/// runs it and returns the answers as a [`UiCommand::Explained`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainRequest {
    /// The request's number; an answer carries it back.
    pub id: u64,
    /// The key: the text after `explain ` on the control line.
    pub key: String,
    /// The nodes to ask, in slot order.
    pub targets: Vec<Target>,
}

/// What reaches the interface from outside the keyboard: the scenario
/// director's commands and the answers to an explain request.
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
    /// The test nodes' answers to an [`ExplainRequest`].
    Explained(Box<Explained>),
    /// Open the explain overlay on `key`, as typed, with `answers` already in:
    /// the scenario asked the test nodes itself.
    Explain {
        /// The key as typed, with its prefix.
        key: String,
        /// What the test nodes answered.
        answers: Box<Explained>,
    },
    /// Close the explain overlay.
    CloseExplain,
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
    /// Where each node's control port is, when the nodes are test nodes the
    /// interface can ask about a key: `{ip}:{gossip_port+134}`.
    pub control: Option<UrlTemplate>,
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
            control: None,
        }
    }
}

/// The key shown the first time the explain overlay opens in the demo: the
/// fleet's keys are `k{n}`, so it names a key the test nodes hold.
pub const SEED_KEY: &str = "k1";

/// The explain overlay: the key being typed and what the test nodes
/// answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainState {
    /// The key as typed, with its prefix.
    pub text: String,
    /// What the last `Enter` did when it asked nobody, or whom it asked.
    pub note: Option<String>,
    /// The number of the request whose answer the overlay waits for.
    pub pending: Option<u64>,
    /// The answers to the last request that came back.
    pub result: Option<Box<Explained>>,
}

/// Whether the overlay waits for `answer`: it is open and the answer carries
/// the number of its pending request. Any other answer, such as one that
/// arrives after the overlay closed, is dropped.
#[must_use]
pub fn accepts(explain: &Option<ExplainState>, answer: &Explained) -> bool {
    explain
        .as_ref()
        .is_some_and(|state| state.pending == Some(answer.id))
}

/// What the overlay says while it waits: the nodes asked, and the live nodes
/// the control template gives no address.
fn asking_note(targets: &data::ControlTargets) -> String {
    let names = |labels: Vec<&str>| labels.join(" ");
    let asked = names(targets.asked.iter().map(|t| t.label.as_str()).collect());
    if targets.skipped.is_empty() {
        format!("asking {asked}")
    } else {
        let skipped = names(targets.skipped.iter().map(|(l, _)| l.as_str()).collect());
        format!("asking {asked} · not asked: {skipped}")
    }
}

/// The key a request for `text` carries, when a test node can be asked about
/// it: the text a control line sends after `explain `.
#[must_use]
pub fn asked_key(text: &str) -> Option<String> {
    let spec = KeySpec::parse(text).ok()?;
    key::not_askable(&spec)
        .is_none()
        .then(|| spec.text().to_owned())
}

/// The answers to show for the text typed: the last result, while the text
/// still names the key it answered.
#[must_use]
pub fn result_for(state: &ExplainState) -> Option<&Explained> {
    let asked = asked_key(&state.text)?;
    state.result.as_deref().filter(|result| result.key == asked)
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
    /// The explain overlay, while it is open.
    pub explain: Option<ExplainState>,
    prev_view: Option<View>,
    selected: Option<SocketAddr>,
    cache: Option<SmolStr>,
    frozen: Option<Model>,
    pinned: bool,
    scroll: usize,
    pin_len: usize,
    last_key: Option<String>,
    next_id: u64,
    config: AppConfig,
    motion: Motion,
    lifeline_window: Cell<Duration>,
    too_small: Cell<bool>,
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
            explain: None,
            prev_view: None,
            selected: None,
            cache: None,
            frozen: None,
            pinned: false,
            scroll: 0,
            pin_len: 0,
            last_key: None,
            next_id: 1,
            config,
            motion: Motion::default(),
            lifeline_window: Cell::new(Duration::ZERO),
            too_small: Cell::new(false),
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

    /// Records whether the last frame was too small to draw anything but the
    /// size notice. The explain overlay takes keys only while it is on
    /// screen, so the next key press reads this.
    pub fn note_too_small(&self, small: bool) {
        self.too_small.set(small);
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
    /// overlay, else the explain overlay, else the raw-sample list, else
    /// returns to the previous view; it never quits. While help is open every key
    /// but `?`, `Esc`, `q` and `Ctrl-C` is ignored. Otherwise, while the
    /// explain overlay is on screen, it takes every key but `Ctrl-C`:
    /// letters, `q` and `?` type into the key. The overlay is not on screen
    /// when the last frame was [too small](Self::note_too_small), and then
    /// `q` quits, `e` opens nothing and `Esc` still closes the overlay. The
    /// demo keys `S K L R` act only in demo mode.
    pub fn handle_key(&mut self, key: KeyEvent, live: &Model) -> Action {
        if key.kind == KeyEventKind::Release {
            return Action::None;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl && key.code == KeyCode::Char('c') {
            return Action::Quit;
        }
        if self.explain.is_some() && !self.help && !self.too_small.get() {
            return self.explain_key(key, live);
        }
        if key.code == KeyCode::Char('q') {
            return Action::Quit;
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
            KeyCode::Char('e') if !self.too_small.get() => self.open_explain(),
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

    /// Opens the explain overlay on the key typed last; the first time, the
    /// demo's seed key, which its test nodes hold.
    fn open_explain(&mut self) {
        let text = self.last_key.clone().unwrap_or_else(|| {
            if self.config.demo {
                SEED_KEY.to_owned()
            } else {
                String::new()
            }
        });
        self.explain = Some(ExplainState {
            text,
            note: None,
            pending: None,
            result: None,
        });
    }

    /// Closes the explain overlay and keeps its text for the next opening.
    /// An answer to a request still pending is dropped when it arrives.
    fn close_explain(&mut self) {
        if let Some(state) = self.explain.take() {
            self.last_key = Some(state.text);
        }
    }

    /// Handles a key while the explain overlay is open: it types into the
    /// key, `Enter` asks the test nodes, `Up` and `Down` select a node, `Esc`
    /// closes the overlay.
    fn explain_key(&mut self, key: KeyEvent, live: &Model) -> Action {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Esc => return self.escape(),
            KeyCode::Enter => return self.ask(live),
            KeyCode::Up => self.select_row(live, Step::Prev),
            KeyCode::Down => self.select_row(live, Step::Next),
            KeyCode::Backspace => self.edit_key(|text| {
                text.pop();
            }),
            KeyCode::Char('u') if ctrl => self.edit_key(String::clear),
            KeyCode::Char(c) if !ctrl && !key.modifiers.contains(KeyModifiers::ALT) => {
                self.edit_key(|text| {
                    if text.chars().count() < key::MAX_CHARS {
                        text.push(c);
                    }
                });
            }
            _ => return Action::None,
        }
        Action::Redraw
    }

    /// Edits the key being typed and drops the note of the last `Enter`, which
    /// described the key before the edit.
    fn edit_key(&mut self, edit: impl FnOnce(&mut String)) {
        if let Some(state) = &mut self.explain {
            edit(&mut state.text);
            state.note = None;
        }
    }

    /// Asks the test nodes about the key typed. A key a node cannot be asked
    /// about, a lens that knows no control ports and a cluster with no live
    /// node to ask each leave a note and ask nobody.
    fn ask(&mut self, live: &Model) -> Action {
        let Some(state) = &mut self.explain else {
            return Action::None;
        };
        let spec = match KeySpec::parse(&state.text) {
            Ok(spec) => spec,
            Err(error) => {
                state.note = Some(format!("nobody is asked: {error}"));
                return Action::Redraw;
            }
        };
        let Some(template) = &self.config.control else {
            state.note = Some(
                "nobody is asked: this lens does not know the nodes' control ports, \
                 which only the demo's test nodes serve"
                    .to_owned(),
            );
            return Action::Redraw;
        };
        if let Some(why) = key::not_askable(&spec) {
            state.note = Some(format!("nobody is asked: {why}"));
            return Action::Redraw;
        }
        let key = spec.text().to_owned();
        let targets = data::control_targets(live, template);
        if targets.asked.is_empty() {
            state.note = Some("nobody is asked: no live node has a control port to ask".to_owned());
            return Action::Redraw;
        }
        let id = self.next_id;
        self.next_id += 1;
        state.pending = Some(id);
        state.note = Some(asking_note(&targets));
        Action::Fleet(FleetCmd::Explain(ExplainRequest {
            id,
            key,
            targets: targets.asked,
        }))
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
        if self.explain.is_some() {
            self.close_explain();
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

    /// Applies one command for the interface: the scenario director's, or an
    /// answer to an explain request. A scripted `Explain` replaces the
    /// overlay as it stands; `CloseExplain` closes it and keeps its text for
    /// the next opening, as `Esc` does.
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
            UiCommand::Explained(answer) => {
                if accepts(&self.explain, &answer)
                    && let Some(state) = &mut self.explain
                {
                    state.pending = None;
                    state.note = None;
                    state.result = Some(answer);
                }
            }
            UiCommand::Explain { key, answers } => {
                self.explain = Some(ExplainState {
                    text: key,
                    note: None,
                    pending: None,
                    result: Some(answers),
                });
            }
            UiCommand::CloseExplain => self.close_explain(),
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
mod tests;
