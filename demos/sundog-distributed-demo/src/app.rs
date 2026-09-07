//! Interactive TUI application state: node selection, event feed, key handling.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crossterm::event::{Event as TermEvent, KeyCode, KeyEvent, KeyEventKind};

use crate::convergence::{self, Convergence};
use crate::node::DISOWN_GRACE_ROUNDS;
use crate::setup::{self, Demo};

const FEED_CAPACITY: usize = 400;

/// The TUI's mutable state for one run: node handles under `demo`, the rest
/// presentation state.
pub(crate) struct App {
    pub(crate) demo: Demo,
    pub(crate) cursor: usize,
    pub(crate) selected: usize,
    pub(crate) feed: VecDeque<String>,
    pub(crate) started: Instant,
    pub(crate) quit: bool,
}

impl App {
    #[must_use]
    pub(crate) fn new(demo: Demo) -> Self {
        Self {
            demo,
            cursor: 0,
            selected: 0,
            feed: VecDeque::with_capacity(FEED_CAPACITY),
            started: Instant::now(),
            quit: false,
        }
    }

    /// A best-effort convergence snapshot: exact only once the load is
    /// paused, since it races the write load otherwise; the TUI reports it
    /// live regardless, the same way the sum-vs-expected numbers are shown
    /// live for feel rather than as a certified check (that's `--headless`'s
    /// job).
    #[must_use]
    pub(crate) fn convergence(&self) -> Convergence {
        let (total, live) = convergence::total_live_entries(&self.demo.nodes);
        let expected = convergence::expected_entries(
            u64::from(self.demo.owners.get()),
            self.demo.state.surviving_keys(),
        );
        convergence::check(total, expected, live)
    }

    pub(crate) fn drain_feed(&mut self) {
        while let Ok(line) = self.demo.feed_rx.try_recv() {
            if self.feed.len() >= FEED_CAPACITY {
                self.feed.pop_front();
            }
            self.feed.push_back(line);
        }
    }

    pub(crate) fn handle_term_event(&mut self, event: &TermEvent) {
        let TermEvent::Key(key) = event else { return };
        let key = *key;
        if key.kind != KeyEventKind::Press {
            return;
        }
        self.handle_key(key);
    }

    fn handle_key(&mut self, key: KeyEvent) {
        let last = self.demo.nodes.len().saturating_sub(1);
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.quit = true,
            KeyCode::Down | KeyCode::Char('j') => self.cursor = (self.cursor + 1).min(last),
            KeyCode::Up | KeyCode::Char('k') => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Enter => self.selected = self.cursor,
            KeyCode::Char(c @ '1'..='9') => self.select_by_digit(c, last),
            KeyCode::Char('K') => self.spawn_kill(),
            KeyCode::Char('R') => self.spawn_restart(),
            KeyCode::Char('P') => self.toggle_paused(),
            _ => {}
        }
    }

    fn select_by_digit(&mut self, digit: char, last: usize) {
        let idx = usize::try_from(digit as u32 - '1' as u32).unwrap_or(usize::MAX);
        if idx <= last {
            self.cursor = idx;
            self.selected = idx;
        }
    }

    fn toggle_paused(&self) {
        let was_paused = self.demo.paused.fetch_xor(true, Ordering::AcqRel);
        let _ = self.demo.feed_tx.send(format!(
            "load {}",
            if was_paused { "resumed" } else { "paused" }
        ));
    }

    fn spawn_kill(&self) {
        let node = Arc::clone(&self.demo.nodes[self.selected]);
        let feed_tx = self.demo.feed_tx.clone();
        tokio::spawn(async move { node.kill(&feed_tx).await });
    }

    fn spawn_restart(&self) {
        let node = Arc::clone(&self.demo.nodes[self.selected]);
        let feed_tx = self.demo.feed_tx.clone();
        let cluster_name = self.demo.cluster_name.clone();
        let seeds = self.demo.seeds.clone();
        let owners = self.demo.owners;
        tokio::spawn(async move {
            node.restart(
                &cluster_name,
                &seeds,
                setup::AE_INTERVAL,
                setup::TOMBSTONE_TTL,
                owners,
                &feed_tx,
            )
            .await;
        });
    }
}

/// Exposed for `ui`'s status line: the poll bound convergence would need to
/// settle within, informational only in the TUI.
#[must_use]
pub(crate) fn convergence_deadline_secs() -> u64 {
    convergence::poll_deadline(setup::AE_INTERVAL, DISOWN_GRACE_ROUNDS).as_secs()
}
