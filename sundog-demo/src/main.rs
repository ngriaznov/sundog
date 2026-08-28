//! `sundog-demo`: an interactive chaos-testing TUI for a `sundog` cluster —
//! spawns N in-process nodes over static loopback seeds, drives a
//! background write-load, and lets you kill/restart nodes to watch
//! replication and anti-entropy repair the divergence live (plan §11.4).
//! `--headless <SECS>` runs the same load without a terminal, for CI smoke
//! checks.

mod app;
mod cli;
mod convergence;
mod headless;
mod load;
mod node;
mod setup;
mod ui;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossterm::event::{self, Event as TermEvent};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = cli::parse(std::env::args().skip(1))?;

    if let Some(duration) = args.headless {
        let code = headless::run(&args, duration).await?;
        std::process::exit(code);
    }

    run_tui(&args).await
}

async fn run_tui(args: &cli::Args) -> anyhow::Result<()> {
    let demo = setup::bootstrap(args).await?;
    let mut app = app::App::new(demo);

    let mut terminal = ratatui::try_init()?;
    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<TermEvent>();
    let stop_input = Arc::new(AtomicBool::new(false));
    let input_thread = spawn_input_thread(input_tx, Arc::clone(&stop_input));

    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut draw_error = None;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                app.drain_feed();
                if let Err(error) = terminal.draw(|frame| ui::draw(frame, &app)) {
                    draw_error = Some(error);
                    break;
                }
            }
            maybe_event = input_rx.recv() => {
                match maybe_event {
                    Some(event) => app.handle_term_event(&event),
                    None => break,
                }
            }
        }
        if app.quit {
            break;
        }
    }

    stop_input.store(true, Ordering::Relaxed);
    ratatui::try_restore()?;
    drop(input_thread);

    app.demo.shutdown().await;
    if let Some(error) = draw_error {
        return Err(error.into());
    }
    Ok(())
}

fn spawn_input_thread(
    tx: mpsc::UnboundedSender<TermEvent>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match event::poll(Duration::from_millis(100)) {
                Ok(true) => match event::read() {
                    Ok(term_event) => {
                        if tx.send(term_event).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                },
                Ok(false) => {}
                Err(_) => break,
            }
        }
    })
}
