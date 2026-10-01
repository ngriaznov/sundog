//! The `sundog-lens` binary.

use std::process::ExitCode;

use sundog_lens::cli::{self, Command};
#[cfg(unix)]
use sundog_lens::{demo, fleet};
use sundog_lens::{once, watch};

/// What `cluster` and `demo` print on a host without Unix processes and
/// signals.
#[cfg(not(unix))]
const NO_FLEET: &str = "cluster and demo need a Unix host";

fn main() -> ExitCode {
    let command = match cli::parse(std::env::args().skip(1)) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("sundog-lens: {error}");
            eprintln!("try 'sundog-lens --help'");
            return ExitCode::from(2);
        }
    };
    let args = match command {
        Command::Help => {
            print!("{}", cli::HELP);
            return ExitCode::SUCCESS;
        }
        Command::Watch(args) => args,
        #[cfg(unix)]
        Command::Cluster(args) => return run_blocking(fleet::cluster_cmd(args)),
        #[cfg(unix)]
        Command::Demo(args) => return run_blocking(demo::run(args)),
        #[cfg(not(unix))]
        Command::Cluster(_) | Command::Demo(_) => {
            eprintln!("sundog-lens: {NO_FLEET}");
            return ExitCode::from(2);
        }
    };
    run_blocking(async {
        if args.once.is_some() {
            once::run(args).await
        } else {
            watch::run(args).await
        }
    })
}

/// Runs `command` to the end on a fresh runtime and maps its outcome to an
/// exit code.
fn run_blocking(command: impl Future<Output = anyhow::Result<()>>) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("sundog-lens: starting the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sundog-lens: {error:#}");
            ExitCode::FAILURE
        }
    }
}
