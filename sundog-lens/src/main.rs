//! The `sundog-lens` binary.

use std::process::ExitCode;

use sundog_lens::cli::{self, Command};
use sundog_lens::{once, watch};

/// What `cluster` and `demo` print: this build starts no local fleet.
const NO_FLEET: &str = "this build does not start a local fleet";

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
        Command::Cluster(_) | Command::Demo(_) => {
            eprintln!("sundog-lens: {NO_FLEET}");
            return ExitCode::from(2);
        }
    };
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
    let outcome = runtime.block_on(async {
        if args.once.is_some() {
            once::run(args).await
        } else {
            watch::run(args).await
        }
    });
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sundog-lens: {error:#}");
            ExitCode::FAILURE
        }
    }
}
