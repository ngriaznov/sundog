//! The `sundog-lens` binary.

use std::process::ExitCode;

use sundog_lens::cli;

fn main() -> ExitCode {
    match cli::parse(std::env::args().skip(1)) {
        Ok(_) => {
            print!("{}", cli::HELP);
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("sundog-lens: {error}");
            eprintln!("try 'sundog-lens --help'");
            ExitCode::from(2)
        }
    }
}
