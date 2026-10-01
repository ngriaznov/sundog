//! The `sundog-lens` binary: help, and a refusal of bad arguments.

use std::process::Command;

fn lens(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_sundog-lens"))
        .args(args)
        .output()
        .expect("the binary runs")
}

#[test]
fn no_arguments_print_the_help_and_exit_zero() {
    let output = lens(&[]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.starts_with("sundog-lens: watch a sundog cluster from outside"));
    assert!(stdout.contains("USAGE"));
}

#[test]
fn help_flags_print_the_help() {
    for flag in ["--help", "-h"] {
        let output = lens(&[flag]);
        assert!(output.status.success(), "{flag}");
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            sundog_lens::cli::HELP
        );
    }
}

#[test]
fn a_bad_flag_exits_two_and_names_the_flag() {
    let output = lens(&["lens-demo", "--bogus"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--bogus"), "{stderr}");
    assert!(stderr.contains("--help"), "{stderr}");
}

#[test]
fn a_missing_cluster_exits_two() {
    let output = lens(&["watch"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("cluster name")
    );
}
