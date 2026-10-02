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
    assert!(output.stdout.is_empty(), "{:?}", output.stdout);
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

#[test]
fn watch_without_a_terminal_refuses_and_points_at_once() {
    // The test harness pipes stdout, so it is no terminal.
    let output = lens(&["watch", "lens-demo", "--seed", "127.0.0.1:9"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty(), "{:?}", output.stdout);
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("needs a terminal"), "{stderr}");
    assert!(stderr.contains("--once"), "{stderr}");
}

#[test]
fn a_bad_metrics_template_fails_before_any_network_use() {
    let output = lens(&[
        "lens-demo",
        "--once",
        "--metrics",
        "http://{nope}/metrics",
        "--seed",
        "127.0.0.1:9",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--metrics"), "{stderr}");
    assert!(stderr.contains("nope"), "{stderr}");
}

#[cfg(unix)]
#[test]
fn the_fleet_commands_without_a_test_node_fail_before_starting_anything() {
    for command in ["cluster", "demo"] {
        let output = lens(&[command, "--testnode", "/no/such/sundog-testnode"]);
        assert_eq!(output.status.code(), Some(1), "{command}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("--testnode /no/such/sundog-testnode"),
            "{stderr}"
        );
        assert!(stderr.contains("not a file"), "{stderr}");
    }
}

#[cfg(unix)]
#[test]
fn a_scenario_file_with_a_bad_line_fails_naming_the_file_and_the_line() {
    let dir = std::env::temp_dir().join(format!("sundog-lens-cli-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let scenario = dir.join("bad.txt");
    std::fs::write(&scenario, "pause 1s\nfrobnicate\n").unwrap();
    let output = lens(&[
        "demo",
        "--headless",
        "--scenario",
        scenario.to_str().unwrap(),
        "--testnode",
        "/no/such/sundog-testnode",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("bad.txt"), "{stderr}");
    assert!(stderr.contains("line 2"), "{stderr}");
    assert!(stderr.contains("frobnicate"), "{stderr}");
}

#[cfg(not(unix))]
#[test]
fn the_fleet_commands_need_a_unix_host() {
    for command in ["cluster", "demo"] {
        let output = lens(&[command]);
        assert_eq!(output.status.code(), Some(2), "{command}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("need a Unix host"), "{stderr}");
    }
}
